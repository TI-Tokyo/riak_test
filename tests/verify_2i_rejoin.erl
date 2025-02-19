%% -------------------------------------------------------------------
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
%% 
%% A single node test, that exercises the API, and allows for profiling
%% of that API activity

-module(verify_2i_rejoin).
-export([confirm/0]).

-import(secondary_index_tests, [http_query/3, pb_query/3]).

-import(location, [
    plan_and_wait/2
]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("riakc/include/riakc.hrl").

-define(DEFAULT_RING_SIZE, 16).
-define(CLIENT_COUNT, 2).
-define(QUERY_EVERY, 100).
-define(GET_EVERY, 4).
-define(GETS_PER_GET, 1).
-define(UPDATE_EVERY, 2).
-define(LOG_EVERY, 5000).
-define(KEY_COUNT, 160000).
-define(OBJECT_SIZE_BYTES, 256).
-define(REQUEST_PAUSE_UPTO, 3).
-define(QUERY_COUNT, 100000).

-if(?OTP_RELEASE > 23).
-define(RPC_MODULE, erpc).
-else.
-define(RPC_MODULE, rpc).
-endif.

-define(CONF,
        [
            {riak_kv,
                [
                    {anti_entropy, {off, []}},
                    {delete_mode, keep},
                    {tictacaae_active, active},
                    {tictacaae_parallelstore, leveled_ko},
                    {tictacaae_storeheads, true},
                    {tictacaae_rebuildtick, 3600000}, % don't tick for an hour!
                    {tictacaae_suspend, true},
                    {log_index_fsm, true}
                ]
            },
            {leveled,
                [
                    {compaction_runs_perday, 48},
                    {journal_objectcount, 20000}
                ]
            },
            {riak_core,
                [
                    {ring_creation_size, ?DEFAULT_RING_SIZE},
                    {default_bucket_props, [{allow_mult, true}]},
                    {handoff_concurrency,       8},
                    {forced_ownership_handoff,  8},
                    {vnode_inactivity_timeout,  4000},
                    {vnode_management_timer,    2000},
                    {gossip_limit, {50, 30000}}
                ]
            }
        ]
       ).

confirm() ->
    Nodes = rt:build_cluster(4, ?CONF),
    lists:foreach(fun(N) -> rt:wait_for_service(N, riak_kv) end, Nodes),
    confirm_pb(Nodes).

confirm_pb(Nodes) ->
    perf_test(Nodes, riakc_pb_socket, ?CLIENT_COUNT).

perf_test(Nodes, ClientMod, ClientCount) ->
    Node = hd(Nodes),
    Node4 = lists:last(Nodes),
    Clients = get_clients(ClientCount, Node, ClientMod),
    Buckets =
        lists:map(
            fun(I) -> list_to_binary(io_lib:format("BucketName~w", [I])) end,
            lists:seq(1, ClientCount)
        ),
    ClientBPairs = lists:zip(Clients, Buckets),

    TestProcess = self(),
    StartTime = os:system_time(millisecond),

    SpawnUpdateFun =
        fun({C, B}) ->
            fun() ->
                V = crypto:strong_rand_bytes(?OBJECT_SIZE_BYTES),
                lists:foreach(
                    fun(I) ->
                        act(C, ClientMod, B, I, V)
                    end,
                    lists:seq(1, ?KEY_COUNT)
                ),
                TestProcess ! complete
            end
        end,
    SpawnFuns = lists:map(SpawnUpdateFun, ClientBPairs),
    lists:foreach(fun spawn/1, SpawnFuns),
    
    ok = receive_complete(0, length(Clients)),

    ok = rt:staged_leave(Node4),
    rt:wait_until_ring_converged(Nodes),
    ok = plan_and_wait(Node, Nodes -- [Node4]),
    rt:wait_until_unpingable(Node4),

    {CQ, BQ} = hd(ClientBPairs),
    QueryFun =
        fun() ->
            QueryResult =
                ClientMod:get_index_range(
                    CQ,
                    BQ,
                    {binary_index, "bin5"},
                    to_index(?KEY_COUNT - 99), to_index(?KEY_COUNT),
                    []
                ),
            case QueryResult of
                {ok, ?INDEX_RESULTS{keys=HttpResKeys}}
                    when length(HttpResKeys) == 200 ->
                    ok;
                {ok, ?INDEX_RESULTS{keys=HttpResKeys}} ->
                    ?LOG_WARNING("Unexpected result count ~w", [length(HttpResKeys)]);
                Error ->
                    ?LOG_WARNING("Unexpected result ~0p", [Error])
            end
        end,

    SpawnQueryFun =
        fun() ->
            lists:foreach(
                fun(I) ->
                    QueryFun(),
                    case I rem 2000 of
                        0 ->
                            ?LOG_INFO("~w queries run", [I]);
                        _ ->
                            ok
                    end
                end,
                lists:seq(1, ?QUERY_COUNT)
            ),
            TestProcess ! complete
        end,
    spawn(SpawnQueryFun),

    rt:start_and_wait(Node4),
    rt:wait_until_pingable(Node4),
    rt:staged_join(Node4, Node),
    ok = plan_and_wait(Node, Nodes),

    ok = receive_complete(0, 1),

    close_clients(Clients, ClientMod),

    EndTime = os:system_time(millisecond),
    ?LOG_INFO("Test took ~w ms", [EndTime - StartTime]),
    pass.

receive_complete(Target, Target) ->
    ok;
receive_complete(T, Target) ->
    receive complete -> receive_complete(T + 1, Target) end.
    
get_clients(ClientsPerNode, Node, ClientMod) ->
    lists:map(
        fun(N) ->
            case ClientMod of
                riakc_pb_socket ->
                    rt:pbc(N);
                rhc ->
                    rt:httpc(N)
            end
        end,
        lists:flatten(lists:duplicate(ClientsPerNode, Node))
    ).

close_clients(Clients, ClientMod) ->
    case ClientMod of
        riakc_pb_socket ->
            lists:foreach(
                fun(C) -> riakc_pb_socket:stop(C) end,
                Clients
            );
        rhc ->
            ok
    end.

to_key(N) ->
    list_to_binary(io_lib:format("K~8..0B", [N])).

to_index(N) ->
    list_to_binary(io_lib:format("I~8..0B", [N])).

to_meta(N) ->
    list_to_binary(io_lib:format("M~8..0B", [N])).

act(Client, ClientMod, Bucket, I, V) ->
    K = to_key(I),
    Obj = riakc_obj:new(Bucket, K, <<I:32/integer, V/binary>>),
    MD0 = riakc_obj:get_metadata(Obj),
    FieldList =
        ["bin1", "bin2", "bin3", "bin4", "bin5", "bin6", "bin7", "bin8"],
    MD1 =
        lists:foldl(
            fun(IdxName, MDAcc) ->
                riakc_obj:set_secondary_index(
                    MDAcc,
                    {{binary_index, IdxName},
                    [to_index(I), to_index(I + 1)]}
                )
            end,
            MD0,
            FieldList
        ),
    MD2 =
        lists:foldl(
            fun(MDEntry, MDAcc) ->
                riakc_obj:set_user_metadata_entry(MDAcc, MDEntry)
            end,
            MD1,
            [{<<"K1">>, to_meta(I)}, {<<"K2">>, to_meta(I + 1)}]
        ),
    ok = ClientMod:put(Client, riakc_obj:update_metadata(Obj, MD2)),
    case I rem ?UPDATE_EVERY of
        0  when I > 1000 ->
            UpdK = to_key(rand:uniform(I - 1000)),
            {ok, PrvObj} = ClientMod:get(Client, Bucket, UpdK),
            NewObj =
                riakc_obj:update_value(PrvObj, <<I:32/integer, V/binary>>),
            PrvMD = riakc_obj:get_metadata(PrvObj),
            NewMD =
                riakc_obj:add_secondary_index(
                    PrvMD,
                    {{binary_index, "upd_index"}, [to_index(I), to_index(I+1)]}
                ),
            ok =
                ClientMod:put(
                    Client,
                    riakc_obj:update_metadata(NewObj, NewMD)
                );
        _ ->
            ok
    end,
    case I rem ?GET_EVERY of
        0 when I > 1000 ->
            lists:foreach(
                fun(_I) ->
                    {ok, _PastObj} =
                        ClientMod:get(
                            Client, Bucket, to_key(rand:uniform(I - 1000))
                        )
                end,
                lists:seq(1, ?GETS_PER_GET)
            );
        _ ->
            ok
    end,
    case I rem ?QUERY_EVERY of
        0 when I > ?QUERY_EVERY ->
            {ok, ?INDEX_RESULTS{keys=HttpResKeys}} =
                case ClientMod of
                    riakc_pb_socket ->
                        ClientMod:get_index_range(
                            Client,
                            Bucket,
                            {binary_index,
                                lists:nth(rand:uniform(5), FieldList)},
                            to_index(I - 99), to_index(I),
                            []
                        );
                    rhc ->
                        ClientMod:get_index(
                            Client,
                            Bucket,
                            {binary_index,
                                lists:nth(rand:uniform(5), FieldList)},
                            {to_index(I - 99), to_index(I)}
                        )
                end,
            ?assertMatch(200, length(HttpResKeys));
        _ ->
            ok
    end,
    case I rem ?LOG_EVERY of
        0 ->
            ?LOG_INFO("Client ~p at ~w", [Client, I]);
        _ ->
            ok
    end,
    timer:sleep(rand:uniform(?REQUEST_PAUSE_UPTO))
    .

