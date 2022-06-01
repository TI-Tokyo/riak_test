%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
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
%% @doc Verification of AAE fold based on n_val (with cached trees)
%%
%% Confirm that trees are returned that vary along with the data in the
%% store

-module(verify_aaefold_nval).
-export([confirm/0, verify_aae_fold/1]).
-include_lib("eunit/include/eunit.hrl").

% I would hope this would come from the testing framework some day
% to use the test in small and large scenarios.
-define(DEFAULT_RING_SIZE, 8).
-define(REBUILD_TICK, 30 * 1000).
-define(CFG_NOREBUILD(FetchClocksRepair),
        [{riak_kv,
          [
           {anti_entropy, {off, []}},
           {aae_fetchclocks_repair, FetchClocksRepair},
           {tictacaae_active, active},
           {tictacaae_parallelstore, leveled_ko},
                % if backend not leveled will use parallel key-ordered
                % store
           {tictacaae_rebuildwait, 4},
           {tictacaae_rebuilddelay, 3600},
           {tictacaae_exchangetick, 5 * 1000}, % 5 seconds
           {tictacaae_rebuildtick, 3600000} % don't tick for an hour!
          ]},
         {riak_core,
          [
           {ring_creation_size, ?DEFAULT_RING_SIZE}
          ]}]
       ).
-define(CFG_REBUILD,
        [{riak_kv,
          [
           % Speedy AAE configuration
           {anti_entropy, {off, []}},
           {tictacaae_active, active},
           {tictacaae_parallelstore, leveled_ko},
                % if backend not leveled will use parallel key-ordered
                % store
           {tictacaae_rebuildwait, 0},
           {tictacaae_rebuilddelay, 60},
           {tictacaae_exchangetick, 5 * 1000}, % 5 seconds
           {tictacaae_rebuildtick, ?REBUILD_TICK} % Check for rebuilds!
          ]},
         {riak_core,
          [
           {ring_creation_size, ?DEFAULT_RING_SIZE}
          ]}]
       ).
-define(NUM_NODES, 3).
-define(NUM_KEYS_NVAL3_PERNODE, 8000).
-define(NUM_KEYS_NVAL2_PERNODE, 5000).
-define(BUCKET, <<"test_bucket">>).
-define(NVAL_TYPE, <<"nval2_type">>).
-define(NVAL_BUCKET, <<"nval2_bucket">>).
-define(N_VAL, 3).
-define(DELTA_COUNT, 10).

confirm() ->
    Nodes0 = rt:build_cluster(?NUM_NODES, ?CFG_NOREBUILD(false)),
    ok = verify_aae_fold(Nodes0),
    rt:clean_cluster(Nodes0),

    Nodes1 = rt:build_cluster(?NUM_NODES, ?CFG_NOREBUILD(true)),
    ok = verify_aae_fold(Nodes1),
    rt:clean_cluster(Nodes1),

    Nodes2 = rt:build_cluster(?NUM_NODES, ?CFG_REBUILD),
    logger:info("Sleeping for twice rebuild tick - testing with rebuilds ongoing"),
    timer:sleep(2 * ?REBUILD_TICK),
    ok = verify_aae_fold(Nodes2),
    pass.


verify_aae_fold(Nodes) ->
    
    {ok, CH} = riak:client_connect(hd(Nodes)),
    {ok, CT} = riak:client_connect(lists:last(Nodes)),

    logger:info("Fold for empty root"),
    {ok, RH0} = riak_client:aae_fold({merge_root_nval, ?N_VAL}, CH),
    {ok, RT0} = riak_client:aae_fold({merge_root_nval, ?N_VAL}, CT),
    NoDirtyBranches = aae_exchange:compare_roots(RH0, RT0),
    logger:info("NoDirtyBranches ~w", [NoDirtyBranches]),

    logger:info("Commencing object load"),
    KeyLoadFun = 
        fun(Node, KeyCount) ->
            KVs = test_data(KeyCount + 1,
                                KeyCount + ?NUM_KEYS_NVAL3_PERNODE,
                                list_to_binary("U1")),
            ok = write_data(Node, KVs),
            KeyCount + ?NUM_KEYS_NVAL3_PERNODE
        end,

    lists:foldl(KeyLoadFun, 1, Nodes),
    logger:info("Loaded ~w objects", [?NUM_KEYS_NVAL3_PERNODE * length(Nodes)]),
    wait_until_root_stable(CH),
    wait_until_root_stable(CT),

    logger:info("Fold for busy root"),
    {ok, RH1} = riak_client:aae_fold({merge_root_nval, ?N_VAL}, CH),
    {ok, RT1} = riak_client:aae_fold({merge_root_nval, ?N_VAL}, CT),
    
    ?assertMatch(true, RH1 == RT1),
    ?assertMatch(true, RH0 == RT0),
    ?assertMatch(false, RH0 == RH1),
    
    logger:info("Make ~w changes", [?DELTA_COUNT]),
    Changes2 = test_data(1, ?DELTA_COUNT, list_to_binary("U2")),
    ok = write_data(hd(Nodes), Changes2),
    wait_until_root_stable(CH),

    {ok, RH2} = riak_client:aae_fold({merge_root_nval, ?N_VAL}, CH),
    DirtyBranches2 = aae_exchange:compare_roots(RH1, RH2),

    logger:info("Found ~w branch deltas", [length(DirtyBranches2)]),
    ?assertMatch(true, length(DirtyBranches2) > 0),
    ?assertMatch(true, length(DirtyBranches2) =< ?DELTA_COUNT),

    {ok, BH2} =
        riak_client:aae_fold({merge_branch_nval, ?N_VAL, DirtyBranches2}, CH),

    logger:info("Make ~w changes to same keys", [?DELTA_COUNT]),
    Changes3 = test_data(1, ?DELTA_COUNT, list_to_binary("U3")),
    ok = write_data(hd(Nodes), Changes3),
    wait_until_root_stable(CH),

    {ok, RH3} = riak_client:aae_fold({merge_root_nval, ?N_VAL}, CH),
    DirtyBranches3 = aae_exchange:compare_roots(RH2, RH3),

    logger:info("Found ~w branch deltas", [length(DirtyBranches3)]),
    ?assertMatch(true, DirtyBranches2 == DirtyBranches3),

    {ok, BH3} =
        riak_client:aae_fold({merge_branch_nval, ?N_VAL, DirtyBranches3}, CH),
    
    DirtySegments1 = aae_exchange:compare_branches(BH2, BH3),
    logger:info("Found ~w mismatched segments", [length(DirtySegments1)]),
    ?assertMatch(true, length(DirtySegments1) > 0),
    ?assertMatch(true, length(DirtySegments1) =< ?DELTA_COUNT),

    {ok, KCL1} =
        riak_client:aae_fold({fetch_clocks_nval, ?N_VAL, DirtySegments1}, CH),

    logger:info("Found ~w mismatched keys", [length(KCL1)]),

    ?assertMatch(true, length(KCL1) >= ?DELTA_COUNT),
    MappedKCL1 = lists:map(fun({B, K, VC}) -> {{B, K}, VC} end, KCL1),

    logger:info("Checking all mismatched keys in result"),
    MatchFun = 
        fun(I) ->
            K = to_key(I),
            InFetchClocks = lists:keyfind({?BUCKET, K}, 1, MappedKCL1),
            ?assertMatch(true, {?BUCKET, K} == element(1, InFetchClocks))
        end,
    lists:foreach(MatchFun, lists:seq(1, ?DELTA_COUNT)),
    
    logger:info("Create bucket type with nval of 2"),
    rt:create_and_activate_bucket_type(hd(Nodes), ?NVAL_TYPE, [{n_val, 2}]),
    rt:wait_until_bucket_type_status(?NVAL_TYPE, active, Nodes),
    rt:wait_until_bucket_props(Nodes, {?NVAL_TYPE, ?NVAL_BUCKET}, [{n_val, 2}]),

    logger:info("Stop and start nodes to notice new preflists"),
    lists:foreach(fun(N) -> rt:stop_and_wait(N) end, Nodes),
    lists:foreach(fun(N) -> rt:start_and_wait(N) end, Nodes),
    rt:wait_until_all_members(Nodes),
    rt:wait_for_cluster_service(Nodes, riak_kv),

    logger:info("Check fetch_clocks for n_val 3"),
    {ok, KCL2} =
        riak_client:aae_fold({fetch_clocks_nval, ?N_VAL, DirtySegments1}, CH),
    logger:info("Check fetch root for n_val 2 is empty root"),
    {ok, RH0N2} = riak_client:aae_fold({merge_root_nval, 2}, CH),
    ?assertMatch(KCL1, KCL2),
    ?assertMatch(RH0, RH0N2),
    {ok, RB0N2} =
        riak_client:aae_fold({merge_branch_nval, 2, lists:seq(1, 128)}, CH),
    
    logger:info("Write values to nval=2 bucket"),
    KVs =
        test_data(1,
                    ?NUM_NODES * ?NUM_KEYS_NVAL2_PERNODE,
                    list_to_binary("NV2")),
    ok = write_data({?NVAL_TYPE, ?NVAL_BUCKET}, hd(Nodes), KVs, []),
    logger:info("Check fetch root for n_val 2 is not empty"),
    {ok, RH1N2} = riak_client:aae_fold({merge_root_nval, 2}, CH),
    ?assertMatch(false, RH0N2 == RH1N2),

    logger:info("Check fetch_clocks for n_val=3 unchanged"),
    {ok, KCL3} =
        riak_client:aae_fold({fetch_clocks_nval, ?N_VAL, DirtySegments1}, CH),
    ?assertMatch(KCL2, KCL3),
    logger:info("Check root for n_val=3 unchanged"),
    {ok, RH4} = riak_client:aae_fold({merge_root_nval, ?N_VAL}, CH),
    ?assertMatch(RH3, RH4),

    logger:info("Fetch clocks for nval=2 bucket"),
    {ok, RB1N2} =
        riak_client:aae_fold({merge_branch_nval, 2, lists:seq(1, 64)}, CH),
    DirtySegmentsN2 = aae_exchange:compare_branches(RB1N2, RB0N2),
    logger:info("Dirty segments ~w", [length(DirtySegmentsN2)]),
    logger:info("Confirm only nval=2 keys returned from fetch_clocks nval2"),
    {ok, KCL0N2} =
        riak_client:aae_fold({fetch_clocks_nval,
                                    2,
                                    lists:sublist(DirtySegmentsN2, 32)},
                                CH),
    lists:foreach(fun({B, _K, _C}) ->
                        ?assertMatch({?NVAL_TYPE, ?NVAL_BUCKET}, B)
                    end,
                    KCL0N2),
    logger:info("Fetched ~w keys from 32 dirty segments", [length(KCL0N2)]),
    ?assertMatch(true, length(KCL0N2) >= 32),
    {ok, KCL1N2} =
        riak_client:aae_fold({fetch_clocks_nval,
                                    2,
                                    lists:sublist(DirtySegmentsN2, 32)},
                                CT),
    ?assertMatch(true, lists:sort(KCL0N2) == lists:sort(KCL1N2)),



    logger:info("No nval=2 keys from fetch_clocks when searching for nval=3"),
    {ok, KCL0N3} =
        riak_client:aae_fold({fetch_clocks_nval,
                                    3,
                                    lists:sublist(DirtySegmentsN2, 10)},
                                CH),
    lists:foreach(fun({B, _K, _C}) ->
                        ?assertMatch(?BUCKET, B)
                    end,
                    KCL0N3),

    logger:info("Stopping a node - query results should be unchanged"),
    rt:stop_and_wait(hd(tl(Nodes))),
    {ok, BH4} =
        riak_client:aae_fold({merge_branch_nval, ?N_VAL, DirtyBranches3}, CH),
    ?assertMatch(true, BH3 == BH4),
    {ok, KCL2} =
        riak_client:aae_fold({fetch_clocks_nval, ?N_VAL, DirtySegments1}, CH),
    ?assertMatch(true, lists:sort(KCL1) == lists:sort(KCL2)),

    
    % Need to re-start or clean will fail
    rt:start_and_wait(hd(tl(Nodes))).


to_key(N) ->
    list_to_binary(io_lib:format("K~4..0B", [N])).

test_data(Start, End, V) ->
    Keys = [to_key(N) || N <- lists:seq(Start, End)],
    [{K, <<K/binary, V/binary>>} || K <- Keys].

write_data(Node, KVs) ->
    write_data(?BUCKET, Node, KVs, []).

write_data(Bucket, Node, KVs, Opts) ->
    PB = rt:pbc(Node),
    [begin
         O =
         case riakc_pb_socket:get(PB, Bucket, K) of
             {ok, Prev} ->
                 riakc_obj:update_value(Prev, V);
             _ ->
                 riakc_obj:new(Bucket, K, V)
         end,
         ?assertMatch(ok, riakc_pb_socket:put(PB, O, Opts))
     end || {K, V} <- KVs],
    riakc_pb_socket:stop(PB),
    timer:sleep(5000),
    ok.

wait_until_root_stable(Client) ->
    {ok, RH0} = riak_client:aae_fold({merge_root_nval, ?N_VAL}, Client),
    timer:sleep(2000),
    {ok, RH1} = riak_client:aae_fold({merge_root_nval, ?N_VAL}, Client),
    case aae_exchange:compare_roots(RH0, RH1) of
        [] ->
            logger:info("Root appears stable matched");
        [L] ->
            Pre = L * 4,
            <<_B0:Pre/binary, V0:32/integer, _Post0/binary>> = RH0,
            <<_B1:Pre/binary, V1:32/integer, _Post1/binary>> = RH1,
            logger:info("Root not stable: branch ~w compares ~w with ~w",
                        [L, V0, V1]),
            wait_until_root_stable(Client)
    end.
