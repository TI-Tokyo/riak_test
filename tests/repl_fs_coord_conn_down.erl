%% -------------------------------------------------------------------
%%
%% Copyright (c) 2018-2023 Workday, Inc.
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
%% Test fullsync when the node in the sink cluster that the source
%% coordinator is connected to goes down.
%%
%% This test uses an intercept to crash the source server, closing
%% the coordinator socket when a fullsync "whereis" message is received.
%%
%% The coordinator should be able to cope with this by connecting
%% to another node in the sink cluster.
-module(repl_fs_coord_conn_down).
-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").
-define(TEST_BUCKET, <<"repl_fs_stat_caching">>).
-compile({parse_transform, rt_intercept_pt}).
-export([confirm/0]).

coord_status(Node) ->
    StatusResult = rpc:block_call(
        Node, riak_repl2_fscoordinator, status, []),
    % lager:info("COORD STATUS ~p ~p", [Node, StatusResult]),
    StatusResult.

coord_sink_connected_node(SourceNode, SinkCluster) ->
    [{"sink", StatusProps}] = coord_status(SourceNode),
    SocketProps = proplists:get_value(socket, StatusProps),
    Peername = proplists:get_value(peername, SocketProps),
    NodeIndex = list_to_integer(string:substr(Peername, length(Peername)-1, 1)),
    lists:nth(NodeIndex-3, SinkCluster).

fullsyncs_completed_status(Node) ->
    [{_, StatusProps}] = coord_status(Node),
    proplists:get_value(fullsyncs_completed, StatusProps).

retry_fun(Retries, _, _) when Retries =< 0 ->
    error("Retries cannot be zero or negative");
retry_fun(1, _, Fun) ->
    %% do not catch on the last attempt to return the error
    Fun();
retry_fun(Retries, Delay, Fun) ->
    try
        Fun()
    catch
        _:_ ->
            timer:sleep(Delay),
            retry_fun(Retries-1, Delay, Fun)
    end.

confirm() ->
    {{SrcLeader, _SrcCluster}, {SinkLeader, _SinkCluster}} = setup(),

    repl_util:enable_fullsync(SrcLeader, "sink"),

    SinkPort = repl_util:get_cluster_mgr_port(SinkLeader),
    repl_util:connect_cluster(SrcLeader, "127.0.0.1", SinkPort),

    start_fullsync(SrcLeader),
    rt:wait_until(
        fun() ->
            lager:info("Waiting for fullsync to complete using source leader ~p", [SrcLeader]),
            fullsyncs_completed_status(SrcLeader) > 0
        end
    ),
    SinkReplNode = retry_fun(50, 500, fun() -> coord_sink_connected_node(SrcLeader, _SinkCluster) end),
    lager:info("Coordinator connected to sink node ~w",[SinkReplNode]),
    lager:info("Source cluster leader is ~w",[SrcLeader]),

    put_objects(SrcLeader),

    Intercept = {{handle_protocol_msg,2}, handle_protocol_msg_error},
    [rt_intercept:add(SinkNode,
        {riak_repl2_fscoordinator_serv, [Intercept]}) || SinkNode <- [SinkReplNode]],

    start_fullsync(SrcLeader),

    retry_fun(50, 500, fun() -> 2 = fullsyncs_completed_status(SrcLeader) end),
    get_objects(SinkLeader),
    pass.

put_objects(Node1) ->
    Client = rt:pbc(Node1),
    V = <<"value">>,
    [put(Client, integer_to_binary(N), V) || N <- lists:seq(1,200)],
    ok.

get_objects(Node1) ->
    Client = rt:pbc(Node1),
    Bucket = <<"bucket">>,
    GetFn =
        fun(N) ->
            riakc_pb_socket:get(Client, Bucket, integer_to_binary(N))
        end,
    Result = [to_char_result(GetFn(N)) || N <- lists:seq(1,200)],
    lager:info(Result,[]),
    ?assertNot(lists:member($x, Result)).

to_char_result({ok,_}) -> $.;
to_char_result({error,notfound}) -> $x.

put(Client, K, V) ->
    Obj = riakc_obj:new(<<"bucket">>, K, V),
    riakc_pb_socket:put(Client, Obj).

start_fullsync(Node) ->
    ClusterName = "sink",
    rpc:call(Node, riak_repl_console, fullsync, [["start", ClusterName]]).

setup() ->
    rt:set_conf(all, [{"buckets.default.allow_mult", "false"}]),
    NodeCount = rt_config:get(num_nodes, 6),

    lager:info("Deploy ~p nodes", [NodeCount]),
    Nodes = rt:deploy_nodes(NodeCount, cluster_conf(), [riak_kv, riak_repl]),
    SplitSize = NodeCount div 2,
    {SourceNodes, SinkNodes} = lists:split(SplitSize, Nodes),

    lager:info("making cluster Source from ~p", [SourceNodes]),
    repl_util:make_cluster(SourceNodes),

    lager:info("making cluster Sink from ~p", [SinkNodes]),
    repl_util:make_cluster(SinkNodes),

    SrcHead = hd(SourceNodes),
    SinkHead = hd(SinkNodes),
    repl_util:name_cluster(SrcHead, "source"),
    repl_util:name_cluster(SinkHead, "sink"),

    rt:wait_until_ring_converged(SourceNodes),
    rt:wait_until_ring_converged(SinkNodes),

    rt:wait_until_transfers_complete(SourceNodes),
    rt:wait_until_transfers_complete(SinkNodes),

    ok = repl_util:wait_until_leader_converge(SourceNodes),
    ok = repl_util:wait_until_leader_converge(SinkNodes),

    SourceLeader = repl_util:get_leader(SrcHead),
    SinkLeader = repl_util:get_leader(SinkHead),

    {{SourceLeader, SourceNodes}, {SinkLeader, SinkNodes}}.

cluster_conf() ->
    [
        {riak_repl, [
            {fullsync_on_connect, false},
            {fullsync_interval, disabled},
            {max_fssource_cluster, 3},
            {max_fssource_node, 1},
            {max_fssink_node, 20},
            {rtq_max_bytes, 1048576}
        ]},
        %% Turbo mode
        {riak_core, [
            {ring_creation_size, 8},
            {vnode_inactivity_timeout, 1000},
            {vnode_management_timer, 100},
            {handoff_concurrency, 8}
        ]}
    ].
