%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2014 Basho Technologies, Inc.
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
-module(verify_handoff).
-behavior(riak_test).

-export([confirm/0, run_test/4]).

-include_lib("stdlib/include/assert.hrl").

%%
%% TODO Portions of this test are commented out pending removal of elevedb backend stats, which
%% require calling the vnode to acquire status.  This call appears to interfere with convergence
%% of handoff when stats are requested in this test.
%%

%% We've got a separate test for capability negotiation and other mechanisms, so the test here is fairly
%% straightforward: get a list of different versions of nodes and join them into a cluster, making sure that
%% each time our data has been replicated:
confirm() ->
    NTestItems    = 1000,                                   %% How many test items to write/verify?
    NTestNodes    = 3,                                      %% How many nodes to spin up for tests?
    TestMode      = true,                                   %% Set to false for "production tests", true if too slow.

    case rt:get_backends() of
        bitcask ->
            lager:info("Verified bitcask backend for this test"),
            run_test(TestMode, NTestItems, NTestNodes, default),
            lager:info("Test verify_handoff passed."),
            pass;
        SomethingElse ->
            lager:error("Unexpected backends: ~p", [SomethingElse]),
            fail
    end.

run_test(TestMode, NTestItems, NTestNodes, Encoding) ->
    lager:info("Testing handoff (items ~p, encoding: ~p)", [NTestItems, Encoding]),

    lager:info("Spinning up test nodes"),
    [RootNode | TestNodes] = Nodes = deploy_test_nodes(TestMode, NTestNodes),

    rt:wait_for_service(RootNode, riak_kv),

    set_handoff_encoding(Encoding, Nodes),

    %% Insert delay into handoff folding to test the efficacy of the
    %% handoff heartbeat addition
    [rt_intercept:add(N, {riak_core_handoff_sender,
                          [{{visit_item, 3}, delayed_visit_item_3}]})
     || N <- Nodes],

    % rt_kv_worker_proc:add_intercept(Nodes),

    lager:info("Populating root node."),
    rt:systest_write(RootNode, NTestItems),
    %% write one object with a bucket type
    rt:create_and_activate_bucket_type(RootNode, <<"type">>, []),
    %% allow cluster metadata some time to propogate
    rt:systest_write(RootNode, 1, 2, {<<"type">>, <<"bucket">>}, 2),

    %% Test ownership handoff on each node:
    lager:info("Testing ownership handoff for cluster."),
    lists:foreach(fun(TestNode) -> test_ownership_handoff(RootNode, TestNode, NTestItems) end, TestNodes),

    %% Test hinted handoff by stopping the root node and forcing some handoff
    test_hinted_handoff(Nodes, NTestItems),

    %% Test repair handoff by stopping the root node, deleting data, and running a repair
    test_repair_handoff(Nodes),

    %% Prepare for the next call to our test (we aren't polite about it, it's faster that way):
    lager:info("Bringing down test nodes."),
    lists:foreach(fun(N) -> rt:brutal_kill(N) end, TestNodes),

    %% The "root" node can't leave() since it's the only node left:
    lager:info("Stopping root node."),
    rt:brutal_kill(RootNode).

set_handoff_encoding(default, _) ->
    lager:info("Using default encoding type."),
    true;
set_handoff_encoding(Encoding, Nodes) ->
    lager:info("Forcing encoding type to ~p.", [Encoding]),

    %% Update all nodes (capabilities are not re-negotiated):
    [begin
         rt:update_app_config(Node, override_data(Encoding)),
         assert_using(Node, {riak_kv, handoff_data_encoding}, Encoding)
     end || Node <- Nodes].

override_data(Encoding) ->
    [
     { riak_core,
       [
        { override_capability,
          [
           { handoff_data_encoding,
             [
              {    use, Encoding},
              { prefer, Encoding}
             ]
           }
          ]
        }
       ]}].

%% See if we get the same data back from our new nodes as we put into the root node:
test_ownership_handoff(RootNode, NewNode, NTestItems) ->

    lager:info("Testing ownership handoff adding node ~p ...", [NewNode]),

    lager:info("Waiting for service on new node."),
    rt:wait_for_service(NewNode, riak_kv),

    % BeforeHandoffStats = get_handoff_stats(ownership, [RootNode]),

    % {ok, _Pid} = rt_kv_worker_proc:start_link([
    %     {started_callback,
    %         fun(NodePid, Accum) ->
    %             work_started(NodePid, Accum, ownership)
    %         end
    %     },
    %     {completed_callback,
    %         fun(NodePid, Accum) ->
    %             work_completed(NodePid, Accum, ownership)
    %         end
    %     },
    %     {accum, []}
    % ]),
    % lager:info("rt_kv_worker_proc started"),

    lager:info("Joining new node ~p with cluster.", [NewNode]),
    rt:join(NewNode, RootNode),
    ?assertEqual(ok, rt:wait_until_nodes_ready([RootNode, NewNode])),
    rt:wait_until_no_pending_changes([RootNode, NewNode]),

    % {ok, []} = rt_kv_worker_proc:stop(),

    % AfterHandoffStats = get_handoff_stats(ownership, [RootNode]),
    % check_all_node_stats_increased(BeforeHandoffStats, AfterHandoffStats, ownership, bytes_sent),
    % check_all_node_stats_increased(BeforeHandoffStats, AfterHandoffStats, ownership, objects_sent),

    %% See if we get the same data back from the joined node that we added to the root node.
    %%  Note: systest_read() returns /non-matching/ items, so getting nothing back is good:
    lager:info("Validating data after handoff:"),
    Results = rt:systest_read(NewNode, NTestItems),
    ?assertEqual(0, length(Results)),
    Results2 = rt:systest_read(RootNode, 1, 2, {<<"type">>, <<"bucket">>}, 2),
    ?assertEqual(0, length(Results2)),
    lager:info("Data looks ok.").

test_hinted_handoff([RootNode | OtherNodes] = Nodes, NTestItems) ->

    lager:info("Testing hinting handoff by stopping ~p, doing some writes, and restarting ...", [RootNode]),

    % BeforeHandoffStats = get_handoff_stats(hinted, OtherNodes),

    % {ok, _Pid} = rt_kv_worker_proc:start_link([
    %     {started_callback,
    %         fun(NodePid, Accum) ->
    %             work_started(NodePid, Accum, hinted)
    %         end
    %     },
    %     {completed_callback,
    %         fun(NodePid, Accum) ->
    %             work_completed(NodePid, Accum, hinted)
    %         end
    %     },
    %     {accum, []}
    % ]),
    rt:stop(RootNode),

    [AnotherNode | _Rest] = OtherNodes,
    lager:info("Writing ~p objects to ~p ...", [NTestItems, AnotherNode]),
    rt:systest_write(AnotherNode, NTestItems),
    Results = rt:systest_read(AnotherNode, NTestItems),
    ?assertEqual(0, length(Results)),

    rt:start(RootNode),
    rt:wait_for_service(RootNode, riak_kv),
    rt:wait_until_no_pending_changes(Nodes),
    rt:wait_until_transfers_complete(Nodes),

    % case rt_kv_worker_proc:stop() of
    %     {ok, []} ->
    %         ok;
    %     %% the root node may trigger a repair on its way down that is never completed
    %     {ok, [{RootNode, 1}]} ->
    %         ok;
    %     AnythingElse ->
    %         lager:error("Unexepcted state in worker proc after stop: ~p", [AnythingElse]),
    %         throw(AnythingElse)
    % end,

    % AfterHandoffStats = get_handoff_stats(hinted, OtherNodes),
    % check_all_node_stats_increased(BeforeHandoffStats, AfterHandoffStats, hinted, bytes_sent),
    % check_all_node_stats_increased(BeforeHandoffStats, AfterHandoffStats, hinted, objects_sent),

    ok.

test_repair_handoff([RootNode | _OtherNodes] = Nodes) ->

    lager:info("Testing repair handoff by stopping ~p, deleting some data, and issuing a repair ...", [RootNode]),

    % BeforeHandoffStats = get_handoff_stats(repair, OtherNodes),

    [Partition | _] = rt:partitions_for_node(RootNode),

    % {ok, _Pid} = rt_kv_worker_proc:start_link([
    %     {started_callback,
    %         fun(NodePid, Accum) ->
    %             work_started(NodePid, Accum, repair)
    %         end
    %     },
    %     {completed_callback,
    %         fun(NodePid, Accum) ->
    %             work_completed(NodePid, Accum, repair)
    %         end
    %     },
    %     {accum, []}
    % ]),
    rt:stop(RootNode),

    delete_bitcask_dir(RootNode, Partition),

    rt:start(RootNode),
    rt:wait_for_service(RootNode, riak_kv),
    rt:wait_until_no_pending_changes(Nodes),
    rt:wait_until_transfers_complete(Nodes),

    issue_repair(RootNode, Partition),
    rt:wait_until_transfers_complete(Nodes),

    % {ok, []} = rt_kv_worker_proc:stop(),

    % AfterHandoffStats = get_handoff_stats(hinted, OtherNodes),
    % check_all_node_stats_increased(BeforeHandoffStats, AfterHandoffStats, repair, bytes_sent),
    % check_all_node_stats_increased(BeforeHandoffStats, AfterHandoffStats, repair, objects_sent),

    ok.

delete_bitcask_dir(Node, Partition) ->
    rt:clean_data_dir(Node, lists:flatten(io_lib:format("bitcask/~p/", [Partition]))).

issue_repair(Node, Partition) ->
    rpc:call(Node, riak_kv_vnode, repair, Partition).

% get_handoff_stats(Type, Node) when is_atom(Node) ->
%     Stats = rt:get_stats(Node),
%     BytesSentStatName   = stat_name(Type, bytes_sent),
%     ObjectsSentStatName = stat_name(Type, objects_sent),
%     [
%         {BytesSentStatName,   rt:get_stat(Stats, BytesSentStatName)},
%         {ObjectsSentStatName, rt:get_stat(Stats, ObjectsSentStatName)}
%     ];
% get_handoff_stats(Type, Nodes) ->
%     [{Node, get_handoff_stats(Type, Node)} || Node <- Nodes].


% stat_name(Type, Moniker) ->
%     list_to_atom(atom_to_list(Type) ++ "_handoff_" ++ atom_to_list(Moniker)).

% check_all_node_stats_increased(BeforeHandoffStats, AfterHandoffStats, Type, Stat) ->
%     StatName = stat_name(Type, Stat),
%     lager:info("Verifying ~p stat always increased...", [StatName]),
%     ?assert(lists:all(
%         fun({{_Node, BeforeStats}, {_Node, AfterStats}}) ->
%             proplists:get_value(StatName, BeforeStats) <  proplists:get_value(StatName, AfterStats)
%         end,
%         lists:zip(BeforeHandoffStats, AfterHandoffStats)
%     )).

assert_using(Node, {CapabilityCategory, CapabilityName}, ExpectedCapabilityName) ->
    lager:info("assert_using ~p =:= ~p", [ExpectedCapabilityName, CapabilityName]),
    ExpectedCapabilityName =:= rt:capability(Node, {CapabilityCategory, CapabilityName}).

%% For some testing purposes, making these limits smaller is helpful:
deploy_test_nodes(false, N) ->
    Config = [{riak_core, [{ring_creation_size, 8},
                           {vnode_inactivity_timeout, 5000},
                           {handoff_acksync_threshold, 20},
                           {handoff_receive_timeout, 2000}]}],
    rt:deploy_nodes(N, Config);
deploy_test_nodes(true,  N) ->
    lager:info("WARNING: Using turbo settings for testing."),
    Config = [{riak_core, [{forced_ownership_handoff, 8},
                           {ring_creation_size, 8},
                           {handoff_concurrency, 8},
                           {vnode_inactivity_timeout, 1000},
                           {handoff_acksync_threshold, 20},
                           {handoff_receive_timeout, 2000},
                           {vnode_management_timer, 100},
                           {gossip_limit, {10000000, 60000}}]}],
    rt:deploy_nodes(N, Config).

% work_started({Node, Pid}, Accum, Type) ->
%     lager:info("Started ~p handoff on node ~p pid ~p (Accum=~p)", [Type, Node, Pid, Accum]),
%     increment_counter(Accum, Node).

% work_completed({Node, Pid}, Accum, Type) ->
%     lager:info("Completed ~p handoff on node ~p pid ~p (Accum=~p)", [Type, Node, Pid, Accum]),
%     NumWorkersOnNode = get_counter(Accum, Node),
%     StatName = stat_name(Type, outbound_active_transfers),
%     wait_until_stat_at_least(Node, StatName, NumWorkersOnNode),
%     {ok, decrement_counter(Accum, Node)}.


% increment_counter(Accum, Node) ->
%     case proplists:get_value(Node, Accum) of
%         undefined ->
%             [{Node, 1} | Accum];
%         Value ->
%             [{Node, Value + 1} | proplists:delete(Node, Accum)]
%     end.


% get_counter(Accum, Node) ->
%     case proplists:get_value(Node, Accum) of
%         undefined ->
%             {error, {not_found, Node, Accum}};
%         Value ->
%             Value
%     end.


% decrement_counter(Accum, Node) ->
%     case proplists:get_value(Node, Accum) of
%         undefined ->
%             {error, {not_found, Node, Accum}};
%         1 ->
%             proplists:delete(Node, Accum);
%         Value ->
%             [{Node, Value - 1} | proplists:delete(Node, Accum)]
%     end.


% wait_until_stat_at_least(Node, StatName, ExpectedValue) ->
%     rt:wait_until(
%         fun() ->
%             Stat = rt:get_stat(Node, StatName),
%             case Stat of
%                 Value when ExpectedValue =< Value ->
%                     true;
%                 _ ->
%                     lager:info("Stat ~p: ~p Expecting at least: ~p", [StatName, Stat, ExpectedValue]),
%                     false
%             end
%         end
%     ).
