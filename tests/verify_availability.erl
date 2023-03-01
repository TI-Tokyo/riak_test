%% -------------------------------------------------------------------
%%
%% Copyright (c) 2019-2023 Workday, Inc.
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
-module(verify_availability).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(CONFIG, [
    %% Turbo mode
    {riak_core, [
        {ring_creation_size, 8},
        {vnode_inactivity_timeout, 1000},
        {vnode_management_timer, 100},
        {handoff_concurrency, 8}
    ]}
]).

confirm() ->
    %%
    %% Build a 5-node cluster
    %%
    [Node1, Node2, Node3, Node4, Node5] = Nodes = rt:deploy_nodes(5),
    rt:join_cluster(Nodes),
    %%
    %% We should be available for reads for r=1, r=2, r=3
    %%
    [check_is_available(Node, 3, 1) || Node <- Nodes],
    [check_is_available(Node, 3, 2) || Node <- Nodes],
    [check_is_available(Node, 3, 3) || Node <- Nodes],
    [check_empty_uncovered_preflists_stat(Node) || Node <- Nodes],
    %%`
    %% Stop node1.  We should be available for reads for r=1 and r=2, but not r=3
    %%
    stop_node(Node1, [Node2, Node3, Node4, Node5]),
    [check_is_available(Node, 3, 1) || Node <- [Node2, Node3, Node4, Node5]],
    [check_is_available(Node, 3, 2) || Node <- [Node2, Node3, Node4, Node5]],
    [check_is_not_available(Node, 3, 3) || Node <- [Node2, Node3, Node4, Node5]],
    [check_empty_uncovered_preflists_stat(Node) || Node <- [Node2, Node3, Node4, Node5]],
    %%
    %% Stop node3.  We should only be available for reads for r=1
    %%
    stop_node(Node3, [Node2, Node4, Node5]),
    [check_is_available(Node, 3, 1) || Node <- [Node2, Node4, Node5]],
    [check_is_not_available(Node, 3, 2) || Node <- [Node2, Node4, Node5]],
    [check_is_not_available(Node, 3, 3) || Node <- [Node2, Node4, Node5]],
    [check_empty_uncovered_preflists_stat(Node) || Node <- [Node2, Node4, Node5]],
    %%
    %% Stop node5.  We should _still_ only be available for reads for r=1
    %%
    stop_node(Node5, [Node2, Node4]),
    [check_is_available(Node, 3, 1) || Node <- [Node2, Node4]],
    [check_is_not_available(Node, 3, 2) || Node <- [Node2, Node4]],
    [check_is_not_available(Node, 3, 3) || Node <- [Node2, Node4]],
    [check_empty_uncovered_preflists_stat(Node) || Node <- [Node2, Node4]],
    %%
    %% Stop node5.  We should be unavailable for all r values <= 3.
    %%
    stop_node(Node4, [Node2]),
    [check_is_not_available(Node, 3, 1) || Node <- [Node2]],
    [check_is_not_available(Node, 3, 2) || Node <- [Node2]],
    [check_is_not_available(Node, 3, 3) || Node <- [Node2]],
    [check_nonempty_uncovered_preflists_stat(Node) || Node <- [Node2]],
    %%
    %% Restart all the nodes we stopped
    %%
    start_node(Node1, [Node1, Node2]),
    start_node(Node3, [Node1, Node2, Node3]),
    start_node(Node3, [Node1, Node2, Node3]),
    start_node(Node4, [Node1, Node2, Node3, Node4]),
    start_node(Node5, [Node1, Node2, Node3, Node4, Node5]),
    %%
    %% We should be available for reads for r=1, r=2, r=3
    %%
    [check_is_available(Node, 3, 1) || Node <- Nodes],
    [check_is_available(Node, 3, 2) || Node <- Nodes],
    [check_is_available(Node, 3, 3) || Node <- Nodes],
    [check_empty_uncovered_preflists_stat(Node) || Node <- Nodes],
    %%
    %% dun
    %%
    pass.

stop_node(Node, ExpectedNodes) ->
    rt:stop(Node),
    [wait_until_node_watcher_converges(N, ExpectedNodes) || N <- ExpectedNodes].

start_node(Node, ExpectedNodes) ->
    rt:start(Node),
    [wait_until_node_watcher_converges(N, ExpectedNodes) || N <- ExpectedNodes].

check_is_available(Node, NVal, Min) ->
    F = fun() ->
        lager:info("Checking ~p for availability with NVal ~p and Min ~p", [Node, NVal, Min]),
        case get_uncovered_preflists(Node, NVal, Min) of
            [] ->
                true;
            _ ->
                false
        end
    end,
    rt:wait_until(F).

check_is_not_available(Node, NVal, Min) ->
    F = fun() ->
        lager:info("Checking ~p for non-availability with NVal ~p and Min ~p", [Node, NVal, Min]),
        case get_uncovered_preflists(Node, NVal, Min) of
            L when is_list(L) andalso erlang:length(L) > 0 ->
                true;
            _ ->
                false
        end
    end,
    rt:wait_until(F).

check_empty_uncovered_preflists_stat(Node) ->
    F = fun() ->
        lager:info("Checking ~p for empty uncovered preflists stat", [Node]),
        case get_uncovered_preflists_stat(Node) of
            [] ->
                true;
            Other ->
                lager:warning("Expected empty uncovered preflists on node ~p, but got ~p", [Node, Other]),
                false
        end
    end,
    rt:wait_until(F).

check_nonempty_uncovered_preflists_stat(Node) ->
    F = fun() ->
        lager:info("Checking ~p for non-empty uncovered preflists stat", [Node]),
        case get_uncovered_preflists_stat(Node) of
            L when is_list(L) andalso erlang:length(L) > 0 ->
                true;
            Other ->
                lager:warning("Expected non-empty uncovered preflists on node ~p, but got ~p", [Node, Other]),
                false
        end
    end,
    rt:wait_until(F).

get_uncovered_preflists(Node, NVal, Min) ->
    UpNodes = riak_core_util:safe_rpc(Node, riak_core_node_watcher, nodes, [riak_kv]),
    riak_core_util:safe_rpc(Node, riak_core_ring_util, uncovered_preflists, [UpNodes, NVal, Min]).

get_uncovered_preflists_stat(Node) ->
    Stats = rt:get_stats(Node),
    proplists:get_value(<<"uncovered_preflists">>, Stats).

wait_until_node_watcher_converges(Node, ExpectedNodes) ->
    ExpectedSet = sets:from_list(ExpectedNodes),
    rt:wait_until(
        fun() ->
            lager:info("Waiting for ~p to have expected up nodes: ~p", [Node, ExpectedNodes]),
            UpNodes = riak_core_util:safe_rpc(Node, riak_core_node_watcher, nodes, [riak_kv]),
            UpSet = sets:from_list(UpNodes),
            equal_sets(UpSet, ExpectedSet)
        end
    ).

equal_sets(A, B) ->
    sets:is_subset(A, B) andalso sets:is_subset(B, A).
