%% -------------------------------------------------------------------
%%
%% Copyright (c) 2018 Russell Brown.
%% Copyright (c) 2022-2023 Workday, Inc.
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
%%% @doc
%%% riak_test for capability guard against using put fsm use of
%%% "soft-limits" via riak_core_vnode_proxy message queues in a mixed
%%% cluster
%%% @end
-module(verify_vnode_polling_caps).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(BUCKET, <<"test-bucket">>).
-define(KEY, <<"key">>).
-define(VALUE, <<"value">>).

%% First Riak version with soft limits.
-define(MIN_SL_VSN, [2,9,0]).

confirm() ->
    %% Create a mixed cluster of current and previous
    %% and do some puts, checking stats to show that no "soft-limit"
    %% stats have changed
    %% Upgrade nodes to current
    %% Do some PUTs and check that stats have changed to indicate
    %% soft-limit use

    MinSLVsn = rt_vsn:new_version(?MIN_SL_VSN),
    [{_HdTag, HdVsn} | _] = CfgVersions = rt_vsn:configured_versions(),
    %% Confirm that we have a version new enough for the test
    ?assert(rt_vsn:compare_versions(HdVsn, MinSLVsn) >= 0),
    %% NewTag == 'current' if 'current' meets the criteria
    {NewTag, NewVsn} = rt_vsn:find_version_at_least(MinSLVsn, CfgVersions),
    NewStr = rt_vsn:version_to_string(NewVsn),

    %% We'll only run the mixed cluster tests if we found an old enough
    %% version. At the end of this case, return Nodes as a suitable cluster
    %% for the next applicable test phase.
    {OldEnough, Nodes} = case
        rt_vsn:find_version_before(MinSLVsn, CfgVersions) of
        {OldTag, OldVsn} ->
            OldStr = rt_vsn:version_to_string(OldVsn),
            ?LOG_INFO(
                "Version ~s (~s) found not supporting soft limits",
                [OldStr, OldTag]),
            ?LOG_INFO(
                "Building mixed cluster of version ~s (~s) and ~s (~s) nodes",
                [OldStr, OldTag, NewStr, NewTag]),
            {true, rt:build_cluster([OldTag, OldTag, NewTag, NewTag])};
        _ ->
            ?LOG_INFO("No version found not supporting soft limits"),
            ?LOG_INFO(
                "Building cluster of version ~s (~s) nodes",
                [NewStr, NewTag]),
            {false, rt:build_cluster(lists:duplicate(4, NewTag))}
    end,

    [Node1, Node2 | _] = Nodes,

    Preflist = rt:get_preflist(Node1, ?BUCKET, ?KEY),
    ?LOG_INFO("Preflist ~0p", [Preflist]),

    %% If we found an old enough version, the first two nodes of the cluster
    %% don't support soft limits so we can run the mixed cluster test.
    %% Either way, at the end of this 'if' Nodes is a fully soft-limit-enabled
    %% 4-node cluster suitable for the following test.
    ExpectedStatAcc = if
        OldEnough ->
            ExpectedStatAcc1 = lists:foldl(
                fun(Node, Acc) ->
                    test_no_mbox_check(Nodes, Preflist, Node, Acc)
                end,
                new_expected_stat_acc(),
                Nodes),
            ?LOG_INFO("upgrade all to ~s (~s)", [NewStr, NewTag]),
            ok = rt:upgrade(Node1, NewTag),
            ok = rt:upgrade(Node2, NewTag),
            %% upgrade restarts, and restarts clear stats
            clear_stats(Node2, clear_stats(Node1, ExpectedStatAcc1));
        true ->
            new_expected_stat_acc()
    end,

    [?assertMatch(ok,
        rt:wait_until_capability(Node, {riak_kv, put_soft_limit}, true))
        || Node <- Nodes],

    lists:foldl(
        fun(Node, Acc) ->
            test_mbox_check(Nodes, Preflist, Node, Acc)
        end,
        ExpectedStatAcc,
        Nodes),

    pass.

%% @doc in a mixed cluster state, there should be no soft-limits
test_no_mbox_check(Nodes, Preflist, TargetNode, ExpectedStatAcc0) ->
    ?LOG_INFO("test_no_mbox_check ~0p", [TargetNode]),

    {ok, Client} = riak:client_connect(TargetNode),
    ?assertMatch(ok, client_write(Client, ?BUCKET, ?KEY, ?VALUE)),

    Stats = get_all_nodes_stats(Nodes),
    TargetNodeStats = proplists:get_value(TargetNode, Stats),

    case node_on_preflist(TargetNode, Preflist) of
        false ->
            %% should be a normal good old fashioned coord_redirect stat bump
            {ExpectedCoodRedirCnt, NewAcc} = increment_expected({TargetNode, coord_redirs_total}, ExpectedStatAcc0),
            CoordRedirCnt = proplists:get_value(coord_redirs_total, TargetNodeStats),
            ?assertEqual(ExpectedCoodRedirCnt, CoordRedirCnt),
            %% if undefined then zero
            ExpectedCoordMboxRedirCnt = get_expected({TargetNode, coord_redir_unloaded_total}, NewAcc),
            CoordMboxRedirCnt = proplists:get_value(coord_redir_unloaded_total, TargetNodeStats, 0),
            ?assertEqual(ExpectedCoordMboxRedirCnt, CoordMboxRedirCnt),
            NewAcc;
        true ->
            %% no stat for local coord put to check
            ExpectedCoordRedirCnt= get_expected({TargetNode, coord_redirs_total}, ExpectedStatAcc0),
            CoordRedirCnt = proplists:get_value(coord_redirs_total, TargetNodeStats),
            ?assertEqual(ExpectedCoordRedirCnt, CoordRedirCnt),
            ExpectedLocalCoordCnt = get_expected({TargetNode, coord_local_unloaded_total}, ExpectedStatAcc0),
            LocalCoordCnt = proplists:get_value(coord_local_unloaded_total, TargetNodeStats, 0),
            ?assertEqual(ExpectedLocalCoordCnt, LocalCoordCnt),
            ExpectedStatAcc0
    end.

%% @doc when all nodes are upgraded they should agree on the
%% capability, and soft-limits should be used
test_mbox_check(Nodes, Preflist, TargetNode, ExpectedStatAcc0) ->
    ?LOG_INFO("test_mbox_check ~0p", [TargetNode]),

    {ok, Client} = riak:client_connect(TargetNode),
    ?assertMatch(ok, client_write(Client, ?BUCKET, ?KEY, ?VALUE)),

    Stats = get_all_nodes_stats(Nodes),
    TargetNodeStats = proplists:get_value(TargetNode, Stats),

    case node_on_preflist(TargetNode, Preflist) of
        false ->
            {ExpectedCoodMboxRedirCnt, NewAcc} = increment_expected({TargetNode, coord_redir_unloaded_total}, ExpectedStatAcc0),
            CoordMboxRedirCnt = proplists:get_value(coord_redir_unloaded_total, TargetNodeStats),
            ?assertEqual(ExpectedCoodMboxRedirCnt, CoordMboxRedirCnt),
            %% i.e. unchanged from above (therefore different code path!)
            ExpectedCoordRedirCnt= get_expected({TargetNode, coord_redirs_total}, ExpectedStatAcc0),
            CoordRedirCnt = proplists:get_value(coord_redirs_total, TargetNodeStats),
            ?assertEqual(ExpectedCoordRedirCnt, CoordRedirCnt),
            NewAcc;
        true ->
            %% i.e. stat MUST exist
            {ExpectedLocalCoordCnt, NewAcc} = increment_expected({TargetNode, coord_local_unloaded_total}, ExpectedStatAcc0),
            LocalCoordCnt = proplists:get_value(coord_local_unloaded_total, TargetNodeStats),
            ?assertEqual(ExpectedLocalCoordCnt, LocalCoordCnt),
            NewAcc
    end.

client_write(Client, Bucket, Key, Value) ->
    client_write(Client, Bucket, Key, Value, []).

client_write(Client, Bucket, Key, Value, Opts) ->
    Obj = riak_object:new(Bucket, Key, Value),
    riak_client:put(Obj, Opts, Client).

get_all_nodes_stats(Nodes) ->
    [{Nd, rpc:call(Nd, riak_kv_stat, get_stats, [])} || Nd <- Nodes].

node_on_preflist(Node, Preflist) ->
    [PLNode || {{_Idx, PLNode}, _Type} <- Preflist,
               PLNode == Node] == [Node].

new_expected_stat_acc() ->
    orddict:new().

increment_expected(StatName, Acc) ->
    Acc2 = orddict:update_counter(StatName, 1, Acc),
    {orddict:fetch(StatName, Acc2), Acc2}.

get_expected(StatName, Acc) ->
    case orddict:find(StatName, Acc) of
        error ->
            0;
        {ok, Cnt} ->
            Cnt
    end.

%% @private stats are lost when a node restarts, and a node restarts
%% when it is upgraded, so clear the expected stats for `Node'
clear_stats(Node, StatAcc) ->
    orddict:filter(
        fun({N, _Stat}, _Val) ->
            Node /= N
        end,
        StatAcc).
