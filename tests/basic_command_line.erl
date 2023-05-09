%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012-2016 Basho Technologies, Inc.
%% Copyright (c) 2023 Workday, Inc.
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
-module(basic_command_line).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("stdlib/include/assert.hrl").

confirm() ->

    %% Deploy a node to test against
    lager:info("Deploy node to test command line"),
    [Node] = rt:deploy_nodes(1),
    ?assertEqual(ok, rt:wait_until_nodes_ready([Node])),

    %% Verify node-up behavior
    ping_up_test(Node),
    attach_direct_up_test(Node),
    status_up_test(Node),
    console_up_test(Node),
    start_up_test(Node),
    getpid_up_test(Node),

    %% Stop the node, Verify node-down behavior
    stop_test(Node),
    ping_down_test(Node),
    attach_down_test(Node),
    status_down_test(Node),
    console_test(Node),
    start_test(Node),
    getpid_down_test(Node),

    pass.

console_up_test(Node) ->
    lager:info("Node is already up, `riak console` should fail"),
    {ok, ConsoleFail} = rt:riak(Node, ["console"]),
    ?assert(rt:str(ConsoleFail, "Node is already running!")),
    ok.

console_test(Node) ->
    %% Make sure the cluster will start up with /usr/sbin/riak console, then quit
    lager:info("Testing riak console on ~s", [Node]),

    %% Start and stop node, to test console working
    Ops = [
        {expect, "(abort with ^G)", 20000}, % give it some time to start
        {send, "riak_core_ring_manager:get_my_ring().\n"},
        {expect, "dict,", 10000},
        {send, "q().\n"},
        {expect, "ok"}
    ],
    ?assertMatch({ok, 0}, rt:interact(Node, "console", Ops)),
    rt:wait_until_unpingable(Node).

start_up_test(Node) ->
    %% Try starting again and check you get the node is already running message
    lager:info("Testing riak start now will return 'already running'"),
    {ok, StartOut} = rt:riak(Node, ["start"]),
    ?assert(rt:str(StartOut, "Node is already running!")),
    ok.


start_test(Node) ->
    %% Test starting with /bin/riak start
    lager:info("Testing riak start works on ~s", [Node]),

    {ok, Output} = rt:riak(Node, ["start"]),
    StartPass = string:trim(Output),
    lager:info("StartPass: ~p", [StartPass]),
    ?assert(StartPass =:= "" orelse string:str(StartPass, "WARNING") =/= 0),
    rt:stop_and_wait(Node),
    ok.

stop_test(Node) ->
    ?assert(rt:is_pingable(Node)),

    {ok, "ok\n"} = rt:riak(Node, ["stop"]),

    ?assertNot(rt:is_pingable(Node)),
    ok.

ping_up_test(Node) ->

    %% check /usr/sbin/riak ping
    lager:info("Testing riak ping on ~s", [Node]),

    %% ping / pong
    %% rt:start_and_wait(Node),
    lager:info("Node up, should ping"),
    {ok, PongOut} = rt:riak(Node, ["ping"]),
    ?assert(rt:str(PongOut, "pong")),
    ok.

ping_down_test(Node) ->
    %% ping / pang
    lager:info("Node down, should pang"),
    {ok, PangOut} = rt:riak(Node, ["ping"]),
    ?assert(rt:str(PangOut, "not responding to pings")),
    ok.

attach_down_test(Node) ->
    lager:info("Testing riak 3+ 'attach' while down"),
    {ok, AttachOut} = rt:riak(Node, ["attach"]),
    ?assert(rt:str(AttachOut, "Node is not running!")).

attach_direct_up_test(Node) ->
    lager:info("Testing riak 3+ 'attach'"),
    Ops = [
        {expect, "(^D to exit)"},
        {send, "riak_core_ring_manager:get_my_ring().\n"},
        {expect, "dict,", 10000},
        {send, [4]}     %% 4 = Ctrl + D
    ],
    ?assertMatch({ok, 0}, rt:interact(Node, "attach", Ops)).

status_up_test(Node) ->
    lager:info("Test riak admin status on ~s", [Node]),
    {ok, {ExitCode, StatusOut}} = rt:admin(Node, ["status"], [return_exit_code]),
    lager:info("Result of status: ~s", [StatusOut]),
    ?assertEqual(0, ExitCode),
    ?assert(rt:str(StatusOut, "1-minute stats")),
    ?assert(rt:str(StatusOut, "kernel_version")),
    ok.

status_down_test(Node) ->
    lager:info("Test riak admin status on ~s while down", [Node]),
    {ok, {ExitCode, StatusOut}} = rt:admin(Node, ["status"], [return_exit_code]),
    ?assertNotEqual(0, ExitCode),
    ?assert(rt:str(StatusOut, "not responding to pings")),
    ok.

getpid_up_test(Node) ->
    lager:info("Test riak get pid on ~s", [Node]),
    {ok, Output} = rt:riak(Node, ["pid"]),
    PidOut = string:trim(Output),   % trailing newline
    ?assertMatch([_|_], PidOut),
    ?assertEqual(PidOut, rpc:call(Node, os, getpid, [])),
    ok.

getpid_down_test(Node) ->
    lager:info("Test riak getpid fails on ~s", [Node]),
    {ok, PidOut} = rt:riak(Node, ["pid"]),
    ?assert(rt:str(PidOut, "not responding to pings")),
    ok.
