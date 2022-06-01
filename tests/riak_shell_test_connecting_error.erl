%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.
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
-module(riak_shell_test_connecting_error).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

-define(DONT_INCREMENT_PROMPT, false).

%% we cant run the test in this process as it receives various messages
%% and the running test interprets then as being messages to the shell
confirm() ->
    Nodes = riak_shell_test_util:build_cluster(),
    logger:info("Built a cluster of ~p~n", [Nodes]),
    Self = self(),
    _Pid = spawn_link(fun() -> run_test(Self) end),
    riak_shell_test_util:loop().

run_test(Pid) ->
    State = riak_shell_test_util:shell_init(),
    logger:info("~n~nStart running the command set-------------------------", []),
    Cmds = [
            %% 'connection prompt on' means you need to do unicode printing and stuff
            {run,
             "connection_prompt off;"},
            {run,
             "show_cookie;"},
            {run,
             "show_connection;"},
            {{match, "Connection to 'made up guff' failed"},
             "connect 'made up guff';"}
           ],
    Result = riak_shell_test_util:run_commands(Cmds, State,
                                               ?DONT_INCREMENT_PROMPT),
    logger:info("Result is ~p~n", [Result]),
    logger:info("~n~n------------------------------------------------------", []),
    Pid ! Result.
