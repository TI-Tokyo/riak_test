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

%% @doc riak_test_runner runs a riak_test module's run/0 function.
-module(riak_test_runner).
-export([confirm/5, metadata/0, metadata/1, function_name/1]).
%% Need to export to use with `spawn_link'.
-export([return_to_exit/3, run_common_test/1]).
-include_lib("eunit/include/eunit.hrl").

-spec(metadata() -> [{atom(), term()}]).
%% @doc fetches test metadata from spawned test process
metadata() ->
    riak_test ! metadata,
    receive
        {metadata, TestMeta} -> TestMeta
    end.

metadata(Pid) ->
    riak_test ! {metadata, Pid},
    receive
        {metadata, TestMeta} -> TestMeta
    end.

-spec(confirm(integer(), atom(), riak_test | common_test, [{atom(), term()}], list()) -> [tuple()]).
%% @doc Runs a module's run/0 function after setting up a log capturing backend for logger.
%%      It then cleans up that backend and returns the logs as part of the return proplist.
confirm(TestModule, TestType, Outdir, TestMetaData, HarnessArgs)
  when TestType =:= riak_test orelse
       TestType =:= common_test ->
    rt:setup_harness(TestModule, HarnessArgs),
    {Mod, Fun} = function_name(TestModule),
    ThisModLog = filename:join([Outdir, atom_to_list(TestModule) ++ "-test.log"]),
    logger:add_handler(
      tested_mod_capture,
      logger_std_h,
      #{config => #{type => file,
                    file => ThisModLog},
        formatter => {logger_formatter, #{template => [time," [",level,"] ",msg,"\n"],
                                          time_designator => $ }
                     }
       }
     ),
    {Status, Reason} =
        case check_prereqs(Mod) of
            true ->
                logger:notice("================== BEGIN TEST ~s ==========", [TestModule]),
                execute(TestModule, TestType, {Mod, Fun}, TestMetaData);
            not_present ->
                {fail, test_does_not_exist};
            _ ->
                {fail, all_prereqs_not_present}
        end,
    logger:remove_handler(tested_mod_capture),

    logger:notice("------------------ END TEST ~s (~p) -------", [TestModule, Status]),

    RetList = [{test, TestModule}, {status, Status},
               {logs, get_test_logs(ThisModLog)} | proplists:delete(backend, TestMetaData)],
    case Status of
        fail -> RetList ++ [{reason, iolist_to_binary(io_lib:format("~p", [Reason]))}];
        _ -> RetList
    end.

get_test_logs(ThisModLog) ->
    case file:read_file(ThisModLog) of
        {ok, Log} ->
            Log;
        {error, enoent} ->
            logger:notice("test ~s did not profuce any logs", [ThisModLog]),
            []
    end.


%% does some group_leader swapping, in the style of EUnit.
execute(TestModule, TestType, {Mod, Fun}, TestMetaData) ->
    process_flag(trap_exit, true),
    OldGroupLeader = group_leader(),
    NewGroupLeader = riak_test_group_leader:new_group_leader(self()),
    group_leader(NewGroupLeader, self()),

    UName = os:cmd("uname -a"),
    logger:info("Test Runner `uname -a`: ~s", [UName]),

    Pid = case TestType of
              riak_test   -> spawn_link(?MODULE, return_to_exit, [Mod, Fun, []]);
              common_test -> spawn_link(?MODULE, run_common_test, [TestModule])
          end,
    Ref = case rt_config:get(test_timeout, undefined) of
        Timeout when is_integer(Timeout) ->
            erlang:send_after(Timeout, self(), test_took_too_long);
        _ ->
            undefined
    end,

    {Status, Reason} = rec_loop(Pid, TestModule, TestMetaData),
    case Ref of
        undefined ->
            ok;
        _ ->
            erlang:cancel_timer(Ref)
    end,
    riak_test_group_leader:tidy_up(OldGroupLeader),
    case Status of
        fail ->
            ErrorHeader = "================ " ++ atom_to_list(TestModule) ++
                " failure stack trace =====================",
            ErrorFooter = [ $= || _X <- lists:seq(1,length(ErrorHeader))],
            Error = io_lib:format("\n~s\n~p\n~s\n\n", [ErrorHeader, Reason, ErrorFooter]),
            io:format(Error);
        _ -> meh
    end,
    {Status, Reason}.

run_common_test(TestModule) ->
    ct:install([{config, ["config_node.ctc",
                          "config_user.ctc"]}]),
    TestDir = "tests",
    LogDir = "ct_logs",
    %% nonce file name required to get a directory
    ok = filelib:ensure_dir(LogDir ++ "/nonce.txt"),
    Opts = [
            {auto_compile, false},
            {dir,          TestDir},
            {suite,        TestModule},
            {logdir,       LogDir},
            {include,      [
                            "include",
                            "deps/*/ebin",
                            "deps/*/include"
                           ]}
           ],
    case ct:run_test(Opts) of
        {Pass, 0, {UserSkip, 0}} ->
            ct:print(default, 50, "Common Test: all passed ~p skipped ~p at the users request\n", [Pass, UserSkip]),
            'pass all the tests';
        {Pass, Fail, {UserSkip, AutoSkip}} ->
            ct:print(default, 50, "Common Test: passed ~p failed ~p common test skipped ~p skipped at the users request ~p~n",
                      [Pass, Fail, UserSkip, AutoSkip]),
            exit('tests failing');
        Other ->
            ct:print(default, 50, "Common test failed with ~p~n", [Other]),
            exit('tests failing')
    end.

function_name(TestModule) ->
    TMString = atom_to_list(TestModule),
    Tokz = string:tokens(TMString, ":"),
    case length(Tokz) of
        1 -> {TestModule, confirm};
        2 ->
            [Module, Function] = Tokz,
            {list_to_atom(Module), list_to_atom(Function)}
    end.

rec_loop(Pid, TestModule, TestMetaData) ->
    receive
        test_took_too_long ->
            exit(Pid, kill),
            {fail, test_timed_out};
        metadata ->
            Pid ! {metadata, TestMetaData},
            rec_loop(Pid, TestModule, TestMetaData);
        {metadata, P} ->
            P ! {metadata, TestMetaData},
            rec_loop(Pid, TestModule, TestMetaData);
        {'EXIT', Pid, normal} -> {pass, undefined};
        {'EXIT', Pid, Error} ->
            {fail, Error}
    end.

%% A return of `fail' must be converted to a non normal exit since
%% status is determined by `rec_loop'.
%%
%% @see rec_loop/3
-spec return_to_exit(module(), atom(), list()) -> ok.
return_to_exit(Mod, Fun, Args) ->
    case apply(Mod, Fun, Args) of
        pass ->
            %% same as exit(normal)
            ok;
        fail ->
            exit(fail)
    end.

check_prereqs(Module) ->
    try Module:module_info(attributes) of
        Attrs ->
            Prereqs = proplists:get_all_values(prereq, Attrs),
            P2 = [ {Prereq, rt_local:which(Prereq)} || Prereq <- Prereqs],
            case P2 of
                [] ->
                    logger:debug("~s has no prereqs", [P2]);
                _ ->
                    logger:info("~s prereqs: ~p", [Module, P2])
            end,
            [ logger:warning("~s prereq '~s' not installed.", [Module, P]) || {P, false} <- P2],
            lists:all(fun({_, Present}) -> Present end, P2)
    catch
        _DontCare:_Really ->
            not_present
    end.
