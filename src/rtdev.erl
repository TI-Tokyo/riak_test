%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2014 Basho Technologies, Inc.
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

%% @private
-module(rtdev).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").

-define(DEVS(N), lists:concat(["dev", N, "@127.0.0.1"])).
-define(DEV(N), list_to_atom(?DEVS(N))).
-define(PATH, (rt_config:get(rtdev_path))).
-define(DEBUG_LOG_FILE(N),
        "dev" ++ integer_to_list(N) ++ "@127.0.0.1-riak-debug.tar.gz").

get_deps() ->
    lists:flatten(io_lib:format("~s/dev/dev1/riak/lib", [relpath(current)])).

riakcmd(Path, N, Cmd) ->
    ExecName = rt_config:get(exec_name, "riak"),
    io_lib:format("~s/dev/dev~b/riak/bin/~s~s~s", [Path, N, ExecName, maybe_hyphen(Cmd), Cmd]).
maybe_hyphen("chkconfig") -> "-";
maybe_hyphen("admin") -> "-";
maybe_hyphen("debug") -> "-";
maybe_hyphen("repl") -> "-";
maybe_hyphen(_) -> " ".


riakreplcmd(Path, N, Cmd) ->
    io_lib:format("~s/dev/dev~b/riak/bin/riak-repl ~s", [Path, N, Cmd]).

riak_admin_cmd(Path, N) ->
    ExecName = rt_config:get(exec_name, "riak"),
    lists:flatten(io_lib:format("~s/dev/dev~b/riak/bin/~s-admin", [Path, N, ExecName])).

riak_debug_cmd(Path, N) ->
    ExecName = rt_config:get(exec_name, "riak"),
    lists:flatten(io_lib:format("~s/dev/dev~b/riak/bin/~s-debug", [Path, N, ExecName])).

run_riak(N, Path, Cmd) ->
    logger:info("Running: ~s", [riakcmd(Path, N, Cmd)]),
    R = os:cmd(riakcmd(Path, N, Cmd)),
    case Cmd of
        "start" ->
            rt_cover:maybe_start_on_node(?DEV(N), node_version(N)),
            %% Intercepts may load code on top of the cover compiled
            %% modules. We'll just get no coverage info then.
            case rt_intercept:are_intercepts_loaded(?DEV(N)) of
                false ->
                    ok = rt_intercept:load_intercepts([?DEV(N)]);
                true ->
                    ok
            end,
            R;
        "stop" ->
            rt_cover:maybe_stop_on_node(?DEV(N)),
            R;
        _ ->
            R
    end.

riak_data(N) when is_integer(N) ->
    lists:flatten(io_lib:format("~s/dev/dev~b/data", [relpath(current), N])).

run_riak_repl(N, Path, Cmd) ->
    logger:info("Running: ~s", [riakcmd(Path, N, Cmd)]),
    os:cmd(riakreplcmd(Path, N, Cmd)).
    %% don't mess with intercepts and/or coverage,
    %% they should already be setup at this point

setup_harness(_Test, _Args) ->
    %% make sure we stop any cover processes on any nodes
    %% otherwise, if the next test boots a legacy node we'll end up with cover
    %% incompatabilities and crash the cover server
    rt_cover:maybe_stop_on_nodes(),
    %% %% Stop all discoverable nodes, not just nodes we'll be using for this test.
    %% rt:pmap(fun(X) -> stop_all(X ++ "/dev") end, devpaths()),

    logger:info("Cleaning up lingering pipe directories"),
    rt:pmap(fun(Dir) ->
                    %% when joining two absolute paths, filename:join intentionally
                    %% throws away the first one. ++ gets us around that, while
                    %% keeping some of the security of filename:join.
                    %% the extra slashes will be pruned by filename:join, but this
                    %% ensures that there will be at least one between "/tmp" and Dir
                    PipeDir = filename:join(["/tmp/" ++ Dir, "dev"]),
                    os:cmd("rm -rf " ++ PipeDir)
            end, devpaths()),

    logger:info("Cleaning up data dirs"),
    lists:map(fun(X) -> clean_data_dir_all(X) end,
              devpaths()),

    NodesN = lists:seq(1, 8),
    Nodes = [?DEV(N) || N <- NodesN],
    NodeMap = orddict:from_list(lists:zip(Nodes, NodesN)),
    rt_config:set(rt_nodes, NodeMap),

    ok.

clean_data_dir_all(DevPath) ->
    Devs = filelib:wildcard(DevPath ++ "/dev/*"),
    Clean = fun(C) ->
                    rm_dir(C ++ "/riak/data")
            end,
    [Clean(D) || D <- Devs],
    ok.

relpath(Vsn) ->
    Path = ?PATH,
    relpath(Vsn, Path).

relpath(Version, Paths=[{_,_}|_]) ->
    rt_util:find_atom_or_string_dict(Version, orddict:from_list(Paths));
relpath(current, Path) ->
    Path;
relpath(root, Path) ->
    Path;
relpath(_, _) ->
    throw("Version requested but only one path provided").

upgrade(Node, NewVersion, UpgradeCallback) when is_function(UpgradeCallback) ->
    upgrade(Node, NewVersion, same, UpgradeCallback).

upgrade(Node, NewVersion, Config, UpgradeCallback) ->
    N = node_id(Node),
    Version = node_version(N),
    logger:info("Upgrading ~p : ~p -> ~p", [Node, Version, NewVersion]),
    stop(Node),
    rt:wait_until_unpingable(Node),
    OldPath = relpath(Version),
    NewPath = relpath(NewVersion),

    Commands = [
        io_lib:format("cp -p -P -R \"~s/dev/dev~b/riak/data\" \"~s/dev/dev~b/riak\"",
                       [OldPath, N, NewPath, N]),
        io_lib:format("rm -rf ~s/dev/dev~b/riak/data/*",
                       [OldPath, N]),
        io_lib:format("cp -p -P -R \"~s/dev/dev~b/riak/etc\" \"~s/dev/dev~b/riak\"",
                       [OldPath, N, NewPath, N])
    ],
    [ begin
        logger:info("Running: ~s", [Cmd]),
        os:cmd(Cmd)
    end || Cmd <- Commands],
    VersionMap = orddict:store(N, NewVersion, rt_config:get(rt_versions)),
    rt_config:set(rt_versions, VersionMap),
    case Config of
        same -> ok;
        _ when is_function(Config) ->
            RiakConfPath =
                io_lib:format("~s/dev/dev~b/etc/riak.conf", [NewPath, N]),
            Config(RiakConfPath);
        _ -> update_app_config(Node, Config)
    end,
    Params = [
        {old_data_dir, io_lib:format("~s/dev/dev~b/riak/data", [OldPath, N])},
        {new_data_dir, io_lib:format("~s/dev/dev~b/riak/data", [NewPath, N])},
        {new_conf_dir, io_lib:format("~s/dev/dev~b/riak/etc",  [NewPath, N])},
        {old_version, Version},
        {new_version, NewVersion}
    ],
    ok = UpgradeCallback(Params),
    start(Node),
    rt:wait_until_pingable(Node),
    ok.

-spec copy_conf(integer(), atom() | string(), atom() | string()) -> ok.
copy_conf(NumNodes, FromVersion, ToVersion) ->
    logger:info("Copying config from ~p to ~p", [FromVersion, ToVersion]),

    FromPath = relpath(FromVersion),
    ToPath = relpath(ToVersion),

    [copy_node_conf(N, FromPath, ToPath) || N <- lists:seq(1, NumNodes)].

copy_node_conf(NodeNum, FromPath, ToPath) ->
    Command = io_lib:format("cp -p -P -R \"~s/dev/dev~b/riak/etc\" \"~s/dev/dev~b/riak\"",
                            [FromPath, NodeNum, ToPath, NodeNum]),
    os:cmd(Command),
    ok.

-spec set_conf(atom() | string(), [{string(), string()}]) -> ok.
set_conf(Node, NameValuePairs) when is_atom(Node) ->
    append_to_conf_file(get_riak_conf(Node), NameValuePairs),
    ok.

set_advanced_conf(all, NameValuePairs) ->
    logger:info("rtdev:set_advanced_conf(all, ~p)", [NameValuePairs]),
    [ set_advanced_conf(DevPath, NameValuePairs) || DevPath <- devpaths()],
    ok;
set_advanced_conf(Node, NameValuePairs) when is_atom(Node) ->
    update_app_config_file(get_advanced_riak_conf(Node), NameValuePairs),
    ok.

make_advanced_confs(DevPath) ->
    case filelib:is_dir(DevPath) of
        false ->
            logger:error("Failed generating advanced.conf ~p is not a directory.", [DevPath]),
            [];
        true ->
            Wildcard = io_lib:format("~s/dev/dev*/riak/etc", [DevPath]),
            ConfDirs = filelib:wildcard(Wildcard),
            [
             begin
                 AC = filename:join(Path, "advanced.config"),
                 logger:debug("writing advanced.conf to ~p", [AC]),
                 file:write_file(AC, io_lib:fwrite("~p.\n",[[]])),
                 AC
             end || Path <- ConfDirs]
    end.

get_riak_conf(Node) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    io_lib:format("~s/dev/dev~b/riak/etc/riak.conf", [Path, N]).

get_advanced_riak_conf(Node) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    io_lib:format("~s/dev/dev~b/riak/etc/advanced.config", [Path, N]).

append_to_conf_file(File, NameValuePairs) ->
    Settings = lists:flatten(
                 [io_lib:format("~n~s = ~s~n", [Name, Value])
                  || {Name, Value} <- NameValuePairs]),
    file:write_file(File, Settings, [append]).

%% update_app_config(all, Config) ->
%%     logger:debug("rtdev:update_app_config(all, ~p)", [Config]),
%%     [ update_app_config(DevPath, Config) || DevPath <- devpaths()];
update_app_config(Node, Config) when is_atom(Node) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    FileFormatString = "~s/dev/dev~b/riak/etc/~s.config",

    AppConfigFile = io_lib:format(FileFormatString, [Path, N, "app"]),
    AdvConfigFile = io_lib:format(FileFormatString, [Path, N, "advanced"]),
    %% If there's an app.config, do it old style
    %% if not, use cuttlefish's adavnced.config
    case filelib:is_file(AppConfigFile) of
        true ->
            update_app_config_file(AppConfigFile, Config);
        _ ->
            update_app_config_file(AdvConfigFile, Config)
    end.

update_app_config_file(ConfigFile, Config) ->
    logger:debug("rtdev:update_app_config_file(~s, ~p)", [ConfigFile, Config]),
    BaseConfig = case file:consult(ConfigFile) of
        {ok, [ValidConfig]} ->
            ValidConfig;
        {error, enoent} ->
            []
    end,
    MergeA = orddict:from_list(Config),
    MergeB = orddict:from_list(BaseConfig),
    NewConfig =
        orddict:merge(fun(_, VarsA, VarsB) ->
                              MergeC = orddict:from_list(VarsA),
                              MergeD = orddict:from_list(VarsB),
                              orddict:merge(fun(_, ValA, _ValB) ->
                                                    ValA
                                            end, MergeC, MergeD)
                      end, MergeA, MergeB),
    NewConfigOut = io_lib:format("~p.", [NewConfig]),
    ?assertEqual(ok, file:write_file(ConfigFile, NewConfigOut)),
    ok.

get_backend(Node) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    AppConfig = io_lib:format("~s/dev/dev~b/riak/etc/~s.config", [Path, N, "advanced"]),

    logger:debug("get_backend(~s)", [AppConfig]),
    Tokens = lists:reverse(filename:split(AppConfig)),
    ConfigFile = case Tokens of
        ["app.config"| _ ] ->
            AppConfig;
        ["advanced.config" | T] ->
            ["etc", "riak", [$d, $e, $v | N], "dev" | RPath] = T,
            Path = filename:join(lists:reverse(RPath)),
            %% Why chkconfig? It generates an app.config from cuttlefish
            %% without starting riak.

            ChkConfigOutput = string:tokens(run_riak(list_to_integer(N), Path, "chkconfig"), "\n"),

            ConfigFileOutputLine = lists:last(ChkConfigOutput),

            %% ConfigFileOutputLine looks like this:
            %% -config /path/to/app.config -args_file /path/to/vm.args -vm_args /path/to/vm.args
            Files =[ Filename || Filename <- string:tokens(ConfigFileOutputLine, "\s"),
                                 ".config" == filename:extension(Filename) ],

            case Files of
                [] -> %% No file generated by chkconfig. this isn't great
                    logger:error("Cuttlefish Failure."),
                    [ logger:error("~s", [Line]) || Line <- ChkConfigOutput ],
                    ?assert(false);
                _ ->
                    File = hd(Files),
                    case filename:pathtype(Files) of
                        absolute -> File;
                        relative ->
                            io_lib:format("~s/dev/dev~s/riak/~s", [Path, N, tl(hd(Files))])
                    end
                end
    end,

    case file:consult(ConfigFile) of
        {ok, [Config]} ->
            rt:get_backend(Config);
        E ->
            logger:error("Error reading ~s, ~p", [ConfigFile, E]),
            error
    end.

node_path(Node) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    lists:flatten(io_lib:format("~s/dev/dev~b/riak/", [Path, N])).

get_ip(_Node) ->
    %% localhost 4 lyfe
    "127.0.0.1".

create_dirs(Nodes) ->
    Snmp = [node_path(Node) ++ "/data/snmp/agent/db" || Node <- Nodes],
    [?assertCmd("mkdir -p " ++ Dir) || Dir <- Snmp].

clean_data_dir(Nodes, SubDir) when is_list(Nodes) ->
    logger:debug("Cleaning out data dirs"),
    DataDirs = [node_path(Node) ++ "/data/" ++ SubDir || Node <- Nodes],
    lists:foreach(fun rm_dir/1, DataDirs).

rm_dir(Dir) ->
    logger:debug("Removing directory ~s", [Dir]),
    ?assertCmd("rm -rf " ++ Dir),
    ?assertEqual(false, filelib:is_dir(Dir)).

restore_data_dir(Nodes, BackendFldr, BackupFldr) when is_list(Nodes) ->
    RestoreNodeFun =
        fun(Node) ->
            Backend = node_path(Node) ++ "/data/" ++ BackendFldr,
            Backup = node_path(Node) ++ "/data/" ++ BackupFldr,
            logger:info("Restoring Node ~s from ~s", [Backend, Backup]),
            ?assertCmd("mkdir -p " ++ Backend),
            ?assertCmd("cp -R " ++ Backup ++ "/* " ++ Backend)
        end,
    lists:foreach(RestoreNodeFun, Nodes).

add_default_node_config(Nodes) ->
    case rt_config:get(rt_default_config, undefined) of
        undefined -> ok;
        Defaults when is_list(Defaults) ->
            rt:pmap(fun(Node) ->
                            update_app_config(Node, Defaults)
                    end, Nodes),
            ok;
        BadValue ->
            logger:error("Invalid value for rt_default_config : ~p", [BadValue]),
            throw({invalid_config, {rt_default_config, BadValue}})
    end.

deploy_clusters(ClusterConfigs) ->
    NumNodes = rt_config:get(num_nodes),
    RequestedNodes = lists:flatten(ClusterConfigs),

    case length(RequestedNodes) > NumNodes of
        true ->
            erlang:error("Requested more nodes than available");
        false ->
            Nodes = deploy_nodes(RequestedNodes),
            {DeployedClusters, _} = lists:foldl(
                    fun(Cluster, {Clusters, RemNodes}) ->
                        {A, B} = lists:split(length(Cluster), RemNodes),
                        {Clusters ++ [A], B}
                end, {[], Nodes}, ClusterConfigs),
            DeployedClusters
    end.

deploy_nodes(NodeConfig) ->
    Path = relpath(root),
    NumNodes = length(NodeConfig),
    NodesN = lists:seq(1, NumNodes),
    Nodes = [?DEV(N) || N <- NodesN],
    {Versions, Configs} = lists:unzip(NodeConfig),
    VersionMap = lists:zip(NodesN, Versions),

    %% Check that you have the right versions available
    [ check_node(Version) || Version <- VersionMap ],

    rt_config:set(rt_versions, VersionMap),

    create_dirs(Nodes),
    clean_data_dir(Nodes, ""),

    %% Set initial config
    add_default_node_config(Nodes),
    rt:pmap(fun({_, default}) ->
                    ok;
               ({Node, {cuttlefish, Config}}) ->
                    set_conf(Node, Config);
               ({Node, Config}) ->
                    update_app_config(Node, Config)
            end,
            lists:zip(Nodes, Configs)),

    %% create snmp dirs, for EE
    create_dirs(Nodes),

    create_or_restore_config_backups(Nodes),

    %% Start nodes
    %%[run_riak(N, relpath(node_version(N)), "start") || N <- Nodes],
    lists:map(fun(N) -> run_riak(N, relpath(node_version(N)), "start") end, NodesN),

    %% Ensure nodes started
    [ok = rt:wait_until_pingable(N) || N <- Nodes],

    %% We have to make sure that riak_core_ring_manager is running before we can go on.
    [ok = rt:wait_until_registered(N, riak_core_ring_manager) || N <- Nodes],

    %% Ensure nodes are singleton clusters
    [ok = rt:check_singleton_node(?DEV(N)) || {N, Version} <- VersionMap,
                                              Version /= "0.14.2"],

    logger:info("Deployed nodes: ~p", [Nodes]),
    Nodes.

create_or_restore_config_backups(Nodes) ->
    lists:foreach(
      fun(Node) ->
              NodePath = node_path(Node),
              ConfFile = io_lib:format("~s/etc/riak.conf", [NodePath]),
              AdvCfgFile = io_lib:format("~s/etc/advanced.config", [NodePath]),
              [begin
                   case filelib:is_regular(F ++ ".backup") of
                       true ->
                           logger:debug("found existing backup of ~s; restoring it", [F]),
                           [] = os:cmd(io_lib:format("cp -a \"~s.backup\" \"~s\"", [F, F]));
                       false ->
                           logger:debug("backing up ~s", [F]),
                           [] = os:cmd(io_lib:format("cp -a \"~s\" \"~s.backup\"", [F, F]))
                   end
               end || F <- [ConfFile, AdvCfgFile]]
      end,
      Nodes).

gen_stop_fun(Timeout) ->
    fun({C,Node}) ->
            net_kernel:hidden_connect_node(Node),
            case rpc:call(Node, os, getpid, []) of
                PidStr when is_list(PidStr) ->
                    logger:info("Preparing to stop node ~p (process ID ~s) with init:stop/0...",
                                [Node, PidStr]),
                    rpc:call(Node, init, stop, []),
                    %% If init:stop/0 fails here, the wait_for_pid/2 call
                    %% below will timeout and the process will get cleaned
                    %% up by the kill_stragglers/2 function
                    wait_for_pid(PidStr, Timeout);
                {badrpc, nodedown} ->
                    ok;
                BadRpc ->
                    Cmd = C ++ "/bin/riak stop",
                    logger:debug("RPC to node ~p returned ~p, will try stop anyway... ~s",
                                [Node, BadRpc, Cmd]),
                    Output = os:cmd(Cmd),
                    Status = case Output of
                                 "ok\n" ->
                                     %% Telling the node to stop worked,
                                     %% but now we must wait the full node
                                     %% shutdown_time to allow it to
                                     %% properly shut down, otherwise it
                                     %% will get prematurely killed by
                                     %% kill_stragglers/2 below.
                                     timer:sleep(Timeout),
                                     "ok";
                                 _ ->
                                     "wasn't running"
                             end,
                    logger:info("Stopped node ~p, stop status: ~s.", [Node, Status])
            end
    end.

kill_stragglers(DevPath, Timeout) ->
    {ok, Re} = re:compile("^\\s*\\S+\\s+(\\d+).+\\d+\\s+"++DevPath++"\\S+/beam"),
    ReOpts = [{capture,all_but_first,list}],
    Pids = tl(string:tokens(os:cmd("ps -ef"), "\n")),
    Fold = fun(Proc, Acc) ->
                   case re:run(Proc, Re, ReOpts) of
                       nomatch ->
                           Acc;
                       {match,[Pid]} ->
                           logger:info("Process ~s still running, killing...",
                                       [Pid]),
                           os:cmd("kill -15 "++Pid),
                           case wait_for_pid(Pid, Timeout) of
                               ok -> ok;
                               fail ->
                                   logger:info("Process ~s still hasn't stopped, "
                                               "resorting to kill -9...", [Pid]),
                                   os:cmd("kill -9 "++Pid)
                           end,
                           [Pid|Acc]
                   end
           end,
    lists:foldl(Fold, [], Pids).

wait_for_pid(PidStr, Timeout) ->
    F = fun() ->
                os:cmd("kill -0 "++PidStr) =/= []
        end,
    Retries = Timeout div 1000,
    case rt:wait_until(F, Retries, 1000) of
        {fail, _} -> fail;
        _ -> ok
    end.

stop_all(DevPath) ->
    case filelib:is_dir(DevPath) of
        true ->
            Devs = filelib:wildcard(DevPath ++ "/dev*/riak/"),
            Nodes = [?DEV(N) || N <- lists:seq(1, length(Devs))],
            MyNode = 'riak_test@127.0.0.1',
            case net_kernel:start([MyNode, longnames]) of
                {ok, _} ->
                    true = erlang:set_cookie(MyNode, riak);
                {error,{already_started,_}} ->
                    ok
            end,
            logger:info("Trying to obtain node shutdown_time via RPC..."),
            Tmout = case rpc:call(hd(Nodes), init, get_argument, [shutdown_time]) of
                        {ok,[[Tm]]} -> list_to_integer(Tm)+10000;
                        _ -> 20000
                    end,
            logger:info("Using node shutdown_time of ~w", [Tmout]),
            rt:pmap(gen_stop_fun(Tmout), lists:zip(Devs, Nodes)),
            kill_stragglers(DevPath, Tmout);
        _ ->
            logger:info("~s is not a directory.", [DevPath])
    end,
    ok.

stop(Node) ->
    RiakPid = rpc:call(Node, os, getpid, []),
    N = node_id(Node),
    rt_cover:maybe_stop_on_node(Node),
    run_riak(N, relpath(node_version(N)), "stop"),
    F = fun(_N) ->
            os:cmd("kill -0 " ++ RiakPid) =/= []
    end,
    ?assertEqual(ok, rt:wait_until(Node, F)),
    ok.

start(Node) ->
    N = node_id(Node),
    run_riak(N, relpath(node_version(N)), "start"),
    ok.

attach(Node, Expected) ->
    interactive(Node, "attach", Expected).

attach_direct(Node, Expected) ->
    interactive(Node, "attach-direct", Expected).

console(Node, Expected) ->
    interactive(Node, "console", Expected).

interactive(Node, Command, Exp) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    Cmd = riakcmd(Path, N, Command),
    logger:info("Opening a port for riak ~s.", [Command]),
    logger:debug("Calling open_port with cmd ~s", [binary_to_list(iolist_to_binary(Cmd))]),
    P = open_port({spawn, binary_to_list(iolist_to_binary(Cmd))},
                  [stream, use_stdio, exit_status, binary, stderr_to_stdout]),
    interactive_loop(P, Exp).

interactive_loop(Port, Expected) ->
    receive
        {Port, {data, Data}} ->
            %% We've gotten some data, so the port isn't done executing
            %% Let's break it up by newline and display it.
            Tokens = string:tokens(binary_to_list(Data), "\n"),
            [logger:debug("~s", [Text]) || Text <- Tokens],

            %% Now we're going to take hd(Expected) which is either {expect, X}
            %% or {send, X}. If it's {expect, X}, we foldl through the Tokenized
            %% data looking for a partial match via rt:str/2. If we find one,
            %% we pop hd off the stack and continue iterating through the list
            %% with the next hd until we run out of input. Once hd is a tuple
            %% {send, X}, we send that test to the port. The assumption is that
            %% once we send data, anything else we still have in the buffer is
            %% meaningless, so we skip it. That's what that {sent, sent} thing
            %% is about. If there were a way to abort mid-foldl, I'd have done
            %% that. {sent, _} -> is just a pass through to get out of the fold.

            NewExpected = lists:foldl(fun(X, Expect) ->
                    [{Type, Text}|RemainingExpect] = case Expect of
                        [] -> [{done, "done"}|[]];
                        E -> E
                    end,
                    case {Type, rt:str(X, Text)} of
                        {expect, true} ->
                            RemainingExpect;
                        {expect, false} ->
                            [{Type, Text}|RemainingExpect];
                        {send, _} ->
                            port_command(Port, list_to_binary(Text ++ "\n")),
                            [{sent, "sent"}|RemainingExpect];
                        {sent, _} ->
                            Expect;
                        {done, _} ->
                            []
                    end
                end, Expected, Tokens),
            %% Now that the fold is over, we should remove {sent, sent} if it's there.
            %% The fold might have ended not matching anything or not sending anything
            %% so it's possible we don't have to remove {sent, sent}. This will be passed
            %% to interactive_loop's next iteration.
            NewerExpected = case NewExpected of
                [{sent, "sent"}|E] -> E;
                E -> E
            end,
            %% If NewerExpected is empty, we've met all expected criteria and in order to boot
            %% Otherwise, loop.
            case NewerExpected of
                [] -> ?assert(true);
                _ -> interactive_loop(Port, NewerExpected)
            end;
        {Port, {exit_status,_}} ->
            %% This port has exited. Maybe the last thing we did was {send, [4]} which
            %% as Ctrl-D would have exited the console. If Expected is empty, then
            %% We've met every expectation. Yay! If not, it means we've exited before
            %% something expected happened.
            ?assertEqual([], Expected)
        after rt_config:get(rt_max_wait_time) ->
            %% interactive_loop is going to wait until it matches expected behavior
            %% If it doesn't, the test should fail; however, without a timeout it
            %% will just hang forever in search of expected behavior. See also: Parenting
            ?assertEqual([], Expected)
    end.

admin(Node, Args, Options) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    Cmd = riak_admin_cmd(Path, N),
    logger:debug("Running: ~ts with args: ~p", [Cmd, Args]),
    Result = execute_admin_cmd(Cmd, Options ++ [{args, Args}]),
    logger:debug("~ts", [Result]),
    {ok, Result}.

execute_admin_cmd(Cmd, Options) ->
    {_ExitCode, Result} = FullResult = wait_for_cmd(spawn_cmd(Cmd, Options)),
    case lists:member(return_exit_code, Options) of
        true ->
            FullResult;
        false ->
            Result
    end.

riak(Node, Args) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    Result = run_riak(N, Path, Args),
    logger:info("~s", [Result]),
    {ok, Result}.


riak_repl(Node, Args) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    Result = run_riak_repl(N, Path, Args),
    logger:info("~s", [Result]),
    {ok, Result}.

node_id(Node) ->
    NodeMap = rt_config:get(rt_nodes),
    orddict:fetch(Node, NodeMap).

node_version(N) ->
    VersionMap = rt_config:get(rt_versions),
    rt_util:find_atom_or_string_dict(N, VersionMap).

spawn_cmd(Cmd) ->
    spawn_cmd(Cmd, []).
spawn_cmd(Cmd, Opts) ->
    open_port({spawn_executable, lists:flatten(Cmd)},
              [stream, in, exit_status, stderr_to_stdout] ++ Opts).

wait_for_cmd(Port) ->
    rt:wait_until(node(),
                  fun(_) ->
                          receive
                              {Port, Msg={exit_status, _}} ->
                                  catch port_close(Port),
                                  self() ! {Port, Msg},
                                  true
                          after 0 ->
                                  false
                          end
                  end),
    get_cmd_result(Port, []).

cmd(Cmd) ->
    cmd(Cmd, []).

cmd(Cmd, Opts) ->
    wait_for_cmd(spawn_cmd(Cmd, Opts)).

get_cmd_result(Port, Acc) ->
    receive
        {Port, {data, Bytes}} ->
            get_cmd_result(Port, [Bytes|Acc]);
        {Port, {exit_status, Status}} ->
            Output = lists:flatten(lists:reverse(Acc)),
            {Status, Output}
    after 0 ->
          timeout
    end.

check_node({_N, Version}) ->
    case rt_util:find_atom_or_string(Version, rt_config:get(rtdev_path)) of
        undefined ->
            logger:error("You don't have Riak ~s installed or configured", [Version]),
            erlang:error(lists:flatten(io_lib:format("You don't have Riak ~p installed or configured", [Version])));
        _ -> ok
    end.

%% not applicable for riak_ts
set_backend(Node, Backend) ->
    set_backend(Node, Backend, []).

set_backend(Node, Backend, OtherOpts) ->
    logger:debug("rtdev:set_backend(~p, ~p, ~p)", [Node, Backend, OtherOpts]),
    Opts = [{storage_backend, Backend} | OtherOpts],
    update_app_config(Node, [{riak_kv, Opts}]),
    get_backend(Node).

%% @doc Read the VERSION file from an arbitrarily tagged
%% version (e.g. current,
-spec(get_version(atom()) -> binary()).
get_version(Vsn) ->
    case file:read_file(relpath(Vsn) ++ "/VERSION") of
        {error, enoent} -> unknown;
        {ok, Version} -> Version
    end.

%% @doc Read the VERSION file for the `current` version
get_version() ->
    get_version(current).

%% Check all of the versions and find the devrel matching
%% Any of the binary version names, e.g., <<"riak_ts-1.3.1">>
find_version_by_name(Names) when is_list(Names) ->
    Versions = rt:versions(),
    Matches = lists:foldl(
        fun(Name, Acc) ->
            find_version_by_name(Versions, Name) ++ Acc
        end, [], Names),
    Matches.

find_version_by_name(Versions, Name) ->
    Match = lists:filter(
        fun(Vsn) ->
            NodeName = node_name_as_string(rt:get_version(Vsn)),
            case string:str(NodeName, Name) of
                0 -> false;
                _ -> true
            end
        end, Versions),
    Match.

node_name_as_string(unknown) ->
    "";
node_name_as_string(BinName) when is_binary(BinName) ->
    binary_to_list(BinName).

teardown() ->
    rt_cover:maybe_stop_on_nodes(),
    %% Stop all discoverable nodes, not just nodes we'll be using for this test.
    rt:pmap(fun(X) -> stop_all(X ++ "/dev") end, devpaths()).

whats_up() ->
    io:format("Here's what's running...~n"),

    Up = [rpc:call(Node, os, cmd, ["pwd"]) || Node <- nodes()],
    [io:format("  ~s~n",[string:substr(Dir, 1, length(Dir)-1)]) || Dir <- Up].

devpaths() ->
    lists:usort([ DevPath || {_Name, DevPath} <- proplists:delete(root, rt_config:get(rtdev_path))]).

versions() ->
    proplists:get_keys(rt_config:get(rtdev_path)) -- [root].

get_node_logs() ->
    Root = filename:absname(proplists:get_value(root, ?PATH)),
    RootLen = length(Root) + 1, %% Remove the leading slash
    [ begin
          {ok, Port} = file:open(Filename, [read, binary]),
          {lists:nthtail(RootLen, Filename), Port}
      end || Filename <- filelib:wildcard(Root ++ "/*/dev/dev*/riak/log/*") ].

get_node_debug_logs() ->
    NodeMap = rt_config:get(rt_nodes),
    lists:foldl(fun get_node_debug_logs/2,
                [], NodeMap).

get_node_debug_logs({_Node, NodeNum}, Acc) ->
    DebugLogFile = ?DEBUG_LOG_FILE(NodeNum),
    delete_existing_debug_log_file(DebugLogFile),
    Path = relpath(node_version(NodeNum)),
    Args = ["--logs"],
    Cmd = riak_debug_cmd(Path, NodeNum),
    {ExitCode, Result} = wait_for_cmd(spawn_cmd(Cmd, [{args, Args}])),
    logger:info("~p ExitCode ~p, Result = ~p", [Cmd, ExitCode, Result]),
    case filelib:is_file(DebugLogFile) of
        true ->
            {ok, Binary} = file:read_file(DebugLogFile),
            Acc ++ [{DebugLogFile, Binary}];
        _ ->
            Acc
    end.

%% If the debug log file exists from a previous test run it will cause the
%% `riak_debug_cmd' to fail. Therefore, delete the `DebugLogFile' if it exists.
%% Note that by ignoring the return value of `file:delete/1' this function
%% works whether or not the `DebugLogFile' actually exists at the time it is
%% called.
delete_existing_debug_log_file(DebugLogFile) ->
    file:delete(DebugLogFile).

%% @doc Performs a search against the log files on `Node' and returns all
%% matching lines.
-spec search_logs(node(), Pattern::iodata()) ->
    [{Path::string(), LineNum::pos_integer(), Match::string()}].
search_logs(Node, Pattern) ->
    Root = filename:absname(proplists:get_value(root, ?PATH)),
    Wildcard = Root ++ "/*/dev/" ++ node_name(Node) ++ "/riak/log/*",
    LogFiles = filelib:wildcard(Wildcard),
    AllMatches = rt:pmap(fun(File) ->
                                 search_file(File, Pattern)
                         end,
                         LogFiles),
    lists:flatten(AllMatches).

search_file(File, Pattern) ->
    {ok, Device} = file:open(File, [read]),
    Matches = search_file(Device, File, Pattern, 1, []),
    lists:reverse(Matches).

search_file(Device, File, Pattern, LineNum, Accum) ->
    case io:get_line(Device, "") of
        eof ->
            file:close(Device),
            Accum;
        Line ->
            NewAccum = case re:run(Line, Pattern) of
                           {match, _Captured} ->
                               Match = {File, LineNum, Line},
                               [Match|Accum];
                           nomatch ->
                               Accum
                       end,
            search_file(Device, File, Pattern, LineNum + 1, NewAccum)
    end.


-spec node_name(node()) -> string().
node_name(Node) ->
    lists:takewhile(fun(C) -> C /= $@ end, atom_to_list(Node)).

node_by_idx(N) ->
    ?DEV(N).

-ifdef(TEST).

release_versions_test() ->
    ok = rt_config:set(rtdev_path, [{root, "/Users/hazen/dev/rt/riak"},
             {current, "/Users/hazen/dev/rt/riak/current"},
             {previous, "/Users/hazen/dev/rt/riak/riak-2.0.6"},
             {legacy, "/Users/hazen/dev/rt/riak/riak-1.4.12"},
             {'2.0.2', "/Users/hazen/dev/rt/riak/riak-2.0.2"},
             {"2.0.4", "/Users/hazen/dev/rt/riak/riak-2.0.4"}]),
    ?assertEqual(ok, check_node({foo, '2.0.2'})),
    ?assertEqual(ok, check_node({foo, "2.0.4"})),
    ?assertEqual("/Users/hazen/dev/rt/riak/current", relpath(current)),
    ?assertEqual("/Users/hazen/dev/rt/riak/riak-2.0.2", relpath('2.0.2')),
    ?assertEqual("/Users/hazen/dev/rt/riak/riak-2.0.4", relpath("2.0.4")).

-endif.
