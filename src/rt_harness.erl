%% -*- mode: erlang; erlang-indent-level: 4; indent-tabs-mode: nil -*-
%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2016 Basho Technologies, Inc.
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
%%
%% @doc `rt_harness' behavior, for confirming harness implementations.
%%
%% EDoc can't, at present, generate actual documentation for callbacks,
%% so we're left with attempting to provide useful information with
%% descriptive parameter and type names.
%%
%% @end
-module(rt_harness).

%% ===================================================================
%% Behavior Callback API
%% ===================================================================

%% IMPORTANT:
%%  Returned results from callbacks with result pattern
%%      ok | rtt:std_error()
%%  are not to be trusted until they've all been verified.
%%  At present, that pattern is just a placeholder for implemented functions
%%  whose results weren't expected to be checked.

-callback admin(
    Node :: node(),
    Args :: rtt:cmd_args(),
    Options :: list(return_exit_code) ) ->
        {ok, rtt:output_line() | {rtt:cmd_rc(), rtt:output_line()}}.

-callback admin_stats(
    Node :: node(),
    Options :: list(return_exit_code | lines) ) ->
        {ok, rtt:output_line() | rtt:output_lines()} |
        {ok, {rtt:cmd_rc(), rtt:output_line() | rtt:output_lines()}}.

-callback attach(
    Node :: node(),
    Ops :: list({expect | send, string()}) ) ->
        ok | rtt:std_error().

-callback attach_direct(
    Node :: node(),
    Ops :: list({expect | send, string()}) ) ->
        ok | rtt:std_error().

-callback clean_data_dir(
    Nodes :: list(node()),
    SubDir :: rtt:fs_path() ) ->
        ok | rtt:std_error().

-callback cmd(
    Cmd :: rtt:cmd_exe(),
    Args :: rtt:cmd_args() ) ->
        {rtt:cmd_rc(), rtt:output_line()} | rtt:cmd_error().

-callback console(
    Node :: node(),
    Ops :: list({expect | send, string()}) ) ->
        ok | rtt:std_error().

-callback copy_conf(
    NumNodes :: pos_integer(),
    FromVersion :: rtt:vsn_tag(),
    ToVersion :: rtt:vsn_tag() ) ->
        ok | rtt:std_error().

-callback deploy_clusters(
    ClusterConfigs :: rtt:node_configs() ) ->
        rtt:nodes().

-callback deploy_nodes(
    Configs :: rtt:node_configs() ) ->
        rtt:nodes().

-callback get_backends() ->
        list(rtt:backend() | error).

-callback get_deps() ->
        rtt:fs_path().

-callback get_ip(Node :: node() ) ->
        nonempty_string().

-callback get_node_debug_logs() ->
        list({rtt:fs_path(), binary()}).

-callback get_node_id(
    Node :: node() ) ->
        rtt:node_id().

-callback get_node_logs() ->
        list({rtt:fs_path(), binary()}).

-callback get_node_path(
    Node :: node() ) ->
        rtt:fs_path().

-callback get_node_version(
    Node :: rtt:node_id() | node() ) ->
        rtt:vsn_str().

-callback get_version() ->
        rtt:vsn_str() | unknown.

-callback get_version(
    VsnTag :: rtt:vsn_tag() ) ->
        rtt:vsn_str() | unknown.

-callback restore_data_dir(
    Nodes :: rtt:nodes(),
    BackendFldr :: rtt:fs_path(),
    BackupFldr :: rtt:fs_path() ) ->
        ok | rtt:std_error().

-callback riak(
    Node :: node(),
    Args :: string() ) ->
        string().

-callback riak_repl(
    Node :: node(),
    Args :: string() ) ->
        string().

-callback search_logs(
    Node :: node(),
    Pattern :: iodata() ) ->
        list( {
            LogFile :: rtt:fs_path(),
            LineNum :: pos_integer(),
            MatchingLine :: string() } ).

-callback set_advanced_conf(
    Where :: node() | rtt:fs_path() | all,
    ConfigElems :: rtt:app_config() ) ->
        ok | rtt:std_error().

-callback set_conf(
    Where :: node() | rtt:fs_path() | all,
    ConfigElems :: rtt:app_config() ) ->
        ok | rtt:std_error().

-callback setup_harness(
    TestModule :: module(),
    HarnessArgs :: list(string()) ) ->
        ok | rtt:std_error().

-callback spawn_cmd(
    Cmd :: rtt:cmd_exe(),
    Args :: rtt:cmd_args() ) ->
        rtt:cmd_token() | rtt:cmd_exec_err().

-callback spawned_result(
    CmdToken :: rtt:cmd_token() ) ->
        {rtt:cmd_rc(), rtt:output_line()} | rtt:cmd_timeout_err().

-callback start(
    Node :: node() ) ->
        ok | rtt:std_error().

-callback start_naked(
    Node :: node() ) ->
        ok | rtt:std_error().

-callback stop(
    Node :: node() ) ->
        ok | rtt:std_error().

-callback teardown() ->
        ok | rtt:std_error().

-callback update_app_config(
    Where :: node() | rtt:fs_path() | all,
    ConfigElems :: rtt:app_config() ) ->
        ok | rtt:std_error().

-callback upgrade(
    Node :: node(),
    NewVersion :: rtt:vsn_key(),
    UpgradeCallback :: rtt:upgrade_cb() ) ->
        ok | rtt:std_error().

-callback upgrade(
    Node :: node(),
    NewVersion :: rtt:vsn_key(),
    NewAppConfig :: rtt:app_config(),
    UpgradeCallback :: rtt:upgrade_cb() ) ->
        ok | rtt:std_error().

-callback versions() ->
        list(rtt:vsn_tag() | rtt:vsn_str()).

-callback whats_up() ->
        list(rtt:fs_path()).

%%
%% Implementing sources include between -ifdef(behavior_include) below.
%% Regenerate the list with
%%  erl -pa _build/default/lib/riak_test/ebin -noshell -eval \
%%      'lists:foreach(fun({F,A}) -> io:format("    ~s/~b,~n", [F, A]) end, \
%%          rt_harness:behaviour_info(callbacks)), halt(0).'
%%
-ifdef(behavior_include).
-behavior(rt_harness).

-exports([
    admin/3,
    admin_stats/2,
    attach/2,
    attach_direct/2,
    clean_data_dir/2,
    cmd/2,
    console/2,
    copy_conf/3,
    deploy_clusters/1,
    deploy_nodes/1,
    get_backends/0,
    get_deps/0,
    get_ip/1,
    get_node_debug_logs/0,
    get_node_id/1,
    get_node_logs/0,
    get_node_path/1,
    get_node_version/1,
    get_version/0,
    get_version/1,
    restore_data_dir/3,
    riak/2,
    riak_repl/2,
    search_logs/2,
    set_advanced_conf/2,
    set_conf/2,
    setup_harness/2,
    spawn_cmd/2,
    spawned_result/1,
    start/1,
    start_naked/1,
    stop/1,
    teardown/0,
    update_app_config/2,
    upgrade/3,
    upgrade/4,
    versions/0,
    whats_up/0
]).
-endif. % behavior_include
