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
%% @doc Shared type definitions for riak_test.
%%
%% Many types are necessarily loosely defined - these are more placeholders
%% than real type definitions, where their names are (hopefully) descriptive
%% of the sort of data they represent.
%%
-module(rtt).

-export_type([
    app_config/0,
    backend/0,
    backends/0,
    capabilities/0,
    capability/0,
    cmd_arg/0,
    cmd_args/0,
    cmd_dir/0,
    cmd_error/0,
    cmd_exe/0,
    cmd_exec_err/0,
    cmd_list/0,
    cmd_out_format/0,
    cmd_rc/0,
    cmd_result/0,
    cmd_timeout_err/0,
    cmd_token/0,
    cmdline/0,
    config_elem/0,
    config_key/0,
    config_val/0,
    conn_info/0,
    env_var/0,
    env_vars/0,
    fs_path/0,
    interface/0,
    interfaces/0,
    millisecs/0,
    node_config/0,
    node_configs/0,
    node_id/0,
    node_ids/0,
    nodes/0,
    output_line/0,
    output_lines/0,
    predicate/1,
    product/0,
    products/0,
    stat/0,
    stat_key/0,
    stat_keys/0,
    stat_val/0,
    stats/0,
    std_error/0,
    strings/0,
    up_params/0,
    up_prm_dir/0,
    up_prm_vsn/0,
    upgrade_cb/0,
    vsn_key/0,
    vsn_str/0,
    vsn_tag/0
]).

%% For generating the export list, massaging required:
%% sed -En 's!^-(type|opaque) *([^\(]+)\(\) *::.+$!\2/0,!p' src/rtt.erl
%% sed -En 's!^-(type|opaque) *([^\(]+)\(([^\)]+)\) *::.+$!\2/\3,!p' src/rtt.erl

-type app_config() :: list(config_elem()).
-type backend() :: bitcask | eleveldb | leveled | memory.
-type backends() :: list(backend()).
-type capabilities() :: list(capability()).
-type capability() :: atom() | {atom(), tuple()}.
-type cmd_arg() :: string().
-type cmd_args() :: list(cmd_arg()).
-type cmd_dir() :: cwd | pwd | fs_path().
-type cmd_error() :: cmd_exec_err() | cmd_timeout_err().
-type cmd_exe() :: fs_path().
-type cmd_exec_err() :: {error, {file:posix(), fs_path()}}.
-type cmd_list() :: [cmd_exe() | cmd_args()].
-type cmd_out_format() :: rlines | lines | string.
-type cmd_rc() :: integer().
-type cmd_result() :: {cmd_rc(), cmd_out_format(), output_lines() | output_line()}.
-type cmd_timeout_err() :: {error, {timeout, {cmd_list(), output_lines()}}}.
-type cmd_token() :: term().
-type cmdline() :: nonempty_string().
-type config_elem() :: {config_key(), config_val()}.
-type config_key() :: atom().
-type config_val() :: term().
-type conn_info() :: [{node(), interfaces()}].
-type env_var() :: {os:env_var_name(), os:env_var_value() | false}.
-type env_vars() :: list(env_var()).
-type fs_path() :: nonempty_string().
-type interface() :: {http, tuple()} | {pb, tuple()}.
-type interfaces() :: list(interface()).
-type millisecs() :: pos_integer().
-type node_config() :: {vsn_tag(), app_config()}.
-type node_configs() :: list(node_config()).
-type node_id() :: pos_integer().
-type node_ids() :: list(node_id()).
-type nodes() :: list(node()).
-type output_line() :: string().
-type output_lines() :: list(string()).
-type predicate(ArgType) :: fun((ArgType) -> boolean()).
-type product() :: riak | riak_cs | unknown.
-type products() :: list(product()).
-type stat() :: {stat_key(), stat_val()}.
-type stat_key() :: binary() | nonempty_string().
-type stat_keys() :: list(stat_key()).
-type stat_val() :: term().
-type stats() :: list(stat()).
-type std_error() :: {error, term()}.
-type strings() :: list(string()).
-type up_params() :: list(up_prm_dir() | up_prm_vsn()).
-type up_prm_dir() :: {new_conf_dir | old_conf_dir | new_data_dir | old_data_dir, fs_path()}.
-type up_prm_vsn() :: {new_version | old_version, vsn_key()}.
-type upgrade_cb() :: fun((up_params()) -> ok | std_error()).
-type vsn_key() :: vsn_tag() | vsn_str().
-type vsn_str() :: binary() | nonempty_string().
-type vsn_tag() :: atom().
