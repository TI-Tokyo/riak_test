%% -------------------------------------------------------------------
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

-module(lager).

-export(
    [debug/1, debug/2,
        info/1, info/2,
        notice/1, notice/2,
        warning/1, warning/2,
        error/1, error/2]).

-include_lib("kernel/include/logger.hrl").

debug(Msg) ->
    debug(Msg, []).

debug(Msg, Args) ->
    ?LOG_DEBUG(Msg, Args).

info(Msg) ->
    info(Msg, []).

info(Msg, Args) ->
    ?LOG_INFO(Msg, Args).

notice(Msg) ->
    notice(Msg, []).

notice(Msg, Args) ->
    ?LOG_NOTICE(Msg, Args).

warning(Msg) ->
    warning(Msg, []).

warning(Msg, Args) ->
    ?LOG_WARNING(Msg, Args).

error(Msg) ->
    log_error(Msg, []).

error(Msg, Args) ->
    log_error(Msg, Args).

log_error(Msg, Args) ->
    ?LOG_ERROR(Msg, Args).