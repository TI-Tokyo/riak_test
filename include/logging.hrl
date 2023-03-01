%% -*- mode: erlang; erlang-indent-level: 4; indent-tabs-mode: nil -*-
%% -------------------------------------------------------------------
%%
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
%% Compatibility layer for converting code from lager to the Erlang logger.
%%
%% Define one of the following to choose log destination:
%%  - 'LOG_TO_LOGGER'   uses the macros defined in the Erlang kernel app
%%  - 'LOG_TO_LAGER'    (default) uses lager via its parse-transform
%%  - 'LOG_TO_IODEV'    writes lager-like messages to ?LOG_TO_IODEV
%%  - 'LOG_TO_STDIO'    equivalent to -define(LOG_TO_IODEV, standard_io).
%%
%% Users are advised NOT to define macros, other than ONE of the above, named
%% or starting with LOG or _LOG while including this file to avoid conflicts.
%%
-ifndef(logging_included).
-define(logging_included, true).

%% For now, default to lager.
-ifndef(LOG_TO_LOGGER).
-ifndef(LOG_TO_IODEV).
-ifndef(LOG_TO_STDIO).
-ifndef(LOG_TO_LAGER).
-define(LOG_TO_LAGER, true).
-endif. % LOG_TO_LAGER
-endif. % LOG_TO_STDIO
-endif. % LOG_TO_IODEV
-endif. % LOG_TO_LOGGER

-ifdef(LOG_TO_LOGGER).
-include_lib("kernel/include/logger.hrl").
-else.  % Map logger macros onto lager/stdio
%%
%% These macros match the behavior of the Erlang Logger macros.
%% The parameters are renamed for clarity, but the patterns are unchanged.
%%
%% Parameters:
%%  L :: logger:level() - emergency..debug
%%  T :: term() - for compatibility should be string-ish or logger:report()
%%  F :: io:format() - format string
%%  A :: list(term()) - format args
%%  M :: logger:metadata() - MUST be a map, see below
%%
%% Metadata:
%%  The Metadata param MUST be a map!
%%  Lager accepts a naked pid() as the Metadata param, we don't.
%%  To pass just a pid, wrap it in #{pid => Pid} in the Metadata param.
%%

-define(LOG_EMERGENCY(T),       ?LOG(emergency, T)).
-define(LOG_EMERGENCY(F, A),    ?LOG(emergency, F, A)).
-define(LOG_EMERGENCY(F, A, M), ?LOG(emergency, F, A, M)).

-define(LOG_ALERT(T),           ?LOG(alert, T)).
-define(LOG_ALERT(F, A),        ?LOG(alert, F, A)).
-define(LOG_ALERT(F, A, M),     ?LOG(alert, F, A, M)).

-define(LOG_CRITICAL(T),        ?LOG(critical, T)).
-define(LOG_CRITICAL(F, A),     ?LOG(critical, F, A)).
-define(LOG_CRITICAL(F, A, M),  ?LOG(critical, F, A, M)).

-define(LOG_ERROR(T),           ?LOG(error, T)).
-define(LOG_ERROR(F, A),        ?LOG(error, F, A)).
-define(LOG_ERROR(F, A, M),     ?LOG(error, F, A, M)).

-define(LOG_WARNING(T),         ?LOG(warning, T)).
-define(LOG_WARNING(F, A),      ?LOG(warning, F, A)).
-define(LOG_WARNING(F, A, M),   ?LOG(warning, F, A, M)).

-define(LOG_NOTICE(T),          ?LOG(notice, T)).
-define(LOG_NOTICE(F, A),       ?LOG(notice, F, A)).
-define(LOG_NOTICE(F, A, M),    ?LOG(notice, F, A, M)).

-define(LOG_INFO(T),            ?LOG(info, T)).
-define(LOG_INFO(F, A),         ?LOG(info, F, A)).
-define(LOG_INFO(F, A, M),      ?LOG(info, F, A, M)).

-define(LOG_DEBUG(T),           ?LOG(debug, T)).
-define(LOG_DEBUG(F, A),        ?LOG(debug, F, A)).
-define(LOG_DEBUG(F, A, M),     ?LOG(debug, F, A, M)).

-ifdef(LOG_TO_STDIO).
-define(LOG_TO_IODEV, standard_io).
-endif. % LOG_TO_STDIO
-ifdef(LOG_TO_IODEV).
%% This block *tries* to replicate (approximately) lager/logger output.
%% It's pretty expensive with the inline calendar and io calls, though it's
%% not clear whether the cost is noticeably greater than that incurred when
%% one of the other subsystems handles the call.
-define(_LOG_TIMESTAMP,
    calendar:system_time_to_rfc3339(
        erlang:system_time(millisecond),
        [{unit, millisecond}, {time_designator, $\s}])).

-define(_LOG_(F, A),            io:format(?LOG_TO_IODEV, F, A)).

-define(_LOG_HEADER_FMT0,       "~s [~s] ~p@~s:~s:~b ").
-define(_LOG_HEADER_FMT(F),     ?_LOG_HEADER_FMT0 ++ F ++ "~n").
-define(_LOG_HEADER_FMT(F, M),  ?_LOG_HEADER_FMT0 ++ "~0p " ++ F ++ "~n").

-define(_LOG_HEADER_ARGS(L),
    ?_LOG_TIMESTAMP, L, erlang:self(), ?MODULE, ?FUNCTION_NAME, ?LINE).
-define(_LOG_HEADER_ARGS(L, M), ?_LOG_HEADER_ARGS(L), M).

-define(LOG(L, T),
    ?_LOG_(?_LOG_HEADER_FMT("~p"),  [?_LOG_HEADER_ARGS(L), T])).
-define(LOG(L, F, A),
    ?_LOG_(?_LOG_HEADER_FMT(F),     [?_LOG_HEADER_ARGS(L) | A])).
-define(LOG(L, F, A, M),
    ?_LOG_(?_LOG_HEADER_FMT(F, M),  [?_LOG_HEADER_ARGS(L, M) | A])).

-else.  % LOG_TO_LAGER
-define(LOG(L, T),          lager:L("~p", [T])).
-define(LOG(L, F, A),       lager:L(F, A)).
-define(LOG(L, F, A, M),    lager:L(maps:to_list(M), F, A)).
-endif. % ! LOG_TO_LOGGER

-endif. % LOG_TO_LOGGER

%% Shorthand for a common typo.
-define(LOG_WARN(T),        ?LOG_WARNING(T)).
-define(LOG_WARN(F, A),     ?LOG_WARNING(F, A)).
-define(LOG_WARN(F, A, M),  ?LOG_WARNING(F, A, M)).

-endif. % logging_included
