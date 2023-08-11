%% -*- mode: erlang; erlang-indent-level: 4; indent-tabs-mode: nil -*-
%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012-2013 Basho Technologies, Inc.
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
%%
%% @doc Logger handler to execute on a remote Riak 3.0- node, installed
%% via RPC by `rt_logger'.
%%
%% Other `rt_remote_*' modules support later Riak versions.
%%
%% Portions lifted from the old `riak_test_lager_backend' module.
%%
%% The oldest lager version in use is 2.0.3 (Riak 2.0).
%%
%% !!! IMPORTANT !!!
%%
%% If tests try to use this on pre-current nodes, we could run into a
%% situation where we either have to pre-compile this module with an older
%% version of Erlang OR have to compile riak_test with an older version in
%% order for older nodes to be able to load this module as a binary.
%%
%% A (likely preferable) alternative would be shipping it over in source form
%% and compiling it there, which would require at least removing the rt_logger
%% behavior attribute. The module has no other external dependencies.
%% If the target node is on the local machine that's not hard to do (as in
%% rt_intercepts), but ideally such functionality should be wired up in the
%% harness to allow the source file to be sent to a remote node as needed and
%% compiled there. It's possible to read the source file here then rpc it to
%% the compilation operations on the remote node, keeping everything in memory,
%% but that's a pretty large hassle - much easier for the harness to handle
%% getting the file there and compiling it.
%%
%% Not dealing with any of it until it's actually shown to be needed, though.
%%
%% @end
-module(rt_remote_lager).
-behavior(gen_event).
-behavior(rt_logger).

% rt_logger callbacks
-export([
    connect/2,
    disconnect/2,
    log_local/3
]).

% gen_event callbacks
-export([
    code_change/3,
    handle_call/2,
    handle_event/2,
    handle_info/2,
    init/1,
    terminate/2
]).

-define(MANAGER, lager_event).
-define(HANDLER, ?MODULE).

-define(DFLT_LVL, info).

-define(METADATA, [{line, ?LINE}, {pid, erlang:self()},
    {module, ?MODULE}, {function, ?FUNCTION_NAME}, {arity, ?FUNCTION_ARITY}]).

-define(LOG(Lvl, Fmt, Args), lager:log(Lvl, ?METADATA, Fmt, Args)).

-ifdef(HIDE_FROM_EDOC).
-type state() :: #{
    dest := rt_logger:coll_ref(),
    from := rt_logger:src_id(),
    level := logger:level() | {mask, non_neg_integer()},
    format := term()
}.
-endif. % HIDE_FROM_EDOC

%% ===================================================================
%% rt_logger callbacks
%% ===================================================================

-spec connect(
    LogCollector :: rt_logger:coll_ref(), LogSourceId :: rt_logger:src_id() )
        -> ok | {error, term()}.
connect(LogCollector, LogSourceId) ->
    %% As close as lager can get to rt_logger:formatter(true, false)
    IfPid = {pid, [pid, "@"], []},
    IfMod = {module, [module, ":"], []},
    IfFun = {function, [function, ":"], []},
    IfLine = {line, [line, ":"], []},
    FConfig = [
        date, " " , time, " [", severity, "] ",
        IfPid, IfMod, IfFun, IfLine, " ", message
    ],
    HConfig = #{
        dest => LogCollector,
        from => LogSourceId,
        level => info,
        format => FConfig
    },
    gen_event:add_handler(?MANAGER, ?HANDLER, HConfig).

-spec disconnect(
    LogCollector :: rt_logger:coll_ref(), LogSourceId :: rt_logger:src_id() )
        -> ok | {error, term()}.
disconnect(_LogCollector, _LogSourceId) ->
    gen_event:delete_handler(?MANAGER, ?HANDLER, ?FUNCTION_NAME).

-spec log_local(
    Level :: logger:level(), Format :: io:format(), Args :: list() ) -> ok.
log_local(Level, Format, Args) ->
    lager:log(Level, [{pid, riak_test}], Format, Args).

%% ===================================================================
%% gen_event callbacks
%% ===================================================================

-spec init(State :: state()) -> {ok, state()}.
%% @private
init(#{dest := _, from := _, level := Level, format := [_|_]} = State) ->
    case parse_level(Level) of
        {error, _} = Error ->
            ?LOG(error, "invalid log level ~0p", [Level]),
            Error;
        Mask ->
            {ok, State#{level => Mask}}
    end.

-spec handle_event(Event :: term(), State :: state()) -> {ok, state()}.
%% @private
handle_event({log, Msg}, #{level := Level} = State) ->
    case lager_util:is_loggable(Msg, Level, ?MODULE) of
        true ->
            forward_log(
                lager_default_formatter:format(Msg, maps:get(format, State)),
                State),
            {ok, State};
        _ ->
            {ok, State}
    end;
handle_event(Event, State) ->
    io_lib:deep_char_list(Event) andalso forward_log(Event, State),
    {ok, State}.

-spec handle_call(Request :: term(), State :: state())
        -> {ok, term(), state()}.
%% @private
handle_call(get_loglevel, #{level := Level} = State) ->
    {ok, Level, State};
handle_call({set_loglevel, NewLevel}, State) ->
    case parse_level(NewLevel) of
        {error, _} = Error ->
            ?LOG(error, "invalid log level ~0p", [NewLevel]),
            {ok, Error, State};
        Mask ->
            {ok, ok, State#{level => Mask}}
    end;
handle_call(_Request, State) ->
    {ok, ignored, State}.

-spec handle_info(Request :: term(), State :: state()) -> {ok, state()}.
%% @private
handle_info(_Request, State) ->
    {ok, State}.

-spec terminate(Arg :: term(), State :: state()) -> ok.
%% @private
terminate(_Arg, _State) ->
    ok.

-spec code_change(OldVsn :: term(), State :: state(), Extra :: term())
        -> {ok, state()}.
%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ===================================================================
%% Internal
%% ===================================================================

forward_log(LogLine, #{dest := Collector, from := Source}) ->
    gen_server:cast(Collector, {Source, LogLine}).

parse_level(Level) ->
    try
        lager_util:config_to_mask(Level)
    catch
        error:_ ->
            {error, {bad_log_level, Level}}
    end.
