%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.
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

%% @doc This logger backend keeps a buffer of logs in memory and returns them all
%% when the handler terminates.

-module(riak_test_logger_backend).

-behavior(gen_server).

-export([start_link/0]).
-export([log/2,
         get_logs/0,
         clear/0]).
-export([init/1,
         handle_call/3,
         handle_info/2,
         handle_cast/2,
         terminate/2,
         code_change/3]).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


-record(state, {level :: logger:level(),
                verbose :: boolean(),
                log = [] :: [unicode:chardata()]
               }).

-spec get_logs() -> [iolist()] | {error, term()}.
get_logs() ->
    gen_server:call(?MODULE, get_logs).

-spec clear() -> ok.
clear() ->
    gen_server:call(?MODULE, clear).

-spec log(logger:log_event(), logger:handler_config()) -> ok.
log(Event, Config) ->
    gen_server:call(?MODULE, {log, Event, Config}).


-spec(init([term()]) -> {ok, #state{}} | {error, atom()}).
init([]) ->
    init([info, false]);
init([Level]) when is_atom(Level) ->
    init([Level, false]);
init([Level, Verbose]) ->
    {ok, #state{level = Level, verbose = Verbose}}.

-spec(handle_call(term(), pid(), #state{}) -> {ok, #state{}}).
handle_call({log,
             #{level := Level, meta := Meta} = LogEvent,
             Config},
            _From,
            #state{level=ThresholdLevel, verbose=Verbose, log = Logs} = State) ->
    File = maps:get(file, Meta, ""),
    Line = maps:get(line, Meta, ""),
    {Mfa_M, Mfa_F, _} = maps:get(mfa, Meta, {"", "", []}),
    case logger:compare_levels(Level, ThresholdLevel) of
        A when A == gt; A == eq ->
            FormattedMsg = logger_formatter:format(LogEvent, Config),
            Log = case Verbose of
                true ->
                    io_lib:format("~s | ~s@~s:~s:~s", [FormattedMsg, File, Mfa_M, Mfa_F, Line]);
                _ ->
                    FormattedMsg
            end,
            {reply, ok, State#state{log=[Log|Logs]}};
        lt ->
            {reply, ok, State}
    end;
handle_call(clear, _From, State) ->
    {reply, ok, State#state{log = []}};
handle_call(get_loglevel, _From, #state{level = Level} = State) ->
    {reply, Level, State};
handle_call({set_loglevel, Level}, _From, State) ->
    {reply, ok, State#state{level = Level}};
handle_call(get_logs, _From, #state{log = Logs} = State) ->
    {reply, Logs, State};
handle_call(_BadMsg, _From, State) ->
    logger:warning("don't call me that! (~p)", [_BadMsg]),
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    logger:warning("not expecting any casts: ~p", [_Msg]),
    {reply, ok, State}.

-spec(handle_info(any(), #state{}) -> {ok, #state{}}).
handle_info(_, State) ->
    {ok, State}.

-spec(code_change(any(), #state{}, any()) -> {ok, #state{}}).
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-spec(terminate(any(), #state{}) -> {ok, list()}).
terminate(_Reason, #state{log=Logs}) ->
    {ok, lists:reverse(Logs)}.
