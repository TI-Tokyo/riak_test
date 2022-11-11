%% -------------------------------------------------------------------
%%
%% Copyright (c) 2018-2022 Workday, Inc.
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
-module(rt_kv_worker_proc).

%%
%% This module defines a named process (rt_kv_worker_proc), which is designed to work
%% in tandem with the riak_kv_worker intercept.
%%
%% Once started, this process will communicate with the riak_kv_worker, and will
%% be informed of when a worker starts and stops its work.  Users can install
%% callbacks into this process, which can maintain state information at the
%% start and stop points, and test various conditions during or after this process
%% has run.
%%

-export([
    add_intercept/1,
    start_link/0,
    start_link/1,
    stop/0,
    wait_until_work_completed/0,
    wait_until_work_completed/1
]).

-type accum() :: term().

-record(state, {
    rt_proc                                 :: pid(),
    started_callback                        :: fun(),
    completed_callback                      :: fun(),
    num_completions = 0                     :: pos_integer(),
    num_expected_completions                :: pos_integer(),
    current_work = sets:new()               :: any(),
    accum                                   :: accum(),
    timeout                                 :: pos_integer()
}).

%% @doc Add the requisite riak_kv_worker intercepts to the selected node/s.
-spec add_intercept(node() | [node()]) -> ok | [ok].
add_intercept(Node) when is_atom(Node) ->
    lager:info("Adding handle_work_handoff_intercept to node ~p", [Node]),
    rt_intercept:add(
        Node, {
            riak_kv_worker,
            [{{handle_work, 3}, handle_work_handoff_intercept}]
        }
    );
add_intercept(Nodes) ->
    [add_intercept(Node) || Node <- Nodes].

%% @doc effects start_link([])
start_link() ->
    start_link([]).

-type opts() :: [{atom(), term()}].

%% @doc Start the rt_kv_worker_proc
%% TODO doc options
-spec start_link(opts()) -> {ok, pid()}.
start_link(Opts) ->
    StartedCallback = proplists:get_value(started_callback, Opts, fun no_op_started/2),
    CompletedCallback = proplists:get_value(completed_callback, Opts, fun no_op_completed/2),
    NumExpectedCompletions = proplists:get_value(num_expected_completions, Opts, undefined),
    Accum = proplists:get_value(accum, Opts, undefined),
    Timeout = proplists:get_value(timeout, Opts, infinity),
    Self = self(),
    State = #state{
        rt_proc = Self,
        started_callback = StartedCallback,
        completed_callback = CompletedCallback,
        num_expected_completions = NumExpectedCompletions,
        accum = Accum,
        timeout = Timeout
    },
    Pid = spawn_link(fun() -> start_loop(State) end),
    yes = global:register_name(?MODULE, Pid),
    receive ok -> {ok, Pid} end.

%% @doc effects stop(60000)
-spec stop() -> accum().
stop() ->
    stop(60000).

%% @doc stop the rt_kv_worker_proc and return the current app state
-spec stop(Timeout::pos_integer()) -> accum().
stop(Timeout) ->
    _Pid = global:send(?MODULE, stop),
    receive
        Accum ->
            global:unregister_name(?MODULE),
            Accum
    after Timeout ->
        {error, timeout}
    end.

%% @doc effects wait_until_work_completed(60000)
-spec wait_until_work_completed() -> {ok, accum()} | {error, term()}.
wait_until_work_completed() ->
    wait_until_work_completed(60000).

%% @doc wait until all expected work has completed, and complete the app state, if
%% all work has completed as expected; otherwise, return an error.
-spec wait_until_work_completed(Timeout::pos_integer()) -> {ok, accum()} | {error, term()}.
wait_until_work_completed(Timeout) ->
    receive
        X -> X
    after Timeout ->
        {error, timeout}
    end.

%%
%% internal functions
%%

%% @private
start_loop(State) ->
    State#state.rt_proc ! ok,
    loop(State).

%% @private
loop(State) ->
    #state{
        rt_proc=RTProc,
        current_work=CurrentWork,
        started_callback=StartedCallback,
        completed_callback=CompletedCallback,
        num_completions=NumCompletions,
        num_expected_completions=NumExpectedCompletions,
        accum=Accum,
        timeout=Timeout
    } = State,
    receive
        {work_started, {Node, Ref}, Pid} ->
            Accum1 = StartedCallback({Node, Pid}, Accum),
            Pid ! {Ref, ok},
            loop(State#state{
                current_work = sets:add_element({Node, Pid}, CurrentWork),
                accum = Accum1
            });
        {work_completed, {Node, Ref}, Pid} ->
            NodePid = {Node, Pid},
            case sets:is_element(NodePid, CurrentWork) of
                true ->
                    {Result, Accum1} = CompletedCallback({Node, Pid}, Accum),
                    Pid ! {Ref, ok},
                    case Result of
                        ok ->
                            %% what to do next...
                            NewWork = sets:del_element(NodePid, CurrentWork),
                            case {NumCompletions + 1, sets:to_list(NewWork)} of
                                {NumExpectedCompletions, []} ->
                                    RTProc ! {ok, Accum1};
                                {NumExpectedCompletions, _} ->
                                    RTProc ! {error, more_completions_than_expected};
                                _ ->
                                    loop(State#state{
                                        current_work = NewWork,
                                        num_completions = NumCompletions + 1,
                                        accum = Accum1}
                                    )
                            end;
                        _ ->
                            RTProc ! {error, {unexpected_result, Result}}
                    end;
                _ ->
                    RTProc ! {error, {not_found, {Node, Pid}}}
            end;
        stop ->
            RTProc ! {ok, Accum}
    after Timeout ->
        RTProc ! {error, timeout}
    end.

no_op_started(_NodePair, Accum) -> Accum.
no_op_completed(_NodePair, Accum) -> {ok, Accum}.
