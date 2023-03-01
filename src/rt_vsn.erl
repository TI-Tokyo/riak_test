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
%% @doc Functions for version parsing and comparison.
%%
%% The motivation for this module is finding Riak versions in the configured
%% RT staging tree that do or don't have features introduced at a specific
%% version in order to determine what test scenarios are supported.
%%
%% The {@link parse_version/1} function, though, can digest almost any
%% representation of a semver-ish version string, including those produced by
%% `git describe' and Rebar, as well as those found in devrel VERSION files,
%% into a version representation that is strictly ordered and efficiently
%% comparable, should the need arise to compare library, application, or other
%% versions.
%%
%% @todo The version parsing and comparison functions might belong in Riak.
%%
%% @end
%%
-module(rt_vsn).

-compile(inline).
-ifndef(TEST).
-compile(no_auto_import).
-endif. % TEST

% Public API
-export([
    compare_versions/2,
    configured_versions/0,
    find_version_at_least/1, find_version_at_least/2,
    find_version_before/1, find_version_before/2,
    new_version/1,
    parse_version/1,
    tagged_version/1,
    version_to_string/1
]).

% Public Types
-export_type([
    anyvsn/0,
    cfgvsn/0,
    rt_vsn/0
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif. % TEST

-record(rtv, {
    vsn :: nonempty_list(non_neg_integer())
}).

-type anyvsn() :: rt_vsn() | binstr() | vstruct().
-type binstr() :: binary() | string().
-type cfgvsn() :: {vsntag(), rt_vsn()}.
-type rt_vsn() :: #rtv{}.
-type vsnpath() :: {vsntag(), string()}.
-type vsntag() :: atom() | binstr().    % as found in rt.config `rtdev_path'
-type vstruct() :: {vsn, binstr()}.     % as found in `.app' files

%% ===================================================================
%% Public API
%% ===================================================================

-spec compare_versions(Left :: anyvsn(), Right :: anyvsn()) -> integer().
%% @doc Compares two versions and returns an integer result indicator.
%%
%% @returns <ul>
%%  <li><b>0</b> if the versions are strictly equal.</li>
%%  <li><b>&lt;0</b> if Left &lt; Right.</li>
%%  <li><b>&gt;0</b> if Left &gt; Right.</li>
%% </ul>
compare_versions(#rtv{vsn = Same}, #rtv{vsn = Same}) ->
    0;
compare_versions(#rtv{vsn = Left}, #rtv{vsn = Right}) ->
    compare_version_vals(Left, Right);
compare_versions(Vsn, Vsn) ->
    0;
compare_versions(Left, Right) ->
    compare_versions(parse_version(Left), parse_version(Right)).

-spec configured_versions() -> list(cfgvsn()).
%% @doc Returns an ordered list of the versions configured in riak_test.config.
%%
%% The returned  list is ordered by descending version, and picks up <i>ALL</i>
%% versions under `rtdev_path', not just `current/previous/legacy'.
configured_versions() ->
    configured_versions(rt_config:get(rtdev_path), []).

-spec find_version_at_least(TargetVsn :: anyvsn()) -> cfgvsn() | false.
%% @doc Return the first version (by priority) in {@link configured_versions/0}
%%      that is >= TargetVsn.
find_version_at_least(TargetVsn) ->
    find_version_at_least(TargetVsn, configured_versions()).

-spec find_version_at_least(
    TargetVsn :: anyvsn(), Versions :: list(cfgvsn())) -> cfgvsn() | false.
%% @doc Return the first version (by priority) in Versions that is >= TargetVsn.
find_version_at_least(_TargetVsn, []) ->
    false;
find_version_at_least(
    #rtv{} = TargetVsn, [{current, #rtv{} = CurVsn} = CurVsnRec | Vsns]) ->
    % In most scenarios, this should be the most common case.
    case compare_versions(CurVsn, TargetVsn) of
        I when I >= 0 ->
            CurVsnRec;
        _ ->
            find_version_at_least(TargetVsn, Vsns)
    end;
find_version_at_least(
    #rtv{} = TargetVsn, [{_HdTag, #rtv{} = HdVsn} = HdVsnRec | Vsns]) ->
    % If we fell through to here, it likely means that we're dealing with
    % some weird config where either a) 'current' isn't at the head of the
    % list OR b) 'current' isn't the highest version OR c) the list isn't
    % ordered as returned by configured_versions/0.
    % Prefer 'current' if it meets the predicate.
    case compare_versions(HdVsn, TargetVsn) of
        I when I >= 0 ->
            case lists:keyfind(current, 1, Vsns) of
                {current, #rtv{} = CurVsn} = CurVsnRec ->
                    case compare_versions(CurVsn, TargetVsn) of
                        I when I >= 0 ->
                            CurVsnRec;
                        _ ->
                            HdVsnRec
                    end;
                _ ->
                    HdVsnRec
            end;
        _ ->
            find_version_at_least(TargetVsn, Vsns)
    end;
find_version_at_least(#rtv{} = TargetVsn, Versions) ->
    %% Versions isn't a [proper] list.
    erlang:error(badarg, [TargetVsn, Versions]);
find_version_at_least(TargetVsn, Versions) ->
    find_version_at_least(parse_version(TargetVsn), Versions).

-spec find_version_before(TargetVsn :: anyvsn()) -> cfgvsn() | false.
%% @doc Return the first version in configured_versions/0 that is less
%%      than TargetVsn.
find_version_before(TargetVsn) ->
    find_version_before(TargetVsn, configured_versions()).

-spec find_version_before(
    TargetVsn :: anyvsn(), Versions :: list(cfgvsn())) -> cfgvsn() | false.
%% @doc Return the first version in Versions that is less than TargetVsn.
find_version_before(_TargetVsn, []) ->
    false;
find_version_before(
    #rtv{} = TargetVsn, [{_HdTag, #rtv{} = HdVsn} = HdVsnRec | Vsns]) ->
    case compare_versions(HdVsn, TargetVsn) of
        I when I < 0 ->
            HdVsnRec;
        _ ->
            find_version_before(TargetVsn, Vsns)
    end;
find_version_before(#rtv{} = TargetVsn, Versions) ->
    erlang:error(badarg, [TargetVsn, Versions]);
find_version_before(TargetVsn, Versions) ->
    find_version_before(parse_version(TargetVsn), Versions).

-spec new_version(nonempty_list(non_neg_integer())) -> rt_vsn().
%% @doc Create a {@link rt_vsn()} from a list of integers.
%%
%% Note that this function will happily create a version from a string,
%% because it's a list of integers, but that's probably not what you want
%% - use {@link parse_version/1} for that.
new_version(Ints) when erlang:is_list(Ints) andalso erlang:length(Ints) > 0 ->
    case lists:all(fun(I) -> erlang:is_integer(I) andalso I >= 0 end, Ints) of
        true ->
            #rtv{vsn = Ints};
        _ ->
            erlang:error(badarg, Ints)
    end;
new_version(Arg) ->
    erlang:error(badarg, [Arg]).

-spec parse_version(VsnIn :: anyvsn()) -> rt_vsn() | no_return().
%% @doc Parses [almost] anything into a {@link rt_vsn()}.
%% @end
%%  This is likely overkill within riak_test, but I already had the tested
%%  code so just lifting it verbatim.
parse_version(#rtv{} = VsnIn) ->
    VsnIn;
parse_version({vsn, VsnIn})
    when erlang:is_list(VsnIn) orelse erlang:is_binary(VsnIn) ->
    parse_version(VsnIn);
parse_version(VsnIn) when erlang:is_binary(VsnIn) ->
    parse_version(binary:bin_to_list(VsnIn));
parse_version(VsnIn) when erlang:is_list(VsnIn) ->
    Str = case string:trim(lists:flatten(VsnIn)) of
        % Handle the case of a exactly one leading 'v' before the first digit.
        [$v | [C | _] = Rest] when C >= $0 andalso C =< $9 ->
            Rest;
        VS ->
            VS
    end,
    % It's tempting to use lists:foldr/2, assuming only a few segments, but
    % we really don't know what may be passed in so play it safe.
    parse_version(string:lexemes(Str, ".-+"), []);
parse_version(VsnIn) ->
    erlang:error(badarg, [VsnIn]).

-spec version_to_string(Vsn :: rt_vsn()) -> string().
%% @doc Returns the dotted decimal string representation of a {@link rt_vsn()}.
version_to_string(#rtv{vsn = Vsn}) ->
    lists:flatten(lists:join(".", [erlang:integer_to_list(Seg) || Seg <- Vsn]));
version_to_string(Arg) ->
    erlang:error(badarg, [Arg]).

-spec tagged_version(Tag :: vsntag()) -> {vsntag(), rt_vsn()} | undefined | unknown.
%% @doc Returns the version associated with Tag in a tuple, or an error indicator.
%%
%% @returns <ul>
%%  <li>`undefined' if Tag isn't found in `rtdev_path'.</li>
%%  <li>`unknown' if Tag's Riak instance doesn't have a VERSION file.</li>
%% </ul>
tagged_version(Tag) ->
    try rt:get_version(Tag) of
        unknown = U ->
            U;
        Bin ->
            {Tag, parse_version(Bin)}
    catch
        _:_:_ ->
            undefined
    end.

%% ===================================================================
%% Internal
%% ===================================================================

-spec compare_version_tags(Left :: vsntag(), Right :: vsntag()) -> integer().
%% @hidden Version tag priority comparison
compare_version_tags(Tag, Tag) -> 0;
% Impose explicit order on the known tags
compare_version_tags(current, _) -> 1;
compare_version_tags(_, current) -> -1;
compare_version_tags(previous, _) -> 1;
compare_version_tags(_, previous) -> -1;
compare_version_tags(legacy, _) -> 1;
compare_version_tags(_, legacy) -> -1;
% Compare like kinds directly
compare_version_tags(L, R)
    when erlang:is_atom(L) andalso erlang:is_atom(R) andalso L > R -> 1;
compare_version_tags(L, R)
    when erlang:is_atom(L) andalso erlang:is_atom(R) andalso L < R -> -1;
compare_version_tags(L, R)
    when erlang:is_list(L) andalso erlang:is_list(R) andalso L > R -> 1;
compare_version_tags(L, R)
    when erlang:is_list(L) andalso erlang:is_list(R) andalso L < R -> -1;
compare_version_tags(L, R)
    when erlang:is_binary(L) andalso erlang:is_binary(R) andalso L > R -> 1;
compare_version_tags(L, R)
    when erlang:is_binary(L) andalso erlang:is_binary(R) andalso L < R -> -1;
% For everything else apply a priority before comparison
compare_version_tags(L, R) ->
    {PL, VL} = compare_prioritize(L),
    {PR, VR} = compare_prioritize(R),
    if
        VL > VR -> 1;
        VL < VR -> -1;
        %% Must be equal values, but from different source types.
        %% Compare priorities, which are in descending order.
        true -> (PR - PL)
    end.

-spec compare_prioritize(Tag :: term()) -> {integer(), string()}.
%% @hidden compare_version_tags helper
compare_prioritize(Tag) when erlang:is_atom(Tag) ->
    {1, erlang:atom_to_list(Tag)};
compare_prioritize(Tag) when erlang:is_list(Tag) ->
    {3, Tag};
compare_prioritize(Tag) when erlang:is_binary(Tag) ->
    {5, binary:bin_to_list(Tag)};
compare_prioritize(Tag) ->
    {9, lists:flatten(io_lib:format("~0p", [Tag]))}.

-spec compare_version_vals(
    Left :: list(non_neg_integer()), Right :: list(non_neg_integer())) -> integer().
%% @hidden Integer list comparison
compare_version_vals([], []) ->
    0;
compare_version_vals([], _) ->
    -1;
compare_version_vals(_, []) ->
    1;
compare_version_vals([I], [I]) ->
    0;
compare_version_vals([I | Left], [I | Right]) ->
    compare_version_vals(Left, Right);
compare_version_vals([L | _], [R | _]) ->
    (L - R).

-spec configured_versions(
    Elems :: list(vsnpath()), Result :: list(cfgvsn())) -> list(cfgvsn()).
%% @hidden Convert `rtdev_path' into an ordered list of tagged versions.
configured_versions([], Result) ->
    configured_tagged_versions(Result, []);
configured_versions([{root, _} | Elems], Result) ->
    configured_versions(Elems, Result);
configured_versions([{Tag, _} | Elems], Result) ->
    configured_versions(Elems, [Tag | Result]).

-spec configured_tagged_versions(
    Tags :: list(vsntag()), Result :: list(cfgvsn())) -> list(cfgvsn()).
%% @hidden Convert Tags into an ordered list of tagged versions.
configured_tagged_versions([], Result) ->
    lists:reverse(lists:sort(fun compare_tagged_versions/2, Result));
configured_tagged_versions([Tag | Tags], Result) ->
    case tagged_version(Tag) of
        {_Tag, #rtv{}} = TV ->
            configured_tagged_versions(Tags, [TV | Result]);
        _ ->
            % Drop anything without a version
            configured_tagged_versions(Tags, Result)
    end.

-spec compare_tagged_versions(
    L :: {vsntag(), rt_vsn()}, R :: {vsntag(), rt_vsn()}) -> boolean().
%% @hidden lists:sort/2 comparator
compare_tagged_versions({LT, #rtv{vsn = LV}}, {RT, #rtv{vsn = RV}}) ->
    case compare_version_vals(LV, RV) of
        0 ->
            compare_version_tags(LT, RT) =< 0;
        N ->
            N < 0
    end.

-spec parse_version(
    Segments :: list(string()), Result :: list(non_neg_integer())) -> rt_vsn().
%% @hidden The safe way to handle any number of segments.
parse_version([], Result) ->
    #rtv{vsn = lists:reverse(Result)};
parse_version([Segment | Segments], Result) ->
    case string:to_integer(Segment) of
        {error, _} ->
            parse_version(Segments, Result);
        {Seg, _} ->
            parse_version(Segments, [Seg | Result])
    end.

%% ===================================================================
%% Tests
%% ===================================================================
-ifdef(TEST).

%% Records look like:
%%  {[N1,N2...Nx], ["N1.N2...Nx" | StringRepresentations]}
%% in ascending order
-define(VSN_DATA, [
    {[1,2,3],   ["1.2.3",   "foo-1.2-tag-3-suffix"]},
    {[1,2,3,4], ["1.2.3.4", "foo-1.2-tag-3.rc-4", "foo-1.2.3-patch-4"]},
    {[2,3],     ["2.3",     "v2.3"]}
]).

new_version_test() ->
    ?assertEqual({rtv, [1,2]},          new_version([1,2])),
    ?assertEqual({rtv, [3,4,5]},        new_version([3,4,5])),
    ?assertEqual({rtv, [7,0,8,0,9]},    new_version([7,0,8,0,9])).

parse_version_test() ->
    lists:foreach(
        fun({IntVsn, StrVsns}) ->
            Vsn = new_version(IntVsn),
            lists:foreach(
                fun(StrVsn) ->
                    ?assertEqual(Vsn, parse_version(StrVsn))
                end, StrVsns)
        end, ?VSN_DATA).

version_to_string_test() ->
    lists:foreach(
        fun({IntVsn, [StrVsn | _]}) ->
            Vsn = new_version(IntVsn),
            ?assertEqual(StrVsn, version_to_string(Vsn))
        end, ?VSN_DATA).

compare_versions_1_test() ->
    lists:foreach(
        fun({IntVsn, StrVsns}) ->
            Vsn = new_version(IntVsn),
            lists:foreach(
                fun(StrVsn) ->
                    ?assertEqual(0, compare_versions(Vsn, StrVsn)),
                    ?assertEqual(0, compare_versions(StrVsn, StrVsn))
                end, StrVsns)
        end, ?VSN_DATA).

compare_versions_2_test() ->
    [LoVsn, MidVsn, HiVsn | _] = [new_version(I) || {I, _} <- ?VSN_DATA],
    ?assert(compare_versions(LoVsn, MidVsn) < 0),
    ?assert(compare_versions(LoVsn, HiVsn) < 0),
    ?assert(compare_versions(MidVsn, LoVsn) > 0),
    ?assert(compare_versions(HiVsn, MidVsn) > 0).

compare_version_tags_test() ->
    ?assert(compare_version_tags('current', "10") > 0),
    ?assert(compare_version_tags('legacy', '9.9.9') > 0),
    ?assert(compare_version_tags("9.9.9", '9.9.9') < 0).

-endif. % TEST
