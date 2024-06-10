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
-module(verify_bulk_handoff).

%% Not an actual test
%% Used to create a scenario with a loaded cluster, and a node to be joined
%% To be used in manual experiments

-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET_A, <<"bulkload-systest_a">>).
-define(BUCKET_B, <<"bulkload-systest_b">>).
-define(BUCKET_C, <<"bulkload-systest_c">>).
-define(BUCKET_D, <<"bulkload-systest_d">>).

-define(CONFIG(RingSize, NVal), [
        {riak_core,
            [
             {ring_creation_size, RingSize},
             {default_bucket_props,
                 [
                     {n_val, NVal},
                     {allow_mult, true},
                     {dvv_enabled, true}
                 ]}
            ]
        },
        {riak_kv,
          [
           {anti_entropy, {off, []}},
           {tictacaae_active, active},
           {tictacaae_parallelstore, leveled_ko},
                % if backend not leveled will use parallel key-ordered
                % store
           {tictacaae_rebuildwait, 4},
           {tictacaae_rebuilddelay, 3600},
           {tictacaae_exchangetick, 120 * 1000},
           {tictacaae_rebuildtick, 3600000}, % don't tick for an hour!
           {delete_mode, keep}
          ]}
        ]).


confirm() ->
    KeyCount = 2000000,

    AllNodes = rt:deploy_nodes(6, ?CONFIG(32, 3)),
    [Node1, Node2, Node3, Node4, Node5, _Node6] = AllNodes,

    rt:staged_join(Node2, Node1),
    rt:staged_join(Node3, Node1),
    rt:staged_join(Node4, Node1),
    rt:staged_join(Node5, Node1),
    plan_and_wait(Node1, [Node1, Node2, Node3, Node4, Node5]),

    ok = bulk_load(Node1, KeyCount),
    
    halt.


bulk_load(Node, KeyCount) ->
    CountPerBucket = KeyCount div 4,

    CommonValBinA = <<"CommonValueToWriteForNV4ObjectsA">>,
    CommonValBinB = <<"CommonValueToWriteForNV4ObjectsB">>,
    CommonValBinC = <<"CommonValueToWriteForNV4ObjectsC">>,
    CommonValBinD = <<"CommonValueToWriteForNV4ObjectsD">>,
    write_to_cluster(Node, 1, CountPerBucket, ?BUCKET_A, true, CommonValBinA),
    write_to_cluster(Node, 1, CountPerBucket, ?BUCKET_B, true, CommonValBinB),
    write_to_cluster(Node, 1, CountPerBucket, ?BUCKET_C, true, CommonValBinC),
    write_to_cluster(Node, 1, CountPerBucket, ?BUCKET_D, true, CommonValBinD),
    ok.


-spec plan_and_wait(node(), [node()]) -> ok.
plan_and_wait(Claimant, Nodes) ->
    location:plan_and_wait(Claimant, Nodes).

write_to_cluster(Node, StartKey, Endkey, Bucket, NewObj, CommonValBin) ->
    nextgenrepl_ttaaefs_manual:write_to_cluster(
        Node, StartKey, Endkey, Bucket, NewObj, CommonValBin).