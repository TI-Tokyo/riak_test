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
%% @deprecated Needs replacement!
%% This module is pretty unusable due to it's unworkable structure.
-module(replication2_pg).
-deprecated(module).
-behavior(riak_test).

-export([
    confirm/0,

    %% individual tests
    test_12_pg_mode_repl12/0,
    test_12_pg_mode_repl12_ssl/0,
    test_12_pg_mode_repl_mixed/0,
    test_12_pg_mode_repl_mixed_ssl/0,
    test_basic_pg_mode_mixed/0,
    test_basic_pg_mode_mixed_ssl/0,
    test_basic_pg_mode_repl13/0,
    test_basic_pg_mode_repl13_ssl/0,
    test_bidirectional_pg/0,
    test_bidirectional_pg_ssl/0,
    test_cluster_mapping/0,
    test_mixed_pg/0,
    test_mixed_pg_ssl/0,
    test_multiple_sink_pg/0,
    test_multiple_sink_pg_ssl/0,
    test_pg_proxy/0,
    test_pg_proxy_ssl/0
]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-ignore_xref([
    %% This exists, but including intercepts in xref coverage isn't fun.
    {riak_repl2_leader_intercepts, set_leader_node, 1},

    %% Shut up the xref warnings about stuff that's not in Riak v3.
    {riak_repl_pb_api, get, 4},
    {riak_repl_pb_api, get, 5},
    {riak_repl_pb_api, get_clusterid, 1}
]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Test proxy_get in Default and Advanced mode of 1.3+ repl
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%setup_repl_clusters(Conf) ->
%%    setup_repl_clusters(Conf, false).

setup_repl_clusters(Conf, SSL) ->
    NumNodes = 6,
    ?LOG_INFO("Deploy ~b nodes", [NumNodes]),

    CertDir = rt_config:get(rt_scratch_dir) ++ "/certs",

    %% make a bunch of crypto keys
    make_certs:rootCA(CertDir, "rootCA"),
    make_certs:intermediateCA(CertDir, "intCA", "rootCA"),
    make_certs:endusers(CertDir, "rootCA", ["site3.basho.com", "site4.basho.com"]),
    make_certs:endusers(CertDir, "intCA", ["site1.basho.com", "site2.basho.com"]),

    SSLConfig1 = [
            {riak_core,
             [
                    {ssl_enabled, true},
                    {certfile, filename:join([CertDir,
                                              "site1.basho.com/cert.pem"])},
                    {keyfile, filename:join([CertDir,
                                             "site1.basho.com/key.pem"])},
                    {cacertdir, filename:join([CertDir,
                                               "site1.basho.com/cacerts.pem"])}
                    ]}
            ],

    SSLConfig2 = [
            {riak_core,
             [
                    {ssl_enabled, true},
                    {certfile, filename:join([CertDir,
                                              "site2.basho.com/cert.pem"])},
                    {keyfile, filename:join([CertDir,
                                             "site2.basho.com/key.pem"])},
                    {cacertdir, filename:join([CertDir,
                                               "site2.basho.com/cacerts.pem"])}
                    ]}
            ],

    SSLConfig3 = [
            {riak_core,
             [
                    {ssl_enabled, true},
                    {certfile, filename:join([CertDir,
                                              "site3.basho.com/cert.pem"])},
                    {keyfile, filename:join([CertDir,
                                             "site3.basho.com/key.pem"])},
                    {cacertdir, filename:join([CertDir,
                                               "site3.basho.com/cacerts.pem"])}
                    ]}
            ],


    rt:set_advanced_conf(all, Conf),
    Nodes = [ANodes, BNodes, CNodes] = rt:build_clusters([2, 2, 2]),

    rt:wait_for_cluster_service(ANodes, riak_repl),
    rt:wait_for_cluster_service(BNodes, riak_repl),
    rt:wait_for_cluster_service(CNodes, riak_repl),

    AFirst = hd(ANodes),
    BFirst = hd(BNodes),
    CFirst = hd(CNodes),

    rt:log_to_nodes(Nodes, "Starting replication2_pg test"),

    ?LOG_INFO("ANodes: ~0p", [ANodes]),
    ?LOG_INFO("BNodes: ~0p", [BNodes]),
    ?LOG_INFO("CNodes: ~0p", [CNodes]),

    case SSL of
        true ->
            ?LOG_INFO("Enabling SSL for this test"),
            [rt:update_app_config(N, merge_config(SSLConfig1, Conf)) ||
                N <- ANodes],
            [rt:update_app_config(N, merge_config(SSLConfig2, Conf)) ||
                N <- BNodes],
            [rt:update_app_config(N, merge_config(SSLConfig3, Conf)) ||
                N <- CNodes];
        _ ->
            ?LOG_INFO("SSL not enabled for this test")
    end,

    rt:log_to_nodes(Nodes, "Building and connecting clusters"),

    repl_util:name_cluster(AFirst, "A"),
    repl_util:name_cluster(BFirst, "B"),
    repl_util:name_cluster(CFirst, "C"),
    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),
    rt:wait_until_ring_converged(CNodes),

    %% get the leader for the first cluster
    repl_util:wait_until_leader(AFirst),
    LeaderA = rpc:call(AFirst, riak_core_cluster_mgr, get_leader, []),

    {ok, {BIP, BPort}} = rpc:call(BFirst, application, get_env,
                                  [riak_core, cluster_mgr]),
    repl_util:connect_cluster(LeaderA, BIP, BPort),

    {ok, {CIP, CPort}} = rpc:call(CFirst, application, get_env,
                                  [riak_core, cluster_mgr]),
    repl_util:connect_cluster(LeaderA, CIP, CPort),

    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),
    rt:wait_until_ring_converged(ANodes),

    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "C")),
    rt:wait_until_ring_converged(ANodes),

    rt:wait_until_transfers_complete(ANodes),
    rt:wait_until_transfers_complete(BNodes),
    rt:wait_until_transfers_complete(CNodes),

    {LeaderA, ANodes, BNodes, CNodes, Nodes}.


make_test_object(Suffix) ->
    Bucket = <<"test_bucket">>,
    KeyText = "test_key" ++ Suffix,
    ValueText = "testdata_" ++ Suffix,
    Key    = erlang:list_to_binary(KeyText),
    Value =  erlang:list_to_binary(ValueText),
    {Bucket, Key, Value}.


test_basic_pg(Mode) ->
    test_basic_pg(Mode, false).

test_basic_pg(Mode, SSL) ->
    banner(io_lib:format("test_basic_pg with ~0p mode", [Mode]), SSL),
    Conf = [
            {riak_repl,
             [
              {proxy_get, enabled},
              {fullsync_on_connect, false}
             ]}
           ],
    {LeaderA, ANodes, BNodes, CNodes, AllNodes} =
        setup_repl_clusters(Conf, SSL),
    rt:log_to_nodes(AllNodes, "Testing basic pg"),

    case Mode of
        mode_repl13 ->
            ModeRes = rpc:call(LeaderA, riak_repl_console, modes, [["mode_repl13"]]),
            ?LOG_INFO("ModeRes = ~0p", [ModeRes]);
        mixed ->
            ?LOG_INFO("Using mode_repl12, mode_repl13"),
            ok
    end,
    rt:wait_until_ring_converged(ANodes),

    PGEnableResult = rpc:call(LeaderA, riak_repl_console, proxy_get, [["enable","B"]]),
    ?LOG_INFO("Enabled pg: ~0p", [PGEnableResult]),
    Status = rpc:call(LeaderA, riak_repl_console, status, [quiet]),

    case proplists:get_value(proxy_get_enabled, Status) of
        undefined -> ?assert(false);
        EnabledFor -> ?LOG_INFO("PG enabled for cluster ~0p",[EnabledFor])
    end,

    PidA = rt:pbc(LeaderA),
    {ok,CidA}=riak_repl_pb_api:get_clusterid(PidA),
    ?LOG_INFO("Cluster ID for A = ~0p", [CidA]),

    {Bucket, KeyA, ValueA} = make_test_object("a"),
    {Bucket, KeyB, ValueB} = make_test_object("b"),

    rt:pbc_write(PidA, Bucket, KeyA, ValueA),
    rt:pbc_write(PidA, Bucket, KeyB, ValueB),

    _ = hd(ANodes),
    FirstB = hd(BNodes),
    FirstC = hd(CNodes),
    PidB = rt:pbc(FirstB),
    ?LOG_INFO("Connected to cluster B"),
    {ok, PGResult} = riak_repl_pb_api:get(PidB,Bucket,KeyA,CidA),
    ?assertEqual(ValueA, riakc_obj:get_value(PGResult)),

    rt:log_to_nodes(AllNodes, "Disabling pg on A"),
    PGDisableResult = rpc:call(LeaderA, riak_repl_console, proxy_get, [["disable","B"]]),
    ?LOG_INFO("Disable pg ~0p", [PGDisableResult]),
    Status2 = rpc:call(LeaderA, riak_repl_console, status, [quiet]),

    ?assertEqual([], proplists:get_value(proxy_get_enabled, Status2)),

    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),

    %% After the clusters are disconnected, see if the object was
    %% written locally after the PG
    {ok, PG2Value} = riak_repl_pb_api:get(PidB,Bucket,KeyA,CidA),

    ?assertEqual(ValueA, riakc_obj:get_value(PG2Value)),

    %% test an object that wasn't previously "proxy-gotten", it should fail
    FailedResult = riak_repl_pb_api:get(PidB,Bucket,KeyB,CidA),
    ?assertEqual({error, notfound}, FailedResult),

    PGEnableResult2 = rpc:call(LeaderA, riak_repl_console, proxy_get, [["enable","B"]]),
    ?LOG_INFO("Enabled pg: ~0p", [PGEnableResult2]),
    Status3 = rpc:call(LeaderA, riak_repl_console, status, [quiet]),

    case proplists:get_value(proxy_get_enabled, Status3) of
        undefined -> ?assert(false);
        EnabledFor2 -> ?LOG_INFO("PG enabled for cluster ~0p",[EnabledFor2])
    end,

    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),

    {ok, PGResult2} = riak_repl_pb_api:get(PidB,Bucket,KeyA,CidA),
    ?assertEqual(ValueA, riakc_obj:get_value(PGResult2)),

    %% Test with optional n_val and sloppy_quorum Options.
    %% KeyB is not on C yet. Try via proxy get with above options.

    PGEnableResult3 = rpc:call(LeaderA, riak_repl_console, proxy_get, [["enable","C"]]),
    ?LOG_INFO("Enabled pg: ~0p", [PGEnableResult3]),
    Status4 = rpc:call(LeaderA, riak_repl_console, status, [quiet]),

    case proplists:get_value(proxy_get_enabled, Status4) of
        undefined -> ?assert(false);
        EnabledFor3 -> ?LOG_INFO("PG enabled for cluster ~0p",[EnabledFor3])
    end,

    PidC = rt:pbc(FirstC),

    Options = [{n_val, 1}, {sloppy_quorum, false}],
    ?LOG_INFO("Test proxy get from C using options: ~0p", [Options]),
    PGResult3 = riak_repl_pb_api:get(PidC,Bucket,KeyA,CidA,Options),
    % it's ok if the first request fails due to the options,
    % try it again without options to see if it passes
    RetriableGet = case PGResult3 of
        {ok, PGResult3Value} ->
            riakc_obj:get_value(PGResult3Value);
        {error, notfound} ->
            RetryOptions = [{n_val, 1}],
            case riak_repl_pb_api:get(PidC,Bucket,KeyA,CidA,RetryOptions) of
                {ok, PGResult4Value} -> riakc_obj:get_value(PGResult4Value);
                UnknownResult -> UnknownResult
            end;
        UnknownResult ->
            %% welp, we might have been expecting a notfound, but we got
            %% something else.
            UnknownResult
    end,
    ?assertEqual(ValueA, RetriableGet),

    verify_topology_change(ANodes, BNodes),

    pass.

%% test 1.2 replication (aka "Default" repl)
%% Mode is either mode_repl12 or mixed.
%% "mixed" is the default in 1.3: mode_repl12, mode_repl13
test_12_pg(Mode) ->
    test_12_pg(Mode, false).

test_12_pg(Mode, SSL) ->
    banner(io_lib:format("test_12_pg with ~0p mode", [Mode]), SSL),
    Conf = [
            {riak_repl,
             [
              {proxy_get, enabled},
              {fullsync_on_connect, false}
             ]}
           ],
    {LeaderA, ANodes, BNodes, CNodes, AllNodes} =
        setup_repl_clusters(Conf, SSL),

    {Bucket, KeyA, ValueA} = make_test_object("a"),
    {Bucket, KeyB, ValueB} = make_test_object("b"),

    rt:log_to_nodes(AllNodes, "Test 1.2 proxy_get"),
    _ = hd(ANodes),
    FirstB = hd(BNodes),
    _ = hd(CNodes),
    case Mode of
        mode_repl12 ->
            ModeRes = rpc:call(FirstB, riak_repl_console, modes, [["mode_repl12"]]),
            ?LOG_INFO("ModeRes = ~0p", [ModeRes]);
        mixed ->
            ?LOG_INFO("Using mode_repl12, mode_repl13"),
            ok
    end,
    [rt:wait_until_ring_converged(Ns) || Ns <- [ANodes, BNodes, CNodes]],

    PidA = rt:pbc(LeaderA),
    rt:pbc_write(PidA, Bucket, KeyA, ValueA),
    rt:pbc_write(PidA, Bucket, KeyB, ValueB),

    {ok,CidA}=riak_repl_pb_api:get_clusterid(PidA),
    ?LOG_INFO("Cluster ID for A = ~0p", [CidA]),

    LeaderB = rpc:call(FirstB, riak_repl2_leader, leader_node, []),
    rt:log_to_nodes([LeaderB], "Trying to use PG while it's disabled"),
    PidB = rt:pbc(LeaderB),
    ?assertEqual({error, notfound},
                  riak_repl_pb_api:get(PidB, Bucket, KeyA, CidA)),

    rt:log_to_nodes([LeaderA], "Adding a listener"),
    LeaderAIP = rt:get_ip(LeaderA),
    ListenerArgs = [[atom_to_list(LeaderA), LeaderAIP, "5666"]],
    Res = rpc:call(LeaderA, riak_repl_console, add_listener, ListenerArgs),
    ?assertEqual(ok, Res),

    [rt:wait_until_ring_converged(Ns) || Ns <- [ANodes, BNodes, CNodes]],

    rt:log_to_nodes([FirstB], "Adding a site"),
    SiteArgs = [LeaderAIP, "5666", "rtmixed"],
    Res = rpc:call(FirstB, riak_repl_console, add_site, [SiteArgs]),
    ?LOG_INFO("Res = ~0p", [Res]),

    rt:log_to_nodes(AllNodes, "Waiting until connected"),
    wait_until_12_connection(LeaderA),

    [rt:wait_until_ring_converged(Ns) || Ns <- [ANodes, BNodes, CNodes]],
    ?LOG_INFO("Trying proxy_get"),

    LeaderB2 = rpc:call(FirstB, riak_repl2_leader, leader_node, []),
    PidB2 = rt:pbc(LeaderB2),
    {ok, PGResult} = riak_repl_pb_api:get(PidB2, Bucket, KeyB, CidA),
    ?LOG_INFO("PGResult: ~0p", [PGResult]),
    ?assertEqual(ValueB, riakc_obj:get_value(PGResult)),

    ?LOG_INFO("Disable repl and wait for clusters to disconnect"),

    rt:log_to_nodes([LeaderA], "Delete listener"),
    DelListenerArgs = [[atom_to_list(LeaderA), LeaderAIP, "5666"]],
    DelListenerRes = rpc:call(LeaderA, riak_repl_console, del_listener, DelListenerArgs),
    ?assertEqual(ok, DelListenerRes),

    [rt:wait_until_ring_converged(Ns) || Ns <- [ANodes, BNodes, CNodes]],

    rt:log_to_nodes([FirstB], "Delete site"),
    DelSiteArgs = [LeaderAIP, "5666", "rtmixed"],
    DelSiteRes = rpc:call(FirstB, riak_repl_console, add_site, [DelSiteArgs]),
    ?LOG_INFO("Res = ~0p", [DelSiteRes]),

    rt:log_to_nodes(AllNodes, "Waiting until disconnected"),
    wait_until_12_no_connection(LeaderA),

    [rt:wait_until_ring_converged(Ns) || Ns <- [ANodes, BNodes, CNodes]],

    rt:log_to_nodes(AllNodes, "Trying proxy_get without a connection"),
    ?assertEqual({error, notfound},
                  riak_repl_pb_api:get(PidB, Bucket, KeyA, CidA)),
    pass.

%% test shutting down nodes in source + sink clusters
test_pg_proxy() ->
    test_pg_proxy(false).

test_pg_proxy(SSL) ->
    banner("test_pg_proxy", SSL),
    Conf = [
            {riak_repl,
             [
              {proxy_get, enabled},
              {fullsync_on_connect, false}
             ]}
           ],
    {LeaderA, ANodes, BNodes, CNodes, AllNodes} =
        setup_repl_clusters(Conf, SSL),
    rt:log_to_nodes(AllNodes, "Testing pg proxy"),
    rt:wait_until_ring_converged(ANodes),

    PGEnableResult = rpc:call(LeaderA, riak_repl_console, proxy_get, [["enable","B"]]),
    ?LOG_INFO("Enabled pg: ~0p", [PGEnableResult]),
    Status = rpc:call(LeaderA, riak_repl_console, status, [quiet]),

    case proplists:get_value(proxy_get_enabled, Status) of
        undefined -> ?assert(false);
        EnabledFor -> ?LOG_INFO("PG enabled for cluster ~0p",[EnabledFor])
    end,

    PidA = rt:pbc(LeaderA),
    {ok,CidA}=riak_repl_pb_api:get_clusterid(PidA),
    ?LOG_INFO("Cluster ID for A = ~0p", [CidA]),

    %% Write a new k/v for every PG test, otherwise you'll get a locally written response
    {Bucket, KeyA, ValueA} = make_test_object("a"),
    {Bucket, KeyB, ValueB} = make_test_object("b"),
    {Bucket, KeyC, ValueC} = make_test_object("c"),
    {Bucket, KeyD, ValueD} = make_test_object("d"),

    rt:pbc_write(PidA, Bucket, KeyA, ValueA),
    rt:pbc_write(PidA, Bucket, KeyB, ValueB),
    rt:pbc_write(PidA, Bucket, KeyC, ValueC),
    rt:pbc_write(PidA, Bucket, KeyD, ValueD),
    %% sanity check. You know, like the 10000 tests that autoconf runs
    %% before it actually does any work.
    FirstA = hd(ANodes),
    FirstB = hd(BNodes),
    _ = hd(CNodes),
    PidB = rt:pbc(FirstB),
    ?LOG_INFO("Connected to cluster B"),
    {ok, PGResult} = riak_repl_pb_api:get(PidB,Bucket,KeyA,CidA),
    ?assertEqual(ValueA, riakc_obj:get_value(PGResult)),

    rt:wait_until_transfers_complete(ANodes),
    rt:wait_until_transfers_complete(BNodes),

    ?LOG_INFO("Stopping leader on requester cluster"),
    PGLeaderB = rpc:call(FirstB, riak_core_cluster_mgr, get_leader, []),
    rt:log_to_nodes(AllNodes, "Killing leader on requester cluster"),
    rt:stop(PGLeaderB),
    [RunningBNode | _ ] = BNodes -- [PGLeaderB],
    repl_util:wait_until_leader(RunningBNode),
    PidB2 = rt:pbc(RunningBNode),
    ?LOG_INFO("Now trying proxy_get"),
    ?assertEqual(ok, wait_until_pg(RunningBNode, PidB2, Bucket, KeyC, CidA)),
    ?LOG_INFO("If you got here, proxy_get worked after the pg block requesting leader was killed"),

    ?LOG_INFO("Stopping leader on provider cluster"),
    PGLeaderA = rpc:call(FirstA, riak_core_cluster_mgr, get_leader, []),
    rt:stop(PGLeaderA),
    [RunningANode | _ ] = ANodes -- [PGLeaderA],
    repl_util:wait_until_leader(RunningANode),
    ?assertEqual(ok, wait_until_pg(RunningBNode, PidB2, Bucket, KeyD, CidA)),
    ?LOG_INFO("If you got here, proxy_get worked after the pg block providing leader was killed"),
    ?LOG_INFO("pg_proxy test complete. Time to obtain celebratory cheese sticks."),

    pass.

%% test mapping of cluster from a retired cluster to an active one, repl issue 306
test_cluster_mapping() ->
    test_cluster_mapping(false).

test_cluster_mapping(SSL) ->
    banner("test_cluster_mapping", SSL),
    Conf = [
            {riak_repl,
             [
              {proxy_get, enabled},
              {fullsync_on_connect, false}
             ]}
           ],
    {LeaderA, ANodes, BNodes, CNodes, _AllNodes} =
        setup_repl_clusters(Conf, SSL),

    _ = hd(ANodes),
    FirstB = hd(BNodes),
    FirstC = hd(CNodes),
    LeaderB = rpc:call(FirstB, riak_core_cluster_mgr, get_leader, []),
    LeaderC = rpc:call(FirstC, riak_core_cluster_mgr, get_leader, []),

    % Cluser C-> connection must be set up for the proxy gets to work
    % with the cluster ID mapping
    {ok, {CIP, CPort}} = rpc:call(FirstC, application, get_env,
                                  [riak_core, cluster_mgr]),
    repl_util:connect_cluster(LeaderB, CIP, CPort),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderB, "C")),

    % enable A to serve blocks to C
    PGEnableResultA = rpc:call(LeaderA, riak_repl_console, proxy_get, [["enable","C"]]),
    % enable B to serve blocks to C
    PGEnableResultB = rpc:call(LeaderB, riak_repl_console, proxy_get, [["enable","C"]]),

    ?LOG_INFO("Enabled pg to A:~0p", [PGEnableResultA]),
    ?LOG_INFO("Enabled pg to B:~0p", [PGEnableResultB]),

    StatusA = rpc:call(LeaderA, riak_repl_console, status, [quiet]),
    case proplists:get_value(proxy_get_enabled, StatusA) of
        undefined -> ?assert(false);
        EnabledForA -> ?LOG_INFO("PG enabled for cluster ~0p",[EnabledForA])
    end,
    StatusB = rpc:call(LeaderB, riak_repl_console, status, [quiet]),
    case proplists:get_value(proxy_get_enabled, StatusB) of
        undefined -> ?assert(false);
        EnabledForB -> ?LOG_INFO("PG enabled for cluster ~0p",[EnabledForB])
    end,

    [rt:wait_until_ring_converged(Ns) || Ns <- [ANodes, BNodes, CNodes]],

    PidA = rt:pbc(LeaderA),
    {ok,CidA}=riak_repl_pb_api:get_clusterid(PidA),
    ?LOG_INFO("Cluster ID for A = ~0p", [CidA]),

    PidB = rt:pbc(LeaderB),
    {ok,CidB}=riak_repl_pb_api:get_clusterid(PidB),
    ?LOG_INFO("Cluster ID for B = ~0p", [CidB]),

    PidC = rt:pbc(LeaderC),
    {ok,CidC}=riak_repl_pb_api:get_clusterid(PidC),
    ?LOG_INFO("Cluster ID for C = ~0p", [CidC]),

    %% Write a new k/v for every PG test, otherwise you'll get a locally written response
    {Bucket, KeyA, ValueA} = make_test_object("a"),
    {Bucket, KeyB, ValueB} = make_test_object("b"),
    {Bucket, KeyC, ValueC} = make_test_object("c"),
    {Bucket, KeyD, ValueD} = make_test_object("d"),

    rt:pbc_write(PidA, Bucket, KeyA, ValueA),
    rt:pbc_write(PidA, Bucket, KeyB, ValueB),
    rt:pbc_write(PidA, Bucket, KeyC, ValueC),
    rt:pbc_write(PidA, Bucket, KeyD, ValueD),


    {ok, PGResult} = riak_repl_pb_api:get(PidA,Bucket,KeyA,CidA),
    ?assertEqual(ValueA, riakc_obj:get_value(PGResult)),

    % Configure cluster_mapping on C to map cluster_id A -> C
    ?LOG_INFO("Configuring cluster C to map its cluster_id to B's cluster_id"),
    %rpc:call(LeaderC, riak_core_metadata, put, [{<<"replication">>, <<"cluster-mapping">>}, CidA, CidB]),
    rpc:call(LeaderC, riak_repl_console, add_block_provider_redirect, [[CidA, CidB]]),
    Res = rpc:call(LeaderC, riak_core_metadata, get, [{<<"replication">>, <<"cluster-mapping">>}, CidA]),
    ?LOG_INFO("result: ~0p", [Res]),

    % full sync from CS Block Provider A to CS Block Provider B
    repl_util:enable_fullsync(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),

    {Time,_} = timer:tc(repl_util,start_and_wait_until_fullsync_complete,[LeaderA]),
    ?LOG_INFO("Fullsync completed in ~0p seconds", [Time/1000/1000]),

    % shut down cluster A
    ?LOG_INFO("Shutting down cluster A"),
    [ rt:stop(Node)  || Node <- ANodes ],
    [ rt:wait_until_unpingable(Node)  || Node <- ANodes ],

    rt:wait_until_ring_converged(BNodes),
    rt:wait_until_ring_converged(CNodes),

    % proxy-get from cluster C, using A's clusterID
    % Should redirect requester C from Cid A, to Cid B, and still
    % return the correct value for the Key
    {ok, PGResultC} = riak_repl_pb_api:get(PidC, Bucket, KeyC, CidA),
    ?LOG_INFO("PGResultC: ~0p", [PGResultC]),
    ?assertEqual(ValueC, riakc_obj:get_value(PGResultC)),

    % now delete the redirect and make sure it's gone
    rpc:call(LeaderC, riak_repl_console, delete_block_provider_redirect, [[CidA]]),
    case rpc:call(LeaderC, riak_core_metadata, get, [{<<"replication">>, <<"cluster-mapping">>}, CidA]) of
        undefined ->
            ?LOG_INFO("cluster-mapping no longer found in meta data, after delete, which is expected");
        Match ->
            ?LOG_INFO("cluster mapping ~0p still in meta data after delete; problem!", [Match]),
            ?assert(false)
    end,
    pass.

%% connect source + sink clusters, pg bidirectionally
test_bidirectional_pg() ->
    test_bidirectional_pg(false).

test_bidirectional_pg(SSL) ->
    banner("test_bidirectional_pg", SSL),
    Conf = [
            {riak_repl,
             [
              {proxy_get, enabled},
              {fullsync_on_connect, false}
             ]}
           ],
    {LeaderA, ANodes, BNodes, CNodes, AllNodes} =
        setup_repl_clusters(Conf, SSL),
    rt:log_to_nodes(AllNodes, "Testing bidirectional proxy-get"),

    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),

    FirstA = hd(ANodes),
    FirstB = hd(BNodes),
    _ = hd(CNodes),

    LeaderB = rpc:call(FirstB, riak_repl2_leader, leader_node, []),

    {ok, {AIP, APort}} = rpc:call(FirstA, application, get_env,
                                  [riak_core, cluster_mgr]),
    repl_util:connect_cluster(LeaderB, AIP, APort),

    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),

    PGEnableResult = rpc:call(LeaderA, riak_repl_console, proxy_get, [["enable","B"]]),
    PGEnableResult = rpc:call(LeaderB, riak_repl_console, proxy_get, [["enable","A"]]),

    ?LOG_INFO("Enabled bidirectional pg ~0p", [PGEnableResult]),
    StatusA = rpc:call(LeaderA, riak_repl_console, status, [quiet]),

    case proplists:get_value(proxy_get_enabled, StatusA) of
        undefined -> ?assert(false);
        EnabledForA -> ?LOG_INFO("PG enabled for cluster ~0p",[EnabledForA])
    end,

    StatusB = rpc:call(LeaderB, riak_repl_console, status, [quiet]),

    case proplists:get_value(proxy_get_enabled, StatusB) of
        undefined -> ?assert(false);
        EnabledForB -> ?LOG_INFO("PG enabled for cluster ~0p",[EnabledForB])
    end,

    PidA = rt:pbc(LeaderA),
    PidB = rt:pbc(FirstB),

    {ok,CidA}=riak_repl_pb_api:get_clusterid(PidA),
    {ok,CidB}=riak_repl_pb_api:get_clusterid(PidB),
    ?LOG_INFO("Cluster ID for A = ~0p", [CidA]),
    ?LOG_INFO("Cluster ID for B = ~0p", [CidB]),

    {Bucket, KeyA, ValueA} = make_test_object("a"),
    {Bucket, KeyB, ValueB} = make_test_object("b"),

    %% write some data to cluster A
    rt:pbc_write(PidA, Bucket, KeyA, ValueA),

    %% write some data to cluster B
    rt:pbc_write(PidB, Bucket, KeyB, ValueB),

    ?LOG_INFO("Trying first get"),
    wait_until_pg(LeaderB, PidB, Bucket, KeyA, CidA),
    ?LOG_INFO("First get worked"),

    ?LOG_INFO("Trying second get"),
    wait_until_pg(LeaderA, PidA, Bucket, KeyB, CidB),
    ?LOG_INFO("Second get worked"),

    verify_topology_change(ANodes, BNodes),

    pass.

%% Test multiple sinks against a single source
test_multiple_sink_pg() ->
    test_multiple_sink_pg(false).

test_multiple_sink_pg(SSL) ->
    banner("test_multiple_sink_pg", SSL),
    Conf = [
            {riak_repl,
             [
              {proxy_get, enabled},
              {fullsync_on_connect, false}
             ]}
           ],
    {LeaderA, ANodes, BNodes, CNodes, AllNodes} =
        setup_repl_clusters(Conf, SSL),
    rt:log_to_nodes(AllNodes, "Testing basic pg"),

    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),
    rt:wait_until_ring_converged(CNodes),

    PGEnableResultB = rpc:call(LeaderA, riak_repl_console, proxy_get, [["enable","B"]]),
    PGEnableResultC = rpc:call(LeaderA, riak_repl_console, proxy_get, [["enable","C"]]),

    ?LOG_INFO("Enabled pg to B:~0p", [PGEnableResultB]),
    ?LOG_INFO("Enabled pg to C:~0p", [PGEnableResultC]),
    Status = rpc:call(LeaderA, riak_repl_console, status, [quiet]),

    case proplists:get_value(proxy_get_enabled, Status) of
        undefined -> ?assert(false);
        EnabledForC -> ?LOG_INFO("PG enabled for cluster ~0p",[EnabledForC])
    end,

    PidA = rt:pbc(LeaderA),
    {ok,CidA}=riak_repl_pb_api:get_clusterid(PidA),
    ?LOG_INFO("Cluster ID for A = ~0p", [CidA]),

    {Bucket, KeyA, ValueA} = make_test_object("a"),
    {Bucket, KeyB, ValueB} = make_test_object("b"),

    rt:pbc_write(PidA, Bucket, KeyA, ValueA),
    rt:pbc_write(PidA, Bucket, KeyB, ValueB),

    _ = hd(ANodes),
    FirstB = hd(BNodes),
    FirstC = hd(CNodes),

    PidB = rt:pbc(FirstB),
    PidC = rt:pbc(FirstC),

    {ok, PGResultB} = riak_repl_pb_api:get(PidB,Bucket,KeyA,CidA),
    ?assertEqual(ValueA, riakc_obj:get_value(PGResultB)),

    {ok, PGResultC} = riak_repl_pb_api:get(PidC,Bucket,KeyB,CidA),
    ?assertEqual(ValueB, riakc_obj:get_value(PGResultC)),

    pass.

%% test 1.2 + 1.3 repl being used at the same time
test_mixed_pg() ->
    test_mixed_pg(false).

test_mixed_pg(SSL) ->
    banner("test_mixed_pg", SSL),
    Conf = [
            {riak_repl,
             [
              {proxy_get, enabled},
              {fullsync_on_connect, false}
             ]}
           ],
    {LeaderA, ANodes, BNodes, CNodes, AllNodes} =
        setup_repl_clusters(Conf, SSL),
    rt:log_to_nodes(AllNodes, "Testing basic pg"),

    rt:wait_until_ring_converged(ANodes),

    PGEnableResult = rpc:call(LeaderA, riak_repl_console, proxy_get, [["enable","B"]]),
    ?LOG_INFO("Enabled pg: ~0p", [PGEnableResult]),
    Status = rpc:call(LeaderA, riak_repl_console, status, [quiet]),

    case proplists:get_value(proxy_get_enabled, Status) of
        undefined -> ?assert(false);
        EnabledFor -> ?LOG_INFO("PG enabled for cluster ~0p",[EnabledFor])
    end,

    PidA = rt:pbc(LeaderA),
    {ok,CidA}=riak_repl_pb_api:get_clusterid(PidA),
    ?LOG_INFO("Cluster ID for A = ~0p", [CidA]),

    {Bucket, KeyB, ValueB} = make_test_object("b"),
    {Bucket, KeyC, ValueC} = make_test_object("c"),

    rt:pbc_write(PidA, Bucket, KeyB, ValueB),
    rt:pbc_write(PidA, Bucket, KeyC, ValueC),

    _ = hd(ANodes),
    FirstB = hd(BNodes),
    FirstC = hd(CNodes),
    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),

    rt:log_to_nodes([LeaderA], "Adding a listener"),
    ListenerIP = rt:get_ip(LeaderA),
    ListenerArgs = [[atom_to_list(LeaderA), ListenerIP, "5666"]],
    Res = rpc:call(LeaderA, riak_repl_console, add_listener, ListenerArgs),
    ?assertEqual(ok, Res),

    [rt:wait_until_ring_converged(Ns) || Ns <- [ANodes, BNodes, CNodes]],

    rt:log_to_nodes([FirstC], "Adding a site"),
    SiteArgs = [ListenerIP, "5666", "rtmixed"],
    Res = rpc:call(FirstC, riak_repl_console, add_site, [SiteArgs]),
    ?LOG_INFO("Res = ~0p", [Res]),

    rt:log_to_nodes(AllNodes, "Waiting until connected"),
    wait_until_12_connection(LeaderA),

    [rt:wait_until_ring_converged(Ns) || Ns <- [ANodes, BNodes, CNodes]],
    ?LOG_INFO("Trying proxy_get"),

    LeaderC = rpc:call(FirstC, riak_repl2_leader, leader_node, []),
    PidB = rt:pbc(FirstB),
    PidC = rt:pbc(LeaderC),

    {ok, PGResultB} = riak_repl_pb_api:get(PidB, Bucket, KeyB, CidA),
    ?LOG_INFO("PGResultB: ~0p", [PGResultB]),
    ?assertEqual(ValueB, riakc_obj:get_value(PGResultB)),

    {ok, PGResultC} = riak_repl_pb_api:get(PidC, Bucket, KeyC, CidA),
    ?LOG_INFO("PGResultC: ~0p", [PGResultC]),
    ?assertEqual(ValueC, riakc_obj:get_value(PGResultC)),

    pass.


wait_until_12_connection(Node) ->
    rt:wait_until(Node,
        fun(_) ->
                case rpc:call(Node, riak_repl_console, status, [quiet]) of
                    {badrpc, _} ->
                        false;
                    Status ->
                        case proplists:get_value(server_stats, Status) of
                            [] ->
                                false;
                            [{_, _, too_busy}] ->
                                false;
                            [_C] ->
                                true;
                            Conns ->
                                ?LOG_WARNING("multiple connections detected: ~0p",
                                    [Conns]),
                                true
                        end
                end
        end). %% 40 seconds is enough for repl

wait_until_12_no_connection(Node) ->
    rt:wait_until(Node,
        fun(_) ->
                case rpc:call(Node, riak_repl_console, status, [quiet]) of
                    {badrpc, _} ->
                        false;
                    Status ->
                        case proplists:get_value(server_stats, Status) of
                            undefined ->
                                true;
                            [] ->
                                true;
                            [{_, _, too_busy}] ->
                                false;
                            [_C] ->
                                false;
                            Conns ->
                                ?LOG_WARNING("multiple connections detected: ~0p",
                                    [Conns]),
                                false
                        end
                end
        end). %% 40 seconds is enough for repl



%% these funs allow you to call:
%% riak_test -t replication2_pg:test_basic_pg_mode_repl13 etc
test_basic_pg_mode_repl13() ->
    test_basic_pg(mode_repl13).

test_basic_pg_mode_mixed() ->
    test_basic_pg(mixed).

test_12_pg_mode_repl12() ->
    test_12_pg(mode_repl12).

test_12_pg_mode_repl_mixed() ->
         test_12_pg(mixed).


test_basic_pg_mode_repl13_ssl() ->
    test_basic_pg(mode_repl13, true).

test_basic_pg_mode_mixed_ssl() ->
    test_basic_pg(mixed, true).

test_12_pg_mode_repl12_ssl() ->
    test_12_pg(mode_repl12, true).

test_12_pg_mode_repl_mixed_ssl() ->
    test_12_pg(mixed, true).

test_mixed_pg_ssl() ->
    test_mixed_pg(true).

test_multiple_sink_pg_ssl() ->
    test_multiple_sink_pg(true).

test_bidirectional_pg_ssl() ->
    test_bidirectional_pg(true).

test_pg_proxy_ssl() ->
    test_pg_proxy(true).

confirm() ->
    AllTests =
        [
            test_basic_pg_mode_repl13,
            test_basic_pg_mode_mixed,
            test_12_pg_mode_repl12,
            test_12_pg_mode_repl_mixed,
            test_mixed_pg,
            test_multiple_sink_pg,
            test_bidirectional_pg,
            test_cluster_mapping,
            test_pg_proxy,
            test_basic_pg_mode_repl13_ssl,
            test_basic_pg_mode_mixed_ssl,
            test_12_pg_mode_repl12_ssl,
            test_12_pg_mode_repl_mixed_ssl,
            test_mixed_pg_ssl,
            test_multiple_sink_pg_ssl,
            test_bidirectional_pg_ssl,
            test_pg_proxy_ssl

        ],
    ?LOG_ERROR("run riak_test with -t Mod:test1 -t Mod:test2"),
    ?LOG_ERROR("The runnable tests in this module are: ~0p", [AllTests]),
    %% TODO: The problem with this LC is that it doesn't incorporate any
    %% of riak_test's setup/teardown per test.
    [?assertEqual(pass, erlang:apply(?MODULE, Test, [])) || Test <- AllTests].

%%banner(T) ->
%%    banner(T, false).

banner(T, SSL) ->
    ?LOG_INFO("----------------------------------------------"),
    ?LOG_INFO("----------------------------------------------"),
    ?LOG_INFO("~s, SSL ~s",[T, SSL]),
    ?LOG_INFO("----------------------------------------------"),
    ?LOG_INFO("----------------------------------------------").

wait_until_pg(Node, Pid, Bucket, Key, Cid) ->
    rt:wait_until(Node,
        fun(_) ->
                case riak_repl_pb_api:get(Pid,Bucket,Key,Cid) of
                    {error, notfound} ->
                        false;
                    {ok, _Value} -> true;
                    _ -> false
                end
        end).

merge_config(Mixin, Base) ->
    lists:ukeymerge(1, lists:keysort(1, Mixin), lists:keysort(1, Base)).

verify_topology_change(SourceNodes, SinkNodes) ->
    ?LOG_INFO("Verify topology changes doesn't break the proxy get."),

    %% Get connections
    [SourceNode1, _SourceNode2] = SourceNodes,
    SourceNode1Pid = rt:pbc(SourceNode1),
    [SinkNode1, SinkNode2] = SinkNodes,
    SinkNode1Pid = rt:pbc(SinkNode1),
    {ok, SourceCid} = riak_repl_pb_api:get_clusterid(SourceNode1Pid),

    %% Write new object to source.
    ?LOG_INFO("Writing key 'before' to the source."),
    {Bucket, KeyBefore, ValueBefore} = make_test_object("before"),
    rt:pbc_write(SourceNode1Pid, Bucket, KeyBefore, ValueBefore),

    %% Verify proxy_get through the sink works.
    ?LOG_INFO("Verifying key 'before' can be read through the sink."),
    {ok, PGResult1} = riak_repl_pb_api:get(SinkNode1Pid,
                                           Bucket, KeyBefore, SourceCid),
    ?assertEqual(ValueBefore, riakc_obj:get_value(PGResult1)),

    %% Remove leader from the sink cluster.
    SinkLeader = rpc:call(SinkNode1,
                          riak_repl2_leader, leader_node, []),

    %% Sad this takes 2.5 minutes
    ?LOG_INFO("Removing current leader from the cluster: ~0p.",
               [SinkLeader]),
    rt:leave(SinkLeader),
    ?assertEqual(ok, rt:wait_until_unpingable(SinkLeader)),

    %% Wait for everything to restart, and rings to converge.
    ?LOG_INFO("Starting leader node back up and waiting for repl."),
    rt:start(SinkLeader),
    rt:wait_for_service(SinkLeader, riak_repl),
    rt:wait_until_ring_converged(SinkNodes),

    %% Assert nodes have different leaders, which are themselves.
    ?LOG_INFO("Ensure that each node is its own leader."),
    SinkNode1Leader = rpc:call(SinkNode1,
                               riak_repl2_leader, leader_node, []),
    SinkNode2Leader = rpc:call(SinkNode2,
                               riak_repl2_leader, leader_node, []),
    ?assertEqual(SinkNode1, SinkNode1Leader),
    ?assertEqual(SinkNode2, SinkNode2Leader),
    ?assertNotEqual(SinkNode1Leader, SinkNode2Leader),

    %% Before we join the nodes, install an intercept on all nodes for
    %% the leader election callback.
    ?LOG_INFO("Installing set_leader_node intercept."),
    Result = riak_repl2_leader_intercepts:set_leader_node(SinkLeader),

    ?LOG_INFO("riak_repl2_leader_intercepts:set_leader_node(~0p) = ~0p", [SinkLeader, Result]),
    [ begin
        rt_intercept:load_code(N),
        ok = rt_intercept:add(N, {riak_repl2_leader, [{{set_leader,3}, set_leader_node}]})
    end || N <- SinkNodes ],

    %% Restart former leader and rejoin to the cluster.
    ?LOG_INFO("Rejoining former leader."),
    case SinkLeader of
        SinkNode1 ->
            rt:join(SinkNode1, SinkNode2);
        SinkNode2 ->
            rt:join(SinkNode2, SinkNode1)
    end,
    rt:wait_until_ring_converged(SinkNodes),

    %% Assert that all nodes have the same leader.
    ?LOG_INFO("Assert that all nodes have the same leader."),
    SinkNode1LeaderRejoin = rpc:call(SinkNode1,
                                     riak_repl2_leader, leader_node, []),
    SinkNode2LeaderRejoin = rpc:call(SinkNode2,
                                     riak_repl2_leader, leader_node, []),
    ?assertEqual(SinkNode1LeaderRejoin, SinkNode2LeaderRejoin),

    %% Assert that the leader is the former leader.
    ?LOG_INFO("Assert that new leader is the former leader."),
    ?assertEqual(SinkLeader, SinkNode1LeaderRejoin),

    %% Write new object to source.
    ?LOG_INFO("Writing key 'after' to the source."),
    {ok, SourceCid} = riak_repl_pb_api:get_clusterid(SourceNode1Pid),
    {Bucket, KeyPost, ValuePost} = make_test_object("after"),
    rt:pbc_write(SourceNode1Pid, Bucket, KeyPost, ValuePost),

    %% Verify we can retrieve from source.
    ?LOG_INFO("Verifying key 'after' can be read through the source."),
    {ok, PGResult2} = riak_repl_pb_api:get(SourceNode1Pid,
                                           Bucket, KeyPost, SourceCid),
    ?assertEqual(ValuePost, riakc_obj:get_value(PGResult2)),

    %% Verify proxy_get through the sink works.
    ?LOG_INFO("Verifying key 'after' can be read through the sink."),
    wait_until_pg(SinkNode1, SinkNode1Pid, Bucket, KeyPost, SourceCid),

    %% We're good!
    pass.
