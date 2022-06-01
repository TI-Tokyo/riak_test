-module(replication_upgrade).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

confirm() ->

    OrigDelay = rt_config:get(rt_retry_delay, 1000),
    logger:info("Original rt_retry_delay is ~p", [OrigDelay]),
    rt_config:set(rt_retry_delay, OrigDelay*5),
    NewDelay = rt_config:get(rt_retry_delay),
    logger:info("New rt_retry_delay is ~p", [NewDelay]),

    TestMetaData = riak_test_runner:metadata(),
    FromVersion = proplists:get_value(upgrade_version, TestMetaData, previous),

    logger:info("Doing rolling replication upgrade test from ~p to ~p",
        [FromVersion, "current"]),

    NumNodes = rt_config:get(num_nodes, 6),

    UpgradeOrder = rt_config:get(repl_upgrade_order, "forwards"),

    logger:info("Deploy ~p nodes", [NumNodes]),
    Conf = [
            {riak_repl,
             [
                {fullsync_on_connect, false},
                {fullsync_interval, disabled}
             ]}
    ],

    NodeConfig = [{FromVersion, Conf} || _ <- lists:seq(1, NumNodes)],

    Nodes = rt:deploy_nodes(NodeConfig, [riak_kv, riak_repl]),

    NodeUpgrades = case UpgradeOrder of
        "forwards" ->
            Nodes;
        "backwards" ->
            lists:reverse(Nodes);
        "alternate" ->
            %% eg 1, 4, 2, 5, 3, 6
            lists:flatten(lists:foldl(fun(E, [A,B,C]) -> [B, C, A ++ [E]] end,
                    [[],[],[]], Nodes));
        "random" ->
            %% halfass randomization
            lists:sort(fun(_, _) -> rand:uniform(100) < 50 end, Nodes);
        Other ->
            logger:error("Invalid upgrade ordering ~p", [Other]),
            erlang:exit()
    end,

    ClusterASize = rt_config:get(cluster_a_size, 3),
    {ANodes, BNodes} = lists:split(ClusterASize, Nodes),
    logger:info("ANodes: ~p", [ANodes]),
    logger:info("BNodes: ~p", [BNodes]),

    logger:info("Build cluster A"),
    repl_util:make_cluster(ANodes),

    logger:info("Build cluster B"),
    repl_util:make_cluster(BNodes),

    logger:info("Replication First pass...homogenous cluster"),

    %% initial replication run, homogeneous cluster
    replication:replication(ANodes, BNodes, false),

    logger:info("Upgrading nodes in order: ~p", [NodeUpgrades]),
    rt:log_to_nodes(Nodes, "Upgrading nodes in order: ~p", [NodeUpgrades]),
    %% upgrade the nodes, one at a time
    ok = lists:foreach(fun(Node) ->
                               logger:info("Upgrade node: ~p", [Node]),
                               rt:log_to_nodes(Nodes, "Upgrade node: ~p", [Node]),
                               rt:upgrade(Node, current, fun replication2_upgrade:remove_jmx_from_conf/1),
                               rt:wait_until_pingable(Node),
                               rt:wait_for_service(Node, [riak_kv, riak_pipe, riak_repl]),
                               [rt:wait_until_ring_converged(N) || N <- [ANodes, BNodes]],
                               %% Prior to 1.4.8 riak_repl registered
                               %% as a service before completing all
                               %% initialization including establishing
                               %% realtime connections.
                               %%
                               %% @TODO Ideally the test would only wait
                               %% for the connection in the case of the
                               %% node version being < 1.4.8, but currently
                               %% the rt API does not provide a
                               %% harness-agnostic method do get the node
                               %% version. For now the test waits for all
                               %% source cluster nodes to establish a
                               %% connection before proceeding.
                               case lists:member(Node, ANodes) of
                                   true ->
                                       replication:wait_until_connection(Node);
                                   false ->
                                       ok
                               end,
                               logger:info("Replication with upgraded node: ~p", [Node]),
                               rt:log_to_nodes(Nodes, "Replication with upgraded node: ~p", [Node]),
                               replication:replication(ANodes, BNodes, true)
                       end, NodeUpgrades),
    logger:info("Resetting rt_retry_delay to ~p", [OrigDelay]),
    rt_config:set(rt_retry_delay, OrigDelay),
    pass.
