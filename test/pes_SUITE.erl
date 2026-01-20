-module(pes_SUITE).
-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------

-define(TEST_PROCESS(TTL), spawn(fun() -> timer:sleep(TTL) end)).
-define(TEST_PROCESS(Node, TTL), rpc:call(Node, erlang, spawn, [fun() -> timer:sleep(TTL) end])).
-define(TEST_HEARTBEAT_TIMEOUT, 500).

suite() ->
    [{timetrap, {minutes, 10}}].

init_per_suite(Config) ->
    application:set_env(pes, heartbeat, ?TEST_HEARTBEAT_TIMEOUT),
    {ok, _} = application:ensure_all_started(pes),
    pes_cfg:set(heartbeat, ?TEST_HEARTBEAT_TIMEOUT),
    Config.

end_per_suite(_Config) ->
    application:stop(pes),
    ok.

init_per_group(GroupName, Config) ->
    ?MODULE:GroupName({setup, Config}).

end_per_group(GroupName, Config) ->
    ?MODULE:GroupName({tear_down, Config}).

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

groups() ->
    [
        {cluster_group, [{repeat_until_any_fail, 1}], [
            register_no_majority,
            register_one_node_not_up_to_date,
            register_previous_record_expired,
            register_previous_record_expired_but_alive,
            reallocate_guard_process,
            repair_multiple_processes
        ]}
    ].

all() ->
    [
        pes_server,
        pes_server_live_upgrade,
        register,
        re_register,
        register_already_running,
        register_monitored_process_died,
        register_monitored_process_shutdown,
        register_died_process,
        register_guard_process_died,
        send_undefined,
        send_ok,
        unregister_undefined,
        update_undefined,
        update_ok,
        repair,
        cfg_set,
        clean,
        stat,
        pes_server_state,
        {group, cluster_group}
    ].

pes_server(_Config) ->
    Id = <<"pes_server_test_case">>,

    % no record but commit
    ?assertEqual({nack, {node(), not_found}}, pes_call(commit, [node(), Id, 1, test])),

    % no record
    ?assertEqual(ack, pes_call(prepare, [node(), Id, 1])),
    % skip term
    ?assertEqual(ack, pes_call(prepare, [node(), Id, 122])),
    % Lower term
    ?assertEqual(nack, pes_call(prepare, [node(), Id, 2])),

    % commit success
    ack = pes_call(prepare, [node(), Id, 250]),
    ?assertEqual(ack, pes_call(commit, [node(), Id, 250, test2])),

    % lower term for commit
    ack = pes_call(prepare, [node(), Id, 252]),
    ?assertEqual({nack, {node(), {252, self()}}}, pes_call(commit, [node(), Id, 251, test3])),

    % higher term for commit
    ack = pes_call(prepare, [node(), Id, 255]),
    ?assertEqual({nack, {node(), {255, self()}}}, pes_call(commit, [node(), Id, 256, test3])),

    % concurrent
    ?assertEqual(ack, pes_call(prepare, [node(), Id, 1000])),
    ?assertEqual(ack, pes_call(prepare, [node(), Id, 1001])),
    ?assertEqual({nack, {node(), {1001, self()}}}, pes_call(commit, [node(), Id, 1000, test])),
    ?assertEqual(nack, pes_call(prepare, [node(), Id, 1001])),
    ?assertEqual(ack, pes_call(commit, [node(), Id, 1001, test])),
    ok.

pes_server_live_upgrade(_Config) ->
    {_, Pid, worker, _} = hd(supervisor:which_children(pes_server_sup)),
    ok = sys:suspend(Pid),
    ok = sys:change_code(Pid, pes_server, undefined, []),
    ok = sys:resume(Pid),
    sys:terminate(Pid, violent),
    ok.

register(_Config) ->
    TestPid = ?TEST_PROCESS(1000),
    Id = <<"reg_1">>,
    ?assertEqual(yes, pes:register_name(Id, TestPid)),
    ?assertEqual(TestPid, pes:whereis_name(Id)),
    ?assertEqual(ok, pes:unregister_name(Id)),
    ?assertEqual(undefined, pes:whereis_name(Id)),
    ok.

re_register(_Config) ->
    TestPidA = ?TEST_PROCESS(1000),
    TestPidB = ?TEST_PROCESS(1000),
    Id = <<"re_reg_1">>,
    ?assertEqual(yes, pes:register_name(Id, TestPidA)),
    ?assertEqual(ok, pes:unregister_name(Id)),
    ?assertEqual(undefined, pes:whereis_name(Id)),
    ?assertEqual(yes, pes:register_name(Id, TestPidB)),
    ?assertEqual(TestPidB, pes:whereis_name(Id)),
    ok.

register_already_running(_Config) ->
    TestPid = ?TEST_PROCESS(1000),
    Id = <<"reg_already_running">>,
    ?assertEqual(yes, pes:register_name(Id, TestPid)),
    ?assertEqual(no, pes:register_name(Id, TestPid)),
    ok.

register_monitored_process_died(_Config) ->
    TestPid = ?TEST_PROCESS(1000),
    Id = <<"proc_died">>,
    ?assertEqual(yes, pes:register_name(Id, TestPid)),
    exit(TestPid, kill),
    % need some to to process down messages
    ct:sleep(10),
    ?assertEqual(undefined, pes:whereis_name(Id)),
    ok.

register_monitored_process_shutdown(_Config) ->
    TestPid = ?TEST_PROCESS(1000),
    Id = <<"proc_shutdown">>,
    ?assertEqual(yes, pes:register_name(Id, TestPid)),
    exit(TestPid, shutdown),
    % need some to to process down messages
    ct:sleep(10),
    ?assertEqual(undefined, pes:whereis_name(Id)),
    ok.

register_died_process(_Config) ->
    TestPid = ?TEST_PROCESS(0),
    Id = <<"proc_alrady_died">>,
    ct:sleep(1),
    ?assertEqual(no, pes:register_name(Id, TestPid)),
    ok.

register_guard_process_died(_Config) ->
    TestPid = ?TEST_PROCESS(1000),
    Id = <<"procs_reg_guard_died_1">>,
    ?assertEqual(yes, pes:register_name(Id, TestPid)),
    {ok, _Term, {TestPid, GuardPid, _Ts}} = pes_promise:await(pes_server_sup:read(node(), Id)),
    exit(GuardPid, kill),
    % need to wait the timeout threshold time
    ct:sleep((pes_cfg:heartbeat() * 3) + 5),
    ?assertEqual(undefined, pes:whereis_name(Id)),
    ok.

send_undefined(_Config) ->
    % non exist
    ?assertException(exit, {badarg, {unknown_reg_pid, test}}, pes:send(unknown_reg_pid, test)),

    % stopped
    yes = pes:register_name(send_undefined, ?TEST_PROCESS(1000)),
    ok = pes:unregister_name(send_undefined),
    ?assertException(exit, {badarg, {send_undefined, test}}, pes:send(send_undefined, test)),

    % died
    yes = pes:register_name(send_undefined2, ?TEST_PROCESS(6)),
    ct:sleep(15),
    ?assertException(exit, {badarg, {send_undefined2, test}}, pes:send(send_undefined2, test)).

send_ok(_Config) ->
    S = self(),
    Id = <<"send_ok">>,
    Pid = spawn(fun() ->
        receive
            Msg -> S ! {echo, Id, self(), Msg}
        end
    end),
    yes = pes:register_name(Id, Pid),
    pes:send(Id, test_msg),
    receive
        {echo, Id, Pid, EchoMsg} ->
            ?assertEqual(test_msg, EchoMsg)
    after 1000 ->
        ?assert(false, "echo message not came back")
    end.

unregister_undefined(_Config) ->
    ?assertEqual(ok, pes:unregister_name(not_registered)).

update_undefined(_Config) ->
    Id = <<"up_reg100_nf">>,
    TestPid = ?TEST_PROCESS(1000),
    ?assertEqual({error, not_found}, pes:update(Id, TestPid)).

update_ok(_Config) ->
    TestPidA = ?TEST_PROCESS(150),
    TestPidB = ?TEST_PROCESS(250),
    Id = <<"up_reg100">>,
    ?assertEqual(yes, pes:register_name(Id, TestPidA)),
    ?assertEqual(TestPidA, pes:whereis_name(Id)),
    % same pid test
    ?assertEqual(ok, pes:update(Id, TestPidA)),

    ?assertEqual(ok, pes:update(Id, TestPidB)),
    ?assertEqual(TestPidB, pes:whereis_name(Id)),
    % wait until the process A dies
    ct:sleep(150),
    ?assertEqual(TestPidB, pes:whereis_name(Id)),
    ?assertEqual(ok, pes:unregister_name(Id)),
    ct:sleep(50),
    ?assertEqual(undefined, pes:whereis_name(Id)).

repair(_Config) ->
    Id = repair_test,
    Tp = ?TEST_PROCESS(1000),
    fake_entry(node(), Id, 2, ?TEST_PROCESS(10)),
    {nack, {Server, OldTerm}} = pes_call(commit, [node(), Id, 3, test2]),
    pes_call(repair, [Server, Id, OldTerm, {3, self()}, {Tp, self(), pes_time:now()}]),
    ?assertEqual(ack, pes_call(commit, [node(), Id, 3, test2])),
    ok.

cfg_set(_Config) ->
    pes_cfg:set(cleanup_period_time, 1000),
    % gossiping is async so we need to wait a bit to the propagated data
    timer:sleep(15),
    ?assertEqual(1000, pes_cfg:get(cleanup_period_time, 2000)),

    % default with auto set
    ?assertEqual(foo_bar, pes_cfg:get(test_data, foo_bar)),
    % fetch from cache
    ?assertEqual(foo_bar, pes_cfg:get(test_data, foo_bar2)),
    % gossiping is async so we need to wait a bit to the propagated data
    timer:sleep(15),
    ?assertEqual(foo_bar, pes_cfg:get(test_data, foo_bar2)),

    pes_cfg:set(test_data, foo_bar22),
    % gossiping is async so we need to wait a bit to the propagated data
    timer:sleep(15),
    ?assertEqual(foo_bar22, pes_cfg:get(test_data, foo_bar2)),

    ok = simple_gossip:set(undefined),
    % gossiping is async so we need to wait a bit to the propagated data
    timer:sleep(15),
    ?assertEqual(foo_bar2, pes_cfg:get(test_data_undef, foo_bar2)),
    % gossiping is async so we need to wait a bit to the propagated data
    timer:sleep(15),
    ?assertEqual(foo_bar2, pes_cfg:get(test_data_undef, foo_bar2)),

    simple_gossip:set(undefined),
    % gossiping is async so we need to wait a bit to the propagated data
    timer:sleep(15),
    pes_cfg:set(test_data_undef_foo, bar),
    % gossiping is async so we need to wait a bit to the propagated data
    timer:sleep(15),
    ?assertEqual(bar, pes_cfg:get(test_data_undef_foo, foo_bar2)),
    ok.

clean(_Config) ->
    [pes:register_name("a_" ++ integer_to_list(I), ?TEST_PROCESS(100)) || I <- lists:seq(1, 100)],
    InitialMemory = erlang:memory(ets),
    OrigTimeout = pes_cfg:get(cleanup_period_time, 5000),
    Threshold = pes_cfg:get(delete_time_threshold, 5000),
    pes_cfg:set(cleanup_period_time, 100),
    pes_cfg:set(delete_time_threshold, 100),
    ct:sleep(OrigTimeout + 100),
    pes_cfg:set(cleanup_period_time, OrigTimeout),
    pes_cfg:set(delete_time_threshold, Threshold),
    ?assert(erlang:memory(ets) < InitialMemory).

stat(_Config) ->
    [
        {[registrar, active], ActiveRegistrarCount},
        {[registrar, response_time], _},
        {[registrar, start_rate], _},
        {[server, request_count], _},
        {[server, ack], _},
        {[server, nack], _},
        {[lookup, response_time], _},
        {[server, repair], _}
    ] = pes:stat(),
    yes = pes:register_name(send_undefined, ?TEST_PROCESS(1000)),
    [
        {[registrar, active], ActiveRegistrarCount2},
        {[registrar, response_time], _},
        {[registrar, start_rate], _},
        {[server, request_count], _},
        {[server, ack], _},
        {[server, nack], _},
        {[lookup, response_time], _},
        {[server, repair], _}
    ] = pes:stat(),
    ?assert(ActiveRegistrarCount2 > ActiveRegistrarCount).

pes_server_state(_Config) ->
    % for cover
    [Server1 | _] = pes_server_sup:servers(),
    _ = sys:get_state(Server1).

cluster_group({setup, Config}) ->
    {ok, Node1} = pes_test_cluster:start_node(node_1),
    {ok, Node2} = pes_test_cluster:start_node(node_2),
    ok = pes:join(Node1),
    ok = pes:join(Node2),
    [{nodes, [Node1, Node2]} | Config];
cluster_group({tear_down, Config}) ->
    [Node1 | _] = Nodes = proplists:get_value(nodes, Config),
    pes:leave(Node1),
    [pes_test_cluster:stop_node(Node) || Node <- Nodes].

register_no_majority(Config) ->
    [NodeA, NodeB] = proplists:get_value(nodes, Config),
    NodeC = node(),
    Id = <<"no_consensus">>,
    % pre set 3 different data on 3 different node

    TestPid1 = ?TEST_PROCESS(1000),
    TestPid2 = ?TEST_PROCESS(1000),
    TestPid3 = ?TEST_PROCESS(1000),

    fake_entry(NodeA, Id, 1, TestPid1),
    fake_entry(NodeB, Id, 1, TestPid2),
    fake_entry(NodeC, Id, 1, TestPid3),

    ActualProc = ?TEST_PROCESS(1000),

    ?assertEqual(yes, pes:register_name(Id, ActualProc)),
    ?assertEqual(ActualProc, pes:whereis_name(Id)),
    ok.

register_one_node_not_up_to_date(Config) ->
    [NodeA, NodeB] = proplists:get_value(nodes, Config),
    NodeC = node(),
    Id = <<"not_up_to_data">>,
    % pre set 3 different data on 3 different node
    GuardPidA = ?TEST_PROCESS(10),
    GuardPidB = ?TEST_PROCESS(1000),

    TestPidA = ?TEST_PROCESS(1000),
    TestPidB = ?TEST_PROCESS(1000),

    fake_entry(NodeA, Id, 1, GuardPidA, TestPidA, pes_time:now() - 1200),
    fake_entry(NodeB, Id, 2, GuardPidB, TestPidB, pes_time:now()),
    fake_entry(NodeC, Id, 2, GuardPidB, TestPidB, pes_time:now()),

    ActualProc = ?TEST_PROCESS(1000),

    ?assertEqual(no, pes:register_name(Id, ActualProc)),
    ?assertEqual(TestPidB, pes:whereis_name(Id)),
    ok.

register_previous_record_expired(Config) ->
    [NodeA, NodeB] = proplists:get_value(nodes, Config),
    NodeC = node(),
    Id = <<"prev_expired">>,
    % pre set 3 different data on 3 different node
    GuardPidA = ?TEST_PROCESS(0),
    TestPidA = ?TEST_PROCESS(0),

    Expired = pes_time:now() - 50000,

    fake_entry(NodeA, Id, 1, GuardPidA, TestPidA, Expired),
    fake_entry(NodeB, Id, 2, GuardPidA, TestPidA, Expired),
    fake_entry(NodeC, Id, 2, GuardPidA, TestPidA, Expired),
    ct:sleep(1),
    ActualProc = ?TEST_PROCESS(1000),

    ?assertEqual(undefined, pes:whereis_name(Id)),
    ?assertEqual(yes, pes:register_name(Id, ActualProc)),
    ?assertEqual(ActualProc, pes:whereis_name(Id)),
    ok.

register_previous_record_expired_but_alive(Config) ->
    [NodeA, NodeB] = proplists:get_value(nodes, Config),
    NodeC = node(),
    Id = <<"prev_expired_but_alive">>,
    % pre set 3 different data on 3 different node
    GuardPidA = ?TEST_PROCESS(0),
    TestPidA = ?TEST_PROCESS(1000),

    Expired = pes_time:now() - 50000,

    fake_entry(NodeA, Id, 2, GuardPidA, TestPidA, Expired),
    fake_entry(NodeB, Id, 2, GuardPidA, TestPidA, Expired),
    fake_entry(NodeC, Id, 2, GuardPidA, TestPidA, Expired),
    ActualProc = ?TEST_PROCESS(1000),

    ?assertEqual(undefined, pes:whereis_name(Id)),
    ?assertEqual(no, pes:register_name(Id, ActualProc)),
    ok.

reallocate_guard_process(Config) ->
    [NodeA, _] = proplists:get_value(nodes, Config),
    Id = <<"reallocate_id">>,
    TestPidA = ?TEST_PROCESS(1000),

    ?assertEqual(yes, pes:register_name(Id, TestPidA)),
    ?assertEqual(TestPidA, pes:whereis_name(Id)),
    {ok, {_, GuardPidA}} = pes:lookup(Id),
    ?assertEqual(node(), node(GuardPidA)),

    TestPidB = ?TEST_PROCESS(NodeA, 1000),
    ok = pes:update(Id, TestPidB),
    ?assertEqual(TestPidB, pes:whereis_name(Id)),

    {ok, {_, GuardPidB}} = pes:lookup(Id),
    ?assertEqual(NodeA, node(GuardPidB)),

    CNodes = [node() | proplists:get_value(nodes, Config)],

    ?assertEqual(
        lists:sort(CNodes),
        lists:sort(pes:nodes())
    ),

    ok.

repair_multiple_processes(Config) ->
    pes_cfg:set(heartbeat, 50),
    ct:sleep(10),
    [NodeA, NodeB] = proplists:get_value(nodes, Config),
    Id1 = repair_test_m,

    Tp1 = ?TEST_PROCESS(1500),
    Tp2 = ?TEST_PROCESS(NodeA, 1500),
    Tp3 = ?TEST_PROCESS(NodeB, 1500),

    ?assertEqual(yes, pes:register_name(Id1, Tp1)),
    fake_entry(NodeA, Id1, 2, Tp2),
    fake_entry(NodeB, Id1, 3, Tp3),
    % wait for monitor kick begin
    ct:sleep(pes_cfg:heartbeat() * 2),

    % no majority, registrar should stop
    ?assertEqual({[undefined, undefined, undefined], []}, rpc:multicall(pes, whereis_name, [Id1])),

    Id2 = repair_test_m2,
    Tp1n = ?TEST_PROCESS(1500),
    Tp2n = ?TEST_PROCESS(NodeA, 1500),
    Tp3n = ?TEST_PROCESS(NodeB, 1),

    ?assertEqual(yes, pes:register_name(Id2, Tp1n)),
    fake_entry(NodeA, Id2, 3, Tp2n),
    fake_entry(NodeB, Id2, 3, Tp3n),
    % wait fot tp3n die
    ct:sleep(10),

    ?assertEqual({[Tp1n, Tp1n, Tp1n], []}, rpc:multicall(pes, whereis_name, [Id2])),
    % should repair kicks in
    ct:sleep(pes_cfg:heartbeat() * 2),

    RecordCheck = fun(Node, Id, Value) ->
        case pes_promise:await(pes_server_sup:read(Node, Id)) of
            {ok, _, {ReadPid1, _, _}} ->
                S = lists:flatten(
                    io_lib:format("~s", [
                        io_lib:format("~p should be ~p instead of ~p (~p) on server ~p", [
                            Id, Value, ReadPid1, node(ReadPid1), Node
                        ])
                    ])
                ),
                ?assertEqual(Value, ReadPid1, S);
            _ ->
                ?assert(false, "no proper value in server")
        end
    end,

    % all nodes agree on the same value Tp2 killed lately by it's own registar
    ?assertEqual({[Tp1n, Tp1n, Tp1n], []}, rpc:multicall(pes, whereis_name, [Id2])),
    RecordCheck(NodeB, Id2, Tp1n),

    pes_cfg:set(heartbeat, 1000),
    ok.

pes_call(Function, Args) ->
    pes_promise:await(apply(pes_server_sup, Function, Args)).

fake_entry(Node, Id, Term, Pid) ->
    fake_entry(Node, Id, Term, ?TEST_PROCESS(0), Pid, pes_time:now()).

fake_entry(Node, Id, Term, GuardProcess, Pid, Ts) ->
    S = self(),
    P = spawn_link(fun() ->
        pes_server_sup:prepare(Node, Id, {Term, GuardProcess}),
        pes_server_sup:commit(Node, Id, {Term, GuardProcess}, {Pid, GuardProcess, Ts}),
        S ! {self(), done}
    end),
    receive
        {P, done} -> ok
    end.
