-module(prop_pes).
-include_lib("proper/include/proper.hrl").

%% Model Callbacks
-export([command/1, initial_state/1, next_state/3,
  precondition/2, postcondition/3, cut_connections/1, fix_connections/1]).

%%%%%%%%%%%%%%%%%%
%%% PROPERTIES %%%
%%%%%%%%%%%%%%%%%%
prop_test() ->
  application:stop(pes),
  io:format(user, "~nStopping net kernel", []),
  application:stop(net_kernel),
  io:format(user, "~nKilling epmd", []),
  _ = os:cmd("kill -9 $(ps x | grep epmd | grep -v grep | awk '{print $1}')"),
  io:format(user, "~nStart empdpxy on port 4369", []),
  {ok, _} = epmdpxy:start(4369),
  io:format(user, "~nStarting netkernel", []),
  net_kernel:start([proper_0]),
  {ok, _} = application:ensure_all_started(pes),
  io:format(user, "~nStarting slave nodes... ", []),
  {ok, NodeA} = start_node(node_a),
  io:format(user, "~p", [NodeA]),
  {ok, NodeB} = start_node(node_b),
  io:format(user, ", ~p", [NodeB]),
  {ok, NodeC} = start_node(node_c),
  io:format(user, ", ~p", [NodeC]),
  Nodes = [NodeA, NodeB, NodeC],
  io:format(user, " done", []),

  TickTime = 5,
  io:format(user, "~nNet tick time se to ~p", [TickTime]),
  rpc:multicall([node() | Nodes], net_kernel, set_net_ticktime, [TickTime, 15]),

  rpc:multicall([node() | Nodes], application, set_env, [pes, process_timeout, 2000]),
  rpc:multicall([node() | Nodes], application, set_env, [pes, heartbeat, 1000]),

  rpc:multicall([node() | Nodes], application, set_env, [pes, delete_time_threshold, 10000]),

  [OnNode | TailNodes] = Nodes,
  [rpc:call(OnNode, pes, join, [Node]) || Node <- TailNodes],

  %io:format(user, "~n", []),
  %{group_leader, GLPid} = erlang:process_info(self(), group_leader),
  %set_group_leader(NodeA, GLPid),
  %set_group_leader(NodeB, GLPid),
  %set_group_leader(NodeC, GLPid),

  io:format(user, "~n~n", []),
  ?FORALL(Cmds, commands(?MODULE, initial_state(#{nodes => Nodes})),
          begin
            {History, State, Result} = run_commands(?MODULE, Cmds),
            ?WHENFAIL(io:format("History: ~p\nState: ~p\nResult: ~p\n",
                                [History,State,Result]),
                      aggregate(extract_command_names(Cmds), Result =:= ok))
          end).

extract_command_names({Cmds, L}) ->
  lists:flatten([extract_command_names(Cmds)|[ extract_command_names(Cs) || Cs <- L ]]);
extract_command_names({set, _Var, Call}) ->
  extract_command_names(Call);
extract_command_names({call, rpc, call, [Node, Module, Fn, Args]}) ->
  {{on_node, Node}, Module, Fn, length(Args)};
extract_command_names({call, M, F, Args}) ->
  {M, F, length(Args)};
extract_command_names(Cmds) ->
  [extract_command_names(Cmd) || {set, _Var, {call, _M, _F, _Args}} = Cmd <- Cmds].


%%%%%%%%%%%%%
%%% MODEL %%%
%%%%%%%%%%%%%
%% @doc Initial model value at system start. Should be deterministic.
initial_state(#{nodes := Nodes}) ->
  #{nodes => Nodes, processes => #{}, islands => []}.

%% @doc List of possible commands to run against the system
command(#{nodes := Nodes} = State) ->
    frequency([
      {90, {call, rpc, call, [oneof(Nodes), pes, register_name, [key(), process()]]}},
      {40, {call, rpc, call, [oneof(Nodes), pes, unregister_name, [key()]]}},
      {80, {call, rpc, call, [oneof(Nodes), pes, whereis_name, [key()]]}}%,
      %{26, {call, ?MODULE, fix_connections, [State]}},
      %{1, {call, ?MODULE, cut_connections, [islands(Nodes)]}}
    ]).

%% @doc Determines whether a command should be valid under the
%% current state.
precondition(_State, {call, _Mod, _Fun, _Args}) ->
  true.

%% @doc Given the state `State' *prior* to the call
%% `{call, Mod, Fun, Args}', determine whether the result
%% `Res' (coming from the actual system) makes sense.
postcondition(#{processes := Procs}, {call, _Mod, register, [Key, Pid]}, {ok, Pid}) ->
  Threshold = 1000,
  Now = erlang:system_time(millisecond),
  case maps:get(Key, Procs, not_found) of
    not_found ->
      true;
    {_RPid, Ts} when Ts+Threshold =< Now ->
      true;
    {RPid, Ts} ->
      io:format(user, "There is a process ~p for key ~p already registered with Ts: ~p", [Key, RPid, Ts]),
      false
  end;
postcondition(_State, {call, _Mod, _Fun, _Args}, _Res) ->
  true.

%% @doc Assuming the postcondition for a call was true, update the model
%% accordingly for the test to proceed.
next_state(#{processes := Procs} = State, {ok, Pid}, {call, _Mod, register, [Key, Pid]}) ->
  State#{processes => Procs#{Key => {Pid, erlang:system_time(millisecond)}}};

next_state(#{islands := Islands} = State, ok, {call, ?MODULE, cut_connections, NewIslands}) ->
  State#{islands => [NewIslands | Islands]};
next_state(State, ok, {call, ?MODULE, fix_connections, _}) ->
  State#{islands => []};
next_state(State, _, {call, _Mod, _Fun, _Args}) ->
  State.

key() ->
  resize(10, term()).

process() ->
  ?LAZY(?LET(Alive, integer(1, 5000), begin spawn(fun() -> timer:sleep(Alive) end) end)).

islands(Nodes) ->
  ?LET(Split, integer(1, length(Nodes)-1), begin lists:split(Split, Nodes) end).

start_node(Name) ->
  Opts = [{monitor_master, true}, {kill_if_fail, true}],
  case ct_slave:start(Name, Opts) of
    {error, started_not_connected, Node} ->
      net_kernel:connect_node(Node),
      ct_slave:stop(Node),
      start_node(Name);
    {error, already_started, Node} ->
      ct_slave:stop(Node),
      start_node(Name);
    {ok, Node} ->
      ok = rpc:call(Node, code, add_paths, [code:get_path()]),
      {ok, _} = rpc:call(Node, application, ensure_all_started, [pes]),
      true = net_kernel:connect_node(Node),
      {ok, Node}
  end.

cut_connections({IslandA, IslandB}) ->
  cut(IslandA, IslandB).

fix_connections(#{islands := Islands}) ->
  [fix(IslandA, IslandB) || {IslandA, IslandB} <- Islands].

cut(IslandA, IslandB) ->
  %ct:pal("cut cables between ~p <-> ~p", [IslandA, IslandB]),
  ok = epmdpxy:cut_cables(IslandA, IslandB),
  %timer:sleep(5),
  ok.

fix(IslandA, IslandB) ->
  %ct:pal("fix cables between ~p <-> ~p", [IslandA, IslandB]),
  ok = epmdpxy:fix_cables(IslandA, IslandB),
  %timer:sleep(5),
  ok.