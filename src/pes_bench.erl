-module(pes_bench).

%% API
-export([test/4, test/3, on_all_nodes/4]).

on_all_nodes(Concurrency, KeySpace, ProcessMaxAliveTime, Count) ->
  Nodes = ['1@Peters-MacBook-Pro', '2@Peters-MacBook-Pro', '3@Peters-MacBook-Pro'],
  [net_kernel:connect_node(N) || N <- Nodes],
  rpc:multicall(persistent_term, put, [{pes_cluster, nodes}, Nodes]),
  rpc:multicall(?MODULE, test, [Concurrency, KeySpace, ProcessMaxAliveTime, Count]).

test(Concurrency, KeySpace, ProcessMaxAliveTime, Count) ->
  P = [spawn_monitor(fun() -> test(KeySpace, ProcessMaxAliveTime, Count) end)
    || _ <- lists:seq(1, Concurrency)],
  wait_for_ready(P).

test(KeySpace, ProcessMaxAliveTime, Count) ->
  Process = fun() -> spawn(fun() -> sleep(ProcessMaxAliveTime) end) end,
  {Time, R} =
    timer:tc(fun() ->
      [pes:register_name(100000+rand:uniform(KeySpace), Process()) || _I <- lists:seq(1, Count)]
             end),
  io:format(user, "~n time: ~p C: ~p~n", [Time, Count]),
  io:format(user, "RPS: ~p~n", [Count / ((Time / 1000) / 1000)]),
  R.

sleep({fix, Time}) ->
  timer:sleep(Time);
sleep(Time) ->
  sleep({fix, rand:uniform(Time)}).

wait_for_ready([]) ->
  ok;
wait_for_ready([{Pid, Ref} | Rest]) ->
  receive
    {'DOWN', Ref, process, Pid, _} ->
      wait_for_ready(Rest)
  end.
