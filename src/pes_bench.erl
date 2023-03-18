-module(pes_bench).

-behaviour(gen_server).

%% API
-export([test/4, test/3, on_all_nodes/4, alive_process_counter/0,
  init/1, handle_call/3, handle_cast/2, handle_info/2, sleep/1]).

on_all_nodes(Concurrency, KeySpace, ProcessMaxAliveTime, Count) ->
  Nodes = ['1@Peters-MacBook-Pro', '2@Peters-MacBook-Pro', '3@Peters-MacBook-Pro'],
  [pes_cluster:join(N) || N <- Nodes],
  %rpc:multicall(net_kernel, set_net_ticktime, [5, 20]),
  rpc:multicall(?MODULE, test, [Concurrency, KeySpace, ProcessMaxAliveTime, Count]).

test(Concurrency, KeySpace, ProcessMaxAliveTime, Count) ->
  P = [spawn_monitor(fun() -> test(KeySpace, ProcessMaxAliveTime, Count) end)
    || _ <- lists:seq(1, Concurrency)],
  wait_for_ready(P).

test(KeySpace, ProcessMaxAliveTime, Count) ->
  %Process = fun() -> spawn(fun() ->
  %                          case erlang:whereis(alive_process_counter) of
  %                            undefined ->
  %                              ok;
  %                            Pid ->
  %                              Pid ! self()
  %                          end,
  %                          sleep(ProcessMaxAliveTime)
  %                         end) end,
  {Time, R} =
    timer:tc(fun() ->
      %[pes:register_name(100000 + rand:uniform(KeySpace), Process()) || _I <- lists:seq(1, Count)]
      [gen_server:start({via, pes, 100000 + rand:uniform(KeySpace)}, ?MODULE, ProcessMaxAliveTime, []) || _I <- lists:seq(1, Count)]
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

alive_process_counter() ->
  spawn(fun() ->
    erlang:register(alive_process_counter, self()),
    self() ! tick,
    alive_process_counter(0)
  end).

alive_process_counter(Count) ->
  receive
    Pid when is_pid(Pid) ->
      erlang:monitor(process, Pid),
      alive_process_counter(Count+1);
    tick when Count > 0 ->
      io:format(user, "alive process count: ~p~n", [Count]),
      erlang:send_after(1000, self(), tick),
      alive_process_counter(Count);
    tick ->
      erlang:send_after(1000, self(), tick),
      alive_process_counter(Count);
    {'DOWN', _Ref, process, _Pid, _} ->
      alive_process_counter(Count-1)
  end.

%% *** TEST gen server ***
init(SleepTime) ->
  case erlang:whereis(alive_process_counter) of
    undefined ->
      ok;
    Pid ->
      Pid ! self()
  end,
  ActualSleep = case SleepTime of
                  {fix, Time} ->
                    Time;
                  _ ->
                    rand:uniform(SleepTime)
                end,
  {ok, state, ActualSleep}.

handle_call(_Request, _From, _State) ->
  erlang:error(not_implemented).

handle_cast(_Request, _State) ->
  erlang:error(not_implemented).

handle_info(timeout, State) ->
  {stop, normal, State}.