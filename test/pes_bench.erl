-module(pes_bench).

-behaviour(gen_server).

%% API
-export([test/4, test/3, on_all_nodes/5, alive_process_counter/0,
  init/1, handle_call/3, handle_cast/2, handle_info/2, sleep/1, load/5, rep/6]).


rep(Concurrency, KeySpace, ProcessMaxAliveTime, TimeR, Rate, Time) ->
  spawn(fun() ->
    F = fun R () ->
          rpc:multicall([node() | nodes()], pes_bench, alive_process_counter, []),
          rpc:multicall([node() | nodes()], pes_bench, load, [Concurrency, KeySpace, ProcessMaxAliveTime, TimeR, Rate]),
          R()
        end,
    timer:apply_after(Time, erlang, exit, [self(), kill]),
    F()
  end).

on_all_nodes(Nodes, Concurrency, KeySpace, ProcessMaxAliveTime, Count) ->
  [pes:join(N) || N <- Nodes],
  rpc:multicall(?MODULE, test, [Concurrency, KeySpace, ProcessMaxAliveTime, Count]).

load(Concurrency, KeySpace, ProcessMaxAliveTime, Time, Rate) ->
  EndTime = erlang:system_time(millisecond)+Time,
  P = [spawn_monitor(fun() -> load(KeySpace, ProcessMaxAliveTime, EndTime, Rate) end)
    || _ <- lists:seq(1, Concurrency)],
  wait_for_ready(P).

load(KeySpace, ProcessMaxAliveTime, EndTime, Rate) ->
  case EndTime < erlang:system_time(millisecond) of
    true ->
      ok;
    _ ->
      gen_server:start({via, pes, 100000 + rand:uniform(KeySpace)}, ?MODULE, ProcessMaxAliveTime, []),
      timer:sleep(rand:uniform(Rate)),
      load(KeySpace, ProcessMaxAliveTime, EndTime, Rate)
  end.

test(Concurrency, KeySpace, ProcessMaxAliveTime, Count) ->
  P = [spawn_monitor(fun() -> test(KeySpace, ProcessMaxAliveTime, Count) end)
    || _ <- lists:seq(1, Concurrency)],
  wait_for_ready(P).

test(KeySpace, ProcessMaxAliveTime, Count) ->
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
    case erlang:whereis(alive_process_counter) of
      undefined ->
        erlang:register(alive_process_counter, self()),
        self() ! tick,
        alive_process_counter(0);
      _ ->
        already_started
    end
  end).

alive_process_counter(Count) ->
  receive
    Pid when is_pid(Pid) ->
      erlang:monitor(process, Pid),
      alive_process_counter(Count+1);
    tick when Count > 0 ->
      io:format(user, "~n*** * Stat * ***~n~p~n", [pes:stat()]),
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