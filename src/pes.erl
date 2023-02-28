%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2023, systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(pes).

-include_lib("pes_promise.hrl").

%% API
-export([register_name/2, unregister_name/1, whereis_name/1, send/2]).

-spec register_name(Name, Pid) -> 'yes' | 'no' when
  Name :: term(),
  Pid :: pid().
register_name(Name, Pid) when is_pid(Pid) ->
  case pes_registrar:register(Name, Pid) of
    registered ->
      yes;
    _Reason ->
      no
  end.

-spec unregister_name(Name) -> _ when
  Name :: term().
unregister_name(Name) ->
  case lookup(Name) of
    undefined ->
      ok;
    {ok, {_Pid, GuardPid, _Ts}} ->
      pes_registrar:unregister(GuardPid)
  end.

-spec whereis_name(Name) -> pid() | 'undefined' when
  Name :: term().
whereis_name(Name) ->
  case lookup(Name) of
    {ok, {Pid, _GuardPid, _Ts}} ->
      Pid;
    undefined ->
      undefined
  end.

lookup(Name) ->
  Parent = self(),
  P = spawn_link(fun() ->
    Nodes = pes_cluster:nodes(),
    Majority = (length(Nodes) div 2) + 1,
    Promises = [pes_proxy:read(Node, Name) || Node <- Nodes],
    Parent ! {'$reply', self(), wait_for_responses(Majority, Promises, #{})}
  end),
  receive
    {'$reply', P, {ok, _Term, {Pid, GuardPid, TimeStamp}}} ->
      case pes_time:is_expired(TimeStamp) of
        true ->
          undefined;
        _ ->
          {ok, {Pid, GuardPid, TimeStamp}}
      end;
    {'$reply', P, {ok, _Term, undefined}} ->
      undefined;
    {'$reply', P, not_found} ->
      undefined
  after 5000 ->
    timeout
  end.

wait_for_responses(_Majority, [], _Replies) ->
  {error, no_consensus};
wait_for_responses(Majority, Promises, Replies) ->
  receive
    #promise_reply{ref = Ref, result = Result} = Reply ->
      pes_promise:resolved(Reply),
      ResultCount = maps:get(Result, Replies, 0)+1,
      case ResultCount >= Majority of
        true ->
          Result;
        false ->
          wait_for_responses(Majority,
                             lists:delete({promise, Ref}, Promises),
                             Replies#{Result => ResultCount})
      end;
    {'DOWN', Ref, process, _Pid, _Reason} ->
      wait_for_responses(Majority, lists:delete({promise, Ref}, Promises), Replies)

  end.


-spec send(Name, Msg) -> Pid when
  Name :: term(),
  Msg :: term(),
  Pid :: pid().
send(Name, Msg) ->
  case whereis_name(Name) of
    Pid when is_pid(Pid) ->
      Pid ! Msg,
      Pid;
    undefined ->
      exit({badarg, {Name, Msg}})
  end.