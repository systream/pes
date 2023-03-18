%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2023, systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(pes).

-include_lib("pes_promise.hrl").

-define(DEFAULT_TIMEOUT, 5000).

%% API
-export([register_name/2, unregister_name/1, whereis_name/1, send/2,
         join/1, leave/1]).

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
    {ok, {_Pid, GuardPid}} ->
      pes_registrar:unregister(GuardPid)
  end.

-spec whereis_name(Name) -> pid() | 'undefined' when
  Name :: term().
whereis_name(Name) ->
  case lookup(Name) of
    {ok, {Pid, _GuardPid}} ->
      Pid;
    undefined ->
      undefined
  end.

-spec lookup(term()) ->
  undefined | {ok, {Pid :: pid(), GuardPid :: pid()}} |
  {error, timeout | no_consensus}.
lookup(Name) ->
  lookup(Name, 3).

-spec lookup(term(), non_neg_integer()) ->
  undefined | {ok, {Pid :: pid(), GuardPid :: pid()}} |{error, timeout | no_consensus}.
lookup(Name, Retry) ->
  Parent = self(),
  P = spawn_link(fun() ->
    Nodes = pes_cluster:nodes(),
    Majority = (length(Nodes) div 2) + 1,
    Promises = [pes_proxy:read(Node, Name) || Node <- Nodes],
    Parent ! {'$reply', self(), wait_for_responses(Majority, Promises, #{})}
  end),
  receive
    {'$reply', P, {ok, _Term, {Pid, GuardPid}}} ->
      {ok, {Pid, GuardPid}};
    {'$reply', P, {ok, _Term, undefined}} ->
      undefined;
    {'$reply', P, not_found} ->
      undefined;
    {'$reply', P, {error, no_consensus}} when Retry > 0 ->
      timer:sleep(5),
      lookup(Name, Retry-1);
    {'$reply', P, {error, no_consensus}} ->
      {error, no_consensus}
  after ?DEFAULT_TIMEOUT ->
    exit(P, kill),
    {error, timeout}
  end.

wait_for_responses(_Majority, [], _Replies) ->
  {error, no_consensus};
wait_for_responses(Majority, Promises, Replies) ->
  receive
    #promise_reply{ref = Ref, result = Result} = Reply ->
      pes_promise:resolved(Reply),
      NewResult = case Result of
                    {ok, _, undefined} -> not_found;
                    {ok, Term, {Pid, GuardPid, TimeStamp}} ->
                      case pes_time:is_expired(TimeStamp) of
                        true ->
                          not_found;
                        _ ->
                          {ok, Term, {Pid, GuardPid}}
                      end;
                    Else -> Else
                  end,
      ResultCount = maps:get(NewResult, Replies, 0) + 1,
      case ResultCount >= Majority of
        true ->
          NewResult;
        false ->
          wait_for_responses(Majority,
                             lists:delete({promise, Ref}, Promises),
                             Replies#{NewResult => ResultCount})
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

-spec join(node()) -> ok.
join(Node) ->
  pes_cluster:join(Node).

-spec leave(node()) -> ok.
leave(Node) ->
  pes_cluster:leave(Node).