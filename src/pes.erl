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
-define(DEFAULT_RETRY_COUNT, 3).

%% API
-export([register_name/2, unregister_name/1, whereis_name/1, send/2,
  join/1, leave/1, stat/0]).

-spec register_name(Name, Pid) -> 'yes' | 'no' when
  Name :: term(),
  Pid :: pid().
register_name(Name, Pid) when is_pid(Pid) ->
  case pes_registrar:register(Name, Pid) of
    registered ->
      yes;
    {error, {already_registered, _}} ->
      no;
    Reason ->
      logger:notice("Could not register ~p name: ~p", [Name, Reason]),
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
  lookup(Name, ?DEFAULT_RETRY_COUNT).

-spec lookup(term(), non_neg_integer()) ->
  undefined | {ok, {Pid :: pid(), GuardPid :: pid()}} | {error, timeout | no_consensus}.
lookup(Name, Retry) ->
  lookup(Name, Retry, ?DEFAULT_TIMEOUT).

-spec lookup(term(), non_neg_integer(), pos_integer() | infinity) ->
  undefined | {ok, {Pid :: pid(), GuardPid :: pid()}} | {error, timeout | no_consensus}.
lookup(Name, Retry, Timeout) ->
  Parent = self(),
  Gatherer = spawn_link(fun() ->
    StartTime = erlang:system_time(microsecond),
    Nodes = pes_cluster:nodes(),
    Majority = (length(Nodes) div 2) + 1,
    Promises = [pes_server_sup:read(Node, Name) || Node <- Nodes],
    Response = wait_for_responses(Majority, Promises, #{}),
    erlang:send(Parent, {'$reply', self(), Response}),
    pes_stat:update([lookup, response_time], erlang:system_time(microsecond) - StartTime)
  end),
  receive
    {'$reply', Gatherer, {ok, _Term, {Pid, GuardPid}}} ->
      {ok, {Pid, GuardPid}};
    {'$reply', Gatherer, {ok, _Term, undefined}} ->
      undefined;
    {'$reply', Gatherer, not_found} ->
      undefined;
    {'$reply', Gatherer, {error, no_consensus}} when Retry > 0 ->
      timer:sleep(2 + rand:uniform(8)),
      lookup(Name, Retry - 1);
    {'$reply', Gatherer, {error, no_consensus}} ->
      {error, no_consensus}
  after Timeout ->
    exit(Gatherer, kill),
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

-spec join(node()) -> ok | {error, term()}.
join(Node) ->
  pes_cluster:join(Node).

-spec leave(node()) -> ok.
leave(Node) ->
  pes_cluster:leave(Node).

-spec stat() -> [{list(atom()), number()}].
stat() ->
  pes_stat:stat().