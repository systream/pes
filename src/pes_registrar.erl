%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2023, systream
%%% @doc
%%% Process who responsible for registering (proposer) and monitoring process.
%%% @end
%%%-------------------------------------------------------------------
-module(pes_registrar).
-behaviour(gen_statem).

-compile({no_auto_import, [is_process_alive/1]}).
-include_lib("pes_promise.hrl").

-define(DEFAULT_HEARTBEAT, 8048).
-define(DEFAULT_TIMEOUT, 5000).

%-define(trace(Msg, Args), io:format(user, Msg ++ "~n", Args)).
-define(TRACE(Msg, Args, Id), logger:warning(Msg, Args, #{node => node(), cid => Id,
                                                          state_name => erlang:get(state_name)})).

%% API
-export([register/2, unregister/1]).

%% gen_statem callbacks
-export([init/1, handle_event/4, callback_mode/0]).


-record(state, {
  id :: term(),
  pid :: pid(),
  nodes :: [node()],
  majority :: pos_integer(),
  caller :: pid(),
  term = 1 :: pos_integer(),
  promises = [] :: [{promise, reference()}],
  replies = #{} :: #{}
}).

-opaque state() :: #state{}.
-export_type([state/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec register(term(), pid()) ->
  registered |
  {error, {could_not_register, Reason :: term()} | {already_registered, pid()} | timeout | term()}.
register(Id, Value) ->
  case gen_statem:start_monitor(?MODULE, {Id, Value, self()}, []) of
    {ok, {ServerPid, Ref}} ->
       receive
         {'$reply', ServerPid, Result} ->
           erlang:demonitor(Ref, [flush]),
           Result;
         {'DOWN', Ref, process, ServerPid, Reason} ->
           {error, Reason}
       after ?DEFAULT_TIMEOUT ->
         exit(ServerPid, kill),
         erlang:demonitor(Ref, [flush]),
         {error, timeout}
       end;
    {error, _} = Error ->
      Error
  end.

-spec unregister(pid()) -> ok.
unregister(Server) ->
  gen_statem:call(Server, stop).

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

-spec init({term(), pid(), pid()}) -> {ok, reg_check, state()}.
init({Id, Pid, Caller}) ->
  %?trace("Staring FSM for ~p ", [Pid], Id),
  erlang:monitor(process, Pid),
  Nodes = pes_cluster:nodes(),
  Majority = majority(Nodes),
  {ok, reg_check, #state{id = Id, caller = Caller, pid = Pid, nodes = Nodes, majority = Majority}}.

-spec callback_mode() -> [handle_event_function | state_enter].
callback_mode() ->
  [handle_event_function, state_enter].

% target process has died so FSM not needed anymore.
handle_event(info, {'DOWN', _Ref, process, Pid, Reason}, StateName,
             #state{pid = Pid, id = Id, term = Term, nodes = Nodes} = State) ->
  % tries to unregister from catalog
  % If the server not in monitoring, or update state client should be notified
  case unregister_from_catalog(StateName, Id, Term, Nodes) of
    committed -> ok;
    not_committed -> reply(State, {error, {could_not_register, target_pid_died}})
  end,
  case Reason of
    normal -> {stop, normal, State};
    _ -> {stop, {registered_process_died, Pid, Reason, StateName}, State}
  end;
handle_event({call, From}, stop, StateName, #state{id = Id, term = Term, nodes = Nodes} = State) ->
  % tries to unregister from catalog
  %?trace("Registered process ~p down ~p --> ~p", [Pid, Reason, StateName], Id),
  % If the server not in monitoring, or update state client should be notfied
  case unregister_from_catalog(StateName, Id, Term, Nodes) of
    committed -> ok;
    not_committed -> reply(State, {error, {could_not_register, got_unregister_request}})
  end,
  gen_statem:reply(From, ok),
  {stop, normal, State};

% read
handle_event(enter, _, reg_check, #state{id = Id, nodes = Nodes} = State) ->
  %?trace("Entered reg check", [], Id),
  put(state_name, reg_check),
  {keep_state, set_promises([pes_proxy:read(Node, Id) || Node <- Nodes], State)};
handle_event(info, #promise_reply{ref = Ref, result = Response} = Reply, reg_check, State) ->
  pes_promise:resolved(Reply),
  handle_read(Ref, Response, State);
handle_event(info, {'DOWN', Ref, process, _Pid, Reason}, reg_check, State) ->
  handle_read(Ref, {error, Reason}, State);

% prepare
handle_event(enter, _, prepare, #state{id = Id, nodes = Nodes, term = Term} = State) ->
  %?trace("Entered prepare", [], Id),
  put(state_name, prepare),
  {keep_state, set_promises([prepare(Node, Id, Term) || Node <- Nodes], State)};
handle_event(info, #promise_reply{ref = Ref, result = Response} = Reply, prepare, State) ->
  pes_promise:resolved(Reply),
  handle_consensus_responses(Ref, Response, State, commit);
handle_event(info, {'DOWN', Ref, process, _Pid, Reason}, prepare, State) ->
  handle_consensus_responses(Ref, {error, Reason}, State, commit);

% commit
handle_event(enter, _, commit, #state{id = Id, nodes = Nodes, term = Term, pid = Pid} = State) ->
  %?trace("Entered commit", [], Id),
  put(state_name, commit),
  Data = {Pid, self(), pes_time:now()},
  {keep_state, set_promises([commit(Node, Id, Term, Data) || Node <- Nodes], State)};
handle_event(info, #promise_reply{ref = Ref, result = Response} = Reply, commit, State) ->
  pes_promise:resolved(Reply),
  handle_consensus_responses(Ref, Response, State, registered);
handle_event(info, {'DOWN', Ref, process, _Pid, Reason}, commit, State) ->
  handle_consensus_responses(Ref, {error, Reason}, State, registered);

% registered
handle_event(enter, _, registered, State) ->
  put(state_name, registered),
  %?trace("Entered registered", [], State#state.id),
  reply(State, registered),
  {keep_state, State#state{replies = #{}}, {state_timeout, 0, monitoring}};
handle_event(state_timeout, monitoring, registered, #state{} = State) ->
  {next_state, monitoring, State};

% heartbeat
handle_event(enter, _, monitoring, #state{id = Id, term = Term, pid = Pid} = State) ->
  %?trace("Entered monitoring", [], State#state.id),
  put(state_name, monitoring),
  Data = {Pid, self(), pes_time:now()},
  HeartBeat = application:get_env(pes, heartbeat, ?DEFAULT_HEARTBEAT) + rand:uniform(10),
  Nodes = pes_cluster:nodes(),
  NewState = set_promises([commit(Node, Id, Term, Data) || Node <- Nodes], set_nodes(State, Nodes)),
  {keep_state, NewState, [{state_timeout, HeartBeat, heartbeat}]};
handle_event(state_timeout, heartbeat, monitoring, #state{replies = Replies,
                                                          majority = Majority} = State) ->
  case evaluate_replies(Replies, Majority) of
    ack ->
      {repeat_state, State};
    Answer ->
      ?TRACE("cannot_renew timeout ~p", [Answer], State#state.id),
      {stop, cannot_renew_pid_timeout, State}
  end;
handle_event(info, #promise_reply{result = {nack, {Server, OldTerm}}} = Reply, monitoring,
             #state{id = Id, term = Term, pid = Pid} = State) ->
  % we are in monitoring phase se we can do repair because we have the majority
  repair(Server, Id, OldTerm, Term, {Pid, self(), pes_time:now()}),
  handle_event(info, Reply#promise_reply{result = nack}, monitoring, State);
handle_event(info, #promise_reply{ref = Ref, result = Response} = Reply, monitoring, State) ->
  pes_promise:resolved(Reply),
  handle_update_responses(Ref, Response, State);
handle_event(info, {'DOWN', Ref, process, _Pid, Reason}, monitoring, State) ->
  handle_update_responses(Ref, {error, Reason}, State);

% we need to just drop late messages, these messages can be dropped
handle_event(info, #promise_reply{} = Reply, _StateName, _State) ->
  pes_promise:resolved(Reply),
  %?trace("[~p] reply dropped ~p", [StateName, Reply], State#state.id),
  keep_state_and_data.


%%%===================================================================
%%% Internal functions
%%%===================================================================

handle_read(Ref, Reply, State) ->
  case evaluate_response(Ref, Reply, State) of
    {keep_state, NewState} ->
      {keep_state, NewState};
    {{no_consensus, _Replies}, NewState = #state{id = Id, pid = Pid}} ->
      wait(Id, Pid, 16),
      {next_state, prepare, increase_term(NewState)};
    {{ok, GetTerm, {Pid, _GuardPid, Timestamp}}, NewState} ->
      case pes_time:is_expired(Timestamp) of
        true -> % expired
          case is_process_alive(Pid) of
            true ->
              reply(State, {error, {already_registered, Pid}}),
              {stop, normal, NewState};
            false ->
              {next_state, prepare, increase_term(NewState#state{term = GetTerm})};
            unknown ->
              repeat_state_and_data
          end;
        false ->
          reply(State, {error, {already_registered, Pid}}),
          {stop, normal, NewState}
      end;
    {{ok, GetTerm, undefined}, NewState = #state{term = Term}} ->
      {next_state, prepare, increase_term(NewState#state{term = max(GetTerm, Term)})};
    {not_found, NewState} ->
      {next_state, prepare, NewState}
  end.

handle_consensus_responses(Ref, Reply, State, NextState) ->
  %?trace("Handle consensus resp ~p -> ~p", [Ref, Reply], State#state.id),
  case evaluate_response(Ref, Reply, State) of
    {keep_state, NewState} ->
      {keep_state, NewState};
    {{no_consensus, _}, NewState = #state{id = Id, pid = Pid}} ->
      %?trace("consensus -> no consensus -> ~p", [NewState#state.term], NewState#state.id),
      wait(Id, Pid, 32),
      {repeat_state, increase_term(NewState)};
    {ack, NewState} ->
      %?trace("consensus -> ack -> ~p", [NewState#state.term], NewState#state.id),
      {next_state, NextState, NewState};
    {nack, NewState = #state{id = Id, pid = Pid}} ->
      % this case happening probably because other nodes trying to
      % register their process simultaneously
      wait(Id, Pid, 32),
      %?trace("consensus -> nack -> ~p", [NewState#state.term], NewState#state.id),
      {next_state, reg_check, NewState}
  end.


handle_update_responses(Ref, Reply, State) ->
  %?trace("Handle update ~p -> ~p", [Ref, Reply], State#state.id),
  case evaluate_response(Ref, Reply, State) of
    {keep_state, NewState} ->
      {keep_state, NewState};
    {ack, NewState} ->
      %?trace("consensus -> ack -> ~p", [NewState#state.term], NewState#state.id),
      {keep_state, NewState};
    {nack, NewState} ->
      % we cannot update the record, the process should die
      ?TRACE("cannot_renew ~p -> ~p -> ~p",
            [NewState#state.pid, Reply, NewState], NewState#state.id),
      exit(NewState#state.pid, kill),
      {stop, {cannot_renew_pid, no_consensus, State#state.id}, NewState};
    {{no_consensus, _}, NewState} ->
      ?TRACE("cannot_renew no consensus ~p -> ~p -> ~p",
            [NewState#state.pid, Reply, NewState], NewState#state.id),
      exit(NewState#state.pid, kill),
      {stop, {cannot_renew_pid, no_consensus, State#state.id}, NewState};
    {{error, Reason}, NewState} ->
      % we cannot update the record, the process should die
      exit(NewState#state.pid, kill),
      {stop, {cannot_renew_pid, Reason, State#state.id}, NewState}
  end.

evaluate_response(Ref, {nack, _}, State) ->
  evaluate_response(Ref, nack, State);
evaluate_response(Ref, Result, #state{replies = Replies,
                                      majority = Majority,
                                      promises = Promises} = State) ->
  case lists:member({promise, Ref}, Promises) of
    true ->
      ResultCount = maps:get(Result, Replies, 0) + 1,
      NewReplies = Replies#{Result => ResultCount},
      NewState = State#state{replies = NewReplies,
                             promises = lists:delete({promise, Ref}, Promises)},
      case ResultCount >= Majority of
        true ->
          {Result, NewState};
        false when NewState#state.promises =:= [] ->
          case evaluate_replies(NewReplies, Majority) of
            no_consensus ->
              % non of the values reached the majority
              %throw({stop, {consensus_cannot_be_reached, no_majority, State#state.id}});
              {{no_consensus, NewReplies}, NewState};
            RepliesResult ->
              {RepliesResult, NewState}
          end;
        false ->
          {keep_state, NewState}
      end;
    _ ->
      {keep_state, State}
  end.

increase_term(#state{term = Term} = State) ->
  State#state{term = Term+1}.

set_promises(Promises, State) ->
  State#state{promises = Promises, replies = #{}}.

-spec is_process_alive(pid()) -> true | false | unknown.
is_process_alive(Pid) ->
  case rpc:call(node(Pid), erlang, is_process_alive, [Pid]) of
    {badrpc, _} ->
      unknown;
    Else ->
      Else
  end.

prepare(Node, Id, Term) ->
  pes_proxy:prepare(Node, Id, encapsulate_term(Term)).

commit(Node, Id, Term, Value) ->
  pes_proxy:commit(Node, Id, encapsulate_term(Term), Value).

repair(Node, Id, OldTerm, NewTerm, Value) ->
  pes_proxy:repair(Node, Id, OldTerm, encapsulate_term(NewTerm), Value).

encapsulate_term(Term) ->
  {Term, self()}.

reply(#state{caller = Caller}, Response) ->
  erlang:send(Caller, {'$reply', self(), Response}, [nosuspend, noconnect]).

wait(Id, Pid, Rand) ->
  % Each node/pid/id combo has a weight, how much likely to run on that node,
  % this will participate on wait time to help that node to register itself
  BaseTime = erlang:phash2({node(), Pid, Id}, 1 bsl 8),
  timer:sleep(BaseTime + rand:uniform(Rand)).

evaluate_replies(Replies, Majority) ->
  maps:fold(fun(Result, VoteCount, _Acc) when VoteCount >= Majority ->
                  Result;
               (_Result, _VoteCount, Acc) ->
                  Acc
            end, no_consensus, Replies).

majority(Nodes) ->
  (length(Nodes) div 2) + 1.

set_nodes(State, Nodes) ->
  State#state{nodes = Nodes, majority = majority(Nodes)}.

unregister_from_catalog(StateName, Id, Term, Nodes)
  when StateName =:= monitoring orelse StateName =:= update_entry ->
  [commit(Node, Id, Term, undefined) || Node <- Nodes],
  committed;
unregister_from_catalog(_StateName, _Id, _Term, _Nodes) ->
  not_committed.