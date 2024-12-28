%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2023, systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(pes_server).

-include_lib("pes_promise.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-type id() :: term().
-type consensus_term() :: pos_integer().
-type consensus_term_proposal() :: {consensus_term(), pid()} | consensus_term().
-type value() :: term().
-type target() :: node() | {atom(), node()}.

-export_type([id/0, value/0, target/0, consensus_term/0, consensus_term_proposal/0]).

-compile({no_auto_import, [register/2, is_process_alive/1, send/2]}).

%% API
-export([prepare/3, commit/4, read/2, repair/5]).

-export([start_link/1, init/1, loop/1]).
-export([system_continue/3, system_terminate/4, system_get_state/1, system_code_change/4]).

-define(DEFAULT_CLEANUP_TIMEOUT, 5001).
-define(DEFAULT_DELETE_LIMIT, 250).
-define(DEFAULT_DELETE_THRESHOLD, 200000).

-define(TERM_STORAGE, pes_term_storage).
-define(DATA_STORAGE, pes_data_storage).

-define(SERVER, ?MODULE).

-record(state, {
  term_storage_ref :: ets:tid(),
  data_storage_ref :: ets:tid()
}).

-opaque state() :: #state{}.
-export_type([state/0]).

-spec prepare(pes_server:target(), pes_server:id(), pes_server:consensus_term_proposal()) ->
  pes_promise:promise().
prepare(Node, Id, {_, _} = Term) ->
  async(Node, {prepare, Id, Term});
prepare(Node, Id, Term) ->
  prepare(Node, Id, {Term, self()}).

-spec commit(target(), id(), consensus_term_proposal(), value()) -> pes_promise:promise().
commit(Node, Id, {_, _} = Term, Value) ->
  async(Node, {commit, Id, Term, Value});
commit(Node, Id, Term, Value) ->
  commit(Node, Id, {Term, self()}, Value).

-spec read(target(), id()) -> pes_promise:promise().
read(Node, Id) ->
  async(Node, {read, Id}).

-spec repair(target(), id(), consensus_term_proposal(), consensus_term_proposal(), value()) ->
  pes_promise:promise().
repair(Node, Id, CurrentTerm, NewTerm, Value) ->
  async(Node, {repair, Id, CurrentTerm, NewTerm, Value}).

-spec async(target(), term()) -> pes_promise:promise().
async({Server, Node}, Command) ->
  case pes_cluster:is_node_alive(Node) of
    true -> %
      pes_promise:async({Server, Node}, Command);
    false ->
      % fake promise, fake down message
      Ref = make_ref(),
      self() ! {'DOWN', Ref, process, {Server, Node}, noconnection},
      {promise, Ref}
  end.

-spec start_link(atom()) -> {ok, pid()}.
start_link(Server) ->
  proc_lib:start_link(?MODULE, init, [Server]).

-spec init(atom()) -> no_return().
init(Server) ->
  true = erlang:register(Server, self()),
  proc_lib:init_ack({ok, self()}),
  schedule_cleanup(),
  loop(#state{term_storage_ref = ets:new(?TERM_STORAGE, [protected, compressed]),
              data_storage_ref = ets:new(?DATA_STORAGE, [protected, compressed])}).

-spec loop(state()) -> no_return().
loop(State) ->
  receive
    #pes_promise_call{from = From, command = Command} ->
      pes_promise:reply(From, handle_command(Command, State)),
      pes_stat:count([server, request_count]),
      ?MODULE:loop(State);
    cleanup ->
      clean_expired_data(State),
      schedule_cleanup(),
      ?MODULE:loop(State);
    {system, From, Request} ->
      [Parent | _] = get('$ancestors'),
      sys:handle_system_msg(Request, From, Parent, ?MODULE, [], undefined)
  end.

-spec handle_command(term(), state()) -> term().
handle_command({read, Id}, #state{data_storage_ref = DSR}) ->
  case ets:lookup(DSR, Id) of
    [{Id, {Term, Value}, _Ts}] ->
      {ok, Term, Value};
    _ ->
      not_found
  end;
handle_command({prepare, Id, {Term, Server}}, #state{term_storage_ref = TSR}) ->
  case ets:lookup(TSR, Id) of
    [{Id, {StoredTerm, _StoredServer}}] when StoredTerm >= Term ->
      pes_stat:count([server, nack]),
      nack;
    _ -> % not found or StoredTerm is lower than this
      true = ets:insert(TSR, {Id, {Term, Server}}),
      pes_stat:count([server, ack]),
      ack
  end;
handle_command({commit, Id, {Term, Server}, Value}, #state{data_storage_ref = DSR,
                                                           term_storage_ref = TSR}) ->
  case ets:lookup(TSR, Id) of
    [{Id, {StoredTerm, StoredServer}}] when StoredTerm =:= Term andalso StoredServer =:= Server ->
      Now = pes_time:now(),
      true = ets:insert(DSR, {Id, {Term, Value}, Now}),
      pes_stat:count([server, ack]),
      ack;
    [{Id, {StoredTerm, StoredServer}}] ->
      pes_stat:count([server, nack]),
      {nack, {node(), {StoredTerm, StoredServer}}};
    [] ->
      pes_stat:count([server, nack]),
      {nack, {node(), not_found}}
  end;
% Commit can reply with nack and the actual term, and server data.
% To ensure in the mean time no other registration attempt were made,
% we need to send back those values.
handle_command({repair, Id, Term, {NewTermId, _} = NewTerm, Value},
                #state{data_storage_ref = DSR,
                       term_storage_ref = TSR}) ->
  pes_stat:count([server, repair]),
  Result = case ets:lookup(TSR, Id) of
             [{Id, StoredTerm}] when StoredTerm =:= Term -> ack; % the term matches
             [] when Term =:= not_found -> ack;
             _E -> nack
           end,
  case Result of
    ack ->
      true = ets:insert(TSR, {Id, NewTerm}),
      true = ets:insert(DSR, {Id, {NewTermId, Value}, pes_time:now()});
    nack ->
      ok
  end,
  Result.

system_continue(_Parent, _Deb, State) ->
  ?MODULE:loop(State).

system_terminate(Reason, _Parent, _Deb, _Chs) ->
  exit(Reason).

system_get_state(Chs) ->
  {ok, Chs}.

-spec system_code_change(State :: state(), Module :: atom(), OldVsn :: term(),
    Extra :: term()) -> {ok, NewState :: state()}.
system_code_change(State, ?MODULE, _OldVsn, _Extra) ->
  {ok, State}.

-spec clean_expired_data(state()) -> ok.
clean_expired_data(#state{data_storage_ref = DSR, term_storage_ref = TSR}) ->
  DeleteThreshold = pes_cfg:get(delete_time_threshold, ?DEFAULT_DELETE_THRESHOLD),
  Threshold = pes_time:now() - DeleteThreshold,
  DeleteLimit = pes_cfg:get(delete_limit, ?DEFAULT_DELETE_LIMIT),
  Select = ets:fun2ms(fun({Id, {TermId, _Value}, Ts}) when Ts < Threshold -> {Id, TermId} end),
  case ets:select(DSR, Select, DeleteLimit) of
    '$end_of_table' ->
      ok;
    {List, _Continuation} ->
      lists:foreach(fun({Id, TermId}) ->
                      % data can be deleted, because it has expired
                      ets:delete(DSR, Id),
                      % Term data can not be deleted when
                      % We are in the middle of a process registration
                      case ets:lookup(TSR, Id) of
                        [{Id, {TermId, _}}] -> ets:delete(TSR, Id);
                        _ -> ok
                      end
                    end, List)
  end.

-spec schedule_cleanup() -> ok.
schedule_cleanup() ->
  RescheduleTime = pes_cfg:get(cleanup_period_time, ?DEFAULT_CLEANUP_TIMEOUT),
  erlang:send_after(RescheduleTime + rand:uniform(100), self(), cleanup),
  ok.
