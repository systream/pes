%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2023, systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(pes_server).

-include_lib("pes_promise.hrl").

-type id() :: term().
-type consensus_term() :: pos_integer().
-type consensus_term_proposal() :: {consensus_term(), {node(), pid()}} | consensus_term().
-type value() :: term().
-type target() :: node() | {atom(), node()}.

-compile({no_auto_import, [register/2, is_process_alive/1, send/2]}).

%% API
-export([prepare/3, commit/4, read/2, repair/5]).

-export([start_link/1, init/1, loop/1]).
-export([system_continue/3, system_terminate/4, system_get_state/1, system_code_change/4]).

-define(TERM_STORAGE, pes_term_storage).
-define(DATA_STORAGE, pes_data_storage).

-define(SERVER, ?MODULE).

-record(state, {
  term_storage_ref ::reference(),
  data_storage_ref :: reference()
}).

-type state() :: #state{}.

-spec prepare(target(), id(), consensus_term_proposal()) -> pes_promise:promise().
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
  pes_promise:async({Server, Node}, Command).

-spec start_link(atom()) -> {ok, pid()}.
start_link(Server) ->
  proc_lib:start_link(?MODULE, init, [{Server, self()}]).

-spec init(pid()) -> no_return().
init({Server, Parent}) ->
  true = erlang:register(Server, self()),
  proc_lib:init_ack(Parent, {ok, self()}),
  loop(#state{term_storage_ref = ets:new(?TERM_STORAGE, [protected]),
              data_storage_ref = ets:new(?DATA_STORAGE, [protected])}).

-spec loop(state()) -> no_return().
loop(State) ->
  receive
    #pes_promise_call{from = From, command = Command} ->
      pes_promise:reply(From, handle_command(Command, State)),
      ?MODULE:loop(State);
    {system, From, Request} ->
      sys:handle_system_msg(Request, From, self(), ?MODULE, [], undefined)
  end.

-spec handle_command(term(), state()) -> term().
handle_command({read, Id}, #state{data_storage_ref = DSR}) ->
  case ets:lookup(DSR, Id) of
    [{Id, {Term, Value}}] ->
      {ok, Term, Value};
    _ ->
      not_found
  end;
handle_command({prepare, Id, {Term, Server}}, #state{term_storage_ref = TSR}) ->
  case ets:lookup(TSR, Id) of
    [{Id, {StoredTerm, _StoredServer}}] when StoredTerm >= Term ->
      nack;
    _ -> % not found or StoredTerm is lower than this
      true = ets:insert(TSR, {Id, {Term, Server}}),
      ack
  end;
handle_command({commit, Id, {Term, Server}, Value}, #state{data_storage_ref = DSR,
                                                           term_storage_ref = TSR}) ->
  case ets:lookup(TSR, Id) of
    [{Id, {StoredTerm, StoredServer}}] when StoredTerm =:= Term andalso StoredServer =:= Server ->
      true = ets:insert(DSR, {Id, {Term, Value}}),
      ack;
    [{Id, {StoredTerm, StoredServer}}] ->
      {nack, {node(), {StoredTerm, StoredServer}}};
    [] ->
      {nack, {node(), not_found}}
  end;
% Commit can reply with nack and the actual term, server data.
% To ensure in the mean time no other registration attempt were made, we send back those values.
handle_command({repair, Id, Term, NewTern, Value}, #state{data_storage_ref = DSR,
                                                          term_storage_ref = TSR}) ->
  case ets:lookup(TSR, Id) of
    [{Id, StoredTerm}] when StoredTerm =:= Term -> % the term matches
      io:format(user, "repaied: ~p -> ~p - ~p ~n", [Id, NewTern, Value]),
      true = ets:insert(TSR, {Id, NewTern}),
      true = ets:insert(DSR, {Id, {Term, Value}}),
      ack;
    _ ->
      nack
  end.

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
