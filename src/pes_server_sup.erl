%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2023, systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(pes_server_sup).
-behaviour(supervisor).

% @TODO different nodes can have different pes_server shard count, and it does not work!!! :/

-export([start_link/1, prepare/3, commit/4, read/2, repair/5, force_repair/4]).

-export([servers/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

-spec prepare(node(), pes_server:id(), pes_server:consensus_term_proposal()) ->
  pes_promise:promise().
prepare(Node, Id, Term) ->
  pes_server:prepare({hash(Id), Node}, Id, Term).

-spec commit(node(), pes_server:id(), pes_server:consensus_term_proposal(), term()) ->
  pes_promise:promise().
commit(Node, Id, Term, Value) ->
  pes_server:commit({hash(Id), Node}, Id, Term, Value).

-spec read(node(), pes_server:id()) -> pes_promise:promise().
read(Node, Id) ->
  pes_server:read({hash(Id), Node}, Id).

-spec repair(node(), Id, Term, NewTerm, term()) ->
  pes_promise:promise() when
  Id :: pes_server:id(),
  Term :: pes_server:consensus_term_proposal() | not_found,
  NewTerm :: {pes_server:consensus_term(), pid()}.
repair(Node, Id, OldTerm, NewTerm, Value) ->
  pes_server:repair({hash(Id), Node}, Id, OldTerm, NewTerm, Value).

-spec force_repair(node(), Id, NewTerm, term()) ->
  pes_promise:promise() when
  Id :: pes_server:id(),
  NewTerm :: {pes_server:consensus_term(), pid()}.
force_repair(Node, Id, NewTerm, Value) ->
  pes_server:force_repair({hash(Id), Node}, Id, NewTerm, Value).

-spec start_link(pos_integer()) -> {ok, pid()}.
start_link(ServerCount) ->
  supervisor:start_link({local, ?SERVER}, ?MODULE, ServerCount).

-spec init(pos_integer()) ->
  {ok, {SupFlags :: supervisor:sup_flags(), [ChildSpec :: supervisor:child_spec()]}}.
init(ServerCount) ->
  SupFlags = #{strategy => one_for_one,
               intensity => 5,
               period => 10
             },
  ServerNames = [name(I) || I <- lists:seq(0, ServerCount-1)],
  persistent_term:put({?MODULE, servers}, ServerNames),
  {ok, {SupFlags, [child_spec(Name) || Name <- ServerNames]}}.

-spec name(pos_integer()) -> atom().
name(Idx) ->
  % I know it's bad.. :/
  list_to_atom("pes_server_" ++ integer_to_list(Idx)).

-spec child_spec(atom()) -> map().
child_spec(Name) ->
  #{id => Name,
    start => {pes_server, start_link, [Name]},
    restart => permanent,
    shutdown => 5000,
    type => worker,
    modules => [pes_server]}.

-spec servers() -> [atom()].
servers() ->
  persistent_term:get({?MODULE, servers}, []).

-spec hash(term()) -> atom().
hash(Id) ->
  Servers = servers(),
  lists:nth(erlang:phash2(Id, length(Servers)) + 1, Servers).
