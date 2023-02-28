%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2023, systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(pes_proxy).

% @TODO different nodes can have different amount of servers, and it does not work!

-type id() :: term().
-type consensus_term() :: pos_integer().
-type consensus_term_proposal() :: {consensus_term(), {node(), pid()}} | consensus_term().

-export([start_link/1, prepare/3, commit/4, read/2]).

-export([init/1]).

-define(SERVER, ?MODULE).

-spec prepare(node(), id(), consensus_term_proposal()) -> pes_promise:promise().
prepare(Node, Id, Term) ->
  pes_server:prepare({hash(Id), Node}, Id, Term).

-spec commit(node(), id(), consensus_term_proposal(), term()) -> pes_promise:promise().
commit(Node, Id, Term, Value) ->
  pes_server:commit({hash(Id), Node}, Id, Term, Value).

-spec read(node(), id()) -> pes_promise:promise().
read(Node, Id) ->
  pes_server:read({hash(Id), Node}, Id).

start_link(ServerCount) ->
  supervisor:start_link({local, ?SERVER}, ?MODULE, ServerCount).

init(ServerCount) ->
  SupFlags = #{strategy => one_for_one,
    intensity => 5,
    period => 10},
  ServerNames = [name(I) || I <- lists:seq(0, ServerCount-1)],
  persistent_term:put({?MODULE, servers}, ServerNames),
  {ok, {SupFlags, [child_spec(Name) || Name <- ServerNames]}}.

name(Idx) ->
  % I know it's bad.. :/
  list_to_atom("pes_server_" ++ integer_to_list(Idx)).

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

hash(Id) ->
  Servers = servers(),
  lists:nth(erlang:phash2(Id, length(Servers))+1, Servers).

