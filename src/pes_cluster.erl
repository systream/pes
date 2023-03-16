%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2023, systream
%%% @doc
%%% Managing cluster nodes
%%% @end
%%%-------------------------------------------------------------------
-module(pes_cluster).

-compile({no_auto_import, [nodes/0]}).

-define(NODES_KEY, {?MODULE, nodes}).

%% API
-export([nodes/0, join/1, leave/1]).
-export([start_link/0, init/0, loop/0]).

-spec join(node()) -> ok.
join(Node) ->
  ok = simple_gossip:join(Node).

-spec leave(node()) -> ok.
leave(Node) ->
  ok = simple_gossip:leave(Node).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Spawns the server and registers the local name (unique)
-spec(start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  {ok, spawn_link(?MODULE, init, [])}.

-spec init() -> no_return().
init() ->
  simple_gossip:subscribe(self(), rumor),
  persistent_term:put(?NODES_KEY, cluster_nodes()),
  erlang:register(pes_cluster, self()),
  ?MODULE:loop().

-spec loop() -> no_return().
loop() ->
  receive
    {rumor_changed, Rumor} ->
      Nodes = lists:usort(simple_gossip_rumor:nodes(Rumor)),
      CurrentNodes = lists:usort(nodes()),
      case CurrentNodes =:= Nodes of
        true -> ok;
        _ -> persistent_term:put(?NODES_KEY, Nodes)
      end,
      loop()
  end.

-spec nodes() -> [node()].
nodes() ->
  persistent_term:get(?NODES_KEY, [node()]).

cluster_nodes() ->
  case simple_gossip:status() of
    {ok, _Vsn, _Leader, Nodes} ->
      Nodes;
    {error, _, _Leader, Nodes} ->
      Nodes
  end.