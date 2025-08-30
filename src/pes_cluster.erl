%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2023, systream
%%% @doc
%%% Managing cluster nodes
%%% @end
%%%-------------------------------------------------------------------
-module(pes_cluster).

-behaviour(pes_gen_process).
-compile({no_auto_import, [nodes/0]}).

-define(NODES_KEY, {?MODULE, nodes}).
-define(DEAD_NODES_KEY(Node), {?MODULE, dead_nodes, Node}).

%% API
-export([nodes/0, join/1, leave/1, live_nodes/0, is_node_alive/1]).
-export([start_link/0, init/0, handle_message/2]).

-spec join(node()) -> ok | {error, term()}.
join(Node) ->
  simple_gossip:join(Node).

-spec leave(node()) -> ok.
leave(Node) ->
  ok = simple_gossip:leave(Node).

%%%===================================================================
%%% API
%%%===================================================================

-spec nodes() -> [node()].
nodes() ->
  persistent_term:get(?NODES_KEY, [node()]).

-spec live_nodes() -> [node()].
live_nodes() ->
  lists:filter(fun is_node_alive/1, nodes()).

-spec is_node_alive(node()) -> boolean().
is_node_alive(Node) ->
  not persistent_term:get(?DEAD_NODES_KEY(Node), false).

cluster_nodes() ->
  case simple_gossip:status() of
    {ok, _Vsn, _Leader, Nodes} ->
      Nodes;
    {error, _, _Leader, Nodes} ->
      Nodes
  end.

%% @doc Spawns the server and registers the local name (unique)
-spec(start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  pes_gen_process:start_link(?MODULE).

-spec init() -> {ok, State :: term()}.
init() ->
  simple_gossip:subscribe(self(), rumor),
  persistent_term:put(?NODES_KEY, cluster_nodes()),
  ok = net_kernel:monitor_nodes(true),
  {ok, no_state}.


-spec handle_message(Info :: term(), State :: term()) ->
  {ok, State :: term()}.
handle_message({rumor_changed, Rumor}, State) ->
  Nodes = lists:usort(simple_gossip_rumor:nodes(Rumor)),
  CurrentNodes = lists:usort(nodes()),
  case CurrentNodes =:= Nodes of
    true -> ok;
    _ ->
      persistent_term:put(?NODES_KEY, Nodes),
      % clean dead node registry for removed nodes
      lists:foreach(fun (Node) ->
                      persistent_term:erase(?DEAD_NODES_KEY(Node))
                    end, CurrentNodes -- Nodes)
  end,
  {ok, State};
handle_message({nodeup, Node}, State) ->
  case lists:member(Node, nodes()) of
    true ->
      % @todo have a better solution
      % Give some time to the remote node to start properly,
      % so before adding back again wait a bit
      timer:sleep(15000),
      logger:info("Cluster Member ~p become UP", [Node]),
      persistent_term:erase(?DEAD_NODES_KEY(Node)),
      ok;
    _ ->
      ok
  end,
  {ok, State};

handle_message({nodedown, Node}, State) ->
  case lists:member(Node, nodes()) of
    true ->
      persistent_term:put(?DEAD_NODES_KEY(Node), true),
      logger:warning("Cluster Member ~p went DOWN", [Node]);
    _ ->
      ok
  end,
  {ok, State}.