%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2023, systream
%%% @doc
%%% Managing cluster nodes
%%% @end
%%%-------------------------------------------------------------------
-module(pes_cluster).

-behaviour(gen_server).
-compile({no_auto_import, [nodes/0]}).

-define(NODES_KEY, {?MODULE, nodes}).
-define(SERVER, ?MODULE).
-define(DEAD_NODES_KEY(Node), {?MODULE, dead_nodes, Node}).

-record(state, {}).

%% API
-export([nodes/0, join/1, leave/1, live_nodes/0, is_node_alive/1]).
-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2]).

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
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec init(Args :: term()) -> {ok, State :: term()}.
init(_) ->
  simple_gossip:subscribe(self(), rumor),
  persistent_term:put(?NODES_KEY, cluster_nodes()),
  {ok, #state{}}.

-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: term()) ->
  no_return().
handle_call(_Request, _From, _State) ->
  erlang:error(not_implemented).

-spec handle_cast(Request :: term(), State :: term()) -> no_return().
handle_cast(_Request, _State) ->
  erlang:error(not_implemented).

-spec handle_info(Info :: timeout | term(), State :: term()) ->
  {noreply, NewState :: term()} |
  {noreply, NewState :: term(), timeout() | hibernate | {continue, term()}} |
  {stop, Reason :: term(), NewState :: term()}.
handle_info({rumor_changed, Rumor}, State) ->
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
  {noreply, State};
handle_info({nodeup, Node}, State) ->
  case lists:member(Node, nodes()) of
    true ->
      persistent_term:erase(?DEAD_NODES_KEY(Node)),
      logger:info("Cluster Member ~p become UP", [Node]),
      ok;
    _ ->
      ok
  end,
  {noreply, State};

handle_info({nodedown, Node}, State) ->
  case lists:member(Node, nodes()) of
    true ->
      persistent_term:put(?DEAD_NODES_KEY(Node), true),
      logger:warning("Cluster Member ~p went DOWN", [Node]);
    _ ->
      ok
  end,
  {noreply, State}.