%%%-------------------------------------------------------------------
%% @doc pes top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(pes_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).
-define(DEFAULT_SHARD_COUNT, 16).

-spec start_link() -> {'ok', pid()} | 'ignore' | {'error', term()}.
start_link() ->
  supervisor:start_link({local, ?SERVER}, ?MODULE, []).

-spec init([]) ->
  {ok, {SupFlags :: supervisor:sup_flags(), [ChildSpec :: supervisor:child_spec()]}}.
init([]) ->
  SupFlags = #{strategy => one_for_all,
               intensity => 5,
               period => 10},
  Cluster = #{id => cluster,
              start => {pes_cluster, start_link, []},
              restart => permanent,
              shutdown => 1000,
              type => worker,
              modules => [pes_cluster]},
  Acceptor = #{id => pes_server_cluster,
               start => {pes_server_sup, start_link, [shard_count()]},
               restart => permanent,
               shutdown => 5000,
               type => supervisor,
               modules => [pes_proxy, pes_server]},
    {ok, {SupFlags, [Cluster, Acceptor]}}.

%% internal functions

-spec shard_count() -> pos_integer().
shard_count() ->
    application:get_env(pes, shard_count, ?DEFAULT_SHARD_COUNT).


