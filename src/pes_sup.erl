%%%-------------------------------------------------------------------
%% @doc pes top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(pes_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
  supervisor:start_link({local, ?SERVER}, ?MODULE, []).

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
               start => {pes_proxy, start_link, [shard_count()]},
               restart => permanent,
               shutdown => 5000,
               type => supervisor,
               modules => [pes_proxy, pes_server]},
    {ok, {SupFlags, [Cluster, Acceptor]}}.

%% internal functions

shard_count() ->
    application:get_env(pes, shard_count, erlang:system_info(schedulers_online)).


