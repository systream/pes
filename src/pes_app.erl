%%%-------------------------------------------------------------------
%% @doc pes public API
%% @end
%%%-------------------------------------------------------------------
-module(pes_app).
-behaviour(application).

-export([start/2, stop/1]).

-spec start(_, _) -> {ok, pid()}.
start(_StartType, _StartArgs) ->
    pes_stat:init(),
    pes_sup:start_link().

-spec stop(_) -> ok.
stop(_State) ->
    pes_cluster:leave(node()),
    ok.
