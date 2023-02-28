%%%-------------------------------------------------------------------
%% @doc pes public API
%% @end
%%%-------------------------------------------------------------------

-module(pes_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    pes_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
