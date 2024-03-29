%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2023, systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(pes_time).

-compile({no_auto_import, [now/0]}).

%% API
-export([now/0, is_expired/1]).

-spec now() -> pos_integer().
now() ->
  erlang:system_time(millisecond).

-spec is_expired(pos_integer()) -> boolean().
is_expired(Time) ->
  Time + (pes_cfg:heartbeat() * 3) < now().
