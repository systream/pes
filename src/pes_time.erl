%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2023, systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(pes_time).

-define(DEFAULT_PROCESS_TIMEOUT, 12000).

-compile({no_auto_import, [now/0]}).

%% API
-export([now/0, is_expired/1]).

now() ->
  erlang:system_time(millisecond).

is_expired(Time) ->
  Time+application:get_env(pes, process_timeout, ?DEFAULT_PROCESS_TIMEOUT) < now().
