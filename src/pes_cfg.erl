%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2023, systream
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(pes_cfg).

-define(DEFAULT_HEARTBEAT, 8048).

-define(KEY(K), {?MODULE, K}).

-export([set/2, get/2, heartbeat/0]).


%%%===================================================================
%%% API
%%%===================================================================

%% pes_cfg:set(cleanup_period_time, 25000).
%% pes_cfg:set(delete_limit, 100).

-spec heartbeat() -> pos_integer().
heartbeat() ->
  simple_gossip:get(heartbeat, application:get_env(pes, default_heartbeat, ?DEFAULT_HEARTBEAT)).

-spec get(term(), term()) -> term().
get(Key, Default) ->
  SGKey = {?MODULE, Key},
  case simple_gossip:get(SGKey, {default, Default}) of
    {default, Default} ->
      ok = simple_gossip:set(fun(Status) -> do_set_if_not_set(SGKey, Default, Status) end),
      Default;
    Result ->
      Result
  end.

-spec set(term(), term()) -> ok.
set(Key, Value) ->
  simple_gossip:set({?MODULE, Key}, Value).

-spec do_set_if_not_set(term(), term(), undefined | map()) -> {change, map()} | no_change.
do_set_if_not_set(Key, Value, undefined) ->
  {change, #{Key => Value}};
do_set_if_not_set(Key, _Value, Data) when is_map_key(Key, Data) ->
  no_change;
do_set_if_not_set(Key, Value, Data) ->
  {change, Data#{Key => Value}}.