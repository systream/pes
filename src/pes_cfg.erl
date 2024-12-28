%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2023, systream
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(pes_cfg).

-define(DEFAULT_HEARTBEAT, 8048).

-define(KEY(K), {?MODULE, K}).

-behaviour(pes_gen_process).

%% API
-export([start_link/0, init/0, handle_message/2]).

-export([set/2, get/2, heartbeat/0]).


%%%===================================================================
%%% API
%%%===================================================================

%% pes_cfg:set(cleanup_period_time, 25000).
%% pes_cfg:set(delete_limit, 100).

-spec heartbeat() -> pos_integer().
heartbeat() ->
  get(heartbeat, ?DEFAULT_HEARTBEAT).

-spec get(term(), term()) -> term().
get(Key, Default) ->
  case persistent_term:get(?KEY(Key), '__not_set__') of
    '__not_set__' ->
      case simple_gossip:get() of
        #{Key := Value} = Data ->
          persist(Data),
          Value;
        _ ->
          ok = simple_gossip:set(fun(Status) -> do_set_if_not_set(Key, Default, Status) end),
          Default
      end;
    Value ->
      Value
  end.

-spec set(term(), term()) -> ok.
set(Key, Value) ->
  ok = simple_gossip:set(fun(Data) ->
                            {change, do_set(Key, Value, Data)}
                         end).

-spec do_set(term(), term(), undefined | map()) -> map().
do_set(Key, Value, undefined) ->
  do_set(Key, Value, #{});
do_set(Key, Value, Data) ->
  Data#{Key => Value}.

-spec do_set_if_not_set(term(), term(), undefined | map()) -> {change, map()} | no_change.
do_set_if_not_set(Key, Value, undefined) ->
  {change, #{Key => Value}};
do_set_if_not_set(Key, _Value, Data) when is_map_key(Key, Data) ->
  no_change;
do_set_if_not_set(Key, Value, Data) ->
  {change, Data#{Key => Value}}.

%% @doc Spawns the server and registers the local name (unique)
-spec(start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  pes_gen_process:start_link(?MODULE).

-spec init() -> {ok, no_state}.
init() ->
  simple_gossip:subscribe(self(), data),
  {ok, no_state}.

-spec handle_message({data_changed, term()}, term()) -> {ok, term()}.
handle_message({data_changed, undefined}, State) ->
  {ok, State};
handle_message({data_changed, Data}, State) ->
  persist(Data),
  {ok, State}.

-spec persist(map()) -> ok.
persist(Data) ->
  maps:map(fun(Key, Value) -> persistent_term:put(?KEY(Key), Value) end, Data),
  ok.