%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2023, systream
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(pes_cfg).

-behaviour(gen_server).

-define(SERVER, ?MODULE).

-record(state, {}).
-define(KEY(K), {?MODULE, K}).

%% API
-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2]).

-export([set/2, get/2]).


%%%===================================================================
%%% API
%%%===================================================================

%% pes_cfg:set(cleanup_period_time, 25000).
%% pes_cfg:set(delete_limit, 100).

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
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec init(Args :: term()) -> {ok, State :: term()}.
init(_) ->
  simple_gossip:subscribe(self(), data),
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
handle_info({data_changed, undefined}, State) ->
  {noreply, State};
handle_info({data_changed, Data}, State) ->
  persist(Data),
  {noreply, State}.

-spec persist(map()) -> ok.
persist(Data) ->
  maps:map(fun(Key, Value) -> persistent_term:put(?KEY(Key), Value) end, Data),
  ok.