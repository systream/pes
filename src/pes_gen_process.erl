%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2024, systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(pes_gen_process).

-record(state, {
  module :: module(),
  sub_state :: term()
}).

-opaque state() :: #state{}.

-export_type([state/0]).

-callback init() -> State :: term().
-callback handle_message(Message :: term(), State :: term()) ->
  {ok, NewState :: term()}.

%% API
-export([start_link/1, init/1, loop/1]).

-spec start_link(module()) -> {ok, pid()}.
start_link(Module) ->
  proc_lib:start_link(?MODULE, init, [Module]).

-spec init(module()) -> no_return().
init(Module) ->
  Me = self(),
  {ok, SubState} = Module:init(),
  proc_lib:init_ack({ok, Me}),
  ?MODULE:loop(#state{module = Module, sub_state = SubState}).

-spec loop(state()) -> no_return().
loop(#state{module = Module, sub_state = SubState} = State) ->
  receive
    %{system, From, Request} ->
    %  sys:handle_system_msg(Request, From, Parent, ?MODULE, [], State);
    Message ->
      {ok, NewSubState} = Module:handle_message(Message, SubState),
      erlang:hibernate(?MODULE, loop, [State#state{sub_state = NewSubState}])
  end.

