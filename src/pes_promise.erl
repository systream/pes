%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2023, systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(pes_promise).

-compile({no_auto_import, [send/2]}).
-include("pes_promise.hrl").

-type pes_promise_reply() :: #promise_reply{}.
-export_type([pes_promise_reply/0]).

-type promise() :: {promise, reference()}.
-export_type([promise/0]).

%% API
-export([async/2, await/1, reply/2, resolved/1]).

-spec async({module(), node()}, term()) -> promise().
async(Target, Command) ->
  Ref = erlang:monitor(process, Target),
  send(Target, #pes_promise_call{from = {self(), Ref}, command = Command}),
  {promise, Ref}.

-spec await(promise()) -> term() | {error, term()}.
await({promise, Ref}) ->
  receive
    #promise_reply{result = Result} = Reply ->
      resolved(Reply),
      Result;
    {'DOWN', Ref, process, _Pid, Reason} ->
      {error, Reason}
  end.

-spec resolved(promise() | pes_promise_reply()) -> ok.
resolved(#promise_reply{ref = Ref}) ->
  erlang:demonitor(Ref, [flush]);
resolved({promise, Ref}) ->
  erlang:demonitor(Ref, [flush]),
  ok.

-spec reply({pid(), reference()}, term()) -> ok.
reply({Caller, Ref} = _From, Response) ->
  send(Caller, #promise_reply{ref = Ref, result = Response}).

-spec send(pid() | {atom(), node()}, term()) -> ok.
send(To, Msg) ->
  erlang:send(To, Msg, [nosuspend, noconnect]),
  ok.
