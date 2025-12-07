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
-type promise() :: {promise, reference()}.

-export_type([promise/0, pes_promise_reply/0]).

%% API
-export([async/2, await/1, await/2, reply/2, fake_reply/1, fake_reply/2, resolved/1]).

-spec async({module(), node()}, term()) -> promise().
async(Target, Command) ->
  Ref = erlang:monitor(process, Target, [{alias, reply_demonitor}]),
  send(Target, #pes_promise_call{from = Ref, command = Command}),
  {promise, Ref}.

-spec await(promise()) -> term() | {error, term()}.
await(Promise) ->
  await(Promise, infinity).

-spec await(promise(), Timeout :: pos_integer() | infinity) ->
  term() | {error, timeout | term()}.
await({promise, Ref}, Timeout) ->
  receive
    #promise_reply{result = Result, ref = Ref} = Reply ->
      resolved(Reply),
      Result;
    {'DOWN', Ref, process, _Pid, Reason} ->
      {error, Reason}
  after Timeout ->
    {error, timeout}
  end.

-spec resolved(promise() | pes_promise_reply()) -> ok.
resolved(#promise_reply{ref = Ref}) ->
  erlang:demonitor(Ref, [flush]);
resolved({promise, Ref}) ->
  erlang:demonitor(Ref, [flush]),
  ok.

-spec reply(reference(), term()) -> ok.
reply(AliasRef, Response) ->
  send(AliasRef, #promise_reply{ref = AliasRef, result = Response}).

-spec fake_reply(term()) -> promise().
fake_reply(Reply) ->
  fake_reply(self(), Reply).

-spec fake_reply(pid(), term()) -> promise().
fake_reply(Pid, Reply) ->
  Ref = make_ref(),
  send(Pid, #promise_reply{ref = Ref, result = Reply}),
  {promise, Ref}.

-spec send(pid() | {atom(), node()} | reference(), term()) -> ok.
send(To, Msg) ->
  erlang:send(To, Msg, [nosuspend, noconnect]),
  ok.
