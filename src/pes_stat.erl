%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2023, systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(pes_stat).

%% API
-export([init/0, update/2, count/1, decrease/1, increase/1, stat/0]).

-spec init() -> ok.
init() ->
  ok = exometer:ensure([pes, registrar, active], counter, []),
  ok = exometer:ensure([pes, registrar, response_time], histogram, []),
  ok = exometer:ensure([pes, server, ack], spiral, []),
  ok = exometer:ensure([pes, server, nack], spiral, []),
  ok = exometer:ensure([pes, server, repair], spiral, []),
  ok = exometer:ensure([pes, server, request_count], spiral, []),
  ok = exometer:ensure([pes, lookup, response_time], histogram, []).

-spec update([atom()], number()) -> ok.
update(Name, Value) ->
  ok = exometer:update([pes | Name], Value).

-spec count([atom()]) -> ok.
count(Name) ->
  update(Name, 1).

-spec increase([atom()]) -> ok.
increase(Name) ->
  update(Name, 1).

-spec decrease([atom()]) -> ok.
decrease(Name) ->
  update(Name, -1).

-spec stat() -> [{list(atom()), number()}].
stat() ->
  {ok, [{value, ActiveRegistrarCount}]} = exometer:get_value([pes, registrar, active], value),
  {ok, [{one, ServerAckRate}]} = exometer:get_value([pes, server, ack], one),
  {ok, [{one, ServerNackRate}]} = exometer:get_value([pes, server, nack], one),
  {ok, [{one, ReqC}]} = exometer:get_value([pes, server, request_count], one),
  {ok, [{one, RepairC}]} = exometer:get_value([pes, server, repair], one),
  [
    {[registrar, active], ActiveRegistrarCount},
    {[registrar, response_time], histogram([registrar, response_time])},
    {[server, request_count], ReqC div 60},
    {[server, ack], ServerAckRate div 60},
    {[server, nack], ServerNackRate div 60},
    {[lookup, response_time], histogram([lookup, response_time])},
    {[server, repair], RepairC}
  ].

histogram(Name) ->
  {ok, Metrics} = exometer:get_value([pes | Name], [min, max, mean, 99]),
  Metrics.
