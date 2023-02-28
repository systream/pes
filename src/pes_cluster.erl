%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2023, systream
%%% @doc
%%% Managing cluster nodes
%%% @end
%%%-------------------------------------------------------------------
-module(pes_cluster).
-author("Peter Tihanyi").

-compile({no_auto_import, [nodes/0]}).

%% API
-export([nodes/0, join/1]).

%@TODO: implement !!!!

-spec join(node()) -> ok | {error, term()}.
join(Node) ->
  true = net_kernel:connect_node(Node),
  ok.

-spec nodes() -> [node()].
nodes() ->
  % @TODO fix, there should a be a process
  persistent_term:get({?MODULE, nodes}, [erlang:node() | erlang:nodes()]).
  %['1@Peters-MacBook-Pro', '2@Peters-MacBook-Pro', '3@Peters-MacBook-Pro'].
