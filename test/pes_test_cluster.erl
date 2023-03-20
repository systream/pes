-module(pes_test_cluster).

%% API
-export([start_node/1, start_node/2, stop_node/1]).

start_node(Name) ->
  start_node(Name, [{monitor_master, true}]).

start_node(Name, Opts) ->
  case ct_slave:start(Name, Opts) of
    {error, started_not_connected, Node} ->
      net_kernel:connect_node(Node),
      ct_slave:stop(Node),
      start_node(Name);
    {error, already_started, Node} ->
      ct_slave:stop(Node),
      start_node(Name);
    {ok, Node} ->
      ok = rpc:call(Node, code, add_paths, [code:get_path()]),
      {ok, _} = rpc:call(Node, application, ensure_all_started, [pes]),
      true = net_kernel:connect_node(Node),
      {ok, Node}
  end.

stop_node(Name) ->
  ct_slave:stop(Name).