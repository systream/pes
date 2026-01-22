# Process Election Service (PES)

PES is a distributed process registration and election service for Erlang applications. It provides a consensus-based mechanism (similar to Paxos) to ensure that processes are uniquely registered across a cluster.

## Features

*   **Cluster-wide Process Registry**: Register processes with a global name across the Erlang cluster.
*   **Leader Election**: Automatically handles race conditions to ensure only one process owns a name.
*   **Guard Processes**: Uses statem machines to monitor registered processes and handle heartbeats.
*   **Handoff Support**: Allows transferring a name registration to a new process.

## Prerequisites

*   Erlang/OTP 25 or higher

## Installation

Add `pes` to your `rebar.config` dependencies:

```erlang
{deps, [
    {pes, {git, "https://github.com/systream/pes.git", {branch, "master"}}}
]}.
```

## Usage

### Process Registration

You can register a process using the `{via, pes, Name}` tuple, which is compatible with `gen_server` and other OTP behaviours.

**Start a singleton GenServer:**

```erlang
Name = <<"merlin">>,
gen_server:start({via, pes, Name}, wizard_mod, [], []).
```

**Call the registered process:**

```erlang
gen_server:call({via, pes, <<"merlin">>}, do_magic).
```

### Manual API

You can also use the `pes` module directly:

```erlang
%% Register a process
pes:register_name(<<"my_process">>, self()).

%% Look up a process
case pes:whereis_name(<<"my_process">>) of
    Pid when is_pid(Pid) -> 
        io:format("Found pid: ~p~n", [Pid]);
    undefined -> 
        io:format("Process not found~n")
end.
```

### Cluster Management

PES nodes need to form a cluster to share the registry.

**Join a cluster:**

```erlang
%% Run this on the node that wants to join an existing cluster node
pes:join('node_b@serverfarm1').
```

**Leave a cluster:**

```erlang
pes:leave('node_d@serverfarm1').
```

### Process Handoff

If you need to replace a registered process with a new one, you must perform a handoff. The registry will update the pid associated with the name.

```erlang
%% 1. Start the new process
{ok, NewMerlinServer} = gen_server:start(wizard_mod, [], []),

%% 2. Get the current registered pid
MerlinPid = pes:whereis_name(<<"merlin">>),

%% 3. Coordinate the handoff (application specific logic)
%%    For example, tell the old process to transfer state and stop
gen_server:call(MerlinPid, {handoff, NewMerlinServer}),

%% 4. Update the registry to point to the new pid
ok = pes:update(<<"merlin">>, NewMerlinServer),

%% 5. Terminate the old process
gen_server:terminate(MerlinPid).
```

## Configuration

PES can be configured via `sys.config` or application environment variables.

| Key                 | Description                                                                                                                          |
|:--------------------|:-------------------------------------------------------------------------------------------------------------------------------------|
| `shard_count`       | Number of gen_servers for accepting consensus request, should be same on all nodes                                                   |
| `default_heartbeat` | Amount of time when guard process update the registry, after 3 missing heartbeat a process considered dead, and new value can be set |

## Development

**Build:**

```bash
rebar3 compile
```

**Run Tests:**

```bash
rebar3 ct
```

**Run Property-Based Tests:**

```bash
rebar3 proper
```

## License

MIT
