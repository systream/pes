# Process Election Service - PES
Service for consensus based process selection/election service.

## Usage

Can register processes with `{via, pes, term()}` tupple. 

```erlang
gen_server:start({via, pes, <<"merlin">>}, wizard_mod, [], []).
```

```erlang
gen_server:call({via, pes, <<"merlin">>}, do_magic).
```

## Forming a cluster

Join to cluster:
```erlang
pes:join('node_b@serverfarm1').
```

Leave a node from cluster
```erlang
pes:leave('node_d@serverfarm1').
```

## Concept
Paxos like consensus algorithm.

When a new value should be registered a statem process started 
to guide through the registration process. 

After a successful registration that statem process keep alive and
monitoring the started process. 
It is also responsible update/heartbeat the registry periodically.


# Handoff process
It is possible to update the process id in PES's catalog.
But pls keep it in ming that it is tou responsibility to design you own handoff method. 

Here is some example: 
```erlang
  NewMerlinServer = gen_server:start(wizard_mod, [], []),
  MerlinPid = pes:whereis_name(<<"merlin">>),
  gen_server:call(MerlinPid, {handoff, NewMerlinServer}),

  pes:update(<<"merlin">>, NewMerlinServer),

  gen_server:terminate(MerlinPid).
```

Build
-----

    $ rebar3 compile
