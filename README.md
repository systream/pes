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

## Concept
Paxos like consensus algorithm.

When a new value should be registered a statem process started 
to guide through the registration process. 

After a successful registration that statem process keep alive and
monitoring the started process. 
It is also responsible update/heartbeat the registry periodically.


Build
-----

    $ rebar3 compile
