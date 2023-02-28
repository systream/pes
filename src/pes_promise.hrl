-record(pes_promise_call, {
  from :: {pid(), reference()},
  command :: term()
}).

-record(promise_reply, {
  ref :: reference(),
  result :: term()
}).

-type pes_promise_reply() :: #promise_reply{}.