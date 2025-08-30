-record(pes_promise_call, {
  from :: reference(),
  command :: term()
}).

-record(promise_reply, {
  ref :: reference(),
  result :: term()
}).