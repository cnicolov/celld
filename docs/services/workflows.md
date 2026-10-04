# Workflows

A Workflow is a durable function that runs as a named sequence of steps,
sleeps, and events. An application uses a Workflow for work that must survive a
failure or a long delay, such as an order pipeline, a staged import, or an
export that retries. celld runs each instance as a cell, so the state of an
instance lives in that cell's storage and moves with the cell. Read the
[Cloudflare Workflows documentation](https://developers.cloudflare.com/workflows/build/workers-api/)
for the standard API behavior.

## Example

The [Workflow example](../../examples/workflow) fetches a document and stores
the result of a durable step.

<!-- celld-example: workflow -->

## Steps and replay

A Workflow class extends `WorkflowEntrypoint` and implements
`run(event, step)`. `event.payload` holds the parameters that `create()`
supplied, and the `step` object supplies the durable operations. A value that
`run()` returns becomes the `output` field of `status()`. Read the
[Workers API documentation](https://developers.cloudflare.com/workflows/build/workers-api/)
for the complete signature.

celld does not stop `run()` in place and continue it later. Each time an
instance makes progress, celld calls `run()` again from the first line. A step
that already finished returns its stored result, and its callback does not run
a second time. celld writes each result into the storage of the instance cell,
under a key that joins the step name and the occurrence count of that name, so
a step inside a loop keeps one record for each iteration.

![A resume re-enters run from the top, the ledger in the instance cell answers for each finished step, and the first blocked step arms the cell alarm so the cell hibernates until the next replay](workflows-flow.svg)

This design puts one rule on the application code: everything outside a step
callback runs again on every replay. Put each subrequest, each side effect, and
each value that must stay stable inside a `step.do()` callback. Cloudflare
gives the same rule in the
[rules of Workflows](https://developers.cloudflare.com/workflows/build/rules-of-workflows/).

celld ends an invocation when no step callback runs and no step can make
progress. `run()` can also await work that is not a step. Upstream permits that
await and does not make it durable, and celld accepts it for a short time. A
replay cannot resume such an await, therefore celld fails the instance when it
keeps `run()` pending for 60 seconds while no step runs and none waits.

## Sleeping and waiting for an event

`step.sleep(name, duration)` and `step.sleepUntil(name, timestamp)` stop the
instance until a deadline. `step.waitForEvent(name, options)` stops it until
`sendEvent()` delivers an event of the matching type, or until a timeout that
is 24 hours by default. Read
[sleeping and retrying](https://developers.cloudflare.com/workflows/build/sleeping-and-retrying/)
and
[events and parameters](https://developers.cloudflare.com/workflows/build/events-and-parameters/)
for the durations and the event rules.

celld computes the deadline once and commits it with the step record. A later
replay reads the stored deadline instead of adding the duration to the current
time, therefore a crash or a slow resume cannot move the wake further away.

A waiting instance holds no isolate. celld arms the alarm of the instance cell
at the earliest blocked deadline and returns, so the cell hibernates like any
other [cell](durable-objects.md) when the alarm is further away than the
near-alarm residency window. That window is one hour, and
`CELLD_ALARM_RESIDENT_MS` changes it. The armed alarm has a durable wake entry
in the fleet, and the node that owns the cell at the deadline runs the next
replay.

An event that arrives before the instance reaches its wait step is buffered,
and a matching step then consumes the buffered events in arrival order. celld
deletes the event and writes the step record in one SQLite commit, so an
acknowledged event cannot disappear. celld compares the stored deadline before
the buffer, therefore a late replay cannot accept an event that arrived after
the step timed out.

## Retries, failure, and a node that stops

`step.do()` retries a failed callback. celld uses the upstream defaults when
the call supplies no `retries` object: a limit of 5 retries, a delay of 10
seconds, exponential backoff, and a timeout of 10 minutes for one attempt. A
`NonRetryableError` stops the retries at once. When the retries run out, the
step promise rejects and `run()` can catch it. An error that escapes `run()`
moves the instance to the `errored` status.

celld commits the time of the next attempt when an attempt fails, so a replay
waits out the rest of the backoff instead of starting it again. A pending retry
suspends the instance in the same way a sleep does, and `status()` reports
`waiting` for both.

A node that stops does not lose an instance. The instance is a cell, so it has
the ownership, the fencing, and the replication that every cell has. celld
leaves the alarm that started an invocation armed for the whole invocation, and
it consumes that alarm only after the invocation returns cleanly. A node that
dies in the middle therefore leaves an armed alarm, and the next owner replays
`run()` over the step results that reached a replicated commit. A step whose
result did not commit runs again, which is why a step callback must tolerate a
second attempt.

celld enforces no limit on the number of instances that run at once. The memory
of the fleet and the resident-cell cap bound that number instead. A waiting
instance costs no memory, so a fleet holds far more waiting instances than
running ones.

## Subscribe to execution events

`WorkflowInstance.subscribe({ cursor?, filter? })` reads retained execution history
and then waits for live events. It observes engine transitions, including native
timeouts, result-validation failures, retry backoff, sleeps, waits, and lifecycle
controls. Every event includes `instanceId`, an increasing numeric `eventId`, and
an observed UNIX `timestamp` in milliseconds. Event-specific fields follow the
[Cloudflare event definitions](https://developers.cloudflare.com/workflows/build/subscribe-to-instance-events/#event-fields).

```js
const instance = await env.REPORTS.get(instanceId);
using subscription = await instance.subscribe({
  cursor: lastProcessedEventId,
  filter: ["attempt_errored", "step_completed", "workflow_completed", "workflow_errored"],
});

while (true) {
  const result = await subscription.next();
  if (result.done) break;
  await archiveEvent(result.value);
  lastProcessedEventId = result.value.eventId;
}
```

- An omitted cursor starts at the beginning of the current execution generation.
  A supplied cursor starts strictly after that ID. Cursors must be non-negative
  safe integers and cannot be ahead of retained history.
- An omitted filter selects all events. An empty filter selects none, but still
  waits for the instance to end. Unknown event types are rejected.
- A subscription ends at the first `workflow_completed`, `workflow_errored`, or
  `workflow_terminated` event, even if the filter excludes that event. Subsequent
  `next()` calls return `{ done: true, value: undefined }`.
- Use `using` or `subscription[Symbol.dispose]()` to release the handle. Disposal
  completes a pending `next()` with `done: true`. One handle supports one pending
  `next()` at a time; concurrent readers should create separate subscriptions.
- A failed RPC does not advance the handle's cursor. Reconnect after a process or
  ownership change with the last event ID your application successfully processed.

History and the corresponding ledger transition share one SQLite transaction.
Delivery waits for the cell's replicated durability proof. Subscribers pull bounded
history pages; there is no per-subscriber backlog of live events in memory. Live
reads long-poll the owner for up to 25 seconds. Disposal sends routed cancellation;
the long-poll deadline bounds cleanup if the caller disconnects without disposing.
An actively observed waiting instance has resident RPC work; unobserved waiting
instances still hibernate normally. The public handle is a caller-local RPC target,
subject to celld's existing RPC-target transport restrictions.

An automatic crash replay can emit `attempt_started` again with the same Step name
and attempt number. Each observation has its own event ID: the interrupted callback
did not become a newly numbered retry. celld does not invent an attempt settlement
or exact crash time for work whose result never committed.

Explicit `restart()` preserves the instance's history and keeps event IDs increasing.
A new subscription without a cursor observes the new generation. To read an older
generation, supply its cursor (or `0` for the first generation); that subscription
still stops at the first terminal event it encounters. A selected restart does not
emit new completion events for copied checkpoints. History expires with the instance
under its configured retention and is removed by `delete()`. Existing handles reject
if that instance is deleted, expired, or replaced. Instances created before this
feature continue to execute but `subscribe()` rejects because their complete history
was never recorded.

The [Workflow example](../../examples/workflow/index.js) exposes `/events?id=ID` as
a WebSocket, with optional `cursor=EVENT_ID`. See
[the subscription test instructions](../../tests/workflow-subscribe/README.md) for
the real-runtime restart, timeout, cursor and disposal checks.

With telemetry enabled, these native transitions also produce a correlated
Workflow/Step/attempt span tree, including retry delays, sleeps, event waits and
pause intervals. See [Workflow timelines](../telemetry.md#workflow-timelines) for
OTLP/Parquet attributes, recovery semantics and a runnable motel demo. The retained
subscription history is authoritative; the telemetry exporter is sampled and
best effort.

## Differences from Cloudflare

- celld retains a successful or failed instance for 30 days by default. Each
  duration in the `retention` option can be at most 30 days.
- `locationHint` accepts the Cloudflare values, but fleet ownership selects the
  cell location.
- Non-step work cannot remain pending for more than 60 seconds.
- A step result, an event payload, and the workflow parameters each have a
  1 MiB limit.
- Rollback, a sensitive step result, and a `ReadableStream` step result are
  unavailable.
- A `workflows` entry cannot carry `schedules`, `limits`, or a `script_name`
  that names another script.
- The Workflows REST API and the `wrangler workflows` commands are
  unavailable. Drive an instance through the binding.

The [Cloudflare compatibility](../cloudflare-compat.md#services) page lists
the runtime APIs and the unsupported services.
