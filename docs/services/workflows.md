# Workflows

A Workflow is a durable function that runs as a named sequence of steps,
sleeps, and events. celld runs each instance as a cell. Read the
[Cloudflare Workflows documentation](https://developers.cloudflare.com/workflows/build/workers-api/)
for the API.

## Example

The [Workflow example](../../examples/workflow) fetches a document and stores
the result of a durable step.

<!-- celld-example: workflow -->

## API

- A class extends `WorkflowEntrypoint` and implements `run(event, step)`.
  `event.payload` holds the parameters, and the return value becomes the
  `output` field of `status()`.
- `step.do(name, callback)` runs a durable step, and it retries the callback on
  failure.
- `step.sleep(name, duration)` and `step.sleepUntil(name, timestamp)` stop the
  instance until a deadline.
- `step.waitForEvent(name, options)` stops the instance until a matching event.
- `env.MY_WORKFLOW.create()`, `createBatch()`, and `get(id)` return an
  instance. `deleteBatch()` deletes several instances.
- An instance has `status()`, `sendEvent()`, `pause()`, `resume()`,
  `restart()`, `terminate()`, and `delete()`.

## Replay

Each time an instance makes progress, celld calls `run()` again from the first
line. A finished step returns its stored result and does not run its callback
again. All code outside a step callback therefore runs again on every replay.
Put each subrequest, each side effect, and each value that must stay stable
inside a `step.do()` callback, as the
[rules of Workflows](https://developers.cloudflare.com/workflows/build/rules-of-workflows/)
require. A step whose result did not commit before a node failure runs again,
so a step callback must tolerate a second attempt.

`run()` can await work that is not a step, but a replay cannot resume that
await. celld fails the instance when such work keeps `run()` pending for 60
seconds while no step runs or waits.

## Sleeps, events, and retries

`step.waitForEvent()` times out after 24 hours by default. An event that
arrives before the instance reaches its wait step is buffered. A sleep, a wait,
and a pending retry each store their deadline, so a crash or a slow replay
cannot move the deadline. `status()` reports `waiting` for all three.

A waiting instance holds no isolate. Its cell hibernates when the next deadline
is further away than the near-alarm residency window. That window is one hour,
and `CELLD_ALARM_RESIDENT_MS` changes it.

`step.do()` uses the Cloudflare retry defaults when the call supplies no
`retries` object: 5 retries, a delay of 10 seconds, exponential backoff, and a
timeout of 10 minutes for one attempt.

celld sets no limit on the number of concurrent instances. Fleet memory and the
resident-cell cap bound it.

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
