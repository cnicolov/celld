# Cross-isolate RPC capabilities

This is the minimal Cloudflare OS pattern: an HTTP Worker calls a Durable Object
that returns an `RpcTarget`, then invokes the returned target. It also exercises
callbacks in the reverse direction, forwarding, callable targets, property
pipelining, service stubs with props, duplication/disposal, streaming, and a
storage write through a returned capability. `/loaded` exercises forwarding and
callbacks through a third isolate, a dynamically loaded Worker.

```sh
cargo build -p celld
target/debug/celld dev examples/rpc-capabilities --no-watch --port 8788
curl http://127.0.0.1:8788/scalar
curl http://127.0.0.1:8788/capability
curl http://127.0.0.1:8788/callback
curl http://127.0.0.1:8788/tracing
```

All calls should return HTTP 200. On v0.6.1, callbacks fail with
`RPC stubs cannot cross isolate boundaries yet.`; the returned target cannot be
used or disposed.

Run the full-binary regression:

```sh
cargo test -p celld --test rpc_capabilities
```

The test uses a temporary project and local state, and checks eager calls after
disposal, call ordering with asynchronous interleaving, reentrant critical
sections, error propagation, durable writes, and exactly-once target and result
disposal. It also checks aliased handles and rollback of handle transfers and
stream locks when serialization fails.

`/tracing` checks the additional Cloudflare OS dependency on
`cloudflare:workers`' span helpers: synchronous and asynchronous callbacks,
return values, error identity, attributes, and manual span ending. These are
unsampled compatibility spans (`isTraced === false`), matching workerd with
custom tracing disabled. The helper API does not export custom application
spans into celld's telemetry sink.

Routes are process-local. A transient target is tied to its originating isolate
and Durable Object residency; it does not become a cross-node or persistent
capability.
