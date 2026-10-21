# Hard placement example

```js
const cell = await env.MYCELL.create({ name: "gpu-session", placement: "gpu" });
await cell.fetch("https://cell/run");
```

This example stores a counter and runs an alarm; it does not execute GPU kernels.
Deploy it to a fleet with a `gpu` pool. Two GPU nodes with
`CELLD_HOT_RECOVERY=1` replicate to each other. A lone GPU node waits for bucket
durability instead of recruiting a CPU follower. No GPU means unavailable, not
execution on a CPU.

```sh
celld deploy examples/placement --bucket s3://my-cells-bucket
curl 'http://localhost:8080/?name=alice'            # increment on a GPU owner
curl 'http://localhost:8080/?name=alice&mode=read'  # read without incrementing
curl 'http://localhost:8080/?name=alice&mode=rpc'   # native RPC
curl 'http://localhost:8080/?name=alice&mode=alarm'
```

`mode=register` demonstrates lazy creation. `unique=1` creates a random ID;
otherwise the name is idempotent. Passing another `placement` for the same name
fails: an existing cell cannot change pools. A regular `getByName()`/`get()`
still routes a previously registered ID according to its durable placement.

See [placement configuration and rollout](../../docs/placement.md) and
`scripts/placement-pools-smoke.py` for the isolated RustFS fault test.
