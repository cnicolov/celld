# Cell placement pools

Placement is a **hard execution constraint**, not a location hint:

```js
const cell = await env.MYCELL.create({ name: "agent-42", placement: "gpu" });
await cell.fetch("https://cell/run");
// Native RPC and ordinary get()/getByName() use the same persisted placement.
```

`create()` returns a Durable Object stub after reserving placement in the bucket.
Execution is lazy: registration can succeed with no eligible node, but a fetch
or RPC fails with `PlacementUnavailable` rather than running on another pool.
Omit `name` for a new random ID. Reusing a name is idempotent **only** with the
same placement. Creation does not run the constructor or change existing state.
Unknown creation options and invalid pool names fail rather than becoming hints.

Pools are 1–63 lowercase ASCII letters, digits, `-` or `_`, starting with a
letter. `default` is the legacy pool. Omitting `placement` selects `default`.

## Node configuration

Each node belongs to exactly one pool:

```sh
# On both GPU machines, using distinct node names and persistent recovery disks:
CELLD_NODE=gpu-a CELLD_PLACEMENT_POOL=gpu CELLD_HOT_RECOVERY=1 \
  CELLD_DURABILITY=fleet CELLD_WATCH=/var/lib/celld/gpu-a \
  celld --bucket s3://my-cells-bucket \
    --listen 0.0.0.0:8080 --internal-listen 10.0.0.12:8081 \
    --advertise gpu-a.internal:8081
```

Use `gpu-b` and its own recovery disk/address on the second machine. CPU nodes
can leave `CELLD_PLACEMENT_POOL` unset. Set `CELLD_HOT_RECOVERY=1` on nodes that
should act as hot followers; it does not make a CPU node a GPU node.
Pool names are operator assertions: celld does not detect hardware, allocate
GPU devices, replicate GPU memory, or provide a GPU compute API.

Both the node identity and pool are pinned to the recovery disk. Restarting the
same disk in a different pool is refused. Legacy disks belong to `default`;
use a new **node name and data directory** for a named pool. Do not retire an old
recovery disk until `celld disk check` proves its retained obligations complete.

## Two-GPU behavior

| Available nodes | Execution | Replication / durability |
| --- | --- | --- |
| Two GPU nodes with hot recovery enabled | Exactly one GPU owner | Other GPU is an eligible durable-log follower and SQLite hot standby; bucket upload still runs |
| One GPU plus any number of CPU nodes | GPU owner only | No eligible GPU follower: new writes wait for bucket durability |
| GPU peer without hot recovery enabled | GPU execution is allowed | Not eligible as a named-pool follower; bucket fallback if there is no other eligible peer |
| No live GPU node | Unavailable, including alarms | Bucket state remains recoverable; no CPU execution fallback |

Fleet acknowledgements still require durable follower log fsyncs, **not** a
ready SQLite image. Hot images are bounded acceleration caches, and an eligible
follower may be lagging, over budget, or not warm yet. Promotion retains all
checkpoint, identity, completeness, epoch, and ownership-CAS checks; failure
falls back to conservative recovery on an eligible GPU owner. See
[hot recovery](designs/hot-recovery.md). There is no active-active execution.

Losing every copy before fleet-acknowledged data reaches the bucket is not made
safe by placement. Keep the ordinary fleet/disk-retirement guarantees; choose
`CELLD_DURABILITY=bucket` if every acknowledgement must wait for bucket storage.

## Why pools, not per-cell replica groups

celld has **one follower ensemble per owner process session**. This version
filters that entire ensemble to the owner's pool. A default node cannot own GPU
cells, and a GPU node cannot own default cells. Default pools retain ordinary
log replication to default peers, even when hot recovery is disabled. Named
pools require matching, hot-capable peers for their replication ensemble.

Any ingress node may forward a request to an eligible owner. New placement,
expired-owner takeover, shutdown adoption, load shedding, ownership rebalancing,
and alarm waking all preserve the pool. Rebalance weights and ownership targets
are computed **within** a pool; other pools cannot donate or receive its cells.
Alarm discovery has one advisory waker lease per pool; fleet GC remains global.

Built-in cells (KV, D1, queues, workflows, cron) still use `default`. Keep default
capacity if the application needs them. Per-cell replica sets, multiple pools on
one node, changing an existing cell's pool, and preferential selection of warm
standbys are not implemented.

## Persistence and rollout

`cells/<scope>/own.json` carries immutable `placement_pool` through claims and
releases. Registration is an absent-only CAS with no owner at epoch zero. The
first eligible owner claims epoch one. Legacy ownership and node records without
pool metadata decode as `default`; missing hot capability means disabled.
Node leases advertise `placement_pool` and `hot_recovery` independently of load.

New Durable Object deployments require the `cell-placement-v1` deployment
feature. **Upgrade every node before introducing named pools.** Older binaries
do not understand hard placement and can ignore the new ownership field; this
is not a safe mixed-version scheduling protocol. Drain/fence older processes,
complete their recovery obligations, deploy with the updated CLI, then start
named-pool nodes. Do not roll back to placement-unaware binaries while placed
cells remain in the bucket.

## Validation

```sh
cargo test -p celld --lib placement_tests --locked
rustc --edition=2021 --test crates/logic/lib.rs -o target/logic-placement-tests
target/logic-placement-tests
cargo build -p celld --bin celld --locked
python3 -m pip install boto3
python3 scripts/placement-pools-smoke.py --endpoint http://127.0.0.1:19000
```

The smoke requires a running RustFS with demo credentials (`celld-demo` /
`celld-demo-secret`, overridable with standard AWS environment variables).
It creates its **own** bucket and three celld processes, using `gpu` as a
synthetic capability label. It checks lazy/immutable creation, CPU ingress, RPC,
legacy default routing, bucket-only owner crash/restart, same-pool standby
materialization and promotion, alarms, and loss of all GPU capacity. Existing
clusters are untouched. Logs, the report, and the test bucket remain as evidence.
This does not qualify physical GPU execution or live S3/GCS/Azure rollout.
