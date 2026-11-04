// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Operational metrics, named after the OpenTelemetry semantic conventions.
//!
//! Off unless `CELLD_METRICS=1`. A disabled node allocates no series, and a
//! recording site costs one `OnceLock` read. Like the monitor, these values are
//! observations for an operator's backend: no placement, ownership or admission
//! decision reads them.
//!
//! Instruments that count events (counters and histograms) live in [`Metrics`]
//! and are recorded where the event happens. Values that celld already keeps
//! elsewhere (memory, residency, followers) are read only when an exporter
//! collects, through callbacks registered with [`observe`], so they cost nothing
//! between collections.
//!
//! One collection renders two ways: OTLP for a collector
//! ([`crate::otlp::metrics_request`]) and the Prometheus text format for the
//! operator listener's `/metrics`, using the specification's OTLP-to-Prometheus
//! name translation. Sums are cumulative from process start, which both formats
//! and PromQL read.

use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::fmt::Write as _;
use std::hash::BuildHasher as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Duration;

pub fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        crate::env_vars::flag("CELLD_METRICS", false).expect("validated CELLD_METRICS")
    })
}

/// The process's instruments, or `None` when metrics are off.
pub fn get() -> Option<&'static Metrics> {
    static METRICS: OnceLock<Option<Metrics>> = OnceLock::new();
    METRICS
        .get_or_init(|| enabled().then(Metrics::new))
        .as_ref()
}

/// Register a callback that reads current values at collection time. A no-op
/// when metrics are off, so subsystems can register unconditionally.
pub fn observe(observer: impl Fn(&mut Vec<Metric>) + Send + Sync + 'static) {
    if let Some(metrics) = get() {
        lock(&metrics.observers).push(Box::new(observer));
    }
}

/// The identity every exported point carries. It matches the resource that
/// the trace and log exporters send, so a backend joins the three signals.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Resource {
    pub node: String,
    pub region: String,
    pub service: String,
}

static RESOURCE: OnceLock<Resource> = OnceLock::new();

pub fn set_resource(resource: Resource) {
    let _ = RESOURCE.set(resource);
}

pub fn resource() -> Option<&'static Resource> {
    RESOURCE.get()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// A monotonic sum.
    Counter,
    /// A non-monotonic sum: a current amount that rises and falls.
    UpDownCounter,
    Gauge,
    Histogram,
}

#[derive(Debug)]
pub struct Descriptor {
    pub name: &'static str,
    pub description: &'static str,
    /// A UCUM unit: `s`, `By`, `1`, or a `{noun}` annotation for counts.
    pub unit: &'static str,
    pub kind: Kind,
}

/// Attribute key/value pairs, sorted by key so equal sets compare equal.
pub type Attributes = Vec<(&'static str, String)>;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Number {
    Int(i64),
    Double(f64),
}

#[derive(Clone, Debug, PartialEq)]
pub struct HistogramPoint {
    pub attributes: Attributes,
    pub count: u64,
    pub sum: f64,
    /// One count per bucket: `bounds.len() + 1` entries, the last above every bound.
    pub bucket_counts: Vec<u64>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Data {
    Numbers(Vec<(Attributes, Number)>),
    Histogram {
        bounds: &'static [f64],
        points: Vec<HistogramPoint>,
    },
}

#[derive(Clone, Debug)]
pub struct Metric {
    pub descriptor: &'static Descriptor,
    pub data: Data,
}

impl Metric {
    pub fn numbers(descriptor: &'static Descriptor, points: Vec<(Attributes, Number)>) -> Self {
        Self {
            descriptor,
            data: Data::Numbers(points),
        }
    }

    pub fn single(descriptor: &'static Descriptor, value: Number) -> Self {
        Self::numbers(descriptor, vec![(Vec::new(), value)])
    }
}

/// The `celld.script` value for a script name. Worker Loader isolates are
/// named per loaded Worker, so they share one label instead of creating a
/// series for each dynamic Worker.
pub fn script_label(script: &str) -> &str {
    if script.starts_with("__loader:") {
        "__loader"
    } else {
        script
    }
}

/// The `celld.cell.class` value for a cell scope (`Class:instance`). A scope
/// without a class gets the conventions' catch-all rather than its own series.
pub fn cell_class(scope: &str) -> &str {
    scope.split_once(':').map_or("_OTHER", |(class, _)| class)
}

/// Count a refused claim or creation of a cell whose ownership record is lost.
pub fn record_lost_ownership(cell: &str, path: &'static str) {
    if let Some(metrics) = get() {
        metrics.ownership_record_losses.add(
            &[
                ("celld.cell.class", cell_class(cell)),
                ("celld.ownership.path", path),
            ],
            1,
        );
    }
}

pub fn attributes(pairs: &[(&'static str, &str)]) -> Attributes {
    let mut owned: Attributes = pairs
        .iter()
        .map(|(key, value)| (*key, (*value).to_string()))
        .collect();
    owned.sort_by(|a, b| a.0.cmp(b.0));
    owned
}

/// The semantic conventions' advisory boundaries for request durations.
pub const DURATION_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0,
];

/// Turns and follower fsyncs are routinely under five milliseconds, where the
/// default boundaries would put almost everything into one bucket.
pub const FAST_BUCKETS: &[f64] = &[
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// Series keyed by attribute set. A lookup hashes the borrowed pairs, in any
/// order, under a shared lock, so recording into an existing series neither
/// allocates nor excludes other recorders. Only a new attribute set takes the
/// write lock and allocates its key.
struct Keyed<T> {
    hasher: RandomState,
    series: RwLock<HashMap<u64, Vec<Entry<T>>>>,
}

/// One attribute set and its series. Distinct sets can share a hash bucket.
type Entry<T> = (Attributes, Arc<T>);

impl<T> Keyed<T> {
    fn new() -> Self {
        Self {
            hasher: RandomState::new(),
            series: RwLock::new(HashMap::new()),
        }
    }

    fn get_or_insert(&self, pairs: &[(&'static str, &str)], make: impl FnOnce() -> T) -> Arc<T> {
        // A sum of per-pair hashes does not depend on the order of the pairs.
        let hash = pairs.iter().fold(0u64, |sum, pair| {
            sum.wrapping_add(self.hasher.hash_one(pair))
        });
        let found = read(&self.series)
            .get(&hash)
            .and_then(|entries| find(entries, pairs));
        if let Some(found) = found {
            return found;
        }
        let mut series = write(&self.series);
        let entries = series.entry(hash).or_default();
        if let Some(found) = find(entries, pairs) {
            return found;
        }
        let value = Arc::new(make());
        entries.push((attributes(pairs), value.clone()));
        value
    }

    /// Every series, sorted by attributes so a collection renders in a
    /// stable order.
    fn snapshot(&self) -> Vec<Entry<T>> {
        let mut all: Vec<_> = read(&self.series)
            .values()
            .flatten()
            .map(|(attributes, value)| (attributes.clone(), value.clone()))
            .collect();
        all.sort_by(|a, b| a.0.cmp(&b.0));
        all
    }
}

fn find<T>(entries: &[Entry<T>], pairs: &[(&'static str, &str)]) -> Option<Arc<T>> {
    entries
        .iter()
        .find(|(attributes, _)| {
            attributes.len() == pairs.len()
                && pairs.iter().all(|pair| {
                    attributes
                        .iter()
                        .any(|(key, value)| (*key, value.as_str()) == *pair)
                })
        })
        .map(|(_, value)| value.clone())
}

pub struct Counter {
    descriptor: &'static Descriptor,
    series: Keyed<AtomicU64>,
}

impl Counter {
    fn new(descriptor: &'static Descriptor) -> Self {
        Self {
            descriptor,
            series: Keyed::new(),
        }
    }

    pub fn add(&self, pairs: &[(&'static str, &str)], value: u64) {
        self.series
            .get_or_insert(pairs, || AtomicU64::new(0))
            .fetch_add(value, Ordering::Relaxed);
    }

    fn collect(&self) -> Option<Metric> {
        let series = self.series.snapshot();
        (!series.is_empty()).then(|| {
            Metric::numbers(
                self.descriptor,
                series
                    .into_iter()
                    .map(|(attributes, total)| {
                        (
                            attributes,
                            Number::Int(total.load(Ordering::Relaxed) as i64),
                        )
                    })
                    .collect(),
            )
        })
    }
}

pub struct Histogram {
    descriptor: &'static Descriptor,
    bounds: &'static [f64],
    series: Keyed<Series>,
}

impl Histogram {
    fn new(descriptor: &'static Descriptor, bounds: &'static [f64]) -> Self {
        Self {
            descriptor,
            bounds,
            series: Keyed::new(),
        }
    }

    /// The series for one attribute set. A hot path can keep the handle and
    /// record into it without looking it up again.
    pub fn series(&self, pairs: &[(&'static str, &str)]) -> Arc<Series> {
        self.series
            .get_or_insert(pairs, || Series::new(self.bounds))
    }

    pub fn record(&self, pairs: &[(&'static str, &str)], seconds: f64) {
        self.series(pairs).record(seconds);
    }

    fn collect(&self) -> Option<Metric> {
        let series = self.series.snapshot();
        (!series.is_empty()).then(|| Metric {
            descriptor: self.descriptor,
            data: Data::Histogram {
                bounds: self.bounds,
                points: series
                    .into_iter()
                    .map(|(attributes, series)| series.point(attributes))
                    .collect(),
            },
        })
    }
}

/// One histogram series of durations. Bucket counts are independent atomics,
/// so a collection can observe a recording half-applied; the point's count is
/// derived from the buckets it read, which keeps it consistent with them.
///
/// The sum is whole nanoseconds, so a recording is plain atomic adds. A
/// floating-point sum needs a compare-and-swap loop, which retries under
/// contention and measured about twice as slow with eight recording threads.
pub struct Series {
    bounds: &'static [f64],
    buckets: Box<[AtomicU64]>,
    sum_ns: AtomicU64,
}

impl Series {
    fn new(bounds: &'static [f64]) -> Self {
        Self {
            bounds,
            buckets: (0..=bounds.len()).map(|_| AtomicU64::new(0)).collect(),
            sum_ns: AtomicU64::new(0),
        }
    }

    pub fn record(&self, seconds: f64) {
        if !seconds.is_finite() {
            return;
        }
        let seconds = seconds.max(0.0);
        // Explicit bounds are inclusive upper limits: a value equal to a bound
        // belongs to that bound's bucket.
        let index = self.bounds.partition_point(|bound| *bound < seconds);
        self.buckets[index].fetch_add(1, Ordering::Relaxed);
        self.sum_ns
            .fetch_add((seconds * 1e9) as u64, Ordering::Relaxed);
    }

    pub fn record_duration(&self, duration: Duration) {
        self.record(duration.as_secs_f64());
    }

    fn point(&self, attributes: Attributes) -> HistogramPoint {
        let bucket_counts: Vec<u64> = self
            .buckets
            .iter()
            .map(|bucket| bucket.load(Ordering::Relaxed))
            .collect();
        HistogramPoint {
            attributes,
            count: bucket_counts.iter().sum(),
            sum: self.sum_ns.load(Ordering::Relaxed) as f64 / 1e9,
            bucket_counts,
        }
    }
}

macro_rules! descriptor {
    ($name:ident, $metric:literal, $kind:ident, $unit:literal, $description:literal) => {
        pub static $name: Descriptor = Descriptor {
            name: $metric,
            description: $description,
            unit: $unit,
            kind: Kind::$kind,
        };
    };
}

// Process: the semantic conventions' own names.
descriptor!(
    PROCESS_CPU_TIME,
    "process.cpu.time",
    Counter,
    "s",
    "Total CPU seconds broken down by mode."
);
descriptor!(
    PROCESS_MEMORY_USAGE,
    "process.memory.usage",
    UpDownCounter,
    "By",
    "The amount of physical memory in use (RSS)."
);
descriptor!(
    PROCESS_UPTIME,
    "process.uptime",
    Gauge,
    "s",
    "The time the process has been running."
);

// Node memory and the pressure classifier's view of it.
descriptor!(
    MEMORY_USAGE,
    "celld.memory.usage",
    UpDownCounter,
    "By",
    "Memory measured by source: allocator-adjusted RSS and the cgroup charges."
);
descriptor!(
    MEMORY_LIMIT,
    "celld.memory.limit",
    UpDownCounter,
    "By",
    "Memory ceilings: the ordinary shedding ceiling, the hard cap, and the cgroup limit."
);
descriptor!(
    MEMORY_UTILIZATION,
    "celld.memory.utilization",
    Gauge,
    "1",
    "The pressure measurement as a fraction of the ordinary ceiling."
);
descriptor!(
    MEMORY_SHEDDING,
    "celld.memory.shedding",
    UpDownCounter,
    "1",
    "1 while the node sheds cells to recover memory, by reason."
);

// Isolates. `v8js.memory.heap.used` is defined per heap space, and V8 has no
// standard limit metric, so total use, limit and utilization are celld's.
descriptor!(
    ISOLATE_HEAP_USAGE,
    "celld.isolate.heap.usage",
    UpDownCounter,
    "By",
    "V8 heap used, summed over the script's isolates."
);
descriptor!(
    ISOLATE_HEAP_LIMIT,
    "celld.isolate.heap.limit",
    UpDownCounter,
    "By",
    "The heap limit of one of the script's isolates."
);
descriptor!(
    ISOLATE_HEAP_UTILIZATION,
    "celld.isolate.heap.utilization",
    Gauge,
    "1",
    "Heap used over its limit, for the script's fullest isolate."
);
descriptor!(
    ISOLATE_COUNT,
    "celld.isolate.count",
    UpDownCounter,
    "{isolate}",
    "Live isolates by state."
);
descriptor!(
    ISOLATE_CONDEMNATIONS,
    "celld.isolate.condemnations",
    Counter,
    "{condemnation}",
    "Isolates condemned: at their V8 heap limit, or after a fatal guest error."
);
descriptor!(
    ISOLATE_HEAP_RECOVERIES,
    "celld.isolate.heap.recoveries",
    Counter,
    "{recovery}",
    "Condemned isolates whose heap fell back under the recovery line."
);
descriptor!(
    ISOLATE_ADMISSION_REFUSALS,
    "celld.isolate.admission.refusals",
    Counter,
    "{refusal}",
    "Work refused because the isolate's heap was over the admission share."
);
descriptor!(
    TURN_DURATION,
    "celld.turn.duration",
    Histogram,
    "s",
    "Time one synchronous turn held its isolate."
);
descriptor!(
    TURN_WAIT_DURATION,
    "celld.turn.wait.duration",
    Histogram,
    "s",
    "Time a turn waited for its isolate."
);

// Cells.
descriptor!(
    CELL_COUNT,
    "celld.cell.count",
    UpDownCounter,
    "{cell}",
    "Cells on this node by lifecycle phase."
);
descriptor!(
    CELL_HANDOFFS,
    "celld.cell.handoffs",
    Counter,
    "{handoff}",
    "Cells handed to another node, rebalances included; error.type is set on failure."
);
descriptor!(
    CELL_REBALANCES,
    "celld.cell.rebalances",
    Counter,
    "{rebalance}",
    "Cells moved by rebalancing; error.type is set on failure."
);
descriptor!(
    CELL_ACTIVATION_DURATION,
    "celld.cell.activation.duration",
    Histogram,
    "s",
    "Time a cold route took to make a cell serve here or find its owner."
);
descriptor!(
    WEBSOCKET_COUNT,
    "celld.websocket.count",
    UpDownCounter,
    "{connection}",
    "WebSocket connections held by this node."
);

// Invocations: the FaaS conventions for cell events and queue batches, HTTP
// for stateless Workers.
descriptor!(
    FAAS_INVOKE_DURATION,
    "faas.invoke_duration",
    Histogram,
    "s",
    "Duration of a cell event or queue batch, from admission to its outcome."
);
descriptor!(
    FAAS_ERRORS,
    "faas.errors",
    Counter,
    "{error}",
    "Cell events and queue batches that failed; error.type names the failure."
);
descriptor!(
    HTTP_SERVER_REQUEST_DURATION,
    "http.server.request.duration",
    Histogram,
    "s",
    "Duration of stateless Worker HTTP requests."
);

// Durability, the bucket, and the fleet.
descriptor!(
    DURABILITY_PROOF_DURATION,
    "celld.durability.proof.duration",
    Histogram,
    "s",
    "Time a write waited for its durability proof, by proof source."
);
descriptor!(
    DURABILITY_FOLLOWER_COUNT,
    "celld.durability.follower.count",
    UpDownCounter,
    "{follower}",
    "Followers in this node's write ensemble; 0 means bucket proofs only."
);
descriptor!(
    LOG_HEAD_STALLS,
    "celld.log.head.stalls",
    Counter,
    "{stall}",
    "Times the replicated log head stalled."
);
descriptor!(
    BUCKET_OPERATION_DURATION,
    "celld.bucket.operation.duration",
    Histogram,
    "s",
    "Duration of object-store operations; error.type is set on failure."
);
descriptor!(
    FLEET_NODE_COUNT,
    "celld.fleet.node.count",
    UpDownCounter,
    "{node}",
    "Fleet nodes seen by this node's last sample, by state."
);
descriptor!(OWNERSHIP_RECORD_LOSSES, "celld.ownership.record.losses", Counter, "{loss}", "Claims or creations refused because a cell's ownership record is missing while its LTX is in the bucket; each holds the cell refused for 10 s.");
descriptor!(
    NODE_LEASE_RENEWALS,
    "celld.node.lease.renewals",
    Counter,
    "{renewal}",
    "Node lease acquire and renew attempts; error.type is set when one did not apply."
);
descriptor!(
    TELEMETRY_DROPPED,
    "celld.telemetry.dropped",
    Counter,
    "{event}",
    "Spans and logs dropped because the export queue was full or a record was refused."
);

type Observer = Box<dyn Fn(&mut Vec<Metric>) + Send + Sync>;

/// Instruments recorded where an event happens. Read the field and call it;
/// `get()` returns `None` when metrics are off.
pub struct Metrics {
    pub isolate_condemnations: Counter,
    pub isolate_heap_recoveries: Counter,
    pub isolate_admission_refusals: Counter,
    pub turn_duration: Histogram,
    pub turn_wait_duration: Histogram,
    pub cell_activation_duration: Histogram,
    pub faas_invoke_duration: Histogram,
    pub faas_errors: Counter,
    pub http_server_request_duration: Histogram,
    pub durability_proof_duration: Histogram,
    pub log_head_stalls: Counter,
    pub bucket_operation_duration: Histogram,
    pub node_lease_renewals: Counter,
    pub ownership_record_losses: Counter,
    observers: Mutex<Vec<Observer>>,
    started_unix_ns: u64,
    started_mono_us: u64,
}

impl Metrics {
    fn new() -> Self {
        Self {
            isolate_condemnations: Counter::new(&ISOLATE_CONDEMNATIONS),
            isolate_heap_recoveries: Counter::new(&ISOLATE_HEAP_RECOVERIES),
            isolate_admission_refusals: Counter::new(&ISOLATE_ADMISSION_REFUSALS),
            turn_duration: Histogram::new(&TURN_DURATION, FAST_BUCKETS),
            turn_wait_duration: Histogram::new(&TURN_WAIT_DURATION, FAST_BUCKETS),
            cell_activation_duration: Histogram::new(&CELL_ACTIVATION_DURATION, DURATION_BUCKETS),
            faas_invoke_duration: Histogram::new(&FAAS_INVOKE_DURATION, DURATION_BUCKETS),
            faas_errors: Counter::new(&FAAS_ERRORS),
            http_server_request_duration: Histogram::new(
                &HTTP_SERVER_REQUEST_DURATION,
                DURATION_BUCKETS,
            ),
            durability_proof_duration: Histogram::new(&DURABILITY_PROOF_DURATION, FAST_BUCKETS),
            log_head_stalls: Counter::new(&LOG_HEAD_STALLS),
            bucket_operation_duration: Histogram::new(&BUCKET_OPERATION_DURATION, DURATION_BUCKETS),
            node_lease_renewals: Counter::new(&NODE_LEASE_RENEWALS),
            ownership_record_losses: Counter::new(&OWNERSHIP_RECORD_LOSSES),
            observers: Mutex::new(Vec::new()),
            started_unix_ns: unix_ns(),
            started_mono_us: crate::asyncrt::mono_us(),
        }
    }

    /// The start of every cumulative sum: a restart begins new series.
    pub fn started_unix_ns(&self) -> u64 {
        self.started_unix_ns
    }

    /// Every instrument with data, then every observer's current values.
    /// Observations of one metric from several observers merge into one.
    pub fn collect(&self) -> Vec<Metric> {
        let mut metrics: Vec<Metric> = [
            self.isolate_condemnations.collect(),
            self.isolate_heap_recoveries.collect(),
            self.isolate_admission_refusals.collect(),
            self.turn_duration.collect(),
            self.turn_wait_duration.collect(),
            self.cell_activation_duration.collect(),
            self.faas_invoke_duration.collect(),
            self.faas_errors.collect(),
            self.http_server_request_duration.collect(),
            self.durability_proof_duration.collect(),
            self.log_head_stalls.collect(),
            self.bucket_operation_duration.collect(),
            self.node_lease_renewals.collect(),
            self.ownership_record_losses.collect(),
        ]
        .into_iter()
        .flatten()
        .collect();
        self.collect_process(&mut metrics);
        collect_memory(&mut metrics);
        collect_node(&mut metrics);
        for observer in lock(&self.observers).iter() {
            observer(&mut metrics);
        }
        merge(metrics)
    }

    fn collect_process(&self, metrics: &mut Vec<Metric>) {
        if let Some((user, system)) = process_cpu_seconds() {
            metrics.push(Metric::numbers(
                &PROCESS_CPU_TIME,
                vec![
                    (attributes(&[("cpu.mode", "user")]), Number::Double(user)),
                    (
                        attributes(&[("cpu.mode", "system")]),
                        Number::Double(system),
                    ),
                ],
            ));
        }
        let rss = crate::memory::resident_bytes();
        if rss > 0 {
            metrics.push(Metric::single(
                &PROCESS_MEMORY_USAGE,
                Number::Int(rss as i64),
            ));
        }
        let uptime_us = crate::asyncrt::mono_us().saturating_sub(self.started_mono_us);
        metrics.push(Metric::single(
            &PROCESS_UPTIME,
            Number::Double(uptime_us as f64 / 1e6),
        ));
    }
}

/// Cell phases, lifetime handoff and rebalance counts, and whether the node
/// sheds, as the core reports them.
pub fn collect_core(core: &crate::actor::CoreMetrics, out: &mut Vec<Metric>) {
    if !core.phases.is_empty() {
        out.push(Metric::numbers(
            &CELL_COUNT,
            core.phases
                .iter()
                .map(|(phase, count)| {
                    (
                        attributes(&[("celld.cell.phase", phase)]),
                        Number::Int(*count as i64),
                    )
                })
                .collect(),
        ));
    }
    let activity = &core.activity;
    out.push(outcomes(
        &CELL_HANDOFFS,
        activity.handed_off,
        activity.handoff_failed,
    ));
    out.push(outcomes(
        &CELL_REBALANCES,
        activity.rebalanced,
        activity.rebalance_failed,
    ));
    let shedding = match core.shed_reason {
        Some(reason) => (
            attributes(&[("celld.memory.shedding.reason", reason)]),
            Number::Int(1),
        ),
        None => (Vec::new(), Number::Int(0)),
    };
    out.push(Metric::numbers(&MEMORY_SHEDDING, vec![shedding]));
}

/// A counter split by outcome. The core does not classify its failures
/// further, so they carry the conventions' catch-all `error.type`.
fn outcomes(descriptor: &'static Descriptor, succeeded: u64, failed: u64) -> Metric {
    Metric::numbers(
        descriptor,
        vec![
            (Vec::new(), Number::Int(succeeded as i64)),
            (
                attributes(&[("error.type", "_OTHER")]),
                Number::Int(failed as i64),
            ),
        ],
    )
}

/// Node memory by source, the pressure ceilings, and the pressure
/// measurement as a share of the ordinary ceiling. The measurement repeats
/// the core's own arithmetic over a fresh sample, so it can differ slightly
/// from the last sample the core classified.
fn collect_memory(out: &mut Vec<Metric>) {
    static CONFIG: OnceLock<Option<celld_logic::pressure::PressureConfig>> = OnceLock::new();
    let config = *CONFIG.get_or_init(|| crate::machine::pressure_config().ok());
    let sample = crate::memory::sample();
    let mut usage = vec![(
        attributes(&[("celld.memory.source", "in_use")]),
        Number::Int(sample.in_use_bytes as i64),
    )];
    for (source, bytes) in [
        ("cgroup_working_set", sample.cgroup_working_set_bytes),
        ("cgroup_current", sample.cgroup_current_bytes),
    ] {
        if let Some(bytes) = bytes {
            usage.push((
                attributes(&[("celld.memory.source", source)]),
                Number::Int(bytes as i64),
            ));
        }
    }
    out.push(Metric::numbers(&MEMORY_USAGE, usage));

    let high_bytes = config.and_then(|config| config.high_bytes);
    let limits: Vec<_> = [
        ("ordinary", high_bytes),
        ("hard", config.and_then(|config| config.rss_hard_bytes)),
        ("cgroup", cgroup_limit_bytes()),
    ]
    .into_iter()
    .filter_map(|(kind, bytes)| {
        Some((
            attributes(&[("celld.memory.limit.kind", kind)]),
            Number::Int(bytes? as i64),
        ))
    })
    .collect();
    if !limits.is_empty() {
        out.push(Metric::numbers(&MEMORY_LIMIT, limits));
    }

    if let Some(high_bytes) = high_bytes.filter(|bytes| *bytes > 0) {
        let load = celld_logic::pressure::Load {
            resident_cells: 0,
            rss_bytes: sample.rss_bytes,
            in_use_bytes: sample.in_use_bytes,
            cgroup_working_set_bytes: sample.cgroup_working_set_bytes,
            cgroup_current_bytes: sample.cgroup_current_bytes,
            container_reserved_bytes: crate::container::reserved_memory_bytes(),
        };
        out.push(Metric::single(
            &MEMORY_UTILIZATION,
            Number::Double(load.memory_bytes() as f64 / high_bytes as f64),
        ));
    }
}

#[cfg(target_os = "linux")]
fn cgroup_limit_bytes() -> Option<u64> {
    crate::memory::cgroup_limit_bytes()
}

#[cfg(not(target_os = "linux"))]
fn cgroup_limit_bytes() -> Option<u64> {
    None
}

/// Values the node already publishes for its peers and its telemetry pump.
fn collect_node(out: &mut Vec<Metric>) {
    if let Some(load) = crate::asyncrt::services().node_load() {
        out.push(Metric::single(
            &WEBSOCKET_COUNT,
            Number::Int(load.host_websockets.load(Ordering::Relaxed) as i64),
        ));
    }
    if let Some(dropped) = crate::telemetry::dropped_total() {
        out.push(Metric::single(
            &TELEMETRY_DROPPED,
            Number::Int(dropped as i64),
        ));
    }
}

/// Where and how often to push metrics: the trace and log collector from
/// `CELLD_OTEL`, every `OTEL_METRIC_EXPORT_INTERVAL` milliseconds (default 60 s).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OtlpExport {
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub timeout: Duration,
    pub interval: Duration,
}

impl OtlpExport {
    /// `None` unless metrics are on and `CELLD_OTEL` names a collector. The
    /// bucket sink has no metrics form; `/metrics` serves that deployment.
    pub fn from_telemetry(
        config: Option<&crate::telemetry::Config>,
    ) -> anyhow::Result<Option<Self>> {
        let interval = crate::env_vars::positive::<u64>("OTEL_METRIC_EXPORT_INTERVAL")?
            .map_or(Duration::from_secs(60), Duration::from_millis);
        let Some(config) = config.filter(|_| enabled()) else {
            return Ok(None);
        };
        let crate::telemetry::SinkChoice::Otlp { endpoint } = &config.sink else {
            return Ok(None);
        };
        Ok(Some(Self {
            url: format!("{endpoint}/v1/metrics"),
            headers: config.otlp_headers.clone(),
            timeout: config.otlp_timeout,
            interval,
        }))
    }
}

/// Push one collection per interval until the process exits. `collect`
/// supplies the complete collection, including values only the caller can
/// reach. A failed push is retried like a trace batch and then dropped:
/// cumulative sums carry the counts into the next push.
pub async fn export_otlp<F, Fut>(export: OtlpExport, collect: F)
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Vec<Metric>>,
{
    let (Some(metrics), Some(resource)) = (get(), resource()) else {
        return;
    };
    let client = match reqwest::Client::builder().timeout(export.timeout).build() {
        Ok(client) => client,
        Err(error) => {
            tracing::warn!(%error, "metrics export disabled: no OTLP client");
            return;
        }
    };
    let mut tick = crate::asyncrt::interval(export.interval);
    tick.set_missed_tick_behavior(crate::asyncrt::MissedTickBehavior::Delay);
    tick.tick().await;
    loop {
        tick.tick().await;
        let collected = collect().await;
        let body = crate::otlp::metrics_request(
            &collected,
            metrics.started_unix_ns(),
            now_unix_ns(),
            resource,
        );
        crate::telemetry::post_otlp(&client, &export.url, &export.headers, body.into()).await;
    }
}

fn merge(metrics: Vec<Metric>) -> Vec<Metric> {
    let mut merged: Vec<Metric> = Vec::with_capacity(metrics.len());
    for metric in metrics {
        let Some(existing) = merged
            .iter_mut()
            .find(|existing| std::ptr::eq(existing.descriptor, metric.descriptor))
        else {
            merged.push(metric);
            continue;
        };
        match (&mut existing.data, metric.data) {
            (Data::Numbers(points), Data::Numbers(more)) => points.extend(more),
            (Data::Histogram { points, .. }, Data::Histogram { points: more, .. }) => {
                points.extend(more)
            }
            _ => debug_assert!(
                false,
                "{} observed with two data types",
                metric.descriptor.name
            ),
        }
    }
    merged
}

fn unix_ns() -> u64 {
    (crate::telemetry::now_unix_us().max(0) as u64).saturating_mul(1_000)
}

/// Collection time for an export, in Unix nanoseconds.
pub fn now_unix_ns() -> u64 {
    unix_ns()
}

#[cfg(unix)]
fn process_cpu_seconds() -> Option<(f64, f64)> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage writes a complete rusage on success and reads nothing.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return None;
    }
    // SAFETY: the successful call above initialized `usage`.
    let usage = unsafe { usage.assume_init() };
    let seconds = |time: libc::timeval| time.tv_sec as f64 + time.tv_usec as f64 / 1e6;
    Some((seconds(usage.ru_utime), seconds(usage.ru_stime)))
}

#[cfg(not(unix))]
fn process_cpu_seconds() -> Option<(f64, f64)> {
    None
}

// Observability must not turn another thread's panic into this one's.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn read<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn write<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// The Prometheus text exposition (format 0.0.4) of one collection.
///
/// Names follow the OpenTelemetry specification's translation: dots become
/// underscores, the unit becomes a suffix (`_seconds`, `_bytes`, `_ratio` for
/// a unitless gauge; `{annotations}` are dropped), and counters end in
/// `_total`. The resource becomes a `target_info` series, which a collector's
/// Prometheus receiver turns back into resource attributes.
pub fn prometheus_text(metrics: &[Metric], resource: Option<&Resource>) -> String {
    let mut out = String::new();
    if let Some(resource) = resource {
        out.push_str("# HELP target_info Target metadata\n# TYPE target_info gauge\n");
        let labels = [
            ("service_name", resource.service.as_str()),
            ("service_version", env!("CARGO_PKG_VERSION")),
            ("service_instance_id", resource.node.as_str()),
            ("celld_region", resource.region.as_str()),
        ];
        out.push_str("target_info");
        write_labels(
            &mut out,
            labels.iter().map(|(key, value)| (*key, *value)),
            None,
        );
        out.push_str(" 1\n");
    }
    for metric in metrics {
        let name = prometheus_name(metric.descriptor);
        let kind = match metric.descriptor.kind {
            Kind::Counter => "counter",
            Kind::UpDownCounter | Kind::Gauge => "gauge",
            Kind::Histogram => "histogram",
        };
        let _ = writeln!(
            out,
            "# HELP {name} {}",
            metric
                .descriptor
                .description
                .replace('\\', "\\\\")
                .replace('\n', "\\n")
        );
        let _ = writeln!(out, "# TYPE {name} {kind}");
        match &metric.data {
            Data::Numbers(points) => {
                for (attributes, value) in points {
                    out.push_str(&name);
                    write_labels(&mut out, label_pairs(attributes), None);
                    out.push(' ');
                    write_number(&mut out, *value);
                    out.push('\n');
                }
            }
            Data::Histogram { bounds, points } => {
                for point in points {
                    let mut cumulative = 0u64;
                    for (index, count) in point.bucket_counts.iter().enumerate() {
                        cumulative = cumulative.saturating_add(*count);
                        let bound = bounds
                            .get(index)
                            .map_or_else(|| "+Inf".to_string(), |bound| format_float(*bound));
                        let _ = write!(out, "{name}_bucket");
                        write_labels(&mut out, label_pairs(&point.attributes), Some(&bound));
                        let _ = writeln!(out, " {cumulative}");
                    }
                    let _ = write!(out, "{name}_sum");
                    write_labels(&mut out, label_pairs(&point.attributes), None);
                    let _ = writeln!(out, " {}", format_float(point.sum));
                    let _ = write!(out, "{name}_count");
                    write_labels(&mut out, label_pairs(&point.attributes), None);
                    let _ = writeln!(out, " {}", point.count);
                }
            }
        }
    }
    out
}

fn label_pairs(attributes: &Attributes) -> impl Iterator<Item = (&'static str, &str)> {
    attributes.iter().map(|(key, value)| (*key, value.as_str()))
}

fn write_labels<'a>(
    out: &mut String,
    labels: impl Iterator<Item = (&'static str, &'a str)>,
    le: Option<&str>,
) {
    let mut labels = labels.peekable();
    if labels.peek().is_none() && le.is_none() {
        return;
    }
    out.push('{');
    let mut first = true;
    for (key, value) in labels {
        if !first {
            out.push(',');
        }
        first = false;
        out.push_str(&sanitize(key));
        out.push_str("=\"");
        escape_label_value(out, value);
        out.push('"');
    }
    if let Some(le) = le {
        if !first {
            out.push(',');
        }
        let _ = write!(out, "le=\"{le}\"");
    }
    out.push('}');
}

fn escape_label_value(out: &mut String, value: &str) {
    for character in value.chars() {
        match character {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
}

fn write_number(out: &mut String, value: Number) {
    match value {
        Number::Int(value) => {
            let _ = write!(out, "{value}");
        }
        Number::Double(value) => out.push_str(&format_float(value)),
    }
}

fn format_float(value: f64) -> String {
    if value.is_nan() {
        return "NaN".into();
    }
    if value.is_infinite() {
        return if value > 0.0 { "+Inf" } else { "-Inf" }.into();
    }
    format!("{value}")
}

/// Replace characters outside `[a-zA-Z0-9_:]` with `_` and collapse repeats.
fn sanitize(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for character in name.chars() {
        let valid = character.is_ascii_alphanumeric() || character == '_' || character == ':';
        let character = if valid { character } else { '_' };
        if character == '_' && out.ends_with('_') {
            continue;
        }
        out.push(character);
    }
    out
}

fn prometheus_unit(descriptor: &Descriptor) -> &'static str {
    match descriptor.unit {
        "s" => "seconds",
        "ms" => "milliseconds",
        "us" => "microseconds",
        "ns" => "nanoseconds",
        "By" => "bytes",
        "1" if descriptor.kind == Kind::Gauge => "ratio",
        _ => "",
    }
}

pub fn prometheus_name(descriptor: &Descriptor) -> String {
    let mut name = sanitize(descriptor.name);
    let unit = prometheus_unit(descriptor);
    if !unit.is_empty() && !name.ends_with(&format!("_{unit}")) {
        name.push('_');
        name.push_str(unit);
    }
    if descriptor.kind == Kind::Counter && !name.ends_with("_total") {
        name.push_str("_total");
    }
    name
}

#[cfg(test)]
mod tests {
    use super::*;

    descriptor!(
        TEST_COUNTER,
        "celld.test.refusals",
        Counter,
        "{refusal}",
        "Test counter."
    );
    descriptor!(
        TEST_GAUGE,
        "celld.test.utilization",
        Gauge,
        "1",
        "Test gauge."
    );
    descriptor!(
        TEST_BYTES,
        "celld.test.usage",
        UpDownCounter,
        "By",
        "Test bytes."
    );
    descriptor!(
        TEST_HISTOGRAM,
        "celld.test.duration",
        Histogram,
        "s",
        "Test histogram."
    );

    #[test]
    fn prometheus_names_follow_the_otlp_translation() {
        assert_eq!(prometheus_name(&TEST_COUNTER), "celld_test_refusals_total");
        assert_eq!(prometheus_name(&TEST_GAUGE), "celld_test_utilization_ratio");
        assert_eq!(prometheus_name(&TEST_BYTES), "celld_test_usage_bytes");
        assert_eq!(
            prometheus_name(&TEST_HISTOGRAM),
            "celld_test_duration_seconds"
        );
        assert_eq!(
            prometheus_name(&PROCESS_CPU_TIME),
            "process_cpu_time_seconds_total"
        );
        assert_eq!(
            prometheus_name(&FAAS_INVOKE_DURATION),
            "faas_invoke_duration_seconds"
        );
        assert_eq!(sanitize("a..b-c"), "a_b_c");
    }

    #[test]
    fn histogram_buckets_are_inclusive_upper_bounds_and_cumulative_in_text() {
        let histogram = Histogram::new(&TEST_HISTOGRAM, &[0.1, 1.0]);
        let series = histogram.series(&[("celld.script", "api")]);
        series.record(0.1);
        series.record(0.5);
        series.record(3.0);
        series.record(f64::NAN);
        let metric = histogram.collect().unwrap();
        let Data::Histogram { points, .. } = &metric.data else {
            panic!("histogram data")
        };
        assert_eq!(points[0].bucket_counts, vec![1, 1, 1]);
        assert_eq!(points[0].count, 3);
        assert!((points[0].sum - 3.6).abs() < 1e-9);
        let text = prometheus_text(&[metric], None);
        assert!(text.contains("# TYPE celld_test_duration_seconds histogram\n"));
        assert!(text
            .contains("celld_test_duration_seconds_bucket{celld_script=\"api\",le=\"0.1\"} 1\n"));
        assert!(
            text.contains("celld_test_duration_seconds_bucket{celld_script=\"api\",le=\"1\"} 2\n")
        );
        assert!(text
            .contains("celld_test_duration_seconds_bucket{celld_script=\"api\",le=\"+Inf\"} 3\n"));
        assert!(text.contains("celld_test_duration_seconds_count{celld_script=\"api\"} 3\n"));
    }

    #[test]
    fn counters_accumulate_per_attribute_set_and_escape_label_values() {
        let counter = Counter::new(&TEST_COUNTER);
        counter.add(&[("error.type", "timeout"), ("celld.script", "a\"b")], 2);
        counter.add(&[("celld.script", "a\"b"), ("error.type", "timeout")], 3);
        counter.add(&[], 1);
        let metric = counter.collect().unwrap();
        let text = prometheus_text(&[metric], None);
        assert!(text.contains("# TYPE celld_test_refusals_total counter\n"));
        assert!(text.contains("celld_test_refusals_total 1\n"));
        assert!(text.contains(
            "celld_test_refusals_total{celld_script=\"a\\\"b\",error_type=\"timeout\"} 5\n"
        ));
    }

    #[test]
    fn unrecorded_instruments_are_absent_not_zero() {
        assert!(Counter::new(&TEST_COUNTER).collect().is_none());
        assert!(Histogram::new(&TEST_HISTOGRAM, DURATION_BUCKETS)
            .collect()
            .is_none());
    }

    #[test]
    fn observations_of_one_metric_merge_and_the_resource_becomes_target_info() {
        let merged = merge(vec![
            Metric::numbers(
                &TEST_BYTES,
                vec![(attributes(&[("k", "a")]), Number::Int(1))],
            ),
            Metric::single(&TEST_GAUGE, Number::Double(0.5)),
            Metric::numbers(
                &TEST_BYTES,
                vec![(attributes(&[("k", "b")]), Number::Int(2))],
            ),
        ]);
        assert_eq!(merged.len(), 2);
        let resource = Resource {
            node: "node-a".into(),
            region: "local".into(),
            service: "celld".into(),
        };
        let text = prometheus_text(&merged, Some(&resource));
        assert_eq!(
            text.matches("# TYPE celld_test_usage_bytes gauge").count(),
            1
        );
        assert!(text.contains("celld_test_usage_bytes{k=\"a\"} 1\n"));
        assert!(text.contains("celld_test_usage_bytes{k=\"b\"} 2\n"));
        assert!(text.contains("celld_test_utilization_ratio 0.5\n"));
        assert!(text.contains("service_instance_id=\"node-a\""));
    }
}
