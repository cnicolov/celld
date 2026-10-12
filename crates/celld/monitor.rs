// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Observational resource accounting. No placement, ownership or admission
//! decision reads these values. Disabled nodes allocate no per-cell counters.
#![allow(clippy::disallowed_methods)]

use celld_monitor::{BufferMemory, IsolateMemory, SqliteMemory, Turns};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Instant;

pub fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        crate::env_vars::flag("CELLD_MONITOR", false).expect("validated CELLD_MONITOR")
    })
}

pub struct Asset {
    pub name: &'static str,
    pub content_type: &'static str,
    pub bytes: &'static [u8],
}

include!(concat!(env!("OUT_DIR"), "/monitor_assets.rs"));

pub fn asset(name: &str) -> Option<&'static Asset> {
    ASSETS.iter().find(|asset| asset.name == name)
}

pub(crate) fn unix_ms() -> u64 {
    crate::telemetry::now_unix_us().max(0) as u64 / 1_000
}

pub(crate) fn process_id() -> &'static str {
    static BOOT_ID: OnceLock<String> = OnceLock::new();
    BOOT_ID.get_or_init(|| {
        // A PID is not a boot identity: containers commonly restart as PID 1.
        // This nonce is observational only, never an input to core policy.
        let mut nonce = [0u8; 16];
        getrandom::fill(&mut nonce).expect("monitor boot identity entropy");
        format!("{:032x}", u128::from_le_bytes(nonce))
    })
}

static NEXT_RESIDENCY: AtomicU64 = AtomicU64::new(1);

/// Cell-owned buffers can survive hibernation. Their identity follows the
/// resource, not a scheduled turn or the current isolate/residency.
#[derive(Default)]
pub(crate) struct BufferRegistry {
    cells: Mutex<BTreeMap<String, Weak<BufferCounters>>>,
}

impl BufferRegistry {
    pub fn owner(self: &Arc<Self>, cell: &str) -> Arc<BufferCounters> {
        let mut cells = self.cells.lock().unwrap();
        if let Some(owner) = cells.get(cell).and_then(Weak::upgrade) {
            return owner;
        }
        let owner = Arc::new(BufferCounters {
            cell: cell.into(),
            registry: Arc::downgrade(self),
            http: AtomicU64::new(0),
            websocket: AtomicU64::new(0),
        });
        cells.insert(cell.into(), Arc::downgrade(&owner));
        owner
    }
}

pub(crate) fn buffer_owner(cell: &str) -> Option<Arc<BufferCounters>> {
    (enabled() && !cell.is_empty())
        .then(|| crate::asyncrt::services().monitor_buffers().owner(cell))
}

pub(crate) struct BufferCounters {
    cell: String,
    registry: Weak<BufferRegistry>,
    http: AtomicU64,
    websocket: AtomicU64,
}

impl BufferCounters {
    pub fn snapshot(&self) -> BufferMemory {
        BufferMemory {
            http_stream_bytes: self.http.load(Ordering::Relaxed),
            websocket_bytes: self.websocket.load(Ordering::Relaxed),
        }
    }

    fn counter(&self, kind: BufferKind) -> &AtomicU64 {
        match kind {
            BufferKind::Http => &self.http,
            BufferKind::WebSocket => &self.websocket,
        }
    }
}

impl Drop for BufferCounters {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            let mut cells = registry.cells.lock().unwrap();
            // A last-owner drop can race a replacement's registration.
            if cells
                .get(&self.cell)
                .is_some_and(|entry| std::ptr::eq(entry.as_ptr(), self))
            {
                cells.remove(&self.cell);
            }
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum BufferKind {
    Http,
    WebSocket,
}

pub(crate) struct BufferCharge {
    owner: Arc<BufferCounters>,
    kind: BufferKind,
    bytes: u64,
}

impl BufferCharge {
    pub fn new(
        owner: Option<&Arc<BufferCounters>>,
        kind: BufferKind,
        bytes: usize,
    ) -> Option<Self> {
        let owner = owner?.clone();
        owner
            .counter(kind)
            .fetch_add(bytes as u64, Ordering::Relaxed);
        Some(Self {
            owner,
            kind,
            bytes: bytes as u64,
        })
    }
}

impl Drop for BufferCharge {
    fn drop(&mut self) {
        self.owner
            .counter(self.kind)
            .fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

/// Payload and charge move together. Failed sends, dropped queues and abandoned
/// gate reservations release exactly once without special cleanup bookkeeping.
pub struct Buffered<T> {
    pub(crate) value: T,
    charge: Option<BufferCharge>,
}

impl<T> Buffered<T> {
    pub(crate) fn new(
        value: T,
        owner: Option<&Arc<BufferCounters>>,
        kind: BufferKind,
        bytes: usize,
    ) -> Self {
        Self {
            value,
            charge: BufferCharge::new(owner, kind, bytes),
        }
    }

    pub(crate) fn untracked(value: T) -> Self {
        Self {
            value,
            charge: None,
        }
    }

    pub(crate) fn track_if_untracked(
        mut self,
        owner: Option<&Arc<BufferCounters>>,
        kind: BufferKind,
        bytes: usize,
    ) -> Self {
        if self.charge.is_none() {
            self.charge = BufferCharge::new(owner, kind, bytes);
        }
        self
    }

    pub fn into_inner(self) -> T {
        let Self { value, charge } = self;
        drop(charge);
        value
    }
}

pub struct Query {
    pub search: String,
    pub class: Option<String>,
    pub offset: usize,
    pub limit: usize,
}

impl Default for Query {
    fn default() -> Self {
        Self {
            search: String::new(),
            class: None,
            offset: 0,
            limit: celld_monitor::PAGE_SIZE,
        }
    }
}

impl Query {
    pub fn parse(query: Option<&str>) -> anyhow::Result<Self> {
        let mut parsed = Self {
            limit: celld_monitor::PAGE_SIZE,
            ..Self::default()
        };
        for (name, value) in url::form_urlencoded::parse(query.unwrap_or("").as_bytes()) {
            match name.as_ref() {
                "q" if value.len() <= 256 => parsed.search = value.to_lowercase(),
                "class" if value.len() <= 256 => {
                    parsed.class = (!value.is_empty()).then(|| value.into_owned())
                }
                "offset" => {
                    parsed.offset = value
                        .parse()
                        .map_err(|_| anyhow::anyhow!("offset must be an unsigned integer"))?
                }
                "limit" => {
                    parsed.limit = value
                        .parse()
                        .map_err(|_| anyhow::anyhow!("limit must be an unsigned integer"))?;
                    anyhow::ensure!(
                        (1..=celld_monitor::MAX_PAGE_SIZE).contains(&parsed.limit),
                        "limit must be between 1 and {}",
                        celld_monitor::MAX_PAGE_SIZE
                    );
                }
                _ => anyhow::bail!("unknown or oversized monitoring query parameter: {name}"),
            }
        }
        Ok(parsed)
    }
}

pub(crate) struct Counters {
    pub residency: u64,
    buffers: Option<Arc<BufferCounters>>,
    cpu_nanos: AtomicU64,
    cpu_unavailable: AtomicBool,
    completed: AtomicU64,
    queued: AtomicUsize,
    running: AtomicUsize,
    queue_wait_nanos: AtomicU64,
}

impl Default for Counters {
    fn default() -> Self {
        Self {
            residency: NEXT_RESIDENCY.fetch_add(1, Ordering::Relaxed),
            buffers: None,
            cpu_nanos: AtomicU64::new(0),
            cpu_unavailable: AtomicBool::new(crate::js::thread_cpu_nanos().is_none()),
            completed: AtomicU64::new(0),
            queued: AtomicUsize::new(0),
            running: AtomicUsize::new(0),
            queue_wait_nanos: AtomicU64::new(0),
        }
    }
}

impl Counters {
    pub fn buffer_memory(&self) -> Option<BufferMemory> {
        self.buffers.as_ref().map(|buffers| buffers.snapshot())
    }

    pub fn snapshot(&self) -> Turns {
        Turns {
            cpu_nanos: (!self.cpu_unavailable.load(Ordering::Relaxed))
                .then(|| self.cpu_nanos.load(Ordering::Relaxed)),
            completed: self.completed.load(Ordering::Relaxed),
            queued: self.queued.load(Ordering::Relaxed),
            running: self.running.load(Ordering::Relaxed),
            queue_wait_nanos: self.queue_wait_nanos.load(Ordering::Relaxed),
        }
    }

    fn finish(&self, cpu_nanos: Option<u64>) {
        if let Some(cpu_nanos) = cpu_nanos {
            self.cpu_nanos.fetch_add(cpu_nanos, Ordering::Relaxed);
        } else {
            // Wall time is not CPU. Never silently fall back to it.
            self.cpu_unavailable.store(true, Ordering::Relaxed);
        }
        self.completed.fetch_add(1, Ordering::Relaxed);
    }
}

pub(crate) struct TurnAccounting {
    isolate: Arc<Counters>,
    cell: Option<Arc<Counters>>,
    waiting: Instant,
    started: bool,
    cpu_started: Option<u64>,
}

impl TurnAccounting {
    pub fn queued(isolate: Arc<Counters>, cell: Option<Arc<Counters>>) -> Self {
        for counters in std::iter::once(&isolate).chain(cell.iter()) {
            counters.queued.fetch_add(1, Ordering::Relaxed);
        }
        Self {
            isolate,
            cell,
            waiting: Instant::now(),
            started: false,
            cpu_started: None,
        }
    }

    pub fn start(&mut self) {
        let wait = self.waiting.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
        for counters in std::iter::once(&self.isolate).chain(self.cell.iter()) {
            counters.queued.fetch_sub(1, Ordering::Relaxed);
            counters.running.fetch_add(1, Ordering::Relaxed);
            counters.queue_wait_nanos.fetch_add(wait, Ordering::Relaxed);
        }
        self.started = true;
        self.cpu_started = crate::js::thread_cpu_nanos();
    }
}

impl Drop for TurnAccounting {
    fn drop(&mut self) {
        let cpu = self
            .cpu_started
            .zip(crate::js::thread_cpu_nanos())
            .and_then(|(start, end)| end.checked_sub(start));
        for counters in std::iter::once(&self.isolate).chain(self.cell.iter()) {
            if self.started {
                counters.finish(cpu);
                counters.running.fetch_sub(1, Ordering::Relaxed);
            } else {
                // Cancelled while waiting: no execution or CPU was consumed.
                counters.queued.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct MemorySample {
    pub heap: IsolateMemory,
    pub sqlite: BTreeMap<String, SqliteMemory>,
}

pub(crate) struct SlotResources {
    pub busy: bool,
    pub turns: Turns,
    pub resident_cells: usize,
    pub requests: usize,
    pub retiring: bool,
    pub memory: Option<MemorySample>,
}

#[derive(Default)]
pub(crate) struct SlotMonitor {
    pub turns: Arc<Counters>,
    cells: Mutex<BTreeMap<String, Arc<Counters>>>,
    pub memory: Mutex<Option<MemorySample>>,
}

impl SlotMonitor {
    pub fn register(&self, cell: &str) -> Arc<Counters> {
        let counters = Arc::new(Counters {
            buffers: buffer_owner(cell),
            ..Counters::default()
        });
        self.cells
            .lock()
            .unwrap()
            .insert(cell.to_string(), counters.clone());
        if let Some(memory) = self.memory.lock().unwrap().as_mut() {
            memory.sqlite.remove(cell);
        }
        counters
    }

    pub fn cell(&self, cell: &str) -> Option<Arc<Counters>> {
        self.cells.lock().unwrap().get(cell).cloned()
    }

    pub fn unregister(&self, cell: &str, counters: &Arc<Counters>) {
        let mut cells = self.cells.lock().unwrap();
        if cells
            .get(cell)
            .is_some_and(|current| Arc::ptr_eq(current, counters))
        {
            cells.remove(cell);
            if let Some(memory) = self.memory.lock().unwrap().as_mut() {
                memory.sqlite.remove(cell);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buffer_charges_follow_resources_not_turns_or_residencies() {
        let registry = Arc::new(BufferRegistry::default());
        let a = registry.owner("Counter:a");
        let b = registry.owner("Counter:b");
        let payload = Buffered::new(vec![0; 123], Some(&a), BufferKind::Http, 123);
        let frame = Buffered::new(vec![0; 456], Some(&a), BufferKind::WebSocket, 456);
        assert_eq!(a.snapshot().total_bytes(), 579);
        assert_eq!(b.snapshot().total_bytes(), 0);
        drop(a);
        // A resource can keep memory alive while its cell hibernates.
        let resumed = registry.owner("Counter:a");
        assert_eq!(resumed.snapshot().total_bytes(), 579);
        assert_eq!(payload.into_inner().len(), 123);
        assert_eq!(resumed.snapshot().http_stream_bytes, 0);
        drop(frame);
        assert_eq!(resumed.snapshot().total_bytes(), 0);
        drop(resumed);
        drop(b);
        assert!(registry.cells.lock().unwrap().is_empty());
    }

    #[test]
    fn buffer_accounting_is_domain_local_and_optional() {
        let first = Arc::new(BufferRegistry::default());
        let second = Arc::new(BufferRegistry::default());
        let a = first.owner("Counter:a");
        let b = second.owner("Counter:a");
        let frame = Buffered::new((), Some(&a), BufferKind::WebSocket, 100);
        assert_eq!(b.snapshot().total_bytes(), 0);
        let untracked = Buffered::new((), None, BufferKind::Http, 1_000);
        assert!(untracked.charge.is_none());
        drop(frame);
        assert_eq!(a.snapshot().total_bytes(), 0);
    }

    #[test]
    fn monitor_identity_is_a_stable_boot_nonce_not_a_reused_pid() {
        let boot = process_id();
        assert_eq!(boot.len(), 32);
        assert!(boot.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_eq!(boot, process_id());
        assert_ne!(boot, std::process::id().to_string());
    }

    #[test]
    fn queries_are_bounded_and_decode_search_terms() {
        let query = Query::parse(Some("q=Counter%3Aa&class=Counter&limit=25&offset=10")).unwrap();
        assert_eq!(query.search, "counter:a");
        assert_eq!(query.class.as_deref(), Some("Counter"));
        assert_eq!(query.offset, 10);
        assert_eq!(query.limit, 25);
        for invalid in [
            "limit=0",
            "limit=251",
            "limit=abc",
            "offset=-1",
            "unknown=1",
        ] {
            assert!(Query::parse(Some(invalid)).is_err(), "{invalid}");
        }
        assert_eq!(Query::parse(None).unwrap().limit, celld_monitor::PAGE_SIZE);
    }

    #[test]
    fn cancelling_a_waiting_turn_does_not_charge_execution() {
        let counters = Arc::new(Counters::default());
        let queued = TurnAccounting::queued(counters.clone(), None);
        assert_eq!(counters.snapshot().queued, 1);
        drop(queued);
        let sample = counters.snapshot();
        assert_eq!(sample.queued, 0);
        assert_eq!(sample.running, 0);
        assert_eq!(sample.completed, 0);
    }

    #[test]
    fn execution_is_recorded_without_a_cpu_limit_or_trace() {
        let counters = Arc::new(Counters::default());
        let cell = Arc::new(Counters::default());
        let mut turn = TurnAccounting::queued(counters.clone(), Some(cell.clone()));
        turn.start();
        assert_eq!(cell.snapshot().running, 1);
        drop(turn);
        assert_eq!(counters.snapshot().completed, 1);
        assert_eq!(cell.snapshot().completed, 1);
        assert_eq!(cell.snapshot().running, 0);
        assert_eq!(cell.snapshot().cpu_nanos, counters.snapshot().cpu_nanos);
    }

    #[test]
    fn missing_cpu_is_not_reported_as_zero_or_wall_time() {
        let counters = Counters::default();
        counters.finish(None);
        assert_eq!(counters.snapshot().cpu_nanos, None);
    }

    #[test]
    fn old_residency_cannot_delete_replacement_counters() {
        let monitor = SlotMonitor::default();
        let old = monitor.register("Counter:a");
        let next = monitor.register("Counter:a");
        assert_ne!(old.residency, next.residency);
        monitor.unregister("Counter:a", &old);
        assert_eq!(monitor.cell("Counter:a").unwrap().residency, next.residency);
        monitor.unregister("Counter:a", &next);
        assert!(monitor.cell("Counter:a").is_none());
    }

    #[test]
    fn sharing_a_heap_does_not_split_turn_cpu_between_cells() {
        let monitor = SlotMonitor::default();
        let a = monitor.register("Counter:a");
        let b = monitor.register("Counter:b");
        let before = b.snapshot();
        let mut turn = TurnAccounting::queued(monitor.turns.clone(), Some(a.clone()));
        turn.start();
        drop(turn);
        assert_eq!(a.snapshot().completed, 1);
        assert_eq!(b.snapshot(), before);
        assert_eq!(monitor.turns.snapshot().cpu_nanos, a.snapshot().cpu_nanos);
    }

    #[test]
    fn unregistering_a_cell_removes_only_its_cached_sqlite_memory() {
        let monitor = SlotMonitor::default();
        let a = monitor.register("Counter:a");
        let _b = monitor.register("Counter:b");
        *monitor.memory.lock().unwrap() = Some(MemorySample {
            heap: IsolateMemory {
                sampled_unix_ms: 123,
                heap_used_bytes: 100,
                heap_physical_bytes: 200,
                external_bytes: 300,
            },
            sqlite: ["Counter:a", "Counter:b"]
                .into_iter()
                .map(|cell| {
                    (
                        cell.into(),
                        SqliteMemory {
                            cache_bytes: 1,
                            schema_bytes: 2,
                            statement_bytes: 3,
                        },
                    )
                })
                .collect(),
        });
        monitor.unregister("Counter:a", &a);
        let memory = monitor.memory.lock().unwrap();
        let memory = memory.as_ref().unwrap();
        assert!(!memory.sqlite.contains_key("Counter:a"));
        assert!(memory.sqlite.contains_key("Counter:b"));
        assert_eq!(memory.heap.heap_used_bytes, 100);
    }
}
