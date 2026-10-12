// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The read-only monitoring wire contract and browser-side rate calculations.
//! Shared by the native runtime and the Rust/WebAssembly dashboard.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};

pub const SCHEMA_VERSION: u32 = 2;
pub const PAGE_SIZE: usize = 100;
pub const MAX_PAGE_SIZE: usize = 250;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterSnapshot {
    pub schema_version: u32,
    pub coordinator: String,
    pub sampled_unix_ms: u64,
    pub discovery_age_ms: Option<u64>,
    pub discovery_error: Option<String>,
    pub known_nodes: usize,
    pub partial: bool,
    pub total_matches: usize,
    pub collected_matches: usize,
    pub offset: usize,
    pub nodes: Vec<ClusterNode>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterNode {
    pub id: String,
    pub address: String,
    pub error: Option<String>,
    pub truncated: bool,
    pub snapshot: Option<Snapshot>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TraceSpan {
    pub node: String,
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub name: String,
    #[serde(default)]
    pub kind: Option<i64>,
    pub start_unix_us: i64,
    pub duration_us: i64,
    pub ok: bool,
    pub error: Option<String>,
    pub cell: Option<String>,
    pub url: Option<String>,
    pub http_status: Option<i64>,
    pub queue_wait_us: Option<i64>,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TraceLog {
    pub node: String,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub time_unix_us: i64,
    pub severity_text: String,
    pub body: String,
    #[serde(default)]
    pub severity_number: Option<i64>,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TracePage {
    pub from_unix_us: i64,
    pub to_unix_us: i64,
    pub spans: Vec<TraceSpan>,
    pub logs: Vec<TraceLog>,
    pub scanned_files: usize,
    pub matched_spans: usize,
    pub truncated: bool,
    pub warnings: Vec<String>,
}

/// A shareable trace drill-down with an explicit historical window. Form
/// submissions use a fresh relative window instead of reusing this one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceLink {
    pub trace_id: String,
    pub from_unix_ms: i64,
    pub to_unix_ms: i64,
}

impl TraceLink {
    pub fn parse(fragment: &str) -> Option<Self> {
        if fragment.len() > 256 {
            return None;
        }
        let mut trace_id = None;
        let mut from = None;
        let mut to = None;
        for field in fragment.strip_prefix('#')?.split('&') {
            let (key, value) = field.split_once('=')?;
            match key {
                "trace" if trace_id.is_none() => trace_id = Some(value),
                "from" if from.is_none() => from = Some(value.parse::<i64>().ok()?),
                "to" if to.is_none() => to = Some(value.parse::<i64>().ok()?),
                _ => return None,
            }
        }
        let trace_id = trace_id?;
        let from_unix_ms = from?;
        let to_unix_ms = to?;
        if trace_id.len() != 32
            || !trace_id.bytes().all(|c| c.is_ascii_hexdigit())
            || from_unix_ms < 0
            || !(1..=86_400_000).contains(&to_unix_ms.checked_sub(from_unix_ms)?)
        {
            return None;
        }
        Some(Self {
            trace_id: trace_id.to_ascii_lowercase(),
            from_unix_ms,
            to_unix_ms,
        })
    }

    pub fn fragment(&self) -> String {
        format!(
            "#trace={}&from={}&to={}",
            self.trace_id, self.from_unix_ms, self.to_unix_ms
        )
    }
}

#[cfg(test)]
mod trace_link_tests {
    use super::*;

    #[test]
    fn trace_links_preserve_a_bounded_historical_window() {
        let link = TraceLink {
            trace_id: "ab".repeat(16),
            from_unix_ms: 1000,
            to_unix_ms: 2000,
        };
        assert_eq!(TraceLink::parse(&link.fragment()), Some(link));
        for invalid in [
            "#traces",
            "#trace=oops&from=1000&to=2000",
            "#trace=ab&from=-1&to=2000",
        ] {
            assert!(TraceLink::parse(invalid).is_none());
        }
        for (from, to) in [(0, 86_400_001), (1, 0), (1, 1), (-1, i64::MAX)] {
            assert!(
                TraceLink::parse(&format!("#trace={}&from={from}&to={to}", "ab".repeat(16)))
                    .is_none()
            );
        }
        assert!(
            TraceLink::parse(&format!("#trace={}&from=1&to=2&from=1", "ab".repeat(16))).is_none()
        );
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub schema_version: u32,
    pub process: String,
    pub sampled_unix_ms: u64,
    pub monotonic_ms: u64,
    pub node: Node,
    pub classes: Vec<String>,
    pub total_matches: usize,
    pub offset: usize,
    pub cells: Vec<Cell>,
    pub isolates: Vec<Isolate>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Node {
    pub id: String,
    pub deployment: String,
    pub cores: usize,
    /// Process CPU expressed as a percentage of one core, not node capacity.
    pub cpu_percent_of_one_core: Option<f64>,
    pub rss_bytes: Option<u64>,
    pub resident_cells: usize,
    pub pressured: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Cell {
    pub id: String,
    pub class: String,
    pub epoch: u64,
    pub generation: u64,
    pub residency: u64,
    pub isolate: String,
    pub script: String,
    pub starting: bool,
    /// Fetch/RPC admission only; not an event count or alarm count.
    pub in_flight_requests: usize,
    pub turns: Turns,
    pub sqlite: Option<SqliteMemory>,
    /// Live native payload capacities, not V8/transport buffers or queue overhead.
    #[serde(default)]
    pub buffers: Option<BufferMemory>,
    pub memory_sampled_unix_ms: Option<u64>,
    pub memory_stale: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BufferMemory {
    pub http_stream_bytes: u64,
    pub websocket_bytes: u64,
}

impl BufferMemory {
    pub fn total_bytes(&self) -> u64 {
        self.http_stream_bytes.saturating_add(self.websocket_bytes)
    }
}

impl Cell {
    pub fn tracked_bytes(&self) -> Option<u64> {
        Some(
            self.sqlite
                .as_ref()?
                .total_bytes()
                .saturating_add(self.buffers.as_ref()?.total_bytes()),
        )
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Turns {
    /// CPU inside completed scheduled turns, including synchronous native work
    /// and foreground GC. Foreign microtasks can run within the same turn.
    /// This is deliberately NOT advertised as exact per-event execution CPU.
    pub cpu_nanos: Option<u64>,
    pub completed: u64,
    pub queued: usize,
    pub running: usize,
    pub queue_wait_nanos: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteMemory {
    pub cache_bytes: u64,
    pub schema_bytes: u64,
    pub statement_bytes: u64,
}

impl SqliteMemory {
    pub fn total_bytes(&self) -> u64 {
        self.cache_bytes
            .saturating_add(self.schema_bytes)
            .saturating_add(self.statement_bytes)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Isolate {
    pub id: String,
    pub script: String,
    pub generation: u64,
    pub kind: String,
    pub resident_cells: usize,
    pub cells: Vec<String>,
    pub requests: usize,
    pub retiring: bool,
    pub busy: bool,
    pub turns: Turns,
    pub memory: Option<IsolateMemory>,
    pub memory_stale: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IsolateMemory {
    pub sampled_unix_ms: u64,
    pub heap_used_bytes: u64,
    pub heap_physical_bytes: u64,
    pub external_bytes: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Point {
    pub at_ms: u64,
    pub cpu_percent: Option<f64>,
    pub native_bytes: Option<u64>,
}

#[derive(Clone, Default)]
pub struct History {
    process: String,
    series: BTreeMap<(String, u64), VecDeque<Point>>,
    previous: BTreeMap<(String, u64), (u64, Option<u64>)>,
}

impl History {
    /// Bounded session history. It is not persistent Parquet history.
    pub fn observe(&mut self, snapshot: &Snapshot) {
        if snapshot.process != self.process {
            self.process.clone_from(&snapshot.process);
            self.series.clear();
            self.previous.clear();
        }
        for cell in &snapshot.cells {
            let key = (cell.id.clone(), cell.residency);
            let previous = self
                .previous
                .insert(key.clone(), (snapshot.monotonic_ms, cell.turns.cpu_nanos));
            let cpu_percent = previous.and_then(|(at, cpu)| {
                // A resumed tab/revisited page is not a fresh two-second rate.
                (snapshot.monotonic_ms.saturating_sub(at) <= 5_000)
                    .then(|| cpu_percent(cpu?, cell.turns.cpu_nanos?, at, snapshot.monotonic_ms))
                    .flatten()
            });
            let series = self.series.entry(key).or_default();
            series.push_back(Point {
                at_ms: snapshot.monotonic_ms,
                cpu_percent,
                native_bytes: (!cell.memory_stale).then(|| cell.tracked_bytes()).flatten(),
            });
            while series.len() > 300 {
                series.pop_front();
            }
            let cutoff = snapshot.monotonic_ms.saturating_sub(600_000);
            while series.front().is_some_and(|point| point.at_ms < cutoff) {
                series.pop_front();
            }
        }
        let cutoff = snapshot.monotonic_ms.saturating_sub(600_000);
        self.series
            .retain(|_, series| series.back().is_some_and(|point| point.at_ms >= cutoff));
        self.previous.retain(|key, _| self.series.contains_key(key));
        // Paging through or churning cells cannot retain an unbounded registry.
        while self.series.len() > MAX_PAGE_SIZE * 4 {
            let oldest = self
                .series
                .iter()
                .min_by_key(|(_, series)| series.back().map_or(0, |point| point.at_ms))
                .map(|(key, _)| key.clone());
            if let Some(key) = oldest {
                self.series.remove(&key);
                self.previous.remove(&key);
            }
        }
    }

    pub fn points(&self, cell: &Cell) -> Vec<Point> {
        self.series
            .get(&(cell.id.clone(), cell.residency))
            .map(|points| points.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn cpu_percent(&self, cell: &Cell) -> Option<f64> {
        self.series
            .get(&(cell.id.clone(), cell.residency))?
            .back()?
            .cpu_percent
    }
}

pub fn cpu_percent(previous: u64, current: u64, previous_ms: u64, current_ms: u64) -> Option<f64> {
    let elapsed_ms = current_ms
        .checked_sub(previous_ms)
        .filter(|elapsed| *elapsed > 0)?;
    let used = current.checked_sub(previous)?;
    Some(used as f64 / (elapsed_ms as f64 * 1_000_000.0) * 100.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_is_percent_of_one_core_and_rejects_resets() {
        assert_eq!(cpu_percent(5, 250_000_005, 100, 1_100), Some(25.0));
        assert_eq!(cpu_percent(100, 99, 100, 1_100), None);
        assert_eq!(cpu_percent(0, 1, 100, 100), None);
        assert_eq!(cpu_percent(0, 1, 100, 99), None);
    }

    #[test]
    fn sqlite_memory_is_only_the_measured_sqlite_categories() {
        let memory = SqliteMemory {
            cache_bytes: 10,
            schema_bytes: 20,
            statement_bytes: 30,
        };
        assert_eq!(memory.total_bytes(), 60);
        let json = serde_json::to_value(memory).unwrap();
        assert!(json.get("heap_used_bytes").is_none());
        assert!(json.get("buffer_bytes").is_none());
    }

    fn snapshot(process: &str, at: u64, residency: u64, cpu: Option<u64>) -> Snapshot {
        Snapshot {
            schema_version: SCHEMA_VERSION,
            process: process.into(),
            sampled_unix_ms: at,
            monotonic_ms: at,
            node: Node {
                id: "node".into(),
                deployment: "v1".into(),
                cores: 4,
                cpu_percent_of_one_core: None,
                rss_bytes: None,
                resident_cells: 1,
                pressured: false,
            },
            classes: vec!["Counter".into()],
            total_matches: 1,
            offset: 0,
            isolates: vec![],
            cells: vec![Cell {
                id: "Counter:a".into(),
                class: "Counter".into(),
                epoch: 1,
                generation: 1,
                residency,
                isolate: "42".into(),
                script: "counter".into(),
                starting: false,
                in_flight_requests: 0,
                turns: Turns {
                    cpu_nanos: cpu,
                    ..Default::default()
                },
                sqlite: None,
                buffers: None,
                memory_sampled_unix_ms: None,
                memory_stale: true,
            }],
        }
    }

    #[test]
    fn history_does_not_join_residencies_or_processes() {
        let mut history = History::default();
        history.observe(&snapshot("one", 1_000, 1, Some(0)));
        let next = snapshot("one", 2_000, 1, Some(100_000_000));
        history.observe(&next);
        assert_eq!(history.cpu_percent(&next.cells[0]), Some(10.0));
        let replaced = snapshot("one", 3_000, 2, Some(10));
        history.observe(&replaced);
        assert_eq!(history.cpu_percent(&replaced.cells[0]), None);
        let restarted = snapshot("two", 4_000, 2, Some(100_000_010));
        history.observe(&restarted);
        assert_eq!(history.cpu_percent(&restarted.cells[0]), None);
        assert!(history.points(&next.cells[0]).is_empty());
    }

    #[test]
    fn tracked_native_total_includes_owned_buffers_without_inventing_missing_data() {
        let mut sample = snapshot("one", 1_000, 1, None);
        let cell = &mut sample.cells[0];
        cell.sqlite = Some(SqliteMemory {
            cache_bytes: 10,
            schema_bytes: 20,
            statement_bytes: 30,
        });
        assert_eq!(cell.tracked_bytes(), None);
        cell.buffers = Some(BufferMemory {
            http_stream_bytes: 40,
            websocket_bytes: 50,
        });
        assert_eq!(cell.tracked_bytes(), Some(150));
        cell.memory_stale = false;
        let mut history = History::default();
        history.observe(&sample);
        assert_eq!(history.points(&sample.cells[0])[0].native_bytes, Some(150));
        let json = serde_json::to_value(&sample.cells[0]).unwrap();
        assert!(json.get("heap_used_bytes").is_none());
        let mut old_contract = json;
        old_contract.as_object_mut().unwrap().remove("buffers");
        assert_eq!(
            serde_json::from_value::<Cell>(old_contract)
                .unwrap()
                .tracked_bytes(),
            None
        );
    }

    #[test]
    fn missing_cpu_and_stale_memory_stay_missing() {
        let mut history = History::default();
        history.observe(&snapshot("one", 1_000, 1, None));
        let mut next = snapshot("one", 2_000, 1, None);
        next.cells[0].sqlite = Some(SqliteMemory {
            cache_bytes: 1,
            schema_bytes: 2,
            statement_bytes: 3,
        });
        history.observe(&next);
        let point = history.points(&next.cells[0]).pop().unwrap();
        assert_eq!(point.cpu_percent, None);
        assert_eq!(point.native_bytes, None);
    }

    #[test]
    fn history_is_bounded_and_long_gaps_do_not_create_fresh_cpu_rates() {
        let mut history = History::default();
        for index in 0..400 {
            history.observe(&snapshot("one", index * 2_000, 1, Some(index)));
        }
        let last = snapshot("one", 798_000, 1, Some(399));
        assert_eq!(history.points(&last.cells[0]).len(), 300);
        let resumed = snapshot("one", 1_500_000, 1, Some(500));
        history.observe(&resumed);
        assert_eq!(history.points(&resumed.cells[0]).len(), 1);
        assert_eq!(history.cpu_percent(&resumed.cells[0]), None);
        for residency in 2..1_100 {
            history.observe(&snapshot("one", 1_500_000 + residency, residency, Some(0)));
        }
        assert!(history.series.len() <= MAX_PAGE_SIZE * 4);
        assert_eq!(history.series.len(), history.previous.len());
    }
}
