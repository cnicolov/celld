// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Bounded, read-only queries of the existing fleet Parquet export. No SQL or
//! user-supplied storage URLs are accepted, and decoding stays off async workers.
use celld::bucket::Bucket;
use celld_monitor::{TraceLog, TracePage, TraceSpan};
use chrono::{DateTime, Utc};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{Field, Row};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, OnceLock};

const MAX_FILES: usize = 64;
const MAX_BYTES: u64 = 64 * 1024 * 1024;
const MAX_ROWS: usize = 200_000;
const MAX_RESULTS: usize = 500;
const MAX_LISTS: usize = 256;

fn export_time(key: &str) -> Option<i64> {
    key.rsplit('/').next()?.split('-').next()?.parse().ok()
}

fn late_export(key: &str, query: &Query, flush_ms: u64) -> bool {
    export_time(key).is_some_and(|at| {
        at > query
            .to
            .saturating_add(flush_ms.min(86_400_000) as i64 * 1000)
    })
}

struct Service {
    bucket: Bucket,
    flush_ms: u64,
    admission: Arc<tokio::sync::Semaphore>,
}

static SERVICE: OnceLock<Service> = OnceLock::new();

pub(super) fn init(bucket: Option<Bucket>, flush_ms: u64) {
    if let Some(bucket) = bucket {
        let _ = SERVICE.set(Service {
            bucket,
            flush_ms,
            admission: Arc::new(tokio::sync::Semaphore::new(1)),
        });
    }
}

#[derive(Clone, Debug)]
pub(super) struct Query {
    from: i64,
    to: i64,
    trace: String,
    cell: String,
    node: String,
    search: String,
    errors: bool,
    min_us: i64,
    logs_only: bool,
}

impl Query {
    pub fn parse(raw: Option<&str>) -> anyhow::Result<Self> {
        let now = celld::ownership_store::now_ms() as i64 * 1000;
        let mut query = Self {
            from: now - 900_000_000,
            to: now,
            trace: String::new(),
            cell: String::new(),
            node: String::new(),
            search: String::new(),
            errors: false,
            min_us: 0,
            logs_only: false,
        };
        anyhow::ensure!(raw.unwrap_or("").len() <= 4096, "Query is too long");
        for (key, value) in url::form_urlencoded::parse(raw.unwrap_or("").as_bytes()) {
            anyhow::ensure!(value.len() <= 512, "Filter is too long");
            match key.as_ref() {
                "from" => {
                    query.from = value
                        .parse::<i64>()?
                        .checked_mul(1000)
                        .ok_or_else(|| anyhow::anyhow!("Invalid start time"))?
                }
                "to" => {
                    query.to = value
                        .parse::<i64>()?
                        .checked_mul(1000)
                        .ok_or_else(|| anyhow::anyhow!("Invalid end time"))?
                }
                "minutes" => query.from = now - value.parse::<i64>()?.clamp(1, 1440) * 60_000_000,
                "trace_id" => query.trace = value.into_owned(),
                "cell" => query.cell = value.into_owned(),
                "node" => query.node = value.into_owned(),
                "q" => query.search = value.to_lowercase(),
                "signal" => {
                    anyhow::ensure!(
                        matches!(value.as_ref(), "traces" | "logs"),
                        "signal must be traces or logs"
                    );
                    query.logs_only = value == "logs";
                }
                "errors" => {
                    anyhow::ensure!(matches!(value.as_ref(), "0" | "1"), "errors must be 0 or 1");
                    query.errors = value == "1";
                }
                "min_ms" => {
                    query.min_us = value
                        .parse::<i64>()?
                        .checked_mul(1000)
                        .ok_or_else(|| anyhow::anyhow!("Invalid duration"))?
                }
                _ => anyhow::bail!("Unknown trace query parameter: {key}"),
            }
        }
        anyhow::ensure!(
            query.from >= 0
                && query.from < query.to
                && query.to <= now + 60_000_000
                && query.to - query.from <= 86_400_000_000,
            "Choose a time range of at most 24 hours, ending no later than now"
        );
        anyhow::ensure!(query.min_us >= 0, "Minimum duration cannot be negative");
        anyhow::ensure!(
            query.trace.is_empty()
                || (query.trace.len() == 32 && query.trace.bytes().all(|c| c.is_ascii_hexdigit())),
            "Trace ID must be 32 hexadecimal characters"
        );
        query.trace.make_ascii_lowercase();
        Ok(query)
    }

    fn matches(&self, span: &TraceSpan) -> bool {
        span.start_unix_us >= self.from
            && span.start_unix_us <= self.to
            && (self.trace.is_empty() || span.trace_id == self.trace)
            && (self.node.is_empty() || span.node == self.node)
            && (self.cell.is_empty()
                || span
                    .cell
                    .as_ref()
                    .is_some_and(|cell| cell.contains(&self.cell)))
            && (!self.errors || !span.ok)
            && span.duration_us >= self.min_us
            && (self.search.is_empty()
                || [
                    &span.name,
                    span.url.as_deref().unwrap_or(""),
                    span.error.as_deref().unwrap_or(""),
                    &span.trace_id,
                    &span
                        .metadata
                        .as_ref()
                        .map_or_else(String::new, ToString::to_string),
                ]
                .iter()
                .any(|text| text.to_lowercase().contains(&self.search)))
    }
}

pub(super) async fn query(query: Query) -> anyhow::Result<TracePage> {
    let service = SERVICE.get().ok_or_else(|| anyhow::anyhow!("Bucket trace queries require CELLD_OTEL=1. OTLP collector data must be queried in its external backend."))?;
    let permit = Arc::new(
        service
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| anyhow::anyhow!("Another trace query is running; retry shortly"))?,
    );
    // A cancelled HTTP read cannot admit a second decoder while spawn_blocking
    // is still working. The decoder holds the same owned permit through finish.
    tokio::time::timeout(
        std::time::Duration::from_secs(15),
        service.query(query, permit),
    )
    .await?
}

impl Service {
    async fn query(
        &self,
        query: Query,
        permit: Arc<tokio::sync::OwnedSemaphorePermit>,
    ) -> anyhow::Result<TracePage> {
        let mut page = TracePage { from_unix_us: query.from, to_unix_us: query.to, spans: Vec::new(), logs: Vec::new(), scanned_files: 0, matched_spans: 0, truncated: false,
            warnings: vec![format!("Persisted export only: flush interval {}s plus upload delay. Sampling and dropped telemetry can leave gaps; late exports outside the scanned flush window can be missing.", self.flush_ms / 1000)] };
        let mut nodes = BTreeSet::new();
        let signals = if query.logs_only {
            vec!["logs"]
        } else if query.trace.is_empty() {
            vec!["traces"]
        } else {
            vec!["traces", "logs"]
        };
        let mut lists = 0;
        for signal in &signals {
            let inventory = self
                .bucket
                .common_prefixes_page(&format!("telemetry/{signal}/"), None, None, 64)
                .await?;
            lists += 1;
            page.truncated |= inventory.page_token.is_some();
            for prefix in inventory.prefixes {
                if let Some(node) = prefix.trim_end_matches('/').rsplit('/').next() {
                    // Never allow a query filter to become an arbitrary storage prefix.
                    if query.node.is_empty() || query.node == node {
                        nodes.insert(node.to_string());
                    }
                }
            }
        }
        let first_hour = query.from / 3_600_000_000;
        let last_hour = (query
            .to
            .saturating_add(self.flush_ms.min(86_400_000) as i64 * 1000)
            .saturating_add(60_000_000))
            / 3_600_000_000;
        let mut objects = BTreeMap::new();
        'hours: for hour in (first_hour..=last_hour).rev() {
            let date = DateTime::<Utc>::from_timestamp(hour * 3600, 0).expect("validated time");
            for node in &nodes {
                for signal in &signals {
                    if lists >= MAX_LISTS {
                        page.truncated = true;
                        break 'hours;
                    }
                    let prefix =
                        format!("telemetry/{signal}/{node}/{}", date.format("%Y/%m/%d/%H"));
                    let mut token = None;
                    loop {
                        if lists >= MAX_LISTS {
                            page.truncated = true;
                            break 'hours;
                        }
                        let listing = self.bucket.objects_page(&prefix, token, MAX_FILES).await?;
                        lists += 1;
                        for object in listing.objects {
                            let key = object.location.to_string();
                            // Names carry the flush timestamp, so narrow historic
                            // queries do not spend their file budget on another
                            // minute in the same hour. Late writes remain disclosed.
                            let flushed = export_time(&key);
                            let end = query
                                .to
                                .saturating_add(self.flush_ms.min(86_400_000) as i64 * 1000)
                                .saturating_add(60_000_000);
                            if key.ends_with(".parquet")
                                && flushed.is_none_or(|at| at >= query.from && at <= end)
                            {
                                objects.insert(key, object);
                            }
                        }
                        token = listing.page_token;
                        if token.is_none() {
                            break;
                        }
                    }
                }
            }
            if objects.len() >= MAX_FILES {
                page.truncated |= hour > first_hour || objects.len() > MAX_FILES;
                break;
            }
        }
        // Object modification time is export time, not span start time.
        let mut objects = objects.into_values().collect::<Vec<_>>();
        objects.sort_by(|a, b| {
            // The one-minute late-flush allowance must not push every export
            // from the actual requested window out of the file budget. Scan
            // normal exports first, then possible late records if budget allows.
            late_export(a.location.as_ref(), &query, self.flush_ms)
                .cmp(&late_export(b.location.as_ref(), &query, self.flush_ms))
                .then(
                    b.last_modified
                        .cmp(&a.last_modified)
                        .then(a.location.cmp(&b.location)),
                )
        });
        let mut bytes = 0u64;
        let mut rows = 0usize;
        for object in objects {
            if page.scanned_files >= MAX_FILES
                || bytes.saturating_add(object.size) > MAX_BYTES
                || rows >= MAX_ROWS
            {
                page.truncated = true;
                break;
            }
            bytes += object.size;
            let key = object.location.to_string();
            let body = match self.bucket.get(&key).await {
                Ok(Some((body, _))) => body,
                Ok(None) => {
                    page.truncated = true;
                    page.warnings
                        .push("An export was removed during the query".into());
                    continue;
                }
                Err(_) => {
                    page.truncated = true;
                    page.warnings
                        .push("An export could not be downloaded".into());
                    continue;
                }
            };
            anyhow::ensure!(
                body.len() as u64 <= MAX_BYTES,
                "Export exceeds download budget"
            );
            let remaining = MAX_ROWS - rows;
            let query_copy = query.clone();
            let logs = key.starts_with("telemetry/logs/");
            let permit = permit.clone();
            let decoded = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                decode(body, &query_copy, logs, remaining)
            })
            .await?;
            page.scanned_files += 1;
            match decoded {
                Ok(decoded) => {
                    rows += decoded.rows;
                    page.matched_spans += decoded.matched;
                    page.truncated |= decoded.truncated;
                    page.spans.extend(decoded.spans);
                    page.spans
                        .sort_by_key(|span| std::cmp::Reverse(span.start_unix_us));
                    if page.spans.len() > MAX_RESULTS {
                        page.spans.truncate(MAX_RESULTS);
                        page.truncated = true;
                    }
                    page.logs.extend(decoded.logs);
                    page.logs.sort_by_key(|log| log.time_unix_us);
                    if page.logs.len() > MAX_RESULTS {
                        page.logs.truncate(MAX_RESULTS);
                        page.truncated = true;
                    }
                }
                Err(_) => {
                    page.truncated = true;
                    page.warnings
                        .push("An incompatible or oversized Parquet export was skipped".into());
                }
            }
        }
        if page.truncated {
            page.warnings.push("Partial query: bounded to 64 nodes, 256 listings, 64 files, 64 MiB downloaded, 200,000 rows and 500 spans/logs. Narrow the time range or choose a node.".into());
        }
        page.warnings.sort();
        page.warnings.dedup();
        Ok(page)
    }
}

#[derive(Default)]
struct Decoded {
    spans: Vec<TraceSpan>,
    logs: Vec<TraceLog>,
    rows: usize,
    matched: usize,
    truncated: bool,
}

fn text(row: &Row, name: &str) -> Option<String> {
    row.get_column_iter().find_map(|(key, value)| {
        if key == name {
            if let Field::Str(value) = value {
                Some(value.clone())
            } else {
                None
            }
        } else {
            None
        }
    })
}

fn integer(row: &Row, name: &str) -> Option<i64> {
    row.get_column_iter().find_map(|(key, value)| {
        if key == name {
            match value {
                Field::Long(value) => Some(*value),
                Field::Int(value) => Some(*value as i64),
                Field::UShort(value) => Some(*value as i64),
                Field::UByte(value) => Some(*value as i64),
                _ => None,
            }
        } else {
            None
        }
    })
}

/// Workflow exports predate application metadata and keep their scalar
/// attributes in a separate column. Preserve both contracts in the monitor.
fn span_metadata(row: &Row) -> Option<serde_json::Value> {
    let mut metadata = text(row, "metadata_json")
        .and_then(|json| serde_json::from_str::<serde_json::Value>(&json).ok())
        .filter(serde_json::Value::is_object);
    if let Some(attributes) = text(row, "attributes_json").and_then(|json| {
        serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&json).ok()
    }) {
        let fields = metadata.get_or_insert_with(|| serde_json::json!({}));
        let values = fields
            .as_object_mut()
            .unwrap()
            .entry("attributes")
            .or_insert_with(|| serde_json::json!({}));
        if let Some(values) = values.as_object_mut() {
            for (key, value) in attributes {
                values.entry(key).or_insert(value);
            }
        }
    }
    metadata
}

fn decode(
    body: bytes::Bytes,
    query: &Query,
    logs: bool,
    row_limit: usize,
) -> anyhow::Result<Decoded> {
    let reader = SerializedFileReader::new(body)?;
    let uncompressed: i64 = reader
        .metadata()
        .row_groups()
        .iter()
        .map(|group| group.total_byte_size())
        .sum();
    anyhow::ensure!(
        (0..=128 * 1024 * 1024).contains(&uncompressed),
        "Uncompressed export too large"
    );
    let mut result = Decoded::default();
    for row in reader.get_row_iter(None)? {
        if result.rows >= row_limit {
            result.truncated = true;
            break;
        }
        result.rows += 1;
        let row = row?;
        if logs {
            let at = integer(&row, "time_unix_us").unwrap_or(0);
            let trace_id = text(&row, "trace_id");
            let metadata: Option<serde_json::Value> =
                text(&row, "metadata_json").and_then(|json| serde_json::from_str(&json).ok());
            let node = text(&row, "node").unwrap_or_default();
            let body = text(&row, "body").unwrap_or_default();
            let severity_number = integer(&row, "severity_number");
            if at < query.from
                || at > query.to
                || (!query.trace.is_empty() && trace_id.as_deref() != Some(&query.trace))
                || (!query.node.is_empty() && node != query.node)
                || (!query.cell.is_empty()
                    && !metadata
                        .as_ref()
                        .and_then(|m| m.get("cell"))
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|cell| cell.contains(&query.cell)))
                || (query.logs_only && query.errors && severity_number.unwrap_or(9) < 17)
                || (query.logs_only
                    && !query.search.is_empty()
                    && !body.to_lowercase().contains(&query.search)
                    && !metadata
                        .as_ref()
                        .is_some_and(|m| m.to_string().to_lowercase().contains(&query.search)))
            {
                continue;
            }
            if result.logs.len() >= MAX_RESULTS {
                result.truncated = true;
                continue;
            }
            result.logs.push(TraceLog {
                node,
                trace_id,
                span_id: text(&row, "span_id"),
                time_unix_us: at,
                severity_text: text(&row, "severity_text").unwrap_or_else(|| "INFO".into()),
                body,
                severity_number,
                metadata,
            });
        } else {
            let span = TraceSpan {
                node: text(&row, "node").ok_or_else(|| anyhow::anyhow!("Missing node"))?,
                trace_id: text(&row, "trace_id")
                    .ok_or_else(|| anyhow::anyhow!("Missing trace_id"))?,
                span_id: text(&row, "span_id").unwrap_or_default(),
                parent_span_id: text(&row, "parent_span_id"),
                name: text(&row, "name").unwrap_or_default(),
                kind: integer(&row, "kind"),
                start_unix_us: integer(&row, "start_unix_us").unwrap_or(0),
                duration_us: integer(&row, "duration_us").unwrap_or(0),
                ok: row
                    .get_column_iter()
                    .any(|(key, value)| key == "ok" && matches!(value, Field::Bool(true))),
                error: text(&row, "error"),
                cell: text(&row, "cell"),
                url: text(&row, "url"),
                http_status: integer(&row, "http_status"),
                queue_wait_us: integer(&row, "queue_wait_us"),
                metadata: span_metadata(&row),
            };
            if query.matches(&span) {
                result.matched += 1;
                result.spans.push(span);
                if result.spans.len() > MAX_RESULTS * 2 {
                    result
                        .spans
                        .sort_by_key(|span| std::cmp::Reverse(span.start_unix_us));
                    result.spans.truncate(MAX_RESULTS);
                    result.truncated = true;
                }
            }
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_attributes_remain_queryable_alongside_application_metadata() {
        use celld::telemetry::{AttributeValue, Span, TraceIds};
        let mut query = Query::parse(None).unwrap();
        let mut span = Span::new(
            TraceIds {
                trace_id: [1; 16],
                span_id: [2; 8],
            },
            "workflow.step",
            1,
        );
        span.start_unix_us = query.to - 1000;
        span.attributes = vec![
            (
                "celld.workflow.instance_id".into(),
                AttributeValue::String("quote-214".into()),
            ),
            (
                "celld.workflow.retry_delay_ms".into(),
                AttributeValue::Float(30.5),
            ),
        ];
        let mut spans = [span];
        let bytes = celld::telemetry::encode_spans(&spans, "node-a", "local").unwrap();
        let page = decode(bytes.clone().into(), &query, false, 10).unwrap();
        let metadata = page.spans[0]
            .metadata
            .as_ref()
            .expect("native Workflow attributes must be visible in the monitor");
        assert_eq!(
            metadata["attributes"]["celld.workflow.instance_id"],
            "quote-214"
        );
        assert_eq!(
            metadata["attributes"]["celld.workflow.retry_delay_ms"],
            30.5
        );
        query.search = "quote-214".into();
        assert_eq!(
            decode(bytes.into(), &query, false, 10).unwrap().spans.len(),
            1
        );
        query.search.clear();
        spans[0].metadata = Some(
            serde_json::json!({"scopeName": "effect", "attributes": {"verified": true}, "events": [{"name": "loaded"}]}),
        );
        let bytes = celld::telemetry::encode_spans(&spans, "node-a", "local").unwrap();
        let page = decode(bytes.into(), &query, false, 10).unwrap();
        let metadata = page.spans[0].metadata.as_ref().unwrap();
        assert_eq!(metadata["attributes"]["verified"], true);
        assert_eq!(
            metadata["attributes"]["celld.workflow.retry_delay_ms"],
            30.5
        );
        assert_eq!(metadata["events"][0]["name"], "loaded");
    }

    #[test]
    fn late_flush_allowance_does_not_starve_the_requested_window() {
        let query = Query::parse(None).unwrap();
        let normal = format!(
            "telemetry/traces/node-a/2026/10/11/14/{}-1.parquet",
            query.to + 1_000_000
        );
        let delayed = format!(
            "telemetry/traces/node-b/2026/10/11/14/{}-1.parquet",
            query.to + 59_000_000
        );
        assert!(!late_export(&normal, &query, 2000));
        assert!(late_export(&delayed, &query, 2000));
        let mut exports = [delayed, normal.clone()];
        exports.sort_by_key(|key| late_export(key, &query, 2000));
        assert_eq!(exports[0], normal);
    }
    #[test]
    fn queries_reject_unbounded_ranges_invalid_ids_and_sql() {
        for query in [
            "trace_id=oops",
            "from=0",
            "min_ms=-1",
            "sql=select+1",
            "to=9223372036854775807",
        ] {
            assert!(Query::parse(Some(query)).is_err(), "{query}");
        }
        assert!(Query::parse(Some("minutes=15&errors=1&cell=Counter&min_ms=2")).is_ok());
    }

    #[test]
    fn real_parquet_exports_round_trip_and_filters_work() {
        let mut query = Query::parse(None).unwrap();
        let ids = celld::telemetry::TraceIds {
            trace_id: [1; 16],
            span_id: [2; 8],
        };
        let mut span = celld::telemetry::Span::new(ids, "cell.fetch", 1);
        span.start_unix_us = query.to - 1000;
        span.duration_us = 10_000;
        span.ok = false;
        span.cell = Some("Counter:alpha".into());
        span.http_status = Some(503);
        span.metadata = Some(
            serde_json::json!({"scopeName": "effect", "attributes": {"cart.items": 2}, "events": [{"name": "loaded"}]}),
        );
        let body = celld::telemetry::encode_spans(&[span], "node-a", "local").unwrap();
        let decoded = decode(body.clone().into(), &query, false, 10).unwrap();
        assert_eq!(decoded.spans.len(), 1);
        assert_eq!(decoded.spans[0].http_status, Some(503));
        assert_eq!(decoded.spans[0].kind, Some(1));
        assert_eq!(
            decoded.spans[0].metadata.as_ref().unwrap()["attributes"]["cart.items"],
            2
        );
        query.cell = "other".into();
        assert!(decode(body.into(), &query, false, 10)
            .unwrap()
            .spans
            .is_empty());
        let log = celld::telemetry::Log {
            trace_id: Some(ids.trace_id),
            span_id: Some(ids.span_id),
            time_unix_us: query.to - 500,
            severity: celld::telemetry::Severity::Fatal,
            body: "hello".into(),
            metadata: Some(
                serde_json::json!({"scopeName": "effect", "body": ["hello", 2], "attributes": {"verified": true}}),
            ),
        };
        query.trace = "01".repeat(16);
        query.cell.clear();
        let logs = celld::telemetry::encode_logs(&[log], "node-b", "local").unwrap();
        let decoded = decode(logs.clone().into(), &query, true, 10).unwrap();
        assert_eq!(decoded.logs[0].body, "hello");
        assert_eq!(decoded.logs[0].severity_number, Some(21));
        assert_eq!(
            decoded.logs[0].metadata.as_ref().unwrap()["body"],
            serde_json::json!(["hello", 2])
        );
        query.trace.clear();
        query.logs_only = true;
        query.search = "verified".into();
        assert_eq!(
            decode(logs.clone().into(), &query, true, 10)
                .unwrap()
                .logs
                .len(),
            1
        );
        query.search = "missing".into();
        assert!(decode(logs.into(), &query, true, 10)
            .unwrap()
            .logs
            .is_empty());
    }
}
