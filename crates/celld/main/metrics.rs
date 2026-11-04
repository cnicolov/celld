// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Operational metrics on the operator listener (`/metrics`) and over OTLP.
//!
//! One collection serves both: the library's instruments, plus the values
//! only this binary can reach. Those are the core's phase census, the isolate
//! pools, the node log's ensemble, and the fleet sample. Like `/state`, the
//! route is never served on the Worker listener and has no authentication.

use super::{response, AppHandle, HttpReply};
use celld::metrics::{attributes, Metric, Number};
use hyper::body::Incoming;
use hyper::{Method, Request, StatusCode};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, OnceLock};

static NODE_LOG: OnceLock<Arc<celld::node_log::NodeLogManager>> = OnceLock::new();

/// Live fleet nodes at the last successful fleet sample, this node included;
/// negative until the first. A failed sample keeps the previous count rather
/// than reporting an empty fleet.
static FLEET_LIVE: AtomicI64 = AtomicI64::new(-1);

pub(super) fn follow_node_log(manager: Arc<celld::node_log::NodeLogManager>) {
    let _ = NODE_LOG.set(manager);
}

pub(super) fn fleet_sampled(live: usize) {
    FLEET_LIVE.store(live as i64, Ordering::Relaxed);
}

/// The complete collection. Reading `/proc`, the allocator and isolate heaps
/// is blocking work, so it runs off the request threads; isolates that are
/// running report their last reading instead of being waited for.
pub(super) async fn collect(app: &AppHandle) -> Vec<Metric> {
    let Some(metrics) = celld::metrics::get() else {
        return Vec::new();
    };
    let runtime = app.runtime.clone();
    let mut collected = celld::asyncrt::blocking(move || {
        let mut collected = metrics.collect();
        if let Some(runtime) = &runtime {
            runtime.collect_isolate_metrics(&mut collected);
        }
        collected
    })
    .await
    .unwrap_or_default();
    if let Some(core) = app.core_metrics().await {
        celld::metrics::collect_core(&core, &mut collected);
    }
    if let Some(manager) = NODE_LOG.get() {
        collected.push(Metric::single(
            &celld::metrics::DURABILITY_FOLLOWER_COUNT,
            Number::Int(manager.proving_followers() as i64),
        ));
    }
    let live = FLEET_LIVE.load(Ordering::Relaxed);
    if live >= 0 {
        collected.push(Metric::numbers(
            &celld::metrics::FLEET_NODE_COUNT,
            vec![(
                attributes(&[("celld.node.state", "live")]),
                Number::Int(live),
            )],
        ));
    }
    collected
}

/// Start timing an event, or `None` when metrics are off.
pub(super) fn start() -> Option<u64> {
    celld::metrics::get().map(|_| celld::asyncrt::mono_us())
}

fn elapsed_seconds(started_us: u64) -> f64 {
    celld::asyncrt::mono_us().saturating_sub(started_us) as f64 / 1e6
}

/// One stateless Worker request, under the HTTP semantic conventions. The
/// duration ends at the response headers; a streamed body can outlast it.
pub(super) fn record_http(started_us: Option<u64>, method: &str, status: StatusCode) {
    let (Some(metrics), Some(started_us)) = (celld::metrics::get(), started_us) else {
        return;
    };
    let method = match method {
        "GET" | "HEAD" | "POST" | "PUT" | "DELETE" | "CONNECT" | "OPTIONS" | "TRACE" | "PATCH" => {
            method
        }
        _ => "_OTHER",
    };
    let status_code = status.as_u16().to_string();
    let mut pairs = vec![
        ("http.request.method", method),
        ("http.response.status_code", status_code.as_str()),
    ];
    if status.is_server_error() {
        pairs.push(("error.type", status_code.as_str()));
    }
    metrics
        .http_server_request_duration
        .record(&pairs, elapsed_seconds(started_us));
}

/// One queue consumer batch, under the FaaS conventions with the
/// messaging destination named.
pub(super) fn record_queue_batch(
    started_us: Option<u64>,
    script: &str,
    queue: &str,
    error: Option<&'static str>,
) {
    let (Some(metrics), Some(started_us)) = (celld::metrics::get(), started_us) else {
        return;
    };
    let pairs = [
        ("faas.trigger", "pubsub"),
        ("celld.script", celld::metrics::script_label(script)),
        ("messaging.destination.name", queue),
    ];
    metrics
        .faas_invoke_duration
        .record(&pairs, elapsed_seconds(started_us));
    if let Some(error) = error {
        let [trigger, script, queue] = pairs;
        metrics
            .faas_errors
            .add(&[trigger, script, queue, ("error.type", error)], 1);
    }
}

/// `GET /metrics`: the Prometheus text exposition of one collection.
pub(super) async fn handle(request: &Request<Incoming>, app: &AppHandle) -> HttpReply {
    if !matches!(*request.method(), Method::GET | Method::HEAD) {
        let mut reply = response(
            StatusCode::METHOD_NOT_ALLOWED,
            "{\"error\":\"method_not_allowed\"}",
        );
        reply.headers_mut().insert(
            hyper::header::ALLOW,
            hyper::header::HeaderValue::from_static("GET, HEAD"),
        );
        return reply;
    }
    if celld::metrics::get().is_none() {
        return response(
            StatusCode::NOT_FOUND,
            "{\"error\":\"metrics_disabled\",\"hint\":\"set CELLD_METRICS=1\"}",
        );
    }
    let collected = collect(app).await;
    let body = if request.method() == Method::HEAD {
        String::new()
    } else {
        celld::metrics::prometheus_text(&collected, celld::metrics::resource())
    };
    let mut reply = response(StatusCode::OK, body);
    for (name, value) in [
        ("content-type", "text/plain; version=0.0.4; charset=utf-8"),
        ("cache-control", "no-store"),
    ] {
        reply.headers_mut().insert(
            hyper::header::HeaderName::from_static(name),
            hyper::header::HeaderValue::from_static(value),
        );
    }
    reply
}
