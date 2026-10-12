// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! OTLP/HTTP protobuf encoding for the telemetry sink, by hand.
//!
//! The export messages celld emits are a small, stable corner of the
//! OTLP proto — flat spans and log records with scalar attributes — and
//! protobuf's wire format is varints and length-delimited fields. Hand
//! encoding keeps prost, tonic, and the generated proto crates out of
//! the binary, the same trade the Parquet sink made by skipping arrow.
//! Field numbers follow opentelemetry-proto v1: trace/v1/trace.proto,
//! logs/v1/logs.proto, common/v1/common.proto, resource/v1/resource.proto.

use crate::telemetry::AttributeValue;
use crate::telemetry::Log;
use crate::telemetry::Span;

const WIRE_VARINT: u64 = 0;
const WIRE_FIXED64: u64 = 1;
const WIRE_LEN: u64 = 2;

fn varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn key(out: &mut Vec<u8>, field: u64, wire: u64) {
    varint(out, (field << 3) | wire);
}

fn field_bytes(out: &mut Vec<u8>, field: u64, bytes: &[u8]) {
    key(out, field, WIRE_LEN);
    varint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

fn field_str(out: &mut Vec<u8>, field: u64, value: &str) {
    field_bytes(out, field, value.as_bytes());
}

fn field_varint(out: &mut Vec<u8>, field: u64, value: u64) {
    key(out, field, WIRE_VARINT);
    varint(out, value);
}

fn field_fixed64(out: &mut Vec<u8>, field: u64, value: u64) {
    key(out, field, WIRE_FIXED64);
    out.extend_from_slice(&value.to_le_bytes());
}

/// common.v1.AnyValue: string_value=1, bool_value=2, int_value=3, double_value=4.
fn any_string(value: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(value.len() + 4);
    field_str(&mut out, 1, value);
    out
}

fn any_bool(value: bool) -> Vec<u8> {
    let mut out = Vec::new();
    field_varint(&mut out, 2, value as u64);
    out
}

fn any_int(value: i64) -> Vec<u8> {
    let mut out = Vec::new();
    field_varint(&mut out, 3, value as u64);
    out
}

fn any_double(value: f64) -> Vec<u8> {
    let mut out = Vec::new();
    field_fixed64(&mut out, 4, value.to_bits());
    out
}

/// Preserve application attribute/body types, including arrays and objects.
fn any_value(value: &serde_json::Value) -> Vec<u8> {
    use serde_json::Value;
    match value {
        Value::String(value) => any_string(value),
        Value::Bool(value) => any_bool(*value),
        Value::Number(value) if value.as_i64().is_some() => any_int(value.as_i64().unwrap()),
        Value::Number(value) => {
            let mut out = Vec::new();
            field_fixed64(&mut out, 4, value.as_f64().unwrap_or_default().to_bits());
            out
        }
        Value::Array(values) => {
            let mut array = Vec::new();
            for value in values {
                field_bytes(&mut array, 1, &any_value(value));
            }
            let mut out = Vec::new();
            field_bytes(&mut out, 5, &array);
            out
        }
        Value::Object(values) => {
            let mut list = Vec::new();
            for (key, value) in values {
                key_value(&mut list, 1, key, any_value(value));
            }
            let mut out = Vec::new();
            field_bytes(&mut out, 6, &list);
            out
        }
        Value::Null => Vec::new(),
    }
}

fn application_attributes(out: &mut Vec<u8>, field: u64, value: Option<&serde_json::Value>) {
    if let Some(values) = value.and_then(serde_json::Value::as_object) {
        for (name, value) in values {
            key_value(out, field, name, any_value(value));
        }
    }
}

/// common.v1.KeyValue: key=1, value=2 (AnyValue).
fn key_value(out: &mut Vec<u8>, field: u64, name: &str, value: Vec<u8>) {
    let mut kv = Vec::with_capacity(name.len() + value.len() + 8);
    field_str(&mut kv, 1, name);
    field_bytes(&mut kv, 2, &value);
    field_bytes(out, field, &kv);
}

/// resource.v1.Resource: attributes=1.
fn resource(node: &str, region: &str, service: &str) -> Vec<u8> {
    let mut out = Vec::new();
    key_value(&mut out, 1, "service.name", any_string(service));
    key_value(
        &mut out,
        1,
        "service.version",
        any_string(env!("CARGO_PKG_VERSION")),
    );
    key_value(&mut out, 1, "service.instance.id", any_string(node));
    key_value(&mut out, 1, "celld.region", any_string(region));
    out
}

/// common.v1.InstrumentationScope: name=1, version=2.
fn scope(name: &str) -> Vec<u8> {
    let mut out = Vec::new();
    field_str(&mut out, 1, name);
    if name == "celld" {
        field_str(&mut out, 2, env!("CARGO_PKG_VERSION"));
    }
    out
}

/// trace.v1.Span.
fn span_message(span: &Span) -> Vec<u8> {
    let mut out = Vec::new();
    field_bytes(&mut out, 1, &span.ids.trace_id);
    field_bytes(&mut out, 2, &span.ids.span_id);
    if let Some(parent) = span.parent_span_id {
        field_bytes(&mut out, 4, &parent);
    }
    field_str(&mut out, 5, &span.name);
    field_varint(&mut out, 6, span.kind as u64);
    let start_ns = span.start_unix_us.max(0) as u64 * 1_000;
    let end_ns = start_ns + span.duration_us.max(0) as u64 * 1_000;
    field_fixed64(&mut out, 7, start_ns);
    field_fixed64(&mut out, 8, end_ns);
    // Attributes (9): the same promoted columns the Parquet schema has,
    // under otel semantic-convention names where one exists.
    if let Some(url) = &span.url {
        key_value(&mut out, 9, "url.full", any_string(url));
    }
    if let Some(status) = span.http_status {
        key_value(
            &mut out,
            9,
            "http.response.status_code",
            any_int(status as i64),
        );
    }
    if let Some(request_id) = &span.request_id {
        key_value(&mut out, 9, "celld.request_id", any_string(request_id));
    }
    if let Some(cell) = &span.cell {
        key_value(&mut out, 9, "celld.cell", any_string(cell));
    }
    if let Some(epoch) = span.epoch {
        key_value(&mut out, 9, "celld.epoch", any_int(epoch as i64));
    }
    if let Some(isolate) = span.isolate {
        key_value(&mut out, 9, "celld.isolate", any_int(isolate as i64));
    }
    if let Some(queue_wait_us) = span.queue_wait_us {
        key_value(&mut out, 9, "celld.queue_wait_us", any_int(queue_wait_us));
    }
    if let Some(remote) = span.parent_remote {
        key_value(&mut out, 9, "celld.parent_remote", any_bool(remote));
    }
    for (name, value) in &span.attributes {
        let value = match value {
            AttributeValue::String(value) => any_string(value),
            AttributeValue::Int(value) => any_int(*value),
            AttributeValue::Bool(value) => any_bool(*value),
            AttributeValue::Float(value) => any_double(*value),
        };
        key_value(&mut out, 9, name, value);
    }
    if let Some(metadata) = &span.metadata {
        application_attributes(&mut out, 9, metadata.get("attributes"));
        if let Some(events) = metadata.get("events").and_then(serde_json::Value::as_array) {
            for event in events {
                let mut encoded = Vec::new();
                if let Some(at) = event
                    .get("timeUnixNano")
                    .and_then(serde_json::Value::as_str)
                    .and_then(|at| at.parse().ok())
                {
                    field_fixed64(&mut encoded, 1, at);
                }
                if let Some(name) = event.get("name").and_then(serde_json::Value::as_str) {
                    field_str(&mut encoded, 2, name);
                }
                application_attributes(&mut encoded, 3, event.get("attributes"));
                field_bytes(&mut out, 11, &encoded);
            }
        }
        if let Some(links) = metadata.get("links").and_then(serde_json::Value::as_array) {
            for link in links {
                let mut encoded = Vec::new();
                if let Some(trace) = link
                    .get("traceId")
                    .and_then(serde_json::Value::as_str)
                    .and_then(|id| crate::telemetry::unhex::<16>(id.as_bytes()))
                {
                    field_bytes(&mut encoded, 1, &trace);
                }
                if let Some(span) = link
                    .get("spanId")
                    .and_then(serde_json::Value::as_str)
                    .and_then(|id| crate::telemetry::unhex::<8>(id.as_bytes()))
                {
                    field_bytes(&mut encoded, 2, &span);
                }
                application_attributes(&mut encoded, 4, link.get("attributes"));
                field_bytes(&mut out, 13, &encoded);
            }
        }
    }
    // Status (15): unset when ok, per the spec; ERROR (code=3 value 2)
    // with the message when not.
    if !span.ok
        || span
            .metadata
            .as_ref()
            .and_then(|m| m.get("statusCode"))
            .and_then(serde_json::Value::as_u64)
            == Some(1)
    {
        let mut status = Vec::new();
        if let Some(error) = &span.error {
            field_str(&mut status, 2, error);
        }
        field_varint(&mut status, 3, if span.ok { 1 } else { 2 });
        field_bytes(&mut out, 15, &status);
    }
    out
}

/// collector.v1.ExportTraceServiceRequest: resource_spans=1, holding one
/// ResourceSpans{resource=1, scope_spans=2{scope=1, spans=2}}.
pub fn traces_request(spans: &[Span], node: &str, region: &str, service: &str) -> Vec<u8> {
    let mut groups = std::collections::BTreeMap::new();
    for span in spans {
        let identity = identity(span.metadata.as_ref(), service);
        let scope_spans = groups.entry(identity).or_insert_with(|| {
            let mut out = Vec::new();
            field_bytes(&mut out, 1, &scope(identity.1));
            out
        });
        field_bytes(scope_spans, 2, &span_message(span));
    }
    let mut out = Vec::new();
    for ((service, _), scopes) in groups {
        let mut resource_spans = Vec::new();
        field_bytes(&mut resource_spans, 1, &resource(node, region, service));
        field_bytes(&mut resource_spans, 2, &scopes);
        field_bytes(&mut out, 1, &resource_spans);
    }
    out
}

/// logs.v1.LogRecord: time=1, severity_number=2, severity_text=3, body=5,
/// trace_id=9, span_id=10, observed_time=11.
fn log_message(log: &Log) -> Vec<u8> {
    let mut out = Vec::new();
    let time_ns = log.time_unix_us.max(0) as u64 * 1_000;
    field_fixed64(&mut out, 1, time_ns);
    field_varint(&mut out, 2, log.severity.number() as u64);
    field_str(&mut out, 3, log.severity.text());
    let body = log
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("body"));
    field_bytes(
        &mut out,
        5,
        &body.map_or_else(|| any_string(&log.body), any_value),
    );
    application_attributes(
        &mut out,
        6,
        log.metadata
            .as_ref()
            .and_then(|metadata| metadata.get("attributes")),
    );
    if let Some(trace_id) = log.trace_id {
        field_bytes(&mut out, 9, &trace_id);
    }
    if let Some(span_id) = log.span_id {
        field_bytes(&mut out, 10, &span_id);
    }
    field_fixed64(&mut out, 11, time_ns);
    out
}

/// collector.v1.ExportLogsServiceRequest: resource_logs=1, holding one
/// ResourceLogs{resource=1, scope_logs=2{scope=1, log_records=2}}.
pub fn logs_request(logs: &[Log], node: &str, region: &str, service: &str) -> Vec<u8> {
    let mut groups = std::collections::BTreeMap::new();
    for log in logs {
        let identity = identity(log.metadata.as_ref(), service);
        let scope_logs = groups.entry(identity).or_insert_with(|| {
            let mut out = Vec::new();
            field_bytes(&mut out, 1, &scope(identity.1));
            out
        });
        field_bytes(scope_logs, 2, &log_message(log));
    }
    let mut out = Vec::new();
    for ((service, _), scopes) in groups {
        let mut resource_logs = Vec::new();
        field_bytes(&mut resource_logs, 1, &resource(node, region, service));
        field_bytes(&mut resource_logs, 2, &scopes);
        field_bytes(&mut out, 1, &resource_logs);
    }
    out
}

fn identity<'a>(metadata: Option<&'a serde_json::Value>, service: &'a str) -> (&'a str, &'a str) {
    let name = |key| {
        metadata
            .and_then(|value| value.get(key))
            .and_then(serde_json::Value::as_str)
    };
    (
        name("serviceName").unwrap_or(service),
        name("scopeName").unwrap_or("celld"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // Read length-delimited protobuf fields without adding a protobuf runtime.
    fn fields(mut input: &[u8], wanted: u64) -> Vec<&[u8]> {
        fn take_varint(input: &mut &[u8]) -> u64 {
            let mut result = 0;
            for shift in (0..64).step_by(7) {
                let byte = input[0];
                *input = &input[1..];
                result |= u64::from(byte & 0x7f) << shift;
                if byte < 128 {
                    return result;
                }
            }
            panic!("invalid test protobuf")
        }
        let mut values = Vec::new();
        while !input.is_empty() {
            let key = take_varint(&mut input);
            match key & 7 {
                0 => {
                    take_varint(&mut input);
                }
                1 => input = &input[8..],
                2 => {
                    let length = take_varint(&mut input) as usize;
                    if key >> 3 == wanted {
                        values.push(&input[..length]);
                    }
                    input = &input[length..];
                }
                wire => panic!("unexpected wire {wire}"),
            }
        }
        values
    }

    #[test]
    fn application_otlp_preserves_typed_bodies_events_links_and_resource_groups() {
        let ids = crate::telemetry::TraceIds {
            trace_id: [1; 16],
            span_id: [2; 8],
        };
        let native = Span::new(ids, "celld.fetch", 2);
        let mut application = Span::new(ids, String::from("effect.checkout"), 1);
        application.metadata = Some(serde_json::json!({
            "serviceName": "checkout", "scopeName": "effect", "statusCode": 1,
            "attributes": {"verified": true, "items": 2},
            "events": [{"name": "loaded", "timeUnixNano": "1000000", "attributes": {"hit": true}}],
            "links": [{"traceId": "03".repeat(16), "spanId": "04".repeat(8), "attributes": {"linked": true}}],
        }));
        let encoded = span_message(&application);
        assert_eq!(fields(&encoded, 11).len(), 1);
        assert_eq!(
            fields(fields(&encoded, 11)[0], 2),
            vec![b"loaded".as_slice()]
        );
        assert_eq!(fields(fields(&encoded, 13)[0], 1), vec![[3; 16].as_slice()]);
        let request = traces_request(&[native, application], "node-a", "local", "celld");
        assert_eq!(fields(&request, 1).len(), 2);
        let log = Log {
            trace_id: Some(ids.trace_id),
            span_id: Some(ids.span_id),
            time_unix_us: 1000,
            severity: crate::telemetry::Severity::Fatal,
            body: "display".into(),
            metadata: Some(
                serde_json::json!({"body": ["structured", 2, true, {"nested": 3}], "attributes": {"verified": true}}),
            ),
        };
        let encoded = log_message(&log);
        let body = fields(&encoded, 5)[0];
        let array = fields(body, 5)[0];
        let elements = fields(array, 1);
        assert_eq!(elements.len(), 4);
        assert_eq!(fields(elements[0], 1), vec![b"structured".as_slice()]);
        assert_eq!(elements[1], any_int(2));
        assert_eq!(elements[2], any_bool(true));
        assert_eq!(fields(elements[3], 6).len(), 1);
        assert_eq!(fields(&encoded, 9), vec![[1; 16].as_slice()]);
        assert_eq!(fields(&encoded, 3), vec![b"FATAL".as_slice()]);
    }
}
