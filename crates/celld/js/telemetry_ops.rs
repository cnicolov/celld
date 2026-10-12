// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The in-process telemetry seam. No sockets, exporter tasks or JS-held Rust
//! pointers: each finished record enters the existing bounded native queue.
use super::*;
use crate::telemetry::{self, Log, Severity, Span, TraceContext};
use serde::Deserialize;

const RECORD_CAP: usize = 32 * 1024;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FinishedSpan {
    trace_id: String,
    span_id: String,
    parent_span_id: Option<String>,
    name: String,
    kind: u8,
    start_time_unix_nano: String,
    end_time_unix_nano: String,
    sampled: bool,
    status_code: u8,
    error: Option<String>,
    metadata: Option<serde_json::Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ApplicationLog {
    trace_id: Option<String>,
    span_id: Option<String>,
    time_unix_nano: String,
    severity_number: i32,
    body: String,
    metadata: Option<serde_json::Value>,
}

fn id<const N: usize>(text: &str) -> Option<[u8; N]> {
    let bytes = telemetry::unhex(text.as_bytes())?;
    (bytes != [0; N]).then_some(bytes)
}

fn micros(nanos: &str) -> Option<i64> {
    // OTLP fixed64 timestamps must also remain representable when the native
    // sink converts the microsecond column back to nanoseconds.
    i64::try_from(nanos.parse::<u64>().ok()? / 1000).ok()
}

fn decode_span(json: &str) -> Option<Span> {
    if json.len() > RECORD_CAP {
        return None;
    }
    let input: FinishedSpan = serde_json::from_str(json).ok()?;
    if !input.sampled
        || input.name.is_empty()
        || input.name.len() > 1024
        || !(1..=5).contains(&input.kind)
        || input.status_code > 2
        || input
            .metadata
            .as_ref()
            .is_some_and(|value| !value.is_object())
    {
        return None;
    }
    let start = micros(&input.start_time_unix_nano)?;
    let end = micros(&input.end_time_unix_nano)?;
    let duration = end.checked_sub(start).filter(|duration| *duration >= 0)?;
    let mut span = Span::new(
        telemetry::TraceIds {
            trace_id: id(&input.trace_id)?,
            span_id: id(&input.span_id)?,
        },
        input.name,
        input.kind,
    );
    span.parent_span_id = optional_id(input.parent_span_id.as_deref())?;
    span.parent_remote = span.parent_span_id.map(|_| false);
    span.start_unix_us = start;
    span.duration_us = duration;
    span.ok = input.status_code != 2;
    span.error = input.error.map(telemetry::cap_error);
    let mut metadata = input.metadata.unwrap_or_else(|| serde_json::json!({}));
    metadata
        .as_object_mut()?
        .insert("statusCode".into(), input.status_code.into());
    span.metadata = Some(metadata);
    Some(span)
}

fn optional_id<const N: usize>(value: Option<&str>) -> Option<Option<[u8; N]>> {
    match value {
        None => Some(None),
        Some(value) => id(value).map(Some),
    }
}

fn decode_log(json: &str) -> Option<Log> {
    if json.len() > RECORD_CAP {
        return None;
    }
    let input: ApplicationLog = serde_json::from_str(json).ok()?;
    if input
        .metadata
        .as_ref()
        .is_some_and(|value| !value.is_object())
    {
        return None;
    }
    let trace_id = optional_id(input.trace_id.as_deref())?;
    let span_id = optional_id(input.span_id.as_deref())?;
    if span_id.is_some() && trace_id.is_none() {
        return None;
    }
    Some(Log {
        trace_id,
        span_id,
        time_unix_us: micros(&input.time_unix_nano)?,
        severity: Severity::from_number(input.severity_number)?,
        body: input.body,
        metadata: input.metadata,
    })
}

fn payload(scope: &mut v8::PinScope, value: v8::Local<v8::Value>) -> Option<String> {
    let string = v8::Local::<v8::String>::try_from(value).ok()?;
    // Check before copying out of V8, then check UTF-8 bytes as well.
    if string.length() > RECORD_CAP {
        return None;
    }
    let text = string.to_rust_string_lossy(scope);
    (text.len() <= RECORD_CAP).then_some(text)
}

pub(super) fn op_current(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let Some(context) = current_trace_context(scope) else {
        return;
    };
    let header = telemetry::traceparent(&context);
    let json = serde_json::json!({
        "traceId": &header[3..35], "spanId": &header[36..52], "sampled": context.sampled,
    })
    .to_string();
    if let Some(json) = v8::String::new(scope, &json) {
        if let Some(value) = v8::json::parse(scope, json) {
            rv.set(value);
        }
    }
}

pub(super) fn op_with_context(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let Ok(callback) = v8::Local::<v8::Function>::try_from(args.get(1)) else {
        return loader_throw(scope, "withContext requires a callback");
    };
    let requested = v8::Local::<v8::String>::try_from(args.get(0))
        .ok()
        .filter(|value| value.length() == 55)
        .and_then(|value| telemetry::parse_traceparent(&value.to_rust_string_lossy(scope)));
    let previous = match (current_trace_context(scope), requested) {
        (Some(ambient), Some(requested)) => install_trace(
            scope,
            Some(&TraceContext {
                trace_id: requested.trace_id,
                span_id: requested.span_id,
                // Application code cannot upgrade an unsampled native context.
                sampled: ambient.sampled && requested.sampled,
            }),
        ),
        (_, None) => return loader_throw(scope, "Invalid telemetry context"),
        (None, _) => None,
    };
    let receiver = v8::undefined(scope).into();
    let result = callback.call(scope, receiver, &[]);
    // Also restore when the callback throws. V8 promise reactions snapshot the
    // temporary trace rider; the ALS frame and native IoContext are unchanged.
    restore_trace(scope, previous);
    if let Some(value) = result {
        rv.set(value);
    }
}

pub(super) fn op_span(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    rv.set(v8::Boolean::new(scope, false).into());
    if !current_trace_context(scope).is_some_and(|context| context.sampled) {
        return;
    }
    let Some(mut span) = payload(scope, args.get(0)).as_deref().and_then(decode_span) else {
        telemetry::count_rejected();
        return;
    };
    // Resource ownership is native, not an application-supplied column.
    span.cell = current_reaction_or_untracked_io_context(scope)
        .and_then(|context| current_cell_event(&context))
        .map(|event| event.storage);
    rv.set(v8::Boolean::new(scope, telemetry::try_record(span)).into());
}

pub(super) fn op_log(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    rv.set(v8::Boolean::new(scope, false).into());
    // Structured Effect logs are independent of trace sampling, unlike native
    // console capture. They still need an active entry, not bootstrap authority.
    let Some(ambient) = current_trace_context(scope) else {
        return;
    };
    let Some(mut log) = payload(scope, args.get(0)).as_deref().and_then(decode_log) else {
        telemetry::count_rejected();
        return;
    };
    if log.trace_id.is_none() {
        log.trace_id = Some(ambient.trace_id);
        log.span_id = Some(ambient.span_id);
    }
    let cell = current_reaction_or_untracked_io_context(scope)
        .and_then(|context| current_cell_event(&context))
        .map(|event| event.storage);
    if cell.is_some() || log.metadata.is_some() {
        let metadata = log.metadata.get_or_insert_with(|| serde_json::json!({}));
        if let Some(metadata) = metadata.as_object_mut() {
            metadata.remove("cell");
            if let Some(cell) = cell {
                metadata.insert("cell".into(), cell.into());
            }
        }
    }
    rv.set(v8::Boolean::new(scope, telemetry::try_record_log(log)).into());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn application_records_preserve_identity_types_and_validate_limits() {
        let mut input = serde_json::json!({
            "traceId": "01".repeat(16), "spanId": "02".repeat(8), "parentSpanId": "03".repeat(8),
            "name": "effect.checkout", "kind": 1, "sampled": true, "statusCode": 2,
            "startTimeUnixNano": "1000001", "endTimeUnixNano": "2000001", "error": "declined",
            "metadata": {"attributes": {"items": 2, "verified": true}, "events": []},
        });
        let span = decode_span(&input.to_string()).unwrap();
        assert_eq!(span.ids.trace_id, [1; 16]);
        assert_eq!(span.ids.span_id, [2; 8]);
        assert_eq!(span.parent_span_id, Some([3; 8]));
        assert_eq!(span.duration_us, 1000);
        assert!(!span.ok);
        assert_eq!(span.metadata.unwrap()["attributes"]["items"], 2);
        input["spanId"] = "00".repeat(8).into();
        assert!(decode_span(&input.to_string()).is_none());
        input["spanId"] = "02".repeat(8).into();
        input["endTimeUnixNano"] = "-1".into();
        assert!(decode_span(&input.to_string()).is_none());
        assert!(decode_span(&"x".repeat(RECORD_CAP + 1)).is_none());
        assert!(micros("18446744073709551616").is_none());
        for number in [1, 5, 9, 13, 17, 21] {
            let log = decode_log(&serde_json::json!({"timeUnixNano": "1000000", "severityNumber": number, "body": "hello"}).to_string()).unwrap();
            assert_eq!(log.severity.number(), number);
        }
        assert!(decode_log(r#"{"timeUnixNano":"0","severityNumber":99,"body":"no"}"#).is_none());
    }
}
