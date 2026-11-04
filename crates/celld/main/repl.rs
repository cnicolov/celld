// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Explicitly enabled arbitrary evaluation on the internal operator listener.

use super::{response, AppHandle, HttpReply};
use hyper::body::Incoming;
use hyper::{Method, Request, StatusCode};

const MAX_ATTACH_BODY_BYTES: usize = 4 * 1024;
const MAX_EVALUATE_BODY_BYTES: usize = 1024 * 1024 + 4 * 1024;

pub(super) async fn handle(request: Request<Incoming>, app: AppHandle) -> HttpReply {
    let mut reply = handle_inner(request, app).await;
    for (name, value) in [
        ("cache-control", "no-store"),
        ("x-content-type-options", "nosniff"),
        ("referrer-policy", "no-referrer"),
    ] {
        reply.headers_mut().insert(
            hyper::header::HeaderName::from_static(name),
            hyper::header::HeaderValue::from_static(value),
        );
    }
    reply
}

async fn handle_inner(request: Request<Incoming>, app: AppHandle) -> HttpReply {
    if !celld::repl::enabled() {
        return error_response(
            StatusCode::NOT_FOUND,
            "REPL disabled; local dev enables it, otherwise set CELLD_REPL=1",
        );
    }
    if !super::monitor::same_origin(request.headers())
        || (std::env::var_os("CELLD_INTERNAL_DEV_STORE").is_some()
            && !super::monitor::local_host(request.headers()))
    {
        return error_response(StatusCode::FORBIDDEN, "cross-origin REPL request");
    }
    let path = request.uri().path().to_string();
    if let Some(id) = path
        .strip_prefix("/repl/")
        .and_then(|suffix| suffix.strip_suffix("/complete"))
    {
        if request.method() != Method::POST {
            return error_response(StatusCode::METHOD_NOT_ALLOWED, "POST required");
        }
        if id.len() != 32 || !id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return error_response(StatusCode::BAD_REQUEST, "invalid REPL session ID");
        }
        return complete(request, app, id.to_string()).await;
    }
    let session = path.strip_prefix("/repl/").map(str::to_string);
    if let Some(id) = &session {
        if id.len() != 32 || !id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return error_response(StatusCode::BAD_REQUEST, "invalid REPL session ID");
        }
    }
    match (request.method().clone(), session) {
        (Method::POST, None) if path == "/repl" => attach(request, app).await,
        (Method::POST, Some(id)) => evaluate(request, app, id).await,
        (Method::DELETE, Some(id)) => {
            if celld::repl::close(&id) {
                response(StatusCode::OK, "{\"ok\":true}")
            } else {
                error_response(StatusCode::NOT_FOUND, "REPL session not found")
            }
        }
        (Method::GET | Method::HEAD, _) => {
            error_response(StatusCode::METHOD_NOT_ALLOWED, "POST or DELETE required")
        }
        _ => error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed"),
    }
}

async fn attach(request: Request<Incoming>, app: AppHandle) -> HttpReply {
    let Some(runtime) = &app.runtime else {
        return error_response(StatusCode::SERVICE_UNAVAILABLE, "runtime unavailable");
    };
    let body = match super::collect_limited_body(request.into_body(), MAX_ATTACH_BODY_BYTES).await {
        Ok(body) => body,
        Err(error) => return super::body_read_error("REPL attach", error),
    };
    let attach = match serde_json::from_slice::<celld::repl::AttachRequest>(&body) {
        Ok(attach) => attach,
        Err(error) => {
            return error_response(StatusCode::BAD_REQUEST, format!("invalid attach: {error}"))
        }
    };
    if let Some(isolate) = attach.isolate.as_deref() {
        if isolate.is_empty()
            || isolate.len() > 20
            || !isolate.bytes().all(|byte| byte.is_ascii_digit())
        {
            return error_response(StatusCode::BAD_REQUEST, "invalid isolate ID");
        }
    }
    match celld::repl::attach(runtime, attach.isolate.as_deref(), attach.object.as_deref()).await {
        Ok(session) => json(StatusCode::OK, &session),
        Err(error) => repl_error(error),
    }
}

async fn evaluate(request: Request<Incoming>, app: AppHandle, id: String) -> HttpReply {
    if app.runtime.is_none() {
        return error_response(StatusCode::SERVICE_UNAVAILABLE, "runtime unavailable");
    }
    let body = match super::collect_limited_body(request.into_body(), MAX_EVALUATE_BODY_BYTES).await
    {
        Ok(body) => body,
        Err(error) => return super::body_read_error("REPL evaluation", error),
    };
    let evaluation = match serde_json::from_slice::<celld::repl::EvaluateRequest>(&body) {
        Ok(evaluation) => evaluation,
        Err(error) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                format!("invalid evaluation: {error}"),
            )
        }
    };
    match celld::repl::evaluate(&app, &id, evaluation.code).await {
        Ok(evaluation) => json(StatusCode::OK, &evaluation),
        Err(error) => repl_error(error),
    }
}

async fn complete(request: Request<Incoming>, app: AppHandle, id: String) -> HttpReply {
    if app.runtime.is_none() {
        return error_response(StatusCode::SERVICE_UNAVAILABLE, "runtime unavailable");
    }
    let body = match super::collect_limited_body(request.into_body(), MAX_EVALUATE_BODY_BYTES).await
    {
        Ok(body) => body,
        Err(error) => return super::body_read_error("REPL completeness check", error),
    };
    let evaluation = match serde_json::from_slice::<celld::repl::EvaluateRequest>(&body) {
        Ok(evaluation) => evaluation,
        Err(error) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                format!("invalid completeness check: {error}"),
            )
        }
    };
    match celld::repl::complete(&id, evaluation.code).await {
        Ok(complete) => json(StatusCode::OK, &serde_json::json!({ "complete": complete })),
        Err(error) => repl_error(error),
    }
}

fn repl_error(error: celld::repl::Error) -> HttpReply {
    let status = match error {
        celld::repl::Error::IsolateGone
        | celld::repl::Error::ObjectGone(_)
        | celld::repl::Error::SessionGone => StatusCode::NOT_FOUND,
        celld::repl::Error::InvalidObject(_) => StatusCode::BAD_REQUEST,
        celld::repl::Error::CodeTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
        celld::repl::Error::Admission(_) | celld::repl::Error::Gate(_) => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        celld::repl::Error::Evaluation(_) => StatusCode::UNPROCESSABLE_ENTITY,
    };
    error_response(status, error)
}

fn json(status: StatusCode, value: &impl serde::Serialize) -> HttpReply {
    response(
        status,
        serde_json::to_vec(value).expect("serializable REPL response"),
    )
}

fn error_response(status: StatusCode, message: impl std::fmt::Display) -> HttpReply {
    response(
        status,
        serde_json::json!({ "error": message.to_string() }).to_string(),
    )
}
