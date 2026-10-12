// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The monitor is read-only and exists only on the operator listener. Its
//! static assets are generated from Rust and embedded at native build time.

use super::{response, AppHandle, HttpReply};
use hyper::body::{Bytes, Incoming};
use hyper::{HeaderMap, Method, Request, StatusCode};

pub(super) async fn handle(request: Request<Incoming>, app: AppHandle) -> HttpReply {
    let mut reply = handle_inner(&request, &app).await;
    for (name, value) in [
        ("cache-control", "no-store"),
        ("x-content-type-options", "nosniff"),
        ("referrer-policy", "no-referrer"),
        (
            "content-security-policy",
            celld::monitor::CONTENT_SECURITY_POLICY,
        ),
    ] {
        reply.headers_mut().insert(
            hyper::header::HeaderName::from_static(name),
            hyper::header::HeaderValue::from_str(value).expect("static monitor header"),
        );
    }
    reply
}

async fn handle_inner(request: &Request<Incoming>, app: &AppHandle) -> HttpReply {
    if !matches!(*request.method(), Method::GET | Method::HEAD) {
        let mut reply = response(StatusCode::METHOD_NOT_ALLOWED, "{\"error\":\"read_only\"}");
        reply.headers_mut().insert(
            hyper::header::ALLOW,
            hyper::header::HeaderValue::from_static("GET, HEAD"),
        );
        return reply;
    }
    if !allowed_origin(request.headers(), request.uri().path())
        || (std::env::var_os("CELLD_INTERNAL_DEV_STORE").is_some()
            && !local_host(request.headers()))
    {
        return response(
            StatusCode::FORBIDDEN,
            "{\"error\":\"cross_origin_monitor_request\"}",
        );
    }
    if !celld::monitor::enabled() {
        return response(
            StatusCode::NOT_FOUND,
            "{\"error\":\"monitor_disabled\",\"hint\":\"set CELLD_MONITOR=1\"}",
        );
    }
    let name = match request.uri().path() {
        "/monitor" | "/monitor/" => "index.html",
        "/monitor/cluster" => {
            let (query, node) = match cluster_query(request.uri().query()) {
                Ok(query) => query,
                Err(error) => return json_error(StatusCode::BAD_REQUEST, error),
            };
            return match super::cluster_monitor::service()
                .snapshot(app, &query, node.as_deref())
                .await
            {
                Ok(snapshot) => json_reply(request.method(), &snapshot),
                Err(error) => json_error(StatusCode::SERVICE_UNAVAILABLE, error),
            };
        }
        "/monitor/traces" => {
            let query = match super::trace_monitor::Query::parse(request.uri().query()) {
                Ok(query) => query,
                Err(error) => return json_error(StatusCode::BAD_REQUEST, error),
            };
            return match super::trace_monitor::query(query).await {
                Ok(page) => json_reply(request.method(), &page),
                Err(error) => json_error(StatusCode::SERVICE_UNAVAILABLE, error),
            };
        }
        "/monitor/api" => {
            let query = match celld::monitor::Query::parse(request.uri().query()) {
                Ok(query) => query,
                Err(error) => {
                    return response(
                        StatusCode::BAD_REQUEST,
                        serde_json::json!({"error": error.to_string()}).to_string(),
                    )
                }
            };
            let Some(runtime) = &app.runtime else {
                return response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "{\"error\":\"runtime_unavailable\"}",
                );
            };
            let snapshot = runtime.monitor_snapshot(&query);
            let bytes = serde_json::to_vec(&snapshot).expect("serializable monitor snapshot");
            return response(
                StatusCode::OK,
                if request.method() == Method::HEAD {
                    Vec::new()
                } else {
                    bytes
                },
            );
        }
        path => path.strip_prefix("/monitor/").unwrap_or(""),
    };
    if name == "index.html" && !celld::monitor::BUILT {
        let html = "<!doctype html><html lang=\"en\"><meta charset=\"utf-8\"><title>celld monitor</title><h1>Dashboard assets have not been built</h1><p>The native monitoring API is available at <a href=\"/monitor/api\">/monitor/api</a>.</p><p>Build the Rust/WebAssembly dashboard, then rebuild celld:</p><pre>trunk build --release --config crates/monitor/Trunk.toml\ncargo build -p celld</pre></html>";
        let mut reply = response(
            StatusCode::SERVICE_UNAVAILABLE,
            if request.method() == Method::HEAD {
                ""
            } else {
                html
            },
        );
        reply.headers_mut().insert(
            hyper::header::CONTENT_TYPE,
            hyper::header::HeaderValue::from_static("text/html; charset=utf-8"),
        );
        return reply;
    }
    let Some(asset) = celld::monitor::asset(name) else {
        return response(
            StatusCode::NOT_FOUND,
            "{\"error\":\"monitor_asset_not_found\"}",
        );
    };
    let mut reply = response(
        StatusCode::OK,
        if request.method() == Method::HEAD {
            Bytes::new()
        } else {
            Bytes::from_static(asset.bytes)
        },
    );
    reply.headers_mut().insert(
        hyper::header::CONTENT_TYPE,
        hyper::header::HeaderValue::from_static(asset.content_type),
    );
    reply
}

fn json_error(status: StatusCode, error: impl std::fmt::Display) -> HttpReply {
    response(
        status,
        serde_json::json!({"error": error.to_string()}).to_string(),
    )
}

fn cluster_query(raw: Option<&str>) -> anyhow::Result<(celld::monitor::Query, Option<String>)> {
    anyhow::ensure!(raw.unwrap_or("").len() <= 4096, "Cluster query too long");
    let mut node = None;
    let mut params = url::form_urlencoded::Serializer::new(String::new());
    for (key, value) in url::form_urlencoded::parse(raw.unwrap_or("").as_bytes()) {
        if key == "node" {
            anyhow::ensure!(value.len() <= 256, "Node ID too long");
            node = (!value.is_empty()).then(|| value.into_owned());
        } else {
            params.append_pair(&key, &value);
        }
    }
    Ok((celld::monitor::Query::parse(Some(&params.finish()))?, node))
}

fn json_reply(method: &Method, value: &impl serde::Serialize) -> HttpReply {
    response(
        StatusCode::OK,
        if method == Method::HEAD {
            Vec::new()
        } else {
            serde_json::to_vec(value).expect("serializable monitor response")
        },
    )
}

/// Peer reads use HMAC authentication, not the browser's origin exception.
pub(super) async fn peer(request: Request<Incoming>, app: AppHandle) -> HttpReply {
    let (parts, body) = request.into_parts();
    let body = match super::collect_limited_body(body, super::MAX_PEER_CONTROL_BODY_BYTES).await {
        Ok(body) => body,
        Err(error) => return super::peer_response(super::body_read_error("monitor", error)),
    };
    let path = parts
        .uri
        .path_and_query()
        .map_or_else(|| parts.uri.path().to_string(), ToString::to_string);
    if let Err(error) = app.peer_auth.verify(
        &parts.method,
        &path,
        &parts.headers,
        &body,
        app.peer_auth.source(),
    ) {
        return super::peer_response(response(error.status(), error.message()));
    }
    let reply = if parts.method != Method::GET || !body.is_empty() {
        response(
            StatusCode::METHOD_NOT_ALLOWED,
            "GET without a body required",
        )
    } else if !celld::monitor::enabled() {
        response(StatusCode::NOT_FOUND, "Monitor disabled")
    } else {
        match (
            app.runtime.as_ref(),
            celld::monitor::Query::parse(parts.uri.query()),
        ) {
            (Some(runtime), Ok(query)) => {
                json_reply(&parts.method, &runtime.monitor_snapshot(&query))
            }
            (_, Err(error)) => json_error(StatusCode::BAD_REQUEST, error),
            (None, _) => response(StatusCode::SERVICE_UNAVAILABLE, "Runtime unavailable"),
        }
    };
    let mut reply = super::peer_response(reply);
    reply.headers_mut().insert(
        hyper::header::CACHE_CONTROL,
        hyper::header::HeaderValue::from_static("no-store"),
    );
    reply
}

fn allowed_origin(headers: &HeaderMap, path: &str) -> bool {
    // Opening a bookmark/link must work even when Chrome reports cross-site
    // navigation. The static shell contains no measurements. Data and assets
    // retain the same-origin guard; CSP separately prevents iframe embedding.
    same_origin(headers)
        || (matches!(path, "/monitor" | "/monitor/" | "/monitor/index.html")
            && headers
                .get("sec-fetch-mode")
                .is_some_and(|mode| mode == "navigate")
            && headers
                .get("sec-fetch-dest")
                .is_some_and(|dest| dest == "document"))
}

fn same_origin(headers: &HeaderMap) -> bool {
    if headers
        .get("sec-fetch-site")
        .is_some_and(|value| !matches!(value.to_str(), Ok("same-origin" | "none")))
    {
        return false;
    }
    let Some(origin) = headers.get(hyper::header::ORIGIN) else {
        return true;
    };
    let Some(host) = headers
        .get(hyper::header::HOST)
        .and_then(|host| host.to_str().ok())
    else {
        return false;
    };
    let Ok(origin) = origin
        .to_str()
        .ok()
        .and_then(|origin| url::Url::parse(origin).ok())
        .ok_or(())
    else {
        return false;
    };
    if !matches!(origin.scheme(), "http" | "https") {
        return false;
    }
    // TLS may terminate at the private authenticated operator proxy. Compare
    // the original Host authority, not untrusted X-Forwarded-* headers.
    let Ok(host) = url::Url::parse(&format!("{}://{host}", origin.scheme())) else {
        return false;
    };
    origin.host() == host.host()
        && origin.port_or_known_default() == host.port_or_known_default()
        && origin.username().is_empty()
        && origin.password().is_none()
        && host.username().is_empty()
        && host.password().is_none()
        && host.path() == "/"
        && host.query().is_none()
        && host.fragment().is_none()
}

fn local_host(headers: &HeaderMap) -> bool {
    let Some(host) = headers
        .get(hyper::header::HOST)
        .and_then(|host| host.to_str().ok())
    else {
        return false;
    };
    let Ok(host) = url::Url::parse(&format!("http://{host}")) else {
        return false;
    };
    match host.host() {
        Some(url::Host::Domain("localhost")) => true,
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_node_filter_does_not_accept_a_fetch_url() {
        let (query, node) = cluster_query(Some("node=node-a&q=Counter&limit=10")).unwrap();
        assert_eq!(node.as_deref(), Some("node-a"));
        assert_eq!(query.search, "counter");
        assert!(cluster_query(Some("url=http://localhost")).is_err());
    }

    #[test]
    fn browser_cross_origin_requests_are_refused() {
        let mut headers = HeaderMap::new();
        headers.insert("host", "127.0.0.1:9877".parse().unwrap());
        headers.insert("origin", "http://127.0.0.1:9877".parse().unwrap());
        assert!(same_origin(&headers));
        headers.insert("origin", "http://attacker.example".parse().unwrap());
        assert!(!same_origin(&headers));
        headers.remove("origin");
        headers.insert("sec-fetch-site", "cross-site".parse().unwrap());
        assert!(!same_origin(&headers));
        headers.insert("sec-fetch-site", "same-site".parse().unwrap());
        assert!(!same_origin(&headers));
    }

    #[test]
    fn dev_hosts_do_not_allow_dns_rebinding() {
        let mut headers = HeaderMap::new();
        for host in ["127.0.0.1:9877", "[::1]:9877", "localhost:9877"] {
            headers.insert("host", host.parse().unwrap());
            assert!(local_host(&headers));
        }
        headers.insert("host", "attacker.example:9877".parse().unwrap());
        assert!(!local_host(&headers));
    }

    #[test]
    fn opening_the_dashboard_from_a_link_is_allowed_but_not_api_reads() {
        let mut headers = HeaderMap::new();
        headers.insert("host", "127.0.0.1:9877".parse().unwrap());
        headers.insert("sec-fetch-site", "cross-site".parse().unwrap());
        headers.insert("sec-fetch-mode", "navigate".parse().unwrap());
        headers.insert("sec-fetch-dest", "document".parse().unwrap());
        assert!(allowed_origin(&headers, "/monitor/"));
        assert!(!allowed_origin(&headers, "/monitor/api"));
        headers.insert("sec-fetch-dest", "iframe".parse().unwrap());
        assert!(!allowed_origin(&headers, "/monitor/"));
    }

    #[test]
    fn tls_operator_proxies_must_preserve_the_same_host_authority() {
        let mut headers = HeaderMap::new();
        headers.insert("host", "monitor.example:9443".parse().unwrap());
        headers.insert("origin", "https://monitor.example:9443".parse().unwrap());
        headers.insert("sec-fetch-site", "same-origin".parse().unwrap());
        assert!(same_origin(&headers));
        headers.insert("origin", "https://monitor.example:9444".parse().unwrap());
        assert!(!same_origin(&headers));
        headers.insert("origin", "https://attacker.invalid".parse().unwrap());
        headers.insert("x-forwarded-host", "attacker.invalid".parse().unwrap());
        assert!(!same_origin(&headers));
    }
}
