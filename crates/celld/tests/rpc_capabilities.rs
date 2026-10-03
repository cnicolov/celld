// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Full-binary regression for the Cloudflare OS capability/callback pattern.
//! The ingress Worker and cell runtime use distinct isolate pools.
#![cfg(unix)]
// This harness runs outside the node execution domain and controls a real
// subprocess, filesystem, and clock; production code keeps the boundary lint.
#![allow(clippy::disallowed_methods)]
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

struct Server(std::process::Child);
impl Drop for Server {
    fn drop(&mut self) {
        // Give the dev supervisor time to stop its node and local store.
        unsafe {
            libc::kill(self.0.id() as i32, libc::SIGTERM);
        }
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn capabilities_cross_worker_and_durable_object_isolates() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let project = tempfile::tempdir().unwrap();
    for name in ["index.js", "wrangler.jsonc"] {
        std::fs::copy(
            root.join("examples/rpc-capabilities").join(name),
            project.path().join(name),
        )
        .unwrap();
    }
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let mut server = Server(
        Command::new(env!("CARGO_BIN_EXE_celld"))
            .args([
                "dev",
                project.path().to_str().unwrap(),
                "--port",
                &port.to_string(),
                "--no-watch",
                "--logs",
            ])
            .env("CELLD_SHUTDOWN_TOTAL_MS", "1000")
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap();
    let base = format!("http://127.0.0.1:{port}");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(response) = client
            .get(format!("{base}/.well-known/celld/health"))
            .send()
            .await
        {
            if response.status().is_success() {
                break;
            }
        }
        assert!(
            server.0.try_wait().unwrap().is_none(),
            "celld exited during startup"
        );
        assert!(Instant::now() < deadline, "celld did not become ready");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    for (path, expected) in [
        ("scalar", "scalar RPC works"),
        ("tracing", "{\"sync\":42,\"asyncValue\":42,\"active\":\"active span works\",\"instance\":true,\"syncError\":true,\"asyncError\":true,\"isTraced\":false}"),
        ("capability", "returned RPC capability works"),
        ("property", "42"),
        ("callback", "callback works"),
        ("callback-dup", "duplicate callback works"),
        ("service", "hello, isolate"),
        ("callable", "callable works"),
        (
            "loaded",
            "[\"loaded target works\",\"loaded callback works\",\"returned RPC capability works\"]",
        ),
        ("reentrant", "returned RPC capability works"),
        ("forward", "returned RPC capability works"),
        ("invoke", "returned RPC capability works"),
        ("duplicate", "returned RPC capability works"),
        ("eager-dispose", "returned RPC capability works"),
        ("order", "[\"1\",\"1,2\",\"1,2,3\"]"),
        ("interleave", "interleaved calls work"),
        ("write", "\"durable capability write\""),
        ("tree", "42"),
        ("rollback", "{\"stubError\":\"DataCloneError\",\"streamError\":\"DataCloneError\",\"locked\":false,\"hello\":\"returned RPC capability works\"}"),
        ("aliases", "{\"same\":true,\"hello\":\"returned RPC capability works\"}"),
        (
            "stream",
            "{\"first\":{\"value\":\"stream works\",\"done\":false},\"last\":{\"done\":true}}",
        ),
    ] {
        let response = client.get(format!("{base}/{path}")).send().await.unwrap();
        let status = response.status();
        let body = response.text().await.unwrap();
        assert_eq!(status, 200, "{path}: {body}");
        assert_eq!(body, expected, "{path}");
    }
    for (path, name, message) in [
        ("error", "TypeError", "capability error"),
        ("disposed", "Error", "RPC stub used after being disposed."),
    ] {
        let response = client.get(format!("{base}/{path}")).send().await.unwrap();
        assert_eq!(response.status(), 500, "{path}");
        let error: serde_json::Value = response.json().await.unwrap();
        assert_eq!(error["name"], name, "{path}");
        assert_eq!(error["message"], message, "{path}");
    }
    // Disposals are asynchronous, but releasing all aliases must eventually
    // reach the origin exactly once for each Session and result disposer.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let count: u64 = client
            .get(format!("{base}/disposals"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if count == 16 {
            break;
        }
        assert!(
            count < 16 && Instant::now() < deadline,
            "expected 16 disposals, got {count}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
