//! Exercise the residency boundary through a real node and shared cell isolate.
//! A released cell's detached I/O must stop without stopping a neighboring cell
//! or losing mutations acknowledged before release.
// This subprocess fixture is outside the Actor execution boundary: its files
// and wall-clock waits belong to the test runner, not injected node storage.
#![allow(clippy::disallowed_methods)]
use std::fs;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use serde_json::{json, Value};

const WORKER: &str = r#"
import { DurableObject } from 'cloudflare:workers';
const ticks = new Map();
export class Proof extends DurableObject {
  id = crypto.randomUUID();
  retained;
  fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === '/start' && !this.retained) {
      this.retained = Array.from({length: 4096}, () => Array(64).fill(1));
      const key = this.ctx.id.name + ':' + this.id;
      ticks.set(key, 0);
      setInterval(() => {
        this.retained[0][0]++;
        ticks.set(key, ticks.get(key) + 1);
      }, 25);
      if (url.searchParams.get('background') === 'waitUntil')
        this.ctx.waitUntil(new Promise(() => {}));
      this.ctx.storage.sql.exec('CREATE TABLE IF NOT EXISTS generations (id TEXT PRIMARY KEY NOT NULL)');
      this.ctx.storage.sql.exec('INSERT INTO generations (id) VALUES (?)', this.id);
    }
    const durable = this.retained
      ? this.ctx.storage.sql.exec('SELECT count(*) AS count FROM generations').one().count
      : null;
    return Response.json({id: this.id, scope: 'Proof:' + this.ctx.id.toString(), durable, ticks: Object.fromEntries(ticks)});
  }
}
export default {fetch(request, env) {
  return env.PROOF.getByName(new URL(request.url).searchParams.get('cell') ?? 'observer').fetch(request);
}};
"#;

struct Node {
    child: Child,
    _directory: tempfile::TempDir,
    log: PathBuf,
    origin: String,
    internal: String,
    client: reqwest::Client,
}

impl Node {
    async fn start() -> Self {
        Self::start_with_idle("300").await
    }

    async fn start_with_idle(idle_seconds: &str) -> Self {
        let directory = tempfile::tempdir().unwrap();
        fs::write(directory.path().join("worker.js"), WORKER).unwrap();
        fs::write(
            directory.path().join("wrangler.json"),
            json!({
                "name": "idle-release-probe",
                "main": "worker.js",
                "no_bundle": true,
                "compatibility_date": "2026-01-01",
                "durable_objects": {"bindings": [{"name": "PROOF", "class_name": "Proof"}]},
                "migrations": [{"tag": "v1", "new_sqlite_classes": ["Proof"]}]
            })
            .to_string(),
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let log = directory.path().join("node.log");
        let output = fs::File::create(&log).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_celld"))
            .args([
                "dev",
                directory.path().to_str().unwrap(),
                "--port",
                &port.to_string(),
                "--no-watch",
                "--logs",
            ])
            .env("CELLD_IDLE_EVICT_S", idle_seconds)
            .env("CELLD_V8_HEAP_LIMIT_MB", "64")
            .env("NO_COLOR", "1")
            .stdout(Stdio::from(output.try_clone().unwrap()))
            .stderr(Stdio::from(output))
            .spawn()
            .unwrap();
        let mut node = Self {
            child,
            _directory: directory,
            log,
            origin: format!("http://localhost:{port}"),
            internal: String::new(),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(3))
                .build()
                .unwrap(),
        };
        for _ in 0..300 {
            let text = fs::read_to_string(&node.log).unwrap();
            if let Some(address) = text.lines().find_map(|line| {
                line.split_once("celld internal listening on ")
                    .and_then(|(_, address)| address.split_whitespace().next())
            }) {
                node.internal = format!("http://{address}");
                if node
                    .client
                    .get(format!("{}/metrics", node.origin))
                    .send()
                    .await
                    .is_ok_and(|response| response.status().is_success())
                {
                    return node;
                }
            }
            assert!(node.child.try_wait().unwrap().is_none(), "{text}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!(
            "node did not start: {}",
            fs::read_to_string(&node.log).unwrap()
        );
    }

    async fn read(&self, path: &str) -> Value {
        self.client
            .get(format!("{}{path}", self.origin))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    async fn release(&self, scope: &str) {
        let response = self
            .client
            .post(format!("{}/evict/{scope}", self.internal))
            .send()
            .await
            .unwrap();
        let status = response.status();
        let body = response.text().await.unwrap();
        assert!(status.is_success(), "release {scope}: {status} {body}");
    }

    async fn retained_requests(&self) -> u64 {
        let state: Value = self
            .client
            .get(format!("{}/state", self.internal))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        state["deployment"]["isolates"]["cells"]["idle-release-probe"]["requests"]
            .as_u64()
            .unwrap()
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        #[cfg(unix)]
        unsafe {
            libc::kill(self.child.id() as libc::pid_t, libc::SIGINT);
        }
        for _ in 0..100 {
            if self.child.try_wait().unwrap().is_some() {
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_retires_only_old_cell_work_and_reactivation_preserves_commits() {
    let node = Node::start().await;
    let neighbor = node.read("/start?cell=neighbor").await;
    let neighbor_key = format!("neighbor:{}", neighbor["id"].as_str().unwrap());
    let mut frozen = Vec::<(String, Value)>::new();
    for (mode_index, background) in ["interval", "waitUntil"].into_iter().enumerate() {
        for cycle in 0..5 {
            let active = node
                .read(&format!("/start?cell=target&background={background}"))
                .await;
            assert_eq!(active["durable"], json!(mode_index * 5 + cycle + 1));
            let key = format!("target:{}", active["id"].as_str().unwrap());
            tokio::time::sleep(Duration::from_millis(100)).await;
            let before = node.read("/metrics").await;
            assert!(before["ticks"][&key].as_u64().unwrap() > 0);
            node.release(active["scope"].as_str().unwrap()).await;
            let retired = node.read("/metrics").await;
            tokio::time::sleep(Duration::from_millis(150)).await;
            let after = node.read("/metrics").await;
            assert_eq!(
                after["ticks"][&key], retired["ticks"][&key],
                "released {background} generation continued executing"
            );
            assert!(
                after["ticks"][&neighbor_key].as_u64().unwrap()
                    > retired["ticks"][&neighbor_key].as_u64().unwrap(),
                "release interrupted neighboring cell work"
            );
            assert_eq!(node.retained_requests().await, 1);
            for (old_key, count) in &frozen {
                assert_eq!(&after["ticks"][old_key], count, "old generation revived");
            }
            frozen.push((key.clone(), after["ticks"][&key].clone()));
        }
    }
    node.release(neighbor["scope"].as_str().unwrap()).await;
    for _ in 0..20 {
        if node.retained_requests().await == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("native requests survived final residency release");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn natural_idle_eviction_retires_detached_work_before_reactivation() {
    let node = Node::start_with_idle("1").await;
    let active = node.read("/start?cell=target").await;
    let key = format!("target:{}", active["id"].as_str().unwrap());
    assert_eq!(active["durable"], json!(1));
    let mut retired = None;
    for _ in 0..300 {
        let metrics = node.read("/metrics").await;
        if node.retained_requests().await == 0 {
            retired = Some(metrics);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let retired = retired.expect("idle eviction left detached I/O alive");
    tokio::time::sleep(Duration::from_millis(150)).await;
    let after = node.read("/metrics").await;
    assert_eq!(after["ticks"][&key], retired["ticks"][&key]);
    let reactivated = node.read("/start?cell=target").await;
    assert_ne!(reactivated["id"], active["id"]);
    assert_eq!(reactivated["durable"], json!(2));
    node.release(reactivated["scope"].as_str().unwrap()).await;
}
