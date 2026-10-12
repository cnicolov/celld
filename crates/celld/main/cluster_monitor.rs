// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Observational fan-out over the authenticated peer wire, never public ingress.
use super::AppHandle;
use celld::monitor::Query;
use celld_monitor::{ClusterNode, ClusterSnapshot, Snapshot, MAX_PAGE_SIZE, SCHEMA_VERSION};
use futures_util::{stream, StreamExt};
use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

const MAX_NODES: usize = 64;
const MAX_NODE_CELLS: usize = 1_000;
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const BUDGET: Duration = Duration::from_secs(2);

#[derive(Clone)]
struct Directory {
    sampled_unix_ms: Option<u64>,
    received: Instant,
    peers: BTreeMap<String, String>,
    error: Option<String>,
}

struct Cached {
    key: String,
    received: Instant,
    data: ClusterSnapshot,
}

pub(super) struct Service {
    directory: Mutex<Directory>,
    cache: tokio::sync::Mutex<Option<Cached>>,
    http: reqwest::Client,
}

pub(super) fn service() -> &'static Service {
    static SERVICE: OnceLock<Service> = OnceLock::new();
    SERVICE.get_or_init(|| Service {
        directory: Mutex::new(Directory {
            sampled_unix_ms: None,
            received: Instant::now(),
            peers: BTreeMap::new(),
            error: Some("Fleet discovery is waiting for its first sample".into()),
        }),
        cache: tokio::sync::Mutex::new(None),
        http: reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_millis(500))
            .timeout(BUDGET)
            .build()
            .expect("monitor HTTP client"),
    })
}

impl Service {
    pub fn update(&self, at: u64, peers: &[celld_logic::CapacityPeer]) {
        let mut directory = self.directory.lock().unwrap();
        *directory = Directory {
            sampled_unix_ms: Some(at),
            received: Instant::now(),
            error: None,
            peers: peers
                .iter()
                .filter(|peer| peer.expires_ms > at)
                .map(|peer| (peer.node.clone(), peer.addr.clone()))
                .collect(),
        };
    }

    pub fn discovery_failed(&self) {
        // Keep the last inventory, but never call it complete/fresh.
        self.directory.lock().unwrap().error = Some(
            "The latest fleet membership sample failed; using the last known inventory".into(),
        );
    }

    pub fn standalone(&self) {
        let mut directory = self.directory.lock().unwrap();
        directory.sampled_unix_ms = None;
        directory.error = None;
    }

    pub async fn snapshot(
        &self,
        app: &AppHandle,
        query: &Query,
        node: Option<&str>,
    ) -> anyhow::Result<ClusterSnapshot> {
        let directory = self.directory.lock().unwrap().clone();
        let key = format!(
            "{:?}|{:?}|{:?}|{:?}",
            query.search, query.class, node, directory.peers
        );
        let mut cache = tokio::time::timeout(Duration::from_secs(3), self.cache.lock()).await?;
        let fresh = cache.as_ref().is_some_and(|cached| {
            cached.key == key && cached.received.elapsed() < Duration::from_secs(2)
        });
        if !fresh {
            let data = self.collect(app, query, node, &directory).await;
            *cache = Some(Cached {
                key,
                received: Instant::now(),
                data,
            });
        }
        let mut data = cache.as_ref().unwrap().data.clone();
        data.discovery_age_ms = directory
            .sampled_unix_ms
            .map(|at| celld::ownership_store::now_ms().saturating_sub(at));
        data.discovery_error = directory.error.clone().or_else(|| {
            (directory.sampled_unix_ms.is_some()
                && directory.received.elapsed() > Duration::from_secs(30))
            .then(|| "Fleet membership sample is stale".into())
        });
        data.partial |= data.discovery_error.is_some();
        data.offset = query.offset;
        let mut cursor = 0usize;
        for member in &mut data.nodes {
            if let Some(snapshot) = &mut member.snapshot {
                let start = cursor;
                let available = snapshot.cells.len();
                snapshot.cells.retain(|_| {
                    let keep =
                        cursor >= query.offset && cursor.saturating_sub(query.offset) < query.limit;
                    cursor += 1;
                    keep
                });
                snapshot.offset = query.offset.saturating_sub(start).min(available);
            }
        }
        Ok(data)
    }

    async fn collect(
        &self,
        app: &AppHandle,
        query: &Query,
        node: Option<&str>,
        directory: &Directory,
    ) -> ClusterSnapshot {
        let mut peers = directory.peers.clone();
        peers.insert(app.peer_auth.source().into(), app.advertise.clone());
        let known_nodes = peers.len();
        let limited = peers.len() > MAX_NODES;
        let targets = peers
            .into_iter()
            .filter(|(id, _)| node.is_none_or(|node| node == id))
            .take(MAX_NODES)
            .collect::<Vec<_>>();
        let mut members: BTreeMap<_, _> = targets
            .iter()
            .map(|(id, address)| {
                (
                    id.clone(),
                    ClusterNode {
                        id: id.clone(),
                        address: address.clone(),
                        error: Some("Node did not answer before the cluster query deadline".into()),
                        truncated: false,
                        snapshot: None,
                    },
                )
            })
            .collect();
        let work = stream::iter(targets.into_iter().map(|(id, address)| async move {
            let result = self.node_snapshot(app, &id, &address, query).await;
            (id, result)
        }))
        .buffer_unordered(8);
        tokio::pin!(work);
        let deadline = tokio::time::Instant::now() + BUDGET;
        while let Ok(Some((id, result))) = tokio::time::timeout_at(deadline, work.next()).await {
            let member = members.get_mut(&id).unwrap();
            match result {
                Ok(snapshot) => {
                    member.truncated = snapshot.cells.len() < snapshot.total_matches;
                    member.error = None;
                    member.snapshot = Some(snapshot);
                }
                Err(error) => member.error = Some(error.to_string()),
            }
        }
        let nodes = members.into_values().collect::<Vec<_>>();
        ClusterSnapshot {
            schema_version: SCHEMA_VERSION,
            coordinator: app.peer_auth.source().into(),
            sampled_unix_ms: celld::ownership_store::now_ms(),
            discovery_age_ms: None,
            discovery_error: directory.error.clone(),
            known_nodes,
            partial: limited
                || nodes
                    .iter()
                    .any(|node| node.error.is_some() || node.truncated),
            total_matches: nodes
                .iter()
                .filter_map(|node| node.snapshot.as_ref())
                .map(|snapshot| snapshot.total_matches)
                .sum(),
            collected_matches: nodes
                .iter()
                .filter_map(|node| node.snapshot.as_ref())
                .map(|snapshot| snapshot.cells.len())
                .sum(),
            offset: 0,
            nodes,
        }
    }

    async fn node_snapshot(
        &self,
        app: &AppHandle,
        id: &str,
        address: &str,
        query: &Query,
    ) -> anyhow::Result<Snapshot> {
        let mut cells = Vec::new();
        let mut process = None;
        let mut last;
        loop {
            let page = Query {
                search: query.search.clone(),
                class: query.class.clone(),
                offset: cells.len(),
                limit: MAX_PAGE_SIZE,
            };
            last = if id == app.peer_auth.source() {
                app.runtime
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("Node has no Worker runtime"))?
                    .monitor_snapshot(&page)
            } else {
                fetch_node(&self.http, app, id, address, &page).await?
            };
            anyhow::ensure!(
                last.schema_version == SCHEMA_VERSION
                    && last.node.id == id
                    && last.offset == page.offset,
                "Node identity, page or monitor version mismatch"
            );
            if let Some(process) = &process {
                anyhow::ensure!(
                    *process == last.process,
                    "Node restarted during paginated collection; retry"
                );
            } else {
                process = Some(last.process.clone());
            }
            anyhow::ensure!(
                last.cells.len() <= MAX_PAGE_SIZE,
                "Node exceeded its requested page size"
            );
            if last.cells.is_empty() {
                break;
            }
            cells.append(&mut last.cells);
            if cells.len() >= last.total_matches || cells.len() >= MAX_NODE_CELLS {
                break;
            }
        }
        last.cells = cells;
        last.offset = 0;
        Ok(last)
    }
}

fn peer_url(address: &str, path: &str) -> anyhow::Result<url::Url> {
    let base = url::Url::parse(&format!("http://{address}"))?;
    anyhow::ensure!(
        base.host_str().is_some()
            && base.username().is_empty()
            && base.password().is_none()
            && base.path() == "/"
            && base.query().is_none()
            && base.fragment().is_none(),
        "Invalid advertised peer authority"
    );
    Ok(base.join(path)?)
}

async fn fetch_node(
    http: &reqwest::Client,
    app: &AppHandle,
    node: &str,
    address: &str,
    query: &Query,
) -> anyhow::Result<Snapshot> {
    let params = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("q", &query.search)
        .append_pair("class", query.class.as_deref().unwrap_or(""))
        .append_pair("offset", &query.offset.to_string())
        .append_pair("limit", &query.limit.to_string())
        .finish();
    let path = format!("/peer/monitor?{params}");
    let request =
        app.peer_auth
            .sign(http.get(peer_url(address, &path)?), "GET", &path, &[], node)?;
    let mut response = request.send().await?;
    celld::peer_auth::validate_response(response.headers())?;
    anyhow::ensure!(
        response.status().is_success(),
        "Node monitoring returned HTTP {} (monitor may be disabled)",
        response.status()
    );
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        anyhow::ensure!(
            body.len().saturating_add(chunk.len()) <= MAX_RESPONSE_BYTES,
            "Node monitoring response exceeds 8 MiB"
        );
        body.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&body)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn peers_are_only_advertised_authorities_not_user_supplied_urls() {
        assert!(peer_url("127.0.0.1:1234", "/peer/monitor").is_ok());
        for invalid in [
            "user@host",
            "host/path",
            "host?x=1",
            "host#x",
            "http://host",
        ] {
            assert!(peer_url(invalid, "/peer/monitor").is_err(), "{invalid}");
        }
    }
}
