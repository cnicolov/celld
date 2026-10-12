// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Fork regressions exercise the production disk, CAS, and recovery adapters.
//! They do not require the external upstream simulation harness.

use super::*;
use std::sync::OnceLock;

fn run(future: impl std::future::Future<Output = anyhow::Result<()>>) {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    let runtime = RUNTIME.get_or_init(|| {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        crate::asyncrt::set_host_handle(runtime.handle().clone());
        runtime
    });
    runtime.block_on(future).unwrap();
}

async fn lease(bucket: &Bucket, node: &str, generation: &str, log: serde_json::Value) {
    bucket
        .put(
            &lease_key(node),
            serde_json::to_vec(&serde_json::json!({
                "node": node,
                "expires_ms": 0,
                "addr": "127.0.0.1:1",
                "ownership_index_generation": generation,
                "log": log,
            }))
            .unwrap(),
        )
        .await
        .unwrap();
}

struct UnavailablePeer;

fn sqlite_entries(root: &Path, cell: &str) -> anyhow::Result<Vec<Entry>> {
    let path = root.join("producer.sqlite");
    let mut capture = celld_ltx::Db::open(&path)?;
    let connection = rusqlite::Connection::open(&path)?;
    connection.execute_batch("CREATE TABLE data (id INTEGER PRIMARY KEY, value BLOB)")?;
    let mut entries = Vec::new();
    for (index, byte) in [17_u8, 43].into_iter().enumerate() {
        connection.execute(
            "INSERT OR REPLACE INTO data VALUES (1, ?1)",
            [vec![byte; (index + 1) * 16384]],
        )?;
        capture.sync()?;
        let position = capture.pos()?;
        entries.push(Entry {
            seq: index as u64 + 1,
            cell: cell.to_string(),
            cell_epoch: 1,
            txid: position.txid.0,
            bytes: capture.read_ltx_file(0, position.txid, position.txid)?,
        });
    }
    capture.close()?;
    Ok(entries)
}

async fn wait_hot(follower: &FollowerStore) -> anyhow::Result<()> {
    for _ in 0..500 {
        if follower.hot_recovery_status()["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["ready"] == true)
        {
            return Ok(());
        }
        crate::asyncrt::sleep(std::time::Duration::from_millis(10)).await;
    }
    anyhow::bail!(
        "standby checkpoints did not catch up: {}",
        follower.hot_recovery_status()
    );
}

#[test]
fn hot_recovery_certifies_only_the_checkpointed_cell_and_promotes_after_the_claim() {
    run(async {
        let directory = tempfile::tempdir()?;
        let bucket = Arc::new(Bucket::open_dev(&directory.path().join("bucket.sqlite"))?);
        let manager = manager_for(directory.path(), bucket.clone(), Arc::new(UnavailablePeer))?;
        let follower = Arc::new(FollowerStore::new(
            directory.path(),
            Some(bucket.clone()),
            "recoverer",
        ));
        manager.enable_hot_recovery(follower.clone(), directory.path(), 4 * 1024 * 1024, 4)?;
        lease(
            &bucket,
            "leader",
            "old",
            serde_json::json!({
                "epoch": 1, "ensemble": ["recoverer"], "tiered": 0, "state": "open", "active": true,
            }),
        )
        .await;
        bucket
            .put(
                "cells/Counter:hot/own.json",
                serde_json::to_vec(&serde_json::json!({ "node": "leader", "epoch": 1 }))?,
            )
            .await?;
        follower.persist(
            "leader/old",
            FollowerState {
                fragment_epoch: 1,
                ..FollowerState::default()
            },
        )?;
        let entries = sqlite_entries(directory.path(), "Counter:hot")?;
        assert!(
            follower
                .append(AppendReq {
                    leader: "leader/old".into(),
                    epoch: 1,
                    truncate_to: 0,
                    entries
                })
                .await
                .ok
        );
        wait_hot(&follower).await?;
        let (wire, _) = bucket.get(&lease_key("leader")).await?.unwrap();
        let mut live: serde_json::Value = serde_json::from_slice(&wire)?;
        live["expires_ms"] = serde_json::json!(crate::ownership_store::now_ms() + 60_000);
        bucket
            .put(&lease_key("leader"), serde_json::to_vec(&live)?)
            .await?;
        assert!(
            !manager
                .prepare_hot_recovery("Counter:hot", "leader")
                .await?,
            "a warm copy cannot fence a live lease"
        );
        live["expires_ms"] = serde_json::json!(0);
        bucket
            .put(&lease_key("leader"), serde_json::to_vec(&live)?)
            .await?;
        assert!(
            manager
                .prepare_hot_recovery("Counter:hot", "leader")
                .await?
        );
        assert_eq!(
            read_record(&bucket, "leader").await?.unwrap().record.state,
            LogState::Recovering,
            "a cell proof must not certify the entire predecessor session"
        );
        assert!(!follower_fragment_closed(&bucket, "leader/old", 1).await?);
        assert!(
            crate::asyncrt::fs()
                .metadata(
                    &directory
                        .path()
                        .join("recoverer/Counter:hot/ltx/e2/db.sqlite")
                )
                .is_err(),
            "preparing the interlock is not permission to open a writable successor"
        );
        // Represent the ownership CAS won by the serial actor.
        bucket
            .put(
                "cells/Counter:hot/own.json",
                serde_json::to_vec(&serde_json::json!({ "node": "recoverer", "epoch": 2 }))?,
            )
            .await?;
        let activation = manager
            .ltx
            .activate(crate::replication::ActivationOptions {
                cell: "Counter:hot",
                epoch: 2,
                fresh: false,
                took_over: true,
                resume_local: false,
                prior: Some("leader".into()),
            })
            .await?;
        assert!(activation.restored);
        assert!(
            activation.vfs.is_none(),
            "promotion should be a materialized database, not a sparse cold restore"
        );
        let connection = rusqlite::Connection::open(&activation.path)?;
        let value: Vec<u8> =
            connection.query_row("SELECT value FROM data WHERE id = 1", [], |row| row.get(0))?;
        assert_eq!(value, vec![43; 32768]);
        assert_eq!(
            connection.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))?,
            "ok"
        );
        assert!(
            !bucket
                .list("cells/Counter:hot/ltx/e2/0000/")
                .await?
                .is_empty(),
            "the successor marker must be durable before serving"
        );
        manager.task_stop.request_stop();
        manager.child_tasks.join().await;
        Ok(())
    });
}

#[test]
fn a_bucket_only_commit_ahead_of_the_standby_rejects_hot_recovery() {
    run(async {
        let directory = tempfile::tempdir()?;
        let bucket = Arc::new(Bucket::open_dev(&directory.path().join("bucket.sqlite"))?);
        let manager = manager_for(directory.path(), bucket.clone(), Arc::new(UnavailablePeer))?;
        let follower = Arc::new(FollowerStore::new(
            directory.path(),
            Some(bucket.clone()),
            "recoverer",
        ));
        manager.enable_hot_recovery(follower.clone(), directory.path(), 4 * 1024 * 1024, 4)?;
        lease(
            &bucket,
            "leader",
            "old",
            serde_json::json!({
                "epoch": 1, "ensemble": ["recoverer"], "tiered": 0, "state": "open", "active": true,
            }),
        )
        .await;
        bucket
            .put(
                "cells/Counter:hot/own.json",
                serde_json::to_vec(&serde_json::json!({ "node": "leader", "epoch": 1 }))?,
            )
            .await?;
        follower.persist(
            "leader/old",
            FollowerState {
                fragment_epoch: 1,
                ..FollowerState::default()
            },
        )?;
        let mut entries = sqlite_entries(directory.path(), "Counter:hot")?;
        let newer = entries.pop().unwrap();
        assert!(
            follower
                .append(AppendReq {
                    leader: "leader/old".into(),
                    epoch: 1,
                    truncate_to: 0,
                    entries
                })
                .await
                .ok
        );
        wait_hot(&follower).await?;
        manager
            .ltx
            .upload_raw_l0("Counter:hot", 1, newer.txid, newer.txid, &newer.bytes)
            .await?;
        assert!(
            !manager
                .prepare_hot_recovery("Counter:hot", "leader")
                .await?,
            "a healthy follower image cannot omit a later acknowledged bucket-only write"
        );
        assert_eq!(
            read_record(&bucket, "leader").await?.unwrap().record.state,
            LogState::Recovering
        );
        manager.task_stop.request_stop();
        manager.child_tasks.join().await;
        Ok(())
    });
}

#[test]
fn hot_recovery_streams_a_canonical_seed_when_the_first_follower_entry_is_a_delta() {
    run(async {
        let directory = tempfile::tempdir()?;
        let bucket = Arc::new(Bucket::open_dev(&directory.path().join("bucket.sqlite"))?);
        let manager = manager_for(directory.path(), bucket.clone(), Arc::new(UnavailablePeer))?;
        let follower = Arc::new(FollowerStore::new(
            directory.path(),
            Some(bucket.clone()),
            "recoverer",
        ));
        manager.enable_hot_recovery(follower.clone(), directory.path(), 4 * 1024 * 1024, 4)?;
        lease(
            &bucket,
            "leader",
            "old",
            serde_json::json!({
                "epoch": 1, "ensemble": ["recoverer"], "tiered": 1, "state": "open", "active": true,
            }),
        )
        .await;
        bucket
            .put(
                "cells/Counter:seed/own.json",
                serde_json::to_vec(&serde_json::json!({ "node": "leader", "epoch": 1 }))?,
            )
            .await?;
        let mut entries = sqlite_entries(directory.path(), "Counter:seed")?;
        let delta = entries.pop().unwrap();
        let snapshot = entries.pop().unwrap();
        assert!(!celld_ltx::ltx::Header::parse(&delta.bytes)?.is_snapshot());
        manager
            .ltx
            .upload_raw_l0(
                "Counter:seed",
                1,
                snapshot.txid,
                snapshot.txid,
                &snapshot.bytes,
            )
            .await?;
        follower.persist(
            "leader/old",
            FollowerState {
                fragment_epoch: 1,
                base: 1,
                end: 1,
                ..Default::default()
            },
        )?;
        assert!(
            follower
                .append(AppendReq {
                    leader: "leader/old".into(),
                    epoch: 1,
                    truncate_to: 1,
                    entries: vec![delta]
                })
                .await
                .ok
        );
        wait_hot(&follower).await?;
        assert!(
            manager
                .prepare_hot_recovery("Counter:seed", "leader")
                .await?
        );
        let promotion = manager.ltx.hot_promotions_for_test("Counter:seed");
        promotion.verify()?;
        let connection = rusqlite::Connection::open_with_flags(
            &promotion.path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        let value: Vec<u8> =
            connection.query_row("SELECT value FROM data WHERE id = 1", [], |row| row.get(0))?;
        assert_eq!(value, vec![43; 32768]);
        manager.task_stop.request_stop();
        manager.child_tasks.join().await;
        Ok(())
    });
}

#[test]
fn hot_recovery_retained_bundle_footers_reject_newer_or_unreadable_rows() {
    run(async {
        for corrupt in [false, true] {
            let directory = tempfile::tempdir()?;
            let bucket = Arc::new(Bucket::open_dev(&directory.path().join("bucket.sqlite"))?);
            let manager = manager_for(directory.path(), bucket.clone(), Arc::new(UnavailablePeer))?;
            let follower = Arc::new(FollowerStore::new(
                directory.path(),
                Some(bucket.clone()),
                "recoverer",
            ));
            manager.enable_hot_recovery(follower.clone(), directory.path(), 4 * 1024 * 1024, 4)?;
            lease(&bucket, "leader", "old", serde_json::json!({
                "epoch": 1, "ensemble": ["recoverer"], "tiered": 0, "state": "open", "active": true,
            })).await;
            bucket
                .put(
                    "cells/Counter:bundle/own.json",
                    serde_json::to_vec(&serde_json::json!({ "node": "leader", "epoch": 1 }))?,
                )
                .await?;
            let mut entries = sqlite_entries(directory.path(), "Counter:bundle")?;
            let newer = entries.pop().unwrap();
            follower.persist(
                "leader/old",
                FollowerState {
                    fragment_epoch: 1,
                    ..Default::default()
                },
            )?;
            assert!(
                follower
                    .append(AppendReq {
                        leader: "leader/old".into(),
                        epoch: 1,
                        truncate_to: 0,
                        entries
                    })
                    .await
                    .ok
            );
            wait_hot(&follower).await?;
            let bytes = if corrupt {
                b"unreadable retained envelope".to_vec()
            } else {
                celld_ltx::bundle::encode(&[celld_ltx::bundle::BundleEntry {
                    cell: newer.cell,
                    cell_epoch: newer.cell_epoch,
                    txid: newer.txid,
                    bytes: newer.bytes,
                }])?
            };
            bucket.put("log/leader/old/bundle/e1-1.ltxb", bytes).await?;
            let prepared = manager
                .prepare_hot_recovery("Counter:bundle", "leader")
                .await;
            if corrupt {
                assert!(prepared.is_err());
            } else {
                assert!(!prepared?);
            }
            assert_eq!(
                read_record(&bucket, "leader").await?.unwrap().record.state,
                LogState::Recovering
            );
            assert!(!follower_fragment_closed(&bucket, "leader/old", 1).await?);
            assert_eq!(
                follower.hot_recovery_status()["prepared"],
                0,
                "a rejected certificate releases its disk reservation"
            );
            manager.task_stop.request_stop();
            manager.child_tasks.join().await;
        }
        Ok(())
    });
}

#[test]
fn hot_recovery_prepared_images_expire_after_a_lost_claim() {
    run(async {
        let directory = tempfile::tempdir()?;
        let bucket = Arc::new(Bucket::open_dev(&directory.path().join("bucket.sqlite"))?);
        let manager = manager_for(directory.path(), bucket, Arc::new(UnavailablePeer))?;
        let cache = Arc::new(crate::hot_recovery::HotStandbys::new(
            directory.path(),
            1024 * 1024,
            1,
        )?);
        let range = |end| FollowerState {
            fragment_epoch: 1,
            end,
            ..Default::default()
        };
        cache.observe(
            "leader/old",
            range(0),
            range(2),
            &sqlite_entries(directory.path(), "Counter:expiry")?,
        );
        while let Some(job) = cache.next_checkpoint() {
            cache.checkpointed(&job);
        }
        let mut promotion = cache
            .take("leader/old", "Counter:expiry", 1, range(2))
            .unwrap();
        let path = promotion.path.clone();
        promotion.expires_mono_ms = 0;
        manager
            .ltx
            .install_hot_promotion("Counter:expiry", promotion);
        manager.ltx.prune_hot_promotions();
        assert!(crate::asyncrt::fs().metadata(&path).is_err());
        assert_eq!(cache.status()["prepared"], 0);
        assert_eq!(cache.status()["bytes"], 0);
        Ok(())
    });
}

#[test]
fn hot_recovery_never_publishes_a_delta_past_a_missing_canonical_baseline() {
    run(async {
        let directory = tempfile::tempdir()?;
        let bucket = Arc::new(Bucket::open_dev(&directory.path().join("bucket.sqlite"))?);
        let manager = manager_for(directory.path(), bucket.clone(), Arc::new(UnavailablePeer))?;
        let follower = Arc::new(FollowerStore::new(
            directory.path(),
            Some(bucket.clone()),
            "recoverer",
        ));
        manager.enable_hot_recovery(follower.clone(), directory.path(), 4 * 1024 * 1024, 4)?;
        lease(
            &bucket,
            "leader",
            "old",
            serde_json::json!({
                "epoch": 1, "ensemble": ["recoverer"], "tiered": 1, "state": "open", "active": true,
            }),
        )
        .await;
        let mut entries = sqlite_entries(directory.path(), "Counter:missing-base")?;
        let delta = entries.pop().unwrap();
        let snapshot = entries.pop().unwrap();
        // The original base exists only in a retained bundle. Publishing t2
        // directly would make max-based bundle GC incorrectly discard t1.
        let bundle = celld_ltx::bundle::encode(&[celld_ltx::bundle::BundleEntry {
            cell: snapshot.cell,
            cell_epoch: snapshot.cell_epoch,
            txid: snapshot.txid,
            bytes: snapshot.bytes,
        }])?;
        bucket
            .put("log/leader/old/bundle/e1-1.ltxb", bundle)
            .await?;
        follower.persist(
            "leader/old",
            FollowerState {
                fragment_epoch: 1,
                base: 1,
                end: 1,
                ..Default::default()
            },
        )?;
        assert!(
            follower
                .append(AppendReq {
                    leader: "leader/old".into(),
                    epoch: 1,
                    truncate_to: 1,
                    entries: vec![delta]
                })
                .await
                .ok
        );
        for _ in 0..500 {
            if follower.hot_recovery_status()["candidates"]
                .as_array()
                .unwrap()
                .is_empty()
            {
                break;
            }
            crate::asyncrt::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(follower.hot_recovery_status()["candidates"]
            .as_array()
            .unwrap()
            .is_empty());
        assert!(
            bucket
                .list("cells/Counter:missing-base/ltx/")
                .await?
                .is_empty(),
            "no canonical coverage watermark may advance over t1"
        );
        assert!(bucket
            .get("log/leader/old/bundle/e1-1.ltxb")
            .await?
            .is_some());
        assert_eq!(
            follower.load("leader/old")?.end,
            2,
            "the advisory cache cannot revoke a durable follower acknowledgement"
        );
        assert_eq!(
            read_record(&bucket, "leader").await?.unwrap().record.state,
            LogState::Open
        );
        manager.task_stop.request_stop();
        manager.child_tasks.join().await;
        Ok(())
    });
}

fn manager_for(
    root: &Path,
    bucket: Arc<Bucket>,
    transport: Arc<dyn LogTransport>,
) -> anyhow::Result<NodeLogManager> {
    let ownership = Arc::new(crate::ownership_store::BucketOwnership::new(
        bucket.as_ref().clone(),
        bucket.as_ref().clone(),
        "recoverer".into(),
        "generation".into(),
    ));
    let ltx = Arc::new(crate::ltx_repl::LtxRepl::start(
        &root.join("recoverer"),
        crate::bucket::StorageBackend::Local,
        bucket.name.clone(),
        String::new(),
        None,
        "auto".into(),
        None,
    )?);
    Ok(NodeLogManager::new_with_log_transport(
        "recoverer/generation",
        bucket,
        Arc::new(OwnLog {
            ownership,
            nudge: Box::new(|| {}),
            write_lock: tokio::sync::Mutex::new(()),
        }),
        ltx,
        transport,
        eviction_policy_from_env()?,
    ))
}

#[test]
fn unreadable_retained_bundles_keep_the_fold_pending_without_declaring_loss() {
    run(async {
        let mut bad_row = celld_ltx::bundle::encode(&[celld_ltx::bundle::BundleEntry {
            cell: "Counter:test".into(),
            cell_epoch: 1,
            txid: 1,
            bytes: vec![42],
        }])?;
        let offset = 1 + 2 + "Counter:test".len() + 16;
        bad_row[offset..offset + 8].copy_from_slice(&u64::MAX.to_le_bytes());
        for bytes in [b"torn envelope".to_vec(), bad_row] {
            let directory = tempfile::tempdir()?;
            let bucket = Arc::new(Bucket::open_dev(&directory.path().join("bucket.sqlite"))?);
            let manager = manager_for(directory.path(), bucket.clone(), Arc::new(UnavailablePeer))?;
            let key = "log/recoverer/generation/bundle/e1-1.ltxb";
            bucket.put(key, bytes.clone()).await?;
            assert!(
                manager.uncovered_bundle_rows().await.is_err(),
                "corruption does not prove that every alternative copy is lost"
            );
            assert_eq!(bucket.get(key).await?.unwrap().0.as_ref(), bytes);
            assert!(!bucket
                .list("log/recoverer/")
                .await?
                .iter()
                .any(|meta| meta.location.as_ref().ends_with(".loss.json")));
        }
        Ok(())
    });
}

struct QuickPages;

struct DiskPeer {
    follower: Arc<FollowerStore>,
    cursors: Mutex<Vec<u64>>,
    failed_once: std::sync::atomic::AtomicBool,
}

impl LogTransport for DiskPeer {
    fn post<'a>(
        &'a self,
        _node: &'a str,
        _addr: &'a str,
        path: &'a str,
        body: Vec<u8>,
        _deadline: Option<std::time::Duration>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<bytes::Bytes>> + Send + 'a>,
    > {
        Box::pin(async move {
            match path {
                "/peer/log/seal" => Ok(serde_json::to_vec(
                    &self.follower.seal(&serde_json::from_slice(&body)?).await?,
                )?
                .into()),
                "/peer/log/tail-page" => {
                    let mut req: TailPageReq = serde_json::from_slice(&body)?;
                    let cursor = req.after.unwrap_or_default();
                    self.cursors.lock().unwrap().push(cursor);
                    if cursor == 1 && !self.failed_once.swap(true, Ordering::SeqCst) {
                        anyhow::bail!("injected transient failure on the second page");
                    }
                    // One entry per page makes bundle/tail ordering and
                    // same-cursor retries visible in a small real-disk test.
                    req.max_bytes = 1;
                    Ok(encode_tail_page(&self.follower.tail_page(&req).await?).into())
                }
                _ => anyhow::bail!("unexpected request: {path}"),
            }
        })
    }
}

#[test]
fn paged_recovery_retries_its_cursor_and_folds_bundles_before_the_newer_tail() {
    run(async {
        let directory = tempfile::tempdir()?;
        let bucket = Arc::new(Bucket::open_dev(&directory.path().join("bucket.sqlite"))?);
        let follower = Arc::new(FollowerStore::new(
            &directory.path().join("follower"),
            Some(bucket.clone()),
            "follower",
        ));
        follower.persist(
            "leader/old",
            FollowerState {
                fragment_epoch: 1,
                ..FollowerState::default()
            },
        )?;
        let mut prior = 0;
        let mut rows = Vec::new();
        for txid in 1..=6 {
            let page = vec![txid as u8; 4096];
            let post = celld_ltx::ltx::checksum_page(1, &page);
            let bytes = celld_ltx::ltx::encode_file(
                &celld_ltx::ltx::Header {
                    version: celld_ltx::ltx::VERSION,
                    page_size: 4096,
                    commit: 1,
                    min_txid: celld_ltx::TXID(txid),
                    max_txid: celld_ltx::TXID(txid),
                    pre_apply_checksum: prior,
                    ..Default::default()
                },
                &[(1, page)],
                post,
            )?;
            prior = post;
            rows.push(celld_ltx::bundle::BundleEntry {
                cell: "Counter:test".into(),
                cell_epoch: 1,
                txid,
                bytes,
            });
        }
        bucket
            .put(
                "log/leader/old/bundle/e1-1.ltxb",
                celld_ltx::bundle::encode(&rows[..2])?,
            )
            .await?;
        assert!(
            follower
                .append(AppendReq {
                    leader: "leader/old".into(),
                    epoch: 1,
                    truncate_to: 0,
                    entries: rows
                        .into_iter()
                        .skip(2)
                        .enumerate()
                        .map(|(index, row)| Entry {
                            seq: index as u64 + 1,
                            cell: row.cell,
                            cell_epoch: row.cell_epoch,
                            txid: row.txid,
                            bytes: row.bytes,
                        })
                        .collect(),
                })
                .await
                .ok
        );
        lease(&bucket, "follower", "generation", serde_json::Value::Null).await;
        lease(
            &bucket,
            "leader",
            "old",
            serde_json::json!({
                "epoch": 1, "ensemble": ["follower"], "tiered": 0, "state": "open", "active": true,
            }),
        )
        .await;
        let transport = Arc::new(DiskPeer {
            follower: follower.clone(),
            cursors: Mutex::new(Vec::new()),
            failed_once: std::sync::atomic::AtomicBool::new(false),
        });
        let manager = manager_for(directory.path(), bucket.clone(), transport.clone())?;
        manager.recover_as("leader/old", RecoverMode::Boot).await?;
        assert_eq!(*transport.cursors.lock().unwrap(), vec![0, 1, 1, 2, 3]);
        assert_eq!(
            read_record(&bucket, "leader/old")
                .await?
                .unwrap()
                .record
                .state,
            LogState::Sealed
        );
        let mut ranges = Vec::new();
        for meta in bucket.list("cells/Counter:test/ltx/e1/0000/").await? {
            let bytes = bucket.get(meta.location.as_ref()).await?.unwrap().0;
            let decoded = celld_ltx::ltx::decode_file(&bytes)?;
            ranges.push((decoded.header.min_txid.0, decoded.header.max_txid.0));
            assert_eq!(
                celld_ltx::ltx::decode_file_pages(&bytes)?[0].1,
                vec![decoded.header.max_txid.0 as u8; 4096]
            );
        }
        ranges.sort_unstable();
        let covered: Vec<_> = ranges
            .into_iter()
            .flat_map(|(first, last)| first..=last)
            .collect();
        assert_eq!(
            covered,
            vec![1, 2, 3, 4, 5, 6],
            "the tail watermark must not hide the earlier bundled rows"
        );
        follower.gc_fragments().await;
        assert!(follower
            .tail(&TailReq {
                leader: "leader/old".into()
            })
            .await?
            .entries
            .is_empty());
        Ok(())
    });
}

impl LogTransport for QuickPages {
    fn post<'a>(
        &'a self,
        _node: &'a str,
        _addr: &'a str,
        _path: &'a str,
        body: Vec<u8>,
        _deadline: Option<std::time::Duration>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<bytes::Bytes>> + Send + 'a>,
    > {
        Box::pin(async move {
            let req: TailPageReq = serde_json::from_slice(&body)?;
            let range = req.range.unwrap();
            let seq = req.after.unwrap() + 1;
            // Each request finishes before the 5s heartbeat wait, but the
            // three-page transfer spans a heartbeat deadline.
            crate::asyncrt::sleep(std::time::Duration::from_secs(3)).await;
            Ok(encode_tail_page(&TailResp {
                range,
                entries: vec![Entry {
                    seq,
                    cell: "Counter:test".into(),
                    cell_epoch: 1,
                    txid: seq,
                    bytes: vec![42],
                }],
            })
            .into())
        })
    }
}

#[test]
fn quick_pages_do_not_starve_the_recovery_claim_heartbeat() {
    run(async {
        let directory = tempfile::tempdir()?;
        let bucket = Arc::new(Bucket::open_dev(&directory.path().join("bucket.sqlite"))?);
        lease(
            &bucket,
            "leader",
            "old",
            serde_json::json!({
                "epoch": 1, "ensemble": ["follower"], "tiered": 0, "state": "recovering",
                "active": true, "claimant": "recoverer", "claimed_ms": 1,
            }),
        )
        .await;
        let ownership = Arc::new(crate::ownership_store::BucketOwnership::new(
            bucket.as_ref().clone(),
            bucket.as_ref().clone(),
            "recoverer".into(),
            "generation".into(),
        ));
        let ltx = Arc::new(crate::ltx_repl::LtxRepl::start(
            &directory.path().join("recoverer"),
            crate::bucket::StorageBackend::Local,
            bucket.name.clone(),
            String::new(),
            None,
            "auto".into(),
            None,
        )?);
        let manager = NodeLogManager::new_with_log_transport(
            "recoverer/generation",
            bucket.clone(),
            Arc::new(OwnLog {
                ownership,
                nudge: Box::new(|| {}),
                write_lock: tokio::sync::Mutex::new(()),
            }),
            ltx,
            Arc::new(QuickPages),
            eviction_policy_from_env()?,
        );
        let mut beat = ClaimBeat::new();
        let mut tail = manager
            .fetch_tail(
                "follower",
                "unused",
                "leader/old",
                Some(TailRange {
                    fragment_epoch: 1,
                    base: 0,
                    end: 3,
                    complete: true,
                }),
                Some(("leader/old", &mut beat)),
            )
            .await?;
        let record = read_record(&bucket, "leader/old").await?.unwrap();
        assert!(
            record.record.claimed_ms.unwrap() > 1,
            "successful quick pages must not reset the heartbeat wait forever"
        );
        for seq in 1..=3 {
            assert_eq!(tail.next()?.unwrap()[0].seq, seq);
        }
        assert!(tail.next()?.is_none());
        Ok(())
    });
}

#[test]
fn follower_gc_does_not_use_an_unidentified_lease_for_a_named_session() {
    run(async {
        let directory = tempfile::tempdir()?;
        let bucket = Arc::new(Bucket::open_dev(&directory.path().join("bucket.sqlite"))?);
        lease(
            &bucket,
            "leader",
            "",
            serde_json::json!({
                "epoch": 1, "ensemble": [], "tiered": 1, "state": "sealed", "active": false,
            }),
        )
        .await;
        assert!(
            !follower_fragment_closed(&bucket, "leader/old", 1).await?,
            "an untagged lease cannot prove which process session was closed"
        );
        Ok(())
    });
}

#[test]
fn missing_own_session_proof_cannot_authorize_disk_retirement() {
    run(async {
        let directory = tempfile::tempdir()?;
        crate::node_identity::load_or_create(directory.path(), Some("retiring"))?;
        let bucket = Arc::new(Bucket::open_dev(&directory.path().join("bucket.sqlite"))?);
        let result = check_disk_retirement(bucket.clone(), directory.path()).await?;
        assert!(
            !result.safe,
            "no lease record is not an own-session completion proof"
        );
        lease(
            &bucket,
            "retiring",
            "generation",
            serde_json::json!({
                "epoch": 1, "ensemble": [], "tiered": 0, "state": "sealed", "active": false,
            }),
        )
        .await;
        assert!(check_disk_retirement(bucket, directory.path()).await?.safe);
        Ok(())
    });
}

#[test]
fn an_empty_seal_response_pins_no_disk_but_cannot_certify_a_witness() {
    run(async {
        let directory = tempfile::tempdir()?;
        crate::node_identity::load_or_create(directory.path(), Some("follower"))?;
        let bucket = Arc::new(Bucket::open_dev(&directory.path().join("bucket.sqlite"))?);
        lease(&bucket, "follower", "generation", serde_json::Value::Null).await;
        let follower = FollowerStore::new(directory.path(), Some(bucket.clone()), "follower");
        let seal = follower
            .seal(&SealReq {
                leader: "leader/missing".into(),
                epoch: 1,
            })
            .await?;
        assert_eq!(seal.fragment_epoch, 0);
        assert!(
            check_disk_retirement(bucket.clone(), directory.path())
                .await?
                .safe
        );
        follower.filesystem.write(
            &follower.dir("leader/missing").join("1-1.batch"),
            b"unknown payload",
        )?;
        assert!(check_disk_retirement(bucket, directory.path())
            .await
            .is_err());
        Ok(())
    });
}

#[test]
#[allow(clippy::disallowed_methods)] // The production S3 client must classify an actual HTTP status.
fn s3_conflict_is_retryable_but_a_failed_precondition_is_a_lost_cas() {
    run(async {
        use axum::{http::StatusCode, Router};
        use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let app = Router::new().fallback(|uri: axum::http::Uri| async move {
            if uri.path().ends_with("conflict") || uri.path().ends_with("conflict.json") {
                (
                    StatusCode::CONFLICT,
                    "<Error><Code>ConditionalRequestConflict</Code></Error>",
                )
            } else {
                (
                    StatusCode::PRECONDITION_FAILED,
                    "<Error><Code>PreconditionFailed</Code></Error>",
                )
            }
        });
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let directory = tempfile::tempdir()?;
        let mut bucket = Bucket::open_dev(&directory.path().join("bucket.sqlite"))?;
        let s3 = Arc::new(
            AmazonS3Builder::new()
                .with_bucket_name("test")
                .with_endpoint(format!("http://{address}"))
                .with_region("us-east-1")
                .with_access_key_id("fixture")
                .with_secret_access_key("fixture")
                .with_allow_http(true)
                .with_conditional_put(S3ConditionalPut::ETagMatch)
                .with_retry(object_store::RetryConfig {
                    max_retries: 0,
                    ..Default::default()
                })
                .build()?,
        );
        bucket.store = s3.clone();
        bucket.cas_store = s3;
        bucket.backend = crate::bucket::StorageBackend::S3;
        let conflict = bucket
            .put_cas("conflict", vec![42], Some("prior-etag"))
            .await;
        let rejected = bucket
            .put_cas("precondition", vec![42], Some("prior-etag"))
            .await;
        let create_conflict = bucket.put_cas("conflict", vec![42], None).await;
        let create_rejected = bucket.put_cas("precondition", vec![42], None).await;
        assert!(
            create_conflict.is_err(),
            "HTTP 409 on create remains retryable"
        );
        assert_eq!(
            create_rejected?, None,
            "HTTP 412 on If-None-Match is a definitive create rejection, including RustFS"
        );
        assert!(
            conflict.is_err(),
            "S3's HTTP 409 does not prove the lease token changed"
        );
        assert!(crate::bucket::cas_write_did_not_commit(
            &conflict.unwrap_err()
        ));
        assert_eq!(
            rejected?, None,
            "HTTP 412 remains a definitive lost CAS race"
        );
        let ownership = crate::ownership_store::BucketOwnership::new(
            bucket.clone(),
            bucket.clone(),
            "conflict".into(),
            "generation".into(),
        );
        let renewed = ownership
            .cas_node_lease(
                celld_logic::CasGuard::Match("prior-etag".into()),
                &celld_logic::NodeLeaseRecord {
                    node: "conflict".into(),
                    addr: "unused".into(),
                    expires_ms: crate::ownership_store::now_ms() + 10000,
                    peer_protocol: crate::peer_auth::PROTOCOL_VERSION,
                    generation: "generation".into(),
                    log_state: None,
                    etag: "prior-etag".into(),
                },
                &mut None,
            )
            .await;
        server.abort();
        assert!(matches!(renewed, Err(crate::ownership_store::LeaseCasError::NotCommitted(_))),
                "renewal must reach the core as retryable, not a definitive rejection that fences the node");
        Ok(())
    });
}

#[test]
#[allow(clippy::disallowed_methods)] // Deliberately exercise the real network and wall-clock timeout.
fn slow_tail_body_keeps_progressing_beyond_the_old_whole_request_timeout() {
    run(async {
        use axum::{body::Body, routing::post, Router};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let app = Router::new().route(
            "/peer/log/tail-page",
            post(|| async {
                let stream = futures_util::stream::unfold(0, |chunk| async move {
                    if chunk == 12 {
                        return None;
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    Some((
                        Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"payload")),
                        chunk + 1,
                    ))
                });
                axum::http::Response::builder()
                    .header(
                        crate::peer_auth::RESPONSE_VERSION_HEADER,
                        crate::peer_auth::PROTOCOL_VERSION_TEXT,
                    )
                    .body(Body::from_stream(stream))
                    .unwrap()
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let transport = SignedPeerTransport::new(Arc::new(PeerAuth::new([7; 32], "recoverer")?));
        let response = transport
            .post(
                "follower",
                &address.to_string(),
                "/peer/log/tail-page",
                b"{}".to_vec(),
                None,
            )
            .await;
        server.abort();
        let response = response?;
        assert_eq!(response.as_ref(), b"payload".repeat(12));
        Ok(())
    });
}

#[test]
fn a_corrupt_later_page_cannot_certify_an_earlier_page_as_the_whole_tail() {
    run(async {
        let directory = tempfile::tempdir()?;
        let follower = FollowerStore::new(directory.path(), None, "follower");
        follower.persist(
            "leader/old",
            FollowerState {
                fragment_epoch: 1,
                ..FollowerState::default()
            },
        )?;
        for seq in 1..=2 {
            assert!(
                follower
                    .append(AppendReq {
                        leader: "leader/old".into(),
                        epoch: 1,
                        truncate_to: 0,
                        entries: vec![Entry {
                            seq,
                            cell: "Counter:test".into(),
                            cell_epoch: 1,
                            txid: seq,
                            bytes: vec![42; 1024]
                        }],
                    })
                    .await
                    .ok
            );
        }
        let sealed = follower
            .seal(&SealReq {
                leader: "leader/old".into(),
                epoch: 1,
            })
            .await?;
        let range = TailRange {
            fragment_epoch: 1,
            base: 0,
            end: sealed.end,
            complete: true,
        };
        let first = follower
            .tail_page(&TailPageReq {
                leader: "leader/old".into(),
                after: Some(0),
                max_bytes: 1100,
                range: Some(range),
            })
            .await?;
        assert_eq!(first.entries.len(), 1);
        follower
            .filesystem
            .write(&follower.dir("leader/old").join("2-2.batch"), b"torn")?;
        let second = follower
            .tail_page(&TailPageReq {
                leader: "leader/old".into(),
                after: Some(1),
                max_bytes: 1100,
                range: Some(range),
            })
            .await;
        assert!(
            second.is_err(),
            "cached range evidence must not bypass revalidation of each file"
        );
        Ok(())
    });
}

#[test]
fn tail_pages_are_bounded_and_cover_a_fixed_sealed_range_once() {
    run(async {
        let directory = tempfile::tempdir()?;
        let follower = FollowerStore::new(directory.path(), None, "follower");
        follower.persist(
            "leader/old",
            FollowerState {
                fragment_epoch: 1,
                ..FollowerState::default()
            },
        )?;
        for batch in 0..8 {
            assert!(
                follower
                    .append(AppendReq {
                        leader: "leader/old".into(),
                        epoch: 1,
                        truncate_to: 0,
                        entries: (batch * 4 + 1..=batch * 4 + 4)
                            .map(|seq| Entry {
                                seq,
                                cell: "Counter:test".into(),
                                cell_epoch: 1,
                                txid: seq,
                                bytes: vec![seq as u8; 1024],
                            })
                            .collect(),
                    })
                    .await
                    .ok
            );
        }
        let sealed = follower
            .seal(&SealReq {
                leader: "leader/old".into(),
                epoch: 1,
            })
            .await?;
        let range = TailRange {
            fragment_epoch: sealed.fragment_epoch,
            base: sealed.base.unwrap(),
            end: sealed.end,
            complete: true,
        };
        let mut after = range.base;
        let mut seen = Vec::new();
        while after < range.end {
            let page = follower
                .tail_page(&TailPageReq {
                    leader: "leader/old".into(),
                    after: Some(after),
                    max_bytes: 4096,
                    range: Some(range),
                })
                .await?;
            assert_eq!(page.range, range);
            let encoded = encode_tail_page(&page);
            let decoded = decode_tail_page(&encoded)?;
            assert_eq!(decoded.range, range);
            assert_eq!(decoded.entries.len(), page.entries.len());
            assert!(
                decode_tail_resp(&encoded).is_err(),
                "a page must never be accepted as whole-tail evidence"
            );
            assert!(
                encode_tail_resp(&page).len() <= 4096 + 64,
                "tail response exceeded its page budget"
            );
            assert!(!page.entries.is_empty());
            for entry in page.entries {
                assert_eq!(entry.seq, after + 1);
                assert_eq!(entry.bytes, vec![entry.seq as u8; 1024]);
                after = entry.seq;
                seen.push(entry.seq);
            }
        }
        assert_eq!(seen, (1..=32).collect::<Vec<_>>());
        Ok(())
    });
}

#[test]
fn generated_node_identity_survives_a_restart_on_the_same_disk() {
    run(async {
        let directory = tempfile::tempdir()?;
        let first = crate::node_identity::load_or_create(directory.path(), None)?;
        let restarted = crate::node_identity::load_or_create(directory.path(), None)?;
        assert_eq!(first, restarted);
        Ok(())
    });
}

#[test]
fn explicit_identity_is_remembered_and_cannot_silently_change() {
    run(async {
        let directory = tempfile::tempdir()?;
        assert_eq!(
            crate::node_identity::load_or_create(directory.path(), Some("node-a"))?,
            "node-a"
        );
        assert_eq!(
            crate::node_identity::load_or_create(directory.path(), None)?,
            "node-a"
        );
        assert!(crate::node_identity::load_or_create(directory.path(), Some("node-b")).is_err());
        Ok(())
    });
}

impl LogTransport for UnavailablePeer {
    fn post<'a>(
        &'a self,
        _node: &'a str,
        _addr: &'a str,
        _path: &'a str,
        _body: Vec<u8>,
        _deadline: Option<std::time::Duration>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<bytes::Bytes>> + Send + 'a>,
    > {
        Box::pin(async { Err(std::io::Error::from(std::io::ErrorKind::ConnectionRefused).into()) })
    }
}

#[test]
fn an_unavailable_follower_does_not_authorize_loss_or_sealing() {
    run(async {
        let directory = tempfile::tempdir()?;
        let bucket = Arc::new(Bucket::open_dev(&directory.path().join("bucket.sqlite"))?);
        lease(&bucket, "follower", "old", serde_json::Value::Null).await;
        lease(
            &bucket,
            "leader",
            "old",
            serde_json::json!({
                "epoch": 1,
                "ensemble": ["follower"],
                "tiered": 0,
                "state": "open",
                "active": true,
            }),
        )
        .await;
        let ownership = Arc::new(crate::ownership_store::BucketOwnership::new(
            bucket.as_ref().clone(),
            bucket.as_ref().clone(),
            "recoverer".into(),
            "generation".into(),
        ));
        let ltx = Arc::new(crate::ltx_repl::LtxRepl::start(
            &directory.path().join("recoverer"),
            crate::bucket::StorageBackend::Local,
            bucket.name.clone(),
            String::new(),
            None,
            "auto".into(),
            None,
        )?);
        let manager = NodeLogManager::new_with_log_transport(
            "recoverer/generation",
            bucket.clone(),
            Arc::new(OwnLog {
                ownership,
                nudge: Box::new(|| {}),
                write_lock: tokio::sync::Mutex::new(()),
            }),
            ltx,
            Arc::new(UnavailablePeer),
            eviction_policy_from_env()?,
        );
        let result = manager.recover_as("leader/old", RecoverMode::Boot).await;
        assert!(
            result.is_err(),
            "a missing witness must keep recovery pending"
        );
        let record = read_record(&bucket, "leader/old").await?.unwrap();
        assert_eq!(record.record.state, LogState::Recovering);
        assert!(bucket.get("log/leader/old.e1.loss.json").await?.is_none());
        Ok(())
    });
}

#[test]
fn follower_gc_reclaims_a_recovered_then_replaced_session() {
    run(async {
        let directory = tempfile::tempdir()?;
        let bucket = Arc::new(Bucket::open_dev(&directory.path().join("bucket.sqlite"))?);
        let follower = FollowerStore::new(directory.path(), Some(bucket.clone()), "follower");
        crate::node_identity::load_or_create(directory.path(), Some("follower"))?;
        lease(&bucket, "follower", "own", serde_json::Value::Null).await;
        follower.persist(
            "leader/old",
            FollowerState {
                fragment_epoch: 1,
                ..FollowerState::default()
            },
        )?;
        assert!(
            follower
                .append(AppendReq {
                    leader: "leader/old".into(),
                    epoch: 1,
                    truncate_to: 0,
                    entries: vec![Entry {
                        seq: 1,
                        cell: "Counter:test".into(),
                        cell_epoch: 1,
                        txid: 1,
                        bytes: vec![42],
                    }],
                })
                .await
                .ok
        );
        assert!(
            !check_disk_retirement(bucket.clone(), directory.path())
                .await?
                .safe,
            "a missing leader record must keep the follower disk pinned"
        );
        let committed_file = follower.dir("leader/old").join("1-1.batch");
        let committed_bytes = follower.filesystem.read(&committed_file)?;
        // Startup may replace this generation only after predecessor recovery.
        lease(&bucket, "leader", "new", serde_json::Value::Null).await;
        follower.gc_fragments().await;
        assert!(
            check_disk_retirement(bucket.clone(), directory.path())
                .await?
                .safe
        );
        lease(
            &bucket,
            "follower",
            "own",
            serde_json::json!({
                "epoch": 1, "ensemble": [], "tiered": 0, "state": "open", "active": false,
            }),
        )
        .await;
        assert!(
            !check_disk_retirement(bucket.clone(), directory.path())
                .await?
                .safe,
            "reclaimed follower fragments do not discharge an open own session"
        );
        assert!(!follower
            .filesystem
            .metadata(&follower.dir("leader/old").join("1-1.batch"))
            .is_ok());
        let state = follower.load("leader/old")?;
        assert_eq!(state.base, state.end);
        assert!(state.sealed_to >= 1);
        // A crash can happen after the GC marker fsync and before deletion.
        follower
            .filesystem
            .write(&committed_file, &committed_bytes)?;
        let restarted = FollowerStore::new(directory.path(), Some(bucket.clone()), "follower");
        restarted.gc_fragments().await;
        assert!(
            restarted.filesystem.metadata(&committed_file).is_err(),
            "GC must retry payload removal even when its marker already says empty"
        );
        // Reclamation must not let delayed old-owner appends resurrect data.
        assert!(
            !follower
                .append(AppendReq {
                    leader: "leader/old".into(),
                    epoch: 1,
                    truncate_to: 0,
                    entries: vec![Entry {
                        seq: 2,
                        cell: "Counter:test".into(),
                        cell_epoch: 1,
                        txid: 2,
                        bytes: vec![43],
                    }],
                })
                .await
                .ok
        );
        Ok(())
    });
}
