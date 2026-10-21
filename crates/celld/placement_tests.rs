// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Public decision-core and production CAS regressions, without the upstream
//! external simulation harness or process-wide environment mutation.
#![allow(clippy::disallowed_methods)]

use crate::bucket::Bucket;
use crate::ownership_store::BucketOwnership;
use crate::test_runtime::run;
use celld_logic::placement::Pool;
use celld_logic::*;

fn gpu() -> Pool {
    "gpu".parse().unwrap()
}

fn adapter(bucket: &Bucket, node: &str, pool: Pool) -> BucketOwnership {
    BucketOwnership::new(
        bucket.clone(),
        bucket.clone(),
        node.into(),
        "probe-key".into(),
    )
    .with_placement_pool(pool, true)
}

fn state(pool: Pool) -> State {
    let mut state = State::new(
        "self",
        Config {
            placement_pool: pool,
            max_resident: 10,
            max_activations: 10,
            max_evictions: 2,
            max_releases: 2,
            max_outbound_websockets: 4,
            ownership_on_evict: OwnershipOnEvict::Release,
            require_node_lease: true,
            peer_protocol: crate::peer_auth::PROTOCOL_VERSION,
            operation_deadline_ms: Some(1000),
            owner_log_recovery_backoff_ms: 100,
            owner_log_recovery_attempts: 3,
            alarm_resident_ms: 1000,
            idle_evict_ms: None,
            pressure: pressure::PressureConfig::default(),
        },
    );
    let effects = on_event(
        &mut state,
        Event::StartNodeLease {
            now_ms: 1000,
            now_mono_ms: 100,
            spec: NodeLeaseSpec {
                addr: "127.0.0.1:1".into(),
                peer_protocol: crate::peer_auth::PROTOCOL_VERSION,
                generation: "generation".into(),
                resume_generation: None,
                ttl_ms: 60_000,
            },
        },
    );
    let op = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::ReadSelfNodeLease { op, .. } => Some(*op),
            _ => None,
        })
        .unwrap();
    let effects = on_event(
        &mut state,
        Event::SelfNodeLeaseRead {
            op,
            now_ms: 1000,
            now_mono_ms: 100,
            result: Ok(None),
        },
    );
    let op = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::CasNodeLease { op, record, .. } => {
                assert_eq!(record.placement_pool, pool);
                Some(*op)
            }
            _ => None,
        })
        .unwrap();
    on_event(
        &mut state,
        Event::NodeLeaseCasCompleted {
            op,
            now_mono_ms: 100,
            result: Ok(LeaseCasOutcome::Applied {
                etag: "lease".into(),
            }),
            stamped_log_state: None,
        },
    );
    assert!(state.ready_to_serve());
    state
}

fn owner_read(state: &mut State, owner: Option<OwnerRecord>) -> Vec<Effect> {
    let effects = on_event(
        state,
        Event::Request {
            request: 1,
            cell: "Cell:test".into(),
        },
    );
    let op = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::ReadOwner { op, .. } => Some(*op),
            _ => None,
        })
        .unwrap();
    let effects = on_event(
        state,
        Event::OwnerRead {
            op,
            now_ms: 1000,
            result: Ok(owner),
        },
    );
    state.validate().unwrap();
    effects
}

fn unowned(pool: Pool) -> OwnerRecord {
    OwnerRecord {
        placement_pool: pool,
        node: None,
        epoch: 0,
        etag: "registration".into(),
    }
}

fn capacity(effects: &[Effect]) -> OpId {
    effects
        .iter()
        .find_map(|effect| match effect {
            Effect::ReadCapacityPeers { op, .. } => Some(*op),
            _ => None,
        })
        .unwrap()
}

fn peer(node: &str, pool: Pool, owned: usize) -> CapacityPeer {
    CapacityPeer {
        placement_pool: pool,
        hot_recovery: true,
        node: node.into(),
        addr: "127.0.0.1:2".into(),
        expires_ms: 60_000,
        peer_protocol: crate::peer_auth::PROTOCOL_VERSION,
        sampled_ms: 1000,
        owned_cells: Some(owned),
        placement_weight: Some(1),
        bucket_format: Some(1),
        resident_cells: owned,
        host_websockets: 0,
        rss_bytes: 0,
        in_use_bytes: Some(0),
        pressured: false,
        memory_headroom: Some(true),
        restoring: 0,
        paced_handoff: true,
        rebalance_paused: false,
        draining: false,
    }
}

#[test]
fn cpu_ingress_selects_only_gpu_capacity_and_never_claims_locally() {
    let mut state = state(Pool::DEFAULT);
    let effects = owner_read(&mut state, Some(unowned(gpu())));
    assert!(!effects
        .iter()
        .any(|effect| matches!(effect, Effect::CasOwner { .. } | Effect::Restore { .. })));
    let effects = on_event(
        &mut state,
        Event::CapacityPeersRead {
            op: capacity(&effects),
            now_ms: 1000,
            result: Ok(vec![
                peer("cpu", Pool::DEFAULT, 0),
                peer("gpu-peer", gpu(), 5),
            ]),
        },
    );
    assert!(effects.iter().any(|effect| matches!(effect,
        Effect::Complete { result: Ok(Route::Remote { node, epoch: 0, .. }), .. } if node == "gpu-peer")));
    state.validate().unwrap();
}

#[test]
fn absent_gpu_or_failed_membership_scan_fails_closed() {
    for result in [
        Ok(vec![peer("cpu", Pool::DEFAULT, 0)]),
        Err(Failure::Definite),
    ] {
        let mut state = state(Pool::DEFAULT);
        let effects = owner_read(&mut state, Some(unowned(gpu())));
        let effects = on_event(
            &mut state,
            Event::CapacityPeersRead {
                op: capacity(&effects),
                now_ms: 1000,
                result,
            },
        );
        assert!(effects.iter().any(|effect| matches!(
            effect,
            Effect::Complete {
                result: Err(RequestError::PlacementUnavailable),
                ..
            }
        )));
        assert!(!effects
            .iter()
            .any(|effect| matches!(effect, Effect::CasOwner { .. } | Effect::Restore { .. })));
        state.validate().unwrap();
    }
}

#[test]
fn gpu_nodes_do_not_silently_claim_legacy_default_cells() {
    let mut state = state(gpu());
    let effects = owner_read(&mut state, None);
    let effects = on_event(
        &mut state,
        Event::CapacityPeersRead {
            op: capacity(&effects),
            now_ms: 1000,
            result: Ok(vec![]),
        },
    );
    assert!(effects.iter().any(|effect| matches!(
        effect,
        Effect::Complete {
            result: Err(RequestError::PlacementUnavailable),
            ..
        }
    )));
    state.validate().unwrap();
}

#[test]
fn reserved_cell_first_activation_is_fresh_but_released_cell_is_not() {
    for epoch in [0, 6] {
        let mut state = state(gpu());
        let mut owner = unowned(gpu());
        owner.epoch = epoch;
        let effects = owner_read(&mut state, Some(owner));
        let op = effects
            .iter()
            .find_map(|effect| match effect {
                Effect::CasOwner {
                    op, epoch: claimed, ..
                } => {
                    assert_eq!(*claimed, epoch + 1);
                    Some(*op)
                }
                _ => None,
            })
            .unwrap();
        let effects = on_event(
            &mut state,
            Event::OwnerCasCompleted {
                op,
                result: Ok(CasOutcome::Applied),
            },
        );
        assert!(effects.iter().any(|effect| matches!(effect,
            Effect::Restore { spec, .. } if spec.fresh == (epoch == 0) && spec.took_over == (epoch != 0))));
        state.validate().unwrap();
    }
}

#[test]
fn live_wrong_pool_owner_lease_is_unavailable_not_takeover_permission() {
    let mut state = state(Pool::DEFAULT);
    let mut owner = unowned(gpu());
    owner.node = Some("foreign".into());
    owner.epoch = 4;
    let effects = owner_read(&mut state, Some(owner));
    let op = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::ReadNodeLease { op, .. } => Some(*op),
            _ => None,
        })
        .unwrap();
    let effects = on_event(
        &mut state,
        Event::NodeLeaseRead {
            op,
            now_ms: 1000,
            result: Ok(Some(NodeLeaseRecord {
                placement_pool: Pool::DEFAULT,
                node: "foreign".into(),
                addr: "127.0.0.1:2".into(),
                expires_ms: 60_000,
                peer_protocol: crate::peer_auth::PROTOCOL_VERSION,
                generation: "foreign-generation".into(),
                log_state: None,
                etag: "lease".into(),
            })),
        },
    );
    assert!(effects.iter().any(|effect| matches!(
        effect,
        Effect::Complete {
            result: Err(RequestError::PlacementUnavailable),
            ..
        }
    )));
    assert!(!effects
        .iter()
        .any(|effect| matches!(effect, Effect::CasOwner { .. })));
    state.validate().unwrap();
}

#[test]
fn cpu_cannot_take_over_dead_gpu_owner_and_gpu_still_recovers_its_log() {
    for pool in [Pool::DEFAULT, gpu()] {
        let mut state = state(pool);
        let mut owner = unowned(gpu());
        owner.node = Some("dead".into());
        owner.epoch = 4;
        let effects = owner_read(&mut state, Some(owner));
        let op = effects
            .iter()
            .find_map(|effect| match effect {
                Effect::ReadNodeLease { op, .. } => Some(*op),
                _ => None,
            })
            .unwrap();
        let effects = on_event(
            &mut state,
            Event::NodeLeaseRead {
                op,
                now_ms: 1000,
                result: Ok(Some(NodeLeaseRecord {
                    placement_pool: gpu(),
                    node: "dead".into(),
                    addr: "127.0.0.1:2".into(),
                    expires_ms: 500,
                    peer_protocol: crate::peer_auth::PROTOCOL_VERSION,
                    generation: "dead-generation".into(),
                    log_state: Some(log_tier::LogState::Open),
                    etag: "lease".into(),
                })),
            },
        );
        assert!(!effects
            .iter()
            .any(|effect| matches!(effect, Effect::CasOwner { .. })));
        if pool == gpu() {
            assert!(effects
                .iter()
                .any(|effect| matches!(effect, Effect::RecoverNodeLog { .. })));
        } else {
            capacity(&effects);
        }
        state.validate().unwrap();
    }
}

#[test]
fn rebalancing_counts_and_selects_only_the_owners_pool() {
    let peers = vec![
        peer("self", gpu(), 10),
        peer("gpu-peer", gpu(), 0),
        peer("cpu", Pool::DEFAULT, 0),
    ];
    assert_eq!(
        rebalance::surplus(&peers, "self", 1000, 10_000, 0, 20),
        Some(4)
    );
    assert_eq!(
        rebalance::receivers(&peers, "self", 1000, 10_000),
        vec!["gpu-peer".to_string()]
    );
    assert_eq!(
        rebalance::surplus(
            &[peers[0].clone(), peers[2].clone()],
            "self",
            1000,
            10_000,
            0,
            20
        ),
        None
    );
}

#[test]
fn placement_registration_is_idempotent_immutable_and_cas_fenced() {
    run(async {
        let dir = tempfile::tempdir()?;
        let bucket = Bucket::open_dev(&dir.path().join("bucket.sqlite"))?;
        let cpu = adapter(&bucket, "cpu", Pool::DEFAULT);
        let a = adapter(&bucket, "gpu-a", gpu());
        let b = adapter(&bucket, "gpu-b", gpu());
        cpu.create_cell("Cell:test", gpu()).await?;
        cpu.create_cell("Cell:test", gpu()).await?;
        assert!(cpu.create_cell("Cell:test", Pool::DEFAULT).await.is_err());
        let owner = cpu.read_owner("Cell:test").await?.unwrap();
        assert_eq!(
            owner,
            OwnerRecord {
                placement_pool: gpu(),
                node: None,
                epoch: 0,
                etag: owner.etag.clone()
            }
        );
        let guard = CasGuard::Match(owner.etag.clone());
        assert_eq!(
            cpu.cas_owner("Cell:test", guard.clone(), 1).await?,
            CasOutcome::Rejected
        );
        assert_eq!(
            a.cas_owner("Cell:test", guard.clone(), 1).await?,
            CasOutcome::Applied
        );
        assert_eq!(
            b.cas_owner("Cell:test", guard, 1).await?,
            CasOutcome::Rejected
        );
        assert_eq!(a.release_owner("Cell:test", 1).await?, CasOutcome::Applied);
        let released = a.read_owner("Cell:test").await?.unwrap();
        assert_eq!(
            (released.placement_pool, released.node, released.epoch),
            (gpu(), None, 1)
        );
        assert_eq!(
            b.cas_owner("Cell:test", CasGuard::Match(released.etag), 2)
                .await?,
            CasOutcome::Applied
        );
        assert!(cpu.create_cell("Cell:test", Pool::DEFAULT).await.is_err());
        assert!(a.create_cell("../escape", gpu()).await.is_err());
        Ok(())
    });
}

#[test]
fn concurrent_registration_cannot_overwrite_a_different_pool() {
    run(async {
        let dir = tempfile::tempdir()?;
        let bucket = Bucket::open_dev(&dir.path().join("bucket.sqlite"))?;
        let a = adapter(&bucket, "a", gpu());
        let b = adapter(&bucket, "b", Pool::DEFAULT);
        let (left, right) = tokio::join!(
            a.create_cell("Cell:test", gpu()),
            b.create_cell("Cell:test", Pool::DEFAULT)
        );
        assert_ne!(left.is_ok(), right.is_ok());
        let owner = a.read_owner("Cell:test").await?.unwrap();
        assert_eq!(
            owner.placement_pool,
            if left.is_ok() { gpu() } else { Pool::DEFAULT }
        );
        Ok(())
    });
}

#[test]
fn legacy_owner_and_node_lease_decode_as_default_without_hot_capability() {
    run(async {
        let dir = tempfile::tempdir()?;
        let bucket = Bucket::open_dev(&dir.path().join("bucket.sqlite"))?;
        bucket
            .put(
                "cells/Cell:test/own.json",
                br#"{"node":"cpu","epoch":9}"#.to_vec(),
            )
            .await?;
        bucket
            .put(
                "nodes/cpu.json",
                br#"{"node":"cpu","addr":"127.0.0.1:1","expires_ms":60000}"#.to_vec(),
            )
            .await?;
        let a = adapter(&bucket, "cpu", Pool::DEFAULT);
        assert_eq!(
            a.read_owner("Cell:test").await?.unwrap().placement_pool,
            Pool::DEFAULT
        );
        let peers = a.read_capacity_peers().await?;
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].placement_pool, Pool::DEFAULT);
        assert!(!peers[0].hot_recovery);
        assert!(a.create_cell("Cell:test", gpu()).await.is_err());
        Ok(())
    });
}

#[test]
fn gpu_pool_requires_matching_hot_follower_but_default_preserves_legacy_policy() {
    assert!(gpu().accepts_follower(gpu(), true));
    assert!(!gpu().accepts_follower(gpu(), false));
    assert!(!gpu().accepts_follower(Pool::DEFAULT, true));
    assert!(!Pool::DEFAULT.accepts_follower(gpu(), true));
    assert!(Pool::DEFAULT.accepts_follower(Pool::DEFAULT, false));
}

#[test]
fn pool_wakers_are_independent_and_probe_only_matching_cells() {
    run(async {
        let dir = tempfile::tempdir()?;
        let bucket = Bucket::open_dev(&dir.path().join("bucket.sqlite"))?;
        let cpu = adapter(&bucket, "cpu", Pool::DEFAULT);
        let gpu_node = adapter(&bucket, "gpu-a", gpu());
        cpu.create_cell("Cell:cpu", Pool::DEFAULT).await?;
        cpu.create_cell("Cell:gpu", gpu()).await?;
        let now = crate::asyncrt::wall_ms();
        assert!(crate::wake::try_hold_pool_waker(&bucket, "cpu", Pool::DEFAULT, now, 10_000).await);
        assert!(crate::wake::try_hold_pool_waker(&bucket, "gpu-a", gpu(), now, 10_000).await);
        assert!(!crate::wake::try_hold_pool_waker(&bucket, "gpu-b", gpu(), now, 10_000).await);
        let due = vec![("Cell:cpu".into(), now), ("Cell:gpu".into(), now)];
        let live = Default::default();
        assert_eq!(
            crate::wake::elected_due(&cpu, "cpu", &live, due.clone()).await,
            vec![("Cell:cpu".into(), now)]
        );
        assert_eq!(
            crate::wake::elected_due(&gpu_node, "gpu-a", &live, due).await,
            vec![("Cell:gpu".into(), now)]
        );
        Ok(())
    });
}

#[test]
fn node_disk_cannot_change_placement_pool_or_upgrade_a_legacy_disk_to_gpu() {
    run(async {
        let dir = tempfile::tempdir()?;
        crate::node_identity::validate_placement_pool(dir.path(), gpu())?;
        crate::node_identity::load_or_create(dir.path(), Some("gpu-a"))?;
        crate::node_identity::validate_placement_pool(dir.path(), gpu())?;
        assert!(crate::node_identity::validate_placement_pool(dir.path(), Pool::DEFAULT).is_err());
        let legacy = tempfile::tempdir()?;
        crate::node_identity::load_or_create(legacy.path(), Some("cpu"))?;
        assert!(crate::node_identity::validate_placement_pool(legacy.path(), gpu()).is_err());
        crate::node_identity::validate_placement_pool(legacy.path(), Pool::DEFAULT)?;
        Ok(())
    });
}
