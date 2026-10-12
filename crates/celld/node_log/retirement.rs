// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Offline disk retirement checks: own-session and follower obligations are
//! separate. Run after stopping this disk's process, and keep it stopped until
//! the disk is retired. A graceful cell handoff proves only the former.

use super::*;

#[derive(serde::Serialize)]
pub struct DiskRetirement {
    pub node: String,
    pub safe: bool,
    pub pending: Vec<String>,
}

pub async fn check_disk_retirement(
    bucket: Arc<Bucket>,
    root: &Path,
) -> anyhow::Result<DiskRetirement> {
    let filesystem = crate::asyncrt::fs();
    let bytes = filesystem
        .read(&root.join("node-id"))
        .context("read the recovery disk's node-id")?;
    let node = std::str::from_utf8(&bytes)?
        .trim_end_matches('\n')
        .to_string();
    crate::machine::validate_node_name(&node)?;
    let mut pending = Vec::new();
    if let Some((bytes, _)) = bucket.get(&lease_key(&node)).await? {
        let wire: crate::ownership_store::NodeLeaseWire = serde_json::from_slice(&bytes)?;
        anyhow::ensure!(wire.node == node, "disk retirement lease identity mismatch");
        if wire.expires_ms > crate::ownership_store::now_ms() {
            pending.push(format!("own session {node}: node lease is still live"));
        }
        if let Some(log) = &wire.log {
            if log_from_wire(log)?.state != LogState::Sealed {
                pending.push(format!(
                    "own session {node}/{}: log is not bucket-complete",
                    wire.generation
                ));
            }
        }
    } else {
        pending.push(format!(
            "own session {node}: node lease completion proof is missing"
        ));
    }
    // Inventory errors must fail the check, never masquerade as no fragments.
    let mut sessions = BTreeSet::new();
    match filesystem.read_dir(&root.join("peerlog")) {
        Ok(leaders) => {
            for leader in leaders {
                anyhow::ensure!(leader.is_dir, "unexpected file at the peerlog root");
                let name = leader
                    .file_name
                    .to_str()
                    .context("invalid follower leader name")?;
                crate::machine::validate_node_name(name)?;
                let mut legacy = false;
                for entry in filesystem.read_dir(&leader.path)? {
                    if entry.is_dir {
                        let generation = entry
                            .file_name
                            .to_str()
                            .context("invalid follower generation")?;
                        crate::machine::validate_node_name(generation)?;
                        filesystem.read_dir(&entry.path)?;
                        sessions.insert(format!("{name}/{generation}"));
                    } else {
                        legacy = true;
                    }
                }
                if legacy {
                    sessions.insert(name.to_string());
                }
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("inventory follower disk obligations"),
    }
    let follower = FollowerStore::new(root, Some(bucket.clone()), &node);
    for session in sessions {
        let state = follower.load(&session)?;
        if state.fragment_epoch == 0 {
            // A seal can create an empty, non-witness state on a member that
            // never adopted this leader. It pins no payload. Unknown files or
            // a payload without fragment identity still make the check fail.
            anyhow::ensure!(
                state.base == 0 && state.end == 0,
                "unidentified nonempty follower fragment {session}"
            );
            anyhow::ensure!(
                filesystem
                    .read_dir(&follower.dir(&session))?
                    .iter()
                    .all(|entry| { !entry.is_dir && entry.file_name == "state.json" }),
                "unidentified payloads for {session}; retain this disk"
            );
            continue;
        }
        if !follower_fragment_closed(&bucket, &session, state.fragment_epoch).await? {
            pending.push(format!(
                "follower fragment {session}/e{}: leader has not closed or replaced the fragment",
                state.fragment_epoch
            ));
        }
    }
    Ok(DiskRetirement {
        node,
        safe: pending.is_empty(),
        pending,
    })
}
