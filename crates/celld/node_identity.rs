// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The logical identity associated with a node's local recovery disk.

use anyhow::Context;
use std::io::Write;
use std::path::Path;

const IDENTITY_FILE: &str = "node-id";

/// A recovery disk cannot silently change hardware/placement identity. Legacy
/// disks belong to default; adding a named pool requires a new node/data dir.
#[allow(clippy::disallowed_methods)] // Atomic no-clobber host startup primitive.
pub fn validate_placement_pool(
    data_dir: &Path,
    pool: celld_logic::placement::Pool,
) -> anyhow::Result<()> {
    let filesystem = crate::asyncrt::fs();
    let path = data_dir.join("placement-pool");
    let read = || -> anyhow::Result<Option<celld_logic::placement::Pool>> {
        match filesystem.read(&path) {
            Ok(bytes) => Ok(Some(
                std::str::from_utf8(&bytes)?
                    .trim_end_matches('\n')
                    .parse()
                    .map_err(anyhow::Error::msg)?,
            )),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error).context("read persisted placement pool"),
        }
    };
    if let Some(stored) = read()? {
        anyhow::ensure!(stored == pool, "CELLD_PLACEMENT_POOL does not match this recovery disk's pool ({stored}); use a new node/data directory");
        return Ok(());
    }
    if pool != celld_logic::placement::Pool::DEFAULT && read_identity(data_dir, None)?.is_some() {
        anyhow::bail!("legacy recovery disks belong to the default pool; use a new node/data directory for {pool}");
    }
    filesystem.create_dir_all(data_dir)?;
    let mut temporary = tempfile::NamedTempFile::new_in(data_dir)?;
    writeln!(temporary, "{pool}")?;
    temporary.as_file().sync_all()?;
    match temporary.persist_noclobber(&path) {
        Ok(_) => {}
        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.error).context("publish placement pool"),
    }
    filesystem.sync_all(data_dir)?;
    anyhow::ensure!(
        read()? == Some(pool),
        "placement pool changed during startup"
    );
    Ok(())
}

fn read_identity(data_dir: &Path, explicit: Option<&str>) -> anyhow::Result<Option<String>> {
    let bytes = match crate::asyncrt::fs().read(&data_dir.join(IDENTITY_FILE)) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).context("read persisted node identity"),
    };
    let text = std::str::from_utf8(&bytes).context("persisted node identity is not UTF-8")?;
    let node = text.strip_suffix('\n').unwrap_or(text);
    crate::machine::validate_node_name(node).context("invalid persisted node identity")?;
    anyhow::ensure!(
        explicit.is_none_or(|configured| configured == node),
        "CELLD_NODE does not match the node identity stored in {}; keep the identity with its recovery disk",
        data_dir.join(IDENTITY_FILE).display()
    );
    Ok(Some(node.to_string()))
}

/// Publish a complete, fsynced identity without replacing an existing one.
/// Process generations remain fresh; only the logical disk identity persists.
#[allow(clippy::disallowed_methods)] // Atomic no-clobber publication is a host startup primitive.
pub fn load_or_create(data_dir: &Path, explicit: Option<&str>) -> anyhow::Result<String> {
    if let Some(node) = read_identity(data_dir, explicit)? {
        return Ok(node);
    }
    let node = explicit
        .map(str::to_owned)
        .unwrap_or_else(crate::machine::random_node_session_id);
    crate::machine::validate_node_name(&node)?;
    let filesystem = crate::asyncrt::fs();
    filesystem.create_dir_all(data_dir)?;
    // A unique temporary file plus no-clobber publication prevents two starts
    // from choosing different identities, or a torn write becoming an identity.
    let mut temporary = tempfile::NamedTempFile::new_in(data_dir)?;
    writeln!(temporary, "{node}")?;
    temporary.as_file().sync_all()?;
    match temporary.persist_noclobber(data_dir.join(IDENTITY_FILE)) {
        Ok(_) => {}
        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.error).context("publish persisted node identity"),
    }
    // Barrier the identity and newly created ancestor directory entries.
    for directory in data_dir
        .ancestors()
        .filter(|path| !path.as_os_str().is_empty())
    {
        filesystem.sync_all(directory)?;
    }
    read_identity(data_dir, explicit)?.context("persisted node identity disappeared")
}
