// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The logical identity associated with a node's local recovery disk.

use anyhow::Context;
use std::io::Write;
use std::path::Path;

const IDENTITY_FILE: &str = "node-id";

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
