// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Inspect an offline node disk's outstanding durability obligations.

use crate::cli_options::FleetFlags;
use crate::cli_output::{Format, Output};
use anyhow::Context;
use std::{path::PathBuf, sync::Arc};

#[allow(clippy::disallowed_methods)] // Operator configuration is outside the node execution domain.
pub async fn run(arguments: Vec<String>) -> anyhow::Result<()> {
    let mut arguments = arguments.into_iter();
    let command = arguments.next().unwrap_or_default();
    if command == "--help" || command == "-h" {
        return Output::new(Format::Text).help(
            "USAGE: celld disk check DATA_DIR --bucket NAME [--endpoint URL] [--region REGION]\n\nStop the node first. The JSON result checks both its own session and every follower fragment.\nExit 0 means bucket-complete; exit 1 means retain the disk or the proof could not be read.\n"
        );
    }
    anyhow::ensure!(
        command == "check",
        "expected `celld disk check DATA_DIR --bucket NAME`"
    );
    let root = PathBuf::from(arguments.next().context("disk check requires DATA_DIR")?);
    let mut fleet = FleetFlags::default();
    while let Some(argument) = arguments.next() {
        let mut value = |name: &str| {
            arguments
                .next()
                .with_context(|| format!("{name} requires a value"))
        };
        anyhow::ensure!(
            fleet.consume(&argument, &mut value)?,
            "unknown disk check option: {argument}"
        );
    }
    let storage = fleet.resolve("disk check")?;
    let bucket = if let Some(database) = std::env::var_os("CELLD_INTERNAL_DEV_STORE") {
        anyhow::ensure!(
            storage.bucket == "celld-dev" && storage.endpoint.is_none(),
            "invalid internal development disk check configuration"
        );
        crate::dev::open_local_bucket(&PathBuf::from(database))?
    } else {
        storage.open().await?
    };
    let result = crate::node_log::check_disk_retirement(Arc::new(bucket), &root).await?;
    Output::new(Format::Json).line(format_args!("{}", serde_json::to_string(&result)?))?;
    anyhow::ensure!(
        result.safe,
        "disk still has outstanding durability obligations"
    );
    Ok(())
}
