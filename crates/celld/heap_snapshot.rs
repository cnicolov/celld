// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! On-demand V8 heap diagnostics, outside the Actor execution domain.
//! Serialize to an anonymous file rather than retaining the JSON in node RAM.
#![allow(clippy::disallowed_methods)]

use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use std::io::{self, Seek, Write};
use std::sync::{Arc, OnceLock};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const CHUNK_BYTES: usize = 64 * 1024;
const MAX_SNAPSHOT_BYTES: u64 = 1024 * 1024 * 1024;

#[derive(Debug)]
pub enum Error {
    Busy,
    Gone,
    Io(io::Error),
    Task(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Busy => formatter.write_str("the isolate or another heap snapshot is busy"),
            Self::Gone => formatter.write_str("the isolate is no longer resident"),
            Self::Io(error) => write!(formatter, "heap snapshot failed: {error}"),
            Self::Task(error) => write!(formatter, "heap snapshot task failed: {error}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// One complete snapshot. The permit follows its download, so a slow reader
/// cannot accumulate more temporary snapshots by submitting more requests.
pub struct Snapshot {
    file: std::fs::File,
    bytes: u64,
    permit: OwnedSemaphorePermit,
}

impl Snapshot {
    pub fn len(&self) -> u64 {
        self.bytes
    }

    pub fn is_empty(&self) -> bool {
        self.bytes == 0
    }

    pub fn into_stream(self) -> impl Stream<Item = io::Result<Bytes>> + Send + 'static {
        let reader = tokio_util::io::ReaderStream::with_capacity(
            tokio::fs::File::from_std(self.file),
            CHUNK_BYTES,
        );
        futures_util::stream::unfold((reader, self.permit), |(mut reader, permit)| async move {
            reader.next().await.map(|bytes| (bytes, (reader, permit)))
        })
    }
}

pub(crate) async fn capture(slot: Arc<crate::pool::Slot>) -> Result<Snapshot, Error> {
    static GATE: OnceLock<Arc<Semaphore>> = OnceLock::new();
    let permit = GATE
        .get_or_init(|| Arc::new(Semaphore::new(1)))
        .clone()
        .try_acquire_owned()
        .map_err(|_| Error::Busy)?;
    // The closure owns the permit even if the HTTP caller disconnects. V8
    // snapshot construction is synchronous; other isolates keep their workers.
    tokio::task::spawn_blocking(move || {
        let mut file = tempfile::tempfile()?;
        let bytes = {
            let mut writer = io::BufWriter::with_capacity(CHUNK_BYTES, &mut file);
            slot.write_heap_snapshot(&mut writer)?
        };
        file.rewind()?;
        Ok(Snapshot {
            file,
            bytes,
            permit,
        })
    })
    .await
    .map_err(|error| Error::Task(error.to_string()))?
}

/// V8 calls the callback with an empty slice only at successful end of stream.
/// A failed write, size limit or missing completion must not become a successful
/// download containing truncated JSON.
pub(crate) fn write(isolate: &mut v8::Isolate, output: &mut impl Write) -> Result<u64, Error> {
    write_bounded(isolate, output, MAX_SNAPSHOT_BYTES).map_err(Error::Io)
}

fn write_bounded(
    isolate: &mut v8::Isolate,
    output: &mut impl Write,
    maximum: u64,
) -> io::Result<u64> {
    let mut bytes = 0_u64;
    let mut complete = false;
    let mut failure = None;
    isolate.take_heap_snapshot(|chunk| {
        if failure.is_some() {
            return false;
        }
        if chunk.is_empty() {
            complete = true;
            return true;
        }
        if chunk.len() as u64 > maximum.saturating_sub(bytes) {
            failure = Some(io::Error::other(
                "heap snapshot exceeds its 1 GiB output limit",
            ));
            return false;
        }
        if let Err(error) = output.write_all(chunk) {
            failure = Some(error);
            return false;
        }
        bytes += chunk.len() as u64;
        true
    });
    if let Some(error) = failure {
        return Err(error);
    }
    if !complete || bytes == 0 {
        return Err(io::Error::other("V8 did not complete the heap snapshot"));
    }
    output.flush()?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::TryStreamExt as _;

    #[test]
    fn capture_uses_the_resident_permit_and_releases_download_resources() {
        let runtime = crate::test_runtime::init();
        crate::runtime::init_v8();
        let config = Arc::new(crate::js::WorkerConfig::new(crate::js::WorkerConfigOptions {
            src: "globalThis.heapProof = { marker: 'celld-snapshot-retained-proof' }; export default { fetch() { return new Response('ok'); } };".into(),
            script_name: "heap-snapshot-test".into(),
            do_classes: vec![], bindings: vec![], r2_bindings: vec![],
            d1_bindings: vec![], kv_bindings: vec![], queue_bindings: vec![],
            queue_consumers: vec![], workflow_bindings: vec![], vars: vec![],
            node: "test-node".into(), modules: vec![], compat: Default::default(),
        }));
        let slot =
            crate::pool::Slot::loaded_worker(crate::js::Worker::load_config(config).unwrap());

        let (entered, ready) = std::sync::mpsc::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let running = {
            let slot = slot.clone();
            runtime.spawn(async move {
                slot.turn(move |_| {
                    entered.send(()).unwrap();
                    let _ = blocked.recv();
                })
                .await;
            })
        };
        ready
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let busy = runtime.block_on(async {
            tokio::time::timeout(std::time::Duration::from_secs(2), capture(slot.clone())).await
        });
        release.send(()).unwrap();
        runtime.block_on(running).unwrap();
        assert!(matches!(busy, Ok(Err(Error::Busy))));

        let snapshot = runtime.block_on(capture(slot.clone())).unwrap();
        let expected = snapshot.len();
        assert!(matches!(
            runtime.block_on(capture(slot.clone())),
            Err(Error::Busy)
        ));
        let bytes = runtime
            .block_on(
                snapshot
                    .into_stream()
                    .try_fold(Vec::new(), |mut all, chunk| async move {
                        all.extend_from_slice(&chunk);
                        Ok(all)
                    }),
            )
            .unwrap();
        assert_eq!(bytes.len() as u64, expected);
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(json["snapshot"]["node_count"].as_u64().unwrap() > 0);
        let fields = json["snapshot"]["meta"]["node_fields"]
            .as_array()
            .unwrap()
            .len();
        assert_eq!(json["nodes"].as_array().unwrap().len() % fields, 0);
        assert!(json["strings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value == "celld-snapshot-retained-proof"));

        // Dropping an unconsumed download also releases the anonymous file and
        // the node-wide permit; the same resident isolate remains capturable.
        drop(runtime.block_on(capture(slot.clone())).unwrap());
        assert!(!runtime.block_on(capture(slot)).unwrap().is_empty());
    }

    #[test]
    fn incomplete_output_is_rejected_and_the_isolate_remains_usable() {
        crate::runtime::init_v8();
        let mut isolate = v8::Isolate::new(Default::default());
        let mut bytes = Vec::new();
        assert!(write_bounded(&mut isolate, &mut bytes, 8).is_err());
        assert!(bytes.is_empty());

        struct Broken;
        impl Write for Broken {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                Err(io::Error::new(
                    io::ErrorKind::StorageFull,
                    "fixture full disk",
                ))
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let error = write_bounded(&mut isolate, &mut Broken, MAX_SNAPSHOT_BYTES).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::StorageFull);

        write_bounded(&mut isolate, &mut bytes, MAX_SNAPSHOT_BYTES).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(json["snapshot"]["node_count"].as_u64().unwrap() > 0);
    }
}
