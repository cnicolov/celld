// Copyright 2026 Deno Land Inc. Apache-2.0 license.

use std::sync::OnceLock;

/// The native host domain is process-global; all in-process adapter suites
/// must share its one initialization, including when Cargo runs tests in parallel.
pub(crate) fn init() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        crate::asyncrt::set_host_handle(runtime.handle().clone());
        runtime
    })
}

pub(crate) fn run(future: impl std::future::Future<Output = anyhow::Result<()>>) {
    init().block_on(future).unwrap();
}
