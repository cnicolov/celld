use celld::js::{WorkerConfig, WorkerConfigOptions};
use celld::runtime::{init_v8, StatelessRuntime};
use std::ffi::OsString;
use std::sync::{Arc, Mutex, MutexGuard};

static ENVIRONMENT: Mutex<()> = Mutex::new(());

struct PoolEnvironment {
    previous: [(&'static str, Option<OsString>); 2],
    _lock: MutexGuard<'static, ()>,
}

impl PoolEnvironment {
    fn new(minimum: Option<&str>, maximum: Option<&str>) -> Self {
        let lock = ENVIRONMENT
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let settings = [
            ("CELLD_MIN_STATELESS_ISOLATES", minimum),
            ("CELLD_MAX_STATELESS_ISOLATES", maximum),
        ];
        let previous = settings.map(|(name, _)| (name, std::env::var_os(name)));
        for (name, value) in settings {
            match value {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }
        Self {
            previous,
            _lock: lock,
        }
    }
}

impl Drop for PoolEnvironment {
    fn drop(&mut self) {
        for (name, value) in &self.previous {
            match value {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }
    }
}

fn worker_config() -> Arc<WorkerConfig> {
    Arc::new(WorkerConfig::new(WorkerConfigOptions {
        src: "let requests = 0; export default { fetch() { return new Response(String(++requests)); } };".into(),
        script_name: "warm-pool-test".into(),
        do_classes: vec![],
        bindings: vec![],
        r2_bindings: vec![],
        d1_bindings: vec![],
        kv_bindings: vec![],
        queue_bindings: vec![],
        queue_consumers: vec![],
        workflow_bindings: vec![],
        vars: vec![],
        node: "test-node".into(),
        modules: vec![],
        compat: Default::default(),
    }))
}

fn start() -> StatelessRuntime {
    celld::env_vars::validate().unwrap();
    init_v8();
    // Outside a Tokio context: drive maintenance explicitly, without sleeps.
    StatelessRuntime::start(worker_config(), "test-node".into(), "test-region".into()).unwrap()
}

#[test]
fn configured_minimum_reuses_the_warm_isolate_after_repeated_maintenance() {
    let _environment = PoolEnvironment::new(Some("1"), Some("2"));
    let runtime = start();
    let first = runtime.isolates.admit(false).unwrap();
    let original_heap = first.slot().heap_id();
    drop(first);
    let executor = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for expected in 1..=4 {
        runtime.isolates.reap();
        assert_eq!(runtime.isolates.census().live, 1);
        let request = runtime.isolates.admit(false).unwrap();
        assert_eq!(request.slot().heap_id(), original_heap);
        drop(request);
        let response = executor
            .block_on(runtime.fetch(
                "http://localhost/".into(),
                "GET".into(),
                celld::js::RequestBody::Bytes(Default::default()),
                vec![],
                None,
            ))
            .unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(response.body, expected.to_string().as_bytes());
    }
}

#[test]
// Observe the V8 shell's real wall-clock progress from outside its execution boundary.
#[allow(clippy::disallowed_methods)]
fn application_progress_is_readable_during_a_stalled_turn_and_recovers_on_return() {
    let _environment = PoolEnvironment::new(Some("1"), Some("2"));
    let runtime = start();
    let request = runtime.isolates.admit(false).unwrap();
    // Long-lived request affiliations (native I/O) spend no execution budget.
    assert!(runtime.isolates.application_progressing(1));
    let slot = request.slot().clone();
    let executor = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    executor.block_on(async {
        let (entered, receive_entered) = tokio::sync::oneshot::channel();
        let (release, receive_release) = std::sync::mpsc::channel();
        let running = tokio::spawn(async move {
            slot.turn(move |_| {
                entered.send(()).unwrap();
                // Stand in for a non-returning V8/GC turn. The observer must
                // not acquire the Worker lock this callback already holds.
                receive_release.recv().unwrap();
            })
            .await;
        });
        receive_entered.await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let observed = std::time::Instant::now();
        assert!(!runtime.isolates.application_progressing(1));
        assert!(runtime.isolates.census().max_active_turn_ms >= 1);
        assert!(observed.elapsed() < std::time::Duration::from_millis(100));
        release.send(()).unwrap();
        running.await.unwrap();
        assert!(runtime.isolates.application_progressing(1));
        assert_eq!(runtime.isolates.census().max_active_turn_ms, 0);
    });
}

#[test]
fn configured_minimum_is_prewarmed_and_superseded_pools_still_drain() {
    let _environment = PoolEnvironment::new(Some("3"), Some("3"));
    let runtime = start();
    assert_eq!(runtime.isolates.census().live, 3);
    runtime.isolates.reap();
    assert_eq!(runtime.isolates.census().live, 3);
    let outstanding = runtime.isolates.admit(false).unwrap();
    runtime.isolates.retire_all();
    assert_eq!(runtime.isolates.census().live, 0);
    assert!(!runtime.isolates.is_drained());
    drop(outstanding);
    runtime.isolates.reap();
    assert!(runtime.isolates.is_drained());
}

#[test]
fn stateless_retention_does_not_keep_empty_cell_heaps_alive() {
    let _environment = PoolEnvironment::new(Some("1"), Some("2"));
    celld::env_vars::validate().unwrap();
    init_v8();
    let config = worker_config();
    let pool = celld::pool::Pool::new(
        celld::runtime::pool_limits(),
        std::time::Duration::from_millis(10),
        Box::new(move || celld::js::Worker::load_config(config.clone())),
    );
    let residency = pool.place_cell().unwrap();
    assert_eq!(pool.census().cells, 1);
    drop(residency);
    pool.reap_empty();
    assert_eq!(pool.census().live, 0);
    assert!(pool.is_drained());
}

#[test]
fn burst_capacity_is_reclaimed_to_the_floor_and_drained_slots_are_reused() {
    let _environment = PoolEnvironment::new(Some("1"), Some("3"));
    celld::env_vars::validate().unwrap();
    init_v8();
    let mut limits = celld::runtime::pool_limits();
    limits.min_isolates = 1;
    // Make each admission ask for burst capacity without timing-dependent turns.
    limits.grow_at = 0;
    let config = worker_config();
    let pool = celld::pool::Pool::new(
        limits,
        std::time::Duration::from_millis(10),
        Box::new(move || celld::js::Worker::load_config(config.clone())),
    );
    pool.warm().unwrap();
    for _ in 0..3 {
        let first = pool.admit(false).unwrap();
        let second = pool.admit(false).unwrap();
        let at_ceiling = pool.admit(false).unwrap();
        assert_eq!(pool.census().live, 3);
        assert_eq!(pool.len(), 3);
        drop((first, second, at_ceiling));
        pool.reap();
        assert_eq!(pool.census().live, 2);
        pool.reap();
        assert_eq!(pool.census().live, 1);
        assert_eq!(pool.census().freed, 2);
        pool.reap();
        assert_eq!(pool.census().live, 1);
    }
}

#[test]
fn node_pressure_can_reclaim_the_warm_minimum_and_requests_can_rebuild_it() {
    use celld::ownership_store::{set_node_load, LiveLoad};
    use std::sync::atomic::Ordering;

    struct Pressure(Arc<LiveLoad>);
    impl Drop for Pressure {
        fn drop(&mut self) {
            self.0.pressured.store(false, Ordering::Relaxed);
        }
    }

    let _environment = PoolEnvironment::new(Some("1"), Some("2"));
    let runtime = start();
    let load = Arc::new(LiveLoad::default());
    let pressure = Pressure(load.clone());
    set_node_load(load.clone());
    load.pressured.store(true, Ordering::Relaxed);
    runtime.isolates.reap();
    assert_eq!(runtime.isolates.census().live, 0);
    assert!(runtime.isolates.is_drained());
    drop(pressure);
    let request = runtime.isolates.admit(false).unwrap();
    assert_eq!(runtime.isolates.census().live, 1);
    drop(request);
    runtime.isolates.reap();
    assert_eq!(runtime.isolates.census().live, 1);
}

#[test]
fn concurrent_admission_and_maintenance_preserve_affiliations_and_pool_bounds() {
    let _environment = PoolEnvironment::new(Some("1"), Some("2"));
    let runtime = start();
    let pool = runtime.isolates;
    let started = std::sync::Barrier::new(4);
    std::thread::scope(|threads| {
        for _ in 0..3 {
            let pool = &pool;
            let started = &started;
            threads.spawn(move || {
                let executor = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                started.wait();
                for _ in 0..40 {
                    let request = pool.admit(false).unwrap();
                    executor.block_on(request.slot().turn(|worker| {
                        assert!(worker.heap_bytes().is_some());
                    }));
                    drop(request);
                    std::thread::yield_now();
                }
            });
        }
        let pool = &pool;
        let started = &started;
        threads.spawn(move || {
            started.wait();
            for _ in 0..120 {
                pool.reap();
                assert!((1..=2).contains(&pool.census().live));
                std::thread::yield_now();
            }
        });
    });
    pool.reap();
    pool.reap();
    assert_eq!(pool.census().live, 1);
    assert_eq!(pool.census().requests, 0);
    assert_eq!(pool.census().turns, 0);
}

#[test]
fn unset_or_zero_minimum_allows_idle_pools_to_empty() {
    for minimum in [None, Some("0")] {
        let _environment = PoolEnvironment::new(minimum, Some("1"));
        let runtime = start();
        assert_eq!(runtime.isolates.census().live, 1);
        runtime.isolates.reap();
        assert_eq!(runtime.isolates.census().live, 0);
        assert!(runtime.isolates.is_drained());
    }
}

#[test]
fn invalid_minimum_is_rejected_before_runtime_startup() {
    for minimum in ["", "-1", "invalid", "3", "184467440737095516160"] {
        let _environment = PoolEnvironment::new(Some(minimum), Some("2"));
        let error = celld::env_vars::validate().unwrap_err().to_string();
        assert!(error.contains("CELLD_MIN_STATELESS_ISOLATES"), "{error}");
    }
    let cores = std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(4);
    let minimum = (cores + 1).to_string();
    let _environment = PoolEnvironment::new(Some(&minimum), None);
    let error = celld::env_vars::validate().unwrap_err().to_string();
    assert!(error.contains("CELLD_MIN_STATELESS_ISOLATES"));
    assert!(error.contains("CELLD_MAX_STATELESS_ISOLATES"));
}
