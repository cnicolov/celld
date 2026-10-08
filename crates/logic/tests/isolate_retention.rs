use celld_logic::isolate::{may_free, retire, IsolateLoad, PoolLimits, PoolLoad};

fn pool(minimum: usize, isolates: Vec<IsolateLoad>) -> PoolLoad {
    PoolLoad {
        isolates,
        limits: PoolLimits {
            grow_at: 2,
            shrink_under: 1,
            min_isolates: minimum,
            max_stateless: 4,
            max_requests: None,
            max_cells: 32,
        },
    }
}

#[test]
fn zero_minimum_can_retire_the_last_idle_isolate() {
    let load = pool(0, vec![IsolateLoad::default()]);
    assert_eq!(retire(&load, false), Some(0));
}

#[test]
fn quiet_maintenance_keeps_the_configured_number_of_live_isolates() {
    for minimum in 1..=3 {
        let mut load = pool(minimum, vec![IsolateLoad::default(); 4]);
        let mut retired = 0;
        while let Some(id) = retire(&load, false) {
            assert!(!load.isolates[id].retiring);
            load.isolates[id].retiring = true;
            retired += 1;
        }
        assert_eq!(retired, 4 - minimum);
        assert_eq!(
            load.isolates.iter().filter(|slot| !slot.retiring).count(),
            minimum
        );
    }
}

#[test]
fn already_retiring_slots_do_not_count_toward_the_warm_minimum() {
    let load = pool(
        1,
        vec![
            IsolateLoad {
                retiring: true,
                ..Default::default()
            },
            IsolateLoad::default(),
        ],
    );
    assert_eq!(retire(&load, false), None);
}

#[test]
fn an_io_suspension_does_not_allow_normal_maintenance_below_the_minimum() {
    let load = pool(
        1,
        vec![IsolateLoad {
            requests: 2,
            ..Default::default()
        }],
    );
    assert_eq!(retire(&load, false), None);
}

#[test]
fn memory_pressure_may_reclaim_the_minimum_but_not_an_executing_last_turn() {
    let mut load = pool(1, vec![IsolateLoad::default()]);
    assert_eq!(retire(&load, true), Some(0));
    load.isolates[0].turns = 1;
    assert_eq!(retire(&load, true), None);
}

#[test]
fn housed_cells_are_never_retirement_candidates_even_under_pressure() {
    let load = pool(
        0,
        vec![IsolateLoad {
            cells: 1,
            ..Default::default()
        }],
    );
    assert_eq!(retire(&load, false), None);
    assert_eq!(retire(&load, true), None);
}

#[test]
fn retired_heaps_wait_for_requests_turns_and_cells_to_drain() {
    let mut slot = IsolateLoad {
        retiring: true,
        requests: 1,
        ..Default::default()
    };
    assert!(!may_free(&slot));
    slot.requests = 0;
    slot.turns = 1;
    assert!(!may_free(&slot));
    slot.turns = 0;
    slot.cells = 1;
    assert!(!may_free(&slot));
    slot.cells = 0;
    assert!(may_free(&slot));
}
