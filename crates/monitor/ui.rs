// Copyright 2026 Deno Land Inc. Apache-2.0 license.

use celld_monitor::{Cell, ClusterSnapshot, History, Point, Snapshot, PAGE_SIZE, SCHEMA_VERSION};
use gloo_net::http::Request;
use gloo_timers::future::TimeoutFuture;
use leptos::prelude::*;
use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

#[path = "cluster_ui.rs"]
mod cluster_ui;
#[path = "traces_ui.rs"]
mod traces_ui;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Page {
    Overview,
    Cells,
    Isolates,
    Traces,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Sort {
    Cpu,
    Memory,
    Name,
}

fn bytes(value: Option<u64>) -> String {
    match value {
        None => "—".into(),
        Some(value) if value >= 1024 * 1024 => format!("{:.1} MiB", value as f64 / 1_048_576.0),
        Some(value) if value >= 1024 => format!("{:.1} KiB", value as f64 / 1024.0),
        Some(value) => format!("{value} B"),
    }
}

fn percent(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.1}%"))
        .unwrap_or_else(|| "—".into())
}

fn api_url(search: &str, class: &str, offset: usize, node: &str) -> String {
    // encodeURIComponent is accessed through its generated Rust binding; there
    // is no application JavaScript, and user input never enters markup.
    let q: String = js_sys::encode_uri_component(search).into();
    let class: String = js_sys::encode_uri_component(class).into();
    let node: String = js_sys::encode_uri_component(node).into();
    format!("/monitor/cluster?q={q}&class={class}&offset={offset}&limit={PAGE_SIZE}&node={node}")
}

#[component]
pub fn App() -> impl IntoView {
    let snapshot = RwSignal::new(None::<Snapshot>);
    let cluster = RwSignal::new(None::<ClusterSnapshot>);
    let histories = RwSignal::new(BTreeMap::<String, History>::new());
    let node = RwSignal::new(String::new());
    let inventory = RwSignal::new(Vec::<String>::new());
    let trace_cell = RwSignal::new(String::new());
    let history = RwSignal::new(History::default());
    let selected = RwSignal::new(None::<(String, u64)>);
    let fragment = traces_ui::location_fragment();
    let linked = fragment.as_deref().is_some_and(|fragment| {
        fragment == "#traces" || celld_monitor::TraceLink::parse(fragment).is_some()
    });
    let page = RwSignal::new(if linked { Page::Traces } else { Page::Cells });
    let sort = RwSignal::new(Sort::Cpu);
    let search = RwSignal::new(String::new());
    let class = RwSignal::new(String::new());
    let offset = RwSignal::new(0usize);
    let paused = RwSignal::new(false);
    let error = RwSignal::new(None::<String>);
    let received = RwSignal::new(None::<f64>);
    let now = RwSignal::new(js_sys::Date::now());
    let alive = Arc::new(AtomicBool::new(true));
    let cleanup = alive.clone();
    on_cleanup(move || cleanup.store(false, Ordering::Relaxed));
    wasm_bindgen_futures::spawn_local(async move {
        while alive.load(Ordering::Relaxed) {
            now.set(js_sys::Date::now());
            if paused.get_untracked() {
                TimeoutFuture::new(500).await;
                continue;
            }
            let url = api_url(
                &search.get_untracked(),
                &class.get_untracked(),
                offset.get_untracked(),
                &node.get_untracked(),
            );
            let result = async {
                let response = Request::get(&url).send().await.map_err(|error| error.to_string())?;
                if !response.ok() {
                    return Err(format!("Monitoring API returned HTTP {}. Check CELLD_MONITOR=1 and the node connection.", response.status()));
                }
                let snapshot = response.json::<ClusterSnapshot>().await.map_err(|error| error.to_string())?;
                if snapshot.schema_version != SCHEMA_VERSION {
                    return Err("Dashboard/API versions differ. Reload after rebuilding celld.".into());
                }
                Ok(snapshot)
            }.await;
            if !alive.load(Ordering::Relaxed) {
                break;
            }
            // A search/page change during the request cannot install an old page.
            if url
                == api_url(
                    &search.get_untracked(),
                    &class.get_untracked(),
                    offset.get_untracked(),
                    &node.get_untracked(),
                )
            {
                match result {
                    Ok(next) => {
                        if node.get_untracked().is_empty() {
                            inventory.set(next.nodes.iter().map(|node| node.id.clone()).collect());
                        }
                        histories.update(|histories| {
                            for member in &next.nodes {
                                if let Some(snapshot) = &member.snapshot {
                                    histories
                                        .entry(member.id.clone())
                                        .or_default()
                                        .observe(snapshot);
                                }
                            }
                            // Bound history even if the cluster inventory churns.
                            while histories.len() > 64 {
                                let key = histories
                                    .keys()
                                    .find(|key| !inventory.get_untracked().contains(key))
                                    .cloned()
                                    .or_else(|| histories.keys().next().cloned())
                                    .unwrap();
                                histories.remove(&key);
                            }
                        });
                        let current = next
                            .nodes
                            .iter()
                            .find(|member| member.id == node.get_untracked())
                            .or_else(|| next.nodes.first())
                            .and_then(|member| member.snapshot.clone());
                        if selected.get_untracked().is_none() {
                            selected.set(current.as_ref().and_then(|next| {
                                next.cells
                                    .first()
                                    .map(|cell| (cell.id.clone(), cell.residency))
                            }));
                        }
                        history.set(
                            current
                                .as_ref()
                                .and_then(|current| {
                                    histories
                                        .with_untracked(|all| all.get(&current.node.id).cloned())
                                })
                                .unwrap_or_default(),
                        );
                        snapshot.set(current);
                        cluster.set(Some(next));
                        received.set(Some(js_sys::Date::now()));
                        error.set(None);
                    }
                    Err(message) => error.set(Some(message)),
                }
            }
            now.set(js_sys::Date::now());
            TimeoutFuture::new(2_000).await;
        }
    });

    let selected_cell = Memo::new(move |_| {
        let (id, residency) = selected.get()?;
        snapshot.with(|snapshot| {
            snapshot
                .as_ref()?
                .cells
                .iter()
                .find(|cell| cell.id == id && cell.residency == residency)
                .cloned()
        })
    });

    view! {
        <div class="shell">
            <aside class="sidebar">
                <a class="brand" href="/monitor/"><Icon name="cube"/><span><strong>"celld"</strong>" monitor"</span></a>
                <nav aria-label="Monitoring views">
                    <NavButton label="Overview" icon="overview" target=Page::Overview page/>
                    <NavButton label="Cells" icon="cells" target=Page::Cells page/>
                    <NavButton label="Isolates" icon="chip" target=Page::Isolates page/>
                    <NavButton label="Traces" icon="trace" target=Page::Traces page/>
                </nav>
                <div class="sidebar-bottom">
                    <div class="nav muted"><Icon name="history"/>"Session history"</div>
                    <p class="sidebar-note">"Last 10 minutes while this tab is open. Not persisted."</p>
                    <div class="node-label"><span class="status-dot"></span><span>{move || cluster.with(|s| s.as_ref().map(|s| format!("{} known nodes", s.known_nodes)).unwrap_or_else(|| "Connecting…".into()))}</span></div>
                    <span class="muted small">"Rust + WebAssembly"</span>
                </div>
            </aside>
            <main>
                <header class="topbar">
                    <div class="title-group"><h1>{move || match page.get() { Page::Overview => "Cluster overview", Page::Cells => "Cells", Page::Isolates => "Isolates", Page::Traces => "Traces" }}</h1><span class="badge">"READ ONLY"</span></div>
                    <div class="topbar-right">
                        <span class="muted small">"10 min · session"</span>
                        <button class="button compact" on:click=move |_| paused.update(|paused| *paused = !*paused)>{move || if paused.get() { "Resume" } else { "Pause updates" }}</button>
                        <span class="freshness" class:offline=move || error.get().is_some()>{move || {
                            if error.get().is_some() { return "Disconnected · showing last sample".into(); }
                            if paused.get() { return "Updates paused".into(); }
                            received.get().map(|received| format!("Updated {:.0}s ago", ((now.get() - received) / 1000.0).max(0.0)))
                                .unwrap_or_else(|| "Connecting…".into())
                        }}</span>
                    </div>
                </header>
                {move || error.get().map(|message| view! { <div class="notice error" role="alert">{message}</div> })}
                <div class="scope-bar"><strong>"Cluster scope"</strong>
                    <select aria-label="Filter by node" prop:value=move || node.get() on:change=move |event| { node.set(event_target_value(&event)); offset.set(0); selected.set(None); snapshot.set(None); }>
                        <option value="">"All nodes"</option>
                        {move || inventory.get().into_iter().map(|id| view! { <option value=id.clone()>{id.clone()}</option> }).collect_view()}
                    </select>
                    <span class="muted small">{move || cluster.with(|s| s.as_ref().map(|s| format!("{} / {} nodes responding · membership {}", s.nodes.iter().filter(|node| node.snapshot.is_some()).count(), if node.get().is_empty() { s.known_nodes } else { s.nodes.len() }, s.discovery_age_ms.map(|ms| format!("{:.0}s old", ms as f64 / 1000.0)).unwrap_or_else(|| "standalone".into()))).unwrap_or_else(|| "Discovering fleet…".into()))}</span>
                </div>
                {move || cluster.with(|s| s.as_ref().filter(|s| s.partial).map(|s| view! { <div class="notice error" role="status">{format!("Partial cluster data. {}", s.discovery_error.as_deref().unwrap_or("Some nodes are unreachable or the bounded collection limit was reached. See Overview for node status."))}</div> }))}
                <div class="summary-grid">
                    <Summary label="Process CPU" icon="chip" value=Signal::derive(move || cluster_ui::cpu(cluster.get().as_ref())) detail=Signal::derive(|| "Sum · percent of one core".into())/>
                    <Summary label="Process RSS" icon="memory" value=Signal::derive(move || cluster_ui::rss(cluster.get().as_ref())) detail=Signal::derive(|| "Sum across responding nodes".into())/>
                    <Summary label="Resident cells" icon="cube" value=Signal::derive(move || cluster_ui::residents(cluster.get().as_ref())) detail=Signal::derive(|| "Responding nodes · not unique IDs".into())/>
                    <Summary label="Active isolates" icon="chip" value=Signal::derive(move || cluster_ui::isolates(cluster.get().as_ref())) detail=Signal::derive(|| "Cells, Workers and services".into())/>
                </div>
                {move || match page.get() {
                    Page::Cells if node.get().is_empty() => view! { <cluster_ui::Cells cluster histories sort search class offset node snapshot history selected/> }.into_any(),
                    Page::Cells => view! {
                        <div class="content-grid">
                            <div class="left-column">
                                <section class="panel">
                                    <div class="panel-heading filters"><h2>"Resident cells"</h2>
                                        <div class="filter-controls">
                                            <input type="search" placeholder="Search cells…" aria-label="Search cells" prop:value=move || search.get()
                                                on:input=move |event| { search.set(event_target_value(&event)); offset.set(0); selected.set(None); }/>
                                            <select aria-label="Filter by class" prop:value=move || class.get() on:change=move |event| { class.set(event_target_value(&event)); offset.set(0); selected.set(None); }>
                                                <option value="">"All classes"</option>
                                                {move || snapshot.with(|s| s.as_ref().map(|s| s.classes.clone()).unwrap_or_default()).into_iter().map(|class| view! { <option value=class.clone()>{class.clone()}</option> }).collect_view()}
                                            </select>
                                        </div>
                                    </div>
                                    <div class="table-scroll"><table class="cell-table"><thead><tr>
                                        <th><button class="sort" on:click=move |_| sort.set(Sort::Name)>"Cell"</button></th>
                                        <th><button class="sort" on:click=move |_| sort.set(Sort::Cpu)>"Turn CPU / core"</button></th>
                                        <th><button class="sort" on:click=move |_| sort.set(Sort::Memory)>"Tracked memory"</button></th>
                                        <th>"Isolate"</th><th>"State"</th>
                                    </tr></thead><tbody>
                                        <For each=move || {
                                            let mut cells = snapshot.with(|s| s.as_ref().map(|s| s.cells.clone()).unwrap_or_default());
                                            let order = sort.get();
                                            history.with(|history| cells.sort_by(|a, b| match order {
                                                Sort::Name => a.id.cmp(&b.id),
                                                Sort::Memory => b.tracked_bytes().cmp(&a.tracked_bytes()).then(a.id.cmp(&b.id)),
                                                Sort::Cpu => history.cpu_percent(b).unwrap_or(-1.0).total_cmp(&history.cpu_percent(a).unwrap_or(-1.0)).then(a.id.cmp(&b.id)),
                                            }));
                                            cells
                                        } key=|cell| (cell.id.clone(), cell.residency) children=move |cell| view! {
                                            <CellRow id=cell.id residency=cell.residency snapshot selected history/>
                                        }/>
                                    </tbody></table></div>
                                    {move || snapshot.with(|s| s.as_ref().is_some_and(|s| s.cells.is_empty())).then(|| view! {
                                        <div class="empty"><Icon name="cube"/><h3>"No matching resident cells"</h3><p>"Send a request to your Worker, or clear the filters. Dormant cells are not activated by monitoring."</p></div>
                                    })}
                                    <div class="table-footer"><span class="muted small">"Ranked within this page · SQLite + owned native buffers; shared heap excluded."</span>
                                        <div class="pagination">
                                            <button class="button compact" disabled=move || offset.get() == 0 on:click=move |_| { offset.update(|offset| *offset = offset.saturating_sub(PAGE_SIZE)); selected.set(None); }>"Previous"</button>
                                            <span class="small">{move || snapshot.with(|s| s.as_ref().map(|s| format!("{}–{} of {}", if s.cells.is_empty() { 0 } else { s.offset + 1 }, s.offset + s.cells.len(), s.total_matches)).unwrap_or_else(|| "Loading…".into()))}</span>
                                            <button class="button compact" disabled=move || snapshot.with(|s| s.as_ref().is_none_or(|s| s.offset + s.cells.len() >= s.total_matches)) on:click=move |_| { offset.update(|offset| *offset += PAGE_SIZE); selected.set(None); }>"Next"</button>
                                        </div>
                                    </div>
                                </section>
                                {move || selected_cell.get().map(|cell| {
                                    let points = history.with(|history| history.points(&cell));
                                    view! { <Chart title="Scheduled-turn CPU" subtitle="Percent of one core · completed turns only" name=cell.id.clone() points memory=false/> }
                                })}
                                <section class="panel quiet"><div class="panel-heading"><h2>"Trace drill-down"</h2><span class="badge">"BUCKET HISTORY"</span></div><p class="muted">"Search persisted spans and correlated console logs across the fleet."</p>
                                    <button class="button" on:click=move |_| { trace_cell.set(selected_cell.get().map(|cell| cell.id).unwrap_or_default()); page.set(Page::Traces); }>"Query cell traces"</button>
                                </section>
                            </div>
                            <div class="right-column">
                                {move || selected_cell.get().map(|cell| view! { <CellDetail cell snapshot history/> }.into_any()).unwrap_or_else(|| view! {
                                    <section class="panel empty detail-empty"><Icon name="cells"/><h2>"Select a cell"</h2><p>"Its CPU, tracked native memory, and shared isolate will appear here. Cells that leave residency disappear from the live list."</p></section>
                                }.into_any())}
                            </div>
                        </div>
                    }.into_any(),
                    Page::Isolates => view! { <cluster_ui::Isolates cluster/> }.into_any(),
                    Page::Overview => view! { <cluster_ui::Overview cluster node offset selected/> }.into_any(),
                    Page::Traces => view! { <traces_ui::Traces initial_cell=trace_cell/> }.into_any(),
                }}
                <footer class="page-footer"><span>"Native Rust measurements · Rust/WASM dashboard"</span><span>"CPU attribution: scheduled turn, including foreground GC and foreign microtasks."</span></footer>
            </main>
        </div>
    }
}

#[component]
fn NavButton(
    label: &'static str,
    icon: &'static str,
    target: Page,
    page: RwSignal<Page>,
) -> impl IntoView {
    view! { <button class="nav" aria-label=label title=label class:active=move || page.get() == target on:click=move |_| page.set(target)><Icon name=icon/>{label}</button> }
}

#[component]
fn Summary(
    label: &'static str,
    icon: &'static str,
    value: Signal<String>,
    detail: Signal<String>,
) -> impl IntoView {
    view! { <section class="summary"><Icon name=icon/><div><div class="summary-label">{label}</div><strong>{move || value.get()}</strong><span class="muted small">{move || detail.get()}</span></div></section> }
}

#[component]
fn CellRow(
    id: String,
    residency: u64,
    snapshot: RwSignal<Option<Snapshot>>,
    selected: RwSignal<Option<(String, u64)>>,
    history: RwSignal<History>,
) -> impl IntoView {
    let id = StoredValue::new(id);
    let cell = Memo::new(move |_| {
        snapshot.with(|s| {
            s.as_ref()?
                .cells
                .iter()
                .find(|cell| cell.id == id.get_value() && cell.residency == residency)
                .cloned()
        })
    });
    view! {
        <tr class:selected=move || selected.get().is_some_and(|key| key == (id.get_value(), residency))>
            <td><button class="cell-link" title=move || id.get_value() on:click=move |_| selected.set(Some((id.get_value(), residency)))>{move || id.get_value()}</button></td>
            <td class="numeric">{move || percent(cell.get().and_then(|cell| history.with(|history| history.cpu_percent(&cell))))}</td>
            <td class="numeric" class:stale=move || cell.get().is_none_or(|cell| cell.memory_stale)>{move || cell.get().map(|cell| {
                let value = bytes(cell.tracked_bytes());
                if cell.memory_stale && cell.sqlite.is_some() { format!("{value} · stale") } else { value }
            }).unwrap_or_else(|| "—".into())}</td>
            <td>{move || cell.get().map(|cell| format!("#{}", cell.isolate)).unwrap_or_default()}</td>
            <td><span class="state">{move || cell.get().map(|cell| if cell.starting { "Starting" } else if cell.turns.running + cell.turns.queued + cell.in_flight_requests > 0 { "Active" } else { "Resident" }).unwrap_or("Gone")}</span></td>
        </tr>
    }
}

#[component]
fn CellDetail(
    cell: Cell,
    snapshot: RwSignal<Option<Snapshot>>,
    history: RwSignal<History>,
) -> impl IntoView {
    let cpu = history.with(|history| history.cpu_percent(&cell));
    let points = history.with(|history| history.points(&cell));
    let isolate = snapshot.with(|s| {
        s.as_ref()?
            .isolates
            .iter()
            .find(|isolate| isolate.id == cell.isolate)
            .cloned()
    });
    let sample_age = snapshot.with(|s| {
        s.as_ref().and_then(|s| {
            cell.memory_sampled_unix_ms
                .map(|at| s.sampled_unix_ms.saturating_sub(at) / 1000)
        })
    });
    let sqlite = cell.sqlite.clone();
    view! {
        <section class="panel detail">
            <div class="detail-title"><h2 title=cell.id.clone()>{cell.id.clone()}</h2><span class="state">{if cell.starting { "Starting" } else { "Resident" }}</span></div>
            <p class="muted small">{format!("{} · epoch {} · generation {}", cell.script, cell.epoch, cell.generation)}</p>
            <div class="detail-stats"><div><span>"Turn CPU"</span><strong>{percent(cpu)}</strong><small>"of one core"</small></div><div><span>"Fetch/RPC in flight"</span><strong>{cell.in_flight_requests}</strong><small>{format!("{} queued turns", cell.turns.queued)}</small></div></div>
            <h3>"Tracked cell memory"</h3>
            <dl>
                <dt>"SQLite cache"</dt><dd>{bytes(sqlite.as_ref().map(|s| s.cache_bytes))}</dd>
                <dt>"SQLite schema"</dt><dd>{bytes(sqlite.as_ref().map(|s| s.schema_bytes))}</dd>
                <dt>"Prepared statements"</dt><dd>{bytes(sqlite.as_ref().map(|s| s.statement_bytes))}</dd>
                <dt>"HTTP stream buffers"</dt><dd>{bytes(cell.buffers.as_ref().map(|b| b.http_stream_bytes))}</dd>
                <dt>"WebSocket buffers"</dt><dd>{bytes(cell.buffers.as_ref().map(|b| b.websocket_bytes))}</dd>
                <dt class="total">"Tracked native total"</dt><dd class="total">{bytes(cell.tracked_bytes())}</dd>
            </dl>
            <p class="muted small">{sample_age.map(|age| format!("Memory sample: {age}s old{}", if cell.memory_stale { " · stale / isolate busy" } else { "" })).unwrap_or_else(|| "Memory not sampled yet; isolate may be busy.".into())}</p>
            <p class="muted small">"Buffer counters are live, including while the isolate is busy. Native payload capacity only; JS queues, socket/TLS buffers and queue overhead are excluded."</p>
            <div class="shared-heap"><h3>{format!("Shared isolate #{}", cell.isolate)}</h3>
                <dl>
                    <dt>"Heap used"</dt><dd>{bytes(isolate.as_ref().and_then(|i| i.memory.as_ref().map(|m| m.heap_used_bytes)))}</dd>
                    <dt>"Heap committed"</dt><dd>{bytes(isolate.as_ref().and_then(|i| i.memory.as_ref().map(|m| m.heap_physical_bytes)))}</dd>
                    <dt>"External memory"</dt><dd>{bytes(isolate.as_ref().and_then(|i| i.memory.as_ref().map(|m| m.external_bytes)))}</dd>
                    <dt>"Cells sharing isolate"</dt><dd>{isolate.as_ref().map(|i| i.resident_cells.to_string()).unwrap_or_else(|| "—".into())}</dd>
                </dl>
                <p class="shared-warning"><Icon name="warning"/>"Shared JS heap is not attributable to this cell."</p>
                {isolate.as_ref().is_some_and(|i| i.memory_stale).then(|| view! { <p class="small stale">"Shared heap sample is stale or unavailable."</p> })}
            </div>
            <Chart title="Tracked native memory" subtitle="SQLite + owned buffers · this browser session" name="".into() points memory=true/>
            <h3>"Investigate"</h3>
            <button class="button investigate" disabled title="V8 Inspector integration is not implemented yet"><Icon name="chip"/>"Inspect isolate"<span class="soon">"Not implemented"</span></button>
            <div class="button-row"><button class="button" disabled>"CPU profile"</button><button class="button" disabled>"Heap snapshot"</button></div>
            <p class="muted small">"Profiling and debugger attachment are a later slice. Pausing an isolate would affect all its cells."</p>
        </section>
    }.into_any()
}

#[component]
fn IsolateTable(snapshot: Signal<Option<Snapshot>>) -> impl IntoView {
    view! {
        <section class="panel isolate-panel"><div class="panel-heading"><h2>"Shared isolates"</h2><span class="muted small">"No per-cell heap estimates"</span></div>
            <div class="table-scroll"><table><thead><tr><th>"Isolate"</th><th>"Script / kind"</th><th>"Cells"</th><th>"Heap used"</th><th>"Committed"</th><th>"External"</th><th>"Sample"</th></tr></thead><tbody>
                {move || snapshot.with(|s| s.as_ref().map(|s| s.isolates.clone()).unwrap_or_default()).into_iter().map(|isolate| {
                    let memory = isolate.memory.as_ref();
                    view! { <tr><td>{format!("#{}", isolate.id)}</td><td><strong>{isolate.script}</strong><span class="muted small sublabel">{format!("{} · generation {}{}", isolate.kind, isolate.generation, if isolate.retiring { " · retiring" } else { "" })}</span></td><td title=isolate.cells.join(", ")>{isolate.resident_cells}</td><td class="numeric">{bytes(memory.map(|m| m.heap_used_bytes))}</td><td class="numeric">{bytes(memory.map(|m| m.heap_physical_bytes))}</td><td class="numeric">{bytes(memory.map(|m| m.external_bytes))}</td><td class:stale=isolate.memory_stale>{if memory.is_none() { "Unavailable" } else if isolate.memory_stale { "Stale / busy" } else { "Fresh" }}</td></tr> }
                }).collect_view()}
            </tbody></table></div>
            <p class="panel-note muted">"Heap used, committed heap, external memory and RSS are separate measurements—not an additive memory total. Busy isolates keep their previous sample."</p>
        </section>
    }
}

#[component]
fn Chart(
    title: &'static str,
    subtitle: &'static str,
    name: String,
    points: Vec<Point>,
    memory: bool,
) -> impl IntoView {
    let values = points
        .iter()
        .map(|point| {
            if memory {
                point.native_bytes.map(|bytes| bytes as f64 / 1_048_576.0)
            } else {
                point.cpu_percent
            }
        })
        .collect::<Vec<_>>();
    let max = values
        .iter()
        .flatten()
        .copied()
        .fold(if memory { 0.01f64 } else { 1.0f64 }, f64::max)
        * 1.15;
    let first = points.first().map_or(0, |point| point.at_ms);
    let last = points.last().map_or(first, |point| point.at_ms);
    let duration = last.saturating_sub(first).max(1) as f64;
    let mut path = String::new();
    let mut drawing = false;
    for (point, value) in points.iter().zip(&values) {
        if let Some(value) = value {
            let x = 12.0 + point.at_ms.saturating_sub(first) as f64 / duration * 636.0;
            let y = 146.0 - value / max * 128.0;
            path.push_str(&format!(
                "{}{x:.2},{y:.2} ",
                if drawing { "L" } else { "M" }
            ));
            drawing = true;
        } else {
            drawing = false;
        }
    }
    let enough = values.iter().flatten().count() >= 2;
    view! {
        <section class="panel chart"><div class="panel-heading"><div><h2>{title}</h2><p class="muted small">{subtitle}</p></div><span class="chart-name small" title=name.clone()>{name.clone()}</span></div>
            <div class="chart-scale muted small">{format!("{max:.1}{}", if memory { " MiB" } else { "%" })}</div>
            <div class="plot"><svg viewBox="0 0 660 164" role="img" aria-label=title preserveAspectRatio="none">
                <path class="grid-lines" d="M12 18H648 M12 82H648 M12 146H648"/>
                <path class="chart-line" d=path/>
            </svg>{(!enough).then(|| view! { <span class="chart-empty">"Collecting samples…"</span> })}</div>
            <div class="chart-axis muted small"><span>{format!("{:.0}s ago", (last - first) as f64 / 1000.0)}</span><span>"Now"</span></div>
        </section>
    }
}

#[component]
fn Icon(name: &'static str) -> impl IntoView {
    let path = match name {
        "cube" => "M12 3 3 8v9l9 5 9-5V8L12 3ZM3 8l9 5 9-5M12 13v9",
        "overview" => "m3 10 9-7 9 7M5 9v12h5v-7h4v7h5V9",
        "cells" => "M3 5h18v14H3V5Zm4 4h1m4 0h5M7 14h1m4 0h5",
        "memory" => "M3 6h18v12H3V6Zm4 4v4m5-4v4m5-4v4M6 18v3m4-3v3m4-3v3m4-3v3",
        "trace" => "M5 3h10l4 4v14H5V3Zm9 0v5h5M8 12h8m-8 4h6",
        "history" => "M3 12a9 9 0 1 0 3-6M3 3v5h5m4-1v6l4 2",
        "warning" => "m12 3 10 18H2L12 3Zm0 7v5m0 3h.01",
        _ => "M6 6h12v12H6V6ZM9 2v4m6-4v4M9 18v4m6-4v4M2 9h4m-4 6h4m12-6h4m-4 6h4",
    };
    view! { <svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d=path/></svg> }
}
