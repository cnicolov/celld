// Copyright 2026 Deno Land Inc. Apache-2.0 license.
use super::*;

pub(super) fn cpu(cluster: Option<&ClusterSnapshot>) -> String {
    percent(cluster.and_then(|cluster| {
        let values = cluster
            .nodes
            .iter()
            .filter_map(|node| node.snapshot.as_ref())
            .map(|s| s.node.cpu_percent_of_one_core)
            .collect::<Option<Vec<_>>>()?;
        (!values.is_empty()).then(|| values.iter().sum())
    }))
}
pub(super) fn rss(cluster: Option<&ClusterSnapshot>) -> String {
    bytes(cluster.and_then(|cluster| {
        let values = cluster
            .nodes
            .iter()
            .filter_map(|node| node.snapshot.as_ref())
            .map(|s| s.node.rss_bytes)
            .collect::<Option<Vec<_>>>()?;
        (!values.is_empty()).then(|| values.iter().sum())
    }))
}
pub(super) fn residents(cluster: Option<&ClusterSnapshot>) -> String {
    cluster
        .map(|c| {
            c.nodes
                .iter()
                .filter_map(|n| n.snapshot.as_ref())
                .map(|s| s.node.resident_cells)
                .sum::<usize>()
                .to_string()
        })
        .unwrap_or_else(|| "—".into())
}
pub(super) fn isolates(cluster: Option<&ClusterSnapshot>) -> String {
    cluster
        .map(|c| {
            c.nodes
                .iter()
                .filter_map(|n| n.snapshot.as_ref())
                .map(|s| s.isolates.iter().filter(|i| !i.retiring).count())
                .sum::<usize>()
                .to_string()
        })
        .unwrap_or_else(|| "—".into())
}

#[component]
pub(super) fn Overview(
    cluster: RwSignal<Option<ClusterSnapshot>>,
    node: RwSignal<String>,
    offset: RwSignal<usize>,
    selected: RwSignal<Option<(String, u64)>>,
) -> impl IntoView {
    view! {
        <section class="panel"><div class="panel-heading"><h2>"Fleet nodes"</h2><span class="badge">"AUTHENTICATED PEER SNAPSHOTS"</span></div>
            <div class="table-scroll"><table><thead><tr><th>"Node"</th><th>"Status"</th><th>"CPU / core"</th><th>"RSS"</th><th>"Cells"</th><th>"Deployment"</th></tr></thead><tbody>
                {move || cluster.get().map(|c| c.nodes).unwrap_or_default().into_iter().map(|member| {
                    let s = member.snapshot.as_ref();
                    let id = member.id.clone();
                    view! { <tr><td><button class="cell-link" on:click=move |_| { node.set(id.clone()); offset.set(0); selected.set(None); }>{member.id.clone()}</button><span class="sublabel muted small">{member.address}</span></td>
                        <td class:stale=member.error.is_some()>{member.error.clone().unwrap_or_else(|| if member.truncated { "Cell inventory truncated".into() } else if s.is_some_and(|s| s.node.pressured) { "Memory pressure".into() } else { "Healthy".into() })}</td>
                        <td>{percent(s.and_then(|s| s.node.cpu_percent_of_one_core))}</td><td>{bytes(s.and_then(|s| s.node.rss_bytes))}</td><td>{s.map(|s| s.node.resident_cells.to_string()).unwrap_or_else(|| "—".into())}</td><td>{s.map(|s| s.node.deployment.clone()).unwrap_or_else(|| "—".into())}</td>
                    </tr> }
                }).collect_view()}
            </tbody></table></div>
            <p class="panel-note muted">"Membership comes from the shared fleet sample. Unreachable nodes stay visible. Snapshots are collected independently, not atomically; the same cell may appear on two nodes during handoff. CPU is measured per process, not deduplicated host capacity."</p>
        </section>
    }
}

#[component]
pub(super) fn Cells(
    cluster: RwSignal<Option<ClusterSnapshot>>,
    histories: RwSignal<BTreeMap<String, History>>,
    sort: RwSignal<Sort>,
    search: RwSignal<String>,
    class: RwSignal<String>,
    offset: RwSignal<usize>,
    node: RwSignal<String>,
    snapshot: RwSignal<Option<Snapshot>>,
    history: RwSignal<History>,
    selected: RwSignal<Option<(String, u64)>>,
) -> impl IntoView {
    view! {
        <section class="panel"><div class="panel-heading filters"><h2>"Cells across the cluster"</h2><div class="filter-controls">
            <input type="search" placeholder="Search cells…" aria-label="Search cells" prop:value=move || search.get() on:input=move |event| { search.set(event_target_value(&event)); offset.set(0); }/>
            <select aria-label="Filter by class" prop:value=move || class.get() on:change=move |event| { class.set(event_target_value(&event)); offset.set(0); }><option value="">"All classes"</option>
                {move || cluster.get().map(|c| c.nodes.into_iter().filter_map(|n| n.snapshot).flat_map(|s| s.classes).collect::<std::collections::BTreeSet<_>>()).unwrap_or_default().into_iter().map(|class| view! { <option value=class.clone()>{class.clone()}</option> }).collect_view()}
            </select>
        </div></div>
        <div class="table-scroll"><table><thead><tr><th><button class="sort" on:click=move |_| sort.set(Sort::Name)>"Cell"</button></th><th>"Node"</th><th><button class="sort" on:click=move |_| sort.set(Sort::Cpu)>"Turn CPU / core"</button></th><th><button class="sort" on:click=move |_| sort.set(Sort::Memory)>"Tracked memory"</button></th><th>"Isolate"</th><th>"State"</th></tr></thead><tbody>
            {move || {
                let mut cells = cluster.get().map(|c| c.nodes.into_iter().filter_map(|n| n.snapshot).flat_map(|s| s.cells.into_iter().map(move |cell| (s.node.id.clone(), cell))).collect::<Vec<_>>()).unwrap_or_default();
                let cpu = |node: &str, cell: &Cell| histories.with(|all| all.get(node).and_then(|h| h.cpu_percent(cell)));
                cells.sort_by(|(an, a), (bn, b)| match sort.get() {
                    Sort::Name => a.id.cmp(&b.id).then(an.cmp(bn)),
                    Sort::Memory => b.tracked_bytes().cmp(&a.tracked_bytes()).then(a.id.cmp(&b.id)),
                    Sort::Cpu => cpu(bn, b).unwrap_or(-1.0).total_cmp(&cpu(an, a).unwrap_or(-1.0)).then(a.id.cmp(&b.id)),
                });
                cells.into_iter().map(|(id, cell)| {
                    let cpu = cpu(&id, &cell);
                    let target = id.clone();
                    let key = (cell.id.clone(), cell.residency);
                    view! { <tr><td><button class="cell-link" on:click=move |_| {
                        snapshot.set(cluster.get().and_then(|c| c.nodes.into_iter().find(|n| n.id == target).and_then(|n| n.snapshot)));
                        history.set(histories.with(|all| all.get(&target).cloned()).unwrap_or_default());
                        selected.set(Some(key.clone())); node.set(target.clone()); offset.set(0);
                    }>{cell.id.clone()}</button></td><td>{id}</td><td class="numeric">{percent(cpu)}</td><td class="numeric" class:stale=cell.memory_stale>{bytes(cell.tracked_bytes())}</td><td>{format!("#{}", cell.isolate)}</td><td><span class="state">{if cell.starting { "Starting" } else if cell.in_flight_requests + cell.turns.running + cell.turns.queued > 0 { "Active" } else { "Resident" }}</span></td></tr> }
                }).collect_view()
            }}
        </tbody></table></div>
        <div class="table-footer"><span class="muted small">"Ranked within this page · click a cell for node-scoped detail. Shared heap excluded."</span><div class="pagination">
            <button class="button compact" disabled=move || offset.get() == 0 on:click=move |_| offset.update(|o| *o = o.saturating_sub(PAGE_SIZE))>"Previous"</button>
            <span>{move || cluster.with(|c| c.as_ref().map(|c| format!("{} collected / {} matches", c.collected_matches, c.total_matches)).unwrap_or_default())}</span>
            <button class="button compact" disabled=move || cluster.with(|c| c.as_ref().is_none_or(|c| offset.get() + PAGE_SIZE >= c.collected_matches)) on:click=move |_| offset.update(|o| *o += PAGE_SIZE)>"Next"</button>
        </div></div></section>
    }
}

#[component]
pub(super) fn Isolates(cluster: RwSignal<Option<ClusterSnapshot>>) -> impl IntoView {
    view! { <div class="left-column">
        <For each=move || cluster.with(|c| c.as_ref().map(|c| c.nodes.iter().filter(|n| n.snapshot.is_some()).map(|n| n.id.clone()).collect::<Vec<_>>()).unwrap_or_default()) key=|id| id.clone() children=move |id| {
            let label = id.clone();
            let snapshot = Signal::derive(move || cluster.with(|c| c.as_ref()?.nodes.iter().find(|n| n.id == id)?.snapshot.clone()));
            view! { <div><h2 class="node-heading">{label}</h2><IsolateTable snapshot/></div> }
        }/>
    </div> }
}
