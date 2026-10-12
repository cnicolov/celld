// Copyright 2026 Deno Land Inc. Apache-2.0 license.
use super::*;
use celld_monitor::{TraceLink, TraceLog, TracePage, TraceSpan};

pub(super) fn location_fragment() -> Option<String> {
    let location = js_sys::Reflect::get(&js_sys::global(), &"location".into()).ok()?;
    js_sys::Reflect::get(&location, &"hash".into())
        .ok()?
        .as_string()
}

fn set_fragment(fragment: &str) {
    if let Ok(location) = js_sys::Reflect::get(&js_sys::global(), &"location".into()) {
        let _ = js_sys::Reflect::set(&location, &"hash".into(), &fragment.into());
    }
}

enum QueryAction {
    Form,
    DrillDown(String),
    Linked(TraceLink),
}

fn encoded(value: &str) -> String {
    js_sys::encode_uri_component(value).into()
}
fn time(us: i64) -> String {
    let date: String = js_sys::Date::new(&(us as f64 / 1000.0).into())
        .to_iso_string()
        .into();
    date.get(11..23).unwrap_or(&date).to_string()
}

#[component]
pub(super) fn Traces(initial_cell: RwSignal<String>) -> impl IntoView {
    let linked = location_fragment().as_deref().and_then(TraceLink::parse);
    let cell = RwSignal::new(initial_cell.get_untracked());
    let search = RwSignal::new(String::new());
    let trace = RwSignal::new(
        linked
            .as_ref()
            .map_or_else(String::new, |link| link.trace_id.clone()),
    );
    let node = RwSignal::new(String::new());
    let minutes = RwSignal::new("15".to_string());
    let duration = RwSignal::new("0".to_string());
    let errors = RwSignal::new(false);
    let logs_only = RwSignal::new(false);
    let showing_logs = RwSignal::new(false);
    let data = RwSignal::new(None::<TracePage>);
    let busy = RwSignal::new(false);
    let error = RwSignal::new(None::<String>);
    let detail = RwSignal::new(None::<String>);
    let alive = Arc::new(AtomicBool::new(true));
    let cleanup = alive.clone();
    on_cleanup(move || cleanup.store(false, Ordering::Relaxed));
    let run = Callback::new(move |action: QueryAction| {
        if busy.get_untracked() {
            return;
        }
        let (chosen, fixed_bounds, drill_down) = match action {
            QueryAction::Form => (trace.get_untracked(), None, false),
            QueryAction::DrillDown(id) => {
                let bounds = data.get_untracked().map(|page| {
                    format!(
                        "from={}&to={}",
                        page.from_unix_us / 1000,
                        page.to_unix_us / 1000
                    )
                });
                (id, bounds, true)
            }
            QueryAction::Linked(link) => (
                link.trace_id,
                Some(format!("from={}&to={}", link.from_unix_ms, link.to_unix_ms)),
                true,
            ),
        };
        let want_logs = logs_only.get_untracked() && !drill_down;
        let is_detail = !chosen.is_empty() && !want_logs;
        // A typed Trace ID is a new form query. Only a selected row/deep
        // link preserves the previous historic window.
        let bounds = fixed_bounds.unwrap_or_else(|| format!("minutes={}", minutes.get_untracked()));
        let url = if is_detail {
            format!("/monitor/traces?{bounds}&trace_id={}", encoded(&chosen))
        } else {
            format!(
                "/monitor/traces?{bounds}&q={}&cell={}&node={}&errors={}&min_ms={}&signal={}&trace_id={}",
                encoded(&search.get_untracked()),
                encoded(&cell.get_untracked()),
                encoded(&node.get_untracked()),
                u8::from(errors.get_untracked()),
                encoded(&duration.get_untracked()),
                if want_logs { "logs" } else { "traces" },
                encoded(&chosen),
            )
        };
        busy.set(true);
        error.set(None);
        data.set(None);
        showing_logs.set(want_logs);
        let selected_id = is_detail.then_some(chosen);
        detail.set(selected_id.clone());
        let alive = alive.clone();
        wasm_bindgen_futures::spawn_local(async move {
            let result = async {
                let response = Request::get(&url).send().await.map_err(|e| e.to_string())?;
                if !response.ok() {
                    let body = response.text().await.unwrap_or_default();
                    return Err(format!(
                        "Trace query returned HTTP {}: {body}",
                        response.status()
                    ));
                }
                response
                    .json::<TracePage>()
                    .await
                    .map_err(|e| e.to_string())
            }
            .await;
            if !alive.load(Ordering::Relaxed) {
                return;
            }
            busy.set(false);
            match result {
                Ok(page) => {
                    if let Some(trace_id) = selected_id {
                        let link = TraceLink {
                            trace_id,
                            from_unix_ms: page.from_unix_us / 1000,
                            to_unix_ms: page.to_unix_us / 1000,
                        };
                        set_fragment(&link.fragment());
                    } else {
                        set_fragment("#traces");
                    }
                    data.set(Some(page));
                }
                Err(message) => error.set(Some(message)),
            }
        });
    });
    run.run(linked.map_or(QueryAction::Form, QueryAction::Linked));

    view! {
        <section class="panel trace-query"><div class="panel-heading"><div><h2>"Query fleet traces"</h2><p class="muted small">"RustFS / S3 Parquet · persisted history across current and former nodes"</p></div><span class="badge">"ON-DEMAND QUERY"</span></div>
            <form class="trace-filters" on:submit=move |event| { event.prevent_default(); run.run(QueryAction::Form); }>
                <label>"Search spans / logs / attributes"<input type="search" placeholder="e.g. effect or /checkout" prop:value=move || search.get() on:input=move |e| search.set(event_target_value(&e))/></label>
                <label>"Cell contains"<input placeholder="e.g. Counter:alpha" prop:value=move || cell.get() on:input=move |e| cell.set(event_target_value(&e))/></label>
                <label>"Node ID"<input placeholder="All export nodes" prop:value=move || node.get() on:input=move |e| node.set(event_target_value(&e))/></label>
                <label>"Trace ID"<input placeholder="32 hex characters" prop:value=move || trace.get() on:input=move |e| trace.set(event_target_value(&e))/></label>
                <label>"Time window"<select prop:value=move || minutes.get() on:change=move |e| minutes.set(event_target_value(&e))><option value="1">"Last minute"</option><option value="5">"Last 5 minutes"</option><option value="15">"Last 15 minutes"</option><option value="60">"Last hour"</option><option value="1440">"Last 24 hours"</option></select></label>
                <label>"Minimum span duration (ms)"<input type="number" min="0" prop:value=move || duration.get() on:input=move |e| duration.set(event_target_value(&e))/></label>
                <label class="checkbox"><input type="checkbox" prop:checked=move || errors.get() on:change=move |e| errors.set(event_target_checked(&e))/>"Errors only"</label>
                <label class="checkbox"><input type="checkbox" prop:checked=move || logs_only.get() on:change=move |e| logs_only.set(event_target_checked(&e))/>"Logs only"</label>
                <button class="button primary" type="submit" disabled=move || busy.get()>{move || if busy.get() { "Scanning exports…" } else { "Run query" }}</button>
            </form>
            <p class="panel-note muted">"Trace filters match spans; opening a trace fetches its available spans and correlated logs across all nodes. Logs-only queries search messages and annotations independently of trace sampling (duration is ignored). This is a bounded browser, not an arbitrary SQL engine."</p>
        </section>
        {move || error.get().map(|error| view! { <div class="notice error" role="alert">{error}</div> })}
        {move || data.get().map(|page| {
            let matched = if showing_logs.get_untracked() { format!("{} matching logs", page.logs.len()) } else { format!("{} matching spans", page.matched_spans) };
            let info = format!("{} files scanned · {} · {}", page.scanned_files, matched, if page.truncated { "PARTIAL RESULTS" } else { "scan completed within limits" });
            let warnings = page.warnings.clone();
            view! { <div class="trace-results">
                <div class="notice"><strong>{info}</strong>{warnings.into_iter().map(|warning| view! { <p class="small muted">{warning}</p> }).collect_view()}</div>
                {if let Some(id) = detail.get() {
                    view! { <TraceDetail id page/> }.into_any()
                } else if showing_logs.get_untracked() {
                    view! { <LogRecords logs=page.logs title="Matching logs"/> }.into_any()
                } else {
                    let mut groups = BTreeMap::<String, Vec<TraceSpan>>::new();
                    for span in page.spans { groups.entry(span.trace_id.clone()).or_default().push(span); }
                    let mut groups = groups.into_iter().collect::<Vec<_>>();
                    groups.sort_by_key(|(_, spans)| std::cmp::Reverse(spans.iter().map(|s| s.start_unix_us).max().unwrap_or(0)));
                    let empty = groups.is_empty();
                    view! { <section class="panel"><div class="panel-heading"><h2>"Matching traces"</h2><span class="muted small">"Newest first · duration is longest matching span"</span></div>
                        <div class="table-scroll"><table><thead><tr><th>"Trace / span"</th><th>"Started (UTC)"</th><th>"Duration"</th><th>"Nodes"</th><th>"Spans"</th><th>"Status"</th></tr></thead><tbody>
                            {groups.into_iter().map(|(id, spans)| {
                                let root = spans.iter().find(|s| s.parent_span_id.is_none()).unwrap_or(&spans[0]);
                                let nodes = spans.iter().map(|s| s.node.clone()).collect::<std::collections::BTreeSet<_>>().into_iter().collect::<Vec<_>>().join(", ");
                                let failed = spans.iter().any(|s| !s.ok);
                                let target = id.clone();
                                view! { <tr><td><button class="cell-link" disabled=move || busy.get() on:click=move |_| run.run(QueryAction::DrillDown(target.clone()))>{root.name.clone()}</button><span class="sublabel muted small mono">{id}</span><span class="sublabel small">{root.cell.clone().or_else(|| root.url.clone()).unwrap_or_default()}</span></td>
                                    <td>{time(root.start_unix_us)}</td><td class="numeric">{format!("{:.2} ms", spans.iter().map(|s| s.duration_us).max().unwrap_or(0) as f64 / 1000.0)}</td><td>{nodes}</td><td>{spans.len()}</td><td class:stale=failed>{if failed { "Error" } else { "OK" }}</td></tr> }
                            }).collect_view()}
                        </tbody></table></div>
                        {empty.then(|| view! { <div class="empty"><h3>"No matching exported spans"</h3><p>"Generate traffic, wait for the export interval, or broaden the filters."</p></div> })}
                    </section> }.into_any()
                }}
            </div> }
        })}
    }
}

#[component]
fn TraceDetail(id: String, page: TracePage) -> impl IntoView {
    let mut spans = page.spans;
    spans.sort_by_key(|s| s.start_unix_us);
    let start = spans.first().map_or(0, |s| s.start_unix_us);
    let end = spans
        .iter()
        .map(|s| s.start_unix_us.saturating_add(s.duration_us))
        .max()
        .unwrap_or(start);
    let width = (end - start).max(1) as f64;
    let count = spans.len();
    let parents = spans
        .iter()
        .map(|s| (s.span_id.clone(), s.parent_span_id.clone()))
        .collect::<BTreeMap<_, _>>();
    view! {
        <section class="panel"><div class="panel-heading"><div><h2>"Trace timeline"</h2><p class="muted small mono">{id}</p></div><span class="badge">{format!("{count} SPANS")}</span></div>
            <div class="table-scroll"><table class="trace-timeline"><thead><tr><th>"Span / parent"</th><th>"Node / cell"</th><th>"Duration"</th><th>"Timeline"</th><th>"Status"</th></tr></thead><tbody>
                {spans.into_iter().map(|span| {
                    let left = (span.start_unix_us - start) as f64 / width * 100.0;
                    let length = (span.duration_us as f64 / width * 100.0).clamp(0.5, 100.0);
                    let mut depth = 0;
                    let mut parent = span.parent_span_id.clone();
                    while depth < 8 {
                        match parent.and_then(|id| parents.get(&id).cloned()) { Some(next) => { depth += 1; parent = next; }, None => break }
                    }
                    view! { <tr><td style=format!("padding-left:{}px", 20 + depth * 14)><strong>{span.name}</strong><span class="sublabel small muted mono">{format!("{} ← {}", span.span_id, span.parent_span_id.unwrap_or_else(|| "root".into()))}</span><span class="sublabel small">{span.url.unwrap_or_default()}</span>{span.metadata.map(|metadata| view! { <details><summary class="small muted">"Attributes / events / links"</summary><pre class="small log-body">{serde_json::to_string_pretty(&metadata).unwrap_or_default()}</pre></details> })}</td>
                        <td>{span.node}<span class="sublabel muted small">{span.cell.unwrap_or_default()}</span></td><td class="numeric">{format!("{:.2} ms", span.duration_us as f64 / 1000.0)}<span class="sublabel muted small">{span.queue_wait_us.map(|us| format!("queue {:.2} ms", us as f64 / 1000.0)).unwrap_or_default()}</span></td>
                        <td><div class="waterfall" title=format!("+{:.2} ms", (span.start_unix_us - start) as f64 / 1000.0)><div class="waterfall-bar" class:failed=!span.ok style=format!("left:{left:.2}%;width:{length:.2}%")></div></div></td><td class:stale=!span.ok>{span.error.unwrap_or_else(|| span.http_status.map(|s| s.to_string()).unwrap_or_else(|| if span.ok { "OK".into() } else { "Error".into() }))}</td>
                    </tr> }
                }).collect_view()}
            </tbody></table></div>
            <p class="panel-note muted">"Timestamps come from node wall clocks; clock skew can distort cross-node ordering. Missing parents may be external, unsampled, outside the time range, unflushed or truncated."</p>
        </section>
        <LogRecords logs=page.logs title="Correlated logs"/>
    }
}

#[component]
fn LogRecords(logs: Vec<TraceLog>, title: &'static str) -> impl IntoView {
    let empty = logs.is_empty();
    view! {
        <section class="panel"><div class="panel-heading"><h2>{title}</h2><span class="muted small">{format!("{} records", logs.len())}</span></div>
            <div class="table-scroll"><table><thead><tr><th>"Time (UTC)"</th><th>"Node"</th><th>"Severity"</th><th>"Message / span"</th></tr></thead><tbody>
                {logs.into_iter().map(|log| view! { <tr><td>{time(log.time_unix_us)}</td><td>{log.node}</td><td>{log.severity_text}<span class="sublabel muted small">{log.severity_number.map(|number| format!("OTel {number}")).unwrap_or_default()}</span></td><td class="log-body">{log.body}<span class="sublabel muted small mono">{format!("{} / {}", log.trace_id.unwrap_or_default(), log.span_id.unwrap_or_default())}</span>{log.metadata.map(|metadata| view! { <details><summary class="small muted">"Structured body / annotations"</summary><pre class="small log-body">{serde_json::to_string_pretty(&metadata).unwrap_or_default()}</pre></details> })}</td></tr> }).collect_view()}
            </tbody></table></div>
            {empty.then(|| view! { <div class="empty">"No matching exported logs"</div> })}
        </section>
    }
}
