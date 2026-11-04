// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// The REPL client reads an operator's terminal and local development metadata.
#![allow(clippy::disallowed_methods)]

//! Attach an operator to a real Worker isolate.
//!
//! A session owns one isolate affiliation, so an idle stateless pool cannot
//! reclaim the heap and its REPL globals between two inputs. Evaluations still
//! use the ordinary worker driver and turn queue; this is not a second V8
//! context or a separate JavaScript process.

use anyhow::{anyhow, bail, Context as _};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{IsTerminal as _, Write as _};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::io::AsyncBufReadExt as _;

const MAX_CODE_BYTES: usize = 1024 * 1024;
const SESSION_IDLE_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const SESSION_REAP_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsoleLog {
    pub level: String,
    pub message: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Evaluation {
    pub value: String,
    pub logs: Vec<ConsoleLog>,
    #[serde(skip)]
    pub write_position: Option<u64>,
    #[serde(skip)]
    pub observed_position: Option<u64>,
}

impl crate::engine_api::GatedAnswer for Evaluation {
    fn write_position(&self) -> Option<u64> {
        self.write_position
    }

    fn observed_position(&self) -> Option<u64> {
        self.observed_position
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttachRequest {
    pub isolate: Option<String>,
    pub object: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluateRequest {
    pub code: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionInfo {
    pub id: String,
    pub heap_id: String,
    pub object: Option<String>,
}

#[derive(Debug)]
pub enum Error {
    IsolateGone,
    ObjectGone(String),
    InvalidObject(String),
    SessionGone,
    CodeTooLarge,
    Admission(anyhow::Error),
    Gate(anyhow::Error),
    Evaluation(anyhow::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IsolateGone => formatter.write_str("the isolate is no longer resident"),
            Self::ObjectGone(scope) => write!(
                formatter,
                "object {scope} is not resident on this node; call it once first"
            ),
            Self::InvalidObject(error) => formatter.write_str(error),
            Self::SessionGone => {
                formatter.write_str("the REPL session expired or no longer exists")
            }
            Self::CodeTooLarge => write!(formatter, "REPL input exceeds {MAX_CODE_BYTES} bytes"),
            Self::Admission(error) => {
                write!(formatter, "could not admit a Worker isolate: {error:#}")
            }
            Self::Gate(error) => write!(formatter, "object output gate failed: {error:#}"),
            Self::Evaluation(error) => write!(formatter, "{error:#}"),
        }
    }
}

impl std::error::Error for Error {}

/// Arbitrary evaluation is a development/operator capability, not part of the
/// read-only monitor. Local development opts in by default. Every other node
/// must make the explicit choice because its internal listener is unauthenticated.
pub fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        let development = std::env::var_os("CELLD_INTERNAL_DEV_STORE").is_some();
        crate::env_vars::flag("CELLD_REPL", development).expect("validated CELLD_REPL")
    })
}

struct Session {
    heap_id: String,
    scope: Option<String>,
    affiliation: crate::pool::Affiliation,
    evaluation: tokio::sync::Mutex<()>,
    active: AtomicUsize,
    last_used: Mutex<Instant>,
}

impl Session {
    fn new(affiliation: crate::pool::Affiliation, scope: Option<String>) -> Self {
        let heap_id = affiliation.slot().heap_id().to_string();
        Self {
            heap_id,
            scope,
            affiliation,
            evaluation: tokio::sync::Mutex::new(()),
            active: AtomicUsize::new(0),
            last_used: Mutex::new(Instant::now()),
        }
    }

    fn touch(&self) {
        *self.last_used.lock().expect("REPL session clock poisoned") = Instant::now();
    }

    fn expired(&self) -> bool {
        self.active.load(Ordering::Acquire) == 0
            && self
                .last_used
                .lock()
                .expect("REPL session clock poisoned")
                .elapsed()
                >= SESSION_IDLE_TIMEOUT
    }
}

#[derive(Default)]
struct Registry {
    sessions: Mutex<HashMap<String, Arc<Session>>>,
}

fn registry() -> &'static Arc<Registry> {
    static REGISTRY: OnceLock<Arc<Registry>> = OnceLock::new();
    REGISTRY.get_or_init(|| Arc::new(Registry::default()))
}

struct ActiveSession(Arc<Session>);

impl ActiveSession {
    fn acquire(session: Arc<Session>) -> Self {
        session.active.fetch_add(1, Ordering::AcqRel);
        Self(session)
    }
}

impl Drop for ActiveSession {
    fn drop(&mut self) {
        self.0.touch();
        self.0.active.fetch_sub(1, Ordering::AcqRel);
    }
}

fn start_reaper() {
    static START: OnceLock<()> = OnceLock::new();
    START.get_or_init(|| {
        crate::asyncrt::spawn(async {
            loop {
                crate::asyncrt::sleep(SESSION_REAP_INTERVAL).await;
                let registry = registry();
                let expired = registry
                    .sessions
                    .lock()
                    .expect("REPL registry poisoned")
                    .iter()
                    .filter(|(_, session)| session.expired())
                    .map(|(id, _)| id.clone())
                    .collect::<Vec<_>>();
                if expired.is_empty() {
                    continue;
                }
                let mut sessions = registry.sessions.lock().expect("REPL registry poisoned");
                for id in expired {
                    if sessions.get(&id).is_some_and(|session| session.expired()) {
                        sessions.remove(&id);
                    }
                }
            }
        })
        .detach();
    });
}

fn object_parts(target: &str) -> Result<(&str, &str), Error> {
    let Some((class, name)) = target.split_once(':') else {
        return Err(Error::InvalidObject(
            "--object requires CLASS:NAME, for example Counter:demo".into(),
        ));
    };
    if class.is_empty()
        || class.len() > 128
        || !class
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$'))
    {
        return Err(Error::InvalidObject(
            "the object class must be a JavaScript class name".into(),
        ));
    }
    if name.is_empty() || name.len() > 1024 {
        return Err(Error::InvalidObject(
            "the object name must contain 1 to 1024 bytes".into(),
        ));
    }
    Ok((class, name))
}

pub async fn attach(
    runtime: &crate::runtime::RuntimeManager,
    isolate: Option<&str>,
    object: Option<&str>,
) -> Result<SessionInfo, Error> {
    start_reaper();
    if isolate.is_some() && object.is_some() {
        return Err(Error::InvalidObject(
            "--object and --isolate are mutually exclusive".into(),
        ));
    }
    let (affiliation, scope) = match object {
        Some(target) => {
            let (class, name) = object_parts(target)?;
            let scope = runtime.repl_cell_scope(class, name);
            let affiliation = runtime
                .repl_cell(&scope)
                .ok_or_else(|| Error::ObjectGone(scope.clone()))?;
            (affiliation, Some(scope))
        }
        None => {
            let affiliation = match isolate {
                Some(heap_id) => runtime
                    .repl_slot(heap_id)
                    .map(|slot| slot.affiliate())
                    .ok_or(Error::IsolateGone)?,
                None => runtime
                    .generation()
                    .stateless
                    .attach_repl()
                    .await
                    .map_err(|error| Error::Admission(error.into()))?,
            };
            (affiliation, None)
        }
    };
    let session = Arc::new(Session::new(affiliation, scope));
    let info = SessionInfo {
        id: new_session_id(),
        heap_id: session.heap_id.clone(),
        object: object.map(str::to_string),
    };
    registry()
        .sessions
        .lock()
        .expect("REPL registry poisoned")
        .insert(info.id.clone(), session);
    Ok(info)
}

fn new_session_id() -> String {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).expect("REPL session entropy");
    format!("{:032x}", u128::from_le_bytes(bytes))
}

fn session(id: &str) -> Result<Arc<Session>, Error> {
    registry()
        .sessions
        .lock()
        .expect("REPL registry poisoned")
        .get(id)
        .cloned()
        .ok_or(Error::SessionGone)
}

/// Compile one buffered interactive input without evaluating it. `false`
/// means V8 expects more input; other syntax errors return `true`, so the
/// ordinary evaluator reports them immediately instead of waiting forever.
pub async fn complete(id: &str, code: String) -> Result<bool, Error> {
    if code.len() > MAX_CODE_BYTES {
        return Err(Error::CodeTooLarge);
    }
    let session = session(id)?;
    let _active = ActiveSession::acquire(session.clone());
    let _serial = session.evaluation.lock().await;
    session.touch();
    session
        .affiliation
        .slot()
        .turn(|worker| worker.repl_input_complete(&code))
        .await
        .ok_or_else(|| Error::Evaluation(anyhow!("the REPL isolate is no longer available")))
}

pub async fn evaluate(
    app: &crate::actor::AppHandle,
    id: &str,
    code: String,
) -> Result<Evaluation, Error> {
    let scope = session(id)?.scope.clone();
    let result = evaluate_session(id, code).await;
    if let Some(scope) = scope {
        gate_evaluation(app, &scope, &result).await?;
    }
    result
}

async fn evaluate_session(id: &str, code: String) -> Result<Evaluation, Error> {
    if code.len() > MAX_CODE_BYTES {
        return Err(Error::CodeTooLarge);
    }
    let session = session(id)?;
    let _active = ActiveSession::acquire(session.clone());
    let _serial = session.evaluation.lock().await;
    session.touch();
    evaluate_target(
        session.affiliation.slot().clone(),
        session.scope.clone(),
        code,
    )
    .await
}

fn evaluation_ticket(result: &Result<Evaluation, Error>) -> Option<crate::actor::GateTicket> {
    match result {
        Ok(evaluation) => Some(crate::actor::GateTicket::response(
            evaluation.write_position,
            evaluation.observed_position,
        )),
        Err(Error::Evaluation(error)) => {
            error
                .downcast_ref::<crate::js::FailedInTurn>()
                .map(|failed| {
                    crate::actor::GateTicket::response(
                        failed.write_position,
                        failed.observed_position,
                    )
                })
        }
        _ => None,
    }
}

async fn gate_evaluation(
    app: &crate::actor::AppHandle,
    scope: &str,
    result: &Result<Evaluation, Error>,
) -> Result<(), Error> {
    let Some(ticket) = evaluation_ticket(result) else {
        return Ok(());
    };
    let routed = app
        .gate_request(scope.to_string())
        .await
        .map_err(|error| Error::Gate(anyhow!("{error:?}")))?;
    let _activity = app.activity(routed.request, scope.to_string());
    if routed.route != celld_logic::Route::Local {
        return Err(Error::Gate(anyhow!(
            "object moved to another node before the result could be released"
        )));
    }
    app.gate_output(routed.request, ticket)
        .await
        .map_err(|error| Error::Gate(anyhow!("{error:?}")))
}

#[cfg(test)]
async fn evaluate_slot(slot: Arc<crate::pool::Slot>, code: String) -> Result<Evaluation, Error> {
    evaluate_target(slot, None, code).await
}

async fn evaluate_target(
    slot: Arc<crate::pool::Slot>,
    scope: Option<String>,
    code: String,
) -> Result<Evaluation, Error> {
    let (reply, receive) = tokio::sync::oneshot::channel();
    if let Some(scope) = scope {
        let job = crate::js::CellJob::Eval { scope, code, reply };
        crate::asyncrt::spawn(crate::runtime::drive_cell(
            slot.affiliate(),
            job,
            None,
            None,
        ))
        .detach();
    } else {
        let job = crate::WorkerJob::Eval { code, reply };
        crate::asyncrt::spawn(crate::runtime::drive(slot, job, None)).detach();
    }
    receive
        .await
        .map_err(|error| Error::Evaluation(anyhow!("Worker dropped REPL result: {error}")))?
        .map_err(Error::Evaluation)
}

pub fn close(id: &str) -> bool {
    registry()
        .sessions
        .lock()
        .expect("REPL registry poisoned")
        .remove(id)
        .is_some()
}

#[derive(Default)]
struct Options {
    project: Option<std::path::PathBuf>,
    url: Option<String>,
    isolate: Option<String>,
    object: Option<String>,
    eval: Option<String>,
}

fn options(arguments: Vec<String>) -> anyhow::Result<Option<Options>> {
    let mut options = Options::default();
    let mut arguments = arguments.into_iter();
    while let Some(argument) = arguments.next() {
        let mut value = |name: &str| {
            arguments
                .next()
                .with_context(|| format!("{name} requires a value"))
        };
        match argument.as_str() {
            "-h" | "--help" => return Ok(None),
            "--url" => options.url = Some(value("--url")?),
            "--isolate" => options.isolate = Some(value("--isolate")?),
            "--object" => options.object = Some(value("--object")?),
            "-e" | "--eval" => options.eval = Some(value("--eval")?),
            _ if argument.starts_with('-') => bail!("unknown REPL option: {argument}"),
            _ if options.project.is_none() => {
                options.project = Some(std::path::PathBuf::from(argument));
            }
            _ => bail!("unexpected REPL argument: {argument}"),
        }
    }
    if let Some(isolate) = &options.isolate {
        anyhow::ensure!(
            !isolate.is_empty()
                && isolate.len() <= 20
                && isolate.bytes().all(|byte| byte.is_ascii_digit()),
            "--isolate must be a numeric heap ID from the monitor API"
        );
    }
    if let Some(object) = &options.object {
        object_parts(object)?;
    }
    anyhow::ensure!(
        options.isolate.is_none() || options.object.is_none(),
        "--object and --isolate are mutually exclusive"
    );
    Ok(Some(options))
}

fn help() -> anyhow::Result<()> {
    crate::cli_output::Output::new(crate::cli_output::Format::Text).help(
        "celld repl — evaluate JavaScript in a running Worker isolate\n\n\
         USAGE:\n  celld repl [PROJECT] [--url INTERNAL_URL] [--isolate HEAP_ID | --object CLASS:NAME]\n  celld repl [PROJECT] [--url INTERNAL_URL] [--isolate HEAP_ID | --object CLASS:NAME] --eval CODE\n\n\
         Without --url, PROJECT/.celld/dev/operator supplies the running dev node's\n\
         operator address (default PROJECT: .). Without --isolate, the current main\n\
         Worker's first isolate is used. env and exports are available. With --object,\n\
         object, state, storage, and cell bind to the live Durable Object instance.\n\
         Returned promises are awaited; use an async IIFE for top-level await. Globals\n\
         persist for the session. Interactive commands: .help, .cancel, .exit.\n\n\
         OPTIONS:\n  --url URL            Operator listener URL (or CELLD_REPL_URL)\n  --isolate HEAP_ID    Attach to a specific resident isolate\n  --object CLASS:NAME  Debug a live Durable Object on this node\n  -e, --eval CODE      Evaluate once and exit; stdin is evaluated when piped\n  -h, --help           Show this help\n\n\
         Local dev enables the REPL by default. Other nodes require CELLD_REPL=1.\n\
         Evaluation has the Worker's full authority; use only a trusted listener.",
    )
}

struct Client {
    http: reqwest::Client,
    base: String,
    session: SessionInfo,
}

impl Client {
    async fn attach(
        url: &str,
        isolate: Option<String>,
        object: Option<String>,
    ) -> anyhow::Result<Self> {
        let base = url.trim_end_matches('/').to_string();
        let parsed = url::Url::parse(&base).context("parse the REPL operator URL")?;
        anyhow::ensure!(
            matches!(parsed.scheme(), "http" | "https"),
            "the REPL operator URL must use http or https"
        );
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .build()
            .context("build the REPL HTTP client")?;
        let session = request(
            http.post(format!("{base}/repl"))
                .json(&serde_json::json!({ "isolate": isolate, "object": object })),
        )
        .await
        .context("attach the REPL")?;
        Ok(Self {
            http,
            base,
            session,
        })
    }

    async fn evaluate(&self, code: String) -> anyhow::Result<Evaluation> {
        request(
            self.http
                .post(format!("{}/repl/{}", self.base, self.session.id))
                .json(&EvaluateRequest { code }),
        )
        .await
    }

    /// Ask the attached V8 whether this buffer parses, without running it.
    async fn complete(&self, code: String) -> anyhow::Result<bool> {
        #[derive(Deserialize)]
        struct Completeness {
            complete: bool,
        }
        let response: Completeness = request(
            self.http
                .post(format!("{}/repl/{}/complete", self.base, self.session.id))
                .json(&EvaluateRequest { code }),
        )
        .await?;
        Ok(response.complete)
    }

    async fn close(self) {
        let _ = self
            .http
            .delete(format!("{}/repl/{}", self.base, self.session.id))
            .send()
            .await;
    }
}

async fn request<T: for<'de> Deserialize<'de>>(
    request: reqwest::RequestBuilder,
) -> anyhow::Result<T> {
    let response = request.send().await.context("contact the REPL operator")?;
    let status = response.status();
    let body = response.text().await.context("read the REPL reply")?;
    if !status.is_success() {
        let message = serde_json::from_str::<serde_json::Value>(&body)
            .ok()
            .and_then(|value| {
                value
                    .get("error")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            })
            .unwrap_or(body);
        bail!("{message} (HTTP {status})");
    }
    serde_json::from_str(&body).context("decode the REPL reply")
}

fn operator_url(options: &Options) -> anyhow::Result<String> {
    if let Some(url) = &options.url {
        return Ok(url.clone());
    }
    if let Some(url) = std::env::var("CELLD_REPL_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
    {
        return Ok(url);
    }
    let config = crate::deploy::resolve_config(options.project.clone())?;
    let project = config
        .parent()
        .context("the Wrangler config has no project directory")?;
    let metadata = project.join(".celld/dev/operator");
    let url = std::fs::read_to_string(&metadata).with_context(|| {
        format!(
            "read {}; start `celld dev` or pass --url INTERNAL_URL",
            metadata.display()
        )
    })?;
    Ok(url.trim().to_string())
}

fn print_evaluation(evaluation: Evaluation) -> anyhow::Result<()> {
    for log in evaluation.logs {
        match log.level.as_str() {
            "error" | "warn" => crate::note!("{}: {}", log.level, log.message),
            _ => crate::cli_output::Output::new(crate::cli_output::Format::Text)
                .line(format_args!("{}", log.message))?,
        }
    }
    crate::cli_output::Output::new(crate::cli_output::Format::Text)
        .line(format_args!("{}", evaluation.value))
}

pub async fn run(arguments: Vec<String>) -> anyhow::Result<()> {
    let Some(options) = options(arguments)? else {
        return help();
    };
    let url = operator_url(&options)?;
    let client = Client::attach(&url, options.isolate, options.object).await?;
    if let Some(code) = options.eval {
        let result = client.evaluate(code).await;
        client.close().await;
        print_evaluation(result?)?;
        return Ok(());
    }
    if !std::io::stdin().is_terminal() {
        let mut code = String::new();
        tokio::io::AsyncReadExt::read_to_string(&mut tokio::io::stdin(), &mut code)
            .await
            .context("read REPL input from stdin")?;
        let result = client.evaluate(code).await;
        client.close().await;
        print_evaluation(result?)?;
        return Ok(());
    }

    if let Some(object) = &client.session.object {
        eprintln!("Attached to {object}. Type .help for help; .exit to quit.");
    } else {
        eprintln!(
            "Attached to isolate {}. Type .help for help; .exit to quit.",
            client.session.heap_id
        );
    }
    let stdin = tokio::io::stdin();
    let mut lines = tokio::io::BufReader::new(stdin).lines();
    let mut continuation = String::new();
    loop {
        if continuation.is_empty() {
            eprint!("> ");
        } else {
            eprint!("... ");
        }
        std::io::stderr().flush().ok();
        let Some(line) = lines.next_line().await.context("read REPL input")? else {
            break;
        };
        if continuation.is_empty() {
            match line.trim() {
                "" => continue,
                ".exit" | ".quit" => break,
                ".help" => {
                    eprintln!("env and exports are available. Returned promises are awaited.");
                    eprintln!("Object sessions also expose object, state, storage, and cell.");
                    eprintln!(
                        "Multiline input continues until V8 can parse it; .cancel discards it."
                    );
                    eprintln!("Use an async IIFE for top-level await. .exit leaves the REPL.");
                    continue;
                }
                _ => {}
            }
        } else if line.trim() == ".cancel" {
            continuation.clear();
            continue;
        }
        if line.ends_with('\\') {
            // Keep the backslash: inside a string it is JavaScript's own line
            // continuation. Outside one, an unfinished expression already
            // continues naturally, so the marker is only a convenience.
            continuation.push_str(&line);
            continuation.push('\n');
            continue;
        }
        continuation.push_str(&line);
        continuation.push('\n');
        // Compile in the attached isolate, never evaluate twice. An unfinished
        // block keeps the buffer; another syntax error goes to the evaluator
        // now so its message is not hidden behind a continuation prompt.
        match client.complete(continuation.clone()).await {
            Ok(false) => continue,
            Ok(true) => {}
            Err(error) => eprintln!("completeness check failed: {error:#}"),
        }
        let code = std::mem::take(&mut continuation);
        match client.evaluate(code).await {
            Ok(evaluation) => print_evaluation(evaluation)?,
            Err(error) => eprintln!("{error:#}"),
        }
    }
    if !continuation.is_empty() {
        // EOF should not silently discard a buffered block.
        match client.evaluate(continuation).await {
            Ok(evaluation) => print_evaluation(evaluation)?,
            Err(error) => eprintln!("{error:#}"),
        }
    }
    client.close().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> Arc<crate::js::WorkerConfig> {
        Arc::new(crate::js::WorkerConfig::new(crate::js::WorkerConfigOptions {
            src: "export const answer = { value: 42 }; export default { fetch() { return new Response('ok'); } };".into(),
            script_name: "repl-test".into(),
            do_classes: vec![],
            bindings: vec![],
            r2_bindings: vec![],
            d1_bindings: vec![],
            kv_bindings: vec![],
            queue_bindings: vec![],
            queue_consumers: vec![],
            workflow_bindings: vec![],
            vars: vec![("TEST".into(), "present".into())],
            node: "test-node".into(),
            modules: vec![],
            compat: Default::default(),
        }))
    }

    #[test]
    fn parses_attach_options() {
        let parsed = options(vec![
            "example".into(),
            "--url".into(),
            "http://127.0.0.1:9877".into(),
            "--isolate".into(),
            "12".into(),
            "--eval".into(),
            "1 + 1".into(),
        ])
        .unwrap()
        .unwrap();
        assert_eq!(parsed.project, Some(std::path::PathBuf::from("example")));
        assert_eq!(parsed.url.as_deref(), Some("http://127.0.0.1:9877"));
        assert_eq!(parsed.isolate.as_deref(), Some("12"));
        assert_eq!(parsed.eval.as_deref(), Some("1 + 1"));
        assert!(options(vec!["--isolate".into(), "worker".into()]).is_err());
        assert!(options(vec!["--unknown".into()]).is_err());
        let object = options(vec!["--object".into(), "Counter:demo".into()])
            .unwrap()
            .unwrap();
        assert_eq!(object.object.as_deref(), Some("Counter:demo"));
        assert!(options(vec!["--object".into(), "missing-name".into()]).is_err());
        assert!(options(vec![
            "--object".into(),
            "Counter:demo".into(),
            "--isolate".into(),
            "1".into(),
        ])
        .is_err());
    }

    #[test]
    fn evaluates_on_the_real_worker_with_persistent_globals_and_async_results() {
        let runtime = crate::test_runtime::init();
        crate::runtime::init_v8();
        let config = test_config();
        let slot = crate::pool::Slot::loaded_worker(
            crate::js::Worker::load_config(config).expect("load the REPL test Worker"),
        );
        runtime.block_on(async {
            let first = evaluate_slot(
                slot.clone(),
                "var replAnswer = exports.answer.value; globalThis.replCount = 1; env.TEST"
                    .into(),
            )
            .await
            .unwrap();
            assert_eq!(first.value, "\"present\"");

            let second = evaluate_slot(
                slot.clone(),
                "(async () => { console.log('answer', replAnswer); await Promise.resolve(); return { count: ++replCount, env: env.TEST }; })()"
                    .into(),
            )
            .await
            .unwrap();
            assert!(second.value.contains("\"count\": 2"), "{}", second.value);
            assert!(second.value.contains("\"env\": \"present\""), "{}", second.value);
            assert_eq!(second.logs.len(), 1);
            assert_eq!(second.logs[0].message, "answer 42");

            let error = evaluate_slot(slot.clone(), "await (".into()).await;
            assert!(matches!(error, Err(Error::Evaluation(_))));
            let third = evaluate_slot(slot, "++replCount".into()).await.unwrap();
            assert_eq!(third.value, "3");
        });
    }

    #[test]
    fn multiline_completeness_uses_v8_without_evaluating_the_buffer() {
        let runtime = crate::test_runtime::init();
        crate::runtime::init_v8();
        let slot = crate::pool::Slot::loaded_worker(
            crate::js::Worker::load_config(test_config()).expect("load the REPL test Worker"),
        );
        let mut failures = Vec::new();
        for (code, expected) in [
            ("1 + 1", true),
            ("const f = () => {\n  return 1;\n};", true),
            ("({ answer: 42 })", true),
            ("const x =", false),
            ("function f() {", false),
            ("({ answer: 42", false),
            ("const a = [1, 2,", false),
            ("if (true) { console.log(1)", false),
            ("const s = \"unterminated", false),
            ("const t = `unterminated", false),
            ("/* unterminated comment", false),
            ("/unterminated", false),
            ("const = ;", true),
            ("@", true),
            ("await )", true),
        ] {
            let actual = runtime
                .block_on(slot.turn(|worker| worker.repl_input_complete(code)))
                .unwrap();
            if actual != expected {
                failures.push((code, expected, actual));
            }
        }
        assert!(failures.is_empty(), "{failures:?}");
        let unevaluated = runtime
            .block_on(async { evaluate_slot(slot, "typeof f".into()).await })
            .unwrap();
        assert_eq!(
            unevaluated.value, "\"undefined\"",
            "a completeness check must not execute declarations"
        );
    }

    #[test]
    fn default_attach_admits_a_fresh_isolate_after_the_idle_pool_empties() {
        let runtime = crate::test_runtime::init();
        crate::runtime::init_v8();
        let worker = crate::runtime::StatelessRuntime::start(
            test_config(),
            Arc::from("test-node"),
            Arc::from("test-region"),
        )
        .unwrap();
        worker.isolates.reap_empty();
        assert_eq!(worker.isolates.live(), 0);

        runtime.block_on(async {
            let affiliation = worker.attach_repl().await.unwrap();
            assert!(!affiliation.slot().is_retiring());
            let evaluation = evaluate_slot(affiliation.slot().clone(), "40 + 2".into())
                .await
                .unwrap();
            assert_eq!(evaluation.value, "42");
        });
    }

    #[test]
    fn object_evaluation_reaches_the_live_instance_and_its_storage() {
        let runtime = crate::test_runtime::init();
        crate::runtime::init_v8();
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("counter.sqlite");
        let database = database.to_string_lossy().into_owned();
        let scope = format!("Counter:{}", "0".repeat(64));
        let config = Arc::new(crate::js::WorkerConfig::new(crate::js::WorkerConfigOptions {
            src: "export class Counter { constructor(state) { this.state = state; this.label = 'live'; } async fetch() { return Response.json({}); } } export default { fetch() { return new Response('ok'); } };".into(),
            script_name: "repl-object-test".into(),
            do_classes: vec!["Counter".into()],
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
        }));
        let slot = crate::pool::Slot::loaded_worker(
            crate::js::Worker::load_config(config).expect("load the object REPL Worker"),
        );
        runtime.block_on(async {
            slot.turn(|worker| {
                worker.own_cell(
                    &scope,
                    Some(crate::js::CellStorage {
                        path: &database,
                        epoch: 1,
                        replicated_wake: false,
                        vfs: None,
                    }),
                )
            })
            .await
            .unwrap();
            let evaluation = evaluate_target(
                slot,
                Some(scope),
                "(async()=>{await storage.put('n',42);return {type:object.constructor.name,stateType:state.constructor.name,label:object.label,n:await storage.get('n')};})()"
                    .into(),
            )
            .await
            .unwrap();
            assert!(evaluation.value.contains("\"type\": \"Counter\""));
            assert!(evaluation.value.contains("\"stateType\": \"DurableObjectState\""));
            assert!(evaluation.value.contains("\"label\": \"live\""));
            assert!(evaluation.value.contains("\"n\": 42"));
            assert!(evaluation.write_position.is_some());
        });
    }
}
