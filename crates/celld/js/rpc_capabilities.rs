// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Process-local routes for transient RPC capabilities. The route holds an
//! affiliation, not a V8 handle: every invocation re-enters through the slot's
//! async permit, and nothing holds an isolate lock while awaiting another one.
use super::*;

struct Route {
    affiliation: crate::pool::Affiliation,
    scope: Option<String>,
    refs: usize,
}

type Key = (String, u64);
static ROUTES: OnceLock<Mutex<HashMap<Key, Route>>> = OnceLock::new();

fn routes() -> &'static Mutex<HashMap<Key, Route>> {
    ROUTES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn release_route(key: &Key) -> Option<Route> {
    let mut routes = routes().lock().unwrap();
    let route = routes.get_mut(key)?;
    route.refs -= 1;
    if route.refs == 0 {
        routes.remove(key)
    } else {
        None
    }
}

fn dispose_remote(route: Route, id: u64) {
    crate::asyncrt::spawn(async move {
        let _ = dispatch(
            route,
            crate::RpcCapabilityOperation {
                id,
                path: vec![],
                args: None,
                dispose: true,
                order: None,
            },
        )
        .await;
    })
    .detach();
}

// An eager call keeps the entry alive even if its last user handle is disposed
// before the callee's first turn. Cancellation drops this lease as well.
struct CallLease {
    key: Key,
}
impl Drop for CallLease {
    fn drop(&mut self) {
        if let Some(route) = release_route(&self.key) {
            dispose_remote(route, self.key.1);
        }
    }
}

fn key(scope: &mut v8::PinScope, args: &v8::FunctionCallbackArguments) -> Key {
    (
        args.get(0).to_rust_string_lossy(scope),
        args.get(1).integer_value(scope).unwrap_or(0).max(0) as u64,
    )
}

pub(super) fn register(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let Some(slot) = crate::pool::current_slot() else {
        // Direct engine harnesses have no placement pool to route through.
        rv.set(v8::Boolean::new(scope, false).into());
        return;
    };
    let key = key(scope, &args);
    let cell = args.get(2);
    let cell = (!cell.is_null_or_undefined()).then(|| cell.to_rust_string_lossy(scope));
    routes()
        .lock()
        .unwrap()
        .entry(key)
        .or_insert_with(|| Route {
            affiliation: slot.affiliate(),
            scope: cell,
            refs: 1,
        });
    rv.set(v8::Boolean::new(scope, true).into());
}

pub(super) fn retain(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let key = key(scope, &args);
    let mut routes = routes().lock().unwrap();
    let retained = match routes.get_mut(&key) {
        Some(route) => {
            route.refs += 1;
            true
        }
        None => false,
    };
    rv.set(v8::Boolean::new(scope, retained).into());
}

pub(super) fn forget(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    routes().lock().unwrap().remove(&key(scope, &args));
}

pub(super) fn release(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let key = key(scope, &args);
    let Some(route) = release_route(&key) else {
        return;
    };
    if crate::pool::current_slot().is_some_and(|slot| Arc::ptr_eq(&slot, route.affiliation.slot()))
    {
        // The origin is already entered: JS can dispose the target directly.
        rv.set(v8::Boolean::new(scope, true).into());
    } else {
        dispose_remote(route, key.1);
    }
}

async fn dispatch(
    route: Route,
    operation: crate::RpcCapabilityOperation,
) -> Result<Vec<u8>, String> {
    let slot = route.affiliation.slot().clone();
    if let Some(cell) = route.scope {
        let (reply, receive) = tokio::sync::oneshot::channel();
        let job = CellJob::Capability {
            scope: cell,
            operation,
            reply,
        };
        let driving = tokio::spawn(crate::runtime::drive_cell(
            route.affiliation,
            job,
            None,
            None,
        ));
        let result = receive
            .await
            .map_err(|_| "RPC capability owner disconnected".to_string())?
            .map_err(|error| format!("{error:#}"))?;
        drop(driving);
        match result.data {
            RpcData::V8(bytes) => Ok(bytes.to_vec()),
            RpcData::Json(_) => Err("RPC capability answered non-bytes".to_string()),
        }
    } else {
        let (reply, receive) = tokio::sync::oneshot::channel();
        let job = crate::WorkerJob::Rpc {
            entrypoint: String::new(),
            operation: crate::WorkerRpcOperation::Capability(operation),
            props: vec![],
            invocation_limits: None,
            reply,
        };
        let driving = tokio::spawn(crate::runtime::drive(slot, job, None));
        let result = receive
            .await
            .map_err(|_| "RPC capability owner disconnected".to_string())?
            .map_err(|error| format!("{error:#}"));
        drop(driving);
        result
    }
}

pub(super) fn call(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let key = key(scope, &args);
    let path: Vec<String> = match serde_json::from_str(&args.get(2).to_rust_string_lossy(scope)) {
        Ok(path) => path,
        Err(error) => return loader_throw(scope, &format!("invalid RPC capability path: {error}")),
    };
    let payload = args.get(3);
    let operation = crate::RpcCapabilityOperation {
        id: key.1,
        path,
        args: (!payload.is_null_or_undefined()).then(|| view_bytes(payload).unwrap_or_default()),
        dispose: false,
        order: Some(enter_call_order(
            event_context(scope),
            &format!("rpc:{}:{}", key.0, key.1),
        )),
    };
    // Capture a fresh affiliation before the caller may release its handle.
    let route = routes().lock().unwrap().get_mut(&key).map(|route| {
        route.refs += 1;
        Route {
            affiliation: route.affiliation.slot().affiliate(),
            scope: route.scope.clone(),
            refs: 1,
        }
    });
    let lease = route.as_ref().map(|_| CallLease { key });
    let gate = egress_gate_request(&event_context(scope), celld_logic::Channel::CellRpc);
    let id = asyncrt::enqueue(async move {
        let _lease = lease;
        await_egress_gate(gate).await?;
        dispatch(
            route.ok_or_else(|| {
                "RPC capability owner is no longer running on this node".to_string()
            })?,
            operation,
        )
        .await
    });
    rv.set(promise_for(scope, id));
}

pub(super) fn output_gate(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let gate = egress_gate_request(&event_context(scope), celld_logic::Channel::CellRpc);
    let id = asyncrt::enqueue(async move {
        await_egress_gate(gate).await?;
        Ok(Vec::<u8>::new())
    });
    rv.set(promise_for(scope, id));
}
