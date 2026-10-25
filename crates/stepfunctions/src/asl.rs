//! Minimal ASL (Amazon States Language) interpreter for Step Functions.
//!
//! Supports the state types the compatibility suite exercises: Pass
//! (Result/Parameters/ResultPath), Choice (comparators + Default), Fail,
//! Succeed, Wait, Parallel (branches), Map (inline Iterator) and Task
//! (treated as a pass-through). Errors flow through the era `_handle_error`
//! path: Retry blocks re-execute the state with capped backoff, Catch blocks
//! route a `{"Error", "Cause"}` payload to the catcher's Next state.
//! Executions run on a detached thread so StopExecution can abort them while
//! they are waiting.

use crate::models::Execution;
use parking_lot::Mutex;
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::sync::Arc;

/// AWS ends an execution once its event history reaches this size; a
/// self-referential Next cycle otherwise runs forever emitting events.
pub const MAX_EVENT_HISTORY: usize = 25_000;

pub fn now_f() -> f64 {
    (chrono::Utc::now().timestamp_millis() as f64) / 1000.0
}

#[derive(Debug)]
pub struct FlowError {
    pub error: String,
    pub cause: String,
}

impl FlowError {
    pub fn new(error: &str, cause: String) -> Self {
        Self {
            error: error.to_string(),
            cause,
        }
    }
}

/// Serialise a JSON value as the wire string used in event details.
pub fn value_to_string(v: &Value) -> String {
    serde_json::to_string(v).unwrap_or_else(|_| "null".to_string())
}

/// Resolve a JSONPath of the form `$.a.b` against a value.
pub fn resolve_json_path(path: &str, base: &Value) -> Option<Value> {
    let rest = path.strip_prefix("$.")?;
    if rest.is_empty() {
        return Some(base.clone());
    }
    let mut cur = base.clone();
    for part in rest.split('.') {
        cur = cur.get(part)?.clone();
    }
    Some(cur)
}

/// Interpreter context: emits history events into the execution record.
pub struct Ctx {
    pub exec: Arc<Execution>,
    /// Id of the most recent state-exited event; "entered" events reference it
    /// as their previousEventId, mirroring moto's event chain.
    last_exited: Mutex<i64>,
}

impl Ctx {
    pub fn new(exec: Arc<Execution>) -> Self {
        Self {
            exec,
            last_exited: Mutex::new(0),
        }
    }

    fn detail_prefix(state_type: &str) -> String {
        let mut chars = state_type.chars();
        if let Some(first) = chars.next() {
            format!("{}{}", first.to_lowercase(), chars.as_str())
        } else {
            state_type.to_string()
        }
    }

    pub fn emit_entered(&self, state_type: &str, name: &str, input: &Value) {
        let key = format!(
            "{}StateEnteredEventDetails",
            Self::detail_prefix(state_type)
        );
        let prev = *self.last_exited.lock();
        let event = json!({
            "timestamp": now_f(),
            "type": format!("{state_type}StateEntered"),
            "previousEventId": prev,
            key: {
                "name": name,
                "input": value_to_string(input),
            },
        });
        self.exec.push_event(event);
    }

    pub fn emit_exited(&self, state_type: &str, name: &str, output: &Value) {
        let key = format!("{}StateExitedEventDetails", Self::detail_prefix(state_type));
        let entered_id = {
            let events = self.exec.events.read();
            events.last().and_then(|e| e["id"].as_i64()).unwrap_or(0)
        };
        let event = json!({
            "timestamp": now_f(),
            "type": format!("{state_type}StateExited"),
            "previousEventId": entered_id,
            key: {
                "name": name,
                "output": value_to_string(output),
            },
        });
        let exited_id = self.exec.push_event(event);
        *self.last_exited.lock() = exited_id;
    }
}

/// Insert `value` at a nested `$.a.b` path inside `root`, creating parents.
fn insert_at_path(root: &mut Value, path: &str, value: Value) {
    let Some(rest) = path.strip_prefix("$.") else {
        return;
    };
    if rest.is_empty() {
        *root = value;
        return;
    }
    let parts: Vec<&str> = rest.split('.').collect();
    insert_nested(root, &parts, value);
}

fn insert_nested(cur: &mut Value, parts: &[&str], value: Value) {
    if parts.is_empty() {
        *cur = value;
        return;
    }
    let key = parts[0].to_string();
    if !cur.is_object() {
        *cur = json!({});
    }
    if let Some(obj) = cur.as_object_mut() {
        let slot = obj.entry(key).or_insert_with(|| json!({}));
        insert_nested(slot, &parts[1..], value);
    }
}

/// Apply ResultPath semantics exactly like the Python era's
/// `_apply_result_path(original, result, result_path)`:
///
/// ```python
/// if result_path is None:
///     return original  # Discard result
/// if result_path == "$":
///     return result  # Replace entirely
/// if not result_path.startswith("$."):
///     return result
/// # Set nested path
/// output = copy.deepcopy(original) if isinstance(original, dict) else {}
/// ```
///
/// So `ResultPath: null` keeps the input (the result is thrown away), `"$"`
/// replaces the input entirely (no merging!), and a nested path inserts the
/// result into a copy of the input.
fn apply_result_path(path: Option<&str>, input: &Value, state_out: Value) -> Value {
    match path {
        None => input.clone(),
        Some("$") => state_out,
        Some(p) if p.starts_with("$.") => {
            let mut merged = if input.is_object() {
                input.clone()
            } else {
                json!({})
            };
            insert_at_path(&mut merged, p, state_out);
            merged
        }
        Some(_) => state_out,
    }
}

/// Normalise the state's ResultPath member to the era `state_def.get(
/// "ResultPath", "$")` shape: absent (or non-string) falls back to `"$"`,
/// an explicit `null` becomes `None` (which discards the result).
fn result_path_spec(obj: &Map<String, Value>) -> Option<&str> {
    match obj.get("ResultPath") {
        None => Some("$"),
        Some(Value::String(s)) => Some(s.as_str()),
        Some(Value::Null) => None,
        Some(_) => Some("$"),
    }
}

/// Apply a Parameters object: plain keys pass through; `key.$` entries are
/// resolved against the state input.
fn eval_parameters(params: &Map<String, Value>, input: &Value) -> Value {
    let mut out = Map::new();
    for (k, v) in params {
        if let Some(expr) = k.strip_suffix(".$") {
            let resolved = v.as_str().unwrap_or("$");
            out.insert(
                expr.to_string(),
                resolve_json_path(resolved, input).unwrap_or(Value::Null),
            );
        } else {
            out.insert(k.clone(), v.clone());
        }
    }
    Value::Object(out)
}

fn compare_numeric(
    resolved: &Value,
    expected: &Value,
    f: impl Fn(f64, f64) -> bool,
) -> Option<bool> {
    match (resolved.as_f64(), expected.as_f64()) {
        (Some(a), Some(b)) => Some(f(a, b)),
        _ => Some(false),
    }
}

/// Evaluate one Choice rule; returns true when it matches. Only top-level
/// rules carry `Next`; nested And/Or/Not rules are bare comparator groups.
fn eval_choice(rule: &Value, input: &Value, top_level: bool) -> bool {
    let Some(obj) = rule.as_object() else {
        return false;
    };
    if let Some(and) = obj.get("And").and_then(|a| a.as_array()) {
        if !and.iter().all(|r| eval_choice(r, input, false)) {
            return false;
        }
    }
    if let Some(or) = obj.get("Or").and_then(|a| a.as_array()) {
        if !or.iter().any(|r| eval_choice(r, input, false)) {
            return false;
        }
    }
    if let Some(not) = obj.get("Not") {
        if eval_choice(not, input, false) {
            return false;
        }
    }

    if let Some(variable) = obj.get("Variable").and_then(|v| v.as_str()) {
        let resolved = resolve_json_path(variable, input).unwrap_or(Value::Null);
        for (op, expected) in obj.iter() {
            let matched = match op.as_str() {
                "NumericEquals" => {
                    let a = resolved.as_f64().unwrap_or(f64::NAN);
                    let b = expected.as_f64().unwrap_or(f64::NAN);
                    Some(a == b)
                }
                "NumericGreaterThan" => compare_numeric(&resolved, expected, |a, b| a > b),
                "NumericGreaterThanEquals" => compare_numeric(&resolved, expected, |a, b| a >= b),
                "NumericLessThan" => compare_numeric(&resolved, expected, |a, b| a < b),
                "NumericLessThanEquals" => compare_numeric(&resolved, expected, |a, b| a <= b),
                "StringEquals" => {
                    let a = resolved.as_str();
                    let b = expected.as_str();
                    a.zip(b).map(|(x, y)| x == y).or(Some(false))
                }
                "BooleanEquals" => resolved
                    .as_bool()
                    .zip(expected.as_bool())
                    .map(|(x, y)| x == y)
                    .or(Some(false)),
                "IsNull" => {
                    let expect_is_null = expected.as_bool().unwrap_or(false);
                    let is_null = resolved.is_null();
                    Some(is_null == expect_is_null)
                }
                "IsString" | "IsNumeric" | "IsBoolean" | "IsArray" | "IsObject" => None,
                "IsPresent" => {
                    // A json pointer missing from input resolved to Null above;
                    // real presence is tracked by the caller-provided value.
                    let present = resolve_json_path(variable, input).is_some();
                    Some(present == expected.as_bool().unwrap_or(false))
                }
                _ => None,
            };
            if Some(false) == matched {
                return false;
            }
        }
    } else if !(obj.contains_key("And")
        || obj.contains_key("Or")
        || obj.contains_key("Not")
        || obj.get("Next").is_some())
    {
        return false;
    }

    // A rule matches when all its comparators held and it points somewhere.
    // Nested rules have no Next of their own: they match once the comparators
    // held (the surrounding And/Or/Not uses their result).
    if !top_level {
        return true;
    }
    obj.get("Next").and_then(|n| n.as_str()).is_some()
}

fn wait_seconds(obj: &Map<String, Value>, input: &Value) -> f64 {
    if let Some(secs) = obj.get("Seconds").and_then(|v| v.as_f64()) {
        return secs.clamp(0.0, 300.0);
    }
    if let Some(p) = obj.get("SecondsPath").and_then(|v| v.as_str()) {
        if let Some(v) = resolve_json_path(p, input) {
            return v.as_f64().unwrap_or(0.0).clamp(0.0, 300.0);
        }
    }
    0.0
}

fn sleep_with_abort(exec: &Execution, seconds: f64) {
    use std::time::Duration;
    let total = Duration::from_secs_f64(seconds);
    let step = Duration::from_millis(100);
    let mut elapsed = Duration::ZERO;
    while elapsed < total {
        if exec.is_aborted() {
            return;
        }
        let chunk = step.min(total - elapsed);
        std::thread::sleep(chunk);
        elapsed += chunk;
    }
}

/// Extract (states, start_at) from a Branch or Iterator definition.
fn sub_flow_states(container: &Value) -> Result<(Map<String, Value>, String), FlowError> {
    let states = container
        .get("States")
        .and_then(|s| s.as_object())
        .cloned()
        .unwrap_or_default();
    let start = container
        .get("StartAt")
        .and_then(|s| s.as_str())
        .unwrap_or_default()
        .to_string();
    Ok((states, start))
}

/// Execute a `arn:aws:lambda:...` / `:function:` task resource via the lambda
/// crate's bridge. Errors mirror the Python executor: missing functions map
/// to `Lambda.ServiceException`, handler errors to `Lambda.Unknown`, and
/// code-less functions echo the input like moto's simple mode.
fn invoke_lambda_resource(
    resource: &str,
    event: &Value,
    original_input: &Value,
) -> Result<Value, FlowError> {
    match lambda::invoke_by_arn(resource, event) {
        Some(outcome) if outcome.function_error.is_none() => {
            Ok(serde_json::from_str(&outcome.payload).unwrap_or(original_input.clone()))
        }
        Some(outcome) => Err(FlowError::new("Lambda.Unknown", outcome.payload)),
        None => Err(FlowError::new(
            "Lambda.ServiceException",
            format!("Function not found: {resource}"),
        )),
    }
}

/// Recognise `arn:aws:states:::lambda:invoke[.waitForTaskCallback]` SDK
/// integrations; runs the addressed function with the provided payload.
fn lambda_sdk_invoke(resource: &str, input: &Value) -> Option<Result<Value, FlowError>> {
    let service_resource = resource.strip_prefix("arn:aws:states:::")?;
    let (service, action) = match service_resource.split_once(':') {
        Some((s, a)) => (s, a.trim_end_matches(".waitForTaskCallback")),
        None => return None,
    };
    if service != "lambda" || !matches!(action, "invoke") {
        return None;
    }
    Some((|| {
        let fn_arn = input
            .get("FunctionName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        if fn_arn.is_empty() {
            return Err(FlowError::new(
                "Lambda.ServiceException",
                "FunctionName is required".to_string(),
            ));
        }
        let payload = input.get("Payload").unwrap_or(input);
        invoke_lambda_resource(fn_arn, payload, payload)
    })())
}

/// Apply OutputPath to a state output.
fn apply_output_path(path: Option<&str>, input: Value) -> Value {
    if let Some("$") = path {
        return input;
    }
    if let Some(p) = path.and_then(|p| resolve_json_path(p, &input)) {
        return p;
    }
    input
}

/// Run one state-graph flow; returns the flow output value.
///
/// Mirrors the era `ASLExecutor.execute` loop: each visited state is executed
/// through `execute_state`, a raised error flows through `recover_from_error`
/// (the era `_handle_error` Retry/Catch path) and the resulting output becomes
/// the flowing data of the next state.
pub fn run_flow(
    ctx: &Ctx,
    states: &Map<String, Value>,
    start_at: String,
    input: Value,
) -> Result<Value, FlowError> {
    let mut current = start_at;
    let mut data = input;
    // Retry attempt counters keyed by state name — one map per flow level
    // (era keeps `self._retry_counts` on the executing ASLExecutor, so nested
    // Parallel / Map sub-flows count their retries independently).
    let mut retry_counts: HashMap<String, u32> = HashMap::new();

    loop {
        if ctx.exec.is_aborted() {
            // Aborted executions leave the engine without changing state.
            return Ok(data);
        }
        if ctx.exec.events.read().len() >= MAX_EVENT_HISTORY {
            return Err(FlowError::new(
                "States.ExceedMaxEventHistoryReached",
                "Too many events in the execution history".to_string(),
            ));
        }

        let Some(state) = states.get(&current) else {
            return Err(FlowError::new(
                "States.StateNotFound",
                format!("An error occurred while executing the state '{current}'"),
            ));
        };
        let obj = state.as_object().cloned().unwrap_or_default();
        let state_type = obj
            .get("Type")
            .and_then(|t| t.as_str())
            .unwrap_or("Pass")
            .to_string();
        let name = current.clone();

        ctx.emit_entered(&state_type, &name, &data);

        let (output, next_state) = match execute_state(ctx, &name, &obj, &data) {
            Ok(ok) => ok,
            Err(e) => recover_from_error(ctx, &mut retry_counts, &name, &obj, &data, e)?,
        };

        ctx.emit_exited(&state_type, &name, &output);

        if obj.get("End").and_then(|v| v.as_bool()).unwrap_or(false) || next_state.is_none() {
            return Ok(output);
        }
        data = output;
        current = next_state.expect("next_state checked above");
    }
}

/// Execute a single state attempt (era `_execute_state`): run the state's own
/// logic on `data` and return `(state_output, next_state_name)`. Any raised
/// error flows back to the caller, which drives Retry/Catch around it.
fn execute_state(
    ctx: &Ctx,
    name: &str,
    obj: &Map<String, Value>,
    data: &Value,
) -> Result<(Value, Option<String>), FlowError> {
    let state_type = obj.get("Type").and_then(|t| t.as_str()).unwrap_or("Pass");
    let next_state = obj
        .get("Next")
        .and_then(|v| v.as_str())
        .map(|v| v.to_string());
    match state_type {
        "Fail" => {
            let error = obj
                .get("Error")
                .and_then(|e| e.as_str())
                .unwrap_or("")
                .to_string();
            let cause = obj
                .get("Cause")
                .and_then(|e| e.as_str())
                .unwrap_or("")
                .to_string();
            Err(FlowError { error, cause })
        }
        "Succeed" => Ok((data.clone(), None)),
        "Wait" => {
            let seconds = wait_seconds(obj, data);
            if seconds > 0.0 {
                sleep_with_abort(&ctx.exec, seconds);
            }
            if ctx.exec.is_aborted() {
                // Aborted: leave the engine, the loop-top check stops the flow.
                return Ok((data.clone(), None));
            }
            Ok((data.clone(), next_state))
        }
        "Choice" => {
            let mut target: Option<String> = None;
            if let Some(choices) = obj.get("Choices").and_then(|c| c.as_array()) {
                for rule in choices {
                    if eval_choice(rule, data, true) {
                        target = rule.get("Next").and_then(|n| n.as_str()).map(String::from);
                        break;
                    }
                }
            }
            if target.is_none() {
                if let Some(default) = obj.get("Default").and_then(|d| d.as_str()) {
                    target = Some(default.to_string());
                }
            }
            match target {
                Some(next) => Ok((data.clone(), Some(next))),
                None => Err(FlowError::new(
                    "States.NoChoiceMatched",
                    format!("An error occurred while executing the state '{name}'"),
                )),
            }
        }
        "Parallel" => {
            let mut outputs: Vec<Value> = Vec::new();
            if let Some(branches) = obj.get("Branches").and_then(|b| b.as_array()) {
                for branch in branches {
                    let (b_states, b_start) = sub_flow_states(branch)?;
                    let out = run_flow(ctx, &b_states, b_start, data.clone())?;
                    outputs.push(out);
                }
            }
            let merged = apply_result_path(result_path_spec(obj), data, Value::Array(outputs));
            Ok((merged, next_state))
        }
        "Map" => {
            let items = match obj.get("ItemsPath").and_then(|p| p.as_str()) {
                Some(path) => resolve_json_path(path, data).unwrap_or(Value::Null),
                None => data.clone(),
            };
            let mut outputs: Vec<Value> = Vec::new();
            if let Value::Array(array) = items {
                if let Some(iterator) = obj.get("Iterator") {
                    let (it_states, it_start) = sub_flow_states(iterator)?;
                    for item in array {
                        let out = run_flow(ctx, &it_states, it_start.clone(), item)?;
                        outputs.push(out);
                    }
                }
            }
            let merged = apply_result_path(result_path_spec(obj), data, Value::Array(outputs));
            Ok((merged, next_state))
        }
        "Task" => {
            // Lambda integrations run the function synchronously (the
            // execution already runs on a detached thread); other
            // resources complete with the current input like before.
            let resource = obj
                .get("Resource")
                .and_then(|r| r.as_str())
                .unwrap_or_default()
                .to_string();
            let raw_out = if !resource.ends_with(".waitForTaskCallback")
                && (resource.starts_with("arn:aws:lambda:") || resource.contains(":function:"))
            {
                // Ground truth (python asl.py::_dispatch_task): dict
                // input is the event; scalars are wrapped under "input".
                let event = if data.is_object() {
                    data.clone()
                } else {
                    json!({ "input": data })
                };
                invoke_lambda_resource(&resource, &event, data)?
            } else if let Some(sdk_fn) = lambda_sdk_invoke(&resource, data) {
                sdk_fn?
            } else {
                match obj.get("Result") {
                    Some(r) => r.clone(),
                    None => data.clone(),
                }
            };
            let merged = apply_result_path(result_path_spec(obj), data, raw_out);
            Ok((merged, next_state))
        }
        // "Pass" and anything unrecognised passes through.
        _ => {
            let processed = if let Some(params) = obj.get("Parameters").and_then(|p| p.as_object())
            {
                eval_parameters(params, data)
            } else if let Some(result) = obj.get("Result") {
                result.clone()
            } else {
                data.clone()
            };
            let merged = apply_result_path(result_path_spec(obj), data, processed);
            let merged = apply_output_path(obj.get("OutputPath").and_then(|p| p.as_str()), merged);
            Ok((merged, next_state))
        }
    }
}

/// Era Retry/Catch matcher: `"States.ALL" in error_equals or error in
/// error_equals` (`_handle_error` runs this against Retry and Catch blocks).
fn error_equals_match(error_equals: Option<&Value>, error: &str) -> bool {
    error_equals
        .and_then(|v| v.as_array())
        .map(|list| {
            list.iter()
                .any(|e| e.as_str() == Some("States.ALL") || e.as_str() == Some(error))
        })
        .unwrap_or(false)
}

/// Era `_handle_error`: check Retry blocks first — the first matching retrier
/// re-executes the state synchronously (with capped backoff between attempts)
/// until `MaxAttempts` is reached — then Catch blocks, where the first
/// matching catcher's `Next` carries a `{"Error", "Cause"}` merged into the
/// state input at the catcher's `ResultPath`.
fn recover_from_error(
    ctx: &Ctx,
    retry_counts: &mut HashMap<String, u32>,
    name: &str,
    obj: &Map<String, Value>,
    data: &Value,
    err: FlowError,
) -> Result<(Value, Option<String>), FlowError> {
    if let Some(retriers) = obj.get("Retry").and_then(|r| r.as_array()) {
        if let Some(retrier) = retriers
            .iter()
            .find(|r| error_equals_match(r.get("ErrorEquals"), &err.error))
        {
            let max_attempts = retrier
                .get("MaxAttempts")
                .and_then(|v| v.as_f64())
                .unwrap_or(3.0) as u32;
            let interval = retrier
                .get("IntervalSeconds")
                .and_then(|v| v.as_f64())
                .unwrap_or(1.0);
            let backoff = retrier
                .get("BackoffRate")
                .and_then(|v| v.as_f64())
                .unwrap_or(2.0);
            let count = retry_counts.get(name).copied().unwrap_or(0);
            if count < max_attempts {
                retry_counts.insert(name.to_string(), count + 1);
                // Wait with backoff, capped for tests (era:
                // `time.sleep(min(wait_time, 0.1))`).
                let wait_time = interval * backoff.powi(count as i32);
                sleep_with_abort(&ctx.exec, wait_time.min(0.1));
                // Re-execute the state; a repeated failure recurses through
                // the same Retry/Catch handling (era `_handle_error`).
                return match execute_state(ctx, name, obj, data) {
                    Ok(ok) => Ok(ok),
                    Err(e) => recover_from_error(ctx, retry_counts, name, obj, data, e),
                };
            }
            // Retries exhausted, fall through to the Catch blocks.
        }
    }

    if let Some(catchers) = obj.get("Catch").and_then(|c| c.as_array()) {
        if let Some(catch) = catchers
            .iter()
            .find(|c| error_equals_match(c.get("ErrorEquals"), &err.error))
        {
            let error_output = json!({ "Error": err.error, "Cause": err.cause });
            let result_path = match catch.get("ResultPath") {
                None => Some("$"),
                Some(Value::String(s)) => Some(s.as_str()),
                Some(Value::Null) => None,
                Some(_) => None,
            };
            let output = apply_result_path(result_path, data, error_output);
            // Era: `return catch.get("Next"), output` — a catcher without a
            // Next leaves next_state None, which the caller re-raises.
            return match catch.get("Next").and_then(|n| n.as_str()).map(String::from) {
                Some(next) => Ok((output, Some(next))),
                None => Err(err),
            };
        }
    }
    Err(err)
}

/// Kick off an execution on a detached thread: emits ExecutionStarted, walks
/// the ASL graph, then records SUCCEEDED/FAILED unless the execution was
/// aborted in the meantime.
pub fn run_on_thread(exec: Arc<Execution>, definition: String) {
    std::thread::spawn(move || {
        exec.push_event(json!({
            "timestamp": now_f(),
            "type": "ExecutionStarted",
            "previousEventId": 0,
            "executionStartedEventDetails": {
                "input": exec.input,
                "roleArn": "",
            },
        }));
        let ctx = Ctx::new(exec.clone());
        let result = (|| {
            let def: Value = serde_json::from_str(&definition).unwrap_or(Value::Null);
            let states = def
                .get("States")
                .and_then(|s| s.as_object())
                .ok_or_else(|| {
                    FlowError::new("States.StatesMissing", "States is required".to_string())
                })?;
            let start_at = def
                .get("StartAt")
                .and_then(|s| s.as_str())
                .unwrap_or_default()
                .to_string();
            let input: Value = serde_json::from_str(&exec.input).unwrap_or_else(|_| json!({}));
            run_flow(&ctx, states, start_at, input)
        })();
        let last_event_id = ctx.exec.events.read().len() as i64;
        match result {
            Ok(output) => ctx
                .exec
                .mark_succeeded(value_to_string(&output), last_event_id),
            Err(e) => ctx.exec.mark_failed(e.error, e.cause, last_event_id),
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nested_choice_rules() {
        let input = json!({"a": "1", "b": 2});
        let rel = [
            json!({"Variable": "$.a", "StringEquals": "1"}),
            json!({"Variable": "$.b", "NumericEquals": 2}),
        ];
        // Nested leaf rules (no Next) match on their comparators alone.
        let rule = json!({"And": rel, "Next": "Yes"});
        assert!(eval_choice(&rule, &input, true));
        assert!(eval_choice(&rule, &input, false));
        // A nested leaf that does not match keeps the group false.
        let miss = json!({"And": [rel[0], json!({"Variable": "$.b", "NumericEquals": 9})]});
        assert!(!eval_choice(&miss, &input, true));
        // Or / Not leaves.
        assert!(eval_choice(
            &json!({"Or": [json!({"Variable": "$.a", "StringEquals": "9"}), rel[1]], "Next": "Y"}),
            &input,
            true
        ));
        assert!(!eval_choice(
            &json!({"Not": rel[0], "Next": "N"}),
            &input,
            true
        ));
        // Top-level rules still need a Next target.
        assert!(!eval_choice(
            &json!({"Variable": "$.a", "StringEquals": "1"}),
            &input,
            true
        ));
        assert!(eval_choice(
            &json!({"Variable": "$.a", "StringEquals": "1"}),
            &input,
            false
        ));
    }

    #[test]
    fn test_result_path_discard_replace_and_merge() {
        let input = json!({"original": "data", "flag": true});
        // ResultPath "$" — the result replaces the input entirely and keys
        // from the input do NOT leak through (era `_apply_result_path`).
        let replaced = apply_result_path(Some("$"), &input, json!({"completed": true}));
        assert_eq!(replaced, json!({"completed": true}));
        // ResultPath null (None) — input passes through untouched.
        let discarded = apply_result_path(None, &input, json!({"ignored": 1}));
        assert_eq!(discarded, input);
        // Nested path merges into a copy of the input.
        let merged = apply_result_path(Some("$.extra"), &input, json!({"added": true}));
        assert_eq!(
            merged,
            json!({"original": "data", "flag": true, "extra": {"added": true}})
        );
        // Non-object input is started from a fresh object (era: not a dict
        // -> {}).
        let from_scalar = apply_result_path(Some("$.x"), &json!("scalar"), json!(7));
        assert_eq!(from_scalar, json!({"x": 7}));
        // Non-`$`-prefixed path falls back to replacing with the result.
        let other = apply_result_path(Some("weird"), &input, json!(1));
        assert_eq!(other, json!(1));
    }

    // ---- Retry/Catch engine (era `_handle_error`) ----

    use crate::models::Execution;

    fn test_ctx() -> (Ctx, Arc<Execution>) {
        let exec = Arc::new(Execution {
            arn: "arn:aws:states:us-east-1:123456789012:execution:t:tester".into(),
            name: "tester".into(),
            sm_arn: "arn:aws:states:us-east-1:123456789012:stateMachine:t".into(),
            input: "{}".into(),
            start_date: 0.0,
            status: parking_lot::RwLock::new("RUNNING".into()),
            stop_date: parking_lot::RwLock::new(None),
            output: parking_lot::RwLock::new(None),
            error: parking_lot::RwLock::new(None),
            cause: parking_lot::RwLock::new(None),
            events: parking_lot::RwLock::new(Vec::new()),
        });
        (Ctx::new(exec.clone()), exec)
    }

    fn run(def: &Value, input: Value) -> Result<Value, FlowError> {
        let (ctx, _exec) = test_ctx();
        let states = def
            .get("States")
            .and_then(|s| s.as_object())
            .cloned()
            .unwrap_or_default();
        let start_at = def
            .get("StartAt")
            .and_then(|s| s.as_str())
            .unwrap_or_default()
            .to_string();
        run_flow(&ctx, &states, start_at, input)
    }

    /// Build a zip artifact the same way the lambda crate's bridge tests do
    /// (a real zipfile assembled by python3, which the runtime unpacks).
    fn make_zip(code: &str) -> Vec<u8> {
        let dir = std::env::temp_dir().join(format!("sfn-asl-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        let zpath = dir.join("t.zip");
        let snippet = "import io, sys, zipfile\nbuf = io.BytesIO()\nze = zipfile.ZipFile(buf, 'w')\nze.writestr(sys.argv[3], sys.argv[2])\nze.close()\nopen(sys.argv[1], 'wb').write(buf.getvalue())\n";
        let st = std::process::Command::new("python3")
            .arg("-c")
            .arg(snippet)
            .arg(zpath.as_os_str())
            .arg(code)
            .arg("lambda_function.py")
            .stdin(std::process::Stdio::null())
            .output()
            .expect("python3 failed");
        assert!(st.status.success(), "zip maker failed");
        let zip = std::fs::read(&zpath).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        zip
    }

    /// Register a lambda in an isolated account that raises until its
    /// invocation count reaches `fail_below`, then returns the count.
    fn register_counting_function(
        account: u64,
        region: &str,
        name: &str,
        counter_path: &str,
        fail_below: u32,
    ) {
        let code = format!(
            "import os\n\
             COUNT_FILE = {counter_path:?}\n\
             FAIL_BELOW = {fail_below}\n\
             def handler(event, context):\n\
             \x20   n = 0\n\
             \x20   if os.path.exists(COUNT_FILE):\n\
             \x20       with open(COUNT_FILE) as f:\n\
             \x20           n = int(f.read())\n\
             \x20   n += 1\n\
             \x20   with open(COUNT_FILE, 'w') as f:\n\
             \x20       f.write(str(n))\n\
             \x20   if n < FAIL_BELOW:\n\
             \x20       raise RuntimeError('not ready')\n\
             \x20   return {{'attempts': n}}\n"
        );
        let func = lambda::models::LambdaFunction::new(account, region, name.to_string());
        *func.handler.write() = "lambda_function.handler".into();
        *func.code_zip.write() = make_zip(&code);
        let state = lambda::models::LambdaState::new();
        state.functions.write().insert(
            name.to_string(),
            lambda::models::FunctionEntry {
                latest: Some(std::sync::Arc::new(func)),
                versions: Vec::new(),
            },
        );
        lambda::bridge::register_state(account, region, &state);
    }

    #[test]
    fn test_catch_routes_to_next_state_with_error_payload() {
        let def = json!({
            "StartAt": "Boom",
            "States": {
                "Boom": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::lambda:invoke",
                    "Parameters": {"Payload": {"v": 1}},
                    "Catch": [{
                        "ErrorEquals": ["States.ALL"],
                        "Next": "Handle",
                        "ResultPath": "$.cause",
                    }],
                },
                "Handle": {
                    "Type": "Pass",
                    "Result": "handled",
                    "End": true,
                },
            }
        });
        let out = run(&def, json!({"v": 1})).expect("catch routes the task error");
        // The Handle Pass replaces the input with its Result — the point of
        // this test is that the flow CONTINUED at Handle instead of failing.
        assert_eq!(out, json!("handled"));
    }

    #[test]
    fn test_catch_merges_error_and_cause_into_result_path() {
        let def = json!({
            "StartAt": "Boom",
            "States": {
                "Boom": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::lambda:invoke",
                    "Parameters": {"Payload": {"v": 1}},
                    "Catch": [{
                        "ErrorEquals": ["States.ALL"],
                        "Next": "Handle",
                        "ResultPath": "$.cause",
                    }],
                },
                // Echo the (possibly merged) input.
                "Handle": {"Type": "Pass", "End": true},
            }
        });
        let out = run(&def, json!({"v": 1})).expect("catch routes the task error");
        assert_eq!(
            out,
            json!({
                "v": 1,
                "cause": {
                    "Error": "Lambda.ServiceException",
                    "Cause": "FunctionName is required",
                },
            })
        );
    }

    #[test]
    fn test_retry_exhaustion_falls_through_to_catch() {
        let def = json!({
            "StartAt": "Boom",
            "States": {
                "Boom": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::lambda:invoke",
                    "Retry": [{
                        "ErrorEquals": ["States.ALL"],
                        "MaxAttempts": 2,
                        "IntervalSeconds": 0.05,
                        "BackoffRate": 1.0,
                    }],
                    "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Handle"}],
                },
                "Handle": {"Type": "Pass", "End": true},
            }
        });
        let out = run(&def, json!({})).expect("retries exhaust into the catcher");
        // The catcher has no ResultPath, so the era default "$" replaces the
        // input with the {"Error", "Cause"} payload: proving the exhausted
        // retries fell through to Catch (instead of failing the flow).
        assert_eq!(
            out["Error"],
            json!("Lambda.ServiceException"),
            "exhausted retries must land in the Catch block: {out}"
        );
    }

    #[test]
    fn test_retry_recovers_transient_task_failure() {
        let counter = std::env::temp_dir()
            .join(format!("sfn-retry-{}.cnt", uuid::Uuid::new_v4().simple()))
            .to_string_lossy()
            .to_string();
        let (account, region, name) = (834234000001u64, "sfn-retry-test", "flaky-fn");
        register_counting_function(account, region, name, &counter, 3);
        let def = json!({
            "StartAt": "Flaky",
            "States": {
                "Flaky": {
                    "Type": "Task",
                    "Resource": format!("arn:aws:lambda:{region}:{account}:function:{name}"),
                    "Retry": [{
                        "ErrorEquals": ["Lambda.Unknown"],
                        "MaxAttempts": 3,
                        "IntervalSeconds": 0.05,
                        "BackoffRate": 1.0,
                    }],
                    "End": true,
                },
            }
        });
        let out = run(&def, json!({})).expect("retry re-executes until success");
        assert_eq!(out, json!({"attempts": 3}), "handler ran 3 times: {out}");
        let _ = std::fs::remove_file(&counter);
    }

    #[test]
    fn test_uncaught_error_fails_the_flow() {
        let def = json!({
            "StartAt": "Boom",
            "States": {
                "Boom": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::lambda:invoke",
                    "Parameters": {"Payload": {"v": 1}},
                    "Next": "Never",
                },
                "Never": {"Type": "Pass", "End": true},
            }
        });
        let err = run(&def, json!({})).expect_err("task error without Catch fails");
        assert_eq!(err.error, "Lambda.ServiceException");
        assert_eq!(err.cause, "FunctionName is required");
    }

    #[test]
    fn test_retry_counter_is_per_flow_and_per_state() {
        // The era keys retry counts on the executor: two parallel branches
        // hitting the same retrier error share no counters. Exercise that by
        // an always-failing SDK invoke inside each branch with its own Catch.
        let def = json!({
            "StartAt": "Par",
            "States": {
                "Par": {
                    "Type": "Parallel",
                    "Branches": [
                        {
                            "StartAt": "BoomA",
                            "States": {
                                "BoomA": {
                                    "Type": "Task",
                                    "Resource": "arn:aws:states:::lambda:invoke",
                                    "Retry": [{
                                        "ErrorEquals": ["States.ALL"],
                                        "MaxAttempts": 1,
                                        "IntervalSeconds": 0.05,
                                    }],
                                    "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "FallA"}],
                                },
                                "FallA": {"Type": "Pass", "Result": "a", "End": true},
                            },
                        },
                        {
                            "StartAt": "BoomB",
                            "States": {
                                "BoomB": {
                                    "Type": "Task",
                                    "Resource": "arn:aws:states:::lambda:invoke",
                                    "Retry": [{
                                        "ErrorEquals": ["States.ALL"],
                                        "MaxAttempts": 1,
                                        "IntervalSeconds": 0.05,
                                    }],
                                    "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "FallB"}],
                                },
                                "FallB": {"Type": "Pass", "Result": "b", "End": true},
                            },
                        },
                    ],
                    "End": true,
                },
            }
        });
        let out = run(&def, json!({})).expect("branch retries stay isolated");
        assert_eq!(out, json!(["a", "b"]), "both branches completed: {out}");
    }
}
