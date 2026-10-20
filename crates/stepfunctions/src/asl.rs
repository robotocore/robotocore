//! Minimal ASL (Amazon States Language) interpreter for Step Functions.
//!
//! Supports the state types the compatibility suite exercises: Pass
//! (Result/Parameters/ResultPath), Choice (comparators + Default), Fail,
//! Succeed, Wait, Parallel (branches), Map (inline Iterator) and Task
//! (treated as a pass-through). Executions run on a detached thread so
//! StopExecution can abort them while they are waiting.

use crate::models::Execution;
use parking_lot::Mutex;
use serde_json::{json, Map, Value};
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

/// Apply ResultPath semantics: merge the state output into the state input.
fn apply_result_path(path: Option<&str>, input: &Value, state_out: Value) -> Value {
    match path {
        None => state_out,
        Some("$") => {
            let mut merged = input.clone();
            if let (Some(dst), Some(src)) = (merged.as_object_mut(), state_out.as_object()) {
                for (k, v) in src {
                    dst.insert(k.clone(), v.clone());
                }
            }
            merged
        }
        Some(p) if p.starts_with("$.") => {
            let mut merged = input.clone();
            insert_at_path(&mut merged, p, state_out);
            merged
        }
        Some(_) => state_out,
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
pub fn run_flow(
    ctx: &Ctx,
    states: &Map<String, Value>,
    start_at: String,
    input: Value,
) -> Result<Value, FlowError> {
    let mut current = start_at;
    loop {
        if ctx.exec.is_aborted() {
            // Aborted executions leave the engine without changing state.
            return Ok(input);
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
        let is_end = obj.get("End").and_then(|v| v.as_bool()).unwrap_or(false);
        let next_state = obj.get("Next").and_then(|v| v.as_str()).map(String::from);

        ctx.emit_entered(&state_type, &name, &input);

        match state_type.as_str() {
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
                return Err(FlowError { error, cause });
            }
            "Succeed" => {
                return Ok(input);
            }
            "Wait" => {
                let seconds = wait_seconds(&obj, &input);
                if seconds > 0.0 {
                    sleep_with_abort(&ctx.exec, seconds);
                }
                if ctx.exec.is_aborted() {
                    return Ok(input);
                }
                ctx.emit_exited("Wait", &name, &input);
                match next_state {
                    Some(n) => current = n,
                    None if is_end => return Ok(input),
                    None => {
                        return Err(FlowError::new(
                            "States.NoArcMatched",
                            format!("State {name} has neither End nor Next"),
                        ))
                    }
                }
            }
            "Choice" => {
                let mut target: Option<String> = None;
                if let Some(choices) = obj.get("Choices").and_then(|c| c.as_array()) {
                    for rule in choices {
                        if eval_choice(rule, &input, true) {
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
                ctx.emit_exited("Choice", &name, &input);
                match target {
                    Some(n) => current = n,
                    None => {
                        return Err(FlowError::new(
                            "States.NoChoiceMatched",
                            format!("An error occurred while executing the state '{name}'"),
                        ))
                    }
                }
            }
            "Parallel" => {
                let mut outputs: Vec<Value> = Vec::new();
                if let Some(branches) = obj.get("Branches").and_then(|b| b.as_array()) {
                    for branch in branches {
                        let (b_states, b_start) = sub_flow_states(branch)?;
                        let out = run_flow(ctx, &b_states, b_start, input.clone())?;
                        outputs.push(out);
                    }
                }
                let merged = apply_result_path(
                    obj.get("ResultPath").and_then(|p| p.as_str()),
                    &input,
                    Value::Array(outputs),
                );
                ctx.emit_exited("Parallel", &name, &merged);
                if is_end || next_state.is_none() {
                    return Ok(merged);
                }
                current = next_state.unwrap();
            }
            "Map" => {
                let items = match obj.get("ItemsPath").and_then(|p| p.as_str()) {
                    Some(path) => resolve_json_path(path, &input).unwrap_or(Value::Null),
                    None => input.clone(),
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
                let merged = apply_result_path(
                    obj.get("ResultPath").and_then(|p| p.as_str()),
                    &input,
                    Value::Array(outputs),
                );
                ctx.emit_exited("Map", &name, &merged);
                if is_end || next_state.is_none() {
                    return Ok(merged);
                }
                current = next_state.unwrap();
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
                    let event = if input.is_object() {
                        input.clone()
                    } else {
                        json!({ "input": input })
                    };
                    invoke_lambda_resource(&resource, &event, &input)?
                } else if let Some(sdk_fn) = lambda_sdk_invoke(&resource, &input) {
                    sdk_fn?
                } else {
                    match obj.get("Result") {
                        Some(r) => r.clone(),
                        None => input.clone(),
                    }
                };
                let merged = apply_result_path(
                    obj.get("ResultPath").and_then(|p| p.as_str()),
                    &input,
                    raw_out,
                );
                ctx.emit_exited("Task", &name, &merged);
                if is_end || next_state.is_none() {
                    return Ok(merged);
                }
                current = next_state.unwrap();
            }
            // "Pass" and anything unrecognised passes through.
            _ => {
                let processed =
                    if let Some(params) = obj.get("Parameters").and_then(|p| p.as_object()) {
                        eval_parameters(params, &input)
                    } else if let Some(result) = obj.get("Result") {
                        result.clone()
                    } else {
                        input.clone()
                    };
                let merged = apply_result_path(
                    obj.get("ResultPath").and_then(|p| p.as_str()),
                    &input,
                    processed,
                );
                let merged =
                    apply_output_path(obj.get("OutputPath").and_then(|p| p.as_str()), merged);
                ctx.emit_exited(&state_type, &name, &merged);
                if is_end || next_state.is_none() {
                    return Ok(merged);
                }
                current = next_state.unwrap();
            }
        }
    }
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
}
