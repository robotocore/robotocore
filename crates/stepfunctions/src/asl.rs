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

/// Apply a Parameters template exactly like era `_resolve_parameters`:
/// `key.$` entries resolve their value against `input` — `States.*`
/// prefixed strings are intrinsic function calls (the era routes them to
/// `evaluate_intrinsic`; unresolved here, they stay null), `$$` references
/// resolve against the Context Object (top-level only: nested recursion
/// carries no context, like the era), `$.`-paths resolve against the state
/// input, and any other string is a literal value. Non-string `.$` values
/// pass through untouched; nested objects and lists of objects recurse
/// (also without the context).
fn eval_parameters(params: &Map<String, Value>, input: &Value, context: Option<&Value>) -> Value {
    let mut out = Map::new();
    for (k, v) in params {
        if let Some(expr) = k.strip_suffix(".$") {
            let resolved = match v {
                Value::String(s) => evaluate_param_ref(s, input, context),
                // Era: `else: result[real_key] = value` (non-string passes
                // through as-is).
                other => other.clone(),
            };
            out.insert(expr.to_string(), resolved);
        } else {
            out.insert(
                k.clone(),
                match v {
                    Value::Array(items) => Value::Array(
                        items
                            .iter()
                            .map(|item| match item {
                                Value::Object(o) => eval_parameters(o, input, None),
                                other => other.clone(),
                            })
                            .collect(),
                    ),
                    Value::Object(o) => eval_parameters(o, input, None),
                    other => other.clone(),
                },
            );
        }
    }
    Value::Object(out)
}

/// One dynamic `key.$` value from era `_resolve_parameters`.
fn evaluate_param_ref(value: &str, input: &Value, context: Option<&Value>) -> Value {
    if value.starts_with("States.") {
        // Era: `result[real_key] = evaluate_intrinsic(value, input_data)`.
        // Intrinsic evaluation is not part of this lane's findings — an
        // unevaluated intrinsic expression resolves to null.
        Value::Null
    } else if let Some(rest) = value.strip_prefix("$$") {
        // Context Object reference, resolved against the built context when
        // one is in scope (era passes it only at the top call level).
        match context {
            Some(ctx) => resolve_json_path(&format!("${rest}"), ctx).unwrap_or(Value::Null),
            None => Value::Null,
        }
    } else if value.starts_with("$") {
        resolve_json_path(value, input).unwrap_or(Value::Null)
    } else {
        json!(value)
    }
}

/// Era `_build_context` — the Context Object behind `$$` references.
fn build_context(name: &str, ctx: &Ctx) -> Value {
    json!({
        "Execution": {
            "Id": ctx.exec.arn,
            "Name": ctx.exec.arn.rsplit(':').next().unwrap_or_default(),
            "StartTime": "",
        },
        "State": {
            "Name": name,
            "EnteredTime": "",
        },
        "StateMachine": {
            "Id": "",
            "Name": "",
        },
    })
}

/// Numeric compare with era's guard (`isinstance(value, (int, float)) and
/// ...`): both operands must be numeric, any other type fails the rule.
fn numeric_cmp(value: &Value, expected: &Value, f: impl Fn(f64, f64) -> bool) -> bool {
    match (value.as_f64(), expected.as_f64()) {
        (Some(a), Some(b)) => f(a, b),
        _ => false,
    }
}

/// String compare guarding the value type like era `isinstance(value, str)
/// and value > rule[...]`.
fn string_cmp(value: &Value, expected: &Value, f: impl Fn(&str, &str) -> bool) -> bool {
    match (value.as_str(), expected.as_str()) {
        (Some(a), Some(b)) => f(a, b),
        _ => false,
    }
}

/// Bool expectations hold the negation/affirmation flag for the Is* family.
fn is_flag(expected: &Value) -> bool {
    expected.as_bool().unwrap_or(false)
}

/// Era `_evaluate_choice_rule` comparator ladder, in the era source's own
/// order: each rule is evaluated by the FIRST operator it carries.
const CHOICE_COMPARATORS: &[&str] = &[
    "StringEquals",
    "StringEqualsPath",
    "StringGreaterThan",
    "StringLessThan",
    "StringMatches",
    "NumericEquals",
    "NumericGreaterThan",
    "NumericGreaterThanEquals",
    "NumericLessThan",
    "NumericLessThanEquals",
    "BooleanEquals",
    "IsPresent",
    "IsNull",
    "IsString",
    "IsNumeric",
    "IsBoolean",
    "IsTimestamp",
    "StringLessThanEquals",
    "StringGreaterThanEquals",
    "TimestampEquals",
    "TimestampGreaterThan",
    "TimestampGreaterThanEquals",
    "TimestampLessThan",
    "TimestampLessThanEquals",
    "NumericEqualsPath",
    "NumericGreaterThanPath",
    "NumericLessThanPath",
];

/// Resolve a Choice rule path the era way: `_resolve_path` returns None for
/// a missing path and for a null leaf alike, so both land as JSON null.
fn choice_value(path: &str, input: &Value) -> Value {
    resolve_json_path(path, input)
        .filter(|v| !v.is_null())
        .unwrap_or(Value::Null)
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

    // Era: `variable = rule.get("Variable", "$"); value = _resolve_path(data,
    // variable)` — one comparator per rule, picked by the ladder above.
    let Some(op) = CHOICE_COMPARATORS.iter().find(|op| obj.contains_key(**op)) else {
        return if top_level {
            obj.get("Next").is_some()
        } else {
            true
        };
    };
    let expected = &obj[*op];
    let variable = obj.get("Variable").and_then(|v| v.as_str()).unwrap_or("$");
    let value = choice_value(variable, input);
    let matched = match *op {
        "StringEquals" => value == *expected,
        "StringEqualsPath" => expected
            .as_str()
            .map(|p| {
                let other = choice_value(p, input);
                value == other
            })
            .unwrap_or(false),
        "StringGreaterThan" => string_cmp(&value, expected, |a, b| a > b),
        "StringLessThan" => string_cmp(&value, expected, |a, b| a < b),
        "StringGreaterThanEquals" => string_cmp(&value, expected, |a, b| a >= b),
        "StringLessThanEquals" => string_cmp(&value, expected, |a, b| a <= b),
        "StringMatches" => value
            .as_str()
            .zip(expected.as_str())
            .map(|(v, pattern)| fnmatch(v, pattern))
            .unwrap_or(false),
        "NumericEquals" => numeric_cmp(&value, expected, |a, b| a == b),
        "NumericGreaterThan" => numeric_cmp(&value, expected, |a, b| a > b),
        "NumericGreaterThanEquals" => numeric_cmp(&value, expected, |a, b| a >= b),
        "NumericLessThan" => numeric_cmp(&value, expected, |a, b| a < b),
        "NumericLessThanEquals" => numeric_cmp(&value, expected, |a, b| a <= b),
        "BooleanEquals" => value
            .as_bool()
            .zip(expected.as_bool())
            .map(|(a, b)| a == b)
            .unwrap_or(false),
        "IsPresent" => (!value.is_null()) == is_flag(expected),
        "IsNull" => value.is_null() == is_flag(expected),
        "IsString" => value.is_string() == is_flag(expected),
        "IsNumeric" => value.is_number() == is_flag(expected),
        "IsBoolean" => value.is_boolean() == is_flag(expected),
        "IsTimestamp" => value.as_str().map(is_iso_timestamp).unwrap_or(false) == is_flag(expected),
        // Era timestamp comparisons are plain lexicographic string compares.
        "TimestampEquals" => string_cmp(&value, expected, |a, b| a == b),
        "TimestampGreaterThan" => string_cmp(&value, expected, |a, b| a > b),
        "TimestampGreaterThanEquals" => string_cmp(&value, expected, |a, b| a >= b),
        "TimestampLessThan" => string_cmp(&value, expected, |a, b| a < b),
        "TimestampLessThanEquals" => string_cmp(&value, expected, |a, b| a <= b),
        "NumericEqualsPath" => expected
            .as_str()
            .map(|p| numeric_cmp(&value, &choice_value(p, input), |a, b| a == b))
            .unwrap_or(false),
        "NumericGreaterThanPath" => expected
            .as_str()
            .map(|p| numeric_cmp(&value, &choice_value(p, input), |a, b| a > b))
            .unwrap_or(false),
        "NumericLessThanPath" => expected
            .as_str()
            .map(|p| numeric_cmp(&value, &choice_value(p, input), |a, b| a < b))
            .unwrap_or(false),
        other => {
            debug_assert!(!CHOICE_COMPARATORS.contains(&other));
            false
        }
    };

    // A rule matches when its comparators held; nested rules have no Next of
    // their own (the surrounding And/Or/Not uses their result), while a
    // top-level rule still needs its Next target to route to.
    if !top_level {
        return matched;
    }
    matched && obj.get("Next").is_some()
}

/// fnmatch (the repo-runs-on-POSIX form of `fnmatch.fnmatch`): `*`, `?` and
/// `[...]` classes with `!` negation, `]` as a first member literal.
fn fnmatch(text: &str, pattern: &str) -> bool {
    fn inner(t: &[u8], p: &[u8]) -> bool {
        if p.is_empty() {
            return t.is_empty();
        }
        match p[0] {
            b'*' => (0..=t.len()).any(|i| inner(&t[i..], &p[1..])),
            b'?' => !t.is_empty() && inner(&t[1..], &p[1..]),
            b'[' => {
                if t.is_empty() {
                    return false;
                }
                let c = t[0];
                let mut i = 1;
                let negated = if i < p.len() && (p[i] == b'!' || p[i] == b'^') {
                    i += 1;
                    true
                } else {
                    false
                };
                // A leading `]` in the class is a literal member.
                let mut j = if i < p.len() && p[i] == b']' {
                    i + 1
                } else {
                    i
                };
                while j < p.len() && p[j] != b']' {
                    j += 1;
                }
                if j >= p.len() {
                    // Unterminated class: fnmatch treats `[` literally.
                    return c == b'[' && inner(&t[1..], &p[1..]);
                }
                let members = &p[i..j];
                if members.contains(&c) != negated {
                    inner(&t[1..], &p[j + 1..])
                } else {
                    false
                }
            }
            c => !t.is_empty() && t[0] == c && inner(&t[1..], &p[1..]),
        }
    }
    inner(text.as_bytes(), pattern.as_bytes())
}

/// Era `_is_iso_timestamp`:
/// `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$`.
fn is_iso_timestamp(value: &str) -> bool {
    let b = value.as_bytes();
    let digits = |i: usize, n: usize| b[i..i + n].iter().all(|c| c.is_ascii_digit());
    if b.len() < 20
        || !digits(0, 4)
        || b[4] != b'-'
        || !digits(5, 2)
        || b[7] != b'-'
        || !digits(8, 2)
        || b[10] != b'T'
        || !digits(11, 2)
        || b[13] != b':'
        || !digits(14, 2)
        || b[16] != b':'
        || !digits(17, 2)
    {
        return false;
    }
    let mut i = 19;
    if i < b.len() && b[i] == b'.' {
        let start = i + 1;
        while i + 1 < b.len() && b[i + 1].is_ascii_digit() {
            i += 1;
        }
        if i + 1 == start {
            return false;
        }
        i += 1;
    }
    if i < b.len() && b[i] == b'Z' {
        return i + 1 == b.len();
    }
    i + 6 == b.len()
        && (b[i] == b'+' || b[i] == b'-')
        && digits(i + 1, 2)
        && b[i + 3] == b':'
        && digits(i + 4, 2)
}

fn wait_seconds(obj: &Map<String, Value>, input: &Value) -> f64 {
    // Era `_execute_wait` caps waits for tests:
    // `time.sleep(min(state_def["Seconds"], 1))` — "In testing, we don't
    // actually wait long". The cap applies to Seconds and SecondsPath alike;
    // a Timestamp wait never blocks.
    if let Some(secs) = obj.get("Seconds").and_then(|v| v.as_f64()) {
        return secs.clamp(0.0, 1.0);
    }
    if let Some(p) = obj.get("SecondsPath").and_then(|v| v.as_str()) {
        if let Some(v) = resolve_json_path(p, input) {
            return v.as_f64().unwrap_or(0.0).clamp(0.0, 1.0);
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

/// Era `_apply_path(value, path)` for OutputPath-style slicing: an absent
/// path or "$" keeps the value, `$.a.b` resolves (a missing path is null,
/// like era `_resolve_path`'s None), JSON null yields an empty object, and
/// non-path strings pass through.
fn apply_output_path(path: Option<&Value>, input: Value) -> Value {
    match path {
        Some(Value::String(p)) if p == "$" => input,
        Some(Value::String(p)) if p.starts_with("$") => {
            resolve_json_path(p, &input).unwrap_or(Value::Null)
        }
        Some(Value::Null) => json!({}),
        _ => input,
    }
}

/// Era `_apply_path(data, state_def.get("InputPath", "$"))` for the state
/// input, with the same cases as `apply_output_path` but applied to `data`.
fn apply_input_path(data: &Value, path: Option<&Value>) -> Value {
    match path {
        Some(Value::String(p)) if p == "$" => data.clone(),
        Some(Value::String(p)) if p.starts_with("$") => {
            resolve_json_path(p, data).unwrap_or(Value::Null)
        }
        Some(Value::Null) => json!({}),
        _ => data.clone(),
    }
}

/// Era `_execute_state` result tail for the states that produce a result
/// (Task, Wait, Parallel, Map, Pass): ResultSelector resolves the result
/// against itself, ResultPath then merges it into the result base — the
/// flowing state input `data`, or the preprocessed input when InputPath was
/// null — and OutputPath slices the merged output.
fn postprocess_output(
    obj: &Map<String, Value>,
    data: &Value,
    effective: &Value,
    result: Value,
) -> Value {
    let result = match obj.get("ResultSelector") {
        Some(Value::Object(rs)) => eval_parameters(rs, &result, None),
        _ => result,
    };
    // Era: `result_base = effective_input if state_def.get("InputPath") is
    // None else data` — with the key absent, `effective_input` is `data`
    // anyway (the default InputPath is "$").
    let base = match obj.get("InputPath") {
        None | Some(Value::Null) => effective,
        Some(_) => data,
    };
    let out = apply_result_path(result_path_spec(obj), base, result);
    apply_output_path(obj.get("OutputPath"), out)
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

/// Execute a single state attempt (era `_execute_state`): pre-process the
/// input (InputPath, Parameters), run the state's own logic on it and return
/// `(state_output, next_state_name)`. Any raised error flows back to the
/// caller, which drives Retry/Catch around it.
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

    // Input pre-processing (era `_execute_state` head), applied to every
    // state type: InputPath slices the flowing data, then Parameters
    // builds the effective input with the Context Object in scope.
    let effective = apply_input_path(data, obj.get("InputPath"));
    let effective = match obj.get("Parameters") {
        Some(Value::Object(params)) => {
            let context = build_context(name, ctx);
            eval_parameters(params, &effective, Some(&context))
        }
        _ => effective,
    };

    match state_type {
        "Fail" => {
            // Era Fail: dynamic ErrorPath/CausePath resolve from the
            // preprocessed input; defaults are "States.Fail" and "".
            let error = resolve_error_field(obj, &effective, "Error", "ErrorPath", "States.Fail");
            let cause = resolve_error_field(obj, &effective, "Cause", "CausePath", "");
            Err(FlowError { error, cause })
        }
        "Succeed" => Ok((apply_output_path(obj.get("OutputPath"), effective), None)),
        "Wait" => {
            let seconds = wait_seconds(obj, &effective);
            if seconds > 0.0 {
                sleep_with_abort(&ctx.exec, seconds);
            }
            if ctx.exec.is_aborted() {
                // Aborted: leave the engine, the loop-top check stops the flow.
                return Ok((effective, None));
            }
            // Era Wait: `result = effective_input` flows through the tail.
            let output = postprocess_output(obj, data, &effective, effective.clone());
            Ok((output, next_state))
        }
        "Choice" => {
            let mut target: Option<String> = None;
            if let Some(choices) = obj.get("Choices").and_then(|c| c.as_array()) {
                for rule in choices {
                    if eval_choice(rule, &effective, true) {
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
                // Era: `return data, ...` — the Choice state passes the
                // unprocessed data through, only routing.
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
                    let out = run_flow(ctx, &b_states, b_start, effective.clone())?;
                    outputs.push(out);
                }
            }
            let output = postprocess_output(obj, data, &effective, Value::Array(outputs));
            Ok((output, next_state))
        }
        "Map" => {
            // Era `_execute_map`: ItemsPath resolves against the
            // preprocessed input (default "$"), and a non-list result is
            // wrapped into a one-element list:
            // `if not isinstance(items, list): items = [items]`.
            let items = match obj.get("ItemsPath").and_then(|p| p.as_str()) {
                Some(path) => resolve_json_path(path, &effective).unwrap_or(Value::Null),
                None => effective.clone(),
            };
            let items: Vec<Value> = match items {
                Value::Array(array) => array,
                other => vec![other],
            };
            let mut outputs: Vec<Value> = Vec::new();
            // Era iterator selection: `state_def.get("Iterator") or
            // state_def.get("ItemProcessor", {})`.
            if let Some(iterator) = obj.get("Iterator").or_else(|| obj.get("ItemProcessor")) {
                let (it_states, it_start) = sub_flow_states(iterator)?;
                for item in items {
                    let out = run_flow(ctx, &it_states, it_start.clone(), item)?;
                    outputs.push(out);
                }
            }
            let output = postprocess_output(obj, data, &effective, Value::Array(outputs));
            Ok((output, next_state))
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
                let event = if effective.is_object() {
                    effective.clone()
                } else {
                    json!({ "input": effective })
                };
                invoke_lambda_resource(&resource, &event, &effective)?
            } else if let Some(sdk_fn) = lambda_sdk_invoke(&resource, &effective) {
                sdk_fn?
            } else {
                match obj.get("Result") {
                    Some(r) => r.clone(),
                    None => effective.clone(),
                }
            };
            let output = postprocess_output(obj, data, &effective, raw_out);
            Ok((output, next_state))
        }
        // "Pass" and anything unrecognised passes through.
        _ => {
            let result = match obj.get("Result") {
                Some(r) => r.clone(),
                None => effective.clone(),
            };
            Ok((
                postprocess_output(obj, data, &effective, result),
                next_state,
            ))
        }
    }
}

/// Era Fail-state error/cause resolution: a static `Error`/`Cause` value or
/// a dynamic `ErrorPath`/`CausePath` resolved from the preprocessed input;
/// non-string resolutions stringify, null falls back to the given default.
fn resolve_error_field(
    obj: &Map<String, Value>,
    input: &Value,
    static_key: &str,
    path_key: &str,
    default: &str,
) -> String {
    if let Some(path) = obj.get(path_key).and_then(|v| v.as_str()) {
        return match resolve_json_path(path, input) {
            Some(Value::String(s)) => s,
            // Era: `str(cause) if cause is not None else default` — a null
            // resolution behaves like a missing one (era `_resolve_path`
            // returns None for both) and falls through to the static
            // default.
            Some(Value::Null) | None => default.to_string(),
            Some(other) => serde_json::to_string(&other).unwrap_or_else(|_| default.to_string()),
        };
    }
    obj.get(static_key)
        .and_then(|v| v.as_str())
        .unwrap_or(default)
        .to_string()
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

    /// Register a lambda in an isolated account with the given python code.
    fn register_python_function(account: u64, region: &str, name: &str, code: &str) {
        let func = lambda::models::LambdaFunction::new(account, region, name.to_string());
        *func.handler.write() = "lambda_function.handler".into();
        *func.code_zip.write() = make_zip(code);
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

    /// Register a lambda that echoes its event verbatim.
    fn register_echo_function(account: u64, region: &str, name: &str) {
        register_python_function(account, region, name, "def handler(e, c): return e");
    }

    /// Register a lambda that raises until its invocation count reaches
    /// `fail_below`, then returns the count.
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
        register_python_function(account, region, name, &code);
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

    // ---- Input pre-processing for all state types (era `_execute_state`
    // head) ----

    #[test]
    fn test_input_path_applies_to_wait_state() {
        let def = json!({
            "StartAt": "Wait",
            "States": {
                "Wait": {
                    "Type": "Wait",
                    "InputPath": "$.msg",
                    "Next": "Echo",
                },
                "Echo": {"Type": "Pass", "End": true},
            }
        });
        let out = run(&def, json!({"msg": {"payload": 1}, "other": true})).expect("flow completes");
        // The Wait carries its post-InputPath effective input forward; the
        // Pass echoes it (era `_execute_state` slices every state's input).
        assert_eq!(out, json!({"payload": 1}));
    }

    #[test]
    fn test_input_path_applies_to_choice_state() {
        let def = json!({
            "StartAt": "Route",
            "States": {
                "Route": {
                    "Type": "Choice",
                    "InputPath": "$.msg",
                    "Choices": [{
                        "Variable": "$.kind",
                        "StringEquals": "paid",
                        "Next": "Paid",
                    }],
                    "Default": "Free",
                },
                "Paid": {"Type": "Pass", "Result": "paid", "Next": "Done"},
                "Free": {"Type": "Pass", "Result": "free", "Next": "Done"},
                "Done": {"Type": "Pass", "End": true},
            }
        });
        let out = run(&def, json!({"msg": {"kind": "paid"}, "junk": true}))
            .expect("choice matches on the preprocessed input");
        assert_eq!(out, json!("paid"));
    }

    #[test]
    fn test_task_dispatch_receives_parameters_resolved_input() {
        let (account, region, name) = (847009000001u64, "sfn-params-test", "echo-fn");
        register_echo_function(account, region, name);
        let def = json!({
            "StartAt": "Task",
            "States": {
                "Task": {
                    "Type": "Task",
                    "Resource": format!("arn:aws:lambda:{region}:{account}:function:{name}"),
                    "InputPath": "$.event",
                    "Parameters": {"captured.$": "$.waldo"},
                    "End": true,
                }
            }
        });
        let out = run(&def, json!({"event": {"waldo": "found", "extra": 1}}))
            .expect("task runs against the preprocessed parameters");
        // The event sent to the function is the Parameters template resolved
        // against the InputPath-sliced input — anything else is a leak.
        assert_eq!(out, json!({"captured": "found"}), "got {out}");
    }

    #[test]
    fn test_result_selector_selects_from_result() {
        let (account, region, name) = (847009000002u64, "sfn-selector-test", "echo-fn");
        register_echo_function(account, region, name);
        let def = json!({
            "StartAt": "Task",
            "States": {
                "Task": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::lambda:invoke".to_string(),
                    "Parameters": {
                        "FunctionName": format!("arn:aws:lambda:{region}:{account}:function:{name}"),
                        "Payload": {"waldo": "found", "junk": 1},
                    },
                    "ResultSelector": {"picked.$": "$.waldo"},
                    "End": true,
                }
            }
        });
        let out = run(&def, json!({})).expect("result selector applies to the Task result");
        assert_eq!(out, json!({"picked": "found"}), "got {out}");
    }

    #[test]
    fn test_context_object_reference_in_parameters() {
        let def = json!({
            "StartAt": "P",
            "States": {
                "P": {
                    "Type": "Pass",
                    "Parameters": {
                        "execId.$": "$$.Execution.Id",
                        "stateName.$": "$$.State.Name",
                    },
                    "End": true,
                }
            }
        });
        let out = run(&def, json!({})).expect("context references resolve");
        assert_eq!(
            out["execId"],
            json!("arn:aws:states:us-east-1:123456789012:execution:t:tester")
        );
        assert_eq!(out["stateName"], json!("P"));
    }

    #[test]
    fn test_dollar_value_literals_pass_through() {
        let def = json!({
            "StartAt": "P",
            "States": {
                "P": {
                    "Type": "Pass",
                    "Parameters": {
                        "literal.$": "plain-string",
                        "obj.$": {"k": "v"},
                        "num.$": 42,
                        "pathVal.$": "$.real",
                        "intrinsic.$": "States.Format('x', $.real)",
                    },
                    "End": true,
                }
            }
        });
        let out = run(&def, json!({"real": 7})).expect("parameters resolve");
        // Era branch split: strings not starting with $ are literal values;
        // non-string `.$` values pass through untouched.
        assert_eq!(
            out,
            json!({
                "literal": "plain-string",
                "obj": {"k": "v"},
                "num": 42,
                "pathVal": 7,
                // Intrinsic evaluation is not part of this lane's findings;
                // unresolved expressions stay null (their previous behavior).
                "intrinsic": Value::Null,
            })
        );
    }

    #[test]
    fn test_output_path_missing_resolves_null() {
        let def = json!({
            "StartAt": "P",
            "States": {"P": {"Type": "Pass", "OutputPath": "$.missing", "End": true}}
        });
        let out = run(&def, json!({"a": 1})).expect("flow completes");
        // Era `_resolve_path` yields None for a missing OutputPath target,
        // so the state output is JSON null.
        assert_eq!(out, Value::Null);
    }

    #[test]
    fn test_output_path_null_yields_empty_object() {
        let def = json!({
            "StartAt": "P",
            "States": {"P": {"Type": "Pass", "OutputPath": Value::Null, "End": true}}
        });
        let out = run(&def, json!({"a": 1})).expect("flow completes");
        assert_eq!(out, json!({}));
    }

    #[test]
    fn test_fail_error_path_resolves_from_input() {
        let def = json!({
            "StartAt": "FailHere",
            "States": {
                "FailHere": {
                    "Type": "Fail",
                    "ErrorPath": "$.err",
                    "CausePath": "$.why",
                }
            }
        });
        let err =
            run(&def, json!({"err": "DynamicError", "why": "because"})).expect_err("Fail raises");
        assert_eq!(err.error, "DynamicError");
        assert_eq!(err.cause, "because");
        // Non-string resolutions stringify (era `str(...)` fallback).
        let err = run(&def, json!({"err": 42, "why": Value::Null})).expect_err("Fail raises");
        assert_eq!(err.error, "42");
        // Null causeresolution falls back to the era default (empty cause).
        assert_eq!(err.cause, "");
    }

    // ---- Map non-list item wrapping + ItemProcessor (era `_execute_map`)
    // ----

    fn map_definition() -> Value {
        json!({
            "StartAt": "Iterate",
            "States": {
                "Iterate": {
                    "Type": "Map",
                    "ItemProcessor": {
                        "StartAt": "PassItem",
                        "States": {
                            "PassItem": {
                                "Type": "Pass",
                                "Result": {"echo": true},
                                "End": true,
                            }
                        },
                    },
                    "End": true,
                }
            }
        })
    }

    #[test]
    fn test_map_wraps_non_list_item() {
        // An ItemsPath resolving to a non-list (object or null) is wrapped
        // into a one-element list (era `[items]`).
        let mut def = map_definition();
        def["States"]["Iterate"]["ItemsPath"] = json!("$.item");
        let out = run(&def, json!({"item": {"k": 1}})).expect("map runs once");
        assert_eq!(out, json!([{"echo": true}]), "got {out}");
        // Null resoluting also wraps (era: [None]) and the item flows in.
        let out = run(&def, json!({"no_item": true})).expect("map runs on null");
        assert_eq!(out, json!([{"echo": true}]), "got {out}");
    }

    #[test]
    fn test_map_without_items_path_uses_whole_input() {
        // Era: `items_path = state_def.get("ItemsPath", "$")` — the whole
        // preprocessed input is the single iterated item.
        let def = map_definition();
        let out = run(&def, json!({"id": 7})).expect("map runs once");
        assert_eq!(out, json!([{"echo": true}]), "got {out}");
    }

    #[test]
    fn test_map_iterator_and_item_processor_both_work() {
        // Era: `state_def.get("Iterator") or state_def.get("ItemProcessor")`
        // — both container forms drive the iterations.
        let mut def = map_definition();
        def["States"]["Iterate"]["ItemsPath"] = json!("$.items");
        def["States"]["Iterate"]["Iterator"] = def["States"]["Iterate"]["ItemProcessor"].clone();
        let out = run(&def, json!({"items": [1, 2]})).expect("map uses Iterator");
        assert_eq!(out, json!([{"echo": true}, {"echo": true}]), "got {out}");
        let out = run(&def, json!({"items": []})).expect("empty map");
        assert_eq!(out, json!([]), "got {out}");
    }

    // ---- Choice operator set expansion (era `_evaluate_choice_rule`
    // comparator ladder) ----

    /// Build a bare comparator rule (no Next), like a nested And/Or/Not leaf.
    fn comparator_rule(variable: &str, op: &str, expected: Value) -> Value {
        let mut obj = Map::new();
        obj.insert("Variable".to_string(), json!(variable));
        obj.insert(op.to_string(), expected);
        Value::Object(obj)
    }

    #[test]
    fn test_choice_string_comparators() {
        let input = json!({"a": "apple", "b": "banana"});
        let b = "banana";
        // Strict comparators exclude the equal boundary; the *_Equals forms
        // include it.
        assert!(eval_choice(
            &comparator_rule("$.b", "StringGreaterThan", json!("apple")),
            &input,
            false
        ));
        assert!(eval_choice(
            &comparator_rule("$.b", "StringGreaterThanEquals", json!("banana")),
            &input,
            false
        ));
        assert!(!eval_choice(
            &comparator_rule("$.b", "StringGreaterThan", json!("banana")),
            &input,
            false
        ));
        assert!(!eval_choice(
            &comparator_rule("$.a", "StringLessThan", json!("apple")),
            &input,
            false
        ));
        assert!(eval_choice(
            &comparator_rule("$.a", "StringLessThanEquals", json!("banana")),
            &input,
            false
        ));
        let _ = b;
        // Equality against a full string.
        assert!(eval_choice(
            &comparator_rule("$.a", "StringEquals", json!("apple")),
            &input,
            false
        ));
        // StringEqualsPath compares two resolved values: $.a ("apple")
        // against $.a is equal; $.a against $.b ("banana") is not.
        assert!(eval_choice(
            &comparator_rule("$.a", "StringEqualsPath", json!("$.a")),
            &input,
            false
        ));
        // A missing path resolves to null, which is not equal to "apple".
        assert!(!eval_choice(
            &comparator_rule("$.a", "StringEqualsPath", json!("$.missing")),
            &input,
            false
        ));
        // StringMatches (fnmatch): stars, ?, classes, negation.
        assert!(eval_choice(
            &comparator_rule("$.a", "StringMatches", json!("app*")),
            &input,
            false
        ));
        assert!(eval_choice(
            &comparator_rule("$.a", "StringMatches", json!("?????")),
            &input,
            false
        ));
        assert!(eval_choice(
            &comparator_rule("$.a", "StringMatches", json!("[ab]pple")),
            &input,
            false
        ));
        assert!(eval_choice(
            &comparator_rule("$.a", "StringMatches", json!("[!bc]pple")),
            &input,
            false
        ));
        assert!(!eval_choice(
            &comparator_rule("$.a", "StringMatches", json!("banana")),
            &input,
            false
        ));
        // The numeric-on-string type guard lives in
        // test_choice_numeric_and_path_comparators.
    }

    #[test]
    fn test_choice_numeric_and_path_comparators() {
        let input = json!({"n": 5, "m": 7});
        assert!(eval_choice(
            &comparator_rule("$.n", "NumericGreaterThan", json!(4)),
            &input,
            false
        ));
        assert!(eval_choice(
            &comparator_rule("$.n", "NumericLessThanEquals", json!(5)),
            &input,
            false
        ));
        assert!(!eval_choice(
            &comparator_rule("$.n", "NumericGreaterThan", json!(5)),
            &input,
            false
        ));
        // Path comparators resolve both operands against the input.
        assert!(eval_choice(
            &comparator_rule("$.n", "NumericEqualsPath", json!("$.n")),
            &input,
            false
        ));
        assert!(!eval_choice(
            &comparator_rule("$.n", "NumericGreaterThanPath", json!("$.m")),
            &input,
            false
        ));
        assert!(eval_choice(
            &comparator_rule("$.n", "NumericLessThanPath", json!("$.m")),
            &input,
            false
        ));
        // Non-numeric operands fail (era both_numeric guard).
        assert!(!eval_choice(
            &comparator_rule("$.missing", "NumericEqualsPath", json!("$.m")),
            &input,
            false
        ));
        // A bool value is not numeric in the twin's JSON type system.
        assert!(!eval_choice(
            &comparator_rule("$.flag", "NumericEquals", json!(1)),
            &json!({"flag": true}),
            false
        ));
    }

    #[test]
    fn test_choice_is_family_and_timestamp_comparators() {
        let input = json!({"s": "str", "n": 1, "b": true, "nullv": Value::Null});
        assert!(eval_choice(
            &comparator_rule("$.s", "IsString", json!(true)),
            &input,
            false
        ));
        assert!(!eval_choice(
            &comparator_rule("$.n", "IsString", json!(true)),
            &input,
            false
        ));
        assert!(eval_choice(
            &comparator_rule("$.n", "IsNumeric", json!(true)),
            &input,
            false
        ));
        assert!(eval_choice(
            &comparator_rule("$.b", "IsBoolean", json!(true)),
            &input,
            false
        ));
        assert!(!eval_choice(
            &comparator_rule("$.n", "IsBoolean", json!(true)),
            &input,
            false
        ));
        // Era conflation: a missing path and an explicit null both resolve to
        // null, so neither is present and both are null.
        assert!(eval_choice(
            &comparator_rule("$.missing", "IsPresent", json!(false)),
            &input,
            false
        ));
        assert!(!eval_choice(
            &comparator_rule("$.missing", "IsPresent", json!(true)),
            &input,
            false
        ));
        assert!(eval_choice(
            &comparator_rule("$.nullv", "IsNull", json!(true)),
            &input,
            false
        ));
        assert!(eval_choice(
            &comparator_rule("$.missing", "IsNull", json!(true)),
            &input,
            false
        ));
        // Timestamp family: plain lexicographic string comparisons.
        assert!(eval_choice(
            &comparator_rule("$.ts", "IsTimestamp", json!(true)),
            &json!({"ts": "2026-10-25T07:34:04Z"}),
            false
        ));
        assert!(!eval_choice(
            &comparator_rule("$.s", "IsTimestamp", json!(true)),
            &input,
            false
        ));
        assert!(eval_choice(
            &comparator_rule("$.ts", "TimestampEquals", json!("2026-10-25T07:34:04Z")),
            &json!({"ts": "2026-10-25T07:34:04Z"}),
            false
        ));
        assert!(eval_choice(
            &comparator_rule("$.ts", "TimestampLessThan", json!("2027-01-01T00:00:00Z")),
            &json!({"ts": "2026-10-25T07:34:04Z"}),
            false
        ));
        assert!(eval_choice(
            &comparator_rule(
                "$.ts",
                "TimestampGreaterThanEquals",
                json!("2026-10-25T07:34:04Z")
            ),
            &json!({"ts": "2026-10-25T07:34:04Z"}),
            false
        ));
        assert!(!eval_choice(
            &comparator_rule(
                "$.ts",
                "TimestampGreaterThan",
                json!("2027-01-01T00:00:00Z")
            ),
            &json!({"ts": "2026-10-25T07:34:04Z"}),
            false
        ));
    }

    #[test]
    fn test_choice_routes_on_expanded_operator() {
        // End-to-end: StringMatches drives the actual routing.
        let def = json!({
            "StartAt": "Route",
            "States": {
                "Route": {
                    "Type": "Choice",
                    "Choices": [{
                        "Variable": "$.stage",
                        "StringMatches": "*-prod",
                        "Next": "Prod",
                    }],
                    "Default": "Other",
                },
                "Prod": {"Type": "Pass", "Result": "prod", "End": true},
                "Other": {"Type": "Pass", "Result": "other", "End": true},
            }
        });
        let out = run(&def, json!({"stage": "api-prod"})).expect("matches prod");
        assert_eq!(out, json!("prod"), "routing fell through to Default: {out}");
        let out = run(&def, json!({"stage": "api-dev"})).expect("falls to Default");
        assert_eq!(out, json!("other"), "got {out}");
    }

    #[test]
    fn test_wait_caps_seconds_for_tests() {
        // Era `_execute_wait`: `time.sleep(min(state_def["Seconds"], 1))` —
        // a 300s Wait must finish in about a second.
        use std::time::Instant;
        let def = json!({
            "StartAt": "W",
            "States": {
                "W": {"Type": "Wait", "Seconds": 300, "Next": "Done"},
                "Done": {"Type": "Pass", "Result": "done", "End": true},
            }
        });
        let started = Instant::now();
        let out = run(&def, json!({})).expect("wait completes");
        assert_eq!(out, json!("done"));
        assert!(
            started.elapsed() < std::time::Duration::from_secs(3),
            "Wait Seconds=300 must cap at the era's 1s, was {:?}",
            started.elapsed()
        );
    }

    #[test]
    fn test_wait_caps_seconds_path_resolutions() {
        use std::time::Instant;
        let def = json!({
            "StartAt": "W",
            "States": {
                "W": {"Type": "Wait", "SecondsPath": "$.secs", "Next": "Done"},
                "Done": {"Type": "Pass", "Result": "done", "End": true},
            }
        });
        let started = Instant::now();
        let out = run(&def, json!({"secs": 42})).expect("wait completes");
        assert_eq!(out, json!("done"), "Done's Result replaces the output");
        assert!(
            started.elapsed() < std::time::Duration::from_secs(3),
            "SecondsPath resolution must cap at the era's 1s, was {:?}",
            started.elapsed()
        );
    }
}
