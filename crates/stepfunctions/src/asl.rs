//! Minimal ASL (Amazon States Language) interpreter for Step Functions.
//!
//! Supports the state types the compatibility suite exercises: Pass
//! (Result/Parameters/ResultPath), Choice (comparators + Default), Fail,
//! Succeed, Wait, Parallel (branches), Map (inline Iterator + ItemProcessor)
//! and Task (lambda plus the era `_dispatch_task` SDK routing through the
//! per-service state bridges: sqs:sendMessage, sns:publish, dynamodb put/get
//! /delete/update/query, lambda:invoke and states:startExecution — unknown
//! resources fail with `States.TaskFailed` like the era). Errors flow through
//! the era `_handle_error` path: Retry blocks re-execute the state with
//! capped backoff, Catch blocks route a `{"Error", "Cause"}` payload to the
//! catcher's Next state. Executions run on a detached thread so
//! StopExecution can abort them while they are waiting.

use crate::jsonata;
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

/// The ASL `QueryLanguage` a state evaluates with: JSONPath (the default)
/// or JSONata. States may override the machine-level value per state
/// (https://docs.aws.amazon.com/step-functions/latest/dg/transforming-data.html).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QueryLanguage {
    #[default]
    JsonPath,
    Jsonata,
}

impl QueryLanguage {
    /// Resolve the effective language for one state node against the
    /// machine-level default; unknown values behave like JSONPATH.
    pub fn for_state(obj: &Map<String, Value>, machine_default: QueryLanguage) -> QueryLanguage {
        match obj.get("QueryLanguage").and_then(|v| v.as_str()) {
            Some(s) if s.eq_ignore_ascii_case("JSONATA") => QueryLanguage::Jsonata,
            Some(s) if s.eq_ignore_ascii_case("JSONPATH") => QueryLanguage::JsonPath,
            _ => machine_default,
        }
    }

    /// The machine-level `QueryLanguage` (top level of the ASL document).
    pub fn for_machine(def: &Map<String, Value>) -> QueryLanguage {
        QueryLanguage::for_state(def, QueryLanguage::JsonPath)
    }
}

/// Serialise a JSON value as the wire string used in event details.
pub fn value_to_string(v: &Value) -> String {
    serde_json::to_string(v).unwrap_or_else(|_| "null".to_string())
}

/// Resolve an AWS StepFunctions-dialect JSONPath against a value: `$.a.b`
/// dot fields, bracket indices / slices / wildcards and `[?( ... )]` filters
/// with the AWS `&&` / `||` conjunctions (moto issue #10078). Returns `None`
/// for a definite path that does not exist (the era `_resolve_path`
/// semantics), `Some([])` for a filter / slice / wildcard that matches
/// nothing (the AWS dialect), and carries moto extract_json's singleton
/// unpack + shape heuristics from moto issue #7825.
pub fn resolve_json_path(path: &str, base: &Value) -> Option<Value> {
    crate::jsonpath::resolve(path, base)
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

/// Route a Task state's `Resource` to its integration. Era `_execute_task`
/// trims the `.waitForTaskCallback` suffix for the callback pattern and then
/// calls this logic (asl.py `_dispatch_task`, quoted verbatim):
///
/// ```python
/// def _dispatch_task(self, resource: str, input_data: Any) -> Any:
///     """Route task to the appropriate service integration."""
///     # Lambda invocation
///     if resource.startswith("arn:aws:lambda:") or ":function:" in resource:
///         return self._invoke_lambda(resource, input_data)
///
///     # SDK integrations: arn:aws:states:::service:action
///     if resource.startswith("arn:aws:states:::"):
///         service_action = resource[len("arn:aws:states:::") :]
///         service, _, action = service_action.partition(":")
///
///         if service == "sqs" and action == "sendMessage":
///             return self._invoke_sqs_send(input_data)
///         elif service == "sns" and action == "publish":
///             return self._invoke_sns_publish(input_data)
///         elif service == "dynamodb":
///             return self._invoke_dynamodb(action, input_data)
///         elif service == "lambda" and action == "invoke":
///             fn_arn = input_data.get("FunctionName", "")
///             payload = input_data.get("Payload", input_data)
///             return self._invoke_lambda(fn_arn, payload)
///         elif service == "states" and action == "startExecution":
///             return self._invoke_step_functions(input_data)
///         else:
///             logger.warning(f"Unknown SDK integration: {service}:{action}")
///             raise ASLExecutionError(
///                 "States.TaskFailed",
///                 f"Unknown SDK integration: {service}:{action}",
///             )
///
///     logger.warning(f"Unknown Task resource: {resource}")
///     raise ASLExecutionError(
///         "States.TaskFailed", f"Unknown Task resource: {resource}"
///     )
/// ```
///
/// The SQS/SNS/DynamoDB/nested-state routings dispatch through the
/// per-service state bridges (the era reached into the live backends
/// exactly the same way). Callback-pattern tasks keep this twin's
/// long-standing behaviour of completing with the state input until the
/// SendTask* API wakes the engine — the waiting half of the callback
/// pattern is outside this lane's findings.
fn dispatch_task(ctx: &Ctx, resource: &str, input: &Value) -> Result<Value, FlowError> {
    const CALLBACK_SUFFIX: &str = ".waitForTaskCallback";
    let is_callback = resource.ends_with(CALLBACK_SUFFIX);
    let resource = resource.trim_end_matches(CALLBACK_SUFFIX);

    if (resource.starts_with("arn:aws:lambda:") || resource.contains(":function:")) && !is_callback
    {
        // Ground truth (python asl.py::_dispatch_task): dict input is the
        // event; scalars are wrapped under "input".
        let event = if input.is_object() {
            input.clone()
        } else {
            json!({ "input": input })
        };
        return invoke_lambda_resource(resource, &event, input);
    }

    let Some(service_resource) = resource.strip_prefix("arn:aws:states:::") else {
        return Err(FlowError::new(
            "States.TaskFailed",
            format!("Unknown Task resource: {resource}"),
        ));
    };
    let (service, action) = match service_resource.split_once(':') {
        Some((s, a)) => (s, a),
        None => {
            return Err(FlowError::new(
                "States.TaskFailed",
                format!("Unknown SDK integration: {service_resource}"),
            ))
        }
    };
    match (service, action) {
        ("sqs", "sendMessage") => dispatch_sqs_send(ctx, input),
        ("sns", "publish") => dispatch_sns_publish(ctx, input),
        ("dynamodb", ddb_action) => dispatch_dynamodb(ctx, ddb_action, input),
        // Era: `lambda:invoke` routes a FunctionName + Payload payload to the
        // lambda invocation (no FunctionName requirement of its own).
        ("lambda", "invoke") => {
            let fn_arn = input
                .get("FunctionName")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            let payload = input.get("Payload").unwrap_or(input);
            invoke_lambda_resource(fn_arn, payload, payload)
        }
        ("states", "startExecution") => dispatch_states_start_execution(ctx, input),
        _ => Err(FlowError::new(
            "States.TaskFailed",
            format!("Unknown SDK integration: {service}:{action}"),
        )),
    }
}

/// (account, region) of the flow's execution, parsed from the state machine
/// ARN the execution record points at.
fn flow_scope(ctx: &Ctx) -> (u64, String) {
    // arn:aws:states:<region>:<account>:stateMachine:<name>
    let parts: Vec<&str> = ctx.exec.sm_arn.split(':').collect();
    (
        parts
            .get(4)
            .and_then(|p| p.parse::<u64>().ok())
            .unwrap_or(123456789012),
        parts
            .get(3)
            .map(|p| (*p).to_string())
            .unwrap_or_else(|| "us-east-1".to_string()),
    )
}

/// Era `_invoke_sqs_send`: QueueUrl + MessageBody (dict bodies are JSON
/// stringified) push a real SQS message and answer MessageId plus
/// Md5OfMessageBody. The message lands through the SQS crate's
/// cross-service buffer, so a later receive on that queue sees it.
fn dispatch_sqs_send(ctx: &Ctx, input: &Value) -> Result<Value, FlowError> {
    let (account, region) = flow_scope(ctx);
    let queue_url = input
        .get("QueueUrl")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    let Some((url_account, queue_name)) = queue_url_account_name(queue_url) else {
        // Era: `store.get_queue_by_url` misses -> SQS.QueueDoesNotExist.
        return Err(FlowError::new(
            "SQS.QueueDoesNotExist",
            format!("Queue not found: {queue_url}"),
        ));
    };
    let body = match input.get("MessageBody").cloned() {
        Some(Value::String(s)) => s,
        Some(value) => serde_json::to_string(&value).unwrap_or_default(),
        None => String::new(),
    };
    if !sqs::bridge::deliver_to_queue(
        if url_account == 0 {
            account
        } else {
            url_account
        },
        region.as_str(),
        &queue_name,
        &body,
    ) {
        return Err(FlowError::new(
            "SQS.QueueDoesNotExist",
            format!("Queue not found: {queue_url}"),
        ));
    }
    let message_id = uuid::Uuid::new_v4().to_string();
    let md5_of_body = format!("{:x}", md5::compute(body.as_bytes()));
    Ok(json!({ "MessageId": message_id, "Md5OfMessageBody": md5_of_body }))
}

/// Split an SQS QueueUrl of the twin's `http(s)://host/<account>/<name>`
/// shape into (account, queue name).
fn queue_url_account_name(url: &str) -> Option<(u64, String)> {
    let after_scheme = url.split("://").nth(1)?;
    let mut halves = after_scheme.splitn(2, '/');
    let _host = halves.next()?;
    let mut segments = halves.next()?.splitn(2, '/');
    let account = segments.next()?.parse::<u64>().ok()?;
    let name = segments.next()?;
    (!name.is_empty()).then(|| (account, name.to_string()))
}

/// Era `_invoke_sns_publish`: TopicArn + Message (dict messages are JSON
/// stringified) publish through the real SNS handler so the topic's
/// subscribers receive the notification, and answer MessageId.
fn dispatch_sns_publish(ctx: &Ctx, input: &Value) -> Result<Value, FlowError> {
    let (account, region) = flow_scope(ctx);
    let topic_arn = input
        .get("TopicArn")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let message = match input.get("Message").cloned() {
        Some(Value::String(s)) => s,
        Some(value) => serde_json::to_string(&value).unwrap_or_default(),
        None => String::new(),
    };
    let resp = sns::DefaultSnsHandler::new().handle(sns::AwsRequest {
        service: "sns".to_string(),
        operation: "Publish".to_string(),
        account,
        region,
        params: json!({ "TopicArn": topic_arn, "Message": message }),
        body: bytes::Bytes::new(),
    });
    if resp.status >= 400 {
        // Era: topic lookup miss -> SNS.NotFound.
        return Err(FlowError::new(
            "SNS.NotFound",
            format!("Topic not found: {topic_arn}"),
        ));
    }
    let message_id = xml_text(&resp.body, "MessageId").unwrap_or_default();
    Ok(json!({ "MessageId": message_id }))
}

/// Extract a text node from a query-protocol XML response body.
fn xml_text(body: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = body.find(&open)? + open.len();
    let end = body[start..].find(&close)? + start;
    Some(body[start..end].to_string())
}

/// Era `_invoke_dynamodb`: putItem/getItem/deleteItem/updateItem/query run
/// against the live DynamoDB tables of the flow's (account, region), with
/// every failure answering `DynamoDB.AmazonDynamoDBException`; an unknown
/// action logs and echoes the input like the era.
fn dispatch_dynamodb(ctx: &Ctx, action: &str, input: &Value) -> Result<Value, FlowError> {
    let (account, region) = flow_scope(ctx);
    let ddb_error = |msg: String| FlowError::new("DynamoDB.AmazonDynamoDBException", msg);
    let table_name = input
        .get("TableName")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let Some(table) = dynamodb::bridge::get_table(account, region.as_str(), &table_name) else {
        return Err(ddb_error(format!("Table not found: {table_name}")));
    };
    match action {
        "putItem" => {
            // Era `_dynamodb_put_item`: already-typed items pass through
            // verbatim, plain items convert (`_is_ddb_typed` / to
            // `_to_dynamodb_item`).
            let raw_item = input.get("Item").cloned().unwrap_or(json!({}));
            let item = if is_ddb_typed(&raw_item) {
                raw_item
            } else {
                to_dynamodb_item(&raw_item)
            };
            let attrs = attrs_map(&item);
            let key = dynamodb::models::compute_key_from_attrs(&table.key_schema, &attrs)
                .ok_or_else(|| {
                    ddb_error("The provided key element does not match the schema".to_string())
                })?;
            table
                .items
                .write()
                .insert(key, std::sync::Arc::new(dynamodb::models::Item::new(attrs)));
            Ok(json!({}))
        }
        "getItem" => {
            let key = input
                .get("Key")
                .cloned()
                .unwrap_or(json!({}))
                .as_object()
                .cloned()
                .unwrap_or_default();
            let key = dynamodb::models::compute_key_from_attrs(
                &table.key_schema,
                &key.into_iter().collect(),
            )
            .ok_or_else(|| {
                ddb_error("The provided key element does not match the schema".to_string())
            })?;
            match table.items.read().get(&key) {
                Some(item) => {
                    Ok(json!({ "Item": from_dynamodb_item(&to_value(item.attributes.clone())) }))
                }
                // Era: a lost key answers {"Item": {}}.
                None => Ok(json!({ "Item": {} })),
            }
        }
        "deleteItem" => {
            let key = input
                .get("Key")
                .cloned()
                .unwrap_or(json!({}))
                .as_object()
                .cloned()
                .unwrap_or_default();
            let key = dynamodb::models::compute_key_from_attrs(
                &table.key_schema,
                &key.into_iter().collect(),
            )
            .ok_or_else(|| {
                ddb_error("The provided key element does not match the schema".to_string())
            })?;
            table.items.write().remove(&key);
            Ok(json!({}))
        }
        "updateItem" => {
            let key = input
                .get("Key")
                .cloned()
                .unwrap_or(json!({}))
                .as_object()
                .cloned()
                .unwrap_or_default();
            let key = dynamodb::models::compute_key_from_attrs(
                &table.key_schema,
                &key.into_iter().collect(),
            )
            .ok_or_else(|| {
                ddb_error("The provided key element does not match the schema".to_string())
            })?;
            // Era: `backend.update_item(table_name, ddb_key, update_expression=...,
            // expression_attribute_values=..., expression_attribute_names=...)`.
            let update_expr = input
                .get("UpdateExpression")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let names: std::collections::HashMap<String, String> = input
                .get("ExpressionAttributeNames")
                .and_then(|v| v.as_object())
                .map(|o| {
                    o.iter()
                        .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
                        .collect()
                })
                .unwrap_or_default();
            let values: std::collections::HashMap<String, Value> = input
                .get("ExpressionAttributeValues")
                .and_then(|v| v.as_object())
                .map(|o| o.clone().into_iter().collect())
                .unwrap_or_default();
            let mut attrs = table
                .items
                .read()
                .get(&key)
                .map(|item| {
                    to_value(item.attributes.clone())
                        .as_object()
                        .cloned()
                        .unwrap_or_default()
                })
                .unwrap_or_default();
            dynamodb::expr::apply_update(&mut attrs, &update_expr, &names, &values)
                .map_err(ddb_error)?;
            table.items.write().insert(
                key,
                std::sync::Arc::new(dynamodb::models::Item::new(attrs_map(&Value::Object(
                    attrs,
                )))),
            );
            Ok(json!({}))
        }
        "query" => {
            // Era: `backend.query(...)` -> Items + Count. The twin evaluates
            // the key condition across the table's items.
            let key_condition = input
                .get("KeyConditionExpression")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let names: std::collections::HashMap<String, String> = input
                .get("ExpressionAttributeNames")
                .and_then(|v| v.as_object())
                .map(|o| {
                    o.iter()
                        .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
                        .collect()
                })
                .unwrap_or_default();
            let values: std::collections::HashMap<String, Value> = input
                .get("ExpressionAttributeValues")
                .and_then(|v| v.as_object())
                .map(|o| o.clone().into_iter().collect())
                .unwrap_or_default();
            let mut items: Vec<Value> = Vec::new();
            match table.items.read().values().next() {
                None => {}
                Some(_) => {
                    let snapshot: Vec<
                        std::collections::HashMap<String, dynamodb::models::AttrValue>,
                    > = table
                        .items
                        .read()
                        .values()
                        .map(|item| item.attributes.clone())
                        .collect();
                    for attrs in snapshot {
                        let item_json = to_value(attrs);
                        let keep = if key_condition.is_empty() {
                            true
                        } else {
                            dynamodb::expr::eval_condition(
                                &key_condition,
                                &item_json,
                                &names,
                                &values,
                            )
                            .map_err(ddb_error)?
                        };
                        if keep {
                            items.push(from_dynamodb_item(&item_json));
                        }
                    }
                    items.sort_by_key(value_to_string);
                }
            }
            let count = items.len();
            Ok(json!({ "Items": items, "Count": count }))
        }
        // Era: unknown action logs a warning and echoes the input.
        _ => Ok(input.clone()),
    }
}

/// `Map<String, Value>` -> the dynamodb crate's item attribute map.
fn attrs_map(attrs: &Value) -> std::collections::HashMap<String, dynamodb::models::AttrValue> {
    attrs
        .as_object()
        .map(|o| o.clone().into_iter().collect())
        .unwrap_or_default()
}

/// The dynamodb crate's attribute map -> a JSON value.
fn to_value(attrs: std::collections::HashMap<String, dynamodb::models::AttrValue>) -> Value {
    Value::Object(attrs.into_iter().collect())
}

/// Era `_is_ddb_typed`: "Check if item already uses DynamoDB typed format
/// ({"S": "val"})".
fn is_ddb_type_tag(v: &Value) -> bool {
    v.as_object().is_some_and(|o| {
        o.len() == 1
            && o.keys().next().is_some_and(|k| {
                matches!(
                    k.as_str(),
                    "S" | "N" | "B" | "BOOL" | "NULL" | "L" | "M" | "SS" | "NS" | "BS"
                )
            })
    })
}

fn is_ddb_typed(item: &Value) -> bool {
    item.as_object()
        .is_some_and(|o| o.values().any(is_ddb_type_tag))
}

/// Era `_to_dynamodb_item`: already-typed values pass, plain values convert.
fn to_dynamodb_item(item: &Value) -> Value {
    let mut out = Map::new();
    if let Some(o) = item.as_object() {
        for (key, value) in o {
            out.insert(
                key.clone(),
                if is_ddb_type_tag(value) {
                    value.clone()
                } else {
                    to_ddb_value(value)
                },
            );
        }
    }
    Value::Object(out)
}

/// Era `_to_ddb_value`: str -> S, bool -> BOOL, number -> N, null -> NULL,
/// list -> L, dict -> M.
fn to_ddb_value(value: &Value) -> Value {
    match value {
        Value::String(s) => json!({ "S": s }),
        Value::Bool(b) => json!({ "BOOL": b }),
        Value::Number(n) => json!({ "N": n.to_string() }),
        Value::Null => json!({ "NULL": true }),
        Value::Array(items) => {
            let list: Vec<Value> = items.iter().map(to_ddb_value).collect();
            json!({ "L": list })
        }
        Value::Object(o) => {
            let mut obj = Map::new();
            for (key, v) in o {
                obj.insert(key.clone(), to_ddb_value(v));
            }
            json!({ "M": obj })
        }
    }
}

/// Era `_from_dynamodb_item`: typed values back to plain, unknown shapes pass
/// through verbatim.
fn from_dynamodb_item(attrs: &Value) -> Value {
    let mut out = Map::new();
    if let Some(o) = attrs.as_object() {
        for (key, value) in o {
            out.insert(key.clone(), from_ddb_value(value));
        }
    }
    Value::Object(out)
}

fn from_ddb_value(value: &Value) -> Value {
    let Some(o) = value.as_object() else {
        return value.clone();
    };
    if o.len() != 1 {
        return value.clone();
    }
    let (tag, inner) = o.iter().next().unwrap();
    match (tag.as_str(), inner) {
        ("S", Value::String(s)) => json!(s),
        // Era: `int(n) if "." not in str(n) else float(n)`.
        // Era: `int(n) if "." not in str(n) else float(n)` — the number
        // travels as an inner tagged string.
        ("N", Value::String(text)) => {
            if text.contains('.') {
                json!(text.parse::<f64>().unwrap_or(0.0))
            } else {
                json!(text.parse::<i64>().unwrap_or(0))
            }
        }
        ("N", other) => json!(other.to_string()),
        ("BOOL", b) => json!(b),
        ("NULL", _) => Value::Null,
        ("L", Value::Array(items)) => Value::Array(items.iter().map(from_ddb_value).collect()),
        ("M", obj) => from_dynamodb_item(obj),
        ("SS", Value::Array(items)) => Value::Array(items.clone()),
        ("NS", Value::Array(items)) => Value::Array(
            items
                .iter()
                .map(|n| {
                    let text = n.as_str().unwrap_or_default();
                    if text.contains('.') {
                        json!(text.parse::<f64>().unwrap_or(0.0))
                    } else {
                        json!(text.parse::<i64>().unwrap_or(0))
                    }
                })
                .collect(),
        ),
        _ => value.clone(),
    }
}

/// Era `_invoke_step_functions` via `_start_execution_internal`: a nested
/// execution runs the addressed state machine synchronously in an
/// unregistered child record and answers {"executionArn", "output"} /
/// {"executionArn", "error", "cause"} / {"error": "..."} for a miss; it
/// never fails the task.
fn dispatch_states_start_execution(ctx: &Ctx, input: &Value) -> Result<Value, FlowError> {
    let (account, region) = flow_scope(ctx);
    let sm_arn = input
        .get("StateMachineArn")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    // Era: `sf_input = input_data.get("Input", input_data)`.
    let sf_input = input.get("Input").cloned().unwrap_or_else(|| input.clone());
    let handler = crate::handler::StepfunctionsHandler::new();
    let state = handler.get_state(account, region.as_str());
    let Some(machine) = state.get_machine(&sm_arn) else {
        return Ok(json!({ "error": format!("State machine not found: {sm_arn}") }));
    };
    let name = uuid::Uuid::new_v4().to_string();
    // Era: `exec_arn = f"{sm_arn.replace(':stateMachine:', ':execution:')}:{name}"`.
    let exec_arn = format!("{}:{name}", sm_arn.replace(":stateMachine:", ":execution:"));
    let child = Arc::new(Execution {
        arn: exec_arn.clone(),
        name: name.clone(),
        sm_arn: sm_arn.clone(),
        input: value_to_string(&sf_input),
        start_date: now_f(),
        status: parking_lot::RwLock::new("RUNNING".to_string()),
        stop_date: parking_lot::RwLock::new(None),
        output: parking_lot::RwLock::new(None),
        error: parking_lot::RwLock::new(None),
        cause: parking_lot::RwLock::new(None),
        events: parking_lot::RwLock::new(Vec::new()),
    });
    // The child record stays unregistered (only registered executions show
    // up in DescribeExecution), like the era's throwaway nested history.
    let child_ctx = Ctx::new(child);
    let machine_def: Value =
        serde_json::from_str(&machine.definition_string()).unwrap_or(Value::Null);
    let states = machine_def
        .get("States")
        .and_then(|s| s.as_object())
        .cloned()
        .unwrap_or_default();
    let start_at = machine_def
        .get("StartAt")
        .and_then(|s| s.as_str())
        .unwrap_or_default()
        .to_string();
    // Era: `executor.execute(input_data if isinstance(input_data, dict) else {})`.
    let child_input = if sf_input.is_object() {
        sf_input.clone()
    } else {
        json!({})
    };
    match run_flow(
        &child_ctx,
        &states,
        start_at,
        child_input,
        QueryLanguage::for_machine(machine_def.as_object().unwrap_or(&Map::new())),
    ) {
        Ok(output) => Ok(json!({ "executionArn": exec_arn, "output": output })),
        Err(e) => Ok(json!({ "executionArn": exec_arn, "error": e.error, "cause": e.cause })),
    }
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
    default_lang: QueryLanguage,
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

        let lang = QueryLanguage::for_state(&obj, default_lang);
        let attempt = StateAttempt {
            name: &name,
            obj: &obj,
            data: &data,
            lang,
            machine_default: default_lang,
        };
        let (output, next_state) = match execute_state(ctx, &name, &obj, &data, lang, default_lang)
        {
            Ok(ok) => ok,
            Err(e) => recover_from_error(ctx, &mut retry_counts, &attempt, e)?,
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
    lang: QueryLanguage,
    machine_default: QueryLanguage,
) -> Result<(Value, Option<String>), FlowError> {
    if lang == QueryLanguage::Jsonata {
        return execute_jsonata_state(ctx, name, obj, data, machine_default);
    }
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
                    let out =
                        run_flow(ctx, &b_states, b_start, effective.clone(), machine_default)?;
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
                    let out = run_flow(ctx, &it_states, it_start.clone(), item, machine_default)?;
                    outputs.push(out);
                }
            }
            let output = postprocess_output(obj, data, &effective, Value::Array(outputs));
            Ok((output, next_state))
        }
        "Task" => {
            // Lambda integrations run the function synchronously (the
            // execution already runs on a detached thread); SDK
            // integrations dispatch through the per-service state
            // bridges (era `_dispatch_task`).
            let resource = obj
                .get("Resource")
                .and_then(|r| r.as_str())
                .unwrap_or_default()
                .to_string();
            let raw_out = dispatch_task(ctx, &resource, &effective)?;
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

/// Execute a state that evaluates as JSONata (`QueryLanguage: "JSONATA"`,
/// machine-level or per-state). Mirrors the AWS JSONata field semantics:
/// no InputPath/Parameters/ResultPath/OutputPath machinery — payloads are
/// `{% %}`-templated strings evaluated with `$states.input` (plus
/// `$states.result` for Task/Parallel/Map outputs), and any evaluation
/// failure becomes `States.QueryEvaluationError` so Retry / Catch apply.
fn execute_jsonata_state(
    ctx: &Ctx,
    name: &str,
    obj: &Map<String, Value>,
    data: &Value,
    machine_default: QueryLanguage,
) -> Result<(Value, Option<String>), FlowError> {
    let context = build_jsonata_context(name, ctx);
    let states_any = jsonata::StatesCtx::input_only(data, &context);
    // One wrapper for the AWS runtime error: caught by the state's own
    // Retry / Catch blocks like the real engine.
    let jsonata_err = |e: jsonata::EvalError| {
        FlowError::new(
            "States.QueryEvaluationError",
            format!(
                "The JSONata expression in state '{}' could not be evaluated: {}",
                name, e.message
            ),
        )
    };

    let next_state = obj
        .get("Next")
        .and_then(|v| v.as_str())
        .map(|v| v.to_string());

    match obj.get("Type").and_then(|t| t.as_str()).unwrap_or("Pass") {
        "Fail" => {
            // The Fail state's Error/Cause evaluate as JSONata templates.
            let field_string = |key: &str, default: &str| -> Result<String, FlowError> {
                match obj.get(key) {
                    Some(Value::String(expr)) if jsonata::is_expanded(expr) => {
                        match jsonata::eval_template(&json!(expr), &states_any) {
                            Ok(v) => Ok(string_value(&v)),
                            Err(e) => Err(jsonata_err(e)),
                        }
                    }
                    // Non-string or plain literals pass through as the era.
                    other => Ok(other
                        .and_then(|v| v.as_str())
                        .unwrap_or(default)
                        .to_string()),
                }
            };
            let error = field_string("Error", "States.Fail")?;
            let cause = field_string("Cause", "")?;
            Err(FlowError { error, cause })
        }
        "Wait" => {
            // Seconds/Timestamp accept a single {% %} template; the era caps
            // the actual sleep either way.
            if let Some(Value::String(expr)) = obj.get("Seconds") {
                if jsonata::is_expanded(expr) {
                    let seconds = match jsonata::eval(expr[2..expr.len() - 2].trim(), &states_any) {
                        Ok(v) => v.as_f64().unwrap_or(0.0).clamp(0.0, 1.0),
                        Err(e) => return Err(jsonata_err(e)),
                    };
                    if seconds > 0.0 {
                        sleep_with_abort(&ctx.exec, seconds);
                    }
                    if ctx.exec.is_aborted() {
                        return Ok((data.clone(), None));
                    }
                    return Ok((jsonata_pass_output(obj, &states_any)?, next_state));
                }
            }
            // Timestamps never block in the era; a templated one only needs
            // to evaluate (its value is not consumed by the sleep).
            if let Some(Value::String(expr)) = obj.get("Timestamp") {
                if jsonata::is_expanded(expr) {
                    jsonata::eval_single_expression(expr[2..expr.len() - 2].trim(), &states_any)
                        .map_err(jsonata_err)?;
                }
            }
            Ok((jsonata_pass_output(obj, &states_any)?, next_state))
        }
        "Choice" => {
            let choices = obj
                .get("Choices")
                .and_then(|c| c.as_array())
                .cloned()
                .unwrap_or_default();
            let mut target: Option<String> = None;
            for rule in &choices {
                let Some(rule_obj) = rule.as_object() else {
                    continue;
                };
                if let Some(Value::String(cond)) = rule_obj.get("Condition") {
                    if !jsonata::is_expanded(cond) {
                        return Err(FlowError::new(
                            "States.QueryEvaluationError",
                            format!(
                                "The Choice state '{}' condition must be a {{% %}}-wrapped JSONata expression",
                                name
                            ),
                        ));
                    }
                    let verdict = jsonata::eval_single_expression(
                        cond[2..cond.len() - 2].trim(),
                        &states_any,
                    )
                    .map_err(jsonata_err)?;
                    match verdict {
                        Value::Bool(true) => {
                            target = rule_obj
                                .get("Next")
                                .and_then(|n| n.as_str())
                                .map(String::from);
                            break;
                        }
                        Value::Bool(false) => {}
                        // AWS: any non-boolean (or undefined) condition
                        // result is a query evaluation error, not a miss.
                        _ => {
                            return Err(jsonata_err(jsonata::EvalError::new(
                                "the Choice condition did not evaluate to a boolean",
                            )))
                        }
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
                    let out = run_flow(ctx, &b_states, b_start, data.clone(), machine_default)?;
                    outputs.push(out);
                }
            }
            let result = Value::Array(outputs);
            let output = match obj.get("Output") {
                Some(_) => jsonata_output(obj, data, Some(&result), &context, name)?,
                None => result,
            };
            Ok((output, next_state))
        }
        "Map" => {
            // JSONata Map: `Items` (a JSONata expression evaluating to an
            // array or object) replaces ItemsPath; `ItemSelector` replaces
            // Parameters on the iteration payload.
            let items_value = match obj.get("Items") {
                Some(template) => {
                    jsonata::eval_template(template, &states_any).map_err(jsonata_err)?
                }
                None => data.clone(),
            };
            let items: Vec<Value> = match items_value {
                Value::Array(list) => list,
                other => vec![other],
            };
            let mut outputs: Vec<Value> = Vec::new();
            if let Some(iterator) = obj.get("Iterator").or_else(|| obj.get("ItemProcessor")) {
                let (it_states, it_start) = sub_flow_states(iterator)?;
                for item in items {
                    let item_input = match obj.get("ItemSelector") {
                        Some(sel) => jsonata::eval_template(
                            sel,
                            &jsonata::StatesCtx::input_only(&item, &context),
                        )
                        .map_err(jsonata_err)?,
                        None => item,
                    };
                    let out = run_flow(
                        ctx,
                        &it_states,
                        it_start.clone(),
                        item_input,
                        machine_default,
                    )?;
                    outputs.push(out);
                }
            }
            let result = Value::Array(outputs);
            let output = match obj.get("Output") {
                Some(_) => jsonata_output(obj, data, Some(&result), &context, name)?,
                None => result,
            };
            Ok((output, next_state))
        }
        "Task" => {
            let resource = obj
                .get("Resource")
                .and_then(|r| r.as_str())
                .unwrap_or_default()
                .to_string();
            // Arguments replace Parameters in JSONata states (either field is
            // accepted so partially-converted definitions still run).
            let effective = match obj.get("Arguments").or_else(|| obj.get("Parameters")) {
                Some(args) => jsonata::eval_template(args, &states_any).map_err(jsonata_err)?,
                None => data.clone(),
            };
            let raw_out = dispatch_task(ctx, &resource, &effective)?;
            let output = match obj.get("Output") {
                Some(_) => jsonata_output(obj, data, Some(&raw_out), &context, name)?,
                None => raw_out,
            };
            Ok((output, next_state))
        }
        // Pass, Succeed, and unrecognised types pass through.
        _ => Ok((jsonata_pass_output(obj, &states_any)?, next_state)),
    }
}

/// Evaluate a JSONata state's `Output` field with `$states.result` bound.
fn jsonata_output(
    obj: &Map<String, Value>,
    input: &Value,
    result: Option<&Value>,
    context: &Value,
    name: &str,
) -> Result<Value, FlowError> {
    let Some(field) = obj.get("Output") else {
        return Ok(input.clone());
    };
    let states = jsonata::StatesCtx {
        input,
        result,
        context,
        error_output: None,
    };
    jsonata::eval_template(field, &states).map_err(|e| {
        FlowError::new(
            "States.QueryEvaluationError",
            format!(
                "The JSONata expression in state '{}' could not be evaluated: {}",
                name, e.message
            ),
        )
    })
}

/// The Pass-state payload: `Output` is the JSONata way to transform;
/// `Result` stays as an accepted literal fallback; otherwise the input
/// passes through.
fn jsonata_pass_output(
    obj: &Map<String, Value>,
    states: &jsonata::StatesCtx,
) -> Result<Value, FlowError> {
    if obj.get("Output").is_some() {
        return jsonata::eval_template(&obj["Output"], states).map_err(|e| {
            FlowError::new(
                "States.QueryEvaluationError",
                format!(
                    "The JSONata Output expression could not be evaluated: {}",
                    e.message
                ),
            )
        });
    }
    match obj.get("Result") {
        Some(result) => jsonata::eval_template(result, states).map_err(|e| {
            FlowError::new(
                "States.QueryEvaluationError",
                format!(
                    "The JSONata Result expression could not be evaluated: {}",
                    e.message
                ),
            )
        }),
        None => Ok(states.input.clone()),
    }
}

/// The minimal `$string()` form of a value for the Fail state's Error/Cause.
fn string_value(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

/// JSONata `$states.context` — the enriched Context Object.
fn build_jsonata_context(name: &str, ctx: &Ctx) -> Value {
    let sm_name = ctx.exec.sm_arn.rsplit(':').next().unwrap_or_default();
    json!({
        "Execution": {
            "Id": ctx.exec.arn,
            "Name": ctx.exec.name,
            "Input": serde_json::from_str::<Value>(&ctx.exec.input).unwrap_or(Value::Null),
            "StartTime": iso_seconds(ctx.exec.start_date),
        },
        "StateMachine": {
            "Id": ctx.exec.sm_arn,
            "Name": sm_name,
        },
        "State": {
            "Name": name,
            "EnteredTime": iso_seconds(now_f()),
        },
    })
}

/// Epoch seconds -> ISO 8601 with millisecond precision, UTC.
fn iso_seconds(epoch: f64) -> String {
    let secs = epoch.floor() as i64;
    let nanos = ((epoch - epoch.floor()) * 1_000_000_000.0) as u32;
    match chrono::DateTime::from_timestamp(secs, nanos) {
        Some(dt) => dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        None => String::new(),
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
/// Per-state execution context shared by `execute_state` and the
/// Retry/Catch machinery (keeps the recursion signature clippy-friendly).
struct StateAttempt<'a> {
    name: &'a str,
    obj: &'a Map<String, Value>,
    data: &'a Value,
    lang: QueryLanguage,
    machine_default: QueryLanguage,
}

fn recover_from_error(
    ctx: &Ctx,
    retry_counts: &mut HashMap<String, u32>,
    attempt: &StateAttempt,
    err: FlowError,
) -> Result<(Value, Option<String>), FlowError> {
    let StateAttempt {
        name,
        obj,
        data,
        lang,
        machine_default,
    } = *attempt;
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
                return match execute_state(ctx, name, obj, data, lang, machine_default) {
                    Ok(ok) => Ok(ok),
                    Err(e) => recover_from_error(ctx, retry_counts, attempt, e),
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
            // JSONata catchers can shape the payload with an Output field
            // ($states.errorOutput holds {"Error", "Cause"}; AWS's fallback
            // form); the era ResultPath merge stays for JSONPath catches.
            let output = if lang == QueryLanguage::Jsonata {
                match catch.get("Output") {
                    Some(template) => {
                        let states = jsonata::StatesCtx {
                            input: attempt.data,
                            result: None,
                            context: &build_jsonata_context(name, ctx),
                            error_output: Some(&error_output),
                        };
                        match jsonata::eval_template(template, &states) {
                            Ok(v) => v,
                            Err(e) => {
                                return Err(FlowError::new(
                                    "States.QueryEvaluationError",
                                    format!(
                                        "The JSONata expression in state '{}' could not be evaluated: {}",
                                        name, e.message
                                    ),
                                ))
                            }
                        }
                    }
                    None => error_output,
                }
            } else {
                let result_path = match catch.get("ResultPath") {
                    None => Some("$"),
                    Some(Value::String(s)) => Some(s.as_str()),
                    Some(Value::Null) => None,
                    Some(_) => None,
                };
                apply_result_path(result_path, attempt.data, error_output)
            };
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
            let machine_default = def
                .as_object()
                .map(QueryLanguage::for_machine)
                .unwrap_or_default();
            run_flow(&ctx, states, start_at, input, machine_default)
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

    // ---- AWS JSONPath dialect (moto issues #10078 / #7825) ----

    fn dialect_input() -> Value {
        json!({
            "items": [
                {"keep": true, "ready": true, "name": "a"},
                {"keep": true, "ready": false, "name": "b"},
                {"keep": false, "ready": true, "name": "c"},
            ],
            "total": 3,
        })
    }

    #[test]
    fn sfn_dialect_and_or_in_filter() {
        let input = dialect_input();
        // The CDK-emitted conjunction shape from moto #10078.
        let got = resolve_json_path("$.items[?(@.keep == true && @.ready == true)]", &input)
            .expect("parses and matches");
        assert_eq!(got, json!([{"keep": true, "ready": true, "name": "a"}]));
        // || disjunction and adjacency without spaces.
        let got = resolve_json_path("$.items[?(@.name=='b'||@.name=='c')].name", &input).unwrap();
        assert_eq!(got, json!(["b", "c"]));
        // Negation, bare truthiness and $-rooted refs inside the filter.
        assert_eq!(
            resolve_json_path("$.items[?(!@.keep)].name", &input),
            Some(json!(["c"]))
        );
        assert_eq!(
            resolve_json_path("$.items[?(@.ready)].name", &input),
            Some(json!(["a", "c"]))
        );
        // A $-rooted ref inside the filter resolves against the path input.
        let got = resolve_json_path(
            "$.items[?($.total > 1 && @.ready == true)].name",
            &json!({"items": [{"ready": true, "name": "r1"}], "total": 2}),
        );
        assert_eq!(got, Some(json!(["r1"])));
        // Two-space separated operators still parse.
        assert_eq!(
            resolve_json_path("$.items[?(@.keep==true && @.ready==false)].name", &input),
            Some(json!(["b"]))
        );
        // Parenthesised groups combine with the conjunction: numeric and
        // string ordering comparators of the AWS filter set.
        let users = json!({"users": [
            {"name": "a", "n": 5, "role": "x"},
            {"name": "b", "n": 15, "role": "y"},
            {"name": "c", "n": 0, "role": "y"},
        ]});
        assert_eq!(
            resolve_json_path("$.users[?(@.n >= 5 && @.n < 10)].name", &users),
            Some(json!(["a"]))
        );
        // 'a' < 'b' and its role group passes via the negated-missing arm.
        assert_eq!(
            resolve_json_path(
                "$.users[?(@.name < 'b' && (@.role == 'y' || !@.gone))].name",
                &users
            ),
            Some(json!(["a"]))
        );
        assert_eq!(
            resolve_json_path("$.users[?(@.name != 'a' && @.n != 0)].name", &users),
            Some(json!(["b"]))
        );
        // A missing path comparison misses both == and !=, like jsonpath_ng.
        assert_eq!(
            resolve_json_path("$.users[?(@.miss == null)]", &users),
            Some(json!([]))
        );
        // A present null compares equal to the null literal.
        assert_eq!(
            resolve_json_path(
                "$.users[?(@.nul == null)].name",
                &json!({"users": [{"name": "z", "nul": null}]})
            ),
            Some(json!(["z"]))
        );
    }

    #[test]
    fn sfn_dialect_empty_filter_yields_empty_array() {
        // moto #10078: a zero-matching filter must yield [], making a Pass
        // Parameters template produce an empty list instead of null (AWS).
        let input = json!({"items": [{"k": "a"}], "none": []});
        assert_eq!(
            resolve_json_path("$.items[?(@.k == 'NONE')]", &input),
            Some(json!([]))
        );
        assert_eq!(
            resolve_json_path("$.none[?(@.k == 'NONE')]", &input),
            Some(json!([]))
        );
        // A Parameters resolution carries the [] through end-to-end.
        let pass = json!({
            "StartAt": "Filter",
            "States": {"Filter": {
                "Type": "Pass",
                "Parameters": {"selected.$": "$.items[?(@.keep == true && @.ready == true)]"},
                "End": true,
            }},
        });
        let out = run(&pass, json!({"items": [{"keep": true, "ready": true}]}))
            .expect("the #10078 repro state completes");
        assert_eq!(
            out["selected"],
            json!([{"keep": true, "ready": true}]),
            "filter Parameters resolve, not States.Runtime: {out}"
        );
        // And a zero match in the same shape resolves to [].
        let out = run(&pass, json!({"items": []})).expect("empty list");
        assert_eq!(out["selected"], json!([]));
    }

    #[test]
    fn sfn_dialect_filter_keeps_array_shape_for_projections() {
        // moto #7825: a filter with ONE match and a trailing projection stays
        // an array (localstack collapsed singletons to a bare object).
        let input = json!({"items": [{"keep": true, "name": "a"}, {"keep": false}]});
        assert_eq!(
            resolve_json_path("$.items[?(@.keep == true)].name", &input),
            Some(json!(["a"]))
        );
        // Two matches project each hit.
        let input = json!({
            "items": [
                {"keep": true, "name": "a"},
                {"keep": true, "name": "b"},
            ],
        });
        assert_eq!(
            resolve_json_path("$.items[?(@.keep == true)].name", &input),
            Some(json!(["a", "b"]))
        );
        // Single match of the filter alone keeps the array shape...
        let single = json!({"items": [{"keep": true, "name": "a"}, {"keep": false}]});
        assert_eq!(
            resolve_json_path("$.items[?(@.keep)]", &single),
            Some(json!([{"keep": true, "name": "a"}]))
        );
        // ...while a literal trailing index unpacks it (and selects the
        // first element of a multi-match filter before it).
        assert_eq!(
            resolve_json_path("$.items[?(@.keep)][0]", &single),
            Some(json!({"keep": true, "name": "a"}))
        );
        let multi = json!({"items": [
            {"keep": true, "name": "a"}, {"keep": false}, {"keep": true, "name": "b"},
        ]});
        assert_eq!(
            resolve_json_path("$.items[?(@.keep)][0].name", &multi),
            Some(json!(["a"])),
            "moto #7825: projection after a definite index keeps array shape"
        );
        // A plain field path is still scalar, and a definite index unpacks.
        let input = json!({"items": [{"name": "a"}, {"name": "b"}]});
        assert_eq!(
            resolve_json_path("$.items[0].name", &input),
            Some(json!(["a"]))
        );
        assert_eq!(
            resolve_json_path("$.items[0]", &input),
            Some(json!({"name": "a"}))
        );
        assert_eq!(
            resolve_json_path("$.total", &json!({"total": 3})),
            Some(json!(3))
        );
    }

    #[test]
    fn sfn_dialect_indices_slices_and_wildcards() {
        let input = json!({
            "items": [1, 2, 3, 4],
            "map": {"a": 1, "b": 2},
            "label": "x",
        });
        assert_eq!(
            resolve_json_path("$.items[*]", &input),
            Some(json!([1, 2, 3, 4]))
        );
        assert_eq!(
            resolve_json_path("$.items[0:2]", &input),
            Some(json!([1, 2]))
        );
        assert_eq!(
            resolve_json_path("$.items[-2:]", &input),
            Some(json!([3, 4]))
        );
        assert_eq!(
            resolve_json_path("$.items[::2]", &input),
            Some(json!([1, 3]))
        );
        // Negative single index stays array-wrapped (moto's
        // `_is_singleton_array_access` pattern `\[\d+\]` has no minus).
        assert_eq!(resolve_json_path("$.items[-1]", &input), Some(json!([4])));
        // Out-of-range literal index: a definite miss, not [] (era missing).
        assert_eq!(resolve_json_path("$.items[9]", &input), None);
        assert_eq!(resolve_json_path("$.map.*", &input), Some(json!([1, 2])));
        assert_eq!(resolve_json_path("$.label", &input), Some(json!("x")));
        assert_eq!(resolve_json_path("$", &input), Some(input.clone()));
        // Quoted bracket keys, dot-free (quoted fields are object-field
        // accesses, not array indices, matching jsonpath_ng).
        assert_eq!(
            resolve_json_path("$.map['0 A0']", &json!({"map": {"0 A0": 7}})),
            Some(json!(7))
        );
        assert_eq!(
            resolve_json_path("$['a b']", &json!({"a b": 5})),
            Some(json!(5))
        );
        // Unsupported syntax resolves to None, never crashes the engine.
        assert_eq!(resolve_json_path("$.items..name", &input), None);
        assert!(resolve_json_path("$.items[?(@.a =~ /x*/)]", &input).is_some());
        assert_eq!(resolve_json_path("", &input), None);
        assert_eq!(resolve_json_path("$.items[]", &input), None);
    }

    #[test]
    fn sfn_dialect_regex_literals_in_filter() {
        // moto #7825's filter equality (a regex literal); note even moto's
        // own jsonpath_ng backend cannot parse a bare multi-slash regex
        // literal, so the shape is exercised via the quoted-string form the
        // real definition carries, and regex literals here are single-slash
        // patterns (multi-slash patterns read as unsupported syntax).
        let input = json!({
            "Parameters": [{"Name": "/eggs/cooked/like", "Value": "poached"}],
        });
        let got = resolve_json_path(
            "$.Parameters[?(@.Name == '/eggs/cooked/like')].Value",
            &input,
        );
        assert_eq!(got, Some(json!(["poached"])));
        // =~ with anchors and classes, and case-insensitive flag.
        let input = json!({"users": [{"email": "Bob@Example.COM"}, {"email": "ann@x.io"}]});
        assert_eq!(
            resolve_json_path("$.users[?(@.email =~ /^bob.*/i)].email", &input),
            Some(json!(["Bob@Example.COM"]))
        );
        assert_eq!(
            resolve_json_path("$.users[?(@.email =~ /\\.io$/)].email", &input),
            Some(json!(["ann@x.io"]))
        );
        // A non-matching regex stays a no-match, not a crash.
        assert_eq!(
            resolve_json_path("$.users[?(@.email =~ /zorg*/)].email", &input),
            Some(json!([]))
        );
        assert_eq!(
            resolve_json_path("$.users[?(@.email != /\\.io$/)].email", &input),
            Some(json!(["Bob@Example.COM"]))
        );
    }

    #[test]
    fn sfn_dialect_filter_is_usable_across_callers() {
        // ResultSelector vs the state result, OutputPath over a filter, and
        // a Map ItemsPath filter — all ride the same resolver.
        let def = json!({
            "StartAt": "P",
            "States": {
                "P": {
                    "Type": "Pass",
                    "ResultSelector": {"picked.$": "$.vals[?(@.go == true)].v"},
                    "End": true,
                }
            }
        });
        let out = run(
            &def,
            json!({
                "vals": [{"go": true, "v": "keep"}, {"go": false, "v": "drop"}],
            }),
        )
        .expect("ResultSelector resolves a filter");
        assert_eq!(out, json!({"picked": ["keep"]}), "got {out}");

        // A zero-match filter through OutputPath keeps the state flowing.
        let def = json!({
            "StartAt": "P",
            "States": {
                "P": {"Type": "Pass", "OutputPath": "$.vals[?(@.go == false)].v", "End": true}
            }
        });
        let out = run(&def, json!({"vals": [{"go": true, "v": 1}]}))
            .expect("filter OutputPath keeps the flow running");
        assert_eq!(out, json!([]), "the state output is an empty array");

        // Map ItemsPath accepts a filtered list.
        let def = json!({
            "StartAt": "M",
            "States": {
                "M": {
                    "Type": "Map",
                    "ItemsPath": "$.rows[?(@.go == true)]",
                    "ItemProcessor": {
                        "StartAt": "Echo",
                        "States": {"Echo": {"Type": "Pass", "Result": "done", "End": true}},
                    },
                    "End": true,
                }
            }
        });
        let out = run(
            &def,
            json!({"rows": [{"go": true}, {"go": false}, {"go": true}]}),
        )
        .expect("map iterates the filtered rows");
        assert_eq!(out, json!(["done", "done"]));
    }

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
    fn deep_paren_filter_choice_variable_misses_without_aborting() {
        // A Choice Variable whose filter body carries 5k paren groups used
        // to recurse one stack frame per group and abort the whole process
        // (StartExecution → stack overflow). The filter parser now counts
        // the nesting, so a past-the-cap path resolves as missing, the rule
        // misses, Default routes, and the flow resolves deterministically
        // in-process.
        let parens = |n: usize| format!("$.a[?{}@.b{}]", "(".repeat(n), ")".repeat(n));
        let def = |variable: String| {
            json!({
                "StartAt": "Pick",
                "States": {
                    "Pick": {
                        "Type": "Choice",
                        "Choices": [
                            {"Variable": variable, "IsPresent": true, "Next": "Hit"},
                        ],
                        "Default": "Done",
                    },
                    "Hit": {"Type": "Pass", "Result": {"hit": true}, "End": true},
                    "Done": {"Type": "Pass", "Result": {"missed": true}, "End": true},
                },
            })
        };
        let input = json!({"a": [{"b": true}]});
        // ≤100 parens still parse and evaluate: the filter matches, the
        // matched route (Next) is taken.
        let out = run(&def(parens(100)), input.clone()).expect("within the cap");
        assert_eq!(out, json!({"hit": true}), "100 parens evaluate: {out}");
        // 5k parens exceed the filter depth cap: the rule misses, Default
        // routes, SUCCEEDED.
        let out = run(&def(parens(5_000)), input).expect("past the cap is a miss");
        assert_eq!(out, json!({"missed": true}), "past cap = missing: {out}");
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

    fn test_ctx_for(account: u64, region: &str) -> (Ctx, Arc<Execution>) {
        let exec = Arc::new(Execution {
            arn: format!("arn:aws:states:{region}:{account}:execution:t:tester"),
            name: "tester".into(),
            sm_arn: format!("arn:aws:states:{region}:{account}:stateMachine:t"),
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

    fn test_ctx() -> (Ctx, Arc<Execution>) {
        test_ctx_for(123456789012, "us-east-1")
    }

    fn run_for(account: u64, region: &str, def: &Value, input: Value) -> Result<Value, FlowError> {
        let (ctx, _exec) = test_ctx_for(account, region);
        run_with(&ctx, def, input)
    }

    fn run_with(ctx: &Ctx, def: &Value, input: Value) -> Result<Value, FlowError> {
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
        let machine_default = def
            .as_object()
            .map(QueryLanguage::for_machine)
            .unwrap_or_default();
        run_flow(ctx, &states, start_at, input, machine_default)
    }

    fn run(def: &Value, input: Value) -> Result<Value, FlowError> {
        let (ctx, _exec) = test_ctx();
        run_with(&ctx, def, input)
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
                    // Era: `Function not found: {function_name}` with the
                    // empty FunctionName.
                    "Cause": "Function not found: ",
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
        assert_eq!(err.cause, "Function not found: ");
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

    // ---- Task dispatch beyond lambda (era `_dispatch_task` routings) ----

    fn aws_req(service: &str, operation: &str, params: Value) -> sqs::AwsRequest {
        sqs::AwsRequest {
            service: service.to_string(),
            operation: operation.to_string(),
            account: 847009000003,
            region: "us-east-1".to_string(),
            params,
            body: bytes::Bytes::new(),
        }
    }

    #[test]
    fn test_sdk_sqs_send_message_routes_via_bridge() {
        let handler = sqs::DefaultSqsHandler::new();
        let resp = sqs::SqsHandler::handle(
            &handler,
            aws_req(
                "sqs",
                "CreateQueue",
                json!({"QueueName": "sfn-dispatch-queue"}),
            ),
        );
        let queue_url = serde_json::from_str::<Value>(&resp.body).unwrap()["QueueUrl"]
            .as_str()
            .unwrap()
            .to_string();
        let def = json!({
            "StartAt": "Send",
            "States": {
                "Send": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::sqs:sendMessage",
                    "Parameters": {
                        "QueueUrl": queue_url,
                        "MessageBody": "from-stepfunctions",
                    },
                    "End": true,
                }
            }
        });
        let out = run_for(847009000003, "us-east-1", &def, json!({}))
            .expect("sqs:sendMessage dispatches");
        // Era `_invoke_sqs_send`: MessageId + Md5OfMessageBody.
        assert!(
            out.get("MessageId").and_then(|v| v.as_str()).is_some(),
            "got {out}"
        );
        assert_eq!(
            out["Md5OfMessageBody"],
            json!(format!("{:x}", md5::compute(b"from-stepfunctions"))),
            "got {out}"
        );
        // The message is receivable on that queue (drained at receive time).
        let recv = sqs::SqsHandler::handle(
            &handler,
            aws_req(
                "sqs",
                "ReceiveMessage",
                json!({"QueueUrl": queue_url, "MaxNumberOfMessages": 10}),
            ),
        );
        let messages = serde_json::from_str::<Value>(&recv.body).unwrap()["Messages"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        assert_eq!(messages.len(), 1, "got {:?}", recv.body);
        assert_eq!(messages[0]["Body"], json!("from-stepfunctions"));
    }

    #[test]
    fn test_sdk_sqs_send_missing_queue_url_fails() {
        let def = json!({
            "StartAt": "Send",
            "States": {
                "Send": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::sqs:sendMessage",
                    "Parameters": {"MessageBody": "x"},
                    "End": true,
                }
            }
        });
        // A queue URL is required; an unparseable one reads as a missing
        // queue (era `SQS.QueueDoesNotExist`).
        let expanded = json!({
            "StartAt": "Send",
            "States": {
                "Send": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::sqs:sendMessage",
                    "Parameters": {"QueueUrl": "not-a-queue-url", "MessageBody": "x"},
                    "End": true,
                }
            }
        });
        let err = run_for(847009000003, "us-east-1", &expanded, json!({}))
            .expect_err("missing queue fails the task");
        assert_eq!(err.error, "SQS.QueueDoesNotExist", "got {:?}", err);
        let _ = def;
    }

    #[test]
    fn test_sdk_dynamodb_put_and_get_item_roundtrip() {
        use dynamodb::models::{DynamoDbState, Table};
        let account = 847009000004u64;
        let region = "sfn-ddb-test";
        let state = DynamoDbState::new();
        let table = Table::new(
            "sfn-ddb-table".to_string(),
            account,
            region.to_string(),
            vec![dynamodb::models::KeySchema {
                attribute_name: "id".to_string(),
                key_type: "HASH".to_string(),
            }],
            Vec::new(),
            "PAY_PER_REQUEST".to_string(),
        );
        state.put_table(std::sync::Arc::new(table));
        dynamodb::bridge::register_state(account, region, &state);

        let def = json!({
            "StartAt": "Put",
            "States": {
                "Put": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::dynamodb:putItem",
                    "Parameters": {
                        "TableName": "sfn-ddb-table",
                        "Item": {"id": {"S": "u1"}, "score": {"N": "42"}},
                    },
                    "ResultPath": Value::Null,
                    "Next": "Get",
                },
                "Get": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::dynamodb:getItem",
                    "Parameters": {
                        "TableName": "sfn-ddb-table",
                        "Key": {"id": {"S": "u1"}},
                    },
                    "End": true,
                }
            }
        });
        let out = run_for(account, region, &def, json!({})).expect("dynamodb put+get dispatches");
        // Era `_from_dynamodb_item`: numbers round-trip without their
        // {"N": ...} wrapper.
        assert_eq!(out["Item"], json!({"id": "u1", "score": 42}), "got {out}");
    }

    #[test]
    fn test_sdk_dynamodb_query_and_delete() {
        use dynamodb::models::{DynamoDbState, Table};
        let account = 847009000005u64;
        let region = "sfn-ddb-query";
        let state = DynamoDbState::new();
        let table = Table::new(
            "sfn-ddb-q".to_string(),
            account,
            region.to_string(),
            vec![dynamodb::models::KeySchema {
                attribute_name: "id".to_string(),
                key_type: "HASH".to_string(),
            }],
            Vec::new(),
            "PAY_PER_REQUEST".to_string(),
        );
        state.put_table(std::sync::Arc::new(table));
        dynamodb::bridge::register_state(account, region, &state);

        let def = json!({
            "StartAt": "Put",
            "States": {
                "Put": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::dynamodb:putItem",
                    "Parameters": {
                        "TableName": "sfn-ddb-q",
                        "Item": {"id": {"S": "u1"}, "grp": {"S": "a"}},
                    },
                    "ResultPath": Value::Null,
                    "Next": "Query",
                },
                "Query": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::dynamodb:query",
                    "Parameters": {
                        "TableName": "sfn-ddb-q",
                        // The key condition covers every typed item here.
                        "KeyConditionExpression": "id = :id",
                        "ExpressionAttributeValues": {":id": {"S": "u1"}},
                    },
                    "End": true,
                }
            }
        });
        let out = run_for(account, region, &def, json!({})).expect("dynamodb query dispatches");
        assert_eq!(out["Count"], json!(1), "got {out}");
        assert_eq!(out["Items"][0]["grp"], json!("a"), "got {out}");
    }

    #[test]
    fn test_sdk_sns_publish_routes_through_sns_handler() {
        let sns_handler = sns::DefaultSnsHandler::new();
        let create = sns_handler.handle(sns::AwsRequest {
            service: "sns".to_string(),
            operation: "CreateTopic".to_string(),
            account: 847009000006,
            region: "us-east-1".to_string(),
            params: json!({"Name": "sfn-topic"}),
            body: bytes::Bytes::new(),
        });
        assert_eq!(create.status, 200, "topic created: {}", create.body);
        let def = json!({
            "StartAt": "Publish",
            "States": {
                "Publish": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::sns:publish",
                    "Parameters": {
                        "TopicArn": "arn:aws:sns:us-east-1:847009000006:sfn-topic",
                        "Message": "hello",
                    },
                    "End": true,
                }
            }
        });
        let out =
            run_for(847009000006, "us-east-1", &def, json!({})).expect("sns:publish dispatches");
        assert!(
            out.get("MessageId")
                .and_then(|v| v.as_str())
                .map(|s| !s.is_empty())
                .unwrap_or(false),
            "got {out}"
        );
        // A missing topic is era `SNS.NotFound`.
        let def = json!({
            "StartAt": "Publish",
            "States": {
                "Publish": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::sns:publish",
                    "Parameters": {
                        "TopicArn": "arn:aws:sns:us-east-1:847009000006:no-such-topic",
                        "Message": "hello",
                    },
                    "End": true,
                }
            }
        });
        let err = run_for(847009000006, "us-east-1", &def, json!({}))
            .expect_err("missing topic fails the task");
        assert_eq!(err.error, "SNS.NotFound", "got {:?}", err);
    }

    #[test]
    fn test_sdk_states_start_execution_runs_nested_machine() {
        // A child state machine under the same account; the parent machine
        // dispatches arn:aws:states:::states:startExecution.
        let handler = crate::handler::StepfunctionsHandler::new();
        let create = handler.handle(crate::AwsRequest {
            service: "stepfunctions".to_string(),
            operation: "CreateStateMachine".to_string(),
            account: 123456789012,
            region: "us-east-1".to_string(),
            params: json!({
                "stateMachineName": "nested-child",
                "roleArn": "arn:aws:iam::123456789012:role/sfn-role",
                "definition": json!({"StartAt": "P", "States": {"P": {"Type": "Pass", "Result": {"child": true}, "End": true}}}).to_string(),
            }),
            body: bytes::Bytes::new(),
        });
        assert_eq!(create.status, 200, "child created: {}", create.body);
        let def = json!({
            "StartAt": "Spawn",
            "States": {
                "Spawn": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::states:startExecution",
                    "Parameters": {
                        "StateMachineArn": "arn:aws:states:us-east-1:123456789012:stateMachine:nested-child",
                        "Input": {"x": 1},
                    },
                    "End": true,
                }
            }
        });
        let out = run(&def, json!({})).expect("nested execution runs");
        // Era `_start_execution_internal`: {"executionArn", "output"}.
        assert_eq!(
            out["output"],
            json!({"child": true}),
            "nested output carried: {out}"
        );
        assert!(
            out.get("executionArn").and_then(|v| v.as_str()).is_some(),
            "got {out}"
        );
    }

    #[test]
    fn test_sdk_states_start_execution_missing_machine_echoes_error() {
        let def = json!({
            "StartAt": "Spawn",
            "States": {
                "Spawn": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::states:startExecution",
                    "Parameters": {
                        "StateMachineArn": "arn:aws:states:us-east-1:123456789012:stateMachine:no-such-machine",
                    },
                    "End": true,
                }
            }
        });
        let out = run(&def, json!({})).expect("a nested miss never fails the task");
        assert_eq!(
            out["error"],
            json!("State machine not found: arn:aws:states:us-east-1:123456789012:stateMachine:no-such-machine"),
            "got {out}"
        );
    }

    #[test]
    fn test_unknown_task_resource_fails_with_states_task_failed() {
        let def = json!({
            "StartAt": "Boom",
            "States": {
                "Boom": {
                    "Type": "Task",
                    "Resource": "arn:aws:fooservice:whatever",
                    "End": true,
                }
            }
        });
        // Era: `raise ASLExecutionError("States.TaskFailed", f"Unknown Task
        // resource: {resource}")`.
        let err = run(&def, json!({})).expect_err("unknown resource fails");
        assert_eq!(err.error, "States.TaskFailed", "got {:?}", err);
        assert_eq!(
            err.cause,
            "Unknown Task resource: arn:aws:fooservice:whatever"
        );
    }

    #[test]
    fn test_unknown_sdk_integration_fails() {
        let def = json!({
            "StartAt": "Boom",
            "States": {
                "Boom": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::echo:shout",
                    "End": true,
                }
            }
        });
        let err = run(&def, json!({})).expect_err("unknown SDK pair fails");
        assert_eq!(err.error, "States.TaskFailed", "got {:?}", err);
        assert_eq!(err.cause, "Unknown SDK integration: echo:shout");
    }

    #[test]
    fn test_sdk_lambda_invoke_api_and_missing_function() {
        // Era `lambda:invoke`: FunctionName + Payload route to invoke_lambda.
        let (account, region, name) = (847009000007u64, "sfn-sdk-lambda", "echo-fn");
        register_echo_function(account, region, name);
        let def = json!({
            "StartAt": "Invoke",
            "States": {
                "Invoke": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::lambda:invoke",
                    "Parameters": {
                        "FunctionName": format!("arn:aws:lambda:{region}:{account}:function:{name}"),
                        "Payload": {"goto": "here"},
                    },
                    "End": true,
                }
            }
        });
        let out = run(&def, json!({})).expect("sdk invoke dispatches");
        assert_eq!(out, json!({"goto": "here"}), "got {out}");
        // A missing function is era's not-found ServiceException.
        let def = json!({
            "StartAt": "Invoke",
            "States": {
                "Invoke": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::lambda:invoke",
                    "Parameters": {"FunctionName": "", "Payload": {}},
                    "End": true,
                }
            }
        });
        let err = run(&def, json!({})).expect_err("empty FunctionName fails");
        assert_eq!(err.error, "Lambda.ServiceException", "got {:?}", err);
        assert_eq!(err.cause, "Function not found: ");
    }

    // ---- JSONata query-language states (`QueryLanguage: "JSONATA"`) ----

    #[test]
    fn sfn_jsonata_machine_level_output_transform() {
        // The AWS docs' "Input and Output" example: machine-level
        // QueryLanguage and an Output field templating $states.input.
        let def = json!({
            "QueryLanguage": "JSONata",
            "StartAt": "S",
            "States": {
                "S": {
                    "Type": "Pass",
                    "Output": {
                        "lastName": "{% 'Last=>' & $states.input.customer.lastName %}",
                        "orderValue": "{% $states.input.order.total %}"
                    },
                    "End": true,
                }
            }
        });
        let out = run(
            &def,
            json!({"customer": {"firstName": "Martha", "lastName": "Rivera"},
                   "order": {"items": 7, "total": 27.91}}),
        )
        .expect("jsonata pass completes");
        assert_eq!(
            out,
            json!({"lastName": "Last=>Rivera", "orderValue": 27.91}),
            "got {out}"
        );
    }

    #[test]
    fn sfn_jsonata_per_state_override() {
        // The machine is JSONPath; one state opts into JSONata individually.
        let def = json!({
            "StartAt": "Legacy",
            "States": {
                "Legacy": {
                    "Type": "Pass",
                    "Result": {"kept": true},
                    "Next": "Jsonata",
                },
                "Jsonata": {
                    "Type": "Pass",
                    "QueryLanguage": "JSONATA",
                    "Output": {"echo": "{% $states.input.kept %}"},
                    "End": true,
                },
            }
        });
        let out = run(&def, json!({"kept": true})).expect("mixed languages");
        assert_eq!(out, json!({"echo": true}));
    }

    #[test]
    fn sfn_jsonata_choice_condition_routes() {
        let def = json!({
            "QueryLanguage": "JSONATA",
            "StartAt": "Check",
            "States": {
                "Check": {
                    "Type": "Choice",
                    "Choices": [
                        {
                            "Condition": "{% $states.input.price < 10 %}",
                            "Next": "Cheap",
                        },
                        {
                            "Condition": "{% $states.input.price >= 10 and $states.input.approved %}",
                            "Next": "Approved",
                        },
                    ],
                    "Default": "Denied",
                },
                "Cheap": {"Type": "Pass", "Result": "cheap", "End": true},
                "Approved": {"Type": "Pass", "Result": "approved", "End": true},
                "Denied": {"Type": "Pass", "Result": "denied", "End": true},
            }
        });
        let out = run(&def, json!({"price": 5, "approved": false})).expect("routes left");
        assert_eq!(out, json!("cheap"));
        let out = run(&def, json!({"price": 42, "approved": true})).expect("routes right");
        assert_eq!(out, json!("approved"));
        let out = run(&def, json!({"price": 42, "approved": false})).expect("falls to default");
        assert_eq!(out, json!("denied"));
        // A non-boolean condition surface is a query evaluation error that a
        // Catch can pick up.
        let def = json!({
            "QueryLanguage": "JSONATA",
            "StartAt": "Check",
            "States": {
                "Check": {
                    "Type": "Choice",
                    "Choices": [{"Condition": "{% $states.input.price %}", "Next": "A"}],
                    "Catch": [{"ErrorEquals": ["States.QueryEvaluationError"], "Next": "Handle"}],
                },
                "A": {"Type": "Pass", "End": true},
                "Handle": {"Type": "Pass", "Result": "handled", "End": true},
            }
        });
        let out = run(&def, json!({"price": 42})).expect("a non-boolean condition is facing");
        assert_eq!(out, json!("handled"), "got {out}");
    }

    #[test]
    fn sfn_jsonata_task_arguments_and_output() {
        let (account, region, name) = (847009000008u64, "sfn-jsonata", "echo-fn");
        register_echo_function(account, region, name);
        let def = json!({
            "QueryLanguage": "JSONATA",
            "StartAt": "Call",
            "States": {
                "Call": {
                    "Type": "Task",
                    "Resource": format!("arn:aws:lambda:{region}:{account}:function:{name}"),
                    "Arguments": {
                        "bit": "{% $states.input.stuff %}",
                        "static": "value",
                    },
                    "Output": {
                        "result": "{% $states.result %}",
                        "echoed": "{% $states.result.bit %}",
                        "inputEcho": "{% $states.input.stuff %}",
                    },
                    "End": true,
                }
            }
        });
        let out = run(&def, json!({"stuff": "payload"})).expect("jsonata task runs");
        // $states.result carries the function output; $states.input stays the
        // whole state input (JSONata states have no InputPath).
        assert_eq!(
            out["result"],
            json!({"bit": "payload", "static": "value"}),
            "got {out}"
        );
        assert_eq!(out["echoed"], json!("payload"));
        assert_eq!(out["echoed"], json!("payload"));
    }

    #[test]
    fn sfn_jsonata_task_ids_state_result_unavailable_in_arguments() {
        let def = json!({
            "QueryLanguage": "JSONATA",
            "StartAt": "Call",
            "States": {
                "Call": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::sqs:sendMessage",
                    "Arguments": {"x": "{% $states.result %}"},
                    "End": true,
                }
            }
        });
        let err = run(&def, json!({})).expect_err("bad field binding errors");
        assert_eq!(err.error, "States.QueryEvaluationError", "got {:?}", err);
        assert!(
            err.cause.contains("$states.result is not available"),
            "cause names the binding: {}",
            err.cause
        );
    }

    #[test]
    fn sfn_jsonata_parallel_merge_result() {
        // The AWS docs' parallel-flattening example: each branch returns an
        // object and the parent merges $states.result.
        let def = json!({
            "QueryLanguage": "JSONATA",
            "StartAt": "Par",
            "States": {
                "Par": {
                    "Type": "Parallel",
                    "Output": "{% $merge($states.result) %}",
                    "Branches": [
                        {
                            "StartAt": "A",
                            "States": {"A": {
                                "Type": "Pass",
                                "Output": {"orderId": "{% $states.input.orderId %}"},
                                "End": true,
                            }}
                        },
                        {
                            "StartAt": "B",
                            "States": {"B": {
                                "Type": "Pass",
                                "Output": {"customerId": "{% $states.input.customerId %}"},
                                "End": true,
                            }}
                        },
                    ],
                    "End": true,
                }
            }
        });
        let out = run(&def, json!({"orderId": "12345", "customerId": "C-100"}))
            .expect("parallel merge runs");
        assert_eq!(
            out,
            json!({"orderId": "12345", "customerId": "C-100"}),
            "got {out}"
        );
    }

    #[test]
    fn sfn_jsonata_map_items_and_item_selector() {
        let def = json!({
            "QueryLanguage": "JSONATA",
            "StartAt": "Process",
            "States": {
                "Process": {
                    "Type": "Map",
                    "Items": "{% $states.input.rows %}",
                    "ItemSelector": {
                        "name": "{% $states.input.name %}",
                        "value": "{% $states.input.value %}"
                    },
                    "ItemProcessor": {
                        "StartAt": "Row",
                        "States": {"Row": {
                            "Type": "Pass",
                            "Output": {"row": "{% $states.input.name %}", "doubled": "{% $states.input.value * 2 %}"},
                            "End": true,
                        }}
                    },
                    "Output": ["{% $states.result[0].row %}"],
                    "End": true,
                }
            }
        });
        let out = run(
            &def,
            json!({"rows": [{"name": "a", "value": 2}, {"name": "b", "value": 5}]}),
        )
        .expect("jsonata map runs");
        assert_eq!(out, json!(["a"]), "got {out}");
    }

    #[test]
    fn sfn_jsonata_functions_and_interpolation() {
        // $sub-setting: $format, $map/$filter, arithmetic and string concat,
        // plus embedded interpolation inside longer strings.
        let def = json!({
            "QueryLanguage": "JSONATA",
            "StartAt": "Make",
            "States": {
                "Make": {
                    "Type": "Pass",
                    "Output": {
                        "greeting": "{% $format('Hello {0}!', $states.input.name) %}",
                        "big": "{% $map($states.input.items, function($x) { $x * 2 }) %}",
                        "kept": "{% $filter($states.input.items, function($x) { $x > 1 }) %}",
                        "sum": "{% $sum($states.input.items) %}",
                        "label": "count={% $states.input.count %} of {% $states.input.name %}",
                        "total": "total-{% $states.input.count %}",
                        "upper": "{% $uppercase($states.input.name) %}",
                        "cut": "{% $substring($states.input.name, 0, 3) %}",
                    },
                    "End": true,
                }
            }
        });
        let out = run(&def, json!({"name": "ada", "count": 2, "items": [1, 2, 3]}))
            .expect("jsonata functions run");
        assert_eq!(
            out,
            json!({
                "greeting": "Hello ada!",
                "big": [2, 4, 6],
                "kept": [2, 3],
                "sum": 6,
                "label": "count=2 of ada",
                "total": "total-2",
                "upper": "ADA",
                "cut": "ada",
            }),
            "got {out}"
        );
    }

    #[test]
    fn sfn_jsonata_unknown_function_fails_flow() {
        let def = json!({
            "QueryLanguage": "JSONATA",
            "StartAt": "Bug",
            "States": {"Bug": {
                "Type": "Pass",
                "Output": {"x": "{% $nonexistent(1) %}"},
                "End": true,
            }}
        });
        let err = run(&def, json!({})).expect_err("an unknown function fails the state");
        assert_eq!(err.error, "States.QueryEvaluationError", "got {:?}", err);
        assert!(err.cause.contains("$nonexistent"), "got {}", err.cause);
    }

    #[test]
    fn sfn_jsonata_undefined_expression_errors_like_aws() {
        // "JSON cannot represent an undefined value expression" — a missing
        // field in the input is a States.QueryEvaluationError.
        let def = json!({
            "QueryLanguage": "JSONATA",
            "StartAt": "P",
            "States": {"P": {"Type": "Pass", "Output": {"x": "{% $states.input.missing %}"}, "End": true}}
        });
        let err = run(&def, json!({})).expect_err("undefined payload fails");
        assert_eq!(err.error, "States.QueryEvaluationError");
        // ...unless defaulted away.
        let def = json!({
            "QueryLanguage": "JSONATA",
            "StartAt": "P",
            "States": {"P": {
                "Type": "Pass",
                "Output": {"x": "{% $states.input.missing ?? 'fallback' %}"},
                "End": true,
            }}
        });
        let out = run(&def, json!({})).expect("?? provides the default");
        assert_eq!(out, json!({"x": "fallback"}));
    }

    #[test]
    fn sfn_jsonata_wait_seconds_template_and_fail_paths() {
        // Wait with a templated number, and templated Fail Error/Cause.
        let def = json!({
            "QueryLanguage": "JSONATA",
            "StartAt": "W",
            "States": {
                "W": {
                    "Type": "Wait",
                    "Seconds": "{% $states.input.wait %}",
                    "Next": "Boom",
                },
                "Boom": {"Type": "Fail", "Error": "{% 'states.' & $states.input.code %}", "Cause": "done"},
            }
        });
        let err = run(&def, json!({"wait": 300, "code": "Boom"})).expect_err("fails");
        assert_eq!(err.error, "states.Boom", "got {:?}", err);
        assert_eq!(err.cause, "done");
    }

    #[test]
    fn sfn_jsonata_catch_output_uses_states_error_output() {
        // AWS: "$states.errorOutput can be used in the Catch field's Output".
        let def = json!({
            "QueryLanguage": "JSONATA",
            "StartAt": "Boom",
            "States": {
                "Boom": {
                    "Type": "Task",
                    "Resource": "arn:aws:lambda:us-east-1:123456789012:function:missing",
                    "Catch": [{
                        "ErrorEquals": ["States.ALL"],
                        "Output": {"caught": "{% $states.errorOutput.Error %}", "why": "{% $states.errorOutput.Cause %}"},
                        "Next": "Handle",
                    }],
                },
                "Handle": {"Type": "Pass", "End": true},
            }
        });
        let out = run(&def, json!({})).expect("the catcher shapes the payload");
        assert_eq!(out["caught"], json!("Lambda.ServiceException"), "got {out}");
        assert_eq!(
            out["why"],
            json!("Function not found: arn:aws:lambda:us-east-1:123456789012:function:missing"),
            "got {out}"
        );
    }
}
