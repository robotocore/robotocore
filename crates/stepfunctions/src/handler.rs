//! Step Functions operation handler.

use crate::asl;
use crate::models::{Alias, Execution, StateMachine, StepfunctionsState};
use crate::protocol::{AwsRequest, AwsResponse};
use parking_lot::RwLock;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

pub struct StepfunctionsHandler {
    /// Per-(account, region) state, shared process-wide (see `global_states`).
    pub(crate) state: &'static RwLock<HashMap<(u64, String), StepfunctionsState>>,
}

/// Process-global state registry, shared by every handler instance.
///
/// Extra handler instances must never fork state, mirroring Moto's
/// in-process backend semantics (same pattern as the SQS/EventBridge
/// stores).
fn global_states() -> &'static RwLock<HashMap<(u64, String), StepfunctionsState>> {
    use std::sync::OnceLock;
    static STATES: OnceLock<RwLock<HashMap<(u64, String), StepfunctionsState>>> = OnceLock::new();
    STATES.get_or_init(|| RwLock::new(HashMap::new()))
}

fn param_str(req: &AwsRequest, keys: &[&str]) -> String {
    for key in keys {
        if let Some(v) = req.params.get(*key).and_then(|v| v.as_str()) {
            return v.to_string();
        }
    }
    String::new()
}

/// moto stepfunctions/models.py:905-914 `SFBackend._validate_name`. The
/// real-AWS charset is enumerated at :505-541 (space, brackets <>{}[],
/// wildcards ?*, specials " #%\^|~`$&,;:/) plus the control characters
/// U+0000-001F / U+007F-009F named in moto's own comment — `char::is_control`
/// is exactly those Cc ranges. A hit answers 400 InvalidName "Invalid Name:
/// '<name>'"; a name over 80 chars passes into NameTooLongException, a
/// ValidationException subclass rendering the modeled `1 validation error`
/// text with the 80-bound (exceptions.py:50,66-73).
fn validate_name(name: &str) -> Result<(), AwsResponse> {
    const INVALID_CHARS: &[char; 23] = &[
        ' ', '{', '}', '[', ']', '<', '>', '?', '*', '"', '#', '%', '\\', '^', '|', '~', '`', '$',
        '&', ',', ';', ':', '/',
    ];
    if name
        .chars()
        .any(|c| INVALID_CHARS.contains(&c) || c.is_control())
    {
        return Err(AwsResponse::error(
            400,
            "InvalidName",
            &format!("Invalid Name: '{name}'"),
        ));
    }
    if name.chars().count() > 80 {
        return Err(AwsResponse::error(
            400,
            "ValidationException",
            &format!(
                "1 validation error detected: Value '{name}' at 'name' failed to satisfy \
                 constraint: Member must have length less than or equal to 80"
            ),
        ));
    }
    Ok(())
}

/// moto's paginated list ops decode `nextToken` through botocore's
/// TokenDecoder (utilities/paginator.py:110-131): the token is the
/// base64 of a JSON dict; undecodable input answers 400 InvalidToken
/// "Invalid Token: Invalid token" (stepfunctions/exceptions.py:38-41).
/// Applies only to the four PAGINATION_MODEL ops, never the versions /
/// map-runs lists. A token that DECODES continues into moto's checksum
/// arm (utilities/paginator.py:118-125, :135-143): the decoded dict must
/// carry a `parameterChecksum` equal to the hashing of the request's own
/// kwargs — a value this twin never mints (no paging) and a replay of
/// another process's token cannot carry (python `hash()` is per-process),
/// so decoded tokens answer 400 InvalidToken "Invalid Token: Input
/// inconsistent with page token: {<python repr>}" — the measured row
/// renders `{'limit': 1}`.
fn check_page_token(req: &AwsRequest) -> Option<AwsResponse> {
    let token = req.params.get("nextToken").and_then(|v| v.as_str())?;
    let undecodable = || AwsResponse::error(400, "InvalidToken", "Invalid Token: Invalid token");
    use base64::Engine as _;
    let raw = match base64::engine::general_purpose::STANDARD.decode(token) {
        Ok(raw) => raw,
        Err(_) => return Some(undecodable()),
    };
    let text = match std::str::from_utf8(&raw) {
        Ok(t) => t,
        Err(_) => return Some(undecodable()),
    };
    let Ok(decoded) = serde_json::from_str::<Value>(text) else {
        return Some(undecodable());
    };
    Some(AwsResponse::error(
        400,
        "InvalidToken",
        &format!(
            "Invalid Token: Input inconsistent with page token: {}",
            python_repr(&decoded)
        ),
    ))
}

/// The slice of python `repr()` the decoded-token message needs: dicts in
/// key order with `'{k: v, …}'` pairs, single-quoted strings, bare
/// numbers/booleans, None, lists — the shapes a JSON token can decode to.
fn python_repr(v: &Value) -> String {
    match v {
        Value::Null => "None".to_string(),
        Value::Bool(b) => if *b { "True" } else { "False" }.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => python_str_repr(s),
        Value::Array(items) => {
            let inner: Vec<String> = items.iter().map(python_repr).collect();
            format!("[{}]", inner.join(", "))
        }
        Value::Object(map) => {
            let inner: Vec<String> = map
                .iter()
                .map(|(k, val)| format!("{}: {}", python_str_repr(k), python_repr(val)))
                .collect();
            format!("{{{}}}", inner.join(", "))
        }
    }
}

/// python single-quoted string repr: `'…'` with backslash-escaping for
/// `'`, `\`, `\n`, `\r`, `\t`.
fn python_str_repr(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        match c {
            '\'' => out.push_str("\\'"),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(c),
        }
    }
    out.push('\'');
    out
}

/// moto stepfunctions/models.py:724: the describe miss wording shared by
/// DescribeStateMachine, StartExecution, ListExecutions, ListAliases (a
/// missing state machine is 400 StateMachineDoesNotExist, never an empty
/// page — the fork resolves fans through describe_state_machine, :819-836).
fn state_machine_does_not_exist(arn: &str) -> AwsResponse {
    AwsResponse::error(
        400,
        "StateMachineDoesNotExist",
        &format!("State Machine Does Not Exist: '{arn}'"),
    )
}

/// moto stepfunctions/models.py:608-612 + utilities/utils.py:21: an
/// execution ARN must match the partition-anchored
/// *:states:<region>:<account>:execution:<rest> shape or
/// `_validate_execution_arn` raises 400 InvalidArn — with the SAME
/// "Execution Does Not Exist: '<arn>'" text the missing-execution branches
/// use (the :930-935 code arm maps the regex onto InvalidArn; a census
/// probe shaped like a machine ARN measured that code flip, so the twin's
/// DescribeExecution fan runs this gate before its own lookup).
fn execution_arn_is_well_formed(arn: &str) -> bool {
    const PARTITIONS: [&str; 6] = [
        "aws",
        "aws-cn",
        "aws-eusc",
        "aws-iso",
        "aws-iso-b",
        "aws-us-gov",
    ];
    let Some(rest) = arn.strip_prefix("arn:") else {
        return false;
    };
    for partition in PARTITIONS {
        let Some(after) = rest
            .strip_prefix(partition)
            .and_then(|a| a.strip_prefix(':'))
        else {
            continue;
        };
        let Some(s) = after.strip_prefix("states:") else {
            continue;
        };
        let mut segs = s.splitn(3, ':');
        let Some(region) = segs.next() else { continue };
        if region.is_empty()
            || !region
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-')
        {
            continue;
        }
        let Some(account) = segs.next() else { continue };
        if account.len() != 12 || !account.chars().all(|c| c.is_ascii_digit()) {
            continue;
        }
        let Some(tail) = segs.next() else { continue };
        if let Some(exec_rest) = tail.strip_prefix("execution:") {
            return !exec_rest.is_empty();
        }
    }
    false
}

/// describe_execution's ladder (moto models.py:849-852): shape first,
/// then the execution lookup, both carrying "Execution Does Not Exist:
/// '<arn>'".
fn describe_execution_arn_gate(execution_arn: &str) -> Option<AwsResponse> {
    if !execution_arn_is_well_formed(execution_arn) {
        return Some(AwsResponse::error(
            400,
            "InvalidArn",
            &format!("Execution Does Not Exist: '{execution_arn}'"),
        ));
    }
    None
}

impl StepfunctionsHandler {
    pub fn new() -> Self {
        Self {
            state: global_states(),
        }
    }

    pub(crate) fn get_state(&self, account: u64, region: &str) -> StepfunctionsState {
        self.state
            .write()
            .entry((account, region.to_string()))
            .or_default()
            .clone()
    }

    fn machine_arn(&self, req: &AwsRequest, name: &str) -> String {
        format!(
            "arn:aws:states:{}:{}:stateMachine:{}",
            req.region, req.account, name
        )
    }

    // ---------------- State machine CRUD ----------------

    fn create_sm(&self, req: &AwsRequest) -> AwsResponse {
        let name = param_str(req, &["name", "stateMachineName"]);
        if name.is_empty() {
            return AwsResponse::error(400, "ValidationException", "name is required");
        }
        // moto create_state_machine validates the name first
        // (stepfunctions/models.py:642 → _validate_name).
        if let Err(resp) = validate_name(&name) {
            return resp;
        }
        let definition = param_str(req, &["definition"]);
        if definition.is_empty() {
            return AwsResponse::error(400, "ValidationException", "definition is required");
        }
        let state = self.get_state(req.account, &req.region);
        let arn = self.machine_arn(req, &name);
        if state.state_machines.read().contains_key(&arn) {
            return AwsResponse::error(
                400,
                "StateMachineAlreadyExists",
                &format!("State Machine {name} already exists"),
            );
        }
        let role_arn = param_str(req, &["roleArn"]);
        let sm_type = param_str(req, &["type"]);
        let sm_type = if sm_type.is_empty() {
            "STANDARD".to_string()
        } else {
            sm_type
        };
        let creation_date = asl::now_f();
        let machine = Arc::new(StateMachine {
            name: name.clone(),
            arn: arn.clone(),
            definition: RwLock::new(definition),
            role_arn: RwLock::new(role_arn),
            sm_type,
            creation_date,
        });
        if let Some(tags) = req.params.get("tags").and_then(|t| t.as_array()).cloned() {
            state.tags.write().insert(arn.clone(), tags);
        }
        state.state_machines.write().insert(arn.clone(), machine);
        AwsResponse::json(
            200,
            json!({ "stateMachineArn": arn, "creationDate": creation_date }),
        )
    }

    fn delete_sm(&self, req: &AwsRequest) -> AwsResponse {
        let arn = param_str(req, &["stateMachineArn"]);
        let state = self.get_state(req.account, &req.region);
        if state.state_machines.write().remove(&arn).is_none() {
            return AwsResponse::error(
                400,
                "StateMachineDoesNotExist",
                &format!("State Machine Does Not Exist: '{arn}'"),
            );
        }
        // Cascade: published versions and aliases reference the machine.
        state.versions.write().remove(&arn);
        let removed_aliases: Vec<String> = state
            .aliases
            .read()
            .keys()
            .filter(|a| a.starts_with(&format!("{arn}:")))
            .cloned()
            .collect();
        let mut aliases = state.aliases.write();
        for key in removed_aliases {
            aliases.remove(&key);
        }
        AwsResponse::json(200, json!({}))
    }

    fn describe_sm(&self, req: &AwsRequest) -> AwsResponse {
        let arn = param_str(req, &["stateMachineArn"]);
        let state = self.get_state(req.account, &req.region);
        match state.get_machine(&arn) {
            Some(sm) => AwsResponse::json(200, sm.to_value()),
            None => AwsResponse::error(
                400,
                "StateMachineDoesNotExist",
                &format!("State Machine Does Not Exist: '{arn}'"),
            ),
        }
    }

    fn list_sms(&self, req: &AwsRequest) -> AwsResponse {
        if let Some(resp) = check_page_token(req) {
            return resp;
        }
        let state = self.get_state(req.account, &req.region);
        let rows: Vec<Value> = state
            .state_machines
            .read()
            .values()
            .map(|sm| {
                json!({
                    "name": sm.name,
                    "stateMachineArn": sm.arn,
                    "type": sm.sm_type,
                    "creationDate": sm.creation_date,
                    "status": "ACTIVE",
                })
            })
            .collect();
        AwsResponse::json(200, json!({ "stateMachines": rows }))
    }

    fn update_sm(&self, req: &AwsRequest) -> AwsResponse {
        let arn = param_str(req, &["stateMachineArn"]);
        let state = self.get_state(req.account, &req.region);
        if state.get_machine(&arn).is_none() {
            return AwsResponse::error(
                400,
                "StateMachineDoesNotExist",
                &format!("State Machine Does Not Exist: '{arn}'"),
            );
        }
        {
            let machines = state.state_machines.read();
            if let Some(sm) = machines.get(&arn) {
                if let Some(def) = req.params.get("definition") {
                    *sm.definition.write() = def.as_str().unwrap_or("").to_string();
                }
                if let Some(role) = req.params.get("roleArn") {
                    *sm.role_arn.write() = role.as_str().unwrap_or("").to_string();
                }
            }
        }
        AwsResponse::json(
            200,
            json!({ "stateMachineArn": arn, "updateDate": asl::now_f() }),
        )
    }

    // ---------------- Executions ----------------

    fn start_execution(&self, req: &AwsRequest) -> AwsResponse {
        let sm_arn = param_str(req, &["stateMachineArn"]);
        let name = param_str(req, &["name"]);
        // moto start_execution validates the name when present BEFORE the
        // machine lookup (stepfunctions/models.py:817-819).
        if !name.is_empty() {
            if let Err(resp) = validate_name(&name) {
                return resp;
            }
        }
        let state = self.get_state(req.account, &req.region);
        let Some(machine) = state.get_machine(&sm_arn) else {
            return AwsResponse::error(
                400,
                "StateMachineDoesNotExist",
                &format!("State Machine Does Not Exist: '{sm_arn}'"),
            );
        };
        let exec_name = if name.is_empty() {
            uuid::Uuid::new_v4().to_string()
        } else {
            name
        };
        // moto's execution ARN carries the machine NAME, not the machine
        // ARN: "arn:<part>:states:<region>:<acct>:execution:<sm>:<exec>"
        // (models.py:352-360) — the era derivation `sm_arn
        // .replace(":stateMachine:", ":execution:"):name` matches it and
        // passes `_validate_execution_arn`'s regex (:608-612).
        let exec_arn = format!(
            "{}:{exec_name}",
            sm_arn.replace(":stateMachine:", ":execution:")
        );
        let input = param_str(req, &["input"]);
        let input = if input.is_empty() {
            "{}".to_string()
        } else {
            input
        };
        let exec = Arc::new(Execution {
            arn: exec_arn.clone(),
            name: exec_name,
            sm_arn: sm_arn.clone(),
            input: input.clone(),
            start_date: asl::now_f(),
            status: RwLock::new("RUNNING".to_string()),
            stop_date: RwLock::new(None),
            output: RwLock::new(None),
            error: RwLock::new(None),
            cause: RwLock::new(None),
            events: RwLock::new(Vec::new()),
        });
        state
            .executions
            .write()
            .insert(exec_arn.clone(), exec.clone());
        asl::run_on_thread(exec, machine.definition_string());
        AwsResponse::json(
            200,
            json!({ "executionArn": exec_arn, "startDate": asl::now_f() }),
        )
    }

    /// Run an execution inline (StartSyncExecution); returns the exec record.
    fn run_sync_execution(
        &self,
        state: &StepfunctionsState,
        machine: &StateMachine,
        input: String,
    ) -> Arc<Execution> {
        let name = uuid::Uuid::new_v4().to_string();
        // The express variant keeps that shape on :express: (the fork has
        // no express ARN template; the era's replace rides on).
        let exec_arn = format!(
            "{}:{name}",
            machine.arn.replace(":stateMachine:", ":express:")
        );
        let arn = exec_arn.clone();
        let exec = Arc::new(Execution {
            arn,
            name,
            sm_arn: machine.arn.clone(),
            input: input.clone(),
            start_date: asl::now_f(),
            status: RwLock::new("RUNNING".to_string()),
            stop_date: RwLock::new(None),
            output: RwLock::new(None),
            error: RwLock::new(None),
            cause: RwLock::new(None),
            events: RwLock::new(Vec::new()),
        });
        state
            .executions
            .write()
            .insert(exec_arn.clone(), exec.clone());
        // Run inline so the response carries the completed execution.
        exec.push_event(json!({
            "timestamp": asl::now_f(),
            "type": "ExecutionStarted",
            "previousEventId": 0,
            "executionStartedEventDetails": {
                "input": exec.input.clone(),
                "roleArn": "",
            },
        }));
        let ctx = asl::Ctx::new(exec.clone());
        let result = (|| {
            let def: Value =
                serde_json::from_str(&machine.definition_string()).unwrap_or(Value::Null);
            let states = def
                .get("States")
                .and_then(|s| s.as_object())
                .ok_or_else(|| {
                    asl::FlowError::new("States.StatesMissing", "States is required".to_string())
                })?;
            let start_at = def
                .get("StartAt")
                .and_then(|s| s.as_str())
                .unwrap_or_default()
                .to_string();
            let parsed: Value = serde_json::from_str(&input).unwrap_or_else(|_| json!({}));
            let machine_default = def
                .as_object()
                .map(asl::QueryLanguage::for_machine)
                .unwrap_or_default();
            asl::run_flow(&ctx, states, start_at, parsed, machine_default)
        })();
        let last_event_id = ctx.exec.events.read().len() as i64;
        match result {
            Ok(output) => {
                ctx.exec
                    .mark_succeeded(asl::value_to_string(&output), last_event_id);
            }
            Err(e) => ctx.exec.mark_failed(e.error, e.cause, last_event_id),
        }
        exec
    }

    fn start_sync_execution(&self, req: &AwsRequest) -> AwsResponse {
        let sm_arn = param_str(req, &["stateMachineArn"]);
        let input = param_str(req, &["input"]);
        let input = if input.is_empty() {
            "{}".to_string()
        } else {
            input
        };
        let state = self.get_state(req.account, &req.region);
        let Some(machine) = state.get_machine(&sm_arn) else {
            return AwsResponse::error(
                400,
                "StateMachineDoesNotExist",
                &format!("State Machine Does Not Exist: '{sm_arn}'"),
            );
        };
        let exec = self.run_sync_execution(&state, &machine, input);
        AwsResponse::json(200, exec.describe_value())
    }

    fn describe_exec(&self, req: &AwsRequest) -> AwsResponse {
        let arn = param_str(req, &["executionArn"]);
        if let Some(resp) = describe_execution_arn_gate(&arn) {
            return resp;
        }
        let state = self.get_state(req.account, &req.region);
        match state.get_execution(&arn) {
            Some(exec) => AwsResponse::json(200, exec.describe_value()),
            None => AwsResponse::error(
                400,
                "ExecutionDoesNotExist",
                &format!("Execution Does Not Exist: '{arn}'"),
            ),
        }
    }

    fn list_executions(&self, req: &AwsRequest) -> AwsResponse {
        let sm_arn = param_str(req, &["stateMachineArn"]);
        let status_filter = param_str(req, &["statusFilter"]);
        let state = self.get_state(req.account, &req.region);
        // moto resolves the machine BEFORE the pagination wrapper decodes
        // the token (responses.py:291 vs utilities/paginator.py eager
        // parse), so a missing machine with a garbage token answers
        // StateMachineDoesNotExist — the measured row; ListStateMachine-
        // Aliases has no pre-resolve and keeps token-first (measured).
        if state.get_machine(&sm_arn).is_none() {
            return state_machine_does_not_exist(&sm_arn);
        }
        if let Some(resp) = check_page_token(req) {
            return resp;
        }
        let mut rows: Vec<Value> = state
            .executions
            .read()
            .values()
            .filter(|exec| exec.sm_arn == sm_arn)
            .filter(|exec| status_filter.is_empty() || *exec.status.read() == status_filter)
            .map(|exec| exec.list_value())
            .collect();
        rows.reverse();
        AwsResponse::json(200, json!({ "executions": rows }))
    }

    fn stop_execution(&self, req: &AwsRequest) -> AwsResponse {
        let exec_arn = param_str(req, &["executionArn"]);
        if let Some(resp) = describe_execution_arn_gate(&exec_arn) {
            return resp;
        }
        let state = self.get_state(req.account, &req.region);
        match state.get_execution(&exec_arn) {
            Some(exec) => {
                if exec.is_running() {
                    let now = asl::now_f();
                    *exec.stop_date.write() = Some(now);
                    *exec.status.write() = "ABORTED".to_string();
                    if let Some(err) = req.params.get("error").and_then(|v| v.as_str()) {
                        *exec.error.write() = Some(err.to_string());
                    }
                    if let Some(cause) = req.params.get("cause").and_then(|v| v.as_str()) {
                        *exec.cause.write() = Some(cause.to_string());
                    }
                }
                AwsResponse::json(200, json!({ "stopDate": asl::now_f() }))
            }
            None => AwsResponse::error(
                400,
                "ExecutionDoesNotExist",
                &format!("Execution Does Not Exist: '{exec_arn}'"),
            ),
        }
    }

    fn describe_sm_for_execution(&self, req: &AwsRequest) -> AwsResponse {
        let arn = param_str(req, &["executionArn"]);
        let state = self.get_state(req.account, &req.region);
        let Some(exec) = state.get_execution(&arn) else {
            // moto: any missing execution on this op is ResourceNotFound
            // "Resource not found: '<arn>'" — no shape gate here
            // (models.py:869-874; measured 2026-11-11 with a well-formed
            // missing ARN).
            return AwsResponse::error(
                400,
                "ResourceNotFound",
                &format!("Resource not found: '{arn}'"),
            );
        };
        match state.get_machine(&exec.sm_arn) {
            Some(sm) => {
                let mut v = sm.to_value();
                v["updateDate"] = json!(sm.creation_date);
                AwsResponse::json(200, v)
            }
            None => AwsResponse::error(
                400,
                "StateMachineDoesNotExist",
                &format!("State Machine Does Not Exist: '{}'", exec.sm_arn),
            ),
        }
    }

    fn get_execution_history(&self, req: &AwsRequest) -> AwsResponse {
        // moto validates the exec-ARN shape first here too (models.py:861).
        let arn = param_str(req, &["executionArn"]);
        if let Some(resp) = describe_execution_arn_gate(&arn) {
            return resp;
        }
        let reverse = req
            .params
            .get("reverseOrder")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let state = self.get_state(req.account, &req.region);
        let Some(exec) = state.get_execution(&arn) else {
            return AwsResponse::error(
                400,
                "ExecutionDoesNotExist",
                &format!("Execution Does Not Exist: '{arn}'"),
            );
        };
        let mut events = exec.events.read().clone();
        if reverse {
            events.reverse();
        }
        AwsResponse::json(200, json!({ "events": events }))
    }

    fn redrive_execution(&self, req: &AwsRequest) -> AwsResponse {
        let arn = param_str(req, &["executionArn"]);
        let state = self.get_state(req.account, &req.region);
        match state.get_execution(&arn) {
            Some(_) => AwsResponse::json(200, json!({ "redriveDate": asl::now_f() })),
            None => AwsResponse::error(
                400,
                "ExecutionDoesNotExist",
                &format!("Execution {arn} not found"),
            ),
        }
    }

    // ---------------- Versions ----------------

    fn publish_version(&self, req: &AwsRequest) -> AwsResponse {
        let arn = param_str(req, &["stateMachineArn"]);
        let state = self.get_state(req.account, &req.region);
        if state.get_machine(&arn).is_none() {
            return AwsResponse::error(
                400,
                "StateMachineDoesNotExist",
                &format!("State Machine Does Not Exist: '{arn}'"),
            );
        }
        let mut versions = state.versions.write();
        let entry = versions.entry(arn.clone()).or_default();
        let next = entry.keys().max().map(|m| m + 1).unwrap_or(1);
        let creation = asl::now_f();
        entry.insert(next, creation);
        let version_arn = format!("{arn}:{next}");
        AwsResponse::json(
            200,
            json!({ "stateMachineVersionArn": version_arn, "creationDate": creation }),
        )
    }

    fn list_versions(&self, req: &AwsRequest) -> AwsResponse {
        let arn = param_str(req, &["stateMachineArn"]);
        let state = self.get_state(req.account, &req.region);
        let rows: Vec<Value> = state
            .versions
            .read()
            .get(&arn)
            .map(|v| {
                v.iter()
                    .map(|(num, created)| {
                        json!({ "stateMachineVersionArn": format!("{arn}:{num}"), "creationDate": created })
                    })
                    .collect()
            })
            .unwrap_or_default();
        AwsResponse::json(
            200,
            json!({ "stateMachineVersions": rows, "nextToken": Value::Null }),
        )
    }

    fn delete_version(&self, req: &AwsRequest) -> AwsResponse {
        let version_arn = param_str(req, &["stateMachineVersionArn"]);
        let Some((base, num_str)) = version_arn.rsplit_once(':') else {
            return AwsResponse::error(
                400,
                "ValidationException",
                "stateMachineVersionArn is invalid",
            );
        };
        let Ok(num) = num_str.parse::<u64>() else {
            return AwsResponse::error(400, "ValidationException", "Invalid version ARN");
        };
        let state = self.get_state(req.account, &req.region);
        if let Some(versions) = state.versions.write().get_mut(base) {
            versions.remove(&num);
        }
        AwsResponse::json(200, json!({}))
    }

    // ---------------- Aliases ----------------

    fn create_alias(&self, req: &AwsRequest) -> AwsResponse {
        let name = param_str(req, &["name"]);
        let routing = req
            .params
            .get("routingConfiguration")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        // The alias ARN derives from the first routing entry's version ARN.
        let base_arn = routing
            .first()
            .and_then(|r| r.get("stateMachineVersionArn").and_then(|v| v.as_str()))
            .and_then(|v| v.rsplit_once(':').map(|(base, _)| base.to_string()))
            .unwrap_or_default();
        if base_arn.is_empty() || name.is_empty() {
            return AwsResponse::error(
                400,
                "ValidationException",
                "name and routingConfiguration are required",
            );
        }
        let alias_arn = format!("{base_arn}:{name}");
        let state = self.get_state(req.account, &req.region);
        let now = asl::now_f();
        let alias = Arc::new(Alias {
            name: name.clone(),
            arn: alias_arn.clone(),
            routing_configuration: RwLock::new(routing),
            creation_date: now,
            update_date: RwLock::new(now),
        });
        state
            .aliases
            .write()
            .insert(alias_arn.clone(), alias.clone());
        AwsResponse::json(
            200,
            json!({ "stateMachineAliasArn": alias.arn, "creationDate": now }),
        )
    }

    fn update_alias(&self, req: &AwsRequest) -> AwsResponse {
        // Missing BOTH update fields is the input gate before any lookup
        // (moto responses.py:204-210).
        let description = req.params.get("description");
        let routing = req.params.get("routingConfiguration");
        if description.is_none() && routing.is_none() {
            return AwsResponse::error(
                400,
                "ValidationException",
                "You must provide at least one of routingConfiguration or description",
            );
        }
        let arn = param_str(req, &["stateMachineAliasArn"]);
        let state = self.get_state(req.account, &req.region);
        let alias = match Self::alias_ladder(&state, &arn) {
            Ok(alias) => alias,
            Err(resp) => return resp,
        };
        if let Some(routing) = req
            .params
            .get("routingConfiguration")
            .and_then(|v| v.as_array())
        {
            *alias.routing_configuration.write() = routing.clone();
        }
        let now = asl::now_f();
        *alias.update_date.write() = now;
        AwsResponse::json(
            200,
            json!({ "stateMachineAliasArn": alias.arn, "updateDate": now }),
        )
    }

    fn delete_alias(&self, req: &AwsRequest) -> AwsResponse {
        let arn = param_str(req, &["stateMachineAliasArn"]);
        let state = self.get_state(req.account, &req.region);
        // moto's delete runs the describe ladder then removes the row
        // (models.py:753-762) — a missing owner or alias is a 400, never
        // a silent 200 (measured 2026-11-11).
        match Self::alias_ladder(&state, &arn) {
            Ok(alias) => {
                state.aliases.write().remove(alias.arn.as_str());
                AwsResponse::json(200, json!({}))
            }
            Err(resp) => resp,
        }
    }

    /// moto's alias ladder (models.py:727-762, responses.py:203-215):
    /// the alias arn rplits into its owner machine arn and the alias
    /// NAME; an owner MISS raises StateMachineDoesNotExist carrying the
    /// TRUNCATED owner text ("…:stateMachine:<n>:alias" — the fork's
    /// reprsplit dropped the alias segment, measured 2026-11-11), the
    /// owner's alias row miss is ResourceNotFound "State Machine Alias
    /// Does Not Exist: '<full>''" wrapping (exceptions.py:74-76), and
    /// Update refuses BOTH-absent routing/description first
    /// (ValidationException, responses.py:204-210).
    fn alias_ladder(
        state: &StepfunctionsState,
        arn: &str,
    ) -> Result<std::sync::Arc<crate::models::Alias>, AwsResponse> {
        let Some((owner_arn, _alias_name)) = arn.rsplit_once(':') else {
            return Err(AwsResponse::error(
                400,
                "ResourceNotFound",
                &format!("Resource not found: '{arn}'"),
            ));
        };
        // moto validates the owner as a machine arn then looks it up
        // (models.py:736-741): a well-formed owner that does not exist is
        // StateMachineDoesNotExist with the owner text.
        if state.get_machine(owner_arn).is_none() {
            return Err(state_machine_does_not_exist(owner_arn));
        }
        match state.aliases.read().get(arn).cloned() {
            Some(alias) => Ok(alias),
            // moto's ResourceNotFound wraps the inner wording one more
            // layer ("Resource not found: 'State Machine Alias Does Not
            // Exist: '<full>''") — models.py:745-746 via the
            // ResourceNotFound wrapper at exceptions.py:74-76; measured
            // 2026-11-12.
            None => Err(AwsResponse::error(
                400,
                "ResourceNotFound",
                &format!("Resource not found: 'State Machine Alias Does Not Exist: '{arn}''"),
            )),
        }
    }

    fn describe_alias(&self, req: &AwsRequest) -> AwsResponse {
        let arn = param_str(req, &["stateMachineAliasArn"]);
        let state = self.get_state(req.account, &req.region);
        match Self::alias_ladder(&state, &arn) {
            Ok(alias) => AwsResponse::json(200, alias.describe_value()),
            Err(resp) => resp,
        }
    }

    fn list_aliases(&self, req: &AwsRequest) -> AwsResponse {
        let sm_arn = param_str(req, &["stateMachineArn"]);
        if let Some(resp) = check_page_token(req) {
            return resp;
        }
        let state = self.get_state(req.account, &req.region);
        // Measured 2026-11-11: an unknown machine ARN answers
        // StateMachineDoesNotExist here like ListExecutions (the fork
        // resolves aliases through the owning machine), never 200-empty.
        if state.get_machine(&sm_arn).is_none() {
            return state_machine_does_not_exist(&sm_arn);
        }
        let rows: Vec<Value> = state
            .aliases
            .read()
            .values()
            .filter(|alias| alias.arn.starts_with(&format!("{sm_arn}:")))
            .map(|alias| json!({ "stateMachineAliasArn": alias.arn, "creationDate": alias.creation_date }))
            .collect();
        AwsResponse::json(200, json!({ "stateMachineAliases": rows }))
    }

    // ---------------- Activities ----------------

    fn create_activity(&self, req: &AwsRequest) -> AwsResponse {
        let name = param_str(req, &["name"]);
        let arn = format!(
            "arn:aws:states:{}:{}:activity:{}",
            req.region, req.account, name
        );
        let now = asl::now_f();
        let state = self.get_state(req.account, &req.region);
        state.activities.write().insert(
            arn.clone(),
            Arc::new(crate::models::Activity {
                name: name.clone(),
                arn: arn.clone(),
                creation_date: now,
                scheduled: RwLock::new(Value::Null),
            }),
        );
        AwsResponse::json(200, json!({ "activityArn": arn, "creationDate": now }))
    }

    fn describe_activity(&self, req: &AwsRequest) -> AwsResponse {
        let arn = param_str(req, &["activityArn"]);
        let state = self.get_state(req.account, &req.region);
        match state.get_activity(&arn) {
            Some(activity) => AwsResponse::json(
                200,
                json!({
                    "activityArn": activity.arn,
                    "name": activity.name,
                    "creationDate": activity.creation_date,
                }),
            ),
            None => AwsResponse::error(
                404,
                "ActivityDoesNotExist",
                &format!("Activity {arn} does not exist"),
            ),
        }
    }

    fn delete_activity(&self, req: &AwsRequest) -> AwsResponse {
        let arn = param_str(req, &["activityArn"]);
        let state = self.get_state(req.account, &req.region);
        if state.activities.write().remove(&arn).is_none() {
            return AwsResponse::error(
                404,
                "ActivityDoesNotExist",
                &format!("Activity {arn} does not exist"),
            );
        }
        state
            .task_tokens
            .write()
            .retain(|_token, activity| activity != &arn);
        AwsResponse::json(200, json!({}))
    }

    fn list_activities(&self, req: &AwsRequest) -> AwsResponse {
        if let Some(resp) = check_page_token(req) {
            return resp;
        }
        let state = self.get_state(req.account, &req.region);
        let rows: Vec<Value> = state
            .activities
            .read()
            .values()
            .map(|activity| {
                json!({
                    "activityArn": activity.arn,
                    "name": activity.name,
                    "creationDate": activity.creation_date,
                })
            })
            .collect();
        AwsResponse::json(200, json!({ "activities": rows }))
    }

    fn get_activity_task(&self, req: &AwsRequest) -> AwsResponse {
        let arn = param_str(req, &["activityArn"]);
        let state = self.get_state(req.account, &req.region);
        let Some(activity) = state.get_activity(&arn) else {
            return AwsResponse::error(
                404,
                "ActivityDoesNotExist",
                &format!("Activity {arn} does not exist"),
            );
        };
        let scheduled = activity.scheduled.read().clone();
        let token = uuid::Uuid::new_v4().to_string();
        state.task_tokens.write().insert(token.clone(), arn.clone());
        // botocore drops null members, so an empty activity reports "" input
        // (JSON null would make the member vanish client-side).
        let input = if scheduled.is_null() {
            json!("")
        } else {
            scheduled
        };
        AwsResponse::json(200, json!({ "taskToken": token, "input": input }))
    }

    // ---------------- SendTask* callback ops ----------------

    fn send_task_result(&self, req: &AwsRequest, success: bool) -> AwsResponse {
        let token = param_str(req, &["taskToken"]);
        let state = self.get_state(req.account, &req.region);
        if state.task_tokens.write().remove(&token).is_none() {
            return AwsResponse::error(
                400,
                "TaskDoesNotExist",
                &format!("Task token not found: {token}"),
            );
        }
        if success {
            let _output = param_str(req, &["output"]);
        } else {
            let _error = param_str(req, &["error"]);
        }
        AwsResponse::json(200, json!({}))
    }

    fn send_task_heartbeat(&self, req: &AwsRequest) -> AwsResponse {
        let token = param_str(req, &["taskToken"]);
        let state = self.get_state(req.account, &req.region);
        if !state.task_tokens.read().contains_key(&token) {
            return AwsResponse::error(
                400,
                "TaskDoesNotExist",
                &format!("Task token not found: {token}"),
            );
        }
        AwsResponse::json(200, json!({}))
    }

    // ---------------- Map runs ----------------

    fn describe_map_run(&self, req: &AwsRequest) -> AwsResponse {
        let arn = param_str(req, &["mapRunArn"]);
        // No distributed map runs are created by the Rust twin yet.
        AwsResponse::error(400, "ExecutionDoesNotExist", &arn)
    }

    fn list_map_runs(&self, _req: &AwsRequest) -> AwsResponse {
        AwsResponse::json(200, json!({ "mapRuns": [], "nextToken": Value::Null }))
    }

    // ---------------- Validation ----------------

    fn validate_definition(&self, req: &AwsRequest) -> AwsResponse {
        let definition = param_str(req, &["definition"]);
        match serde_json::from_str::<Value>(&definition) {
            Ok(_) => AwsResponse::json(
                200,
                json!({ "result": "OK", "diagnostics": [], "truncated": false }),
            ),
            Err(_) => AwsResponse::json(
                200,
                json!({
                    "result": "FAIL",
                    "diagnostics": [{
                        "severity": "ERROR",
                        "code": "INVALID_JSON_DESCRIPTION",
                        "message": "Could not parse the state machine definition.",
                    }],
                    "truncated": false,
                }),
            ),
        }
    }

    // ---------------- Tags ----------------

    fn tag_resource(&self, req: &AwsRequest) -> AwsResponse {
        let arn = param_str(req, &["resourceArn"]);
        let tags: Vec<Value> = req
            .params
            .get("tags")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let mut existing = state.tags.read().get(&arn).cloned().unwrap_or_default();
        for tag in tags {
            let key = tag
                .get("key")
                .and_then(|k| k.as_str())
                .unwrap_or_default()
                .to_string();
            let found = existing
                .iter_mut()
                .any(|t| t.get("key").and_then(|k| k.as_str()) == Some(key.as_str()));
            if found {
                for t in existing.iter_mut() {
                    if t.get("key").and_then(|k| k.as_str()) == Some(key.as_str()) {
                        *t = tag.clone();
                    }
                }
            } else {
                existing.push(tag);
            }
        }
        state.tags.write().insert(arn, existing);
        AwsResponse::json(200, json!({}))
    }

    fn untag_resource(&self, req: &AwsRequest) -> AwsResponse {
        let arn = param_str(req, &["resourceArn"]);
        let keys: Vec<String> = req
            .params
            .get("tagKeys")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|k| k.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let mut existing = state.tags.read().get(&arn).cloned().unwrap_or_default();
        existing.retain(|t| {
            t.get("key")
                .and_then(|k| k.as_str())
                .map(|k| !keys.contains(&k.to_string()))
                .unwrap_or(true)
        });
        state.tags.write().insert(arn, existing);
        AwsResponse::json(200, json!({}))
    }

    fn list_tags(&self, req: &AwsRequest) -> AwsResponse {
        let arn = param_str(req, &["resourceArn"]);
        let state = self.get_state(req.account, &req.region);
        let tags = state.tags.read().get(&arn).cloned().unwrap_or_default();
        AwsResponse::json(200, json!({ "tags": tags }))
    }

    // ---------------- dispatch ----------------

    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        let op = req.operation.as_str();
        match op {
            "CreateStateMachine" => self.create_sm(&req),
            "DeleteStateMachine" => self.delete_sm(&req),
            "DescribeStateMachine" => self.describe_sm(&req),
            "ListStateMachines" => self.list_sms(&req),
            "UpdateStateMachine" => self.update_sm(&req),
            "StartExecution" => self.start_execution(&req),
            "StartSyncExecution" => self.start_sync_execution(&req),
            "DescribeExecution" => self.describe_exec(&req),
            "ListExecutions" => self.list_executions(&req),
            "StopExecution" => self.stop_execution(&req),
            "DescribeStateMachineForExecution" => self.describe_sm_for_execution(&req),
            "GetExecutionHistory" => self.get_execution_history(&req),
            "RedriveExecution" => self.redrive_execution(&req),
            "PublishStateMachineVersion" => self.publish_version(&req),
            "ListStateMachineVersions" => self.list_versions(&req),
            "DeleteStateMachineVersion" => self.delete_version(&req),
            "CreateStateMachineAlias" => self.create_alias(&req),
            "UpdateStateMachineAlias" => self.update_alias(&req),
            "DeleteStateMachineAlias" => self.delete_alias(&req),
            "DescribeStateMachineAlias" => self.describe_alias(&req),
            "ListStateMachineAliases" => self.list_aliases(&req),
            "CreateActivity" => self.create_activity(&req),
            "DescribeActivity" => self.describe_activity(&req),
            "DeleteActivity" => self.delete_activity(&req),
            "ListActivities" => self.list_activities(&req),
            "GetActivityTask" => self.get_activity_task(&req),
            "SendTaskSuccess" => self.send_task_result(&req, true),
            "SendTaskFailure" => self.send_task_result(&req, false),
            "SendTaskHeartbeat" => self.send_task_heartbeat(&req),
            "DescribeMapRun" => self.describe_map_run(&req),
            "UpdateMapRun" => AwsResponse::json(200, json!({})),
            "ListMapRuns" => self.list_map_runs(&req),
            "ValidateStateMachineDefinition" => self.validate_definition(&req),
            "TagResource" => self.tag_resource(&req),
            "UntagResource" => self.untag_resource(&req),
            "ListTagsForResource" => self.list_tags(&req),
            "TestState" => AwsResponse::error(
                400,
                "NotImplemented",
                "The test_state action has not been implemented",
            ),
            other => AwsResponse::error(
                400,
                "ValidationException",
                &format!("The operation {other} is not implemented"),
            ),
        }
    }
}

impl Default for StepfunctionsHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn make_req(operation: &str, params: Value) -> AwsRequest {
        AwsRequest {
            service: "stepfunctions".to_string(),
            operation: operation.to_string(),
            account: 123456789012,
            region: "us-east-1".to_string(),
            params,
            body: Bytes::new(),
        }
    }

    fn body(resp: &AwsResponse) -> Value {
        serde_json::from_str(&resp.body).unwrap()
    }

    /// Create one clean machine and return its ARN (unique per test: the
    /// handler's state registry is process-global).
    fn mint_sm(handler: &StepfunctionsHandler, name: &str) -> String {
        let resp = handler.handle(make_req(
            "CreateStateMachine",
            json!({"stateMachineName": name,
                   "roleArn": "arn:aws:iam::123456789012:role/sfn-role",
                   "definition": "{}"}),
        ));
        assert_eq!(resp.status, 200, "body: {}", resp.body);
        body(&resp)["stateMachineArn"].as_str().unwrap().to_string()
    }

    // ==== create-path name gate + missing-entity wordings + token gate
    // (2026-11-11 differential; moto stepfunctions/models.py:642, 724,
    // 817-818, 836, 905-914; exceptions.py:50,66-73; paginator decode via
    // utilities/paginator.py:110-131 + botocore TokenDecoder) ====

    /// A space in the name is the measured `Invalid Name: '<name>'` row:
    /// 400 InvalidName, not a silent create.
    #[test]
    fn create_rejects_name_with_space() {
        let handler = StepfunctionsHandler::new();
        let resp = handler.handle(make_req(
            "CreateStateMachine",
            json!({"stateMachineName": "sfn-gate bad name",
                   "roleArn": "arn:aws:iam::123456789012:role/sfn-role",
                   "definition": "{}"}),
        ));
        assert_eq!(resp.status, 400, "body: {}", resp.body);
        assert!(
            resp.body.contains("\"__type\":\"InvalidName\""),
            "body: {}",
            resp.body
        );
        assert!(
            resp.body.contains("Invalid Name: 'sfn-gate bad name'"),
            "body: {}",
            resp.body
        );
    }

    /// An 81-char name rides NameTooLongException: ValidationException with
    /// the modeled 80-cap message (moto exceptions.py:66-73).
    #[test]
    fn create_rejects_long_name() {
        let handler = StepfunctionsHandler::new();
        let name: String = std::iter::repeat_n('a', 81).collect();
        let resp = handler.handle(make_req(
            "CreateStateMachine",
            json!({"stateMachineName": name,
                   "roleArn": "arn:aws:iam::123456789012:role/sfn-role",
                   "definition": "{}"}),
        ));
        assert_eq!(resp.status, 400, "body: {}", resp.body);
        assert!(
            resp.body.contains("\"__type\":\"ValidationException\""),
            "body: {}",
            resp.body
        );
        assert!(
            resp.body
                .contains("Member must have length less than or equal to 80"),
            "body: {}",
            resp.body
        );
    }

    /// StartExecution honors the same name gate when a name rides along
    /// (moto models.py:817-818) — the machine may exist or not; the name
    /// check runs first.
    #[test]
    fn start_execution_rejects_bad_name() {
        let handler = StepfunctionsHandler::new();
        let resp = handler.handle(make_req(
            "StartExecution",
            json!({"stateMachineArn": "arn:aws:states:us-east-1:123456789012:stateMachine:nope",
                   "name": "bad name"}),
        ));
        assert_eq!(resp.status, 400, "body: {}", resp.body);
        assert!(
            resp.body.contains("Invalid Name: 'bad name'"),
            "body: {}",
            resp.body
        );
    }

    /// ListExecutions resolves the machine first (moto :836 through the
    /// describe miss at :724): a missing machine is a 400 with the measured
    /// wording, never a 200-empty page.
    #[test]
    fn list_executions_missing_sm_400() {
        let handler = StepfunctionsHandler::new();
        let arn = "arn:aws:states:us-east-1:123456789012:stateMachine:gate-miss";
        let resp = handler.handle(make_req("ListExecutions", json!({"stateMachineArn": arn})));
        assert_eq!(resp.status, 400, "body: {}", resp.body);
        assert!(
            resp.body.contains(
                "State Machine Does Not Exist: 'arn:aws:states:us-east-1:123456789012:stateMachine:gate-miss'"
            ),
            "body: {}",
            resp.body
        );
    }

    /// ListStateMachineAliases behaves like ListExecutions on a missing
    /// machine (2026-11-11 measured row, both engines).
    #[test]
    fn list_aliases_missing_sm_400() {
        let handler = StepfunctionsHandler::new();
        let arn = "arn:aws:states:us-east-1:123456789012:stateMachine:gate-alias-miss";
        let resp = handler.handle(make_req(
            "ListStateMachineAliases",
            json!({"stateMachineArn": arn}),
        ));
        assert_eq!(resp.status, 400, "body: {}", resp.body);
        assert!(
            resp.body.contains("State Machine Does Not Exist"),
            "body: {}",
            resp.body
        );
    }

    /// The describe miss renders moto's exact wording after the port
    /// (previously "State machine <arn> does not exist" — the census row).
    #[test]
    fn missing_machine_wording_matches_moto() {
        let handler = StepfunctionsHandler::new();
        let arn = "arn:aws:states:us-east-1:123456789012:stateMachine:wording";
        let describe = handler.handle(make_req(
            "DescribeStateMachine",
            json!({"stateMachineArn": arn}),
        ));
        assert_eq!(describe.status, 400, "body: {}", describe.body);
        assert!(
            describe.body.contains(
                "State Machine Does Not Exist: 'arn:aws:states:us-east-1:123456789012:stateMachine:wording'"
            ),
            "body: {}",
            describe.body
        );
    }

    /// DescribeExecution's miss wording (moto models.py:852, "Execution
    /// Does Not Exist: '<arn>'").
    #[test]
    fn missing_execution_wording_matches_moto() {
        let handler = StepfunctionsHandler::new();
        mint_sm(&handler, "exec-wording-sm");
        let resp = handler.handle(make_req(
            "DescribeExecution",
            json!({"executionArn": "arn:aws:states:us-east-1:123456789012:execution:exec-wording-sm:absent"}),
        ));
        assert_eq!(resp.status, 400, "body: {}", resp.body);
        assert!(
            resp.body.contains("__type\":\"ExecutionDoesNotExist"),
            "body: {}",
            resp.body
        );
        assert!(
            resp.body.contains("Execution Does Not Exist"),
            "body: {}",
            resp.body
        );
    }

    /// A garbage pagination token is 400 InvalidToken "Invalid Token:
    /// Invalid token" on every PAGINATION_MODEL op (moto paginate decode,
    /// utilities/paginator.py:110-131); a healthy list without a nextToken
    /// param omits the member (moto's serializer drops None members,
    /// core/serialize.py:407-411 — the measured {"executions":[]} shape).
    #[test]
    fn list_ops_gate_tokens_and_omit_null_next_token() {
        let handler = StepfunctionsHandler::new();
        let arn = mint_sm(&handler, "token-gate-sm");
        for op in [
            "ListStateMachines",
            "ListExecutions",
            "ListStateMachineAliases",
            "ListActivities",
        ] {
            let resp = handler.handle(make_req(
                op,
                json!({ "stateMachineArn": arn, "nextToken": "AAA-garbage" }),
            ));
            assert_eq!(resp.status, 400, "{op}: {}", resp.body);
            assert!(
                resp.body.contains("\"Invalid Token: Invalid token\""),
                "{op}: {}",
                resp.body
            );
            assert!(
                resp.body.contains("__type\":\"InvalidToken\""),
                "{op}: {}",
                resp.body
            );
        }
        for op in ["ListStateMachines", "ListExecutions"] {
            let resp = handler.handle(make_req(op, json!({ "stateMachineArn": arn })));
            assert_eq!(resp.status, 200, "{op}: {}", resp.body);
            assert!(!resp.body.contains("nextToken"), "{op}: {}", resp.body);
        }
    }

    /// A machine-shaped execution ARN answers code InvalidArn (not
    /// ExecutionDoesNotExist) with the same "Execution Does Not Exist"
    /// text — the measured shape flip from moto's _validate_execution_arn
    /// regex arm (models.py:608-612, 930-935).
    #[test]
    fn describe_execution_bad_arn_shape_is_invalid_arn() {
        let handler = StepfunctionsHandler::new();
        let machine_shaped =
            "arn:aws:states:us-east-1:123456789012:stateMachine:sfn-blah:execution:gone";
        let resp = handler.handle(make_req(
            "DescribeExecution",
            json!({"executionArn": machine_shaped}),
        ));
        assert_eq!(resp.status, 400, "body: {}", resp.body);
        assert!(
            resp.body.contains("__type\":\"InvalidArn\""),
            "body: {}",
            resp.body
        );
        assert!(
            resp.body.contains("Execution Does Not Exist"),
            "body: {}",
            resp.body
        );
    }

    /// A token whose base64 decodes to valid JSON continues into moto's
    /// checksum arm: without the request's own parameterChecksum it
    /// answers InvalidToken "Invalid Token: Input inconsistent with page
    /// token: {'limit': 1}" — the measured repr (adversarially-verified
    /// 2026-11-12; the review's SF-1 row).
    #[test]
    fn token_gate_inconsistent_checksum_message() {
        let handler = StepfunctionsHandler::new();
        let arn = mint_sm(&handler, "token-decode-sm");
        use base64::Engine as _;
        let token = base64::engine::general_purpose::STANDARD.encode(r#"{"limit":1}"#);
        let resp = handler.handle(make_req(
            "ListExecutions",
            json!({ "stateMachineArn": arn, "nextToken": token }),
        ));
        assert_eq!(resp.status, 400, "body: {}", resp.body);
        assert!(
            resp.body
                .contains("Input inconsistent with page token: {'limit': 1}"),
            "body: {}",
            resp.body
        );
        assert!(
            resp.body.contains("__type\":\"InvalidToken"),
            "body: {}",
            resp.body
        );
    }

    #[test]
    fn test_create_and_list_sms() {
        let handler = StepfunctionsHandler::new();
        handler.handle(make_req(
            "CreateStateMachine",
            json!({
                "stateMachineName": "my-sm",
                "roleArn": "arn:aws:iam::123456789012:role/sfn-role",
                "definition": "{}"
            }),
        ));
        let resp = handler.handle(make_req("ListStateMachines", json!({})));
        assert_eq!(resp.status, 200);
        assert!(resp.body.contains("my-sm"));
    }

    #[test]
    fn test_start_execution() {
        let handler = StepfunctionsHandler::new();
        handler.handle(make_req("CreateStateMachine", json!({
            "stateMachineName": "exec-sm",
            "roleArn": "arn:aws:iam::123456789012:role/sfn-role",
            "definition": "{\"StartAt\": \"Pass\", \"States\": {\"Pass\": {\"Type\": \"Pass\", \"End\": true}}}"
        })));
        let resp = handler.handle(make_req(
            "StartExecution",
            json!({
                "stateMachineArn": "arn:aws:states:us-east-1:123456789012:stateMachine:exec-sm"
            }),
        ));
        assert_eq!(resp.status, 200);
        assert!(resp.body.contains("executionArn"));
    }

    #[test]
    fn test_handler_instances_share_state() {
        let a = StepfunctionsHandler::new();
        a.handle(make_req(
            "CreateStateMachine",
            json!({
                "stateMachineName": "shared-sm",
                "roleArn": "arn:aws:iam::123456789012:role/sfn-role",
                "definition": "{\"StartAt\": \"Pass\", \"States\": {}}"
            }),
        ));
        // A second instance must see (and be able to use) the same state.
        let b = StepfunctionsHandler::new();
        let resp = b.handle(make_req(
            "StartExecution",
            json!({
                "stateMachineArn": "arn:aws:states:us-east-1:123456789012:stateMachine:shared-sm"
            }),
        ));
        assert_eq!(
            resp.status, 200,
            "second instance sees the machine: {}",
            resp.body
        );
        assert!(resp.body.contains("executionArn"));
        let arn = body(&resp)["executionArn"].as_str().unwrap().to_string();
        let c = StepfunctionsHandler::new();
        let desc = body(&c.handle(make_req(
            "DescribeExecution",
            json!({ "executionArn": arn }),
        )));
        // The description comes from shared state, not from instance c's own.
        assert!(desc.get("executionArn").is_some());
    }

    #[test]
    fn test_self_referential_loop_hits_history_cap() {
        let handler = StepfunctionsHandler::new();
        handler.handle(make_req("CreateStateMachine", json!({
            "stateMachineName": "loop-sm",
            "roleArn": "arn:aws:iam::123456789012:role/sfn-role",
            "definition": "{\"StartAt\": \"Loop\", \"States\": {\"Loop\": {\"Type\": \"Pass\", \"Next\": \"Loop\"}}}"
        })));
        let resp = handler.handle(make_req(
            "StartExecution",
            json!({
                "stateMachineArn": "arn:aws:states:us-east-1:123456789012:stateMachine:loop-sm"
            }),
        ));
        let arn = body(&resp)["executionArn"].as_str().unwrap().to_string();
        // The engine must fail the execution once the event history is full.
        for _ in 0..500 {
            let exec = handler
                .get_state(123456789012, "us-east-1")
                .get_execution(&arn)
                .unwrap();
            if !exec.is_running() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        let desc = body(&handler.handle(make_req(
            "DescribeExecution",
            json!({ "executionArn": arn }),
        )));
        assert_eq!(desc["status"], "FAILED");
        assert_eq!(desc["error"], "States.ExceedMaxEventHistoryReached");
    }

    #[test]
    fn test_choice_executes_to_correct_branch() {
        let handler = StepfunctionsHandler::new();
        let definition = json!({
            "StartAt": "ChoiceState",
            "States": {
                "ChoiceState": {
                    "Type": "Choice",
                    "Choices": [{"Variable": "$.value", "NumericGreaterThan": 10, "Next": "Big"}],
                    "Default": "Small",
                },
                "Big": {"Type": "Pass", "Result": "big", "End": true},
                "Small": {"Type": "Pass", "Result": "small", "End": true},
            }
        })
        .to_string();
        handler.handle(make_req(
            "CreateStateMachine",
            json!({
                "stateMachineName": "choice-sm",
                "roleArn": "arn:aws:iam::123456789012:role/sfn-role",
                "definition": definition
            }),
        ));
        let resp = handler.handle(make_req(
            "StartExecution",
            json!({
                "stateMachineArn": "arn:aws:states:us-east-1:123456789012:stateMachine:choice-sm",
                "input": "{\"value\": 20}"
            }),
        ));
        let arn = body(&resp)["executionArn"].as_str().unwrap().to_string();
        // Wait briefly for the engine thread to finish.
        for _ in 0..50 {
            let exec = handler
                .get_state(123456789012, "us-east-1")
                .get_execution(&arn)
                .unwrap();
            if !exec.is_running() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        let desc = body(&handler.handle(make_req(
            "DescribeExecution",
            json!({ "executionArn": arn }),
        )));
        assert_eq!(desc["status"], "SUCCEEDED");
        assert_eq!(
            serde_json::from_str::<Value>(desc["output"].as_str().unwrap()).unwrap(),
            json!("big")
        );
    }

    #[test]
    fn test_choice_nested_and_rules() {
        let handler = StepfunctionsHandler::new();
        let definition = json!({
            "StartAt": "ChoiceState",
            "States": {
                "ChoiceState": {
                    "Type": "Choice",
                    "Choices": [{
                        "And": [
                            {"Variable": "$.a", "StringEquals": "1"},
                            {"Variable": "$.b", "NumericEquals": 2},
                        ],
                        "Next": "Both",
                    }],
                    "Default": "Small",
                },
                "Both": {"Type": "Pass", "Result": "both", "End": true},
                "Small": {"Type": "Pass", "Result": "small", "End": true},
            }
        })
        .to_string();
        handler.handle(make_req(
            "CreateStateMachine",
            json!({
                "stateMachineName": "choice-nested-sm",
                "roleArn": "arn:aws:iam::123456789012:role/sfn-role",
                "definition": definition
            }),
        ));
        let resp = handler.handle(make_req(
            "StartExecution",
            json!({
                "stateMachineArn": "arn:aws:states:us-east-1:123456789012:stateMachine:choice-nested-sm",
                "input": "{\"a\": \"1\", \"b\": 2}"
            }),
        ));
        let arn = body(&resp)["executionArn"].as_str().unwrap().to_string();
        for _ in 0..50 {
            let exec = handler
                .get_state(123456789012, "us-east-1")
                .get_execution(&arn)
                .unwrap();
            if !exec.is_running() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        let desc = body(&handler.handle(make_req(
            "DescribeExecution",
            json!({ "executionArn": arn }),
        )));
        assert_eq!(desc["status"], "SUCCEEDED");
        assert_eq!(
            serde_json::from_str::<Value>(desc["output"].as_str().unwrap()).unwrap(),
            json!("both")
        );
    }
}

/// moto master 96a7b09e (#10405): create_state_machine() must store the
/// requested `type` (previously every machine reported STANDARD) and
/// list_state_machines() must include the type of each machine.
/// DescribeStateMachine already echoed `type`; moto master renders both
/// forms.
#[cfg(test)]
mod catchup_fx5_type_tests {
    use super::*;
    use bytes::Bytes;

    fn make_req(operation: &str, params: Value) -> AwsRequest {
        AwsRequest {
            service: "stepfunctions".to_string(),
            operation: operation.to_string(),
            account: 876543210987,
            region: "us-fx5-catchup".to_string(),
            params,
            body: Bytes::new(),
        }
    }

    fn body(resp: &AwsResponse) -> Value {
        serde_json::from_str(&resp.body).unwrap()
    }

    fn handler() -> StepfunctionsHandler {
        StepfunctionsHandler::new()
    }

    fn definition() -> String {
        json!({"StartAt": "Pass", "States": {"Pass": {"Type": "Pass", "End": true}}}).to_string()
    }

    #[test]
    fn state_machine_creation_stores_the_requested_type() {
        let h = handler();
        let _ = h.handle(make_req(
            "CreateStateMachine",
            json!({
                "stateMachineName": "standard",
                "definition": definition(),
                "roleArn": "arn:aws:iam::876543210987:role/sfn-role",
            }),
        ));
        let _ = h.handle(make_req(
            "CreateStateMachine",
            json!({
                "stateMachineName": "express",
                "definition": definition(),
                "roleArn": "arn:aws:iam::876543210987:role/sfn-role",
                "type": "EXPRESS",
            }),
        ));

        let standard = body(&h.handle(make_req(
            "DescribeStateMachine",
            json!({"stateMachineArn": "arn:aws:states:us-fx5-catchup:876543210987:stateMachine:standard"}),
        )));
        assert_eq!(standard["type"], "STANDARD");

        let express = body(&h.handle(make_req(
            "DescribeStateMachine",
            json!({"stateMachineArn": "arn:aws:states:us-fx5-catchup:876543210987:stateMachine:express"}),
        )));
        assert_eq!(express["type"], "EXPRESS");
    }

    #[test]
    fn state_machine_list_returns_the_type_of_each_state_machine() {
        let h = handler();
        let _ = h.handle(make_req(
            "CreateStateMachine",
            json!({
                "stateMachineName": "standard",
                "definition": definition(),
                "roleArn": "arn:aws:iam::876543210987:role/sfn-role",
            }),
        ));
        let _ = h.handle(make_req(
            "CreateStateMachine",
            json!({
                "stateMachineName": "express",
                "definition": definition(),
                "roleArn": "arn:aws:iam::876543210987:role/sfn-role",
                "type": "EXPRESS",
            }),
        ));

        let list = body(&h.handle(make_req("ListStateMachines", json!({}))));
        let rows = list["stateMachines"].as_array().unwrap();
        let types: Vec<(&str, &str)> = rows
            .iter()
            .map(|r| (r["name"].as_str().unwrap(), r["type"].as_str().unwrap()))
            .collect();
        assert!(types.contains(&("standard", "STANDARD")), "got {types:?}");
        assert!(types.contains(&("express", "EXPRESS")), "got {types:?}");
    }
}
