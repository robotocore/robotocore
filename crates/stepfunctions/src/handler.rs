//! Step Functions operation handler.

use crate::asl;
use crate::models::{Alias, Execution, StateMachine, StepfunctionsState};
use crate::protocol::{AwsRequest, AwsResponse};
use parking_lot::RwLock;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

pub struct StepfunctionsHandler {
    state: RwLock<HashMap<(u64, String), StepfunctionsState>>,
}

fn param_str(req: &AwsRequest, keys: &[&str]) -> String {
    for key in keys {
        if let Some(v) = req.params.get(*key).and_then(|v| v.as_str()) {
            return v.to_string();
        }
    }
    String::new()
}

impl StepfunctionsHandler {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(HashMap::new()),
        }
    }

    fn get_state(&self, account: u64, region: &str) -> StepfunctionsState {
        let mut states = self.state.write();
        states
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
                &format!("State machine {arn} not found"),
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
                &format!("State machine {arn} does not exist"),
            ),
        }
    }

    fn list_sms(&self, req: &AwsRequest) -> AwsResponse {
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
        AwsResponse::json(
            200,
            json!({ "stateMachines": rows, "nextToken": Value::Null }),
        )
    }

    fn update_sm(&self, req: &AwsRequest) -> AwsResponse {
        let arn = param_str(req, &["stateMachineArn"]);
        let state = self.get_state(req.account, &req.region);
        if state.get_machine(&arn).is_none() {
            return AwsResponse::error(
                400,
                "StateMachineDoesNotExist",
                &format!("State machine {arn} does not exist"),
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
        let state = self.get_state(req.account, &req.region);
        let Some(machine) = state.get_machine(&sm_arn) else {
            return AwsResponse::error(
                400,
                "StateMachineDoesNotExist",
                &format!("State machine {sm_arn} not found"),
            );
        };
        let name = param_str(req, &["name"]);
        let exec_name = if name.is_empty() {
            uuid::Uuid::new_v4().to_string()
        } else {
            name
        };
        let exec_arn = format!("{sm_arn}:execution:{}", exec_name);
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
        let exec_arn = format!("{}:execution:{}", machine.arn, name);
        let arn = exec_arn.replace("stateMachine:", "express:");
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
            asl::run_flow(&ctx, states, start_at, parsed)
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
                &format!("State machine {sm_arn} does not exist"),
            );
        };
        let exec = self.run_sync_execution(&state, &machine, input);
        AwsResponse::json(200, exec.describe_value())
    }

    fn describe_exec(&self, req: &AwsRequest) -> AwsResponse {
        let arn = param_str(req, &["executionArn"]);
        let state = self.get_state(req.account, &req.region);
        match state.get_execution(&arn) {
            Some(exec) => AwsResponse::json(200, exec.describe_value()),
            None => AwsResponse::error(
                400,
                "ExecutionDoesNotExist",
                &format!("Execution {arn} does not exist"),
            ),
        }
    }

    fn list_executions(&self, req: &AwsRequest) -> AwsResponse {
        let sm_arn = param_str(req, &["stateMachineArn"]);
        let status_filter = param_str(req, &["statusFilter"]);
        let state = self.get_state(req.account, &req.region);
        let mut rows: Vec<Value> = state
            .executions
            .read()
            .values()
            .filter(|exec| exec.sm_arn == sm_arn)
            .filter(|exec| status_filter.is_empty() || *exec.status.read() == status_filter)
            .map(|exec| exec.list_value())
            .collect();
        rows.reverse();
        AwsResponse::json(200, json!({ "executions": rows, "nextToken": Value::Null }))
    }

    fn stop_execution(&self, req: &AwsRequest) -> AwsResponse {
        let exec_arn = param_str(req, &["executionArn"]);
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
                &format!("Execution {exec_arn} does not exist"),
            ),
        }
    }

    fn describe_sm_for_execution(&self, req: &AwsRequest) -> AwsResponse {
        let arn = param_str(req, &["executionArn"]);
        let state = self.get_state(req.account, &req.region);
        let Some(exec) = state.get_execution(&arn) else {
            return AwsResponse::error(
                400,
                "ExecutionDoesNotExist",
                &format!("Execution {arn} does not exist"),
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
                &format!("State machine {} does not exist", exec.sm_arn),
            ),
        }
    }

    fn get_execution_history(&self, req: &AwsRequest) -> AwsResponse {
        let arn = param_str(req, &["executionArn"]);
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
                &format!("Execution {arn} does not exist"),
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
                &format!("State machine {arn} does not exist"),
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
        let arn = param_str(req, &["stateMachineAliasArn"]);
        let state = self.get_state(req.account, &req.region);
        let existing = state.aliases.read().get(&arn).cloned();
        match existing {
            Some(alias) => {
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
            None => AwsResponse::error(
                400,
                "ResourceNotFound",
                &format!("Resource not found: '{arn}'"),
            ),
        }
    }

    fn delete_alias(&self, req: &AwsRequest) -> AwsResponse {
        let arn = param_str(req, &["stateMachineAliasArn"]);
        let state = self.get_state(req.account, &req.region);
        state.aliases.write().remove(&arn);
        AwsResponse::json(200, json!({}))
    }

    fn describe_alias(&self, req: &AwsRequest) -> AwsResponse {
        let arn = param_str(req, &["stateMachineAliasArn"]);
        let state = self.get_state(req.account, &req.region);
        let existing = state.aliases.read().get(&arn).cloned();
        match existing {
            Some(alias) => AwsResponse::json(200, alias.describe_value()),
            None => AwsResponse::error(
                400,
                "ResourceNotFound",
                &format!("Resource not found: '{arn}'"),
            ),
        }
    }

    fn list_aliases(&self, req: &AwsRequest) -> AwsResponse {
        let sm_arn = param_str(req, &["stateMachineArn"]);
        let state = self.get_state(req.account, &req.region);
        let rows: Vec<Value> = state
            .aliases
            .read()
            .values()
            .filter(|alias| alias.arn.starts_with(&format!("{sm_arn}:")))
            .map(|alias| json!({ "stateMachineAliasArn": alias.arn, "creationDate": alias.creation_date }))
            .collect();
        AwsResponse::json(
            200,
            json!({ "stateMachineAliases": rows, "nextToken": Value::Null }),
        )
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
        AwsResponse::json(200, json!({ "activities": rows, "nextToken": Value::Null }))
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
}
