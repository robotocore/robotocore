//! Step Functions in-memory state models.

use parking_lot::RwLock;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// A state machine definition.
#[derive(Debug)]
pub struct StateMachine {
    pub name: String,
    pub arn: String,
    /// The raw definition string exactly as supplied (mutable via UpdateStateMachine).
    pub definition: RwLock<String>,
    pub role_arn: RwLock<String>,
    /// STANDARD or EXPRESS.
    pub sm_type: String,
    pub creation_date: f64,
}

impl StateMachine {
    pub fn to_value(&self) -> Value {
        json!({
            "name": self.name,
            "stateMachineArn": self.arn,
            "definition": self.definition.read().clone(),
            "roleArn": self.role_arn.read().clone(),
            "type": self.sm_type,
            "creationDate": self.creation_date,
            "status": "ACTIVE",
        })
    }

    /// The current definition string (snapshot for the ASL engine).
    pub fn definition_string(&self) -> String {
        self.definition.read().clone()
    }
}

/// A single state machine execution, mutated by the ASL engine thread.
#[derive(Debug)]
pub struct Execution {
    pub arn: String,
    pub name: String,
    pub sm_arn: String,
    pub input: String,
    pub start_date: f64,
    pub status: RwLock<String>,
    pub stop_date: RwLock<Option<f64>>,
    pub output: RwLock<Option<String>>,
    pub error: RwLock<Option<String>>,
    pub cause: RwLock<Option<String>>,
    /// Full GetExecutionHistory event list (chronological).
    pub events: RwLock<Vec<Value>>,
}

impl Execution {
    pub fn is_aborted(&self) -> bool {
        *self.status.read() == "ABORTED"
    }

    pub fn is_running(&self) -> bool {
        *self.status.read() == "RUNNING"
    }

    /// Append a custom-constructed event; returns its id.
    pub fn push_event(&self, mut event: Value) -> i64 {
        let mut events = self.events.write();
        let id = events.len() as i64 + 1;
        event["id"] = json!(id);
        events.push(event);
        id
    }

    pub fn mark_succeeded(&self, output: String, last_event_id: i64) {
        if self.is_aborted() {
            return;
        }
        let now = crate::asl::now_f();
        {
            let mut events = self.events.write();
            let mut event = json!({
                "timestamp": now,
                "type": "ExecutionSucceeded",
                "previousEventId": last_event_id,
                "executionSucceededEventDetails": { "output": output },
            });
            let id = events.len() as i64 + 1;
            event["id"] = json!(id);
            events.push(event);
        }
        *self.output.write() = Some(output);
        *self.stop_date.write() = Some(now);
        *self.status.write() = "SUCCEEDED".to_string();
    }

    pub fn mark_failed(&self, error: String, cause: String, last_event_id: i64) {
        if self.is_aborted() {
            return;
        }
        let now = crate::asl::now_f();
        {
            let mut events = self.events.write();
            let mut event = json!({
                "timestamp": now,
                "type": "ExecutionFailed",
                "previousEventId": last_event_id,
                "executionFailedEventDetails": {
                    "error": error,
                    "cause": cause,
                },
            });
            let id = events.len() as i64 + 1;
            event["id"] = json!(id);
            events.push(event);
        }
        *self.error.write() = Some(error);
        *self.cause.write() = Some(cause);
        *self.stop_date.write() = Some(now);
        *self.status.write() = "FAILED".to_string();
    }

    /// Serialise for DescribeExecution / StartSyncExecution outputs.
    pub fn describe_value(&self) -> Value {
        let mut out = json!({
            "executionArn": self.arn,
            "stateMachineArn": self.sm_arn,
            "name": self.name,
            "status": self.status.read().clone(),
            "startDate": self.start_date,
            "input": self.input,
        });
        let obj = out.as_object_mut().unwrap();
        if let Some(stop) = *self.stop_date.read() {
            obj.insert("stopDate".into(), json!(stop));
        }
        if let Some(outp) = self.output.read().clone() {
            obj.insert("output".into(), json!(outp));
        }
        if let Some(err) = self.error.read().clone() {
            obj.insert("error".into(), json!(err));
            if let Some(cause) = self.cause.read().clone() {
                obj.insert("cause".into(), json!(cause));
            }
        }
        out
    }

    /// Summary entry for ListExecutions.
    pub fn list_value(&self) -> Value {
        let mut out = json!({
            "executionArn": self.arn,
            "stateMachineArn": self.sm_arn,
            "name": self.name,
            "status": self.status.read().clone(),
            "startDate": self.start_date,
        });
        if let Some(stop) = *self.stop_date.read() {
            out["stopDate"] = json!(stop);
        }
        out
    }
}

/// A Step Functions activity.
#[derive(Debug)]
pub struct Activity {
    pub name: String,
    pub arn: String,
    pub creation_date: f64,
    /// Payload scheduled but not yet polled by GetActivityTask.
    pub scheduled: RwLock<Value>,
}

#[derive(Debug)]
pub struct Alias {
    pub name: String,
    pub arn: String,
    pub routing_configuration: RwLock<Vec<Value>>,
    pub creation_date: f64,
    pub update_date: RwLock<f64>,
}

impl Alias {
    pub fn describe_value(&self) -> Value {
        json!({
            "stateMachineAliasArn": self.arn,
            "name": self.name,
            "description": "",
            "routingConfiguration": self.routing_configuration.read().clone(),
            "creationDate": self.creation_date,
            "updateDate": self.update_date.read().clone(),
        })
    }
}

/// Store for one (account, region).
#[derive(Clone, Default)]
pub struct StepfunctionsState {
    /// State machines keyed by ARN.
    pub state_machines: Arc<RwLock<HashMap<String, Arc<StateMachine>>>>,
    /// Executions keyed by ARN.
    pub executions: Arc<RwLock<HashMap<String, Arc<Execution>>>>,
    /// Activities keyed by ARN.
    pub activities: Arc<RwLock<HashMap<String, Arc<Activity>>>>,
    /// Published versions: state machine ARN -> (version number -> creation date).
    pub versions: Arc<RwLock<HashMap<String, BTreeMap<u64, f64>>>>,
    /// Aliases keyed by alias ARN.
    pub aliases: Arc<RwLock<HashMap<String, Arc<Alias>>>>,
    /// Tags keyed by resource ARN.
    pub tags: Arc<RwLock<HashMap<String, Vec<Value>>>>,
    /// Live callback task tokens: task token -> activity ARN.
    pub task_tokens: Arc<RwLock<HashMap<String, String>>>,
}

impl StepfunctionsState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_machine(&self, arn: &str) -> Option<Arc<StateMachine>> {
        self.state_machines.read().get(arn).cloned()
    }

    pub fn get_execution(&self, arn: &str) -> Option<Arc<Execution>> {
        self.executions.read().get(arn).cloned()
    }

    pub fn get_activity(&self, arn: &str) -> Option<Arc<Activity>> {
        self.activities.read().get(arn).cloned()
    }
}

impl std::fmt::Debug for StepfunctionsState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StepfunctionsState")
    }
}
