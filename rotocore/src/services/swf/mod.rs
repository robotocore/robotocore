//! AWS SWF (Simple Workflow Service) backend — moto-equivalent (json protocol).
//!
//! Replaces the moto sidecar for `swf` (`X-Amz-Target: SimpleWorkflowService.<Op>`).
//! State is per-(account, region), mirroring moto's `SWFBackend` (moto/swf/models):
//! domains, activity/workflow types, workflow executions with full event
//! history, decision/activity task lists, decision timers, and resource tags.
//!
//! Contract: the restored suite `tests/compatibility/test_swf_compat.py` and
//! the moto 5.1.23 fork's swf source (`models/*.py`, `responses.py`). moto's
//! hardcoded history timestamps repeat on the wire (type creationDate
//! 1420066800, execution medium startTimestamp 1420066800.123) — the same
//! values moto served. Documented simplifications, all unreachable through
//! the boto3 suite: long polls answer instantly instead of moto's `sleep(1)`
//! on an empty task list, decision timers track state without firing
//! `TimerFired` asynchronously (no background threads), bad-typed parameters
//! skip moto's `SerializationException` checks (boto3 serializes shapes
//! client-side), and moto's dict-vs-string `closeStatusFilter` quirk in
//! ListClosedWorkflowExecutions (which always empties the result) is
//! reproduced as-is.

use crate::proto::parse_json_body;
use crate::store::{self, ScopeKey};
use crate::{Request, Response};
use parking_lot::RwLock;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

type Stores = Arc<RwLock<HashMap<ScopeKey, SwfStore>>>;

fn stores() -> Stores {
    store::slot::<SwfStore>("swf")
}

/// Per-(account, region) in-memory state for Amazon SWF.
#[derive(Default)]
struct SwfStore {
    domains: Vec<Domain>,
}

fn with_store(req: &Request, f: impl FnOnce(&mut SwfStore) -> Response) -> Response {
    let slot = stores();
    let mut map = slot.write();
    let st = map.entry((req.account, req.region.clone())).or_default();
    f(st)
}

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

const ACTIVITY: &str = "activity";
const WORKFLOW: &str = "workflow";

const REGISTERED: &str = "REGISTERED";
const DEPRECATED: &str = "DEPRECATED";
const OPEN: &str = "OPEN";
const CLOSED: &str = "CLOSED";
const SCHEDULED: &str = "SCHEDULED";
const STARTED: &str = "STARTED";
const COMPLETED: &str = "COMPLETED";
const TIMED_OUT: &str = "TIMED_OUT";
const FAILED: &str = "FAILED";
const CANCELED: &str = "CANCELED";

/// moto's `KNOWN_DECISION_TYPES` — order decides validation error messages.
const KNOWN_DECISION_TYPES: [&str; 13] = [
    "CompleteWorkflowExecution",
    "StartTimer",
    "RequestCancelExternalWorkflowExecution",
    "SignalExternalWorkflowExecution",
    "CancelTimer",
    "RecordMarker",
    "ScheduleActivityTask",
    "ContinueAsNewWorkflowExecution",
    "ScheduleLambdaFunction",
    "FailWorkflowExecution",
    "RequestCancelActivityTask",
    "StartChildWorkflowExecution",
    "CancelWorkflowExecution",
];

/// Members moto's DECISIONS_FIELDS marks `required` per decision-attribute
/// shape; lists preserve moto's map order so messages match on multi-faults.
fn required_decision_fields(attr: &str) -> &'static [&'static str] {
    match attr {
        "cancelTimerDecisionAttributes" => &["timerId"],
        "recordMarkerDecisionAttributes" => &["markerName"],
        "requestCancelActivityTaskDecisionAttributes" => &["activityId"],
        "requestCancelExternalWorkflowExecutionDecisionAttributes" => &["workflowId"],
        "scheduleActivityTaskDecisionAttributes" => &["activityId", "activityType"],
        "scheduleLambdaFunctionDecisionAttributes" => &["id", "name"],
        "signalExternalWorkflowExecutionDecisionAttributes" => &["signalName", "workflowId"],
        "startChildWorkflowExecutionDecisionAttributes" => &["workflowId", "workflowType"],
        "startTimerDecisionAttributes" => &["startToFireTimeout", "timerId"],
        _ => &[],
    }
}

/// moto's `get_partition`.
fn partition(region: &str) -> &'static str {
    if region.starts_with("cn-") {
        "aws-cn"
    } else if region.starts_with("us-gov-") {
        "aws-us-gov"
    } else if region.starts_with("us-iso-") {
        "aws-iso"
    } else if region.starts_with("us-isob-") {
        "aws-iso-b"
    } else if region.starts_with("eusc-") {
        "aws-eusc"
    } else {
        "aws"
    }
}

fn unix_now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// Registered or deprecated activity/workflow type (moto's GenericType).
#[derive(Clone)]
struct SwfType {
    kind: &'static str,
    name: String,
    version: String,
    status: String,
    description: Option<String>,
    /// From `defaultTaskList.name` — moto stores it as the `task_list` attr.
    task_list: Option<String>,
    // Activity defaults.
    default_task_heartbeat_timeout: Option<String>,
    default_task_schedule_to_close_timeout: Option<String>,
    default_task_schedule_to_start_timeout: Option<String>,
    default_task_start_to_close_timeout: Option<String>,
    // Workflow defaults.
    default_child_policy: Option<String>,
    default_execution_start_to_close_timeout: Option<String>,
    default_task_priority: Option<String>,
    default_lambda_role: Option<String>,
}

/// moto's `GenericType._configuration_keys`, per kind, driving the full dict.
fn configuration_keys(kind: &str) -> &'static [&'static str] {
    match kind {
        ACTIVITY => &[
            "defaultTaskHeartbeatTimeout",
            "defaultTaskScheduleToCloseTimeout",
            "defaultTaskScheduleToStartTimeout",
            "defaultTaskStartToCloseTimeout",
        ],
        _ => &[
            "defaultChildPolicy",
            "defaultExecutionStartToCloseTimeout",
            "defaultTaskStartToCloseTimeout",
            "defaultTaskPriority",
            "defaultLambdaRole",
        ],
    }
}

impl SwfType {
    fn new(kind: &'static str, name: &str, version: &str) -> Self {
        SwfType {
            kind,
            name: name.to_string(),
            version: version.to_string(),
            status: REGISTERED.to_string(),
            description: None,
            task_list: None,
            default_task_heartbeat_timeout: None,
            default_task_schedule_to_close_timeout: None,
            default_task_schedule_to_start_timeout: None,
            default_task_start_to_close_timeout: None,
            default_child_policy: None,
            default_execution_start_to_close_timeout: None,
            default_task_priority: None,
            default_lambda_role: None,
        }
    }

    /// moto's type-descriptor attr per configuration key (`None` when unset).
    fn default_attr(&self, key: &str) -> Option<&str> {
        match key {
            "defaultTaskHeartbeatTimeout" => self.default_task_heartbeat_timeout.as_deref(),
            "defaultTaskScheduleToCloseTimeout" => {
                self.default_task_schedule_to_close_timeout.as_deref()
            }
            "defaultTaskScheduleToStartTimeout" => {
                self.default_task_schedule_to_start_timeout.as_deref()
            }
            "defaultTaskStartToCloseTimeout" => self.default_task_start_to_close_timeout.as_deref(),
            "defaultChildPolicy" => self.default_child_policy.as_deref(),
            "defaultExecutionStartToCloseTimeout" => {
                self.default_execution_start_to_close_timeout.as_deref()
            }
            "defaultTaskPriority" => self.default_task_priority.as_deref(),
            "defaultLambdaRole" => self.default_lambda_role.as_deref(),
            _ => None,
        }
    }

    fn to_short_dict(&self) -> Value {
        json!({ "name": self.name, "version": self.version })
    }

    /// moto's `GenericType.to_medium_dict`.
    fn to_medium_dict(&self) -> Value {
        let mut hsh = json!({
            format!("{}Type", self.kind): self.to_short_dict(),
            "creationDate": 1420066800,
            "status": self.status,
        });
        if self.status == DEPRECATED {
            hsh["deprecationDate"] = json!(1422745200);
        }
        if let Some(desc) = &self.description {
            hsh["description"] = json!(desc);
        }
        hsh
    }

    /// moto's `GenericType.to_full_dict`.
    fn to_full_dict(&self) -> Value {
        let mut configuration = Map::new();
        if let Some(tl) = &self.task_list {
            configuration.insert("defaultTaskList".into(), json!({ "name": tl }));
        }
        for key in configuration_keys(self.kind) {
            if let Some(v) = self.default_attr(key) {
                configuration.insert((*key).into(), json!(v));
            }
        }
        json!({ "typeInfo": self.to_medium_dict(), "configuration": configuration })
    }
}

/// Deciders, writers and pollers all key off (workflow_id, run_id); tasks
/// store the owning execution's index into `Domain::workflow_executions`
/// (append-only, never pruned — the index stays valid for the process life).
struct WorkflowExecution {
    workflow_id: String,
    run_id: String,
    cancel_requested: bool,
    close_cause: Option<String>,
    close_status: Option<String>,
    close_timestamp: Option<f64>,
    execution_status: String,
    latest_activity_task_timestamp: Option<f64>,
    latest_execution_context: Option<String>,
    start_timestamp: Option<f64>,
    tag_list: Vec<String>,
    timeout_type: Option<String>,
    // Type snapshot (moto keeps a reference; values never change after
    // registration, so an owned copy is equivalent).
    wtype: SwfType,
    execution_start_to_close_timeout: String,
    task_list: String,
    task_start_to_close_timeout: String,
    child_policy: String,
    input: Option<String>,
    open_counts: OpenCounts,
    events: Vec<HistoryEvent>,
    previous_started_event_id: Option<i64>,
    /// The "should_schedule_decision_next" flag moto sets on failed schedules.
    should_schedule_decision_next: bool,
    timers: BTreeMap<String, TimerState>,
}

#[derive(Default)]
struct OpenCounts {
    open_timers: i64,
    open_decision_tasks: i64,
    open_activity_tasks: i64,
    open_child_workflow_executions: i64,
    open_lambda_functions: i64,
}

impl OpenCounts {
    fn to_dict(&self) -> Value {
        json!({
            "openTimers": self.open_timers,
            "openDecisionTasks": self.open_decision_tasks,
            "openActivityTasks": self.open_activity_tasks,
            "openChildWorkflowExecutions": self.open_child_workflow_executions,
            "openLambdaFunctions": self.open_lambda_functions,
        })
    }
}

struct TimerState {
    started_event_id: i64,
    /// moto's `is_alive()`; a cancelled timer loses liveness. Timers never
    /// fire asynchronously in this port (see module docs).
    alive: bool,
}

impl WorkflowExecution {
    fn open(&self) -> bool {
        self.execution_status == OPEN
    }

    /// moto's `WorkflowExecution.to_short_dict`.
    fn to_short_dict(&self) -> Value {
        json!({ "workflowId": self.workflow_id, "runId": self.run_id })
    }

    /// moto's `WorkflowExecution.to_medium_dict` (startTimestamp is one of
    /// moto's hardcoded timestamps).
    fn to_medium_dict(&self) -> Value {
        let mut hsh = json!({
            "execution": self.to_short_dict(),
            "workflowType": self.wtype.to_short_dict(),
            "startTimestamp": 1420066800.123,
            "executionStatus": self.execution_status,
            "cancelRequested": self.cancel_requested,
        });
        if !self.tag_list.is_empty() {
            hsh["tagList"] = json!(self.tag_list);
        }
        hsh
    }

    /// moto's `WorkflowExecution.to_full_dict`.
    fn to_full_dict(&self) -> Value {
        let mut hsh = json!({
            "executionInfo": self.to_medium_dict(),
            "executionConfiguration": { "taskList": { "name": self.task_list } },
        });
        let info = &mut hsh["executionInfo"];
        if self.execution_status == CLOSED {
            info["closeStatus"] = json!(self.close_status);
            info["closeTimestamp"] = json!(self.close_timestamp);
        }
        let cfg = &mut hsh["executionConfiguration"];
        // moto's _configuration_keys: taskPriority has no attribute on the
        // execution — the loop skips it via hasattr.
        if !self.execution_start_to_close_timeout.is_empty() {
            cfg["executionStartToCloseTimeout"] = json!(self.execution_start_to_close_timeout);
        }
        if !self.child_policy.is_empty() {
            cfg["childPolicy"] = json!(self.child_policy);
        }
        if !self.task_start_to_close_timeout.is_empty() {
            cfg["taskStartToCloseTimeout"] = json!(self.task_start_to_close_timeout);
        }
        hsh["openCounts"] = self.open_counts.to_dict();
        if let Some(ctx) = &self.latest_execution_context {
            hsh["latestExecutionContext"] = json!(ctx);
        }
        if let Some(ts) = self.latest_activity_task_timestamp {
            hsh["latestActivityTaskTimestamp"] = json!(ts);
        }
        hsh
    }

    /// moto's `WorkflowExecution.to_list_dict`.
    fn to_list_dict(&self) -> Value {
        let mut hsh = json!({
            "execution": { "workflowId": self.workflow_id, "runId": self.run_id },
            "workflowType": self.wtype.to_short_dict(),
            "startTimestamp": self.start_timestamp,
            "executionStatus": self.execution_status,
            "cancelRequested": self.cancel_requested,
        });
        if !self.tag_list.is_empty() {
            hsh["tagList"] = json!(self.tag_list);
        }
        if let Some(cs) = &self.close_status {
            hsh["closeStatus"] = json!(cs);
        }
        if let Some(ct) = self.close_timestamp {
            hsh["closeTimestamp"] = json!(ct);
        }
        hsh
    }

    /// Second deadline for the execution timeout (`first_timeout` in moto).
    fn first_timeout_ts(&self) -> Option<f64> {
        if !self.open() {
            return None;
        }
        let start = self.start_timestamp?;
        let secs = self.execution_start_to_close_timeout.parse::<f64>().ok()?;
        Some(start + secs)
    }

    /// moto's `WorkflowExecution.timeout`.
    fn timeout(&mut self, timestamp: f64) {
        self.execution_status = CLOSED.to_string();
        self.close_status = Some("TIMED_OUT".to_string());
        self.timeout_type = Some("START_TO_CLOSE".to_string());
        self.add_timed_event(
            timestamp,
            "WorkflowExecutionTimedOut",
            vec![
                ("childPolicy", json!(self.child_policy)),
                ("timeoutType", json!("START_TO_CLOSE")),
            ],
        );
    }
}

/// History event with pre-shaped camelCase attributes (moto's HistoryEvent).
struct HistoryEvent {
    event_id: i64,
    event_type: String,
    event_timestamp: f64,
    attributes: Map<String, Value>,
}

impl HistoryEvent {
    fn to_dict(&self) -> Value {
        let key = format!("{}EventAttributes", decapitalize(&self.event_type));
        json!({
            "eventId": self.event_id,
            "eventType": self.event_type,
            "eventTimestamp": self.event_timestamp,
            key: self.attributes,
        })
    }
}

/// Decapitalized first letter (moto's `utils.decapitalize`).
fn decapitalize(key: &str) -> String {
    let mut chars = key.chars();
    match chars.next() {
        Some(first) => first.to_lowercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

/// moto's HistoryEvent truthiness filter: falsy values drop off the wire.
fn truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0 && !f.is_nan()).unwrap_or(false),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

/// One scheduled decision task (moto's DecisionTask), stored per task list.
struct DecisionTask {
    wfe_idx: usize,
    task_token: String,
    scheduled_event_id: i64,
    previous_started_event_id: Option<i64>,
    started_event_id: Option<i64>,
    started_timestamp: Option<f64>,
    state: String,
    /// Domain-level monotonic sequence — moto's `scheduled_at` ordering.
    scheduled_at: u64,
    timeout_type: Option<String>,
}

/// One scheduled activity task (moto's ActivityTask).
struct ActivityTask {
    wfe_idx: usize,
    activity_id: String,
    activity_type_name: String,
    activity_type_version: String,
    details: Option<String>,
    input: Option<String>,
    last_heartbeat_timestamp: f64,
    scheduled_event_id: i64,
    started_event_id: Option<i64>,
    state: String,
    task_token: String,
    timeouts: ActivityTimeouts,
    timeout_type: Option<String>,
    scheduled_at: u64,
}

#[derive(Clone)]
struct ActivityTimeouts {
    /// moto's first_timeout only consumes heartbeatTimeout; the other
    /// schedule/start timeouts are validated at schedule time and dropped.
    heartbeat: String,
}

impl ActivityTask {
    fn open(&self) -> bool {
        self.state == SCHEDULED || self.state == STARTED
    }

    /// moto's `ActivityTask.to_full_dict`.
    fn to_full_dict(&self, wfe: &WorkflowExecution) -> Value {
        let mut hsh = json!({
            "activityId": self.activity_id,
            "activityType": { "name": self.activity_type_name, "version": self.activity_type_version },
            "taskToken": self.task_token,
            "startedEventId": self.started_event_id,
            "workflowExecution": wfe.to_short_dict(),
        });
        if let Some(inp) = &self.input {
            hsh["input"] = json!(inp);
        }
        hsh
    }
}

/// One SWF domain (moto's Domain).
struct Domain {
    name: String,
    /// The retention string as the client sent it ("30", "90", ...).
    retention: String,
    description: Option<String>,
    status: String,
    account: u64,
    region: String,
    types: BTreeMap<&'static str, BTreeMap<String, BTreeMap<String, SwfType>>>,
    workflow_executions: Vec<WorkflowExecution>,
    activity_task_lists: BTreeMap<String, Vec<ActivityTask>>,
    decision_task_lists: BTreeMap<String, Vec<DecisionTask>>,
    /// moto keeps an ordered list of {key, value} dicts.
    tags: Vec<(String, String)>,
    seq: u64,
}

impl Domain {
    fn new(
        account: u64,
        region: &str,
        name: &str,
        retention: &str,
        description: Option<&str>,
    ) -> Self {
        Domain {
            name: name.to_string(),
            retention: retention.to_string(),
            description: description.map(String::from),
            status: REGISTERED.to_string(),
            account,
            region: region.to_string(),
            types: BTreeMap::new(),
            workflow_executions: Vec::new(),
            activity_task_lists: BTreeMap::new(),
            decision_task_lists: BTreeMap::new(),
            tags: Vec::new(),
            seq: 0,
        }
    }

    fn arn(&self) -> String {
        format!(
            "arn:{}:swf:{}:{}:/domain/{}",
            partition(&self.region),
            self.region,
            self.account,
            self.name
        )
    }

    fn to_short_dict(&self) -> Value {
        let mut hsh = json!({ "name": self.name, "status": self.status });
        if self.description.as_deref().is_some_and(|d| !d.is_empty()) {
            hsh["description"] = json!(self.description);
        }
        hsh["arn"] = json!(self.arn());
        hsh
    }

    fn to_full_dict(&self) -> Value {
        json!({
            "domainInfo": self.to_short_dict(),
            "configuration": { "workflowExecutionRetentionPeriodInDays": self.retention },
        })
    }

    fn next_seq(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }

    fn find_type(&self, kind: &str, name: &str, version: &str) -> Option<&SwfType> {
        self.types.get(kind)?.get(name)?.get(version)
    }

    fn add_type(&mut self, t: SwfType) {
        self.types
            .entry(t.kind)
            .or_default()
            .entry(t.name.clone())
            .or_default()
            .insert(t.version.clone(), t);
    }

    /// typeInfos for List{Activity,Workflow}Types — sorted by name, reversed.
    fn list_type_infos(&self, kind: &str, status: &str, reverse_order: bool) -> Vec<Value> {
        let mut infos: Vec<(String, Value)> = self
            .types
            .get(kind)
            .map(|per_name| {
                per_name
                    .values()
                    .flat_map(|versions| versions.values())
                    .filter(|t| t.status == status)
                    .map(|t| (t.name.clone(), t.to_medium_dict()))
                    .collect()
            })
            .unwrap_or_default();
        infos.sort_by(|a, b| a.0.cmp(&b.0));
        let dicts: Vec<Value> = infos.into_iter().map(|(_, v)| v).collect();
        if reverse_order {
            dicts.into_iter().rev().collect()
        } else {
            dicts
        }
    }

    fn remove_type(&mut self, kind: &str, name: &str, version: &str) {
        if let Some(per_name) = self.types.get_mut(kind) {
            if let Some(versions) = per_name.get_mut(name) {
                versions.remove(version);
                if versions.is_empty() {
                    per_name.remove(name);
                }
            }
        }
    }

    /// moto's `Domain.get_workflow_execution` query semantics:
    /// - with run_id: exact (workflow_id, run_id) match, any status;
    /// - without: first open execution with that workflow_id.
    fn wfe_index(&self, workflow_id: &str, run_id: Option<&str>) -> Option<usize> {
        match run_id {
            Some(r) => self
                .workflow_executions
                .iter()
                .position(|w| w.workflow_id == workflow_id && w.run_id == r),
            None => self
                .workflow_executions
                .iter()
                .position(|w| w.workflow_id == workflow_id && w.open()),
        }
    }

    /// moto's get_workflow_execution with raise_if_closed handling; the
    /// UnknownResourceFault message differs with/without run_id.
    fn require_wfe(
        &self,
        workflow_id: &str,
        run_id: Option<&str>,
        raise_if_closed: bool,
    ) -> Result<usize, Response> {
        let idx = self.wfe_index(workflow_id, run_id).filter(|&i| {
            !(raise_if_closed && self.workflow_executions[i].execution_status == CLOSED)
        });
        idx.ok_or_else(|| unknown_execution(workflow_id, run_id))
    }

    fn wfe_mut(&mut self, idx: usize) -> &mut WorkflowExecution {
        &mut self.workflow_executions[idx]
    }

    fn decision_task_mut(&mut self, token: &str) -> Option<&mut DecisionTask> {
        self.decision_task_lists
            .values_mut()
            .find_map(|tasks| tasks.iter_mut().find(|t| t.task_token == token))
    }

    fn activity_task_mut(&mut self, token: &str) -> Option<&mut ActivityTask> {
        self.activity_task_lists
            .values_mut()
            .find_map(|tasks| tasks.iter_mut().find(|t| t.task_token == token))
    }

    // -- history -----------------------------------------------------------

    fn schedule_decision_task(&mut self, wfe_idx: usize) {
        let (task_list, tsct) = {
            let w = &self.workflow_executions[wfe_idx];
            (w.task_list.clone(), w.task_start_to_close_timeout.clone())
        };
        let mut has_started = false;
        let mut has_scheduled = false;
        for tasks in self.decision_task_lists.values() {
            for t in tasks {
                if t.wfe_idx == wfe_idx {
                    match t.state.as_str() {
                        STARTED => has_started = true,
                        SCHEDULED => has_scheduled = true,
                        _ => {}
                    }
                }
            }
        }
        // moto: a started decision task blocks a second concurrent schedule.
        if has_started && has_scheduled {
            return;
        }
        let tsct_value = json!(tsct);
        let evt_id = self.wfe_mut(wfe_idx).add_event(
            "DecisionTaskScheduled",
            vec![
                ("startToCloseTimeout", tsct_value),
                ("taskList", json!({ "name": task_list })),
            ],
        );
        let task = DecisionTask {
            wfe_idx,
            task_token: Uuid::new_v4().to_string(),
            scheduled_event_id: evt_id,
            previous_started_event_id: None,
            started_event_id: None,
            started_timestamp: None,
            state: SCHEDULED.to_string(),
            scheduled_at: self.next_seq(),
            timeout_type: None,
        };
        self.decision_task_lists
            .entry(task_list)
            .or_default()
            .push(task);
        self.wfe_mut(wfe_idx).open_counts.open_decision_tasks += 1;
    }

    /// Decision task info needed to start it (moto's start_decision_task).
    fn start_decision_task(&mut self, wfe_idx: usize, token: &str, identity: Option<&str>) {
        let scheduled_event_id = match self.decision_task_mut(token) {
            Some(t) => t.scheduled_event_id,
            None => return,
        };
        let evt_id = self.wfe_mut(wfe_idx).add_event(
            "DecisionTaskStarted",
            vec![
                ("scheduledEventId", json!(scheduled_event_id)),
                ("identity", json!(identity)),
            ],
        );
        let previous = self.wfe_mut(wfe_idx).previous_started_event_id;
        if let Some(t) = self.decision_task_mut(token) {
            t.state = STARTED.to_string();
            t.started_timestamp = Some(unix_now());
            t.started_event_id = Some(evt_id);
            t.previous_started_event_id = previous;
        }
        self.wfe_mut(wfe_idx).previous_started_event_id = Some(evt_id);
    }

    fn decision_response(&self, token: &str, reverse_order: bool) -> Value {
        let Some((wfe_idx, previous, started)) = self
            .decision_task_lists
            .values()
            .flat_map(|tasks| tasks.iter())
            .find(|t| t.task_token == token)
            .map(|t| (t.wfe_idx, t.previous_started_event_id, t.started_event_id))
        else {
            return json!({});
        };
        let wfe = &self.workflow_executions[wfe_idx];
        let events: Vec<Value> = if reverse_order {
            wfe.events.iter().rev().map(HistoryEvent::to_dict).collect()
        } else {
            wfe.events.iter().map(HistoryEvent::to_dict).collect()
        };
        let mut hsh = json!({
            "events": events,
            "taskToken": token,
            "workflowExecution": wfe.to_short_dict(),
            "workflowType": wfe.wtype.to_short_dict(),
        });
        if let Some(prev) = previous {
            hsh["previousStartedEventId"] = json!(prev);
        }
        if let Some(started) = started {
            hsh["startedEventId"] = json!(started);
        }
        hsh
    }

    /// Find the earliest due timeout among execution / decision / activity
    /// timeouts (moto's `_process_timeouts` candidate collection).
    fn first_timeout_candidate(&self) -> Option<TimeoutCandidate> {
        let now = unix_now();
        let mut best: Option<TimeoutCandidate> = None;
        let consider = |cand: Option<(f64, TimeoutObj)>, best: &mut Option<TimeoutCandidate>| {
            if let Some((timestamp, object)) = cand {
                if best.as_ref().is_none_or(|b| timestamp < b.timestamp) {
                    *best = Some(TimeoutCandidate { object, timestamp });
                }
            }
        };

        for (i, w) in self.workflow_executions.iter().enumerate() {
            let due = w.first_timeout_ts().filter(|ts| now >= *ts);
            consider(
                due.map(|ts| (ts, TimeoutObj::WorkflowExecution(i))),
                &mut best,
            );
        }

        for tasks in self.decision_task_lists.values() {
            for t in tasks {
                let w = match self.workflow_executions.get(t.wfe_idx) {
                    Some(w) => w,
                    None => continue,
                };
                let due = decision_first_timeout(t, w, now);
                consider(
                    due.map(|ts| {
                        (
                            ts,
                            TimeoutObj::DecisionTask {
                                wfe: t.wfe_idx,
                                token: t.task_token.clone(),
                            },
                        )
                    }),
                    &mut best,
                );
            }
        }

        for tasks in self.activity_task_lists.values() {
            for t in tasks {
                let w = match self.workflow_executions.get(t.wfe_idx) {
                    Some(w) => w,
                    None => continue,
                };
                let due = activity_first_timeout(t, w, now);
                consider(
                    due.map(|ts| {
                        (
                            ts,
                            TimeoutObj::ActivityTask {
                                wfe: t.wfe_idx,
                                token: t.task_token.clone(),
                            },
                        )
                    }),
                    &mut best,
                );
            }
        }
        best
    }

    /// moto's `_process_timeouts`: process the earliest timeout, then repeat
    /// until nothing is due.
    fn process_timeouts(&mut self) {
        loop {
            let Some(c) = self.first_timeout_candidate() else {
                return;
            };
            match c.object {
                TimeoutObj::WorkflowExecution(i) => {
                    self.wfe_mut(i).timeout(c.timestamp);
                }
                TimeoutObj::DecisionTask { wfe, token } => {
                    self.timeout_decision_task(wfe, &token, c.timestamp);
                    self.schedule_decision_task(wfe);
                }
                TimeoutObj::ActivityTask { wfe, token } => {
                    self.timeout_activity_task(wfe, &token, c.timestamp);
                    self.schedule_decision_task(wfe);
                }
            }
        }
    }

    fn timeout_decision_task(&mut self, wfe_idx: usize, token: &str, timestamp: f64) {
        let (scheduled, started) = match self.decision_task_mut(token) {
            Some(t) => (t.scheduled_event_id, t.started_event_id),
            None => return,
        };
        if let Some(t) = self.decision_task_mut(token) {
            t.state = TIMED_OUT.to_string();
            t.timeout_type = Some("START_TO_CLOSE".to_string());
        }
        self.wfe_mut(wfe_idx).add_timed_event(
            timestamp,
            "DecisionTaskTimedOut",
            vec![
                ("scheduledEventId", json!(scheduled)),
                ("startedEventId", json!(started)),
                ("timeoutType", json!("START_TO_CLOSE")),
            ],
        );
    }

    fn timeout_activity_task(&mut self, wfe_idx: usize, token: &str, timestamp: f64) {
        let (scheduled, started, details) = match self.activity_task_mut(token) {
            Some(t) => (t.scheduled_event_id, t.started_event_id, t.details.clone()),
            None => return,
        };
        if let Some(t) = self.activity_task_mut(token) {
            t.state = TIMED_OUT.to_string();
            t.timeout_type = Some("HEARTBEAT".to_string());
        }
        self.wfe_mut(wfe_idx).add_timed_event(
            timestamp,
            "ActivityTaskTimedOut",
            vec![
                ("details", json!(details)),
                ("scheduledEventId", json!(scheduled)),
                ("startedEventId", json!(started)),
                ("timeoutType", json!("HEARTBEAT")),
            ],
        );
    }

    // -- workflow execution lifecycle ---------------------------------------

    fn start_wfe(&mut self, wfe_idx: usize) {
        let (child, esct, list, tsct, input) = {
            let w = &self.workflow_executions[wfe_idx];
            (
                w.child_policy.clone(),
                w.execution_start_to_close_timeout.clone(),
                w.task_list.clone(),
                w.task_start_to_close_timeout.clone(),
                w.input.clone(),
            )
        };
        let attrs = vec![
            ("childPolicy", json!(child)),
            ("executionStartToCloseTimeout", json!(esct)),
            ("taskList", json!({ "name": list })),
            ("taskStartToCloseTimeout", json!(tsct)),
            (
                "workflowType",
                json!({ "name": self.workflow_executions[wfe_idx].wtype.name, "version": self.workflow_executions[wfe_idx].wtype.version }),
            ),
            ("input", json!(input)),
        ];
        self.workflow_executions[wfe_idx].start_timestamp = Some(unix_now());
        self.wfe_mut(wfe_idx)
            .add_event("WorkflowExecutionStarted", attrs);
        self.schedule_decision_task(wfe_idx);
    }
}

// ---------------------------------------------------------------------------
// Timeouts (moto's Timeout / first_timeout computations)
// ---------------------------------------------------------------------------

struct TimeoutCandidate {
    object: TimeoutObj,
    timestamp: f64,
}

enum TimeoutObj {
    WorkflowExecution(usize),
    DecisionTask { wfe: usize, token: String },
    ActivityTask { wfe: usize, token: String },
}

/// Decision task deadline: started + taskStartToClose (moto's DecisionTask
/// `first_timeout`; its "NONE" handling is a TODO in moto — a non-numeric
/// timeout simply never fires here instead of crashing).
fn decision_first_timeout(t: &DecisionTask, w: &WorkflowExecution, now: f64) -> Option<f64> {
    if t.state != STARTED || !w.open() {
        return None;
    }
    let started_ts = t.started_timestamp?;
    let secs = w.task_start_to_close_timeout.parse::<f64>().ok()?;
    let deadline = started_ts + secs;
    (now >= deadline).then_some(deadline)
}

/// Activity heartbeat deadline (moto's ActivityTask `first_timeout`).
fn activity_first_timeout(t: &ActivityTask, w: &WorkflowExecution, now: f64) -> Option<f64> {
    if !t.open() || !w.open() {
        return None;
    }
    if t.timeouts.heartbeat == "NONE" {
        return None;
    }
    let secs = t.timeouts.heartbeat.parse::<f64>().ok()?;
    let deadline = t.last_heartbeat_timestamp + secs;
    (now >= deadline).then_some(deadline)
}

impl WorkflowExecution {
    /// moto's HistoryEvent truthiness filter applies: falsy kwargs drop.
    fn add_event(&mut self, event_type: &str, attrs: Vec<(&'static str, Value)>) -> i64 {
        let event_id = self.events.iter().map(|e| e.event_id).max().unwrap_or(0) + 1;
        let mut attributes = Map::new();
        for (key, value) in attrs {
            if truthy(&value) {
                attributes.insert(key.to_string(), value);
            }
        }
        self.events.push(HistoryEvent {
            event_id,
            event_type: event_type.to_string(),
            event_timestamp: unix_now(),
            attributes,
        });
        event_id
    }

    /// Timeout events carry an explicit event_timestamp in moto.
    fn add_timed_event(
        &mut self,
        timestamp: f64,
        event_type: &str,
        attrs: Vec<(&'static str, Value)>,
    ) -> i64 {
        let event_id = self.events.iter().map(|e| e.event_id).max().unwrap_or(0) + 1;
        let mut attributes = Map::new();
        for (key, value) in attrs {
            if truthy(&value) {
                attributes.insert(key.to_string(), value);
            }
        }
        self.events.push(HistoryEvent {
            event_id,
            event_type: event_type.to_string(),
            event_timestamp: timestamp,
            attributes,
        });
        event_id
    }

    /// moto's `WorkflowExecution.complete`.
    fn complete_wfe(&mut self, event_id: i64, result: Option<&str>) {
        self.execution_status = CLOSED.to_string();
        self.close_status = Some("COMPLETED".to_string());
        self.close_timestamp = Some(unix_now());
        self.add_event(
            "WorkflowExecutionCompleted",
            vec![
                ("decisionTaskCompletedEventId", json!(event_id)),
                ("result", json!(result)),
            ],
        );
    }

    /// moto's `WorkflowExecution.fail`.
    fn fail_wfe(&mut self, event_id: i64, details: &Value, reason: Option<&str>) {
        self.execution_status = CLOSED.to_string();
        self.close_status = Some("FAILED".to_string());
        self.close_timestamp = Some(unix_now());
        self.add_event(
            "WorkflowExecutionFailed",
            vec![
                ("decisionTaskCompletedEventId", json!(event_id)),
                ("details", details.clone()),
                ("reason", json!(reason)),
            ],
        );
    }

    /// moto's `WorkflowExecution.cancel` (the CancelWorkflowExecution decision).
    fn cancel_wfe(&mut self, event_id: i64, details: &Value) {
        self.cancel_requested = true;
        // Can only cancel when this is the only pending decision task.
        if self.open_counts.open_decision_tasks != 1 {
            self.add_event(
                "CancelWorkflowExecutionFailed",
                vec![
                    ("decisionTaskCompletedEventId", json!(event_id)),
                    ("cause", json!("UNHANDLED_DECISION")),
                ],
            );
            return;
        }
        self.execution_status = CLOSED.to_string();
        self.close_status = Some("CANCELED".to_string());
        self.close_timestamp = Some(unix_now());
        self.add_event(
            "WorkflowExecutionCanceled",
            vec![
                ("decisionTaskCompletedEventId", json!(event_id)),
                ("details", details.clone()),
            ],
        );
    }

    /// moto's `WorkflowExecution.terminate` — note close_timestamp stays
    /// unset (moto never sets it on the terminate path).
    fn terminate_wfe(&mut self, child_policy: Option<&str>, details: &Value, reason: Option<&str>) {
        let child = child_policy
            .filter(|s| !s.is_empty())
            .map(String::from)
            .unwrap_or_else(|| self.child_policy.clone());
        self.add_event(
            "WorkflowExecutionTerminated",
            vec![
                ("cause", json!("OPERATOR_INITIATED")),
                ("childPolicy", json!(child)),
                ("details", details.clone()),
                ("reason", json!(reason)),
            ],
        );
        self.execution_status = CLOSED.to_string();
        self.close_status = Some("TERMINATED".to_string());
        self.close_cause = Some("OPERATOR_INITIATED".to_string());
    }

    /// moto's `WorkflowExecution.signal`.
    fn signal_wfe(&mut self, signal_name: &str, input: Option<&str>) {
        self.add_event(
            "WorkflowExecutionSignaled",
            vec![("signalName", json!(signal_name)), ("input", json!(input))],
        );
    }

    /// moto's `WorkflowExecution.request_cancel` (RequestCancelWorkflowExecution op).
    fn request_cancel_wfe(&mut self) {
        self.cancel_requested = true;
        self.add_event(
            "WorkflowExecutionCancelRequested",
            vec![("cause", json!("UNKNOWN"))],
        );
    }
}

// ---------------------------------------------------------------------------
// Domain-level business logic (moto's WorkflowExecution + backend methods)
// ---------------------------------------------------------------------------

impl Domain {
    /// moto's `WorkflowExecution._schedule_decision_task` + poll start.
    fn poll_decision_task(
        &mut self,
        task_list: &str,
        identity: Option<&str>,
        reverse_order: bool,
    ) -> Value {
        let task_list = task_list.to_string();
        let mut candidates: Vec<(u64, String, usize)> = Vec::new();
        for (i, w) in self.workflow_executions.iter().enumerate() {
            if w.task_list != task_list || !w.open() {
                continue;
            }
            let mut found_started = false;
            for tasks in self.decision_task_lists.values() {
                for t in tasks {
                    if t.wfe_idx == i {
                        match t.state.as_str() {
                            STARTED => {
                                found_started = true;
                                break;
                            }
                            SCHEDULED => candidates.push((t.scheduled_at, t.task_token.clone(), i)),
                            _ => {}
                        }
                    }
                }
                if found_started {
                    break;
                }
            }
        }
        match candidates.iter().min_by_key(|(seq, _, _)| *seq) {
            Some((_, token, wfe_idx)) => {
                let token = token.clone();
                let wfe_idx = *wfe_idx;
                self.start_decision_task(wfe_idx, &token, identity);
                self.decision_response(&token, reverse_order)
            }
            // moto sleeps 1s here; we answer the empty poll immediately.
            None => json!({ "previousStartedEventId": 0, "startedEventId": 0 }),
        }
    }

    /// moto's `WorkflowExecution.start_activity_task` + poll response.
    fn poll_activity_task(&mut self, task_list: &str, identity: Option<&str>) -> Value {
        let task_list = task_list.to_string();
        let mut candidates: Vec<(u64, String, usize)> = Vec::new();
        for (list_name, tasks) in &self.activity_task_lists {
            if list_name == &task_list {
                for t in tasks {
                    if t.state == SCHEDULED {
                        candidates.push((t.scheduled_at, t.task_token.clone(), t.wfe_idx));
                    }
                }
            }
        }
        let Some((_, token, wfe_idx)) = candidates.iter().min_by_key(|(seq, _, _)| *seq) else {
            return json!({ "startedEventId": 0 });
        };
        let token = token.clone();
        let wfe_idx = *wfe_idx;
        let scheduled_event_id = match self.activity_task_mut(&token) {
            Some(t) => t.scheduled_event_id,
            None => return json!({ "startedEventId": 0 }),
        };
        let evt_id = self.wfe_mut(wfe_idx).add_event(
            "ActivityTaskStarted",
            vec![
                ("scheduledEventId", json!(scheduled_event_id)),
                ("identity", json!(identity)),
            ],
        );
        if let Some(t) = self.activity_task_mut(&token) {
            t.state = STARTED.to_string();
            t.started_event_id = Some(evt_id);
        }
        let wfe = &self.workflow_executions[wfe_idx];
        match self
            .activity_task_lists
            .values()
            .flat_map(|tasks| tasks.iter())
            .find(|t| t.task_token == token)
        {
            Some(t) => t.to_full_dict(wfe),
            None => json!({ "startedEventId": 0 }),
        }
    }

    /// moto's `WorkflowExecution.complete_decision_task`.
    fn complete_decision_task(
        &mut self,
        wfe_idx: usize,
        token: &str,
        decisions: Vec<Value>,
        context: Option<&str>,
    ) -> Result<(), Response> {
        validate_decisions(&decisions)?;
        let (scheduled_event_id, started_event_id) = match self.decision_task_mut(token) {
            Some(t) => (t.scheduled_event_id, t.started_event_id),
            None => return Err(validation_exception("Invalid token")),
        };
        let evt_id = self.wfe_mut(wfe_idx).add_event(
            "DecisionTaskCompleted",
            vec![
                ("scheduledEventId", json!(scheduled_event_id)),
                ("startedEventId", json!(started_event_id)),
                ("executionContext", json!(context)),
            ],
        );
        if let Some(t) = self.decision_task_mut(token) {
            t.state = COMPLETED.to_string();
        }
        self.workflow_executions[wfe_idx].should_schedule_decision_next = false;
        self.handle_decisions(wfe_idx, evt_id, &decisions)?;
        if self.workflow_executions[wfe_idx].should_schedule_decision_next {
            self.schedule_decision_task(wfe_idx);
        }
        self.wfe_mut(wfe_idx).latest_execution_context = context.map(String::from);
        Ok(())
    }

    /// moto's `WorkflowExecution.handle_decisions`. Decision types moto
    /// leaves as TODOs produce moto's unhandled-exception 500.
    fn handle_decisions(
        &mut self,
        wfe_idx: usize,
        event_id: i64,
        decisions: &[Value],
    ) -> Result<(), Response> {
        for decision in decisions {
            let decision_type = decision
                .get("decisionType")
                .and_then(Value::as_str)
                .unwrap_or("");
            let attrs_key = format!("{}DecisionAttributes", decapitalize(decision_type));
            let attributes = decision
                .get(&attrs_key)
                .cloned()
                .unwrap_or_else(|| json!({}));
            match decision_type {
                "CompleteWorkflowExecution" => {
                    let result = attributes.get("result").and_then(Value::as_str);
                    self.wfe_mut(wfe_idx).complete_wfe(event_id, result);
                }
                "FailWorkflowExecution" => {
                    let reason = attributes.get("reason").and_then(Value::as_str);
                    let details = attributes.get("details").cloned().unwrap_or(Value::Null);
                    self.wfe_mut(wfe_idx).fail_wfe(event_id, &details, reason);
                }
                "CancelWorkflowExecution" => {
                    let details = attributes.get("details").cloned().unwrap_or(Value::Null);
                    self.wfe_mut(wfe_idx).cancel_wfe(event_id, &details);
                }
                "ScheduleActivityTask" => {
                    self.schedule_activity_task(wfe_idx, event_id, &attributes)?;
                }
                "RecordMarker" => self.record_marker(wfe_idx, event_id, &attributes),
                "StartTimer" => self.start_timer(wfe_idx, event_id, &attributes),
                "CancelTimer" => {
                    let timer_id = attributes
                        .get("timerId")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    self.cancel_timer(wfe_idx, event_id, timer_id);
                }
                other => {
                    return Err(swf_error(
                        500,
                        "com.amazon.coral.service#InternalFailure",
                        &format!("Cannot handle decision: {other}"),
                    ));
                }
            }
        }
        self.wfe_mut(wfe_idx).open_counts.open_decision_tasks -= 1;
        Ok(())
    }

    /// moto's `WorkflowExecution.record_marker`.
    fn record_marker(&mut self, wfe_idx: usize, event_id: i64, attributes: &Value) {
        let marker_name = attributes
            .get("markerName")
            .and_then(Value::as_str)
            .unwrap_or("");
        let details = attributes.get("details").cloned().unwrap_or(Value::Null);
        self.wfe_mut(wfe_idx).add_event(
            "MarkerRecorded",
            vec![
                ("decisionTaskCompletedEventId", json!(event_id)),
                ("details", details),
                ("markerName", json!(marker_name)),
            ],
        );
    }

    /// moto's `WorkflowExecution.start_timer`. Timers never fire
    /// asynchronously in this port (see module docs).
    fn start_timer(&mut self, wfe_idx: usize, event_id: i64, attributes: &Value) {
        let timer_id = attributes
            .get("timerId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let alive = self.workflow_executions[wfe_idx]
            .timers
            .get(&timer_id)
            .map(|t| t.alive)
            .unwrap_or(false);
        if alive {
            self.wfe_mut(wfe_idx).add_event(
                "StartTimerFailed",
                vec![
                    ("cause", json!("TIMER_ID_ALREADY_IN_USE")),
                    ("decisionTaskCompletedEventId", json!(event_id)),
                    ("timerId", json!(timer_id)),
                ],
            );
            return;
        }
        let started_event_id = self.wfe_mut(wfe_idx).add_event(
            "TimerStarted",
            vec![
                ("control", json!(attributes.get("control"))),
                ("decisionTaskCompletedEventId", json!(event_id)),
                (
                    "startToFireTimeout",
                    json!(attributes.get("startToFireTimeout")),
                ),
                ("timerId", json!(timer_id)),
            ],
        );
        self.workflow_executions[wfe_idx].timers.insert(
            timer_id,
            TimerState {
                started_event_id,
                alive: true,
            },
        );
    }

    /// moto's `WorkflowExecution.cancel_timer`.
    fn cancel_timer(&mut self, wfe_idx: usize, event_id: i64, timer_id: &str) {
        let known_alive = self.workflow_executions[wfe_idx]
            .timers
            .get(timer_id)
            .map(|t| t.alive)
            .unwrap_or(false);
        if !known_alive {
            self.wfe_mut(wfe_idx).add_event(
                "CancelTimerFailed",
                vec![
                    ("cause", json!("TIMER_ID_UNKNOWN")),
                    ("decisionTaskCompletedEventId", json!(event_id)),
                ],
            );
            return;
        }
        let started_event_id = self.workflow_executions[wfe_idx]
            .timers
            .remove(timer_id)
            .map(|t| t.started_event_id);
        self.wfe_mut(wfe_idx).add_event(
            "TimerCancelled",
            vec![
                ("decisionTaskCompletedEventId", json!(event_id)),
                ("startedEventId", json!(started_event_id)),
                ("timerId", json!(timer_id)),
            ],
        );
    }

    /// Shared failure event for ScheduleActivityTask decisions (moto's
    /// `fail_schedule_activity_task`).
    fn schedule_activity_failed(
        &mut self,
        wfe_idx: usize,
        event_id: i64,
        activity_id: &str,
        type_name: &str,
        type_version: &str,
        cause: &str,
    ) {
        self.wfe_mut(wfe_idx).add_event(
            "ScheduleActivityTaskFailed",
            vec![
                ("activityId", json!(activity_id)),
                (
                    "activityType",
                    json!({ "name": type_name, "version": type_version }),
                ),
                ("cause", json!(cause)),
                ("decisionTaskCompletedEventId", json!(event_id)),
            ],
        );
        self.workflow_executions[wfe_idx].should_schedule_decision_next = true;
    }

    /// moto's `WorkflowExecution.schedule_activity_task`, including its
    /// default-fallback failure modes and activity counters.
    fn schedule_activity_task(
        &mut self,
        wfe_idx: usize,
        event_id: i64,
        attributes: &Value,
    ) -> Result<(), Response> {
        let activity_id = attributes
            .get("activityId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let a_type = &attributes["activityType"];
        let type_name = a_type.get("name").and_then(Value::as_str).unwrap_or("");
        let type_version = a_type.get("version").and_then(Value::as_str).unwrap_or("");

        let Some(atype) = self.find_type(ACTIVITY, type_name, type_version) else {
            self.schedule_activity_failed(
                wfe_idx,
                event_id,
                &activity_id,
                type_name,
                type_version,
                "ACTIVITY_TYPE_DOES_NOT_EXIST",
            );
            return Ok(());
        };
        if atype.status == DEPRECATED {
            self.schedule_activity_failed(
                wfe_idx,
                event_id,
                &activity_id,
                type_name,
                type_version,
                "ACTIVITY_TYPE_DEPRECATED",
            );
            return Ok(());
        }
        let id_in_use = self
            .activity_task_lists
            .values()
            .flat_map(|tasks| tasks.iter())
            .any(|t| t.wfe_idx == wfe_idx && t.activity_id == activity_id);
        if id_in_use {
            self.schedule_activity_failed(
                wfe_idx,
                event_id,
                &activity_id,
                type_name,
                type_version,
                "ACTIVITY_ID_ALREADY_IN_USE",
            );
            return Ok(());
        }
        // Copy the type slot data out so the borrow ends before mutations.
        let type_task_list = atype.task_list.clone();
        let type_schedule_to_start = atype.default_task_schedule_to_start_timeout.clone();
        let type_schedule_to_close = atype.default_task_schedule_to_close_timeout.clone();
        let type_start_to_close = atype.default_task_start_to_close_timeout.clone();
        let type_heartbeat = atype.default_task_heartbeat_timeout.clone();

        // Task list from the decision or the activity-type default.
        let task_list = attributes
            .get("taskList")
            .and_then(|tl| tl.get("name"))
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(String::from)
            .or_else(|| type_task_list.clone())
            .unwrap_or_default();
        if task_list.is_empty() {
            self.schedule_activity_failed(
                wfe_idx,
                event_id,
                &activity_id,
                type_name,
                type_version,
                "DEFAULT_TASK_LIST_UNDEFINED",
            );
            return Ok(());
        }

        // Timeouts from the decision or the activity-type defaults, checked
        // in moto's order (scheduleToStart, scheduleToClose, startToClose,
        // heartbeat) so the first miss produces its failure event.
        let fallback = |attr: &str, default: &Option<String>| -> Option<String> {
            attributes
                .get(attr)
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .or_else(|| default.clone().filter(|d| !d.is_empty()))
        };
        let pairs = [
            (
                "scheduleToStartTimeout",
                fallback("scheduleToStartTimeout", &type_schedule_to_start),
            ),
            (
                "scheduleToCloseTimeout",
                fallback("scheduleToCloseTimeout", &type_schedule_to_close),
            ),
            (
                "startToCloseTimeout",
                fallback("startToCloseTimeout", &type_start_to_close),
            ),
            (
                "heartbeatTimeout",
                fallback("heartbeatTimeout", &type_heartbeat),
            ),
        ];
        let mut timeouts: Vec<(String, String)> = Vec::with_capacity(4);
        for (name, value) in pairs {
            match value {
                Some(v) => timeouts.push((name.to_string(), v)),
                None => {
                    let cause = format!(
                        "DEFAULT_{}_UNDEFINED",
                        format!("default_task_{}", camel_to_underscores(name))
                            .replace("default_task_", "default_")
                            .to_uppercase()
                    );
                    self.schedule_activity_failed(
                        wfe_idx,
                        event_id,
                        &activity_id,
                        type_name,
                        type_version,
                        &cause,
                    );
                    return Ok(());
                }
            }
        }
        let get_timeout = |name: &str| -> String {
            timeouts
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, v)| v.clone())
                .unwrap_or_default()
        };

        // Success: event, task, counters (moto's ActivityTaskScheduled block).
        let evt_id = self.wfe_mut(wfe_idx).add_event(
            "ActivityTaskScheduled",
            vec![
                ("activityId", json!(activity_id)),
                (
                    "activityType",
                    json!({ "name": type_name, "version": type_version }),
                ),
                ("control", json!(attributes.get("control"))),
                ("decisionTaskCompletedEventId", json!(event_id)),
                (
                    "heartbeatTimeout",
                    json!(attributes.get("heartbeatTimeout")),
                ),
                ("input", json!(attributes.get("input"))),
                (
                    "scheduleToCloseTimeout",
                    json!(attributes.get("scheduleToCloseTimeout")),
                ),
                (
                    "scheduleToStartTimeout",
                    json!(attributes.get("scheduleToStartTimeout")),
                ),
                (
                    "startToCloseTimeout",
                    json!(attributes.get("startToCloseTimeout")),
                ),
                ("taskList", json!({ "name": task_list })),
                ("taskPriority", json!(attributes.get("taskPriority"))),
            ],
        );
        let task = ActivityTask {
            wfe_idx,
            activity_id: activity_id.clone(),
            activity_type_name: type_name.to_string(),
            activity_type_version: type_version.to_string(),
            details: None,
            input: attributes
                .get("input")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(String::from),
            last_heartbeat_timestamp: unix_now(),
            scheduled_event_id: evt_id,
            started_event_id: None,
            state: SCHEDULED.to_string(),
            task_token: Uuid::new_v4().to_string(),
            timeouts: ActivityTimeouts {
                heartbeat: get_timeout("heartbeatTimeout"),
            },
            timeout_type: None,
            scheduled_at: self.next_seq(),
        };
        self.activity_task_lists
            .entry(task_list)
            .or_default()
            .push(task);
        self.wfe_mut(wfe_idx).open_counts.open_activity_tasks += 1;
        self.wfe_mut(wfe_idx).latest_activity_task_timestamp = Some(unix_now());
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Wire helpers (moto's JsonRESTError shapes)
// ---------------------------------------------------------------------------

/// moto's JsonRESTError: full fault type in the body `__type`, the relative
/// (post-`#`) name in the `X-Amzn-ErrorType` header.
fn swf_error(status: u16, full_type: &str, message: &str) -> Response {
    let mut resp =
        Response::json_status(status, &json!({ "__type": full_type, "message": message }));
    let relative = full_type.rsplit('#').next().unwrap_or(full_type);
    resp.headers
        .push(("x-amzn-ErrorType".to_string(), format!("{relative}:")));
    resp
}

fn unknown_resource(resource_type: &str, resource_name: Option<&str>) -> Response {
    let message = match resource_name {
        Some(name) => format!("Unknown {resource_type}: {name}"),
        None => format!("Unknown {resource_type}"),
    };
    swf_error(
        400,
        "com.amazonaws.swf.base.model#UnknownResourceFault",
        &message,
    )
}

/// moto's get_workflow_execution raise args differ with/without run_id.
fn unknown_execution(workflow_id: &str, run_id: Option<&str>) -> Response {
    match run_id {
        Some(run_id) => unknown_resource(
            "execution",
            Some(&format!(
                "WorkflowExecution=[workflowId={workflow_id}, runId={run_id}]"
            )),
        ),
        None => unknown_resource(&format!("execution, workflowId = {workflow_id}"), None),
    }
}

fn domain_already_exists(name: &str) -> Response {
    swf_error(
        400,
        "com.amazonaws.swf.base.model#DomainAlreadyExistsFault",
        name,
    )
}

fn domain_deprecated_fault(name: &str) -> Response {
    swf_error(
        400,
        "com.amazonaws.swf.base.model#DomainDeprecatedFault",
        name,
    )
}

fn type_already_exists(upper_kind: &str, name: &str, version: &str) -> Response {
    swf_error(
        400,
        "com.amazonaws.swf.base.model#TypeAlreadyExistsFault",
        &format!("{upper_kind}Type=[name={name}, version={version}]"),
    )
}

fn type_deprecated_fault(upper_kind: &str, name: &str, version: &str) -> Response {
    swf_error(
        400,
        "com.amazonaws.swf.base.model#TypeDeprecatedFault",
        &format!("{upper_kind}Type=[name={name}, version={version}]"),
    )
}

fn wfe_already_started() -> Response {
    swf_error(
        400,
        "com.amazonaws.swf.base.model#WorkflowExecutionAlreadyStartedFault",
        "Already Started",
    )
}

fn default_undefined(camel_key: &str) -> Response {
    swf_error(
        400,
        "com.amazonaws.swf.base.model#DefaultUndefinedFault",
        camel_key,
    )
}

fn validation_exception(message: &str) -> Response {
    swf_error(
        400,
        "com.amazon.coral.validate#ValidationException",
        message,
    )
}

fn serialization_exception(value: &Value) -> Response {
    let message = format!(
        "class java.lang.Foo can not be converted to an String  (not a real SWF exception ; happened on: {value})"
    );
    swf_error(
        400,
        "com.amazonaws.swf.base.model#SerializationException",
        &message,
    )
}

/// moto's SWFDecisionValidationException formatting.
fn decision_validation_exception(messages: Vec<String>) -> Response {
    let count = messages.len();
    let prefix = if count < 2 {
        format!("{count} validation error detected: ")
    } else {
        format!("{count} validation errors detected: ")
    };
    swf_error(
        400,
        "com.amazon.coral.validate#ValidationException",
        &format!("{}{}", prefix, messages.join("; ")),
    )
}

fn internal_failure(detail: &str) -> Response {
    swf_error(500, "com.amazon.coral.service#InternalFailure", detail)
}

/// moto's `WorkflowExecution.validate_decisions`: close-decision ordering,
/// required decision attributes and unknown decision types; any problem
/// aborts the whole decision set *before* history events are written.
fn validate_decisions(decisions: &[Value]) -> Result<(), Response> {
    if !decisions.is_empty() {
        for d in decisions.iter().take(decisions.len() - 1) {
            let decision_type = d.get("decisionType").and_then(Value::as_str).unwrap_or("");
            if matches!(
                decision_type,
                "CompleteWorkflowExecution" | "FailWorkflowExecution" | "CancelWorkflowExecution"
            ) {
                return Err(validation_exception("Close must be last decision in list"));
            }
        }
    }
    let mut problems: Vec<String> = Vec::new();
    for (i, d) in decisions.iter().enumerate() {
        let n = i + 1;
        let decision_type = d.get("decisionType").and_then(Value::as_str).unwrap_or("");
        let mut attrs_to_check: Vec<String> = d
            .as_object()
            .map(|o| {
                o.keys()
                    .filter(|k| k.ends_with("DecisionAttributes"))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        if KNOWN_DECISION_TYPES.contains(&decision_type) {
            attrs_to_check.push(format!("{}DecisionAttributes", decapitalize(decision_type)));
        }
        for attr in attrs_to_check {
            let value = d.get(&attr).cloned().unwrap_or_else(|| json!({}));
            for key in required_decision_fields(&attr) {
                if !value.get(key).map(truthy).unwrap_or(false) {
                    problems.push(format!(
                        "Value null at 'decisions.{n}.member.{attr}.{key}' failed to satisfy constraint: Member must not be null"
                    ));
                }
            }
        }
        if !KNOWN_DECISION_TYPES.contains(&decision_type) {
            problems.push(format!(
                "Value '{decision_type}' at 'decisions.{n}.member.decisionType' failed to satisfy constraint: Member must satisfy enum value set: [{}]",
                KNOWN_DECISION_TYPES.join(", ")
            ));
        }
    }
    if problems.is_empty() {
        Ok(())
    } else {
        Err(decision_validation_exception(problems))
    }
}

fn capitalize(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

/// moto's `utils.camelcase_to_underscores` for ASCII identifiers.
fn camel_to_underscores(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 8);
    for ch in name.chars() {
        if ch.is_ascii_uppercase() {
            out.push('_');
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

// --- body param readers -----------------------------------------------------

fn body_str<'a>(v: &'a Value, key: &str) -> &'a str {
    v.get(key).and_then(Value::as_str).unwrap_or("")
}

/// moto's `.get(key)`: absent, null or empty all read as absent.
fn opt_str<'a>(v: &'a Value, key: &str) -> Option<&'a str> {
    v.get(key).and_then(Value::as_str).filter(|s| !s.is_empty())
}

fn tag_from_filter(v: Option<&Value>) -> Option<&str> {
    v.and_then(|f| f.get("tag"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
}

/// moto's `_check_exclusivity`: at most one of the keys may be present.
fn check_exclusivity(body: &Value, keys: &[&str]) -> Result<(), Response> {
    let present = keys
        .iter()
        .filter(|k| body.get(**k).is_some_and(|v| !v.is_null()))
        .count();
    if present <= 1 {
        return Ok(());
    }
    let message = if keys.len() == 2 {
        format!("Cannot specify both a {} and a {}", keys[0], keys[1])
    } else {
        let list = keys
            .iter()
            .map(|k| format!("'{k}'"))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "Cannot specify more than one exclusive filters in the same query: dict_keys([{list}])"
        )
    };
    Err(validation_exception(&message))
}

/// moto's `_check_float_or_int` for history age filters.
fn check_float_or_int(v: Option<&Value>) -> Result<(), Response> {
    match v {
        Some(Value::Number(_)) | None => Ok(()),
        Some(other) => Err(serialization_exception(other)),
    }
}

/// ListXWorkflowExecutions' maximumPageSize — default 1000, must be numeric
/// per moto's `_check_int`.
fn maximum_page_size(body: &Value) -> Result<i64, Response> {
    match body.get("maximumPageSize") {
        None => Ok(1000),
        Some(Value::Number(n)) => Ok(n.as_i64().unwrap_or(1000)),
        Some(other) => Err(serialization_exception(other)),
    }
}

// ---------------------------------------------------------------------------
// More domain helpers used by handlers
// ---------------------------------------------------------------------------

impl Domain {
    fn has_decision_token(&self, token: &str) -> bool {
        self.decision_task_lists
            .values()
            .any(|tasks| tasks.iter().any(|t| t.task_token == token))
    }

    fn has_activity_token(&self, token: &str) -> bool {
        self.activity_task_lists
            .values()
            .any(|tasks| tasks.iter().any(|t| t.task_token == token))
    }

    /// (wfe_idx, state, scheduled_event_id) for the respond-time checks.
    fn decision_task_info(&self, token: &str) -> Option<(usize, String, i64)> {
        self.decision_task_lists
            .values()
            .flat_map(|tasks| tasks.iter())
            .find(|t| t.task_token == token)
            .map(|t| (t.wfe_idx, t.state.clone(), t.scheduled_event_id))
    }

    fn activity_task_info(&self, token: &str) -> Option<(usize, String, i64)> {
        self.activity_task_lists
            .values()
            .flat_map(|tasks| tasks.iter())
            .find(|t| t.task_token == token)
            .map(|t| (t.wfe_idx, t.state.clone(), t.scheduled_event_id))
    }

    /// moto's `WorkflowExecution.complete_activity_task`.
    fn complete_activity_task(&mut self, token: &str, result: Option<&str>) {
        let Some((wfe_idx, scheduled, started)) = self.activity_event_source(token) else {
            return;
        };
        self.wfe_mut(wfe_idx).add_event(
            "ActivityTaskCompleted",
            vec![
                ("scheduledEventId", json!(scheduled)),
                ("startedEventId", json!(started)),
                ("result", json!(result)),
            ],
        );
        if let Some(t) = self.activity_task_mut(token) {
            t.state = COMPLETED.to_string();
        }
        self.wfe_mut(wfe_idx).open_counts.open_activity_tasks -= 1;
        self.schedule_decision_task(wfe_idx);
    }

    /// moto's `WorkflowExecution.fail_activity_task`.
    fn fail_activity_task(&mut self, token: &str, reason: Option<&str>, details: &Value) {
        let Some((wfe_idx, scheduled, started)) = self.activity_event_source(token) else {
            return;
        };
        self.wfe_mut(wfe_idx).add_event(
            "ActivityTaskFailed",
            vec![
                ("scheduledEventId", json!(scheduled)),
                ("startedEventId", json!(started)),
                ("reason", json!(reason)),
                ("details", details.clone()),
            ],
        );
        if let Some(t) = self.activity_task_mut(token) {
            t.state = FAILED.to_string();
        }
        self.wfe_mut(wfe_idx).open_counts.open_activity_tasks -= 1;
        self.schedule_decision_task(wfe_idx);
    }

    /// moto's `WorkflowExecution.cancel_activity_task`.
    fn cancel_activity_task(&mut self, token: &str, details: &Value) {
        let Some((wfe_idx, scheduled, started)) = self.activity_event_source(token) else {
            return;
        };
        self.wfe_mut(wfe_idx).add_event(
            "ActivityTaskCanceled",
            vec![
                ("scheduledEventId", json!(scheduled)),
                ("startedEventId", json!(started)),
                ("details", details.clone()),
            ],
        );
        if let Some(t) = self.activity_task_mut(token) {
            t.state = CANCELED.to_string();
        }
        self.wfe_mut(wfe_idx).open_counts.open_activity_tasks -= 1;
        self.schedule_decision_task(wfe_idx);
    }

    /// moto's `ActivityTask.reset_heartbeat_clock` (+ optional details).
    fn record_heartbeat(&mut self, token: &str, details: Option<&str>) {
        if let Some(t) = self.activity_task_mut(token) {
            t.last_heartbeat_timestamp = unix_now();
            if let Some(d) = details {
                t.details = Some(d.to_string());
            }
        }
    }

    /// (wfe_idx, scheduled_event_id, started_event_id) helper for events.
    fn activity_event_source(&self, token: &str) -> Option<(usize, i64, Option<i64>)> {
        self.activity_task_lists
            .values()
            .flat_map(|tasks| tasks.iter())
            .find(|t| t.task_token == token)
            .map(|t| (t.wfe_idx, t.scheduled_event_id, t.started_event_id))
    }

    /// moto's Domain.tag_resource: update values for known keys, else append.
    fn tag_upsert(&mut self, tags: &[Value]) {
        for tag in tags {
            let Some(key) = tag.get("key").and_then(Value::as_str) else {
                continue;
            };
            let value = tag.get("value").and_then(Value::as_str).unwrap_or("");
            if let Some(existing) = self.tags.iter_mut().find(|(k, _)| k == key) {
                existing.1 = value.to_string();
            } else {
                self.tags.push((key.to_string(), value.to_string()));
            }
        }
    }

    fn untag(&mut self, tag_keys: &[&str]) {
        self.tags.retain(|(k, _)| !tag_keys.contains(&k.as_str()));
    }
}

impl SwfStore {
    fn process_timeouts_all(&mut self) {
        for i in 0..self.domains.len() {
            self.domains[i].process_timeouts();
        }
    }
}

/// Errors in the swf handlers are responses too — flatten either arm.
fn flatten(result: Result<Response, Response>) -> Response {
    match result {
        Ok(resp) | Err(resp) => resp,
    }
}

fn ok(body: Value) -> Response {
    Response::json(200, serde_json::to_vec(&body).unwrap_or_default())
}

/// moto's empty-string success body for JSON services.
fn ok_empty() -> Response {
    Response::json(200, Vec::new())
}

/// Run `f` against the named domain or raise moto's UnknownResourceFault.
fn with_domain(st: &mut SwfStore, name: &str, f: impl FnOnce(&mut Domain) -> Response) -> Response {
    match st.domains.iter().position(|d| d.name == name) {
        Some(i) => f(&mut st.domains[i]),
        None => unknown_resource("domain", Some(name)),
    }
}

fn is_true(v: Option<&Value>) -> bool {
    v.and_then(Value::as_bool).unwrap_or(false)
}

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "swf" {
        return None;
    }
    Some(with_store(req, |st| route(req, st)))
}

fn route(req: &Request, st: &mut SwfStore) -> Response {
    let body = parse_json_body(&req.body);
    match req.operation.as_str() {
        // --- domains --------------------------------------------------------
        "RegisterDomain" => register_domain(st, req.account, &req.region, &body),
        "DeprecateDomain" => with_domain(st, body_str(&body, "name"), |dm| {
            if dm.status == DEPRECATED {
                domain_deprecated_fault(&dm.name)
            } else {
                dm.status = DEPRECATED.to_string();
                ok_empty()
            }
        }),
        "UndeprecateDomain" => with_domain(st, body_str(&body, "name"), |dm| {
            if dm.status == REGISTERED {
                domain_already_exists(&dm.name)
            } else {
                dm.status = REGISTERED.to_string();
                ok_empty()
            }
        }),
        "DescribeDomain" => with_domain(st, body_str(&body, "name"), |dm| ok(dm.to_full_dict())),
        "ListDomains" => {
            let status = body_str(&body, "registrationStatus");
            let reverse = is_true(body.get("reverseOrder"));
            let mut doms: Vec<&Domain> = st.domains.iter().filter(|d| d.status == status).collect();
            doms.sort_by_key(|d| d.name.clone());
            if reverse {
                doms.reverse();
            }
            ok(json!({
                "domainInfos": doms.into_iter().map(Domain::to_short_dict).collect::<Vec<_>>()
            }))
        }
        // --- types ----------------------------------------------------------
        "RegisterActivityType" => register_type(st, &body, ACTIVITY),
        "RegisterWorkflowType" => register_type(st, &body, WORKFLOW),
        "DeprecateActivityType" => toggle_type(st, &body, ACTIVITY, false),
        "UndeprecateActivityType" => toggle_type(st, &body, ACTIVITY, true),
        "DeprecateWorkflowType" => toggle_type(st, &body, WORKFLOW, false),
        "UndeprecateWorkflowType" => toggle_type(st, &body, WORKFLOW, true),
        "DescribeActivityType" => describe_type(st, &body, ACTIVITY),
        "DescribeWorkflowType" => describe_type(st, &body, WORKFLOW),
        "DeleteActivityType" => delete_type(st, &body, ACTIVITY),
        "DeleteWorkflowType" => delete_type(st, &body, WORKFLOW),
        "ListActivityTypes" => list_types(st, &body, ACTIVITY),
        "ListWorkflowTypes" => list_types(st, &body, WORKFLOW),
        // --- workflow executions -------------------------------------------
        "StartWorkflowExecution" => flatten(start_workflow_execution(st, &body)),
        "DescribeWorkflowExecution" => describe_workflow_execution(st, &body),
        "GetWorkflowExecutionHistory" => execution_history(st, &body),
        "TerminateWorkflowExecution" => terminate_workflow_execution(st, &body),
        "SignalWorkflowExecution" => signal_workflow_execution(st, &body),
        "RequestCancelWorkflowExecution" => request_cancel_workflow_execution(st, &body),
        // --- decision tasks -------------------------------------------------
        "PollForDecisionTask" => poll_for_decision_task(st, &body),
        "CountPendingDecisionTasks" => {
            st.process_timeouts_all();
            count_pending_tasks(st, &body, false)
        }
        "RespondDecisionTaskCompleted" => respond_decision_task_completed(st, &body),
        // --- activity tasks -------------------------------------------------
        "PollForActivityTask" => poll_for_activity_task(st, &body),
        "CountPendingActivityTasks" => {
            st.process_timeouts_all();
            count_pending_tasks(st, &body, true)
        }
        "RecordActivityTaskHeartbeat" => record_activity_task_heartbeat(st, &body),
        "RespondActivityTaskCompleted" => {
            respond_activity_task(st, &body, RespondActivity::Completed)
        }
        "RespondActivityTaskFailed" => respond_activity_task(st, &body, RespondActivity::Failed),
        "RespondActivityTaskCanceled" => {
            respond_activity_task(st, &body, RespondActivity::Canceled)
        }
        // --- counts / lists -------------------------------------------------
        "CountClosedWorkflowExecutions" => flatten(count_workflow_executions(st, &body, false)),
        "CountOpenWorkflowExecutions" => flatten(count_workflow_executions(st, &body, true)),
        "ListClosedWorkflowExecutions" => flatten(list_workflow_executions(st, &body, false)),
        "ListOpenWorkflowExecutions" => flatten(list_workflow_executions(st, &body, true)),
        // --- tags ----------------------------------------------------------
        "TagResource" => tag_resource(st, &body),
        "UntagResource" => untag_resource(st, &body),
        "ListTagsForResource" => list_tags_for_resource(st, &body),
        other => crate::proto::json_error(
            501,
            "NotImplemented",
            &format!("The {other} action has not been implemented"),
        ),
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

fn register_domain(st: &mut SwfStore, account: u64, region: &str, body: &Value) -> Response {
    let name = body_str(body, "name").to_string();
    let retention = body_str(body, "workflowExecutionRetentionPeriodInDays").to_string();
    let description = opt_str(body, "description").map(String::from);
    if st.domains.iter().any(|d| d.name == name) {
        return domain_already_exists(&name);
    }
    st.domains.push(Domain::new(
        account,
        region,
        &name,
        &retention,
        description.as_deref(),
    ));
    ok_empty()
}

/// Register{Activity,Workflow}Type.
fn register_type(st: &mut SwfStore, body: &Value, kind: &'static str) -> Response {
    let domain_name = body_str(body, "domain").to_string();
    with_domain(st, &domain_name, |dm| {
        let name = body_str(body, "name").to_string();
        let version = body_str(body, "version").to_string();
        if dm.find_type(kind, &name, &version).is_some() {
            return type_already_exists(&capitalize(kind), &name, &version);
        }
        let mut t = SwfType::new(kind, &name, &version);
        t.description = opt_str(body, "description").map(String::from);
        t.task_list = body
            .get("defaultTaskList")
            .and_then(|tl| tl.get("name"))
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(String::from);
        if kind == ACTIVITY {
            t.default_task_heartbeat_timeout =
                opt_str(body, "defaultTaskHeartbeatTimeout").map(String::from);
            t.default_task_schedule_to_close_timeout =
                opt_str(body, "defaultTaskScheduleToCloseTimeout").map(String::from);
            t.default_task_schedule_to_start_timeout =
                opt_str(body, "defaultTaskScheduleToStartTimeout").map(String::from);
            t.default_task_start_to_close_timeout =
                opt_str(body, "defaultTaskStartToCloseTimeout").map(String::from);
        } else {
            t.default_child_policy = opt_str(body, "defaultChildPolicy").map(String::from);
            t.default_execution_start_to_close_timeout =
                opt_str(body, "defaultExecutionStartToCloseTimeout").map(String::from);
            t.default_task_start_to_close_timeout =
                opt_str(body, "defaultTaskStartToCloseTimeout").map(String::from);
            t.default_task_priority = opt_str(body, "defaultTaskPriority").map(String::from);
            t.default_lambda_role = opt_str(body, "defaultLambdaRole").map(String::from);
        }
        dm.add_type(t);
        ok_empty()
    })
}

/// Deprecate / Undeprecate{Activity,Workflow}Type.
fn toggle_type(st: &mut SwfStore, body: &Value, kind: &str, undeprecate: bool) -> Response {
    let domain_name = body_str(body, "domain").to_string();
    let type_args = body
        .get(format!("{}Type", kind))
        .cloned()
        .unwrap_or(json!({}));
    let name = body_str(&type_args, "name").to_string();
    let version = body_str(&type_args, "version").to_string();
    let upper = capitalize(kind);
    with_domain(st, &domain_name, |dm| {
        let status = dm
            .find_type(kind, &name, &version)
            .map(|t| t.status.clone());
        let unknown = unknown_resource(
            "type",
            Some(&format!("{upper}Type=[name={name}, version={version}]")),
        );
        match status {
            None => unknown,
            Some(s) => {
                if undeprecate {
                    if s == REGISTERED {
                        type_already_exists(&upper, &name, &version)
                    } else {
                        if let Some(t) = dm
                            .types
                            .get_mut(kind)
                            .and_then(|m| m.get_mut(&name))
                            .and_then(|m| m.get_mut(&version))
                        {
                            t.status = REGISTERED.to_string();
                        }
                        ok_empty()
                    }
                } else if s == DEPRECATED {
                    type_deprecated_fault(&upper, &name, &version)
                } else if let Some(t) = dm
                    .types
                    .get_mut(kind)
                    .and_then(|m| m.get_mut(&name))
                    .and_then(|m| m.get_mut(&version))
                {
                    t.status = DEPRECATED.to_string();
                    ok_empty()
                } else {
                    unreachable!("type vanished mid-call")
                }
            }
        }
    })
}

/// Describe{Activity,Workflow}Type.
fn describe_type(st: &mut SwfStore, body: &Value, kind: &str) -> Response {
    let domain_name = body_str(body, "domain").to_string();
    let type_args = body
        .get(format!("{}Type", kind))
        .cloned()
        .unwrap_or(json!({}));
    let name = body_str(&type_args, "name").to_string();
    let version = body_str(&type_args, "version").to_string();
    let upper = capitalize(kind);
    with_domain(st, &domain_name, |dm| {
        match dm.find_type(kind, &name, &version) {
            Some(t) => ok(t.to_full_dict()),
            None => unknown_resource(
                "type",
                Some(&format!("{upper}Type=[name={name}, version={version}]")),
            ),
        }
    })
}

/// Delete{Activity,Workflow}Type — only deprecated types may be deleted.
fn delete_type(st: &mut SwfStore, body: &Value, kind: &str) -> Response {
    let domain_name = body_str(body, "domain").to_string();
    let type_args = body
        .get(format!("{}Type", kind))
        .cloned()
        .unwrap_or(json!({}));
    let name = body_str(&type_args, "name").to_string();
    let version = body_str(&type_args, "version").to_string();
    let upper = capitalize(kind);
    with_domain(st, &domain_name, |dm| {
        let status = dm
            .find_type(kind, &name, &version)
            .map(|t| t.status.clone());
        let unknown = unknown_resource(
            "type",
            Some(&format!("{upper}Type=[name={name}, version={version}]")),
        );
        match status {
            None => unknown,
            Some(s) if s != DEPRECATED => validation_exception(&format!(
                "The {kind} type must be deprecated first: {upper}Type=[name={name}, version={version}]"
            )),
            Some(_) => {
                dm.remove_type(kind, &name, &version);
                ok_empty()
            }
        }
    })
}

/// List{Activity,Workflow}Types.
fn list_types(st: &mut SwfStore, body: &Value, kind: &str) -> Response {
    let domain_name = body_str(body, "domain").to_string();
    with_domain(st, &domain_name, |dm| {
        let status = body_str(body, "registrationStatus");
        let reverse = is_true(body.get("reverseOrder"));
        ok(json!({ "typeInfos": dm.list_type_infos(kind, status, reverse) }))
    })
}

// ---------------------------------------------------------------------------
// Workflow execution handlers
// ---------------------------------------------------------------------------

/// StartWorkflowExecution — default resolution order and the
/// DefaultUndefinedFault chain mirror moto's `WorkflowExecution.__init__`.
fn start_workflow_execution(st: &mut SwfStore, body: &Value) -> Result<Response, Response> {
    let domain_name = body_str(body, "domain").to_string();
    let di = match st.domains.iter().position(|d| d.name == domain_name) {
        Some(i) => i,
        None => return Err(unknown_resource("domain", Some(&domain_name))),
    };
    let dm = &mut st.domains[di];
    let workflow_type = body.get("workflowType").cloned().unwrap_or(json!({}));
    let type_name = body_str(&workflow_type, "name").to_string();
    let type_version = body_str(&workflow_type, "version").to_string();
    let wtype = dm.find_type(WORKFLOW, &type_name, &type_version).cloned();
    let Some(wtype) = wtype else {
        return Err(unknown_resource(
            "type",
            Some(&format!(
                "WorkflowType=[name={type_name}, version={type_version}]"
            )),
        ));
    };
    if wtype.status == DEPRECATED {
        return Err(type_deprecated_fault(
            &capitalize(WORKFLOW),
            &type_name,
            &type_version,
        ));
    }
    // moto's `_get_from_kwargs_or_workflow_type` chain, in its order:
    // execution start-to-close, task list, task start-to-close, child policy.
    let esct = opt_str(body, "executionStartToCloseTimeout")
        .map(String::from)
        .or_else(|| wtype.default_execution_start_to_close_timeout.clone())
        .filter(|s| !s.is_empty());
    let Some(esct) = esct else {
        return Err(default_undefined("executionStartToCloseTimeout"));
    };
    fn resolve(
        arg: Option<String>,
        fallback: impl FnOnce() -> Option<String>,
        camel: &str,
    ) -> Result<String, Response> {
        match arg
            .filter(|s| !s.is_empty())
            .or_else(fallback)
            .filter(|s| !s.is_empty())
        {
            Some(v) => Ok(v),
            None => Err(default_undefined(camel)),
        }
    }
    let task_arg = body
        .get("taskList")
        .and_then(|tl| tl.get("name"))
        .and_then(Value::as_str)
        .map(String::from);
    let task_list = resolve(task_arg, || wtype.task_list.clone(), "taskList")?;
    let tsct = resolve(
        opt_str(body, "taskStartToCloseTimeout").map(String::from),
        || wtype.default_task_start_to_close_timeout.clone(),
        "taskStartToCloseTimeout",
    )?;
    let child_policy = resolve(
        opt_str(body, "childPolicy").map(String::from),
        || wtype.default_child_policy.clone(),
        "childPolicy",
    )?;

    let workflow_id = body_str(body, "workflowId").to_string();
    if dm.wfe_index(&workflow_id, None).is_some() {
        return Err(wfe_already_started());
    }
    let run_id = Uuid::new_v4().simple().to_string();
    let wfe = WorkflowExecution {
        workflow_id: workflow_id.clone(),
        run_id: run_id.clone(),
        cancel_requested: false,
        close_cause: None,
        close_status: None,
        close_timestamp: None,
        execution_status: OPEN.to_string(),
        latest_activity_task_timestamp: None,
        latest_execution_context: None,
        start_timestamp: None,
        tag_list: body
            .get("tagList")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(String::from)
                    .collect()
            })
            .unwrap_or_default(),
        timeout_type: None,
        wtype,
        execution_start_to_close_timeout: esct,
        task_list,
        task_start_to_close_timeout: tsct,
        child_policy,
        input: opt_str(body, "input").map(String::from),
        open_counts: OpenCounts::default(),
        events: Vec::new(),
        previous_started_event_id: None,
        should_schedule_decision_next: false,
        timers: BTreeMap::new(),
    };
    let wfe_idx = dm.workflow_executions.len();
    dm.workflow_executions.push(wfe);
    dm.start_wfe(wfe_idx);
    Ok(ok(json!({ "runId": run_id })))
}

// ---------------------------------------------------------------------------
// Execution / task / count / list / tag handlers
// ---------------------------------------------------------------------------

/// DescribeWorkflowExecution.
fn describe_workflow_execution(st: &mut SwfStore, body: &Value) -> Response {
    st.process_timeouts_all();
    let params = match workflow_execution_params(body) {
        Ok(p) => p,
        Err(resp) => return resp,
    };
    with_domain(st, &params.domain, |dm| {
        let wfe_idx = match dm.require_wfe(&params.workflow_id, params.run_id.as_deref(), false) {
            Ok(i) => i,
            Err(resp) => return resp,
        };
        ok(dm.workflow_executions[wfe_idx].to_full_dict())
    })
}

/// Generated workflow-execution params (moto master d3b9faed #10262 —
/// `_get_workflow_execution_params`): the execution-focused ops must
/// validate `domain`, `execution.runId` and `execution.workflowId` as
/// required strings before touching the domain. Missing/null members
/// answer ValidationException with moto's exact
/// "N validation {e,}rror(s) detected: Value null at '{name}' failed to
/// satisfy constraint: Member must not be null" composition (execution
/// before domain, runId before workflowId); non-string members answer
/// SerializationException.
struct WorkflowExecutionParams {
    domain: String,
    run_id: Option<String>,
    workflow_id: String,
}

fn workflow_execution_params(body: &Value) -> Result<WorkflowExecutionParams, Response> {
    let execution = body.get("execution").filter(|e| !e.is_null());
    let mut checks: Vec<(&str, Option<&Value>)> = Vec::new();
    match execution {
        None => checks.push(("execution", None)),
        Some(execution) => {
            checks.push(("execution.runId", execution.get("runId")));
            checks.push(("execution.workflowId", execution.get("workflowId")));
        }
    }
    checks.push(("domain", body.get("domain")));

    let mut errors: Vec<String> = Vec::new();
    for (name, value) in &checks {
        match value {
            None => {
                errors.push(format!(
                    "Value null at '{name}' failed to satisfy constraint: Member must not be null"
                ));
            }
            Some(v) if !v.is_string() => return Err(serialization_exception(v)),
            Some(_) => {}
        }
    }
    if !errors.is_empty() {
        let label = if errors.len() == 1 { "error" } else { "errors" };
        return Err(validation_exception(&format!(
            "{} validation {} detected: {}",
            errors.len(),
            label,
            errors.join("; ")
        )));
    }
    let execution = body.get("execution").unwrap_or(&Value::Null);
    let run_id = execution
        .get("runId")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(String::from);
    let workflow_id = execution
        .get("workflowId")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    Ok(WorkflowExecutionParams {
        domain: body
            .get("domain")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        run_id,
        workflow_id,
    })
}

/// GetWorkflowExecutionHistory — the full event list, optionally reversed.
fn execution_history(st: &mut SwfStore, body: &Value) -> Response {
    st.process_timeouts_all();
    let params = match workflow_execution_params(body) {
        Ok(p) => p,
        Err(resp) => return resp,
    };
    with_domain(st, &params.domain, |dm| {
        let wfe_idx = match dm.require_wfe(&params.workflow_id, params.run_id.as_deref(), false) {
            Ok(i) => i,
            Err(resp) => return resp,
        };
        let reverse = is_true(body.get("reverseOrder"));
        let events: Vec<Value> = if reverse {
            dm.workflow_executions[wfe_idx]
                .events
                .iter()
                .rev()
                .map(HistoryEvent::to_dict)
                .collect()
        } else {
            dm.workflow_executions[wfe_idx]
                .events
                .iter()
                .map(HistoryEvent::to_dict)
                .collect()
        };
        ok(json!({ "events": events }))
    })
}

fn terminate_workflow_execution(st: &mut SwfStore, body: &Value) -> Response {
    st.process_timeouts_all();
    let domain_name = body_str(body, "domain").to_string();
    with_domain(st, &domain_name, |dm| {
        let workflow_id = body_str(body, "workflowId").to_string();
        let run_id = opt_str(body, "runId");
        let wfe_idx = match dm.require_wfe(&workflow_id, run_id, true) {
            Ok(i) => i,
            Err(resp) => return resp,
        };
        let child_policy = opt_str(body, "childPolicy");
        let details = body.get("details").cloned().unwrap_or(Value::Null);
        let reason = opt_str(body, "reason");
        dm.wfe_mut(wfe_idx)
            .terminate_wfe(child_policy, &details, reason);
        ok_empty()
    })
}

fn signal_workflow_execution(st: &mut SwfStore, body: &Value) -> Response {
    st.process_timeouts_all();
    let domain_name = body_str(body, "domain").to_string();
    with_domain(st, &domain_name, |dm| {
        let workflow_id = body_str(body, "workflowId").to_string();
        let run_id = opt_str(body, "runId");
        let wfe_idx = match dm.require_wfe(&workflow_id, run_id, true) {
            Ok(i) => i,
            Err(resp) => return resp,
        };
        let signal_name = body_str(body, "signalName").to_string();
        let input = opt_str(body, "input").map(String::from);
        {
            let wfe = dm.wfe_mut(wfe_idx);
            wfe.signal_wfe(&signal_name, input.as_deref());
        }
        dm.schedule_decision_task(wfe_idx);
        ok_empty()
    })
}

fn request_cancel_workflow_execution(st: &mut SwfStore, body: &Value) -> Response {
    st.process_timeouts_all();
    let domain_name = body_str(body, "domain").to_string();
    with_domain(st, &domain_name, |dm| {
        let workflow_id = body_str(body, "workflowId").to_string();
        let run_id = opt_str(body, "runId");
        let wfe_idx = match dm.require_wfe(&workflow_id, run_id, true) {
            Ok(i) => i,
            Err(resp) => return resp,
        };
        dm.wfe_mut(wfe_idx).request_cancel_wfe();
        dm.schedule_decision_task(wfe_idx);
        ok_empty()
    })
}

/// PollForDecisionTask.
fn poll_for_decision_task(st: &mut SwfStore, body: &Value) -> Response {
    st.process_timeouts_all();
    let domain_name = body_str(body, "domain").to_string();
    with_domain(st, &domain_name, |dm| {
        let task_list = body
            .get("taskList")
            .and_then(|tl| tl.get("name"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let identity = opt_str(body, "identity").map(String::from);
        let reverse = is_true(body.get("reverseOrder"));
        ok(dm.poll_decision_task(&task_list, identity.as_deref(), reverse))
    })
}

/// CountPending{Decision,Activity}Tasks by task list.
fn count_pending_tasks(st: &mut SwfStore, body: &Value, activity: bool) -> Response {
    let domain_name = body_str(body, "domain").to_string();
    with_domain(st, &domain_name, |dm| {
        let task_list = body
            .get("taskList")
            .and_then(|tl| tl.get("name"))
            .and_then(Value::as_str)
            .unwrap_or("");
        let count: i64 = if activity {
            dm.activity_task_lists
                .iter()
                .filter(|(name, _)| name.as_str() == task_list)
                .flat_map(|(_, tasks)| tasks.iter())
                .filter(|t| t.state == SCHEDULED || t.state == STARTED)
                .count() as i64
        } else {
            dm.workflow_executions
                .iter()
                .filter(|w| w.task_list == task_list)
                .map(|w| w.open_counts.open_decision_tasks)
                .sum()
        };
        ok(json!({ "count": count, "truncated": false }))
    })
}

/// RespondDecisionTaskCompleted.
fn respond_decision_task_completed(st: &mut SwfStore, body: &Value) -> Response {
    st.process_timeouts_all();
    let token = body_str(body, "taskToken").to_string();
    let di = st.domains.iter().position(|d| d.has_decision_token(&token));
    let di = match di {
        Some(i) => i,
        None => return validation_dead_token(),
    };
    let dm = &mut st.domains[di];
    let Some((wfe_idx, state, scheduled_event_id)) = dm.decision_task_info(&token) else {
        return validation_dead_token();
    };
    let wfe = &dm.workflow_executions[wfe_idx];
    if !wfe.open() {
        return unknown_resource(
            "execution",
            Some(&format!(
                "WorkflowExecution=[workflowId={}, runId={}]",
                wfe.workflow_id, wfe.run_id
            )),
        );
    }
    if state != STARTED {
        if state == COMPLETED {
            return unknown_resource(
                &format!("decision task, scheduledEventId = {scheduled_event_id}"),
                None,
            );
        }
        // moto raises an internal ValueError here — reachable only through
        // direct token manipulation.
        return internal_failure("decision task is not started");
    }
    let decisions = body
        .get("decisions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let context = opt_str(body, "executionContext").map(String::from);
    if let Err(resp) = dm.complete_decision_task(wfe_idx, &token, decisions, context.as_deref()) {
        return resp;
    }
    ok_empty()
}

fn validation_dead_token() -> Response {
    validation_exception("Invalid token")
}

/// RespondActivityTask{Completed, Failed, Canceled} share moto's token
/// checks and drive the task state + a new decision task.
enum RespondActivity {
    Completed,
    Failed,
    Canceled,
}

fn respond_activity_task(st: &mut SwfStore, body: &Value, which: RespondActivity) -> Response {
    st.process_timeouts_all();
    let token = body_str(body, "taskToken").to_string();
    let di = match st.domains.iter().position(|d| d.has_activity_token(&token)) {
        Some(i) => i,
        None => return validation_dead_token(),
    };
    let dm = &mut st.domains[di];
    let Some((wfe_idx, state, scheduled_event_id)) = dm.activity_task_info(&token) else {
        return validation_dead_token();
    };
    let wfe = &dm.workflow_executions[wfe_idx];
    if !wfe.open() {
        return unknown_resource(
            "execution",
            Some(&format!(
                "WorkflowExecution=[workflowId={}, runId={}]",
                wfe.workflow_id, wfe.run_id
            )),
        );
    }
    if state != STARTED {
        if state == COMPLETED {
            return unknown_resource(
                &format!("activity, scheduledEventId = {scheduled_event_id}"),
                None,
            );
        }
        return internal_failure("activity task is not started");
    }
    match which {
        RespondActivity::Completed => {
            let result = opt_str(body, "result").map(String::from);
            dm.complete_activity_task(&token, result.as_deref());
        }
        RespondActivity::Failed => {
            let reason = opt_str(body, "reason").map(String::from);
            let details = body.get("details").cloned().unwrap_or(Value::Null);
            dm.fail_activity_task(&token, reason.as_deref(), &details);
        }
        RespondActivity::Canceled => {
            let details = body.get("details").cloned().unwrap_or(Value::Null);
            dm.cancel_activity_task(&token, &details);
        }
    }
    ok_empty()
}

/// RecordActivityTaskHeartbeat.
fn record_activity_task_heartbeat(st: &mut SwfStore, body: &Value) -> Response {
    st.process_timeouts_all();
    let token = body_str(body, "taskToken").to_string();
    let di = match st.domains.iter().position(|d| d.has_activity_token(&token)) {
        Some(i) => i,
        None => return validation_dead_token(),
    };
    let dm = &mut st.domains[di];
    let Some((wfe_idx, state, scheduled_event_id)) = dm.activity_task_info(&token) else {
        return validation_dead_token();
    };
    let wfe = &dm.workflow_executions[wfe_idx];
    if !wfe.open() {
        return unknown_resource(
            "execution",
            Some(&format!(
                "WorkflowExecution=[workflowId={}, runId={}]",
                wfe.workflow_id, wfe.run_id
            )),
        );
    }
    if state != STARTED {
        if state == COMPLETED {
            return unknown_resource(
                &format!("activity, scheduledEventId = {scheduled_event_id}"),
                None,
            );
        }
        return internal_failure("activity task is not started");
    }
    let details = opt_str(body, "details").map(String::from);
    dm.record_heartbeat(&token, details.as_deref());
    ok(json!({ "cancelRequested": false }))
}

/// PollForActivityTask.
fn poll_for_activity_task(st: &mut SwfStore, body: &Value) -> Response {
    st.process_timeouts_all();
    let domain_name = body_str(body, "domain").to_string();
    with_domain(st, &domain_name, |dm| {
        let task_list = body
            .get("taskList")
            .and_then(|tl| tl.get("name"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let identity = opt_str(body, "identity").map(String::from);
        ok(dm.poll_activity_task(&task_list, identity.as_deref()))
    })
}

/// CountClosed/CountOpenWorkflowExecutions.
fn count_workflow_executions(
    st: &mut SwfStore,
    body: &Value,
    open: bool,
) -> Result<Response, Response> {
    st.process_timeouts_all();
    let domain_name = body_str(body, "domain").to_string();
    // moto validates exclusivity and requires a time filter before the
    // deprecated-domain check.
    if open {
        check_exclusivity(body, &["executionFilter", "typeFilter", "tagFilter"])?;
        if body.get("startTimeFilter").is_none_or(|v| v.is_null()) {
            return Err(validation_exception("Must specify time filter"));
        }
    } else {
        check_exclusivity(
            body,
            &[
                "executionFilter",
                "typeFilter",
                "tagFilter",
                "closeStatusFilter",
            ],
        )?;
        check_exclusivity(body, &["startTimeFilter", "closeTimeFilter"])?;
        if body.get("startTimeFilter").is_none_or(|v| v.is_null())
            && body.get("closeTimeFilter").is_none_or(|v| v.is_null())
        {
            return Err(validation_exception("Must specify time filter"));
        }
    }
    Ok(with_domain(st, &domain_name, |dm| {
        if dm.status == DEPRECATED {
            return domain_deprecated_fault(&dm.name);
        }
        let tag = tag_from_filter(body.get("tagFilter"));
        let close_status: Option<Option<&str>> = body
            .get("closeStatusFilter")
            .filter(|v| !v.is_null())
            .map(|f| f.get("status").and_then(Value::as_str));
        let count = dm
            .workflow_executions
            .iter()
            .filter(|w| {
                if open {
                    w.execution_status == OPEN
                } else {
                    w.execution_status == CLOSED
                }
            })
            .filter(|w| tag.is_none_or(|t| w.tag_list.iter().any(|x| x == t)))
            .filter(|w| {
                if open {
                    // count_open has no close-status filter (moto applies the
                    // close_status drop only in the closed counter).
                    true
                } else {
                    close_status.is_none_or(|cs| w.close_status.as_deref() == cs)
                }
            })
            .count() as i64;
        ok(json!({ "count": count, "truncated": false }))
    }))
}

/// ListClosed/ListOpenWorkflowExecutions.
fn list_workflow_executions(
    st: &mut SwfStore,
    body: &Value,
    open: bool,
) -> Result<Response, Response> {
    if open {
        check_exclusivity(body, &["executionFilter", "typeFilter", "tagFilter"])?;
        // moto reads startTimeFilter directly — a missing key crashes with a
        // KeyError, which surfaces as an internal failure.
        match body.get("startTimeFilter") {
            Some(tf) if !tf.is_null() => {
                check_float_or_int(tf.get("oldestDate"))?;
                check_float_or_int(tf.get("latestDate"))?;
            }
            _ => return Err(internal_failure("'startTimeFilter'")),
        }
    } else {
        check_exclusivity(
            body,
            &[
                "executionFilter",
                "typeFilter",
                "tagFilter",
                "closeStatusFilter",
            ],
        )?;
        check_exclusivity(body, &["startTimeFilter", "closeTimeFilter"])?;
        if body.get("startTimeFilter").is_none_or(|v| v.is_null())
            && body.get("closeTimeFilter").is_none_or(|v| v.is_null())
        {
            return Err(validation_exception("Must specify time filter"));
        }
        check_float_or_int(
            body.get("startTimeFilter")
                .and_then(|tf| tf.get("oldestDate")),
        )?;
        check_float_or_int(
            body.get("startTimeFilter")
                .and_then(|tf| tf.get("latestDate")),
        )?;
        check_float_or_int(
            body.get("closeTimeFilter")
                .and_then(|tf| tf.get("oldestDate")),
        )?;
        check_float_or_int(
            body.get("closeTimeFilter")
                .and_then(|tf| tf.get("latestDate")),
        )?;
    }
    let max_page = maximum_page_size(body)?;
    st.process_timeouts_all();
    let domain_name = body_str(body, "domain").to_string();
    Ok(with_domain(st, &domain_name, |dm| {
        if dm.status == DEPRECATED {
            return domain_deprecated_fault(&dm.name);
        }
        let tag = tag_from_filter(body.get("tagFilter"));
        // moto compares the closeStatusFilter *object* with the close_status
        // string — never equal — so passing the filter empties the result.
        // Ported verbatim.
        let close_filter_present = body.get("closeStatusFilter").is_some_and(|v| !v.is_null());
        let mut infos: Vec<Value> = dm
            .workflow_executions
            .iter()
            .filter(|w| {
                if open {
                    w.execution_status == OPEN
                } else {
                    w.execution_status == CLOSED
                }
            })
            .filter(|w| tag.is_none_or(|t| w.tag_list.iter().any(|x| x == t)))
            .filter(|_| !close_filter_present)
            .map(WorkflowExecution::to_list_dict)
            .collect();
        if is_true(body.get("reverseOrder")) {
            infos.reverse();
        }
        infos.truncate(max_page.unsigned_abs() as usize);
        ok(json!({ "executionInfos": infos }))
    }))
}

// ---------------------------------------------------------------------------
// Tag handlers
// ---------------------------------------------------------------------------

fn with_domain_by_arn(
    st: &mut SwfStore,
    arn: &str,
    f: impl FnOnce(&mut Domain) -> Response,
) -> Response {
    match st.domains.iter().position(|d| d.arn() == arn) {
        Some(i) => f(&mut st.domains[i]),
        None => unknown_resource("domain", Some(arn)),
    }
}

fn tag_resource(st: &mut SwfStore, body: &Value) -> Response {
    let arn = body_str(body, "resourceArn").to_string();
    with_domain_by_arn(st, &arn, |dm| {
        if let Some(tags) = body.get("tags").and_then(Value::as_array) {
            dm.tag_upsert(tags);
        }
        ok_empty()
    })
}

fn untag_resource(st: &mut SwfStore, body: &Value) -> Response {
    let arn = body_str(body, "resourceArn").to_string();
    let tag_keys: Vec<String> = body
        .get("tagKeys")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(String::from)
                .collect()
        })
        .unwrap_or_default();
    let keys: Vec<&str> = tag_keys.iter().map(String::as_str).collect();
    with_domain_by_arn(st, &arn, |dm| {
        dm.untag(&keys);
        ok_empty()
    })
}

fn list_tags_for_resource(st: &mut SwfStore, body: &Value) -> Response {
    let arn = body_str(body, "resourceArn").to_string();
    with_domain_by_arn(st, &arn, |dm| {
        let tags: Vec<Value> = dm
            .tags
            .iter()
            .map(|(key, value)| json!({ "key": key, "value": value }))
            .collect();
        ok(json!({ "tags": tags }))
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::atomic::{AtomicU64, Ordering};

    static ACCOUNTS: AtomicU64 = AtomicU64::new(100_000_000_101);

    /// Every test takes a fresh account so the shared store stays isolated.
    fn next_account() -> u64 {
        ACCOUNTS.fetch_add(1, Ordering::SeqCst)
    }

    fn send(account: u64, operation: &str, body: Value) -> (u16, Value) {
        let request = Request {
            service: "swf".to_string(),
            operation: operation.to_string(),
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            account,
            region: "us-east-1".to_string(),
        };
        let resp = dispatch(&request).expect("swf dispatch");
        (
            resp.status,
            serde_json::from_slice(&resp.body).unwrap_or(json!({})),
        )
    }

    fn register_domain(account: u64, name: &str) -> u16 {
        send(
            account,
            "RegisterDomain",
            json!({
                "name": name,
                "workflowExecutionRetentionPeriodInDays": "30",
            }),
        )
        .0
    }

    /// Domain + workflow/activity types with the defaults the suite uses.
    fn setup_workflow(account: u64, domain: &str, task_list: &str) {
        assert_eq!(register_domain(account, domain), 200);
        assert_eq!(
            send(
                account,
                "RegisterWorkflowType",
                json!({
                    "domain": domain,
                    "name": "wftest",
                    "version": "1.0",
                    "defaultExecutionStartToCloseTimeout": "3600",
                    "defaultTaskStartToCloseTimeout": "300",
                    "defaultTaskList": { "name": task_list },
                    "defaultChildPolicy": "TERMINATE",
                }),
            )
            .0,
            200
        );
        assert_eq!(
            send(
                account,
                "RegisterActivityType",
                json!({
                    "domain": domain,
                    "name": "acttest",
                    "version": "1.0",
                    "defaultTaskStartToCloseTimeout": "300",
                    "defaultTaskHeartbeatTimeout": "60",
                    "defaultTaskList": { "name": task_list },
                    "defaultTaskScheduleToStartTimeout": "300",
                    "defaultTaskScheduleToCloseTimeout": "600",
                }),
            )
            .0,
            200
        );
    }

    fn start_exec(account: u64, domain: &str, workflow_id: &str, task_list: &str) -> String {
        let (status, body) = send(
            account,
            "StartWorkflowExecution",
            json!({
                "domain": domain,
                "workflowId": workflow_id,
                "workflowType": { "name": "wftest", "version": "1.0" },
                "taskList": { "name": task_list },
            }),
        );
        assert_eq!(status, 200);
        body["runId"].as_str().expect("runId").to_string()
    }

    #[test]
    fn dispatch_ignores_other_services() {
        let request = Request {
            service: "s3".to_string(),
            operation: "RegisterDomain".to_string(),
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: Bytes::new(),
            account: 123456789012,
            region: "us-east-1".to_string(),
        };
        assert!(dispatch(&request).is_none());
    }

    #[test]
    fn unknown_operation_is_not_implemented() {
        let (status, body) = send(next_account(), "NopeOperation", json!({}));
        assert_eq!(status, 501);
        assert_eq!(body["__type"], "NotImplemented");
    }

    #[test]
    fn domain_lifecycle() {
        let account = next_account();
        let name = "domain-lifecycle";
        assert_eq!(register_domain(account, name), 200);

        let (_, body) = send(account, "DescribeDomain", json!({ "name": name }));
        assert_eq!(body["domainInfo"]["name"], json!(name));
        assert_eq!(body["domainInfo"]["status"], "REGISTERED");
        assert_eq!(
            body["domainInfo"]["arn"],
            json!(format!("arn:aws:swf:us-east-1:{account}:/domain/{name}"))
        );
        assert_eq!(
            body["configuration"]["workflowExecutionRetentionPeriodInDays"],
            "30"
        );

        // duplicate registration faults
        let (status, body) = send(
            account,
            "RegisterDomain",
            json!({
                "name": name,
                "workflowExecutionRetentionPeriodInDays": "30",
            }),
        );
        assert_eq!(status, 400);
        assert_eq!(
            body["__type"],
            "com.amazonaws.swf.base.model#DomainAlreadyExistsFault"
        );

        // unknown domain faults carry the model prefix
        let (status, body) = send(account, "DescribeDomain", json!({ "name": "nope" }));
        assert_eq!(status, 400);
        assert_eq!(
            body["__type"],
            "com.amazonaws.swf.base.model#UnknownResourceFault"
        );
        assert_eq!(body["message"], "Unknown domain: nope");

        assert_eq!(
            send(account, "DeprecateDomain", json!({ "name": name })).0,
            200
        );
        let (_, body) = send(account, "DescribeDomain", json!({ "name": name }));
        assert_eq!(body["domainInfo"]["status"], "DEPRECATED");
        // deprecating twice faults
        let (status, body) = send(account, "DeprecateDomain", json!({ "name": name }));
        assert_eq!(status, 400);
        assert_eq!(
            body["__type"],
            "com.amazonaws.swf.base.model#DomainDeprecatedFault"
        );

        // undeprecate restores the domain
        assert_eq!(
            send(account, "UndeprecateDomain", json!({ "name": name })).0,
            200
        );
        let (_, body) = send(account, "DescribeDomain", json!({ "name": name }));
        assert_eq!(body["domainInfo"]["status"], "REGISTERED");

        // list in both statuses
        send(account, "DeprecateDomain", json!({ "name": name }));
        let (_, body) = send(
            account,
            "ListDomains",
            json!({ "registrationStatus": "DEPRECATED" }),
        );
        let names: Vec<&str> = body["domainInfos"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|d| d["name"].as_str())
            .collect();
        assert!(names.contains(&name));
    }

    #[test]
    fn type_lifecycle() {
        let account = next_account();
        let domain = "type-lifecycle-domain";
        register_domain(account, domain);

        let (status, _) = send(
            account,
            "RegisterActivityType",
            json!({
                "domain": domain,
                "name": "act",
                "version": "1",
                "defaultTaskList": { "name": "tl" },
                "defaultTaskHeartbeatTimeout": "60",
                "description": "d",
            }),
        );
        assert_eq!(status, 200);

        // re-register faults with the model-prefixed type
        let (status, body) = send(
            account,
            "RegisterActivityType",
            json!({ "domain": domain, "name": "act", "version": "1" }),
        );
        assert_eq!(status, 400);
        assert_eq!(
            body["__type"],
            "com.amazonaws.swf.base.model#TypeAlreadyExistsFault"
        );
        assert_eq!(body["message"], "ActivityType=[name=act, version=1]");

        let (_, body) = send(
            account,
            "DescribeActivityType",
            json!({ "domain": domain, "activityType": { "name": "act", "version": "1" } }),
        );
        assert_eq!(body["typeInfo"]["status"], "REGISTERED");
        assert_eq!(body["typeInfo"]["creationDate"], 1420066800);
        assert_eq!(body["typeInfo"]["description"], "d");
        assert_eq!(body["configuration"]["defaultTaskList"]["name"], "tl");
        assert_eq!(body["configuration"]["defaultTaskHeartbeatTimeout"], "60");

        assert_eq!(
            send(
                account,
                "DeprecateActivityType",
                json!({ "domain": domain, "activityType": { "name": "act", "version": "1" } }),
            )
            .0,
            200
        );
        let (_, body) = send(
            account,
            "DescribeActivityType",
            json!({ "domain": domain, "activityType": { "name": "act", "version": "1" } }),
        );
        assert_eq!(body["typeInfo"]["status"], "DEPRECATED");
        assert_eq!(body["typeInfo"]["deprecationDate"], 1422745200);

        // delete only works on deprecated types
        let (status, _body) = send(
            account,
            "DeleteActivityType",
            json!({ "domain": domain, "activityType": { "name": "act", "version": "1" } }),
        );
        assert_eq!(status, 200);
        let (_, body) = send(
            account,
            "ListActivityTypes",
            json!({ "domain": domain, "registrationStatus": "DEPRECATED" }),
        );
        assert_eq!(body["typeInfos"], json!([]));

        // delete of a registered type must fault
        send(
            account,
            "RegisterWorkflowType",
            json!({ "domain": domain, "name": "wf", "version": "1" }),
        );
        let (status, body) = send(
            account,
            "DeleteWorkflowType",
            json!({ "domain": domain, "workflowType": { "name": "wf", "version": "1" } }),
        );
        assert_eq!(status, 400);
        assert_eq!(
            body["__type"],
            "com.amazon.coral.validate#ValidationException"
        );
        assert_eq!(
            body["message"],
            "The workflow type must be deprecated first: WorkflowType=[name=wf, version=1]"
        );
    }

    #[test]
    fn start_requires_defaults() {
        let account = next_account();
        let domain = "defaults-domain";
        register_domain(account, domain);
        send(
            account,
            "RegisterWorkflowType",
            json!({ "domain": domain, "name": "bare", "version": "1" }),
        );
        let (status, body) = send(
            account,
            "StartWorkflowExecution",
            json!({
                "domain": domain,
                "workflowId": "wf1",
                "workflowType": { "name": "bare", "version": "1" },
            }),
        );
        assert_eq!(status, 400);
        assert_eq!(
            body["__type"],
            "com.amazonaws.swf.base.model#DefaultUndefinedFault"
        );
        assert_eq!(body["message"], "executionStartToCloseTimeout");
    }

    #[test]
    fn workflow_execution_lifecycle() {
        let account = next_account();
        let domain = "lifecycle-domain";
        let task_list = "lifecycle-list";
        setup_workflow(account, domain, task_list);

        // start requires workflowId uniqueness while open
        let run_id = start_exec(account, domain, "wf-life", task_list);
        assert_eq!(run_id.len(), 32);

        let (status, body) = send(
            account,
            "StartWorkflowExecution",
            json!({
                "domain": domain,
                "workflowId": "wf-life",
                "workflowType": { "name": "wftest", "version": "1.0" },
                "taskList": { "name": task_list },
            }),
        );
        assert_eq!(status, 400);
        assert_eq!(
            body["__type"],
            "com.amazonaws.swf.base.model#WorkflowExecutionAlreadyStartedFault"
        );

        // count + describe the open execution
        let (_, body) = send(
            account,
            "CountOpenWorkflowExecutions",
            json!({
                "domain": domain,
                "startTimeFilter": { "oldestDate": 0.0 },
            }),
        );
        assert_eq!(body["count"], 1);
        assert_eq!(body["truncated"], false);

        let (_, body) = send(
            account,
            "DescribeWorkflowExecution",
            json!({
                "domain": domain,
                "execution": { "workflowId": "wf-life", "runId": run_id.clone() },
            }),
        );
        assert_eq!(body["executionInfo"]["execution"]["workflowId"], "wf-life");
        assert_eq!(body["executionInfo"]["execution"]["runId"], run_id.clone());
        assert_eq!(body["executionInfo"]["executionStatus"], "OPEN");
        assert_eq!(body["executionInfo"]["startTimestamp"], 1420066800.123);
        assert_eq!(
            body["executionConfiguration"]["taskList"]["name"],
            task_list
        );
        assert_eq!(body["executionConfiguration"]["childPolicy"], "TERMINATE");
        assert_eq!(body["openCounts"]["openDecisionTasks"], 1);

        // poll + respond with an activity schedule decision
        let (_, decision) = send(
            account,
            "PollForDecisionTask",
            json!({ "domain": domain, "taskList": { "name": task_list } }),
        );
        let token = decision["taskToken"].as_str().unwrap().to_string();
        assert!(!token.is_empty());
        assert_eq!(decision["workflowType"]["name"], "wftest");
        let (status, _) = send(
            account,
            "RespondDecisionTaskCompleted",
            json!({
                "taskToken": token,
                "decisions": [
                    {
                        "decisionType": "ScheduleActivityTask",
                        "scheduleActivityTaskDecisionAttributes": {
                            "activityType": { "name": "acttest", "version": "1.0" },
                            "activityId": "act-1",
                            "input": "work-item",
                        },
                    }
                ],
            }),
        );
        assert_eq!(status, 200);

        // moto decrements openDecisionTasks when the decision set is handled
        // and schedules the next decision only on activity-task completion —
        // so no pending decision tasks remain here yet.
        let (_, body) = send(
            account,
            "CountPendingDecisionTasks",
            json!({ "domain": domain, "taskList": { "name": task_list } }),
        );
        assert_eq!(body["count"], 0);

        // poll the activity task; history must hold the scheduling events
        let (_, activity) = send(
            account,
            "PollForActivityTask",
            json!({ "domain": domain, "taskList": { "name": task_list } }),
        );
        assert_eq!(activity["activityId"], "act-1");
        assert_eq!(activity["input"], "work-item");
        let atoken = activity["taskToken"].as_str().unwrap().to_string();

        let (_, history) = send(
            account,
            "GetWorkflowExecutionHistory",
            json!({
                "domain": domain,
                "execution": { "workflowId": "wf-life", "runId": run_id.clone() },
            }),
        );
        let types: Vec<String> = history["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["eventType"].as_str().unwrap().to_string())
            .collect();
        assert!(types.contains(&"WorkflowExecutionStarted".to_string()));
        assert!(types.contains(&"DecisionTaskScheduled".to_string()));
        assert!(types.contains(&"DecisionTaskStarted".to_string()));
        assert!(types.contains(&"DecisionTaskCompleted".to_string()));
        assert!(types.contains(&"ActivityTaskScheduled".to_string()));
        assert!(types.contains(&"ActivityTaskStarted".to_string()));

        // heartbeat then complete
        let (status, body) = send(
            account,
            "RecordActivityTaskHeartbeat",
            json!({ "taskToken": atoken.clone() }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["cancelRequested"], false);

        let (status, _) = send(
            account,
            "RespondActivityTaskCompleted",
            json!({ "taskToken": atoken, "result": "done" }),
        );
        assert_eq!(status, 200);

        // a fresh decision task carries the completion through
        let (_, decision2) = send(
            account,
            "PollForDecisionTask",
            json!({ "domain": domain, "taskList": { "name": task_list } }),
        );
        let token2 = decision2["taskToken"].as_str().unwrap().to_string();
        let (status, _) = send(
            account,
            "RespondDecisionTaskCompleted",
            json!({
                "taskToken": token2,
                "decisions": [
                    {
                        "decisionType": "CompleteWorkflowExecution",
                        "completeWorkflowExecutionDecisionAttributes": {
                            "result": "final",
                        },
                    }
                ],
            }),
        );
        assert_eq!(status, 200);
        let (_, body) = send(
            account,
            "DescribeWorkflowExecution",
            json!({
                "domain": domain,
                "execution": { "workflowId": "wf-life", "runId": run_id.clone() },
            }),
        );
        assert_eq!(body["executionInfo"]["executionStatus"], "CLOSED");
        assert_eq!(body["executionInfo"]["closeStatus"], "COMPLETED");
        assert!(body["executionInfo"]["closeTimestamp"].is_number());

        // close makes lists/counters expose the execution
        let (_, body) = send(
            account,
            "CountClosedWorkflowExecutions",
            json!({
                "domain": domain,
                "startTimeFilter": { "oldestDate": 0.0 },
            }),
        );
        assert_eq!(body["count"], 1);
        let (_, body) = send(
            account,
            "ListClosedWorkflowExecutions",
            json!({
                "domain": domain,
                "startTimeFilter": { "oldestDate": 0.0 },
            }),
        );
        assert_eq!(
            body["executionInfos"][0]["execution"]["workflowId"],
            "wf-life"
        );
        assert_eq!(body["executionInfos"][0]["closeStatus"], "COMPLETED");

        // completed executions can be restarted with the same workflowId
        let (status, _) = send(
            account,
            "StartWorkflowExecution",
            json!({
                "domain": domain,
                "workflowId": "wf-life",
                "workflowType": { "name": "wftest", "version": "1.0" },
                "taskList": { "name": task_list },
            }),
        );
        assert_eq!(status, 200);
    }

    #[test]
    fn terminate_and_signal() {
        let account = next_account();
        let domain = "term-domain";
        let task_list = "term-list";
        setup_workflow(account, domain, task_list);
        let run_id = start_exec(account, domain, "wf-term", task_list);

        let (status, _) = send(
            account,
            "SignalWorkflowExecution",
            json!({ "domain": domain, "workflowId": "wf-term", "signalName": "go" }),
        );
        assert_eq!(status, 200);

        let (status, _) = send(
            account,
            "RequestCancelWorkflowExecution",
            json!({ "domain": domain, "workflowId": "wf-term" }),
        );
        assert_eq!(status, 200);

        let (_, body) = send(
            account,
            "DescribeWorkflowExecution",
            json!({
                "domain": domain,
                "execution": { "workflowId": "wf-term", "runId": run_id.clone() },
            }),
        );
        assert_eq!(body["executionInfo"]["cancelRequested"], true);

        // terminating twice faults (raise_if_closed)
        send(
            account,
            "TerminateWorkflowExecution",
            json!({ "domain": domain, "workflowId": "wf-term", "reason": "by test" }),
        );
        let (status, body) = send(
            account,
            "TerminateWorkflowExecution",
            json!({ "domain": domain, "workflowId": "wf-term" }),
        );
        assert_eq!(status, 400);
        assert_eq!(
            body["__type"],
            "com.amazonaws.swf.base.model#UnknownResourceFault"
        );
        assert_eq!(body["message"], "Unknown execution, workflowId = wf-term");

        let (_, body) = send(
            account,
            "ListClosedWorkflowExecutions",
            json!({
                "domain": domain,
                "startTimeFilter": { "oldestDate": 0.0 },
            }),
        );
        assert_eq!(
            body["executionInfos"][0]["execution"]["workflowId"],
            "wf-term"
        );
        // moto's terminate path never sets closeTimestamp — even in the list.
        assert!(body["executionInfos"][0].get("closeTimestamp").is_none());
        assert_eq!(body["executionInfos"][0]["closeStatus"], "TERMINATED");
    }

    #[test]
    fn decision_validation_faults() {
        let account = next_account();
        let domain = "decision-domain";
        let task_list = "decision-list";
        setup_workflow(account, domain, task_list);
        start_exec(account, domain, "wf-dec", task_list);
        let (_, decision) = send(
            account,
            "PollForDecisionTask",
            json!({ "domain": domain, "taskList": { "name": task_list } }),
        );
        let token = decision["taskToken"].as_str().unwrap().to_string();

        // required decision attributes are validated before anything happens
        let (status, body) = send(
            account,
            "RespondDecisionTaskCompleted",
            json!({
                "taskToken": token.clone(),
                "decisions": [
                    {"decisionType": "ScheduleActivityTask"}
                ],
            }),
        );
        assert_eq!(status, 400);
        assert_eq!(
            body["__type"],
            "com.amazon.coral.validate#ValidationException"
        );
        let message = body["message"].as_str().unwrap();
        assert!(message.contains("validation error"));
        assert!(message.contains("scheduleActivityTaskDecisionAttributes.activityId"));

        // bad decision type lists the enum in the message
        let (status, body) = send(
            account,
            "RespondDecisionTaskCompleted",
            json!({
                "taskToken": token.clone(),
                "decisions": [
                    {"decisionType": "BogusType"}
                ],
            }),
        );
        assert_eq!(status, 400);
        let message = body["message"].as_str().unwrap();
        assert!(message.contains("BogusType"));
        assert!(message.contains("CompleteWorkflowExecution"));

        // unhandled (TODO'd) decision types crash moto with a 500
        let (status, __body) = send(
            account,
            "RespondDecisionTaskCompleted",
            json!({
                "taskToken": token,
                "decisions": [
                    {
                        "decisionType": "StartChildWorkflowExecution",
                        "startChildWorkflowExecutionDecisionAttributes": {
                            "workflowId": "child",
                            "workflowType": {"name": "n", "version": "1"},
                        },
                    }
                ],
            }),
        );
        assert_eq!(status, 500);
    }

    #[test]
    fn close_must_be_last() {
        let account = next_account();
        let domain = "close-last-domain";
        let task_list = "close-last-list";
        setup_workflow(account, domain, task_list);
        start_exec(account, domain, "wf-cl", task_list);
        let (_, decision) = send(
            account,
            "PollForDecisionTask",
            json!({ "domain": domain, "taskList": { "name": task_list } }),
        );
        let token = decision["taskToken"].as_str().unwrap().to_string();
        let (status, body) = send(
            account,
            "RespondDecisionTaskCompleted",
            json!({
                "taskToken": token,
                "decisions": [
                    {"decisionType": "CompleteWorkflowExecution"},
                    {"decisionType": "ScheduleActivityTask"},
                ],
            }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["message"], "Close must be last decision in list");
    }

    #[test]
    fn zero_timeouts_progress_state() {
        let account = next_account();
        let domain = "timeout-domain";
        let task_list = "timeout-list";
        assert_eq!(register_domain(account, domain), 200);
        send(
            account,
            "RegisterWorkflowType",
            json!({
                "domain": domain,
                "name": "wftest",
                "version": "1.0",
                "defaultExecutionStartToCloseTimeout": "0",
                "defaultTaskStartToCloseTimeout": "300",
                "defaultTaskList": { "name": task_list },
                "defaultChildPolicy": "TERMINATE",
            }),
        );
        let run_id = start_exec(account, domain, "wf-timeout", task_list);

        // a 0-second execution timeout closes the workflow on the next op
        let (_, body) = send(
            account,
            "ListOpenWorkflowExecutions",
            json!({
                "domain": domain,
                "startTimeFilter": { "oldestDate": 0.0 },
            }),
        );
        assert_eq!(body["executionInfos"], json!([]));
        let (_, body) = send(
            account,
            "ListClosedWorkflowExecutions",
            json!({
                "domain": domain,
                "startTimeFilter": { "oldestDate": 0.0 },
            }),
        );
        assert_eq!(body["executionInfos"][0]["closeStatus"], "TIMED_OUT");
        let (_, history) = send(
            account,
            "GetWorkflowExecutionHistory",
            json!({
                "domain": domain,
                "execution": { "workflowId": "wf-timeout", "runId": run_id },
            }),
        );
        let types: Vec<String> = history["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["eventType"].as_str().unwrap().to_string())
            .collect();
        assert!(types.contains(&"WorkflowExecutionTimedOut".to_string()));
        let timed_out = history["events"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["eventType"] == "WorkflowExecutionTimedOut")
            .unwrap();
        // timeout events carry the deadline as eventTimestamp
        assert!(timed_out["eventTimestamp"].is_number());
    }

    #[test]
    fn markers_and_timers() {
        let account = next_account();
        let domain = "timer-domain";
        let task_list = "timer-list";
        setup_workflow(account, domain, task_list);
        let run_id = start_exec(account, domain, "wf-timer", task_list);
        let (_, decision) = send(
            account,
            "PollForDecisionTask",
            json!({ "domain": domain, "taskList": { "name": task_list } }),
        );
        let token = decision["taskToken"].as_str().unwrap().to_string();
        let (status, _) = send(
            account,
            "RespondDecisionTaskCompleted",
            json!({
                "taskToken": token,
                "decisions": [
                    {
                        "decisionType": "RecordMarker",
                        "recordMarkerDecisionAttributes": { "markerName": "m1" },
                    },
                    {
                        "decisionType": "StartTimer",
                        "startTimerDecisionAttributes": {
                            "startToFireTimeout": "60",
                            "timerId": "t1",
                        },
                    },
                    {
                        "decisionType": "CancelTimer",
                        "cancelTimerDecisionAttributes": { "timerId": "t1" },
                    },
                    {
                        "decisionType": "CancelTimer",
                        "cancelTimerDecisionAttributes": { "timerId": "unknown" },
                    },
                ],
            }),
        );
        assert_eq!(status, 200);
        let (_, history) = send(
            account,
            "GetWorkflowExecutionHistory",
            json!({
                "domain": domain,
                "execution": { "workflowId": "wf-timer", "runId": run_id },
            }),
        );
        let types: Vec<String> = history["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["eventType"].as_str().unwrap().to_string())
            .collect();
        assert!(types.contains(&"MarkerRecorded".to_string()));
        assert!(types.contains(&"TimerStarted".to_string()));
        // the timer lives until cancelled within the same decision set
        assert!(types.contains(&"TimerCancelled".to_string()));
        // cancelling an unknown timer records the failure event
        assert!(types.contains(&"CancelTimerFailed".to_string()));
    }

    #[test]
    fn tags_round_trip() {
        let account = next_account();
        let domain = "tag-domain";
        register_domain(account, domain);
        let (_, body) = send(account, "DescribeDomain", json!({ "name": domain }));
        let arn = body["domainInfo"]["arn"].as_str().unwrap().to_string();

        let (status, _) = send(
            account,
            "TagResource",
            json!({
                "resourceArn": arn,
                "tags": [
                    { "key": "env", "value": "test" },
                    { "key": "project", "value": "robotocore" },
                ],
            }),
        );
        assert_eq!(status, 200);
        let (_, body) = send(
            account,
            "ListTagsForResource",
            json!({ "resourceArn": arn }),
        );
        let tags = body["tags"].as_array().unwrap();
        assert_eq!(tags.len(), 2);

        // upsert an existing tag
        send(
            account,
            "TagResource",
            json!({
                "resourceArn": arn,
                "tags": [{ "key": "env", "value": "prod" }],
            }),
        );
        let (_, body) = send(
            account,
            "ListTagsForResource",
            json!({ "resourceArn": arn }),
        );
        let tags = body["tags"].as_array().unwrap();
        assert_eq!(tags.len(), 2);
        assert!(tags.contains(&json!({ "key": "env", "value": "prod" })));

        send(
            account,
            "UntagResource",
            json!({ "resourceArn": arn, "tagKeys": ["env"] }),
        );
        let (_, body) = send(
            account,
            "ListTagsForResource",
            json!({ "resourceArn": arn }),
        );
        let keys: Vec<&str> = body["tags"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|t| t["key"].as_str())
            .collect();
        assert_eq!(keys, vec!["project"]);

        // unknown arns fault
        let (status, body) = send(
            account,
            "ListTagsForResource",
            json!({ "resourceArn": "arn:aws:swf:us-east-1:1:/domain/zzz" }),
        );
        assert_eq!(status, 400);
        assert_eq!(
            body["__type"],
            "com.amazonaws.swf.base.model#UnknownResourceFault"
        );
    }

    #[test]
    fn polls_close_over_state_safely() {
        let account = next_account();
        let domain = "polls-domain";
        let task_list = "polls-list";
        setup_workflow(account, domain, task_list);
        send(
            account,
            "StartWorkflowExecution",
            json!({
                "domain": domain,
                "workflowId": "wf-polls",
                "workflowType": { "name": "wftest", "version": "1.0" },
                "taskList": { "name": task_list },
            }),
        );

        // empty activity poll reports zero startedEventId
        let (status, body) = send(
            account,
            "PollForActivityTask",
            json!({ "domain": domain, "taskList": { "name": task_list } }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["startedEventId"], 0);
    }

    // ---------------------------------------------------------------
    // moto master d3b9faed (#10262) — DescribeWorkflowExecution /
    // GetWorkflowExecutionHistory validate the required workflow
    // execution parameters before touching the domain. Missing/null
    // members answer 400 ValidationException with moto's exact
    // composite message; junk-typed members answer 400
    // SerializationException. (Moto 5.2.4 crashed with a 500 on all of
    // these — the pre-fix behavior the twin had mirrored as
    // UnknownResourceFault fall-through.)
    // ---------------------------------------------------------------

    fn error_code(body: &Value) -> String {
        body.get("__type")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .rsplit('#')
            .next()
            .unwrap_or_default()
            .to_string()
    }

    #[test]
    fn workflow_execution_invalid_parameters() {
        let account = next_account();
        for operation in ["DescribeWorkflowExecution", "GetWorkflowExecutionHistory"] {
            // invalid-type members answer SerializationException.
            let (status, body) = send(
                account,
                operation,
                json!({ "domain": 123, "execution": { "runId": "r", "workflowId": "w" } }),
            );
            assert_eq!(status, 400, "got {body}");
            assert_eq!(error_code(&body), "SerializationException", "got {body}");

            let (status, body) = send(
                account,
                operation,
                json!({ "domain": "d", "execution": { "runId": 123, "workflowId": "w" } }),
            );
            assert_eq!(status, 400);
            assert_eq!(error_code(&body), "SerializationException", "got {body}");

            let (status, body) = send(
                account,
                operation,
                json!({ "domain": "d", "execution": { "runId": "r", "workflowId": [] } }),
            );
            assert_eq!(status, 400);
            assert_eq!(error_code(&body), "SerializationException", "got {body}");
        }
    }

    #[test]
    fn workflow_execution_missing_or_null_members() {
        let account = next_account();
        for operation in ["DescribeWorkflowExecution", "GetWorkflowExecutionHistory"] {
            // no parameters: 'execution' and 'domain' both null.
            let (status, body) = send(account, operation, json!({}));
            assert_eq!(status, 400, "got {body}");
            assert_eq!(error_code(&body), "ValidationException", "got {body}");
            assert_eq!(
                body["message"],
                "2 validation errors detected: Value null at 'execution' failed to satisfy constraint: Member must not be null; Value null at 'domain' failed to satisfy constraint: Member must not be null",
                "got {body}"
            );

            // domain only: 'execution' null.
            let (status, body) = send(account, operation, json!({ "domain": "test-domain" }));
            assert_eq!(status, 400);
            assert_eq!(error_code(&body), "ValidationException");
            assert_eq!(
                body["message"],
                "1 validation error detected: Value null at 'execution' failed to satisfy constraint: Member must not be null",
                "got {body}"
            );

            // empty execution object: runId/workflowId null.
            let (status, body) = send(
                account,
                operation,
                json!({ "domain": "test-domain", "execution": {} }),
            );
            assert_eq!(status, 400);
            assert_eq!(error_code(&body), "ValidationException");
            assert_eq!(
                body["message"],
                "2 validation errors detected: Value null at 'execution.runId' failed to satisfy constraint: Member must not be null; Value null at 'execution.workflowId' failed to satisfy constraint: Member must not be null",
                "got {body}"
            );

            // A well-formed request still reaches the domain lookup — the
            // unknown domain fault comes after validation.
            let (status, body) = send(
                account,
                operation,
                json!({ "domain": "no-such-domain", "execution": { "runId": "r", "workflowId": "w" } }),
            );
            assert_eq!(status, 400);
            assert_eq!(error_code(&body), "UnknownResourceFault", "got {body}");
        }
    }
}
