//! EventBridge in-memory state models.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

/// Shared rule map type: (bus name, rule name) -> rule.
pub type RuleMap = HashMap<(String, String), Arc<Rule>>;

/// An EventBridge rule.
#[derive(Debug, Serialize, Deserialize)]
pub struct Rule {
    pub name: String,
    pub bus_name: String,
    pub arn: String,
    /// The creating account — moto renders it as CreatedBy in every
    /// rule's to_dict (events models.py rule fields, the 2026-11-12
    /// census row).
    #[serde(default)]
    pub created_by: u64,
    /// moto only strips an ABSENT description (models.py:370-375); an
    /// explicitly-empty one stays.
    #[serde(default)]
    pub description: Option<String>,
    /// moto EventPattern.load accepts ANY string and `dump()` returns the
    /// raw pattern — an explicitly-empty one is kept on the render
    /// (models.py:1129-1136, 1159-1164; probed 200).
    #[serde(default)]
    pub event_pattern: Option<String>,
    #[serde(with = "crate::snapshot::rw")]
    pub state: RwLock<String>,
    pub role_arn: Option<String>,
    pub schedule_expression: Option<String>,
    /// Rule-level delivery failure DLQ (`PutRule`); a target-level
    /// `DeadLetterConfig` takes precedence when dispatching (era models
    /// carried both and the dispatcher fell back to the rule's).
    #[serde(default)]
    pub dead_letter_config: Option<Value>,
    pub created: f64,
    #[serde(with = "crate::snapshot::rw")]
    pub targets: RwLock<Vec<Value>>,
}

impl Rule {
    /// Serialise a rule for ListRules/DescribeRule output.
    pub fn to_value(&self) -> Value {
        let mut resp = json!({
            "Name": self.name,
            "Arn": self.arn,
            "State": self.state.read().clone(),
            "EventBusName": self.bus_name,
            "CreatedBy": self.created_by.to_string(),
        });
        let obj = resp.as_object_mut().unwrap();
        // moto describe() (events/models.py:359-375) strips ONLY absent
        // values: an explicitly-empty Description stays on the render.
        if let Some(ref description) = self.description {
            obj.insert("Description".into(), json!(description));
        }
        if let Some(ref event_pattern) = self.event_pattern {
            obj.insert("EventPattern".into(), json!(event_pattern));
        }
        if let Some(ref sched) = self.schedule_expression {
            obj.insert("ScheduleExpression".into(), json!(sched));
        }
        // moto describe() (events/models.py:368) renders RoleArn when set.
        if let Some(ref role) = self.role_arn {
            obj.insert("RoleArn".into(), json!(role));
        }
        if let Some(ref dlq) = self.dead_letter_config {
            if !dlq.is_null() {
                obj.insert("DeadLetterConfig".into(), dlq.clone());
            }
        }
        resp
    }
}

/// The EventBridge state store for one (account, region).
#[derive(Clone, Default)]
pub struct EventsState {
    /// Rules keyed by (bus name, rule name).
    pub rules: Arc<RwLock<RuleMap>>,
    /// Custom event buses: name -> bus record.
    pub event_buses: Arc<RwLock<HashMap<String, Value>>>,
    /// Per-bus permission policy JSON (bus name -> policy document).
    pub bus_policies: Arc<RwLock<HashMap<String, Value>>>,
    /// Tags keyed by resource ARN (rules, buses, archives, connections, ...).
    pub tags: Arc<RwLock<HashMap<String, Vec<Value>>>>,
    /// Archives keyed by archive name.
    pub archives: Arc<RwLock<HashMap<String, Value>>>,
    /// Archived events keyed by archive name (PutEvents capture, replayed
    /// by StartReplay).
    pub archived_events: Arc<RwLock<HashMap<String, Vec<Value>>>>,
    /// Connections keyed by connection name.
    pub connections: Arc<RwLock<HashMap<String, Value>>>,
    /// API destinations keyed by destination name.
    pub api_destinations: Arc<RwLock<HashMap<String, Value>>>,
    /// Replays keyed by replay name.
    pub replays: Arc<RwLock<HashMap<String, Value>>>,
    /// Partner event sources keyed by source name.
    pub partner_sources: Arc<RwLock<HashMap<String, Value>>>,
    /// Endpoints keyed by endpoint name.
    pub endpoints: Arc<RwLock<HashMap<String, Value>>>,
    /// The virtual default bus' create stamp (moto `_add_default_event_bus`
    /// stamps it once at backend init, models.py:1221-1225) — the default
    /// bus is never a record in `event_buses`, so its stickiness lives
    /// here.
    pub default_bus_created: Arc<RwLock<Option<String>>>,
}

/// Format a datetime the way moto serializes `str(datetime)` on its
/// CreationTime/LastModifiedTime (census: `2026-11-12 09:11:04.916508+00:00`).
pub(crate) fn fmt_moto_dt(dt: chrono::DateTime<chrono::Utc>) -> String {
    dt.format("%Y-%m-%d %H:%M:%S%.6f+00:00").to_string()
}

impl EventsState {
    pub fn new() -> Self {
        Self {
            default_bus_created: Arc::new(RwLock::new(Some(fmt_moto_dt(chrono::Utc::now())))),
            ..Default::default()
        }
    }

    pub fn get_rule(&self, bus: &str, name: &str) -> Option<Arc<Rule>> {
        self.rules
            .read()
            .get(&(bus.to_string(), name.to_string()))
            .cloned()
    }

    /// Bus ARN for a bus name (the default bus is implicit, never stored).
    pub fn bus_arn(&self, account: u64, region: &str, bus: &str) -> String {
        if bus != "default" {
            let buses = self.event_buses.read();
            if let Some(Value::Object(b)) = buses.get(bus) {
                if let Some(arn) = b.get("Arn").and_then(|a| a.as_str()) {
                    return arn.to_string();
                }
            }
        }
        format!("arn:aws:events:{region}:{account}:event-bus/{bus}")
    }

    pub fn set_tags(&self, arn: &str, tags: Vec<Value>) {
        self.tags.write().insert(arn.to_string(), tags);
    }

    pub fn get_tags(&self, arn: &str) -> Vec<Value> {
        self.tags.read().get(arn).cloned().unwrap_or_default()
    }
}

impl std::fmt::Debug for EventsState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("EventsState")
    }
}
