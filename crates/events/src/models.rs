//! EventBridge in-memory state models.

use parking_lot::RwLock;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

/// Shared rule map type: (bus name, rule name) -> rule.
pub type RuleMap = HashMap<(String, String), Arc<Rule>>;

/// An EventBridge rule.
#[derive(Debug)]
pub struct Rule {
    pub name: String,
    pub bus_name: String,
    pub arn: String,
    pub description: String,
    /// Event pattern as a JSON string ("" when the rule only has a schedule).
    pub event_pattern: String,
    pub state: RwLock<String>,
    pub role_arn: Option<String>,
    pub schedule_expression: Option<String>,
    pub created: f64,
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
        });
        let obj = resp.as_object_mut().unwrap();
        if !self.description.is_empty() {
            obj.insert("Description".into(), json!(self.description));
        }
        if !self.event_pattern.is_empty() {
            obj.insert("EventPattern".into(), json!(self.event_pattern));
        }
        if let Some(ref sched) = self.schedule_expression {
            obj.insert("ScheduleExpression".into(), json!(sched));
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
}

impl EventsState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_rule(&self, bus: &str, name: &str) -> Option<Arc<Rule>> {
        self.rules.read().get(&(bus.to_string(), name.to_string())).cloned()
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
