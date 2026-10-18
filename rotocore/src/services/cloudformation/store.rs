//! CloudFormation state: stacks, change sets, stack sets, exports.
//! Per-(account, region), like the Python provider's `_stores`.

use serde_json::Value;
use std::collections::HashMap;
use std::sync::Mutex;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct CfnResource {
    pub logical_id: String,
    pub resource_type: String,
    /// Resolved properties (intrinsics applied).
    pub properties: Value,
    pub physical_id: Option<String>,
    pub attributes: HashMap<String, Value>,
    pub status: String,
    pub deletion_policy: String,
}

#[derive(Clone, Debug)]
pub struct CfnEvent {
    pub stack_id: String,
    pub stack_name: String,
    pub event_id: String,
    pub logical_id: String,
    pub physical_id: String,
    pub resource_type: String,
    pub resource_status: String,
    pub resource_status_reason: Option<String>,
    pub timestamp: String,
}

impl CfnEvent {
    pub fn to_json(&self) -> Value {
        let mut m = serde_json::Map::new();
        m.insert("StackId".into(), Value::String(self.stack_id.clone()));
        m.insert("StackName".into(), Value::String(self.stack_name.clone()));
        m.insert("EventId".into(), Value::String(self.event_id.clone()));
        m.insert(
            "LogicalResourceId".into(),
            Value::String(self.logical_id.clone()),
        );
        m.insert(
            "PhysicalResourceId".into(),
            Value::String(self.physical_id.clone()),
        );
        m.insert(
            "ResourceType".into(),
            Value::String(self.resource_type.clone()),
        );
        m.insert(
            "ResourceStatus".into(),
            Value::String(self.resource_status.clone()),
        );
        if let Some(reason) = &self.resource_status_reason {
            m.insert("ResourceStatusReason".into(), Value::String(reason.clone()));
        }
        m.insert("Timestamp".into(), Value::String(self.timestamp.clone()));
        Value::Object(m)
    }
}

#[derive(Clone, Debug)]
pub struct CfnStack {
    pub stack_id: String,
    pub stack_name: String,
    pub template_body: String,
    /// User parameters only (pseudo/internal keys live on the resolve ctx).
    pub parameters: Vec<(String, String)>,
    /// Resources in creation order.
    pub resources: Vec<CfnResource>,
    /// Output name -> {OutputKey, OutputValue, Description, ExportName?}.
    pub outputs: Vec<(String, Value)>,
    pub status: String,
    pub status_reason: String,
    pub created: i64,
    pub tags: Vec<(String, String)>,
    pub description: String,
    pub events: Vec<CfnEvent>,
    /// Exports registered by this stack (name -> value).
    pub exports: Vec<(String, String)>,
    pub policy: Option<String>,
    pub enable_termination_protection: bool,
}

impl CfnStack {
    pub fn new(stack_id: String, stack_name: String, template_body: String) -> Self {
        Self {
            stack_id,
            stack_name,
            template_body,
            parameters: Vec::new(),
            resources: Vec::new(),
            outputs: Vec::new(),
            status: "CREATE_IN_PROGRESS".to_string(),
            status_reason: String::new(),
            created: chrono::Utc::now().timestamp(),
            tags: Vec::new(),
            description: String::new(),
            events: Vec::new(),
            exports: Vec::new(),
            policy: None,
            enable_termination_protection: false,
        }
    }

    pub fn arn(&self) -> &str {
        &self.stack_id
    }

    pub fn resource(&self, logical_id: &str) -> Option<&CfnResource> {
        self.resources.iter().find(|r| r.logical_id == logical_id)
    }

    pub fn resource_mut(&mut self, logical_id: &str) -> Option<&mut CfnResource> {
        self.resources
            .iter_mut()
            .find(|r| r.logical_id == logical_id)
    }
}

#[derive(Clone, Debug)]
pub struct ExportEntry {
    pub value: String,
    pub stack_id: String,
}

#[derive(Clone, Debug)]
pub struct CfnChangeSet {
    pub change_set_id: String,
    pub change_set_name: String,
    pub stack_name: String,
    pub stack_id: String,
    pub template_body: String,
    pub status: String,
    pub status_reason: String,
    pub change_set_type: String,
    pub parameters: Vec<(String, String)>,
    pub created: i64,
}

#[derive(Clone, Debug)]
pub struct StackSetOperation {
    pub operation_id: String,
    pub action: String,
    pub status: String,
    pub creation_timestamp: String,
    pub instances: Vec<(String, String)>,
}

#[derive(Clone, Debug)]
pub struct StackInstance {
    pub account: String,
    pub region: String,
    pub stack_id: String,
    pub parameter_overrides: Value,
}

impl StackInstance {
    pub fn to_json(&self, stack_set_id: &str, stack_set_name: &str) -> Value {
        serde_json::json!({
            "StackId": crate::services::cloudformation::store::stack_arn(&self.stack_set_stack_name(stack_set_name), &self.region, &self.account),
            "StackSetId": stack_set_id,
            "Region": self.region,
            "Account": self.account,
            "Status": "CURRENT",
            "ParameterOverrides": self.parameter_overrides,
            "StackInstanceStatus": {"DetailedStatus": "SUCCEEDED"},
        })
    }

    fn stack_set_stack_name(&self, name: &str) -> String {
        format!("StackSet-{name}")
    }
}

#[derive(Clone, Debug, Default)]
pub struct StackSet {
    pub id: String,
    pub name: String,
    pub template_body: String,
    pub description: String,
    pub tags: Vec<(String, String)>,
    pub permission_model: String,
    pub parameters: Vec<(String, String)>,
    pub status: String,
    pub instances: Vec<StackInstance>,
    pub operations: Vec<StackSetOperation>,
}

#[derive(Clone, Debug)]
pub struct GeneratedTemplate {
    pub name: String,
    pub id: String,
    pub resources: Value,
    pub created: i64,
}

#[derive(Default)]
pub struct CfnStore {
    /// All stacks (including DELETE_COMPLETE ones, like the Python store).
    pub stacks: Vec<CfnStack>,
    /// Change sets keyed by their arn/id.
    pub change_sets: Vec<CfnChangeSet>,
    /// Export name -> entry.
    pub exports: Vec<(String, ExportEntry)>,
    pub stack_sets: Vec<StackSet>,
    pub generated_templates: Vec<GeneratedTemplate>,
}

impl CfnStore {
    /// Find a stack by name (preferring non-deleted, like py) or stack id.
    pub fn find_stack(&self, name_or_id: &str) -> Option<&CfnStack> {
        if let Some(s) = self.stacks.iter().find(|s| s.stack_id == name_or_id) {
            return Some(s);
        }
        let mut deleted: Option<&CfnStack> = None;
        for s in &self.stacks {
            if s.stack_name == name_or_id {
                if s.status != "DELETE_COMPLETE" {
                    return Some(s);
                }
                deleted = Some(s);
            }
        }
        deleted
    }

    pub fn find_stack_mut(&mut self, name_or_id: &str) -> Option<&mut CfnStack> {
        let matches = |s: &CfnStack| s.stack_id == name_or_id || s.stack_name == name_or_id;
        // Prefer a live stack over a DELETE_COMPLETE one when matching names.
        if let Some(pos) = self
            .stacks
            .iter()
            .position(|s| matches(s) && s.status != "DELETE_COMPLETE")
        {
            return Some(&mut self.stacks[pos]);
        }
        self.stacks.iter_mut().find(|s| matches(s))
    }

    pub fn find_change_set(&self, cs_name: &str, stack_name: &str) -> Option<&CfnChangeSet> {
        if let Some(cs) = self.change_sets.iter().find(|c| c.change_set_id == cs_name) {
            return Some(cs);
        }
        self.change_sets
            .iter()
            .find(|c| {
                c.change_set_name == cs_name
                    && (stack_name.is_empty() || c.stack_name == stack_name)
            })
            .or_else(|| {
                self.change_sets
                    .iter()
                    .find(|c| c.change_set_name == cs_name)
            })
    }

    pub fn find_change_set_mut(
        &mut self,
        cs_name: &str,
        stack_name: &str,
    ) -> Option<&mut CfnChangeSet> {
        let snapshot = self
            .find_change_set(cs_name, stack_name)
            .map(|c| c.change_set_id.clone());
        match snapshot {
            Some(id) => self.change_sets.iter_mut().find(|c| c.change_set_id == id),
            None => self
                .change_sets
                .iter_mut()
                .find(|c| c.change_set_name == cs_name),
        }
    }

    pub fn find_stack_set(&self, name: &str) -> Option<&StackSet> {
        self.stack_sets
            .iter()
            .find(|ss| ss.id == name || (ss.name == name && ss.status != "DELETED"))
    }

    /// Exports lookup: name -> value.
    pub fn export_map(&self) -> HashMap<String, String> {
        self.exports
            .iter()
            .map(|(k, e)| (k.clone(), e.value.clone()))
            .collect()
    }

    pub fn export_exists(&self, name: &str) -> bool {
        self.exports.iter().any(|(k, _)| k == name)
    }

    pub fn put_export(&mut self, name: &str, value: &str, stack_id: &str) {
        if let Some((_, e)) = self.exports.iter_mut().find(|(k, _)| k == name) {
            e.value = value.to_string();
            e.stack_id = stack_id.to_string();
        } else {
            self.exports.push((
                name.to_string(),
                ExportEntry {
                    value: value.to_string(),
                    stack_id: stack_id.to_string(),
                },
            ));
        }
    }

    pub fn remove_stack_exports(&mut self, stack_id: &str) {
        self.exports.retain(|(_, e)| e.stack_id != stack_id);
    }
}

fn global_stores() -> &'static Mutex<HashMap<(u64, String), CfnStore>> {
    static STORES: std::sync::OnceLock<Mutex<HashMap<(u64, String), CfnStore>>> =
        std::sync::OnceLock::new();
    STORES.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn with_store<T>(account: u64, region: &str, f: impl FnOnce(&mut CfnStore) -> T) -> T {
    let mut stores = global_stores().lock().unwrap_or_else(|p| p.into_inner());
    let store = stores.entry((account, region.to_string())).or_default();
    f(store)
}

pub fn new_stack_id(stack_name: &str, region: &str, account: &str) -> String {
    format!(
        "arn:aws:cloudformation:{region}:{account}:stack/{stack_name}/{}",
        Uuid::new_v4()
    )
}

pub fn new_change_set_id(cs_name: &str, region: &str, account: &str) -> String {
    format!(
        "arn:aws:cloudformation:{region}:{account}:changeSet/{cs_name}/{}",
        Uuid::new_v4()
    )
}

/// Synthetic stack id for stack-set instances.
pub fn stack_arn(name: &str, region: &str, account: &str) -> String {
    format!(
        "arn:aws:cloudformation:{region}:{account}:stack/{name}/{}",
        uuid::Uuid::new_v4()
    )
}
