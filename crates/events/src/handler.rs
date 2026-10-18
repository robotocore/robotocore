//! EventBridge operation handler.

use once_cell::sync::Lazy;
use parking_lot::RwLock;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

use crate::models::{EventsState, Rule};
use crate::protocol::{AwsRequest, AwsResponse};

/// Process-global EventBridge state registry, shared by every handler
/// instance so cross-service publishers (S3) can reach the same rules.
static GLOBAL_STATE: Lazy<RwLock<HashMap<(u64, String), EventsState>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

fn global_state(account: u64, region: &str) -> EventsState {
    let mut states = GLOBAL_STATE.write();
    let key = (account, region.to_string());
    states
        .entry(key)
        .or_default()
        .clone()
}

/// Routing target ARN supported for delivery.
const SQS_ARN_PREFIX: &str = "arn:aws:sqs:";

/// Route a pre-built event through EventBridge rule matching and deliver it
/// to matching rules' SQS targets. Called by S3 and other event producers.
pub fn publish_event_to_bus(event: &Value, account: u64, region: &str) {
    let state = {
        let mut states = GLOBAL_STATE.write();
        let key = (account, region.to_string());
        states.entry(key).or_default().clone()
    };
    let rules = state.rules.read();
    for rule in rules.values() {
        if *rule.state.read() != "ENABLED" {
            continue;
        }
        let pattern: Value = serde_json::from_str(&rule.event_pattern).unwrap_or(json!({}));
        if match_pattern(&pattern, event) {
            for target in rule.targets.read().iter() {
                let arn = target
                    .get("Arn")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                if arn.starts_with(SQS_ARN_PREFIX) {
                    sqs::deliver_to_queue(&arn, &serde_json::to_string(event).unwrap_or_default());
                }
            }
        }
    }
}

/// Match an event against an EventBridge pattern.
///
/// Pattern fields: objects require every key present in the event and
/// matching; arrays act as "any of" filters; other values compare exactly.
fn match_pattern(pattern: &Value, event: &Value) -> bool {
    match (pattern, event) {
        (Value::Object(p), Value::Object(e)) => p.iter().all(|(k, pv)| {
            e.get(k)
                .map(|ev| match_pattern(pv, ev))
                .unwrap_or(false)
        }),
        (Value::Array(p), _) => p.iter().any(|pv| match_pattern(pv, event)),
        _ => pattern == event,
    }
}

pub struct EventsHandler {}

impl EventsHandler {
    pub fn new() -> Self {
        Self {}
    }

    fn get_state(&self, account: u64, region: &str) -> EventsState {
        global_state(account, region)
    }

    fn rule_value(r: &Rule) -> Value {
        let mut resp = json!({
            "Name": r.name,
            "Arn": r.arn,
            "State": r.state.read().clone(),
            "Description": r.description,
            "EventBusName": "default",
        });
        if !r.event_pattern.is_empty() {
            resp.as_object_mut().unwrap().insert("EventPattern".into(), json!(r.event_pattern));
        }
        if let Some(ref sched) = r.schedule_expression {
            resp.as_object_mut().unwrap().insert("ScheduleExpression".into(), json!(sched));
        }
        resp
    }

    fn json_stub(&self, _req: &AwsRequest, field: &str) -> AwsResponse {
        AwsResponse::json(200, json!({ field: "" }))
    }

    fn json_stub_list(&self, _req: &AwsRequest, field: &str) -> AwsResponse {
        AwsResponse::json(200, json!({ field: [] }))
    }

    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        let op = req.operation.as_str();
        match op {
            "PutRule" => self.put_rule(&req),
            "GetRule" => self.get_rule(&req),
            "DeleteRule" => self.delete_rule(&req),
            "CreateConnection" => self.json_stub(&req, "CreateConnection"),
            "DescribeConnection" => self.json_stub(&req, "DescribeConnection"),
            "UpdateConnection" => self.json_stub(&req, "UpdateConnection"),
            "DeleteConnection" => self.json_stub(&req, "DeleteConnection"),
            "ListConnections" => self.json_stub_list(&req, "Connections"),
            "CreateApiDestination" => self.json_stub(&req, "CreateApiDestination"),
            "DescribeApiDestination" => self.json_stub(&req, "DescribeApiDestination"),
            "UpdateApiDestination" => self.json_stub(&req, "UpdateApiDestination"),
            "DeleteApiDestination" => self.json_stub(&req, "DeleteApiDestination"),
            "ListApiDestinations" => self.json_stub_list(&req, "ApiDestinations"),
            "PutPartnerEventsSource" => AwsResponse::json(200, json!({
                "FailedEntryCount": 0
            })),
            "ListPartnerEventsSources" => self.json_stub_list(&req, "PartnerEventsSources"),
            "ListRules" => self.list_rules(&req),
            "PutTargets" => self.put_targets(&req),
            "RemoveTargets" => self.remove_targets(&req),
            "ListRuleNamesByTarget" => self.json_stub_list(&req, "RuleNames"),
            "ListTargetsByRule" => self.list_targets_by_rule(&req),
            "PutEvents" => self.put_events(&req),
            "TestEventPattern" => self.test_event_pattern(&req),
            "CreateEventBus" => self.create_event_bus(&req),
            "DeleteEventBus" => self.delete_event_bus(&req),
            "ListEventBuses" => self.list_event_buses(&req),
            "ActivateEventSource" => self.json_stub(&req, "{}"),
            "DeactivateEventSource" => self.json_stub(&req, "{}"),
            "TestEventConnection" => self.json_stub(&req, "{}"),
            "CreateEndpoint" => self.json_stub(&req, "EndpointArn"),
            "DeleteEndpoint" => self.json_stub(&req, "{}"),
            "UpdateEndpoint" => self.json_stub(&req, "EndpointArn"),
            "TestEventSource" => self.json_stub(&req, "{}"),
            "CreatePartnerEventSource" => self.json_stub(&req, "PartnerEventSource"),
            "DeletePartnerEventSource" => self.json_stub(&req, "{}"),
            "ListPartnerEventSourceFactories" => self.json_stub_list(&req, "PartnerEventSourceFactories"),
            "CreateArchive" => self.create_archive(&req),
            "DeleteArchive" => self.json_stub(&req, "{}"),
            "DescribeArchive" => self.describe_archive(&req),
            "ListArchives" => self.list_archives(&req),
            "DescribeEventBus" => self.describe_event_bus(&req),
            "DescribeRule" => self.describe_rule(&req),
            "StartArchive" => self.json_stub(&req, "State"),
            "StopArchive" => self.json_stub(&req, "State"),
            "PutPartnerEvents" => self.json_stub(&req, "{}"),
                "DisableRule" => self.set_rule_state(&req, "DISABLED"),
    "EnableRule" => self.set_rule_state(&req, "ENABLED"),
    "TagResource" => self.tag_resource(&req),
    "UntagResource" => self.untag_resource(&req),
    "ListTagsForResource" => self.list_tags_for_resource(&req),
other => AwsResponse::error(400, "ValidationException",
                &format!("The operation {} is not implemented", other)),
        }
    }

    fn put_rule(&self, req: &AwsRequest) -> AwsResponse {
        let name = req.params.get("Name").and_then(|v| v.as_str()).unwrap_or_default().to_string();
        if name.is_empty() {
            return AwsResponse::error(400, "ValidationException", "Name is required");
        }
        let state = self.get_state(req.account, &req.region);
        let rule = Arc::new(Rule {
            name: name.clone(),
            arn: format!("arn:aws:events:{}:{}:rule/{}", req.region, req.account, name),
            description: req.params.get("Description").and_then(|v| v.as_str()).unwrap_or("").to_string(),
            event_pattern: req.params.get("EventPattern").and_then(|v| v.as_str()).unwrap_or("{}").to_string(),
            state: parking_lot::RwLock::new("ENABLED".to_string()),
            role_arn: req.params.get("RoleArn").and_then(|v| v.as_str()).map(String::from),
            schedule_expression: req.params.get("ScheduleExpression").and_then(|v| v.as_str()).map(String::from),
            created: chrono::Utc::now().timestamp() as u64,
            targets: RwLock::new(Vec::new()),
            tags: RwLock::new(Vec::new()),
            inputs: RwLock::new(Vec::new()),
        });
        state.rules.write().insert(name.clone(), rule.clone());
        AwsResponse::json(200, json!({ "RuleArn": rule.arn }))
    }

    fn get_rule(&self, req: &AwsRequest) -> AwsResponse {
        let name = req.params.get("Name").and_then(|v| v.as_str()).unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        match state.get_rule(name) {
            Some(rule) => AwsResponse::json(200, Self::rule_value(&rule)),
            None => AwsResponse::error(404, "ResourceNotFoundException",
                &format!("Rule not found: {}", name)),
        }
    }

    fn delete_rule(&self, req: &AwsRequest) -> AwsResponse {
        let name = req.params.get("Name").and_then(|v| v.as_str()).unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        if state.rules.write().remove(name).is_none() {
            return AwsResponse::error(404, "ResourceNotFoundException",
                &format!("Rule not found: {}", name));
        }
        AwsResponse::json(200, json!({}))
    }

    fn list_rules(&self, req: &AwsRequest) -> AwsResponse {
        let name_prefix = req.params.get("NamePrefix").and_then(|v| v.as_str()).unwrap_or("");
        let state = self.get_state(req.account, &req.region);
        let rules: Vec<Value> = state.rules.read().values()
            .filter(|r| r.name.starts_with(name_prefix))
            .map(|r| Self::rule_value(r.as_ref()))
            .collect::<Vec<Value>>();
        AwsResponse::json(200, json!({ "Rules": rules }))
    }

    fn put_targets(&self, req: &AwsRequest) -> AwsResponse {
        let rule_name = req.params.get("Rule").and_then(|v| v.as_str()).unwrap_or_default();
        let targets: Vec<Value> = req.params.get("Targets")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        if let Some(rule) = state.get_rule(rule_name) {
            let mut existing = rule.targets.write().clone();
            existing.extend(targets);
            *rule.targets.write() = existing;
        }
        AwsResponse::json(200, json!({
            "FailedEntryCount": 0,
            "FailedEntries": []
        }))
    }

    fn remove_targets(&self, req: &AwsRequest) -> AwsResponse {
        let rule_name = req.params.get("Rule").and_then(|v| v.as_str()).unwrap_or_default();
        let ids: Vec<String> = req.params.get("Ids")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        if let Some(rule) = state.get_rule(rule_name) {
            rule.targets.write().retain(|t| {
                t.get("Id").and_then(|i| i.as_str())
                    .map(|i| !ids.contains(&i.to_string()))
                    .unwrap_or(true)
            });
        }
        AwsResponse::json(200, json!({
            "FailedEntryCount": 0,
            "FailedEntries": []
        }))
    }

    fn list_targets_by_rule(&self, req: &AwsRequest) -> AwsResponse {
        let rule_name = req.params.get("Rule").and_then(|v| v.as_str()).unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let targets = state.get_rule(rule_name)
            .map(|r| r.targets.read().clone())
            .unwrap_or_default();
        AwsResponse::json(200, json!({
            "Targets": targets,
            "NextToken": null
        }))
    }

    fn put_events(&self, req: &AwsRequest) -> AwsResponse {
        let entries: Vec<Value> = req.params.get("Entries")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        AwsResponse::json(200, json!({
            "FailedEntryCount": 0,
            "Entries": entries.iter().map(|_| json!({
                "EventId": uuid::Uuid::new_v4().to_string()
            })).collect::<Vec<_>>()
        }))
    }

    fn test_event_pattern(&self, _req: &AwsRequest) -> AwsResponse {
        AwsResponse::json(200, json!({
            "Result": true
        }))
    }

    fn create_event_bus(&self, req: &AwsRequest) -> AwsResponse {
        let name = req.params.get("Name").and_then(|v| v.as_str()).unwrap_or_default().to_string();
        let arn = format!("arn:aws:events:{}:{}:event-bus/{}", req.region, req.account, name);
        let state = self.get_state(req.account, &req.region);
        state.event_buses.write().insert(name.clone(), arn.clone());
        AwsResponse::json(200, json!({ "EventBusName": name, "EventBusArn": arn }))
    }

    fn delete_event_bus(&self, req: &AwsRequest) -> AwsResponse {
        let name = req.params.get("Name").and_then(|v| v.as_str()).unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        if state.event_buses.write().remove(name).is_none() {
            return AwsResponse::error(404, "ResourceNotFoundException",
                &format!("Event bus not found: {}", name));
        }
        AwsResponse::json(200, json!({}))
    }

    fn list_event_buses(&self, _req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(_req.account, &_req.region);
        let buses: Vec<Value> = state.event_buses.read().iter()
            .map(|(name, arn)| json!({
                "Name": name,
                "Arn": arn
            }))
            .collect();
        // Always include the default bus
        let mut all = vec![json!({
            "Name": "default",
            "Arn": format!("arn:aws:events:{}:123456789012:event-bus/default", _req.region)
        })];
        all.extend(buses);
        AwsResponse::json(200, json!({
            "EventBuses": all,
            "NextToken": null
        }))
    }

    fn describe_rule(&self, req: &AwsRequest) -> AwsResponse {
        let name = req.params.get("Name").and_then(|v| v.as_str()).unwrap_or_default().to_string();
        let state = self.get_state(req.account, &req.region);
        let rule = {
            let rules = state.rules.read();
            match rules.get(&name) {
                Some(r) => r.clone(),
                None => {
                    return AwsResponse::error(400, "ResourceNotFoundException",
                        &format!("Rule not found: {}", name));
                }
            }
        };
        let mut resp = json!({
            "Name": rule.name,
            "Arn": rule.arn,
            "State": rule.state.read().clone(),
            "EventBusName": "default",
        });
        if let Some(ref sched) = rule.schedule_expression {
            resp.as_object_mut().unwrap().insert("ScheduleExpression".into(), json!(sched));
        }
        if !rule.event_pattern.is_empty() {
            resp.as_object_mut().unwrap().insert("EventPattern".into(), json!(rule.event_pattern));
        }
        if !rule.description.is_empty() {
            resp.as_object_mut().unwrap().insert("Description".into(), json!(rule.description));
        }
        AwsResponse::json(200, resp)
    }

    fn describe_event_bus(&self, req: &AwsRequest) -> AwsResponse {
        let name = req.params.get("Name").and_then(|v| v.as_str()).unwrap_or("default").to_string();
        let arn = format!("arn:aws:events:{}:{}:event-bus/{}", req.region, req.account, name);
        AwsResponse::json(200, json!({
            "Name": name,
            "Arn": arn
        }))
    }

    fn create_archive(&self, req: &AwsRequest) -> AwsResponse {
        let name = req.params.get("ArchiveName").and_then(|v| v.as_str()).unwrap_or_default().to_string();
        let arn = format!("arn:aws:events:{}:{}:archive/{}", req.region, req.account, name);
        let state = self.get_state(req.account, &req.region);
        let archive = json!({
            "ArchiveName": name,
            "ArchiveArn": arn,
            "EventSourceArn": req.params.get("EventSourceArn").cloned().unwrap_or(Value::Null),
            "Description": req.params.get("Description").cloned().unwrap_or(Value::Null),
            "RetentionDays": req.params.get("RetentionDays").and_then(|v| v.as_u64()).unwrap_or(0),
            "State": "ENABLED",
            "CreationTime": chrono::Utc::now().to_rfc3339()
        });
        state.archives.write().insert(name.clone(), archive.clone());
        AwsResponse::json(200, archive)
    }



    fn describe_archive(&self, req: &AwsRequest) -> AwsResponse {
        let name = req.params.get("ArchiveName").and_then(|v| v.as_str()).unwrap_or_default().to_string();
        let arn = format!("arn:aws:events:{}:{}:archive/{}", req.region, req.account, name);
        AwsResponse::json(200, json!({
            "ArchiveName": name,
            "ArchiveArn": arn,
            "EventSourceArn": format!("arn:aws:events:{}:{}:event-bus/default", req.region, req.account),
            "State": "ENABLED",
            "CreationTime": chrono::Utc::now().to_rfc3339(),
            "RetentionDays": 0
        }))
    }

    fn list_archives(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let archives: Vec<Value> = state.archives.read().values().cloned().collect();
        AwsResponse::json(200, json!({ "Archives": archives }))
    }

    fn set_rule_state(&self, req: &AwsRequest, state: &str) -> AwsResponse {
        let name = req.params.get("Name")
            .and_then(|v| v.as_str()).unwrap_or_default();
        let state_store = self.get_state(req.account, &req.region);
        if let Some(rule) = state_store.get_rule(name) {
            *rule.state.write() = state.to_string();
        }
        AwsResponse::json(200, json!({}))
    }

    fn tag_resource(&self, req: &AwsRequest) -> AwsResponse {
        let arn = req.params.get("ResourceArn").and_then(|v| v.as_str()).unwrap_or_default();
        let tags: Vec<Value> = req.params.get("Tags")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        if let Some(rule) = state.rules.read().values().find(|r| r.arn == arn) {
            let mut existing = rule.tags.write();
            for tag in tags {
                if !existing.iter().any(|t| t.get("Key").and_then(|k| k.as_str()) == tag.get("Key").and_then(|k| k.as_str())) {
                    existing.push(tag);
                }
            }
        }
        AwsResponse::json(200, json!({}))
    }

    fn untag_resource(&self, req: &AwsRequest) -> AwsResponse {
        let arn = req.params.get("ResourceArn").and_then(|v| v.as_str()).unwrap_or_default();
        let keys: Vec<String> = req.params.get("TagKeys")
            .and_then(|v| v.as_array())
            .map(|a| a.iter().filter_map(|k| k.as_str().map(|s| s.to_string())).collect())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        if let Some(rule) = state.rules.read().values().find(|r| r.arn == arn) {
            let mut existing = rule.tags.write();
            existing.retain(|t| {
                let key = t.get("Key").and_then(|k| k.as_str()).unwrap_or_default();
                !keys.contains(&key.to_string())
            });
        }
        AwsResponse::json(200, json!({}))
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> AwsResponse {
        let arn = req.params.get("ResourceArn").and_then(|v| v.as_str()).unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let tags = state.rules.read().values().find(|r| r.arn == arn)
            .map(|r| r.tags.read().clone())
            .unwrap_or_default();
        AwsResponse::json(200, json!({ "Tags": tags }))
    }
}

impl Default for EventsHandler {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use serde_json::json;

    fn make_req(operation: &str, params: Value) -> AwsRequest {
        AwsRequest {
            service: "events".to_string(),
            operation: operation.to_string(),
            account: 123456789012,
            region: "us-east-1".to_string(),
            params,
            body: Bytes::new(),
        }
    }

    #[test]
    fn test_put_and_list_rules() {
        let handler = EventsHandler::new();
        handler.handle(make_req("PutRule", json!({
            "Name": "my-rule",
            "EventPattern": "{}"
        })));
        let resp = handler.handle(make_req("ListRules", json!({})));
        assert_eq!(resp.status, 200);
        assert!(resp.body.contains("my-rule"));
    }

    #[test]
    fn test_put_events() {
        let handler = EventsHandler::new();
        let resp = handler.handle(make_req("PutEvents", json!({
            "Entries": [
                {"EventBusName": "default", "Detail": "{}", "DetailType": "test", "EventSource": "test"},
                {"EventBusName": "default", "Detail": "{}", "DetailType": "test", "EventSource": "test"}
            ]
        })));
        assert_eq!(resp.status, 200);
        let result: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(result["FailedEntryCount"], 0);
    }
}
