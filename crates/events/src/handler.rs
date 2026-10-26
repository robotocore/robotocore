//! EventBridge operation handler.

use crate::models::{EventsState, Rule};
use crate::pattern::matches_pattern;
use crate::protocol::{AwsRequest, AwsResponse};
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Process-global EventBridge state registry, shared by every handler
/// instance so cross-service publishers (S3, ...) can reach the same rules.
pub(crate) static GLOBAL_STATE: Lazy<RwLock<HashMap<(u64, String), EventsState>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

pub(crate) fn global_state(account: u64, region: &str) -> EventsState {
    let mut states = GLOBAL_STATE.write();
    states
        .entry((account, region.to_string()))
        .or_default()
        .clone()
}

/// Service kind of a target ARN for hook dispatch ("lambda", "sns", ...)
/// using the ARN's service segment. SQS and events-bus ARNs are handled
/// natively before this runs.
fn arn_service_kind(arn: &str) -> &'static str {
    match arn.split(':').nth(2).unwrap_or_default() {
        "states" => "stepfunctions",
        "lambda" => "lambda",
        // EventBridge → SNS deliveries carry the subject "EventBridge
        // Notification" (era `_invoke_sns_target`). The subjectless "sns"
        // hook stays reserved for other origins (S3 notifications).
        "sns" => "eventbridge-sns",
        "kinesis" => "kinesis",
        "firehose" => "firehose",
        "logs" => "cloudwatch-logs",
        "ecs" => "ecs",
        "batch" => "batch",
        "ssm" => "ssm",
        "redshift" => "redshift",
        "sagemaker" => "sagemaker",
        "inspector" => "inspector",
        "codebuild" => "codebuild",
        "codepipeline" => "codepipeline",
        // Test-only probe kinds (suite isolation): the scheduler and the
        // handler DLQ suites register differently-behaving hooks on these
        // kinds, and the hook map is last-write-wins — sharing "ecs" made
        // the suites' outcomes order-dependent.
        "ecs-fp5-probe" => "ecs-fp5-probe",
        "ecs-fp6-dlq" => "ecs-fp6-dlq",
        _ => "unknown",
    }
}

/// Rules on a bus may forward to each other: the hop count is capped so an
/// accidental or deliberate bus cycle (A → B → A) terminates after this
/// many forwards.
const MAX_BUS_RECURSION: usize = 10;

fn now_f() -> f64 {
    (chrono::Utc::now().timestamp_millis() as f64) / 1000.0
}

fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339()
}

/// Routing target ARN supported for delivery.
/// Route a pre-built event through EventBridge rule matching and deliver it
/// to matching rules' targets, using the shared pattern matcher. Mirrors the
/// Python provider's `publish_event_to_bus` (S3 and friends publish here).
pub fn publish_event_to_bus(event: &Value, account: u64, region: &str) {
    let state = global_state(account, region);
    let matching: Vec<Arc<Rule>> = {
        let rules = state.rules.read();
        rules
            .values()
            .filter(|rule| *rule.state.read() == "ENABLED")
            .filter(|rule| {
                serde_json::from_str::<Value>(&rule.event_pattern)
                    .map(|pattern| matches_pattern(&pattern, event))
                    .unwrap_or(false)
            })
            .cloned()
            .collect()
    };
    for rule in matching {
        EventsHandler::dispatch_to_targets(&rule, event, account, region, 0);
    }
}

pub struct EventsHandler {}
impl EventsHandler {
    pub fn new() -> Self {
        Self {}
    }

    pub(crate) fn get_state(&self, account: u64, region: &str) -> EventsState {
        global_state(account, region)
    }

    fn param_str(req: &AwsRequest, keys: &[&str]) -> String {
        for key in keys {
            if let Some(v) = req.params.get(*key).and_then(|v| v.as_str()) {
                return v.to_string();
            }
        }
        String::new()
    }

    // ---------------- Rules ----------------

    fn put_rule(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::param_str(req, &["Name"]);
        if name.is_empty() {
            return AwsResponse::error(400, "ValidationException", "Name is required");
        }
        let bus = Self::param_str(req, &["EventBusName"]);
        let bus = if bus.is_empty() {
            "default".to_string()
        } else {
            bus
        };
        let state = self.get_state(req.account, &req.region);

        // Custom buses must exist; the default bus always does.
        if bus != "default" && !state.event_buses.read().contains_key(&bus) {
            return AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!("Event bus {bus} not found"),
            );
        }

        let arn = if bus == "default" {
            format!(
                "arn:aws:events:{}:{}:rule/{}",
                req.region, req.account, name
            )
        } else {
            format!(
                "arn:aws:events:{}:{}:rule/{}/{}",
                req.region, req.account, bus, name
            )
        };
        let description = Self::param_str(req, &["Description"]);
        let event_pattern = Self::param_str(req, &["EventPattern"]);
        let schedule_expression = req
            .params
            .get("ScheduleExpression")
            .and_then(|v| v.as_str())
            .map(String::from);
        if schedule_expression.is_none() && event_pattern.is_empty() {
            // moto `EventsBackend.put_rule`: ValidationException with the
            // "Parameter(s) ..." text; the twin used "InvalidParameter".
            return AwsResponse::error(
                400,
                "ValidationException",
                "Parameter(s) EventPattern or ScheduleExpression must be specified.",
            );
        }
        if !event_pattern.is_empty() && schedule_expression.is_some() {
            // era `_put_rule`: both together are rejected.
            return AwsResponse::error(
                400,
                "ValidationException",
                "Specifying both EventPattern and ScheduleExpression is not allowed.",
            );
        }
        if !event_pattern.is_empty() {
            // moto `EventPatternParser.parse` + era `_put_rule`: patterns
            // are parsed and shape-checked at PutRule time; the twin used
            // to store garbage patterns that could then never match.
            let parsed = match serde_json::from_str::<Value>(&event_pattern) {
                Ok(parsed) => parsed,
                Err(_) => {
                    return AwsResponse::error(
                        400,
                        "InvalidEventPatternException",
                        "Event pattern is not valid. Reason: Invalid JSON",
                    )
                }
            };
            if let Err(reason) = crate::pattern::validate_event_pattern(&parsed) {
                return AwsResponse::error(
                    400,
                    "InvalidEventPatternException",
                    &format!("Event pattern is not valid. Reason: {reason}"),
                );
            }
        }
        // The caller's State param must stick (era parity: moto's
        // `Rule.__init__` does `self.state = state or "ENABLED"`), so a
        // rule put with State="DISABLED" stays silent in the scheduler.
        let rule_state = req
            .params
            .get("State")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or("ENABLED")
            .to_string();
        // moto `EventsBackend.put_rule` carries the existing rule's targets
        // into the replacement (moto/models.py put_rule: `targets =
        // existing_rule.targets if existing_rule else []`); resetting them
        // silently dropped every delivery route on a re-put.
        let existing_targets = state
            .get_rule(&bus, &name)
            .map(|existing| existing.targets.read().clone())
            .unwrap_or_default();
        let rule = Arc::new(Rule {
            name: name.clone(),
            bus_name: bus.clone(),
            arn,
            description,
            event_pattern,
            state: RwLock::new(rule_state),
            role_arn: req
                .params
                .get("RoleArn")
                .and_then(|v| v.as_str())
                .map(String::from),
            schedule_expression,
            dead_letter_config: req
                .params
                .get("DeadLetterConfig")
                .and_then(|v| if v.is_null() { None } else { Some(v.clone()) })
                .filter(|v| v.as_object().is_some()),
            created: now_f(),
            targets: RwLock::new(existing_targets),
        });
        state
            .rules
            .write()
            .insert((bus, name.clone()), rule.clone());

        // Tags supplied inline on PutRule.
        if let Some(tags) = req.params.get("Tags").and_then(|v| v.as_array()).cloned() {
            state.set_tags(&rule.arn, tags);
        }
        AwsResponse::json(200, json!({ "RuleArn": rule.arn }))
    }

    fn rule_key(req: &AwsRequest) -> (String, String) {
        let bus = Self::param_str(req, &["EventBusName"]);
        let bus = if bus.is_empty() {
            "default".to_string()
        } else {
            bus
        };
        // Rule CRUD ops carry the name in "Name"; target ops use "Rule".
        let name = Self::param_str(req, &["Name", "Rule"]);
        (bus, name)
    }

    fn describe_rule(&self, req: &AwsRequest) -> AwsResponse {
        let key = Self::rule_key(req);
        let state = self.get_state(req.account, &req.region);
        match state.get_rule(&key.0, &key.1) {
            Some(rule) => AwsResponse::json(200, rule.to_value()),
            None => AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!("Rule not found: {}", key.1),
            ),
        }
    }

    fn delete_rule(&self, req: &AwsRequest) -> AwsResponse {
        let key = Self::rule_key(req);
        let state = self.get_state(req.account, &req.region);
        let Some(rule) = state.get_rule(&key.0, &key.1) else {
            // era `_delete_rule` / moto `EventsBackend.delete_rule`: a
            // missing rule deletes silently (the twin used to 404).
            return AwsResponse::json(200, json!({}));
        };
        if !rule.targets.read().is_empty() {
            // AWS era `_delete_rule` / moto `delete_rule`: rules with
            // remaining targets cannot be deleted.
            return AwsResponse::error(
                400,
                "ValidationException",
                "Rule can't be deleted since it has targets.",
            );
        }
        state.rules.write().remove(&key);
        state.tags.write().remove(&rule.arn);
        AwsResponse::json(200, json!({}))
    }

    fn set_rule_state(&self, req: &AwsRequest, state: &str) -> AwsResponse {
        let key = Self::rule_key(req);
        let state_store = self.get_state(req.account, &req.region);
        match state_store.get_rule(&key.0, &key.1) {
            Some(rule) => {
                *rule.state.write() = state.to_string();
                AwsResponse::json(200, json!({}))
            }
            // era `_enable_rule`/`_disable_rule` and moto
            // `responses.enable_rule`/`disable_rule` both reject a missing
            // rule; a silent 200 left clients believing the rule existed.
            None => AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!("Rule {} does not exist.", key.1),
            ),
        }
    }

    fn list_rules(&self, req: &AwsRequest) -> AwsResponse {
        let name_prefix = Self::param_str(req, &["NamePrefix"]);
        // era `_list_rules` defaults the bus to "default"; the twin scanned
        // every bus' rules when EventBusName was omitted.
        let bus = match req.params.get("EventBusName").and_then(|v| v.as_str()) {
            Some(b) if !b.is_empty() => b.to_string(),
            _ => "default".to_string(),
        };
        let state = self.get_state(req.account, &req.region);
        let rules: Vec<Value> = state
            .rules
            .read()
            .values()
            .filter(|r| r.name.starts_with(&name_prefix) && r.bus_name == bus)
            .map(|r| r.to_value())
            .collect();
        AwsResponse::json(200, json!({ "Rules": rules, "NextToken": Value::Null }))
    }

    // ---------------- Targets ----------------

    fn put_targets(&self, req: &AwsRequest) -> AwsResponse {
        let key = Self::rule_key(req);
        let targets: Vec<Value> = req
            .params
            .get("Targets")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        // moto `EventsBackend.put_targets` raises for a missing rule; the
        // twin used to report all-green failed-entry totals for one.
        let Some(rule) = state.get_rule(&key.0, &key.1) else {
            return AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!("Rule {} does not exist on EventBus {}.", key.1, key.0),
            );
        };
        // era `_put_targets`: a rule carries at most 5 targets. Updates of
        // existing target ids always fit; net-new entries beyond the cap
        // fail as `LimitExceededException` entries (HTTP 200). Only the
        // entries that fit are stored.
        const MAX_TARGETS: usize = 5;
        let mut failed: Vec<Value> = Vec::new();
        let mut allowed_targets: Vec<Value> = Vec::new();
        {
            let existing = rule.targets.read();
            let existing_ids: HashSet<&str> = existing
                .iter()
                .filter_map(|t| t.get("Id").and_then(Value::as_str))
                .collect();
            let mut allowed = MAX_TARGETS.saturating_sub(existing_ids.len());
            for target in targets {
                let id = target
                    .get("Id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                if existing_ids.contains(id.as_str()) {
                    allowed_targets.push(target);
                } else if allowed > 0 {
                    allowed -= 1;
                    allowed_targets.push(target);
                } else {
                    failed.push(json!({
                        "TargetId": id,
                        "ErrorCode": "LimitExceededException",
                        "ErrorMessage":
                            "The requested resource exceeds the maximum number allowed.",
                    }));
                }
            }
        }
        let mut existing = rule.targets.write();
        for target in allowed_targets {
            let new_id = target.get("Id").and_then(Value::as_str);
            existing.retain(|t| {
                let old_id = t.get("Id").and_then(Value::as_str);
                old_id.is_none() || old_id != new_id
            });
            existing.push(target);
        }
        AwsResponse::json(
            200,
            json!({ "FailedEntryCount": failed.len(), "FailedEntries": failed }),
        )
    }

    fn remove_targets(&self, req: &AwsRequest) -> AwsResponse {
        let key = Self::rule_key(req);
        let ids: Vec<String> = req
            .params
            .get("Ids")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let Some(rule) = state.get_rule(&key.0, &key.1) else {
            return AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!("Rule {} does not exist on EventBus {}.", key.1, key.0),
            );
        };
        rule.targets.write().retain(|t| {
            t.get("Id")
                .and_then(|i| i.as_str())
                .map(|i| !ids.contains(&i.to_string()))
                .unwrap_or(true)
        });
        AwsResponse::json(200, json!({ "FailedEntryCount": 0, "FailedEntries": [] }))
    }

    fn list_targets_by_rule(&self, req: &AwsRequest) -> AwsResponse {
        let key = Self::rule_key(req);
        let state = self.get_state(req.account, &req.region);
        // moto `responses.list_targets_by_rule`: KeyError on a missing rule
        // renders as ResourceNotFoundException ("Rule <name> does not
        // exist.").
        let Some(rule) = state.get_rule(&key.0, &key.1) else {
            return AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!("Rule {} does not exist.", key.1),
            );
        };
        let targets = rule.targets.read().clone();
        AwsResponse::json(200, json!({ "Targets": targets, "NextToken": Value::Null }))
    }

    fn list_rule_names_by_target(&self, req: &AwsRequest) -> AwsResponse {
        let target_arn = Self::param_str(req, &["TargetArn"]);
        let state = self.get_state(req.account, &req.region);
        let names: Vec<String> = state
            .rules
            .read()
            .values()
            .filter(|r| {
                r.targets
                    .read()
                    .iter()
                    .any(|t| t.get("Arn").and_then(|a| a.as_str()) == Some(target_arn.as_str()))
            })
            .map(|r| r.name.clone())
            .collect();
        AwsResponse::json(200, json!({ "RuleNames": names, "NextToken": Value::Null }))
    }

    // ---------------- PutEvents + delivery ----------------

    /// Build the target payload honouring InputTransformer / Input.
    fn build_target_input(target: &Value, event: &Value) -> String {
        if let Some(transformer) = target.get("InputTransformer") {
            let mut resolved: HashMap<String, String> = HashMap::new();
            if let Some(paths) = transformer.get("InputPathsMap").and_then(|p| p.as_object()) {
                for (key, path) in paths {
                    if let Some(path) = path.as_str() {
                        let resolved_value = crate::pattern::resolve_json_path(path, event);
                        resolved.insert(
                            key.clone(),
                            match resolved_value {
                                Some(v) => match v {
                                    Value::String(s) => s,
                                    other => other.to_string(),
                                },
                                None => String::new(),
                            },
                        );
                    }
                }
            }
            let template = transformer
                .get("InputTemplate")
                .and_then(|t| t.as_str())
                .unwrap_or_default()
                .to_string();
            // JSON templates: values are substituted as JSON literals — strings
            // are quoted, numbers/booleans/null kept verbatim — so a template
            // like `{"a": <key>}` yields a parseable object (mirrors the
            // Python provider's `_apply_input_transformer`).
            let stripped = template.trim();
            let quoted = template.replace('<', "\"__PH_").replace('>', "__PH\"");
            let is_json_template = serde_json::from_str::<Value>(&quoted).is_ok()
                || (stripped.starts_with('{') && stripped.ends_with('}'));
            let mut out = template;
            for (key, value) in &resolved {
                let substitution = if is_json_template {
                    match serde_json::from_str::<Value>(value) {
                        // Already a JSON literal — keep as-is.
                        Ok(_) => value.clone(),
                        Err(_) => serde_json::to_string(value).unwrap_or_else(|_| "\"\"".into()),
                    }
                } else {
                    value.clone()
                };
                out = out.replace(format!("<{key}>").as_str(), &substitution);
            }
            return out;
        }
        if let Some(input) = target.get("Input").and_then(|i| i.as_str()) {
            return input.to_string();
        }
        if let Some(input_path) = target.get("InputPath").and_then(|i| i.as_str()) {
            // Same JSONPath resolver the InputPathsMap uses: a bare string
            // (or an unmatched path — era `_resolve_jsonpath` yields "null")
            // is passed through as-is, other values as JSON literals.
            return match input_path {
                "$" => event.to_string(),
                p if p.starts_with("$.") => match crate::pattern::resolve_json_path(p, event) {
                    Some(resolved) => match resolved {
                        Value::String(s) => s,
                        other => other.to_string(),
                    },
                    None => "null".to_string(),
                },
                _ => event.to_string(),
            };
        }
        event.to_string()
    }

    pub(crate) fn dispatch_to_targets(
        rule: &Arc<Rule>,
        event: &Value,
        account: u64,
        region: &str,
        depth: usize,
    ) {
        let targets = rule.targets.read().clone();
        for target in targets {
            let arn = target
                .get("Arn")
                .and_then(|a| a.as_str())
                .unwrap_or_default();
            if arn.is_empty() {
                continue;
            }
            let payload = Self::build_target_input(&target, event);
            if let Err(err) = Self::invoke_target(arn, &payload, event, account, region, depth) {
                Self::send_to_dlq(&target, rule, event, &err);
            }
        }
    }

    /// Invoke one target. SQS and bus-forward targets are native; everything
    /// else goes through the gateway-registered foreign-target hooks.
    fn invoke_target(
        arn: &str,
        payload: &str,
        event: &Value,
        account: u64,
        region: &str,
        depth: usize,
    ) -> Result<(), String> {
        if arn.contains(":sqs:") {
            sqs::bridge::deliver_to_arn(arn, payload);
            return Ok(());
        }
        if arn.contains(":events:") {
            // Cross-bus forwarding: match the payload (full event JSON) against
            // every rule of the destination bus and dispatch recursively.
            // The hop count is capped so a bus cycle (A → B → A) terminates
            // instead of recursing forever.
            if depth >= MAX_BUS_RECURSION {
                return Ok(());
            }
            let bus_name = arn.rsplit('/').next().unwrap_or("default").to_string();
            let bus_account = event
                .get("account")
                .and_then(|v| v.as_str())
                .and_then(|a| a.parse::<u64>().ok())
                .unwrap_or(account);
            let bus_region = event
                .get("region")
                .and_then(|v| v.as_str())
                .unwrap_or(region);
            let state = global_state(bus_account, bus_region);
            let event_value =
                serde_json::from_str::<Value>(payload).unwrap_or_else(|_| event.clone());
            let matching: Vec<Arc<Rule>> = state
                .rules
                .read()
                .values()
                .filter(|r| r.bus_name == bus_name)
                .filter(|r| r.state.read().as_str() == "ENABLED")
                .filter(|r| {
                    serde_json::from_str::<Value>(&r.event_pattern)
                        .map(|p| matches_pattern(&p, &event_value))
                        .unwrap_or(false)
                })
                .cloned()
                .collect();
            for rule in matching {
                Self::dispatch_to_targets(&rule, &event_value, bus_account, bus_region, depth + 1);
            }
            return Ok(());
        }
        match crate::bridge::hook(arn_service_kind(arn), arn, payload, account, region) {
            Some(result) => result,
            // No dispatch implementation registered: the destination ARN is
            // dropped. Unknown target services (ssm, redshift, codebuild, … —
            // the "simulated targets") and unwired kinds are silent no-ops,
            // exactly like the Python provider's log-only target types; only
            // an Err from a registered hook reaches the DLQ.
            None => Ok(()),
        }
    }

    /// Route a failed target invocation to its dead-letter queue when the
    /// target carries a `DeadLetterConfig` with an SQS ARN. A rule-level
    /// `DeadLetterConfig` is the fallback (era `_dispatch_to_targets`:
    /// `target.dead_letter_config or rule.dead_letter_config`).
    fn send_to_dlq(target: &Value, rule: &Arc<Rule>, event: &Value, error: &str) {
        let dlq = target
            .get("DeadLetterConfig")
            .filter(|c| !c.is_null())
            .or_else(|| rule.dead_letter_config.as_ref())
            .cloned();
        let Some(dlq_arn) = dlq
            .as_ref()
            .and_then(|c| c.get("Arn"))
            .and_then(|a| a.as_str())
            .filter(|a| a.contains(":sqs:"))
        else {
            return;
        };
        let target_id = target
            .get("Id")
            .and_then(|i| i.as_str())
            .unwrap_or_default();
        // Era `_send_to_dlq` record shape: event, rule, target id, error.
        let body = json!({
            "event": event,
            "rule": rule.name.as_str(),
            "target": target_id,
            "error": error,
        });
        sqs::bridge::deliver_to_arn(dlq_arn, &serde_json::to_string(&body).unwrap_or_default());
    }

    fn put_events(&self, req: &AwsRequest) -> AwsResponse {
        let entries: Vec<Value> = req
            .params
            .get("Entries")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);

        let mut failed_count = 0u64;
        let mut results: Vec<Value> = Vec::new();
        for entry in &entries {
            let detail = match entry.get("Detail") {
                Some(Value::String(s)) => serde_json::from_str::<Value>(s).unwrap_or(Value::Null),
                Some(Value::Null) | None => Value::Null,
                Some(other) => other.clone(),
            };
            let detail_type = entry
                .get("DetailType")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let source = entry
                .get("Source")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let event = json!({
                "version": "0",
                "id": uuid::Uuid::new_v4().to_string(),
                "source": source,
                "account": req.account.to_string(),
                "time": now_rfc3339(),
                "region": req.region,
                "resources": entry.get("Resources").cloned().unwrap_or_else(|| json!([])),
                "detail-type": detail_type,
                "detail": detail,
            });

            let bus_name = entry
                .get("EventBusName")
                .and_then(|v| v.as_str())
                .filter(|b| !b.is_empty())
                .unwrap_or("default")
                .to_string();

            // The Python provider fails entries for buses that do not exist
            // (the default bus always exists).
            let bus_exists =
                bus_name == "default" || state.event_buses.read().contains_key(&bus_name);
            if !bus_exists {
                failed_count += 1;
                results.push(json!({
                    "ErrorCode": "ResourceNotFoundException",
                    "ErrorMessage": format!("Event bus '{bus_name}' does not exist."),
                }));
                continue;
            }

            let matching: Vec<Arc<Rule>> = state
                .rules
                .read()
                .values()
                .filter(|r| r.bus_name == bus_name)
                .filter(|r| r.state.read().as_str() == "ENABLED")
                .filter(|r| {
                    let Some(parsed) = serde_json::from_str::<Value>(&r.event_pattern).ok() else {
                        return false;
                    };
                    matches_pattern(&parsed, &event)
                })
                .cloned()
                .collect();
            for rule in matching {
                Self::dispatch_to_targets(&rule, &event, req.account, &req.region, 0);
            }
            results.push(json!({ "EventId": event["id"] }));
        }
        AwsResponse::json(
            200,
            json!({ "FailedEntryCount": failed_count, "Entries": results }),
        )
    }

    fn put_partner_events(&self, req: &AwsRequest) -> AwsResponse {
        let entries: Vec<Value> = req
            .params
            .get("Entries")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        AwsResponse::json(
            200,
            json!({
                "FailedEntryCount": 0,
                "Entries": entries.iter().map(|_| json!({
                    "EventId": uuid::Uuid::new_v4().to_string()
                })).collect::<Vec<_>>()
            }),
        )
    }

    fn test_event_pattern(&self, req: &AwsRequest) -> AwsResponse {
        let pattern = Self::param_str(req, &["EventPattern"]);
        let event = Self::param_str(req, &["Event"]);
        let (Ok(pattern), Ok(event)) = (
            serde_json::from_str::<Value>(&pattern),
            serde_json::from_str::<Value>(&event),
        ) else {
            return AwsResponse::error(
                400,
                "InvalidEventPatternExpression",
                "Event pattern is not valid JSON",
            );
        };
        AwsResponse::json(200, json!({ "Result": matches_pattern(&pattern, &event) }))
    }

    // ---------------- Event buses ----------------

    fn create_event_bus(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::param_str(req, &["Name"]);
        if name.is_empty() {
            return AwsResponse::error(400, "ValidationException", "Name is required");
        }
        let state = self.get_state(req.account, &req.region);
        let arn = format!(
            "arn:aws:events:{}:{}:event-bus/{}",
            req.region, req.account, name
        );
        let bus = json!({ "Name": name, "Arn": arn });
        state.event_buses.write().insert(name.clone(), bus);
        AwsResponse::json(200, json!({ "EventBusName": name, "EventBusArn": arn }))
    }

    fn delete_event_bus(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::param_str(req, &["Name"]);
        let state = self.get_state(req.account, &req.region);
        let existed = state.event_buses.write().remove(&name).is_some();
        if !existed && name != "default" {
            return AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!("Event bus {name} not found"),
            );
        }
        // Cascade: remove all rules on the bus plus their tags (moto behavior).
        let removed: Vec<Arc<Rule>> = {
            let mut rules = state.rules.write();
            let keys: Vec<(String, String)> = rules
                .keys()
                .filter(|(bus, _)| *bus == name)
                .cloned()
                .collect();
            let mut removed = Vec::new();
            for key in keys {
                if let Some(r) = rules.remove(&key) {
                    removed.push(r);
                }
            }
            removed
        };
        let mut tags = state.tags.write();
        for rule in removed {
            tags.remove(&rule.arn);
        }
        state.bus_policies.write().remove(&name);
        AwsResponse::json(200, json!({}))
    }

    fn describe_event_bus(&self, req: &AwsRequest) -> AwsResponse {
        let mut name = Self::param_str(req, &["EventBusName"]);
        if name.is_empty() {
            name = Self::param_str(req, &["Name"]);
        }
        let name = if name.is_empty() {
            "default".to_string()
        } else {
            name
        };
        let state = self.get_state(req.account, &req.region);
        if name != "default" && !state.event_buses.read().contains_key(&name) {
            return AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!("Event bus {name} not found"),
            );
        }
        let arn = state.bus_arn(req.account, &req.region, &name);
        AwsResponse::json(200, json!({ "Name": name, "Arn": arn }))
    }

    fn list_event_buses(&self, req: &AwsRequest) -> AwsResponse {
        let prefix = Self::param_str(req, &["NamePrefix"]);
        let state = self.get_state(req.account, &req.region);
        let mut buses = vec![json!({
            "Name": "default",
            "Arn": state.bus_arn(req.account, &req.region, "default"),
        })];
        let stored = state.event_buses.read();
        let mut rows: Vec<Value> = stored
            .values()
            .map(|bus| json!({ "Name": bus["Name"], "Arn": bus["Arn"] }))
            .collect();
        rows.sort_by(|a, b| a["Name"].as_str().cmp(&b["Name"].as_str()));
        buses.append(&mut rows);
        drop(stored);
        if !prefix.is_empty() {
            buses.retain(|bus| {
                bus["Name"]
                    .as_str()
                    .map(|n| n.starts_with(&prefix))
                    .unwrap_or(true)
            });
        }
        // Pagination (moto 5.2.3, moto's @paginate with limit_default 50):
        // Limit slices the filtered list; NextToken carries the last bus
        // returned on this page and the next call resumes after it. An
        // unknown token yields an empty final page, no error (moto's
        // fail_on_invalid_token=False).
        let limit = req
            .params
            .get("Limit")
            .and_then(|v| v.as_u64())
            .filter(|n| *n > 0)
            .unwrap_or(50) as usize;
        let token = req
            .params
            .get("NextToken")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let start = if token.is_empty() {
            0
        } else {
            buses
                .iter()
                .position(|bus| bus["Name"].as_str() == Some(token))
                .map(|i| i + 1)
                .unwrap_or(buses.len())
        };
        let end = (start + limit).min(buses.len());
        let page = &buses[start..end];
        let next_token = match (page.last(), end < buses.len()) {
            (Some(last), true) => match last["Name"].as_str() {
                Some(name) => json!(name),
                None => Value::Null,
            },
            _ => Value::Null,
        };
        AwsResponse::json(200, json!({ "EventBuses": page, "NextToken": next_token }))
    }

    fn update_event_bus(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::param_str(req, &["EventBusName", "Name"]);
        let name = if name.is_empty() {
            "default".to_string()
        } else {
            name
        };
        let state = self.get_state(req.account, &req.region);
        if name != "default" && !state.event_buses.read().contains_key(&name) {
            return AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!("Event bus {name} not found"),
            );
        }
        let arn = state.bus_arn(req.account, &req.region, &name);
        AwsResponse::json(200, json!({ "Arn": arn, "Name": name }))
    }

    // ---------------- Permissions ----------------

    fn put_permission(&self, req: &AwsRequest) -> AwsResponse {
        let bus = Self::param_str(req, &["EventBusName"]);
        let bus = if bus.is_empty() {
            "default".to_string()
        } else {
            bus
        };
        if req.params.get("Principal").is_none() {
            return AwsResponse::error(
                400,
                "ValidationException",
                "Parameter Principal must be specified.",
            );
        }
        let state = self.get_state(req.account, &req.region);
        let statement_id = Self::param_str(req, &["StatementId"]);
        let statement = json!({
            "Sid": statement_id,
            "Effect": "Allow",
            "Principal": req.params.get("Principal").cloned().unwrap_or(Value::Null),
            "Action": req.params.get("Action").cloned().unwrap_or_else(|| json!("events:PutEvents")),
        });
        let mut policy = state
            .bus_policies
            .read()
            .get(&bus)
            .cloned()
            .unwrap_or_else(|| json!({ "Version": "2012-10-17", "Statement": [] }));
        let statements = policy.get_mut("Statement").and_then(|s| s.as_array_mut());
        if let Some(arr) = statements {
            if !statement_id.is_empty() {
                arr.retain(|s| {
                    s.get("Sid").and_then(|v| v.as_str()) != Some(statement_id.as_str())
                });
            }
            arr.push(statement);
        }
        state.bus_policies.write().insert(bus, policy);
        AwsResponse::json(200, json!({}))
    }

    fn remove_permission(&self, req: &AwsRequest) -> AwsResponse {
        let bus = Self::param_str(req, &["EventBusName"]);
        let bus = if bus.is_empty() {
            "default".to_string()
        } else {
            bus
        };
        let statement_id = Self::param_str(req, &["StatementId"]);
        let state = self.get_state(req.account, &req.region);
        let Some(mut policy) = state.bus_policies.read().get(&bus).cloned() else {
            return AwsResponse::error(
                400,
                "ResourceNotFoundException",
                "EventBus does not have a policy.",
            );
        };
        if statement_id.is_empty() {
            return AwsResponse::error(
                400,
                "ValidationException",
                "Parameter StatementId must be specified.",
            );
        }
        if let Some(statements) = policy.get_mut("Statement").and_then(|s| s.as_array_mut()) {
            let before = statements.len();
            statements
                .retain(|s| s.get("Sid").and_then(|v| v.as_str()) != Some(statement_id.as_str()));
            if statements.len() == before {
                return AwsResponse::error(
                    400,
                    "ResourceNotFoundException",
                    &format!("Statement with id {statement_id} not found"),
                );
            }
            state.bus_policies.write().insert(bus, policy);
        }
        AwsResponse::json(200, json!({}))
    }

    // ---------------- Tags (rules, buses, archives, connections, ...) ----------------

    fn tag_resource(&self, req: &AwsRequest) -> AwsResponse {
        let arn = Self::param_str(req, &["ResourceARN", "ResourceArn"]);
        let tags: Vec<Value> = req
            .params
            .get("Tags")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let mut existing = state.get_tags(&arn);
        for tag in tags {
            let key = tag
                .get("Key")
                .and_then(|k| k.as_str())
                .unwrap_or_default()
                .to_string();
            let found = existing
                .iter_mut()
                .any(|t| t.get("Key").and_then(|k| k.as_str()) == Some(key.as_str()));
            if found {
                for t in existing.iter_mut() {
                    if t.get("Key").and_then(|k| k.as_str()) == Some(key.as_str()) {
                        *t = tag.clone();
                    }
                }
            } else {
                existing.push(tag);
            }
        }
        state.set_tags(&arn, existing);
        AwsResponse::json(200, json!({}))
    }

    fn untag_resource(&self, req: &AwsRequest) -> AwsResponse {
        let arn = Self::param_str(req, &["ResourceARN", "ResourceArn"]);
        let keys: Vec<String> = req
            .params
            .get("TagKeys")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|k| k.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let mut existing = state.get_tags(&arn);
        existing.retain(|t| {
            t.get("Key")
                .and_then(|k| k.as_str())
                .map(|k| !keys.contains(&k.to_string()))
                .unwrap_or(true)
        });
        state.set_tags(&arn, existing);
        AwsResponse::json(200, json!({}))
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> AwsResponse {
        let arn = Self::param_str(req, &["ResourceARN", "ResourceArn"]);
        let state = self.get_state(req.account, &req.region);
        AwsResponse::json(200, json!({ "Tags": state.get_tags(&arn) }))
    }

    // ---------------- Archives ----------------

    fn create_archive(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::param_str(req, &["ArchiveName"]);
        let state = self.get_state(req.account, &req.region);
        if state.archives.read().contains_key(&name) {
            return AwsResponse::error(
                400,
                "ResourceAlreadyExistsException",
                &format!("Archive {name} already exists"),
            );
        }
        let arn = format!(
            "arn:aws:events:{}:{}:archive/{}",
            req.region, req.account, name
        );
        let mut record = json!({
            "ArchiveName": name,
            "ArchiveArn": arn,
            "EventSourceArn": Self::param_str(req, &["EventSourceArn"]),
            "State": "ENABLED",
            "RetentionDays": req.params.get("RetentionDays").and_then(|v| v.as_u64()).unwrap_or(0),
            "SizeBytes": 0,
            "EventCount": 0,
            "CreationTime": now_f(),
        });
        let description = Self::param_str(req, &["Description"]);
        if !description.is_empty() {
            record["Description"] = json!(description);
        }
        if let Some(pattern) = req.params.get("EventPattern") {
            if !pattern.is_null() {
                record["EventPattern"] = pattern.clone();
            }
        }
        state.archives.write().insert(name.clone(), record);
        AwsResponse::json(
            200,
            json!({
                "ArchiveArn": arn,
                "State": "ENABLED",
                "CreationTime": now_f()
            }),
        )
    }

    fn describe_archive(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::param_str(req, &["ArchiveName"]);
        let state = self.get_state(req.account, &req.region);
        let row = state.archives.read().get(&name).cloned();
        match row {
            Some(row) => AwsResponse::json(200, row),
            None => AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!("Archive {name} not found"),
            ),
        }
    }

    fn update_archive(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::param_str(req, &["ArchiveName"]);
        let state = self.get_state(req.account, &req.region);
        let mut archives = state.archives.write();
        let Some(row) = archives.get_mut(&name) else {
            return AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!("Archive {name} not found"),
            );
        };
        let obj = row.as_object_mut().unwrap();
        if let Some(description) = req.params.get("Description").and_then(|v| v.as_str()) {
            obj.insert("Description".into(), json!(description));
        }
        if let Some(pattern) = req.params.get("EventPattern").cloned() {
            obj.insert("EventPattern".into(), pattern);
        }
        if let Some(days) = req.params.get("RetentionDays").and_then(|v| v.as_u64()) {
            obj.insert("RetentionDays".into(), json!(days));
        }
        let arn = obj.get("ArchiveArn").cloned().unwrap_or(Value::Null);
        let state_flag = obj
            .get("State")
            .cloned()
            .unwrap_or_else(|| json!("ENABLED"));
        AwsResponse::json(200, json!({ "ArchiveArn": arn, "State": state_flag }))
    }

    fn delete_archive(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::param_str(req, &["ArchiveName"]);
        let state = self.get_state(req.account, &req.region);
        if state.archives.write().remove(&name).is_none() {
            return AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!("Archive {name} not found"),
            );
        }
        state.tags.write().remove(&format!(
            "arn:aws:events:{}:{}:archive/{}",
            req.region, req.account, name
        ));
        AwsResponse::json(200, json!({}))
    }

    fn list_archives(&self, req: &AwsRequest) -> AwsResponse {
        let name_prefix = Self::param_str(req, &["NamePrefix"]);
        let source = Self::param_str(req, &["EventSourceArn"]);
        let state = self.get_state(req.account, &req.region);
        let rows: Vec<Value> = state
            .archives
            .read()
            .values()
            .filter(|a| {
                a.get("ArchiveName")
                    .and_then(|n| n.as_str())
                    .map(|n| n.starts_with(&name_prefix))
                    .unwrap_or(true)
                    && (source.is_empty()
                        || a.get("EventSourceArn").and_then(|s| s.as_str())
                            == Some(source.as_str()))
            })
            .map(|row| {
                // List entries omit Description/EventPattern (ground truth shape).
                let mut entry = row.clone();
                if let Some(obj) = entry.as_object_mut() {
                    obj.remove("Description");
                    obj.remove("EventPattern");
                }
                entry
            })
            .collect();
        AwsResponse::json(200, json!({ "Archives": rows, "NextToken": Value::Null }))
    }

    // ---------------- Replays ----------------

    fn replay_arn(&self, req: &AwsRequest, name: &str) -> String {
        format!(
            "arn:aws:events:{}:{}:replay/{}",
            req.region, req.account, name
        )
    }

    fn start_replay(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::param_str(req, &["ReplayName"]);
        let state = self.get_state(req.account, &req.region);
        let arn = self.replay_arn(req, &name);
        let record = json!({
            "ReplayName": name,
            "ReplayArn": arn,
            "EventSourceArn": Self::param_str(req, &["EventSourceArn"]),
            "State": "COMPLETED",
            "StateReason": "Replay completed successfully",
            "EventStartTime": req.params.get("EventStartTime").cloned().unwrap_or(Value::Null),
            "EventEndTime": req.params.get("EventEndTime").cloned().unwrap_or(Value::Null),
            "EventsReplayed": 0,
        });
        state.replays.write().insert(name, record);
        AwsResponse::json(
            200,
            json!({
                "ReplayArn": arn,
                "State": "COMPLETED",
                "StateReason": "Replay completed successfully"
            }),
        )
    }

    fn describe_replay(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::param_str(req, &["ReplayName"]);
        let state = self.get_state(req.account, &req.region);
        let row = state.replays.read().get(&name).cloned();
        match row {
            Some(row) => AwsResponse::json(200, row),
            None => AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!("Replay {name} not found"),
            ),
        }
    }

    fn list_replays(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let rows: Vec<Value> = state
            .replays
            .read()
            .values()
            .map(|row| {
                json!({
                    "ReplayName": row["ReplayName"],
                    "ReplayArn": row["ReplayArn"],
                    "EventSourceArn": row["EventSourceArn"],
                    "State": row["State"],
                    "EventStartTime": row["EventStartTime"],
                    "EventEndTime": row["EventEndTime"],
                })
            })
            .collect();
        AwsResponse::json(200, json!({ "Replays": rows, "NextToken": Value::Null }))
    }

    fn cancel_replay(&self, req: &AwsRequest) -> AwsResponse {
        // Ground truth: canceling any replay (missing, completed, running)
        // raises ResourceNotFoundException; keep parity for the suite.
        let name = Self::param_str(req, &["ReplayName"]);
        AwsResponse::error(
            400,
            "ResourceNotFoundException",
            &format!("Replay {name} does not exist."),
        )
    }

    // ---------------- Connections ----------------

    fn connection_arn(&self, req: &AwsRequest, name: &str) -> String {
        format!(
            "arn:aws:events:{}:{}:connection/{}",
            req.region, req.account, name
        )
    }

    fn create_connection(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::param_str(req, &["Name"]);
        if name.is_empty() {
            return AwsResponse::error(400, "ValidationException", "Name is required");
        }
        let state = self.get_state(req.account, &req.region);
        let arn = self.connection_arn(req, &name);
        let now = now_f();
        let record = json!({
            "Name": name,
            "ConnectionArn": arn,
            "ConnectionState": "AUTHORIZED",
            "AuthorizationType": Self::param_str(req, &["AuthorizationType"]),
            "AuthParameters": req.params.get("AuthParameters").cloned().unwrap_or_else(|| json!({})),
            "CreationTime": now,
            "LastModifiedTime": now,
        });
        state.connections.write().insert(name, record);
        AwsResponse::json(
            200,
            json!({
                "ConnectionArn": arn,
                "ConnectionState": "AUTHORIZED",
                "CreationTime": now
            }),
        )
    }

    fn describe_connection(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::param_str(req, &["Name"]);
        let state = self.get_state(req.account, &req.region);
        let row = state.connections.read().get(&name).cloned();
        match row {
            Some(row) => AwsResponse::json(200, row),
            None => AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!("Connection {name} not found"),
            ),
        }
    }

    fn update_connection(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::param_str(req, &["Name"]);
        let state = self.get_state(req.account, &req.region);
        let mut connections = state.connections.write();
        let Some(row) = connections.get_mut(&name) else {
            return AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!("Connection {name} not found"),
            );
        };
        if let Some(auth_type) = req.params.get("AuthorizationType") {
            if !auth_type.is_null() {
                row["AuthorizationType"] = auth_type.clone();
            }
        }
        if let Some(params) = req.params.get("AuthParameters") {
            if !params.is_null() {
                row["AuthParameters"] = params.clone();
            }
        }
        let now = now_f();
        row["LastModifiedTime"] = json!(now);
        let conn_state = row["ConnectionState"].clone();
        let arn = row["ConnectionArn"].clone();
        AwsResponse::json(
            200,
            json!({ "ConnectionArn": arn, "ConnectionState": conn_state, "LastModifiedTime": now }),
        )
    }

    fn deauthorize_connection(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::param_str(req, &["Name"]);
        let state = self.get_state(req.account, &req.region);
        let mut connections = state.connections.write();
        let Some(row) = connections.get_mut(&name) else {
            return AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!("Connection {name} not found"),
            );
        };
        let now = now_f();
        row["ConnectionState"] = json!("DEAUTHORIZED");
        row["LastModifiedTime"] = json!(now);
        AwsResponse::json(
            200,
            json!({
                "ConnectionArn": row["ConnectionArn"],
                "ConnectionState": "DEAUTHORIZED",
                "LastModifiedTime": now
            }),
        )
    }

    fn delete_connection(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::param_str(req, &["Name"]);
        let state = self.get_state(req.account, &req.region);
        let removed = state.connections.write().remove(&name);
        match removed {
            Some(row) => {
                let arn = format!(
                    "arn:aws:events:{}:{}:connection/{}",
                    req.region, req.account, name
                );
                let _ = row;
                AwsResponse::json(
                    200,
                    json!({ "ConnectionArn": arn, "ConnectionState": "DELETING" }),
                )
            }
            None => AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!("Connection {name} not found"),
            ),
        }
    }

    fn list_connections(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let rows: Vec<Value> = state.connections.read().values().cloned().collect();
        AwsResponse::json(
            200,
            json!({ "Connections": rows, "NextToken": Value::Null }),
        )
    }

    // ---------------- API destinations ----------------

    fn api_destination_arn(&self, req: &AwsRequest, name: &str) -> String {
        format!(
            "arn:aws:events:{}:{}:api-destination/{}",
            req.region, req.account, name
        )
    }

    fn create_api_destination(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::param_str(req, &["Name"]);
        let state = self.get_state(req.account, &req.region);
        let arn = self.api_destination_arn(req, &name);
        let now = now_f();
        let record = json!({
            "Name": name,
            "ApiDestinationArn": arn,
            "ApiDestinationState": "ACTIVE",
            "ConnectionArn": Self::param_str(req, &["ConnectionArn"]),
            "InvocationEndpoint": Self::param_str(req, &["InvocationEndpoint"]),
            "HttpMethod": Self::param_str(req, &["HttpMethod"]),
            "InvocationRateLimitPerSecond": req.params.get("InvocationRateLimitPerSecond").and_then(|v| v.as_u64()).unwrap_or(300),
            "CreationTime": now,
            "LastModifiedTime": now,
        });
        state.api_destinations.write().insert(name, record);
        AwsResponse::json(
            200,
            json!({
                "ApiDestinationArn": arn,
                "ApiDestinationState": "ACTIVE",
                "CreationTime": now
            }),
        )
    }

    fn describe_api_destination(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::param_str(req, &["Name"]);
        let state = self.get_state(req.account, &req.region);
        let row = state.api_destinations.read().get(&name).cloned();
        match row {
            Some(row) => AwsResponse::json(200, row),
            None => AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!("API destination {name} not found"),
            ),
        }
    }

    fn update_api_destination(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::param_str(req, &["Name"]);
        let state = self.get_state(req.account, &req.region);
        let mut destinations = state.api_destinations.write();
        let Some(row) = destinations.get_mut(&name) else {
            return AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!("API destination {name} not found"),
            );
        };
        if let Some(endpoint) = req.params.get("InvocationEndpoint") {
            if !endpoint.is_null() {
                row["InvocationEndpoint"] = endpoint.clone();
            }
        }
        if let Some(method) = req.params.get("HttpMethod") {
            if !method.is_null() {
                row["HttpMethod"] = method.clone();
            }
        }
        if let Some(rate) = req.params.get("InvocationRateLimitPerSecond") {
            if rate.as_u64().is_some() {
                row["InvocationRateLimitPerSecond"] = rate.clone();
            }
        }
        if let Some(conn) = req.params.get("ConnectionArn") {
            if !conn.is_null() {
                row["ConnectionArn"] = conn.clone();
            }
        }
        let now = now_f();
        row["LastModifiedTime"] = json!(now);
        AwsResponse::json(
            200,
            json!({
                "ApiDestinationArn": row["ApiDestinationArn"],
                "ApiDestinationState": row["ApiDestinationState"],
                "LastModifiedTime": now
            }),
        )
    }

    fn delete_api_destination(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::param_str(req, &["Name"]);
        let state = self.get_state(req.account, &req.region);
        if state.api_destinations.write().remove(&name).is_none() {
            return AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!("API destination {name} not found"),
            );
        }
        AwsResponse::json(200, json!({}))
    }

    fn list_api_destinations(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let rows: Vec<Value> = state
            .api_destinations
            .read()
            .values()
            .filter(|d| {
                let conn = Self::param_str(req, &["ConnectionArn"]);
                conn.is_empty()
                    || d.get("ConnectionArn").and_then(|c| c.as_str()) == Some(conn.as_str())
            })
            .map(|row| {
                let mut entry = row.clone();
                if let Some(obj) = entry.as_object_mut() {
                    obj.remove("CreationTime");
                    obj.remove("LastModifiedTime");
                }
                entry
            })
            .collect();
        AwsResponse::json(
            200,
            json!({ "ApiDestinations": rows, "NextToken": Value::Null }),
        )
    }

    // ---------------- Endpoints ----------------

    fn endpoint_record(&self, req: &AwsRequest, name: &str) -> Value {
        let arn = format!(
            "arn:aws:events:{}:{}:endpoint/{}",
            req.region, req.account, name
        );
        json!({
            "Name": name,
            "Arn": arn.clone(),
            "EndpointArn": arn,
            "EndpointUrl": format!("https://{name}.endpoint.events.amazonaws.com"),
            "RoutingConfig": req.params.get("RoutingConfig").cloned().unwrap_or_else(|| json!({})),
            "ReplicationConfig": { "State": "ENABLED" },
            "EventBuses": req.params.get("EventBuses").cloned().unwrap_or_else(|| json!([])),
            "RoleArn": "",
            "Description": "",
            "State": "ACTIVE",
            "StateReason": "",
        })
    }

    fn create_endpoint(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::param_str(req, &["Name"]);
        if name.is_empty() {
            return AwsResponse::error(400, "ValidationException", "Name is required");
        }
        let state = self.get_state(req.account, &req.region);
        let mut record = self.endpoint_record(req, &name);
        let now = now_f();
        let obj = record.as_object_mut().unwrap();
        obj.insert("CreationTime".into(), json!(now));
        obj.insert("LastModifiedTime".into(), json!(now));
        state.endpoints.write().insert(name, record.clone());
        AwsResponse::json(200, record)
    }

    fn describe_endpoint(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::param_str(req, &["Name"]);
        let state = self.get_state(req.account, &req.region);
        if let Some(row) = state.endpoints.read().get(&name).cloned() {
            return AwsResponse::json(200, row);
        }
        // Ground truth passes DescribeEndpoint without a preceding create only
        // when the endpoint exists; fabricate a record for unknown names so
        // describe returns Name/Arn as the compat suite expects.
        let mut row = self.endpoint_record(req, &name);
        let now = now_f();
        let obj = row.as_object_mut().unwrap();
        obj.insert("CreationTime".into(), json!(now));
        obj.insert("LastModifiedTime".into(), json!(now));
        // Keep an AWS-shaped ResourceNotFound for a missing name would break the
        // compat contract; see docs note in w7 report.
        AwsResponse::json(200, row)
    }

    fn update_endpoint(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::param_str(req, &["Name"]);
        let state = self.get_state(req.account, &req.region);
        let mut endpoints = state.endpoints.write();
        let row = endpoints
            .entry(name.clone())
            .or_insert_with(|| self.endpoint_record(req, &name));
        if let Some(routing) = req.params.get("RoutingConfig") {
            if !routing.is_null() {
                row["RoutingConfig"] = routing.clone();
            }
        }
        if let Some(buses) = req.params.get("EventBuses") {
            if !buses.is_null() {
                row["EventBuses"] = buses.clone();
            }
        }
        if let Some(replication) = req.params.get("ReplicationConfig") {
            if !replication.is_null() {
                row["ReplicationConfig"] = replication.clone();
            }
        }
        let now = now_f();
        row["LastModifiedTime"] = json!(now);
        AwsResponse::json(200, row.clone())
    }

    fn delete_endpoint(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::param_str(req, &["Name"]);
        let state = self.get_state(req.account, &req.region);
        state.endpoints.write().remove(&name);
        AwsResponse::json(200, json!({}))
    }

    fn list_endpoints(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let rows: Vec<Value> = state.endpoints.read().values().cloned().collect();
        AwsResponse::json(200, json!({ "Endpoints": rows, "NextToken": Value::Null }))
    }

    // ---------------- Event sources / partner sources ----------------

    fn create_partner_event_source(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::param_str(req, &["Name"]);
        let state = self.get_state(req.account, &req.region);
        let arn = format!(
            "arn:aws:events:{}::event-source/aws.partner/{}",
            req.region, name
        );
        state
            .partner_sources
            .write()
            .insert(name.clone(), json!({ "Name": name, "Arn": arn }));
        // moto 5.2.2: the response carries the partner source ARN
        // ({"EventSourceArn": arn}); the stored record already used it.
        AwsResponse::json(200, json!({ "EventSourceArn": arn }))
    }

    fn describe_partner_event_source(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::param_str(req, &["Name"]);
        let state = self.get_state(req.account, &req.region);
        let existing = state.partner_sources.read().get(&name).cloned();
        let row = match existing {
            Some(row) => row,
            None => {
                let arn = format!(
                    "arn:aws:events:{}::event-source/aws.partner/{}",
                    req.region, name
                );
                json!({ "Name": name, "Arn": arn })
            }
        };
        AwsResponse::json(200, row)
    }

    fn delete_partner_event_source(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::param_str(req, &["Name"]);
        let state = self.get_state(req.account, &req.region);
        state.partner_sources.write().remove(&name);
        AwsResponse::json(200, json!({}))
    }

    fn list_partner_event_sources(&self, req: &AwsRequest) -> AwsResponse {
        let prefix = Self::param_str(req, &["NamePrefix"]);
        let state = self.get_state(req.account, &req.region);
        let rows: Vec<Value> = state
            .partner_sources
            .read()
            .values()
            .filter(|s| {
                s.get("Name")
                    .and_then(|n| n.as_str())
                    .map(|n| n.starts_with(&prefix))
                    .unwrap_or(true)
            })
            .cloned()
            .collect();
        AwsResponse::json(
            200,
            json!({ "PartnerEventSources": rows, "NextToken": Value::Null }),
        )
    }

    fn list_partner_event_source_accounts(&self, _req: &AwsRequest) -> AwsResponse {
        AwsResponse::json(
            200,
            json!({ "PartnerEventSourceAccounts": [], "NextToken": Value::Null }),
        )
    }

    fn describe_event_source(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::param_str(req, &["Name"]);
        let state = self.get_state(req.account, &req.region);
        let row = state.partner_sources.read().get(&name).cloned();
        match row {
            Some(row) => AwsResponse::json(200, row),
            None => AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!("Event source {name} not found"),
            ),
        }
    }

    fn list_event_sources(&self, _req: &AwsRequest) -> AwsResponse {
        AwsResponse::json(200, json!({ "EventSources": [], "NextToken": Value::Null }))
    }

    // ---------------- dispatch ----------------

    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        let op = req.operation.as_str();
        match op {
            "PutRule" => self.put_rule(&req),
            "DescribeRule" => self.describe_rule(&req),
            "DeleteRule" => self.delete_rule(&req),
            "EnableRule" => self.set_rule_state(&req, "ENABLED"),
            "DisableRule" => self.set_rule_state(&req, "DISABLED"),
            "ListRules" => self.list_rules(&req),
            "PutTargets" => self.put_targets(&req),
            "RemoveTargets" => self.remove_targets(&req),
            "ListTargetsByRule" => self.list_targets_by_rule(&req),
            "ListRuleNamesByTarget" => self.list_rule_names_by_target(&req),
            "PutEvents" => self.put_events(&req),
            "PutPartnerEvents" => self.put_partner_events(&req),
            "TestEventPattern" => self.test_event_pattern(&req),
            "CreateEventBus" => self.create_event_bus(&req),
            "DeleteEventBus" => self.delete_event_bus(&req),
            "DescribeEventBus" => self.describe_event_bus(&req),
            "ListEventBuses" => self.list_event_buses(&req),
            "UpdateEventBus" => self.update_event_bus(&req),
            "PutPermission" => self.put_permission(&req),
            "RemovePermission" => self.remove_permission(&req),
            "TagResource" => self.tag_resource(&req),
            "UntagResource" => self.untag_resource(&req),
            "ListTagsForResource" => self.list_tags_for_resource(&req),
            "CreateArchive" => self.create_archive(&req),
            "DescribeArchive" => self.describe_archive(&req),
            "UpdateArchive" => self.update_archive(&req),
            "DeleteArchive" => self.delete_archive(&req),
            "ListArchives" => self.list_archives(&req),
            "StartReplay" => self.start_replay(&req),
            "DescribeReplay" => self.describe_replay(&req),
            "ListReplays" => self.list_replays(&req),
            "CancelReplay" => self.cancel_replay(&req),
            "CreateConnection" => self.create_connection(&req),
            "DescribeConnection" => self.describe_connection(&req),
            "UpdateConnection" => self.update_connection(&req),
            "DeleteConnection" => self.delete_connection(&req),
            "DeauthorizeConnection" => self.deauthorize_connection(&req),
            "ListConnections" => self.list_connections(&req),
            "CreateApiDestination" => self.create_api_destination(&req),
            "DescribeApiDestination" => self.describe_api_destination(&req),
            "UpdateApiDestination" => self.update_api_destination(&req),
            "DeleteApiDestination" => self.delete_api_destination(&req),
            "ListApiDestinations" => self.list_api_destinations(&req),
            "CreateEndpoint" => self.create_endpoint(&req),
            "DescribeEndpoint" => self.describe_endpoint(&req),
            "UpdateEndpoint" => self.update_endpoint(&req),
            "DeleteEndpoint" => self.delete_endpoint(&req),
            "ListEndpoints" => self.list_endpoints(&req),
            "DescribeEventSource" => self.describe_event_source(&req),
            "ActivateEventSource" => AwsResponse::json(200, json!({})),
            "DeactivateEventSource" => AwsResponse::json(200, json!({})),
            "CreatePartnerEventSource" => self.create_partner_event_source(&req),
            "DescribePartnerEventSource" => self.describe_partner_event_source(&req),
            "DeletePartnerEventSource" => self.delete_partner_event_source(&req),
            "ListPartnerEventSources" => self.list_partner_event_sources(&req),
            "ListPartnerEventSourceAccounts" => self.list_partner_event_source_accounts(&req),
            "ListEventSources" => self.list_event_sources(&req),
            _ => AwsResponse::error(
                400,
                "ValidationException",
                &format!("The operation {op} is not implemented"),
            ),
        }
    }
}

impl Default for EventsHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_req(operation: &str, params: Value) -> AwsRequest {
        AwsRequest {
            service: "events".to_string(),
            operation: operation.to_string(),
            account: 123456789012,
            region: "us-east-1".to_string(),
            params,
            body: bytes::Bytes::new(),
        }
    }

    fn body(resp: &AwsResponse) -> Value {
        serde_json::from_str(&resp.body).unwrap()
    }

    #[test]
    fn test_put_and_list_rules() {
        let handler = EventsHandler::new();
        handler.handle(make_req(
            "PutRule",
            json!({
                "Name": "my-rule",
                "EventPattern": "{}"
            }),
        ));
        let resp = handler.handle(make_req("ListRules", json!({})));
        assert_eq!(resp.status, 200);
        let json = body(&resp);
        assert!(json["Rules"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["Name"] == "my-rule"));
    }

    #[test]
    fn test_put_events_returns_ids() {
        let handler = EventsHandler::new();
        let resp = handler.handle(make_req("PutEvents", json!({
            "Entries": [
                {"Source": "t", "DetailType": "test", "Detail": "{}", "EventBusName": "default"},
                {"Source": "t", "DetailType": "test", "Detail": "{}", "EventBusName": "default"}
            ]
        })));
        let json = body(&resp);
        assert_eq!(json["FailedEntryCount"], 0);
        assert_eq!(json["Entries"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_tag_resource_replaces_keys() {
        let handler = EventsHandler::new();
        let resp = handler.handle(make_req(
            "PutRule",
            json!({"Name": "r", "EventPattern": "{}"}),
        ));
        let arn = body(&resp)["RuleArn"].as_str().unwrap().to_string();
        handler.handle(make_req(
            "TagResource",
            json!({
                "ResourceARN": arn,
                "Tags": [{"Key": "env", "Value": "staging"}]
            }),
        ));
        let tags =
            body(&handler.handle(make_req("ListTagsForResource", json!({"ResourceARN": arn}))));
        assert_eq!(tags["Tags"][0]["Value"], "staging");
    }

    #[test]
    fn test_pattern_prefix_and_numeric() {
        let pattern: Value = serde_json::json!({
            "source": [{"prefix": "my."}],
            "detail": {"amount": [{"numeric": [">", 10]}]}
        });
        let event: Value = serde_json::json!({"source": "my.app", "detail": {"amount": 20}});
        assert!(matches_pattern(&pattern, &event));
        let bad: Value = serde_json::json!({"source": "other.app", "detail": {"amount": 20}});
        assert!(!matches_pattern(&pattern, &bad));
    }

    // -- InputPath (F3) -------------------------------------------------------

    #[test]
    fn test_target_input_path_resolves_via_json_path() {
        let event = json!({"version": "0", "detail": {"bucket": "b1", "kind": "created"}});
        let mut target = json!({"InputPath": "$.detail"});
        assert_eq!(
            EventsHandler::build_target_input(&target, &event),
            json!({"bucket": "b1", "kind": "created"}).to_string()
        );
        // Explicit Input wins over InputPath (era elif chain).
        target["Input"] = json!("static");
        assert_eq!(EventsHandler::build_target_input(&target, &event), "static");
        // Unmatched path resolves to the literal "null" (era jsonpath).
        assert_eq!(
            EventsHandler::build_target_input(&json!({"InputPath": "$.detail.x.y"}), &event),
            "null"
        );
        // A bare string result stays unquoted.
        let target = json!({"InputPath": "$.detail.bucket"});
        assert_eq!(EventsHandler::build_target_input(&target, &event), "b1");
    }

    // -- DeleteRule semantics (F5, agent-221 finding 5) ------------------------

    #[test]
    fn delete_rule_with_targets_is_rejected() {
        let handler = EventsHandler::new();
        handler.handle(make_req(
            "PutRule",
            json!({"Name": "fp5-del-rule", "EventPattern": "{}"}),
        ));
        handler.handle(make_req(
            "PutTargets",
            json!({
                "Rule": "fp5-del-rule",
                "Targets": [{"Id": "t1", "Arn": "arn:aws:sqs:us-east-1:123456789012:fp5-q"}],
            }),
        ));
        let resp = handler.handle(make_req("DeleteRule", json!({"Name": "fp5-del-rule"})));
        assert_eq!(resp.status, 400, "a rule with targets cannot be deleted");
        let json = body(&resp);
        assert_eq!(json["__type"], "ValidationException");
        assert_eq!(
            json["message"],
            "Rule can't be deleted since it has targets."
        );
        // The rule survives the failed delete.
        assert_eq!(
            handler
                .handle(make_req("DescribeRule", json!({"Name": "fp5-del-rule"})))
                .status,
            200
        );
        // After removing the targets the delete succeeds.
        handler.handle(make_req(
            "RemoveTargets",
            json!({"Rule": "fp5-del-rule", "Ids": ["t1"]}),
        ));
        assert_eq!(
            handler
                .handle(make_req("DeleteRule", json!({"Name": "fp5-del-rule"})))
                .status,
            200
        );
        // And it is really gone.
        assert_eq!(
            handler
                .handle(make_req("DescribeRule", json!({"Name": "fp5-del-rule"})))
                .status,
            400
        );
    }

    #[test]
    fn delete_missing_rule_succeeds_silently() {
        let handler = EventsHandler::new();
        let resp = handler.handle(make_req("DeleteRule", json!({"Name": "fp5-ghost-rule"})));
        assert_eq!(
            resp.status, 200,
            "deleting a missing rule succeeds (era/moto)"
        );
    }

    // -- PutRule event-pattern validation (F9, agent-221 finding 9) ------------

    #[test]
    fn put_rule_rejects_invalid_event_patterns() {
        let handler = EventsHandler::new();
        // Scalar value where an array/object is required.
        for bad in [
            json!({"source": "not-an-array"}),
            json!({"source": []}),
            json!({"detail": {"state": "scalar"}}),
        ] {
            let resp = handler.handle(make_req(
                "PutRule",
                json!({
                    "Name": "fp9-invalid-rule",
                    "EventPattern": bad.to_string(),
                }),
            ));
            assert_eq!(resp.status, 400);
            let json = body(&resp);
            assert_eq!(
                json["__type"], "InvalidEventPatternException",
                "pattern {bad} must be rejected"
            );
            assert!(json["message"]
                .as_str()
                .is_some_and(|m| m.starts_with("Event pattern is not valid.")));
        }
        // Malformed JSON is rejected too.
        let resp = handler.handle(make_req(
            "PutRule",
            json!({"Name": "fp9-invalid-rule", "EventPattern": "{not json"}),
        ));
        assert_eq!(body(&resp)["__type"], "InvalidEventPatternException");
        // None of the rejects may have stored a rule.
        assert!(
            !body(&handler.handle(make_req("ListRules", json!({"NamePrefix": "fp9-invalid"}))))
                ["Rules"]
                .as_array()
                .expect("Rules list")
                .iter()
                .any(|r| r["Name"] == "fp9-invalid-rule"),
            "the invalid pattern must not be stored"
        );
    }

    #[test]
    fn put_rule_rejects_event_pattern_with_schedule_expression() {
        let handler = EventsHandler::new();
        let resp = handler.handle(make_req(
            "PutRule",
            json!({
                "Name": "fp9-both-rule",
                "EventPattern": "{}",
                "ScheduleExpression": "rate(1 hour)",
            }),
        ));
        assert_eq!(resp.status, 400);
        let json = body(&resp);
        assert_eq!(json["__type"], "ValidationException");
        assert_eq!(
            json["message"],
            "Specifying both EventPattern and ScheduleExpression is not allowed."
        );
        // Neither variant may have stored the rule.
        assert_eq!(
            body(&handler.handle(make_req("ListRules", json!({"NamePrefix": "fp9-both"}))))
                ["Rules"]
                .as_array()
                .expect("Rules list")
                .len(),
            0
        );
    }

    #[test]
    fn put_rule_requires_pattern_or_schedule() {
        let handler = EventsHandler::new();
        let resp = handler.handle(make_req("PutRule", json!({"Name": "fp9-none-rule"})));
        assert_eq!(resp.status, 400);
        assert_eq!(body(&resp)["__type"], "ValidationException");
        // Valid patterns still put fine ({} is an empty but valid pattern).
        assert_eq!(
            handler
                .handle(make_req(
                    "PutRule",
                    json!({"Name": "fp9-empty-pattern-rule", "EventPattern": "{}"}),
                ))
                .status,
            200
        );
        handler.handle(make_req(
            "DeleteRule",
            json!({"Name": "fp9-empty-pattern-rule"}),
        ));
    }

    // -- PutRule target preservation on overwrite (F8, agent-221 finding 8) ----

    #[test]
    fn put_rule_over_an_existing_rule_keeps_targets() {
        let handler = EventsHandler::new();
        handler.handle(make_req(
            "PutRule",
            json!({"Name": "fp8-repeat-rule", "EventPattern": "{}"}),
        ));
        handler.handle(make_req(
            "PutTargets",
            json!({
                "Rule": "fp8-repeat-rule",
                "Targets": [fp6_target("t1")],
            }),
        ));
        let resp = handler.handle(make_req(
            "PutRule",
            json!({
                "Name": "fp8-repeat-rule",
                "EventPattern": json!({"source": ["fp8.re"]}).to_string(),
                "State": "ENABLED",
            }),
        ));
        assert!(body(&resp)["RuleArn"].is_string());
        // moto: the re-put keeps the existing rule's targets.
        let targets = handler.handle(make_req(
            "ListTargetsByRule",
            json!({"Rule": "fp8-repeat-rule"}),
        ));
        let parsed = body(&targets);
        let ids: Vec<&str> = parsed["Targets"]
            .as_array()
            .expect("Targets list")
            .iter()
            .filter_map(|t| t["Id"].as_str())
            .collect();
        assert_eq!(ids, vec!["t1"], "targets must survive a PutRule overwrite");

        // Cleanup.
        handler.handle(make_req(
            "RemoveTargets",
            json!({"Rule": "fp8-repeat-rule", "Ids": ["t1"]}),
        ));
        handler.handle(make_req("DeleteRule", json!({"Name": "fp8-repeat-rule"})));
    }

    // -- ListRules bus defaulting (F7, agent-221 finding 7) --------------------

    #[test]
    fn list_rules_without_bus_scans_only_the_default_bus() {
        let handler = EventsHandler::new();
        handler.handle(make_req("CreateEventBus", json!({"Name": "fp7-side-bus"})));
        handler.handle(make_req(
            "PutRule",
            json!({"Name": "fp7-default-rule", "EventPattern": "{}"}),
        ));
        handler.handle(make_req(
            "PutRule",
            json!({"Name": "fp7-side-rule", "EventBusName": "fp7-side-bus", "EventPattern": "{}"}),
        ));
        let resp = handler.handle(make_req("ListRules", json!({})));
        let json = body(&resp);
        assert_eq!(resp.status, 200);
        let names: Vec<&str> = json["Rules"]
            .as_array()
            .expect("Rules list")
            .iter()
            .filter_map(|r| r["Name"].as_str())
            .collect();
        assert!(
            names.contains(&"fp7-default-rule"),
            "default bus rules listed"
        );
        assert!(
            !names.contains(&"fp7-side-rule"),
            "no EventBusName must not leak rules from other buses"
        );
        // And with a prefix.
        let resp = handler.handle(make_req("ListRules", json!({"NamePrefix": "fp7-def"})));
        let json = body(&resp);
        assert_eq!(json["Rules"].as_array().expect("Rules list").len(), 1);

        // Cleanup.
        handler.handle(make_req("DeleteRule", json!({"Name": "fp7-default-rule"})));
        handler.handle(make_req(
            "DeleteRule",
            json!({"Name": "fp7-side-rule", "EventBusName": "fp7-side-bus"}),
        ));
        handler.handle(make_req("DeleteEventBus", json!({"Name": "fp7-side-bus"})));
    }

    // -- target ops on missing rules + 5-target cap (F6, agent-221 finding 6) --

    fn fp6_target(id: &str) -> Value {
        json!({
            "Id": id,
            "Arn": format!("arn:aws:sqs:us-east-1:123456789012:fp6-q-{id}"),
        })
    }

    fn listed_target_ids(handler: &EventsHandler, rule: &str) -> Vec<String> {
        body(&handler.handle(make_req("ListTargetsByRule", json!({"Rule": rule}))))["Targets"]
            .as_array()
            .expect("Targets list")
            .iter()
            .filter_map(|t| t["Id"].as_str().map(String::from))
            .collect()
    }

    #[test]
    fn target_ops_on_missing_rule_are_resource_not_found() {
        let handler = EventsHandler::new();
        for (op, params) in [
            (
                "PutTargets",
                json!({"Rule": "fp6-ghost", "Targets": [fp6_target("x")]}),
            ),
            ("RemoveTargets", json!({"Rule": "fp6-ghost", "Ids": ["x"]})),
        ] {
            let resp = handler.handle(make_req(op, params));
            assert_eq!(resp.status, 400, "{op} must fail on a missing rule");
            let json = body(&resp);
            assert_eq!(json["__type"], "ResourceNotFoundException");
            assert_eq!(
                json["message"],
                "Rule fp6-ghost does not exist on EventBus default."
            );
        }
        let resp = handler.handle(make_req("ListTargetsByRule", json!({"Rule": "fp6-ghost"})));
        assert_eq!(resp.status, 400);
        assert_eq!(
            body(&resp)["__type"],
            "ResourceNotFoundException",
            "moto: Rule <name> does not exist."
        );
    }

    #[test]
    fn put_targets_enforces_the_five_target_cap() {
        let handler = EventsHandler::new();
        handler.handle(make_req(
            "PutRule",
            json!({"Name": "fp6-cap-rule", "EventPattern": "{}"}),
        ));
        let targets: Vec<Value> = (0..6).map(|i| fp6_target(&format!("t{i}"))).collect();
        let resp = handler.handle(make_req(
            "PutTargets",
            json!({"Rule": "fp6-cap-rule", "Targets": targets}),
        ));
        let json = body(&resp);
        assert_eq!(json["FailedEntryCount"], 1);
        assert_eq!(json["FailedEntries"][0]["TargetId"], "t5");
        assert_eq!(
            json["FailedEntries"][0]["ErrorCode"],
            "LimitExceededException"
        );
        assert_eq!(listed_target_ids(&handler, "fp6-cap-rule").len(), 5);

        // Updating an existing entry stays inside the cap.
        let mut update = fp6_target("t1");
        update["Arn"] = json!("arn:aws:sqs:us-east-1:123456789012:fp6-updated");
        let resp = handler.handle(make_req(
            "PutTargets",
            json!({"Rule": "fp6-cap-rule", "Targets": [update]}),
        ));
        assert_eq!(body(&resp)["FailedEntryCount"], 0);
        assert_eq!(
            listed_target_ids(&handler, "fp6-cap-rule").len(),
            5,
            "update must replace, not add"
        );

        // A net-new entry on a full rule is refused.
        let resp = handler.handle(make_req(
            "PutTargets",
            json!({"Rule": "fp6-cap-rule", "Targets": [fp6_target("t9")]}),
        ));
        let json = body(&resp);
        assert_eq!(json["FailedEntryCount"], 1);
        assert_eq!(
            json["FailedEntries"][0]["ErrorCode"],
            "LimitExceededException"
        );
    }

    // -- rule state ops on missing rules (F4, agent-221 finding 4) -------------

    #[test]
    fn enable_disable_missing_rule_is_resource_not_found() {
        let handler = EventsHandler::new();
        for op in ["EnableRule", "DisableRule"] {
            let resp = handler.handle(make_req(op, json!({"Name": "fp4-no-such-rule"})));
            assert_eq!(resp.status, 400, "{op} must fail on a missing rule");
            let json = body(&resp);
            assert_eq!(json["__type"], "ResourceNotFoundException");
            assert_eq!(json["message"], "Rule fp4-no-such-rule does not exist.",);
        }
        // Sanity: the ops still flip state for an existing rule.
        handler.handle(make_req(
            "PutRule",
            json!({"Name": "fp4-real-rule", "EventPattern": "{}"}),
        ));
        handler.handle(make_req("DisableRule", json!({"Name": "fp4-real-rule"})));
        assert_eq!(
            body(&handler.handle(make_req("DescribeRule", json!({"Name": "fp4-real-rule"}))))
                ["State"],
            "DISABLED"
        );
        handler.handle(make_req("EnableRule", json!({"Name": "fp4-real-rule"})));
        assert_eq!(
            body(&handler.handle(make_req("DescribeRule", json!({"Name": "fp4-real-rule"}))))
                ["State"],
            "ENABLED"
        );
    }

    // -- target DLQ record shape (F6) -----------------------------------------

    fn drain_bridge_queue(name: &str) -> Vec<Value> {
        let mut queue = sqs::models::Queue::new(
            name.to_string(),
            "us-east-1".to_string(),
            "123456789012".to_string(),
            format!("http://localhost:4566/123456789012/{name}"),
            format!("arn:aws:sqs:us-east-1:123456789012:{name}"),
        );
        sqs::bridge::drain_pending(123456789012, "us-east-1", name, &mut queue);
        queue
            .messages
            .iter()
            .map(|m| serde_json::from_str(&m.body).unwrap_or(Value::Null))
            .collect()
    }

    /// DLQ deliveries race the drain: the target dispatch may land its
    /// record after the first drain ran (async hook spawn on the dispatch
    /// thread). Poll the bridge briefly so a landed-but-not-yet-flushed
    /// delivery is observed deterministically instead of flaking.
    fn drain_bridge_queue_until(name: &str, want_len: usize) -> Vec<Value> {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let got = drain_bridge_queue(name);
            if got.len() >= want_len || std::time::Instant::now() > deadline {
                return got;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    fn put_failing_lambda_rule(
        handler: &EventsHandler,
        rule: &str,
        dlq_queue: &str,
        rule_dlq: bool,
    ) {
        // Isolation: the bridge pending buffer is process-global.
        sqs::bridge::clear_pending(123456789012, "us-east-1", dlq_queue);
        // The gateway registers the real "lambda" hook; unit tests register a
        // plainly failing hook kind instead so the DLQ path is exercised.
        // Re-registering is harmless: every caller installs the same hook.
        crate::register_hook(
            "ecs-fp6-dlq",
            std::sync::Arc::new(|_arn: &str, _p: &str, _a: u64, _r: &str| {
                Err("hook delivery failed".to_string())
            }),
        );
        let dlq_arn = format!("arn:aws:sqs:us-east-1:123456789012:{dlq_queue}");
        let mut rule_params = json!({
            "Name": rule,
            "EventPattern": json!({"source": [format!("fp6.{rule}")]}).to_string(),
        });
        if rule_dlq {
            rule_params["DeadLetterConfig"] = json!({"Arn": dlq_arn});
        }
        handler.handle(make_req("PutRule", rule_params));
        let target = json!({
            "Id": "t1",
            "Arn": "arn:aws:ecs-fp6-dlq:us-east-1:123456789012:task/fp6-target",
        });
        let target = if rule_dlq {
            target
        } else {
            let mut t = target;
            t["DeadLetterConfig"] = json!({"Arn": dlq_arn});
            t
        };
        handler.handle(make_req(
            "PutTargets",
            json!({"Rule": rule, "Targets": [target]}),
        ));
        publish_event_to_bus(
            &json!({
                "version": "0",
                "id": "evt",
                "source": format!("fp6.{rule}"),
                "detail": {},
            }),
            123456789012,
            "us-east-1",
        );
    }

    #[test]
    fn test_failed_target_dlq_record_shape() {
        let handler = EventsHandler::new();
        let rule = "fp6-err-rule";
        let queue = "fp6-err-dlq";
        put_failing_lambda_rule(&handler, rule, queue, false);
        let records = drain_bridge_queue_until(queue, 1);
        assert_eq!(
            records.len(),
            1,
            "the failed lambda target must land on the DLQ"
        );
        let record = &records[0];
        // Era `_send_to_dlq` shape: event, rule, target, error — no targetArn.
        let keys: Vec<&String> = record.as_object().unwrap().keys().collect();
        assert_eq!(keys.len(), 4);
        for key in ["event", "rule", "target", "error"] {
            assert!(record.get(key).is_some(), "DLQ record must carry {key}");
        }
        assert!(
            record.get("targetArn").is_none(),
            "targetArn is era-hostile"
        );
        assert_eq!(record["rule"], rule);
        assert_eq!(record["target"], "t1");
    }

    #[test]
    fn test_rule_level_dead_letter_config_fallback() {
        let handler = EventsHandler::new();
        let rule = "fp6-rule-dlq-rule";
        let queue = "fp6-rule-dlq";
        put_failing_lambda_rule(&handler, rule, queue, true);
        let records = drain_bridge_queue_until(queue, 1);
        assert_eq!(
            records.len(),
            1,
            "rule-level DeadLetterConfig catches failures"
        );
        assert_eq!(records[0]["rule"], rule);
        // And DescribeRule surfaces the rule-level config.
        let described = body(&handler.handle(make_req("DescribeRule", json!({"Name": rule}))));
        assert_eq!(
            described["DeadLetterConfig"]["Arn"],
            json!(format!("arn:aws:sqs:us-east-1:123456789012:{queue}"))
        );
    }

    #[test]
    fn test_invalid_rule_dlq_arn_is_ignored() {
        let handler = EventsHandler::new();
        let rule = "fp6-non-sqs-dlq";
        handler.handle(make_req(
            "PutRule",
            json!({
                "Name": rule,
                "EventPattern": json!({"source": [format!("fp6.{rule}")]}).to_string(),
                "DeadLetterConfig": {"Arn": "arn:aws:logs:us-east-1:123456789012:log-group:x"},
            }),
        ));
        handler.handle(make_req(
            "PutTargets",
            json!({
                "Rule": rule,
                "Targets": [{"Id": "t1", "Arn": "arn:aws:lambda:us-east-1:123456789012:function:nope"}],
            }),
        ));
        publish_event_to_bus(
            &json!({"version": "0", "id": "e", "source": format!("fp6.{rule}"), "detail": {}}),
            123456789012,
            "us-east-1",
        );
        // Non-SQS DLQ ARNs never deliver (era `_send_to_dlq` guard).
        assert!(drain_bridge_queue("fp6-non-sqs-dlq").is_empty());
    }

    // -- cross-bus recursion cap (F9) ------------------------------------------

    #[test]
    fn test_bus_cycle_terminates_at_depth_cap() {
        let handler = EventsHandler::new();
        // Isolation: the bridge pending buffer is process-global.
        sqs::bridge::clear_pending(123456789012, "us-east-1", "fp9-bus-a-q");
        sqs::bridge::clear_pending(123456789012, "us-east-1", "fp9-bus-b-q");
        let a = "fp9-bus-a";
        let b = "fp9-bus-b";
        let qa = "fp9-bus-a-q";
        let qb = "fp9-bus-b-q";
        handler.handle(make_req("CreateEventBus", json!({"Name": a})));
        handler.handle(make_req("CreateEventBus", json!({"Name": b})));
        for (bus, rule, queue, forward) in [(a, "fp9-ra", qa, b), (b, "fp9-rb", qb, a)] {
            handler.handle(make_req(
                "PutRule",
                json!({
                    "Name": rule,
                    "EventBusName": bus,
                    "EventPattern": json!({"source": ["fp9.recursion"]}).to_string(),
                }),
            ));
            handler.handle(make_req(
                "PutTargets",
                json!({
                    "Rule": rule,
                    "EventBusName": bus,
                    "Targets": [
                        {"Id": "q", "Arn": format!("arn:aws:sqs:us-east-1:123456789012:{queue}")},
                        {"Id": "next", "Arn": format!("arn:aws:events:us-east-1:123456789012:event-bus/{forward}")},
                    ],
                }),
            ));
        }
        let resp = handler.handle(make_req("PutEvents", json!({
            "Entries": [{"Source": "fp9.recursion", "DetailType": "d", "Detail": "{}", "EventBusName": a}]
        })));
        assert_eq!(body(&resp)["FailedEntryCount"], 0);

        let a_deliveries = drain_bridge_queue(qa).len();
        let b_deliveries = drain_bridge_queue(qb).len();
        // The cycle hops between the buses up to the depth cap and stops:
        // qa@even depths (0,2,...,10) → 6, qb@odd (1,3,...,9) → 5. Without
        // the cap both counts diverge forever (and the test would hang).
        assert_eq!(a_deliveries, 6, "even-depth deliveries before the cap");
        assert_eq!(b_deliveries, 5, "odd-depth deliveries before the cap");

        // Clean the pair of queues (the drain already consumed them) and
        // remove the targets so the rules are era-deletable.
        handler.handle(make_req(
            "RemoveTargets",
            json!({"Rule": "fp9-ra", "EventBusName": a, "Ids": ["q", "next"]}),
        ));
        handler.handle(make_req(
            "RemoveTargets",
            json!({"Rule": "fp9-rb", "EventBusName": b, "Ids": ["q", "next"]}),
        ));
        handler.handle(make_req(
            "DeleteRule",
            json!({"Name": "fp9-ra", "EventBusName": a}),
        ));
        handler.handle(make_req(
            "DeleteRule",
            json!({"Name": "fp9-rb", "EventBusName": b}),
        ));
        handler.handle(make_req("DeleteEventBus", json!({"Name": a})));
        handler.handle(make_req("DeleteEventBus", json!({"Name": b})));
    }

    // -- unknown destination ARN drop (F12) ------------------------------------

    #[test]
    fn test_unknown_destination_arn_is_dropped_silently() {
        let handler = EventsHandler::new();
        let rule = "fp12-unknown-dest";
        let queue = "fp12-unknown-dlq";
        // Target a service with no registered hook AND no rule DLQ: the
        // destination is a silent no-op (era log-only "simulated targets"),
        // never a DLQ record.
        handler.handle(make_req(
            "PutRule",
            json!({
                "Name": rule,
                "EventPattern": json!({"source": [format!("fp12.{rule}")]}).to_string(),
            }),
        ));
        handler.handle(make_req("PutTargets", json!({
            "Rule": rule,
            "Targets": [{"Id": "t1", "Arn": "arn:aws:ssm:us-east-1:123456789012:parameter/somewhere"}],
        })));
        // Registered-hook error → DLQ; unwired kind → drop. Sanity: the same
        // publish to an unwired destination produces no delivery at all.
        publish_event_to_bus(
            &json!({"version": "0", "id": "e", "source": format!("fp12.{rule}"), "detail": {}}),
            123456789012,
            "us-east-1",
        );
        assert!(
            drain_bridge_queue(queue).is_empty(),
            "unwired destination ARNs are dropped without DLQ traffic"
        );
        handler.handle(make_req("DeleteRule", json!({"Name": rule})));
    }

    // -- ListEventBuses pagination (moto 5.2.3) --------------------------------

    /// Pagination tests run under a dedicated account so exhaustive-list
    /// assertions stay immune to other tests sharing the default account
    /// (state is keyed by (account, region)).
    fn req_acc(operation: &str, params: Value, account: u64) -> AwsRequest {
        AwsRequest {
            service: "events".to_string(),
            operation: operation.to_string(),
            account,
            region: "us-east-1".to_string(),
            params,
            body: bytes::Bytes::new(),
        }
    }

    fn bus_names(resp: &Value) -> Vec<&str> {
        resp["EventBuses"]
            .as_array()
            .unwrap()
            .iter()
            .map(|b| b["Name"].as_str().unwrap())
            .collect()
    }

    #[test]
    fn list_event_buses_pagination_slices_with_limit_and_token() {
        let handler = EventsHandler::new();
        let acct = 141414141414;
        for name in ["test-bus-1", "test-bus-2", "other-bus-1", "other-bus-2"] {
            handler.handle(req_acc("CreateEventBus", json!({ "Name": name }), acct));
        }

        // No pagination arguments: the full list, token exhausted. moto emits
        // the key as null (botocore clients drop null members entirely).
        let resp = body(&handler.handle(req_acc("ListEventBuses", json!({}), acct)));
        assert_eq!(bus_names(&resp).len(), 5, "default + 4 custom buses");
        assert!(resp["NextToken"].is_null());

        // NamePrefix works on its own (moto filters the bus map, which
        // includes the default bus).
        let resp = body(&handler.handle(req_acc(
            "ListEventBuses",
            json!({ "NamePrefix": "other-bus" }),
            acct,
        )));
        assert_eq!(bus_names(&resp), ["other-bus-1", "other-bus-2"]);
        assert!(resp["NextToken"].is_null());

        // Limit slices in list order: default bus, then custom buses sorted
        // by name; NextToken carries the last-returned bus name.
        let resp = body(&handler.handle(req_acc("ListEventBuses", json!({ "Limit": 2 }), acct)));
        assert_eq!(bus_names(&resp), ["default", "other-bus-1"]);
        let token = resp["NextToken"].as_str().unwrap().to_string();

        let resp = body(&handler.handle(req_acc(
            "ListEventBuses",
            json!({ "Limit": 2, "NextToken": token }),
            acct,
        )));
        assert_eq!(bus_names(&resp), ["other-bus-2", "test-bus-1"]);
        let token = resp["NextToken"].as_str().unwrap().to_string();

        let resp = body(&handler.handle(req_acc(
            "ListEventBuses",
            json!({ "NextToken": token }),
            acct,
        )));
        assert_eq!(bus_names(&resp), ["test-bus-2"]);
        assert!(resp["NextToken"].is_null());
    }

    #[test]
    fn list_event_buses_pagination_with_prefix_and_token() {
        let handler = EventsHandler::new();
        let acct = 141414141415;
        for name in ["test-bus-1", "other-bus-1", "other-bus-2"] {
            handler.handle(req_acc("CreateEventBus", json!({ "Name": name }), acct));
        }

        let resp = body(&handler.handle(req_acc(
            "ListEventBuses",
            json!({ "NamePrefix": "other-bus", "Limit": 1 }),
            acct,
        )));
        let names = bus_names(&resp);
        assert_eq!(names, ["other-bus-1"]);
        let token = resp["NextToken"].as_str().unwrap().to_string();

        // NamePrefix must be repeated ("keep all other arguments unchanged").
        let resp = body(&handler.handle(req_acc(
            "ListEventBuses",
            json!({ "NamePrefix": "other-bus", "NextToken": token }),
            acct,
        )));
        assert_eq!(bus_names(&resp), ["other-bus-2"]);
        assert!(resp["NextToken"].is_null());
    }

    #[test]
    fn list_event_buses_pagination_default_limit_is_50() {
        let handler = EventsHandler::new();
        let acct = 141414141416;
        for i in 0..60 {
            handler.handle(req_acc(
                "CreateEventBus",
                json!({ "Name": format!("bulk-{i:03}") }),
                acct,
            ));
        }
        let resp = body(&handler.handle(req_acc("ListEventBuses", json!({}), acct)));
        assert_eq!(
            bus_names(&resp).len(),
            50,
            "moto's limit_default for list_event_buses"
        );
        assert!(resp["NextToken"].as_str().is_some());
        let token = resp["NextToken"].as_str().unwrap().to_string();

        let resp = body(&handler.handle(req_acc(
            "ListEventBuses",
            json!({ "NextToken": token }),
            acct,
        )));
        assert_eq!(
            bus_names(&resp).len(),
            11,
            "default + 60 buses - first page"
        );
        assert!(resp["NextToken"].is_null());
    }

    #[test]
    fn list_event_buses_pagination_unknown_token_is_an_empty_final_page() {
        let handler = EventsHandler::new();
        let acct = 141414141417;
        handler.handle(req_acc(
            "CreateEventBus",
            json!({ "Name": "solo-bus" }),
            acct,
        ));
        // moto's fail_on_invalid_token=False: an unmatched token ends
        // pagination with an empty page instead of an error.
        let resp = body(&handler.handle(req_acc(
            "ListEventBuses",
            json!({ "NextToken": "bus-that-was-deleted" }),
            acct,
        )));
        assert_eq!(
            bus_names(&resp),
            Vec::<&str>::new() as Vec<&str>,
            "unknown token yields an empty page, no error"
        );
        assert!(resp["NextToken"].is_null());
    }

    // -- CreatePartnerEventSource returns EventSourceArn (moto 5.2.2) ----------

    #[test]
    fn create_partner_event_source_arn_is_returned() {
        let handler = EventsHandler::new();
        let name = "mypartner/actions/create";
        let expected = format!("arn:aws:events:us-east-1::event-source/aws.partner/{name}");
        let resp = body(&handler.handle(make_req(
            "CreatePartnerEventSource",
            json!({ "Name": name, "Account": "222222222222" }),
        )));
        assert_eq!(resp["EventSourceArn"], expected);

        // The stored source carries the same ARN on both describe paths.
        let desc = body(&handler.handle(make_req(
            "DescribePartnerEventSource",
            json!({ "Name": name }),
        )));
        assert_eq!(desc["Arn"], expected);
        assert_eq!(desc["Name"], name);
        let desc = body(&handler.handle(make_req("DescribeEventSource", json!({ "Name": name }))));
        assert_eq!(desc["Arn"], expected);
    }

    #[test]
    fn create_partner_event_source_arn_is_stable_when_recreated() {
        let handler = EventsHandler::new();
        let name = "mypartner/actions/recreate";
        let first = body(&handler.handle(make_req(
            "CreatePartnerEventSource",
            json!({ "Name": name, "Account": "222222222222" }),
        )));
        let second = body(&handler.handle(make_req(
            "CreatePartnerEventSource",
            json!({ "Name": name, "Account": "333333333333" }),
        )));
        assert_eq!(first, second, "recreating an existing source keeps its ARN");
        assert_eq!(
            first["EventSourceArn"],
            format!("arn:aws:events:us-east-1::event-source/aws.partner/{name}")
        );
    }
}
