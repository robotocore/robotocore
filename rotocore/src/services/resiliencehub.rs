//! AWS Resilience Hub backend — port of moto's resiliencehub engine.
//!
//! Rest-json service with lowercase-camel wire keys (the spec's
//! locationNames). Applications keep a draft AppVersion plus published
//! releases; app components, resources and resource mappings hang off the
//! draft; policies / assessments / recommendation templates are name keyed.
//! Error semantics mirror moto: AppNotFound / ResiliencyPolicyNotFound /
//! AppVersionNotFound, plus the create_resiliency_policy map-key validation.

use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Map, Value};
use std::collections::HashMap;

static SLOT: Lazy<store::Slot<RhState>> = Lazy::new(|| store::slot::<RhState>("resiliencehub"));

#[derive(Default)]
struct RhState {
    apps: HashMap<String, App>,
    policies: HashMap<String, Policy>,
    assessments: HashMap<String, Value>,
    metrics_exports: HashMap<String, Value>,
    grouping_tasks: HashMap<String, Value>,
    assessment_templates: HashMap<String, Value>,
    tags: HashMap<String, Map<String, Value>>,
    region: String,
    account: String,
}

struct App {
    arn: String,
    name: String,
    status: String,
    description: Option<Value>,
    assessment_schedule: String,
    event_subscriptions: Option<Value>,
    permission_model: Option<Value>,
    policy_arn: Option<Value>,
    created: f64,
    template_body: String,
    versions: Vec<Version>,
    input_sources: Value,
    tags: Map<String, Value>,
}

struct Version {
    app_version: String,
    version_name: Option<Value>,
    identifier: i64,
    created: f64,
    additional_info: Option<Value>,
    app_components: Vec<Value>,
    resources: Vec<Value>,
    resource_mappings: Vec<Value>,
    source_arns: Vec<Value>,
}

struct Policy {
    arn: String,
    policy: Value,
    policy_name: String,
    policy_description: Option<Value>,
    data_location_constraint: Option<Value>,
    tier: Value,
    created: f64,
}

fn now() -> f64 {
    crate::proto::now_unix()
}

fn uuid_str() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn app_not_found(arn: &str) -> Response {
    crate::proto::json_error(
        404,
        " ResourceNotFoundException".trim(),
        &format!("App not found for appArn {arn}"),
    )
}

fn policy_not_found(arn: &str) -> Response {
    crate::proto::json_error(
        404,
        "ResourceNotFoundException",
        &format!("ResiliencyPolicy with policyArn {arn} not found"),
    )
}

fn not_found_app_version() -> Response {
    crate::proto::json_error(404, "ResourceNotFoundException", "AppVersion not found")
}

fn app_tags(state: &RhState, arn: &str) -> Value {
    Value::Object(state.tags.get(arn).cloned().unwrap_or_default())
}

impl Version {
    fn to_json(&self) -> Value {
        let mut resp = json!({
            "appVersion": self.app_version,
            "creationTime": self.created,
            "identifier": self.identifier,
        });
        if let Some(name) = &self.version_name {
            resp["versionName"] = name.clone();
        }
        resp
    }

    fn by_name(&self, name: &str) -> bool {
        self.app_version == name
    }
}

impl App {
    fn to_json(&self, st: &RhState) -> Value {
        let mut resp = json!({
            "appArn": self.arn,
            "assessmentSchedule": self.assessment_schedule,
            "complianceStatus": "NotAssessed",
            "creationTime": self.created,
            "driftStatus": "NotChecked",
            "name": self.name,
            "resilienceScore": 0.0,
            "status": self.status,
            "tags": app_tags(st, &self.arn),
        });
        if let Some(d) = &self.description {
            resp["description"] = d.clone();
        }
        if let Some(e) = &self.event_subscriptions {
            resp["eventSubscriptions"] = e.clone();
        }
        if let Some(p) = &self.permission_model {
            resp["permissionModel"] = p.clone();
        }
        if let Some(arn) = &self.policy_arn {
            resp["policyArn"] = arn.clone();
        }
        resp
    }

    fn get_version_index(&self, name: &str) -> Option<usize> {
        self.versions.iter().position(|v| v.by_name(name))
    }
}

impl Policy {
    fn to_json(&self, st: &RhState) -> Value {
        let mut resp = json!({
            "creationTime": self.created,
            "policy": self.policy,
            "policyArn": self.arn,
            "policyName": self.policy_name,
            "tags": app_tags(st, &self.arn),
            "tier": self.tier,
        });
        if let Some(c) = &self.data_location_constraint {
            resp["dataLocationConstraint"] = c.clone();
        }
        if let Some(d) = &self.policy_description {
            resp["policyDescription"] = d.clone();
        }
        resp
    }
}

fn params_str<'a>(params: &'a Value, key: &str) -> Option<&'a Value> {
    params.get(key)
}

fn unknown(req: &Request) -> Response {
    crate::proto::json_error(
        501,
        "NotImplemented",
        &format!("The {} action has not been implemented", req.operation),
    )
}

pub fn dispatch(req: &Request) -> Response {
    let mut params = crate::proto::parse_json_body(&req.body);
    if let Value::Object(map) = &mut params {
        for (key, values) in crate::proto::query_map(&req.query_string) {
            if let Some(single) = values.first() {
                map.insert(key, json!(single));
            }
        }
    } else {
        params = json!({});
    }
    let mut guard = SLOT.write();
    let st = guard.entry((req.account, req.region.clone())).or_default();
    if st.region.is_empty() {
        st.region = req.region.clone();
        st.account = req.account.to_string();
    }
    let path = req.path.trim_start_matches('/').trim_end_matches('/');
    let method = req.method.as_str();

    // Tags ride /tags/{arn} REST paths (GET/POST/DELETE). The dispatch
    // already holds the state write guard, so tags are answered from the
    // current (account, region) scope directly.
    if let Some(arn) = path.strip_prefix("tags/") {
        let decoded = crate::proto::url_decode(arn);
        return match method {
            "GET" => Response::json_status(
                200,
                &json!({"tags": st.tags.get(&decoded).cloned().map(Value::Object).unwrap_or(json!({}))}),
            ),
            "POST" => {
                if let Some(tags) = params.get("tags").and_then(Value::as_object) {
                    let entry = st.tags.entry(decoded.clone()).or_default();
                    for (k, v) in tags {
                        entry.insert(k.clone(), v.clone());
                    }
                }
                Response::json_status(200, &json!({}))
            }
            "DELETE" => {
                let keys: Vec<String> = crate::proto::query_map(&req.query_string)
                    .get("tagKeys")
                    .cloned()
                    .unwrap_or_default();
                if let Some(entry) = st.tags.get_mut(&decoded) {
                    for key in &keys {
                        entry.remove(key);
                    }
                }
                Response::json_status(200, &json!({}))
            }
            _ => unknown(req),
        };
    }

    match (method, path) {
        ("POST", "create-app") => st.create_app(&params),
        ("POST", "describe-app") => st.describe_app(&params),
        ("POST", "update-app") => st.update_app(&params),
        ("POST", "delete-app") => st.delete_app(&params),
        ("GET", "list-apps") | ("POST", "list-apps") => st.list_apps(&params),
        ("POST", "create-resiliency-policy") => st.create_policy(&params),
        ("POST", "describe-resiliency-policy") => st.describe_policy(&params),
        ("POST", "update-resiliency-policy") => st.update_policy(&params),
        ("POST", "delete-resiliency-policy") => st.delete_policy(&params),
        ("GET", "list-resiliency-policies") | ("POST", "list-resiliency-policies") => {
            st.list_policies(&params)
        }
        ("POST", "create-app-version-app-component") => {
            st.create_app_version_app_component(&params)
        }
        ("POST", "update-app-version-app-component") => {
            st.update_app_version_app_component(&params)
        }
        ("POST", "delete-app-version-app-component") => {
            st.delete_app_version_app_component(&params)
        }
        ("POST", "describe-app-version-app-component") => {
            st.describe_app_version_app_component(&params)
        }
        ("POST", "create-app-version-resource") => st.create_app_version_resource(&params),
        ("POST", "update-app-version-resource") => st.update_app_version_resource(&params),
        ("POST", "delete-app-version-resource") => st.delete_app_version_resource(&params),
        ("POST", "describe-app-version-resource") => st.describe_app_version_resource(&params),
        ("POST", "list-app-version-resources") => st.list_app_version_resources(&params),
        ("POST", "list-app-version-app-components") => st.list_app_version_app_components(&params),
        ("POST", "list-app-versions") => st.list_app_versions(&params),
        ("POST", "publish-app-version") => st.publish_app_version(&params),
        ("POST", "describe-app-version") => st.describe_app_version(&params),
        ("POST", "update-app-version") => st.update_app_version(&params),
        ("POST", "describe-app-version-template") => st.describe_app_version_template(&params),
        ("POST", "put-draft-app-version-template") => st.put_draft_template(&params),
        ("POST", "import-resources-to-draft-app-version") => st.import_resources_to_draft(&params),
        ("POST", "add-draft-app-version-resource-mappings") => {
            st.add_draft_resource_mappings(&params)
        }
        ("POST", "remove-draft-app-version-resource-mappings") => {
            st.remove_draft_resource_mappings(&params)
        }
        ("POST", "list-app-version-resource-mappings") => {
            st.list_app_version_resource_mappings(&params)
        }
        ("POST", "describe-draft-app-version-resources-import-status") => {
            st.describe_import_status(&params)
        }
        ("POST", "describe-app-version-resources-resolution-status") => {
            st.describe_resolution_status(&params)
        }
        ("POST", "resolve-app-version-resources") => st.resolve_resources(&params),
        ("POST", "start-app-assessment") => st.start_app_assessment(&params),
        ("POST", "describe-app-assessment") => st.describe_assessment(&params),
        ("POST", "delete-app-assessment") => st.delete_assessment(&params),
        ("GET", "list-app-assessments") | ("POST", "list-app-assessments") => st.list_assessments(),
        ("POST", "list-app-input-sources") => st.list_app_input_sources(&params),
        ("GET", "list-alarm-recommendations") | ("POST", "list-alarm-recommendations") => {
            Response::json_status(200, &json!({"alarmRecommendations": [], "nextToken": null}))
        }
        ("POST", "list-sop-recommendations") => {
            Response::json_status(200, &json!({"sopRecommendations": [], "nextToken": null}))
        }
        ("POST", "list-test-recommendations") => {
            Response::json_status(200, &json!({"testRecommendations": [], "nextToken": null}))
        }
        ("POST", "list-app-component-compliances") => {
            let assessment = params.get("assessmentArn").cloned().unwrap_or(json!(null));
            let arn = assessment.as_str().unwrap_or("");
            let compliances = st
                .assessments
                .get(arn)
                .and_then(|a| a.get("componentCompliances"))
                .cloned()
                .unwrap_or(json!([]));
            Response::json_status(
                200,
                &json!({"componentCompliances": compliances, "nextToken": null}),
            )
        }
        ("POST", "list-app-component-recommendations") => {
            let assessment = params.get("assessmentArn").cloned().unwrap_or(json!(null));
            let arn = assessment.as_str().unwrap_or("");
            let recs = st
                .assessments
                .get(arn)
                .and_then(|a| a.get("componentRecommendations"))
                .cloned()
                .unwrap_or(json!([]));
            Response::json_status(
                200,
                &json!({"componentRecommendations": recs, "nextToken": null}),
            )
        }
        ("POST", "list-app-assessment-compliance-drifts") => Response::json_status(
            200,
            &json!({"maxResults": 100, "complianceDrifts": [], "nextToken": null}),
        ),
        ("POST", "list-app-assessment-resource-drifts") => {
            Response::json_status(200, &json!({"resourceDrifts": [], "nextToken": null}))
        }
        ("POST", "list-unsupported-app-version-resources") => {
            st.list_unsupported_app_version_resources(&params)
        }
        ("GET", "list-suggested-resiliency-policies")
        | ("POST", "list-suggested-resiliency-policies") => {
            Response::json_status(200, &json!({"resiliencyPolicies": [], "nextToken": null}))
        }
        ("POST", "start-metrics-export") => st.start_metrics_export(&params),
        ("POST", "describe-metrics-export") => st.describe_metrics_export(&params),
        ("POST", "list-metrics") => {
            Response::json_status(200, &json!({"rows": [], "nextToken": null}))
        }
        ("POST", "start-resource-grouping-recommendation-task") => st.start_grouping_task(&params),
        ("POST", "describe-resource-grouping-recommendation-task") => {
            st.describe_grouping_task(&params)
        }
        ("GET", "list-resource-grouping-recommendations")
        | ("POST", "list-resource-grouping-recommendations") => {
            st.list_grouping_recommendations(&params)
        }
        ("POST", "accept-resource-grouping-recommendations") => {
            st.grouping_decisions(&params, json!([]))
        }
        ("POST", "reject-resource-grouping-recommendations") => {
            st.grouping_decisions(&params, json!([]))
        }
        ("POST", "batch-update-recommendation-status") => {
            st.batch_update_recommendation_status(&params)
        }
        ("POST", "list-recommendation-templates") => Response::json_status(
            200,
            &json!({"recommendationTemplates": [], "nextToken": null}),
        ),
        ("GET", "list-tags-for-resource") => {
            let qmap = crate::proto::query_map(&req.query_string);
            let arn = crate::proto::query_first(&qmap, "resourceArn").unwrap_or("");
            Response::json_status(
                200,
                &json!({"tags": st.tags.get(arn).cloned().map(Value::Object).unwrap_or(json!({}))}),
            )
        }
        ("GET", "list-recommendation-templates") => Response::json_status(
            200,
            &json!({"recommendationTemplates": [], "nextToken": null}),
        ),
        ("POST", "create-recommendation-template") => st.create_recommendation_template(&params),
        ("POST", "delete-recommendation-template") => st.delete_recommendation_template(&params),
        ("POST", "delete-app-input-source") => st.delete_app_input_source(&params),
        _ => unknown(req),
    }
}

fn app_or<'a>(state: &'a RhState, arn: &str) -> std::result::Result<&'a App, Response> {
    state.apps.get(arn).ok_or_else(|| app_not_found(arn))
}

impl RhState {
    fn create_app(&mut self, params: &Value) -> Response {
        let name = params
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let arn = format!(
            "arn:aws:resiliencehub:{region}:{account}:app/{uuid}",
            region = self.region,
            account = self.account,
            uuid = uuid::Uuid::new_v4()
        );
        if let Some(tags) = params.get("tags") {
            if let Some(map) = tags.as_object() {
                let entry = self.tags.entry(arn.clone()).or_default();
                for (k, v) in map {
                    entry.insert(k.clone(), v.clone());
                }
            }
        }
        let app = App {
            arn: arn.clone(),
            name: name.clone(),
            status: "Active".into(),
            description: params.get("description").cloned(),
            assessment_schedule: params
                .get("assessmentSchedule")
                .and_then(Value::as_str)
                .unwrap_or("Disabled")
                .to_string(),
            event_subscriptions: params.get("eventSubscriptions").cloned(),
            permission_model: params.get("permissionModel").cloned(),
            policy_arn: params.get("policyArn").cloned(),
            created: now(),
            template_body: String::new(),
            versions: vec![Version {
                app_version: "draft".into(),
                version_name: None,
                identifier: 0,
                created: now(),
                additional_info: None,
                app_components: vec![],
                resources: vec![],
                resource_mappings: vec![],
                source_arns: vec![],
            }],
            input_sources: json!([]),
            tags: self.tags.get(&arn).cloned().unwrap_or_default(),
        };
        self.apps.insert(arn.clone(), app);
        let out = self.apps.get(&arn).unwrap().to_json(self);
        Response::json_status(200, &json!({"app": out}))
    }

    fn describe_app(&self, params: &Value) -> Response {
        let arn = params.get("appArn").and_then(Value::as_str).unwrap_or("");
        match app_or(self, arn) {
            Ok(app) => Response::json_status(200, &json!({"app": app.to_json(self)})),
            Err(resp) => resp,
        }
    }

    fn update_app(&mut self, params: &Value) -> Response {
        let arn = params
            .get("appArn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if !self.apps.contains_key(&arn) {
            return app_not_found(&arn);
        }
        {
            let app = self.apps.get_mut(&arn).unwrap();
            if let Some(schedule) = params.get("assessmentSchedule") {
                app.assessment_schedule = schedule.as_str().unwrap_or("Disabled").to_string();
            }
            if let Some(desc) = params.get("description") {
                app.description = Some(desc.clone());
            }
            if let Some(events) = params.get("eventSubscriptions") {
                app.event_subscriptions = Some(events.clone());
            }
            if let Some(pm) = params.get("permissionModel") {
                app.permission_model = Some(pm.clone());
            }
            if params
                .get("clearResiliencyPolicyArn")
                .and_then(Value::as_bool)
                == Some(true)
            {
                app.policy_arn = None;
            } else if let Some(arn_value) = params.get("policyArn") {
                app.policy_arn = Some(arn_value.clone());
            }
        }
        let out = self.apps.get(&arn).unwrap().to_json(self);
        Response::json_status(200, &json!({"app": out}))
    }

    fn delete_app(&mut self, params: &Value) -> Response {
        let arn = params
            .get("appArn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        match self.apps.remove(&arn) {
            Some(_) => Response::json_status(200, &json!({"appArn": arn})),
            None => app_not_found(&arn),
        }
    }

    fn list_apps(&self, params: &Value) -> Response {
        let name = params.get("name").and_then(Value::as_str);
        let arn = params.get("appArn").and_then(Value::as_str);
        let summaries: Vec<Value> = self
            .apps
            .values()
            .filter(|app| match (name, arn) {
                (Some(n), _) => app.name == n,
                (None, Some(a)) => app.arn == a,
                (None, None) => true,
            })
            .map(|app| {
                let full = app.to_json(self);
                json!({
                    "appArn": full["appArn"],
                    "assessmentSchedule": full["assessmentSchedule"],
                    "complianceStatus": full["complianceStatus"],
                    "creationTime": full["creationTime"],
                    "driftStatus": full["driftStatus"],
                    "name": full["name"],
                    "resilienceScore": full["resilienceScore"],
                    "status": full["status"],
                })
            })
            .collect();
        Response::json_status(200, &json!({"appSummaries": summaries, "nextToken": null}))
    }

    // ---- policies -----------------------------------------------------------

    fn create_policy(&mut self, params: &Value) -> Response {
        let policy = params.get("policy").cloned().unwrap_or(json!({}));
        // moto validates map keys + required types before storing.
        let all_types = ["Software", "Hardware", "AZ", "Region"];
        let bad = policy
            .as_object()
            .map(|map| map.keys().any(|k| !all_types.contains(&k.as_str())))
            .unwrap_or(false);
        if bad {
            return crate::proto::json_error(
                400,
                "ValidationException",
                "1 validation error detected: Value at 'policy' failed to satisfy constraint: Map keys must satisfy constraint: [Member must satisfy enum value set: [Software, Hardware, Region, AZ]]",
            );
        }
        for required in ["Software", "Hardware", "AZ"] {
            if !policy.get(required).is_some() {
                return crate::proto::json_error(
                    400,
                    "ValidationException",
                    &format!("FailureType {} does not exist", required.to_uppercase()),
                );
            }
        }
        let policy_name = params
            .get("policyName")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let arn = format!(
            "arn:aws:resiliencehub:{region}:{account}:resiliency-policy/{uuid}",
            region = self.region,
            account = self.account,
            uuid = uuid::Uuid::new_v4()
        );
        if let Some(tags) = params.get("tags").and_then(Value::as_object) {
            let entry = self.tags.entry(arn.clone()).or_default();
            for (k, v) in tags {
                entry.insert(k.clone(), v.clone());
            }
        }
        self.policies.insert(
            arn.clone(),
            Policy {
                arn: arn.clone(),
                policy: policy.clone(),
                policy_name,
                policy_description: params.get("policyDescription").cloned(),
                data_location_constraint: params.get("dataLocationConstraint").cloned(),
                tier: params
                    .get("tier")
                    .cloned()
                    .unwrap_or(json!("NotApplicable")),
                created: now(),
            },
        );
        let out = self.policies.get(&arn).unwrap().to_json(self);
        Response::json_status(200, &json!({"policy": out}))
    }

    fn describe_policy(&self, params: &Value) -> Response {
        let arn = params
            .get("policyArn")
            .and_then(Value::as_str)
            .unwrap_or("");
        match self.policies.get(arn) {
            Some(policy) => Response::json_status(200, &json!({"policy": policy.to_json(self)})),
            None => crate::proto::json_error(
                404,
                "ResourceNotFoundException",
                &format!("ResiliencyPolicy with policyArn {arn} not found"),
            ),
        }
    }

    fn list_policies(&self, params: &Value) -> Response {
        let name = params.get("policyName").and_then(Value::as_str);
        let policies: Vec<Value> = self
            .policies
            .values()
            .filter(|p| match name {
                Some(n) => p.policy_name == n,
                None => true,
            })
            .map(|p| p.to_json(self))
            .collect();
        Response::json_status(200, &json!({"resiliencyPolicies": policies}))
    }

    fn update_policy(&mut self, params: &Value) -> Response {
        let arn = params
            .get("policyArn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if !self.policies.contains_key(&arn) {
            return crate::proto::json_error(
                404,
                "ResourceNotFoundException",
                &format!("ResiliencyPolicy with policyArn {arn} not found"),
            );
        }
        let policy = self.policies.get_mut(&arn).unwrap();
        if let Some(p) = params.get("policy") {
            policy.policy = p.clone();
        }
        if let Some(d) = params.get("policyDescription") {
            policy.policy_description = Some(d.clone());
        }
        if let Some(n) = params.get("policyName") {
            policy.policy_name = n.as_str().unwrap_or("").to_string();
        }
        if let Some(t) = params.get("tier") {
            policy.tier = t.clone();
        }
        if let Some(c) = params.get("dataLocationConstraint") {
            policy.data_location_constraint = Some(c.clone());
        }
        let out = self.policies.get(&arn).unwrap().to_json(self);
        Response::json_status(200, &json!({"policy": out}))
    }

    fn delete_policy(&mut self, params: &Value) -> Response {
        let arn = params
            .get("policyArn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        match self.policies.remove(&arn) {
            Some(_) => {
                self.tags.remove(&arn);
                Response::json_status(200, &json!({"policyArn": arn}))
            }
            None => crate::proto::json_error(
                404,
                "ResourceNotFoundException",
                &format!("ResiliencyPolicy with policyArn {arn} not found"),
            ),
        }
    }

    // ---- app versions ----------------------------------------------------------

    fn draft_index(&self, arn: &str) -> Result<usize, Response> {
        let app = app_or(self, arn)?;
        app.get_version_index("draft")
            .ok_or(not_found_app_version())
    }

    fn create_app_version_app_component(&mut self, params: &Value) -> Response {
        let arn = params
            .get("appArn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if self.draft_index(&arn).is_err() {
            return app_not_found(&arn);
        }
        let name = params
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let component = json!({
            "id": name,
            "name": name,
            "type": params.get("type").cloned().unwrap_or(json!("")),
        });
        let component = match component.as_object() {
            Some(map) => {
                let mut fixed = map.clone();
                fixed.insert(
                    "type".into(),
                    params.get("type").cloned().unwrap_or(json!("")),
                );
                Value::Object(fixed)
            }
            None => component,
        };
        let app = self.apps.get_mut(&arn).unwrap();
        let draft = app.versions.get_mut(0).unwrap();
        draft.app_components.push(component.clone());
        Response::json_status(
            200,
            &json!({"appArn": arn, "appComponent": component, "appVersion": "draft"}),
        )
    }

    fn update_app_version_app_component(&mut self, params: &Value) -> Response {
        let arn = params
            .get("appArn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if self.draft_index(&arn).is_err() {
            return app_not_found(&arn);
        }
        let id = params
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let mut component = json!({"id": id, "name": "", "type": ""});
        {
            let app = self.apps.get_mut(&arn).unwrap();
            let draft = app.versions.get_mut(0).unwrap();
            if let Some(existing) = draft
                .app_components
                .iter_mut()
                .find(|c| c.get("id").and_then(Value::as_str) == Some(id.as_str()))
            {
                if let Some(name) = params.get("name") {
                    existing["name"] = name.clone();
                }
                if let Some(t) = params.get("type") {
                    existing["type"] = t.clone();
                }
                if let Some(info) = params.get("additionalInfo") {
                    existing["additionalInfo"] = info.clone();
                }
                component = existing.clone();
            }
        }
        Response::json_status(
            200,
            &json!({"appArn": arn, "appComponent": component, "appVersion": "draft"}),
        )
    }

    fn describe_app_version_app_component(&self, params: &Value) -> Response {
        let arn = params
            .get("appArn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if self.draft_index(&arn).is_err() {
            return app_not_found(&arn);
        }
        let id = params.get("id").and_then(Value::as_str).unwrap_or("");
        let component = self
            .apps
            .get(&arn)
            .and_then(|app| app.versions.get(0))
            .and_then(|draft| {
                draft
                    .app_components
                    .iter()
                    .find(|c| c.get("id").and_then(Value::as_str) == Some(id))
            })
            .cloned();
        Response::json_status(
            200,
            &json!({
                "appArn": arn,
                "appComponent": component.unwrap_or(json!({})),
                "appVersion": params.get("appVersion").cloned().unwrap_or(json!("draft")),
            }),
        )
    }

    fn delete_app_version_app_component(&mut self, params: &Value) -> Response {
        let arn = params
            .get("appArn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if self.draft_index(&arn).is_err() {
            return app_not_found(&arn);
        }
        let id = params
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let mut removed = None;
        if let Some(app) = self.apps.get_mut(&arn) {
            if let Some(draft) = app.versions.get_mut(0) {
                if let Some(pos) = draft
                    .app_components
                    .iter()
                    .position(|c| c.get("id").and_then(Value::as_str) == Some(id.as_str()))
                {
                    removed = Some(draft.app_components.remove(pos));
                }
            }
        }
        Response::json_status(
            200,
            &json!({
                "appArn": arn,
                "appComponent": removed,
                "appVersion": "draft",
            }),
        )
    }

    fn create_app_version_resource(&mut self, params: &Value) -> Response {
        let arn = params
            .get("appArn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if self.draft_index(&arn).is_err() {
            return app_not_found(&arn);
        }
        let components_wanted: Vec<String> = params
            .get("appComponents")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(String::from)
                    .collect()
            })
            .unwrap_or_default();
        let app = self.apps.get_mut(&arn).unwrap();
        let draft = app.versions.get_mut(0).unwrap();
        let components: Vec<Value> = draft
            .app_components
            .iter()
            .filter(|c| {
                let wanted = &components_wanted;
                wanted
                    .iter()
                    .any(|w| c.get("id").and_then(Value::as_str) == Some(w))
            })
            .cloned()
            .collect();
        let logical = params
            .get("logicalResourceId")
            .cloned()
            .unwrap_or(json!({}));
        let physical = params
            .get("physicalResourceId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let resource_name = logical
            .get("identifier")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let resource = json!({
            "appComponents": components,
            "resourceType": params.get("resourceType").cloned().unwrap_or(json!("")),
            "logicalResourceId": logical,
            "physicalResourceId": {"identifier": physical},
            "resourceName": resource_name,
        });
        let draft = self
            .apps
            .get_mut(&arn)
            .unwrap()
            .versions
            .get_mut(0)
            .unwrap();
        draft.resources.push(resource.clone());
        Response::json_status(
            200,
            &json!({"appArn": arn, "appVersion": "draft", "physicalResource": resource}),
        )
    }

    fn update_app_version_resource(&mut self, params: &Value) -> Response {
        let arn = params
            .get("appArn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if self.draft_index(&arn).is_err() {
            return app_not_found(&arn);
        }
        let logical = params
            .get("logicalResourceId")
            .cloned()
            .unwrap_or(json!({}));
        let resource = json!({
            "appComponents": params.get("appComponents").cloned().unwrap_or(json!([])),
            "resourceType": params.get("resourceType").cloned().unwrap_or(json!("")),
            "logicalResourceId": logical.clone(),
            "physicalResourceId": {"identifier": params.get("physicalResourceId").cloned().unwrap_or(json!(""))},
            "resourceName": params.get("resourceName").cloned().unwrap_or(json!(logical["identifier"])),
            "additionalInfo": params.get("additionalInfo").cloned().unwrap_or(json!({})),
        });
        Response::json_status(
            200,
            &json!({"appArn": arn, "appVersion": "draft", "physicalResource": resource}),
        )
    }

    fn delete_app_version_resource(&mut self, params: &Value) -> Response {
        self.update_app_version_resource(params)
    }

    fn describe_app_version_resource(&self, params: &Value) -> Response {
        let arn = params
            .get("appArn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if self.draft_index(&arn).is_err() {
            return app_not_found(&arn);
        }
        let resource = self
            .apps
            .get(&arn)
            .and_then(|app| app.versions.get(0))
            .and_then(|draft| draft.resources.first().cloned())
            .unwrap_or(json!({}));
        Response::json_status(
            200,
            &json!({
                "appArn": arn,
                "appVersion": params.get("appVersion").cloned().unwrap_or(json!("draft")),
                "physicalResource": resource,
            }),
        )
    }

    fn list_app_version_app_components(&self, params: &Value) -> Response {
        let arn = params.get("appArn").and_then(Value::as_str).unwrap_or("");
        let version = params
            .get("appVersion")
            .and_then(Value::as_str)
            .unwrap_or("draft");
        let app = match app_or(self, arn) {
            Ok(app) => app,
            Err(resp) => return resp,
        };
        let components = app
            .get_version_index(version)
            .and_then(|idx| app.versions.get(idx))
            .map(|v| v.app_components.clone())
            .unwrap_or_default();
        Response::json_status(
            200,
            &json!({"appVersion": version, "appComponents": components}),
        )
    }

    fn list_app_version_resources(&self, params: &Value) -> Response {
        let arn = params.get("appArn").and_then(Value::as_str).unwrap_or("");
        let version = params
            .get("appVersion")
            .and_then(Value::as_str)
            .unwrap_or("draft");
        let app = match app_or(self, arn) {
            Ok(app) => app,
            Err(resp) => return resp,
        };
        let resources = app
            .get_version_index(version)
            .and_then(|idx| app.versions.get(idx))
            .map(|v| v.resources.clone())
            .unwrap_or_default();
        Response::json_status(200, &json!({"physicalResources": resources}))
    }

    fn list_app_versions(&self, params: &Value) -> Response {
        let arn = params.get("appArn").and_then(Value::as_str).unwrap_or("");
        match app_or(self, arn) {
            Ok(app) => {
                let versions: Vec<Value> = app.versions.iter().map(|v| v.to_json()).collect();
                Response::json_status(200, &json!({"appVersions": versions}))
            }
            Err(resp) => resp,
        }
    }

    fn publish_app_version(&mut self, params: &Value) -> Response {
        let arn = params
            .get("appArn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let version_name = params
            .get("versionName")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if !self.apps.contains_key(&arn) {
            return app_not_found(&arn);
        }
        let app = self.apps.get_mut(&arn).unwrap();
        for old in &mut app.versions {
            if old.app_version == "release" {
                old.app_version = old.identifier.to_string();
            }
        }
        let identifier = app.versions.len() as i64;
        let version = Version {
            app_version: "release".into(),
            version_name: Some(json!(version_name)),
            identifier,
            created: now(),
            additional_info: None,
            app_components: vec![],
            resources: vec![],
            resource_mappings: vec![],
            source_arns: vec![],
        };
        let created = version.created;
        app.versions.push(version);
        Response::json_status(
            200,
            &json!({
                "appArn": arn,
                "appVersion": "release",
                "creationTime": created,
                "identifier": identifier,
                "versionName": version_name,
            }),
        )
    }

    fn describe_app_version(&self, params: &Value) -> Response {
        let arn = params
            .get("appArn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let version_name = params
            .get("appVersion")
            .and_then(Value::as_str)
            .unwrap_or("");
        if self.draft_index(&arn).is_err() {
            return app_not_found(&arn);
        }
        let app = self.apps.get(&arn).unwrap();
        let version = app
            .get_version_index(version_name)
            .map(|idx| &app.versions[idx]);
        let Some(version) = version else {
            return not_found_app_version();
        };
        let mut resp = json!({"appArn": arn, "appVersion": version.app_version});
        if let Some(info) = &version.additional_info {
            resp["additionalInfo"] = info.clone();
        }
        Response::json_status(200, &resp)
    }

    fn update_app_version(&mut self, params: &Value) -> Response {
        let arn = params
            .get("appArn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if self.draft_index(&arn).is_err() {
            return app_not_found(&arn);
        }
        if let Some(info) = params.get("additionalInfo") {
            let draft = self
                .apps
                .get_mut(&arn)
                .unwrap()
                .versions
                .get_mut(0)
                .unwrap();
            draft.additional_info = Some(info.clone());
        }
        let draft = self.apps.get(&arn).unwrap().versions.get(0).unwrap();
        let mut resp = json!({"appArn": arn, "appVersion": draft.app_version});
        if let Some(info) = &draft.additional_info {
            resp["additionalInfo"] = info.clone();
        }
        Response::json_status(200, &resp)
    }

    fn describe_app_version_template(&self, params: &Value) -> Response {
        let arn = params
            .get("appArn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if self.draft_index(&arn).is_err() {
            return app_not_found(&arn);
        }
        let app = self.apps.get(&arn).unwrap();
        let version = params
            .get("appVersion")
            .and_then(Value::as_str)
            .unwrap_or("draft")
            .to_string();
        Response::json_status(
            200,
            &json!({
                "appArn": arn,
                "appTemplateBody": app.template_body,
                "appVersion": version,
            }),
        )
    }

    fn put_draft_template(&mut self, params: &Value) -> Response {
        let arn = params
            .get("appArn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if self.draft_index(&arn).is_err() {
            return app_not_found(&arn);
        }
        let body = params
            .get("appTemplateBody")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        self.apps.get_mut(&arn).unwrap().template_body = body;
        Response::json_status(200, &json!({"appArn": arn, "appVersion": "draft"}))
    }

    fn import_resources_to_draft(&mut self, params: &Value) -> Response {
        let arn = params
            .get("appArn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if self.draft_index(&arn).is_err() {
            return app_not_found(&arn);
        }
        {
            let draft = self
                .apps
                .get_mut(&arn)
                .unwrap()
                .versions
                .get_mut(0)
                .unwrap();
            if let Some(list) = params.get("sourceArns").and_then(Value::as_array) {
                draft.source_arns.extend(list.clone());
            }
            draft.app_components.push(json!({
                "id": "appcommon",
                "name": "appcommon",
                "type": "AWS::ResilienceHub::AppCommonAppComponent",
            }));
        }
        let draft = self.apps.get(&arn).unwrap().versions.get(0).unwrap();
        let resp = draft.to_json();
        return Response::json_status(
            200,
            &json!({
                "appArn": arn,
                "appVersion": draft.app_version,
                "eksSources": json!([]),
                "sourceArns": draft.source_arns,
                "terraformSources": json!([]),
                "appId": Value::Null,
                "appComponents": draft.app_components,
                "extraAppVersionForEnv": Value::Null,
                "status": draft_status(draft),
            }),
        );
    }

    fn add_draft_resource_mappings(&mut self, params: &Value) -> Response {
        let arn = params
            .get("appArn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if self.draft_index(&arn).is_err() {
            return app_not_found(&arn);
        }
        let mut mappings = Vec::new();
        if let Some(new) = params.get("resourceMappings").and_then(Value::as_array) {
            let draft = self
                .apps
                .get_mut(&arn)
                .unwrap()
                .versions
                .get_mut(0)
                .unwrap();
            draft.resource_mappings.extend(new.clone());
            mappings = draft.resource_mappings.clone();
        }
        Response::json_status(
            200,
            &json!({"appArn": arn, "appVersion": "draft", "resourceMappings": mappings}),
        )
    }

    fn remove_draft_resource_mappings(&mut self, params: &Value) -> Response {
        let arn = params
            .get("appArn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if self.draft_index(&arn).is_err() {
            return app_not_found(&arn);
        }
        let mut names = Vec::new();
        for key in [
            "appRegistryAppNames",
            "eksSourceNames",
            "logicalStackNames",
            "resourceGroupNames",
            "resourceNames",
            "terraformSourceNames",
        ] {
            if let Some(list) = params.get(key).and_then(Value::as_array) {
                names.extend(list.iter().filter_map(Value::as_str).map(String::from));
            }
        }
        let draft = self
            .apps
            .get_mut(&arn)
            .unwrap()
            .versions
            .get_mut(0)
            .unwrap();
        draft.resource_mappings.retain(|m| {
            let name_hit = m
                .get("resourceName")
                .and_then(Value::as_str)
                .map(|n| names.contains(&n.to_string()))
                .unwrap_or(false);
            let physical = m
                .get("physicalResourceId")
                .and_then(|p| p.get("identifier"))
                .and_then(Value::as_str)
                .map(|n| names.contains(&n.to_string()))
                .unwrap_or(false);
            !(name_hit || physical)
        });
        Response::json_status(200, &json!({"appArn": arn, "appVersion": "draft"}))
    }

    fn list_app_version_resource_mappings(&self, params: &Value) -> Response {
        let arn = params
            .get("appArn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if self.draft_index(&arn).is_err() {
            return app_not_found(&arn);
        }
        let version = params
            .get("appVersion")
            .and_then(Value::as_str)
            .unwrap_or("draft");
        let mappings = self
            .apps
            .get(&arn)
            .and_then(|app| app.get_version_index(version))
            .and_then(|idx| self.apps.get(&arn).unwrap().versions.get(idx))
            .map(|v| v.resource_mappings.clone())
            .unwrap_or_default();
        Response::json_status(
            200,
            &json!({"resourceMappings": mappings, "nextToken": null}),
        )
    }

    fn describe_import_status(&self, params: &Value) -> Response {
        let arn = params
            .get("appArn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if self.draft_index(&arn).is_err() {
            return app_not_found(&arn);
        }
        Response::json_status(
            200,
            &json!({
                "appArn": arn,
                "appVersion": "draft",
                "status": "Success",
                "statusChangeTime": now(),
            }),
        )
    }

    fn describe_resolution_status(&self, params: &Value) -> Response {
        let arn = params
            .get("appArn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let version = params
            .get("appVersion")
            .and_then(Value::as_str)
            .unwrap_or("draft")
            .to_string();
        if self.draft_index(&arn).is_err() {
            return app_not_found(&arn);
        }
        Response::json_status(
            200,
            &json!({
                "appArn": arn,
                "appVersion": version,
                "resolutionId": params.get("resolutionId").cloned().unwrap_or(json!(uuid::Uuid::new_v4().to_string())),
                "status": "Success",
            }),
        )
    }

    fn resolve_resources(&mut self, params: &Value) -> Response {
        let arn = params
            .get("appArn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let version = params
            .get("appVersion")
            .and_then(Value::as_str)
            .unwrap_or("draft")
            .to_string();
        if self.draft_index(&arn).is_err() {
            return app_not_found(&arn);
        }
        Response::json_status(
            200,
            &json!({
                "appArn": arn,
                "appVersion": version,
                "resolutionId": uuid::Uuid::new_v4().to_string(),
                "status": "Success",
            }),
        )
    }

    fn start_app_assessment(&mut self, params: &Value) -> Response {
        let arn = params
            .get("appArn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if !self.apps.contains_key(&arn) {
            return app_not_found(&arn);
        }
        let assessment_arn = format!(
            "arn:aws:resiliencehub:{region}:{account}:app-assessment/{uuid}",
            region = self.region,
            account = self.account,
            uuid = uuid::Uuid::new_v4()
        );
        let mut assessment = json!({
            "appArn": arn,
            "appVersion": params.get("appVersion").cloned().unwrap_or(json!("draft")),
            "assessmentArn": assessment_arn.clone(),
            "assessmentName": params.get("assessmentName").cloned().unwrap_or(json!("")),
            "assessmentStatus": "Success",
            "complianceStatus": "PolicyBreached",
            "cost": {},
            "compliance": {},
            "creationTime": now(),
            "endTime": now(),
            "invoker": "User",
            "policy": {},
            "resilienceScore": {"disruptionScore": {}, "score": 0.0},
            "resourceErrorsCount": 0,
            "tags": {},
        });
        if let Some(tags) = params.get("tags").and_then(Value::as_object) {
            let _entry = self.tags.entry(assessment_arn.clone()).or_default();
            let mut map = assessment["tags"].as_object().cloned().unwrap_or_default();
            for (k, v) in tags {
                map.insert(k.clone(), v.clone());
            }
            assessment["tags"] = Value::Object(map);
        }
        self.assessments.insert(assessment_arn, assessment.clone());
        Response::json_status(200, &json!({"assessment": assessment}))
    }

    fn describe_assessment(&self, params: &Value) -> Response {
        let arn = params
            .get("assessmentArn")
            .and_then(Value::as_str)
            .unwrap_or("");
        match self.assessments.get(arn) {
            Some(a) => Response::json_status(200, &json!({"assessment": a})),
            None => crate::proto::json_error(
                404,
                "ResourceNotFoundException",
                &format!("AppAssessment with assessmentArn {arn} not found"),
            ),
        }
    }

    fn delete_assessment(&mut self, params: &Value) -> Response {
        let arn = params
            .get("assessmentArn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if self.assessments.remove(&arn).is_none() {
            return crate::proto::json_error(
                404,
                "ResourceNotFoundException",
                &format!("AppAssessment with assessmentArn {arn} not found"),
            );
        }
        Response::json_status(
            200,
            &json!({"assessmentArn": arn, "assessmentStatus": "Success"}),
        )
    }

    fn list_assessments(&self) -> Response {
        Response::json_status(200, &json!({"assessmentSummaries": []}))
    }

    fn list_app_input_sources(&self, params: &Value) -> Response {
        let arn = params.get("appArn").and_then(Value::as_str).unwrap_or("");
        let app = match app_or(self, arn) {
            Ok(app) => app,
            Err(resp) => return resp,
        };
        let _ = app;
        Response::json_status(200, &json!({"appInputSources": [], "nextToken": null}))
    }

    fn list_unsupported_app_version_resources(&self, params: &Value) -> Response {
        let arn = params.get("appArn").and_then(Value::as_str).unwrap_or("");
        match app_or(self, arn) {
            Ok(_) => Response::json_status(
                200,
                &json!({
                    "unsupportedResources": [],
                    "nextToken": null,
                }),
            ),
            Err(resp) => resp,
        }
    }

    fn start_metrics_export(&mut self, params: &Value) -> Response {
        let export_id = uuid::Uuid::new_v4().to_string();
        let bucket = params.get("bucketName").cloned().unwrap_or(json!(null));
        self.metrics_exports.insert(
            export_id.clone(),
            json!({
                "metricsExportId": export_id,
                "status": "Success",
                "exportLocation": {
                    "bucket": bucket,
                    "prefix": format!("resiliencehub-metrics/{export_id}"),
                },
            }),
        );
        Response::json_status(
            200,
            &json!({"metricsExportId": export_id, "status": "Success"}),
        )
    }

    fn describe_metrics_export(&self, params: &Value) -> Response {
        let id = params
            .get("metricsExportId")
            .and_then(Value::as_str)
            .unwrap_or("");
        match self.metrics_exports.get(id) {
            Some(record) => Response::json_status(200, record),
            None => Response::json_status(
                200,
                &json!({
                    "metricsExportId": id,
                    "status": "Failed",
                    "errorMessage": "Export not found",
                }),
            ),
        }
    }

    fn start_grouping_task(&mut self, params: &Value) -> Response {
        let arn = params.get("appArn").and_then(Value::as_str).unwrap_or("");
        if app_or(self, arn).is_err() {
            return app_not_found(arn);
        }
        let grouping_id = uuid::Uuid::new_v4().to_string();
        let record = json!({
            "appArn": arn,
            "groupingId": grouping_id,
            "status": "Success",
        });
        self.grouping_tasks.insert(grouping_id, record.clone());
        Response::json_status(200, &record)
    }

    fn describe_grouping_task(&self, params: &Value) -> Response {
        let arn = params.get("appArn").and_then(Value::as_str).unwrap_or("");
        let grouping_id = params.get("groupingId").and_then(Value::as_str);
        if app_or(self, arn).is_err() {
            return app_not_found(arn);
        }
        match grouping_id.and_then(|id| self.grouping_tasks.get(id)) {
            Some(record) => Response::json_status(200, record),
            None => Response::json_status(
                200,
                &json!({
                    "groupingId": grouping_id.map(String::from).unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
                    "status": "Success",
                }),
            ),
        }
    }

    fn list_grouping_recommendations(&self, params: &Value) -> Response {
        let arn = params.get("appArn").and_then(Value::as_str).unwrap_or("");
        if app_or(self, arn).is_err() {
            return app_not_found(arn);
        }
        Response::json_status(
            200,
            &json!({"groupingRecommendations": [], "nextToken": null}),
        )
    }

    fn grouping_decisions(&self, params: &Value, failed: Value) -> Response {
        let arn = params.get("appArn").and_then(Value::as_str).unwrap_or("");
        if app_or(self, arn).is_err() {
            return app_not_found(arn);
        }
        Response::json_status(200, &json!({"appArn": arn, "failedEntries": failed}))
    }

    fn create_recommendation_template(&mut self, params: &Value) -> Response {
        let assessment_arn = params
            .get("assessmentArn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let name = params
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if !assessment_arn.is_empty() && !self.assessments.contains_key(&assessment_arn) {
            return crate::proto::json_error(
                404,
                "ResourceNotFoundException",
                &format!("AppAssessment with assessmentArn {assessment_arn} not found"),
            );
        }
        let arn = format!(
            "arn:aws:resiliencehub:{region}:{account}:recommendation-template/{uuid}",
            region = self.region,
            account = self.account,
            uuid = uuid::Uuid::new_v4()
        );
        let bucket = params
            .get("bucketName")
            .and_then(Value::as_str)
            .unwrap_or(&format!("resiliencehub-{}-{}", self.region, self.account))
            .to_string();
        let record = json!({
            "assessmentArn": assessment_arn,
            "endTime": now(),
            "format": params.get("format").cloned().unwrap_or(json!("CfnYaml")),
            "name": name,
            "recommendationTemplateArn": arn.clone(),
            "recommendationTypes": params.get("recommendationTypes").cloned().unwrap_or(json!(["Alarm", "Sop", "Test"])),
            "startTime": now(),
            "status": "Success",
            "tags": {},
            "templatesLocation": {
                "bucket": bucket,
                "prefix": format!("recommendations/{}", uuid::Uuid::new_v4()),
            },
        });
        self.recommendation_templates_insert(arn, record.clone());
        Response::json_status(200, &json!({"recommendationTemplate": record}))
    }

    fn recommendation_templates_insert(&mut self, arn: String, record: Value) {
        self.assessment_templates.insert(arn, record);
    }

    fn delete_recommendation_template(&mut self, params: &Value) -> Response {
        let arn = params
            .get("recommendationTemplateArn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if self.assessment_templates.remove(&arn).is_none() {
            return crate::proto::json_error(
                404,
                "ResourceNotFoundException",
                &format!("RecommendationTemplate with recommendationTemplateArn {arn} not found"),
            );
        }
        Response::json_status(
            200,
            &json!({"recommendationTemplateArn": arn, "status": "Success"}),
        )
    }

    fn delete_app_input_source(&mut self, params: &Value) -> Response {
        let arn = params
            .get("appArn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if app_or(self, &arn).is_err() {
            return app_not_found(&arn);
        }
        let removed = if let Some(source_arn) = params.get("sourceArn").and_then(Value::as_str) {
            json!({"sourceArn": source_arn, "importType": "CFN_STACK"})
        } else if let Some(ns) = params.get("eksSourceClusterNamespace") {
            json!({"eksSourceClusterNamespace": ns, "importType": "EKS"})
        } else if let Some(tf) = params.get("terraformSource") {
            json!({"terraformSource": tf, "importType": "TERRAFORM"})
        } else {
            json!({})
        };
        if let Some(source_arn) = params.get("sourceArn").and_then(Value::as_str) {
            let app = self.apps.get_mut(&arn).unwrap();
            app.input_sources = json!(app
                .input_sources
                .as_array()
                .map(|list| {
                    list.iter()
                        .filter(|s| s.get("sourceArn").and_then(Value::as_str) != Some(source_arn))
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default());
        }
        Response::json_status(200, &json!({"appArn": arn, "appInputSource": removed}))
    }

    fn batch_update_recommendation_status(&self, params: &Value) -> Response {
        let arn = params.get("appArn").and_then(Value::as_str).unwrap_or("");
        if app_or(self, arn).is_err() {
            return app_not_found(arn);
        }
        let successful: Vec<Value> = params
            .get("requestEntries")
            .and_then(Value::as_array)
            .map(|list| {
                list.iter()
                    .map(|entry| {
                        json!({
                            "entryId": entry.get("entryId").cloned().unwrap_or(json!("")),
                            "excluded": entry.get("excluded").cloned().unwrap_or(json!(false)),
                            "excludeReason": entry.get("excludeReason").cloned().unwrap_or(json!("")),
                            "item": entry.get("item").cloned().unwrap_or(json!({})),
                            "referenceId": entry.get("referenceId").cloned().unwrap_or(json!("")),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Response::json_status(
            200,
            &json!({
                "appArn": arn,
                "failedEntries": [],
                "successfulEntries": successful,
            }),
        )
    }
}

fn draft_status(_draft: &Version) -> &'static str {
    "Pending"
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::HashMap;

    fn req(method: &str, path: &str, body: Value) -> Request {
        let (path, query_string) = match path.split_once('?') {
            Some((p, q)) => (p.to_string(), q.to_string()),
            None => (path.to_string(), String::new()),
        };
        Request {
            service: "resiliencehub".into(),
            operation: String::new(),
            method: method.into(),
            path: path.into(),
            query_string,
            headers: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            account: 123456789001,
            region: "us-east-1".into(),
        }
    }

    fn json_body(resp: Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap()
    }

    #[test]
    fn app_lifecycle() {
        let created = json_body(dispatch(&req(
            "POST",
            "/create-app",
            json!({"name": "appA", "description": "yes", "policyArn": "arn:policy"}),
        )));
        let app = &created["app"];
        let arn = app["appArn"].as_str().unwrap().to_string();
        assert!(arn.contains("app/"));
        assert_eq!(app["name"], "appA");
        assert_eq!(app["status"], "Active");
        assert!(app["creationTime"].is_number());

        let described = json_body(dispatch(&req(
            "POST",
            "/describe-app",
            json!({"appArn": arn}),
        )));
        assert_eq!(described["app"]["name"], "appA");

        let list = json_body(dispatch(&req("GET", "/list-apps?maxResults=10", json!({}))));
        assert!(list["appSummaries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|a| a["appArn"] == arn));

        // draft exists
        let versions = json_body(dispatch(&req(
            "POST",
            "/list-app-versions",
            json!({"appArn": arn}),
        )));
        assert!(versions["appVersions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["appVersion"] == "draft"));

        assert_eq!(
            dispatch(&req("POST", "/delete-app", json!({"appArn": arn}))).status,
            200
        );
        let miss = dispatch(&req("POST", "/describe-app", json!({"appArn": arn})));
        assert_eq!(miss.status, 404);
    }

    #[test]
    fn policies_crud_with_validation() {
        let bad = dispatch(&req(
            "POST",
            "/create-resiliency-policy",
            json!({"policyName": "p", "tier": "NotApplicable", "policy": {"Nope": {"rpoInSecs": 1}}}),
        ));
        assert_eq!(bad.status, 400);
        let err: Value = serde_json::from_slice(&bad.body).unwrap();
        assert_eq!(err["__type"], "ValidationException");

        let created = json_body(dispatch(&req(
            "POST",
            "/create-resiliency-policy",
            json!({
                "policyName": "poly",
                "policyDescription": "d",
                "tier": "NotApplicable",
                "policy": {
                    "Software": {"rpoInSecs": 3600, "rtoInSecs": 3600},
                    "Hardware": {"rpoInSecs": 3600, "rtoInSecs": 3600},
                    "AZ": {"rpoInSecs": 3600, "rtoInSecs": 3600},
                },
            }),
        )));
        let policy_arn = created["policy"]["policyArn"].as_str().unwrap().to_string();
        assert!(policy_arn.contains("resiliency-policy/"));

        let listed = json_body(dispatch(&req(
            "GET",
            "/list-resiliency-policies",
            json!({}),
        )));
        assert!(listed["resiliencyPolicies"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["policyArn"] == policy_arn));

        let described = json_body(dispatch(&req(
            "POST",
            "/describe-resiliency-policy",
            json!({"policyArn": policy_arn}),
        )));
        assert_eq!(described["policy"]["policyName"], "poly");

        assert_eq!(
            dispatch(&req(
                "POST",
                "/delete-resiliency-policy",
                json!({"policyArn": policy_arn})
            ))
            .status,
            200
        );
    }

    #[test]
    fn app_versions_components_resources() {
        let created = json_body(dispatch(&req(
            "POST",
            "/create-app",
            json!({"name": "appV"}),
        )));
        let arn = created["app"]["appArn"].as_str().unwrap().to_string();

        let comp = json_body(dispatch(&req(
            "POST",
            "/create-app-version-app-component",
            json!({"appArn": arn, "name": "comp1", "type": "AWS::ResilienceHub::ComputeAppComponent"}),
        )));
        assert_eq!(comp["appComponent"]["name"], "comp1");

        let listed = json_body(dispatch(&req(
            "POST",
            "/list-app-version-app-components",
            json!({"appArn": arn, "appVersion": "draft"}),
        )));
        assert_eq!(listed["appComponents"].as_array().unwrap().len(), 1);

        let resource = json_body(dispatch(&req(
            "POST",
            "/create-app-version-resource",
            json!({
                "appArn": arn,
                "appComponents": ["comp1"],
                "logicalResourceId": {"identifier": "res1"},
                "physicalResourceId": "i-abc",
                "resourceType": "AWS::EC2::Instance",
            }),
        )));
        assert_eq!(resource["physicalResource"]["resourceName"], "res1");

        let resources = json_body(dispatch(&req(
            "POST",
            "/list-app-version-resources",
            json!({"appArn": arn, "appVersion": "draft"}),
        )));
        assert_eq!(resources["physicalResources"].as_array().unwrap().len(), 1);

        let published = json_body(dispatch(&req(
            "POST",
            "/publish-app-version",
            json!({"appArn": arn, "versionName": "release1"}),
        )));
        assert_eq!(published["versionName"], "release1");

        let assessment = json_body(dispatch(&req(
            "POST",
            "/start-app-assessment",
            json!({"appArn": arn, "appVersion": "release1", "assessmentName": "asm1"}),
        )));
        assert_eq!(assessment["assessment"]["assessmentStatus"], "Success");
        let saw = dispatch(&req(
            "POST",
            "/describe-app-assessment",
            json!({"assessmentArn": assessment["assessment"]["assessmentArn"]}),
        ));
        assert_eq!(saw.status, 200);
    }

    #[test]
    fn metrics_grouping_and_draft_ops() {
        let created = json_body(dispatch(&req(
            "POST",
            "/create-app",
            json!({"name": "appM"}),
        )));
        let arn = created["app"]["appArn"].as_str().unwrap().to_string();

        let export = json_body(dispatch(&req(
            "POST",
            "/start-metrics-export",
            json!({"bucketName": "b"}),
        )));
        assert_eq!(export["status"], "Success");
        let described = json_body(dispatch(&req(
            "POST",
            "/describe-metrics-export",
            json!({"metricsExportId": export["metricsExportId"]}),
        )));
        assert_eq!(described["status"], "Success");

        let task = json_body(dispatch(&req(
            "POST",
            "/start-resource-grouping-recommendation-task",
            json!({"appArn": arn}),
        )));
        assert_eq!(task["status"], "Success");

        let add = json_body(dispatch(&req(
            "POST",
            "/add-draft-app-version-resource-mappings",
            json!({
                "appArn": arn,
                "resourceMappings": [{"resourceName": "n1", "physicalResourceId": {"identifier": "p1"}}],
            }),
        )));
        assert_eq!(add["resourceMappings"].as_array().unwrap().len(), 1);

        let rm = dispatch(&req(
            "POST",
            "/remove-draft-app-version-resource-mappings",
            json!({"appArn": arn, "resourceNames": ["n1"]}),
        ));
        assert_eq!(rm.status, 200);

        let rows = json_body(dispatch(&req("POST", "/list-metrics", json!({}))));
        assert_eq!(rows["rows"].as_array().unwrap().len(), 0);

        let body = json_body(dispatch(&req(
            "POST",
            "/put-draft-app-version-template",
            json!({"appArn": arn, "appTemplateBody": "{}"}),
        )));
        assert_eq!(body["appVersion"], "draft");
        let template = json_body(dispatch(&req(
            "POST",
            "/describe-app-version-template",
            json!({"appArn": arn}),
        )));
        assert_eq!(template["appTemplateBody"], "{}");
    }
}
