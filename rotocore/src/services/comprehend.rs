//! AWS Comprehend backend — ported from moto's `comprehend` service.
//!
//! Moto's comprehend responses are produced by direct `json.dumps` handlers,
//! so null-valued members are emitted verbatim (no serializer pruning) —
//! `NextToken: null`, `activeModelArn: null` etc. are reproduced exactly.
//! Detection APIs return moto's canned heuristic payloads.

use crate::proto::parse_json_body;
use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use uuid::Uuid;

static SLOT: Lazy<store::Slot<ComprehendState>> =
    Lazy::new(|| store::slot::<ComprehendState>("comprehend"));

#[derive(Default)]
struct ComprehendState {
    recognizers: HashMap<String, Value>,
    classifiers: HashMap<String, Value>,
    endpoints: HashMap<String, Value>,
    flywheels: HashMap<String, Value>,
    datasets: HashMap<String, Value>,
    resource_policies: HashMap<String, Value>,
    jobs: HashMap<String, Value>,
    /// Internal resource names for arn-keyed entities (filters only).
    names: HashMap<String, String>,
    /// ARN -> tags (moto's TaggingService shape).
    tags: HashMap<String, Vec<Value>>,
    region: String,
    account: String,
}

pub fn dispatch(req: &Request) -> Response {
    let params = parse_json_body(&req.body);
    let mut guard = SLOT.write();
    let st = guard.entry((req.account, req.region.clone())).or_default();
    if st.region.is_empty() {
        st.region = req.region.clone();
        st.account = req.account.to_string();
    }
    match req.operation.as_str() {
        "ListEntityRecognizers" => st.op_list_entity_recognizers(&params),
        "CreateEntityRecognizer" => st.op_create_entity_recognizer(&params),
        "DescribeEntityRecognizer" => st.op_describe_by_arn(
            &params,
            "EntityRecognizerArn",
            "recognizers",
            "EntityRecognizerProperties",
        ),
        "StopTrainingEntityRecognizer" => {
            st.op_stop_training(&params, "EntityRecognizerArn", "recognizers", "TRAINED")
        }
        "DeleteEntityRecognizer" => {
            st.op_delete_by_arn(&params, "EntityRecognizerArn", "recognizers")
        }
        "ListDocumentClassifiers" => st.op_list_document_classifiers(&params),
        "CreateDocumentClassifier" => st.op_create_document_classifier(&params),
        "DescribeDocumentClassifier" => st.op_describe_by_arn(
            &params,
            "DocumentClassifierArn",
            "classifiers",
            "DocumentClassifierProperties",
        ),
        "StopTrainingDocumentClassifier" => {
            st.op_stop_training(&params, "DocumentClassifierArn", "classifiers", "TRAINING")
        }
        "DeleteDocumentClassifier" => {
            st.op_delete_by_arn(&params, "DocumentClassifierArn", "classifiers")
        }
        "ListDocumentClassifierSummaries" => st.op_list_classifier_summaries(),
        "ListEntityRecognizerSummaries" => st.op_list_recognizer_summaries(),
        "CreateEndpoint" => st.op_create_endpoint(&params),
        "DescribeEndpoint" => {
            st.op_describe_by_arn(&params, "EndpointArn", "endpoints", "EndpointProperties")
        }
        "UpdateEndpoint" => st.op_update_endpoint(&params),
        "DeleteEndpoint" => st.op_delete_by_arn(&params, "EndpointArn", "endpoints"),
        "ListEndpoints" => st.op_list_endpoints(&params),
        "CreateFlywheel" => st.op_create_flywheel(&params),
        "DescribeFlywheel" => {
            st.op_describe_by_arn(&params, "FlywheelArn", "flywheels", "FlywheelProperties")
        }
        "UpdateFlywheel" => st.op_update_flywheel(&params),
        "DeleteFlywheel" => st.op_delete_by_arn(&params, "FlywheelArn", "flywheels"),
        "ListFlywheels" => st.op_list_flywheels(&params),
        "StartFlywheelIteration" => st.op_start_flywheel_iteration(&params),
        "DescribeFlywheelIteration" => st.op_describe_flywheel_iteration(&params),
        "ListFlywheelIterationHistory" => st.op_list_flywheel_iteration_history(&params),
        "CreateDataset" => st.op_create_dataset(&params),
        "DescribeDataset" => st.op_describe_dataset(&params),
        "ListDatasets" => st.op_list_datasets(&params),
        "PutResourcePolicy" => st.op_put_resource_policy(&params),
        "DescribeResourcePolicy" => st.op_describe_resource_policy(&params),
        "DeleteResourcePolicy" => st.op_delete_resource_policy(&params),
        "ImportModel" => st.op_import_model(&params),
        "TagResource" => st.op_tag_resource(&params),
        "UntagResource" => st.op_untag_resource(&params),
        "ListTagsForResource" => st.op_list_tags_for_resource(&params),
        "DetectSentiment" => st.op_detect_sentiment(&params),
        "DetectKeyPhrases" => st.op_detect_key_phrases(&params),
        "DetectPiiEntities" => st.op_detect_pii_entities(&params),
        "DetectDominantLanguage" => st.op_detect_dominant_language(&params),
        "DetectEntities" => st.op_detect_entities(&params),
        "DetectSyntax" => st.op_detect_syntax(&params),
        "DetectTargetedSentiment" => st.op_detect_targeted_sentiment(&params),
        "ContainsPiiEntities" => st.op_contains_pii_entities(&params),
        "ClassifyDocument" => st.op_classify_document(&params),
        "DetectToxicContent" => st.op_detect_toxic_content(&params),
        "BatchDetectDominantLanguage" => st.op_batch_detect_dominant_language(&params),
        "BatchDetectEntities" => st.op_batch("Entities", &params),
        "BatchDetectKeyPhrases" => st.op_batch("KeyPhrases", &params),
        "BatchDetectSyntax" => st.op_batch("SyntaxTokens", &params),
        "BatchDetectTargetedSentiment" => st.op_batch("Entities", &params),
        "BatchDetectSentiment" => st.op_batch_detect_sentiment(&params),
        other => {
            for job_type in JOB_TYPES {
                if other == format!("Start{job_type}Job") {
                    return st.op_start_job(job_type, &params);
                }
                if other == format!("Describe{job_type}Job") {
                    return st.op_describe_job(&params);
                }
                if other == format!("Stop{job_type}Job") {
                    return st.op_stop_job(&params);
                }
                if other == format!("List{job_type}Jobs") {
                    return st.op_list_jobs(job_type, &params);
                }
            }
            crate::proto::json_error(
                501,
                "NotImplemented",
                &format!("The {} action has not been implemented", req.operation),
            )
        }
    }
}

/// The nine asynchronous-job families sharing one generic model.
const JOB_TYPES: [&str; 9] = [
    "DocumentClassification",
    "DominantLanguageDetection",
    "EntitiesDetection",
    "EventsDetection",
    "KeyPhrasesDetection",
    "PiiEntitiesDetection",
    "SentimentDetection",
    "TargetedSentimentDetection",
    "TopicsDetection",
];

// --- Shared helpers --------------------------------------------------------

fn now_iso() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.6f+00:00")
        .to_string()
}

fn json_error(status: u16, code: &str, message: &str) -> Response {
    Response::json_status(status, &json!({ "__type": code, "message": message }))
}

fn resource_not_found() -> Response {
    json_error(
        400,
        "ResourceNotFoundException",
        "RESOURCE_NOT_FOUND: Could not find specified resource.",
    )
}

fn invalid_request(message: &str) -> Response {
    json_error(400, "InvalidRequestException", message)
}

/// Invalid LanguageCode in the detect family. botocore's comprehend model
/// carries InvalidRequestException (ValidationException is not modeled for
/// the service), so the enum violation surfaces typed.
fn unsupported_language(language: &str, languages: &[&str]) -> Response {
    let joined = languages.join(", ");
    json_error(
        400,
        "InvalidRequestException",
        &format!(
            "Value '{}' at 'languageCode'failed to satisfy constraint: Member must satisfy enum value set: {}",
            language, joined
        ),
    )
}

fn text_size_exceeded(size: usize) -> Response {
    json_error(
        400,
        "TextSizeLimitExceededException",
        &format!(
            "Input text size exceeds limit. Max length of request text allowed is 100000 bytes while in this request the text size is {size} bytes"
        ),
    )
}

fn ok(body: Value) -> Response {
    Response::json_status(200, &body)
}

const DETECT_KEY_PHRASES_LANGUAGES: [&str; 12] = [
    "ar", "hi", "ko", "zh-TW", "ja", "zh", "de", "pt", "en", "it", "fr", "es",
];

fn sentiment_score() -> Value {
    json!({
        "Positive": 0.008101312443614006,
        "Negative": 0.0002824589901138097,
        "Neutral": 0.9916020035743713,
        "Mixed": 1.4156351426208857e-05,
    })
}

/// CamelCase job type → kebab-case for the job ARN path
/// (`PiiEntitiesDetection` → `pii-entities-detection`).
fn job_type_kebab(job_type: &str) -> String {
    let mut out = String::new();
    for c in job_type.chars() {
        if c.is_uppercase() {
            if !out.is_empty() {
                out.push('-');
            }
            out.extend(c.to_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

impl ComprehendState {
    fn region(&self) -> &str {
        if self.region.is_empty() {
            "us-east-1"
        } else {
            &self.region
        }
    }

    fn with_arn(&self, kind: &str, name: &str) -> String {
        format!(
            "arn:aws:comprehend:{r}:{a}:{kind}/{name}",
            r = self.region(),
            a = self.account
        )
    }

    // ------------------------------------------------------------------
    // Generic arn-keyed resources
    // ------------------------------------------------------------------

    /// Describe{Resource} against one of the arn-keyed stores.
    fn op_describe_by_arn(
        &mut self,
        p: &Value,
        param: &str,
        store: &str,
        wrapper: &str,
    ) -> Response {
        let arn = p.get(param).and_then(Value::as_str).unwrap_or("");
        let found = match store {
            "recognizers" => self.recognizers.get(arn),
            "classifiers" => self.classifiers.get(arn),
            "endpoints" => self.endpoints.get(arn),
            "flywheels" => self.flywheels.get(arn),
            _ => None,
        };
        match found {
            Some(v) => ok(json!({ wrapper: v.clone() })),
            None => resource_not_found(),
        }
    }

    fn op_delete_by_arn(&mut self, p: &Value, param: &str, store: &str) -> Response {
        let arn = p.get(param).and_then(Value::as_str).unwrap_or("");
        match store {
            "recognizers" => {
                self.recognizers.remove(arn);
            }
            "classifiers" => {
                self.classifiers.remove(arn);
            }
            "endpoints" => {
                self.endpoints.remove(arn);
            }
            "flywheels" => {
                self.flywheels.remove(arn);
            }
            _ => {}
        }
        ok(json!({}))
    }

    fn op_stop_training(
        &mut self,
        p: &Value,
        param: &str,
        store: &str,
        training_status: &str,
    ) -> Response {
        let arn = p.get(param).and_then(Value::as_str).unwrap_or("");
        let found = match store {
            "recognizers" => self.recognizers.get_mut(arn),
            "classifiers" => self.classifiers.get_mut(arn),
            _ => None,
        };
        match found {
            Some(v) => {
                if v.get("Status").and_then(Value::as_str) == Some(training_status) {
                    v["Status"] = json!("STOP_REQUESTED");
                }
                ok(json!({}))
            }
            None => resource_not_found(),
        }
    }

    // ------------------------------------------------------------------
    // Recognizers / classifiers
    // ------------------------------------------------------------------

    fn op_list_entity_recognizers(&mut self, p: &Value) -> Response {
        let name_filter = p
            .get("Filter")
            .and_then(|f| f.get("RecognizerName"))
            .and_then(Value::as_str);
        let names = &self.names;
        let list: Vec<Value> = self
            .recognizers
            .values()
            .filter(|r| {
                name_filter
                    .map(|n| {
                        r.get("EntityRecognizerArn")
                            .and_then(Value::as_str)
                            .and_then(|a| names.get(a))
                            .map(|name| name == n)
                            .unwrap_or(false)
                    })
                    .unwrap_or(true)
            })
            .cloned()
            .collect();
        ok(json!({"EntityRecognizerPropertiesList": list, "NextToken": null}))
    }

    fn op_create_entity_recognizer(&mut self, p: &Value) -> Response {
        let name = p
            .get("RecognizerName")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let version = p.get("VersionName").and_then(Value::as_str);
        let mut arn = self.with_arn("entity-recognizer", name);
        if let Some(v) = version {
            arn = format!("{arn}/version/{v}");
        }
        let props = json!({
            "EntityRecognizerArn": arn.clone(),
            "LanguageCode": p.get("LanguageCode"),
            "Status": "TRAINED",
            "InputDataConfig": p.get("InputDataConfig"),
            "DataAccessRoleArn": p.get("DataAccessRoleArn"),
            "VersionName": p.get("VersionName"),
            "VolumeKmsKeyId": p.get("VolumeKmsKeyId"),
            "VpcConfig": p.get("VpcConfig"),
            "ModelKmsKeyId": p.get("ModelKmsKeyId"),
            "ModelPolicy": p.get("ModelPolicy"),
        });
        self.names.insert(arn.clone(), name.to_string());
        self.recognizers.insert(arn.clone(), props);
        self.tag_init(&arn, p);
        ok(json!({"EntityRecognizerArn": arn}))
    }

    fn op_list_document_classifiers(&mut self, p: &Value) -> Response {
        let filter = p.get("Filter");
        let name_filter = filter
            .and_then(|f| f.get("DocumentClassifierName"))
            .and_then(Value::as_str);
        let status_filter = filter.and_then(|f| f.get("Status")).and_then(Value::as_str);
        let names = &self.names;
        let list: Vec<Value> = self
            .classifiers
            .values()
            .filter(|c| {
                if let Some(n) = name_filter {
                    if c.get("DocumentClassifierArn")
                        .and_then(Value::as_str)
                        .and_then(|a| names.get(a))
                        .map(|name| name != n)
                        .unwrap_or(true)
                    {
                        return false;
                    }
                }
                if let Some(s) = status_filter {
                    if c.get("Status").and_then(Value::as_str) != Some(s) {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect();
        ok(json!({"DocumentClassifierPropertiesList": list, "NextToken": null}))
    }

    fn op_create_document_classifier(&mut self, p: &Value) -> Response {
        let name = p
            .get("DocumentClassifierName")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let version = p
            .get("VersionName")
            .and_then(Value::as_str)
            .map(String::from)
            .unwrap_or_else(|| "None".to_string());
        let arn = format!(
            "arn:aws:comprehend:{r}:{a}:document-classifier/{name}/{version}",
            r = self.region(),
            a = self.account
        );
        let props = json!({
            "DocumentClassifierArn": arn.clone(),
            "LanguageCode": p.get("LanguageCode"),
            "Status": "TRAINING",
            "InputDataConfig": p.get("InputDataConfig"),
            "OutputDataConfig": p.get("OutputDataConfig"),
            "DataAccessRoleArn": p.get("DataAccessRoleArn"),
            "VolumeKmsKeyId": p.get("VolumeKmsKeyId"),
            "Mode": p.get("Mode"),
            "VpcConfig": p.get("VpcConfig"),
            "ModelKmsKeyId": p.get("ModelKmsKeyId"),
            "ModelPolicy": p.get("ModelPolicy"),
        });
        self.names.insert(arn.clone(), name.to_string());
        self.classifiers.insert(arn.clone(), props);
        self.tag_init(&arn, p);
        ok(json!({"DocumentClassifierArn": arn}))
    }

    fn op_list_recognizer_summaries(&mut self) -> Response {
        let summaries: Vec<Value> = self
            .recognizers
            .values()
            .map(|r| {
                json!({
                    "RecognizerName": self.names.get(r.get("EntityRecognizerArn").and_then(Value::as_str).unwrap_or("")).cloned().unwrap_or_default(),
                    "NumberOfVersions": 1,
                    "LatestVersionCreatedAt": null,
                    "LatestVersionName": null,
                    "LatestVersionStatus": r.get("Status"),
                })
            })
            .collect();
        ok(json!({"EntityRecognizerSummariesList": summaries, "NextToken": null}))
    }

    fn op_list_classifier_summaries(&mut self) -> Response {
        let summaries: Vec<Value> = self
            .classifiers
            .values()
            .map(|c| {
                json!({
                    "DocumentClassifierName": self.names.get(c.get("DocumentClassifierArn").and_then(Value::as_str).unwrap_or("")).cloned().unwrap_or_default(),
                    "NumberOfVersions": 1,
                    "LatestVersionCreatedAt": null,
                    "LatestVersionName": c.get("VersionName"),
                    "LatestVersionStatus": c.get("Status"),
                })
            })
            .collect();
        ok(json!({"DocumentClassifierSummariesList": summaries, "NextToken": null}))
    }

    // ------------------------------------------------------------------
    // Endpoints
    // ------------------------------------------------------------------

    fn op_create_endpoint(&mut self, p: &Value) -> Response {
        let name = p
            .get("EndpointName")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let model_arn = p
            .get("ModelArn")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let arn = format!(
            "arn:aws:comprehend:{r}:{a}:endpoint/{name}/{model_arn}",
            r = self.region(),
            a = self.account
        );
        let props = json!({
            "EndpointArn": arn.clone(),
            "ModelArn": model_arn,
            "ClientRequestToken": p.get("ClientRequestToken"),
            "DataAccessRoleArn": p.get("DataAccessRoleArn"),
            "FlywheelArn": p.get("FlywheelArn"),
            "DesiredInferenceUnits": p.get("DesiredInferenceUnits"),
            "Status": "IN_SERVICE",
        });
        self.endpoints.insert(arn.clone(), props.clone());
        self.tag_init(&arn, p);
        ok(json!({"EndpointArn": arn, "ModelArn": model_arn}))
    }

    fn op_update_endpoint(&mut self, p: &Value) -> Response {
        let desired = p.get("DesiredModelArn").cloned().unwrap_or(Value::Null);
        if let Some(arn) = p.get("EndpointArn").and_then(Value::as_str) {
            if let Some(ep) = self.endpoints.get_mut(arn) {
                if !desired.is_null() {
                    ep["ModelArn"] = desired.clone();
                }
                if let Some(units) = p.get("DesiredInferenceUnits") {
                    ep["DesiredInferenceUnits"] = units.clone();
                }
                if let Some(role) = p.get("DesiredDataAccessRoleArn") {
                    ep["DataAccessRoleArn"] = role.clone();
                }
            }
        }
        ok(json!({"DesiredModelArn": desired}))
    }

    fn op_list_endpoints(&mut self, p: &Value) -> Response {
        let filter = p.get("Filter");
        let model_filter = filter
            .and_then(|f| f.get("ModelArn"))
            .and_then(Value::as_str);
        let status_filter = filter.and_then(|f| f.get("Status")).and_then(Value::as_str);
        let list: Vec<Value> = self
            .endpoints
            .values()
            .filter(|e| {
                if let Some(m) = model_filter {
                    if e.get("ModelArn").and_then(Value::as_str) != Some(m) {
                        return false;
                    }
                }
                if let Some(s) = status_filter {
                    if e.get("Status").and_then(Value::as_str) != Some(s) {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect();
        ok(json!({"EndpointPropertiesList": list, "NextToken": null}))
    }

    // ------------------------------------------------------------------
    // Flywheels and datasets
    // ------------------------------------------------------------------

    fn op_create_flywheel(&mut self, p: &Value) -> Response {
        let name = p
            .get("FlywheelName")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let arn = self.with_arn("flywheel", name);
        let props = json!({
            "FlywheelArn": arn.clone(),
            "ActiveModelArn": p.get("ActiveModelArn"),
            "DataAccessRoleArn": p.get("DataAccessRoleArn"),
            "TaskConfig": p.get("TaskConfig"),
            "ModelType": p.get("ModelType"),
            "DataLakeS3Uri": p.get("DataLakeS3Uri"),
            "DataSecurityConfig": p.get("DataSecurityConfig"),
            "ClientRequestToken": p.get("ClientRequestToken"),
            "Status": "ACTIVE",
        });
        self.flywheels.insert(arn.clone(), props.clone());
        self.tag_init(&arn, p);
        ok(
            json!({"FlywheelArn": arn, "activeModelArn": p.get("ActiveModelArn").cloned().unwrap_or(Value::Null)}),
        )
    }

    fn op_update_flywheel(&mut self, p: &Value) -> Response {
        let arn = p.get("FlywheelArn").and_then(Value::as_str).unwrap_or("");
        let Some(fw) = self.flywheels.get_mut(arn) else {
            return resource_not_found();
        };
        for (req_key, prop_key) in [
            ("ActiveModelArn", "ActiveModelArn"),
            ("DataAccessRoleArn", "DataAccessRoleArn"),
            ("DataSecurityConfig", "DataSecurityConfig"),
        ] {
            if let Some(v) = p.get(req_key) {
                fw[prop_key] = v.clone();
            }
        }
        // Status is excluded from the FlywheelProperties payload
        let out = drop_key(fw, "Status");
        ok(json!({"FlywheelProperties": out}))
    }

    fn op_list_flywheels(&mut self, p: &Value) -> Response {
        let status_filter = p
            .get("Filter")
            .and_then(|f| f.get("Status"))
            .and_then(Value::as_str);
        let list: Vec<Value> = self
            .flywheels
            .values()
            .filter(|f| {
                status_filter
                    .map(|s| f.get("Status").and_then(Value::as_str) == Some(s))
                    .unwrap_or(true)
            })
            .map(|f| drop_key(f, "Status"))
            .collect();
        ok(json!({"FlywheelSummaryList": list, "nextToken": null}))
    }

    fn op_start_flywheel_iteration(&mut self, p: &Value) -> Response {
        let arn = p.get("FlywheelArn").and_then(Value::as_str).unwrap_or("");
        if !self.flywheels.contains_key(arn) {
            return resource_not_found();
        }
        ok(json!({
            "FlywheelArn": arn,
            "FlywheelIterationId": rand_int(),
        }))
    }

    fn op_describe_flywheel_iteration(&mut self, p: &Value) -> Response {
        let arn = p.get("FlywheelArn").and_then(Value::as_str).unwrap_or("");
        if !self.flywheels.contains_key(arn) {
            return resource_not_found();
        }
        ok(json!({
            "FlywheelIterationProperties": {
                "FlywheelArn": arn,
                "FlywheelIterationId": p.get("FlywheelIterationId"),
                "Status": "COMPLETED",
            }
        }))
    }

    fn op_list_flywheel_iteration_history(&mut self, p: &Value) -> Response {
        let arn = p.get("FlywheelArn").and_then(Value::as_str).unwrap_or("");
        if !self.flywheels.contains_key(arn) {
            return resource_not_found();
        }
        ok(json!({"FlywheelIterationPropertiesList": [], "NextToken": null}))
    }

    fn op_create_dataset(&mut self, p: &Value) -> Response {
        let flywheel_arn = p.get("FlywheelArn").and_then(Value::as_str).unwrap_or("");
        if !self.flywheels.contains_key(flywheel_arn) {
            return resource_not_found();
        }
        let name = p
            .get("DatasetName")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let tail = flywheel_arn.rsplit('/').next().unwrap_or("");
        let arn = format!(
            "arn:aws:comprehend:{r}:{a}:flywheel/{tail}/dataset/{name}",
            r = self.region(),
            a = self.account
        );
        let dataset = json!({
            "DatasetArn": arn.clone(),
            "DatasetName": name,
            "DatasetType": p.get("DatasetType").cloned().unwrap_or(json!("TRAIN")),
            "Description": p.get("Description").cloned().unwrap_or(json!("")),
            "Status": "CREATING",
            "InputDataConfig": p.get("InputDataConfig"),
        });
        self.datasets.insert(arn.clone(), dataset);
        ok(json!({"DatasetArn": arn}))
    }

    fn op_describe_dataset(&mut self, p: &Value) -> Response {
        let arn = p.get("DatasetArn").and_then(Value::as_str).unwrap_or("");
        match self.datasets.get(arn) {
            Some(d) => ok(json!({"DatasetProperties": d.clone()})),
            None => resource_not_found(),
        }
    }

    fn op_list_datasets(&mut self, p: &Value) -> Response {
        let flywheel_arn = p.get("FlywheelArn").and_then(Value::as_str);
        if let Some(f) = flywheel_arn {
            if !self.flywheels.contains_key(f) {
                return resource_not_found();
            }
        }
        let list: Vec<Value> = self
            .datasets
            .values()
            .filter(|d| {
                flywheel_arn
                    .map(|f| {
                        d.get("DatasetArn")
                            .and_then(Value::as_str)
                            .map(|a| a.contains(f))
                            .unwrap_or(false)
                    })
                    .unwrap_or(true)
            })
            .cloned()
            .collect();
        ok(json!({"DatasetPropertiesList": list, "NextToken": null}))
    }

    // ------------------------------------------------------------------
    // Resource policies
    // ------------------------------------------------------------------

    fn op_put_resource_policy(&mut self, p: &Value) -> Response {
        let arn = p.get("ResourceArn").and_then(Value::as_str).unwrap_or("");
        let revision = Uuid::new_v4().to_string();
        let now = now_iso();
        let creation = self
            .resource_policies
            .get(arn)
            .and_then(|v| v.get("CreationTime").cloned())
            .unwrap_or_else(|| json!(now));
        self.resource_policies.insert(
            arn.to_string(),
            json!({
                "ResourcePolicy": p.get("ResourcePolicy"),
                "PolicyRevisionId": revision.clone(),
                "CreationTime": creation,
                "LastModifiedTime": now,
            }),
        );
        ok(json!({"PolicyRevisionId": revision}))
    }

    fn op_describe_resource_policy(&mut self, p: &Value) -> Response {
        let arn = p.get("ResourceArn").and_then(Value::as_str).unwrap_or("");
        match self.resource_policies.get(arn) {
            Some(policy) => ok(json!({
                "ResourcePolicy": policy.get("ResourcePolicy"),
                "CreationTime": policy.get("CreationTime"),
                "LastModifiedTime": policy.get("LastModifiedTime"),
                "PolicyRevisionId": policy.get("PolicyRevisionId"),
            })),
            None => resource_not_found(),
        }
    }

    fn op_delete_resource_policy(&mut self, p: &Value) -> Response {
        let arn = p.get("ResourceArn").and_then(Value::as_str).unwrap_or("");
        if self.resource_policies.remove(arn).is_none() {
            return resource_not_found();
        }
        ok(json!({}))
    }

    // ------------------------------------------------------------------
    // Import model + tags
    // ------------------------------------------------------------------

    fn op_import_model(&mut self, p: &Value) -> Response {
        let name = p
            .get("ModelName")
            .and_then(Value::as_str)
            .map(String::from)
            .unwrap_or_else(|| {
                p.get("SourceModelArn")
                    .and_then(Value::as_str)
                    .and_then(|a| a.rsplit('/').next())
                    .unwrap_or_default()
                    .to_string()
            });
        let arn = format!(
            "arn:aws:comprehend:{r}:{a}:document-classifier/{name}",
            r = self.region(),
            a = self.account
        );
        ok(json!({"ModelArn": arn}))
    }

    fn op_tag_resource(&mut self, p: &Value) -> Response {
        let arn = p.get("ResourceArn").and_then(Value::as_str).unwrap_or("");
        let tags = p
            .get("Tags")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let entry = self.tags.entry(arn.to_string()).or_default();
        for tag in tags {
            let key = tag.get("Key").and_then(Value::as_str).unwrap_or("");
            if !key.is_empty() {
                entry.retain(|t| t.get("Key").and_then(Value::as_str) != Some(key));
            }
            entry.push(tag);
        }
        ok(json!({}))
    }

    fn op_untag_resource(&mut self, p: &Value) -> Response {
        let arn = p.get("ResourceArn").and_then(Value::as_str).unwrap_or("");
        let keys: Vec<String> = p
            .get("TagKeys")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(String::from)
                    .collect()
            })
            .unwrap_or_default();
        if let Some(entry) = self.tags.get_mut(arn) {
            entry.retain(|t| {
                let k = t.get("Key").and_then(Value::as_str).unwrap_or("");
                !keys.contains(&k.to_string())
            });
        }
        ok(json!({}))
    }

    fn op_list_tags_for_resource(&mut self, p: &Value) -> Response {
        let arn = p.get("ResourceArn").and_then(Value::as_str).unwrap_or("");
        let tags = self.tags.get(arn).cloned().unwrap_or_default();
        ok(json!({"ResourceArn": arn, "Tags": tags}))
    }

    fn tag_init(&mut self, arn: &str, p: &Value) {
        if let Some(tags) = p
            .get("Tags")
            .filter(|t| t.as_array().is_some_and(|a| !a.is_empty()))
        {
            if let Some(arr) = tags.as_array() {
                let entry = self.tags.entry(arn.to_string()).or_default();
                entry.extend(arr.clone());
            }
        }
    }

    // ------------------------------------------------------------------
    // Detection APIs (canned payloads)
    // ------------------------------------------------------------------

    fn op_detect_sentiment(&mut self, p: &Value) -> Response {
        let text_len = p
            .get("Text")
            .and_then(Value::as_str)
            .map(str::len)
            .unwrap_or(0);
        if let Some(response) = check_text_size(text_len, 5000) {
            return response;
        }
        match check_language(p, &DETECT_KEY_PHRASES_LANGUAGES) {
            Some(response) => response,
            None => ok(json!({"Sentiment": "NEUTRAL", "SentimentScore": sentiment_score()})),
        }
    }

    fn op_detect_key_phrases(&mut self, p: &Value) -> Response {
        let text_len = p
            .get("Text")
            .and_then(Value::as_str)
            .map(str::len)
            .unwrap_or(0);
        if let Some(response) = check_text_size(text_len, 100000) {
            return response;
        }
        match check_language(p, &DETECT_KEY_PHRASES_LANGUAGES) {
            Some(response) => response,
            None => ok(json!({"KeyPhrases": canned_phrases()})),
        }
    }

    fn op_detect_pii_entities(&mut self, p: &Value) -> Response {
        let text_len = p
            .get("Text")
            .and_then(Value::as_str)
            .map(str::len)
            .unwrap_or(0);
        if let Some(response) = check_text_size(text_len, 100000) {
            return response;
        }
        match check_language(p, &["en"]) {
            Some(response) => response,
            None => ok(json!({"Entities": canned_pii()})),
        }
    }

    fn op_detect_dominant_language(&mut self, p: &Value) -> Response {
        let text_len = p
            .get("Text")
            .and_then(Value::as_str)
            .map(str::len)
            .unwrap_or(0);
        if let Some(response) = check_text_size(text_len, 100000) {
            return response;
        }
        ok(json!({"Languages": [{"LanguageCode": "en", "Score": 0.99}]}))
    }

    fn op_detect_entities(&mut self, p: &Value) -> Response {
        let text_len = p
            .get("Text")
            .and_then(Value::as_str)
            .map(str::len)
            .unwrap_or(0);
        if let Some(response) = check_text_size(text_len, 100000) {
            return response;
        }
        match check_language(p, &DETECT_KEY_PHRASES_LANGUAGES) {
            Some(response) => response,
            None => ok(json!({"Entities": []})),
        }
    }

    fn op_detect_syntax(&mut self, p: &Value) -> Response {
        let text_len = p
            .get("Text")
            .and_then(Value::as_str)
            .map(str::len)
            .unwrap_or(0);
        if let Some(response) = check_text_size(text_len, 5000) {
            return response;
        }
        match check_language(p, &["en", "es", "fr", "de", "it", "pt"]) {
            Some(response) => response,
            None => ok(json!({"SyntaxTokens": []})),
        }
    }

    fn op_detect_targeted_sentiment(&mut self, p: &Value) -> Response {
        let text_len = p
            .get("Text")
            .and_then(Value::as_str)
            .map(str::len)
            .unwrap_or(0);
        if let Some(response) = check_text_size(text_len, 5000) {
            return response;
        }
        match check_language(p, &DETECT_KEY_PHRASES_LANGUAGES) {
            Some(response) => response,
            None => ok(json!({"Entities": []})),
        }
    }

    fn op_contains_pii_entities(&mut self, p: &Value) -> Response {
        let text_len = p
            .get("Text")
            .and_then(Value::as_str)
            .map(str::len)
            .unwrap_or(0);
        if let Some(response) = check_text_size(text_len, 100000) {
            return response;
        }
        match check_language(p, &["en"]) {
            Some(response) => response,
            None => ok(json!({"Labels": []})),
        }
    }

    fn op_classify_document(&mut self, _p: &Value) -> Response {
        ok(json!({
            "Classes": [{"Name": "POSITIVE", "Score": 0.5}],
            "Labels": [],
        }))
    }

    fn op_detect_toxic_content(&mut self, p: &Value) -> Response {
        let segments = p
            .get("TextSegments")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let labels = json!([
            {"Name": "PROFANITY", "Score": 0.01},
            {"Name": "HATE_SPEECH", "Score": 0.01},
            {"Name": "INSULT", "Score": 0.01},
            {"Name": "GRAPHIC", "Score": 0.01},
            {"Name": "HARASSMENT_OR_ABUSE", "Score": 0.01},
            {"Name": "SEXUAL", "Score": 0.01},
            {"Name": "VIOLENCE_OR_THREAT", "Score": 0.01},
        ]);
        let result_list: Vec<Value> = (0..segments.len())
            .map(|_| json!({"Labels": labels.clone(), "Toxicity": 0.01}))
            .collect();
        ok(json!({"ResultList": result_list}))
    }

    fn op_batch(&mut self, result_key: &str, p: &Value) -> Response {
        let texts = p
            .get("TextList")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let result_list: Vec<Value> = texts
            .iter()
            .enumerate()
            .map(|(i, _)| json!({"Index": i, result_key: []}))
            .collect();
        ok(json!({"ResultList": result_list, "ErrorList": []}))
    }

    fn op_batch_detect_dominant_language(&mut self, p: &Value) -> Response {
        let texts = p
            .get("TextList")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let result_list: Vec<Value> = texts
            .iter()
            .enumerate()
            .map(|(i, _)| json!({"Index": i, "Languages": [{"LanguageCode": "en", "Score": 0.99}]}))
            .collect();
        ok(json!({"ResultList": result_list, "ErrorList": []}))
    }

    fn op_batch_detect_sentiment(&mut self, p: &Value) -> Response {
        let texts = p
            .get("TextList")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let result_list: Vec<Value> = texts
            .iter()
            .enumerate()
            .map(|(i, _)| {
                json!({"Index": i, "Sentiment": "NEUTRAL", "SentimentScore": sentiment_score()})
            })
            .collect();
        ok(json!({"ResultList": result_list, "ErrorList": []}))
    }
}

/// Map minus one key.
fn drop_key(v: &Value, key: &str) -> Value {
    match v {
        Value::Object(map) => {
            let mut out: Map<String, Value> = Map::new();
            for (k, val) in map {
                if k != key {
                    out.insert(k.clone(), val.clone());
                }
            }
            Value::Object(out)
        }
        other => other.clone(),
    }
}

/// Poor-man's random int for FlywheelIterationId (moto uses random.randint).
fn rand_int() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEED: AtomicU64 = AtomicU64::new(0);
    let base = chrono::Utc::now().timestamp_subsec_nanos() as u64;
    SEED.fetch_add(base % 1000 + 1, Ordering::Relaxed) % 1_000_000
}

fn check_text_size(size: usize, limit: usize) -> Option<Response> {
    if size > limit {
        return Some(text_size_exceeded(size));
    }
    None
}

fn check_language(p: &Value, languages: &[&str]) -> Option<Response> {
    let lang = p.get("LanguageCode").and_then(Value::as_str).unwrap_or("");
    if languages.contains(&lang) {
        return None;
    }
    Some(unsupported_language(lang, languages))
}

fn canned_phrases() -> Value {
    json!([
        {"Score": 0.9999890923500061, "BeginOffset": 50, "EndOffset": 58},
        {"Score": 0.9999966621398926, "BeginOffset": 230, "EndOffset": 259},
        {"Score": 0.9999954700469971, "BeginOffset": 334, "EndOffset": 349},
    ])
}

fn canned_pii() -> Value {
    json!([
        {"Score": 0.9999890923500061, "Type": "NAME", "BeginOffset": 50, "EndOffset": 58},
        {"Score": 0.9999966621398926, "Type": "EMAIL", "BeginOffset": 230, "EndOffset": 259},
        {"Score": 0.9999954700469971, "Type": "BANK_ACCOUNT_NUMBER", "BeginOffset": 334, "EndOffset": 349},
    ])
}

impl ComprehendState {
    fn op_start_job(&mut self, job_type: &str, p: &Value) -> Response {
        if job_type == "EventsDetection" && p.get("TargetEventTypes").is_none() {
            return invalid_request(
                "The request is missing the required parameter: TargetEventTypes.",
            );
        }
        let job_id = Uuid::new_v4().to_string();
        let kebab = job_type_kebab(job_type);
        let job_arn = format!(
            "arn:aws:comprehend:{r}:{a}:{kebab}-job/{job_id}",
            r = self.region(),
            a = self.account
        );
        let job_name = p
            .get("JobName")
            .and_then(Value::as_str)
            .map(String::from)
            .unwrap_or_else(|| format!("moto-job-{job_id}"));
        let mut job = json!({
            "JobId": job_id.clone(),
            "JobArn": job_arn.clone(),
            "JobName": job_name,
            "JobStatus": "SUBMITTED",
            "SubmitTime": now_iso(),
            "EndTime": null,
            "InputDataConfig": p.get("InputDataConfig"),
            "OutputDataConfig": p.get("OutputDataConfig"),
            "DataAccessRoleArn": p.get("DataAccessRoleArn"),
            "LanguageCode": p.get("LanguageCode").filter(|x| !x.is_null()),
        });
        // moto's ComprehendJob stores every un-parsed kwarg verbatim.
        if let Value::Object(params) = p {
            for (k, v) in params {
                if ![
                    "JobName",
                    "InputDataConfig",
                    "OutputDataConfig",
                    "DataAccessRoleArn",
                    "LanguageCode",
                ]
                .contains(&k.as_str())
                {
                    job[k] = v.clone();
                }
            }
        }
        job["job_type"] = json!(job_type);
        self.jobs.insert(job_id.clone(), job);
        ok(json!({"JobId": job_id, "JobArn": job_arn, "JobStatus": "SUBMITTED"}))
    }

    fn op_describe_job(&mut self, p: &Value) -> Response {
        let job_id = p.get("JobId").and_then(Value::as_str).unwrap_or("");
        let Some(job) = self.jobs.get(job_id) else {
            return resource_not_found();
        };
        let job_type = job.get("job_type").and_then(Value::as_str).unwrap_or("");
        let mut props = drop_key(job, "job_type");
        if let Some(t) = props.get_mut("SubmitTime") {
            if t.is_null() {
                // cannot happen; kept for parity
            }
        }
        ok(json!({ format!("{job_type}JobProperties"): props }))
    }

    fn op_stop_job(&mut self, p: &Value) -> Response {
        let job_id = p.get("JobId").and_then(Value::as_str).unwrap_or("");
        let Some(job) = self.jobs.get_mut(job_id) else {
            return resource_not_found();
        };
        let status = job.get("JobStatus").and_then(Value::as_str).unwrap_or("");
        if status == "SUBMITTED" || status == "IN_PROGRESS" {
            job["JobStatus"] = json!("STOP_REQUESTED");
            job["EndTime"] = json!(now_iso());
        }
        ok(json!({"JobId": job_id, "JobStatus": "STOP_REQUESTED"}))
    }

    fn op_list_jobs(&mut self, job_type: &str, p: &Value) -> Response {
        let filter = p.get("Filter");
        let name_filter = filter
            .and_then(|f| f.get("JobName"))
            .and_then(Value::as_str);
        let status_filter = filter
            .and_then(|f| f.get("JobStatus"))
            .and_then(Value::as_str);
        let mut list: Vec<Value> = self
            .jobs
            .values()
            .filter(|j| {
                if j.get("job_type").and_then(Value::as_str) != Some(job_type) {
                    return false;
                }
                if let Some(n) = name_filter {
                    if j.get("JobName").and_then(Value::as_str) != Some(n) {
                        return false;
                    }
                }
                if let Some(s) = status_filter {
                    if j.get("JobStatus").and_then(Value::as_str) != Some(s) {
                        return false;
                    }
                }
                true
            })
            .map(|j| drop_key(j, "job_type"))
            .collect();
        // Submit/End times in job dicts are already iso strings; nothing to do.
        let _ = &mut list;
        ok(json!({ format!("{job_type}JobPropertiesList"): list, "NextToken": null }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static ACCOUNT: AtomicU64 = AtomicU64::new(971_000_000_001);

    fn next_account() -> u64 {
        ACCOUNT.fetch_add(1, Ordering::Relaxed)
    }

    fn call(op: &str, params: Value, account: u64) -> (u16, Value) {
        let req = Request {
            service: "comprehend".to_string(),
            operation: op.to_string(),
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: serde_json::to_vec(&params).unwrap().into(),
            account,
            region: "us-east-1".to_string(),
        };
        let resp = dispatch(&req);
        (resp.status, serde_json::from_slice(&resp.body).unwrap())
    }

    #[test]
    fn endpoint_lifecycle() {
        let acct = next_account();
        let (s, r) = call(
            "CreateEndpoint",
            json!({"EndpointName": "ep1", "ModelArn": "arn:m", "DesiredInferenceUnits": 1, "ClientRequestToken": "t"}),
            acct,
        );
        assert_eq!(s, 200);
        let arn = r["EndpointArn"].as_str().unwrap().to_string();
        assert!(arn.contains(":endpoint/ep1/arn:m"));
        assert_eq!(r["ModelArn"], "arn:m");

        let (_s, r) = call("DescribeEndpoint", json!({"EndpointArn": arn}), acct);
        assert_eq!(r["EndpointProperties"]["Status"], "IN_SERVICE");
        assert_eq!(r["EndpointProperties"]["DesiredInferenceUnits"], 1);

        let (s, _r) = call(
            "UpdateEndpoint",
            json!({"EndpointArn": arn, "DesiredInferenceUnits": 2}),
            acct,
        );
        assert_eq!(s, 200);

        let (_s, r) = call("ListEndpoints", json!({}), acct);
        assert_eq!(r["EndpointPropertiesList"].as_array().unwrap().len(), 1);

        let (s, _) = call("DeleteEndpoint", json!({"EndpointArn": arn}), acct);
        assert_eq!(s, 200);
        let (s, _) = call("DescribeEndpoint", json!({"EndpointArn": arn}), acct);
        assert_eq!(s, 400);
    }

    #[test]
    fn detect_apis_canned() {
        let acct = next_account();
        let (_s, r) = call(
            "DetectSentiment",
            json!({"Text": "I am very happy", "LanguageCode": "en"}),
            acct,
        );
        assert_eq!(r["Sentiment"], "NEUTRAL");
        assert!(r["SentimentScore"]["Neutral"].as_f64().unwrap() > 0.99);

        let (_s, r) = call(
            "DetectKeyPhrases",
            json!({"Text": "rain", "LanguageCode": "en"}),
            acct,
        );
        assert_eq!(r["KeyPhrases"].as_array().unwrap().len(), 3);

        let (_s, r) = call(
            "DetectPiiEntities",
            json!({"Text": "SSN", "LanguageCode": "en"}),
            acct,
        );
        assert_eq!(r["Entities"][0]["Type"], "NAME");

        let (_s, r) = call("DetectDominantLanguage", json!({"Text": "hi"}), acct);
        assert_eq!(r["Languages"][0]["LanguageCode"], "en");

        let (_s, r) = call(
            "ContainsPiiEntities",
            json!({"Text": "x", "LanguageCode": "en"}),
            acct,
        );
        let labels = r["Labels"].as_array().unwrap();
        assert!(labels.is_empty());

        let (_s, r) = call(
            "ClassifyDocument",
            json!({"Text": "t", "EndpointArn": "arn:x"}),
            acct,
        );
        assert_eq!(r["Classes"][0]["Name"], "POSITIVE");
    }

    #[test]
    fn unsupported_lang_is_invalid_request() {
        let acct = next_account();
        let (_s, r) = call(
            "DetectSentiment",
            json!({"Text": "x", "LanguageCode": "zz"}),
            acct,
        );
        assert_eq!(r["__type"], "InvalidRequestException");
        assert!(r["message"].as_str().unwrap().contains("enum value set"));
    }

    #[test]
    fn job_family_lifecycle() {
        let acct = next_account();
        let (_s, r) = call(
            "StartSentimentDetectionJob",
            json!({
                "InputDataConfig": {"S3Uri": "s3://b/in"},
                "OutputDataConfig": {"S3Uri": "s3://b/out"},
                "DataAccessRoleArn": "r",
                "LanguageCode": "en"
            }),
            acct,
        );
        assert_eq!(r["JobStatus"], "SUBMITTED");
        assert!(r["JobArn"]
            .as_str()
            .unwrap()
            .contains("sentiment-detection-job"));
        let job_id = r["JobId"].as_str().unwrap().to_string();

        let (_s, r) = call(
            "DescribeSentimentDetectionJob",
            json!({"JobId": job_id.clone()}),
            acct,
        );
        let props = &r["SentimentDetectionJobProperties"];
        assert_eq!(props["JobStatus"], "SUBMITTED");
        assert!(props["SubmitTime"].as_str().unwrap().ends_with("+00:00"));

        let (_s, r) = call("StopSentimentDetectionJob", json!({"JobId": job_id}), acct);
        assert_eq!(r["JobStatus"], "STOP_REQUESTED");

        let (_s, r) = call("ListSentimentDetectionJobs", json!({}), acct);
        let items = r["SentimentDetectionJobPropertiesList"].as_array().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["JobStatus"], "STOP_REQUESTED");

        // wrong family list stays empty
        let (_s, r) = call("ListTopicsDetectionJobs", json!({}), acct);
        assert_eq!(
            r["TopicsDetectionJobPropertiesList"]
                .as_array()
                .unwrap()
                .len(),
            0
        );

        // missing job -> ResourceNotFound
        let (s, _) = call(
            "DescribeSentimentDetectionJob",
            json!({"JobId": "nope"}),
            acct,
        );
        assert_eq!(s, 400);
    }

    #[test]
    fn events_job_requires_target_types() {
        let acct = next_account();
        let (_s, r) = call(
            "StartEventsDetectionJob",
            json!({"InputDataConfig": {"S3Uri": "s3://b"}, "OutputDataConfig": {"S3Uri": "s3://o"}, "DataAccessRoleArn": "r"}),
            acct,
        );
        assert_eq!(r["__type"], "InvalidRequestException");
    }

    #[test]
    fn tags_and_flywheel() {
        let acct = next_account();
        let (_s, r) = call(
            "CreateFlywheel",
            json!({"FlywheelName": "fw", "DataAccessRoleArn": "r", "DataLakeS3Uri": "s3://b/"}),
            acct,
        );
        let arn = r["FlywheelArn"].as_str().unwrap().to_string();

        let (_s, r) = call(
            "DescribeFlywheel",
            json!({"FlywheelArn": arn.clone()}),
            acct,
        );
        assert_eq!(r["FlywheelProperties"]["Status"], "ACTIVE");

        let (_s, _) = call(
            "StartFlywheelIteration",
            json!({"FlywheelArn": arn.clone()}),
            acct,
        );
        let (_s, r) = call(
            "StartFlywheelIteration",
            json!({"FlywheelArn": "arn:aws:comprehend:us-east-1:971000000001:flywheel/zzz"}),
            acct,
        );
        assert_eq!(r["__type"], "ResourceNotFoundException");

        // endpoint tagging roundtrip
        let (_s, r) = call(
            "CreateEndpoint",
            json!({"EndpointName": "ept", "ModelArn": "arn:m", "DesiredInferenceUnits": 1, "Tags": [{"Key": "env", "Value": "test"}]}),
            acct,
        );
        let ep_arn = r["EndpointArn"].as_str().unwrap().to_string();
        let (_s, r) = call("ListTagsForResource", json!({"ResourceArn": ep_arn}), acct);
        assert_eq!(r["Tags"][0]["Value"], "test");
        let (_s, _) = call(
            "UntagResource",
            json!({"ResourceArn": ep_arn, "TagKeys": ["env"]}),
            acct,
        );
        let (_s, r) = call("ListTagsForResource", json!({"ResourceArn": ep_arn}), acct);
        assert_eq!(r["Tags"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn batch_ops_shape() {
        let acct = next_account();
        let (_s, r) = call(
            "BatchDetectSentiment",
            json!({"TextList": ["a", "b"], "LanguageCode": "en"}),
            acct,
        );
        assert_eq!(r["ResultList"].as_array().unwrap().len(), 2);
        assert_eq!(r["ErrorList"].as_array().unwrap().len(), 0);
        let (_s, r) = call(
            "BatchDetectEntities",
            json!({"TextList": ["a"], "LanguageCode": "en"}),
            acct,
        );
        assert_eq!(r["ResultList"][0]["Index"], 0);
    }
}
