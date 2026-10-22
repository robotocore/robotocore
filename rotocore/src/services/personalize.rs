//! Amazon Personalize backend (JSON protocol, target
//! `AmazonPersonalize.<Op>`).
//!
//! Port of moto's personalize surface: the moto-fork implements the schema
//! family (create/delete/describe/list) and this module mirrors it; the
//! remaining botocore surface — dataset groups, datasets, import/export
//! jobs, solutions, campaigns, recommenders, event trackers, filters,
//! metric attributions, batch jobs, deletion jobs and the built-in
//! recipe/algorithm catalogs — is implemented straight from the botocore
//! wire model (`personalize-2018-05-22`), since moto answers those actions
//! with `NotImplementedError` (501). Modeled semantics: resources answer
//! with real AWS's steady-state status (`ACTIVE`), missing ARNs with the
//! modeled `ResourceNotFoundException` (400), and tag sets are tracked
//! per ARN.
//!
//! Schema-family quirks preserved verbatim from the moto-fork port: the
//! create response has only `schemaArn`, DescribeSchema forces
//! `lastUpdatedDateTime` to the creation date, and ListSchemas strips the
//! `schema` member from every summary.

use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Value};

#[derive(Default)]
struct State {
    /// schemaArn -> {name, schemaArn, domain, creationDateTime, schema?}
    schemas: Vec<Value>,
    /// datasetGroupArn -> describe body (status ACTIVE once created)
    dataset_groups: Vec<Value>,
    /// datasetArn -> describe body
    datasets: Vec<Value>,
    /// job ARNs -> describe bodies
    dataset_import_jobs: Vec<Value>,
    dataset_export_jobs: Vec<Value>,
    batch_inference_jobs: Vec<Value>,
    batch_segment_jobs: Vec<Value>,
    data_deletion_jobs: Vec<Value>,
    /// solutionArn / solutionVersionArn -> describe bodies
    solutions: Vec<Value>,
    solution_versions: Vec<Value>,
    /// campaignArn / recommenderArn / eventTrackerArn / filterArn /
    /// metricAttributionArn -> describe bodies
    campaigns: Vec<Value>,
    recommenders: Vec<Value>,
    event_trackers: Vec<Value>,
    filters: Vec<Value>,
    metric_attributions: Vec<Value>,
    /// ARN -> tags applied by Create* `tags` or TagResource
    tags: Vec<(String, Vec<(String, String)>)>,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("personalize"));
    SLOT.clone()
}

fn ok(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

/// `handle` returns a Response, so the Result-returning validators surface
/// their error response through this try-combinator instead of `?`.
macro_rules! try_response {
    ($e:expr) => {
        match $e {
            Ok(v) => v,
            Err(e) => return e,
        }
    };
}

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "personalize" {
        return None;
    }
    let body = crate::proto::parse_json_body(&req.body);
    let slot = state();
    let mut st = slot.write();
    let b = st.entry((req.account, req.region.clone())).or_default();
    Some(handle(b, &body, &req.region, req.account, req))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn error(kind: &str, message: &str) -> Response {
    Response::json(
        400,
        serde_json::to_vec(&json!({
            "__type": kind,
            "message": format!("{kind}: {message}"),
        }))
        .unwrap_or_default(),
    )
}

fn resource_not_found(arn: &str) -> Response {
    // The model declares ResourceNotFoundException with just `message`;
    // the fork's schema ops use the same "ResourceNotFoundException <arn>"
    // wording, kept verbatim.
    error("ResourceNotFoundException", arn)
}

fn str_param(p: &Value, key: &str, err_kind: &str) -> Result<String, Response> {
    p.get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| error(err_kind, &format!("{key} is required")))
}

fn require<'a>(records: &'a [Value], arn_field: &str, arn: &str) -> Result<&'a Value, Response> {
    find(records, arn_field, arn).ok_or_else(|| resource_not_found(arn))
}

fn resource_arn(region: &str, account: u64, kind: &str, name: &str) -> String {
    format!("arn:aws:personalize:{region}:{account}:{kind}/{name}")
}

fn now() -> Value {
    json!(crate::proto::now_iso())
}

fn find<'a>(records: &'a [Value], arn_field: &str, arn: &str) -> Option<&'a Value> {
    records.iter().find(|r| r[arn_field] == json!(arn))
}

fn remove(records: &mut Vec<Value>, arn_field: &str, arn: &str) -> bool {
    let before = records.len();
    records.retain(|r| r[arn_field] != json!(arn));
    records.len() != before
}

fn pick(record: &Value, members: &[&str]) -> Value {
    let mut out = serde_json::Map::new();
    if let Some(obj) = record.as_object() {
        for name in members {
            if let Some(v) = obj.get(*name) {
                if !v.is_null() {
                    out.insert((*name).to_string(), v.clone());
                }
            }
        }
    }
    Value::Object(out)
}

fn str_field<'a>(v: &'a Value, key: &str) -> Option<&'a str> {
    v.get(key).and_then(Value::as_str)
}

fn tags_json(st: &State, arn: &str) -> Value {
    let mut out = Vec::new();
    if let Some((_, tags)) = st.tags.iter().find(|(a, _)| a == arn) {
        for (k, v) in tags {
            // the model names these members tagKey / tagValue
            out.push(json!({"tagKey": k, "tagValue": v}));
        }
    }
    json!(out)
}

fn set_tags(st: &mut State, arn: &str, tags: &Value) {
    if let Some(list) = tags.as_array() {
        let mut entry: Vec<(String, String)> = st
            .tags
            .iter()
            .find(|(a, _)| a == arn)
            .map(|(_, t)| t.clone())
            .unwrap_or_default();
        for tag in list {
            if let (Some(k), Some(v)) = (str_field(tag, "tagKey"), str_field(tag, "tagValue")) {
                if let Some(kv) = entry.iter_mut().find(|(ek, _)| ek == k) {
                    kv.1 = v.to_string();
                } else {
                    entry.push((k.to_string(), v.to_string()));
                }
            }
        }
        st.tags.retain(|(a, _)| a != arn);
        st.tags.push((arn.to_string(), entry));
    }
}

fn schema_arn(region: &str, account: u64, name: &str) -> String {
    format!("arn:aws:personalize:{region}:{account}:schema/{name}")
}

// ---------------------------------------------------------------------------
// Built-in catalogs: the personalization recipes/programs are real AWS
// static resources; nothing here creates user-owned recipes.
// ---------------------------------------------------------------------------

fn builtin_recipes() -> Vec<Value> {
    [
        (
            "aws-hrn-user-personalization",
            "arn:aws:personalize:::algorithm/aws-hrn-user-personalization",
            "USER_PERSONALIZATION",
        ),
        (
            "aws-user-personalization",
            "arn:aws:personalize:::algorithm/aws-user-personalization",
            "USER_PERSONALIZATION",
        ),
        (
            "aws-similar-items",
            "arn:aws:personalize:::algorithm/aws-similar-items",
            "RELATED_ITEMS",
        ),
        (
            "aws-sims",
            "arn:aws:personalize:::algorithm/aws-aoss-similar-items",
            "RELATED_ITEMS",
        ),
        (
            "aws-ranked-news",
            "arn:aws:personalize:::algorithm/aws-ranked-news",
            "USER_PERSONALIZATION",
        ),
        (
            "aws-popularity-count",
            "arn:aws:personalize:::algorithm/recommender-popularity",
            "USER_PERSONALIZATION",
        ),
    ]
    .iter()
    .map(|(name, algo, recipe_type)| {
        json!({
            "name": name,
            "recipeArn": format!("arn:aws:personalize:::recipe/{name}"),
            "algorithmArn": algo,
            "featureTransformationArn": format!("arn:aws:personalize:::feature-transformation/{name}"),
            "status": "ACTIVE",
            "recipeType": recipe_type,
            "creationDateTime": "2020-01-01T00:00:00Z",
            "lastUpdatedDateTime": "2020-01-01T00:00:00Z",
        })
    })
    .collect()
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

fn handle(st: &mut State, p: &Value, region: &str, account: u64, req: &Request) -> Response {
    match req.operation.as_str() {
        "CreateSchema" => {
            let name = p
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("null")
                .to_string();
            let arn = schema_arn(region, account, &name);
            let mut entry = json!({
                "name": name,
                "schemaArn": &arn,
                "domain": p.get("domain").cloned().unwrap_or(Value::Null),
                "creationDateTime": now(),
                "lastUpdatedDateTime": now(),
            });
            if let Some(s) = p.get("schema") {
                entry["schema"] = s.clone();
            }
            if let Some(tags) = p.get("tags") {
                set_tags(st, &arn, tags);
            }
            st.schemas.push(entry);
            ok(&json!({"schemaArn": &arn}))
        }
        "DescribeSchema" => {
            let arn = p.get("schemaArn").and_then(Value::as_str).unwrap_or("");
            match find(&st.schemas, "schemaArn", arn) {
                Some(s) => {
                    let mut full = s.clone();
                    full["lastUpdatedDateTime"] = s["creationDateTime"].clone();
                    ok(&json!({"schema": full}))
                }
                None => resource_not_found(arn),
            }
        }
        "ListSchemas" => {
            let schemas: Vec<Value> = st
                .schemas
                .iter()
                .map(|s| {
                    let mut summary = s.clone();
                    summary.as_object_mut().unwrap().remove("schema");
                    summary
                })
                .collect();
            ok(&json!({"schemas": schemas}))
        }
        "DeleteSchema" => {
            let arn = p.get("schemaArn").and_then(Value::as_str).unwrap_or("");
            let before = st.schemas.len();
            st.schemas.retain(|s| s["schemaArn"] != json!(arn));
            match before == st.schemas.len() {
                true => resource_not_found(arn),
                false => ok(&json!({})),
            }
        }

        // -------------------------------------------------------------------
        // Dataset groups and datasets
        // -------------------------------------------------------------------
        "CreateDatasetGroup" => {
            let name = try_response!(str_param(p, "name", "InvalidInputException"));
            let arn = resource_arn(region, account, "dataset-group", &name);
            let entry = json!({
                "name": name,
                "datasetGroupArn": &arn,
                "status": "ACTIVE",
                "roleArn": p.get("roleArn").cloned().unwrap_or(Value::Null),
                "kmsKeyArn": p.get("kmsKeyArn").cloned().unwrap_or(Value::Null),
                "domain": p.get("domain").cloned().unwrap_or(Value::Null),
                "creationDateTime": now(),
                "lastUpdatedDateTime": now(),
            });
            if let Some(tags) = p.get("tags") {
                set_tags(st, &arn, tags);
            }
            st.dataset_groups.push(entry);
            ok(&json!({
                "datasetGroupArn": &arn,
                "domain": p.get("domain").cloned().unwrap_or(Value::Null),
            }))
        }
        "DescribeDatasetGroup" => {
            let arn = try_response!(str_param(p, "datasetGroupArn", "ResourceNotFoundException"));
            match require(&st.dataset_groups, "datasetGroupArn", &arn) {
                Ok(r) => ok(&json!({"datasetGroup": r})),
                Err(e) => e,
            }
        }
        "DeleteDatasetGroup" => {
            let arn = try_response!(str_param(p, "datasetGroupArn", "ResourceNotFoundException"));
            if remove(&mut st.dataset_groups, "datasetGroupArn", &arn) {
                // real AWS deletes the group's dependents with the group;
                // cascade so a deleted group IDs stop resolving.
                st.datasets.retain(|d| d["datasetGroupArn"] != json!(arn));
                st.event_trackers
                    .retain(|e| e["datasetGroupArn"] != json!(arn));
                st.filters.retain(|f| f["datasetGroupArn"] != json!(arn));
                st.solutions.retain(|s| s["datasetGroupArn"] != json!(arn));
                st.recommenders
                    .retain(|r| r["datasetGroupArn"] != json!(arn));
                ok(&json!({}))
            } else {
                resource_not_found(&arn)
            }
        }
        "ListDatasetGroups" => {
            let groups: Vec<Value> = st
                .dataset_groups
                .iter()
                .map(|r| {
                    pick(
                        r,
                        &[
                            "name",
                            "datasetGroupArn",
                            "status",
                            "creationDateTime",
                            "lastUpdatedDateTime",
                            "domain",
                        ],
                    )
                })
                .collect();
            ok(&json!({"datasetGroups": groups, "nextToken": Value::Null}))
        }
        "CreateDataset" => {
            let name = try_response!(str_param(p, "name", "InvalidInputException"));
            let dsg_arn =
                try_response!(str_param(p, "datasetGroupArn", "ResourceNotFoundException"));
            if find(&st.dataset_groups, "datasetGroupArn", &dsg_arn).is_none() {
                return resource_not_found(&dsg_arn);
            }
            let sch_arn = try_response!(str_param(p, "schemaArn", "ResourceNotFoundException"));
            if find(&st.schemas, "schemaArn", &sch_arn).is_none() {
                return resource_not_found(&sch_arn);
            }
            let arn = resource_arn(region, account, "dataset", &name);
            let entry = json!({
                "name": name,
                "datasetArn": &arn,
                "datasetGroupArn": dsg_arn,
                "datasetType": p.get("datasetType").cloned().unwrap_or(Value::Null),
                "schemaArn": sch_arn,
                "status": "ACTIVE",
                "creationDateTime": now(),
                "lastUpdatedDateTime": now(),
            });
            if let Some(tags) = p.get("tags") {
                set_tags(st, &arn, tags);
            }
            st.datasets.push(entry);
            ok(&json!({"datasetArn": &arn}))
        }
        "DescribeDataset" => {
            let arn = try_response!(str_param(p, "datasetArn", "ResourceNotFoundException"));
            match require(&st.datasets, "datasetArn", &arn) {
                Ok(r) => ok(&json!({"dataset": r})),
                Err(e) => e,
            }
        }
        "UpdateDataset" => {
            let arn = try_response!(str_param(p, "datasetArn", "ResourceNotFoundException"));
            if find(
                &st.schemas,
                "schemaArn",
                str_field(p, "schemaArn").unwrap_or_default(),
            )
            .is_none()
            {
                return resource_not_found(str_field(p, "schemaArn").unwrap_or_default());
            }
            match st
                .datasets
                .iter_mut()
                .find(|d| d["datasetArn"] == json!(arn))
            {
                Some(r) => {
                    r["schemaArn"] = p.get("schemaArn").cloned().unwrap_or(Value::Null);
                    r["latestDatasetUpdate"] = json!({
                        "schemaArn": r["schemaArn"],
                        "status": "UPDATE IN PROGRESS",
                        "creationDateTime": now(),
                        "lastUpdatedDateTime": now(),
                    });
                    ok(&json!({"datasetArn": arn}))
                }
                None => resource_not_found(&arn),
            }
        }
        "DeleteDataset" => {
            let arn = try_response!(str_param(p, "datasetArn", "ResourceNotFoundException"));
            if remove(&mut st.datasets, "datasetArn", &arn) {
                ok(&json!({}))
            } else {
                resource_not_found(&arn)
            }
        }
        "ListDatasets" => {
            let filter = p.get("datasetGroupArn").and_then(Value::as_str);
            let datasets: Vec<Value> = st
                .datasets
                .iter()
                .filter(|d| filter.is_none_or(|f| d["datasetGroupArn"] == json!(f)))
                .map(|d| {
                    pick(
                        d,
                        &[
                            "name",
                            "datasetArn",
                            "datasetType",
                            "status",
                            "creationDateTime",
                            "lastUpdatedDateTime",
                        ],
                    )
                })
                .collect();
            ok(&json!({"datasets": datasets, "nextToken": Value::Null}))
        }

        // -------------------------------------------------------------------
        // Dataset import / export / deletion jobs
        // -------------------------------------------------------------------
        "CreateDatasetImportJob" => {
            let name = try_response!(str_param(p, "jobName", "InvalidInputException"));
            let ds_arn = try_response!(str_param(p, "datasetArn", "ResourceNotFoundException"));
            try_response!(require(&st.datasets, "datasetArn", &ds_arn));
            let arn = resource_arn(region, account, "dataset-import-job", &name);
            let entry = json!({
                "jobName": name,
                "datasetImportJobArn": &arn,
                "datasetArn": ds_arn,
                "dataSource": p.get("dataSource").cloned().unwrap_or(Value::Null),
                "importMode": p.get("importMode").cloned().unwrap_or(Value::Null),
                "roleArn": p.get("roleArn").cloned().unwrap_or(Value::Null),
                "publishAttributionMetricsToS3": p
                    .get("publishAttributionMetricsToS3")
                    .cloned()
                    .unwrap_or(Value::Null),
                "status": "ACTIVE",
                "creationDateTime": now(),
                "lastUpdatedDateTime": now(),
            });
            if let Some(tags) = p.get("tags") {
                set_tags(st, &arn, tags);
            }
            st.dataset_import_jobs.push(entry);
            ok(&json!({"datasetImportJobArn": &arn}))
        }
        "DescribeDatasetImportJob" => {
            let arn = try_response!(str_param(
                p,
                "datasetImportJobArn",
                "ResourceNotFoundException"
            ));
            match require(&st.dataset_import_jobs, "datasetImportJobArn", &arn) {
                Ok(r) => {
                    let mut full = r.clone();
                    full["failureReason"] = Value::Null;
                    full.as_object_mut().unwrap().remove("failureReason");
                    ok(&json!({"datasetImportJob": full}))
                }
                Err(e) => e,
            }
        }
        "ListDatasetImportJobs" => {
            let filter = p.get("datasetArn").and_then(Value::as_str);
            let jobs: Vec<Value> = st
                .dataset_import_jobs
                .iter()
                .filter(|j| filter.is_none_or(|f| j["datasetArn"] == json!(f)))
                .map(|j| {
                    pick(
                        j,
                        &[
                            "datasetImportJobArn",
                            "jobName",
                            "status",
                            "creationDateTime",
                            "lastUpdatedDateTime",
                            "importMode",
                        ],
                    )
                })
                .collect();
            ok(&json!({"datasetImportJobs": jobs, "nextToken": Value::Null}))
        }
        "CreateDatasetExportJob" => {
            let name = try_response!(str_param(p, "jobName", "InvalidInputException"));
            let ds_arn = try_response!(str_param(p, "datasetArn", "ResourceNotFoundException"));
            try_response!(require(&st.datasets, "datasetArn", &ds_arn));
            let arn = resource_arn(region, account, "dataset-export-job", &name);
            let entry = json!({
                "jobName": name,
                "datasetExportJobArn": &arn,
                "datasetArn": ds_arn,
                "ingestionMode": p.get("ingestionMode").cloned().unwrap_or(Value::Null),
                "roleArn": p.get("roleArn").cloned().unwrap_or(Value::Null),
                "jobOutput": p.get("jobOutput").cloned().unwrap_or(Value::Null),
                "status": "ACTIVE",
                "creationDateTime": now(),
                "lastUpdatedDateTime": now(),
            });
            if let Some(tags) = p.get("tags") {
                set_tags(st, &arn, tags);
            }
            st.dataset_export_jobs.push(entry);
            ok(&json!({"datasetExportJobArn": &arn}))
        }
        "DescribeDatasetExportJob" => {
            let arn = try_response!(str_param(
                p,
                "datasetExportJobArn",
                "ResourceNotFoundException"
            ));
            match require(&st.dataset_export_jobs, "datasetExportJobArn", &arn) {
                Ok(r) => {
                    let mut full = r.clone();
                    full["failureReason"] = Value::Null;
                    full.as_object_mut().unwrap().remove("failureReason");
                    ok(&json!({"datasetExportJob": full}))
                }
                Err(e) => e,
            }
        }
        "ListDatasetExportJobs" => {
            let filter = p.get("datasetArn").and_then(Value::as_str);
            let jobs: Vec<Value> = st
                .dataset_export_jobs
                .iter()
                .filter(|j| filter.is_none_or(|f| j["datasetArn"] == json!(f)))
                .map(|j| {
                    pick(
                        j,
                        &[
                            "datasetExportJobArn",
                            "jobName",
                            "status",
                            "creationDateTime",
                            "lastUpdatedDateTime",
                        ],
                    )
                })
                .collect();
            ok(&json!({"datasetExportJobs": jobs, "nextToken": Value::Null}))
        }
        "CreateDataDeletionJob" => {
            let name = try_response!(str_param(p, "jobName", "InvalidInputException"));
            let dsg_arn =
                try_response!(str_param(p, "datasetGroupArn", "ResourceNotFoundException"));
            try_response!(require(&st.dataset_groups, "datasetGroupArn", &dsg_arn));
            let arn = resource_arn(region, account, "data-deletion-job", &name);
            let entry = json!({
                "jobName": name,
                "dataDeletionJobArn": &arn,
                "datasetGroupArn": dsg_arn,
                "dataSource": p.get("dataSource").cloned().unwrap_or(Value::Null),
                "roleArn": p.get("roleArn").cloned().unwrap_or(Value::Null),
                "status": "ACTIVE",
                "numDeleted": 0,
                "creationDateTime": now(),
                "lastUpdatedDateTime": now(),
            });
            if let Some(tags) = p.get("tags") {
                set_tags(st, &arn, tags);
            }
            st.data_deletion_jobs.push(entry);
            ok(&json!({"dataDeletionJobArn": &arn}))
        }
        "DescribeDataDeletionJob" => {
            let arn = try_response!(str_param(
                p,
                "dataDeletionJobArn",
                "ResourceNotFoundException"
            ));
            match require(&st.data_deletion_jobs, "dataDeletionJobArn", &arn) {
                Ok(r) => {
                    let mut full = r.clone();
                    full["failureReason"] = Value::Null;
                    full.as_object_mut().unwrap().remove("failureReason");
                    ok(&json!({"dataDeletionJob": full}))
                }
                Err(e) => e,
            }
        }
        "ListDataDeletionJobs" => {
            let filter = p.get("datasetGroupArn").and_then(Value::as_str);
            let jobs: Vec<Value> = st
                .data_deletion_jobs
                .iter()
                .filter(|j| filter.is_none_or(|f| j["datasetGroupArn"] == json!(f)))
                .map(|j| {
                    pick(
                        j,
                        &[
                            "dataDeletionJobArn",
                            "datasetGroupArn",
                            "jobName",
                            "status",
                            "creationDateTime",
                            "lastUpdatedDateTime",
                        ],
                    )
                })
                .collect();
            ok(&json!({"dataDeletionJobs": jobs, "nextToken": Value::Null}))
        }

        // -------------------------------------------------------------------
        // Batch inference and segment jobs
        // -------------------------------------------------------------------
        "CreateBatchInferenceJob" => {
            let name = try_response!(str_param(p, "jobName", "InvalidInputException"));
            let sv_arn = try_response!(str_param(
                p,
                "solutionVersionArn",
                "ResourceNotFoundException"
            ));
            try_response!(require(
                &st.solution_versions,
                "solutionVersionArn",
                &sv_arn
            ));
            let arn = resource_arn(region, account, "batch-inference-job", &name);
            let entry = json!({
                "jobName": name,
                "batchInferenceJobArn": &arn,
                "solutionVersionArn": sv_arn,
                "filterArn": p.get("filterArn").cloned().unwrap_or(Value::Null),
                "jobInput": p.get("jobInput").cloned().unwrap_or(Value::Null),
                "jobOutput": p.get("jobOutput").cloned().unwrap_or(Value::Null),
                "batchInferenceJobConfig": p
                    .get("batchInferenceJobConfig")
                    .cloned()
                    .unwrap_or(Value::Null),
                "batchInferenceJobMode": p
                    .get("batchInferenceJobMode")
                    .cloned()
                    .unwrap_or(Value::Null),
                "numResults": p.get("numResults").cloned().unwrap_or(Value::Null),
                "roleArn": p.get("roleArn").cloned().unwrap_or(Value::Null),
                "status": "ACTIVE",
                "creationDateTime": now(),
                "lastUpdatedDateTime": now(),
            });
            if let Some(tags) = p.get("tags") {
                set_tags(st, &arn, tags);
            }
            st.batch_inference_jobs.push(entry);
            ok(&json!({"batchInferenceJobArn": &arn}))
        }
        "DescribeBatchInferenceJob" => {
            let arn = try_response!(str_param(
                p,
                "batchInferenceJobArn",
                "ResourceNotFoundException"
            ));
            match require(&st.batch_inference_jobs, "batchInferenceJobArn", &arn) {
                Ok(r) => {
                    let mut full = r.clone();
                    full["failureReason"] = Value::Null;
                    full.as_object_mut().unwrap().remove("failureReason");
                    ok(&json!({"batchInferenceJob": full}))
                }
                Err(e) => e,
            }
        }
        "ListBatchInferenceJobs" => {
            let filter = p.get("solutionVersionArn").and_then(Value::as_str);
            let jobs: Vec<Value> = st
                .batch_inference_jobs
                .iter()
                .filter(|j| filter.is_none_or(|f| j["solutionVersionArn"] == json!(f)))
                .map(|j| {
                    pick(
                        j,
                        &[
                            "batchInferenceJobArn",
                            "jobName",
                            "status",
                            "creationDateTime",
                            "lastUpdatedDateTime",
                            "batchInferenceJobMode",
                        ],
                    )
                })
                .collect();
            ok(&json!({"batchInferenceJobs": jobs, "nextToken": Value::Null}))
        }
        "CreateBatchSegmentJob" => {
            let name = try_response!(str_param(p, "jobName", "InvalidInputException"));
            let sv_arn = try_response!(str_param(
                p,
                "solutionVersionArn",
                "ResourceNotFoundException"
            ));
            try_response!(require(
                &st.solution_versions,
                "solutionVersionArn",
                &sv_arn
            ));
            let arn = resource_arn(region, account, "batch-segment-job", &name);
            let entry = json!({
                "jobName": name,
                "batchSegmentJobArn": &arn,
                "solutionVersionArn": sv_arn,
                "filterArn": p.get("filterArn").cloned().unwrap_or(Value::Null),
                "jobInput": p.get("jobInput").cloned().unwrap_or(Value::Null),
                "jobOutput": p.get("jobOutput").cloned().unwrap_or(Value::Null),
                "numResults": p.get("numResults").cloned().unwrap_or(Value::Null),
                "roleArn": p.get("roleArn").cloned().unwrap_or(Value::Null),
                "status": "ACTIVE",
                "creationDateTime": now(),
                "lastUpdatedDateTime": now(),
            });
            if let Some(tags) = p.get("tags") {
                set_tags(st, &arn, tags);
            }
            st.batch_segment_jobs.push(entry);
            ok(&json!({"batchSegmentJobArn": &arn}))
        }
        "DescribeBatchSegmentJob" => {
            let arn = try_response!(str_param(
                p,
                "batchSegmentJobArn",
                "ResourceNotFoundException"
            ));
            match require(&st.batch_segment_jobs, "batchSegmentJobArn", &arn) {
                Ok(r) => {
                    let mut full = r.clone();
                    full["failureReason"] = Value::Null;
                    full.as_object_mut().unwrap().remove("failureReason");
                    ok(&json!({"batchSegmentJob": full}))
                }
                Err(e) => e,
            }
        }
        "ListBatchSegmentJobs" => {
            let filter = p.get("solutionVersionArn").and_then(Value::as_str);
            let jobs: Vec<Value> = st
                .batch_segment_jobs
                .iter()
                .filter(|j| filter.is_none_or(|f| j["solutionVersionArn"] == json!(f)))
                .map(|j| {
                    pick(
                        j,
                        &[
                            "batchSegmentJobArn",
                            "jobName",
                            "status",
                            "creationDateTime",
                            "lastUpdatedDateTime",
                        ],
                    )
                })
                .collect();
            ok(&json!({"batchSegmentJobs": jobs, "nextToken": Value::Null}))
        }

        // -------------------------------------------------------------------
        // Solutions and solution versions
        // -------------------------------------------------------------------
        "CreateSolution" => {
            let name = try_response!(str_param(p, "name", "InvalidInputException"));
            let dsg_arn =
                try_response!(str_param(p, "datasetGroupArn", "ResourceNotFoundException"));
            try_response!(require(&st.dataset_groups, "datasetGroupArn", &dsg_arn));
            let arn = resource_arn(region, account, "solution", &name);
            let entry = json!({
                "name": name,
                "solutionArn": &arn,
                "datasetGroupArn": dsg_arn,
                "recipeArn": p.get("recipeArn").cloned().unwrap_or(Value::Null),
                "performHPO": p.get("performHPO").cloned().unwrap_or(Value::Null),
                "performAutoML": p.get("performAutoML").cloned().unwrap_or(Value::Null),
                "performAutoTraining": p.get("performAutoTraining").cloned().unwrap_or(Value::Null),
                "performIncrementalUpdate": p
                    .get("performIncrementalUpdate")
                    .cloned()
                    .unwrap_or(Value::Null),
                "eventType": p.get("eventType").cloned().unwrap_or(Value::Null),
                "solutionConfig": p.get("solutionConfig").cloned().unwrap_or(Value::Null),
                "status": "ACTIVE",
                "creationDateTime": now(),
                "lastUpdatedDateTime": now(),
            });
            if let Some(tags) = p.get("tags") {
                set_tags(st, &arn, tags);
            }
            st.solutions.push(entry);
            ok(&json!({"solutionArn": &arn}))
        }
        "DescribeSolution" => {
            let arn = try_response!(str_param(p, "solutionArn", "ResourceNotFoundException"));
            match require(&st.solutions, "solutionArn", &arn) {
                Ok(r) => {
                    let mut full = r.clone();
                    // the newest stored version surfaces as the summary
                    let latest = st
                        .solution_versions
                        .iter()
                        .rev()
                        .find(|v| v["solutionArn"] == json!(arn));
                    if let Some(v) = latest {
                        full["latestSolutionVersion"] = json!({
                            "solutionVersionArn": v["solutionVersionArn"],
                            "status": v["status"],
                            "trainingMode": v["trainingMode"],
                            "creationDateTime": v["creationDateTime"],
                            "lastUpdatedDateTime": v["lastUpdatedDateTime"],
                        });
                    }
                    ok(&json!({"solution": full}))
                }
                Err(e) => e,
            }
        }
        "DeleteSolution" => {
            let arn = try_response!(str_param(p, "solutionArn", "ResourceNotFoundException"));
            if remove(&mut st.solutions, "solutionArn", &arn) {
                st.solution_versions
                    .retain(|v| v["solutionArn"] != json!(arn));
                st.campaigns.retain(|c| {
                    !c["solutionVersionArn"]
                        .as_str()
                        .map(|a| a.starts_with(&format!("{arn}/")))
                        .unwrap_or(false)
                });
                ok(&json!({}))
            } else {
                resource_not_found(&arn)
            }
        }
        "ListSolutions" => {
            let filter = p.get("datasetGroupArn").and_then(Value::as_str);
            let solutions: Vec<Value> = st
                .solutions
                .iter()
                .filter(|s| filter.is_none_or(|f| s["datasetGroupArn"] == json!(f)))
                .map(|s| {
                    pick(
                        s,
                        &[
                            "name",
                            "solutionArn",
                            "status",
                            "creationDateTime",
                            "lastUpdatedDateTime",
                            "recipeArn",
                        ],
                    )
                })
                .collect();
            ok(&json!({"solutions": solutions, "nextToken": Value::Null}))
        }
        "UpdateSolution" => {
            let arn = try_response!(str_param(p, "solutionArn", "ResourceNotFoundException"));
            match st
                .solutions
                .iter_mut()
                .find(|s| s["solutionArn"] == json!(arn))
            {
                Some(r) => {
                    if let Some(v) = p.get("performAutoTraining").filter(|v| !v.is_null()) {
                        r["performAutoTraining"] = v.clone();
                    }
                    if let Some(v) = p.get("performIncrementalUpdate").filter(|v| !v.is_null()) {
                        r["performIncrementalUpdate"] = v.clone();
                    }
                    if let Some(cfg) = p.get("solutionUpdateConfig").filter(|v| !v.is_null()) {
                        r["solutionUpdateConfig"] = cfg.clone();
                    }
                    r["lastUpdatedDateTime"] = now();
                    ok(&json!({"solutionArn": arn}))
                }
                None => resource_not_found(&arn),
            }
        }
        "CreateSolutionVersion" => {
            let sol_arn = try_response!(str_param(p, "solutionArn", "ResourceNotFoundException"));
            let solution = try_response!(require(&st.solutions, "solutionArn", &sol_arn));
            let version = st
                .solution_versions
                .iter()
                .filter(|v| v["solutionArn"] == json!(sol_arn))
                .count()
                + 1;
            let arn = format!("{sol_arn}/{version:08}");
            let entry = json!({
                "name": solution["name"],
                "solutionVersionArn": &arn,
                "solutionArn": sol_arn,
                "recipeArn": solution["recipeArn"],
                "datasetGroupArn": solution["datasetGroupArn"],
                "performHPO": solution["performHPO"],
                "performAutoML": solution["performAutoML"],
                "performIncrementalUpdate": solution["performIncrementalUpdate"],
                "eventType": solution["eventType"],
                "solutionConfig": solution["solutionConfig"],
                "trainingMode": p.get("trainingMode").cloned().unwrap_or(Value::Null),
                "status": "ACTIVE",
                "creationDateTime": now(),
                "lastUpdatedDateTime": now(),
            });
            if let Some(tags) = p.get("tags") {
                set_tags(st, &arn, tags);
            }
            st.solution_versions.push(entry);
            ok(&json!({"solutionVersionArn": &arn}))
        }
        "DescribeSolutionVersion" => {
            let arn = try_response!(str_param(
                p,
                "solutionVersionArn",
                "ResourceNotFoundException"
            ));
            match require(&st.solution_versions, "solutionVersionArn", &arn) {
                Ok(r) => {
                    let mut full = r.clone();
                    full["trainingHours"] = json!(1.0);
                    full["trainingType"] = json!("FULL");
                    ok(&json!({"solutionVersion": full}))
                }
                Err(e) => e,
            }
        }
        "ListSolutionVersions" => {
            let filter = p.get("solutionArn").and_then(Value::as_str);
            let versions: Vec<Value> = st
                .solution_versions
                .iter()
                .filter(|v| filter.is_none_or(|f| v["solutionArn"] == json!(f)))
                .map(|v| {
                    let mut summary = pick(
                        v,
                        &[
                            "solutionVersionArn",
                            "status",
                            "creationDateTime",
                            "lastUpdatedDateTime",
                            "trainingMode",
                        ],
                    );
                    summary["trainingType"] = json!("FULL");
                    summary
                })
                .collect();
            ok(&json!({"solutionVersions": versions, "nextToken": Value::Null}))
        }
        "StopSolutionVersionCreation" => {
            let arn = try_response!(str_param(
                p,
                "solutionVersionArn",
                "ResourceNotFoundException"
            ));
            match st
                .solution_versions
                .iter_mut()
                .find(|v| v["solutionVersionArn"] == json!(arn))
            {
                Some(r) => {
                    r["status"] = json!("CREATE STOPPED");
                    ok(&json!({}))
                }
                None => resource_not_found(&arn),
            }
        }
        "GetSolutionMetrics" => {
            let arn = try_response!(str_param(
                p,
                "solutionVersionArn",
                "ResourceNotFoundException"
            ));
            match require(&st.solution_versions, "solutionVersionArn", &arn) {
                Ok(_) => ok(&json!({
                    "solutionVersionArn": arn,
                    "metrics": {"precision@10": 0.53},
                })),
                Err(e) => e,
            }
        }

        // -------------------------------------------------------------------
        // Campaigns and recommenders
        // -------------------------------------------------------------------
        "CreateCampaign" => {
            let name = try_response!(str_param(p, "name", "InvalidInputException"));
            let sv_arn = try_response!(str_param(
                p,
                "solutionVersionArn",
                "ResourceNotFoundException"
            ));
            try_response!(require(
                &st.solution_versions,
                "solutionVersionArn",
                &sv_arn
            ));
            let arn = resource_arn(region, account, "campaign", &name);
            let entry = json!({
                "name": name,
                "campaignArn": &arn,
                "solutionVersionArn": sv_arn,
                "minProvisionedTPS": p.get("minProvisionedTPS").cloned().unwrap_or(Value::Null),
                "campaignConfig": p.get("campaignConfig").cloned().unwrap_or(Value::Null),
                "status": "ACTIVE",
                "creationDateTime": now(),
                "lastUpdatedDateTime": now(),
            });
            if let Some(tags) = p.get("tags") {
                set_tags(st, &arn, tags);
            }
            st.campaigns.push(entry);
            ok(&json!({"campaignArn": &arn}))
        }
        "DescribeCampaign" => {
            let arn = try_response!(str_param(p, "campaignArn", "ResourceNotFoundException"));
            match require(&st.campaigns, "campaignArn", &arn) {
                Ok(r) => {
                    let mut full = r.clone();
                    full["failureReason"] = Value::Null;
                    full.as_object_mut().unwrap().remove("failureReason");
                    ok(&json!({"campaign": full}))
                }
                Err(e) => e,
            }
        }
        "DeleteCampaign" => {
            let arn = try_response!(str_param(p, "campaignArn", "ResourceNotFoundException"));
            if remove(&mut st.campaigns, "campaignArn", &arn) {
                ok(&json!({}))
            } else {
                resource_not_found(&arn)
            }
        }
        "ListCampaigns" => {
            let filter = p.get("solutionArn").and_then(Value::as_str);
            let campaigns: Vec<Value> = st
                .campaigns
                .iter()
                .filter(|c| {
                    filter.is_none_or(|f| {
                        c["solutionVersionArn"]
                            .as_str()
                            .map(|a| a.starts_with(&format!("{f}/")))
                            .unwrap_or(false)
                    })
                })
                .map(|c| {
                    pick(
                        c,
                        &[
                            "name",
                            "campaignArn",
                            "status",
                            "creationDateTime",
                            "lastUpdatedDateTime",
                        ],
                    )
                })
                .collect();
            ok(&json!({"campaigns": campaigns, "nextToken": Value::Null}))
        }
        "UpdateCampaign" => {
            let arn = try_response!(str_param(p, "campaignArn", "ResourceNotFoundException"));
            match st
                .campaigns
                .iter_mut()
                .find(|c| c["campaignArn"] == json!(arn))
            {
                Some(r) => {
                    if let Some(v) = p.get("solutionVersionArn").filter(|v| !v.is_null()) {
                        r["solutionVersionArn"] = v.clone();
                    }
                    if let Some(v) = p.get("minProvisionedTPS").filter(|v| !v.is_null()) {
                        r["minProvisionedTPS"] = v.clone();
                    }
                    if let Some(v) = p.get("campaignConfig").filter(|v| !v.is_null()) {
                        r["campaignConfig"] = v.clone();
                    }
                    r["latestCampaignUpdate"] = json!({
                        "solutionVersionArn": r["solutionVersionArn"],
                        "minProvisionedTPS": r["minProvisionedTPS"],
                        "status": "UPDATE IN PROGRESS",
                        "creationDateTime": now(),
                        "lastUpdatedDateTime": now(),
                    });
                    r["lastUpdatedDateTime"] = now();
                    ok(&json!({"campaignArn": arn}))
                }
                None => resource_not_found(&arn),
            }
        }
        "CreateRecommender" => {
            let name = try_response!(str_param(p, "name", "InvalidInputException"));
            let dsg_arn =
                try_response!(str_param(p, "datasetGroupArn", "ResourceNotFoundException"));
            try_response!(require(&st.dataset_groups, "datasetGroupArn", &dsg_arn));
            let arn = resource_arn(region, account, "recommender", &name);
            let entry = json!({
                "name": name,
                "recommenderArn": &arn,
                "datasetGroupArn": dsg_arn,
                "recipeArn": p.get("recipeArn").cloned().unwrap_or(Value::Null),
                "recommenderConfig": p.get("recommenderConfig").cloned().unwrap_or(Value::Null),
                "modelMetrics": {"precision@10": 0.21},
                "status": "ACTIVE",
                "creationDateTime": now(),
                "lastUpdatedDateTime": now(),
            });
            if let Some(tags) = p.get("tags") {
                set_tags(st, &arn, tags);
            }
            st.recommenders.push(entry);
            ok(&json!({"recommenderArn": &arn}))
        }
        "DescribeRecommender" => {
            let arn = try_response!(str_param(p, "recommenderArn", "ResourceNotFoundException"));
            match require(&st.recommenders, "recommenderArn", &arn) {
                Ok(r) => {
                    let mut full = r.clone();
                    full["failureReason"] = Value::Null;
                    full.as_object_mut().unwrap().remove("failureReason");
                    ok(&json!({"recommender": full}))
                }
                Err(e) => e,
            }
        }
        "DeleteRecommender" => {
            let arn = try_response!(str_param(p, "recommenderArn", "ResourceNotFoundException"));
            if remove(&mut st.recommenders, "recommenderArn", &arn) {
                ok(&json!({}))
            } else {
                resource_not_found(&arn)
            }
        }
        "ListRecommenders" => {
            let filter = p.get("datasetGroupArn").and_then(Value::as_str);
            let recommenders: Vec<Value> = st
                .recommenders
                .iter()
                .filter(|r| filter.is_none_or(|f| r["datasetGroupArn"] == json!(f)))
                .map(|r| {
                    pick(
                        r,
                        &[
                            "name",
                            "recommenderArn",
                            "datasetGroupArn",
                            "recipeArn",
                            "recommenderConfig",
                            "status",
                            "creationDateTime",
                            "lastUpdatedDateTime",
                        ],
                    )
                })
                .collect();
            ok(&json!({"recommenders": recommenders, "nextToken": Value::Null}))
        }
        "UpdateRecommender" => {
            let arn = try_response!(str_param(p, "recommenderArn", "ResourceNotFoundException"));
            match st
                .recommenders
                .iter_mut()
                .find(|r| r["recommenderArn"] == json!(arn))
            {
                Some(r) => {
                    r["recommenderConfig"] =
                        p.get("recommenderConfig").cloned().unwrap_or(Value::Null);
                    r["latestRecommenderUpdate"] = json!({
                        "recommenderConfig": r["recommenderConfig"],
                        "status": "UPDATE IN PROGRESS",
                        "creationDateTime": now(),
                        "lastUpdatedDateTime": now(),
                    });
                    r["lastUpdatedDateTime"] = now();
                    ok(&json!({"recommenderArn": arn}))
                }
                None => resource_not_found(&arn),
            }
        }
        "StartRecommender" => {
            let arn = try_response!(str_param(p, "recommenderArn", "ResourceNotFoundException"));
            match st
                .recommenders
                .iter_mut()
                .find(|r| r["recommenderArn"] == json!(arn))
            {
                Some(r) => {
                    r["status"] = json!("ACTIVE");
                    ok(&json!({"recommenderArn": arn}))
                }
                None => resource_not_found(&arn),
            }
        }
        "StopRecommender" => {
            let arn = try_response!(str_param(p, "recommenderArn", "ResourceNotFoundException"));
            match st
                .recommenders
                .iter_mut()
                .find(|r| r["recommenderArn"] == json!(arn))
            {
                Some(r) => {
                    r["status"] = json!("INACTIVE");
                    ok(&json!({"recommenderArn": arn}))
                }
                None => resource_not_found(&arn),
            }
        }

        // -------------------------------------------------------------------
        // Event trackers and filters
        // -------------------------------------------------------------------
        "CreateEventTracker" => {
            let name = try_response!(str_param(p, "name", "InvalidInputException"));
            let dsg_arn = try_response!(str_param(p, "datasetGroupArn", "InvalidInputException"));
            try_response!(require(&st.dataset_groups, "datasetGroupArn", &dsg_arn));
            let arn = resource_arn(region, account, "event-tracker", &name);
            let tracking_id = uuid::Uuid::new_v4().to_string();
            let entry = json!({
                "name": name,
                "eventTrackerArn": &arn,
                "trackingId": tracking_id,
                "datasetGroupArn": dsg_arn,
                "status": "ACTIVE",
                "creationDateTime": now(),
                "lastUpdatedDateTime": now(),
            });
            if let Some(tags) = p.get("tags") {
                set_tags(st, &arn, tags);
            }
            st.event_trackers.push(entry);
            ok(&json!({"eventTrackerArn": &arn, "trackingId": tracking_id}))
        }
        "DescribeEventTracker" => {
            let arn = try_response!(str_param(p, "eventTrackerArn", "ResourceNotFoundException"));
            match require(&st.event_trackers, "eventTrackerArn", &arn) {
                Ok(r) => ok(&json!({"eventTracker": r})),
                Err(e) => e,
            }
        }
        "DeleteEventTracker" => {
            let arn = try_response!(str_param(p, "eventTrackerArn", "ResourceNotFoundException"));
            if remove(&mut st.event_trackers, "eventTrackerArn", &arn) {
                ok(&json!({}))
            } else {
                resource_not_found(&arn)
            }
        }
        "ListEventTrackers" => {
            let filter = p.get("datasetGroupArn").and_then(Value::as_str);
            let trackers: Vec<Value> = st
                .event_trackers
                .iter()
                .filter(|t| filter.is_none_or(|f| t["datasetGroupArn"] == json!(f)))
                .map(|t| {
                    pick(
                        t,
                        &[
                            "name",
                            "eventTrackerArn",
                            "status",
                            "creationDateTime",
                            "lastUpdatedDateTime",
                        ],
                    )
                })
                .collect();
            ok(&json!({"eventTrackers": trackers, "nextToken": Value::Null}))
        }
        "CreateFilter" => {
            let name = try_response!(str_param(p, "name", "InvalidInputException"));
            let dsg_arn =
                try_response!(str_param(p, "datasetGroupArn", "ResourceNotFoundException"));
            try_response!(require(&st.dataset_groups, "datasetGroupArn", &dsg_arn));
            let expression =
                try_response!(str_param(p, "filterExpression", "InvalidInputException"));
            let arn = resource_arn(region, account, "filter", &name);
            let entry = json!({
                "name": name,
                "filterArn": &arn,
                "datasetGroupArn": dsg_arn,
                "filterExpression": expression,
                "status": "ACTIVE",
                "creationDateTime": now(),
                "lastUpdatedDateTime": now(),
            });
            if let Some(tags) = p.get("tags") {
                set_tags(st, &arn, tags);
            }
            st.filters.push(entry);
            ok(&json!({"filterArn": &arn}))
        }
        "DescribeFilter" => {
            let arn = try_response!(str_param(p, "filterArn", "ResourceNotFoundException"));
            match require(&st.filters, "filterArn", &arn) {
                Ok(r) => {
                    let mut full = r.clone();
                    full["failureReason"] = Value::Null;
                    full.as_object_mut().unwrap().remove("failureReason");
                    ok(&json!({"filter": full}))
                }
                Err(e) => e,
            }
        }
        "DeleteFilter" => {
            let arn = try_response!(str_param(p, "filterArn", "ResourceNotFoundException"));
            if remove(&mut st.filters, "filterArn", &arn) {
                ok(&json!({}))
            } else {
                resource_not_found(&arn)
            }
        }
        "ListFilters" => {
            let filter = p.get("datasetGroupArn").and_then(Value::as_str);
            let filters: Vec<Value> = st
                .filters
                .iter()
                .filter(|f| filter.is_none_or(|g| f["datasetGroupArn"] == json!(g)))
                .map(|f| {
                    pick(
                        f,
                        &[
                            "name",
                            "filterArn",
                            "creationDateTime",
                            "lastUpdatedDateTime",
                        ],
                    )
                })
                .collect();
            ok(&json!({"Filters": filters, "nextToken": Value::Null}))
        }

        // -------------------------------------------------------------------
        // Metric attributions
        // -------------------------------------------------------------------
        "CreateMetricAttribution" => {
            let name = try_response!(str_param(p, "name", "InvalidInputException"));
            let dsg_arn =
                try_response!(str_param(p, "datasetGroupArn", "ResourceNotFoundException"));
            try_response!(require(&st.dataset_groups, "datasetGroupArn", &dsg_arn));
            let arn = resource_arn(region, account, "metric-attribution", &name);
            let entry = json!({
                "name": name,
                "metricAttributionArn": &arn,
                "datasetGroupArn": dsg_arn,
                "metrics": p.get("metrics").cloned().unwrap_or(Value::Null),
                "metricsOutputConfig": p.get("metricsOutputConfig").cloned().unwrap_or(Value::Null),
                "status": "ACTIVE",
                "creationDateTime": now(),
                "lastUpdatedDateTime": now(),
            });
            st.metric_attributions.push(entry);
            ok(&json!({"metricAttributionArn": &arn}))
        }
        "DescribeMetricAttribution" => {
            let arn = try_response!(str_param(
                p,
                "metricAttributionArn",
                "ResourceNotFoundException"
            ));
            match require(&st.metric_attributions, "metricAttributionArn", &arn) {
                Ok(r) => {
                    let mut full = r.clone();
                    full["failureReason"] = Value::Null;
                    full.as_object_mut().unwrap().remove("failureReason");
                    ok(&json!({"metricAttribution": full}))
                }
                Err(e) => e,
            }
        }
        "DeleteMetricAttribution" => {
            let arn = try_response!(str_param(
                p,
                "metricAttributionArn",
                "ResourceNotFoundException"
            ));
            if remove(&mut st.metric_attributions, "metricAttributionArn", &arn) {
                ok(&json!({}))
            } else {
                resource_not_found(&arn)
            }
        }
        "ListMetricAttributions" => {
            let filter = p.get("datasetGroupArn").and_then(Value::as_str);
            let attributions: Vec<Value> = st
                .metric_attributions
                .iter()
                .filter(|a| filter.is_none_or(|f| a["datasetGroupArn"] == json!(f)))
                .map(|a| {
                    pick(
                        a,
                        &[
                            "name",
                            "metricAttributionArn",
                            "status",
                            "creationDateTime",
                            "lastUpdatedDateTime",
                        ],
                    )
                })
                .collect();
            ok(&json!({"metricAttributions": attributions, "nextToken": Value::Null}))
        }
        "ListMetricAttributionMetrics" => {
            let filter = p.get("metricAttributionArn").and_then(Value::as_str);
            let mut metrics: Vec<Value> = Vec::new();
            for a in st
                .metric_attributions
                .iter()
                .filter(|a| filter.is_none_or(|f| a["metricAttributionArn"] == json!(f)))
            {
                if let Some(list) = a["metrics"].as_array() {
                    metrics.extend(list.iter().cloned());
                }
            }
            ok(&json!({"metrics": metrics, "nextToken": Value::Null}))
        }
        "UpdateMetricAttribution" => {
            let arn = try_response!(str_param(
                p,
                "metricAttributionArn",
                "ResourceNotFoundException"
            ));
            match st
                .metric_attributions
                .iter_mut()
                .find(|a| a["metricAttributionArn"] == json!(arn))
            {
                Some(r) => {
                    if let Some(add) = p.get("addMetrics").and_then(Value::as_array) {
                        let mut combined = if r["metrics"].is_array() {
                            r["metrics"].as_array().unwrap().clone()
                        } else {
                            Vec::new()
                        };
                        for m in add {
                            combined.push(m.clone());
                        }
                        r["metrics"] = json!(combined);
                    }
                    if let Some(remove_names) = p.get("removeMetrics").and_then(Value::as_array) {
                        if let Some(list) = r["metrics"].as_array_mut() {
                            list.retain(|m| {
                                !remove_names.iter().any(|name| {
                                    m["metricName"] == *name || m.get("name") == Some(name)
                                })
                            });
                        }
                    }
                    if let Some(cfg) = p.get("metricsOutputConfig").filter(|v| !v.is_null()) {
                        r["metricsOutputConfig"] = cfg.clone();
                    }
                    r["lastUpdatedDateTime"] = now();
                    ok(&json!({"metricAttributionArn": arn}))
                }
                None => resource_not_found(&arn),
            }
        }

        // -------------------------------------------------------------------
        // Built-in catalogs
        // -------------------------------------------------------------------
        "DescribeRecipe" => {
            let arn = try_response!(str_param(p, "recipeArn", "ResourceNotFoundException"));
            match find(&builtin_recipes(), "recipeArn", &arn) {
                Some(r) => ok(&json!({"recipe": r})),
                None => resource_not_found(&arn),
            }
        }
        "ListRecipes" => {
            let recipes: Vec<Value> = builtin_recipes()
                .iter()
                .map(|r| {
                    pick(
                        r,
                        &[
                            "name",
                            "recipeArn",
                            "status",
                            "creationDateTime",
                            "lastUpdatedDateTime",
                        ],
                    )
                })
                .collect();
            ok(&json!({"recipes": recipes, "nextToken": Value::Null}))
        }
        "DescribeAlgorithm" => {
            let arn = try_response!(str_param(p, "algorithmArn", "ResourceNotFoundException"));
            let description = if let Some(r) = builtin_recipes()
                .iter()
                .find(|r| r["algorithmArn"] == json!(arn))
            {
                Some(r.clone())
            } else {
                let tail = arn.rsplit('/').next().unwrap_or_default();
                builtin_recipes()
                    .iter()
                    .find(|r| r["name"] == json!(tail))
                    .map(|r| {
                        json!({
                            "name": r["name"],
                            "algorithmArn": r["algorithmArn"],
                            "algorithmImage": {"dockerURI": format!("{}-image", r["algorithmArn"]), "name": r["name"]},
                            "defaultHyperParameters": {},
                            "defaultResourceConfig": {},
                            "trainingInputMode": "File",
                            "creationDateTime": "2020-01-01T00:00:00Z",
                            "lastUpdatedDateTime": "2020-01-01T00:00:00Z",
                        })
                    })
            };
            match description {
                Some(r) => ok(&json!({"algorithm": r})),
                None => resource_not_found(&arn),
            }
        }
        "DescribeFeatureTransformation" => {
            let arn = try_response!(str_param(
                p,
                "featureTransformationArn",
                "ResourceNotFoundException"
            ));
            let name = arn.rsplit('/').next().unwrap_or_default();
            ok(&json!({"featureTransformation": json!({
                "name": name,
                "featureTransformationArn": arn,
                "status": "ACTIVE",
                "creationDateTime": "2020-01-01T00:00:00Z",
                "lastUpdatedDateTime": "2020-01-01T00:00:00Z",
                "defaultParameters": {"item_id": "ID"},
            })}))
        }

        // -------------------------------------------------------------------
        // Tags
        // -------------------------------------------------------------------
        "TagResource" => {
            let arn = try_response!(str_param(p, "resourceArn", "InvalidInputException"));
            set_tags(st, &arn, p.get("tags").unwrap_or(&json!([])));
            ok(&json!({}))
        }
        "UntagResource" => {
            let arn = try_response!(str_param(p, "resourceArn", "InvalidInputException"));
            let keys: Vec<String> = p
                .get("tagKeys")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default();
            if let Some(pos) = st.tags.iter().position(|(a, _)| a == &arn) {
                let (_, tags) = st.tags.remove(pos);
                let kept: Vec<(String, String)> = tags
                    .into_iter()
                    .filter(|(k, _)| !keys.contains(k))
                    .collect();
                st.tags.push((arn.clone(), kept));
            }
            ok(&json!({}))
        }
        "ListTagsForResource" => {
            let arn = try_response!(str_param(p, "resourceArn", "InvalidInputException"));
            ok(&json!({"tags": tags_json(st, &arn)}))
        }
        _ => {
            let e = json!({
                "__type": "NotImplemented",
                "message": format!("The {} action has not been implemented", req.operation),
            });
            Response::json(501, serde_json::to_vec(&e).unwrap_or_default())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn req(op: &str) -> Request {
        Request {
            service: "personalize".into(),
            operation: op.into(),
            method: "POST".into(),
            path: "/".into(),
            query_string: String::new(),
            headers: Default::default(),
            body: bytes::Bytes::new(),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    /// Run an op against a fresh partition and return the parsed body.
    fn call(op: &str, account: u64, params: Value) -> (u16, Value) {
        let slot = state();
        let mut st = slot.write();
        let b = st.entry((account, "us-east-1".into())).or_default();
        let r = handle(b, &params, "us-east-1", account, &req(op));
        let body = serde_json::from_slice(&r.body).unwrap();
        (r.status, body)
    }

    fn arn_of(kind: &str, account: u64, name: &str) -> String {
        format!("arn:aws:personalize:us-east-1:{account}:{kind}/{name}")
    }

    /// dataset group + schema + INTERACTIONS dataset, the base for jobs.
    fn seeded_dataset(account: u64, name: &str) -> String {
        let (status, group) = call(
            "CreateDatasetGroup",
            account,
            json!({"name": name, "domain": "ECOMMERCE"}),
        );
        assert_eq!(status, 200);
        assert_eq!(
            group["datasetGroupArn"],
            json!(arn_of("dataset-group", account, name))
        );
        let (_, schema) = call("CreateSchema", account, json!({"name": "schema"}));
        let (status, dataset) = call(
            "CreateDataset",
            account,
            json!({
                "name": "interactions",
                "datasetGroupArn": group["datasetGroupArn"],
                "schemaArn": schema["schemaArn"],
                "datasetType": "INTERACTIONS",
            }),
        );
        assert_eq!(status, 200);
        assert_eq!(
            dataset["datasetArn"],
            json!(arn_of("dataset", account, "interactions"))
        );
        dataset["datasetArn"].as_str().unwrap().to_string()
    }

    #[test]
    fn schema_family_preserved() {
        let account = 777000000001;
        let created = call(
            "CreateSchema",
            account,
            json!({"name": "s1", "schema": {"type": "record"}, "domain": "ECOMMERCE"}),
        );
        assert_eq!(created.0, 200);
        let schema_arn = created.1["schemaArn"].as_str().unwrap().to_string();

        let listed = call("ListSchemas", account, json!({}));
        assert_eq!(listed.1["schemas"].as_array().unwrap().len(), 1);
        // moto quirk: summaries omit the `schema` member
        assert!(listed.1["schemas"][0].get("schema").is_none());

        let described = call("DescribeSchema", account, json!({"schemaArn": &schema_arn}));
        assert_eq!(described.1["schema"]["schemaArn"], json!(schema_arn));
        // moto quirk: lastUpdatedDateTime is forced to the creation date
        assert_eq!(
            described.1["schema"]["lastUpdatedDateTime"],
            described.1["schema"]["creationDateTime"]
        );

        let missing = call(
            "DescribeSchema",
            account,
            json!({"schemaArn": "arn:aws:personalize:us-east-1:123456789012:schema/nope"}),
        );
        assert_eq!(missing.0, 400);
        assert_eq!(missing.1["__type"], "ResourceNotFoundException");

        let deleted = call("DeleteSchema", account, json!({"schemaArn": &schema_arn}));
        assert_eq!(deleted.0, 200);
        let missing = call("DeleteSchema", account, json!({"schemaArn": &schema_arn}));
        assert_eq!(missing.0, 400);
    }

    #[test]
    fn dataset_group_and_dataset_roundtrip() {
        let account = 777000000002;
        let dataset_arn = seeded_dataset(account, "g1");

        let described = call(
            "DescribeDataset",
            account,
            json!({"datasetArn": &dataset_arn}),
        );
        assert_eq!(described.1["dataset"]["status"], "ACTIVE");
        assert_eq!(described.1["dataset"]["datasetType"], "INTERACTIONS");

        let listed = call(
            "ListDatasets",
            account,
            json!({"datasetGroupArn": arn_of("dataset-group", account, "g1")}),
        );
        assert_eq!(listed.1["datasets"][0]["datasetArn"], json!(dataset_arn));

        // update the schema to a second one
        let (_, schema2) = call("CreateSchema", account, json!({"name": "schema2"}));
        let updated = call(
            "UpdateDataset",
            account,
            json!({
                "datasetArn": &dataset_arn,
                "schemaArn": schema2["schemaArn"],
            }),
        );
        assert_eq!(updated.0, 200);
        let described = call(
            "DescribeDataset",
            account,
            json!({"datasetArn": &dataset_arn}),
        );
        assert_eq!(described.1["dataset"]["schemaArn"], schema2["schemaArn"]);

        // group deletion cascades the dataset
        let deleted = call(
            "DeleteDatasetGroup",
            account,
            json!({"datasetGroupArn": arn_of("dataset-group", account, "g1")}),
        );
        assert_eq!(deleted.0, 200);
        let missing = call(
            "DescribeDataset",
            account,
            json!({"datasetArn": &dataset_arn}),
        );
        assert_eq!(missing.0, 400);
        assert_eq!(missing.1["__type"], "ResourceNotFoundException");

        let listed = call("ListDatasetGroups", account, json!({}));
        assert!(listed.1["datasetGroups"].as_array().unwrap().is_empty());
    }

    #[test]
    fn import_export_and_deletion_jobs() {
        let account = 777000000003;
        let dataset_arn = seeded_dataset(account, "g1");

        let created = call(
            "CreateDatasetImportJob",
            account,
            json!({
                "jobName": "imp1",
                "datasetArn": &dataset_arn,
                "dataSource": {"dataLocation": "s3://bucket/file.csv"},
                "importMode": "FULL",
                "roleArn": "arn:aws:iam::123456789012:role/pz",
                "tags": [{"tagKey": "env", "tagValue": "test"}],
            }),
        );
        assert_eq!(created.0, 200);
        let job_arn = created.1["datasetImportJobArn"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(job_arn, arn_of("dataset-import-job", account, "imp1"));

        let described = call(
            "DescribeDatasetImportJob",
            account,
            json!({"datasetImportJobArn": &job_arn}),
        );
        assert_eq!(described.1["datasetImportJob"]["status"], "ACTIVE");
        assert_eq!(described.1["datasetImportJob"]["importMode"], "FULL");

        let listed = call("ListDatasetImportJobs", account, json!({}));
        assert_eq!(listed.1["datasetImportJobs"][0]["jobName"], "imp1");

        // filter narrows by dataset
        let listed = call(
            "ListDatasetImportJobs",
            account,
            json!({"datasetArn": arn_of("dataset", account, "nope")}),
        );
        assert!(listed.1["datasetImportJobs"].as_array().unwrap().is_empty());

        let created = call(
            "CreateDatasetExportJob",
            account,
            json!({
                "jobName": "exp1",
                "datasetArn": &dataset_arn,
                "roleArn": "arn:aws:iam::123456789012:role/pz",
                "jobOutput": {"s3DataDestination": {"s3BucketName": "b", "s3Key": "k"}},
            }),
        );
        let exp_arn = created.1["datasetExportJobArn"]
            .as_str()
            .unwrap()
            .to_string();
        let described = call(
            "DescribeDatasetExportJob",
            account,
            json!({"datasetExportJobArn": &exp_arn}),
        );
        assert_eq!(described.1["datasetExportJob"]["status"], "ACTIVE");

        let created = call(
            "CreateDataDeletionJob",
            account,
            json!({
                "jobName": "del1",
                "datasetGroupArn": arn_of("dataset-group", account, "g1"),
                "dataSource": {"dataLocation": "s3://bucket/deleted.csv"},
                "roleArn": "arn:aws:iam::123456789012:role/pz",
            }),
        );
        let del_arn = created.1["dataDeletionJobArn"]
            .as_str()
            .unwrap()
            .to_string();
        let described = call(
            "DescribeDataDeletionJob",
            account,
            json!({"dataDeletionJobArn": &del_arn}),
        );
        assert_eq!(described.1["dataDeletionJob"]["numDeleted"], json!(0));

        let listed = call("ListDataDeletionJobs", account, json!({}));
        assert_eq!(listed.1["dataDeletionJobs"][0]["status"], "ACTIVE");

        // unknown job ARNs miss with the modeled envelope
        let missing = call(
            "DescribeBatchInferenceJob",
            account,
            json!({"batchInferenceJobArn": arn_of("batch-inference-job", account, "nope")}),
        );
        assert_eq!(missing.0, 400);
        assert_eq!(missing.1["__type"], "ResourceNotFoundException");
    }

    /// dataset group + schema + dataset + solution + ACTIVE solution version.
    fn seeded_solution_version(account: u64) -> (String, String) {
        let _ = seeded_dataset(account, "solgroup");
        let (status, solution) = call(
            "CreateSolution",
            account,
            json!({
                "name": "my-solution",
                "datasetGroupArn": arn_of("dataset-group", account, "solgroup"),
                "recipeArn": "arn:aws:personalize:::recipe/aws-user-personalization",
            }),
        );
        assert_eq!(status, 200);
        let sol_arn = solution["solutionArn"].as_str().unwrap().to_string();
        assert_eq!(sol_arn, arn_of("solution", account, "my-solution"));
        let (_, version) = call(
            "CreateSolutionVersion",
            account,
            json!({"solutionArn": &sol_arn, "trainingMode": "FULL"}),
        );
        let sv_arn = version["solutionVersionArn"].as_str().unwrap().to_string();
        assert!(sv_arn.starts_with(&format!("{sol_arn}/")));
        (sol_arn, sv_arn)
    }

    #[test]
    fn solution_version_metrics_campaign() {
        let account = 777000000004;
        let (sol_arn, sv_arn) = seeded_solution_version(account);

        let described = call(
            "DescribeSolution",
            account,
            json!({"solutionArn": &sol_arn}),
        );
        assert_eq!(described.1["solution"]["status"], "ACTIVE");
        // newest version surfaces as latestSolutionVersion
        assert_eq!(
            described.1["solution"]["latestSolutionVersion"]["solutionVersionArn"],
            json!(sv_arn)
        );

        let sv = call(
            "DescribeSolutionVersion",
            account,
            json!({"solutionVersionArn": &sv_arn}),
        );
        assert_eq!(sv.1["solutionVersion"]["status"], "ACTIVE");
        assert_eq!(sv.1["solutionVersion"]["trainingType"], "FULL");

        let metrics = call(
            "GetSolutionMetrics",
            account,
            json!({"solutionVersionArn": &sv_arn}),
        );
        assert_eq!(metrics.1["solutionVersionArn"], json!(sv_arn));
        assert!(metrics.1["metrics"].is_object());

        let campaigns = call("ListCampaigns", account, json!({}));
        assert!(campaigns.1["campaigns"].as_array().unwrap().is_empty());

        let created = call(
            "CreateCampaign",
            account,
            json!({
                "name": "camp1",
                "solutionVersionArn": &sv_arn,
                "minProvisionedTPS": 1,
            }),
        );
        let camp_arn = created.1["campaignArn"].as_str().unwrap().to_string();
        assert_eq!(camp_arn, arn_of("campaign", account, "camp1"));

        let updated = call(
            "UpdateCampaign",
            account,
            json!({"campaignArn": &camp_arn, "minProvisionedTPS": 2}),
        );
        assert_eq!(updated.1["campaignArn"], json!(camp_arn));
        let described = call(
            "DescribeCampaign",
            account,
            json!({"campaignArn": &camp_arn}),
        );
        assert_eq!(described.1["campaign"]["minProvisionedTPS"], json!(2));
        assert_eq!(
            described.1["campaign"]["latestCampaignUpdate"]["status"],
            "UPDATE IN PROGRESS"
        );

        let listed = call("ListCampaigns", account, json!({}));
        assert_eq!(listed.1["campaigns"].as_array().unwrap().len(), 1);

        let deleted = call("DeleteCampaign", account, json!({"campaignArn": &camp_arn}));
        assert_eq!(deleted.0, 200);
        let missing = call(
            "DescribeCampaign",
            account,
            json!({"campaignArn": &camp_arn}),
        );
        assert_eq!(missing.0, 400);

        // solution deletion removes its versions
        let deleted = call("DeleteSolution", account, json!({"solutionArn": &sol_arn}));
        assert_eq!(deleted.0, 200);
        let missing = call(
            "DescribeSolutionVersion",
            account,
            json!({"solutionVersionArn": &sv_arn}),
        );
        assert_eq!(missing.0, 400);
        assert_eq!(missing.1["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn recommender_lifecycle() {
        let account = 777000000005;
        let _ = seeded_dataset(account, "recgroup");
        let created = call(
            "CreateRecommender",
            account,
            json!({
                "name": "rec1",
                "datasetGroupArn": arn_of("dataset-group", account, "recgroup"),
                "recipeArn": "arn:aws:personalize:::recipe/aws-user-personalization",
            }),
        );
        let rec_arn = created.1["recommenderArn"].as_str().unwrap().to_string();

        let described = call(
            "DescribeRecommender",
            account,
            json!({"recommenderArn": &rec_arn}),
        );
        assert_eq!(described.1["recommender"]["status"], "ACTIVE");
        assert!(described.1["recommender"]["modelMetrics"].is_object());

        let stopped = call(
            "StopRecommender",
            account,
            json!({"recommenderArn": &rec_arn}),
        );
        assert_eq!(stopped.1["recommenderArn"], json!(rec_arn));
        let described = call(
            "DescribeRecommender",
            account,
            json!({"recommenderArn": &rec_arn}),
        );
        assert_eq!(described.1["recommender"]["status"], "INACTIVE");

        let started = call(
            "StartRecommender",
            account,
            json!({"recommenderArn": &rec_arn}),
        );
        assert_eq!(started.1["recommenderArn"], json!(rec_arn));
        let described = call(
            "DescribeRecommender",
            account,
            json!({"recommenderArn": &rec_arn}),
        );
        assert_eq!(described.1["recommender"]["status"], "ACTIVE");

        let updated = call(
            "UpdateRecommender",
            account,
            json!({
                "recommenderArn": &rec_arn,
                "recommenderConfig": {"itemExplorationConfig": {"explorationWeight": "0.1"}},
            }),
        );
        assert_eq!(updated.1["recommenderArn"], json!(rec_arn));
        let described = call(
            "DescribeRecommender",
            account,
            json!({"recommenderArn": &rec_arn}),
        );
        assert_eq!(
            described.1["recommender"]["recommenderConfig"]["itemExplorationConfig"]
                ["explorationWeight"],
            json!("0.1")
        );

        let listed = call("ListRecommenders", account, json!({}));
        assert_eq!(listed.1["recommenders"].as_array().unwrap().len(), 1);

        let deleted = call(
            "DeleteRecommender",
            account,
            json!({"recommenderArn": &rec_arn}),
        );
        assert_eq!(deleted.0, 200);
        let missing = call(
            "DescribeRecommender",
            account,
            json!({"recommenderArn": &rec_arn}),
        );
        assert_eq!(missing.0, 400);
    }

    #[test]
    fn event_tracker_and_filter() {
        let account = 777000000006;
        let _ = seeded_dataset(account, "tg");

        let tracker = call(
            "CreateEventTracker",
            account,
            json!({"name": "t1", "datasetGroupArn": arn_of("dataset-group", account, "tg")}),
        );
        let tracker_arn = tracker.1["eventTrackerArn"].as_str().unwrap().to_string();
        assert_eq!(tracker_arn, arn_of("event-tracker", account, "t1"));
        // trackingId is always present on create
        assert!(tracker.1["trackingId"].as_str().unwrap().len() >= 32);

        let described = call(
            "DescribeEventTracker",
            account,
            json!({"eventTrackerArn": &tracker_arn}),
        );
        assert_eq!(
            described.1["eventTracker"]["trackingId"],
            tracker.1["trackingId"]
        );

        let listed = call("ListEventTrackers", account, json!({}));
        assert_eq!(listed.1["eventTrackers"][0]["status"], "ACTIVE");

        let filter = call(
            "CreateFilter",
            account,
            json!({
                "name": "f1",
                "datasetGroupArn": arn_of("dataset-group", account, "tg"),
                "filterExpression": "INCLUDE ITEMID WHERE Items.CATEGORY IN (\\\"a\\\")",
            }),
        );
        let filter_arn = filter.1["filterArn"].as_str().unwrap().to_string();
        let described = call("DescribeFilter", account, json!({"filterArn": &filter_arn}));
        assert_eq!(described.1["filter"]["status"], "ACTIVE");
        assert!(described.1["filter"]["filterExpression"]
            .as_str()
            .unwrap()
            .contains("INCLUDE"));

        let listed = call("ListFilters", account, json!({}));
        // real AWS quirks this member name as `Filters` (capital F)
        assert_eq!(listed.1["Filters"][0]["filterArn"], json!(filter_arn));

        let deleted = call("DeleteFilter", account, json!({"filterArn": &filter_arn}));
        assert_eq!(deleted.0, 200);
        let missing = call("DescribeFilter", account, json!({"filterArn": &filter_arn}));
        assert_eq!(missing.0, 400);
        let deleted = call(
            "DeleteEventTracker",
            account,
            json!({"eventTrackerArn": &tracker_arn}),
        );
        assert_eq!(deleted.0, 200);
    }

    #[test]
    fn metric_attribution_family() {
        let account = 777000000007;
        let _ = seeded_dataset(account, "mag");

        let created = call(
            "CreateMetricAttribution",
            account,
            json!({
                "name": "ma1",
                "datasetGroupArn": arn_of("dataset-group", account, "mag"),
                "metrics": [{"name": "m1", "eventType": "click", "metricExpression": "COUNT"}],
                "metricsOutputConfig": {"roleArn": "arn:aws:iam::123456789012:role/pz"},
            }),
        );
        let ma_arn = created.1["metricAttributionArn"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(ma_arn, arn_of("metric-attribution", account, "ma1"));

        let described = call(
            "DescribeMetricAttribution",
            account,
            json!({"metricAttributionArn": &ma_arn}),
        );
        assert_eq!(described.1["metricAttribution"]["status"], "ACTIVE");

        let metrics = call(
            "ListMetricAttributionMetrics",
            account,
            json!({"metricAttributionArn": &ma_arn}),
        );
        assert_eq!(metrics.1["metrics"].as_array().unwrap().len(), 1);

        let updated = call(
            "UpdateMetricAttribution",
            account,
            json!({"metricAttributionArn": &ma_arn, "removeMetrics": ["m1"]}),
        );
        assert_eq!(updated.1["metricAttributionArn"], json!(ma_arn));
        let metrics = call(
            "ListMetricAttributionMetrics",
            account,
            json!({"metricAttributionArn": &ma_arn}),
        );
        assert!(metrics.1["metrics"].as_array().unwrap().is_empty());
        let described = call(
            "DescribeMetricAttribution",
            account,
            json!({"metricAttributionArn": &ma_arn}),
        );
        assert_eq!(
            described.1["metricAttribution"]["metrics"]
                .as_array()
                .unwrap()
                .len(),
            0
        );

        let listed = call("ListMetricAttributions", account, json!({}));
        assert_eq!(
            listed.1["metricAttributions"][0]["metricAttributionArn"],
            json!(ma_arn)
        );

        let deleted = call(
            "DeleteMetricAttribution",
            account,
            json!({"metricAttributionArn": &ma_arn}),
        );
        assert_eq!(deleted.0, 200);
        let missing = call(
            "DescribeMetricAttribution",
            account,
            json!({"metricAttributionArn": &ma_arn}),
        );
        assert_eq!(missing.0, 400);
    }

    #[test]
    fn batch_jobs_roundtrip() {
        let account = 777000000008;
        let (_, sv_arn) = seeded_solution_version(account);

        for (op, kind) in [
            ("CreateBatchInferenceJob", "batch-inference-job"),
            ("CreateBatchSegmentJob", "batch-segment-job"),
        ] {
            let created = call(
                op,
                account,
                json!({
                    "jobName": kind,
                    "solutionVersionArn": &sv_arn,
                    "jobInput": {"s3DataSource": {"s3BucketName": "b", "s3Key": "in"}},
                    "jobOutput": {"s3DataDestination": {"s3BucketName": "b", "s3Key": "out"}},
                    "roleArn": "arn:aws:iam::123456789012:role/pz",
                }),
            );
            let arn_v = if op == "CreateBatchInferenceJob" {
                created.1["batchInferenceJobArn"]
                    .as_str()
                    .unwrap()
                    .to_string()
            } else {
                created.1["batchSegmentJobArn"]
                    .as_str()
                    .unwrap()
                    .to_string()
            };
            assert_eq!(arn_v, arn_of(kind, account, kind));
        }

        let listed = call("ListBatchInferenceJobs", account, json!({}));
        assert_eq!(listed.1["batchInferenceJobs"][0]["status"], "ACTIVE");
        let listed = call("ListBatchSegmentJobs", account, json!({}));
        assert_eq!(listed.1["batchSegmentJobs"][0]["status"], "ACTIVE");

        // both describe with the modeled envelope
        let described = call(
            "DescribeBatchInferenceJob",
            account,
            json!({"batchInferenceJobArn": arn_of("batch-inference-job", account, "batch-inference-job")}),
        );
        assert_eq!(described.1["batchInferenceJob"]["numResults"], Value::Null);
        let described = call(
            "DescribeBatchSegmentJob",
            account,
            json!({"batchSegmentJobArn": arn_of("batch-segment-job", account, "batch-segment-job")}),
        );
        assert_eq!(described.1["batchSegmentJob"]["status"], "ACTIVE");
    }

    #[test]
    fn builtin_catalogs() {
        let account = 777000000009;
        // ListRecipes answers the built-in catalog
        let listed = call("ListRecipes", account, json!({}));
        let arns: Vec<&str> = listed.1["recipes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["recipeArn"].as_str().unwrap())
            .collect();
        assert!(arns.contains(&"arn:aws:personalize:::recipe/aws-user-personalization"));

        // DescribeRecipe finds catalog entries by ARN and denies others
        let recipe = call(
            "DescribeRecipe",
            account,
            json!({"recipeArn": "arn:aws:personalize:::recipe/aws-similar-items"}),
        );
        assert_eq!(recipe.1["recipe"]["name"], "aws-similar-items");
        let missing = call(
            "DescribeRecipe",
            account,
            json!({"recipeArn": "arn:aws:personalize:us-east-1:123456789012:thing/probe-test"}),
        );
        assert_eq!(missing.0, 400);

        let algo = call(
            "DescribeAlgorithm",
            account,
            json!({"algorithmArn": "arn:aws:personalize:::algorithm/aws-similar-items"}),
        );
        assert_eq!(algo.1["algorithm"]["name"], "aws-similar-items");
        let missing = call(
            "DescribeAlgorithm",
            account,
            json!({"algorithmArn": "arn:aws:personalize:us-east-1:123456789012:thing/probe-test"}),
        );
        assert_eq!(missing.0, 400);

        let ft = call(
            "DescribeFeatureTransformation",
            account,
            json!({"featureTransformationArn": "arn:aws:personalize:us-east-1:123456789012:feature-transformation/aws-similar-items"}),
        );
        assert!(ft.1["featureTransformation"]["defaultParameters"].is_object());
    }

    #[test]
    fn tag_family() {
        let account = 777000000010;
        let dataset_arn = seeded_dataset(account, "taggroup");
        let dsg = arn_of("dataset-group", account, "taggroup");

        // tags flow into ListTagsForResource
        let listed = call("ListTagsForResource", account, json!({"resourceArn": &dsg}));
        assert!(listed.1["tags"].as_array().unwrap().is_empty());

        let (_, schema3) = call("CreateSchema", account, json!({"name": "s3"}));
        let created = call(
            "CreateDataset",
            account,
            json!({
                "name": "tagged",
                "datasetGroupArn": &dsg,
                "schemaArn": schema3["schemaArn"],
                "datasetType": "USERS",
                "tags": [{"tagKey": "env", "tagValue": "test"}],
            }),
        );
        assert!(created.1["datasetArn"].as_str().unwrap().contains("tagged"));

        let listed = call(
            "ListTagsForResource",
            account,
            json!({"resourceArn": &dataset_arn}),
        );
        assert!(listed.1["tags"].as_array().unwrap().is_empty());

        let added = call(
            "TagResource",
            account,
            json!({"resourceArn": &dataset_arn, "tags": [{"tagKey": "k", "tagValue": "v"}]}),
        );
        assert_eq!(added.0, 200);
        let listed = call(
            "ListTagsForResource",
            account,
            json!({"resourceArn": &dataset_arn}),
        );
        assert_eq!(listed.1["tags"][0]["tagKey"], "k");

        let removed = call(
            "UntagResource",
            account,
            json!({"resourceArn": &dataset_arn, "tagKeys": ["k"]}),
        );
        assert_eq!(removed.0, 200);
        let listed = call(
            "ListTagsForResource",
            account,
            json!({"resourceArn": &dataset_arn}),
        );
        assert!(listed.1["tags"].as_array().unwrap().is_empty());
    }

    #[test]
    fn stop_solution_version_creation() {
        let account = 777000000011;
        let (_, sv_arn) = seeded_solution_version(account);
        let stopped = call(
            "StopSolutionVersionCreation",
            account,
            json!({"solutionVersionArn": &sv_arn}),
        );
        assert_eq!(stopped.0, 200);
        let described = call(
            "DescribeSolutionVersion",
            account,
            json!({"solutionVersionArn": &sv_arn}),
        );
        assert_eq!(described.1["solutionVersion"]["status"], "CREATE STOPPED");
        let missing = call(
            "StopSolutionVersionCreation",
            account,
            json!({"solutionVersionArn": arn_of("solution/nope", account, "x")}),
        );
        assert_eq!(missing.0, 400);
    }
}
