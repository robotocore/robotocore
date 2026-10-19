//! AWS Data Pipeline backend (JSON protocol, target `DataPipeline.<Op>`).
//!
//! Port of moto's datapipeline backend: pipeline CRUD, definition storage,
//! object queries, expressions, and the task-runner stubs. Wire names match
//! botocore's datapipeline-2012-10-29 model.

use chrono::{DateTime, Utc};
use once_cell::sync::Lazy;
use serde_json::{json, Value};

use crate::store;
use crate::{Request, Response};

/// An object from a pipeline definition: id/name plus the submitted fields
/// kept verbatim (only `SetStatus` ever mutates them).
struct PipelineObject {
    id: String,
    name: String,
    fields: Value,
}

impl PipelineObject {
    fn to_json(&self) -> Value {
        json!({"fields": self.fields, "id": self.id, "name": self.name})
    }
}

struct Pipeline {
    name: String,
    unique_id: String,
    description: String,
    pipeline_id: String,
    creation_time: DateTime<Utc>,
    status: String,
    objects: Vec<PipelineObject>,
    tags: Vec<Value>,
}

impl Pipeline {
    fn new(name: &str, unique_id: &str, description: &str, tags: Vec<Value>) -> Self {
        Self {
            name: name.to_string(),
            unique_id: unique_id.to_string(),
            description: description.to_string(),
            pipeline_id: random_pipeline_id(),
            creation_time: Utc::now(),
            status: "PENDING".to_string(),
            objects: Vec::new(),
            tags,
        }
    }

    fn to_meta_json(&self) -> Value {
        json!({"id": self.pipeline_id, "name": self.name})
    }

    /// The fields every pipeline surfaces on DescribePipelines.
    fn to_fields(&self) -> Value {
        json!([
            {"key": "@pipelineState", "stringValue": self.status},
            {"key": "description", "stringValue": self.description},
            {"key": "name", "stringValue": self.name},
            {
                "key": "@creationTime",
                "stringValue": self.creation_time.format("%Y-%m-%dT%H-%M-%S").to_string(),
            },
            {"key": "@id", "stringValue": self.pipeline_id},
            {"key": "@sphere", "stringValue": "PIPELINE"},
            {"key": "@version", "stringValue": "1"},
            {"key": "@userId", "stringValue": "924374875933"},
            {"key": "@accountId", "stringValue": "924374875933"},
            {"key": "uniqueId", "stringValue": self.unique_id},
        ])
    }

    fn to_json(&self) -> Value {
        json!({
            "description": self.description,
            "fields": self.to_fields(),
            "name": self.name,
            "pipelineId": self.pipeline_id,
            "tags": self.tags,
        })
    }

    /// Merge tags: a later tag replaces any earlier tag with the same key.
    fn add_tags(&mut self, tags: &[Value]) {
        for tag in tags {
            let key = tag.get("key").and_then(Value::as_str).unwrap_or("");
            match self.tags.iter().position(|t| t["key"] == json!(key)) {
                Some(i) => self.tags[i] = tag.clone(),
                None => self.tags.push(tag.clone()),
            }
        }
    }

    fn remove_tags(&mut self, tag_keys: &[Value]) {
        self.tags
            .retain(|t| !tag_keys.iter().any(|k| t["key"] == json!(k)));
    }

    fn set_pipeline_objects(&mut self, objects: &[Value]) {
        self.objects = objects
            .iter()
            .map(|o| PipelineObject {
                id: o["id"].as_str().unwrap_or("").to_string(),
                name: o["name"].as_str().unwrap_or("").to_string(),
                fields: lower_first_of_keys(&o["fields"]),
            })
            .collect();
    }
}

#[derive(Default)]
struct State {
    pipelines: Vec<Pipeline>,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("datapipeline"));
    SLOT.clone()
}

fn ok(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

/// `df-` followed by 19 lowercase hex characters, moto's id format.
fn random_pipeline_id() -> String {
    let hex = uuid::Uuid::new_v4().simple().to_string();
    format!("df-{}", &hex[..19])
}

/// Lowercase the leading character of every mapping key, nested trees
/// included (moto's `remove_capitalization_of_dict_keys`).
fn lower_first_of_keys(v: &Value) -> Value {
    match v {
        Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (k, val) in map {
                let mut chars = k.chars();
                let key = match chars.next() {
                    Some(first) => format!("{}{}", first.to_lowercase(), chars.as_str()),
                    None => k.clone(),
                };
                out.insert(key, lower_first_of_keys(val));
            }
            Value::Object(out)
        }
        Value::Array(arr) => Value::Array(arr.iter().map(lower_first_of_keys).collect()),
        other => other.clone(),
    }
}

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "datapipeline" {
        return None;
    }
    let body = crate::proto::parse_json_body(&req.body);
    let slot = state();
    let mut st = slot.write();
    let b = st.entry((req.account, req.region.clone())).or_default();
    Some(handle(b, &body, req))
}

fn not_found(kind: &str, pipeline_id: &str) -> Response {
    crate::proto::json_error(400, kind, &format!("Missing Pipeline {pipeline_id}"))
}

/// Mutable access to a pipeline or a `PipelineNotFoundException` response.
fn with_pipeline<'a>(st: &'a mut State, id: &str) -> Result<&'a mut Pipeline, Response> {
    let not_found = not_found("PipelineNotFoundException", id);
    match st.pipelines.iter_mut().find(|p| p.pipeline_id == id) {
        Some(p) => Ok(p),
        None => Err(not_found),
    }
}

fn handle(st: &mut State, p: &Value, req: &Request) -> Response {
    match req.operation.as_str() {
        "CreatePipeline" => {
            let name = p["name"].as_str().unwrap_or("");
            let unique_id = p["uniqueId"].as_str().unwrap_or("");
            let description = p["description"].as_str().unwrap_or("");
            let tags = p["tags"].as_array().cloned().unwrap_or_default();
            let pipeline = Pipeline::new(name, unique_id, description, tags);
            let id = pipeline.pipeline_id.clone();
            st.pipelines.push(pipeline);
            ok(&json!({"pipelineId": id}))
        }
        "ListPipelines" => {
            let ids: Vec<String> = st.pipelines.iter().map(|p| p.pipeline_id.clone()).collect();
            let max = 50usize;
            let marker = p["marker"].as_str().unwrap_or("");
            let start = if marker.is_empty() {
                0
            } else {
                ids.iter().position(|id| id == marker).map_or(0, |i| i + 1)
            };
            let page: Vec<Value> = st
                .pipelines
                .iter()
                .skip(start)
                .take(max)
                .map(Pipeline::to_meta_json)
                .collect();
            let has_more = start + max < ids.len().saturating_sub(1);
            let marker = if has_more {
                page.last().and_then(|m| m["id"].as_str()).unwrap_or("")
            } else {
                ""
            };
            let marker = if marker.is_empty() {
                Value::Null
            } else {
                json!(marker)
            };
            ok(&json!({
                "hasMoreResults": has_more,
                "marker": marker,
                "pipelineIdList": page,
            }))
        }
        "DescribePipelines" => {
            let wanted = p["pipelineIds"].as_array().cloned().unwrap_or_default();
            let list: Vec<Value> = st
                .pipelines
                .iter()
                .filter(|pl| wanted.iter().any(|w| w == &json!(pl.pipeline_id)))
                .map(|pl| pl.to_json())
                .collect();
            ok(&json!({"pipelineDescriptionList": list}))
        }
        "DeletePipeline" => {
            let id = p["pipelineId"].as_str().unwrap_or("");
            st.pipelines.retain(|pl| pl.pipeline_id != id);
            ok(&json!({}))
        }
        "PutPipelineDefinition" => {
            let id = p["pipelineId"].as_str().unwrap_or("");
            match with_pipeline(st, id) {
                Err(resp) => resp,
                Ok(pipeline) => {
                    let objects = p["pipelineObjects"].as_array().cloned().unwrap_or_default();
                    pipeline.set_pipeline_objects(&objects);
                    ok(&json!({"errored": false}))
                }
            }
        }
        "GetPipelineDefinition" => {
            let id = p["pipelineId"].as_str().unwrap_or("");
            match with_pipeline(st, id) {
                Err(resp) => resp,
                Ok(pl) => {
                    let objects: Vec<Value> = pl.objects.iter().map(|o| o.to_json()).collect();
                    ok(&json!({"pipelineObjects": objects}))
                }
            }
        }
        "DescribeObjects" => {
            let id = p["pipelineId"].as_str().unwrap_or("");
            match with_pipeline(st, id) {
                Err(resp) => resp,
                Ok(pl) => {
                    let wanted = p["objectIds"].as_array().cloned().unwrap_or_default();
                    let objects: Vec<Value> = pl
                        .objects
                        .iter()
                        .filter(|o| wanted.iter().any(|w| w == &json!(o.id)))
                        .map(|o| o.to_json())
                        .collect();
                    ok(
                        &json!({"hasMoreResults": false, "marker": Value::Null, "pipelineObjects": objects}),
                    )
                }
            }
        }
        "ActivatePipeline" | "DeactivatePipeline" => {
            let id = p["pipelineId"].as_str().unwrap_or("");
            match with_pipeline(st, id) {
                Err(resp) => resp,
                Ok(pl) => {
                    pl.status = if req.operation == "ActivatePipeline" {
                        "SCHEDULED"
                    } else {
                        "DEACTIVATING"
                    }
                    .to_string();
                    ok(&json!({}))
                }
            }
        }
        "AddTags" => {
            let id = p["pipelineId"].as_str().unwrap_or("");
            match with_pipeline(st, id) {
                Err(resp) => resp,
                Ok(pl) => {
                    let tags = p["tags"].as_array().cloned().unwrap_or_default();
                    pl.add_tags(&tags);
                    ok(&json!({}))
                }
            }
        }
        "RemoveTags" => {
            let id = p["pipelineId"].as_str().unwrap_or("");
            match with_pipeline(st, id) {
                Err(resp) => resp,
                Ok(pl) => {
                    let keys = p["tagKeys"].as_array().cloned().unwrap_or_default();
                    pl.remove_tags(&keys);
                    ok(&json!({}))
                }
            }
        }
        "ValidatePipelineDefinition" => {
            let id = p["pipelineId"].as_str().unwrap_or("");
            match with_pipeline(st, id) {
                Err(resp) => resp,
                Ok(_) => ok(&json!({
                    "errored": false,
                    "validationErrors": [],
                    "validationWarnings": [],
                })),
            }
        }
        "QueryObjects" => {
            let id = p["pipelineId"].as_str().unwrap_or("");
            match with_pipeline(st, id) {
                Err(resp) => resp,
                Ok(pl) => {
                    // Only the COMPONENT sphere exposes object ids; per moto
                    // the INSTANCE/ATTEMPT spheres stay empty.
                    let sphere = p["sphere"].as_str().unwrap_or("");
                    let ids: Vec<Value> = if sphere == "INSTANCE" || sphere == "ATTEMPT" {
                        Vec::new()
                    } else {
                        pl.objects.iter().map(|o| json!(o.id)).collect()
                    };
                    ok(&json!({"hasMoreResults": false, "ids": ids, "marker": Value::Null}))
                }
            }
        }
        "EvaluateExpression" => {
            let id = p["pipelineId"].as_str().unwrap_or("");
            match with_pipeline(st, id) {
                Err(resp) => resp,
                // Basic stub: the expression is echoed back unchanged.
                Ok(_) => ok(&json!({
                    "evaluatedExpression": p["expression"].as_str().unwrap_or(""),
                })),
            }
        }
        "SetStatus" => {
            let id = p["pipelineId"].as_str().unwrap_or("");
            match with_pipeline(st, id) {
                Err(resp) => resp,
                Ok(pl) => {
                    let wanted = p["objectIds"].as_array().cloned().unwrap_or_default();
                    let status = json!(p["status"].as_str().unwrap_or(""));
                    for obj in pl.objects.iter_mut() {
                        if !wanted.iter().any(|w| w == &json!(obj.id)) {
                            continue;
                        }
                        if let Some(fields) = obj.fields.as_array_mut() {
                            match fields.iter_mut().find(|f| f["key"] == json!("@status")) {
                                Some(field) => field["stringValue"] = status.clone(),
                                None => {
                                    fields.push(json!({"key": "@status", "stringValue": status}))
                                }
                            }
                        }
                    }
                    ok(&json!({}))
                }
            }
        }
        "SetTaskStatus" => ok(&json!({})),
        "ReportTaskProgress" => ok(&json!({"canceled": false})),
        "ReportTaskRunnerHeartbeat" => ok(&json!({"terminate": false})),
        "PollForTask" => ok(&json!({"taskObject": {}})),
        _ => crate::proto::json_error(
            501,
            "NotImplemented",
            &format!("The {} action has not been implemented", req.operation),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn req(op: &str) -> Request {
        Request {
            service: "datapipeline".into(),
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

    /// Run one operation against fresh state, returning the parsed body.
    fn run(st: &mut State, op: &str, body: Value) -> Value {
        let mut r = req(op);
        r.body = bytes::Bytes::from(serde_json::to_vec(&body).unwrap());
        let resp = handle(st, &body, &r);
        serde_json::from_slice(&resp.body).unwrap()
    }

    /// Create a tagged definition region and return its pipeline id.
    fn create_pipeline(st: &mut State, name: &str) -> String {
        run(
            st,
            "CreatePipeline",
            json!({"name": name, "uniqueId": format!("{name}-unique")}),
        )["pipelineId"]
            .as_str()
            .unwrap()
            .to_string()
    }

    fn definition_objects() -> Value {
        json!([{
            "id": "Default",
            "name": "Default",
            "fields": [{"key": "type", "stringValue": "Default"}],
        }])
    }

    #[test]
    fn create_list_describe_activate_delete_roundtrip() {
        let slot = state();
        let mut st = slot.write();
        let b = st.entry((123456789012, "us-east-1".into())).or_default();
        let id = create_pipeline(b, "dp-wire-svc");

        let listed = run(b, "ListPipelines", json!({}));
        assert!(listed["pipelineIdList"].as_array().unwrap().len() > 0);
        assert!(listed["pipelineIdList"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["id"] == json!(id)));
        assert!(listed.get("hasMoreResults").is_some());

        let described = run(b, "DescribePipelines", json!({"pipelineIds": [id]}));
        let descs = described["pipelineDescriptionList"].as_array().unwrap();
        assert_eq!(descs.len(), 1);
        assert_eq!(descs[0]["pipelineId"], json!(id));
        assert_eq!(descs[0]["name"], json!("dp-wire-svc"));
        // The state field starts PENDING and flips on ActivatePipeline.
        assert_eq!(descs[0]["fields"][0]["key"], json!("@pipelineState"));
        assert_eq!(descs[0]["fields"][0]["stringValue"], json!("PENDING"));

        assert!(run(b, "ActivatePipeline", json!({"pipelineId": id}))
            .get("pipelineId")
            .is_none());
        let described = run(b, "DescribePipelines", json!({"pipelineIds": [id]}));
        assert_eq!(
            described["pipelineDescriptionList"][0]["fields"][0]["stringValue"],
            json!("SCHEDULED")
        );
        assert_eq!(
            run(b, "DeactivatePipeline", json!({"pipelineId": id})),
            json!({})
        );

        assert_eq!(
            run(b, "DeletePipeline", json!({"pipelineId": id})),
            json!({})
        );
        let listed = run(b, "ListPipelines", json!({}));
        assert!(!listed["pipelineIdList"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["id"] == json!(id)));
    }

    #[test]
    fn definition_put_get_and_describe_objects() {
        let slot = state();
        let mut st = slot.write();
        let b = st.entry((123456789012, "us-east-1".into())).or_default();
        let id = create_pipeline(b, "dp-definition");

        let put = run(
            b,
            "PutPipelineDefinition",
            json!({"pipelineId": id, "pipelineObjects": definition_objects()}),
        );
        assert_eq!(put["errored"], json!(false));

        let got = run(b, "GetPipelineDefinition", json!({"pipelineId": id}));
        let objs = got["pipelineObjects"].as_array().unwrap();
        assert_eq!(objs.len(), 1);
        assert_eq!(objs[0]["id"], json!("Default"));
        assert_eq!(objs[0]["fields"][0]["key"], json!("type"));
        assert_eq!(objs[0]["fields"][0]["stringValue"], json!("Default"));

        let described = run(
            b,
            "DescribeObjects",
            json!({"pipelineId": id, "objectIds": ["Default"]}),
        );
        assert!(described.get("hasMoreResults").is_some());
        assert_eq!(described["pipelineObjects"][0]["name"], json!("Default"));
    }

    #[test]
    fn tags_add_then_remove_surface_in_describe() {
        let slot = state();
        let mut st = slot.write();
        let b = st.entry((123456789012, "us-east-1".into())).or_default();
        let id = create_pipeline(b, "dp-tags");

        assert_eq!(
            run(
                b,
                "AddTags",
                json!({"pipelineId": id, "tags": [{"key": "env", "value": "test"}]}),
            ),
            json!({})
        );
        let described = run(b, "DescribePipelines", json!({"pipelineIds": [id]}));
        let tags = described["pipelineDescriptionList"][0]["tags"].clone();
        assert_eq!(tags, json!([{"key": "env", "value": "test"}]));

        assert_eq!(
            run(
                b,
                "RemoveTags",
                json!({"pipelineId": id, "tagKeys": ["env"]})
            ),
            json!({})
        );
        let described = run(b, "DescribePipelines", json!({"pipelineIds": [id]}));
        assert_eq!(described["pipelineDescriptionList"][0]["tags"], json!([]));
    }

    #[test]
    fn task_runner_stubs_answer_plain_json() {
        let slot = state();
        let mut st = slot.write();
        let b = st.entry((123456789012, "us-east-1".into())).or_default();

        assert_eq!(
            run(b, "PollForTask", json!({"workerGroup": "wg"})),
            json!({"taskObject": {}})
        );
        assert_eq!(
            run(b, "ReportTaskProgress", json!({"taskId": "t1"})),
            json!({"canceled": false})
        );
        assert_eq!(
            run(
                b,
                "ReportTaskRunnerHeartbeat",
                json!({"taskrunnerId": "r1"})
            ),
            json!({"terminate": false})
        );
        assert_eq!(
            run(
                b,
                "SetTaskStatus",
                json!({"taskId": "t1", "taskStatus": "FINISHED"})
            ),
            json!({})
        );
    }

    #[test]
    fn query_evaluate_validate_and_set_status() {
        let slot = state();
        let mut st = slot.write();
        let b = st.entry((123456789012, "us-east-1".into())).or_default();
        let id = create_pipeline(b, "dp-query");

        run(
            b,
            "PutPipelineDefinition",
            json!({"pipelineId": id, "pipelineObjects": definition_objects()}),
        );
        let queried = run(
            b,
            "QueryObjects",
            json!({"pipelineId": id, "sphere": "COMPONENT"}),
        );
        assert_eq!(queried["ids"], json!(["Default"]));
        assert!(queried.get("hasMoreResults").is_some());
        // INSTANCE sphere lists no objects.
        let queried = run(
            b,
            "QueryObjects",
            json!({"pipelineId": id, "sphere": "INSTANCE"}),
        );
        assert_eq!(queried["ids"], json!([]));

        assert_eq!(
            run(
                b,
                "EvaluateExpression",
                json!({"pipelineId": id, "objectId": "Default", "expression": "#{node.name}"}),
            ),
            json!({"evaluatedExpression": "#{node.name}"})
        );

        let validated = run(
            b,
            "ValidatePipelineDefinition",
            json!({"pipelineId": id, "pipelineObjects": definition_objects()}),
        );
        assert_eq!(validated["errored"], json!(false));
        assert_eq!(validated["validationErrors"], json!([]));

        assert_eq!(
            run(
                b,
                "SetStatus",
                json!({"pipelineId": id, "objectIds": ["Default"], "status": "FINISHED"}),
            ),
            json!({})
        );
        let got = run(b, "GetPipelineDefinition", json!({"pipelineId": id}));
        let fields = &got["pipelineObjects"][0]["fields"];
        assert_eq!(
            fields[fields.as_array().unwrap().len() - 1]["key"],
            json!("@status")
        );
        assert_eq!(
            fields[fields.as_array().unwrap().len() - 1]["stringValue"],
            json!("FINISHED")
        );
    }

    #[test]
    fn unknown_pipeline_id_rejects_cleanly() {
        let slot = state();
        let mut st = slot.write();
        let b = st.entry((123456789012, "us-east-1".into())).or_default();
        let mut r = req("ActivatePipeline");
        r.body =
            bytes::Bytes::from(serde_json::to_vec(&json!({"pipelineId": "df-nosuch"})).unwrap());
        let resp = handle(b, &json!({"pipelineId": "df-nosuch"}), &r);
        assert_eq!(resp.status, 400);
        assert!(std::str::from_utf8(&resp.body)
            .unwrap()
            .contains("PipelineNotFoundException"));
    }
}
