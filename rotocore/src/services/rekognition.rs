//! AWS Rekognition backend — port of the py gateway's native rekognition
//! provider (collections, faces, users, projects, stream processors, video
//! analysis start/get pairs, liveness, tagging) plus moto-shape mirrors for
//! the ops the py provider forwarded to moto (CompareFaces/DetectLabels/
//! DetectText/DetectCustomLabels fixtures, FaceSearch/TextDetection job
//! stubs, dataset + project-version stores, stream processor start/stop).
//!
//! The MediaAnalysis/ProjectPolicy/UpdateDatasetEntries/DistributeEntries
//! ops stay 501 like moto (they fail identically on the py ground truth).
//! Errors carry the spec's JSON `Message` capitalization the provider used.

use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Map, Value};
use std::collections::HashMap;

static SLOT: Lazy<store::Slot<RekState>> = Lazy::new(|| store::slot::<RekState>("rekognition"));

#[derive(Default)]
struct RekState {
    collections: HashMap<String, Collection>,
    faces: HashMap<String, HashMap<String, Value>>,
    users: HashMap<String, HashMap<String, Value>>,
    projects: HashMap<String, Project>,
    project_versions: HashMap<String, Value>,
    datasets: HashMap<String, Value>,
    stream_processors: HashMap<String, StreamProcessor>,
    liveness_sessions: HashMap<String, Value>,
    video_jobs: HashMap<String, Value>,
    tags: HashMap<String, Map<String, Value>>,
    region: String,
    account: String,
}

struct Collection {
    id: String,
    arn: String,
    face_model_version: String,
    creation_timestamp: f64,
}

struct StreamProcessor {
    name: String,
    arn: String,
    status: String,
    creation_timestamp: f64,
    input: Value,
    output: Value,
    role_arn: Value,
    settings: Value,
    regions_of_interest: Option<Value>,
    last_update_timestamp: Option<Value>,
}

struct Project {
    name: String,
    arn: String,
    creation_timestamp: f64,
    status: String,
}

fn now() -> f64 {
    crate::proto::now_unix()
}

fn err_not_found(message: &str) -> Response {
    let body = serde_json::to_vec(&json!({
        "__type": "ResourceNotFoundException",
        "Message": message,
    }))
    .unwrap_or_default();
    Response::json(400, body)
}

fn already_exists(message: &str) -> Response {
    let body = serde_json::to_vec(&json!({
        "__type": "ResourceAlreadyExistsException",
        "Message": message,
    }))
    .unwrap_or_default();
    Response::json(400, body)
}

fn in_use(message: &str) -> Response {
    let body = serde_json::to_vec(&json!({
        "__type": "ResourceInUseException",
        "Message": message,
    }))
    .unwrap_or_default();
    Response::json(400, body)
}

fn collection_arn(account: &str, region: &str, id: &str) -> String {
    format!("arn:aws:rekognition:{region}:{account}:collection/{id}")
}

fn params_of(req: &Request) -> Value {
    crate::proto::parse_json_body(&req.body)
}

pub fn dispatch(req: &Request) -> Response {
    let params = params_of(req);
    let mut guard = SLOT.write();
    let st = guard.entry((req.account, req.region.clone())).or_default();
    if st.region.is_empty() {
        st.region = req.region.clone();
        st.account = req.account.to_string();
    }
    match req.operation.as_str() {
        "CreateCollection" => st.create_collection(&params),
        "DescribeCollection" => st.describe_collection(&params),
        "ListCollections" => st.list_collections(&params),
        "DeleteCollection" => st.delete_collection(&params),
        "TagResource" => st.tag_resource(&params),
        "ListTagsForResource" => st.list_tags_for_resource(&params),
        "UntagResource" => st.untag_resource(&params),
        "IndexFaces" => st.index_faces(&params),
        "ListFaces" => st.list_faces(&params),
        "SearchFaces" => st.search_faces(&params),
        "SearchFacesByImage" => st.search_faces_by_image(&params),
        "DeleteFaces" => st.delete_faces(&params),
        "DetectFaces" => st.detect_faces(),
        "DetectModerationLabels" => st.detect_moderation_labels(),
        "DetectProtectiveEquipment" => st.detect_protective_equipment(),
        "RecognizeCelebrities" => st.recognize_celebrities(),
        "GetCelebrityInfo" => st.get_celebrity_info(&params),
        "CompareFaces" => st.compare_faces(),
        "DetectLabels" => st.detect_labels(),
        "DetectText" => st.detect_text(),
        "DetectCustomLabels" => st.detect_custom_labels(),
        "StartFaceDetection" => st.start_face_detection(),
        "GetFaceDetection" => st.get_face_detection(&params),
        "StartLabelDetection" => st.start_label_detection(),
        "GetLabelDetection" => st.get_label_detection(&params),
        "StartCelebrityRecognition" => st.start_celebrity_recognition(),
        "GetCelebrityRecognition" => st.get_celebrity_recognition(&params),
        "StartContentModeration" => st.start_content_moderation(),
        "GetContentModeration" => st.get_content_moderation(&params),
        "StartPersonTracking" => st.start_person_tracking(),
        "GetPersonTracking" => st.get_person_tracking(&params),
        "StartSegmentDetection" => st.start_segment_detection(),
        "GetSegmentDetection" => st.get_segment_detection(&params),
        "StartFaceSearch" => st.start_face_search(&params),
        "GetFaceSearch" => st.get_face_search(&params),
        "StartTextDetection" => st.start_text_detection(),
        "GetTextDetection" => st.get_text_detection(&params),
        "CreateProject" => st.create_project(&params),
        "DescribeProjects" => st.describe_projects(&params),
        "DeleteProject" => st.delete_project(&params),
        "CreateDataset" => st.create_dataset(&params),
        "DescribeDataset" => st.describe_dataset(&params),
        "ListDatasetEntries" => st.list_dataset_entries(&params),
        "ListDatasetLabels" => st.list_dataset_labels(&params),
        "DeleteDataset" => st.delete_dataset(&params),
        "CreateProjectVersion" => st.create_project_version(&params),
        "DescribeProjectVersions" => st.describe_project_versions(&params),
        "StartProjectVersion" => st.start_project_version(&params),
        "StopProjectVersion" => st.stop_project_version(&params),
        "DeleteProjectVersion" => st.delete_project_version(&params),
        "CreateStreamProcessor" => st.create_stream_processor(&params),
        "DescribeStreamProcessor" => st.describe_stream_processor(&params),
        "ListStreamProcessors" => st.list_stream_processors(&params),
        "DeleteStreamProcessor" => st.delete_stream_processor(&params),
        "UpdateStreamProcessor" => st.update_stream_processor(&params),
        "StartStreamProcessor" => st.start_stream_processor(&params),
        "StopStreamProcessor" => st.stop_stream_processor(&params),
        "CreateFaceLivenessSession" => st.create_liveness_session(),
        "GetFaceLivenessSessionResults" => st.get_liveness_results(&params),
        "CreateUser" => st.create_user(&params),
        "DeleteUser" => st.delete_user(&params),
        "ListUsers" => st.list_users(&params),
        "SearchUsers" => st.search_users(&params),
        "SearchUsersByImage" => st.search_users_by_image(&params),
        "AssociateFaces" => st.associate_faces(&params),
        "DisassociateFaces" => st.disassociate_faces(&params),
        _ => crate::proto::json_error(
            501,
            "NotImplemented",
            &format!("The {} action has not been implemented", req.operation),
        ),
    }
}

fn random_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn inline_json(s: &str) -> Value {
    serde_json::from_str(s).unwrap_or(Value::Null)
}

impl RekState {
    // ---- collections ------------------------------------------------------

    fn create_collection(&mut self, params: &Value) -> Response {
        let id = params
            .get("CollectionId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if self.collections.contains_key(&id) {
            return already_exists("A collection with the specified ID already exists.");
        }
        let arn = collection_arn(&self.account, &self.region, &id);
        self.collections.insert(
            id.clone(),
            Collection {
                id: id.clone(),
                arn: arn.clone(),
                face_model_version: "6.0".into(),
                creation_timestamp: now(),
            },
        );
        self.tags.insert(
            arn.clone(),
            params
                .get("Tags")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default(),
        );
        Response::json_status(
            200,
            &json!({"StatusCode": 200, "CollectionArn": arn, "FaceModelVersion": "6.0"}),
        )
    }

    fn collection_or(&self, id: &str) -> Result<&Collection, Response> {
        self.collections
            .get(id)
            .ok_or_else(|| err_not_found(&format!("The collection id: {id} does not exist")))
    }

    fn describe_collection(&self, params: &Value) -> Response {
        let id = params
            .get("CollectionId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let col = match self.collection_or(&id) {
            Ok(c) => c,
            Err(resp) => return resp,
        };
        Response::json_status(
            200,
            &json!({
                "FaceCount": self.faces.get(&id).map(|f| f.len()).unwrap_or(0),
                "FaceModelVersion": col.face_model_version,
                "CollectionARN": col.arn,
                "CreationTimestamp": col.creation_timestamp,
            }),
        )
    }

    fn list_collections(&self, params: &Value) -> Response {
        let mut ids: Vec<&String> = self.collections.keys().collect();
        ids.sort();
        let max = params
            .get("MaxResults")
            .and_then(Value::as_u64)
            .unwrap_or(1000) as usize;
        let start = params
            .get("NextToken")
            .and_then(Value::as_str)
            .and_then(|t| t.parse::<usize>().ok())
            .unwrap_or(0);
        let page: Vec<Value> = ids
            .iter()
            .skip(start)
            .take(max)
            .map(|id| json!(id.as_str()))
            .collect();
        let versions: Vec<Value> = page
            .iter()
            .map(|v| {
                json!(self
                    .collections
                    .get(v.as_str().unwrap_or(""))
                    .map(|c| c.face_model_version.clone())
                    .unwrap_or_default())
            })
            .collect();
        let end = start + page.len();
        let mut out = json!({"CollectionIds": page, "FaceModelVersions": versions});
        if end < ids.len() {
            out["NextToken"] = json!(end.to_string());
        }
        Response::json_status(200, &out)
    }

    fn delete_collection(&mut self, params: &Value) -> Response {
        let id = params
            .get("CollectionId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        match self.collection_or(&id) {
            Ok(col) => {
                let arn = col.arn.clone();
                self.collections.remove(&id);
                self.tags.remove(&arn);
                self.faces.remove(&id);
                self.users.remove(&id);
                Response::json_status(200, &json!({"StatusCode": 200}))
            }
            Err(resp) => resp,
        }
    }

    // ---- tags -------------------------------------------------------------

    fn resource_exists(&self, arn: &str) -> bool {
        self.collections.values().any(|c| c.arn == arn)
            || self.projects.values().any(|p| p.arn == arn)
            || self.stream_processors.values().any(|sp| sp.arn == arn)
    }

    fn tag_resource(&mut self, params: &Value) -> Response {
        let arn = params
            .get("ResourceArn")
            .and_then(Value::as_str)
            .unwrap_or("");
        if !self.resource_exists(arn) {
            return err_not_found("The resource with the specified ARN was not found.");
        }
        if let Some(tags) = params.get("Tags").and_then(Value::as_object) {
            let entry = self.tags.entry(arn.to_string()).or_default();
            for (k, v) in tags {
                entry.insert(k.clone(), v.clone());
            }
        }
        Response::json_status(200, &json!({}))
    }

    fn list_tags_for_resource(&self, params: &Value) -> Response {
        let arn = params
            .get("ResourceArn")
            .and_then(Value::as_str)
            .unwrap_or("");
        if !self.resource_exists(arn) {
            return err_not_found("The resource with the specified ARN was not found.");
        }
        let tags = self.tags.get(arn).cloned().unwrap_or_else(Map::new);
        Response::json_status(200, &json!({"Tags": Value::Object(tags)}))
    }

    fn untag_resource(&mut self, params: &Value) -> Response {
        let arn = params
            .get("ResourceArn")
            .and_then(Value::as_str)
            .unwrap_or("");
        if !self.resource_exists(arn) {
            return err_not_found("The resource with the specified ARN was not found.");
        }
        if let Some(keys) = params.get("TagKeys").and_then(Value::as_array) {
            if let Some(entry) = self.tags.get_mut(arn) {
                for key in keys {
                    if let Some(k) = key.as_str() {
                        entry.remove(k);
                    }
                }
            }
        }
        Response::json_status(200, &json!({}))
    }

    // ---- faces --------------------------------------------------------------

    fn index_faces(&mut self, params: &Value) -> Response {
        let id = params
            .get("CollectionId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if self.collection_or(&id).is_err() {
            return err_not_found(&format!("The collection id: {id} does not exist"));
        }
        let face_id = random_id();
        let external = params
            .get("ExternalImageId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let face = json!({
            "FaceId": face_id,
            "ImageId": random_id(),
            "ExternalImageId": external,
            "Confidence": 99.99,
            "BoundingBox": inline_json(r#"{"Width": 0.5, "Height": 0.6, "Left": 0.2, "Top": 0.1}"#),
        });
        let face_entry = face.clone();
        self.faces
            .entry(id.clone())
            .or_default()
            .insert(face_id, face_entry);
        Response::json_status(
            200,
            &json!({
                "FaceRecords": [{
                    "Face": face,
                    "FaceDetail": {
                        "BoundingBox": inline_json(r#"{"Width": 0.5, "Height": 0.6, "Left": 0.2, "Top": 0.1}"#),
                        "Confidence": 99.99,
                        "Landmarks": [
                            {"Type": "eyeLeft", "X": 0.3, "Y": 0.3},
                            {"Type": "eyeRight", "X": 0.5, "Y": 0.3},
                            {"Type": "nose", "X": 0.4, "Y": 0.5},
                        ],
                        "Quality": {"Brightness": 80.0, "Sharpness": 90.0},
                    },
                }],
                "FaceModelVersion": "6.0",
                "UnindexedFaces": [],
            }),
        )
    }

    fn list_faces(&self, params: &Value) -> Response {
        let id = params
            .get("CollectionId")
            .and_then(Value::as_str)
            .unwrap_or("");
        if self.collection_or(id).is_err() {
            return err_not_found(&format!("The collection id: {id} does not exist"));
        }
        let max = params
            .get("MaxResults")
            .and_then(Value::as_u64)
            .unwrap_or(1000) as usize;
        let faces: Vec<Value> = self
            .faces
            .get(id)
            .map(|f| f.values().cloned().collect())
            .unwrap_or_default();
        Response::json_status(
            200,
            &json!({"Faces": faces.into_iter().take(max).collect::<Vec<_>>(),
                    "FaceModelVersion": "6.0"}),
        )
    }

    fn search_faces(&self, params: &Value) -> Response {
        let id = params
            .get("CollectionId")
            .and_then(Value::as_str)
            .unwrap_or("");
        let face_id = params.get("FaceId").and_then(Value::as_str).unwrap_or("");
        if self.collection_or(id).is_err() {
            return err_not_found(&format!("The collection id: {id} does not exist"));
        }
        let matches: Vec<Value> = self
            .faces
            .get(id)
            .map(|faces| {
                faces
                    .iter()
                    .filter(|(fid, _)| fid.as_str() != face_id)
                    .map(|(_, face)| json!({"Similarity": 95.0, "Face": face}))
                    .collect()
            })
            .unwrap_or_default();
        Response::json_status(
            200,
            &json!({
                "SearchedFaceId": face_id,
                "FaceMatches": matches,
                "FaceModelVersion": "6.0",
            }),
        )
    }

    fn search_faces_by_image(&self, params: &Value) -> Response {
        let id = params
            .get("CollectionId")
            .and_then(Value::as_str)
            .unwrap_or("");
        if self.collection_or(id).is_err() {
            return err_not_found(&format!("The collection id: {id} does not exist"));
        }
        let matches: Vec<Value> = self
            .faces
            .get(id)
            .map(|faces| {
                faces
                    .values()
                    .map(|face| json!({"Similarity": 95.0, "Face": face}))
                    .collect()
            })
            .unwrap_or_default();
        Response::json_status(
            200,
            &json!({
                "SearchedFaceBoundingBox": inline_json(r#"{"Width": 0.5, "Height": 0.6, "Left": 0.2, "Top": 0.1}"#),
                "SearchedFaceConfidence": 99.99,
                "FaceMatches": matches,
                "FaceModelVersion": "6.0",
            }),
        )
    }

    fn delete_faces(&mut self, params: &Value) -> Response {
        let id = params
            .get("CollectionId")
            .and_then(Value::as_str)
            .unwrap_or("");
        if self.collection_or(id).is_err() {
            return err_not_found(&format!("The collection id: {id} does not exist"));
        }
        let mut deleted = Vec::new();
        if let Some(faces) = self.faces.get_mut(id) {
            for fid in params
                .get("FaceIds")
                .and_then(Value::as_array)
                .unwrap_or(&vec![])
            {
                if let Some(fid) = fid.as_str() {
                    if faces.remove(fid).is_some() {
                        deleted.push(json!(fid));
                    }
                }
            }
        }
        Response::json_status(200, &json!({"DeletedFaces": deleted}))
    }

    // ---- image analysis -------------------------------------------------------

    fn detect_faces(&self) -> Response {
        Response::json_status(
            200,
            &json!({"FaceDetails": [{
                "BoundingBox": inline_json(r#"{"Width": 0.5, "Height": 0.6, "Left": 0.2, "Top": 0.1}"#),
                "Confidence": 99.99,
                "Landmarks": [
                    {"Type": "eyeLeft", "X": 0.3, "Y": 0.3},
                    {"Type": "eyeRight", "X": 0.5, "Y": 0.3},
                    {"Type": "nose", "X": 0.4, "Y": 0.5},
                    {"Type": "mouthLeft", "X": 0.3, "Y": 0.7},
                    {"Type": "mouthRight", "X": 0.5, "Y": 0.7},
                ],
                "Quality": {"Brightness": 80.0, "Sharpness": 90.0},
                "Pose": {"Roll": 0.0, "Yaw": 0.0, "Pitch": 0.0},
            }]}),
        )
    }

    fn detect_moderation_labels(&self) -> Response {
        Response::json_status(
            200,
            &json!({"ModerationLabels": [], "ModerationModelVersion": "6.0"}),
        )
    }

    fn detect_protective_equipment(&self) -> Response {
        Response::json_status(
            200,
            &json!({"Persons": [{
                "Id": 0,
                "BoundingBox": inline_json(r#"{"Width": 0.5, "Height": 0.8, "Left": 0.2, "Top": 0.1}"#),
                "Confidence": 99.0,
                "BodyParts": [{"Name": "FACE", "Confidence": 99.0, "EquipmentDetections": []}],
            }],
            "ProtectiveEquipmentModelVersion": "1.0"}),
        )
    }

    fn recognize_celebrities(&self) -> Response {
        Response::json_status(
            200,
            &json!({
                "CelebrityFaces": [],
                "UnrecognizedFaces": [{
                    "BoundingBox": inline_json(r#"{"Width": 0.5, "Height": 0.6, "Left": 0.2, "Top": 0.1}"#),
                    "Confidence": 99.0,
                    "Landmarks": [
                        {"Type": "eyeLeft", "X": 0.3, "Y": 0.3},
                        {"Type": "eyeRight", "X": 0.5, "Y": 0.3},
                    ],
                }],
            }),
        )
    }

    fn get_celebrity_info(&self, params: &Value) -> Response {
        let id = params.get("Id").and_then(Value::as_str).unwrap_or("");
        Response::json_status(200, &json!({"Name": format!("Celebrity-{id}"), "Urls": []}))
    }

    fn compare_faces(&self) -> Response {
        Response::json_status(
            200,
            &json!({
                "FaceMatches": [{
                    "Similarity": 100.0,
                    "Face": moto_face_record(),
                }],
                "SourceImageFace": {
                    "BoundingBox": inline_json(r#"{"Width": 0.5521978139877319, "Top": 0.1203877404332161, "Left": 0.23626373708248138, "Height": 0.3126954436302185}"#),
                    "Confidence": 99.98751068115234,
                },
                "UnmatchedFaces": [],
                "FaceModelVersion": "6.0",
            }),
        )
    }

    fn detect_labels(&self) -> Response {
        Response::json_status(
            200,
            &json!({
                "Labels": [{
                    "Name": "Mobile Phone",
                    "Parents": [{"Name": "Phone"}],
                    "Categories": [{"Name": "Technology and Computing"}],
                    "Confidence": 99.9364013671875,
                    "Instances": [],
                }],
                "LabelModelVersion": "3.0",
            }),
        )
    }

    fn detect_text(&self) -> Response {
        Response::json_status(
            200,
            &json!({
                "TextDetections": [{
                    "Confidence": 99.35693359375,
                    "DetectedText": "IT'S",
                    "Geometry": {
                        "BoundingBox": inline_json(r#"{"Height": 0.09988046437501907, "Left": 0.6684935688972473, "Top": 0.18226495385169983, "Width": 0.1461552083492279}"#),
                    },
                    "Id": 0,
                    "Type": "LINE",
                }],
                "TextModelVersion": "3.1",
            }),
        )
    }

    fn detect_custom_labels(&self) -> Response {
        Response::json_status(
            200,
            &json!({"CustomLabels": [{
                "Name": "MyLogo",
                "Confidence": 77.7729721069336,
                "Geometry": {
                    "BoundingBox": inline_json(r#"{"Width": 0.198987677693367, "Height": 0.31296101212501526, "Left": 0.07924537360668182, "Top": 0.4037395715713501}"#),
                },
            }]}),
        )
    }

    // ---- video start/get pairs ----------------------------------------------

    fn video_metadata() -> Value {
        json!({
            "Codec": "h264",
            "DurationMillis": 5000,
            "Format": "QuickTime / MOV",
            "FrameRate": 29.97,
            "FrameHeight": 720,
            "FrameWidth": 1280,
        })
    }

    fn start_video_job(&mut self, job_type: &str, payload: Value) -> Response {
        let job_id = random_id();
        let mut record = json!({"JobType": job_type, "JobStatus": "SUCCEEDED"});
        record["VideoMetadata"] = Self::video_metadata();
        if let Some(map) = payload.as_object() {
            for (k, v) in map {
                record[k] = v.clone();
            }
        }
        self.video_jobs.insert(job_id.clone(), record);
        Response::json_status(200, &json!({"JobId": job_id}))
    }

    fn start_face_detection(&mut self) -> Response {
        self.start_video_job(
            "FaceDetection",
            json!({"Faces": [{
                "Timestamp": 0,
                "Face": {
                    "BoundingBox": inline_json(r#"{"Width": 0.5, "Height": 0.6, "Left": 0.2, "Top": 0.1}"#),
                    "Confidence": 99.0,
                },
            }]}),
        )
    }

    fn get_video(&self, params: &Value, keys: &[&str]) -> Response {
        let job_id = params.get("JobId").and_then(Value::as_str).unwrap_or("");
        let job = self.video_jobs.get(job_id).cloned().unwrap_or(json!({}));
        let mut out = json!({"JobStatus": "SUCCEEDED"});
        for key in keys {
            out[*key] = job.get(*key).cloned().unwrap_or(self.default_for(key));
        }
        Response::json_status(200, &out)
    }

    fn default_for(&self, key: &str) -> Value {
        match key {
            "Faces" => json!([]),
            "Labels" => json!([]),
            "Celebrities" => json!([]),
            "ModerationLabels" => json!([]),
            "Persons" => json!([]),
            "Segments" => json!([]),
            "VideoMetadata" => Self::video_metadata(),
            "SelectedSegmentTypes" => json!([
                {"Type": "TECHNICAL_CUE", "ModelVersion": "2.0"},
                {"Type": "SHOT", "ModelVersion": "2.0"},
            ]),
            "LabelModelVersion" => json!("3.0"),
            "ModerationModelVersion" => json!("6.0"),
            "TextModelVersion" => json!("3.1"),
            _ => json!(null),
        }
    }

    fn get_face_detection(&self, params: &Value) -> Response {
        self.get_video(params, &["Faces", "VideoMetadata"])
    }

    fn start_label_detection(&mut self) -> Response {
        self.start_video_job(
            "LabelDetection",
            json!({"Labels": [{
                "Timestamp": 0,
                "Label": {"Name": "Person", "Confidence": 98.0, "Instances": [], "Parents": []},
            }]}),
        )
    }

    fn get_label_detection(&self, params: &Value) -> Response {
        self.get_video(params, &["Labels", "VideoMetadata", "LabelModelVersion"])
    }

    fn start_celebrity_recognition(&mut self) -> Response {
        self.start_video_job("CelebrityRecognition", json!({"Celebrities": []}))
    }

    fn get_celebrity_recognition(&self, params: &Value) -> Response {
        self.get_video(params, &["Celebrities", "VideoMetadata"])
    }

    fn start_content_moderation(&mut self) -> Response {
        self.start_video_job("ContentModeration", json!({"ModerationLabels": []}))
    }

    fn get_content_moderation(&self, params: &Value) -> Response {
        self.get_video(
            params,
            &[
                "ModerationLabels",
                "VideoMetadata",
                "ModerationModelVersion",
            ],
        )
    }

    fn start_person_tracking(&mut self) -> Response {
        self.start_video_job(
            "PersonTracking",
            json!({"Persons": [{
                "Timestamp": 0,
                "Person": {"Index": 0,
                    "BoundingBox": inline_json(r#"{"Width": 0.5, "Height": 0.8, "Left": 0.2, "Top": 0.1}"#)},
            }]}),
        )
    }

    fn get_person_tracking(&self, params: &Value) -> Response {
        self.get_video(params, &["Persons", "VideoMetadata"])
    }

    fn start_segment_detection(&mut self) -> Response {
        let job_id = random_id();
        self.video_jobs.insert(
            job_id.clone(),
            json!({
                "JobType": "SegmentDetection",
                "JobStatus": "SUCCEEDED",
                "VideoMetadata": [Self::video_metadata()],
                "Segments": [],
                "SelectedSegmentTypes": [
                    {"Type": "TECHNICAL_CUE", "ModelVersion": "2.0"},
                    {"Type": "SHOT", "ModelVersion": "2.0"},
                ],
            }),
        );
        Response::json_status(200, &json!({"JobId": job_id}))
    }

    fn get_segment_detection(&self, params: &Value) -> Response {
        let job_id = params.get("JobId").and_then(Value::as_str).unwrap_or("");
        let job = self.video_jobs.get(job_id).cloned().unwrap_or(json!({}));
        Response::json_status(
            200,
            &json!({
                "JobStatus": "SUCCEEDED",
                // The spec's SegmentDetection video metadata is a LIST.
                "VideoMetadata": job.get("VideoMetadata").cloned().unwrap_or(json!([Self::video_metadata()])),
                "Segments": job.get("Segments").cloned().unwrap_or(json!([])),
                "SelectedSegmentTypes": job.get("SelectedSegmentTypes").cloned().unwrap_or(json!([
                    {"Type": "TECHNICAL_CUE", "ModelVersion": "2.0"},
                    {"Type": "SHOT", "ModelVersion": "2.0"},
                ])),
            }),
        )
    }

    fn start_face_search(&mut self, _params: &Value) -> Response {
        let job_id = random_id();
        self.video_jobs.insert(
            job_id.clone(),
            json!({"JobType": "FaceSearch", "JobStatus": "SUCCEEDED", "Persons": []}),
        );
        Response::json_status(200, &json!({"JobId": job_id}))
    }

    /// moto's GetFaceSearch ignores the JobId and returns canned values.
    fn get_face_search(&self, _params: &Value) -> Response {
        Response::json_status(
            200,
            &json!({
                "JobStatus": "SUCCEEDED",
                "StatusMessage": "",
                "VideoMetadata": Self::video_metadata(),
                "Persons": moto_persons(),
                "NextToken": "",
            }),
        )
    }

    fn start_text_detection(&mut self) -> Response {
        let job_id = random_id();
        self.video_jobs.insert(
            job_id.clone(),
            json!({"JobType": "TextDetection", "JobStatus": "SUCCEEDED", "TextDetections": moto_text_detections()}),
        );
        Response::json_status(200, &json!({"JobId": job_id}))
    }

    fn get_text_detection(&self, _params: &Value) -> Response {
        Response::json_status(
            200,
            &json!({
                "JobStatus": "SUCCEEDED",
                "StatusMessage": "",
                "VideoMetadata": Self::video_metadata(),
                "TextDetections": moto_text_detections(),
                "NextToken": "",
                "TextModelVersion": "3.1",
            }),
        )
    }

    // ---- projects ---------------------------------------------------------------

    fn create_project(&mut self, params: &Value) -> Response {
        let name = params
            .get("ProjectName")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if self.projects.contains_key(&name) {
            return in_use(&format!("The project {name} already exists."));
        }
        let arn = format!(
            "arn:aws:rekognition:{region}:{account}:project/{name}/{stamp}",
            region = self.region,
            account = self.account,
            stamp = now() as i64
        );
        self.projects.insert(
            name.clone(),
            Project {
                name: name.clone(),
                arn: arn.clone(),
                creation_timestamp: now(),
                status: "CREATED".into(),
            },
        );
        self.tags.insert(arn.clone(), Map::new());
        Response::json_status(200, &json!({"ProjectArn": arn}))
    }

    fn describe_projects(&self, params: &Value) -> Response {
        let max = params
            .get("MaxResults")
            .and_then(Value::as_u64)
            .unwrap_or(100) as usize;
        let projects: Vec<Value> = self
            .projects
            .values()
            .take(max)
            .map(|p| {
                json!({
                    "ProjectArn": p.arn,
                    "CreationTimestamp": p.creation_timestamp,
                    "Status": p.status,
                })
            })
            .collect();
        Response::json_status(200, &json!({"ProjectDescriptions": projects}))
    }

    fn delete_project(&mut self, params: &Value) -> Response {
        let arn = params
            .get("ProjectArn")
            .and_then(Value::as_str)
            .unwrap_or("");
        let found = self
            .projects
            .iter()
            .find(|(_, p)| p.arn == arn)
            .map(|(k, _)| k.clone());
        match found {
            Some(name) => {
                self.projects.remove(&name);
                self.tags.remove(arn);
                Response::json_status(200, &json!({"Status": "DELETING"}))
            }
            None => err_not_found(&format!("The project with ARN {arn} was not found.")),
        }
    }

    // ---- datasets ------------------------------------------------------------------

    fn create_dataset(&mut self, params: &Value) -> Response {
        let project_arn = params
            .get("ProjectArn")
            .and_then(Value::as_str)
            .unwrap_or("");
        let Some(project) = self.projects.values().find(|p| p.arn == project_arn) else {
            return err_not_found(&format!(
                "The project with ARN {project_arn} does not exist"
            ));
        };
        let dataset_type = params
            .get("DatasetType")
            .and_then(Value::as_str)
            .unwrap_or("TRAIN")
            .to_string();
        let arn = format!(
            "arn:aws:rekognition:{region}:{account}:project/{pname}/dataset/{dtype}/{rand}",
            region = self.region,
            account = self.account,
            pname = project.name,
            dtype = dataset_type.to_ascii_lowercase(),
            rand = &uuid::Uuid::new_v4().simple().to_string()[..8],
        );
        self.datasets.insert(
            arn.clone(),
            json!({
                "DatasetArn": arn,
                "ProjectArn": project_arn,
                "DatasetType": dataset_type,
                "Status": "CREATE_COMPLETE",
                "StatusMessage": "",
                "StatusMessageCode": "SUCCESS",
                "CreationTimestamp": now(),
                "LastUpdatedTimestamp": now(),
                "DatasetStats": {},
                "entries": [],
                "labels": [],
            }),
        );
        Response::json_status(200, &json!({"DatasetArn": arn}))
    }

    fn dataset_not_found(arn: &str) -> Response {
        err_not_found(&format!("The dataset with ARN {arn} does not exist"))
    }

    fn describe_dataset(&self, params: &Value) -> Response {
        let arn = params
            .get("DatasetArn")
            .and_then(Value::as_str)
            .unwrap_or("");
        match self.datasets.get(arn) {
            Some(ds) => Response::json_status(
                200,
                &json!({"DatasetDescription": {
                    "CreationTimestamp": ds["CreationTimestamp"],
                    "LastUpdatedTimestamp": ds["LastUpdatedTimestamp"],
                    "Status": ds["Status"],
                    "StatusMessage": ds["StatusMessage"],
                    "StatusMessageCode": ds["StatusMessageCode"],
                    "DatasetStats": ds["DatasetStats"],
                }}),
            ),
            None => Self::dataset_not_found(arn),
        }
    }

    fn list_dataset_entries(&self, params: &Value) -> Response {
        let arn = params
            .get("DatasetArn")
            .and_then(Value::as_str)
            .unwrap_or("");
        match self.datasets.get(arn) {
            Some(ds) => {
                let entries = ds.get("entries").cloned().unwrap_or(json!([]));
                Response::json_status(
                    200,
                    &json!({"DatasetEntries": entries.as_array()
                        .map(|list| {
                            list.iter()
                                .map(|e| json!(e.to_string()))
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default()}),
                )
            }
            None => Self::dataset_not_found(arn),
        }
    }

    fn list_dataset_labels(&self, params: &Value) -> Response {
        let arn = params
            .get("DatasetArn")
            .and_then(Value::as_str)
            .unwrap_or("");
        match self.datasets.get(arn) {
            Some(ds) => {
                let labels: Vec<Value> = ds
                    .get("labels")
                    .and_then(Value::as_array)
                    .map(|list| list.iter().map(|lbl| json!({"LabelName": lbl})).collect())
                    .unwrap_or_default();
                Response::json_status(200, &json!({"DatasetLabelDescriptions": labels}))
            }
            None => Self::dataset_not_found(arn),
        }
    }

    fn delete_dataset(&mut self, params: &Value) -> Response {
        let arn = params
            .get("DatasetArn")
            .and_then(Value::as_str)
            .unwrap_or("");
        if self.datasets.remove(arn).is_none() {
            return Self::dataset_not_found(arn);
        }
        Response::json_status(200, &json!({}))
    }

    // ---- project versions ---------------------------------------------------------------

    fn create_project_version(&mut self, params: &Value) -> Response {
        let project_arn = params
            .get("ProjectArn")
            .and_then(Value::as_str)
            .unwrap_or("");
        let Some(project) = self.projects.values().find(|p| p.arn == project_arn) else {
            return err_not_found(&format!(
                "The project with ARN {project_arn} does not exist"
            ));
        };
        let version_name = params
            .get("VersionName")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let dup = self.project_versions.values().any(|v| {
            v.get("ProjectArn").and_then(Value::as_str) == Some(project_arn)
                && v.get("VersionName").and_then(Value::as_str) == Some(version_name.as_str())
        });
        if dup {
            return already_exists(&format!(
                "A project version with name {version_name} already exists"
            ));
        }
        let version_arn = format!(
            "arn:aws:rekognition:{region}:{account}:project/{pname}/version/{vname}/{rand}",
            region = self.region,
            account = self.account,
            pname = project.name,
            vname = version_name,
            rand = now() as i64,
        );
        self.project_versions.insert(
            version_arn.clone(),
            json!({
                "ProjectArn": project_arn,
                "ProjectVersionArn": version_arn,
                "VersionName": version_name,
                "Status": "TRAINING",
                "CreationTimestamp": now(),
            }),
        );
        Response::json_status(200, &json!({"ProjectVersionArn": version_arn}))
    }

    fn version_not_found(arn: &str) -> Response {
        err_not_found(&format!(
            "The project version with ARN {arn} does not exist"
        ))
    }

    fn describe_project_versions(&self, params: &Value) -> Response {
        let project_arn = params
            .get("ProjectArn")
            .and_then(Value::as_str)
            .unwrap_or("");
        if !self.projects.values().any(|p| p.arn == project_arn) {
            return err_not_found(&format!(
                "The project with ARN {project_arn} does not exist"
            ));
        }
        let names: Vec<&str> = params
            .get("VersionNames")
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        let versions: Vec<Value> = self
            .project_versions
            .values()
            .filter(|v| v.get("ProjectArn").and_then(Value::as_str) == Some(project_arn))
            .filter(|v| {
                names.is_empty()
                    || names.contains(&v.get("VersionName").and_then(Value::as_str).unwrap_or(""))
            })
            .cloned()
            .collect();
        Response::json_status(200, &json!({"ProjectVersionDescriptions": versions}))
    }

    fn start_project_version(&mut self, params: &Value) -> Response {
        let arn = params
            .get("ProjectVersionArn")
            .and_then(Value::as_str)
            .unwrap_or("");
        let Some(version) = self.project_versions.get_mut(arn) else {
            return Self::version_not_found(arn);
        };
        version["Status"] = json!("RUNNING");
        if let Some(units) = params.get("MinInferenceUnits") {
            version["MinInferenceUnits"] = units.clone();
        }
        if let Some(units) = params.get("MaxInferenceUnits") {
            version["MaxInferenceUnits"] = units.clone();
        }
        Response::json_status(200, &json!({"Status": "STARTING"}))
    }

    fn stop_project_version(&mut self, params: &Value) -> Response {
        let arn = params
            .get("ProjectVersionArn")
            .and_then(Value::as_str)
            .unwrap_or("");
        let Some(version) = self.project_versions.get_mut(arn) else {
            return Self::version_not_found(arn);
        };
        version["Status"] = json!("STOPPED");
        Response::json_status(200, &json!({"Status": "STOPPING"}))
    }

    fn delete_project_version(&mut self, params: &Value) -> Response {
        let arn = params
            .get("ProjectVersionArn")
            .and_then(Value::as_str)
            .unwrap_or("");
        match self.project_versions.remove(arn) {
            Some(version) => {
                if version.get("Status").and_then(Value::as_str) == Some("RUNNING") {
                    self.project_versions.insert(arn.to_string(), version);
                    return in_use("The project version is running and cannot be deleted");
                }
                Response::json_status(200, &json!({"Status": "DELETING"}))
            }
            None => Self::version_not_found(arn),
        }
    }

    // ---- stream processors ----------------------------------------------------------------

    fn create_stream_processor(&mut self, params: &Value) -> Response {
        let name = params
            .get("Name")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if self.stream_processors.contains_key(&name) {
            return in_use(&format!("The stream processor {name} already exists."));
        }
        let arn = format!(
            "arn:aws:rekognition:{region}:{account}:streamprocessor/{name}",
            region = self.region,
            account = self.account
        );
        self.stream_processors.insert(
            name.clone(),
            StreamProcessor {
                name: name.clone(),
                arn: arn.clone(),
                status: "STOPPED".into(),
                creation_timestamp: now(),
                input: params.get("Input").cloned().unwrap_or(json!({})),
                output: params.get("Output").cloned().unwrap_or(json!({})),
                role_arn: params.get("RoleArn").cloned().unwrap_or(json!("")),
                settings: params.get("Settings").cloned().unwrap_or(json!({})),
                regions_of_interest: None,
                last_update_timestamp: None,
            },
        );
        self.tags.insert(arn.clone(), Map::new());
        Response::json_status(200, &json!({"StreamProcessorArn": arn}))
    }

    fn processor_not_found(name: &str) -> Response {
        err_not_found(&format!("The stream processor {name} was not found."))
    }

    fn describe_stream_processor(&self, params: &Value) -> Response {
        let name = params.get("Name").and_then(Value::as_str).unwrap_or("");
        match self.stream_processors.get(name) {
            Some(sp) => {
                let mut out = json!({
                    "Name": sp.name,
                    "StreamProcessorArn": sp.arn,
                    "Status": sp.status,
                    "CreationTimestamp": sp.creation_timestamp,
                    "Input": sp.input,
                    "Output": sp.output,
                    "RoleArn": sp.role_arn,
                    "Settings": sp.settings,
                });
                if let Some(roi) = &sp.regions_of_interest {
                    out["RegionsOfInterest"] = roi.clone();
                }
                if let Some(ts) = &sp.last_update_timestamp {
                    out["LastUpdateTimestamp"] = ts.clone();
                }
                Response::json_status(200, &out)
            }
            None => Self::processor_not_found(name),
        }
    }

    fn list_stream_processors(&self, params: &Value) -> Response {
        let max = params
            .get("MaxResults")
            .and_then(Value::as_u64)
            .unwrap_or(100) as usize;
        let processors: Vec<Value> = self
            .stream_processors
            .values()
            .take(max)
            .map(|sp| json!({"Name": sp.name, "Status": sp.status}))
            .collect();
        Response::json_status(200, &json!({"StreamProcessors": processors}))
    }

    fn delete_stream_processor(&mut self, params: &Value) -> Response {
        let name = params.get("Name").and_then(Value::as_str).unwrap_or("");
        match self.stream_processors.remove(name) {
            Some(sp) => {
                self.tags.remove(&sp.arn);
                Response::json_status(200, &json!({}))
            }
            None => Self::processor_not_found(name),
        }
    }

    fn update_stream_processor(&mut self, params: &Value) -> Response {
        let name = params.get("Name").and_then(Value::as_str).unwrap_or("");
        let Some(sp) = self.stream_processors.get_mut(name) else {
            return Self::processor_not_found(name);
        };
        if let Some(update) = params.get("SettingsForUpdate") {
            if let (Some(base), Some(extra)) = (sp.settings.as_object_mut(), update.as_object()) {
                for (k, v) in extra {
                    base.insert(k.clone(), v.clone());
                }
            }
        }
        if let Some(roi) = params.get("RegionsOfInterestForUpdate") {
            sp.regions_of_interest = Some(roi.clone());
        }
        if let Some(pref) = params.get("DataSharingPreferenceForUpdate") {
            sp.input = json!({"DataSharingPreference": pref.clone()});
        }
        if let Some(delete_list) = params.get("ParametersToDelete").and_then(Value::as_array) {
            for key in delete_list {
                if let Some(k) = key.as_str() {
                    if k == "RegionsOfInterest" {
                        sp.regions_of_interest = None;
                    } else if k == "DataSharingPreference" {
                        sp.input = json!({});
                    }
                }
            }
        }
        sp.last_update_timestamp = Some(json!(now()));
        Response::json_status(200, &json!({}))
    }

    fn start_stream_processor(&mut self, params: &Value) -> Response {
        let name = params.get("Name").and_then(Value::as_str).unwrap_or("");
        let Some(sp) = self.stream_processors.get_mut(name) else {
            return err_not_found(&format!(
                "The stream processor with name {name} does not exist"
            ));
        };
        if sp.status == "RUNNING" {
            return in_use(&format!("The stream processor {name} is already running"));
        }
        sp.status = "RUNNING".into();
        Response::json_status(200, &json!({"Status": "STARTING"}))
    }

    fn stop_stream_processor(&mut self, params: &Value) -> Response {
        let name = params.get("Name").and_then(Value::as_str).unwrap_or("");
        let Some(sp) = self.stream_processors.get_mut(name) else {
            return err_not_found(&format!(
                "The stream processor with name {name} does not exist"
            ));
        };
        sp.status = "STOPPED".into();
        sp.last_update_timestamp = Some(json!(now()));
        Response::json_status(200, &json!({"Status": "STOPPING"}))
    }

    // ---- liveness --------------------------------------------------------------------------

    fn create_liveness_session(&mut self) -> Response {
        let session_id = random_id();
        self.liveness_sessions.insert(
            session_id.clone(),
            json!({
                "SessionId": session_id.clone(),
                "Status": "CREATED",
                "Confidence": 99.5,
                "CreatedTimestamp": now(),
            }),
        );
        Response::json_status(200, &json!({"SessionId": session_id}))
    }

    fn get_liveness_results(&self, params: &Value) -> Response {
        let session_id = params
            .get("SessionId")
            .and_then(Value::as_str)
            .unwrap_or("");
        match self.liveness_sessions.get(session_id) {
            Some(session) => {
                let confidence = session.get("Confidence").cloned().unwrap_or(json!(99.5));
                Response::json_status(
                    200,
                    &json!({
                        "SessionId": session_id,
                        "Status": "SUCCEEDED",
                        "Confidence": confidence,
                    }),
                )
            }
            None => {
                let body = serde_json::to_vec(&json!({
                    "__type": "SessionNotFoundException",
                    "Message": "The session was not found.",
                }))
                .unwrap_or_default();
                Response::json(400, body)
            }
        }
    }

    // ---- users -------------------------------------------------------------------------------

    fn create_user(&mut self, params: &Value) -> Response {
        let id = params
            .get("CollectionId")
            .and_then(Value::as_str)
            .unwrap_or("");
        let user_id = params.get("UserId").and_then(Value::as_str).unwrap_or("");
        if self.collection_or(id).is_err() {
            return err_not_found(&format!("The collection id: {id} does not exist"));
        }
        let store = self.users.entry(id.to_string()).or_default();
        if store.contains_key(user_id) {
            return already_exists(&format!(
                "A user with the specified ID already exists. UserId: {user_id}"
            ));
        }
        store.insert(
            user_id.to_string(),
            json!({"UserId": user_id, "UserStatus": "ACTIVE"}),
        );
        Response::json_status(200, &json!({}))
    }

    fn delete_user(&mut self, params: &Value) -> Response {
        let id = params
            .get("CollectionId")
            .and_then(Value::as_str)
            .unwrap_or("");
        let user_id = params.get("UserId").and_then(Value::as_str).unwrap_or("");
        if self.collection_or(id).is_err() {
            return err_not_found(&format!("The collection id: {id} does not exist"));
        }
        let removed = self
            .users
            .get_mut(id)
            .and_then(|users| users.remove(user_id))
            .is_some();
        if removed {
            Response::json_status(200, &json!({}))
        } else {
            err_not_found(&format!(
                "The user id: {user_id} does not exist in the collection"
            ))
        }
    }

    fn list_users(&self, params: &Value) -> Response {
        let id = params
            .get("CollectionId")
            .and_then(Value::as_str)
            .unwrap_or("");
        if self.collection_or(id).is_err() {
            return err_not_found(&format!("The collection id: {id} does not exist"));
        }
        let max = params
            .get("MaxResults")
            .and_then(Value::as_u64)
            .unwrap_or(100) as usize;
        let page: Vec<Value> = self
            .users
            .get(id)
            .map(|users| {
                users
                    .values()
                    .take(max)
                    .map(|u| json!({"UserId": u["UserId"], "UserStatus": u["UserStatus"]}))
                    .collect()
            })
            .unwrap_or_default();
        Response::json_status(200, &json!({"Users": page}))
    }

    fn search_users(&self, params: &Value) -> Response {
        let id = params
            .get("CollectionId")
            .and_then(Value::as_str)
            .unwrap_or("");
        let user_id = params.get("UserId").and_then(Value::as_str).unwrap_or("");
        let face_id = params.get("FaceId").and_then(Value::as_str).unwrap_or("");
        let max = params
            .get("MaxUsers")
            .and_then(Value::as_u64)
            .unwrap_or(100) as usize;
        if self.collection_or(id).is_err() {
            return err_not_found(&format!("The collection id: {id} does not exist"));
        }
        let matches: Vec<Value> = self
            .users
            .get(id)
            .map(|users| {
                users
                    .iter()
                    .filter(|(uid, _)| user_id.is_empty() || uid.as_str() != user_id)
                    .take(max)
                    .map(|(_, user)| {
                        json!({
                            "User": {"UserId": user["UserId"], "UserStatus": user["UserStatus"]},
                            "Similarity": 99.99,
                            "MatchConfidence": 99.99,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        let mut result = json!({
            "UserMatches": matches,
            "FaceModelVersion": face_model_or_default(self, id),
        });
        if !user_id.is_empty() {
            result["SearchedUser"] = json!({"UserId": user_id});
        }
        if !face_id.is_empty() {
            result["SearchedFace"] = json!({"FaceId": face_id});
        }
        Response::json_status(200, &result)
    }

    fn search_users_by_image(&self, params: &Value) -> Response {
        let id = params
            .get("CollectionId")
            .and_then(Value::as_str)
            .unwrap_or("");
        if self.collection_or(id).is_err() {
            return err_not_found(&format!("The collection id: {id} does not exist"));
        }
        let max = params
            .get("MaxUsers")
            .and_then(Value::as_u64)
            .unwrap_or(100) as usize;
        let matches: Vec<Value> = self
            .users
            .get(id)
            .map(|users| {
                users
                    .values()
                    .take(max)
                    .map(|user| {
                        json!({
                            "User": {"UserId": user["UserId"], "UserStatus": user["UserStatus"]},
                            "Similarity": 99.99,
                            "MatchConfidence": 99.99,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Response::json_status(
            200,
            &json!({
                "UserMatches": matches,
                "FaceModelVersion": "6.0",
                "SearchedFace": {"FaceDetail": {
                    "BoundingBox": inline_json(r#"{"Width": 0.55, "Top": 0.12, "Left": 0.24, "Height": 0.31}"#),
                    "Confidence": 99.99,
                }},
            }),
        )
    }

    fn associate_faces(&mut self, params: &Value) -> Response {
        let id = params
            .get("CollectionId")
            .and_then(Value::as_str)
            .unwrap_or("");
        let user_id = params
            .get("UserId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if self.collection_or(id).is_err() {
            return err_not_found(&format!("The collection id: {id} does not exist"));
        }
        if !self
            .users
            .get(id)
            .map(|users| users.contains_key(&user_id))
            .unwrap_or(false)
        {
            return err_not_found(&format!(
                "The user id: {user_id} does not exist in the collection"
            ));
        }
        let collection_faces: Vec<String> = self
            .faces
            .get(id)
            .map(|faces| faces.keys().cloned().collect())
            .unwrap_or_default();
        let mut associated = Vec::new();
        let mut unsuccessful = Vec::new();
        for fid in params
            .get("FaceIds")
            .and_then(Value::as_array)
            .unwrap_or(&vec![])
        {
            let fare = fid.as_str().unwrap_or("");
            if collection_faces.iter().any(|f| f == fare) {
                associated.push(json!({"FaceId": fare}));
                // record on the user record
                let user = self.users.get_mut(id).unwrap().get_mut(&user_id).unwrap();
                let faces_list = user
                    .as_object_mut()
                    .unwrap()
                    .entry(String::from("AssociatedFaces"))
                    .or_insert(json!([]));
                if !faces_list
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|x| x.as_str() == Some(fare))
                {
                    faces_list.as_array_mut().unwrap().push(json!(fare));
                }
            } else {
                unsuccessful.push(json!({
                    "FaceId": fare,
                    "Reasons": ["FACE_NOT_FOUND"],
                    "Confidence": 0.0,
                }));
            }
        }
        Response::json_status(
            200,
            &json!({
                "AssociatedFaces": associated,
                "UnsuccessfulFaceAssociations": unsuccessful,
                "UserStatus": self
                    .users
                    .get(id)
                    .and_then(|users| users.get(&user_id))
                    .map(|u| u["UserStatus"].clone())
                    .unwrap_or(json!("ACTIVE")),
            }),
        )
    }

    fn disassociate_faces(&mut self, params: &Value) -> Response {
        let id = params
            .get("CollectionId")
            .and_then(Value::as_str)
            .unwrap_or("");
        let user_id = params
            .get("UserId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if self.collection_or(id).is_err() {
            return err_not_found(&format!("The collection id: {id} does not exist"));
        }
        let still_there = self
            .users
            .get(id)
            .map(|users| users.contains_key(&user_id))
            .unwrap_or(false);
        if !still_there {
            return err_not_found(&format!(
                "The user id: {user_id} does not exist in the collection"
            ));
        }
        let mut disassociated = Vec::new();
        let mut unsuccessful = Vec::new();
        for fid in params
            .get("FaceIds")
            .and_then(Value::as_array)
            .unwrap_or(&vec![])
        {
            let fare = fid.as_str().unwrap_or("").to_string();
            let user = self.users.get_mut(id).unwrap().get_mut(&user_id).unwrap();
            let faces_list = user
                .as_object_mut()
                .unwrap()
                .entry(String::from("AssociatedFaces"))
                .or_insert(json!([]));
            let had = faces_list
                .as_array()
                .map(|list| list.iter().any(|x| x.as_str() == Some(fare.as_str())))
                .unwrap_or(false);
            if had {
                let remaining: Vec<Value> = faces_list
                    .as_array()
                    .map(|list| {
                        list.iter()
                            .filter(|x| x.as_str() != Some(fare.as_str()))
                            .cloned()
                            .collect()
                    })
                    .unwrap_or_default();
                *faces_list = json!(remaining);
                disassociated.push(json!({"FaceId": fare}));
            } else {
                unsuccessful.push(json!({
                    "FaceId": fare,
                    "Reasons": ["ASSOCIATED_TO_A_DIFFERENT_USER"],
                    "Confidence": 0.0,
                }));
            }
        }
        let user_status = self
            .users
            .get(id)
            .unwrap()
            .get(&user_id)
            .map(|u| u["UserStatus"].clone())
            .unwrap_or(json!("ACTIVE"));
        Response::json_status(
            200,
            &json!({
                "DisassociatedFaces": disassociated,
                "UnsuccessfulFaceDisassociations": unsuccessful,
                "UserStatus": user_status,
            }),
        )
    }
}

fn face_model_or_default(state: &RekState, id: &str) -> String {
    state
        .collections
        .get(id)
        .map(|c| c.face_model_version.clone())
        .unwrap_or_else(|| "6.0".into())
}

fn moto_face_record() -> Value {
    json_inline(&format!(
        r#"{{"BoundingBox": {{"Width": 0.5521978139877319, "Top": 0.1203877404332161, "Left": 0.23626373708248138, "Height": 0.3126954436302185}}, "Confidence": 99.98751068115234, "FaceId": "{}"}}"#,
        "00000000-0000-0000-0000-000000000001"
    ))
}

fn moto_persons() -> Vec<Value> {
    vec![json!({
        "Timestamp": 0,
        "Person": {
            "Index": 0,
            "BoundingBox": {"Width": 0.42217350006103516, "Height": 0.9352386593818665, "Left": 0.31870967149734497, "Top": -0.0049947104416787624},
        },
    })]
}

fn moto_text_detections() -> Vec<Value> {
    vec![json!({
        "Confidence": 99.35693359375,
        "DetectedText": "IT'S",
        "Id": 0,
        "Type": "LINE",
    })]
}

fn json_inline(text: &str) -> Value {
    serde_json::from_str(text).unwrap_or(Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::HashMap;

    fn req(op: &str, body: Value) -> Request {
        Request {
            service: "rekognition".into(),
            operation: op.into(),
            method: "POST".into(),
            path: "/".into(),
            query_string: String::new(),
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
    fn collection_and_tag_lifecycle() {
        let created = json_body(dispatch(&req(
            "CreateCollection",
            json!({"CollectionId": "col1", "Tags": {"env": "test"}}),
        )));
        let arn = created["CollectionArn"].as_str().unwrap().to_string();
        assert!(arn.contains("collection/col1"));

        let describe = json_body(dispatch(&req(
            "DescribeCollection",
            json!({"CollectionId": "col1"}),
        )));
        assert_eq!(describe["FaceCount"], 0);

        let tags = json_body(dispatch(&req(
            "ListTagsForResource",
            json!({"ResourceArn": arn}),
        )));
        assert_eq!(tags["Tags"]["env"], "test");

        dispatch(&req(
            "UntagResource",
            json!({"ResourceArn": arn, "TagKeys": ["env"]}),
        ));
        let after = json_body(dispatch(&req(
            "ListTagsForResource",
            json!({"ResourceArn": arn}),
        )));
        assert!(after["Tags"].as_object().unwrap().is_empty());

        let listed = json_body(dispatch(&req("ListCollections", json!({}))));
        assert!(listed["CollectionIds"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c == "col1"));

        let deleted = json_body(dispatch(&req(
            "DeleteCollection",
            json!({"CollectionId": "col1"}),
        )));
        assert_eq!(deleted["StatusCode"], 200);
    }

    #[test]
    fn faces_and_users() {
        dispatch(&req("CreateCollection", json!({"CollectionId": "col2"})));
        let indexed = json_body(dispatch(&req(
            "IndexFaces",
            json!({"CollectionId": "col2", "ExternalImageId": "alice"}),
        )));
        assert_eq!(indexed["FaceRecords"].as_array().unwrap().len(), 1);
        let face_id = indexed["FaceRecords"][0]["Face"]["FaceId"]
            .as_str()
            .unwrap()
            .to_string();

        let faces = json_body(dispatch(&req("ListFaces", json!({"CollectionId": "col2"}))));
        assert_eq!(faces["Faces"].as_array().unwrap().len(), 1);

        let created = dispatch(&req(
            "CreateUser",
            json!({"CollectionId": "col2", "UserId": "u1"}),
        ));
        assert_eq!(created.status, 200);
        let assoc = json_body(dispatch(&req(
            "AssociateFaces",
            json!({"CollectionId": "col2", "UserId": "u1", "FaceIds": [face_id, "missing-face"]}),
        )));
        assert_eq!(assoc["AssociatedFaces"].as_array().unwrap().len(), 1);
        assert_eq!(
            assoc["UnsuccessfulFaceAssociations"][0]["Reasons"][0],
            "FACE_NOT_FOUND"
        );

        let users = json_body(dispatch(&req("ListUsers", json!({"CollectionId": "col2"}))));
        assert_eq!(users["Users"][0]["UserId"], "u1");
    }

    #[test]
    fn video_pairs_and_projects() {
        let started = json_body(dispatch(&req("StartFaceDetection", json!({"Video": {}}))));
        assert!(started["JobId"].as_str().is_some());
        let got = json_body(dispatch(&req(
            "GetFaceDetection",
            json!({"JobId": started["JobId"]}),
        )));
        assert_eq!(got["JobStatus"], "SUCCEEDED");
        assert!(got["VideoMetadata"].is_object());

        let fake = json_body(dispatch(&req("GetFaceSearch", json!({"JobId": "fake"}))));
        assert_eq!(fake["JobStatus"], "SUCCEEDED");
        assert!(fake["Persons"].is_array());

        let project = json_body(dispatch(&req(
            "CreateProject",
            json!({"ProjectName": "proj1"}),
        )));
        assert!(project["ProjectArn"]
            .as_str()
            .unwrap()
            .contains("project/proj1/"));
        let missing = dispatch(&req(
            "CreateDataset",
            json!({"ProjectArn": "arn:aws:rekognition:us-east-1:123456789001:project/no/1", "DatasetType": "TRAIN"}),
        ));
        assert_eq!(missing.status, 400);
        let err: Value = serde_json::from_slice(&missing.body).unwrap();
        assert_eq!(err["__type"], "ResourceNotFoundException");

        let sp = json_body(dispatch(&req(
            "CreateStreamProcessor",
            json!({"Name": "sp1", "Input": {"KinesisVideoStream": {}}, "Output": {"KinesisDataStream": {}}, "RoleArn": "arn:role", "Settings": {}}),
        )));
        assert!(sp["StreamProcessorArn"]
            .as_str()
            .unwrap()
            .contains("streamprocessor/sp1"));

        let live = json_body(dispatch(&req("CreateFaceLivenessSession", json!({}))));
        let sid = live["SessionId"].as_str().unwrap().to_string();
        let results = json_body(dispatch(&req(
            "GetFaceLivenessSessionResults",
            json!({"SessionId": sid}),
        )));
        assert_eq!(results["Status"], "SUCCEEDED");

        let unimplemented = dispatch(&req("ListMediaAnalysisJobs", json!({})));
        assert_eq!(unimplemented.status, 501);
    }
}
