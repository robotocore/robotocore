//! Firehose operation handler (mirrors src/robotocore/services/firehose/provider.py).

use base64::Engine as _;
use parking_lot::RwLock;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};

use crate::models::{DeliveryStream, FirehoseState};
use crate::protocol::{AwsRequest, AwsResponse};

pub struct FirehoseHandler {
    state: RwLock<HashMap<(u64, String), FirehoseState>>,
}

impl FirehoseHandler {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(HashMap::new()),
        }
    }

    fn get_state(&self, account: u64, region: &str) -> FirehoseState {
        let mut states = self.state.write();
        states
            .entry((account, region.to_string()))
            .or_default()
            .clone()
    }

    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        let op = req.operation.as_str();
        match op {
            "CreateDeliveryStream" => self.create_stream(&req),
            "DeleteDeliveryStream" => self.delete_stream(&req),
            "DescribeDeliveryStream" => self.describe_stream(&req),
            "ListDeliveryStreams" => self.list_streams(&req),
            "PutRecord" => self.put_record(&req),
            "PutRecordBatch" => self.put_record_batch(&req),
            "UpdateDestination" => self.update_destination(&req),
            "StartDeliveryStreamEncryption" => self.start_encryption(&req),
            "StopDeliveryStreamEncryption" => self.stop_encryption(&req),
            "TagDeliveryStream" => self.tag_stream(&req),
            "UntagDeliveryStream" => self.untag_stream(&req),
            "ListTagsForDeliveryStream" => self.list_tags(&req),
            other => AwsResponse::error(
                400,
                "ValidationException",
                &format!("The operation {} is not implemented", other),
            ),
        }
    }

    fn create_stream(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("DeliveryStreamName")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        if name.is_empty() {
            return AwsResponse::error(
                400,
                "ValidationException",
                "DeliveryStreamName is required",
            );
        }
        let state = self.get_state(req.account, &req.region);
        if state.get(&name).is_some() {
            return AwsResponse::error(
                400,
                "ResourceInUseException",
                &format!("Stream {name} already exists"),
            );
        }
        let arn = format!(
            "arn:aws:firehose:{}:{}:deliverystream/{}",
            req.region, req.account, name
        );
        let stream_type = req
            .params
            .get("DeliveryStreamType")
            .and_then(|v| v.as_str())
            .unwrap_or("DirectPut")
            .to_string();
        let s3_config = req
            .params
            .get("ExtendedS3DestinationConfiguration")
            .or_else(|| req.params.get("S3DestinationConfiguration"))
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();
        let mut tags = BTreeMap::new();
        if let Some(initial) = req.params.get("Tags").and_then(|v| v.as_array()) {
            for tag in initial {
                if let (Some(k), Some(v)) =
                    (tag.get("Key").and_then(|v| v.as_str()), tag.get("Value"))
                {
                    tags.insert(k.to_string(), v.as_str().unwrap_or_default().to_string());
                }
            }
        }
        let stream = DeliveryStream {
            name: name.clone(),
            arn: arn.clone(),
            status: "ACTIVE".to_string(),
            stream_type,
            created: chrono::Utc::now().timestamp_millis() as f64 / 1000.0,
            version_id: 1,
            tags,
            s3_config,
            encryption: None,
        };
        state.put(stream);
        AwsResponse::json(200, json!({ "DeliveryStreamARN": arn }))
    }

    fn delete_stream(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("DeliveryStreamName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        match state.remove(name) {
            Some(_) => AwsResponse::json(200, json!({})),
            None => AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!("Stream {name} not found"),
            ),
        }
    }

    fn describe_stream(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("DeliveryStreamName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        match state.get(name) {
            Some(s) => {
                let mut destinations: Vec<Value> = Vec::new();
                if !s.s3_config.is_empty() {
                    let cfg = &s.s3_config;
                    let mut s3_desc = Map::new();
                    s3_desc.insert(
                        "BucketARN".into(),
                        json!(cfg
                            .get("BucketARN")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default()),
                    );
                    s3_desc.insert(
                        "Prefix".into(),
                        json!(cfg
                            .get("Prefix")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default()),
                    );
                    s3_desc.insert(
                        "RoleARN".into(),
                        json!(cfg
                            .get("RoleARN")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default()),
                    );
                    s3_desc.insert(
                        "BufferingHints".into(),
                        cfg.get("BufferingHints").cloned().unwrap_or(json!({})),
                    );
                    s3_desc.insert(
                        "CompressionFormat".into(),
                        cfg.get("CompressionFormat")
                            .and_then(|v| v.as_str())
                            .unwrap_or("UNCOMPRESSED")
                            .into(),
                    );
                    if let Some(eop) = cfg.get("ErrorOutputPrefix") {
                        s3_desc.insert("ErrorOutputPrefix".into(), eop.clone());
                    }
                    destinations.push(json!({
                        "DestinationId": "dest-1",
                        "ExtendedS3DestinationDescription": s3_desc
                    }));
                }

                let mut desc = json!({
                    "DeliveryStreamName": s.name,
                    "DeliveryStreamARN": s.arn,
                    "DeliveryStreamStatus": s.status,
                    "DeliveryStreamType": s.stream_type,
                    "VersionId": s.version_id.to_string(),
                    "Destinations": destinations,
                    "HasMoreDestinations": false,
                    "CreateTimestamp": s.created,
                });
                if let Some(enc) = &s.encryption {
                    desc["DeliveryStreamEncryptionConfiguration"] = enc.clone();
                }
                AwsResponse::json(200, json!({ "DeliveryStreamDescription": desc }))
            }
            None => AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!("Stream {name} not found"),
            ),
        }
    }

    fn list_streams(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let mut names = state.list_names();
        let limit = req
            .params
            .get("Limit")
            .and_then(|v| v.as_u64())
            .unwrap_or(100) as usize;
        if let Some(start) = req
            .params
            .get("ExclusiveStartDeliveryStreamName")
            .and_then(|v| v.as_str())
        {
            if let Some(pos) = names.iter().position(|n| n == start) {
                names.drain(..=pos);
            }
        }
        let has_more = names.len() > limit;
        names.truncate(limit);
        AwsResponse::json(
            200,
            json!({
                "DeliveryStreamNames": names,
                "HasMoreDeliveryStreams": has_more
            }),
        )
    }

    fn put_record(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("DeliveryStreamName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        if state.get(name).is_none() {
            return AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!("Stream {name} not found"),
            );
        }
        let data_b64 = req
            .params
            .get("Record")
            .and_then(|r| r.get("Data"))
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let _data = base64::engine::general_purpose::STANDARD
            .decode(data_b64)
            .unwrap_or_default();
        AwsResponse::json(
            200,
            json!({
                "RecordId": uuid::Uuid::new_v4().simple().to_string(),
                "Encrypted": false
            }),
        )
    }

    fn put_record_batch(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("DeliveryStreamName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let records: Vec<Value> = req
            .params
            .get("Records")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        if state.get(name).is_none() {
            return AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!("Stream {name} not found"),
            );
        }
        let mut responses = Vec::with_capacity(records.len());
        for rec in records {
            let data_b64 = rec.get("Data").and_then(|v| v.as_str()).unwrap_or_default();
            let _data = base64::engine::general_purpose::STANDARD
                .decode(data_b64)
                .unwrap_or_default();
            responses.push(json!({ "RecordId": uuid::Uuid::new_v4().simple().to_string() }));
        }
        AwsResponse::json(
            200,
            json!({
                "FailedPutCount": 0,
                "Encrypted": false,
                "RequestResponses": responses
            }),
        )
    }

    fn update_destination(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("DeliveryStreamName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let destination_id = req
            .params
            .get("DestinationId")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let mut s = match state.get(name) {
            Some(s) => s,
            None => {
                return AwsResponse::error(
                    400,
                    "ResourceNotFoundException",
                    &format!("Stream {name} not found"),
                )
            }
        };
        if destination_id.is_empty() {
            return AwsResponse::error(400, "ValidationException", "DestinationId is required");
        }
        if let Some(current_version) = req.params.get("CurrentDeliveryStreamVersionId") {
            let expected = s.version_id.to_string();
            let provided = match current_version {
                Value::String(v) => v.clone(),
                Value::Number(n) => n.to_string(),
                other => other.to_string(),
            };
            if provided != expected {
                return AwsResponse::error(
                    400,
                    "InvalidArgumentException",
                    &format!("Version mismatch: expected {expected}, got {provided}"),
                );
            }
        }
        let s3_update = req
            .params
            .get("ExtendedS3DestinationUpdate")
            .or_else(|| req.params.get("S3DestinationUpdate"))
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();
        for (field, value) in s3_update {
            match (
                value.as_object(),
                s.s3_config.get(&field).and_then(|v| v.as_object()),
            ) {
                (Some(upd), Some(existing)) => {
                    let mut merged = existing.clone();
                    for (k, v) in upd {
                        merged.insert(k.clone(), v.clone());
                    }
                    s.s3_config.insert(field, Value::Object(merged));
                }
                _ => {
                    s.s3_config.insert(field, value);
                }
            }
        }
        s.version_id += 1;
        state.put(s);
        AwsResponse::json(200, json!({}))
    }

    fn tag_stream(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("DeliveryStreamName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let mut s = match state.get(name) {
            Some(s) => s,
            None => {
                return AwsResponse::error(
                    400,
                    "ResourceNotFoundException",
                    &format!("Stream {name} not found"),
                )
            }
        };
        if let Some(tags) = req.params.get("Tags").and_then(|v| v.as_array()).cloned() {
            for tag in tags {
                if let Some(k) = tag.get("Key").and_then(|v| v.as_str()) {
                    s.tags.insert(
                        k.to_string(),
                        tag.get("Value")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default()
                            .to_string(),
                    );
                }
            }
        }
        state.put(s);
        AwsResponse::json(200, json!({}))
    }

    fn untag_stream(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("DeliveryStreamName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let mut s = match state.get(name) {
            Some(s) => s,
            None => {
                return AwsResponse::error(
                    400,
                    "ResourceNotFoundException",
                    &format!("Stream {name} not found"),
                )
            }
        };
        let keys: Vec<String> = req
            .params
            .get("TagKeys")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        for key in keys {
            s.tags.remove(&key);
        }
        state.put(s);
        AwsResponse::json(200, json!({}))
    }

    fn list_tags(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("DeliveryStreamName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let limit = req
            .params
            .get("Limit")
            .and_then(|v| v.as_u64())
            .unwrap_or(50) as usize;
        let state = self.get_state(req.account, &req.region);
        let s = match state.get(name) {
            Some(s) => s,
            None => {
                return AwsResponse::error(
                    400,
                    "ResourceNotFoundException",
                    &format!("Stream {name} not found"),
                )
            }
        };
        let mut all_tags: Vec<Value> = s
            .tags
            .iter()
            .map(|(k, v)| json!({"Key": k, "Value": v}))
            .collect();
        if let Some(start) = req
            .params
            .get("ExclusiveStartTagKey")
            .and_then(|v| v.as_str())
        {
            if let Some(pos) = all_tags.iter().position(|t| t["Key"] == json!(start)) {
                all_tags.drain(..=pos);
            }
        }
        let has_more = all_tags.len() > limit;
        all_tags.truncate(limit);
        AwsResponse::json(200, json!({ "Tags": all_tags, "HasMoreTags": has_more }))
    }

    fn start_encryption(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("DeliveryStreamName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let mut s = match state.get(name) {
            Some(s) => s,
            None => {
                return AwsResponse::error(
                    400,
                    "ResourceNotFoundException",
                    &format!("Stream {name} not found"),
                )
            }
        };
        let input = req
            .params
            .get("DeliveryStreamEncryptionConfigurationInput")
            .or_else(|| req.params.get("DeliveryStreamEncryptionInput"))
            .cloned()
            .unwrap_or(json!({}));
        s.encryption = Some(json!({
            "KeyType": input.get("KeyType").and_then(|v| v.as_str()).unwrap_or("AWS_OWNED_CMK"),
            "KeyARN": input.get("KeyARN").cloned().unwrap_or(Value::Null),
            "Status": "ENABLED"
        }));
        state.put(s);
        AwsResponse::json(200, json!({}))
    }

    fn stop_encryption(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("DeliveryStreamName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let mut s = match state.get(name) {
            Some(s) => s,
            None => {
                return AwsResponse::error(
                    400,
                    "ResourceNotFoundException",
                    &format!("Stream {name} not found"),
                )
            }
        };
        s.encryption = Some(json!({ "Status": "DISABLED" }));
        state.put(s);
        AwsResponse::json(200, json!({}))
    }
}

impl Default for FirehoseHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use serde_json::json;

    fn make_req(operation: &str, params: Value) -> AwsRequest {
        AwsRequest {
            service: "firehose".to_string(),
            operation: operation.to_string(),
            account: 123456789012,
            region: "us-east-1".to_string(),
            params,
            body: Bytes::new(),
        }
    }

    #[test]
    fn test_create_describe_and_update() {
        let handler = FirehoseHandler::new();
        let create = handler.handle(make_req(
            "CreateDeliveryStream",
            json!({
                "DeliveryStreamName": "test-stream",
                "ExtendedS3DestinationConfiguration": {
                    "BucketARN": "arn:aws:s3:::bkt",
                    "RoleARN": "arn:aws:iam::123456789012:role/r",
                    "Prefix": "data/",
                    "BufferingHints": {"SizeInMBs": 5, "IntervalInSeconds": 60},
                    "CompressionFormat": "GZIP"
                }
            }),
        ));
        assert_eq!(create.status, 200);
        assert!(create.body.contains("DeliveryStreamARN"));

        let desc = serde_json::from_str::<Value>(
            &handler
                .handle(make_req(
                    "DescribeDeliveryStream",
                    json!({"DeliveryStreamName": "test-stream"}),
                ))
                .body,
        )
        .unwrap();
        let d = &desc["DeliveryStreamDescription"];
        assert_eq!(d["DeliveryStreamType"], json!("DirectPut"));
        assert_eq!(d["VersionId"], json!("1"));
        let dest = &d["Destinations"][0];
        assert_eq!(dest["DestinationId"], json!("dest-1"));
        assert_eq!(
            dest["ExtendedS3DestinationDescription"]["CompressionFormat"],
            json!("GZIP")
        );
        assert_eq!(
            dest["ExtendedS3DestinationDescription"]["BufferingHints"]["SizeInMBs"],
            json!(5)
        );

        // Update destination bumps version and merges config
        let upd = handler.handle(make_req(
            "UpdateDestination",
            json!({
                "DeliveryStreamName": "test-stream",
                "CurrentDeliveryStreamVersionId": "1",
                "DestinationId": "dest-1",
                "ExtendedS3DestinationUpdate": {
                    "BufferingHints": {"SizeInMBs": 10, "IntervalInSeconds": 120}
                }
            }),
        ));
        assert_eq!(upd.status, 200);
        let desc2 = serde_json::from_str::<Value>(
            &handler
                .handle(make_req(
                    "DescribeDeliveryStream",
                    json!({"DeliveryStreamName": "test-stream"}),
                ))
                .body,
        )
        .unwrap();
        let d2 = &desc2["DeliveryStreamDescription"];
        assert_eq!(d2["VersionId"], json!("2"));
        assert_eq!(
            d2["Destinations"][0]["ExtendedS3DestinationDescription"]["BufferingHints"]
                ["SizeInMBs"],
            json!(10)
        );
        // Wrong version raises InvalidArgumentException
        let bad = handler.handle(make_req(
            "UpdateDestination",
            json!({
                "DeliveryStreamName": "test-stream",
                "CurrentDeliveryStreamVersionId": "999",
                "DestinationId": "dest-1",
                "ExtendedS3DestinationUpdate": {}
            }),
        ));
        assert!(bad.body.contains("InvalidArgumentException"));
    }

    #[test]
    fn test_tags_limit_and_exclusive_start() {
        let handler = FirehoseHandler::new();
        handler.handle(make_req(
            "CreateDeliveryStream",
            json!({"DeliveryStreamName": "tag-stream"}),
        ));
        for i in 0..5 {
            handler.handle(make_req(
                "TagDeliveryStream",
                json!({
                    "DeliveryStreamName": "tag-stream",
                    "Tags": [{"Key": format!("k{i}"), "Value": format!("v{i}")}]
                }),
            ));
        }
        let resp = serde_json::from_str::<Value>(
            &handler
                .handle(make_req(
                    "ListTagsForDeliveryStream",
                    json!({"DeliveryStreamName": "tag-stream", "Limit": 2}),
                ))
                .body,
        )
        .unwrap();
        assert_eq!(resp["Tags"].as_array().unwrap().len(), 2);
        assert_eq!(resp["HasMoreTags"], json!(true));
        let resp = serde_json::from_str::<Value>(
            &handler
                .handle(make_req(
                    "ListTagsForDeliveryStream",
                    json!({"DeliveryStreamName": "tag-stream", "ExclusiveStartTagKey": "k0"}),
                ))
                .body,
        )
        .unwrap();
        let keys: Vec<String> = resp["Tags"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["Key"].as_str().unwrap().to_string())
            .collect();
        assert!(!keys.contains(&"k0".to_string()));
    }

    #[test]
    fn test_list_pagination() {
        let handler = FirehoseHandler::new();
        for name in ["a-stream", "b-stream", "c-stream"] {
            handler.handle(make_req(
                "CreateDeliveryStream",
                json!({"DeliveryStreamName": name}),
            ));
        }
        let resp = serde_json::from_str::<Value>(
            &handler
                .handle(make_req("ListDeliveryStreams", json!({"Limit": 1})))
                .body,
        )
        .unwrap();
        assert_eq!(resp["DeliveryStreamNames"].as_array().unwrap().len(), 1);
        assert_eq!(resp["HasMoreDeliveryStreams"], json!(true));
        let resp = serde_json::from_str::<Value>(
            &handler
                .handle(make_req(
                    "ListDeliveryStreams",
                    json!({"ExclusiveStartDeliveryStreamName": "a-stream"}),
                ))
                .body,
        )
        .unwrap();
        let names: Vec<String> = resp["DeliveryStreamNames"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n.as_str().unwrap().to_string())
            .collect();
        assert!(!names.contains(&"a-stream".to_string()));
    }

    #[test]
    fn test_put_record_resources() {
        let handler = FirehoseHandler::new();
        handler.handle(make_req(
            "CreateDeliveryStream",
            json!({"DeliveryStreamName": "pr-stream"}),
        ));
        let resp = serde_json::from_str::<Value>(
            &handler
                .handle(make_req(
                    "PutRecord",
                    json!({"DeliveryStreamName": "pr-stream",
            "Record": {"Data": base64::engine::general_purpose::STANDARD.encode(b"hello\n")}}),
                ))
                .body,
        )
        .unwrap();
        assert!(resp["RecordId"].is_string());
        assert_eq!(resp["Encrypted"], json!(false));
        let resp = serde_json::from_str::<Value>(
            &handler
                .handle(make_req(
                    "PutRecordBatch",
                    json!({"DeliveryStreamName": "pr-stream", "Records": [
                        {"Data": ""}, {"Data": ""}
                    ]}),
                ))
                .body,
        )
        .unwrap();
        assert_eq!(resp["FailedPutCount"], json!(0));
        assert_eq!(resp["RequestResponses"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_encryption_roundtrip() {
        let handler = FirehoseHandler::new();
        handler.handle(make_req(
            "CreateDeliveryStream",
            json!({"DeliveryStreamName": "enc-stream"}),
        ));
        handler.handle(make_req(
            "StartDeliveryStreamEncryption",
            json!({
                "DeliveryStreamName": "enc-stream",
                "DeliveryStreamEncryptionConfigurationInput": {"KeyType": "AWS_OWNED_CMK"}
            }),
        ));
        let desc = serde_json::from_str::<Value>(
            &handler
                .handle(make_req(
                    "DescribeDeliveryStream",
                    json!({"DeliveryStreamName": "enc-stream"}),
                ))
                .body,
        )
        .unwrap();
        assert_eq!(
            desc["DeliveryStreamDescription"]["DeliveryStreamEncryptionConfiguration"]["KeyType"],
            json!("AWS_OWNED_CMK")
        );
        handler.handle(make_req(
            "StopDeliveryStreamEncryption",
            json!({"DeliveryStreamName": "enc-stream"}),
        ));
        let desc = serde_json::from_str::<Value>(
            &handler
                .handle(make_req(
                    "DescribeDeliveryStream",
                    json!({"DeliveryStreamName": "enc-stream"}),
                ))
                .body,
        )
        .unwrap();
        assert_eq!(
            desc["DeliveryStreamDescription"]["DeliveryStreamEncryptionConfiguration"]["Status"],
            json!("DISABLED")
        );
    }
}
