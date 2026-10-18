//! Kinesis operation handler (mirrors src/robotocore/services/kinesis/provider.py).

use base64::Engine as _;
use parking_lot::RwLock;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

use crate::models::{
    build_shards, format_sequence, parse_sequence, ConsumerInfo, KinesisState, KinesisStream, Shard,
};
use crate::protocol::{AwsRequest, AwsResponse};

pub struct KinesisHandler {
    state: RwLock<HashMap<(u64, String), KinesisState>>,
}

impl KinesisHandler {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(HashMap::new()),
        }
    }

    fn get_state(&self, account: u64, region: &str) -> KinesisState {
        let mut states = self.state.write();
        states
            .entry((account, region.to_string()))
            .or_default()
            .clone()
    }

    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        let op = req.operation.as_str();
        match op {
            "CreateStream" => self.create_stream(&req),
            "DeleteStream" => self.delete_stream(&req),
            "DescribeStream" => self.describe_stream(&req),
            "ListStreams" => self.list_streams(&req),
            "PutRecord" => self.put_record(&req),
            "PutRecords" => self.put_records(&req),
            "GetShardIterator" => self.get_shard_iterator(&req),
            "GetRecords" => self.get_records(&req),
            "UpdateShardingConfiguration" => self.update_sharding_config(&req),
            "MergeShards" => self.merge_shards(&req),
            "SplitShard" => self.split_shard(&req),
            "DescribeLimits" => self.describe_limits(&req),
            "DescribeStreamSummary" => self.describe_stream_summary(&req),
            "UpdateShardCount" => self.update_shard_count(&req),
            "ListShards" => self.list_shards(&req),
            "IncreaseStreamRetentionPeriod" | "IncreaseRetentionPeriod" => {
                self.increase_retention(&req)
            }
            "DecreaseStreamRetentionPeriod" | "DecreaseRetentionPeriod" => {
                self.decrease_retention(&req)
            }
            "AddTagsToStream" => self.add_tags(&req),
            "RemoveTagsFromStream" => self.remove_tags(&req),
            "ListTagsForStream" => self.list_tags_for_stream(&req),
            "TagResource" => self.tag_resource(&req),
            "UntagResource" => self.untag_resource(&req),
            "ListTagsForResource" => self.list_tags_for_resource(&req),
            "UpdateStreamMode" => self.update_stream_mode(&req),
            "StartStreamEncryption" => self.start_stream_encryption(&req),
            "StopStreamEncryption" => self.stop_stream_encryption(&req),
            "DescribeStreamEncryption" => self.describe_stream_encryption(&req),
            "EnableEnhancedMonitoring" => self.enable_enhanced_monitoring(&req),
            "DisableEnhancedMonitoring" => self.disable_enhanced_monitoring(&req),
            "RegisterStreamConsumer" => self.register_stream_consumer(&req),
            "DescribeStreamConsumer" => self.describe_stream_consumer(&req),
            "ListStreamConsumers" => self.list_stream_consumers(&req),
            "DeregisterStreamConsumer" => self.deregister_stream_consumer(&req),
            "PutResourcePolicy" => self.put_resource_policy(&req),
            "GetResourcePolicy" => self.get_resource_policy(&req),
            "DeleteResourcePolicy" => self.delete_resource_policy(&req),
            "SubscribeToShard" => self.subscribe_to_shard(&req),
            "DescribeAccountSettings" => self.describe_account_settings(&req),
            "UpdateAccountSettings" => self.update_account_settings(&req),
            "UpdateMaxRecordSize" => self.update_max_record_size(&req),
            "UpdateStreamWarmThroughput" => self.update_stream_warm_throughput(&req),
            other => AwsResponse::error(
                400,
                "ValidationException",
                &format!("The operation {} is not implemented", other),
            ),
        }
    }

    // --- Stream CRUD ---

    fn create_stream(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("StreamName")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        if name.is_empty() {
            return AwsResponse::error(400, "ValidationException", "StreamName is required");
        }
        let shard_count = req
            .params
            .get("ShardCount")
            .and_then(|v| v.as_u64())
            .unwrap_or(1) as u32;
        let state = self.get_state(req.account, &req.region);
        if state.streams.read().contains_key(&name) {
            return AwsResponse::error(
                400,
                "ResourceInUseException",
                &format!(
                    "Stream {name} under account {} already exists.",
                    req.account
                ),
            );
        }
        let stream = Arc::new(KinesisStream::new(
            req.account,
            &req.region,
            name.clone(),
            shard_count,
        ));
        state.streams.write().insert(name, stream);
        AwsResponse::json(200, json!({}))
    }

    fn delete_stream(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("StreamName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let mut streams = state.streams.write();
        match streams.remove(name) {
            Some(stream) => {
                // Clean up resource policies for this stream and its consumers
                let prefix = format!("{}/consumer/", stream.arn);
                state
                    .resource_policies
                    .lock()
                    .retain(|arn, _| arn != &stream.arn && !arn.starts_with(&prefix));
                AwsResponse::json(200, json!({}))
            }
            None => AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!("Stream {name} under account {} not found.", req.account),
            ),
        }
    }

    fn stream_describe_value(stream: &KinesisStream) -> Value {
        json!({
            "StreamName": stream.name,
            "StreamARN": stream.arn,
            "StreamStatus": stream.status,
            "StreamModeDetails": { "StreamMode": "PROVISIONED" },
            "RetentionPeriodHours": *stream.retention_hours.read(),
            "EnhancedMonitoring": [ { "ShardLevelMetrics": [] } ],
            "EncryptionType": *stream.encryption_type.read(),
            "KeyId": Option::<String>::None,
            "StreamCreationTimestamp": stream.created,
        })
    }

    fn describe_stream(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("StreamName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let stream = match state.get_stream(name) {
            Some(s) => s,
            None => return not_found_stream(name, req.account),
        };

        let mut all_shards: Vec<Value> = stream
            .shards
            .read()
            .iter()
            .map(|s| shard_value(s))
            .collect();
        let limit = req.params.get("Limit").and_then(|v| v.as_u64());
        let exclusive_start = req
            .params
            .get("ExclusiveStartShardId")
            .and_then(|v| v.as_str());
        if let Some(start) = exclusive_start {
            if let Some(pos) = all_shards.iter().position(|s| s["ShardId"] == start) {
                all_shards.drain(..=pos);
            }
        }
        let has_more = limit.is_some_and(|l| all_shards.len() as u64 > l);
        if let Some(l) = limit {
            all_shards.truncate(l as usize);
        }

        let mut desc = Self::stream_describe_value(&stream);
        desc["Shards"] = json!(all_shards);
        desc["HasMoreShards"] = json!(has_more);
        AwsResponse::json(200, json!({ "StreamDescription": desc }))
    }

    fn describe_stream_summary(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("StreamName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        match state.get_stream(name) {
            Some(stream) => {
                let open_shard_count = stream.shards.read().len();
                let summary = Self::stream_describe_value(&stream);
                let summary = json!({
                    "StreamName": summary["StreamName"],
                    "StreamARN": summary["StreamARN"],
                    "StreamStatus": summary["StreamStatus"],
                    "StreamModeDetails": summary["StreamModeDetails"],
                    "RetentionPeriodHours": summary["RetentionPeriodHours"],
                    "StreamCreationTimestamp": stream.created,
                    "EnhancedMonitoring": [ { "ShardLevelMetrics": [] } ],
                    "OpenShardCount": open_shard_count,
                    "ConsumerCount": stream.consumers.read().len(),
                });
                AwsResponse::json(200, json!({ "StreamDescriptionSummary": summary }))
            }
            None => not_found_stream(name, req.account),
        }
    }

    fn describe_limits(&self, _req: &AwsRequest) -> AwsResponse {
        AwsResponse::json(
            200,
            json!({
                "ShardLimit": 6000,
                "OpenShardCount": 0,
                "OnDemandStreamCount": 0,
                "OnDemandStreamCountLimit": 50,
            }),
        )
    }

    fn describe_account_settings(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        AwsResponse::json(
            200,
            json!({
                "AccountSettings": {
                    "OpenShardCount": 0,
                    "StreamCount": state.streams.read().len(),
                }
            }),
        )
    }

    fn update_account_settings(&self, req: &AwsRequest) -> AwsResponse {
        let status = req
            .params
            .get("MinimumThroughputBillingCommitment")
            .and_then(|v| v.get("Status"))
            .and_then(|v| v.as_str())
            .unwrap_or("DISABLED");
        AwsResponse::json(
            200,
            json!({
                "MinimumThroughputBillingCommitment": { "Status": status }
            }),
        )
    }

    fn list_streams(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let mut names = state.list_stream_names();
        let limit = req
            .params
            .get("Limit")
            .and_then(|v| v.as_u64())
            .unwrap_or(100) as usize;
        if let Some(start) = req
            .params
            .get("ExclusiveStartStreamName")
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
                "StreamNames": names,
                "HasMoreStreams": has_more
            }),
        )
    }

    // --- Records ---

    fn put_record(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("StreamName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let stream = match state.get_stream(name) {
            Some(s) => s,
            None => return not_found_stream(name, req.account),
        };
        let partition_key = req
            .params
            .get("PartitionKey")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let data_b64 = req
            .params
            .get("Data")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let data = base64::engine::general_purpose::STANDARD
            .decode(data_b64)
            .unwrap_or_default();
        let explicit = req.params.get("ExplicitHashKey").and_then(|v| v.as_str());
        match stream.put_record(partition_key, data, explicit) {
            Some((shard, record)) => AwsResponse::json(
                200,
                json!({
                    "ShardId": shard.shard_id,
                    "SequenceNumber": record.sequence_number,
                    "EncryptionType": "NONE",
                }),
            ),
            None => AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!("Shard for key {partition_key} not found."),
            ),
        }
    }

    fn put_records(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("StreamName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let stream = match state.get_stream(name) {
            Some(s) => s,
            None => return not_found_stream(name, req.account),
        };
        let records_input = req
            .params
            .get("Records")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let mut result_records = Vec::with_capacity(records_input.len());
        for rec in records_input {
            let partition_key = rec
                .get("PartitionKey")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            let data_b64 = rec.get("Data").and_then(|v| v.as_str()).unwrap_or_default();
            let data = base64::engine::general_purpose::STANDARD
                .decode(data_b64)
                .unwrap_or_default();
            let explicit = rec.get("ExplicitHashKey").and_then(|v| v.as_str());
            if let Some((shard, record)) = stream.put_record(partition_key, data, explicit) {
                result_records.push(json!({
                    "ShardId": shard.shard_id,
                    "SequenceNumber": record.sequence_number
                }));
            }
        }
        AwsResponse::json(
            200,
            json!({
                "FailedRecordCount": 0,
                "EncryptionType": "NONE",
                "Records": result_records
            }),
        )
    }

    fn get_shard_iterator(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("StreamName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let stream = match state.get_stream(name) {
            Some(s) => s,
            None => return not_found_stream(name, req.account),
        };
        let shard_id = req
            .params
            .get("ShardId")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let shard = match stream.shard_by_id(shard_id) {
            Some(s) => s,
            None => {
                return AwsResponse::error(
                    400,
                    "ResourceNotFoundException",
                    &format!("Shard {shard_id} in stream {name} not found."),
                )
            }
        };

        let iterator_type = req
            .params
            .get("ShardIteratorType")
            .and_then(|v| v.as_str())
            .unwrap_or("TRIM_HORIZON")
            .to_string();
        let starting_seq = req
            .params
            .get("StartingSequenceNumber")
            .map(|v| v.as_str().map(str::to_string).unwrap_or_default())
            .unwrap_or_default();

        let (seq, timestamp) = match iterator_type.as_str() {
            "TRIM_HORIZON" => ("00000000000000000000".to_string(), None),
            "LATEST" => (shard.get_latest_sequence(), None),
            "AT_SEQUENCE_NUMBER" => {
                if starting_seq.is_empty() {
                    return invalid_argument(&format!(
                        "Invalid StartingSequenceNumber: {starting_seq}"
                    ));
                }
                (format_sequence(parse_sequence(&starting_seq)), None)
            }
            "AFTER_SEQUENCE_NUMBER" => {
                if starting_seq.is_empty() {
                    return invalid_argument(&format!(
                        "Invalid StartingSequenceNumber: {starting_seq}"
                    ));
                }
                (
                    format_sequence(parse_sequence(&starting_seq).saturating_add(1)),
                    None,
                )
            }
            "AT_TIMESTAMP" => (
                "00000000000000000000".to_string(),
                req.params.get("Timestamp").and_then(|v| v.as_f64()),
            ),
            other => {
                return invalid_argument(&format!("Invalid ShardIteratorType: {other}"));
            }
        };

        let token = encode_iterator(name, shard_id, &iterator_type, &seq, &req.region, timestamp);
        AwsResponse::json(200, json!({ "ShardIterator": token }))
    }

    fn get_records(&self, req: &AwsRequest) -> AwsResponse {
        let token = req
            .params
            .get("ShardIterator")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let limit = req
            .params
            .get("Limit")
            .and_then(|v| v.as_u64())
            .unwrap_or(10_000)
            .min(10_000) as usize;
        let info = match decode_iterator(token) {
            Ok(i) => i,
            Err(resp) => return resp,
        };
        let stream_name = info
            .get("stream")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let shard_id = info
            .get("shard")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let seq = info
            .get("seq")
            .and_then(|v| v.as_str())
            .unwrap_or("0")
            .to_string();
        let iter_type = info
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("AT_SEQUENCE_NUMBER")
            .to_string();
        let timestamp = info.get("timestamp").and_then(|v| v.as_f64());

        let state = self.get_state(req.account, &req.region);
        let stream = match state.get_stream(&stream_name) {
            Some(s) => s,
            None => {
                return AwsResponse::error(
                    400,
                    "ResourceNotFoundException",
                    &format!("Stream {stream_name} not found."),
                )
            }
        };
        let shard = match stream.shard_by_id(&shard_id) {
            Some(s) => s,
            None => {
                return AwsResponse::error(
                    400,
                    "ResourceNotFoundException",
                    &format!("Shard {shard_id} not found."),
                )
            }
        };

        let (records, next_seq) = shard.get_records(&seq, limit);
        let mut records = records;
        if iter_type == "AT_TIMESTAMP" {
            if let Some(ts) = timestamp {
                records.retain(|r| r.timestamp >= ts);
            }
        }
        let new_seq = next_seq.as_deref().unwrap_or(&seq);
        let next_token = encode_iterator(
            &stream_name,
            &shard_id,
            "AT_SEQUENCE_NUMBER",
            new_seq,
            &req.region,
            None,
        );

        AwsResponse::json(
            200,
            json!({
                "Records": records.iter().map(|r| r.to_output()).collect::<Vec<_>>(),
                "NextShardIterator": next_token,
                "MillisBehindLatest": 0
            }),
        )
    }

    // --- Resharding ---

    fn update_sharding_config(&self, _req: &AwsRequest) -> AwsResponse {
        AwsResponse::json(200, json!({}))
    }

    fn merge_shards(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("StreamName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let stream = match state.get_stream(name) {
            Some(s) => s,
            None => return not_found_stream(name, req.account),
        };
        let shard1_id = req
            .params
            .get("ShardToMerge")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let shard2_id = req
            .params
            .get("AdjacentShardToMerge")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let s1 = stream.shard_by_id(shard1_id);
        let s2 = stream.shard_by_id(shard2_id);
        let (s1, s2) = match (s1, s2) {
            (Some(a), Some(b)) => (a, b),
            _ => return AwsResponse::error(400, "ResourceNotFoundException", "Shard not found"),
        };
        let mut shards = stream.shards.write();
        // Same guarded allocation as split_shard: compute from the held
        // write guard to avoid a self-deadlock on re-acquire.
        let next = shards
            .iter()
            .filter_map(|s| s.shard_id.rsplit('-').next())
            .filter_map(|n| n.trim_start_matches('0').parse::<u32>().ok())
            .max()
            .unwrap_or(0)
            + 1;
        let merged = Arc::new(Shard::new(
            format!("shardId-{next:012}"),
            s1.hash_key_start.min(s2.hash_key_start),
            s1.hash_key_end.max(s2.hash_key_end),
        ));
        shards.retain(|s| s.shard_id != s1.shard_id && s.shard_id != s2.shard_id);
        shards.push(merged);
        AwsResponse::json(200, json!({}))
    }

    fn split_shard(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("StreamName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let stream = match state.get_stream(name) {
            Some(s) => s,
            None => return not_found_stream(name, req.account),
        };
        let shard_id = req
            .params
            .get("ShardToSplit")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let new_hash = req
            .params
            .get("NewStartingHashKey")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let target = match stream.shard_by_id(shard_id) {
            Some(s) => s,
            None => {
                return AwsResponse::error(
                    400,
                    "ResourceNotFoundException",
                    &format!("Shard {shard_id} not found"),
                )
            }
        };
        let mid = if new_hash.is_empty() {
            (target.hash_key_start + target.hash_key_end) / 2
        } else {
            new_hash.parse::<u128>().unwrap_or(0)
        };
        let mut shards = stream.shards.write();
        // Allocate the successor shard ids numerically greater than every
        // existing id (moto's scheme) — computed from the already-held write
        // guard: a second RwLock acquire here would self-deadlock.
        let next = shards
            .iter()
            .filter_map(|s| s.shard_id.rsplit('-').next())
            .filter_map(|n| n.trim_start_matches('0').parse::<u32>().ok())
            .max()
            .unwrap_or(0)
            + 1;
        let shard1 = Arc::new(Shard::new(
            format!("shardId-{next:012}"),
            target.hash_key_start,
            mid.saturating_sub(1),
        ));
        let shard2 = Arc::new(Shard::new(
            format!("shardId-{:012}", next + 1),
            mid,
            target.hash_key_end,
        ));
        shards.retain(|s| s.shard_id != target.shard_id);
        // Records already stored here land on the shard that covers their
        // partition hash after the split (moto re-fires records post-split).
        for r in target.take_records() {
            let h = KinesisStream::hash_key(&r.partition_key);
            if h < mid {
                shard1.put_record(&shard1.shard_id, r.partition_key.clone(), r.data.clone());
            } else {
                shard2.put_record(&shard2.shard_id, r.partition_key.clone(), r.data.clone());
            }
        }
        shards.push(shard1);
        shards.push(shard2);
        AwsResponse::json(200, json!({}))
    }

    fn update_shard_count(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("StreamName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let stream = match state.get_stream(name) {
            Some(s) => s,
            None => return not_found_stream(name, req.account),
        };
        let target = req.params.get("TargetShardCount").and_then(|v| v.as_u64());
        let target = match target {
            Some(t) => t,
            None => {
                return AwsResponse::error(
                    400,
                    "ValidationException",
                    "TargetShardCount is required",
                )
            }
        };
        let scaling_type = req
            .params
            .get("ScalingType")
            .and_then(|v| v.as_str())
            .unwrap_or("UNIFORM_SCALING");
        if scaling_type != "UNIFORM_SCALING" {
            return AwsResponse::error(400, "ValidationException",
                &format!("ScalingType {scaling_type} is not supported. Only UNIFORM_SCALING is supported."));
        }
        if target < 1 {
            return AwsResponse::error(
                400,
                "InvalidArgumentException",
                "TargetShardCount must be at least 1.",
            );
        }
        let current = stream.shards.read().len() as u64;
        let new_shards: Vec<Arc<Shard>> = build_shards(target as u32)
            .into_iter()
            .map(Arc::new)
            .collect();
        *stream.shards.write() = new_shards;
        AwsResponse::json(
            200,
            json!({
                "StreamName": stream.name,
                "StreamARN": stream.arn,
                "CurrentShardCount": current,
                "TargetShardCount": target
            }),
        )
    }

    fn list_shards(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let mut name = req
            .params
            .get("StreamName")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let mut start_idx: usize = 0;
        if let Some(token) = req.params.get("NextToken").and_then(|v| v.as_str()) {
            let mut parts = token.splitn(2, ':');
            if let (Some(n), Some(i)) = (parts.next(), parts.next()) {
                name = n.to_string();
                start_idx = i.parse().unwrap_or(0);
            }
        }
        let max_results = req
            .params
            .get("MaxResults")
            .and_then(|v| v.as_u64())
            .unwrap_or(10_000) as usize;
        let stream = match state.get_stream(&name) {
            Some(s) => s,
            None => return not_found_stream(&name, req.account),
        };
        let all_shards: Vec<Value> = stream
            .shards
            .read()
            .iter()
            .map(|s| shard_value(s))
            .collect();
        let paged: Vec<Value> = all_shards
            .into_iter()
            .skip(start_idx)
            .take(max_results)
            .collect();
        let taken = start_idx + paged.len();
        let total = stream.shards.read().len();
        let mut result = json!({ "Shards": paged });
        if taken < total {
            result["NextToken"] = json!(format!("{}:{}", name, taken));
        }
        AwsResponse::json(200, result)
    }

    // --- Retention ---

    fn increase_retention(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("StreamName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let stream = match state.get_stream(name) {
            Some(s) => s,
            None => return not_found_stream(name, req.account),
        };
        let hours = req
            .params
            .get("RetentionPeriodHours")
            .and_then(|v| v.as_u64())
            .unwrap_or(48) as u32;
        if hours < *stream.retention_hours.read() {
            return AwsResponse::error(
                400,
                "InvalidArgumentException",
                &format!(
                    "Retention period {hours} must be >= current {}.",
                    *stream.retention_hours.read()
                ),
            );
        }
        *stream.retention_hours.write() = hours;
        AwsResponse::json(200, json!({}))
    }

    fn decrease_retention(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("StreamName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let stream = match state.get_stream(name) {
            Some(s) => s,
            None => return not_found_stream(name, req.account),
        };
        let hours = req
            .params
            .get("RetentionPeriodHours")
            .and_then(|v| v.as_u64())
            .unwrap_or(24) as u32;
        if hours >= *stream.retention_hours.read() {
            return AwsResponse::error(
                400,
                "InvalidArgumentException",
                &format!(
                    "Retention period {hours} must be less than current {}.",
                    *stream.retention_hours.read()
                ),
            );
        }
        if hours < 24 {
            return AwsResponse::error(
                400,
                "InvalidArgumentException",
                "Minimum retention period is 24 hours.",
            );
        }
        *stream.retention_hours.write() = hours;
        AwsResponse::json(200, json!({}))
    }

    // --- Tags ---

    fn add_tags(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("StreamName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let stream = match state.get_stream(name) {
            Some(s) => s,
            None => return not_found_stream(name, req.account),
        };
        if let Some(map) = req.params.get("Tags").and_then(|v| v.as_object()) {
            let mut tags = stream.tags.write();
            for (k, v) in map {
                if let Some(val) = v.as_str() {
                    tags.insert(k.clone(), val.to_string());
                }
            }
        }
        AwsResponse::json(200, json!({}))
    }

    fn remove_tags(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("StreamName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let stream = match state.get_stream(name) {
            Some(s) => s,
            None => return not_found_stream(name, req.account),
        };
        let keys: Vec<String> = req
            .params
            .get("TagKeys")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|k| k.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let mut tags = stream.tags.write();
        for key in keys {
            tags.remove(&key);
        }
        AwsResponse::json(200, json!({}))
    }

    fn list_tags_for_stream(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("StreamName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let stream = match state.get_stream(name) {
            Some(s) => s,
            None => return not_found_stream(name, req.account),
        };
        let mut all_tags: Vec<Value> = stream
            .tags
            .read()
            .iter()
            .map(|(k, v)| json!({"Key": k, "Value": v}))
            .collect();
        if let Some(start) = req
            .params
            .get("ExclusiveStartTagKey")
            .and_then(|v| v.as_str())
        {
            all_tags.retain(|t| t["Key"].as_str().is_some_and(|k| k > start));
        }
        let limit = req
            .params
            .get("Limit")
            .and_then(|v| v.as_u64())
            .unwrap_or(10) as usize;
        let has_more = all_tags.len() > limit;
        all_tags.truncate(limit);
        AwsResponse::json(200, json!({ "Tags": all_tags, "HasMoreTags": has_more }))
    }

    fn tag_resource(&self, req: &AwsRequest) -> AwsResponse {
        let arn = req
            .params
            .get("ResourceARN")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let stream = match state.find_stream_by_arn(arn) {
            Some(s) => s,
            None => {
                return AwsResponse::error(
                    400,
                    "ResourceNotFoundException",
                    &format!("Resource {arn} not found."),
                )
            }
        };
        if let Some(map) = req.params.get("Tags").and_then(|v| v.as_object()) {
            let mut tags = stream.tags.write();
            for (k, v) in map {
                if let Some(val) = v.as_str() {
                    tags.insert(k.clone(), val.to_string());
                }
            }
        }
        AwsResponse::json(200, json!({}))
    }

    fn untag_resource(&self, req: &AwsRequest) -> AwsResponse {
        let arn = req
            .params
            .get("ResourceARN")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let stream = match state.find_stream_by_arn(arn) {
            Some(s) => s,
            None => {
                return AwsResponse::error(
                    400,
                    "ResourceNotFoundException",
                    &format!("Resource {arn} not found."),
                )
            }
        };
        let keys: Vec<String> = req
            .params
            .get("TagKeys")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|k| k.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let mut tags = stream.tags.write();
        for key in keys {
            tags.remove(&key);
        }
        AwsResponse::json(200, json!({}))
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> AwsResponse {
        let arn = req
            .params
            .get("ResourceARN")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let stream = match state.find_stream_by_arn(arn) {
            Some(s) => s,
            None => {
                return AwsResponse::error(
                    400,
                    "ResourceNotFoundException",
                    &format!("Resource {arn} not found."),
                )
            }
        };
        let tags: Vec<Value> = stream
            .tags
            .read()
            .iter()
            .map(|(k, v)| json!({"Key": k, "Value": v}))
            .collect();
        AwsResponse::json(200, json!({ "Tags": tags }))
    }

    // --- Encryption ---

    fn start_stream_encryption(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("StreamName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let stream = match state.get_stream(name) {
            Some(s) => s,
            None => {
                return AwsResponse::error(
                    400,
                    "ResourceNotFoundException",
                    &format!("Stream {name} not found"),
                )
            }
        };
        let encryption_type = req
            .params
            .get("EncryptionType")
            .and_then(|v| v.as_str())
            .unwrap_or("KMS");
        let key_id = req
            .params
            .get("KeyId")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        *stream.encryption_type.write() = encryption_type.to_string();
        *stream.key_id.write() = key_id.to_string();
        AwsResponse::json(200, json!({}))
    }

    fn stop_stream_encryption(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("StreamName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let stream = match state.get_stream(name) {
            Some(s) => s,
            None => {
                return AwsResponse::error(
                    400,
                    "ResourceNotFoundException",
                    &format!("Stream {name} not found"),
                )
            }
        };
        *stream.encryption_type.write() = "NONE".to_string();
        *stream.key_id.write() = String::new();
        AwsResponse::json(200, json!({}))
    }

    fn describe_stream_encryption(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("StreamName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        match state.get_stream(name) {
            Some(stream) => AwsResponse::json(
                200,
                json!({
                    "EncryptionType": *stream.encryption_type.read(),
                    "KeyId": *stream.key_id.read()
                }),
            ),
            None => AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!("Stream {name} not found"),
            ),
        }
    }

    // --- Enhanced monitoring ---

    fn enhanced_monitoring_stream(
        req: &AwsRequest,
        state: &KinesisState,
    ) -> Result<Arc<KinesisStream>, AwsResponse> {
        let name = req
            .params
            .get("StreamName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let arn = req
            .params
            .get("StreamARN")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let stream = if !name.is_empty() {
            state.get_stream(name)
        } else if !arn.is_empty() {
            state.find_stream_by_arn(arn)
        } else {
            None
        };
        stream.ok_or_else(|| not_found_stream(name, req.account))
    }

    fn enable_enhanced_monitoring(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let stream = match Self::enhanced_monitoring_stream(req, &state) {
            Ok(s) => s,
            Err(resp) => return resp,
        };
        let metrics: Vec<String> = req
            .params
            .get("ShardLevelMetrics")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|m| m.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let current = stream.shard_level_metrics.read().clone();
        let desired = if metrics.iter().any(|m| m == "ALL") {
            vec![
                "IncomingBytes".to_string(),
                "IncomingRecords".to_string(),
                "OutgoingBytes".to_string(),
                "OutgoingRecords".to_string(),
                "WriteProvisionedThroughputExceeded".to_string(),
                "ReadProvisionedThroughputExceeded".to_string(),
                "IteratorAgeMilliseconds".to_string(),
                "ALL".to_string(),
            ]
        } else {
            let mut set: Vec<String> = current.clone();
            for m in metrics {
                if !set.contains(&m) {
                    set.push(m);
                }
            }
            set
        };
        *stream.shard_level_metrics.write() = desired.clone();
        AwsResponse::json(
            200,
            json!({
                "StreamName": stream.name,
                "StreamARN": stream.arn,
                "CurrentShardLevelMetrics": current,
                "DesiredShardLevelMetrics": desired
            }),
        )
    }

    fn disable_enhanced_monitoring(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let stream = match Self::enhanced_monitoring_stream(req, &state) {
            Ok(s) => s,
            Err(resp) => return resp,
        };
        let to_disable: Vec<String> = req
            .params
            .get("ShardLevelMetrics")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|m| m.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let current = stream.shard_level_metrics.read().clone();
        let desired: Vec<String> = if to_disable.iter().any(|m| m == "ALL") {
            Vec::new()
        } else {
            current
                .iter()
                .filter(|m| !to_disable.contains(m))
                .cloned()
                .collect()
        };
        *stream.shard_level_metrics.write() = desired.clone();
        AwsResponse::json(
            200,
            json!({
                "StreamName": stream.name,
                "StreamARN": stream.arn,
                "CurrentShardLevelMetrics": current,
                "DesiredShardLevelMetrics": desired
            }),
        )
    }

    // --- Stream consumers ---

    fn register_stream_consumer(&self, req: &AwsRequest) -> AwsResponse {
        let stream_arn = req
            .params
            .get("StreamARN")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let consumer_name = req
            .params
            .get("ConsumerName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let stream = match state.find_stream_by_arn(stream_arn) {
            Some(s) => s,
            None => {
                return AwsResponse::error(
                    400,
                    "ResourceNotFoundException",
                    &format!("Stream not found: {stream_arn}"),
                )
            }
        };
        if stream.consumers.read().contains_key(consumer_name) {
            return AwsResponse::error(
                400,
                "ResourceInUseException",
                &format!("Consumer {consumer_name} already exists on stream {stream_arn}"),
            );
        }
        let ts = chrono::Utc::now().timestamp_millis() as f64 / 1000.0;
        let arn = format!("{stream_arn}/consumer/{consumer_name}:{}", ts as u64);
        let consumer = ConsumerInfo {
            name: consumer_name.to_string(),
            arn: arn.clone(),
            status: "ACTIVE".to_string(),
            created: ts,
        };
        stream
            .consumers
            .write()
            .insert(consumer_name.to_string(), consumer.clone());
        AwsResponse::json(200, json!({ "Consumer": consumer.to_value() }))
    }

    fn find_consumer_by_arn(
        state: &KinesisState,
        consumer_arn: &str,
    ) -> Option<(Arc<KinesisStream>, ConsumerInfo)> {
        for stream in state.streams.read().values() {
            for consumer in stream.consumers.read().values() {
                if consumer.arn == consumer_arn {
                    return Some((stream.clone(), consumer.clone()));
                }
            }
        }
        None
    }

    fn describe_stream_consumer(&self, req: &AwsRequest) -> AwsResponse {
        let stream_arn = req
            .params
            .get("StreamARN")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let consumer_name = req
            .params
            .get("ConsumerName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let consumer_arn = req
            .params
            .get("ConsumerARN")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);

        if !consumer_arn.is_empty() {
            return match Self::find_consumer_by_arn(&state, consumer_arn) {
                Some((_, consumer)) => {
                    AwsResponse::json(200, json!({ "ConsumerDescription": consumer.to_value() }))
                }
                None => AwsResponse::error(
                    400,
                    "ResourceNotFoundException",
                    &format!("Consumer {consumer_arn} not found"),
                ),
            };
        }

        for stream in state.streams.read().values() {
            if stream.arn == stream_arn {
                if let Some(consumer) = stream.consumers.read().get(consumer_name) {
                    return AwsResponse::json(
                        200,
                        json!({ "ConsumerDescription": consumer.to_value() }),
                    );
                }
            }
        }
        AwsResponse::error(
            400,
            "ResourceNotFoundException",
            &format!("Consumer {consumer_name} not found"),
        )
    }

    fn list_stream_consumers(&self, req: &AwsRequest) -> AwsResponse {
        let stream_arn = req
            .params
            .get("StreamARN")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let stream = match state.find_stream_by_arn(stream_arn) {
            Some(s) => s,
            None => {
                return AwsResponse::error(
                    400,
                    "ResourceNotFoundException",
                    &format!("Stream not found: {stream_arn}"),
                )
            }
        };
        let consumers: Vec<Value> = stream
            .consumers
            .read()
            .values()
            .map(|c| c.to_value())
            .collect();
        AwsResponse::json(200, json!({ "Consumers": consumers }))
    }

    fn deregister_stream_consumer(&self, req: &AwsRequest) -> AwsResponse {
        let stream_arn = req
            .params
            .get("StreamARN")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let consumer_name = req
            .params
            .get("ConsumerName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let consumer_arn = req
            .params
            .get("ConsumerARN")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);

        // ConsumerARN-only lookup
        if !consumer_arn.is_empty() && stream_arn.is_empty() {
            if let Some((stream, consumer)) = Self::find_consumer_by_arn(&state, consumer_arn) {
                let _ = stream.consumers.write().remove(&consumer.name);
            } else {
                return AwsResponse::error(
                    400,
                    "ResourceNotFoundException",
                    &format!(
                        "Consumer {consumer_arn}, account {} not found.",
                        req.account
                    ),
                );
            }
            return AwsResponse::json(200, json!({}));
        }

        let stream = match state.find_stream_by_arn(stream_arn) {
            Some(s) => s,
            None => {
                return AwsResponse::error(
                    400,
                    "ResourceNotFoundException",
                    &format!("Stream not found: {stream_arn}"),
                )
            }
        };
        let mut consumers = stream.consumers.write();
        if consumers.remove(consumer_name).is_none() {
            return AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!("Consumer {consumer_name} not found on stream {stream_arn}"),
            );
        }
        AwsResponse::json(200, json!({}))
    }

    fn subscribe_to_shard(&self, req: &AwsRequest) -> AwsResponse {
        let consumer_arn = req
            .params
            .get("ConsumerARN")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let (stream, _) = match Self::find_consumer_by_arn(&state, consumer_arn) {
            Some(pair) => pair,
            None => {
                return AwsResponse::error(
                    400,
                    "ResourceNotFoundException",
                    &format!(
                        "Consumer {consumer_arn}, account {} not found.",
                        req.account
                    ),
                )
            }
        };
        let shard_id = req
            .params
            .get("ShardId")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let shard = stream.shard_by_id(shard_id);
        let last_seq = shard
            .map(|s| s.get_latest_sequence())
            .unwrap_or_else(|| "0".to_string());
        AwsResponse::json(
            200,
            json!({
                "EventStream": {
                    "SubscribeToShardEvent": {
                        "Records": [],
                        "ContinuationSequenceNumber": last_seq,
                        "MillisBehindLatest": 0
                    }
                }
            }),
        )
    }

    // --- Resource policies ---

    fn put_resource_policy(&self, req: &AwsRequest) -> AwsResponse {
        let resource_arn = req
            .params
            .get("ResourceARN")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let policy = req
            .params
            .get("Policy")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let stream_name = stream_name_from_arn(resource_arn);
        if state.get_stream(stream_name).is_none() {
            return AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!(
                    "Stream {resource_arn} under account {} not found.",
                    req.account
                ),
            );
        }
        state
            .resource_policies
            .lock()
            .insert(resource_arn.to_string(), policy.to_string());
        AwsResponse::json(200, json!({}))
    }

    fn get_resource_policy(&self, req: &AwsRequest) -> AwsResponse {
        let resource_arn = req
            .params
            .get("ResourceARN")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let stream_name = stream_name_from_arn(resource_arn);
        if state.get_stream(stream_name).is_none() {
            return AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!(
                    "Stream {resource_arn} under account {} not found.",
                    req.account
                ),
            );
        }
        let policy = state
            .resource_policies
            .lock()
            .get(resource_arn)
            .cloned()
            .unwrap_or_else(|| "{}".to_string());
        AwsResponse::json(200, json!({ "Policy": policy }))
    }

    fn delete_resource_policy(&self, req: &AwsRequest) -> AwsResponse {
        let resource_arn = req
            .params
            .get("ResourceARN")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let stream_name = stream_name_from_arn(resource_arn);
        if state.get_stream(stream_name).is_none() {
            return AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!(
                    "Stream {resource_arn} under account {} not found.",
                    req.account
                ),
            );
        }
        let removed = state.resource_policies.lock().remove(resource_arn);
        match removed {
            Some(_) => AwsResponse::json(200, json!({})),
            None => AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!("No resource policy found for resource ARN {resource_arn}."),
            ),
        }
    }

    // --- Stream mode / on-demand ops ---

    fn update_stream_mode(&self, req: &AwsRequest) -> AwsResponse {
        let arn = req
            .params
            .get("StreamARN")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let stream = match state.find_stream_by_arn(arn) {
            Some(s) => s,
            None => {
                return AwsResponse::error(
                    400,
                    "ResourceNotFoundException",
                    &format!("Stream not found: {arn}"),
                )
            }
        };
        let mode = req
            .params
            .get("StreamModeDetails")
            .and_then(|v| v.get("StreamMode"))
            .and_then(|v| v.as_str())
            .unwrap_or("PROVISIONED");
        *stream.stream_mode.write() = mode.to_string();
        AwsResponse::json(200, json!({}))
    }

    fn update_max_record_size(&self, req: &AwsRequest) -> AwsResponse {
        let stream_arn = req
            .params
            .get("StreamARN")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        if state.find_stream_by_arn(stream_arn).is_none() {
            return AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!(
                    "Stream {stream_arn} under account {} not found.",
                    req.account
                ),
            );
        }
        let _size = req
            .params
            .get("MaxRecordSizeInKiB")
            .and_then(|v| v.as_u64());
        AwsResponse::json(200, json!({}))
    }

    fn update_stream_warm_throughput(&self, req: &AwsRequest) -> AwsResponse {
        let stream_arn = req
            .params
            .get("StreamARN")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let stream_name = req
            .params
            .get("StreamName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let warm_throughput_mibps = req
            .params
            .get("WarmThroughputMiBps")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let state = self.get_state(req.account, &req.region);
        let stream = if !stream_arn.is_empty() {
            state.find_stream_by_arn(stream_arn)
        } else if !stream_name.is_empty() {
            state.get_stream(stream_name)
        } else {
            None
        };
        let stream = match stream {
            Some(s) => s,
            None => {
                let lookup = if stream_arn.is_empty() {
                    stream_name
                } else {
                    stream_arn
                };
                return AwsResponse::error(
                    400,
                    "ResourceNotFoundException",
                    &format!("Stream {lookup} under account {} not found.", req.account),
                );
            }
        };
        AwsResponse::json(
            200,
            json!({
                "StreamARN": stream.arn,
                "StreamName": stream.name,
                "WarmThroughput": {
                    "TargetMiBps": warm_throughput_mibps,
                    "CurrentMiBps": warm_throughput_mibps
                }
            }),
        )
    }
}

fn not_found_stream(name: &str, account: u64) -> AwsResponse {
    AwsResponse::error(
        400,
        "ResourceNotFoundException",
        &format!("Stream {name} under account {account} not found."),
    )
}

fn invalid_argument(msg: &str) -> AwsResponse {
    AwsResponse::error(400, "InvalidArgumentException", msg)
}

/// Shard representation in DescribeStream / ListShards responses.
fn shard_value(s: &Shard) -> Value {
    json!({
        "ShardId": s.shard_id,
        "HashKeyRange": {
            "StartingHashKey": s.hash_key_start.to_string(),
            "EndingHashKey": s.hash_key_end.to_string()
        },
        "SequenceNumberRange": {
            "StartingSequenceNumber": s.starting_sequence_number
        }
    })
}

/// Extract the stream name from a stream or consumer ARN.
fn stream_name_from_arn(resource_arn: &str) -> &str {
    let parts: Vec<&str> = resource_arn.split('/').collect();
    for (i, part) in parts.iter().enumerate() {
        if *part == "stream" || part.ends_with(":stream") {
            if let Some(next) = parts.get(i + 1) {
                return next;
            }
        }
    }
    parts.last().copied().unwrap_or_default()
}

fn encode_iterator(
    stream_name: &str,
    shard_id: &str,
    iterator_type: &str,
    sequence: &str,
    region: &str,
    timestamp: Option<f64>,
) -> String {
    let payload = json!({
        "stream": stream_name,
        "shard": shard_id,
        "type": iterator_type,
        "seq": sequence,
        "region": region,
        "ts": chrono::Utc::now().timestamp_millis() as f64 / 1000.0,
        "timestamp": timestamp,
    });
    base64::engine::general_purpose::STANDARD.encode(payload.to_string())
}

fn decode_iterator(token: &str) -> Result<Value, AwsResponse> {
    let payload = base64::engine::general_purpose::STANDARD
        .decode(token)
        .ok()
        .and_then(|b| serde_json::from_slice::<Value>(&b).ok());
    match payload {
        Some(v) if v.is_object() => Ok(v),
        _ => Err(AwsResponse::error(
            400,
            "InvalidArgumentException",
            "Invalid ShardIterator.",
        )),
    }
}

impl Default for KinesisHandler {
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
            service: "kinesis".to_string(),
            operation: operation.to_string(),
            account: 123456789012,
            region: "us-east-1".to_string(),
            params,
            body: Bytes::new(),
        }
    }

    fn encode_data(data: &str) -> String {
        base64::engine::general_purpose::STANDARD
            .encode(data.as_bytes())
            .to_string()
    }

    fn parse_body(resp: &AwsResponse) -> Value {
        serde_json::from_str(&resp.body).unwrap()
    }

    #[test]
    fn test_create_and_list_streams() {
        let handler = KinesisHandler::new();
        handler.handle(make_req(
            "CreateStream",
            json!({"StreamName": "test-stream", "ShardCount": 2}),
        ));
        let resp = handler.handle(make_req("ListStreams", json!({})));
        assert_eq!(resp.status, 200);
        assert!(resp.body.contains("test-stream"));
    }

    #[test]
    fn test_put_and_get_records() {
        let handler = KinesisHandler::new();
        handler.handle(make_req(
            "CreateStream",
            json!({"StreamName": "rec-stream"}),
        ));
        let put = parse_body(&handler.handle(make_req(
            "PutRecord",
            json!({
                "StreamName": "rec-stream",
                "Data": encode_data("hello"),
                "PartitionKey": "pk1"
            }),
        )))
        .clone();
        let seq = put["SequenceNumber"].as_str().unwrap().to_string();
        let shard_id = put["ShardId"].as_str().unwrap().to_string();

        let iter = parse_body(&handler.handle(make_req(
            "GetShardIterator",
            json!({
                "StreamName": "rec-stream", "ShardId": shard_id,
                "ShardIteratorType": "TRIM_HORIZON"
            }),
        )))["ShardIterator"]
            .as_str()
            .unwrap()
            .to_string();

        let recs = parse_body(&handler.handle(make_req(
            "GetRecords",
            json!({
                "ShardIterator": iter
            }),
        )));
        assert_eq!(recs["Records"].as_array().unwrap().len(), 1);
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(recs["Records"][0]["Data"].as_str().unwrap())
                .unwrap(),
            b"hello"
        );
        assert!(recs["NextShardIterator"].is_string());
        assert_eq!(recs["MillisBehindLatest"], json!(0));
    }

    #[test]
    fn test_latest_iterator_skips_existing() {
        let handler = KinesisHandler::new();
        let name = "latest-stream";
        handler.handle(make_req("CreateStream", json!({"StreamName": name})));
        handler.handle(make_req(
            "PutRecord",
            json!({
                "StreamName": name, "Data": encode_data("before"), "PartitionKey": "pk1"
            }),
        ));
        let shard =
            parse_body(&handler.handle(make_req("ListShards", json!({"StreamName": name}))))
                ["Shards"][0]["ShardId"]
                .clone()
                .as_str()
                .unwrap()
                .to_string();
        let latest = parse_body(&handler.handle(make_req(
            "GetShardIterator",
            json!({
                "StreamName": name, "ShardId": shard, "ShardIteratorType": "LATEST"
            }),
        )))["ShardIterator"]
            .as_str()
            .unwrap()
            .to_string();
        let recs =
            parse_body(&handler.handle(make_req("GetRecords", json!({"ShardIterator": latest}))));
        assert_eq!(recs["Records"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn test_after_sequence_number_skips_first() {
        let handler = KinesisHandler::new();
        let name = "after-seq-stream";
        handler.handle(make_req("CreateStream", json!({"StreamName": name})));
        let put = parse_body(&handler.handle(make_req(
            "PutRecord",
            json!({
                "StreamName": name, "Data": encode_data("first"), "PartitionKey": "pk1"
            }),
        )));
        let seq = put["SequenceNumber"].as_str().unwrap().to_string();
        let shard = put["ShardId"].as_str().unwrap().to_string();
        handler.handle(make_req(
            "PutRecord",
            json!({
                "StreamName": name, "Data": encode_data("second"), "PartitionKey": "pk1"
            }),
        ));
        let iter = parse_body(&handler.handle(make_req(
            "GetShardIterator",
            json!({
                "StreamName": name, "ShardId": shard,
                "ShardIteratorType": "AFTER_SEQUENCE_NUMBER", "StartingSequenceNumber": seq
            }),
        )))["ShardIterator"]
            .as_str()
            .unwrap()
            .to_string();
        let recs =
            parse_body(&handler.handle(make_req("GetRecords", json!({"ShardIterator": iter}))));
        assert_eq!(recs["Records"][0]["PartitionKey"], json!("pk1"));
        assert_eq!(recs["Records"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn test_register_describe_consumer() {
        let handler = KinesisHandler::new();
        handler.handle(make_req(
            "CreateStream",
            json!({"StreamName": "cons-stream"}),
        ));
        let arn = parse_body(&handler.handle(make_req(
            "DescribeStream",
            json!({"StreamName": "cons-stream"}),
        )))["StreamDescription"]["StreamARN"]
            .clone()
            .as_str()
            .unwrap()
            .to_string();
        let reg = parse_body(&handler.handle(make_req(
            "RegisterStreamConsumer",
            json!({
                "StreamARN": arn, "ConsumerName": "c1"
            }),
        )));
        assert_eq!(reg["Consumer"]["ConsumerName"], json!("c1"));
        let consumer_arn = reg["Consumer"]["ConsumerARN"]
            .clone()
            .as_str()
            .unwrap()
            .to_string();
        let desc = parse_body(&handler.handle(make_req(
            "DescribeStreamConsumer",
            json!({
                "ConsumerARN": consumer_arn
            }),
        )));
        assert_eq!(
            desc["ConsumerDescription"]["ConsumerARN"],
            json!(consumer_arn)
        );
        let list =
            parse_body(&handler.handle(make_req("ListStreamConsumers", json!({"StreamARN": arn}))));
        assert_eq!(list["Consumers"].as_array().unwrap().len(), 1);
        handler.handle(make_req(
            "DeregisterStreamConsumer",
            json!({
                "StreamARN": arn, "ConsumerName": "c1"
            }),
        ));
        let list =
            parse_body(&handler.handle(make_req("ListStreamConsumers", json!({"StreamARN": arn}))));
        assert_eq!(list["Consumers"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn test_split_shard_adds_shards() {
        let handler = KinesisHandler::new();
        handler.handle(make_req(
            "CreateStream",
            json!({"StreamName": "split-stream"}),
        ));
        let shards = parse_body(&handler.handle(make_req(
            "ListShards",
            json!({"StreamName": "split-stream"}),
        )))["Shards"]
            .clone();
        let shard_id = shards[0]["ShardId"].clone().as_str().unwrap().to_string();
        let start: u128 = shards[0]["HashKeyRange"]["StartingHashKey"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        let end: u128 = shards[0]["HashKeyRange"]["EndingHashKey"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(end, 0xffffffffffffffffffffffffffffffffu128);
        handler.handle(make_req(
            "SplitShard",
            json!({
                "StreamName": "split-stream", "ShardToSplit": shard_id,
                "NewStartingHashKey": format!("{}", (start + end) / 2)
            }),
        ));
        let shards = parse_body(&handler.handle(make_req(
            "ListShards",
            json!({"StreamName": "split-stream"}),
        )))["Shards"]
            .clone();
        assert_eq!(shards.as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_update_shard_count_rebuilds() {
        let handler = KinesisHandler::new();
        handler.handle(make_req(
            "CreateStream",
            json!({"StreamName": "usc-stream"}),
        ));
        let resp = parse_body(&handler.handle(make_req(
            "UpdateShardCount",
            json!({
                "StreamName": "usc-stream", "TargetShardCount": 2, "ScalingType": "UNIFORM_SCALING"
            }),
        )));
        assert_eq!(resp["CurrentShardCount"], json!(1));
        assert_eq!(resp["TargetShardCount"], json!(2));
        let shards = parse_body(
            &handler.handle(make_req("ListShards", json!({"StreamName": "usc-stream"}))),
        )["Shards"]
            .clone();
        assert_eq!(shards.as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_tags_roundtrip_and_overwrite() {
        let handler = KinesisHandler::new();
        handler.handle(make_req(
            "CreateStream",
            json!({"StreamName": "tag-stream"}),
        ));
        handler.handle(make_req(
            "AddTagsToStream",
            json!({
                "StreamName": "tag-stream", "Tags": {"mykey": "original", "extra": "x"}
            }),
        ));
        handler.handle(make_req(
            "AddTagsToStream",
            json!({
                "StreamName": "tag-stream", "Tags": {"mykey": "updated"}
            }),
        ));
        let tags = parse_body(&handler.handle(make_req(
            "ListTagsForStream",
            json!({"StreamName": "tag-stream"}),
        )));
        assert_eq!(tags["Tags"][0]["Value"], json!("x"));
        assert_eq!(tags["Tags"][0]["Key"], json!("extra"));
        assert_eq!(tags["Tags"][1]["Value"], json!("updated"));
        assert_eq!(tags["HasMoreTags"], json!(false));
    }

    #[test]
    fn test_duplicate_create_and_delete_missing() {
        let handler = KinesisHandler::new();
        handler.handle(make_req("CreateStream", json!({"StreamName": "dup"})));
        let resp = handler.handle(make_req("CreateStream", json!({"StreamName": "dup"})));
        assert_eq!(resp.body, json!({"__type": "ResourceInUseException", "message": "Stream dup under account 123456789012 already exists."}).to_string());
        let resp = handler.handle(make_req("DeleteStream", json!({"StreamName": "missing"})));
        assert_eq!(resp.status, 400);
        assert!(resp.body.contains("ResourceNotFoundException"));
    }

    #[test]
    fn test_hash_ranges_cover_full_space() {
        for count in [1u32, 2, 3, 4, 5] {
            let shards = crate::models::build_shards(count);
            let mut expected_start = 0u128;
            for (i, s) in shards.iter().enumerate() {
                assert_eq!(s.hash_key_start, expected_start, "shard {i}/{count}");
                if i + 1 == count as usize {
                    assert_eq!(s.hash_key_end, crate::models::MAX_HASH_KEY);
                } else {
                    expected_start = s.hash_key_end.checked_add(1).unwrap();
                }
            }
        }
    }

    #[test]
    fn test_update_shard_count_current_before_rebuild() {
        let handler = KinesisHandler::new();
        handler.handle(make_req(
            "CreateStream",
            json!({"StreamName": "usc2-stream"}),
        ));

        // CurrentShardCount must reflect the count at call time
        handler.handle(make_req(
            "UpdateShardCount",
            json!({
                "StreamName": "usc2-stream", "TargetShardCount": 4, "ScalingType": "UNIFORM_SCALING"
            }),
        ));
        let shards = parse_body(
            &handler.handle(make_req("ListShards", json!({"StreamName": "usc2-stream"}))),
        )["Shards"]
            .clone();
        assert_eq!(shards.as_array().unwrap().len(), 4);
        let total = shards.as_array().unwrap().len() as u64;
        let resp = parse_body(&handler.handle(make_req(
            "UpdateShardCount",
            json!({
                "StreamName": "usc2-stream", "TargetShardCount": 2, "ScalingType": "UNIFORM_SCALING"
            }),
        )));
        assert_eq!(resp["CurrentShardCount"], json!(total));
    }

    #[test]
    fn test_stream_summary_fields() {
        let handler = KinesisHandler::new();
        handler.handle(make_req(
            "CreateStream",
            json!({"StreamName": "sum-stream"}),
        ));
        let resp = parse_body(&handler.handle(make_req(
            "DescribeStreamSummary",
            json!({
                "StreamName": "sum-stream"
            }),
        )));
        let summary = resp["StreamDescriptionSummary"].clone();
        assert!(summary["StreamName"].as_str().is_some());
        assert!(summary["StreamDescriptionSummary"].is_null());
        assert!(summary["EnhancedMonitoring"].is_array());
        assert!(summary["OpenShardCount"].as_u64().is_some());
        assert!(summary["ConsumerCount"].as_u64().is_some());
    }

    #[test]
    fn test_update_shard_count_times_out_first_call() {
        // Regression guard: UpdateShardCount must not panic when shards are empty
        let handler = KinesisHandler::new();
        handler.handle(make_req(
            "CreateStream",
            json!({"StreamName": "usc3-stream"}),
        ));
        let resp = handler.handle(make_req(
            "UpdateShardCount",
            json!({
                "StreamName": "usc3-stream", "TargetShardCount": 3
            }),
        ));
        assert_eq!(resp.status, 200);
    }
}
