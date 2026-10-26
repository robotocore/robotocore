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
    pub(crate) state: RwLock<HashMap<(u64, String), KinesisState>>,
}

impl KinesisHandler {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) fn get_state(&self, account: u64, region: &str) -> KinesisState {
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

    /// moto's `Stream.to_json_summary()["StreamDescriptionSummary"]`.
    fn stream_summary_value(stream: &KinesisStream) -> Value {
        json!({
            "StreamName": stream.name,
            "StreamARN": stream.arn,
            "StreamStatus": stream.status,
            "StreamModeDetails": { "StreamMode": "PROVISIONED" },
            "RetentionPeriodHours": *stream.retention_hours.read(),
            "StreamCreationTimestamp": stream.created,
            "EnhancedMonitoring": [ { "ShardLevelMetrics": [] } ],
            "OpenShardCount": stream.shards.read().len(),
            "ConsumerCount": stream.consumers.read().len(),
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
            Some(stream) => AwsResponse::json(
                200,
                json!({
                    "StreamDescriptionSummary": Self::stream_summary_value(&stream),
                }),
            ),
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
        // moto keys `streams` in a dict; this twin uses a BTreeMap, so the
        // traversal order is sorted by name.
        let mut names = state.list_stream_names();
        // ExclusiveStartStreamName keeps working: it trims the list before
        // pagination (starting after the named stream).
        if let Some(start) = req
            .params
            .get("ExclusiveStartStreamName")
            .and_then(|v| v.as_str())
        {
            if let Some(pos) = names.iter().position(|n| n == start) {
                names.drain(..=pos);
            }
        }
        // moto PR #10385: ListStreams pages on NextToken. The token carries
        // the first excluded stream name in botocore's token encoding
        // ({"uniqueAttributes": "<name>"}); limit defaults to moto's 10.
        // An undecodable token is treated as no token
        // (`fail_on_invalid_token: False`); a well-formed token that names
        // no stream on this page answers an empty page.
        let limit = req
            .params
            .get("Limit")
            .and_then(|v| v.as_u64())
            .unwrap_or(10) as usize;
        let start = match req
            .params
            .get("NextToken")
            .and_then(Value::as_str)
            .map(decode_list_streams_token)
        {
            Some(Ok(unique)) => match unique.and_then(|name| names.iter().position(|n| n == &name))
            {
                Some(pos) => pos,
                None => {
                    return AwsResponse::json(
                        200,
                        json!({
                            "StreamNames": [],
                            "HasMoreStreams": false,
                            "StreamSummaries": [],
                        }),
                    );
                }
            },
            // undecodable token (or none): page from the start
            _ => 0,
        };
        let remaining = &names[start..];
        let streams_map = state.streams.read();
        let page: Vec<Arc<KinesisStream>> = remaining
            .iter()
            .take(limit)
            .filter_map(|name| streams_map.get(name).cloned())
            .collect();
        let index_end = start + limit;
        let has_next = !page.is_empty() && index_end < names.len();
        let next_name = if has_next {
            remaining.get(limit).map(String::as_str)
        } else {
            None
        };
        let mut out = json!({
            "StreamNames": page.iter().map(|s| s.name.clone()).collect::<Vec<_>>(),
            "StreamSummaries": page
                .iter()
                .map(|s| Self::stream_summary_value(s))
                .collect::<Vec<_>>(),
            "HasMoreStreams": next_name.is_some(),
        });
        if let Some(name) = next_name {
            out["NextToken"] = json!(encode_list_streams_token(name));
        }
        AwsResponse::json(200, out)
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
        let data_len = data.len();
        let explicit = req.params.get("ExplicitHashKey").and_then(|v| v.as_str());
        match stream.put_record(partition_key, data, explicit) {
            Some((shard, record)) => {
                // moto 5.2.4 KinesisBackend.put_record → _send_stream_metrics:
                // IncomingBytes = len(record data) + len(partition key),
                // where the partition-key length is counted in CODE POINTS
                // (moto's len(str), not UTF-8 bytes — deliberately inherited
                // from moto; real AWS counts bytes. Live-verified on the
                // pinned 5.2.4 fork: a record whose data + 4-code-point
                // partition key totals exactly 1048576 code points is
                // accepted, though it exceeds 1048576 UTF-8 bytes).
                // IncomingRecords = 1, dimensioned by StreamName only.
                cloudwatch::bridge::ingest_kinesis_put_metrics(
                    req.account,
                    &req.region,
                    name,
                    (data_len + partition_key.chars().count()) as u64,
                    1,
                );
                AwsResponse::json(
                    200,
                    json!({
                        "ShardId": shard.shard_id,
                        "SequenceNumber": record.sequence_number,
                        "EncryptionType": "NONE",
                    }),
                )
            }
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
        // moto 5.2.4 KinesisBackend.put_records validates before storing any
        // record and before _send_stream_metrics (moto/kinesis/models.py
        // ::put_records): record count first, then the call's total size,
        // then the first per-record offender. A record's size is its data
        // length plus the partition-key length in code points, matching the
        // metric accounting below and moto's len(str) — live-verified: a
        // record whose data+partition-key code points total exactly
        // 1048576 is accepted.
        if records_input.len() > 500 {
            // moto kinesis/exceptions.py :: TooManyRecords.
            return AwsResponse::error(
                400,
                "ValidationException",
                "1 validation error detected: Value at 'records' failed to satisfy constraint: Member must have length less than or equal to 500",
            );
        }
        // moto accounts every input record into the call's aggregated
        // datapoint (data size + partition-key size in code points),
        // successful or not.
        let mut incoming_bytes: u64 = 0;
        let mut over_limit_position: Option<usize> = None;
        let mut decoded_records: Vec<(String, Vec<u8>, Option<String>)> =
            Vec::with_capacity(records_input.len());
        for (idx, rec) in records_input.iter().enumerate() {
            let partition_key = rec
                .get("PartitionKey")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let data_b64 = rec.get("Data").and_then(|v| v.as_str()).unwrap_or_default();
            let data = base64::engine::general_purpose::STANDARD
                .decode(data_b64)
                .unwrap_or_default();
            let size = data.len() + partition_key.chars().count();
            incoming_bytes += size as u64;
            if size > 1048576 && over_limit_position.is_none() {
                over_limit_position = Some(idx + 1);
            }
            let explicit = rec
                .get("ExplicitHashKey")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            decoded_records.push((partition_key, data, explicit));
        }
        if incoming_bytes > 5242880 {
            // moto kinesis/exceptions.py :: TotalRecordsSizeExceedsLimit.
            return AwsResponse::error(
                400,
                "InvalidArgumentException",
                "Records size exceeds 5 MB limit",
            );
        }
        if let Some(position) = over_limit_position {
            // moto kinesis/exceptions.py :: RecordSizeExceedsLimit (1-based).
            return AwsResponse::error(
                400,
                "ValidationException",
                &format!(
                    "1 validation error detected: Value at 'records.{position}.member.data' failed to satisfy constraint: Member must have length less than or equal to 1048576"
                ),
            );
        }
        let mut result_records = Vec::with_capacity(decoded_records.len());
        for (partition_key, data, explicit) in decoded_records {
            if let Some((shard, record)) =
                stream.put_record(&partition_key, data, explicit.as_deref())
            {
                result_records.push(json!({
                    "ShardId": shard.shard_id,
                    "SequenceNumber": record.sequence_number
                }));
            }
        }
        // moto 5.2.4 KinesisBackend.put_records → _send_stream_metrics: one
        // aggregated datapoint pair for the whole call — IncomingBytes =
        // summed record sizes, IncomingRecords = number of records sent.
        cloudwatch::bridge::ingest_kinesis_put_metrics(
            req.account,
            &req.region,
            name,
            incoming_bytes,
            records_input.len() as u64,
        );
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

/// The ListStreams page token: botocore's TokenEncoder shape (base64 of
/// `{"uniqueAttributes": "<stream name>"}`). moto additionally embeds a
/// parameterChecksum derived from Python's salted `hash`, which is not
/// wire-portable, so this twin mints only the unique attribute — and
/// likewise ignores the checksum member of an incoming token.
fn encode_list_streams_token(stream_name: &str) -> String {
    base64::engine::general_purpose::STANDARD
        .encode(json!({ "uniqueAttributes": stream_name }).to_string())
}

/// `Ok(Some(name))` for a decodable token naming a stream, `Ok(None)` when
/// the decoded token carries no uniqueAttributes member, `Err(())` for an
/// undecodable token — moto's paginate treats that as no token
/// (`fail_on_invalid_token: False`). Padding is optional (botocore strips
/// it when encoding).
fn decode_list_streams_token(token: &str) -> Result<Option<String>, ()> {
    let mut padded = token.to_string();
    let rem = padded.len() % 4;
    if rem != 0 {
        padded.push_str(&"=".repeat(4 - rem));
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(padded)
        .map_err(|_| ())?;
    let value: Value = serde_json::from_slice(&bytes).map_err(|_| ())?;
    Ok(value
        .get("uniqueAttributes")
        .and_then(Value::as_str)
        .map(String::from))
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
    fn kinesis_list_streams_paginates() {
        let handler = KinesisHandler::new();
        let names = ["a", "b", "c", "d", "e"];
        for name in names {
            handler.handle(make_req(
                "CreateStream",
                json!({"StreamName": name, "ShardCount": 1}),
            ));
        }

        // moto PR #10385: pages carry both the names and the summaries and
        // mint a NextToken whose uniqueAttributes is the first excluded
        // stream name.
        let page1 = parse_body(&handler.handle(make_req("ListStreams", json!({"Limit": 2}))));
        assert_eq!(page1["StreamNames"], json!(["a", "b"]));
        assert_eq!(
            json!(page1["StreamSummaries"]
                .as_array()
                .unwrap()
                .iter()
                .map(|s| s["StreamName"].clone())
                .collect::<Vec<_>>()),
            json!(["a", "b"])
        );
        assert_eq!(page1["HasMoreStreams"], true);
        assert_eq!(
            decode_list_streams_token(page1["NextToken"].as_str().unwrap()),
            Ok(Some("c".to_string()))
        );

        let page2 = parse_body(&handler.handle(make_req(
            "ListStreams",
            json!({"Limit": 2, "NextToken": page1["NextToken"]}),
        )));
        assert_eq!(page2["StreamNames"], json!(["c", "d"]));
        assert_eq!(page2["HasMoreStreams"], true);

        let page3 = parse_body(&handler.handle(make_req(
            "ListStreams",
            json!({"Limit": 2, "NextToken": page2["NextToken"]}),
        )));
        assert_eq!(page3["StreamNames"], json!(["e"]));
        assert_eq!(page3["HasMoreStreams"], false);
        assert!(page3.get("NextToken").is_none());

        // The default page size is moto's 10.
        let unpaged = parse_body(&handler.handle(make_req("ListStreams", json!({}))));
        assert_eq!(unpaged["StreamNames"].as_array().unwrap().len(), 5);
        assert_eq!(unpaged["HasMoreStreams"], false);
        assert!(unpaged.get("NextToken").is_none());

        // ExclusiveStartStreamName still trims the list before pagination.
        let trimmed = parse_body(&handler.handle(make_req(
            "ListStreams",
            json!({"Limit": 100, "ExclusiveStartStreamName": "a"}),
        )));
        assert_eq!(trimmed["StreamNames"], json!(["b", "c", "d", "e"]));

        // An undecodable token behaves like no token; a well-formed one
        // naming an unknown stream answers an empty page with no token.
        let garbage = parse_body(&handler.handle(make_req(
            "ListStreams",
            json!({"Limit": 1, "NextToken": "garbage"}),
        )));
        assert_eq!(garbage["StreamNames"].as_array().unwrap().len(), 1);
        let unknown = parse_body(&handler.handle(make_req(
            "ListStreams",
            json!({
                "Limit": 1,
                "NextToken": encode_list_streams_token("no-such-stream"),
            }),
        )));
        assert_eq!(unknown["StreamNames"].as_array().unwrap().len(), 0);
        assert_eq!(unknown["HasMoreStreams"], false);
    }

    /// moto 5.2.4: put_record/put_records push IncomingBytes +
    /// IncomingRecords metrics to CloudWatch (namespace AWS/Kinesis,
    /// StreamName dimension, one aggregated datapoint pair per call).
    #[test]
    fn kput_emits_cloudwatch_metrics() {
        // The ingest registry is process-global and first-wins; reset it so
        // this test binds its own handler regardless of test order in this
        // binary (sole-registrant contract documented in
        // cloudwatch::bridge::reset_registry).
        cloudwatch::bridge::reset_registry();
        // The cloudwatch handler registers itself into the process-global
        // ingest bridge on construction; account scoping keeps the
        // datapoints isolated from other tests sharing this process.
        let cw = cloudwatch::DefaultCloudwatchHandler::new();
        let handler = KinesisHandler::new();
        let account: u64 = 447089000321;
        let req = |operation: &str, params: Value| AwsRequest {
            service: "kinesis".to_string(),
            operation: operation.to_string(),
            account,
            region: "us-east-1".to_string(),
            params,
            body: Bytes::new(),
        };
        let cw_req = |operation: &str, params: Value| cloudwatch::AwsRequest {
            service: "cloudwatch".to_string(),
            operation: operation.to_string(),
            account,
            region: "us-east-1".to_string(),
            params,
            body: Bytes::new(),
        };

        handler.handle(req("CreateStream", json!({"StreamName": "cw-stream"})));
        // "hello" (5 bytes) + "pk1" (3 chars) = 8 incoming bytes, 1 record.
        handler.handle(req(
            "PutRecord",
            json!({
                "StreamName": "cw-stream",
                "Data": encode_data("hello"),
                "PartitionKey": "pk1"
            }),
        ));
        // ("alpha" 5 + "pk1" 3) + ("beta" 4 + "pk2" 3) = 15 bytes, 2 records.
        handler.handle(req(
            "PutRecords",
            json!({
                "StreamName": "cw-stream",
                "Records": [
                    {"Data": encode_data("alpha"), "PartitionKey": "pk1"},
                    {"Data": encode_data("beta"), "PartitionKey": "pk2"},
                ],
            }),
        ));
        // A put to a missing stream lands nowhere, so metrics must stay off.
        let missing = handler.handle(req(
            "PutRecord",
            json!({
                "StreamName": "no-such-stream",
                "Data": encode_data("ghost"),
                "PartitionKey": "pk"
            }),
        ));
        assert_eq!(missing.status, 400);

        let cw_parse =
            |resp: &cloudwatch::AwsResponse| serde_json::from_str::<Value>(&resp.body).unwrap();

        let list = cw_parse(&cw.handle(cw_req("ListMetrics", json!({"Namespace": "AWS/Kinesis"}))));
        let metrics = list["Metrics"].as_array().unwrap();
        assert_eq!(metrics.len(), 2, "got {metrics:?}");
        for m in metrics {
            assert_eq!(m["Namespace"], json!("AWS/Kinesis"));
            let dims = m["Dimensions"].as_array().unwrap();
            assert_eq!(dims.len(), 1);
            assert_eq!(dims[0], json!({"Name": "StreamName", "Value": "cw-stream"}));
        }
        let names: Vec<&str> = metrics
            .iter()
            .map(|m| m["MetricName"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"IncomingBytes"), "got {metrics:?}");
        assert!(names.contains(&"IncomingRecords"), "got {metrics:?}");

        // StartTime/EndTime are read as epoch milliseconds by the handler.
        let start = chrono::Utc::now().timestamp_millis() - 3_600_000;
        let end = chrono::Utc::now().timestamp_millis() + 3_600_000;
        let stats = |metric: &str, statistics: Value| {
            cw_parse(&cw.handle(cw_req(
                "GetMetricStatistics",
                json!({
                    "Namespace": "AWS/Kinesis",
                    "MetricName": metric,
                    "Statistics": statistics,
                    "Period": 60,
                    "StartTime": start,
                    "EndTime": end,
                }),
            )))
        };

        // Two puts in one 60s bucket: 2 datapoints, summed values.
        let bytes = stats("IncomingBytes", json!(["Sum", "SampleCount"]));
        assert_eq!(bytes["Datapoints"].as_array().unwrap().len(), 1, "{bytes}");
        assert_eq!(bytes["Datapoints"][0]["Sum"], json!(23.0));
        assert_eq!(bytes["Datapoints"][0]["SampleCount"], json!(2.0));
        assert_eq!(bytes["Datapoints"][0]["Unit"], json!("Bytes"));

        // IncomingRecords: 1 (put_record) + 2 (put_records) datapoint values.
        let records = stats("IncomingRecords", json!(["Sum", "SampleCount"]));
        assert_eq!(records["Datapoints"][0]["Sum"], json!(3.0));
        assert_eq!(records["Datapoints"][0]["SampleCount"], json!(2.0));
        assert_eq!(records["Datapoints"][0]["Unit"], json!("Count"));

        // Per-account scoping: another account sees none of these metrics.
        let other = cw_parse(&cw.handle(cloudwatch::AwsRequest {
            account: 447089000322,
            ..cw_req(
                "GetMetricStatistics",
                json!({
                    "Namespace": "AWS/Kinesis",
                    "MetricName": "IncomingBytes",
                    "Statistics": ["Sum"],
                    "Period": 60,
                    "StartTime": start,
                    "EndTime": end,
                }),
            )
        }));
        assert_eq!(other["Datapoints"].as_array().unwrap().len(), 0);
    }

    fn encode_bytes(data: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(data)
    }

    /// The limit rejections must fire before any CloudWatch ingest (moto
    /// raises in put_records before storing and before _send_stream_metrics).
    /// The bridge is process-global and possibly bound to another test's
    /// handler (kput_emits_cloudwatch_metrics registers one in parallel
    /// threads), but each call below uses a per-test account that no other
    /// test pushes to, and a failing put never reaches the ingest — so
    /// whichever handler is bound, this account shows no datapoints.
    fn assert_no_put_metrics(account: u64, stream: &str) {
        let cw = cloudwatch::bridge::test_registered_handler()
            .unwrap_or_else(|| Arc::new(cloudwatch::handler::CloudwatchHandler::new()));
        let now = chrono::Utc::now().timestamp_millis();
        let cw_req = |operation: &str, params: Value| cloudwatch::AwsRequest {
            service: "cloudwatch".to_string(),
            operation: operation.to_string(),
            account,
            region: "us-east-1".to_string(),
            params,
            body: Bytes::new(),
        };
        for metric in ["IncomingBytes", "IncomingRecords"] {
            let stats: Value = serde_json::from_str(
                &cw.handle(cw_req(
                    "GetMetricStatistics",
                    json!({
                        "Namespace": "AWS/Kinesis",
                        "MetricName": metric,
                        "Statistics": ["Sum", "SampleCount"],
                        "Period": 60,
                        "StartTime": now - 3_600_000,
                        "EndTime": now + 3_600_000,
                    }),
                ))
                .body,
            )
            .unwrap();
            assert!(
                stats["Datapoints"].as_array().unwrap().is_empty(),
                "{stream}: unexpected {metric} datapoints: {stats}"
            );
        }
    }

    #[test]
    fn kput_records_limits_over_count() {
        let handler = KinesisHandler::new();
        let account: u64 = 447089000555;
        let req = |operation: &str, params: Value| AwsRequest {
            service: "kinesis".to_string(),
            operation: operation.to_string(),
            account,
            region: "us-east-1".to_string(),
            params,
            body: Bytes::new(),
        };
        handler.handle(req(
            "CreateStream",
            json!({"StreamName": "lim-count", "ShardCount": 1}),
        ));
        // moto kinesis/exceptions.py :: TooManyRecords — first (before the
        // size checks), 400 ValidationException.
        let over = parse_body(&handler.handle(req(
            "PutRecords",
            json!({
                "StreamName": "lim-count",
                "Records": (0..501)
                    .map(|_| json!({"Data": encode_data("x"), "PartitionKey": "p"}))
                    .collect::<Vec<_>>(),
            }),
        )));
        assert_eq!(over["__type"], json!("ValidationException"));
        assert_eq!(
            over["message"],
            json!("1 validation error detected: Value at 'records' failed to satisfy constraint: Member must have length less than or equal to 500")
        );
        assert_no_put_metrics(account, "lim-count");
    }

    #[test]
    fn kput_records_limits_over_total() {
        let handler = KinesisHandler::new();
        let account: u64 = 447089000556;
        let req = |operation: &str, params: Value| AwsRequest {
            service: "kinesis".to_string(),
            operation: operation.to_string(),
            account,
            region: "us-east-1".to_string(),
            params,
            body: Bytes::new(),
        };
        handler.handle(req(
            "CreateStream",
            json!({"StreamName": "lim-total", "ShardCount": 1}),
        ));
        // Six 1-MiB records: each within the per-record limit, but the call
        // sum (6291456) exceeds 5242880. moto kinesis/exceptions.py ::
        // TotalRecordsSizeExceedsLimit (400 InvalidArgumentException).
        let megabyte_b64 = encode_bytes(&vec![b'a'; 1048576]);
        let over = parse_body(&handler.handle(req(
            "PutRecords",
            json!({
                "StreamName": "lim-total",
                "Records": (0..6)
                    .map(|_| json!({"Data": megabyte_b64, "PartitionKey": "p"}))
                    .collect::<Vec<_>>(),
            }),
        )));
        assert_eq!(over["__type"], json!("InvalidArgumentException"));
        assert_eq!(over["message"], json!("Records size exceeds 5 MB limit"));
        assert_no_put_metrics(account, "lim-total");
    }

    #[test]
    fn kput_records_limits_single_over() {
        let handler = KinesisHandler::new();
        let account: u64 = 447089000557;
        let req = |operation: &str, params: Value| AwsRequest {
            service: "kinesis".to_string(),
            operation: operation.to_string(),
            account,
            region: "us-east-1".to_string(),
            params,
            body: Bytes::new(),
        };
        handler.handle(req(
            "CreateStream",
            json!({"StreamName": "lim-one", "ShardCount": 1}),
        ));
        // One record over the 1 MiB limit. moto kinesis/exceptions.py ::
        // RecordSizeExceedsLimit (400 ValidationException, 1-based position).
        let over = parse_body(&handler.handle(req(
            "PutRecords",
            json!({
                "StreamName": "lim-one",
                "Records": [{"Data": encode_bytes(&[b'a'; 1048577]), "PartitionKey": "p"}],
            }),
        )));
        assert_eq!(over["__type"], json!("ValidationException"));
        assert_eq!(
            over["message"],
            json!("1 validation error detected: Value at 'records.1.member.data' failed to satisfy constraint: Member must have length less than or equal to 1048576")
        );

        // The first offender names the position: record #2 here.
        let over2 = parse_body(&handler.handle(req(
            "PutRecords",
            json!({
                "StreamName": "lim-one",
                "Records": [
                    {"Data": encode_data("x"), "PartitionKey": "p"},
                    {"Data": encode_bytes(&[b'a'; 1048577]), "PartitionKey": "p"},
                    {"Data": encode_data("x"), "PartitionKey": "p"},
                ],
            }),
        )));
        assert_eq!(
            over2["message"],
            json!("1 validation error detected: Value at 'records.2.member.data' failed to satisfy constraint: Member must have length less than or equal to 1048576")
        );

        assert_no_put_metrics(account, "lim-one");

        // The rejected calls must have stored nothing: a following valid
        // put is the only record a reader sees.
        handler.handle(req(
            "PutRecords",
            json!({
                "StreamName": "lim-one",
                "Records": [{"Data": encode_data("kept"), "PartitionKey": "p"}],
            }),
        ));
        let shard =
            parse_body(&handler.handle(req("ListShards", json!({"StreamName": "lim-one"}))))
                ["Shards"][0]["ShardId"]
                .as_str()
                .unwrap()
                .to_string();
        let iter = parse_body(&handler.handle(req(
            "GetShardIterator",
            json!({
                "StreamName": "lim-one",
                "ShardId": shard,
                "ShardIteratorType": "TRIM_HORIZON"
            }),
        )))["ShardIterator"]
            .as_str()
            .unwrap()
            .to_string();
        let recs = parse_body(&handler.handle(req("GetRecords", json!({"ShardIterator": iter}))));
        let records = recs["Records"].as_array().unwrap();
        assert_eq!(records.len(), 1, "got {records:?}");
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(records[0]["Data"].as_str().unwrap())
                .unwrap(),
            b"kept"
        );
    }

    #[test]
    fn kput_records_limits_within_bounds() {
        let handler = KinesisHandler::new();
        let account: u64 = 447089000558;
        let req = |operation: &str, params: Value| AwsRequest {
            service: "kinesis".to_string(),
            operation: operation.to_string(),
            account,
            region: "us-east-1".to_string(),
            params,
            body: Bytes::new(),
        };
        handler.handle(req(
            "CreateStream",
            json!({"StreamName": "lim-ok", "ShardCount": 1}),
        ));
        let tiny = json!({"Data": encode_data("x"), "PartitionKey": "p"});
        // Live-verified on moto 5.2.4: the checks are strict — exactly 500
        // records, a record summing to exactly 1048576 (data 1048575 +
        // 1-code-point key) and a call totalling exactly 5242880 all pass.
        let ok = parse_body(&handler.handle(req(
            "PutRecords",
            json!({
                "StreamName": "lim-ok",
                "Records": (0..500).map(|_| tiny.clone()).collect::<Vec<_>>(),
            }),
        )));
        assert_eq!(ok["FailedRecordCount"], json!(0));
        assert_eq!(ok["Records"].as_array().unwrap().len(), 500);

        let at_limit = parse_body(&handler.handle(req(
            "PutRecords",
            json!({
                "StreamName": "lim-ok",
                "Records": [
                    {"Data": encode_bytes(&[b'a'; 1048575]), "PartitionKey": "p"}
                ],
            }),
        )));
        assert_eq!(at_limit["FailedRecordCount"], json!(0));
        assert_eq!(at_limit["Records"].as_array().unwrap().len(), 1);

        let at_total = parse_body(&handler.handle(req(
            "PutRecords",
            json!({
                "StreamName": "lim-ok",
                "Records": (0..5)
                    .map(|_| json!({"Data": encode_bytes(&[b'a'; 1048575]), "PartitionKey": "p"}))
                    .collect::<Vec<_>>(),
            }),
        )));
        assert_eq!(at_total["FailedRecordCount"], json!(0));
        assert_eq!(at_total["Records"].as_array().unwrap().len(), 5);
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
