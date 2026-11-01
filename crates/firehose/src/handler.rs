//! Firehose operation handler (mirrors src/robotocore/services/firehose/provider.py).

use base64::Engine as _;
use parking_lot::RwLock;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};

use crate::models::{DeliveryStream, FirehoseState};
use crate::protocol::{AwsRequest, AwsResponse};

pub struct FirehoseHandler {
    pub(crate) state: RwLock<HashMap<(u64, String), FirehoseState>>,
}

impl FirehoseHandler {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) fn get_state(&self, account: u64, region: &str) -> FirehoseState {
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
            // moto (firehose models.py:217-221); note moto's "accountId"
            // spelling here.
            return AwsResponse::error(
                400,
                "ResourceInUseException",
                &format!(
                    "Firehose {name} under accountId {} already exists",
                    req.account
                ),
            );
        }
        // moto's per-region stream quota (firehose models.py:223-228): the
        // 51st stream raises LimitExceededException.
        let existing = state.list_names();
        if existing.len() >= MAX_STREAMS_PER_REGION {
            // moto renders Python's list repr (models.py:226-228)
            let quoted: Vec<String> = existing.iter().map(|n| format!("'{n}'")).collect();
            return AwsResponse::error(
                400,
                "LimitExceededException",
                &format!(
                    "You have already consumed your firehose quota of {} hoses. Firehose names: [{}]",
                    MAX_STREAMS_PER_REGION,
                    quoted.join(", ")
                ),
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
            s3_config_is_extended: req
                .params
                .get("ExtendedS3DestinationConfiguration")
                .is_some(),
            last_failure: None,
            s3_config,
            encryption: None,
            http_endpoint_config: req
                .params
                .get("HttpEndpointDestinationConfiguration")
                .and_then(|v| v.as_object())
                .cloned(),
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
            None => stream_not_found(name, req.account),
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
                    // botocore carries two separate destination members:
                    // `ExtendedS3DestinationDescription` (created via
                    // ExtendedS3DestinationConfiguration; adds the
                    // processing/s3backup members) vs the minimal
                    // `S3DestinationDescription` for the plain shape (moto
                    // keeps S3/ExtendedS3 separate, models.py:146-164). The
                    // stream remembers which flavor was configured. Restore
                    // paths from older snapshots may have dropped the flag —
                    // default back to ExtendedS3 (the description the twin
                    // always rendered before it existed).
                    let mut destination = json!({ "DestinationId": dest_id_for(&s) });
                    let key = if s.s3_config_is_extended {
                        "ExtendedS3DestinationDescription"
                    } else {
                        "S3DestinationDescription"
                    };
                    destination[key] = Value::Object(s3_desc);
                    destinations.push(destination);
                }
                // The HttpEndpoint destination renders verbatim with moto's
                // `{key}DestinationDescription` naming (firehose
                // models.py:331-340: every destination dict key other than
                // destination_id becomes `{key}DestinationDescription`).
                if let Some(http_cfg) = &s.http_endpoint_config {
                    if !http_cfg.is_empty() {
                        destinations.push(json!({
                            "DestinationId": dest_id_for(&s),
                            "HttpEndpointDestinationDescription": Value::Object(http_cfg.clone()),
                        }));
                    }
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
                // Real-AWS async-failure semantics: failed deliveries surface
                // under botocore's modeled FailureDescription member
                // (firehose service model: DeliveryStreamDescription.
                // FailureDescription {Type, Details}; the S3-destination
                // miss class is none of the KMS/VPC/ENI failure types, so
                // it maps to the enum's UNKNOWN_ERROR slot).
                if let Some(reason) = &s.last_failure {
                    desc["FailureDescription"] =
                        json!({"Type": "UNKNOWN_ERROR", "Details": reason});
                }
                AwsResponse::json(200, json!({ "DeliveryStreamDescription": desc }))
            }
            None => stream_not_found(name, req.account),
        }
    }

    fn list_streams(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        // moto's list_delivery_streams (firehose models.py:344-382): the
        // DeliveryStreamType filter is honored, the list is alphabetical, an
        // unknown exclusive-start name is ignored, and the default limit is
        // MAX_STREAMS_PER_REGION (50), not 100.
        let type_filter = req
            .params
            .get("DeliveryStreamType")
            .and_then(|v| v.as_str())
            .filter(|t| !t.is_empty());
        let mut names = state.list_names();
        if let Some(want) = type_filter {
            names.retain(|n| state.get(n).map(|s| s.stream_type == want).unwrap_or(false));
        }
        let limit = req
            .params
            .get("Limit")
            .and_then(|v| v.as_u64())
            .unwrap_or(MAX_STREAMS_PER_REGION as u64) as usize;
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
            return stream_not_found(name, req.account);
        }
        let data_b64 = req
            .params
            .get("Record")
            .and_then(|r| r.get("Data"))
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let data = base64::engine::general_purpose::STANDARD
            .decode(data_b64)
            .unwrap_or_default();
        let record_id = uuid::Uuid::new_v4().to_string();
        let state = self.get_state(req.account, &req.region);
        let mut failed = 0u32;
        if let Some(stream) = state.get(name) {
            match self.deliver_records(req, name, &stream, &[data]) {
                Ok(()) => self.record_delivery_success(req, name),
                Err(reason) => {
                    failed = 1;
                    self.record_delivery_failure(req, name, &reason);
                }
            }
        }
        // Happy path matches moto's PutRecord shape ({RecordId, Encrypted});
        // a failed landing surfaces the honest FailedPutCount=1 member on
        // top (real-AWS async-failure semantics — the failure also lands
        // in DescribeDeliveryStream via last_failure). Encrypted is moto's
        // static False (firehose models.py:423 — "always set Encrypted to
        // False"), never a delivery indicator.
        let mut out = json!({
            "RecordId": record_id,
            "Encrypted": false
        });
        if failed > 0 {
            out["FailedPutCount"] = json!(failed);
        }
        AwsResponse::json(200, out)
    }

    /// Record a failed delivery on the stream (real-AWS async-failure
    /// semantics: the PutRecord itself stays 200, the failure surfaces in
    /// DescribeDeliveryStream under the modeled FailureDescription member).
    fn record_delivery_failure(&self, req: &AwsRequest, name: &str, reason: &str) {
        let state = self.get_state(req.account, &req.region);
        if let Some(mut stream) = state.get(name) {
            stream.last_failure = Some(reason.to_string());
            state.put(stream);
        }
    }

    /// Clear the failure signal once a delivery actually lands (w6-crate
    /// F3: the stale failure text described a healthy stream forever after
    /// recovery).
    fn record_delivery_success(&self, req: &AwsRequest, name: &str) {
        let state = self.get_state(req.account, &req.region);
        if let Some(mut stream) = state.get(name) {
            if stream.last_failure.is_some() {
                stream.last_failure = None;
                state.put(stream);
            }
        }
    }

    /// moto's `put_s3_records` (firehose models.py:474-501): the joined
    /// record bytes land in the destination bucket under
    /// `Prefix + YYYY/MM/DD/HH/{stream}-{version}-{ts}-{uuid}` (the LocalStack
    /// object-naming contract), delivered through the process's live S3
    /// state via the s3 crate's cross-service bridge.
    ///
    /// Failure contract: moto does NOT create missing buckets — its S3
    /// `put_object` raises `MissingBucket` (models.py:2016-2031), which
    /// firehose converts to its RuntimeError crash. The twin keeps the
    /// PutRecord(Batch) 200 (real-AWS async-failure semantics) and records
    /// the miss: the caller bumps `FailedPutCount` and the stream's
    /// `last_failure` carries moto's literal `NoSuchBucket` message so the
    /// failure surfaces in DescribeDeliveryStream. A bare firehose process
    /// without a registered S3 state (crate unit tests) has the same
    /// "missing bucket" signal — no phantom bucket is ever created.
    fn put_s3_records(
        &self,
        req: &AwsRequest,
        name: &str,
        version_id: u64,
        records: &[Vec<u8>],
    ) -> DeliveredRecords {
        let state = self.get_state(req.account, &req.region);
        let Some(stream) = state.get(name) else {
            return DeliveredRecords::All;
        };
        let Some(bucket) = stream
            .s3_config
            .get("BucketARN")
            .and_then(|v| v.as_str())
            .and_then(|arn| arn.rsplit(':').next())
            .filter(|b| !b.is_empty())
        else {
            return DeliveredRecords::All;
        };
        let prefix = stream
            .s3_config
            .get("Prefix")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let object_path = format_s3_object_path(name, version_id, prefix);
        let joined: Vec<u8> = records.iter().flatten().cloned().collect();
        match s3::bridge::put_object(
            req.account,
            &req.region,
            bucket,
            &object_path,
            joined,
            "application/octet-stream",
        ) {
            s3::bridge::PutObjectOutcome::Delivered(_) => DeliveredRecords::All,
            s3::bridge::PutObjectOutcome::MissingBucket => DeliveredRecords::Failed(format!(
                "Failed to deliver to bucket {bucket}: {MISSING_BUCKET_MESSAGE}"
            )),
        }
    }

    /// moto's `put_record_batch` destination sweep (firehose
    /// models.py:496-542): the S3 flavors deliver via `put_s3_records`,
    /// HttpEndpoint posts real HTTP via `put_http_records`, and the
    /// unimplemented destination kinds (Elasticsearch/Redshift/Snowflake)
    /// answer "proper response" RecordIds with `FailedPutCount` 0 — data
    /// with no destination is honestly reported as succeeded, like moto.
    /// `Err` carries moto's literal failure message when the delivery
    /// itself failed (moto raises its RuntimeError and 500-crashes there;
    /// the twin keeps the 200 and records the failure on the stream — the
    /// crate's documented put failure contract, see `put_s3_records`).
    fn deliver_records(
        &self,
        req: &AwsRequest,
        name: &str,
        stream: &DeliveryStream,
        records: &[Vec<u8>],
    ) -> Result<(), String> {
        if !stream.s3_config.is_empty() {
            // ExtendedS3 must win over the mirrored S3 entry, like moto's
            // if/elif chain (models.py:508-530); the twin keeps the single
            // `s3_config` either way.
            return match self.put_s3_records(req, name, stream.version_id, records) {
                DeliveredRecords::All => Ok(()),
                DeliveredRecords::Failed(reason) => Err(reason),
            };
        }
        if let Some(http) = &stream.http_endpoint_config {
            return put_http_records(http, records);
        }
        Ok(())
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
            return stream_not_found(name, req.account);
        }
        let mut responses = Vec::with_capacity(records.len());
        let mut decoded: Vec<Vec<u8>> = Vec::with_capacity(records.len());
        for rec in records {
            let data_b64 = rec.get("Data").and_then(|v| v.as_str()).unwrap_or_default();
            decoded.push(
                base64::engine::general_purpose::STANDARD
                    .decode(data_b64)
                    .unwrap_or_default(),
            );
            responses.push(json!({ "RecordId": uuid::Uuid::new_v4().to_string() }));
        }
        // moto (firehose models.py:503-546): every record joins into one S3
        // object per put. A FAILED landing (missing bucket) bumps
        // FailedPutCount by the whole put's records — the caller-visible
        // honest failure signal.
        let mut failed_put_count = 0u32;
        if let Some(stream) = state.get(name) {
            match self.deliver_records(req, name, &stream, &decoded) {
                Ok(()) => self.record_delivery_success(req, name),
                Err(reason) => {
                    failed_put_count = decoded.len() as u32;
                    self.record_delivery_failure(req, name, &reason);
                }
            }
        }
        AwsResponse::json(
            200,
            json!({
                "FailedPutCount": failed_put_count,
                // moto's static False (firehose models.py:540), never a
                // delivery indicator.
                "Encrypted": false,
                "RequestResponses": responses
            }),
        )
    }

    /// moto's `update_destination` (firehose models.py:604-702), including
    /// its check order: exactly one `*DestinationUpdate` argument (its
    /// `find_destination_config_in_args`), stream existence, then the
    /// `ConcurrentModificationException` version check, then the
    /// destination-id lookup, then the type-switch guard for
    /// Elasticsearch/HttpEndpoint, then the apply.
    fn update_destination(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("DeliveryStreamName")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let destination_id = req
            .params
            .get("DestinationId")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        // Exactly one destination configuration in the request. moto's
        // DESTINATION_TYPES_TO_NAMES mapping (models.py:45-54): the request
        // member name minus `_destination` (Snowflake keeps its
        // `Configuration` name on the update path, responses.py:113).
        let update_kinds: &[(&str, &str)] = &[
            ("S3", "S3DestinationUpdate"),
            ("ExtendedS3", "ExtendedS3DestinationUpdate"),
            ("Redshift", "RedshiftDestinationUpdate"),
            ("Elasticsearch", "ElasticsearchDestinationUpdate"),
            ("Splunk", "SplunkDestinationUpdate"),
            ("HttpEndpoint", "HttpEndpointDestinationUpdate"),
            ("Snowflake", "SnowflakeDestinationConfiguration"),
            ("Iceberg", "IcebergDestinationUpdate"),
        ];
        let found: Vec<(&str, &Map<String, Value>)> = update_kinds
            .iter()
            .filter_map(|(dest, param)| {
                req.params
                    .get(*param)
                    .and_then(|v| v.as_object())
                    .filter(|cfg| !cfg.is_empty())
                    .map(|cfg| (*dest, cfg))
            })
            .collect();
        if found.len() != 1 {
            return AwsResponse::error(
                400,
                "InvalidArgumentException",
                "Exactly one destination configuration is supported for a Firehose",
            );
        }
        let (dest_name, dest_config) = found[0];
        let state = self.get_state(req.account, &req.region);
        let mut s = match state.get(&name) {
            Some(s) => s,
            // moto's own update lookup spells it `accountId` (models.py:624).
            None => {
                return AwsResponse::error(
                    400,
                    "ResourceNotFoundException",
                    &format!("Firehose {name} under accountId {} not found.", req.account),
                )
            }
        };
        // moto compares the string version id and its message interpolates
        // `None` when the request omitted it (models.py:633-639).
        let provided = match req.params.get("CurrentDeliveryStreamVersionId") {
            Some(Value::String(v)) => v.clone(),
            Some(Value::Number(n)) => n.to_string(),
            Some(other) => other.to_string(),
            None => "None".to_string(),
        };
        let current = s.version_id.to_string();
        if provided != current {
            return AwsResponse::error(
                400,
                "ConcurrentModificationException",
                &format!(
                    "Cannot update firehose: {name} since the current version id: {current} and specified version id: {provided} do not match"
                ),
            );
        }
        // moto's destinations carry `destinationId-000000000001` as the id of
        // the first entry (models.py:146-152); the unknown id is a genuine
        // InvalidArgumentException (models.py:641-648), not a silent
        // version bump.
        if destination_id != dest_id_for(&s) {
            return AwsResponse::error(
                400,
                "InvalidArgumentException",
                &format!("Destination Id {destination_id} not found"),
            );
        }
        // Switching to or from (Elasticsearch|HttpEndpoint) is rejected
        // (models.py:650-659).
        if (dest_name == "Elasticsearch" && s.s3_config.is_empty())
            || (dest_name == "HttpEndpoint" && s.http_endpoint_config.is_none())
        {
            return AwsResponse::error(
                400,
                "InvalidArgumentException",
                &format!(
                    "Changing the destination type to or from {dest_name} is not supported at this time."
                ),
            );
        }
        // Apply. moto keys the branch on `dest_name in destination`
        // (models.py:663-669): same key → shallow top-level `update` —
        // keys in this request REPLACE the stored ones wholesale, keys not
        // named are kept; a truly absent key replaces the whole entry.
        // An ExtendedS3 creation AUTO-CONTAINS the "S3" mirror key
        // (models.py:183-186 → `destinations[0]["S3"] =
        // create_s3_destination_config(config)`), so an S3DestinationUpdate
        // against an ExtendedS3 stream finds "S3" already there and takes
        // the merge path; moto then re-projects the ExtendedS3 key from the
        // merged mirror (models.py:688-693 — shared keys carry the updated
        // values, ExtendedS3-only keys the twin stores keep). moto
        // additionally renders BOTH S3DestinationDescription and
        // ExtendedS3DestinationDescription for such streams (describe
        // renders every destination key, models.py:331-340) — the twin's
        // single-config renderer keeps the ExtendedS3-flavored description
        // (documented simplification). An ExtendedS3Update against a plain
        // S3 stream does replace the whole entry (models.py:666-668) and
        // auto-creates the S3 mirror (680-683) — the twin's replace+flip
        // covers the ExtendedS3 slot; the not-rendered mirror is the same
        // documented simplification.
        match dest_name {
            "HttpEndpoint" => {
                let mut updated = s.http_endpoint_config.clone().unwrap_or_default();
                for (k, v) in dest_config {
                    updated.insert(k.clone(), v.clone());
                }
                s.http_endpoint_config = Some(updated);
            }
            "S3" | "ExtendedS3" => {
                let same_flavor = (dest_name == "ExtendedS3") == s.s3_config_is_extended
                    && !s.s3_config.is_empty();
                // An S3 update on an ExtendedS3-created stream merges into
                // the auto-mirrored S3 slot (moto: "S3" IS in destination).
                let mirrored_s3_update =
                    dest_name == "S3" && s.s3_config_is_extended && !s.s3_config.is_empty();
                if same_flavor || mirrored_s3_update {
                    // moto's `destinations[idx][dest_name].update
                    // (dest_config)` (models.py:663-664) — this request's
                    // top-level keys REPLACE the stored ones wholesale, and
                    // keys not named here are kept — then the
                    // S3Update/S3BackupUpdate/S3BackupConfiguration →
                    // *Description renames (models.py:671-678) shallow-merge
                    // into the stored description. moto keeps the
                    // update-spelled key too; moto's post-update
                    // ExtendedS3-key projection (models.py:688-693) then
                    // drops ExtendedS3-only fields from that render — the
                    // mirror-driven describe divergence the twin does not
                    // carry (documented above).
                    for (k, v) in dest_config.clone() {
                        s.s3_config.insert(k, v);
                    }
                    alt_field_shallow_update(
                        &mut s.s3_config,
                        "S3DestinationDescription",
                        dest_config.get("S3Update"),
                    );
                    alt_field_shallow_update(
                        &mut s.s3_config,
                        "S3BackupDescription",
                        dest_config.get("S3BackupConfiguration"),
                    );
                    alt_field_shallow_update(
                        &mut s.s3_config,
                        "S3BackupDescription",
                        dest_config.get("S3BackupUpdate"),
                    );
                } else {
                    // Different destination type: moto replaces the whole
                    // entry (models.py:666-668). The ExtendedS3 mirror
                    // pruning at models.py:688-693 mutates a stale dict in
                    // moto (a no-op there — the else branch replaced it).
                    s.s3_config = dest_config.clone();
                    s.s3_config_is_extended = dest_name == "ExtendedS3";
                }
            }
            _ => {
                // Destination kinds the twin's create path cannot store
                // (Redshift/ES/Splunk/Snowflake/Iceberg): unreachable
                // through the twin's own surface. moto still bumps the
                // version here — the twin does the same, store as-is.
            }
        }
        // moto's version bump (models.py:696).
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
            None => return stream_not_found(name, req.account),
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
            None => return stream_not_found(name, req.account),
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
            None => return stream_not_found(name, req.account),
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
            None => return stream_not_found(name, req.account),
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
            None => return stream_not_found(name, req.account),
        };
        // moto only flips Status on the existing config (firehose models.py:
        // 595-602) — a whole-config replace erased KeyType/KeyARN from
        // describe output after stop→describe.
        let mut enc = s.encryption.clone().unwrap_or_else(|| json!({}));
        if let Some(obj) = enc.as_object_mut() {
            obj.insert("Status".into(), json!("DISABLED"));
        }
        s.encryption = Some(enc);
        state.put(s);
        AwsResponse::json(200, json!({}))
    }
}

/// Delivery bookkeeping for one PutRecord(Batch) S3 landing.
enum DeliveredRecords {
    /// The whole batch landed.
    All,
    /// The landing itself failed; the message carries moto's literal
    /// MissingBucket wording and the FAILED count covers the put's records.
    Failed(String),
}

/// moto's `DeliveryStream.MAX_STREAMS_PER_REGION` (firehose models.py:118)
/// — the ListDeliveryStreams default limit and the per-region quota.
const MAX_STREAMS_PER_REGION: usize = 50;

/// moto's `ResourceNotFoundException` text:
/// `Firehose {name} under account {account} not found.`
/// (firehose models.py:284-287 and the sibling lookups). UpdateDestination's
/// own lookup spells it `accountId` (models.py:624) — see `update_stream`.
fn stream_not_found(name: &str, account: u64) -> AwsResponse {
    AwsResponse::error(
        400,
        "ResourceNotFoundException",
        &format!("Firehose {name} under account {account} not found."),
    )
}

/// moto's destination id for the stream's first (only) destination —
/// `destinationId-000000000001` (firehose models.py:146-152).
fn dest_id_for(_stream: &DeliveryStream) -> String {
    "destinationId-000000000001".to_string()
}

/// moto's ALTERNATIVE_FIELD_NAMES apply on UpdateDestination
/// (firehose models.py:671-678): `dest_config[old]` shallow-updates into a
/// (created-if-absent) `{new}` member of the destination config.
fn alt_field_shallow_update(
    dest_config: &mut Map<String, Value>,
    new: &str,
    update: Option<&Value>,
) {
    let Some(update) = update else {
        return;
    };
    let mut slot = dest_config
        .get(new)
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();
    if let Some(upd_obj) = update.as_object() {
        for (k, v) in upd_obj {
            slot.insert(k.clone(), v.clone());
        }
        dest_config.insert(new.to_string(), Value::Object(slot));
        return;
    }
    dest_config.insert(new.to_string(), update.clone());
}

/// moto's `put_http_records` (firehose models.py:426-449): the HttpEndpoint
/// destination is delivered for real — one POST of
/// `{requestId: uuid4, timestamp: epoch-seconds, records: [{data: <base64>}]}`
/// with `Content-Type: application/json`, and one `(hyphenated) uuid4`
/// RecordId per record. moto raises its RuntimeError on any failure — the
/// twin surfaces that same literal through the crate's documented 200 +
/// FailedPutCount + last_failure contract.
fn put_http_records(
    http_destination: &Map<String, Value>,
    records: &[Vec<u8>],
) -> Result<(), String> {
    let url = http_destination
        .get("EndpointConfiguration")
        .and_then(|e| e.get("Url"))
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    if url.is_empty() {
        // moto KeyErrors on the absent URL (a client-side validation case
        // botocore already blocks); answer the failure literal instead.
        return Err(HTTP_DELIVERY_FAILED_MESSAGE.to_string());
    }
    let record_to_send = json!({
        "requestId": uuid::Uuid::new_v4().to_string(),
        "timestamp": chrono::Utc::now().timestamp(),
        "records": records
            .iter()
            .map(|r| {
                json!({ "data": base64::engine::general_purpose::STANDARD.encode(r) })
            })
            .collect::<Vec<_>>(),
    });
    // Loopback one-shot posts: close each connection (moto's mirror agent
    // pattern, rotocore cloudformation/mirror.rs client()).
    let outcome = http_agent()
        .post(url)
        .set("Content-Type", "application/json")
        .set("Connection", "close")
        .send_json(&record_to_send);
    match outcome {
        Ok(_) => Ok(()),
        Err(_) => Err(HTTP_DELIVERY_FAILED_MESSAGE.to_string()),
    }
}

/// Shared blocking agent for HttpEndpoint delivery (same shape as
/// rotocore's `client()`: bounded connect/read timeouts).
fn http_agent() -> ureq::Agent {
    use std::sync::OnceLock;
    static AGENT: OnceLock<ureq::Agent> = OnceLock::new();
    AGENT
        .get_or_init(|| {
            ureq::AgentBuilder::new()
                .timeout_connect(std::time::Duration::from_secs(5))
                .timeout_read(std::time::Duration::from_secs(20))
                .build()
        })
        .clone()
}

/// moto's literal RuntimeError text for a failed HTTP delivery
/// (firehose models.py:445-448).
const HTTP_DELIVERY_FAILED_MESSAGE: &str = "Firehose PutRecord(Batch) to HTTP destination failed";

/// moto's MissingBucket wire text (s3 exceptions.py:54-59).
const MISSING_BUCKET_MESSAGE: &str = "The specified bucket does not exist";

/// moto's `_format_s3_object_path` (firehose models.py:510-541): the
/// delivery-path contract — a slash-terminated prefix, then the
/// `YYYY/MM/DD/HH` hour folder, then
/// `{stream}-{version}-{YYYY-MM-DD-HH-MM-SS}-{uuid}`.
fn format_s3_object_path(delivery_stream_name: &str, version_id: u64, prefix: &str) -> String {
    let prefix = if prefix.is_empty() || prefix.ends_with('/') {
        prefix.to_string()
    } else {
        format!("{prefix}/")
    };
    let now = chrono::Utc::now();
    format!(
        "{prefix}{hour}/{name}-{version}-{ts}-{uid}",
        prefix = prefix,
        hour = now.format("%Y/%m/%d/%H"),
        name = delivery_stream_name,
        version = version_id,
        ts = now.format("%Y-%m-%d-%H-%M-%S"),
        uid = uuid::Uuid::new_v4(),
    )
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
        assert_eq!(dest["DestinationId"], json!("destinationId-000000000001"));
        assert_eq!(
            dest["ExtendedS3DestinationDescription"]["CompressionFormat"],
            json!("GZIP")
        );
        assert_eq!(
            dest["ExtendedS3DestinationDescription"]["BufferingHints"]["SizeInMBs"],
            json!(5)
        );

        // Update destination bumps version and replaces the named top-level
        // keys (moto's shallow update — firehose models.py:663-664).
        let upd = handler.handle(make_req(
            "UpdateDestination",
            json!({
                "DeliveryStreamName": "test-stream",
                "CurrentDeliveryStreamVersionId": "1",
                "DestinationId": "destinationId-000000000001",
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
        // Wrong version raises moto's ConcurrentModificationException
        let bad = handler.handle(make_req(
            "UpdateDestination",
            json!({
                "DeliveryStreamName": "test-stream",
                "CurrentDeliveryStreamVersionId": "999",
                "DestinationId": "destinationId-000000000001",
                "ExtendedS3DestinationUpdate": {"CompressionFormat": "GZIP"}
            }),
        ));
        assert!(bad.body.contains("ConcurrentModificationException"));
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

    /// Wave-5 S-5: a PutRecord to a bucket that was never created is an
    /// honest failure — 200 with FailedPutCount=1 (real-AWS async-failure
    /// semantics), the failure text surfaces in DescribeDeliveryStream
    /// (moto's MissingBucket literal), and NO phantom bucket is created.
    #[test]
    fn test_put_record_missing_bucket_fails_honestly() {
        let handler = FirehoseHandler::new();
        handler.handle(make_req(
            "CreateDeliveryStream",
            json!({
                "DeliveryStreamName": "ghost-bucket-stream",
                "ExtendedS3DestinationConfiguration": {
                    "BucketARN": "arn:aws:s3:::ghost-bucket-never-created",
                    "RoleARN": "arn:aws:iam::123456789012:role/fh-role"
                }
            }),
        ));
        let put = serde_json::from_str::<Value>(
            &handler
                .handle(make_req(
                    "PutRecord",
                    json!({
                        "DeliveryStreamName": "ghost-bucket-stream",
                        "Record": {
                            "Data": base64::engine::general_purpose::STANDARD.encode(b"hello")
                        }
                    }),
                ))
                .body,
        )
        .unwrap();
        assert_eq!(
            put["RecordId"].as_str().map(str::len),
            Some(36),
            "RecordId is a hyphenated uuid (moto str(uuid4()))"
        );
        assert_eq!(
            put.get("FailedPutCount"),
            Some(&json!(1)),
            "the honest failure member: FailedPutCount=1"
        );

        // The failure is visible in the stream description under botocore's
        // modeled FailureDescription member (UNKNOWN_ERROR type + moto's
        // MissingBucket text) — and no phantom bucket exists in S3.
        let desc = serde_json::from_str::<Value>(
            &handler
                .handle(make_req(
                    "DescribeDeliveryStream",
                    json!({"DeliveryStreamName": "ghost-bucket-stream"}),
                ))
                .body,
        )
        .unwrap();
        let failure = &desc["DeliveryStreamDescription"]["FailureDescription"];
        assert_eq!(
            failure["Type"], "UNKNOWN_ERROR",
            "the S3 miss maps to the enum's catch-all slot: {desc}"
        );
        assert!(
            failure["Details"]
                .as_str()
                .unwrap_or_default()
                .contains("The specified bucket does not exist"),
            "moto's MissingBucket literal surfaces in Details: {desc}"
        );
    }

    /// The stale failure must clear: after recovery (the bucket exists) the
    /// next delivery erases FailureDescription (w6-crate F3).
    #[test]
    fn test_delivery_failure_clears_on_success() {
        let handler = FirehoseHandler::new();
        handler.handle(make_req(
            "CreateDeliveryStream",
            json!({
                "DeliveryStreamName": "recover-stream",
                "ExtendedS3DestinationConfiguration": {
                    "BucketARN": "arn:aws:s3:::recover-bucket-missing",
                    "RoleARN": "arn:aws:iam::123456789012:role/fh-role"
                }
            }),
        ));
        handler.handle(make_req(
            "PutRecord",
            json!({"DeliveryStreamName": "recover-stream",
                   "Record": {"Data": base64::engine::general_purpose::STANDARD.encode(b"x")}}),
        ));
        let desc = serde_json::from_str::<Value>(
            &handler
                .handle(make_req(
                    "DescribeDeliveryStream",
                    json!({"DeliveryStreamName": "recover-stream"}),
                ))
                .body,
        )
        .unwrap();
        assert!(
            desc["DeliveryStreamDescription"]["FailureDescription"]["Details"]
                .as_str()
                .is_some(),
            "the failed landing signals: {desc}"
        );

        // Register a pre-made bucket through the bridge's registry axis and
        // deliver again — the failure flag must clear.
        let s3_state = s3::models::S3State::new();
        let bucket = std::sync::Arc::new(s3::models::Bucket::new(
            "recover-bucket-missing".to_string(),
            "us-east-1".to_string(),
        ));
        s3_state.put_bucket(bucket);
        s3::bridge::register_state(123456789012, "us-east-1", &s3_state);
        handler.handle(make_req(
            "PutRecord",
            json!({"DeliveryStreamName": "recover-stream",
                   "Record": {"Data": base64::engine::general_purpose::STANDARD.encode(b"y")}}),
        ));
        let desc = serde_json::from_str::<Value>(
            &handler
                .handle(make_req(
                    "DescribeDeliveryStream",
                    json!({"DeliveryStreamName": "recover-stream"}),
                ))
                .body,
        )
        .unwrap();
        assert!(
            desc["DeliveryStreamDescription"]
                .get("FailureDescription")
                .is_none(),
            "the recovered stream must render no FailureDescription: {desc}"
        );
    }

    /// moto's `ResourceNotFoundException` text, used everywhere a stream
    /// lookup fails (firehose models.py:284-287 et al).
    #[test]
    fn test_not_found_text_matches_moto() {
        let handler = FirehoseHandler::new();
        let put = handler.handle(make_req(
            "PutRecord",
            json!({"DeliveryStreamName": "ghost", "Record": {"Data": "aGk="}}),
        ));
        assert_eq!(put.status, 400);
        let parsed: Value = serde_json::from_str(&put.body).unwrap();
        assert_eq!(parsed["__type"], json!("ResourceNotFoundException"));
        assert_eq!(
            parsed["message"],
            json!("Firehose ghost under account 123456789012 not found.")
        );
        let del = handler.handle(make_req(
            "DeleteDeliveryStream",
            json!({"DeliveryStreamName": "ghost"}),
        ));
        assert_eq!(
            serde_json::from_str::<Value>(&del.body).unwrap()["message"],
            json!("Firehose ghost under account 123456789012 not found.")
        );
    }

    /// moto's UpdateDestination branches (firehose models.py:604-702): the
    /// unknown destination id and type-switch are
    /// `InvalidArgumentException`s, the version conflict a
    /// `ConcurrentModificationException` (with `None` when the request
    /// omitted the id), and a same-type update REPLACES the named
    /// top-level keys wholesale while keeping the others.
    #[test]
    fn test_update_destination_moto_branches() {
        let handler = FirehoseHandler::new();
        handler.handle(make_req(
            "CreateDeliveryStream",
            json!({
                "DeliveryStreamName": "upd",
                "ExtendedS3DestinationConfiguration": {
                    "BucketARN": "arn:aws:s3:::bkt",
                    "RoleARN": "arn:aws:iam::123456789012:role/r",
                    "BufferingHints": {"SizeInMBs": 5, "IntervalInSeconds": 300}
                }
            }),
        ));
        let moto_dest_id = "destinationId-000000000001";

        // Unknown destination id: InvalidArgumentException "Destination Id …
        // not found", and the version is NOT bumped.
        let unknown = handler.handle(make_req(
            "UpdateDestination",
            json!({
                "DeliveryStreamName": "upd",
                "CurrentDeliveryStreamVersionId": "1",
                "DestinationId": "destinationId-999999999999",
                "ExtendedS3DestinationUpdate": {"CompressionFormat": "GZIP"}
            }),
        ));
        assert_eq!(unknown.status, 400);
        let parsed: Value = serde_json::from_str(&unknown.body).unwrap();
        assert_eq!(parsed["__type"], json!("InvalidArgumentException"));
        assert_eq!(
            parsed["message"],
            json!("Destination Id destinationId-999999999999 not found")
        );
        let desc = serde_json::from_str::<Value>(
            &handler
                .handle(make_req(
                    "DescribeDeliveryStream",
                    json!({"DeliveryStreamName": "upd"}),
                ))
                .body,
        )
        .unwrap();
        assert_eq!(desc["DeliveryStreamDescription"]["VersionId"], json!("1"));

        // Omitted version id: moto interpolates `None` in the message.
        let no_version = handler.handle(make_req(
            "UpdateDestination",
            json!({
                "DeliveryStreamName": "upd",
                "DestinationId": moto_dest_id,
                "ExtendedS3DestinationUpdate": {"CompressionFormat": "GZIP"}
            }),
        ));
        let parsed: Value = serde_json::from_str(&no_version.body).unwrap();
        assert_eq!(parsed["__type"], json!("ConcurrentModificationException"));
        assert_eq!(
            parsed["message"],
            json!("Cannot update firehose: upd since the current version id: 1 and specified version id: None do not match")
        );

        // Same-type update REPLACES named keys wholesale: BufferingHints
        // loses SizeInMBs, and the untouched BucketARN survives.
        let ok = handler.handle(make_req(
            "UpdateDestination",
            json!({
                "DeliveryStreamName": "upd",
                "CurrentDeliveryStreamVersionId": "1",
                "DestinationId": moto_dest_id,
                "ExtendedS3DestinationUpdate": {
                    "BufferingHints": {"IntervalInSeconds": 120}
                }
            }),
        ));
        assert_eq!(ok.status, 200);
        let desc = serde_json::from_str::<Value>(
            &handler
                .handle(make_req(
                    "DescribeDeliveryStream",
                    json!({"DeliveryStreamName": "upd"}),
                ))
                .body,
        )
        .unwrap();
        let ext = &desc["DeliveryStreamDescription"]["Destinations"][0]
            ["ExtendedS3DestinationDescription"];
        assert_eq!(
            ext["BufferingHints"].get("SizeInMBs"),
            None,
            "moto replaces the nested member wholesale: {ext}"
        );
        assert_eq!(ext["BufferingHints"]["IntervalInSeconds"], json!(120));
        assert_eq!(
            ext["BucketARN"],
            json!("arn:aws:s3:::bkt"),
            "keys not named in the update are kept"
        );
        assert_eq!(desc["DeliveryStreamDescription"]["VersionId"], json!("2"));

        // An S3DestinationUpdate against an ExtendedS3-created stream
        // SHALLOW-MERGES into the auto-mirrored S3 slot rather than
        // replacing the entry: moto's branch key is `dest_name in
        // destination` (models.py:663-669) and an ExtendedS3 creation
        // auto-contains the "S3" mirror key (models.py:183-186), so "S3"
        // is already there. Sampled fields: an updated shared key lands,
        // mirror-inherited keys (BufferingHints) survive the merge, and
        // theExtendedS3 slot is re-projected by moto (models.py:688-693).
        // The twin merges into the single stored config and keeps the
        // ExtendedS3-flavored description (moto renders BOTH descriptions
        // for such streams — the documented simplify-the-mirror gap).
        let merged = handler.handle(make_req(
            "UpdateDestination",
            json!({
                "DeliveryStreamName": "upd",
                "CurrentDeliveryStreamVersionId": "2",
                "DestinationId": moto_dest_id,
                "S3DestinationUpdate": {
                    "BucketARN": "arn:aws:s3:::plain",
                    "RoleARN": "arn:aws:iam::123456789012:role/r"
                }
            }),
        ));
        assert_eq!(merged.status, 200);
        assert_eq!(
            serde_json::from_str::<Value>(
                &handler
                    .handle(make_req(
                        "DescribeDeliveryStream",
                        json!({"DeliveryStreamName": "upd"}),
                    ))
                    .body,
            )
            .unwrap()["DeliveryStreamDescription"]["VersionId"],
            json!("3"),
            "the merge also bumps the version"
        );
        let merged_desc = serde_json::from_str::<Value>(
            &handler
                .handle(make_req(
                    "DescribeDeliveryStream",
                    json!({"DeliveryStreamName": "upd"}),
                ))
                .body,
        )
        .unwrap();
        let merged_dest = &merged_desc["DeliveryStreamDescription"]["Destinations"][0];
        assert!(
            merged_dest
                .get("ExtendedS3DestinationDescription")
                .is_some(),
            "the ExtendedS3 slot never flips away (moto keeps both keys): {merged_dest}"
        );
        let ext = &merged_dest["ExtendedS3DestinationDescription"];
        assert_eq!(
            ext["BucketARN"],
            json!("arn:aws:s3:::plain"),
            "the S3Update's shared key replaces the stored value"
        );
        assert_eq!(
            ext["BufferingHints"]["IntervalInSeconds"],
            json!(120),
            "mirror-inherited keys survive (moto's models.py:663-664 merge              into the auto-created S3 slot; the wave-9 divergence's whole-             entry replace dropped them)"
        );
    }

    /// moto's exactly-one-destination rule on UpdateDestination
    /// (find_destination_config_in_args, firehose models.py:57-87).
    #[test]
    fn test_update_destination_exactly_one_config() {
        let handler = FirehoseHandler::new();
        handler.handle(make_req(
            "CreateDeliveryStream",
            json!({
                "DeliveryStreamName": "one-of",
                "ExtendedS3DestinationConfiguration": {
                    "BucketARN": "arn:aws:s3:::bkt",
                    "RoleARN": "arn:aws:iam::123456789012:role/r"
                }
            }),
        ));
        let none = handler.handle(make_req(
            "UpdateDestination",
            json!({"DeliveryStreamName": "one-of", "DestinationId": "destinationId-000000000001"}),
        ));
        assert_eq!(none.status, 400);
        assert_eq!(
            serde_json::from_str::<Value>(&none.body).unwrap()["message"],
            json!("Exactly one destination configuration is supported for a Firehose")
        );
    }

    /// The HttpEndpoint destination is really delivered: moto's envelope
    /// (requestId/timestamp/records[.data]) lands on the configured URL.
    #[test]
    fn test_http_endpoint_delivery_posts_envelope() {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            let (mut sock, _) = listener.accept().unwrap();
            let mut buf = [0u8; 16384];
            let mut raw: Vec<u8> = Vec::new();
            // Read until the JSON body is complete (Content-Length).
            loop {
                let n = sock.read(&mut buf).unwrap();
                if n == 0 {
                    break;
                }
                raw.extend_from_slice(&buf[..n]);
                let text = String::from_utf8_lossy(&raw);
                if let Some(idx) = text.find("\r\n\r\n") {
                    let headers = &text[..idx];
                    let clen = headers
                        .lines()
                        .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
                        .and_then(|l| l.split(':').nth(1))
                        .and_then(|v| v.trim().parse::<usize>().ok())
                        .unwrap_or(0);
                    if raw.len() >= idx + 4 + clen {
                        break;
                    }
                }
            }
            let body = |raw: &[u8]| -> serde_json::Value {
                let text = String::from_utf8_lossy(raw).to_string();
                let idx = text.find("\r\n\r\n").unwrap();
                match text
                    .find("Content-Length:")
                    .or_else(|| text.find("content-length:"))
                {
                    Some(_) => {
                        let body = &raw[idx + 4..];
                        serde_json::from_slice(body).unwrap()
                    }
                    None => serde_json::from_slice(&raw[idx + 4..]).unwrap(),
                }
            };
            sock.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK")
                .unwrap();
            body(&raw)
        });

        let handler = FirehoseHandler::new();
        handler.handle(make_req(
            "CreateDeliveryStream",
            json!({
                "DeliveryStreamName": "http-stream",
                "HttpEndpointDestinationConfiguration": {
                    "EndpointConfiguration": {
                        "Url": format!("http://127.0.0.1:{port}/endpoint"),
                        "Name": "local"
                    }
                }
            }),
        ));
        // Describe renders the HttpEndpoint destination member (moto's
        // `{key}DestinationDescription`).
        let desc = serde_json::from_str::<Value>(
            &handler
                .handle(make_req(
                    "DescribeDeliveryStream",
                    json!({"DeliveryStreamName": "http-stream"}),
                ))
                .body,
        )
        .unwrap();
        assert_eq!(
            desc["DeliveryStreamDescription"]["Destinations"][0]
                ["HttpEndpointDestinationDescription"]["EndpointConfiguration"]["Url"],
            json!(format!("http://127.0.0.1:{port}/endpoint"))
        );

        let resp = serde_json::from_str::<Value>(
            &handler
                .handle(make_req(
                    "PutRecordBatch",
                    json!({
                        "DeliveryStreamName": "http-stream",
                        "Records": [
                            {"Data": base64::engine::general_purpose::STANDARD.encode(b"one")},
                            {"Data": base64::engine::general_purpose::STANDARD.encode(b"two")}
                        ]
                    }),
                ))
                .body,
        )
        .unwrap();
        assert_eq!(resp["FailedPutCount"], json!(0));
        assert_eq!(resp["RequestResponses"].as_array().unwrap().len(), 2);
        for r in resp["RequestResponses"].as_array().unwrap() {
            assert_eq!(r["RecordId"].as_str().map(str::len), Some(36));
        }

        let received = server.join().unwrap();
        assert!(received["requestId"].as_str().map(str::len) == Some(36));
        assert!(received["timestamp"].is_i64());
        let records = received["records"].as_array().unwrap();
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(records[0]["data"].as_str().unwrap())
                .unwrap(),
            b"one"
        );
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(records[1]["data"].as_str().unwrap())
                .unwrap(),
            b"two"
        );
    }

    /// A failed HttpEndpoint delivery keeps the 200 (the crate's documented
    /// put-failure contract) and answers moto's RuntimeError literal with
    /// FailedPutCount = record count, surfaced on the stream too.
    #[test]
    fn test_http_endpoint_delivery_failure_is_honest() {
        let handler = FirehoseHandler::new();
        handler.handle(make_req(
            "CreateDeliveryStream",
            json!({
                "DeliveryStreamName": "http-fail",
                "HttpEndpointDestinationConfiguration": {
                    "EndpointConfiguration": {
                        "Url": "http://127.0.0.1:9/unreachable"
                    }
                }
            }),
        ));
        let resp = serde_json::from_str::<Value>(
            &handler
                .handle(make_req(
                    "PutRecordBatch",
                    json!({
                        "DeliveryStreamName": "http-fail",
                        "Records": [{"Data": "aGk="}, {"Data": "aGk="}]
                    }),
                ))
                .body,
        )
        .unwrap();
        assert_eq!(resp["FailedPutCount"], json!(2));
        let desc = serde_json::from_str::<Value>(
            &handler
                .handle(make_req(
                    "DescribeDeliveryStream",
                    json!({"DeliveryStreamName": "http-fail"}),
                ))
                .body,
        )
        .unwrap();
        assert_eq!(
            desc["DeliveryStreamDescription"]["FailureDescription"],
            json!({
                "Type": "UNKNOWN_ERROR",
                "Details": "Firehose PutRecord(Batch) to HTTP destination failed"
            })
        );
    }

    /// The HttpEndpoint stream's PutRecord without a bucket-level failure
    /// answers FailedPutCount 0 and a per-record RecordId (moto's success
    /// path shape).
    #[test]
    fn test_put_record_http_ok() {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            let (mut sock, _) = listener.accept().unwrap();
            let mut raw = Vec::new();
            let mut buf = [0u8; 8192];
            loop {
                let n = sock.read(&mut buf).unwrap();
                if n == 0 {
                    break;
                }
                raw.extend_from_slice(&buf[..n]);
                let text = String::from_utf8_lossy(&raw);
                if let Some(idx) = text.find("\r\n\r\n") {
                    let clen = text[..idx]
                        .split("\r\n")
                        .find_map(|l| {
                            let (k, v) = l.split_once(':')?;
                            k.eq_ignore_ascii_case("content-length")
                                .then(|| v.trim().parse::<usize>().ok())?
                        })
                        .unwrap_or(0);
                    if raw.len() >= idx + 4 + clen {
                        break;
                    }
                }
            }
            sock.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK")
                .unwrap();
        });
        let handler = FirehoseHandler::new();
        handler.handle(make_req(
            "CreateDeliveryStream",
            json!({
                "DeliveryStreamName": "http-put",
                "HttpEndpointDestinationConfiguration": {
                    "EndpointConfiguration": {"Url": format!("http://127.0.0.1:{port}/e")}
                }
            }),
        ));
        let resp = serde_json::from_str::<Value>(
            &handler
                .handle(make_req(
                    "PutRecord",
                    json!({
                        "DeliveryStreamName": "http-put",
                        "Record": {"Data": base64::engine::general_purpose::STANDARD.encode(b"hi")}
                    }),
                ))
                .body,
        )
        .unwrap();
        assert_eq!(resp["RecordId"].as_str().map(str::len), Some(36));
        assert_eq!(resp.get("FailedPutCount"), None);
        server.join().unwrap();
    }

    /// moto's per-region stream quota (firehose models.py:223-228): the
    /// 51st create raises LimitExceededException with moto's message.
    #[test]
    fn test_create_quota_limit() {
        let handler = FirehoseHandler::new();
        for i in 0..MAX_STREAMS_PER_REGION {
            let resp = handler.handle(make_req(
                "CreateDeliveryStream",
                json!({"DeliveryStreamName": format!("quota-{i:02}")}),
            ));
            assert_eq!(resp.status, 200, "stream {i} should create");
        }
        let over = handler.handle(make_req(
            "CreateDeliveryStream",
            json!({"DeliveryStreamName": "over-quota"}),
        ));
        assert_eq!(over.status, 400);
        assert!(over.body.contains("LimitExceededException"));
        assert!(over
            .body
            .contains("You have already consumed your firehose quota of 50 hoses."));
    }

    /// moto's ListDeliveryStreams (firehose models.py:344-382): the
    /// DeliveryStreamType filter narrows by the stream's type.
    #[test]
    fn test_list_streams_type_filter() {
        let handler = FirehoseHandler::new();
        handler.handle(make_req(
            "CreateDeliveryStream",
            json!({"DeliveryStreamName": "direct1"}),
        ));
        handler.handle(make_req(
            "CreateDeliveryStream",
            json!({"DeliveryStreamName": "kinesis1", "DeliveryStreamType": "KinesisStreamAsSource"}),
        ));
        let all = serde_json::from_str::<Value>(
            &handler
                .handle(make_req("ListDeliveryStreams", json!({})))
                .body,
        )
        .unwrap();
        assert_eq!(all["DeliveryStreamNames"].as_array().unwrap().len(), 2);
        let filtered = serde_json::from_str::<Value>(
            &handler
                .handle(make_req(
                    "ListDeliveryStreams",
                    json!({"DeliveryStreamType": "KinesisStreamAsSource"}),
                ))
                .body,
        )
        .unwrap();
        let names: Vec<&str> = filtered["DeliveryStreamNames"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n.as_str().unwrap())
            .collect();
        assert_eq!(names, vec!["kinesis1"]);
    }
}
