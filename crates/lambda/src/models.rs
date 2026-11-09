//! Lambda in-memory state models.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// Per-function resource policy (statements in insertion order).
#[derive(Debug, Serialize, Deserialize)]
pub struct FunctionPolicy {
    #[serde(with = "crate::snapshot::rw")]
    pub revision: RwLock<String>,
    #[serde(with = "crate::snapshot::rw")]
    pub statements: RwLock<Vec<Value>>,
}

impl FunctionPolicy {
    pub fn new() -> Self {
        Self {
            revision: RwLock::new(uuid::Uuid::new_v4().to_string()),
            statements: RwLock::new(Vec::new()),
        }
    }
}

impl Default for FunctionPolicy {
    fn default() -> Self {
        Self::new()
    }
}

/// Clone the contents of a `RwLock<T>` where `T: Clone`.
fn cl<T: Clone>(l: &RwLock<T>) -> T {
    l.read().clone()
}

/// A Lambda function (a "latest" record or a published-version snapshot).
#[derive(Debug, Serialize, Deserialize)]
pub struct LambdaFunction {
    pub function_name: String,
    /// Unversioned ARN for the latest record; published snapshots carry `:<N>`
    /// (see `with_version_set`).
    pub function_arn: String,
    #[serde(with = "crate::snapshot::rw")]
    pub runtime: RwLock<String>,
    #[serde(with = "crate::snapshot::rw")]
    pub handler: RwLock<String>,
    #[serde(with = "crate::snapshot::rw")]
    pub role: RwLock<String>,
    #[serde(with = "crate::snapshot::rw")]
    pub code_size: RwLock<u64>,
    #[serde(with = "crate::snapshot::rw")]
    pub description: RwLock<String>,
    #[serde(with = "crate::snapshot::rw")]
    pub timeout: RwLock<u32>,
    #[serde(with = "crate::snapshot::rw")]
    pub memory_size: RwLock<u32>,
    #[serde(with = "crate::snapshot::rw")]
    pub last_modified: RwLock<String>,
    #[serde(with = "crate::snapshot::rw")]
    pub state: RwLock<String>,
    #[serde(with = "crate::snapshot::rw")]
    pub version: RwLock<String>,
    #[serde(with = "crate::snapshot::rw")]
    pub tags: RwLock<HashMap<String, String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub code_sha256: RwLock<String>,
    /// Raw zipped deployment package bytes.
    #[serde(with = "crate::snapshot::rw_b64")]
    pub code_zip: RwLock<Vec<u8>>,
    #[serde(with = "crate::snapshot::rw")]
    pub environment: RwLock<Option<HashMap<String, String>>>,
    /// Layer attachments as `[{"Arn": ..., "CodeSize": N}, ...]`.
    #[serde(with = "crate::snapshot::rw")]
    pub layers: RwLock<Vec<Value>>,
    #[serde(with = "crate::snapshot::rw")]
    pub reserved_concurrency: RwLock<Option<i64>>,
    #[serde(with = "crate::snapshot::rw")]
    pub code_or_config_updated: RwLock<bool>,
    #[serde(with = "crate::snapshot::rw")]
    pub event_invoke_config: RwLock<Option<Value>>,
    /// DeadLetterConfig (`{"TargetArn": ...}`) for failed async invocations.
    #[serde(with = "crate::snapshot::rw")]
    pub dead_letter_config: RwLock<Option<Value>>,
    #[serde(with = "crate::snapshot::rw")]
    pub code_signing_config_arn: RwLock<Option<String>>,
    /// Runtime management config (UpdateRuntimeOn / RuntimeVersionArn).
    #[serde(with = "crate::snapshot::rw")]
    pub runtime_mgmt_config: RwLock<Option<Value>>,
    /// Recursive loop setting: "Allow" | "Terminate" (default "Terminate").
    #[serde(with = "crate::snapshot::rw")]
    pub recursion_config: RwLock<String>,
    /// Scaling configs keyed by qualifier.
    #[serde(with = "crate::snapshot::rw")]
    pub scaling_configs: RwLock<HashMap<String, Value>>,
    /// VpcConfig as passed by the caller (moto echoes it; None when absent).
    #[serde(with = "crate::snapshot::rw")]
    pub vpc_config: RwLock<Option<Value>>,
    pub policy: Arc<FunctionPolicy>,
}

impl LambdaFunction {
    pub fn new(account: u64, region: &str, function_name: String) -> Self {
        Self {
            function_name: function_name.clone(),
            function_arn: format!("arn:aws:lambda:{region}:{account}:function:{function_name}"),
            runtime: RwLock::new("python3.12".to_string()),
            handler: RwLock::new("index.handler".to_string()),
            role: RwLock::new(String::new()),
            code_size: RwLock::new(0),
            description: RwLock::new(String::new()),
            timeout: RwLock::new(3),
            memory_size: RwLock::new(128),
            last_modified: RwLock::new(iso_now()),
            state: RwLock::new("Active".to_string()),
            version: RwLock::new("$LATEST".to_string()),
            tags: RwLock::new(HashMap::new()),
            code_sha256: RwLock::new(String::new()),
            code_zip: RwLock::new(Vec::new()),
            environment: RwLock::new(None),
            layers: RwLock::new(Vec::new()),
            reserved_concurrency: RwLock::new(None),
            code_or_config_updated: RwLock::new(false),
            event_invoke_config: RwLock::new(None),
            dead_letter_config: RwLock::new(None),
            code_signing_config_arn: RwLock::new(None),
            runtime_mgmt_config: RwLock::new(None),
            recursion_config: RwLock::new("Terminate".to_string()),
            scaling_configs: RwLock::new(HashMap::new()),
            vpc_config: RwLock::new(None),
            policy: Arc::new(FunctionPolicy::new()),
        }
    }

    /// Snapshot copy with a numeric published version and a versioned ARN
    /// (mirrors moto's `set_version`).
    pub fn with_version_set(&self, version: i64) -> LambdaFunction {
        LambdaFunction {
            function_name: self.function_name.clone(),
            function_arn: format!("{}:{}", self.function_arn, version),
            runtime: RwLock::new(cl(&self.runtime)),
            handler: RwLock::new(cl(&self.handler)),
            role: RwLock::new(cl(&self.role)),
            code_size: RwLock::new(*self.code_size.read()),
            description: RwLock::new(cl(&self.description)),
            timeout: RwLock::new(*self.timeout.read()),
            memory_size: RwLock::new(*self.memory_size.read()),
            last_modified: RwLock::new(iso_now()),
            state: RwLock::new(cl(&self.state)),
            version: RwLock::new(version.to_string()),
            tags: RwLock::new(cl(&self.tags)),
            code_sha256: RwLock::new(cl(&self.code_sha256)),
            code_zip: RwLock::new(self.code_zip.read().clone()),
            environment: RwLock::new(self.environment.read().clone()),
            layers: RwLock::new(self.layers.read().clone()),
            reserved_concurrency: RwLock::new(*self.reserved_concurrency.read()),
            code_or_config_updated: RwLock::new(false),
            event_invoke_config: RwLock::new(self.event_invoke_config.read().clone()),
            dead_letter_config: RwLock::new(self.dead_letter_config.read().clone()),
            code_signing_config_arn: RwLock::new(cl(&self.code_signing_config_arn)),
            runtime_mgmt_config: RwLock::new(self.runtime_mgmt_config.read().clone()),
            recursion_config: RwLock::new(cl(&self.recursion_config)),
            scaling_configs: RwLock::new(self.scaling_configs.read().clone()),
            vpc_config: RwLock::new(self.vpc_config.read().clone()),
            policy: Arc::clone(&self.policy),
        }
    }
}

/// One entry per function name: latest + published versions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionEntry {
    pub latest: Option<Arc<LambdaFunction>>,
    pub versions: Vec<Arc<LambdaFunction>>,
}

/// A Lambda alias.
#[derive(Debug, Serialize, Deserialize)]
pub struct LambdaAlias {
    pub name: String,
    /// Alias ARN: `<base function arn>:<alias name>`.
    pub function_arn: String,
    #[serde(with = "crate::snapshot::rw")]
    pub function_version: RwLock<String>,
    #[serde(with = "crate::snapshot::rw")]
    pub description: RwLock<String>,
    #[serde(with = "crate::snapshot::rw")]
    pub modified: RwLock<String>,
    #[serde(with = "crate::snapshot::rw")]
    pub routing_config: RwLock<Option<Value>>,
    pub revision_id: String,
}

/// An event source mapping (native store mirroring the Python provider keys).
#[derive(Debug, Serialize, Deserialize)]
pub struct EventSourceMapping {
    pub uuid: String,
    pub function_arn: String,
    pub event_source_arn: String,
    pub state: String,
    pub state_transition_reason: String,
    pub last_modified: f64,
    pub batch_size: u32,
    pub enabled: bool,
    /// Only set for stream sources (kinesis / dynamodb streams); moto stores
    /// it and defaults non-SQS sources to TRIM_HORIZON.
    #[serde(default)]
    pub starting_position: Option<String>,
    /// `{"Filters": [{"Pattern": "…"}]}` when the caller passed one.
    #[serde(default)]
    pub filter_criteria: Option<Value>,
    /// Non-empty only with `FunctionResponseTypes` (e.g. ReportBatchItemFailures).
    #[serde(default)]
    pub function_response_types: Option<Vec<String>>,
    /// Retry each half of a failed batch once. `None` = member unset; moto's
    /// configuration dict filters unset members, so an explicit `false`
    /// still renders (only unconfigured values drop).
    #[serde(default)]
    pub bisect_batch_on_function_error: Option<bool>,
    /// Retry budget; `None` = unconfigured (0..=2 valid, real-AWS `-1` =
    /// never give up rides through and renders like moto's stored value).
    #[serde(default)]
    pub maximum_retry_attempts: Option<i64>,
    /// Per-mapping tags (moto stores them; the TF provider refresh lists
    /// tags on the mapping resource). Snapshot serialized via the shared
    /// rw helper like the other thread-map fields.
    #[serde(default, with = "crate::snapshot::rw")]
    pub tags: RwLock<HashMap<String, String>>,
    /// `MaximumBatchingWindowInSeconds` when the caller configured one —
    /// omitted from the wire dict when un-set (moto shape-filters the
    /// 0-valued members out of the details dict).
    #[serde(default)]
    pub maximum_batching_window_in_seconds: Option<u64>,
    /// `MaximumRecordAgeInSeconds` when configured (stream sources; the
    /// real-AWS `-1` = never-expire sentinel rides through as-is); same
    /// conditional shape as the batching window above.
    #[serde(default)]
    pub maximum_record_age_in_seconds: Option<i64>,
    /// `StartingPositionTimestamp` when the caller passed one
    /// (AT_TIMESTAMP mappings; botocore carries the member on create +
    /// output but NOT on update — moto's update loop has no handler for
    /// it, so it is create-immutable, matching both oracles).
    #[serde(default)]
    pub starting_position_timestamp: Option<f64>,
    /// `ParallelizationFactor` for kinesis mappings (real-AWS range
    /// 1..=100000; nothing in moto or the twin validates it — moto
    /// `spec.get` pass-through like the sibling ratio fields).
    #[serde(default)]
    pub parallelization_factor: Option<i64>,
    /// `DestinationConfig` on-failure destination as passed (moto
    /// `spec.get`; the twin renders the stored shape unvalidated).
    #[serde(default)]
    pub destination_config: Option<Value>,
    /// `ScalingConfig` as passed (SQS mappings; moto `spec.get`).
    #[serde(default)]
    pub scaling_config: Option<Value>,
    /// `TumblingWindowInSeconds` when configured (kinesis; moto `spec.get`).
    #[serde(default)]
    pub tumbling_window_in_seconds: Option<u64>,
    /// Wave-2 `spec.get` members (moto models.py:1331-1347, rendered in
    /// the configuration dict 1414-1428; the update loop handles all but
    /// `Queues` — botocore omits Queues from the update input, so it is
    /// create-immutable like StartingPositionTimestamp):
    /// `KMSKeyArn` (plain string), `MetricsConfig`,
    /// `ProvisionedPollerConfig`, `Queues`, `SourceAccessConfigurations`
    /// (list), `DocumentDBEventSourceConfig` — stored unvalidated.
    #[serde(default)]
    pub kms_key_arn: Option<String>,
    #[serde(default)]
    pub metrics_config: Option<Value>,
    #[serde(default)]
    pub provisioned_poller_config: Option<Value>,
    #[serde(default)]
    pub queues: Option<Value>,
    #[serde(default)]
    pub source_access_configurations: Option<Value>,
    #[serde(default)]
    pub document_db_config: Option<Value>,
    /// Kafka/MQ members moto stores verbatim at create and never updates
    /// (live-probed: botocore's UpdateEventSourceMapping rejects Topics +
    /// SelfManagedEventSource client-side, and moto's update loop ignores
    /// both `*EventSourceConfig` groups — so all four are create-immutable).
    /// `SelfManagedKafkaEventSourceConfig` carries ConsumerGroupId and
    /// SchemaRegistryConfig nested as passed. botocore's local lambda model
    /// has no `LogFormat` in LoggingConfig and moto drops LoggingConfig on
    /// ESMs entirely (both engines render nothing) — out of scope here.
    #[serde(default)]
    pub topics: Option<Value>,
    #[serde(default)]
    pub self_managed_event_source: Option<Value>,
    #[serde(default)]
    pub self_managed_kafka_event_source_config: Option<Value>,
    #[serde(default)]
    pub amazon_managed_kafka_event_source_config: Option<Value>,
    /// Process-lifetime creation counter (AtomicU64 in the handler): the
    /// ONLY ordering source for list_event_source_mappings, mirroring
    /// moto's insertion-ordered backend dict (live-probed: moto lists
    /// alpha..echo in create order; the twin's HashMap iteration is
    /// random). serde-default 0 for pre-PR snapshots; fresh creates
    /// start at 1 so they sort above them. Restore-ordering is honest
    /// for same-version snapshots (true seqs); cross-version restore
    /// sorts at no worse than the pre-PR random order.
    #[serde(default)]
    pub created_seq: u64,
    /// moto stores the RAW create-time FunctionName string
    /// (awslambda/models.py:1306-1313: `self.function_name =
    /// spec["FunctionName"]`) and the list filter compares against that
    /// exact string (2274-2275) — an ARN- or qualifier-created mapping
    /// matches future lists only by THAT form (live-probed by the #485
    /// harness: fullarn_B moto 1 vs extracted-base twin 0). Updates
    /// never touch it: an update's FunctionName only re-points
    /// function_arn (models.py:2225-2229), so the raw string is
    /// frozen-at-create. serde-default None keeps pre-PR snapshots
    /// loadable; the None path falls back to the base-name extraction
    /// (the #485 predicate) so restored mappings keep matching.
    #[serde(default)]
    pub function_name: Option<String>,
    /// Attach-time per-shard watermarks for a `LATEST` kinesis mapping:
    /// shard id → the sequence number that the stream's next put-record will
    /// take, frozen when CreateEventSourceMapping registered the mapping
    /// (moto registers the ESM on the stream synchronously
    /// — moto/kinesis/models.py KinesisStream.lambda_event_source_mappings,
    /// pushed on put_record — so a LATEST mapping only ever sees records
    /// assigned after attach). Carried inside the mapping so the engine's
    /// first sight reads it atomically with the mapping's publication — no
    /// publish-then-bind window for a poll tick to run through, and state
    /// save/restore carries it along. LATEST-only: TRIM_HORIZON keeps its
    /// replay-from-zero contract (real AWS and recorded behavior).
    #[serde(default)]
    pub kinesis_bound_watermarks: Option<std::collections::BTreeMap<String, String>>,
    /// Attach-time seed for a `LATEST` dynamodb-streams mapping: the newest
    /// stream-capture sequence at attach, frozen the same way the kinesis
    /// watermarks are — the engine's first poll skips every record at or
    /// below it instead of replaying the pre-attach history. Real Lambda
    /// delivers nothing for new mappings until a write happens.
    #[serde(default)]
    pub ddb_attached_watermark: Option<String>,
}

impl Clone for EventSourceMapping {
    /// Manual clone like LambdaFunction: the tags map lives behind a
    /// RwLock, so derive(Clone) can't apply — clone through the read guard.
    fn clone(&self) -> Self {
        Self {
            uuid: self.uuid.clone(),
            function_arn: self.function_arn.clone(),
            event_source_arn: self.event_source_arn.clone(),
            state: self.state.clone(),
            state_transition_reason: self.state_transition_reason.clone(),
            last_modified: self.last_modified,
            batch_size: self.batch_size,
            enabled: self.enabled,
            starting_position: self.starting_position.clone(),
            filter_criteria: self.filter_criteria.clone(),
            function_response_types: self.function_response_types.clone(),
            bisect_batch_on_function_error: self.bisect_batch_on_function_error,
            maximum_retry_attempts: self.maximum_retry_attempts,
            tags: RwLock::new(self.tags.read().clone()),
            maximum_batching_window_in_seconds: self.maximum_batching_window_in_seconds,
            maximum_record_age_in_seconds: self.maximum_record_age_in_seconds,
            starting_position_timestamp: self.starting_position_timestamp,
            parallelization_factor: self.parallelization_factor,
            destination_config: self.destination_config.clone(),
            scaling_config: self.scaling_config.clone(),
            tumbling_window_in_seconds: self.tumbling_window_in_seconds,
            kms_key_arn: self.kms_key_arn.clone(),
            metrics_config: self.metrics_config.clone(),
            provisioned_poller_config: self.provisioned_poller_config.clone(),
            queues: self.queues.clone(),
            source_access_configurations: self.source_access_configurations.clone(),
            document_db_config: self.document_db_config.clone(),
            topics: self.topics.clone(),
            self_managed_event_source: self.self_managed_event_source.clone(),
            self_managed_kafka_event_source_config: self
                .self_managed_kafka_event_source_config
                .clone(),
            amazon_managed_kafka_event_source_config: self
                .amazon_managed_kafka_event_source_config
                .clone(),
            created_seq: self.created_seq,
            function_name: self.function_name.clone(),
            kinesis_bound_watermarks: self.kinesis_bound_watermarks.clone(),
            ddb_attached_watermark: self.ddb_attached_watermark.clone(),
        }
    }
}

/// One stored ESM as seen by the gateway's engine thread, tagged with the
/// (account, region) it lives in (the handler keeps states keyed by both).
#[derive(Debug, Clone)]
pub struct EsmMappingEntry {
    pub account: u64,
    pub region: String,
    pub mapping: EventSourceMapping,
    /// The mapped function's execution role — the `invokeIdentityArn` the
    /// kinesis envelope carries (moto builds it from `func.role`). `None`
    /// (or empty) falls back to the function ARN in the engine.
    pub role: Option<String>,
}

/// Result of an engine-triggered invocation (ESM delivery).
pub struct EngineInvokeResult {
    /// `Some("Handled")` when the handler raised / timed out; otherwise None.
    pub function_error: Option<String>,
    /// Raw payload body (a JSON string or "null").
    pub payload: String,
}

/// A single published layer version.
#[derive(Debug, Serialize, Deserialize)]
pub struct LayerVersion {
    pub layer_arn: String,
    pub layer_name: String,
    pub version: u32,
    pub description: String,
    pub created: String,
    pub content_size: u64,
    pub code_sha256: String,
    pub compatible_runtimes: Vec<String>,
    pub compatible_architectures: Vec<String>,
    pub license_info: String,
    #[serde(with = "crate::snapshot::b64")]
    pub code_zip: Vec<u8>,
    pub policy: Arc<FunctionPolicy>,
}

/// A layer and its published versions.
#[derive(Debug, Serialize, Deserialize)]
pub struct Layer {
    pub layer_arn: String,
    pub layer_name: String,
    pub latest_version: u32,
    pub versions: Vec<Arc<LayerVersion>>,
}

/// A stored code-signing config.
#[derive(Debug, Serialize, Deserialize)]
pub struct CodeSigningConfig {
    pub arn: String,
    pub description: String,
    pub allowed_publishers: Value,
    pub policies: Option<Value>,
    pub last_modified: String,
}

/// The Lambda state store (per account+region).
#[derive(Debug, Clone)]
pub struct LambdaState {
    pub functions: Arc<RwLock<HashMap<String, FunctionEntry>>>,
    pub aliases: AliasMap,
    pub event_source_mappings: Arc<RwLock<HashMap<String, Arc<EventSourceMapping>>>>,
    pub layers: Arc<RwLock<HashMap<String, Layer>>>,
    pub url_configs: Arc<RwLock<HashMap<String, Value>>>,
    pub provisioned: ProvisionedMap,
    pub code_signing_configs: Arc<RwLock<HashMap<String, Arc<CodeSigningConfig>>>>,
    pub layer_permissions: LayerPermissionMap,
}

/// Alias table keyed by (function name, alias name).
pub type AliasMap = Arc<RwLock<HashMap<(String, String), Arc<LambdaAlias>>>>;

/// Provisioned concurrency configs keyed by (function name, qualifier).
pub type ProvisionedMap = Arc<RwLock<HashMap<(String, String), Value>>>;

/// Layer-version policies keyed by (layer name, version) → Sid → statement.
pub type LayerPermissionMap = Arc<RwLock<HashMap<(String, i64), BTreeMap<String, Value>>>>;

impl LambdaState {
    pub fn new() -> Self {
        Self {
            functions: Arc::new(RwLock::new(HashMap::new())),
            aliases: Arc::new(RwLock::new(HashMap::new())),
            event_source_mappings: Arc::new(RwLock::new(HashMap::new())),
            layers: Arc::new(RwLock::new(HashMap::new())),
            url_configs: Arc::new(RwLock::new(HashMap::new())),
            provisioned: Arc::new(RwLock::new(HashMap::new())),
            code_signing_configs: Arc::new(RwLock::new(HashMap::new())),
            layer_permissions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Name-only form of a name / ARN expression (strips arn prefix and
    /// qualifier).
    pub fn norm_name(name: &str) -> String {
        if let Some(rest) = name.strip_prefix("arn:aws:lambda:") {
            if let Some(idx) = rest.find(":function:") {
                return rest[idx + ":function:".len()..]
                    .split(':')
                    .next()
                    .unwrap_or("")
                    .to_string();
            }
        }
        name.split(':').next().unwrap_or(name).to_string()
    }

    /// Qualifier suffix from a name/ARN expression (`name:qualifier` or the
    /// portion after `:function:` in an ARN).
    pub fn expr_qualifier(name: &str) -> Option<String> {
        if let Some(rest) = name.strip_prefix("arn:aws:lambda:") {
            if let Some(idx) = rest.find(":function:") {
                let tail = &rest[idx + ":function:".len()..];
                let mut parts = tail.split(':');
                parts.next();
                return parts.next().filter(|q| !q.is_empty()).map(String::from);
            }
            return None;
        }
        let idx = name.find(':')?;
        let q = &name[idx + 1..];
        (!q.is_empty()).then(|| q.to_string())
    }

    /// Qualifier slot presence on a function expression (the FORBID guard's
    /// shape test). Unlike `expr_qualifier`, an EMPTY suffix counts: moto
    /// refuses a single-colon bare name on shape before any existence check
    /// (`name.count(":") == 1`, models.py:1617-1618) and an ARN with a
    /// trailing colon yields qualifier `""` which is *not None* → the same
    /// 400 (models.py:1697-1700 + 1716-1717). `None` for plain names and
    /// plain ARNs. (A many-colon bare name is moto's unknown-function 404
    /// instead — w6 F6 edge, recorded, out of this fix's scope.)
    pub fn qualifier_slot(name: &str) -> Option<String> {
        if let Some(rest) = name.strip_prefix("arn:aws:lambda:") {
            if let Some(idx) = rest.find(":function:") {
                let tail = &rest[idx + ":function:".len()..];
                return tail.split_once(':').map(|(_, q)| q.to_string());
            }
            return None;
        }
        name.split_once(':').map(|(_, q)| q.to_string())
    }

    /// Resolve latest by name or full ARN (no qualifier).
    pub fn get_function(&self, name: &str) -> Option<Arc<LambdaFunction>> {
        self.get_function_with_qualifier(name, None)
    }

    /// Resolve by name + optional qualifier ("$LATEST" or a published version).
    pub fn get_function_with_qualifier(
        &self,
        name: &str,
        qualifier: Option<&str>,
    ) -> Option<Arc<LambdaFunction>> {
        let name = Self::norm_name(name);
        let expr_qual = Self::expr_qualifier(&name);
        let qualifier = qualifier.or(expr_qual.as_deref());
        let funcs = self.functions.read();
        let entry = funcs.get(&name)?;
        match qualifier {
            None | Some("$LATEST") => entry.latest.clone(),
            Some(q) => entry
                .versions
                .iter()
                .find(|v| *v.version.read() == q)
                .cloned(),
        }
    }

    /// Resolve a published-layer-version ARN (`...:layer:name:N`).
    pub fn layers_versions_by_arn(&self, arn: &str) -> Option<Arc<LayerVersion>> {
        let parts: Vec<&str> = arn.split(':').collect();
        // arn:aws:lambda:REGION:ACCOUNT:layer:NAME:VERSION
        if parts.len() < 8 || parts[5] != "layer" {
            return None;
        }
        let name = parts[6];
        let version: i64 = parts[7].parse().ok()?;
        self.layers
            .read()
            .get(name)?
            .versions
            .iter()
            .find(|v| i64::from(v.version) == version)
            .cloned()
    }

    /// Look up an alias for a function.
    pub fn resolve_alias(&self, name: &str, alias: &str) -> Option<Arc<LambdaAlias>> {
        self.aliases
            .read()
            .get(&(Self::norm_name(name), alias.to_string()))
            .cloned()
    }
}

impl Default for LambdaState {
    fn default() -> Self {
        Self::new()
    }
}

/// ISO-8601 timestamp with microsecond precision and timezone
/// (mirrors moto's `iso_8601_datetime_with_nanoseconds`).
pub fn iso_now() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S.%6f+00:00")
        .to_string()
}

/// Layer/URL-config date: "2021-01-01T00:00:00.000+0000".
pub fn layer_date_now() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S.%3f+0000")
        .to_string()
}

/// URL config date (same format as layers).
pub fn url_date_now() -> String {
    layer_date_now()
}

/// Provisioned-concurrency date: "2021-01-01T00:00:00Z".
pub fn utc_date_now() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}
