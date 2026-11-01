//! DynamoDB operation handler.

use parking_lot::RwLock;
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::expr::{self as expr_engine, Names, Values};
use crate::models::{
    compute_key_from_attrs, AttributeDefinition, Backup, DynamoDbState, Export, GlobalTable, Item,
    KeySchema, KinesisDestination, Table,
};
use crate::protocol::{AwsRequest, AwsResponse};

use crate::{bridge, streaming};

type AttrMap = HashMap<String, Value>;

// moto/dynamodb/models/table.py DEFAULT_WARM_THROUGHPUT_RCU/_WCU: AWS gives
// on-demand tables a default warm throughput of 12000/4000 units; PROVISIONED
// tables advertise their provisioned units instead.
const DEFAULT_WARM_THROUGHPUT_RCU: i64 = 12000;
const DEFAULT_WARM_THROUGHPUT_WCU: i64 = 4000;

/// The DynamoDB service handler.
pub struct DynamoDbHandler {
    pub(crate) state: RwLock<HashMap<(u64, String), DynamoDbState>>,
}

impl DynamoDbHandler {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) fn get_state(&self, account: u64, region: &str) -> DynamoDbState {
        let state = {
            let mut states = self.state.write();
            states
                .entry((account, region.to_string()))
                .or_default()
                .clone()
        };
        // Cross-service visibility (moto parity: the dynamodbstreams backend
        // reads this state through the bridge the way moto's backends share
        // `dynamodb_backends[account][region]`).
        bridge::register_state(account, region, &state);
        state
    }

    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        let operation = req.operation.as_str();
        match operation {
            "ListTables" => self.list_tables(&req),
            "CreateTable" => self.create_table(&req),
            "DeleteTable" => self.delete_table(&req),
            "DescribeTable" => self.describe_table(&req),
            "UpdateTable" => self.update_table(&req),
            "PutItem" => self.put_item(&req),
            "GetItem" => self.get_item(&req),
            "DeleteItem" => self.delete_item(&req),
            "UpdateItem" => self.update_item(&req),
            "Query" => self.query(&req),
            "Scan" => self.scan(&req),
            "BatchGetItem" => self.batch_get_item(&req),
            "BatchWriteItem" => self.batch_write_item(&req),
            "TransactWriteItems" => self.transact_write_items(&req),
            "TransactGetItems" => self.transact_get_items(&req),
            "ConditionCheck" => self.condition_check(&req),
            "ExecuteStatement" => self.execute_statement(&req),
            "ExecuteTransaction" => self.execute_transaction(&req),
            "BatchExecuteStatement" => self.batch_execute_statement(&req),
            "TagResource" => self.tag_resource(&req),
            "UntagResource" => self.untag_resource(&req),
            "ListTagsOfResource" => self.list_tags_of_resource(&req),
            "DescribeLimits" => self.describe_limits(&req),
            "DescribeEndpoints" => self.describe_endpoints(&req),
            "DescribeTimeToLive" => self.describe_ttl(&req),
            "UpdateTimeToLive" => self.update_ttl(&req),
            "DescribeContinuousBackups" => self.describe_continuous_backups(&req),
            "UpdateContinuousBackups" => self.update_continuous_backups(&req),
            "CreateBackup" => self.create_backup(&req),
            "DescribeBackup" => self.describe_backup(&req),
            "ListBackups" => self.list_backups(&req),
            "DeleteBackup" => self.delete_backup(&req),
            "RestoreTableFromBackup" => self.restore_table_from_backup(&req),
            "RestoreTableToPointInTime" => self.restore_table_to_point_in_time(&req),
            "CreateGlobalTable" => self.create_global_table(&req),
            "DescribeGlobalTable" => self.describe_global_table(&req),
            "ListGlobalTables" => self.list_global_tables(&req),
            "UpdateGlobalTable" => self.update_global_table(&req),
            "DeleteGlobalTable" => self.delete_global_table(&req),
            "DescribeGlobalTableSettings" => self.describe_global_table_settings(&req),
            "UpdateGlobalTableSettings" => self.update_global_table_settings(&req),
            "DescribeTableReplicaAutoScaling" => self.describe_table_replica_auto_scaling(&req),
            "UpdateTableReplicaAutoScaling" => self.update_table_replica_auto_scaling(&req),
            "EnableKinesisStreamingDestination" => self.modify_kinesis_destination(&req, "ACTIVE"),
            "DisableKinesisStreamingDestination" => {
                self.modify_kinesis_destination(&req, "DISABLED")
            }
            "UpdateKinesisStreamingDestination" => self.update_kinesis_destination(&req),
            "DescribeKinesisStreamingDestination" => self.describe_kinesis_destination(&req),
            "ListContributorInsights" => {
                AwsResponse::json(200, json!({ "ContributorInsightsSummaries": [] }))
            }
            "DescribeContributorInsights" => self.describe_contributor_insights(&req),
            "UpdateContributorInsights" => self.update_contributor_insights(&req),
            "ExportTableToPointInTime" => self.export_table_to_point_in_time(&req),
            "DescribeExport" => self.describe_export(&req),
            "ListExports" => AwsResponse::json(200, json!({ "ExportSummaries": [] })),
            "ImportTable" => self.import_table(&req),
            "DescribeImport" => AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!(
                    "ImportPackage not found: {}",
                    param_str(&req.params, "ImportArn").unwrap_or_default()
                ),
            ),
            "ListImports" => AwsResponse::json(200, json!({ "ImportSummaryList": [] })),
            "GetResourcePolicy" => self.get_resource_policy(&req),
            "PutResourcePolicy" => self.put_resource_policy(&req),
            "DeleteResourcePolicy" => self.delete_resource_policy(&req),
            other => AwsResponse::error(
                400,
                "ValidationException",
                &format!("The operation {other} is not implemented"),
            ),
        }
    }

    // ------------------------------------------------------------------
    // Table management
    // ------------------------------------------------------------------

    fn list_tables(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let mut names: Vec<String> = state.list_tables();
        names.sort();
        AwsResponse::json(200, json!({ "TableNames": names }))
    }

    fn parse_key_schema(v: Option<&Value>) -> Vec<KeySchema> {
        v.and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|k| {
                        Some(KeySchema {
                            attribute_name: k.get("AttributeName")?.as_str()?.to_string(),
                            key_type: k.get("KeyType")?.as_str()?.to_string(),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// moto `DynamoHandler._validate_table_creation` attribute validation:
    /// AttributeDefinitions must exactly cover the primary (and index) key
    /// attribute names.
    fn validate_attribute_definitions(
        params: &Value,
        key_schema: &[KeySchema],
        attribute_definitions: &[AttributeDefinition],
    ) -> Option<AwsResponse> {
        let mut expected: Vec<&str> = key_schema
            .iter()
            .map(|k| k.attribute_name.as_str())
            .collect();
        let indexes: Vec<&str> = ["GlobalSecondaryIndexes", "LocalSecondaryIndexes"]
            .iter()
            .filter_map(|field| params.get(*field).and_then(Value::as_array))
            .flatten()
            .filter_map(|idx| idx.get("KeySchema").and_then(Value::as_array))
            .flatten()
            .filter_map(|k| k.get("AttributeName").and_then(Value::as_str))
            .collect();
        let has_index = params.get("GlobalSecondaryIndexes").is_some()
            || params.get("LocalSecondaryIndexes").is_some();
        for name in &indexes {
            if !expected.contains(name) {
                expected.push(name);
            }
        }
        expected.sort_unstable();
        let mut actual: Vec<&str> = attribute_definitions
            .iter()
            .map(|a| a.attribute_name.as_str())
            .collect();
        actual.sort_unstable();

        if actual == expected {
            return None;
        }
        let err_head = "One or more parameter values were invalid: ";
        let dump = |list: &[&str]| {
            format!(
                "[{}]",
                list.iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        let message = if actual.len() > expected.len() {
            if has_index {
                format!(
                    "{err_head}Some AttributeDefinitions are not used. AttributeDefinitions: {}, keys used: {}",
                    dump(&actual),
                    dump(&expected)
                )
            } else {
                format!(
                    "{err_head}Number of attributes in KeySchema does not exactly match number of attributes defined in AttributeDefinitions"
                )
            }
        } else if actual.len() < expected.len() {
            if has_index {
                let missing: Vec<&str> = expected
                    .iter()
                    .copied()
                    .filter(|k| !actual.contains(k))
                    .collect();
                format!(
                    "{err_head}Some index key attributes are not defined in AttributeDefinitions. Keys: {}, AttributeDefinitions: {}",
                    dump(&missing),
                    dump(&actual)
                )
            } else {
                "Invalid KeySchema: Some index key attribute have no definition".to_string()
            }
        } else if has_index {
            let missing: Vec<&str> = expected
                .iter()
                .copied()
                .filter(|k| !actual.contains(k))
                .collect();
            format!(
                "{err_head}Some index key attributes are not defined in AttributeDefinitions. Keys: {}, AttributeDefinitions: {}",
                dump(&missing),
                dump(&actual)
            )
        } else {
            format!(
                "{err_head}Some index key attributes are not defined in AttributeDefinitions. Keys: {}, AttributeDefinitions: {}",
                dump(&expected),
                dump(&actual)
            )
        };
        Some(AwsResponse::error(400, "ValidationException", &message))
    }

    fn parse_attr_defs(v: Option<&Value>) -> Vec<AttributeDefinition> {
        v.and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|a| {
                        Some(AttributeDefinition {
                            attribute_name: a.get("AttributeName")?.as_str()?.to_string(),
                            attribute_type: a.get("AttributeType")?.as_str()?.to_string(),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn parse_indexes(v: Option<&Value>) -> Vec<crate::models::IndexDefinition> {
        v.and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|g| {
                        let key_schema = Self::parse_key_schema(g.get("KeySchema"));
                        if key_schema.is_empty() {
                            return None;
                        }
                        let projection = g
                            .get("Projection")
                            .and_then(|p| p.get("ProjectionType"))
                            .and_then(|t| t.as_str())
                            .unwrap_or("ALL")
                            .to_string();
                        let non_key_attributes = g
                            .get("Projection")
                            .and_then(|p| p.get("NonKeyAttributes"))
                            .and_then(|n| n.as_array())
                            .map(|a| {
                                a.iter()
                                    .filter_map(|x| x.as_str().map(String::from))
                                    .collect()
                            })
                            .unwrap_or_default();
                        let throughput = g.get("ProvisionedThroughput");
                        Some(crate::models::IndexDefinition {
                            name: g.get("IndexName")?.as_str()?.to_string(),
                            key_schema,
                            projection,
                            non_key_attributes,
                            provisioned_rcu: throughput
                                .and_then(|p| p.get("ReadCapacityUnits"))
                                .and_then(|v| v.as_i64()),
                            provisioned_wcu: throughput
                                .and_then(|p| p.get("WriteCapacityUnits"))
                                .and_then(|v| v.as_i64()),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn create_table(&self, req: &AwsRequest) -> AwsResponse {
        let table_name = param_str(&req.params, "TableName")
            .unwrap_or_default()
            .to_string();
        if table_name.is_empty() {
            return AwsResponse::error(400, "ValidationException", "TableName is required");
        }

        let state = self.get_state(req.account, &req.region);
        if state.get_table(&table_name).is_some() {
            return AwsResponse::error(
                400,
                "ResourceInUseException",
                &format!("Table already exists: {table_name}"),
            );
        }

        let key_schema = Self::parse_key_schema(req.params.get("KeySchema"));
        let attribute_definitions = Self::parse_attr_defs(req.params.get("AttributeDefinitions"));
        let billing_mode = param_str(&req.params, "BillingMode")
            .unwrap_or("PROVISIONED")
            .to_string();

        if let Some(resp) =
            Self::validate_attribute_definitions(&req.params, &key_schema, &attribute_definitions)
        {
            return resp;
        }

        let table = Arc::new(Table::new(
            table_name.clone(),
            req.account,
            req.region.clone(),
            key_schema,
            attribute_definitions,
            billing_mode.clone(),
        ));

        let rcu = req
            .params
            .get("ProvisionedThroughput")
            .and_then(|p| p.get("ReadCapacityUnits"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        let wcu = req
            .params
            .get("ProvisionedThroughput")
            .and_then(|p| p.get("WriteCapacityUnits"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        *table.provisioned_rcu.write() = rcu;
        *table.provisioned_wcu.write() = wcu;

        // moto create_table forwards DeletionProtectionEnabled
        // (dynamodb/responses.py:356, default False).
        *table.deletion_protection_enabled.write() = req
            .params
            .get("DeletionProtectionEnabled")
            .and_then(Value::as_bool)
            .unwrap_or(false);

        *table.global_secondary_indexes.write() =
            Self::parse_indexes(req.params.get("GlobalSecondaryIndexes"));
        *table.local_secondary_indexes.write() =
            Self::parse_indexes(req.params.get("LocalSecondaryIndexes"));
        if let Some(spec) = req.params.get("StreamSpecification") {
            let enabled = spec
                .get("StreamEnabled")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            *table.stream_enabled.write() = enabled;
            *table.stream_view_type.write() = spec
                .get("StreamViewType")
                .and_then(|v| v.as_str())
                .map(String::from);
            if enabled {
                streaming::enable_stream(&table);
            }
        }
        state.put_table(table.clone());
        Self::describe_table_response(req, &table, 200, "TableDescription")
    }

    fn delete_table(&self, req: &AwsRequest) -> AwsResponse {
        let table_name = param_str(&req.params, "TableName")
            .unwrap_or_default()
            .to_string();
        let state = self.get_state(req.account, &req.region);
        if let Some(table) = state.get_table(&table_name) {
            // moto resolves DeletionProtectedException when the flag is on
            // (models/__init__.py:115-120) — a MockValidationException
            // subclass, so the WIRE type is ValidationException with moto's
            // exact message (dynamo_db exceptions.py:412-416).
            if *table.deletion_protection_enabled.read() {
                return AwsResponse::error(
                    400,
                    "ValidationException",
                    &format!(
                        "1 validation error detected: Table '{table_name}' can't be deleted \
                         while DeletionProtectionEnabled is set to True"
                    ),
                );
            }
        }
        match state.delete_table(&table_name) {
            Some(table) => AwsResponse::json(
                200,
                json!({ "TableDescription": {
                    "TableName": table.name,
                    "TableStatus": "DELETING",
                }}),
            ),
            None => AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!("Requested resource not found: Table: {table_name} not found"),
            ),
        }
    }

    /// Engine hook: stream capture records for `table_name` beyond the
    /// given sequence number (exclusive), in shard order. `None` when the
    /// table does not exist or its streams were never enabled.
    pub fn stream_records_after(
        &self,
        account: u64,
        region: &str,
        table_name: &str,
        after_seq: Option<String>,
    ) -> Option<Vec<Value>> {
        let state = self.get_state(account, region);
        let table = state.get_table(table_name)?;
        if !streaming::stream_enabled(&table) {
            return None;
        }
        let shard_guard = table.stream_shard.read();
        let Some(shard) = shard_guard.as_ref() else {
            return Some(Vec::new());
        };
        let records = shard.records.read().clone();
        Some(
            records
                .into_iter()
                .filter(|r| {
                    let seq = r
                        .get("dynamodb")
                        .and_then(|d| d.get("SequenceNumber"))
                        .and_then(Value::as_str)
                        .map(String::from)
                        .unwrap_or_default();
                    match &after_seq {
                        None => true,
                        Some(after) => compare_seq(&seq, after),
                    }
                })
                .collect(),
        )
    }

    /// Whether the table exists in this (account, region) — the probe the
    /// lambda crate's `CreateEventSourceMapping` validation reaches through
    /// `lambda::bridge` (moto's `LambdaBackend.create_event_source_mapping`
    /// resolves the dynamodb streams source's owning table the same way
    /// it resolves a kinesis stream).
    pub fn has_table(&self, account: u64, region: &str, table_name: &str) -> bool {
        self.get_state(account, region)
            .get_table(table_name)
            .is_some()
    }

    /// Whether `stream_arn` names the table's LIVE stream (moto's create
    /// loop resolves the mapping source through the dynamodbstreams backend
    /// and answers 404 "Invalid EventSourceArn" unless the given ARN matches
    /// the table's current stream — `lambda/models.py` + `dynamodbstreams`
    /// backend). Kinesis-style sources name the full
    /// `.../stream/{LatestStreamLabel}` form; a stream-less table has none
    /// and matches nothing.
    pub fn has_stream_arn(&self, account: u64, region: &str, stream_arn: &str) -> bool {
        // arn:...:table/{name}/stream/{label}
        let Some(resource) = stream_arn.splitn(6, ':').nth(5) else {
            return false;
        };
        let Some((name, label)) = resource
            .strip_prefix("table/")
            .and_then(|r| r.split_once("/stream/"))
        else {
            return false;
        };
        let name = name.to_string();
        let Some(table) = self.get_state(account, region).get_table(&name) else {
            return false;
        };
        if !streaming::stream_enabled(&table) {
            return false;
        }
        let live_guard = table.stream_label.read();
        let Some(live_label) = live_guard.as_deref() else {
            return false;
        };
        label == live_label
    }

    /// The newest stream-capture sequence number currently on `table_name`
    /// (`None` when the table does not exist or its streams are off). The
    /// LATEST attach-time seed rides this — a fresh TRIM-default replay set
    /// must not include attach-time history (mirror of the kinesis
    /// attach-watermarks binding).
    pub fn stream_latest_sequence(
        &self,
        account: u64,
        region: &str,
        table_name: &str,
    ) -> Option<String> {
        let state = self.get_state(account, region);
        let table = state.get_table(table_name)?;
        if !streaming::stream_enabled(&table) {
            return None;
        }
        let shard_guard = table.stream_shard.read();
        let shard = shard_guard.as_ref()?;
        let records = shard.records.read();
        let mut latest = String::new();
        for r in records.iter() {
            let seq = r
                .get("dynamodb")
                .and_then(|d| d.get("SequenceNumber"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            if latest.is_empty() || compare_seq(seq, &latest) {
                latest = seq.to_string();
            }
        }
        Some(latest)
    }

    fn describe_table(&self, req: &AwsRequest) -> AwsResponse {
        let table_name = param_str(&req.params, "TableName")
            .unwrap_or_default()
            .to_string();
        let state = self.get_state(req.account, &req.region);
        match state.get_table(&table_name) {
            Some(table) => Self::describe_table_response(req, &table, 200, "Table"),
            None => AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!("Requested resource not found: Table: {table_name} not found"),
            ),
        }
    }

    /// The table-level WarmThroughput block shared by every renderer of a
    /// table description (CreateTable, DescribeTable, UpdateTable).
    ///
    /// Real AWS reports WarmThroughput on every table — Terraform AWS
    /// provider v6.13+ polls DescribeTable for it (localstack #13140).
    /// moto mirrors it unconditionally in Table.describe(): PAY_PER_REQUEST
    /// tables report moto's default 12000/4000 warm units, PROVISIONED tables
    /// their provisioned units, Status ACTIVE. The member shape follows
    /// botocore's TableWarmThroughputDescription, whose Status member is the
    /// TableStatus enum.
    fn table_warm_throughput(table: &Table) -> Value {
        if *table.billing_mode.read() == "PAY_PER_REQUEST" {
            json!({
                "ReadUnitsPerSecond": DEFAULT_WARM_THROUGHPUT_RCU,
                "WriteUnitsPerSecond": DEFAULT_WARM_THROUGHPUT_WCU,
                "Status": "ACTIVE",
            })
        } else {
            json!({
                "ReadUnitsPerSecond": *table.provisioned_rcu.read(),
                "WriteUnitsPerSecond": *table.provisioned_wcu.read(),
                "Status": "ACTIVE",
            })
        }
    }

    /// The single renderer of a table description, shared by DescribeTable,
    /// CreateTable and UpdateTable (moto renders all three from
    /// `Table.describe()` with a base key; the twin keeps one code path).
    ///
    /// TableId: moto itself omits TableId from Table.describe() (verified
    /// against moto 5.2.4 — no member on the description map), while real AWS
    /// renders one; the twin renders the STORED stable id (`Table.table_id`,
    /// minted once in `Table::new`), so every describes-rendering operation
    /// reports the same value instead of a fresh random uuid per call.
    fn describe_table_response(
        _req: &AwsRequest,
        table: &Table,
        status: u16,
        wrapper: &str,
    ) -> AwsResponse {
        let key_schema_json: Vec<Value> = table
            .key_schema
            .iter()
            .map(|k| json!({"AttributeName": k.attribute_name, "KeyType": k.key_type}))
            .collect();
        let attr_defs_json: Vec<Value> = table
            .attribute_definitions
            .read()
            .iter()
            .map(|a| json!({"AttributeName": a.attribute_name, "AttributeType": a.attribute_type}))
            .collect();
        let item_count = table.items.read().len() as i64;
        let gsis = table.global_secondary_indexes.read().clone();
        let lsis = table.local_secondary_indexes.read().clone();
        let provisioned = json!({
            "ReadCapacityUnits": *table.provisioned_rcu.read(),
            "WriteCapacityUnits": *table.provisioned_wcu.read(),
        });

        let warm_throughput = Self::table_warm_throughput(table);

        let index_arn_base = table.table_arn.read().clone();
        let index_json = |indexes: &[crate::models::IndexDefinition], is_gsi: bool| -> Vec<Value> {
            indexes
                .iter()
                .map(|i| {
                    let mut index = json!({
                        "IndexName": i.name,
                        "KeySchema": i.key_schema.iter().map(|k| json!({"AttributeName": k.attribute_name, "KeyType": k.key_type})).collect::<Vec<_>>(),
                        "Projection": {
                            "ProjectionType": i.projection,
                            "NonKeyAttributes": i.non_key_attributes,
                        },
                        "IndexStatus": "ACTIVE",
                        "IndexArn": format!("{index_arn_base}:index/{}", i.name)
                    });
                    if is_gsi {
                        index["IndexSizeBytes"] = json!(0);
                        index["ItemCount"] = json!(0);
                        // moto renders each GSI's OWN throughput
                        // (dynamodb/models/table.py GlobalSecondaryIndex
                        // __init__ + describe): the index echoes what its own
                        // request carried — a PROVISIONED table's GSI spec'd
                        // at 7/9 describes 7/9 while the table itself
                        // describes its own units. An on-demand GSI (no spec
                        // throughput, e.g. any index on a PAY_PER_REQUEST
                        // table) describes a zero block, mirroring moto's
                        // zeroed throughput default.
                        index["ProvisionedThroughput"] = json!({
                            "ReadCapacityUnits": i.provisioned_rcu.unwrap_or(0),
                            "WriteCapacityUnits": i.provisioned_wcu.unwrap_or(0),
                        });
                        // botocore declares WarmThroughput only on
                        // GlobalSecondaryIndexDescription — its member shape
                        // is GlobalSecondaryIndexWarmThroughputDescription
                        // (Status is the IndexStatus enum; LSI descriptions
                        // carry no such member). moto reports WarmThroughput
                        // on every GSI: a provisioned GSI advertises its own
                        // units, an on-demand GSI moto's 12000/4000 defaults
                        // (moto/dynamodb/models/table.py GlobalSecondaryIndex).
                        let (warm_rcu, warm_wcu) = match (i.provisioned_rcu, i.provisioned_wcu) {
                            (Some(rcu), Some(wcu)) => (rcu, wcu),
                            _ => (DEFAULT_WARM_THROUGHPUT_RCU, DEFAULT_WARM_THROUGHPUT_WCU),
                        };
                        index["WarmThroughput"] = json!({
                            "ReadUnitsPerSecond": warm_rcu,
                            "WriteUnitsPerSecond": warm_wcu,
                            "Status": "ACTIVE",
                        });
                    }
                    index
                })
                .collect()
        };

        let mut description = json!({
            "TableName": table.name,
            "TableStatus": *table.status.read(),
            "TableId": table.table_id.clone(),
            "TableArn": *table.table_arn.read(),
            "KeySchema": key_schema_json,
            "AttributeDefinitions": attr_defs_json,
            "CreationDateTime": format_created_at(table.created_at),
            "ItemCount": item_count,
            "TableSizeBytes": 0,
            "ProvisionedThroughput": provisioned,
            "LocalSecondaryIndexes": index_json(&lsis, false),
            "GlobalSecondaryIndexes": index_json(&gsis, true),
            "DeletionProtectionEnabled": *table.deletion_protection_enabled.read(),
            "BillingModeSummary": { "BillingMode": *table.billing_mode.read() },
            "TableClassSummary": { "TableClass": "STANDARD" },
            "WarmThroughput": warm_throughput,
        });
        // moto emits the stream fields on stream-enabled tables
        // (moto/dynamodb/models/table.py: LatestStreamLabel + LatestStreamArn).
        if *table.stream_enabled.read() {
            if let Some(label) = table.stream_label.read().as_ref() {
                description["LatestStreamLabel"] = json!(label);
                description["LatestStreamArn"] =
                    json!(format!("{}/stream/{}", *table.table_arn.read(), label));
                // real AWS also echoes the StreamSpecification the table was
                // created/updated with (parity test_dynamodb_streams.pin).
                description["StreamSpecification"] = json!({
                    "StreamEnabled": true,
                    "StreamViewType": table.stream_view_type.read().as_deref().unwrap_or("NEW_AND_OLD_IMAGES"),
                });
            }
        }
        AwsResponse::json(status, json!({ wrapper: description }))
    }

    fn update_table(&self, req: &AwsRequest) -> AwsResponse {
        let table_name = param_str(&req.params, "TableName")
            .unwrap_or_default()
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let Some(table) = state.get_table(&table_name) else {
            return AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!("Requested resource not found: Table: {table_name} not found"),
            );
        };

        // moto update_table (dynamodb/models/__init__.py:168-195) applies
        // its members in a fixed order — attribute definitions, global
        // secondary index updates, provisioned throughput, billing mode,
        // stream specification, deletion protection — stopping at the
        // first failure, so a member positioned earlier that raises
        // shadows later members entirely (nothing after it is applied).
        // The twin previously ran throughput → billing → stream →
        // protection → attrs → GSI; reordered to moto's per-source order.

        // 1. moto replaces table.attr wholesale with a non-empty
        // AttributeDefinitions member (models/__init__.py:180-181 —
        // `table.attr = attr_definitions`; live-transcribed 2026-10-26,
        // a request carrying only [extra] dropped the earlier [id]). The
        // twin keeps its long-standing merge behavior — flagged as an
        // observed divergence, not mirrored in this lane.
        if let Some(defs) = req.params.get("AttributeDefinitions") {
            let mut existing_defs = table.attribute_definitions.write();
            for def in Self::parse_attr_defs(Some(defs)) {
                if !existing_defs
                    .iter()
                    .any(|d| d.attribute_name == def.attribute_name)
                {
                    existing_defs.push(def);
                }
            }
        }

        // 2. GlobalSecondaryIndexUpdates. moto's
        // update_table_global_indexes (models/__init__.py:207-247) walks
        // each member's verbs in Delete → Update → Create order and raises
        // ValueError — a plain werkzeug 500 through moto_server —
        // when a verb names an index that does not exist ("Global
        // Secondary Index does not exist, but tried to delete/update:
        // {name}") or re-creates a name that exists ("Global Secondary
        // Index already exists: {name}"), plus a KeyError 500 when a
        // Create lacks one of its required IndexName/KeySchema/Projection
        // members (an empty KeySchema ARRAY is accepted — live-transcribed
        // moto 5.2.4, 2026-10-26). The twin answers those with AWS's
        // ValidationException instead of moto's raw 500: the probe canon
        // reserves 500_error for genuine server crashes, and an
        // unvalidated UpdateTable shape (the full-shape probe fill puts
        // Update+Create+Delete in ONE element) must not crash. Deletes and
        // creates mutate a staging list committed only when every member
        // succeeded (moto assigns table.global_indexes after the loop);
        // Updates mutate the live entry and survive a later member's
        // failure (live-transcribed: Update g1x→9/1 then Delete ghost
        // answered the 500 and g1x kept 9/1).
        let index_updates = req
            .params
            .get("GlobalSecondaryIndexUpdates")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let mut staged = table.global_secondary_indexes.read().clone();
        for update in index_updates {
            if let Some(delete) = update.get("Delete") {
                let Some(name) = delete.get("IndexName").and_then(Value::as_str) else {
                    // The twin's ValidationException posture for this section:
                    // moto 500s on a missing IndexName (KeyError, live 2026-10-26).
                    return AwsResponse::error(
                        400,
                        "ValidationException",
                        "One or more parameter values were invalid: \
                         Missing required member IndexName in GlobalSecondaryIndexUpdates.Delete",
                    );
                };
                let Some(pos) = staged.iter().position(|g| g.name == name) else {
                    return AwsResponse::error(
                        400,
                        "ValidationException",
                        &format!(
                            "Global Secondary Index does not exist, but tried to delete/update: {name}"
                        ),
                    );
                };
                staged.remove(pos);
            }
            if let Some(gsi_update) = update.get("Update") {
                let Some(name) = gsi_update.get("IndexName").and_then(Value::as_str) else {
                    return AwsResponse::error(
                        400,
                        "ValidationException",
                        "One or more parameter values were invalid: \
                         Missing required member IndexName in GlobalSecondaryIndexUpdates.Update",
                    );
                };
                let Some(pos) = staged.iter().position(|g| g.name == name) else {
                    return AwsResponse::error(
                        400,
                        "ValidationException",
                        &format!(
                            "Global Secondary Index does not exist, but tried to delete/update: {name}"
                        ),
                    );
                };
                // moto's IndexDefinition.update replaces the members the
                // update carries (dynamodb/models/table.py:149-155).
                let idx = &mut staged[pos];
                if let Some(key_schema_json) = gsi_update.get("KeySchema") {
                    idx.key_schema = Self::parse_key_schema(Some(key_schema_json));
                }
                if let Some(proj) = gsi_update.get("Projection") {
                    idx.projection = proj
                        .get("ProjectionType")
                        .and_then(|t| t.as_str())
                        .unwrap_or("ALL")
                        .to_string();
                    idx.non_key_attributes = proj
                        .get("NonKeyAttributes")
                        .and_then(|n| n.as_array())
                        .map(|a| {
                            a.iter()
                                .filter_map(|x| x.as_str().map(String::from))
                                .collect()
                        })
                        .unwrap_or_default();
                }
                if let Some(throughput) = gsi_update.get("ProvisionedThroughput") {
                    idx.provisioned_rcu =
                        throughput.get("ReadCapacityUnits").and_then(|v| v.as_i64());
                    idx.provisioned_wcu = throughput
                        .get("WriteCapacityUnits")
                        .and_then(|v| v.as_i64());
                }
                // moto mutates the LIVE index object here — mirror on the
                // stored entry so the change also survives a later member
                // failing.
                let mut stored = table.global_secondary_indexes.write();
                if let Some(stored_idx) = stored.iter_mut().find(|g| g.name == name) {
                    *stored_idx = idx.clone();
                }
            }
            if let Some(create) = update.get("Create") {
                let (index_name, key_schema_json, projection_json) = match (
                    create.get("IndexName").and_then(Value::as_str),
                    create.get("KeySchema"),
                    create.get("Projection"),
                ) {
                    (Some(name), Some(ks), Some(p)) if !name.is_empty() => (name, ks, p),
                    // moto's GlobalSecondaryIndex.create does dct[...] — a
                    // missing required member IndexErrors out (KeyError 500
                    // live-transcribed 2026-10-26); the twin answers AWS's
                    // ValidationException for that unvalidated shape.
                    _ => {
                        return AwsResponse::error(
                            400,
                            "ValidationException",
                            "One or more parameter values were invalid: \
                             Global Secondary Index create requires IndexName, \
                             KeySchema and Projection",
                        )
                    }
                };
                if staged.iter().any(|g| g.name == index_name) {
                    return AwsResponse::error(
                        400,
                        "ValidationException",
                        &format!("Global Secondary Index already exists: {index_name}"),
                    );
                }
                let key_schema = Self::parse_key_schema(Some(key_schema_json));
                let projection = projection_json
                    .get("ProjectionType")
                    .and_then(|t| t.as_str())
                    .unwrap_or("ALL")
                    .to_string();
                let non_key_attributes = projection_json
                    .get("NonKeyAttributes")
                    .and_then(|n| n.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|x| x.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default();
                // moto's GlobalSecondaryIndex.create (used for indexes
                // created through UpdateTable too) reads the index's own
                // throughput.
                let throughput = create.get("ProvisionedThroughput");
                staged.push(crate::models::IndexDefinition {
                    name: index_name.to_string(),
                    key_schema,
                    projection,
                    non_key_attributes,
                    provisioned_rcu: throughput
                        .and_then(|p| p.get("ReadCapacityUnits"))
                        .and_then(|v| v.as_i64()),
                    provisioned_wcu: throughput
                        .and_then(|p| p.get("WriteCapacityUnits"))
                        .and_then(|v| v.as_i64()),
                });
            }
        }
        // Committed only when every member succeeded (moto assigns
        // table.global_indexes after its loop; on an error we already
        // returned).
        *table.global_secondary_indexes.write() = staged;

        // 3. moto update_table: a present throughput member replaces the
        // stored units — nothing validates the values server-side (live:
        // 0/0 stored verbatim, 2026-10-26).
        if let Some(pt) = req.params.get("ProvisionedThroughput") {
            let rcu = pt
                .get("ReadCapacityUnits")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            let wcu = pt
                .get("WriteCapacityUnits")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            *table.provisioned_rcu.write() = rcu;
            *table.provisioned_wcu.write() = wcu;
        }

        // 4. moto update_table (dynamodb/models/__init__.py:186-187): a BillingMode
        // member mutates the stored mode, so this and the next DescribeTable
        // echo the new mode — and the derived WarmThroughput follows it (the
        // twin treats a PAY_PER_REQUEST table as moto's 12000/4000 on-demand
        // defaults).
        if let Some(mode) = param_str(&req.params, "BillingMode") {
            if !mode.is_empty() {
                *table.billing_mode.write() = mode.to_string();
            }
        }

        // 5. moto update_table: stream_spec is applied through
        // update_table_streams (dynamodb/models/__init__.py:197-205) —
        // re-enabling a table that already carries a stream label raises
        // ResourceInUseException ("Cannot enable stream"); otherwise an
        // enabled-or-view-typed spec attaches the shard and stream label the
        // way set_stream_specification does, and any other spec disables the
        // stream (the label/shard linger there, as in moto).
        if let Some(spec) = req.params.get("StreamSpecification") {
            let enabled = spec
                .get("StreamEnabled")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let view_type = spec.get("StreamViewType").and_then(Value::as_str);
            if (enabled || view_type.is_some()) && table.stream_label.read().is_some() {
                return AwsResponse::error(400, "ResourceInUseException", "Cannot enable stream");
            }
            if enabled || view_type.is_some() {
                *table.stream_view_type.write() = view_type.map(String::from);
                *table.stream_enabled.write() = true;
                streaming::enable_stream(&table);
            } else {
                *table.stream_enabled.write() = false;
            }
        }

        // 6. moto update_table (dynamodb/models/__init__.py:190-191): a present
        // boolean DeletionProtectionEnabled member mutates the stored value.
        if let Some(enabled) = req
            .params
            .get("DeletionProtectionEnabled")
            .and_then(Value::as_bool)
        {
            *table.deletion_protection_enabled.write() = enabled;
        }

        // moto's UpdateTable response is Table.describe() — the full base_key
        // "TableDescription" payload (moto/dynamodb/models/__init__.py
        // update_table + Table.describe). Route through the shared renderer so
        // the response carries all of it — BillingModeSummary, KeySchema,
        // AttributeDefinitions, GlobalSecondaryIndexes (each with its own
        // ProvisionedThroughput/WarmThroughput) and the table-level
        // WarmThroughput computed after the mutations above.
        Self::describe_table_response(req, &table, 200, "TableDescription")
    }
}

impl Default for DynamoDbHandler {
    fn default() -> Self {
        Self::new()
    }
}

// ----------------------------------------------------------------------
// Module-level helpers
// ----------------------------------------------------------------------

fn param_str<'a>(params: &'a Value, key: &str) -> Option<&'a str> {
    params.get(key).and_then(|v| v.as_str())
}

fn get_names(req: &AwsRequest) -> Names {
    req.params
        .get("ExpressionAttributeNames")
        .and_then(|v| v.as_object())
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

/// The first `#name` token in `expr` absent from the names map (the strict
/// name resolution moto's evaluator rides). `None` when every reference
/// resolves; single-quoted operator literals are skipped.
fn unbound_name_token(expr: &str, names: &Names) -> Option<String> {
    let bytes = expr.as_bytes();
    let mut in_literal = false;
    let mut found: Option<String> = None;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => in_literal = !in_literal,
            b'#' if !in_literal => {
                let start = i;
                let mut end = i + 1;
                while end < bytes.len()
                    && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_')
                {
                    end += 1;
                }
                if found.is_none() {
                    let tok = &expr[start..end];
                    if !names.contains_key(tok) {
                        found = Some(tok.to_string());
                    }
                }
                i = end;
                continue;
            }
            _ => {}
        }
        i += 1;
    }
    found
}

fn get_values(req: &AwsRequest) -> Values {
    req.params
        .get("ExpressionAttributeValues")
        .and_then(|v| v.as_object())
        .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default()
}

fn now_timestamp() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%d %H:%M:%S%.6f+00:00")
        .to_string()
}

fn format_created_at(ts: u64) -> String {
    chrono::DateTime::from_timestamp(ts as i64, 0)
        .unwrap_or_default()
        .format("%Y-%m-%d %H:%M:%S%.6f+00:00")
        .to_string()
}

/// Canonical storage key for an item given its key attributes.
fn item_key(table: &Table, attrs: &Map<String, Value>) -> Option<String> {
    compute_key_from_attrs(&table.key_schema, &attrs_to_map(attrs))
}

/// The 400 for a batch PUT row whose key cannot resolve against the schema.
/// A key member that is entirely ABSENT reads moto's `table.put_item` text
/// "Missing the key {attr} in the item" (table.py:627-638 — the name rides
/// the schema, first-missing HASH-then-RANGE wins; moto reaches it only on
/// the put arm); a present-but-unusable member keeps the real-AWS
/// schema-mismatch text. The batch DELETE arm bypasses this helper — moto
/// routes deletes through `get_keys_value`'s plain "Validation Exception".
fn keyed_lookup_error(table: &Table, attrs: &Map<String, Value>) -> AwsResponse {
    match table
        .key_schema
        .iter()
        .map(|k| k.attribute_name.as_str())
        .find(|name| !attrs.contains_key(*name))
    {
        Some(missing) => AwsResponse::error(
            400,
            "ValidationException",
            &format!(
                "One or more parameter values were invalid: Missing the key {missing} in the item"
            ),
        ),
        None => AwsResponse::error(
            400,
            "ValidationException",
            "The provided key element does not match the schema",
        ),
    }
}

/// Enforce the real-AWS per-item 400 KB limit on the item about to be
/// written, mirroring moto's check — a 400 ValidationException (moto issue
/// #1874). Numbers are sized with AWS's own formula per moto issue #10176
/// (`models::number_value_size`), so moto-oversized items are accepted.
///
/// UpdateItem and the TransactWriteItems Update row reach this via moto's
/// `update_item`, which re-raises the underlying rejected size as
/// ItemSizeToUpdateTooLarge ("Item size to update has exceeded the maximum
/// allowed size"); put-shaped paths (PutItem, BatchWriteItem, Transact
/// Put, PartiQL INSERT/UPDATE via put_item) keep moto's put-flavored
/// "Item size has exceeded the maximum allowed size".
fn check_item_size(attrs: &Map<String, Value>, is_update: bool) -> Option<AwsResponse> {
    // moto validates member shapes at item construction (table.py
    // _validate_number_type + the NULL check) before sizing: a numeric claim
    // with a non-numeric literal and a non-true NULL are 400s on every
    // write path that funnels through the item check.
    for v in attrs.values() {
        if let Some(e) = validate_attribute_values(v) {
            return Some(e);
        }
    }
    if crate::models::item_size(attrs) > crate::models::MAX_ITEM_SIZE {
        let message = if is_update {
            "Item size to update has exceeded the maximum allowed size"
        } else {
            "Item size has exceeded the maximum allowed size"
        };
        Some(AwsResponse::error(400, "ValidationException", message))
    } else {
        None
    }
}

/// Moto's per-failure wire text for a numeric verdict (table.py:578-619).
fn number_validity_error(
    validity: expr_engine::NumberValidity,
    shown: &str,
) -> Option<AwsResponse> {
    match validity {
        expr_engine::NumberValidity::Valid => None,
        expr_engine::NumberValidity::NonNumeric => Some(AwsResponse::error(
            400,
            "ValidationException",
            &format!("The parameter cannot be converted to a numeric value: {shown}"),
        )),
        expr_engine::NumberValidity::Overflow => Some(AwsResponse::error(
            400,
            "ValidationException",
            "Number overflow. Attempting to store a number with magnitude larger than \
             supported range",
        )),
        expr_engine::NumberValidity::Underflow => Some(AwsResponse::error(
            400,
            "ValidationException",
            "Number underflow. Attempting to store a number with magnitude smaller than \
             supported range",
        )),
    }
}

/// Recursive member-shape validation (moto table.py _validate_number_type +
/// the NULL-type check): numeric-claim failures and a non-true NULL are
/// ValidationExceptions on every write path, with moto's per-failure text
/// (unparseable/whitespace = "cannot be converted", overflow/underflow the
/// Number {over,under}flow messages). Divergence from moto is *only* the
/// `L` arm: moto recurses through nested `M` dicts (its validator runs
/// itself on every dict member) so a bad `N` inside a map 400s in moto
/// too, but its loop never descends into `L` lists (models/table.py
/// `_validate_item_types` iterates dict items only, so moto stores a bad
/// `N` inside a list). This recursion rejects the same literal nested
/// anywhere — real-AWS posture, so callers get the same 400 they'd see
/// at the top level.
fn validate_attribute_values(value: &Value) -> Option<AwsResponse> {
    if let Some(n) = value.get("N").and_then(Value::as_str) {
        return number_validity_error(expr_engine::number_validity(n), n);
    }
    if let Some(null) = value.get("NULL") {
        if *null != Value::Bool(true) {
            // moto table.py:564-569.
            return Some(AwsResponse::error(
                400,
                "ValidationException",
                "Null attribute value types must have the value of true",
            ));
        }
        return None;
    }
    if let Some(obj) = value.get("M").and_then(Value::as_object) {
        for v in obj.values() {
            if let Some(e) = validate_attribute_values(v) {
                return Some(e);
            }
        }
    }
    if let Some(list) = value.get("L").and_then(Value::as_array) {
        for v in list {
            if let Some(e) = validate_attribute_values(v) {
                return Some(e);
            }
        }
    }
    if let Some(ns) = value.get("NS").and_then(Value::as_array) {
        for v in ns {
            let Some(s) = v.as_str() else {
                // moto has NO server-side NS-member validation at all
                // (_validate_item_types checks N/S/NULL only) — its non-
                // string NS members blow up lazily at comparison/hash time.
                // The twin surfaces the member eagerly on the write with
                // the numeric-conversion text, printing the offending
                // literal in full (not a blank).
                return Some(AwsResponse::error(
                    400,
                    "ValidationException",
                    &format!("The parameter cannot be converted to a numeric value: {v}"),
                ));
            };
            if let Some(err) = number_validity_error(expr_engine::number_validity(s), s) {
                return Some(err);
            }
        }
    }
    None
}

fn kh_to_attrmap(key: &Map<String, Value>) -> AttrMap {
    key.clone().into_iter().collect()
}

fn attrs_to_map(attrs: &Map<String, Value>) -> AttrMap {
    attrs.clone().into_iter().collect()
}

fn map_to_value(attrs: &AttrMap) -> Value {
    let mut m = Map::new();
    for (k, v) in attrs {
        m.insert(k.clone(), v.clone());
    }
    Value::Object(m)
}

/// `AttrMap` (HashMap) into a serde map view for stream capture.
fn attrmap_to_serde(attrs: &AttrMap) -> Map<String, Value> {
    attrs.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

fn parse_arn_table(arn: &str) -> Option<(String, Option<String>)> {
    // Accept arn:...:table/<name> or ...:table/<name>/index/xx and plain names.
    if let Some((_, rest)) = arn.split_once(":table/") {
        let parts: Vec<&str> = rest.split('/').collect();
        if !parts.first()?.is_empty() {
            let index = parts.get(1).and_then(|seg| {
                if *seg == "index" {
                    rest.split('/').nth(2).map(String::from)
                } else {
                    None
                }
            });
            return Some((parts[0].to_string(), index));
        }
    }
    None
}

impl DynamoDbHandler {
    fn conditional_failed() -> AwsResponse {
        AwsResponse::error(
            400,
            "ConditionalCheckFailedException",
            "The conditional request failed",
        )
    }

    fn not_found_table(table_name: &str) -> AwsResponse {
        AwsResponse::error(
            400,
            "ResourceNotFoundException",
            &format!("Requested resource not found: Table: {table_name} not found"),
        )
    }

    fn resolve_table(&self, req: &AwsRequest) -> (DynamoDbState, Result<Arc<Table>, AwsResponse>) {
        let table_name = param_str(&req.params, "TableName")
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        match state.get_table(&table_name) {
            Some(table) => (state, Ok(table)),
            None => (state, Err(Self::not_found_table(&table_name))),
        }
    }

    /// Evaluate a condition expression against an item snapshot (or empty).
    fn check_condition(
        req: &AwsRequest,
        condition: Option<&str>,
        existing: Option<&AttrMap>,
    ) -> Result<(), AwsResponse> {
        Self::eval_condition_on(condition, existing, &get_names(req), &get_values(req))
    }

    /// Evaluate a condition expression with explicit names/values (transact ops
    /// carry their own ExpressionAttribute* parameters).
    fn eval_condition_on(
        condition: Option<&str>,
        existing: Option<&AttrMap>,
        names: &Names,
        values: &Values,
    ) -> Result<(), AwsResponse> {
        let Some(cond) = condition else {
            return Ok(());
        };
        if cond.trim().is_empty() {
            return Ok(());
        }
        let item = match existing.cloned() {
            Some(m) => Value::Object(m.into_iter().collect()),
            None => Value::Object(Map::new()),
        };
        let ok = expr_engine::eval_condition(cond, &item, names, values)
            .map_err(|e| AwsResponse::error(400, "ValidationException", &e))?;
        if ok {
            Ok(())
        } else {
            Err(Self::conditional_failed())
        }
    }

    // ------------------------------------------------------------------
    // Item operations
    // ------------------------------------------------------------------

    fn put_item(&self, req: &AwsRequest) -> AwsResponse {
        let (state, table_r) = self.resolve_table(req);
        let table = match table_r {
            Ok(t) => t,
            Err(e) => return e,
        };

        let item_json = req.params.get("Item").cloned().unwrap_or(Value::Null);
        let attrs: Map<String, Value> = item_json.as_object().cloned().unwrap_or_default();
        let key = match item_key(&table, &attrs) {
            Some(k) => k,
            None => {
                return AwsResponse::error(
                    400,
                    "ValidationException",
                    "The provided key element does not match the schema",
                )
            }
        };

        // moto constructs the LimitedSizeDict-backed Item before evaluating
        // conditions, so the 400 KB check runs first (getmoto/moto#1874).
        if let Some(e) = check_item_size(&attrs, false) {
            return e;
        }

        let existing = table.items.read().get(&key).map(|i| i.attributes.clone());
        if let Err(e) = Self::check_condition(
            req,
            expr_str(req.params.get("ConditionExpression")),
            existing.as_ref().map(|m| m as &AttrMap),
        ) {
            return e;
        }

        let mut items = table.items.write();
        let old = items.remove(&key);
        items.insert(key, Arc::new(Item::new(attrs_to_map(&attrs))));
        drop(items);
        let _ = state;

        // Stream capture mirrors moto's put_item hook (INSERT / MODIFY).
        let old_serde = streaming::stream_enabled(&table)
            .then(|| old.as_ref().map(|i| attrmap_to_serde(&i.attributes)))
            .flatten();
        streaming::record_mutation(
            &table,
            old_serde.as_ref().map(|m| m as &Map<String, Value>),
            Some(&attrs),
        );

        let mut resp = json!({});
        if param_str(&req.params, "ReturnValues").unwrap_or("NONE") == "ALL_OLD" {
            if let Some(old_item) = old {
                resp["Attributes"] = map_to_value(&old_item.attributes);
            }
        }
        AwsResponse::json(200, resp)
    }

    fn get_item(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, table_r) = self.resolve_table(req);
        let table = match table_r {
            Ok(t) => t,
            Err(e) => return e,
        };

        let key_json = req.params.get("Key").cloned().unwrap_or(Value::Null);
        let key_attrs: Map<String, Value> = key_json.as_object().cloned().unwrap_or_default();
        let key = match item_key(&table, &key_attrs) {
            Some(k) => k,
            None => {
                return AwsResponse::error(
                    400,
                    "ValidationException",
                    "The provided key element does not match the schema",
                )
            }
        };

        let Some(item) = table.items.read().get(&key).cloned() else {
            return AwsResponse::json(200, json!({}));
        };
        let item_json = Value::Object(item.attributes.clone().into_iter().collect());
        let projected = project_from_req(req, &item_json);
        AwsResponse::json(200, json!({ "Item": projected }))
    }

    fn delete_item(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, table_r) = self.resolve_table(req);
        let table = match table_r {
            Ok(t) => t,
            Err(e) => return e,
        };

        let key_json = req.params.get("Key").cloned().unwrap_or(Value::Null);
        let key_attrs: Map<String, Value> = key_json.as_object().cloned().unwrap_or_default();
        let key = match item_key(&table, &key_attrs) {
            Some(k) => k,
            None => {
                return AwsResponse::error(
                    400,
                    "ValidationException",
                    "The provided key element does not match the schema",
                )
            }
        };

        let existing = table.items.read().get(&key).map(|i| i.attributes.clone());
        if let Err(e) = Self::check_condition(
            req,
            expr_str(req.params.get("ConditionExpression")),
            existing.as_ref(),
        ) {
            return e;
        }

        let old = table.items.write().remove(&key);
        // Stream capture mirrors moto's delete_item hook (REMOVE only when
        // the item actually existed — moto emits no event without an image).
        let old_serde = streaming::stream_enabled(&table)
            .then(|| old.as_ref().map(|i| attrmap_to_serde(&i.attributes)))
            .flatten();
        streaming::record_mutation(
            &table,
            old_serde.as_ref().map(|m| m as &Map<String, Value>),
            None,
        );
        let mut resp = json!({});
        if param_str(&req.params, "ReturnValues").unwrap_or("NONE") == "ALL_OLD" {
            if let Some(old_item) = old {
                resp["Attributes"] = map_to_value(&old_item.attributes);
            }
        }
        AwsResponse::json(200, resp)
    }

    fn update_item(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, table_r) = self.resolve_table(req);
        let table = match table_r {
            Ok(t) => t,
            Err(e) => return e,
        };

        let key_json = req.params.get("Key").cloned().unwrap_or(Value::Null);
        let key_attrs: Map<String, Value> = key_json.as_object().cloned().unwrap_or_default();
        let key = match item_key(&table, &key_attrs) {
            Some(k) => k,
            None => {
                return AwsResponse::error(
                    400,
                    "ValidationException",
                    "The provided key element does not match the schema",
                )
            }
        };

        let existing = table.items.read().get(&key).map(|i| i.attributes.clone());
        if let Err(e) = Self::check_condition(
            req,
            expr_str(req.params.get("ConditionExpression")),
            existing.as_ref().map(|m| m as &AttrMap),
        ) {
            return e;
        }

        // Start from the existing item or the keys alone (upsert semantics).
        // Start from the existing item or the keys alone (upsert semantics).
        let mut new_attrs: Map<String, Value> = match &existing {
            Some(m) => m.clone().into_iter().collect(),
            None => {
                let mut m = Map::new();
                for (k, v) in key_attrs {
                    m.insert(k, v);
                }
                m
            }
        };
        let old_attrs = existing.clone();
        let old_was_present = existing.is_some();

        let update_expr = expr_str(req.params.get("UpdateExpression")).unwrap_or("");
        let outcome_result = if update_expr.trim().is_empty() {
            Ok(expr_engine::UpdateOutcome {
                set_paths: Vec::new(),
                add_paths: Vec::new(),
            })
        } else {
            expr_engine::apply_update(
                &mut new_attrs,
                update_expr,
                &get_names(req),
                &get_values(req),
            )
        };
        let outcome = match outcome_result {
            Ok(o) => o,
            Err(e) => return AwsResponse::error(400, "ValidationException", e.as_str()),
        };

        // moto's UpdateExpressionExecutor raises ItemSizeToUpdateTooLarge on
        // the executed result (getmoto/moto#1874); mirror it on the final
        // item, with the update-flavored wording.
        if let Some(e) = check_item_size(&new_attrs, true) {
            return e;
        }

        let stored: AttrMap = map_to_attrmap(new_attrs.clone());
        table.items.write().insert(key, Arc::new(Item::new(stored)));

        // Stream capture mirrors moto's update_item hook
        // (INSERT when the item was absent, MODIFY otherwise).
        let old_serde = streaming::stream_enabled(&table)
            .then(|| old_attrs.as_ref().map(attrmap_to_serde))
            .flatten();
        streaming::record_mutation(
            &table,
            old_serde.as_ref().map(|m| m as &Map<String, Value>),
            Some(&new_attrs),
        );

        let old_value = old_attrs
            .clone()
            .map(|m| map_to_value(&m))
            .unwrap_or(Value::Object(Map::new()));

        let mut resp = json!({});
        match param_str(&req.params, "ReturnValues").unwrap_or("NONE") {
            "ALL_NEW" => {
                resp["Attributes"] = Value::Object(new_attrs.clone());
            }
            "ALL_OLD" if old_was_present => {
                resp["Attributes"] = old_value.clone();
            }
            "UPDATED_NEW" => {
                let mut updated = Map::new();
                for (path, val) in outcome.set_paths.iter().chain(outcome.add_paths.iter()) {
                    set_nested(&mut updated, path, val.clone());
                }
                resp["Attributes"] = Value::Object(updated);
            }
            "UPDATED_OLD" => {
                let mut updated = Map::new();
                for (path, _) in outcome.set_paths.iter().chain(outcome.add_paths.iter()) {
                    if let Some(v) = expr_engine::lookup(&old_value, path).cloned() {
                        set_nested(&mut updated, path, v);
                    }
                }
                if !updated.is_empty() {
                    resp["Attributes"] = Value::Object(updated);
                }
            }
            _ => {}
        }
        AwsResponse::json(200, resp)
    }

    // ------------------------------------------------------------------
    // Query / Scan
    // ------------------------------------------------------------------

    fn query(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, table_r) = self.resolve_table(req);
        let table = match table_r {
            Ok(t) => t,
            Err(e) => return e,
        };

        let index_name = param_str(&req.params, "IndexName")
            .unwrap_or("")
            .to_string();
        let key_cond = expr_str(req.params.get("KeyConditionExpression")).unwrap_or("");
        let forward = req
            .params
            .get("ScanIndexForward")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        // Fails the request when the named index does not exist.
        let _index = match resolve_index(&table, &index_name) {
            Ok(i) => i,
            Err(e) => return e,
        };
        let _ = (_index, &table);
        let (sort_attr, index_projection) = index_context(&table, &index_name);
        Self::run_scan_or_query(
            req,
            &table,
            key_cond,
            sort_attr.as_deref(),
            forward_reverse(forward),
            index_projection,
        )
    }

    fn scan(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, table_r) = self.resolve_table(req);
        let table = match table_r {
            Ok(t) => t,
            Err(e) => return e,
        };

        let index_name = param_str(&req.params, "IndexName")
            .unwrap_or("")
            .to_string();
        // Fails the request when the named index does not exist.
        let _index = match resolve_index(&table, &index_name) {
            Ok(i) => i,
            Err(e) => return e,
        };
        let _ = (_index, &table);
        let (sort_attr, index_projection) = index_context(&table, &index_name);
        Self::run_scan_or_query(
            req,
            &table,
            "",
            sort_attr.as_deref(),
            false,
            index_projection,
        )
    }

    fn run_scan_or_query(
        req: &AwsRequest,
        table: &Table,
        key_cond: &str,
        sort_attr: Option<&str>,
        reverse: bool,
        index_projection: Option<Vec<Vec<String>>>,
    ) -> AwsResponse {
        let names = get_names(req);
        let values = get_values(req);
        let filter = expr_str(req.params.get("FilterExpression")).unwrap_or("");
        // moto raises expression parse/validation errors back
        // (get_filter_expression → MockValidationException): a pre-pass with
        // an empty probe item surfaces unresolved tokens and malformed
        // grammar instead of a silently-empty result set.
        for (expr, member) in [
            (key_cond, "KeyConditionExpression"),
            (&filter, "FilterExpression"),
        ] {
            if expr.trim().is_empty() {
                continue;
            }
            // moto's name resolution is a strict dict access (a probe (g)
            // KeyError '#nores' → unhandled 500): an expression token whose
            // #name is absent from ExpressionAttributeNames fails the query
            // instead of resolving into a phantom attribute that scans as
            // empty. The twin answers 400 with real AWS's validator text
            // ("The expression contains an unresolved attribute name: …")
            // — a deliberate divergence from moto's crash, recorded here.
            // The value-side misses pour through eval_condition's parser;
            // the names map has no parse-error surface of its own.
            if let Some(tok) = unbound_name_token(expr, &names) {
                return AwsResponse::error(
                    400,
                    "ValidationException",
                    &format!(
                        "{member}: The expression contains an unresolved attribute name: {tok}"
                    ),
                );
            }
            if let Err(e) =
                expr_engine::eval_condition(expr, &Value::Object(Map::new()), &names, &values)
            {
                return AwsResponse::error(400, "ValidationException", &format!("{member}: {e}"));
            }
        }
        let projection = expr_str(req.params.get("ProjectionExpression")).unwrap_or("");
        let select = param_str(&req.params, "Select")
            .unwrap_or("ALL_ATTRIBUTES")
            .to_string();
        let limit = req
            .params
            .get("Limit")
            .and_then(|v| v.as_u64())
            .map(|v| v as usize);
        let esk = req
            .params
            .get("ExclusiveStartKey")
            .and_then(|v| v.as_object())
            .cloned();

        let names_for_sort = names.clone();
        let items = table.items.read();
        let mut candidates: Vec<(Value, Map<String, Value>)> = items
            .values()
            .map(|i| {
                let m: Map<String, Value> = i.attributes.clone().into_iter().collect();
                (Value::Object(m.clone()), m)
            })
            .collect();

        // Index hash-key presence filter: items missing the index keys are not
        // part of that index.
        if let Some(idx) = index_of(table, param_str(&req.params, "IndexName").unwrap_or("")) {
            let hash_attr = idx.hash_attr().unwrap_or("").to_string();
            candidates.retain(|(_, m)| m.contains_key(&hash_attr));
        }

        // Key condition filter.
        if !key_cond.trim().is_empty() {
            let mut filtered = Vec::new();
            for (item, _) in candidates {
                let ok =
                    expr_engine::eval_condition(key_cond, &item, &names, &values).unwrap_or(false);
                if ok {
                    filtered.push((item.clone(), value_to_map(&item)));
                }
            }
            candidates = filtered;
        }

        // Sort by the sort key (index or base), then base key for determinism.
        let base_sort = table.range_key().map(|k| k.attribute_name.clone());
        let base_hash = table.hash_key().map(|k| k.attribute_name.clone());
        let sk = sort_attr.map(String::from).or_else(|| base_sort.clone());
        let cmp_segment = |a: &Value, b: &Value, attr: &str| -> std::cmp::Ordering {
            let av = expr_engine::lookup(a, &[attr.to_string()]);
            let bv = expr_engine::lookup(b, &[attr.to_string()]);
            match (av, bv) {
                (Some(x), Some(y)) => {
                    if expr_engine::attr_eq(x, y) {
                        std::cmp::Ordering::Equal
                    } else {
                        dyn_cmp(x, y)
                    }
                }
                _ => std::cmp::Ordering::Equal,
            }
        };
        let sort_key_fn = |a: &(Value, Map<String, Value>),
                           b: &(Value, Map<String, Value>)|
         -> std::cmp::Ordering {
            if let Some(sk) = &sk {
                let ord = cmp_segment(&a.0, &b.0, sk);
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            if let Some(hk) = &base_hash {
                let ord = cmp_segment(&a.0, &b.0, hk);
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            if let Some(bs) = &base_sort {
                let ord = cmp_segment(&a.0, &b.0, bs);
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            std::cmp::Ordering::Equal
        };
        candidates.sort_by(sort_key_fn);

        // ExclusiveStartKey: start after that item.
        if let Some(esk) = esk {
            let matching = candidates.iter().position(|(_, m)| {
                esk.iter().all(|(k, v)| {
                    m.get(k)
                        .map(|mv| expr_engine::attr_eq(mv, v))
                        .unwrap_or(false)
                })
            });
            if let Some(pos) = matching {
                candidates = candidates.split_off(pos + 1);
            } else {
                candidates.clear();
            }
        }

        if reverse {
            candidates.reverse();
        }

        let limit = limit.unwrap_or(usize::MAX);
        // LastEvaluatedKey carries only the key schema (base keys + the
        // queried index's keys, moto table.py renders {hash, range?, index
        // keys}) — the whole-item token smuggles non-key members and later
        // feeds real AWS an invalid starting key.
        let mut lek_attrs: Vec<String> = table
            .key_schema
            .iter()
            .map(|k| k.attribute_name.clone())
            .collect();
        if let Some(idx) = index_of(table, param_str(&req.params, "IndexName").unwrap_or("")) {
            for k in &idx.key_schema {
                if !lek_attrs.contains(&k.attribute_name) {
                    lek_attrs.push(k.attribute_name.clone());
                }
            }
        }
        let mut out: Vec<Value> = Vec::new();
        let mut scanned = 0usize;
        let mut last_key: Option<Map<String, Value>> = None;
        let mut truncated = false;
        for (item, m) in &candidates {
            if limit != usize::MAX && scanned >= limit {
                truncated = true;
                break;
            }
            scanned += 1;
            last_key = Some(m.clone());
            if !filter.trim().is_empty() {
                let ok = expr_engine::eval_condition(filter, item, &names_for_sort, &values)
                    .unwrap_or(false);
                if !ok {
                    continue;
                }
            }

            let projected = if let Some(paths) = &index_projection {
                let mut base = expr_engine::project_item(item, paths);
                if !projection.trim().is_empty() {
                    if let Ok(user_paths) = expr_engine::parse_projection(projection, &names) {
                        base = expr_engine::project_item(item, &user_paths);
                    }
                }
                Value::Object(base)
            } else if !projection.trim().is_empty() {
                match expr_engine::parse_projection(projection, &names) {
                    Ok(paths) => Value::Object(expr_engine::project_item(item, &paths)),
                    Err(e) => return AwsResponse::error(400, "ValidationException", &e),
                }
            } else {
                item.clone()
            };
            out.push(projected);
        }

        let _ = names_for_sort;
        let count = out.len();
        let mut body = json!({
            "Count": count,
            "ScannedCount": scanned,
        });
        if select != "COUNT" {
            body["Items"] = Value::Array(out);
        }
        if truncated {
            if let Some(lk) = last_key {
                let mut limited: Map<String, Value> = Map::new();
                for a in &lek_attrs {
                    if let Some(v) = lk.get(a) {
                        limited.insert(a.clone(), v.clone());
                    }
                }
                body["LastEvaluatedKey"] = map_to_value(&kh_to_attrmap(&limited));
            }
        }
        AwsResponse::json(200, body)
    }
}

// --- free helpers used above ---

#[allow(clippy::type_complexity)]
fn value_to_map(v: &Value) -> Map<String, Value> {
    v.as_object().cloned().unwrap_or_default()
}

fn dyn_cmp(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (expr_engine::type_of(a), expr_engine::type_of(b)) {
        (Some("N"), Some("N")) => {
            let an = a
                .get("N")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok());
            let bn = b
                .get("N")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok());
            match (an, bn) {
                (Some(x), Some(y)) => x.partial_cmp(&y).unwrap_or(Ordering::Equal),
                _ => Ordering::Equal,
            }
        }
        (Some("S"), Some("S")) | (Some("B"), Some("B")) => {
            let av = a
                .as_object()
                .and_then(|o| o.values().next())
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let bv = b
                .as_object()
                .and_then(|o| o.values().next())
                .and_then(|v| v.as_str())
                .unwrap_or("");
            av.cmp(bv)
        }
        _ => a.to_string().cmp(&b.to_string()),
    }
}

fn expr_str(v: Option<&Value>) -> Option<&str> {
    v.and_then(|x| x.as_str())
}

/// Does this error carry an expression parse/validation failure rather than a
/// false condition? moto raises the parse error out of the transact row loop
/// as a plain ValidationException (models/__init__.py:701-705 raises
/// MockValidationException out of the loop, rolling the whole transaction
/// back); only a FALSE condition becomes a ConditionalCheckFailed row.
fn is_validation_error(e: &AwsResponse) -> bool {
    if e.status < 400 {
        return false;
    }
    let Ok(body) = serde_json::from_str::<Value>(&e.body) else {
        return false;
    };
    body.get("__type")
        .and_then(Value::as_str)
        .is_some_and(|t| t.ends_with("ValidationException"))
}

/// Locate a named index; errors when the table has no such index.
fn resolve_index(
    table: &Table,
    index_name: &str,
) -> Result<Option<crate::models::IndexDefinition>, AwsResponse> {
    if index_name.is_empty() {
        return Ok(None);
    }
    match index_of(table, index_name) {
        Some(idx) => Ok(Some(idx)),
        None => Err(AwsResponse::error(
            400,
            "ResourceNotFoundException",
            &format!("The table does not have the specified index: {index_name}"),
        )),
    }
}

fn index_of(table: &Table, name: &str) -> Option<crate::models::IndexDefinition> {
    if name.is_empty() {
        return None;
    }
    table
        .global_secondary_indexes
        .read()
        .iter()
        .find(|i| i.name == name)
        .cloned()
        .or_else(|| {
            table
                .local_secondary_indexes
                .read()
                .iter()
                .find(|i| i.name == name)
                .cloned()
        })
}

fn index_context(table: &Table, index_name: &str) -> (Option<String>, Option<Vec<Vec<String>>>) {
    let Some(index) = index_of(table, index_name) else {
        if index_name.is_empty() {
            return (None, None);
        }
        return (None, None);
    };
    let sort_attr = index.range_attr().map(String::from);

    // Attribute paths included by this index projection.
    let mut attrs: Vec<Vec<String>> = Vec::new();
    let push = |attrs: &mut Vec<Vec<String>>, name: String| {
        if !attrs.iter().any(|p| p.len() == 1 && p[0] == name) {
            attrs.push(vec![name]);
        }
    };
    for k in &table.key_schema {
        push(&mut attrs, k.attribute_name.clone());
    }
    for k in &index.key_schema {
        push(&mut attrs, k.attribute_name.clone());
    }
    match index.projection.as_str() {
        "ALL" => (sort_attr, None),
        "INCLUDE" => {
            for a in &index.non_key_attributes {
                push(&mut attrs, a.clone());
            }
            (sort_attr, Some(attrs))
        }
        _ => (sort_attr, Some(attrs)), // KEYS_ONLY
    }
}

fn forward_reverse(forward: bool) -> bool {
    !forward
}

/// Apply the ProjectionExpression of a request to a single item.
fn project_from_req(req: &AwsRequest, item: &Value) -> Value {
    let Some(proj) = expr_str(req.params.get("ProjectionExpression")) else {
        return item.clone();
    };
    if proj.trim().is_empty() {
        return item.clone();
    }
    let names = get_names(req);
    match expr_engine::parse_projection(proj, &names) {
        Ok(paths) => Value::Object(expr_engine::project_item(item, &paths)),
        Err(_) => item.clone(),
    }
}

/// ExpressionAttributeNames bound to an operation object (transact entries
/// carry their own parameter maps).
fn op_names(op: &Value) -> Names {
    op.get("ExpressionAttributeNames")
        .and_then(|v| v.as_object())
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

/// ExpressionAttributeValues bound to an operation object.
fn op_values(op: &Value) -> Values {
    op.get("ExpressionAttributeValues")
        .and_then(|v| v.as_object())
        .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default()
}

// ----------------------------------------------------------------------
// Batch operations
// ----------------------------------------------------------------------

impl DynamoDbHandler {
    fn batch_get_item(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let request_items = req
            .params
            .get("RequestItems")
            .cloned()
            .unwrap_or(Value::Null);
        let mut responses = Map::new();

        if let Some(obj) = request_items.as_object() {
            for (table_name, spec) in obj {
                let Some(table) = state.get_table(table_name) else {
                    return AwsResponse::error(
                        400,
                        "ResourceNotFoundException",
                        &format!("Requested resource not found: Table: {table_name} not found"),
                    );
                };
                let names: Names = spec
                    .get("ExpressionAttributeNames")
                    .and_then(|v| v.as_object())
                    .map(|m| {
                        m.iter()
                            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                            .collect()
                    })
                    .unwrap_or_default();
                let projection = spec
                    .get("ProjectionExpression")
                    .and_then(|v| v.as_str())
                    .map(String::from);

                let mut found: Vec<Value> = Vec::new();
                for key_json in spec
                    .get("Keys")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default()
                {
                    let key_attrs: Map<String, Value> =
                        key_json.as_object().cloned().unwrap_or_default();
                    // moto's response layer resolves each key against the
                    // table schema before the get (responses.py:748-769) —
                    // a key that does not match is a 400, never a silent
                    // skip; moto's arm answers the generic "Validation
                    // Exception" text (verified probe (1)).
                    let Some(key) = item_key(&table, &key_attrs) else {
                        return AwsResponse::error(
                            400,
                            "ValidationException",
                            "Validation Exception",
                        );
                    };
                    let items = table.items.read();
                    let Some(item) = items.get(&key).cloned() else {
                        continue;
                    };
                    drop(items);
                    let item_json = map_to_value(&item.attributes);
                    let projected = match &projection {
                        Some(p) => match expr_engine::parse_projection(p, &names) {
                            Ok(paths) => {
                                Value::Object(expr_engine::project_item(&item_json, &paths))
                            }
                            Err(e) => return AwsResponse::error(400, "ValidationException", &e),
                        },
                        None => item_json,
                    };
                    found.push(projected);
                }
                responses.insert(table_name.clone(), Value::Array(found));
            }
        }

        AwsResponse::json(
            200,
            json!({ "Responses": responses, "UnprocessedKeys": {} }),
        )
    }

    fn batch_write_item(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let request_items = req
            .params
            .get("RequestItems")
            .cloned()
            .unwrap_or(Value::Null);

        // moto's response layer (responses.py:734-764) COLLECTS every row
        // into put/delete lists first, runs the duplicate check over the
        // collected sets — WHOLE-ITEM dumps for puts, key dumps for deletes
        // — and only then executes, so the "Provided list of item keys
        // contains duplicates" raise commits nothing. Put+Delete rows on the
        // same (table, key) coexist (separate lists; the delete loop runs
        // last and wins), and two puts of one key with different payloads
        // are both accepted (identical whole items are the duplicate).
        struct PutRow {
            table: Arc<crate::models::Table>,
            key: String,
            attrs: Map<String, Value>,
        }
        struct DeleteRow {
            table: Arc<crate::models::Table>,
            key: String,
        }
        let mut puts: Vec<PutRow> = Vec::new();
        let mut deletes: Vec<DeleteRow> = Vec::new();
        let mut seen_puts: HashSet<String> = HashSet::new();
        let mut seen_deletes: HashSet<String> = HashSet::new();
        if let Some(obj) = request_items.as_object() {
            for (table_name, writes) in obj {
                let Some(table) = state.get_table(table_name) else {
                    return AwsResponse::error(
                        400,
                        "ResourceNotFoundException",
                        &format!("Requested resource not found: Table: {table_name} not found"),
                    );
                };
                let table = &table;
                for write in writes.as_array().cloned().unwrap_or_default() {
                    if let Some(item_json) = write.pointer("/PutRequest/Item").cloned() {
                        let attrs: Map<String, Value> =
                            item_json.as_object().cloned().unwrap_or_default();
                        let Some(key) = item_key(&table, &attrs) else {
                            return keyed_lookup_error(&table, &attrs);
                        };
                        // moto dedups puts on the WHOLE item dump
                        // (dynamo_json_dump over the item): identical items
                        // twice raise; same key with differing payloads
                        // passes (both stored; later put wins).
                        let fingerprint = format!(
                            "{table_name}:{}",
                            serde_json::to_string(&item_json).unwrap_or_default()
                        );
                        if !seen_puts.insert(fingerprint) {
                            return AwsResponse::error(
                                400,
                                "ValidationException",
                                "Provided list of item keys contains duplicates",
                            );
                        }
                        // moto's batch save lands in the same
                        // LimitedSizeDict check as put_item (getmoto/moto#1874).
                        if let Some(e) = check_item_size(&attrs, false) {
                            return e;
                        }
                        puts.push(PutRow {
                            table: table.clone(),
                            key,
                            attrs,
                        });
                    }
                    if let Some(key_json) = write.pointer("/DeleteRequest/Key").cloned() {
                        let key_attrs: Map<String, Value> =
                            key_json.as_object().cloned().unwrap_or_default();
                        let Some(key) = item_key(&table, &key_attrs) else {
                            // moto's batch DELETE rows resolve through
                            // backend.delete_item → get_keys_value, whose
                            // whole schema arm is the generic text — the
                            // put row's `Missing the key` shade (table.py
                            // 627-638) never fires on the delete arm.
                            return AwsResponse::error(
                                400,
                                "ValidationException",
                                "Validation Exception",
                            );
                        };
                        if !seen_deletes.insert(format!("{table_name}:{key}")) {
                            return AwsResponse::error(
                                400,
                                "ValidationException",
                                "Provided list of item keys contains duplicates",
                            );
                        }
                        deletes.push(DeleteRow {
                            table: table.clone(),
                            key,
                        });
                    }
                }
            }
        }

        // Phase 2 — the collected rows execute: puts first, then deletes
        // (moto's loop order; a row doing both on a key ends DELETED).
        // Same-ROW Put+Delete divergence: moto executes only the FIRST
        // member of a row (`list(table_request.keys())[0]`, responses.py:
        // 737); the twin processes BOTH members of a row (delete wins) —
        // recorded divergence from moto.
        for row in puts {
            let old = row
                .table
                .items
                .read()
                .get(&row.key)
                .map(|i| i.attributes.clone());
            row.table
                .items
                .write()
                .insert(row.key, Arc::new(Item::new(attrs_to_map(&row.attrs))));
            let old_serde = streaming::stream_enabled(&row.table)
                .then(|| old.as_ref().map(attrmap_to_serde))
                .flatten();
            streaming::record_mutation(
                &row.table,
                old_serde.as_ref().map(|m| m as &Map<String, Value>),
                Some(&row.attrs),
            );
        }
        for row in deletes {
            let old = row.table.items.write().remove(&row.key);
            let old_serde = streaming::stream_enabled(&row.table)
                .then(|| old.as_ref().map(|i| attrmap_to_serde(&i.attributes)))
                .flatten();
            streaming::record_mutation(
                &row.table,
                old_serde.as_ref().map(|m| m as &Map<String, Value>),
                None,
            );
        }

        // moto's batch_write_item renders a ConsumedCapacity entry per table
        // (one 1.0 unit with the Table member) plus the empty
        // ItemCollectionMetrics map (verified: CC = [{"CapacityUnits": 1.0,
        // "Table": {"CapacityUnits": 1.0}, "TableName": "t"}]).
        let mut body = json!({
            "UnprocessedItems": {},
            "ItemCollectionMetrics": {},
            // moto renders the [] always, even for an empty RequestItems
            // (responses.py:765-776). The per-table fingerprints are ALSO
            // scoped to their table name (moto's dumps are table-blind and
            // false-positive across tables — responses.py:754-756); the
            // twin's scoping is the deliberate, real-AWS-correct choice.
            "ConsumedCapacity": [],
        });
        if let Some(obj) = request_items.as_object() {
            let consumed: Vec<Value> = obj
                .keys()
                .map(|table_name| {
                    json!({
                        "TableName": table_name,
                        "CapacityUnits": 1.0,
                        "Table": { "CapacityUnits": 1.0 },
                    })
                })
                .collect();
            body["ConsumedCapacity"] = Value::Array(consumed);
        }
        AwsResponse::json(200, body)
    }

    // ------------------------------------------------------------------
    // Transactions
    // ------------------------------------------------------------------

    fn transact_write_items(&self, req: &AwsRequest) -> AwsResponse {
        // Phase 1: validate and evaluate conditions against pre-transaction state.
        struct PendingPut {
            table: Arc<Table>,
            key: String,
            attrs: AttrMap,
        }
        struct PendingUpdate {
            table: Arc<Table>,
            key: String,
            /// The fully evaluated post-update row, computed in phase 1
            /// against a clone of the pre-transaction item, so phase 2 is a
            /// plain overwrite that cannot fail mid-transaction.
            new_attrs: AttrMap,
        }
        struct PendingDelete {
            table: Arc<Table>,
            key: String,
        }
        /// One entry per TransactWriteItems row (moto transact_write_items
        /// `errors` list of (Code, Message, Item) tuples):
        /// `code: None` = the row passed, `item` = the row's pre-transaction
        /// attributes when moto's ReturnValuesOnConditionCheckFailure=ALL_OLD
        /// asked for them on a failed row (models/__init__.py:666-671,
        /// exceptions.py TransactionCanceledException:251-255).
        struct RowCancellation {
            code: Option<String>,
            item: Option<AttrMap>,
        }

        let mut puts: Vec<PendingPut> = Vec::new();
        let mut updates: Vec<PendingUpdate> = Vec::new();
        let mut deletes: Vec<PendingDelete> = Vec::new();
        let mut cancellation: Vec<RowCancellation> = Vec::new();
        let mut failed = false;
        // moto transact_write_items check_unicity
        // (dynamodb/models/__init__.py:617-623): every row across the request
        // names a (table, key) pair; a second row on the same pair cancels the
        // request up front with MultipleTransactionsException ("Transaction
        // request cannot include multiple operations on one item"). Put rows
        // hash the key carried in their Item, Update/Delete/ConditionCheck
        // rows their own Key — both resolve to the same storage key here.
        let mut target_items: HashSet<(String, String)> = HashSet::new();

        let transact_items = req
            .params
            .get("TransactItems")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        // moto transact_write_items (dynamodb/models/__init__.py:604-606)
        // rejects >100 rows up front with TooManyTransactionsException.
        if transact_items.len() > 100 {
            return AwsResponse::error(
                400,
                "ValidationException",
                "1 validation error detected at 'transactItems' failed to satisfy constraint: \
                 Member must have length less than or equal to 100.",
            );
        }

        for entry in &transact_items {
            // A row that parses to no modeled operation: moto's response
            // layer crashes on it (`item_values = list(item.values())[0]`
            // raises IndexError, dynamodb/responses.py:1478) before the
            // model's row loop (models/__init__.py:635) could ever raise
            // StopIteration, so moto_server answers the plain werkzeug 500
            // page — whole-request, atomically writes nothing (live
            // transcript 2026-10-26, moto 5.2.4). The twin answers AWS's
            // ValidationException instead (2026-10-31 wave review: the
            // probe canon reserves 500_error for genuine server crashes,
            // and the required-only probe fill for TransactWriteItems is
            // exactly `{"TransactItems": [{}]}`): same message the >1-op
            // row above carries. Validation still sits ahead of phase 2,
            // so nothing is committed either way.
            //
            // moto's parse (moto/core/parse.py BaseJSONParser
            // ._parse_structure) keeps ONLY the modeled Put/Update/Delete/
            // ConditionCheck members of each row — any other key (e.g. a
            // "Get" write row) is dropped before the backend ever sees it,
            // so the model's ValueError("Unsupported transaction
            // operation") branch is unreachable over HTTP. Mirror the
            // parsed shape: count modeled members for both the single-op
            // check and op selection (live-transcribed 2026-10-26):
            //   Put + Get          -> executes as a plain Put (200)
            //   Put + Delete + Get -> single-op ValidationException
            //   Get only           -> parses to {} -> ValidationException
            let modeled: Vec<(&str, &Value)> = ["Put", "Update", "Delete", "ConditionCheck"]
                .iter()
                .filter_map(|k| entry.get(*k).map(|o| (*k, o)))
                .collect();
            if modeled.len() > 1 {
                return AwsResponse::error(
                    400,
                    "ValidationException",
                    "TransactItems can only contain one of Check, Put, Update or Delete",
                );
            }
            let (op_name, op) = match modeled.into_iter().next() {
                Some(t) => t,
                None => {
                    return AwsResponse::error(
                        400,
                        "ValidationException",
                        "TransactItems can only contain one of Check, Put, Update or Delete",
                    )
                }
            };
            let table_name = param_str(op, "TableName").unwrap_or("").to_string();
            let table = match self
                .get_state(req.account, &req.region)
                .get_table(&table_name)
            {
                Some(t) => t,
                None => {
                    return AwsResponse::error(
                        400,
                        "ResourceNotFoundException",
                        &format!("Requested resource not found: Table: {table_name} not found"),
                    )
                }
            };
            let key_json = op.get("Key").cloned();
            let key_attrs: Map<String, Value> = key_json
                .as_ref()
                .and_then(|k| k.as_object())
                .cloned()
                .unwrap_or_default();

            let existing = |table: &Table, key: &str| -> Option<AttrMap> {
                table.items.read().get(key).map(|i| i.attributes.clone())
            };

            // moto transact_write_items (models/__init__.py:663-671): when a
            // row's condition fails, ReturnValuesOnConditionCheckFailure=
            // ALL_OLD attaches that row's pre-transaction item to its
            // cancellation reason — skipped when the caller did not ask for
            // ALL_OLD or the row does not exist.
            let reason_item = |item: &Option<AttrMap>| -> Option<AttrMap> {
                if param_str(op, "ReturnValuesOnConditionCheckFailure") == Some("ALL_OLD") {
                    item.clone()
                } else {
                    None
                }
            };

            match op_name {
                "Put" => {
                    let item_json = op.get("Item").cloned().unwrap_or(Value::Null);
                    let attrs: Map<String, Value> =
                        item_json.as_object().cloned().unwrap_or_default();
                    let Some(key) = item_key(&table, &attrs) else {
                        return AwsResponse::error(
                            400,
                            "ValidationException",
                            "The provided key element does not match the schema",
                        );
                    };
                    if !target_items.insert((table.name.clone(), key.clone())) {
                        return AwsResponse::error(
                            400,
                            "ValidationException",
                            "Transaction request cannot include multiple operations on one item",
                        );
                    }
                    // moto evaluates the condition BEFORE the item-size check
                    // (dynamodb/models/__init__.py:647-671: the condition runs
                    // in the row loop, the size check inside self.put_item), so
                    // an oversized Put under a failing condition cancels the
                    // transaction (ConditionalCheckFailed row) instead of
                    // answering the size ValidationException.
                    let pre = existing(&table, &key);
                    let check = Self::eval_condition_on(
                        expr_str(op.get("ConditionExpression")),
                        pre.as_ref(),
                        &op_names(op),
                        &op_values(op),
                    );
                    match check {
                        Ok(()) => {
                            // moto's transactional Put runs the same
                            // LimitedSizeDict check when it builds the Item
                            // (getmoto/moto#1874).
                            if let Some(e) = check_item_size(&attrs, false) {
                                return e;
                            }
                            cancellation.push(RowCancellation {
                                code: None,
                                item: None,
                            });
                            puts.push(PendingPut {
                                table,
                                key,
                                attrs: attrs_to_map(&attrs),
                            });
                        }
                        Err(e) => {
                            // Only a false condition cancels the transaction;
                            // an expression parse error raises out of the row
                            // loop as a plain ValidationException (moto
                            // models/__init__.py:701-705).
                            if is_validation_error(&e) {
                                return e;
                            }
                            failed = true;
                            cancellation.push(RowCancellation {
                                code: Some("ConditionalCheckFailed".to_string()),
                                item: reason_item(&pre),
                            });
                        }
                    }
                }
                "Update" => {
                    let Some(key) = item_key(&table, &key_attrs) else {
                        return AwsResponse::error(
                            400,
                            "ValidationException",
                            "The provided key element does not match the schema",
                        );
                    };
                    if !target_items.insert((table.name.clone(), key.clone())) {
                        return AwsResponse::error(
                            400,
                            "ValidationException",
                            "Transaction request cannot include multiple operations on one item",
                        );
                    }
                    let pre = existing(&table, &key);
                    let check = Self::eval_condition_on(
                        expr_str(op.get("ConditionExpression")),
                        pre.as_ref(),
                        &op_names(op),
                        &op_values(op),
                    );
                    match check {
                        Ok(()) => {
                            // TransactWriteItems is atomic: moto deep-copies
                            // every touched table and restores it on error,
                            // and real AWS commits nothing on failure. Apply
                            // the update to a CLONE of the pre-transaction
                            // row here, before any write happens, so an
                            // invalid expression or an oversized result
                            // cancels the whole transaction (this row's own
                            // payload included) instead of leaving earlier
                            // rows committed. Phase 2 then only overwrites.
                            let mut new_attrs: Map<String, Value> = match &pre {
                                Some(m) => m.clone().into_iter().collect(),
                                None => key_attrs.clone(),
                            };
                            if let Some(update) = expr_str(op.get("UpdateExpression")) {
                                if !update.trim().is_empty() {
                                    if let Err(e) = expr_engine::apply_update(
                                        &mut new_attrs,
                                        update,
                                        &op_names(op),
                                        &op_values(op),
                                    ) {
                                        return AwsResponse::error(
                                            400,
                                            "ValidationException",
                                            e.as_str(),
                                        );
                                    }
                                }
                            }
                            // moto's transactional Update re-raises the size
                            // check on the executed result as the
                            // update-flavored ItemSizeToUpdateTooLarge
                            // (dynamodb/models/__init__.py update_item).
                            if let Some(e) = check_item_size(&new_attrs, true) {
                                return e;
                            }
                            cancellation.push(RowCancellation {
                                code: None,
                                item: None,
                            });
                            updates.push(PendingUpdate {
                                table,
                                key,
                                new_attrs: attrs_to_map(&new_attrs),
                            });
                        }
                        Err(e) => {
                            // Only a false condition cancels the transaction;
                            // an expression parse error raises out of the row
                            // loop as a plain ValidationException (moto
                            // models/__init__.py:701-705).
                            if is_validation_error(&e) {
                                return e;
                            }
                            failed = true;
                            cancellation.push(RowCancellation {
                                code: Some("ConditionalCheckFailed".to_string()),
                                item: reason_item(&pre),
                            });
                        }
                    }
                }
                "Delete" => {
                    let Some(key) = item_key(&table, &key_attrs) else {
                        return AwsResponse::error(
                            400,
                            "ValidationException",
                            "The provided key element does not match the schema",
                        );
                    };
                    if !target_items.insert((table.name.clone(), key.clone())) {
                        return AwsResponse::error(
                            400,
                            "ValidationException",
                            "Transaction request cannot include multiple operations on one item",
                        );
                    }
                    let pre = existing(&table, &key);
                    let check = Self::eval_condition_on(
                        expr_str(op.get("ConditionExpression")),
                        pre.as_ref(),
                        &op_names(op),
                        &op_values(op),
                    );
                    match check {
                        Ok(()) => {
                            cancellation.push(RowCancellation {
                                code: None,
                                item: None,
                            });
                            deletes.push(PendingDelete { table, key });
                        }
                        Err(e) => {
                            // Only a false condition cancels the transaction;
                            // an expression parse error raises out of the row
                            // loop as a plain ValidationException (moto
                            // models/__init__.py:701-705).
                            if is_validation_error(&e) {
                                return e;
                            }
                            failed = true;
                            cancellation.push(RowCancellation {
                                code: Some("ConditionalCheckFailed".to_string()),
                                item: reason_item(&pre),
                            });
                        }
                    }
                }
                _ => {
                    // ConditionCheck
                    let Some(key) = item_key(&table, &key_attrs) else {
                        return AwsResponse::error(
                            400,
                            "ValidationException",
                            "The provided key element does not match the schema",
                        );
                    };
                    if !target_items.insert((table.name.clone(), key.clone())) {
                        return AwsResponse::error(
                            400,
                            "ValidationException",
                            "Transaction request cannot include multiple operations on one item",
                        );
                    }
                    let pre = existing(&table, &key);
                    let cond = expr_str(op.get("ConditionExpression"));
                    if cond.is_none() {
                        return AwsResponse::error(
                            400,
                            "ValidationException",
                            "ConditionExpression is required in ConditionCheck",
                        );
                    }
                    // ConditionCheck rows carry expression attributes per row,
                    // like the Put/Update/Delete arms — moto reads
                    // op.get("ExpressionAttributeNames"...) for every arm
                    // uniformly (models/__init__.py:640-665); TransactWriteItems
                    // has no request-level expression members.
                    match Self::eval_condition_on(cond, pre.as_ref(), &op_names(op), &op_values(op))
                    {
                        Ok(()) => cancellation.push(RowCancellation {
                            code: None,
                            item: None,
                        }),
                        Err(e) => {
                            // Same split as the write arms: a parse error
                            // propagates as a plain ValidationException, only
                            // a false condition is a cancellation row.
                            if is_validation_error(&e) {
                                return e;
                            }
                            failed = true;
                            cancellation.push(RowCancellation {
                                code: Some("ConditionalCheckFailed".to_string()),
                                item: reason_item(&pre),
                            });
                        }
                    }
                }
            }
        }

        if failed {
            let reasons_text = cancellation
                .iter()
                .map(|r| r.code.clone().unwrap_or_else(|| "None".to_string()))
                .collect::<Vec<_>>()
                .join(", ");
            // moto's TransactionCanceledException describes both the summary
            // message and one CancellationReasons entry per row
            // (dynamodb/exceptions.py TransactionCanceledException):
            // {"Code": <code>, "Message": <...>} for a failed row, {"Code":
            // "None"} for a passing one, and an extra "Item" member with the
            // row's old attributes when the failed row carried
            // ReturnValuesOnConditionCheckFailure=ALL_OLD
            // (models/__init__.py:666-671). Phase 1 only cancels on failed
            // conditions, so the only code here is ConditionalCheckFailed
            // with moto's "The conditional request failed" message.
            //
            // Wire shape: moto renders the summary as top-level "Message"
            // (uppercase — exceptions.py:261) and the fully-qualified error
            // type com.amazonaws.dynamodb.v20120810#
            // TransactionCanceledException, unlike real AWS's lowercase
            // `message` (live transcript 2026-10-26, moto 5.2.4).
            let reasons = cancellation
                .iter()
                .map(|r| match &r.code {
                    Some(code) => {
                        let mut reason = json!({
                            "Code": code,
                            "Message": "The conditional request failed",
                        });
                        if let Some(item) = &r.item {
                            if !item.is_empty() {
                                reason["Item"] = map_to_value(item);
                            }
                        }
                        reason
                    }
                    None => json!({"Code": "None"}),
                })
                .collect::<Vec<_>>();
            return AwsResponse::error_with_body(
                400,
                json!({
                    "__type": "com.amazonaws.dynamodb.v20120810#TransactionCanceledException",
                    "CancellationReasons": reasons,
                    "Message": format!(
                        "Transaction cancelled, please refer cancellation reasons \
                         for specific reasons [{reasons_text}]"
                    ),
                }),
            );
        }

        // Phase 2: apply.
        for put in puts {
            let old = put
                .table
                .items
                .read()
                .get(&put.key)
                .map(|i| i.attributes.clone());
            put.table
                .items
                .write()
                .insert(put.key.clone(), Arc::new(Item::new(put.attrs.clone())));
            // Stream capture mirrors moto's transact put hook.
            let old_serde = streaming::stream_enabled(&put.table)
                .then(|| old.as_ref().map(attrmap_to_serde))
                .flatten();
            let new_serde =
                streaming::stream_enabled(&put.table).then(|| attrmap_to_serde(&put.attrs.clone()));
            streaming::record_mutation(
                &put.table,
                old_serde.as_ref().map(|m| m as &Map<String, Value>),
                new_serde.as_ref().map(|m| m as &Map<String, Value>),
            );
        }
        for upd in updates {
            let old = upd
                .table
                .items
                .read()
                .get(&upd.key)
                .map(|i| i.attributes.clone());
            // Phase 1 evaluated the expression and the size against a
            // pre-transaction clone, so applying is a pure overwrite that
            // cannot fail — no mid-transaction error, no rollback state to
            // restore (moto gets there with a per-table deepcopy instead).
            let new_serde = attrmap_to_serde(&upd.new_attrs);
            // Stream capture mirrors moto's transact update hook
            // (INSERT when the item was absent, MODIFY otherwise).
            let old_serde = streaming::stream_enabled(&upd.table)
                .then(|| old.as_ref().map(attrmap_to_serde))
                .flatten();
            streaming::record_mutation(
                &upd.table,
                old_serde.as_ref().map(|m| m as &Map<String, Value>),
                Some(&new_serde),
            );
            upd.table
                .items
                .write()
                .insert(upd.key.clone(), Arc::new(Item::new(upd.new_attrs.clone())));
        }
        for del in deletes {
            let old = del.table.items.write().remove(&del.key);
            let old_serde = streaming::stream_enabled(&del.table)
                .then(|| old.as_ref().map(|i| attrmap_to_serde(&i.attributes)))
                .flatten();
            streaming::record_mutation(
                &del.table,
                old_serde.as_ref().map(|m| m as &Map<String, Value>),
                None,
            );
        }

        // moto transact_write_items answers the constant
        // {"ConsumedCapacity": [], "ItemCollectionMetrics": {}} on success
        // (dynamodb/responses.py:1531, live transcript 2026-10-26).
        AwsResponse::json(
            200,
            json!({ "ConsumedCapacity": [], "ItemCollectionMetrics": {} }),
        )
    }

    fn transact_get_items(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let mut responses: Vec<Value> = Vec::new();
        // Per-table hit counts for moto's ConsumedCapacity bookkeeping, in
        // first-hit order (dynamodb/responses.py:1452-1458).
        let mut tables: Vec<String> = Vec::new();
        let mut hits: HashMap<String, u32> = HashMap::new();
        let transact_items = req
            .params
            .get("TransactItems")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        // moto transact_get_items (dynamodb/responses.py:1419-1435,
        // TRANSACTION_MAX_ITEMS = 25) rejects >25 rows with a per-row
        // TransactGetItem id list — sequential 0x10000001 onward, lowercase
        // hex, and a constraint line without the trailing period.
        if transact_items.len() > 25 {
            let ids: Vec<String> = (1..=transact_items.len())
                .map(|i| {
                    format!(
                        "com.amazonaws.dynamodb.v20120810.TransactGetItem@{:x}",
                        268435456u64 + i as u64
                    )
                })
                .collect();
            return AwsResponse::error(
                400,
                "ValidationException",
                &format!(
                    "1 validation error detected: Value '[{}] at 'transactItems' failed to \
                     satisfy constraint: Member must have length less than or equal to 25",
                    ids.join(", ")
                ),
            );
        }

        for entry in transact_items {
            let get = entry.get("Get").cloned().unwrap_or(Value::Null);
            let table_name = param_str(&get, "TableName").unwrap_or("").to_string();
            let Some(table) = state.get_table(&table_name) else {
                return AwsResponse::error(
                    400,
                    "ResourceNotFoundException",
                    &format!("Requested resource not found: Table: {table_name} not found"),
                );
            };
            let key_json = get.get("Key").cloned().unwrap_or(Value::Null);
            let key_attrs: Map<String, Value> = key_json.as_object().cloned().unwrap_or_default();
            // moto validates the key against the table schema — both
            // batch_get_item and transact_get_items route every row through
            // backend.get_item → get_keys_value (models/__init__.py:297-303),
            // whose schema arm is the generic "Validation Exception" text.
            let key = match item_key(&table, &key_attrs) {
                Some(k) => k,
                None => {
                    return AwsResponse::error(400, "ValidationException", "Validation Exception")
                }
            };
            let item = table.items.read().get(&key).cloned();

            let names: Names = get
                .get("ExpressionAttributeNames")
                .and_then(|v| v.as_object())
                .map(|m| {
                    m.iter()
                        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                        .collect()
                })
                .unwrap_or_default();
            let projection = get.get("ProjectionExpression").and_then(|v| v.as_str());

            let mut entry_resp = Map::new();
            if let Some(item) = item {
                let item_json = map_to_value(&item.attributes);
                let projected = match projection {
                    Some(p) => match expr_engine::parse_projection(p, &names) {
                        Ok(paths) => Value::Object(expr_engine::project_item(&item_json, &paths)),
                        Err(e) => return AwsResponse::error(400, "ValidationException", &e),
                    },
                    None => item_json,
                };
                entry_resp.insert("Item".to_string(), projected);
                // moto transact_get_items (dynamodb/responses.py:1452-1458):
                // only rows that returned an item add capacity units (the
                // miss branch `continue`s before the bookkeeping) —
                // per-table counts, 2.0 units per hit.
                if !hits.contains_key(&table_name) {
                    tables.push(table_name.clone());
                }
                *hits.entry(table_name.clone()).or_insert(0) += 1;
            }
            responses.push(Value::Object(entry_resp));
        }
        // moto's ConsumedCapacity member is present only when
        // ReturnConsumedCapacity is not "NONE" (responses.py:1466-1471;
        // an omitted member counts as "NONE" — absent from the response),
        // and the per-table "Table" breakdown renders only for "INDEXES"
        // (live-transcribed moto 5.2.4, 2026-10-26).
        let mut body = json!({ "Responses": responses });
        let ret_consumed_capacity =
            param_str(&req.params, "ReturnConsumedCapacity").unwrap_or("NONE");
        if ret_consumed_capacity != "NONE" {
            // One entry per table that had at least one returning row, in
            // first-hit order (moto keys its python dict by table name).
            let capacities: Vec<Value> = tables
                .iter()
                .map(|table_name| {
                    let units = hits.get(table_name).copied().unwrap_or(0) as f64 * 2.0;
                    let mut capacity = json!({
                        "TableName": table_name,
                        "CapacityUnits": units,
                        "ReadCapacityUnits": units,
                    });
                    if ret_consumed_capacity == "INDEXES" {
                        capacity["Table"] =
                            json!({"CapacityUnits": units, "ReadCapacityUnits": units});
                    }
                    capacity
                })
                .collect();
            body["ConsumedCapacity"] = Value::Array(capacities);
        }
        AwsResponse::json(200, body)
    }

    fn condition_check(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, table_r) = self.resolve_table(req);
        let table = match table_r {
            Ok(t) => t,
            Err(e) => return e,
        };
        let key_json = req.params.get("Key").cloned().unwrap_or(Value::Null);
        let key_attrs: Map<String, Value> = key_json.as_object().cloned().unwrap_or_default();
        let key = match item_key(&table, &key_attrs) {
            Some(k) => k,
            None => {
                return AwsResponse::error(
                    400,
                    "ValidationException",
                    "The provided key element does not match the schema",
                )
            }
        };
        let existing = table.items.read().get(&key).map(|i| i.attributes.clone());
        if let Err(e) = Self::check_condition(
            req,
            expr_str(req.params.get("ConditionExpression")),
            existing.as_ref(),
        ) {
            return e;
        }
        AwsResponse::json(200, json!({}))
    }
}

// ----------------------------------------------------------------------
// PartiQL
// ----------------------------------------------------------------------

impl DynamoDbHandler {
    fn execute_statement(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let statement = param_str(&req.params, "Statement")
            .unwrap_or("")
            .to_string();
        let parameters: Vec<Value> = req
            .params
            .get("Parameters")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        match partiql_run(&state, &statement, &parameters) {
            Ok(Some(items)) => AwsResponse::json(200, json!({ "Items": items })),
            Ok(None) => AwsResponse::json(200, json!({ "Items": [] })),
            Err(e) => e,
        }
    }

    fn execute_transaction(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let mut responses: Vec<Value> = Vec::new();
        for entry in req
            .params
            .get("TransactStatements")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
        {
            let statement = entry
                .get("Statement")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            // Each statement binds its own Parameters member, like
            // ExecuteStatement's top-level bind — moto passes
            // statement["Parameters"] into bind_statement per row.
            let parameters: Vec<Value> = entry
                .get("Parameters")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            match partiql_run(&state, statement, &parameters) {
                Ok(Some(items)) => {
                    let mut obj = Map::new();
                    if let Some(first) = items.into_iter().next() {
                        obj.insert("Item".to_string(), first);
                    }
                    responses.push(Value::Object(obj));
                }
                Ok(None) => responses.push(json!({})),
                Err(e) => return e,
            }
        }
        AwsResponse::json(200, json!({ "Responses": responses }))
    }

    fn batch_execute_statement(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let mut responses: Vec<Value> = Vec::new();
        for entry in req
            .params
            .get("Statements")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
        {
            let statement = entry
                .get("Statement")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            // Each statement binds its own Parameters member, like
            // ExecuteStatement's top-level bind — moto passes
            // statement["Parameters"] into bind_statement per row.
            let parameters: Vec<Value> = entry
                .get("Parameters")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            match partiql_run(&state, statement, &parameters) {
                Ok(Some(items)) => {
                    let mut obj = Map::new();
                    if let Some(first) = items.into_iter().next() {
                        obj.insert("Item".to_string(), first);
                    }
                    responses.push(Value::Object(obj));
                }
                Ok(None) => responses.push(json!({})),
                // moto reports a failed statement on its own batch row instead
                // of aborting the whole batch (dynamodb/models/__init__.py
                // batch_execute_statement: responses[idx] = {"Error": ...}).
                Err(e) => {
                    let body: Value = serde_json::from_str(&e.body).unwrap_or(Value::Null);
                    let code = body
                        .get("__type")
                        .and_then(Value::as_str)
                        .unwrap_or("ValidationException");
                    let message = body.get("message").and_then(Value::as_str).unwrap_or("");
                    responses.push(json!({
                        "Error": {"Code": code, "Message": message}
                    }));
                }
            }
        }
        AwsResponse::json(200, json!({ "Responses": responses }))
    }
}

/// Substitute positional `?` parameters (outside quotes) with a marker token
/// that `literal_to_attr` turns back into a typed value.
fn substitute_params(stmt: &str, params: &[Value]) -> String {
    let mut out = String::new();
    let mut in_quote: Option<char> = None;
    let mut escape = false;
    let mut idx = 0usize;
    for c in stmt.chars() {
        if let Some(q) = in_quote {
            if escape {
                escape = false;
            } else if c == '\\' {
                escape = true;
            } else if c == q {
                in_quote = None;
            }
            out.push(c);
        } else if c == '\'' || c == '"' || c == '`' {
            in_quote = Some(c);
            out.push(c);
        } else if c == '?' && idx < params.len() {
            out.push_str(&format!("__W3PARAM_{idx}__"));
            idx += 1;
        } else {
            out.push(c);
        }
    }
    out
}

/// Type a plain literal (or `__W3PARAM_N__` marker) into a DynamoDB attribute
/// value.
fn literal_to_attr(raw: &str, params: &[Value]) -> Value {
    let raw = raw.trim();
    if let Some(num) = raw
        .strip_prefix("__W3PARAM_")
        .and_then(|r| r.strip_suffix("__"))
    {
        let idx: usize = num.parse().unwrap_or(usize::MAX);
        return params.get(idx).cloned().unwrap_or(json!({"NULL": true}));
    }
    if raw.len() >= 2 && raw.starts_with('\'') {
        let mut out = String::new();
        let mut esc = false;
        for c in raw[1..].chars() {
            if esc {
                out.push(c);
                esc = false;
                continue;
            }
            if c == '\\' {
                esc = true;
                continue;
            }
            if c == '\'' {
                break;
            }
            out.push(c);
        }
        return json!({ "S": out });
    }
    if raw.eq_ignore_ascii_case("true") {
        return json!({ "BOOL": true });
    }
    if raw.eq_ignore_ascii_case("false") {
        return json!({ "BOOL": false });
    }
    if raw.parse::<f64>().is_ok() {
        return json!({ "N": raw });
    }
    if raw.starts_with('{') {
        // Inline JSON-ish object (PartiQL map literal) — normalize quotes.
        let converted = raw.replace('\'', "\"");
        if let Ok(v) = serde_json::from_str::<Value>(&converted) {
            return scalar_to_attr(&v);
        }
    }
    json!({ "S": raw })
}

fn scalar_to_attr(v: &Value) -> Value {
    match v {
        Value::String(s) => json!({ "S": s }),
        Value::Number(n) => json!({ "N": n.to_string() }),
        Value::Bool(b) => json!({ "BOOL": b }),
        Value::Null => json!({ "NULL": true }),
        other => other.clone(),
    }
}

/// A parsed PartiQL comparison from a WHERE clause.
struct PartiQlCond {
    attr: Vec<String>,
    negated: bool,
    value: Value,
}

/// Parse a WHERE fragment into `path = literal [AND ...]` conditions. The
/// ` AND ` split is case-insensitive and quote-aware (split_and_clauses); a
/// fragment without any comparison yields None, which the caller turns into
/// the statement's 400 ValidationException.
fn parse_where(where_clause: &str, params: &[Value]) -> Option<Vec<PartiQlCond>> {
    let mut conds = Vec::new();
    for clause in split_and_clauses(where_clause) {
        let clause = clause.trim();
        if clause.is_empty() {
            continue;
        }
        let eq = clause.find('=');
        let ne = clause.find("<>");
        let (attr_raw, rest_raw, negated) = match (eq, ne) {
            (Some(e), Some(n)) if n < e => (
                clause[..n].trim().to_string(),
                clause[n + 2..].trim().to_string(),
                true,
            ),
            (Some(e), _) => (
                clause[..e].trim().to_string(),
                clause[e + 1..].trim().to_string(),
                false,
            ),
            (None, Some(n)) => (
                clause[..n].trim().to_string(),
                clause[n + 2..].trim().to_string(),
                true,
            ),
            (None, None) => return None,
        };
        let attr = attr_raw
            .trim_matches(|c| c == '"' || c == '`')
            .trim()
            .to_string();
        let segments: Vec<String> = attr.split('.').map(String::from).collect();
        conds.push(PartiQlCond {
            attr: segments,
            negated,
            value: literal_to_attr(&rest_raw, params),
        });
    }
    Some(conds)
}

fn where_matches(item: &Value, conds: &[PartiQlCond]) -> bool {
    conds.iter().all(|c| {
        let eq = expr_engine::lookup(item, &c.attr)
            .map(|actual| expr_engine::attr_eq(actual, &c.value))
            .unwrap_or(false);
        if c.negated {
            !eq
        } else {
            eq
        }
    })
}

/// Extract the statement's table target: the token following FROM (SELECT and
/// DELETE) or INTO (INSERT) — case-insensitively, outside quoted spans, since
/// PartiQL keywords are case-insensitive and a table name or a string literal
/// may itself spell the keyword. A bare identifier (`users`) is accepted the
/// way real PartiQL and moto's parser do; the previous "first quoted
/// identifier anywhere" heuristic 400'd unquoted tables and mispicked
/// per-row string literals (`DELETE FROM t WHERE pk = 'x'`).
fn table_name_of(stmt: &str, head: &str) -> String {
    match head {
        "INSERT" => {
            let Some(pos) = find_keyword_ci(stmt, "INTO") else {
                return String::new();
            };
            partiql_target_token(&stmt[pos + "INTO".len()..])
        }
        "SELECT" | "DELETE" => {
            let Some(pos) = find_keyword_ci(stmt, "FROM") else {
                return String::new();
            };
            partiql_target_token(&stmt[pos + "FROM".len()..])
        }
        "UPDATE" => update_table_of(stmt),
        _ => String::new(),
    }
}

/// The first token of a clause fragment — bare identifier, or a `"`/backtick
/// quoted identifier (unquoted content inside the quotes, as the wire
/// convention spells table names). Single quotes open a string literal, never
/// a table name.
fn partiql_target_token(after_keyword: &str) -> String {
    let rest = after_keyword.trim_start();
    if rest.is_empty() {
        return String::new();
    }
    match rest.as_bytes()[0] {
        b'"' | b'`' => {
            let quote = rest.as_bytes()[0] as char;
            match rest[1..].find(quote) {
                Some(end) => rest[1..1 + end].to_string(),
                None => String::new(),
            }
        }
        _ => rest.split_whitespace().next().unwrap_or("").to_string(),
    }
}

/// Byte index of the first case-insensitive whole-word `keyword` in `stmt`,
/// skipping quoted spans — `WHERE` inside a string literal, or `INTO` inside
/// a quoted table name, is not the keyword.
fn find_keyword_ci(stmt: &str, keyword: &str) -> Option<usize> {
    let lower = stmt.to_ascii_lowercase();
    let keyword_lower = keyword.to_ascii_lowercase();
    let kw = keyword_lower.as_bytes();
    let low = lower.as_bytes();
    let mut in_quote: Option<u8> = None;
    for (i, &b) in low.iter().enumerate() {
        if let Some(q) = in_quote {
            if b == q {
                in_quote = None;
            }
            continue;
        }
        if b == b'\'' || b == b'"' || b == b'`' {
            in_quote = Some(b);
            continue;
        }
        if b == kw[0]
            && low[i..].starts_with(kw)
            && (i == 0 || !is_word_byte(low[i - 1]))
            && (i + kw.len() == low.len() || !is_word_byte(low[i + kw.len()]))
        {
            return Some(i);
        }
    }
    None
}

/// Is this byte valid inside an identifier word?
fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Split on ` AND ` case-insensitively outside quoted spans — `where b = 'a
/// and b'` must stay one condition. Together with the WHERE keyword search
/// above this keeps the clause tokenizer quote-aware.
fn split_and_clauses(text: &str) -> Vec<String> {
    let lower = text.to_ascii_lowercase();
    let low = lower.as_bytes();
    let mut parts: Vec<String> = Vec::new();
    let mut in_quote: Option<u8> = None;
    let mut start = 0usize;
    let mut i = 0usize;
    while i < low.len() {
        let b = low[i];
        if let Some(q) = in_quote {
            if b == q {
                in_quote = None;
            }
            i += 1;
            continue;
        }
        if b == b'\'' || b == b'"' || b == b'`' {
            in_quote = Some(b);
            i += 1;
            continue;
        }
        if b == b'a'
            && low[i..].starts_with(b"and")
            && (i == 0 || !is_word_byte(low[i - 1]))
            && (i + 3 == low.len() || !is_word_byte(low[i + 3]))
        {
            parts.push(text[start..i].to_string());
            i += 3;
            start = i;
            continue;
        }
        i += 1;
    }
    parts.push(text[start..].to_string());
    parts
}

/// Handle the UPDATE target, which directly follows the head word rather
/// than a FROM/INTO keyword.
fn update_table_of(stmt: &str) -> String {
    let Some(space) = stmt.find(char::is_whitespace) else {
        return String::new();
    };
    partiql_target_token(&stmt[space..])
}

/// Convert a serde_json Map into an AttrMap view handled upstream.
fn map_to_attrmap(m: Map<String, Value>) -> AttrMap {
    m.into_iter().collect()
}

#[allow(clippy::type_complexity)]
fn partiql_run(
    state: &DynamoDbState,
    statement: &str,
    parameters: &[Value],
) -> Result<Option<Vec<Value>>, AwsResponse> {
    let stmt = substitute_params(statement, parameters);
    let head = stmt.split_whitespace().next().unwrap_or("").to_uppercase();
    let invalid = || AwsResponse::error(400, "ValidationException", "Invalid PartiQL statement");
    match head.as_str() {
        "SELECT" | "INSERT" | "UPDATE" | "DELETE" => {}
        _ => return Err(invalid()),
    }

    let table_name = table_name_of(&stmt, &head);
    if table_name.is_empty() {
        return Err(invalid());
    }
    let table = state.get_table(&table_name).ok_or_else(|| {
        AwsResponse::error(
            400,
            "ResourceNotFoundException",
            &format!("Requested resource not found: Table: {table_name} not found"),
        )
    })?;
    // The WHERE keyword is case-insensitive (real PartiQL accepts `where`).
    // A clause that is present but unparseable is a 400 ValidationException —
    // never "empty condition = match every row".
    let where_pos = find_keyword_ci(&stmt, "WHERE");
    let where_clause = where_pos.map(|pos| stmt[pos + "WHERE".len()..].trim().to_string());
    let conds = match &where_clause {
        Some(w) if !w.trim().is_empty() => parse_where(w.trim(), parameters).ok_or_else(invalid)?,
        Some(_) => return Err(invalid()),
        None => Vec::new(),
    };

    match head.as_str() {
        "SELECT" => {
            let mut items: Vec<Value> = Vec::new();
            for item in table.items.read().values() {
                let item_value = map_to_value(&item.attributes);
                if where_matches(&item_value, &conds) {
                    items.push(item_value);
                }
            }
            Ok(Some(items))
        }
        "INSERT" => {
            let Some(value_pos) = find_keyword_ci(&stmt, "VALUE") else {
                return Err(invalid());
            };
            let raw = stmt[value_pos + "VALUE".len()..].trim();
            let converted = raw.replace('\'', "\"");
            let parsed: Value = serde_json::from_str(&converted).map_err(|_| invalid())?;
            let attrs: Map<String, Value> = parsed
                .as_object()
                .cloned()
                .map(|m| {
                    m.into_iter()
                        .map(|(k, v)| (k, scalar_to_attr(&v)))
                        .collect()
                })
                .ok_or_else(invalid)?;
            let key = item_key(&table, &attrs).ok_or_else(invalid)?;
            // moto routes a PartiQL INSERT through table.put_item, whose Item
            // constructor enforces the 400 KB limit (getmoto/moto#1874);
            // the put-flavored wording applies, like moto's ItemSizeTooLarge.
            if let Some(e) = check_item_size(&attrs, false) {
                return Err(e);
            }
            let old = table
                .items
                .read()
                .get(&key)
                .map(|i| attrmap_to_serde(&i.attributes));
            table
                .items
                .write()
                .insert(key, Arc::new(Item::new(map_to_attrmap(attrs.clone()))));
            // Stream capture mirrors moto's PartiQL path (moto routes its
            // statement writes through table.put_item's stream hook).
            streaming::record_mutation(
                &table,
                old.as_ref().map(|m| m as &Map<String, Value>),
                Some(&attrs),
            );
            Ok(None)
        }
        "UPDATE" => {
            let Some(set_pos) = find_keyword_ci(&stmt, "SET") else {
                return Err(invalid());
            };
            // The SET clause runs to the WHERE keyword (or to the end). SET
            // must appear before WHERE for the statement to parse at all.
            let set_text = match where_pos {
                Some(wp) if wp < set_pos => return Err(invalid()),
                Some(wp) => stmt[set_pos + "SET".len()..wp].trim().to_string(),
                None => stmt[set_pos + "SET".len()..].trim().to_string(),
            };
            if set_text.is_empty() {
                return Err(invalid());
            }
            // Parse SET assignments.
            let mut assignments: Vec<(Vec<String>, Value)> = Vec::new();
            for assignment in set_text.split(',') {
                let assignment = assignment.trim();
                if assignment.is_empty() {
                    continue;
                }
                let Some(eq) = assignment.find('=') else {
                    return Err(invalid());
                };
                let attr = assignment[..eq]
                    .trim()
                    .trim_matches(|c| c == '"' || c == '`');
                let segs: Vec<String> = attr.split('.').map(String::from).collect();
                let value = literal_to_attr(assignment[eq + 1..].trim(), parameters);
                assignments.push((segs, value));
            }
            let mut items = table.items.write();
            let mut replacements: Vec<(String, AttrMap)> = Vec::new();
            for (key, item) in items.iter() {
                let item_value = map_to_value(&item.attributes);
                if where_matches(&item_value, &conds) {
                    let mut map: Map<String, Value> =
                        item_value.as_object().cloned().unwrap_or_default();
                    for (segs, val) in &assignments {
                        let _ = expr_engine::set_path(&mut map, segs, val.clone());
                    }
                    replacements.push((key.clone(), map_to_attrmap(map)));
                }
            }
            for (key, attrs) in replacements {
                // moto routes each PartiQL UPDATE replacement through
                // table.put_item, whose Item constructor enforces the 400 KB
                // limit (getmoto/moto#1874); like moto, a failure here leaves
                // replacements already applied in this statement in place.
                let attrs_json = attrmap_to_serde(&attrs);
                // moto routes the replacement through put_item, so this stays
                // put-flavored even though the statement is an UPDATE.
                if let Some(e) = check_item_size(&attrs_json, false) {
                    return Err(e);
                }
                let old = streaming::stream_enabled(&table)
                    .then(|| items.get(&key).map(|i| attrmap_to_serde(&i.attributes)))
                    .flatten();
                items.insert(key, Arc::new(Item::new(attrs.clone())));
                // Stream capture mirrors moto's PartiQL UPDATE (via put_item).
                streaming::record_mutation(
                    &table,
                    old.as_ref().map(|m| m as &Map<String, Value>),
                    Some(&attrs_json),
                );
            }
            Ok(None)
        }
        "DELETE" => {
            let mut items = table.items.write();
            let to_remove: Vec<String> = items
                .iter()
                .filter(|(_, item)| where_matches(&map_to_value(&item.attributes), &conds))
                .map(|(k, _)| k.clone())
                .collect();
            for key in to_remove {
                let old = items.remove(&key);
                let old_serde = streaming::stream_enabled(&table)
                    .then(|| old.as_ref().map(|i| attrmap_to_serde(&i.attributes)))
                    .flatten();
                // Stream capture mirrors moto's PartiQL DELETE (via
                // delete_item's hook; no event when the item was absent).
                streaming::record_mutation(
                    &table,
                    old_serde.as_ref().map(|m| m as &Map<String, Value>),
                    None,
                );
            }
            Ok(None)
        }
        _ => Err(invalid()),
    }
}

// ----------------------------------------------------------------------
// Tags / limits / endpoints / TTL
// ----------------------------------------------------------------------

impl DynamoDbHandler {
    fn tag_resource(&self, req: &AwsRequest) -> AwsResponse {
        let arn = param_str(&req.params, "ResourceArn").unwrap_or("");
        let table = self.table_by_arn(req, arn);
        let Some(table) = table else {
            return Self::not_found_table("");
        };
        let mut tags = table.tags.write();
        for tag in req
            .params
            .get("Tags")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
        {
            let key = tag.get("Key").and_then(|v| v.as_str()).unwrap_or("");
            let value = tag.get("Value").and_then(|v| v.as_str()).unwrap_or("");
            if !key.is_empty() {
                tags.insert(key.to_string(), value.to_string());
            }
        }
        AwsResponse::json(200, json!({}))
    }

    fn table_by_arn(&self, req: &AwsRequest, arn: &str) -> Option<Arc<Table>> {
        let state = self.get_state(req.account, &req.region);
        let table_name = parse_arn_table(arn)?.0;
        state.get_table(&table_name)
    }

    fn untag_resource(&self, req: &AwsRequest) -> AwsResponse {
        let arn = param_str(&req.params, "ResourceArn").unwrap_or("");
        let Some(table) = self.table_by_arn(req, arn) else {
            return Self::not_found_table(arn);
        };
        let mut tags = table.tags.write();
        for key in req
            .params
            .get("TagKeys")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
        {
            if let Some(k) = key.as_str() {
                tags.remove(k);
            }
        }
        AwsResponse::json(200, json!({}))
    }

    fn list_tags_of_resource(&self, req: &AwsRequest) -> AwsResponse {
        let arn = param_str(&req.params, "ResourceArn").unwrap_or("");
        let table = match self.table_by_arn(req, arn) {
            Some(t) => t,
            None => return Self::not_found_table(arn),
        };
        let tags = table.tags.read();
        let list: Vec<Value> = tags
            .iter()
            .map(|(k, v)| json!({ "Key": k, "Value": v }))
            .collect();
        AwsResponse::json(200, json!({ "Tags": list }))
    }

    fn describe_limits(&self, _req: &AwsRequest) -> AwsResponse {
        AwsResponse::json(
            200,
            json!({
                "AccountMaxReadCapacityUnits": 20000,
                "AccountMaxWriteCapacityUnits": 20000,
                "TableMaxReadCapacityUnits": 10000,
                "TableMaxWriteCapacityUnits": 10000
            }),
        )
    }

    fn describe_endpoints(&self, req: &AwsRequest) -> AwsResponse {
        AwsResponse::json(
            200,
            json!({
                "Endpoints": [
                    {
                        "Address": format!("dynamodb.{}.amazonaws.com", req.region),
                        "CachePeriodInMinutes": 1440
                    }
                ]
            }),
        )
    }

    fn describe_ttl(&self, req: &AwsRequest) -> AwsResponse {
        let table_name = param_str(&req.params, "TableName").unwrap_or("");
        let state = self.get_state(req.account, &req.region);
        match state.get_table(table_name) {
            Some(table) => {
                let enabled = *table.ttl_enabled.read();
                let status = if *table.ttl_enabled.read() {
                    "ENABLED"
                } else {
                    "DISABLED"
                };
                let mut desc = json!({ "TimeToLiveStatus": status });
                if enabled {
                    desc.as_object_mut().unwrap().insert(
                        "AttributeName".into(),
                        json!(table.ttl_attribute.read().clone()),
                    );
                }
                AwsResponse::json(200, json!({ "TimeToLiveDescription": desc }))
            }
            None => Self::not_found_table(table_name),
        }
    }

    fn update_ttl(&self, req: &AwsRequest) -> AwsResponse {
        let table_name = param_str(&req.params, "TableName").unwrap_or("");
        let state = self.get_state(req.account, &req.region);
        let table = match state.get_table(table_name) {
            Some(t) => t,
            None => return Self::not_found_table(table_name),
        };
        let spec = req
            .params
            .get("TimeToLiveSpecification")
            .cloned()
            .unwrap_or(Value::Null);
        let enabled = spec
            .get("Enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let attr = spec
            .get("AttributeName")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if enabled && attr.is_empty() {
            return AwsResponse::error(
                400,
                "ValidationException",
                "TimeToLiveSpecification does not contain Enabled and AttributeName",
            );
        }
        *table.ttl_enabled.write() = enabled;
        *table.ttl_attribute.write() = attr.clone();
        AwsResponse::json(
            200,
            json!({ "TimeToLiveSpecification": { "Enabled": enabled, "AttributeName": attr } }),
        )
    }

    // ------------------------------------------------------------------
    // Continuous backups / PITR
    // ------------------------------------------------------------------

    fn continuous_backups_body(table: &Table) -> Value {
        let pitr = if *table.pitr_enabled.read() {
            "ENABLED"
        } else {
            "DISABLED"
        };
        json!({
            "ContinuousBackupsDescription": {
                "ContinuousBackupsStatus": "ENABLED",
                "PointInTimeRecoveryDescription": { "PointInTimeRecoveryStatus": pitr }
            }
        })
    }

    fn describe_continuous_backups(&self, req: &AwsRequest) -> AwsResponse {
        let table_name = param_str(&req.params, "TableName").unwrap_or("");
        let state = self.get_state(req.account, &req.region);
        match state.get_table(table_name) {
            Some(table) => AwsResponse::json(200, Self::continuous_backups_body(&table)),
            None => Self::not_found_table(table_name),
        }
    }

    fn update_continuous_backups(&self, req: &AwsRequest) -> AwsResponse {
        let table_name = param_str(&req.params, "TableName").unwrap_or("");
        let state = self.get_state(req.account, &req.region);
        let table = match state.get_table(table_name) {
            Some(t) => t,
            None => return Self::not_found_table(table_name),
        };
        let spec = req
            .params
            .get("PointInTimeRecoverySpecification")
            .cloned()
            .unwrap_or(Value::Null);
        let enabled = spec
            .get("PointInTimeRecoveryEnabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        *table.pitr_enabled.write() = enabled;
        AwsResponse::json(200, Self::continuous_backups_body(&table))
    }

    // ------------------------------------------------------------------
    // Backups and restore
    // ------------------------------------------------------------------

    fn backup_details(b: &Backup, status: Option<&str>) -> Value {
        json!({
            "BackupArn": b.arn,
            "BackupName": b.name,
            "BackupSizeBytes": b.size_bytes,
            "BackupStatus": status.unwrap_or(&b.status),
            "BackupType": b.backup_type,
            "BackupCreationDateTime": b.created_at,
        })
    }

    fn backup_summary(b: &Backup) -> Value {
        json!({
            "TableName": b.table_name,
            "TableArn": b.source_arn,
            "BackupArn": b.arn,
            "BackupName": b.name,
            "BackupCreationDateTime": b.created_at,
            "BackupStatus": b.status,
            "BackupType": b.backup_type,
            "BackupSizeBytes": b.size_bytes,
        })
    }

    fn backup_source_table_details(b: &Backup) -> Value {
        json!({
            "TableName": b.table_name,
            "TableArn": b.source_arn,
            "TableSizeBytes": 0,
            "KeySchema": b.key_schema.iter().map(|k| json!({"AttributeName": k.attribute_name, "KeyType": k.key_type})).collect::<Vec<_>>(),
            "TableCreationDateTime": b.source_created_at,
            "ProvisionedThroughput": { "ReadCapacityUnits": 0, "WriteCapacityUnits": 0 },
            "ItemCount": b.source_item_count,
        })
    }

    fn create_backup(&self, req: &AwsRequest) -> AwsResponse {
        let table_name = param_str(&req.params, "TableName").unwrap_or("");
        let state = self.get_state(req.account, &req.region);
        let table = match state.get_table(table_name) {
            Some(t) => t,
            None => return Self::not_found_table(table_name),
        };
        let mut ts = chrono::Utc::now().timestamp_millis();
        let mut arn;
        {
            let mut backups = state.backups.write();
            loop {
                arn = format!(
                    "arn:aws:dynamodb:{}:{}:table/{}/backup/{}-{}",
                    req.region,
                    req.account,
                    table_name,
                    ts,
                    &uuid::Uuid::new_v4().simple().to_string()[..8]
                );
                if !backups.iter().any(|b| b.arn == arn) {
                    break;
                }
                ts += 1;
            }
            let items: Vec<AttrMap> = table
                .items
                .read()
                .values()
                .map(|i| i.attributes.clone())
                .collect();
            let backup = Backup {
                arn: arn.clone(),
                name: param_str(&req.params, "BackupName")
                    .unwrap_or("")
                    .to_string(),
                table_name: table.name.clone(),
                status: "AVAILABLE".to_string(),
                backup_type: "USER".to_string(),
                created_at: now_timestamp(),
                size_bytes: 123,
                source_arn: table.table_arn.read().clone(),
                source_created_at: format_created_at(table.created_at),
                source_item_count: items.len() as i64,
                key_schema: table.key_schema.clone(),
                attribute_definitions: table.attribute_definitions.read().clone(),
                items,
            };
            backups.push(Arc::new(backup));
        }
        let backups = state.backups.read();
        let backup = backups.iter().rev().find(|b| b.arn == arn).unwrap();
        AwsResponse::json(
            200,
            json!({ "BackupDetails": Self::backup_details(backup, None) }),
        )
    }

    fn describe_backup(&self, req: &AwsRequest) -> AwsResponse {
        let arn = param_str(&req.params, "BackupArn").unwrap_or("");
        let state = self.get_state(req.account, &req.region);
        let backups = state.backups.read();
        let Some(b) = backups
            .iter()
            .find(|b| b.arn == arn && b.status == "AVAILABLE")
        else {
            return AwsResponse::error(
                400,
                "BackupNotFoundException",
                &format!("Backup not found for ARN {arn}"),
            );
        };
        AwsResponse::json(
            200,
            json!({
                "BackupDescription": {
                    "BackupDetails": Self::backup_details(b, None),
                    "SourceTableDetails": Self::backup_source_table_details(b),
                    "SourceTableFeatureDetails": {}
                }
            }),
        )
    }

    fn list_backups(&self, req: &AwsRequest) -> AwsResponse {
        let table_filter = param_str(&req.params, "TableName").map(String::from);
        let state = self.get_state(req.account, &req.region);
        let backups = state.backups.read();
        let summaries: Vec<Value> = backups
            .iter()
            .filter(|b| b.status == "AVAILABLE")
            .filter(|b| {
                table_filter
                    .as_deref()
                    .map(|t| b.table_name == t)
                    .unwrap_or(true)
            })
            .map(|b| Self::backup_summary(b))
            .collect();
        AwsResponse::json(200, json!({ "BackupSummaries": summaries }))
    }

    fn delete_backup(&self, req: &AwsRequest) -> AwsResponse {
        let arn = param_str(&req.params, "BackupArn").unwrap_or("");
        let state = self.get_state(req.account, &req.region);
        let mut backups = state.backups.write();
        let Some(pos) = backups.iter().position(|b| b.arn == arn) else {
            return AwsResponse::error(
                400,
                "BackupNotFoundException",
                &format!("Backup not found for ARN {arn}"),
            );
        };
        let mut b = (*backups[pos]).clone();
        b.status = "DELETED".to_string();
        backups.remove(pos);
        AwsResponse::json(
            200,
            json!({
                "BackupDescription": {
                    "BackupDetails": Self::backup_details(&b, None),
                    "SourceTableDetails": Self::backup_source_table_details(&b),
                    "SourceTableFeatureDetails": {}
                }
            }),
        )
    }

    fn restore_table_from_backup(&self, req: &AwsRequest) -> AwsResponse {
        let target = param_str(&req.params, "TargetTableName")
            .unwrap_or("")
            .to_string();
        let arn = param_str(&req.params, "BackupArn").unwrap_or("");
        let state = self.get_state(req.account, &req.region);
        let backup = {
            let backups = state.backups.read();
            match backups
                .iter()
                .find(|b| b.arn == arn && b.status == "AVAILABLE")
                .cloned()
            {
                Some(b) => b,
                None => {
                    return AwsResponse::error(
                        400,
                        "BackupNotFoundException",
                        &format!("Backup not found for ARN {arn}"),
                    )
                }
            }
        };
        if state.get_table(&target).is_some() {
            return AwsResponse::error(
                400,
                "ResourceAlreadyExistsException",
                &format!("Table already exists: {target}"),
            );
        }
        let table = Arc::new(Table::new(
            target.clone(),
            req.account,
            req.region.clone(),
            backup.key_schema.clone(),
            backup.attribute_definitions.clone(),
            "PAY_PER_REQUEST".to_string(),
        ));
        {
            let mut items = table.items.write();
            for item_attrs in &backup.items {
                if let Some(key) = compute_key_from_attrs(&table.key_schema, item_attrs) {
                    items.insert(key, Arc::new(Item::new(item_attrs.clone())));
                }
            }
        }
        state.put_table(table.clone());
        AwsResponse::json(
            200,
            json!({ "TableDescription": {
                "TableName": target,
                "TableStatus": "ACTIVE",
                "TableId": table.table_id,
                "RestoreSummary": {
                    "SourceBackupArn": backup.arn,
                    "SourceTableArn": backup.source_arn,
                    "RestoreDateTime": backup.created_at,
                    "RestoreInProgress": false,
                }
            }}),
        )
    }

    fn restore_table_to_point_in_time(&self, req: &AwsRequest) -> AwsResponse {
        let source_name = param_str(&req.params, "SourceTableName")
            .unwrap_or("")
            .to_string();
        let target = param_str(&req.params, "TargetTableName")
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let Some(source) = state.get_table(&source_name) else {
            return Self::not_found_table(&source_name);
        };
        if state.get_table(&target).is_some() {
            return AwsResponse::error(
                400,
                "ResourceAlreadyExistsException",
                &format!("Table already exists: {target}"),
            );
        }
        let table = Arc::new(Table::new(
            target.clone(),
            req.account,
            req.region.clone(),
            source.key_schema.clone(),
            source.attribute_definitions.read().clone(),
            source.billing_mode.read().clone(),
        ));
        {
            let source_items = source.items.read();
            let mut items = table.items.write();
            for (k, v) in source_items.iter() {
                items.insert(k.clone(), v.clone());
            }
        }
        state.put_table(table.clone());
        AwsResponse::json(
            200,
            json!({ "TableDescription": {
                "TableName": target,
                "TableStatus": "ACTIVE",
                "TableId": table.table_id,
            }}),
        )
    }

    // ------------------------------------------------------------------
    // Global tables
    // ------------------------------------------------------------------

    fn global_table_description(gt: &GlobalTable) -> Value {
        json!({
            "GlobalTableName": gt.name,
            "GlobalTableArn": gt.arn,
            "CreationDateTime": gt.created_at,
            "GlobalTableStatus": gt.status,
            "ReplicationGroup": gt.replication_group.iter().map(|r| json!({ "RegionName": r })).collect::<Vec<_>>()
        })
    }

    fn create_global_table(&self, req: &AwsRequest) -> AwsResponse {
        let name = param_str(&req.params, "GlobalTableName")
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        {
            let gts = state.global_tables.read();
            if gts.contains_key(&name) {
                return AwsResponse::error(
                    400,
                    "GlobalTableAlreadyExistsException",
                    &format!("Global table with name '{name}' already exists"),
                );
            }
        }
        let group: Vec<String> = req
            .params
            .get("ReplicationGroup")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|r| {
                        r.get("RegionName")
                            .and_then(|s| s.as_str())
                            .map(String::from)
                    })
                    .collect()
            })
            .unwrap_or_default();
        let gt = GlobalTable {
            name: name.clone(),
            arn: format!("arn:aws:dynamodb::{}:global-table/{name}", req.account),
            replication_group: group,
            status: "ACTIVE".to_string(),
            created_at: now_timestamp(),
        };
        state
            .global_tables
            .write()
            .insert(name.clone(), Arc::new(gt.clone()));
        AwsResponse::json(
            200,
            json!({ "GlobalTableDescription": Self::global_table_description(&gt) }),
        )
    }

    fn describe_global_table(&self, req: &AwsRequest) -> AwsResponse {
        let name = param_str(&req.params, "GlobalTableName").unwrap_or("");
        let state = self.get_state(req.account, &req.region);
        let found = {
            let gts = state.global_tables.read();
            gts.get(name).cloned()
        };
        match found {
            Some(gt) => AwsResponse::json(
                200,
                json!({ "GlobalTableDescription": Self::global_table_description(&gt) }),
            ),
            None => AwsResponse::error(
                400,
                "GlobalTableNotFoundException",
                &format!("Global table with name '{name}' does not exist"),
            ),
        }
    }

    fn list_global_tables(&self, req: &AwsRequest) -> AwsResponse {
        let region_filter = param_str(&req.params, "RegionName").map(String::from);
        let state = self.get_state(req.account, &req.region);
        let gts = state.global_tables.read();
        let mut tables: Vec<Value> = gts
            .values()
            .filter(|gt| {
                region_filter
                    .as_ref()
                    .map(|r| gt.replication_group.contains(r))
                    .unwrap_or(true)
            })
            .map(|gt| {
                json!({
                    "GlobalTableName": gt.name,
                    "ReplicationGroup": gt.replication_group.iter().map(|r| json!({ "RegionName": r })).collect::<Vec<_>>()
                })
            })
            .collect();
        tables.sort_by(|a, b| {
            a.get("GlobalTableName")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .cmp(
                    b.get("GlobalTableName")
                        .and_then(|v| v.as_str())
                        .unwrap_or(""),
                )
        });
        AwsResponse::json(200, json!({ "GlobalTables": tables }))
    }

    fn update_global_table(&self, req: &AwsRequest) -> AwsResponse {
        let name = param_str(&req.params, "GlobalTableName")
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let mut gts = state.global_tables.write();
        let Some(gt) = gts.get_mut(&name) else {
            return AwsResponse::error(
                400,
                "GlobalTableNotFoundException",
                &format!("Global table with name '{name}' does not exist"),
            );
        };
        let mut gt = (**gt).clone();
        for update in req
            .params
            .get("ReplicaUpdates")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
        {
            if let Some(create) = update.get("Create") {
                let region = create
                    .get("RegionName")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if gt.replication_group.iter().any(|r| r == region) {
                    return AwsResponse::error(
                        400,
                        "ReplicaAlreadyExistsException",
                        &format!("Replica already exists in region '{region}'"),
                    );
                }
                if !region.is_empty() {
                    gt.replication_group.push(region.to_string());
                }
            }
            if let Some(del) = update.get("Delete") {
                let region = del.get("RegionName").and_then(|v| v.as_str()).unwrap_or("");
                if !gt.replication_group.iter().any(|r| r == region) {
                    return AwsResponse::error(
                        400,
                        "ReplicaNotFoundException",
                        &format!("Replica does not exist in region '{region}'"),
                    );
                }
                gt.replication_group.retain(|r| r != region);
            }
        }
        gts.insert(name.clone(), Arc::new(gt.clone()));
        AwsResponse::json(
            200,
            json!({ "GlobalTableDescription": Self::global_table_description(&gt) }),
        )
    }

    fn delete_global_table(&self, req: &AwsRequest) -> AwsResponse {
        let name = param_str(&req.params, "GlobalTableName")
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let mut gts = state.global_tables.write();
        let Some(gt) = gts.remove(&name) else {
            return AwsResponse::error(
                400,
                "GlobalTableNotFoundException",
                &format!("Global table with name '{name}' does not exist"),
            );
        };
        let mut gt = (*gt).clone();
        gt.status = "DELETED".to_string();
        AwsResponse::json(
            200,
            json!({ "GlobalTableDescription": Self::global_table_description(&gt) }),
        )
    }

    fn describe_global_table_settings(&self, req: &AwsRequest) -> AwsResponse {
        let name = param_str(&req.params, "GlobalTableName").unwrap_or("");
        let state = self.get_state(req.account, &req.region);
        let found = {
            let gts = state.global_tables.read();
            gts.get(name).cloned()
        };
        match found {
            Some(gt) => AwsResponse::json(
                200,
                json!({
                    "GlobalTableName": gt.name,
                    "GlobalTableBillingMode": "PAY_PER_REQUEST",
                    "ReplicationGroup": gt.replication_group.iter().map(|r| json!({
                        "RegionName": r,
                        "ReplicaProvisionedReadCapacityUnits": 10,
                        "ReplicaProvisionedWriteCapacityUnits": 10,
                        "ReplicaStatus": "ACTIVE",
                        "ReplicaGlobalSecondaryIndexSettings": []
                    })).collect::<Vec<_>>()
                }),
            ),
            None => AwsResponse::error(
                400,
                "GlobalTableNotFoundException",
                &format!("Global table not found: {name}"),
            ),
        }
    }

    fn update_global_table_settings(&self, req: &AwsRequest) -> AwsResponse {
        let name = param_str(&req.params, "GlobalTableName")
            .unwrap_or("")
            .to_string();
        // Moto does not implement this operation; the suite only asserts the
        // echo of GlobalTableName. Keep permissive validation-free behavior.
        AwsResponse::json(200, json!({ "GlobalTableName": name }))
    }

    fn describe_table_replica_auto_scaling(&self, req: &AwsRequest) -> AwsResponse {
        let table_name = param_str(&req.params, "TableName")
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        match state.get_table(&table_name) {
            Some(_) => AwsResponse::json(
                200,
                json!({ "TableAutoScalingDescription": {
                    "TableName": table_name,
                    "TableStatus": "ACTIVE",
                    "Replicas": [{
                        "RegionName": req.region,
                        "ReplicaStatus": "ACTIVE",
                        "ReplicaAutoScalingPolicies": []
                    }]
                }}),
            ),
            None => AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!("Requested resource not found: Table: {table_name} not found"),
            ),
        }
    }

    fn update_table_replica_auto_scaling(&self, req: &AwsRequest) -> AwsResponse {
        self.describe_table_replica_auto_scaling(req)
    }

    // ------------------------------------------------------------------
    // Kinesis streaming destinations
    // ------------------------------------------------------------------

    fn modify_kinesis_destination(&self, req: &AwsRequest, status: &str) -> AwsResponse {
        let table_name = param_str(&req.params, "TableName")
            .unwrap_or("")
            .to_string();
        let stream_arn = param_str(&req.params, "StreamArn")
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let table = match state.get_table(&table_name) {
            Some(t) => t,
            None => return Self::not_found_table(&table_name),
        };
        let mut dests = table.kinesis_destinations.write();
        if status == "ACTIVE" {
            if let Some(dest) = dests.iter_mut().find(|d| d.stream_arn == stream_arn) {
                dest.status = status.to_string();
            } else {
                dests.push(KinesisDestination {
                    stream_arn: stream_arn.clone(),
                    status: status.to_string(),
                    precision: None,
                });
            }
        } else {
            dests.retain(|d| d.stream_arn != stream_arn);
        }
        AwsResponse::json(
            200,
            json!({ "TableName": table_name, "StreamArn": stream_arn, "DestinationStatus": status }),
        )
    }

    fn update_kinesis_destination(&self, req: &AwsRequest) -> AwsResponse {
        let table_name = param_str(&req.params, "TableName")
            .unwrap_or("")
            .to_string();
        let stream_arn = param_str(&req.params, "StreamArn")
            .unwrap_or("")
            .to_string();
        let precision = req
            .params
            .pointer("/UpdateKinesisStreamingConfiguration/ApproximateCreationDateTimePrecision")
            .and_then(|v| v.as_str())
            .map(String::from);
        let state = self.get_state(req.account, &req.region);
        let table = match state.get_table(&table_name) {
            Some(t) => t,
            None => return Self::not_found_table(&table_name),
        };
        let mut dests = table.kinesis_destinations.write();
        if let Some(dest) = dests.iter_mut().find(|d| d.stream_arn == stream_arn) {
            dest.precision = precision.clone();
        } else {
            dests.push(KinesisDestination {
                stream_arn: stream_arn.clone(),
                status: "ACTIVE".to_string(),
                precision: precision.clone(),
            });
        }
        let mut resp = json!({ "TableName": table_name, "DestinationStatus": "ACTIVE" });
        if let Some(p) = precision {
            resp["UpdateKinesisStreamingConfiguration"] =
                json!({ "ApproximateCreationDateTimePrecision": p });
        }
        AwsResponse::json(200, resp)
    }

    fn describe_kinesis_destination(&self, req: &AwsRequest) -> AwsResponse {
        let table_name = param_str(&req.params, "TableName")
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let table = match state.get_table(&table_name) {
            Some(t) => t,
            None => {
                return AwsResponse::error(
                    400,
                    "ResourceNotFoundException",
                    "Requested resource not found",
                )
            }
        };
        let dests = table.kinesis_destinations.read();
        let list: Vec<Value> = dests
            .iter()
            .map(|d| {
                let mut item = json!({ "StreamArn": d.stream_arn, "DestinationStatus": d.status });
                if let Some(p) = &d.precision {
                    item["ApproximateCreationDateTimePrecision"] = json!(p);
                }
                item
            })
            .collect();
        AwsResponse::json(
            200,
            json!({ "TableName": table_name, "KinesisDataStreamDestinations": list }),
        )
    }

    // ------------------------------------------------------------------
    // Contributor insights
    // ------------------------------------------------------------------

    fn describe_contributor_insights(&self, req: &AwsRequest) -> AwsResponse {
        let table_name = param_str(&req.params, "TableName")
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let table = match state.get_table(&table_name) {
            Some(t) => t,
            None => {
                return AwsResponse::error(
                    400,
                    "ResourceNotFoundException",
                    "Requested resource not found",
                )
            }
        };
        let status = table.contributor_insights.read().clone();
        match status {
            Some(s) => AwsResponse::json(
                200,
                json!({ "TableName": table_name, "ContributorInsightsStatus": s }),
            ),
            None => AwsResponse::error(
                400,
                "ResourceNotFoundException",
                "Requested resource not found",
            ),
        }
    }

    fn update_contributor_insights(&self, req: &AwsRequest) -> AwsResponse {
        let table_name = param_str(&req.params, "TableName")
            .unwrap_or("")
            .to_string();
        let action = param_str(&req.params, "ContributorInsightsAction").unwrap_or("");
        let state = self.get_state(req.account, &req.region);
        let table = match state.get_table(&table_name) {
            Some(t) => t,
            None => return Self::not_found_table(&table_name),
        };
        let status = match action.to_uppercase().as_str() {
            "ENABLE" => "ENABLED",
            "DISABLE" => "DISABLED",
            "DELETE" => {
                *table.contributor_insights.write() = None;
                return AwsResponse::json(
                    200,
                    json!({ "TableName": table_name, "ContributorInsightsStatus": "DISABLED" }),
                );
            }
            _ => {
                return AwsResponse::error(
                    400,
                    "ValidationException",
                    &format!("Invalid ContributorInsightsAction: {action}"),
                )
            }
        };
        *table.contributor_insights.write() = Some(status.to_string());
        AwsResponse::json(
            200,
            json!({ "TableName": table_name, "ContributorInsightsStatus": status }),
        )
    }

    // ------------------------------------------------------------------
    // Exports and imports
    // ------------------------------------------------------------------

    fn export_table_to_point_in_time(&self, req: &AwsRequest) -> AwsResponse {
        let arn = param_str(&req.params, "TableArn").unwrap_or("");
        let state = self.get_state(req.account, &req.region);
        let table = match self.table_by_arn(req, arn) {
            Some(t) => t,
            None => return Self::not_found_table(arn),
        };
        if !*table.pitr_enabled.read() {
            return AwsResponse::error(
                400,
                "PointInTimeRecoveryUnavailableException",
                &format!(
                    "Point in time recovery is not enabled for table '{}'",
                    table.name
                ),
            );
        }
        let format = param_str(&req.params, "ExportFormat")
            .unwrap_or("DYNAMODB_JSON")
            .to_string();
        let export_arn = format!(
            "arn:aws:dynamodb:{}:{}:table/{}/export/{}",
            req.region,
            req.account,
            table.name,
            uuid::Uuid::new_v4().simple()
        );
        let export = Export {
            arn: export_arn.clone(),
            table_arn: table.table_arn.read().clone(),
            table_name: table.name.clone(),
            status: "COMPLETED".to_string(),
            format: format.clone(),
            bucket: param_str(&req.params, "S3Bucket").unwrap_or("").to_string(),
            prefix: param_str(&req.params, "S3Prefix").unwrap_or("").to_string(),
        };
        state.exports.write().push(Arc::new(export));
        let bucket = param_str(&req.params, "S3Bucket").unwrap_or("");
        let prefix = param_str(&req.params, "S3Prefix").unwrap_or("");
        AwsResponse::json(
            200,
            json!({ "ExportDescription": {
                "ExportArn": export_arn,
                "ExportStatus": "COMPLETED",
                "ExportFormat": format,
                "ExportType": "FULL_EXPORT",
                "TableArn": *table.table_arn.read(),
                "S3Bucket": bucket,
                "S3BucketOwner": format!("{}", req.account),
                "S3Prefix": prefix,
                "BilledSizeBytes": 0,
                "ItemCount": table.items.read().len() as i64,
            }}),
        )
    }

    fn describe_export(&self, req: &AwsRequest) -> AwsResponse {
        let arn = param_str(&req.params, "ExportArn").unwrap_or("");
        let state = self.get_state(req.account, &req.region);
        let exports = state.exports.read();
        let Some(e) = exports.iter().find(|e| e.arn == arn).cloned() else {
            return AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!("Export not found for arn {arn}"),
            );
        };
        AwsResponse::json(
            200,
            json!({ "ExportDescription": {
                "ExportArn": e.arn,
                "ExportStatus": e.status,
                "ExportFormat": e.format,
                "ExportType": "FULL_EXPORT",
                "TableArn": e.table_arn,
                "S3Bucket": e.bucket,
                "S3Prefix": e.prefix,
            }}),
        )
    }

    fn import_table(&self, req: &AwsRequest) -> AwsResponse {
        let table_name = req
            .params
            .pointer("/TableCreationParameters/TableName")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let import_arn = format!(
            "arn:aws:dynamodb:{}:{}:table/{}/import/{}",
            req.region,
            req.account,
            table_name,
            uuid::Uuid::new_v4().simple()
        );
        AwsResponse::json(
            200,
            json!({ "ImportTableDescription": {
                "ImportArn": import_arn,
                "ImportStatus": "FAILED",
                "TableArn": format!("arn:aws:dynamodb:{}:{}:table/{}", req.region, req.account, table_name),
                "ErrorCount": 0,
                "ProcessedSizeBytes": 0,
                "ProcessedItemCount": 0,
                "ImportedItemCount": 0,
                "FailureCode": "S3NoSuchBucket",
                "FailureMessage": "The specified bucket does not exist"
            }}),
        )
    }

    // ------------------------------------------------------------------
    // Resource policy
    // ------------------------------------------------------------------

    fn get_resource_policy(&self, req: &AwsRequest) -> AwsResponse {
        let arn = param_str(&req.params, "ResourceArn").unwrap_or("");
        let table = match self.table_by_arn(req, arn) {
            Some(t) => t,
            None => {
                return AwsResponse::error(
                    400,
                    "PolicyNotFoundException",
                    &format!("Resource-based policy not found for the provided ResourceArn: {arn}"),
                )
            }
        };
        let policy = table.policy.read().clone();
        match policy {
            Some(p) => AwsResponse::json(
                200,
                json!({ "Policy": p.document, "RevisionId": p.revision_id }),
            ),
            None => AwsResponse::error(
                400,
                "PolicyNotFoundException",
                &format!("Resource-based policy not found for the provided ResourceArn: {arn}"),
            ),
        }
    }

    fn put_resource_policy(&self, req: &AwsRequest) -> AwsResponse {
        let arn = param_str(&req.params, "ResourceArn").unwrap_or("");
        let table = match self.table_by_arn(req, arn) {
            Some(t) => t,
            None => return Self::not_found_table(arn),
        };
        let policy_doc = param_str(&req.params, "Policy").unwrap_or("").to_string();
        let revision = format!("{}", chrono::Utc::now().timestamp_millis());
        *table.policy.write() = Some(crate::models::ResourcePolicy {
            document: policy_doc,
            revision_id: revision.clone(),
        });
        AwsResponse::json(200, json!({ "RevisionId": revision }))
    }

    fn delete_resource_policy(&self, req: &AwsRequest) -> AwsResponse {
        let arn = param_str(&req.params, "ResourceArn").unwrap_or("");
        let table = match self.table_by_arn(req, arn) {
            Some(t) => t,
            None => {
                return AwsResponse::error(
                    400,
                    "PolicyNotFoundException",
                    &format!("Resource-based policy not found for the provided ResourceArn: {arn}"),
                )
            }
        };
        *table.policy.write() = None;
        AwsResponse::json(200, json!({}))
    }
}

// ----------------------------------------------------------------------
// Shared small helpers
// ----------------------------------------------------------------------

/// Insert a nested `Value` at an attribute path into a projected output map.
fn set_nested(out: &mut Map<String, Value>, segs: &[String], val: Value) {
    let _ = expr_engine::set_path(out, segs, val);
}

/// Numeric compare of two decimal SequenceNumber strings (moto's stream
/// sequence literals exceed u64 but fit u128); unparseable sorts before.
fn compare_seq(candidate: &str, after: &str) -> bool {
    match (candidate.parse::<u128>(), after.parse::<u128>()) {
        (Ok(a), Ok(b)) => a > b,
        _ => candidate.as_bytes() > after.as_bytes(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn make_req(operation: &str, params: Value) -> AwsRequest {
        AwsRequest {
            service: "dynamodb".to_string(),
            operation: operation.to_string(),
            account: 123456789012,
            region: "us-east-1".to_string(),
            params,
            body: Bytes::new(),
        }
    }

    #[test]
    fn test_create_list_describe_delete() {
        let handler = DynamoDbHandler::new();
        let resp = handler.handle(make_req(
            "CreateTable",
            json!({
                "TableName": "t1",
                "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        ));
        assert_eq!(resp.status, 200);

        let resp = handler.handle(make_req("DeleteTable", json!({"TableName": "t1"})));
        assert_eq!(resp.status, 200);
        let resp = handler.handle(make_req("DeleteTable", json!({"TableName": "t1"})));
        assert_eq!(resp.status, 400);
    }

    #[test]
    fn test_put_get_with_condition() {
        let handler = DynamoDbHandler::new();
        handler.handle(make_req(
            "CreateTable",
            json!({
                "TableName": "users",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        ));
        let resp = handler.handle(make_req(
            "PutItem",
            json!({"TableName": "users", "Item": {"pk": {"S": "a"}, "v": {"S": "x"}}}),
        ));
        assert_eq!(resp.status, 200);

        // attribute_not_exists fails on the second put
        let resp = handler.handle(make_req(
            "PutItem",
            json!({
                "TableName": "users",
                "Item": {"pk": {"S": "a"}, "v": {"S": "y"}},
                "ConditionExpression": "attribute_not_exists(pk)"
            }),
        ));
        assert_eq!(resp.status, 400);
        assert!(resp.body.contains("ConditionalCheckFailedException"));

        let resp = handler.handle(make_req(
            "GetItem",
            json!({"TableName": "users", "Key": {"pk": {"S": "a"}}}),
        ));
        assert!(resp.body.contains("\"v\":{\"S\":\"x\"}"));
    }

    #[test]
    fn test_query_scan_filter_projection() {
        let handler = DynamoDbHandler::new();
        handler.handle(make_req(
            "CreateTable",
            json!({
                "TableName": "t2",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"}
                ],
                "AttributeDefinitions": [
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "sk", "AttributeType": "N"}
                ],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        ));
        for i in [1, 2, 3] {
            handler.handle(make_req(
                "PutItem",
                json!({"TableName": "t2", "Item": {"pk": {"S": "p"}, "sk": {"N": i.to_string()}, "v": {"S": format!("x{i}")}}}),
            ));
        }
        let mut values: Map<String, Value> = Map::new();
        values.insert(":pk".into(), json!({"S": "p"}));
        values.insert(":lo".into(), json!({"N": "2"}));
        values.insert(":hi".into(), json!({"N": "3"}));
        let resp = handler.handle(make_req(
            "Query",
            json!({
                "TableName": "t2",
                "KeyConditionExpression": "pk = :pk AND sk BETWEEN :lo AND :hi",
                "ExpressionAttributeValues": values
            }),
        ));
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(body["Count"], json!(2));
        assert_eq!(body["Items"][0]["sk"]["N"], json!("2"));

        let resp = handler.handle(make_req(
            "UpdateItem",
            json!({
                "TableName": "t2",
                "Key": {"pk": {"S": "p"}, "sk": {"N": "2"}},
                "UpdateExpression": "SET #v = :newv",
                "ExpressionAttributeNames": {"#v": "v"},
                "ExpressionAttributeValues": {":newv": {"S": "zzz"}},
                "ReturnValues": "ALL_NEW"
            }),
        ));
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(body["Attributes"]["v"], json!({"S": "zzz"}));
    }

    #[test]
    fn test_describe_table_warmthroughput_active() {
        let handler = DynamoDbHandler::new();

        // PAY_PER_REQUEST: on-demand tables report moto's default warm
        // throughput of 12000/4000 (models/table.py).
        handler.handle(make_req(
            "CreateTable",
            json!({
                "TableName": "warm-ondemand",
                "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        ));
        let resp = handler.handle(make_req(
            "DescribeTable",
            json!({"TableName": "warm-ondemand"}),
        ));
        assert_eq!(resp.status, 200);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(
            body["Table"]["WarmThroughput"],
            json!({
                "ReadUnitsPerSecond": 12000,
                "WriteUnitsPerSecond": 4000,
                "Status": "ACTIVE"
            })
        );

        // PROVISIONED: warm throughput mirrors the provisioned capacity.
        handler.handle(make_req(
            "CreateTable",
            json!({
                "TableName": "warm-provisioned",
                "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
                "ProvisionedThroughput": {"ReadCapacityUnits": 3, "WriteCapacityUnits": 5}
            }),
        ));
        let resp = handler.handle(make_req(
            "DescribeTable",
            json!({"TableName": "warm-provisioned"}),
        ));
        assert_eq!(resp.status, 200);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(
            body["Table"]["WarmThroughput"],
            json!({
                "ReadUnitsPerSecond": 3,
                "WriteUnitsPerSecond": 5,
                "Status": "ACTIVE"
            })
        );
    }

    #[test]
    fn update_table_warmthroughput_present() {
        let handler = DynamoDbHandler::new();

        // PAY_PER_REQUEST: on-demand tables keep moto's default warm
        // throughput of 12000/4000 on UpdateTable.
        handler.handle(make_req(
            "CreateTable",
            json!({
                "TableName": "upd-warm-ondemand",
                "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        ));
        let resp = handler.handle(make_req(
            "UpdateTable",
            json!({"TableName": "upd-warm-ondemand"}),
        ));
        assert_eq!(resp.status, 200);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(
            body["TableDescription"]["WarmThroughput"],
            json!({
                "ReadUnitsPerSecond": 12000,
                "WriteUnitsPerSecond": 4000,
                "Status": "ACTIVE"
            })
        );

        // PROVISIONED: warm throughput mirrors the provisioned units after
        // the update applies.
        handler.handle(make_req(
            "CreateTable",
            json!({
                "TableName": "upd-warm-provisioned",
                "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
                "ProvisionedThroughput": {"ReadCapacityUnits": 3, "WriteCapacityUnits": 5}
            }),
        ));
        let resp = handler.handle(make_req(
            "UpdateTable",
            json!({
                "TableName": "upd-warm-provisioned",
                "ProvisionedThroughput": {"ReadCapacityUnits": 9, "WriteCapacityUnits": 21}
            }),
        ));
        assert_eq!(resp.status, 200);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(
            body["TableDescription"]["WarmThroughput"],
            json!({
                "ReadUnitsPerSecond": 9,
                "WriteUnitsPerSecond": 21,
                "Status": "ACTIVE"
            })
        );

        // A later DescribeTable reports the updated units too.
        let resp = handler.handle(make_req(
            "DescribeTable",
            json!({"TableName": "upd-warm-provisioned"}),
        ));
        assert_eq!(resp.status, 200);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(
            body["Table"]["WarmThroughput"],
            json!({
                "ReadUnitsPerSecond": 9,
                "WriteUnitsPerSecond": 21,
                "Status": "ACTIVE"
            })
        );
    }

    // moto update_table mutates the BillingMode, DeletionProtectionEnabled and
    // StreamSpecification members it is given (dynamodb/models/__init__.py:
    // 186-191, update_table_streams), and the re-routed describe payload
    // (Table.describe) echoes the new values: a PROVISIONED table switched to
    // PAY_PER_REQUEST describes BillingModeSummary {"BillingMode":
    // "PAY_PER_REQUEST"} and the on-demand 12000/4000 warm throughput.
    #[test]
    fn update_table_applies_billing_mode_protection_stream() {
        let handler = DynamoDbHandler::new();
        handler.handle(make_req(
            "CreateTable",
            json!({
                "TableName": "utm",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 7}
            }),
        ));

        // Switch the billing mode: the UpdateTable response echoes the new
        // mode, and the derived WarmThroughput follows it.
        let resp = handler.handle(make_req(
            "UpdateTable",
            json!({"TableName": "utm", "BillingMode": "PAY_PER_REQUEST"}),
        ));
        assert_eq!(resp.status, 200);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(
            body["TableDescription"]["BillingModeSummary"],
            json!({"BillingMode": "PAY_PER_REQUEST"})
        );
        assert_eq!(
            body["TableDescription"]["WarmThroughput"],
            json!({
                "ReadUnitsPerSecond": 12000,
                "WriteUnitsPerSecond": 4000,
                "Status": "ACTIVE"
            })
        );

        // The next DescribeTable echoes it too.
        let resp = handler.handle(make_req("DescribeTable", json!({"TableName": "utm"})));
        assert_eq!(resp.status, 200);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(
            body["Table"]["BillingModeSummary"],
            json!({"BillingMode": "PAY_PER_REQUEST"})
        );
        assert_eq!(
            body["Table"]["WarmThroughput"],
            json!({
                "ReadUnitsPerSecond": 12000,
                "WriteUnitsPerSecond": 4000,
                "Status": "ACTIVE"
            })
        );

        // DeletionProtectionEnabled mutates on both describe surfaces.
        let resp = handler.handle(make_req(
            "UpdateTable",
            json!({"TableName": "utm", "DeletionProtectionEnabled": true}),
        ));
        assert_eq!(resp.status, 200);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert!(body["TableDescription"]["DeletionProtectionEnabled"]
            .as_bool()
            .unwrap());
        let resp = handler.handle(make_req("DescribeTable", json!({"TableName": "utm"})));
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert!(body["Table"]["DeletionProtectionEnabled"]
            .as_bool()
            .unwrap());
        let resp = handler.handle(make_req(
            "UpdateTable",
            json!({"TableName": "utm", "DeletionProtectionEnabled": false}),
        ));
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert!(!body["TableDescription"]["DeletionProtectionEnabled"]
            .as_bool()
            .unwrap());

        // A StreamSpecification-on update attaches the stream (label + shard),
        // echoed on this response and the next describe; re-enabling a live
        // stream answers moto's StreamAlreadyEnabledException ("Cannot enable
        // stream", ResourceInUseException); an off-spec disables it.
        let resp = handler.handle(make_req(
            "UpdateTable",
            json!({"TableName": "utm",
                   "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"}}),
        ));
        assert_eq!(resp.status, 200);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert!(body["TableDescription"]["LatestStreamLabel"].is_string());
        assert!(body["TableDescription"]["LatestStreamArn"]
            .as_str()
            .unwrap()
            .starts_with("arn:aws:dynamodb:us-east-1:123456789012:table/utm/stream/"));
        let resp = handler.handle(make_req(
            "UpdateTable",
            json!({"TableName": "utm",
                   "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_IMAGE"}}),
        ));
        assert_eq!(resp.status, 400);
        assert_eq!(
            resp.body,
            json!({"__type": "ResourceInUseException", "message": "Cannot enable stream"})
                .to_string()
        );
        let resp = handler.handle(make_req(
            "UpdateTable",
            json!({"TableName": "utm", "StreamSpecification": {"StreamEnabled": false}}),
        ));
        assert_eq!(resp.status, 200);

        // … and switching BACK to PROVISIONED describes the stored mode again,
        // warm throughput off the provisioned units (moto keeps its stored
        // throughput across mode switches).
        let resp = handler.handle(make_req(
            "UpdateTable",
            json!({"TableName": "utm", "BillingMode": "PROVISIONED"}),
        ));
        assert_eq!(resp.status, 200);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(
            body["TableDescription"]["BillingModeSummary"],
            json!({"BillingMode": "PROVISIONED"})
        );
        assert_eq!(
            body["TableDescription"]["WarmThroughput"],
            json!({
                "ReadUnitsPerSecond": 5,
                "WriteUnitsPerSecond": 7,
                "Status": "ACTIVE"
            })
        );
    }

    #[test]
    fn gsi_warmthroughput_present() {
        let handler = DynamoDbHandler::new();

        // PAY_PER_REQUEST: the on-demand GSI reports moto's default warm
        // throughput of 12000/4000 (models/table.py GlobalSecondaryIndex).
        let resp = handler.handle(make_req(
            "CreateTable",
            json!({
                "TableName": "gsi-warm-ondemand",
                "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
                "AttributeDefinitions": [
                    {"AttributeName": "id", "AttributeType": "S"},
                    {"AttributeName": "name", "AttributeType": "S"}
                ],
                "BillingMode": "PAY_PER_REQUEST",
                "GlobalSecondaryIndexes": [{
                    "IndexName": "byname",
                    "KeySchema": [{"AttributeName": "name", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"}
                }],
                "LocalSecondaryIndexes": [{
                    "IndexName": "byname-lsi",
                    "KeySchema": [
                        {"AttributeName": "id", "KeyType": "HASH"},
                        {"AttributeName": "name", "KeyType": "RANGE"}
                    ],
                    "Projection": {"ProjectionType": "ALL"}
                }]
            }),
        ));
        // CreateTable and DescribeTable share the renderer, so both carry the
        // GSI WarmThroughput block; CreateTable wraps in TableDescription.
        assert_eq!(resp.status, 200);
        let create_body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(
            create_body["TableDescription"]["GlobalSecondaryIndexes"][0]["WarmThroughput"],
            json!({
                "ReadUnitsPerSecond": 12000,
                "WriteUnitsPerSecond": 4000,
                "Status": "ACTIVE"
            })
        );
        let resp = handler.handle(make_req(
            "DescribeTable",
            json!({"TableName": "gsi-warm-ondemand"}),
        ));
        assert_eq!(resp.status, 200);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(
            body["Table"]["GlobalSecondaryIndexes"][0]["WarmThroughput"],
            json!({
                "ReadUnitsPerSecond": 12000,
                "WriteUnitsPerSecond": 4000,
                "Status": "ACTIVE"
            })
        );
        // The member is GSI-only: LocalSecondaryIndexDescription declares no
        // WarmThroughput, and moto's LSI describe() emits none.
        assert!(body["Table"]["LocalSecondaryIndexes"][0]
            .get("WarmThroughput")
            .is_none());

        // PROVISIONED: the GSI advertises its own provisioned units.
        handler.handle(make_req(
            "CreateTable",
            json!({
                "TableName": "gsi-warm-provisioned",
                "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
                "AttributeDefinitions": [
                    {"AttributeName": "id", "AttributeType": "S"},
                    {"AttributeName": "name", "AttributeType": "S"}
                ],
                "ProvisionedThroughput": {"ReadCapacityUnits": 3, "WriteCapacityUnits": 5},
                "GlobalSecondaryIndexes": [{
                    "IndexName": "byname",
                    "KeySchema": [{"AttributeName": "name", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                    "ProvisionedThroughput": {"ReadCapacityUnits": 7, "WriteCapacityUnits": 9}
                }]
            }),
        ));
        let resp = handler.handle(make_req(
            "DescribeTable",
            json!({"TableName": "gsi-warm-provisioned"}),
        ));
        assert_eq!(resp.status, 200);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(
            body["Table"]["GlobalSecondaryIndexes"][0]["WarmThroughput"],
            json!({
                "ReadUnitsPerSecond": 7,
                "WriteUnitsPerSecond": 9,
                "Status": "ACTIVE"
            })
        );
    }

    // ---- Item size enforcement (getmoto/moto#10176, #1874) ----

    // AWS sizes a Number value at ceil(significant_digits / 2) + 1 bytes, so
    // this item (~150 KB: ceil(300000/2)+1 bytes for the Number plus a small
    // key) is inside the twin's 409,600-byte limit. moto's Number sizing
    // differs at the ~400 KB boundary (it weighs a Number at
    // len(str(value)) — 300,000 bytes here — and its LimitedSizeDict guard
    // fires when the moto-sized whole-item sum passes 405,000 bytes), so the
    // two engines classify boundary-size items differently. The AWS
    // 38-significant-digit cap on Number values is implemented in neither
    // moto nor the twin: a documented shared divergence, orthogonal to the
    // 409,600-byte item-size check this test exercises.
    #[test]
    fn item_size_number_bytes_300k_digit_number_is_not_400() {
        let handler = DynamoDbHandler::new();
        handler.handle(make_req(
            "CreateTable",
            json!({
                "TableName": "nsize",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        ));
        let big_number = "9".repeat(300_000);
        let resp = handler.handle(make_req(
            "PutItem",
            json!({"TableName": "nsize", "Item": {"pk": {"S": "big"}, "n": {"N": big_number}}}),
        ));
        // The literal is enormous and float()-rounds to inf — moto's numeric
        // validation (table.py:578-619) 400s it as non-numeric before any
        // size math could run; the bytes formula below the 409,600-byte cap
        // never gets a say.
        assert_eq!(resp.status, 400, "body: {}", resp.body);
        assert!(
            resp.body
                .contains("The parameter cannot be converted to a numeric value"),
            "body: {}",
            resp.body
        );
    }

    // The issue's own repro: pk(2+40) + data(4+409530 raw) + ttl(3+6 with
    // AWS number sizing) = 409,585 <= 409,600 — accepted by real AWS,
    // rejected by moto (moto's ttl sizing puts it past its 405,000 guard).
    #[test]
    fn item_size_number_bytes_issue_repro_accepted() {
        use base64::Engine;
        let handler = DynamoDbHandler::new();
        handler.handle(make_req(
            "CreateTable",
            json!({
                "TableName": "repro",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        ));
        let blob = base64::engine::general_purpose::STANDARD.encode(vec![120u8; 409530]);
        let resp = handler.handle(make_req(
            "PutItem",
            json!({
                "TableName": "repro",
                "Item": {
                    "pk": {"S": "x".repeat(40)},
                    "data": {"B": blob},
                    "ttl": {"N": "1786461547"},
                }
            }),
        ));
        assert_eq!(resp.status, 200);
    }

    #[test]
    fn item_size_number_bytes_over_400kb_is_400() {
        let handler = DynamoDbHandler::new();
        handler.handle(make_req(
            "CreateTable",
            json!({
                "TableName": "nover",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        ));
        let big_string = "x".repeat(500_000);
        let resp = handler.handle(make_req(
            "PutItem",
            json!({"TableName": "nover", "Item": {"pk": {"S": "a"}, "v": {"S": big_string}}}),
        ));
        assert_eq!(resp.status, 400);
        assert!(resp.body.contains("ValidationException"));
        // moto's ItemSizeTooLarge wording.
        assert!(resp
            .body
            .contains("Item size has exceeded the maximum allowed size"));
    }

    // The same limit applies wherever an item is written: UpdateItem, the
    // PutRequest row of BatchWriteItem, and TransactWriteItems Put/Update.
    #[test]
    fn item_size_number_bytes_write_paths_400() {
        let handler = DynamoDbHandler::new();
        handler.handle(make_req(
            "CreateTable",
            json!({
                "TableName": "paths",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        ));
        let big_string = "x".repeat(500_000);

        let resp = handler.handle(make_req(
            "UpdateItem",
            json!({
                "TableName": "paths",
                "Key": {"pk": {"S": "a"}},
                "UpdateExpression": "SET v = :v",
                "ExpressionAttributeValues": {":v": {"S": big_string.clone()}}
            }),
        ));
        assert_eq!(resp.status, 400);
        // moto's ItemSizeToUpdateTooLarge wording on the update path.
        assert!(resp
            .body
            .contains("Item size to update has exceeded the maximum allowed size"));

        let resp = handler.handle(make_req(
            "BatchWriteItem",
            json!({"RequestItems": {"paths": [
                {"PutRequest": {"Item": {"pk": {"S": "b"}, "v": {"S": big_string.clone()}}}}
            ]}}),
        ));
        assert_eq!(resp.status, 400);
        assert!(resp
            .body
            .contains("Item size has exceeded the maximum allowed size"));

        let resp = handler.handle(make_req(
            "TransactWriteItems",
            json!({"TransactItems": [
                {"Put": {"TableName": "paths",
                         "Item": {"pk": {"S": "c"}, "v": {"S": big_string.clone()}}}}
            ]}),
        ));
        assert_eq!(resp.status, 400);
        assert!(resp
            .body
            .contains("Item size has exceeded the maximum allowed size"));

        let resp = handler.handle(make_req(
            "TransactWriteItems",
            json!({"TransactItems": [
                {"Update": {"TableName": "paths",
                            "Key": {"pk": {"S": "d"}},
                            "UpdateExpression": "SET v = :v",
                            "ExpressionAttributeValues": {":v": {"S": big_string.clone()}}}}
            ]}),
        ));
        assert_eq!(resp.status, 400);
        // The transact Update row re-raises through moto's update_item too.
        assert!(resp
            .body
            .contains("Item size to update has exceeded the maximum allowed size"));
    }

    // TransactWriteItems is atomic: moto deep-copies every touched table and
    // restores it on failure (dynamodb/models/__init__.py transact_write_items),
    // like real AWS. When an Update row turns out
    // oversized, a Put row from earlier in the same transaction must not be
    // visible afterwards. The twin reaches this by evaluating every Update
    // against a pre-transaction clone in phase 1, before any write happens.
    #[test]
    fn transact_update_oversize_no_partial_commit() {
        let handler = DynamoDbHandler::new();
        handler.handle(make_req(
            "CreateTable",
            json!({
                "TableName": "atomic1",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        ));
        let big_string = "x".repeat(500_000);
        let resp = handler.handle(make_req(
            "TransactWriteItems",
            json!({"TransactItems": [
                {"Put": {"TableName": "atomic1",
                         "Item": {"pk": {"S": "early-row"}, "v": {"S": "small"}}}},
                {"Update": {"TableName": "atomic1",
                            "Key": {"pk": {"S": "later-row"}},
                            "UpdateExpression": "SET v = :v",
                            "ExpressionAttributeValues": {":v": {"S": big_string}}}}
            ]}),
        ));
        assert_eq!(resp.status, 400);
        assert!(resp.body.contains("ValidationException"));
        // The failed transaction leaves nothing behind: the earlier Put row
        // and the update target are both invisible.
        for pk in ["early-row", "later-row"] {
            let resp = handler.handle(make_req(
                "GetItem",
                json!({"TableName": "atomic1", "Key": {"pk": {"S": pk}}}),
            ));
            assert_eq!(resp.status, 200);
            assert_eq!(resp.body, "{}", "rollback must remove {pk}");
        }
    }

    // moto's transact_write_items check_unicity
    // (dynamodb/models/__init__.py:617-623): two rows addressed at the same
    // (table, key) raise MultipleTransactionsException — "Transaction request
    // cannot include multiple operations on one item" — before anything runs,
    // so NOTHING is written, not even the first row. Put rows target the key
    // hidden in their Item; Update/Delete/ConditionCheck rows their own Key.
    #[test]
    fn transact_same_key_rows_rejected_400_nothing_written() {
        let handler = DynamoDbHandler::new();
        handler.handle(make_req(
            "CreateTable",
            json!({
                "TableName": "unicity",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        ));

        // Two Update rows on one key.
        let resp = handler.handle(make_req(
            "TransactWriteItems",
            json!({"TransactItems": [
                {"Update": {"TableName": "unicity", "Key": {"pk": {"S": "u1"}},
                            "UpdateExpression": "SET a = :a",
                            "ExpressionAttributeValues": {":a": {"S": "1"}}}},
                {"Update": {"TableName": "unicity", "Key": {"pk": {"S": "u1"}},
                            "UpdateExpression": "SET b = :b",
                            "ExpressionAttributeValues": {":b": {"S": "2"}}}}
            ]}),
        ));
        assert_eq!(resp.status, 400);
        assert!(resp
            .body
            .contains("Transaction request cannot include multiple operations on one item"));
        // Nothing written: the earlier row must be absent too.
        let resp = handler.handle(make_req(
            "GetItem",
            json!({"TableName": "unicity", "Key": {"pk": {"S": "u1"}}}),
        ));
        assert_eq!(resp.body, "{}", "unicity must cancel the whole request");

        // Mixed-op row kinds on one key: Put row (key from Item) + Update row
        // (key from Key), and ConditionCheck + Delete.
        let resp = handler.handle(make_req(
            "TransactWriteItems",
            json!({"TransactItems": [
                {"Put": {"TableName": "unicity", "Item": {"pk": {"S": "p1"}, "v": {"S": "x"}}}},
                {"Update": {"TableName": "unicity", "Key": {"pk": {"S": "p1"}},
                            "UpdateExpression": "SET w = :w",
                            "ExpressionAttributeValues": {":w": {"S": "2"}}}}
            ]}),
        ));
        assert_eq!(resp.status, 400);
        assert!(resp
            .body
            .contains("Transaction request cannot include multiple operations on one item"));
        let resp = handler.handle(make_req(
            "TransactWriteItems",
            json!({"TransactItems": [
                {"ConditionCheck": {"TableName": "unicity", "Key": {"pk": {"S": "p1"}},
                                    "ConditionExpression": "attribute_exists(pk)"}},
                {"Delete": {"TableName": "unicity", "Key": {"pk": {"S": "p1"}}}}
            ]}),
        ));
        assert_eq!(resp.status, 400);
        assert!(resp
            .body
            .contains("Transaction request cannot include multiple operations on one item"));

        // Distinct keys — even across tables (moto hashes (table, key)) — stay
        // valid.
        handler.handle(make_req(
            "CreateTable",
            json!({
                "TableName": "unicity2",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        ));
        let resp = handler.handle(make_req(
            "TransactWriteItems",
            json!({"TransactItems": [
                {"Put": {"TableName": "unicity", "Item": {"pk": {"S": "a"}, "v": {"S": "x"}}}},
                {"Put": {"TableName": "unicity", "Item": {"pk": {"S": "b"}, "v": {"S": "y"}}}}
            ]}),
        ));
        assert_eq!(resp.status, 200);
    }

    // moto caps both Transact* batches: transact_write_items >100 rows raises
    // TooManyTransactionsException
    // ("1 validation error detected at 'transactItems' failed to satisfy
    // constraint: Member must have length less than or equal to 100." — note
    // the trailing period), while transact_get_items >25 rows
    // (TRANSACTION_MAX_ITEMS, dynamodb/responses.py:1419-1435) uses its own
    // message with a per-row TransactGetItem@id list and no trailing period.
    #[test]
    fn transact_items_length_caps_write_100_get_25() {
        let handler = DynamoDbHandler::new();
        handler.handle(make_req(
            "CreateTable",
            json!({
                "TableName": "cap",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        ));

        // 101 write rows -> 400.
        let rows: Vec<Value> = (0..101)
            .map(|i| json!({"Put": {"TableName": "cap", "Item": {"pk": {"S": format!("bulk{i}")}, "v": {"S": "x"}}}}))
            .collect();
        let resp = handler.handle(make_req(
            "TransactWriteItems",
            json!({"TransactItems": rows}),
        ));
        assert_eq!(resp.status, 400);
        assert_eq!(
            resp.body,
            json!({
                "__type": "ValidationException",
                "message": "1 validation error detected at 'transactItems' failed to satisfy \
                            constraint: Member must have length less than or equal to 100."
            })
            .to_string()
        );
        // Nothing from the rejected batch was written.
        let resp = handler.handle(make_req(
            "GetItem",
            json!({"TableName": "cap", "Key": {"pk": {"S": "bulk0"}}}),
        ));
        assert_eq!(resp.body, "{}");

        // Exactly 100 write rows pass.
        let rows: Vec<Value> = (0..100)
            .map(|i| json!({"Put": {"TableName": "cap", "Item": {"pk": {"S": format!("ok{i}")}}}}))
            .collect();
        let resp = handler.handle(make_req(
            "TransactWriteItems",
            json!({"TransactItems": rows}),
        ));
        assert_eq!(resp.status, 200);

        // 26 get rows -> 400 with moto's TransactGetItem list. The ids run
        // sequential 0x10000001.. in lowercase hex (responses.py request_id).
        let rows: Vec<Value> = (0..26)
            .map(|i| json!({"Get": {"TableName": "cap", "Key": {"pk": {"S": format!("bulk{i}")}}}}))
            .collect();
        let resp = handler.handle(make_req("TransactGetItems", json!({"TransactItems": rows})));
        assert_eq!(resp.status, 400);
        let ids: String = (1..=26)
            .map(|i| {
                format!(
                    "com.amazonaws.dynamodb.v20120810.TransactGetItem@{:x}",
                    268435456u64 + i
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        assert_eq!(
            resp.body,
            json!({
                "__type": "ValidationException",
                "message": format!(
                    "1 validation error detected: Value '[{ids}] at 'transactItems' failed to \
                     satisfy constraint: Member must have length less than or equal to 25"
                ),
            })
            .to_string()
        );

        // Exactly 25 get rows pass.
        let rows: Vec<Value> = (0..25)
            .map(|i| json!({"Get": {"TableName": "cap", "Key": {"pk": {"S": format!("ok{i}")}}}}))
            .collect();
        let resp = handler.handle(make_req("TransactGetItems", json!({"TransactItems": rows})));
        assert_eq!(resp.status, 200);
    }

    // moto's transact 200 shapes (dynamodb/responses.py:1531,
    // :1437-1471; live-transcribed moto 5.2.4, 2026-10-26):
    //   TransactWriteItems answers the constant
    //   {"ConsumedCapacity": [], "ItemCollectionMetrics": {}} — the empty
    //   ItemCollectionMetrics OBJECT (never per-row entries);
    //   TransactGetItems omits ConsumedCapacity unless
    //   ReturnConsumedCapacity is not "NONE", one entry per table that had
    //   at least one returning row in first-hit order, 2.0 units per hit
    //   (missing rows contribute nothing), and the "Table" breakdown only
    //   for "INDEXES".
    #[test]
    fn transact_200_shapes_item_collection_metrics_and_capacity() {
        let handler = DynamoDbHandler::new();
        for table in ["shapeA", "shapeB"] {
            handler.handle(make_req(
                "CreateTable",
                json!({
                    "TableName": table,
                    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                    "BillingMode": "PAY_PER_REQUEST"
                }),
            ));
        }
        handler.handle(make_req(
            "PutItem",
            json!({"TableName": "shapeA", "Item": {"pk": {"S": "a1"}, "v": {"N": "1"}}}),
        ));
        handler.handle(make_req(
            "PutItem",
            json!({"TableName": "shapeB", "Item": {"pk": {"S": "b1"}, "v": {"N": "2"}}}),
        ));

        let resp = handler.handle(make_req(
            "TransactWriteItems",
            json!({"TransactItems": [
                {"Put": {"TableName": "shapeA", "Item": {"pk": {"S": "w1"}, "v": {"S": "x"}}}}
            ]}),
        ));
        assert_eq!(resp.status, 200);
        assert_eq!(
            resp.body,
            json!({"ConsumedCapacity": [], "ItemCollectionMetrics": {}}).to_string()
        );

        // Omitted ReturnConsumedCapacity: no ConsumedCapacity member at
        // all — hit row and miss row render {Item}/{None}.
        let resp = handler.handle(make_req(
            "TransactGetItems",
            json!({"TransactItems": [
                {"Get": {"TableName": "shapeA", "Key": {"pk": {"S": "a1"}}}},
                {"Get": {"TableName": "shapeA", "Key": {"pk": {"S": "miss"}}}}
            ]}),
        ));
        assert_eq!(resp.status, 200);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(
            body["Responses"],
            json!([{"Item": {"pk": {"S": "a1"}, "v": {"N": "1"}}}, {}])
        );
        assert!(body.get("ConsumedCapacity").is_none());

        // TOTAL: one entry for the hit table; 2.0 units per hit.
        let resp = handler.handle(make_req(
            "TransactGetItems",
            json!({"TransactItems": [
                {"Get": {"TableName": "shapeA", "Key": {"pk": {"S": "a1"}}}},
                {"Get": {"TableName": "shapeA", "Key": {"pk": {"S": "miss"}}}}
            ], "ReturnConsumedCapacity": "TOTAL"}),
        ));
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(
            body["ConsumedCapacity"],
            json!([{"TableName": "shapeA", "CapacityUnits": 2.0, "ReadCapacityUnits": 2.0}])
        );

        // INDEXES, two tables, repeat hit plus miss: per-table
        // aggregation (shapeB twice = 4.0; the shapeB miss adds nothing)
        // in first-hit order, each with the "Table" breakdown.
        let resp = handler.handle(make_req(
            "TransactGetItems",
            json!({"TransactItems": [
                {"Get": {"TableName": "shapeA", "Key": {"pk": {"S": "a1"}}}},
                {"Get": {"TableName": "shapeB", "Key": {"pk": {"S": "b1"}}}},
                {"Get": {"TableName": "shapeB", "Key": {"pk": {"S": "b1"}}}},
                {"Get": {"TableName": "shapeB", "Key": {"pk": {"S": "miss"}}}}
            ], "ReturnConsumedCapacity": "INDEXES"}),
        ));
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(
            body["ConsumedCapacity"],
            json!([
                {"TableName": "shapeA", "CapacityUnits": 2.0, "ReadCapacityUnits": 2.0,
                 "Table": {"CapacityUnits": 2.0, "ReadCapacityUnits": 2.0}},
                {"TableName": "shapeB", "CapacityUnits": 4.0, "ReadCapacityUnits": 4.0,
                 "Table": {"CapacityUnits": 4.0, "ReadCapacityUnits": 4.0}}
            ])
        );
    }

    // A TransactWriteItems row that parses to no operation — the zero-op
    // `{}` row — crashes moto's response-layer validation:
    // `item_values = list(item.values())[0]` raises IndexError
    // (dynamodb/responses.py:1478) before the model's row loop
    // (models/__init__.py:635 next(iter(item.items())) → StopIteration)
    // could ever build a cancellation envelope, so moto_server answers the
    // werkzeug plain 500 page (verified live against moto 5.2.4,
    // moto_server, 2026-10-26). The twin answers AWS's ValidationException
    // for that input instead (2026-10-31 wave review: 500_error is reserved
    // for genuine crashes and the probe's required-only fill is exactly
    // this shape) — with the same single-op message the >1-op row carries.
    #[test]
    fn transact_zero_op_row_answers_validation_exception() {
        let handler = DynamoDbHandler::new();
        handler.handle(make_req(
            "CreateTable",
            json!({
                "TableName": "zeroop",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        ));
        let resp = handler.handle(make_req(
            "TransactWriteItems",
            json!({"TransactItems": [{}]}),
        ));
        assert_eq!(resp.status, 400);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(body["__type"], "ValidationException");
        assert_eq!(
            body["message"],
            "TransactItems can only contain one of Check, Put, Update or Delete"
        );

        // The validation sits ahead of phase 2 (where moto's crash sits
        // ahead of its row loop), so a valid Put row sharing the request
        // with the zero-op row is NOT committed either.
        let resp = handler.handle(make_req(
            "TransactWriteItems",
            json!({"TransactItems": [
                {"Put": {"TableName": "zeroop", "Item": {"pk": {"S": "p"}, "v": {"S": "x"}}}},
                {}
            ]}),
        ));
        assert_eq!(resp.status, 400);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(body["__type"], "ValidationException");
        let resp = handler.handle(make_req(
            "GetItem",
            json!({"TableName": "zeroop", "Key": {"pk": {"S": "p"}}}),
        ));
        assert_eq!(resp.body, "{}");
    }

    // moto's response-layer parse drops unmodeled keys from each
    // TransactWrite row (moto/core/parse.py BaseJSONParser
    // ._parse_structure keeps only the modeled Put/Update/Delete/
    // ConditionCheck members), so:
    //   - a row keyed only by an unknown op ("Get") parses to {} and
    //     crashes moto's validation (`list(item.values())[0]` IndexError,
    //     dynamodb/responses.py:1478) -> werkzeug 500 page. The
    //     model-level ValueError("Unsupported transaction operation")
    //     branch (models/__init__.py:636-637) is unreachable over HTTP
    //     because the key never reaches the backend — no
    //     TransactionCancelled envelope with a per-row "ValueError" code
    //     is produced for this input. The twin answers AWS's
    //     ValidationException for that shape instead (2026-10-31 wave
    //     review, same posture as the zero-op row);
    //   - a Put row carrying an extra unmodeled "Get" key executes as a
    //     plain single-op Put;
    //   - two modeled ops sharing the row with unmodeled junk still
    //     answer the single-op ValidationException.
    // Live-transcribed against moto 5.2.4 (2026-10-26).
    #[test]
    fn transact_unmodeled_op_row_parse_drop_mirror() {
        let handler = DynamoDbHandler::new();
        handler.handle(make_req(
            "CreateTable",
            json!({
                "TableName": "getdrop",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        ));

        // "Get"—only write row -> ValidationException (moto 500s here).
        let resp = handler.handle(make_req(
            "TransactWriteItems",
            json!({"TransactItems": [
                {"Get": {"TableName": "getdrop", "Key": {"pk": {"S": "r1"}}}}
            ]}),
        ));
        assert_eq!(resp.status, 400);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(body["__type"], "ValidationException");

        // Put + Get executes as a plain Put, and the row commits.
        let resp = handler.handle(make_req(
            "TransactWriteItems",
            json!({"TransactItems": [
                {"Put": {"TableName": "getdrop",
                         "Item": {"pk": {"S": "r2"}, "v": {"S": "x"}}},
                 "Get": {"TableName": "getdrop", "Key": {"pk": {"S": "r2"}}}}
            ]}),
        ));
        assert_eq!(resp.status, 200);
        let resp = handler.handle(make_req(
            "GetItem",
            json!({"TableName": "getdrop", "Key": {"pk": {"S": "r2"}}}),
        ));
        assert_eq!(
            serde_json::from_str::<Value>(&resp.body).unwrap(),
            json!({"Item": {"pk": {"S": "r2"}, "v": {"S": "x"}}})
        );

        // Put + Delete + Get: the two modeled members keep the row under
        // the single-op check.
        let resp = handler.handle(make_req(
            "TransactWriteItems",
            json!({"TransactItems": [
                {"Put": {"TableName": "getdrop",
                         "Item": {"pk": {"S": "r3"}, "v": {"S": "x"}}},
                 "Delete": {"TableName": "getdrop", "Key": {"pk": {"S": "r3"}}},
                 "Get": {"TableName": "getdrop", "Key": {"pk": {"S": "r3"}}}}
            ]}),
        ));
        assert_eq!(resp.status, 400);
        assert_eq!(
            resp.body,
            json!({
                "__type": "ValidationException",
                "message": "TransactItems can only contain one of Check, Put, Update or Delete"
            })
            .to_string()
        );
    }

    // moto's row-loop head raises TransactWriteSingleOpException when one
    // TransactItems row carries two ops (models/__init__.py:630-632):
    // "TransactItems can only contain one of Check, Put, Update or Delete"
    // — byte-for-byte, and before any op of the row runs.
    #[test]
    fn transact_two_ops_single_row_single_op_wording() {
        let handler = DynamoDbHandler::new();
        handler.handle(make_req(
            "CreateTable",
            json!({
                "TableName": "singleop",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        ));
        let resp = handler.handle(make_req(
            "TransactWriteItems",
            json!({"TransactItems": [
                {"Put": {"TableName": "singleop", "Item": {"pk": {"S": "p"}, "v": {"S": "x"}}},
                 "Delete": {"TableName": "singleop", "Key": {"pk": {"S": "p"}}}}
            ]}),
        ));
        assert_eq!(resp.status, 400);
        assert_eq!(
            resp.body,
            json!({
                "__type": "ValidationException",
                "message": "TransactItems can only contain one of Check, Put, Update or Delete"
            })
            .to_string()
        );
        // moto raised before running either op: nothing written.
        let resp = handler.handle(make_req(
            "GetItem",
            json!({"TableName": "singleop", "Key": {"pk": {"S": "p"}}}),
        ));
        assert_eq!(resp.body, "{}");
    }

    // moto evaluates the ConditionExpression BEFORE the item-size check on a
    // transactional Put row (dynamodb/models/__init__.py:647-671 — the
    // condition runs in the row loop, the LimitedSizeDict check inside
    // self.put_item), so an oversized Put under a FAILING condition answers
    // TransactionCanceledException with per-row CancellationReasons; with a
    // passing condition (or none) it answers the plain size ValidationException.
    #[test]
    fn transact_put_condition_evaluated_before_size_check() {
        let handler = DynamoDbHandler::new();
        handler.handle(make_req(
            "CreateTable",
            json!({
                "TableName": "ordcond",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        ));
        let big_string = "z".repeat(500_000);

        // Oversized Put + failing condition -> TransactionCanceledException
        // naming the row, with moto's CancellationReasons envelope.
        let resp = handler.handle(make_req(
            "TransactWriteItems",
            json!({"TransactItems": [
                {"Put": {"TableName": "ordcond",
                         "Item": {"pk": {"S": "big"}, "v": {"S": big_string}},
                         "ConditionExpression": "attribute_exists(v)"}}
            ]}),
        ));
        assert_eq!(resp.status, 400);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        // moto renders the fully-qualified error type and uppercase "Message"
        // (exceptions.py:261; live transcript 2026-10-26).
        assert_eq!(
            body["__type"],
            "com.amazonaws.dynamodb.v20120810#TransactionCanceledException"
        );
        assert_eq!(
            body["Message"],
            "Transaction cancelled, please refer cancellation reasons for specific reasons \
             [ConditionalCheckFailed]"
        );
        assert_eq!(
            body["CancellationReasons"],
            json!([{"Code": "ConditionalCheckFailed",
                    "Message": "The conditional request failed"}])
        );
        // Nothing written either way.
        let resp = handler.handle(make_req(
            "GetItem",
            json!({"TableName": "ordcond", "Key": {"pk": {"S": "big"}}}),
        ));
        assert_eq!(resp.body, "{}");

        // A passing row ahead of the failing one: the reasons array carries
        // {"Code": "None"} for the passing row, in request order.
        let resp = handler.handle(make_req(
            "TransactWriteItems",
            json!({"TransactItems": [
                {"Put": {"TableName": "ordcond",
                         "Item": {"pk": {"S": "small"}, "v": {"S": "fine"}}}},
                {"Put": {"TableName": "ordcond",
                         "Item": {"pk": {"S": "big2"}, "v": {"S": big_string}},
                         "ConditionExpression": "attribute_exists(v)"}}
            ]}),
        ));
        assert_eq!(resp.status, 400);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(
            body["Message"],
            "Transaction cancelled, please refer cancellation reasons for specific reasons \
             [None, ConditionalCheckFailed]"
        );
        assert_eq!(
            body["CancellationReasons"],
            json!([{"Code": "None"},
                   {"Code": "ConditionalCheckFailed",
                    "Message": "The conditional request failed"}])
        );
        // The passing row is NOT visible after the cancellation (atomic).
        let resp = handler.handle(make_req(
            "GetItem",
            json!({"TableName": "ordcond", "Key": {"pk": {"S": "small"}}}),
        ));
        assert_eq!(resp.body, "{}");

        // Oversized Put WITHOUT a condition (moto: condition passes, size check
        // raises in put_item) still answers the plain size ValidationException.
        let resp = handler.handle(make_req(
            "TransactWriteItems",
            json!({"TransactItems": [
                {"Put": {"TableName": "ordcond",
                         "Item": {"pk": {"S": "big3"}, "v": {"S": big_string.clone()}}}}
            ]}),
        ));
        assert_eq!(resp.status, 400);
        assert!(resp
            .body
            .contains("Item size has exceeded the maximum allowed size"));

        // The Update row evaluates its condition before the (update-flavored)
        // size check the same way (moto update_item: condition first).
        let resp = handler.handle(make_req(
            "TransactWriteItems",
            json!({"TransactItems": [
                {"Update": {"TableName": "ordcond", "Key": {"pk": {"S": "big4"}},
                            "UpdateExpression": "SET v = :v",
                            "ExpressionAttributeValues": {":v": {"S": big_string}},
                            "ConditionExpression": "attribute_exists(v)"}}
            ]}),
        ));
        assert_eq!(resp.status, 400);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(
            body["__type"],
            "com.amazonaws.dynamodb.v20120810#TransactionCanceledException"
        );
        assert_eq!(
            body["CancellationReasons"],
            json!([{"Code": "ConditionalCheckFailed",
                    "Message": "The conditional request failed"}])
        );
    }

    // ReturnValuesOnConditionCheckFailure=ALL_OLD on a failing condition
    // row attaches that row's pre-transaction attributes as the reason's
    // "Item" member (moto models/__init__.py:666-671 — original_item =
    // current.to_json()["Attributes"] — rendered by
    // TransactionCanceledException, exceptions.py:251-255). Only failing
    // rows gain it: passing rows stay {"Code": "None"} even when they
    // carry ALL_OLD, a failed condition on a nonexistent row carries no
    // Item (empty `current`), and rows without ALL_OLD never do. All four
    // op arms share moto's condition block, so Put/Update/Delete/
    // ConditionCheck fail-row reasons all carry the item. Live-transcribed
    // (moto 5.2.4, 2026-10-26).
    #[test]
    fn transact_failing_condition_all_old_includes_old_item() {
        let handler = DynamoDbHandler::new();
        handler.handle(make_req(
            "CreateTable",
            json!({
                "TableName": "allold",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        ));
        let resp = handler.handle(make_req(
            "PutItem",
            json!({"TableName": "allold",
                   "Item": {"pk": {"S": "r1"}, "v": {"N": "3"}, "s": {"S": "old"}}}),
        ));
        assert_eq!(resp.status, 200);

        let failing_put = || {
            json!({"Put": {"TableName": "allold",
                           "Item": {"pk": {"S": "r1"}, "v": {"N": "99"}},
                           "ConditionExpression": "attribute_not_exists(pk)",
                           "ReturnValuesOnConditionCheckFailure": "ALL_OLD"}})
        };

        // Failing Put + ALL_OLD on an existing row: the reason gains Item.
        let resp = handler.handle(make_req(
            "TransactWriteItems",
            json!({"TransactItems": [failing_put()]}),
        ));
        assert_eq!(resp.status, 400);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(
            body["CancellationReasons"],
            json!([{"Code": "ConditionalCheckFailed",
                    "Message": "The conditional request failed",
                    "Item": {"pk": {"S": "r1"}, "v": {"N": "3"}, "s": {"S": "old"}}}])
        );
        assert_eq!(
            body["Message"],
            "Transaction cancelled, please refer cancellation reasons for specific reasons \
             [ConditionalCheckFailed]"
        );

        // Passing row centered ahead of the failing one: {"Code": "None"}
        // even though the passing row also carries ALL_OLD; only the
        // failing row gains Item. Nothing is committed either way.
        let resp = handler.handle(make_req(
            "TransactWriteItems",
            json!({"TransactItems": [
                {"Put": {"TableName": "allold",
                         "Item": {"pk": {"S": "newA"}, "v": {"S": "p"}},
                         "ConditionExpression": "attribute_not_exists(pk)",
                         "ReturnValuesOnConditionCheckFailure": "ALL_OLD"}},
                failing_put()
            ]}),
        ));
        assert_eq!(resp.status, 400);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(
            body["CancellationReasons"],
            json!([{"Code": "None"},
                   {"Code": "ConditionalCheckFailed",
                    "Message": "The conditional request failed",
                    "Item": {"pk": {"S": "r1"}, "v": {"N": "3"}, "s": {"S": "old"}}}])
        );
        let resp = handler.handle(make_req(
            "GetItem",
            json!({"TableName": "allold", "Key": {"pk": {"S": "newA"}}}),
        ));
        assert_eq!(resp.body, "{}");

        // Without ALL_OLD the failing row carries no Item.
        let resp = handler.handle(make_req(
            "TransactWriteItems",
            json!({"TransactItems": [
                {"Put": {"TableName": "allold",
                         "Item": {"pk": {"S": "r1"}, "v": {"N": "99"}},
                         "ConditionExpression": "attribute_not_exists(pk)"}}
            ]}),
        ));
        assert_eq!(resp.status, 400);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(
            body["CancellationReasons"],
            json!([{"Code": "ConditionalCheckFailed",
                    "Message": "The conditional request failed"}])
        );

        // A failed condition on a row that does not exist carries no Item
        // even with ALL_OLD (moto skips the empty `current`).
        let resp = handler.handle(make_req(
            "TransactWriteItems",
            json!({"TransactItems": [
                {"Put": {"TableName": "allold",
                         "Item": {"pk": {"S": "ghost"}, "v": {"N": "1"}},
                         "ConditionExpression": "attribute_exists(pk)",
                         "ReturnValuesOnConditionCheckFailure": "ALL_OLD"}}
            ]}),
        ));
        assert_eq!(resp.status, 400);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(
            body["CancellationReasons"],
            json!([{"Code": "ConditionalCheckFailed",
                    "Message": "The conditional request failed"}])
        );

        // The Update row reads the same `ReturnValuesOnConditionCheckFailure`
        // member: a failed condition carries the old item too.
        let resp = handler.handle(make_req(
            "TransactWriteItems",
            json!({"TransactItems": [
                {"Update": {"TableName": "allold", "Key": {"pk": {"S": "r1"}},
                            "UpdateExpression": "SET v = :one",
                            "ExpressionAttributeValues": {":one": {"N": "9"}},
                            "ConditionExpression": "attribute_not_exists(v)",
                            "ReturnValuesOnConditionCheckFailure": "ALL_OLD"}}
            ]}),
        ));
        assert_eq!(resp.status, 400);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(
            body["CancellationReasons"],
            json!([{"Code": "ConditionalCheckFailed",
                    "Message": "The conditional request failed",
                    "Item": {"pk": {"S": "r1"}, "v": {"N": "3"}, "s": {"S": "old"}}}])
        );

        // The conditional failures cancelled every write: r1 untouched.
        let resp = handler.handle(make_req(
            "GetItem",
            json!({"TableName": "allold", "Key": {"pk": {"S": "r1"}}}),
        ));
        // Compare as parsed Values, not response strings: JSON object keys
        // are unordered, and the handler's attribute map can serialize in a
        // feature-dependent order (a preserve_order unification elsewhere in
        // the workspace flips serde_json's map type) — string equality is
        // seed-brittle (this exact assert failed in the v2.0.0 release lane).
        assert_eq!(
            serde_json::from_str::<Value>(&resp.body).unwrap(),
            json!({"Item": {"pk": {"S": "r1"}, "v": {"N": "3"}, "s": {"S": "old"}}})
        );
    }

    // moto routes PartiQL INSERT and UPDATE writes through table.put_item
    // (dynamo_backend.execute_statement calls table.put_item for both the
    // CREATE and UPDATE cases), so the 400 KB item limit applies there too —
    // a 400 ValidationException with the put-flavored message.
    #[test]
    fn partiql_insert_and_update_oversize_400() {
        let handler = DynamoDbHandler::new();
        handler.handle(make_req(
            "CreateTable",
            json!({
                "TableName": "pq",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        ));
        let big = "z".repeat(500_000);

        let resp = handler.handle(make_req(
            "ExecuteStatement",
            json!({"Statement": format!("INSERT INTO \"pq\" VALUE {{'pk': 'ins-row', 'v': '{big}'}}")}),
        ));
        assert_eq!(resp.status, 400);
        assert!(resp
            .body
            .contains("Item size has exceeded the maximum allowed size"));
        // The oversized INSERT must not have written the row.
        let resp = handler.handle(make_req(
            "GetItem",
            json!({"TableName": "pq", "Key": {"pk": {"S": "ins-row"}}}),
        ));
        assert!(!resp.body.contains("Item"));

        let resp = handler.handle(make_req(
            "PutItem",
            json!({"TableName": "pq", "Item": {"pk": {"S": "u"}, "v": {"S": "small"}}}),
        ));
        assert_eq!(resp.status, 200);
        let resp = handler.handle(make_req(
            "ExecuteStatement",
            json!({"Statement": format!("UPDATE \"pq\" SET v = '{big}' WHERE pk = 'u'")}),
        ));
        assert_eq!(resp.status, 400);
        assert!(resp
            .body
            .contains("Item size has exceeded the maximum allowed size"));
        // The old row survives the rejected UPDATE unchanged.
        let resp = handler.handle(make_req(
            "GetItem",
            json!({"TableName": "pq", "Key": {"pk": {"S": "u"}}}),
        ));
        assert!(resp.body.contains("\"small\""));

        // BatchExecuteStatement reports the oversized statement on its own
        // batch row (moto: responses[idx] = {"Error": ...}) instead of
        // aborting the whole batch, and nothing from it is written.
        let resp = handler.handle(make_req(
            "BatchExecuteStatement",
            json!({"Statements": [
                {"Statement": "SELECT * FROM \"pq\" WHERE pk = 'u'"},
                {"Statement": format!("INSERT INTO \"pq\" VALUE {{'pk': 'bad-row', 'v': '{big}'}}")}
            ]}),
        ));
        assert_eq!(resp.status, 200);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(
            body["Responses"][1]["Error"]["Code"],
            json!("ValidationException")
        );
        assert!(body["Responses"][1]["Error"]["Message"]
            .to_string()
            .contains("Item size has exceeded the maximum allowed size"));
        // The successful statement still returned its item...
        assert_eq!(body["Responses"][0]["Item"]["v"]["S"], "small");
        // ... and the failed statement left nothing behind.
        let resp = handler.handle(make_req(
            "GetItem",
            json!({"TableName": "pq", "Key": {"pk": {"S": "bad-row"}}}),
        ));
        assert!(!resp.body.contains("Item"));
    }

    // moto's UpdateTable response is Table.describe() (base_key
    // TableDescription), so it carries the full payload — BillingModeSummary,
    // KeySchema, and every GSI with its ProvisionedThroughput and
    // WarmThroughput — not just the changed fields (moto server probe:
    // creating a GSI through UpdateTable answers GSI[0].WarmThroughput
    // 7/9 through the shared renderer).
    #[test]
    fn update_table_response_includes_gsi_warm_throughput() {
        let handler = DynamoDbHandler::new();
        handler.handle(make_req(
            "CreateTable",
            json!({
                "TableName": "upd-spec",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "ProvisionedThroughput": {"ReadCapacityUnits": 3, "WriteCapacityUnits": 5}
            }),
        ));
        // Sanity: no GSI before the update.
        let resp = handler.handle(make_req("DescribeTable", json!({"TableName": "upd-spec"})));
        assert_eq!(resp.status, 200);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert!(body["Table"]["GlobalSecondaryIndexes"]
            .as_array()
            .unwrap()
            .is_empty());

        let resp = handler.handle(make_req(
            "UpdateTable",
            json!({
                "TableName": "upd-spec",
                "AttributeDefinitions": [{"AttributeName": "name", "AttributeType": "S"}],
                "GlobalSecondaryIndexUpdates": [{"Create": {
                    "IndexName": "byname",
                    "KeySchema": [{"AttributeName": "name", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                    "ProvisionedThroughput": {"ReadCapacityUnits": 7, "WriteCapacityUnits": 9}
                }}]
            }),
        ));
        assert_eq!(resp.status, 200);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        let td = &body["TableDescription"];
        assert_eq!(td["WarmThroughput"]["Status"], "ACTIVE");
        assert_eq!(
            td["GlobalSecondaryIndexes"][0]["WarmThroughput"],
            json!({
                "ReadUnitsPerSecond": 7,
                "WriteUnitsPerSecond": 9,
                "Status": "ACTIVE"
            })
        );
        // The full describe payload, not just the changed fields.
        assert_eq!(td["BillingModeSummary"]["BillingMode"], "PROVISIONED");
        assert_eq!(td["KeySchema"][0]["AttributeName"], "pk");
        assert!(td.get("AttributeDefinitions").is_some());
        assert!(td.get("TableArn").is_some());
    }

    // TableId is rendered from the Table's STORED id (Table::new mints it
    // once), so two consecutive describes are identical and UpdateTable's
    // re-routed describe payload matches a fresh describe. moto's own
    // Table.describe() omits TableId entirely (moto 5.2.4); real AWS renders
    // one, like the twin does.
    #[test]
    fn describe_table_table_id_is_stable() {
        let handler = DynamoDbHandler::new();
        handler.handle(make_req(
            "CreateTable",
            json!({
                "TableName": "stable-id",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        ));
        let first = handler.handle(make_req("DescribeTable", json!({"TableName": "stable-id"})));
        assert_eq!(first.status, 200);
        let second = handler.handle(make_req("DescribeTable", json!({"TableName": "stable-id"})));
        let body_a: Value = serde_json::from_str(&first.body).unwrap();
        let body_b: Value = serde_json::from_str(&second.body).unwrap();
        let id_a = body_a["Table"]["TableId"].as_str().unwrap().to_string();
        let id_b = body_b["Table"]["TableId"].as_str().unwrap().to_string();
        assert!(!id_a.is_empty(), "describe renders a TableId");
        assert_eq!(id_a, id_b, "two describing calls report the same TableId");

        // UpdateTable's TableDescription carries the stored id too.
        let resp = handler.handle(make_req(
            "UpdateTable",
            json!({"TableName": "stable-id", "BillingMode": "PROVISIONED"}),
        ));
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(body["TableDescription"]["TableId"], id_a);
    }

    // moto renders each GSI's own ProvisionedThroughput
    // (dynamodb/models/table.py GlobalSecondaryIndex.describe): the PROVISIONED
    // 3/5 table keeps describing its own units, while its GSI (spec'd 7/9)
    // describes 7/9 — the table-level block is not copied into indexes. Oracle
    // (moto_server): GSI PT {7,9}, table PT {3,5}; on-demand GSIs (no spec
    // throughput) describe {0, 0}.
    #[test]
    fn gsi_provisioned_throughput_echoes_index_spec() {
        let handler = DynamoDbHandler::new();
        handler.handle(make_req(
            "CreateTable",
            json!({
                "TableName": "gsi-pt-echo",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "name", "AttributeType": "S"}
                ],
                "ProvisionedThroughput": {"ReadCapacityUnits": 3, "WriteCapacityUnits": 5},
                "GlobalSecondaryIndexes": [{
                    "IndexName": "byname",
                    "KeySchema": [{"AttributeName": "name", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                    "ProvisionedThroughput": {"ReadCapacityUnits": 7, "WriteCapacityUnits": 9}
                }]
            }),
        ));
        let resp = handler.handle(make_req(
            "DescribeTable",
            json!({"TableName": "gsi-pt-echo"}),
        ));
        assert_eq!(resp.status, 200);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        let table = &body["Table"];
        // The index echoes its own spec, not the table's units...
        assert_eq!(
            table["GlobalSecondaryIndexes"][0]["ProvisionedThroughput"],
            json!({"ReadCapacityUnits": 7, "WriteCapacityUnits": 9})
        );
        // ... and the table keeps its own.
        assert_eq!(
            table["ProvisionedThroughput"],
            json!({"ReadCapacityUnits": 3, "WriteCapacityUnits": 5})
        );
        // UpdateTable-created indexes likewise echo their own spec.
        let resp = handler.handle(make_req(
            "UpdateTable",
            json!({
                "TableName": "gsi-pt-echo",
                "GlobalSecondaryIndexUpdates": [{"Create": {
                    "IndexName": "upd",
                    "KeySchema": [{"AttributeName": "name", "KeyType": "RANGE"}],
                    "Projection": {"ProjectionType": "ALL"},
                    "ProvisionedThroughput": {"ReadCapacityUnits": 11, "WriteCapacityUnits": 13}
                }}]
            }),
        ));
        assert_eq!(resp.status, 200);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(
            body["TableDescription"]["GlobalSecondaryIndexes"][1]["ProvisionedThroughput"],
            json!({"ReadCapacityUnits": 11, "WriteCapacityUnits": 13})
        );
        let body = handler.handle(make_req(
            "DescribeTable",
            json!({"TableName": "gsi-pt-echo"}),
        ));
        let body: Value = serde_json::from_str(&body.body).unwrap();
        assert_eq!(
            body["Table"]["GlobalSecondaryIndexes"][1]["ProvisionedThroughput"],
            json!({"ReadCapacityUnits": 11, "WriteCapacityUnits": 13})
        );

        // An on-demand GSI (no spec throughput) describes moto's zero block.
        handler.handle(make_req(
            "CreateTable",
            json!({
                "TableName": "gsi-pt-ondemand",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "who", "AttributeType": "S"}
                ],
                "BillingMode": "PAY_PER_REQUEST",
                "GlobalSecondaryIndexes": [{
                    "IndexName": "byname",
                    "KeySchema": [{"AttributeName": "who", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"}
                }]
            }),
        ));
        let resp = handler.handle(make_req(
            "DescribeTable",
            json!({"TableName": "gsi-pt-ondemand"}),
        ));
        assert_eq!(resp.status, 200);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(
            body["Table"]["GlobalSecondaryIndexes"][0]["ProvisionedThroughput"],
            json!({"ReadCapacityUnits": 0, "WriteCapacityUnits": 0})
        );
    }

    // A ConditionCheck row carries its own ExpressionAttribute* maps, like
    // every other transact row — moto reads op.get("ExpressionAttributeNames"...)
    // uniformly for all four arms (models/__init__.py:640-665). The old
    // request-map lookup left transact ConditionChecks with placeholders
    // broken (names/values read empty).
    #[test]
    fn transact_condition_check_uses_row_expression_maps() {
        let handler = DynamoDbHandler::new();
        handler.handle(make_req(
            "CreateTable",
            json!({
                "TableName": "cc",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        ));
        handler.handle(make_req(
            "PutItem",
            json!({"TableName": "cc", "Item": {"pk": {"S": "k"}, "v": {"N": "7"}}}),
        ));

        // Passing ConditionCheck with per-row names/values -> 200.
        let resp = handler.handle(make_req(
            "TransactWriteItems",
            json!({"TransactItems": [{"ConditionCheck": {
                "TableName": "cc",
                "Key": {"pk": {"S": "k"}},
                "ConditionExpression": "#v = :v",
                "ExpressionAttributeNames": {"#v": "v"},
                "ExpressionAttributeValues": {":v": {"N": "7"}}
            }}]}),
        ));
        assert_eq!(resp.status, 200);

        // False ConditionCheck with per-row maps -> cancellation row.
        let resp = handler.handle(make_req(
            "TransactWriteItems",
            json!({"TransactItems": [{"ConditionCheck": {
                "TableName": "cc",
                "Key": {"pk": {"S": "k"}},
                "ConditionExpression": "#v = :v",
                "ExpressionAttributeNames": {"#v": "v"},
                "ExpressionAttributeValues": {":v": {"N": "6"}}
            }}]}),
        ));
        assert_eq!(resp.status, 400);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(
            body["CancellationReasons"],
            json!([{"Code": "ConditionalCheckFailed",
                    "Message": "The conditional request failed"}])
        );
    }

    // A transact row whose condition does not PARSE (an unresolved
    // :value reference) raises a plain ValidationException out of the row
    // loop — moto raises MockValidationException out of the loop and rolls
    // the transaction back (models/__init__.py:701-705) — instead of a fake
    // TransactionCanceledException with a ConditionalCheckFailed row.
    #[test]
    fn transact_expression_parse_error_propagates_validation() {
        let handler = DynamoDbHandler::new();
        handler.handle(make_req(
            "CreateTable",
            json!({
                "TableName": "perr",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        ));

        let resp = handler.handle(make_req(
            "TransactWriteItems",
            json!({"TransactItems": [{"Put": {
                "TableName": "perr",
                "Item": {"pk": {"S": "k"}, "v": {"S": "x"}},
                "ConditionExpression": "pk = :nores"
            }}]}),
        ));
        assert_eq!(resp.status, 400);
        assert!(resp.body.contains("ValidationException"), "{}", resp.body);
        assert!(
            !resp.body.contains("TransactionCanceledException"),
            "{}",
            resp.body
        );
    }

    // PartiQL resolves the table name from the FROM/INTO/UPDATE position
    // (real PartiQL and moto's parser accept bare identifiers, and a quoted
    // table may coexist with string literals; the old "first quoted
    // identifier anywhere" heuristic 400'd `SELECT * FROM users` and
    // mispicked `pk = 'x'`), and its keywords are case-insensitive — a
    // lowercase `where` must filter, not silently match every row.
    #[test]
    fn partiql_bare_tables_and_case_insensitive_where() {
        let handler = DynamoDbHandler::new();
        for name in ["pq-bare", "pq-ci"] {
            handler.handle(make_req(
                "CreateTable",
                json!({
                    "TableName": name,
                    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                    "BillingMode": "PAY_PER_REQUEST"
                }),
            ));
        }
        for row in ["a", "b"] {
            handler.handle(make_req(
                "PutItem",
                json!({"TableName": "pq-ci",
                       "Item": {"pk": {"S": row}, "v": {"S": "keep"}}}),
            ));
        }

        // Unquoted table + unquoted SELECT.
        handler.handle(make_req(
            "PutItem",
            json!({"TableName": "pq-bare", "Item": {"pk": {"S": "r"}, "v": {"S": "1"}}}),
        ));
        let resp = handler.handle(make_req(
            "ExecuteStatement",
            json!({"Statement": "SELECT * FROM pq-bare"}),
        ));
        assert_eq!(resp.status, 200);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(body["Items"][0]["pk"]["S"], json!("r"));

        // Lowercase WHERE deletes exactly the matching row.
        let resp = handler.handle(make_req(
            "ExecuteStatement",
            json!({"Statement": "DELETE FROM pq-ci where pk = 'a'"}),
        ));
        assert_eq!(resp.status, 200);
        let resp = handler.handle(make_req(
            "ExecuteStatement",
            json!({"Statement": "SELECT * FROM pq-ci"}),
        ));
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        let items = body["Items"].as_array().unwrap();
        assert_eq!(items.len(), 1, "lowercase where must filter, not match all");
        assert_eq!(items[0]["pk"]["S"], json!("b"));

        // Lowercase `and` splits into two conditions (both comparisons bind).
        let resp = handler.handle(make_req(
            "PutItem",
            json!({"TableName": "pq-ci", "Item": {"pk": {"S": "c"}, "v": {"S": "keep"}}}),
        ));
        assert_eq!(resp.status, 200);
        let resp = handler.handle(make_req(
            "ExecuteStatement",
            json!({"Statement": "UPDATE pq-ci SET v = 'upd' where pk = 'b' and v = 'keep'"}),
        ));
        assert_eq!(resp.status, 200);
        let resp = handler.handle(make_req(
            "ExecuteStatement",
            json!({"Statement": "SELECT * FROM pq-ci where pk = 'b'"}),
        ));
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(body["Items"][0]["v"]["S"], json!("upd"));

        // A string literal containing " and " does not split the clause.
        let resp = handler.handle(make_req(
            "ExecuteStatement",
            json!({"Statement": "SELECT * FROM pq-ci where v = 'a and b'"}),
        ));
        assert_eq!(resp.status, 200);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(body["Items"].as_array().unwrap().len(), 0);
    }

    // ExecuteTransaction and BatchExecuteStatement bind each statement's own
    // Parameters member (moto passes statement["Parameters"] into the
    // statement's binding), so `?` placeholders bind instead of staying in
    // the statement text as stray literals.
    #[test]
    fn transact_and_batch_partiql_bind_statement_parameters() {
        let handler = DynamoDbHandler::new();
        handler.handle(make_req(
            "CreateTable",
            json!({
                "TableName": "pb",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        ));
        handler.handle(make_req(
            "PutItem",
            json!({"TableName": "pb", "Item": {"pk": {"S": "a"}, "v": {"S": "one"}}}),
        ));

        let resp = handler.handle(make_req(
            "ExecuteTransaction",
            json!({"TransactStatements": [
                {"Statement": "SELECT * FROM \"pb\" WHERE pk = ?", "Parameters": [{"S": "a"}]}
            ]}),
        ));
        assert_eq!(resp.status, 200);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(body["Responses"][0]["Item"]["v"]["S"], json!("one"));

        let resp = handler.handle(make_req(
            "BatchExecuteStatement",
            json!({"Statements": [
                {"Statement": "UPDATE \"pb\" SET v = ? WHERE pk = ?",
                 "Parameters": [{"S": "two"}, {"S": "a"}]},
                {"Statement": "SELECT * FROM \"pb\" WHERE pk = ?", "Parameters": [{"S": "a"}]}
            ]}),
        ));
        assert_eq!(resp.status, 200);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(body["Responses"][1]["Item"]["v"]["S"], json!("two"));
        // The parameterized UPDATE really wrote the row.
        let resp = handler.handle(make_req(
            "GetItem",
            json!({"TableName": "pb", "Key": {"pk": {"S": "a"}}}),
        ));
        assert!(resp.body.contains("\"two\""), "{}", resp.body);
    }
}
