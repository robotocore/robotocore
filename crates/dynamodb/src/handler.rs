//! DynamoDB operation handler.

use parking_lot::RwLock;
use serde_json::{json, Map, Value};
use std::collections::HashMap;
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
        if table.billing_mode == "PAY_PER_REQUEST" {
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
                        index["ProvisionedThroughput"] = provisioned.clone();
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
            "TableId": format!("{}", uuid::Uuid::new_v4()),
            "TableArn": *table.table_arn.read(),
            "KeySchema": key_schema_json,
            "AttributeDefinitions": attr_defs_json,
            "CreationDateTime": format_created_at(table.created_at),
            "ItemCount": item_count,
            "TableSizeBytes": 0,
            "ProvisionedThroughput": provisioned,
            "LocalSecondaryIndexes": index_json(&lsis, false),
            "GlobalSecondaryIndexes": index_json(&gsis, true),
            "DeletionProtectionEnabled": false,
            "BillingModeSummary": { "BillingMode": table.billing_mode },
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

        let mut provisioned: Option<Value> = None;
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
            provisioned = Some(json!({"ReadCapacityUnits": rcu, "WriteCapacityUnits": wcu}));
        }

        // Merge attribute definitions supplied alongside index updates.
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

        for update in req
            .params
            .get("GlobalSecondaryIndexUpdates")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
        {
            if let Some(create) = update.get("Create") {
                let key_schema = Self::parse_key_schema(create.get("KeySchema"));
                if key_schema.is_empty() {
                    continue;
                }
                let projection = create
                    .get("Projection")
                    .and_then(|p| p.get("ProjectionType"))
                    .and_then(|t| t.as_str())
                    .unwrap_or("ALL")
                    .to_string();
                let non_key_attributes = create
                    .get("Projection")
                    .and_then(|p| p.get("NonKeyAttributes"))
                    .and_then(|n| n.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|x| x.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default();
                // moto's GlobalSecondaryIndex.create (used for indexes created
                // through UpdateTable too) reads the index's own throughput.
                let throughput = create.get("ProvisionedThroughput");
                let idx = crate::models::IndexDefinition {
                    name: create
                        .get("IndexName")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    key_schema,
                    projection,
                    non_key_attributes,
                    provisioned_rcu: throughput
                        .and_then(|p| p.get("ReadCapacityUnits"))
                        .and_then(|v| v.as_i64()),
                    provisioned_wcu: throughput
                        .and_then(|p| p.get("WriteCapacityUnits"))
                        .and_then(|v| v.as_i64()),
                };
                let mut gsis = table.global_secondary_indexes.write();
                if !gsis.iter().any(|g| g.name == idx.name) {
                    gsis.push(idx);
                }
            }
            if let Some(delete) = update.get("Delete") {
                if let Some(name) = delete.get("IndexName").and_then(|v| v.as_str()) {
                    table
                        .global_secondary_indexes
                        .write()
                        .retain(|g| g.name != name);
                }
            }
        }

        let mut description = json!({
            "TableName": table.name,
            "TableStatus": *table.status.read(),
        });
        if let Some(pt) = provisioned {
            description["ProvisionedThroughput"] = pt;
        }
        // moto's UpdateTable response is Table.describe(), so it carries the
        // same WarmThroughput block as DescribeTable — computed after the
        // mutations above, so PROVISIONED tables report the possibly-updated
        // units (moto/dynamodb/models/__init__.py update_table + Table.describe).
        description["WarmThroughput"] = Self::table_warm_throughput(&table);
        AwsResponse::json(200, json!({ "TableDescription": description }))
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

/// Enforce the real-AWS per-item 400 KB limit on the item about to be
/// written, mirroring moto's `LimitedSizeDict` check — a ValidationException
/// reading "Item size has exceeded the maximum allowed size" (moto issue
/// #1874). Numbers are sized with AWS's own formula per moto issue #10176
/// (`models::number_value_size`), so moto-oversized items are accepted.
fn check_item_size(attrs: &Map<String, Value>) -> Option<AwsResponse> {
    if crate::models::item_size(attrs) > crate::models::MAX_ITEM_SIZE {
        Some(AwsResponse::error(
            400,
            "ValidationException",
            "Item size has exceeded the maximum allowed size",
        ))
    } else {
        None
    }
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
        if let Some(e) = check_item_size(&attrs) {
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

        // moto's UpdateExpressionExecutor raises the same size check on the
        // executed result (getmoto/moto#1874); mirror it on the final item.
        if let Some(e) = check_item_size(&new_attrs) {
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
                body["LastEvaluatedKey"] = map_to_value(&kh_to_attrmap(&lk));
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
                    let Some(key) = item_key(&table, &key_attrs) else {
                        continue;
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

        if let Some(obj) = request_items.as_object() {
            for (table_name, writes) in obj {
                let Some(table) = state.get_table(table_name) else {
                    return AwsResponse::error(
                        400,
                        "ResourceNotFoundException",
                        &format!("Requested resource not found: Table: {table_name} not found"),
                    );
                };
                for write in writes.as_array().cloned().unwrap_or_default() {
                    if let Some(item_json) = write.pointer("/PutRequest/Item").cloned() {
                        let attrs: Map<String, Value> =
                            item_json.as_object().cloned().unwrap_or_default();
                        let Some(key) = item_key(&table, &attrs) else {
                            continue;
                        };
                        // moto's batch save lands in the same
                        // LimitedSizeDict check as put_item (getmoto/moto#1874).
                        if let Some(e) = check_item_size(&attrs) {
                            return e;
                        }
                        // Stream capture mirrors moto's batch put hook.
                        let old = table.items.read().get(&key).map(|i| i.attributes.clone());
                        table
                            .items
                            .write()
                            .insert(key, Arc::new(Item::new(attrs_to_map(&attrs))));
                        let old_serde = streaming::stream_enabled(&table)
                            .then(|| old.as_ref().map(attrmap_to_serde))
                            .flatten();
                        streaming::record_mutation(
                            &table,
                            old_serde.as_ref().map(|m| m as &Map<String, Value>),
                            Some(&attrs),
                        );
                    }
                    if let Some(key_json) = write.pointer("/DeleteRequest/Key").cloned() {
                        let key_attrs: Map<String, Value> =
                            key_json.as_object().cloned().unwrap_or_default();
                        if let Some(key) = item_key(&table, &key_attrs) {
                            let old = table.items.write().remove(&key);
                            let old_serde = streaming::stream_enabled(&table)
                                .then(|| old.as_ref().map(|i| attrmap_to_serde(&i.attributes)))
                                .flatten();
                            streaming::record_mutation(
                                &table,
                                old_serde.as_ref().map(|m| m as &Map<String, Value>),
                                None,
                            );
                        }
                    }
                }
            }
        }

        AwsResponse::json(200, json!({ "UnprocessedItems": {} }))
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
            key_attrs: Map<String, Value>,
            update: Option<String>,
            names: Names,
            values: Values,
        }
        struct PendingDelete {
            table: Arc<Table>,
            key: String,
        }

        let mut puts: Vec<PendingPut> = Vec::new();
        let mut updates: Vec<PendingUpdate> = Vec::new();
        let mut deletes: Vec<PendingDelete> = Vec::new();
        let mut cancellation: Vec<Option<String>> = Vec::new();
        let mut failed = false;

        let transact_items = req
            .params
            .get("TransactItems")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        for entry in &transact_items {
            let (op_name, op) = match ["Put", "Update", "Delete", "ConditionCheck"]
                .iter()
                .find_map(|k| entry.get(k).map(|o| (*k, Some(o))))
            {
                Some((k, Some(o))) => (k, o),
                _ => return AwsResponse::error(
                    400,
                    "ValidationException",
                    "TransactItems can only contain one of Put, Update, Delete or ConditionCheck",
                ),
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
                    // moto's transactional Put runs the same LimitedSizeDict
                    // check when it builds the Item (getmoto/moto#1874).
                    if let Some(e) = check_item_size(&attrs) {
                        return e;
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
                            cancellation.push(None);
                            puts.push(PendingPut {
                                table,
                                key,
                                attrs: attrs_to_map(&attrs),
                            });
                        }
                        Err(_) => {
                            failed = true;
                            cancellation.push(Some("ConditionalCheckFailed".to_string()));
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
                    let pre = existing(&table, &key);
                    let check = Self::eval_condition_on(
                        expr_str(op.get("ConditionExpression")),
                        pre.as_ref(),
                        &op_names(op),
                        &op_values(op),
                    );
                    match check {
                        Ok(()) => {
                            cancellation.push(None);
                            updates.push(PendingUpdate {
                                table,
                                key,
                                key_attrs,
                                update: expr_str(op.get("UpdateExpression")).map(String::from),
                                names: op_names(op),
                                values: op_values(op),
                            });
                        }
                        Err(_) => {
                            failed = true;
                            cancellation.push(Some("ConditionalCheckFailed".to_string()));
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
                    let pre = existing(&table, &key);
                    let check = Self::eval_condition_on(
                        expr_str(op.get("ConditionExpression")),
                        pre.as_ref(),
                        &op_names(op),
                        &op_values(op),
                    );
                    match check {
                        Ok(()) => {
                            cancellation.push(None);
                            deletes.push(PendingDelete { table, key });
                        }
                        Err(_) => {
                            failed = true;
                            cancellation.push(Some("ConditionalCheckFailed".to_string()));
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
                    let pre = existing(&table, &key);
                    let cond = expr_str(op.get("ConditionExpression"));
                    if cond.is_none() {
                        return AwsResponse::error(
                            400,
                            "ValidationException",
                            "ConditionExpression is required in ConditionCheck",
                        );
                    }
                    match Self::check_condition(req, cond, pre.as_ref().map(|m| m as &AttrMap)) {
                        Ok(()) => cancellation.push(None),
                        Err(_) => {
                            failed = true;
                            cancellation.push(Some("ConditionalCheckFailed".to_string()));
                        }
                    }
                }
            }
        }

        if failed {
            let reasons = cancellation
                .iter()
                .map(|r| r.clone().unwrap_or_else(|| "None".to_string()))
                .collect::<Vec<_>>()
                .join(", ");
            return AwsResponse::error(
                400,
                "TransactionCanceledException",
                &format!(
                    "Transaction cancelled, please refer cancellation reasons for specific reasons [{reasons}]"
                ),
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
            let mut new_attrs: Map<String, Value> = match &old {
                Some(m) => m.clone().into_iter().collect(),
                None => upd.key_attrs.clone(),
            };
            if let Some(update) = upd.update.as_deref() {
                if !update.trim().is_empty() {
                    if let Err(e) =
                        expr_engine::apply_update(&mut new_attrs, update, &upd.names, &upd.values)
                    {
                        return AwsResponse::error(400, "ValidationException", e.as_str());
                    }
                }
            }
            // moto's transactional Update raises the same size check on the
            // executed result above (getmoto/moto#1874).
            if let Some(e) = check_item_size(&new_attrs) {
                return e;
            }
            // Stream capture mirrors moto's transact update hook
            // (INSERT when the item was absent, MODIFY otherwise).
            let old_serde = streaming::stream_enabled(&upd.table)
                .then(|| old.as_ref().map(attrmap_to_serde))
                .flatten();
            streaming::record_mutation(
                &upd.table,
                old_serde.as_ref().map(|m| m as &Map<String, Value>),
                Some(&new_attrs),
            );
            upd.table
                .items
                .write()
                .insert(upd.key, Arc::new(Item::new(map_to_attrmap(new_attrs))));
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

        AwsResponse::json(200, json!({ "ConsumedCapacity": [] }))
    }

    fn transact_get_items(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let mut responses: Vec<Value> = Vec::new();
        for entry in req
            .params
            .get("TransactItems")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
        {
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
            let key = item_key(&table, &key_attrs).unwrap_or_default();
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
            }
            responses.push(Value::Object(entry_resp));
        }
        AwsResponse::json(
            200,
            json!({ "Responses": responses, "ConsumedCapacity": [] }),
        )
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
            match partiql_run(&state, statement, &[]) {
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
            match partiql_run(&state, statement, &[]) {
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

/// Parse a WHERE fragment into `path = literal [AND ...]` conditions.
fn parse_where(where_clause: &str, params: &[Value]) -> Option<Vec<PartiQlCond>> {
    let mut conds = Vec::new();
    for clause in where_clause.split(" AND ") {
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

/// Extract the first quoted identifier (the table name) from a statement.
fn table_name_of(stmt: &str) -> String {
    let mut in_quoted = false;
    let mut quote = '"';
    let mut name = String::new();
    for c in stmt.chars() {
        if in_quoted {
            if c == quote {
                return name;
            }
            name.push(c);
        } else if c == '"' || c == '`' || c == '\'' {
            in_quoted = true;
            quote = c;
        }
    }
    name
}

fn find_keyword(stmt: &str, keyword: &str) -> Option<usize> {
    stmt.find(keyword)
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

    let table_name = table_name_of(&stmt);
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
    let where_start = stmt
        .split_once(" WHERE ")
        .map(|(_, w)| w.to_string())
        .or_else(|| stmt.split_once(" WHERE").map(|(_, w)| w.trim().to_string()));
    let conds = match &where_start {
        Some(w) if !w.trim().is_empty() => parse_where(w.trim(), parameters).ok_or_else(invalid)?,
        _ => Vec::new(),
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
            let Some(value_pos) = find_keyword(&stmt, "VALUE") else {
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
            let set_start = stmt.find(" SET ").map(|p| p + 5);
            let set_text = match (set_start, where_start.as_ref()) {
                (Some(start), _) => {
                    // find WHERE after the SET clause
                    let after = &stmt[start..];
                    match after.find(" WHERE") {
                        Some(rel) => after[..rel].trim().to_string(),
                        None => after.trim().to_string(),
                    }
                }
                _ => return Err(invalid()),
            };
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
                let old = streaming::stream_enabled(&table)
                    .then(|| items.get(&key).map(|i| attrmap_to_serde(&i.attributes)))
                    .flatten();
                items.insert(key, Arc::new(Item::new(attrs.clone())));
                // Stream capture mirrors moto's PartiQL UPDATE (via put_item).
                streaming::record_mutation(
                    &table,
                    old.as_ref().map(|m| m as &Map<String, Value>),
                    Some(&attrmap_to_serde(&attrs)),
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
            source.billing_mode.clone(),
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

    // getmoto/moto#10176: with AWS's number sizing this ~150 KB item
    // (ceil(300001/2)+1 bytes for the Number) is legal; moto's
    // len(str(value)) sizing rejects it. The twin implements the AWS formula.
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
        // moto treats len(str(value)) = 300,000 > 405,000 as a 400.
        assert_eq!(resp.status, 200);
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
        assert!(resp
            .body
            .contains("Item size has exceeded the maximum allowed size"));

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
        assert!(resp
            .body
            .contains("Item size has exceeded the maximum allowed size"));
    }
}
