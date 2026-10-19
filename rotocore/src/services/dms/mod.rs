//! AWS DMS (Database Migration Service) backend — moto-equivalent (json
//! protocol).
//!
//! Replaces the moto sidecar for `dms` (`X-Amz-Target:
//! AmazonDMSv20160101.<Op>` on `application/x-amz-json-1.1`). State is
//! per-(account, region), mirroring moto's `DatabaseMigrationServiceBackend`
//! (moto/dms): endpoints, replication instances, replication tasks,
//! subnet groups, connections, event subscriptions, certificates,
//! replication configs (serverless), data providers, instance profiles,
//! migration projects, data migrations, and moto's `TaggingService`.
//!
//! Contract: probes/dms.json (per-operation moto-era behavior) and
//! tests/compatibility/test_dms_compat.py. Quirks kept deliberately:
//! - moto's `ManagedState` default progression is "immediate", so a fresh
//!   replication instance reads `available` on its first serialization and a
//!   connection reads `successful` — here those are birth statuses.
//! - `ListTagsForResource` with both/ neither selector reproduces moto's
//!   unguarded `ValueError` as a 500 `InternalError` (recorded in the probe).
//! - Unknown filter names in `DescribeEndpoint(s)`/`DescribeConnections`/
//!   `DescribeReplicationSubnetGroups` raise moto's `ValueError` → 500.
//! - `DescribeReplicationTaskIndividualAssessments*`, Fleet Advisor and
//!   metadata-model describes return moto's empty lists; the start/cancel
//!   operations mint a `RequestIdentifier` id and nothing more.

use crate::proto::{json_error, now_iso, parse_json_body};
use crate::store::{self, ScopeKey};
use crate::{Request, Response};
use parking_lot::RwLock;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

type Stores = Arc<RwLock<HashMap<ScopeKey, DmsStore>>>;

fn stores() -> Stores {
    store::slot::<DmsStore>("dms")
}

/// Per-(account, region) in-memory state for AWS DMS.
#[derive(Default)]
struct DmsStore {
    /// endpoint identifier -> endpoint
    endpoints: BTreeMap<String, Endpoint>,
    /// arn -> replication instance
    replication_instances: BTreeMap<String, ReplicationInstance>,
    /// arn -> replication task
    replication_tasks: BTreeMap<String, ReplicationTask>,
    /// identifier -> replication subnet group
    replication_subnet_groups: BTreeMap<String, ReplicationSubnetGroup>,
    /// TestConnection results, in creation order (moto's list).
    connections: Vec<Connection>,
    /// subscription name -> event subscription
    event_subscriptions: BTreeMap<String, EventSubscription>,
    /// certificate identifier -> certificate
    certificates: BTreeMap<String, Certificate>,
    /// identifier -> replication config (serverless)
    replication_configs: BTreeMap<String, ReplicationConfig>,
    /// data provider name -> data provider
    data_providers: BTreeMap<String, DataProvider>,
    /// instance profile name -> instance profile
    instance_profiles: BTreeMap<String, InstanceProfile>,
    /// migration project name -> migration project
    migration_projects: BTreeMap<String, MigrationProject>,
    /// data migration name -> data migration ("" for an absent name)
    data_migrations: BTreeMap<String, DataMigration>,
    /// arn -> {tag key: value} — moto's TaggingService
    tags: BTreeMap<String, BTreeMap<String, Option<String>>>,
}

fn with_store(req: &Request, f: impl FnOnce(&mut DmsStore) -> Response) -> Response {
    let slot = stores();
    let mut map = slot.write();
    let st = map.entry((req.account, req.region.clone())).or_default();
    f(st)
}

// ---------------------------------------------------------------------------
// Wire helpers
// ---------------------------------------------------------------------------

fn ok(body: Value) -> Response {
    Response::json(200, serde_json::to_vec(&body).unwrap_or_default())
}

fn ok_empty() -> Response {
    ok(json!({}))
}

fn not_found(message: &str) -> Response {
    json_error(400, "ResourceNotFoundFault", message)
}

fn already_exists(message: &str) -> Response {
    json_error(400, "ResourceAlreadyExistsFault", message)
}

fn invalid_resource_state(message: &str) -> Response {
    json_error(400, "InvalidResourceStateFault", message)
}

fn validation_error(message: &str) -> Response {
    json_error(400, "ValidationError", message)
}

/// moto's unguarded `ValueError` paths surface as a 500 `InternalError`.
fn internal_error(message: String) -> Response {
    json_error(500, "InternalError", &message)
}

/// moto's `random_id(uppercase=True, length=N)`: leading letter, then
/// alphanumerics.
fn moto_id(length: usize) -> String {
    let mut n = Uuid::new_v4().as_u128();
    let mut out = String::with_capacity(length);
    if length > 0 {
        // First char comes from the letters only, like moto's choice set.
        out.push((b'A' + (n % 26) as u8) as char);
        n /= 26;
    }
    for _ in 1..length {
        let digit = n % 36;
        n /= 36;
        out.push(if digit < 10 {
            (b'0' + digit as u8) as char
        } else {
            (b'A' + (digit - 10) as u8) as char
        });
    }
    out
}

fn opt_str(body: &Value, key: &str) -> Option<String> {
    body.get(key).and_then(Value::as_str).map(String::from)
}

fn opt_i64(body: &Value, key: &str) -> Option<i64> {
    body.get(key).and_then(Value::as_i64)
}

fn opt_bool(body: &Value, key: &str) -> Option<bool> {
    body.get(key).and_then(Value::as_bool)
}

/// A required string parameter; absent degrades to "" like moto's `.get()`
/// flowing into a `str` field.
fn req_str(body: &Value, key: &str) -> String {
    opt_str(body, key).unwrap_or_default()
}

fn str_list(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|list| {
            list.iter()
                .filter_map(Value::as_str)
                .map(String::from)
                .collect()
        })
        .unwrap_or_default()
}

/// The raw `Values` list of a filter object.
fn filter_values(filter: &Value) -> Vec<String> {
    str_list(filter.get("Values"))
}

/// Python truthiness: false for null, empty containers, ""; true otherwise.
fn truthy(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null) => false,
        Some(Value::Array(a)) => !a.is_empty(),
        Some(Value::Object(o)) => !o.is_empty(),
        Some(Value::String(s)) => !s.is_empty(),
        _ => true,
    }
}

/// Drop `null`-valued members — `{k: v ... if v is not None}` in moto.
fn drop_nulls(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let filtered: Map<String, Value> =
                map.into_iter().filter(|(_, v)| !v.is_null()).collect();
            Value::Object(filtered)
        }
        other => other,
    }
}

fn name_of(filter: &Value) -> String {
    filter
        .get("Name")
        .and_then(Value::as_str)
        .unwrap_or("None")
        .to_string()
}

/// Suffix ARN builder for `arn:aws:dms:{region}:{account}:{kind}:{id}`.
fn dms_arn(region: &str, account: u64, kind: &str, id: &str) -> String {
    format!("arn:aws:dms:{region}:{account}:{kind}:{id}")
}

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

/// The per-engine settings blocks accepted on an endpoint, in wire order.
const ENDPOINT_SETTING_KEYS: &[&str] = &[
    "DynamoDbSettings",
    "S3Settings",
    "DmsTransferSettings",
    "MongoDbSettings",
    "KinesisSettings",
    "KafkaSettings",
    "ElasticsearchSettings",
    "NeptuneSettings",
    "RedshiftSettings",
    "PostgreSQLSettings",
    "MySQLSettings",
    "OracleSettings",
    "SybaseSettings",
    "MicrosoftSQLServerSettings",
    "IBMDb2Settings",
    "DocDbSettings",
    "RedisSettings",
    "GcpMySQLSettings",
    "TimestreamSettings",
];

struct Endpoint {
    identifier: String,
    endpoint_type: String,
    engine_name: String,
    engine_display_name: String,
    username: Option<String>,
    password: Option<String>,
    server_name: Option<String>,
    port: Option<i64>,
    database_name: Option<String>,
    extra_connection_attributes: Option<String>,
    status: String,
    kms_key_id: Option<String>,
    endpoint_arn: String,
    certificate_arn: Option<String>,
    ssl_mode: Option<String>,
    service_access_role_arn: Option<String>,
    external_table_definition: Option<String>,
    /// engine-name -> raw settings object supplied by the client
    settings: BTreeMap<&'static str, Option<Value>>,
}

impl Endpoint {
    #[allow(clippy::too_many_arguments)]
    fn new(
        identifier: &str,
        endpoint_type: &str,
        engine_name: &str,
        username: Option<String>,
        password: Option<String>,
        server_name: Option<String>,
        port: Option<i64>,
        database_name: Option<String>,
        extra_connection_attributes: Option<String>,
        kms_key_id: Option<String>,
        certificate_arn: Option<String>,
        ssl_mode: Option<String>,
        service_access_role_arn: Option<String>,
        external_table_definition: Option<String>,
        resource_identifier: Option<&str>,
        body: &Value,
        region: &str,
        account: u64,
    ) -> Self {
        let id = resource_identifier
            .filter(|v| !v.is_empty())
            .map(String::from)
            .unwrap_or_else(|| moto_id(27));
        let settings: BTreeMap<&'static str, Option<Value>> = ENDPOINT_SETTING_KEYS
            .iter()
            .map(|key| (*key, body.get(*key).filter(|v| !v.is_null()).cloned()))
            .collect();
        Endpoint {
            identifier: identifier.to_string(),
            endpoint_type: endpoint_type.to_string(),
            engine_name: engine_name.to_string(),
            engine_display_name: engine_name.to_string(),
            username,
            password,
            server_name,
            port,
            database_name,
            extra_connection_attributes,
            status: "creating".to_string(),
            kms_key_id,
            endpoint_arn: dms_arn(region, account, "endpoint", &id),
            certificate_arn,
            ssl_mode,
            service_access_role_arn,
            external_table_definition,
            settings,
        }
    }

    /// moto's `Endpoint.to_dict()` — every member present, `None` for absent.
    fn to_dict(&self) -> Value {
        let settings: Value = self
            .settings
            .iter()
            .map(|(key, value)| (key.to_string(), value.clone().unwrap_or(Value::Null)))
            .collect();
        json!({
            "EndpointIdentifier": self.identifier,
            "EndpointType": self.endpoint_type,
            "EngineName": self.engine_name,
            "EngineDisplayName": self.engine_display_name,
            "Username": self.username,
            "ServerName": self.server_name,
            "Port": self.port,
            "DatabaseName": self.database_name,
            "ExtraConnectionAttributes": self.extra_connection_attributes,
            "Status": self.status,
            "KmsKeyId": self.kms_key_id,
            "EndpointArn": self.endpoint_arn,
            "CertificateArn": self.certificate_arn,
            "SslMode": self.ssl_mode,
            "ServiceAccessRoleArn": self.service_access_role_arn,
            "ExternalTableDefinition": self.external_table_definition,
            "ExternalId": Value::Null,
            "DynamoDbSettings": settings["DynamoDbSettings"],
            "S3Settings": settings["S3Settings"],
            "DmsTransferSettings": settings["DmsTransferSettings"],
            "MongoDbSettings": settings["MongoDbSettings"],
            "KinesisSettings": settings["KinesisSettings"],
            "KafkaSettings": settings["KafkaSettings"],
            "ElasticsearchSettings": settings["ElasticsearchSettings"],
            "NeptuneSettings": settings["NeptuneSettings"],
            "RedshiftSettings": settings["RedshiftSettings"],
            "PostgreSQLSettings": settings["PostgreSQLSettings"],
            "MySQLSettings": settings["MySQLSettings"],
            "OracleSettings": settings["OracleSettings"],
            "SybaseSettings": settings["SybaseSettings"],
            "MicrosoftSQLServerSettings": settings["MicrosoftSQLServerSettings"],
            "IBMDb2Settings": settings["IBMDb2Settings"],
            "DocDbSettings": settings["DocDbSettings"],
            "RedisSettings": settings["RedisSettings"],
            "GcpMySQLSettings": settings["GcpMySQLSettings"],
            "TimestreamSettings": settings["TimestreamSettings"],
        })
    }
}

struct ReplicationInstance {
    id: String,
    class: String,
    allocated_storage: i64,
    vpc_security_groups: Vec<Value>,
    availability_zone: Option<String>,
    subnet_group_identifier: Option<String>,
    preferred_maintenance_window: Option<String>,
    multi_az: Option<bool>,
    engine_version: Option<String>,
    auto_minor_version_upgrade: Option<bool>,
    kms_key_id: Option<String>,
    publicly_accessible: Option<bool>,
    dns_name_servers: Option<String>,
    network_type: Option<String>,
    kerberos: Option<Value>,
    arn: String,
    creation_date: String,
    status: String,
}

impl ReplicationInstance {
    #[allow(clippy::too_many_arguments)]
    fn new(body: &Value, region: &str, account: u64) -> Self {
        // moto: `allocated_storage or 50` — a 0 upgrade keeps the default.
        let allocated_storage = opt_i64(body, "AllocatedStorage")
            .filter(|v| *v != 0)
            .unwrap_or(50);
        let vpc_security_group_ids = str_list(body.get("VpcSecurityGroupIds"));
        ReplicationInstance {
            id: req_str(body, "ReplicationInstanceIdentifier"),
            class: req_str(body, "ReplicationInstanceClass"),
            allocated_storage,
            vpc_security_groups: vpc_security_group_ids
                .iter()
                .map(|sg| json!({ "VpcSecurityGroupId": sg, "Status": "active" }))
                .collect(),
            availability_zone: opt_str(body, "AvailabilityZone"),
            subnet_group_identifier: opt_str(body, "ReplicationSubnetGroupIdentifier"),
            preferred_maintenance_window: opt_str(body, "PreferredMaintenanceWindow"),
            multi_az: opt_bool(body, "MultiAZ"),
            engine_version: opt_str(body, "EngineVersion"),
            auto_minor_version_upgrade: opt_bool(body, "AutoMinorVersionUpgrade"),
            kms_key_id: opt_str(body, "KmsKeyId"),
            publicly_accessible: opt_bool(body, "PubliclyAccessible"),
            dns_name_servers: opt_str(body, "DnsNameServers"),
            network_type: opt_str(body, "NetworkType"),
            kerberos: body
                .get("KerberosAuthenticationSettings")
                .filter(|v| !v.is_null())
                .filter(|v| v.as_object().is_some_and(|o| !o.is_empty()))
                .cloned(),
            arn: dms_arn(
                region,
                account,
                "rep",
                &req_str(body, "ReplicationInstanceIdentifier"),
            ),
            creation_date: now_iso(),
            // moto's ManagedState reads `available` on its first serialization.
            status: "available".to_string(),
        }
    }

    /// moto's `FakeReplicationInstance.to_dict()`.
    fn to_dict(&self) -> Value {
        let subnet_group = self.subnet_group_identifier.as_ref().map(|identifier| {
            json!({
                "ReplicationSubnetGroupIdentifier": identifier,
                "ReplicationSubnetGroupDescription": format!("Subnet group for {}", self.id),
                "VpcId": "vpc-12345",
                "SubnetGroupStatus": "Complete",
                "Subnets": [{
                    "SubnetIdentifier": "subnet-12345",
                    "SubnetAvailabilityZone": {
                        "Name": self.availability_zone.clone().unwrap_or_else(|| "us-east-1a".to_string()),
                    },
                    "SubnetStatus": "Active",
                }],
                "SupportedNetworkTypes": match &self.network_type {
                    Some(nt) => json!([nt]),
                    None => json!(["IPV4"]),
                },
            })
        });
        let kerberos = self.kerberos.as_ref().map(|k| {
            json!({
                "KeyCacheSecretId": k.get("KeyCacheSecretId"),
                "KeyCacheSecretIamArn": k.get("KeyCacheSecretIamArn"),
                "Krb5FileContents": k.get("Krb5FileContents"),
            })
        });
        let secondary_az = if self.multi_az == Some(true) {
            self.availability_zone
                .as_ref()
                .map(|az| json!(format!("{az}b")))
                .unwrap_or(Value::Null)
        } else {
            Value::Null
        };
        json!({
            "ReplicationInstanceIdentifier": self.id,
            "ReplicationInstanceClass": self.class,
            "ReplicationInstanceStatus": self.status,
            "AllocatedStorage": self.allocated_storage,
            "InstanceCreateTime": self.creation_date,
            "VpcSecurityGroups": self.vpc_security_groups,
            "AvailabilityZone": self.availability_zone,
            "ReplicationSubnetGroup": subnet_group,
            "PreferredMaintenanceWindow": self.preferred_maintenance_window,
            "PendingModifiedValues": {},
            "MultiAZ": self.multi_az,
            "EngineVersion": self.engine_version,
            "AutoMinorVersionUpgrade": self.auto_minor_version_upgrade,
            "KmsKeyId": self.kms_key_id,
            "ReplicationInstanceArn": self.arn,
            "ReplicationInstancePublicIpAddress": if self.publicly_accessible == Some(true) {
                json!("54.0.0.1")
            } else {
                Value::Null
            },
            "ReplicationInstancePrivateIpAddress": json!("10.0.0.1"),
            "ReplicationInstancePublicIpAddresses": if self.publicly_accessible == Some(true) {
                json!(["54.0.0.1"])
            } else {
                json!([])
            },
            "ReplicationInstancePrivateIpAddresses": json!(["10.0.0.1"]),
            "ReplicationInstanceIpv6Addresses": json!([]),
            "PubliclyAccessible": self.publicly_accessible,
            "SecondaryAvailabilityZone": secondary_az,
            "FreeUntil": Value::Null,
            "DnsNameServers": self.dns_name_servers,
            "NetworkType": self.network_type,
            "KerberosAuthenticationSettings": kerberos,
        })
    }
}

struct ReplicationSubnetGroup {
    identifier: String,
    description: String,
    subnet_ids: Option<Vec<String>>,
}

impl ReplicationSubnetGroup {
    fn to_dict(&self) -> Value {
        json!({
            "ReplicationSubnetGroupIdentifier": self.identifier,
            "ReplicationSubnetGroupDescription": self.description,
            "VpcId": "vpc-12345",
            "SubnetGroupStatus": "Complete",
            "Subnets": [{
                "SubnetIdentifier": "subnet-12345",
                "SubnetAvailabilityZone": { "Name": "us-east-1a" },
                "SubnetStatus": "Active",
            }],
            "SupportedNetworkTypes": ["IPV4"],
        })
    }
}

struct Connection {
    replication_instance_arn: String,
    endpoint_arn: String,
    replication_instance_identifier: String,
    endpoint_identifier: String,
    /// moto's `ManagedState` reads `successful` immediately.
    status: String,
}

impl Connection {
    fn to_dict(&self) -> Value {
        json!({
            "ReplicationInstanceArn": self.replication_instance_arn,
            "EndpointArn": self.endpoint_arn,
            "Status": self.status,
            "LastFailureMessage": "",
            "EndpointIdentifier": self.endpoint_identifier,
            "ReplicationInstanceIdentifier": self.replication_instance_identifier,
        })
    }
}

struct ReplicationTask {
    id: String,
    migration_type: String,
    replication_instance_arn: String,
    source_endpoint_arn: String,
    target_endpoint_arn: String,
    table_mappings: Option<String>,
    replication_task_settings: Option<String>,
    arn: String,
    status: String,
    creation_date: String,
    start_date: Option<String>,
    stop_date: Option<String>,
}

impl ReplicationTask {
    fn to_dict(&self) -> Value {
        let stats = {
            let mut stats = json!({
                "FullLoadProgressPercent": 100,
                "ElapsedTimeMillis": 100,
                "TablesLoaded": 1,
                "TablesLoading": 0,
                "TablesQueued": 0,
                "TablesErrored": 0,
                "FreshStartDate": self.start_date,
                "StartDate": self.start_date,
                "StopDate": self.stop_date,
                "FullLoadStartDate": self.start_date,
                "FullLoadFinishDate": self.stop_date,
            });
            if let Some(obj) = stats.as_object_mut() {
                // moto serializes these nulls straight through.
                for key in [
                    "FreshStartDate",
                    "StartDate",
                    "StopDate",
                    "FullLoadStartDate",
                    "FullLoadFinishDate",
                ] {
                    if obj[key].is_null() {
                        obj[key] = Value::Null;
                    }
                }
            }
            stats
        };
        json!({
            "ReplicationTaskIdentifier": self.id,
            "SourceEndpointArn": self.source_endpoint_arn,
            "TargetEndpointArn": self.target_endpoint_arn,
            "ReplicationInstanceArn": self.replication_instance_arn,
            "MigrationType": self.migration_type,
            "TableMappings": self.table_mappings,
            "ReplicationTaskSettings": self.replication_task_settings,
            "Status": self.status,
            "ReplicationTaskCreationDate": self.creation_date,
            "ReplicationTaskStartDate": self.start_date,
            "ReplicationTaskArn": self.arn,
            "ReplicationTaskStats": stats,
        })
    }
}

struct EventSubscription {
    name: String,
    sns_topic_arn: Option<String>,
    source_type: String,
    event_categories: Vec<String>,
    source_ids: Vec<String>,
    enabled: bool,
    arn: String,
    creation_time: String,
    customer_aws_id: String,
}

impl EventSubscription {
    fn to_dict(&self) -> Value {
        json!({
            "CustomerAwsId": self.customer_aws_id,
            "CustSubscriptionId": self.name,
            "SnsTopicArn": self.sns_topic_arn,
            "Status": "active",
            "SubscriptionCreationTime": self.creation_time,
            "SourceType": self.source_type,
            "SourceIdsList": self.source_ids,
            "EventCategoriesList": self.event_categories,
            "Enabled": self.enabled,
        })
    }
}

struct Certificate {
    identifier: String,
    pem: Option<String>,
    wallet: Option<String>,
    creation_date: String,
    arn: String,
}

impl Certificate {
    fn to_dict(&self) -> Value {
        json!({
            "CertificateIdentifier": self.identifier,
            "CertificateCreationDate": self.creation_date,
            "CertificatePem": self.pem,
            "CertificateWallet": self.wallet,
            "CertificateArn": self.arn,
            "CertificateOwner": "",
            "ValidFromDate": self.creation_date,
            "ValidToDate": self.creation_date,
            "SigningAlgorithm": "SHA256withRSA",
            "KeyLength": 2048,
        })
    }
}

struct ReplicationConfig {
    identifier: String,
    source_endpoint_arn: String,
    target_endpoint_arn: String,
    compute_config: Value,
    replication_type: String,
    table_mappings: Option<String>,
    replication_settings: Option<String>,
    supplemental_settings: Option<String>,
    arn: String,
    status: String,
    creation_date: String,
}

impl ReplicationConfig {
    fn to_dict(&self) -> Value {
        json!({
            "ReplicationConfigIdentifier": self.identifier,
            "ReplicationConfigArn": self.arn,
            "SourceEndpointArn": self.source_endpoint_arn,
            "TargetEndpointArn": self.target_endpoint_arn,
            "ReplicationType": self.replication_type,
            "ComputeConfig": self.compute_config,
            "ReplicationSettings": self.replication_settings,
            "SupplementalSettings": self.supplemental_settings,
            "TableMappings": self.table_mappings,
            "ReplicationConfigCreateTime": self.creation_date,
        })
    }

    /// The summary shape `StartReplication`/`StopReplication` return.
    fn to_summary(&self) -> Value {
        json!({
            "ReplicationConfigIdentifier": self.identifier,
            "ReplicationConfigArn": self.arn,
            "SourceEndpointArn": self.source_endpoint_arn,
            "TargetEndpointArn": self.target_endpoint_arn,
            "ReplicationType": self.replication_type,
            "Status": self.status,
        })
    }
}

struct DataProvider {
    name: String,
    engine: Option<String>,
    settings: Value,
    description: String,
    creation_date: String,
    arn: String,
}

impl DataProvider {
    fn to_dict(&self) -> Value {
        json!({
            "DataProviderName": self.name,
            "DataProviderArn": self.arn,
            "DataProviderCreationTime": self.creation_date,
            "Description": self.description,
            "Engine": self.engine,
            "Settings": self.settings,
        })
    }
}

struct InstanceProfile {
    name: String,
    availability_zone: Option<String>,
    kms_key_arn: Option<String>,
    publicly_accessible: bool,
    network_type: Option<String>,
    description: String,
    subnet_group_identifier: Option<String>,
    vpc_security_groups: Vec<String>,
    creation_date: String,
    arn: String,
}

impl InstanceProfile {
    fn to_dict(&self) -> Value {
        json!({
            "InstanceProfileArn": self.arn,
            "AvailabilityZone": self.availability_zone,
            "KmsKeyArn": self.kms_key_arn,
            "PubliclyAccessible": self.publicly_accessible,
            "NetworkType": self.network_type,
            "InstanceProfileName": self.name,
            "Description": self.description,
            "InstanceProfileCreationTime": self.creation_date,
            "SubnetGroupIdentifier": self.subnet_group_identifier,
            "VpcSecurityGroups": self.vpc_security_groups,
        })
    }
}

struct MigrationProject {
    name: String,
    source_descriptors: Vec<Value>,
    target_descriptors: Vec<Value>,
    instance_profile_identifier: Option<String>,
    transformation_rules: Option<String>,
    description: String,
    schema_conversion_attributes: Option<Value>,
    creation_date: String,
    arn: String,
}

impl MigrationProject {
    fn to_dict(&self) -> Value {
        json!({
            "MigrationProjectName": self.name,
            "MigrationProjectArn": self.arn,
            "MigrationProjectCreationTime": self.creation_date,
            "SourceDataProviderDescriptors": self.source_descriptors,
            "TargetDataProviderDescriptors": self.target_descriptors,
            "InstanceProfileArn": self.instance_profile_identifier,
            "InstanceProfileName": "",
            "TransformationRules": self.transformation_rules,
            "Description": self.description,
            "SchemaConversionApplicationAttributes": self.schema_conversion_attributes,
        })
    }
}

struct DataMigration {
    name: String,
    migration_project_identifier: String,
    data_migration_type: String,
    service_access_role_arn: Option<String>,
    source_data_settings: Vec<Value>,
    enable_cloudwatch_logs: bool,
    status: String,
    creation_date: String,
    arn: String,
}

impl DataMigration {
    fn to_dict(&self) -> Value {
        json!({
            "DataMigrationName": self.name,
            "DataMigrationArn": self.arn,
            "DataMigrationCreateTime": self.creation_date,
            "DataMigrationType": self.data_migration_type,
            "ServiceAccessRoleArn": self.service_access_role_arn,
            "MigrationProjectArn": self.migration_project_identifier,
            "DataMigrationSettings": {
                "EnableCloudwatchLogs": self.enable_cloudwatch_logs,
            },
            "SourceDataSettings": self.source_data_settings,
            "DataMigrationStatus": self.status,
        })
    }
}

// ---------------------------------------------------------------------------
// Tags (moto's TaggingService semantics)
// ---------------------------------------------------------------------------

fn tag_resource(st: &mut DmsStore, arn: &str, tags: Option<&Value>) {
    let Some(tags) = tags.and_then(Value::as_array) else {
        return;
    };
    let row = st.tags.entry(arn.to_string()).or_default();
    for tag in tags {
        let Some(key) = tag.get("Key").and_then(Value::as_str) else {
            continue;
        };
        // A member without `Value` stores null, just like moto's tagger.
        row.insert(
            key.to_string(),
            tag.get("Value").and_then(Value::as_str).map(String::from),
        );
    }
}

fn untag_resource(st: &mut DmsStore, arn: &str, tag_keys: Option<&Value>) {
    let Some(keys) = tag_keys.and_then(Value::as_array) else {
        return;
    };
    if let Some(row) = st.tags.get_mut(arn) {
        for key in keys {
            if let Some(name) = key.as_str() {
                row.remove(name);
            }
        }
    }
}

fn tag_list(st: &DmsStore, resource_arns: &[String]) -> Vec<Value> {
    let mut out = Vec::new();
    for arn in resource_arns {
        if let Some(row) = st.tags.get(arn) {
            for (key, value) in row {
                out.push(json!({
                    "Key": key,
                    "ResourceArn": arn,
                    "Value": value,
                }));
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "dms" {
        return None;
    }
    Some(with_store(req, |st| route(req, st)))
}

fn route(req: &Request, st: &mut DmsStore) -> Response {
    let body = parse_json_body(&req.body);
    let account = req.account;
    let region = req.region.as_str();
    match req.operation.as_str() {
        // --- tagging -------------------------------------------------------
        "AddTagsToResource" => add_tags(st, &body),
        "RemoveTagsFromResource" => {
            untag_resource(st, &req_str(&body, "ResourceArn"), body.get("TagKeys"));
            ok_empty()
        }
        "ListTagsForResource" => list_tags(st, &body),
        // --- replication tasks ----------------------------------------------
        "CreateReplicationTask" => create_replication_task(st, account, region, &body),
        "StartReplicationTask" => task_action(st, &req_str(&body, "ReplicationTaskArn"), |task| {
            task.status = "starting".to_string();
            task.start_date = Some(now_iso());
            task.status = "running".to_string();
            Ok(task.to_dict())
        }),
        "StopReplicationTask" => task_action(st, &req_str(&body, "ReplicationTaskArn"), |task| {
            if task.status != "running" {
                return Err(invalid_resource_state("Replication task is not running"));
            }
            task.status = "stopped".to_string();
            task.stop_date = Some(now_iso());
            Ok(task.to_dict())
        }),
        "DeleteReplicationTask" => {
            let arn = req_str(&body, "ReplicationTaskArn");
            match st.replication_tasks.get_mut(&arn) {
                Some(task) => {
                    task.status = "deleting".to_string();
                    let dict = task.to_dict();
                    let owned = task.arn.clone();
                    st.replication_tasks.remove(&owned);
                    ok(json!({ "ReplicationTask": dict }))
                }
                None => not_found("Replication task could not be found."),
            }
        }
        "DescribeReplicationTasks" => describe_replication_tasks(st, &body),
        "ModifyReplicationTask" => modify_replication_task(st, &body),
        "MoveReplicationTask" => task_action(st, &req_str(&body, "ReplicationTaskArn"), |task| {
            task.replication_instance_arn = req_str(&body, "TargetReplicationInstanceArn");
            Ok(task.to_dict())
        }),
        // --- replication instances -------------------------------------------
        "CreateReplicationInstance" => create_replication_instance(st, account, region, &body),
        "DescribeReplicationInstances" => describe_replication_instances(st, &body),
        "DeleteReplicationInstance" => {
            delete_replication_instance(st, &req_str(&body, "ReplicationInstanceArn"))
        }
        "ModifyReplicationInstance" => modify_replication_instance(st, &body),
        "RebootReplicationInstance" => {
            let arn = req_str(&body, "ReplicationInstanceArn");
            match st.replication_instances.get(&arn) {
                Some(ri) => ok(json!({ "ReplicationInstance": ri.to_dict() })),
                None => not_found("Replication instance could not be found."),
            }
        }
        "DescribeReplicationInstanceTaskLogs" => {
            let arn = req_str(&body, "ReplicationInstanceArn");
            match st.replication_instances.get(&arn) {
                Some(_) => ok(json!({
                    "ReplicationInstanceArn": arn,
                    "ReplicationInstanceTaskLogs": [],
                })),
                None => not_found("Replication instance could not be found."),
            }
        }
        // --- endpoints ---------------------------------------------------------
        "CreateEndpoint" => create_endpoint(st, account, region, &body),
        "DescribeEndpoints" => describe_endpoints(st, &body),
        "DeleteEndpoint" => delete_endpoint(st, &req_str(&body, "EndpointArn")),
        "ModifyEndpoint" => modify_endpoint(st, &body),
        // --- replication subnet groups ----------------------------------------
        "CreateReplicationSubnetGroup" => create_subnet_group(st, &body),
        "DescribeReplicationSubnetGroups" => describe_subnet_groups(st, &body),
        "DeleteReplicationSubnetGroup" => {
            let id = req_str(&body, "ReplicationSubnetGroupIdentifier");
            match st.replication_subnet_groups.remove(&id) {
                Some(_) => ok_empty(),
                None => not_found("Replication subnet group could not be found."),
            }
        }
        "ModifyReplicationSubnetGroup" => modify_subnet_group(st, &body),
        // --- connections ---------------------------------------------------------
        "TestConnection" => test_connection(st, &body),
        "DescribeConnections" => describe_connections(st, &body),
        "DeleteConnection" => delete_connection(st, &body),
        // --- event subscriptions --------------------------------------------------
        "CreateEventSubscription" => create_event_subscription(st, account, region, &body),
        "DescribeEventSubscriptions" => {
            let mut subs: Vec<Value> = Vec::new();
            match opt_str(&body, "SubscriptionName") {
                Some(name) => match st.event_subscriptions.get(&name) {
                    Some(sub) => subs.push(sub.to_dict()),
                    // moto ignores `Filters` here — only the head name selector.
                    None => return not_found("Subscription could not be found."),
                },
                None => {
                    for sub in st.event_subscriptions.values() {
                        subs.push(sub.to_dict());
                    }
                }
            }
            ok(json!({ "EventSubscriptionsList": subs }))
        }
        "DeleteEventSubscription" => {
            let name = req_str(&body, "SubscriptionName");
            match st.event_subscriptions.remove(&name) {
                Some(sub) => ok(json!({ "EventSubscription": sub.to_dict() })),
                None => not_found("Subscription could not be found."),
            }
        }
        "ModifyEventSubscription" => modify_event_subscription(st, &body),
        // --- certificates --------------------------------------------------------------
        "ImportCertificate" => import_certificate(st, account, region, &body),
        "DescribeCertificates" => describe_certificates(st, &body),
        "DeleteCertificate" => {
            let arn = req_str(&body, "CertificateArn");
            let found = st.certificates.values().any(|c| c.arn == arn);
            if !found {
                return not_found("Certificate could not be found.");
            }
            let identifier = st
                .certificates
                .values()
                .find(|c| c.arn == arn)
                .map(|c| c.identifier.clone())
                .expect("checked above");
            let cert = st.certificates.remove(&identifier).expect("checked above");
            ok(json!({ "Certificate": cert.to_dict() }))
        }
        // --- replication configs (serverless) -----------------------------------------
        "CreateReplicationConfig" => create_replication_config(st, account, region, &body),
        "DescribeReplicationConfigs" => describe_replication_configs(st, &body),
        "DeleteReplicationConfig" => {
            delete_replication_config(st, &req_str(&body, "ReplicationConfigArn"))
        }
        "ModifyReplicationConfig" => modify_replication_config(st, &body),
        "StartReplication" => {
            flip_replication(st, &req_str(&body, "ReplicationConfigArn"), "running")
        }
        "StopReplication" => {
            flip_replication(st, &req_str(&body, "ReplicationConfigArn"), "stopped")
        }
        "DescribeReplications" => describe_replications(st, &body),
        // --- data providers / instance profiles / migration projects --------------------
        "CreateDataProvider" => create_data_provider(st, account, region, &body),
        "DescribeDataProviders" => describe_data_providers(st, &body),
        "ModifyDataProvider" => modify_data_provider(st, &body),
        "DeleteDataProvider" => delete_data_provider(st, &req_str(&body, "DataProviderIdentifier")),
        "CreateInstanceProfile" => create_instance_profile(st, account, region, &body),
        "DescribeInstanceProfiles" => describe_instance_profiles(st, &body),
        "ModifyInstanceProfile" => modify_instance_profile(st, &body),
        "DeleteInstanceProfile" => {
            delete_instance_profile(st, &req_str(&body, "InstanceProfileIdentifier"))
        }
        "CreateMigrationProject" => create_migration_project(st, account, region, &body),
        "DescribeMigrationProjects" => describe_migration_projects(st, &body),
        "ModifyMigrationProject" => modify_migration_project(st, &body),
        "DeleteMigrationProject" => {
            delete_migration_project(st, &req_str(&body, "MigrationProjectIdentifier"))
        }
        // --- data migrations -------------------------------------------------------------
        "CreateDataMigration" => create_data_migration(st, account, region, &body),
        "DescribeDataMigrations" => describe_data_migrations(st, &body),
        "ModifyDataMigration" => modify_data_migration(st, &body),
        "DeleteDataMigration" => {
            delete_data_migration(st, &req_str(&body, "DataMigrationIdentifier"))
        }
        "StartDataMigration" => {
            flip_data_migration(st, &req_str(&body, "DataMigrationIdentifier"), "running")
        }
        "StopDataMigration" => {
            flip_data_migration(st, &req_str(&body, "DataMigrationIdentifier"), "stopped")
        }
        // --- describes / static stubs ----------------------------------------------------
        "DescribeAccountAttributes" => describe_account_attributes(st, account),
        "DescribeEndpointTypes" => ok(json!({ "SupportedEndpointTypes": endpoint_types() })),
        "DescribeEndpointSettings" => ok(json!({ "EndpointSettings": [] })),
        "DescribeOrderableReplicationInstances" => {
            ok(json!({ "OrderableReplicationInstances": orderable_instances(region) }))
        }
        "DescribeEventCategories" => {
            let source_type = opt_str(&body, "SourceType");
            let categories: Vec<Value> = [
                (
                    "replication-instance",
                    vec![
                        "low storage",
                        "configuration change",
                        "maintenance",
                        "deletion",
                        "creation",
                        "failover",
                        "failure",
                    ],
                ),
                (
                    "replication-task",
                    vec![
                        "state change",
                        "creation",
                        "deletion",
                        "failure",
                        "configuration change",
                    ],
                ),
            ]
            .iter()
            .filter(|(source, _)| source_type.as_deref().is_none_or(|given| given == *source))
            .map(|(source_type, categories)| {
                json!({
                    "SourceType": source_type,
                    "EventCategories": categories,
                })
            })
            .collect();
            ok(json!({ "EventCategoryGroupList": categories }))
        }
        "DescribeEvents" => ok(json!({ "Events": [] })),
        "DescribeTableStatistics" => {
            let arn = req_str(&body, "ReplicationTaskArn");
            match st.replication_tasks.get(&arn) {
                Some(_) => ok(json!({ "ReplicationTaskArn": arn, "TableStatistics": [] })),
                None => not_found("Replication task could not be found."),
            }
        }
        "DescribeReplicationTableStatistics" => ok(json!({
            "ReplicationConfigArn": req_str(&body, "ReplicationConfigArn"),
            "ReplicationTableStatistics": [],
        })),
        "DescribeSchemas" => ok(json!({ "Schemas": [] })),
        "RefreshSchemas" => ok(json!({
            "RefreshSchemasStatus": {
                "EndpointArn": req_str(&body, "EndpointArn"),
                "ReplicationInstanceArn": req_str(&body, "ReplicationInstanceArn"),
                "Status": "successful",
                "LastRefreshDate": now_iso(),
                "LastFailureMessage": "",
            }
        })),
        "DescribeRefreshSchemasStatus" => ok(json!({
            "RefreshSchemasStatus": {
                "EndpointArn": req_str(&body, "EndpointArn"),
                "ReplicationInstanceArn": "",
                "Status": "successful",
                "LastRefreshDate": now_iso(),
                "LastFailureMessage": "",
            }
        })),
        "ReloadTables" => {
            let arn = req_str(&body, "ReplicationTaskArn");
            match st.replication_tasks.get(&arn) {
                Some(_) => ok(json!({ "ReplicationTaskArn": arn })),
                None => not_found("Replication task could not be found."),
            }
        }
        "ReloadReplicationTables" => {
            ok(json!({ "ReplicationConfigArn": req_str(&body, "ReplicationConfigArn") }))
        }
        "DescribeApplicableIndividualAssessments" => ok(json!({
            "IndividualAssessmentNames": [
                "table-compatibility",
                "unsupported-data-types",
                "large-objects",
                "memory-limitations",
            ]
        })),
        "StartReplicationTaskAssessment" => {
            task_action(st, &req_str(&body, "ReplicationTaskArn"), |task| {
                Ok(task.to_dict())
            })
        }
        "DescribeReplicationTaskAssessmentResults" => {
            ok(json!({ "ReplicationTaskAssessmentResults": [] }))
        }
        "StartReplicationTaskAssessmentRun" => start_assessment_run(st, &body, account, region),
        "CancelReplicationTaskAssessmentRun" => assessment_run_stub(
            &req_str(&body, "ReplicationTaskAssessmentRunArn"),
            "cancelling",
        ),
        "DeleteReplicationTaskAssessmentRun" => assessment_run_stub(
            &req_str(&body, "ReplicationTaskAssessmentRunArn"),
            "deleting",
        ),
        "DescribeReplicationTaskAssessmentRuns" => {
            ok(json!({ "ReplicationTaskAssessmentRuns": [] }))
        }
        "DescribeReplicationTaskIndividualAssessments" => {
            ok(json!({ "ReplicationTaskIndividualAssessments": [] }))
        }
        "DescribePendingMaintenanceActions" => ok(json!({ "PendingMaintenanceActions": [] })),
        "ApplyPendingMaintenanceAction" => {
            let arn = req_str(&body, "ReplicationInstanceArn");
            match st.replication_instances.contains_key(&arn) {
                true => ok(json!({
                    "ResourcePendingMaintenanceActions": {
                        "ResourceIdentifier": arn,
                        "PendingMaintenanceActionDetails": [{
                            "Action": req_str(&body, "ApplyAction"),
                            "OptInStatus": req_str(&body, "OptInType"),
                        }],
                    }
                })),
                false => not_found("Replication instance could not be found."),
            }
        }
        // moto emits `""` for the optional date fields, which the SDK-side
        // timestamp parser rejects (the probe recorded the resulting
        // ValueError). `null` keeps the members with the same "not yet"
        // semantics while staying SDK-parseable — the datapipeline-wave
        // precedent of modeled output over moto's unparseable wire shape.
        "DescribeEngineVersions" => ok(json!({
            "EngineVersions": [
                {
                    "Version": "3.5.2",
                    "Lifecycle": "GA",
                    "ReleaseStatus": "GA",
                    "LaunchDate": "2023-01-01T00:00:00Z",
                    "AutoUpgradeDate": Value::Null,
                    "DeprecationDate": Value::Null,
                    "AvailableUpgrades": [],
                },
                {
                    "Version": "3.4.7",
                    "Lifecycle": "GA",
                    "ReleaseStatus": "GA",
                    "LaunchDate": "2022-01-01T00:00:00Z",
                    "AutoUpgradeDate": Value::Null,
                    "DeprecationDate": Value::Null,
                    "AvailableUpgrades": ["3.5.2"],
                },
            ]
        })),
        "UpdateSubscriptionsToEventBridge" => {
            ok(json!({ "Result": "Subscriptions were updated successfully." }))
        }
        // --- fleet advisor ---------------------------------------------------------------
        "CreateFleetAdvisorCollector" => ok(json!({
            "CollectorReferencedId": moto_id(24),
            "CollectorName": body.get("CollectorName").cloned().unwrap_or(Value::Null),
            "Description": opt_str(&body, "Description").unwrap_or_default(),
            "ServiceAccessRoleArn": opt_str(&body, "ServiceAccessRoleArn").unwrap_or_default(),
            "S3BucketName": opt_str(&body, "S3BucketName").unwrap_or_default(),
        })),
        "DeleteFleetAdvisorCollector" => ok_empty(),
        "DeleteFleetAdvisorDatabases" => {
            ok(json!({ "DatabaseIds": str_list(body.get("DatabaseIds")) }))
        }
        "DescribeFleetAdvisorCollectors" => ok(json!({ "Collectors": [] })),
        "DescribeFleetAdvisorDatabases" => ok(json!({ "Databases": [] })),
        "DescribeFleetAdvisorLsaAnalysis" => ok(json!({ "Analysis": [] })),
        "DescribeFleetAdvisorSchemaObjectSummary" => ok(json!({ "FleetAdvisorSchemaObjects": [] })),
        "DescribeFleetAdvisorSchemas" => ok(json!({ "FleetAdvisorSchemas": [] })),
        "RunFleetAdvisorLsaAnalysis" => {
            ok(json!({ "LsaAnalysisId": moto_id(20), "Status": "COMPLETED" }))
        }
        // --- recommendations ---------------------------------------------------------------
        "StartRecommendations" => ok_empty(),
        "BatchStartRecommendations" => ok(json!({ "ErrorEntries": [] })),
        "DescribeRecommendations" => ok(json!({ "Recommendations": [] })),
        "DescribeRecommendationLimitations" => ok(json!({ "Limitations": [] })),
        // --- metadata model / conversion -------------------------------------------------
        "DescribeConversionConfiguration" => ok(json!({
            "MigrationProjectIdentifier": req_str(&body, "MigrationProjectIdentifier"),
            "ConversionConfiguration": "{}",
        })),
        "ModifyConversionConfiguration" => ok(json!({
            "MigrationProjectIdentifier": req_str(&body, "MigrationProjectIdentifier"),
        })),
        "StartMetadataModelConversion"
        | "StartMetadataModelAssessment"
        | "StartMetadataModelExportAsScript"
        | "StartMetadataModelExportToTarget"
        | "StartMetadataModelImport"
        | "StartMetadataModelCreation"
        | "CancelMetadataModelConversion"
        | "CancelMetadataModelCreation"
        | "StartExtensionPackAssociation" => ok(json!({ "RequestIdentifier": moto_id(20) })),
        "DescribeMetadataModelConversions"
        | "DescribeMetadataModelExportsAsScript"
        | "DescribeMetadataModelExportsToTarget"
        | "DescribeMetadataModelImports"
        | "DescribeMetadataModelAssessments"
        | "DescribeMetadataModelCreations"
        | "DescribeMetadataModel"
        | "DescribeMetadataModelChildren"
        | "DescribeExtensionPackAssociations" => ok(json!({ "Requests": [] })),
        "ExportMetadataModelAssessment" => ok(json!({
            "PdfReport": { "S3ObjectKey": "", "ContentLength": 0 },
            "CsvReport": { "S3ObjectKey": "", "ContentLength": 0 },
        })),
        "GetTargetSelectionRules" => ok(json!({ "TargetSelectionRules": "{}" })),
        other => json_error(
            501,
            "NotImplemented",
            &format!("The {other} action has not been implemented"),
        ),
    }
}

// ---------------------------------------------------------------------------
// Operation helpers — tagging
// ---------------------------------------------------------------------------

fn add_tags(st: &mut DmsStore, body: &Value) -> Response {
    let arn = req_str(body, "ResourceArn");
    tag_resource(st, &arn, body.get("Tags"));
    ok_empty()
}

fn list_tags(st: &mut DmsStore, body: &Value) -> Response {
    let arn_truthy = truthy(body.get("ResourceArn"));
    let list_truthy = truthy(body.get("ResourceArnList"));
    if arn_truthy && list_truthy {
        return internal_error("Both ResourceArn and ResourceArnList cannot be specified.".into());
    }
    if !arn_truthy && !list_truthy {
        return internal_error(
            "Either ResourceArn or ResourceArnList should be specified.".to_string(),
        );
    }
    if arn_truthy {
        let arn = req_str(body, "ResourceArn");
        ok(json!({ "TagList": tag_list(st, std::slice::from_ref(&arn)) }))
    } else {
        let arns = str_list(body.get("ResourceArnList"));
        ok(json!({ "TagList": tag_list(st, &arns) }))
    }
}

// ---------------------------------------------------------------------------
// Operation helpers — replication tasks
// ---------------------------------------------------------------------------

/// Run a task mutation against an existing task, 404 otherwise.
fn task_action(
    st: &mut DmsStore,
    arn: &str,
    f: impl FnOnce(&mut ReplicationTask) -> Result<Value, Response>,
) -> Response {
    match st.replication_tasks.get_mut(arn) {
        Some(task) => match f(task) {
            Ok(dict) => ok(json!({ "ReplicationTask": dict })),
            Err(resp) => resp,
        },
        None => not_found("Replication task could not be found."),
    }
}

/// moto's `create_replication_task`.
fn create_replication_task(
    st: &mut DmsStore,
    account: u64,
    region: &str,
    body: &Value,
) -> Response {
    let id = req_str(body, "ReplicationTaskIdentifier");
    let arn = dms_arn(region, account, "task", &id);
    if st.replication_tasks.contains_key(&arn) {
        return already_exists("The resource you are attempting to create already exists.");
    }
    let task = ReplicationTask {
        id,
        migration_type: req_str(body, "MigrationType"),
        replication_instance_arn: req_str(body, "ReplicationInstanceArn"),
        source_endpoint_arn: req_str(body, "SourceEndpointArn"),
        target_endpoint_arn: req_str(body, "TargetEndpointArn"),
        table_mappings: opt_str(body, "TableMappings"),
        replication_task_settings: opt_str(body, "ReplicationTaskSettings"),
        arn: arn.clone(),
        status: "creating".to_string(),
        creation_date: now_iso(),
        start_date: None,
        stop_date: None,
    };
    let dict = task.to_dict();
    st.replication_tasks.insert(arn.clone(), task);
    // moto: `if tags:` — an absent/empty list never reaches the tagger.
    if truthy(body.get("Tags")) {
        tag_resource(st, &arn, body.get("Tags"));
    }
    ok(json!({ "ReplicationTask": dict }))
}

/// moto's `filter_tasks` — the supported filter names, unknown ones skipped.
fn filtered_tasks<'a>(
    tasks: impl Iterator<Item = &'a ReplicationTask>,
    filters: &[Value],
) -> Vec<&'a ReplicationTask> {
    let mut selected: Vec<&ReplicationTask> = tasks.collect();
    for filter in filters {
        let values = filter_values(filter);
        match name_of(filter).as_str() {
            "replication-task-arn" => {
                selected.retain(|t| values.contains(&t.arn));
            }
            "replication-task-id" => {
                selected.retain(|t| values.contains(&t.id));
            }
            "migration-type" => {
                selected.retain(|t| values.contains(&t.migration_type));
            }
            "endpoint-arn" => {
                selected.retain(|t| {
                    values.contains(&t.source_endpoint_arn)
                        || values.contains(&t.target_endpoint_arn)
                });
            }
            "replication-instance-arn" => {
                selected.retain(|t| values.contains(&t.replication_instance_arn));
            }
            _ => {}
        }
    }
    selected
}

fn describe_replication_tasks(st: &DmsStore, body: &Value) -> Response {
    let filters = str_list_v(body.get("Filters"));
    let mut tasks = filtered_tasks(st.replication_tasks.values(), &filters);
    if let Some(max_records) = opt_i64(body, "MaxRecords") {
        if max_records > 0 {
            tasks.truncate(max_records as usize);
        }
    }
    let dicts: Vec<Value> = tasks.into_iter().map(|t| t.to_dict()).collect();
    ok(json!({ "ReplicationTasks": dicts }))
}

/// Filters as a raw object list (moto passes `Filters` straight through).
fn str_list_v(value: Option<&Value>) -> Vec<Value> {
    value
        .and_then(Value::as_array)
        .map(|l| l.to_vec())
        .unwrap_or_default()
}

fn modify_replication_task(st: &mut DmsStore, body: &Value) -> Response {
    let arn = req_str(body, "ReplicationTaskArn");
    task_action(st, &arn, |task| {
        if let Some(v) = opt_str(body, "ReplicationTaskIdentifier") {
            task.id = v;
        }
        if let Some(v) = opt_str(body, "MigrationType") {
            task.migration_type = v;
        }
        if let Some(v) = opt_str(body, "TableMappings") {
            task.table_mappings = Some(v);
        }
        if let Some(v) = opt_str(body, "ReplicationTaskSettings") {
            task.replication_task_settings = Some(v);
        }
        Ok(task.to_dict())
    })
}

// ---------------------------------------------------------------------------
// Operation helpers — replication instances
// ---------------------------------------------------------------------------

fn create_replication_instance(
    st: &mut DmsStore,
    account: u64,
    region: &str,
    body: &Value,
) -> Response {
    let instance = ReplicationInstance::new(body, region, account);
    if st.replication_instances.contains_key(&instance.arn) {
        return already_exists("The resource you are attempting to create already exists.");
    }
    let dict = instance.to_dict();
    let arn = instance.arn.clone();
    st.replication_instances.insert(arn.clone(), instance);
    if truthy(body.get("Tags")) {
        tag_resource(st, &arn, body.get("Tags"));
    }
    ok(json!({ "ReplicationInstance": dict }))
}

fn describe_replication_instances(st: &DmsStore, body: &Value) -> Response {
    let filters = str_list_v(body.get("Filters"));
    let mut list: Vec<&ReplicationInstance> = st.replication_instances.values().collect();
    for filter in &filters {
        let values = filter_values(filter);
        match name_of(filter).as_str() {
            "replication-instance-id" => {
                list.retain(|i| values.contains(&i.id));
            }
            "replication-instance-arn" => {
                list.retain(|i| values.contains(&i.arn));
            }
            "replication-instance-class" => {
                list.retain(|i| values.contains(&i.class));
            }
            "engine-version" => {
                list.retain(|i| {
                    i.engine_version
                        .as_deref()
                        .is_some_and(|v| values.iter().any(|x| x == v))
                });
            }
            _ => {}
        }
    }
    let dicts: Vec<Value> = list.into_iter().map(|i| i.to_dict()).collect();
    ok(json!({ "ReplicationInstances": dicts }))
}

fn delete_replication_instance(st: &mut DmsStore, arn: &str) -> Response {
    match st.replication_instances.remove(arn) {
        Some(mut instance) => {
            instance.status = "deleting".to_string();
            let dict = instance.to_dict();
            st.connections.retain(|c| c.replication_instance_arn != arn);
            ok(json!({ "ReplicationInstance": dict }))
        }
        None => not_found("Replication instance could not be found."),
    }
}

fn modify_replication_instance(st: &mut DmsStore, body: &Value) -> Response {
    let arn = req_str(body, "ReplicationInstanceArn");
    match st.replication_instances.get_mut(&arn) {
        Some(instance) => {
            let storage = opt_i64(body, "AllocatedStorage");
            if let Some(storage) = storage {
                instance.allocated_storage = storage;
            }
            if let Some(class) = opt_str(body, "ReplicationInstanceClass") {
                instance.class = class;
            }
            if let Some(sg_ids) = body.get("VpcSecurityGroupIds").filter(|v| !v.is_null()) {
                let ids: Vec<String> = sg_ids
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .filter_map(Value::as_str)
                            .map(String::from)
                            .collect()
                    })
                    .unwrap_or_default();
                instance.vpc_security_groups = ids
                    .iter()
                    .map(|sg| json!({ "VpcSecurityGroupId": sg, "Status": "active" }))
                    .collect();
            }
            if let Some(window) = opt_str(body, "PreferredMaintenanceWindow") {
                instance.preferred_maintenance_window = Some(window);
            }
            if let Some(multi_az) = opt_bool(body, "MultiAZ") {
                instance.multi_az = Some(multi_az);
            }
            if let Some(version) = opt_str(body, "EngineVersion") {
                instance.engine_version = Some(version);
            }
            if let Some(upgrade) = opt_bool(body, "AutoMinorVersionUpgrade") {
                instance.auto_minor_version_upgrade = Some(upgrade);
            }
            if let Some(id) = opt_str(body, "ReplicationInstanceIdentifier") {
                instance.id = id;
            }
            if let Some(network) = opt_str(body, "NetworkType") {
                instance.network_type = Some(network);
            }
            let dict = instance.to_dict();
            ok(json!({ "ReplicationInstance": dict }))
        }
        None => not_found("Replication instance could not be found."),
    }
}

// ---------------------------------------------------------------------------
// Operation helpers — endpoints
// ---------------------------------------------------------------------------

fn create_endpoint(st: &mut DmsStore, account: u64, region: &str, body: &Value) -> Response {
    let endpoint_type = req_str(body, "EndpointType");
    if endpoint_type != "source" && endpoint_type != "target" {
        return validation_error("Invalid endpoint type");
    }
    let endpoint = Endpoint::new(
        &req_str(body, "EndpointIdentifier"),
        &endpoint_type,
        &req_str(body, "EngineName"),
        opt_str(body, "Username"),
        opt_str(body, "Password"),
        opt_str(body, "ServerName"),
        opt_i64(body, "Port"),
        opt_str(body, "DatabaseName"),
        opt_str(body, "ExtraConnectionAttributes"),
        opt_str(body, "KmsKeyId"),
        opt_str(body, "CertificateArn"),
        opt_str(body, "SslMode"),
        opt_str(body, "ServiceAccessRoleArn"),
        opt_str(body, "ExternalTableDefinition"),
        opt_str(body, "ResourceIdentifier").as_deref(),
        body,
        region,
        account,
    );
    if truthy(body.get("Tags")) {
        tag_resource(st, &endpoint.endpoint_arn, body.get("Tags"));
    }
    let dict = drop_nulls(endpoint.to_dict());
    st.endpoints.insert(endpoint.identifier.clone(), endpoint);
    ok(json!({ "Endpoint": dict }))
}

/// Apply endpoint describe filters; unknown names raise moto's `ValueError`.
fn filter_endpoint_list<'a>(
    endpoints: impl Iterator<Item = &'a Endpoint>,
    filters: &[Value],
) -> Result<Vec<&'a Endpoint>, Response> {
    let mut selected: Vec<&Endpoint> = endpoints.collect();
    for filter in filters {
        let name = name_of(filter);
        let values = filter_values(filter);
        match name.as_str() {
            "endpoint-arn" => selected.retain(|e| values.contains(&e.endpoint_arn)),
            "endpoint-type" => selected.retain(|e| values.contains(&e.endpoint_type)),
            "endpoint-id" => selected.retain(|e| values.contains(&e.identifier)),
            "engine-name" => selected.retain(|e| values.contains(&e.engine_name)),
            _ => return Err(internal_error(format!("Invalid filter name: {name}"))),
        }
    }
    Ok(selected)
}

fn describe_endpoints(st: &DmsStore, body: &Value) -> Response {
    let filters = str_list_v(body.get("Filters"));
    let selected = filter_endpoint_list(st.endpoints.values(), &filters);
    match selected {
        Ok(list) => {
            let dicts: Vec<Value> = list.into_iter().map(|e| drop_nulls(e.to_dict())).collect();
            ok(json!({ "Endpoints": dicts }))
        }
        Err(resp) => resp,
    }
}

fn delete_endpoint(st: &mut DmsStore, endpoint_arn: &str) -> Response {
    let identifier = st
        .endpoints
        .values()
        .find(|e| e.endpoint_arn == endpoint_arn)
        .map(|e| e.identifier.clone());
    let Some(id) = identifier else {
        return not_found("Endpoint could not be found.");
    };
    let Some(mut endpoint) = st.endpoints.remove(&id) else {
        return not_found("Endpoint could not be found.");
    };
    endpoint.status = "deleting".to_string();
    let dict = endpoint.to_dict();
    st.connections.retain(|c| c.endpoint_arn != endpoint_arn);
    // moto returns the dict unfiltered here — nulls stay on the wire.
    ok(json!({ "Endpoint": dict }))
}

fn modify_endpoint(st: &mut DmsStore, body: &Value) -> Response {
    let arn = req_str(body, "EndpointArn");
    let id = st
        .endpoints
        .values()
        .find(|e| e.endpoint_arn == arn)
        .map(|e| e.identifier.clone());
    let Some(old_id) = id else {
        return not_found("Endpoint could not be found.");
    };
    let Some(mut endpoint) = st.endpoints.remove(&old_id) else {
        return not_found("Endpoint could not be found.");
    };
    if let Some(v) = opt_str(body, "EndpointIdentifier") {
        endpoint.identifier = v;
    }
    if let Some(v) = opt_str(body, "EndpointType") {
        endpoint.endpoint_type = v;
    }
    if let Some(v) = opt_str(body, "EngineName") {
        endpoint.engine_name = v.clone();
        endpoint.engine_display_name = v;
    }
    if let Some(v) = opt_str(body, "Username") {
        endpoint.username = Some(v);
    }
    if let Some(v) = opt_str(body, "Password") {
        endpoint.password = Some(v);
    }
    if let Some(v) = opt_str(body, "ServerName") {
        endpoint.server_name = Some(v);
    }
    if let Some(v) = opt_i64(body, "Port") {
        endpoint.port = Some(v);
    }
    if let Some(v) = opt_str(body, "DatabaseName") {
        endpoint.database_name = Some(v);
    }
    if let Some(v) = opt_str(body, "ExtraConnectionAttributes") {
        endpoint.extra_connection_attributes = Some(v);
    }
    if let Some(v) = opt_str(body, "CertificateArn") {
        endpoint.certificate_arn = Some(v);
    }
    if let Some(v) = opt_str(body, "SslMode") {
        endpoint.ssl_mode = Some(v);
    }
    if let Some(v) = opt_str(body, "ServiceAccessRoleArn") {
        endpoint.service_access_role_arn = Some(v);
    }
    if let Some(v) = opt_str(body, "ExternalTableDefinition") {
        endpoint.external_table_definition = Some(v);
    }
    for &key in ENDPOINT_SETTING_KEYS {
        if let Some(v) = body.get(key).filter(|v| !v.is_null()) {
            endpoint.settings.insert(key, Some(v.clone()));
        }
    }
    // Update the map key when the identifier changed.
    st.endpoints.insert(endpoint.identifier.clone(), endpoint);
    let identifier = opt_str(body, "EndpointIdentifier");
    let current_id = match identifier {
        Some(new_id) if new_id != old_id => new_id,
        _ => old_id,
    };
    let dict = st
        .endpoints
        .get(&current_id)
        .map(|e| drop_nulls(e.to_dict()))
        .unwrap_or(Value::Object(Map::new()));
    ok(json!({ "Endpoint": dict }))
}

// ---------------------------------------------------------------------------
// Operation helpers — replication subnet groups
// ---------------------------------------------------------------------------

fn create_subnet_group(st: &mut DmsStore, body: &Value) -> Response {
    let identifier = req_str(body, "ReplicationSubnetGroupIdentifier");
    if st.replication_subnet_groups.contains_key(&identifier) {
        return already_exists("The resource you are attempting to create already exists.");
    }
    let group = ReplicationSubnetGroup {
        identifier: identifier.clone(),
        description: req_str(body, "ReplicationSubnetGroupDescription"),
        subnet_ids: Some(str_list(body.get("SubnetIds"))),
    };
    // moto keys subnet-group tags by the identifier (not an ARN).
    if truthy(body.get("Tags")) {
        tag_resource(st, &identifier, body.get("Tags"));
    }
    let dict = group.to_dict();
    st.replication_subnet_groups.insert(identifier, group);
    ok(json!({ "ReplicationSubnetGroup": dict }))
}

fn describe_subnet_groups(st: &DmsStore, body: &Value) -> Response {
    for filter in str_list_v(body.get("Filters")) {
        if name_of(&filter) != "replication-subnet-group-id" {
            return internal_error(format!("Invalid filter name: {}", name_of(&filter)));
        }
    }
    let mut selected: Vec<&ReplicationSubnetGroup> =
        st.replication_subnet_groups.values().collect();
    for filter in str_list_v(body.get("Filters")) {
        let values = filter_values(&filter);
        selected.retain(|g| values.contains(&g.identifier));
    }
    let dicts: Vec<Value> = selected
        .into_iter()
        .map(|g| drop_nulls(g.to_dict()))
        .collect();
    ok(json!({ "ReplicationSubnetGroups": dicts }))
}

fn modify_subnet_group(st: &mut DmsStore, body: &Value) -> Response {
    let identifier = req_str(body, "ReplicationSubnetGroupIdentifier");
    match st.replication_subnet_groups.get_mut(&identifier) {
        Some(group) => {
            if let Some(description) = opt_str(body, "ReplicationSubnetGroupDescription") {
                group.description = description;
            }
            if let Some(subnet_ids) = body.get("SubnetIds").filter(|v| !v.is_null()) {
                group.subnet_ids = Some(str_list(Some(subnet_ids)));
            }
            ok(json!({ "ReplicationSubnetGroup": group.to_dict() }))
        }
        None => not_found("Replication subnet group could not be found."),
    }
}

// ---------------------------------------------------------------------------
// Operation helpers — connections
// ---------------------------------------------------------------------------

fn test_connection(st: &mut DmsStore, body: &Value) -> Response {
    let ri_arn = req_str(body, "ReplicationInstanceArn");
    let ep_arn = req_str(body, "EndpointArn");
    let Some(instance) = st.replication_instances.get(&ri_arn) else {
        return not_found("Replication instance could not be found.");
    };
    let instance_id = instance.id.clone();
    let Some(endpoint) = st.endpoints.values().find(|e| e.endpoint_arn == ep_arn) else {
        return not_found("Endpoint could not be found.");
    };
    let endpoint_identifier = endpoint.identifier.clone();
    let existing = st
        .connections
        .iter()
        .position(|c| c.replication_instance_arn == ri_arn && c.endpoint_arn == ep_arn);
    let position = existing.unwrap_or_else(|| {
        st.connections.push(Connection {
            replication_instance_arn: ri_arn.clone(),
            endpoint_arn: ep_arn.clone(),
            replication_instance_identifier: instance_id,
            endpoint_identifier,
            // moto's ManagedState reads `successful` immediately.
            status: "successful".to_string(),
        });
        st.connections.len() - 1
    });
    let connection = &st.connections[position];
    ok(json!({ "Connection": connection.to_dict() }))
}

fn describe_connections(st: &DmsStore, body: &Value) -> Response {
    let mut selected: Vec<&Connection> = st.connections.iter().collect();
    for filter in str_list_v(body.get("Filters")) {
        let name = name_of(&filter);
        let values = filter_values(&filter);
        match name.as_str() {
            "endpoint-arn" => selected.retain(|c| values.contains(&c.endpoint_arn)),
            "replication-instance-arn" => {
                selected.retain(|c| values.contains(&c.replication_instance_arn));
            }
            _ => {
                return internal_error(format!("Invalid filter name: {name}"));
            }
        }
    }
    let dicts: Vec<Value> = selected.into_iter().map(|c| c.to_dict()).collect();
    ok(json!({ "Connections": dicts }))
}

fn delete_connection(st: &mut DmsStore, body: &Value) -> Response {
    let ep_arn = req_str(body, "EndpointArn");
    let ri_arn = req_str(body, "ReplicationInstanceArn");
    let position = st
        .connections
        .iter()
        .position(|c| c.endpoint_arn == ep_arn && c.replication_instance_arn == ri_arn);
    match position {
        Some(index) => {
            let connection = st.connections.remove(index);
            ok(json!({ "Connection": connection.to_dict() }))
        }
        None => not_found("Connection could not be found."),
    }
}

// ---------------------------------------------------------------------------
// Operation helpers — event subscriptions
// ---------------------------------------------------------------------------

fn create_event_subscription(
    st: &mut DmsStore,
    account: u64,
    region: &str,
    body: &Value,
) -> Response {
    let name = req_str(body, "SubscriptionName");
    if st.event_subscriptions.contains_key(&name) {
        return already_exists("The resource you are attempting to create already exists.");
    }
    let sub = EventSubscription {
        name: name.clone(),
        sns_topic_arn: opt_str(body, "SnsTopicArn"),
        source_type: opt_str(body, "SourceType").unwrap_or_default(),
        event_categories: str_list(body.get("EventCategories")),
        source_ids: str_list(body.get("SourceIds")),
        // moto: `enabled if enabled is not None else True`.
        enabled: opt_bool(body, "Enabled").unwrap_or(true),
        arn: dms_arn(region, account, "es", &name),
        creation_time: now_iso(),
        customer_aws_id: account.to_string(),
    };
    let dict = sub.to_dict();
    if truthy(body.get("Tags")) {
        tag_resource(st, &sub.arn, body.get("Tags"));
    }
    st.event_subscriptions.insert(name, sub);
    ok(json!({ "EventSubscription": dict }))
}

fn describe_certificates(st: &DmsStore, body: &Value) -> Response {
    let mut certs: Vec<&Certificate> = st.certificates.values().collect();
    for filter in str_list_v(body.get("Filters")) {
        let values = filter_values(&filter);
        match name_of(&filter).as_str() {
            "certificate-id" => {
                certs.retain(|c| values.contains(&c.identifier));
            }
            "certificate-arn" => {
                certs.retain(|c| values.contains(&c.arn));
            }
            _ => {}
        }
    }
    let dicts: Vec<Value> = certs.into_iter().map(|c| c.to_dict()).collect();
    ok(json!({ "Certificates": dicts }))
}

fn import_certificate(st: &mut DmsStore, account: u64, region: &str, body: &Value) -> Response {
    let identifier = req_str(body, "CertificateIdentifier");
    if st.certificates.contains_key(&identifier) {
        return already_exists("The resource you are attempting to create already exists.");
    }
    let cert = Certificate {
        identifier: identifier.clone(),
        pem: opt_str(body, "CertificatePem"),
        wallet: opt_str(body, "CertificateWallet"),
        creation_date: now_iso(),
        arn: dms_arn(region, account, "cert", &moto_id(24)),
    };
    let dict = cert.to_dict();
    if truthy(body.get("Tags")) {
        tag_resource(st, &cert.arn, body.get("Tags"));
    }
    st.certificates.insert(identifier, cert);
    ok(json!({ "Certificate": dict }))
}

fn modify_event_subscription(st: &mut DmsStore, body: &Value) -> Response {
    let name = req_str(body, "SubscriptionName");
    match st.event_subscriptions.get_mut(&name) {
        Some(sub) => {
            if let Some(arn) = opt_str(body, "SnsTopicArn") {
                sub.sns_topic_arn = Some(arn);
            }
            if let Some(source_type) = opt_str(body, "SourceType") {
                sub.source_type = source_type;
            }
            if let Some(categories) = body.get("EventCategories").filter(|v| !v.is_null()) {
                sub.event_categories = str_list(Some(categories));
            }
            if let Some(enabled) = opt_bool(body, "Enabled") {
                sub.enabled = enabled;
            }
            ok(json!({ "EventSubscription": sub.to_dict() }))
        }
        None => not_found("Subscription could not be found."),
    }
}

// ---------------------------------------------------------------------------
// Operation helpers — replication configs (serverless)
// ---------------------------------------------------------------------------

fn create_replication_config(
    st: &mut DmsStore,
    account: u64,
    region: &str,
    body: &Value,
) -> Response {
    let identifier = req_str(body, "ReplicationConfigIdentifier");
    if st.replication_configs.contains_key(&identifier) {
        return already_exists("The resource you are attempting to create already exists.");
    }
    let rid = opt_str(body, "ResourceIdentifier").unwrap_or_else(|| moto_id(24));
    let config = ReplicationConfig {
        identifier: identifier.clone(),
        source_endpoint_arn: req_str(body, "SourceEndpointArn"),
        target_endpoint_arn: req_str(body, "TargetEndpointArn"),
        compute_config: body.get("ComputeConfig").cloned().unwrap_or(json!({})),
        replication_type: req_str(body, "ReplicationType"),
        table_mappings: opt_str(body, "TableMappings"),
        replication_settings: opt_str(body, "ReplicationSettings"),
        supplemental_settings: opt_str(body, "SupplementalSettings"),
        status: "created".to_string(),
        arn: dms_arn(region, account, "replication-config", &rid),
        creation_date: now_iso(),
    };
    let dict = config.to_dict();
    if truthy(body.get("Tags")) {
        tag_resource(st, &config.arn, body.get("Tags"));
    }
    st.replication_configs.insert(identifier, config);
    ok(json!({ "ReplicationConfig": dict }))
}

fn describe_replication_configs(st: &DmsStore, body: &Value) -> Response {
    let mut configs: Vec<&ReplicationConfig> = st.replication_configs.values().collect();
    for filter in str_list_v(body.get("Filters")) {
        let values = filter_values(&filter);
        match name_of(&filter).as_str() {
            "replication-config-arn" => {
                configs.retain(|c| values.contains(&c.arn));
            }
            "replication-config-id" => {
                configs.retain(|c| values.contains(&c.identifier));
            }
            _ => {}
        }
    }
    let dicts: Vec<Value> = configs.into_iter().map(|c| c.to_dict()).collect();
    ok(json!({ "ReplicationConfigs": dicts }))
}

fn delete_replication_config(st: &mut DmsStore, arn: &str) -> Response {
    let identifier = st
        .replication_configs
        .values()
        .find(|c| c.arn == arn)
        .map(|c| c.identifier.clone());
    match identifier {
        Some(id) => {
            st.replication_configs.remove(&id);
            ok_empty()
        }
        None => not_found("Replication config could not be found."),
    }
}

fn modify_replication_config(st: &mut DmsStore, body: &Value) -> Response {
    let arn = req_str(body, "ReplicationConfigArn");
    let id = st
        .replication_configs
        .values()
        .find(|c| c.arn == arn)
        .map(|c| c.identifier.clone());
    let Some(old_id) = id else {
        return not_found("Replication config could not be found.");
    };
    let Some(mut config) = st.replication_configs.remove(&old_id) else {
        return not_found("Replication config could not be found.");
    };
    if let Some(v) = opt_str(body, "ReplicationConfigIdentifier") {
        config.identifier = v;
    }
    if let Some(v) = opt_str(body, "ReplicationType") {
        config.replication_type = v;
    }
    if let Some(v) = opt_str(body, "TableMappings") {
        config.table_mappings = Some(v);
    }
    if let Some(v) = opt_str(body, "ReplicationSettings") {
        config.replication_settings = Some(v);
    }
    if let Some(v) = opt_str(body, "SupplementalSettings") {
        config.supplemental_settings = Some(v);
    }
    if let Some(v) = body.get("ComputeConfig").filter(|v| !v.is_null()) {
        config.compute_config = v.clone();
    }
    if let Some(v) = opt_str(body, "SourceEndpointArn") {
        config.source_endpoint_arn = v;
    }
    if let Some(v) = opt_str(body, "TargetEndpointArn") {
        config.target_endpoint_arn = v;
    }
    // moto reindexes only on a truthy, changed identifier.
    let new_id = config.identifier.clone();
    let current_id = if truthy_str(&new_id) && new_id != old_id {
        new_id
    } else {
        old_id
    };
    st.replication_configs.insert(current_id, config);
    let arn = req_str(body, "ReplicationConfigArn");
    let dict = st
        .replication_configs
        .values()
        .find(|c| c.arn == arn)
        .map(|c| c.to_dict())
        .unwrap_or(Value::Object(Map::new()));
    ok(json!({ "ReplicationConfig": dict }))
}

fn truthy_str(s: &str) -> bool {
    !s.is_empty()
}

/// `StartReplication`/`StopReplication` — flip status, 404 on unknown ARN.
fn flip_replication(st: &mut DmsStore, arn: &str, status: &str) -> Response {
    let mut found = false;
    for config in st.replication_configs.values_mut() {
        if config.arn == arn {
            config.status = status.to_string();
            found = true;
            break;
        }
    }
    if !found {
        return not_found("Replication config could not be found.");
    }
    let dict = st
        .replication_configs
        .values()
        .find(|c| c.arn == arn)
        .map(|c| c.to_summary())
        .expect("checked above");
    ok(json!({ "Replication": dict }))
}

fn describe_replications(st: &DmsStore, body: &Value) -> Response {
    let mut configs: Vec<&ReplicationConfig> = st.replication_configs.values().collect();
    for filter in str_list_v(body.get("Filters")) {
        if name_of(&filter) == "replication-config-arn" {
            let values = filter_values(&filter);
            configs.retain(|c| values.contains(&c.arn));
        }
    }
    let dicts: Vec<Value> = configs
        .into_iter()
        .map(|c| {
            let mut summary = c.to_summary();
            if let Some(obj) = summary.as_object_mut() {
                obj.insert("StopReason".to_string(), json!(""));
                obj.insert("ReplicationStats".to_string(), json!({}));
            }
            summary
        })
        .collect();
    ok(json!({ "Replications": dicts }))
}

fn start_assessment_run(st: &mut DmsStore, body: &Value, account: u64, region: &str) -> Response {
    let arn = req_str(body, "ReplicationTaskArn");
    match st.replication_tasks.get(&arn) {
        Some(_) => {
            let run_arn = dms_arn(region, account, "assessment-run", &moto_id(17));
            ok(json!({
                "ReplicationTaskAssessmentRun": {
                    "ReplicationTaskAssessmentRunArn": run_arn,
                    "ReplicationTaskArn": arn,
                    "Status": "starting",
                    "ReplicationTaskAssessmentRunCreationDate": now_iso(),
                    "AssessmentProgress": {
                        "IndividualAssessmentCount": 0,
                        "IndividualAssessmentCompletedCount": 0,
                    },
                    "AssessmentRunName": req_str(body, "AssessmentRunName"),
                    "ServiceAccessRoleArn": req_str(body, "ServiceAccessRoleArn"),
                    "ResultLocationBucket": req_str(body, "ResultLocationBucket"),
                    "ResultLocationFolder": opt_str(body, "ResultLocationFolder").unwrap_or_default(),
                    "ResultEncryptionMode": opt_str(body, "ResultEncryptionMode")
                        .unwrap_or_else(|| "SSE_S3".to_string()),
                }
            }))
        }
        None => not_found("Replication task could not be found."),
    }
}

fn assessment_run_stub(arn: &str, status: &str) -> Response {
    ok(json!({
        "ReplicationTaskAssessmentRun": {
            "ReplicationTaskAssessmentRunArn": arn,
            "Status": status,
        }
    }))
}

// ---------------------------------------------------------------------------
// Operation helpers — data providers
// ---------------------------------------------------------------------------

fn create_data_provider(st: &mut DmsStore, account: u64, region: &str, body: &Value) -> Response {
    let name = req_str(body, "DataProviderName");
    if st.data_providers.contains_key(&name) {
        return already_exists("The resource you are attempting to create already exists.");
    }
    let dp = DataProvider {
        name: name.clone(),
        engine: opt_str(body, "Engine"),
        settings: body.get("Settings").cloned().unwrap_or(json!({})),
        description: opt_str(body, "Description").unwrap_or_default(),
        creation_date: now_iso(),
        arn: dms_arn(region, account, "data-provider", &moto_id(24)),
    };
    let dict = dp.to_dict();
    if truthy(body.get("Tags")) {
        tag_resource(st, &dp.arn, body.get("Tags"));
    }
    st.data_providers.insert(name, dp);
    ok(json!({ "DataProvider": dict }))
}

/// Remove a row matched head-name first, then by ARN; returns the key + row.
///
/// moto: `self.map.get(identifier)` then a full ARN scan for these
/// named resources. Requires the row to expose `.arn` and `.name`.
macro_rules! remove_by_name_or_arn {
    ($map:expr, $identifier:expr) => {
        if $map.contains_key($identifier) {
            $map.remove_entry($identifier)
        } else {
            let key = $map
                .values()
                .find(|v| v.arn == $identifier)
                .map(|v| v.name.clone());
            key.and_then(|k| $map.remove_entry(&k))
        }
    };
}

fn describe_data_providers(st: &DmsStore, body: &Value) -> Response {
    let mut providers: Vec<&DataProvider> = st.data_providers.values().collect();
    for filter in str_list_v(body.get("Filters")) {
        if name_of(&filter) == "data-provider-identifier" {
            let values = filter_values(&filter);
            providers.retain(|p| values.contains(&p.name) || values.contains(&p.arn));
        }
    }
    let dicts: Vec<Value> = providers.into_iter().map(|p| p.to_dict()).collect();
    ok(json!({ "DataProviders": dicts }))
}

fn find_data_provider(st: &mut DmsStore, identifier: &str) -> Option<(String, DataProvider)> {
    remove_by_name_or_arn!(st.data_providers, identifier)
}

fn modify_data_provider(st: &mut DmsStore, body: &Value) -> Response {
    let identifier = req_str(body, "DataProviderIdentifier");
    let Some((old_name, mut dp)) = find_data_provider(st, &identifier) else {
        return not_found("Data provider could not be found.");
    };
    if let Some(v) = opt_str(body, "DataProviderName") {
        dp.name = v;
    }
    if let Some(v) = opt_str(body, "Engine") {
        dp.engine = Some(v);
    }
    if let Some(v) = body.get("Settings").filter(|v| !v.is_null()) {
        dp.settings = v.clone();
    }
    if let Some(v) = opt_str(body, "Description") {
        dp.description = v;
    }
    // moto reindexes only on a truthy, changed name.
    let new_name = dp.name.clone();
    let key = if truthy_str(&new_name) && new_name != old_name {
        new_name
    } else {
        old_name
    };
    let dict = dp.to_dict();
    st.data_providers.insert(key, dp);
    ok(json!({ "DataProvider": dict }))
}

fn delete_data_provider(st: &mut DmsStore, identifier: &str) -> Response {
    match find_data_provider(st, identifier) {
        Some((_key, dp)) => ok(json!({ "DataProvider": dp.to_dict() })),
        None => not_found("Data provider could not be found."),
    }
}

// ---------------------------------------------------------------------------
// Operation helpers — instance profiles
// ---------------------------------------------------------------------------

fn create_instance_profile(
    st: &mut DmsStore,
    account: u64,
    region: &str,
    body: &Value,
) -> Response {
    let name = req_str(body, "InstanceProfileName");
    if st.instance_profiles.contains_key(&name) {
        return already_exists("The resource you are attempting to create already exists.");
    }
    let ip = InstanceProfile {
        name: name.clone(),
        availability_zone: opt_str(body, "AvailabilityZone"),
        kms_key_arn: opt_str(body, "KmsKeyArn"),
        // moto: `publicly_accessible if publicly_accessible is not None else False`
        publicly_accessible: opt_bool(body, "PubliclyAccessible").unwrap_or(false),
        network_type: opt_str(body, "NetworkType"),
        description: opt_str(body, "Description").unwrap_or_default(),
        subnet_group_identifier: opt_str(body, "SubnetGroupIdentifier"),
        vpc_security_groups: str_list(body.get("VpcSecurityGroups")),
        creation_date: now_iso(),
        arn: dms_arn(region, account, "instance-profile", &moto_id(24)),
    };
    let dict = ip.to_dict();
    if truthy(body.get("Tags")) {
        tag_resource(st, &ip.arn, body.get("Tags"));
    }
    st.instance_profiles.insert(name, ip);
    ok(json!({ "InstanceProfile": dict }))
}

fn describe_instance_profiles(st: &DmsStore, body: &Value) -> Response {
    let mut profiles: Vec<&InstanceProfile> = st.instance_profiles.values().collect();
    for filter in str_list_v(body.get("Filters")) {
        if name_of(&filter) == "instance-profile-identifier" {
            let values = filter_values(&filter);
            profiles.retain(|p| values.contains(&p.name) || values.contains(&p.arn));
        }
    }
    let dicts: Vec<Value> = profiles.into_iter().map(|p| p.to_dict()).collect();
    ok(json!({ "InstanceProfiles": dicts }))
}

fn find_instance_profile(st: &mut DmsStore, identifier: &str) -> Option<(String, InstanceProfile)> {
    remove_by_name_or_arn!(st.instance_profiles, identifier)
}

fn modify_instance_profile(st: &mut DmsStore, body: &Value) -> Response {
    let identifier = req_str(body, "InstanceProfileIdentifier");
    let Some((old_name, mut ip)) = find_instance_profile(st, &identifier) else {
        return not_found("Instance profile could not be found.");
    };
    if let Some(v) = opt_str(body, "InstanceProfileName") {
        ip.name = v;
    }
    if let Some(v) = opt_str(body, "AvailabilityZone") {
        ip.availability_zone = Some(v);
    }
    if let Some(v) = opt_str(body, "KmsKeyArn") {
        ip.kms_key_arn = Some(v);
    }
    if let Some(v) = opt_bool(body, "PubliclyAccessible") {
        ip.publicly_accessible = v;
    }
    if let Some(v) = opt_str(body, "NetworkType") {
        ip.network_type = Some(v);
    }
    if let Some(v) = opt_str(body, "Description") {
        ip.description = v;
    }
    if let Some(v) = opt_str(body, "SubnetGroupIdentifier") {
        ip.subnet_group_identifier = Some(v);
    }
    if let Some(v) = body.get("VpcSecurityGroups").filter(|v| !v.is_null()) {
        ip.vpc_security_groups = str_list(Some(v));
    }
    let new_name = ip.name.clone();
    let key = if truthy_str(&new_name) && new_name != old_name {
        new_name
    } else {
        old_name
    };
    let dict = ip.to_dict();
    st.instance_profiles.insert(key, ip);
    ok(json!({ "InstanceProfile": dict }))
}

fn delete_instance_profile(st: &mut DmsStore, identifier: &str) -> Response {
    match find_instance_profile(st, identifier) {
        Some((_key, ip)) => ok(json!({ "InstanceProfile": ip.to_dict() })),
        None => not_found("Instance profile could not be found."),
    }
}

// ---------------------------------------------------------------------------
// Operation helpers — migration projects
// ---------------------------------------------------------------------------

fn create_migration_project(
    st: &mut DmsStore,
    account: u64,
    region: &str,
    body: &Value,
) -> Response {
    let name = req_str(body, "MigrationProjectName");
    if st.migration_projects.contains_key(&name) {
        return already_exists("The resource you are attempting to create already exists.");
    }
    let mp = MigrationProject {
        name: name.clone(),
        source_descriptors: str_list_v(body.get("SourceDataProviderDescriptors")),
        target_descriptors: str_list_v(body.get("TargetDataProviderDescriptors")),
        instance_profile_identifier: opt_str(body, "InstanceProfileIdentifier"),
        transformation_rules: opt_str(body, "TransformationRules"),
        description: opt_str(body, "Description").unwrap_or_default(),
        schema_conversion_attributes: body
            .get("SchemaConversionApplicationAttributes")
            .filter(|v| !v.is_null())
            .cloned(),
        creation_date: now_iso(),
        arn: dms_arn(region, account, "migration-project", &moto_id(24)),
    };
    let dict = mp.to_dict();
    if truthy(body.get("Tags")) {
        tag_resource(st, &mp.arn, body.get("Tags"));
    }
    st.migration_projects.insert(name, mp);
    ok(json!({ "MigrationProject": dict }))
}

fn describe_migration_projects(st: &DmsStore, body: &Value) -> Response {
    let mut projects: Vec<&MigrationProject> = st.migration_projects.values().collect();
    for filter in str_list_v(body.get("Filters")) {
        if name_of(&filter) == "migration-project-identifier" {
            let values = filter_values(&filter);
            projects.retain(|p| values.contains(&p.name) || values.contains(&p.arn));
        }
    }
    let dicts: Vec<Value> = projects.into_iter().map(|p| p.to_dict()).collect();
    ok(json!({ "MigrationProjects": dicts }))
}

fn find_migration_project(
    st: &mut DmsStore,
    identifier: &str,
) -> Option<(String, MigrationProject)> {
    remove_by_name_or_arn!(st.migration_projects, identifier)
}

fn modify_migration_project(st: &mut DmsStore, body: &Value) -> Response {
    let identifier = req_str(body, "MigrationProjectIdentifier");
    let Some((old_name, mut mp)) = find_migration_project(st, &identifier) else {
        return not_found("Migration project could not be found.");
    };
    if let Some(v) = opt_str(body, "MigrationProjectName") {
        mp.name = v;
    }
    if let Some(items) = body
        .get("SourceDataProviderDescriptors")
        .filter(|v| !v.is_null())
    {
        mp.source_descriptors = str_list_v(Some(items));
    }
    if let Some(items) = body
        .get("TargetDataProviderDescriptors")
        .filter(|v| !v.is_null())
    {
        mp.target_descriptors = str_list_v(Some(items));
    }
    if let Some(v) = opt_str(body, "InstanceProfileIdentifier") {
        mp.instance_profile_identifier = Some(v);
    }
    if let Some(v) = opt_str(body, "TransformationRules") {
        mp.transformation_rules = Some(v);
    }
    if let Some(v) = opt_str(body, "Description") {
        mp.description = v;
    }
    if let Some(v) = body
        .get("SchemaConversionApplicationAttributes")
        .filter(|v| !v.is_null())
    {
        mp.schema_conversion_attributes = Some(v.clone());
    }
    let new_name = mp.name.clone();
    let key = if truthy_str(&new_name) && new_name != old_name {
        new_name
    } else {
        old_name
    };
    let dict = mp.to_dict();
    st.migration_projects.insert(key, mp);
    ok(json!({ "MigrationProject": dict }))
}

fn delete_migration_project(st: &mut DmsStore, identifier: &str) -> Response {
    match find_migration_project(st, identifier) {
        Some((_key, mp)) => ok(json!({ "MigrationProject": mp.to_dict() })),
        None => not_found("Migration project could not be found."),
    }
}

// ---------------------------------------------------------------------------
// Operation helpers — data migrations
// ---------------------------------------------------------------------------

fn create_data_migration(st: &mut DmsStore, account: u64, region: &str, body: &Value) -> Response {
    // moto keys by name; an absent name (None) collides with the "None"
    // dict key — "" plays that role here.
    let name_key = opt_str(body, "DataMigrationName").unwrap_or_default();
    if st.data_migrations.contains_key(&name_key) {
        return already_exists("The resource you are attempting to create already exists.");
    }
    let dm = DataMigration {
        name: name_key.clone(),
        migration_project_identifier: req_str(body, "MigrationProjectIdentifier"),
        data_migration_type: req_str(body, "DataMigrationType"),
        service_access_role_arn: opt_str(body, "ServiceAccessRoleArn"),
        source_data_settings: str_list_v(body.get("SourceDataSettings")),
        // moto: `enable_cloudwatch_logs or False`.
        enable_cloudwatch_logs: opt_bool(body, "EnableCloudwatchLogs").unwrap_or(false),
        status: "created".to_string(),
        creation_date: now_iso(),
        arn: dms_arn(region, account, "data-migration", &moto_id(24)),
    };
    let dict = dm.to_dict();
    if truthy(body.get("Tags")) {
        tag_resource(st, &dm.arn, body.get("Tags"));
    }
    st.data_migrations.insert(name_key, dm);
    ok(json!({ "DataMigration": dict }))
}

fn describe_data_migrations(st: &DmsStore, body: &Value) -> Response {
    let mut migrations: Vec<&DataMigration> = st.data_migrations.values().collect();
    for filter in str_list_v(body.get("Filters")) {
        if name_of(&filter) == "data-migration-identifier" {
            let values = filter_values(&filter);
            migrations.retain(|m| values.contains(&m.name) || values.contains(&m.arn));
        }
    }
    let dicts: Vec<Value> = migrations.into_iter().map(|m| m.to_dict()).collect();
    ok(json!({ "DataMigrations": dicts }))
}

fn find_data_migration(st: &mut DmsStore, identifier: &str) -> Option<(String, DataMigration)> {
    remove_by_name_or_arn!(st.data_migrations, identifier)
}

fn modify_data_migration(st: &mut DmsStore, body: &Value) -> Response {
    let identifier = req_str(body, "DataMigrationIdentifier");
    let Some((old_name, mut dm)) = find_data_migration(st, &identifier) else {
        return not_found("Data migration could not be found.");
    };
    if let Some(v) = opt_str(body, "DataMigrationName") {
        dm.name = v;
    }
    if let Some(v) = opt_str(body, "DataMigrationType") {
        dm.data_migration_type = v;
    }
    if let Some(v) = opt_str(body, "ServiceAccessRoleArn") {
        dm.service_access_role_arn = Some(v);
    }
    if let Some(items) = body.get("SourceDataSettings").filter(|v| !v.is_null()) {
        dm.source_data_settings = str_list_v(Some(items));
    }
    if let Some(v) = opt_bool(body, "EnableCloudwatchLogs") {
        dm.enable_cloudwatch_logs = v;
    }
    let new_name = dm.name.clone();
    let key = if truthy_str(&new_name) && new_name != old_name {
        new_name
    } else {
        old_name
    };
    let dict = dm.to_dict();
    st.data_migrations.insert(key, dm);
    ok(json!({ "DataMigration": dict }))
}

fn delete_data_migration(st: &mut DmsStore, identifier: &str) -> Response {
    match find_data_migration(st, identifier) {
        Some((_key, dm)) => ok(json!({ "DataMigration": dm.to_dict() })),
        None => not_found("Data migration could not be found."),
    }
}

fn flip_data_migration(st: &mut DmsStore, identifier: &str, status: &str) -> Response {
    let Some((_key, mut dm)) = find_data_migration(st, identifier) else {
        return not_found("Data migration could not be found.");
    };
    dm.status = status.to_string();
    let dict = dm.to_dict();
    // moto reindexes by (possibly None) name; "" plays that key here.
    st.data_migrations.insert(dm.name.clone(), dm);
    ok(json!({ "DataMigration": dict }))
}

// ---------------------------------------------------------------------------
// Operation helpers — account attributes / static catalogs
// ---------------------------------------------------------------------------

fn describe_account_attributes(st: &DmsStore, account: u64) -> Response {
    let storage: i64 = st
        .replication_instances
        .values()
        .map(|i| i.allocated_storage)
        .sum();
    json_ok(json!({
        "AccountQuotas": [
            {
                "AccountQuotaName": "ReplicationInstances",
                "Used": st.replication_instances.len(),
                "Max": 60,
            },
            {
                "AccountQuotaName": "AllocatedStorage",
                "Used": storage,
                "Max": 100000,
            },
            {
                "AccountQuotaName": "Endpoints",
                "Used": st.endpoints.len(),
                "Max": 1000,
            },
            {
                "AccountQuotaName": "ReplicationSubnetGroups",
                "Used": st.replication_subnet_groups.len(),
                "Max": 60,
            },
            {
                "AccountQuotaName": "ReplicationTasks",
                "Used": st.replication_tasks.len(),
                "Max": 200,
            },
            {
                "AccountQuotaName": "EventSubscriptions",
                "Used": st.event_subscriptions.len(),
                "Max": 60,
            },
            {
                "AccountQuotaName": "Certificates",
                "Used": st.certificates.len(),
                "Max": 100,
            },
        ],
        "UniqueAccountIdentifier": account.to_string(),
    }))
}

fn json_ok(body: Value) -> Response {
    ok(body)
}

/// moto's static `describe_endpoint_types` catalog.
fn endpoint_types() -> Vec<Value> {
    const ENGINE_TYPES: &[(&str, &str, bool, bool)] = &[
        ("mysql", "MySQL", true, true),
        ("oracle", "Oracle", true, true),
        ("postgres", "PostgreSQL", true, true),
        ("mariadb", "MariaDB", true, true),
        ("aurora", "Amazon Aurora MySQL", true, true),
        ("aurora-postgresql", "Amazon Aurora PostgreSQL", true, true),
        ("redshift", "Amazon Redshift", false, true),
        ("s3", "Amazon S3", true, true),
        ("db2", "IBM Db2 LUW", true, true),
        ("azuredb", "Azure SQL Database", true, false),
        ("sybase", "SAP ASE", true, true),
        ("dynamodb", "Amazon DynamoDB", false, true),
        ("mongodb", "MongoDB", true, false),
        ("kinesis", "Amazon Kinesis", false, true),
        ("kafka", "Apache Kafka", false, true),
        ("elasticsearch", "Amazon OpenSearch Service", false, true),
        ("docdb", "Amazon DocumentDB", true, false),
        ("sqlserver", "Microsoft SQL Server", true, true),
        ("neptune", "Amazon Neptune", false, true),
    ];
    let mut result = Vec::new();
    for (name, display, supports_source, supports_target) in ENGINE_TYPES {
        if *supports_source {
            result.push(json!({
                "EngineName": name,
                "EngineDisplayName": display,
                "SupportsCDC": true,
                "EndpointType": "source",
                "ReplicationInstanceEngineMinimumVersion": "3.4.7",
            }));
        }
        if *supports_target {
            result.push(json!({
                "EngineName": name,
                "EngineDisplayName": display,
                "SupportsCDC": true,
                "EndpointType": "target",
                "ReplicationInstanceEngineMinimumVersion": "3.4.7",
            }));
        }
    }
    result
}

/// moto's static `describe_orderable_replication_instances` catalog.
fn orderable_instances(region: &str) -> Vec<Value> {
    [
        "dms.t2.micro",
        "dms.t2.small",
        "dms.t2.medium",
        "dms.t2.large",
        "dms.c4.large",
        "dms.c4.xlarge",
        "dms.c4.2xlarge",
        "dms.c4.4xlarge",
        "dms.r4.large",
        "dms.r4.xlarge",
        "dms.r4.2xlarge",
        "dms.r4.4xlarge",
        "dms.r4.8xlarge",
    ]
    .iter()
    .map(|class| {
        json!({
            "EngineVersion": "3.5.2",
            "ReplicationInstanceClass": class,
            "StorageType": "gp2",
            "MinAllocatedStorage": 5,
            "MaxAllocatedStorage": 6144,
            "DefaultAllocatedStorage": 50,
            "IncludedAllocatedStorage": 50,
            "AvailabilityZones": [
                format!("{region}a"),
                format!("{region}b"),
                format!("{region}c"),
            ],
            "ReleaseStatus": "GA",
        })
    })
    .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::atomic::{AtomicU64, Ordering};

    static ACCOUNTS: AtomicU64 = AtomicU64::new(200_000_000_001);

    /// Every test takes a fresh account so the shared store stays isolated.
    fn next_account() -> u64 {
        ACCOUNTS.fetch_add(1, Ordering::SeqCst)
    }

    fn send(account: u64, operation: &str, body: Value) -> (u16, Value) {
        let request = Request {
            service: "dms".to_string(),
            operation: operation.to_string(),
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).expect("serialize")),
            account,
            region: "us-east-1".to_string(),
        };
        let resp = dispatch(&request).expect("dms dispatch");
        (
            resp.status,
            serde_json::from_slice(&resp.body).unwrap_or(json!({})),
        )
    }

    fn create_endpoint(account: u64, id: &str, extra: Value) -> (String, Value) {
        let mut body = json!({
            "EndpointIdentifier": id,
            "EndpointType": "source",
            "EngineName": "mysql",
            "ServerName": "localhost",
            "Port": 3306,
            "Username": "admin",
            "Password": "password",
        });
        if let Some(extra) = extra.as_object() {
            for (k, v) in extra {
                body[k.as_str()] = v.clone();
            }
        }
        let (status, resp) = send(account, "CreateEndpoint", body);
        assert_eq!(status, 200, "create_endpoint: {resp}");
        (
            resp["Endpoint"]["EndpointArn"]
                .as_str()
                .expect("arn")
                .to_string(),
            resp,
        )
    }

    fn create_instance(account: u64, id: &str) -> String {
        let (status, resp) = send(
            account,
            "CreateReplicationInstance",
            json!({
                "ReplicationInstanceIdentifier": id,
                "ReplicationInstanceClass": "dms.t3.micro",
            }),
        );
        assert_eq!(status, 200, "create_replication_instance: {resp}");
        resp["ReplicationInstance"]["ReplicationInstanceArn"]
            .as_str()
            .expect("arn")
            .to_string()
    }

    #[test]
    fn dispatch_ignores_other_services() {
        let request = Request {
            service: "other".to_string(),
            operation: "CreateEndpoint".to_string(),
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: Bytes::new(),
            account: 123,
            region: "us-east-1".to_string(),
        };
        assert!(dispatch(&request).is_none());
    }

    #[test]
    fn unknown_operation_is_not_implemented() {
        let account = next_account();
        let (status, body) = send(account, "FrobnicateThings", json!({}));
        assert_eq!(status, 501);
        assert_eq!(body["__type"], "NotImplemented");
    }

    #[test]
    fn endpoint_crud_round_trip() {
        let account = next_account();
        let (arn, _) = create_endpoint(account, "ep-crud", json!({}));

        // DescribeEndpoints with a matching filter.
        let (status, resp) = send(
            account,
            "DescribeEndpoints",
            json!({ "Filters": [{"Name": "endpoint-id", "Values": ["ep-crud"]}] }),
        );
        assert_eq!(status, 200);
        assert_eq!(resp["Endpoints"].as_array().expect("list").len(), 1);
        assert_eq!(resp["Endpoints"][0]["EndpointIdentifier"], "ep-crud");
        // Nullable members drop off the wire like moto's `if v is not None`.
        assert!(resp["Endpoints"][0].get("DatabaseName").is_none());

        // Modify changes server/port.
        let (status, resp) = send(
            account,
            "ModifyEndpoint",
            json!({ "EndpointArn": arn, "ServerName": "newhost", "Port": 3307 }),
        );
        assert_eq!(status, 200);
        assert_eq!(resp["Endpoint"]["ServerName"], "newhost");
        assert_eq!(resp["Endpoint"]["Port"], 3307);

        // Delete then 404 on the second delete.
        let (status, _) = send(account, "DeleteEndpoint", json!({ "EndpointArn": arn }));
        assert_eq!(status, 200);
        let (status, body) = send(account, "DeleteEndpoint", json!({ "EndpointArn": arn }));
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ResourceNotFoundFault");
    }

    #[test]
    fn invalid_endpoint_type_is_a_validation_error() {
        let account = next_account();
        let (status, body) = send(
            account,
            "CreateEndpoint",
            json!({
                "EndpointIdentifier": "ep-bad",
                "EndpointType": "sideways",
                "EngineName": "mysql",
            }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ValidationError");
    }

    #[test]
    fn unknown_endpoint_filter_is_moto_s_500() {
        let account = next_account();
        let (status, body) = send(
            account,
            "DescribeEndpoints",
            json!({ "Filters": [{"Name": "bogus-filter", "Values": ["x"]}] }),
        );
        assert_eq!(status, 500);
        assert_eq!(body["__type"], "InternalError");
        assert!(body["message"]
            .as_str()
            .expect("message")
            .starts_with("Invalid filter name:"));
    }

    #[test]
    fn task_lifecycle_with_tags() {
        let account = next_account();
        let (src_arn, _) = create_endpoint(account, "ep-src", json!({}));
        let (ri_arn, _) = {
            let arn = create_instance(account, "ri-tasks");
            (arn, json!({}))
        };
        let (status, resp) = send(
            account,
            "CreateReplicationTask",
            json!({
                "ReplicationTaskIdentifier": "task-t",
                "SourceEndpointArn": src_arn,
                "TargetEndpointArn": "arn:aws:dms:us-east-1:123456789012:endpoint:zz",
                "ReplicationInstanceArn": ri_arn,
                "MigrationType": "full-load",
                "TableMappings": "{\"rules\":[]}",
                "Tags": [{"Key": "env", "Value": "staging"}],
            }),
        );
        assert_eq!(status, 200);
        let arn = resp["ReplicationTask"]["ReplicationTaskArn"]
            .as_str()
            .expect("arn")
            .to_string();
        assert!(arn.starts_with("arn:aws:dms:us-east-1:"));
        assert!(arn.contains(":task:task-t"));
        assert_eq!(resp["ReplicationTask"]["Status"], "creating");

        // Start and stop flip statuses.
        let (status, resp) = send(
            account,
            "StartReplicationTask",
            json!({ "ReplicationTaskArn": arn }),
        );
        assert_eq!(status, 200);
        assert_eq!(resp["ReplicationTask"]["Status"], "running");
        let (status, resp) = send(
            account,
            "StopReplicationTask",
            json!({ "ReplicationTaskArn": arn }),
        );
        assert_eq!(status, 200);
        assert_eq!(resp["ReplicationTask"]["Status"], "stopped");

        // Tags round-trip.
        let (status, resp) = send(
            account,
            "ListTagsForResource",
            json!({ "ResourceArn": arn }),
        );
        assert_eq!(status, 200);
        assert_eq!(resp["TagList"][0]["Key"], "env");
        assert_eq!(resp["TagList"][0]["ResourceArn"], arn);

        // Unstopped-but-deleted is fine; second delete 404s.
        let (status, _) = send(
            account,
            "DeleteReplicationTask",
            json!({ "ReplicationTaskArn": arn }),
        );
        assert_eq!(status, 200);
        let (status, body) = send(
            account,
            "DeleteReplicationTask",
            json!({ "ReplicationTaskArn": arn }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ResourceNotFoundFault");
    }

    #[test]
    fn stopping_a_fresh_task_is_invalid_resource_state() {
        let account = next_account();
        let (_src, _) = create_endpoint(account, "ep-x", json!({}));
        let ri_arn = create_instance(account, "ri-x");
        let (_, resp) = send(
            account,
            "CreateReplicationTask",
            json!({
                "ReplicationTaskIdentifier": "task-fresh",
                "SourceEndpointArn": "arn:aws:dms:us-east-1:123456789012:endpoint:zz",
                "TargetEndpointArn": "arn:aws:dms:us-east-1:123456789012:endpoint:zz",
                "ReplicationInstanceArn": ri_arn,
                "MigrationType": "full-load",
                "TableMappings": "{}",
            }),
        );
        let arn = resp["ReplicationTask"]["ReplicationTaskArn"]
            .as_str()
            .expect("arn")
            .to_string();
        let (status, body) = send(
            account,
            "StopReplicationTask",
            json!({ "ReplicationTaskArn": arn }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "InvalidResourceStateFault");
    }

    #[test]
    fn connection_lifecycle() {
        let account = next_account();
        let (ep_arn, _) = create_endpoint(account, "ep-conn", json!({}));
        let ri_arn = create_instance(account, "ri-conn");

        let (status, resp) = send(
            account,
            "TestConnection",
            json!({ "ReplicationInstanceArn": ri_arn, "EndpointArn": ep_arn }),
        );
        assert_eq!(status, 200);
        assert_eq!(resp["Connection"]["Status"], "successful");
        assert_eq!(resp["Connection"]["EndpointArn"], ep_arn);

        let (status, resp) = send(
            account,
            "DescribeConnections",
            json!({ "Filters": [{"Name": "endpoint-arn", "Values": [ep_arn]}] }),
        );
        assert_eq!(status, 200);
        assert_eq!(resp["Connections"].as_array().expect("list").len(), 1);

        let (status, _) = send(
            account,
            "DeleteConnection",
            json!({ "EndpointArn": ep_arn, "ReplicationInstanceArn": ri_arn }),
        );
        assert_eq!(status, 200);
        let (status, body) = send(
            account,
            "DeleteConnection",
            json!({ "EndpointArn": ep_arn, "ReplicationInstanceArn": ri_arn }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ResourceNotFoundFault");
    }

    #[test]
    fn deleting_an_instance_removes_its_connections() {
        let account = next_account();
        let (ep_arn, _) = create_endpoint(account, "ep-deli", json!({}));
        let ri_arn = create_instance(account, "ri-deli");
        send(
            account,
            "TestConnection",
            json!({ "ReplicationInstanceArn": ri_arn, "EndpointArn": ep_arn }),
        );
        send(
            account,
            "DeleteReplicationInstance",
            json!({ "ReplicationInstanceArn": ri_arn }),
        );
        let (_, resp) = send(account, "DescribeConnections", json!({}));
        assert_eq!(resp["Connections"].as_array().expect("list").len(), 0);
    }

    #[test]
    fn list_tags_requires_exactly_one_selector() {
        let account = next_account();
        for operation in [
            json!({}),
            json!({
                "ResourceArn": "arn:aws:dms:us-east-1:123456789012:endpoint:a",
                "ResourceArnList": ["arn:aws:dms:us-east-1:123456789012:endpoint:a"],
            }),
        ] {
            let (status, body) = send(account, "ListTagsForResource", operation);
            assert_eq!(status, 500);
            assert_eq!(body["__type"], "InternalError");
        }
        let (status, body) = send(
            account,
            "ListTagsForResource",
            json!({ "ResourceArn": "arn:aws:dms:us-east-1:123456789012:endpoint:a" }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["TagList"], json!([]));
    }

    #[test]
    fn instance_fields_and_multi_az() {
        let account = next_account();
        let (status, resp) = send(
            account,
            "CreateReplicationInstance",
            json!({
                "ReplicationInstanceIdentifier": "ri-fields",
                "ReplicationInstanceClass": "dms.t3.micro",
                "MultiAZ": false,
                "AllocatedStorage": 50,
            }),
        );
        assert_eq!(status, 200);
        let ri = &resp["ReplicationInstance"];
        assert_eq!(ri["MultiAZ"], false);
        assert_eq!(ri["AllocatedStorage"], 50);
        assert_eq!(ri["ReplicationInstanceStatus"], "available");
        assert!(ri["ReplicationInstanceArn"]
            .as_str()
            .unwrap()
            .starts_with("arn:aws:dms:"));
    }

    #[test]
    fn account_attributes_report_live_counts() {
        let account = next_account();
        create_endpoint(account, "ep-quotas", json!({}));
        create_instance(account, "ri-quotas");
        let (status, resp) = send(account, "DescribeAccountAttributes", json!({}));
        assert_eq!(status, 200);
        let quotas = &resp["AccountQuotas"];
        assert!(quotas.as_array().expect("list").len() >= 7);
        assert_eq!(resp["UniqueAccountIdentifier"], account.to_string());
    }

    #[test]
    fn subnet_group_round_trip_with_filter() {
        let account = next_account();
        let (status, resp) = send(
            account,
            "CreateReplicationSubnetGroup",
            json!({
                "ReplicationSubnetGroupIdentifier": "rsg-t",
                "ReplicationSubnetGroupDescription": "desc",
                "SubnetIds": ["subnet-1", "subnet-2"],
            }),
        );
        assert_eq!(status, 200);
        assert_eq!(resp["ReplicationSubnetGroup"]["VpcId"], "vpc-12345");

        let (status, resp) = send(
            account,
            "DescribeReplicationSubnetGroups",
            json!({ "Filters": [{"Name": "replication-subnet-group-id", "Values": ["rsg-t"]}] }),
        );
        assert_eq!(status, 200);
        assert_eq!(
            resp["ReplicationSubnetGroups"]
                .as_array()
                .expect("list")
                .len(),
            1
        );

        let (status, _) = send(
            account,
            "DeleteReplicationSubnetGroup",
            json!({ "ReplicationSubnetGroupIdentifier": "rsg-t" }),
        );
        assert_eq!(status, 200);

        let (status, _) = send(
            account,
            "DeleteReplicationSubnetGroup",
            json!({ "ReplicationSubnetGroupIdentifier": "rsg-t" }),
        );
        assert_eq!(status, 400);
    }

    #[test]
    fn static_catalogs_and_stubs() {
        let account = next_account();
        let (status, resp) = send(account, "DescribeEngineVersions", json!({}));
        assert_eq!(status, 200);
        assert!(!resp["EngineVersions"].as_array().unwrap().is_empty());
        assert!(resp["EngineVersions"][0].get("Version").is_some());

        let (status, resp) = send(account, "DescribeEndpointTypes", json!({}));
        assert_eq!(status, 200);
        let types = resp["SupportedEndpointTypes"].as_array().expect("list");
        assert!(!types.is_empty());
        assert!(types[0].get("EngineName").is_some());
        for entry in types {
            assert!(entry.get("EndpointType").is_some());
        }

        let (status, _) = send(account, "RunFleetAdvisorLsaAnalysis", json!({}));
        assert_eq!(status, 200);
        let (status, _) = send(
            account,
            "StartMetadataModelConversion",
            json!({ "MigrationProjectIdentifier": "p", "SelectionRules": "{}" }),
        );
        assert_eq!(status, 200);
        let (status, resp) = send(
            account,
            "DescribeSchemas",
            json!({ "EndpointArn": "arn:x" }),
        );
        assert_eq!(status, 200);
        assert_eq!(resp["Schemas"], json!([]));
        // moto's immediate ManagedState: fresh connections read successful.
        let (status, resp) = send(account, "DescribeEvents", json!({}));
        assert_eq!(status, 200);
        assert_eq!(resp["Events"], json!([]));
    }

    #[test]
    fn serverless_replication_flow() {
        let account = next_account();
        // Start/stop on a missing config 404 forwards.
        let (status, body) = send(
            account,
            "StartReplication",
            json!({ "ReplicationConfigArn": "arn:aws:dms:us-east-1:123456789012:replication-config:missing" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ResourceNotFoundFault");

        let (status, resp) = send(
            account,
            "CreateReplicationConfig",
            json!({
                "ReplicationConfigIdentifier": "rc-1",
                "SourceEndpointArn": "arn:aws:dms:us-east-1:123456789012:endpoint:s",
                "TargetEndpointArn": "arn:aws:dms:us-east-1:123456789012:endpoint:t",
                "ComputeConfig": {"MaxCapacityUnits": 2},
                "ReplicationType": "full-load",
                "TableMappings": "{}",
            }),
        );
        assert_eq!(status, 200);
        let rc_arn = resp["ReplicationConfig"]["ReplicationConfigArn"]
            .as_str()
            .expect("arn")
            .to_string();

        let (status, resp) = send(
            account,
            "StopReplication",
            json!({ "ReplicationConfigArn": rc_arn }),
        );
        assert_eq!(status, 200);
        assert_eq!(resp["Replication"]["Status"], "stopped");

        let (status, _) = send(
            account,
            "StopReplication",
            json!({ "ReplicationConfigArn": "arn:aws:dms:us-east-1:123456789012:replication-config:missing" }),
        );
        assert_eq!(status, 400);
    }

    #[test]
    fn state_is_isolated_per_account() {
        let a = next_account();
        let b = next_account();
        create_endpoint(a, "ep-shared-name", json!({}));
        let (_, resp) = send(b, "DescribeEndpoints", json!({}));
        assert_eq!(resp["Endpoints"].as_array().expect("list").len(), 0);
        let (_, resp) = send(a, "DescribeEndpoints", json!({}));
        assert_eq!(resp["Endpoints"].as_array().expect("list").len(), 1);
    }
}
