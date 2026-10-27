//! AWS MSK (Kafka) backend — port of moto's `kafka` engine.
//!
//! Kafka's rest-json wire uses camelCase keys (the spec's locationNames) in
//! both directions; moto echoes stored request values verbatim, so the
//! port keeps echo fields as raw `Value`s. Clusters key by ARN like moto's
//! `KafkaBackend`; tags stay in a separate TaggingService-style map. Everything
//! is (account, region) scoped, and unknown cluster ARNs raise
//! NotFoundException with moto's message.

use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Map, Value};
use std::collections::HashMap;

static SLOT: Lazy<store::Slot<KafkaState>> = Lazy::new(|| store::slot::<KafkaState>("kafka"));

const KAFKA_VERSIONS: [&str; 22] = [
    "1.1.1",
    "2.2.1",
    "2.3.1",
    "2.4.1",
    "2.4.1.1",
    "2.5.1",
    "2.6.0",
    "2.6.1",
    "2.6.2",
    "2.7.0",
    "2.7.1",
    "2.7.2",
    "2.8.0",
    "2.8.1",
    "2.8.2.tiered",
    "3.1.1",
    "3.2.0",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.5.1",
    "3.6.0",
];

fn compatible_versions(source: &str) -> Vec<Value> {
    let out: &[&str] = match source {
        "1.1.1" => &["2.2.1"],
        "2.2.1" => &["2.3.1"],
        "2.3.1" => &["2.4.1", "2.4.1.1"],
        "2.4.1" | "2.4.1.1" => &["2.5.1"],
        "2.5.1" => &["2.6.0", "2.6.1", "2.6.2"],
        "2.6.0" => &["2.6.1", "2.6.2", "2.7.0"],
        "2.6.1" => &["2.6.2", "2.7.0", "2.7.1"],
        "2.6.2" => &["2.7.0", "2.7.1", "2.7.2"],
        "2.7.0" => &["2.7.1", "2.7.2", "2.8.0"],
        "2.7.1" => &["2.7.2", "2.8.0", "2.8.1"],
        "2.7.2" => &["2.8.0", "2.8.1"],
        "2.8.0" => &["2.8.1", "2.8.2.tiered", "3.1.1"],
        "2.8.1" => &["2.8.2.tiered", "3.1.1", "3.2.0"],
        "2.8.2.tiered" => &["3.1.1", "3.2.0"],
        "3.1.1" => &["3.2.0", "3.3.1"],
        "3.2.0" => &["3.3.1", "3.3.2"],
        "3.3.1" => &["3.3.2", "3.4.0"],
        "3.3.2" => &["3.4.0", "3.5.1"],
        "3.4.0" => &["3.5.1", "3.6.0"],
        "3.5.1" => &["3.6.0"],
        _ => &[],
    };
    out.iter().map(|v| json!(v)).collect()
}

#[derive(Default)]
struct KafkaState {
    clusters: HashMap<String, Cluster>,
    configurations: HashMap<String, Configuration>,
    replicators: HashMap<String, Value>,
    replicator_versions: HashMap<String, String>,
    vpc_connections: HashMap<String, VpcConnection>,
    cluster_operations: HashMap<String, Operation>,
    tags: HashMap<String, Map<String, Value>>,
    region: String,
    account: String,
}

struct Cluster {
    name: String,
    id: String,
    arn: String,
    cluster_type: String,
    state: String,
    creation_time: String,
    current_version: String,
    broker_node_group_info: Value,
    client_authentication: Value,
    kafka_version: Option<Value>,
    number_of_broker_nodes: Option<Value>,
    configuration_info: Value,
    encryption_info: Value,
    enhanced_monitoring: String,
    open_monitoring: Value,
    logging_info: Value,
    storage_mode: String,
    zookeeper_connect_string: Option<Value>,
    zookeeper_connect_string_tls: Option<Value>,
    connectivity_info: Option<Value>,
    serverless_config: Option<Value>,
    scram_secrets: Vec<Value>,
    cluster_policy: Option<String>,
    cluster_policy_version: String,
    topics: HashMap<String, Topic>,
    client_vpc_connections: Vec<String>,
}

#[derive(Default, Clone)]
struct Topic {
    topic_name: String,
    topic_arn: String,
    partition_count: i64,
    replication_factor: i64,
    configs: Value,
    status: String,
}

#[derive(Default, Clone)]
struct Configuration {
    name: String,
    arn: String,
    description: String,
    kafka_versions: Value,
    state: String,
    creation_time: String,
    revisions: Vec<Value>,
}

#[derive(Default, Clone)]
struct VpcConnection {
    arn: String,
    target_cluster_arn: String,
    state: String,
    authentication: String,
    vpc_id: String,
    client_subnets: Value,
    security_groups: Value,
    creation_time: String,
}

#[derive(Clone)]
struct Operation {
    cluster_arn: String,
    arn: String,
    creation_time: String,
    end_time: String,
    operation_state: String,
    operation_type: String,
}

fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn not_found_cluster(arn: &str) -> Response {
    crate::proto::json_error(
        404,
        "NotFoundException",
        &format!("The cluster with the Amazon Resource Name (ARN) {arn} was not found."),
    )
}

fn not_found(kind: &str, arn: &str) -> Response {
    crate::proto::json_error(
        404,
        "NotFoundException",
        &format!("The {kind} with the ARN {arn} was not found."),
    )
}

pub fn dispatch(req: &Request) -> Response {
    let params = crate::proto::parse_json_body(&req.body);
    let mut guard = SLOT.write();
    let st = guard.entry((req.account, req.region.clone())).or_default();
    if st.region.is_empty() {
        st.region = req.region.clone();
        st.account = req.account.to_string();
    }
    let path = crate::proto::url_decode(req.path.trim_matches('/'));
    let method = req.method.as_str();
    let query = crate::proto::query_map(&req.query_string);

    match (method, path.as_str()) {
        ("POST", "v1/clusters") => st.create_cluster(&params),
        ("GET", "v1/clusters") => st.list_clusters(),
        ("POST", "api/v2/clusters") => st.create_cluster_v2(&params),
        ("GET", "api/v2/clusters") => st.list_clusters_v2(),
        ("GET", "v1/kafka-versions") => Response::json_status(
            200,
            &json!({
                "kafkaVersions": KAFKA_VERSIONS.iter().map(|v| json!({"version": v, "status": "ACTIVE"})).collect::<Vec<_>>(),
            }),
        ),
        ("GET", "v1/compatible-kafka-versions") => {
            st.get_compatible_kafka_versions(params.get("clusterArn"))
        }
        ("GET", "v1/configurations") => st.list_configurations(),
        ("POST", "v1/configurations") => st.create_configuration(&params),
        ("POST", "replication/v1/replicators") => st.create_replicator(&params),
        ("GET", "replication/v1/replicators") => st.list_replicators(),
        ("POST", "v1/vpc-connection") => st.create_vpc_connection(&params),
        ("GET", "v1/vpc-connections") => st.list_vpc_connections(),
        _ => {
            // ARN-bearing subpaths.
            if let Some(rest) = path_api_rest(&path, "v1/clusters/") {
                return cluster_route(st, method, rest, &params, &query);
            }
            if let Some(rest) = path_api_rest(&path, "api/v2/clusters/") {
                if let Some(arn) = rest.strip_suffix("/operations") {
                    return st.list_cluster_operations(arn, true);
                }
                return st.describe_cluster_v2(rest);
            }
            if let Some(rest) = path_api_rest(&path, "v1/configurations/") {
                return configuration_route(st, method, rest, &params);
            }
            if let Some(rest) = path_api_rest(&path, "v1/vpc-connection/") {
                return match method {
                    "GET" => st.describe_vpc_connection(rest),
                    "DELETE" => st.delete_vpc_connection(rest),
                    _ => unimplemented(req),
                };
            }
            if let Some(rest) = path_api_rest(&path, "replication/v1/replicators/") {
                if let Some(info) = rest.strip_suffix("replication-info") {
                    return st.update_replication_info(info, &params);
                }
                return match method {
                    "GET" => st.describe_replicator(rest),
                    "DELETE" => st.delete_replicator(rest),
                    _ => unimplemented(req),
                };
            }
            if let Some(rest) = path_api_rest(&path, "v1/operations/") {
                return st.describe_cluster_operation(rest);
            }
            if let Some(rest) = path_api_rest(&path, "api/v2/operations/") {
                return st.describe_cluster_operation_v2(rest);
            }
            if let Some(arn) = path_api_rest(&path, "v1/tags/") {
                return match method {
                    "GET" => st.list_tags_for_resource(arn),
                    "POST" => st.tag_resource(arn, &params),
                    "DELETE" => {
                        // TagKeys binds to the querystring, not the body.
                        let keys = query
                            .get("tagKeys")
                            .map(|v| v.iter().map(|k| json!(k)).collect::<Vec<_>>())
                            .unwrap_or_default();
                        st.untag_resource(arn, &json!({"tagKeys": keys}))
                    }
                    _ => unimplemented(req),
                };
            }
            unimplemented(req)
        }
    }
}

fn unimplemented(req: &Request) -> Response {
    crate::proto::json_error(
        501,
        "NotImplemented",
        &format!("The {} action has not been implemented", req.operation),
    )
}

fn path_api_rest<'a>(path: &'a str, prefix: &str) -> Option<&'a str> {
    path.strip_prefix(prefix)
}

/// Everything after `/v1/clusters/`, split into (arn, suffix) with the same
/// suffix-walk moto uses in `_cluster_arn_from_path`.
fn split_cluster_path(rest: &str) -> (String, String) {
    for suffix in [
        "/bootstrap-brokers",
        "/scram-secrets",
        "/client-vpc-connections",
        "/client-vpc-connection",
        "/nodes/count",
        "/nodes/storage",
        "/nodes/type",
        "/nodes",
        "/operations",
        "/configuration",
        "/version",
        "/connectivity",
        "/monitoring",
        "/security",
        "/storage",
        "/rebalancing",
        "/reboot-broker",
        "/policy",
        "/topics",
    ] {
        if let Some(pos) = rest.find(suffix) {
            return (rest[..pos].to_string(), rest[pos..].to_string());
        }
    }
    (rest.to_string(), String::new())
}

/// Parse an int body field with a default (moto's `int(param or default)`).
fn param_str(params: &Value, key: &str) -> String {
    params
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn param_i64(params: &Value, key: &str, default: i64) -> i64 {
    params.get(key).and_then(Value::as_i64).unwrap_or(default)
}

fn cluster_tag(st: &KafkaState, arn: &str) -> Value {
    match st.tags.get(arn) {
        Some(tags) => Value::Object(tags.clone()),
        None => json!({}),
    }
}

fn tag_set(st: &mut KafkaState, arn: &str, tags: Option<&Value>) {
    if let Some(Value::Object(map)) = tags {
        let entry = st.tags.entry(arn.to_string()).or_default();
        for (k, v) in map {
            // moto's TaggingService stores {"Key", "Value"} list pairs
            entry.insert(k.clone(), v.clone());
        }
    }
}

fn tag_remove(st: &mut KafkaState, arn: &str, keys: &[Value]) {
    if let Some(entry) = st.tags.get_mut(arn) {
        for key in keys {
            if let Some(k) = key.as_str() {
                entry.remove(k);
            }
        }
    }
}

fn new_operation(st: &mut KafkaState, cluster_arn: &str, op_type: &str) -> Operation {
    let op = Operation {
        cluster_arn: cluster_arn.to_string(),
        arn: format!(
            "arn:aws:kafka:{region}:{account}:cluster-operation/{uuid}",
            region = st.region,
            account = st.account,
            uuid = uuid::Uuid::new_v4()
        ),
        creation_time: now_iso(),
        end_time: now_iso(),
        operation_state: "UPDATE_COMPLETE".to_string(),
        operation_type: op_type.to_string(),
    };
    op
}

impl Operation {
    fn to_json(&self, v2: bool) -> Value {
        if v2 {
            json!({
                "clusterArn": self.cluster_arn,
                "clusterOperationArn": self.arn,
                "clusterType": "PROVISIONED",
                "startTime": self.creation_time,
                "endTime": self.end_time,
                "operationState": self.operation_state,
                "operationType": self.operation_type,
            })
        } else {
            json!({
                "clusterArn": self.cluster_arn,
                "clusterOperationArn": self.arn,
                "creationTime": self.creation_time,
                "endTime": self.end_time,
                "operationState": self.operation_state,
                "operationType": self.operation_type,
            })
        }
    }
}

impl Configuration {
    fn latest_revision(&self) -> &Value {
        self.revisions.last().expect("at least one revision")
    }

    fn latest_revision_summary(&self) -> Value {
        let rev = self.latest_revision();
        json!({
            "creationTime": rev["creationTime"],
            "description": rev["description"],
            "revision": rev["revision"],
        })
    }

    fn add_revision(&mut self, description: Option<Value>, server_properties: Option<&Value>) {
        let revision = json!({
            "creationTime": now_iso(),
            "description": description.unwrap_or(json!("")),
            "revision": self.revisions.len() + 1,
            "serverProperties": server_properties.cloned().unwrap_or(json!("")),
        });
        self.revisions.push(revision);
    }
}

impl KafkaState {
    fn get_cluster(&self, arn: &str) -> Result<&Cluster, Response> {
        self.clusters.get(arn).ok_or_else(|| not_found_cluster(arn))
    }

    #[allow(dead_code)]
    fn get_cluster_mut(&mut self, arn: &str) -> Result<&mut Cluster, Response> {
        self.clusters
            .get_mut(arn)
            .ok_or_else(|| not_found_cluster(arn))
    }

    // ---- clusters ----------------------------------------------------------

    fn create_cluster(&mut self, params: &Value) -> Response {
        let name = param_str(params, "clusterName");
        let kafka_version = params
            .get("kafkaVersion")
            .cloned()
            .unwrap_or(json!("2.8.1"));
        let cluster = Cluster {
            name: name.clone(),
            id: uuid::Uuid::new_v4().to_string(),
            arn: String::new(),
            cluster_type: "PROVISIONED".into(),
            state: "CREATING".into(),
            creation_time: now_iso(),
            current_version: "1.0".into(),
            broker_node_group_info: params
                .get("brokerNodeGroupInfo")
                .cloned()
                .unwrap_or(json!({})),
            client_authentication: params
                .get("clientAuthentication")
                .cloned()
                .unwrap_or(json!({})),
            kafka_version: Some(kafka_version),
            number_of_broker_nodes: params.get("numberOfBrokerNodes").cloned(),
            configuration_info: params
                .get("configurationInfo")
                .cloned()
                .unwrap_or(json!({})),
            encryption_info: params.get("encryptionInfo").cloned().unwrap_or(json!({})),
            enhanced_monitoring: params
                .get("enhancedMonitoring")
                .and_then(Value::as_str)
                .unwrap_or("DEFAULT")
                .to_string(),
            open_monitoring: params.get("openMonitoring").cloned().unwrap_or(json!({})),
            logging_info: params.get("loggingInfo").cloned().unwrap_or(json!({})),
            storage_mode: params
                .get("storageMode")
                .and_then(Value::as_str)
                .unwrap_or("LOCAL")
                .to_string(),
            zookeeper_connect_string: None,
            zookeeper_connect_string_tls: None,
            connectivity_info: None,
            serverless_config: None,
            scram_secrets: Vec::new(),
            cluster_policy: None,
            cluster_policy_version: "1".into(),
            topics: HashMap::new(),
            client_vpc_connections: Vec::new(),
        };
        let mut cluster = cluster;
        cluster.arn = self.cluster_arn(cluster.id.clone());
        let arn = cluster.arn.clone();
        self.clusters.insert(arn.clone(), cluster);
        tag_set(self, &arn, params.get("tags"));
        Response::json_status(
            200,
            &json!({"clusterArn": arn, "clusterName": name, "state": "CREATING"}),
        )
    }

    fn cluster_arn(&self, id: String) -> String {
        format!(
            "arn:aws:kafka:{region}:{account}:cluster/{id}",
            region = self.region,
            account = self.account
        )
    }

    fn describe_cluster_info(&self, cluster: &Cluster) -> Value {
        json!({
            "activeOperationArn": "arn:aws:kafka:region:account-id:operation/active-operation",
            "brokerNodeGroupInfo": cluster.broker_node_group_info,
            "clientAuthentication": cluster.client_authentication,
            "clusterArn": cluster.arn,
            "clusterName": cluster.name,
            "creationTime": cluster.creation_time,
            "currentBrokerSoftwareInfo": {
                "configurationArn": cluster.configuration_info.get("arn").cloned().unwrap_or(json!("string")),
                "configurationRevision": cluster.configuration_info.get("revision").cloned().unwrap_or(json!(1)),
                "kafkaVersion": cluster.kafka_version.clone(),
            },
            "currentVersion": cluster.current_version,
            "encryptionInfo": cluster.encryption_info,
            "enhancedMonitoring": cluster.enhanced_monitoring,
            "openMonitoring": cluster.open_monitoring,
            "loggingInfo": cluster.logging_info,
            "numberOfBrokerNodes": cluster.number_of_broker_nodes.clone().unwrap_or(json!(0)),
            "state": cluster.state,
            "stateInfo": {"code": "string", "message": "Cluster state details."},
            "tags": cluster_tag(self, &cluster.arn),
            "zookeeperConnectString": cluster.zookeeper_connect_string.clone().unwrap_or(json!("zookeeper.example.com:2181")),
            "zookeeperConnectStringTls": cluster.zookeeper_connect_string_tls.clone().unwrap_or(json!("zookeeper.example.com:2181")),
            "storageMode": cluster.storage_mode,
            "customerActionStatus": "NONE",
        })
    }

    fn describe_cluster(&self, arn: &str) -> Response {
        match self.get_cluster(arn) {
            Err(resp) => resp,
            Ok(cluster) => Response::json_status(
                200,
                &json!({"clusterInfo": self.describe_cluster_info(cluster)}),
            ),
        }
    }

    fn describe_cluster_v2_info(&self, cluster: &Cluster) -> Value {
        let mut info = json!({
            "activeOperationArn": "arn:aws:kafka:region:account-id:operation/active-operation",
            "clusterArn": cluster.arn,
            "clusterName": cluster.name,
            "clusterType": cluster.cluster_type,
            "creationTime": cluster.creation_time,
            "currentVersion": cluster.current_version,
            "state": cluster.state,
            "stateInfo": {"code": "string", "message": "Cluster state details."},
            "tags": cluster_tag(self, &cluster.arn),
        });
        if cluster.cluster_type == "PROVISIONED" {
            info["provisioned"] = json!({
                "brokerNodeGroupInfo": cluster.broker_node_group_info,
                "clientAuthentication": cluster.client_authentication,
                "currentBrokerSoftwareInfo": {
                    "configurationArn": cluster.configuration_info.get("arn").cloned().unwrap_or(json!("string")),
                    "configurationRevision": cluster.configuration_info.get("revision").cloned().unwrap_or(json!(1)),
                    "kafkaVersion": cluster.kafka_version.clone(),
                },
                "encryptionInfo": cluster.encryption_info,
                "enhancedMonitoring": cluster.enhanced_monitoring,
                "openMonitoring": cluster.open_monitoring,
                "loggingInfo": cluster.logging_info,
                "numberOfBrokerNodes": cluster.number_of_broker_nodes.clone().unwrap_or(json!(0)),
                "zookeeperConnectString": cluster.zookeeper_connect_string.clone().unwrap_or(json!("zookeeper.example.com:2181")),
                "zookeeperConnectStringTls": cluster.zookeeper_connect_string_tls.clone().unwrap_or(json!("zookeeper.example.com:2181")),
                "storageMode": cluster.storage_mode,
                "customerActionStatus": "NONE",
            });
        } else {
            info["serverless"] = json!({
                "vpcConfigs": cluster.serverless_config.as_ref()
                    .and_then(|s| s.get("vpcConfigs"))
                    .cloned()
                    .unwrap_or(json!([])),
                "clientAuthentication": cluster.serverless_config.as_ref()
                    .and_then(|s| s.get("clientAuthentication"))
                    .cloned()
                    .unwrap_or(json!({})),
            });
        }
        info
    }

    fn describe_cluster_v2(&self, arn: &str) -> Response {
        match self.get_cluster(arn) {
            Err(resp) => resp,
            Ok(cluster) => Response::json_status(
                200,
                &json!({"clusterInfo": self.describe_cluster_v2_info(cluster)}),
            ),
        }
    }

    fn create_cluster_v2(&mut self, params: &Value) -> Response {
        let provisioned = params.get("provisioned").filter(|v| !v.is_null());
        let serverless = params.get("serverless").filter(|v| !v.is_null());
        let Some(cluster) = (match (provisioned, serverless) {
            (Some(p), _) => Some(Cluster {
                name: param_str(params, "clusterName"),
                id: uuid::Uuid::new_v4().to_string(),
                arn: String::new(),
                cluster_type: "PROVISIONED".into(),
                state: "CREATING".into(),
                creation_time: now_iso(),
                current_version: "1.0".into(),
                broker_node_group_info: p.get("brokerNodeGroupInfo").cloned().unwrap_or(json!({})),
                client_authentication: json!({}),
                kafka_version: p
                    .get("kafkaVersion")
                    .cloned()
                    .or(Some(json!("default-kafka-version"))),
                number_of_broker_nodes: p.get("numberOfBrokerNodes").cloned().map(|v| {
                    json!(match v {
                        Value::Number(_) => v.as_i64().unwrap_or(1),
                        _ => 1,
                    })
                }),
                configuration_info: json!({}),
                encryption_info: json!({}),
                enhanced_monitoring: "DEFAULT".into(),
                open_monitoring: json!({}),
                logging_info: json!({}),
                storage_mode: p
                    .get("storageMode")
                    .and_then(Value::as_str)
                    .unwrap_or("LOCAL")
                    .to_string(),
                zookeeper_connect_string: None,
                zookeeper_connect_string_tls: None,
                connectivity_info: None,
                serverless_config: None,
                scram_secrets: Vec::new(),
                cluster_policy: None,
                cluster_policy_version: "1".into(),
                topics: HashMap::new(),
                client_vpc_connections: Vec::new(),
            }),
            (None, Some(s)) => Some(Cluster {
                name: param_str(params, "clusterName"),
                id: uuid::Uuid::new_v4().to_string(),
                arn: String::new(),
                cluster_type: "SERVERLESS".into(),
                state: "CREATING".into(),
                creation_time: now_iso(),
                current_version: "1.0".into(),
                broker_node_group_info: Value::Null,
                client_authentication: json!({}),
                kafka_version: None,
                number_of_broker_nodes: None,
                configuration_info: json!({}),
                encryption_info: json!({}),
                enhanced_monitoring: "DEFAULT".into(),
                open_monitoring: json!({}),
                logging_info: json!({}),
                storage_mode: "LOCAL".into(),
                zookeeper_connect_string: None,
                zookeeper_connect_string_tls: None,
                connectivity_info: None,
                serverless_config: Some(s.clone()),
                scram_secrets: Vec::new(),
                cluster_policy: None,
                cluster_policy_version: "1".into(),
                topics: HashMap::new(),
                client_vpc_connections: Vec::new(),
            }),
            (None, None) => None,
        }) else {
            return crate::proto::json_error(
                400,
                "BadRequestException",
                "Either provisioned or serverless must be provided.",
            );
        };
        let name = cluster.name.clone();
        let cluster_type = cluster.cluster_type.clone();
        let mut cluster = cluster;
        cluster.arn = self.cluster_arn(cluster.id.clone());
        let arn = cluster.arn.clone();
        self.clusters.insert(arn.clone(), cluster);
        tag_set(self, &arn, params.get("tags"));
        Response::json_status(
            200,
            &json!({
                "clusterArn": arn,
                "clusterName": name,
                "state": "CREATING",
                "clusterType": cluster_type,
            }),
        )
    }

    fn list_clusters(&self) -> Response {
        let clusters: Vec<Value> = self
            .clusters
            .values()
            .map(|c| {
                json!({
                    "clusterArn": c.arn,
                    "clusterName": c.name,
                    "state": c.state,
                    "creationTime": c.creation_time,
                    "clusterType": c.cluster_type,
                })
            })
            .collect();
        Response::json_status(
            200,
            &json!({"clusterInfoList": clusters, "nextToken": Value::Null}),
        )
    }

    fn list_clusters_v2(&self) -> Response {
        let clusters: Vec<Value> = self
            .clusters
            .values()
            .map(|c| self.describe_cluster_v2_info(c))
            .collect();
        Response::json_status(
            200,
            &json!({"clusterInfoList": clusters, "nextToken": Value::Null}),
        )
    }

    fn delete_cluster(&mut self, arn: &str, _params: &Value) -> Response {
        let state = match self.get_cluster(arn) {
            Err(resp) => return resp,
            Ok(c) => c.state.clone(),
        };
        self.clusters.remove(arn);
        self.tags.remove(arn);
        Response::json_status(200, &json!({"clusterArn": arn, "state": state}))
    }

    // ---- bootstrap / nodes / versions ---------------------------------------

    fn get_bootstrap_brokers(&self, arn: &str) -> Response {
        let cluster = match self.get_cluster(arn) {
            Err(resp) => return resp,
            Ok(c) => c,
        };
        let n = cluster
            .number_of_broker_nodes
            .as_ref()
            .and_then(Value::as_u64)
            .unwrap_or(3);
        let brokers: Vec<String> = (1..=n)
            .map(|i| {
                format!(
                    "b-{i}.{name}.kafka.{region}.amazonaws.com",
                    name = cluster.name,
                    region = self.region
                )
            })
            .collect();
        Response::json_status(
            200,
            &json!({
                "bootstrapBrokerString": brokers.join_csv_with_port(9092),
                "bootstrapBrokerStringTls": brokers.join_csv_with_port(9094),
                "bootstrapBrokerStringSaslScram": brokers.join_csv_with_port(9096),
                "bootstrapBrokerStringSaslIam": brokers.join_csv_with_port(9098),
                "bootstrapBrokerStringPublicTls": "",
                "bootstrapBrokerStringPublicSaslScram": "",
                "bootstrapBrokerStringPublicSaslIam": "",
                "bootstrapBrokerStringVpcConnectivityTls": "",
                "bootstrapBrokerStringVpcConnectivitySaslScram": "",
                "bootstrapBrokerStringVpcConnectivitySaslIam": "",
            }),
        )
    }

    fn list_nodes(&self, arn: &str) -> Response {
        let cluster = match self.get_cluster(arn) {
            Err(resp) => return resp,
            Ok(c) => c,
        };
        let n = cluster
            .number_of_broker_nodes
            .as_ref()
            .and_then(Value::as_u64)
            .unwrap_or(3);
        let first_subnet = cluster
            .broker_node_group_info
            .as_object()
            .and_then(|b| b.get("clientSubnets"))
            .and_then(Value::as_array)
            .and_then(|a| a.first().cloned())
            .unwrap_or(json!(["subnet-00000"]));
        let nodes: Vec<Value> = (1..=n)
            .map(|i| {
                json!({
                    "nodeType": "BROKER",
                    "nodeARN": format!("{arn}/broker/{i}"),
                    "brokerNodeInfo": {
                        "attachedENIId": format!("eni-{}", uuid::Uuid::new_v4().simple()),
                        "brokerId": i as f64,
                        "clientSubnet": first_subnet,
                        "clientVpcIpAddress": format!("10.0.{i}.{i}"),
                        "currentBrokerSoftwareInfo": {"kafkaVersion": cluster.kafka_version.clone().unwrap_or(json!("2.8.1"))},
                        "endpoints": [format!("b-{i}.{name}.kafka.{region}.amazonaws.com", name = cluster.name, region = self.region)],
                    },
                })
            })
            .collect();
        Response::json_status(
            200,
            &json!({"nodeInfoList": nodes, "nextToken": Value::Null}),
        )
    }

    // ---- cluster policies ---------------------------------------------------

    fn put_cluster_policy(&mut self, arn: &str, params: &Value) -> Response {
        if self.get_cluster(arn).is_err() {
            return not_found_cluster(arn);
        }
        let policy = params
            .get("policy")
            .map(Value::to_string)
            .unwrap_or_default();
        let version = {
            let cluster = self.clusters.get_mut(arn).unwrap();
            cluster.cluster_policy = Some(policy);
            cluster.cluster_policy_version = increment(&cluster.cluster_policy_version);
            cluster.cluster_policy_version.clone()
        };
        Response::json_status(200, &json!({"currentVersion": version}))
    }

    fn get_cluster_policy(&self, arn: &str) -> Response {
        match self.get_cluster(arn) {
            Err(resp) => resp,
            Ok(cluster) => match &cluster.cluster_policy {
                Some(policy) => Response::json_status(
                    200,
                    &json!({"currentVersion": cluster.cluster_policy_version, "policy": policy}),
                ),
                None => crate::proto::json_error(
                    404,
                    "NotFoundException",
                    &format!("No cluster policy found for cluster {arn}."),
                ),
            },
        }
    }

    fn delete_cluster_policy(&mut self, arn: &str) -> Response {
        if self.get_cluster(arn).is_err() {
            return not_found_cluster(arn);
        }
        if let Some(cluster) = self.clusters.get_mut(arn) {
            cluster.cluster_policy = None;
        }
        Response::json_status(200, &json!({}))
    }

    // ---- scram secrets -------------------------------------------------------

    fn batch_associate_scram_secret(&mut self, arn: &str, params: &Value) -> Response {
        let secrets = params
            .get("secretArnList")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if self.get_cluster(arn).is_err() {
            return not_found_cluster(arn);
        }
        let mut unprocessed = Vec::new();
        {
            let cluster = self.clusters.get_mut(arn).unwrap();
            for sa in secrets {
                let already = cluster.scram_secrets.iter().any(|s| s == &sa);
                if already {
                    unprocessed.push(json!({
                        "errorCode": "InvalidParameterException",
                        "errorMessage": format!("Secret {sa} is already associated."),
                        "secretArn": sa,
                    }));
                } else {
                    cluster.scram_secrets.push(sa);
                }
            }
        }
        Response::json_status(
            200,
            &json!({"clusterArn": arn, "unprocessedScramSecrets": unprocessed}),
        )
    }

    fn batch_disassociate_scram_secret(&mut self, arn: &str, params: &Value) -> Response {
        let secrets = params
            .get("secretArnList")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if self.get_cluster(arn).is_err() {
            return not_found_cluster(arn);
        }
        let mut unprocessed = Vec::new();
        let cluster = self.clusters.get_mut(arn).unwrap();
        for sa in secrets {
            if let Some(pos) = cluster.scram_secrets.iter().position(|x| x == &sa) {
                cluster.scram_secrets.remove(pos);
            } else {
                unprocessed.push(json!({
                    "errorCode": "InvalidParameterException",
                    "errorMessage": format!("Secret {sa} is not associated."),
                    "secretArn": sa,
                }));
            }
        }
        Response::json_status(
            200,
            &json!({"clusterArn": arn, "unprocessedScramSecrets": unprocessed}),
        )
    }

    fn list_scram_secrets(&self, arn: &str) -> Response {
        match self.get_cluster(arn) {
            Err(resp) => resp,
            Ok(cluster) => Response::json_status(
                200,
                &json!({"secretArnList": cluster.scram_secrets, "nextToken": Value::Null}),
            ),
        }
    }

    // ---- cluster operations ---------------------------------------------------

    fn describe_cluster_operation(&self, op_arn: &str) -> Response {
        match self.cluster_operations.get(op_arn) {
            Some(op) => {
                Response::json_status(200, &json!({"clusterOperationInfo": op.to_json(false)}))
            }
            None => not_found("cluster operation", op_arn),
        }
    }

    fn describe_cluster_operation_v2(&self, op_arn: &str) -> Response {
        match self.cluster_operations.get(op_arn) {
            Some(op) => {
                Response::json_status(200, &json!({"clusterOperationInfo": op.to_json(true)}))
            }
            None => not_found("cluster operation", op_arn),
        }
    }

    fn list_cluster_operations(&self, arn: &str, v2: bool) -> Response {
        if self.get_cluster(arn).is_err() {
            return not_found_cluster(arn);
        }
        let ops: Vec<Value> = self
            .cluster_operations
            .values()
            .filter(|op| op.cluster_arn == arn)
            .map(|op| op.to_json(v2))
            .collect();
        Response::json_status(
            200,
            &json!({"clusterOperationInfoList": ops, "nextToken": Value::Null}),
        )
    }

    fn update_generic(
        &mut self,
        arn: &str,
        op_type: &str,
        handle: impl FnOnce(&mut Cluster),
    ) -> Response {
        if self.get_cluster(arn).is_err() {
            return not_found_cluster(arn);
        }
        {
            let cluster = self.clusters.get_mut(arn).unwrap();
            handle(cluster);
            cluster.current_version = increment(&cluster.current_version);
        }
        let op = new_operation(self, arn, op_type);
        self.cluster_operations.insert(op.arn.clone(), op.clone());
        Response::json_status(
            200,
            &json!({"clusterArn": arn, "clusterOperationArn": op.arn}),
        )
    }

    fn reboot_broker(&mut self, arn: &str) -> Response {
        self.update_generic(arn, "REBOOT_BROKER", |_| {})
    }

    // ---- configurations --------------------------------------------------------

    fn create_configuration(&mut self, params: &Value) -> Response {
        let name = param_str(params, "name");
        let description = params
            .get("description")
            .cloned()
            .unwrap_or(json!("".to_string()));
        let mut config = Configuration {
            name: name.clone(),
            arn: format!(
                "arn:aws:kafka:{region}:{account}:configuration/{name}/{uuid}",
                region = self.region,
                account = self.account,
                uuid = uuid::Uuid::new_v4()
            ),
            description: description.as_str().unwrap_or("").to_string(),
            kafka_versions: params.get("kafkaVersions").cloned().unwrap_or(json!([])),
            state: "ACTIVE".into(),
            creation_time: now_iso(),
            revisions: Vec::new(),
        };
        config.add_revision(
            params.get("description").cloned().filter(|v| !v.is_null()),
            params.get("serverProperties"),
        );
        let out = json!({
            "arn": config.arn,
            "creationTime": config.creation_time,
            "latestRevision": config.latest_revision_summary(),
            "name": config.name,
            "state": config.state,
        });
        self.configurations.insert(config.arn.clone(), config);
        Response::json_status(200, &out)
    }

    fn get_compatible_kafka_versions(&self, cluster_arn: Option<&Value>) -> Response {
        if let Some(cluster_arn) = cluster_arn.and_then(Value::as_str) {
            let sv = match self.get_cluster(cluster_arn) {
                Err(resp) => return resp,
                Ok(c) => c
                    .kafka_version
                    .as_ref()
                    .and_then(Value::as_str)
                    .unwrap_or("2.8.1")
                    .to_string(),
            };
            return Response::json_status(
                200,
                &json!({"compatibleKafkaVersions": [{"sourceVersion": sv, "targetVersions": compatible_versions(&sv)}]}),
            );
        }
        let all: Vec<Value> = KAFKA_VERSIONS
            .iter()
            .filter_map(|v| {
                let targets = compatible_versions(v);
                if targets.is_empty() {
                    None
                } else {
                    Some(json!({"sourceVersion": v, "targetVersions": targets}))
                }
            })
            .collect();
        Response::json_status(200, &json!({"compatibleKafkaVersions": all}))
    }

    fn get_configuration(&self, arn: &str) -> Result<&Configuration, Response> {
        self.configurations
            .get(arn)
            .ok_or_else(|| not_found("configuration", arn))
    }

    fn describe_configuration(&self, arn: &str) -> Response {
        match self.get_configuration(arn) {
            Err(resp) => resp,
            Ok(config) => {
                let out = json!({
                    "arn": config.arn,
                    "creationTime": config.creation_time,
                    "description": config.description,
                    "kafkaVersions": config.kafka_versions,
                    "latestRevision": config.latest_revision_summary(),
                    "name": config.name,
                    "state": config.state,
                });
                Response::json_status(200, &out)
            }
        }
    }

    fn delete_configuration(&mut self, arn: &str) -> Response {
        if self.get_configuration(arn).is_err() {
            return not_found("configuration", arn);
        }
        self.configurations.remove(arn);
        self.tags.remove(arn);
        Response::json_status(200, &json!({"arn": arn, "state": "DELETING"}))
    }

    fn update_configuration(&mut self, arn: &str, params: &Value) -> Response {
        let Some(config) = self.configurations.get_mut(arn) else {
            return not_found("configuration", arn);
        };
        config.add_revision(
            params.get("description").cloned(),
            params.get("serverProperties"),
        );
        let out = json!({
            "arn": config.arn,
            "latestRevision": config.latest_revision_summary(),
        });
        Response::json_status(200, &out)
    }

    fn list_configurations(&self) -> Response {
        let configs: Vec<Value> = self
            .configurations
            .values()
            .map(|config| {
                json!({
                    "arn": config.arn,
                    "creationTime": config.creation_time,
                    "description": config.description,
                    "kafkaVersions": config.kafka_versions,
                    "latestRevision": config.latest_revision_summary(),
                    "name": config.name,
                    "state": config.state,
                })
            })
            .collect();
        Response::json_status(
            200,
            &json!({"configurations": configs, "nextToken": Value::Null}),
        )
    }

    fn list_configuration_revisions(&self, arn: &str) -> Response {
        match self.get_configuration(arn) {
            Err(resp) => resp,
            Ok(config) => {
                let revisions: Vec<Value> = config
                    .revisions
                    .iter()
                    .map(|r| {
                        json!({
                            "creationTime": r["creationTime"],
                            "description": r["description"],
                            "revision": r["revision"],
                        })
                    })
                    .collect();
                Response::json_status(
                    200,
                    &json!({"revisions": revisions, "nextToken": Value::Null}),
                )
            }
        }
    }

    fn describe_configuration_revision(&self, arn: &str, revision: &str) -> Response {
        let revision_no: i64 = revision.parse().unwrap_or(-1);
        match self.get_configuration(arn) {
            Err(resp) => resp,
            Ok(config) => match config
                .revisions
                .iter()
                .find(|r| r["revision"].as_i64() == Some(revision_no))
            {
                Some(rev) => Response::json_status(
                    200,
                    &json!({
                        "arn": config.arn,
                        "creationTime": rev["creationTime"],
                        "description": rev["description"],
                        "revision": rev["revision"],
                        "serverProperties": rev["serverProperties"],
                    }),
                ),
                None => crate::proto::json_error(
                    404,
                    "NotFoundException",
                    &format!("Revision {revision} for configuration {arn} was not found."),
                ),
            },
        }
    }

    // ---- replicators -------------------------------------------------------------

    fn create_replicator(&mut self, params: &Value) -> Response {
        let name = param_str(params, "replicatorName");
        let arn = format!(
            "arn:aws:kafka:{region}:{account}:replicator/{name}/{uuid}",
            region = self.region,
            account = self.account,
            uuid = uuid::Uuid::new_v4()
        );
        let replicator = json!({
            "replicatorArn": arn.clone(),
            "replicatorName": name,
            "replicatorState": "RUNNING",
            "creationTime": now_iso(),
            "currentVersion": "1",
            "isReplicatorReference": false,
            "kafkaClusters": params.get("kafkaClusters").cloned().unwrap_or(json!([])),
            "replicationInfoList": params.get("replicationInfoList").cloned().unwrap_or(json!([])),
            "replicatorDescription": param_str(params, "description"),
            "replicatorResourceArn": arn,
            "replicatorState": "RUNNING",
            "serviceExecutionRoleArn": param_str(params, "serviceExecutionRoleArn"),
            "stateInfo": {},
            "tags": {},
        });
        self.replicator_versions
            .insert(arn.clone(), "1".to_string());
        self.replicators.insert(arn.clone(), replicator.clone());
        tag_set(self, &arn, params.get("tags"));
        let out = json!({
            "replicatorArn": arn,
            "replicatorName": replicator["replicatorName"],
            "replicatorState": replicator["replicatorState"],
        });
        Response::json_status(200, &out)
    }

    fn describe_replicator(&self, arn: &str) -> Response {
        match self.replicators.get(arn) {
            Some(r) => Response::json_status(200, r),
            None => not_found("replicator", arn),
        }
    }

    fn delete_replicator(&mut self, arn: &str) -> Response {
        if self.replicators.remove(arn).is_none() {
            return not_found("replicator", arn);
        }
        self.replicator_versions.remove(arn);
        self.tags.remove(arn);
        Response::json_status(
            200,
            &json!({"replicatorArn": arn, "replicatorState": "DELETING"}),
        )
    }

    fn list_replicators(&self) -> Response {
        let reps: Vec<Value> = self
            .replicators
            .values()
            .map(|r| {
                json!({
                    "creationTime": r["creationTime"],
                    "currentVersion": r["currentVersion"],
                    "isReplicatorReference": false,
                    "replicatorArn": r["replicatorArn"],
                    "replicatorName": r["replicatorName"],
                    "replicatorState": r["replicatorState"],
                })
            })
            .collect();
        Response::json_status(200, &json!({"replicators": reps, "nextToken": Value::Null}))
    }

    fn update_replication_info(&mut self, arn: &str, params: &Value) -> Response {
        let Some(repl) = self.replicators.get_mut(arn) else {
            return not_found("replicator", arn);
        };
        if let Some(list) = repl
            .get_mut("replicationInfoList")
            .and_then(Value::as_array_mut)
        {
            for info in list {
                if let Some(cgr) = params.get("consumerGroupReplication") {
                    info["consumerGroupReplication"] = cgr.clone();
                }
                if let Some(tr) = params.get("topicReplication") {
                    info["topicReplication"] = tr.clone();
                }
            }
        }
        let version = self
            .replicator_versions
            .get(arn)
            .cloned()
            .unwrap_or_else(|| "1".into());
        self.replicator_versions
            .insert(arn.to_string(), increment(&version));
        let state = repl["replicatorState"].clone();
        Response::json_status(
            200,
            &json!({"replicatorArn": arn, "replicatorState": state}),
        )
    }

    // ---- vpc connections ------------------------------------------------------------

    fn create_vpc_connection(&mut self, params: &Value) -> Response {
        let arn = format!(
            "arn:aws:kafka:{region}:{account}:vpc-connection/{uuid}",
            region = self.region,
            account = self.account,
            uuid = uuid::Uuid::new_v4()
        );
        let target = param_str(params, "targetClusterArn");
        let vc = VpcConnection {
            arn: arn.clone(),
            target_cluster_arn: target.clone(),
            state: "AVAILABLE".into(),
            authentication: param_str(params, "authentication"),
            vpc_id: param_str(params, "vpcId"),
            client_subnets: params.get("clientSubnets").cloned().unwrap_or(json!([])),
            security_groups: params.get("securityGroups").cloned().unwrap_or(json!([])),
            creation_time: now_iso(),
        };
        tag_set(self, &arn, params.get("tags"));
        let out = json!({
            "vpcConnectionArn": arn.clone(),
            "state": vc.state,
            "authentication": vc.authentication,
            "vpcId": vc.vpc_id,
            "clientSubnets": vc.client_subnets,
            "securityGroups": vc.security_groups,
            "creationTime": vc.creation_time,
            "tags": {},
        });
        if let Some(cluster) = self.clusters.get_mut(&target) {
            cluster.client_vpc_connections.push(arn.clone());
        }
        self.vpc_connections.insert(arn.clone(), vc);
        let _ = arn;
        Response::json_status(200, &out)
    }

    fn get_vpc_connection(&self, arn: &str) -> Result<&VpcConnection, Response> {
        self.vpc_connections
            .get(arn)
            .ok_or_else(|| not_found("VPC connection", arn))
    }

    fn describe_vpc_connection(&self, arn: &str) -> Response {
        match self.get_vpc_connection(arn) {
            Err(resp) => resp,
            Ok(vc) => Response::json_status(
                200,
                &json!({
                    "vpcConnectionArn": vc.arn,
                    "targetClusterArn": vc.target_cluster_arn,
                    "state": vc.state,
                    "authentication": vc.authentication,
                    "vpcId": vc.vpc_id,
                    "subnets": vc.client_subnets,
                    "securityGroups": vc.security_groups,
                    "creationTime": vc.creation_time,
                    "tags": cluster_tag(self, &vc.arn),
                }),
            ),
        }
    }

    fn delete_vpc_connection(&mut self, arn: &str) -> Response {
        let Some(vc) = self.vpc_connections.remove(arn) else {
            return not_found("VPC connection", arn);
        };
        self.tags.remove(arn);
        if let Some(cluster) = self.clusters.get_mut(&vc.target_cluster_arn) {
            cluster.client_vpc_connections.retain(|a| a != arn);
        }
        Response::json_status(200, &json!({"vpcConnectionArn": arn, "state": "DELETING"}))
    }

    fn list_vpc_connections(&self) -> Response {
        let conns: Vec<Value> = self
            .vpc_connections
            .values()
            .map(|vc| {
                json!({
                    "vpcConnectionArn": vc.arn,
                    "targetClusterArn": vc.target_cluster_arn,
                    "creationTime": vc.creation_time,
                    "authentication": vc.authentication,
                    "vpcId": vc.vpc_id,
                    "state": vc.state,
                })
            })
            .collect();
        Response::json_status(
            200,
            &json!({"vpcConnections": conns, "nextToken": Value::Null}),
        )
    }

    fn list_client_vpc_connections(&self, arn: &str) -> Response {
        let cluster = match self.get_cluster(arn) {
            Err(resp) => return resp,
            Ok(c) => c,
        };
        let conns: Vec<Value> = cluster
            .client_vpc_connections
            .iter()
            .filter_map(|a| self.vpc_connections.get(a))
            .map(|vc| {
                json!({
                    "vpcConnectionArn": vc.arn,
                    "owner": self.account.to_string(),
                    "authentication": vc.authentication,
                    "creationTime": vc.creation_time,
                    "state": vc.state,
                })
            })
            .collect();
        Response::json_status(
            200,
            &json!({"clientVpcConnections": conns, "nextToken": Value::Null}),
        )
    }

    fn reject_client_vpc_connection(&mut self, arn: &str, params: &Value) -> Response {
        if self.get_cluster(arn).is_err() {
            return not_found_cluster(arn);
        }
        let vpc_arn = param_str(params, "vpcConnectionArn");
        if let Some(vc) = self.vpc_connections.get_mut(&vpc_arn) {
            vc.state = "REJECTED".into();
        }
        Response::json_status(200, &json!({}))
    }

    // ---- topics -----------------------------------------------------------------------

    fn create_topic(&mut self, arn: &str, params: &Value) -> Response {
        let topic_name = param_str(params, "topicName");
        let replica = param_i64(params, "replicationFactor", 1);
        if self.get_cluster(arn).is_err() {
            return not_found_cluster(arn);
        }
        let region = self.region.clone();
        let account = self.account.clone();
        let cluster_id = self
            .clusters
            .get(arn)
            .map(|c| c.arn.clone())
            .unwrap_or_default();
        {
            let cluster = self.clusters.get_mut(arn).unwrap();
            if cluster.topics.contains_key(&topic_name) {
                return crate::proto::json_error(
                    409,
                    "ConflictException",
                    &format!("Topic {topic_name} already exists."),
                );
            }
            let topic = Topic {
                topic_name: topic_name.clone(),
                topic_arn: format!(
                    "arn:aws:kafka:{region}:{account}:topic/{cluster_id}/{topic_name}"
                ),
                partition_count: param_i64(params, "partitionCount", 1),
                replication_factor: replica,
                configs: params.get("configs").cloned().unwrap_or(json!({})),
                status: "ACTIVE".into(),
            };
            cluster.topics.insert(topic_name.clone(), topic.clone());
            let out = json!({
                "topicArn": topic.topic_arn,
                "topicName": topic.topic_name,
                "status": topic.status,
            });
            Response::json_status(200, &out)
        }
    }

    fn topic_ref(&self, arn: &str, topic_name: &str) -> Result<(String, Value), Response> {
        let cluster = self.get_cluster(arn)?;
        let topic = cluster.topics.get(topic_name).ok_or_else(|| {
            crate::proto::json_error(
                404,
                "NotFoundException",
                &format!("Topic {topic_name} was not found."),
            )
        })?;
        Ok((
            topic.topic_arn.clone(),
            json!({
                "topic_arn": topic.topic_arn,
                "topic_name": topic.topic_name,
                "replication_factor": topic.replication_factor,
                "partition_count": topic.partition_count,
                "configs": topic.configs,
                "status": topic.status,
            }),
        ))
    }

    fn delete_topic(&mut self, arn: &str, topic_name: &str) -> Response {
        let (topic_arn, _topic) = match self.topic_ref(arn, topic_name) {
            Ok(pair) => pair,
            Err(resp) => return resp,
        };
        let out = json!({
            "topicArn": topic_arn,
            "topicName": topic_name,
            "status": "DELETING",
        });
        if let Some(cluster) = self.clusters.get_mut(arn) {
            cluster.topics.remove(topic_name);
        }
        Response::json_status(200, &out)
    }

    fn describe_topic(&self, arn: &str, topic_name: &str) -> Response {
        let (_, topic) = match self.topic_ref(arn, topic_name) {
            Ok(pair) => pair,
            Err(resp) => return resp,
        };
        let out = json!({
            "topicArn": topic["topic_arn"],
            "topicName": topic["topic_name"],
            "replicationFactor": topic["replication_factor"],
            "partitionCount": topic["partition_count"],
            "configs": topic["configs"],
            "status": topic["status"],
        });
        Response::json_status(200, &out)
    }

    fn list_topics(&self, arn: &str) -> Response {
        match self.get_cluster(arn) {
            Err(resp) => resp,
            Ok(cluster) => {
                let topics: Vec<Value> = cluster
                    .topics
                    .values()
                    .map(|t| json!({"topicArn": t.topic_arn, "topicName": t.topic_name, "status": t.status}))
                    .collect();
                Response::json_status(200, &json!({"topics": topics, "nextToken": Value::Null}))
            }
        }
    }

    fn update_topic(&mut self, arn: &str, topic_name: &str, params: &Value) -> Response {
        if self.topic_ref(arn, topic_name).is_err() {
            return match self.topic_ref(arn, topic_name) {
                Ok(_) => unreachable!(),
                Err(resp) => resp,
            };
        }
        let out = {
            let cluster = self.clusters.get_mut(arn).unwrap();
            let topic = cluster.topics.get_mut(topic_name).unwrap();
            if let Some(configs) = params.get("configs") {
                topic.configs = configs.clone();
            }
            if let Some(pc) = params.get("partitionCount") {
                topic.partition_count = pc.as_i64().unwrap_or(1);
            }
            json!({
                "topicArn": topic.topic_arn,
                "topicName": topic.topic_name,
                "status": topic.status,
            })
        };
        Response::json_status(200, &out)
    }

    fn describe_topic_partitions(&self, arn: &str, topic_name: &str) -> Response {
        let (_, topic) = match self.topic_ref(arn, topic_name) {
            Ok(pair) => pair,
            Err(resp) => return resp,
        };
        let partitions: Vec<Value> = (0..topic["partition_count"].as_i64().unwrap_or(1))
            .map(|i| json!({"partitionIndex": i, "isr": [1], "leader": 1, "replicas": [1]}))
            .collect();
        Response::json_status(
            200,
            &json!({"partitions": partitions, "nextToken": Value::Null}),
        )
    }

    // ---- tags ------------------------------------------------------------------------------

    fn list_tags_for_resource(&self, arn: &str) -> Response {
        Response::json_status(200, &json!({"tags": cluster_tag(self, arn)}))
    }

    fn tag_resource(&mut self, arn: &str, params: &Value) -> Response {
        let tags = params.get("tags").cloned().unwrap_or(json!({}));
        tag_set(self, arn, Some(&tags));
        Response::json_status(200, &json!({}))
    }

    fn untag_resource(&mut self, arn: &str, params: &Value) -> Response {
        let keys = params
            .get("tagKeys")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        tag_remove(self, arn, &keys);
        Response::json_status(200, &json!({}))
    }
}

trait JoinCsv {
    fn join_csv_with_port(&self, port: u16) -> String;
}

impl JoinCsv for Vec<String> {
    fn join_csv_with_port(&self, port: u16) -> String {
        self.iter()
            .map(|b| format!("{b}:{port}"))
            .collect::<Vec<_>>()
            .join(",")
    }
}

fn increment(version: &str) -> String {
    match version.parse::<i64>() {
        Ok(v) => (v + 1).to_string(),
        Err(_) => "2".to_string(),
    }
}

impl KafkaState {
    /// Route the sub-suffixes kafka binds under /v1/clusters/{arn}.
    #[allow(dead_code)]
    fn cluster_route_all(&self) {}
}

impl Default for Cluster {
    fn default() -> Self {
        Self {
            name: String::new(),
            id: String::new(),
            arn: String::new(),
            cluster_type: "PROVISIONED".into(),
            state: "CREATING".into(),
            creation_time: now_iso(),
            current_version: "1.0".into(),
            broker_node_group_info: json!({}),
            client_authentication: json!({}),
            kafka_version: Some(json!("2.8.1")),
            number_of_broker_nodes: Some(json!(1)),
            configuration_info: json!({}),
            encryption_info: json!({}),
            enhanced_monitoring: "DEFAULT".into(),
            open_monitoring: json!({}),
            logging_info: json!({}),
            storage_mode: "LOCAL".into(),
            zookeeper_connect_string: None,
            zookeeper_connect_string_tls: None,
            connectivity_info: None,
            serverless_config: None,
            scram_secrets: Vec::new(),
            cluster_policy: None,
            cluster_policy_version: "1".into(),
            topics: HashMap::new(),
            client_vpc_connections: Vec::new(),
        }
    }
}

/// Route cluster sub-paths against the state, mirroring moto's suffix walk.
fn cluster_route(
    st: &mut KafkaState,
    method: &str,
    rest: &str,
    params: &Value,
    _query: &std::collections::BTreeMap<String, Vec<String>>,
) -> Response {
    let (arn, suffix) = split_cluster_path(rest);
    match (method, suffix.as_str()) {
        ("GET", "") => st.describe_cluster(&arn),
        ("DELETE", "") => st.delete_cluster(&arn, params),
        ("GET", "/bootstrap-brokers") => st.get_bootstrap_brokers(&arn),
        ("PUT", "/policy") => st.put_cluster_policy(&arn, params),
        ("GET", "/policy") => st.get_cluster_policy(&arn),
        ("DELETE", "/policy") => st.delete_cluster_policy(&arn),
        ("POST", "/scram-secrets") => st.batch_associate_scram_secret(&arn, params),
        ("PATCH", "/scram-secrets") => st.batch_disassociate_scram_secret(&arn, params),
        ("GET", "/scram-secrets") => st.list_scram_secrets(&arn),
        ("GET", "/client-vpc-connections") => st.list_client_vpc_connections(&arn),
        ("PUT", "/client-vpc-connection") => st.reject_client_vpc_connection(&arn, params),
        ("GET", "/nodes") => st.list_nodes(&arn),
        ("PUT", "/nodes/count") => st.update_generic(&arn, "UPDATE_BROKER_COUNT", |c| {
            let target = params
                .get("targetNumberOfBrokerNodes")
                .cloned()
                .map(|v| json!(v.as_i64().unwrap_or(1)));
            c.number_of_broker_nodes = target;
        }),
        ("PUT", "/nodes/storage") => st.update_generic(&arn, "UPDATE_BROKER_STORAGE", |_| {}),
        ("PUT", "/nodes/type") => {
            let instance = param_str(params, "targetInstanceType");
            st.update_generic(&arn, "UPDATE_BROKER_TYPE", |c| {
                if let Some(b) = c.broker_node_group_info.as_object_mut() {
                    b.insert("instanceType".into(), json!(instance));
                }
            })
        }
        ("PUT", "/configuration") => {
            let info = params
                .get("configurationInfo")
                .cloned()
                .unwrap_or(json!({}));
            st.update_generic(&arn, "UPDATE_CLUSTER_CONFIGURATION", |c| {
                c.configuration_info = info;
            })
        }
        ("PUT", "/version") => {
            let target = params
                .get("targetKafkaVersion")
                .cloned()
                .unwrap_or(json!(""));
            let info = params.get("configurationInfo").cloned();
            st.update_generic(&arn, "UPDATE_CLUSTER_KAFKA_VERSION", |c| {
                c.kafka_version = Some(target.clone());
                if let Some(info) = info.clone() {
                    c.configuration_info = info;
                }
            })
        }
        ("PUT", "/connectivity") => {
            let info = params.get("connectivityInfo").cloned().unwrap_or(json!({}));
            st.update_generic(&arn, "UPDATE_CONNECTIVITY", |c| {
                c.connectivity_info = Some(info);
            })
        }
        ("PUT", "/monitoring") => {
            let enhanced = param_str(params, "enhancedMonitoring");
            let open = params.get("openMonitoring").cloned().unwrap_or(json!({}));
            let logging = params.get("loggingInfo").cloned().unwrap_or(json!({}));
            st.update_generic(&arn, "UPDATE_MONITORING", |c| {
                if !enhanced.is_empty() {
                    c.enhanced_monitoring = enhanced.clone();
                }
                c.open_monitoring = open.clone();
                c.logging_info = logging.clone();
            })
        }
        ("PATCH", "/security") => {
            let auth = params.get("clientAuthentication").cloned();
            let enc = params.get("encryptionInfo").cloned();
            st.update_generic(&arn, "UPDATE_SECURITY", |c| {
                if let Some(auth) = auth.clone() {
                    c.client_authentication = auth;
                }
                if let Some(enc) = enc.clone() {
                    c.encryption_info = enc;
                }
            })
        }
        ("PUT", "/storage") => {
            let mode = params
                .get("storageMode")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            st.update_generic(&arn, "UPDATE_STORAGE", |c| {
                if !mode.is_empty() {
                    c.storage_mode = mode.clone();
                }
            })
        }
        ("PUT", "/rebalancing") => st.update_generic(&arn, "UPDATE_REBALANCING", |_| {}),
        ("PUT", "/reboot-broker") => st.reboot_broker(&arn),
        ("GET", "/operations") => st.list_cluster_operations(&arn, false),
        ("POST", "/topics") => st.create_topic(&arn, params),
        ("GET", "/topics") => st.list_topics(&arn),
        _ => {
            if suffix.starts_with("/topics/") {
                let topic_name = suffix.trim_start_matches("/topics/");
                // topic sub-resources: /{name}/partitions
                let (topic_name, sub) = match topic_name.split_once('/') {
                    Some((name, sub)) => (name, Some(sub)),
                    None => (topic_name, None),
                };
                return match (method, sub) {
                    ("GET", None) => st.describe_topic(&arn, topic_name),
                    ("PATCH" | "PUT", None) => st.update_topic(&arn, topic_name, params),
                    ("DELETE", None) => st.delete_topic(&arn, topic_name),
                    ("GET", Some("partitions")) => st.describe_topic_partitions(&arn, topic_name),
                    _ => unimplemented(&Request {
                        service: "kafka".into(),
                        operation: String::new(),
                        method: method.into(),
                        path: format!("/{rest}"),
                        query_string: String::new(),
                        headers: HashMap::new(),
                        body: bytes::Bytes::new(),
                        account: st.account.parse().unwrap_or(0),
                        region: st.region.clone(),
                    }),
                };
            }
            unimplemented(&Request {
                service: "kafka".into(),
                operation: String::new(),
                method: method.into(),
                path: format!("/{rest}"),
                query_string: String::new(),
                headers: HashMap::new(),
                body: bytes::Bytes::new(),
                account: st.account.parse().unwrap_or(0),
                region: st.region.clone(),
            })
        }
    }
}

fn configuration_route(st: &mut KafkaState, method: &str, rest: &str, params: &Value) -> Response {
    let (arn, rest) = match rest.split_once("/revisions") {
        Some((arn, revs)) => (arn.to_string(), Some(revs.to_string())),
        None => (rest.to_string(), None),
    };
    match (method, rest) {
        ("GET", None) => st.describe_configuration(&arn),
        ("DELETE", None) => st.delete_configuration(&arn),
        ("PUT", None) => st.update_configuration(&arn, params),
        ("GET", Some(revs)) if revs.is_empty() => st.list_configuration_revisions(&arn),
        ("GET", Some(revs)) => {
            let revision = revs.trim_start_matches('/');
            st.describe_configuration_revision(&arn, revision)
        }
        (_, None) => st.describe_configuration(&arn),
        (_, Some(_)) => unimplemented(&unknown_req()),
    }
}

fn unknown_req() -> Request {
    Request {
        service: "kafka".into(),
        operation: String::new(),
        method: "GET".into(),
        path: "/".into(),
        query_string: String::new(),
        headers: HashMap::new(),
        body: bytes::Bytes::new(),
        account: 0,
        region: String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn req(method: &str, path: &str, body: Value) -> Request {
        Request {
            service: "kafka".into(),
            operation: String::new(),
            method: method.into(),
            path: path.into(),
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

    fn cluster_body() -> Value {
        json!({
            "clusterName": "mpk",
            "kafkaVersion": "2.8.1",
            "numberOfBrokerNodes": 3,
            "brokerNodeGroupInfo": {"instanceType": "kafka.m5.large", "clientSubnets": ["subnet-1","subnet-2","subnet-3"]},
        })
    }

    #[test]
    fn cluster_create_describe_delete() {
        let created = json_body(dispatch(&req("POST", "/v1/clusters", cluster_body())));
        let arn = created["clusterArn"].as_str().unwrap().to_string();
        assert_eq!(created["state"], "CREATING");

        let info = json_body(dispatch(&req(
            "GET",
            &format!("/v1/clusters/{arn}"),
            json!({}),
        )));
        let info = &info["clusterInfo"];
        assert_eq!(info["clusterName"], "mpk");
        assert_eq!(
            info["brokerNodeGroupInfo"]["instanceType"],
            "kafka.m5.large"
        );
        assert_eq!(info["numberOfBrokerNodes"], 3);
        assert_eq!(info["currentBrokerSoftwareInfo"]["kafkaVersion"], "2.8.1");
        assert!(info["zookeeperConnectString"].is_string());
        assert!(info["currentVersion"].is_string());

        let deleted = json_body(dispatch(&req(
            "DELETE",
            &format!("/v1/clusters/{arn}"),
            json!({}),
        )));
        assert_eq!(deleted["clusterArn"], arn);
    }

    #[test]
    fn cluster_v2_provisioned_and_serverless() {
        let created = json_body(dispatch(&req(
            "POST",
            "/api/v2/clusters",
            json!({
                "clusterName": "mpkv2",
                "provisioned": {
                    "brokerNodeGroupInfo": {"instanceType": "kafka.m5.large"},
                    "kafkaVersion": "2.8.1",
                    "numberOfBrokerNodes": 3,
                },
            }),
        )));
        let arn = created["clusterArn"].as_str().unwrap().to_string();
        assert_eq!(created["clusterType"], "PROVISIONED");
        let v2 = json_body(dispatch(&req(
            "GET",
            &format!("/api/v2/clusters/{arn}"),
            json!({}),
        )));
        assert_eq!(v2["clusterInfo"]["provisioned"]["numberOfBrokerNodes"], 3);

        let serverless = json_body(dispatch(&req(
            "POST",
            "/api/v2/clusters",
            json!({
                "clusterName": "mpk-sl",
                "serverless": {"vpcConfigs": [{"subnetIds": ["subnet-1"]}],
                               "clientAuthentication": {"sasl": {"iam": {"enabled": true}}}},
            }),
        )));
        assert_eq!(serverless["clusterType"], "SERVERLESS");
    }

    #[test]
    fn bootstrap_brokers_and_nodes() {
        let created = json_body(dispatch(&req("POST", "/v1/clusters", cluster_body())));
        let arn = created["clusterArn"].as_str().unwrap().to_string();

        let brokers = json_body(dispatch(&req(
            "GET",
            &format!("/v1/clusters/{arn}/bootstrap-brokers"),
            json!({}),
        )));
        assert!(brokers
            .get("bootstrapBrokerString")
            .and_then(Value::as_str)
            .is_some());
        assert!(brokers["bootstrapBrokerString"]
            .as_str()
            .unwrap()
            .contains(":9092"));
        assert!(brokers["bootstrapBrokerStringSaslIam"]
            .as_str()
            .unwrap()
            .contains(":9098"));

        let nodes = json_body(dispatch(&req(
            "GET",
            &format!("/v1/clusters/{arn}/nodes"),
            json!({}),
        )));
        assert_eq!(nodes["nodeInfoList"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn policies_scram_and_updates() {
        let created = json_body(dispatch(&req("POST", "/v1/clusters", cluster_body())));
        let arn = created["clusterArn"].as_str().unwrap().to_string();

        let put = json_body(dispatch(&req(
            "PUT",
            &format!("/v1/clusters/{arn}/policy"),
            json!({"policy": "{}"}),
        )));
        assert!(put["currentVersion"].is_string());

        let upd = json_body(dispatch(&req(
            "PUT",
            &format!("/v1/clusters/{arn}/nodes/count"),
            json!({"currentVersion": "1.0", "targetNumberOfBrokerNodes": 6}),
        )));
        assert_eq!(upd["clusterArn"], arn);
        assert!(upd["clusterOperationArn"].as_str().is_some());

        let ops = json_body(dispatch(&req(
            "GET",
            &format!("/v1/clusters/{arn}/operations"),
            json!({}),
        )));
        assert!(!ops["clusterOperationInfoList"]
            .as_array()
            .unwrap()
            .is_empty());

        let assoc = json_body(dispatch(&req(
            "POST",
            &format!("/v1/clusters/{arn}/scram-secrets"),
            json!({"secretArnList": ["arn:aws:secretsmanager:us-east-1:123456789012:secret:AmazonMSK_s"]}),
        )));
        assert_eq!(
            assoc["unprocessedScramSecrets"].as_array().unwrap().len(),
            0
        );
        let listed = json_body(dispatch(&req(
            "GET",
            &format!("/v1/clusters/{arn}/scram-secrets"),
            json!({}),
        )));
        assert_eq!(listed["secretArnList"].as_array().unwrap().len(), 1);

        let fake = dispatch(&req(
            "DELETE",
            "/v1/clusters/arn:aws:kafka:us-east-1:123456789012:cluster/fake/fake-uuid",
            json!({}),
        ));
        assert_eq!(fake.status, 404);
    }

    #[test]
    fn configurations_lifecycle() {
        let created = json_body(dispatch(&req(
            "POST",
            "/v1/configurations",
            json!({"name": "cfg", "serverProperties": base64("auto.create.topics.enable=true"), "kafkaVersions": ["2.8.1"]}),
        )));
        let arn = created["arn"].as_str().unwrap().to_string();
        assert_eq!(created["name"], "cfg");

        let desc = json_body(dispatch(&req(
            "GET",
            &format!("/v1/configurations/{arn}"),
            json!({}),
        )));
        assert_eq!(desc["latestRevision"]["revision"], 1);

        let upd = json_body(dispatch(&req(
            "PUT",
            &format!("/v1/configurations/{arn}"),
            json!({"serverProperties": base64("log.retention.hours=72")}),
        )));
        assert_eq!(upd["latestRevision"]["revision"], 2);

        let revs_list = json_body(dispatch(&req(
            "GET",
            &format!("/v1/configurations/{arn}/revisions"),
            json!({}),
        )));
        assert_eq!(revs_list["revisions"].as_array().unwrap().len(), 2);
        let rev1 = json_body(dispatch(&req(
            "GET",
            &format!("/v1/configurations/{arn}/revisions/1"),
            json!({}),
        )));
        assert_eq!(rev1["revision"], 1);

        let del = json_body(dispatch(&req(
            "DELETE",
            &format!("/v1/configurations/{arn}"),
            json!({}),
        )));
        assert_eq!(del["state"], "DELETING");

        let fake = dispatch(&req(
            "GET",
            "/v1/configurations/arn:aws:kafka:us-east-1:123456789012:configuration/fake/uuid",
            json!({}),
        ));
        assert_eq!(fake.status, 404);
    }

    #[test]
    fn tags_merge_and_remove() {
        let created = json_body(dispatch(&req(
            "POST",
            "/v1/clusters",
            json!({
                "clusterName": "tagged",
                "kafkaVersion": "2.8.1",
                "numberOfBrokerNodes": 1,
                "tags": {"env": "staging"},
            }),
        )));
        let arn = created["clusterArn"].as_str().unwrap().to_string();

        let tags = json_body(dispatch(&req("GET", &format!("/v1/tags/{arn}"), json!({}))));
        assert_eq!(tags["tags"]["env"], "staging");

        dispatch(&req(
            "POST",
            &format!("/v1/tags/{arn}"),
            json!({"tags": {"key": "new"}}),
        ));
        // Untag keys ride the querystring per the spec.
        let mut untag_req = req("DELETE", &format!("/v1/tags/{arn}"), json!({}));
        untag_req.query_string = "tagKeys=env".into();
        dispatch(&untag_req);
        let after = json_body(dispatch(&req("GET", &format!("/v1/tags/{arn}"), json!({}))));
        assert!(after["tags"].get("env").is_none());
        assert_eq!(after["tags"]["key"], "new");
    }

    #[test]
    fn kafka_versions_and_compatible() {
        let versions = json_body(dispatch(&req("GET", "/v1/kafka-versions", json!({}))));
        assert!(!versions["kafkaVersions"].as_array().unwrap().is_empty());
        let compat = json_body(dispatch(&req(
            "GET",
            "/v1/compatible-kafka-versions",
            json!({}),
        )));
        assert!(!compat["compatibleKafkaVersions"]
            .as_array()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn vpc_connection_lifecycle() {
        let created = json_body(dispatch(&req(
            "POST",
            "/v1/vpc-connection",
            json!({
                "targetClusterArn": "arn:aws:kafka:us-east-1:123456789012:cluster/test/test-uuid",
                "authentication": "SASL_IAM",
                "vpcId": "vpc-12345",
                "clientSubnets": ["subnet-1", "subnet-2"],
                "securityGroups": ["sg-12345"],
            }),
        )));
        let arn = created["vpcConnectionArn"].as_str().unwrap().to_string();
        assert_eq!(created["state"], "AVAILABLE");
        assert_eq!(created["authentication"], "SASL_IAM");

        let deleted = json_body(dispatch(&req(
            "DELETE",
            &format!("/v1/vpc-connection/{arn}"),
            json!({}),
        )));
        assert_eq!(deleted["state"], "DELETING");
    }

    #[test]
    fn replicators_round_trip() {
        let created = json_body(dispatch(&req(
            "POST",
            "/replication/v1/replicators",
            json!({
                "replicatorName": "replr",
                "serviceExecutionRoleArn": "arn:aws:iam::123456789012:role/r",
            }),
        )));
        assert_eq!(created["replicatorState"], "RUNNING");
        let arn = created["replicatorArn"].as_str().unwrap().to_string();
        let del = json_body(dispatch(&req(
            "DELETE",
            &format!("/replication/v1/replicators/{arn}"),
            json!({}),
        )));
        assert_eq!(del["replicatorState"], "DELETING");
    }

    fn base64(data: &str) -> String {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD.encode(data)
    }

    #[test]
    fn topic_partitions_and_list() {
        let created = json_body(dispatch(&req("POST", "/v1/clusters", cluster_body())));
        let arn = created["clusterArn"].as_str().unwrap().to_string();
        let encoded = urlencoding::encode(&arn).into_owned();
        let topic = json_body(dispatch(&req(
            "POST",
            &format!("/v1/clusters/{encoded}/topics"),
            json!({"topicName": "tp", "partitionCount": 3, "replicationFactor": 1}),
        )));
        assert_eq!(topic["status"], "ACTIVE");
        let partitions = json_body(dispatch(&req(
            "GET",
            &format!("/v1/clusters/{encoded}/topics/tp/partitions"),
            json!({}),
        )));
        assert_eq!(partitions["partitions"].as_array().unwrap().len(), 3);
        let fake = json_body(dispatch(&req(
            "GET",
            &format!("/v1/clusters/{encoded}/topics/fake/partitions"),
            json!({}),
        )));
        assert_eq!(fake["__type"], "NotFoundException");
    }
}
