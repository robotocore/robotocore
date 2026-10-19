//! Redshift backend — pure-Rust port of the moto redshift service.
//!
//! Wire contract: AWS query protocol, XML namespace
//! `http://redshift.amazonaws.com/doc/2012-12-01/`. The moto sidecar runs
//! every response through botocore's serializers using the model's output
//! shapes; this port reproduces that byte-for-byte by driving the XML
//! renderer from `redshift/shapes.json` (generated from botocore's own
//! service model — see `tooling/gen_redshift_shapes.py`).
//!
//! Request parameters arrive in the urlencoded body (`Action=...&Version=
//! 2012-12-01&...`) using botocore's list encodings (`Tags.Tag.1.Key`,
//! `SnapshotIdentifierList.String.1`, ...) and are parsed shape-driven,
//! mirroring moto's `QueryParser`. Response value trees carry the same
//! CamelCase keys moto's backends produce; members absent or `null` in the
//! tree are omitted from the wire, objects/empty lists render as `<Key/>`.
//!
//! Subnet groups validate subnet ids against the local EC2 state via a
//! loopback gateway mirror (moto reads its own `ec2_backend` there).
//! Error codes/HTTP statuses come from the model's error shapes; moto's own
//! unimplemented operations respond exactly like the sidecar: 501
//! `NotImplemented` (`The <snake_op> action has not been implemented`).

use crate::store;
use crate::{Request, Response};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::sync::LazyLock;

pub const XMLNS: &str = "http://redshift.amazonaws.com/doc/2012-12-01/";

static SHAPES: LazyLock<Value> = LazyLock::new(|| {
    serde_json::from_str(include_str!("redshift/shapes.json"))
        .expect("redshift/shapes.json must parse")
});

// ---------------------------------------------------------------------------
// Randomness / time helpers (charset-compatible with moto's mock_random)
// ---------------------------------------------------------------------------

fn request_id() -> String {
    rand_string(52)
}

/// moto `get_random_hex`: lowercase hex digits.
fn rand_hex(n: usize) -> String {
    const CHARS: &[u8] = b"0123456789abcdef";
    (0..n)
        .map(|i| {
            let u = uuid::Uuid::new_v4();
            CHARS[(u.as_bytes()[i % 16] as usize + u.as_bytes()[(i + 7) % 16] as usize) % 16]
                as char
        })
        .collect()
}

/// moto `get_random_string`: mixed-case letters + digits.
fn rand_string(n: usize) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    (0..n)
        .map(|i| {
            let u = uuid::Uuid::new_v4();
            let b = i % 8;
            CHARS[(u.as_bytes()[b] as usize + u.as_bytes()[15 - b] as usize) % CHARS.len()] as char
        })
        .collect()
}

/// ISO-8601 micros, moto's XML timestamp format (`%Y-%m-%dT%H:%M:%S.%fZ`).
fn iso_micro(now: chrono::DateTime<chrono::Utc>) -> String {
    now.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()
}

fn now_iso() -> String {
    iso_micro(chrono::Utc::now())
}

/// Time `seconds` from now, in the wire timestamp format.
fn iso_in(secs: i64) -> String {
    iso_micro(chrono::Utc::now() + chrono::Duration::seconds(secs))
}

/// Python `str(float)` equivalent for the shapes that render double values.
fn py_float(f: f64) -> String {
    if f == f.trunc() && f.abs() < 1e15 {
        format!("{f:.1}")
    } else {
        format!("{f}")
    }
}

/// moto's `camelcase_to_underscores` for the gap-op error message.
fn snake_case(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 8);
    for (i, c) in name.chars().enumerate() {
        if c.is_ascii_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Params: input-shape-driven parse mirroring moto's QueryParser.
// ---------------------------------------------------------------------------

/// Flat value map from the urlencoded body + query string.
pub fn flat_values(req: &Request) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for (k, v) in decode_pairs(req.query_string.as_bytes()) {
        if !k.is_empty() {
            out.entry(k).or_insert(v);
        }
    }
    for (k, v) in decode_pairs(&req.body) {
        if !k.is_empty() {
            out.entry(k).or_insert(v);
        }
    }
    out
}

fn decode_pairs(bytes: &[u8]) -> Vec<(String, String)> {
    let s = String::from_utf8_lossy(bytes);
    s.split('&')
        .filter(|p| !p.is_empty())
        .map(|pair| match pair.split_once('=') {
            Some((k, v)) => (urldecode(k), urldecode(v)),
            None => (urldecode(pair), String::new()),
        })
        .collect()
}

fn urldecode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("");
                match u8::from_str_radix(hex, 16) {
                    Ok(b) => {
                        out.push(b);
                        i += 3;
                    }
                    Err(_) => {
                        out.push(bytes[i]);
                        i += 1;
                    }
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Parsed node encoding from shapes.json:
/// structure members as `[name, wireName, node]` triples; list node stores
/// its item shape in `i` (with the item's wire name), matches moto's
/// `QueryParser` exactly.
fn parse_node(node: &Value, values: &BTreeMap<String, String>, prefix: &str) -> Option<Value> {
    let t = node["t"].as_str().unwrap_or("");
    match t {
        "structure" => {
            if !prefix.is_empty() && !values.keys().any(|k| k.starts_with(prefix)) {
                return None;
            }
            let mut obj = Map::new();
            for m in node["m"].as_array()? {
                let name = m[0].as_str().unwrap_or_default();
                let wire = m[1].as_str().unwrap_or(name);
                let member_prefix = if prefix.is_empty() {
                    wire.to_string()
                } else {
                    format!("{prefix}.{wire}")
                };
                if let Some(v) = parse_node(&m[2], values, &member_prefix) {
                    obj.insert(name.to_string(), v);
                }
            }
            if obj.is_empty() {
                None
            } else {
                Some(Value::Object(obj))
            }
        }
        "list" => {
            if values.get(prefix).map(String::as_str) == Some("") {
                return Some(Value::Array(Vec::new()));
            }
            let item = &node["i"];
            let item_wire = item["n"].as_str().unwrap_or("member");
            let wire_prefix = format!("{prefix}.{item_wire}");
            let mut items = Vec::new();
            let mut i = 1;
            loop {
                let element_prefix = format!("{wire_prefix}.{i}");
                match parse_node(item, values, &element_prefix) {
                    Some(v) => items.push(v),
                    None => break,
                }
                i += 1;
            }
            if items.is_empty() {
                None
            } else {
                Some(Value::Array(items))
            }
        }
        "boolean" => {
            let v = values.get(prefix)?;
            Some(Value::Bool(v.eq_ignore_ascii_case("true")))
        }
        "integer" | "long" => {
            let v = values.get(prefix)?;
            Some(Value::Number(v.parse::<i64>().ok()?.into()))
        }
        "double" => {
            let v = values.get(prefix)?;
            let f = v.parse::<f64>().ok()?;
            Some(Value::Number(serde_json::Number::from_f64(f)?))
        }
        _ => values.get(prefix).map(|v| Value::String(v.clone())),
    }
}

/// Parse the request body into the input-shape tree for `op`. Unknown keys
/// are dropped (moto's parser is shape-driven the same way). Passthrough
/// scalars stay strings; booleans/ints already coerce per the shape types.
fn parse_input(req: &Request, op: &str) -> Value {
    let values = flat_values(req);
    let Some(shape) = SHAPES["ops"].get(op) else {
        return json!({});
    };
    let Some(members) = shape.get("in") else {
        return json!({});
    };
    parse_node(
        &json!({"t": "structure", "m": members.clone()}),
        &values,
        "",
    )
    .unwrap_or_else(|| json!({}))
}

// ---------------------------------------------------------------------------
// Value-tree accessors (moto dict semantics: keys are CamelCase)
// ---------------------------------------------------------------------------

fn s_opt(s: &Option<String>) -> Option<String> {
    s.clone()
}

fn js<'a>(v: &'a Value, k: &str) -> Option<&'a str> {
    v.get(k).and_then(Value::as_str).filter(|s| !s.is_empty())
}

fn j_str<'a>(v: &'a Value, k: &str) -> Option<&'a str> {
    v.get(k).and_then(Value::as_str)
}

fn j_bool_or(v: &Value, k: &str, default: bool) -> bool {
    v.get(k).and_then(Value::as_bool).unwrap_or(default)
}

fn j_int_or(v: &Value, k: &str, default: i64) -> i64 {
    v.get(k).and_then(Value::as_i64).unwrap_or(default)
}

fn j_list<'a>(v: &'a Value, k: &str) -> &'a [Value] {
    v.get(k)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tag {
    pub key: String,
    pub value: String,
}

/// list of {Key, Value} tag dicts (mot's raw parsed tags).
fn j_tags(v: &Value, k: &str) -> Vec<Tag> {
    j_list(v, k)
        .iter()
        .filter_map(|t| {
            Some(Tag {
                key: js(t, "Key")?.to_string(),
                value: js(t, "Value").unwrap_or("").to_string(),
            })
        })
        .collect()
}

fn tags_json(tags: &[Tag]) -> Value {
    Value::Array(
        tags.iter()
            .map(|t| json!({"Key": t.key, "Value": t.value}))
            .collect(),
    )
}

// ---------------------------------------------------------------------------
// Output renderer: moto QuerySerializer clone, driven by the output shape.
// ---------------------------------------------------------------------------

/// XML-escape a value interpolated into a response document.
fn esc(s: &str) -> String {
    crate::proto::xml_escape(s)
}

/// Python `str()` semantics for scalar leaf values (already escaped).
fn scalar_str(v: &Value) -> String {
    match v {
        Value::String(s) => esc(s),
        Value::Bool(b) => (if *b { "true" } else { "false" }).to_string(),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.to_string()
            } else if let Some(u) = n.as_u64() {
                u.to_string()
            } else if let Some(f) = n.as_f64() {
                py_float(f)
            } else {
                String::new()
            }
        }
        _ => String::new(),
    }
}

/// Render the members of a structure node against a value object.
fn render_members(members: &[Value], obj: &Value, out: &mut String) {
    for m in members {
        let name = m[0].as_str().unwrap_or_default();
        let wire = m[1].as_str().unwrap_or(name);
        let Some(v) = obj.get(name) else { continue };
        if v.is_null() {
            continue; // moto: member_value None → invisible in XML
        }
        let inner = match v {
            Value::Object(_) => {
                let mut s = String::new();
                render_members(
                    m[2]["m"].as_array().map(Vec::as_slice).unwrap_or(&[]),
                    v,
                    &mut s,
                );
                s
            }
            Value::Array(items) => {
                let item_tag = m[2]["i"]["n"].as_str().unwrap_or("member");
                let item_shape = &m[2]["i"];
                let mut s = String::new();
                for item in items {
                    let inner = match item {
                        Value::Object(_) => {
                            let mut s = String::new();
                            render_members(item_shape_members(item_shape), item, &mut s);
                            s
                        }
                        other => scalar_str(other),
                    };
                    if inner.is_empty() {
                        // moto: empty list items are dropped, empty strings
                        // render as `<X/>` inside the wrapper
                        if item.as_str() == Some("") {
                            s.push_str(&format!("<{item_tag}/>"));
                        }
                        continue;
                    }
                    s.push_str(&format!("<{item_tag}>{inner}</{item_tag}>"));
                }
                s
            }
            other => scalar_str(other),
        };
        if inner.is_empty() {
            out.push_str(&format!("<{wire}/>"));
        } else {
            out.push_str(&format!("<{wire}>{inner}</{wire}>"));
        }
    }
}

fn item_shape_members(shape: &Value) -> &[Value] {
    shape["m"].as_array().map(Vec::as_slice).unwrap_or(&[])
}

/// Shape members for op `op` ("in" | "out").
fn op_members<'a>(op: &str, side: &str) -> &'a [Value] {
    SHAPES["ops"][op][side]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

/// The XML body of a moto-style error document.
fn error_xml(status: u16, code: &str, message: &str) -> String {
    let _ = status;
    format!(
        "<ErrorResponse xmlns=\"{XMLNS}\"><Error><Type>Sender</Type><Code>{}</Code><Message>{}</Message></Error><RequestId>{}</RequestId></ErrorResponse>",
        esc(code),
        esc(message),
        request_id()
    )
}

fn xml_response(status: u16, body: String) -> Response {
    Response {
        status,
        headers: vec![
            (
                "Content-Type".to_string(),
                "text/xml; charset=utf-8".to_string(),
            ),
            ("x-amzn-requestid".to_string(), request_id()),
        ],
        body: body.into_bytes(),
    }
}

/// Success envelope: `<Op>Response xmlns=..><Op>Result>...` + metadata.
/// Value tree members carry the moto CamelCase keys; the shape drives order.
fn success(op: &str, value: &Value) -> Response {
    let mut result = String::new();
    render_members(op_members(op, "out"), value, &mut result);
    let meta = format!(
        "<ResponseMetadata><RequestId>{}</RequestId></ResponseMetadata>",
        request_id()
    );
    let result_xml = if result.is_empty() {
        format!("<{o}Result/>", o = op)
    } else {
        format!("<{o}Result>{r}</{o}Result>", o = op, r = result)
    };
    let body = format!(
        "<{o}Response xmlns=\"{XMLNS}\">{result_xml}{meta}</{o}Response>",
        o = op
    );
    xml_response(200, body)
}

/// Envelope for ops with no output shape (EmptyResult in moto).
fn empty_success(op: &str) -> Response {
    let body = format!(
        "<{o}Response xmlns=\"{XMLNS}\"><ResponseMetadata><RequestId>{rid}</RequestId></ResponseMetadata></{o}Response>",
        o = op,
        rid = request_id()
    );
    xml_response(200, body)
}

// ---------------------------------------------------------------------------
// Errors: status from the model's error shapes (moto honors those).
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct RSError {
    pub status: u16,
    pub code: String,
    pub message: String,
}

impl RSError {
    pub fn new(code: &str, message: impl Into<String>) -> Self {
        let status = SHAPES["error_codes"][code].as_u64().unwrap_or(400) as u16;
        Self {
            status,
            code: code.to_string(),
            message: message.into(),
        }
    }

    fn cluster_not_found(id: &str) -> Self {
        Self::new("ClusterNotFound", format!("Cluster {id} not found."))
    }

    fn snapshot_not_found(id: &str) -> Self {
        Self::new(
            "ClusterSnapshotNotFound",
            format!("Snapshot {id} not found."),
        )
    }

    fn subnet_group_not_found(id: &str) -> Self {
        Self::new(
            "ClusterSubnetGroupNotFoundFault",
            format!("Subnet group {id} not found."),
        )
    }

    fn sec_group_not_found(id: Option<&str>) -> Self {
        match id {
            Some(name) => Self::new(
                "ClusterSecurityGroupNotFound",
                format!("Security group {name} not found."),
            ),
            None => Self::new(
                "ClusterSecurityGroupNotFound",
                "The cluster security group name does not refer to an existing cluster security group.",
            ),
        }
    }

    fn param_group_not_found(id: &str) -> Self {
        Self::new(
            "ClusterParameterGroupNotFound",
            format!("ClusterParameterGroup not found: {id}"),
        )
    }

    fn invalid_subnet(ids: &[String]) -> Self {
        // moto renders the Python list repr: `Subnet ['a', 'b'] not found.`
        let list = ids
            .iter()
            .map(|i| format!("'{}'", i.replace('\'', "\\'")))
            .collect::<Vec<_>>()
            .join(", ");
        Self::new("InvalidSubnet", format!("Subnet [{}] not found.", list))
    }

    fn resource_not_found_arn(resource_type: &str, resource_id: &str) -> Self {
        Self::new(
            "ResourceNotFoundFault",
            format!("{resource_type} ({resource_id}) not found."),
        )
    }

    fn resource_not_found_type(resource_type: &str) -> Self {
        Self::new(
            "ResourceNotFoundFault",
            format!("resource of type '{resource_type}' not found."),
        )
    }

    fn invalid_parameter(message: impl Into<String>) -> Self {
        Self::new("InvalidParameterValue", message)
    }

    fn not_implemented(op: &str) -> Self {
        Self {
            status: 501,
            code: "NotImplemented".to_string(),
            message: format!("The {} action has not been implemented", snake_case(op)),
        }
    }

    fn wire(&self) -> Response {
        if self.status == 501 {
            // exactly the sidecar's `_error_response` for gap ops
            let body = format!(
                "<ErrorResponse><Error><Code>{}</Code><Message>{}</Message></Error></ErrorResponse>",
                esc(&self.code),
                esc(&self.message)
            );
            return Response {
                status: 501,
                headers: vec![("Content-Type".to_string(), "application/xml".to_string())],
                body: body.into_bytes(),
            };
        }
        xml_response(
            self.status,
            error_xml(self.status, &self.code, &self.message),
        )
    }
}

pub type OpResult = Result<Value, RSError>;

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

fn default_logging_details() -> Value {
    json!({
        "LoggingEnabled": false,
        "BucketName": "",
        "S3KeyPrefix": "",
        "LastSuccessfulDeliveryTime": now_iso(),
        "LastFailureTime": now_iso(),
        "LastFailureMessage": "",
        "LogDestinationType": "",
        "LogExports": [],
    })
}

#[derive(Clone, Debug)]
pub struct Cluster {
    pub cluster_identifier: String,
    pub unique_cluster_id: String,
    pub create_time: chrono::DateTime<chrono::Utc>,
    pub status: String,
    pub node_type: String,
    pub master_username: String,
    pub db_name: String,
    pub vpc_security_group_ids: Vec<String>,
    pub enhanced_vpc_routing: bool,
    pub cluster_subnet_group_name: Option<String>,
    pub publicly_accessible: bool,
    pub encrypted: bool,
    pub allow_version_upgrade: bool,
    pub cluster_version: String,
    pub port: i64,
    pub automated_snapshot_retention_period: i64,
    pub preferred_maintenance_window: String,
    pub cluster_parameter_group_name: Vec<String>,
    pub cluster_security_groups: Vec<String>,
    pub availability_zone: String,
    pub number_of_nodes: i64,
    pub iam_roles_arn: Vec<String>,
    pub restored_from_snapshot: bool,
    pub kms_key_id: Option<String>,
    pub cluster_snapshot_copy_status: Option<Value>,
    pub total_storage_capacity: i64,
    pub logging_details: Value,
    pub tags: Vec<Tag>,
}

impl Cluster {
    fn address(&self, region: &str) -> String {
        format!(
            "{}.{}.{}.redshift.amazonaws.com",
            self.cluster_identifier, self.unique_cluster_id, region
        )
    }

    /// moto `Cluster`'s rendered dict. `status_override` implements the
    /// sidecar's `CreateClusterResult.Cluster.ClusterStatus → "creating"`
    /// transformer on the create/restore paths. Group memberships filter
    /// against the backend's own stores exactly like moto's properties do.
    fn json(&self, st: &RedshiftState, region: &str, status_override: Option<&str>) -> Value {
        let sec_groups = Value::Array(
            self.cluster_security_groups
                .iter()
                .filter(|name| st.security_groups.contains_key(*name))
                .map(|name| json!({"ClusterSecurityGroupName": name, "Status": "active"}))
                .collect(),
        );
        let param_groups = Value::Array(
            self.cluster_parameter_group_name
                .iter()
                .filter(|name| st.parameter_groups.contains_key(*name))
                .map(|name| json!({"ParameterGroupName": name, "ParameterApplyStatus": "in-sync"}))
                .collect(),
        );
        json!({
            "ClusterIdentifier": self.cluster_identifier,
            "NodeType": self.node_type,
            "ClusterStatus": status_override.unwrap_or(&self.status),
            "MasterUsername": self.master_username,
            "DBName": self.db_name,
            "Endpoint": {
                "Address": self.address(region),
                "Port": self.port,
            },
            "ClusterCreateTime": iso_micro(self.create_time),
            "AutomatedSnapshotRetentionPeriod": self.automated_snapshot_retention_period,
            "ClusterSecurityGroups": sec_groups,
            "VpcSecurityGroups": self.vpc_sec_json(),
            "ClusterParameterGroups": param_groups,
            "ClusterSubnetGroupName": s_opt(&self.cluster_subnet_group_name),
            "AvailabilityZone": self.availability_zone,
            "PreferredMaintenanceWindow": self.preferred_maintenance_window,
            "PendingModifiedValues": [],
            "ClusterVersion": self.cluster_version,
            "AllowVersionUpgrade": self.allow_version_upgrade,
            "NumberOfNodes": self.number_of_nodes,
            "PubliclyAccessible": self.publicly_accessible,
            "Encrypted": self.encrypted,
            "RestoreStatus": self.restore_status_json(),
            "Tags": tags_json(&self.tags),
            "KmsKeyId": s_opt(&self.kms_key_id),
            "EnhancedVpcRouting": self.enhanced_vpc_routing,
            "IamRoles": Value::Array(
                self.iam_roles_arn
                    .iter()
                    .map(|arn| json!({"ApplyStatus": "in-sync", "IamRoleArn": arn}))
                    .collect(),
            ),
            "ClusterSnapshotCopyStatus": self.cluster_snapshot_copy_status.clone(),
            "TotalStorageCapacityInMegaBytes": self.total_storage_capacity,
        })
    }

    /// moto filters membership against its own EC2 security-group store; the
    /// suite never passes VPC security groups, so an empty list mirrors it.
    fn vpc_sec_json(&self) -> Value {
        Value::Array(
            self.vpc_security_group_ids
                .iter()
                .map(|id| json!({"VpcSecurityGroupId": id, "Status": "active"}))
                .collect(),
        )
    }

    fn restore_status_json(&self) -> Option<Value> {
        if !self.restored_from_snapshot {
            return None;
        }
        Some(json!({
            "Status": "completed",
            "CurrentRestoreRateInMegaBytesPerSecond": 123.0,
            "SnapshotSizeInMegaBytes": 123,
            "ProgressInMegaBytes": 123,
            "ElapsedTimeInSeconds": 123,
            "EstimatedTimeToCompletionInSeconds": 123,
        }))
    }
}

// --- static state seeds ------------------------------------------------------

fn seed_security_groups(account: u64, region: &str) -> BTreeMap<String, SecurityGroup> {
    let mut m = BTreeMap::new();
    m.insert(
        "Default".to_string(),
        SecurityGroup {
            cluster_security_group_name: "Default".to_string(),
            description: "Default Redshift Security Group".to_string(),
            account_id: account.to_string(),
            region: region.to_string(),
            tags: Vec::new(),
            ingress_rules: Vec::new(),
            ec2_security_groups: Vec::new(),
            ip_ranges: Vec::new(),
        },
    );
    m
}

fn seed_parameter_groups(account: u64, region: &str) -> BTreeMap<String, ParameterGroup> {
    let mut m = BTreeMap::new();
    m.insert(
        "default.redshift-1.0".to_string(),
        ParameterGroup {
            name: "default.redshift-1.0".to_string(),
            family: "redshift-1.0".to_string(),
            description: "Default Redshift parameter group".to_string(),
            account_id: account.to_string(),
            region: region.to_string(),
            tags: Vec::new(),
        },
    );
    m
}

#[derive(Clone, Debug)]
pub struct SecurityGroup {
    pub cluster_security_group_name: String,
    pub description: String,
    pub account_id: String,
    pub region: String,
    pub tags: Vec<Tag>,
    pub ingress_rules: Vec<String>,
    pub ec2_security_groups: Vec<String>,
    pub ip_ranges: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct ParameterGroup {
    pub name: String,
    pub family: String,
    pub description: String,
    pub account_id: String,
    pub region: String,
    pub tags: Vec<Tag>,
}

#[derive(Clone, Debug)]
pub struct SubnetGroup {
    pub cluster_subnet_group_name: String,
    pub description: String,
    pub subnet_ids: Vec<String>,
    pub account_id: String,
    pub region: String,
    pub tags: Vec<Tag>,
}

#[derive(Clone, Debug)]
pub struct Snapshot {
    pub snapshot_identifier: String,
    pub snapshot_type: String,
    pub status: String,
    pub create_time: chrono::DateTime<chrono::Utc>,
    pub iam_roles_arn: Vec<String>,
    pub cluster_identifier: String,
    pub port: i64,
    pub availability_zone: String,
    pub master_username: String,
    pub cluster_version: String,
    pub node_type: String,
    pub number_of_nodes: i64,
    pub db_name: String,
    pub enhanced_vpc_routing: bool,
    pub encrypted: bool,
    pub accounts_with_restore_access: Vec<Value>,
    pub tags: Vec<Tag>,
}

impl Snapshot {
    fn arn(&self, account: u64, region: &str) -> String {
        format!(
            "arn:aws:redshift:{region}:{account}:snapshot:{}/{}",
            self.cluster_identifier, self.snapshot_identifier
        )
    }

    /// moto `Snapshot` rendered dict (shape-ordered members carry the values).
    fn json(&self, account: u64, region: &str) -> Value {
        json!({
            "SnapshotIdentifier": self.snapshot_identifier,
            "ClusterIdentifier": self.cluster_identifier,
            "SnapshotCreateTime": iso_micro(self.create_time),
            "Status": self.status,
            "Port": self.port,
            "AvailabilityZone": self.availability_zone,
            "MasterUsername": self.master_username,
            "ClusterVersion": self.cluster_version,
            "SnapshotType": self.snapshot_type,
            "NodeType": self.node_type,
            "NumberOfNodes": self.number_of_nodes,
            "DBName": self.db_name,
            "Encrypted": self.encrypted,
            "AccountsWithRestoreAccess": Value::Array(self.accounts_with_restore_access.clone()),
            "Tags": tags_json(&self.tags),
            "EnhancedVpcRouting": self.enhanced_vpc_routing,
            "SnapshotArn": self.arn(account, region),
        })
    }
}

#[derive(Clone, Debug)]
pub struct SnapshotCopyGrant {
    pub snapshot_copy_grant_name: String,
    pub kms_key_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ScheduledAction {
    pub name: String,
    pub target_action: Value,
    pub schedule: String,
    pub iam_role: String,
    pub description: String,
    pub start_time: Option<String>,
    pub end_time: Option<String>,
    pub state: String,
}

impl ScheduledAction {
    fn json(&self) -> Value {
        json!({
            "ScheduledActionName": self.name,
            "TargetAction": self.target_action,
            "Schedule": self.schedule,
            "IamRole": self.iam_role,
            "ScheduledActionDescription": self.description,
            "State": self.state,
            "StartTime": s_opt(&self.start_time),
            "EndTime": s_opt(&self.end_time),
        })
    }
}

#[derive(Clone, Debug)]
pub struct AuthProfile {
    pub name: String,
    pub content: String,
}

#[derive(Clone, Debug)]
pub struct UsageLimit {
    pub usage_limit_id: String,
    pub cluster_identifier: String,
    pub feature_type: String,
    pub limit_type: String,
    pub amount: i64,
    pub period: String,
    pub breach_action: String,
    pub tags: Vec<Tag>,
}

impl UsageLimit {
    fn json(&self) -> Value {
        json!({
            "UsageLimitId": self.usage_limit_id,
            "ClusterIdentifier": self.cluster_identifier,
            "FeatureType": self.feature_type,
            "LimitType": self.limit_type,
            "Amount": self.amount,
            "Period": self.period,
            "BreachAction": self.breach_action,
            "Tags": tags_json(&self.tags),
        })
    }
}

#[derive(Clone, Debug)]
pub struct EndpointAuthorization {
    pub cluster_identifier: String,
    pub account: String,
    pub grantor: String,
    pub status: String,
    pub allowed_all_vpcs: bool,
    pub allowed_vpcs: Vec<String>,
}

impl EndpointAuthorization {
    fn json(&self) -> Value {
        json!({
            "ClusterIdentifier": self.cluster_identifier,
            "Grantor": self.grantor,
            "Grantee": self.account,
            "Status": self.status,
            "AllowedAllVPCs": self.allowed_all_vpcs,
            "AllowedVPCs": self.allowed_vpcs,
        })
    }
}

#[derive(Clone, Debug)]
pub struct EventSubscription {
    pub subscription_name: String,
    pub sns_topic_arn: String,
    pub account_id: String,
    pub region: String,
    pub source_type: Option<String>,
    pub source_ids: Vec<String>,
    pub event_categories: Vec<String>,
    pub severity: String,
    pub enabled: bool,
    pub status: String,
    pub subscription_creation_time: chrono::DateTime<chrono::Utc>,
    pub tags: Vec<Tag>,
}

impl EventSubscription {
    fn json(&self) -> Value {
        json!({
            "CustomerAwsId": self.account_id,
            "CustSubscriptionId": self.subscription_name,
            "SnsTopicArn": self.sns_topic_arn,
            "Status": self.status,
            "SubscriptionCreationTime": iso_micro(self.subscription_creation_time),
            "SourceType": s_opt(&self.source_type),
            "SourceIdsList": self.source_ids,
            "EventCategoriesList": self.event_categories,
            "Severity": self.severity,
            "Enabled": self.enabled,
            "Tags": tags_json(&self.tags),
        })
    }
}

#[derive(Clone, Debug)]
pub struct HsmClientCertificate {
    pub hsm_client_certificate_identifier: String,
    pub hsm_client_certificate_public_key: String,
    pub account_id: String,
    pub region: String,
    pub tags: Vec<Tag>,
}

#[derive(Clone, Debug)]
pub struct HsmConfiguration {
    pub hsm_configuration_identifier: String,
    pub description: String,
    pub hsm_ip_address: String,
    pub hsm_partition_name: String,
    pub account_id: String,
    pub region: String,
    pub tags: Vec<Tag>,
}

#[derive(Clone, Debug)]
pub struct EndpointAccess {
    pub cluster_identifier: String,
    pub resource_owner: String,
    pub endpoint_name: String,
    pub subnet_group_name: String,
    pub region_name: String,
    pub endpoint_status: String,
    pub endpoint_create_time: chrono::DateTime<chrono::Utc>,
    pub port: i64,
    pub address: String,
    pub vpc_security_groups: Vec<Value>,
    pub vpc_endpoint: Value,
}

impl EndpointAccess {
    fn json(&self) -> Value {
        json!({
            "ClusterIdentifier": self.cluster_identifier,
            "ResourceOwner": self.resource_owner,
            "SubnetGroupName": self.subnet_group_name,
            "EndpointStatus": self.endpoint_status,
            "EndpointName": self.endpoint_name,
            "EndpointCreateTime": iso_micro(self.endpoint_create_time),
            "Port": self.port,
            "Address": self.address,
            "VpcSecurityGroups": Value::Array(self.vpc_security_groups.clone()),
            "VpcEndpoint": self.vpc_endpoint,
        })
    }
}

#[derive(Clone, Debug)]
pub struct Partner {
    pub account_id: String,
    pub cluster_identifier: String,
    pub database_name: String,
    pub partner_name: String,
    pub status: String,
    pub status_message: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl Partner {
    fn json(&self) -> Value {
        json!({
            "DatabaseName": self.database_name,
            "PartnerName": self.partner_name,
            "Status": self.status,
            "StatusMessage": self.status_message,
            "CreatedAt": iso_micro(self.created_at),
        })
    }
}

#[derive(Default)]
pub struct RedshiftState {
    pub clusters: BTreeMap<String, Cluster>,
    pub subnet_groups: BTreeMap<String, SubnetGroup>,
    pub security_groups: BTreeMap<String, SecurityGroup>,
    pub parameter_groups: BTreeMap<String, ParameterGroup>,
    pub snapshots: BTreeMap<String, Snapshot>,
    pub snapshot_copy_grants: BTreeMap<String, SnapshotCopyGrant>,
    pub snapshot_schedules: BTreeMap<String, Value>,
    pub scheduled_actions: BTreeMap<String, ScheduledAction>,
    pub authentication_profiles: BTreeMap<String, AuthProfile>,
    pub usage_limits: BTreeMap<String, UsageLimit>,
    pub endpoint_authorizations: BTreeMap<String, EndpointAuthorization>,
    pub event_subscriptions: BTreeMap<String, EventSubscription>,
    pub hsm_client_certificates: BTreeMap<String, HsmClientCertificate>,
    pub hsm_configurations: BTreeMap<String, HsmConfiguration>,
    pub endpoint_access: BTreeMap<String, EndpointAccess>,
    pub partners: BTreeMap<String, Partner>,
    pub resource_policies: BTreeMap<String, Value>,
}

/// The moto `Default`/`default.redshift-1.0` seeds land on first use.
pub fn with_state<T>(account: u64, region: &str, f: impl FnOnce(&mut RedshiftState) -> T) -> T {
    let slot = store::slot::<RedshiftState>("redshift");
    let mut map = slot.write();
    let entry = map
        .entry((account, region.to_string()))
        .or_insert_with(|| RedshiftState {
            security_groups: seed_security_groups(account, region),
            parameter_groups: seed_parameter_groups(account, region),
            ..RedshiftState::default()
        });
    f(entry)
}

// moto's default parameter dict (engine-default source).
pub const DEFAULT_PARAMS: &[(&str, &str)] = &[
    ("auto_analyze", "true"),
    ("datestyle", "ISO, MDY"),
    ("enable_user_activity_logging", "false"),
    ("extra_float_digits", "0"),
    ("max_concurrency_scaling_clusters", "1"),
    ("query_group", "default"),
    ("require_ssl", "false"),
    ("search_path", "$user, public"),
    ("statement_timeout", "0"),
    ("wlm_json_configuration", "[{\"auto_wlm\":true}]"),
    ("use_fips_ssl", "false"),
];

fn default_params_json() -> Vec<Value> {
    DEFAULT_PARAMS
        .iter()
        .map(|(name, value)| {
            json!({
                "ParameterName": name,
                "ParameterValue": value,
                "Description": "mock parameter",
                "Source": "engine-default",
                "DataType": "type",
                "ApplyType": "static",
                "IsModifiable": true,
            })
        })
        .collect()
}

/// moto's AWS partition for a (redshift) region.
fn partition(region: &str) -> &str {
    if region.starts_with("cn-") {
        "aws-cn"
    } else if region.starts_with("us-gov-") {
        "aws-us-gov"
    } else if region.starts_with("us-iso-") {
        "aws-iso"
    } else if region.starts_with("us-isob-") {
        "aws-iso-b"
    } else {
        "aws"
    }
}

// ---------------------------------------------------------------------------
// EC2 subnet mirror: resolve subnet ids through the local gateway so subnet
// groups match moto's SubnetGroup (which reads moto's ec2_backend).
// ---------------------------------------------------------------------------

struct SubnetInfo {
    subnet_id: String,
    vpc_id: Option<String>,
    availability_zone: Option<String>,
}

fn fetch_subnets(req: &Request, ids: &[String]) -> BTreeMap<String, SubnetInfo> {
    let mut out = BTreeMap::new();
    if ids.is_empty() {
        return out;
    }
    let host = req
        .headers
        .get("host")
        .cloned()
        .unwrap_or_else(|| "127.0.0.1:4567".to_string());
    let mut body = String::from("Action=DescribeSubnets&Version=2016-11-15");
    for (i, id) in ids.iter().enumerate() {
        body.push_str(&format!("&SubnetIds.member.{}={}", i + 1, id));
    }
    let auth = format!(
        "AWS4-HMAC-SHA256 Credential={}/20240101/{}/ec2/aws4_request",
        req.account, req.region
    );
    let request = format!(
        "POST / HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/x-www-form-urlencoded\r\n\
         Authorization: {auth}\r\nX-Amz-Date: 20240101T000000Z\r\n\
         Content-Length: {len}\r\nConnection: close\r\n\r\n{body}",
        len = body.len(),
    );
    if let Ok(mut stream) = std::net::TcpStream::connect(&host) {
        use std::io::Read;
        let _ = std::io::Write::write_all(&mut stream, request.as_bytes());
        let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(3)));
        let mut buf = String::new();
        let _ = stream.read_to_string(&mut buf);
        let body = buf.split_once("\r\n\r\n").map(|(_, b)| b).unwrap_or(&buf);
        for chunk in body.split("<item>") {
            if let Some(id) = grab_tag(chunk, "subnetId") {
                out.insert(
                    id.clone(),
                    SubnetInfo {
                        subnet_id: id,
                        vpc_id: grab_tag(chunk, "vpcId"),
                        availability_zone: grab_tag(chunk, "availabilityZone"),
                    },
                );
            }
        }
    }
    out
}

fn grab_tag(text: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let start = text.find(&open)? + open.len();
    let rest = &text[start..];
    let end = rest.find(&format!("</{tag}>"))?;
    Some(rest[..end].to_string())
}

// ---------------------------------------------------------------------------
// Dispatch context
// ---------------------------------------------------------------------------

struct Ctx<'a> {
    req: &'a Request,
    input: &'a Value,
    account: u64,
    region: &'a str,
}

impl Ctx<'_> {}

struct CtxState<'a> {
    ctx: Ctx<'a>,
    st: &'a mut RedshiftState,
}

// ---------------------------------------------------------------------------
// Cluster lifecycle
// ---------------------------------------------------------------------------

impl<'a> CtxState<'a> {
    /// moto `RedshiftBackend.create_cluster` with the parsed kwargs; also
    /// creates the automated snapshot (`rs:{cid}-{YYYY-mm-dd-HH-MM}`)
    /// without tags. Returns the stored cluster.
    fn create_cluster(
        &mut self,
        cluster_identifier: &str,
        k: &ClusterKwargs,
        region_name: &str,
        restored_from_snapshot: bool,
        cluster_tags: Vec<Tag>,
    ) -> Result<Cluster, RSError> {
        if self.st.clusters.contains_key(cluster_identifier) {
            return Err(RSError::new(
                "ClusterAlreadyExists",
                "Cluster already exists",
            ));
        }
        let number_of_nodes = match k.cluster_type.as_deref() {
            Some("single-node") => 1,
            _ => k.number_of_nodes.unwrap_or(1),
        };
        let cluster = Cluster {
            cluster_identifier: cluster_identifier.to_string(),
            unique_cluster_id: rand_hex(12),
            create_time: chrono::Utc::now(),
            status: "available".to_string(),
            node_type: k.node_type.clone(),
            master_username: k.master_username.clone(),
            db_name: k.db_name.clone().unwrap_or_else(|| "dev".to_string()),
            vpc_security_group_ids: k.vpc_security_group_ids.clone(),
            enhanced_vpc_routing: k.enhanced_vpc_routing.unwrap_or(false),
            cluster_subnet_group_name: k.cluster_subnet_group_name.clone(),
            publicly_accessible: k.publicly_accessible.unwrap_or(false),
            encrypted: k.encrypted.unwrap_or(false),
            allow_version_upgrade: k.allow_version_upgrade.unwrap_or(true),
            cluster_version: k
                .cluster_version
                .clone()
                .unwrap_or_else(|| "1.0".to_string()),
            port: k.port.unwrap_or(5439),
            automated_snapshot_retention_period: k.automated_snapshot_retention_period.unwrap_or(1),
            preferred_maintenance_window: k
                .preferred_maintenance_window
                .clone()
                .unwrap_or_else(|| "Mon:03:00-Mon:03:30".to_string()),
            cluster_parameter_group_name: k
                .cluster_parameter_group_name
                .clone()
                .map(|n| vec![n])
                .unwrap_or_else(|| vec!["default.redshift-1.0".to_string()]),
            cluster_security_groups: if k.cluster_security_groups.is_empty() {
                vec!["Default".to_string()]
            } else {
                k.cluster_security_groups.clone()
            },
            availability_zone: k
                .availability_zone
                .clone()
                .unwrap_or_else(|| format!("{region_name}a")),
            number_of_nodes,
            iam_roles_arn: k.iam_roles_arn.clone(),
            restored_from_snapshot,
            kms_key_id: k.kms_key_id.clone(),
            cluster_snapshot_copy_status: None,
            total_storage_capacity: 0,
            logging_details: default_logging_details(),
            tags: cluster_tags,
        };
        // moto creates the automated snapshot right after the cluster.
        let auto_snapshot_id = format!(
            "rs:{}-{}",
            cluster_identifier,
            chrono::Utc::now().format("%Y-%m-%d-%H-%M")
        );
        let snapshot =
            self.build_snapshot(&cluster, auto_snapshot_id.clone(), Vec::new(), "automated");
        self.st.snapshots.insert(auto_snapshot_id, snapshot);
        self.st
            .clusters
            .insert(cluster_identifier.to_string(), cluster.clone());
        Ok(cluster)
    }

    fn build_snapshot(
        &self,
        cluster: &Cluster,
        snapshot_identifier: String,
        tags: Vec<Tag>,
        snapshot_type: &str,
    ) -> Snapshot {
        Snapshot {
            snapshot_identifier,
            snapshot_type: snapshot_type.to_string(),
            status: "available".to_string(),
            create_time: chrono::Utc::now(),
            iam_roles_arn: cluster.iam_roles_arn.clone(),
            cluster_identifier: cluster.cluster_identifier.clone(),
            port: cluster.port,
            availability_zone: cluster.availability_zone.clone(),
            master_username: cluster.master_username.clone(),
            cluster_version: cluster.cluster_version.clone(),
            node_type: cluster.node_type.clone(),
            number_of_nodes: cluster.number_of_nodes,
            db_name: cluster.db_name.clone(),
            enhanced_vpc_routing: cluster.enhanced_vpc_routing,
            encrypted: cluster.encrypted,
            accounts_with_restore_access: Vec::new(),
            tags,
        }
    }

    fn cluster_or_err(&self, cluster_identifier: &str) -> Result<&Cluster, RSError> {
        self.st
            .clusters
            .get(cluster_identifier)
            .ok_or_else(|| RSError::cluster_not_found(cluster_identifier))
    }
}

// ---------------------------------------------------------------------------
// Cluster kwargs — the parsed CreateCluster payload shared by create /
// restore / modify. The Option fields mean "not passed".
// ---------------------------------------------------------------------------

struct ClusterKwargs {
    node_type: String,
    master_username: String,
    db_name: Option<String>,
    cluster_type: Option<String>,
    cluster_security_groups: Vec<String>,
    vpc_security_group_ids: Vec<String>,
    cluster_subnet_group_name: Option<String>,
    availability_zone: Option<String>,
    preferred_maintenance_window: Option<String>,
    cluster_parameter_group_name: Option<String>,
    automated_snapshot_retention_period: Option<i64>,
    port: Option<i64>,
    cluster_version: Option<String>,
    allow_version_upgrade: Option<bool>,
    number_of_nodes: Option<i64>,
    publicly_accessible: Option<bool>,
    encrypted: Option<bool>,
    iam_roles_arn: Vec<String>,
    enhanced_vpc_routing: Option<bool>,
    kms_key_id: Option<String>,
    tags: Vec<Tag>,
}

/// `Vec<String>` out of an optional JSON array member; None when empty.
fn jlist_nonempty(v: Option<&Value>) -> Option<Vec<String>> {
    let out = strings(v);
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

/// `Vec<String>` out of an optional JSON array member.
fn strings(v: Option<&Value>) -> Vec<String> {
    v.and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|s| s.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

/// `Some` only when the wire carried the key (moto only sets passed keys).
fn opt_bool(i: &Value, k: &str) -> Option<bool> {
    i.get(k).and_then(Value::as_bool)
}

fn opt_int(i: &Value, k: &str) -> Option<i64> {
    i.get(k).and_then(Value::as_i64)
}

/// `CreateCluster` kwargs projection of a parsed input.
fn kwargs_from_input(i: &Value) -> ClusterKwargs {
    ClusterKwargs {
        node_type: js(i, "NodeType").unwrap_or_default().to_string(),
        master_username: js(i, "MasterUsername").unwrap_or_default().to_string(),
        db_name: j_str(i, "DBName").map(String::from),
        cluster_type: j_str(i, "ClusterType").map(String::from),
        cluster_security_groups: strings(i.get("ClusterSecurityGroups")),
        vpc_security_group_ids: strings(i.get("VpcSecurityGroupIds")),
        cluster_subnet_group_name: j_str(i, "ClusterSubnetGroupName").map(String::from),
        availability_zone: j_str(i, "AvailabilityZone").map(String::from),
        preferred_maintenance_window: j_str(i, "PreferredMaintenanceWindow").map(String::from),
        cluster_parameter_group_name: j_str(i, "ClusterParameterGroupName").map(String::from),
        automated_snapshot_retention_period: opt_int(i, "AutomatedSnapshotRetentionPeriod"),
        port: opt_int(i, "Port"),
        cluster_version: j_str(i, "ClusterVersion").map(String::from),
        allow_version_upgrade: opt_bool(i, "AllowVersionUpgrade"),
        number_of_nodes: opt_int(i, "NumberOfNodes"),
        publicly_accessible: opt_bool(i, "PubliclyAccessible"),
        encrypted: opt_bool(i, "Encrypted"),
        iam_roles_arn: strings(i.get("IamRoles")),
        enhanced_vpc_routing: opt_bool(i, "EnhancedVpcRouting"),
        kms_key_id: j_str(i, "KmsKeyId").map(String::from),
        tags: j_tags(i, "Tags"),
    }
}

// ---------------------------------------------------------------------------
// Cluster op handlers
// ---------------------------------------------------------------------------

/// `CreateCluster` — returns the stored cluster with ClusterStatus forced
/// to "creating" (the sidecar's RESPONSE_KEY_PATH transformer).
fn op_create_cluster(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let kwargs = kwargs_from_input(&input);
    let cid = js(&input, "ClusterIdentifier")
        .unwrap_or_default()
        .to_string();
    let cluster = cs.create_cluster(&cid, &kwargs, cs.ctx.region, false, kwargs.tags.clone())?;
    Ok(json!({"Cluster": cluster.json(cs.st, cs.ctx.region, Some("creating"))}))
}

/// `DescribeClusters` — moto `describe_clusters`.
fn op_describe_clusters(cs: &mut CtxState) -> OpResult {
    let tag_keys = strings(cs.ctx.input.get("TagKeys"));
    if let Some(cid) = js(cs.ctx.input, "ClusterIdentifier") {
        return cs
            .cluster_or_err(cid)
            .map(|c| json!({"Clusters": [c.json(cs.st, cs.ctx.region, None)]}));
    }
    let clusters = if tag_keys.is_empty() {
        cs.st
            .clusters
            .values()
            .map(|c| c.json(cs.st, cs.ctx.region, None))
            .collect::<Vec<_>>()
    } else {
        cs.st
            .clusters
            .values()
            .filter(|c| c.tags.iter().any(|t| tag_keys.contains(&t.key)))
            .map(|c| c.json(cs.st, cs.ctx.region, None))
            .collect::<Vec<_>>()
    };
    Ok(json!({"Clusters": clusters}))
}

/// Delete-cluster mechanics shared by DeleteCluster and ModifyCluster
/// (rename) — moto `RedshiftBackend.delete_cluster`.
fn delete_cluster_impl(
    cs: &mut CtxState,
    cluster_identifier: &str,
    skip_final_snapshot: bool,
    final_cluster_snapshot_identifier: Option<&str>,
) -> Result<Cluster, RSError> {
    let Some(cluster) = cs.st.clusters.get(cluster_identifier) else {
        return Err(RSError::cluster_not_found(cluster_identifier));
    };
    if !skip_final_snapshot && final_cluster_snapshot_identifier.is_none() {
        return Err(RSError::new(
            "InvalidParameterCombination",
            "FinalClusterSnapshotIdentifier is required unless SkipFinalClusterSnapshot is specified.",
        ));
    }
    let cluster = cluster.clone();
    if let Some(final_id) = final_cluster_snapshot_identifier {
        let snap = cs.build_snapshot(
            &cluster,
            final_id.to_string(),
            cluster.tags.clone(),
            "manual",
        );
        cs.st.snapshots.insert(final_id.to_string(), snap);
    }
    // drop automated snapshots for this cluster
    let automated: Vec<String> = cs
        .st
        .snapshots
        .values()
        .filter(|s| s.cluster_identifier == cluster_identifier && s.snapshot_type == "automated")
        .map(|s| s.snapshot_identifier.clone())
        .collect();
    for id in automated {
        cs.st.snapshots.remove(&id);
    }
    cs.st.clusters.remove(cluster_identifier);
    Ok(cluster)
}

/// `DeleteCluster` — moto `delete_cluster`.
fn op_delete_cluster(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let cid = js(&input, "ClusterIdentifier")
        .unwrap_or_default()
        .to_string();
    let skip = j_bool_or(&input, "SkipFinalClusterSnapshot", false);
    let final_id = j_str(&input, "FinalClusterSnapshotIdentifier");
    let cluster = delete_cluster_impl(cs, &cid, skip, final_id)?;
    Ok(json!({"Cluster": cluster.json(cs.st, cs.ctx.region, None)}))
}

/// `ModifyCluster` — only the passed (and non-empty) keys change the
/// cluster; single-node forces NumberOfNodes=1, multi-node requires >= 2.
fn op_modify_cluster(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let cid = js(&input, "ClusterIdentifier")
        .unwrap_or_default()
        .to_string();
    if let Some(ct) = j_str(&input, "ClusterType") {
        if ct != "multi-node" && ct != "single-node" {
            return Err(RSError::invalid_parameter(
                "Invalid cluster type. Cluster type can be one of multi-node or single-node",
            ));
        }
    }
    let mut number_of_nodes = opt_int(&input, "NumberOfNodes");
    if let Some("single-node") = j_str(&input, "ClusterType") {
        number_of_nodes = Some(1);
    }
    if let Some("multi-node") = j_str(&input, "ClusterType") {
        let nodes = number_of_nodes.unwrap_or(0);
        if nodes < 2 {
            return Err(RSError::new(
                "InvalidParameterCombination",
                "Number of nodes for cluster type multi-node must be greater than or equal to 2",
            ));
        }
    }
    let cluster = cs.cluster_or_err(&cid)?.clone();
    let mut cluster = cluster;
    if let Some(v) = j_str(&input, "NodeType") {
        cluster.node_type = v.to_string();
    }
    if let Some(v) = jlist_nonempty(input.get("ClusterSecurityGroups")) {
        cluster.cluster_security_groups = v;
    }
    if let Some(v) = jlist_nonempty(input.get("VpcSecurityGroupIds")) {
        cluster.vpc_security_group_ids = v;
    }
    if let Some(v) = j_str(&input, "ClusterSubnetGroupName") {
        cluster.cluster_subnet_group_name = Some(v.to_string());
    }
    if let Some(v) = j_str(&input, "PreferredMaintenanceWindow") {
        cluster.preferred_maintenance_window = v.to_string();
    }
    if let Some(v) = j_str(&input, "ClusterParameterGroupName") {
        cluster.cluster_parameter_group_name = vec![v.to_string()];
    }
    if let Some(v) = opt_int(&input, "AutomatedSnapshotRetentionPeriod") {
        cluster.automated_snapshot_retention_period = v;
    }
    if let Some(v) = j_str(&input, "ClusterVersion") {
        cluster.cluster_version = v.to_string();
    }
    if let Some(v) = opt_bool(&input, "AllowVersionUpgrade") {
        cluster.allow_version_upgrade = v;
    }
    if let Some(v) = number_of_nodes {
        cluster.number_of_nodes = v;
    }
    if let Some(v) = opt_bool(&input, "PubliclyAccessible") {
        cluster.publicly_accessible = v;
    }
    if let Some(v) = opt_bool(&input, "Encrypted") {
        cluster.encrypted = v;
    }
    if let Some(v) = jlist_nonempty(input.get("IamRoles")) {
        cluster.iam_roles_arn = v;
    }
    if let Some(v) = opt_bool(&input, "EnhancedVpcRouting") {
        cluster.enhanced_vpc_routing = v;
    }
    if let Some(new_id) = j_str(&input, "NewClusterIdentifier") {
        delete_cluster_impl(cs, &cid, true, None)?;
        cluster.cluster_identifier = new_id.to_string();
        cs.st.clusters.insert(new_id.to_string(), cluster.clone());
    } else {
        cs.st.clusters.insert(cid.clone(), cluster.clone());
    }
    Ok(json!({"Cluster": cluster.json(cs.st, cs.ctx.region, None)}))
}

/// `PauseCluster` / `ResumeCluster` / `RebootCluster` — status flips.
fn op_pause_cluster(cs: &mut CtxState) -> OpResult {
    let cid = js(cs.ctx.input, "ClusterIdentifier")
        .unwrap_or_default()
        .to_string();
    let cluster = cs.cluster_or_err(&cid)?.clone();
    let mut cluster = cluster;
    cluster.status = "paused".to_string();
    cs.st.clusters.insert(cid, cluster.clone());
    Ok(json!({"Cluster": cluster.json(cs.st, cs.ctx.region, None)}))
}

fn op_resume_cluster(cs: &mut CtxState) -> OpResult {
    let cid = js(cs.ctx.input, "ClusterIdentifier")
        .unwrap_or_default()
        .to_string();
    let cluster = cs.cluster_or_err(&cid)?.clone();
    let mut cluster = cluster;
    cluster.status = "available".to_string();
    cs.st.clusters.insert(cid, cluster.clone());
    Ok(json!({"Cluster": cluster.json(cs.st, cs.ctx.region, None)}))
}

fn op_reboot_cluster(cs: &mut CtxState) -> OpResult {
    let cid = js(cs.ctx.input, "ClusterIdentifier")
        .unwrap_or_default()
        .to_string();
    let cluster = cs.cluster_or_err(&cid)?.clone();
    let mut cluster = cluster;
    cluster.status = "available".to_string();
    cs.st.clusters.insert(cid, cluster.clone());
    Ok(json!({"Cluster": cluster.json(cs.st, cs.ctx.region, None)}))
}

fn op_rotate_encryption_key(cs: &mut CtxState) -> OpResult {
    let cid = js(cs.ctx.input, "ClusterIdentifier")
        .unwrap_or_default()
        .to_string();
    let cluster = cs.cluster_or_err(&cid)?.clone();
    Ok(json!({"Cluster": cluster.json(cs.st, cs.ctx.region, None)}))
}

/// `ModifyClusterIamRoles` — add/remove IAM roles from a cluster.
fn op_modify_cluster_iam_roles(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let cid = js(&input, "ClusterIdentifier")
        .unwrap_or_default()
        .to_string();
    let cluster = cs.cluster_or_err(&cid)?.clone();
    let mut cluster = cluster;
    let remove = strings(input.get("RemoveIamRoles"));
    if !remove.is_empty() {
        cluster.iam_roles_arn.retain(|r| !remove.contains(r));
    }
    for role in strings(input.get("AddIamRoles")) {
        if !cluster.iam_roles_arn.contains(&role) {
            cluster.iam_roles_arn.push(role);
        }
    }
    cs.st.clusters.insert(cid, cluster.clone());
    Ok(json!({"Cluster": cluster.json(cs.st, cs.ctx.region, None)}))
}

/// No-op mutators that validate the cluster exists (moto returns the
/// unchanged cluster; none of these mutate suite-visible state).
fn op_cluster_passthrough(cs: &mut CtxState) -> OpResult {
    let cid = js(cs.ctx.input, "ClusterIdentifier")
        .unwrap_or_default()
        .to_string();
    let cluster = cs.cluster_or_err(&cid)?.clone();
    Ok(json!({"Cluster": cluster.json(cs.st, cs.ctx.region, None)}))
}

/// `ModifyAquaConfiguration` — moto stub `{"AquaConfiguration": ...}`.
fn op_modify_aqua_configuration(cs: &mut CtxState) -> OpResult {
    let cid = js(cs.ctx.input, "ClusterIdentifier")
        .unwrap_or_default()
        .to_string();
    cs.cluster_or_err(&cid)?;
    let status = js(cs.ctx.input, "AquaConfigurationStatus").unwrap_or("auto");
    Ok(json!({
        "AquaConfiguration": {
            "AquaStatus": if status == "enabled" { "enabled" } else { "disabled" },
            "AquaConfigurationStatus": status,
        }
    }))
}

/// `DescribeResize` / `CancelResize` — moto resize stub for a live cluster.
fn op_resize_progress(cs: &mut CtxState) -> OpResult {
    let cid = js(cs.ctx.input, "ClusterIdentifier")
        .unwrap_or_default()
        .to_string();
    let cluster = cs.cluster_or_err(&cid)?;
    let cluster_type = if cluster.number_of_nodes > 1 {
        "multi-node"
    } else {
        "single-node"
    };
    Ok(json!({
        "TargetNodeType": cluster.node_type,
        "TargetNumberOfNodes": cluster.number_of_nodes,
        "TargetClusterType": cluster_type,
        "Status": "NONE",
        "ResizeType": "ClassicResize",
    }))
}

// ---------------------------------------------------------------------------
// Subnet group ops (subnet existence checks run through the EC2 mirror)
// ---------------------------------------------------------------------------

impl<'a> CtxState<'a> {
    /// moto `SubnetGroup.json`'s value dict — subnets resolve lazily.
    fn subnet_group_json(&self, group: &SubnetGroup) -> Value {
        let ids = group.subnet_ids.clone();
        let resolved = fetch_subnets(self.ctx.req, &ids);
        let subnets: Vec<Value> = ids
            .iter()
            .filter_map(|id| resolved.get(id))
            .map(|s| {
                json!({
                    "SubnetStatus": "Active",
                    "SubnetIdentifier": s.subnet_id,
                    "SubnetAvailabilityZone": {"Name": s.availability_zone},
                })
            })
            .collect();
        let vpc_id = resolved.values().next().and_then(|s| s.vpc_id.clone());
        json!({
            "ClusterSubnetGroupName": group.cluster_subnet_group_name,
            "Description": group.description,
            "VpcId": vpc_id,
            "SubnetGroupStatus": "Complete",
            "Subnets": subnets,
            "Tags": tags_json(&group.tags),
        })
    }

    /// Validate that at least one subnet id resolves (moto's `InvalidSubnet`
    /// check in `SubnetGroup.__init__`).
    fn validate_subnets(
        &self,
        subnet_ids: &[String],
    ) -> Result<BTreeMap<String, SubnetInfo>, RSError> {
        let resolved = fetch_subnets(self.ctx.req, subnet_ids);
        if resolved.is_empty() {
            return Err(RSError::invalid_subnet(subnet_ids));
        }
        Ok(resolved)
    }
}

fn op_create_cluster_subnet_group(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let name = js(&input, "ClusterSubnetGroupName")
        .unwrap_or_default()
        .to_string();
    let description = j_str(&input, "Description").unwrap_or_default().to_string();
    let subnet_ids = strings(input.get("SubnetIds"));
    let region = cs.ctx.region.to_string();
    let account = cs.ctx.account.to_string();
    let tags = j_tags(&input, "Tags");
    let _ = cs.validate_subnets(&subnet_ids)?;
    let group = SubnetGroup {
        cluster_subnet_group_name: name.clone(),
        description,
        subnet_ids,
        account_id: account,
        region,
        tags,
    };
    let json = cs.subnet_group_json(&group);
    cs.st.subnet_groups.insert(name, group);
    Ok(json!({"ClusterSubnetGroup": json}))
}

fn op_describe_cluster_subnet_groups(cs: &mut CtxState) -> OpResult {
    if let Some(name) = js(cs.ctx.input, "ClusterSubnetGroupName") {
        let Some(group) = cs.st.subnet_groups.get(name) else {
            return Err(RSError::subnet_group_not_found(name));
        };
        return Ok(json!({"ClusterSubnetGroups": [cs.subnet_group_json(group)]}));
    }
    let groups = cs
        .st
        .subnet_groups
        .values()
        .map(|g| cs.subnet_group_json(g))
        .collect::<Vec<_>>();
    Ok(json!({"ClusterSubnetGroups": groups}))
}

fn op_delete_cluster_subnet_group(cs: &mut CtxState) -> OpResult {
    let name = js(cs.ctx.input, "ClusterSubnetGroupName")
        .unwrap_or_default()
        .to_string();
    if cs.st.subnet_groups.remove(&name).is_none() {
        return Err(RSError::subnet_group_not_found(&name));
    }
    Ok(Value::Null)
}

fn op_modify_cluster_subnet_group(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let name = js(&input, "ClusterSubnetGroupName")
        .unwrap_or_default()
        .to_string();
    let Some(group) = cs.st.subnet_groups.get(&name) else {
        return Err(RSError::subnet_group_not_found(&name));
    };
    let mut group = group.clone();
    if let Some(description) = j_str(&input, "Description") {
        group.description = description.to_string();
    }
    if input.get("SubnetIds").is_some() {
        let subnet_ids = strings(input.get("SubnetIds"));
        if input.get("SubnetIds").and_then(Value::as_str) == Some("") {
            // `SubnetIds=` (empty marker) means an empty replacement list
        } else {
            cs.validate_subnets(&subnet_ids)?;
        }
        group.subnet_ids = subnet_ids;
    }
    let json = cs.subnet_group_json(&group);
    cs.st.subnet_groups.insert(name, group);
    Ok(json!({"ClusterSubnetGroup": json}))
}

// ---------------------------------------------------------------------------
// Security group ops
// ---------------------------------------------------------------------------

fn sec_group_json(sg: &SecurityGroup) -> Value {
    // moto renders `ip_ranges` (plain CIDR strings) through the IPRange
    // struct shape, which drops every item — the wire always shows the
    // empty wrapper here (verified against the sidecar).
    json!({
        "ClusterSecurityGroupName": sg.cluster_security_group_name,
        "Description": sg.description,
        "EC2SecurityGroups": [],
        "IPRanges": [],
        "Tags": tags_json(&sg.tags),
    })
}

fn op_create_cluster_security_group(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let name = js(&input, "ClusterSecurityGroupName")
        .unwrap_or_default()
        .to_string();
    let description = j_str(&input, "Description").unwrap_or_default().to_string();
    let sg = SecurityGroup {
        cluster_security_group_name: name.clone(),
        description: description.clone(),
        account_id: cs.ctx.account.to_string(),
        region: cs.ctx.region.to_string(),
        tags: j_tags(&input, "Tags"),
        ingress_rules: Vec::new(),
        ec2_security_groups: Vec::new(),
        ip_ranges: Vec::new(),
    };
    cs.st.security_groups.insert(name.clone(), sg);
    Ok(json!({
        "ClusterSecurityGroup": {
            "ClusterSecurityGroupName": name,
            "Description": description,
            "EC2SecurityGroups": [],
            "IPRanges": [],
            "Tags": tags_json(&j_tags(&input, "Tags")),
        }
    }))
}

fn op_describe_cluster_security_groups(cs: &mut CtxState) -> OpResult {
    if let Some(name) = js(cs.ctx.input, "ClusterSecurityGroupName") {
        let Some(sg) = cs.st.security_groups.get(name) else {
            return Err(RSError::sec_group_not_found(Some(name)));
        };
        return Ok(json!({"ClusterSecurityGroups": [sec_group_json(sg)]}));
    }
    let groups = cs
        .st
        .security_groups
        .values()
        .map(sec_group_json)
        .collect::<Vec<_>>();
    Ok(json!({"ClusterSecurityGroups": groups}))
}

fn op_delete_cluster_security_group(cs: &mut CtxState) -> OpResult {
    let name = js(cs.ctx.input, "ClusterSecurityGroupName")
        .unwrap_or_default()
        .to_string();
    if cs.st.security_groups.remove(&name).is_none() {
        return Err(RSError::sec_group_not_found(Some(&name)));
    }
    Ok(Value::Null)
}

/// `AuthorizeClusterSecurityGroupIngress` — the response is moto's literal
/// dict (real `IPRange` structs, unlike the describe serialization bug).
fn op_authorize_cluster_security_group_ingress(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let name = js(&input, "ClusterSecurityGroupName")
        .unwrap_or_default()
        .to_string();
    let Some(sg) = cs.st.security_groups.get(&name) else {
        return Err(RSError::sec_group_not_found(None));
    };
    let mut sg = sg.clone();
    let cidr = j_str(&input, "CIDRIP").unwrap_or_default().to_string();
    sg.ingress_rules.push(cidr.clone());
    cs.st.security_groups.insert(name.clone(), sg);
    Ok(json!({
        "ClusterSecurityGroup": {
            "ClusterSecurityGroupName": name,
            "Description": cs.st.security_groups[&name].description,
            "IPRanges": [{
                "Status": "authorized",
                "CIDRIP": cidr,
                "Tags": tags_json(&cs.st.security_groups[&name].tags),
            }],
        }
    }))
}

fn op_revoke_cluster_security_group_ingress(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let name = js(&input, "ClusterSecurityGroupName")
        .unwrap_or_default()
        .to_string();
    let Some(sg) = cs.st.security_groups.get(&name) else {
        return Err(RSError::sec_group_not_found(None));
    };
    let mut sg = sg.clone();
    if let Some(cidr) = j_str(&input, "CIDRIP") {
        sg.ingress_rules.retain(|r| r != cidr);
    }
    cs.st.security_groups.insert(name.clone(), sg.clone());
    Ok(json!({"ClusterSecurityGroup": sec_group_json(&sg)}))
}

// ---------------------------------------------------------------------------
// Parameter group ops
// ---------------------------------------------------------------------------

fn param_group_json(pg: &ParameterGroup) -> Value {
    json!({
        "ParameterGroupName": pg.name,
        "ParameterGroupFamily": pg.family,
        "Description": pg.description,
        "Tags": tags_json(&pg.tags),
    })
}

fn op_create_cluster_parameter_group(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let name = js(&input, "ParameterGroupName")
        .unwrap_or_default()
        .to_string();
    let family = j_str(&input, "ParameterGroupFamily")
        .unwrap_or_default()
        .to_string();
    let description = j_str(&input, "Description").unwrap_or_default().to_string();
    let pg = ParameterGroup {
        name: name.clone(),
        family: family.clone(),
        description: description.clone(),
        account_id: cs.ctx.account.to_string(),
        region: cs.ctx.region.to_string(),
        tags: j_tags(&input, "Tags"),
    };
    cs.st.parameter_groups.insert(name.clone(), pg);
    Ok(json!({
        "ClusterParameterGroup": {
            "ParameterGroupName": name,
            "ParameterGroupFamily": family,
            "Description": description,
            "Tags": tags_json(&j_tags(&input, "Tags")),
        }
    }))
}

fn op_describe_cluster_parameter_groups(cs: &mut CtxState) -> OpResult {
    if let Some(name) = js(cs.ctx.input, "ParameterGroupName") {
        let Some(pg) = cs.st.parameter_groups.get(name) else {
            return Err(RSError::param_group_not_found(name));
        };
        return Ok(json!({"ParameterGroups": [param_group_json(pg)]}));
    }
    let groups = cs
        .st
        .parameter_groups
        .values()
        .map(param_group_json)
        .collect::<Vec<_>>();
    Ok(json!({"ParameterGroups": groups}))
}

fn op_delete_cluster_parameter_group(cs: &mut CtxState) -> OpResult {
    let name = js(cs.ctx.input, "ParameterGroupName")
        .unwrap_or_default()
        .to_string();
    if cs.st.parameter_groups.remove(&name).is_none() {
        return Err(RSError::param_group_not_found(&name));
    }
    Ok(Value::Null)
}

fn op_describe_cluster_parameters(cs: &mut CtxState) -> OpResult {
    let name = js(cs.ctx.input, "ParameterGroupName").unwrap_or_default();
    if !cs.st.parameter_groups.contains_key(name) {
        return Err(RSError::param_group_not_found(name));
    }
    Ok(json!({"Parameters": default_params_json()}))
}

fn op_describe_default_cluster_parameters(cs: &mut CtxState) -> OpResult {
    let family = j_str(cs.ctx.input, "ParameterGroupFamily").unwrap_or_default();
    Ok(json!({
        "DefaultClusterParameters": {
            "ParameterGroupFamily": family,
            "Parameters": default_params_json(),
        }
    }))
}

/// `ModifyClusterParameterGroup` / `ResetClusterParameterGroup`.
fn op_modify_cluster_parameter_group(cs: &mut CtxState) -> OpResult {
    let name = js(cs.ctx.input, "ParameterGroupName")
        .unwrap_or_default()
        .to_string();
    if !cs.st.parameter_groups.contains_key(&name) {
        return Err(RSError::param_group_not_found(&name));
    }
    Ok(json!({
        "ParameterGroupName": name,
        "ParameterGroupStatus": "Your parameter group has been updated.",
    }))
}

// ---------------------------------------------------------------------------
// Snapshot ops
// ---------------------------------------------------------------------------

/// `CreateClusterSnapshot` — moto `create_cluster_snapshot`.
fn op_create_cluster_snapshot(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let cid = js(&input, "ClusterIdentifier")
        .unwrap_or_default()
        .to_string();
    let snap_id = js(&input, "SnapshotIdentifier")
        .unwrap_or_default()
        .to_string();
    let tags = j_tags(&input, "Tags");
    let snapshot = cs.create_snapshot(&cid, snap_id.clone(), tags, "manual")?;
    Ok(json!({"Snapshot": cs.snapshot_json(&snapshot)}))
}

impl<'a> CtxState<'a> {
    /// moto `create_cluster_snapshot` — atomic create with duplicates check.
    fn create_snapshot(
        &mut self,
        cluster_identifier: &str,
        snapshot_identifier: String,
        tags: Vec<Tag>,
        snapshot_type: &str,
    ) -> Result<Snapshot, RSError> {
        if self.st.snapshots.contains_key(&snapshot_identifier) {
            return Err(RSError::new(
                "ClusterSnapshotAlreadyExists",
                format!(
                    "Cannot create the snapshot because a snapshot with the identifier {snapshot_identifier} already exists"
                ),
            ));
        }
        let cluster = self.cluster_or_err(cluster_identifier)?.clone();
        let snapshot =
            self.build_snapshot(&cluster, snapshot_identifier.clone(), tags, snapshot_type);
        self.st
            .snapshots
            .insert(snapshot_identifier, snapshot.clone());
        Ok(snapshot)
    }

    /// moto `describe_cluster_snapshots` — the odd fall-through semantics
    /// are preserved exactly.
    fn describe_snapshots_by(
        &self,
        cluster_identifier: Option<&str>,
        snapshot_identifier: Option<&str>,
        snapshot_type: Option<&str>,
    ) -> Result<Vec<Snapshot>, RSError> {
        let snapshot_types: Vec<&str> = match snapshot_type {
            Some(t) => vec![t],
            None => vec!["automated", "manual"],
        };
        if let Some(cid) = cluster_identifier {
            let mut found = Vec::new();
            for s in self.st.snapshots.values() {
                if s.cluster_identifier == cid && snapshot_types.contains(&s.snapshot_type.as_str())
                {
                    found.push(s.clone());
                }
            }
            if !found.is_empty() {
                return Ok(found);
            }
        }
        if let Some(sid) = snapshot_identifier {
            if let Some(s) = self.st.snapshots.get(sid) {
                if snapshot_types.contains(&s.snapshot_type.as_str()) {
                    return Ok(vec![s.clone()]);
                }
            }
            return Err(RSError::snapshot_not_found(
                snapshot_identifier.unwrap_or(sid),
            ));
        }
        Ok(self.st.snapshots.values().cloned().collect())
    }

    fn snapshot_json(&self, s: &Snapshot) -> Value {
        s.json(self.ctx.account, self.ctx.region)
    }
}

fn op_describe_cluster_snapshots(cs: &mut CtxState) -> OpResult {
    let snapshots = cs.describe_snapshots_by(
        js(cs.ctx.input, "ClusterIdentifier"),
        js(cs.ctx.input, "SnapshotIdentifier"),
        js(cs.ctx.input, "SnapshotType"),
    )?;
    Ok(json!({
        "Snapshots": snapshots.iter().map(|s| cs.snapshot_json(s)).collect::<Vec<_>>(),
    }))
}

fn op_delete_cluster_snapshot(cs: &mut CtxState) -> OpResult {
    let snap_id = js(cs.ctx.input, "SnapshotIdentifier")
        .unwrap_or_default()
        .to_string();
    let Some(snapshot) = cs.st.snapshots.get(&snap_id) else {
        return Err(RSError::snapshot_not_found(&snap_id));
    };
    if snapshot.snapshot_type == "automated" {
        return Err(RSError::new(
            "InvalidClusterSnapshotState",
            format!(
                "Cannot delete the snapshot {snap_id} because only manual snapshots may be deleted"
            ),
        ));
    }
    let mut snapshot = snapshot.clone();
    snapshot.status = "deleted".to_string();
    cs.st.snapshots.remove(&snap_id);
    Ok(json!({"Snapshot": cs.snapshot_json(&snapshot)}))
}

/// `RestoreFromClusterSnapshot` — moto `restore_from_cluster_snapshot`.
fn op_restore_from_cluster_snapshot(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let cid = js(&input, "ClusterIdentifier")
        .unwrap_or_default()
        .to_string();
    let snap_id = js(&input, "SnapshotIdentifier")
        .unwrap_or_default()
        .to_string();
    let snapshot = cs
        .describe_snapshots_by(None, Some(snap_id.as_str()), None)?
        .into_iter()
        .next()
        .ok_or_else(|| RSError::snapshot_not_found(&snap_id))?;
    let kwargs = ClusterKwargs {
        node_type: j_str(&input, "NodeType")
            .unwrap_or(snapshot.node_type.as_str())
            .to_string(),
        master_username: snapshot.master_username.clone(),
        db_name: Some(snapshot.db_name.clone()),
        cluster_type: Some(
            if snapshot.number_of_nodes > 1 {
                "multi-node"
            } else {
                "single-node"
            }
            .to_string(),
        ),
        cluster_security_groups: strings(input.get("ClusterSecurityGroups")),
        vpc_security_group_ids: strings(input.get("VpcSecurityGroupIds")),
        cluster_subnet_group_name: j_str(&input, "ClusterSubnetGroupName").map(String::from),
        availability_zone: Some(snapshot.availability_zone.clone()),
        preferred_maintenance_window: j_str(&input, "PreferredMaintenanceWindow").map(String::from),
        cluster_parameter_group_name: j_str(&input, "ClusterParameterGroupName").map(String::from),
        automated_snapshot_retention_period: opt_int(&input, "AutomatedSnapshotRetentionPeriod"),
        port: opt_int(&input, "Port").or(Some(snapshot.port)),
        cluster_version: Some(snapshot.cluster_version.clone()),
        allow_version_upgrade: opt_bool(&input, "AllowVersionUpgrade"),
        number_of_nodes: opt_int(&input, "NumberOfNodes").or(Some(snapshot.number_of_nodes)),
        publicly_accessible: j_str(&input, "PubliclyAccessible").and_then(|v| v.parse().ok()),
        encrypted: Some(snapshot.encrypted),
        iam_roles_arn: strings(input.get("IamRoles")),
        enhanced_vpc_routing: opt_bool(&input, "EnhancedVpcRouting"),
        kms_key_id: j_str(&input, "KmsKeyId").map(String::from),
        tags: snapshot.tags.clone(),
    };
    let cluster = cs.create_cluster(&cid, &kwargs, cs.ctx.region, true, snapshot.tags.clone())?;
    Ok(json!({"Cluster": cluster.json(cs.st, cs.ctx.region, Some("creating"))}))
}

/// `CopyClusterSnapshot` — moto copies the source snapshot metadata.
fn op_copy_cluster_snapshot(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let source = js(&input, "SourceSnapshotIdentifier")
        .unwrap_or_default()
        .to_string();
    let target = js(&input, "TargetSnapshotIdentifier")
        .unwrap_or_default()
        .to_string();
    let Some(source_snapshot) = cs.st.snapshots.get(&source).cloned() else {
        return Err(RSError::snapshot_not_found(&source));
    };
    if cs.st.snapshots.contains_key(&target) {
        return Err(RSError::new(
            "ClusterSnapshotAlreadyExists",
            format!(
                "Cannot create the snapshot because a snapshot with the identifier {target} already exists"
            ),
        ));
    }
    let mut copy = source_snapshot.clone();
    copy.snapshot_identifier = target.clone();
    copy.create_time = chrono::Utc::now();
    cs.st.snapshots.insert(target, copy.clone());
    Ok(json!({"Snapshot": cs.snapshot_json(&copy)}))
}

fn op_authorize_snapshot_access(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let snap_id = js(&input, "SnapshotIdentifier")
        .unwrap_or_default()
        .to_string();
    let account = j_str(&input, "AccountWithRestoreAccess")
        .unwrap_or_default()
        .to_string();
    let Some(snapshot) = cs.st.snapshots.get(&snap_id) else {
        return Err(RSError::snapshot_not_found(&snap_id));
    };
    let mut snapshot = snapshot.clone();
    if !snapshot
        .accounts_with_restore_access
        .iter()
        .any(|a| a.get("AccountId").and_then(Value::as_str) == Some(account.as_str()))
    {
        snapshot_accounts_insert(&mut snapshot, &account);
    }
    cs.st.snapshots.insert(snap_id, snapshot.clone());
    Ok(json!({"Snapshot": cs.snapshot_json(&snapshot)}))
}

fn snapshot_accounts_insert(snapshot: &mut Snapshot, account: &str) {
    snapshot.accounts_with_restore_access.push(json!({
        "AccountId": account,
        "AccountAlias": "",
    }));
}

fn op_revoke_snapshot_access(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let snap_id = js(&input, "SnapshotIdentifier")
        .unwrap_or_default()
        .to_string();
    let account = j_str(&input, "AccountWithRestoreAccess")
        .unwrap_or_default()
        .to_string();
    let Some(snapshot) = cs.st.snapshots.get(&snap_id) else {
        return Err(RSError::snapshot_not_found(&snap_id));
    };
    let mut snapshot = snapshot.clone();
    snapshot
        .accounts_with_restore_access
        .retain(|a| a.get("AccountId").and_then(Value::as_str) != Some(account.as_str()));
    cs.st.snapshots.insert(snap_id, snapshot.clone());
    Ok(json!({"Snapshot": cs.snapshot_json(&snapshot)}))
}

/// `ModifyClusterSnapshot` — moto returns the snapshot unchanged.
fn op_modify_cluster_snapshot(cs: &mut CtxState) -> OpResult {
    let snap_id = js(cs.ctx.input, "SnapshotIdentifier")
        .unwrap_or_default()
        .to_string();
    let Some(snapshot) = cs.st.snapshots.get(&snap_id) else {
        return Err(RSError::snapshot_not_found(&snap_id));
    };
    Ok(json!({"Snapshot": cs.snapshot_json(snapshot)}))
}

/// `BatchDeleteClusterSnapshots` / `BatchModifyClusterSnapshots`.
fn op_batch_delete_cluster_snapshots(cs: &mut CtxState) -> OpResult {
    let identifiers = j_list(cs.ctx.input, "Identifiers")
        .iter()
        .filter_map(|i| js(i, "SnapshotIdentifier").map(String::from))
        .collect::<Vec<_>>();
    let mut deleted: Vec<Value> = Vec::new();
    let mut errors: Vec<Value> = Vec::new();
    for snap_id in identifiers {
        if cs.st.snapshots.remove(&snap_id).is_some() {
            deleted.push(json!(snap_id));
        } else {
            errors.push(json!({
                "SnapshotIdentifier": snap_id,
                "ErrorCode": "ClusterSnapshotNotFound",
                "ErrorMessage": format!("Snapshot {snap_id} not found."),
            }));
        }
    }
    let mut result = json!({"Resources": deleted});
    if errors.is_empty() {
        result.as_object_mut().unwrap().remove("Errors");
    } else {
        result["Errors"] = Value::Array(errors);
    }
    Ok(result)
}

fn op_batch_modify_cluster_snapshots(cs: &mut CtxState) -> OpResult {
    let snap_ids = strings(cs.ctx.input.get("SnapshotIdentifierList"));
    let mut modified: Vec<Value> = Vec::new();
    let mut errors: Vec<Value> = Vec::new();
    for snap_id in snap_ids {
        if cs.st.snapshots.contains_key(&snap_id) {
            modified.push(json!(snap_id));
        } else {
            errors.push(json!({
                "SnapshotIdentifier": snap_id,
                "ErrorCode": "ClusterSnapshotNotFound",
                "ErrorMessage": format!("Snapshot {snap_id} not found."),
            }));
        }
    }
    let mut result = json!({"Resources": modified});
    if !errors.is_empty() {
        result["Errors"] = Value::Array(errors);
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// Snapshot copy grants
// ---------------------------------------------------------------------------

fn op_create_snapshot_copy_grant(cs: &mut CtxState) -> OpResult {
    let name = js(cs.ctx.input, "SnapshotCopyGrantName")
        .unwrap_or_default()
        .to_string();
    if cs.st.snapshot_copy_grants.contains_key(&name) {
        return Err(RSError::new(
            "SnapshotCopyGrantAlreadyExistsFault",
            format!(
                "Cannot create the snapshot copy grant because a grant with the identifier '{name}' already exists"
            ),
        ));
    }
    cs.st.snapshot_copy_grants.insert(
        name.clone(),
        SnapshotCopyGrant {
            snapshot_copy_grant_name: name.clone(),
            kms_key_id: j_str(cs.ctx.input, "KmsKeyId").map(String::from),
        },
    );
    Ok(json!({"SnapshotCopyGrant": {"SnapshotCopyGrantName": name}}))
}

fn op_describe_snapshot_copy_grants(cs: &mut CtxState) -> OpResult {
    if let Some(name) = js(cs.ctx.input, "SnapshotCopyGrantName") {
        if !cs.st.snapshot_copy_grants.contains_key(name) {
            return Err(RSError::new(
                "SnapshotCopyGrantNotFoundFault",
                format!("Snapshot copy grant not found: {name}"),
            ));
        }
    }
    let wanted = js(cs.ctx.input, "SnapshotCopyGrantName");
    let grants: Vec<Value> = cs
        .st
        .snapshot_copy_grants
        .values()
        .filter(|g| {
            wanted
                .map(|w| g.snapshot_copy_grant_name == w)
                .unwrap_or(true)
        })
        .map(|g| {
            json!({
                "SnapshotCopyGrantName": g.snapshot_copy_grant_name,
                "KmsKeyId": s_opt(&g.kms_key_id),
            })
        })
        .collect();
    Ok(json!({"SnapshotCopyGrants": grants}))
}

fn op_delete_snapshot_copy_grant(cs: &mut CtxState) -> OpResult {
    let name = js(cs.ctx.input, "SnapshotCopyGrantName")
        .unwrap_or_default()
        .to_string();
    if cs.st.snapshot_copy_grants.remove(&name).is_none() {
        return Err(RSError::new(
            "SnapshotCopyGrantNotFoundFault",
            format!("Snapshot copy grant not found: {name}"),
        ));
    }
    Ok(Value::Null)
}

// ---------------------------------------------------------------------------
// Temporary credentials + logging + snapshot copy toggles
// ---------------------------------------------------------------------------

/// `GetClusterCredentials` — 900..3600 duration check, IAM:/IAMA: prefixes.
fn op_get_cluster_credentials(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let cid = js(&input, "ClusterIdentifier")
        .unwrap_or_default()
        .to_string();
    let db_user = js(&input, "DbUser").unwrap_or_default().to_string();
    let auto_create = j_bool_or(&input, "AutoCreate", false);
    let duration = j_int_or(&input, "DurationSeconds", 900);
    if !(900..=3600).contains(&duration) {
        return Err(RSError::invalid_parameter(
            "Token duration must be between 900 and 3600 seconds",
        ));
    }
    cs.cluster_or_err(&cid)?;
    let prefix = if auto_create { "IAMA" } else { "IAM" };
    Ok(json!({
        "DbUser": format!("{prefix}:{db_user}"),
        "DbPassword": rand_string(32),
        "Expiration": iso_in(duration),
    }))
}

/// `GetClusterCredentialsWithIAM` — never requires an existing cluster.
fn op_get_cluster_credentials_with_iam(cs: &mut CtxState) -> OpResult {
    let duration = j_int_or(cs.ctx.input, "DurationSeconds", 900);
    if !(900..=3600).contains(&duration) {
        return Err(RSError::invalid_parameter(
            "Token duration must be between 900 and 3600 seconds",
        ));
    }
    let expiration = iso_in(duration);
    Ok(json!({
        "DbUser": "IAM:user",
        "DbPassword": rand_string(32),
        "Expiration": expiration,
        "NextRefreshTime": expiration,
    }))
}

/// `EnableLogging` — moto assigns every call's params into the cluster's
/// logging_details (unpassed params overwrite with None and drop out of the
/// wire shape), then returns the dict.
fn op_enable_logging(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let cid = js(&input, "ClusterIdentifier")
        .unwrap_or_default()
        .to_string();
    let cluster = cs.cluster_or_err(&cid)?.clone();
    let mut details = cluster.logging_details.clone();
    let obj = details.as_object_mut().expect("logging details is a dict");
    obj.insert("LoggingEnabled".into(), json!(true));
    obj.insert(
        "BucketName".into(),
        j_str(&input, "BucketName").map(String::from).into(),
    );
    obj.insert(
        "S3KeyPrefix".into(),
        j_str(&input, "S3KeyPrefix").map(String::from).into(),
    );
    obj.insert(
        "LogDestinationType".into(),
        j_str(&input, "LogDestinationType").map(String::from).into(),
    );
    obj.insert("LogExports".into(), strings(input.get("LogExports")).into());
    cs.st.clusters.insert(
        cid,
        Cluster {
            logging_details: details.clone(),
            ..cluster
        },
    );
    Ok(details)
}

fn op_disable_logging(cs: &mut CtxState) -> OpResult {
    let cid = js(cs.ctx.input, "ClusterIdentifier")
        .unwrap_or_default()
        .to_string();
    let cluster = cs.cluster_or_err(&cid)?.clone();
    let mut details = cluster.logging_details.clone();
    details
        .as_object_mut()
        .expect("logging details is a dict")
        .insert("LoggingEnabled".into(), json!(false));
    cs.st.clusters.insert(
        cid,
        Cluster {
            logging_details: details.clone(),
            ..cluster
        },
    );
    Ok(details)
}

fn op_describe_logging_status(cs: &mut CtxState) -> OpResult {
    let cid = js(cs.ctx.input, "ClusterIdentifier")
        .unwrap_or_default()
        .to_string();
    let cluster = cs.cluster_or_err(&cid)?.clone();
    Ok(cluster.logging_details)
}

fn op_enable_snapshot_copy(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let cid = js(&input, "ClusterIdentifier")
        .unwrap_or_default()
        .to_string();
    let cluster = cs.cluster_or_err(&cid)?.clone();
    if cluster.cluster_snapshot_copy_status.is_some() {
        return Err(RSError::new(
            "SnapshotCopyAlreadyEnabledFault",
            format!("Snapshot Copy is already enabled on Cluster {cid}."),
        ));
    }
    if cluster.encrypted && j_str(&input, "SnapshotCopyGrantName").is_none() {
        return Err(RSError::invalid_parameter(
            "SnapshotCopyGrantName is required for Snapshot Copy on KMS encrypted clusters.",
        ));
    }
    if j_str(&input, "DestinationRegion").unwrap_or_default() == cs.ctx.region {
        return Err(RSError::new(
            "UnknownSnapshotCopyRegionFault",
            format!("Invalid region {}", cs.ctx.region),
        ));
    }
    let status = json!({
        "DestinationRegion": j_str(&input, "DestinationRegion"),
        "RetentionPeriod": j_int_or(&input, "RetentionPeriod", 7),
        "SnapshotCopyGrantName": j_str(&input, "SnapshotCopyGrantName").map(String::from),
    });
    let mut cluster2 = cluster.clone();
    cluster2.cluster_snapshot_copy_status = Some(status);
    cs.st.clusters.insert(cid, cluster2.clone());
    Ok(json!({"Cluster": cluster2.json(cs.st, cs.ctx.region, None)}))
}

fn op_disable_snapshot_copy(cs: &mut CtxState) -> OpResult {
    let cid = js(cs.ctx.input, "ClusterIdentifier")
        .unwrap_or_default()
        .to_string();
    let cluster = cs.cluster_or_err(&cid)?.clone();
    if cluster.cluster_snapshot_copy_status.is_none() {
        return Err(RSError::new(
            "SnapshotCopyAlreadyDisabledFault",
            format!("Snapshot Copy is already disabled on Cluster {cid}."),
        ));
    }
    let mut cluster2 = cluster.clone();
    cluster2.cluster_snapshot_copy_status = None;
    cs.st.clusters.insert(cid, cluster2.clone());
    Ok(json!({"Cluster": cluster2.json(cs.st, cs.ctx.region, None)}))
}

fn op_modify_snapshot_copy_retention_period(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let cid = js(&input, "ClusterIdentifier")
        .unwrap_or_default()
        .to_string();
    let cluster = cs.cluster_or_err(&cid)?.clone();
    match cluster.cluster_snapshot_copy_status.clone() {
        Some(mut status) => {
            let obj = status.as_object_mut().expect("copy status object");
            obj.insert(
                "RetentionPeriod".to_string(),
                json!(j_int_or(&input, "RetentionPeriod", 7)),
            );
            let mut cluster2 = cluster.clone();
            cluster2.cluster_snapshot_copy_status = Some(status);
            cs.st.clusters.insert(cid.clone(), cluster2);
            // moto responds {"Clusters": [...]} against an output shape that
            // only declares `Cluster` — the rendered wire body is empty.
            Ok(json!({"Clusters": [cid]}))
        }
        None => Err(RSError::new(
            "SnapshotCopyDisabledFault",
            format!("Cannot modify retention period because snapshot copy is disabled on Cluster {cid}."),
        )),
    }
}
// ---------------------------------------------------------------------------
// Tags (TaggableResourceMixin semantics + moto's arn -> resource resolver)
// ---------------------------------------------------------------------------

/// The five moto taggable resources. `resource_id` matches moto's
/// `resource_id`/`arn` properties (snapshot ids are `cluster/snapshot`).
#[derive(Clone, Debug)]
enum TagResource {
    Cluster(String),
    ParameterGroup(String),
    SecurityGroup(String),
    Snapshot(String),
    SubnetGroup(String),
}

impl TagResource {
    fn rtype(&self) -> &'static str {
        match self {
            TagResource::Cluster(_) => "cluster",
            TagResource::ParameterGroup(_) => "parametergroup",
            TagResource::SecurityGroup(_) => "securitygroup",
            TagResource::Snapshot(_) => "snapshot",
            TagResource::SubnetGroup(_) => "subnetgroup",
        }
    }
}

/// moto `_get_resource_from_arn`: rtype comes from `arn.split(":")[5]`, the
/// id from `[6]` (`snapshot` ids unwrap the second `/`-joined segment).
fn resolve_arn_resource(st: &RedshiftState, arn: &str) -> Result<TagResource, RSError> {
    let parts: Vec<&str> = arn.split(':').collect();
    let (resource_type, resource_id) = match parts.len() {
        n if n > 6 => {
            let rtype = parts[5];
            let rid = if rtype == "snapshot" {
                parts[6].split('/').nth(1).unwrap_or_default()
            } else {
                parts[6]
            };
            (rtype.to_string(), rid.to_string())
        }
        _ => (arn.to_string(), arn.to_string()),
    };
    let (resource_type, resource_id) = (resource_type, resource_id);
    match resource_type.as_str() {
        "cluster" => {
            if st.clusters.contains_key(&resource_id) {
                return Ok(TagResource::Cluster(resource_id));
            }
        }
        "parametergroup" => {
            if st.parameter_groups.contains_key(&resource_id) {
                return Ok(TagResource::ParameterGroup(resource_id));
            }
        }
        "securitygroup" => {
            if st.security_groups.contains_key(&resource_id) {
                return Ok(TagResource::SecurityGroup(resource_id));
            }
        }
        "snapshot" => {
            if st.snapshots.contains_key(&resource_id) {
                return Ok(TagResource::Snapshot(resource_id));
            }
        }
        "subnetgroup" => {
            if st.subnet_groups.contains_key(&resource_id) {
                return Ok(TagResource::SubnetGroup(resource_id));
            }
        }
        other => {
            return Err(RSError::new(
                "ResourceNotFoundFault",
                format!(
                    "Tagging is not supported for this type of resource: '{}' (the ARN is potentially malformed, \
                     please check the ARN documentation for more information)",
                    other
                ),
            ));
        }
    }
    Err(RSError::resource_not_found_arn(
        resource_type.as_str(),
        resource_id.as_str(),
    ))
}

/// All the tag sets in the backend with their resource tuples, walking the
/// moto RESOURCE_TYPE_MAP order.
fn tag_walk(st: &RedshiftState) -> Vec<(TagResource, Vec<Tag>)> {
    let mut out = Vec::new();
    for (id, c) in st.clusters.iter() {
        out.push((TagResource::Cluster(id.clone()), c.tags.clone()));
    }
    for (id, p) in st.parameter_groups.iter() {
        out.push((TagResource::ParameterGroup(id.clone()), p.tags.clone()));
    }
    for (id, g) in st.security_groups.iter() {
        out.push((TagResource::SecurityGroup(id.clone()), g.tags.clone()));
    }
    for (id, s) in st.snapshots.iter() {
        // snapshot arns embed cluster/snapshot — describe_tags emits the
        // resource arn via the resource's own `arn` property
        out.push((TagResource::Snapshot(id.clone()), s.tags.clone()));
    }
    for (id, g) in st.subnet_groups.iter() {
        out.push((TagResource::SubnetGroup(id.clone()), g.tags.clone()));
    }
    out
}

/// arn of a tag resource (moto TaggableResourceMixin.arn).
fn tag_resource_arn(st: &RedshiftState, account: u64, region: &str, r: &TagResource) -> String {
    let resource_id = match r {
        TagResource::Snapshot(id) => st
            .snapshots
            .get(id)
            .map(|s| format!("{}/{}", s.cluster_identifier, s.snapshot_identifier))
            .unwrap_or_default(),
        TagResource::Cluster(id) => id.clone(),
        TagResource::ParameterGroup(id) => id.clone(),
        TagResource::SecurityGroup(id) => id.clone(),
        TagResource::SubnetGroup(id) => id.clone(),
    };
    format!(
        "arn:{}:redshift:{}:{}:{}:{}",
        partition(region),
        region,
        account,
        r.rtype(),
        resource_id
    )
}

fn op_create_tags(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let arn = js(&input, "ResourceName").unwrap_or_default().to_string();
    let tags = j_tags(&input, "Tags");
    let resource = resolve_arn_resource(cs.st, &arn)?;
    let new_keys: Vec<&str> = tags.iter().map(|t| t.key.as_str()).collect();
    {
        let blob = tags_by_resource_mut(cs.st, &resource);
        blob.retain(|t| !new_keys.contains(&t.key.as_str()));
        blob.extend(tags);
    }
    Ok(Value::Null)
}

fn tags_by_resource_mut<'s>(st: &'s mut RedshiftState, r: &TagResource) -> &'s mut Vec<Tag> {
    match r {
        TagResource::Cluster(id) => &mut st.clusters.get_mut(id).expect("checked").tags,
        TagResource::ParameterGroup(id) => {
            &mut st.parameter_groups.get_mut(id).expect("checked").tags
        }
        TagResource::SecurityGroup(id) => {
            &mut st.security_groups.get_mut(id).expect("checked").tags
        }
        TagResource::Snapshot(id) => &mut st.snapshots.get_mut(id).expect("checked").tags,
        TagResource::SubnetGroup(id) => &mut st.subnet_groups.get_mut(id).expect("checked").tags,
    }
}

fn op_delete_tags(cs: &mut CtxState) -> OpResult {
    let arn = js(cs.ctx.input, "ResourceName")
        .unwrap_or_default()
        .to_string();
    let keys = strings(cs.ctx.input.get("TagKeys"));
    let resource = resolve_arn_resource(cs.st, &arn)?;
    let blob = tags_by_resource_mut(cs.st, &resource);
    blob.retain(|t| !keys.contains(&t.key));
    Ok(Value::Null)
}

fn op_describe_tags(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let name = js(&input, "ResourceName");
    let rtype = js(&input, "ResourceType");
    if name.is_some() && rtype.is_some() {
        return Err(RSError::invalid_parameter(
            "You cannot filter a list of resources using an Amazon Resource Name (ARN) and a resource type together in the \
             same request. Retry the request using either an ARN or a resource type, but not both.",
        ));
    }
    if let Some(rtype) = rtype {
        let rtype = rtype.to_ascii_lowercase();
        let matched: Vec<(TagResource, Vec<Tag>)> = tag_walk(cs.st)
            .into_iter()
            .filter(|(r, _)| r.rtype() == rtype)
            .collect();
        if matched.is_empty() {
            return Err(RSError::resource_not_found_type(&rtype));
        }
        return Ok(json!({"TaggedResources": tagged_resource_json(cs, &matched)}));
    }
    if let Some(name) = name {
        let resource = resolve_arn_resource(cs.st, name)?;
        let tags = tags_by_resource(cs.st, &resource).to_vec();
        return Ok(json!({
            "TaggedResources": tagged_resource_json(cs, &[(resource, tags)]),
        }));
    }
    Ok(json!({
        "TaggedResources": tagged_resource_json(cs, &tag_walk(cs.st)),
    }))
}

fn tags_by_resource<'a>(st: &'a RedshiftState, r: &TagResource) -> &'a [Tag] {
    match r {
        TagResource::Cluster(id) => st
            .clusters
            .get(id)
            .map(|c| c.tags.as_slice())
            .unwrap_or(&[]),
        TagResource::ParameterGroup(id) => st
            .parameter_groups
            .get(id)
            .map(|c| c.tags.as_slice())
            .unwrap_or(&[]),
        TagResource::SecurityGroup(id) => st
            .security_groups
            .get(id)
            .map(|c| c.tags.as_slice())
            .unwrap_or(&[]),
        TagResource::Snapshot(id) => st
            .snapshots
            .get(id)
            .map(|c| c.tags.as_slice())
            .unwrap_or(&[]),
        TagResource::SubnetGroup(id) => st
            .subnet_groups
            .get(id)
            .map(|c| c.tags.as_slice())
            .unwrap_or(&[]),
    }
}

/// moto `_describe_tags_for_resources` — one `TaggedResources` item per tag.
fn tagged_resource_json(cs: &CtxState, resources: &[(TagResource, Vec<Tag>)]) -> Vec<Value> {
    let mut out = Vec::new();
    for (r, tags) in resources {
        for tag in tags {
            out.push(json!({
                "ResourceName": tag_resource_arn(cs.st, cs.ctx.account, cs.ctx.region, r),
                "ResourceType": r.rtype(),
                "Tag": {"Key": tag.key, "Value": tag.value},
            }));
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Snapshot schedules
// ---------------------------------------------------------------------------

fn op_create_snapshot_schedule(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let identifier = j_str(&input, "ScheduleIdentifier").map(String::from);
    let definitions = strings(input.get("ScheduleDefinitions"));
    let tags = j_tags(&input, "Tags");
    let schedule = json!({
        "ScheduleIdentifier": identifier,
        "ScheduleDefinitions": definitions,
        "Tags": tags_json(&tags),
        "AssociatedClusterCount": 0,
    });
    // moto does not reject duplicates here.
    if let Some(id) = identifier {
        cs.st.snapshot_schedules.insert(id, schedule.clone());
    }
    Ok(schedule)
}

fn op_delete_snapshot_schedule(cs: &mut CtxState) -> OpResult {
    let sid = js(cs.ctx.input, "ScheduleIdentifier")
        .unwrap_or_default()
        .to_string();
    if cs.st.snapshot_schedules.remove(&sid).is_none() {
        return Err(RSError::new(
            "ResourceNotFoundFault",
            format!("Snapshot schedule {sid} not found."),
        ));
    }
    Ok(Value::Null)
}

fn op_modify_snapshot_schedule(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let sid = js(&input, "ScheduleIdentifier")
        .unwrap_or_default()
        .to_string();
    let definitions = strings(input.get("ScheduleDefinitions"));
    let Some(schedule) = cs.st.snapshot_schedules.get_mut(&sid) else {
        return Err(RSError::new(
            "ResourceNotFoundFault",
            format!("Snapshot schedule {sid} not found."),
        ));
    };
    schedule["ScheduleDefinitions"] =
        Value::Array(definitions.into_iter().map(Value::String).collect());
    Ok(schedule.clone())
}

fn op_describe_snapshot_schedules(cs: &mut CtxState) -> OpResult {
    let wanted = js(cs.ctx.input, "ScheduleIdentifier").map(String::from);
    let schedules: Vec<Value> = cs
        .st
        .snapshot_schedules
        .iter()
        .filter(|(k, _)| {
            wanted
                .as_ref()
                .map(|w| k.as_str() == w.as_str())
                .unwrap_or(true)
        })
        .map(|(_, v)| v.clone())
        .collect();
    Ok(json!({
        "SnapshotSchedules": if wanted.is_some() && schedules.is_empty() {
            Vec::new()
        } else {
            schedules
        },
    }))
}

/// `ModifyClusterSnapshotSchedule` — moto validates the cluster only.
fn op_modify_cluster_snapshot_schedule(cs: &mut CtxState) -> OpResult {
    let cid = js(cs.ctx.input, "ClusterIdentifier")
        .unwrap_or_default()
        .to_string();
    cs.cluster_or_err(&cid)?;
    Ok(Value::Null)
}

// ---------------------------------------------------------------------------
// Scheduled actions
// ---------------------------------------------------------------------------

fn op_create_scheduled_action(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let name = js(&input, "ScheduledActionName")
        .unwrap_or_default()
        .to_string();
    if cs.st.scheduled_actions.contains_key(&name) {
        return Err(RSError::new(
            "ScheduledActionAlreadyExists",
            format!("Scheduled Action {name} already exists."),
        ));
    }
    let action = ScheduledAction {
        name: name.clone(),
        target_action: input.get("TargetAction").cloned().unwrap_or(json!({})),
        schedule: j_str(&input, "Schedule").unwrap_or_default().to_string(),
        iam_role: j_str(&input, "IamRole").unwrap_or_default().to_string(),
        description: j_str(&input, "ScheduledActionDescription")
            .unwrap_or_default()
            .to_string(),
        state: if j_bool_or(&input, "Enable", true) {
            "ACTIVE".to_string()
        } else {
            "DISABLED".to_string()
        },
        start_time: j_str(&input, "StartTime").map(String::from),
        end_time: j_str(&input, "EndTime").map(String::from),
    };
    cs.st.scheduled_actions.insert(name, action.clone());
    Ok(action.json())
}

fn op_delete_scheduled_action(cs: &mut CtxState) -> OpResult {
    let name = js(cs.ctx.input, "ScheduledActionName")
        .unwrap_or_default()
        .to_string();
    if cs.st.scheduled_actions.remove(&name).is_none() {
        return Err(RSError::new(
            "ScheduledActionNotFoundFault",
            format!("Scheduled Action {name} not found."),
        ));
    }
    Ok(Value::Null)
}

fn op_modify_scheduled_action(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let name = js(&input, "ScheduledActionName")
        .unwrap_or_default()
        .to_string();
    let Some(action) = cs.st.scheduled_actions.get_mut(&name) else {
        return Err(RSError::new(
            "ScheduledActionNotFoundFault",
            format!("Scheduled Action {name} not found."),
        ));
    };
    if let Some(v) = input.get("TargetAction") {
        action.target_action = v.clone();
    }
    if let Some(v) = j_str(&input, "Schedule") {
        action.schedule = v.to_string();
    }
    if let Some(v) = j_str(&input, "IamRole") {
        action.iam_role = v.to_string();
    }
    if let Some(v) = j_str(&input, "ScheduledActionDescription") {
        action.description = v.to_string();
    }
    if let Some(v) = j_str(&input, "StartTime") {
        action.start_time = Some(v.to_string());
    }
    if let Some(v) = j_str(&input, "EndTime") {
        action.end_time = Some(v.to_string());
    }
    if let Some(v) = opt_bool(&input, "Enable") {
        action.state = if v {
            "ACTIVE".to_string()
        } else {
            "DISABLED".to_string()
        };
    }
    Ok(action.json())
}

fn op_describe_scheduled_actions(cs: &mut CtxState) -> OpResult {
    let wanted = js(cs.ctx.input, "ScheduledActionName").map(String::from);
    let actions: Result<Vec<Value>, RSError> = if let Some(name) = &wanted {
        match cs.st.scheduled_actions.get(name) {
            Some(a) => Ok(vec![a.json()]),
            None => Err(RSError::new(
                "ScheduledActionNotFoundFault",
                format!("Scheduled Action {name} not found."),
            )),
        }
    } else {
        Ok(cs.st.scheduled_actions.values().map(|a| a.json()).collect())
    };
    Ok(json!({"ScheduledActions": actions?}))
}

// ---------------------------------------------------------------------------
// Usage limits
// ---------------------------------------------------------------------------

fn op_create_usage_limit(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let cid = js(&input, "ClusterIdentifier")
        .unwrap_or_default()
        .to_string();
    cs.cluster_or_err(&cid)?;
    let usage_limit_id = format!("rul-{}", rand_hex(8));
    let ul = UsageLimit {
        usage_limit_id: usage_limit_id.clone(),
        cluster_identifier: cid,
        feature_type: j_str(&input, "FeatureType").unwrap_or_default().to_string(),
        limit_type: j_str(&input, "LimitType").unwrap_or_default().to_string(),
        amount: j_int_or(&input, "Amount", 0),
        period: j_str(&input, "Period").unwrap_or("monthly").to_string(),
        breach_action: j_str(&input, "BreachAction").unwrap_or("log").to_string(),
        tags: j_tags(&input, "Tags"),
    };
    cs.st.usage_limits.insert(usage_limit_id, ul.clone());
    Ok(ul.json())
}

fn op_delete_usage_limit(cs: &mut CtxState) -> OpResult {
    let id = js(cs.ctx.input, "UsageLimitId")
        .unwrap_or_default()
        .to_string();
    if cs.st.usage_limits.remove(&id).is_none() {
        return Err(RSError::new(
            "UsageLimitNotFound",
            format!("Usage limit {id} not found."),
        ));
    }
    Ok(Value::Null)
}

fn op_modify_usage_limit(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let id = js(&input, "UsageLimitId").unwrap_or_default().to_string();
    let Some(ul) = cs.st.usage_limits.get_mut(&id) else {
        return Err(RSError::new(
            "UsageLimitNotFound",
            format!("Usage limit {id} not found."),
        ));
    };
    if let Some(amount) = opt_int(&input, "Amount") {
        ul.amount = amount;
    }
    if let Some(bre) = j_str(&input, "BreachAction") {
        ul.breach_action = bre.to_string();
    }
    Ok(ul.json())
}

fn op_describe_usage_limits(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let id = js(&input, "UsageLimitId").map(String::from);
    let cluster = js(&input, "ClusterIdentifier").map(String::from);
    let feature = js(&input, "FeatureType").map(String::from);
    let limits: Vec<Value> = cs
        .st
        .usage_limits
        .values()
        .filter(|ul| {
            id.as_ref().map(|i| ul.usage_limit_id == *i).unwrap_or(true)
                && cluster
                    .as_ref()
                    .map(|c| ul.cluster_identifier == *c)
                    .unwrap_or(true)
                && feature
                    .as_ref()
                    .map(|f| ul.feature_type == *f)
                    .unwrap_or(true)
        })
        .map(|ul| ul.json())
        .collect();
    Ok(json!({"UsageLimits": limits}))
}

// ---------------------------------------------------------------------------
// Authentication profiles
// ---------------------------------------------------------------------------

fn op_create_authentication_profile(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let name = js(&input, "AuthenticationProfileName")
        .unwrap_or_default()
        .to_string();
    if cs.st.authentication_profiles.contains_key(&name) {
        return Err(RSError::new(
            "AuthenticationProfileAlreadyExistsFault",
            format!("Authentication profile {name} already exists."),
        ));
    }
    let content = j_str(&input, "AuthenticationProfileContent")
        .unwrap_or_default()
        .to_string();
    cs.st.authentication_profiles.insert(
        name.clone(),
        AuthProfile {
            name: name.clone(),
            content: content.clone(),
        },
    );
    Ok(json!({
        "AuthenticationProfileName": name,
        "AuthenticationProfileContent": content,
    }))
}

fn op_delete_authentication_profile(cs: &mut CtxState) -> OpResult {
    let name = js(cs.ctx.input, "AuthenticationProfileName")
        .unwrap_or_default()
        .to_string();
    match cs.st.authentication_profiles.remove(&name) {
        Some(_) => Ok(json!({"AuthenticationProfileName": name})),
        None => Err(RSError::new(
            "AuthenticationProfileNotFoundFault",
            format!("Authentication profile {name} not found."),
        )),
    }
}

fn op_modify_authentication_profile(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let name = js(&input, "AuthenticationProfileName")
        .unwrap_or_default()
        .to_string();
    let Some(profile) = cs.st.authentication_profiles.get_mut(&name) else {
        return Err(RSError::new(
            "AuthenticationProfileNotFoundFault",
            format!("Authentication profile {name} not found."),
        ));
    };
    profile.content = j_str(&input, "AuthenticationProfileContent")
        .unwrap_or_default()
        .to_string();
    Ok(json!({
        "AuthenticationProfileName": name,
        "AuthenticationProfileContent": profile.content,
    }))
}

fn op_describe_authentication_profiles(cs: &mut CtxState) -> OpResult {
    if let Some(name) = js(cs.ctx.input, "AuthenticationProfileName") {
        let Some(p) = cs.st.authentication_profiles.get(name) else {
            return Err(RSError::new(
                "AuthenticationProfileNotFoundFault",
                format!("Authentication profile {name} not found."),
            ));
        };
        return Ok(json!({"AuthenticationProfiles": [{
            "AuthenticationProfileName": p.name,
            "AuthenticationProfileContent": p.content,
        }]}));
    }
    let profiles: Vec<Value> = cs
        .st
        .authentication_profiles
        .values()
        .map(|p| {
            json!({
                "AuthenticationProfileName": p.name,
                "AuthenticationProfileContent": p.content,
            })
        })
        .collect();
    Ok(json!({"AuthenticationProfiles": profiles}))
}

// ---------------------------------------------------------------------------
// Event subscriptions
// ---------------------------------------------------------------------------

fn op_create_event_subscription(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let name = js(&input, "SubscriptionName")
        .unwrap_or_default()
        .to_string();
    if cs.st.event_subscriptions.contains_key(&name) {
        return Err(RSError::new(
            "SubscriptionAlreadyExist",
            format!("Subscription {name} already exists."),
        ));
    }
    let sub = EventSubscription {
        subscription_name: name.clone(),
        sns_topic_arn: j_str(&input, "SnsTopicArn").unwrap_or_default().to_string(),
        account_id: cs.ctx.account.to_string(),
        region: cs.ctx.region.to_string(),
        source_type: j_str(&input, "SourceType").map(String::from),
        source_ids: strings(input.get("SourceIds")),
        event_categories: strings(input.get("EventCategories")),
        severity: j_str(&input, "Severity").unwrap_or("ERROR").to_string(),
        enabled: j_bool_or(&input, "Enabled", true),
        status: "active".to_string(),
        subscription_creation_time: chrono::Utc::now(),
        tags: j_tags(&input, "Tags"),
    };
    cs.st.event_subscriptions.insert(name, sub.clone());
    Ok(json!({"EventSubscription": sub.json()}))
}

fn op_delete_event_subscription(cs: &mut CtxState) -> OpResult {
    let name = js(cs.ctx.input, "SubscriptionName")
        .unwrap_or_default()
        .to_string();
    if cs.st.event_subscriptions.remove(&name).is_none() {
        return Err(RSError::new(
            "SubscriptionNotFound",
            format!("Subscription {name} not found."),
        ));
    }
    Ok(Value::Null)
}

fn op_modify_event_subscription(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let name = js(&input, "SubscriptionName")
        .unwrap_or_default()
        .to_string();
    let Some(sub) = cs.st.event_subscriptions.get_mut(&name) else {
        return Err(RSError::new(
            "SubscriptionNotFound",
            format!("Subscription {name} not found."),
        ));
    };
    if let Some(v) = j_str(&input, "SnsTopicArn") {
        sub.sns_topic_arn = v.to_string();
    }
    if let Some(v) = j_str(&input, "SourceType") {
        sub.source_type = Some(v.to_string());
    }
    if input.get("SourceIds").is_some() {
        sub.source_ids = strings(input.get("SourceIds"));
    }
    if input.get("EventCategories").is_some() {
        sub.event_categories = strings(input.get("EventCategories"));
    }
    if let Some(v) = j_str(&input, "Severity") {
        sub.severity = v.to_string();
    }
    if let Some(v) = opt_bool(&input, "Enabled") {
        sub.enabled = v;
    }
    Ok(json!({"EventSubscription": sub.json()}))
}

fn op_describe_event_subscriptions(cs: &mut CtxState) -> OpResult {
    if let Some(name) = js(cs.ctx.input, "SubscriptionName") {
        let Some(sub) = cs.st.event_subscriptions.get(name) else {
            return Err(RSError::new(
                "SubscriptionNotFound",
                format!("Subscription {name} not found."),
            ));
        };
        return Ok(json!({"EventSubscriptionsList": [sub.json()]}));
    }
    let subs: Vec<Value> = cs
        .st
        .event_subscriptions
        .values()
        .map(|s| s.json())
        .collect();
    Ok(json!({"EventSubscriptionsList": subs}))
}

// ---------------------------------------------------------------------------
// HSM (client certificates + configurations)
// ---------------------------------------------------------------------------

fn op_create_hsm_client_certificate(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let id = js(&input, "HsmClientCertificateIdentifier")
        .unwrap_or_default()
        .to_string();
    if cs.st.hsm_client_certificates.contains_key(&id) {
        return Err(RSError::new(
            "HsmClientCertificateAlreadyExistsFault",
            format!("HSM client certificate {id} already exists."),
        ));
    }
    let cert = HsmClientCertificate {
        hsm_client_certificate_identifier: id.clone(),
        hsm_client_certificate_public_key: format!(
            "-----BEGIN CERTIFICATE-----\nMOCK{}\n-----END CERTIFICATE-----",
            rand_hex(20)
        ),
        account_id: cs.ctx.account.to_string(),
        region: cs.ctx.region.to_string(),
        tags: j_tags(&input, "Tags"),
    };
    cs.st.hsm_client_certificates.insert(id, cert.clone());
    Ok(json!({
        "HsmClientCertificate": {
            "HsmClientCertificateIdentifier": cert.hsm_client_certificate_identifier,
            "HsmClientCertificatePublicKey": cert.hsm_client_certificate_public_key,
            "Tags": tags_json(&cert.tags),
        }
    }))
}

fn op_delete_hsm_client_certificate(cs: &mut CtxState) -> OpResult {
    let id = js(cs.ctx.input, "HsmClientCertificateIdentifier")
        .unwrap_or_default()
        .to_string();
    if cs.st.hsm_client_certificates.remove(&id).is_none() {
        return Err(RSError::new(
            "HsmClientCertificateNotFoundFault",
            format!("HSM client certificate {id} not found."),
        ));
    }
    Ok(Value::Null)
}

fn op_describe_hsm_client_certificates(cs: &mut CtxState) -> OpResult {
    let wanted = js(cs.ctx.input, "HsmClientCertificateIdentifier").map(String::from);
    if let Some(id) = &wanted {
        if !cs.st.hsm_client_certificates.contains_key(id) {
            return Err(RSError::new(
                "HsmClientCertificateNotFoundFault",
                format!("HSM client certificate {id} not found."),
            ));
        }
    }
    let certs: Vec<Value> = cs
        .st
        .hsm_client_certificates
        .values()
        .filter(|c| {
            wanted
                .as_ref()
                .map(|w| &c.hsm_client_certificate_identifier == w)
                .unwrap_or(true)
        })
        .map(|c| {
            json!({
                "HsmClientCertificateIdentifier": c.hsm_client_certificate_identifier,
                "HsmClientCertificatePublicKey": c.hsm_client_certificate_public_key,
                "Tags": tags_json(&c.tags),
            })
        })
        .collect();
    Ok(json!({"HsmClientCertificates": certs}))
}

fn op_create_hsm_configuration(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let id = js(&input, "HsmConfigurationIdentifier")
        .unwrap_or_default()
        .to_string();
    if cs.st.hsm_configurations.contains_key(&id) {
        return Err(RSError::new(
            "HsmConfigurationAlreadyExistsFault",
            format!("HSM configuration {id} already exists."),
        ));
    }
    let cfg = HsmConfiguration {
        hsm_configuration_identifier: id.clone(),
        description: j_str(&input, "Description").unwrap_or_default().to_string(),
        hsm_ip_address: j_str(&input, "HsmIpAddress")
            .unwrap_or_default()
            .to_string(),
        hsm_partition_name: j_str(&input, "HsmPartitionName")
            .unwrap_or_default()
            .to_string(),
        account_id: cs.ctx.account.to_string(),
        region: cs.ctx.region.to_string(),
        tags: j_tags(&input, "Tags"),
    };
    cs.st.hsm_configurations.insert(id, cfg);
    Ok(json!({
        "HsmConfiguration": {
            "HsmConfigurationIdentifier": j_str(&input, "HsmConfigurationIdentifier"),
            "Description": j_str(&input, "Description"),
            "HsmIpAddress": j_str(&input, "HsmIpAddress"),
            "HsmPartitionName": j_str(&input, "HsmPartitionName"),
            "Tags": tags_json(&j_tags(&input, "Tags")),
        }
    }))
}

fn op_delete_hsm_configuration(cs: &mut CtxState) -> OpResult {
    let id = js(cs.ctx.input, "HsmConfigurationIdentifier")
        .unwrap_or_default()
        .to_string();
    if cs.st.hsm_configurations.remove(&id).is_none() {
        return Err(RSError::new(
            "HsmConfigurationNotFoundFault",
            format!("HSM configuration {id} not found."),
        ));
    }
    Ok(Value::Null)
}

fn op_describe_hsm_configurations(cs: &mut CtxState) -> OpResult {
    let wanted = js(cs.ctx.input, "HsmConfigurationIdentifier").map(String::from);
    if let Some(id) = &wanted {
        if !cs.st.hsm_configurations.contains_key(id) {
            return Err(RSError::new(
                "HsmConfigurationNotFoundFault",
                format!("HSM configuration {id} not found."),
            ));
        }
    }
    let cfgs: Vec<Value> = cs
        .st
        .hsm_configurations
        .values()
        .filter(|c| {
            wanted
                .as_ref()
                .map(|w| &c.hsm_configuration_identifier == w)
                .unwrap_or(true)
        })
        .map(|c| {
            json!({
                "HsmConfigurationIdentifier": c.hsm_configuration_identifier,
                "Description": c.description,
                "HsmIpAddress": c.hsm_ip_address,
                "HsmPartitionName": c.hsm_partition_name,
                "Tags": tags_json(&c.tags),
            })
        })
        .collect();
    Ok(json!({"HsmConfigurations": cfgs}))
}

// ---------------------------------------------------------------------------
// Endpoint access / endpoint authorization
// ---------------------------------------------------------------------------

fn op_create_endpoint_access(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let cid = js(&input, "ClusterIdentifier")
        .unwrap_or_default()
        .to_string();
    cs.cluster_or_err(&cid)?;
    let name = js(&input, "EndpointName").unwrap_or_default().to_string();
    if cs.st.endpoint_access.contains_key(&name) {
        return Err(RSError::new(
            "EndpointAlreadyExists",
            format!("Endpoint {name} already exists."),
        ));
    }
    let region = cs.ctx.region.to_string();
    let ep = EndpointAccess {
        cluster_identifier: cid,
        resource_owner: cs.ctx.account.to_string(),
        endpoint_name: name.clone(),
        subnet_group_name: j_str(&input, "SubnetGroupName")
            .unwrap_or_default()
            .to_string(),
        region_name: region.clone(),
        endpoint_status: "active".to_string(),
        endpoint_create_time: chrono::Utc::now(),
        port: 5439,
        address: format!(
            "{name}.{}.{region}.redshift-serverless.amazonaws.com",
            rand_hex(8)
        ),
        vpc_security_groups: strings(input.get("VpcSecurityGroupIds"))
            .into_iter()
            .map(|id| json!({"VpcSecurityGroupId": id, "Status": "active"}))
            .collect(),
        vpc_endpoint: json!({
            "VpcEndpointId": format!("vpce-{}", rand_hex(8)),
            "VpcId": format!("vpc-{}", rand_hex(8)),
            "NetworkInterfaces": [],
        }),
    };
    cs.st.endpoint_access.insert(name, ep.clone());
    Ok(ep.json())
}

fn op_delete_endpoint_access(cs: &mut CtxState) -> OpResult {
    let name = js(cs.ctx.input, "EndpointName")
        .unwrap_or_default()
        .to_string();
    let Some(ep) = cs.st.endpoint_access.get_mut(&name) else {
        return Err(RSError::new(
            "EndpointNotFound",
            format!("Endpoint {name} not found."),
        ));
    };
    ep.endpoint_status = "deleting".to_string();
    Ok(ep.json())
}

fn op_modify_endpoint_access(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let name = js(&input, "EndpointName").unwrap_or_default().to_string();
    let Some(ep) = cs.st.endpoint_access.get_mut(&name) else {
        return Err(RSError::new(
            "EndpointNotFound",
            format!("Endpoint {name} not found."),
        ));
    };
    if input.get("VpcSecurityGroupIds").is_some() {
        ep.vpc_security_groups = strings(input.get("VpcSecurityGroupIds"))
            .into_iter()
            .map(|id| json!({"VpcSecurityGroupId": id, "Status": "active"}))
            .collect();
    }
    Ok(ep.json())
}

fn op_describe_endpoint_access(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let cid = js(&input, "ClusterIdentifier").map(String::from);
    let name = js(&input, "EndpointName").map(String::from);
    if let Some(name) = &name {
        if !cs.st.endpoint_access.contains_key(name) {
            return Err(RSError::new(
                "EndpointNotFound",
                format!("Endpoint {name} not found."),
            ));
        }
        let ep = &cs.st.endpoint_access[name];
        return Ok(json!({"EndpointAccessList": [ep.json()]}));
    }
    let eps: Vec<Value> = cs
        .st
        .endpoint_access
        .values()
        .filter(|ep| {
            cid.as_ref()
                .map(|c| ep.cluster_identifier == *c)
                .unwrap_or(true)
        })
        .map(|ep| ep.json())
        .collect();
    Ok(json!({"EndpointAccessList": eps}))
}

fn op_authorize_endpoint_access(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let cid = js(&input, "ClusterIdentifier")
        .unwrap_or_default()
        .to_string();
    cs.cluster_or_err(&cid)?;
    let account = j_str(&input, "Account").unwrap_or_default().to_string();
    let key = format!("{cid}:{account}");
    if cs.st.endpoint_authorizations.contains_key(&key) {
        return Err(RSError::new(
            "EndpointAuthorizationAlreadyExists",
            format!(
                "Endpoint authorization already exists for cluster {cid} and account {account}."
            ),
        ));
    }
    let vpc_ids = strings(input.get("VpcIds"));
    let auth = EndpointAuthorization {
        cluster_identifier: cid,
        account: account.clone(),
        grantor: cs.ctx.account.to_string(),
        status: "Authorized".to_string(),
        allowed_all_vpcs: input.get("VpcIds").is_none(),
        allowed_vpcs: vpc_ids,
    };
    cs.st.endpoint_authorizations.insert(key, auth.clone());
    Ok(auth.json())
}

fn op_describe_endpoint_authorization(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let cid = js(&input, "ClusterIdentifier").map(String::from);
    let account = js(&input, "Account").map(String::from);
    let grantee = js(&input, "Grantee").map(String::from);
    let auths: Vec<Value> = cs
        .st
        .endpoint_authorizations
        .values()
        .filter(|a| {
            cid.as_ref()
                .map(|c| a.cluster_identifier == *c)
                .unwrap_or(true)
                && account.as_ref().map(|ac| a.account == *ac).unwrap_or(true)
                && grantee.as_ref().map(|g| a.account == *g).unwrap_or(true)
        })
        .map(|a| a.json())
        .collect();
    Ok(json!({"EndpointAuthorizationList": auths}))
}

fn op_revoke_endpoint_access(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let cid = j_str(&input, "ClusterIdentifier");
    let account = j_str(&input, "Account");
    if let (Some(cid), Some(account)) = (cid, account) {
        let key = format!("{cid}:{account}");
        let Some(auth) = cs.st.endpoint_authorizations.get_mut(key.as_str()) else {
            return Err(RSError::new(
                "EndpointAuthorizationNotFound",
                format!(
                    "Endpoint authorization not found for cluster {cid} and account {account}."
                ),
            ));
        };
        auth.status = "Revoking".to_string();
        return Ok(auth.json());
    }
    Ok(json!({}))
}

// ---------------------------------------------------------------------------
// Partners
// ---------------------------------------------------------------------------

fn op_describe_partners(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let cluster = js(&input, "ClusterIdentifier")
        .unwrap_or_default()
        .to_string();
    let db = js(&input, "DatabaseName").map(String::from);
    let partner = js(&input, "PartnerName").map(String::from);
    let list: Vec<Value> = cs
        .st
        .partners
        .values()
        .filter(|p| p.cluster_identifier == cluster)
        .filter(|p| db.as_ref().map(|d| p.database_name == *d).unwrap_or(true))
        .filter(|p| {
            partner
                .as_ref()
                .map(|n| p.partner_name == *n)
                .unwrap_or(true)
        })
        .map(|p| p.json())
        .collect();
    Ok(json!({"PartnerIntegrationInfoList": list}))
}

fn op_add_partner(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let cid = js(&input, "ClusterIdentifier")
        .unwrap_or_default()
        .to_string();
    cs.cluster_or_err(&cid)?;
    let db = j_str(&input, "DatabaseName").unwrap_or_default();
    let partner = j_str(&input, "PartnerName").unwrap_or_default();
    let key = format!("{cid}:{db}:{partner}");
    cs.st.partners.insert(
        key,
        Partner {
            account_id: js(&input, "AccountId").unwrap_or_default().to_string(),
            cluster_identifier: cid,
            database_name: db.to_string(),
            partner_name: partner.to_string(),
            status: "Active".to_string(),
            status_message: String::new(),
            created_at: chrono::Utc::now(),
        },
    );
    Ok(json!({
        "DatabaseName": db,
        "PartnerName": partner,
    }))
}

fn op_delete_partner(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let cid = js(&input, "ClusterIdentifier")
        .unwrap_or_default()
        .to_string();
    let db = j_str(&input, "DatabaseName").unwrap_or_default();
    let partner = j_str(&input, "PartnerName").unwrap_or_default();
    let key = format!("{cid}:{db}:{partner}");
    if cs.st.partners.remove(&key).is_none() {
        return Err(RSError::new(
            "PartnerNotFound",
            format!("Partner {partner} not found."),
        ));
    }
    Ok(json!({
        "DatabaseName": db,
        "PartnerName": partner,
    }))
}

fn op_update_partner_status(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let cid = js(&input, "ClusterIdentifier")
        .unwrap_or_default()
        .to_string();
    let db = j_str(&input, "DatabaseName").unwrap_or_default();
    let partner = j_str(&input, "PartnerName").unwrap_or_default();
    let key = format!("{cid}:{db}:{partner}");
    let Some(p) = cs.st.partners.get_mut(&key) else {
        return Err(RSError::new(
            "PartnerNotFound",
            format!("Partner {partner} not found."),
        ));
    };
    if let Some(status) = j_str(&input, "Status") {
        p.status = status.to_string();
    }
    Ok(json!({
        "DatabaseName": db,
        "PartnerName": partner,
    }))
}

// ---------------------------------------------------------------------------
// Resource policy
// ---------------------------------------------------------------------------

fn op_put_resource_policy(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let arn = js(&input, "ResourceArn").unwrap_or_default().to_string();
    let policy = json!({"ResourceArn": arn, "Policy": j_str(&input, "Policy").unwrap_or_default()});
    cs.st.resource_policies.insert(arn, policy.clone());
    Ok(json!({"ResourcePolicy": policy}))
}

fn op_get_resource_policy(cs: &mut CtxState) -> OpResult {
    let arn = js(cs.ctx.input, "ResourceArn")
        .unwrap_or_default()
        .to_string();
    if let Some(p) = cs.st.resource_policies.get(&arn) {
        return Ok(json!({"ResourcePolicy": p}));
    }
    Err(RSError::new(
        "ResourceNotFoundFault",
        format!("Resource policy for {arn} not found."),
    ))
}

fn op_delete_resource_policy(cs: &mut CtxState) -> OpResult {
    let arn = js(cs.ctx.input, "ResourceArn")
        .unwrap_or_default()
        .to_string();
    if cs.st.resource_policies.remove(&arn).is_none() {
        return Err(RSError::new(
            "ResourceNotFoundFault",
            format!("Resource policy for {arn} not found."),
        ));
    }
    Ok(Value::Null)
}

// ---------------------------------------------------------------------------
// Data shares (pure stub responses, moto style)
// ---------------------------------------------------------------------------

fn op_authorize_data_share(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let arn = js(&input, "DataShareArn").unwrap_or_default();
    let consumer = j_str(&input, "ConsumerIdentifier").unwrap_or_default();
    Ok(json!({
        "DataShareArn": arn,
        "ProducerArn": arn,
        "AllowPubliclyAccessibleConsumers": false,
        "DataShareAssociations": [{
            "ConsumerIdentifier": consumer,
            "Status": "AUTHORIZED",
            "ConsumerRegion": cs.ctx.region,
        }],
    }))
}

fn op_deauthorize_data_share(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let arn = js(&input, "DataShareArn").unwrap_or_default();
    let consumer = j_str(&input, "ConsumerIdentifier").unwrap_or_default();
    Ok(json!({
        "DataShareArn": arn,
        "ProducerArn": arn,
        "AllowPubliclyAccessibleConsumers": false,
        "DataShareAssociations": [{
            "ConsumerIdentifier": consumer,
            "Status": "DEAUTHORIZED",
            "ConsumerRegion": cs.ctx.region,
        }],
    }))
}

fn op_reject_data_share(cs: &mut CtxState) -> OpResult {
    let arn = js(cs.ctx.input, "DataShareArn").unwrap_or_default();
    Ok(json!({
        "DataShareArn": arn,
        "ProducerArn": arn,
        "AllowPubliclyAccessibleConsumers": false,
        "DataShareAssociations": [],
    }))
}

fn op_associate_data_share_consumer(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let arn = js(&input, "DataShareArn").unwrap_or_default();
    let consumer = j_str(&input, "ConsumerArn").map(String::from);
    let region = j_str(&input, "ConsumerRegion").unwrap_or(cs.ctx.region);
    Ok(json!({
        "DataShareArn": arn,
        "ProducerArn": arn,
        "AllowPubliclyAccessibleConsumers": false,
        "DataShareAssociations": [{
            "ConsumerIdentifier": consumer.unwrap_or_else(|| cs.ctx.account.to_string()),
            "Status": "ACTIVE",
            "ConsumerRegion": region,
        }],
    }))
}

fn op_disassociate_data_share_consumer(cs: &mut CtxState) -> OpResult {
    let arn = js(cs.ctx.input, "DataShareArn").unwrap_or_default();
    Ok(json!({
        "DataShareArn": arn,
        "ProducerArn": arn,
        "AllowPubliclyAccessibleConsumers": false,
        "DataShareAssociations": [],
    }))
}

// ---------------------------------------------------------------------------
// Custom domain associations
// ---------------------------------------------------------------------------

fn op_create_custom_domain_association(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let cid = js(&input, "ClusterIdentifier")
        .unwrap_or_default()
        .to_string();
    cs.cluster_or_err(&cid)?;
    Ok(json!({
        "CustomDomainName": j_str(&input, "CustomDomainName"),
        "CustomDomainCertificateArn": j_str(&input, "CustomDomainCertificateArn"),
        "ClusterIdentifier": cid,
        "CustomDomainCertExpiryTime": "2099-12-31T23:59:59Z",
    }))
}

fn op_delete_custom_domain_association(cs: &mut CtxState) -> OpResult {
    let cid = js(cs.ctx.input, "ClusterIdentifier")
        .unwrap_or_default()
        .to_string();
    cs.cluster_or_err(&cid)?;
    Ok(Value::Null)
}

fn op_modify_custom_domain_association(cs: &mut CtxState) -> OpResult {
    let input = cs.ctx.input.clone();
    let cid = js(&input, "ClusterIdentifier")
        .unwrap_or_default()
        .to_string();
    cs.cluster_or_err(&cid)?;
    Ok(json!({
        "CustomDomainName": j_str(&input, "CustomDomainName"),
        "CustomDomainCertificateArn": j_str(&input, "CustomDomainCertificateArn"),
        "ClusterIdentifier": cid,
        "CustomDomainCertExpiryTime": "2099-12-31T23:59:59Z",
    }))
}

// ---------------------------------------------------------------------------
// Describable stubs — moto returns empty lists / fixed dicts for these.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Describable stubs (moto returns fixed empty/zero responses for these)
// ---------------------------------------------------------------------------

fn stub_list(key: &str) -> OpResult {
    Ok(json!({ key.to_string(): [] }))
}

fn op_describe_account_attributes(cs: &mut CtxState) -> OpResult {
    stub_wrap(cs, "AccountAttributes")
}

fn stub_wrap(_cs: &mut CtxState, key: &str) -> OpResult {
    stub_list(key)
}

fn op_describe_cluster_db_revisions(cs: &mut CtxState) -> OpResult {
    stub_wrap(cs, "ClusterDbRevisions")
}

fn op_describe_cluster_tracks(cs: &mut CtxState) -> OpResult {
    stub_wrap(cs, "MaintenanceTracks")
}

fn op_describe_cluster_versions(cs: &mut CtxState) -> OpResult {
    stub_wrap(cs, "ClusterVersions")
}

fn op_describe_custom_domain_associations(cs: &mut CtxState) -> OpResult {
    stub_wrap(cs, "Associations")
}

fn op_describe_data_shares(cs: &mut CtxState) -> OpResult {
    stub_wrap(cs, "DataShares")
}

fn op_describe_data_shares_for_consumer(cs: &mut CtxState) -> OpResult {
    stub_wrap(cs, "DataShares")
}

fn op_describe_data_shares_for_producer(cs: &mut CtxState) -> OpResult {
    stub_wrap(cs, "DataShares")
}

fn op_describe_event_categories(cs: &mut CtxState) -> OpResult {
    stub_wrap(cs, "EventCategoriesMapList")
}

fn op_describe_events(cs: &mut CtxState) -> OpResult {
    stub_wrap(cs, "Events")
}

fn op_describe_inbound_integrations(cs: &mut CtxState) -> OpResult {
    stub_wrap(cs, "InboundIntegrations")
}

fn op_describe_integrations(cs: &mut CtxState) -> OpResult {
    stub_wrap(cs, "Integrations")
}

fn op_describe_orderable_cluster_options(cs: &mut CtxState) -> OpResult {
    stub_wrap(cs, "OrderableClusterOptions")
}

fn op_describe_redshift_idc_applications(cs: &mut CtxState) -> OpResult {
    stub_wrap(cs, "RedshiftIdcApplications")
}

fn op_describe_reserved_node_exchange_status(cs: &mut CtxState) -> OpResult {
    stub_wrap(cs, "ReservedNodeExchangeStatusDetails")
}

fn op_describe_reserved_node_offerings(cs: &mut CtxState) -> OpResult {
    stub_wrap(cs, "ReservedNodeOfferings")
}

fn op_describe_reserved_nodes(cs: &mut CtxState) -> OpResult {
    stub_wrap(cs, "ReservedNodes")
}

fn op_describe_table_restore_status(cs: &mut CtxState) -> OpResult {
    stub_wrap(cs, "TableRestoreStatusDetails")
}

fn op_list_recommendations(cs: &mut CtxState) -> OpResult {
    stub_wrap(cs, "Recommendations")
}

fn op_describe_node_configuration_options(cs: &mut CtxState) -> OpResult {
    stub_wrap(cs, "NodeConfigurationOptionList")
}

fn op_get_reserved_node_exchange_configuration_options(cs: &mut CtxState) -> OpResult {
    stub_wrap(cs, "ReservedNodeConfigurationOptionList")
}

fn op_get_reserved_node_exchange_offerings(cs: &mut CtxState) -> OpResult {
    stub_wrap(cs, "ReservedNodeOfferings")
}

/// `DescribeStorage` — moto's zero totals (suite asserts floats).
fn op_describe_storage(cs: &mut CtxState) -> OpResult {
    let _ = cs;
    Ok(json!({
        "TotalBackupSizeInMegaBytes": 0.0,
        "TotalProvisionedStorageInMegaBytes": 0.0,
    }))
}

fn op_get_identity_center_auth_token(cs: &mut CtxState) -> OpResult {
    let _ = cs;
    Ok(json!({
        "Token": "mock-token-placeholder",
        "ExpirationTime": now_iso(),
    }))
}

fn op_accept_reserved_node_exchange(cs: &mut CtxState) -> OpResult {
    let _ = cs;
    Ok(json!({"ExchangedReservedNode": {}}))
}
// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

type Handler = fn(&mut CtxState) -> OpResult;

/// moto's covered operations — anything missing falls back to the sidecar's
/// deny-by-default 501 (moto gap surface).
fn handler_for(op: &str) -> Option<Handler> {
    Some(match op {
        "AcceptReservedNodeExchange" => op_accept_reserved_node_exchange,
        "AddPartner" => op_add_partner,
        "AssociateDataShareConsumer" => op_associate_data_share_consumer,
        "AuthorizeClusterSecurityGroupIngress" => op_authorize_cluster_security_group_ingress,
        "AuthorizeDataShare" => op_authorize_data_share,
        "AuthorizeEndpointAccess" => op_authorize_endpoint_access,
        "AuthorizeSnapshotAccess" => op_authorize_snapshot_access,
        "BatchDeleteClusterSnapshots" => op_batch_delete_cluster_snapshots,
        "BatchModifyClusterSnapshots" => op_batch_modify_cluster_snapshots,
        "CancelResize" => op_resize_progress,
        "CopyClusterSnapshot" => op_copy_cluster_snapshot,
        "CreateAuthenticationProfile" => op_create_authentication_profile,
        "CreateCluster" => op_create_cluster,
        "CreateClusterParameterGroup" => op_create_cluster_parameter_group,
        "CreateClusterSecurityGroup" => op_create_cluster_security_group,
        "CreateClusterSnapshot" => op_create_cluster_snapshot,
        "CreateScheduledAction" => op_create_scheduled_action,
        "CreateClusterSubnetGroup" => op_create_cluster_subnet_group,
        "CreateCustomDomainAssociation" => op_create_custom_domain_association,
        "CreateEndpointAccess" => op_create_endpoint_access,
        "CreateEventSubscription" => op_create_event_subscription,
        "CreateHsmClientCertificate" => op_create_hsm_client_certificate,
        "CreateHsmConfiguration" => op_create_hsm_configuration,
        "CreateSnapshotCopyGrant" => op_create_snapshot_copy_grant,
        "CreateSnapshotSchedule" => op_create_snapshot_schedule,
        "CreateTags" => op_create_tags,
        "CreateUsageLimit" => op_create_usage_limit,
        "DeauthorizeDataShare" => op_deauthorize_data_share,
        "DeleteAuthenticationProfile" => op_delete_authentication_profile,
        "DeleteCluster" => op_delete_cluster,
        "DeleteClusterParameterGroup" => op_delete_cluster_parameter_group,
        "DeleteClusterSecurityGroup" => op_delete_cluster_security_group,
        "DeleteClusterSnapshot" => op_delete_cluster_snapshot,
        "DeleteClusterSubnetGroup" => op_delete_cluster_subnet_group,
        "DeleteCustomDomainAssociation" => op_delete_custom_domain_association,
        "DeleteEndpointAccess" => op_delete_endpoint_access,
        "DeleteEventSubscription" => op_delete_event_subscription,
        "DeleteHsmClientCertificate" => op_delete_hsm_client_certificate,
        "DeleteHsmConfiguration" => op_delete_hsm_configuration,
        "DeletePartner" => op_delete_partner,
        "DeleteResourcePolicy" => op_delete_resource_policy,
        "DeleteScheduledAction" => op_delete_scheduled_action,
        "DeleteSnapshotCopyGrant" => op_delete_snapshot_copy_grant,
        "DeleteSnapshotSchedule" => op_delete_snapshot_schedule,
        "DeleteTags" => op_delete_tags,
        "DeleteUsageLimit" => op_delete_usage_limit,
        "DescribeAccountAttributes" => op_describe_account_attributes,
        "DescribeAuthenticationProfiles" => op_describe_authentication_profiles,
        "DescribeClusterDbRevisions" => op_describe_cluster_db_revisions,
        "DescribeClusterParameterGroups" => op_describe_cluster_parameter_groups,
        "DescribeClusterParameters" => op_describe_cluster_parameters,
        "DescribeClusterSecurityGroups" => op_describe_cluster_security_groups,
        "DescribeClusterSnapshots" => op_describe_cluster_snapshots,
        "DescribeClusterSubnetGroups" => op_describe_cluster_subnet_groups,
        "DescribeClusterTracks" => op_describe_cluster_tracks,
        "DescribeClusterVersions" => op_describe_cluster_versions,
        "DescribeClusters" => op_describe_clusters,
        "DescribeCustomDomainAssociations" => op_describe_custom_domain_associations,
        "DescribeDataShares" => op_describe_data_shares,
        "DescribeDataSharesForConsumer" => op_describe_data_shares_for_consumer,
        "DescribeDataSharesForProducer" => op_describe_data_shares_for_producer,
        "DescribeDefaultClusterParameters" => op_describe_default_cluster_parameters,
        "DescribeEndpointAccess" => op_describe_endpoint_access,
        "DescribeEndpointAuthorization" => op_describe_endpoint_authorization,
        "DescribeEventCategories" => op_describe_event_categories,
        "DescribeEventSubscriptions" => op_describe_event_subscriptions,
        "DescribeEvents" => op_describe_events,
        "DescribeHsmClientCertificates" => op_describe_hsm_client_certificates,
        "DescribeHsmConfigurations" => op_describe_hsm_configurations,
        "DescribeInboundIntegrations" => op_describe_inbound_integrations,
        "DescribeIntegrations" => op_describe_integrations,
        "DescribeLoggingStatus" => op_describe_logging_status,
        "DescribeNodeConfigurationOptions" => op_describe_node_configuration_options,
        "DescribeOrderableClusterOptions" => op_describe_orderable_cluster_options,
        "DescribePartners" => op_describe_partners,
        "DescribeRedshiftIdcApplications" => op_describe_redshift_idc_applications,
        "DescribeReservedNodeExchangeStatus" => op_describe_reserved_node_exchange_status,
        "DescribeReservedNodeOfferings" => op_describe_reserved_node_offerings,
        "DescribeReservedNodes" => op_describe_reserved_nodes,
        "DescribeResize" => op_resize_progress,
        "DescribeScheduledActions" => op_describe_scheduled_actions,
        "DescribeSnapshotCopyGrants" => op_describe_snapshot_copy_grants,
        "DescribeSnapshotSchedules" => op_describe_snapshot_schedules,
        "DescribeStorage" => op_describe_storage,
        "DescribeTableRestoreStatus" => op_describe_table_restore_status,
        "DescribeTags" => op_describe_tags,
        "DescribeUsageLimits" => op_describe_usage_limits,
        "DisableLogging" => op_disable_logging,
        "DisableSnapshotCopy" => op_disable_snapshot_copy,
        "DisassociateDataShareConsumer" => op_disassociate_data_share_consumer,
        "EnableLogging" => op_enable_logging,
        "EnableSnapshotCopy" => op_enable_snapshot_copy,
        "GetClusterCredentials" => op_get_cluster_credentials,
        "GetClusterCredentialsWithIAM" => op_get_cluster_credentials_with_iam,
        "GetIdentityCenterAuthToken" => op_get_identity_center_auth_token,
        "GetReservedNodeExchangeConfigurationOptions" => {
            op_get_reserved_node_exchange_configuration_options
        }
        "GetReservedNodeExchangeOfferings" => op_get_reserved_node_exchange_offerings,
        "GetResourcePolicy" => op_get_resource_policy,
        "ListRecommendations" => op_list_recommendations,
        "ModifyAquaConfiguration" => op_modify_aqua_configuration,
        "ModifyAuthenticationProfile" => op_modify_authentication_profile,
        "ModifyCluster" => op_modify_cluster,
        "ModifyClusterDbRevision" => op_cluster_passthrough,
        "ModifyClusterIamRoles" => op_modify_cluster_iam_roles,
        "ModifyClusterMaintenance" => op_cluster_passthrough,
        "ModifyClusterParameterGroup" => op_modify_cluster_parameter_group,
        "ModifyClusterSnapshot" => op_modify_cluster_snapshot,
        "ModifyClusterSnapshotSchedule" => op_modify_cluster_snapshot_schedule,
        "ModifyClusterSubnetGroup" => op_modify_cluster_subnet_group,
        "ModifyCustomDomainAssociation" => op_modify_custom_domain_association,
        "ModifyEndpointAccess" => op_modify_endpoint_access,
        "ModifyEventSubscription" => op_modify_event_subscription,
        "ModifyScheduledAction" => op_modify_scheduled_action,
        "ModifySnapshotCopyRetentionPeriod" => op_modify_snapshot_copy_retention_period,
        "ModifySnapshotSchedule" => op_modify_snapshot_schedule,
        "ModifyUsageLimit" => op_modify_usage_limit,
        "PauseCluster" => op_pause_cluster,
        "PutResourcePolicy" => op_put_resource_policy,
        "RebootCluster" => op_reboot_cluster,
        "RejectDataShare" => op_reject_data_share,
        "ResetClusterParameterGroup" => op_modify_cluster_parameter_group,
        "ResumeCluster" => op_resume_cluster,
        "RevokeClusterSecurityGroupIngress" => op_revoke_cluster_security_group_ingress,
        "RevokeEndpointAccess" => op_revoke_endpoint_access,
        "RestoreFromClusterSnapshot" => op_restore_from_cluster_snapshot,
        "RevokeSnapshotAccess" => op_revoke_snapshot_access,
        "RotateEncryptionKey" => op_rotate_encryption_key,
        "UpdatePartnerStatus" => op_update_partner_status,
        _ => return None,
    })
}

/// States seeded with moto's `Default`/`default.redshift-1.0` first group.
fn seeded_state(account: u64, region: &str) -> RedshiftState {
    RedshiftState {
        security_groups: seed_security_groups(account, region),
        parameter_groups: seed_parameter_groups(account, region),
        ..RedshiftState::default()
    }
}

/// Entry point: route a redshift op to its handler. Unknown ops (moto's own
/// gap surface) answer 501 NotImplemented exactly like the sidecar does.
pub fn dispatch(req: &Request) -> Response {
    let op = if req.operation.is_empty() {
        "Unknown".to_string()
    } else {
        req.operation.trim().to_string()
    };
    let input = parse_input(req, &op);
    let handler = handler_for(&op);
    let slot = store::slot::<RedshiftState>("redshift");
    let mut map = slot.write();
    let st = map
        .entry((req.account, req.region.clone()))
        .or_insert_with(|| seeded_state(req.account, &req.region));
    let mut ctx = CtxState {
        ctx: Ctx {
            req,
            input: &input,
            account: req.account,
            region: req.region.as_str(),
        },
        st,
    };
    match handler {
        Some(h) => match h(&mut ctx) {
            Ok(Value::Null) => empty_success(&op),
            Ok(value) => success(&op, &value),
            Err(e) => e.wire(),
        },
        None => RSError::not_implemented(&op).wire(),
    }
}

// ---------------------------------------------------------------------------
// Unit tests (per family) — one isolated account per test
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::HashMap;

    struct TestCtx {
        account: u64,
    }

    impl TestCtx {
        fn run(&self, action: &str, params: &[(&str, &str)]) -> Response {
            let mut body = format!("Action={action}&Version=2012-12-01");
            for (k, v) in params {
                body.push_str(&format!("&{k}={v}"));
            }
            let request = Request {
                service: "redshift".into(),
                operation: action.into(),
                method: "POST".into(),
                path: "/".into(),
                query_string: String::new(),
                headers: HashMap::from([(
                    "content-type".to_string(),
                    "application/x-www-form-urlencoded".to_string(),
                )]),
                body: Bytes::from(body),
                account: self.account,
                region: "us-east-1".into(),
            };
            dispatch(&request)
        }

        fn create_cluster(&self, cid: &str) -> Response {
            self.run(
                "CreateCluster",
                &[
                    ("ClusterIdentifier", cid),
                    ("NodeType", "dc2.large"),
                    ("MasterUsername", "admin"),
                    ("MasterUserPassword", "Password1!"),
                    ("NumberOfNodes", "1"),
                    ("ClusterType", "single-node"),
                ],
            )
        }
    }

    fn ctx() -> TestCtx {
        TestCtx {
            account: (uuid::Uuid::new_v4().as_u128() % 100_000_000_000_000) as u64,
        }
    }

    fn body_str(resp: &Response) -> String {
        String::from_utf8_lossy(&resp.body).to_string()
    }

    #[test]
    fn create_cluster_wire_shape() {
        let cx = ctx();
        let resp = cx.create_cluster("tst-create-wire");
        assert_eq!(resp.status, 200, "{}", body_str(&resp));
        let b = body_str(&resp);
        assert!(b.starts_with(
            "<CreateClusterResponse xmlns=\"http://redshift.amazonaws.com/doc/2012-12-01/\">"
        ));
        for tag in [
            "<CreateClusterResult><Cluster><ClusterIdentifier>tst-create-wire</ClusterIdentifier>",
            "<NodeType>dc2.large</NodeType>",
            "<ClusterStatus>creating</ClusterStatus>",
            "<MasterUsername>admin</MasterUsername><DBName>dev</DBName>",
            "<Port>5439</Port>",
            "<AutomatedSnapshotRetentionPeriod>1</AutomatedSnapshotRetentionPeriod>",
            "<ClusterSecurityGroups><ClusterSecurityGroup><ClusterSecurityGroupName>Default</ClusterSecurityGroupName><Status>active</Status></ClusterSecurityGroup></ClusterSecurityGroups>",
            "<VpcSecurityGroups/><ClusterParameterGroups><ClusterParameterGroup><ParameterGroupName>default.redshift-1.0</ParameterGroupName><ParameterApplyStatus>in-sync</ParameterApplyStatus></ClusterParameterGroup></ClusterParameterGroups>",
            "<AvailabilityZone>us-east-1a</AvailabilityZone>",
            "<PreferredMaintenanceWindow>Mon:03:00-Mon:03:30</PreferredMaintenanceWindow>",
            "<PendingModifiedValues/><ClusterVersion>1.0</ClusterVersion>",
            "<AllowVersionUpgrade>true</AllowVersionUpgrade><NumberOfNodes>1</NumberOfNodes>",
            "<PubliclyAccessible>false</PubliclyAccessible><Encrypted>false</Encrypted><Tags/>",
            "<EnhancedVpcRouting>false</EnhancedVpcRouting><IamRoles/><TotalStorageCapacityInMegaBytes>0</TotalStorageCapacityInMegaBytes>",
        ] {
            assert!(b.contains(tag), "missing {tag} in {b}");
        }
        assert!(b.contains("<ResponseMetadata><RequestId>"));
        // create also mints the automated snapshot
        let desc = body_str(&cx.run(
            "DescribeClusterSnapshots",
            &[("ClusterIdentifier", "tst-create-wire")],
        ));
        assert!(
            desc.contains("<SnapshotIdentifier>rs:tst-create-wire-"),
            "{desc}"
        );
        cx.run(
            "DeleteCluster",
            &[
                ("ClusterIdentifier", "tst-create-wire"),
                ("SkipFinalClusterSnapshot", "true"),
            ],
        );
    }

    #[test]
    fn duplicate_cluster_fails() {
        let cx = ctx();
        let _ = cx.create_cluster("tst-dup-cluster");
        let resp = cx.create_cluster("tst-dup-cluster");
        assert_eq!(resp.status, 400);
        let b = body_str(&resp);
        assert!(b.contains("<Code>ClusterAlreadyExists</Code>"), "{b}");
        let prefix = format!(
            "<ErrorResponse xmlns=\"{XMLNS}\"><Error><Type>Sender</Type><Code>ClusterAlreadyExists</Code><Message>Cluster already exists</Message></Error><RequestId>"
        );
        assert!(b.starts_with(&prefix), "{b}");
        cx.run(
            "DeleteCluster",
            &[
                ("ClusterIdentifier", "tst-dup-cluster"),
                ("SkipFinalClusterSnapshot", "true"),
            ],
        );
    }

    #[test]
    fn describe_clusters_not_found() {
        let cx = ctx();
        let resp = cx.run(
            "DescribeClusters",
            &[("ClusterIdentifier", "never-existed")],
        );
        assert_eq!(resp.status, 404, "{}", body_str(&resp));
        let b = body_str(&resp);
        assert!(b.contains("<Code>ClusterNotFound</Code>"));
        assert!(b.contains("<Message>Cluster never-existed not found.</Message>"));
        assert!(b.contains("xmlns=\"http://redshift.amazonaws.com/doc/2012-12-01/\""));
        assert!(b.contains("<Type>Sender</Type>"));
    }

    #[test]
    fn modify_cluster_resizes() {
        let cx = ctx();
        let _ = cx.create_cluster("tst-modify");
        let resp = cx.run(
            "ModifyCluster",
            &[
                ("ClusterIdentifier", "tst-modify"),
                ("NumberOfNodes", "2"),
                ("ClusterType", "multi-node"),
                ("NodeType", "dc2.large"),
            ],
        );
        assert_eq!(resp.status, 200);
        let b = body_str(&resp);
        assert!(b.contains("<NumberOfNodes>2</NumberOfNodes>"), "{b}");
        assert!(b.contains("<ClusterStatus>available</ClusterStatus>"));
        cx.run(
            "DeleteCluster",
            &[
                ("ClusterIdentifier", "tst-modify"),
                ("SkipFinalClusterSnapshot", "true"),
            ],
        );
    }

    #[test]
    fn modify_cluster_multi_node_requires_two() {
        let cx = ctx();
        let _ = cx.create_cluster("tst-modify-1");
        let resp = cx.run(
            "ModifyCluster",
            &[
                ("ClusterIdentifier", "tst-modify-1"),
                ("NumberOfNodes", "1"),
                ("ClusterType", "multi-node"),
            ],
        );
        assert_eq!(resp.status, 400);
        assert!(body_str(&resp).contains("<Code>InvalidParameterCombination</Code>"));
        cx.run(
            "DeleteCluster",
            &[
                ("ClusterIdentifier", "tst-modify-1"),
                ("SkipFinalClusterSnapshot", "true"),
            ],
        );
    }

    #[test]
    fn delete_cluster_requires_snapshot_identifier() {
        let cx = ctx();
        let _ = cx.create_cluster("tst-del-req");
        let resp = cx.run(
            "DeleteCluster",
            &[
                ("ClusterIdentifier", "tst-del-req"),
                ("SkipFinalClusterSnapshot", "false"),
            ],
        );
        assert_eq!(resp.status, 400);
        assert!(body_str(&resp).contains("FinalClusterSnapshotIdentifier is required"));
        cx.run(
            "DeleteCluster",
            &[
                ("ClusterIdentifier", "tst-del-req"),
                ("SkipFinalClusterSnapshot", "true"),
            ],
        );
    }

    #[test]
    fn pause_resume_status_cycles() {
        let cx = ctx();
        let _ = cx.create_cluster("tst-pause");
        let pause = body_str(&cx.run("PauseCluster", &[("ClusterIdentifier", "tst-pause")]));
        assert!(
            pause.contains("<ClusterStatus>paused</ClusterStatus>"),
            "{pause}"
        );
        let resume = body_str(&cx.run("ResumeCluster", &[("ClusterIdentifier", "tst-pause")]));
        assert!(resume.contains("<ClusterStatus>available</ClusterStatus>"));
        cx.run(
            "DeleteCluster",
            &[
                ("ClusterIdentifier", "tst-pause"),
                ("SkipFinalClusterSnapshot", "true"),
            ],
        );
    }

    #[test]
    fn snapshot_create_describe_delete_cycle() {
        let cx = ctx();
        let _ = cx.create_cluster("tst-snap-cluster");
        let create = cx.run(
            "CreateClusterSnapshot",
            &[
                ("SnapshotIdentifier", "tst-snap"),
                ("ClusterIdentifier", "tst-snap-cluster"),
                ("Tags.Tag.1.Key", "k"),
                ("Tags.Tag.1.Value", "v"),
            ],
        );
        assert_eq!(create.status, 200);
        let create_body = body_str(&create);
        assert!(
            create_body.contains("<SnapshotType>manual</SnapshotType>"),
            "{create_body}"
        );
        assert!(
            create_body.contains("<Tags><Tag><Key>k</Key><Value>v</Value></Tag></Tags>"),
            "{create_body}"
        );
        let dup = cx.run(
            "CreateClusterSnapshot",
            &[
                ("SnapshotIdentifier", "tst-snap"),
                ("ClusterIdentifier", "tst-snap-cluster"),
            ],
        );
        assert_eq!(dup.status, 400);
        assert!(body_str(&dup).contains("<Code>ClusterSnapshotAlreadyExists</Code>"));
        let descr = body_str(&cx.run(
            "DescribeClusterSnapshots",
            &[("SnapshotIdentifier", "tst-snap")],
        ));
        assert!(descr.contains("tst-snap-cluster"), "{descr}");
        let dele = body_str(&cx.run(
            "DeleteClusterSnapshot",
            &[("SnapshotIdentifier", "tst-snap")],
        ));
        assert!(dele.contains("<Status>deleted</Status>"), "{dele}");
        cx.run(
            "DeleteCluster",
            &[
                ("ClusterIdentifier", "tst-snap-cluster"),
                ("SkipFinalClusterSnapshot", "true"),
            ],
        );
    }

    #[test]
    fn delete_automated_snapshot_rejected() {
        let cx = ctx();
        let _ = cx.create_cluster("tst-autosnap");
        let desc = body_str(&cx.run(
            "DescribeClusterSnapshots",
            &[("ClusterIdentifier", "tst-autosnap")],
        ));
        assert!(
            desc.contains("<SnapshotType>automated</SnapshotType>"),
            "{desc}"
        );
        let open = desc
            .split("<SnapshotIdentifier>")
            .nth(1)
            .expect("snapshot in list");
        let auto_id = &open[..open.find("</SnapshotIdentifier>").expect("closed")];
        let resp = cx.run("DeleteClusterSnapshot", &[("SnapshotIdentifier", auto_id)]);
        assert_eq!(resp.status, 400);
        assert!(
            body_str(&resp).contains("<Code>InvalidClusterSnapshotState</Code>"),
            "{}",
            body_str(&resp)
        );
        cx.run(
            "DeleteCluster",
            &[
                ("ClusterIdentifier", "tst-autosnap"),
                ("SkipFinalClusterSnapshot", "true"),
            ],
        );
    }

    #[test]
    fn restore_from_snapshot_inherits() {
        let cx = ctx();
        let _ = cx.create_cluster("tst-restore-src");
        let _ = cx.run(
            "CreateClusterSnapshot",
            &[
                ("SnapshotIdentifier", "tst-restore-snap"),
                ("ClusterIdentifier", "tst-restore-src"),
            ],
        );
        let restore = body_str(&cx.run(
            "RestoreFromClusterSnapshot",
            &[
                ("ClusterIdentifier", "tst-restore-dest"),
                ("SnapshotIdentifier", "tst-restore-snap"),
            ],
        ));
        assert!(
            restore.contains("<ClusterIdentifier>tst-restore-dest</ClusterIdentifier>"),
            "{restore}"
        );
        assert!(restore.contains("<ClusterStatus>creating</ClusterStatus>"));
        assert!(
            restore.contains("<RestoreStatus><Status>completed</Status>"),
            "{restore}"
        );
        assert!(
            restore.contains("<CurrentRestoreRateInMegaBytesPerSecond>123.0</CurrentRestoreRateInMegaBytesPerSecond>"),
            "{restore}"
        );
        cx.run(
            "DeleteCluster",
            &[
                ("ClusterIdentifier", "tst-restore-dest"),
                ("SkipFinalClusterSnapshot", "true"),
            ],
        );
        cx.run(
            "DeleteClusterSnapshot",
            &[("SnapshotIdentifier", "tst-restore-snap")],
        );
        cx.run(
            "DeleteCluster",
            &[
                ("ClusterIdentifier", "tst-restore-src"),
                ("SkipFinalClusterSnapshot", "true"),
            ],
        );
    }

    #[test]
    fn batch_modify_reports_missing_snapshots() {
        let cx = ctx();
        let resp = cx.run(
            "BatchModifyClusterSnapshots",
            &[("SnapshotIdentifierList.String.1", "tst-missing-snap")],
        );
        assert_eq!(resp.status, 200);
        let b = body_str(&resp);
        assert!(b.contains("<Resources/>"), "{b}");
        assert!(
            b.contains("<Errors><SnapshotErrorMessage><SnapshotIdentifier>tst-missing-snap</SnapshotIdentifier></SnapshotErrorMessage></Errors>"),
            "{b}"
        );
    }

    // -- parameter groups -----------------------------------------------------

    #[test]
    fn parameter_group_crud() {
        let cx = ctx();
        let create = body_str(&cx.run(
            "CreateClusterParameterGroup",
            &[
                ("ParameterGroupName", "tst-pg"),
                ("ParameterGroupFamily", "redshift-1.0"),
                ("Description", "Test parameter group"),
            ],
        ));
        assert!(
            create.contains("<ParameterGroupName>tst-pg</ParameterGroupName>"),
            "{create}"
        );
        let descr = body_str(&cx.run(
            "DescribeClusterParameterGroups",
            &[("ParameterGroupName", "tst-pg")],
        ));
        assert!(descr.contains("redshift-1.0"), "{descr}");
        let params = body_str(&cx.run(
            "DescribeClusterParameters",
            &[("ParameterGroupName", "tst-pg")],
        ));
        assert!(
            params.contains("<ParameterName>auto_analyze</ParameterName>"),
            "{params}"
        );
        let defs = body_str(&cx.run(
            "DescribeDefaultClusterParameters",
            &[("ParameterGroupFamily", "redshift-1.0")],
        ));
        assert!(
            defs.contains("<DefaultClusterParameters><ParameterGroupFamily>redshift-1.0</ParameterGroupFamily>"),
            "{defs}"
        );
        let modify = body_str(&cx.run(
            "ModifyClusterParameterGroup",
            &[
                ("ParameterGroupName", "tst-pg"),
                ("Parameters.Parameter.1.ParameterName", "x"),
                ("Parameters.Parameter.1.ParameterValue", "1"),
            ],
        ));
        assert!(
            modify.contains("Your parameter group has been updated."),
            "{modify}"
        );
        let del = cx.run(
            "DeleteClusterParameterGroup",
            &[("ParameterGroupName", "tst-pg")],
        );
        assert_eq!(del.status, 200);
        let nf = cx.run(
            "DescribeClusterParameterGroups",
            &[("ParameterGroupName", "tst-pg")],
        );
        assert_eq!(nf.status, 404, "{}", body_str(&nf));
    }

    #[test]
    fn default_parameter_group_seeds() {
        let cx = ctx();
        let b = body_str(&cx.run("DescribeClusterParameterGroups", &[]));
        assert!(b.contains("default.redshift-1.0"), "{b}");
        let b2 = body_str(&cx.run("DescribeClusterSecurityGroups", &[]));
        assert!(b2.contains("Default Redshift Security Group"), "{b2}");
    }

    // -- security groups ------------------------------------------------------

    #[test]
    fn security_group_authorize_revoke() {
        let cx = ctx();
        let create = body_str(&cx.run(
            "CreateClusterSecurityGroup",
            &[
                ("ClusterSecurityGroupName", "tst-csg"),
                ("Description", "d"),
            ],
        ));
        assert!(
            create.contains("<EC2SecurityGroups/><IPRanges/>"),
            "{create}"
        );
        let auth = body_str(&cx.run(
            "AuthorizeClusterSecurityGroupIngress",
            &[
                ("ClusterSecurityGroupName", "tst-csg"),
                ("CIDRIP", "10.0.0.0/24"),
            ],
        ));
        assert!(
            auth.contains("<IPRanges><IPRange><Status>authorized</Status><CIDRIP>10.0.0.0/24</CIDRIP><Tags/></IPRange></IPRanges>"),
            "{auth}"
        );
        let revoke = body_str(&cx.run(
            "RevokeClusterSecurityGroupIngress",
            &[
                ("ClusterSecurityGroupName", "tst-csg"),
                ("CIDRIP", "10.0.0.0/24"),
            ],
        ));
        assert!(revoke.contains("<IPRanges/>"), "{revoke}");
        cx.run(
            "DeleteClusterSecurityGroup",
            &[("ClusterSecurityGroupName", "tst-csg")],
        );
        let nf = cx.run(
            "DescribeClusterSecurityGroups",
            &[("ClusterSecurityGroupName", "tst-csg")],
        );
        assert_eq!(nf.status, 404);
    }

    // -- subnet groups ----------------------------------------------------------

    #[test]
    fn subnet_group_invalid_subnet_error() {
        let cx = ctx();
        // no EC2 mirror in unit tests: resolution fails exactly like moto
        let resp = cx.run(
            "CreateClusterSubnetGroup",
            &[
                ("ClusterSubnetGroupName", "tst-sng"),
                ("Description", "Test subnet group"),
                ("SubnetIds.SubnetIdentifier.1", "subnet-00000000000000000"),
            ],
        );
        assert_eq!(resp.status, 400, "{}", body_str(&resp));
        let b = body_str(&resp);
        assert!(b.contains("<Code>InvalidSubnet</Code>"), "{b}");
        assert!(
            b.contains("Subnet [&#39;subnet-00000000000000000&#39;] not found."),
            "{b}"
        );
    }

    // -- tags -------------------------------------------------------------------

    #[test]
    fn tags_create_describe_delete() {
        let cx = ctx();
        let _ = cx.create_cluster("tst-tags-cluster");
        let arn = "arn:aws:redshift:us-east-1:123456789012:cluster:tst-tags-cluster";
        let resp = cx.run(
            "CreateTags",
            &[
                ("ResourceName", arn),
                ("Tags.Tag.1.Key", "env"),
                ("Tags.Tag.1.Value", "test"),
                ("Tags.Tag.2.Key", "b"),
                ("Tags.Tag.2.Value", "2"),
            ],
        );
        assert_eq!(resp.status, 200);
        let descr = body_str(&cx.run("DescribeTags", &[("ResourceName", arn)]));
        assert!(
            descr.contains("<Tag><Key>env</Key><Value>test</Value></Tag>"),
            "{descr}"
        );
        assert!(descr.contains("<ResourceType>cluster</ResourceType>"));
        cx.run(
            "DeleteTags",
            &[("ResourceName", arn), ("TagKeys.TagKey.1", "env")],
        );
        let after = body_str(&cx.run("DescribeTags", &[("ResourceName", arn)]));
        assert!(after.contains("<Key>b</Key>"), "{after}");
        assert!(!after.contains("<Key>env</Key>"), "{after}");
        // unknown resource id -> ResourceNotFoundFault
        let nf = cx.run(
            "CreateTags",
            &[
                (
                    "ResourceName",
                    "arn:aws:redshift:us-east-1:123456789012:parametergroup:missing",
                ),
                ("Tags.Tag.1.Key", "a"),
                ("Tags.Tag.1.Value", "b"),
            ],
        );
        assert_eq!(nf.status, 404);
        assert!(
            body_str(&nf).contains("parametergroup (missing) not found."),
            "{}",
            body_str(&nf)
        );
        cx.run(
            "DeleteCluster",
            &[
                ("ClusterIdentifier", "tst-tags-cluster"),
                ("SkipFinalClusterSnapshot", "true"),
            ],
        );
    }

    // -- credentials ------------------------------------------------------------

    #[test]
    fn get_cluster_credentials_prefixes() {
        let cx = ctx();
        let _ = cx.create_cluster("tst-creds");
        let creds = body_str(&cx.run(
            "GetClusterCredentials",
            &[("ClusterIdentifier", "tst-creds"), ("DbUser", "admin")],
        ));
        assert!(creds.contains("<DbUser>IAM:admin</DbUser>"), "{creds}");
        assert!(creds.contains("<DbPassword>") && creds.contains("</DbPassword>"));
        assert!(creds.contains("<Expiration>"));
        let auto = body_str(&cx.run(
            "GetClusterCredentials",
            &[
                ("ClusterIdentifier", "tst-creds"),
                ("DbUser", "newuser"),
                ("AutoCreate", "true"),
            ],
        ));
        assert!(auto.contains("<DbUser>IAMA:newuser</DbUser>"), "{auto}");
        cx.run(
            "DeleteCluster",
            &[
                ("ClusterIdentifier", "tst-creds"),
                ("SkipFinalClusterSnapshot", "true"),
            ],
        );
    }

    #[test]
    fn credentials_with_iam_needs_no_cluster() {
        let cx = ctx();
        let creds = body_str(&cx.run("GetClusterCredentialsWithIAM", &[]));
        assert!(creds.contains("<DbUser>IAM:user</DbUser>"), "{creds}");
        assert!(creds.contains("<NextRefreshTime>"));
    }

    #[test]
    fn duration_bounds_enforced() {
        let cx = ctx();
        let resp = cx.run(
            "GetClusterCredentials",
            &[
                ("ClusterIdentifier", "some-cluster"),
                ("DbUser", "admin"),
                ("DurationSeconds", "60"),
            ],
        );
        assert_eq!(resp.status, 400);
        assert!(
            body_str(&resp).contains("Token duration must be between 900 and 3600 seconds"),
            "{}",
            body_str(&resp)
        );
    }

    // -- logging ----------------------------------------------------------------

    #[test]
    fn logging_enable_disable_describe() {
        let cx = ctx();
        let _ = cx.create_cluster("tst-log");
        let pre = body_str(&cx.run("DescribeLoggingStatus", &[("ClusterIdentifier", "tst-log")]));
        assert!(
            pre.contains("<LoggingEnabled>false</LoggingEnabled>"),
            "{pre}"
        );
        assert!(pre.contains("<BucketName/>"), "{pre}");
        let en = body_str(&cx.run(
            "EnableLogging",
            &[
                ("ClusterIdentifier", "tst-log"),
                ("BucketName", "rs-log-bkt"),
                ("S3KeyPrefix", "logs/"),
            ],
        ));
        assert!(en.contains("<LoggingEnabled>true</LoggingEnabled>"), "{en}");
        assert!(en.contains("<BucketName>rs-log-bkt</BucketName>"), "{en}");
        assert!(en.contains("<S3KeyPrefix>logs/</S3KeyPrefix>"), "{en}");
        // unpassed params overwrite with None and drop out of the wire shape
        let en2 = body_str(&cx.run(
            "EnableLogging",
            &[
                ("ClusterIdentifier", "tst-log"),
                ("LogDestinationType", "cloudwatch"),
                ("LogExports.LogExport.1", "connectionlog"),
            ],
        ));
        assert!(
            en2.contains("<LogDestinationType>cloudwatch</LogDestinationType>"),
            "{en2}"
        );
        assert!(!en2.contains("BucketName"), "{en2}");
        let dis = body_str(&cx.run("DisableLogging", &[("ClusterIdentifier", "tst-log")]));
        assert!(
            dis.contains("<LoggingEnabled>false</LoggingEnabled>"),
            "{dis}"
        );
        cx.run(
            "DeleteCluster",
            &[
                ("ClusterIdentifier", "tst-log"),
                ("SkipFinalClusterSnapshot", "true"),
            ],
        );
    }

    // -- snapshot copy toggles --------------------------------------------------

    #[test]
    fn snapshot_copy_enable_disable_cycle() {
        let cx = ctx();
        let _ = cx.create_cluster("tst-copy");
        let en = body_str(&cx.run(
            "EnableSnapshotCopy",
            &[
                ("ClusterIdentifier", "tst-copy"),
                ("DestinationRegion", "us-west-2"),
            ],
        ));
        assert!(
            en.contains("<ClusterSnapshotCopyStatus><DestinationRegion>us-west-2</DestinationRegion><RetentionPeriod>7</RetentionPeriod>"),
            "{en}"
        );
        let dup = cx.run(
            "EnableSnapshotCopy",
            &[
                ("ClusterIdentifier", "tst-copy"),
                ("DestinationRegion", "us-west-2"),
            ],
        );
        assert_eq!(dup.status, 400);
        assert!(body_str(&dup).contains("<Code>SnapshotCopyAlreadyEnabledFault</Code>"));
        let mod_ret = cx.run(
            "ModifySnapshotCopyRetentionPeriod",
            &[("ClusterIdentifier", "tst-copy"), ("RetentionPeriod", "14")],
        );
        assert_eq!(mod_ret.status, 200);
        // moto renders {"Clusters": [...]} as an empty result element
        assert!(
            body_str(&mod_ret).contains("<ModifySnapshotCopyRetentionPeriodResult/>"),
            "{}",
            body_str(&mod_ret)
        );
        let _dis = cx.run("DisableSnapshotCopy", &[("ClusterIdentifier", "tst-copy")]);
        let dis2 = cx.run("DisableSnapshotCopy", &[("ClusterIdentifier", "tst-copy")]);
        assert_eq!(dis2.status, 400);
        cx.run(
            "DeleteCluster",
            &[
                ("ClusterIdentifier", "tst-copy"),
                ("SkipFinalClusterSnapshot", "true"),
            ],
        );
    }

    // -- copy grants, schedules, scheduled actions, usage limits ----------------

    #[test]
    fn snapshot_copy_grant_lifecycle() {
        let cx = ctx();
        let create = body_str(&cx.run(
            "CreateSnapshotCopyGrant",
            &[("SnapshotCopyGrantName", "tst-scg")],
        ));
        assert!(
            create.contains("<SnapshotCopyGrantName>tst-scg</SnapshotCopyGrantName>"),
            "{create}"
        );
        let dup = cx.run(
            "CreateSnapshotCopyGrant",
            &[("SnapshotCopyGrantName", "tst-scg")],
        );
        assert_eq!(dup.status, 400);
        let descr = body_str(&cx.run(
            "DescribeSnapshotCopyGrants",
            &[("SnapshotCopyGrantName", "tst-scg")],
        ));
        assert!(descr.contains("tst-scg"), "{descr}");
        cx.run(
            "DeleteSnapshotCopyGrant",
            &[("SnapshotCopyGrantName", "tst-scg")],
        );
        let nf = cx.run(
            "DescribeSnapshotCopyGrants",
            &[("SnapshotCopyGrantName", "tst-scg")],
        );
        assert_eq!(nf.status, 400);
    }

    #[test]
    fn snapshot_schedule_lifecycle() {
        let cx = ctx();
        let create = body_str(&cx.run(
            "CreateSnapshotSchedule",
            &[
                ("ScheduleIdentifier", "tst-sched"),
                ("ScheduleDefinitions.ScheduleDefinition.1", "rate(12 hours)"),
                ("Tags.Tag.1.Key", "a"),
                ("Tags.Tag.1.Value", "b"),
            ],
        ));
        assert!(
            create.contains("<ScheduleDefinitions><ScheduleDefinition>rate(12 hours)</ScheduleDefinition></ScheduleDefinitions>"),
            "{create}"
        );
        assert!(
            create.contains("<ScheduleIdentifier>tst-sched</ScheduleIdentifier>"),
            "{create}"
        );
        assert!(
            create.contains("<AssociatedClusterCount>0</AssociatedClusterCount>"),
            "{create}"
        );
        let by_id = body_str(&cx.run(
            "DescribeSnapshotSchedules",
            &[("ScheduleIdentifier", "tst-sched")],
        ));
        assert!(by_id.contains("tst-sched"), "{by_id}");
        let modify = body_str(&cx.run(
            "ModifySnapshotSchedule",
            &[
                ("ScheduleIdentifier", "tst-sched"),
                ("ScheduleDefinitions.ScheduleDefinition.1", "rate(24 hours)"),
            ],
        ));
        assert!(modify.contains("rate(24 hours)"), "{modify}");
        let del = cx.run(
            "DeleteSnapshotSchedule",
            &[("ScheduleIdentifier", "tst-sched")],
        );
        assert_eq!(del.status, 200);
        let gone = body_str(&cx.run(
            "DescribeSnapshotSchedules",
            &[("ScheduleIdentifier", "tst-sched")],
        ));
        assert!(gone.contains("<SnapshotSchedules/>"), "{gone}");
    }

    #[test]
    fn scheduled_action_lifecycle() {
        let cx = ctx();
        let create = body_str(&cx.run(
            "CreateScheduledAction",
            &[
                ("ScheduledActionName", "tst-sa"),
                ("TargetAction.ResizeCluster.ClusterIdentifier", "fake"),
                ("TargetAction.ResizeCluster.NumberOfNodes", "2"),
                ("Schedule", "rate(1 hour)"),
                ("IamRole", "arn:aws:iam::123456789012:role/r"),
            ],
        ));
        assert!(create.contains("<State>ACTIVE</State>"), "{create}");
        assert!(
            create.contains("<TargetAction><ResizeCluster><ClusterIdentifier>fake</ClusterIdentifier><NumberOfNodes>2</NumberOfNodes></ResizeCluster></TargetAction>"),
            "{create}"
        );
        let descr = body_str(&cx.run(
            "DescribeScheduledActions",
            &[("ScheduledActionName", "tst-sa")],
        ));
        assert!(
            descr.contains("<ScheduledActionName>tst-sa</ScheduledActionName>"),
            "{descr}"
        );
        let modified = body_str(&cx.run(
            "ModifyScheduledAction",
            &[
                ("ScheduledActionName", "tst-sa"),
                ("Schedule", "rate(2 hours)"),
            ],
        ));
        assert!(
            modified.contains("<Schedule>rate(2 hours)</Schedule>"),
            "{modified}"
        );
        let del = cx.run(
            "DeleteScheduledAction",
            &[("ScheduledActionName", "tst-sa")],
        );
        assert_eq!(del.status, 200);
        let gone = cx.run(
            "DescribeScheduledActions",
            &[("ScheduledActionName", "tst-sa")],
        );
        assert_eq!(gone.status, 400);
    }

    #[test]
    fn usage_limit_lifecycle() {
        let cx = ctx();
        let _ = cx.create_cluster("tst-ul");
        let create = body_str(&cx.run(
            "CreateUsageLimit",
            &[
                ("ClusterIdentifier", "tst-ul"),
                ("FeatureType", "spectrum"),
                ("LimitType", "data-scanned"),
                ("Amount", "100"),
            ],
        ));
        assert!(
            create.contains("<FeatureType>spectrum</FeatureType>"),
            "{create}"
        );
        assert!(create.contains("<Amount>100</Amount>"), "{create}");
        assert!(create.contains("<Period>monthly</Period>"), "{create}");
        let open = create.split("<UsageLimitId>rul-").nth(1).expect("limit id");
        let ul_id = format!(
            "rul-{}",
            &open[..open.find("</UsageLimitId>").expect("closed")]
        );
        let modify = body_str(&cx.run(
            "ModifyUsageLimit",
            &[("UsageLimitId", ul_id.as_str()), ("Amount", "200")],
        ));
        assert!(modify.contains("<Amount>200</Amount>"), "{modify}");
        let del = cx.run("DeleteUsageLimit", &[("UsageLimitId", ul_id.as_str())]);
        assert_eq!(del.status, 200);
        let nf = cx.run("DeleteUsageLimit", &[("UsageLimitId", ul_id.as_str())]);
        assert_eq!(nf.status, 404);
        cx.run(
            "DeleteCluster",
            &[
                ("ClusterIdentifier", "tst-ul"),
                ("SkipFinalClusterSnapshot", "true"),
            ],
        );
    }

    // -- event subscriptions + HSM ---------------------------------------------

    #[test]
    fn event_subscription_lifecycle() {
        let cx = ctx();
        let create = body_str(&cx.run(
            "CreateEventSubscription",
            &[
                ("SubscriptionName", "tst-es"),
                ("SnsTopicArn", "arn:aws:sns:us-east-1:123456789012:t"),
            ],
        ));
        assert!(
            create.contains("<CustSubscriptionId>tst-es</CustSubscriptionId>"),
            "{create}"
        );
        let modify = body_str(&cx.run(
            "ModifyEventSubscription",
            &[("SubscriptionName", "tst-es"), ("Severity", "ERROR")],
        ));
        assert!(modify.contains("<Severity>ERROR</Severity>"), "{modify}");
        cx.run("DeleteEventSubscription", &[("SubscriptionName", "tst-es")]);
        let gone = cx.run(
            "DescribeEventSubscriptions",
            &[("SubscriptionName", "tst-es")],
        );
        assert_eq!(gone.status, 404);
    }

    #[test]
    fn hsm_certificate_and_configuration() {
        let cx = ctx();
        let create = body_str(&cx.run(
            "CreateHsmClientCertificate",
            &[("HsmClientCertificateIdentifier", "tst-hsm")],
        ));
        assert!(create.contains("-----BEGIN CERTIFICATE-----"), "{create}");
        let del = cx.run(
            "DeleteHsmClientCertificate",
            &[("HsmClientCertificateIdentifier", "tst-hsm")],
        );
        assert_eq!(del.status, 200);
        let gone = cx.run(
            "DescribeHsmClientCertificates",
            &[("HsmClientCertificateIdentifier", "tst-hsm")],
        );
        assert_eq!(gone.status, 400);
        let cfg = body_str(&cx.run(
            "CreateHsmConfiguration",
            &[
                ("HsmConfigurationIdentifier", "tst-hsmcfg"),
                ("Description", "Test HSM config"),
                ("HsmIpAddress", "10.0.0.1"),
                ("HsmPartitionName", "test-partition"),
                ("HsmPartitionPassword", "P@ssw0rd!"),
                ("HsmServerPublicCertificate", "MIIBogIBAAJBALR..."),
            ],
        ));
        assert!(
            cfg.contains("<HsmConfigurationIdentifier>tst-hsmcfg</HsmConfigurationIdentifier>"),
            "{cfg}"
        );
        assert!(
            cfg.contains("<HsmIpAddress>10.0.0.1</HsmIpAddress>"),
            "{cfg}"
        );
        cx.run(
            "DeleteHsmConfiguration",
            &[("HsmConfigurationIdentifier", "tst-hsmcfg")],
        );
    }

    // -- endpoints + partners + resource policies + data shares -----------------

    #[test]
    fn endpoint_access_not_found_errors() {
        let cx = ctx();
        let resp = cx.run(
            "CreateEndpointAccess",
            &[
                ("ClusterIdentifier", "tst-missing-cluster"),
                ("EndpointName", "tst-ep"),
                ("SubnetGroupName", "default"),
            ],
        );
        assert_eq!(resp.status, 404);
        assert!(body_str(&resp).contains("<Code>ClusterNotFound</Code>"));
        let del = cx.run("DeleteEndpointAccess", &[("EndpointName", "tst-ep-nf")]);
        assert_eq!(del.status, 404);
        assert!(body_str(&del).contains("<Code>EndpointNotFound</Code>"));
    }

    #[test]
    fn partner_not_found_variants() {
        let cx = ctx();
        for action in ["AddPartner", "DeletePartner", "UpdatePartnerStatus"] {
            let resp = cx.run(
                action,
                &[
                    ("AccountId", "123456789012"),
                    ("ClusterIdentifier", "tst-missing-cluster"),
                    ("DatabaseName", "dev"),
                    ("PartnerName", "tst-partner"),
                ],
            );
            assert_eq!(resp.status, 404, "{action}: {}", body_str(&resp));
            let code = if action == "AddPartner" {
                "ClusterNotFound"
            } else {
                "PartnerNotFound"
            };
            assert!(
                body_str(&resp).contains(&format!("<Code>{code}</Code>")),
                "{action}"
            );
        }
    }

    #[test]
    fn resource_policy_cycle() {
        let cx = ctx();
        let arn = "arn:aws:redshift:us-east-1:123456789012:namespace:tst-ns";
        let nf = cx.run("GetResourcePolicy", &[("ResourceArn", arn)]);
        assert_eq!(nf.status, 404);
        let put = body_str(&cx.run(
            "PutResourcePolicy",
            &[
                ("ResourceArn", arn),
                ("Policy", "{\"Version\":\"2012-10-17\"}"),
            ],
        ));
        assert!(put.contains("<ResourcePolicy><ResourceArn>"), "{put}");
        let get = body_str(&cx.run("GetResourcePolicy", &[("ResourceArn", arn)]));
        assert!(get.contains("2012-10-17"), "{get}");
        let del = cx.run("DeleteResourcePolicy", &[("ResourceArn", arn)]);
        assert_eq!(del.status, 200);
    }

    #[test]
    fn data_share_stub_shapes() {
        let cx = ctx();
        let arn = "arn:aws:redshift:us-east-1:123456789012:datashare:fake-ns/fake-share";
        let auth = body_str(&cx.run(
            "AuthorizeDataShare",
            &[
                ("DataShareArn", arn),
                ("ConsumerIdentifier", "123456789012"),
            ],
        ));
        assert!(auth.contains("<Status>AUTHORIZED</Status>"), "{auth}");
        let reject = body_str(&cx.run("RejectDataShare", &[("DataShareArn", arn)]));
        assert!(reject.contains("<DataShareAssociations/>"), "{reject}");
    }

    #[test]
    fn describe_storage_zero_floats() {
        let cx = ctx();
        let b = body_str(&cx.run("DescribeStorage", &[]));
        assert!(
            b.contains("<TotalBackupSizeInMegaBytes>0.0</TotalBackupSizeInMegaBytes>"),
            "{b}"
        );
        assert!(
            b.contains(
                "<TotalProvisionedStorageInMegaBytes>0.0</TotalProvisionedStorageInMegaBytes>"
            ),
            "{b}"
        );
    }

    #[test]
    fn custom_domain_association_validates_cluster() {
        let cx = ctx();
        let resp = cx.run(
            "CreateCustomDomainAssociation",
            &[
                ("CustomDomainName", "x.example.com"),
                (
                    "CustomDomainCertificateArn",
                    "arn:aws:acm:us-east-1:123456789012:certificate/f",
                ),
                ("ClusterIdentifier", "tst-missing-cluster"),
            ],
        );
        assert_eq!(resp.status, 404);
        assert!(body_str(&resp).contains("<Code>ClusterNotFound</Code>"));
    }

    #[test]
    fn gap_ops_answer_501() {
        let cx = ctx();
        for op in [
            "CreateIntegration",
            "DeleteIntegration",
            "ModifyIntegration",
            "CreateRedshiftIdcApplication",
            "DeleteRedshiftIdcApplication",
            "ModifyRedshiftIdcApplication",
            "RegisterNamespace",
            "DeregisterNamespace",
            "PurchaseReservedNodeOffering",
            "ResizeCluster",
            "RestoreTableFromClusterSnapshot",
            "FailoverPrimaryCompute",
            "ModifyLakehouseConfiguration",
            "NotARedshiftAction",
        ] {
            let resp = cx.run(op, &[]);
            assert_eq!(resp.status, 501, "{op}");
            let b = body_str(&resp);
            assert!(b.contains("<Code>NotImplemented</Code>"), "{op}: {b}");
            assert!(
                b.contains(&format!(
                    "The {} action has not been implemented",
                    snake_case(op)
                )),
                "{op}: {b}"
            );
            assert!(b.starts_with("<ErrorResponse><Error>"), "{op}: {b}");
        }
    }
}
