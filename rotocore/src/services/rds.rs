//! RDS backend — pure-Rust port of the moto rds service.
//!
//! Wire contract: AWS query protocol, XML namespace
//! `http://rds.amazonaws.com/doc/2014-10-31/`. Request params arrive in the
//! urlencoded body (`Action=...&Version=2014-10-31&...`) using botocore's
//! list encodings (`Tags.Tag.1.Key` flattened, `TagKeys.member.1` wrapped);
//! the parser below accepts both.
//!
//! Semantics mirror the vendored moto rds implementation for every operation
//! the compatibility suite (`tests/compatibility/test_rds_compat.py`)
//! exercises; anything else stays deny-by-default (501 `NotImplemented`).

use crate::store;
use crate::{Request, Response};
use chrono::{DateTime, SecondsFormat, Utc};
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

pub const XMLNS: &str = "http://rds.amazonaws.com/doc/2014-10-31/";
const DBI_RESOURCE_ID: &str = "db-M5ENSHXFPU6XHZ4G4ZEI5QIO2U";

// ---------------------------------------------------------------------------
// Params: a small value tree parsed from the urlencoded body + query string.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq)]
pub enum Val {
    #[default]
    None,
    S(String),
    L(Vec<Val>),
    M(BTreeMap<String, Val>),
}

impl Val {
    fn get(&self, key: &str) -> &Val {
        match self {
            Val::M(m) => m.get(key).unwrap_or(&Val::None),
            _ => &Val::None,
        }
    }
    /// Lists botocore encodes as EMPTY strings (`OptionsToInclude=`) turn
    /// into empty lists, mirroring moto's QueryParser behavior.
    fn list(&self) -> Vec<String> {
        match self {
            Val::L(items) => items
                .iter()
                .filter_map(|v| match v {
                    Val::S(s) if !s.is_empty() => Some(s.clone()),
                    _ => None,
                })
                .collect(),
            Val::S(s) if !s.is_empty() => vec![s.clone()],
            _ => Vec::new(),
        }
    }
    /// Flattened/wrapped list of structs as string tuples.
    fn map_list(&self) -> Vec<Vec<(String, String)>> {
        match self {
            Val::L(items) => items
                .iter()
                .map(|v| match v {
                    Val::M(m) => m
                        .iter()
                        .filter_map(|(k, v)| match v {
                            Val::S(s) => Some((k.clone(), s.clone())),
                            _ => None,
                        })
                        .collect(),
                    _ => Vec::new(),
                })
                .collect(),
            _ => Vec::new(),
        }
    }
    fn opt_bool(&self) -> Option<bool> {
        match self {
            Val::S(s) if !s.is_empty() => {
                Some(matches!(s.to_ascii_lowercase().as_str(), "true" | "1"))
            }
            _ => None,
        }
    }
    fn bool_or(&self, default: bool) -> bool {
        self.opt_bool().unwrap_or(default)
    }
    fn string(&self) -> Option<&str> {
        match self {
            Val::S(s) if !s.is_empty() => Some(s),
            _ => None,
        }
    }
    fn opt_i64(&self) -> Option<i64> {
        match self {
            Val::S(s) if !s.is_empty() => s.parse().ok(),
            _ => None,
        }
    }
    fn s_or(&self, default: &str) -> String {
        match self {
            Val::S(s) if !s.is_empty() => s.clone(),
            _ => default.to_string(),
        }
    }
}

/// Parse urlencoded key/value pairs into a nested tree.
fn parse_params(req: &Request) -> BTreeMap<String, Val> {
    let mut root: BTreeMap<String, Val> = BTreeMap::new();
    let mut pairs = decode_pairs(&req.body);
    pairs.extend(decode_pairs(req.query_string.as_bytes()));
    for (key, value) in pairs {
        if key.is_empty() || key == "Action" || key == "Version" {
            continue;
        }
        let mut node = Val::None;
        let segments: Vec<&str> = key.split('.').collect();
        insert_val(&mut node, &segments, &Val::S(value));
        root.insert(key, node);
    }
    root
}

fn decode_pairs(bytes: &[u8]) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let s = String::from_utf8_lossy(bytes);
    for pair in s.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = match pair.split_once('=') {
            Some((k, v)) => (k, v),
            None => (pair, ""),
        };
        out.push((form_decode(k), form_decode(v)));
    }
    out
}

fn form_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
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

/// Insert `segments → value`, building nested maps plus (1-based) numbered
/// lists. botocore emits three wire shapes, all from real RDS captures:
/// - `X.member.N`       — wrapped scalar list (TagKeys, VpcSubnetIds…)
/// - `X.member.N.Field` — wrapped struct list (Auth.member.1.AuthScheme)
/// - `X.Suffix.N`       — flattened scalar list (SubnetIds.SubnetIdentifier.1)
/// - `X.Tag.N.Field`    — flattened struct list (Tags.Tag.1.Key)
fn insert_val(node: &mut Val, segments: &[&str], value: &Val) {
    let Some(head) = segments.first() else {
        *node = value.clone();
        return;
    };
    let rest = &segments[1..];
    let idx_and_rest: Option<(usize, &[&str])> = match rest.first() {
        Some(&"member") => match rest.get(1).and_then(|s| s.parse::<usize>().ok()) {
            Some(n) if n >= 1 => Some((n, &rest[2..])),
            _ => None,
        },
        Some(s) => match (*s).parse::<usize>().ok() {
            Some(n) if n >= 1 => Some((n, &rest[1..])),
            _ => match rest.get(1).and_then(|t| t.parse::<usize>().ok()) {
                Some(n) if n >= 1 => Some((n, &rest[2..])),
                _ => None,
            },
        },
        None => None,
    };
    if let Some((n, rest_in)) = idx_and_rest {
        into_list(node);
        if let Val::L(items) = node {
            while items.len() < n {
                items.push(Val::M(BTreeMap::new()));
            }
            let slot = &mut items[n - 1];
            if rest_in.is_empty() {
                *slot = value.clone();
            } else {
                if matches!(slot, Val::S(_) | Val::None) {
                    *slot = Val::M(BTreeMap::new());
                }
                insert_val(slot, rest_in, value);
            }
        }
        return;
    }
    into_map(node);
    let _ = head;
    if let Val::M(map) = node {
        insert_val(map.entry(head.to_string()).or_default(), rest, value);
    }
}

fn into_list(node: &mut Val) {
    if !matches!(node, Val::L(_)) {
        *node = Val::L(Vec::new());
    }
}

fn into_map(node: &mut Val) {
    if !matches!(node, Val::M(_)) {
        *node = Val::M(BTreeMap::new());
    }
}

// ---------------------------------------------------------------------------
// Small utilities: XML escaping, timestamps, randomness.
// ---------------------------------------------------------------------------

fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '&' => out.push_str("&amp;"),
            '\'' => out.push_str("&#39;"),
            '"' => out.push_str("&quot;"),
            c => out.push(c),
        }
    }
    out
}

/// ISO-8601 with microseconds, the format moto's serializers emit.
fn iso(dt: &DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn now_unix() -> i64 {
    Utc::now().timestamp()
}

fn rand_lower(n: usize) -> String {
    uuid::Uuid::new_v4().simple().to_string().chars().take(n).collect()
}

fn rand_upper(n: usize) -> String {
    uuid::Uuid::new_v4()
        .simple()
        .to_string()
        .chars()
        .take(n)
        .collect::<String>()
        .to_uppercase()
}

fn account_str(account: u64) -> String {
    account.to_string()
}

// ---------------------------------------------------------------------------
// Errors: (status, code, message) triples matching moto's RDSClientError.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct RdsError {
    pub status: u16,
    pub code: String,
    pub message: String,
}

impl RdsError {
    pub fn new(status: u16, code: &str, message: impl Into<String>) -> Self {
        Self {
            status,
            code: code.to_string(),
            message: message.into(),
        }
    }
    fn client400(code: &str, message: impl Into<String>) -> Self {
        Self::new(400, code, message)
    }
    fn not_found404(code: &str, message: impl Into<String>) -> Self {
        Self::new(404, code, message)
    }

    /// Validate a DBInstance identifier (moto `_validate_db_identifier`).
    fn check_db_identifier(ident: &str) -> Result<(), Self> {
        let first_alpha = ident.chars().next().is_some_and(|c| c.is_ascii_alphabetic());
        if valid_identifier(ident) && ident.len() <= 63 && first_alpha {
            Ok(())
        } else {
            Err(Self::client400(
                "InvalidParameterValue",
                "The parameter DBInstanceIdentifier is not a valid identifier. \
                 Identifiers must begin with a letter; must contain only ASCII letters, digits, \
                 and hyphens; and must not end with a hyphen or contain two consecutive hyphens.",
            ))
        }
    }

    /// Validate a snapshot identifier (moto `_validate_db_snapshot_identifier`).
    fn check_snapshot_identifier(ident: &str, param: &str) -> Result<(), Self> {
        if ident.is_empty() {
            return Err(Self::client400(
                "InvalidParameterValue",
                format!("The parameter {param} must be provided and must not be blank."),
            ));
        }
        if !ident.chars().next().is_some_and(|c| c.is_ascii_alphabetic())
            || !valid_identifier(ident)
            || ident.len() > 255
        {
            if ident.chars().next().is_some_and(|c| c.is_ascii_alphabetic()) {
                return Err(Self::client400(
                    "InvalidParameterValue",
                    format!(
                        "The parameter {param} is not a valid identifier. \
                         Identifiers must begin with a letter; must contain only ASCII letters, digits, \
                         and hyphens; and must not end with a hyphen or contain two consecutive hyphens."
                    ),
                ));
            }
            return Err(Self::client400(
                "InvalidParameterValue",
                format!("Invalid snapshot identifier:  {ident}"),
            ));
        }
        Ok(())
    }
}

/// Letters/digits/hyphens without consecutive or trailing hyphens.
fn valid_identifier(s: &str) -> bool {
    !s.is_empty()
        && !s.bytes().zip(s.bytes().skip(1)).any(|(a, b)| a == b'-' && b == b'-')
        && !s.ends_with('-')
        && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
}

// ---------------------------------------------------------------------------
// State model
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Tag {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone)]
pub struct AssociatedRole {
    pub role_arn: String,
    pub feature_name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DBInstance {
    /// Lowercased identifier (moto lowercases DB instance identifiers).
    pub id: String,
    pub original_id: String,
    pub db_instance_class: String,
    pub engine: String,
    pub engine_version: String,
    pub port: i64,
    pub status: String,
    pub master_username: Option<String>,
    pub allocated_storage: i64,
    pub max_allocated_storage: Option<i64>,
    pub storage_type: String,
    pub storage_encrypted: bool,
    pub backup_retention_period: i64,
    pub auto_minor_version_upgrade: bool,
    pub license_model: String,
    pub preferred_backup_window: String,
    pub preferred_maintenance_window: String,
    pub availability_zone: String,
    pub multi_az: bool,
    pub publicly_accessible: Option<bool>,
    pub copy_tags_to_snapshot: bool,
    pub db_parameter_group_name: Option<String>,
    pub option_group_name: String,
    pub option_group_supplied: bool,
    pub vpc_security_group_ids: Vec<String>,
    pub db_security_groups: Vec<String>,
    pub db_subnet_group_name: Option<String>,
    pub db_cluster_identifier: Option<String>,
    pub is_replica: bool,
    pub replicas: Vec<String>,
    pub source_db_instance_identifier: Option<String>,
    pub deletion_protection: bool,
    pub iam_auth: bool,
    pub ca_certificate_identifier: String,
    pub tags: Vec<Tag>,
    pub enabled_cloudwatch_logs_exports: Vec<String>,
    pub associated_roles: Vec<AssociatedRole>,
    pub created: DateTime<Utc>,
    pub db_name: Option<String>,
    pub character_set_name: Option<String>,
}

/// Cluster members inherit several attributes from their cluster (moto's
/// DBInstanceClustered property overrides); resolve them here.
#[derive(Clone)]
pub struct EffectiveRefs {
    pub engine_version: String,
    pub master_username: Option<String>,
    pub allocated_storage: i64,
    pub backup_retention_period: i64,
    pub option_group_name: String,
    pub storage_type: String,
    pub license_model: String,
    pub character_set_name: Option<String>,
    pub db_name: Option<String>,
}

impl DBInstance {
    pub fn valid_engine(engine: &str) -> bool {
        [
            "aurora-mysql",
            "aurora-postgresql",
            "custom-oracle-ee",
            "custom-oracle-ee-cdb",
            "custom-sqlserver-ee",
            "custom-sqlserver-se",
            "custom-sqlserver-web",
            "mariadb",
            "mysql",
            "neptune",
            "oracle-ee",
            "oracle-ee-cdb",
            "oracle-se",
            "oracle-se1",
            "oracle-se2",
            "oracle-se2-cdb",
            "postgres",
            "sqlserver-ee",
            "sqlserver-se",
            "sqlserver-ex",
            "sqlserver-web",
        ]
        .contains(&engine)
    }

    pub fn default_storage(engine: &str) -> (i64, &'static str) {
        match engine {
            "aurora" => (0, "gp2"),
            "sqlserver-ee" | "sqlserver-se" => (200, "gp2"),
            _ => (20, "gp2"),
        }
    }

    pub fn default_port(engine: &str) -> i64 {
        match engine {
            "aurora-postgresql" | "postgres" => 5432,
            "neptune" => 8182,
            "oracle-ee" | "oracle-se2" | "oracle-se1" | "oracle-se" => 1521,
            "sqlserver-ee" | "sqlserver-ex" | "sqlserver-se" | "sqlserver-web" => 1433,
            "custom-oracle-ee" | "custom-oracle-ee-cdb" => 1521,
            "custom-sqlserver-ee" | "custom-sqlserver-se" | "custom-sqlserver-web" => 1433,
            _ => 3306,
        }
    }

    pub fn default_engine_version(engine: &str) -> Option<&'static str> {
        match engine {
            "mysql" | "MySQL" => Some("5.6.21"),
            "neptune" => Some("1.3.2.1"),
            "oracle-ee" | "oracle-se" | "oracle-se1" | "oracle-se2" => Some("11.2.0.4.v3"),
            "postgres" => Some("9.3.3"),
            "sqlserver-ee" | "sqlserver-ex" | "sqlserver-se" | "sqlserver-web" => {
                Some("11.00.2100.60.v1")
            }
            _ => None,
        }
    }
}

pub struct DBCluster {
    /// Lowercased identifier (moto lowercases cluster identifiers).
    pub id: String,
    pub original_id: String,
    pub engine: String,
    pub engine_version: String,
    pub engine_mode: String,
    pub status: String,
    /// moto: a fresh cluster reports "creating" once, then "available".
    status_shown: std::sync::atomic::AtomicBool,
    pub port: i64,
    pub master_username: Option<String>,
    pub allocated_storage: i64,
    pub storage_type: String,
    pub storage_encrypted: bool,
    pub backup_retention_period: i64,
    pub auto_minor_version_upgrade: bool,
    pub license_model: String,
    pub preferred_backup_window: String,
    pub preferred_maintenance_window: String,
    pub parameter_group: String,
    pub db_subnet_group_name: String,
    pub publicly_accessible: bool,
    pub vpc_security_group_ids: Vec<String>,
    pub deletion_protection: bool,
    pub iam_auth: bool,
    pub option_group_name: String,
    pub url_identifier: String,
    pub hosted_zone_id: String,
    pub resource_id: String,
    pub tags: Vec<Tag>,
    pub replication_source_identifier: Option<String>,
    pub read_replica_identifiers: Vec<String>,
    pub global_cluster_identifier: Option<String>,
    pub backtrack_window: i64,
    pub created: DateTime<Utc>,
    pub copy_tags_to_snapshot: bool,
    pub character_set_name: Option<String>,
    pub database_name: Option<String>,
    pub enabled_cloudwatch_logs_exports: Vec<String>,
    pub associated_roles: Vec<AssociatedRole>,
    pub availability_zones: Vec<String>,
    pub kms_key_id: String,
}

impl DBCluster {
    pub fn set_kms_key_id(mut self, key: Option<String>) -> Self {
        self.kms_key_id = key.unwrap_or_else(|| "default_kms_key_id".to_string());
        self
    }
}

impl std::fmt::Debug for DBCluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DBCluster").field("id", &self.id).finish()
    }
}

impl Clone for DBCluster {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            original_id: self.original_id.clone(),
            engine: self.engine.clone(),
            engine_version: self.engine_version.clone(),
            engine_mode: self.engine_mode.clone(),
            status: self.status.clone(),
            status_shown: std::sync::atomic::AtomicBool::new(false),
            port: self.port,
            master_username: self.master_username.clone(),
            allocated_storage: self.allocated_storage,
            storage_type: self.storage_type.clone(),
            storage_encrypted: self.storage_encrypted,
            backup_retention_period: self.backup_retention_period,
            auto_minor_version_upgrade: self.auto_minor_version_upgrade,
            license_model: self.license_model.clone(),
            preferred_backup_window: self.preferred_backup_window.clone(),
            preferred_maintenance_window: self.preferred_maintenance_window.clone(),
            parameter_group: self.parameter_group.clone(),
            db_subnet_group_name: self.db_subnet_group_name.clone(),
            publicly_accessible: self.publicly_accessible,
            vpc_security_group_ids: self.vpc_security_group_ids.clone(),
            deletion_protection: self.deletion_protection,
            iam_auth: self.iam_auth,
            option_group_name: self.option_group_name.clone(),
            url_identifier: self.url_identifier.clone(),
            hosted_zone_id: self.hosted_zone_id.clone(),
            resource_id: self.resource_id.clone(),
            tags: self.tags.clone(),
            replication_source_identifier: self.replication_source_identifier.clone(),
            read_replica_identifiers: self.read_replica_identifiers.clone(),
            global_cluster_identifier: self.global_cluster_identifier.clone(),
            backtrack_window: self.backtrack_window,
            created: self.created,
            copy_tags_to_snapshot: self.copy_tags_to_snapshot,
            character_set_name: self.character_set_name.clone(),
            database_name: self.database_name.clone(),
            enabled_cloudwatch_logs_exports: self.enabled_cloudwatch_logs_exports.clone(),
            associated_roles: self.associated_roles.clone(),
            availability_zones: self.availability_zones.clone(),
            kms_key_id: self.kms_key_id.clone(),
        }
    }
}

impl DBCluster {
    pub fn valid_engine(engine: &str) -> bool {
        [
            "aurora-mysql",
            "aurora-postgresql",
            "mysql",
            "neptune",
            "postgres",
        ]
        .contains(&engine)
    }

    pub fn default_engine_version(engine: &str) -> &'static str {
        match engine {
            "aurora-mysql" => "5.7.mysql_aurora.2.07.2",
            "aurora-postgresql" => "12.7",
            "mysql" => "8.0.23",
            "neptune" => "1.3.2.1",
            "postgres" => "13.4",
            _ => "1.0",
        }
    }

    pub fn default_port(engine: &str) -> i64 {
        match engine {
            "aurora-postgresql" | "postgres" => 5432,
            "neptune" => 8182,
            _ => 3306,
        }
    }

    pub fn default_allocated_storage(engine: &str, storage_type: &str) -> i64 {
        match engine {
            "neptune" => 0,
            "mysql" | "postgres" => match storage_type {
                "io1" => 100,
                "standard" => 5,
                _ => 20,
            },
            _ => 1, // aurora engines
        }
    }

    pub fn default_storage_type(iops: Option<i64>) -> &'static str {
        if iops.is_some() {
            "io1"
        } else {
            "gp2"
        }
    }

    pub fn compute_default_option_group(engine: &str, engine_version: &str) -> String {
        let semantic: Vec<&str> = engine_version.split('.').collect();
        let suffix = if semantic.len() > 1 {
            format!("{}-{}", semantic[0], semantic[1])
        } else {
            semantic[0].to_string()
        };
        format!("default:{engine}-{suffix}")
    }

    /// moto flips a fresh cluster's status to available after the first
    /// serialized status read ("creating" once, then "available").
    pub fn publish_status(&self) -> String {
        use std::sync::atomic::Ordering;
        if self.status == "creating" && !self.status_shown.swap(true, Ordering::SeqCst) {
            return "creating".to_string();
        }
        self.status.clone()
    }
}

#[derive(Debug, Clone)]
pub struct DBSnapshot {
    /// Lowercased identifier; moto lowercases snapshot identifiers.
    pub id: String,
    pub original_id: String,
    /// The attributes the snapshot captured at creation (moto keeps a copy).
    pub source: DBInstance,
    pub snapshot_type: String,
    pub status: String,
    pub tags: Vec<Tag>,
    pub attributes_restore: Vec<String>,
    pub created: DateTime<Utc>,
    pub original_created: DateTime<Utc>,
    pub kms_key_id: Option<String>,
    pub encrypted: bool,
}

#[derive(Debug, Clone)]
pub struct DBClusterSnapshot {
    /// Lowercased identifier.
    pub id: String,
    pub original_id: String,
    pub cluster: DBCluster,
    pub snapshot_type: String,
    pub status: String,
    pub tags: Vec<Tag>,
    pub attributes_restore: Vec<String>,
    pub created: DateTime<Utc>,
    pub encrypted: bool,
    pub kms_key_id: String,
}

#[derive(Debug, Clone)]
pub struct DBParameterGroup {
    pub name: String, // lowercased
    pub family: String,
    pub description: String,
    pub tags: Vec<Tag>,
    pub parameters: Vec<(String, BTreeMap<String, String>)>,
}

impl DBParameterGroup {
    pub fn set_parameter(&mut self, name: &str, values: BTreeMap<String, String>) {
        for (n, v) in self.parameters.iter_mut() {
            if n == name {
                for (k, val) in values {
                    v.insert(k, val);
                }
                return;
            }
        }
        self.parameters.push((name.to_string(), values));
    }
    pub fn reset_all(&mut self) {
        self.parameters.clear();
    }
}

#[derive(Debug, Clone, Default)]
pub struct DBClusterParameterGroup {
    pub name: String, // lowercased
    pub family: String,
    pub description: String,
    pub tags: Vec<Tag>,
    pub parameters: Vec<(String, BTreeMap<String, String>)>,
}

impl DBClusterParameterGroup {
    pub fn set_parameter(&mut self, name: &str, values: BTreeMap<String, String>) {
        for (n, v) in self.parameters.iter_mut() {
            if n == name {
                for (k, val) in values {
                    v.insert(k, val);
                }
                return;
            }
        }
        self.parameters.push((name.to_string(), values));
    }
    pub fn reset_all(&mut self) {
        self.parameters.clear();
    }
}

#[derive(Debug, Clone)]
pub struct OptionGroup {
    pub name: String, // lowercased
    pub engine_name: String,
    pub major_engine_version: String,
    pub description: String,
    pub tags: Vec<Tag>,
    /// (OptionName, OptionSetting names)
    pub options: Vec<(String, Vec<String>)>,
}

impl OptionGroup {
    pub fn add_option(&mut self, name: &str, settings: &[(String, String)]) {
        let names = settings.iter().map(|(k, _)| k.clone()).collect();
        for (n, s) in self.options.iter_mut() {
            if n == name {
                *s = names;
                return;
            }
        }
        self.options.push((name.to_string(), names));
    }
    pub fn remove_option(&mut self, name: &str) {
        self.options.retain(|(n, _)| n != name);
    }
}

#[derive(Debug, Clone)]
pub struct SubnetInfo {
    pub id: String,
    pub az: Option<String>,
    pub vpc_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DBSubnetGroup {
    pub name: String, // lowercased
    pub description: String,
    pub subnets: Vec<SubnetInfo>,
    pub tags: Vec<Tag>,
}

impl DBSubnetGroup {
    pub fn vpc_id(&self) -> Option<String> {
        self.subnets.iter().find_map(|s| s.vpc_id.clone())
    }
}

#[derive(Debug, Clone)]
pub struct DBSecurityGroup {
    pub name: String,
    pub description: String,
    pub vpc_id: Option<String>,
    pub ip_ranges: Vec<String>,
    pub ec2_security_groups: Vec<(String, String, String)>, // (name, id, owner)
    pub tags: Vec<Tag>,
}

#[derive(Debug, Clone)]
pub struct EventSubscription {
    pub name: String,
    pub sns_topic_arn: Option<String>,
    pub source_type: Option<String>,
    pub event_categories: Vec<String>,
    pub source_ids: Vec<String>,
    pub enabled: bool,
    pub tags: Vec<Tag>,
    pub created: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct GlobalCluster {
    pub identifier: String, // lowercased
    pub original_id: String,
    pub unique_id: String,
    pub engine: String,
    pub engine_version: String,
    pub storage_encrypted: bool,
    pub deletion_protection: bool,
    pub members: Vec<String>,
    pub status: String,
    pub tags: Vec<Tag>,
}

impl GlobalCluster {
    pub fn arn_for(&self, account: u64) -> String {
        format!("arn:aws:rds::{}:global-cluster:{}", account, self.identifier)
    }
    pub fn resource_id(&self) -> String {
        format!("cluster-{}", self.unique_id)
    }
    pub fn endpoint(&self) -> String {
        format!(
            "{}.global-{}.global.rds.amazonaws.com",
            self.identifier, self.unique_id
        )
    }
}

#[derive(Debug, Clone)]
pub struct DBProxyAuth {
    pub auth_scheme: String,
    pub description: Option<String>,
    pub user_name: Option<String>,
    pub secret_arn: Option<String>,
    pub client_password_auth_type: Option<String>,
    pub iam_auth: String,
}

#[derive(Debug, Clone)]
pub struct DBProxy {
    pub name: String,
    pub engine_family: String,
    pub auth: Vec<DBProxyAuth>,
    pub role_arn: Option<String>,
    pub vpc_subnet_ids: Vec<String>,
    pub vpc_security_group_ids: Vec<String>,
    pub vpc_id: Option<String>,
    pub require_tls: bool,
    pub idle_client_timeout: i64,
    pub debug_logging: bool,
    pub unique_id: String,
    pub url_identifier: String,
    pub tags: Vec<Tag>,
    pub created: DateTime<Utc>,
    // default target group settings
    pub max_connections: i64,
    pub max_idle_connections: i64,
    pub borrow_timeout: i64,
    pub session_pinning_filters: Vec<String>,
    pub targets: Vec<ProxyTarget>,
}

impl DBProxy {
    pub fn arn_for(&self, account: u64, region: &str) -> String {
        format!("arn:aws:rds:{region}:{account}:db-proxy:{}", self.unique_id)
    }
    pub fn endpoint_for(&self, region: &str) -> String {
        format!(
            "{}.db-proxy-{}.{}.rds.amazonaws.com",
            self.name, self.url_identifier, region
        )
    }
}

#[derive(Debug)]
pub struct ProxyTarget {
    pub rds_resource_id: String,
    pub port: i64,
    pub r#type: String,
    /// First serialization reports REGISTERING (moto's `registering` flag).
    pub registering: std::sync::atomic::AtomicBool,
}

impl Clone for ProxyTarget {
    fn clone(&self) -> Self {
        use std::sync::atomic::Ordering;
        Self {
            rds_resource_id: self.rds_resource_id.clone(),
            port: self.port,
            r#type: self.r#type.clone(),
            registering: std::sync::atomic::AtomicBool::new(
                self.registering.load(Ordering::SeqCst),
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DBClusterEndpoint {
    pub id: String,
    pub cluster_identifier: String,
    pub endpoint_type: String,
    pub custom_endpoint_type: String,
    pub static_members: Vec<String>,
    pub excluded_members: Vec<String>,
    pub tags: Vec<Tag>,
    pub status: String,
    pub resource_id: String,
    pub url_identifier: String,
}

impl DBClusterEndpoint {
    pub fn arn_for(&self, account: u64, region: &str) -> String {
        format!("arn:aws:rds:{region}:{account}:cluster-endpoint:{}", self.id)
    }
    pub fn endpoint_for(&self, region: &str) -> String {
        format!(
            "{}.cluster-custom-{}.{}.rds.amazonaws.com",
            self.id, self.url_identifier, region
        )
    }
}

#[derive(Debug, Clone)]
pub struct DBProxyEndpoint {
    pub proxy_name: String,
    pub name: String,
    pub vpc_subnet_ids: Vec<String>,
    pub vpc_security_group_ids: Vec<String>,
    pub vpc_id: Option<String>,
    pub target_role: String,
    pub tags: Vec<Tag>,
    pub status: String,
    pub is_default: bool,
    pub created: DateTime<Utc>,
    pub url_identifier: String,
}

impl DBProxyEndpoint {
    pub fn arn_for(&self, account: u64, region: &str) -> String {
        format!("arn:aws:rds:{region}:{account}:db-proxy-endpoint:{}", self.name)
    }
    pub fn endpoint_for(&self, region: &str) -> String {
        format!(
            "{}.endpoint-{}.{}.rds.amazonaws.com",
            self.name, self.url_identifier, region
        )
    }
}

#[derive(Debug, Clone)]
pub struct ExportTask {
    pub id: String,
    pub kms_key_id: Option<String>,
    pub source_arn: String,
    pub iam_role_arn: Option<String>,
    pub s3_bucket: Option<String>,
    pub s3_prefix: String,
    pub export_only: Vec<String>,
    pub status: String,
    pub created: DateTime<Utc>,
    pub source_type: String,
}

#[derive(Debug, Clone)]
pub struct BlueGreenDeployment {
    pub id: String,
    pub name: String,
    pub source: String,
    pub target: String,
    pub status: String,
    pub status_details: Option<String>,
    pub tags: Vec<Tag>,
    pub created: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct DBShardGroup {
    pub id: String,
    pub cluster_identifier: String,
    pub compute_redundancy: Option<i64>,
    pub max_acu: Option<String>,
    pub min_acu: Option<String>,
    pub status: String,
    pub tags: Vec<Tag>,
    pub created: DateTime<Utc>,
}

impl DBShardGroup {
    pub fn arn_for(&self, account: u64, region: &str) -> String {
        format!("arn:aws:rds:{region}:{account}:shard-group:{}", self.id)
    }
}

#[derive(Debug, Clone)]
pub struct CustomDBEngineVersion {
    pub engine: String,
    pub engine_version: String,
    pub s3_bucket: Option<String>,
    pub s3_prefix: Option<String>,
    pub kms_key_id: Option<String>,
    pub description: Option<String>,
    pub manifest: Option<String>,
    pub image_id: Option<String>,
    pub status: String,
    pub created: DateTime<Utc>,
    pub tags: Vec<Tag>,
}

impl CustomDBEngineVersion {
    pub fn key(&self) -> String {
        format!("{}:{}", self.engine, self.engine_version)
    }
    pub fn arn_for(&self, account: u64, region: &str) -> String {
        format!(
            "arn:aws:rds:{region}:{account}:cev:{}/{}",
            self.engine, self.engine_version
        )
    }
    pub fn major_engine_version(&self) -> String {
        self.engine_version
            .split('.')
            .next()
            .unwrap_or(&self.engine_version)
            .to_string()
    }
    pub fn db_engine_description(&self) -> String {
        format!("Custom {}", self.engine)
    }
    pub fn version_description(&self) -> String {
        self.description
            .clone()
            .unwrap_or_else(|| format!("Custom {} {}", self.engine, self.engine_version))
    }
}

#[derive(Debug, Clone)]
pub struct Integration {
    pub name: String,
    pub source_arn: String,
    pub target_arn: String,
    pub kms_key_id: Option<String>,
    pub description: Option<String>,
    pub data_filter: Option<String>,
    pub status: String,
    pub created: DateTime<Utc>,
    pub arn: String,
    pub tags: Vec<Tag>,
}

#[derive(Debug, Clone)]
pub struct TenantDatabase {
    pub db_instance_identifier: String,
    pub name: String,
    pub master_username: String,
    pub character_set_name: String,
    pub nchar_character_set_name: String,
    pub status: String,
    pub created: DateTime<Utc>,
    pub resource_id: String,
    pub tags: Vec<Tag>,
}

impl TenantDatabase {
    pub fn arn_for(&self, account: u64, region: &str) -> String {
        format!(
            "arn:aws:rds:{region}:{account}:tenant-db:{}/{}",
            self.db_instance_identifier, self.name
        )
    }
}

#[derive(Debug, Clone)]
pub struct ReservedDbInstance {
    pub id: String,
    pub offering_id: String,
    pub db_instance_class: String,
    pub duration: i64,
    pub fixed_price: String,
    pub usage_price: String,
    pub currency_code: String,
    pub product_description: String,
    pub offering_type: String,
    pub multi_az: bool,
    pub recurring: Vec<(String, String)>,
    pub count: i64,
    pub start_time: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct Event {
    pub source_identifier: String,
    pub source_type: String,
    pub message: String,
    pub categories: Vec<String>,
    pub source_arn: String,
    pub date: DateTime<Utc>,
}

pub struct AutomatedBackup {
    pub db_instance_identifier: String,
}

#[derive(Default)]
pub struct RdsState {
    pub databases: BTreeMap<String, DBInstance>,
    pub clusters: BTreeMap<String, DBCluster>,
    pub cluster_snapshots: BTreeMap<String, DBClusterSnapshot>,
    pub database_snapshots: BTreeMap<String, DBSnapshot>,
    pub db_parameter_groups: BTreeMap<String, DBParameterGroup>,
    pub db_cluster_parameter_groups: BTreeMap<String, DBClusterParameterGroup>,
    pub option_groups: BTreeMap<String, OptionGroup>,
    pub subnet_groups: BTreeMap<String, DBSubnetGroup>,
    pub security_groups: BTreeMap<String, DBSecurityGroup>,
    pub event_subscriptions: BTreeMap<String, EventSubscription>,
    pub global_clusters: BTreeMap<String, GlobalCluster>,
    pub export_tasks: BTreeMap<String, ExportTask>,
    pub db_proxies: BTreeMap<String, DBProxy>,
    pub db_cluster_endpoints: BTreeMap<String, DBClusterEndpoint>,
    pub db_proxy_endpoints: BTreeMap<String, DBProxyEndpoint>,
    pub shard_groups: BTreeMap<String, DBShardGroup>,
    pub blue_green_deployments: BTreeMap<String, BlueGreenDeployment>,
    pub custom_db_engine_versions: BTreeMap<String, CustomDBEngineVersion>,
    pub integrations: BTreeMap<String, Integration>,
    pub tenant_databases: BTreeMap<(String, String), TenantDatabase>,
    pub reserved_db_instances: BTreeMap<String, ReservedDbInstance>,
    pub events: Vec<Event>,
    pub known_auto_backups: HashSet<String>,
}

impl RdsState {
    fn ensure_default_parameter_group(&mut self, name: &str, family: &str) {
        if self.db_parameter_groups.contains_key(name) {
            return;
        }
        self.db_parameter_groups.insert(
            name.to_string(),
            DBParameterGroup {
                name: name.to_string(),
                family: family.to_string(),
                description: format!("Default parameter group for {family}"),
                tags: Vec::new(),
                parameters: Vec::new(),
            },
        );
    }

    fn add_event(&mut self, event_type: &str, resource_id: &str, arn: String) {
        let (message, categories): (&str, &[&str]) = match event_type {
            "DB_INSTANCE_BACKUP_START" => ("Backing up DB instance", &["backup"]),
            "DB_INSTANCE_BACKUP_FINISH" => ("Finished DB instance backup", &["backup"]),
            "DB_INSTANCE_CREATE" => ("DB instance created", &["creation"]),
            "DB_INSTANCE_RESET_MASTER_CREDENTIALS" => {
                ("Reset master credentials", &["configuration change"])
            }
            "DB_SNAPSHOT_CREATE_AUTOMATED_START" => {
                ("Creating automated snapshot", &["creation"])
            }
            "DB_SNAPSHOT_CREATE_AUTOMATED_FINISH" => {
                ("Automated snapshot created", &["creation"])
            }
            "DB_SNAPSHOT_CREATE_MANUAL_START" => ("Creating manual snapshot", &["creation"]),
            "DB_SNAPSHOT_CREATE_MANUAL_FINISH" => ("Manual snapshot created", &["creation"]),
            _ => return,
        };
        self.events.push(Event {
            source_identifier: resource_id.to_string(),
            source_type: "db-instance".into(),
            message: message.to_string(),
            categories: categories.iter().map(|c| c.to_string()).collect(),
            source_arn: arn,
            date: Utc::now(),
        });
    }

    /// Cluster-member inheritance (moto's DBInstanceClustered overrides).
    pub fn effective(&self, db: &DBInstance) -> EffectiveRefs {
        let cluster = db
            .db_cluster_identifier
            .as_deref()
            .and_then(|c| self.clusters.get(c));
        match cluster {
            Some(cluster) => EffectiveRefs {
                engine_version: cluster.engine_version.clone(),
                master_username: cluster.master_username.clone(),
                allocated_storage: cluster.allocated_storage,
                backup_retention_period: cluster.backup_retention_period,
                option_group_name: cluster.option_group_name.clone(),
                storage_type: "aurora".to_string(),
                license_model: cluster.license_model.clone(),
                character_set_name: cluster.character_set_name.clone(),
                db_name: db.db_name.clone().or_else(|| cluster.database_name.clone()),
            },
            None => EffectiveRefs {
                engine_version: db.engine_version.clone(),
                master_username: db.master_username.clone(),
                allocated_storage: db.allocated_storage,
                backup_retention_period: db.backup_retention_period,
                option_group_name: db.option_group_name.clone(),
                storage_type: db.storage_type.clone(),
                license_model: db.license_model.clone(),
                character_set_name: db.character_set_name.clone(),
                db_name: db.db_name.clone(),
            },
        }
    }

    /// Automated snapshots grouped by instance, as moto returns them.
    pub fn automated_backups(&self) -> Vec<AutomatedBackup> {
        let mut seen: Vec<String> = Vec::new();
        for snap in self.database_snapshots.values() {
            if snap.snapshot_type == "automated" && !seen.contains(&snap.source.id) {
                seen.push(snap.source.id.clone());
            }
        }
        seen.into_iter()
            .map(|db_instance_identifier| AutomatedBackup { db_instance_identifier })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Serializers — member names and empty-element rendering mirror moto's wire.
// ---------------------------------------------------------------------------

/// `<Name/>` when empty, `<Name><Item>..</Item>…</Name>` otherwise.
fn list_xml(name: &str, member: &str, items: &[String]) -> String {
    if items.is_empty() {
        return format!("<{name}/>");
    }
    let mut out = format!("<{name}>");
    for v in items {
        out.push_str(&format!("<{member}>{}</{member}>", esc(v)));
    }
    out.push_str(&format!("</{name}>"));
    out
}

fn tags_xml(tags: &[Tag], outer: &str) -> String {
    if tags.is_empty() {
        return format!("<{outer}/>");
    }
    let mut out = format!("<{outer}>");
    for t in tags {
        out.push_str(&format!(
            "<Tag><Key>{}</Key><Value>{}</Value></Tag>",
            esc(&t.key),
            esc(&t.value)
        ));
    }
    out.push_str(&format!("</{outer}>"));
    out
}

fn vpc_sg_members(ids: &[String]) -> String {
    if ids.is_empty() {
        return "<VpcSecurityGroups/>".into();
    }
    let mut out = String::from("<VpcSecurityGroups>");
    for id in ids {
        out.push_str(&format!(
            "<VpcSecurityGroupMembership><VpcSecurityGroupId>{}</VpcSecurityGroupId>\
             <Status>active</Status></VpcSecurityGroupMembership>",
            esc(id)
        ));
    }
    out.push_str("</VpcSecurityGroups>");
    out
}

fn inst_address(id: &str, region: &str) -> String {
    format!("{id}.aaaaaaaaaa.{region}.rds.amazonaws.com")
}

/// moto attaches the default security group of the default VPC when an
/// instance has none; the Rust store cannot see moto's EC2 state, so the id
/// stays recognizable but unverifiable against EC2 (tests assert nothing
/// about it).
fn default_sg_placeholder(acct: u64, region: &str) -> String {
    format!("sg-default-{region}-{acct}")
}

/// The parameter group an instance lists, creating the default like moto.
fn default_pg_for(db: &DBInstance) -> String {
    let version = db.engine_version.clone();
    let cut = version.rfind('.').unwrap_or(version.len());
    let family = format!("{}{}", db.engine, &version[..cut]);
    format!("default.{family}")
}

/// Materialize the default parameter groups the instance lists (moto does
/// this lazily inside `db_parameter_groups()`).
fn ensure_pg_state(st: &mut RdsState, db: &DBInstance) -> String {
    let name = match &db.db_parameter_group_name {
        Some(n) if !n.is_empty() => n.clone(),
        _ => {
            let version = db.engine_version.clone();
            let cut = version.rfind('.').unwrap_or(version.len());
            let family = format!("{}{}", db.engine, &version[..cut]);
            let name = format!("default.{family}");
            st.ensure_default_parameter_group(&name, &family);
            name
        }
    };
    name
}

fn roles_xml(outer: &str, member: &str, roles: &[AssociatedRole]) -> String {
    if roles.is_empty() {
        return format!("<{outer}/>");
    }
    let mut out = format!("<{outer}>");
    for r in roles {
        let feature = match &r.feature_name {
            Some(f) => format!("<FeatureName>{}</FeatureName>", esc(f)),
            None => String::new(),
        };
        out.push_str(&format!(
            "<{member}><RoleArn>{}</RoleArn><Status>ACTIVE</Status>{}</{member}>",
            esc(&r.role_arn),
            feature
        ));
    }
    out.push_str(&format!("</{outer}>"));
    out
}

fn db_instance_inner(st: &RdsState, acct: u64, region: &str, db: &DBInstance) -> String {
    let eff = st.effective(db);
    let pg_name = match &db.db_parameter_group_name {
        Some(n) if !n.is_empty() => n.clone(),
        _ => default_pg_for(db),
    };
    let sg_ids = if db.vpc_security_group_ids.is_empty() && db.db_cluster_identifier.is_none() {
        vec![default_sg_placeholder(acct, region)]
    } else {
        db.vpc_security_group_ids.clone()
    };
    let mut out = String::from("<DBInstance>");
    out.push_str(&format!("<DBInstanceIdentifier>{}</DBInstanceIdentifier>", esc(&db.id)));
    out.push_str(&format!("<DBInstanceClass>{}</DBInstanceClass>", esc(&db.db_instance_class)));
    out.push_str(&format!("<Engine>{}</Engine>", esc(&db.engine)));
    out.push_str(&format!("<DBInstanceStatus>{}</DBInstanceStatus>", esc(&db.status)));
    if let Some(u) = &eff.master_username {
        out.push_str(&format!("<MasterUsername>{}</MasterUsername>", esc(u)));
    }
    out.push_str(&format!(
        "<Endpoint><Address>{}</Address><Port>{}</Port></Endpoint>",
        esc(&inst_address(&db.id, region)),
        db.port
    ));
    out.push_str(&format!("<AllocatedStorage>{}</AllocatedStorage>", eff.allocated_storage));
    out.push_str(&format!("<InstanceCreateTime>{}</InstanceCreateTime>", esc(&iso(&db.created))));
    out.push_str(&format!(
        "<PreferredBackupWindow>{}</PreferredBackupWindow>",
        esc(&db.preferred_backup_window)
    ));
    out.push_str(&format!(
        "<BackupRetentionPeriod>{}</BackupRetentionPeriod>",
        eff.backup_retention_period
    ));
    if db.db_security_groups.is_empty() {
        out.push_str("<DBSecurityGroups/>");
    } else {
        out.push_str("<DBSecurityGroups>");
        for g in &db.db_security_groups {
            out.push_str(&format!(
                "<DBSecurityGroup><DBSecurityGroupName>{}\
                 </DBSecurityGroupName><Status>active</Status></DBSecurityGroup>",
                esc(g)
            ));
        }
        out.push_str("</DBSecurityGroups>");
    }
    out.push_str(&vpc_sg_members(&sg_ids));
    out.push_str(&format!(
        "<DBParameterGroups><DBParameterGroup>\
         <DBParameterGroupName>{}</DBParameterGroupName>\
         <ParameterApplyStatus>in-sync</ParameterApplyStatus>\
         </DBParameterGroup></DBParameterGroups>",
        esc(&pg_name)
    ));
    out.push_str(&format!("<AvailabilityZone>{}</AvailabilityZone>", esc(&db.availability_zone)));
    out.push_str(&format!(
        "<PreferredMaintenanceWindow>{}</PreferredMaintenanceWindow>",
        esc(&db.preferred_maintenance_window)
    ));
    out.push_str(&format!(
        "<LatestRestorableTime>{}</LatestRestorableTime>",
        esc(&iso(&Utc::now()))
    ));
    out.push_str(&format!("<MultiAZ>{}</MultiAZ>", db.multi_az));
    out.push_str(&format!("<EngineVersion>{}</EngineVersion>", esc(&eff.engine_version)));
    out.push_str(&format!(
        "<AutoMinorVersionUpgrade>{}</AutoMinorVersionUpgrade>",
        db.auto_minor_version_upgrade
    ));
    if db.is_replica {
        if let Some(src) = &db.source_db_instance_identifier {
            out.push_str(&format!(
                "<ReadReplicaSourceDBInstanceIdentifier>{}</ReadReplicaSourceDBInstanceIdentifier>",
                esc(src)
            ));
        }
    }
    out.push_str(&list_xml(
        "ReadReplicaDBInstanceIdentifiers",
        "ReadReplicaDBInstanceIdentifier",
        &db.replicas,
    ));
    out.push_str(&format!("<LicenseModel>{}</LicenseModel>", esc(&eff.license_model)));
    out.push_str(&format!(
        "<OptionGroupMemberships><OptionGroupMembership>\
         <OptionGroupName>{}</OptionGroupName>\
         <Status>in-sync</Status>\
         </OptionGroupMembership></OptionGroupMemberships>",
        esc(&eff.option_group_name)
    ));
    if let Some(name) = &db.db_subnet_group_name {
        out.push_str(&format!(
            "<DBSubnetGroup>\
             <DBSubnetGroupName>{}</DBSubnetGroupName>\
             <SubnetGroupStatus>Complete</SubnetGroupStatus>\
             </DBSubnetGroup>",
            esc(name)
        ));
    }
    out.push_str(&format!("<StorageType>{}</StorageType>", esc(&eff.storage_type)));
    out.push_str(&format!("<DbInstancePort>{}</DbInstancePort>", db.port));
    out.push_str(&format!("<StorageEncrypted>{}</StorageEncrypted>", db.storage_encrypted));
    out.push_str(&format!("<DbiResourceId>{}</DbiResourceId>", esc(DBI_RESOURCE_ID)));
    out.push_str(&format!(
        "<CACertificateIdentifier>{}</CACertificateIdentifier>",
        esc(&db.ca_certificate_identifier)
    ));
    out.push_str("<DomainMemberships/>");
    out.push_str(&format!(
        "<CopyTagsToSnapshot>{}</CopyTagsToSnapshot>",
        db.copy_tags_to_snapshot
    ));
    if let Some(dbn) = &eff.db_name {
        out.push_str(&format!("<DBName>{}</DBName>", esc(dbn)));
    }
    out.push_str(&format!(
        "<DBInstanceArn>arn:aws:rds:{region}:{acct}:db:{}</DBInstanceArn>",
        esc(&db.id)
    ));
    out.push_str(&format!(
        "<IAMDatabaseAuthenticationEnabled>{}</IAMDatabaseAuthenticationEnabled>",
        db.iam_auth
    ));
    out.push_str(&format!(
        "<DeletionProtection>{}</DeletionProtection>",
        db.deletion_protection
    ));
    if let Some(cs) = &eff.character_set_name {
        out.push_str(&format!("<CharacterSetName>{}</CharacterSetName>", esc(cs)));
    }
    out.push_str(&roles_xml("AssociatedRoles", "DBInstanceRole", &db.associated_roles));
    out.push_str(&tags_xml(&db.tags, "TagList"));
    out.push_str("</DBInstance>");
    out
}

/// Public instance serializer (arguments already validated/updated).
pub fn db_instance_xml(st: &RdsState, acct: u64, region: &str, db: &DBInstance) -> String {
    db_instance_inner(st, acct, region, db)
}

pub fn db_cluster_xml(st: &RdsState, acct: u64, region: &str, cl: &DBCluster) -> String {
    let members: Vec<&DBInstance> = st
        .databases
        .values()
        .filter(|d| d.db_cluster_identifier.as_deref() == Some(&cl.id))
        .collect();
    let member_block = {
        let inner: String = members
            .iter()
            .map(|m| {
                format!(
                    "<DBClusterMember><DBInstanceIdentifier>{}</DBInstanceIdentifier>\
                     <IsClusterWriter>true</IsClusterWriter>\
                     <DBClusterParameterGroupStatus>in-sync</DBClusterParameterGroupStatus>\
                     <PromotionTier>1</PromotionTier></DBClusterMember>",
                    esc(&m.id)
                )
            })
            .collect();
        if inner.is_empty() {
            "<DBClusterMembers/>".to_string()
        } else {
            format!("<DBClusterMembers>{inner}</DBClusterMembers>")
        }
    };
    let multi_az = !cl.read_replica_identifiers.is_empty()
        || cl.replication_source_identifier.is_some()
        || members.len() > 1;
    let replicas: Vec<String> = cl.read_replica_identifiers.clone();
    let mut out = String::from("<DBCluster>");
    out.push_str(&format!("<AllocatedStorage>{}</AllocatedStorage>", cl.allocated_storage));
    out.push_str(&format!(
        "<AvailabilityZones>{}</AvailabilityZones>",
        cl.availability_zones
            .iter()
            .map(|z| format!("<AvailabilityZone>{}</AvailabilityZone>", esc(z)))
            .collect::<String>()
    ));
    out.push_str(&format!(
        "<BackupRetentionPeriod>{}</BackupRetentionPeriod>",
        cl.backup_retention_period
    ));
    out.push_str(&format!("<DBClusterIdentifier>{}</DBClusterIdentifier>", esc(&cl.id)));
    out.push_str(&format!(
        "<DBClusterParameterGroup>{}</DBClusterParameterGroup>",
        esc(&cl.parameter_group)
    ));
    out.push_str(&format!(
        "<DBSubnetGroup>{}</DBSubnetGroup>",
        esc(&cl.db_subnet_group_name)
    ));
    out.push_str(&format!("<Status>{}</Status>", esc(&cl.publish_status())));
    out.push_str(&format!(
        "<EarliestRestorableTime>{}</EarliestRestorableTime>",
        esc(&iso(&cl.created))
    ));
    out.push_str(&format!(
        "<Endpoint>{}.cluster-{}.{}.rds.amazonaws.com</Endpoint>",
        esc(&cl.id),
        cl.url_identifier,
        region
    ));
    out.push_str(&format!(
        "<ReaderEndpoint>{}.cluster-ro-{}.{}.rds.amazonaws.com</ReaderEndpoint>",
        esc(&cl.id),
        cl.url_identifier,
        region
    ));
    out.push_str(&format!("<MultiAZ>{multi_az}</MultiAZ>"));
    out.push_str(&format!("<Engine>{}</Engine>", esc(&cl.engine)));
    out.push_str(&format!("<EngineVersion>{}</EngineVersion>", esc(&cl.engine_version)));
    out.push_str(&format!(
        "<LatestRestorableTime>{}</LatestRestorableTime>",
        esc(&iso(&Utc::now()))
    ));
    out.push_str(&format!("<Port>{}</Port>", cl.port));
    if let Some(u) = &cl.master_username {
        out.push_str(&format!("<MasterUsername>{}</MasterUsername>", esc(u)));
    }
    out.push_str(&format!(
        "<PreferredBackupWindow>{}</PreferredBackupWindow>",
        esc(&cl.preferred_backup_window)
    ));
    out.push_str(&format!(
        "<PreferredMaintenanceWindow>{}</PreferredMaintenanceWindow>",
        esc(&cl.preferred_maintenance_window)
    ));
    out.push_str(&list_xml(
        "ReadReplicaIdentifiers",
        "ReadReplicaIdentifier",
        &replicas,
    ));
    out.push_str(&member_block);
    out.push_str(&vpc_sg_members(&cl.vpc_security_group_ids));
    out.push_str(&format!("<HostedZoneId>{}</HostedZoneId>", esc(&cl.hosted_zone_id)));
    out.push_str(&format!(
        "<StorageEncrypted>{}</StorageEncrypted>",
        cl.storage_encrypted
    ));
    out.push_str(&format!("<KmsKeyId>{}</KmsKeyId>", esc(&cl.kms_key_id)));
    out.push_str(&format!(
        "<DbClusterResourceId>{}</DbClusterResourceId>",
        esc(&cl.resource_id)
    ));
    out.push_str(&format!(
        "<DBClusterArn>arn:aws:rds:{region}:{acct}:cluster:{}</DBClusterArn>",
        esc(&cl.id)
    ));
    out.push_str(&roles_xml("AssociatedRoles", "DBClusterRole", &cl.associated_roles));
    out.push_str(&format!(
        "<IAMDatabaseAuthenticationEnabled>{}</IAMDatabaseAuthenticationEnabled>",
        cl.iam_auth
    ));
    out.push_str(&format!(
        "<ClusterCreateTime>{}</ClusterCreateTime>",
        esc(&iso(&cl.created))
    ));
    out.push_str(&format!(
        "<BacktrackWindow>{}</BacktrackWindow>",
        cl.backtrack_window
    ));
    out.push_str(&list_xml(
        "EnabledCloudwatchLogsExports",
        "member",
        &cl.enabled_cloudwatch_logs_exports,
    ));
    out.push_str(&format!("<EngineMode>{}</EngineMode>", esc(&cl.engine_mode)));
    out.push_str("<ScalingConfigurationInfo/>");
    out.push_str(&format!("<StorageType>{}</StorageType>", esc(&cl.storage_type)));
    out.push_str(&format!(
        "<PubliclyAccessible>{}</PubliclyAccessible>",
        cl.publicly_accessible
    ));
    out.push_str(&format!(
        "<AutoMinorVersionUpgrade>{}</AutoMinorVersionUpgrade>",
        cl.auto_minor_version_upgrade
    ));
    out.push_str(&format!(
        "<DeletionProtection>{}</DeletionProtection>",
        cl.deletion_protection
    ));
    out.push_str("<GlobalWriteForwardingRequested>false</GlobalWriteForwardingRequested>");
    out.push_str("<NetworkType>IPV4</NetworkType>");
    out.push_str(&tags_xml(&cl.tags, "TagList"));
    out.push_str("</DBCluster>");
    out
}

pub fn db_snapshot_xml(acct: u64, region: &str, snap: &DBSnapshot) -> String {
    let db = &snap.source;
    let mut out = String::from("<DBSnapshot>");
    out.push_str(&format!(
        "<DBSnapshotIdentifier>{}</DBSnapshotIdentifier>",
        esc(&snap.id)
    ));
    out.push_str(&format!(
        "<DBInstanceIdentifier>{}</DBInstanceIdentifier>",
        esc(&db.id)
    ));
    out.push_str(&format!(
        "<SnapshotCreateTime>{}</SnapshotCreateTime>",
        esc(&iso(&snap.created))
    ));
    out.push_str(&format!("<Engine>{}</Engine>", esc(&db.engine)));
    out.push_str(&format!(
        "<AllocatedStorage>{}</AllocatedStorage>",
        db.allocated_storage
    ));
    out.push_str(&format!("<Status>{}</Status>", esc(&snap.status)));
    out.push_str(&format!("<Port>{}</Port>", db.port));
    out.push_str(&format!(
        "<InstanceCreateTime>{}</InstanceCreateTime>",
        esc(&iso(&db.created))
    ));
    if let Some(u) = &db.master_username {
        out.push_str(&format!("<MasterUsername>{}</MasterUsername>", esc(u)));
    }
    out.push_str(&format!(
        "<EngineVersion>{}</EngineVersion>",
        esc(&db.engine_version)
    ));
    out.push_str(&format!(
        "<SnapshotType>{}</SnapshotType>",
        esc(&snap.snapshot_type)
    ));
    out.push_str(&format!("<Encrypted>{}</Encrypted>", snap.encrypted));
    out.push_str(&format!(
        "<DBSnapshotArn>arn:aws:rds:{region}:{acct}:snapshot:{}</DBSnapshotArn>",
        esc(&snap.id)
    ));
    out.push_str(&format!(
        "<IAMDatabaseAuthenticationEnabled>{}</IAMDatabaseAuthenticationEnabled>",
        db.iam_auth
    ));
    out.push_str(&format!("<DbiResourceId>{}</DbiResourceId>", esc(DBI_RESOURCE_ID)));
    out.push_str(&tags_xml(&snap.tags, "TagList"));
    out.push_str(&format!(
        "<OriginalSnapshotCreateTime>{}</OriginalSnapshotCreateTime>",
        esc(&iso(&snap.original_created))
    ));
    out.push_str("</DBSnapshot>");
    out
}

pub fn cluster_snapshot_xml(acct: u64, region: &str, snap: &DBClusterSnapshot) -> String {
    let cl = &snap.cluster;
    let mut out = String::from("<DBClusterSnapshot>");
    out.push_str(&format!(
        "<DBClusterSnapshotIdentifier>{}</DBClusterSnapshotIdentifier>",
        esc(&snap.id)
    ));
    out.push_str(&format!(
        "<DBClusterIdentifier>{}</DBClusterIdentifier>",
        esc(&cl.id)
    ));
    out.push_str(&format!(
        "<SnapshotCreateTime>{}</SnapshotCreateTime>",
        esc(&iso(&snap.created))
    ));
    out.push_str(&format!("<Engine>{}</Engine>", esc(&cl.engine)));
    out.push_str(&format!(
        "<AllocatedStorage>{}</AllocatedStorage>",
        cl.allocated_storage
    ));
    out.push_str(&format!("<Status>{}</Status>", esc(&snap.status)));
    out.push_str(&format!("<Port>{}</Port>", cl.port));
    out.push_str(&format!(
        "<ClusterCreateTime>{}</ClusterCreateTime>",
        esc(&iso(&cl.created))
    ));
    if let Some(u) = &cl.master_username {
        out.push_str(&format!("<MasterUsername>{}</MasterUsername>", esc(u)));
    }
    out.push_str(&format!(
        "<EngineVersion>{}</EngineVersion>",
        esc(&cl.engine_version)
    ));
    out.push_str(&format!(
        "<SnapshotType>{}</SnapshotType>",
        esc(&snap.snapshot_type)
    ));
    out.push_str("<PercentProgress>100</PercentProgress>");
    out.push_str(&format!(
        "<StorageEncrypted>{}</StorageEncrypted>",
        snap.encrypted
    ));
    out.push_str(&format!(
        "<KmsKeyId>{}</KmsKeyId>",
        esc(&snap.kms_key_id)
    ));
    out.push_str(&format!(
        "<DBClusterSnapshotArn>arn:aws:rds:{region}:{acct}:cluster-snapshot:{}</DBClusterSnapshotArn>",
        esc(&snap.id)
    ));
    out.push_str(&tags_xml(&snap.tags, "TagList"));
    out.push_str("</DBClusterSnapshot>");
    out
}

/// Snapshot restore-attribute block (`DBSnapshotAttributesResult`).
fn attribute_result_xml(
    outer: &str,
    inner: &str,
    identifier: &str,
    attrs: &[(String, Vec<String>)],
) -> String {
    let mut items = String::new();
    for (name, values) in attrs {
        items.push_str(&format!(
            "<DBSnapshotAttributes><AttributeName>{}</AttributeName>{}</DBSnapshotAttributes>",
            esc(name),
            list_xml("AttributeValues", "AttributeValue", values)
        ));
    }
    format!(
        "<{outer}><{inner}>{id}</{inner}>{inner_tag}{items}</{inner}></{outer}>",
        outer = outer,
        inner = inner,
        id = esc(identifier),
        inner_tag = format!("<DBSnapshotIdentifier>{}</DBSnapshotIdentifier>", esc(identifier)),
    )
}

pub fn parameter_group_xml(acct: u64, region: &str, pg: &DBParameterGroup) -> String {
    format!(
        "<DBParameterGroup><DBParameterGroupName>{}</DBParameterGroupName>\
         <DBParameterGroupFamily>{}</DBParameterGroupFamily>\
         <Description>{}</Description>\
         <DBParameterGroupArn>arn:aws:rds:{region}:{acct}:pg:{}</DBParameterGroupArn>\
         {}</DBParameterGroup>",
        esc(&pg.name),
        esc(&pg.family),
        esc(&pg.description),
        esc(&pg.name),
        tags_xml(&pg.tags, "TagList")
    )
}

pub fn cluster_parameter_group_xml(acct: u64, region: &str, pg: &DBClusterParameterGroup) -> String {
    format!(
        "<DBClusterParameterGroup><DBClusterParameterGroupName>{}</DBClusterParameterGroupName>\
         <DBParameterGroupFamily>{}</DBParameterGroupFamily>\
         <Description>{}</Description>\
         <DBClusterParameterGroupArn>arn:aws:rds:{region}:{acct}:cluster-pg:{}</DBClusterParameterGroupArn>\
         {}</DBClusterParameterGroup>",
        esc(&pg.name),
        esc(&pg.family),
        esc(&pg.description),
        esc(&pg.name),
        tags_xml(&pg.tags, "TagList")
    )
}

/// `<Parameter>` entries for DescribeDBParameters (moto stores the raw dict).
pub fn parameters_xml(params: &[(String, BTreeMap<String, String>)]) -> String {
    if params.is_empty() {
        return "<Parameters/>".into();
    }
    let mut out = String::from("<Parameters>");
    for (name, map) in params {
        out.push_str("<Parameter>");
        out.push_str(&format!("<ParameterName>{}</ParameterName>", esc(name)));
        for (k, v) in map {
            out.push_str(&format!("<{}>{}</{}>", k, esc(v), k));
        }
        out.push_str("</Parameter>");
    }
    out.push_str("</Parameters>");
    out
}

pub fn option_group_xml(acct: u64, region: &str, og: &OptionGroup) -> String {
    let options: String = og
        .options
        .iter()
        .map(|(name, settings)| {
            format!(
                "<Option><OptionName>{}</OptionName>{}</Option>",
                esc(name),
                list_xml("OptionSettings", "member", settings)
            )
        })
        .collect();
    let options_block = if options.is_empty() {
        "<Options/>".to_string()
    } else {
        format!("<Options>{options}</Options>")
    };
    format!(
        "<OptionGroup><OptionGroupName>{}</OptionGroupName>\
         <OptionGroupDescription>{}</OptionGroupDescription>\
         <EngineName>{}</EngineName>\
         <MajorEngineVersion>{}</MajorEngineVersion>\
         {options_block}\
         <AllowsVpcAndNonVpcInstanceMemberships>false</AllowsVpcAndNonVpcInstanceMemberships>\
         <OptionGroupArn>arn:aws:rds:{region}:{acct}:og:{arn_tail}</OptionGroupArn>\
         {tags_}</OptionGroup>",
        esc(&og.name),
        esc(&og.description),
        esc(&og.engine_name),
        esc(&og.major_engine_version),
        options_block = options_block,
        arn_tail = esc(&og.name),
        tags_ = tags_xml(&og.tags, "TagList"),
    )
}

pub fn subnet_group_xml(acct: u64, region: &str, sg: &DBSubnetGroup) -> String {
    let subnets: String = sg
        .subnets
        .iter()
        .map(|s| {
            let az = s
                .az
                .as_ref()
                .map(|z| format!("<Name>{}</Name>", esc(z)))
                .unwrap_or_default();
            format!(
                "<Subnet><SubnetIdentifier>{}</SubnetIdentifier>\
                 <SubnetAvailabilityZone>{}</SubnetAvailabilityZone>\
                 <SubnetStatus>Active</SubnetStatus></Subnet>",
                esc(&s.id),
                az
            )
        })
        .collect();
    let subnet_block = if subnets.is_empty() {
        "<Subnets/>".to_string()
    } else {
        format!("<Subnets>{subnets}</Subnets>")
    };
    let vpc = sg
        .vpc_id()
        .map(|v| format!("<VpcId>{}</VpcId>", esc(&v)))
        .unwrap_or_default();
    format!(
        "<DBSubnetGroup>\
         {vpc}\
         <DBSubnetGroupName>{}</DBSubnetGroupName>\
         <DBSubnetGroupDescription>{}</DBSubnetGroupDescription>\
         <SubnetGroupStatus>Complete</SubnetGroupStatus>\
         {subnet_block}\
         <DBSubnetGroupArn>arn:aws:rds:{region}:{acct}:subgrp:{}</DBSubnetGroupArn>\
         {}</DBSubnetGroup>",
        esc(&sg.name),
        esc(&sg.description),
        esc(&sg.name),
        tags_xml(&sg.tags, "TagList")
    )
}

pub fn security_group_xml(acct: u64, region: &str, sg: &DBSecurityGroup) -> String {
    let ec2_block = if sg.ec2_security_groups.is_empty() {
        "<EC2SecurityGroups/>".to_string()
    } else {
        let inner: String = sg
            .ec2_security_groups
            .iter()
            .map(|(n, id, owner)| {
                format!(
                    "<EC2SecurityGroup><EC2SecurityGroupId>{}</EC2SecurityGroupId>\
                     <EC2SecurityGroupName>{}</EC2SecurityGroupName>\
                     <EC2SecurityGroupOwnerId>{}</EC2SecurityGroupOwnerId>\
                     <Status>authorized</Status></EC2SecurityGroup>",
                    esc(id),
                    esc(n),
                    esc(owner)
                )
            })
            .collect();
        format!("<EC2SecurityGroups>{inner}</EC2SecurityGroups>")
    };
    let ip_block = if sg.ip_ranges.is_empty() {
        "<IPRanges/>".to_string()
    } else {
        let inner: String = sg
            .ip_ranges
            .iter()
            .map(|ip| {
                format!(
                    "<IPRange><Status>authorized</Status><CIDRIP>{}</CIDRIP></IPRange>",
                    esc(ip)
                )
            })
            .collect();
        format!("<IPRanges>{inner}</IPRanges>")
    };
    let vpc = sg
        .vpc_id
        .as_ref()
        .map(|v| format!("<VpcId>{}</VpcId>", esc(v)))
        .unwrap_or_default();
    format!(
        "<DBSecurityGroup>\
         <DBSecurityGroupName>{}</DBSecurityGroupName>\
         <DBSecurityGroupDescription>{}</DBSecurityGroupDescription>\
         {vpc}         {ec2_block}\
         {ip_block}\
         <DBSecurityGroupArn>arn:aws:rds:{region}:{acct}:secgrp:{arn_tail}\
         </DBSecurityGroupArn>\
         {tags_}</DBSecurityGroup>",
        esc(&sg.name),
        esc(&sg.description),
        ec2_block = ec2_block,
        ip_block = ip_block,
        vpc = vpc,
        arn_tail = esc(&sg.name),
        tags_ = tags_xml(&sg.tags, "TagList"),
    )
}

pub fn event_subscription_xml(acct: u64, region: &str, sub: &EventSubscription) -> String {
    let topic = sub
        .sns_topic_arn
        .as_ref()
        .map(|t| format!("<SnsTopicArn>{}</SnsTopicArn>", esc(t)))
        .unwrap_or_default();
    let stype = sub
        .source_type
        .as_ref()
        .map(|t| format!("<SourceType>{}</SourceType>", esc(t)))
        .unwrap_or_default();
    let cats = list_xml("EventCategoriesList", "EventCategory", &sub.event_categories);
    let sources = list_xml("SourceIdsList", "SourceId", &sub.source_ids);
    format!(
        "<EventSubscription>\
         <CustSubscriptionId>{}</CustSubscriptionId>\
         {topic}         {stype}{cats}{sources}\
         <Enabled>{enabled}</Enabled>\
         <Status>active</Status>\
         {created}{arn}{tags_}</EventSubscription>",
        esc(&sub.name),
        topic = topic,
        stype = stype,
        cats = cats,
        sources = sources,
        enabled = sub.enabled,
        created = format!("<SubscriptionCreationTime>{}</SubscriptionCreationTime>", esc(&iso(&sub.created))),
        arn = format!("<EventSubscriptionArn>arn:aws:rds:{region}:{acct}:es:{}</EventSubscriptionArn>", esc(&sub.name)),
        tags_ = tags_xml(&sub.tags, "TagList"),
    )
}

pub fn global_cluster_xml(acct: u64, gc: &GlobalCluster, st: &RdsState) -> String {
    let mut members = String::new();
    for cluster_id in &gc.members {
        if let Some(cl) = st.clusters.get(cluster_id) {
            let arn = format!("arn:aws:rds:us-east-1:{acct}:cluster:{}", cl.id);
            members.push_str(&format!(
                "<GlobalClusterMember><DBClusterArn>{}</DBClusterArn>\
                 <IsWriter>true</IsWriter>\
                 <DBClusterParameterGroupStatus>in-sync</DBClusterParameterGroupStatus>\
                 <PromotionTier>1</PromotionTier>\
                 <GlobalWriteForwardingStatus>disabled</GlobalWriteForwardingStatus>\
                 <Readers/></GlobalClusterMember>",
                esc(&arn)
            ));
        }
    }
    let members_block = if members.is_empty() {
        "<GlobalClusterMembers/>".to_string()
    } else {
        format!("<GlobalClusterMembers>{members}</GlobalClusterMembers>")
    };
    format!(
        "<GlobalCluster>\
         <GlobalClusterIdentifier>{}</GlobalClusterIdentifier>\
         <GlobalClusterResourceId>{}</GlobalClusterResourceId>\
         <GlobalClusterArn>{arn}</GlobalClusterArn>\
         <Status>{}</Status>\
         <Engine>{}</Engine>\
         <EngineVersion>{}</EngineVersion>\
         {encrypted}{protected}{members_block}         {endpoint}{tags_}</GlobalCluster>",
        esc(&gc.identifier),
        esc(&gc.resource_id()),
        esc(&gc.status),
        esc(&gc.engine),
        esc(&gc.engine_version),
        encrypted = format!("<StorageEncrypted>{}</StorageEncrypted><DeletionProtection>{}</DeletionProtection>", gc.storage_encrypted, gc.deletion_protection),
        protected = "",
        members_block = members_block,
        arn = esc(&gc.arn_for(acct)),
        endpoint = format!("<Endpoint>{}</Endpoint>", esc(&gc.endpoint())),
        tags_ = tags_xml(&gc.tags, "TagList"),
    )
}


pub fn db_proxy_xml(acct: u64, region: &str, p: &DBProxy) -> String {
    let mut auth_items = String::new();
    for a in &p.auth {
        let secret = a
            .secret_arn
            .as_ref()
            .map(|s| format!("<SecretArn>{}</SecretArn>", esc(s)))
            .unwrap_or_default();
        auth_items.push_str(&format!(
            "<member><AuthScheme>{scheme}</AuthScheme>{secret}<IAMAuth>{iam}</IAMAuth></member>",
            scheme = esc(&a.auth_scheme),
            iam = esc(&a.iam_auth),
            secret = secret,
        ));
    }
    let auth_block = if auth_items.is_empty() {
        "<Auth/>".to_string()
    } else {
        format!("<Auth>{auth_items}</Auth>")
    };
    let subnets: String = p
        .vpc_subnet_ids
        .iter()
        .map(|s| format!("<member>{}</member>", esc(s)))
        .collect();
    let sgs: String = p
        .vpc_security_group_ids
        .iter()
        .map(|s| format!("<member>{}</member>", esc(s)))
        .collect();
    let vpc_block = p
        .vpc_id
        .as_ref()
        .map(|v| format!("<VpcId>{}</VpcId>", esc(v)))
        .unwrap_or_default();
    let role = p
        .role_arn
        .as_ref()
        .map(|r| format!("<RoleArn>{}</RoleArn>", esc(r)))
        .unwrap_or_default();
    let created = esc(&iso(&p.created));
    format!(
        "<DBProxy>         <DBProxyName>{name}</DBProxyName>         <DBProxyArn>{arn}</DBProxyArn>         <Status>available</Status>         <EngineFamily>{engine_family}</EngineFamily>         {vpc_block}         <VpcSecurityGroupIds>{sgs}</VpcSecurityGroupIds>         <VpcSubnetIds>{subnets}</VpcSubnetIds>         {auth_block}         {role}         <RequireTLS>{require_tls}</RequireTLS>         <Endpoint>{endpoint}</Endpoint>         <IdleClientTimeout>{idle}</IdleClientTimeout>         <DebugLogging>{debug}</DebugLogging>         <CreatedDate>{created}</CreatedDate>         <UpdatedDate>{created}</UpdatedDate>         {tags}         </DBProxy>",
        name = esc(&p.name),
        arn = p.arn_for(acct, region),
        engine_family = esc(&p.engine_family),
        vpc_block = vpc_block,
        sgs = sgs,
        subnets = subnets,
        auth_block = auth_block,
        role = role,
        require_tls = p.require_tls,
        endpoint = esc(&p.endpoint_for(region)),
        idle = p.idle_client_timeout,
        debug = p.debug_logging,
        created = created,
        tags = tags_xml(&p.tags, "TagList"),
    )
}

pub fn proxy_target_xml(t: &ProxyTarget) -> String {
    use std::sync::atomic::Ordering;
    let health = if t.registering.swap(false, Ordering::SeqCst) {
        "REGISTERING"
    } else {
        "AVAILABLE"
    };
    format!(
        "<member>\
         <RdsResourceId>{}</RdsResourceId>\
         <Port>{}</Port>\
         <Type>{typ}</Type>\
         <TargetHealth><State>{health}</State></TargetHealth>\
         </member>",
        esc(&t.rds_resource_id),
        t.port,
        typ = esc(&t.r#type),
        health = health,
    )
}

pub fn proxy_target_groups_xml(acct: u64, region: &str, p: &DBProxy) -> String {
    let target_arn = format!(
        "arn:aws:rds:{region}:{acct}:target-group:prx-tg-{}",
        p.unique_id
    );
    format!(
        "<member>         <DBProxyName>{name}</DBProxyName>         <TargetGroupName>default</TargetGroupName>         <TargetGroupArn>{arn}</TargetGroupArn>         <CreatedDate>{created}</CreatedDate>         <UpdatedDate>{updated}</UpdatedDate>         <Status>available</Status>         <IsDefault>true</IsDefault>         <ConnectionPoolConfig>         <MaxConnectionsPercent>{max_conn}</MaxConnectionsPercent>         <MaxIdleConnectionsPercent>{max_idle}</MaxIdleConnectionsPercent>         <ConnectionBorrowTimeout>{borrow}</ConnectionBorrowTimeout>         {pinning}         </ConnectionPoolConfig>         </member>",
        name = esc(&p.name),
        arn = esc(&target_arn),
        created = esc(&iso(&p.created)),
        updated = esc(&iso(&p.created)),
        max_conn = p.max_connections,
        max_idle = p.max_idle_connections,
        borrow = p.borrow_timeout,
        pinning = list_xml("SessionPinningFilters", "member", &p.session_pinning_filters),
    )
}

pub fn db_cluster_endpoint_xml(acct: u64, region: &str, e: &DBClusterEndpoint) -> String {
    let statics = list_xml("StaticMembers", "member", &e.static_members);
    let excluded = list_xml("ExcludedMembers", "member", &e.excluded_members);
    format!(
        "<DBClusterEndpointIdentifier>{}</DBClusterEndpointIdentifier>\
         <DBClusterIdentifier>{}</DBClusterIdentifier>\
         <DBClusterEndpointResourceIdentifier>{}</DBClusterEndpointResourceIdentifier>\
         <Endpoint>{}\
         </Endpoint>\
         <Status>{}</Status>\
         <EndpointType>{}</EndpointType>\
         <CustomEndpointType>{}</CustomEndpointType>\
         {statics}{excluded}\
         <DBClusterEndpointArn>{}</DBClusterEndpointArn>\
         {tags}",
        esc(&e.id),
        esc(&e.cluster_identifier),
        esc(&e.resource_id),
        esc(&e.endpoint_for(region)),
        esc(&e.status),
        esc(&e.endpoint_type),
        esc(&e.custom_endpoint_type),
        e.arn_for(acct, region),
        tags = tags_xml(&e.tags, "TagList"),
    )
}

pub fn db_proxy_endpoint_xml(acct: u64, region: &str, e: &DBProxyEndpoint) -> String {
    let subnets: String = e
        .vpc_subnet_ids
        .iter()
        .map(|s| format!("<member>{}</member>", esc(s)))
        .collect();
    let sgs: String = e
        .vpc_security_group_ids
        .iter()
        .map(|s| format!("<member>{}</member>", esc(s)))
        .collect();
    let vpc_block = e
        .vpc_id
        .as_ref()
        .map(|v| format!("<VpcId>{}</VpcId>", esc(v)))
        .unwrap_or_default();
    format!(
        "<DBProxyEndpointName>{}</DBProxyEndpointName>\
         <DBProxyEndpointArn>{}</DBProxyEndpointArn>\
         <DBProxyName>{}</DBProxyName>\
         {vpc_block}\
         <TargetRole>{}</TargetRole>\
         <Status>{}</Status>\
         <CreatedDate>{}</CreatedDate>\
         <IsDefault>{}</IsDefault>\
         <VpcSecurityGroupIds>{sgs}</VpcSecurityGroupIds>\
         <VpcSubnetIds>{subnets}</VpcSubnetIds>\
         <Endpoint>{}\
         </Endpoint>\
         {tags}",
        esc(&e.name),
        e.arn_for(acct, region),
        esc(&e.proxy_name),
        e.target_role,
        esc(&e.status),
        esc(&iso(&e.created)),
        e.is_default,
        esc(&e.endpoint_for(region)),
        tags = tags_xml(&e.tags, "TagList"),
    )
}

pub fn export_task_xml(t: &ExportTask) -> String {
    format!(
        "<ExportTaskIdentifier>{}</ExportTaskIdentifier>\
         <SourceArn>{source_arn}</SourceArn>\
         {export_only}         {bucket}         {prefix}         {role}{kms}\
         {status}{source_type}",
        esc(&t.id),
        source_arn = esc(&t.source_arn),
        export_only = list_xml("ExportOnly", "member", &t.export_only),
        bucket = t
            .s3_bucket
            .as_ref()
            .map(|b| format!("<S3Bucket>{}</S3Bucket>", esc(b)))
            .unwrap_or_default(),
        prefix = format!("<S3Prefix>{}</S3Prefix>", esc(&t.s3_prefix)),
        role = t
            .iam_role_arn
            .as_ref()
            .map(|r| format!("<IamRoleArn>{}</IamRoleArn>", esc(r)))
            .unwrap_or_default(),
        kms = t
            .kms_key_id
            .as_ref()
            .map(|k| format!("<KmsKeyId>{}</KmsKeyId>", esc(k)))
            .unwrap_or_default(),
        status = format!("<Status>{}</Status>", esc(&t.status)),
        source_type = format!("<SourceType>{}</SourceType>", esc(&t.source_type)),
    )
}

pub fn blue_green_xml(b: &BlueGreenDeployment) -> String {
    let switchover = format!(
        "<SwitchoverDetails><member>\
         <SourceMember>{}</SourceMember>\
         <TargetMember>{}</TargetMember>\
         <Status>{}</Status>\
         </member></SwitchoverDetails>",
        esc(&b.source),
        esc(&b.target),
        esc(&b.status),
    );
    let tasks = format!(
        "<Tasks><member><Name>CREATING_READ_REPLICA_OF_SOURCE</Name>\
         <Status>COMPLETED</Status></member>\
         <member><Name>CONFIGURE_BACKUPS</Name>\
         <Status>COMPLETED</Status></member></Tasks>"
    );
    format!(
        "<BlueGreenDeployment>\
         <BlueGreenDeploymentIdentifier>{}</BlueGreenDeploymentIdentifier>\
         <BlueGreenDeploymentName>{}</BlueGreenDeploymentName>\
         <Source>{}\
         </Source>\
         <Target>{}\
         </Target>\
         {switchover}{tasks}\
         <CreateTime>{}</CreateTime>\
         {tags}</BlueGreenDeployment>",
        esc(&b.id),
        esc(&b.name),
        esc(&b.source),
        esc(&b.target),
        esc(&iso(&b.created)),
        tags = tags_xml(&b.tags, "TagList"),
    )
}

pub fn shard_group_xml(acct: u64, region: &str, g: &DBShardGroup) -> String {
    let compute = g
        .compute_redundancy
        .map(|c| format!("<ComputeRedundancy>{c}</ComputeRedundancy>"))
        .unwrap_or_default();
    let max_acu = g.max_acu.as_ref().map(|a| format!("<MaxACU>{}</MaxACU>", esc(a))).unwrap_or_default();
    let min_acu = g.min_acu.as_ref().map(|a| format!("<MinACU>{}</MinACU>", esc(a))).unwrap_or_default();
    format!(
        "<DBShardGroup>\
         <DBShardGroupResourceId>shard-group-{}</DBShardGroupResourceId>\
         <DBShardGroupIdentifier>{}</DBShardGroupIdentifier>\
         <DBClusterIdentifier>{}</DBClusterIdentifier>\
         {max_acu}{min_acu}{compute}\
         <Status>{}</Status>\
         <Endpoint>{}</Endpoint>\
         <DBShardGroupArn>{}</DBShardGroupArn>\
         {tags}</DBShardGroup>",
        esc(&g.id),
        esc(&g.id),
        esc(&g.cluster_identifier),
        esc(&g.status),
        format!("{}.{}.rds.amazonaws.com", esc(&g.cluster_identifier), region),
        g.arn_for(acct, region),
        tags = tags_xml(&g.tags, "TagList"),
    )
}

pub fn custom_engine_version_xml(
    acct: u64,
    region: &str,
    cev: &CustomDBEngineVersion,
    include_bucket: bool,
) -> String {
    let bucket_info = if include_bucket {
        let b = cev
            .s3_bucket
            .as_ref()
            .map(|b| format!("<DatabaseInstallationFilesS3BucketName>{}</DatabaseInstallationFilesS3BucketName>", esc(b)))
            .unwrap_or_default();
        b
    } else {
        String::new()
    };
    format!(
        "<Engine>{}</Engine>\
         <MajorEngineVersion>{}</MajorEngineVersion>\
         <EngineVersion>{}</EngineVersion>\
         {bucket}\
         {prefix_kms_desc}\
         <DBEngineDescription>{}</DBEngineDescription>\
         <DBEngineVersionArn>{}</DBEngineVersionArn>\
         <DBEngineVersionDescription>{}</DBEngineVersionDescription>\
         <CreateTime>{}</CreateTime>\
         <SupportedEngineModes><member>provisioned</member></SupportedEngineModes>\
         <Status>{}</Status>\
         <SupportsParallelQuery>false</SupportsParallelQuery>\
         <SupportsGlobalDatabases>false</SupportsGlobalDatabases>\
         {tags}",
        esc(&cev.engine),
        esc(&cev.major_engine_version()),
        esc(&cev.engine_version),
        cev.db_engine_description(),
        cev.arn_for(acct, region),
        esc(&cev.version_description()),
        esc(&cev.status),
        bucket = bucket_info,
        prefix_kms_desc = String::new(),
        tags = tags_xml(&cev.tags, "TagList"),
    )
}

pub fn integration_xml(i: &Integration, include_kms: bool) -> String {
    let kms = i
        .kms_key_id
        .as_ref()
        .map(|k| format!("<KMSKeyId>{}</KMSKeyId>", esc(k)))
        .unwrap_or_default();
    let description = i
        .description
        .as_ref()
        .map(|d| format!("<Description>{}</Description>", esc(d)))
        .unwrap_or_default();
    let data_filter = i
        .data_filter
        .as_ref()
        .map(|d| format!("<DataFilter>{}</DataFilter>", esc(d)))
        .unwrap_or_default();
    format!(
        "<SourceArn>{}\
         </SourceArn>\
         <TargetArn>{}\
         </TargetArn>\
         <IntegrationName>{}</IntegrationName>\
         <IntegrationArn>{}</IntegrationArn>\
         <AdditionalEncryptionContext/>\
         <Status>{}</Status>\
         {kms}{description}{data_filter}\
         <CreateTime>{}</CreateTime>\
         <Errors/>\
         {tags}",
        esc(&i.source_arn),
        esc(&i.target_arn),
        esc(&i.name),
        esc(&i.arn),
        esc(&i.status),
        esc(&iso(&i.created)),
        tags = tags_xml(&i.tags, "TagList"),
    )
}

pub fn tenant_database_xml(acct: u64, region: &str, t: &TenantDatabase) -> String {
    format!(
        "<TenantDatabase\
         ><TenantDatabaseCreateTime>{}</TenantDatabaseCreateTime>\
         <DBInstanceIdentifier>{}</DBInstanceIdentifier>\
         <TenantDBName>{}</TenantDBName>\
         <Status>{}</Status>\
         <MasterUsername>{}</MasterUsername>\
         <DbiResourceId>{}</DbiResourceId>\
         <TenantDatabaseResourceId>{}</TenantDatabaseResourceId>\
         <TenantDatabaseARN>{}</TenantDatabaseARN>\
         <CharacterSetName>{}</CharacterSetName>\
         <NcharCharacterSetName>{}</NcharCharacterSetName>\
         {tags}</TenantDatabase>",
        esc(&iso(&t.created)),
        esc(&t.db_instance_identifier),
        esc(&t.name),
        esc(&t.status),
        esc(&t.master_username),
        esc(DBI_RESOURCE_ID),
        esc(&t.resource_id),
        esc(&t.arn_for(acct, region)),
        esc(&t.character_set_name),
        esc(&t.nchar_character_set_name),
        tags = tags_xml(&t.tags, "TagList"),
    )
}

pub fn reserved_offering_xml(o: &ReservedDbInstance) -> String {
    let recurring: String = o
        .recurring
        .iter()
        .map(|(amount, freq)| {
            format!(
                "<RecurringCharge><RecurringChargeAmount>{amount}</RecurringChargeAmount>\
                 <RecurringChargeFrequency>{freq}</RecurringChargeFrequency></RecurringCharge>"
            )
        })
        .collect();
    format!(
        "<ReservedDBInstancesOffering>\
         <ReservedDBInstancesOfferingId>{}</ReservedDBInstancesOfferingId>\
         <DBInstanceClass>{}</DBInstanceClass>\
         <Duration>{}</Duration>\
         <FixedPrice>{}</FixedPrice>\
         <UsagePrice>{}</UsagePrice>\
         <CurrencyCode>{}</CurrencyCode>\
         <ProductDescription>{}</ProductDescription>\
         <OfferingType>{}</OfferingType>\
         <MultiAZ>{}</MultiAZ>\
         <RecurringCharges>{recurring}</RecurringCharges>\
         </ReservedDBInstancesOffering>",
        esc(&o.offering_id),
        esc(&o.db_instance_class),
        o.duration,
        o.fixed_price,
        o.usage_price,
        esc(&o.currency_code),
        esc(&o.product_description),
        esc(&o.offering_type),
        o.multi_az,
    )
}

pub fn reserved_instance_xml(acct: u64, region: &str, r: &ReservedDbInstance) -> String {
    let recurring: String = r
        .recurring
        .iter()
        .map(|(amount, freq)| {
            format!(
                "<RecurringCharge><RecurringChargeAmount>{amount}</RecurringChargeAmount>\
                 <RecurringChargeFrequency>{freq}</RecurringChargeFrequency></RecurringCharge>"
            )
        })
        .collect();
    format!(
        "<ReservedDBInstance>\
         <ReservedDBInstanceId>{}</ReservedDBInstanceId>\
         <ReservedDBInstancesOfferingId>{}</ReservedDBInstancesOfferingId>\
         <DBInstanceClass>{}</DBInstanceClass>\
         <Duration>{}</Duration>\
         <FixedPrice>{}</FixedPrice>\
         <UsagePrice>{}</UsagePrice>\
         <CurrencyCode>{}</CurrencyCode>\
         <ProductDescription>{}</ProductDescription>\
         <OfferingType>{}</OfferingType>\
         <MultiAZ>{}</MultiAZ>\
         <RecurringCharges>{recurring}</RecurringCharges>\
         <DBInstanceCount>{}</DBInstanceCount>\
         <State>active</State>\
         <StartTime>{}</StartTime>\
         <ReservedDBInstanceArn>arn:aws:rds:{region}:{acct}:ri:{}\
         </ReservedDBInstanceArn>\
         </ReservedDBInstance>",
        esc(&r.id),
        esc(&r.offering_id),
        esc(&r.db_instance_class),
        r.duration,
        r.fixed_price,
        r.usage_price,
        esc(&r.currency_code),
        esc(&r.product_description),
        esc(&r.offering_type),
        r.multi_az,
        r.count,
        esc(&iso(&r.start_time)),
        esc(&r.id),
    )
}

pub fn description_of_event(e: &Event) -> String {
    let cats: String = e
        .categories
        .iter()
        .map(|c| format!("<EventCategory>{}</EventCategory>", esc(c)))
        .collect();
    format!(
        "<Event>\
         <SourceIdentifier>{}</SourceIdentifier>\
         <SourceType>{}</SourceType>\
         <Message>{}</Message>\
         <EventCategories>{cats}</EventCategories>\
         <Date>{}</Date>\
         <SourceArn>{}\
         </SourceArn>\
         </Event>",
        esc(&e.source_identifier),
        esc(&e.source_type),
        esc(&e.message),
        esc(&iso(&e.date)),
        esc(&e.source_arn),
    )
}

/// DescribeDBLogFiles list entries.
pub fn db_log_file_entry(name: &str, last_written_millis: i64, size: i64) -> String {
    format!(
        "<DescribeDBLogFilesDetails>\
         <LogFileName>{name}</LogFileName>\
         <LastWritten>{last_written_millis}</LastWritten>\
         <Size>{size}</Size>\
         </DescribeDBLogFilesDetails>",
        name = esc(name),
        last_written_millis = last_written_millis,
    )
}


// ---------------------------------------------------------------------------
// Handler helpers
// ---------------------------------------------------------------------------

fn lowercase_first(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) => c.to_lowercase().to_string() + chars.as_str(),
        None => String::new(),
    }
}

fn effective_engine_version(st: &RdsState, db: &DBInstance) -> String {
    match db.db_cluster_identifier.as_deref().and_then(|c| st.clusters.get(c)) {
        Some(cl) => cl.engine_version.clone(),
        None => db.engine_version.clone(),
    }
}

/// Build a snapshot record the way moto's DBSnapshot init does.
fn snapshot_from_db(db: &DBInstance, snapshot_id: &str, snapshot_type: &str, tags: &[Tag]) -> DBSnapshot {
    DBSnapshot {
        id: snapshot_id.to_lowercase(),
        original_id: snapshot_id.to_string(),
        source: db.clone(),
        snapshot_type: snapshot_type.to_string(),
        status: "available".to_string(),
        tags: if tags.is_empty() && db.copy_tags_to_snapshot {
            db.tags.clone()
        } else {
            tags.to_vec()
        },
        attributes_restore: Vec::new(),
        created: Utc::now(),
        original_created: Utc::now(),
        kms_key_id: if db.storage_encrypted {
            Some("default_kms_key_id".to_string())
        } else {
            None
        },
        encrypted: db.storage_encrypted,
    }
}

/// The default parameter group name for an instance; creates it when absent.
fn ensure_pg_for_instance(st: &mut RdsState, db: &DBInstance) -> String {
    match &db.db_parameter_group_name {
        Some(n) if !n.is_empty() => n.clone(),
        _ => {
            let version = effective_engine_version(st, db);
            let cut = version.rfind('.').unwrap_or(version.len());
            let family = format!("{}{}", db.engine, &version[..cut]);
            let name = format!("default.{family}");
            st.ensure_default_parameter_group(&name, &family);
            name
        }
    }
}

/// Snapshot identifier moto auto-generates for the per-instance backup.
fn auto_snapshot_id(identifier: &str) -> String {
    let timestamp = Utc::now().format("%Y-%m-%d-%H-%M");
    format!("rds:{identifier}-{timestamp}")
}

fn db_cluster_ref(p: &Params) -> Option<String> {
    param(p, "DBClusterIdentifier").string().map(|s| s.to_lowercase())
}


// ---------------------------------------------------------------------------
// Dispatch + response envelope + param accessors
// ---------------------------------------------------------------------------

/// Serialized `<{Op}Result>` inner XML; `None` renders no Result element.
pub type Body = Option<String>;

type Params = BTreeMap<String, Val>;

fn param<'a>(p: &'a Params, name: &str) -> &'a Val {
    p.get(name).unwrap_or(&Val::None)
}

fn p_str<'p>(p: &'p Params, name: &str) -> Option<&'p str> {
    param(p, name).string()
}

fn p_string(p: &Params, name: &str) -> Option<String> {
    param(p, name).string().map(|s| s.to_string())
}

fn p_bool(p: &Params, name: &str) -> Option<bool> {
    param(p, name).opt_bool()
}

fn p_i64(p: &Params, name: &str) -> Option<i64> {
    param(p, name).opt_i64()
}

fn p_f64(p: &Params, name: &str) -> Option<String> {
    param(p, name).string().map(|s| s.to_string())
}

fn p_list(p: &Params, name: &str) -> Vec<String> {
    param(p, name).list()
}

fn p_maps(p: &Params, name: &str) -> Vec<Vec<(String, String)>> {
    param(p, name).map_list()
}

fn tags_param(p: &Params) -> Vec<Tag> {
    let mut out = Vec::new();
    for m in p_maps(p, "Tags") {
        let mut key = None;
        let mut value = None;
        for (k, v) in m {
            if k == "Key" {
                key = Some(v);
            } else if k == "Value" {
                value = Some(v);
            }
        }
        if let (Some(key), Some(value)) = (key, value) {
            out.push(Tag { key, value });
        }
    }
    out
}

fn body_of(p: &Params, name: &str, memb: &str) -> Vec<Vec<(String, String)>> {
    let _ = memb;
    p_maps(p, name)
}

fn request_id() -> String {
    uuid::Uuid::new_v4()
        .simple()
        .to_string()
        .to_uppercase()
        .chars()
        .take(27)
        .collect()
}

fn response_xml(op: &str, inner: Option<String>) -> Response {
    let mut body = match inner {
        Some(s) => format!("<{op}Result>{s}</{op}Result>"),
        None => String::new(),
    };
    let rid = request_id();
    body.push_str(&format!(
        "<ResponseMetadata><RequestId>{rid}</RequestId></ResponseMetadata>"
    ));
    let full = format!("<{op}Response xmlns=\"{XMLNS}\">{body}</{op}Response>");
    Response::xml(200, full.into_bytes())
}

fn error_response(e: &RdsError) -> Response {
    let rid = request_id();
    let full = format!(
        "<ErrorResponse xmlns=\"{XMLNS}\"><Error><Type>Sender</Type>\
         <Code>{}</Code><Message>{}</Message></Error><RequestId>{rid}</RequestId></ErrorResponse>",
        esc(&e.code),
        esc(&e.message)
    );
    Response::xml(e.status, full.into_bytes())
}

/// Subnet ids relevant to subnet-group / proxy-shaped requests.
fn relevant_subnet_ids(p: &Params) -> Vec<String> {
    let mut relevant = p_list(p, "SubnetIds");
    relevant.extend(p_list(p, "VpcSubnetIds"));
    relevant.retain(|s| s.starts_with("subnet-"));
    relevant
}

/// Best-effort EC2 subnet lookup through the local gateway so subnet groups
/// and proxies can report the right VpcId; never fails the request.
fn fetch_subnet_info(
    req: &Request,
    p: &Params,
) -> BTreeMap<String, (Option<String>, Option<String>)> {
    let ids = relevant_subnet_ids(p);
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
        body.push_str("&SubnetIds.member.");
        body.push_str(&(i + 1).to_string());
        body.push('=');
        body.push_str(id);
    }
    let auth = format!(
        "AWS4-HMAC-SHA256 Credential={}/20240101/{}/ec2/aws4_request",
        req.account,
        req.region
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
                let vpc = grab_tag(chunk, "vpcId");
                let az = grab_tag(chunk, "availabilityZone");
                out.insert(id, (vpc, az));
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

/// Entry point: route an RDS op to its handler.
pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "rds" {
        return None;
    }
    let op = req.operation.trim().to_string();
    let params = parse_params(req);
    let subnet_info = fetch_subnet_info(req, &params);
    let slots = store::slot::<RdsState>("rds");
    let mut map = slots.write();
    let key = (req.account, req.region.clone());
    let state = map.entry(key).or_default();
    match run_op(req, state, &op, &params, subnet_info) {
        Ok(body) => Some(response_xml(&op, body)),
        Err(e) => Some(error_response(&e)),
    }
}


fn run_op(
    req: &Request,
    st: &mut RdsState,
    op: &str,
    p: &Params,
    subnet_info: BTreeMap<String, (Option<String>, Option<String>)>,
) -> Result<Body, RdsError> {
    let _ = (req, p, subnet_info);
    let _ = &mut *st;
    Err(RdsError::new(
        501,
        "NotImplemented",
        format!("The {} action has not been implemented", lowercase_first(op)),
    ))
}


// ---------------------------------------------------------------------------
// DB instance operations
// ---------------------------------------------------------------------------

fn cluster_info_missing(cid: Option<&str>, st: &RdsState) -> bool {
    match cid {
        Some(c) => !st.clusters.contains_key(&c.to_lowercase()),
        None => false,
    }
}

fn create_db_instance(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBInstanceIdentifier").unwrap_or_default();
    RdsError::check_db_identifier(&identifier)?;
    let engine = p_string(p, "Engine").unwrap_or_default();
    if !DBInstance::valid_engine(&engine) {
        return Err(RdsError::client400(
            "InvalidParameterValue",
            format!("Value {engine} for parameter Engine is invalid. Reason: engine {engine} not supported"),
        ));
    }
    let id = identifier.to_lowercase();
    if st.databases.contains_key(&id) {
        return Err(RdsError::client400(
            "DBInstanceAlreadyExists",
            format!("DBInstance {identifier} already exists"),
        ));
    }
    let (storage_default, _) = DBInstance::default_storage(&engine);
    let engine_version = match p_string(p, "EngineVersion") {
        Some(v) => v,
        None => DBInstance::default_engine_version(&engine).unwrap_or("").to_string(),
    };
    let supplied_og = p_string(p, "OptionGroupName");
    let option_group = match &supplied_og {
        Some(name) => {
            if !st.option_groups.contains_key(&name.to_lowercase()) {
                return Err(RdsError::not_found404(
                    "OptionGroupNotFoundFault",
                    format!("Specified OptionGroupName: {name} not found."),
                ));
            }
            name.clone()
        }
        None => DBCluster::compute_default_option_group(&engine, &engine_version),
    };
    let supplied_db_subnet = p_string(p, "DBSubnetGroupName");
    if let Some(group) = &supplied_db_subnet {
        if !st.subnet_groups.contains_key(&group.to_lowercase()) {
            return Err(RdsError::not_found404(
                "DBSubnetGroupNotFoundFault",
                format!("Subnet Group {group} not found."),
            ));
        }
    }
    if let Some(pg) = p_string(p, "DBParameterGroupName") {
        let default_prefix = format!("default.{}", engine.to_lowercase());
        if !pg.to_lowercase().starts_with(&default_prefix)
            && !st.db_parameter_groups.contains_key(&pg.to_lowercase())
        {
            return Err(RdsError::not_found404(
                "DBParameterGroupNotFound",
                format!("DB Parameter Group {pg} not found."),
            ));
        }
    }
    let master_username = p_string(p, "MasterUsername");
    let cluster_name = p_string(p, "DBClusterIdentifier");
    let cluster_ref = cluster_name.clone().map(|s| s.to_lowercase());
    let managing_password = p_bool(p, "ManageMasterUserPassword").unwrap_or(false);
    if cluster_ref.is_none() && master_username.is_none() && !managing_password {
        return Err(RdsError::client400(
            "InvalidParameterValue",
            "The parameter MasterUsername must be provided and must not be blank.",
        ));
    }
    if let Some(pass) = p_string(p, "MasterUserPassword") {
        if pass.len() < 8 && !managing_password {
            return Err(RdsError::client400(
                "InvalidParameterValue",
                "The parameter MasterUserPassword is not a valid password because it is shorter than 8 characters.",
            ));
        }
    }
    if cluster_info_missing(cluster_ref.as_deref(), st) {
        let shown = cluster_name.clone().unwrap_or_default();
        return Err(RdsError::not_found404(
            "DBClusterNotFoundFault",
            format!("The source cluster could not be found or cannot be accessed: {shown}"),
        ));
    }
    let cluster = cluster_ref
        .as_ref()
        .and_then(|cid| st.clusters.get(cid));
    let cluster_engine = cluster.map(|cl| cl.engine.clone());
    let cluster_version = cluster.map(|cl| cl.engine_version.clone());
    let cluster_username = cluster.and_then(|cl| cl.master_username.clone());
    if cluster_engine.is_some() && cluster_engine.as_deref() != Some(engine.as_str()) {
        let ce = cluster_engine.clone().unwrap_or_default();
        return Err(RdsError::client400(
            "InvalidParameterCombination",
            format!("The engine name requested for your DB instance ({engine}) doesn't match the engine name of your DB cluster ({ce})."),
        ));
    }
    let db = DBInstance {
        id: id.clone(),
        original_id: identifier.clone(),
        db_instance_class: p_string(p, "DBInstanceClass").unwrap_or_else(|| "db.t3.micro".into()),
        engine: engine.clone(),
        engine_version: cluster_version.clone().unwrap_or(engine_version),
        port: p_i64(p, "Port").unwrap_or_else(|| DBInstance::default_port(&engine)),
        status: "available".into(),
        master_username: if cluster.is_some() {
            None
        } else {
            master_username.clone()
        },
        allocated_storage: p_i64(p, "AllocatedStorage").unwrap_or(storage_default),
        max_allocated_storage: p_i64(p, "MaxAllocatedStorage"),
        storage_type: p_string(p, "StorageType").unwrap_or_else(|| "gp2".into()),
        storage_encrypted: p_bool(p, "StorageEncrypted").unwrap_or(false),
        backup_retention_period: p_i64(p, "BackupRetentionPeriod").unwrap_or(1),
        auto_minor_version_upgrade: p_bool(p, "AutoMinorVersionUpgrade").unwrap_or(true),
        license_model: p_string(p, "LicenseModel")
            .unwrap_or_else(|| "general-public-license".into()),
        preferred_backup_window: p_string(p, "PreferredBackupWindow")
            .unwrap_or_else(|| "13:14-13:44".into()),
        preferred_maintenance_window: p_string(p, "PreferredMaintenanceWindow")
            .unwrap_or_else(|| "wed:06:38-wed:07:08".into())
            .to_lowercase(),
        availability_zone: p_string(p, "AvailabilityZone")
            .unwrap_or_else(|| format!("{region}a")),
        multi_az: p_bool(p, "MultiAZ").unwrap_or(false),
        publicly_accessible: p_bool(p, "PubliclyAccessible"),
        copy_tags_to_snapshot: p_bool(p, "CopyTagsToSnapshot").unwrap_or(false),
        db_parameter_group_name: p_string(p, "DBParameterGroupName"),
        option_group_name: option_group,
        option_group_supplied: supplied_og.is_some(),
        vpc_security_group_ids: p_list(p, "VpcSecurityGroupIds"),
        db_security_groups: p_list(p, "DBSecurityGroups"),
        db_subnet_group_name: supplied_db_subnet,
        db_cluster_identifier: cluster_ref,
        is_replica: false,
        replicas: Vec::new(),
        source_db_instance_identifier: None,
        deletion_protection: p_bool(p, "DeletionProtection").unwrap_or(false),
        iam_auth: p_bool(p, "EnableIAMDatabaseAuthentication").unwrap_or(false),
        ca_certificate_identifier: p_string(p, "CACertificateIdentifier")
            .unwrap_or_else(|| "rds-ca-default".into()),
        tags: tags_param(p),
        enabled_cloudwatch_logs_exports: p_list(p, "EnableCloudwatchLogsExports"),
        associated_roles: Vec::new(),
        created: Utc::now(),
        db_name: p_string(p, "DBName"),
        character_set_name: p_string(p, "CharacterSetName"),
    };
    let _ = cluster_username;
    ensure_pg_for_instance(st, &db);
    st.databases.insert(db.id.clone(), db);
    let arn = format!("arn:aws:rds:{region}:{acct}:db:{id}");
    st.add_event("DB_INSTANCE_CREATE", &id, arn);
    create_automated_snapshot(st, acct, region, &id, &auto_snapshot_id(&id));
    match st.databases.get(&id) {
        Some(found) => Ok(Some(db_instance_xml(st, acct, region, found))),
        None => Ok(None),
    }
}


/// Snapshot record from an instance with the moto auto-id, stored as `automated`.
fn create_automated_snapshot(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    db_id: &str,
    snapshot_id: &str,
) {
    if let Some(db) = st.databases.get(db_id).cloned() {
        let snapshot = snapshot_from_db(&db, snapshot_id, "automated", &[]);
        let snap_id = snapshot.id.clone();
        st.database_snapshots.insert(snap_id, snapshot);
        let _ = (acct, region);
    }
}


// DB instance describe/modify/delete/lifecycle -------------------------------

fn describe_db_instances_xml(
    st: &RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let filter = p_string(p, "DBInstanceIdentifier");
    let matched: Vec<&DBInstance> = match &filter {
        Some(rid) => {
            let lower = rid.to_lowercase();
            let found: Vec<&DBInstance> =
                st.databases.values().filter(|d| d.id == lower).collect();
            if found.is_empty() {
                return Err(RdsError::not_found404(
                    "DBInstanceNotFound",
                    format!("DBInstance {rid} not found."),
                ));
            }
            found
        }
        None => st.databases.values().collect(),
    };
    let mut list = String::new();
    for db in &matched {
        list.push_str(&db_instance_xml(st, acct, region, db));
    }
    Ok(Some(format!("<DBInstances>{list}</DBInstances>")))
}

/// Apply ModifyDBInstance kwargs (moto `database.update(**db_kwargs)`).
fn apply_instance_update(db: &mut DBInstance, p: &Params) {
    if let Some(v) = p_string(p, "DBInstanceClass") {
        db.db_instance_class = v;
    }
    if let Some(v) = p_string(p, "EngineVersion") {
        db.engine_version = v;
    }
    if let Some(v) = p_string(p, "AllocatedStorage") {
        if let Ok(n) = v.parse::<i64>() {
            db.allocated_storage = n;
        }
    }
    if let Some(v) = p_string(p, "MaxAllocatedStorage") {
        if let Ok(n) = v.parse::<i64>() {
            db.max_allocated_storage = Some(n);
        }
    }
    if let Some(v) = p_string(p, "BackupRetentionPeriod") {
        if let Ok(n) = v.parse::<i64>() {
            db.backup_retention_period = n;
        }
    }
    if let Some(v) = p_string(p, "PreferredBackupWindow") {
        db.preferred_backup_window = v;
    }
    if let Some(v) = p_string(p, "PreferredMaintenanceWindow") {
        db.preferred_maintenance_window = v.to_lowercase();
    }
    if let Some(v) = p_bool(p, "MultiAZ") {
        db.multi_az = v;
    }
    if let Some(v) = p_string(p, "AvailabilityZone") {
        db.availability_zone = v;
    }
    if let Some(v) = p_string(p, "DBParameterGroupName") {
        db.db_parameter_group_name = Some(v);
    }
    if let Some(v) = p_string(p, "OptionGroupName") {
        db.option_group_name = v;
        db.option_group_supplied = true;
    }
    if let Some(v) = p_string(p, "DBSubnetGroupName") {
        db.db_subnet_group_name = Some(v);
    }
    if let Some(v) = p_string(p, "MasterUsername") {
        db.master_username = Some(v);
    }
    if let Some(v) = p_bool(p, "CopyTagsToSnapshot") {
        db.copy_tags_to_snapshot = v;
    }
    if let Some(v) = p_string(p, "StorageType") {
        db.storage_type = v;
    }
    if let Some(v) = p_bool(p, "StorageEncrypted") {
        db.storage_encrypted = v;
    }
    if let Some(v) = p_string(p, "LicenseModel") {
        db.license_model = v;
    }
    if let Some(v) = p_bool(p, "AutoMinorVersionUpgrade") {
        db.auto_minor_version_upgrade = v;
    }
    if let Some(v) = p_bool(p, "DeletionProtection") {
        db.deletion_protection = v;
    }
    if let Some(v) = p_string(p, "CACertificateIdentifier") {
        db.ca_certificate_identifier = v;
    }
    if let Some(v) = p_i64(p, "Port") {
        db.port = v;
    }
    if let Some(v) = p_string(p, "CharacterSetName") {
        db.character_set_name = Some(v);
    }
}

fn modify_db_instance(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBInstanceIdentifier").unwrap_or_default();
    let id = identifier.to_lowercase();
    if !st.databases.contains_key(&id) {
        return Err(RdsError::not_found404(
            "DBInstanceNotFound",
            format!("DBInstance {identifier} not found."),
        ));
    }
    if let Some(group) = p_string(p, "DBParameterGroupName") {
        if !st.db_parameter_groups.contains_key(&group.to_lowercase()) {
            return Err(RdsError::not_found404(
                "DBParameterGroupNotFound",
                format!("DB Parameter Group {group} not found."),
            ));
        }
    }
    if let Some(v) = p_string(p, "MasterUserPassword") {
        if v.len() < 8 {
            return Err(RdsError::client400(
                "InvalidParameterValue",
                "The parameter MasterUserPassword is not a valid password because it is shorter than 8 characters.",
            ));
        }
    }
    if p_bool(p, "RotateMasterUserPassword").unwrap_or(false)
        && !p_bool(p, "ApplyImmediately").unwrap_or(false)
    {
        return Err(RdsError::client400(
            "InvalidParameterCombination",
            "You must specify apply immediately when rotating the master user password.",
        ));
    }
    let new_id = p_string(p, "NewDBInstanceIdentifier");
    {
        let db = st.databases.get_mut(&id).unwrap();
        apply_instance_update(db, p);
    }
    let mut renamed_to: Option<String> = None;
    if let Some(new_id) = new_id {
        RdsError::check_db_identifier(&new_id)?;
        let lower = new_id.to_lowercase();
        if lower != id && st.databases.contains_key(&lower) {
            return Err(RdsError::client400(
                "DBInstanceAlreadyExists",
                format!("DBInstance {new_id} already exists"),
            ));
        }
        if let Some(mut db) = st.databases.remove(&id) {
            db.id = lower.clone();
            db.original_id = new_id;
            st.databases.insert(lower.clone(), db);
            renamed_to = Some(lower);
        }
    }
    let lookup = renamed_to.unwrap_or_else(|| id.clone());
    match st.databases.get(&lookup) {
        Some(found) => Ok(Some(db_instance_xml(st, acct, region, found))),
        None => Ok(None),
    }
}

fn delete_db_instance(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBInstanceIdentifier").unwrap_or_default();
    let id = identifier.to_lowercase();
    let db = match st.databases.get(&id) {
        Some(db) => db.clone(),
        None => {
            return Err(RdsError::not_found404(
                "DBInstanceNotFound",
                format!("DBInstance {identifier} not found."),
            ))
        }
    };
    if db.deletion_protection {
        return Err(RdsError::client400(
            "InvalidParameterCombination",
            "Cannot delete protected DB Instance, please disable deletion protection and try again.",
        ));
    }
    let skip_final = p_bool(p, "SkipFinalSnapshot").unwrap_or(false);
    if let Some(final_snap) = p_string(p, "FinalDBSnapshotIdentifier") {
        RdsError::check_snapshot_identifier(&final_snap, "FinalDBSnapshotIdentifier")?;
        if !skip_final {
            let snapshot = snapshot_from_db(&db, &final_snap, "manual", &[]);
            let snap_id = snapshot.id.clone();
            st.database_snapshots.insert(snap_id, snapshot);
        }
    }
    st.databases.remove(&id);
    if db.is_replica {
        if let Some(src) = &db.source_db_instance_identifier {
            if let Some(src_db) = st.databases.get_mut(&src.to_lowercase()) {
                src_db.replicas.retain(|r| *r != db.id);
            }
        }
    }
    if p_bool(p, "DeleteAutomatedBackups").unwrap_or(true) {
        let auto_ids: Vec<String> = st
            .database_snapshots
            .iter()
            .filter(|(_, snap)| snap.snapshot_type == "automated" && snap.source.id == db.id)
            .map(|(k, _)| k.clone())
            .collect();
        for snap_id in auto_ids {
            st.database_snapshots.remove(&snap_id);
        }
    }
    let response_db = DBInstance {
        status: "deleting".to_string(),
        ..db
    };
    Ok(Some(db_instance_xml(st, acct, region, &response_db)))
}

fn create_manual_snapshot(db: &DBInstance, snapshot_id: &str, tags: &[Tag]) -> DBSnapshot {
    snapshot_from_db(db, snapshot_id, "manual", tags)
}

// __APPEND_MARKER__
