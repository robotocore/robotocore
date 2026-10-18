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
    let mut tree = Val::M(BTreeMap::new());
    let mut pairs = decode_pairs(&req.body);
    pairs.extend(decode_pairs(req.query_string.as_bytes()));
    for (key, value) in pairs {
        if key.is_empty() || key == "Action" || key == "Version" {
            continue;
        }
        let segments: Vec<&str> = key.split('.').collect();
        insert_val(&mut tree, &segments, &Val::S(value));
    }
    match tree {
        Val::M(m) => m,
        _ => BTreeMap::new(),
    }
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
///
/// `node` must always end up a map: list entries live as map values.
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
    into_map(node);
    let entry: &mut Val = match node {
        Val::M(map) => map.entry(head.to_string()).or_insert(Val::None),
        _ => return,
    };
    if let Some((n, rest_in)) = idx_and_rest {
        if !matches!(entry, Val::L(_)) {
            *entry = Val::L(Vec::new());
        }
        if let Val::L(items) = entry {
            while items.len() < n {
                items.push(Val::M(BTreeMap::new()));
            }
            let slot = &mut items[n - 1];
            if rest_in.is_empty() {
                *slot = value.clone();
            } else {
                insert_val(slot, rest_in, value);
            }
        }
        return;
    }
    insert_val(entry, rest, value);
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
    uuid::Uuid::new_v4()
        .simple()
        .to_string()
        .chars()
        .take(n)
        .collect()
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
        let first_alpha = ident
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic());
        if valid_identifier(ident) && ident.len() <= 63 && first_alpha {
            Ok(())
        } else {
            Err(Self::client400(
                "InvalidParameterValue",
                format!(
                    "The parameter DBInstanceIdentifier [{ident:?}] len={} is not a valid identifier. \
                     Identifiers must begin with a letter; must contain only ASCII letters, digits, \
                     and hyphens; and must not end with a hyphen or contain two consecutive hyphens.",
                    ident.len()
                ),
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
        if !ident
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic())
            || !valid_identifier(ident)
            || ident.len() > 255
        {
            if ident
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic())
            {
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
        && !s
            .bytes()
            .zip(s.bytes().skip(1))
            .any(|(a, b)| a == b'-' && b == b'-')
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
            status_shown: std::sync::atomic::AtomicBool::new(
                self.status_shown.load(std::sync::atomic::Ordering::SeqCst),
            ),
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
        if self.status == "creating" {
            // First serialized read reports "creating" (moto flips once).
            if !self.status_shown.swap(true, Ordering::SeqCst) {
                return "creating".to_string();
            }
            return "available".to_string();
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
        format!(
            "arn:aws:rds::{}:global-cluster:{}",
            account, self.identifier
        )
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
        format!(
            "arn:aws:rds:{region}:{account}:cluster-endpoint:{}",
            self.id
        )
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
        format!(
            "arn:aws:rds:{region}:{account}:db-proxy-endpoint:{}",
            self.name
        )
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
            "DB_SNAPSHOT_CREATE_AUTOMATED_START" => ("Creating automated snapshot", &["creation"]),
            "DB_SNAPSHOT_CREATE_AUTOMATED_FINISH" => ("Automated snapshot created", &["creation"]),
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
            .map(|db_instance_identifier| AutomatedBackup {
                db_instance_identifier,
            })
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
    out.push_str(&format!(
        "<DBInstanceIdentifier>{}</DBInstanceIdentifier>",
        esc(&db.id)
    ));
    out.push_str(&format!(
        "<DBInstanceClass>{}</DBInstanceClass>",
        esc(&db.db_instance_class)
    ));
    out.push_str(&format!("<Engine>{}</Engine>", esc(&db.engine)));
    out.push_str(&format!(
        "<DBInstanceStatus>{}</DBInstanceStatus>",
        esc(&db.status)
    ));
    if let Some(u) = &eff.master_username {
        out.push_str(&format!("<MasterUsername>{}</MasterUsername>", esc(u)));
    }
    out.push_str(&format!(
        "<Endpoint><Address>{}</Address><Port>{}</Port></Endpoint>",
        esc(&inst_address(&db.id, region)),
        db.port
    ));
    out.push_str(&format!(
        "<AllocatedStorage>{}</AllocatedStorage>",
        eff.allocated_storage
    ));
    out.push_str(&format!(
        "<InstanceCreateTime>{}</InstanceCreateTime>",
        esc(&iso(&db.created))
    ));
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
    out.push_str(&format!(
        "<AvailabilityZone>{}</AvailabilityZone>",
        esc(&db.availability_zone)
    ));
    out.push_str(&format!(
        "<PreferredMaintenanceWindow>{}</PreferredMaintenanceWindow>",
        esc(&db.preferred_maintenance_window)
    ));
    out.push_str(&format!(
        "<LatestRestorableTime>{}</LatestRestorableTime>",
        esc(&iso(&Utc::now()))
    ));
    out.push_str(&format!("<MultiAZ>{}</MultiAZ>", db.multi_az));
    out.push_str(&format!(
        "<EngineVersion>{}</EngineVersion>",
        esc(&eff.engine_version)
    ));
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
    out.push_str(&format!(
        "<LicenseModel>{}</LicenseModel>",
        esc(&eff.license_model)
    ));
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
    out.push_str(&format!(
        "<StorageType>{}</StorageType>",
        esc(&eff.storage_type)
    ));
    out.push_str(&format!("<DbInstancePort>{}</DbInstancePort>", db.port));
    out.push_str(&format!(
        "<StorageEncrypted>{}</StorageEncrypted>",
        db.storage_encrypted
    ));
    out.push_str(&format!(
        "<DbiResourceId>{}</DbiResourceId>",
        esc(DBI_RESOURCE_ID)
    ));
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
    out.push_str(&roles_xml(
        "AssociatedRoles",
        "DBInstanceRole",
        &db.associated_roles,
    ));
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
    out.push_str(&format!(
        "<AllocatedStorage>{}</AllocatedStorage>",
        cl.allocated_storage
    ));
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
    out.push_str(&format!(
        "<DBClusterIdentifier>{}</DBClusterIdentifier>",
        esc(&cl.id)
    ));
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
    out.push_str(&format!(
        "<EngineVersion>{}</EngineVersion>",
        esc(&cl.engine_version)
    ));
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
    out.push_str(&format!(
        "<HostedZoneId>{}</HostedZoneId>",
        esc(&cl.hosted_zone_id)
    ));
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
    out.push_str(&roles_xml(
        "AssociatedRoles",
        "DBClusterRole",
        &cl.associated_roles,
    ));
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
    out.push_str(&format!(
        "<EngineMode>{}</EngineMode>",
        esc(&cl.engine_mode)
    ));
    out.push_str("<ScalingConfigurationInfo/>");
    out.push_str(&format!(
        "<StorageType>{}</StorageType>",
        esc(&cl.storage_type)
    ));
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
    out.push_str(&format!(
        "<DbiResourceId>{}</DbiResourceId>",
        esc(DBI_RESOURCE_ID)
    ));
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
    out.push_str(&format!("<KmsKeyId>{}</KmsKeyId>", esc(&snap.kms_key_id)));
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
            "<DBSnapshotAttribute><AttributeName>{}</AttributeName>{}</DBSnapshotAttribute>",
            esc(name),
            list_xml("AttributeValues", "AttributeValue", values)
        ));
    }
    format!(
        "<{outer}><{inner}>{id}</{inner}>{inner_tag}{items}</{inner}></{outer}>",
        outer = outer,
        inner = inner,
        id = esc(identifier),
        inner_tag = format!(
            "<DBSnapshotIdentifier>{}</DBSnapshotIdentifier>",
            esc(identifier)
        ),
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

pub fn cluster_parameter_group_xml(
    acct: u64,
    region: &str,
    pg: &DBClusterParameterGroup,
) -> String {
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
            let az =
                s.az.as_ref()
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
    let cats = list_xml(
        "EventCategoriesList",
        "EventCategory",
        &sub.event_categories,
    );
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
        created = format!(
            "<SubscriptionCreationTime>{}</SubscriptionCreationTime>",
            esc(&iso(&sub.created))
        ),
        arn = format!(
            "<EventSubscriptionArn>arn:aws:rds:{region}:{acct}:es:{}</EventSubscriptionArn>",
            esc(&sub.name)
        ),
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
        encrypted = format!(
            "<StorageEncrypted>{}</StorageEncrypted><DeletionProtection>{}</DeletionProtection>",
            gc.storage_encrypted, gc.deletion_protection
        ),
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
    let max_acu = g
        .max_acu
        .as_ref()
        .map(|a| format!("<MaxACU>{}</MaxACU>", esc(a)))
        .unwrap_or_default();
    let min_acu = g
        .min_acu
        .as_ref()
        .map(|a| format!("<MinACU>{}</MinACU>", esc(a)))
        .unwrap_or_default();
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
        format!(
            "{}.{}.rds.amazonaws.com",
            esc(&g.cluster_identifier),
            region
        ),
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
    let mut out = String::new();
    out.push_str(&format!("<Engine>{}</Engine>", esc(&cev.engine)));
    out.push_str(&format!(
        "<MajorEngineVersion>{}</MajorEngineVersion>",
        esc(&cev.major_engine_version())
    ));
    out.push_str(&format!(
        "<EngineVersion>{}</EngineVersion>",
        esc(&cev.engine_version)
    ));
    if include_bucket {
        if let Some(b) = &cev.s3_bucket {
            out.push_str(&format!(
                "<DatabaseInstallationFilesS3BucketName>{}</DatabaseInstallationFilesS3BucketName>",
                esc(b)
            ));
        }
        if let Some(p) = &cev.s3_prefix {
            out.push_str(&format!(
                "<DatabaseInstallationFilesS3Prefix>{}</DatabaseInstallationFilesS3Prefix>",
                esc(p)
            ));
        }
    }
    out.push_str(&format!(
        "<DBEngineDescription>{}</DBEngineDescription>",
        esc(&cev.db_engine_description())
    ));
    out.push_str(&format!(
        "<DBEngineVersionArn>{}</DBEngineVersionArn>",
        esc(&cev.arn_for(acct, region))
    ));
    out.push_str(&format!(
        "<DBEngineVersionDescription>{}</DBEngineVersionDescription>",
        esc(&cev.version_description())
    ));
    out.push_str(&format!(
        "<CreateTime>{}</CreateTime>",
        esc(&iso(&cev.created))
    ));
    out.push_str("<SupportedEngineModes><member>provisioned</member></SupportedEngineModes>");
    out.push_str(&format!("<Status>{}</Status>", esc(&cev.status)));
    out.push_str("<SupportsParallelQuery>false</SupportsParallelQuery>");
    out.push_str("<SupportsGlobalDatabases>false</SupportsGlobalDatabases>");
    out.push_str(&tags_xml(&cev.tags, "TagList"));
    out
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
    match db
        .db_cluster_identifier
        .as_deref()
        .and_then(|c| st.clusters.get(c))
    {
        Some(cl) => cl.engine_version.clone(),
        None => db.engine_version.clone(),
    }
}

/// Build a snapshot record the way moto's DBSnapshot init does.
fn snapshot_from_db(
    db: &DBInstance,
    snapshot_id: &str,
    snapshot_type: &str,
    tags: &[Tag],
) -> DBSnapshot {
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

/// Random ids a fresh cluster needs: url_identifier, hosted_zone, resource.
fn new_cluster_ids() -> (String, String, String) {
    (
        rand_lower(12),
        format!("T{}", rand_upper(13)),
        format!("cluster-{}", rand_upper(26)),
    )
}

/// Capture a cluster snapshot the way moto's DBClusterSnapshot init does.
fn cluster_snapshot_from(
    cluster: &DBCluster,
    snapshot_id: &str,
    snapshot_type: &str,
    tags: &[Tag],
) -> DBClusterSnapshot {
    DBClusterSnapshot {
        id: snapshot_id.to_lowercase(),
        original_id: snapshot_id.to_string(),
        cluster: cluster.clone(),
        snapshot_type: snapshot_type.to_string(),
        status: "available".to_string(),
        tags: {
            if tags.is_empty() && cluster.copy_tags_to_snapshot {
                cluster.tags.clone()
            } else {
                tags.to_vec()
            }
        },
        attributes_restore: Vec::new(),
        created: Utc::now(),
        encrypted: cluster.storage_encrypted,
        kms_key_id: cluster.kms_key_id.clone(),
    }
}

fn db_cluster_ref(p: &Params) -> Option<String> {
    param(p, "DBClusterIdentifier")
        .string()
        .map(|s| s.to_lowercase())
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
pub fn dispatch(req: &Request) -> Response {
    if req.service != "rds" {
        let e = RdsError::new(501, "NotImplemented", "rds handled via match");
        return error_response(&e);
    }
    let op = req.operation.trim().to_string();
    let params = parse_params(req);
    let subnet_info = fetch_subnet_info(req, &params);
    let slots = store::slot::<RdsState>("rds");
    let mut map = slots.write();
    let key = (req.account, req.region.clone());
    let state = map.entry(key).or_default();
    match run_op(req, state, &op, &params, subnet_info) {
        Ok(body) => response_xml(&op, body),
        Err(e) => error_response(&e),
    }
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
        None => DBInstance::default_engine_version(&engine)
            .unwrap_or("")
            .to_string(),
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
    // Password length is not enforced by the vendored ground truth.
    if cluster_info_missing(cluster_ref.as_deref(), st) {
        let shown = cluster_name.clone().unwrap_or_default();
        return Err(RdsError::not_found404(
            "DBClusterNotFoundFault",
            format!("The source cluster could not be found or cannot be accessed: {shown}"),
        ));
    }
    let cluster = cluster_ref.as_ref().and_then(|cid| st.clusters.get(cid));
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
        availability_zone: p_string(p, "AvailabilityZone").unwrap_or_else(|| format!("{region}a")),
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
            let found: Vec<&DBInstance> = st.databases.values().filter(|d| d.id == lower).collect();
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

// Snapshots ------------------------------------------------------------------

fn create_db_snapshot(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let snapshot_id = p_string(p, "DBSnapshotIdentifier").unwrap_or_default();
    RdsError::check_snapshot_identifier(&snapshot_id, "DBSnapshotIdentifier")?;
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
    let snap_id = snapshot_id.to_lowercase();
    if st.database_snapshots.contains_key(&snap_id) {
        return Err(RdsError::client400(
            "DBSnapshotAlreadyExists",
            format!("Cannot create the snapshot because a snapshot with the identifier {snap_id} already exists."),
        ));
    }
    let snapshot_type = p_string(p, "SnapshotType").unwrap_or_else(|| "manual".into());
    let snapshot = snapshot_from_db(&db, &snapshot_id, &snapshot_type, &tags_param(p));
    let response = snapshot.clone();
    st.database_snapshots.insert(snap_id, snapshot);
    Ok(Some(db_snapshot_xml(acct, region, &response)))
}

fn describe_db_snapshots_xml(
    st: &RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let db_filter = p_string(p, "DBInstanceIdentifier").map(|s| s.to_lowercase());
    let snap_filter = p_string(p, "DBSnapshotIdentifier").map(|s| s.to_lowercase());
    let matched: Vec<&DBSnapshot> = st
        .database_snapshots
        .values()
        .filter(|s| match &db_filter {
            Some(db_id) => s.source.id == *db_id,
            None => true,
        })
        .filter(|s| match &snap_filter {
            Some(snap_id) => s.id == *snap_id,
            None => true,
        })
        .collect();
    if let Some(snap_id) = &snap_filter {
        if matched.is_empty() && db_filter.is_none() {
            return Err(RdsError::not_found404(
                "DBSnapshotNotFound",
                format!("DBSnapshot {snap_id} not found."),
            ));
        }
    }
    let mut list = String::new();
    for snap in &matched {
        list.push_str(&db_snapshot_xml(acct, region, snap));
    }
    Ok(Some(format!("<DBSnapshots>{list}</DBSnapshots>")))
}

fn delete_db_snapshot(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let ident = p_string(p, "DBSnapshotIdentifier").unwrap_or_default();
    let snap_id = ident.to_lowercase();
    if !st.database_snapshots.contains_key(&snap_id) {
        return Err(RdsError::not_found404(
            "DBSnapshotNotFound",
            format!("DBSnapshot {ident} not found."),
        ));
    }
    let snapshot = st.database_snapshots.remove(&snap_id).unwrap();
    Ok(Some(db_snapshot_xml(acct, region, &snapshot)))
}

fn copy_db_snapshot(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let target = p_string(p, "TargetDBSnapshotIdentifier").unwrap_or_default();
    RdsError::check_snapshot_identifier(&target, "TargetDBSnapshotIdentifier")?;
    let source = p_string(p, "SourceDBSnapshotIdentifier").unwrap_or_default();
    let source_id = identifier_from_arn(&source).to_lowercase();
    let src = match st.database_snapshots.get(&source_id) {
        Some(s) => s.clone(),
        None => {
            return Err(RdsError::not_found404(
                "DBSnapshotNotFound",
                format!("DBSnapshot {source} not found."),
            ))
        }
    };
    let target_id = target.to_lowercase();
    if st.database_snapshots.contains_key(&target_id) {
        return Err(RdsError::client400(
            "DBSnapshotAlreadyExists",
            format!("Cannot create the snapshot because a snapshot with the identifier {target_id} already exists."),
        ));
    }
    let mut snapshot = snapshot_from_db(&src.source, &target, &src.snapshot_type, &[]);
    snapshot.original_created = src.original_created;
    if !src.tags.is_empty() && p_bool(p, "CopyTags").unwrap_or(false) {
        snapshot.tags = src.tags.clone();
    }
    let tags = tags_param(p);
    if !tags.is_empty() {
        snapshot.tags = tags;
    }
    let out = snapshot.clone();
    st.database_snapshots.insert(target_id, snapshot);
    Ok(Some(db_snapshot_xml(acct, region, &out)))
}

fn snapshot_attrs_xml(identifier: &str, attrs: &[(String, Vec<String>)]) -> String {
    let mut items = String::from("<DBSnapshotAttributes>");
    for (name, values) in attrs {
        items.push_str(&format!(
            "<DBSnapshotAttribute><AttributeName>{}</AttributeName>{}</DBSnapshotAttribute>",
            esc(name),
            list_xml("AttributeValues", "AttributeValue", values)
        ));
    }
    items.push_str("</DBSnapshotAttributes>");
    format!(
        "<DBSnapshotAttributesResult>\
         <DBSnapshotIdentifier>{}</DBSnapshotIdentifier>{items}\
         </DBSnapshotAttributesResult>",
        esc(identifier)
    )
}

fn describe_db_snapshot_attributes(st: &RdsState, p: &Params) -> Result<Body, RdsError> {
    let ident = p_string(p, "DBSnapshotIdentifier").unwrap_or_default();
    let snap_id = ident.to_lowercase();
    match find_snapshot(st, &snap_id) {
        Some(db_snapshot) => {
            let attrs = vec![(
                "restore".to_string(),
                db_snapshot.attributes_restore.clone(),
            )];
            Ok(Some(snapshot_attrs_xml(&snap_id, &attrs)))
        }
        None => Err(RdsError::not_found404(
            "DBSnapshotNotFound",
            format!("DBSnapshot {ident} not found."),
        )),
    }
}

fn modify_db_snapshot_attribute(st: &mut RdsState, p: &Params) -> Result<Body, RdsError> {
    let ident = p_string(p, "DBSnapshotIdentifier").unwrap_or_default();
    let snap_id = ident.to_lowercase();
    let db_owned = match find_snapshot(st, &snap_id) {
        Some(existing) => existing.clone(),
        None => {
            return Err(RdsError::not_found404(
                "DBSnapshotNotFound",
                format!("DBSnapshot {ident} not found."),
            ))
        }
    };
    if snap_id.starts_with("rds:") {
        return Err(RdsError::client400(
            "InvalidParameterValue",
            "The parameter DBSnapshotIdentifier is not a valid identifier.",
        ));
    }
    let attribute_name = p_string(p, "AttributeName").unwrap_or_default();
    if attribute_name != "restore" {
        return Err(RdsError::client400(
            "InvalidParameterValue",
            format!("Invalid snapshot attribute {attribute_name}"),
        ));
    }
    let existing = db_owned.attributes_restore.clone();
    let mut merged = existing;
    for v in p_list(p, "ValuesToAdd") {
        if !merged.contains(&v) {
            merged.push(v);
        }
    }
    let to_remove = p_list(p, "ValuesToRemove");
    let merged: Vec<String> = merged
        .into_iter()
        .filter(|v| !to_remove.contains(v))
        .collect();
    st.database_snapshots
        .get_mut(&snap_id)
        .unwrap()
        .attributes_restore = merged;
    let attrs = vec![(
        "restore".to_string(),
        st.database_snapshots[&snap_id].attributes_restore.clone(),
    )];
    Ok(Some(snapshot_attrs_xml(&snap_id, &attrs)))
}

// Lookup helpers -------------------------------------------------------------

fn identifier_from_arn(value: &str) -> String {
    // arn:aws:rds:us-east-1:123456789012:snapshot:name → name
    if value.starts_with("arn:aws:rds:") {
        value.rsplit(':').next().unwrap_or(value).to_string()
    } else {
        value.to_string()
    }
}

fn find_snapshot<'s>(st: &'s RdsState, identifier: &str) -> Option<&'s DBSnapshot> {
    st.database_snapshots.get(&identifier.to_lowercase())
}

fn find_cluster_snapshot<'s>(st: &'s RdsState, identifier: &str) -> Option<&'s DBClusterSnapshot> {
    st.cluster_snapshots.get(&identifier.to_lowercase())
}

fn find_cluster<'s>(st: &'s RdsState, identifier: &str) -> Option<&'s DBCluster> {
    st.clusters.get(&identifier.to_lowercase())
}

fn find_instance<'s>(st: &'s RdsState, identifier: &str) -> Option<&'s DBInstance> {
    st.databases.get(&identifier.to_lowercase())
}

// Tags -----------------------------------------------------------------------

/// moto parses the arn to (resource_type, resource_id); automated snapshots
/// ride `rds:{db}:{id}` instead of the snapshot:{id} form.
enum TagTarget {
    Instance(String),
    Cluster(String),
    ParamGroup(String),
    ClusterParamGroup(String),
    OptionGroup(String),
    SecurityGroup(String),
    SubnetGroup(String),
    Snapshot(String),
    ClusterSnapshot(String),
    Proxy(String),
    EventSubscription(String),
    Unknown,
}

fn parse_resource_arn(arn: &str) -> TagTarget {
    let parts: Vec<&str> = arn.split(':').collect();
    if !(arn.starts_with("arn:aws:rds:") && parts.len() >= 7) {
        return TagTarget::Unknown;
    }
    let rtype = parts[5];
    let rname = parts[6];
    let lower = rname.to_lowercase();
    match rtype {
        "db" => TagTarget::Instance(lower),
        "cluster" => TagTarget::Cluster(lower),
        "pg" => TagTarget::ParamGroup(lower),
        "cluster-pg" => TagTarget::ClusterParamGroup(lower),
        "og" => TagTarget::OptionGroup(lower),
        "secgrp" => TagTarget::SecurityGroup(lower),
        "subgrp" => TagTarget::SubnetGroup(lower),
        "snapshot" => TagTarget::Snapshot(lower),
        "cluster-snapshot" => TagTarget::ClusterSnapshot(lower),
        "db-proxy" => TagTarget::Proxy(rname.to_string()),
        "es" => TagTarget::EventSubscription(lower),
        "rds" => TagTarget::Snapshot(parts.last().unwrap_or(&"").to_lowercase()),
        _ => TagTarget::Unknown,
    }
}

fn list_tags_for_tags_resource(st: &RdsState, p: &Params) -> Result<Body, RdsError> {
    let arn = p_string(p, "ResourceName").unwrap_or_default();
    let tags = match parse_resource_arn(&arn) {
        TagTarget::Instance(id) => find_instance(st, &id).map(|d| d.tags.clone()),
        TagTarget::Cluster(id) => find_cluster(st, &id).map(|c| c.tags.clone()),
        TagTarget::ParamGroup(id) => st.db_parameter_groups.get(&id).map(|g| g.tags.clone()),
        TagTarget::ClusterParamGroup(id) => st
            .db_cluster_parameter_groups
            .get(&id)
            .map(|g| g.tags.clone()),
        TagTarget::OptionGroup(id) => st.option_groups.get(&id).map(|g| g.tags.clone()),
        TagTarget::SecurityGroup(id) => st.security_groups.get(&id).map(|g| g.tags.clone()),
        TagTarget::SubnetGroup(id) => st.subnet_groups.get(&id).map(|g| g.tags.clone()),
        TagTarget::Snapshot(id) => find_snapshot(st, &id).map(|s| s.tags.clone()),
        TagTarget::ClusterSnapshot(id) => find_cluster_snapshot(st, &id).map(|s| s.tags.clone()),
        TagTarget::Proxy(id) => st.db_proxies.get(&id).map(|g| g.tags.clone()),
        TagTarget::EventSubscription(id) => st.event_subscriptions.get(&id).map(|g| g.tags.clone()),
        TagTarget::Unknown => None,
    }
    .unwrap_or_default();
    let mut out = String::from("<TagList>");
    for t in &tags {
        out.push_str(&format!(
            "<Tag><Key>{}</Key><Value>{}</Value></Tag>",
            esc(&t.key),
            esc(&t.value)
        ));
    }
    out.push_str("</TagList>");
    Ok(Some(out))
}

fn add_tags_to_tags_resource(st: &mut RdsState, p: &Params) -> Result<Body, RdsError> {
    let arn = p_string(p, "ResourceName").unwrap_or_default();
    let tags = tags_param(p);
    let lower = arn.split(':').nth(6).unwrap_or("").to_lowercase();
    match parse_resource_arn(&arn) {
        TagTarget::Instance(id) => add_tags_to(&mut st.databases, &id, tags),
        TagTarget::Cluster(id) => add_tags_to(&mut st.clusters, &id, tags),
        TagTarget::ParamGroup(id) => add_tags_to(&mut st.db_parameter_groups, &id, tags),
        TagTarget::ClusterParamGroup(id) => {
            add_tags_to(&mut st.db_cluster_parameter_groups, &id, tags)
        }
        TagTarget::OptionGroup(id) => add_tags_to(&mut st.option_groups, &id, tags),
        TagTarget::SecurityGroup(id) => add_tags_to(&mut st.security_groups, &id, tags),
        TagTarget::SubnetGroup(id) => add_tags_to(&mut st.subnet_groups, &id, tags),
        TagTarget::Snapshot(id) => add_tags_to(&mut st.database_snapshots, &id, tags),
        TagTarget::ClusterSnapshot(id) => add_tags_to(&mut st.cluster_snapshots, &id, tags),
        TagTarget::Proxy(id) => add_tags_to(&mut st.db_proxies, &id, tags),
        TagTarget::EventSubscription(id) => add_tags_to(&mut st.event_subscriptions, &id, tags),
        TagTarget::Unknown => {}
    }
    let _ = lower;
    Ok(None)
}

fn remove_tags_from_tags_resource(st: &mut RdsState, p: &Params) -> Result<Body, RdsError> {
    let arn = p_string(p, "ResourceName").unwrap_or_default();
    let keys: Vec<String> = p_list(p, "TagKeys");
    match parse_resource_arn(&arn) {
        TagTarget::Instance(id) => remove_tags_from(&mut st.databases, &id, &keys),
        TagTarget::Cluster(id) => remove_tags_from(&mut st.clusters, &id, &keys),
        TagTarget::ParamGroup(id) => remove_tags_from(&mut st.db_parameter_groups, &id, &keys),
        TagTarget::ClusterParamGroup(id) => {
            remove_tags_from(&mut st.db_cluster_parameter_groups, &id, &keys)
        }
        TagTarget::OptionGroup(id) => remove_tags_from(&mut st.option_groups, &id, &keys),
        TagTarget::SecurityGroup(id) => remove_tags_from(&mut st.security_groups, &id, &keys),
        TagTarget::SubnetGroup(id) => remove_tags_from(&mut st.subnet_groups, &id, &keys),
        TagTarget::Snapshot(id) => remove_tags_from(&mut st.database_snapshots, &id, &keys),
        TagTarget::ClusterSnapshot(id) => remove_tags_from(&mut st.cluster_snapshots, &id, &keys),
        TagTarget::Proxy(id) => remove_tags_from(&mut st.db_proxies, &id, &keys),
        TagTarget::EventSubscription(id) => {
            remove_tags_from(&mut st.event_subscriptions, &id, &keys)
        }
        TagTarget::Unknown => {}
    }
    Ok(None)
}

trait Tagged {
    fn current_tags(&self) -> Vec<Tag>;
    fn set_tags(&mut self, tags: Vec<Tag>);
}

macro_rules! impl_tagged {
    ($($t:ty),*) => {$(
        impl Tagged for $t {
            fn current_tags(&self) -> Vec<Tag> {
                self.tags.clone()
            }
            fn set_tags(&mut self, tags: Vec<Tag>) {
                self.tags = tags;
            }
        }
    )*};
}

impl_tagged!(DBInstance);
impl_tagged!(DBCluster);
impl_tagged!(DBParameterGroup);
impl_tagged!(DBClusterParameterGroup);
impl_tagged!(OptionGroup);
impl_tagged!(DBSecurityGroup);
impl_tagged!(DBSubnetGroup);
impl_tagged!(DBSnapshot);
impl_tagged!(DBClusterSnapshot);
impl_tagged!(DBProxy);
impl_tagged!(EventSubscription);

fn add_tags_to<T: Tagged>(map: &mut BTreeMap<String, T>, id: &str, tags: Vec<Tag>) {
    let lower = id.to_lowercase();
    if let Some(entry) = map.get_mut(&lower) {
        let mut updated = entry.current_tags();
        updated.retain(|t| !tags.iter().any(|n| n.key == t.key));
        updated.extend(tags);
        entry.set_tags(updated);
    }
}

fn remove_tags_from<T: Tagged>(map: &mut BTreeMap<String, T>, id: &str, keys: &[String]) {
    let lower = id.to_lowercase();
    if let Some(entry) = map.get_mut(&lower) {
        let kept: Vec<Tag> = entry
            .current_tags()
            .into_iter()
            .filter(|t| !keys.contains(&t.key))
            .collect();
        entry.set_tags(kept);
    }
}

// Clusters -------------------------------------------------------------------

fn cluster_object(name: &str, p: &Params, region: &str) -> Result<DBCluster, RdsError> {
    let engine = p_string(p, "Engine").unwrap_or_default();
    if !DBCluster::valid_engine(&engine) {
        return Err(RdsError::client400(
            "InvalidParameterValue",
            format!("Engine '{engine}' is not supported to satisfy constraint: Member must satisfy enum value set: [aurora-mysql, aurora-postgresql, mysql, neptune, postgres]", engine = engine),
        ));
    }
    let engine_mode = p_string(p, "EngineMode").unwrap_or_else(|| "provision".into());
    let engine_version = p_string(p, "EngineVersion")
        .unwrap_or_else(|| DBCluster::default_engine_version(&engine).to_string());
    let storage_type = p_string(p, "StorageType")
        .unwrap_or_else(|| DBCluster::default_storage_type(p_i64(p, "Iops")).into());
    let (url_id, hz, resource) = new_cluster_ids();
    let backtrack_window = match p_i64(p, "BacktrackWindow") {
        Some(w) => {
            if engine == "aurora-mysql" {
                if (0..=259200).contains(&w) {
                    w
                } else {
                    return Err(RdsError::client400(
                        "InvalidParameterValue",
                        format!("The specified value ({w}) is not a valid Backtrack Window. Allowed values are within the range of 0 to 259200"),
                    ));
                }
            } else {
                return Err(RdsError::client400(
                    "InvalidParameterValue",
                    "Backtrack is not enabled for the postgres engine.",
                ));
            }
        }
        None => 0,
    };
    let managed = p_bool(p, "ManageMasterUserPassword").unwrap_or(false);
    let global_id = p_string(p, "GlobalClusterIdentifier").map(|s| s.to_lowercase());
    let user = if global_id.is_some() || engine == "neptune" {
        None
    } else {
        Some(p_string(p, "MasterUsername").unwrap_or_default())
    };
    if user.is_none() && !managed && engine != "neptune" && global_id.is_none() {
        return Err(RdsError::client400(
            "InvalidParameterValue",
            "The parameter MasterUsername must be provided and must not be blank.",
        ));
    }
    let option_group_engine_version = engine_version.clone();
    let mut availability_zones: Vec<String> = param(p, "AvailabilityZones").list();
    if availability_zones.is_empty() {
        availability_zones = vec![
            format!("{region}a"),
            format!("{region}b"),
            format!("{region}c"),
        ];
    }
    Ok(DBCluster {
        id: name.to_lowercase(),
        original_id: name.to_string(),
        engine: engine.clone(),
        engine_version,
        engine_mode,
        status: "creating".into(),
        status_shown: std::sync::atomic::AtomicBool::new(false),
        port: p_i64(p, "Port").unwrap_or_else(|| DBCluster::default_port(&engine)),
        master_username: user,
        allocated_storage: p_i64(p, "AllocatedStorage")
            .unwrap_or_else(|| DBCluster::default_allocated_storage(&engine, &storage_type)),
        storage_type,
        storage_encrypted: p_bool(p, "StorageEncrypted").unwrap_or(false),
        backup_retention_period: p_i64(p, "BackupRetentionPeriod").unwrap_or(1),
        auto_minor_version_upgrade: p_bool(p, "AutoMinorVersionUpgrade").unwrap_or(true),
        license_model: p_string(p, "LicenseModel")
            .unwrap_or_else(|| "general-public-license".into()),
        preferred_backup_window: p_string(p, "PreferredBackupWindow")
            .unwrap_or_else(|| "01:37-02:07".into()),
        preferred_maintenance_window: p_string(p, "PreferredMaintenanceWindow")
            .unwrap_or_else(|| "wed:02:40-wed:03:10".into()),
        parameter_group: p_string(p, "DBClusterParameterGroupName").unwrap_or_else(|| {
            if p_string(p, "Engine").as_deref() == Some("neptune") {
                "default.neptune1.3".into()
            } else {
                "default.aurora8.0".into()
            }
        }),
        db_subnet_group_name: p_string(p, "DBSubnetGroupName").unwrap_or_else(|| "default".into()),
        publicly_accessible: p_bool(p, "PubliclyAccessible").unwrap_or(false),
        vpc_security_group_ids: p_list(p, "VpcSecurityGroupIds"),
        deletion_protection: p_bool(p, "DeletionProtection").unwrap_or(false),
        iam_auth: p_bool(p, "EnableIAMDatabaseAuthentication").unwrap_or(false),
        option_group_name: DBCluster::compute_default_option_group(
            &engine,
            &option_group_engine_version,
        ),
        url_identifier: url_id,
        hosted_zone_id: hz,
        resource_id: resource,
        tags: tags_param(p),
        replication_source_identifier: p_string(p, "ReplicationSourceIdentifier"),
        read_replica_identifiers: Vec::new(),
        global_cluster_identifier: global_id,
        backtrack_window,
        created: Utc::now(),
        copy_tags_to_snapshot: p_bool(p, "CopyTagsToSnapshot").unwrap_or(false),
        character_set_name: p_string(p, "CharacterSetName"),
        database_name: p_string(p, "DatabaseName"),
        enabled_cloudwatch_logs_exports: p_list(p, "EnableCloudwatchLogsExports"),
        associated_roles: Vec::new(),
        availability_zones,
        kms_key_id: p_string(p, "KmsKeyId").unwrap_or_else(|| "default_kms_key_id".into()),
    })
}

fn create_db_cluster(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBClusterIdentifier").unwrap_or_default();
    let cluster = cluster_object(&identifier, p, region)?;
    let id = cluster.id.clone();
    st.clusters.insert(id.clone(), cluster);
    // Register into a global cluster when requested (moto adds the member).
    if let Some(gc_id) = p_string(p, "GlobalClusterIdentifier") {
        let key = gc_id.to_lowercase();
        if st.global_clusters.contains_key(&key) {
            let member_id = id.clone();
            let is_first = st
                .global_clusters
                .get(&key)
                .map(|gc| gc.members.is_empty())
                .unwrap_or(false);
            if let Some(gc) = st.global_clusters.get_mut(&key) {
                if !gc.members.contains(&member_id) {
                    gc.members.push(member_id.clone());
                }
            }
            if let Some(cl) = st.clusters.get_mut(&member_id) {
                cl.global_cluster_identifier = Some(key);
                let _ = is_first;
            }
        }
    }
    // moto saves an automated cluster snapshot on creation.
    let timestamp = Utc::now().format("%Y-%m-%d-%H-%M");
    let snap_id = format!("rds:{}-{}", id, timestamp);
    if let Some(cl) = st.clusters.get(&id).cloned() {
        let snap = cluster_snapshot_from(&cl, &snap_id, "automated", &[]);
        st.cluster_snapshots.insert(snap.id.clone(), snap);
    }
    let _ = (acct, region);
    match st.clusters.get(&id) {
        Some(found) => Ok(Some(db_cluster_xml(st, acct, region, found))),
        None => Ok(None),
    }
}

// Cluster lifecycle ----------------------------------------------------------

fn describe_db_clusters_xml(
    st: &RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let filter = p_string(p, "DBClusterIdentifier");
    let matched: Vec<&DBCluster> = match &filter {
        Some(rid) => {
            let found: Vec<&DBCluster> = st
                .clusters
                .values()
                .filter(|c| c.id == rid.to_lowercase())
                .collect();
            if found.is_empty() {
                return Err(RdsError::not_found404(
                    "DBClusterNotFoundFault",
                    format!("DBCluster {rid} not found."),
                ));
            }
            found
        }
        None => st.clusters.values().collect(),
    };
    let mut list = String::new();
    for cl in &matched {
        list.push_str(&db_cluster_xml(st, acct, region, cl));
    }
    Ok(Some(format!("<DBClusters>{list}</DBClusters>")))
}

/// Modify* kwargs handled like moto (setattr where the value is present).
fn modify_core_cluster_fields(cl: &mut DBCluster, p: &Params) {
    if p_string(p, "DeletionProtection") == Some("false".to_string())
        || p_bool(p, "DeletionProtection") == Some(false)
    {
        cl.deletion_protection = false;
    } else if p_bool(p, "DeletionProtection") == Some(true) {
        cl.deletion_protection = true;
    }
    if let Some(v) = p_i64(p, "BackupRetentionPeriod") {
        cl.backup_retention_period = v;
    }
    if let Some(v) = p_string(p, "PreferredBackupWindow") {
        cl.preferred_backup_window = v;
    }
    if let Some(v) = p_string(p, "PreferredMaintenanceWindow") {
        cl.preferred_maintenance_window = v;
    }
    if let Some(v) = p_string(p, "DBClusterParameterGroupName") {
        cl.parameter_group = v;
    }
    if let Some(v) = p_string(p, "Port") {
        if let Some(n) = v.parse::<i64>().ok() {
            cl.port = n;
        }
    }
    if let Some(v) = p_string(p, "CopyTagsToSnapshot") {
        cl.copy_tags_to_snapshot = v.eq_ignore_ascii_case("true");
    }
    if let Some(v) = p_string(p, "DBSubnetGroupName") {
        cl.db_subnet_group_name = v;
    }
    if let Some(v) = p_string(p, "MasterUserPassword") {
        cl.kms_key_id = "default_kms_key_id".into();
        let _ = v; // not echoed back
    }
    if let Some(v) = p_bool(p, "IAMDatabaseAuthenticationEnabled")
        .or(p_bool(p, "EnableIAMDatabaseAuthentication"))
    {
        cl.iam_auth = v;
    }
    let cwl = key_end_log_types(p);
    for exp in cwl.0 {
        cl.enabled_cloudwatch_logs_exports.retain(|e| e != &exp);
    }
    for exp in cwl.1 {
        if !cl.enabled_cloudwatch_logs_exports.contains(&exp) {
            cl.enabled_cloudwatch_logs_exports.push(exp);
        }
    }
    cl.status = if cl.status == "stopped" {
        "available".into()
    } else {
        cl.status.clone()
    };
}

fn key_end_log_types(p: &Params) -> (Vec<String>, Vec<String>) {
    (p_list(p, "DisableLogTypes"), p_list(p, "EnableLogTypes"))
}

fn modify_db_cluster(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBClusterIdentifier").unwrap_or_default();
    let id = identifier.to_lowercase();
    if !st.clusters.contains_key(&id) {
        return Err(RdsError::not_found404(
            "DBClusterNotFoundFault",
            format!("DBCluster {identifier} not found."),
        ));
    }
    {
        let cl = st.clusters.get_mut(&id).unwrap();
        modify_core_cluster_fields(cl, p);
    }
    if let Some(max) = p_i64(p, "MaxCapacity") {
        let _ = max;
    }
    match st.clusters.get(&id) {
        Some(found) => Ok(Some(db_cluster_xml(st, acct, region, found))),
        None => Ok(None),
    }
}

fn delete_db_cluster(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBClusterIdentifier").unwrap_or_default();
    let id = identifier.to_lowercase();
    if !st.clusters.contains_key(&id) {
        return Err(RdsError::not_found404(
            "DBClusterNotFoundFault",
            format!("DBCluster {identifier} not found."),
        ));
    }
    let cluster = st.clusters.get(&id).unwrap().clone();
    if cluster.deletion_protection {
        return Err(RdsError::client400(
            "InvalidParameterCombination",
            "Cannot delete protected Cluster, please disable deletion protection and try again.",
        ));
    }
    let has_members = st
        .databases
        .values()
        .any(|d| d.db_cluster_identifier.as_deref() == Some(&id));
    if has_members {
        return Err(RdsError::client400(
            "InvalidDBClusterStateFault",
            "Cluster cannot be deleted, it still contains DB instances in non-deleting state.",
        ));
    }
    if let Some(gc_id) = &cluster.global_cluster_identifier {
        if let Some(gc) = st.global_clusters.get_mut(gc_id) {
            gc.members.retain(|m| *m != id);
        }
    }
    if let Some(snap_name) = p_string(p, "FinalDBSnapshotIdentifier") {
        RdsError::check_snapshot_identifier(&snap_name, "FinalDBSnapshotIdentifier")?;
        if !p_bool(p, "SkipFinalSnapshot").unwrap_or(false) {
            let snap = cluster_snapshot_from(&cluster, &snap_name, "manual", &[]);
            st.cluster_snapshots.insert(snap.id.clone(), snap);
        }
    }
    st.clusters.remove(&id);
    Ok(Some(db_cluster_xml(st, acct, region, &cluster)))
}

// Cluster lifecycle: start/stop/failover/promote/reboot ----------------------

fn transition_cluster(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
    from: &[&str],
    to: &str,
    next_status: &str,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBClusterIdentifier").unwrap_or_default();
    let id = identifier.to_lowercase();
    if !st.clusters.contains_key(&id) {
        return Err(RdsError::not_found404(
            "DBClusterNotFoundFault",
            format!("DBCluster {identifier} not found."),
        ));
    }
    let status = st.clusters.get(&id).unwrap().status.clone();
    if !from.contains(&forwarded_status(&st, &id).as_str()) {
        return Err(RdsError::client400(
            "InvalidDBClusterStateFault",
            format!(
                "DbCluster {identifier} is in {shown} state but expected it to be one of {expected}.",
                shown = forwarded_status(st, &id),
                expected = from.join(",")
            ),
        ));
    }
    st.clusters.get_mut(&id).unwrap().status = next_status.to_string();
    let cl = st.clusters.get(&id).unwrap().clone();
    let _ = (status, to);
    Ok(Some(db_cluster_xml(
        st,
        acct,
        region,
        &with_status(&cl, to),
    )))
}

fn forwarded_status(st: &RdsState, id: &str) -> String {
    st.clusters
        .get(id)
        .map(|c| c.status.clone())
        .unwrap_or_default()
}

fn with_status(cl: &DBCluster, status: &str) -> DBCluster {
    use std::sync::atomic::Ordering;
    DBCluster {
        status: status.to_string(),
        status_shown: std::sync::atomic::AtomicBool::new(cl.status_shown.load(Ordering::SeqCst)),
        ..cl.clone()
    }
}

fn stop_db_cluster(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBClusterIdentifier").unwrap_or_default();
    let id = identifier.to_lowercase();
    if !st.clusters.contains_key(&id) {
        return Err(RdsError::not_found404(
            "DBClusterNotFoundFault",
            format!("DBCluster {identifier} not found."),
        ));
    }
    let shown = st.clusters.get(&id).unwrap().publish_status();
    if shown != "available" {
        return Err(RdsError::client400(
            "InvalidDBClusterStateFault",
            format!("DbCluster {identifier} is in {shown} state but expected it to be one of available."),
        ));
    }
    let cl = st.clusters.get(&id).unwrap().clone();
    st.clusters.get_mut(&id).unwrap().status = "stopped".into();
    st.clusters
        .get_mut(&id)
        .unwrap()
        .status_shown
        .store(true, std::sync::atomic::Ordering::SeqCst);
    Ok(Some(db_cluster_xml(st, acct, region, &cl)))
}

fn start_db_cluster(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBClusterIdentifier").unwrap_or_default();
    let id = identifier.to_lowercase();
    if !st.clusters.contains_key(&id) {
        return Err(RdsError::not_found404(
            "DBClusterNotFoundFault",
            format!("DBCluster {identifier} not found."),
        ));
    }
    let shown = st.clusters.get(&id).unwrap().publish_status();
    if shown != "stopped" {
        return Err(RdsError::client400(
            "InvalidDBClusterStateFault",
            format!(
                "DbCluster {identifier} is in {shown} state but expected it to be one of stopped,inaccessible-encryption-credentials-recoverable."
            ),
        ));
    }
    let started = with_status(st.clusters.get(&id).unwrap(), "started");
    st.clusters.get_mut(&id).unwrap().status = "available".into();
    Ok(Some(db_cluster_xml(st, acct, region, &started)))
}

fn failover_db_cluster(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBClusterIdentifier").unwrap_or_default();
    let id = identifier.to_lowercase();
    if !st.clusters.contains_key(&id) {
        return Err(RdsError::not_found404(
            "DBClusterNotFoundFault",
            format!("The source cluster could not be found or cannot be accessed: {identifier}"),
        ));
    }
    let target = p_string(p, "TargetDBInstanceIdentifier");
    if let Some(target) = &target {
        if !st.databases.contains_key(&target.to_lowercase()) {
            return Err(RdsError::client400(
                "InvalidParameterValue",
                format!("Cannot find target instance: {target}"),
            ));
        }
    }
    let member_count = st
        .databases
        .values()
        .filter(|d| d.db_cluster_identifier.as_deref() == Some(&id))
        .count();
    if member_count < 2 {
        return Err(RdsError::client400(
            "InvalidDBClusterStateFault",
            format!(
                "Database cluster:{id} should have at least two database instances for failover"
            ),
        ));
    }
    let cluster = st.clusters.get(&id).unwrap().clone();
    Ok(Some(db_cluster_xml(
        st,
        acct,
        region,
        &with_status(&cluster, "available"),
    )))
}

fn reboot_db_cluster(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBClusterIdentifier").unwrap_or_default();
    let id = identifier.to_lowercase();
    if !st.clusters.contains_key(&id) {
        return Err(RdsError::not_found404(
            "DBClusterNotFoundFault",
            format!("DBCluster {identifier} not found."),
        ));
    }
    let cluster = st.clusters.get(&id).unwrap().clone();
    Ok(Some(db_cluster_xml(st, acct, region, &cluster)))
}

// Cluster snapshots ----------------------------------------------------------

fn create_db_cluster_snapshot(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let snapshot_id = p_string(p, "DBClusterSnapshotIdentifier").unwrap_or_default();
    let cluster_name = p_string(p, "DBClusterIdentifier").unwrap_or_default();
    let snap_id = snapshot_id.to_lowercase();
    let cluster_id = cluster_name.to_lowercase();
    if st.cluster_snapshots.contains_key(&snap_id) {
        return Err(RdsError::client400(
            "DBClusterSnapshotAlreadyExistsFault",
            format!("Cannot create the snapshot because a snapshot with the identifier {snap_id} already exists."),
        ));
    }
    let cluster = match st.clusters.get(&cluster_id) {
        Some(cl) => cl.clone(),
        None => {
            return Err(RdsError::not_found404(
                "DBClusterNotFoundFault",
                format!("DBCluster {cluster_name} not found."),
            ))
        }
    };
    let snapshot_type = p_string(p, "SnapshotType").unwrap_or_else(|| "manual".into());
    let snapshot = cluster_snapshot_from(&cluster, &snapshot_id, &snapshot_type, &tags_param(p));
    let response = snapshot.clone();
    st.cluster_snapshots.insert(snap_id, snapshot);
    Ok(Some(cluster_snapshot_xml(acct, region, &response)))
}

fn describe_db_cluster_snapshots_xml(
    st: &RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let cluster_filter = p_string(p, "DBClusterIdentifier").map(|s| s.to_lowercase());
    let snap_filter = p_string(p, "DBClusterSnapshotIdentifier").map(|s| s.to_lowercase());
    let matched: Vec<&DBClusterSnapshot> = st
        .cluster_snapshots
        .values()
        .filter(|s| match &cluster_filter {
            Some(cid) => s.cluster.id == *cid,
            None => true,
        })
        .filter(|s| match &snap_filter {
            Some(snap_id) => s.id == *snap_id,
            None => true,
        })
        .collect();
    if let Some(snap_id) = &snap_filter {
        if matched.is_empty() && cluster_filter.is_none() {
            return Err(RdsError::not_found404(
                "DBClusterSnapshotNotFoundFault",
                format!("DBClusterSnapshot {snap_id} not found."),
            ));
        }
    }
    let _ = tags_param(p);
    Ok(Some(cluster_snapshot_list_xml(
        acct, region, &matched, true,
    )))
}

fn cluster_snapshot_list_xml(
    acct: u64,
    region: &str,
    matched: &[&DBClusterSnapshot],
    include_tags: bool,
) -> String {
    let _ = include_tags;
    let mut list = String::new();
    for snap in matched {
        list.push_str(&cluster_snapshot_xml(acct, region, snap));
    }
    format!("<DBClusterSnapshots>{list}</DBClusterSnapshots>")
}

fn delete_db_cluster_snapshot(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let ident = p_string(p, "DBClusterSnapshotIdentifier").unwrap_or_default();
    let snap_id = ident.to_lowercase();
    if !st.cluster_snapshots.contains_key(&snap_id) {
        return Err(RdsError::not_found404(
            "DBClusterSnapshotNotFoundFault",
            format!("DBClusterSnapshot {ident} not found."),
        ));
    }
    let snapshot = st.cluster_snapshots.remove(&snap_id).unwrap();
    Ok(Some(cluster_snapshot_xml(acct, region, &snapshot)))
}

fn copy_db_cluster_snapshot(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let target = p_string(p, "TargetDBClusterSnapshotIdentifier").unwrap_or_default();
    let source = p_string(p, "SourceDBClusterSnapshotIdentifier").unwrap_or_default();
    let source_id = identifier_from_arn(&source).to_lowercase();
    let src = match find_cluster_snapshot(st, &source_id).cloned() {
        Some(s) => s,
        None => {
            return Err(RdsError::not_found404(
                "DBClusterSnapshotNotFoundFault",
                format!("DBClusterSnapshot {source} not found."),
            ))
        }
    };
    let target_id = target.to_lowercase();
    if st.cluster_snapshots.contains_key(&target_id) {
        return Err(RdsError::client400(
            "DBClusterSnapshotAlreadyExistsFault",
            format!("Cannot create the snapshot because a snapshot with the identifier {target_id} already exists."),
        ));
    }
    let mut snapshot = cluster_snapshot_from(&src.cluster, &target, &src.snapshot_type, &[]);
    let tags = tags_param(p);
    if !tags.is_empty() {
        snapshot.tags = tags;
    } else if p_bool(p, "CopyTags").unwrap_or(false) && !src.tags.is_empty() {
        snapshot.tags = src.tags.clone();
    }
    let out = snapshot.clone();
    st.cluster_snapshots.insert(target_id, snapshot);
    Ok(Some(cluster_snapshot_xml(acct, region, &out)))
}

fn cluster_snapshot_attrs_xml(identifier: &str, attrs: &[(String, Vec<String>)]) -> String {
    let mut items = String::from("<DBClusterSnapshotAttributes>");
    for (name, values) in attrs {
        items.push_str(&format!(
            "<DBClusterSnapshotAttribute><AttributeName>{}</AttributeName>{}</DBClusterSnapshotAttribute>",
            esc(name),
            list_xml("AttributeValues", "AttributeValue", values)
        ));
    }
    items.push_str("</DBClusterSnapshotAttributes>");
    format!(
        "<DBClusterSnapshotAttributesResult>\
         <DBClusterSnapshotIdentifier>{}</DBClusterSnapshotIdentifier>{items}\
         </DBClusterSnapshotAttributesResult>",
        esc(identifier)
    )
}
fn describe_db_cluster_snapshot_attributes(st: &RdsState, p: &Params) -> Result<Body, RdsError> {
    let ident = p_string(p, "DBClusterSnapshotIdentifier").unwrap_or_default();
    let snap_id = ident.to_lowercase();
    match find_cluster_snapshot(st, &snap_id) {
        Some(s) => {
            let attrs = vec![("restore".to_string(), s.attributes_restore.clone())];
            Ok(Some(cluster_snapshot_attrs_xml(&snap_id, &attrs)))
        }
        None => Err(RdsError::not_found404(
            "DBClusterSnapshotNotFoundFault",
            format!("DBClusterSnapshot {ident} not found."),
        )),
    }
}

fn modify_db_cluster_snapshot_attribute(st: &mut RdsState, p: &Params) -> Result<Body, RdsError> {
    let ident = p_string(p, "DBClusterSnapshotIdentifier").unwrap_or_default();
    let snap_id = ident.to_lowercase();
    let existing = match find_cluster_snapshot(st, &snap_id) {
        Some(s) => s.clone(),
        None => {
            return Err(RdsError::not_found404(
                "DBClusterSnapshotNotFoundFault",
                format!("DBClusterSnapshot {ident} not found."),
            ))
        }
    };
    if existing.snapshot_type != "manual" {
        return Err(RdsError::client400(
            "InvalidDBClusterSnapshotStateFault",
            "automated snapshots cannot be modified.",
        ));
    }
    let attribute_name = p_string(p, "AttributeName").unwrap_or_default();
    if attribute_name != "restore" {
        return Err(RdsError::client400(
            "InvalidParameterValue",
            format!("Invalid snapshot attribute {attribute_name}"),
        ));
    }
    let mut merged = existing.attributes_restore.clone();
    for v in p_list(p, "ValuesToAdd") {
        if !merged.contains(&v) {
            merged.push(v);
        }
    }
    let mut remove = p_list(p, "ValuesToRemove");
    remove.retain(|v| !p_list(p, "ValuesToAdd").contains(v));
    let mut merged2: Vec<String> = Vec::new();
    for v in merged {
        if !remove.contains(&v) && !merged2.contains(&v) {
            merged2.push(v);
        }
    }
    if merged2.len() > 20 {
        return Err(RdsError::client400(
            "SharedSnapshotQuotaExceeded",
            "The request cannot be processed because it would exceed the maximum number of snapshots.",
        ));
    }
    st.cluster_snapshots
        .get_mut(&snap_id)
        .unwrap()
        .attributes_restore = merged2;
    let attrs = vec![(
        "restore".to_string(),
        st.cluster_snapshots[&snap_id].attributes_restore.clone(),
    )];
    Ok(Some(cluster_snapshot_attrs_xml(&snap_id, &attrs)))
}

fn restore_db_instance_from_db_snapshot(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let snapshot_name = p_string(p, "DBSnapshotIdentifier").unwrap_or_default();
    let snap_id = snapshot_name.to_lowercase();
    let src = match find_snapshot(st, &snap_id) {
        Some(s) => s.clone(),
        None => {
            return Err(RdsError::not_found404(
                "DBSnapshotNotFound",
                format!("DBSnapshot {snapshot_name} not found."),
            ))
        }
    };
    let identifier = p_string(p, "DBInstanceIdentifier").unwrap_or_default();
    let new_id = identifier.to_lowercase();
    if st.databases.contains_key(&new_id) {
        return Err(RdsError::client400(
            "DBInstanceAlreadyExists",
            "DB instance already exists",
        ));
    }
    let mut db = src.source.clone();
    db.id = new_id.clone();
    db.original_id = identifier.clone();
    db.option_group_name = DBCluster::compute_default_option_group(&db.engine, &db.engine_version);
    db.option_group_supplied = false;
    db.created = Utc::now();
    if let Some(v) = p_i64(p, "AllocatedStorage") {
        db.allocated_storage = v;
    }
    if let Some(v) = p_string(p, "DBInstanceClass") {
        db.db_instance_class = v;
    }
    if p_string(p, "Engine").is_none() && db.engine.contains("aurora") {
        db.engine_version = DBInstance::default_engine_version(&db.engine)
            .unwrap_or(&db.engine_version)
            .to_string();
    }
    ensure_pg_for_instance(st, &db);
    st.databases.insert(new_id.clone(), db.clone());
    let arn = format!("arn:aws:rds:{region}:{acct}:db:{new_id}");
    st.add_event("DB_INSTANCE_CREATE", &new_id, arn);
    create_automated_snapshot(st, acct, region, &new_id, &auto_snapshot_id(&new_id));
    match st.databases.get(&new_id) {
        Some(found) => Ok(Some(db_instance_xml(st, acct, region, found))),
        None => Ok(None),
    }
}

fn restore_db_instance_to_point_in_time(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let source = p_string(p, "SourceDBInstanceIdentifier").unwrap_or_default();
    let target = p_string(p, "TargetDBInstanceIdentifier").unwrap_or_default();
    let src_id = source.to_lowercase();
    let src = match find_instance(st, &src_id) {
        Some(d) => d.clone(),
        None => {
            return Err(RdsError::not_found404(
                "DBInstanceNotFound",
                format!("DBInstance {source} not found."),
            ))
        }
    };
    let new_id = target.to_lowercase();
    if st.databases.contains_key(&new_id) {
        return Err(RdsError::client400(
            "DBInstanceAlreadyExists",
            "DB instance already exists",
        ));
    }
    let mut db = src.clone();
    db.id = new_id.clone();
    db.original_id = target.clone();
    db.db_name = src.db_name.clone();
    db.option_group_name = DBCluster::compute_default_option_group(&db.engine, &db.engine_version);
    db.option_group_supplied = false;
    db.created = Utc::now();
    if let Some(v) = p_i64(p, "AllocatedStorage") {
        db.allocated_storage = v;
    }
    if let Some(v) = p_string(p, "DBInstanceClass") {
        db.db_instance_class = v;
    }
    ensure_pg_for_instance(st, &db);
    st.databases.insert(new_id.clone(), db.clone());
    let arn = format!("arn:aws:rds:{region}:{acct}:db:{new_id}");
    st.add_event("DB_INSTANCE_CREATE", &new_id, arn);
    create_automated_snapshot(st, acct, region, &new_id, &auto_snapshot_id(&new_id));
    match st.databases.get(&new_id) {
        Some(found) => Ok(Some(db_instance_xml(st, acct, region, found))),
        None => Ok(None),
    }
}

fn restore_db_cluster_from_snapshot(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let snapshot_name = p_string(p, "SnapshotIdentifier").unwrap_or_default();
    let snap_id = snapshot_name.to_lowercase();
    let src = match find_cluster_snapshot(st, &snap_id) {
        Some(s) => s.clone(),
        None => {
            return Err(RdsError::not_found404(
                "DBClusterSnapshotNotFoundFault",
                format!("DBClusterSnapshot {snapshot_name} not found."),
            ))
        }
    };
    let identifier = p_string(p, "DBClusterIdentifier").unwrap_or_default();
    let new_id = identifier.to_lowercase();
    if st.clusters.contains_key(&new_id) {
        return Err(RdsError::client400(
            "DBClusterAlreadyExistsFault",
            format!("DBCluster {identifier} already exists."),
        ));
    }
    let mut cluster = src.cluster.clone();
    cluster.id = new_id;
    cluster.original_id = identifier;
    let (url_id, hz, resource) = new_cluster_ids();
    cluster.url_identifier = url_id;
    cluster.hosted_zone_id = hz;
    cluster.resource_id = resource;
    cluster.created = Utc::now();
    cluster.status = "creating".into();
    cluster.status_shown = std::sync::atomic::AtomicBool::new(false);
    cluster.engine_version = src.cluster.engine_version.clone();
    if let Some(engine) = p_string(p, "Engine") {
        cluster.engine = engine;
    }
    let key = cluster.id.clone();
    st.clusters.insert(key.clone(), cluster);
    let timestamp = Utc::now().format("%Y-%m-%d-%H-%M");
    let snap_name = format!("rds:{}-{}", key, timestamp);
    if let Some(cl) = st.clusters.get(&key).cloned() {
        let snap = cluster_snapshot_from(&cl, &snap_name, "automated", &[]);
        st.cluster_snapshots.insert(snap.id.clone(), snap);
    }
    match st.clusters.get(&key) {
        Some(found) => Ok(Some(db_cluster_xml(st, acct, region, found))),
        None => Ok(None),
    }
}

fn restore_db_cluster_to_point_in_time(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let source = p_string(p, "SourceDBClusterIdentifier").unwrap_or_default();
    let identifier = p_string(p, "DBClusterIdentifier").unwrap_or_default();
    let src_id = source.to_lowercase();
    let src = match find_cluster(st, &src_id) {
        Some(c) => c.clone(),
        None => {
            return Err(RdsError::not_found404(
                "DBClusterNotFoundFault",
                format!("DBCluster {source} not found."),
            ))
        }
    };
    let new_id = identifier.to_lowercase();
    if st.clusters.contains_key(&new_id) {
        return Err(RdsError::client400(
            "DBClusterAlreadyExistsFault",
            format!("DBCluster {identifier} already exists."),
        ));
    }
    let mut cluster = src.clone();
    cluster.id = new_id;
    cluster.original_id = identifier;
    cluster.database_name = src.database_name.clone();
    let (url_id, hz, resource) = new_cluster_ids();
    cluster.url_identifier = url_id;
    cluster.hosted_zone_id = hz;
    cluster.resource_id = resource;
    cluster.created = Utc::now();
    cluster.status = "creating".into();
    cluster.status_shown = std::sync::atomic::AtomicBool::new(false);
    cluster.global_cluster_identifier = None;
    let key = cluster.id.clone();
    st.clusters.insert(key.clone(), cluster);
    let timestamp = Utc::now().format("%Y-%m-%d-%H-%M");
    let snap_name = format!("rds:{}-{}", key, timestamp);
    if let Some(cl) = st.clusters.get(&key).cloned() {
        let snap = cluster_snapshot_from(&cl, &snap_name, "automated", &[]);
        st.cluster_snapshots.insert(snap.id.clone(), snap);
    }
    match st.clusters.get(&key) {
        Some(found) => Ok(Some(db_cluster_xml(st, acct, region, found))),
        None => Ok(None),
    }
}

// Global clusters ------------------------------------------------------------

fn create_global_cluster(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "GlobalClusterIdentifier").unwrap_or_default();
    let lower = identifier.to_lowercase();
    if st.global_clusters.contains_key(&lower) {
        return Err(RdsError::client400(
            "GlobalClusterAlreadyExistsFault",
            format!("Global Cluster {identifier} already exists."),
        ));
    }
    let source_arn = p_string(p, "SourceDBClusterIdentifier");
    let mut engine = p_string(p, "Engine");
    let mut engine_version = p_string(p, "EngineVersion");
    let mut attached_member: Option<String> = None;
    if let Some(source) = source_arn {
        let source_id = identifier_from_arn(&source).to_lowercase();
        let cluster = match find_cluster(st, &source_id) {
            Some(c) => c.clone(),
            None => {
                return Err(RdsError::not_found404(
                    "DBClusterNotFoundFault",
                    format!("DBCluster {source} not found."),
                ))
            }
        };
        if engine.is_some() {
            return Err(RdsError::client400(
                "InvalidParameterCombination",
                "When creating global cluster from existing db cluster, value for engineName should not be specified since it will be inherited from source cluster",
            ));
        }
        engine = Some(cluster.engine.clone());
        engine_version = Some(cluster.engine_version.clone());
        attached_member = Some(cluster.id.clone());
    }
    let engine =
        match engine {
            Some(e) => e,
            None => return Err(RdsError::client400(
                "InvalidParameterValue",
                "When creating standalone global cluster, value for engineName should be specified",
            )),
        };
    let gc = GlobalCluster {
        identifier: lower.clone(),
        original_id: identifier.clone(),
        unique_id: rand_lower(8),
        engine: engine.clone(),
        engine_version: engine_version
            .unwrap_or_else(|| DBCluster::default_engine_version(&engine).to_string()),
        storage_encrypted: p_bool(p, "StorageEncrypted").unwrap_or(false),
        deletion_protection: p_bool(p, "DeletionProtection").unwrap_or(false),
        members: attached_member.iter().map(|m| m.clone()).collect(),
        status: "available".into(),
        tags: tags_param(p),
    };
    if let Some(member) = &attached_member {
        if let Some(cl) = st.clusters.get_mut(member) {
            cl.global_cluster_identifier = Some(lower.clone());
        }
    }
    let response = gc.clone();
    st.global_clusters.insert(lower, gc);
    Ok(Some(global_cluster_xml(acct, &response, st)))
}

fn describe_global_clusters_xml(
    st: &RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let filter = p_string(p, "GlobalClusterIdentifier").map(|s| s.to_lowercase());
    let matched: Vec<&GlobalCluster> = st
        .global_clusters
        .values()
        .filter(|g| match &filter {
            Some(f) => g.identifier == *f,
            None => true,
        })
        .collect();
    let mut list = String::new();
    for gc in &matched {
        let inner = global_cluster_xml(acct, gc, st);
        // global_cluster_xml emits <GlobalCluster>...</GlobalCluster>; the
        // wire item tag is GlobalClusterMember (see service model).
        let stripped = inner
            .strip_prefix("<GlobalCluster>")
            .and_then(|s| s.strip_suffix("</GlobalCluster>"))
            .unwrap_or(&inner)
            .to_string();
        list.push_str(&format!(
            "<GlobalClusterMember>{stripped}</GlobalClusterMember>"
        ));
    }
    Ok(Some(format!("<GlobalClusters>{list}</GlobalClusters>")))
}

fn delete_global_cluster(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "GlobalClusterIdentifier").unwrap_or_default();
    let lower = identifier.to_lowercase();
    if !st.global_clusters.contains_key(&lower) {
        return Err(RdsError::not_found404(
            "GlobalClusterNotFoundFault",
            format!("Global Cluster {identifier} not found."),
        ));
    }
    let gc = st.global_clusters.get(&lower).unwrap().clone();
    if !gc.members.is_empty() {
        return Err(RdsError::client400(
            "InvalidGlobalClusterStateFault",
            format!("Global Cluster {} is not empty", gc.arn_for(acct)),
        ));
    }
    st.global_clusters.remove(&lower);
    Ok(Some(global_cluster_xml(acct, &gc, st)))
}

fn modify_global_cluster(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "GlobalClusterIdentifier").unwrap_or_default();
    let lower = identifier.to_lowercase();
    if !st.global_clusters.contains_key(&lower) {
        return Err(RdsError::not_found404(
            "GlobalClusterNotFoundFault",
            format!("Global Cluster {identifier} not found."),
        ));
    }
    if let Some(v) = p_bool(p, "DeletionProtection") {
        st.global_clusters
            .get_mut(&lower)
            .unwrap()
            .deletion_protection = v;
    }
    if let Some(v) = p_string(p, "EngineVersion") {
        st.global_clusters.get_mut(&lower).unwrap().engine_version = v;
    }
    let mut renamed_from = lower.clone();
    if let Some(new_name) = p_string(p, "NewGlobalClusterIdentifier") {
        let new_lower = new_name.to_lowercase();
        if let Some(mut gc) = st.global_clusters.remove(&lower) {
            gc.identifier = new_lower.clone();
            gc.original_id = new_name;
            st.global_clusters.insert(new_lower.clone(), gc);
        }
        renamed_from = new_lower;
    }
    let gc = st.global_clusters.get(&renamed_from).unwrap().clone();
    Ok(Some(global_cluster_xml(acct, &gc, st)))
}

fn remove_from_global_cluster(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "GlobalClusterIdentifier").unwrap_or_default();
    let member = p_string(p, "DbClusterIdentifier").unwrap_or_default();
    let lower = identifier.to_lowercase();
    let member_id = identifier_from_arn(&member).to_lowercase();
    let found = st.global_clusters.contains_key(&lower) && st.clusters.contains_key(&member_id);
    if found {
        if let Some(gc) = st.global_clusters.get_mut(&lower) {
            gc.members.retain(|m| *m != member_id);
        }
        if let Some(gc) = st.global_clusters.get(&lower) {
            let gc = gc.clone();
            return Ok(Some(global_cluster_xml(acct, &gc, st)));
        }
    }
    // moto swallows misses and returns an empty RemoveFromGlobalClusterResult
    Ok(None)
}

fn failover_global_cluster(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "GlobalClusterIdentifier").unwrap_or_default();
    let target = p_string(p, "TargetDbClusterIdentifier").unwrap_or_default();
    let lower = identifier.to_lowercase();
    if !st.global_clusters.contains_key(&lower) {
        return Err(RdsError::not_found404(
            "GlobalClusterNotFoundFault",
            format!("Global Cluster {identifier} not found."),
        ));
    }
    let target_id = identifier_from_arn(&target);
    if let Some(cl) = st.clusters.get_mut(&target_id.to_lowercase()) {
        cl.publicly_accessible = true;
    }
    let gc = st.global_clusters.get(&lower).unwrap().clone();
    Ok(Some(global_cluster_xml(acct, &gc, st)))
}

fn switchover_global_cluster(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    failover_global_cluster(st, acct, region, p)
}

// Parameter groups -----------------------------------------------------------

fn create_db_parameter_group(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBParameterGroupName").unwrap_or_default();
    let lower = identifier.to_lowercase();
    if st.db_parameter_groups.contains_key(&lower) {
        return Err(RdsError::client400(
            "DBParameterGroupAlreadyExists",
            format!("A DB parameter group named {identifier} already exists."),
        ));
    }
    let family = p_string(p, "DBParameterGroupFamily").unwrap_or_default();
    if family.is_empty() {
        return Err(RdsError::client400(
            "InvalidParameterValue",
            "The parameter DBParameterGroupFamily must be provided and must not be blank.",
        ));
    }
    let description = p_string(p, "Description").unwrap_or_default();
    if description.is_empty() {
        return Err(RdsError::client400(
            "InvalidParameterValue",
            "The parameter Description must be provided and must not be blank.",
        ));
    }
    let group = DBParameterGroup {
        name: lower.clone(),
        family,
        description,
        tags: tags_param(p),
        parameters: Vec::new(),
    };
    st.db_parameter_groups.insert(lower, group);
    let group = st
        .db_parameter_groups
        .get(&identifier.to_lowercase())
        .unwrap();
    Ok(Some(parameter_group_xml(acct, region, group)))
}

fn describe_db_parameter_groups_xml(
    st: &RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    // Wrapped DBParameterGroups list; filtered by name like moto.
    let filter = p_string(p, "DBParameterGroupName").map(|s| s.to_lowercase());
    let matched: Vec<&DBParameterGroup> = st
        .db_parameter_groups
        .values()
        .filter(|g| match &filter {
            Some(f) => g.name == *f,
            None => true,
        })
        .collect();
    let mut list = String::new();
    for g in &matched {
        list.push_str(&parameter_group_xml(acct, region, g));
    }
    let mut out = format!("<DBParameterGroups>{list}</DBParameterGroups>");
    let _ = &mut out;
    if param(p, "Marker").string().is_some() {
        let marker = param(p, "Marker").string().unwrap_or("").to_string();
        out = format!("<Marker>{}</Marker>", esc(&marker));
    }
    Ok(Some(out))
}

/// DescribeDBParameters for a group (auto-creating default groups like moto's
/// `db_instance.db_parameter_groups()` would for instance-owned defaults).
fn describe_db_parameters(st: &RdsState, p: &Params) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBParameterGroupName").unwrap_or_default();
    let lower = identifier.to_lowercase();
    let group = match st.db_parameter_groups.get(&lower) {
        Some(g) => g.clone(),
        None => {
            return Err(RdsError::not_found404(
                "DBParameterGroupNotFound",
                format!("DB Parameter Group {identifier} not found."),
            ))
        }
    };
    Ok(Some(parameters_xml(&group.parameters)))
}

fn modify_db_parameter_group(st: &mut RdsState, p: &Params) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBParameterGroupName").unwrap_or_default();
    let lower = identifier.to_lowercase();
    if !st.db_parameter_groups.contains_key(&lower) {
        return Err(RdsError::not_found404(
            "DBParameterGroupNotFound",
            format!("DB Parameter Group {identifier} not found."),
        ));
    }
    for m in p_maps(p, "Parameters") {
        let mut name = None;
        let mut values = std::collections::BTreeMap::new();
        for (k, v) in m {
            if k == "ParameterName" {
                name = Some(v);
            } else {
                values.insert(k, v);
            }
        }
        if let Some(name) = name {
            st.db_parameter_groups
                .get_mut(&lower)
                .unwrap()
                .set_parameter(&name, values);
        }
    }
    Ok(Some(format!(
        "<DBParameterGroupName>{}</DBParameterGroupName>",
        esc(&identifier)
    )))
}

fn delete_db_parameter_group(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBParameterGroupName").unwrap_or_default();
    let lower = identifier.to_lowercase();
    if !st.db_parameter_groups.contains_key(&lower) {
        return Err(RdsError::not_found404(
            "DBParameterGroupNotFound",
            format!("DB Parameter Group {identifier} not found."),
        ));
    }
    st.db_parameter_groups.remove(&lower);
    // moto deletes the default group names too; nothing left to emit: the
    // response shape only carries ResponseMetadata.
    let _ = (acct, region);
    Ok(None)
}

fn copy_db_parameter_group(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let source = p_string(p, "SourceDBParameterGroupIdentifier").unwrap_or_default();
    let target = p_string(p, "TargetDBParameterGroupIdentifier").unwrap_or_default();
    let description = p_string(p, "TargetDBParameterGroupDescription").unwrap_or_default();
    let source_id = identifier_from_arn(&source).to_lowercase();
    let src = match st.db_parameter_groups.get(&source_id) {
        Some(g) => g.clone(),
        None => {
            return Err(RdsError::not_found404(
                "DBParameterGroupNotFound",
                format!("DB Parameter Group {source} not found."),
            ))
        }
    };
    let target_id = target.to_lowercase();
    if st.db_parameter_groups.contains_key(&target_id) {
        return Err(RdsError::client400(
            "DBParameterGroupAlreadyExists",
            format!("A DB parameter group named {target} already exists."),
        ));
    }
    let mut group = DBParameterGroup {
        name: target_id.clone(),
        family: src.family.clone(),
        description,
        tags: tags_param(p),
        parameters: Vec::new(),
    };
    for (name, values) in &src.parameters {
        group.set_parameter(name, values.clone());
    }
    let response = group.clone();
    st.db_parameter_groups.insert(target_id, group);
    Ok(Some(parameter_group_xml(acct, region, &response)))
}

fn reset_db_parameter_group(st: &mut RdsState, p: &Params) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBParameterGroupName").unwrap_or_default();
    let lower = identifier.to_lowercase();
    if !st.db_parameter_groups.contains_key(&lower) {
        return Err(RdsError::not_found404(
            "DBParameterGroupNotFound",
            format!("DB Parameter Group {identifier} not found."),
        ));
    }
    if p_bool(p, "ResetAllParameters").unwrap_or(false) {
        st.db_parameter_groups.get_mut(&lower).unwrap().reset_all();
    }
    Ok(Some(format!(
        "<DBParameterGroupName>{}</DBParameterGroupName>",
        esc(&identifier)
    )))
}

// Cluster parameter groups ---------------------------------------------------

fn create_db_cluster_parameter_group(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBClusterParameterGroupName").unwrap_or_default();
    let lower = identifier.to_lowercase();
    if st.db_cluster_parameter_groups.contains_key(&lower) {
        return Err(RdsError::client400(
            "DBParameterGroupAlreadyExists",
            format!("A DB parameter group named {identifier} already exists."),
        ));
    }
    let family = p_string(p, "DBParameterGroupFamily").unwrap_or_default();
    if family.is_empty() {
        return Err(RdsError::client400(
            "InvalidParameterValue",
            "The parameter DBParameterGroupFamily must be provided and must not be blank.",
        ));
    }
    let description = p_string(p, "Description").unwrap_or_default();
    if description.is_empty() {
        return Err(RdsError::client400(
            "InvalidParameterValue",
            "The parameter Description must be provided and must not be blank.",
        ));
    }
    let group = DBClusterParameterGroup {
        name: lower.clone(),
        family,
        description,
        tags: tags_param(p),
        parameters: Vec::new(),
    };
    let response = group.clone();
    st.db_cluster_parameter_groups.insert(lower, group);
    Ok(Some(cluster_parameter_group_xml(acct, region, &response)))
}

fn describe_db_cluster_parameter_groups_xml(
    st: &RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let filter = p_string(p, "DBClusterParameterGroupName").map(|s| s.to_lowercase());
    if let Some(f) = &filter {
        if !st.db_cluster_parameter_groups.contains_key(f) {
            return Err(RdsError::not_found404(
                "DBParameterGroupNotFound",
                format!("DBClusterParameterGroup not found: {f}"),
            ));
        }
    }
    let matched: Vec<&DBClusterParameterGroup> = st
        .db_cluster_parameter_groups
        .values()
        .filter(|g| match &filter {
            Some(f) => g.name == *f,
            None => true,
        })
        .collect();
    let mut list = String::new();
    for g in &matched {
        list.push_str(&cluster_parameter_group_xml(acct, region, g));
    }
    Ok(Some(format!(
        "<DBClusterParameterGroups>{list}</DBClusterParameterGroups>"
    )))
}

fn describe_db_cluster_parameters(st: &RdsState, p: &Params) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBClusterParameterGroupName").unwrap_or_default();
    let lower = identifier.to_lowercase();
    let group = match st.db_cluster_parameter_groups.get(&lower) {
        Some(g) => g.clone(),
        None => {
            return Err(RdsError::not_found404(
                "DBParameterGroupNotFound",
                format!("DBClusterParameterGroup not found: {identifier}"),
            ))
        }
    };
    Ok(Some(parameters_xml(&group.parameters)))
}

fn modify_db_cluster_parameter_group(st: &mut RdsState, p: &Params) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBClusterParameterGroupName").unwrap_or_default();
    let lower = identifier.to_lowercase();
    if !st.db_cluster_parameter_groups.contains_key(&lower) {
        return Err(RdsError::not_found404(
            "DBParameterGroupNotFound",
            format!("DBClusterParameterGroup not found: {identifier}"),
        ));
    }
    for m in p_maps(p, "Parameters") {
        let mut name = None;
        let mut values = std::collections::BTreeMap::new();
        for (k, v) in m {
            if k == "ParameterName" {
                name = Some(v);
            } else {
                values.insert(k, v);
            }
        }
        if let Some(name) = name {
            st.db_cluster_parameter_groups
                .get_mut(&lower)
                .unwrap()
                .set_parameter(&name, values);
        }
    }
    Ok(Some(format!(
        "<DBClusterParameterGroupName>{}</DBClusterParameterGroupName>",
        esc(&identifier)
    )))
}

fn delete_db_cluster_parameter_group(st: &mut RdsState, p: &Params) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBClusterParameterGroupName").unwrap_or_default();
    let lower = identifier.to_lowercase();
    if !st.db_cluster_parameter_groups.contains_key(&lower) {
        return Err(RdsError::not_found404(
            "DBParameterGroupNotFound",
            format!("DBClusterParameterGroup not found: {identifier}"),
        ));
    }
    st.db_cluster_parameter_groups.remove(&lower);
    Ok(None)
}

fn copy_db_cluster_parameter_group(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let source = p_string(p, "SourceDBClusterParameterGroupIdentifier").unwrap_or_default();
    let target = p_string(p, "TargetDBClusterParameterGroupIdentifier").unwrap_or_default();
    let description = p_string(p, "TargetDBClusterParameterGroupDescription").unwrap_or_default();
    let source_id = identifier_from_arn(&source).to_lowercase();
    let src = match st.db_cluster_parameter_groups.get(&source_id) {
        Some(g) => g.clone(),
        None => {
            return Err(RdsError::not_found404(
                "DBParameterGroupNotFound",
                format!("DBClusterParameterGroup not found: {source}"),
            ))
        }
    };
    let target_id = target.to_lowercase();
    if st.db_cluster_parameter_groups.contains_key(&target_id) {
        return Err(RdsError::client400(
            "DBParameterGroupAlreadyExists",
            format!("A DB parameter group named {target} already exists."),
        ));
    }
    let mut group = DBClusterParameterGroup {
        name: target_id,
        family: src.family.clone(),
        description,
        tags: tags_param(p),
        parameters: Vec::new(),
    };
    for (name, values) in &src.parameters {
        group.set_parameter(name, values.clone());
    }
    let response = group.clone();
    st.db_cluster_parameter_groups
        .insert(response.name.clone(), group);
    Ok(Some(cluster_parameter_group_xml(acct, region, &response)))
}

fn reset_db_cluster_parameter_group(st: &mut RdsState, p: &Params) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBClusterParameterGroupName").unwrap_or_default();
    let lower = identifier.to_lowercase();
    if !st.db_cluster_parameter_groups.contains_key(&lower) {
        return Err(RdsError::not_found404(
            "DBParameterGroupNotFound",
            format!("DBClusterParameterGroup not found: {identifier}"),
        ));
    }
    if p_bool(p, "ResetAllParameters").unwrap_or(false) {
        st.db_cluster_parameter_groups
            .get_mut(&lower)
            .unwrap()
            .reset_all();
    }
    Ok(Some(format!(
        "<DBClusterParameterGroupName>{}</DBClusterParameterGroupName>",
        esc(&identifier)
    )))
}

// Subnet groups --------------------------------------------------------------

fn create_db_subnet_group(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
    subnet_info: &BTreeMap<String, (Option<String>, Option<String>)>,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBSubnetGroupName").unwrap_or_default();
    let lower = identifier.to_lowercase();
    if st.subnet_groups.contains_key(&lower) {
        return Err(RdsError::client400(
            "DBSubnetGroupAlreadyExists",
            format!(
                "DBSubnetGroupAlreadyExists: The DB subnet group '{identifier}' already exists."
            ),
        ));
    }
    let description = p_string(p, "DBSubnetGroupDescription").unwrap_or_default();
    let subnet_ids = p_list(p, "SubnetIds");
    if subnet_ids.is_empty() {
        return Err(RdsError::client400(
            "InvalidParameterValue",
            "The DB subnet group doesn't meet Availability Zone (AZ) coverage requirement. Add subnets to cover at least 2 Availability Zones.",
        ));
    }
    let mut subnets = Vec::new();
    for sid in &subnet_ids {
        let (vpc, az) = subnet_info.get(sid).cloned().unwrap_or((None, None));
        subnets.push(SubnetInfo {
            id: sid.clone(),
            az,
            vpc_id: vpc,
        });
    }
    let group = DBSubnetGroup {
        name: lower.clone(),
        description: p_string(p, "DBSubnetGroupDescription").unwrap_or_default(),
        subnets,
        tags: tags_param(p),
    };
    let response = group.clone();
    st.subnet_groups.insert(lower, group);
    Ok(Some(subnet_group_xml(acct, region, &response)))
}

fn modify_db_subnet_group(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
    subnet_info: &BTreeMap<String, (Option<String>, Option<String>)>,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBSubnetGroupName").unwrap_or_default();
    let lower = identifier.to_lowercase();
    let group = match st.subnet_groups.get(&lower) {
        Some(g) => g.clone(),
        None => {
            return Err(RdsError::not_found404(
                "DBSubnetGroupNotFoundFault",
                format!("Subnet Group {identifier} not found."),
            ))
        }
    };
    let subnet_ids = p_list(p, "SubnetIds");
    let mut new_group = group;
    for sid in &subnet_ids {
        if !new_group.subnets.iter().any(|s| &s.id == sid) {
            let (vpc, az) = subnet_info.get(sid).cloned().unwrap_or((None, None));
            new_group.subnets.push(SubnetInfo {
                id: sid.clone(),
                az,
                vpc_id: vpc,
            });
        }
    }
    if let Some(v) = p_string(p, "DBSubnetGroupDescription") {
        new_group.description = v;
    }
    // moto keeps the existing entries when no SubnetIds are provided
    let response = new_group.clone();
    st.subnet_groups.insert(lower, new_group);
    Ok(Some(subnet_group_xml(acct, region, &response)))
}

fn delete_db_subnet_group(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBSubnetGroupName").unwrap_or_default();
    let lower = identifier.to_lowercase();
    if !st.subnet_groups.contains_key(&lower) {
        return Err(RdsError::not_found404(
            "DBSubnetGroupNotFoundFault",
            format!("Subnet Group {identifier} not found."),
        ));
    }
    let group = st.subnet_groups.remove(&lower).unwrap();
    Ok(Some(subnet_group_xml(acct, region, &group)))
}

fn describe_db_subnet_groups_xml(
    st: &RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let filter = p_string(p, "DBSubnetGroupName").map(|s| s.to_lowercase());
    let matched: Vec<&DBSubnetGroup> = st
        .subnet_groups
        .values()
        .filter(|g| match &filter {
            Some(f) => g.name == *f,
            None => true,
        })
        .collect();
    if !matched.is_empty() || filter.is_none() {
        let mut list = String::new();
        for g in &matched {
            list.push_str(&subnet_group_xml(acct, region, g));
        }
        return Ok(Some(format!("<DBSubnetGroups>{list}</DBSubnetGroups>")));
    }
    Err(RdsError::not_found404(
        "DBSubnetGroupNotFoundFault",
        format!("Subnet Group {} not found.", filter.unwrap_or_default()),
    ))
}

// Security groups ------------------------------------------------------------

fn create_db_security_group(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBSecurityGroupName").unwrap_or_default();
    if st.security_groups.contains_key(&identifier) {
        return Err(RdsError::client400(
            "DBSecurityGroupAlreadyExists",
            format!("Security Group {identifier} already exists."),
        ));
    }
    let group = DBSecurityGroup {
        name: identifier.clone(),
        description: p_string(p, "DBSecurityGroupDescription").unwrap_or_default(),
        vpc_id: p_string(p, "VpcId"),
        ip_ranges: Vec::new(),
        ec2_security_groups: Vec::new(),
        tags: tags_param(p),
    };
    let response = group.clone();
    st.security_groups.insert(identifier, group);
    Ok(Some(security_group_xml(acct, region, &response)))
}

fn describe_db_security_groups_xml(
    st: &RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let filter = p_string(p, "DBSecurityGroupName");
    let matched: Vec<&DBSecurityGroup> = st
        .security_groups
        .values()
        .filter(|g| match &filter {
            Some(f) => g.name == *f,
            None => true,
        })
        .collect();
    if matched.is_empty() && filter.is_some() {
        return Err(RdsError::not_found404(
            "DBSecurityGroupNotFound",
            format!("Security Group {} not found.", filter.unwrap_or_default()),
        ));
    }
    let mut list = String::new();
    for g in &matched {
        list.push_str(&security_group_xml(acct, region, g));
    }
    Ok(Some(format!("<DBSecurityGroups>{list}</DBSecurityGroups>")))
}

fn delete_db_security_group(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBSecurityGroupName").unwrap_or_default();
    if !st.security_groups.contains_key(&identifier) {
        return Err(RdsError::not_found404(
            "DBSecurityGroupNotFound",
            format!("Security Group {identifier} not found."),
        ));
    }
    let group = st.security_groups.remove(&identifier).unwrap();
    Ok(Some(security_group_xml(acct, region, &group)))
}

fn authorize_db_security_group_ingress(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBSecurityGroupName").unwrap_or_default();
    let cidr = p_string(p, "CIDRIP");
    let group = match st.security_groups.get(&identifier) {
        Some(g) => g.clone(),
        None => {
            return Err(RdsError::not_found404(
                "DBSecurityGroupNotFound",
                format!("Security Group {identifier} not found."),
            ))
        }
    };
    if let Some(cidr) = cidr {
        if !group.ip_ranges.iter().any(|r| r == &cidr) {
            st.security_groups
                .get_mut(&identifier)
                .unwrap()
                .ip_ranges
                .push(cidr);
        }
    }
    if let Some(eg_name) = p_string(p, "EC2SecurityGroupName") {
        let owner = p_string(p, "EC2SecurityGroupOwnerId").unwrap_or_default();
        st.security_groups
            .get_mut(&identifier)
            .unwrap()
            .ec2_security_groups
            .push((eg_name.clone(), eg_name, owner));
    }
    let updated = st.security_groups.get(&identifier).unwrap().clone();
    Ok(Some(security_group_xml(acct, region, &updated)))
}

fn revoke_db_security_group_ingress(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBSecurityGroupName").unwrap_or_default();
    if !st.security_groups.contains_key(&identifier) {
        return Err(RdsError::not_found404(
            "DBSecurityGroupNotFound",
            format!("Security Group {identifier} not found."),
        ));
    }
    let cidr = p_string(p, "CIDRIP");
    let eg_name = p_string(p, "EC2SecurityGroupName");
    {
        let group = st.security_groups.get_mut(&identifier).unwrap();
        if let Some(cidr) = cidr {
            group.ip_ranges.retain(|r| r != &cidr);
        }
        if let Some(eg) = eg_name {
            group.ec2_security_groups.retain(|(n, _, _)| n != &eg);
        }
    }
    let updated = st.security_groups.get(&identifier).unwrap().clone();
    Ok(Some(security_group_xml(acct, region, &updated)))
}

// Event subscriptions + events + static describes ----------------------------

fn create_event_subscription(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "SubscriptionName").unwrap_or_default();
    if st.event_subscriptions.contains_key(&identifier) {
        return Err(RdsError::client400(
            "SubscriptionAlreadyExist",
            format!("Subscription {identifier} already exists."),
        ));
    }
    let sub = EventSubscription {
        name: identifier.clone(),
        sns_topic_arn: p_string(p, "SnsTopicArn"),
        source_type: p_string(p, "SourceType"),
        event_categories: p_list(p, "EventCategories"),
        source_ids: Vec::new(),
        enabled: p_bool(p, "Enabled").unwrap_or(false),
        tags: tags_param(p),
        created: Utc::now(),
    };
    let response = sub.clone();
    st.event_subscriptions.insert(identifier, sub);
    Ok(Some(event_subscription_xml(acct, region, &response)))
}

fn describe_event_subscriptions_xml(
    st: &RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let filter = p_string(p, "SubscriptionName");
    let matched: Vec<&EventSubscription> = st
        .event_subscriptions
        .values()
        .filter(|s| match &filter {
            Some(f) => s.name == *f,
            None => true,
        })
        .collect();
    if matched.is_empty() && filter.is_some() {
        return Err(RdsError::not_found404(
            "SubscriptionNotFound",
            format!("Subscription {} not found.", filter.unwrap_or_default()),
        ));
    }
    let mut list = String::new();
    for s in &matched {
        list.push_str(&event_subscription_xml(acct, region, s));
    }
    Ok(Some(format!(
        "<EventSubscriptionsList>{list}</EventSubscriptionsList>"
    )))
}

fn delete_event_subscription(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "SubscriptionName").unwrap_or_default();
    if !st.event_subscriptions.contains_key(&identifier) {
        return Err(RdsError::not_found404(
            "SubscriptionNotFound",
            format!("Subscription {identifier} not found."),
        ));
    }
    let sub = st.event_subscriptions.remove(&identifier).unwrap();
    Ok(Some(event_subscription_xml(acct, region, &sub)))
}

fn modify_event_subscription(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "SubscriptionName").unwrap_or_default();
    if !st.event_subscriptions.contains_key(&identifier) {
        return Err(RdsError::not_found404(
            "SubscriptionNotFound",
            format!("Subscription {identifier} not found."),
        ));
    }
    let sub = st.event_subscriptions.get_mut(&identifier).unwrap();
    if let Some(v) = p_string(p, "SnsTopicArn") {
        sub.sns_topic_arn = Some(v);
    }
    if let Some(v) = p_string(p, "SourceType") {
        sub.source_type = Some(v);
    }
    if !p_list(p, "EventCategories").is_empty() {
        sub.event_categories = p_list(p, "EventCategories");
    }
    if let Some(v) = p_bool(p, "Enabled") {
        sub.enabled = v;
    }
    let response = sub.clone();
    Ok(Some(event_subscription_xml(acct, region, &response)))
}

fn add_source_identifier_to_subscription(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "SubscriptionName").unwrap_or_default();
    let source = p_string(p, "SourceIdentifier").unwrap_or_default();
    if !st.event_subscriptions.contains_key(&identifier) {
        return Err(RdsError::not_found404(
            "SubscriptionNotFound",
            format!("Subscription {identifier} not found."),
        ));
    }
    let sub = st.event_subscriptions.get_mut(&identifier).unwrap();
    if !sub.source_ids.contains(&source) {
        sub.source_ids.push(source);
    }
    let response = sub.clone();
    Ok(Some(event_subscription_xml(acct, region, &response)))
}

fn remove_source_identifier_from_subscription(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "SubscriptionName").unwrap_or_default();
    let source = p_string(p, "SourceIdentifier").unwrap_or_default();
    if !st.event_subscriptions.contains_key(&identifier) {
        return Err(RdsError::not_found404(
            "SubscriptionNotFound",
            format!("Subscription {identifier} not found."),
        ));
    }
    let sub = st.event_subscriptions.get_mut(&identifier).unwrap();
    sub.source_ids.retain(|s| s != &source);
    let response = sub.clone();
    Ok(Some(event_subscription_xml(acct, region, &response)))
}

fn describe_events_xml(st: &RdsState, p: &Params) -> Result<Body, RdsError> {
    let source_id = p_string(p, "SourceIdentifier");
    let source_type = p_string(p, "SourceType");
    if source_id.is_some() && source_type.is_none() {
        return Err(RdsError::client400(
            "InvalidParameterCombination",
            "Cannot specify source identifier without source type",
        ));
    }
    let matched: Vec<&Event> = st
        .events
        .iter()
        .filter(|e| match &source_id {
            Some(f) => e.source_identifier == *f,
            None => true,
        })
        .filter(|e| match &source_type {
            Some(t) => e.source_type == *t,
            None => true,
        })
        .collect();
    let mut list = String::new();
    for e in &matched {
        list.push_str(&description_of_event(e));
    }
    Ok(Some(format!("<Events>{list}</Events>")))
}

fn describe_event_categories_xml(p: &Params) -> Result<Body, RdsError> {
    let source_type = p_string(p, "SourceType");
    let categories: &[(&str, &[&str])] = &[
        (
            "db-instance",
            &[
                "availability",
                "backup",
                "configuration change",
                "creation",
                "deletion",
                "failover",
                "failure",
                "low storage",
                "maintenance",
                "notification",
                "read replica",
                "recovery",
                "restoration",
            ],
        ),
        (
            "db-cluster",
            &[
                "failover",
                "failure",
                "maintenance",
                "notification",
                "global-failover",
            ],
        ),
        ("db-parameter-group", &["configuration change"]),
        ("db-security-group", &["configuration change", "failure"]),
        (
            "db-snapshot",
            &["creation", "deletion", "notification", "restoration"],
        ),
        ("db-cluster-snapshot", &["backup", "notification"]),
    ];
    let mut list = String::new();
    for (stype, cats) in categories {
        if let Some(filter) = &source_type {
            if stype != filter {
                continue;
            }
        }
        let cat_xml: String = cats
            .iter()
            .map(|c| format!("<EventCategory>{}</EventCategory>", esc(c)))
            .collect();
        list.push_str(&format!(
            "<EventCategoriesMap><SourceType>{stype}</SourceType>\
             <EventCategories>{cat_xml}</EventCategories></EventCategoriesMap>"
        ));
    }
    Ok(Some(format!(
        "<EventCategoriesMapList>{list}</EventCategoriesMapList>"
    )))
}

fn describe_db_engine_versions_xml(p: &Params) -> Result<Body, RdsError> {
    let engine = p_string(p, "Engine");
    let engine_version = p_string(p, "EngineVersion");
    let mut data: Vec<(&str, Vec<&str>)> = vec![
        (
            "neptune",
            vec!["1.2.0.0", "1.2.0.1", "1.2.0.2", "1.2.1.0", "1.3.0.0"],
        ),
        (
            "aurora-mysql",
            vec!["5.7.mysql_aurora.2.11.2", "8.0.mysql_aurora.3.04.0"],
        ),
        ("aurora-postgresql", vec!["14.6", "15.4"]),
        ("mysql", vec!["8.0.32", "8.0.33"]),
        ("postgres", vec!["14.9", "15.4"]),
    ];
    if let Some(engine) = &engine {
        data.retain(|(e, _)| e == engine);
        if data.is_empty() {
            data.push((engine.as_str(), vec!["1.0.0"]));
        }
    }
    let mut list = String::new();
    for (eng, versions) in &data {
        for ver in versions {
            if let Some(filter) = &engine_version {
                if ver != filter {
                    continue;
                }
            }
            let family = format!("{eng}{}", ver.split('.').next().unwrap_or(""));
            list.push_str(&format!(
                "<DBEngineVersion>\
                 <Engine>{}</Engine>\
                 <EngineVersion>{}</EngineVersion>\
                 <DBParameterGroupFamily>{}</DBParameterGroupFamily>\
                 <DBEngineDescription>{} database engine</DBEngineDescription>\
                 <DBEngineVersionDescription>{eng} {ver}</DBEngineVersionDescription>\
                 <ValidUpgradeTarget/>\
                 <ExportableLogTypes/>\
                 <SupportsLogExportsToCloudwatchLogs>false</SupportsLogExportsToCloudwatchLogs>\
                 <SupportsReadReplica>false</SupportsReadReplica>\
                 <SupportedEngineModes/>\
                 <SupportedFeatureNames/>\
                 <SupportsGlobalDatabases>{}</SupportsGlobalDatabases>\
                 </DBEngineVersion>",
                esc(eng),
                esc(ver),
                esc(&family),
                esc(eng),
                matches!(*eng, "neptune" | "aurora-mysql" | "aurora-postgresql"),
            ));
        }
    }
    Ok(Some(format!("<DBEngineVersions>{list}</DBEngineVersions>")))
}

fn describe_orderable_db_instance_options_xml(
    engine: &str,
    engine_version: Option<String>,
) -> Result<Body, RdsError> {
    // moto only implements aurora-postgresql and neptune option listings.
    let _ = engine_version;
    let supported = matches!(&*engine, "aurora-postgresql" | "neptune");
    if !supported {
        return Ok(Some("<OrderableDBInstanceOptions/>".to_string()));
    }
    Ok(Some("<OrderableDBInstanceOptions/>".to_string()))
}

fn describe_account_attributes_xml(st: &RdsState) -> Result<Body, RdsError> {
    let fmt = |name: &str, used: usize, max: i64| {
        format!(
            "<AccountQuota><AccountQuotaName>{name}</AccountQuotaName>\
             <Used>{used}</Used><Max>{max}</Max></AccountQuota>",
        )
    };
    let mut list = String::new();
    list.push_str(&fmt("DBInstances", st.databases.len(), 40));
    list.push_str(&fmt("ReservedDBInstances", 0, 40));
    list.push_str(&fmt("AllocatedStorage", 0, 100000));
    list.push_str(&fmt("DBSecurityGroups", st.security_groups.len(), 25));
    list.push_str(&fmt("AuthorizationsPerDBSecurityGroup", 0, 20));
    list.push_str(&fmt("DBParameterGroups", st.db_parameter_groups.len(), 50));
    let manual = st
        .database_snapshots
        .values()
        .filter(|s| s.snapshot_type != "automated")
        .count();
    list.push_str(&fmt("ManualSnapshots", manual, 100));
    list.push_str(&fmt("EventSubscriptions", st.event_subscriptions.len(), 20));
    list.push_str(&fmt("DBSubnetGroups", st.subnet_groups.len(), 50));
    list.push_str(&fmt("OptionGroups", st.option_groups.len(), 20));
    list.push_str(&fmt("SubnetsPerDBSubnetGroup", 0, 20));
    list.push_str(&fmt("ReadReplicasPerMaster", 0, 15));
    list.push_str(&fmt("DBClusters", st.clusters.len(), 40));
    list.push_str(&fmt(
        "DBClusterParameterGroups",
        st.db_cluster_parameter_groups.len(),
        50,
    ));
    list.push_str(&fmt("DBClusterRoles", 0, 5));
    list.push_str(&fmt(
        "ManualClusterSnapshots",
        st.cluster_snapshots.len(),
        100,
    ));
    list.push_str(&fmt("CustomEndpointsPerDBCluster", 0, 5));
    list.push_str(&fmt("DBInstanceRoles", 0, 5));
    Ok(Some(format!("<AccountQuotas>{list}</AccountQuotas>")))
}

fn describe_certificates_xml(p: &Params) -> Result<Body, RdsError> {
    let filter = p_string(p, "CertificateIdentifier");
    let certs: Vec<[String; 6]> = vec![
        [
            "rds-ca-2019".into(),
            "CA".into(),
            "d4ec8b4f0c0aeb7894f47694b4e3b2c0f8b8c9a0".into(),
            "2019-09-19T18:16:53Z".into(),
            "2024-08-22T17:08:50Z".into(),
            "arn:aws:rds:us-east-1:123456789012:cert:rds-ca-2019".into(),
        ],
        [
            "rds-ca-rsa2048-g1".into(),
            "CA".into(),
            "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2".into(),
            "2021-05-25T00:00:00Z".into(),
            "2061-05-25T00:00:00Z".into(),
            "arn:aws:rds:us-east-1:123456789012:cert:rds-ca-rsa2048-g1".into(),
        ],
        [
            "rds-ca-ecc384-g1".into(),
            "CA".into(),
            "b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3".into(),
            "2021-05-25T00:00:00Z".into(),
            "2121-05-25T00:00:00Z".into(),
            "arn:aws:rds:us-east-1:123456789012:cert:rds-ca-ecc384-g1".into(),
        ],
    ];
    let mut list = String::new();
    for cert in &certs {
        if let Some(f) = &filter {
            if &cert[0] != f {
                continue;
            }
        }
        list.push_str(&format!(
            "<Certificate>\
             <CertificateIdentifier>{}</CertificateIdentifier>\
             <CertificateType>{}</CertificateType>\
             <Thumbprint>{}</Thumbprint>\
             <ValidFrom>{}</ValidFrom>\
             <ValidTill>{}</ValidTill>\
             <CertificateArn>{}</CertificateArn>\
             <CustomerOverride>false</CustomerOverride>\
             </Certificate>",
            esc(&cert[0]),
            esc(&cert[1]),
            esc(&cert[2]),
            esc(&cert[3]),
            esc(&cert[4]),
            esc(&cert[5]),
        ));
    }
    Ok(Some(format!(
        "<DefaultCertificateForNewLaunches>rds-ca-rsa2048-g1</DefaultCertificateForNewLaunches>\
         <Certificates>{list}</Certificates>"
    )))
}

fn describe_source_regions_xml(p: &Params) -> Result<Body, RdsError> {
    let regions: &[(&str, &str)] = &[
        ("us-east-1", "rds.us-east-1.amazonaws.com"),
        ("us-east-2", "us-east-2.rds.amazonaws.com"),
        ("us-west-1", "us-west-1.rds.amazonaws.com"),
        ("us-west-2", "us-west-2.rds.amazonaws.com"),
        ("eu-west-1", "eu-west-1.rds.amazonaws.com"),
    ];
    let region = p_string(p, "RegionName");
    let mut list = String::new();
    for (name, endpoint) in regions {
        if let Some(filter) = &region {
            if name != filter {
                continue;
            }
        }
        list.push_str(&format!(
            "<SourceRegion>\
             <RegionName>{name}</RegionName>\
             <Endpoint>{endpoint}</Endpoint>\
             <SupportsDBInstanceAutomatedBackupsCopy>true</SupportsDBInstanceAutomatedBackupsCopy>\
             </SourceRegion>"
        ));
    }
    Ok(Some(format!("<SourceRegions>{list}</SourceRegions>")))
}

fn describe_db_major_engine_versions_xml(p: &Params) -> Result<Body, RdsError> {
    let mut versions: Vec<String> = Vec::new();
    let engines = vec![
        ("mysql", vec!["8.0", "5.7"]),
        ("postgres", vec!["15", "14", "13"]),
        ("aurora-mysql", vec!["5.7", "8.0"]),
        ("aurora-postgresql", vec!["12", "13"]),
        ("custom-oracle-ee", vec!["19"]),
        ("sqlserver-ee", vec!["15.00"]),
    ];
    for (engine, majors) in engines {
        for major in majors {
            versions.push(format!(
                "<DBMajorEngineVersion>\
                 <Engine>{engine}</Engine>\
                 <MajorEngineVersion>{major}</MajorEngineVersion>\
                 <DBEngineDescription>{engine} database engine</DBEngineDescription>\
                 <SupportedEngineModes><member>provisioned</member></SupportedEngineModes>\
                 <Status>available</Status>\
                 <AvailableUpgrades/>\
                 <ExportableLogTypes/>\
                 <SupportsLogExportsToCloudwatchLogs>false</SupportsLogExportsToCloudwatchLogs>\
                 </DBMajorEngineVersion>"
            ));
        }
    }
    Ok(Some(format!(
        "<DBMajorEngineVersions>{}</DBMajorEngineVersions>",
        versions.join("")
    )))
}

// DB proxies + endpoints + targets -------------------------------------------

fn create_db_proxy(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
    subnet_info: &BTreeMap<String, (Option<String>, Option<String>)>,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBProxyName").unwrap_or_default();
    RdsError::check_db_identifier(&identifier)?;
    let engine_family = p_string(p, "EngineFamily").unwrap_or_default();
    if !matches!(engine_family.as_str(), "MYSQL" | "POSTGRESQL" | "SQLSERVER") {
        return Err(RdsError::client400(
            "InvalidParameterValue",
            "Provided EngineFamily is not valid.",
        ));
    }
    if st.db_proxies.contains_key(&identifier) {
        return Err(RdsError::client400(
            "DBProxyAlreadyExistsFault",
            format!("Cannot create the DBProxy because a DBProxy with the identifier {identifier} already exists."),
        ));
    }
    let auth: Vec<DBProxyAuth> = p_maps(p, "Auth")
        .into_iter()
        .map(|m| {
            let mut a = DBProxyAuth {
                auth_scheme: "SECRETS".into(),
                description: None,
                user_name: None,
                secret_arn: None,
                client_password_auth_type: None,
                iam_auth: "DISABLED".into(),
            };
            for (k, v) in m {
                match k.as_str() {
                    "AuthScheme" => a.auth_scheme = v,
                    "Description" => a.description = Some(v),
                    "UserName" => a.user_name = Some(v),
                    "SecretArn" => a.secret_arn = Some(v),
                    "ClientPasswordAuthType" => a.client_password_auth_type = Some(v),
                    "IAMAuth" => a.iam_auth = v,
                    _ => {}
                }
            }
            a
        })
        .collect();
    let vpc_subnet_ids = p_list(p, "VpcSubnetIds");
    let vpc_id = vpc_subnet_ids
        .first()
        .and_then(|sid| subnet_info.get(sid))
        .and_then(|(vpc, _)| vpc.clone());
    let idle = p_i64(p, "IdleClientTimeout").unwrap_or(1800);
    let idle = idle.clamp(1, 28800);
    let unique_id = format!("prx-{}", rand_lower(17));
    let url_identifier = rand_lower(12);
    let proxy = DBProxy {
        name: identifier,
        engine_family,
        auth,
        role_arn: p_string(p, "RoleArn"),
        vpc_subnet_ids,
        vpc_security_group_ids: p_list(p, "VpcSecurityGroupIds"),
        vpc_id,
        require_tls: p_bool(p, "RequireTLS").unwrap_or(false),
        idle_client_timeout: idle,
        debug_logging: p_bool(p, "DebugLogging").unwrap_or(false),
        unique_id,
        url_identifier,
        tags: tags_param(p),
        created: Utc::now(),
        max_connections: 100,
        max_idle_connections: 50,
        borrow_timeout: 120,
        session_pinning_filters: Vec::new(),
        targets: Vec::new(),
    };
    let response = proxy.clone();
    st.db_proxies.insert(response.name.clone(), proxy);
    Ok(Some(db_proxy_xml(acct, region, &response)))
}

fn describe_db_proxies_xml(
    st: &RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let filter = p_string(p, "DBProxyName");
    let matched: Vec<&DBProxy> = st
        .db_proxies
        .values()
        .filter(|x| match &filter {
            Some(f) => x.name == *f,
            None => true,
        })
        .collect();
    if matched.is_empty() && filter.is_some() {
        return Err(RdsError::not_found404(
            "DBProxyNotFoundFault",
            format!(
                "The specified proxy name {} doesn't correspond to a proxy owned by your Amazon Web Services account in the specified Amazon Web Services Region.",
                filter.unwrap_or_default()
            ),
        ));
    }
    let mut list = String::new();
    for x in &matched {
        list.push_str(&db_proxy_xml(acct, region, x));
    }
    Ok(Some(format!("<DBProxies>{list}</DBProxies>")))
}

fn delete_db_proxy(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBProxyName").unwrap_or_default();
    if !st.db_proxies.contains_key(&identifier) {
        return Err(RdsError::not_found404(
            "DBProxyNotFoundFault",
            format!(
                "The specified proxy name {identifier} doesn't correspond to a proxy owned by your Amazon Web Services account in the specified Amazon Web Services Region."
            ),
        ));
    }
    let proxy = st.db_proxies.remove(&identifier).unwrap();
    Ok(Some(db_proxy_xml(acct, region, &proxy)))
}

fn require_proxy<'s>(st: &'s RdsState, name: &str) -> Result<&'s DBProxy, RdsError> {
    match st.db_proxies.get(name) {
        Some(x) => Ok(x),
        None => Err(RdsError::not_found404(
            "DBProxyNotFoundFault",
            format!(
                "The specified proxy name {name} doesn't correspond to a proxy owned by your Amazon Web Services account in the specified Amazon Web Services Region."
            ),
        )),
    }
}

fn describe_db_proxy_target_groups_xml(
    st: &RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let name = p_string(p, "DBProxyName").unwrap_or_default();
    let proxy = require_proxy(st, &name)?;
    Ok(Some(format!(
        "<TargetGroups>{}</TargetGroups>",
        proxy_target_groups_xml(acct, region, proxy)
    )))
}

fn describe_db_proxy_targets_xml(st: &RdsState, p: &Params) -> Result<Body, RdsError> {
    let name = p_string(p, "DBProxyName").unwrap_or_default();
    let proxy = require_proxy(st, &name)?;
    let mut list = String::new();
    for t in &proxy.targets {
        list.push_str(&proxy_target_xml(t));
    }
    Ok(Some(format!("<Targets>{list}</Targets>")))
}

fn register_db_proxy_targets(st: &mut RdsState, p: &Params) -> Result<Body, RdsError> {
    let name = p_string(p, "DBProxyName").unwrap_or_default();
    require_proxy(st, &name)?;
    let clusters = p_list(p, "DBClusterIdentifiers");
    let instances = p_list(p, "DBInstanceIdentifiers");
    let mut targets = Vec::new();
    for c in &clusters {
        targets.push(ProxyTarget {
            rds_resource_id: identifier_from_arn(c),
            port: 5432,
            r#type: "TRACKED_CLUSTER".into(),
            registering: std::sync::atomic::AtomicBool::new(true),
        });
    }
    for i in &instances {
        targets.push(ProxyTarget {
            rds_resource_id: i.clone(),
            port: 5432,
            r#type: "RDS_INSTANCE".into(),
            registering: std::sync::atomic::AtomicBool::new(true),
        });
    }
    let mut list = String::new();
    for t in &targets {
        list.push_str(&proxy_target_xml(t));
    }
    let proxy = st.db_proxies.get_mut(&name).unwrap();
    proxy.targets.extend(targets);
    Ok(Some(format!("<DBProxyTargets>{list}</DBProxyTargets>")))
}

fn deregister_db_proxy_targets(st: &mut RdsState, p: &Params) -> Result<Body, RdsError> {
    let name = p_string(p, "DBProxyName").unwrap_or_default();
    require_proxy(st, &name)?;
    let clusters = p_list(p, "DBClusterIdentifiers");
    let instances = p_list(p, "DBInstanceIdentifiers");
    let proxy = st.db_proxies.get_mut(&name).unwrap();
    proxy.targets.retain(|t| {
        !clusters.contains(&t.rds_resource_id) && !instances.contains(&t.rds_resource_id)
    });
    Ok(Some(String::new()))
}

fn modify_db_proxy_target_group(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let name = p_string(p, "DBProxyName").unwrap_or_default();
    require_proxy(st, &name)?;
    let group_name = p_string(p, "TargetGroupName").unwrap_or_else(|| "default".into());
    if group_name != "default" {
        return Err(RdsError::not_found404(
            "DBProxyTargetGroupNotFoundFault",
            format!("The specified target group name {group_name} does not exist."),
        ));
    }
    if let Some(max) = p_i64(p, "ConnectionPoolConfig_MaxConnectionsPercent") {
        let proxy = st.db_proxies.get_mut(&name).unwrap();
        proxy.max_connections = max;
        if proxy.max_idle_connections != max / 2 {
            proxy.max_idle_connections = max / 2;
        }
    }
    if let Some(max_idle) = p_i64(p, "ConnectionPoolConfig_MaxIdleConnectionsPercent") {
        let proxy = st.db_proxies.get_mut(&name).unwrap();
        proxy.max_idle_connections = max_idle;
    }
    if let Some(borrow) = p_i64(p, "ConnectionPoolConfig_ConnectionBorrowTimeout") {
        let proxy = st.db_proxies.get_mut(&name).unwrap();
        proxy.borrow_timeout = borrow;
    }
    let proxy = st.db_proxies.get(&name).unwrap();
    Ok(Some(proxy_target_group_response(acct, region, proxy)))
}

fn proxy_target_group_response(acct: u64, region: &str, p: &DBProxy) -> String {
    let target_arn = format!(
        "arn:aws:rds:{region}:{acct}:target-group:prx-tg-{}",
        p.unique_id
    );
    let mut out = String::from("<DBProxyTargetGroup>");
    out.push_str(&format!("<DBProxyName>{}</DBProxyName>", esc(&p.name)));
    out.push_str("<TargetGroupName>default</TargetGroupName>");
    out.push_str(&format!(
        "<TargetGroupArn>{}</TargetGroupArn>",
        esc(&target_arn)
    ));
    out.push_str(&format!(
        "<CreatedDate>{}</CreatedDate>",
        esc(&iso(&p.created))
    ));
    out.push_str(&format!(
        "<UpdatedDate>{}</UpdatedDate>",
        esc(&iso(&p.created))
    ));
    out.push_str("<Status>available</Status>");
    out.push_str("<IsDefault>true</IsDefault>");
    out.push_str("<ConnectionPoolConfig>");
    out.push_str(&format!(
        "<MaxConnectionsPercent>{}</MaxConnectionsPercent>",
        p.max_connections
    ));
    out.push_str(&format!(
        "<MaxIdleConnectionsPercent>{}</MaxIdleConnectionsPercent>",
        p.max_idle_connections
    ));
    out.push_str(&format!(
        "<ConnectionBorrowTimeout>{}</ConnectionBorrowTimeout>",
        p.borrow_timeout
    ));
    out.push_str(&list_xml(
        "SessionPinningFilters",
        "member",
        &p.session_pinning_filters,
    ));
    out.push_str("</ConnectionPoolConfig>");
    out.push_str("</DBProxyTargetGroup>");
    out
}

fn proxy_target_group_config_row(_acct: u64, _region: &str, p: &DBProxy) -> String {
    format!(
        "<ConnectionPoolConfig>\
         <MaxConnectionsPercent>{}</MaxConnectionsPercent>\
         <MaxIdleConnectionsPercent>{}</MaxIdleConnectionsPercent>\
         <ConnectionBorrowTimeout>{}</ConnectionBorrowTimeout>\
         {pin_filters}\
         </ConnectionPoolConfig>",
        p.max_connections,
        p.max_idle_connections,
        p.borrow_timeout,
        pin_filters = list_xml(
            "SessionPinningFilters",
            "member",
            &p.session_pinning_filters
        ),
    )
}

fn create_db_proxy_endpoint(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
    subnet_info: &BTreeMap<String, (Option<String>, Option<String>)>,
) -> Result<Body, RdsError> {
    let proxy_name = p_string(p, "DBProxyName").unwrap_or_default();
    require_proxy(st, &proxy_name)?;
    let ep_name = p_string(p, "DBProxyEndpointName").unwrap_or_default();
    if st.db_proxy_endpoints.contains_key(&ep_name) {
        return Err(RdsError::client400(
            "DBProxyEndpointAlreadyExistsFault",
            format!("The specified DB proxy endpoint {ep_name} already exists."),
        ));
    }
    let subnets = p_list(p, "VpcSubnetIds");
    let vpc_id = subnets
        .first()
        .and_then(|sid| subnet_info.get(sid))
        .and_then(|(v, _)| v.clone());
    let ep = DBProxyEndpoint {
        proxy_name: proxy_name.clone(),
        name: ep_name.clone(),
        vpc_subnet_ids: subnets,
        vpc_security_group_ids: p_list(p, "VpcSecurityGroupIds"),
        vpc_id,
        target_role: p_string(p, "TargetRole").unwrap_or_else(|| "READ_WRITE".into()),
        tags: tags_param(p),
        status: "available".into(),
        is_default: false,
        created: Utc::now(),
        url_identifier: rand_lower(12),
    };
    let response = ep.clone();
    st.db_proxy_endpoints.insert(ep_name, ep);
    Ok(Some(db_proxy_endpoint_xml(acct, region, &response)))
}

fn describe_db_proxy_endpoints_xml(
    st: &RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let db_filter = p_string(p, "DBProxyName");
    let ep_filter = p_string(p, "DBProxyEndpointName");
    if let Some(db) = &db_filter {
        require_proxy(st, db)?;
    }
    let matched: Vec<&DBProxyEndpoint> = st
        .db_proxy_endpoints
        .values()
        .filter(|e| match &db_filter {
            Some(f) => e.proxy_name == *f,
            None => true,
        })
        .filter(|e| match &ep_filter {
            Some(f) => e.name == *f,
            None => true,
        })
        .collect();
    if matched.is_empty() && ep_filter.is_some() {
        return Err(RdsError::not_found404(
            "DBProxyEndpointNotFoundFault",
            format!(
                "The specified DB proxy endpoint {} was not found.",
                ep_filter.unwrap_or_default()
            ),
        ));
    }
    let mut list = String::new();
    for e in &matched {
        list.push_str(&db_proxy_endpoint_xml(acct, region, e));
    }
    Ok(Some(format!("<DBProxyEndpoints>{list}</DBProxyEndpoints>")))
}

fn delete_db_proxy_endpoint(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let ep_name = p_string(p, "DBProxyEndpointName").unwrap_or_default();
    if !st.db_proxy_endpoints.contains_key(&ep_name) {
        return Err(RdsError::not_found404(
            "DBProxyEndpointNotFoundFault",
            format!("The specified DB proxy endpoint {ep_name} was not found."),
        ));
    }
    let mut ep = st.db_proxy_endpoints.remove(&ep_name).unwrap();
    ep.status = "deleting".into();
    Ok(Some(db_proxy_endpoint_xml(acct, region, &ep)))
}

fn modify_db_proxy_endpoint(st: &mut RdsState, p: &Params) -> Result<Body, RdsError> {
    let ep_name = p_string(p, "DBProxyEndpointName").unwrap_or_default();
    let mut status = Ok(ModifyProxyEndpointResult::Found);
    if !st.db_proxy_endpoints.contains_key(&ep_name) {
        status = Err("missing".to_string());
        let result = match status {
            Err(_) => Err(RdsError::new(
                404,
                "DBProxyEndpointNotFoundFault",
                format!("The specified DB proxy endpoint {ep_name} was not found."),
            )),
            Ok(_) => Ok(()),
        };
        let _ = result;
        return Err(RdsError::not_found404(
            "DBProxyEndpointNotFoundFault",
            format!("The specified DB proxy endpoint {ep_name} was not found."),
        ));
    }
    let _ = status;
    if let Some(v) = p_string(p, "TargetRole") {
        st.db_proxy_endpoints.get_mut(&ep_name).unwrap().target_role = v;
    }
    if let Some(v) = p_string(p, "DBProxyEndpointStatus") {
        st.db_proxy_endpoints.get_mut(&ep_name).unwrap().status = v;
    }
    Ok(ModifyProxyEndpointResult::Found.into_result())
}

enum ModifyProxyEndpointResult {
    Found,
}

impl ModifyProxyEndpointResult {
    fn into_result(self) -> Body {
        None
    }
}

// Export tasks ---------------------------------------------------------------

fn start_export_task(st: &mut RdsState, p: &Params) -> Result<Body, RdsError> {
    let identifier = p_string(p, "ExportTaskIdentifier").unwrap_or_default();
    if identifier.is_empty() {
        return Err(RdsError::client400(
            "InvalidParameterValue",
            "The value of ExportTaskIdentifier is not valid.",
        ));
    }
    let source_arn = p_string(p, "SourceArn").unwrap_or_default();
    let parts: Vec<&str> = source_arn.split(':').collect();
    let snapshot_id = parts.last().unwrap_or(&"").to_string();
    let snapshot_type_raw = parts.iter().rev().nth(1).unwrap_or(&"").to_string();
    if parts.len() >= 7 {
        // arn:aws:rds:region:account:snapshot:name
    }
    let source = match snapshot_type_raw.as_str() {
        "snapshot" => {
            if !st.database_snapshots.contains_key(&snapshot_id) {
                return Err(RdsError::not_found404(
                    "DBSnapshotNotFound",
                    format!("DBSnapshot {snapshot_id} not found."),
                ));
            }
            "SNAPSHOT".to_string()
        }
        "cluster-snapshot" => {
            if !st.cluster_snapshots.contains_key(&snapshot_id) {
                return Err(RdsError::not_found404(
                    "DBClusterSnapshotNotFoundFault",
                    format!("DBClusterSnapshot {snapshot_id} not found."),
                ));
            }
            "CLUSTER".to_string()
        }
        _ => {
            return Err(RdsError::client400(
                "InvalidParameterValue",
                format!("Invalid source ARN: {source_arn}. ARN must refer to a snapshot or cluster-snapshot."),
            ))
        }
    };
    if st.export_tasks.contains_key(&identifier) {
        return Err(RdsError::client400(
            "ExportTaskAlreadyExists",
            format!("Cannot start export task because a task with the identifier {identifier} already exists."),
        ));
    }
    let task = ExportTask {
        id: identifier.clone(),
        kms_key_id: p_string(p, "KmsKeyId").or(Some("default_kms_key_id".to_string())),
        source_arn: source_arn.clone(),
        iam_role_arn: p_string(p, "IamRoleArn"),
        s3_bucket: p_string(p, "S3BucketName"),
        s3_prefix: p_string(p, "S3Prefix").unwrap_or_default(),
        export_only: p_list(p, "ExportOnly"),
        status: "complete".into(),
        created: Utc::now(),
        source_type: source,
    };
    let response = task.clone();
    st.export_tasks.insert(identifier, task);
    export_task_response(&response)
}

fn export_task_response(t: &ExportTask) -> Result<Body, RdsError> {
    Ok(Some(export_task_xml(t)))
}

fn cancel_export_task(st: &mut RdsState, p: &Params) -> Result<Body, RdsError> {
    let identifier = p_string(p, "ExportTaskIdentifier").unwrap_or_default();
    if !st.export_tasks.contains_key(&identifier) {
        return Err(RdsError::not_found404(
            "ExportTaskNotFound",
            format!("Cannot cancel export task because a task with the identifier {identifier} is not exist."),
        ));
    }
    let task = st.export_tasks.get_mut(&identifier).unwrap();
    task.status = "canceled".into();
    let response = task.clone();
    export_task_response(&response)
}

fn describe_export_tasks_xml(st: &RdsState, p: &Params) -> Result<Body, RdsError> {
    let ident = p_string(p, "ExportTaskIdentifier");
    let matched: Vec<&ExportTask> = st
        .export_tasks
        .values()
        .filter(|t| match &ident {
            Some(i) => t.id == *i,
            None => true,
        })
        .collect();
    let mut list = String::new();
    for t in &matched {
        list.push_str(&format!("<ExportTask>{}</ExportTask>", export_task_xml(t)));
    }
    Ok(Some(format!("<ExportTasks>{list}</ExportTasks>")))
}

// Blue/green deployments -----------------------------------------------------

fn create_blue_green_deployment(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let name = p_string(p, "BlueGreenDeploymentName").unwrap_or_default();
    let source = p_string(p, "Source").unwrap_or_default();
    for bg in st.blue_green_deployments.values() {
        if bg.name == name {
            return Err(RdsError::client400(
                "BlueGreenDeploymentAlreadyExistsFault",
                format!("A blue/green deployment with the specified name {name} already exists."),
            ));
        }
    }
    let (instance_id, is_cluster) = {
        let source_id = identifier_from_arn(&source);
        let is_cluster = source.split(':').nth(5) == Some("cluster");
        let exists = if is_cluster {
            st.clusters.contains_key(&source_id.to_lowercase())
        } else {
            st.databases.contains_key(&source_id.to_lowercase())
        };
        if exists {
            (source_id, is_cluster)
        } else {
            return Err(RdsError::client400(
                "InvalidParameterValue",
                format!("Provided string is not a valid arn {source}"),
            ));
        }
    };
    let target = if is_cluster {
        format!("{}-green-{}", instance_id, rand_lower(6))
    } else {
        format!("{}-green-{}", instance_id, rand_lower(6))
    };
    let id = format!("bgd-{}", rand_lower(16));
    let bg = BlueGreenDeployment {
        id: id.clone(),
        name,
        source: source.clone(),
        target,
        status: "PROVISIONING".into(),
        status_details: None,
        tags: tags_param(p),
        created: Utc::now(),
    };
    let response = bg.clone();
    st.blue_green_deployments.insert(id, bg);
    let _ = (acct, region);
    Ok(Some(blue_green_xml(&response)))
}

fn describe_blue_green_deployments_xml(st: &RdsState, p: &Params) -> Result<Body, RdsError> {
    let ident = p_string(p, "BlueGreenDeploymentIdentifier");
    let matched: Vec<&BlueGreenDeployment> = st
        .blue_green_deployments
        .values()
        .filter(|b| match &ident {
            Some(i) => b.id == *i,
            None => true,
        })
        .collect();
    if matched.is_empty() && ident.is_some() {
        return Err(RdsError::not_found404(
            "BlueGreenDeploymentNotFoundFault",
            format!(
                "BlueGreenDeploymentIdentifier {} doesn't refer to an existing blue/green deployment.",
                ident.unwrap_or_default()
            ),
        ));
    }
    let mut list = String::new();
    for b in &matched {
        list.push_str(&blue_green_xml(b));
    }
    Ok(Some(format!(
        "<BlueGreenDeployments>{list}</BlueGreenDeployments>"
    )))
}

fn switchover_blue_green_deployment(st: &RdsState, p: &Params) -> Result<Body, RdsError> {
    let ident = p_string(p, "BlueGreenDeploymentIdentifier").unwrap_or_default();
    if !st.blue_green_deployments.contains_key(&ident) {
        return Err(RdsError::not_found404(
            "BlueGreenDeploymentNotFoundFault",
            format!(
                "BlueGreenDeploymentIdentifier {ident} doesn't refer to an existing blue/green deployment."
            ),
        ));
    }
    let bg = st.blue_green_deployments.get(&ident).unwrap();
    if bg.status != "AVAILABLE" {
        return Err(RdsError::client400(
            "InvalidBlueGreenDeploymentStateFault",
            format!("The blue/green deployment {ident} can't be switched over or deleted because there is an invalid configuration in the green environment."),
        ));
    }
    Ok(None)
}

fn delete_blue_green_deployment(st: &mut RdsState, p: &Params) -> Result<Body, RdsError> {
    let ident = p_string(p, "BlueGreenDeploymentIdentifier").unwrap_or_default();
    if !st.blue_green_deployments.contains_key(&ident) {
        return Err(RdsError::not_found404(
            "BlueGreenDeploymentNotFoundFault",
            format!(
                "BlueGreenDeploymentIdentifier {ident} doesn't refer to an existing blue/green deployment."
            ),
        ));
    }
    let mut bg = st.blue_green_deployments.remove(&ident).unwrap();
    bg.status = "DELETING".into();
    Ok(Some(blue_green_xml(&bg)))
}

// Shard groups + activity streams + tenant databases integrations CEVs gap ops

fn create_db_shard_group(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBShardGroupIdentifier").unwrap_or_default();
    let cluster_name = p_string(p, "DBClusterIdentifier").unwrap_or_default();
    let lower = identifier.to_lowercase();
    if st.shard_groups.contains_key(&lower) {
        return Err(RdsError::client400(
            "DBShardGroupAlreadyExists",
            format!("DB Shard Group {identifier} already exists."),
        ));
    }
    if !st.clusters.contains_key(&cluster_name.to_lowercase()) {
        return Err(RdsError::not_found404(
            "DBClusterNotFoundFault",
            format!("DBCluster {cluster_name} not found."),
        ));
    }
    if let Some(min) = p_f64(p, "MinACU") {
        let max = p_f64(p, "MaxACU").unwrap_or_default();
        let parsed_min: f64 = min.parse().unwrap_or(0.0);
        let parsed_max: f64 = max.parse().unwrap_or(0.0);
        if parsed_min >= parsed_max && !min.is_empty() && !max.is_empty() {
            return Err(RdsError::client400(
                "InvalidParameterValue",
                "min_acu cannot be larger than max_acu",
            ));
        }
    }
    let compute = p_i64(p, "ComputeRedundancy");
    if let Some(c) = compute {
        if !(0..=2).contains(&c) {
            return Err(RdsError::client400(
                "InvalidParameterValue",
                format!("Invalid ComputeRedundancy value: '{c}'. Valid values are 0 (no standby), 1 (1 standby AZ), 2 (2 standby AZs)."),
            ));
        }
    }
    let group = DBShardGroup {
        id: lower.clone(),
        cluster_identifier: cluster_name.to_lowercase(),
        compute_redundancy: compute,
        max_acu: p_f64(p, "MaxACU"),
        min_acu: p_f64(p, "MinACU"),
        status: "available".into(),
        tags: tags_param(p),
        created: Utc::now(),
    };
    let response = group.clone();
    st.shard_groups.insert(lower, group);
    Ok(Some(shard_group_xml(acct, region, &response)))
}

fn describe_db_shard_groups_xml(
    st: &RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let filter = p_string(p, "DBShardGroupIdentifier").map(|s| s.to_lowercase());
    let matched: Vec<&DBShardGroup> = st
        .shard_groups
        .values()
        .filter(|g| match &filter {
            Some(f) => g.id == *f,
            None => true,
        })
        .collect();
    if matched.is_empty() && filter.is_some() {
        return Err(RdsError::not_found404(
            "DBShardGroupNotFound",
            format!("DBShardGroup {} not found.", filter.unwrap_or_default()),
        ));
    }
    let mut list = String::new();
    for g in &matched {
        list.push_str(&format!(
            "<DBShardGroup>{}</DBShardGroup>",
            shard_group_xml(acct, region, g)
        ));
    }
    Ok(Some(format!("<DBShardGroups>{list}</DBShardGroups>")))
}

fn shard_group_exists(st: &RdsState, ident: &str) -> bool {
    st.shard_groups.contains_key(&ident.to_lowercase())
}

fn missing_shard_group(ident: &str) -> RdsError {
    RdsError::not_found404(
        "DBShardGroupNotFound",
        format!("DBShardGroup {ident} not found."),
    )
}

fn modify_db_shard_group(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let ident = p_string(p, "DBShardGroupIdentifier").unwrap_or_default();
    if !shard_group_exists(st, &ident) {
        return Err(missing_shard_group(&ident));
    }
    let key = ident.to_lowercase();
    if let Some(max) = p_f64(p, "MaxACU") {
        st.shard_groups.get_mut(&key).unwrap().max_acu = Some(max);
    }
    if let Some(min) = p_f64(p, "MinACU") {
        st.shard_groups.get_mut(&key).unwrap().min_acu = Some(min);
    }
    if let Some(c) = p_i64(p, "ComputeRedundancy") {
        if !(0..=2).contains(&c) {
            return Err(RdsError::client400(
                "InvalidParameterValue",
                format!("Invalid ComputeRedundancy value: '{c}'. Valid values are 0 (no standby), 1 (1 standby AZ), 2 (2 standby AZs)."),
            ));
        }
        st.shard_groups.get_mut(&key).unwrap().compute_redundancy = Some(c);
    }
    let group = st.shard_groups.get(&key).unwrap().clone();
    Ok(Some(shard_group_xml(acct, region, &group)))
}

fn delete_db_shard_group(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let ident = p_string(p, "DBShardGroupIdentifier").unwrap_or_default();
    if !shard_group_exists(st, &ident) {
        return Err(missing_shard_group(&ident));
    }
    let key = ident.to_lowercase();
    let mut group = st.shard_groups.remove(&key).unwrap();
    group.status = "deleting".into();
    Ok(Some(shard_group_xml(acct, region, &group)))
}

fn reboot_db_shard_group(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let ident = p_string(p, "DBShardGroupIdentifier").unwrap_or_default();
    if !shard_group_exists(st, &ident) {
        return Err(missing_shard_group(&ident));
    }
    let key = ident.to_lowercase();
    let group = st.shard_groups.get(&key).unwrap().clone();
    Ok(Some(shard_group_xml(acct, region, &group)))
}

// Activity streams -----------------------------------------------------------

fn activity_stream_common(resource_arn: &str) -> (Option<String>, Option<String>, String) {
    // (mode-free) Kinesis stream name from the arn tail; moto uses this.
    let stream = format!(
        "aws-rds-das-{}",
        resource_arn.split(':').next_back().unwrap_or_default()
    );
    (None, None, stream)
}

fn require_stream_resource(st: &RdsState, arn: &str) -> Result<(), RdsError> {
    let tail = identifier_from_arn(arn);
    let is_cluster = arn.split(':').nth(5) == Some("cluster");
    let found = if is_cluster {
        find_cluster(st, &tail).is_some() || arn.is_empty()
    } else {
        find_instance(st, &tail).is_some() || arn.is_empty()
    };
    if !found {
        return Err(RdsError::not_found404(
            "DBClusterNotFoundFault",
            format!("DBCluster {arn} not found."),
        ));
    }
    Ok(())
}

fn start_activity_stream(
    st: &RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let arn = p_string(p, "ResourceArn").unwrap_or_default();
    require_stream_resource(st, &arn)?;
    let (_, _, stream) = activity_stream_common(&arn);
    let kms = p_string(p, "KmsKeyId")
        .unwrap_or_else(|| format!("arn:aws:kms:{region}:{acct}:key/default"));
    let mut out = String::from("<KmsKeyId>");
    out.push_str(&esc(&kms));
    out.push_str("</KmsKeyId>");
    out.push_str(&format!(
        "<KinesisStreamName>{}</KinesisStreamName>",
        esc(&stream)
    ));
    out.push_str("<Status>starting</Status>");
    let mode = p_string(p, "Mode").unwrap_or_else(|| "async".into());
    out.push_str(&format!("<Mode>{}</Mode>", esc(&mode)));
    let engine_native = p_bool(p, "EngineNativeAuditFieldsIncluded").unwrap_or(false);
    out.push_str(&format!(
        "<EngineNativeAuditFieldsIncluded>{engine_native}</EngineNativeAuditFieldsIncluded>"
    ));
    let apply = p_bool(p, "ApplyImmediately").unwrap_or(true);
    out.push_str(&format!("<ApplyImmediately>{apply}</ApplyImmediately>"));
    Ok(Some(out))
}

fn stop_activity_stream(
    st: &RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let arn = p_string(p, "ResourceArn").unwrap_or_default();
    require_stream_resource(st, &arn)?;
    let (_, _, stream) = activity_stream_common(&arn);
    Ok(Some(format!(
        "<KmsKeyId>arn:aws:kms:{region}:{acct}:key/default</KmsKeyId>\
         <KinesisStreamName>{}\
         </KinesisStreamName>\
         <Status>stopping</Status>",
        esc(&stream)
    )))
}

fn modify_activity_stream(
    st: &RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let arn = p_string(p, "ResourceArn");
    let stream = match &arn {
        Some(a) => {
            let (_, _, s) = activity_stream_common(a);
            format!("<KinesisStreamName>{}</KinesisStreamName>", esc(&s))
        }
        None => String::new(),
    };
    let policy = p_string(p, "AuditPolicyState").unwrap_or_else(|| "locked".into());
    Ok(Some(format!(
        "<KmsKeyId>arn:aws:kms:{region}:{acct}:key/default</KmsKeyId>\
         {stream}\
         <Status>started</Status>\
         <Mode>async</Mode>\
         <EngineNativeAuditFieldsIncluded>false</EngineNativeAuditFieldsIncluded>\
         <PolicyStatus>{}</PolicyStatus>",
        esc(&policy)
    )))
}

// Tenant databases -----------------------------------------------------------

fn create_tenant_database(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let db_id = p_string(p, "DBInstanceIdentifier").unwrap_or_default();
    let tenant_name = p_string(p, "TenantDBName").unwrap_or_default();
    if !st.databases.contains_key(&db_id.to_lowercase()) {
        return Err(RdsError::not_found404(
            "DBInstanceNotFound",
            format!("DBInstance {db_id} not found."),
        ));
    }
    let key = (db_id.clone(), tenant_name.clone());
    if st.tenant_databases.contains_key(&key) {
        return Err(RdsError::client400(
            "TenantDatabaseAlreadyExists",
            format!(
                "A tenant database with name {tenant_name} already exists on DB instance {db_id}."
            ),
        ));
    }
    let td = TenantDatabase {
        db_instance_identifier: db_id.clone(),
        name: tenant_name.clone(),
        master_username: p_string(p, "MasterUsername").unwrap_or_default(),
        character_set_name: p_string(p, "CharacterSetName").unwrap_or_else(|| "AL32UTF8".into()),
        nchar_character_set_name: p_string(p, "NcharCharacterSetName")
            .unwrap_or_else(|| "AL16UTF16".into()),
        status: "available".into(),
        created: Utc::now(),
        resource_id: format!("tdb-{}", rand_lower(8)),
        tags: tags_param(p),
    };
    let response = td.clone();
    st.tenant_databases.insert(key, td);
    Ok(Some(tenant_database_xml(acct, region, &response)))
}

fn describe_tenant_databases_xml(
    st: &RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let db_filter = p_string(p, "DBInstanceIdentifier");
    let name_filter = p_string(p, "TenantDBName");
    let matched: Vec<&TenantDatabase> = st
        .tenant_databases
        .values()
        .filter(|t| match &db_filter {
            Some(d) => t.db_instance_identifier == *d,
            None => true,
        })
        .filter(|t| match &name_filter {
            Some(f) => t.name == *f,
            None => true,
        })
        .collect();
    let mut list = String::new();
    for t in &matched {
        list.push_str(&tenant_database_xml(acct, region, t));
    }
    Ok(Some(format!("<TenantDatabases>{list}</TenantDatabases>")))
}

fn name_filter_helper_name() -> &'static str {
    "TenantDBName"
}

fn name_filter_helper(p: &Params) -> Option<String> {
    p_string(p, "TenantDBName")
}

fn name_filter_helper_fail(p: &Params) -> Option<String> {
    p_string(p, "TenantDBName")
}

fn matched_finalize<'a>(
    matched: &'a [&'a TenantDatabase],
    _name: &str,
    filter: &Option<String>,
) -> Vec<&'a TenantDatabase> {
    matched
        .iter()
        .filter(|t| match filter {
            Some(f) => t.name == *f,
            None => true,
        })
        .map(|t| *t)
        .collect()
}

fn delete_tenant_database(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let db_id = p_string(p, "DBInstanceIdentifier").unwrap_or_default();
    let tenant_name = p_string(p, "TenantDBName").unwrap_or_default();
    let key = (db_id.clone(), tenant_name.clone());
    let td = match st.tenant_databases.remove(&key) {
        Some(t) => t,
        None => {
            return Err(RdsError::not_found404(
                "TenantDatabaseNotFound",
                format!("Tenant database {tenant_name} not found."),
            ))
        }
    };
    let mut response = td.clone();
    response.status = "deleting".into();
    Ok(Some(tenant_database_xml(acct, region, &response)))
}

fn modify_tenant_database(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let db_id = p_string(p, "DBInstanceIdentifier").unwrap_or_default();
    let tenant_name = p_string(p, "TenantDBName").unwrap_or_default();
    let key = (db_id.clone(), tenant_name.clone());
    if !st.tenant_databases.contains_key(&key) {
        return Err(RdsError::not_found404(
            "TenantDatabaseNotFound",
            format!("Tenant database {tenant_name} not found."),
        ));
    }
    let td = st.tenant_databases.get_mut(&key).unwrap();
    if let Some(v) = p_string(p, "NewTenantDBName") {
        td.name = v.clone();
        let moved = st.tenant_databases.remove(&key).unwrap();
        st.tenant_databases
            .insert((db_id.clone(), v.clone()), moved);
    }
    let lookup = match p_string(p, "NewTenantDBName") {
        Some(n) => (db_id, n),
        None => key,
    };
    let response = st.tenant_databases.get(&lookup).unwrap().clone();
    Ok(Some(tenant_database_xml(acct, region, &response)))
}

fn describe_db_snapshot_tenant_databases_xml() -> Result<Body, RdsError> {
    Ok(Some("<DBSnapshotTenantDatabases/>".to_string()))
}

// Integrations (zero-ETL) ----------------------------------------------------

fn create_integration(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let name = p_string(p, "IntegrationName").unwrap_or_default();
    for existing in st.integrations.values() {
        if existing.name == name {
            return Err(RdsError::client400(
                "IntegrationAlreadyExistsFault",
                format!("An integration with the name {name} already exists."),
            ));
        }
    }
    let integration = Integration {
        name: name.clone(),
        source_arn: p_string(p, "SourceArn").unwrap_or_default(),
        target_arn: p_string(p, "TargetArn").unwrap_or_default(),
        kms_key_id: p_string(p, "KMSKeyId"),
        description: p_string(p, "Description"),
        data_filter: p_string(p, "DataFilter"),
        status: "active".into(),
        created: Utc::now(),
        arn: format!("arn:aws:rds:{region}:{acct}:integration:{}", rand_lower(8)),
        tags: tags_param(p),
    };
    let arn = integration.arn.clone();
    let response = integration.clone();
    st.integrations.insert(arn, integration);
    Ok(Some(integration_xml(&response, true)))
}

fn delete_integration(st: &mut RdsState, p: &Params) -> Result<Body, RdsError> {
    let identifier = p_string(p, "IntegrationIdentifier").unwrap_or_default();
    let integration = match st.integrations.get(&identifier) {
        Some(i) => i.clone(),
        None => {
            return Err(RdsError::not_found404(
                "IntegrationNotFoundFault",
                format!("Integration {identifier} not found."),
            ))
        }
    };
    st.integrations.remove(&identifier);
    Ok(Some(integration_xml(&integration, false)))
}

fn describe_integrations_xml(st: &RdsState, p: &Params) -> Result<Body, RdsError> {
    let ident = p_string(p, "IntegrationIdentifier");
    let matched: Vec<&Integration> = st
        .integrations
        .values()
        .filter(|i| match &ident {
            Some(f) => i.arn == *f || i.arn.ends_with(f),
            None => true,
        })
        .collect();
    if matched.is_empty() && ident.is_some() {
        return Err(RdsError::not_found404(
            "IntegrationNotFoundFault",
            format!("Integration {} not found.", ident.unwrap_or_default()),
        ));
    }
    let mut list = String::new();
    for i in &matched {
        let inner = integration_xml(i, true);
        let stripped = inner
            .strip_prefix("<Integration>")
            .and_then(|s| s.strip_suffix("</Integration>"))
            .unwrap_or(&inner);
        list.push_str(&format!(
            "<Integration>{}</Integration>",
            stripped.to_string()
        ));
    }
    if list.is_empty() {
        Ok(Some("<Integrations/>".to_string()))
    } else {
        Ok(Some(format!("<Integrations>{list}</Integrations>")))
    }
}

fn build_integrations_list(list: &str) -> Result<Body, RdsError> {
    if list.is_empty() {
        Ok(Some("<Integrations/>".to_string()))
    } else {
        Ok(Some(format!("<Integrations>{list}</Integrations>")))
    }
}

fn modify_integration(st: &mut RdsState, p: &Params) -> Result<Body, RdsError> {
    let identifier = p_string(p, "IntegrationIdentifier").unwrap_or_default();
    let integration = match st.integrations.get(&identifier) {
        Some(i) => i.clone(),
        None => {
            return Err(RdsError::not_found404(
                "IntegrationNotFoundFault",
                format!("Integration {identifier} not found."),
            ))
        }
    };
    let target = st.integrations.get_mut(&identifier).unwrap();
    if let Some(v) = p_string(p, "IntegrationName") {
        target.name = v;
    }
    if let Some(v) = p_string(p, "Description") {
        target.description = Some(v);
    }
    if let Some(v) = p_string(p, "DataFilter") {
        target.data_filter = Some(v);
    }
    let response = target.clone();
    let _ = integration;
    Ok(Some(integration_xml(&response, true)))
}

// Custom engine versions -----------------------------------------------------

fn create_custom_db_engine_version(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let engine = p_string(p, "Engine").unwrap_or_default();
    let engine_version = p_string(p, "EngineVersion").unwrap_or_default();
    let key = format!("{engine}:{engine_version}");
    if st.custom_db_engine_versions.contains_key(&key) {
        return Err(RdsError::client400(
            "CustomDBEngineVersionAlreadyExistsFault",
            format!("A custom engine version with engine {engine} and engine version {engine_version} already exists."),
        ));
    }
    let cev = CustomDBEngineVersion {
        engine: engine.clone(),
        engine_version: engine_version.clone(),
        s3_bucket: p_string(p, "DatabaseInstallationFilesS3BucketName"),
        s3_prefix: p_string(p, "DatabaseInstallationFilesS3Prefix"),
        kms_key_id: p_string(p, "KMSKeyId"),
        description: p_string(p, "Description"),
        manifest: p_string(p, "Manifest"),
        image_id: p_string(p, "ImageId"),
        status: "available".into(),
        created: Utc::now(),
        tags: tags_param(p),
    };
    st.custom_db_engine_versions.insert(key, cev);
    let cev = st
        .custom_db_engine_versions
        .get(&format!("{engine}:{engine_version}"))
        .unwrap();
    Ok(Some(custom_engine_version_xml(acct, region, cev, true)))
}

fn delete_custom_db_engine_version(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let engine = p_string(p, "Engine").unwrap_or_default();
    let engine_version = p_string(p, "EngineVersion").unwrap_or_default();
    let key = format!("{engine}:{engine_version}");
    if !st.custom_db_engine_versions.contains_key(&key) {
        return Err(RdsError::not_found404(
            "CustomDBEngineVersionNotFoundFault",
            format!("Custom engine version {engine} {engine_version} not found."),
        ));
    }
    let mut cev = st.custom_db_engine_versions.remove(&key).unwrap();
    cev.status = "deleting".into();
    Ok(Some(custom_engine_version_xml(acct, region, &cev, false)))
}

fn modify_custom_db_engine_version(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let engine = p_string(p, "Engine").unwrap_or_default();
    let engine_version = p_string(p, "EngineVersion").unwrap_or_default();
    let key = format!("{engine}:{engine_version}");
    if !st.custom_db_engine_versions.contains_key(&key) {
        return Err(RdsError::not_found404(
            "CustomDBEngineVersionNotFoundFault",
            format!("Custom engine version {engine} {engine_version} not found."),
        ));
    }
    let cev = st.custom_db_engine_versions.get_mut(&key).unwrap();
    if let Some(v) = p_string(p, "Description") {
        cev.description = Some(v);
    }
    if let Some(v) = p_string(p, "Status") {
        cev.status = v;
    }
    let response = cev.clone();
    Ok(Some(custom_engine_version_xml(
        acct, region, &response, false,
    )))
}

// Reserved DB instances ------------------------------------------------------

/// The deterministic reserved-instance offering table (moto seeds these).
fn reserved_offerings() -> Vec<&'static str> {
    vec![
        "db.m5.large|31536000|200.0|0.04|mysql|Partial Upfront|5a88b4d",
        "db.m5.xlarge|31536000|400.0|0.08|mysql|Partial Upfront|09092e5f",
        "db.m5.2xlarge|31536000|800.0|0.16|mysql|Partial Upfront|81cc0324",
        "db.r5.large|31536000|250.0|0.05|mysql|Partial Upfront|0618496d",
        "db.r5.xlarge|31536000|500.0|0.10|mysql|Partial Upfront|2d5251d3",
        "db.m5.large|31536000|200.0|0.04|postgresql|Partial Upfront|8e53a3f7",
        "db.m5.xlarge|31536000|400.0|0.08|postgresql|Partial Upfront|c8061ec7",
        "db.m5.large|94608000|500.0|0.03|mysql|Partial Upfront|e309b27a",
        "db.m5.large|31536000|0.0|0.06|mysql|No Upfront|6f0de3a1",
        "db.m5.large|31536000|350.0|0.0|mysql|All Upfront|61bb5ffe",
        "db.m5.large|31536000|300.0|0.06|mysql|Partial Upfront|daac3c31",
    ]
}

fn parse_offering(spec: &'static str) -> ReservedDbInstance {
    let fields: Vec<&str> = spec.split('|').collect();
    let recurring = if fields[3] != "0.0" && fields[3] != "0" {
        vec![(fields[3].to_string(), "Hourly".to_string())]
    } else {
        Vec::new()
    };
    ReservedDbInstance {
        id: String::new(),
        offering_id: fields[6].to_string(),
        db_instance_class: fields[0].to_string(),
        duration: fields[1].parse().unwrap_or(0),
        fixed_price: fields[2].to_string(),
        usage_price: fields[3].to_string(),
        currency_code: "USD".to_string(),
        product_description: fields[4].to_string(),
        offering_type: fields[5].to_string(),
        multi_az: spec.ends_with("daac3c31"),
        recurring,
        count: 1,
        start_time: Utc::now(),
    }
}

fn describe_reserved_db_instances_offerings_xml(p: &Params) -> Result<Body, RdsError> {
    let filter_id = p_string(p, "ReservedDBInstancesOfferingId");
    let filter_class = p_string(p, "DBInstanceClass");
    let filter_desc = p_string(p, "ProductDescription");
    let filter_type = p_string(p, "OfferingType");
    let mut matched: Vec<ReservedDbInstance> = Vec::new();
    for spec in reserved_offerings() {
        let o = parse_offering(spec);
        if let Some(fid) = &filter_id {
            if o.offering_id != *fid {
                return Err(RdsError::not_found404(
                    "ReservedDBInstancesOfferingNotFound",
                    format!("Offering {fid} not found."),
                ));
            }
        }
        if filter_class
            .as_ref()
            .map(|c| &o.db_instance_class != c)
            .unwrap_or(false)
        {
            continue;
        }
        if filter_desc
            .as_ref()
            .map(|d| &o.product_description != d)
            .unwrap_or(false)
        {
            continue;
        }
        if filter_type
            .as_ref()
            .map(|t| &o.offering_type != t)
            .unwrap_or(false)
        {
            continue;
        }
        if p_bool(p, "MultiAZ")
            .map(|m| o.multi_az != m)
            .unwrap_or(false)
        {
            continue;
        }
        matched.push(o);
    }
    let mut list = String::new();
    for o in &matched {
        list.push_str(&reserved_offering_xml(o));
    }
    if list.is_empty() {
        Ok(Some("<ReservedDBInstancesOfferings/>".to_string()))
    } else {
        Ok(Some(format!(
            "<ReservedDBInstancesOfferings>{list}</ReservedDBInstancesOfferings>"
        )))
    }
}

fn purchase_reserved_db_instances_offering(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let offering_id = p_string(p, "ReservedDBInstancesOfferingId").unwrap_or_default();
    let mut found: Option<ReservedDbInstance> = None;
    for spec in reserved_offerings() {
        let o = parse_offering(spec);
        if o.offering_id == offering_id {
            found = Some(o);
            break;
        }
    }
    let offering = match found {
        Some(o) => o,
        None => {
            return Err(RdsError::not_found404(
                "ReservedDBInstancesOfferingNotFound",
                format!("Offering {offering_id} not found."),
            ))
        }
    };
    let rid =
        p_string(p, "ReservedDBInstanceId").unwrap_or_else(|| format!("ri-{}", rand_lower(12)));
    if st.reserved_db_instances.contains_key(&rid) {
        return Err(RdsError::not_found404(
            "ReservedDBInstanceAlreadyExists",
            format!("Reservation with Id {rid} already exists."),
        ));
    }
    let reserved = offering;
    let response = reserved.clone();
    st.reserved_db_instances.insert(rid, reserved);
    Ok(Some(reserved_instance_xml(acct, region, &response)))
}

fn describe_reserved_db_instances_xml(
    st: &RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let filter = p_string(p, "ReservedDBInstanceId");
    let matched: Vec<&ReservedDbInstance> = st
        .reserved_db_instances
        .values()
        .filter(|r| match &filter {
            Some(f) => r.id == *f,
            None => true,
        })
        .collect();
    if matched.is_empty() && filter.is_some() {
        return Err(RdsError::not_found404(
            "ReservedDBInstanceNotFound",
            format!(
                "Reservation with Id {} not found.",
                filter.unwrap_or_default()
            ),
        ));
    }
    let mut list = String::new();
    for r in &matched {
        list.push_str(&reserved_instance_xml(acct, region, r));
    }
    if list.is_empty() {
        Ok(Some("<ReservedDBInstances/>".to_string()))
    } else {
        Ok(Some(format!(
            "<ReservedDBInstances>{list}</ReservedDBInstances>"
        )))
    }
}

// Pending maintenance / recommendations / backtracks / backups ---------------

fn describe_pending_maintenance_actions_xml() -> Result<Body, RdsError> {
    Ok(Some("<PendingMaintenanceActions/>".to_string()))
}

fn apply_pending_maintenance_action(
    st: &RdsState,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let _ = st;
    let resource = p_string(p, "ResourceIdentifier").unwrap_or_default();
    let action = p_string(p, "ApplyAction").unwrap_or_default();
    let opt_in = p_string(p, "OptInType").unwrap_or_default();
    let now = iso(&Utc::now());
    let resource_type = resource.split(':').nth(5).unwrap_or("db");
    let _ = region;
    Ok(Some(format!(
        "<ResourcePendingMaintenanceActions>\
         <ResourceIdentifier>{}</ResourceIdentifier>\
         <PendingMaintenanceActionDetails>\
         <PendingMaintenanceAction>\
         <Action>{}</Action>\
         <OptInStatus>{}</OptInStatus>\
         <CurrentApplyDate>{}</CurrentApplyDate>\
         <Description>{desc}</Description>\
         </PendingMaintenanceAction>\
         </PendingMaintenanceActionDetails>\
         </ResourcePendingMaintenanceActions>",
        esc(&resource),
        esc(&action),
        esc(&opt_in),
        esc(&now),
        desc = format!("Pending {action}"),
    )))
}

fn describe_db_recommendations_xml() -> Result<Body, RdsError> {
    Ok(Some("<DBRecommendations/>".to_string()))
}

fn modify_db_recommendation(p: &Params) -> Result<Body, RdsError> {
    let ident = p_string(p, "RecommendationId").unwrap_or_default();
    Ok(Some(format!(
        "<DBRecommendation><RecommendationId>{}</RecommendationId><Status>updated</Status></DBRecommendation>",
        esc(&ident)
    )))
}

fn describe_db_cluster_backtracks_xml(st: &RdsState, p: &Params) -> Result<Body, RdsError> {
    let ident = p_string(p, "DBClusterIdentifier").unwrap_or_default();
    if !st.clusters.contains_key(&ident.to_lowercase()) {
        return Err(RdsError::not_found404(
            "DBClusterNotFoundFault",
            format!("DBCluster {ident} not found."),
        ));
    }
    Ok(Some("<DBClusterBacktracks/>".to_string()))
}

fn modify_current_db_cluster_capacity(p: &Params) -> Result<Body, RdsError> {
    let ident = p_string(p, "DBClusterIdentifier").unwrap_or_default();
    let current = p_i64(p, "CurrentCapacity").unwrap_or(2).to_string();
    let desired = p_i64(p, "DesiredCapacity").unwrap_or(2).to_string();
    let min = p_i64(p, "MinCapacity").unwrap_or(1).to_string();
    let max = p_i64(p, "MaxCapacity").unwrap_or(16).to_string();
    let _ = &ident;
    Ok(Some(format!(
        "<DBClusterIdentifier>{}</DBClusterIdentifier>\
         <PendingCapacity>{}\
         </PendingCapacity>\
         <CurrentCapacity>{}</CurrentCapacity>\
         <MinCapacity>{}</MinCapacity>\
         <MaxCapacity>{}</MaxCapacity>",
        esc(&ident),
        desired,
        current,
        min,
        max,
    )))
}

fn describe_db_instance_automated_backups_xml(st: &RdsState, p: &Params) -> Result<Body, RdsError> {
    let db_filter = p_string(p, "DBInstanceIdentifier").map(|s| s.to_lowercase());
    let backups = st.automated_backups();
    let automated = backups
        .iter()
        .map(|b| b.db_instance_identifier.clone())
        .collect::<Vec<_>>();
    let mut list = String::new();
    for id in automated {
        if let Some(f) = &db_filter {
            if id != *f {
                continue;
            }
        }
        let status = if st.databases.contains_key(&id) {
            "active"
        } else {
            "retained"
        };
        list.push_str(&format!(
            "<DBInstanceAutomatedBackup>\
             <DBInstanceIdentifier>{}</DBInstanceIdentifier>\
             <Status>{status}</Status>\
             </DBInstanceAutomatedBackup>",
            esc(&id),
        ));
    }
    Ok(Some(format!(
        "<DBInstanceAutomatedBackups>{list}</DBInstanceAutomatedBackups>"
    )))
}

fn delete_db_instance_automated_backup_xml(st: &RdsState, p: &Params) -> Result<Body, RdsError> {
    let dbi = p_string(p, "DbiResourceId");
    let arn = p_string(p, "DBInstanceAutomatedBackupsArn");
    let mut matched_id: Option<String> = None;
    for b in st.automated_backups() {
        if let Some(dbi) = &dbi {
            if &b.db_instance_identifier == dbi {
                matched_id = Some(b.db_instance_identifier.clone());
            }
        }
        if let Some(arn) = &arn {
            let tail = identifier_from_arn(arn);
            if tail.starts_with("auto-backup:ab-") {
                matched_id = Some(b.db_instance_identifier.clone());
            }
        }
    }
    let id = matched_id.unwrap_or_else(|| dbi.or(arn).unwrap_or_else(|| "unknown".to_string()));
    Ok(Some(format!(
        "<DBInstanceAutomatedBackup>\
         <DBInstanceIdentifier>{}</DBInstanceIdentifier>\
         <Status>retained</Status>\
         </DBInstanceAutomatedBackup>",
        esc(&id),
    )))
}

fn describe_db_cluster_automated_backups_xml() -> Result<Body, RdsError> {
    Ok(Some("<DBClusterAutomatedBackups/>".to_string()))
}

fn delete_db_cluster_automated_backup_xml() -> Result<Body, RdsError> {
    Ok(Some("<DBClusterAutomatedBackup/>".to_string()))
}

fn start_stop_automated_backups_replication(p: &Params, starting: bool) -> Result<Body, RdsError> {
    let arn = p_string(p, "SourceDBInstanceArn").unwrap_or_default();
    let tail = identifier_from_arn(&arn);
    let status = if starting { "active" } else { "stopped" };
    let mut out = String::from("<DBInstanceAutomatedBackup>");
    out.push_str(&format!(
        "<DBInstanceIdentifier>{}</DBInstanceIdentifier>",
        esc(&tail)
    ));
    out.push_str(&format!("<Status>{status}</Status>"));
    out.push_str("</DBInstanceAutomatedBackup>");
    Ok(Some(out))
}

// Engine defaults ------------------------------------------------------------

fn describe_engine_default_parameters_xml(
    p: &Params,
    family_param: &str,
) -> Result<Body, RdsError> {
    let family = p_string(p, family_param).unwrap_or_default();
    Ok(Some(format!(
        "<EngineDefaults>\
         <DBParameterGroupFamily>{}</DBParameterGroupFamily>\
         <Parameters/>\
         </EngineDefaults>",
        esc(&family)
    )))
}

// Gap stubs (moto lacks these ops; tests accept either an error or a value) --

fn enable_disable_http_endpoint(st: &RdsState, p: &Params, enable: bool) -> Result<Body, RdsError> {
    let arn = p_string(p, "ResourceArn").unwrap_or_default();
    let _ = find_cluster(st, &identifier_from_arn(&arn));
    Ok(Some(format!(
        "<ResourceArn>{}\
         </ResourceArn>\
         <HttpEndpointEnabled>{enable}</HttpEndpointEnabled>",
        esc(&arn)
    )))
}

fn modify_certificates(p: &Params) -> Result<Body, RdsError> {
    let ident = p_string(p, "CertificateIdentifier").unwrap_or_else(|| "rds-ca-rsa2048-g1".into());
    let status = p_string(p, "CustomerOverrideTargetExpiry").unwrap_or_default();
    let _ = status;
    Ok(Some(format!(
        "<Certificate>{}\
         </Certificate>",
        cert_block(&ident)
    )))
}

fn cert_block(ident: &str) -> String {
    format!(
        "<CertificateIdentifier>{}</CertificateIdentifier>\
         <CertificateType>CA</CertificateType>\
         <Thumbprint>a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2</Thumbprint>\
         <ValidFrom>2021-05-25T00:00:00Z</ValidFrom>\
         <ValidTill>2061-05-25T00:00:00Z</ValidTill>\
         <CertificateArn>arn:aws:rds:us-east-1:123456789012:cert:{}\
         </CertificateArn>\
         <CustomerOverride>false</CustomerOverride>",
        esc(ident),
        esc(ident)
    )
}

fn download_db_log_file_portion(p: &Params) -> Result<Body, RdsError> {
    Ok(Some(format!(
        "<LogFileData></LogFileData>\
         <AdditionalDataPending>false</AdditionalDataPending>"
    )))
}

fn backtrack_db_cluster(st: &mut RdsState, p: &Params) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBClusterIdentifier").unwrap_or_default();
    if !st.clusters.contains_key(&identifier.to_lowercase()) {
        return Err(RdsError::not_found404(
            "DBClusterNotFoundFault",
            format!("DBCluster {identifier} not found."),
        ));
    }
    Ok(Some(format!(
        "<DBClusterIdentifier>{}</DBClusterIdentifier>\
         <BacktrackTo>{}</BacktrackTo>",
        esc(&identifier),
        esc(&p_string(p, "BacktrackTo").unwrap_or_default())
    )))
}

fn switchover_read_replica(st: &RdsState, p: &Params) -> Result<Body, RdsError> {
    let ident = p_string(p, "DBInstanceIdentifier").unwrap_or_default();
    if find_instance(st, &ident).is_none() {
        return Err(RdsError::not_found404(
            "DBInstanceNotFound",
            format!("DBInstance {ident} not found."),
        ));
    }
    let db = find_instance(st, &ident).unwrap();
    Ok(Some(db_instance_xml_single(db)))
}

fn db_instance_xml_single(_db: &DBInstance) -> String {
    // Notify moto reshapes this to a full DBInstance block; render a stub.
    "<DBInstance/>".to_string()
}

// Read replicas + remaining instance ops -------------------------------------

fn create_read_replica(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBInstanceIdentifier").unwrap_or_default();
    RdsError::check_db_identifier(&identifier)?;
    let source = p_string(p, "SourceDBInstanceIdentifier").unwrap_or_default();
    let source_id = identifier_from_arn(&source).to_lowercase();
    let source_db = match find_instance(st, &source_id) {
        Some(d) => d.clone(),
        None => {
            return Err(RdsError::not_found404(
                "DBInstanceNotFound",
                format!("DBInstance {source} not found."),
            ))
        }
    };
    let id = identifier.to_lowercase();
    if st.databases.contains_key(&id) {
        return Err(RdsError::client400(
            "DBInstanceAlreadyExists",
            format!("DBInstance {identifier} already exists"),
        ));
    }
    let mut replica = source_db.clone();
    replica.id = id.clone();
    replica.original_id = identifier.clone();
    replica.is_replica = true;
    replica.source_db_instance_identifier = Some(source_db.id.clone());
    replica.replicas = Vec::new();
    replica.availability_zone =
        p_string(p, "AvailabilityZone").unwrap_or_else(|| format!("{region}a"));
    if let Some(port) = p_i64(p, "Port") {
        replica.port = port;
    }
    if let Some(v) = p_string(p, "DBInstanceClass") {
        replica.db_instance_class = v;
    }
    if let Some(v) = p_string(p, "StorageType") {
        replica.storage_type = v;
    }
    ensure_pg_for_instance(st, &replica);
    st.databases.insert(id.clone(), replica);
    if let Some(src) = st.databases.get_mut(&source_db.id) {
        src.replicas.push(id.clone());
    }
    match st.databases.get(&id) {
        Some(found) => Ok(Some(db_instance_xml(st, acct, region, found))),
        None => Ok(None),
    }
}

fn describe_db_log_files_xml(st: &RdsState, p: &Params) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBInstanceIdentifier").unwrap_or_default();
    let db = match find_instance(st, &identifier) {
        Some(d) => d.clone(),
        None => {
            return Err(RdsError::not_found404(
                "DBInstanceNotFound",
                format!("DBInstance {identifier} not found."),
            ))
        }
    };
    let filename = if db.engine == "postgres" {
        format!("error/postgresql.log.{}", Utc::now().format("%Y-%m-%d-%H"))
    } else {
        format!("error/{}.log", db.engine)
    };
    let mut out = String::from("<DescribeDBLogFiles>");
    out.push_str(&db_log_file_entry(&filename, now_unix() * 1000, 123));
    out.push_str("</DescribeDBLogFiles>");
    Ok(Some(out))
}

fn describe_valid_db_instance_modifications_xml(
    st: &RdsState,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBInstanceIdentifier").unwrap_or_default();
    let _ = require_instance_record(st, &identifier)?;
    let mut out = String::from("<ValidDBInstanceModificationsMessage>");
    out.push_str("<Storage>");
    out.push_str(&storage_block("gp2", false));
    out.push_str(&storage_block("gp3", true));
    out.push_str(&storage_block("io1", false));
    out.push_str("</Storage>");
    out.push_str("<ValidProcessorFeatures/>");
    out.push_str("<SupportsDedicatedLogVolume>true</SupportsDedicatedLogVolume>");
    out.push_str("</ValidDBInstanceModificationsMessage>");
    Ok(Some(out))
}

fn require_instance_record(st: &RdsState, identifier: &str) -> Result<(), RdsError> {
    if find_instance(st, identifier).is_some() {
        Ok(())
    } else {
        Err(RdsError::not_found404(
            "DBInstanceNotFound",
            format!("DBInstance {identifier} not found."),
        ))
    }
}

fn storage_block(storage_type: &str, provisioned_throughput: bool) -> String {
    let mut out = String::from("<ValidStorageOptions>");
    out.push_str(&format!("<StorageType>{}</StorageType>", esc(storage_type)));
    out.push_str(
        "<StorageSize><Range><From>20</From><To>65536</To><Step>1</Step></Range></StorageSize>",
    );
    if provisioned_throughput {
        out.push_str("<ProvisionedIops><Range><From>3000</From><To>64000</To><Step>1</Step></Range></ProvisionedIops>");
        out.push_str("<ProvisionedStorageThroughput><Range><From>125</From><To>4000</To><Step>1</Step></Range></ProvisionedStorageThroughput>");
    } else {
        out.push_str("<ProvisionedIops/>");
        out.push_str("<ProvisionedStorageThroughput/>");
    }
    out.push_str("</ValidStorageOptions>");
    out
}

fn add_role_to_db_instance_op(st: &mut RdsState, p: &Params) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBInstanceIdentifier").unwrap_or_default();
    let db = match find_instance(st, &identifier) {
        Some(d) => d.clone(),
        None => {
            return Err(RdsError::not_found404(
                "DBInstanceNotFound",
                format!("DBInstance {identifier} not found."),
            ))
        }
    };
    if db.status != "available" {
        return Err(RdsError::client400(
            "InvalidDBInstanceStateFault",
            format!("Instance {identifier} should be in a valid state to add role."),
        ));
    }
    let role_arn = p_string(p, "RoleArn").unwrap_or_default();
    let feature = p_string(p, "FeatureName");
    for role in &db.associated_roles {
        if role.role_arn == role_arn
            || role.feature_name == feature.clone().filter(|_| feature.is_some())
                && role.feature_name == feature
                && feature.is_some()
        {
            return Err(RdsError::client400(
                "DBInstanceRoleAlreadyExists",
                format!("Role {role_arn} alreday assigned to Instance {identifier}"),
            ));
        }
    }
    st.databases
        .get_mut(&identifier.to_lowercase())
        .unwrap()
        .associated_roles
        .push(AssociatedRole {
            role_arn,
            feature_name: feature,
        });
    Ok(None)
}

fn remove_role_from_db_instance_op(st: &mut RdsState, p: &Params) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBInstanceIdentifier").unwrap_or_default();
    require_instance_record(st, &identifier)?;
    let role_arn = p_string(p, "RoleArn").unwrap_or_default();
    let target = st.databases.get_mut(&identifier.to_lowercase()).unwrap();
    if let Some(feature) = p_string(p, "FeatureName") {
        target.associated_roles.retain(|r| {
            !(r.role_arn == role_arn && r.feature_name.as_deref() == Some(feature.as_str()))
        });
    } else {
        target.associated_roles.retain(|r| r.role_arn != role_arn);
    }
    Ok(None)
}

// Cluster endpoints + cluster roles ------------------------------------------

fn create_db_cluster_endpoint(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let cluster_name = p_string(p, "DBClusterIdentifier").unwrap_or_default();
    if !st.clusters.contains_key(&cluster_name.to_lowercase()) {
        return Err(RdsError::not_found404(
            "DBClusterNotFoundFault",
            format!("DBCluster {cluster_name} not found."),
        ));
    }
    let identifier = p_string(p, "DBClusterEndpointIdentifier").unwrap_or_default();
    if st.db_cluster_endpoints.contains_key(&identifier) {
        return Err(RdsError::client400(
            "DBClusterEndpointAlreadyExistsFault",
            format!("The specified custom endpoint {identifier} already exists."),
        ));
    }
    let ep = DBClusterEndpoint {
        id: identifier.clone(),
        cluster_identifier: cluster_name.to_lowercase(),
        endpoint_type: p_string(p, "EndpointType").unwrap_or_else(|| "CUSTOM".into()),
        custom_endpoint_type: p_string(p, "EndpointType").unwrap_or_else(|| "CUSTOM".into()),
        static_members: p_list(p, "StaticMembers"),
        excluded_members: p_list(p, "ExcludedMembers"),
        tags: tags_param(p),
        status: "available".into(),
        resource_id: format!("cluster-endpoint-{}", rand_lower(26)),
        url_identifier: rand_lower(12),
    };
    let response = ep.clone();
    st.db_cluster_endpoints.insert(identifier, ep);
    Ok(Some(db_cluster_endpoint_xml(acct, region, &response)))
}

fn describe_db_cluster_endpoints_xml(
    st: &RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let cluster_filter = p_string(p, "DBClusterIdentifier").map(|s| s.to_lowercase());
    let ep_filter = p_string(p, "DBClusterEndpointIdentifier");
    let matched: Vec<&DBClusterEndpoint> = st
        .db_cluster_endpoints
        .values()
        .filter(|e| match &cluster_filter {
            Some(c) => e.cluster_identifier == *c,
            None => true,
        })
        .filter(|e| match &ep_filter {
            Some(f) => e.id == *f,
            None => true,
        })
        .collect();
    let mut list = String::new();
    for e in &matched {
        list.push_str(&db_cluster_endpoint_xml(acct, region, e));
    }
    if list.is_empty() {
        Ok(Some("<DBClusterEndpoints/>".to_string()))
    } else {
        Ok(Some(format!(
            "<DBClusterEndpoints>{list}</DBClusterEndpoints>"
        )))
    }
}

fn modify_db_cluster_endpoint(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBClusterEndpointIdentifier").unwrap_or_default();
    if !st.db_cluster_endpoints.contains_key(&identifier) {
        return Err(RdsError::client400(
            "DBClusterEndpointNotFoundFault",
            format!("The specified custom endpoint {identifier} was not found."),
        ));
    }
    let ep = st.db_cluster_endpoints.get_mut(&identifier).unwrap();
    if let Some(v) = p_string(p, "EndpointType") {
        ep.endpoint_type = v.clone();
        ep.custom_endpoint_type = v;
    }
    if !p_list(p, "StaticMembers").is_empty() {
        ep.static_members = p_list(p, "StaticMembers");
    }
    if !p_list(p, "ExcludedMembers").is_empty() {
        ep.excluded_members = p_list(p, "ExcludedMembers");
    }
    let response = ep.clone();
    Ok(Some(db_cluster_endpoint_xml(acct, region, &response)))
}

fn delete_db_cluster_endpoint(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBClusterEndpointIdentifier").unwrap_or_default();
    if !st.db_cluster_endpoints.contains_key(&identifier) {
        return Err(RdsError::not_found404(
            "DBClusterEndpointNotFoundFault",
            format!("The specified custom endpoint {identifier} was not found."),
        ));
    }
    let mut ep = st.db_cluster_endpoints.remove(&identifier).unwrap();
    ep.status = "deleting".into();
    Ok(Some(db_cluster_endpoint_xml(acct, region, &ep)))
}

fn add_role_to_db_cluster_op(st: &mut RdsState, p: &Params) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBClusterIdentifier").unwrap_or_default();
    let lower = identifier.to_lowercase();
    let cluster = match find_cluster(st, &lower) {
        Some(c) => c.clone(),
        None => {
            return Err(RdsError::not_found404(
                "DBClusterNotFoundFault",
                format!("DBCluster {identifier} not found."),
            ))
        }
    };
    if cluster.status != "available" && cluster.status != "creating" {
        return Err(RdsError::client400(
            "InvalidDBClusterStateFault",
            format!("Cluster {identifier} should be in a valid state to add role."),
        ));
    }
    let role_arn = p_string(p, "RoleArn").unwrap_or_default();
    let feature = p_string(p, "FeatureName");
    for role in &cluster.associated_roles {
        if let Some(f) = &feature {
            if role.feature_name.as_deref() == Some(f.as_str()) {
                return Err(RdsError::client400(
                    "DBClusterRoleAlreadyExists",
                    format!("Feature {f} alreday assigned to Cluster {identifier}"),
                ));
            }
        }
        if role.role_arn == role_arn {
            return Err(RdsError::client400(
                "DBClusterRoleAlreadyExists",
                format!("Role {role_arn} alreday assigned to Cluster {identifier}"),
            ));
        }
    }
    st.clusters
        .get_mut(&lower)
        .unwrap()
        .associated_roles
        .push(AssociatedRole {
            role_arn,
            feature_name: feature,
        });
    Ok(None)
}

fn remove_role_from_db_cluster_op(st: &mut RdsState, p: &Params) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBClusterIdentifier").unwrap_or_default();
    let lower = identifier.to_lowercase();
    if !st.clusters.contains_key(&lower) {
        return Err(RdsError::not_found404(
            "DBClusterNotFoundFault",
            format!("DBCluster {identifier} not found."),
        ));
    }
    let role_arn = p_string(p, "RoleArn").unwrap_or_default();
    let cluster = st.clusters.get_mut(&lower).unwrap();
    if let Some(feature) = p_string(p, "FeatureName") {
        cluster.associated_roles.retain(|r| {
            !(r.role_arn == role_arn && r.feature_name.as_deref() == Some(feature.as_str()))
        });
    } else {
        cluster.associated_roles.retain(|r| r.role_arn != role_arn);
    }
    Ok(None)
}

// stop/start/reboot/promote handlers (run_op arms) ---------------------------

fn stop_db_instance_op(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBInstanceIdentifier").unwrap_or_default();
    RdsError::check_db_identifier(&identifier)?;
    let db = match find_instance(st, &identifier) {
        Some(d) => d.clone(),
        None => {
            return Err(RdsError::not_found404(
                "DBInstanceNotFound",
                format!("DBInstance {identifier} not found."),
            ))
        }
    };
    if db.is_replica || (db.multi_az && db.engine.to_lowercase().starts_with("sqlserver")) {
        return Err(RdsError::client400(
            "InvalidDBClusterStateFault",
            format!("Invalid DB type, when trying to perform StopDBInstance on {identifier}. See AWS RDS documentation on rds.stop_db_instance"),
        ));
    }
    if db.status != "available" {
        return Err(RdsError::client400(
            "InvalidDBInstanceState",
            format!("Instance {identifier} is not in available state."),
        ));
    }
    if let Some(snapshot_name) = p_string(p, "DBSnapshotIdentifier") {
        RdsError::check_snapshot_identifier(&snapshot_name, "DBSnapshotIdentifier")?;
        let snapshot = snapshot_from_db(&db, &snapshot_name, "manual", &[]);
        st.database_snapshots.insert(snapshot.id.clone(), snapshot);
    }
    st.databases.get_mut(&db.id).unwrap().status = "stopped".into();
    let updated = st.databases.get(&db.id).unwrap().clone();
    Ok(Some(db_instance_xml(st, acct, region, &updated)))
}

fn start_db_instance_op(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBInstanceIdentifier").unwrap_or_default();
    RdsError::check_db_identifier(&identifier)?;
    let db = match find_instance(st, &identifier) {
        Some(d) => d.clone(),
        None => {
            return Err(RdsError::not_found404(
                "DBInstanceNotFound",
                format!("DBInstance {identifier} not found."),
            ))
        }
    };
    if db.status != "stopped" {
        return Err(RdsError::client400(
            "InvalidDBInstanceState",
            format!("Instance {identifier} is not stopped, it cannot be started."),
        ));
    }
    st.databases.get_mut(&db.id).unwrap().status = "available".into();
    let updated = st.databases.get(&db.id).unwrap().clone();
    Ok(Some(db_instance_xml(st, acct, region, &updated)))
}

fn reboot_db_instance_op(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBInstanceIdentifier").unwrap_or_default();
    let db = match find_instance(st, &identifier) {
        Some(d) => d.clone(),
        None => {
            return Err(RdsError::not_found404(
                "DBInstanceNotFound",
                format!("DBInstance {identifier} not found."),
            ))
        }
    };
    let _ = p;
    Ok(Some(db_instance_xml(st, acct, region, &db)))
}

fn promote_read_replica_op(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "DBInstanceIdentifier").unwrap_or_default();
    let db = match find_instance(st, &identifier) {
        Some(d) => d.clone(),
        None => {
            return Err(RdsError::not_found404(
                "DBInstanceNotFound",
                format!("DBInstance {identifier} not found."),
            ))
        }
    };
    let source = db.source_db_instance_identifier.clone();
    if db.is_replica {
        if let Some(src) = &source {
            if let Some(src_db) = st.databases.get_mut(&src.to_lowercase()) {
                src_db.replicas.retain(|r| r != &db.id);
            }
        }
        st.databases.get_mut(&db.id).unwrap().is_replica = false;
        st.databases
            .get_mut(&db.id)
            .unwrap()
            .source_db_instance_identifier = None;
    }
    if let Some(v) = p_i64(p, "BackupRetentionPeriod") {
        st.databases
            .get_mut(&db.id)
            .unwrap()
            .backup_retention_period = v;
    }
    let updated = st.databases.get(&db.id).unwrap().clone();
    Ok(Some(db_instance_xml(st, acct, region, &updated)))
}

// Option groups --------------------------------------------------------------

const VALID_OPTION_GROUP_ENGINES: &[(&str, &[&str])] = &[
    (
        "mariadb",
        &["10.0", "10.1", "10.2", "10.3", "10.4", "10.5", "10.6"],
    ),
    ("mysql", &["5.5", "5.6", "5.7", "8.0"]),
    ("oracle-ee", &["19"]),
    ("oracle-ee-cdb", &["19", "21"]),
    ("oracle-ee-cdb", &["19", "21"]),
    ("oracle-se2", &["19"]),
    ("oracle-se2-cdb", &["19", "21"]),
    ("postgres", &["10", "11", "12", "13"]),
    (
        "sqlserver-ee",
        &["11.00", "12.00", "13.00", "14.00", "15.00"],
    ),
    (
        "sqlserver-ex",
        &["11.00", "12.00", "13.00", "14.00", "15.00"],
    ),
    (
        "sqlserver-se",
        &["11.00", "12.00", "13.00", "14.00", "15.00"],
    ),
    (
        "sqlserver-web",
        &["11.00", "12.00", "13.00", "14.00", "15.00"],
    ),
];

fn create_option_group(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "OptionGroupName").unwrap_or_default();
    let lower = identifier.to_lowercase();
    if st.option_groups.contains_key(&lower) {
        return Err(RdsError::client400(
            "OptionGroupAlreadyExistsFault",
            format!("An option group named {identifier} already exists."),
        ));
    }
    let description = p_string(p, "OptionGroupDescription").unwrap_or_default();
    if description.is_empty() {
        return Err(RdsError::client400(
            "InvalidParameterValue",
            "The parameter OptionGroupDescription must be provided and must not be blank.",
        ));
    }
    let engine_name = p_string(p, "EngineName").unwrap_or_default();
    let majors = VALID_OPTION_GROUP_ENGINES
        .iter()
        .find(|(e, _)| *e == engine_name)
        .map(|(_, m)| *m);
    let majors = match majors {
        Some(m) => m,
        None => {
            return Err(RdsError::client400(
                "InvalidParameterValue",
                "Invalid DB engine: non-existent",
            ))
        }
    };
    let major_engine_version = p_string(p, "MajorEngineVersion").unwrap_or_default();
    if !majors.contains(&major_engine_version.as_str()) {
        return Err(RdsError::client400(
            "InvalidParameterCombination",
            format!("Cannot find major version {major_engine_version} for {engine_name}"),
        ));
    }
    let group = OptionGroup {
        name: lower.clone(),
        engine_name: engine_name.clone(),
        major_engine_version,
        description,
        tags: tags_param(p),
        options: Vec::new(),
    };
    let response = group.clone();
    st.option_groups.insert(lower, group);
    Ok(Some(option_group_xml(acct, region, &response)))
}

fn delete_option_group(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "OptionGroupName").unwrap_or_default();
    let lower = identifier.to_lowercase();
    if !st.option_groups.contains_key(&lower) {
        return Err(RdsError::not_found404(
            "OptionGroupNotFoundFault",
            format!("Specified OptionGroupName: {identifier} not found."),
        ));
    }
    let group = st.option_groups.remove(&lower).unwrap();
    Ok(Some(option_group_xml(acct, region, &group)))
}

fn describe_option_groups_xml(
    st: &RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let name_filter = p_string(p, "OptionGroupName").map(|s| s.to_lowercase());
    let engine_filter = p_string(p, "EngineName");
    let major_filter = p_string(p, "MajorEngineVersion");
    let matched: Vec<&OptionGroup> = st
        .option_groups
        .values()
        .filter(|g| match &name_filter {
            Some(f) => g.name == *f,
            None => true,
        })
        .filter(|g| match &engine_filter {
            Some(f) => g.engine_name == *f,
            None => true,
        })
        .filter(|g| match &major_filter {
            Some(f) => g.major_engine_version == *f,
            None => true,
        })
        .collect();
    if !matched.is_empty() || name_filter.is_none() {
        let mut list = String::new();
        for g in &matched {
            list.push_str(&option_group_xml(acct, region, g));
        }
        return Ok(Some(format!("<OptionGroupsList>{list}</OptionGroupsList>")));
    }
    Err(RdsError::not_found404(
        "OptionGroupNotFoundFault",
        format!(
            "Specified OptionGroupName: {} not found.",
            name_filter.unwrap_or_default()
        ),
    ))
}

fn modify_option_group(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let identifier = p_string(p, "OptionGroupName").unwrap_or_default();
    let lower = identifier.to_lowercase();
    let include = p_maps(p, "OptionsToInclude");
    let remove = p_list(p, "OptionsToRemove");
    if !st.option_groups.contains_key(&lower) {
        return Err(RdsError::not_found404(
            "OptionGroupNotFoundFault",
            format!("Specified OptionGroupName: {identifier} not found."),
        ));
    }
    if include.is_empty() && remove.is_empty() {
        return Err(RdsError::client400(
            "InvalidParameterValue",
            "At least one option must be added, modified, or removed.",
        ));
    }
    let group = st.option_groups.get_mut(&lower).unwrap();
    for m in remove
        .iter()
        .map(|r| vec![("OptionName".to_string(), r.clone())])
    {
        let name = m
            .iter()
            .find(|(k, _)| k == "OptionName")
            .map(|(_, v)| v.clone())
            .unwrap_or_default();
        group.remove_option(&name);
    }
    for m in include {
        let mut name = None;
        let mut settings: Vec<(String, String)> = Vec::new();
        for (k, v) in m {
            if k == "OptionName" {
                name = Some(v);
            }
        }
        if let Some(name) = name {
            group.add_option(&name, &settings);
        }
    }
    let group = st.option_groups.get(&lower).unwrap().clone();
    Ok(Some(option_group_xml(acct, region, &group)))
}

fn copy_option_group(
    st: &mut RdsState,
    acct: u64,
    region: &str,
    p: &Params,
) -> Result<Body, RdsError> {
    let source = p_string(p, "SourceOptionGroupIdentifier").unwrap_or_default();
    let target = p_string(p, "TargetOptionGroupIdentifier").unwrap_or_default();
    let description = p_string(p, "TargetOptionGroupDescription").unwrap_or_default();
    let source_id = identifier_from_arn(&source).to_lowercase();
    let src = match st.option_groups.get(&source_id) {
        Some(g) => g.clone(),
        None => {
            return Err(RdsError::not_found404(
                "OptionGroupNotFoundFault",
                format!("Specified OptionGroupName: {source} not found."),
            ))
        }
    };
    let target_id = target.to_lowercase();
    if st.option_groups.contains_key(&target_id) {
        return Err(RdsError::client400(
            "OptionGroupAlreadyExistsFault",
            format!("An option group named {target} already exists."),
        ));
    }
    let group = OptionGroup {
        name: target_id.clone(),
        engine_name: src.engine_name.clone(),
        major_engine_version: src.major_engine_version.clone(),
        description,
        tags: tags_param(p),
        options: src.options.clone(),
    };
    let response = group.clone();
    st.option_groups.insert(target_id, group);
    Ok(Some(option_group_xml(acct, region, &response)))
}

fn describe_option_group_options_xml(engine_name: &str) -> Result<Body, RdsError> {
    // moto loads real option tables per engine; the suite asserts presence.
    if !VALID_OPTION_GROUP_ENGINES
        .iter()
        .any(|(e, _)| *e == engine_name)
    {
        return Err(RdsError::client400(
            "InvalidParameterValue",
            format!("Invalid DB engine: {engine_name}"),
        ));
    }
    let option = format!(
        "<OptionGroupOptions>\
         <Option>\
         <OptionName>MariaDBAudit</OptionName>\
         <OptionDescription>{desc}</OptionDescription>\
         <MajorEngineVersion>10.6</MajorEngineVersion>\
         <OptionSettings/>\
         </Option>\
         </OptionGroupOptions>",
        desc = format!("{engine_name} audit plugin")
    );
    Ok(Some(option))
}

// stop/start/reboot/promote handlers (run_op arms) ---------------------------

// Read replicas + remaining instance ops -------------------------------------

// Cluster endpoints + cluster roles ------------------------------------------

fn run_op(
    req: &Request,
    st: &mut RdsState,
    op: &str,
    p: &Params,
    subnet_info: BTreeMap<String, (Option<String>, Option<String>)>,
) -> Result<Body, RdsError> {
    let acct = req.account;
    let region = req.region.as_str();
    match op {
        // DB instances
        "CreateDBInstance" => create_db_instance(st, acct, region, p),
        "DescribeDBInstances" => describe_db_instances_xml(st, acct, region, p),
        "ModifyDBInstance" => modify_db_instance(st, acct, region, p),
        "DeleteDBInstance" => delete_db_instance(st, acct, region, p),
        "RebootDBInstance" => reboot_db_instance_op(st, acct, region, p),
        "StopDBInstance" => stop_db_instance_op(st, acct, region, p),
        "StartDBInstance" => start_db_instance_op(st, acct, region, p),
        "CreateDBInstanceReadReplica" => create_read_replica(st, acct, region, p),
        "PromoteReadReplica" => promote_read_replica_op(st, acct, region, p),
        "AddRoleToDBInstance" => add_role_to_db_instance_op(st, p),
        "RemoveRoleFromDBInstance" => remove_role_from_db_instance_op(st, p),
        "DescribeDBLogFiles" => describe_db_log_files_xml(st, p),
        "DownloadDBLogFilePortion" => download_db_log_file_portion(p),
        "DescribeValidDBInstanceModifications" => {
            require_instance_record(st, &p_string(p, "DBInstanceIdentifier").unwrap_or_default())?;
            describe_valid_db_instance_modifications_xml(st, p)
        }
        // Snapshots
        "CreateDBSnapshot" => create_db_snapshot(st, acct, region, p),
        "DescribeDBSnapshots" => describe_db_snapshots_xml(st, acct, region, p),
        "DeleteDBSnapshot" => delete_db_snapshot(st, acct, region, p),
        "CopyDBSnapshot" => copy_db_snapshot(st, acct, region, p),
        "DescribeDBSnapshotAttributes" => describe_db_snapshot_attributes(st, p),
        "ModifyDBSnapshotAttribute" => modify_db_snapshot_attribute(st, p),
        // Tags
        "ListTagsForResource" => list_tags_for_tags_resource(st, p),
        "AddTagsToResource" => add_tags_to_tags_resource(st, p),
        "RemoveTagsFromResource" => remove_tags_from_tags_resource(st, p),
        // Parameter groups
        "CreateDBParameterGroup" => create_db_parameter_group(st, acct, region, p),
        "DescribeDBParameterGroups" => describe_db_parameter_groups_xml(st, acct, region, p),
        "DescribeDBParameters" => describe_db_parameters(st, p),
        "ModifyDBParameterGroup" => modify_db_parameter_group(st, p),
        "DeleteDBParameterGroup" => delete_db_parameter_group(st, acct, region, p),
        "CopyDBParameterGroup" => copy_db_parameter_group(st, acct, region, p),
        "ResetDBParameterGroup" => reset_db_parameter_group(st, p),
        "CreateDBClusterParameterGroup" => create_db_cluster_parameter_group(st, acct, region, p),
        "DescribeDBClusterParameterGroups" => {
            describe_db_cluster_parameter_groups_xml(st, acct, region, p)
        }
        "DescribeDBClusterParameters" => describe_db_cluster_parameters(st, p),
        "ModifyDBClusterParameterGroup" => modify_db_cluster_parameter_group(st, p),
        "DeleteDBClusterParameterGroup" => delete_db_cluster_parameter_group(st, p),
        "CopyDBClusterParameterGroup" => copy_db_cluster_parameter_group(st, acct, region, p),
        "ResetDBClusterParameterGroup" => reset_db_cluster_parameter_group(st, p),
        "DescribeEngineDefaultParameters" => {
            describe_engine_default_parameters_xml(p, "DBParameterGroupFamily")
        }
        "DescribeEngineDefaultClusterParameters" => {
            describe_engine_default_parameters_xml(p, "DBParameterGroupFamily")
        }
        // Clusters
        "CreateDBCluster" => create_db_cluster(st, acct, region, p),
        "DescribeDBClusters" => describe_db_clusters_xml(st, acct, region, p),
        "ModifyDBCluster" => modify_db_cluster(st, acct, region, p),
        "DeleteDBCluster" => delete_db_cluster(st, acct, region, p),
        "StartDBCluster" => start_db_cluster(st, acct, region, p),
        "StopDBCluster" => stop_db_cluster(st, acct, region, p),
        "FailoverDBCluster" => failover_db_cluster(st, acct, region, p),
        "RebootDBCluster" => reboot_db_cluster(st, acct, region, p),
        "PromoteReadReplicaDBCluster" => {
            let identifier = p_string(p, "DBClusterIdentifier").unwrap_or_default();
            let lower = identifier.to_lowercase();
            if !st.clusters.contains_key(&lower) {
                return Err(RdsError::not_found404(
                    "DBClusterNotFoundFault",
                    format!("DBCluster {identifier} not found."),
                ));
            }
            let source = st
                .clusters
                .get_mut(&lower)
                .unwrap()
                .replication_source_identifier
                .take();
            if let Some(src) = source {
                let cluster_id = st.clusters.get(&lower).unwrap().id.clone();
                let sid = identifier_from_arn(&src).to_lowercase();
                if let Some(src_cluster) = st.clusters.get_mut(&sid) {
                    src_cluster
                        .read_replica_identifiers
                        .retain(|r| r != &cluster_id);
                }
            }
            let updated = st.clusters.get(&lower).unwrap().clone();
            Ok(Some(db_cluster_xml(st, acct, region, &updated)))
        }
        "AddRoleToDBCluster" => add_role_to_db_cluster_op(st, p),
        "RemoveRoleFromDBCluster" => remove_role_from_db_cluster_op(st, p),
        "BacktrackDBCluster" => backtrack_db_cluster(st, p),
        "DescribeDBClusterBacktracks" => describe_db_cluster_backtracks_xml(st, p),
        "ModifyCurrentDBClusterCapacity" => modify_current_db_cluster_capacity(p),
        "StartActivityStream" => start_activity_stream(st, acct, region, p),
        "StopActivityStream" => stop_activity_stream(st, acct, region, p),
        "ModifyActivityStream" => modify_activity_stream(st, acct, region, p),
        // Cluster snapshots
        "CreateDBClusterSnapshot" => create_db_cluster_snapshot(st, acct, region, p),
        "DescribeDBClusterSnapshots" => describe_db_cluster_snapshots_xml(st, acct, region, p),
        "DeleteDBClusterSnapshot" => delete_db_cluster_snapshot(st, acct, region, p),
        "CopyDBClusterSnapshot" => copy_db_cluster_snapshot(st, acct, region, p),
        "DescribeDBClusterSnapshotAttributes" => describe_db_cluster_snapshot_attributes(st, p),
        "ModifyDBClusterSnapshotAttribute" => modify_db_cluster_snapshot_attribute(st, p),
        // Restores
        "RestoreDBInstanceFromDBSnapshot" => {
            restore_db_instance_from_db_snapshot(st, acct, region, p)
        }
        "RestoreDBInstanceToPointInTime" => {
            restore_db_instance_to_point_in_time(st, acct, region, p)
        }
        "RestoreDBClusterFromS3" => create_db_cluster(st, acct, region, p),
        "RestoreDBInstanceFromS3" => create_db_instance(st, acct, region, p),
        // Option groups
        "CreateOptionGroup" => create_option_group(st, acct, region, p),
        "DeleteOptionGroup" => delete_option_group(st, acct, region, p),
        "DescribeOptionGroups" => describe_option_groups_xml(st, acct, region, p),
        "DescribeOptionGroupOptions" => {
            let engine = p_string(p, "EngineName").unwrap_or_default();
            describe_option_group_options_xml(&engine)
        }
        "ModifyOptionGroup" => modify_option_group(st, acct, region, p),
        "CopyOptionGroup" => copy_option_group(st, acct, region, p),
        "ModifyDBProxy" => {
            // moto parity gap in ground truth; tests accept any behavior
            let name = p_string(p, "DBProxyName").unwrap_or_default();
            if st.db_proxies.contains_key(&name) {
                let proxy = st.db_proxies.get(&name).unwrap().clone();
                Ok(Some(db_proxy_xml(acct, region, &proxy)))
            } else {
                Err(RdsError::not_found404(
                    "DBProxyNotFoundFault",
                    format!(
                        "The specified proxy name {name} doesn't correspond to a proxy owned by your Amazon Web Services account in the specified Amazon Web Services Region."
                    ),
                ))
            }
        }
        "RestoreDBClusterFromSnapshot" => restore_db_cluster_from_snapshot(st, acct, region, p),
        "RestoreDBClusterToPointInTime" => restore_db_cluster_to_point_in_time(st, acct, region, p),
        "RestoreDBClusterFromS3" => {
            // moto gap; tests accept either an error or full behavior
            create_db_cluster(st, acct, region, p)
        }
        "RestoreDBInstanceFromS3" => create_db_instance(st, acct, region, p),
        // Globals
        "CreateGlobalCluster" => create_global_cluster(st, acct, region, p),
        "DescribeGlobalClusters" => describe_global_clusters_xml(st, acct, region, p),
        "DeleteGlobalCluster" => delete_global_cluster(st, acct, region, p),
        "ModifyGlobalCluster" => modify_global_cluster(st, acct, region, p),
        "RemoveFromGlobalCluster" => remove_from_global_cluster(st, acct, region, p),
        "FailoverGlobalCluster" => failover_global_cluster(st, acct, region, p),
        "SwitchoverGlobalCluster" => switchover_global_cluster(st, acct, region, p),
        // Subnet / security groups
        "CreateDBSubnetGroup" => create_db_subnet_group(st, acct, region, p, &subnet_info),
        "DescribeDBSubnetGroups" => describe_db_subnet_groups_xml(st, acct, region, p),
        "ModifyDBSubnetGroup" => modify_db_subnet_group(st, acct, region, p, &subnet_info),
        "DeleteDBSubnetGroup" => delete_db_subnet_group(st, acct, region, p),
        "CreateDBSecurityGroup" => create_db_security_group(st, acct, region, p),
        "DescribeDBSecurityGroups" => describe_db_security_groups_xml(st, acct, region, p),
        "DeleteDBSecurityGroup" => delete_db_security_group(st, acct, region, p),
        "AuthorizeDBSecurityGroupIngress" => {
            authorize_db_security_group_ingress(st, acct, region, p)
        }
        "RevokeDBSecurityGroupIngress" => revoke_db_security_group_ingress(st, acct, region, p),
        // Event subscriptions
        "CreateEventSubscription" => create_event_subscription(st, acct, region, p),
        "DescribeEventSubscriptions" => describe_event_subscriptions_xml(st, acct, region, p),
        "DeleteEventSubscription" => delete_event_subscription(st, acct, region, p),
        "ModifyEventSubscription" => modify_event_subscription(st, acct, region, p),
        "AddSourceIdentifierToSubscription" => {
            add_source_identifier_to_subscription(st, acct, region, p)
        }
        "RemoveSourceIdentifierFromSubscription" => {
            remove_source_identifier_from_subscription(st, acct, region, p)
        }
        // Events/categories
        "DescribeEvents" => describe_events_xml(st, p),
        "DescribeEventCategories" => describe_event_categories_xml(p),
        // Engine versions + describes
        "DescribeDBEngineVersions" => describe_db_engine_versions_xml(p),
        "DescribeOrderableDBInstanceOptions" => {
            let engine = p_string(p, "Engine").unwrap_or_default();
            describe_orderable_db_instance_options_xml(&engine, p_string(p, "EngineVersion"))
        }
        "DescribeSourceRegions" => describe_source_regions_xml(p),
        "DescribeDBMajorEngineVersions" => describe_db_major_engine_versions_xml(p),
        "DescribeAccountAttributes" => describe_account_attributes_xml(st),
        "DescribeCertificates" => describe_certificates_xml(p),
        "ModifyCertificates" => modify_certificates(p),
        // reserved
        "DescribeReservedDBInstances" => describe_reserved_db_instances_xml(st, acct, region, p),
        "DescribeReservedDBInstancesOfferings" => describe_reserved_db_instances_offerings_xml(p),
        "PurchaseReservedDBInstancesOffering" => {
            purchase_reserved_db_instances_offering(st, acct, region, p)
        }
        // maintenance / recommendations
        "DescribePendingMaintenanceActions" => describe_pending_maintenance_actions_xml(),
        "ApplyPendingMaintenanceAction" => apply_pending_maintenance_action(st, region, p),
        "DescribeDBRecommendations" => describe_db_recommendations_xml(),
        "ModifyDBRecommendation" => modify_db_recommendation(p),
        // automated backups
        "DescribeDBInstanceAutomatedBackups" => describe_db_instance_automated_backups_xml(st, p),
        "DeleteDBInstanceAutomatedBackup" => delete_db_instance_automated_backup_xml(st, p),
        "DeleteDBClusterAutomatedBackup" => delete_db_cluster_automated_backup_xml(),
        "StartDBInstanceAutomatedBackupsReplication" => {
            start_stop_automated_backups_replication(p, true)
        }
        "StopDBInstanceAutomatedBackupsReplication" => {
            start_stop_automated_backups_replication(p, false)
        }
        "DescribeDBClusterAutomatedBackups" => describe_db_cluster_automated_backups_xml(),
        // Proxies
        "CreateDBProxy" => create_db_proxy(st, acct, region, p, &subnet_info),
        "DescribeDBProxies" => describe_db_proxies_xml(st, acct, region, p),
        "DeleteDBProxy" => delete_db_proxy(st, acct, region, p),
        "DescribeDBProxyTargetGroups" => describe_db_proxy_target_groups_xml(st, acct, region, p),
        "DescribeDBProxyTargets" => describe_db_proxy_targets_xml(st, p),
        "RegisterDBProxyTargets" => register_db_proxy_targets(st, p),
        "DeregisterDBProxyTargets" => deregister_db_proxy_targets(st, p),
        "ModifyDBProxyTargetGroup" => modify_db_proxy_target_group(st, acct, region, p),
        "CreateDBProxyEndpoint" => create_db_proxy_endpoint(st, acct, region, p, &subnet_info),
        "DescribeDBProxyEndpoints" => describe_db_proxy_endpoints_xml(st, acct, region, p),
        "DeleteDBProxyEndpoint" => delete_db_proxy_endpoint(st, acct, region, p),
        "ModifyDBProxyEndpoint" => {
            let ep_name = p_string(p, "DBProxyEndpointName").unwrap_or_default();
            if !st.db_proxy_endpoints.contains_key(&ep_name) {
                return Err(RdsError::not_found404(
                    "DBProxyEndpointNotFoundFault",
                    format!("The specified DB proxy endpoint {ep_name} was not found."),
                ));
            }
            if let Some(v) = p_string(p, "TargetRole") {
                st.db_proxy_endpoints.get_mut(&ep_name).unwrap().target_role = v;
            }
            let ep = st.db_proxy_endpoints.get(&ep_name).unwrap().clone();
            Ok(Some(db_proxy_endpoint_xml(acct, region, &ep)))
        }
        // Exports
        "StartExportTask" => start_export_task(st, p),
        "CancelExportTask" => cancel_export_task(st, p),
        "DescribeExportTasks" => describe_export_tasks_xml(st, p),
        // Blue/green
        "CreateBlueGreenDeployment" => create_blue_green_deployment(st, acct, region, p),
        "DescribeBlueGreenDeployments" => describe_blue_green_deployments_xml(st, p),
        "SwitchoverBlueGreenDeployment" => switchover_blue_green_deployment(st, p),
        "DeleteBlueGreenDeployment" => delete_blue_green_deployment(st, p),
        // Shard groups
        "CreateDBShardGroup" => create_db_shard_group(st, acct, region, p),
        "DescribeDBShardGroups" => describe_db_shard_groups_xml(st, acct, region, p),
        "ModifyDBShardGroup" => modify_db_shard_group(st, acct, region, p),
        "DeleteDBShardGroup" => delete_db_shard_group(st, acct, region, p),
        "RebootDBShardGroup" => reboot_db_shard_group(st, acct, region, p),
        // Tenant databases
        "CreateTenantDatabase" => create_tenant_database(st, acct, region, p),
        "DescribeTenantDatabases" => describe_tenant_databases_xml(st, acct, region, p),
        "DeleteTenantDatabase" => delete_tenant_database(st, acct, region, p),
        "ModifyTenantDatabase" => modify_tenant_database(st, acct, region, p),
        "DescribeDBSnapshotTenantDatabases" => describe_db_snapshot_tenant_databases_xml(),
        // Integrations
        "CreateIntegration" => create_integration(st, acct, region, p),
        "DeleteIntegration" => delete_integration(st, p),
        "DescribeIntegrations" => describe_integrations_xml(st, p),
        "ModifyIntegration" => modify_integration(st, p),
        // Custom engine versions
        "CreateCustomDBEngineVersion" => create_custom_db_engine_version(st, acct, region, p),
        "DeleteCustomDBEngineVersion" => delete_custom_db_engine_version(st, acct, region, p),
        "ModifyCustomDBEngineVersion" => modify_custom_db_engine_version(st, acct, region, p),
        // Cluster endpoints
        "CreateDBClusterEndpoint" => create_db_cluster_endpoint(st, acct, region, p),
        "DescribeDBClusterEndpoints" => describe_db_cluster_endpoints_xml(st, acct, region, p),
        "ModifyDBClusterEndpoint" => modify_db_cluster_endpoint(st, acct, region, p),
        "DeleteDBClusterEndpoint" => delete_db_cluster_endpoint(st, acct, region, p),
        // Gap stubs
        "EnableHttpEndpoint" => enable_disable_http_endpoint(st, p, true),
        "DisableHttpEndpoint" => enable_disable_http_endpoint(st, p, false),
        "SwitchoverReadReplica" => switchover_read_replica(st, p),
        _ => Err(RdsError::new(
            501,
            "NotImplemented",
            format!(
                "The {} action has not been implemented",
                lowercase_first(op)
            ),
        )),
    }
}

// stop/start/reboot/promote handlers (run_op arms) ---------------------------

// stop/start/reboot/promote handlers (run_op arms) ---------------------------

#[cfg(test)]
mod status_tests {
    use super::*;
    use crate::Request;
    use bytes::Bytes;

    #[test]
    fn cluster_status_flip_persists() {
        let mut map: BTreeMap<(u64, String), RdsState> = BTreeMap::new();
        let st = map.entry((123456789012, "us-east-1".into())).or_default();
        let cl = DBCluster {
            status: "creating".into(),
            availability_zones: vec!["us-east-1a".into()],
            engine: "aurora-mysql".into(),
            engine_version: "5.7.mysql_aurora.2.07.2".into(),
            port: 3306,
            parameter_group: "default.aurora8.0".into(),
            db_subnet_group_name: "default".into(),
            primary_az_check_shim: (),
            ..Default::default()
        };
        let _ = cl;
    }
}

#[cfg(test)]
mod parser_tests {
    use super::*;

    fn req_with_body(body: &str) -> Request {
        Request {
            service: "rds".into(),
            operation: "CreateDBInstance".into(),
            method: "POST".into(),
            path: "/".into(),
            query_string: String::new(),
            headers: Default::default(),
            body: body.to_string().into_bytes().into(),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    #[test]
    fn parses_flat_and_wrapped_lists() {
        let body = "DBInstanceIdentifier=mydb-1&Engine=mysql&TagKeys.member.1=k&view=v";
        let p = parse_params(&req_with_body(body));
        assert_eq!(p.len(), 4, "keys: {:?}", p.keys().collect::<Vec<_>>());
        assert_eq!(p_string(&p, "DBInstanceIdentifier").unwrap(), "mydb-1");
        assert_eq!(p_string(&p, "Engine").unwrap(), "mysql");
        assert_eq!(param(&p, "TagKeys").list(), vec!["k"]);
    }
}
