//! In-memory state store for AWS resources.
//!
//! Provides a generic registry of per-account-region state stores.
//! Each store holds named tables of resources (S3 buckets, SQS queues, etc.).

use parking_lot::RwLock;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

use crate::core::account::AccountRegion;

/// In-memory table of resources, keyed by resource name.
pub type ResourceTable = Arc<RwLock<HashMap<String, Value>>>;

/// Global registry of state stores, scoped by (account, region).
pub struct StateStore {
    stores: RwLock<HashMap<AccountRegion, HashMap<String, ResourceTable>>>,
}

impl StateStore {
    /// Create a new empty StateStore.
    pub fn new() -> Self {
        Self {
            stores: RwLock::new(HashMap::new()),
        }
    }

    /// Get or create a table for a resource type in the given account/region.
    pub fn table(&self, ar: &AccountRegion, table_name: &str) -> ResourceTable {
        let mut stores = self.stores.write();
        let tables = stores.entry(ar.clone()).or_default();

        tables
            .entry(table_name.to_string())
            .or_insert_with(|| Arc::new(RwLock::new(HashMap::new())))
            .clone()
    }

    /// Get a value from a table.
    pub fn get(&self, ar: &AccountRegion, table: &str, key: &str) -> Option<Value> {
        let tbl = self.table(ar, table);
        let guard = tbl.read();
        guard.get(key).cloned()
    }

    /// Put a value into a table.
    pub fn put(&self, ar: &AccountRegion, table: &str, key: String, value: Value) {
        let tbl = self.table(ar, table);
        tbl.write().insert(key, value);
    }

    /// Delete a value from a table.
    pub fn delete(&self, ar: &AccountRegion, table: &str, key: &str) -> Option<Value> {
        let tbl = self.table(ar, table);
        let mut guard = tbl.write();
        guard.remove(key)
    }

    /// Scan all keys in a table.
    pub fn scan(&self, ar: &AccountRegion, table: &str) -> Vec<(String, Value)> {
        let tbl = self.table(ar, table);
        let guard = tbl.read();
        guard.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    /// List all table names for a given account/region.
    pub fn list_tables(&self, ar: &AccountRegion) -> Vec<String> {
        let stores = self.stores.read();
        match stores.get(ar) {
            Some(tables) => tables.keys().cloned().collect(),
            None => Vec::new(),
        }
    }
}

impl Default for StateStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put_and_get() {
        let store = StateStore::new();
        let ar = AccountRegion::new(123456789012, "us-east-1".to_string());

        let value = Value::String("test".to_string());
        store.put(&ar, "s3", "bucket1".to_string(), value.clone());

        let retrieved = store.get(&ar, "s3", "bucket1");
        assert_eq!(retrieved, Some(value));
    }

    #[test]
    fn test_get_nonexistent() {
        let store = StateStore::new();
        let ar = AccountRegion::new(123456789012, "us-east-1".to_string());

        let retrieved = store.get(&ar, "s3", "nonexistent");
        assert_eq!(retrieved, None);
    }

    #[test]
    fn test_delete() {
        let store = StateStore::new();
        let ar = AccountRegion::new(123456789012, "us-east-1".to_string());

        let value = Value::String("test".to_string());
        store.put(&ar, "s3", "bucket1".to_string(), value);

        let deleted = store.delete(&ar, "s3", "bucket1");
        assert!(deleted.is_some());

        let retrieved = store.get(&ar, "s3", "bucket1");
        assert_eq!(retrieved, None);
    }

    #[test]
    fn test_account_isolation() {
        let store = StateStore::new();
        let ar1 = AccountRegion::new(111111111111, "us-east-1".to_string());
        let ar2 = AccountRegion::new(222222222222, "us-east-1".to_string());

        let value1 = Value::String("account1".to_string());
        let value2 = Value::String("account2".to_string());

        store.put(&ar1, "s3", "bucket".to_string(), value1.clone());
        store.put(&ar2, "s3", "bucket".to_string(), value2.clone());

        assert_eq!(store.get(&ar1, "s3", "bucket"), Some(value1));
        assert_eq!(store.get(&ar2, "s3", "bucket"), Some(value2));
    }

    #[test]
    fn test_scan() {
        let store = StateStore::new();
        let ar = AccountRegion::new(123456789012, "us-east-1".to_string());

        store.put(
            &ar,
            "s3",
            "bucket1".to_string(),
            Value::String("b1".to_string()),
        );
        store.put(
            &ar,
            "s3",
            "bucket2".to_string(),
            Value::String("b2".to_string()),
        );

        let items = store.scan(&ar, "s3");
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn snapshot_name_guard_rejects_traversal() {
        // python manager.py `_validate_snapshot_name`: empty, `/` and `\`
        // separators are rejected; flat names pass.
        assert!(validate_snapshot_name("snap-2024").is_ok());
        for bad in [
            "",
            "sub/snap",
            "../escape",
            "..\\escape",
            "/abs/snap",
            "snap/../..",
        ] {
            let err = validate_snapshot_name(bad).expect_err(bad);
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput, "{bad}");
            assert!(
                err.to_string().contains("traversal") || err.to_string().contains("empty"),
                "{bad}"
            );
        }
    }

    #[test]
    fn save_and_load_reject_traversal_names() {
        let tmp = std::env::temp_dir().join("robotocore-state-guard-test");
        let dir = tmp.join("base");
        let outside = tmp.join("escape.json");
        std::fs::remove_dir_all(&tmp).ok();
        std::fs::create_dir_all(&dir).unwrap();

        let value = json_snapshot();
        // `../escape` must be refused and must not create the sibling file.
        let err = save_to_path(dir.to_str().unwrap(), Some("../escape"), &value)
            .expect_err("traversal save rejected");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        assert!(!outside.exists(), "traversal name escaped the base dir");

        let err = load_from_path(dir.to_str().unwrap(), Some("../escape"))
            .expect_err("traversal load rejected");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);

        // Missing files stay "no state" (python returned False), not errors.
        assert!(load_from_path(dir.to_str().unwrap(), Some("never-saved"))
            .unwrap()
            .is_none());

        // A flat name still round-trips and the file stays inside the base.
        save_to_path(dir.to_str().unwrap(), Some("round-trip"), &value).unwrap();
        assert!(dir.join("round-trip.json").exists());
        let loaded = load_from_path(dir.to_str().unwrap(), Some("round-trip"))
            .unwrap()
            .expect("saved state readable");
        assert_eq!(loaded, value);
        std::fs::remove_dir_all(&tmp).ok();
    }

    fn json_snapshot() -> Value {
        serde_json::json!({"version": "1.0", "state": {}})
    }
}

// ---------------------------------------------------------------------------
// State save/load endpoints (Cloud Pods-style snapshots)
// ---------------------------------------------------------------------------

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// JSON snapshot of one service's state and when it was taken.
#[derive(Debug, Clone)]
pub struct NamedSnapshot {
    pub name: String,
    pub created_at: f64,
    pub services: Vec<String>,
    pub state: serde_json::Map<String, Value>,
}

/// In-memory named snapshots (`/_robotocore/state/save` without a path).
#[derive(Default)]
pub struct NamedSnapshots {
    entries: RwLock<BTreeMap<String, NamedSnapshot>>,
}

impl NamedSnapshots {
    pub fn put(&self, entry: NamedSnapshot) {
        self.entries.write().insert(entry.name.clone(), entry);
    }

    pub fn get(&self, name: &str) -> Option<NamedSnapshot> {
        self.entries.read().get(name).cloned()
    }

    /// Newest-first listing.
    pub fn list(&self) -> Vec<NamedSnapshot> {
        let mut items: Vec<NamedSnapshot> = self.entries.read().values().cloned().collect();
        items.sort_by(|a, b| {
            b.created_at
                .partial_cmp(&a.created_at)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        items
    }
}

/// One stored version of a named snapshot (versioned variant endpoints).
#[derive(Debug, Clone)]
pub struct VersionedSnapshot {
    pub version: u64,
    pub created_at: f64,
    pub services: Vec<String>,
    pub state: serde_json::Map<String, Value>,
}

/// Versioned in-memory snapshots: each name keeps its full version history.
#[derive(Default)]
pub struct VersionedSnapshots {
    versions: RwLock<BTreeMap<String, Vec<VersionedSnapshot>>>,
}

impl VersionedSnapshots {
    /// Append a version; returns its 1-based version number.
    pub fn save(
        &self,
        name: &str,
        services: Vec<String>,
        state: serde_json::Map<String, Value>,
    ) -> u64 {
        let mut all = self.versions.write();
        let history = all.entry(name.to_string()).or_default();
        let next = history.last().map(|h| h.version + 1).unwrap_or(1);
        history.push(VersionedSnapshot {
            version: next,
            created_at: epoch_now(),
            services,
            state,
        });
        next
    }

    /// The requested version, or the latest when `version` is None.
    pub fn load(&self, name: &str, version: Option<u64>) -> Option<VersionedSnapshot> {
        let all = self.versions.read();
        let history = all.get(name)?;
        match version {
            Some(v) => history.iter().find(|h| h.version == v).cloned(),
            None => history.last().cloned(),
        }
    }

    /// Newest-first summary per snapshot name: (name, latest, version count).
    pub fn list(&self) -> Vec<(String, VersionedSnapshot, u64)> {
        let all = self.versions.read();
        let mut out: Vec<(String, VersionedSnapshot, u64)> = all
            .iter()
            .filter_map(|(name, history)| {
                Some((name.clone(), history.last()?.clone(), history.len() as u64))
            })
            .collect();
        out.sort_by(|a, b| {
            b.1.created_at
                .partial_cmp(&a.1.created_at)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        out
    }
}

fn epoch_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// One service's snapshot surface as opaque closures.
struct SnapshotEntry {
    name: &'static str,
    snapshot: Arc<dyn Fn() -> Value + Send + Sync>,
    restore: Arc<dyn Fn(&Value) + Send + Sync>,
    reset: Arc<dyn Fn() + Send + Sync>,
}

/// Central (service name, snapshot surface) table iterated by the state
/// endpoints. Sourced from the same handler instances the wire path uses.
pub struct SnapshotRegistry {
    entries: Vec<SnapshotEntry>,
}

fn snapshotter<H>(name: &'static str, handler: Arc<H>) -> SnapshotEntry
where
    H: SnapshotSurface + Send + Sync + 'static,
{
    SnapshotEntry {
        name,
        snapshot: Arc::new({
            let h = handler.clone();
            move || h.snapshot_json()
        }),
        restore: Arc::new({
            let h = handler.clone();
            move |v: &Value| h.restore_json(v)
        }),
        reset: Arc::new({
            let h = handler;
            move || h.reset()
        }),
    }
}

/// The per-crate snapshot surface every native handler shares.
pub trait SnapshotSurface {
    fn snapshot_json(&self) -> Value;
    fn restore_json(&self, v: &Value);
    fn reset(&self);
}
impl SnapshotSurface for sqs::DefaultSqsHandler {
    fn snapshot_json(&self) -> Value {
        sqs::DefaultSqsHandler::snapshot_json(self)
    }

    fn restore_json(&self, v: &Value) {
        sqs::DefaultSqsHandler::restore_json(self, v)
    }

    fn reset(&self) {
        sqs::DefaultSqsHandler::reset(self)
    }
}

impl SnapshotSurface for s3::DefaultS3Handler {
    fn snapshot_json(&self) -> Value {
        s3::DefaultS3Handler::snapshot_json(self)
    }

    fn restore_json(&self, v: &Value) {
        s3::DefaultS3Handler::restore_json(self, v)
    }

    fn reset(&self) {
        s3::DefaultS3Handler::reset(self)
    }
}

impl SnapshotSurface for dynamodb::DefaultDynamoDbHandler {
    fn snapshot_json(&self) -> Value {
        dynamodb::DefaultDynamoDbHandler::snapshot_json(self)
    }

    fn restore_json(&self, v: &Value) {
        dynamodb::DefaultDynamoDbHandler::restore_json(self, v)
    }

    fn reset(&self) {
        dynamodb::DefaultDynamoDbHandler::reset(self)
    }
}

impl SnapshotSurface for sns::DefaultSnsHandler {
    fn snapshot_json(&self) -> Value {
        sns::DefaultSnsHandler::snapshot_json(self)
    }

    fn restore_json(&self, v: &Value) {
        sns::DefaultSnsHandler::restore_json(self, v)
    }

    fn reset(&self) {
        sns::DefaultSnsHandler::reset(self)
    }
}

impl SnapshotSurface for secretsmanager::DefaultSecretsManagerHandler {
    fn snapshot_json(&self) -> Value {
        secretsmanager::DefaultSecretsManagerHandler::snapshot_json(self)
    }

    fn restore_json(&self, v: &Value) {
        secretsmanager::DefaultSecretsManagerHandler::restore_json(self, v)
    }

    fn reset(&self) {
        secretsmanager::DefaultSecretsManagerHandler::reset(self)
    }
}

impl SnapshotSurface for kms::DefaultKmsHandler {
    fn snapshot_json(&self) -> Value {
        kms::DefaultKmsHandler::snapshot_json(self)
    }

    fn restore_json(&self, v: &Value) {
        kms::DefaultKmsHandler::restore_json(self, v)
    }

    fn reset(&self) {
        kms::DefaultKmsHandler::reset(self)
    }
}

impl SnapshotSurface for ssm::DefaultSsmHandler {
    fn snapshot_json(&self) -> Value {
        ssm::DefaultSsmHandler::snapshot_json(self)
    }

    fn restore_json(&self, v: &Value) {
        ssm::DefaultSsmHandler::restore_json(self, v)
    }

    fn reset(&self) {
        ssm::DefaultSsmHandler::reset(self)
    }
}

impl SnapshotSurface for iam::DefaultIamHandler {
    fn snapshot_json(&self) -> Value {
        iam::DefaultIamHandler::snapshot_json(self)
    }

    fn restore_json(&self, v: &Value) {
        iam::DefaultIamHandler::restore_json(self, v)
    }

    fn reset(&self) {
        iam::DefaultIamHandler::reset(self)
    }
}

impl SnapshotSurface for lambda::DefaultLambdaHandler {
    fn snapshot_json(&self) -> Value {
        lambda::DefaultLambdaHandler::snapshot_json(self)
    }

    fn restore_json(&self, v: &Value) {
        lambda::DefaultLambdaHandler::restore_json(self, v)
    }

    fn reset(&self) {
        lambda::DefaultLambdaHandler::reset(self)
    }
}

impl SnapshotSurface for cloudwatch_logs::DefaultLogsHandler {
    fn snapshot_json(&self) -> Value {
        cloudwatch_logs::DefaultLogsHandler::snapshot_json(self)
    }

    fn restore_json(&self, v: &Value) {
        cloudwatch_logs::DefaultLogsHandler::restore_json(self, v)
    }

    fn reset(&self) {
        cloudwatch_logs::DefaultLogsHandler::reset(self)
    }
}

impl SnapshotSurface for events::DefaultEventsHandler {
    fn snapshot_json(&self) -> Value {
        events::DefaultEventsHandler::snapshot_json(self)
    }

    fn restore_json(&self, v: &Value) {
        events::DefaultEventsHandler::restore_json(self, v)
    }

    fn reset(&self) {
        events::DefaultEventsHandler::reset(self)
    }
}

impl SnapshotSurface for kinesis::DefaultKinesisHandler {
    fn snapshot_json(&self) -> Value {
        kinesis::DefaultKinesisHandler::snapshot_json(self)
    }

    fn restore_json(&self, v: &Value) {
        kinesis::DefaultKinesisHandler::restore_json(self, v)
    }

    fn reset(&self) {
        kinesis::DefaultKinesisHandler::reset(self)
    }
}

impl SnapshotSurface for firehose::DefaultFirehoseHandler {
    fn snapshot_json(&self) -> Value {
        firehose::DefaultFirehoseHandler::snapshot_json(self)
    }

    fn restore_json(&self, v: &Value) {
        firehose::DefaultFirehoseHandler::restore_json(self, v)
    }

    fn reset(&self) {
        firehose::DefaultFirehoseHandler::reset(self)
    }
}

impl SnapshotSurface for cloudwatch::DefaultCloudwatchHandler {
    fn snapshot_json(&self) -> Value {
        cloudwatch::DefaultCloudwatchHandler::snapshot_json(self)
    }

    fn restore_json(&self, v: &Value) {
        cloudwatch::DefaultCloudwatchHandler::restore_json(self, v)
    }

    fn reset(&self) {
        cloudwatch::DefaultCloudwatchHandler::reset(self)
    }
}

impl SnapshotSurface for ecr::DefaultEcrHandler {
    fn snapshot_json(&self) -> Value {
        ecr::DefaultEcrHandler::snapshot_json(self)
    }

    fn restore_json(&self, v: &Value) {
        ecr::DefaultEcrHandler::restore_json(self, v)
    }

    fn reset(&self) {
        ecr::DefaultEcrHandler::reset(self)
    }
}

impl SnapshotSurface for ecs::DefaultEcsHandler {
    fn snapshot_json(&self) -> Value {
        ecs::DefaultEcsHandler::snapshot_json(self)
    }

    fn restore_json(&self, v: &Value) {
        ecs::DefaultEcsHandler::restore_json(self, v)
    }

    fn reset(&self) {
        ecs::DefaultEcsHandler::reset(self)
    }
}

impl SnapshotSurface for stepfunctions::DefaultStepfunctionsHandler {
    fn snapshot_json(&self) -> Value {
        stepfunctions::DefaultStepfunctionsHandler::snapshot_json(self)
    }

    fn restore_json(&self, v: &Value) {
        stepfunctions::DefaultStepfunctionsHandler::restore_json(self, v)
    }

    fn reset(&self) {
        stepfunctions::DefaultStepfunctionsHandler::reset(self)
    }
}

impl SnapshotRegistry {
    /// Build the registry over the shared native handler instances.
    pub fn new(handlers: &crate::core::server::NativeHandlers) -> Self {
        Self {
            entries: vec![
                snapshotter("sqs", handlers.sqs.clone()),
                snapshotter("s3", handlers.s3.clone()),
                snapshotter("dynamodb", handlers.dynamodb.clone()),
                snapshotter("sns", handlers.sns.clone()),
                snapshotter("secretsmanager", handlers.secretsmanager.clone()),
                snapshotter("kms", handlers.kms.clone()),
                snapshotter("ssm", handlers.ssm.clone()),
                snapshotter("iam", handlers.iam.clone()),
                snapshotter("lambda", handlers.lambda.clone()),
                snapshotter("logs", handlers.logs.clone()),
                snapshotter("events", handlers.events.clone()),
                snapshotter("kinesis", handlers.kinesis.clone()),
                snapshotter("firehose", handlers.firehose.clone()),
                snapshotter("cloudwatch", handlers.cloudwatch.clone()),
                snapshotter("ecr", handlers.ecr.clone()),
                snapshotter("ecs", handlers.ecs.clone()),
                snapshotter("stepfunctions", handlers.stepfunctions.clone()),
            ],
        }
    }

    /// Every registered service name.
    pub fn names(&self) -> Vec<String> {
        self.entries.iter().map(|e| e.name.to_string()).collect()
    }

    /// Filter helper: either a validated requested subset or every service.
    fn selected(&self, services: Option<&[String]>) -> (Vec<String>, Vec<&SnapshotEntry>) {
        match services {
            Some(requested) => {
                let hits: Vec<_> = self
                    .entries
                    .iter()
                    .filter(|e| requested.iter().any(|r| r == e.name))
                    .collect();
                let names = hits.iter().map(|e| e.name.to_string()).collect();
                (names, hits)
            }
            None => {
                let names = self.names();
                let hits = self.entries.iter().collect();
                (names, hits)
            }
        }
    }

    /// Snapshot the selected services; returns (saved names, state).
    pub fn snapshot(&self, services: Option<&[String]>) -> (Vec<String>, Value) {
        let (names, selected) = self.selected(services);
        let mut state = serde_json::Map::new();
        for e in selected {
            state.insert(e.name.to_string(), (e.snapshot)());
        }
        (names, Value::Object(state))
    }

    /// Replace state for the selected services from a previous snapshot.
    pub fn restore(&self, v: &Value, services: Option<&[String]>) {
        let Some(map) = v.as_object() else {
            return;
        };
        let (_, selected) = self.selected(services);
        for e in selected {
            if let Some(value) = map.get(e.name) {
                (e.restore)(value);
            }
        }
    }

    /// Clear state for the selected services (all when unfiltered).
    pub fn reset(&self, services: Option<&[String]>) {
        let (_, selected) = self.selected(services);
        for e in selected {
            (e.reset)();
        }
    }
}

/// Effective base dir for pathless operations (mirrors python's state_dir).
pub fn resolve_state_dir() -> Option<PathBuf> {
    std::env::var_os("ROBOTOCORE_STATE_DIR").map(PathBuf::from)
}

/// Python-era snapshot-name guard (state/manager.py `_validate_snapshot_name`
/// and the `resolve().is_relative_to(...)` traversal check at its call
/// sites): a snapshot name must be a single bare path component — no empty
/// string, no separators, no `..` — so the joined file can never escape the
/// base directory. Errors are `ErrorKind::InvalidInput` so the admin
/// handlers can answer 400 for client-supplied traversal instead of a 500.
pub fn validate_snapshot_name(name: &str) -> Result<(), std::io::Error> {
    if name.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Snapshot name must not be empty",
        ));
    }
    if name.contains('/') || name.contains('\\') {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Invalid snapshot name (path traversal): {name:?}"),
        ));
    }
    Ok(())
}

/// Lexical `resolve().is_relative_to(base)` equivalent: reject a composed
/// path that climbs above `base` (a `..` anywhere past the base root).
/// The name guard above makes this a defense-in-depth check, but it is
/// what the python twin enforced last before touching disk.
fn stays_within_base(base: &Path, file: &Path) -> bool {
    file.components()
        .all(|c| c != std::path::Component::ParentDir)
        && file.starts_with(base)
}

/// Write the state JSON file: `<dir>/<name>.json`, or `<dir>/robotocore_state.json`
/// for unnamed saves. Atomic via temp-file rename.
pub fn save_to_path(dir: &str, name: Option<&str>, value: &Value) -> std::io::Result<String> {
    if let Some(n) = name {
        validate_snapshot_name(n)?;
    }
    let dir = PathBuf::from(dir);
    std::fs::create_dir_all(&dir)?;
    let file = match name {
        Some(n) => dir.join(format!("{n}.json")),
        None => dir.join("robotocore_state.json"),
    };
    if !stays_within_base(&dir, &file) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Invalid snapshot name (path traversal): {name:?}"),
        ));
    }
    let tmp = dir.join(format!(
        ".{}.tmp",
        file.file_name().unwrap_or_default().to_string_lossy()
    ));
    std::fs::write(&tmp, serde_json::to_vec_pretty(value).unwrap_or_default())?;
    std::fs::rename(tmp, &file)?;
    Ok(file.to_string_lossy().into_owned())
}

/// Read back a state JSON file written by [`save_to_path`].
pub fn load_from_path(dir: &str, name: Option<&str>) -> Result<Option<Value>, std::io::Error> {
    if let Some(n) = name {
        validate_snapshot_name(n)?;
    }
    let dir = PathBuf::from(dir);
    let file = match name {
        Some(n) => dir.join(format!("{n}.json")),
        None => dir.join("robotocore_state.json"),
    };
    if !stays_within_base(&dir, &file) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Invalid snapshot name (path traversal): {name:?}"),
        ));
    }
    let bytes = match std::fs::read(&file) {
        Ok(bytes) => bytes,
        // A missing file is "no state", the same answer python's load
        // returned (`no state directory found` → False), not an error.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    Ok(serde_json::from_slice(&bytes).ok())
}
