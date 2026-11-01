//! Cross-service state bridge.
//!
//! Lets other service crates (Lambda, ...) read object bodies from the S3
//! store without owning an `S3Handler` instance. S3 states register here as
//! they are touched; because `S3State` holds its bucket map behind an `Arc`,
//! a registered state stays live and any later object writes are visible.
//! Mirrors the `sqs::bridge` pattern for process-global access.

use crate::models::{Bucket, ObjectVersion, S3State};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::OnceLock;

fn registered() -> &'static RwLock<HashMap<(u64, String), S3State>> {
    static REGISTRY: OnceLock<RwLock<HashMap<(u64, String), S3State>>> = OnceLock::new();
    REGISTRY.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Register (or re-register) a live S3 state for (account, region). Called by
/// the S3 handler whenever it resolves its per-account-region state.
pub fn register_state(account: u64, region: &str, state: &S3State) {
    registered()
        .write()
        .insert((account, region.to_string()), state.clone());
}

/// Whether a bucket exists in the registered state for (account, region).
pub fn bucket_exists(account: u64, region: &str, bucket: &str) -> bool {
    get_bucket(account, region, bucket).is_some()
}

/// Live handle of `bucket` in the registered state for (account, region).
///
/// The returned `Arc` shares the bucket's interior `RwLock`s, so callers can
/// read or update bucket-level configuration (policy, tagging, versioning,
/// lifecycle, replication) exactly like moto's cross-backend delegation.
pub fn get_bucket(account: u64, region: &str, bucket: &str) -> Option<std::sync::Arc<Bucket>> {
    registered()
        .read()
        .get(&(account, region.to_string()))
        .and_then(|state| state.get_bucket(bucket))
}

/// Body of the current (newest) version of `key` in `bucket`, from the
/// registered state for (account, region). Returns None when no state is
/// registered, the bucket or the key is unknown.
pub fn get_object_body(account: u64, region: &str, bucket: &str, key: &str) -> Option<Vec<u8>> {
    let state = registered()
        .read()
        .get(&(account, region.to_string()))?
        .clone();
    let bucket = state.get_bucket(bucket)?;
    let objects = bucket.objects.read();
    objects.get(key)?.versions.first().map(|v| v.data.clone())
}

/// Write `data` into `bucket/key` through the registered state for
/// (account, region) — the cross-service landing harbor for moto parity
/// flows like firehose delivery (`put_s3_records`, firehose models.py:474-
/// :501). moto's S3 `put_object` implicitly creates a missing bucket, and
/// so does this. Returns the stored version id, or None when no state is
/// registered for (account, region) — a bare cross-crate process has
/// nothing to write into (callers skip delivery there; the registered
/// gateway process always has the S3 partition after any S3 op).
pub fn put_object(
    account: u64,
    region: &str,
    bucket: &str,
    key: &str,
    data: Vec<u8>,
    content_type: &str,
) -> Option<String> {
    let state = registered()
        .read()
        .get(&(account, region.to_string()))?
        .clone();
    let bucket = match state.get_bucket(bucket) {
        Some(b) => b,
        // moto's S3 backends auto-create a missing bucket on put_object.
        None => {
            let b = std::sync::Arc::new(Bucket::new(bucket.to_string(), region.to_string()));
            state.put_bucket(b);
            state.get_bucket(bucket)?
        }
    };
    let version_id = if bucket.is_versioned() {
        uuid::Uuid::new_v4().to_string()
    } else {
        crate::models::NULL_VERSION_ID.to_string()
    };
    let mut version = ObjectVersion::null_version(data, content_type.to_string());
    version.version_id = version_id.clone();
    version.seq = bucket.next_seq();
    version.last_modified = chrono::Utc::now().timestamp() as u64;
    let mut objects = bucket.objects.write();
    let entry = objects.entry(key.to_string()).or_default();
    if version_id != crate::models::NULL_VERSION_ID {
        entry.versions.insert(0, version);
    } else {
        entry.versions.clear();
        entry.versions.push(version);
    }
    Some(version_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{Bucket, ObjectVersion};
    use std::sync::Arc;

    #[test]
    fn test_register_and_read_object_body() {
        let bucket = Arc::new(Bucket::new("reg-bkt".to_string(), "us-east-1".to_string()));
        bucket.objects.write().insert(
            "k.zip".to_string(),
            crate::models::ObjectEntry {
                versions: vec![ObjectVersion::null_version(
                    b"zip-bytes".to_vec(),
                    "application/zip".to_string(),
                )],
                markers: Vec::new(),
            },
        );
        let state = S3State::new();
        state.put_bucket(bucket);
        register_state(777777777777, "us-east-1", &state);

        assert!(bucket_exists(777777777777, "us-east-1", "reg-bkt"));
        assert!(!bucket_exists(777777777777, "us-east-1", "no-such-bkt"));
        assert_eq!(
            get_object_body(777777777777, "us-east-1", "reg-bkt", "k.zip"),
            Some(b"zip-bytes".to_vec())
        );
        assert_eq!(
            get_object_body(777777777777, "us-east-1", "reg-bkt", "nope"),
            None
        );
        assert_eq!(get_object_body(1, "us-east-1", "reg-bkt", "k.zip"), None);
    }

    /// put_object writes through the registered state and mirrors moto's
    /// implicit bucket auto-create; unregistered partitions answer None.
    #[test]
    fn test_put_object_writes_and_auto_creates_bucket() {
        let state = S3State::new();
        register_state(777777777778, "us-west-2", &state);
        let vid = put_object(
            777777777778,
            "us-west-2",
            "auto-bkt",
            "firehose/2026/11/01/01/stream-1-...",
            b"joined-records",
            "application/octet-stream",
        )
        .expect("state registered → write succeeds");
        assert_eq!(
            get_object_body(
                777777777778,
                "us-west-2",
                "auto-bkt",
                "firehose/2026/11/01/01/stream-1-..."
            ),
            Some(b"joined-records".to_vec())
        );
        assert!(bucket_exists(777777777778, "us-west-2", "auto-bkt"));
        assert_eq!(
            put_object(2, "us-west-2", "auto-bkt", "k", b"x", "text/plain"),
            None
        );
        let _ = vid;
    }
}
