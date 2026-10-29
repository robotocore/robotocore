//! Cross-service state bridge.
//!
//! Lets other service crates (Lambda, ...) read object bodies from the S3
//! store without owning an `S3Handler` instance. S3 states register here as
//! they are touched; because `S3State` holds its bucket map behind an `Arc`,
//! a registered state stays live and any later object writes are visible.
//! Mirrors the `sqs::bridge` pattern for process-global access.

use crate::models::{Bucket, S3State};
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
}
