//! State snapshot/restore surface for the S3 handler.
//!
//! `snapshot_json` serializes every (account, region) store into a JSON
//! value; `restore_json` replaces store contents from that shape. Used by
//! the server's `/_robotocore/state/*` endpoints.

use serde_json::Value;
use std::sync::Arc;

use crate::handler::S3Handler;
use crate::models::Bucket;

pub(crate) mod rw {
    use parking_lot::RwLock;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<T: Serialize, S: Serializer>(v: &RwLock<T>, s: S) -> Result<S::Ok, S::Error> {
        T::serialize(&*v.read(), s)
    }

    pub fn deserialize<'de, T: Deserialize<'de>, D: Deserializer<'de>>(
        d: D,
    ) -> Result<RwLock<T>, D::Error> {
        Ok(RwLock::new(T::deserialize(d)?))
    }
}

pub(crate) mod atomic_u64 {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::sync::atomic::AtomicU64;

    pub fn serialize<S: Serializer>(v: &AtomicU64, s: S) -> Result<S::Ok, S::Error> {
        v.load(std::sync::atomic::Ordering::SeqCst).serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<AtomicU64, D::Error> {
        Ok(AtomicU64::new(u64::deserialize(d)?))
    }
}

pub(crate) mod b64 {
    use base64::Engine;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &Vec<u8>, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&base64::engine::general_purpose::STANDARD.encode(v))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let text = String::deserialize(d)?;
        base64::engine::general_purpose::STANDARD
            .decode(text.as_bytes())
            .map_err(serde::de::Error::custom)
    }
}

impl S3Handler {
    /// Serialize the full handler state (all accounts/regions) to JSON.
    pub fn snapshot_json(&self) -> Value {
        self.state
            .read()
            .iter()
            .map(|((account, region), st)| {
                let mut buckets = serde_json::Map::new();
                for (name, b) in st.buckets.read().iter() {
                    buckets.insert(
                        name.clone(),
                        serde_json::to_value(&**b).unwrap_or(Value::Null),
                    );
                }
                (format!("{account}|{region}"), Value::Object(buckets))
            })
            .collect::<serde_json::Map<String, Value>>()
            .into()
    }

    /// Replace handler state from a JSON value produced by `snapshot_json`.
    ///
    /// Replace semantics: every existing partition is emptied first, then
    /// the snapshot's entries are applied.
    pub fn restore_json(&self, v: &Value) {
        let existing: Vec<(u64, String)> = self.state.read().keys().cloned().collect();
        for (account, region) in existing {
            self.get_state(account, &region).buckets.write().clear();
        }

        let Some(obj) = v.as_object() else {
            return;
        };
        for (key, store_state) in obj {
            let Some((account, region)) = key.split_once('|') else {
                continue;
            };
            let Ok(account) = account.parse::<u64>() else {
                continue;
            };
            let st = self.get_state(account, region);
            let mut buckets = st.buckets.write();
            buckets.clear();
            if let Some(entries) = store_state.as_object() {
                for (name, b) in entries {
                    if let Ok(bucket) = serde_json::from_value::<Bucket>(b.clone()) {
                        buckets.insert(name.clone(), Arc::new(bucket));
                    }
                }
            }
        }
    }

    /// Clear all S3 state (every account/region).
    pub fn reset(&self) {
        let mut states = self.state.write();
        for st in states.values_mut() {
            st.buckets.write().clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{ObjectEntry, ObjectVersion, NULL_VERSION_ID};
    use std::collections::HashMap;

    #[test]
    fn test_snapshot_round_trip_restores_bucket_and_objects() {
        let handler = S3Handler::new();
        // Dedicated (account, region): other tests run in parallel.
        let st = handler.get_state(424242424242, "eu-roundtrip-1");
        st.put_bucket(Arc::new(Bucket::new(
            "rt-bucket".to_string(),
            "eu-roundtrip-1".to_string(),
        )));
        let bucket = st.get_bucket("rt-bucket").unwrap();
        bucket.objects.write().insert(
            "k.txt".to_string(),
            ObjectEntry {
                versions: vec![ObjectVersion {
                    version_id: NULL_VERSION_ID.to_string(),
                    seq: 1,
                    data: b"state-data".to_vec(),
                    content_type: "text/plain".to_string(),
                    metadata: HashMap::new(),
                    etag: "abc".to_string(),
                    size: 10,
                    last_modified: 99,
                    storage_class: "STANDARD".to_string(),
                    tags: HashMap::new(),
                    legal_hold: None,
                    retention: None,
                }],
                markers: vec![],
            },
        );

        let snap = handler.snapshot_json();
        assert!(snap.to_string().contains("rt-bucket"));

        // Mutate after the snapshot: remove the bucket entirely.
        st.delete_bucket("rt-bucket");

        handler.restore_json(&snap);
        let bucket = st.get_bucket("rt-bucket").expect("bucket restored");
        let entry = bucket.objects.read().get("k.txt").map(|e| {
            (
                e.is_live(),
                e.versions[0].data.clone(),
                e.versions[0].version_id.clone(),
            )
        });
        let Some((is_live, data, version_id)) = entry else {
            panic!("object entry restored");
        };
        assert!(is_live);
        assert_eq!(data, b"state-data".to_vec());
        assert_eq!(version_id, NULL_VERSION_ID);
        assert_eq!(bucket.seq.load(std::sync::atomic::Ordering::SeqCst), 0);

        // Cleanup: do not leak state into other tests in this process.
        st.delete_bucket("rt-bucket");
    }
}
