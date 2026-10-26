//! State snapshot/restore surface for the ecr handler.
//!
//! `snapshot_json` serializes every (account, region) store into a JSON
//! value; `restore_json` replaces store contents from that shape. Used by
//! the server's `/_robotocore/state/*` endpoints.

use serde_json::Value;

use crate::models::EcrState;

/// `Arc<RwLock<T>>` as plain `T` (for derived state models in models.rs).
pub(crate) mod arc_rw {
    use parking_lot::RwLock;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::sync::Arc;

    pub fn serialize<T: Serialize, S: Serializer>(
        v: &Arc<RwLock<T>>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        T::serialize(&*v.read(), s)
    }

    pub fn deserialize<'de, T: Deserialize<'de>, D: Deserializer<'de>>(
        d: D,
    ) -> Result<Arc<RwLock<T>>, D::Error> {
        Ok(Arc::new(RwLock::new(T::deserialize(d)?)))
    }
}

/// `Vec<u8>` as a base64 string (keeps blob-heavy state compact in snapshots).
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

/// Replace one store's contents in place from `v`, preserving `Arc` identity.
fn store_restore_json(st: &EcrState, v: &Value) {
    let restored: EcrState = serde_json::from_value(v.clone()).unwrap_or_default();
    macro_rules! swap {
        ($field:ident) => {
            *st.$field.write() = restored.$field.read().clone();
        };
    }
    swap!(repositories);
    swap!(images);
    swap!(uploads);
    swap!(layers);
    swap!(repository_policies);
    swap!(lifecycle_policies);
    swap!(tags);
    swap!(scanning_configs);
    swap!(registry_policy);
    swap!(replication_configuration);
    swap!(registry_scanning_configuration);
    swap!(pull_through_cache_rules);
    swap!(repository_creation_templates);
    swap!(account_settings);
}

impl crate::handler::EcrHandler {
    /// Serialize the full handler state (all accounts/regions) to JSON.
    pub fn snapshot_json(&self) -> Value {
        self.state
            .read()
            .iter()
            .map(|((account, region), st)| {
                (
                    format!("{account}|{region}"),
                    serde_json::to_value(st).unwrap_or(Value::Null),
                )
            })
            .collect::<serde_json::Map<String, Value>>()
            .into()
    }

    /// Replace handler state from a JSON value produced by `snapshot_json`.
    ///
    /// Replace semantics: every existing partition is emptied first, then
    /// the snapshot's entries are applied.
    pub fn restore_json(&self, v: &Value) {
        let empty = serde_json::Value::Object(serde_json::Map::new());
        {
            let mut states = self.state.write();
            for st in states.values_mut() {
                store_restore_json(st, &empty);
            }
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
            store_restore_json(&st, store_state);
        }
    }

    /// Clear all ECR state (every account/region).
    pub fn reset(&self) {
        let empty = serde_json::Value::Object(serde_json::Map::new());
        let mut states = self.state.write();
        for st in states.values_mut() {
            store_restore_json(st, &empty);
        }
    }
}
