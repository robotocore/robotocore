//! State snapshot/restore surface for the KMS handler.
//!
//! `snapshot_json` serializes every (account, region) store into a JSON
//! value; `restore_json` replaces store contents from that shape. Used by
//! the server's `/_robotocore/state/*` endpoints.
//!
//! Custom semantics: the RSA/EC private keys inside [`crate::models::Key`]
//! are skipped (`#[serde(skip)]`) and re-generated on restore — generating
//! fresh key material keeps the in-memory key self-consistent; ciphertext
//! produced under the previous private key does not survive a round trip.
//! Symmetric/HMAC key material IS preserved.

use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

use crate::models::{CustomKeyStore, Grant, Key, KmsState};

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

pub(crate) mod rw_b64 {
    use base64::Engine;
    use parking_lot::RwLock;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &RwLock<Vec<u8>>, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&base64::engine::general_purpose::STANDARD.encode(&*v.read()))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<RwLock<Vec<u8>>, D::Error> {
        let text = String::deserialize(d)?;
        let data = base64::engine::general_purpose::STANDARD
            .decode(text.as_bytes())
            .map_err(serde::de::Error::custom)?;
        Ok(RwLock::new(data))
    }
}

impl KmsState {
    /// Serialize this store's keys, aliases, tags, grants and key stores.
    pub fn snapshot_json(&self) -> Value {
        let mut keys = serde_json::Map::new();
        for (id, k) in self.keys.read().iter() {
            keys.insert(
                id.clone(),
                serde_json::to_value(&**k).unwrap_or(Value::Null),
            );
        }
        let mut key_stores = serde_json::Map::new();
        for (id, ks) in self.custom_key_stores.read().iter() {
            key_stores.insert(
                id.clone(),
                serde_json::to_value(&**ks).unwrap_or(Value::Null),
            );
        }
        serde_json::json!({
            "keys": Value::Object(keys),
            "aliases": *self.aliases.read(),
            "tags": *self.tags.read(),
            "grants": *self.grants.read(),
            "custom_key_stores": Value::Object(key_stores),
        })
    }

    /// Replace this store's contents from `v`.
    pub fn restore_json(&self, v: &Value) {
        let empty = serde_json::Map::new();

        let mut keys: HashMap<String, Arc<Key>> = HashMap::new();
        for (id, k) in v.get("keys").and_then(Value::as_object).unwrap_or(&empty) {
            if let Ok(key) = serde_json::from_value::<Key>(k.clone()) {
                keys.insert(id.clone(), Arc::new(key));
            }
        }
        for key in keys.values() {
            key.generate_key_material();
        }
        *self.keys.write() = keys;

        let aliases = v
            .get("aliases")
            .cloned()
            .and_then(|a| serde_json::from_value::<HashMap<String, String>>(a).ok())
            .unwrap_or_default();
        *self.aliases.write() = aliases;

        let tags = v
            .get("tags")
            .cloned()
            .and_then(|a| {
                serde_json::from_value::<HashMap<String, serde_json::Map<String, Value>>>(a).ok()
            })
            .unwrap_or_default();
        *self.tags.write() = tags;

        let grants = v
            .get("grants")
            .cloned()
            .and_then(|a| serde_json::from_value::<Vec<Grant>>(a).ok())
            .unwrap_or_default();
        *self.grants.write() = grants;

        let mut stores: HashMap<String, Arc<CustomKeyStore>> = HashMap::new();
        for (id, ks) in v
            .get("custom_key_stores")
            .and_then(Value::as_object)
            .unwrap_or(&empty)
        {
            if let Ok(ks) = serde_json::from_value::<CustomKeyStore>(ks.clone()) {
                stores.insert(id.clone(), Arc::new(ks));
            }
        }
        *self.custom_key_stores.write() = stores;
    }
}

impl crate::handler::KmsHandler {
    /// Serialize the full handler state (all accounts/regions) to JSON.
    pub fn snapshot_json(&self) -> Value {
        self.state
            .read()
            .iter()
            .map(|((account, region), st)| (format!("{account}|{region}"), st.snapshot_json()))
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
                st.restore_json(&empty);
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
            st.restore_json(store_state);
        }
    }

    /// Clear all KMS state (every account/region).
    pub fn reset(&self) {
        let empty = serde_json::Value::Object(serde_json::Map::new());
        let mut states = self.state.write();
        for st in states.values_mut() {
            st.restore_json(&empty);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Forward-compat: a snapshot written by a binary predating the
    /// deletion-schedule fields would fail `Key` deserialization and the
    /// restore loop silently DROPS unparseable rows — every key wiped on
    /// `state/load` after an upgrade. `serde(default)` on both fields
    /// keeps such rows parseable; this test pins that.
    #[test]
    fn test_snapshot_restore_tolerates_missing_deletion_fields() {
        let handler = crate::handler::KmsHandler::new();
        let st = handler.get_state(424242424242, "old-snap-1");
        let key = Key::new(
            424242424242,
            "old-snap-1".to_string(),
            "ENCRYPT_DECRYPT",
            "SYMMETRIC_DEFAULT",
        );
        st.put_key(Arc::new(key));
        let key = st.list_keys().into_iter().next().unwrap();
        let key_id = key.key_id.clone();

        // Snapshot, then strip the two new fields like an old snapshot would.
        let snap = handler.snapshot_json();
        let mut snap = snap;
        if let Some(keys) = snap.get_mut("keys").and_then(Value::as_object_mut) {
            for entry in keys.values_mut() {
                if let Some(obj) = entry.as_object_mut() {
                    obj.remove("deletion_ms");
                    obj.remove("pending_window_days");
                }
            }
        }

        // Empty the partition entirely, then restore the stripped snapshot
        // (replace semantics, like `state/load` on a new partition).
        st.keys.write().clear();
        st.aliases.write().clear();
        handler.restore_json(&snap);

        let restored = st.list_keys();
        assert_eq!(
            restored.len(),
            1,
            "the key survives the upgrade — keys must not be wiped"
        );
        assert_eq!(restored[0].key_id, key_id);
        assert_eq!(
            *restored[0].deletion_ms.read(),
            None,
            "missing fields default to None"
        );

        // Cleanup: do not leak state into other tests in this process.
        st.keys.write().clear();
        st.aliases.write().clear();
    }

    #[test]
    fn test_snapshot_round_trip_restores_keys_and_aliases() {
        let handler = crate::handler::KmsHandler::new();
        let st = handler.get_state(424242424242, "eu-roundtrip-1");
        let key = Key::new(
            424242424242,
            "eu-roundtrip-1".to_string(),
            "ENCRYPT_DECRYPT",
            "SYMMETRIC_DEFAULT",
        );
        st.put_key(Arc::new(key));
        let key = st.list_keys().into_iter().next().unwrap();
        let key_id = key.key_id.clone();
        st.create_alias("alias/rt-test", &key_id);

        let snap = handler.snapshot_json();
        assert!(snap.to_string().contains(&key_id));
        let aliases_before = st.aliases.read().get("alias/rt-test").cloned();

        // Mutate after the snapshot: forget the alias.
        st.aliases.write().clear();

        handler.restore_json(&snap);
        assert_eq!(
            st.aliases.read().get("alias/rt-test").cloned(),
            aliases_before,
            "alias must survive the round trip"
        );
        let material = st.get_key(&key_id).expect("key restored");
        assert_eq!(material.key_material.read().len(), 32);

        // Cleanup: do not leak state into other tests in this process.
        st.keys.write().clear();
        st.aliases.write().clear();
    }
}
