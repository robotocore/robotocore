//! State snapshot/restore surface for the Secrets Manager handler.
//!
//! `snapshot_json` serializes every (account, region) store into a JSON
//! value; `restore_json` replaces store contents from that shape. Used by
//! the server's `/_robotocore/state/*` endpoints.

use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

use crate::models::{Secret, SmState};

/// Serde with-helpers for snapshot-friendly field types.
pub(crate) mod helper {
    use parking_lot::RwLock;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    /// `RwLock<T>` as plain `T`.
    pub mod rw {
        use super::*;

        pub fn serialize<T: Serialize, S: Serializer>(
            v: &RwLock<T>,
            s: S,
        ) -> Result<S::Ok, S::Error> {
            T::serialize(&*v.read(), s)
        }

        pub fn deserialize<'de, T: Deserialize<'de>, D: Deserializer<'de>>(
            d: D,
        ) -> Result<RwLock<T>, D::Error> {
            Ok(RwLock::new(T::deserialize(d)?))
        }
    }
}

pub(crate) use helper::rw;

impl SmState {
    /// Serialize this store's secrets.
    pub fn snapshot_json(&self) -> Value {
        let mut out = serde_json::Map::new();
        for (name, s) in self.secrets.read().iter() {
            out.insert(
                name.clone(),
                serde_json::to_value(&**s).unwrap_or(Value::Null),
            );
        }
        Value::Object(out)
    }

    /// Replace this store's secrets from `v`.
    pub fn restore_json(&self, v: &Value) {
        let empty = serde_json::Map::new();
        let mut secrets: HashMap<String, Arc<Secret>> = HashMap::new();
        for (name, s) in v.as_object().unwrap_or(&empty) {
            if let Ok(secret) = serde_json::from_value::<Secret>(s.clone()) {
                secrets.insert(name.clone(), Arc::new(secret));
            }
        }
        *self.secrets.write() = secrets;
    }
}

impl crate::handler::SecretsManagerHandler {
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

    /// Clear all Secrets Manager state (every account/region).
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
    use crate::handler::SecretsManagerHandler;

    #[test]
    fn test_snapshot_round_trip_restores_secret() {
        let handler = SecretsManagerHandler::new();
        let st = handler.get_state(424242424242, "eu-roundtrip-1");
        let secret = Secret::new(
            "rt-secret".to_string(),
            424242424242,
            "eu-roundtrip-1".to_string(),
        );
        secret.versions.write().insert(
            "AWSCURRENT".to_string(),
            crate::models::SecretVersion {
                version_id: "v1".to_string(),
                secret_string: Some("hunter2".to_string()),
                secret_binary: None,
                stages: vec!["AWSCURRENT".to_string()],
                created_date: 123.0,
            },
        );
        st.put_secret(Arc::new(secret));

        let snap = handler.snapshot_json();
        assert!(snap.to_string().contains("hunter2"));

        // Mutate after the snapshot: delete the secret.
        st.delete_secret("rt-secret");

        handler.restore_json(&snap);
        let restored = st.get_secret("rt-secret").expect("secret restored");
        let version = restored
            .get_version("AWSCURRENT")
            .expect("version restored");
        assert_eq!(version.secret_string.as_deref(), Some("hunter2"));

        // Cleanup: do not leak state into other tests in this process.
        st.delete_secret("rt-secret");
    }
}
