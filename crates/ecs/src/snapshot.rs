//! State snapshot/restore surface for the ecs handler.
//!
//! `snapshot_json` serializes every (account, region) store into a JSON
//! value; `restore_json` replaces store contents from that shape. Used by
//! the server's `/_robotocore/state/*` endpoints.

use serde_json::Value;

use crate::models::EcsState;

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

/// Replace one store's contents in place from `v`, preserving `Arc` identity.
fn store_restore_json(st: &EcsState, v: &Value) {
    let restored: EcsState = serde_json::from_value(v.clone()).unwrap_or_default();
    macro_rules! swap {
        ($field:ident) => {
            *st.$field.write() = restored.$field.read().clone();
        };
    }
    swap!(clusters);
    swap!(services);
    swap!(tasks);
    swap!(container_instances);
    swap!(task_definitions);
    swap!(task_sets);
    swap!(attributes);
    swap!(tags);
    swap!(account_settings);
    swap!(capacity_providers);
}

impl crate::handler::EcsHandler {
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

    /// Clear all ECS state (every account/region).
    pub fn reset(&self) {
        let empty = serde_json::Value::Object(serde_json::Map::new());
        let mut states = self.state.write();
        for st in states.values_mut() {
            store_restore_json(st, &empty);
        }
    }
}
