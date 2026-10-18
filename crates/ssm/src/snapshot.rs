//! State snapshot/restore surface for the SSM handler.
//!
//! `snapshot_json` serializes every (account, region) store into a JSON
//! value; `restore_json` replaces store contents from that shape. Used by
//! the server's `/_robotocore/state/*` endpoints.

use parking_lot::RwLock;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::models::{Command, Parameter, PatchGroupTable, SsmState};

/// `RwLock<T>` as plain `T` (for derived leaf models in models.rs).
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

/// Lock-wrapped map of lock wrappers (documents, associations, ...).
pub(crate) type LockedMap<V> = Arc<RwLock<BTreeMap<String, Arc<RwLock<V>>>>>;

/// Convert a lock-wrapped map of lock wrappers into a JSON object.
fn btree_rwlock_map_json<V: serde::Serialize>(m: &LockedMap<V>) -> Value {
    let mut out = serde_json::Map::new();
    let outer = m.read();
    for (k, store) in outer.iter() {
        out.insert(
            k.clone(),
            serde_json::to_value(&*store.read()).unwrap_or(Value::Null),
        );
    }
    Value::Object(out)
}

fn btree_rwlock_map_from<V, F>(v: &Value, key: &str, make: F) -> BTreeMap<String, Arc<RwLock<V>>>
where
    V: serde::de::DeserializeOwned,
    F: Fn(V) -> Arc<RwLock<V>>,
{
    let mut out: BTreeMap<String, Arc<RwLock<V>>> = BTreeMap::new();
    if let Some(entries) = v.get(key).and_then(Value::as_object) {
        for (k, val) in entries {
            if let Ok(val) = serde_json::from_value::<V>(val.clone()) {
                out.insert(k.clone(), make(val));
            }
        }
    }
    out
}

/// Convert a lock-wrapped map of lock-wrapped values into a JSON object.
fn btree_rwlock_val_json(m: &Arc<RwLock<BTreeMap<String, RwLock<Value>>>>) -> Value {
    let mut out = serde_json::Map::new();
    let outer = m.read();
    for (k, store) in outer.iter() {
        out.insert(k.clone(), store.read().clone());
    }
    Value::Object(out)
}

fn btree_rwlock_val_from(v: &Value) -> BTreeMap<String, RwLock<Value>> {
    let mut out: BTreeMap<String, RwLock<Value>> = BTreeMap::new();
    if let Some(entries) = v.as_object() {
        for (k, val) in entries {
            out.insert(k.clone(), RwLock::new(val.clone()));
        }
    }
    out
}

/// Serialize a simple lock-wrapped map of shared values.
fn btree_shared_map_json<V: serde::Serialize>(m: &Arc<RwLock<BTreeMap<String, Arc<V>>>>) -> Value {
    let map = m.read();
    let mut out = serde_json::Map::new();
    for (k, val) in map.iter() {
        out.insert(
            k.clone(),
            serde_json::to_value(&**val).unwrap_or(Value::Null),
        );
    }
    Value::Object(out)
}

fn btree_shared_map_from<V, F>(v: &Value, key: &str, make: F) -> BTreeMap<String, Arc<V>>
where
    V: serde::de::DeserializeOwned,
    F: Fn(V) -> Arc<V>,
{
    let mut out: BTreeMap<String, Arc<V>> = BTreeMap::new();
    if let Some(entries) = v.get(key).and_then(Value::as_object) {
        for (k, val) in entries {
            if let Ok(val) = serde_json::from_value::<V>(val.clone()) {
                out.insert(k.clone(), make(val));
            }
        }
    }
    out
}

/// Patch-group table (`Option<String>` inner keys) as grouped triple lists.
fn patch_groups_json(t: &Arc<RwLock<PatchGroupTable>>) -> Value {
    let mut out = serde_json::Map::new();
    let outer = t.read();
    for (group, inner) in outer.iter() {
        let pairs: Vec<Value> = inner
            .read()
            .iter()
            .map(|(k, v)| serde_json::json!([k.clone(), v]))
            .collect();
        out.insert(group.clone(), Value::Array(pairs));
    }
    Value::Object(out)
}

fn patch_groups_from(v: &Value) -> PatchGroupTable {
    let mut out: PatchGroupTable = BTreeMap::new();
    if let Some(entries) = v.as_object() {
        for (group, pairs) in entries {
            let mut inner: BTreeMap<Option<String>, String> = BTreeMap::new();
            if let Some(pairs) = pairs.as_array() {
                for p in pairs {
                    let Some(arr) = p.as_array() else {
                        continue;
                    };
                    if arr.len() != 2 {
                        continue;
                    }
                    let key = match &arr[0] {
                        Value::String(s) => Some(s.clone()),
                        Value::Null => None,
                        _ => continue,
                    };
                    let value = arr[1].as_str().unwrap_or_default();
                    inner.insert(key, value.to_string());
                }
            }
            out.insert(group.clone(), Arc::new(RwLock::new(inner)));
        }
    }
    out
}

impl SsmState {
    /// Serialize this store into a JSON value.
    pub fn snapshot_json(&self) -> Value {
        let params = self.parameters.read();
        let mut parameters = serde_json::Map::new();
        for (name, p) in params.iter() {
            parameters.insert(
                name.clone(),
                serde_json::to_value(&**p).unwrap_or(Value::Null),
            );
        }
        let commands: Vec<Value> = self
            .commands
            .read()
            .iter()
            .map(|c| serde_json::to_value(&**c).unwrap_or(Value::Null))
            .collect();
        serde_json::json!({
            "parameters": Value::Object(parameters),
            "documents": btree_rwlock_map_json(&self.documents),
            "document_tags": *self.document_tags.read(),
            "resource_tags": *self.resource_tags.read(),
            "commands": commands,
            "windows": btree_shared_map_json(&self.windows),
            "baselines": btree_shared_map_json(&self.baselines),
            "patch_groups": patch_groups_json(&self.patch_groups),
            "associations": btree_rwlock_map_json(&self.associations),
            "ops_items": btree_rwlock_map_json(&self.ops_items),
            "ops_metadata": btree_rwlock_map_json(&self.ops_metadata),
            "activations": btree_shared_map_json(&self.activations),
            "resource_data_syncs": btree_rwlock_map_json(&self.resource_data_syncs),
            "automation_executions": btree_shared_map_json(&self.automation_executions),
            "resource_policies": btree_rwlock_val_json(&self.resource_policies),
        })
    }

    /// Replace this store's contents from `v`.
    pub fn restore_json(&self, v: &Value) {
        let empty = serde_json::Map::new();

        let mut parameters: BTreeMap<String, Arc<Parameter>> = BTreeMap::new();
        for (name, p) in v
            .get("parameters")
            .and_then(Value::as_object)
            .unwrap_or(&empty)
        {
            if let Ok(param) = serde_json::from_value::<Parameter>(p.clone()) {
                parameters.insert(name.clone(), Arc::new(param));
            }
        }
        *self.parameters.write() = parameters;

        let commands: Vec<Arc<Command>> = v
            .get("commands")
            .and_then(Value::as_array)
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|c| {
                serde_json::from_value::<Command>(c.clone())
                    .ok()
                    .map(Arc::new)
            })
            .collect();
        *self.commands.write() = commands;

        let null = Value::Null;
        let get_or_null = |k: &str| v.get(k).unwrap_or(&null);

        *self.documents.write() =
            btree_rwlock_map_from::<crate::models::Documents, _>(v, "documents", |d| {
                Arc::new(RwLock::new(d))
            });
        *self.document_tags.write() = get_or_null("document_tags")
            .as_object()
            .map(|m| {
                m.iter()
                    .filter_map(|(k, vv)| {
                        serde_json::from_value::<Vec<(String, String)>>(vv.clone())
                            .ok()
                            .map(|list| (k.clone(), list))
                    })
                    .collect()
            })
            .unwrap_or_default();
        *self.resource_tags.write() = get_or_null("resource_tags")
            .as_object()
            .map(|m| {
                m.iter()
                    .filter_map(|(k, vv)| {
                        serde_json::from_value::<BTreeMap<String, BTreeMap<String, String>>>(
                            vv.clone(),
                        )
                        .ok()
                        .map(|tags| (k.clone(), tags))
                    })
                    .collect()
            })
            .unwrap_or_default();
        *self.windows.write() =
            btree_shared_map_from::<crate::models::MaintenanceWindow, _>(v, "windows", Arc::new);
        *self.baselines.write() =
            btree_shared_map_from::<crate::models::PatchBaseline, _>(v, "baselines", Arc::new);
        *self.patch_groups.write() = patch_groups_from(get_or_null("patch_groups"));
        *self.associations.write() =
            btree_rwlock_map_from::<crate::models::Association, _>(v, "associations", |a| {
                Arc::new(RwLock::new(a))
            });
        *self.ops_items.write() =
            btree_rwlock_map_from::<crate::models::OpsItem, _>(v, "ops_items", |o| {
                Arc::new(RwLock::new(o))
            });
        *self.ops_metadata.write() =
            btree_rwlock_map_from::<crate::models::OpsMetadata, _>(v, "ops_metadata", |o| {
                Arc::new(RwLock::new(o))
            });
        *self.activations.write() =
            btree_shared_map_from::<crate::models::Activation, _>(v, "activations", Arc::new);
        *self.resource_data_syncs.write() = btree_rwlock_map_from::<
            crate::models::ResourceDataSync,
            _,
        >(v, "resource_data_syncs", |rds| {
            Arc::new(RwLock::new(rds))
        });
        *self.automation_executions.write() =
            btree_shared_map_from::<AutomationExecution, _>(v, "automation_executions", Arc::new);
        *self.resource_policies.write() = btree_rwlock_val_from(get_or_null("resource_policies"));
    }
}

use crate::models::AutomationExecution;

impl crate::handler::SsmHandler {
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

    /// Clear all SSM state (every account/region).
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

    #[test]
    fn test_snapshot_round_trip_restores_parameter() {
        let handler = crate::handler::SsmHandler::new();
        let st = handler.get_state(424242424242, "eu-roundtrip-1");
        st.parameters.write().insert(
            "rt-param".to_string(),
            Arc::new(Parameter {
                name: "rt-param".to_string(),
                versions: RwLock::new(vec![crate::models::ParamVersion {
                    version: 1,
                    value: "production".to_string(),
                    parameter_type: "String".to_string(),
                    data_type: "text".to_string(),
                    description: None,
                    allowed_pattern: None,
                    keyid: None,
                    tier: "Standard".to_string(),
                    policies: None,
                    labels: vec![],
                    last_modified: chrono::Utc::now(),
                }]),
            }),
        );

        let snap = handler.snapshot_json();
        assert!(snap.to_string().contains("production"));

        // Mutate after the snapshot: remove the parameter.
        st.parameters.write().clear();

        handler.restore_json(&snap);
        let p = st.get_parameter("rt-param").expect("parameter restored");
        assert_eq!(p.current().map(|v| v.value), Some("production".to_string()));

        // Cleanup: do not leak state into other tests in this process.
        st.parameters.write().clear();
    }
}
