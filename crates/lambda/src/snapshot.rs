//! State snapshot/restore surface for the Lambda handler.
//!
//! `snapshot_json` serializes every (account, region) store into a JSON
//! value; `restore_json` replaces store contents from that shape. Used by
//! the server's `/_robotocore/state/*` endpoints.
//!
//! Tuple-keyed maps (`(function, qualifier)`) are serialized as objects
//! with `"name::qualifier"` keys; lambda function/alias/qualifier names
//! are AWS-restricted to `[A-Za-z0-9-_]` so the separator is unambiguous.

use crate::handler::LambdaHandler;
use crate::models::{
    CodeSigningConfig, EventSourceMapping, FunctionEntry, LambdaAlias, LambdaState, Layer,
};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

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

/// `RwLock<Vec<u8>>` as base64 text.
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

/// `Vec<u8>` as base64 text.
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

impl LambdaState {
    /// Serialize this store into a JSON value.
    pub fn snapshot_json(&self) -> Value {
        let mut out = serde_json::Map::new();

        let mut functions = serde_json::Map::new();
        for (name, fe) in self.functions.read().iter() {
            functions.insert(
                name.clone(),
                serde_json::to_value(fe).unwrap_or(Value::Null),
            );
        }
        out.insert("functions".into(), Value::Object(functions));

        let mut aliases = serde_json::Map::new();
        for ((a, b), alias) in self.aliases.read().iter() {
            aliases.insert(
                format!("{a}::{b}"),
                serde_json::to_value(&**alias).unwrap_or(Value::Null),
            );
        }
        out.insert("aliases".into(), Value::Object(aliases));

        let mut mappings = serde_json::Map::new();
        for (uuid, m) in self.event_source_mappings.read().iter() {
            mappings.insert(
                uuid.clone(),
                serde_json::to_value(&**m).unwrap_or(Value::Null),
            );
        }
        out.insert("event_source_mappings".into(), Value::Object(mappings));

        let mut layers = serde_json::Map::new();
        for (name, l) in self.layers.read().iter() {
            layers.insert(name.clone(), serde_json::to_value(l).unwrap_or(Value::Null));
        }
        out.insert("layers".into(), Value::Object(layers));

        out.insert(
            "url_configs".into(),
            serde_json::to_value(&*self.url_configs.read()).unwrap_or(Value::Null),
        );

        let mut provisioned = serde_json::Map::new();
        for ((a, b), v) in self.provisioned.read().iter() {
            provisioned.insert(format!("{a}::{b}"), v.clone());
        }
        out.insert("provisioned".into(), Value::Object(provisioned));

        let mut csc = serde_json::Map::new();
        for (arn, c) in self.code_signing_configs.read().iter() {
            csc.insert(
                arn.clone(),
                serde_json::to_value(&**c).unwrap_or(Value::Null),
            );
        }
        out.insert("code_signing_configs".into(), Value::Object(csc));

        let mut perms = serde_json::Map::new();
        for ((name, ver), btree) in self.layer_permissions.read().iter() {
            perms.insert(
                format!("{name}::{ver}"),
                serde_json::to_value(btree).unwrap_or(Value::Null),
            );
        }
        out.insert("layer_permissions".into(), Value::Object(perms));

        Value::Object(out)
    }

    /// Replace this store's contents from `v` (Arcs keep their identity).
    pub fn restore_json(&self, v: &Value) {
        let empty = serde_json::Map::new();

        let mut functions: HashMap<String, FunctionEntry> = HashMap::new();
        for (name, f) in v
            .get("functions")
            .and_then(Value::as_object)
            .unwrap_or(&empty)
        {
            if let Ok(fe) = serde_json::from_value::<FunctionEntry>(f.clone()) {
                functions.insert(name.clone(), fe);
            }
        }
        *self.functions.write() = functions;

        let mut aliases: HashMap<(String, String), Arc<LambdaAlias>> = HashMap::new();
        for (key, a) in v
            .get("aliases")
            .and_then(Value::as_object)
            .unwrap_or(&empty)
        {
            let Some((fn_name, alias_name)) = key.split_once("::") else {
                continue;
            };
            let Ok(alias) = serde_json::from_value::<LambdaAlias>(a.clone()) else {
                continue;
            };
            aliases.insert(
                (fn_name.to_string(), alias_name.to_string()),
                Arc::new(alias),
            );
        }
        *self.aliases.write() = aliases;

        let mut mappings: HashMap<String, Arc<EventSourceMapping>> = HashMap::new();
        for (uuid, m) in v
            .get("event_source_mappings")
            .and_then(Value::as_object)
            .unwrap_or(&empty)
        {
            if let Ok(m) = serde_json::from_value::<EventSourceMapping>(m.clone()) {
                mappings.insert(uuid.clone(), Arc::new(m));
            }
        }
        *self.event_source_mappings.write() = mappings;

        let mut layers: HashMap<String, Layer> = HashMap::new();
        for (name, l) in v.get("layers").and_then(Value::as_object).unwrap_or(&empty) {
            if let Ok(l) = serde_json::from_value::<Layer>(l.clone()) {
                layers.insert(name.clone(), l);
            }
        }
        *self.layers.write() = layers;

        let empty_urls = HashMap::new();
        *self.url_configs.write() = v
            .get("url_configs")
            .and_then(|u| serde_json::from_value::<HashMap<String, Value>>(u.clone()).ok())
            .unwrap_or(empty_urls);

        let mut provisioned: HashMap<(String, String), Value> = HashMap::new();
        for (key, val) in v
            .get("provisioned")
            .and_then(Value::as_object)
            .unwrap_or(&empty)
        {
            let Some((a, b)) = key.split_once("::") else {
                continue;
            };
            provisioned.insert((a.to_string(), b.to_string()), val.clone());
        }
        *self.provisioned.write() = provisioned;

        let mut csc: HashMap<String, Arc<CodeSigningConfig>> = HashMap::new();
        for (arn, c) in v
            .get("code_signing_configs")
            .and_then(Value::as_object)
            .unwrap_or(&empty)
        {
            if let Ok(c) = serde_json::from_value::<CodeSigningConfig>(c.clone()) {
                csc.insert(arn.clone(), Arc::new(c));
            }
        }
        *self.code_signing_configs.write() = csc;

        let mut perms: HashMap<(String, i64), BTreeMap<String, Value>> = HashMap::new();
        for (key, btree) in v
            .get("layer_permissions")
            .and_then(Value::as_object)
            .unwrap_or(&empty)
        {
            let Some((layer, ver)) = key.rsplit_once("::") else {
                continue;
            };
            let Ok(ver) = ver.parse::<i64>() else {
                continue;
            };
            if let Ok(btree) = serde_json::from_value::<BTreeMap<String, Value>>(btree.clone()) {
                perms.insert((layer.to_string(), ver), btree);
            }
        }
        *self.layer_permissions.write() = perms;
    }
}

impl LambdaHandler {
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
            let st = self.state_for(account, region);
            st.restore_json(store_state);
        }
    }

    /// Clear all Lambda state (every account/region).
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
    use crate::models::FunctionEntry;

    #[test]
    fn test_snapshot_round_trip_restores_function_and_alias() {
        let handler = LambdaHandler::new();
        let st = handler.state_for(424242424242, "eu-roundtrip-1");
        let entry = FunctionEntry {
            latest: None,
            versions: Vec::new(),
        };
        st.functions
            .write()
            .entry("rt-fn".to_string())
            .or_insert(entry);

        let snap = handler.snapshot_json();
        assert!(snap.to_string().contains("rt-fn"), "fn in snapshot");

        // Mutate after the snapshot: remove the function.
        st.functions.write().clear();

        handler.restore_json(&snap);
        assert!(st.functions.read().contains_key("rt-fn"));

        // Cleanup: do not leak state into other tests in this process.
        st.functions.write().clear();
    }
}
