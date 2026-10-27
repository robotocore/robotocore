//! State snapshot/restore surface for the Kinesis handler.
//!
//! `snapshot_json` serializes every (account, region) store into a JSON
//! value; `restore_json` replaces store contents from that shape. Used by
//! the server's `/_robotocore/state/*` endpoints.

use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::models::{KinesisRecord, KinesisState, KinesisStream};

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

pub(crate) mod u128_str {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &u128, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&v.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<u128, D::Error> {
        let text = String::deserialize(d)?;
        text.parse::<u128>().map_err(serde::de::Error::custom)
    }
}

pub(crate) mod mutex_serde {
    use parking_lot::Mutex;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use crate::models::KinesisRecord;

    pub fn serialize<S: Serializer>(
        v: &Mutex<Vec<KinesisRecord>>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        Vec::<KinesisRecord>::serialize(&*v.lock(), s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<Mutex<Vec<KinesisRecord>>, D::Error> {
        Ok(Mutex::new(Vec::<KinesisRecord>::deserialize(d)?))
    }
}

pub(crate) mod mutex_u64 {
    use parking_lot::Mutex;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(v: &Mutex<u64>, s: S) -> Result<S::Ok, S::Error> {
        u64::serialize(&*v.lock(), s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Mutex<u64>, D::Error> {
        Ok(Mutex::new(u64::deserialize(d)?))
    }
}

impl KinesisState {
    /// Serialize this store into a JSON value.
    pub fn snapshot_json(&self) -> Value {
        let mut streams = serde_json::Map::new();
        for (name, s) in self.streams.read().iter() {
            streams.insert(
                name.clone(),
                serde_json::to_value(&**s).unwrap_or(Value::Null),
            );
        }
        serde_json::json!({
            "streams": Value::Object(streams),
            "resource_policies": &*self.resource_policies.lock(),
        })
    }

    /// Replace this store's contents from `v`.
    pub fn restore_json(&self, v: &Value) {
        let empty = serde_json::Map::new();

        let mut streams: BTreeMap<String, Arc<KinesisStream>> = BTreeMap::new();
        for (name, s) in v
            .get("streams")
            .and_then(Value::as_object)
            .unwrap_or(&empty)
        {
            if let Ok(stream) = serde_json::from_value::<KinesisStream>(s.clone()) {
                streams.insert(name.clone(), Arc::new(stream));
            }
        }
        *self.streams.write() = streams;

        let mut policies: HashMap<String, String> = HashMap::new();
        for (name, p) in v
            .get("resource_policies")
            .and_then(Value::as_object)
            .unwrap_or(&empty)
        {
            if let Some(p) = p.as_str() {
                policies.insert(name.clone(), p.to_string());
            }
        }
        *self.resource_policies.lock() = policies;
    }
}

impl crate::handler::KinesisHandler {
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

    /// Clear all Kinesis state (every account/region).
    pub fn reset(&self) {
        let empty = serde_json::Value::Object(serde_json::Map::new());
        let mut states = self.state.write();
        for st in states.values_mut() {
            st.restore_json(&empty);
        }
    }
}

/// Records and consumers travel inside streams; keep the models referenced
/// for the handler-level docs.
#[allow(dead_code)]
fn _type_sink(_: &KinesisRecord) {}
