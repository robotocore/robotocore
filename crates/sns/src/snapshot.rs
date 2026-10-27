//! State snapshot/restore surface for the SNS handler.
//!
//! `snapshot_json` serializes every (account, region) store into a JSON
//! value; `restore_json` replaces store contents from that shape. Used by
//! the server's `/_robotocore/state/*` endpoints.

use parking_lot::RwLock;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

use crate::models::{
    PlatformApplication, PlatformEndpoint, SmsSandboxNumber, SnsState, Subscription, Topic,
};

/// Serde with-helpers for snapshot-friendly field types.
pub(crate) mod helper {
    use parking_lot::RwLock;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::sync::Arc;

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

    /// `Arc<RwLock<T>>` as plain `T`.
    pub mod arc_rw {
        use super::*;

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
}

pub(crate) use helper::{arc_rw, rw};

impl SnsState {
    /// Serialize topics, subscriptions, platform entities and SMS state.
    pub fn snapshot_json(&self) -> Value {
        let map_value = |m: &RwLock<HashMap<String, Arc<Topic>>>| {
            let mut out = serde_json::Map::new();
            for (name, t) in m.read().iter() {
                out.insert(
                    name.clone(),
                    serde_json::to_value(&**t).unwrap_or(Value::Null),
                );
            }
            Value::Object(out)
        };
        let topics = map_value(&self.topics);

        let mut apps = serde_json::Map::new();
        for (arn, a) in self.platform_applications.read().iter() {
            apps.insert(
                arn.clone(),
                serde_json::to_value(&**a).unwrap_or(Value::Null),
            );
        }
        let mut endpoints = serde_json::Map::new();
        for (arn, e) in self.platform_endpoints.read().iter() {
            endpoints.insert(
                arn.clone(),
                serde_json::to_value(&**e).unwrap_or(Value::Null),
            );
        }
        let subs: Vec<Value> = self
            .subscriptions
            .read()
            .iter()
            .map(|s| serde_json::to_value(s).unwrap_or(Value::Null))
            .collect();
        let mut sandbox = serde_json::Map::new();
        for (phone, n) in self.sms_sandbox_numbers.read().iter() {
            sandbox.insert(
                phone.clone(),
                serde_json::to_value(n).unwrap_or(Value::Null),
            );
        }
        serde_json::json!({
            "topics": topics,
            "subscriptions": subs,
            "platform_applications": Value::Object(apps),
            "platform_endpoints": Value::Object(endpoints),
            "sms_attributes": *self.sms_attributes.read(),
            "opted_out": *self.opted_out.read(),
            "sms_sandbox_numbers": Value::Object(sandbox),
        })
    }

    /// Replace this store's contents from `v`.
    pub fn restore_json(&self, v: &Value) {
        let empty = serde_json::Map::new();

        let mut topics: HashMap<String, Arc<Topic>> = HashMap::new();
        for (arn, t) in v.get("topics").and_then(Value::as_object).unwrap_or(&empty) {
            if let Ok(topic) = serde_json::from_value::<Topic>(t.clone()) {
                topics.insert(arn.clone(), Arc::new(topic));
            }
        }
        *self.topics.write() = topics;

        let mut subs: Vec<Subscription> = Vec::new();
        for s in v
            .get("subscriptions")
            .and_then(Value::as_array)
            .unwrap_or(&vec![])
        {
            if let Ok(sub) = serde_json::from_value::<Subscription>(s.clone()) {
                subs.push(sub);
            }
        }
        *self.subscriptions.write() = subs;

        let mut apps: HashMap<String, Arc<PlatformApplication>> = HashMap::new();
        for (arn, a) in v
            .get("platform_applications")
            .and_then(Value::as_object)
            .unwrap_or(&empty)
        {
            if let Ok(app) = serde_json::from_value::<PlatformApplication>(a.clone()) {
                apps.insert(arn.clone(), Arc::new(app));
            }
        }
        *self.platform_applications.write() = apps;

        let mut ends: HashMap<String, Arc<PlatformEndpoint>> = HashMap::new();
        for (arn, e) in v
            .get("platform_endpoints")
            .and_then(Value::as_object)
            .unwrap_or(&empty)
        {
            if let Ok(ep) = serde_json::from_value::<PlatformEndpoint>(e.clone()) {
                ends.insert(arn.clone(), Arc::new(ep));
            }
        }
        *self.platform_endpoints.write() = ends;

        let sms_attrs: Option<HashMap<String, String>> = v
            .get("sms_attributes")
            .cloned()
            .and_then(|a| serde_json::from_value::<HashMap<String, String>>(a).ok());
        if let Some(attrs) = sms_attrs {
            *self.sms_attributes.write() = attrs;
        }

        let mut opted: std::collections::HashSet<String> = std::collections::HashSet::new();
        for o in v
            .get("opted_out")
            .and_then(Value::as_array)
            .unwrap_or(&vec![])
        {
            if let Some(phone) = o.as_str() {
                opted.insert(phone.to_string());
            }
        }
        *self.opted_out.write() = opted;

        let mut sandbox: HashMap<String, SmsSandboxNumber> = HashMap::new();
        for (phone, n) in v
            .get("sms_sandbox_numbers")
            .and_then(Value::as_object)
            .unwrap_or(&empty)
        {
            if let Ok(num) = serde_json::from_value::<SmsSandboxNumber>(n.clone()) {
                sandbox.insert(phone.clone(), num);
            }
        }
        *self.sms_sandbox_numbers.write() = sandbox;
    }
}

impl crate::handler::SnsHandler {
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

    /// Clear all SNS state (every account/region).
    pub fn reset(&self) {
        let empty = serde_json::Value::Object(serde_json::Map::new());
        let mut states = self.state.write();
        for st in states.values_mut() {
            st.restore_json(&empty);
        }
    }
}
