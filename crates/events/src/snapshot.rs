//! State snapshot/restore surface for the EventBridge handler.
//!
//! `snapshot_json` serializes every (account, region) store into a JSON
//! value; `restore_json` replaces store contents from that shape. Used by
//! the server's `/_robotocore/state/*` endpoints.
//!
//! The rule map is keyed by (bus, rule); both names are JSON strings via
//! project validation so the `"bus::rule"` joined key round-trips.

use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

use crate::handler::{EventsHandler, GLOBAL_STATE};
use crate::models::{EventsState, Rule};

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

/// Serialize one store; the rule map uses `"bus::rule"` keys.
fn store_snapshot_json(st: &EventsState) -> Value {
    let mut out = serde_json::Map::new();

    let mut rules = serde_json::Map::new();
    for ((bus, name), rule) in st.rules.read().iter() {
        rules.insert(
            format!("{bus}::{name}"),
            serde_json::to_value(&**rule).unwrap_or(Value::Null),
        );
    }
    out.insert("rules".into(), Value::Object(rules));

    for (key, field) in [
        ("event_buses", &st.event_buses),
        ("bus_policies", &st.bus_policies),
        ("archives", &st.archives),
        ("connections", &st.connections),
        ("api_destinations", &st.api_destinations),
        ("replays", &st.replays),
        ("partner_sources", &st.partner_sources),
        ("endpoints", &st.endpoints),
    ] {
        out.insert(
            key.into(),
            serde_json::to_value(&*field.read()).unwrap_or(Value::Null),
        );
    }
    out.insert(
        "tags".into(),
        serde_json::to_value(&*st.tags.read()).unwrap_or(Value::Null),
    );
    out.insert(
        "archived_events".into(),
        serde_json::to_value(&*st.archived_events.read()).unwrap_or(Value::Null),
    );

    Value::Object(out)
}

/// Replace one store's contents from `v`.
fn store_restore_json(st: &EventsState, v: &Value) {
    let empty = serde_json::Map::new();

    let mut rules: HashMap<(String, String), Arc<Rule>> = HashMap::new();
    for (key, r) in v.get("rules").and_then(Value::as_object).unwrap_or(&empty) {
        let Some((bus, name)) = key.split_once("::") else {
            continue;
        };
        if let Ok(rule) = serde_json::from_value::<Rule>(r.clone()) {
            rules.insert((bus.to_string(), name.to_string()), Arc::new(rule));
        }
    }
    *st.rules.write() = rules;

    macro_rules! swap_field {
        ($field:ident, $key:literal, $ty:ty) => {
            let mut map = <HashMap<String, $ty>>::new();
            for (k, val) in v.get($key).and_then(Value::as_object).unwrap_or(&empty) {
                if let Ok(val) = serde_json::from_value::<$ty>(val.clone()) {
                    map.insert(k.clone(), val);
                }
            }
            *st.$field.write() = map;
        };
    }
    swap_field!(event_buses, "event_buses", Value);
    swap_field!(bus_policies, "bus_policies", Value);
    swap_field!(tags, "tags", Vec<Value>);
    swap_field!(archives, "archives", Value);
    swap_field!(archived_events, "archived_events", Vec<Value>);
    swap_field!(connections, "connections", Value);
    swap_field!(api_destinations, "api_destinations", Value);
    swap_field!(replays, "replays", Value);
    swap_field!(partner_sources, "partner_sources", Value);
    swap_field!(endpoints, "endpoints", Value);
}

impl EventsHandler {
    /// Serialize the full handler state (all accounts/regions) to JSON.
    pub fn snapshot_json(&self) -> Value {
        GLOBAL_STATE
            .read()
            .iter()
            .map(|((account, region), st)| (format!("{account}|{region}"), store_snapshot_json(st)))
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
            let mut states = GLOBAL_STATE.write();
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

    /// Clear all EventBridge state (every account/region).
    pub fn reset(&self) {
        let empty = serde_json::Value::Object(serde_json::Map::new());
        let mut states = GLOBAL_STATE.write();
        for st in states.values_mut() {
            store_restore_json(st, &empty);
        }
    }
}
