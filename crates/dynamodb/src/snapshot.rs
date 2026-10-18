//! State snapshot/restore surface for the DynamoDB handler.
//!
//! `snapshot_json` serializes every (account, region) store into a JSON
//! value; `restore_json` replaces store contents from that shape. Used by
//! the server's `/_robotocore/state/*` endpoints.

use parking_lot::RwLock;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

use crate::models::{Backup, DynamoDbState, Export, GlobalTable, Item, Table};

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

/// Serialize one store's tables/backups/exports/global tables.
fn store_snapshot_json(st: &DynamoDbState) -> Value {
    let map_value = |m: &RwLock<HashMap<String, Arc<Table>>>| {
        let mut out = serde_json::Map::new();
        for (name, t) in m.read().iter() {
            out.insert(
                name.clone(),
                serde_json::to_value(&**t).unwrap_or(Value::Null),
            );
        }
        Value::Object(out)
    };
    let vec_value = |v: &RwLock<Vec<Arc<Backup>>>| {
        Value::Array(
            v.read()
                .iter()
                .map(|b| serde_json::to_value(&**b).unwrap_or(Value::Null))
                .collect(),
        )
    };
    let exports_value = |v: &RwLock<Vec<Arc<Export>>>| {
        Value::Array(
            v.read()
                .iter()
                .map(|e| serde_json::to_value(&**e).unwrap_or(Value::Null))
                .collect(),
        )
    };
    let global_value = |m: &RwLock<HashMap<String, Arc<GlobalTable>>>| {
        let mut out = serde_json::Map::new();
        for (name, g) in m.read().iter() {
            out.insert(
                name.clone(),
                serde_json::to_value(&**g).unwrap_or(Value::Null),
            );
        }
        Value::Object(out)
    };
    serde_json::json!({
        "tables": map_value(&st.tables),
        "backups": vec_value(&st.backups),
        "exports": exports_value(&st.exports),
        "global_tables": global_value(&st.global_tables),
    })
}

/// Replace one store's contents from `v`.
fn store_restore_json(st: &DynamoDbState, v: &Value) {
    let empty = serde_json::Map::new();

    let mut tables: HashMap<String, Arc<Table>> = HashMap::new();
    for (name, t) in v.get("tables").and_then(Value::as_object).unwrap_or(&empty) {
        if let Ok(table) = serde_json::from_value::<Table>(t.clone()) {
            tables.insert(name.clone(), Arc::new(table));
        }
    }
    *st.tables.write() = tables;

    let mut backups: Vec<Arc<Backup>> = Vec::new();
    for b in v
        .get("backups")
        .and_then(Value::as_array)
        .unwrap_or(&vec![])
    {
        if let Ok(backup) = serde_json::from_value::<Backup>(b.clone()) {
            backups.push(Arc::new(backup));
        }
    }
    *st.backups.write() = backups;

    let mut exports: Vec<Arc<Export>> = Vec::new();
    for e in v
        .get("exports")
        .and_then(Value::as_array)
        .unwrap_or(&vec![])
    {
        if let Ok(exp) = serde_json::from_value::<Export>(e.clone()) {
            exports.push(Arc::new(exp));
        }
    }
    *st.exports.write() = exports;

    let mut global: HashMap<String, Arc<GlobalTable>> = HashMap::new();
    for (name, g) in v
        .get("global_tables")
        .and_then(Value::as_object)
        .unwrap_or(&empty)
    {
        if let Ok(gt) = serde_json::from_value::<GlobalTable>(g.clone()) {
            global.insert(name.clone(), Arc::new(gt));
        }
    }
    *st.global_tables.write() = global;

    // Reclaimed but keeps the Arc identity: items are inside tables above.
    let _ = std::mem::take(&mut HashMap::<String, Arc<Item>>::new());
}

impl crate::handler::DynamoDbHandler {
    /// Serialize the full handler state (all accounts/regions) to JSON.
    pub fn snapshot_json(&self) -> Value {
        self.state
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
        let existing: Vec<(u64, String)> = self.state.read().keys().cloned().collect();
        for (account, region) in existing {
            self.get_state(account, &region);
        }
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

    /// Clear all DynamoDB state (every account/region).
    pub fn reset(&self) {
        let empty = serde_json::Value::Object(serde_json::Map::new());
        let mut states = self.state.write();
        for st in states.values_mut() {
            store_restore_json(st, &empty);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::DynamoDbHandler;
    use crate::models::{AttributeDefinition, KeySchema, Table};

    #[test]
    fn test_snapshot_round_trip_restores_table_and_items() {
        let handler = DynamoDbHandler::new();
        let mut states = handler.state.write();
        let st = states
            .entry((424242424242, "eu-roundtrip-1".to_string()))
            .or_default()
            .clone();
        drop(states);

        let table = Table::new(
            "rt-table".to_string(),
            424242424242,
            "eu-roundtrip-1".to_string(),
            vec![KeySchema {
                attribute_name: "pk".to_string(),
                key_type: "HASH".to_string(),
            }],
            vec![AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: "S".to_string(),
            }],
            "PAY_PER_REQUEST".to_string(),
        );
        st.put_table(Arc::new(table));
        let t = st.get_table("rt-table").unwrap();
        t.items.write().insert(
            "pk=k1".to_string(),
            Arc::new(Item {
                attributes: HashMap::from([("pk".to_string(), serde_json::json!({"S": "k1"}))]),
            }),
        );

        let snap = handler.snapshot_json();
        assert!(snap.to_string().contains("k1"));

        // Mutate after the snapshot: drop the table.
        st.delete_table("rt-table");

        handler.restore_json(&snap);
        let t = st.get_table("rt-table").expect("table restored");
        assert!(t.items.read().contains_key("pk=k1"));
        let attr = t
            .attribute_definitions
            .read()
            .first()
            .map(|a| a.attribute_type.clone());
        assert_eq!(attr, Some("S".to_string()));

        // Cleanup: do not leak state into other tests in this process.
        st.delete_table("rt-table");
    }
}
