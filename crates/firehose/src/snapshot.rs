//! State snapshot/restore surface for the Firehose handler.
//!
//! `snapshot_json` serializes every (account, region) store into a JSON
//! value; `restore_json` replaces store contents from that shape. Used by
//! the server's `/_robotocore/state/*` endpoints.

use serde_json::Value;
use std::collections::HashMap;

use crate::models::FirehoseState;

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

impl FirehoseState {
    /// Serialize this store into a JSON value.
    pub fn snapshot_json(&self) -> Value {
        let map = self.resources.read();
        serde_json::to_value(&*map).unwrap_or(Value::Null)
    }

    /// Replace this store's contents from `v`.
    pub fn restore_json(&self, v: &Value) {
        let restored: HashMap<String, crate::models::DeliveryStream> =
            serde_json::from_value(v.clone()).unwrap_or_default();
        *self.resources.write() = restored;
    }
}

impl crate::handler::FirehoseHandler {
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

    /// Clear all Firehose state (every account/region).
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

    use crate::models::DeliveryStream;

    #[test]
    fn test_snapshot_round_trip_restores_stream() {
        let handler = crate::handler::FirehoseHandler::new();
        let st = handler.get_state(424242424242, "eu-roundtrip-1");
        st.put(DeliveryStream {
            name: "rt-stream".to_string(),
            arn: "arn:aws:firehose:eu-roundtrip-1:424242424242:deliverystream/rt-stream"
                .to_string(),
            status: "ACTIVE".to_string(),
            stream_type: "DirectPut".to_string(),
            created: 1.0,
            version_id: 1,
            tags: Default::default(),
            s3_config: Default::default(),
            encryption: None,
        });

        let snap = handler.snapshot_json();
        assert!(snap.to_string().contains("rt-stream"));

        // Mutate after the snapshot: remove the stream.
        st.remove("rt-stream");

        handler.restore_json(&snap);
        assert!(st.get("rt-stream").is_some());

        // Cleanup: do not leak state into other tests in this process.
        st.remove("rt-stream");
    }
}
