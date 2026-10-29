//! State snapshot/restore surface for the SQS handler.
//!
//! `snapshot_json` serializes every (account, region) store into a JSON
//! value; `restore_json` replaces store contents from that shape. Used by
//! the server's `/_robotocore/state/*` endpoints.

use parking_lot::RwLock;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

use crate::handler::DefaultSqsHandler;
use crate::models::{MoveTask, Queue, SqsStore};

/// Serde support for the queue tags field (`RwLock<HashMap<String, String>>`).
pub(crate) mod tags_lock {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    type Map = std::collections::HashMap<String, String>;

    pub fn serialize<S: Serializer>(v: &parking_lot::RwLock<Map>, s: S) -> Result<S::Ok, S::Error> {
        Map::serialize(&*v.read(), s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<parking_lot::RwLock<Map>, D::Error> {
        Ok(parking_lot::RwLock::new(Map::deserialize(d)?))
    }
}

impl SqsStore {
    /// Serialize queues and move tasks to JSON.
    pub fn snapshot_json(&self) -> Value {
        let queues = self.queues.read();
        let mut q_out = serde_json::Map::new();
        for (name, q) in queues.iter() {
            let queue = q.read();
            q_out.insert(
                name.clone(),
                serde_json::to_value(&*queue).unwrap_or(Value::Null),
            );
        }
        let move_tasks = self.move_tasks.read();
        let mut t_out = serde_json::Map::new();
        for (handle, t) in move_tasks.iter() {
            t_out.insert(
                handle.clone(),
                serde_json::to_value(t).unwrap_or(Value::Null),
            );
        }
        json!({
            "queues": q_out,
            "move_tasks": t_out,
        })
    }

    /// Replace queues and move tasks with the state in `v`.
    ///
    /// Queues present in both the live map and the snapshot are restored in
    /// place (`*existing.write() = queue`), so `Arc` handles handed out
    /// before the restore — held by in-flight requests such as a long-poll
    /// receive — stay attached to the queue object the store maps
    /// afterwards. Queues the snapshot does not mention are dropped.
    pub fn restore_json(&self, v: &Value) {
        let empty = serde_json::Map::new();
        let obj = v.as_object().unwrap_or(&empty);

        let mut restored: HashMap<String, Queue> = HashMap::new();
        if let Some(queues_obj) = obj.get("queues").and_then(Value::as_object) {
            for (name, q) in queues_obj {
                if let Ok(queue) = serde_json::from_value::<Queue>(q.clone()) {
                    restored.insert(name.clone(), queue);
                }
            }
        }

        let mut tasks: HashMap<String, MoveTask> = HashMap::new();
        if let Some(tasks_obj) = obj.get("move_tasks").and_then(Value::as_object) {
            for (handle, t) in tasks_obj {
                if let Ok(task) = serde_json::from_value(t.clone()) {
                    tasks.insert(handle.clone(), task);
                }
            }
        }

        let mut queues = self.queues.write();
        queues.retain(|name, _| restored.contains_key(name));
        for (name, queue) in restored {
            match queues.get(&name) {
                Some(existing) => *existing.write() = queue,
                None => {
                    queues.insert(name, Arc::new(RwLock::new(queue)));
                }
            }
        }
        *self.move_tasks.write() = tasks;
    }
}

impl DefaultSqsHandler {
    /// Serialize the full handler state (all accounts/regions) to JSON.
    pub fn snapshot_json(&self) -> Value {
        self.stores
            .read()
            .iter()
            .map(|((account, region), store)| {
                (format!("{account}|{region}"), store.snapshot_json())
            })
            .collect::<serde_json::Map<String, Value>>()
            .into()
    }

    /// Replace handler state from a JSON value produced by `snapshot_json`.
    ///
    /// Replace semantics: stores the snapshot does not mention are emptied;
    /// stores it does mention are restored in place by
    /// [`SqsStore::restore_json`], which drops their absent queues and keeps
    /// live queue handles attached.
    pub fn restore_json(&self, v: &Value) {
        let empty = Value::Object(serde_json::Map::new());
        let snapshot = v.as_object();
        let existing: Vec<(u64, String)> = self.stores.read().keys().cloned().collect();
        for (account, region) in existing {
            if snapshot.is_some_and(|obj| obj.contains_key(&format!("{account}|{region}"))) {
                continue;
            }
            self.get_store(account, &region).restore_json(&empty);
        }

        let Some(obj) = snapshot else {
            return;
        };
        for (key, store_state) in obj {
            let Some((account, region)) = key.split_once('|') else {
                continue;
            };
            let Ok(account) = account.parse::<u64>() else {
                continue;
            };
            self.get_store(account, region).restore_json(store_state);
        }
    }

    /// Clear all SQS state (queues and move tasks, every account/region).
    pub fn reset(&self) {
        self.stores.write().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::SqsMessage;

    /// Queue fixture on a dedicated (account, region) partition.
    fn queue(name: &str, account: u64, region: &str) -> Queue {
        Queue::new(
            name.to_string(),
            region.to_string(),
            account.to_string(),
            format!("https://sqs.{region}.localhost:4566/{account}/{name}"),
            format!("arn:aws:sqs:{region}:{account}:{name}"),
        )
    }

    fn msg(id: &str, body: &str) -> SqsMessage {
        SqsMessage {
            message_id: id.to_string(),
            body: body.to_string(),
            md5_of_body: "abc".to_string(),
            receipt_handle: id.to_string(),
            sent_timestamp: 1,
            visibility_until: None,
            receive_count: 0,
            first_receive_timestamp: None,
            attributes: HashMap::new(),
            message_attributes: HashMap::new(),
            available_at: None,
            deduplication_id: None,
        }
    }

    #[test]
    fn test_snapshot_round_trip_preserves_queues_and_messages() {
        let handler = DefaultSqsHandler::new();
        // Dedicated (account, region): the store map is process-global and
        // other tests run in parallel against their own partitions.
        let store = handler.get_store(424242424242, "eu-roundtrip-1");
        store.create_queue(Queue::new(
            "rt-test".to_string(),
            "us-east-1".to_string(),
            "424242424242".to_string(),
            "https://sqs.us-east-1.localhost:4566/424242424242/rt-test".to_string(),
            "arn:aws:sqs:us-east-1:424242424242:rt-test".to_string(),
        ));
        store
            .get_queue("rt-test")
            .unwrap()
            .write()
            .send_message(SqsMessage {
                message_id: "m-1".to_string(),
                body: "snapshot-me".to_string(),
                md5_of_body: "abc".to_string(),
                receipt_handle: "r-1".to_string(),
                sent_timestamp: 1,
                visibility_until: None,
                receive_count: 0,
                first_receive_timestamp: None,
                attributes: HashMap::new(),
                message_attributes: HashMap::new(),
                available_at: None,
                deduplication_id: None,
            });

        let snap = store.snapshot_json();
        assert!(snap.to_string().contains("snapshot-me"));

        // Mutate after the snapshot: a second message that must not survive
        // the restore (replace semantics).
        store
            .get_queue("rt-test")
            .unwrap()
            .write()
            .send_message(SqsMessage {
                message_id: "m-2".to_string(),
                body: "post-snapshot".to_string(),
                md5_of_body: "def".to_string(),
                receipt_handle: "r-2".to_string(),
                sent_timestamp: 2,
                visibility_until: None,
                receive_count: 0,
                first_receive_timestamp: None,
                attributes: HashMap::new(),
                message_attributes: HashMap::new(),
                available_at: None,
                deduplication_id: None,
            });
        store.delete_queue("rt-test");

        store.restore_json(&snap);
        let q = store.get_queue("rt-test").expect("queue restored");
        let q = q.read();
        assert_eq!(q.messages.len(), 1, "restore must replace, not merge");
        assert_eq!(q.messages[0].body, "snapshot-me");
        assert_eq!(q.attributes.visibility_timeout, 30);

        // Cleanup: do not leak state into other tests in this process.
        store.delete_queue("rt-test");
    }

    /// A queue handle taken before a restore (as an in-flight request holds
    /// one, e.g. a long-poll receive) must stay attached to the queue object
    /// the store maps afterwards, and must observe the restored content.
    #[test]
    fn test_restore_preserves_live_queue_handles() {
        let handler = DefaultSqsHandler::new();
        // Dedicated (account, region): the store map is process-global and
        // other tests run in parallel against their own partitions.
        let store = handler.get_store(424242424243, "eu-livehandle-1");
        store.create_queue(queue("live-handle", 424242424243, "us-east-1"));

        // The pre-restore handle, as a mid-request caller would hold it.
        let live = store.get_queue("live-handle").expect("queue exists");

        let snap = store.snapshot_json();

        // Mutate after the snapshot: the restore must revert this even
        // though the queue is restored in place (replace semantics).
        live.write().send_message(msg("m-post", "post-snapshot"));

        store.restore_json(&snap);

        // Same allocation, not a detached leftover: sends through `live`
        // land on what the store maps now.
        let observed = store.get_queue("live-handle").expect("queue restored");
        assert!(
            Arc::ptr_eq(&live, &observed),
            "restore must not detach live queue handles"
        );
        assert!(
            observed.read().messages.is_empty(),
            "restore must replace, not merge"
        );

        // A send through the pre-restore handle lands on the mapped queue.
        live.write().send_message(msg("m-live", "via-old-handle"));
        let seen = store.get_queue("live-handle").expect("queue still mapped");
        let seen = seen.read();
        assert_eq!(seen.messages.len(), 1);
        assert_eq!(seen.messages[0].body, "via-old-handle");

        // Cleanup: do not leak state into other tests in this process.
        store.delete_queue("live-handle");
    }

    /// Queues the snapshot does not mention are dropped, and deletions are
    /// reverted (replace semantics); in-place restore only covers overlap.
    #[test]
    fn test_restore_drops_absent_queues_and_reverts_deletions() {
        let handler = DefaultSqsHandler::new();
        let store = handler.get_store(424242424246, "eu-livehandle-3");
        store.create_queue(queue("kept", 424242424246, "us-east-1"));
        store.create_queue(queue("second", 424242424246, "us-east-1"));
        let kept = store.get_queue("kept").expect("queue exists");

        let snap = store.snapshot_json();

        // Live-only divergence after the snapshot: "ephemeral" was created
        // after it (absent from the snapshot), "second" was deleted after it
        // (that deletion must be reverted).
        store.create_queue(queue("ephemeral", 424242424246, "us-east-1"));
        store.delete_queue("second");

        store.restore_json(&snap);

        assert!(
            store.get_queue("ephemeral").is_none(),
            "post-snapshot queue dropped (replace semantics)"
        );
        assert!(store.get_queue("second").is_some(), "deletion reverted");
        assert!(
            Arc::ptr_eq(&kept, &store.get_queue("kept").unwrap()),
            "overlapping queue restored in place"
        );

        // The held handle for the overlapping queue still observes sends.
        kept.write().send_message(msg("m-kept", "still-here"));
        let seen = store.get_queue("kept").expect("queue still mapped");
        assert_eq!(seen.read().messages[0].body, "still-here");

        // Cleanup: do not leak state into other tests in this process.
        store.delete_queue("kept");
        store.delete_queue("second");
    }
}
