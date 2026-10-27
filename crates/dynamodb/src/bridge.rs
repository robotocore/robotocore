//! Cross-service state bridge (moto parity: dynamodbstreams reaches into
//! the dynamodb backend, same as moto's `DynamoDBStreamsBackend.dynamodb`).
//!
//! Lets the `dynamodbstreams` rotocore backend read the live table state —
//! stream labels, shards and captured mutation records — from the DynamoDB
//! handler instances registered here. States register as they are touched
//! because `DynamoDbState` holds its maps behind `Arc`s, so a registered
//! state stays live for later mutations. Mirrors the `s3::bridge` pattern.

use crate::models::{DynamoDbState, Table};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::OnceLock;

fn registered() -> &'static RwLock<HashMap<(u64, String), DynamoDbState>> {
    static REGISTRY: OnceLock<RwLock<HashMap<(u64, String), DynamoDbState>>> = OnceLock::new();
    REGISTRY.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Register (or re-register) a live DynamoDB state for (account, region).
/// Called by the DynamoDB handler whenever it resolves its per-account/region
/// state.
pub fn register_state(account: u64, region: &str, state: &DynamoDbState) {
    registered()
        .write()
        .insert((account, region.to_string()), state.clone());
}

/// Live handle of the registered state for (account, region).
pub fn get_state(account: u64, region: &str) -> Option<DynamoDbState> {
    registered()
        .read()
        .get(&(account, region.to_string()))
        .cloned()
}

/// Live handle of table `name` in the registered state for (account, region).
pub fn get_table(account: u64, region: &str, name: &str) -> Option<std::sync::Arc<Table>> {
    get_state(account, region).and_then(|state| state.get_table(name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{AttributeDefinition, KeySchema, Table};
    use std::sync::Arc;

    #[test]
    fn test_registered_state_stays_live_for_mutations() {
        let state = DynamoDbState::new();
        register_state(424242424242, "bridge-test-1", &state);

        let table = Arc::new(Table::new(
            "bridge-table".to_string(),
            424242424242,
            "bridge-test-1".to_string(),
            vec![KeySchema {
                attribute_name: "pk".to_string(),
                key_type: "HASH".to_string(),
            }],
            vec![AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: "S".to_string(),
            }],
            "PAY_PER_REQUEST".to_string(),
        ));
        state.put_table(table);

        let resolved = get_table(424242424242, "bridge-test-1", "bridge-table");
        assert!(
            resolved.is_some(),
            "registration must make the table visible"
        );
        assert!(
            get_table(1, "bridge-test-1", "bridge-table").is_none(),
            "accounts are isolated"
        );
    }
}
