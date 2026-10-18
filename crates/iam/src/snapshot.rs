//! State snapshot/restore surface for the IAM handler.
//!
//! `snapshot_json` serializes every account store into a JSON value;
//! `restore_json` replaces store contents from that shape. Used by the
//! server's `/_robotocore/state/*` endpoints.

use serde_json::Value;

use crate::models::IamState;

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

/// `Arc<RwLock<T>>` as plain `T`.
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
fn store_restore_json(st: &IamState, v: &Value) {
    let restored: IamState = serde_json::from_value(v.clone()).unwrap_or_default();
    macro_rules! swap {
        ($field:ident) => {
            *st.$field.write() = restored.$field.read().clone();
        };
    }
    swap!(users);
    swap!(roles);
    swap!(groups);
    swap!(policies);
    swap!(account_alias);
    swap!(password_policy);
    swap!(instance_profiles);
    swap!(instance_profile_tags);
    swap!(instance_profile_ids);
    swap!(virtual_mfas);
    swap!(saml_providers);
    swap!(oidc_providers);
    swap!(ssh_keys);
    swap!(server_certs);
    swap!(login_profiles);
    swap!(ssc_creds);
}

impl crate::handler::IamHandler {
    /// Serialize the full handler state (all accounts) to JSON.
    pub fn snapshot_json(&self) -> Value {
        self.state
            .read()
            .iter()
            .map(|(account, st)| {
                (
                    account.to_string(),
                    serde_json::to_value(st).unwrap_or(Value::Null),
                )
            })
            .collect::<serde_json::Map<String, Value>>()
            .into()
    }

    /// Replace handler state from a JSON value produced by `snapshot_json`.
    ///
    /// Replace semantics: every existing account is emptied first, then the
    /// snapshot's entries are applied.
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
            let Ok(account) = key.parse::<u64>() else {
                continue;
            };
            let st = self.get_state(account);
            store_restore_json(&st, store_state);
        }
    }

    /// Clear all IAM state (every account).
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

    use crate::handler::IamHandler;
    use crate::models::{Role, User};

    #[test]
    fn test_snapshot_round_trip_restores_user_and_role() {
        let handler = IamHandler::new();
        let st = handler.get_state(424242424242);
        st.roles.write().insert(
            "rt-role".to_string(),
            std::sync::Arc::new(Role::new(
                424242424242,
                "rt-role".to_string(),
                "{}".to_string(),
            )),
        );
        st.users.write().insert(
            "rt-user".to_string(),
            std::sync::Arc::new(User::new(424242424242, "rt-user".to_string())),
        );

        let snap = handler.snapshot_json();
        assert!(snap.to_string().contains("rt-role"));

        // Mutate after the snapshot: remove both.
        st.roles.write().clear();
        st.users.write().clear();

        handler.restore_json(&snap);
        assert!(st.get_role("rt-role").is_some());
        assert!(st.get_user("rt-user").is_some());

        // Cleanup: do not leak state into other tests in this process.
        st.roles.write().clear();
        st.users.write().clear();
    }
}
