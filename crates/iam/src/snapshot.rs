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
/// Returns `Err(what)` only for a NON-EMPTY slice that failed to parse (the
/// swap-pour then empties the store — silent before w7-store N3 made it
/// visible). The empty-`{}` restore (reset / replace-then-empty) is the
/// designed parse-failure → empty path and stays invisible.
fn store_restore_json(st: &IamState, v: &Value) -> Result<(), String> {
    let is_empty_slice = v.as_object().is_some_and(|m| m.is_empty());
    let restored: IamState = match serde_json::from_value(v.clone()) {
        Ok(parsed) => parsed,
        Err(e) => {
            if is_empty_slice {
                // The intentional clear: the slice carries no data.
                IamState::default()
            } else {
                return Err(e.to_string());
            }
        }
    };
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
    swap!(instance_profile_paths);
    swap!(instance_profile_created);
    swap!(virtual_mfas);
    swap!(saml_providers);
    swap!(oidc_providers);
    swap!(ssh_keys);
    swap!(server_certs);
    swap!(login_profiles);
    swap!(ssc_creds);
    Ok(())
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
                let _ = store_restore_json(st, &empty);
            }
        }
        let Some(obj) = v.as_object() else {
            return;
        };
        let mut parse_errors: Vec<String> = Vec::new();
        for (key, store_state) in obj {
            let Ok(account) = key.parse::<u64>() else {
                continue;
            };
            let st = self.get_state(account);
            if let Err(what) = store_restore_json(&st, store_state) {
                // The empty-default semantics stay (forward compat for
                // missing fields), but a struct-wide parse failure is no
                // longer silent: the swap-pour just EMPTIED this account
                // and the caller reported success — surface what happened
                // (w7-store N3: restores must never fail invisibly).
                parse_errors.push(format!("account {account}: {what}"));
            }
        }
        if !parse_errors.is_empty() {
            eprintln!(
                "[iam] snapshot restore INCOMPLETE (parse failures; affected accounts emptied): {}",
                parse_errors.join("; ")
            );
            return;
        }
        // Re-seed the group creation counter beyond the highest restored
        // seq (process-global counter restarts at 1; without the sweep a
        // post-restore create collides and inverts creation order —
        // w7-store N1).
        let max_seq = self
            .state
            .read()
            .values()
            .map(|st| {
                st.groups
                    .read()
                    .values()
                    .map(|g| g.created_seq)
                    .filter(|seq| *seq != 0)
                    .max()
                    .unwrap_or(0)
            })
            .max()
            .unwrap_or(0);
        crate::models::reseed_group_seq(max_seq);
    }

    /// Clear all IAM state (every account).
    pub fn reset(&self) {
        let empty = serde_json::Value::Object(serde_json::Map::new());
        let mut states = self.state.write();
        for st in states.values_mut() {
            let _ = store_restore_json(st, &empty);
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

    /// Forward-compat: a snapshot saved by a binary that predates
    /// `instance_profile_created` (no `#[serde(default)]` then) must NOT
    /// wipe live state on load (w6-crate F1 live-proved the empty-restore
    /// data loss). Everything else restores; the CreateDate falls back to
    /// stamp-on-first-read via the render-time fallback.
    #[test]
    fn test_snapshot_restore_tolerates_missing_instance_profile_created() {
        use bytes::Bytes;
        fn req(operation: &str, params: serde_json::Value) -> crate::protocol::AwsRequest {
            crate::protocol::AwsRequest {
                service: "iam".to_string(),
                operation: operation.to_string(),
                account: 123456789012,
                region: "us-east-1".to_string(),
                params,
                body: Bytes::new(),
                query: String::new(),
            }
        }
        let handler = IamHandler::new();
        handler.handle(req(
            "CreateUser",
            serde_json::json!({ "UserName": "fc-user" }),
        ));
        handler.handle(req(
            "CreateRole",
            serde_json::json!({ "RoleName": "fc-role", "AssumeRolePolicyDocument": "{}" }),
        ));
        handler.handle(req(
            "CreateInstanceProfile",
            serde_json::json!({ "InstanceProfileName": "fc-ip" }),
        ));

        let snap = handler.snapshot_json();
        let mut stripped = snap.clone();
        if let Some(state) = stripped
            .as_object_mut()
            .and_then(|o| o.get_mut("123456789012"))
            .and_then(serde_json::Value::as_object_mut)
        {
            state.remove("instance_profile_created");
        }
        assert!(
            stripped.to_string().contains("fc-user"),
            "the strip must leave the rest of the snapshot intact"
        );

        // Nuke live state first, so only the snapshot can repopulate it.
        handler.reset();
        assert!(handler
            .get_state(123456789012)
            .get_user("fc-user")
            .is_none());
        handler.restore_json(&stripped);

        let st = handler.get_state(123456789012);
        assert!(st.get_user("fc-user").is_some(), "user restored");
        assert!(st.get_role("fc-role").is_some(), "role restored");
        assert!(
            st.instance_profiles.read().contains_key("fc-ip"),
            "instance profile restored"
        );
        // CreateDate falls back to the render-time stamp-and-persist.
        let resp = handler.handle(req(
            "GetInstanceProfile",
            serde_json::json!({ "InstanceProfileName": "fc-ip" }),
        ));
        assert_eq!(resp.status, 200, "body: {}", resp.body);
        assert!(resp.body.contains("<CreateDate>"), "body: {}", resp.body);
        // Cleanup: do not leak state into other tests in this process.
        handler.reset();
    }

    /// A loaded snapshot must leave the group sequencing coherent:
    /// `restore_json` re-seeds NEXT_GROUP_SEQ beyond the restored max, so a
    /// post-restore create keeps sorting AFTER the restored groups even
    /// when its name would sort first (w7-store N1: the leftover counter
    /// collided and inverted creation order after restart+load).
    #[test]
    fn test_snapshot_restore_reseeds_group_seq() {
        use bytes::Bytes;
        let handler = IamHandler::new();
        let acct = 961000000001;
        let make = |operation: &str, params: serde_json::Value| crate::protocol::AwsRequest {
            service: "iam".to_string(),
            operation: operation.to_string(),
            account: acct,
            region: "us-east-1".to_string(),
            params,
            body: Bytes::new(),
            query: String::new(),
        };
        handler.handle(make(
            "CreateUser",
            serde_json::json!({ "UserName": "seq-user" }),
        ));
        handler.handle(make(
            "CreateGroup",
            serde_json::json!({ "GroupName": "mc1" }),
        ));
        handler.handle(make(
            "CreateGroup",
            serde_json::json!({ "GroupName": "mc2" }),
        ));
        for group in ["mc1", "mc2"] {
            handler.handle(make(
                "AddUserToGroup",
                serde_json::json!({ "GroupName": group, "UserName": "seq-user" }),
            ));
        }
        let snap = handler.snapshot_json();
        // Restart simulation: nothing stops the counter from having moved —
        // reset + restore replays the saved seq values, and the sweep must
        // push the counter past them.
        handler.reset();
        handler.restore_json(&snap);

        handler.handle(make(
            "CreateGroup",
            serde_json::json!({ "GroupName": "aa-fresh-last" }),
        ));
        handler.handle(make(
            "AddUserToGroup",
            serde_json::json!({ "GroupName": "aa-fresh-last", "UserName": "seq-user" }),
        ));
        let resp = handler.handle(make(
            "ListGroupsForUser",
            serde_json::json!({ "UserName": "seq-user" }),
        ));
        assert_eq!(resp.status, 200, "body: {}", resp.body);
        let names: Vec<String> = resp
            .body
            .split("<GroupName>")
            .skip(1)
            .map(|t| {
                t.split("</GroupName>")
                    .next()
                    .unwrap_or_default()
                    .to_string()
            })
            .collect();
        assert_eq!(
            names,
            vec!["mc1", "mc2", "aa-fresh-last"],
            "creation order survives restore even though 'aa…' sorts first: {names:?}"
        );
        handler.reset();
    }

    /// Legacy snapshots (pre-`created_seq`) restore with the field's
    /// serde default (seq 0) — the groups come back intact and name-ordered
    /// among their own ties (w7-store N5: the created_seq strip twin of the
    /// instance_profile_created test).
    #[test]
    fn test_snapshot_restore_tolerates_missing_group_created_seq() {
        use bytes::Bytes;
        let handler = IamHandler::new();
        let acct = 961000000002;
        let make = |operation: &str, params: serde_json::Value| crate::protocol::AwsRequest {
            service: "iam".to_string(),
            operation: operation.to_string(),
            account: acct,
            region: "us-east-1".to_string(),
            params,
            body: Bytes::new(),
            query: String::new(),
        };
        handler.handle(make(
            "CreateUser",
            serde_json::json!({ "UserName": "seq-user2" }),
        ));
        handler.handle(make(
            "CreateGroup",
            serde_json::json!({ "GroupName": "mc1" }),
        ));
        handler.handle(make(
            "CreateGroup",
            serde_json::json!({ "GroupName": "mc2" }),
        ));
        for group in ["mc1", "mc2"] {
            handler.handle(make(
                "AddUserToGroup",
                serde_json::json!({ "GroupName": group, "UserName": "seq-user2" }),
            ));
        }
        let snap = handler.snapshot_json();
        let mut stripped = snap.clone();
        let state = stripped
            .as_object_mut()
            .and_then(|o| o.get_mut(acct.to_string().as_str()))
            .and_then(serde_json::Value::as_object_mut)
            .expect("snapshot carries the account");
        let groups = state
            .get_mut("groups")
            .and_then(serde_json::Value::as_object_mut)
            .expect("snapshot carries groups");
        for (_name, group) in groups.iter_mut() {
            group
                .as_object_mut()
                .expect("group object")
                .remove("created_seq");
        }
        handler.reset();
        handler.restore_json(&stripped);
        let st = handler.get_state(acct);
        assert!(
            st.groups.read().contains_key("mc1") && st.groups.read().contains_key("mc2"),
            "both groups restored without created_seq"
        );
        handler.reset();
    }
}
