//! Cross-service state bridge (IAM → Lambda).
//!
//! Lets other service crates (Lambda's create-path role gate) resolve IAM
//! role existence without owning an `IamHandler` instance. IAM states
//! register here as they are touched; because `IamState` holds its maps
//! behind `Arc`s a registered state stays live — later role writes and the
//! in-place snapshot restore both stay visible. Mirrors the `s3::bridge`
//! and `lambda::bridge` pattern for process-global access. IAM is
//! regionless (role ARNs carry no region), so the registry keys on
//! account alone.

use crate::models::IamState;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::OnceLock;

fn registered() -> &'static RwLock<HashMap<u64, IamState>> {
    static REGISTRY: OnceLock<RwLock<HashMap<u64, IamState>>> = OnceLock::new();
    REGISTRY.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Register (or re-register) the live IAM state for `account`. Called by
/// the IAM handler whenever it resolves a per-account state.
pub fn register_state(account: u64, state: &IamState) {
    registered().write().insert(account, state.clone());
}

/// moto `get_role_by_arn` (iam/models.py:2193-2201): whether the IAM state
/// registered for `account` holds a role whose stored ARN equals
/// `role_arn` — an exact full-ARN match, path suffixes included — like
/// moto, which walks the backend's roles comparing `role.arn` to the
/// lambda `Role` parameter verbatim.
pub fn role_arn_exists(account: u64, role_arn: &str) -> bool {
    let Some(state) = registered().read().get(&account).cloned() else {
        return false;
    };
    state.get_role_by_arn(role_arn).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Role;
    use std::sync::Arc;

    /// Pathed roles answer by exact ARN equality (moto compares `role.arn`
    /// against the lambda Role parameter verbatim), and only for the
    /// account whose state is registered.
    #[test]
    fn test_role_arn_exact_match_includes_paths() {
        let acct = 778000000001u64;
        let role = Role::full(
            acct,
            "br-role".to_string(),
            "/svc/dev/".to_string(),
            "{}".to_string(),
            String::new(),
            3600,
            false,
        );
        let state = IamState::default();
        state
            .roles
            .write()
            .insert("br-role".to_string(), Arc::new(role));
        assert!(
            !role_arn_exists(acct, "arn:aws:iam::778000000001:role/svc/dev/br-role"),
            "not registered yet"
        );
        register_state(acct, &state);
        assert!(
            role_arn_exists(acct, "arn:aws:iam::778000000001:role/svc/dev/br-role"),
            "pathed role matched by full ARN"
        );
        assert!(
            !role_arn_exists(acct, "arn:aws:iam::778000000001:role/other-role"),
            "wrong name misses"
        );
        assert!(
            !role_arn_exists(acct + 1, "arn:aws:iam::778000000001:role/svc/dev/br-role"),
            "another account cannot see the role"
        );
    }
}
