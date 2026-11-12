//! Cross-service state bridge (Secrets Manager → SSM).
//!
//! Lets the SSM crate resolve the `/aws/reference/secretsmanager/<name>`
//! parameter-reference family through the owning Secrets Manager store
//! (moto ssm/models.py `_get_secretsmanager_parameter`: the whole
//! Parameter list for the secret rides back — one entry per version).
//! States register here as they are touched, mirroring the `s3::bridge`
//! pattern. Secrets Manager is regionless at pick time per-account.

use crate::models::SmState;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::OnceLock;

fn registered() -> &'static RwLock<HashMap<(u64, String), SmState>> {
    static REGISTRY: OnceLock<RwLock<HashMap<(u64, String), SmState>>> = OnceLock::new();
    REGISTRY.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Register the live Secrets Manager state for `(account, region)`.
pub fn register_state(account: u64, region: &str, state: &SmState) {
    registered()
        .write()
        .insert((account, region.to_string()), state.clone());
}

/// The AWSCURRENT value of `secret_name` in the registered state —
/// `Some((name, value))` when the secret carries an AWSCURRENT string
/// version. `None` when no state is registered or the name misses.
pub fn current_string_value(
    account: u64,
    region: &str,
    secret_name: &str,
) -> Option<(String, String)> {
    current_string_entry(account, region, secret_name).map(|(n, v, _, _, _, _, _)| (n, v))
}

/// The richer row the SSM reference arm renders: the AWSCURRENT
/// `(secret name, value, arn, created epoch, version id)`. `None` when
/// no state is registered or the name misses.
pub fn current_string_entry(
    account: u64,
    region: &str,
    secret_name: &str,
) -> Option<(
    String,
    String,
    String,
    i64,
    Option<String>,
    Option<String>,
    String,
)> {
    let state = registered()
        .read()
        .get(&(account, region.to_string()))
        .cloned()?;
    let secret = state.secrets.read().get(secret_name)?.clone();
    if *secret.removed.read() {
        return None;
    }
    let description = secret.description.read().clone();
    let kms_key_id = secret.kms_key_id.read().clone();
    let versions = secret.versions.read();
    let version = versions
        .values()
        .find(|v| v.stages.iter().any(|s| s == "AWSCURRENT"))?;
    Some((
        secret.name.clone(),
        version.secret_string.clone()?,
        secret.arn.clone(),
        version.created_date as i64,
        description,
        kms_key_id,
        version.version_id.clone(),
    ))
}
