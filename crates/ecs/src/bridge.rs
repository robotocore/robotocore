//! Cross-service state bridge.
//!
//! Lets other service crates read ECS services without owning an `EcsHandler`
//! instance. ECS states register here as they are touched (an `EcsState` holds
//! its maps behind `Arc`s, so a registered state stays live). Mirrors the
//! `s3::bridge` pattern for process-global access; consumed by the
//! applicationautoscaling backend's ECS-service existence check.

use crate::models::EcsState;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::OnceLock;

fn registered() -> &'static RwLock<HashMap<(u64, String), EcsState>> {
    static REGISTRY: OnceLock<RwLock<HashMap<(u64, String), EcsState>>> = OnceLock::new();
    REGISTRY.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Register (or re-register) a live ECS state for (account, region). Called by
/// the ECS handler whenever it resolves its per-account-region state.
pub fn register_state(account: u64, region: &str, state: &EcsState) {
    registered()
        .write()
        .insert((account, region.to_string()), state.clone());
}

/// Whether `service` exists in `cluster` for (account, region), mirroring moto's
/// `ecs_backend.describe_services(cluster, [service])` one-hit probe.
pub fn service_exists(account: u64, region: &str, cluster: &str, service: &str) -> bool {
    registered()
        .read()
        .get(&(account, region.to_string()))
        .map(|state| {
            let services = state.services.read();
            services
                .get(cluster)
                .and_then(|per_cluster| per_cluster.get(service))
                .is_some()
        })
        .unwrap_or(false)
}
