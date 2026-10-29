//! Process-global state registry shared by every rotocore backend.
//!
//! Backends keep their own state (mirroring moto's per-service backends);
//! this registry only hands each backend a typed slot so multiple instances
//! of a backend (server adapter, tests) see the same state.

use once_cell::sync::Lazy;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Key for a partition of state: (account, region).
pub type ScopeKey = (u64, String);

/// Opaque, service-owned state partition.
pub type Slot<T> = Arc<RwLock<HashMap<ScopeKey, T>>>;

fn slots() -> &'static RwLock<HashMap<&'static str, Box<dyn std::any::Any + Send + Sync>>> {
    static SLOTS: Lazy<RwLock<HashMap<&'static str, Box<dyn std::any::Any + Send + Sync>>>> =
        Lazy::new(|| RwLock::new(HashMap::new()));
    &SLOTS
}

fn downcast_slot<T: Send + Sync + 'static>(
    box_any: &mut Box<dyn std::any::Any + Send + Sync>,
) -> Slot<T> {
    match box_any.downcast_ref::<Slot<T>>() {
        Some(s) => Arc::clone(s),
        None => panic!("rotocore slot type collision; all callers must pass the same T"),
    }
}

/// Get (creating on first use) the state slot for `service`.
pub fn slot<T: Default + Send + Sync + 'static>(service: &'static str) -> Slot<T> {
    let mut map = slots().write();
    let entry = map.entry(service).or_insert_with(|| {
        Box::new(Arc::new(RwLock::new(HashMap::<ScopeKey, T>::new())) as Slot<T>)
    });
    downcast_slot(entry)
}
