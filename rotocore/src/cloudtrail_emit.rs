//! Cross-service CloudTrail management-event emit.
//!
//! Services that carry CloudTrail management events (elbv2 target
//! register/deregister, autoscaling group create/delete, ec2
//! RunInstances/TerminateInstances) publish the standard
//! "AWS API Call via CloudTrail" envelope onto the default EventBridge bus
//! when their mutating handlers succeed (issue #347: LaunchDarkly's
//! streamer-verifier drain flow). Dependency direction: rotocore must not
//! depend on the events crate (crate deps only ever flow the other way), so
//! the gateway binds the events crate's publisher here at startup — first
//! registration wins, the same rule as the events hook registry
//! (`src/core/cross_service.rs`) and the cloudwatch handler registry
//! (`crates/cloudwatch/src/bridge.rs`).
//!
//! The envelope itself lives once in
//! `crates/events/src/bridge.rs::emit_cloudtrail_api_call`; this module is
//! only the indirection that lets service handlers fire it. Without a
//! registered emitter the push is a no-op (unit tests, throwaway handler
//! instances), and the emit never fails the API call that triggered it.

use parking_lot::RwLock;
use std::sync::{Arc, OnceLock};

/// Emit signature: `(account, region, event_source, event_name,
/// request_parameters_json)` — mirrors
/// `events::bridge::emit_cloudtrail_api_call`.
pub type CloudtrailEmitFn = Arc<dyn Fn(u64, &str, &str, &str, &str) + Send + Sync>;

fn registered() -> &'static RwLock<Option<CloudtrailEmitFn>> {
    static REGISTRY: OnceLock<RwLock<Option<CloudtrailEmitFn>>> = OnceLock::new();
    REGISTRY.get_or_init(|| RwLock::new(None))
}

/// Register the process-global emitter. The first registration binds: the
/// gateway wires the serving emitter at startup, and later throwaway
/// instances must not redirect the ingest (same rule as the events-hook
/// registry).
pub fn register_emitter(emit: CloudtrailEmitFn) {
    let mut reg = registered().write();
    if reg.is_none() {
        *reg = Some(emit);
    }
}

/// Test helper: unbind the process-global registry so a test can install its
/// own spy. Not `#[cfg(test)]` on purpose — mirror of
/// `crates/cloudwatch/src/bridge.rs::reset_registry`.
///
/// Contract: call it as the first line of any test that claims the registry,
/// and call it again at the end to restore the unbound default. Tests must be
/// sole registrants in their binary — other tests only ever push through the
/// registered emitter (never reset or rebind), so binding order decides
/// which emitter sees those pushes, and per-(account) scoping keeps the
/// assertions independent.
pub fn reset_registry() {
    *registered().write() = None;
}

/// Test helper: whatever emitter the registry currently holds (if any), for
/// cross-crate tests that assert against the live ingest target.
#[doc(hidden)]
pub fn test_registered_emitter() -> Option<CloudtrailEmitFn> {
    registered().read().clone()
}

/// Fire-and-forget: publish the CloudTrail API-call event for one succeeded
/// mutating call. A missing/cold registry drops the push and never fails the
/// emitting service handler.
pub fn emit_cloudtrail_api_call(
    account: u64,
    region: &str,
    event_source: &str,
    event_name: &str,
    request_parameters_json: &str,
) {
    let emitter = registered().read().clone();
    if let Some(emit) = emitter {
        emit(
            account,
            region,
            event_source,
            event_name,
            request_parameters_json,
        );
    }
}
