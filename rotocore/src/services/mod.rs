//! Service backends. Each module owns one AWS service and a `dispatch`.
//!
//! Only PORTED services appear here — a module replaces the moto sidecar
//! for its service the moment it publishes real behavior. Until then the
//! gateway falls back to the sidecar, so the wire contract stays intact.

use crate::{Request, Response};

pub mod acm;
pub mod cloudformation;
pub mod comprehend;
pub mod ec2;
pub mod polly;
pub mod sagemaker;
pub mod support;
pub mod transcribe;

/// Dispatch a routed request to its service backend, or `None` when the
/// service has no ported backend yet (the gateway then uses the fallback).
pub fn dispatch(req: &Request) -> Option<Response> {
    match req.service.as_str() {
        "comprehend" => Some(comprehend::dispatch(req)),
        "polly" => Some(polly::dispatch(req)),
        "sagemaker" => Some(sagemaker::dispatch(req)),
        "support" => Some(support::dispatch(req)),
        "transcribe" => Some(transcribe::dispatch(req)),
        _ => None,
    }
}
