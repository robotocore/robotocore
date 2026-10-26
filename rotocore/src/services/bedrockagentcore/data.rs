//! Bedrock AgentCore data plane — `bedrock-agentcore` handlers.
//!
//! Wired behind the union dispatcher in `super`; handlers land with issue
//! #371 (memory events with moto 5.2.4's messages, code-interpreter
//! sessions, derived list ops). Every data-plane operation answers the
//! generic modeled 501 until then.

#![allow(dead_code)] // stub: handlers land in the follow-up commits

use super::*;

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

pub(crate) fn route(req: &Request, op: &str, _st: &mut AgentCoreStore) -> Response {
    let _ = (parse_body(req), req.account, req.region.clone());
    not_implemented(op)
}
