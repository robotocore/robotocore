//! Bedrock AgentCore control plane — `bedrock-agentcore-control` handlers.
//!
//! Wired behind the union dispatcher in `super`; handlers land with issue
//! #371 (the code-interpreter / runtime / gateway / memory CRUD mirrors
//! moto 5.2.4's pinned `bedrockagentcorecontrol` backend).

#![allow(dead_code)] // stub: handlers land in the follow-up commits

use super::*;

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

/// Every control-plane operation answers the generic modeled 501 until its
/// handler lands; the tag routes keep their shaping so `/tags/{arn}` URIs
/// resolve.
pub(crate) fn route(req: &Request, op: &str, _st: &mut AgentCoreStore) -> Response {
    let _ = parse_body(req);
    not_implemented(op)
}

/// Control-plane tag surface (`/tags/{resourceArn}`, ARN-greedy) — resolved
/// before the segment tables by the dispatcher in `super`.
pub(crate) fn route_tags(req: &Request, op: &str, st: &mut AgentCoreStore) -> Response {
    let body = parse_body(req);
    let arn = req.path.trim_start_matches("/tags/").to_string();
    match op {
        "TagResource" => {
            if let Some(tags) = body.get("tags").and_then(Value::as_object) {
                let row = st.tags.entry(arn).or_default();
                for (key, value) in tags {
                    row.insert(key.clone(), value.clone());
                }
            }
            ok(json!({}))
        }
        "UntagResource" => {
            if let Some(row) = st.tags.get_mut(&arn) {
                for key in crate::proto::query_map(&req.query_string)
                    .get("tagKeys")
                    .cloned()
                    .unwrap_or_default()
                {
                    row.remove(&key);
                }
            }
            ok(json!({}))
        }
        "ListTagsForResource" => {
            let tags = st
                .tags
                .get(&arn)
                .map(|row| {
                    row.iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect::<serde_json::Map<String, Value>>()
                })
                .unwrap_or_default();
            ok(json!({ "tags": tags }))
        }
        other => not_implemented(other),
    }
}
