//! Bedrock AgentCore data plane — `bedrock-agentcore` handlers.
//!
//! Mirrors moto 5.2.4's memory-events surface (`moto/bedrockagentcore`:
//! Create/Get/List/Delete Event with its ValidationException messages and
//! `{epoch-ms}#{16hex}` event ids) and implements the code-interpreter
//! session CRUD the fork lacks (modeled from botocore's shapes —
//! Start/Get/Stop/List sessions over the control plane's interpreter
//! registry, the SYSTEM id seeded lazily). Derived list ops
//! (ListActors/ListSessions) read the (memory, actor, session) event
//! keying. `InvokeCodeInterpreter` returns an event stream — it stays
//! genuinely absent with the generic 501, like moto. Everything else
//! (memory records, extraction jobs, browser sessions, runtime invocations,
//! workload tokens, evaluations) answers the modeled 501 via the default
//! arm.

use super::*;
use chrono::Utc;

pub(crate) fn route(req: &Request, op: &str, st: &mut AgentCoreStore) -> Response {
    let body = parse_body(req);
    match op {
        "StartCodeInterpreterSession" => start_ci_session(req, st, &body),
        "GetCodeInterpreterSession" => get_ci_session(req, st),
        "StopCodeInterpreterSession" => stop_ci_session(req, st, &body),
        "ListCodeInterpreterSessions" => list_ci_sessions(req, st, &body),
        // InvokeCodeInterpreter's response payload is an event stream
        // (`CodeInterpreterStreamOutput`, `eventstream: true` in botocore) —
        // a bidirectional streaming surface this twin (and moto) does not
        // serve: the generic modeled 501 is the truthful answer.
        "InvokeCodeInterpreter" => not_implemented(op),
        "CreateEvent" => create_event(req, st, &body),
        "GetEvent" => get_event(req, st),
        "ListEvents" => list_events(req, st, &body),
        "DeleteEvent" => delete_event(req, st),
        "ListActors" => list_actors(req, st, &body),
        "ListSessions" => list_sessions(req, st, &body),
        other => not_implemented(other),
    }
}

const SYSTEM_INTERPRETER: &str = "aws.codeinterpreter.v1";

fn ci_missing(id: &str) -> Response {
    not_found(&format!("Could not find Code Interpreter with ID {id}"))
}

/// True when the interpreter exists, seeding the SYSTEM id on first touch.
fn ci_exists(st: &mut AgentCoreStore, id: &str) -> bool {
    if id == SYSTEM_INTERPRETER && !st.code_interpreters.contains_key(id) {
        st.code_interpreters
            .insert(SYSTEM_INTERPRETER.into(), CodeInterpreter::system());
    }
    st.code_interpreters.contains_key(id)
}

/// The id slot of every data-plane URI root
/// (`/code-interpreters/{id}/...`, `/memories/{id}/...`, `/browsers/{id}/...`).
fn slot_id(req: &Request) -> String {
    path_segs(req.path.as_str())
        .get(1)
        .copied()
        .unwrap_or_default()
        .to_string()
}

/// Positional key parts for the memory-event URIs:
/// `/memories/{m}/actor/{a}/sessions/{s}/events/{e}`.
fn event_path_keys(req: &Request) -> (String, String, String, String) {
    let segs = path_segs(req.path.as_str());
    let pick = |i: usize| segs.get(i).copied().unwrap_or_default().to_string();
    (pick(1), pick(3), pick(5), pick(7))
}

// ---------------------------------------------------------------------------
// code interpreter sessions (exceeds-moto set, botocore-shaped)
// ---------------------------------------------------------------------------

/// PUT /code-interpreters/{codeInterpreterIdentifier}/sessions/start
fn start_ci_session(req: &Request, st: &mut AgentCoreStore, body: &Value) -> Response {
    let ci_id = slot_id(req);
    if !ci_exists(st, &ci_id) {
        return ci_missing(&ci_id);
    }
    let session = CodeInterpreterSession::new(&ci_id, body);
    let out = json!({
        "codeInterpreterIdentifier": session.ci_id,
        "sessionId": session.session_id,
        "createdAt": session.created_at,
    });
    st.code_interpreter_sessions
        .insert((session.ci_id.clone(), session.session_id.clone()), session);
    ok(out)
}

/// GET /code-interpreters/{codeInterpreterIdentifier}/sessions/get —
/// `sessionId` rides the querystring per the shape's location.
fn get_ci_session(req: &Request, st: &mut AgentCoreStore) -> Response {
    let ci_id = slot_id(req);
    let Some(session_id) =
        crate::proto::query_first(&crate::proto::query_map(&req.query_string), "sessionId")
            .map(String::from)
    else {
        return validation("sessionId is a required parameter");
    };
    match st
        .code_interpreter_sessions
        .get(&(ci_id, session_id.clone()))
    {
        Some(session) => ok(session.to_dict()),
        None => session_missing(&session_id),
    }
}

fn session_missing(session_id: &str) -> Response {
    not_found(&format!(
        "Could not find Code Interpreter Session with ID {session_id}"
    ))
}

/// PUT /code-interpreters/{codeInterpreterIdentifier}/sessions/stop
fn stop_ci_session(req: &Request, st: &mut AgentCoreStore, body: &Value) -> Response {
    let ci_id = slot_id(req);
    // botocore carries the session id in the body; the query string is
    // accepted too (the GET variant of this family uses it).
    let session_id = vstr(body, "sessionId")
        .or_else(|| {
            crate::proto::query_first(&crate::proto::query_map(&req.query_string), "sessionId")
                .map(String::from)
        })
        .unwrap_or_default();
    let Some(session) = st
        .code_interpreter_sessions
        .get_mut(&(ci_id, session_id.clone()))
    else {
        return session_missing(&session_id);
    };
    session.status = "TERMINATED".to_string();
    session.last_updated_at = now();
    let out = json!({
        "codeInterpreterIdentifier": session.ci_id,
        "sessionId": session.session_id,
        "lastUpdatedAt": session.last_updated_at,
    });
    ok(out)
}

/// POST /code-interpreters/{codeInterpreterIdentifier}/sessions/list —
/// `maxResults`/`nextToken`/`status` in the body (no querystring location);
/// botocore's documented default page is 10.
fn list_ci_sessions(req: &Request, st: &mut AgentCoreStore, body: &Value) -> Response {
    let ci_id = slot_id(req);
    if !ci_exists(st, &ci_id) {
        return ci_missing(&ci_id);
    }
    let max_results = body
        .get("maxResults")
        .and_then(Value::as_i64)
        .filter(|m| *m > 0)
        .map(|m| m as usize)
        .unwrap_or(10);
    let token_id = vstr(body, "nextToken").and_then(|t| decode_token(&t));
    let status_filter = vstr(body, "status");
    let summaries: Vec<Value> = st
        .code_interpreter_sessions
        .iter()
        .filter(|((cid, _), s)| {
            cid == &ci_id
                && status_filter
                    .as_deref()
                    .map(|f| s.status == f)
                    .unwrap_or(true)
        })
        .map(|(_, s)| s.to_summary())
        .collect();
    let (page, next_token) = id_page(summaries, "sessionId", Some(max_results), token_id);
    ok(json!({ "items": page, "nextToken": next_token }))
}

// ---------------------------------------------------------------------------
// memory events (moto parity, `moto/bedrockagentcore`)
// ---------------------------------------------------------------------------

/// POST /memories/{memoryId}/events (201). moto's falsy checks with its
/// exact python-style messages; the id slot mirrors `params["memoryId"]`
/// (a missing path id stays moto's bare crash page).
fn create_event(req: &Request, st: &mut AgentCoreStore, body: &Value) -> Response {
    if body.is_null() && !req.body.is_empty() {
        return crate::proto::moto_crash_response();
    }
    let memory_id = slot_id(req);
    if memory_id.is_empty() {
        return crate::proto::moto_crash_response();
    }
    let Some(actor_id) = vstr(body, "actorId").filter(|a| !a.is_empty()) else {
        return validation(
            "1 validation error detected: Value at 'actorId' failed to satisfy constraint: Member must not be null",
        );
    };
    let Some(session_id) = vstr(body, "sessionId").filter(|s| !s.is_empty()) else {
        return validation(
            "1 validation error detected: Value at 'sessionId' failed to satisfy constraint: Member must not be null",
        );
    };
    let Some(payload) = body.get("payload").filter(|p| !p.is_null()) else {
        return validation(
            "1 validation error detected: Value at 'payload' failed to satisfy constraint: Member must not be empty",
        );
    };
    let event_timestamp = vnon_null(body, "eventTimestamp").unwrap_or_else(|| json!(now()));
    let branch = vnon_null(body, "branch").unwrap_or_else(|| json!({"name": "main"}));
    let metadata = body.get("metadata").cloned().unwrap_or(json!({}));
    let event_id = format!("{}#{}", Utc::now().timestamp_millis(), hex_id(16));
    let record = EventRecord {
        memory_id: memory_id.clone(),
        actor_id: actor_id.clone(),
        session_id: session_id.clone(),
        event_id: event_id.clone(),
        event_timestamp,
        payload: payload.clone(),
        branch,
        metadata,
    };
    let dict = record.to_dict(true);
    st.events
        .entry((memory_id, actor_id, session_id))
        .or_default()
        .push(record);
    ok_status(201, json!({ "event": dict }))
}

/// GET /memories/{m}/actor/{a}/sessions/{s}/events/{e}
fn get_event(req: &Request, st: &mut AgentCoreStore) -> Response {
    let (memory_id, actor_id, session_id, event_id) = event_path_keys(req);
    let Some(list) = st.events.get(&(memory_id, actor_id, session_id)) else {
        return event_missing(&event_id);
    };
    match list.iter().find(|e| e.event_id == event_id) {
        Some(record) => ok(json!({ "event": record.to_dict(true) })),
        None => event_missing(&event_id),
    }
}

fn event_missing(event_id: &str) -> Response {
    not_found(&format!("Event {event_id} not found"))
}

/// POST /memories/{m}/actor/{a}/sessions/{s} — moto honors `maxResults`
/// and returns a null token; `filter` is accepted and ignored.
fn list_events(req: &Request, st: &mut AgentCoreStore, body: &Value) -> Response {
    let segs = path_segs(req.path.as_str());
    let memory_id = segs.get(1).copied().unwrap_or_default().to_string();
    let actor_id = segs.get(3).copied().unwrap_or_default().to_string();
    let session_id = segs.get(5).copied().unwrap_or_default().to_string();
    let include_payloads = body
        .get("includePayloads")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let max_results = body
        .get("maxResults")
        .and_then(Value::as_i64)
        .map(|m| m as usize);
    let Some(list) = st.events.get(&(memory_id, actor_id, session_id)) else {
        return ok(json!({ "events": [], "nextToken": Value::Null }));
    };
    let mut events: Vec<Value> = list.iter().map(|e| e.to_dict(include_payloads)).collect();
    if let Some(max) = max_results {
        events = events.into_iter().take(max).collect();
    }
    ok(json!({ "events": events, "nextToken": Value::Null }))
}

/// DELETE /memories/{m}/actor/{a}/sessions/{s}/events/{e}
fn delete_event(req: &Request, st: &mut AgentCoreStore) -> Response {
    let (memory_id, actor_id, session_id, event_id) = event_path_keys(req);
    let Some(list) = st.events.get_mut(&(memory_id, actor_id, session_id)) else {
        return event_missing(&event_id);
    };
    let Some(index) = list.iter().position(|e| e.event_id == event_id) else {
        return event_missing(&event_id);
    };
    list.remove(index);
    ok(json!({ "eventId": event_id }))
}

/// POST /memories/{memoryId}/actors — distinct actor ids, sorted (moto has
/// no handler; the derived list stays deterministic by key order).
fn list_actors(req: &Request, st: &mut AgentCoreStore, body: &Value) -> Response {
    let memory_id = slot_id(req);
    let max_results = body
        .get("maxResults")
        .and_then(Value::as_i64)
        .filter(|m| *m > 0)
        .map(|m| m as usize)
        .unwrap_or(20);
    let token_id = vstr(body, "nextToken").and_then(|t| decode_token(&t));
    let mut actor_ids: Vec<String> = st
        .events
        .keys()
        .filter(|(m, _, _)| m == &memory_id)
        .map(|(_, a, _)| a.clone())
        .collect();
    actor_ids.sort();
    actor_ids.dedup();
    let summaries: Vec<Value> = actor_ids.iter().map(|a| json!({ "actorId": a })).collect();
    let (page, next_token) = id_page(summaries, "actorId", Some(max_results), token_id);
    ok(json!({ "actorSummaries": page, "nextToken": next_token }))
}

/// POST /memories/{memoryId}/actor/{actorId}/sessions — sessions derived
/// from the event keying, createdAt from the first event's timestamp.
fn list_sessions(req: &Request, st: &mut AgentCoreStore, body: &Value) -> Response {
    let segs = path_segs(req.path.as_str());
    let memory_id = segs.get(1).copied().unwrap_or("").to_string();
    let actor_id = segs.get(3).copied().unwrap_or("").to_string();
    let max_results = body
        .get("maxResults")
        .and_then(Value::as_i64)
        .filter(|m| *m > 0)
        .map(|m| m as usize)
        .unwrap_or(20);
    let token_id = vstr(body, "nextToken").and_then(|t| decode_token(&t));
    let summaries: Vec<Value> = st
        .events
        .iter()
        .filter(|((m, a, _), _)| m == &memory_id && a == &actor_id)
        .map(|((_, _, s), events)| {
            let created_at = events
                .first()
                .map(|e| e.event_timestamp.clone())
                .unwrap_or_else(|| json!(now()));
            json!({
                "sessionId": s,
                "actorId": actor_id,
                "createdAt": created_at,
            })
        })
        .collect();
    let (page, next_token) = id_page(summaries, "sessionId", Some(max_results), token_id);
    ok(json!({ "sessionSummaries": page, "nextToken": next_token }))
}

// ---------------------------------------------------------------------------
// tests — data-plane session/event CRUD (own account per test)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn reqacct(method: &str, path: &str, query: &str, body: Value, account: u64) -> Request {
        Request {
            service: "bedrock-agentcore".into(),
            operation: String::new(),
            method: method.into(),
            path: path.into(),
            query_string: query.into(),
            headers: std::collections::HashMap::new(),
            body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap()),
            account,
            region: "us-east-1".into(),
        }
    }

    fn body(resp: Option<Response>) -> Value {
        serde_json::from_slice(&resp.unwrap().body).unwrap()
    }

    #[test]
    fn code_interpreter_session_lifecycle() {
        let account = 437000000011;
        // the SYSTEM interpreter needs no create
        let started = body(dispatch(&reqacct(
            "PUT",
            "/code-interpreters/aws.codeinterpreter.v1/sessions/start",
            "",
            json!({"name": "do-things", "sessionTimeoutSeconds": 900}),
            account,
        )));
        let session_id = started["sessionId"].as_str().unwrap().to_string();
        assert_eq!(
            started["codeInterpreterIdentifier"],
            "aws.codeinterpreter.v1"
        );
        assert!(started["createdAt"].is_string());

        let got = body(dispatch(&reqacct(
            "GET",
            "/code-interpreters/aws.codeinterpreter.v1/sessions/get",
            &format!("sessionId={session_id}"),
            json!({}),
            account,
        )));
        assert_eq!(got["status"], "READY");
        assert_eq!(got["sessionId"], session_id.as_str());
        assert_eq!(got["sessionTimeoutSeconds"], 900);

        // unknown interpreter -> the modeled ResourceNotFoundException
        let missing = body(dispatch(&reqacct(
            "PUT",
            "/code-interpreters/nowhere-x/sessions/start",
            "",
            json!({}),
            account,
        )));
        assert_eq!(missing["__type"], "ResourceNotFoundException");
        assert_eq!(
            missing["message"],
            "Could not find Code Interpreter with ID nowhere-x"
        );

        // stop -> TERMINATED; the list endpoint's status filter honors it
        let stopped = body(dispatch(&reqacct(
            "PUT",
            "/code-interpreters/aws.codeinterpreter.v1/sessions/stop",
            "",
            json!({"sessionId": session_id}),
            account,
        )));
        assert!(stopped["lastUpdatedAt"].is_string());
        let listed = body(dispatch(&reqacct(
            "POST",
            "/code-interpreters/aws.codeinterpreter.v1/sessions/list",
            "",
            json!({"status": "TERMINATED"}),
            account,
        )));
        assert_eq!(listed["items"][0]["sessionId"], session_id.as_str());
        let active_list = body(dispatch(&reqacct(
            "POST",
            "/code-interpreters/aws.codeinterpreter.v1/sessions/list",
            "",
            json!({"status": "READY"}),
            account,
        )));
        assert_eq!(active_list["items"].as_array().unwrap().len(), 0);

        // stopping an unknown session → ResourceNotFoundException
        assert_eq!(
            body(dispatch(&reqacct(
                "PUT",
                "/code-interpreters/aws.codeinterpreter.v1/sessions/stop",
                "",
                json!({"sessionId": "missing"}),
                account,
            )))["__type"],
            "ResourceNotFoundException"
        );
    }

    #[test]
    fn event_validation_and_lifecycle() {
        let account = 437000000012;
        // moto's exact validation failure messages, in its order
        let no_actor = body(dispatch(&reqacct(
            "POST",
            "/memories/m1/events",
            "",
            json!({"sessionId": "s1", "payload": [{"conversational": {"text": "hi"}}]}),
            account,
        )));
        assert_eq!(no_actor["__type"], "ValidationException");
        assert!(no_actor["message"]
            .as_str()
            .unwrap()
            .contains("Value at 'actorId'"));
        let no_session = body(dispatch(&reqacct(
            "POST",
            "/memories/m1/events",
            "",
            json!({"actorId": "a1", "payload": [{"conversational": {"text": "hi"}}]}),
            account,
        )));
        assert!(no_session["message"]
            .as_str()
            .unwrap()
            .contains("Value at 'sessionId'"));
        let no_payload = body(dispatch(&reqacct(
            "POST",
            "/memories/m1/events",
            "",
            json!({"actorId": "a1", "sessionId": "s1"}),
            account,
        )));
        assert!(no_payload["message"]
            .as_str()
            .unwrap()
            .contains("Value at 'payload'"));

        // moto applies no memoryId existence check — the event lands anyway
        let created = dispatch(&reqacct(
            "POST",
            "/memories/m1/events",
            "",
            json!({
                "actorId": "a1",
                "sessionId": "s1",
                "payload": [{"conversational": {"text": "hi"}}],
                "metadata": {"k": "v"},
            }),
            account,
        ))
        .unwrap();
        assert_eq!(created.status, 201);
        let event = body(Some(created))["event"].clone();
        assert_eq!(event["memoryId"], "m1");
        assert_eq!(event["actorId"], "a1");
        assert_eq!(event["sessionId"], "s1");
        assert_eq!(event["branch"]["name"], "main");
        assert_eq!(event["metadata"], json!({"k": "v"}));
        assert!(event["eventId"].as_str().unwrap().contains('#'));
        let event_id = event["eventId"].as_str().unwrap().to_string();
        assert!(event["payload"].as_array().unwrap().len() == 1);

        // GetEvent on the same triple
        let got = body(dispatch(&reqacct(
            "GET",
            "/memories/m1/actor/a1/sessions/s1/events/{event_id}"
                .replace("{event_id}", &event_id)
                .as_str(),
            "",
            json!({}),
            account,
        )));
        assert_eq!(got["event"]["eventId"], event_id.as_str());

        // ListEvents: includePayloads=false strips the payload (moto's flag)
        let listed = body(dispatch(&reqacct(
            "POST",
            "/memories/m1/actor/a1/sessions/s1",
            "",
            json!({"includePayloads": false}),
            account,
        )));
        assert!(!listed["events"].as_array().unwrap().is_empty());
        assert!(listed["events"][0].get("payload").is_none());
        let listed_full = body(dispatch(&reqacct(
            "POST",
            "/memories/m1/actor/a1/sessions/s1",
            "",
            json!({"includePayloads": true, "maxResults": 1}),
            account,
        )));
        assert!(listed_full["events"][0].get("payload").is_some());
        assert!(listed_full.get("nextToken").is_some());

        // unknown event / unknown triple -> the exact moto message
        let missing_event = body(dispatch(&reqacct(
            "GET",
            "/memories/m1/actor/a1/sessions/s1/events/nope#1",
            "",
            json!({}),
            account,
        )));
        assert_eq!(missing_event["__type"], "ResourceNotFoundException");
        assert_eq!(missing_event["message"], "Event nope#1 not found");

        // DeleteEvent answers the id
        let deleted = body(dispatch(&reqacct(
            "DELETE",
            &format!("/memories/m1/actor/a1/sessions/s1/events/{event_id}"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(deleted["eventId"], event_id.as_str());
        assert!(
            body(dispatch(&reqacct(
                "GET",
                &format!("/memories/m1/actor/a1/sessions/s1/events/{event_id}"),
                "",
                json!({}),
                account,
            )))["__type"]
                == "ResourceNotFoundException"
        );
    }

    #[test]
    fn actors_sessions_derive_from_events() {
        let account = 437000000013;
        for (actor, session) in [("a1", "s1"), ("a1", "s2"), ("a2", "s3")] {
            dispatch(&reqacct(
                "POST",
                "/memories/m9/events",
                "",
                json!({
                    "actorId": actor,
                    "sessionId": session,
                    "payload": [{"conversational": {"text": "hey"}}],
                }),
                account,
            ))
            .unwrap();
        }
        let actors = body(dispatch(&reqacct(
            "POST",
            "/memories/m9/actors",
            "",
            json!({}),
            account,
        )));
        let actor_ids: Vec<&str> = actors["actorSummaries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a["actorId"].as_str().unwrap())
            .collect();
        assert_eq!(actor_ids, ["a1", "a2"]);

        let sessions = body(dispatch(&reqacct(
            "POST",
            "/memories/m9/actor/a1/sessions",
            "",
            json!({}),
            account,
        )));
        let session_ids: Vec<&str> = sessions["sessionSummaries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["sessionId"].as_str().unwrap())
            .collect();
        assert_eq!(session_ids, ["s1", "s2"]);
    }

    #[test]
    fn stream_op_and_records_stay_genuinely_absent() {
        let account = 437000000014;
        // InvokeCodeInterpreter's return is an event stream — no 501 for the
        // modeled result shape, the generic envelope only.
        let invoke = dispatch(&reqacct(
            "POST",
            "/code-interpreters/aws.codeinterpreter.v1/tools/invoke",
            "",
            json!({"name": "execute_code", "arguments": {"code": "x=1"}}),
            account,
        ))
        .unwrap();
        assert_eq!(invoke.status, 501);
        assert!(String::from_utf8_lossy(&invoke.body)
            .contains("invoke_code_interpreter action has not been implemented"));
        // memory records are absent too
        let record = dispatch(&reqacct(
            "POST",
            "/memories/m1/memoryRecords",
            "",
            json!({"namespace": "/ns"}),
            account,
        ))
        .unwrap();
        assert_eq!(record.status, 501);
    }
}
