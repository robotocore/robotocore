//! IoT Data Plane backend (rest-json: shadow + publish).
//!
//! Port of moto's `iotdata` service: UpdateThingShadow / GetThingShadow /
//! DeleteThingShadow (default + named), ListNamedShadowsForThing,
//! Publish and the retained-message/connection stubs. The shadow
//! document follows moto's FakeShadow semantics exactly: version bump
//! per write, `state.desired` / `state.reported` merged with
//! null-removal, a computed `delta`, and deleted shadows keeping only
//! `timestamp` + `version`.
//!
//! Shadow records live in this module's own (account, region) slot but
//! are tied to the control plane: every data-plane call requires the
//! thing to exist in [`crate::services::iot::iot_state`], mirroring
//! moto's `iot_backend.describe_thing` guard.

use crate::services::iot::iot_state;
use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

/// `(version, desired, reported, deleted)` — the FakeShadow fields.
type ShadowRecord = (i64, Option<Value>, Option<Value>, bool);

#[derive(Default)]
pub struct DataState {
    /// (thing_name, shadow_name) -> record; `None` shadow_name is the
    /// classic unnamed shadow (moto keys its dict by Option<str>).
    pub shadows: BTreeMap<(String, Option<String>), ShadowRecord>,
    /// (topic, payload) pairs — moto keeps `published_payloads`.
    pub published_payloads: Vec<(String, Vec<u8>)>,
}

fn data_state() -> store::Slot<DataState> {
    static SLOT: Lazy<store::Slot<DataState>> = Lazy::new(|| store::slot("iot-data"));
    SLOT.clone()
}

// ---------------------------------------------------------------------------
// Wire helpers
// ---------------------------------------------------------------------------

fn ok_json(v: &Value) -> Response {
    Response::new(
        200,
        "application/json",
        serde_json::to_vec(v).unwrap_or_default(),
    )
}

fn json_err(status: u16, code: &str, message: &str) -> Response {
    let body = json!({"__type": code, "message": message});
    let mut resp = Response::new(
        status,
        "application/json",
        serde_json::to_vec(&body).unwrap_or_default(),
    );
    resp.headers
        .push(("x-amzn-ErrorType".to_string(), format!("{code}:")));
    resp
}

fn not_found() -> Response {
    json_err(
        404,
        "ResourceNotFoundException",
        "The specified resource does not exist",
    )
}

fn invalid_request(message: &str) -> Response {
    json_err(400, "InvalidRequestException", message)
}

fn conflict_request(message: &str) -> Response {
    json_err(409, "ConflictException", message)
}

fn now_epoch() -> i64 {
    chrono::Utc::now().timestamp()
}

/// Merge `updates` into `state`, dropping null fields (moto's
/// `merge_dicts(..., remove_nulls=True)`).
fn merge_state(state: &mut Map<String, Value>, updates: &Map<String, Value>) {
    for (key, value) in updates {
        if value.is_null() {
            state.remove(key);
        } else if let (Some(Value::Object(target)), Some(new_obj)) =
            (state.get_mut(key), value.as_object())
        {
            for (sub_key, sub_value) in new_obj {
                if sub_value.is_null() {
                    target.remove(sub_key);
                } else if let Some(existing) = target.get_mut(sub_key) {
                    *existing = sub_value.clone();
                } else {
                    target.insert(sub_key.clone(), sub_value.clone());
                }
            }
        } else {
            let _ = state.insert(key.clone(), value.clone());
        }
    }
}

/// moto's FakeShadow._compute_delta: keys where desired differs from
/// reported (deep for maps, desired value otherwise).
fn compute_delta(desired: Option<&Value>, reported: Option<&Value>) -> Option<Value> {
    let desired_obj = desired.and_then(Value::as_object)?;
    let reported_obj = reported.and_then(Value::as_object);
    let mut delta = Map::new();
    for (key, want) in desired_obj {
        let rep = reported_obj.and_then(|r| r.get(key));
        match (want, rep) {
            (Value::Object(_), Some(Value::Object(rep_obj))) => {
                let rep_value = Value::Object(rep_obj.clone());
                if let Some(nested) = compute_delta(Some(want), Some(&rep_value)) {
                    delta.insert(key.clone(), nested);
                }
            }
            (_, Some(rep_value)) => {
                if rep_value != want {
                    delta.insert(key.clone(), want.clone());
                }
            }
            (_, None) => {
                delta.insert(key.clone(), want.clone());
            }
        }
    }
    if delta.is_empty() {
        None
    } else {
        Some(Value::Object(delta))
    }
}

fn metadata_for(state: Option<&Value>) -> Value {
    if let Some(v) = state {
        let mut meta = Map::new();
        for (k, _) in v.as_object().unwrap_or(&Map::new()) {
            let mut stamp = Map::new();
            stamp.insert("timestamp".into(), json!(now_epoch()));
            meta.insert(k.clone(), Value::Object(stamp));
        }
        Value::Object(meta)
    } else {
        Value::Null
    }
}

/// moto's FakeShadow.to_dict (GET shape, delta included).
fn shadow_to_dict(shadow: &ShadowRecord) -> Value {
    let (version, desired, reported, deleted) = shadow;
    if *deleted {
        return json!({"timestamp": now_epoch(), "version": version});
    }
    let mut state = Map::new();
    if let Some(d) = desired {
        state.insert("desired".into(), d.clone());
    }
    if let Some(r) = reported {
        state.insert("reported".into(), r.clone());
    }
    if let Some(delta) = compute_delta(desired.as_ref(), reported.as_ref()) {
        state.insert("delta".into(), delta);
    }
    let mut metadata = Map::new();
    if let Some(meta) = metadata_for(desired.as_ref()).as_object() {
        for (k, v) in meta {
            metadata.insert(k.clone(), v.clone());
        }
    }
    if let Some(meta) = metadata_for(reported.as_ref()).as_object() {
        for (k, v) in meta {
            metadata.insert(k.clone(), v.clone());
        }
    }
    json!({
        "state": Value::Object(state),
        "metadata": Value::Object(metadata),
        "timestamp": now_epoch(),
        "version": version,
    })
}

/// moto's FakeShadow.to_response_dict (UPDATE shape, request-driven).
fn shadow_to_response_dict(shadow: &ShadowRecord, requested: &Value) -> Value {
    let (version, _desired, _reported, _deleted) = shadow;
    let mut metadata = Map::new();
    let state: Value = match requested.get("state") {
        Some(Value::Null) => {
            metadata.insert("timestamp".into(), json!(now_epoch()));
            Value::Null
        }
        Some(target) => {
            let mut st = Map::new();
            if let Some(d) = target.get("desired").filter(|v| !v.is_null()) {
                st.insert("desired".into(), d.clone());
                if let Some(meta) = metadata_for(Some(d)).as_object() {
                    for (k, v) in meta {
                        metadata.insert(k.clone(), v.clone());
                    }
                }
            }
            if let Some(r) = target.get("reported").filter(|v| !v.is_null()) {
                st.insert("reported".into(), r.clone());
                if let Some(meta) = metadata_for(Some(r)).as_object() {
                    for (k, v) in meta {
                        metadata.insert(k.clone(), v.clone());
                    }
                }
            }
            Value::Object(st)
        }
        None => Value::Null,
    };
    json!({
        "state": state,
        "metadata": Value::Object(metadata),
        "timestamp": now_epoch(),
        "version": version,
    })
}

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "iotdata" {
        return None;
    }
    let method = req.method.as_str();
    let path = req.path.as_str();
    let query = crate::proto::query_map(&req.query_string);

    if let Some(topic) = path.strip_prefix("/topics/") {
        if method != "POST" {
            return None;
        }
        return Some(publish(req, topic));
    }
    if let Some(rest) = path.strip_prefix("/api/things/shadow/ListNamedShadowsForThing/") {
        if method != "GET" {
            return None;
        }
        let thing_name = decode_path_tail(rest);
        return Some(list_named_shadows_for_thing(req, &thing_name));
    }
    if let Some(rest) = path.strip_prefix("/things/") {
        let (thing_name, tail) = rest.split_once('/')?;
        if !tail.starts_with("shadow") {
            return None;
        }
        let shadow_name = query.get("name").and_then(|v| v.first().cloned());
        return match method {
            "POST" => Some(update_thing_shadow(
                req,
                &decode_path_tail(thing_name),
                shadow_name,
            )),
            "GET" => Some(get_thing_shadow(
                req,
                &decode_path_tail(thing_name),
                shadow_name,
            )),
            "DELETE" => Some(delete_thing_shadow(
                req,
                &decode_path_tail(thing_name),
                shadow_name,
            )),
            _ => None,
        };
    }
    if path.starts_with("/connections/") && method == "DELETE" {
        return Some(ok_json(&json!({})));
    }
    if path == "/retainedMessage" && method == "GET" {
        return Some(ok_json(&json!({
            "retainedMessages": [],
            "nextToken": Value::Null,
        })));
    }
    if path.starts_with("/retainedMessage/") {
        return Some(not_found());
    }
    None
}

/// Topic and thing names ride the URL path; decode `%XX` only (`+` is a
/// literal character in MQTT topics).
fn decode_path_tail(s: &str) -> String {
    pct_decode(s.split('?').next().unwrap_or(s))
}

fn pct_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' if i + 2 < b.len() => {
                let hexpart = std::str::from_utf8(&b[i + 1..i + 3]).unwrap_or("");
                match u8::from_str_radix(hexpart, 16) {
                    Ok(v) => {
                        out.push(v);
                        i += 3;
                    }
                    Err(_) => {
                        out.push(b[i]);
                        i += 1;
                    }
                }
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Thing must exist in the control plane (moto's describe_thing guard).
fn require_thing(req: &Request, thing_name: &str) -> Result<(), Response> {
    let slot = iot_state();
    let guard = slot.read();
    let scope = (req.account, req.region.clone());
    let exists = guard
        .get(&scope)
        .map(|s| {
            s.things
                .iter()
                .any(|t| t["thingName"].as_str() == Some(thing_name))
        })
        .unwrap_or(false);
    if exists {
        Ok(())
    } else {
        Err(not_found())
    }
}

fn publish(req: &Request, topic: &str) -> Response {
    let slot = data_state();
    let mut guard = slot.write();
    let s = guard.entry((req.account, req.region.clone())).or_default();
    s.published_payloads
        .push((decode_path_tail(topic), req.body.to_vec()));
    ok_json(&json!({}))
}

fn update_thing_shadow(req: &Request, thing_name: &str, shadow_name: Option<String>) -> Response {
    if require_thing(req, thing_name).is_err() {
        return not_found();
    }
    let Ok(parsed) = serde_json::from_str::<Value>(String::from_utf8_lossy(&req.body).as_ref())
    else {
        return invalid_request("invalid json");
    };
    if parsed.get("state").is_none() {
        return invalid_request("need node `state`");
    }
    let state = parsed["state"].clone();
    if !state.is_null() {
        if !state.is_object() {
            return invalid_request("state node must be an Object");
        }
        if let Some(obj) = state.as_object() {
            for key in obj.keys() {
                if key != "desired" && key != "reported" {
                    return invalid_request("State contains an invalid node");
                }
            }
        }
    }
    let key = (thing_name.to_string(), shadow_name.clone());
    let slot = data_state();
    let mut guard = slot.write();
    let s = guard.entry((req.account, req.region.clone())).or_default();
    let (previous_version, previous) = match s.shadows.get(&key) {
        Some((v, d, r, _)) => (*v, Some((d.clone(), r.clone()))),
        None => (0, None),
    };
    if let Some(want) = parsed.get("version").and_then(Value::as_i64) {
        if want != previous_version {
            return conflict_request("Version conflict");
        }
    }
    let version = previous_version + 1;
    let mut desired = previous
        .as_ref()
        .and_then(|(d, _)| d.clone())
        .unwrap_or_else(|| json!({}));
    let mut reported = previous
        .as_ref()
        .and_then(|(_, r)| r.clone())
        .unwrap_or_else(|| json!({}));
    for (member, current) in [("desired", &mut desired), ("reported", &mut reported)] {
        if let Some(update) = state
            .get(member)
            .filter(|v| !v.is_null())
            .and_then(Value::as_object)
        {
            if let Some(obj) = current.as_object_mut() {
                merge_state(obj, update);
            }
        }
    }
    let record: ShadowRecord = (
        version,
        (desired.as_object().map(|o| !o.is_empty()).unwrap_or(false)).then_some(desired.clone()),
        (reported.as_object().map(|o| !o.is_empty()).unwrap_or(false)).then_some(reported),
        false,
    );
    s.shadows.insert(key, record.clone());
    let requested = parsed;
    drop(guard);
    ok_json(&shadow_to_response_dict(&record, &requested))
}

fn get_thing_shadow(req: &Request, thing_name: &str, shadow_name: Option<String>) -> Response {
    if require_thing(req, thing_name).is_err() {
        return not_found();
    }
    let slot = data_state();
    let mut guard = slot.write();
    let s = guard.entry((req.account, req.region.clone())).or_default();
    let key = (thing_name.to_string(), shadow_name);
    match s.shadows.get(&key) {
        Some((_version, _desired, _reported, deleted)) if !*deleted => {
            let record: ShadowRecord = s.shadows[&key].clone();
            ok_json(&shadow_to_dict(&record))
        }
        _ => not_found(),
    }
}

fn delete_thing_shadow(req: &Request, thing_name: &str, shadow_name: Option<String>) -> Response {
    if require_thing(req, thing_name).is_err() {
        return not_found();
    }
    let key = (thing_name.to_string(), shadow_name);
    let slot = data_state();
    let mut guard = slot.write();
    let s = guard.entry((req.account, req.region.clone())).or_default();
    if !s.shadows.contains_key(&key) {
        return not_found();
    }
    let version = s.shadows[&key].0 + 1;
    s.shadows.insert(key, (version, None, None, true));
    ok_json(&json!({"timestamp": now_epoch(), "version": version}))
}

fn list_named_shadows_for_thing(req: &Request, thing_name: &str) -> Response {
    if require_thing(req, thing_name).is_err() {
        return not_found();
    }
    let slot = data_state();
    let guard = slot.read();
    let results: Vec<String> = guard
        .get(&(req.account, req.region.clone()))
        .map(|s| {
            s.shadows
                .keys()
                .filter_map(|(thing, name)| {
                    if *thing == thing_name {
                        name.clone()
                    } else {
                        None
                    }
                })
                .collect()
        })
        .unwrap_or_default();
    ok_json(&json!({"results": results}))
}

// ---------------------------------------------------------------------------
// Unit tests — shadow semantics (moto parity) + publish.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn scope() -> u64 {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    fn iot_req(method: &str, path: &str, body: Value, account: u64) -> Request {
        Request {
            service: "iot".into(),
            operation: String::new(),
            method: method.into(),
            path: path.into(),
            query_string: String::new(),
            headers: std::collections::HashMap::new(),
            body: serde_json::to_vec(&body).unwrap_or_default().into(),
            account,
            region: "us-east-1".into(),
        }
    }

    fn data_req(method: &str, path: &str, payload: &str, account: u64) -> Request {
        Request {
            service: "iotdata".into(),
            operation: String::new(),
            method: method.into(),
            path: path.into(),
            query_string: String::new(),
            headers: std::collections::HashMap::new(),
            body: payload.as_bytes().to_vec().into(),
            account,
            region: "us-east-1".into(),
        }
    }

    fn seed_thing(account: u64, thing: &str) {
        let slot = iot_state();
        let mut guard = slot.write();
        guard
            .entry((account, "us-east-1".to_string()))
            .or_default()
            .things
            .push(json!({
                "thingName": thing,
                "thingArn": format!("arn:aws:iot:us-east-1:{account}:thing/{thing}"),
            }));
    }

    fn record(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap_or(Value::Null)
    }

    #[test]
    fn shadow_update_get_delete_roundtrip() {
        let account = scope();
        let thing = format!("shadow-{}", account % 1000);
        let slot = iot_state();
        let mut guard = slot.write();
        guard
            .entry((account, "us-east-1".to_string()))
            .or_default()
            .things
            .push(json!({"thingName": thing}));
        drop(guard);

        let resp = dispatch(&data_req(
            "POST",
            &format!("/things/{thing}/shadow"),
            r#"{"state": {"desired": {"temperature": 72}}}"#,
            account,
        ))
        .expect("response");
        assert_eq!(resp.status, 200);
        let updated = record(&resp);
        assert_eq!(updated["state"]["desired"]["temperature"], json!(72));
        assert_eq!(updated["version"], json!(1));

        let resp = dispatch(&data_req(
            "GET",
            &format!("/things/{thing}/shadow"),
            "",
            account,
        ))
        .expect("response");
        let got = record(&resp);
        assert_eq!(got["state"]["desired"]["temperature"], json!(72));

        // version conflict on stale write
        let conflict = dispatch(&data_req(
            "POST",
            &format!("/things/{thing}/shadow"),
            r#"{"state": {"desired": {"temperature": 70}}, "version": 0}"#,
            account,
        ))
        .expect("response");
        assert_eq!(conflict.status, 409);

        let deleted = dispatch(&data_req(
            "DELETE",
            &format!("/things/{thing}/shadow"),
            "",
            account,
        ))
        .expect("response");
        assert_eq!(deleted.status, 200);
        let gone = dispatch(&data_req(
            "GET",
            &format!("/things/{thing}/shadow"),
            "",
            account,
        ))
        .expect("response");
        assert_eq!(gone.status, 404);
    }

    #[test]
    fn shadow_requires_existing_thing_and_valid_state() {
        let account = scope() + 7;
        let ghost = format!("no-thing-{}", account);
        let notfound = dispatch(&data_req(
            "POST",
            &format!("/things/{ghost}/shadow"),
            r#"{"state": {}}"#,
            account,
        ))
        .expect("response");
        assert_eq!(notfound.status, 404);

        let thing = format!("valid-{}", account);
        let slot = iot_state();
        let mut guard = slot.write();
        guard
            .entry((account, "us-east-1".to_string()))
            .or_default()
            .things
            .push(json!({"thingName": thing}));
        drop(guard);

        let bad = dispatch(&data_req(
            "POST",
            &format!("/things/{thing}/shadow"),
            "not json",
            account,
        ))
        .expect("response");
        assert_eq!(bad.status, 400);

        let r = dispatch(&data_req(
            "POST",
            &format!("/things/{thing}/shadow"),
            r#"{"state": {}}"#,
            account,
        ));
        assert_eq!(r.expect("response").status, 200);

        let r = dispatch(&data_req(
            "GET",
            &format!("/api/things/shadow/ListNamedShadowsForThing/{thing}"),
            "",
            account,
        ));
        let listed = record(&r.expect("response"));
        assert_eq!(listed["results"], json!([]));
    }

    #[test]
    fn publish_is_recorded_per_scope() {
        let account = scope() + 100;
        let r = dispatch(&data_req(
            "POST",
            "/topics/room/sensor",
            "payload-bytes",
            account,
        ));
        assert_eq!(r.expect("response").status, 200);
        let slot = data_state();
        let guard = slot.read();
        let s = guard.get(&(account, "us-east-1".to_string())).unwrap();
        assert_eq!(s.published_payloads.len(), 1);
        assert_eq!(s.published_payloads[0].0, "room/sensor");
        assert_eq!(s.published_payloads[0].1, b"payload-bytes".to_vec());
    }
}
