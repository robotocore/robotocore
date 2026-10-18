//! IVS backend (REST-JSON, path-per-operation).
//!
//! Port of moto's ivs: channels, stream keys, playback key pairs,
//! recording configurations, playback restriction policies and the
//! stream/session/metadata/tag operations. Wire shapes mirror moto's
//! lowercase camel JSON keys; errors use JsonRESTError statuses
//! (ResourceNotFound 404, Conflict 409).

use crate::proto::parse_json_body;
use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Value};

#[derive(Default)]
struct State {
    channels: Vec<Value>,
    stream_keys: Vec<Value>,
    playback_key_pairs: Vec<Value>,
    recording_configurations: Vec<Value>,
    restriction_policies: Vec<Value>,
    /// resource arn -> tags map (only present once first tagged or
    /// created with tags, like moto's `tags_store`).
    tags_store: Vec<(String, Value)>,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("ivs"));
    SLOT.clone()
}

fn partition(region: &str) -> &'static str {
    for (prefix, name) in [
        ("cn-", "aws-cn"),
        ("us-gov-", "aws-us-gov"),
        ("us-iso-", "aws-iso"),
        ("us-isob-", "aws-iso-b"),
        ("eusc-", "aws-eusc"),
    ] {
        if region.starts_with(prefix) {
            return name;
        }
    }
    "aws"
}

fn ok_json(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

fn ok_empty(status: u16) -> Response {
    Response::new(status, "application/json", Vec::new())
}

fn not_found(arn: &str) -> Response {
    let body = json!({
        "__type": "ResourceNotFoundException",
        "message": format!("Resource: {arn} not found"),
    });
    Response::json(404, serde_json::to_vec(&body).unwrap_or_default())
}

fn conflict(message: &str) -> Response {
    let body = json!({"__type": "ConflictException", "message": message});
    Response::json(409, serde_json::to_vec(&body).unwrap_or_default())
}

/// Percent-decode a path segment (ARNs ride encoded in /tags/{arn}).
/// NB: this casts decoded bytes one at a time, fine for ARNs (ASCII).
fn pct_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut decoded = String::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' if i + 2 < b.len() => {
                let hex = std::str::from_utf8(&b[i + 1..i + 3]).unwrap_or("");
                match u8::from_str_radix(hex, 16) {
                    Ok(v) => {
                        decoded.push(v as char);
                        i += 3;
                    }
                    Err(_) => {
                        decoded.push(b[i] as char);
                        i += 1;
                    }
                }
            }
            c => {
                decoded.push(c as char);
                i += 1;
            }
        }
    }
    decoded
}

/// moto's get_random_string(12): lowercase letters + digits.
fn random_string(len: usize) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let bytes = uuid::Uuid::new_v4().as_bytes().to_vec();
    (0..len)
        .map(|i| ALPHABET[bytes[i % bytes.len()] as usize % ALPHABET.len()] as char)
        .collect()
}

fn arn_for(region: &str, account: u64, kind: &str, id: &str) -> String {
    format!(
        "arn:{}:ivs:{}:{}:{kind}/{}",
        partition(region),
        region,
        account,
        id
    )
}

pub fn dispatch(req: &Request) -> Option<Response> {
    // Tag operations ride on /tags/{resourceArn} (GET list / POST tag /
    // DELETE untag), per botocore's model and moto's url_paths; the ARN
    // comes from the path, params from the body or query string.
    if let Some(raw_arn) = req.path.strip_prefix("/tags/").filter(|s| !s.is_empty()) {
        let op = match req.method.as_str() {
            "GET" => "ListTagsForResource",
            "POST" => "TagResource",
            "DELETE" => "UntagResource",
            _ => return None,
        };
        let arn = pct_decode(raw_arn);
        let mut pl = parse_json_body(&req.body);
        if let Some(obj) = pl.as_object_mut() {
            obj.insert("resourceArn".into(), json!(arn));
        }
        if op == "UntagResource" {
            // DELETE rest-json carries tagKeys in the query string.
            if pl.get("tagKeys").is_none() && !req.query_string.is_empty() {
                let keys: Vec<Value> = req
                    .query_string
                    .split('&')
                    .filter_map(|kv| kv.strip_prefix("tagKeys="))
                    .map(|kv| Value::String(kv.trim_start_matches("member.").to_string()))
                    .collect();
                pl["tagKeys"] = Value::Array(keys);
            }
        }
        let slot = state();
        let mut st = slot.write();
        let b = st.entry((req.account, req.region.clone())).or_default();
        let resp = match op {
            "ListTagsForResource" => list_tags_for_resource(b, &pl),
            "TagResource" => tag_resource(b, &pl),
            _ => untag_resource(b, &pl),
        };
        return Some(resp);
    }

    let op = match req.path.as_str() {
        "/CreateChannel" => "CreateChannel",
        "/ListChannels" => "ListChannels",
        "/GetChannel" => "GetChannel",
        "/BatchGetChannel" => "BatchGetChannel",
        "/UpdateChannel" => "UpdateChannel",
        "/DeleteChannel" => "DeleteChannel",
        "/CreateStreamKey" => "CreateStreamKey",
        "/GetStreamKey" => "GetStreamKey",
        "/DeleteStreamKey" => "DeleteStreamKey",
        "/BatchGetStreamKey" => "BatchGetStreamKey",
        "/ListStreamKeys" => "ListStreamKeys",
        "/ImportPlaybackKeyPair" => "ImportPlaybackKeyPair",
        "/GetPlaybackKeyPair" => "GetPlaybackKeyPair",
        "/DeletePlaybackKeyPair" => "DeletePlaybackKeyPair",
        "/ListPlaybackKeyPairs" => "ListPlaybackKeyPairs",
        "/CreatePlaybackRestrictionPolicy" => "CreatePlaybackRestrictionPolicy",
        "/GetPlaybackRestrictionPolicy" => "GetPlaybackRestrictionPolicy",
        "/UpdatePlaybackRestrictionPolicy" => "UpdatePlaybackRestrictionPolicy",
        "/DeletePlaybackRestrictionPolicy" => "DeletePlaybackRestrictionPolicy",
        "/ListPlaybackRestrictionPolicies" => "ListPlaybackRestrictionPolicies",
        "/ListStreams" => "ListStreams",
        "/StopStream" => "StopStream",
        "/PutMetadata" => "PutMetadata",
        "/ListStreamSessions" => "ListStreamSessions",
        "/GetStreamSession" => "GetStreamSession",
        "/StartViewerSessionRevocation" => "StartViewerSessionRevocation",
        "/BatchStartViewerSessionRevocation" => "BatchStartViewerSessionRevocation",
        "/CreateRecordingConfiguration" => "CreateRecordingConfiguration",
        "/GetRecordingConfiguration" => "GetRecordingConfiguration",
        "/DeleteRecordingConfiguration" => "DeleteRecordingConfiguration",
        "/ListRecordingConfigurations" => "ListRecordingConfigurations",
        "/TagResource" => "TagResource",
        "/UntagResource" => "UntagResource",
        "/ListTagsForResource" => "ListTagsForResource",
        _ => return None,
    };
    if req.method != "POST" {
        return None;
    }
    let p = parse_json_body(&req.body);

    let slot = state();
    let mut st = slot.write();
    let b = st.entry((req.account, req.region.clone())).or_default();

    handle(op, b, &p, &req.region, req.account)
}

fn handle(op: &str, st: &mut State, p: &Value, region: &str, account: u64) -> Option<Response> {
    let resp = match op {
        "CreateChannel" => create_channel(st, p, region, account),
        "ListChannels" => list_channels(st, p),
        "GetChannel" => by_arn_wrap(&st.channels, p, "channel"),
        "BatchGetChannel" => batch_get(&st.channels, p, "channels"),
        "UpdateChannel" => update_channel(st, p),
        "DeleteChannel" => delete_channel(st, p),
        "CreateStreamKey" => create_stream_key(st, p, region, account),
        "GetStreamKey" => by_arn_wrap(&st.stream_keys, p, "streamKey"),
        "DeleteStreamKey" => delete_stream_key(st, p),
        "BatchGetStreamKey" => batch_get(&st.stream_keys, p, "streamKeys"),
        "ListStreamKeys" => list_stream_keys(st, p),
        "ImportPlaybackKeyPair" => import_playback_key_pair(st, p, region, account),
        "GetPlaybackKeyPair" => by_arn_wrap(&st.playback_key_pairs, p, "keyPair"),
        "DeletePlaybackKeyPair" => delete_playback_key_pair(st, p),
        "ListPlaybackKeyPairs" => list_playback_key_pairs(st),
        "CreatePlaybackRestrictionPolicy" => {
            create_playback_restriction_policy(st, p, region, account)
        }
        "GetPlaybackRestrictionPolicy" => get_policy(st, p),
        "UpdatePlaybackRestrictionPolicy" => update_policy(st, p),
        "DeletePlaybackRestrictionPolicy" => delete_policy(st, p),
        "ListPlaybackRestrictionPolicies" => ok_json(&json!({
            "playbackRestrictionPolicies": st.restriction_policies.clone(),
            "nextToken": Value::Null,
        })),
        "ListStreams" => ok_json(&json!({"streams": [], "nextToken": Value::Null})),
        "StopStream" => stop_stream(st, p),
        "PutMetadata" => put_metadata(st, p),
        "ListStreamSessions" => list_stream_sessions(st, p),
        "GetStreamSession" => get_stream_session(st, p),
        "StartViewerSessionRevocation" => channel_exists_then(st, p, || ok_json(&json!({}))),
        "BatchStartViewerSessionRevocation" => batch_start_viewer_session_revocation(st, p),
        "CreateRecordingConfiguration" => create_recording_configuration(st, p, region, account),
        "GetRecordingConfiguration" => {
            by_arn_wrap(&st.recording_configurations, p, "recordingConfiguration")
        }
        "DeleteRecordingConfiguration" => delete_recording_configuration(st, p),
        "ListRecordingConfigurations" => ok_json(&json!({
            "recordingConfigurations": st.recording_configurations.clone(),
            "nextToken": Value::Null,
        })),
        "TagResource" => tag_resource(st, p),
        "UntagResource" => untag_resource(st, p),
        "ListTagsForResource" => list_tags_for_resource(st, p),
        _ => return None,
    };
    Some(resp)
}

fn by_arn_wrap(records: &[Value], p: &Value, key: &str) -> Response {
    let arn = p["arn"].as_str().unwrap_or_default();
    match records.iter().find(|v| v["arn"].as_str() == Some(arn)) {
        Some(v) => ok_json(&json!({ key: v })),
        None => not_found(arn),
    }
}

/// Fetch records whose arn is in the request's `arns` list, wrapped with
/// moto's batch shape (matching records + empty errors).
fn batch_get(records: &[Value], p: &Value, key: &str) -> Response {
    let arns = p["arns"].as_array().cloned().unwrap_or_default();
    let items: Vec<Value> = records
        .iter()
        .filter(|v| arns.iter().any(|a| *a == v["arn"]))
        .cloned()
        .collect();
    ok_json(&json!({ key: items, "errors": [] }))
}

/// Run the handler body after verifying the channel-Arn param exists.
fn channel_exists_then<F: FnOnce() -> Response>(st: &State, p: &Value, f: F) -> Response {
    let channel_arn = p["channelArn"].as_str().unwrap_or_default();
    if st
        .channels
        .iter()
        .all(|c| c["arn"].as_str() != Some(channel_arn))
    {
        return not_found(channel_arn);
    }
    f()
}

fn new_stream_key(channel_arn: &str, tags: &Value, region: &str, account: u64) -> Value {
    let id = random_string(12);
    json!({
        "arn": arn_for(region, account, "stream-key", &id),
        "channelArn": channel_arn,
        "tags": tags.clone(),
        "value": format!("sk_{region}_{}", uuid::Uuid::new_v4().simple()),
    })
}

fn create_channel(st: &mut State, p: &Value, region: &str, account: u64) -> Response {
    let id = random_string(12);
    let tags = if p["tags"].is_null() {
        json!({})
    } else {
        p["tags"].clone()
    };
    let channel = json!({
        "arn": arn_for(region, account, "channel", &id),
        "authorized": if p["authorized"].is_null() { json!(false) } else { p["authorized"].clone() },
        "ingestEndpoint": "ingest.example.com",
        "insecureIngest": if p["insecureIngest"].is_null() { json!(false) } else { p["insecureIngest"].clone() },
        "latencyMode": if p["latencyMode"].is_null() { json!("LOW") } else { p["latencyMode"].clone() },
        "name": p["name"].clone(),
        "playbackUrl": format!("https://playback.example.com/{region}.{account}.{id}.m3u8"),
        "preset": if p["preset"].is_null() { json!("") } else { p["preset"].clone() },
        "recordingConfigurationArn": if p["recordingConfigurationArn"].is_null() { json!("") } else { p["recordingConfigurationArn"].clone() },
        "tags": &tags,
        "type": if p["type"].is_null() { json!("STANDARD") } else { p["type"].clone() },
    });
    let arn = channel["arn"].as_str().unwrap_or_default().to_string();
    if tags.as_object().map(|m| !m.is_empty()).unwrap_or(false) {
        st.tags_store.push((arn.clone(), tags.clone()));
    }
    st.channels.push(channel.clone());
    let stream_key = new_stream_key(&arn, &tags, region, account);
    st.stream_keys.push(stream_key.clone());
    ok_json(&json!({"channel": channel, "streamKey": stream_key}))
}

fn list_channels(st: &State, p: &Value) -> Response {
    let items: Vec<Value> = if let Some(name) = p["filterByName"].as_str() {
        st.channels
            .iter()
            .filter(|c| c["name"].as_str() == Some(name))
            .cloned()
            .collect()
    } else {
        match p["filterByRecordingConfigurationArn"].as_str() {
            Some(rc_arn) => st
                .channels
                .iter()
                .filter(|c| c["recordingConfigurationArn"].as_str() == Some(rc_arn))
                .cloned()
                .collect(),
            None => st.channels.clone(),
        }
    };
    ok_json(&json!({"channels": items, "nextToken": Value::Null}))
}

fn update_channel(st: &mut State, p: &Value) -> Response {
    let arn = p["arn"].as_str().unwrap_or_default();
    let Some(idx) = st
        .channels
        .iter()
        .position(|c| c["arn"].as_str() == Some(arn))
    else {
        return not_found(arn);
    };
    let c = &mut st.channels[idx];
    for key in ["authorized", "insecureIngest"] {
        if let Some(v) = p[key].as_bool() {
            c[key] = json!(v);
        }
    }
    for key in [
        "latencyMode",
        "name",
        "preset",
        "recordingConfigurationArn",
        "type",
    ] {
        if let Some(v) = p[key].as_str() {
            c[key] = json!(v);
        }
    }
    ok_json(&json!({"channel": st.channels[idx].clone()}))
}

fn delete_channel(st: &mut State, p: &Value) -> Response {
    let arn = p["arn"].as_str().unwrap_or_default().to_string();
    if st
        .channels
        .iter()
        .all(|c| c["arn"].as_str() != Some(arn.as_str()))
    {
        return not_found(&arn);
    }
    st.channels
        .retain(|c| c["arn"].as_str() != Some(arn.as_str()));
    st.stream_keys
        .retain(|sk| sk["channelArn"].as_str() != Some(arn.as_str()));
    st.tags_store.retain(|(k, _)| *k != arn);
    ok_empty(200)
}

fn by_arn_idx(records: &[Value], arn: &str) -> Option<usize> {
    records.iter().position(|v| v["arn"].as_str() == Some(arn))
}

fn create_stream_key(st: &mut State, p: &Value, region: &str, account: u64) -> Response {
    let channel_arn = p["channelArn"].as_str().unwrap_or_default();
    if st
        .channels
        .iter()
        .all(|c| c["arn"].as_str() != Some(channel_arn))
    {
        return not_found(channel_arn);
    }
    let tags = if p["tags"].is_null() {
        json!({})
    } else {
        p["tags"].clone()
    };
    let sk = new_stream_key(channel_arn, &tags, region, account);
    let arn = sk["arn"].as_str().unwrap_or_default().to_string();
    if tags.as_object().map(|m| !m.is_empty()).unwrap_or(false) {
        st.tags_store.push((arn.clone(), tags));
    }
    st.stream_keys.push(sk.clone());
    ok_json(&json!({"streamKey": sk}))
}

fn delete_stream_key(st: &mut State, p: &Value) -> Response {
    let arn = p["arn"].as_str().unwrap_or_default().to_string();
    if by_arn_idx(&st.stream_keys, &arn).is_none() {
        return not_found(&arn);
    }
    st.stream_keys
        .retain(|sk| sk["arn"].as_str() != Some(arn.as_str()));
    st.tags_store.retain(|(k, _)| *k != arn);
    ok_empty(200)
}

fn list_stream_keys(st: &State, p: &Value) -> Response {
    let channel_arn = p["channelArn"].as_str().unwrap_or_default();
    let items: Vec<Value> = st
        .stream_keys
        .iter()
        .filter(|sk| sk["channelArn"].as_str() == Some(channel_arn))
        .cloned()
        .collect();
    ok_json(&json!({"streamKeys": items, "nextToken": Value::Null}))
}

fn import_playback_key_pair(st: &mut State, p: &Value, region: &str, account: u64) -> Response {
    let id = random_string(12);
    let name = p["name"].as_str().unwrap_or_default();
    let tags = if p["tags"].is_null() {
        json!({})
    } else {
        p["tags"].clone()
    };
    let kp = json!({
        "arn": arn_for(region, account, "playback-key", &id),
        "fingerprint": uuid::Uuid::new_v4().simple().to_string(),
        "name": name,
        "tags": tags,
    });
    let arn = kp["arn"].as_str().unwrap_or_default().to_string();
    if kp["tags"]
        .as_object()
        .map(|m| !m.is_empty())
        .unwrap_or(false)
    {
        st.tags_store.push((arn.clone(), kp["tags"].clone()));
    }
    st.playback_key_pairs.push(kp.clone());
    ok_json(&json!({"keyPair": kp}))
}

fn delete_playback_key_pair(st: &mut State, p: &Value) -> Response {
    let arn = p["arn"].as_str().unwrap_or_default().to_string();
    if by_arn_idx(&st.playback_key_pairs, &arn).is_none() {
        return not_found(&arn);
    }
    st.playback_key_pairs
        .retain(|kp| kp["arn"].as_str() != Some(arn.as_str()));
    st.tags_store.retain(|(k, _)| *k != arn);
    ok_empty(200)
}

fn list_playback_key_pairs(st: &State) -> Response {
    ok_json(&json!({
        "keyPairs": st.playback_key_pairs.clone(),
        "nextToken": Value::Null,
    }))
}

fn create_recording_configuration(
    st: &mut State,
    p: &Value,
    region: &str,
    account: u64,
) -> Response {
    let id = random_string(12);
    let name = p["name"].as_str().unwrap_or_default();
    let rc = json!({
        "arn": arn_for(region, account, "recording-configuration", &id),
        "destinationConfiguration": p["destinationConfiguration"].clone(),
        "name": name,
        "recordingReconnectWindowSeconds": if p["recordingReconnectWindowSeconds"].is_null() { json!(0) } else { p["recordingReconnectWindowSeconds"].clone() },
        "renditionConfiguration": if p["renditionConfiguration"].is_null() { json!({}) } else { p["renditionConfiguration"].clone() },
        "state": "ACTIVE",
        "tags": p["tags"].clone(),
        "thumbnailConfiguration": if p["thumbnailConfiguration"].is_null() { json!({}) } else { p["thumbnailConfiguration"].clone() },
    });
    let arn = rc["arn"].as_str().unwrap_or_default().to_string();
    if rc["tags"]
        .as_object()
        .map(|m| !m.is_empty())
        .unwrap_or(false)
    {
        st.tags_store.push((arn.clone(), rc["tags"].clone()));
    }
    st.recording_configurations.push(rc.clone());
    ok_json(&json!({"recordingConfiguration": rc}))
}

fn delete_recording_configuration(st: &mut State, p: &Value) -> Response {
    let arn = p["arn"].as_str().unwrap_or_default().to_string();
    if by_arn_idx(&st.recording_configurations, &arn).is_none() {
        return not_found(&arn);
    }
    if let Some(c) = st
        .channels
        .iter()
        .find(|c| c["recordingConfigurationArn"].as_str() == Some(arn.as_str()))
    {
        let channel_arn = c["arn"].as_str().unwrap_or_default();
        return conflict(&format!(
            "Recording configuration {arn} is in use by channel {channel_arn}"
        ));
    }
    st.recording_configurations
        .retain(|rc| rc["arn"].as_str() != Some(arn.as_str()));
    st.tags_store.retain(|(k, _)| *k != arn);
    ok_empty(200)
}

fn create_playback_restriction_policy(
    st: &mut State,
    p: &Value,
    region: &str,
    account: u64,
) -> Response {
    let id = random_string(12);
    let name = p["name"].as_str().unwrap_or_default();
    let tags = if p["tags"].is_null() {
        json!({})
    } else {
        p["tags"].clone()
    };
    let policy = json!({
        "arn": arn_for(region, account, "playback-restriction-policy", &id),
        "name": name,
        "allowedCountries": if p["allowedCountries"].is_null() { json!([]) } else { p["allowedCountries"].clone() },
        "allowedOrigins": if p["allowedOrigins"].is_null() { json!([]) } else { p["allowedOrigins"].clone() },
        "enableStrictOriginEnforcement": if p["enableStrictOriginEnforcement"].is_null() { json!(false) } else { p["enableStrictOriginEnforcement"].clone() },
        "tags": tags,
    });
    let arn = policy["arn"].as_str().unwrap_or_default().to_string();
    if policy["tags"]
        .as_object()
        .map(|m| !m.is_empty())
        .unwrap_or(false)
    {
        st.tags_store.push((arn.clone(), policy["tags"].clone()));
    }
    st.restriction_policies.push(policy.clone());
    ok_json(&json!({"playbackRestrictionPolicy": policy}))
}

fn get_policy(st: &State, p: &Value) -> Response {
    let arn = p["arn"].as_str().unwrap_or_default();
    match st
        .restriction_policies
        .iter()
        .find(|v| v["arn"].as_str() == Some(arn))
    {
        Some(v) => ok_json(&json!({"playbackRestrictionPolicy": v})),
        None => not_found(arn),
    }
}

fn update_policy(st: &mut State, p: &Value) -> Response {
    let arn = p["arn"].as_str().unwrap_or_default();
    match st
        .restriction_policies
        .iter_mut()
        .find(|v| v["arn"].as_str() == Some(arn))
    {
        Some(policy) => {
            if p["allowedCountries"].is_array() {
                policy["allowedCountries"] = p["allowedCountries"].clone();
            }
            if p["allowedOrigins"].is_array() {
                policy["allowedOrigins"] = p["allowedOrigins"].clone();
            }
            if let Some(v) = p["enableStrictOriginEnforcement"].as_bool() {
                policy["enableStrictOriginEnforcement"] = json!(v);
            }
            if let Some(v) = p["name"].as_str() {
                policy["name"] = json!(v);
            }
            ok_json(&json!({"playbackRestrictionPolicy": policy.clone()}))
        }
        None => not_found(arn),
    }
}

fn delete_policy(st: &mut State, p: &Value) -> Response {
    let arn = p["arn"].as_str().unwrap_or_default().to_string();
    if st
        .restriction_policies
        .iter()
        .all(|v| v["arn"].as_str() != Some(arn.as_str()))
    {
        return not_found(&arn);
    }
    st.restriction_policies
        .retain(|v| v["arn"].as_str() != Some(arn.as_str()));
    st.tags_store.retain(|(k, _)| *k != arn);
    ok_empty(204)
}

fn stop_stream(st: &mut State, p: &Value) -> Response {
    channel_exists_then(st, p, || ok_json(&json!({})))
}

fn put_metadata(st: &State, p: &Value) -> Response {
    let channel_arn = p["channelArn"].as_str().unwrap_or_default();
    if st
        .channels
        .iter()
        .all(|c| c["arn"].as_str() != Some(channel_arn))
    {
        return not_found(channel_arn);
    }
    ok_json(&json!({}))
}

fn list_stream_sessions(st: &State, p: &Value) -> Response {
    channel_exists_then(st, p, || {
        ok_json(&json!({"streamSessions": [], "nextToken": Value::Null}))
    })
}

fn get_stream_session(st: &State, p: &Value) -> Response {
    let channel_arn = p["channelArn"].as_str().unwrap_or_default();
    if st
        .channels
        .iter()
        .all(|c| c["arn"].as_str() != Some(channel_arn))
    {
        return not_found(channel_arn);
    }
    // moto keeps no session records; without a specific live streamId the
    // backend synthesizes an ACTIVE session.
    let stream_id = p["streamId"].as_str();
    if let Some(stream_id) = stream_id {
        return not_found(stream_id); // no sessions are ever stored
    }
    let session = json!({
        "channelArn": channel_arn,
        "streamId": random_string(12),
        "health": "HEALTHY",
        "state": "LIVE",
        "truncatedEvents": [],
    });
    ok_json(&json!({"streamSession": session}))
}

fn batch_start_viewer_session_revocation(st: &State, p: &Value) -> Response {
    let mut errors: Vec<Value> = Vec::new();
    if let Some(sessions) = p["viewerSessions"].as_array() {
        for vs in sessions {
            let channel_arn = vs["channelArn"].as_str().unwrap_or_default();
            if st
                .channels
                .iter()
                .all(|c| c["arn"].as_str() != Some(channel_arn))
            {
                errors.push(json!({
                    "channelArn": channel_arn,
                    "viewerId": vs["viewerId"].as_str().unwrap_or_default(),
                    "code": "ResourceNotFoundException",
                    "message": format!("Resource: {channel_arn} not found"),
                }));
            }
        }
    }
    ok_json(&json!({"errors": errors}))
}

fn stored_tags<'a>(st: &'a State, arn: &str) -> Option<&'a Value> {
    st.tags_store
        .iter()
        .find(|(k, _)| *k == arn)
        .map(|(_, v)| v)
}

fn stored_tags_mut<'a>(st: &'a mut State, arn: &str) -> Option<&'a mut Value> {
    st.tags_store
        .iter_mut()
        .find(|(k, _)| *k == arn)
        .map(|(_, v)| v)
}

/// Push the tags_store copy back onto the resource record (moto keeps both
/// in sync for eligible resources).
fn sync_tags(st: &mut State, arn: &str) {
    let stored = stored_tags(st, arn).cloned();
    if let Some(res) = find_resource_mut(st, arn) {
        if let (Some(obj), Some(tags)) = (res.as_object_mut(), stored) {
            obj.insert("tags".into(), tags);
        }
    }
}

fn find_resource_mut<'a>(st: &'a mut State, arn: &str) -> Option<&'a mut Value> {
    st.channels
        .iter_mut()
        .find(|v| v["arn"].as_str() == Some(arn))
        .or_else(|| {
            st.stream_keys
                .iter_mut()
                .find(|v| v["arn"].as_str() == Some(arn))
        })
        .or_else(|| {
            st.playback_key_pairs
                .iter_mut()
                .find(|v| v["arn"].as_str() == Some(arn))
        })
        .or_else(|| {
            st.recording_configurations
                .iter_mut()
                .find(|v| v["arn"].as_str() == Some(arn))
        })
        .or_else(|| {
            st.restriction_policies
                .iter_mut()
                .find(|v| v["arn"].as_str() == Some(arn))
        })
}

fn tag_resource(st: &mut State, p: &Value) -> Response {
    let arn = p["resourceArn"].as_str().unwrap_or_default().to_string();
    if find_resource(st, &arn).is_none() {
        return not_found(&arn);
    }
    let incoming = p["tags"].as_object().cloned().unwrap_or_default();
    if st.tags_store.iter().all(|(k, _)| *k != arn) {
        st.tags_store.push((arn.clone(), json!({})));
    }
    if let Some(existing) = stored_tags_mut(st, &arn).and_then(Value::as_object_mut) {
        for (k, v) in incoming {
            existing.insert(k, v);
        }
    }
    sync_tags(st, &arn);
    ok_empty(200)
}

fn untag_resource(st: &mut State, p: &Value) -> Response {
    let arn = p["resourceArn"].as_str().unwrap_or_default().to_string();
    if find_resource(st, &arn).is_none() {
        return not_found(&arn);
    }
    if stored_tags(st, &arn).is_some() {
        if let Some(keys) = p["tagKeys"].as_array() {
            if let Some(map) = stored_tags_mut(st, &arn).and_then(Value::as_object_mut) {
                for key in keys {
                    if let Some(k) = key.as_str() {
                        map.remove(k);
                    }
                }
            }
        }
        sync_tags(st, &arn);
    }
    ok_empty(200)
}

fn list_tags_for_resource(st: &State, p: &Value) -> Response {
    let arn = p["resourceArn"].as_str().unwrap_or_default();
    if find_resource(st, arn).is_none() {
        return not_found(arn);
    }
    let tags = stored_tags(st, arn).cloned().unwrap_or(json!({}));
    ok_json(&json!({"tags": tags}))
}

fn find_resource<'a>(st: &'a State, arn: &str) -> Option<&'a Value> {
    st.channels
        .iter()
        .find(|v| v["arn"].as_str() == Some(arn))
        .or_else(|| {
            st.stream_keys
                .iter()
                .find(|v| v["arn"].as_str() == Some(arn))
        })
        .or_else(|| {
            st.playback_key_pairs
                .iter()
                .find(|v| v["arn"].as_str() == Some(arn))
        })
        .or_else(|| {
            st.recording_configurations
                .iter()
                .find(|v| v["arn"].as_str() == Some(arn))
        })
        .or_else(|| {
            st.restriction_policies
                .iter()
                .find(|v| v["arn"].as_str() == Some(arn))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::HashMap;

    fn req(path: &str, body: Value) -> Request {
        Request {
            service: "ivs".into(),
            operation: String::new(),
            method: "POST".into(),
            path: path.into(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap_or_default()),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    fn body(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap_or(Value::Null)
    }

    fn scope() -> u64 {
        uuid::Uuid::new_v4().as_u128() as u64
    }

    fn make_channel(acct: u64) -> (Request, String) {
        let mut r = req("/CreateChannel", json!({"name": "ch"}));
        r.account = acct;
        let arn = body(&dispatch(&r).unwrap())["channel"]["arn"]
            .as_str()
            .unwrap()
            .to_string();
        (r, arn)
    }

    #[test]
    fn channel_lifecycle_with_stream_key() {
        let acct = scope();
        let (mut r, arn) = make_channel(acct);

        // create returns streamKey tied to the channel
        let mut create = req("/CreateChannel", json!({"name": "ch2"}));
        create.account = acct;
        let b = body(&dispatch(&create).unwrap());
        assert_eq!(
            b["streamKey"]["channelArn"].as_str().unwrap(),
            b["channel"]["arn"].as_str().unwrap()
        );
        assert!(b["channel"]["arn"]
            .as_str()
            .unwrap()
            .starts_with("arn:aws:ivs:us-east-1:"));

        r.method = "POST".into();
        r.path = "/GetChannel".into();
        r.body = Bytes::from(serde_json::to_vec(&json!({"arn": &arn})).unwrap());
        assert_eq!(
            body(&dispatch(&r).unwrap())["channel"]["arn"]
                .as_str()
                .unwrap(),
            arn.as_str()
        );

        r.path = "/ListChannels".into();
        let b = body(&dispatch(&r).unwrap());
        assert!(b["channels"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c["arn"] == arn.clone()));

        r.path = "/DeleteChannel".into();
        r.body = Bytes::from(serde_json::to_vec(&json!({"arn": &arn})).unwrap());
        assert_eq!(dispatch(&r).unwrap().status, 200);

        r.path = "/GetChannel".into();
        assert_eq!(dispatch(&r).unwrap().status, 404);
    }

    #[test]
    fn stream_keys_follow_channel() {
        let acct = scope();
        let mut r = req("/CreateChannel", json!({"name": "ch"}));
        r.account = acct;
        let ch_arn = body(&dispatch(&r).unwrap())["channel"]["arn"]
            .as_str()
            .unwrap()
            .to_string();

        r.path = "/CreateStreamKey".into();
        r.body = Bytes::from(serde_json::to_vec(&json!({"channelArn": &ch_arn})).unwrap());
        let sk_arn = body(&dispatch(&r).unwrap())["streamKey"]["arn"]
            .as_str()
            .unwrap()
            .to_string();

        r.path = "/ListStreamKeys".into();
        r.body = Bytes::from(serde_json::to_vec(&json!({"channelArn": &ch_arn})).unwrap());
        let b = body(&dispatch(&r).unwrap());
        assert!(!b["streamKeys"].as_array().unwrap().is_empty());
        assert!(b["streamKeys"]
            .as_array()
            .unwrap()
            .iter()
            .all(|sk| sk["channelArn"] == ch_arn.clone()));

        r.path = "/GetStreamKey".into();
        r.body = Bytes::from(serde_json::to_vec(&json!({"arn": &sk_arn})).unwrap());
        assert_eq!(
            body(&dispatch(&r).unwrap())["streamKey"]["arn"]
                .as_str()
                .unwrap(),
            sk_arn.as_str()
        );

        r.path = "/DeleteStreamKey".into();
        r.body = Bytes::from(serde_json::to_vec(&json!({"arn": &sk_arn})).unwrap());
        assert_eq!(dispatch(&r).unwrap().status, 200);
        r.path = "/GetStreamKey".into();
        r.body = Bytes::from(serde_json::to_vec(&json!({"arn": &sk_arn})).unwrap());
        assert_eq!(dispatch(&r).unwrap().status, 404);
    }

    #[test]
    fn recording_configs_and_key_pairs() {
        let acct = scope();
        let mut r = req(
            "/CreateRecordingConfiguration",
            json!({"destinationConfiguration": {"s3": {"bucketName": "bkt"}}}),
        );
        r.account = acct;
        let rc_arn = body(&dispatch(&r).unwrap())["recordingConfiguration"]["arn"]
            .as_str()
            .unwrap()
            .to_string();
        r.path = "/GetRecordingConfiguration".into();
        r.body = Bytes::from(serde_json::to_vec(&json!({"arn": &rc_arn})).unwrap());
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(b["recordingConfiguration"]["state"], "ACTIVE");
        assert_eq!(
            b["recordingConfiguration"]["destinationConfiguration"]["s3"]["bucketName"],
            "bkt"
        );

        // deleting twice: second time not found
        r.path = "/DeleteRecordingConfiguration".into();
        r.body = Bytes::from(serde_json::to_vec(&json!({"arn": &rc_arn})).unwrap());
        assert_eq!(dispatch(&r).unwrap().status, 200);
        assert_eq!(dispatch(&r).unwrap().status, 404);

        // key pair
        r.path = "/ImportPlaybackKeyPair".into();
        r.body = Bytes::from(serde_json::to_vec(&json!({"publicKeyMaterial": "PUB"})).unwrap());
        let resp = dispatch(&r).unwrap();
        let kp_arn = body(&resp)["keyPair"]["arn"].as_str().unwrap().to_string();
        assert!(!body(&resp)["keyPair"]["fingerprint"]
            .as_str()
            .unwrap()
            .is_empty());
        r.path = "/GetPlaybackKeyPair".into();
        r.body = Bytes::from(serde_json::to_vec(&json!({"arn": &kp_arn})).unwrap());
        assert_eq!(dispatch(&r).unwrap().status, 200);
        r.path = "/DeletePlaybackKeyPair".into();
        r.body = Bytes::from(serde_json::to_vec(&json!({"arn": &kp_arn})).unwrap());
        assert_eq!(dispatch(&r).unwrap().status, 200);
        r.path = "/GetPlaybackKeyPair".into();
        assert_eq!(dispatch(&r).unwrap().status, 404);
    }

    #[test]
    fn restriction_policy_lifecycle() {
        let acct = scope();
        let mut r = req(
            "/CreatePlaybackRestrictionPolicy",
            json!({"allowedCountries": ["US"], "name": "p"}),
        );
        r.account = acct;
        let arn = body(&dispatch(&r).unwrap())["playbackRestrictionPolicy"]["arn"]
            .as_str()
            .unwrap()
            .to_string();

        r.path = "/UpdatePlaybackRestrictionPolicy".into();
        r.body = Bytes::from(
            serde_json::to_vec(&json!({"arn": &arn, "allowedCountries": ["US", "DE"]})).unwrap(),
        );
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(
            b["playbackRestrictionPolicy"]["allowedCountries"]
                .as_array()
                .unwrap()
                .len(),
            2
        );

        r.path = "/DeletePlaybackRestrictionPolicy".into();
        r.body = Bytes::from(serde_json::to_vec(&json!({"arn": &arn})).unwrap());
        let resp = dispatch(&r).unwrap();
        assert_eq!(resp.status, 204);
        r.path = "/GetPlaybackRestrictionPolicy".into();
        assert_eq!(dispatch(&r).unwrap().status, 404);
    }

    #[test]
    fn tagging() {
        let acct = scope();
        let mut r = req(
            "/CreateChannel",
            json!({"name": "ch", "tags": {"env": "test"}}),
        );
        r.account = acct;
        let arn = body(&dispatch(&r).unwrap())["channel"]["arn"]
            .as_str()
            .unwrap()
            .to_string();

        r.path = "/ListTagsForResource".into();
        r.body = Bytes::from(serde_json::to_vec(&json!({"resourceArn": &arn})).unwrap());
        assert_eq!(body(&dispatch(&r).unwrap())["tags"]["env"], "test");

        r.path = "/TagResource".into();
        r.body = Bytes::from(
            serde_json::to_vec(&json!({"resourceArn": &arn, "tags": {"team": "dev"}})).unwrap(),
        );
        assert_eq!(dispatch(&r).unwrap().status, 200);

        r.path = "/ListTagsForResource".into();
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(b["tags"]["team"], "dev");
        assert_eq!(b["tags"]["env"], "test");

        r.path = "/UntagResource".into();
        r.body = Bytes::from(
            serde_json::to_vec(&json!({"resourceArn": &arn, "tagKeys": ["env"]})).unwrap(),
        );
        assert_eq!(dispatch(&r).unwrap().status, 200);

        r.path = "/ListTagsForResource".into();
        let b = body(&dispatch(&r).unwrap());
        assert!(b["tags"].get("env").is_none());
        assert_eq!(b["tags"]["team"], "dev");

        // unknown resource -> 404
        r.path = "/UntagResource".into();
        r.body = Bytes::from(
            serde_json::to_vec(
                &json!({"resourceArn": "arn:aws:ivs:us-east-1:1:channel/x", "tagKeys": []}),
            )
            .unwrap(),
        );
        assert_eq!(dispatch(&r).unwrap().status, 404);
    }

    #[test]
    fn session_and_viewer_ops() {
        let acct = scope();
        let mut r = req("/CreateChannel", json!({"name": "ch"}));
        r.account = acct;
        let arn = body(&dispatch(&r).unwrap())["channel"]["arn"]
            .as_str()
            .unwrap()
            .to_string();

        r.path = "/GetStreamSession".into();
        r.body = Bytes::from(serde_json::to_vec(&json!({"channelArn": &arn})).unwrap());
        let b = body(&dispatch(&r).unwrap());
        assert!(b["streamSession"]["streamId"].as_str().unwrap().len() == 12);

        r.path = "/ListStreamSessions".into();
        assert!(body(&dispatch(&r).unwrap())["streamSessions"]
            .as_array()
            .unwrap()
            .is_empty());

        r.path = "/StopStream".into();
        assert_eq!(dispatch(&r).unwrap().status, 200);

        r.path = "/BatchStartViewerSessionRevocation".into();
        r.body = Bytes::from(
            serde_json::to_vec(&json!({"viewerSessions": [
                {"channelArn": &arn, "viewerId": "v1"},
                {"channelArn": "arn:aws:ivs:us-east-1:1:channel/missing", "viewerId": "v2"}
            ]}))
            .unwrap(),
        );
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(b["errors"].as_array().unwrap().len(), 1);
        assert_eq!(b["errors"][0]["code"], "ResourceNotFoundException");
    }
}
