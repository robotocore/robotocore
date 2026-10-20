//! MediaPackageV2 backend (REST-JSON).
//!
//! Port of moto's `mediapackagev2`: channel groups → channels → origin
//! endpoints → harvest jobs, plus channel/endpoint policies, state resets
//! and tag round-trips. Wire shapes mirror moto's automatic serializer,
//! verified against the deployed moto fork (live probe capture):
//!
//! - timestamps serialize as unix seconds (integer when whole, float with
//!   millisecond precision otherwise) — moto's rest-json default
//!   (`TimestampSerializer::_timestamp_unixtimestamp`);
//! - per-shape quirk: tag echoes serialize under `tags` for `Update*Response`,
//!   `Get*GroupResponse` and `ListTagsForResourceResponse`, under `Tags`
//!   everywhere else (the botocore shapes' `locationName` decides);
//! - CreateChannel omits `InputType` (moto's `never_return` transformer),
//!   GetChannel/UpdateChannel/ListChannels include it;
//! - empty results answer 200 `{}`; TagResource/UntagResource answer 204
//!   with no body (the model's `responseCode`);
//! - every error is HTTP 400 with `{"__type": code, "message": msg}`;
//! - list pagination uses moto's `{"parameterChecksum", "uniqueAttributes"}`
//!   base64 token whose `uniqueAttributes` names the item *after* the page;
//! - `ListHarvestJobs` never paginates and ignores `includeStatus`: moto
//!   reads the `status` param, a key botocore's `includeStatus` locationName
//!   never sends, so only `channelName`/`originEndpointName` filter;
//! - deletes of missing channels/endpoints succeed silently (dict-pop), but
//!   a missing parent channel group still answers group-not-found.

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use chrono::Utc;
use once_cell::sync::Lazy;
use serde_json::{json, Map, Value};

use crate::proto::{json_error, parse_json_body, query_first, query_map, url_decode};
use crate::store;
use crate::{Request, Response};

const DEFAULT_PAGE_LIMIT: i64 = 10000;

#[derive(Default)]
struct State {
    /// moto's `TaggingService`: tags live one layer above resources, keyed
    /// by ARN, independent of the create-time `tags` echoed on records.
    tagger: std::collections::BTreeMap<String, std::collections::BTreeMap<String, String>>,
    groups: Vec<(String, Group)>,
}

#[derive(Default, Clone)]
struct Group {
    name: String,
    arn: String,
    egress_domain: String,
    created_ms: i64,
    modified_ms: i64,
    etag: String,
    description: String,
    tags: Option<Value>,
    channels: Vec<(String, Channel)>,
}

#[derive(Default, Clone)]
struct Channel {
    name: String,
    group_name: String,
    arn: String,
    input_type: String,
    created_ms: i64,
    modified_ms: i64,
    etag: String,
    description: String,
    ingest: Value,
    input_switch: Value,
    output_header: Value,
    tags: Option<Value>,
    policy: Option<String>,
    origin_endpoints: Vec<(String, OriginEndpoint)>,
}

#[derive(Default, Clone)]
struct OriginEndpoint {
    name: String,
    channel_name: String,
    group_name: String,
    arn: String,
    description: String,
    container_type: Option<Value>,
    segment: Value,
    startover: i64,
    hls_manifests: Value,
    low_latency_hls_manifests: Value,
    dash_manifests: Value,
    mss_manifests: Value,
    force_endpoint_error: Option<Value>,
    created_ms: i64,
    modified_ms: i64,
    etag: String,
    reset_ms: Option<i64>,
    tags: Option<Value>,
    policy: Option<String>,
    cdn_auth: Option<Value>,
    harvest_jobs: Vec<(String, HarvestJob)>,
}

#[derive(Default, Clone)]
struct HarvestJob {
    name: String,
    channel_name: String,
    group_name: String,
    origin_endpoint_name: String,
    arn: String,
    status: String,
    description: String,
    created_ms: i64,
    modified_ms: i64,
    etag: String,
    harvested_manifests: Option<Value>,
    schedule: Option<Value>,
    destination: Option<Value>,
    tags: Option<Value>,
    error_message: String,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("mediapackagev2"));
    SLOT.clone()
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

/// Body member: `Some(&v)` when the key exists and is not JSON null — moto's
/// `_get_param` yields None for both "absent" and explicit null.
fn present<'a>(p: &'a Value, key: &str) -> Option<&'a Value> {
    match p.get(key) {
        Some(Value::Null) | None => None,
        v => v,
    }
}

/// Body member defaulting like moto's `self._get_param(name, "")`.
fn body_str(p: &Value, key: &str, default: &str) -> String {
    present(p, key)
        .and_then(Value::as_str)
        .unwrap_or(default)
        .to_string()
}

/// Deterministic stand-in for moto's per-process `hash()` checksum inside
/// pagination tokens; it only needs to stay run-stable.
fn fnv(parts: &[&str]) -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    for part in parts {
        for b in part.as_bytes() {
            hash ^= u32::from(*b);
            hash = hash.wrapping_mul(0x0100_0193);
        }
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

fn hex_n(n: usize) -> String {
    let full = uuid::Uuid::new_v4().simple().to_string();
    full[..n.min(32)].to_string()
}

fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

/// Unix seconds exactly like moto's rest-json `unixtimestamp` formatter: an
/// integer when whole, else a float (renders as moto's `1792449120.21`).
fn ts(unix_ms: i64) -> Value {
    if unix_ms % 1000 == 0 {
        json!(unix_ms / 1000)
    } else {
        json!(unix_ms as f64 / 1000.0)
    }
}

fn make_token(checksum: u32, unique_attributes: &str) -> String {
    B64.encode(
        json!({"parameterChecksum": checksum, "uniqueAttributes": unique_attributes}).to_string(),
    )
}

fn bad_token() -> Response {
    json_error(400, "InvalidToken", "Invalid Token: Resource not found!")
}

/// moto's `paginate`: token names the item *after* the page (its unique
/// `arn`), base64 of `{"parameterChecksum": .., "uniqueAttributes": ..}`.
/// Token arity errors answer 400 `InvalidToken`.
fn paginate<T: Clone>(
    checksum: u32,
    items: &[T],
    token: Option<&str>,
    max_results: Option<i64>,
    unique_of: impl Fn(&T) -> &str,
) -> Result<(Vec<T>, Option<String>), Response> {
    let limit = max_results.unwrap_or(DEFAULT_PAGE_LIMIT).max(0) as usize;
    let mut start = 0usize;
    if let Some(tok) = token {
        let decoded = B64
            .decode(tok.as_bytes())
            .ok()
            .and_then(|raw| serde_json::from_slice::<Value>(&raw).ok())
            .and_then(|v| {
                v.get("uniqueAttributes")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            });
        let Some(unique) = decoded else {
            return Err(bad_token());
        };
        start = items
            .iter()
            .position(|i| unique_of(i) == unique)
            .ok_or_else(bad_token)?;
    }
    let end = start.saturating_add(limit).min(items.len());
    let page = items[start.min(items.len())..end].to_vec();
    let next = if !page.is_empty() && end < items.len() {
        Some(make_token(checksum, unique_of(&items[end])))
    } else {
        None
    };
    Ok((page, next))
}

fn page_params(query: &str) -> (Option<i64>, Option<String>) {
    let q = query_map(query);
    (
        query_first(&q, "maxResults").and_then(|v| v.parse().ok()),
        query_first(&q, "nextToken").map(str::to_string),
    )
}

fn ok_json(v: &Value) -> Response {
    Response::new(
        200,
        "application/json",
        serde_json::to_vec(v).unwrap_or_default(),
    )
}

fn list_json(items: Vec<Value>, next: Option<String>) -> Response {
    match next {
        Some(token) => ok_json(&json!({"Items": items, "NextToken": token})),
        None => ok_json(&json!({"Items": items})),
    }
}

/// Insertion-keeping replace (moto's dict assignment keeps its position).
fn upsert<V>(records: &mut Vec<(String, V)>, key: &str, val: V) {
    match records.iter_mut().find(|(k, _)| k == key) {
        Some(entry) => entry.1 = val,
        None => records.push((key.to_string(), val)),
    }
}

fn tag_map_of(value: &Value) -> std::collections::BTreeMap<String, String> {
    match value {
        Value::Object(m) => m
            .iter()
            .map(|(k, v)| (k.clone(), Value::as_str(v).unwrap_or("").to_string()))
            .collect(),
        _ => Default::default(),
    }
}

/// Seed create-time tags on both the record echo and moto's tagger (moto
/// only consults the tagger when `tags` is truthy).
fn register_tags(
    tagger: &mut std::collections::BTreeMap<String, std::collections::BTreeMap<String, String>>,
    input: Option<&Value>,
    record: &mut Option<Value>,
    arn: &str,
) {
    match input {
        Some(value) => {
            let tags = tag_map_of(value);
            if !tags.is_empty() {
                tagger.insert(arn.to_string(), tags.clone());
            }
            *record = Some(json!(tags));
        }
        _ => *record = Some(json!({})),
    }
}

// ---------------------------------------------------------------------------
// Error responses — exact moto messages (fork exceptions.py), HTTP 400.
// ---------------------------------------------------------------------------

const GROUP_NOT_FOUND: &str = "MediaPackage can't process your request because we can't find your channel group. Verify your channel group name or add a channel group and then try again";
const CHANNEL_NOT_FOUND: &str = "MediaPackage can't process your request because we can't find your channel. Verify your channel name or add a channel and then try again";
const ENDPOINT_NOT_FOUND: &str = "MediaPackage can't process your request because we can't find your origin endpoint. Verify your origin endpoint name or add an origin endpoint and then try again";
const HARVEST_NOT_FOUND: &str = "MediaPackage can't process your request because we can't find your harvest job. Verify your harvest job name and try again";
const CHANNEL_POLICY_NOT_FOUND: &str =
    "MediaPackage can't process your request because we can't find the policy for this channel";
const ENDPOINT_POLICY_NOT_FOUND: &str =
    "MediaPackage can't process your request because we can't find the policy for this origin endpoint";
const GROUP_NOT_EMPTY: &str = "The channel group you tried to delete has channels attached to it. If you want to delete this channel group, you must first delete the channels attached to it";

fn not_found(code: &str, msg: &str) -> Response {
    json_error(400, code, msg)
}

fn group_not_found() -> Response {
    not_found("ResourceNotFoundException", GROUP_NOT_FOUND)
}

fn channel_not_found() -> Response {
    not_found("ResourceNotFoundException", CHANNEL_NOT_FOUND)
}

fn endpoint_not_found() -> Response {
    not_found("ResourceNotFoundException", ENDPOINT_NOT_FOUND)
}

/// moto's `EmptyResult` — HTTP 200 with body `{}`.
fn empty_result() -> Response {
    Response::new(200, "application/json", "{}")
}

/// Tag ops answer HTTP 204 (botocore's `responseCode`) with no body — the
/// werkzeug layer suppresses the `{}` that the serializer would build.
fn no_content() -> Response {
    Response {
        status: 204,
        headers: vec![("Content-Type".to_string(), "application/json".to_string())],
        body: Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// Response builders — member order mirrors the botocore output shapes.
// ---------------------------------------------------------------------------

fn group_created(g: &Group) -> Value {
    json!({
        "ChannelGroupName": g.name,
        "Arn": g.arn,
        "EgressDomain": g.egress_domain,
        "CreatedAt": ts(g.created_ms),
        "ModifiedAt": ts(g.modified_ms),
        "ETag": g.etag,
        "Description": g.description,
        "Tags": g.tags.clone().unwrap_or_else(|| json!({})),
    })
}

fn group_fetched(g: &Group) -> Value {
    json!({
        "ChannelGroupName": g.name,
        "Arn": g.arn,
        "EgressDomain": g.egress_domain,
        "CreatedAt": ts(g.created_ms),
        "ModifiedAt": ts(g.modified_ms),
        "Description": g.description,
        "ETag": g.etag,
        "tags": g.tags.clone().unwrap_or_else(|| json!({})),
    })
}

fn group_listed(g: &Group) -> Value {
    json!({
        "ChannelGroupName": g.name,
        "Arn": g.arn,
        "CreatedAt": ts(g.created_ms),
        "ModifiedAt": ts(g.modified_ms),
        "Description": g.description,
    })
}

fn channel_created(c: &Channel) -> Value {
    // CreateChannel never returns InputType — moto's never_return transform.
    json!({
        "Arn": c.arn,
        "ChannelName": c.name,
        "ChannelGroupName": c.group_name,
        "CreatedAt": ts(c.created_ms),
        "ModifiedAt": ts(c.modified_ms),
        "Description": c.description,
        "IngestEndpoints": c.ingest,
        "ETag": c.etag,
        "Tags": c.tags.clone().unwrap_or_else(|| json!({})),
        "InputSwitchConfiguration": c.input_switch,
        "OutputHeaderConfiguration": c.output_header,
    })
}

fn channel_fetched(c: &Channel) -> Value {
    json!({
        "Arn": c.arn,
        "ChannelName": c.name,
        "ChannelGroupName": c.group_name,
        "CreatedAt": ts(c.created_ms),
        "ModifiedAt": ts(c.modified_ms),
        "Description": c.description,
        "IngestEndpoints": c.ingest,
        "InputType": c.input_type,
        "ETag": c.etag,
        "Tags": c.tags.clone().unwrap_or_else(|| json!({})),
        "InputSwitchConfiguration": c.input_switch,
        "OutputHeaderConfiguration": c.output_header,
    })
}

fn channel_updated(c: &Channel) -> Value {
    // UpdateChannelResponse is the channel shape serializing `tags`.
    json!({
        "Arn": c.arn,
        "ChannelName": c.name,
        "ChannelGroupName": c.group_name,
        "CreatedAt": ts(c.created_ms),
        "ModifiedAt": ts(c.modified_ms),
        "Description": c.description,
        "IngestEndpoints": c.ingest,
        "InputType": c.input_type,
        "ETag": c.etag,
        "tags": c.tags.clone().unwrap_or_else(|| json!({})),
        "InputSwitchConfiguration": c.input_switch,
        "OutputHeaderConfiguration": c.output_header,
    })
}

fn channel_listed(c: &Channel) -> Value {
    json!({
        "Arn": c.arn,
        "ChannelName": c.name,
        "ChannelGroupName": c.group_name,
        "CreatedAt": ts(c.created_ms),
        "ModifiedAt": ts(c.modified_ms),
        "Description": c.description,
        "InputType": c.input_type,
    })
}

fn endpoint_body(ep: &OriginEndpoint, tags_key: &str) -> Value {
    let mut m = Map::new();
    m.insert("Arn".into(), json!(ep.arn));
    m.insert("ChannelGroupName".into(), json!(ep.group_name));
    m.insert("ChannelName".into(), json!(ep.channel_name));
    m.insert("OriginEndpointName".into(), json!(ep.name));
    if let Some(ct) = &ep.container_type {
        m.insert("ContainerType".into(), ct.clone());
    }
    m.insert("Segment".into(), ep.segment.clone());
    m.insert("CreatedAt".into(), ts(ep.created_ms));
    m.insert("ModifiedAt".into(), ts(ep.modified_ms));
    if let Some(rms) = ep.reset_ms {
        m.insert("ResetAt".into(), ts(rms));
    }
    m.insert("Description".into(), json!(ep.description));
    m.insert("StartoverWindowSeconds".into(), json!(ep.startover));
    m.insert("HlsManifests".into(), ep.hls_manifests.clone());
    m.insert(
        "LowLatencyHlsManifests".into(),
        ep.low_latency_hls_manifests.clone(),
    );
    m.insert("DashManifests".into(), ep.dash_manifests.clone());
    m.insert("MssManifests".into(), ep.mss_manifests.clone());
    if let Some(fe) = &ep.force_endpoint_error {
        m.insert("ForceEndpointErrorConfiguration".into(), fe.clone());
    }
    m.insert("ETag".into(), json!(ep.etag));
    m.insert(
        tags_key.into(),
        ep.tags.clone().unwrap_or_else(|| json!({})),
    );
    Value::Object(m)
}

fn endpoint_listed(ep: &OriginEndpoint) -> Value {
    // List item shape: no Segment / StartoverWindowSeconds / ETag / Tags.
    let mut m = Map::new();
    m.insert("Arn".into(), json!(ep.arn));
    m.insert("ChannelGroupName".into(), json!(ep.group_name));
    m.insert("ChannelName".into(), json!(ep.channel_name));
    m.insert("OriginEndpointName".into(), json!(ep.name));
    if let Some(ct) = &ep.container_type {
        m.insert("ContainerType".into(), ct.clone());
    }
    m.insert("Description".into(), json!(ep.description));
    m.insert("CreatedAt".into(), ts(ep.created_ms));
    m.insert("ModifiedAt".into(), ts(ep.modified_ms));
    m.insert("HlsManifests".into(), ep.hls_manifests.clone());
    m.insert(
        "LowLatencyHlsManifests".into(),
        ep.low_latency_hls_manifests.clone(),
    );
    m.insert("DashManifests".into(), ep.dash_manifests.clone());
    m.insert("MssManifests".into(), ep.mss_manifests.clone());
    if let Some(fe) = &ep.force_endpoint_error {
        m.insert("ForceEndpointErrorConfiguration".into(), fe.clone());
    }
    Value::Object(m)
}

fn harvest_full(j: &HarvestJob) -> Value {
    let mut m = Map::new();
    m.insert("ChannelGroupName".into(), json!(j.group_name));
    m.insert("ChannelName".into(), json!(j.channel_name));
    m.insert("OriginEndpointName".into(), json!(j.origin_endpoint_name));
    if let Some(d) = &j.destination {
        m.insert("Destination".into(), d.clone());
    }
    m.insert("HarvestJobName".into(), json!(j.name));
    if let Some(hm) = &j.harvested_manifests {
        m.insert("HarvestedManifests".into(), hm.clone());
    }
    m.insert("Description".into(), json!(j.description));
    if let Some(sc) = &j.schedule {
        m.insert("ScheduleConfiguration".into(), normalize_schedule(sc));
    }
    m.insert("Arn".into(), json!(j.arn));
    m.insert("CreatedAt".into(), ts(j.created_ms));
    m.insert("ModifiedAt".into(), ts(j.modified_ms));
    m.insert("Status".into(), json!(j.status));
    m.insert("ErrorMessage".into(), json!(j.error_message));
    m.insert("ETag".into(), json!(j.etag));
    m.insert("Tags".into(), j.tags.clone().unwrap_or_else(|| json!({})));
    Value::Object(m)
}

fn harvest_listed(j: &HarvestJob) -> Value {
    // List items ride the bare `HarvestJob` shape: no Tags member.
    let mut m = Map::new();
    m.insert("ChannelGroupName".into(), json!(j.group_name));
    m.insert("ChannelName".into(), json!(j.channel_name));
    m.insert("OriginEndpointName".into(), json!(j.origin_endpoint_name));
    if let Some(d) = &j.destination {
        m.insert("Destination".into(), d.clone());
    }
    m.insert("HarvestJobName".into(), json!(j.name));
    if let Some(hm) = &j.harvested_manifests {
        m.insert("HarvestedManifests".into(), hm.clone());
    }
    m.insert("Description".into(), json!(j.description));
    if let Some(sc) = &j.schedule {
        m.insert("ScheduleConfiguration".into(), normalize_schedule(sc));
    }
    m.insert("Arn".into(), json!(j.arn));
    m.insert("CreatedAt".into(), ts(j.created_ms));
    m.insert("ModifiedAt".into(), ts(j.modified_ms));
    m.insert("Status".into(), json!(j.status));
    m.insert("ErrorMessage".into(), json!(j.error_message));
    m.insert("ETag".into(), json!(j.etag));
    Value::Object(m)
}

/// `ScheduleConfiguration` members are botocore Timestamps: moto re-parses
/// the stored raw values at serialization time into unix seconds.
fn normalize_schedule(schedule: &Value) -> Value {
    match schedule {
        Value::Object(m) => {
            let mut out = Map::new();
            for (key, value) in m {
                match key.as_str() {
                    "StartTime" | "EndTime" => {
                        out.insert(key.clone(), parse_ts_value(value));
                    }
                    other => {
                        out.insert(other.to_string(), value.clone());
                    }
                };
            }
            Value::Object(out)
        }
        other => other.clone(),
    }
}

/// Parse a botocore timestamp into unix seconds; unparsable or numeric
/// values pass through unchanged.
fn parse_ts_value(v: &Value) -> Value {
    match v {
        Value::String(s) => match chrono::DateTime::parse_from_rfc3339(s) {
            Ok(t) => ts(t.timestamp_millis()),
            _ => v.clone(),
        },
        other => other.clone(),
    }
}

// ---------------------------------------------------------------------------
// Handlers — channel groups
// ---------------------------------------------------------------------------

fn upsert_state_group(st: &mut State, group: Group) {
    let name = group.name.clone();
    upsert(&mut st.groups, &name, group);
}

fn group_or_404(st: &State, name: &str) -> Result<Group, Response> {
    st.groups
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, g)| g.clone())
        .ok_or(group_not_found())
}

fn create_channel_group(
    region: &str,
    account: u64,
    body: &Value,
    st: &mut State,
) -> Result<Response, Response> {
    let name = body_str(body, "ChannelGroupName", "");
    let description = body_str(body, "Description", "");
    let arn = format!("arn:aws:mediapackagev2:{region}:{account}:channelGroup/{name}");
    // moto: two random 6-hex words `X.egress.Y.mediapackagev2.<region>...`
    let hex12 = hex_n(12);
    let egress_domain = format!(
        "{}.egress.{}.mediapackagev2.{region}.amazonaws.com",
        &hex12[..6],
        &hex12[6..],
    );
    let mut group = Group {
        name: name.clone(),
        arn: arn.clone(),
        egress_domain,
        created_ms: now_ms(),
        modified_ms: now_ms(),
        etag: hex_n(10),
        description,
        ..Default::default()
    };
    register_tags(&mut st.tagger, present(body, "tags"), &mut group.tags, &arn);
    upsert(&mut st.groups, &name, group.clone());
    Ok(ok_json(&group_created(&group)))
}

fn update_channel_group(st: &mut State, name: &str, body: &Value) -> Result<Response, Response> {
    let mut group = group_or_404(st, name)?;
    if let Some(d) = present(body, "Description").and_then(Value::as_str) {
        group.description = d.to_string();
    }
    group.modified_ms = now_ms();
    group.etag = hex_n(10);
    upsert_state_group(st, group.clone());
    Ok(ok_json(&group_fetched(&group)))
}

fn get_channel_group(st: &State, name: &str) -> Result<Response, Response> {
    let g = group_or_404(st, name)?;
    Ok(ok_json(&group_fetched(&g)))
}

/// moto: only a group with channels raises; deletes of missing groups pop
/// nothing and answer 200.
fn delete_channel_group(st: &mut State, name: &str) -> Result<Response, Response> {
    if let Some((_, g)) = st.groups.iter().find(|(k, _)| k == name) {
        if !g.channels.is_empty() {
            return Err(not_found("ConflictException", GROUP_NOT_EMPTY));
        }
    }
    st.groups.retain(|(k, _)| k != name);
    Ok(empty_result())
}

fn list_channel_groups(st: &State, query: &str) -> Result<Response, Response> {
    let (max, token) = page_params(query);
    let (page, next) = paginate(
        fnv(&["channelGroups"]),
        &st.groups,
        token.as_deref(),
        max,
        |(_, g)| g.arn.as_str(),
    )?;
    let items: Vec<Value> = page.iter().map(|(_, g)| group_listed(g)).collect();
    Ok(list_json(items, next))
}

// ---------------------------------------------------------------------------
// Handlers — channels
// ---------------------------------------------------------------------------

fn ingest_endpoints(egress_domain: &str, cg: &str, ch: &str) -> Value {
    let ingest1 = egress_domain.replace(".egress", "-1.ingest");
    let ingest2 = egress_domain.replace(".egress", "-2.ingest");
    json!([
        {"Id": "1", "Url": format!("https://{ingest1}/in/v1/{cg}/1/{ch}/index")},
        {"Id": "2", "Url": format!("https://{ingest2}/in/v1/{cg}/2/{ch}/index")}
    ])
}

fn create_channel(cg: &str, body: &Value, st: &mut State) -> Result<Response, Response> {
    let mut group = group_or_404(st, cg)?;
    let name = body_str(body, "ChannelName", "");
    let description = body_str(body, "Description", "");
    let input_type = present(body, "InputType")
        .and_then(Value::as_str)
        .unwrap_or("HLS")
        .to_string();
    let arn = format!("{}/channel/{}", group.arn, name);
    let now = now_ms();
    let mut channel = Channel {
        name: name.clone(),
        group_name: cg.to_string(),
        arn: arn.clone(),
        input_type,
        created_ms: now,
        modified_ms: now,
        etag: hex_n(10),
        description,
        ingest: ingest_endpoints(&group.egress_domain, cg, &name),
        input_switch: json!({"MQCSInputSwitching": false}),
        output_header: json!({"PublishMQCS": false}),
        ..Default::default()
    };
    register_tags(
        &mut st.tagger,
        present(body, "tags"),
        &mut channel.tags,
        &arn,
    );
    upsert(&mut group.channels, &name, channel.clone());
    upsert_state_group(st, group);
    Ok(ok_json(&channel_created(&channel)))
}

fn channel_of<'a>(group: &'a Group, ch: &str) -> Option<&'a Channel> {
    group.channels.iter().find(|(k, _)| k == ch).map(|(_, c)| c)
}

fn channel_of_mut<'a>(group: &'a mut Group, ch: &str) -> Option<&'a mut Channel> {
    group
        .channels
        .iter_mut()
        .find(|(k, _)| k == ch)
        .map(|(_, c)| c)
}

fn take_channel(group: &Group, ch: &str) -> Result<Channel, Response> {
    channel_of(group, ch).cloned().ok_or(channel_not_found())
}

fn list_channels(st: &State, cg: &str, query: &str) -> Result<Response, Response> {
    let group = group_or_404(st, cg)?;
    let items_src: Vec<Value> = group
        .channels
        .iter()
        .map(|(_, c)| channel_listed(c))
        .collect();
    let (max, token) = page_params(query);
    let (page, next) = paginate(
        fnv(&["channels", cg]),
        &items_src,
        token.as_deref(),
        max,
        |v| v.get("Arn").and_then(Value::as_str).unwrap_or_default(),
    )?;
    Ok(list_json(page, next))
}

fn get_channel(st: &State, cg: &str, ch: &str) -> Result<Response, Response> {
    let group = group_or_404(st, cg)?;
    let channel = channel_of(&group, ch).ok_or_else(channel_not_found)?;
    Ok(ok_json(&channel_fetched(channel)))
}

fn update_channel(st: &mut State, cg: &str, ch: &str, body: &Value) -> Result<Response, Response> {
    let mut group = group_or_404(st, cg)?;
    if !group.channels.iter().any(|(k, _)| k == ch) {
        return Err(channel_not_found());
    }
    {
        let channel = channel_of_mut(&mut group, ch).expect("checked above");
        if let Some(d) = present(body, "Description").and_then(Value::as_str) {
            channel.description = d.to_string();
        }
        if let Some(v) = present(body, "InputSwitchConfiguration") {
            channel.input_switch = v.clone();
        }
        if let Some(v) = present(body, "OutputHeaderConfiguration") {
            channel.output_header = v.clone();
        }
        channel.modified_ms = now_ms();
        channel.etag = hex_n(10);
    }
    let updated = take_channel(&group, ch)?;
    upsert_state_group(st, group);
    Ok(ok_json(&channel_updated(&updated)))
}

fn delete_channel(st: &mut State, cg: &str, ch: &str) -> Result<Response, Response> {
    let mut group = group_or_404(st, cg)?;
    // moto pops the key blindly: deleting a missing channel still 200s.
    group.channels.retain(|(k, _)| k != ch);
    upsert_state_group(st, group);
    Ok(empty_result())
}

fn put_channel_policy(
    st: &mut State,
    cg: &str,
    ch: &str,
    body: &Value,
) -> Result<Response, Response> {
    let mut group = group_or_404(st, cg)?;
    let channel = channel_of_mut(&mut group, ch).ok_or_else(channel_not_found)?;
    channel.policy = present(body, "Policy")
        .and_then(Value::as_str)
        .map(str::to_string);
    upsert_state_group(st, group);
    Ok(empty_result())
}

fn get_channel_policy(st: &State, cg: &str, ch: &str) -> Result<Response, Response> {
    let group = group_or_404(st, cg)?;
    let channel = channel_of(&group, ch).ok_or_else(channel_not_found)?;
    let policy = channel
        .policy
        .as_deref()
        .ok_or_else(|| not_found("ResourceNotFoundException", CHANNEL_POLICY_NOT_FOUND))?;
    Ok(ok_json(&json!({
        "ChannelGroupName": cg,
        "ChannelName": ch,
        "Policy": policy,
    })))
}

fn delete_channel_policy(st: &mut State, cg: &str, ch: &str) -> Result<Response, Response> {
    let mut group = group_or_404(st, cg)?;
    let channel = channel_of_mut(&mut group, ch).ok_or_else(channel_not_found)?;
    channel.policy = None;
    upsert_state_group(st, group);
    Ok(empty_result())
}

fn reset_channel_state(st: &State, cg: &str, ch: &str) -> Result<Response, Response> {
    let group = group_or_404(st, cg)?;
    let channel = channel_of(&group, ch).ok_or_else(channel_not_found)?;
    Ok(ok_json(&json!({
        "ChannelGroupName": cg,
        "ChannelName": ch,
        "Arn": channel.arn,
        "ResetAt": ts(now_ms()),
    })))
}

// ---------------------------------------------------------------------------
// Handlers — origin endpoints
// ---------------------------------------------------------------------------

fn endpoint_of<'a>(channel: &'a Channel, ep: &str) -> Option<&'a OriginEndpoint> {
    channel
        .origin_endpoints
        .iter()
        .find(|(k, _)| k == ep)
        .map(|(_, e)| e)
}

fn endpoint_of_mut<'a>(channel: &'a mut Channel, ep: &str) -> Option<&'a mut OriginEndpoint> {
    channel
        .origin_endpoints
        .iter_mut()
        .find(|(k, _)| k == ep)
        .map(|(_, e)| e)
}

fn channel_group_or_404(st: &mut State, cg: &str, ch: &str) -> Result<(Group, Channel), Response> {
    let group = group_or_404(st, cg)?;
    let channel = take_channel(&group, ch)?;
    Ok((group, channel))
}

fn create_origin_endpoint(
    cg: &str,
    ch: &str,
    body: &Value,
    st: &mut State,
) -> Result<Response, Response> {
    let (mut group, mut channel) = channel_group_or_404(st, cg, ch)?;
    let name = body_str(body, "OriginEndpointName", "");
    let arn = format!("{}/originEndpoint/{}", channel.arn, name);
    let now = now_ms();
    let mut ep = OriginEndpoint {
        name: name.clone(),
        channel_name: ch.to_string(),
        group_name: cg.to_string(),
        arn: arn.clone(),
        description: body_str(body, "Description", ""),
        container_type: present(body, "ContainerType").cloned(),
        segment: present(body, "Segment")
            .cloned()
            .unwrap_or_else(|| json!({})),
        startover: present(body, "StartoverWindowSeconds")
            .and_then(Value::as_i64)
            .unwrap_or(0),
        hls_manifests: present(body, "HlsManifests")
            .cloned()
            .unwrap_or_else(|| json!([])),
        low_latency_hls_manifests: present(body, "LowLatencyHlsManifests")
            .cloned()
            .unwrap_or_else(|| json!([])),
        dash_manifests: present(body, "DashManifests")
            .cloned()
            .unwrap_or_else(|| json!([])),
        mss_manifests: present(body, "MssManifests")
            .cloned()
            .unwrap_or_else(|| json!([])),
        force_endpoint_error: present(body, "ForceEndpointErrorConfiguration").cloned(),
        created_ms: now,
        modified_ms: now,
        etag: hex_n(10),
        ..Default::default()
    };
    register_tags(&mut st.tagger, present(body, "tags"), &mut ep.tags, &arn);
    upsert(&mut channel.origin_endpoints, &name, ep.clone());
    upsert(&mut group.channels, ch, channel);
    upsert_state_group(st, group);
    Ok(ok_json(&endpoint_body(&ep, "Tags")))
}

fn endpoint_or_404(
    st: &mut State,
    cg: &str,
    ch: &str,
    ep: &str,
) -> Result<(Group, Channel, OriginEndpoint), Response> {
    let (group, channel) = channel_group_or_404(st, cg, ch)?;
    let endpoint = endpoint_of(&channel, ep)
        .cloned()
        .ok_or_else(endpoint_not_found)?;
    Ok((group, channel, endpoint))
}

fn get_origin_endpoint(st: &mut State, cg: &str, ch: &str, ep: &str) -> Result<Response, Response> {
    let endpoint = endpoint_or_404(st, cg, ch, ep)?.2;
    Ok(ok_json(&endpoint_body(&endpoint, "Tags")))
}

fn update_origin_endpoint(
    st: &mut State,
    cg: &str,
    ch: &str,
    ep_name: &str,
    body: &Value,
) -> Result<Response, Response> {
    let (mut group, mut channel, mut endpoint) = endpoint_or_404(st, cg, ch, ep_name)?;
    if let Some(v) = present(body, "ContainerType") {
        endpoint.container_type = Some(v.clone());
    }
    if let Some(v) = present(body, "Segment") {
        endpoint.segment = v.clone();
    }
    if let Some(d) = present(body, "Description").and_then(Value::as_str) {
        endpoint.description = d.to_string();
    }
    if let Some(v) = present(body, "StartoverWindowSeconds").and_then(Value::as_i64) {
        endpoint.startover = v;
    }
    for (member, field) in [
        ("HlsManifests", "hls"),
        ("LowLatencyHlsManifests", "low_latency_hls"),
        ("DashManifests", "dash"),
        ("MssManifests", "mss"),
    ] {
        if let Some(v) = present(body, member) {
            match field {
                "hls" => endpoint.hls_manifests = v.clone(),
                "low_latency_hls" => endpoint.low_latency_hls_manifests = v.clone(),
                "dash" => endpoint.dash_manifests = v.clone(),
                _ => endpoint.mss_manifests = v.clone(),
            }
        }
    }
    if let Some(v) = present(body, "ForceEndpointErrorConfiguration") {
        endpoint.force_endpoint_error = Some(v.clone());
    }
    endpoint.modified_ms = now_ms();
    endpoint.etag = hex_n(10);
    let out = endpoint.clone();
    upsert(&mut channel.origin_endpoints, ep_name, endpoint);
    upsert(&mut group.channels, ch, channel);
    upsert_state_group(st, group);
    // UpdateOriginEndpointResponse serializes tags as lowercase `tags`.
    Ok(ok_json(&endpoint_body(&out, "tags")))
}

fn delete_origin_endpoint(
    st: &mut State,
    cg: &str,
    ch: &str,
    ep: &str,
) -> Result<Response, Response> {
    let (mut group, mut channel) = channel_group_or_404(st, cg, ch)?;
    // moto pops the key blindly: deleting a missing endpoint still 200s.
    channel.origin_endpoints.retain(|(k, _)| k != ep);
    upsert(&mut group.channels, ch, channel);
    upsert_state_group(st, group);
    Ok(empty_result())
}

fn list_origin_endpoints(
    st: &mut State,
    cg: &str,
    ch: &str,
    query: &str,
) -> Result<Response, Response> {
    let (_group, channel) = channel_group_or_404(st, cg, ch)?;
    let items_src: Vec<Value> = channel
        .origin_endpoints
        .iter()
        .map(|(_, e)| endpoint_listed(e))
        .collect();
    let (max, token) = page_params(query);
    let (page, next) = paginate(
        fnv(&["originEndpoints", cg, ch]),
        &items_src,
        token.as_deref(),
        max,
        |v| v.get("Arn").and_then(Value::as_str).unwrap_or_default(),
    )?;
    Ok(list_json(page, next))
}

fn put_origin_endpoint_policy(
    st: &mut State,
    cg: &str,
    ch: &str,
    ep_name: &str,
    body: &Value,
) -> Result<Response, Response> {
    let (mut group, mut channel) = channel_group_or_404(st, cg, ch)?;
    let ep = endpoint_of_mut(&mut channel, ep_name).ok_or_else(endpoint_not_found)?;
    ep.policy = present(body, "Policy")
        .and_then(Value::as_str)
        .map(str::to_string);
    if let Some(v) = present(body, "CdnAuthConfiguration") {
        ep.cdn_auth = Some(v.clone());
    }
    upsert(&mut group.channels, ch, channel);
    upsert_state_group(st, group);
    Ok(empty_result())
}

fn get_origin_endpoint_policy(
    st: &mut State,
    cg: &str,
    ch: &str,
    ep_name: &str,
) -> Result<Response, Response> {
    let endpoint = endpoint_or_404(st, cg, ch, ep_name)?.2;
    let policy = endpoint
        .policy
        .as_deref()
        .ok_or_else(|| not_found("ResourceNotFoundException", ENDPOINT_POLICY_NOT_FOUND))?;
    let mut out = json!({
        "ChannelGroupName": cg,
        "ChannelName": ch,
        "OriginEndpointName": ep_name,
        "Policy": policy,
    });
    if let Some(cdn) = &endpoint.cdn_auth {
        out["CdnAuthConfiguration"] = cdn.clone();
    }
    Ok(ok_json(&out))
}

fn delete_origin_endpoint_policy(
    st: &mut State,
    cg: &str,
    ch: &str,
    ep_name: &str,
) -> Result<Response, Response> {
    let (mut group, mut channel, mut endpoint) = endpoint_or_404(st, cg, ch, ep_name)?;
    endpoint.policy = None;
    endpoint.cdn_auth = None;
    upsert(&mut channel.origin_endpoints, ep_name, endpoint);
    upsert(&mut group.channels, ch, channel);
    upsert_state_group(st, group);
    Ok(empty_result())
}

fn reset_origin_endpoint_state(
    st: &mut State,
    cg: &str,
    ch: &str,
    ep_name: &str,
) -> Result<Response, Response> {
    let (mut group, mut channel, mut endpoint) = endpoint_or_404(st, cg, ch, ep_name)?;
    let now = now_ms();
    endpoint.reset_ms = Some(now);
    let arn = endpoint.arn.clone();
    upsert(&mut channel.origin_endpoints, ep_name, endpoint);
    upsert(&mut group.channels, ch, channel);
    upsert_state_group(st, group);
    Ok(ok_json(&json!({
        "ChannelGroupName": cg,
        "ChannelName": ch,
        "OriginEndpointName": ep_name,
        "Arn": arn,
        "ResetAt": ts(now),
    })))
}

// ---------------------------------------------------------------------------
// Handlers — harvest jobs
// ---------------------------------------------------------------------------

/// `ScheduleConfiguration` timestamps reach moto's model as-is and are
/// unix-normalized at serialization; store the raw body value.
fn create_harvest_job(
    cg: &str,
    ch: &str,
    ep_name: &str,
    body: &Value,
    st: &mut State,
) -> Result<Response, Response> {
    let (mut group, mut channel) = channel_group_or_404(st, cg, ch)?;
    let position = channel
        .origin_endpoints
        .iter()
        .position(|(k, _)| k == ep_name)
        .ok_or_else(endpoint_not_found)?;
    let endpoint = &channel.origin_endpoints[position].1;
    let name = present(body, "HarvestJobName")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| hex_n(12));
    let arn = format!("{}/harvestJob/{}", endpoint.arn, name);
    let now = now_ms();
    let mut job = HarvestJob {
        name: name.clone(),
        channel_name: ch.to_string(),
        group_name: cg.to_string(),
        origin_endpoint_name: ep_name.to_string(),
        arn: arn.clone(),
        status: "QUEUED".to_string(),
        description: body_str(body, "Description", ""),
        created_ms: now,
        modified_ms: now,
        etag: hex_n(10),
        harvested_manifests: present(body, "HarvestedManifests").cloned(),
        schedule: present(body, "ScheduleConfiguration").cloned(),
        destination: present(body, "Destination").cloned(),
        ..Default::default()
    };
    let job_arn = job.arn.clone();
    register_tags(
        &mut st.tagger,
        present(body, "tags"),
        &mut job.tags,
        &job_arn,
    );
    let mut endpoint_for_upsert = channel.origin_endpoints[position].1.clone();
    upsert(&mut endpoint_for_upsert.harvest_jobs, &name, job.clone());
    channel.origin_endpoints[position] = (ep_name.to_string(), endpoint_for_upsert);
    upsert(&mut group.channels, ch, channel);
    upsert_state_group(st, group);
    Ok(ok_json(&harvest_full(&job)))
}

fn harvest_of<'a>(ep: &'a OriginEndpoint, hj: &str) -> Option<&'a HarvestJob> {
    ep.harvest_jobs
        .iter()
        .find(|(k, _)| k == hj)
        .map(|(_, j)| j)
}

fn harvest_or_404(
    st: &mut State,
    cg: &str,
    ch: &str,
    ep_name: &str,
    hj: &str,
) -> Result<(Group, Channel, OriginEndpoint), Response> {
    let (group, channel) = channel_group_or_404(st, cg, ch)?;
    let endpoint = endpoint_of(&channel, ep_name)
        .cloned()
        .ok_or_else(endpoint_not_found)?;
    if harvest_of(&endpoint, hj).is_none() {
        return Err(harvest_not_found());
    }
    Ok((group, channel, endpoint))
}

fn harvest_not_found() -> Response {
    not_found("ResourceNotFoundException", HARVEST_NOT_FOUND)
}

fn get_harvest_job(
    st: &mut State,
    cg: &str,
    ch: &str,
    ep_name: &str,
    hj: &str,
) -> Result<Response, Response> {
    let (_, _, ep) = harvest_or_404(st, cg, ch, ep_name, hj)?;
    let job = harvest_of(&ep, hj).cloned().unwrap_or_default();
    Ok(ok_json(&harvest_full(&job)))
}

fn cancel_harvest_job(
    st: &mut State,
    cg: &str,
    ch: &str,
    ep_name: &str,
    hj: &str,
) -> Result<Response, Response> {
    let (mut group, mut channel, mut endpoint) = harvest_or_404(st, cg, ch, ep_name, hj)?;
    let job = harvest_of(&endpoint, hj).cloned().unwrap_or_default();
    let cancelled = HarvestJob {
        status: "CANCELLED".to_string(),
        modified_ms: now_ms(),
        ..job
    };
    upsert(&mut endpoint.harvest_jobs, hj, cancelled);
    upsert(&mut channel.origin_endpoints, ep_name, endpoint);
    upsert(&mut group.channels, ch, channel);
    upsert_state_group(st, group);
    Ok(empty_result())
}

fn list_harvest_jobs(st: &State, cg: &str, query: &str) -> Result<Response, Response> {
    let group = group_or_404(st, cg)?;
    let q = query_map(query);
    let channel_filter = query_first(&q, "channelName");
    let endpoint_filter = query_first(&q, "originEndpointName");
    // moto reads `status` here — but botocore serializes the Status input as
    // `includeStatus`, so the filter is dead code upstream; replicate by not
    // filtering at all.
    let mut items: Vec<Value> = Vec::new();
    for (_, channel) in &group.channels {
        if let Some(cn) = channel_filter {
            if channel.name != cn {
                continue;
            }
        }
        for (_, ep) in &channel.origin_endpoints {
            if let Some(en) = endpoint_filter {
                if ep.name != en {
                    continue;
                }
            }
            for (_, job) in &ep.harvest_jobs {
                items.push(harvest_listed(job));
            }
        }
    }
    Ok(ok_json(&json!({"Items": items})))
}

// ---------------------------------------------------------------------------
// Tags
// ---------------------------------------------------------------------------

fn list_tags(st: &State, arn: &str) -> Response {
    let tags = st.tagger.get(arn).cloned().unwrap_or_default();
    ok_json(&json!({"tags": tags}))
}

fn tag_resource(st: &mut State, arn: &str, body: &Value) -> Response {
    if let Some(tags) = present(body, "tags") {
        st.tagger
            .entry(arn.to_string())
            .or_default()
            .extend(tag_map_of(tags));
    }
    no_content()
}

fn untag_resource(st: &mut State, arn: &str, query: &str) -> Response {
    let keys = query_map(query).get("tagKeys").cloned().unwrap_or_default();
    if (!keys.is_empty()) && !arn.is_empty() {
        if let Some(entry) = st.tagger.get_mut(arn) {
            for key in keys {
                entry.remove(&key);
            }
        }
    }
    no_content()
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "mediapackagev2" {
        return None;
    }
    let slot = state();
    let mut map = slot.write();
    // /tags/{ResourceArn}: the ARN may contain slashes; the percent-decoded
    // raw tail is the ARN, like moto's `_get_uri_param`.
    if let Some(rest) = req.path.strip_prefix("/tags/") {
        let arn = url_decode(rest);
        let st = map.entry((req.account, req.region.clone())).or_default();
        return match req.method.as_str() {
            "GET" => Some(list_tags(st, &arn)),
            "POST" => Some(tag_resource(st, &arn, &parse_json_body(&req.body))),
            "DELETE" => Some(untag_resource(st, &arn, &req.query_string)),
            _ => None,
        };
    }
    let path = req.path.trim_end_matches('/');
    let parts: Vec<&str> = path.split('/').skip(1).collect();
    let method = req.method.as_str();
    let body = parse_json_body(&req.body);
    let st = map.entry((req.account, req.region.clone())).or_default();
    let route = match parts.as_slice() {
        ["channelGroup"] => match method {
            "POST" => create_channel_group(&req.region, req.account, &body, st),
            "GET" => list_channel_groups(st, &req.query_string),
            _ => return None,
        },
        ["channelGroup", cg] => match method {
            "GET" => get_channel_group(st, cg),
            "PUT" => update_channel_group(st, cg, &body),
            "DELETE" => delete_channel_group(st, cg),
            _ => return None,
        },
        ["channelGroup", cg, "channel"] => match method {
            "POST" => create_channel(cg, &body, st),
            "GET" => list_channels(st, cg, &req.query_string),
            _ => return None,
        },
        ["channelGroup", cg, "channel", ch] => match method {
            "GET" => get_channel(st, cg, ch),
            "PUT" => update_channel(st, cg, ch, &body),
            "DELETE" => delete_channel(st, cg, ch),
            _ => return None,
        },
        ["channelGroup", cg, "channel", ch, "policy"] => match method {
            "GET" => get_channel_policy(st, cg, ch),
            "PUT" => put_channel_policy(st, cg, ch, &body),
            "DELETE" => delete_channel_policy(st, cg, ch),
            _ => return None,
        },
        ["channelGroup", cg, "channel", ch, "reset"] => match method {
            "POST" => reset_channel_state(st, cg, ch),
            _ => return None,
        },
        ["channelGroup", cg, "channel", ch, "originEndpoint"] => match method {
            "POST" => create_origin_endpoint(cg, ch, &body, st),
            "GET" => list_origin_endpoints(st, cg, ch, &req.query_string),
            _ => return None,
        },
        ["channelGroup", cg, "channel", ch, "originEndpoint", ep] => match method {
            "GET" => get_origin_endpoint(st, cg, ch, ep),
            "PUT" => update_origin_endpoint(st, cg, ch, ep, &body),
            "DELETE" => delete_origin_endpoint(st, cg, ch, ep),
            _ => return None,
        },
        ["channelGroup", cg, "channel", ch, "originEndpoint", ep, "policy"] => match method {
            "GET" => get_origin_endpoint_policy(st, cg, ch, ep),
            "POST" => put_origin_endpoint_policy(st, cg, ch, ep, &body),
            "DELETE" => delete_origin_endpoint_policy(st, cg, ch, ep),
            _ => return None,
        },
        ["channelGroup", cg, "channel", ch, "originEndpoint", ep, "reset"] => match method {
            "POST" => reset_origin_endpoint_state(st, cg, ch, ep),
            _ => return None,
        },
        ["channelGroup", cg, "channel", ch, "originEndpoint", ep, "harvestJob"] => match method {
            "POST" => create_harvest_job(cg, ch, ep, &body, st),
            _ => return None,
        },
        ["channelGroup", cg, "channel", ch, "originEndpoint", ep, "harvestJob", hj] => match method
        {
            "GET" => get_harvest_job(st, cg, ch, ep, hj),
            "PUT" => cancel_harvest_job(st, cg, ch, ep, hj),
            _ => return None,
        },
        ["channelGroup", cg, "harvestJob"] => match method {
            "GET" => list_harvest_jobs(st, cg, &req.query_string),
            _ => return None,
        },
        _ => return None,
    };
    // Handled errors are already wire-shaped: every handler answers moto's
    // HTTP 400 `{"__type": code, "message": msg}` payloads.
    Some(match route {
        Ok(resp) => resp,
        Err(resp) => resp,
    })
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn req(method: &str, path: &str, body: Value) -> Request {
        Request {
            service: "mediapackagev2".into(),
            operation: String::new(),
            method: method.into(),
            path: path.into(),
            query_string: String::new(),
            headers: std::collections::HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap_or_default()),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    fn req_at(method: &str, path: &str, body: Value, acct: u64) -> Request {
        let mut r = req(method, path, body);
        // Query strings live in their own request field at the wire.
        if let Some((p, q)) = path.split_once('?') {
            r.path = p.into();
            r.query_string = q.into();
        }
        r.account = acct;
        r
    }

    fn code_of(resp: &Response) -> String {
        serde_json::from_slice::<Value>(&resp.body)
            .ok()
            .and_then(|v| v.get("__type").and_then(Value::as_str).map(str::to_string))
            .unwrap_or_default()
    }

    fn scope() -> u64 {
        uuid::Uuid::new_v4().as_u128() as u64
    }

    fn mk_group(acct: u64) -> (String, Value) {
        let name = format!("cg{}", hex_n(8));
        let resp = dispatch(&req_at(
            "POST",
            "/channelGroup",
            json!({"ChannelGroupName": name.clone()}),
            acct,
        ))
        .unwrap();
        let body_of = serde_json::from_slice::<Value>(&resp.body).unwrap();
        (name, body_of)
    }

    fn mk_channel(acct: u64, cg: &str) -> Value {
        let name = format!("ch{}", hex_n(8));
        let resp = dispatch(&req_at(
            "POST",
            &format!("/channelGroup/{cg}/channel"),
            json!({"ChannelName": name}),
            acct,
        ))
        .unwrap();
        serde_json::from_slice::<Value>(&resp.body).unwrap()
    }

    #[test]
    fn channel_group_lifecycle() {
        let acct = scope();
        let (cg, created) = mk_group(acct);
        assert_eq!(created["ChannelGroupName"], cg);
        assert_eq!(
            created["Arn"],
            format!("arn:aws:mediapackagev2:us-east-1:{acct}:channelGroup/{cg}")
        );
        assert!(created["EgressDomain"]
            .as_str()
            .unwrap()
            .contains(".egress."));
        assert!(created["EgressDomain"]
            .as_str()
            .unwrap()
            .contains("mediapackagev2.us-east-1.amazonaws.com"));
        // unix seconds with fraction (moto rest-json default)
        assert!(created["CreatedAt"].is_number());

        let g = serde_json::from_slice::<Value>(
            &dispatch(&req_at(
                "GET",
                &format!("/channelGroup/{cg}"),
                json!({}),
                acct,
            ))
            .unwrap()
            .body,
        )
        .unwrap();
        assert_eq!(g["ChannelGroupName"], cg);
        // Get* shape serializes lowercase tags — always {} on no-input create
        assert!(g.get("tags").is_some());
        assert!(!g.get("Tags").is_some());

        let upd = serde_json::from_slice::<Value>(
            &dispatch(&req_at(
                "PUT",
                &format!("/channelGroup/{cg}"),
                json!({"Description": "new-desc"}),
                acct,
            ))
            .unwrap()
            .body,
        )
        .unwrap();
        assert_eq!(upd["Description"], "new-desc");
        // moto regenerates the ETag on every update (models.py update_channel_group)
        assert_ne!(upd["ETag"], g["ETag"]);
        assert_ne!(upd["ETag"], created["ETag"]);

        let list = serde_json::from_slice::<Value>(
            &dispatch(&req_at("GET", "/channelGroup", json!({}), acct))
                .unwrap()
                .body,
        )
        .unwrap();
        assert!(list["Items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|i| i["ChannelGroupName"] == cg.as_str()));
        // list items omit ETag/EgressDomain
        assert!(list["Items"][0].get("ETag").is_none());

        assert_eq!(
            dispatch(&req_at(
                "DELETE",
                &format!("/channelGroup/{cg}"),
                json!({}),
                acct
            ))
            .unwrap()
            .status,
            200
        );
        let missing = dispatch(&req_at(
            "GET",
            &format!("/channelGroup/{cg}"),
            json!({}),
            acct,
        ))
        .unwrap();
        assert_eq!(missing.status, 400);
        assert_eq!(code_of(&missing), "ResourceNotFoundException");
        assert!(serde_json::from_slice::<Value>(&missing.body)
            .unwrap()["message"]
            .as_str()
            .unwrap()
            .starts_with("MediaPackage can't process your request because we can't find your channel group"));
    }

    #[test]
    fn delete_group_with_channels_conflicts() {
        let acct = scope();
        let (cg, _) = mk_group(acct);
        mk_channel(acct, &cg);
        let resp = dispatch(&req_at(
            "DELETE",
            &format!("/channelGroup/{cg}"),
            json!({}),
            acct,
        ))
        .unwrap();
        assert_eq!(resp.status, 400);
        assert_eq!(code_of(&resp), "ConflictException");
    }

    #[test]
    fn channel_lifecycle() {
        let acct = scope();
        let (cg, _) = mk_group(acct);
        let ch_name = format!("ch-{}", hex_n(8));
        let mut r = req_at(
            "POST",
            &format!("/channelGroup/{cg}/channel"),
            json!({"ChannelName": ch_name.clone(), "tags": {"t": "1"}}),
            acct,
        );
        let created: Value = serde_json::from_slice(&dispatch(&r).unwrap().body).unwrap();
        // Create echoes Tags (capital) and no InputType
        assert_eq!(created["ChannelName"], ch_name);
        assert_eq!(created["ChannelGroupName"], cg);
        assert_eq!(created["Tags"]["t"], "1");
        assert!(created.get("InputType").is_none());
        assert!(created["ETag"].as_str().unwrap().len() == 10);
        let ingest = created["IngestEndpoints"].as_array().unwrap();
        assert_eq!(ingest.len(), 2);
        assert!(ingest[0]["Url"].as_str().unwrap().contains("/in/v1/"));
        assert!(ingest[0]["Url"].as_str().unwrap().contains("-1.ingest"));
        assert!(ingest[1]["Url"].as_str().unwrap().contains("-2.ingest"));

        // GET with the botocore-style trailing slash
        r = req_at(
            "GET",
            &format!("/channelGroup/{cg}/channel/{ch_name}/"),
            json!({}),
            acct,
        );
        let got = serde_json::from_slice::<Value>(&dispatch(&r).unwrap().body).unwrap();
        assert_eq!(got["InputType"], "HLS");
        assert_eq!(got["Tags"]["t"], "1");
        assert_eq!(got["InputSwitchConfiguration"]["MQCSInputSwitching"], false);

        // PUT keeps InputType, lowercases tags
        r = req_at(
            "PUT",
            &format!("/channelGroup/{cg}/channel/{ch_name}/"),
            json!({
                "Description": "d2",
                "InputSwitchConfiguration": {"MQCSInputSwitching": true}
            }),
            acct,
        );
        let upd = serde_json::from_slice::<Value>(&dispatch(&r).unwrap().body).unwrap();
        assert_eq!(upd["Description"], "d2");
        assert_eq!(upd["InputType"], "HLS");
        assert_eq!(upd["tags"]["t"], "1");
        assert_eq!(upd["InputSwitchConfiguration"]["MQCSInputSwitching"], true);

        // LIST
        r = req_at(
            "GET",
            &format!("/channelGroup/{cg}/channel"),
            json!({}),
            acct,
        );
        let list = serde_json::from_slice::<Value>(&dispatch(&r).unwrap().body).unwrap();
        assert!(list["Items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|i| i["InputType"] == "HLS" && i["ChannelName"] == ch_name));

        // DELETE idempotent; GET then 400
        assert_eq!(
            dispatch(&req_at(
                "DELETE",
                &format!("/channelGroup/{cg}/channel/{ch_name}/"),
                json!({}),
                acct
            ))
            .unwrap()
            .status,
            200
        );
        let gone = dispatch(&req_at(
            "GET",
            &format!("/channelGroup/{cg}/channel/{ch_name}/"),
            json!({}),
            acct,
        ))
        .unwrap();
        assert_eq!(gone.status, 400);
        assert_eq!(
            gone.body,
            json_error(400, "ResourceNotFoundException", CHANNEL_NOT_FOUND).body
        );
    }

    #[test]
    fn channel_create_missing_group() {
        let acct = scope();
        let resp = dispatch(&req_at(
            "POST",
            "/channelGroup/nope/channel",
            json!({"ChannelName": "c"}),
            acct,
        ))
        .unwrap();
        assert_eq!(resp.status, 400);
        let body = serde_json::from_slice::<Value>(&resp.body).unwrap();
        assert_eq!(body["__type"], "ResourceNotFoundException");
        assert!(body["message"].as_str().unwrap().contains("channel group"));
    }

    #[test]
    fn overwrite_create_replaces_in_place() {
        let acct = scope();
        let (cg, _) = mk_group(acct);
        for _ in 0..2 {
            dispatch(&req_at(
                "POST",
                &format!("/channelGroup/{cg}/channel"),
                json!({"ChannelName": "dup"}),
                acct,
            ))
            .unwrap();
        }
        let b = serde_json::from_slice::<Value>(
            &dispatch(&req_at(
                "GET",
                &format!("/channelGroup/{cg}/channel"),
                json!({}),
                acct,
            ))
            .unwrap()
            .body,
        )
        .unwrap();
        let dups: Vec<&Value> = b["Items"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|i| i["ChannelName"] == "dup")
            .collect();
        assert_eq!(dups.len(), 1);
    }

    #[test]
    fn origin_endpoint_lifecycle() {
        let acct = scope();
        let (cg, _) = mk_group(acct);
        let ch = mk_channel(acct, &cg);
        let ch_name = ch["ChannelName"].as_str().unwrap().to_string();
        let ep_name = format!("ep-{}", hex_n(8));
        let create = serde_json::from_slice::<Value>(
            &dispatch(&req_at(
                "POST",
                &format!("/channelGroup/{cg}/channel/{ch_name}/originEndpoint"),
                json!({
                    "OriginEndpointName": ep_name.clone(),
                    "ContainerType": "TS",
                    "Description": "d",
                    "StartoverWindowSeconds": 600,
                    "HlsManifests": [{"ManifestName": "index", "ManifestWindowSeconds": 60}],
                    "ForceEndpointErrorConfiguration": {"EndpointErrorConditions": []}
                }),
                acct,
            ))
            .unwrap()
            .body,
        )
        .unwrap();
        assert_eq!(create["OriginEndpointName"], ep_name);
        assert_eq!(create["ContainerType"], "TS");
        assert_eq!(create["StartoverWindowSeconds"], 600);
        assert_eq!(create["HlsManifests"][0]["ManifestWindowSeconds"], 60);
        assert_eq!(create["Segment"], json!({}));
        assert_eq!(create["LowLatencyHlsManifests"], json!([]));
        assert!(create["ForceEndpointErrorConfiguration"].is_object());
        assert!(create["Arn"].as_str().unwrap().contains("/originEndpoint/"));

        // GET (trailing-slash-free path)
        let got = serde_json::from_slice::<Value>(
            &dispatch(&req_at(
                "GET",
                &format!("/channelGroup/{cg}/channel/{ch_name}/originEndpoint/{ep_name}"),
                json!({}),
                acct,
            ))
            .unwrap()
            .body,
        )
        .unwrap();
        assert_eq!(got["ETag"], create["ETag"]);
        assert!(got.get("ResetAt").is_none());

        // UPDATE replaces present members only
        let upd = serde_json::from_slice::<Value>(
            &dispatch(&req_at(
                "PUT",
                &format!("/channelGroup/{cg}/channel/{ch_name}/originEndpoint/{ep_name}"),
                json!({
                    "ContainerType": "TS",
                    "Description": "u",
                    "HlsManifests": [{"ManifestName": "index"}]
                }),
                acct,
            ))
            .unwrap()
            .body,
        )
        .unwrap();
        assert_eq!(upd["Description"], "u");
        assert_eq!(upd["HlsManifests"][0]["ManifestName"], "index");
        assert_eq!(upd["StartoverWindowSeconds"], 600);

        // LIST items drop Segment/Startover/ETag/Tags but keep manifests
        let list = serde_json::from_slice::<Value>(
            &dispatch(&req_at(
                "GET",
                &format!("/channelGroup/{cg}/channel/{ch_name}/originEndpoint"),
                json!({}),
                acct,
            ))
            .unwrap()
            .body,
        )
        .unwrap();
        let item = list["Items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|i| i["OriginEndpointName"] == ep_name)
            .unwrap();
        assert!(item.get("Segment").is_none());
        assert!(item.get("ETag").is_none());

        // DELETE then GET → exact endpoint not-found body
        assert_eq!(
            dispatch(&req_at(
                "DELETE",
                &format!("/channelGroup/{cg}/channel/{ch_name}/originEndpoint/{ep_name}"),
                json!({}),
                acct
            ))
            .unwrap()
            .status,
            200
        );
        let gone = dispatch(&req_at(
            "GET",
            &format!("/channelGroup/{cg}/channel/{ch_name}/originEndpoint/{ep_name}"),
            json!({}),
            acct,
        ))
        .unwrap();
        assert_eq!(
            gone.body,
            not_found("ResourceNotFoundException", ENDPOINT_NOT_FOUND).body
        );
    }

    #[test]
    fn endpoint_create_missing_channel() {
        let acct = scope();
        let (cg, _) = mk_group(acct);
        let resp = dispatch(&req_at(
            "POST",
            &format!("/channelGroup/{cg}/channel/ghost/originEndpoint"),
            json!({"OriginEndpointName": "e", "ContainerType": "TS"}),
            acct,
        ))
        .unwrap();
        assert_eq!(
            resp.body,
            not_found("ResourceNotFoundException", CHANNEL_NOT_FOUND).body
        );
    }

    #[test]
    fn policies_round_trip() {
        let acct = scope();
        let (cg, _) = mk_group(acct);
        let ch_name = format!("ch-{}", hex_n(8));
        let r = req_at(
            "POST",
            &format!("/channelGroup/{cg}/channel"),
            json!({"ChannelName": ch_name.clone()}),
            acct,
        );
        dispatch(&r).unwrap();

        // before any put -> policy not found
        let resp = dispatch(&req_at(
            "GET",
            &format!("/channelGroup/{cg}/channel/{ch_name}/policy"),
            json!({}),
            acct,
        ))
        .unwrap();
        assert_eq!(resp.status, 400);
        assert_eq!(
            resp.body,
            not_found("ResourceNotFoundException", CHANNEL_POLICY_NOT_FOUND).body
        );

        assert_eq!(
            dispatch(&req_at(
                "PUT",
                &format!("/channelGroup/{cg}/channel/{ch_name}/policy"),
                json!({"Policy": "{\"Version\": \"2012-10-17\"}"}),
                acct
            ))
            .unwrap()
            .status,
            200
        );
        let got = serde_json::from_slice::<Value>(
            &dispatch(&req_at(
                "GET",
                &format!("/channelGroup/{cg}/channel/{ch_name}/policy"),
                json!({}),
                acct,
            ))
            .unwrap()
            .body,
        )
        .unwrap();
        assert_eq!(got["ChannelGroupName"], cg);
        assert_eq!(got["ChannelName"], ch_name);
        assert_eq!(got["Policy"], "{\"Version\": \"2012-10-17\"}");

        assert_eq!(
            dispatch(&req_at(
                "DELETE",
                &format!("/channelGroup/{cg}/channel/{ch_name}/policy"),
                json!({}),
                acct
            ))
            .unwrap()
            .status,
            200
        );
        let after = dispatch(&req_at(
            "GET",
            &format!("/channelGroup/{cg}/channel/{ch_name}/policy"),
            json!({}),
            acct,
        ))
        .unwrap();
        assert_eq!(
            after.body,
            not_found("ResourceNotFoundException", CHANNEL_POLICY_NOT_FOUND).body
        );

        // endpoint policy with CdnAuthConfiguration echo
        let ep_name = "epp";
        let create = req_at(
            "POST",
            &format!("/channelGroup/{cg}/channel/{ch_name}/originEndpoint"),
            json!({"OriginEndpointName": ep_name, "ContainerType": "TS"}),
            acct,
        );
        dispatch(&create).unwrap();
        let put = dispatch(&req_at(
            "POST",
            &format!("/channelGroup/{cg}/channel/{ch_name}/originEndpoint/{ep_name}/policy"),
            json!({
                "Policy": "{}",
                "CdnAuthConfiguration": {
                    "CdnIdentifierSecretArns": ["arn:aws:secretsmanager:us-east-1:123456789012:secret:x"],
                    "SecretsRoleArn": "arn:aws:iam::123456789012:role/r"
                }
            }),
            acct,
        ))
        .unwrap();
        assert_eq!(put.status, 200);
        let pol = serde_json::from_slice::<Value>(
            &dispatch(&req_at(
                "GET",
                &format!("/channelGroup/{cg}/channel/{ch_name}/originEndpoint/{ep_name}/policy"),
                json!({}),
                acct,
            ))
            .unwrap()
            .body,
        )
        .unwrap();
        assert_eq!(pol["Policy"], "{}");
        assert_eq!(
            pol["CdnAuthConfiguration"]["SecretsRoleArn"],
            "arn:aws:iam::123456789012:role/r"
        );
        assert_eq!(
            dispatch(&req_at(
                "DELETE",
                &format!("/channelGroup/{cg}/channel/{ch_name}/originEndpoint/{ep_name}/policy"),
                json!({}),
                acct
            ))
            .unwrap()
            .status,
            200
        );
    }

    #[test]
    fn harvest_job_flow() {
        let acct = scope();
        let (cg, _) = mk_group(acct);
        let ch_name = format!("ch-{}", hex_n(8));
        let mut r = req_at(
            "POST",
            &format!("/channelGroup/{cg}/channel"),
            json!({"ChannelName": ch_name.clone()}),
            acct,
        );
        dispatch(&r).unwrap();
        let ep_name = "ep-h";
        dispatch(&req_at(
            "POST",
            &format!("/channelGroup/{cg}/channel/{ch_name}/originEndpoint"),
            json!({"OriginEndpointName": ep_name, "ContainerType": "TS"}),
            acct,
        ))
        .unwrap();

        r = req_at(
            "POST",
            &format!("/channelGroup/{cg}/channel/{ch_name}/originEndpoint/{ep_name}/harvestJob"),
            json!({
                "HarvestedManifests": {"HlsManifests": [{"ManifestName": "index"}]},
                "ScheduleConfiguration": {"StartTime": "2026-01-01T00:00:00Z", "EndTime": "2026-01-02T00:00:00Z"},
                "Destination": {"S3Destination": {"BucketName": "b", "DestinationPath": "p/"}}
            }),
            acct,
        );
        let created = serde_json::from_slice::<Value>(&dispatch(&r).unwrap().body).unwrap();
        let hj = created["HarvestJobName"].as_str().unwrap().to_string();
        assert_eq!(created["Status"], "QUEUED");
        assert_eq!(created["ErrorMessage"], "");
        // schedule timestamps serialize to unix ints
        assert_eq!(created["ScheduleConfiguration"]["StartTime"], 1767225600);
        let arn = created["Arn"].as_str().unwrap();
        assert!(arn.ends_with(&format!("/harvestJob/{hj}")));

        let got = serde_json::from_slice::<Value>(
            &dispatch(&req_at(
                "GET",
                &format!(
                    "/channelGroup/{cg}/channel/{ch_name}/originEndpoint/{ep_name}/harvestJob/{hj}"
                ),
                json!({}),
                acct,
            ))
            .unwrap()
            .body,
        )
        .unwrap();
        assert_eq!(got["HarvestJobName"], hj);

        // cancel (PUT) then status CANCELLED
        assert_eq!(
            dispatch(&req_at(
                "PUT",
                &format!(
                    "/channelGroup/{cg}/channel/{ch_name}/originEndpoint/{ep_name}/harvestJob/{hj}"
                ),
                json!({}),
                acct
            ))
            .unwrap()
            .status,
            200
        );
        let after = serde_json::from_slice::<Value>(
            &dispatch(&req_at(
                "GET",
                &format!(
                    "/channelGroup/{cg}/channel/{ch_name}/originEndpoint/{ep_name}/harvestJob/{hj}"
                ),
                json!({}),
                acct,
            ))
            .unwrap()
            .body,
        )
        .unwrap();
        assert_eq!(after["Status"], "CANCELLED");

        // list: name filters work; includeStatus is dead upstream (moto quirk)
        let filtered = serde_json::from_slice::<Value>(
            &dispatch(&req_at(
                "GET",
                &format!("/channelGroup/{cg}/harvestJob?channelName={ch_name}&originEndpointName={ep_name}"),
                json!({}),
                acct,
            ))
            .unwrap()
            .body,
        )
        .unwrap();
        assert_eq!(filtered["Items"].as_array().unwrap().len(), 1);
        let wrong_endpoint = serde_json::from_slice::<Value>(
            &dispatch(&req_at(
                "GET",
                &format!(
                    "/channelGroup/{cg}/harvestJob?channelName={ch_name}&originEndpointName=other"
                ),
                json!({}),
                acct,
            ))
            .unwrap()
            .body,
        )
        .unwrap();
        assert!(wrong_endpoint["Items"].as_array().unwrap().is_empty());
        // the queued/cancelled filter key is never honored, by moto parity
        let status_filtered = serde_json::from_slice::<Value>(
            &dispatch(&req_at(
                "GET",
                &format!(
                    "/channelGroup/{cg}/harvestJob?channelName={ch_name}&includeStatus=QUEUED"
                ),
                json!({}),
                acct,
            ))
            .unwrap()
            .body,
        )
        .unwrap();
        assert_eq!(status_filtered["Items"].as_array().unwrap().len(), 1); // CANCELLED job still listed
                                                                           // list items lack Tags
        assert!(status_filtered["Items"][0].get("Tags").is_none());

        // missing harvest job
        let miss = dispatch(&req_at(
            "GET",
            &format!(
                "/channelGroup/{cg}/channel/{ch_name}/originEndpoint/{ep_name}/harvestJob/nope"
            ),
            json!({}),
            acct,
        ))
        .unwrap();
        assert_eq!(
            miss.body,
            not_found("ResourceNotFoundException", HARVEST_NOT_FOUND).body
        );
    }

    #[test]
    fn resets_emit_unix_reset_at() {
        let acct = scope();
        let (cg, _) = mk_group(acct);
        let ch_name = format!("ch-{}", hex_n(8));
        let r = req_at(
            "POST",
            &format!("/channelGroup/{cg}/channel"),
            json!({"ChannelName": ch_name.clone()}),
            acct,
        );
        dispatch(&r).unwrap();
        let resp = serde_json::from_slice::<Value>(
            &dispatch(&req_at(
                "POST",
                &format!("/channelGroup/{cg}/channel/{ch_name}/reset"),
                json!({}),
                acct,
            ))
            .unwrap()
            .body,
        )
        .unwrap();
        assert_eq!(resp["ChannelGroupName"], cg);
        assert!(resp["ResetAt"].is_number());

        // endpoint reset makes ResetAt visible on subsequent GET
        let ep_name = "ep-r";
        dispatch(&req_at(
            "POST",
            &format!("/channelGroup/{cg}/channel/{ch_name}/originEndpoint"),
            json!({"OriginEndpointName": ep_name, "ContainerType": "TS"}),
            acct,
        ))
        .unwrap();
        let ereset = serde_json::from_slice::<Value>(
            &dispatch(&req_at(
                "POST",
                &format!("/channelGroup/{cg}/channel/{ch_name}/originEndpoint/{ep_name}/reset"),
                json!({}),
                acct,
            ))
            .unwrap()
            .body,
        )
        .unwrap();
        assert!(ereset["ResetAt"].is_number());
        let eget = serde_json::from_slice::<Value>(
            &dispatch(&req_at(
                "GET",
                &format!("/channelGroup/{cg}/channel/{ch_name}/originEndpoint/{ep_name}"),
                json!({}),
                acct,
            ))
            .unwrap()
            .body,
        )
        .unwrap();
        assert_eq!(eget["ResetAt"], ereset["ResetAt"]);
    }

    #[test]
    fn tags_round_trip_and_record_echo_split() {
        let acct = scope();
        let (cg, created) = mk_group(acct);
        let arn = created["Arn"].as_str().unwrap().to_string();
        let enc = url_encode_arn(&arn);

        // no tags at create -> resource echo {}
        let g = serde_json::from_slice::<Value>(
            &dispatch(&req_at(
                "GET",
                &format!("/channelGroup/{cg}"),
                json!({}),
                acct,
            ))
            .unwrap()
            .body,
        )
        .unwrap();
        assert_eq!(g["tags"], json!({}));

        // tag via API then list — lowercase "tags" key
        let tag = req_at(
            "POST",
            &format!("/tags/{enc}"),
            json!({"tags": {"team": "x", "second": "y"}}),
            acct,
        );
        assert_eq!(dispatch(&tag).unwrap().status, 204);
        let listed = serde_json::from_slice::<Value>(
            &dispatch(&req_at("GET", &format!("/tags/{enc}"), json!({}), acct))
                .unwrap()
                .body,
        )
        .unwrap();
        assert_eq!(listed["tags"]["team"], "x");

        // resource echoes stay create-time after tagger changes
        let g2 = serde_json::from_slice::<Value>(
            &dispatch(&req_at(
                "GET",
                &format!("/channelGroup/{cg}"),
                json!({}),
                acct,
            ))
            .unwrap()
            .body,
        )
        .unwrap();
        assert_eq!(g2["tags"], json!({}));

        // untag removes
        let mut untag = req_at("DELETE", &format!("/tags/{enc}"), json!({}), acct);
        untag.query_string = "tagKeys=team".to_string();
        assert_eq!(dispatch(&untag).unwrap().status, 204);
        let after = serde_json::from_slice::<Value>(
            &dispatch(&req_at("GET", &format!("/tags/{enc}"), json!({}), acct))
                .unwrap()
                .body,
        )
        .unwrap();
        assert_eq!(after["tags"], json!({"second": "y"}));
    }

    fn url_encode_arn(arn: &str) -> String {
        arn.replace(':', "%3A").replace('/', "%2F")
    }

    #[test]
    fn pagination_pages_groups() {
        let acct = scope();
        let names: Vec<String> = (0..3)
            .map(|_| {
                let (name, _) = mk_group(acct);
                name
            })
            .collect();
        let mut page = serde_json::from_slice::<Value>(
            &dispatch(&req_at(
                "GET",
                "/channelGroup?maxResults=1",
                json!({}),
                acct,
            ))
            .unwrap()
            .body,
        )
        .unwrap();
        let mut seen: Vec<String> = page["Items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["ChannelGroupName"].as_str().unwrap().to_string())
            .collect();
        let mut first_token = page
            .get("NextToken")
            .and_then(Value::as_str)
            .map(String::from);
        assert!(first_token.is_some());
        let mut pages = 1;
        while let Some(tok) = first_token.clone() {
            page = serde_json::from_slice::<Value>(
                &dispatch(&req_at(
                    "GET",
                    &format!("/channelGroup?maxResults=1&nextToken={tok}"),
                    json!({}),
                    acct,
                ))
                .unwrap()
                .body,
            )
            .unwrap();
            seen.extend(
                page["Items"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|i| i["ChannelGroupName"].as_str().unwrap().to_string()),
            );
            first_token = page
                .get("NextToken")
                .and_then(Value::as_str)
                .map(String::from);
            pages += 1;
            assert!(pages < 5);
        }
        assert_eq!(seen.len(), names.len());
        assert!(names.iter().all(|n| seen.contains(n)));
        assert!(seen.windows(2).all(|w| w[0] != w[1]));
    }

    #[test]
    fn invalid_pagination_token_rejected() {
        let acct = scope();
        let resp = dispatch(&req_at(
            "GET",
            "/channelGroup?nextToken=not-a-token",
            json!({}),
            acct,
        ))
        .unwrap();
        assert_eq!(resp.status, 400);
        assert_eq!(code_of(&resp), "InvalidToken");

        // valid-but-unknown arn -> also InvalidToken
        let fake = make_token(1, "arn:aws:mediapackagev2:x:123456789012:channelGroup/gone");
        let resp = dispatch(&req_at(
            "GET",
            &format!("/channelGroup?nextToken={}", B64.encode(fake)),
            json!({}),
            acct,
        ))
        .unwrap();
        assert_eq!(resp.status, 400);
        assert_eq!(code_of(&resp), "InvalidToken");
    }

    #[test]
    fn unknown_routes_answer_none() {
        let acct = scope();
        let (cg, _) = mk_group(acct);
        // wrong method on known path
        assert!(dispatch(&req_at(
            "GET",
            &format!("/channelGroup/{cg}/policy"),
            json!({}),
            acct
        ))
        .is_none());
        // unknown sub-resource
        assert!(dispatch(&req_at(
            "POST",
            &format!("/channelGroup/{cg}/ingest"),
            json!({}),
            acct
        ))
        .is_none());
        // bare /tags
        let r = req_at("GET", "/tags", json!({}), acct);
        assert!(dispatch(&r).is_none());
        // empty path
        let r = req_at("GET", "/", json!({}), acct);
        assert!(dispatch(&r).is_none());
    }

    #[test]
    fn timestamps_millisecond_precision() {
        // the ts helper emits ints on whole seconds, else 3-decimal floats
        assert_eq!(ts(1_767_225_600_000), json!(1_767_225_600));
        let t = json!(1_767_225_600_210_i64);
        assert_eq!(
            serde_json::to_string(&ts(1_767_225_600_210)).unwrap(),
            serde_json::to_string(&json!(1_767_225_600.21)).unwrap()
        );
        let _ = t;
    }

    #[test]
    fn per_account_isolation() {
        let acct_a = scope();
        let (cg, _) = mk_group(acct_a);
        let listing_a = serde_json::from_slice::<Value>(
            &dispatch(&req_at("GET", "/channelGroup", json!({}), acct_a))
                .unwrap()
                .body,
        )
        .unwrap();
        let listing_b = serde_json::from_slice::<Value>(
            &dispatch(&req_at(
                "GET",
                "/channelGroup",
                json!({}),
                acct_b_isolated(),
            ))
            .unwrap()
            .body,
        )
        .unwrap();
        assert!(listing_a["Items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|i| i["ChannelGroupName"] == cg));
        // second account starts empty except any groups created there
        assert!(listing_b["Items"].as_array().unwrap().len() <= 1);
        let _ = acct_b_isolated;
    }

    fn acct_b_isolated() -> u64 {
        scope()
    }
}
