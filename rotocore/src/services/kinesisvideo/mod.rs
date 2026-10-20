//! AWS Kinesis Video Streams backend (REST-JSON, one URI per operation).
//!
//! Port of the moto fork's `kinesisvideo` backend: streams, signaling
//! channels, tag stores and the `describe*Configuration` views. Wire
//! shapes carry the botocore member names verbatim (PascalCase keys).
//! moto's serializer drops members whose python value is `None`, so
//! e.g. a stream created without `DataRetentionInHours` simply lacks
//! the key in `describe_stream` output — mirrored here by omitting the
//! member instead of emitting `null`.
//!
//! Kept-faithful moto quirks:
//! - `AccessDeniedException` renders on the wire as
//!   `ClientLimitExceededException` with HTTP 401 (moto's
//!   `kinesisvideo/exceptions.py` raises the mismatched type).
//! - `Stream` ARNs always end in the fixed `/1598784211076` suffix.
//! - `list_signaling_channels` honors only `BEGINS_WITH`.
//! - A missing required `StreamName`/`ChannelName` renders as `None`
//!   in the resource name/ARN, exactly like moto's f-string.
//!
//! Surface boundary: eight botocore operations have no route in moto's
//! `urls.py`/`responses.py` (edge-configuration family and the
//! update-* configuration writers) — see `ABSENT_OPS`; they answer
//! moto's `NotImplemented` (501). Requests that are not Kinesis Video
//! control-plane APIs at all (archived-media/media URIs such as
//! `/getMedia`, which sign with the same `kinesisvideo` credential
//! scope) return `None` so the gateway's generic handling stays in
//! charge.

use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

struct Stream {
    arn: String,
    stream_name: String,
    device_name: Option<String>,
    media_type: Option<String>,
    kms_key_id: Option<String>,
    data_retention_in_hours: Option<i64>,
    tags: BTreeMap<String, String>,
    data_endpoint_number: String,
    version: String,
    creation_time: String,
}

impl Stream {
    /// moto's `Stream.to_dict()`; the wire omits `None` members.
    fn to_dict(&self) -> Value {
        let mut result = Map::new();
        if let Some(device) = &self.device_name {
            result.insert("DeviceName".into(), json!(device));
        }
        result.insert("StreamName".into(), json!(self.stream_name));
        result.insert("StreamARN".into(), json!(self.arn));
        if let Some(media_type) = &self.media_type {
            result.insert("MediaType".into(), json!(media_type));
        }
        if let Some(kms_key_id) = &self.kms_key_id {
            result.insert("KmsKeyId".into(), json!(kms_key_id));
        }
        result.insert("Version".into(), json!(self.version));
        result.insert("Status".into(), json!("ACTIVE"));
        result.insert("CreationTime".into(), json!(self.creation_time));
        if let Some(retention) = self.data_retention_in_hours {
            result.insert("DataRetentionInHours".into(), json!(retention));
        }
        Value::Object(result)
    }

    /// moto's `Stream.get_data_endpoint`: media APIs get an `s-`
    /// endpoint, everything else `b-`.
    fn data_endpoint(&self, region: &str, api_name: Option<&str>) -> String {
        let prefix = match api_name {
            Some("PUT_MEDIA") | Some("GET_MEDIA") => "s-",
            _ => "b-",
        };
        format!(
            "https://{prefix}{}.kinesisvideo.{region}.amazonaws.com",
            self.data_endpoint_number
        )
    }
}

struct SignalingChannel {
    arn: String,
    channel_name: String,
    channel_type: String,
    single_master_configuration: Value,
    tags: BTreeMap<String, String>,
    version: String,
    creation_time: String,
}

impl SignalingChannel {
    /// moto's `SignalingChannel.to_dict()`.
    fn to_dict(&self) -> Value {
        json!({
            "ChannelName": self.channel_name,
            "ChannelARN": self.arn,
            "ChannelType": self.channel_type,
            "ChannelStatus": "ACTIVE",
            "CreationTime": self.creation_time,
            "Version": self.version,
            "SingleMasterConfiguration": self.single_master_configuration,
        })
    }
}

#[derive(Default)]
struct State {
    streams: Vec<Stream>,
    signaling_channels: Vec<SignalingChannel>,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("kinesisvideo"));
    SLOT.clone()
}

// ---------------------------------------------------------------------------
// Wire helpers
// ---------------------------------------------------------------------------

fn ok(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

fn ok_empty() -> Response {
    ok(&json!({}))
}

fn error(status: u16, code: &str, message: &str) -> Response {
    let body = json!({"__type": code, "message": message});
    let mut resp = Response::json(status, serde_json::to_vec(&body).unwrap_or_default());
    resp.headers
        .push(("x-amzn-ErrorType".to_string(), format!("{code}:")));
    resp
}

/// moto's `ResourceNotFoundException` (HTTP 404) with the fork's
/// default message.
fn stream_not_found() -> Response {
    error(
        404,
        "ResourceNotFoundException",
        "The requested stream is not found or not active.",
    )
}

fn channel_not_found() -> Response {
    error(
        404,
        "ResourceNotFoundException",
        "The signaling channel was not found.",
    )
}

fn resource_not_found(arn: &str) -> Response {
    error(
        404,
        "ResourceNotFoundException",
        &format!("Resource {arn} not found"),
    )
}

fn in_use(message: String) -> Response {
    error(400, "ResourceInUseException", &message)
}

/// moto quirk: version mismatches raise `AccessDeniedException`, which
/// the fork's exceptions.py renders as HTTP 401 with code
/// `ClientLimitExceededException`.
fn access_denied(message: &str) -> Response {
    error(401, "ClientLimitExceededException", message)
}

fn not_implemented(op: &str) -> Response {
    error(
        501,
        "NotImplemented",
        &format!("The {op} action has not been implemented"),
    )
}

/// moto's `utcnow` rendered through the serializer's ISO8601_MICRO
/// format (microseconds, `Z`).
fn now_iso_micro() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.6fZ")
        .to_string()
}

/// moto's `get_random_string(include_digits=False, lower_case=True)`
/// — 20 lowercase letters.
fn random_version() -> String {
    const LOWER: &[u8] = b"abcdefghijklmnopqrstuvwxyz";
    let bytes = uuid::Uuid::new_v4().as_bytes().to_vec();
    (0..20)
        .map(|i| LOWER[bytes[i % bytes.len()] as usize % LOWER.len()] as char)
        .collect()
}

/// moto's `get_random_hex(length)` — lowercase hex digits.
fn random_hex(len: usize) -> String {
    const HEX: &[u8] = b"abcdef0123456789";
    let bytes = uuid::Uuid::new_v4().as_bytes().to_vec();
    (0..len)
        .map(|i| HEX[bytes[i] as usize % HEX.len()] as char)
        .collect()
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

fn param_str<'a>(p: &'a Value, key: &str) -> Option<&'a str> {
    p.get(key).and_then(Value::as_str)
}

/// moto's `_get_param` default: a missing name renders `None` in the
/// f-string ARN as the literal `None`.
fn param_name(p: &Value, key: &str) -> String {
    param_str(p, key).unwrap_or("None").to_string()
}

fn param_i64(p: &Value, key: &str) -> Option<i64> {
    match p.get(key) {
        Some(Value::Number(n)) => n.as_i64(),
        Some(Value::String(s)) => s.trim().parse::<i64>().ok(),
        _ => None,
    }
}

/// Tag map from the wire (CreateStream / TagStream): `{"key": "value"}`.
fn tags_map(p: &Value, key: &str) -> BTreeMap<String, String> {
    p.get(key)
        .and_then(Value::as_object)
        .map(|o| {
            o.iter()
                .filter_map(|(k, v)| v.as_str().map(|v| (k.to_string(), v.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

/// Tag list from the wire (CreateSignalingChannel / TagResource):
/// `[{"Key": .., "Value": ..}]`.
fn tags_list(p: &Value, key: &str) -> BTreeMap<String, String> {
    p.get(key)
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|t| {
                    Some((
                        t.get("Key")?.as_str()?.to_string(),
                        t.get("Value")?.as_str()?.to_string(),
                    ))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn tag_key_list(p: &Value, key: &str) -> Vec<String> {
    p.get(key)
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .map(|v| v.as_str().unwrap_or_default().to_string())
                .collect()
        })
        .unwrap_or_default()
}

fn tags_json(tags: &BTreeMap<String, String>) -> Value {
    let mut map = Map::new();
    for (k, v) in tags {
        map.insert(k.clone(), json!(v));
    }
    Value::Object(map)
}

// ---------------------------------------------------------------------------
// Operation resolution: requestUri -> operation, per botocore's
// kinesisvideo-2017-09-30 model (all operations are POST on their own
// URI; the three tag-resource operations keep PascalCase URIs).
// ---------------------------------------------------------------------------

const RULES: &[(&str, &str)] = &[
    ("createSignalingChannel", "CreateSignalingChannel"),
    ("createStream", "CreateStream"),
    ("deleteEdgeConfiguration", "DeleteEdgeConfiguration"),
    ("deleteSignalingChannel", "DeleteSignalingChannel"),
    ("deleteStream", "DeleteStream"),
    ("describeEdgeConfiguration", "DescribeEdgeConfiguration"),
    (
        "describeImageGenerationConfiguration",
        "DescribeImageGenerationConfiguration",
    ),
    (
        "describeMappedResourceConfiguration",
        "DescribeMappedResourceConfiguration",
    ),
    (
        "describeMediaStorageConfiguration",
        "DescribeMediaStorageConfiguration",
    ),
    (
        "describeNotificationConfiguration",
        "DescribeNotificationConfiguration",
    ),
    ("describeSignalingChannel", "DescribeSignalingChannel"),
    ("describeStream", "DescribeStream"),
    (
        "describeStreamStorageConfiguration",
        "DescribeStreamStorageConfiguration",
    ),
    ("getDataEndpoint", "GetDataEndpoint"),
    ("getSignalingChannelEndpoint", "GetSignalingChannelEndpoint"),
    ("listEdgeAgentConfigurations", "ListEdgeAgentConfigurations"),
    ("listSignalingChannels", "ListSignalingChannels"),
    ("listStreams", "ListStreams"),
    ("ListTagsForResource", "ListTagsForResource"),
    ("listTagsForStream", "ListTagsForStream"),
    (
        "startEdgeConfigurationUpdate",
        "StartEdgeConfigurationUpdate",
    ),
    ("TagResource", "TagResource"),
    ("tagStream", "TagStream"),
    ("UntagResource", "UntagResource"),
    ("untagStream", "UntagStream"),
    ("updateDataRetention", "UpdateDataRetention"),
    (
        "updateImageGenerationConfiguration",
        "UpdateImageGenerationConfiguration",
    ),
    (
        "updateMediaStorageConfiguration",
        "UpdateMediaStorageConfiguration",
    ),
    (
        "updateNotificationConfiguration",
        "UpdateNotificationConfiguration",
    ),
    ("updateSignalingChannel", "UpdateSignalingChannel"),
    ("updateStream", "UpdateStream"),
    (
        "updateStreamStorageConfiguration",
        "UpdateStreamStorageConfiguration",
    ),
];

/// botocore members with no route in the moto fork's `urls.py` — the
/// edge-configuration family plus the update-* configuration writers.
/// moto answers them 501 (the pre-port probes agree).
const ABSENT_OPS: &[&str] = &[
    "DeleteEdgeConfiguration",
    "DescribeEdgeConfiguration",
    "ListEdgeAgentConfigurations",
    "StartEdgeConfigurationUpdate",
    "UpdateImageGenerationConfiguration",
    "UpdateMediaStorageConfiguration",
    "UpdateNotificationConfiguration",
    "UpdateStreamStorageConfiguration",
];

fn wire_operation(path: &str) -> Option<&'static str> {
    let path = path.trim_start_matches('/').trim_end_matches('/');
    RULES
        .iter()
        .find(|(uri, _)| *uri == path)
        .map(|(_, op)| *op)
}

// ---------------------------------------------------------------------------
// Resource lookups (moto's `_get_stream` / `_get_signaling_channel`)
// ---------------------------------------------------------------------------

/// name wins, then ARN, otherwise `ResourceNotFoundException`; an empty
/// name/ARN counts as absent (moto's `stream_name or ""`).
fn find_stream_idx(b: &State, stream_name: &str, stream_arn: &str) -> Option<usize> {
    if !stream_name.is_empty() {
        return b.streams.iter().position(|s| s.stream_name == stream_name);
    }
    b.streams.iter().position(|s| s.arn == stream_arn)
}

fn find_stream(b: &State, p: &Value) -> Result<usize, Response> {
    let name = param_str(p, "StreamName").unwrap_or_default();
    let arn = param_str(p, "StreamARN").unwrap_or_default();
    find_stream_idx(b, name, arn).ok_or_else(stream_not_found)
}

/// ARN first, then channel name (moto's `_get_signaling_channel`).
fn find_channel(b: &State, channel_name: Option<&str>, channel_arn: Option<&str>) -> usize {
    if let Some(arn) = channel_arn {
        if let Some(index) = b.signaling_channels.iter().position(|c| c.arn == arn) {
            return index;
        }
    }
    if let Some(name) = channel_name {
        if let Some(index) = b
            .signaling_channels
            .iter()
            .position(|c| c.channel_name == name)
        {
            return index;
        }
    }
    b.signaling_channels.len()
}

fn find_channel_by_arn(b: &State, channel_arn: Option<&str>) -> Option<usize> {
    b.signaling_channels
        .iter()
        .position(|c| Some(c.arn.as_str()) == channel_arn)
}

/// Data endpoint for a sibling plane consumer: the archived-media backend
/// (`moto.kinesisvideoarchivedmedia`) reads the same streams from the
/// shared `kinesisvideo` backend. Resolves the stream exactly like moto's
/// `_get_stream` (name wins, then ARN) and reports what
/// `Stream.get_data_endpoint(api_name)` would return; `None` means the
/// stream is missing and the caller answers ResourceNotFoundException.
pub(crate) fn stream_data_endpoint(
    account: u64,
    region: &str,
    stream_name: &str,
    stream_arn: &str,
    api_name: &str,
) -> Option<String> {
    let slot = state();
    let st = slot.read();
    let b = st.get(&(account, region.to_string()))?;
    let index = find_stream_idx(b, stream_name, stream_arn)?;
    Some(b.streams[index].data_endpoint(region, Some(api_name)))
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

fn handle(op: &str, b: &mut State, p: &Value, region: &str, account: u64) -> Response {
    match op {
        "CreateSignalingChannel" => create_signaling_channel(b, p, region, account),
        "CreateStream" => create_stream(b, p, region, account),
        "DeleteSignalingChannel" => delete_signaling_channel(b, p),
        "DeleteStream" => delete_stream(b, p),
        "DescribeImageGenerationConfiguration" => {
            describe_stream_view(b, p, "ImageGenerationConfiguration", json!({}))
        }
        "DescribeMappedResourceConfiguration" => {
            describe_stream_view(b, p, "MappedResourceConfigurationList", json!([]))
        }
        "DescribeMediaStorageConfiguration" => describe_media_storage_configuration(b, p),
        "DescribeNotificationConfiguration" => {
            describe_stream_view(b, p, "NotificationConfiguration", json!({}))
        }
        "DescribeSignalingChannel" => describe_signaling_channel(b, p),
        "DescribeStream" => describe_stream(b, p),
        "DescribeStreamStorageConfiguration" => {
            describe_stream_view(b, p, "StreamStorageConfigurations", json!([]))
        }
        "GetDataEndpoint" => get_data_endpoint(b, p, region),
        "GetSignalingChannelEndpoint" => get_signaling_channel_endpoint(b, p, region),
        "ListSignalingChannels" => list_signaling_channels(b, p),
        "ListStreams" => list_streams(b),
        "ListTagsForResource" => list_tags_for_resource(b, p),
        "ListTagsForStream" => list_tags_for_stream(b, p),
        "TagResource" => tag_resource(b, p),
        "TagStream" => tag_stream(b, p),
        "UntagResource" => untag_resource(b, p),
        "UntagStream" => untag_stream(b, p),
        "UpdateDataRetention" => update_data_retention(b, p),
        "UpdateSignalingChannel" => update_signaling_channel(b, p),
        "UpdateStream" => update_stream(b, p),
        // Unreachable for members of RULES; safety net for retypings.
        _ => not_implemented(op),
    }
}

// --- streams ----------------------------------------------------------------

fn stream_arn(region: &str, account: u64, stream_name: &str) -> String {
    // moto pins the stream-ARN resource suffix to this fixed value.
    format!(
        "arn:{}:kinesisvideo:{region}:{account}:stream/{stream_name}/1598784211076",
        partition(region)
    )
}

fn create_stream(b: &mut State, p: &Value, region: &str, account: u64) -> Response {
    let stream_name = param_name(p, "StreamName");
    if b.streams.iter().any(|s| s.stream_name == stream_name) {
        return in_use(format!("The stream {stream_name} already exists."));
    }
    let stream = Stream {
        arn: stream_arn(region, account, &stream_name),
        stream_name,
        device_name: param_str(p, "DeviceName").map(str::to_string),
        media_type: param_str(p, "MediaType").map(str::to_string),
        kms_key_id: param_str(p, "KmsKeyId").map(str::to_string),
        data_retention_in_hours: param_i64(p, "DataRetentionInHours"),
        tags: tags_map(p, "Tags"),
        data_endpoint_number: random_hex(8),
        version: random_version(),
        creation_time: now_iso_micro(),
    };
    let arn = stream.arn.clone();
    b.streams.push(stream);
    ok(&json!({"StreamARN": arn}))
}

fn describe_stream(b: &State, p: &Value) -> Response {
    let index = match find_stream(b, p) {
        Ok(index) => index,
        Err(err) => return err,
    };
    ok(&json!({"StreamInfo": b.streams[index].to_dict()}))
}

fn stream_current_version(b: &State, index: usize) -> String {
    b.streams[index].version.clone()
}

fn update_stream(b: &mut State, p: &Value) -> Response {
    let index = match find_stream(b, p) {
        Ok(index) => index,
        Err(err) => return err,
    };
    if b.streams[index].version != param_str(p, "CurrentVersion").unwrap_or_default() {
        return access_denied("The stream version does not match the current version.");
    }
    let stream = &mut b.streams[index];
    if let Some(v) = param_str(p, "DeviceName") {
        stream.device_name = Some(v.to_string());
    }
    if let Some(v) = param_str(p, "MediaType") {
        stream.media_type = Some(v.to_string());
    }
    stream.version = random_version();
    ok_empty()
}

fn list_streams(b: &State) -> Response {
    let list: Vec<Value> = b.streams.iter().map(Stream::to_dict).collect();
    ok(&json!({"StreamInfoList": list}))
}

fn delete_stream(b: &mut State, p: &Value) -> Response {
    // moto's delete_stream resolves only by ARN.
    let Some(index) = find_stream_idx(b, "", param_str(p, "StreamARN").unwrap_or_default()) else {
        return stream_not_found();
    };
    b.streams.remove(index);
    ok_empty()
}

fn get_data_endpoint(b: &State, p: &Value, region: &str) -> Response {
    let index = match find_stream(b, p) {
        Ok(index) => index,
        Err(err) => return err,
    };
    let endpoint = b.streams[index].data_endpoint(region, param_str(p, "APIName"));
    ok(&json!({"DataEndpoint": endpoint}))
}

fn tag_stream(b: &mut State, p: &Value) -> Response {
    let index = match find_stream(b, p) {
        Ok(index) => index,
        Err(err) => return err,
    };
    for (k, v) in tags_map(p, "Tags") {
        b.streams[index].tags.insert(k, v);
    }
    ok_empty()
}

fn untag_stream(b: &mut State, p: &Value) -> Response {
    let index = match find_stream(b, p) {
        Ok(index) => index,
        Err(err) => return err,
    };
    for key in tag_key_list(p, "TagKeyList") {
        b.streams[index].tags.remove(&key);
    }
    ok_empty()
}

fn list_tags_for_stream(b: &State, p: &Value) -> Response {
    let index = match find_stream(b, p) {
        Ok(index) => index,
        Err(err) => return err,
    };
    ok(&json!({"Tags": tags_json(&b.streams[index].tags)}))
}

// --- signaling channels -------------------------------------------------------

fn create_signaling_channel(b: &mut State, p: &Value, region: &str, account: u64) -> Response {
    let channel_name = param_name(p, "ChannelName");
    if b.signaling_channels
        .iter()
        .any(|c| c.channel_name == channel_name)
    {
        return in_use(format!(
            "The signaling channel {channel_name} already exists."
        ));
    }
    let channel_type = param_str(p, "ChannelType")
        .unwrap_or("SINGLE_MASTER")
        .to_string();
    let single_master_configuration = p
        .get("SingleMasterConfiguration")
        .cloned()
        .unwrap_or_else(|| json!({"MessageTtlSeconds": 60}));
    let channel = SignalingChannel {
        arn: format!(
            "arn:{}:kinesisvideo:{region}:{account}:channel/{channel_name}/{}",
            partition(region),
            random_hex(8)
        ),
        channel_name,
        channel_type,
        single_master_configuration,
        tags: tags_list(p, "Tags"),
        version: random_version(),
        creation_time: now_iso_micro(),
    };
    let arn = channel.arn.clone();
    b.signaling_channels.push(channel);
    ok(&json!({"ChannelARN": arn}))
}

fn describe_signaling_channel(b: &State, p: &Value) -> Response {
    let index = find_channel(b, param_str(p, "ChannelName"), param_str(p, "ChannelARN"));
    if index >= b.signaling_channels.len() {
        return channel_not_found();
    }
    ok(&json!({"ChannelInfo": b.signaling_channels[index].to_dict()}))
}

fn update_signaling_channel(b: &mut State, p: &Value) -> Response {
    let Some(index) = find_channel_by_arn(b, param_str(p, "ChannelARN")) else {
        return channel_not_found();
    };
    if b.signaling_channels[index].version != param_str(p, "CurrentVersion").unwrap_or_default() {
        return access_denied("The channel version does not match the current version.");
    }
    let channel = &mut b.signaling_channels[index];
    if let Some(config) = p.get("SingleMasterConfiguration") {
        if !config.is_null() {
            channel.single_master_configuration = config.clone();
        }
    }
    channel.version = random_version();
    ok_empty()
}

fn delete_signaling_channel(b: &mut State, p: &Value) -> Response {
    let Some(index) = find_channel_by_arn(b, param_str(p, "ChannelARN")) else {
        return channel_not_found();
    };
    b.signaling_channels.remove(index);
    ok_empty()
}

fn list_signaling_channels(b: &State, p: &Value) -> Response {
    if let Some(condition) = p.get("ChannelNameCondition").filter(|c| !c.is_null()) {
        let operator = condition
            .get("ComparisonOperator")
            .and_then(Value::as_str)
            .unwrap_or("BEGINS_WITH");
        let value = condition
            .get("ComparisonValue")
            .and_then(Value::as_str)
            .unwrap_or("");
        // moto implements only BEGINS_WITH; other operators are ignored.
        if operator == "BEGINS_WITH" {
            let list: Vec<Value> = b
                .signaling_channels
                .iter()
                .filter(|c| c.channel_name.starts_with(value))
                .map(SignalingChannel::to_dict)
                .collect();
            return ok(&json!({"ChannelInfoList": list}));
        }
    }
    let list: Vec<Value> = b
        .signaling_channels
        .iter()
        .map(SignalingChannel::to_dict)
        .collect();
    ok(&json!({"ChannelInfoList": list}))
}

fn get_signaling_channel_endpoint(b: &State, p: &Value, region: &str) -> Response {
    let Some(channel_arn) = param_str(p, "ChannelARN") else {
        return channel_not_found();
    };
    if b.signaling_channels.iter().all(|c| c.arn != channel_arn) {
        return error(
            404,
            "ResourceNotFoundException",
            &format!("Channel {channel_arn} not found"),
        );
    }
    let mut protocols: Vec<String> = p
        .get("SingleMasterChannelEndpointConfiguration")
        .and_then(|c| c.get("Protocols"))
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_else(|| vec!["WSS".into(), "HTTPS".into()]);
    if protocols.is_empty() {
        protocols = vec!["WSS".into(), "HTTPS".into()];
    }
    let list: Vec<Value> = protocols
        .into_iter()
        .map(|protocol| {
            json!({
                "Protocol": protocol,
                "ResourceEndpoint":
                    format!("https://{channel_arn}.kinesisvideo.{region}.amazonaws.com"),
            })
        })
        .collect();
    ok(&json!({"ResourceEndpointList": list}))
}

fn describe_media_storage_configuration(b: &State, p: &Value) -> Response {
    if find_channel_by_arn(b, param_str(p, "ChannelARN")).is_none() {
        // moto matches the channel by ARN and reports 404 otherwise.
        return error(
            404,
            "ResourceNotFoundException",
            &format!(
                "Channel {} not found",
                param_str(p, "ChannelARN").unwrap_or_default()
            ),
        );
    }
    // moto keeps no media storage configuration on the fork's channel
    // model — the reported config is always the empty shape.
    ok(&json!({"MediaStorageConfiguration": {}}))
}

// --- tags across resources -----------------------------------------------------

fn tag_resource(b: &mut State, p: &Value) -> Response {
    let Some(resource_arn) = param_str(p, "ResourceARN").map(str::to_string) else {
        return resource_not_found("");
    };
    let tags = tags_list(p, "Tags");
    if let Some(index) = b.streams.iter().position(|s| s.arn == resource_arn) {
        for (k, v) in tags {
            b.streams[index].tags.insert(k, v);
        }
        return ok_empty();
    }
    if let Some(index) = b
        .signaling_channels
        .iter()
        .position(|c| c.arn == resource_arn)
    {
        for (k, v) in tags {
            b.signaling_channels[index].tags.insert(k, v);
        }
        return ok_empty();
    }
    resource_not_found(&resource_arn)
}

fn untag_resource(b: &mut State, p: &Value) -> Response {
    let Some(resource_arn) = param_str(p, "ResourceARN").map(str::to_string) else {
        return resource_not_found("");
    };
    let keys = tag_key_list(p, "TagKeyList");
    if let Some(index) = b.streams.iter().position(|s| s.arn == resource_arn) {
        for key in &keys {
            b.streams[index].tags.remove(key);
        }
        return ok_empty();
    }
    if let Some(index) = b
        .signaling_channels
        .iter()
        .position(|c| c.arn == resource_arn)
    {
        for key in &keys {
            b.signaling_channels[index].tags.remove(key);
        }
        return ok_empty();
    }
    resource_not_found(&resource_arn)
}

fn list_tags_for_resource(b: &State, p: &Value) -> Response {
    let Some(resource_arn) = param_str(p, "ResourceARN") else {
        return resource_not_found("");
    };
    if let Some(index) = b.streams.iter().position(|s| s.arn == resource_arn) {
        return ok(&json!({"Tags": tags_json(&b.streams[index].tags)}));
    }
    if let Some(index) = b
        .signaling_channels
        .iter()
        .position(|c| c.arn == resource_arn)
    {
        return ok(&json!({"Tags": tags_json(&b.signaling_channels[index].tags)}));
    }
    resource_not_found(resource_arn)
}

// --- configuration views -------------------------------------------------------

/// Body of the `describe*Configuration` stream views: resolve the stream
/// (name or ARN) and report the stored view. The fork never populates
/// these members (nothing writes them), so every report is empty.
fn describe_stream_view(b: &State, p: &Value, member: &str, empty: Value) -> Response {
    let index = match find_stream(b, p) {
        Ok(index) => index,
        Err(err) => return err,
    };
    let _arn: String = b.streams[index].arn.clone();
    ok(&json!({member: empty}))
}

fn update_data_retention(b: &mut State, p: &Value) -> Response {
    let index = match find_stream(b, p) {
        Ok(index) => index,
        Err(err) => return err,
    };
    if stream_current_version(b, index) != param_str(p, "CurrentVersion").unwrap_or_default() {
        return access_denied("The stream version does not match the current version.");
    }
    let change = param_i64(p, "DataRetentionChangeInHours").unwrap_or(0);
    let stream = &mut b.streams[index];
    match param_str(p, "Operation") {
        Some("INCREASE_DATA_RETENTION") => {
            stream.data_retention_in_hours =
                Some(stream.data_retention_in_hours.unwrap_or(0) + change);
        }
        Some("DECREASE_DATA_RETENTION") => {
            stream.data_retention_in_hours =
                Some((stream.data_retention_in_hours.unwrap_or(0) - change).max(0));
        }
        _ => {}
    }
    ok_empty()
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "kinesisvideo" {
        return None;
    }
    // Every control-plane operation rides on POST + its own URI; other
    // methods (or paths outside the botocore model, e.g. the
    // archived-media URIs that share this signing scope) stay unowned.
    if req.method != "POST" {
        return None;
    }
    let op = wire_operation(&req.path)?;
    if ABSENT_OPS.contains(&op) {
        return Some(not_implemented(op));
    }
    let p = crate::proto::parse_json_body(&req.body);
    let slot = state();
    let mut st = slot.write();
    let b = st.entry((req.account, req.region.clone())).or_default();
    Some(handle(op, b, &p, &req.region, req.account))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn mk(method: &str, uri: &str, body: Value) -> Request {
        Request {
            service: "kinesisvideo".into(),
            operation: String::new(),
            method: method.into(),
            path: format!("/{uri}"),
            query_string: String::new(),
            headers: std::collections::HashMap::new(),
            body: serde_json::to_vec(&body).unwrap_or_default().into(),
            account: 111000111001,
            region: "us-east-1".into(),
        }
    }

    fn body(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap_or(Value::Null)
    }

    fn create_stream(account: u64, name: &str, retention: Option<i64>) -> String {
        let mut params = json!({"StreamName": name});
        if let Some(r) = retention {
            params["DataRetentionInHours"] = json!(r);
        }
        let mut req = mk("POST", "createStream", params);
        req.account = account;
        let resp = dispatch(&req).expect("dispatch");
        assert_eq!(resp.status, 200);
        body(&resp)["StreamARN"].as_str().unwrap().to_string()
    }

    fn create_channel(account: u64, name: &str) -> String {
        let mut req = mk(
            "POST",
            "createSignalingChannel",
            json!({"ChannelName": name}),
        );
        req.account = account;
        let resp = dispatch(&req).expect("dispatch");
        assert_eq!(resp.status, 200);
        body(&resp)["ChannelARN"].as_str().unwrap().to_string()
    }

    #[test]
    fn stream_lifecycle() {
        let account = 111000111002;
        let arn = create_stream(account, "kv-life", Some(24));
        assert!(arn.ends_with(":stream/kv-life/1598784211076"));

        // describe by name and by ARN
        let mut req = mk("POST", "describeStream", json!({"StreamName": "kv-life"}));
        req.account = account;
        let info = body(&dispatch(&req).unwrap())["StreamInfo"].clone();
        assert_eq!(info["StreamARN"], json!(arn));
        assert_eq!(info["Status"], json!("ACTIVE"));
        assert_eq!(info["DataRetentionInHours"], json!(24));
        assert!(info["CreationTime"].as_str().unwrap().contains('T'));

        let mut req = mk("POST", "describeStream", json!({"StreamARN": arn}));
        req.account = account;
        assert_eq!(
            body(&dispatch(&req).unwrap())["StreamInfo"]["StreamName"],
            json!("kv-life")
        );
    }

    #[test]
    fn stream_without_retention_omits_the_member() {
        let account = 111000111003;
        let arn = create_stream(account, "kv-noretention", None);
        let mut req = mk("POST", "describeStream", json!({"StreamARN": arn}));
        req.account = account;
        let info = body(&dispatch(&req).unwrap())["StreamInfo"].clone();
        // moto's serializer drops None members entirely.
        assert!(info.get("DataRetentionInHours").is_none());
        assert!(info.get("MediaType").is_none());
    }

    #[test]
    fn duplicate_stream_is_in_use() {
        let account = 111000111004;
        create_stream(account, "kv-dup", None);
        let mut req = mk("POST", "createStream", json!({"StreamName": "kv-dup"}));
        req.account = account;
        let resp = dispatch(&req).unwrap();
        assert_eq!(resp.status, 400);
        assert_eq!(body(&resp)["__type"], json!("ResourceInUseException"));
        assert_eq!(
            body(&resp)["message"],
            json!("The stream kv-dup already exists.")
        );
    }

    #[test]
    fn get_data_endpoint_media_apis_use_s_prefix() {
        let account = 111000111005;
        let arn = create_stream(account, "kv-ep", None);
        for (api, prefix) in [("GET_MEDIA", "s-"), ("GET_CLIP", "b-")] {
            let mut req = mk(
                "POST",
                "getDataEndpoint",
                json!({"StreamARN": arn, "APIName": api}),
            );
            req.account = account;
            let endpoint = body(&dispatch(&req).unwrap())["DataEndpoint"]
                .as_str()
                .unwrap()
                .to_string();
            assert!(endpoint.starts_with(&format!("https://{prefix}")), "{api}");
            assert!(
                endpoint.ends_with(".kinesisvideo.us-east-1.amazonaws.com"),
                "{api}"
            );
        }
    }

    #[test]
    fn update_stream_rotates_version_and_checks_it() {
        let account = 111000111006;
        let arn = create_stream(account, "kv-update", None);
        let mut req = mk("POST", "describeStream", json!({"StreamARN": arn}));
        req.account = account;
        let version = body(&dispatch(&req).unwrap())["StreamInfo"]["Version"]
            .as_str()
            .unwrap()
            .to_string();

        // wrong version → moto's AccessDeniedException quirk (401 +
        // ClientLimitExceededException).
        let mut req = mk(
            "POST",
            "updateStream",
            json!({"StreamARN": arn, "CurrentVersion": "bogus", "MediaType": "video/h264"}),
        );
        req.account = account;
        let resp = dispatch(&req).unwrap();
        assert_eq!(resp.status, 401);
        assert_eq!(body(&resp)["__type"], json!("ClientLimitExceededException"));

        let mut req = mk(
            "POST",
            "updateStream",
            json!({"StreamARN": arn, "CurrentVersion": version, "MediaType": "video/h264"}),
        );
        req.account = account;
        assert_eq!(dispatch(&req).unwrap().status, 200);
        let mut req = mk("POST", "describeStream", json!({"StreamARN": arn}));
        req.account = account;
        let info = body(&dispatch(&req).unwrap())["StreamInfo"].clone();
        assert_eq!(info["MediaType"], json!("video/h264"));
        assert_ne!(info["Version"], json!(version));
    }

    #[test]
    fn update_data_retention_increases_and_decreases() {
        let account = 111000111007;
        let arn = create_stream(account, "kv-retention", Some(1));
        let version_of = |arn: &str| {
            let mut req = mk("POST", "describeStream", json!({"StreamARN": arn}));
            req.account = account;
            body(&dispatch(&req).unwrap())["StreamInfo"]["Version"]
                .as_str()
                .unwrap()
                .to_string()
        };
        let version = version_of(&arn);
        let mut req = mk(
            "POST",
            "updateDataRetention",
            json!({
                "StreamARN": arn,
                "CurrentVersion": version,
                "Operation": "INCREASE_DATA_RETENTION",
                "DataRetentionChangeInHours": 3,
            }),
        );
        req.account = account;
        assert_eq!(dispatch(&req).unwrap().status, 200);
        let mut req = mk("POST", "describeStream", json!({"StreamARN": arn}));
        req.account = account;
        assert_eq!(
            body(&dispatch(&req).unwrap())["StreamInfo"]["DataRetentionInHours"],
            json!(4)
        );
        let version = version_of(&arn);
        let mut req = mk(
            "POST",
            "updateDataRetention",
            json!({
                "StreamARN": arn,
                "CurrentVersion": version,
                "Operation": "DECREASE_DATA_RETENTION",
                "DataRetentionChangeInHours": 10,
            }),
        );
        req.account = account;
        assert_eq!(dispatch(&req).unwrap().status, 200);
        let mut req = mk("POST", "describeStream", json!({"StreamARN": arn}));
        req.account = account;
        assert_eq!(
            body(&dispatch(&req).unwrap())["StreamInfo"]["DataRetentionInHours"],
            json!(0)
        );
    }

    #[test]
    fn stream_tags_through_both_apis() {
        let account = 111000111008;
        let arn = create_stream(account, "kv-tags", None);
        // TagStream takes a map.
        let mut req = mk(
            "POST",
            "tagStream",
            json!({"StreamARN": arn, "Tags": {"env": "test", "team": "dev"}}),
        );
        req.account = account;
        assert_eq!(dispatch(&req).unwrap().status, 200);
        // UntagStream removes one key.
        let mut req = mk(
            "POST",
            "untagStream",
            json!({"StreamARN": arn, "TagKeyList": ["env"]}),
        );
        req.account = account;
        assert_eq!(dispatch(&req).unwrap().status, 200);
        let mut req = mk("POST", "ListTagsForResource", json!({"ResourceARN": arn}));
        req.account = account;
        assert_eq!(
            body(&dispatch(&req).unwrap())["Tags"],
            json!({"team": "dev"})
        );

        // TagResource/UntagResource operate on the same tags.
        let mut req = mk(
            "POST",
            "TagResource",
            json!({"ResourceARN": arn, "Tags": [{"Key": "env", "Value": "staging"}]}),
        );
        req.account = account;
        assert_eq!(dispatch(&req).unwrap().status, 200);
        let mut req = mk(
            "POST",
            "UntagResource",
            json!({"ResourceARN": arn, "TagKeyList": ["team"]}),
        );
        req.account = account;
        assert_eq!(dispatch(&req).unwrap().status, 200);
        let mut req = mk("POST", "ListTagsForResource", json!({"ResourceARN": arn}));
        req.account = account;
        assert_eq!(
            body(&dispatch(&req).unwrap())["Tags"],
            json!({"env": "staging"})
        );

        // Stream-level view reports the same store.
        let mut req = mk(
            "POST",
            "listTagsForStream",
            json!({"StreamName": "kv-tags"}),
        );
        req.account = account;
        assert_eq!(
            body(&dispatch(&req).unwrap())["Tags"],
            json!({"env": "staging"})
        );
    }

    #[test]
    fn deleted_stream_disappears_and_forties() {
        let account = 111000111009;
        let arn = create_stream(account, "kv-delete", None);
        let mut req = mk("POST", "deleteStream", json!({"StreamARN": arn}));
        req.account = account;
        assert_eq!(dispatch(&req).unwrap().status, 200);
        let mut req = mk("POST", "deleteStream", json!({"StreamARN": arn}));
        req.account = account;
        let resp = dispatch(&req).unwrap();
        assert_eq!(resp.status, 404);
        assert_eq!(body(&resp)["__type"], json!("ResourceNotFoundException"));

        // list_streams is per-(account, region) isolated.
        let mut req = mk("POST", "listStreams", json!({}));
        req.account = account;
        let names: Vec<String> = body(&dispatch(&req).unwrap())["StreamInfoList"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|s| s["StreamName"].as_str())
            .map(str::to_string)
            .collect();
        assert!(!names.contains(&"kv-delete".to_string()));
    }

    #[test]
    fn signaling_channel_lifecycle() {
        let account = 111000111010;
        let arn = create_channel(account, "kv-sig");
        assert!(arn.contains(":channel/kv-sig/"));

        let mut req = mk(
            "POST",
            "describeSignalingChannel",
            json!({"ChannelARN": arn}),
        );
        req.account = account;
        let info = body(&dispatch(&req).unwrap())["ChannelInfo"].clone();
        assert_eq!(info["ChannelName"], json!("kv-sig"));
        assert_eq!(info["ChannelStatus"], json!("ACTIVE"));
        assert_eq!(info["ChannelType"], json!("SINGLE_MASTER"));
        assert_eq!(
            info["SingleMasterConfiguration"],
            json!({"MessageTtlSeconds": 60})
        );

        // duplicate name is rejected.
        let mut req = mk(
            "POST",
            "createSignalingChannel",
            json!({"ChannelName": "kv-sig"}),
        );
        req.account = account;
        let resp = dispatch(&req).unwrap();
        assert_eq!(resp.status, 400);
        assert_eq!(body(&resp)["__type"], json!("ResourceInUseException"));

        // update rotates the version.
        let version = info["Version"].as_str().unwrap().to_string();
        let mut req = mk(
            "POST",
            "updateSignalingChannel",
            json!({"ChannelARN": arn, "CurrentVersion": version}),
        );
        req.account = account;
        assert_eq!(dispatch(&req).unwrap().status, 200);

        let mut req = mk("POST", "deleteSignalingChannel", json!({"ChannelARN": arn}));
        req.account = account;
        assert_eq!(dispatch(&req).unwrap().status, 200);
        let mut req = mk(
            "POST",
            "describeSignalingChannel",
            json!({"ChannelARN": arn}),
        );
        req.account = account;
        let resp = dispatch(&req).unwrap();
        assert_eq!(resp.status, 404);
        assert_eq!(
            body(&resp)["message"],
            json!("The signaling channel was not found.")
        );
    }

    #[test]
    fn list_signaling_channels_begins_with_filter() {
        let account = 111000111011;
        create_channel(account, "kv-forward-1");
        create_channel(account, "kv-other-1");
        let mut req = mk(
            "POST",
            "listSignalingChannels",
            json!({"ChannelNameCondition": {
                "ComparisonOperator": "BEGINS_WITH",
                "ComparisonValue": "kv-for",
            }}),
        );
        req.account = account;
        let list = body(&dispatch(&req).unwrap())["ChannelInfoList"]
            .as_array()
            .unwrap()
            .clone();
        assert!(list
            .iter()
            .all(|c| c["ChannelName"].as_str().unwrap().starts_with("kv-for")));
        assert_eq!(list.len(), 1);
    }

    #[test]
    fn signaling_channel_endpoint_lists_protocols() {
        let account = 111000111012;
        let arn = create_channel(account, "kv-ep");
        let mut req = mk(
            "POST",
            "getSignalingChannelEndpoint",
            json!({
                "ChannelARN": arn,
                "SingleMasterChannelEndpointConfiguration": {
                    "Protocols": ["WSS"],
                    "Role": "MASTER",
                },
            }),
        );
        req.account = account;
        let list = body(&dispatch(&req).unwrap())["ResourceEndpointList"]
            .as_array()
            .unwrap()
            .clone();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0]["Protocol"], json!("WSS"));

        // unknown channel → ResourceNotFoundException.
        let unknown = "arn:aws:kinesisvideo:us-east-1:123456789012:channel/nope/0".to_string();
        let mut req = mk(
            "POST",
            "getSignalingChannelEndpoint",
            json!({"ChannelARN": unknown}),
        );
        req.account = account;
        let resp = dispatch(&req).unwrap();
        assert_eq!(resp.status, 404);
        assert_eq!(body(&resp)["__type"], json!("ResourceNotFoundException"));
        assert_eq!(
            body(&resp)["message"],
            json!(format!("Channel {unknown} not found"))
        );
    }

    #[test]
    fn describe_media_storage_configuration_needs_a_channel() {
        let account = 111000111013;
        let unknown = "arn:aws:kinesisvideo:us-east-1:123456789012:channel/nope/1".to_string();
        let mut req = mk(
            "POST",
            "describeMediaStorageConfiguration",
            json!({"ChannelARN": unknown}),
        );
        req.account = account;
        let resp = dispatch(&req).unwrap();
        assert_eq!(resp.status, 404);
        assert_eq!(body(&resp)["__type"], json!("ResourceNotFoundException"));

        let arn = create_channel(account, "kv-media");
        let mut req = mk(
            "POST",
            "describeMediaStorageConfiguration",
            json!({"ChannelARN": arn}),
        );
        req.account = account;
        let resp = dispatch(&req).unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(body(&resp)["MediaStorageConfiguration"], json!({}));
    }

    #[test]
    fn describe_stream_views_are_empty() {
        let account = 111000111014;
        let arn = create_stream(account, "kv-views", None);
        for (uri, member, expected) in [
            (
                "describeImageGenerationConfiguration",
                "ImageGenerationConfiguration",
                json!({}),
            ),
            (
                "describeNotificationConfiguration",
                "NotificationConfiguration",
                json!({}),
            ),
            (
                "describeStreamStorageConfiguration",
                "StreamStorageConfigurations",
                json!([]),
            ),
            (
                "describeMappedResourceConfiguration",
                "MappedResourceConfigurationList",
                json!([]),
            ),
        ] {
            let mut req = mk("POST", uri, json!({"StreamARN": arn}));
            req.account = account;
            let resp = dispatch(&req).unwrap();
            assert_eq!(resp.status, 200, "{uri}");
            assert_eq!(body(&resp)[member], expected, "{uri}");
        }

        // unknown stream view still 404s.
        let mut req = mk(
            "POST",
            "describeImageGenerationConfiguration",
            json!({"StreamName": "no-such-stream"}),
        );
        req.account = account;
        let resp = dispatch(&req).unwrap();
        assert_eq!(resp.status, 404);
        assert_eq!(body(&resp)["__type"], json!("ResourceNotFoundException"));
    }

    #[test]
    fn absent_ops_answer_motos_501() {
        let account = 111000111015;
        for uri in [
            "deleteEdgeConfiguration",
            "describeEdgeConfiguration",
            "listEdgeAgentConfigurations",
            "startEdgeConfigurationUpdate",
            "updateImageGenerationConfiguration",
            "updateMediaStorageConfiguration",
            "updateNotificationConfiguration",
            "updateStreamStorageConfiguration",
        ] {
            let mut req = mk("POST", uri, json!({}));
            req.account = account;
            let resp = dispatch(&req).unwrap();
            assert_eq!(resp.status, 501, "/{uri}");
            let b = body(&resp);
            assert_eq!(b["__type"], json!("NotImplemented"), "/{uri}");
            assert_eq!(
                b["message"],
                json!(format!(
                    "The {} action has not been implemented",
                    wire_operation(&format!("/{uri}")).unwrap()
                )),
                "/{uri}"
            );
        }
    }

    #[test]
    fn non_control_plane_uris_stay_unowned() {
        // Archived-media URIs sign with the same kinesisvideo scope but
        // belong to other services; dispatch declines them.
        for uri in ["getMedia", "getClip", "getHLSStreamingSessionURL"] {
            let req = mk("POST", uri, json!({}));
            assert!(dispatch(&req).is_none(), "/{uri}");
        }
        assert!(dispatch(&Request {
            service: "s3".into(),
            operation: String::new(),
            method: "POST".into(),
            path: "/createStream".into(),
            query_string: String::new(),
            headers: std::collections::HashMap::new(),
            body: serde_json::to_vec(&json!({})).unwrap_or_default().into(),
            account: 111000111016,
            region: "us-east-1".into(),
        })
        .is_none());
    }
}
