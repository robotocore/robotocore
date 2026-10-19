//! AWS Direct Connect backend — moto-equivalent (json protocol).
//!
//! Replaces the moto sidecar for `directconnect`
//! (`X-Amz-Target: DirectConnect_20121206.<Op>` on
//! `application/x-amz-json-1.1`). State is per-(account, region), mirroring
//! moto's `DirectConnectBackend` (moto/directconnect): connections, LAGs and
//! a tagger with moto's `TaggingService` semantics.
//!
//! The deployed moto fork's surface for this service is 11 handlers —
//! DescribeConnections, CreateConnection, DeleteConnection, UpdateConnection,
//! AssociateMacSecKey, DisassociateMacSecKey, CreateLag, DescribeLags,
//! TagResource, UntagResource, DescribeTags — all implemented 1:1 here,
//! including the fork's quirks:
//!
//! - connection/LAG ids are ARN-flavoured (`dx-con/dx-moto-<name>-<ts>`,
//!   `dxlag/dxlag-moto-<name>-<ts>`, `%Y%m%d%H%M%S` timestamps),
//! - static mock fields (`awsDevice`: `mock_device`, `partnerName`:
//!   `mock_partner`, `portEncryptionStatus`: `Encryption Down`, `vlan`: 0),
//! - `DeleteConnection` marks the record `deleted` instead of dropping it,
//! - `CreateLag` spawns `numberOfConnections` child connections named
//!   `Requested Connection <n> for Lag <lag id>` (also registered at the
//!   top level); `CreateLag` with a `connectionId` is moto's unguarded
//!   `NotImplementedError` crash → a 500 `InternalError`,
//! - `MacSecKey` records render `{secretARN, ckn, state, startOn}` (never
//!   `cak`); a missing `secretARN` coalesces to `mock_secret_arn`,
//! - associating a MacSec key with a LAG id (`dxlag-`) keys the LAG and
//!   re-binds each member connection's key list to the LAG's — the visible
//!   effect of the fork's shared-list aliasing,
//! - `MacSecKeyNotFound` only surfaces while keys remain; an empty key list
//!   (or unknown id, whose lookup hides behind an empty list) is moto's
//!   `ConnectionNotFound`.
//!
//! The other botocore operations answer the gateway's generic
//! `501 NotImplemented`: the fork's `moto/directconnect/responses.py` has no
//! handler for them (verified live: its backend raises
//! `NotImplementedError: The <op> action has not been implemented`).
//! `loaIssueTime`/`startOn` mirror moto's `%Y-%m-%d %H:%M:%S` strings; UTC
//! is used where moto uses naive local time (containers run UTC).
//!
//! Contract: `tests/compatibility/test_directconnect_compat.py` (the 11
//! fork-handler operations) plus unit tests below.

use crate::proto::{json_error, parse_json_body};
use crate::store::{self, ScopeKey};
use crate::{Request, Response};
use parking_lot::RwLock;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

type Stores = Arc<RwLock<HashMap<ScopeKey, DxStore>>>;

fn stores() -> Stores {
    store::slot::<DxStore>("directconnect")
}

/// Per-(account, region) in-memory state for Direct Connect.
#[derive(Default)]
struct DxStore {
    /// connection id -> connection
    connections: BTreeMap<String, Connection>,
    /// lag id -> lag
    lags: BTreeMap<String, Lag>,
    /// arn -> {tag key: value} — moto's `TaggingService`
    tags: BTreeMap<String, BTreeMap<String, Option<String>>>,
}

fn with_store(req: &Request, f: impl FnOnce(&mut DxStore) -> Response) -> Response {
    let slot = stores();
    let mut map = slot.write();
    let st = map.entry((req.account, req.region.clone())).or_default();
    f(st)
}

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

/// One MacSec key (fork's `MacSecKey` dataclass minus the never-rendered
/// `cak` member).
#[derive(Clone)]
struct MacSecKey {
    secret_arn: String,
    ckn: String,
    state: String,
    start_on: String,
}

impl MacSecKey {
    /// The mock pair moto's `CreateConnection`/`CreateLag` plant.
    fn default_mock() -> Self {
        MacSecKey {
            secret_arn: "mock_secret_arn".to_string(),
            ckn: "mock_ckn".to_string(),
            state: "associated".to_string(),
            start_on: dx_time(),
        }
    }

    /// moto's `to_dict`: falsy `secret_arn`/`ckn` coalesce to `""`; `cak`
    /// stays off the wire.
    fn to_dict(&self) -> Value {
        json!({
            "secretARN": self.secret_arn,
            "ckn": self.ckn,
            "state": self.state,
            "startOn": self.start_on,
        })
    }
}

/// A Direct Connect connection (fork's `Connection` dataclass).
#[derive(Clone)]
struct Connection {
    connection_id: String,
    connection_name: String,
    connection_state: String,
    bandwidth: String,
    location: String,
    lag_id: Option<String>,
    loa_issue_time: String,
    provider_name: Option<String>,
    /// Rendered verbatim — `null` unless the caller sent `requestMACSec`.
    mac_sec_capable: Value,
    encryption_mode: String,
    mac_sec_keys: Vec<MacSecKey>,
    region: String,
}

/// The MacSec key list a connection renders: LAG members show the LAG's
/// shared list (moto re-binds each member's list to the LAG's).
fn lag_member_keys<'a>(st: &'a DxStore, conn: &'a Connection) -> &'a [MacSecKey] {
    if let Some(lag_id) = &conn.lag_id {
        if let Some(lag) = st.lags.get(lag_id) {
            if lag.connections.contains(&conn.connection_id) {
                return &lag.mac_sec_keys;
            }
        }
    }
    &conn.mac_sec_keys
}

/// moto's `Connection.to_dict()`: the full record, null members included.
/// Member connections of a LAG render the LAG's MacSec key list — moto
/// re-binds each member's list to the LAG's shared list.
fn connection_dict(st: &DxStore, conn: &Connection) -> Value {
    let mac_sec_keys = lag_member_keys(st, conn)
        .iter()
        .map(MacSecKey::to_dict)
        .collect::<Vec<_>>();
    json!({
        "awsDevice": "mock_device",
        "awsDeviceV2": "mock_device_v2",
        "awsLogicalDeviceId": "mock_logical_device_id",
        "bandwidth": conn.bandwidth,
        "connectionId": conn.connection_id,
        "connectionName": conn.connection_name,
        "connectionState": conn.connection_state,
        "encryptionMode": conn.encryption_mode,
        "hasLogicalRedundancy": false,
        "jumboFrameCapable": false,
        "lagId": conn.lag_id,
        "loaIssueTime": conn.loa_issue_time,
        "location": conn.location,
        "macSecCapable": conn.mac_sec_capable,
        "macSecKeys": mac_sec_keys,
        "partnerName": "mock_partner",
        "portEncryptionStatus": "Encryption Down",
        "providerName": conn.provider_name,
        "region": conn.region,
        "tags": list_tags_for_resource(st, &conn.connection_id),
        "vlan": 0,
    })
}

/// A Direct Connect LAG (fork's `LAG` dataclass). Member connections are
/// stored by id: moto's python LAG keeps shared object references, so every
/// render reads the member mutating through the connections map.
#[derive(Clone)]
struct Lag {
    lag_id: String,
    lag_name: String,
    lag_state: String,
    connections_bandwidth: String,
    number_of_connections: i64,
    minimum_links: i64,
    connections: Vec<String>,
    location: String,
    provider_name: Option<String>,
    mac_sec_capable: Value,
    encryption_mode: String,
    mac_sec_keys: Vec<MacSecKey>,
    region: String,
}

/// moto's `LAG.to_dict()`.
fn lag_dict(st: &DxStore, lag: &Lag) -> Value {
    json!({
        "awsDevice": "mock_device",
        "awsDeviceV2": "mock_device_v2",
        "awsLogicalDeviceId": "mock_logical_device_id",
        "connectionsBandwidth": lag.connections_bandwidth,
        "numberOfConnections": lag.number_of_connections,
        "minimumLinks": lag.minimum_links,
        "connections": lag
            .connections
            .iter()
            .filter_map(|id| st.connections.get(id))
            .map(|c| connection_dict(st, c))
            .collect::<Vec<_>>(),
        "lagId": lag.lag_id,
        "lagName": lag.lag_name,
        "lagState": lag.lag_state,
        "encryptionMode": lag.encryption_mode,
        "hasLogicalRedundancy": false,
        "jumboFrameCapable": false,
        "location": lag.location,
        "macSecCapable": lag.mac_sec_capable,
        "macSecKeys": lag
            .mac_sec_keys
            .iter()
            .map(MacSecKey::to_dict)
            .collect::<Vec<_>>(),
        "providerName": lag.provider_name,
        "region": lag.region,
        "tags": list_tags_for_resource(st, &lag.lag_id),
    })
}

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

/// Operations the backend answers.
pub const OPERATIONS: [&str; 11] = [
    "DescribeConnections",
    "CreateConnection",
    "DeleteConnection",
    "UpdateConnection",
    "AssociateMacSecKey",
    "DisassociateMacSecKey",
    "CreateLag",
    "DescribeLags",
    "TagResource",
    "UntagResource",
    "DescribeTags",
];

/// True when `operation` is a Direct Connect operation.
pub fn handles_operation(operation: &str) -> bool {
    OPERATIONS.contains(&operation)
}

pub fn dispatch(req: &Request) -> Option<Response> {
    // The gateway router maps directconnect's botocore targetPrefix
    // `OvertureService` to `support` (a python-era registry row that
    // predates this backend), so Direct Connect requests arrive labeled
    // `support` with one of `OPERATIONS` — support's own operations are
    // disjoint. The crate dispatcher consults `handles_operation`.
    match req.service.as_str() {
        "directconnect" => Some(with_store(req, |st| route(req, st))),
        "support" if handles_operation(req.operation.as_str()) => {
            Some(with_store(req, |st| route(req, st)))
        }
        _ => None,
    }
}

fn route(req: &Request, st: &mut DxStore) -> Response {
    let body = parse_json_body(&req.body);
    match req.operation.as_str() {
        // --- connections ---------------------------------------------------------
        "DescribeConnections" => describe_connections(st, &body, &req.region),
        "CreateConnection" => {
            let conn = create_connection_inner(st, req.account, &req.region, &body, None);
            ok(connection_dict(st, &conn))
        }
        "DeleteConnection" => delete_connection(st, &body, &req.region),
        "UpdateConnection" => update_connection(st, &body, &req.region),
        "AssociateMacSecKey" => associate_mac_sec_key(st, &body, &req.region),
        "DisassociateMacSecKey" => disassociate_mac_sec_key(st, &body, &req.region),
        // --- lags ---------------------------------------------------------------
        "CreateLag" => create_lag(st, req.account, &req.region, &body),
        "DescribeLags" => describe_lags(st, &body, &req.region),
        // --- tags -----------------------------------------------------------------
        "TagResource" => {
            let tags = body
                .get("tags")
                .and_then(Value::as_array)
                .filter(|t| !t.is_empty())
                .cloned();
            tag_resource(
                st,
                string_at(&body, "resourceArn").unwrap_or(""),
                tags.as_ref(),
            );
            ok_empty()
        }
        "UntagResource" => {
            let keys = str_list(body.get("tagKeys"));
            untag_resource(st, string_at(&body, "resourceArn").unwrap_or(""), &keys);
            ok_empty()
        }
        "DescribeTags" => describe_tags(st, &body),
        other => json_error(
            501,
            "NotImplemented",
            &format!("The {other} action has not been implemented"),
        ),
    }
}

// ---------------------------------------------------------------------------
// Operation helpers
// ---------------------------------------------------------------------------

/// moto's `describe_connections`: unknown filter id raises, empty stays all.
fn describe_connections(st: &DxStore, body: &Value, region: &str) -> Response {
    let Some(connection_id) = truthy_str(body, "connectionId") else {
        let conns: Vec<Value> = st
            .connections
            .values()
            .map(|c| connection_dict(st, c))
            .collect();
        return ok(json!({ "connections": conns }));
    };
    match st.connections.get(connection_id) {
        Some(conn) => ok(json!({ "connections": [connection_dict(st, conn)] })),
        None => connection_not_found(connection_id, region),
    }
}

/// Build a connection as the fork's `create_connection` does; `lag_id` is
/// only set by `CreateLag`'s child spawns. Tags land in the store when the
/// member is truthy, exactly like moto's `if tags:` guard.
fn build_connection(
    st: &mut DxStore,
    account: u64,
    region: &str,
    params: &Value,
    lag_id: Option<String>,
) -> Connection {
    let connection_name = string_at(params, "connectionName")
        .unwrap_or("")
        .to_string();
    let request_mac_sec = params.get("requestMACSec").cloned().unwrap_or(Value::Null);
    let (encryption_mode, mac_sec_keys) = if truthy(&request_mac_sec) {
        ("must_encrypt".to_string(), vec![MacSecKey::default_mock()])
    } else {
        ("no_encrypt".to_string(), Vec::new())
    };
    let connection = Connection {
        connection_id: dx_arn(
            region,
            account,
            "dx-con",
            &format!("dx-moto-{connection_name}-{}", dx_time_ts()),
        ),
        connection_name,
        connection_state: "available".to_string(),
        bandwidth: string_at(params, "bandwidth").unwrap_or("").to_string(),
        location: string_at(params, "location").unwrap_or("").to_string(),
        lag_id,
        loa_issue_time: dx_time(),
        provider_name: params
            .get("providerName")
            .and_then(Value::as_str)
            .map(String::from),
        // moto params.get(...) — absent/explicit-null renders `null`.
        mac_sec_capable: if request_mac_sec.is_null() {
            Value::Null
        } else {
            request_mac_sec
        },
        encryption_mode,
        mac_sec_keys,
        region: region.to_string(),
    };
    if let Some(tags) = truthy_array(params.get("tags")) {
        tag_resource(st, &connection.connection_id, Some(tags));
    }
    connection
}

/// moto's `create_connection` — the record joins the shared dict.
fn create_connection_inner(
    st: &mut DxStore,
    account: u64,
    region: &str,
    body: &Value,
    lag_id: Option<String>,
) -> Connection {
    let conn = build_connection(st, account, region, body, lag_id);
    st.connections
        .insert(conn.connection_id.clone(), conn.clone());
    conn
}

fn delete_connection(st: &mut DxStore, body: &Value, region: &str) -> Response {
    let Some(connection_id) = string_at(body, "connectionId") else {
        return connection_id_missing();
    };
    match st.connections.get_mut(connection_id) {
        Some(conn) => {
            conn.connection_state = "deleted".to_string();
            let record = conn.clone();
            ok(connection_dict(st, &record))
        }
        None => connection_not_found(connection_id, region),
    }
}

fn update_connection(st: &mut DxStore, body: &Value, region: &str) -> Response {
    let Some(connection_id) = string_at(body, "connectionId") else {
        return connection_id_missing();
    };
    match st.connections.get_mut(connection_id) {
        Some(conn) => {
            if let Some(name) = truthy_str(body, "connectionName") {
                conn.connection_name = name.to_string();
            }
            if let Some(mode) = truthy_str(body, "encryptionMode") {
                conn.encryption_mode = mode.to_string();
            }
            let record = conn.clone();
            ok(connection_dict(st, &record))
        }
        None => connection_not_found(connection_id, region),
    }
}

/// moto's `associate_mac_sec_key`: `dxlag-` ids route to the LAG and re-bind
/// each member connection's key list; everything else is a connection.
fn associate_mac_sec_key(st: &mut DxStore, body: &Value, region: &str) -> Response {
    let connection_id = string_at(body, "connectionId").unwrap_or("");
    let mac_sec_key = MacSecKey {
        // moto: `secret_arn or "mock_secret_arn"`, `ckn` as sent (may be "").
        secret_arn: string_at(body, "secretARN")
            .filter(|s| !s.is_empty())
            .unwrap_or("mock_secret_arn")
            .to_string(),
        ckn: string_at(body, "ckn").unwrap_or("").to_string(),
        state: "associated".to_string(),
        start_on: dx_time(),
    };
    if connection_id.contains("dxlag-") {
        let Some(lag) = st.lags.get_mut(connection_id) else {
            return lag_not_found(connection_id, region);
        };
        // moto re-binds member connections to the LAG's shared list; members
        // are stored by id here, so the lag's list is the shared view.
        lag.mac_sec_keys.push(mac_sec_key);
        let keys = lag.mac_sec_keys.clone();
        return ok(json!({
            "connectionId": connection_id,
            "macSecKeys": keys.iter().map(MacSecKey::to_dict).collect::<Vec<_>>(),
        }));
    }
    let Some(conn) = st.connections.get_mut(connection_id) else {
        return connection_not_found(connection_id, region);
    };
    conn.mac_sec_keys.push(mac_sec_key);
    let keys = conn.mac_sec_keys.clone();
    ok(json!({
        "connectionId": connection_id,
        "macSecKeys": keys.iter().map(MacSecKey::to_dict).collect::<Vec<_>>(),
    }))
}

/// moto's `disassociate_mac_sec_key`: pops the key matching `secretARN`
/// (casefold) after marking it `disassociated`. An unknown id or an emptied
/// key list is `ConnectionNotFound`; no key matches and it's
/// `MacSecKeyNotFound`.
fn disassociate_mac_sec_key(st: &mut DxStore, body: &Value, region: &str) -> Response {
    let connection_id = string_at(body, "connectionId").unwrap_or("");
    let secret_arn = string_at(body, "secretARN").unwrap_or("");
    let is_lag = connection_id.contains("dxlag-");
    let keys = if is_lag {
        st.lags.get(connection_id).map(|l| l.mac_sec_keys.clone())
    } else {
        st.connections
            .get(connection_id)
            .map(|c| c.mac_sec_keys.clone())
    };
    let Some(keys) = keys else {
        return connection_not_found(connection_id, region);
    };
    if keys.is_empty() {
        return connection_not_found(connection_id, region);
    }
    let wanted = secret_arn.to_lowercase();
    let Some(pos) = keys
        .iter()
        .position(|k| k.secret_arn.to_lowercase() == wanted)
    else {
        return mac_sec_key_not_found(secret_arn, connection_id);
    };
    let mut key = keys[pos].clone();
    key.state = "disassociated".to_string();
    if is_lag {
        let lag = st.lags.get_mut(connection_id).expect("checked above");
        lag.mac_sec_keys.remove(pos);
    } else {
        let conn = st
            .connections
            .get_mut(connection_id)
            .expect("checked above");
        conn.mac_sec_keys.remove(pos);
    }
    ok(json!({
        "connectionId": connection_id,
        "macSecKeys": [key.to_dict()],
    }))
}

/// moto's `create_lag`: spawns children named
/// `Requested Connection <n> for Lag <lag id>`; a `connectionId` member is
/// moto's unguarded `NotImplementedError` → 500 `InternalError`.
fn create_lag(st: &mut DxStore, account: u64, region: &str, body: &Value) -> Response {
    // moto: `if connection_id:` — an empty member keeps the code path.
    if truthy_str(body, "connectionId").is_some() {
        return internal_error_500(
            "creating a lag with a connection_id is not currently supported by moto",
        );
    }
    let number_of_connections = match body.get("numberOfConnections").and_then(Value::as_i64) {
        Some(n) => n,
        // moto: `range(None)` TypeError crash (the suite always sends a count).
        None => {
            return internal_error_500(
                "RangeError: 'NoneType' object cannot be interpreted as an integer",
            )
        }
    };
    let request_mac_sec = body.get("requestMACSec").cloned().unwrap_or(Value::Null);
    let mac_sec = if truthy(&request_mac_sec) {
        ("must_encrypt".to_string(), Some(MacSecKey::default_mock()))
    } else {
        ("no_encrypt".to_string(), None)
    };
    let lag_id = dx_arn(
        region,
        account,
        "dxlag",
        &format!(
            "dxlag-moto-{}-{}",
            string_at(body, "lagName").unwrap_or(""),
            dx_time_ts()
        ),
    );
    let mut lag = Lag {
        lag_id: lag_id.clone(),
        lag_name: string_at(body, "lagName").unwrap_or("").to_string(),
        lag_state: "available".to_string(),
        connections_bandwidth: string_at(body, "connectionsBandwidth")
            .unwrap_or("")
            .to_string(),
        number_of_connections,
        minimum_links: 0,
        connections: Vec::new(),
        location: string_at(body, "location").unwrap_or("").to_string(),
        provider_name: body
            .get("providerName")
            .and_then(Value::as_str)
            .map(String::from),
        mac_sec_capable: if request_mac_sec.is_null() {
            Value::Null
        } else {
            request_mac_sec.clone()
        },
        encryption_mode: mac_sec.0.clone(),
        mac_sec_keys: mac_sec.1.clone().into_iter().collect(),
        region: region.to_string(),
    };
    for i in 0..number_of_connections {
        // moto creates children through `create_connection` with
        // `connection_name=f"Requested Connection {i + 1} for Lag {lag.lag_id}"`.
        let mut child_params = Map::new();
        child_params.insert(
            "location".into(),
            body.get("location").cloned().unwrap_or(Value::Null),
        );
        child_params.insert(
            "bandwidth".into(),
            body.get("connectionsBandwidth")
                .cloned()
                .unwrap_or(Value::Null),
        );
        child_params.insert(
            "connectionName".into(),
            json!(format!("Requested Connection {} for Lag {lag_id}", i + 1)),
        );
        child_params.insert(
            "tags".into(),
            body.get("childConnectionTags")
                .cloned()
                .unwrap_or(Value::Null),
        );
        child_params.insert(
            "providerName".into(),
            body.get("providerName").cloned().unwrap_or(Value::Null),
        );
        child_params.insert("requestMACSec".into(), Value::Bool(false));
        let mut child = build_connection(
            st,
            account,
            region,
            &Value::Object(child_params),
            Some(lag_id.clone()),
        );
        // moto then pushes the LAG's MACSec mode onto each child.
        if truthy(&request_mac_sec) {
            child.mac_sec_capable = Value::Bool(true);
            child.mac_sec_keys = mac_sec.1.clone().into_iter().collect();
            child.encryption_mode = mac_sec.0.clone();
        }
        // moto spawns children through `create_connection`, so they also
        // register at the top level; the LAG keeps their ids.
        st.connections
            .insert(child.connection_id.clone(), child.clone());
        lag.connections.push(child.connection_id);
    }
    // moto: `if tags:` — an absent/empty list never reaches the tagger.
    if let Some(tags) = truthy_array(body.get("tags")) {
        tag_resource(st, &lag_id, Some(tags));
    }
    let rendered = lag_dict(st, &lag);
    st.lags.insert(lag_id, lag);
    ok(rendered)
}

/// moto's `describe_lags`: unknown filter id raises, empty stays all.
fn describe_lags(st: &DxStore, body: &Value, region: &str) -> Response {
    let Some(lag_id) = truthy_str(body, "lagId") else {
        let lags: Vec<Value> = st.lags.values().map(|l| lag_dict(st, l)).collect();
        return ok(json!({ "lags": lags }));
    };
    match st.lags.get(lag_id) {
        Some(lag) => ok(json!({ "lags": [lag_dict(st, lag)] })),
        None => lag_not_found(lag_id, region),
    }
}

/// moto's `describe_tags` — a row per requested arn, empty tags included.
fn describe_tags(st: &DxStore, body: &Value) -> Response {
    let rows: Vec<Value> = str_list(body.get("resourceArns"))
        .into_iter()
        .map(|arn| {
            json!({
                "resourceArn": arn,
                "tags": list_tags_for_resource(st, &arn),
            })
        })
        .collect();
    ok(json!({ "resourceTags": rows }))
}

// ---------------------------------------------------------------------------
// Tags (moto's TaggingService semantics)
// ---------------------------------------------------------------------------

/// `TaggingService.tag_resource`: keys overwrite, members without a value
/// store null. `directconnect` calls it with `tags if tags else []`, so an
/// empty list still creates an (untagged) row, just like moto.
fn tag_resource(st: &mut DxStore, arn: &str, tags: Option<&Vec<Value>>) {
    let Some(tags) = tags else {
        return;
    };
    let row = st.tags.entry(arn.to_string()).or_default();
    for tag in tags {
        let Some(key) = tag
            .get("key")
            .and_then(Value::as_str)
            .or_else(|| tag.get("Key").and_then(Value::as_str))
        else {
            continue;
        };
        // A member without `value` (or with a null value) stores null.
        let value = tag
            .get("value")
            .and_then(Value::as_str)
            .or_else(|| tag.get("Value").and_then(Value::as_str))
            .map(String::from);
        row.insert(key.to_string(), value);
    }
}

fn untag_resource(st: &mut DxStore, arn: &str, tag_keys: &[String]) {
    if let Some(row) = st.tags.get_mut(arn) {
        for key in tag_keys {
            row.remove(key);
        }
    }
}

/// moto's `DirectConnectBackend.list_tags_for_resource`:
/// `[{key, value}]`, empty when the row is absent or untagged.
fn list_tags_for_resource(st: &DxStore, arn: &str) -> Vec<Value> {
    st.tags
        .get(arn)
        .map(|row| {
            row.iter()
                .map(|(k, v)| json!({ "key": k, "value": tag_value(v.as_deref()) }))
                .collect()
        })
        .unwrap_or_default()
}

fn tag_value(value: Option<&str>) -> Value {
    match value {
        Some(v) => json!(v),
        None => Value::Null,
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn string_at<'a>(body: &'a Value, key: &str) -> Option<&'a str> {
    body.get(key).and_then(Value::as_str)
}

/// Only a present, non-empty string passes moto's `if x:` check.
fn truthy_str<'a>(body: &'a Value, key: &str) -> Option<&'a str> {
    string_at(body, key).filter(|s| !s.is_empty())
}

fn truthy_array(value: Option<&Value>) -> Option<&Vec<Value>> {
    value.and_then(Value::as_array).filter(|a| !a.is_empty())
}

/// moto truthiness: null/false/0/empty-string/empty-list/empty-object falsy.
fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(true),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

fn str_list(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|list| {
            list.iter()
                .filter_map(Value::as_str)
                .map(String::from)
                .collect()
        })
        .unwrap_or_default()
}

/// `arn:aws:directconnect:{region}:{account}:{resource}` — moto's id prefix.
fn dx_arn(region: &str, account: u64, resource: &str, id: &str) -> String {
    format!("arn:aws:directconnect:{region}:{account}:{resource}/{id}")
}

/// moto's `datetime.now().strftime("%Y-%m-%d %H:%M:%S")`.
fn dx_time() -> String {
    chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string()
}

/// moto's id timestamp `strftime("%Y%m%d%H%M%S")`.
fn dx_time_ts() -> String {
    chrono::Utc::now().format("%Y%m%d%H%M%S").to_string()
}

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

fn ok(body: Value) -> Response {
    Response::json(200, serde_json::to_vec(&body).unwrap_or_default())
}

fn ok_empty() -> Response {
    ok(json!({}))
}

fn connection_not_found(connection_id: &str, region: &str) -> Response {
    json_error(
        400,
        "ConnectionNotFound",
        &format!("{connection_id} does not match any connections in region {region}."),
    )
}

fn lag_not_found(lag_id: &str, region: &str) -> Response {
    json_error(
        400,
        "LagNotFound",
        &format!("{lag_id} does not match any LAGs in region {region}."),
    )
}

fn mac_sec_key_not_found(secret_arn: &str, connection_id: &str) -> Response {
    json_error(
        400,
        "MacSecKeyNotFound",
        &format!("Secret ARN '{secret_arn}' not found for connection {connection_id}."),
    )
}

fn connection_id_missing() -> Response {
    json_error(400, "ConnectionIdMissing", "The connection ID is missing.")
}

/// moto's crash-shaped 500 for its unguarded `NotImplementedError`.
fn internal_error_500(message: &str) -> Response {
    json_error(500, "InternalError", message)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::atomic::{AtomicU64, Ordering};

    static ACCOUNTS: AtomicU64 = AtomicU64::new(200_000_000_001);

    /// Every test takes a fresh account so the shared store stays isolated.
    fn next_account() -> u64 {
        ACCOUNTS.fetch_add(1, Ordering::SeqCst)
    }

    fn send(account: u64, operation: &str, body: Value) -> (u16, Value) {
        let request = Request {
            service: "directconnect".to_string(),
            operation: operation.to_string(),
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            account,
            region: "us-east-1".to_string(),
        };
        let resp = dispatch(&request).expect("directconnect dispatch");
        (
            resp.status,
            serde_json::from_slice(&resp.body).unwrap_or(json!({})),
        )
    }

    fn create_connection(account: u64, name: &str) -> Value {
        let (status, body) = send(
            account,
            "CreateConnection",
            json!({
                "location": "EqDC2",
                "bandwidth": "1Gbps",
                "connectionName": name,
            }),
        );
        assert_eq!(status, 200, "create failed: {body}");
        body
    }

    #[test]
    fn connection_lifecycle() {
        let account = next_account();
        let created = create_connection(account, "life-conn");
        assert_eq!(created["connectionState"], "available");
        let cid = created["connectionId"].as_str().unwrap().to_string();
        assert!(cid.starts_with("arn:aws:directconnect:us-east-1:"));
        assert!(cid.contains(":dx-con/dx-moto-life-"));
        // moto's static mock fields.
        assert_eq!(created["awsDevice"], "mock_device");
        assert_eq!(created["awsDeviceV2"], "mock_device_v2");
        assert_eq!(created["awsLogicalDeviceId"], "mock_logical_device_id");
        assert_eq!(created["partnerName"], "mock_partner");
        assert_eq!(created["portEncryptionStatus"], "Encryption Down");
        assert_eq!(created["hasLogicalRedundancy"], false);
        assert_eq!(created["jumboFrameCapable"], false);
        assert_eq!(created["vlan"], 0);
        assert_eq!(created["region"], "us-east-1");
        assert!(created["loaIssueTime"].as_str().unwrap().len() == 19);
        // lagId / providerName absent from the request → moto's null members.
        assert!(created.get("lagId").map(Value::is_null).unwrap_or(false));

        let (status, body) = send(
            account,
            "DescribeConnections",
            json!({ "connectionId": cid }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["connections"][0]["bandwidth"], "1Gbps");
        assert_eq!(body["connections"][0]["connectionName"], "life-conn");

        // Update renames; delete marks deleted but keeps the record.
        let (status, body) = send(
            account,
            "UpdateConnection",
            json!({ "connectionId": cid, "connectionName": "renamed" }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["connectionName"], "renamed");

        let (status, _) = send(account, "DeleteConnection", json!({ "connectionId": cid }));
        assert_eq!(status, 200);

        let (status, body) = send(
            account,
            "DescribeConnections",
            json!({ "connectionId": cid }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["connections"][0]["connectionState"], "deleted");
        assert_eq!(body["connections"][0]["connectionName"], "renamed");
    }

    #[test]
    fn describe_unknown_is_connection_not_found() {
        let account = next_account();
        let (status, body) = send(
            account,
            "DescribeConnections",
            json!({ "connectionId": "dxcon-nope" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ConnectionNotFound");
        assert_eq!(
            body["message"],
            "dxcon-nope does not match any connections in region us-east-1."
        );
    }

    #[test]
    fn describe_empty_lists_all() {
        let account = next_account();
        create_connection(account, "first");
        create_connection(account, "second");
        let (status, body) = send(account, "DescribeConnections", json!({}));
        assert_eq!(status, 200);
        assert_eq!(body["connections"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn accounts_are_isolated() {
        let a = next_account();
        let b = next_account();
        let a_conn = create_connection(a, "mine");
        let b_conn = create_connection(b, "mine");
        // a's record is invisible in b's account scope.
        let (status, body) = send(
            b,
            "DescribeConnections",
            json!({ "connectionId": a_conn["connectionId"] }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ConnectionNotFound");
        // Blind (no-id) describe in b lists only b's own record.
        let (_, body) = send(b, "DescribeConnections", json!({}));
        let conns = body["connections"].as_array().unwrap();
        assert_eq!(conns.len(), 1);
        assert_eq!(conns[0]["connectionId"], b_conn["connectionId"]);
    }

    #[test]
    fn create_connection_macsec_shape() {
        let account = next_account();
        let (_, plain) = send(
            account,
            "CreateConnection",
            json!({"location": "L", "bandwidth": "B", "connectionName": "plain"}),
        );
        // Absent requestMACSec renders moto's null member (botocore strips it).
        assert!(plain
            .get("macSecCapable")
            .map(|v| v.is_null())
            .unwrap_or(true));
        assert_eq!(plain["encryptionMode"], "no_encrypt");
        assert_eq!(plain["macSecKeys"], json!([]));
        assert_eq!(plain["portEncryptionStatus"], "Encryption Down");

        let (_, macsec) = send(
            account,
            "CreateConnection",
            json!({
                "location": "L",
                "bandwidth": "B",
                "connectionName": "macsec",
                "requestMACSec": true,
            }),
        );
        assert_eq!(macsec["encryptionMode"], "must_encrypt");
        assert_eq!(macsec["macSecCapable"], true);
        let key = &macsec["macSecKeys"][0];
        assert_eq!(key["secretARN"], "mock_secret_arn");
        assert_eq!(key["ckn"], "mock_ckn");
        assert_eq!(key["state"], "associated");
        assert_eq!(key["startOn"].as_str().unwrap().len(), 19);
        assert!(key.get("cak").is_none(), "cak never renders");
    }

    #[test]
    fn tag_roundtrip() {
        let account = next_account();
        // Tags on create land in the tagger and the record's own dict.
        let (_, created) = send(
            account,
            "CreateConnection",
            json!({
                "location": "L",
                "bandwidth": "B",
                "connectionName": "tagged",
                "tags": [{"key": "env", "value": "dev"}],
            }),
        );
        let cid = created["connectionId"].as_str().unwrap().to_string();
        assert_eq!(created["tags"], json!([{ "key": "env", "value": "dev" }]));

        // The tagging shows up on describe_tags too.
        let (status, body) = send(account, "DescribeTags", json!({ "resourceArns": [cid] }));
        assert_eq!(status, 200);
        assert_eq!(
            body["resourceTags"][0]["tags"],
            json!([{ "key": "env", "value": "dev" }])
        );

        send(
            account,
            "TagResource",
            json!({ "resourceArn": cid, "tags": [{"key": "k1", "value": "v1"}] }),
        );
        // TagResource adds keys; the earlier create-tag survives. An
        // untagged arn still renders a row with an empty tag list.
        let (_, body) = send(
            account,
            "DescribeTags",
            json!({ "resourceArns": [cid, "arn:unknown"] }),
        );
        assert_eq!(body["resourceTags"][0]["resourceArn"], json!(cid));
        assert_eq!(
            body["resourceTags"][0]["tags"],
            json!([
                { "key": "env", "value": "dev" },
                { "key": "k1", "value": "v1" }
            ])
        );
        assert_eq!(body["resourceTags"][1]["tags"], json!([]));

        let (_, body) = send(
            account,
            "DescribeConnections",
            json!({ "connectionId": cid }),
        );
        assert_eq!(
            body["connections"][0]["tags"],
            json!([
                { "key": "env", "value": "dev" },
                { "key": "k1", "value": "v1" }
            ])
        );

        send(
            account,
            "UntagResource",
            json!({ "resourceArn": cid, "tagKeys": ["k1"] }),
        );
        let (_, body) = send(account, "DescribeTags", json!({ "resourceArns": [cid] }));
        assert_eq!(
            body["resourceTags"][0]["tags"],
            json!([{ "key": "env", "value": "dev" }])
        );
    }

    #[test]
    fn mac_sec_associate_and_disassociate() {
        let account = next_account();
        let created = create_connection(account, "mac");
        let cid = created["connectionId"].as_str().unwrap().to_string();

        let (status, body) = send(
            account,
            "AssociateMacSecKey",
            json!({"connectionId": cid, "ckn": "ckn", "cak": "cak"}),
        );
        assert_eq!(status, 200);
        assert_eq!(body["connectionId"], json!(cid));
        assert_eq!(body["macSecKeys"][0]["secretARN"], "mock_secret_arn");
        assert_eq!(body["macSecKeys"][0]["state"], "associated");

        let (status, body) = send(
            account,
            "DisassociateMacSecKey",
            json!({"connectionId": cid, "secretARN": "mock_secret_arn"}),
        );
        assert_eq!(status, 200);
        assert_eq!(body["macSecKeys"][0]["state"], "disassociated");

        // The key list is empty now — a second disassociate is moto's
        // ConnectionNotFound.
        let (status, body) = send(
            account,
            "DisassociateMacSecKey",
            json!({"connectionId": cid, "secretARN": "arn:gone"}),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ConnectionNotFound");

        // Unknown connection on both operations.
        for op in ["AssociateMacSecKey", "DisassociateMacSecKey"] {
            let (status, body) = send(
                account,
                op,
                json!({"connectionId": "dxcon-nope", "secretARN": "s"}),
            );
            assert_eq!(status, 400);
            assert_eq!(body["__type"], "ConnectionNotFound", "{op}");
        }
    }

    #[test]
    fn mac_sec_key_not_found_error_shape() {
        let account = next_account();
        let created = create_connection(account, "mknf");
        let cid = created["connectionId"].as_str().unwrap().to_string();
        send(
            account,
            "AssociateMacSecKey",
            json!({"connectionId": cid, "secretARN": "a", "ckn": "c", "cak": "k"}),
        );
        send(
            account,
            "AssociateMacSecKey",
            json!({"connectionId": cid, "secretARN": "b", "ckn": "c", "cak": "k"}),
        );
        let (status, body) = send(
            account,
            "DisassociateMacSecKey",
            json!({"connectionId": cid, "secretARN": "gone"}),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "MacSecKeyNotFound");
        assert_eq!(
            body["message"],
            format!("Secret ARN 'gone' not found for connection {cid}.")
        );
    }

    #[test]
    fn lag_lifecycle_and_children() {
        let account = next_account();
        let (status, body) = send(
            account,
            "CreateLag",
            json!({
                "numberOfConnections": 2,
                "location": "EqDC2",
                "connectionsBandwidth": "1Gbps",
                "lagName": "life-lag",
            }),
        );
        assert_eq!(status, 200);
        let lag_id = body["lagId"].as_str().unwrap().to_string();
        assert!(lag_id.contains(":dxlag/dxlag-moto-life-lag-"));
        assert_eq!(body["lagState"], "available");
        assert_eq!(body["connectionsBandwidth"], "1Gbps");
        assert_eq!(body["numberOfConnections"], 2);
        assert_eq!(body["minimumLinks"], 0);
        assert_eq!(body["connections"].as_array().unwrap().len(), 2);
        for (i, child) in body["connections"].as_array().unwrap().iter().enumerate() {
            assert_eq!(
                child["connectionName"],
                format!("Requested Connection {} for Lag {lag_id}", i + 1)
            );
            assert_eq!(child["bandwidth"], "1Gbps");
            assert_eq!(child["lagId"], json!(lag_id));
            // Children also register at the top level, like moto's dict put.
            let (_, desc) = send(
                account,
                "DescribeConnections",
                json!({ "connectionId": child["connectionId"] }),
            );
            assert_eq!(
                desc["connections"][0]["connectionName"],
                child["connectionName"]
            );
        }

        let (status, body) = send(account, "DescribeLags", json!({ "lagId": lag_id }));
        assert_eq!(status, 200);
        assert_eq!(body["lags"][0]["lagId"], json!(lag_id));

        let (status, body) = send(account, "DescribeLags", json!({}));
        assert_eq!(status, 200);
        assert_eq!(body["lags"].as_array().unwrap().len(), 1);

        let (status, body) = send(account, "DescribeLags", json!({ "lagId": "dxlag-nope" }));
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "LagNotFound");
        assert_eq!(
            body["message"],
            "dxlag-nope does not match any LAGs in region us-east-1."
        );
    }

    #[test]
    fn lag_mac_sec_keys_propagate_to_children() {
        let account = next_account();
        let (_, lag) = send(
            account,
            "CreateLag",
            json!({
                "numberOfConnections": 1,
                "location": "L",
                "connectionsBandwidth": "B",
                "lagName": "lagkey",
            }),
        );
        let lag_id = lag["lagId"].as_str().unwrap().to_string();
        let child_id = lag["connections"][0]["connectionId"]
            .as_str()
            .unwrap()
            .to_string();

        let (status, body) = send(
            account,
            "AssociateMacSecKey",
            json!({"connectionId": lag_id, "secretARN": "arn:s", "ckn": "c", "cak": "k"}),
        );
        assert_eq!(status, 200);
        assert_eq!(body["connectionId"], json!(lag_id));
        assert_eq!(body["macSecKeys"][0]["secretARN"], "arn:s");

        // moto re-binds the member's list to the LAG's on every call.
        let (_, child) = send(
            account,
            "DescribeConnections",
            json!({ "connectionId": child_id }),
        );
        assert_eq!(
            child["connections"][0]["macSecKeys"][0]["secretARN"],
            "arn:s"
        );

        let (status, body) = send(
            account,
            "DisassociateMacSecKey",
            json!({"connectionId": lag_id, "secretARN": "arn:s"}),
        );
        assert_eq!(status, 200);
        assert_eq!(body["macSecKeys"][0]["state"], "disassociated");

        // The pop drops it from the LAG and its members (shared-list alias).
        let (_, child) = send(
            account,
            "DescribeConnections",
            json!({ "connectionId": child_id }),
        );
        assert_eq!(child["connections"][0]["macSecKeys"], json!([]));
    }

    #[test]
    fn lag_child_tags_and_macsec_propagation() {
        let account = next_account();
        let (_, lag) = send(
            account,
            "CreateLag",
            json!({
                "numberOfConnections": 1,
                "location": "L",
                "connectionsBandwidth": "B",
                "lagName": "mlag",
                "requestMACSec": true,
                "tags": [{"key": "lt", "value": "lv"}],
                "childConnectionTags": [{"key": "ct", "value": "cv"}],
            }),
        );
        assert_eq!(lag["encryptionMode"], "must_encrypt");
        assert_eq!(lag["macSecCapable"], true);
        assert_eq!(lag["macSecKeys"][0]["secretARN"], "mock_secret_arn");
        assert_eq!(lag["tags"], json!([{ "key": "lt", "value": "lv" }]));
        let child = &lag["connections"][0];
        assert_eq!(child["tags"], json!([{ "key": "ct", "value": "cv" }]));
        assert_eq!(child["macSecCapable"], true);
        assert_eq!(child["encryptionMode"], "must_encrypt");
        assert_eq!(child["macSecKeys"][0]["secretARN"], "mock_secret_arn");
    }

    #[test]
    fn lag_with_connection_id_is_moto_500() {
        let account = next_account();
        let created = create_connection(account, "lagcid");
        let cid = created["connectionId"].as_str().unwrap().to_string();
        let (status, body) = send(
            account,
            "CreateLag",
            json!({
                "numberOfConnections": 1,
                "location": "L",
                "connectionsBandwidth": "B",
                "lagName": "x",
                "connectionId": cid,
            }),
        );
        assert_eq!(status, 500);
        assert_eq!(body["__type"], "InternalError");
        assert_eq!(
            body["message"],
            "creating a lag with a connection_id is not currently supported by moto"
        );
    }

    #[test]
    fn update_connection_unknown_is_not_found() {
        let account = next_account();
        let (status, body) = send(
            account,
            "UpdateConnection",
            json!({ "connectionId": "dxcon-nope", "connectionName": "n" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ConnectionNotFound");
        assert_eq!(
            body["message"],
            "dxcon-nope does not match any connections in region us-east-1."
        );
    }

    #[test]
    fn unsupported_operations_answer_501() {
        let account = next_account();
        let (status, body) = send(account, "DescribeLocations", json!({}));
        assert_eq!(status, 501);
        assert_eq!(body["__type"], "NotImplemented");
        assert_eq!(
            body["message"],
            "The DescribeLocations action has not been implemented"
        );

        let (status, body) = send(
            account,
            "CreatePrivateVirtualInterface",
            json!({ "connectionId": "dxcon-x", "newPrivateVirtualInterface": {} }),
        );
        assert_eq!(status, 501);
        assert_eq!(
            body["message"],
            "The CreatePrivateVirtualInterface action has not been implemented"
        );
    }

    #[test]
    fn regions_are_isolated() {
        let account = next_account();
        let request_body = json!({"location": "L", "bandwidth": "B", "connectionName": "r"});
        let request = |region: &str| Request {
            service: "directconnect".to_string(),
            operation: "CreateConnection".to_string(),
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&request_body).unwrap()),
            account,
            region: region.to_string(),
        };
        let east = serde_json::from_slice::<Value>(
            &dispatch(&request("us-east-1")).expect("dispatch").body,
        )
        .unwrap();
        let west = serde_json::from_slice::<Value>(
            &dispatch(&request("eu-west-1")).expect("dispatch").body,
        )
        .unwrap();
        assert_eq!(east["region"], "us-east-1");
        assert_eq!(west["region"], "eu-west-1");
        // west's record is invisible in the us-east-1 scope.
        let (status, body) = send(
            account,
            "DescribeConnections",
            json!({ "connectionId": west["connectionId"] }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ConnectionNotFound");
        // Region-scoped stores: west's id embeds its region, so identical
        // requests in two regions are distinct records.
        let (_, body) = send(account, "DescribeConnections", json!({}));
        let conns = body["connections"].as_array().unwrap();
        assert_eq!(conns.len(), 1);
        assert_eq!(conns[0]["connectionId"], east["connectionId"]);
    }
}
