//! Managed Blockchain backend (rest-json).
//!
//! Port of moto's `managedblockchain`: networks, members, proposals,
//! invitations, nodes and accessors per (account, region). Response shapes
//! are copied wire-exactly from the moto-bridge (the compat suite asserts
//! member names), including its serializer's key sets.

use std::collections::HashMap;

use crate::proto::parse_json_body;
use crate::store::{self, ScopeKey};
use crate::{Request, Response};
use serde_json::{json, Value};

// --- state -------------------------------------------------------------------

/// Per-(account, region) state; Vecs preserve insertion order like moto's dicts.
#[derive(Default)]
pub struct BlockchainState {
    networks: Vec<Network>,
    members: Vec<Member>,
    proposals: Vec<Proposal>,
    invitations: Vec<Invitation>,
    nodes: Vec<Node>,
    accessors: Vec<Accessor>,
}

struct Network {
    id: String,
    name: String,
    description: Option<String>,
    edition: String,
    voting_policy: Value,
}

struct Member {
    id: String,
    network_id: String,
    name: String,
    description: Option<String>,
    admin_username: String,
    log_publishing_configuration: Value,
    status: &'static str,
}

struct Proposal {
    id: String,
    network_id: String,
    status: &'static str,
    yes_vote_count: u64,
    no_vote_count: u64,
    /// Total members in the network when the proposal was created.
    num_members: u64,
    outstanding_vote_count: u64,
    threshold_percentage: f64,
    threshold_comparator: String,
    /// member id → {"MemberId", "MemberName", "Vote"}
    votes: Vec<(String, Value)>,
    /// Pending proposal actions (Invitations / Removals), applied on approval.
    invitations_to_create: usize,
    removals: Vec<String>,
}

struct Invitation {
    id: String,
    network_id: String,
    network_name: String,
    network_framework: String,
    network_creation_date: String,
    status: &'static str,
    network_status: &'static str,
}

struct Node {
    id: String,
    network_id: String,
    member_id: String,
    instance_type: String,
    availability_zone: String,
    log_publishing_configuration: Value,
    status: &'static str,
    creation_date: String,
}

struct Accessor {
    id: String,
    accessor_type: String,
    billing_token: String,
    tags: HashMap<String, String>,
    network_type: String,
    creation_date: String,
}

// --- ids & stamps -------------------------------------------------------------

/// `{prefix} + 26 random uppercase alphanumerics` (moto's id helpers).
fn prefixed_id(prefix: &str) -> String {
    const ALPHABET: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let mut out = String::with_capacity(prefix.len() + 26);
    out.push_str(prefix);
    while out.len() < prefix.len() + 26 {
        let uuid = uuid::Uuid::new_v4();
        for b in uuid.as_bytes() {
            if out.len() == prefix.len() + 26 {
                break;
            }
            out.push(ALPHABET[(*b % 36) as usize] as char);
        }
    }
    out
}

/// `a-` + 12 uppercase hex (moto's `get_random_hex(12).upper()`).
fn accessor_id() -> String {
    const HEX: &[u8] = b"0123456789ABCDEF";
    let mut out = String::from("a-");
    while out.len() < 14 {
        let uuid = uuid::Uuid::new_v4();
        for b in uuid.as_bytes() {
            if out.len() == 14 {
                break;
            }
            out.push(HEX[(*b % 16) as usize] as char);
        }
    }
    out
}

fn now_stamp_aware() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%d %H:%M:%S%.6f+00:00")
        .to_string()
}

fn now_stamp_naive() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%d %H:%M:%S%.6f")
        .to_string()
}

// --- wire helpers ---------------------------------------------------------------

pub type MbError = (u16, &'static str, String);

fn ok_json(value: &Value) -> Response {
    Response::new(
        200,
        "application/json",
        serde_json::to_vec(value).unwrap_or_default(),
    )
}

fn ok_json_body(value: Value) -> Response {
    Response::new(
        200,
        "application/json",
        serde_json::to_vec(&value).unwrap_or_default(),
    )
}

fn ok_empty() -> Response {
    Response::new(200, "application/json", Vec::new())
}

/// moto's JsonRESTError shape: message embeds code + operation, code goes out
/// via the `x-amzn-ErrorType` header.
fn error_resp(status: u16, code: &str, op: &str, msg: &str) -> Response {
    let full = format!("An error occurred ({code}) when calling the {op} operation: {msg}");
    Response {
        status,
        headers: vec![
            ("Content-Type".into(), "application/json".into()),
            ("x-amzn-ErrorType".into(), code.into()),
        ],
        body: serde_json::to_vec(&json!({ "message": full })).unwrap_or_default(),
    }
}

fn not_found(op: &str, msg: String) -> MbError {
    (404, "ResourceNotFoundException", msg_with_op(op, msg))
}

fn bad_request(op: &str, msg: String) -> MbError {
    (400, "BadRequestException", msg_with_op(op, msg))
}

fn invalid_request(op: &str, msg: String) -> MbError {
    (400, "InvalidRequestException", msg_with_op(op, msg))
}

fn already_exists(op: &str, msg: String) -> MbError {
    (409, "ResourceAlreadyExistsException", msg_with_op(op, msg))
}

fn limit_exceeded(op: &str, msg: String) -> MbError {
    (429, "ResourceLimitExceededException", msg_with_op(op, msg))
}

fn msg_with_op(op: &str, msg: String) -> String {
    format!("{op}\u{1}{msg}")
}

// --- dispatch -------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Response {
    let body = parse_json_body(&req.body);
    match handle(req, &body) {
        Ok(resp) => resp,
        Err((status, code, msg)) => {
            let (op, detail) = msg.split_once('\u{1}').unwrap_or(("", msg.as_str()));
            error_resp(status, code, op, detail)
        }
    }
}

/// REST routing by method + path segments (the gateway derives REST
/// operations from method+path, matching moto's routing for this service).
fn handle(req: &Request, body: &Value) -> Result<Response, MbError> {
    let segs: Vec<&str> = req
        .path
        .trim_start_matches('/')
        .trim_end_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();
    let method = req.method.as_str();

    let slot = store::slot::<BlockchainState>("managedblockchain");
    let mut scopes = slot.write();
    let scope = scopes.entry((req.account, req.region.clone())).or_default();

    match (method, segs.as_slice()) {
        // --- Accessors ---
        ("POST", ["accessors"]) => Ok(create_accessor(scope, body)),
        ("GET", ["accessors", aid]) => Ok(get_accessor(scope, aid)),
        ("DELETE", ["accessors", aid]) => delete_accessor(scope, aid),
        ("GET", ["accessors"]) => Ok(list_accessors(scope)),
        // --- Accessor tags (ARN contains slashes; botocore may percent-encode) ---
        ("GET", ["tags", ..]) => {
            let arn = urldecode(&tail_of(&req.path, "/tags/"));
            Ok(list_tags(scope, &arn))
        }
        ("POST", ["tags", ..]) => {
            let arn = urldecode(&tail_of(&req.path, "/tags/"));
            Ok(tag_resource(scope, &arn, body))
        }
        ("DELETE", ["tags", ..]) => {
            let arn = urldecode(&tail_of(&req.path, "/tags/"));
            Ok(untag_resource(
                scope,
                &arn,
                query_values(&req.query_string, "tagKeys"),
            ))
        }
        // --- Invitations ---
        ("GET", ["invitations"]) => Ok(list_invitations(scope)),
        ("DELETE", ["invitations", iid]) => reject_invitation(scope, iid),
        // --- Networks ---
        ("POST", ["networks"]) => create_network(scope, body),
        ("GET", ["networks"]) => Ok(list_networks(scope)),
        ("GET", ["networks", nid]) => get_network(scope, nid, &req.region),
        // --- Members ---
        ("POST", ["networks", nid, "members"]) => create_member(scope, nid, body),
        ("GET", ["networks", nid, "members", mid]) => get_member(scope, nid, mid, &req.region),
        ("PATCH", ["networks", nid, "members", mid]) => update_member(scope, nid, mid, body),
        ("DELETE", ["networks", nid, "members", mid]) => delete_member(scope, nid, mid),
        ("GET", ["networks", nid, "members"]) => list_members(scope, nid),
        // --- Nodes (memberId rides in the body/query for modern botocore) ---
        ("POST", ["networks", nid, "nodes"]) => create_node(scope, nid, body),
        ("GET", ["networks", nid, "nodes"]) => {
            list_nodes(scope, nid, rid_member(req, body).as_deref())
        }
        ("GET", ["networks", nid, "nodes", ndid]) => get_node(
            scope,
            nid,
            rid_member(req, body).as_deref(),
            ndid,
            &req.region,
        ),
        ("PATCH", ["networks", nid, "nodes", ndid]) => {
            update_node(scope, nid, rid_member(req, body).as_deref(), ndid, body)
        }
        ("DELETE", ["networks", nid, "nodes", ndid]) => {
            delete_node(scope, nid, rid_member(req, body).as_deref(), ndid)
        }
        // --- Proposals ---
        ("POST", ["networks", nid, "proposals"]) => create_proposal(scope, nid, body),
        ("GET", ["networks", nid, "proposals"]) => list_proposals(scope, nid),
        ("GET", ["networks", nid, "proposals", pid]) => get_proposal(scope, nid, pid),
        ("GET", ["networks", nid, "proposals", pid, "votes"]) => {
            list_proposal_votes(scope, nid, pid)
        }
        ("POST", ["networks", nid, "proposals", pid, "votes"]) => {
            vote_on_proposal(scope, nid, pid, body)
        }
        _ => Err(bad_request(
            "ManagedBlockchain",
            format!("Unsupported route {} {}", method, req.path),
        )),
    }
}

fn tail_of(path: &str, marker: &str) -> String {
    path.split_once(marker)
        .map(|(_, tail)| tail.to_string())
        .unwrap_or_default()
}

/// memberId lives in the query (`memberId=`) or the JSON body for >= botocore
/// 1.19.41.
fn rid_member(req: &Request, body: &Value) -> Option<String> {
    req.query_string
        .split('&')
        .find_map(|kv| {
            let (k, v) = kv.split_once('=')?;
            (k == "memberId").then(|| urldecode(v))
        })
        .or_else(|| {
            body.get("MemberId")
                .and_then(|m| m.as_str())
                .map(String::from)
        })
}

fn query_values(query_string: &str, key: &str) -> Vec<String> {
    query_string
        .split('&')
        .filter_map(|kv| {
            let (k, v) = kv.split_once('=')?;
            if k == key || k == format!("{key}[]") {
                Some(urldecode(v))
            } else {
                None
            }
        })
        .collect()
}

fn urldecode(v: &str) -> String {
    let bytes = v.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("25");
                out.push(u8::from_str_radix(hex, 16).unwrap_or(b'?'));
                i += 3;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

// --- accessors ------------------------------------------------------------------

fn create_accessor(scope: &mut BlockchainState, body: &Value) -> Response {
    let id = accessor_id();
    let accessor_type = body
        .get("AccessorType")
        .and_then(|v| v.as_str())
        .unwrap_or("BILLING_TOKEN")
        .to_string();
    let network_type = body
        .get("NetworkType")
        .and_then(|v| v.as_str())
        .unwrap_or("ETHEREUM_GOERLI")
        .to_string();
    let tags: HashMap<String, String> = body
        .get("Tags")
        .and_then(|v| v.as_object())
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default();
    scope.accessors.push(Accessor {
        id: id.clone(),
        accessor_type,
        billing_token: uuid::Uuid::new_v4().to_string(),
        tags,
        network_type,
        creation_date: now_stamp_naive(),
    });
    ok_json(
        &json!({ "AccessorId": id, "BillingToken": scope.accessors.last().unwrap().billing_token }),
    )
}

fn find_accessor<'a>(scope: &'a BlockchainState, id: &str) -> Result<&'a Accessor, MbError> {
    scope
        .accessors
        .iter()
        .find(|a| a.id == id)
        .ok_or_else(|| not_found("GetAccessor", format!("Accessor {id} not found.")))
}

fn get_accessor(scope: &mut BlockchainState, id: &str) -> Response {
    match find_accessor(scope, id) {
        Ok(a) => ok_json(&json!({
            "Accessor": {
                "Id": a.id,
                "Type": a.accessor_type,
                "BillingToken": a.billing_token,
                "Status": "AVAILABLE",
                "CreationDate": a.creation_date,
                "Arn": accessor_arn(&a.id),
                "Tags": a.tags,
                "NetworkType": a.network_type,
            }
        })),
        Err(e) => return_err(e),
    }
}

fn delete_accessor(scope: &mut BlockchainState, id: &str) -> Result<Response, MbError> {
    match scope.accessors.iter().position(|a| a.id == id) {
        Some(idx) => {
            scope.accessors.remove(idx);
            Ok(ok_empty())
        }
        None => Err(not_found(
            "DeleteAccessor",
            format!("Accessor {id} not found."),
        )),
    }
}

fn list_accessors(scope: &BlockchainState) -> Response {
    let list: Vec<Value> = scope
        .accessors
        .iter()
        .map(|a| {
            json!({
                "Id": a.id,
                "Type": a.accessor_type,
                "Status": "AVAILABLE",
                "CreationDate": a.creation_date,
                "Arn": accessor_arn(&a.id),
            })
        })
        .collect();
    ok_json(&json!({ "Accessors": list }))
}

fn accessor_arn(id: &str) -> String {
    format!("arn:aws:managedblockchain:us-east-1:123456789012:accessors/{id}")
}

// --- accessor tags ------------------------------------------------------------------

fn list_tags(scope: &mut BlockchainState, arn: &str) -> Response {
    match scope.accessors.iter().find(|a| accessor_arn(&a.id) == arn) {
        Some(a) => Response::new(
            200,
            "application/json",
            serde_json::to_vec(&json!({ "Tags": a.tags })).unwrap_or_default(),
        ),
        None => return_err(not_found(
            "ListTagsForResource",
            format!("Resource {arn} not found."),
        )),
    }
}

fn tag_resource(scope: &mut BlockchainState, arn: &str, body: &Value) -> Response {
    let new_tags: Vec<(String, String)> = body
        .get("Tags")
        .and_then(|v| v.as_object())
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default();
    match scope
        .accessors
        .iter_mut()
        .find(|a| accessor_arn(&a.id) == arn)
    {
        Some(a) => {
            for (k, v) in new_tags {
                a.tags.insert(k, v);
            }
            ok_empty2()
        }
        None => return_err(not_found(
            "TagResource",
            format!("Resource {arn} not found."),
        )),
    }
}

fn untag_resource(scope: &mut BlockchainState, arn: &str, keys: Vec<String>) -> Response {
    match scope
        .accessors
        .iter_mut()
        .find(|a| accessor_arn(&a.id) == arn)
    {
        Some(a) => {
            for k in &keys {
                a.tags.remove(k);
            }
            ok_empty2()
        }
        None => return_err(not_found(
            "UntagResource",
            format!("Resource {arn} not found."),
        )),
    }
}

fn ok_empty2() -> Response {
    Response::new(200, "application/json", Vec::new())
}

/// Bridge the Result-style ops into plain responses where storage lives
/// behind a mutable borrow (cannot `?` them in the dispatcher match).
fn return_err(e: MbError) -> Response {
    let (status, code, msg) = e;
    let (op, detail) = msg.split_once('\u{1}').unwrap_or(("", msg.as_str()));
    error_resp(status, code, op, detail)
}

// --- networks -------------------------------------------------------------------------

fn create_network(scope: &mut BlockchainState, body: &Value) -> Result<Response, MbError> {
    let framework = body
        .get("Framework")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    if framework != "HYPERLEDGER_FABRIC" {
        return Err(bad_request(
            "CreateNetwork",
            "Invalid request body".to_string(),
        ));
    }
    let version = body
        .get("FrameworkVersion")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    if version != "1.2" {
        return Err(bad_request(
            "CreateNetwork",
            format!("Invalid version {version} requested for framework HYPERLEDGER_FABRIC"),
        ));
    }
    let edition = body["FrameworkConfiguration"]["Fabric"]["Edition"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    if edition != "STARTER" && edition != "STANDARD" {
        return Err(bad_request(
            "CreateNetwork",
            "Invalid request body".to_string(),
        ));
    }
    let network_id = prefixed_id("n-");
    let member_id = prefixed_id("m-");
    let member_configuration = body
        .get("MemberConfiguration")
        .cloned()
        .unwrap_or(json!({}));
    scope.members.push(Member {
        id: member_id.clone(),
        network_id: network_id.clone(),
        name: member_configuration["Name"]
            .as_str()
            .unwrap_or_default()
            .to_string(),
        description: member_configuration
            .get("Description")
            .and_then(|v| v.as_str())
            .map(String::from),
        admin_username: member_configuration["FrameworkConfiguration"]["Fabric"]["AdminUsername"]
            .as_str()
            .unwrap_or_default()
            .to_string(),
        log_publishing_configuration: member_configuration
            .get("LogPublishingConfiguration")
            .cloned()
            .unwrap_or(json!({})),
        status: "AVAILABLE",
    });
    scope.networks.push(Network {
        id: network_id.clone(),
        name: body
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string(),
        description: body
            .get("Description")
            .and_then(|v| v.as_str())
            .map(String::from),
        edition,
        voting_policy: body.get("VotingPolicy").cloned().unwrap_or(json!({})),
    });
    Ok(ok_json(
        &json!({ "NetworkId": network_id, "MemberId": member_id }),
    ))
}

fn find_network<'a>(
    scope: &'a BlockchainState,
    id: &str,
    op: &str,
) -> Result<&'a Network, MbError> {
    scope
        .networks
        .iter()
        .find(|n| n.id == id)
        .ok_or_else(|| not_found(op, format!("Network {id} not found.")))
}

fn get_network(scope: &mut BlockchainState, id: &str, region: &str) -> Result<Response, MbError> {
    let n = find_network(scope, id, "GetNetwork")?;
    let out = json!({
        "Network": {
            "Id": n.id,
            "Name": n.name,
            "Framework": "HYPERLEDGER_FABRIC",
            "FrameworkVersion": "1.2",
            "FrameworkAttributes": {
                "Fabric": {
                    "OrderingServiceEndpoint":
                        format!("orderer.{}.managedblockchain.{region}.amazonaws.com:30001", n.id.to_lowercase()),
                    "Edition": n.edition,
                }
            },
            "VotingPolicy": n.voting_policy,
        }
    });
    Ok(Response::new(
        200,
        "application/json",
        serde_json::to_vec(&out).unwrap_or_default(),
    ))
}

fn list_networks(scope: &BlockchainState) -> Response {
    let list: Vec<Value> = scope
        .networks
        .iter()
        .map(|n| {
            json!({
                "Id": n.id,
                "Name": n.name,
                "Framework": "HYPERLEDGER_FABRIC",
                "FrameworkVersion": "1.2",
            })
        })
        .collect();
    ok_json(&json!({ "Networks": list }))
}

// --- members ----------------------------------------------------------------------------

fn member_out_full(m: &Member, network_id: &str, region: &str) -> Value {
    let mut out = json!({
        "NetworkId": network_id,
        "Id": m.id,
        "Name": m.name,
        "FrameworkAttributes": {
            "Fabric": {
                "AdminUsername": m.admin_username,
                "CaEndpoint": format!(
                    "ca.{}.{}.managedblockchain.{region}.amazonaws.com:30002",
                    m.id.to_lowercase(),
                    network_id.to_lowercase(),
                ),
            }
        },
        "LogPublishingConfiguration": m.log_publishing_configuration,
        "Status": m.status,
    });
    if let Some(desc) = &m.description {
        out["Description"] = Value::String(desc.clone());
    }
    out
}

fn get_member(
    scope: &mut BlockchainState,
    nid: &str,
    mid: &str,
    region: &str,
) -> Result<Response, MbError> {
    find_network(scope, nid, "GetMember")?;
    let m = scope
        .members
        .iter()
        .find(|m| m.id == mid)
        .ok_or_else(|| not_found("GetMember", format!("Member {mid} not found.")))?;
    // A deleted member cannot be fetched (it still shows up in lists).
    if m.status == "DELETED" {
        return Err(not_found("GetMember", format!("Member {mid} not found.")));
    }
    let out = json!({ "Member": member_out_full(m, nid, region) });
    Ok(Response::new(
        200,
        "application/json",
        serde_json::to_vec(&out).unwrap_or_default(),
    ))
}

fn list_members(scope: &mut BlockchainState, nid: &str) -> Result<Response, MbError> {
    find_network(scope, nid, "ListMembers")?;
    let list: Vec<Value> = scope
        .members
        .iter()
        .filter(|m| m.network_id == nid)
        .map(|m| {
            let mut out = json!({
                "Id": m.id,
                "Name": m.name,
                "Status": m.status,
            });
            if let Some(desc) = &m.description {
                out["Description"] = Value::String(desc.clone());
            }
            out
        })
        .collect();
    Ok(ok_json(&json!({ "Members": list })))
}

fn update_member(
    scope: &mut BlockchainState,
    nid: &str,
    mid: &str,
    body: &Value,
) -> Result<Response, MbError> {
    find_network(scope, nid, "UpdateMember")?;
    let member = scope
        .members
        .iter_mut()
        .find(|m| m.id == mid)
        .ok_or_else(|| not_found("UpdateMember", format!("Member {mid} not found.")))?;
    if let Some(lpc) = body.get("LogPublishingConfiguration") {
        member.log_publishing_configuration = lpc.clone();
    }
    Ok(ok_empty())
}

fn delete_member(scope: &mut BlockchainState, nid: &str, mid: &str) -> Result<Response, MbError> {
    find_network(scope, nid, "DeleteMember")?;
    let member = scope
        .members
        .iter_mut()
        .find(|m| m.id == mid)
        .ok_or_else(|| not_found("DeleteMember", format!("Member {mid} not found.")))?;
    member.status = "DELETED";
    // Last member deleted → network goes away, invitations marked DELETED.
    if scope.members.iter().all(|m| m.status == "DELETED") {
        for inv in scope.invitations.iter_mut() {
            if inv.network_id == nid {
                inv.network_status = "DELETED";
            }
        }
        scope.networks.retain(|n| n.id != nid);
    }
    scope.nodes.retain(|n| n.member_id != mid);
    Ok(ok_empty())
}

fn create_member(
    scope: &mut BlockchainState,
    nid: &str,
    body: &Value,
) -> Result<Response, MbError> {
    find_network(scope, nid, "CreateMember")?;
    let invitation_id = body
        .get("InvitationId")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    let invite = scope
        .invitations
        .iter()
        .find(|i| i.id == invitation_id)
        .ok_or_else(|| {
            invalid_request(
                "CreateMember",
                format!("Invitation {invitation_id} not valid"),
            )
        })?;
    if invite.status != "PENDING" {
        return Err(invalid_request(
            "CreateMember",
            format!("Invitation {invitation_id} not valid"),
        ));
    }
    let member_configuration = body
        .get("MemberConfiguration")
        .cloned()
        .unwrap_or(json!({}));
    let name = member_configuration["Name"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    if scope
        .members
        .iter()
        .any(|m| m.network_id == nid && m.name == name)
    {
        return Err(invalid_request(
            "CreateMember",
            format!("Member name {name} already exists in network {nid}."),
        ));
    }
    // STARTER edition cap (the only edition this suite exercises beyond mocks).
    if scope.members.iter().filter(|m| m.network_id == nid).count() >= EDITION_MAX_MEMBERS {
        return Err(limit_exceeded(
            "CreateMember",
            format!(
                "You cannot create a member in network {nid}.{EDITION_MAX_MEMBERS} is the maximum number of members allowed in a STARTER Edition network."
            ),
        ));
    }
    let admin_password = member_configuration["FrameworkConfiguration"]["Fabric"]["AdminPassword"]
        .as_str()
        .unwrap_or_default();
    if !admin_password_ok(admin_password) {
        return Err(bad_request(
            "CreateMember",
            "Invalid request body".to_string(),
        ));
    }
    // Accept the invitation.
    if let Some(inv) = scope.invitations.iter_mut().find(|i| i.id == invitation_id) {
        inv.status = "ACCEPTED";
    }
    let member_id = prefixed_id("m-");
    scope.members.push(Member {
        id: member_id.clone(),
        network_id: nid.to_string(),
        name,
        description: member_configuration
            .get("Description")
            .and_then(|v| v.as_str())
            .map(String::from),
        admin_username: member_configuration["FrameworkConfiguration"]["Fabric"]["AdminUsername"]
            .as_str()
            .unwrap_or_default()
            .to_string(),
        log_publishing_configuration: member_configuration
            .get("LogPublishingConfiguration")
            .cloned()
            .unwrap_or(json!({})),
        status: "AVAILABLE",
    });
    Ok(ok_json(&json!({ "MemberId": member_id })))
}

const EDITION_MAX_MEMBERS: usize = 5;
const EDITION_MAX_NODES: usize = 2;

/// moto's `admin_password_ok`: needs upper, lower and digit; no quote/slash/@.
fn admin_password_ok(password: &str) -> bool {
    password.chars().any(|c| c.is_ascii_lowercase())
        && password.chars().any(|c| c.is_ascii_uppercase())
        && password.chars().any(|c| c.is_ascii_digit())
        && !password.contains(['"', '\'', '@', '\\', '/'])
}

// --- proposals -------------------------------------------------------------------------------

fn create_proposal(
    scope: &mut BlockchainState,
    nid: &str,
    body: &Value,
) -> Result<Response, MbError> {
    find_network(scope, nid, "CreateProposal")?;
    let member_id = body
        .get("MemberId")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    scope
        .members
        .iter()
        .find(|m| m.id == member_id)
        .ok_or_else(|| not_found("CreateProposal", format!("Member {member_id} not found.")))?;
    let actions = body.get("Actions").cloned().unwrap_or(json!({}));
    if let Some(invitations) = actions.get("Invitations").and_then(|v| v.as_array()) {
        for inv in invitations {
            let principal = inv
                .get("Principal")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            if !valid_aws_account(principal) {
                return Err(invalid_request(
                    "CreateProposal",
                    "Account ID format specified in proposal is not valid.".to_string(),
                ));
            }
        }
    }
    if let Some(removals) = actions.get("Removals").and_then(|v| v.as_array()) {
        for rm in removals {
            let mid = rm
                .get("MemberId")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            if !scope.members.iter().any(|m| m.id == mid) {
                return Err(invalid_request(
                    "CreateProposal",
                    "Member ID format specified in proposal is not valid.".to_string(),
                ));
            }
        }
    }
    let network = scope
        .networks
        .iter()
        .find(|n| n.id == nid)
        .ok_or_else(|| not_found("CreateProposal", format!("Network {nid} not found.")))?;
    let threshold = network.voting_policy["ApprovalThresholdPolicy"]["ThresholdPercentage"]
        .as_f64()
        .unwrap_or(0.0);
    let comparator = network.voting_policy["ApprovalThresholdPolicy"]["ThresholdComparator"]
        .as_str()
        .unwrap_or("GREATER_THAN")
        .to_string();
    let num_members = scope.members.iter().filter(|m| m.network_id == nid).count() as u64;
    let proposal_id = prefixed_id("p-");
    let invitations_to_create = actions
        .get("Invitations")
        .and_then(|v| v.as_array())
        .map(|a| a.len())
        .unwrap_or(0);
    let removals = actions
        .get("Removals")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|rm| {
                    rm.get("MemberId")
                        .and_then(|m| m.as_str())
                        .map(String::from)
                })
                .collect()
        })
        .unwrap_or_default();
    scope.proposals.push(Proposal {
        id: proposal_id.clone(),
        network_id: nid.to_string(),
        status: "IN_PROGRESS",
        yes_vote_count: 0,
        no_vote_count: 0,
        num_members,
        outstanding_vote_count: num_members,
        threshold_percentage: threshold,
        threshold_comparator: comparator,
        votes: Vec::new(),
        invitations_to_create,
        removals,
    });
    Ok(ok_json(&json!({ "ProposalId": proposal_id })))
}

/// moto validates with `re.match("[0-9]{12}", principal)` — 12 leading digits.
fn valid_aws_account(principal: &str) -> bool {
    principal.len() >= 12
        && principal.as_bytes()[..12]
            .iter()
            .all(|b| b.is_ascii_digit())
}

fn list_proposals(scope: &mut BlockchainState, nid: &str) -> Result<Response, MbError> {
    find_network(scope, nid, "ListProposals")?;
    let list: Vec<Value> = scope
        .proposals
        .iter()
        .filter(|p| p.network_id == nid)
        .map(|p| json!({ "ProposalId": p.id, "Status": p.status }))
        .collect();
    Ok(ok_json(&json!({ "Proposals": list })))
}

fn get_proposal(scope: &mut BlockchainState, nid: &str, pid: &str) -> Result<Response, MbError> {
    find_network(scope, nid, "GetProposal")?;
    let p = scope
        .proposals
        .iter()
        .find(|p| p.id == pid)
        .ok_or_else(|| not_found("GetProposal", format!("Proposal {pid} not found.")))?;
    // moto's serializer emits an empty Actions dict for proposals.
    let out = json!({
        "Proposal": {
            "ProposalId": p.id,
            "NetworkId": p.network_id,
            "Actions": {},
            "Status": p.status,
            "YesVoteCount": p.yes_vote_count,
            "NoVoteCount": p.no_vote_count,
            "OutstandingVoteCount": p.outstanding_vote_count,
        }
    });
    Ok(Response::new(
        200,
        "application/json",
        serde_json::to_vec(&out).unwrap_or_default(),
    ))
}

fn vote_on_proposal(
    scope: &mut BlockchainState,
    nid: &str,
    pid: &str,
    body: &Value,
) -> Result<Response, MbError> {
    find_network(scope, nid, "VoteOnProposal")?;
    let voter = body
        .get("VoterMemberId")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    let vote = body
        .get("Vote")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    {
        scope
            .proposals
            .iter()
            .find(|p| p.id == pid)
            .ok_or_else(|| not_found("VoteOnProposal", format!("Proposal {pid} not found.")))?;
    }
    scope
        .members
        .iter()
        .find(|m| m.id == voter)
        .ok_or_else(|| not_found("VoteOnProposal", format!("Member {voter} not found.")))?;
    let vote_upper = vote.to_uppercase();
    if vote_upper != "YES" && vote_upper != "NO" {
        return Err(bad_request(
            "VoteOnProposal",
            "Invalid request body".to_string(),
        ));
    }
    let member_name = scope
        .members
        .iter()
        .find(|m| m.id == voter)
        .map(|m| m.name.clone())
        .unwrap_or_default();

    let mut approved = false;
    {
        let proposals = scope.proposals.iter_mut().filter(|p| p.id == pid);
        for p in proposals {
            if p.status != "IN_PROGRESS" {
                return Err(invalid_request(
                    "VoteOnProposal",
                    format!(
                        "Proposal {pid} has status {} and you cannot vote on it.",
                        p.status
                    ),
                ));
            }
            if p.votes.iter().any(|(mid, _)| mid == voter) {
                return Err(already_exists(
                    "VoteOnProposal",
                    format!("Member {voter} has already voted on proposal {pid}."),
                ));
            }
            if vote_upper == "YES" {
                p.yes_vote_count += 1;
            } else {
                p.no_vote_count += 1;
            }
            p.outstanding_vote_count = p.outstanding_vote_count.saturating_sub(1);
            p.votes.push((
                voter.to_string(),
                json!({ "MemberId": voter, "MemberName": member_name, "Vote": vote_upper }),
            ));
            // moto: vote recorded as {MemberId, MemberName: voter name, Vote}.
        }
        // status update
        let prop = scope.proposals.iter_mut().find(|p| p.id == pid).unwrap();
        let total = prop.num_members.max(1) as f64;
        let perct_yes = (prop.yes_vote_count as f64 / total) * 100.0;
        let perct_no = (prop.no_vote_count as f64 / total) * 100.0;
        if prop.threshold_comparator == "GREATER_THAN_OR_EQUAL_TO" {
            if perct_yes >= prop.threshold_percentage {
                prop.status = "APPROVED";
            } else if perct_no >= prop.threshold_percentage {
                prop.status = "REJECTED";
            }
        } else {
            if perct_yes > prop.threshold_percentage {
                prop.status = "APPROVED";
            } else if perct_no > prop.threshold_percentage {
                prop.status = "REJECTED";
            }
        }
        approved = prop.status == "APPROVED";
    }
    if approved {
        let mut invitations: Vec<(String, Invitation)> = Vec::new();
        let (net_name, net_framework, net_creation) = {
            let n = scope.networks.iter().find(|n| n.id == nid);
            match n {
                Some(n) => (
                    n.name.clone(),
                    "HYPERLEDGER_FABRIC".to_string(),
                    now_stamp_aware(),
                ),
                None => (
                    String::new(),
                    "HYPERLEDGER_FABRIC".to_string(),
                    now_stamp_aware(),
                ),
            }
        };
        let count = scope
            .proposals
            .iter()
            .filter(|p| p.id == pid)
            .map(|p| p.invitations_to_create)
            .max()
            .unwrap_or(0);
        for _ in 0..count {
            invitations.push((
                nid.to_string(),
                Invitation {
                    id: prefixed_id("in-"),
                    network_id: nid.to_string(),
                    network_name: net_name.clone(),
                    network_framework: net_framework.clone(),
                    network_creation_date: net_creation.clone(),
                    status: "PENDING",
                    network_status: "AVAILABLE",
                },
            ));
        }
        for (_, inv) in invitations {
            scope.invitations.push(inv);
        }
        let removals: Vec<String> = scope
            .proposals
            .iter()
            .filter(|p| p.id == pid)
            .flat_map(|p| p.removals.iter().cloned())
            .collect();
        for mid in removals {
            delete_member(scope, nid, &mid)?;
        }
    }
    Ok(Response::new(
        200,
        "application/json",
        serde_json::to_vec(&json!({})).unwrap_or_default(),
    ))
}

fn list_proposal_votes(
    scope: &mut BlockchainState,
    nid: &str,
    pid: &str,
) -> Result<Response, MbError> {
    find_network(scope, nid, "ListProposalVotes")?;
    scope
        .proposals
        .iter()
        .find(|p| p.id == pid)
        .ok_or_else(|| not_found("ListProposalVotes", format!("Proposal {pid} not found.")))?;
    // moto quirk: gathers votes from every proposal of the network.
    let list: Vec<Value> = scope
        .proposals
        .iter()
        .filter(|p| p.network_id == nid)
        .flat_map(|p| p.votes.iter().map(|(_, v)| v).cloned().collect::<Vec<_>>())
        .collect();
    Ok(ok_json(&json!({ "ProposalVotes": list })))
}

// --- invitations -------------------------------------------------------------------------------

fn list_invitations(scope: &BlockchainState) -> Response {
    let list: Vec<Value> = scope
        .invitations
        .iter()
        .map(|i| {
            json!({
                "InvitationId": i.id,
                "Status": i.status,
                "NetworkSummary": {
                    "Id": i.network_id,
                    "Name": i.network_name,
                    "Framework": i.network_framework,
                    "Status": i.network_status,
                    "CreationDate": i.network_creation_date,
                },
            })
        })
        .collect();
    ok_json(&json!({ "Invitations": list }))
}

fn reject_invitation(scope: &mut BlockchainState, iid: &str) -> Result<Response, MbError> {
    match scope.invitations.iter_mut().find(|i| i.id == iid) {
        Some(inv) => {
            inv.status = "REJECTED";
            Ok(ok_empty())
        }
        None => Err(not_found(
            "RejectInvitation",
            format!("InvitationId {iid} not found."),
        )),
    }
}

// --- nodes ---------------------------------------------------------------------------------------

fn create_node(scope: &mut BlockchainState, nid: &str, body: &Value) -> Result<Response, MbError> {
    find_network(scope, nid, "CreateNode")?;
    let mid = body
        .get("MemberId")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    scope
        .members
        .iter()
        .find(|m| m.id == mid)
        .ok_or_else(|| not_found("CreateNode", format!("Member {mid} not found.")))?;
    if scope.nodes.iter().filter(|n| n.member_id == mid).count() >= EDITION_MAX_NODES {
        return Err(limit_exceeded(
            "CreateNode",
            format!(
                "Maximum number of nodes exceeded in member {mid}. The maximum number of nodes you can have in a member in a STARTER Edition network is {EDITION_MAX_NODES}"
            ),
        ));
    }
    let instance_type = body["NodeConfiguration"]["InstanceType"]
        .as_str()
        .unwrap_or_default();
    // Instance family gate (moto checks STANDARD families case-insensitively).
    let families = ["bc.t3", "bc.m5", "bc.c5"];
    let family_ok = families
        .iter()
        .any(|f| instance_type.to_lowercase().starts_with(f));
    if !family_ok {
        return Err(invalid_request(
            "CreateNode",
            format!("Requested instance {instance_type} isn't supported."),
        ));
    }
    if !STARTER_ALLOWED.contains(&instance_type) {
        return Err(invalid_request(
            "CreateNode",
            format!(
                "Instance type {instance_type} is not supported with STARTER Edition networks."
            ),
        ));
    }
    let az = body["NodeConfiguration"]["AvailabilityZone"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    if !az.to_lowercase().starts_with("us-east-1") {
        return Err(invalid_request(
            "CreateNode",
            "Availability Zone is not valid".to_string(),
        ));
    }
    let node_id = prefixed_id("nd-");
    scope.nodes.push(Node {
        id: node_id.clone(),
        network_id: nid.to_string(),
        member_id: mid.to_string(),
        instance_type: instance_type.to_string(),
        availability_zone: az,
        log_publishing_configuration: body["NodeConfiguration"]["LogPublishingConfiguration"]
            .clone(),
        status: "AVAILABLE",
        creation_date: now_stamp_aware(),
    });
    Ok(ok_json(&json!({ "NodeId": node_id })))
}

const STARTER_ALLOWED: [&str; 2] = ["bc.t3.small", "bc.t3.medium"];

fn list_nodes(
    scope: &mut BlockchainState,
    nid: &str,
    mid: Option<&str>,
) -> Result<Response, MbError> {
    find_network(scope, nid, "ListNodes")?;
    let mid = mid.ok_or_else(|| not_found("ListNodes", "Member None not found.".to_string()))?;
    let member = scope
        .members
        .iter()
        .find(|m| m.id == mid)
        .ok_or_else(|| not_found("ListNodes", format!("Member {mid} not found.")))?;
    if member.status == "DELETED" {
        return Err(not_found("ListNodes", format!("Member {mid} not found.")));
    }
    let list: Vec<Value> = scope
        .nodes
        .iter()
        .filter(|n| n.member_id == mid)
        .map(|n| {
            json!({
                "Id": n.id,
                "Status": n.status,
                "CreationDate": n.creation_date,
                "AvailabilityZone": n.availability_zone,
                "InstanceType": n.instance_type,
            })
        })
        .collect();
    Ok(ok_json(&json!({ "Nodes": list })))
}

fn get_node(
    scope: &mut BlockchainState,
    nid: &str,
    mid: Option<&str>,
    ndid: &str,
    region: &str,
) -> Result<Response, MbError> {
    find_network(scope, nid, "GetNode")?;
    let mid = mid.ok_or_else(|| not_found("GetNode", "Member None not found.".to_string()))?;
    scope
        .members
        .iter()
        .find(|m| m.id == mid)
        .ok_or_else(|| not_found("GetNode", format!("Member {mid} not found.")))?;
    let n = scope
        .nodes
        .iter()
        .find(|n| n.id == ndid)
        .ok_or_else(|| not_found("GetNode", format!("Node {ndid} not found.")))?;
    if n.status == "DELETED" {
        return Err(not_found("GetNode", format!("Node {ndid} not found.")));
    }
    let id_l = n.id.to_lowercase();
    let net_l = n.network_id.to_lowercase();
    let mem_l = n.member_id.to_lowercase();
    let domain = format!("managedblockchain.{region}.amazonaws.com");
    let out = json!({
        "Node": {
            "NetworkId": n.network_id,
            "MemberId": n.member_id,
            "Id": n.id,
            "InstanceType": n.instance_type,
            "AvailabilityZone": n.availability_zone,
            "FrameworkAttributes": {
                "Fabric": {
                    "PeerEndpoint": format!("{id_l}.{net_l}.{mem_l}.{domain}:30003"),
                    "PeerEventEndpoint": format!("{id_l}.{net_l}.{mem_l}.{domain}:30004"),
                }
            },
            "LogPublishingConfiguration": n.log_publishing_configuration,
            "Status": n.status,
            "CreationDate": n.creation_date,
        }
    });
    Ok(Response::new(
        200,
        "application/json",
        serde_json::to_vec(&out).unwrap_or_default(),
    ))
}

fn update_node(
    scope: &mut BlockchainState,
    nid: &str,
    mid: Option<&str>,
    ndid: &str,
    body: &Value,
) -> Result<Response, MbError> {
    find_network(scope, nid, "UpdateNode")?;
    let mid = mid.ok_or_else(|| not_found("UpdateNode", "Member None not found.".to_string()))?;
    scope
        .members
        .iter()
        .find(|m| m.id == mid)
        .ok_or_else(|| not_found("UpdateNode", format!("Member {mid} not found.")))?;
    let node = scope
        .nodes
        .iter_mut()
        .find(|n| n.id == ndid)
        .ok_or_else(|| not_found("UpdateNode", format!("Node {ndid} not found.")))?;
    if let Some(lpc) = body.get("LogPublishingConfiguration") {
        node.log_publishing_configuration = lpc.clone();
    }
    Ok(ok_empty())
}

fn delete_node(
    scope: &mut BlockchainState,
    nid: &str,
    mid: Option<&str>,
    ndid: &str,
) -> Result<Response, MbError> {
    find_network(scope, nid, "DeleteNode")?;
    let mid = mid.ok_or_else(|| not_found("DeleteNode", "Member None not found.".to_string()))?;
    scope
        .members
        .iter()
        .find(|m| m.id == mid)
        .ok_or_else(|| not_found("DeleteNode", format!("Member {mid} not found.")))?;
    let node = scope
        .nodes
        .iter_mut()
        .find(|n| n.id == ndid)
        .ok_or_else(|| not_found("DeleteNode", format!("Node {ndid} not found.")))?;
    node.status = "DELETED";
    Ok(ok_empty())
}

// Unit tests covering each op family live in tests at module bottom.
#[cfg(test)]
mod tests;
