//! AWS Transfer Family backend — moto-equivalent (json protocol).
//!
//! Replaces the moto sidecar for `transfer` (`X-Amz-Target: TransferService.<Op>`
//! on `application/x-amz-json-1.1`). State is per-(account, region), mirroring
//! the moto fork's `TransferBackend`/`types.py`: servers (with nested users,
//! accesses, agreements and host keys), certificates, connectors (with the
//! camel→snake key normalization moto applies to `As2Config`/`SftpConfig`),
//! profiles, workflows, web apps (+ customization), the merged tag store
//! (`tagger`, synced back into each resource's `.tags`), and the eight
//! static `SECURITY_POLICIES`.
//!
//! Error split mirrors moto exactly: `ServerNotFound` / `UserNotFound` /
//! `PublicKeyNotFound` are HTTP 400; `AccessNotFound` and `ConnectorNotFound`
//! are HTTP 400 but report a `ResourceNotFoundException` type; every other
//! missing resource is a 404 `ResourceNotFoundException`. Connector file ops
//! mint `t-` / `listing-` / `delete-` / `move-` ids without touching S3.
//!
//! Contract: the moto fork (`moto/transfer`) is the ground truth — this
//! service has no checked-in probe — and tests/compatibility/
//! test_transfer_compat.py.

use crate::proto::{json_error, parse_json_body};
use crate::store::{self, ScopeKey};
use crate::{Request, Response};
use base64::Engine as _;
use parking_lot::RwLock;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

type Stores = Arc<RwLock<std::collections::HashMap<ScopeKey, TransferStore>>>;

fn stores() -> Stores {
    store::slot::<TransferStore>("transfer")
}

const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

/// Per-(account, region) in-memory state for AWS Transfer Family.
#[derive(Default)]
struct TransferStore {
    /// server id -> normalized server record; users/accesses/agreements/host
    /// keys hang off the server, like moto's `Server` dataclass.
    servers: BTreeMap<String, Value>,
    /// certificate id -> record
    certificates: BTreeMap<String, Value>,
    /// connector id -> record
    connectors: BTreeMap<String, Value>,
    /// profile id -> record
    profiles: BTreeMap<String, Value>,
    /// workflow id -> record
    workflows: BTreeMap<String, Value>,
    /// web app id -> record
    web_apps: BTreeMap<String, Value>,
    /// arn -> ordered [{Key, Value}] — moto's `tagger` (merge by Key, first
    /// position preserved).
    tagger: BTreeMap<String, Vec<Value>>,
}

fn with_store(req: &Request, f: impl FnOnce(&mut TransferStore) -> Response) -> Response {
    let slot = stores();
    let mut map = slot.write();
    let st = map.entry((req.account, req.region.clone())).or_default();
    f(st)
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

fn ok(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

/// moto's `mock_random.get_random_hex(n)`: n lowercase hex chars.
fn random_hex(n: usize) -> String {
    let mut s = String::new();
    while s.len() < n {
        s.push_str(&hex::encode(Uuid::new_v4().as_bytes()));
    }
    s.truncate(n);
    s
}

/// moto's `str(unix_time())` — python float repr of unix seconds.
fn now_unix_str() -> String {
    format!("{:?}", crate::proto::now_unix())
}

/// moto's `camelcase_to_underscores`: `_` before each consecutive uppercase
/// run (regex `([A-Z]+)`), lowercased.
fn camel_to_snake(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    let mut upper_run = false;
    for c in s.chars() {
        if c.is_ascii_uppercase() {
            if !upper_run && !out.is_empty() {
                out.push('_');
            }
            out.push(c.to_ascii_lowercase());
            upper_run = true;
        } else {
            out.push(c);
            upper_run = false;
        }
    }
    out
}

/// Snakify every key of a config object (moto's
/// `{camelcase_to_underscores(k): v ...}`).
fn snakify(v: &Value) -> Value {
    let mut out = serde_json::Map::new();
    if let Some(obj) = v.as_object() {
        for (k, val) in obj {
            out.insert(camel_to_snake(k), val.clone());
        }
    }
    Value::Object(out)
}

/// Param getter with python `params.get("Key")` semantics (absent → None).
fn param<'a>(p: &'a Value, key: &str) -> Option<&'a Value> {
    p.get(key).filter(|v| !v.is_null())
}

/// Param getter for optional strings (absent or non-string → None).
fn param_str<'a>(p: &'a Value, key: &str) -> Option<&'a str> {
    param(p, key).and_then(Value::as_str)
}

/// Python truthiness for stored values.
fn truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

// ---------------------------------------------------------------------------
// Errors (the fork's exceptions.py, verbatim)
// ---------------------------------------------------------------------------

fn server_not_found(server_id: &str) -> Response {
    json_error(
        400,
        "ServerNotFound",
        &format!("There are no transfer protocol-enabled servers with ID {server_id}."),
    )
}

fn user_not_found(user_name: &str, server_id: &str) -> Response {
    json_error(
        400,
        "UserNotFound",
        &format!("{user_name} does not match any user associated with server {server_id}."),
    )
}

fn public_key_not_found(user_name: &str, server_id: &str, key_id: &str) -> Response {
    json_error(
        400,
        "PublicKeyNotFound",
        &format!("{key_id} does not match any keys associated with user {user_name} for server {server_id}."),
    )
}

fn resource_not_found(resource_type: &str, resource_id: &str) -> Response {
    json_error(
        404,
        "ResourceNotFoundException",
        &format!("{resource_type} not found: {resource_id}"),
    )
}

fn access_not_found(server_id: &str, external_id: &str) -> Response {
    json_error(
        400,
        "ResourceNotFoundException",
        &format!("Access for ExternalId {external_id} and ServerId {server_id} does not exist."),
    )
}

/// The fork's overloading quirk: describe/update/delete connector raise
/// `ConnectorNotFound` (HTTP 400), while `test_connection` and the file ops
/// raise `ResourceNotFound` (HTTP 404).
fn connector_not_found(connector_id: &str) -> Response {
    json_error(
        400,
        "ResourceNotFoundException",
        &format!("Connector {connector_id} does not exist."),
    )
}

// ---------------------------------------------------------------------------
// Records — lowercase normalized storage, camelCase render
// ---------------------------------------------------------------------------

// --- Server ---

fn new_server_record(region: &str, account: u64, p: &Value) -> Value {
    let server_id = format!("s-{}", random_hex(17));
    json!({
        "arn": format!("arn:aws:transfer:{region}:{account}:server/{server_id}"),
        "server_id": server_id,
        "state": "ONLINE",
        "certificate": param_str(p, "Certificate"),
        "domain": param_str(p, "Domain"),
        "endpoint_type": param_str(p, "EndpointType"),
        "host_key_fingerprint": param_str(p, "HostKey"),
        "identity_provider_type": param_str(p, "IdentityProviderType"),
        "logging_role": param_str(p, "LoggingRole"),
        "post_authentication_login_banner": param_str(p, "PostAuthenticationLoginBanner"),
        "pre_authentication_login_banner": param_str(p, "PreAuthenticationLoginBanner"),
        "protocols": param(p, "Protocols"),
        "security_policy_name": param_str(p, "SecurityPolicyName"),
        "structured_log_destinations": param(p, "StructuredLogDestinations"),
        "as2_service_managed_egress_ip_addresses": ["0.0.0.0/0"],
        "endpoint_details": normalize_endpoint_details(param(p, "EndpointDetails")),
        "identity_provider_details": normalize_identity_provider_details(
            param(p, "IdentityProviderDetails"),
        ),
        "protocol_details": normalize_protocol_details(param(p, "ProtocolDetails")),
        "s3_storage_options": normalize_s3_storage_options(param(p, "S3StorageOptions")),
        "workflow_details": normalize_workflow_details(param(p, "WorkflowDetails")),
        "tags": p.get("Tags").cloned().unwrap_or_else(|| json!([])),
        "user_count": 0,
        "users": [],
        "accesses": [],
        "agreements": [],
        "host_keys": [],
    })
}

/// moto's endpoint_details storage: `{lower_key: value.get(CamelKey)}`.
fn normalize_endpoint_details(p: Option<&Value>) -> Value {
    match p {
        None => json!({}),
        Some(d) => json!({
            "address_allocation_ids": d.get("AddressAllocationIds"),
            "subnet_ids": d.get("SubnetIds"),
            "vpc_endpoint_id": d.get("VpcEndpointId"),
            "vpc_id": d.get("VpcId"),
            "security_group_ids": d.get("SecurityGroupIds"),
        }),
    }
}

fn normalize_identity_provider_details(p: Option<&Value>) -> Value {
    match p {
        None => json!({}),
        Some(d) => json!({
            "url": d.get("Url"),
            "invocation_role": d.get("InvocationRole"),
            "directory_id": d.get("DirectoryId"),
            "function": d.get("Function"),
            "sftp_authentication_methods": d.get("SftpAuthenticationMethods"),
        }),
    }
}

fn normalize_protocol_details(p: Option<&Value>) -> Value {
    match p {
        None => json!({}),
        Some(d) => json!({
            "passive_ip": d.get("PassiveIp"),
            "tls_session_resumption_mode": d.get("TlsSessionResumptionMode"),
            "set_stat_option": d.get("SetStatOption"),
            "as2_transports": d.get("As2Transports"),
        }),
    }
}

fn normalize_s3_storage_options(p: Option<&Value>) -> Value {
    match p {
        None => json!({}),
        Some(d) => json!({"directory_listing_optimization": d.get("DirectoryListingOptimization")}),
    }
}

/// moto's workflow_details normalization: entries keep
/// `{workflow_id, execution_role}`; the render filters the truthy pairs.
fn normalize_workflow_details(p: Option<&Value>) -> Value {
    let steps = |key: &str| -> Vec<Value> {
        p.and_then(|w| w.get(key))
            .and_then(Value::as_array)
            .map(|list| {
                list.iter()
                    .map(|w| {
                        json!({
                            "workflow_id": w.get("WorkflowId"),
                            "execution_role": w.get("ExecutionRole"),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    };
    json!({
        "on_upload": steps("OnUpload"),
        "on_partial_upload": steps("OnPartialUpload"),
    })
}

fn workflow_steps(rec: &Value, store_key: &str) -> Vec<Value> {
    let Some(list) = rec["workflow_details"]
        .get(store_key)
        .and_then(Value::as_array)
    else {
        return Vec::new();
    };
    list.iter()
        .filter_map(|w| {
            let id = &w["workflow_id"];
            let role = &w["execution_role"];
            // python `if workflow_id and execution_role`
            if truthy(id) && truthy(role) {
                Some(json!({"WorkflowId": id, "ExecutionRole": role}))
            } else {
                None
            }
        })
        .collect()
}

/// moto 5.2.3 #10068's `prune_unset`: AWS Transfer omits unset optional
/// fields from its Describe* responses rather than returning them as null,
/// so null members and emptied containers are dropped recursively. List
/// items are still pruned but never dropped, like moto.
fn prune_unset(v: &Value) -> Value {
    match v {
        Value::Object(map) => {
            let mut pruned = serde_json::Map::new();
            for (key, val) in map {
                let cleaned = prune_unset(val);
                let drop = cleaned.is_null()
                    || matches!(cleaned, Value::Object(ref m) if m.is_empty())
                    || matches!(cleaned, Value::Array(ref a) if a.is_empty());
                if !drop {
                    pruned.insert(key.clone(), cleaned);
                }
            }
            Value::Object(pruned)
        }
        Value::Array(items) => Value::Array(items.iter().map(prune_unset).collect()),
        other => other.clone(),
    }
}

/// `Server.to_dict()` — wrapped in `prune_unset` per moto 5.2.3 #10068.
fn server_to_dict(rec: &Value) -> Value {
    let on_upload = workflow_steps(rec, "on_upload");
    let on_partial_upload = workflow_steps(rec, "on_partial_upload");
    let ed = &rec["endpoint_details"];
    let ipd = &rec["identity_provider_details"];
    let pd = &rec["protocol_details"];
    let s3o = &rec["s3_storage_options"];
    // Unset optional members (e.g. Certificate, LoggingRole, the empty
    // EndpointDetails/WorkflowDetails wrappers, Tags on a tag-less server)
    // are omitted below instead of rendered as null.
    prune_unset(&json!({
        "Certificate": rec["certificate"],
        "Domain": rec["domain"],
        "EndpointType": rec["endpoint_type"],
        "HostKeyFingerprint": rec["host_key_fingerprint"],
        "IdentityProviderType": rec["identity_provider_type"],
        "LoggingRole": rec["logging_role"],
        "PostAuthenticationLoginBanner": rec["post_authentication_login_banner"],
        "PreAuthenticationLoginBanner": rec["pre_authentication_login_banner"],
        "Protocols": rec["protocols"],
        "SecurityPolicyName": rec["security_policy_name"],
        "StructuredLogDestinations": rec["structured_log_destinations"],
        "Arn": rec["arn"],
        "As2ServiceManagedEgressIpAddresses": rec["as2_service_managed_egress_ip_addresses"],
        "ServerId": rec["server_id"],
        "State": rec["state"],
        "Tags": rec["tags"],
        "UserCount": rec["user_count"],
        "EndpointDetails": {
            "AddressAllocationIds": ed["address_allocation_ids"],
            "SubnetIds": ed["subnet_ids"],
            "VpcEndpointId": ed["vpc_endpoint_id"],
            "VpcId": ed["vpc_id"],
            "SecurityGroupIds": ed["security_group_ids"],
        },
        "IdentityProviderDetails": {
            "Url": ipd["url"],
            "InvocationRole": ipd["invocation_role"],
            "DirectoryId": ipd["directory_id"],
            "Function": ipd["function"],
            "SftpAuthenticationMethods": ipd["sftp_authentication_methods"],
        },
        "ProtocolDetails": {
            "PassiveIp": pd["passive_ip"],
            "TlsSessionResumptionMode": pd["tls_session_resumption_mode"],
            "SetStatOption": pd["set_stat_option"],
            "As2Transports": pd["as2_transports"],
        },
        "S3StorageOptions": {
            "DirectoryListingOptimization": s3o["directory_listing_optimization"],
        },
        "WorkflowDetails": {"OnUpload": on_upload, "OnPartialUpload": on_partial_upload},
    }))
}

fn server_to_short(rec: &Value) -> Value {
    json!({
        "Arn": rec["arn"],
        "Domain": rec["domain"],
        "EndpointType": rec["endpoint_type"],
        "IdentityProviderType": rec["identity_provider_type"],
        "LoggingRole": rec["logging_role"],
        "ServerId": rec["server_id"],
        "State": rec["state"],
        "UserCount": rec["user_count"],
    })
}

// --- User ---

fn user_record(region: &str, account: u64, server_id: &str, p: &Value) -> Value {
    let user_name = p
        .get("UserName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let mut rec = json!({
        "arn": format!("arn:aws:transfer:{region}:{account}:user/{server_id}/{user_name}"),
        "home_directory": param_str(p, "HomeDirectory"),
        "home_directory_type": param_str(p, "HomeDirectoryType"),
        "policy": param_str(p, "Policy"),
        // python: role=params.get("Role") — None renders as null.
        "role": p.get("Role").filter(|v| !v.is_null()).cloned().unwrap_or(Value::Null),
        "user_name": user_name,
        "home_directory_mappings": [],
        // moto's User default: an empty posix dict, which renders all-null.
        "posix_profile": {},
        "ssh_public_keys": [],
        "tags": p.get("Tags").cloned().unwrap_or_else(|| json!([])),
    });
    if let Some(mappings) = param(p, "HomeDirectoryMappings").and_then(Value::as_array) {
        for m in mappings {
            rec["home_directory_mappings"]
                .as_array_mut()
                .expect("array")
                .push(json!({
                    "entry": m.get("Entry"),
                    "target": m.get("Target"),
                    "type": m.get("Type"),
                }));
        }
    }
    if let Some(posix) = param(p, "PosixProfile") {
        rec["posix_profile"] = normalize_posix(posix);
    }
    if let Some(key_body) = param_str(p, "SshPublicKeyBody") {
        let now = now_unix_str();
        rec["ssh_public_keys"] = json!([{
            "date_imported": now,
            "ssh_public_key_body": key_body,
            "ssh_public_key_id": format!("mock_ssh_public_key_id_{key_body}_{now}"),
        }]);
    }
    rec
}

/// moto's posix normalization on create/update: `{gid, uid, secondary_gids}`.
fn normalize_posix(p: &Value) -> Value {
    json!({
        "gid": p.get("Gid"),
        "uid": p.get("Uid"),
        "secondary_gids": p.get("SecondaryGids"),
    })
}

fn posix_to_render(rec: &Value) -> Value {
    json!({
        "Uid": rec.get("uid"),
        "Gid": rec.get("gid"),
        "SecondaryGids": rec.get("secondary_gids"),
    })
}

/// `User.to_dict()`.
fn user_to_dict(rec: &Value) -> Value {
    let mappings: Vec<Value> = rec["home_directory_mappings"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|m| {
            json!({
                "Entry": m.get("entry"),
                "Target": m.get("target"),
                "Type": m.get("type"),
            })
        })
        .collect();
    let keys: Vec<Value> = rec["ssh_public_keys"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|k| {
            json!({
                "DateImported": k.get("date_imported"),
                "SshPublicKeyBody": k.get("ssh_public_key_body"),
                "SshPublicKeyId": k.get("ssh_public_key_id"),
            })
        })
        .collect();
    json!({
        "HomeDirectory": rec["home_directory"],
        "HomeDirectoryType": rec["home_directory_type"],
        "Policy": rec["policy"],
        "Role": rec["role"],
        "UserName": rec["user_name"],
        "Arn": rec["arn"],
        "HomeDirectoryMappings": mappings,
        "SshPublicKeys": keys,
        "PosixProfile": posix_to_render(&rec["posix_profile"]),
        "Tags": rec["tags"],
    })
}

fn user_to_short(rec: &Value) -> Value {
    json!({
        "Arn": rec["arn"],
        "HomeDirectory": rec["home_directory"],
        "HomeDirectoryType": rec["home_directory_type"],
        "Role": rec["role"],
        "SshPublicKeyCount": rec["ssh_public_keys"].as_array().map_or(0, |a| a.len()),
        "UserName": rec["user_name"],
    })
}

// --- Access ---

fn access_record(p: &Value) -> Value {
    let mut rec = json!({
        "external_id": param_str(p, "ExternalId").unwrap_or_default(),
        "home_directory": param_str(p, "HomeDirectory"),
        "home_directory_type": param_str(p, "HomeDirectoryType"),
        "home_directory_mappings": [],
        "policy": param_str(p, "Policy"),
        "posix_profile": param(p, "PosixProfile").map(normalize_posix),
        "role": param_str(p, "Role").unwrap_or_default(),
    });
    if let Some(mappings) = param(p, "HomeDirectoryMappings").and_then(Value::as_array) {
        rec["home_directory_mappings"] = Value::Array(
            mappings
                .iter()
                .map(|m| json!({"entry": m.get("Entry"), "target": m.get("Target")}))
                .collect(),
        );
    }
    rec
}

/// `Access.to_dict()`.
fn access_to_dict(rec: &Value) -> Value {
    let mut out = json!({
        "ExternalId": rec["external_id"],
        "Role": rec["role"],
    });
    let obj = out.as_object_mut().expect("object");
    if rec.get("home_directory").and_then(Value::as_str).is_some() {
        obj.insert("HomeDirectory".into(), rec["home_directory"].clone());
    }
    if rec
        .get("home_directory_type")
        .and_then(Value::as_str)
        .is_some()
    {
        obj.insert(
            "HomeDirectoryType".into(),
            rec["home_directory_type"].clone(),
        );
    }
    let mappings = rec.get("home_directory_mappings").and_then(Value::as_array);
    if let Some(mappings) = mappings.filter(|m| !m.is_empty()) {
        obj.insert(
            "HomeDirectoryMappings".into(),
            Value::Array(
                mappings
                    .iter()
                    .map(|m| json!({"Entry": m.get("entry"), "Target": m.get("target")}))
                    .collect(),
            ),
        );
    }
    if rec.get("policy").and_then(Value::as_str).is_some() {
        obj.insert("Policy".into(), rec["policy"].clone());
    }
    if rec
        .get("posix_profile")
        .and_then(Value::as_object)
        .is_some()
    {
        obj.insert(
            "PosixProfile".into(),
            posix_to_render(&rec["posix_profile"]),
        );
    }
    out
}

fn access_to_short(rec: &Value) -> Value {
    json!({
        "ExternalId": rec["external_id"],
        "HomeDirectory": rec["home_directory"],
        "HomeDirectoryType": rec["home_directory_type"],
        "Role": rec["role"],
    })
}

// --- Agreement ---

fn agreement_record(region: &str, account: u64, server_id: &str, p: &Value) -> Value {
    let agreement_id = format!("a-{}", random_hex(17));
    json!({
        "arn": format!("arn:aws:transfer:{region}:{account}:agreement/{server_id}/{agreement_id}"),
        "agreement_id": agreement_id,
        "server_id": server_id,
        "local_profile_id": p.get("LocalProfileId").and_then(Value::as_str).unwrap_or_default(),
        "partner_profile_id": p.get("PartnerProfileId").and_then(Value::as_str).unwrap_or_default(),
        "access_role": p.get("AccessRole").and_then(Value::as_str).unwrap_or_default(),
        "description": param_str(p, "Description"),
        "base_directory": param_str(p, "BaseDirectory"),
        "status": param_str(p, "Status").unwrap_or("ACTIVE"),
        "preserve_filename": param_str(p, "PreserveFilename"),
        "enforce_message_signing": param_str(p, "EnforceMessageSigning"),
        "custom_directories": param(p, "CustomDirectories").cloned(),
        "tags": p.get("Tags").cloned().unwrap_or_else(|| json!([])),
    })
}

/// `Agreement.to_dict()`.
fn agreement_to_dict(rec: &Value) -> Value {
    let mut out = json!({
        "Arn": rec["arn"],
        "AgreementId": rec["agreement_id"],
        "Description": rec["description"],
        "Status": rec["status"],
        "ServerId": rec["server_id"],
        "LocalProfileId": rec["local_profile_id"],
        "PartnerProfileId": rec["partner_profile_id"],
        "BaseDirectory": rec["base_directory"],
        "AccessRole": rec["access_role"],
        "Tags": rec["tags"],
    });
    let obj = out.as_object_mut().expect("object");
    if rec
        .get("preserve_filename")
        .and_then(Value::as_str)
        .is_some()
    {
        obj.insert("PreserveFilename".into(), rec["preserve_filename"].clone());
    }
    if rec
        .get("enforce_message_signing")
        .and_then(Value::as_str)
        .is_some()
    {
        obj.insert(
            "EnforceMessageSigning".into(),
            rec["enforce_message_signing"].clone(),
        );
    }
    if rec
        .get("custom_directories")
        .and_then(Value::as_object)
        .is_some()
    {
        obj.insert(
            "CustomDirectories".into(),
            rec["custom_directories"].clone(),
        );
    }
    out
}

fn agreement_to_short(rec: &Value) -> Value {
    json!({
        "Arn": rec["arn"],
        "AgreementId": rec["agreement_id"],
        "Description": rec["description"],
        "Status": rec["status"],
        "ServerId": rec["server_id"],
        "LocalProfileId": rec["local_profile_id"],
        "PartnerProfileId": rec["partner_profile_id"],
    })
}

// --- Certificate ---

fn certificate_record(region: &str, account: u64, p: &Value) -> Value {
    let certificate_id = format!("cert-{}", random_hex(17));
    let cert_type = if p.get("CertificateChain").filter(|v| !v.is_null()).is_some() {
        "CERTIFICATE_WITH_CHAIN"
    } else {
        "CERTIFICATE"
    };
    json!({
        "arn": format!("arn:aws:transfer:{region}:{account}:certificate/{certificate_id}"),
        "certificate_id": certificate_id,
        "usage": p.get("Usage").and_then(Value::as_str).unwrap_or_default(),
        "certificate": p.get("Certificate").and_then(Value::as_str).unwrap_or_default(),
        "certificate_chain": param_str(p, "CertificateChain"),
        "private_key": param_str(p, "PrivateKey"),
        "active_date": param_str(p, "ActiveDate"),
        "inactive_date": param_str(p, "InactiveDate"),
        "description": param_str(p, "Description"),
        "status": "ACTIVE",
        "cert_type": cert_type,
        "tags": p.get("Tags").cloned().unwrap_or_else(|| json!([])),
    })
}

/// `Certificate.to_dict()` (moto never renders `PrivateKey`).
fn certificate_to_dict(rec: &Value) -> Value {
    json!({
        "Arn": rec["arn"],
        "CertificateId": rec["certificate_id"],
        "Usage": rec["usage"],
        "Status": rec["status"],
        "Certificate": rec["certificate"],
        "CertificateChain": rec["certificate_chain"],
        "ActiveDate": rec["active_date"],
        "InactiveDate": rec["inactive_date"],
        "Description": rec["description"],
        "Type": rec["cert_type"],
        "Tags": rec["tags"],
    })
}

fn certificate_to_short(rec: &Value) -> Value {
    json!({
        "Arn": rec["arn"],
        "CertificateId": rec["certificate_id"],
        "Usage": rec["usage"],
        "Status": rec["status"],
        "ActiveDate": rec["active_date"],
        "InactiveDate": rec["inactive_date"],
        "Description": rec["description"],
        "Type": rec["cert_type"],
    })
}

// --- Connector ---

/// The fork's effective `create_connector` (its second, shadowing def): raw
/// storage plus snake_case-rewritten As2Config/SftpConfig; EgressConfig is
/// accepted but dropped.
fn connector_record(region: &str, account: u64, p: &Value) -> Value {
    let connector_id = format!("c-{}", random_hex(17));
    json!({
        "arn": format!("arn:aws:transfer:{region}:{account}:connector/{connector_id}"),
        "connector_id": connector_id,
        "access_role": p.get("AccessRole").and_then(Value::as_str).unwrap_or_default(),
        "url": param_str(p, "Url"),
        // moto: `{camelcase_to_underscores(k): v} if as2_config else None`
        "as2_config": param(p, "As2Config").filter(|v| truthy(v)).map(snakify),
        "logging_role": param_str(p, "LoggingRole"),
        "sftp_config": param(p, "SftpConfig").filter(|v| truthy(v)).map(snakify),
        "security_policy_name": param_str(p, "SecurityPolicyName"),
        "tags": p.get("Tags").cloned().unwrap_or_else(|| json!([])),
    })
}

/// `Connector.to_dict()` — egress addresses get their mock default at render
/// time; As2Config/SftpConfig render with moto's snake_case keys; unset
/// members are omitted (moto 5.2.3 #10068 `prune_unset`), never serialized
/// as null.
fn connector_to_dict(rec: &Value) -> Value {
    let mut out = json!({
        "Arn": rec["arn"],
        "ConnectorId": rec["connector_id"],
        "Url": rec["url"],
        "AccessRole": rec["access_role"],
        "LoggingRole": rec["logging_role"],
        "Tags": rec["tags"],
        "ServiceManagedEgressIpAddresses": ["127.0.0.1", "127.0.0.2"],
        "SecurityPolicyName": rec["security_policy_name"],
    });
    let obj = out.as_object_mut().expect("object");
    if rec.get("as2_config").and_then(Value::as_object).is_some() {
        obj.insert("As2Config".into(), rec["as2_config"].clone());
    }
    if rec.get("sftp_config").and_then(Value::as_object).is_some() {
        obj.insert("SftpConfig".into(), rec["sftp_config"].clone());
    }
    prune_unset(&out)
}

fn connector_to_short(rec: &Value) -> Value {
    json!({
        "Arn": rec["arn"],
        "ConnectorId": rec["connector_id"],
        "Url": rec["url"],
    })
}

// --- Profile ---

fn profile_record(region: &str, account: u64, p: &Value) -> Value {
    let profile_id = format!("p-{}", random_hex(17));
    json!({
        "arn": format!("arn:aws:transfer:{region}:{account}:profile/{profile_id}"),
        "profile_id": profile_id,
        "as2_id": p.get("As2Id").and_then(Value::as_str).unwrap_or_default(),
        "profile_type": p.get("ProfileType").and_then(Value::as_str).unwrap_or_default(),
        "certificate_ids": p.get("CertificateIds").filter(|v| !v.is_null()).cloned(),
        "tags": p.get("Tags").cloned().unwrap_or_else(|| json!([])),
    })
}

/// `Profile.to_dict()`.
fn profile_to_dict(rec: &Value) -> Value {
    json!({
        "Arn": rec["arn"],
        "ProfileId": rec["profile_id"],
        "ProfileType": rec["profile_type"],
        "As2Id": rec["as2_id"],
        "CertificateIds": rec.get("certificate_ids").cloned().unwrap_or_else(|| json!([])),
        "Tags": rec["tags"],
    })
}

fn profile_to_short(rec: &Value) -> Value {
    json!({
        "Arn": rec["arn"],
        "ProfileId": rec["profile_id"],
        "ProfileType": rec["profile_type"],
        "As2Id": rec["as2_id"],
    })
}

// --- Workflow ---

fn workflow_record(region: &str, account: u64, p: &Value) -> Value {
    let workflow_id = format!("w-{}", random_hex(17));
    json!({
        "arn": format!("arn:aws:transfer:{region}:{account}:workflow/{workflow_id}"),
        "workflow_id": workflow_id,
        "description": param_str(p, "Description"),
        "steps": p.get("Steps").cloned().unwrap_or_else(|| json!([])),
        "on_exception_steps": p.get("OnExceptionSteps").filter(|v| !v.is_null()).cloned(),
        "tags": p.get("Tags").cloned().unwrap_or_else(|| json!([])),
    })
}

/// `Workflow.to_dict()`.
fn workflow_to_dict(rec: &Value) -> Value {
    let on_exception = rec
        .get("on_exception_steps")
        .cloned()
        .filter(|v| !v.is_null())
        .unwrap_or_else(|| json!([]));
    json!({
        "Arn": rec["arn"],
        "WorkflowId": rec["workflow_id"],
        "Description": rec["description"],
        "Steps": rec["steps"],
        "OnExceptionSteps": on_exception,
        "Tags": rec["tags"],
    })
}

fn workflow_to_short(rec: &Value) -> Value {
    json!({
        "Arn": rec["arn"],
        "WorkflowId": rec["workflow_id"],
        "Description": rec["description"],
    })
}

// --- HostKey ---

fn host_key_record(region: &str, account: u64, server_id: &str, p: &Value) -> Value {
    let host_key_id = format!("hostkey-{}", random_hex(20));
    json!({
        "arn": format!("arn:aws:transfer:{region}:{account}:host-key/{server_id}/{host_key_id}"),
        "host_key_id": host_key_id,
        "host_key_body": p.get("HostKeyBody").and_then(Value::as_str).unwrap_or_default(),
        "description": param_str(p, "Description"),
        "host_key_fingerprint": format!("SHA256:{}", random_hex(32)),
        "date_imported": now_unix_str(),
        "key_type": "RSA",
        "tags": p.get("Tags").cloned().unwrap_or_else(|| json!([])),
    })
}

/// `HostKey.to_dict()`.
fn host_key_to_dict(rec: &Value) -> Value {
    json!({
        "Arn": rec["arn"],
        "HostKeyId": rec["host_key_id"],
        "HostKeyFingerprint": rec["host_key_fingerprint"],
        "Description": rec["description"],
        "Type": rec["key_type"],
        "DateImported": rec["date_imported"],
        "Tags": rec["tags"],
    })
}

fn host_key_to_short(rec: &Value) -> Value {
    json!({
        "Arn": rec["arn"],
        "HostKeyId": rec["host_key_id"],
        "Description": rec["description"],
        "DateImported": rec["date_imported"],
        "Type": rec["key_type"],
    })
}

// --- WebApp / customization ---

fn web_app_record(region: &str, account: u64, p: &Value) -> Value {
    let web_app_id = format!("webapp-{}", random_hex(20));
    let endpoint = format!("https://{web_app_id}.transfer.{region}.amazonaws.com");
    json!({
        "arn": format!("arn:aws:transfer:{region}:{account}:webapp/{web_app_id}"),
        "web_app_id": web_app_id,
        "identity_provider_details": p.get("IdentityProviderDetails").cloned().unwrap_or(json!({})),
        "access_endpoint": param_str(p, "AccessEndpoint"),
        "web_app_units": param(p, "WebAppUnits").cloned(),
        "web_app_endpoint_policy": param_str(p, "WebAppEndpointPolicy"),
        "endpoint_details": param(p, "EndpointDetails").cloned(),
        "tags": p.get("Tags").cloned().unwrap_or_else(|| json!([])),
        "web_app_endpoint": endpoint,
        "customization": Value::Null,
    })
}

/// `WebApp.to_dict()`.
fn web_app_to_dict(rec: &Value) -> Value {
    let mut out = json!({
        "Arn": rec["arn"],
        "WebAppId": rec["web_app_id"],
        "DescribedIdentityProviderDetails": rec["identity_provider_details"],
        "AccessEndpoint": rec["access_endpoint"],
        "WebAppEndpoint": rec["web_app_endpoint"],
        "WebAppUnits": rec["web_app_units"],
        "Tags": rec["tags"],
        "WebAppEndpointPolicy": rec["web_app_endpoint_policy"],
    });
    if rec
        .get("endpoint_details")
        .and_then(Value::as_object)
        .is_some()
    {
        out["DescribedEndpointDetails"] = rec["endpoint_details"].clone();
    }
    out
}

fn web_app_to_short(rec: &Value) -> Value {
    json!({
        "Arn": rec["arn"],
        "WebAppId": rec["web_app_id"],
        "AccessEndpoint": rec["access_endpoint"],
        "WebAppEndpoint": rec["web_app_endpoint"],
    })
}

/// `WebAppCustomization.to_dict()` — files re-encoded to base64 strings.
fn web_app_customization_to_dict(rec: &Value) -> Value {
    let mut out = json!({
        "Arn": rec["arn"],
        "WebAppId": rec["web_app_id"],
    });
    let obj = out.as_object_mut().expect("object");
    if rec.get("title").and_then(Value::as_str).is_some() {
        obj.insert("Title".into(), rec["title"].clone());
    }
    for (render_key, store_key) in [("LogoFile", "logo_file"), ("FaviconFile", "favicon_file")] {
        if let Some(bytes) = rec.get(store_key).and_then(Value::as_array) {
            let raw: Vec<u8> = bytes
                .iter()
                .filter_map(|b| b.as_u64().map(|n| n as u8))
                .collect();
            obj.insert(render_key.into(), Value::String(B64.encode(raw)));
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Tag store (moto's `tagger` + `_sync_tags_to_resource`)
// ---------------------------------------------------------------------------

/// moto's `_add_tags`: merge by Key, first-seen position preserved.
fn add_tags(st: &mut TransferStore, arn: &str, tags: Option<&Value>) {
    let Some(tags) = tags.and_then(Value::as_array) else {
        return;
    };
    let list = st.tagger.entry(arn.to_string()).or_default();
    for tag in tags {
        let key = tag.get("Key").and_then(Value::as_str).unwrap_or_default();
        if let Some(existing) = list
            .iter_mut()
            .find(|t| t.get("Key").and_then(Value::as_str) == Some(key))
        {
            *existing = tag.clone();
        } else {
            list.push(tag.clone());
        }
    }
}

/// moto's `_remove_tags`; the `tagger[arn] = [...]` assignment creates the
/// arn slot even when it was unknown, so mirror that.
fn remove_tags(st: &mut TransferStore, arn: &str, keys: Option<&Value>) {
    let tag_keys: Vec<String> = keys
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(String::from)
                .collect()
        })
        .unwrap_or_default();
    let list = st.tagger.entry(arn.to_string()).or_default();
    list.retain(|t| {
        t.get("Key")
            .and_then(Value::as_str)
            .map(|k| !tag_keys.iter().any(|tk| tk == k))
            .unwrap_or(true)
    });
}

fn tags_for_arn(st: &TransferStore, arn: &str) -> Vec<Value> {
    st.tagger.get(arn).cloned().unwrap_or_default()
}

/// moto's `_sync_tags_to_resource`: copy the tag-store list back into the
/// resource's own `.tags` so describe shows merged tags; first match wins
/// (servers, then nested users/agreements/host keys, then the flat stores).
fn sync_tags_to_resource(st: &mut TransferStore, arn: &str) {
    let tags = tags_for_arn(st, arn);
    let nested_slot = |slot: &mut Value, key: &str| -> bool {
        // Sync the first nested item under `key` whose arn matches.
        let item = slot[key]
            .as_array_mut()
            .and_then(|items| items.iter_mut().find(|u| u["arn"].as_str() == Some(arn)));
        match item {
            Some(item) => {
                item["tags"] = Value::Array(tags.clone());
                true
            }
            None => false,
        }
    };
    for slot in st
        .servers
        .values_mut()
        .chain(st.certificates.values_mut())
        .chain(st.connectors.values_mut())
        .chain(st.profiles.values_mut())
        .chain(st.workflows.values_mut())
        .chain(st.web_apps.values_mut())
    {
        if slot["arn"].as_str() == Some(arn) {
            slot["tags"] = Value::Array(tags.clone());
            return;
        }
        if nested_slot(slot, "users")
            || nested_slot(slot, "agreements")
            || nested_slot(slot, "host_keys")
        {
            return;
        }
    }
}

// ---------------------------------------------------------------------------
// Security policies — the fork's SECURITY_POLICIES table
// ---------------------------------------------------------------------------

type PolicySpec = (
    &'static str,
    bool,
    &'static [&'static str],
    &'static [&'static str],
    &'static [&'static str],
    &'static [&'static str],
    &'static [&'static str],
    &'static str,
    &'static [&'static str],
);

const SSH_CIPHERS_CLASSIC: &[&str] = &[
    "aes128-ctr",
    "aes192-ctr",
    "aes256-ctr",
    "aes128-gcm@openssh.com",
    "aes256-gcm@openssh.com",
];
const SSH_KEXS_CLASSIC: &[&str] = &[
    "ecdh-sha2-nistp256",
    "ecdh-sha2-nistp384",
    "ecdh-sha2-nistp521",
    "diffie-hellman-group-exchange-sha256",
    "diffie-hellman-group16-sha512",
    "diffie-hellman-group18-sha512",
    "diffie-hellman-group14-sha256",
];
const SSH_KEXS_MODERN: &[&str] = &[
    "ecdh-sha2-nistp256",
    "ecdh-sha2-nistp384",
    "ecdh-sha2-nistp521",
    "diffie-hellman-group16-sha512",
    "diffie-hellman-group18-sha512",
];
const SSH_MACS_FULL: &[&str] = &[
    "hmac-sha2-256-etm@openssh.com",
    "hmac-sha2-512-etm@openssh.com",
    "hmac-sha2-256",
    "hmac-sha2-512",
];
const SSH_MACS_ETM: &[&str] = &[
    "hmac-sha2-256-etm@openssh.com",
    "hmac-sha2-512-etm@openssh.com",
];
const TLS_WIDE: &[&str] = &[
    "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
    "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
    "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
    "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
];
const TLS_PAIR: &[&str] = &[
    "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
    "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
];
const HOST_KEYS_2018: &[&str] = &[
    "ssh-rsa",
    "ecdsa-sha2-nistp256",
    "ecdsa-sha2-nistp384",
    "ecdsa-sha2-nistp521",
];
const HOST_KEYS_RSA2: &[&str] = &[
    "rsa-sha2-256",
    "rsa-sha2-512",
    "ecdsa-sha2-nistp256",
    "ecdsa-sha2-nistp384",
    "ecdsa-sha2-nistp521",
];
const HOST_KEYS_RSA2_ED25519: &[&str] = &[
    "rsa-sha2-256",
    "rsa-sha2-512",
    "ecdsa-sha2-nistp256",
    "ecdsa-sha2-nistp384",
    "ecdsa-sha2-nistp521",
    "ssh-ed25519",
];
const SFTP_FTPS: &[&str] = &["SFTP", "FTPS"];
const SFTP_ONLY: &[&str] = &["SFTP"];

const POLICIES: &[PolicySpec] = &[
    (
        "TransferSecurityPolicy-2018-11",
        false,
        SSH_CIPHERS_CLASSIC,
        SSH_KEXS_CLASSIC,
        SSH_MACS_FULL,
        TLS_WIDE,
        HOST_KEYS_2018,
        "SERVER",
        SFTP_FTPS,
    ),
    (
        "TransferSecurityPolicy-2020-06",
        false,
        SSH_CIPHERS_CLASSIC,
        SSH_KEXS_CLASSIC,
        SSH_MACS_FULL,
        TLS_PAIR,
        HOST_KEYS_RSA2,
        "SERVER",
        SFTP_FTPS,
    ),
    (
        "TransferSecurityPolicy-2022-03",
        false,
        SSH_CIPHERS_CLASSIC,
        SSH_KEXS_MODERN,
        SSH_MACS_ETM,
        TLS_PAIR,
        HOST_KEYS_RSA2_ED25519,
        "SERVER",
        SFTP_FTPS,
    ),
    (
        "TransferSecurityPolicy-2024-01",
        false,
        &[
            "aes128-gcm@openssh.com",
            "aes256-gcm@openssh.com",
            "chacha20-poly1305@openssh.com",
        ],
        &[
            "curve25519-sha256",
            "curve25519-sha256@libssh.org",
            "ecdh-sha2-nistp256",
            "ecdh-sha2-nistp384",
            "ecdh-sha2-nistp521",
            "diffie-hellman-group16-sha512",
            "diffie-hellman-group18-sha512",
        ],
        SSH_MACS_ETM,
        TLS_PAIR,
        HOST_KEYS_RSA2_ED25519,
        "SERVER",
        SFTP_FTPS,
    ),
    (
        "TransferSecurityPolicy-FIPS-2020-06",
        true,
        SSH_CIPHERS_CLASSIC,
        SSH_KEXS_CLASSIC,
        SSH_MACS_FULL,
        TLS_PAIR,
        HOST_KEYS_RSA2,
        "SERVER",
        SFTP_FTPS,
    ),
    (
        "TransferSecurityPolicy-FIPS-2024-01",
        true,
        &["aes128-gcm@openssh.com", "aes256-gcm@openssh.com"],
        SSH_KEXS_MODERN,
        SSH_MACS_ETM,
        TLS_PAIR,
        HOST_KEYS_RSA2,
        "SERVER",
        SFTP_FTPS,
    ),
    (
        "TransferSecurityPolicy-PQ-SSH-Experimental-2023-04",
        false,
        &["aes256-gcm@openssh.com"],
        &[
            "ecdh-nistp384-kyber-768r3-sha384-d00@openquantumsafe.org",
            "curve25519-sha256",
        ],
        &["hmac-sha2-512-etm@openssh.com"],
        &[],
        &[
            "ssh-ed25519",
            "ecdsa-sha2-nistp256",
            "rsa-sha2-256",
            "rsa-sha2-512",
        ],
        "SERVER",
        SFTP_ONLY,
    ),
    (
        "TransferSecurityPolicy-PQ-SSH-FIPS-Experimental-2023-04",
        true,
        &["aes256-gcm@openssh.com"],
        &["ecdh-nistp384-kyber-768r3-sha384-d00@openquantumsafe.org"],
        &["hmac-sha2-512-etm@openssh.com"],
        &[],
        &[
            "ecdsa-sha2-nistp256",
            "ecdsa-sha2-nistp384",
            "ecdsa-sha2-nistp521",
            "rsa-sha2-256",
            "rsa-sha2-512",
        ],
        "SERVER",
        SFTP_ONLY,
    ),
];

fn security_policy_names_sorted() -> Vec<String> {
    let mut names: Vec<String> = POLICIES.iter().map(|(name, ..)| name.to_string()).collect();
    names.sort();
    names
}

fn policy_to_dict(spec: &PolicySpec) -> Value {
    json!({
        "SecurityPolicyName": spec.0,
        "Fips": spec.1,
        "SshCiphers": spec.2,
        "SshKexs": spec.3,
        "SshMacs": spec.4,
        "TlsCiphers": spec.5,
        "SshHostKeyAlgorithms": spec.6,
        "Type": spec.7,
        "Protocols": spec.8,
    })
}

// ---------------------------------------------------------------------------
// Lookups
// ---------------------------------------------------------------------------

fn find_server<'a>(st: &'a TransferStore, server_id: &str) -> Result<&'a Value, Response> {
    st.servers
        .get(server_id)
        .ok_or_else(|| server_not_found(server_id))
}

fn find_server_mut<'a>(
    st: &'a mut TransferStore,
    server_id: &str,
) -> Result<&'a mut Value, Response> {
    st.servers
        .get_mut(server_id)
        .ok_or_else(|| server_not_found(server_id))
}

fn user_index(server: &Value, user_name: &str) -> Option<usize> {
    server["users"]
        .as_array()?
        .iter()
        .position(|u| u["user_name"].as_str() == Some(user_name))
}

fn access_index(server: &Value, external_id: &str) -> Option<usize> {
    server["accesses"]
        .as_array()?
        .iter()
        .position(|a| a["external_id"].as_str() == Some(external_id))
}

fn agreement_index(server: &Value, agreement_id: &str) -> Option<usize> {
    server["agreements"]
        .as_array()?
        .iter()
        .position(|a| a["agreement_id"].as_str() == Some(agreement_id))
}

fn host_key_index(server: &Value, host_key_id: &str) -> Option<usize> {
    server["host_keys"]
        .as_array()?
        .iter()
        .position(|k| k["host_key_id"].as_str() == Some(host_key_id))
}

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "transfer" {
        return None;
    }
    let body = parse_json_body(&req.body);
    Some(with_store(req, |st| handle(req, st, &body)))
}

fn handle(req: &Request, st: &mut TransferStore, p: &Value) -> Response {
    let account = req.account;
    let region = req.region.clone();
    match req.operation.as_str() {
        // --- Server operations -------------------------------------------
        "CreateServer" => {
            let rec = new_server_record(&region, account, p);
            let server_id = rec["server_id"].as_str().unwrap_or_default().to_string();
            let arn = rec["arn"].as_str().unwrap_or_default().to_string();
            add_tags(st, &arn, p.get("Tags"));
            st.servers.insert(server_id.clone(), rec);
            ok(&json!({"ServerId": server_id}))
        }
        "DescribeServer" => {
            let server_id = p
                .get("ServerId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            match find_server(st, server_id) {
                Ok(server) => ok(&json!({"Server": server_to_dict(server)})),
                Err(resp) => resp,
            }
        }
        "DeleteServer" => {
            let server_id = param_str(p, "ServerId").unwrap_or_default().to_string();
            let arn = match find_server(st, &server_id) {
                Ok(server) => server["arn"].as_str().unwrap_or_default().to_string(),
                Err(resp) => return resp,
            };
            st.tagger.remove(&arn);
            st.servers.remove(&server_id);
            ok(&json!({}))
        }
        "UpdateServer" => {
            let server_id = param_str(p, "ServerId").unwrap_or_default().to_string();
            let server = match find_server_mut(st, &server_id) {
                Ok(server) => server,
                Err(resp) => return resp,
            };
            for (req_key, rec_key) in [
                ("Certificate", "certificate"),
                ("EndpointType", "endpoint_type"),
                ("HostKey", "host_key_fingerprint"),
                ("LoggingRole", "logging_role"),
                (
                    "PostAuthenticationLoginBanner",
                    "post_authentication_login_banner",
                ),
                (
                    "PreAuthenticationLoginBanner",
                    "pre_authentication_login_banner",
                ),
                ("Protocols", "protocols"),
                ("SecurityPolicyName", "security_policy_name"),
                ("StructuredLogDestinations", "structured_log_destinations"),
            ] {
                if let Some(v) = param(p, req_key) {
                    server[rec_key] = v.clone();
                }
            }
            if let Some(d) = param(p, "EndpointDetails") {
                server["endpoint_details"] = normalize_endpoint_details(Some(d));
            }
            if let Some(d) = param(p, "IdentityProviderDetails") {
                server["identity_provider_details"] = normalize_identity_provider_details(Some(d));
            }
            if let Some(d) = param(p, "ProtocolDetails") {
                server["protocol_details"] = normalize_protocol_details(Some(d));
            }
            if let Some(d) = param(p, "S3StorageOptions") {
                server["s3_storage_options"] = normalize_s3_storage_options(Some(d));
            }
            if let Some(d) = param(p, "WorkflowDetails") {
                server["workflow_details"] = normalize_workflow_details(Some(d));
            }
            ok(&json!({"ServerId": server_id}))
        }
        "StartServer" => match find_server_mut(st, param_str(p, "ServerId").unwrap_or_default()) {
            Ok(server) => {
                server["state"] = json!("ONLINE");
                ok(&json!({}))
            }
            Err(resp) => resp,
        },
        "StopServer" => match find_server_mut(st, param_str(p, "ServerId").unwrap_or_default()) {
            Ok(server) => {
                server["state"] = json!("OFFLINE");
                ok(&json!({}))
            }
            Err(resp) => resp,
        },
        "ListServers" => {
            let servers: Vec<Value> = st.servers.values().map(server_to_short).collect();
            ok(&json!({"Servers": servers}))
        }

        // --- User operations ---------------------------------------------
        "CreateUser" => {
            let server_id = param_str(p, "ServerId").unwrap_or_default().to_string();
            let rec = user_record(&region, account, &server_id, p);
            let arn = rec["arn"].as_str().unwrap_or_default().to_string();
            add_tags(st, &arn, p.get("Tags"));
            let server = match find_server_mut(st, &server_id) {
                Ok(server) => server,
                Err(resp) => return resp,
            };
            server["users"]
                .as_array_mut()
                .expect("users array")
                .push(rec);
            server["user_count"] = json!(server["user_count"].as_u64().unwrap_or(0) + 1);
            ok(&json!({
                "ServerId": server_id,
                "UserName": p.get("UserName").cloned().unwrap_or(Value::Null),
            }))
        }
        "DescribeUser" => {
            let server_id = p
                .get("ServerId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let user_name = p
                .get("UserName")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let server = match find_server(st, server_id) {
                Ok(server) => server,
                Err(resp) => return resp,
            };
            match user_index(server, user_name) {
                Some(i) => ok(&json!({
                    "ServerId": server_id,
                    "User": user_to_dict(&server["users"][i]),
                })),
                None => user_not_found(user_name, server_id),
            }
        }
        "DeleteUser" => {
            let server_id = param_str(p, "ServerId").unwrap_or_default().to_string();
            let user_name = param_str(p, "UserName").unwrap_or_default().to_string();
            // moto looks the server up first (ServerNotFound), then the user.
            let found = match find_server(st, &server_id) {
                Ok(server) => user_index(server, &user_name).map(|i| {
                    (
                        i,
                        server["users"][i]["arn"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string(),
                    )
                }),
                Err(resp) => return resp,
            };
            let Some((index, arn)) = found else {
                return user_not_found(&user_name, &server_id);
            };
            if !arn.is_empty() {
                st.tagger.remove(&arn);
            }
            let server = st.servers.get_mut(&server_id).expect("checked above");
            server["users"].as_array_mut().expect("array").remove(index);
            server["user_count"] = json!(server["user_count"].as_u64().unwrap_or(0) - 1);
            ok(&json!({}))
        }
        "UpdateUser" => {
            let server_id = param_str(p, "ServerId").unwrap_or_default().to_string();
            let user_name = param_str(p, "UserName").unwrap_or_default().to_string();
            let server = match find_server_mut(st, &server_id) {
                Ok(server) => server,
                Err(resp) => return resp,
            };
            match user_index(server, &user_name) {
                Some(i) => {
                    for (req_key, rec_key) in [
                        ("HomeDirectory", "home_directory"),
                        ("HomeDirectoryType", "home_directory_type"),
                        ("Policy", "policy"),
                        ("Role", "role"),
                    ] {
                        if let Some(v) = param(p, req_key) {
                            server["users"][i][rec_key] = v.clone();
                        }
                    }
                    if let Some(mappings) =
                        param(p, "HomeDirectoryMappings").and_then(Value::as_array)
                    {
                        server["users"][i]["home_directory_mappings"] = Value::Array(
                            mappings
                                .iter()
                                .map(|m| {
                                    json!({
                                        "entry": m.get("Entry"),
                                        "target": m.get("Target"),
                                        "type": m.get("Type"),
                                    })
                                })
                                .collect(),
                        );
                    }
                    if let Some(posix) = param(p, "PosixProfile") {
                        server["users"][i]["posix_profile"] = normalize_posix(posix);
                    }
                    ok(&json!({"ServerId": server_id, "UserName": user_name}))
                }
                None => user_not_found(&user_name, &server_id),
            }
        }
        "ListUsers" => {
            let server_id = p
                .get("ServerId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let server = match find_server(st, server_id) {
                Ok(server) => server,
                Err(resp) => return resp,
            };
            let users: Vec<Value> = server["users"]
                .as_array()
                .map(|list| list.iter().map(user_to_short).collect())
                .unwrap_or_default();
            ok(&json!({"ServerId": server_id, "Users": users}))
        }

        // --- SSH public key operations -----------------------------------
        "ImportSshPublicKey" => {
            let server_id = p
                .get("ServerId")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let user_name = p
                .get("UserName")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let key_body = p
                .get("SshPublicKeyBody")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let server = match find_server_mut(st, &server_id) {
                Ok(server) => server,
                Err(resp) => return resp,
            };
            match user_index(server, &user_name) {
                Some(i) => {
                    let date_imported = now_unix_str();
                    let key_id = format!("{server_id}:{user_name}:public_key:{date_imported}");
                    server["users"][i]["ssh_public_keys"]
                        .as_array_mut()
                        .expect("array")
                        .push(json!({
                            "date_imported": date_imported,
                            "ssh_public_key_body": key_body,
                            "ssh_public_key_id": key_id,
                        }));
                    ok(&json!({
                        "ServerId": server_id,
                        "SshPublicKeyId": key_id,
                        "UserName": user_name,
                    }))
                }
                None => user_not_found(&user_name, &server_id),
            }
        }
        "DeleteSshPublicKey" => {
            let server_id = p
                .get("ServerId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let user_name = p
                .get("UserName")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let key_id = p
                .get("SshPublicKeyId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let server = match find_server_mut(st, server_id) {
                Ok(server) => server,
                Err(resp) => return resp,
            };
            match user_index(server, user_name) {
                Some(i) => {
                    let keys = server["users"][i]["ssh_public_keys"]
                        .as_array_mut()
                        .expect("array");
                    match keys.iter().position(|k| {
                        k.get("ssh_public_key_id").and_then(Value::as_str) == Some(key_id)
                    }) {
                        Some(j) => {
                            keys.remove(j);
                            ok(&json!({}))
                        }
                        None => public_key_not_found(user_name, server_id, key_id),
                    }
                }
                None => user_not_found(user_name, server_id),
            }
        }

        // --- Access operations -------------------------------------------
        "CreateAccess" => {
            let server_id = param_str(p, "ServerId").unwrap_or_default().to_string();
            let external_id = param_str(p, "ExternalId").unwrap_or_default().to_string();
            let rec = access_record(p);
            let server = match find_server_mut(st, &server_id) {
                Ok(server) => server,
                Err(resp) => return resp,
            };
            server["accesses"].as_array_mut().expect("array").push(rec);
            ok(&json!({"ServerId": server_id, "ExternalId": external_id}))
        }
        "DescribeAccess" => {
            let server_id = p
                .get("ServerId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let external_id = p
                .get("ExternalId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let server = match find_server(st, server_id) {
                Ok(server) => server,
                Err(resp) => return resp,
            };
            match access_index(server, external_id) {
                Some(i) => ok(&json!({
                    "ServerId": server_id,
                    "Access": access_to_dict(&server["accesses"][i]),
                })),
                None => access_not_found(server_id, external_id),
            }
        }
        "DeleteAccess" => {
            let server_id = param_str(p, "ServerId").unwrap_or_default().to_string();
            let external_id = param_str(p, "ExternalId").unwrap_or_default().to_string();
            let server = match find_server_mut(st, &server_id) {
                Ok(server) => server,
                Err(resp) => return resp,
            };
            match access_index(server, &external_id) {
                Some(i) => {
                    server["accesses"].as_array_mut().expect("array").remove(i);
                    ok(&json!({}))
                }
                None => access_not_found(&server_id, &external_id),
            }
        }
        "UpdateAccess" => {
            let server_id = param_str(p, "ServerId").unwrap_or_default().to_string();
            let external_id = param_str(p, "ExternalId").unwrap_or_default().to_string();
            let server = match find_server_mut(st, &server_id) {
                Ok(server) => server,
                Err(resp) => return resp,
            };
            match access_index(server, &external_id) {
                Some(i) => {
                    for (req_key, rec_key) in [
                        ("HomeDirectory", "home_directory"),
                        ("HomeDirectoryType", "home_directory_type"),
                        ("Policy", "policy"),
                        ("Role", "role"),
                    ] {
                        if let Some(v) = param(p, req_key) {
                            server["accesses"][i][rec_key] = v.clone();
                        }
                    }
                    if let Some(mappings) =
                        param(p, "HomeDirectoryMappings").and_then(Value::as_array)
                    {
                        server["accesses"][i]["home_directory_mappings"] = Value::Array(
                            mappings
                                .iter()
                                .map(
                                    |m| json!({"entry": m.get("Entry"), "target": m.get("Target")}),
                                )
                                .collect(),
                        );
                    }
                    if let Some(posix) = param(p, "PosixProfile") {
                        server["accesses"][i]["posix_profile"] = normalize_posix(posix);
                    }
                    ok(&json!({"ServerId": server_id, "ExternalId": external_id}))
                }
                None => access_not_found(&server_id, &external_id),
            }
        }
        "ListAccesses" => {
            let server_id = p
                .get("ServerId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let server = match find_server(st, server_id) {
                Ok(server) => server,
                Err(resp) => return resp,
            };
            let accesses: Vec<Value> = server["accesses"]
                .as_array()
                .map(|list| list.iter().map(access_to_short).collect())
                .unwrap_or_default();
            ok(&json!({"ServerId": server_id, "Accesses": accesses}))
        }

        // --- Agreement operations ----------------------------------------
        "CreateAgreement" => {
            let server_id = param_str(p, "ServerId").unwrap_or_default().to_string();
            let rec = agreement_record(&region, account, &server_id, p);
            let arn = rec["arn"].as_str().unwrap_or_default().to_string();
            let agreement_id = rec["agreement_id"].as_str().unwrap_or_default().to_string();
            add_tags(st, &arn, p.get("Tags"));
            let server = match find_server_mut(st, &server_id) {
                Ok(server) => server,
                Err(resp) => return resp,
            };
            server["agreements"]
                .as_array_mut()
                .expect("array")
                .push(rec);
            ok(&json!({"AgreementId": agreement_id}))
        }
        "DescribeAgreement" => {
            let server_id = p
                .get("ServerId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let agreement_id = p
                .get("AgreementId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let server = match find_server(st, server_id) {
                Ok(server) => server,
                Err(resp) => return resp,
            };
            match agreement_index(server, agreement_id) {
                Some(i) => ok(&json!({"Agreement": agreement_to_dict(&server["agreements"][i])})),
                None => resource_not_found("Agreement", agreement_id),
            }
        }
        "DeleteAgreement" => {
            let server_id = param_str(p, "ServerId").unwrap_or_default().to_string();
            let agreement_id = param_str(p, "AgreementId").unwrap_or_default().to_string();
            // moto looks the server up first (ServerNotFound), then the item.
            let found = match find_server(st, &server_id) {
                Ok(server) => agreement_index(server, &agreement_id).map(|i| {
                    (
                        i,
                        server["agreements"][i]["arn"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string(),
                    )
                }),
                Err(resp) => return resp,
            };
            let Some((index, arn)) = found else {
                return resource_not_found("Agreement", &agreement_id);
            };
            if !arn.is_empty() {
                st.tagger.remove(&arn);
            }
            let server = st.servers.get_mut(&server_id).expect("checked above");
            server["agreements"]
                .as_array_mut()
                .expect("array")
                .remove(index);
            ok(&json!({}))
        }
        "UpdateAgreement" => {
            let server_id = param_str(p, "ServerId").unwrap_or_default().to_string();
            let agreement_id = param_str(p, "AgreementId").unwrap_or_default().to_string();
            let server = match find_server_mut(st, &server_id) {
                Ok(server) => server,
                Err(resp) => return resp,
            };
            match agreement_index(server, &agreement_id) {
                Some(i) => {
                    for (req_key, rec_key) in [
                        ("Description", "description"),
                        ("Status", "status"),
                        ("LocalProfileId", "local_profile_id"),
                        ("PartnerProfileId", "partner_profile_id"),
                        ("BaseDirectory", "base_directory"),
                        ("AccessRole", "access_role"),
                        ("PreserveFilename", "preserve_filename"),
                        ("EnforceMessageSigning", "enforce_message_signing"),
                        ("CustomDirectories", "custom_directories"),
                    ] {
                        if let Some(v) = param(p, req_key) {
                            server["agreements"][i][rec_key] = v.clone();
                        }
                    }
                    ok(&json!({"AgreementId": agreement_id}))
                }
                None => resource_not_found("Agreement", &agreement_id),
            }
        }
        "ListAgreements" => {
            let server_id = p
                .get("ServerId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let server = match find_server(st, server_id) {
                Ok(server) => server,
                Err(resp) => return resp,
            };
            let agreements: Vec<Value> = server["agreements"]
                .as_array()
                .map(|list| list.iter().map(agreement_to_short).collect())
                .unwrap_or_default();
            ok(&json!({"Agreements": agreements}))
        }

        // --- Certificate operations --------------------------------------
        "ImportCertificate" => {
            let rec = certificate_record(&region, account, p);
            let certificate_id = rec["certificate_id"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            let arn = rec["arn"].as_str().unwrap_or_default().to_string();
            add_tags(st, &arn, p.get("Tags"));
            st.certificates.insert(certificate_id.clone(), rec);
            ok(&json!({"CertificateId": certificate_id}))
        }
        "DescribeCertificate" => {
            let cert_id = p
                .get("CertificateId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            match st.certificates.get(cert_id) {
                Some(rec) => ok(&json!({"Certificate": certificate_to_dict(rec)})),
                None => resource_not_found("Certificate", cert_id),
            }
        }
        "DeleteCertificate" => {
            let cert_id = param_str(p, "CertificateId")
                .unwrap_or_default()
                .to_string();
            let arn = match st.certificates.get(&cert_id) {
                Some(rec) => rec["arn"].as_str().unwrap_or_default().to_string(),
                None => return resource_not_found("Certificate", &cert_id),
            };
            st.tagger.remove(&arn);
            st.certificates.remove(&cert_id);
            ok(&json!({}))
        }
        "UpdateCertificate" => {
            let cert_id = param_str(p, "CertificateId")
                .unwrap_or_default()
                .to_string();
            let Some(rec) = st.certificates.get_mut(&cert_id) else {
                return resource_not_found("Certificate", &cert_id);
            };
            for (req_key, rec_key) in [
                ("ActiveDate", "active_date"),
                ("InactiveDate", "inactive_date"),
                ("Description", "description"),
            ] {
                if let Some(v) = param(p, req_key) {
                    rec[rec_key] = v.clone();
                }
            }
            ok(&json!({"CertificateId": cert_id}))
        }
        "ListCertificates" => {
            let certs: Vec<Value> = st.certificates.values().map(certificate_to_short).collect();
            ok(&json!({"Certificates": certs}))
        }

        // --- Connector operations ----------------------------------------
        "CreateConnector" => {
            let rec = connector_record(&region, account, p);
            let connector_id = rec["connector_id"].as_str().unwrap_or_default().to_string();
            let arn = rec["arn"].as_str().unwrap_or_default().to_string();
            add_tags(st, &arn, p.get("Tags"));
            st.connectors.insert(connector_id.clone(), rec);
            ok(&json!({"ConnectorId": connector_id}))
        }
        "DescribeConnector" => {
            let connector_id = p
                .get("ConnectorId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            match st.connectors.get(connector_id) {
                Some(rec) => ok(&json!({"Connector": connector_to_dict(rec)})),
                None => connector_not_found(connector_id),
            }
        }
        "DeleteConnector" => {
            let connector_id = param_str(p, "ConnectorId").unwrap_or_default().to_string();
            let arn = match st.connectors.get(&connector_id) {
                Some(rec) => rec["arn"].as_str().unwrap_or_default().to_string(),
                None => return connector_not_found(&connector_id),
            };
            st.tagger.remove(&arn);
            st.connectors.remove(&connector_id);
            ok(&json!({}))
        }
        "UpdateConnector" => {
            let connector_id = param_str(p, "ConnectorId").unwrap_or_default().to_string();
            let Some(rec) = st.connectors.get_mut(&connector_id) else {
                return connector_not_found(&connector_id);
            };
            for (req_key, rec_key) in [
                ("Url", "url"),
                ("AccessRole", "access_role"),
                ("LoggingRole", "logging_role"),
                ("SecurityPolicyName", "security_policy_name"),
            ] {
                if let Some(v) = param(p, req_key) {
                    rec[rec_key] = v.clone();
                }
            }
            if let Some(cfg) = param(p, "As2Config") {
                rec["as2_config"] = snakify(cfg);
            }
            if let Some(cfg) = param(p, "SftpConfig") {
                rec["sftp_config"] = snakify(cfg);
            }
            ok(&json!({"ConnectorId": connector_id}))
        }
        "ListConnectors" => {
            let connectors: Vec<Value> = st.connectors.values().map(connector_to_short).collect();
            ok(&json!({"Connectors": connectors}))
        }
        "TestConnection" => {
            let connector_id = p
                .get("ConnectorId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if !st.connectors.contains_key(connector_id) {
                return resource_not_found("Connector", connector_id);
            }
            ok(&json!({"ConnectorId": connector_id, "Status": "OK", "StatusMessage": ""}))
        }

        // --- Profile operations ------------------------------------------
        "CreateProfile" => {
            let rec = profile_record(&region, account, p);
            let profile_id = rec["profile_id"].as_str().unwrap_or_default().to_string();
            let arn = rec["arn"].as_str().unwrap_or_default().to_string();
            add_tags(st, &arn, p.get("Tags"));
            st.profiles.insert(profile_id.clone(), rec);
            ok(&json!({"ProfileId": profile_id}))
        }
        "DescribeProfile" => {
            let profile_id = p
                .get("ProfileId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            match st.profiles.get(profile_id) {
                Some(rec) => ok(&json!({"Profile": profile_to_dict(rec)})),
                None => resource_not_found("Profile", profile_id),
            }
        }
        "DeleteProfile" => {
            let profile_id = param_str(p, "ProfileId").unwrap_or_default().to_string();
            let arn = match st.profiles.get(&profile_id) {
                Some(rec) => rec["arn"].as_str().unwrap_or_default().to_string(),
                None => return resource_not_found("Profile", &profile_id),
            };
            st.tagger.remove(&arn);
            st.profiles.remove(&profile_id);
            ok(&json!({}))
        }
        "UpdateProfile" => {
            let profile_id = param_str(p, "ProfileId").unwrap_or_default().to_string();
            let Some(rec) = st.profiles.get_mut(&profile_id) else {
                return resource_not_found("Profile", &profile_id);
            };
            if let Some(ids) = param(p, "CertificateIds") {
                rec["certificate_ids"] = ids.clone();
            }
            ok(&json!({"ProfileId": profile_id}))
        }
        "ListProfiles" => {
            // moto filters with `if profile_type:` truthiness.
            let profile_type = param_str(p, "ProfileType").filter(|s| !s.is_empty());
            let profiles: Vec<Value> = st
                .profiles
                .values()
                .filter(|rec| {
                    profile_type
                        .map(|ty| rec["profile_type"].as_str() == Some(ty))
                        .unwrap_or(true)
                })
                .map(profile_to_short)
                .collect();
            ok(&json!({"Profiles": profiles}))
        }

        // --- Workflow operations -----------------------------------------
        "CreateWorkflow" => {
            let rec = workflow_record(&region, account, p);
            let workflow_id = rec["workflow_id"].as_str().unwrap_or_default().to_string();
            let arn = rec["arn"].as_str().unwrap_or_default().to_string();
            add_tags(st, &arn, p.get("Tags"));
            st.workflows.insert(workflow_id.clone(), rec);
            ok(&json!({"WorkflowId": workflow_id}))
        }
        "DescribeWorkflow" => {
            let workflow_id = p
                .get("WorkflowId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            match st.workflows.get(workflow_id) {
                Some(rec) => ok(&json!({"Workflow": workflow_to_dict(rec)})),
                None => resource_not_found("Workflow", workflow_id),
            }
        }
        "DeleteWorkflow" => {
            let workflow_id = param_str(p, "WorkflowId").unwrap_or_default().to_string();
            let arn = match st.workflows.get(&workflow_id) {
                Some(rec) => rec["arn"].as_str().unwrap_or_default().to_string(),
                None => return resource_not_found("Workflow", &workflow_id),
            };
            st.tagger.remove(&arn);
            st.workflows.remove(&workflow_id);
            ok(&json!({}))
        }
        "ListWorkflows" => {
            let workflows: Vec<Value> = st.workflows.values().map(workflow_to_short).collect();
            ok(&json!({"Workflows": workflows}))
        }
        "SendWorkflowStepState" => {
            let workflow_id = p
                .get("WorkflowId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if !st.workflows.contains_key(workflow_id) {
                return resource_not_found("Workflow", workflow_id);
            }
            // moto only validates that the workflow exists.
            ok(&json!({}))
        }
        "DescribeExecution" => {
            let workflow_id = p
                .get("WorkflowId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let execution_id = p
                .get("ExecutionId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if !st.workflows.contains_key(workflow_id) {
                return resource_not_found("Workflow", workflow_id);
            }
            ok(&json!({
                "WorkflowId": workflow_id,
                "Execution": {
                    "ExecutionId": execution_id,
                    "Status": "COMPLETED",
                    "Results": {},
                },
            }))
        }
        "ListExecutions" => {
            let workflow_id = p
                .get("WorkflowId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if !st.workflows.contains_key(workflow_id) {
                return resource_not_found("Workflow", workflow_id);
            }
            ok(&json!({"WorkflowId": workflow_id, "Executions": []}))
        }

        // --- HostKey operations ------------------------------------------
        "ImportHostKey" => {
            let server_id = param_str(p, "ServerId").unwrap_or_default().to_string();
            let rec = host_key_record(&region, account, &server_id, p);
            let host_key_id = rec["host_key_id"].as_str().unwrap_or_default().to_string();
            let arn = rec["arn"].as_str().unwrap_or_default().to_string();
            add_tags(st, &arn, p.get("Tags"));
            let server = match find_server_mut(st, &server_id) {
                Ok(server) => server,
                Err(resp) => return resp,
            };
            server["host_keys"].as_array_mut().expect("array").push(rec);
            ok(&json!({"ServerId": server_id, "HostKeyId": host_key_id}))
        }
        "DescribeHostKey" => {
            let server_id = p
                .get("ServerId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let host_key_id = p
                .get("HostKeyId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let server = match find_server(st, server_id) {
                Ok(server) => server,
                Err(resp) => return resp,
            };
            match host_key_index(server, host_key_id) {
                Some(i) => ok(&json!({"HostKey": host_key_to_dict(&server["host_keys"][i])})),
                None => resource_not_found("HostKey", host_key_id),
            }
        }
        "DeleteHostKey" => {
            let server_id = param_str(p, "ServerId").unwrap_or_default().to_string();
            let host_key_id = param_str(p, "HostKeyId").unwrap_or_default().to_string();
            // moto looks the server up first (ServerNotFound), then the key.
            let found = match find_server(st, &server_id) {
                Ok(server) => host_key_index(server, &host_key_id).map(|i| {
                    (
                        i,
                        server["host_keys"][i]["arn"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string(),
                    )
                }),
                Err(resp) => return resp,
            };
            let Some((index, arn)) = found else {
                return resource_not_found("HostKey", &host_key_id);
            };
            if !arn.is_empty() {
                st.tagger.remove(&arn);
            }
            let server = st.servers.get_mut(&server_id).expect("checked above");
            server["host_keys"]
                .as_array_mut()
                .expect("array")
                .remove(index);
            ok(&json!({}))
        }
        "UpdateHostKey" => {
            let server_id = param_str(p, "ServerId").unwrap_or_default().to_string();
            let host_key_id = param_str(p, "HostKeyId").unwrap_or_default().to_string();
            let description = param_str(p, "Description").unwrap_or_default();
            let server = match find_server_mut(st, &server_id) {
                Ok(server) => server,
                Err(resp) => return resp,
            };
            match host_key_index(server, &host_key_id) {
                Some(i) => {
                    server["host_keys"][i]["description"] = json!(description);
                    ok(&json!({"ServerId": server_id, "HostKeyId": host_key_id}))
                }
                None => resource_not_found("HostKey", &host_key_id),
            }
        }
        "ListHostKeys" => {
            let server_id = p
                .get("ServerId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let server = match find_server(st, server_id) {
                Ok(server) => server,
                Err(resp) => return resp,
            };
            let host_keys: Vec<Value> = server["host_keys"]
                .as_array()
                .map(|list| list.iter().map(host_key_to_short).collect())
                .unwrap_or_default();
            ok(&json!({"ServerId": server_id, "HostKeys": host_keys}))
        }

        // --- WebApp operations -------------------------------------------
        "CreateWebApp" => {
            let rec = web_app_record(&region, account, p);
            let web_app_id = rec["web_app_id"].as_str().unwrap_or_default().to_string();
            let arn = rec["arn"].as_str().unwrap_or_default().to_string();
            add_tags(st, &arn, p.get("Tags"));
            st.web_apps.insert(web_app_id.clone(), rec);
            ok(&json!({"WebAppId": web_app_id}))
        }
        "DescribeWebApp" => {
            let web_app_id = p
                .get("WebAppId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            match st.web_apps.get(web_app_id) {
                Some(rec) => ok(&json!({"WebApp": web_app_to_dict(rec)})),
                None => resource_not_found("WebApp", web_app_id),
            }
        }
        "DeleteWebApp" => {
            let web_app_id = param_str(p, "WebAppId").unwrap_or_default().to_string();
            let arn = match st.web_apps.get(&web_app_id) {
                Some(rec) => rec["arn"].as_str().unwrap_or_default().to_string(),
                None => return resource_not_found("WebApp", &web_app_id),
            };
            st.tagger.remove(&arn);
            st.web_apps.remove(&web_app_id);
            ok(&json!({}))
        }
        "UpdateWebApp" => {
            let web_app_id = param_str(p, "WebAppId").unwrap_or_default().to_string();
            let Some(rec) = st.web_apps.get_mut(&web_app_id) else {
                return resource_not_found("WebApp", &web_app_id);
            };
            for req_key in [
                "AccessEndpoint",
                "WebAppUnits",
                "WebAppEndpointPolicy",
                "EndpointDetails",
            ] {
                if let Some(v) = param(p, req_key) {
                    let snake = camel_to_snake(req_key);
                    rec[&snake] = v.clone();
                }
            }
            if let Some(details) = param(p, "IdentityProviderDetails") {
                rec["identity_provider_details"] = details.clone();
            }
            ok(&json!({"WebAppId": web_app_id}))
        }
        "ListWebApps" => {
            let web_apps: Vec<Value> = st.web_apps.values().map(web_app_to_short).collect();
            ok(&json!({"WebApps": web_apps}))
        }
        "UpdateWebAppCustomization" => {
            let web_app_id = param_str(p, "WebAppId").unwrap_or_default().to_string();
            let arn = match st.web_apps.get(&web_app_id) {
                Some(rec) => format!("{}/customization", rec["arn"].as_str().unwrap_or_default()),
                None => return resource_not_found("WebApp", &web_app_id),
            };
            let custom = json!({
                "web_app_id": web_app_id,
                "arn": arn,
                "title": param_str(p, "Title"),
                "logo_file": param(p, "LogoFile")
                    .and_then(Value::as_str)
                    .map(base64_decode_bytes),
                "favicon_file": param(p, "FaviconFile")
                    .and_then(Value::as_str)
                    .map(base64_decode_bytes),
            });
            let Some(rec) = st.web_apps.get_mut(&web_app_id) else {
                return resource_not_found("WebApp", &web_app_id);
            };
            rec["customization"] = custom;
            ok(&json!({"WebAppId": web_app_id}))
        }
        "DescribeWebAppCustomization" => {
            let web_app_id = p
                .get("WebAppId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let Some(rec) = st.web_apps.get(web_app_id) else {
                return resource_not_found("WebApp", web_app_id);
            };
            match rec.get("customization").and_then(Value::as_object) {
                Some(_) => ok(&json!({
                    "WebAppCustomization": web_app_customization_to_dict(&rec["customization"]),
                })),
                None => resource_not_found("WebAppCustomization", web_app_id),
            }
        }
        "DeleteWebAppCustomization" => {
            let web_app_id = param_str(p, "WebAppId").unwrap_or_default().to_string();
            let Some(rec) = st.web_apps.get_mut(&web_app_id) else {
                return resource_not_found("WebApp", &web_app_id);
            };
            rec["customization"] = Value::Null;
            ok(&json!({}))
        }

        // --- Security Policy operations ----------------------------------
        "DescribeSecurityPolicy" => {
            let name = p
                .get("SecurityPolicyName")
                .and_then(Value::as_str)
                .unwrap_or_default();
            match POLICIES.iter().find(|(n, ..)| *n == name) {
                Some(spec) => ok(&json!({"SecurityPolicy": policy_to_dict(spec)})),
                None => resource_not_found("SecurityPolicy", name),
            }
        }
        "ListSecurityPolicies" => {
            ok(&json!({"SecurityPolicyNames": security_policy_names_sorted()}))
        }

        // --- Tag operations ----------------------------------------------
        "TagResource" => {
            let arn = p
                .get("Arn")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            add_tags(st, &arn, p.get("Tags"));
            sync_tags_to_resource(st, &arn);
            ok(&json!({}))
        }
        "UntagResource" => {
            let arn = p
                .get("Arn")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            remove_tags(st, &arn, p.get("TagKeys"));
            sync_tags_to_resource(st, &arn);
            ok(&json!({}))
        }
        "ListTagsForResource" => {
            let arn = p.get("Arn").and_then(Value::as_str).unwrap_or_default();
            ok(&json!({"Arn": arn, "Tags": tags_for_arn(st, arn)}))
        }

        // --- Test Identity Provider --------------------------------------
        "TestIdentityProvider" => {
            let server_id = p
                .get("ServerId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if let Err(resp) = find_server(st, server_id) {
                return resp;
            }
            ok(&json!({"Response": "", "StatusCode": 200, "Message": "", "Url": ""}))
        }

        // --- Connector file operations -----------------------------------
        "StartFileTransfer" => {
            let connector_id = p
                .get("ConnectorId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if !st.connectors.contains_key(connector_id) {
                return resource_not_found("Connector", connector_id);
            }
            ok(&json!({"TransferId": format!("t-{}", random_hex(17))}))
        }
        "StartDirectoryListing" => {
            let connector_id = p
                .get("ConnectorId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if !st.connectors.contains_key(connector_id) {
                return resource_not_found("Connector", connector_id);
            }
            let listing_id = format!("listing-{}", random_hex(17));
            let output_directory_path = p
                .get("OutputDirectoryPath")
                .and_then(Value::as_str)
                .unwrap_or_default();
            ok(&json!({
                "ListingId": listing_id,
                "OutputFileName": format!("{output_directory_path}/{listing_id}.json"),
            }))
        }
        "StartRemoteDelete" => {
            let connector_id = p
                .get("ConnectorId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if !st.connectors.contains_key(connector_id) {
                return resource_not_found("Connector", connector_id);
            }
            ok(&json!({"DeleteId": format!("delete-{}", random_hex(17))}))
        }
        "StartRemoteMove" => {
            let connector_id = p
                .get("ConnectorId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if !st.connectors.contains_key(connector_id) {
                return resource_not_found("Connector", connector_id);
            }
            ok(&json!({"MoveId": format!("move-{}", random_hex(17))}))
        }
        "ListFileTransferResults" => {
            let connector_id = p
                .get("ConnectorId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if !st.connectors.contains_key(connector_id) {
                return resource_not_found("Connector", connector_id);
            }
            ok(&json!({"FileTransferResults": []}))
        }

        _ => json_error(
            501,
            "NotImplemented",
            &format!("The {} action has not been implemented", req.operation),
        ),
    }
}

/// base64-decode a request blob; decoded bytes are stored and re-encoded on
/// describe (moto decodes the request blob and re-encodes for the response).
fn base64_decode_bytes(value: &str) -> Vec<u8> {
    B64.decode(value).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn req(op: &str, p: Value) -> Request {
        Request {
            service: "transfer".into(),
            operation: op.into(),
            method: "POST".into(),
            path: "/".into(),
            query_string: String::new(),
            headers: Default::default(),
            body: Bytes::from(serde_json::to_vec(&p).unwrap_or_default()),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    /// Runs one op against a fresh store; returns (status, body json).
    fn run(op: &str, p: Value) -> (u16, Value) {
        let r = req(op, p);
        let resp = dispatch(&r).expect("armed");
        let body: Value = serde_json::from_slice(&resp.body).unwrap_or(Value::Null);
        (resp.status, body)
    }

    fn create_server() -> String {
        let (status, body) = run(
            "CreateServer",
            json!({"IdentityProviderType": "SERVICE_MANAGED"}),
        );
        assert_eq!(status, 200);
        body["ServerId"].as_str().unwrap().to_string()
    }

    fn create_user(server_id: &str, user_name: &str) -> Value {
        let (status, body) = run(
            "CreateUser",
            json!({
                "ServerId": server_id,
                "UserName": user_name,
                "Role": "arn:aws:iam::123456789012:role/r",
            }),
        );
        assert_eq!(status, 200);
        body
    }

    #[test]
    fn unknown_operation_is_501() {
        let (status, body) = run("NotAnOperation", json!({}));
        assert_eq!(status, 501);
        assert_eq!(body["__type"], "NotImplemented");
    }

    #[test]
    fn server_lifecycle_and_state() {
        let server_id = create_server();
        assert!(server_id.starts_with("s-"), "{server_id}");

        let (status, body) = run("DescribeServer", json!({"ServerId": server_id}));
        assert_eq!(status, 200);
        let server = &body["Server"];
        assert_eq!(server["ServerId"], server_id.as_str());
        assert_eq!(server["State"], "ONLINE");
        assert_eq!(server["IdentityProviderType"], "SERVICE_MANAGED");
        assert_eq!(server["UserCount"], 0);
        assert!(server["Arn"]
            .as_str()
            .unwrap()
            .starts_with("arn:aws:transfer:"));

        run("StopServer", json!({"ServerId": server_id}));
        let (_, body) = run("DescribeServer", json!({"ServerId": server_id}));
        assert_eq!(body["Server"]["State"], "OFFLINE");
        run("StartServer", json!({"ServerId": server_id}));
        let (_, body) = run("DescribeServer", json!({"ServerId": server_id}));
        assert_eq!(body["Server"]["State"], "ONLINE");

        let (status, body) = run(
            "UpdateServer",
            json!({"ServerId": server_id, "LoggingRole": "arn:aws:iam::1:role/log"}),
        );
        assert_eq!(status, 200);
        assert_eq!(body["ServerId"], server_id.as_str());
        let (_, body) = run("DescribeServer", json!({"ServerId": server_id}));
        assert_eq!(body["Server"]["LoggingRole"], "arn:aws:iam::1:role/log");

        run("DeleteServer", json!({"ServerId": server_id}));
        let (status, err) = run("DescribeServer", json!({"ServerId": server_id}));
        assert_eq!(status, 400);
        assert_eq!(err["__type"], "ServerNotFound");
    }

    #[test]
    fn create_user_counts_and_not_found() {
        let server_id = create_server();
        create_user(&server_id, "alice");
        let (_, body) = run("DescribeServer", json!({"ServerId": server_id}));
        assert_eq!(body["Server"]["UserCount"], 1);
        create_user(&server_id, "bob");
        let (_, body) = run("DescribeServer", json!({"ServerId": server_id}));
        assert_eq!(body["Server"]["UserCount"], 2);

        let (_, body) = run(
            "DescribeUser",
            json!({"ServerId": server_id, "UserName": "alice"}),
        );
        let user = &body["User"];
        assert_eq!(user["UserName"], "alice");
        assert!(user["Arn"].as_str().unwrap().contains(&server_id));

        run(
            "DeleteUser",
            json!({"ServerId": server_id, "UserName": "alice"}),
        );
        let (_, body) = run("DescribeServer", json!({"ServerId": server_id}));
        assert_eq!(body["Server"]["UserCount"], 1);

        let (status, err) = run(
            "DescribeUser",
            json!({"ServerId": server_id, "UserName": "ghost"}),
        );
        assert_eq!(status, 400);
        assert_eq!(err["__type"], "UserNotFound");

        let (status, err) = run(
            "CreateUser",
            json!({"ServerId": "s-00000000000000000", "UserName": "x", "Role": "r"}),
        );
        assert_eq!(status, 400);
        assert_eq!(err["__type"], "ServerNotFound");
    }

    #[test]
    fn ssh_public_key_import_and_delete() {
        let server_id = create_server();
        create_user(&server_id, "keys");
        let (status, body) = run(
            "ImportSshPublicKey",
            json!({
                "ServerId": server_id,
                "UserName": "keys",
                "SshPublicKeyBody": "ssh-rsa AAAA test@test",
            }),
        );
        assert_eq!(status, 200);
        let key_id = body["SshPublicKeyId"].as_str().unwrap().to_string();
        assert!(
            key_id.starts_with(&format!("{server_id}:keys:public_key:")),
            "{key_id}"
        );

        let (_, body) = run(
            "DescribeUser",
            json!({"ServerId": server_id, "UserName": "keys"}),
        );
        let keys = body["User"]["SshPublicKeys"].as_array().unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0]["SshPublicKeyBody"], "ssh-rsa AAAA test@test");

        run(
            "DeleteSshPublicKey",
            json!({"ServerId": server_id, "UserName": "keys", "SshPublicKeyId": key_id}),
        );
        let (_, body) = run(
            "DescribeUser",
            json!({"ServerId": server_id, "UserName": "keys"}),
        );
        assert_eq!(body["User"]["SshPublicKeys"].as_array().unwrap().len(), 0);

        let (status, err) = run(
            "DeleteSshPublicKey",
            json!({"ServerId": server_id, "UserName": "keys", "SshPublicKeyId": "key-0"}),
        );
        assert_eq!(status, 400);
        assert_eq!(err["__type"], "PublicKeyNotFound");

        let (status, err) = run(
            "ImportSshPublicKey",
            json!({"ServerId": server_id, "UserName": "ghost", "SshPublicKeyBody": "k"}),
        );
        assert_eq!(status, 400);
        assert_eq!(err["__type"], "UserNotFound");
    }

    #[test]
    fn accesses_agreements_and_not_found_codes() {
        let server_id = create_server();
        let ext_id = "S-1-1-12-1";
        run(
            "CreateAccess",
            json!({
                "ServerId": server_id,
                "ExternalId": ext_id,
                "Role": "arn:aws:iam::1:role/r",
                "HomeDirectory": "/bucket",
            }),
        );
        let (_, body) = run("ListAccesses", json!({"ServerId": server_id}));
        assert_eq!(body["Accesses"].as_array().unwrap().len(), 1);

        let (_, body) = run(
            "DescribeAccess",
            json!({"ServerId": server_id, "ExternalId": ext_id}),
        );
        assert_eq!(body["Access"]["HomeDirectory"], "/bucket");

        let (status, err) = run(
            "DescribeAccess",
            json!({"ServerId": server_id, "ExternalId": "nope"}),
        );
        // AccessNotFound: HTTP 400 with a ResourceNotFoundException type.
        assert_eq!(status, 400);
        assert_eq!(err["__type"], "ResourceNotFoundException");

        // Agreements default to ACTIVE; missing agreement is 404.
        run(
            "CreateAgreement",
            json!({
                "ServerId": server_id,
                "LocalProfileId": "p-1",
                "PartnerProfileId": "p-2",
                "AccessRole": "arn:aws:iam::1:role/r",
                "BaseDirectory": "/bucket",
            }),
        );
        let (_, body) = run("ListAgreements", json!({"ServerId": server_id}));
        let agreement = &body["Agreements"].as_array().unwrap()[0];
        assert_eq!(agreement["Status"], "ACTIVE");
        // BaseDirectory lives on the full dict only, like moto's to_short_dict.
        let agreement_id = agreement["AgreementId"].as_str().unwrap().to_string();
        let (_, body) = run(
            "DescribeAgreement",
            json!({"ServerId": server_id, "AgreementId": agreement_id}),
        );
        assert_eq!(body["Agreement"]["BaseDirectory"], "/bucket");
        assert_eq!(body["Agreement"]["LocalProfileId"], "p-1");
        let (status, err) = run(
            "DescribeAgreement",
            json!({"ServerId": server_id, "AgreementId": "a-00000000000000000"}),
        );
        assert_eq!(status, 404);
        assert_eq!(err["__type"], "ResourceNotFoundException");
        assert_eq!(err["message"], "Agreement not found: a-00000000000000000");

        // Server must exist first for agreement ops.
        let (status, err) = run(
            "DeleteAgreement",
            json!({"ServerId": "s-00000000000000000", "AgreementId": agreement_id}),
        );
        assert_eq!(status, 400);
        assert_eq!(err["__type"], "ServerNotFound");
    }

    #[test]
    fn certificates_profiles_workflows() {
        let cert_body = "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----";
        let (status, body) = run(
            "ImportCertificate",
            json!({"Usage": "SIGNING", "Certificate": cert_body}),
        );
        assert_eq!(status, 200);
        let cert_id = body["CertificateId"].as_str().unwrap().to_string();
        assert!(cert_id.starts_with("cert-"));

        let (_, body) = run("DescribeCertificate", json!({"CertificateId": cert_id}));
        assert_eq!(body["Certificate"]["Usage"], "SIGNING");
        assert_eq!(body["Certificate"]["Type"], "CERTIFICATE");
        assert_eq!(body["Certificate"]["Status"], "ACTIVE");

        run("DeleteCertificate", json!({"CertificateId": cert_id}));
        let (status, err) = run("DescribeCertificate", json!({"CertificateId": cert_id}));
        assert_eq!(status, 404);
        assert_eq!(err["__type"], "ResourceNotFoundException");

        let (status, body) = run(
            "CreateProfile",
            json!({"As2Id": "as2-1", "ProfileType": "LOCAL"}),
        );
        assert_eq!(status, 200);
        let profile_id = body["ProfileId"].as_str().unwrap().to_string();
        let (_, body) = run("DescribeProfile", json!({"ProfileId": profile_id}));
        assert_eq!(body["Profile"]["ProfileType"], "LOCAL");
        assert_eq!(body["Profile"]["As2Id"], "as2-1");
        run("DeleteProfile", json!({"ProfileId": profile_id}));

        let (status, body) = run(
            "CreateWorkflow",
            json!({"Description": "wf", "Steps": [{"Type": "COPY"}]}),
        );
        assert_eq!(status, 200);
        let workflow_id = body["WorkflowId"].as_str().unwrap().to_string();
        let (_, body) = run("DescribeWorkflow", json!({"WorkflowId": workflow_id}));
        assert_eq!(body["Workflow"]["Description"], "wf");
        assert_eq!(body["Workflow"]["OnExceptionSteps"], json!([]));

        let fake_exec = "a".repeat(36);
        let (_, body) = run(
            "DescribeExecution",
            json!({"WorkflowId": workflow_id, "ExecutionId": fake_exec}),
        );
        assert_eq!(body["WorkflowId"], workflow_id.as_str());
        assert_eq!(body["Execution"]["Status"], "COMPLETED");

        let (status, err) = run(
            "SendWorkflowStepState",
            json!({"WorkflowId": "w-00000000000000000", "ExecutionId": "e", "Token": "t", "Status": "SUCCESS"}),
        );
        assert_eq!(status, 404);
        assert_eq!(err["__type"], "ResourceNotFoundException");
        run("DeleteWorkflow", json!({"WorkflowId": workflow_id}));
    }

    #[test]
    fn connectors_error_codes_and_snakified_config() {
        // Describe/update/delete: HTTP 400; test_connection: HTTP 404.
        let (status, err) = run(
            "DescribeConnector",
            json!({"ConnectorId": "c-00000000000000000"}),
        );
        assert_eq!(status, 400);
        assert_eq!(err["__type"], "ResourceNotFoundException");
        assert_eq!(
            err["message"],
            "Connector c-00000000000000000 does not exist."
        );
        let (status, _) = run(
            "TestConnection",
            json!({"ConnectorId": "c-00000000000000000"}),
        );
        assert_eq!(status, 404);

        let (status, body) = run(
            "CreateConnector",
            json!({
                "Url": "sftp://example.com",
                "As2Config": {"LocalProfileId": "p1", "MdnResponse": "SYNC"},
                "AccessRole": "arn:aws:iam::1:role/r",
            }),
        );
        assert_eq!(status, 200);
        let connector_id = body["ConnectorId"].as_str().unwrap().to_string();
        assert!(connector_id.starts_with("c-"));

        let (_, body) = run("DescribeConnector", json!({"ConnectorId": connector_id}));
        let connector = &body["Connector"];
        assert_eq!(connector["Url"], "sftp://example.com");
        // moto rewrites config keys snake_case.
        assert_eq!(connector["As2Config"]["local_profile_id"], "p1");
        assert_eq!(
            connector["ServiceManagedEgressIpAddresses"],
            json!(["127.0.0.1", "127.0.0.2"])
        );

        let (status, body) = run("StartFileTransfer", json!({"ConnectorId": connector_id}));
        assert_eq!(status, 200);
        assert!(body["TransferId"].as_str().unwrap().starts_with("t-"));

        run("DeleteConnector", json!({"ConnectorId": connector_id}));
        let (status, _) = run(
            "ListFileTransferResults",
            json!({"ConnectorId": connector_id, "TransferId": "t-1"}),
        );
        assert_eq!(status, 404);
    }

    #[test]
    fn tags_merge_remove_and_sync_into_describe() {
        let server_id = create_server();
        let (_, body) = run("DescribeServer", json!({"ServerId": server_id}));
        let arn = body["Server"]["Arn"].as_str().unwrap().to_string();

        run(
            "TagResource",
            json!({"Arn": arn, "Tags": [{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "x"}]}),
        );
        // TagResource syncs into the resource, visible on describe.
        let (_, body) = run("DescribeServer", json!({"ServerId": server_id}));
        let tags = body["Server"]["Tags"].as_array().unwrap();
        assert_eq!(tags.len(), 2);

        let (_, body) = run("ListTagsForResource", json!({"Arn": arn}));
        let tags = body["Tags"].as_array().unwrap();
        assert_eq!(tags[0]["Key"], "env");
        assert_eq!(tags[0]["Value"], "test");

        run("UntagResource", json!({"Arn": arn, "TagKeys": ["team"]}));
        let (_, body) = run("ListTagsForResource", json!({"Arn": arn}));
        let tags = body["Tags"].as_array().unwrap();
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0]["Key"], "env");
    }

    #[test]
    fn security_policies_table() {
        let (status, body) = run(
            "DescribeSecurityPolicy",
            json!({"SecurityPolicyName": "TransferSecurityPolicy-2020-06"}),
        );
        assert_eq!(status, 200);
        let policy = &body["SecurityPolicy"];
        assert_eq!(
            policy["SecurityPolicyName"],
            "TransferSecurityPolicy-2020-06"
        );
        assert!(policy["SshCiphers"].is_array());

        let (status, body) = run("ListSecurityPolicies", json!({}));
        assert_eq!(status, 200);
        let names = body["SecurityPolicyNames"].as_array().unwrap();
        assert_eq!(names.len(), 8);
        // sorted: 2018-11 first
        assert_eq!(names[0], "TransferSecurityPolicy-2018-11");
        let (status, body) = run(
            "DescribeSecurityPolicy",
            json!({"SecurityPolicyName": names[0].as_str().unwrap()}),
        );
        assert_eq!(status, 200);
        assert_eq!(body["SecurityPolicy"]["Protocols"], json!(["SFTP", "FTPS"]));

        let (status, err) = run(
            "DescribeSecurityPolicy",
            json!({"SecurityPolicyName": "Nope"}),
        );
        assert_eq!(status, 404);
        assert_eq!(err["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn web_apps_and_customization() {
        let (status, body) = run(
            "CreateWebApp",
            json!({"IdentityProviderDetails": {"IdentityCenterConfig": {"InstanceArn": "arn"}}}),
        );
        assert_eq!(status, 200);
        let web_app_id = body["WebAppId"].as_str().unwrap().to_string();
        assert!(web_app_id.starts_with("webapp-"));

        let (_, body) = run("DescribeWebApp", json!({"WebAppId": web_app_id}));
        assert!(body["WebApp"]["WebAppEndpoint"]
            .as_str()
            .unwrap()
            .contains(&web_app_id));

        let (status, err) = run(
            "DescribeWebApp",
            json!({"WebAppId": "webapp-0000000000000000000000"}),
        );
        assert_eq!(status, 404);
        assert_eq!(err["__type"], "ResourceNotFoundException");

        let (status, _) = run(
            "UpdateWebAppCustomization",
            json!({"WebAppId": web_app_id, "Title": "My title"}),
        );
        assert_eq!(status, 200);
        let (_, body) = run(
            "DescribeWebAppCustomization",
            json!({"WebAppId": web_app_id}),
        );
        assert_eq!(body["WebAppCustomization"]["Title"], "My title");

        run("DeleteWebAppCustomization", json!({"WebAppId": web_app_id}));
        let (status, err) = run(
            "DescribeWebAppCustomization",
            json!({"WebAppId": web_app_id}),
        );
        assert_eq!(status, 404);
        assert_eq!(err["__type"], "ResourceNotFoundException");
        assert_eq!(
            err["message"],
            format!("WebAppCustomization not found: {web_app_id}")
        );

        run("DeleteWebApp", json!({"WebAppId": web_app_id}));
        let (status, _) = run("DescribeWebApp", json!({"WebAppId": web_app_id}));
        assert_eq!(status, 404);
    }

    #[test]
    fn host_keys_and_identity_provider() {
        let server_id = create_server();
        let (status, body) = run(
            "ImportHostKey",
            json!({"ServerId": server_id, "HostKeyBody": "-----BEGIN RSA PRIVATE KEY-----\nabc\n-----END RSA PRIVATE KEY-----"}),
        );
        assert_eq!(status, 200);
        assert_eq!(body["ServerId"], server_id.as_str());
        let host_key_id = body["HostKeyId"].as_str().unwrap().to_string();
        assert!(host_key_id.starts_with("hostkey-"));

        let (_, body) = run(
            "DescribeHostKey",
            json!({"ServerId": server_id, "HostKeyId": host_key_id}),
        );
        assert_eq!(body["HostKey"]["HostKeyId"], host_key_id.as_str());
        assert_eq!(body["HostKey"]["Type"], "RSA");

        let (_, body) = run("ListHostKeys", json!({"ServerId": server_id}));
        assert_eq!(body["HostKeys"].as_array().unwrap().len(), 1);

        run(
            "DeleteHostKey",
            json!({"ServerId": server_id, "HostKeyId": host_key_id}),
        );
        let (status, _) = run(
            "DescribeHostKey",
            json!({"ServerId": server_id, "HostKeyId": host_key_id}),
        );
        assert_eq!(status, 404);

        let (status, body) = run(
            "TestIdentityProvider",
            json!({"ServerId": server_id, "UserName": "u", "ServerProtocol": "SFTP"}),
        );
        assert_eq!(status, 200);
        assert_eq!(body["StatusCode"], 200);
    }

    #[test]
    fn update_user_and_access_partial_semantics() {
        let server_id = create_server();
        create_user(&server_id, "bob");
        let (status, body) = run(
            "UpdateUser",
            json!({
                "ServerId": server_id,
                "UserName": "bob",
                "HomeDirectory": "/new-home",
            }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["UserName"], "bob");
        let (_, body) = run(
            "DescribeUser",
            json!({"ServerId": server_id, "UserName": "bob"}),
        );
        assert_eq!(body["User"]["HomeDirectory"], "/new-home");

        // Users list via short dicts with key counts.
        let (_, body) = run("ListUsers", json!({"ServerId": server_id}));
        let entry = body["Users"].as_array().unwrap()[0].clone();
        assert_eq!(entry["SshPublicKeyCount"], 0);

        // Access on a missing server reports ServerNotFound first.
        let (status, err) = run(
            "UpdateAccess",
            json!({"ServerId": "s-00000000000000000", "ExternalId": "e", "Role": "r"}),
        );
        assert_eq!(status, 400);
        assert_eq!(err["__type"], "ServerNotFound");
    }

    #[test]
    fn camel_to_snake_matches_moto() {
        assert_eq!(camel_to_snake("LocalProfileId"), "local_profile_id");
        assert_eq!(
            camel_to_snake("MdnSigningAlgorithm"),
            "mdn_signing_algorithm"
        );
        assert_eq!(camel_to_snake("AS2Id"), "as2_id"); // upper-run split, like moto
        assert_eq!(camel_to_snake("Compression"), "compression");
    }

    #[test]
    fn transfer_describe_no_nulls() {
        // moto 5.2.3 #10068: AWS Transfer omits unset optional fields from
        // describe_server()/describe_connector() responses instead of
        // returning them as null.
        let (status, body) = run(
            "CreateServer",
            json!({"IdentityProviderType": "SERVICE_MANAGED"}),
        );
        assert_eq!(status, 200);
        let server_id = body["ServerId"].as_str().unwrap().to_string();
        let (_, body) = run("DescribeServer", json!({"ServerId": server_id}));
        let server = &body["Server"];
        for omitted in [
            "Certificate",
            "LoggingRole",
            "SecurityPolicyName",
            "HostKeyFingerprint",
            "PreAuthenticationLoginBanner",
            "PostAuthenticationLoginBanner",
            "EndpointDetails",
            "IdentityProviderDetails",
            "ProtocolDetails",
            "S3StorageOptions",
            "WorkflowDetails",
            "Tags",
        ] {
            assert!(
                server.get(omitted).is_none(),
                "describe_server must omit unset {omitted}: {server}"
            );
        }
        // Fields that are always populated remain present.
        assert_eq!(server["ServerId"], server_id.as_str());
        assert!(server["Arn"].as_str().is_some());
        assert_eq!(server["State"], "ONLINE");

        let (status, body) = run(
            "CreateConnector",
            json!({
                "Url": "sftp://example.com",
                "AccessRole": "arn:aws:iam::123456789012:role/TransferAccessRole",
            }),
        );
        assert_eq!(status, 200);
        let connector_id = body["ConnectorId"].as_str().unwrap().to_string();
        let (_, body) = run("DescribeConnector", json!({"ConnectorId": connector_id}));
        let connector = &body["Connector"];
        for omitted in [
            "LoggingRole",
            "SecurityPolicyName",
            "As2Config",
            "SftpConfig",
        ] {
            assert!(
                connector.get(omitted).is_none(),
                "describe_connector must omit unset {omitted}: {connector}"
            );
        }
        assert_eq!(connector["ConnectorId"], connector_id.as_str());
        assert_eq!(connector["Url"], "sftp://example.com");
        assert_eq!(
            connector["AccessRole"],
            "arn:aws:iam::123456789012:role/TransferAccessRole"
        );
        assert!(connector["ServiceManagedEgressIpAddresses"]
            .as_array()
            .is_some());
    }
}
