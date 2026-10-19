//! WorkSpaces Web backend (REST-JSON; path-per-operation).
//!
//! Port of moto's workspacesweb: portals and their four settings families
//! (browser, network, user, user-access-logging) with portal-association
//! wiring and a tag store, keyed per (account, region). The remainder of
//! the botocore surface — data-protection / ip-access / trust-store /
//! session-logger / identity-provider settings, sessions, disassociate and
//! update families — has no handler in the moto fork either and answers
//! moto's generic `501 NotImplemented` ("The {action} action has not been
//! implemented").

use crate::proto::{parse_json_body, query_first, query_map, url_decode};
use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Value};

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("workspacesweb"));
    SLOT.clone()
}

/// The portal or one of the four settings families the routes cover.
#[derive(Clone, Copy, PartialEq)]
enum Family {
    Portal,
    Browser,
    Network,
    User,
    Logging,
}

#[derive(Default)]
struct State {
    browser_settings: Vec<Value>,
    network_settings: Vec<Value>,
    user_settings: Vec<Value>,
    user_access_logging_settings: Vec<Value>,
    portals: Vec<Value>,
    /// resource arn -> {Key: Value} tags (moto's TaggingService).
    tags_store: Vec<(String, Value)>,
}

impl State {
    fn recs(&self, family: Family) -> &Vec<Value> {
        match family {
            Family::Portal => &self.portals,
            Family::Browser => &self.browser_settings,
            Family::Network => &self.network_settings,
            Family::User => &self.user_settings,
            Family::Logging => &self.user_access_logging_settings,
        }
    }

    fn recs_mut(&mut self, family: Family) -> &mut Vec<Value> {
        match family {
            Family::Portal => &mut self.portals,
            Family::Browser => &mut self.browser_settings,
            Family::Network => &mut self.network_settings,
            Family::User => &mut self.user_settings,
            Family::Logging => &mut self.user_access_logging_settings,
        }
    }

    fn find(&self, family: Family, arn: &str) -> Option<&Value> {
        self.recs(family)
            .iter()
            .find(|v| v[family.arn_key()].as_str() == Some(arn))
    }

    fn find_mut(&mut self, family: Family, arn: &str) -> Option<&mut Value> {
        self.recs_mut(family)
            .iter_mut()
            .find(|v| v[family.arn_key()].as_str() == Some(arn))
    }

    fn remove(&mut self, family: Family, arn: &str) -> bool {
        let before = self.recs(family).len();
        self.recs_mut(family)
            .retain(|v| v[family.arn_key()].as_str() != Some(arn));
        self.recs(family).len() != before
    }
}

impl Family {
    /// The family's exact root URI (botocore's create/list requestUri).
    fn root(self) -> &'static str {
        match self {
            Family::Portal => "/portals",
            Family::Browser => "/browserSettings",
            Family::Network => "/networkSettings",
            Family::User => "/userSettings",
            Family::Logging => "/userAccessLoggingSettings",
        }
    }

    /// The ARN resource part (`browser-settings/…`, `portal/…`).
    fn kind(self) -> &'static str {
        match self {
            Family::Portal => "portal",
            Family::Browser => "browser-settings",
            Family::Network => "network-settings",
            Family::User => "user-settings",
            Family::Logging => "user-access-logging-settings",
        }
    }

    /// ARN member name — the Create output key, the key stored in every
    /// record, and the portal's matching association field.
    fn arn_key(self) -> &'static str {
        match self {
            Family::Portal => "portalArn",
            Family::Browser => "browserSettingsArn",
            Family::Network => "networkSettingsArn",
            Family::User => "userSettingsArn",
            Family::Logging => "userAccessLoggingSettingsArn",
        }
    }

    /// List output container name.
    fn plural(self) -> &'static str {
        match self {
            Family::Portal => "portals",
            Family::Browser => "browserSettings",
            Family::Network => "networkSettings",
            Family::User => "userSettings",
            Family::Logging => "userAccessLoggingSettings",
        }
    }

    /// Get output wrapper name (singular for the portal only).
    fn noun(self) -> &'static str {
        match self {
            Family::Portal => "portal",
            fam => fam.plural(),
        }
    }

    /// The botocore action name for `{verb}{Family}` ops; portals only
    /// pluralize for List.
    fn op(self, verb: &str) -> String {
        match self {
            Family::Portal if verb == "List" => "ListPortals".to_string(),
            Family::Portal => format!("{verb}Portal"),
            fam => format!("{verb}{}", fam.plural()),
        }
    }

    /// Association action names per botocore ("AssociateUserSettings"…).
    fn associate_op(self) -> &'static str {
        match self {
            Family::Browser => "AssociateBrowserSettings",
            Family::Network => "AssociateNetworkSettings",
            Family::User => "AssociateUserSettings",
            Family::Logging => "AssociateUserAccessLoggingSettings",
            Family::Portal => "AssociatePortal",
        }
    }

    fn disassociate_op(self) -> &'static str {
        match self {
            Family::Browser => "DisassociateBrowserSettings",
            Family::Network => "DisassociateNetworkSettings",
            Family::User => "DisassociateUserSettings",
            Family::Logging => "DisassociateUserAccessLoggingSettings",
            Family::Portal => "DisassociatePortal",
        }
    }
}

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "workspacesweb" {
        return None;
    }
    let body = parse_json_body(&req.body);
    let slot = state();
    let mut st = slot.write();
    let b = st.entry((req.account, req.region.clone())).or_default();
    Some(handle(req, b, &body))
}

fn handle(req: &Request, st: &mut State, p: &Value) -> Response {
    let path = req.path.as_str();
    let method = req.method.as_str();
    let region = req.region.as_str();
    let account = req.account;

    // Tag operations ride on /tags/{resourceArn} (GET list / POST tag /
    // DELETE untag, per botocore); the ARN rides percent-encoded in the
    // path like moto's greedy regex capture.
    if let Some(rest) = path.strip_prefix("/tags/") {
        let Some(arn) = tag_arn(rest, p) else {
            return not_found(rest);
        };
        return match method {
            "GET" => ok(&json!({"tags": list_tags_for_resource(st, &arn)})),
            "POST" => {
                if non_empty_tags(p) {
                    tag_resource(st, &arn, p["tags"].as_array().unwrap());
                }
                ok(&json!({}))
            }
            "DELETE" => {
                untag_resource(st, &arn, &untag_keys(p, &req.query_string));
                ok(&json!({}))
            }
            _ => not_implemented("TagResource"),
        };
    }

    // Portal root: create / list.
    if path == "/portals" {
        return match method {
            "GET" => list_portals(st),
            "POST" => create_portal(st, p, region, account),
            _ => not_implemented("CreatePortal"),
        };
    }

    // Nested under /portals/{portalArn}: the four association suffixes,
    // then sessions (out of moto's surface), then the portal itself.
    // Portal arns are one slash-free segment (`arn:…:portal/{uuid}`), so
    // the suffix matching stays exact.
    if let Some(rest) = path.strip_prefix("/portals/") {
        for family in [
            Family::Browser,
            Family::Network,
            Family::User,
            Family::Logging,
        ] {
            let Some(portal_arn) = rest.strip_suffix(family.root()) else {
                continue;
            };
            return match method {
                // boto3 wires AssociateX as PUT on this URI; the setting
                // arn is a `querystring`-location member (fallback: body,
                // moto's _get_param ordering) since botocore 5.x.
                "PUT" => {
                    let q = query_map(&req.query_string);
                    let setting_arn = query_first(&q, family.arn_key())
                        .map(str::to_string)
                        .or_else(|| p[family.arn_key()].as_str().map(str::to_string));
                    let Some(setting_arn) = setting_arn else {
                        return validation(&format!(
                            "1 validation error detected: Value null at '{}' failed to satisfy constraint: Member must not be null",
                            family.arn_key()
                        ));
                    };
                    if setting_arn.is_empty() {
                        return validation(&format!(
                            "1 validation error detected: Value '{setting_arn}' failed to satisfy constraint on '{}': Variable must not be empty",
                            family.arn_key()
                        ));
                    }
                    associate(st, family, url_decode(portal_arn), setting_arn)
                }
                // DELETE here is Disassociate* — no handler in the moto fork.
                "DELETE" => not_implemented(family.disassociate_op()),
                _ => not_implemented(family.associate_op()),
            };
        }
        if rest == "portalServiceProviderMetadata" {
            // GET /portals/{portalId+}/portalServiceProviderMetadata
            return not_implemented("GetPortalServiceProviderMetadata");
        }
        if rest.ends_with("/expire") {
            // POST /portals/{id}/sessions/{sessionId}/expire
            return not_implemented("ExpireSession");
        }
        if rest.contains("/sessions") {
            // GET /portals/{id}/sessions[/…] has no entry in moto's urls.py.
            return not_implemented("GetSession");
        }
        let arn = url_decode(rest);
        return match method {
            "GET" => get(st, Family::Portal, &arn),
            "DELETE" => delete(st, Family::Portal, &arn),
            // PUT /portals/{arn} is UpdatePortal — outside moto's surface.
            _ => not_implemented("UpdatePortal"),
        };
    }

    // The four settings families: exact root = create/list, `{root}{id}` =
    // get/delete.
    for family in [
        Family::Browser,
        Family::Network,
        Family::User,
        Family::Logging,
    ] {
        let Some(tail) = path.strip_prefix(family.root()) else {
            continue;
        };
        if tail.is_empty() {
            return match method {
                "GET" => list_family(st, family),
                "POST" => create_family(st, family, p, region, account),
                _ => not_implemented(&family.op("Create")),
            };
        }
        if let Some(raw_arn) = tail.strip_prefix('/') {
            let arn = url_decode(raw_arn);
            return match method {
                "GET" => get(st, family, &arn),
                "DELETE" => delete(st, family, &arn),
                // Update*Settings rides the same URI shape but sits
                // outside moto's surface.
                _ => not_implemented(&family.op("Update")),
            };
        }
        break;
    }

    // Every other prefix (trustStores, dataProtectionSettings,
    // ipAccessSettings, sessionLoggers, identityProviders, …) is outside
    // the moto fork's routed surface. Best-effort noun to name the action
    // moto would have answered.
    let first = path.split('/').find(|s| !s.is_empty()).unwrap_or("");
    let noun = match first {
        "trustStores" => "TrustStore",
        "dataProtectionSettings" => "DataProtectionSettings",
        "ipAccessSettings" => "IpAccessSettings",
        "sessionLoggers" => "SessionLogger",
        "identityProviders" => "IdentityProvider",
        _ => "WorkSpacesWeb",
    };
    let verb = match method {
        "GET" => "Get",
        "POST" => "Create",
        "PUT" => "Update",
        "DELETE" => "Delete",
        _ => "Get",
    };
    not_implemented(&format!("{verb}{noun}"))
}

// ---- portals ----

fn create_portal(st: &mut State, p: &Value, region: &str, account: u64) -> Response {
    let id = uuid::Uuid::new_v4().to_string();
    let arn = arn_of(region, account, Family::Portal, &id);
    let endpoint = format!("{id}.portal.aws");
    let portal = json!({
        "additionalEncryptionContext": p["additionalEncryptionContext"],
        "authenticationType": p["authenticationType"],
        "browserSettingsArn": Value::Null,
        "browserType": "Chrome",
        "creationDate": crate::proto::now_iso(),
        "customerManagedKey": p["customerManagedKey"],
        "displayName": p["displayName"],
        "instanceType": p["instanceType"],
        "ipAccessSettingsArn": Value::Null,
        "maxConcurrentSessions": p["maxConcurrentSessions"],
        "networkSettingsArn": Value::Null,
        "portalArn": &arn,
        "portalEndpoint": &endpoint,
        "portalStatus": "CREATED",
        "rendererType": "AppStream",
        "statusReason": "TestStatusReason",
        "trustStoreArn": Value::Null,
        "userAccessLoggingSettingsArn": Value::Null,
        "userSettingsArn": Value::Null,
    });
    if non_empty_tags(p) {
        tag_resource(st, &arn, p["tags"].as_array().unwrap());
    }
    st.recs_mut(Family::Portal).push(portal);
    ok(&json!({"portalArn": &arn, "portalEndpoint": &endpoint}))
}

/// moto's list_portals echoes the full portal summary minus
/// `additionalEncryptionContext`.
fn list_portals(st: &State) -> Response {
    let items: Vec<Value> = st
        .recs(Family::Portal)
        .iter()
        .map(|v| {
            let mut out = v.clone();
            if let Some(obj) = out.as_object_mut() {
                obj.remove("additionalEncryptionContext");
            }
            out
        })
        .collect();
    ok(&json!({"portals": items}))
}

// ---- settings families ----

fn create_family(
    st: &mut State,
    family: Family,
    p: &Value,
    region: &str,
    account: u64,
) -> Response {
    let id = uuid::Uuid::new_v4().to_string();
    let arn = arn_of(region, account, family, &id);
    let mut rec = match family {
        Family::Browser => json!({
            "additionalEncryptionContext": p["additionalEncryptionContext"],
            "browserPolicy": p["browserPolicy"],
            "customerManagedKey": p["customerManagedKey"],
        }),
        Family::Network => json!({
            "securityGroupIds": p["securityGroupIds"],
            "subnetIds": p["subnetIds"],
            "vpcId": p["vpcId"],
        }),
        Family::User => json!({
            "additionalEncryptionContext": p["additionalEncryptionContext"],
            "clientToken": p["clientToken"],
            "cookieSynchronizationConfiguration": p["cookieSynchronizationConfiguration"],
            "copyAllowed": p["copyAllowed"],
            "customerManagedKey": p["customerManagedKey"],
            "deepLinkAllowed": p["deepLinkAllowed"],
            "disconnectTimeoutInMinutes": p["disconnectTimeoutInMinutes"],
            "downloadAllowed": p["downloadAllowed"],
            "idleDisconnectTimeoutInMinutes": p["idleDisconnectTimeoutInMinutes"],
            "pasteAllowed": p["pasteAllowed"],
            "printAllowed": p["printAllowed"],
            "uploadAllowed": p["uploadAllowed"],
        }),
        Family::Logging => json!({
            "kinesisStreamArn": p["kinesisStreamArn"],
        }),
        Family::Portal => unreachable!("portals never reach create_family"),
    };
    let obj = rec.as_object_mut().unwrap();
    obj.insert("associatedPortalArns".into(), json!([]));
    obj.insert(family.arn_key().into(), json!(&arn));
    // moto quirk (`x_allowed if x_allowed else "Disabled"`): absent and
    // otherwise-falsy flags read back as the literal string "Disabled".
    if family == Family::User {
        for key in [
            "copyAllowed",
            "deepLinkAllowed",
            "downloadAllowed",
            "pasteAllowed",
            "printAllowed",
            "uploadAllowed",
        ] {
            let incoming = obj.get(key).cloned().unwrap_or(Value::Null);
            obj.insert(key.into(), permissive_or_disabled(&incoming));
        }
    }
    if non_empty_tags(p) {
        tag_resource(st, &arn, p["tags"].as_array().unwrap());
    }
    st.recs_mut(family).push(rec);
    ok(&json!({ family.arn_key(): &arn }))
}

/// moto: `x_allowed if x_allowed else "Disabled"` — absent/empty values
/// read back as the literal string "Disabled".
fn permissive_or_disabled(v: &Value) -> Value {
    if v.as_str().map(|s| !s.is_empty()).unwrap_or(false) || v.as_bool() == Some(true) {
        return v.clone();
    }
    json!("Disabled")
}

/// moto's list shapes: the family ARN each, plus vpcId for network
/// settings.
fn list_family(st: &State, family: Family) -> Response {
    let items: Vec<Value> = st
        .recs(family)
        .iter()
        .map(|v| match family {
            Family::Network => json!({
                "networkSettingsArn": v["networkSettingsArn"],
                "vpcId": v["vpcId"],
            }),
            f => json!({ f.arn_key(): v[f.arn_key()] }),
        })
        .collect();
    ok(&json!({ family.plural(): items }))
}

fn get(st: &State, family: Family, arn: &str) -> Response {
    match st.find(family, arn) {
        Some(v) => ok(&json!({ family.noun(): v })),
        None => not_found(arn),
    }
}

fn delete(st: &mut State, family: Family, arn: &str) -> Response {
    if !st.remove(family, arn) {
        return not_found(arn);
    }
    ok(&json!({}))
}

/// One Associate* op: both arns must exist, then the portal's per-family
/// field flips to the setting arn and the setting's
/// `associatedPortalArns` grows a duplicate-tolerant entry (moto appends
/// on every call).
fn associate(st: &mut State, family: Family, portal_arn: String, setting_arn: String) -> Response {
    if st.find(family, &setting_arn).is_none() {
        return not_found(&setting_arn);
    }
    if st.find(Family::Portal, &portal_arn).is_none() {
        return not_found(&portal_arn);
    }
    if let Some(v) = st.find_mut(family, &setting_arn) {
        if let Some(list) = v
            .get_mut("associatedPortalArns")
            .and_then(Value::as_array_mut)
        {
            list.push(json!(&portal_arn));
        }
    }
    if let Some(portal) = st.find_mut(Family::Portal, &portal_arn) {
        portal[family.arn_key()] = json!(&setting_arn);
    }
    ok(&json!({ family.arn_key(): &setting_arn, "portalArn": &portal_arn }))
}

// ---- tags ----

/// moto's TaggingService.tag_resource: merge {Key, Value} pairs into the
/// per-arn map (a missing Value stores null); an empty list is a no-op.
fn tag_resource(st: &mut State, arn: &str, tags: &[Value]) {
    if tags.is_empty() {
        return;
    }
    let entry = match st.tags_store.iter_mut().find(|(a, _)| a == arn) {
        Some((_, v)) => v,
        None => {
            st.tags_store.push((arn.to_string(), json!({})));
            let i = st.tags_store.len() - 1;
            &mut st.tags_store[i].1
        }
    };
    let Some(obj) = entry.as_object_mut() else {
        return;
    };
    for tag in tags {
        let Some(key) = tag.get("Key").and_then(Value::as_str) else {
            continue;
        };
        obj.insert(
            key.to_string(),
            tag.get("Value").cloned().unwrap_or(Value::Null),
        );
    }
}

/// Untag keys arrive in the DELETE body like real rest-json; older clients
/// may repeat `?tagKeys=a&tagKeys=b` on the query line.
fn untag_keys(p: &Value, query_string: &str) -> Vec<Value> {
    if let Some(keys) = p["tagKeys"].as_array() {
        return keys.clone();
    }
    query_map(query_string)
        .get("tagKeys")
        .cloned()
        .map(|vals| vals.into_iter().map(Value::String).collect())
        .unwrap_or_default()
}

fn untag_resource(st: &mut State, arn: &str, keys: &[Value]) {
    let Some((_, tags)) = st.tags_store.iter_mut().find(|(a, _)| a == arn) else {
        return;
    };
    let Some(obj) = tags.as_object_mut() else {
        return;
    };
    for key in keys {
        if let Some(k) = key.as_str() {
            obj.remove(k);
        }
    }
}

fn list_tags_for_resource(st: &State, arn: &str) -> Vec<Value> {
    match st.tags_store.iter().find(|(a, _)| a == arn) {
        Some((_, tags)) => tags
            .as_object()
            .map(|obj| {
                obj.iter()
                    .map(|(k, v)| json!({"Key": k, "Value": v}))
                    .collect()
            })
            .unwrap_or_default(),
        None => vec![],
    }
}

/// resourceArn: body parameter when present (moto's JSON-body fallback
/// wins over the path), else the percent-decoded path remainder.
fn tag_arn(raw: &str, p: &Value) -> Option<String> {
    if let Some(arn) = p["resourceArn"].as_str() {
        if !arn.is_empty() {
            return Some(url_decode(arn));
        }
    }
    if raw.is_empty() {
        return None;
    }
    Some(url_decode(raw))
}

fn non_empty_tags(p: &Value) -> bool {
    p["tags"].as_array().map(|a| !a.is_empty()).unwrap_or(false)
}

// ---- errors & helpers ----

fn ok(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

fn not_found(arn: &str) -> Response {
    error(
        404,
        "ResourceNotFoundException",
        &format!("Resource Arn: {arn} does not exist."),
    )
}

fn not_implemented(action: &str) -> Response {
    error(
        501,
        "NotImplemented",
        &format!("The {action} action has not been implemented"),
    )
}

fn validation(message: &str) -> Response {
    error(400, "ValidationException", message)
}

fn error(status: u16, code: &str, message: &str) -> Response {
    let body = json!({"__type": code, "message": message});
    Response::json(status, serde_json::to_vec(&body).unwrap_or_default())
}

fn arn_of(region: &str, account: u64, family: Family, id: &str) -> String {
    format!(
        "arn:{}:workspaces-web:{}:{}:{}/{}",
        partition(region),
        region,
        account,
        family.kind(),
        id
    )
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
