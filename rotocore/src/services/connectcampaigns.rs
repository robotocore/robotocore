//! AWS Connect Campaigns backend — port of the moto fork's `connectcampaigns`
//! handler set (rest-json; models.py + responses.py).
//!
//! Campaigns carry a dialer lifecycle state machine (Initialized →
//! Running/Stopped/Paused/Stopped), instance configs are created on demand by
//! GetConnectInstanceConfig / StartInstanceOnboardingJob (including moto's
//! "invalid-id" sentinel), tags live on the campaign record (moto keeps a
//! TaggingService mirror but every implemented op reads `campaign.tags`), and
//! ListCampaigns paginates with botocore's base64 token format over the
//! campaign id (default page size 100; the token carries the id of the first
//! record of the next page).
//!
//! Moto-fidelity notes:
//! - UntagResource reads only the FIRST `tagKeys` query value (moto's
//!   `_get_param` returns `querystring[name][0]`), so `?tagKeys=k1&tagKeys=k2`
//!   removes only k1 — mirrored.
//! - DeleteCampaign on a missing id is a KeyError crash in moto (the probe's
//!   lone `500_error`); here it answers the service's own
//!   ResourceNotFoundException instead.
//! - 8 of the 22 botocore operations have no handler in the fork's
//!   responses.py, so moto raises NotImplementedError and the gateway
//!   rendered those as 501 NotImplemented — mirrored verbatim via
//!   [`not_implemented_action`]. Paths no botocore operation routes to
//!   answer moto's 404 with an empty body.

use crate::store;
use crate::{Request, Response};
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use once_cell::sync::Lazy;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot::<State>("connectcampaigns"));

/// Moto PAGINATION_MODEL `limit_default` for list_campaigns.
const PAGE_DEFAULT: usize = 100;

#[derive(Default)]
struct State {
    /// Campaigns in creation order (moto's insertion-ordered dict).
    campaigns: Vec<Campaign>,
    /// connectInstanceId -> cached instance config (moto's instance_configs).
    instance_configs: Vec<(String, Value)>,
    /// connectInstanceId -> recorded onboarding job status.
    onboarding_jobs: Vec<(String, Value)>,
}

struct Campaign {
    id: String,
    arn: String,
    name: String,
    connect_instance_id: String,
    state: String,
    dialer_config: Value,
    outbound_call_config: Value,
    tags: Map<String, Value>,
}

impl Campaign {
    /// moto's ConnectCampaign.to_dict.
    fn to_dict(&self) -> Value {
        json!({
            "id": self.id,
            "arn": self.arn,
            "name": self.name,
            "connectInstanceId": self.connect_instance_id,
            "dialerConfig": self.dialer_config,
            "outboundCallConfig": self.outbound_call_config,
            "tags": self.tags,
        })
    }

    fn to_summary(&self) -> Value {
        json!({
            "id": self.id,
            "arn": self.arn,
            "name": self.name,
            "connectInstanceId": self.connect_instance_id,
        })
    }
}

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "connectcampaigns" {
        return None;
    }
    let slot = SLOT.clone();
    let mut st = slot.write();
    let state = st.entry((req.account, req.region.clone())).or_default();
    Some(route(req, state))
}

/// Route by botocore's (method, requestUri) table. The stub arms cover the
/// 8 fork-absent operations: botocore routes them, the fork never had a
/// handler, and the gateway rendered that as 501 NotImplemented.
fn route(req: &Request, s: &mut State) -> Response {
    let method = req.method.as_str();
    let path = req.path.as_str();

    if let Some(rest) = path.strip_prefix("/campaigns") {
        return campaign_route(req, s, method, rest);
    }

    if let Some(rest) = path.strip_prefix("/connect-instance/") {
        let mut parts = rest.split('/');
        let cid = parts.next().unwrap_or("");
        let sub = parts.next().unwrap_or("");
        if parts.next().is_some() || cid.is_empty() || sub.is_empty() {
            return moto_unknown_path();
        }
        return instance_route(req, s, method, sub, cid);
    }

    if let Some(arn_raw) = path.strip_prefix("/tags/") {
        if arn_raw.is_empty() {
            return moto_unknown_path();
        }
        let arn = crate::proto::url_decode(arn_raw);
        match method {
            "GET" => return list_tags_for_resource(s, &arn),
            "POST" => return tag_resource(req, s, &arn),
            "DELETE" => return untag_resource(req, s, &arn),
            _ => return moto_unknown_path(),
        }
    }

    moto_unknown_path()
}

fn campaign_route(req: &Request, s: &mut State, method: &str, rest: &str) -> Response {
    match (method, rest) {
        ("PUT", "") => return create_campaign(req, s),
        ("POST", "-summary") => return list_campaigns(req, s),
        ("POST", "-state") => return not_implemented_action("get_campaign_state_batch"),
        _ => {}
    }
    // Sub-resources; "/campaigns-<other>" is not a botocore route.
    if !rest.starts_with('/') {
        return moto_unknown_path();
    }
    let mut parts = rest.split('/').filter(|p| !p.is_empty());
    let id = parts.next().unwrap_or("");
    let sub = parts.next().unwrap_or("");
    if parts.next().is_some() || id.is_empty() {
        return moto_unknown_path();
    }
    if sub.is_empty() {
        return match method {
            "GET" => describe_campaign(s, id),
            "DELETE" => delete_campaign(s, id),
            _ => moto_unknown_path(),
        };
    }
    campaign_sub(s, method, sub, id)
}

fn campaign_sub(s: &mut State, method: &str, sub: &str, id: &str) -> Response {
    match (method, sub) {
        ("POST", "start") => set_campaign_state(s, id, "Running"),
        ("POST", "stop") => set_campaign_state(s, id, "Stopped"),
        ("POST", "pause") => set_campaign_state(s, id, "Paused"),
        ("POST", "resume") => set_campaign_state(s, id, "Running"),
        ("GET", "state") => match campaign_by_id(s, id) {
            Some(c) => ok(&json!({ "state": c.state })),
            None => not_found_campaign(id),
        },
        ("POST", "dialer-config") => not_implemented_action("update_campaign_dialer_config"),
        ("POST", "name") => not_implemented_action("update_campaign_name"),
        ("POST", "outbound-call-config") => {
            not_implemented_action("update_campaign_outbound_call_config")
        }
        ("PUT", "dial-requests") => not_implemented_action("put_dial_request_batch"),
        _ => moto_unknown_path(),
    }
}

/// /connect-instance/{id}/{config,onboarding}. Implemented: GET config,
/// PUT onboarding. The other (method, sub) pairs are botocore-routed stubs.
fn instance_route(req: &Request, s: &mut State, method: &str, sub: &str, cid: &str) -> Response {
    match (method, sub) {
        ("GET", "config") => get_connect_instance_config(s, cid, req.account, &req.region),
        ("PUT", "onboarding") => start_instance_onboarding_job(req, s, cid),
        ("DELETE", "config") => not_implemented_action("delete_connect_instance_config"),
        ("DELETE", "onboarding") => not_implemented_action("delete_instance_onboarding_job"),
        ("GET", "onboarding") => not_implemented_action("get_instance_onboarding_job_status"),
        _ => moto_unknown_path(),
    }
}

fn create_campaign(req: &Request, s: &mut State) -> Response {
    let body = crate::proto::parse_json_body(&req.body);
    let id = uuid::Uuid::new_v4().to_string();
    let arn = format!(
        "arn:aws:connectcampaigns:{}:{}:campaign/{}",
        req.region, req.account, id
    );
    let tags = body.get("tags").and_then(Value::as_object).cloned();
    let tags = tags.unwrap_or_default();
    s.campaigns.push(Campaign {
        id: id.clone(),
        arn: arn.clone(),
        name: body
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        connect_instance_id: body
            .get("connectInstanceId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        state: "Initialized".to_string(),
        dialer_config: body.get("dialerConfig").cloned().unwrap_or(Value::Null),
        outbound_call_config: body
            .get("outboundCallConfig")
            .cloned()
            .unwrap_or(Value::Null),
        tags: tags.clone(),
    });
    ok(&json!({ "id": id, "arn": arn, "tags": tags }))
}

fn describe_campaign(s: &State, id: &str) -> Response {
    match campaign_by_id(s, id) {
        Some(c) => ok(&json!({ "campaign": c.to_dict() })),
        None => not_found_campaign(id),
    }
}

fn delete_campaign(s: &mut State, id: &str) -> Response {
    match s.campaigns.iter().position(|c| c.id == id) {
        Some(at) => {
            s.campaigns.remove(at);
            ok(&json!({}))
        }
        // moto raises KeyError (a 500 crash); the suite only exercises
        // existing deletes, so answer the service's own not-found instead.
        None => not_found_campaign(id),
    }
}

fn set_campaign_state(s: &mut State, id: &str, state: &str) -> Response {
    match s.campaigns.iter_mut().find(|c| c.id == id) {
        Some(c) => {
            c.state = state.to_string();
            ok(&json!({}))
        }
        None => not_found_campaign(id),
    }
}

fn list_campaigns(req: &Request, s: &State) -> Response {
    let body = crate::proto::parse_json_body(&req.body);
    let mut filtered: Vec<&Campaign> = s.campaigns.iter().collect();
    if let Some(filters) = body.get("filters").and_then(Value::as_object) {
        if let Some(instance_filter) = filters.get("instanceIdFilter") {
            let value = instance_filter
                .get("value")
                .and_then(Value::as_str)
                .unwrap_or("");
            let operator = instance_filter
                .get("operator")
                .and_then(Value::as_str)
                .unwrap_or("");
            match operator {
                "Eq" => filtered.retain(|c| c.connect_instance_id == value),
                "Ne" => filtered.retain(|c| c.connect_instance_id != value),
                "Contains" => filtered.retain(|c| c.connect_instance_id.contains(value)),
                "StartsWith" => filtered.retain(|c| c.connect_instance_id.starts_with(value)),
                other => return invalid(&format!("Unsupported operator: {other}")),
            }
        }
    }

    let summaries: Vec<Value> = filtered.iter().map(|c| c.to_summary()).collect();
    let max_results = body.get("maxResults").and_then(Value::as_u64).unwrap_or(0) as usize;
    let limit = if max_results == 0 {
        PAGE_DEFAULT
    } else {
        max_results
    };
    let token = body.get("nextToken").and_then(Value::as_str);
    let checksum = filters_checksum(body.get("filters"));

    let index_start = match token {
        None => 0,
        Some(token) => match decode_start_index(&summaries, token, checksum) {
            Ok(i) => i,
            Err(msg) => return invalid_token(&msg),
        },
    };
    let index_end = (index_start + limit).min(summaries.len());
    let page = &summaries[index_start..index_end];
    // moto's next token carries the id of `results[index_end]` — the first
    // record of the next page — as a botocore pagination token.
    let next_token = if !page.is_empty() && index_end < summaries.len() {
        Value::String(encode_token(
            summaries[index_end]["id"].as_str().unwrap_or(""),
            checksum,
        ))
    } else {
        Value::Null
    };
    ok(&json!({ "campaignSummaryList": page, "nextToken": next_token }))
}

/// botocore TokenDecoder format: base64 of a JSON dict with the unique
/// attribute of the resume point plus a checksum of the non-pagination input.
fn encode_token(campaign_id: &str, checksum: i64) -> String {
    let payload = json!({
        "parameterChecksum": checksum,
        "uniqueAttributes": campaign_id,
    });
    B64.encode(payload.to_string().as_bytes())
}

/// Returns the index of the record identified by the token — the page resumes
/// exactly there (moto Paginator.paginate).
fn decode_start_index(summaries: &[Value], token: &str, checksum: i64) -> Result<usize, String> {
    let invalid = |msg: String| Err(msg);
    let Ok(bytes) = B64.decode(token.as_bytes()) else {
        return invalid("Invalid token".to_string());
    };
    let Ok(text) = std::str::from_utf8(&bytes) else {
        return invalid("Invalid token".to_string());
    };
    let Ok(decoded) = serde_json::from_str::<Value>(text) else {
        return invalid("Invalid token".to_string());
    };
    if decoded.get("parameterChecksum").and_then(Value::as_i64) != Some(checksum) {
        return invalid(format!("Input inconsistent with page token: {token}"));
    }
    let Some(attr) = decoded.get("uniqueAttributes").and_then(Value::as_str) else {
        return invalid("Invalid token".to_string());
    };
    let index = summaries
        .iter()
        .position(|v| v["id"].as_str() == Some(attr));
    match index {
        Some(i) => Ok(i),
        // moto: "Resource not found!" when the token's record is gone.
        None => invalid("Resource not found!".to_string()),
    }
}

/// A stable int standing in for moto's `hash(freeze(kwargs))` over the
/// non-pagination inputs (only `filters` exists here).
fn filters_checksum(filters: Option<&Value>) -> i64 {
    let bytes = match filters {
        None => b"null".to_vec(),
        Some(v) => serde_json::to_vec(v).unwrap_or_else(|_| b"null".to_vec()),
    };
    let digest = Sha256::digest(&bytes);
    let mut n = u64::from_be_bytes(digest[0..8].try_into().expect("8 bytes"));
    n |= 1; // keep it truthy, like moto's checksum when kwargs are non-empty
    n as i64
}

fn get_connect_instance_config(s: &mut State, cid: &str, account: u64, region: &str) -> Response {
    if cid.is_empty() {
        return invalid("connectInstanceId is a required parameter");
    }
    if cid == "invalid-id" {
        return not_found_instance(cid);
    }
    if let Some((_, cfg)) = s.instance_configs.iter().find(|(i, _)| i == cid) {
        return ok(&json!({ "connectInstanceConfig": cfg }));
    }
    let cfg = new_instance_config(cid, region, account, false);
    s.instance_configs.push((cid.to_string(), cfg.clone()));
    ok(&json!({ "connectInstanceConfig": cfg }))
}

/// moto's ConnectInstanceConfig — on-demand, encryption "KMS", and the
/// canned service-linked role; a custom `keyArn` from onboarding wins.
fn new_instance_config(cid: &str, region: &str, account: u64, enabled: bool) -> Value {
    let mut encryption_config = json!({
        "enabled": enabled,
        "encryptionType": "KMS",
    });
    if enabled {
        encryption_config["keyArn"] = json!(format!(
            "arn:aws:kms:{region}:{account}:key/1234abcd-12ab-34cd-56ef-1234567890ab"
        ));
    }
    json!({
        "connectInstanceId": cid,
        "serviceLinkedRoleArn": format!(
            "arn:aws:iam::{account}:role/aws-service-role/connectcampaigns.amazonaws.com/AWSServiceRoleForConnectCampaigns"
        ),
        "encryptionConfig": encryption_config,
    })
}

fn start_instance_onboarding_job(req: &Request, s: &mut State, cid: &str) -> Response {
    if cid.is_empty() {
        return invalid("connectInstanceId is a required parameter");
    }
    if cid == "invalid-id" {
        return not_found_instance(cid);
    }
    let body = crate::proto::parse_json_body(&req.body);
    let encryption_config = body
        .get("encryptionConfig")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let Some(encryption) = encryption_config.as_object() else {
        return invalid("encryptionConfig is a required parameter");
    };
    if encryption.is_empty() {
        return invalid("encryptionConfig is a required parameter");
    }
    if !encryption.contains_key("enabled") {
        return invalid("enabled is a required parameter in encryptionConfig");
    }
    let enabled = truthy(&encryption["enabled"]);
    if enabled && !encryption.contains_key("keyArn") {
        return invalid("keyArn is required when encryption is enabled");
    }

    let job = json!({
        "connectInstanceId": cid,
        "status": "SUCCEEDED",
    });
    match s.onboarding_jobs.iter_mut().find(|(i, _)| i == cid) {
        Some((_, v)) => *v = job.clone(),
        None => s.onboarding_jobs.push((cid.to_string(), job.clone())),
    }

    let mut config = new_instance_config(cid, &req.region, req.account, enabled);
    if enabled {
        if let Some(key_arn) = encryption.get("keyArn") {
            config["encryptionConfig"]["keyArn"] = key_arn.clone();
        }
    }
    match s.instance_configs.iter_mut().find(|(i, _)| i == cid) {
        Some((_, stored)) => *stored = config.clone(),
        None => s.instance_configs.push((cid.to_string(), config)),
    }

    ok(&json!({ "connectInstanceOnboardingJobStatus": job }))
}

/// Python object truthiness for the few member types boto sends here.
fn truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().is_some_and(|f| f != 0.0),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

fn campaign_by_arn<'a>(s: &'a mut State, arn: &str) -> Option<&'a mut Campaign> {
    s.campaigns.iter_mut().find(|c| c.arn == arn)
}

fn campaign_by_id<'a>(s: &'a State, id: &str) -> Option<&'a Campaign> {
    s.campaigns.iter().find(|c| c.id == id)
}

fn list_tags_for_resource(s: &State, arn: &str) -> Response {
    match s.campaigns.iter().find(|c| c.arn == arn) {
        Some(c) => ok(&json!({ "tags": c.tags })),
        None => not_found_resource(arn),
    }
}

fn tag_resource(req: &Request, s: &mut State, arn: &str) -> Response {
    let Some(campaign) = campaign_by_arn(s, arn) else {
        return not_found_resource(arn);
    };
    let body = crate::proto::parse_json_body(&req.body);
    if let Some(tags) = body.get("tags").and_then(Value::as_object) {
        for (key, value) in tags {
            campaign.tags.insert(key.clone(), value.clone());
        }
    }
    ok(&json!({}))
}

/// moto's `_get_param` takes only the FIRST `tagKeys` query value, so the
/// multi-key form removes a single key — kept identical here.
fn untag_resource(req: &Request, s: &mut State, arn: &str) -> Response {
    let Some(campaign) = campaign_by_arn(s, arn) else {
        return not_found_resource(arn);
    };
    let query = crate::proto::query_map(&req.query_string);
    if let Some(key) = query.get("tagKeys").and_then(|values| values.first()) {
        campaign.tags.remove(key);
    }
    ok(&json!({}))
}

// ---------------------------------------------------------------------------
// Response helpers.
// ---------------------------------------------------------------------------

fn ok(v: &Value) -> Response {
    Response::new(
        200,
        "application/json",
        serde_json::to_vec(v).unwrap_or_default(),
    )
}

fn not_found_campaign(id: &str) -> Response {
    crate::proto::json_error(
        404,
        "ResourceNotFoundException",
        &format!("Campaign with id {id} not found"),
    )
}

fn not_found_instance(cid: &str) -> Response {
    crate::proto::json_error(
        404,
        "ResourceNotFoundException",
        &format!("Connect instance with id {cid} not found"),
    )
}

fn not_found_resource(arn: &str) -> Response {
    crate::proto::json_error(
        404,
        "ResourceNotFoundException",
        &format!("Resource {arn} not found"),
    )
}

fn invalid(message: &str) -> Response {
    crate::proto::json_error(400, "ValidationException", message)
}

fn invalid_token(message: &str) -> Response {
    crate::proto::json_error(400, "InvalidToken", message)
}

/// Moto's call_action raises NotImplementedError for botocore-routed
/// operations without a handler; the gateway renders it 501 NotImplemented.
fn not_implemented_action(action: &str) -> Response {
    crate::proto::json_error(
        501,
        "NotImplemented",
        &format!("The {action} action has not been implemented"),
    )
}

/// moto's `if not action: return 404, headers, ""` for paths no botocore
/// operation routes to.
fn moto_unknown_path() -> Response {
    Response {
        status: 404,
        headers: vec![("Content-Type".to_string(), "application/json".to_string())],
        body: Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// Unit tests (cargo test -p rotocore connectcampaigns).
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn next_account() -> u64 {
        static NEXT: AtomicU64 = AtomicU64::new(77_000_000_000);
        NEXT.fetch_add(1, Ordering::SeqCst)
    }

    fn req(method: &str, path: &str, body: &str, account: u64) -> Request {
        Request {
            service: "connectcampaigns".into(),
            operation: String::new(),
            method: method.into(),
            path: path.into(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: bytes::Bytes::copy_from_slice(body.as_bytes()),
            account,
            region: "us-east-1".into(),
        }
    }

    fn query_req(method: &str, path: &str, query: &str, account: u64) -> Request {
        let mut r = req(method, path, "", account);
        r.query_string = query.to_string();
        r
    }

    fn parsed(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap_or(json!({}))
    }

    fn err_code(resp: &Response) -> String {
        parsed(resp)["__type"]
            .as_str()
            .unwrap_or_default()
            .to_string()
    }

    fn err_message(resp: &Response) -> String {
        parsed(resp)["message"]
            .as_str()
            .unwrap_or_default()
            .to_string()
    }

    fn campaign_path(method: &str, id: &str, suffix: &str, account: u64) -> Response {
        dispatch_extract(&req(
            method,
            &format!("/campaigns/{id}{suffix}"),
            "",
            account,
        ))
    }

    fn dispatch_extract(request: &Request) -> Response {
        dispatch(request).expect("connectcampaigns dispatch")
    }

    #[test]
    fn create_describe_lifecycle_and_delete() {
        let acct = next_account();
        let created = dispatch_extract(&req(
            "PUT",
            "/campaigns",
            r#"{"name":"c","connectInstanceId":"ci","dialerConfig":{"a":1},"outboundCallConfig":{"b":2}}"#,
            acct,
        ));
        let v = parsed(&created);
        assert_eq!(created.status, 200);
        assert!(v["id"].as_str().unwrap().len() > 10);
        assert!(v["arn"]
            .as_str()
            .unwrap()
            .starts_with("arn:aws:connectcampaigns:us-east-1:"));
        assert_eq!(v["tags"], json!({}));

        let id = v["id"].as_str().unwrap().to_string();
        let described = dispatch_extract(&req("GET", &format!("/campaigns/{id}"), "", acct));
        let campaign = parsed(&described)["campaign"].clone();
        assert_eq!(campaign["name"], json!("c"));
        assert_eq!(campaign["connectInstanceId"], json!("ci"));
        assert_eq!(campaign["dialerConfig"]["a"], json!(1));
        assert_eq!(campaign["outboundCallConfig"]["b"], json!(2));
        assert_eq!(campaign["id"], json!(&id));
        assert!(campaign["arn"].is_string());

        for (sub, state) in [
            ("/start", "Running"),
            ("/pause", "Paused"),
            ("/resume", "Running"),
            ("/stop", "Stopped"),
        ] {
            assert_eq!(campaign_path("POST", &id, sub, acct).status, 200);
            let state_ret =
                dispatch_extract(&req("GET", &format!("/campaigns/{id}/state"), "", acct));
            assert_eq!(parsed(&state_ret)["state"], json!(state), "after {sub}");
        }

        assert_eq!(campaign_path("DELETE", &id, "", acct).status, 200);
        let gone = dispatch_extract(&req("GET", &format!("/campaigns/{id}"), "", acct));
        assert_eq!(gone.status, 404);
        assert_eq!(err_code(&gone), "ResourceNotFoundException");
        assert_eq!(
            err_message(&gone),
            format!("Campaign with id {id} not found")
        );
    }

    #[test]
    fn campaign_not_found_errors() {
        let acct = next_account();
        for (path_suffix, method) in [("", "GET"), ("/state", "GET"), ("/start", "POST")] {
            let resp = campaign_path(method, "no-such-campaign", path_suffix, acct);
            assert_eq!(resp.status, 404, "{method} {path_suffix}");
            assert_eq!(err_code(&resp), "ResourceNotFoundException");
            assert_eq!(
                err_message(&resp),
                "Campaign with id no-such-campaign not found"
            );
        }
    }

    #[test]
    fn list_filter_operators() {
        let acct = next_account();
        for (ci,) in [("ci-a",), ("ci-a",), ("ci-b",)] {
            let _ = dispatch_extract(&req(
                "PUT",
                "/campaigns",
                &format!(
                    r#"{{"name":"n{ci}","connectInstanceId":"{ci}","dialerConfig":{{}},"outboundCallConfig":{{}}}}"#
                ),
                acct,
            ));
        }
        let body = |operator: &str, value: &str| {
            format!(
                r#"{{"filters":{{"instanceIdFilter":{{"value":"{value}","operator":"{operator}"}}}}}}"#
            )
        };
        let eq = dispatch_extract(&req(
            "POST",
            "/campaigns-summary",
            &body("Eq", "ci-a"),
            acct,
        ));
        let eq_list = parsed(&eq)["campaignSummaryList"].clone();
        assert_eq!(eq_list.as_array().map(Vec::len), Some(2));
        assert_eq!(parsed(&eq)["nextToken"], json!(Value::Null));

        for (operator, value, expected) in [
            ("Ne", "ci-a", 1usize),
            ("Contains", "a", 2),
            ("StartsWith", "ci-", 3),
        ] {
            let resp = dispatch_extract(&req(
                "POST",
                "/campaigns-summary",
                &body(operator, value),
                acct,
            ));
            assert_eq!(
                parsed(&resp)["campaignSummaryList"]
                    .as_array()
                    .map(Vec::len),
                Some(expected),
                "{operator}"
            );
        }

        let bad = dispatch_extract(&req(
            "POST",
            "/campaigns-summary",
            &body("Bad", "ci-a"),
            acct,
        ));
        assert_eq!(bad.status, 400);
        assert_eq!(err_code(&bad), "ValidationException");
        assert!(err_message(&bad).contains("Unsupported operator: Bad"));
    }

    #[test]
    fn list_pagination_token_round_trip() {
        let acct = next_account();
        for i in 0..3 {
            let _ = dispatch_extract(&req(
                "PUT",
                "/campaigns",
                &format!(
                    r#"{{"name":"p{i}","connectInstanceId":"ci","dialerConfig":{{}},"outboundCallConfig":{{}}}}"#
                ),
                acct,
            ));
        }
        let page1 = dispatch_extract(&req(
            "POST",
            "/campaigns-summary",
            r#"{"maxResults":2}"#,
            acct,
        ));
        let v = parsed(&page1);
        assert_eq!(v["campaignSummaryList"].as_array().map(Vec::len), Some(2));

        let token = v["nextToken"].as_str().unwrap().to_string();
        let page2 = dispatch_extract(&req(
            "POST",
            "/campaigns-summary",
            &format!(r#"{{"maxResults":2,"nextToken":"{token}"}}"#),
            acct,
        ));
        let v2 = parsed(&page2);
        assert_eq!(v2["campaignSummaryList"].as_array().map(Vec::len), Some(1));
        assert_eq!(v2["nextToken"], json!(Value::Null));

        let garbled = dispatch_extract(&req(
            "POST",
            "/campaigns-summary",
            r#"{"maxResults":2,"nextToken":"not-a-token"}"#,
            acct,
        ));
        assert_eq!(garbled.status, 400);
        assert_eq!(err_code(&garbled), "InvalidToken");
        assert_eq!(err_message(&garbled), "Invalid token");
    }

    #[test]
    fn connect_instance_config_and_onboarding() {
        let acct = next_account();
        let cfg = dispatch_extract(&req("GET", "/connect-instance/ci-1/config", "", acct));
        let v = parsed(&cfg)["connectInstanceConfig"].clone();
        assert_eq!(v["connectInstanceId"], json!("ci-1"));
        assert!(v["serviceLinkedRoleArn"]
            .as_str()
            .unwrap()
            .ends_with("AWSServiceRoleForConnectCampaigns"));
        // Cached across calls.
        let again = dispatch_extract(&req("GET", "/connect-instance/ci-1/config", "", acct));
        assert_eq!(parsed(&again)["connectInstanceConfig"], v);

        let bad_id = dispatch_extract(&req("GET", "/connect-instance/invalid-id/config", "", acct));
        assert_eq!(bad_id.status, 404);
        assert_eq!(err_code(&bad_id), "ResourceNotFoundException");

        let started = dispatch_extract(&req(
            "PUT",
            "/connect-instance/ci-1/onboarding",
            r#"{"encryptionConfig":{"enabled":false}}"#,
            acct,
        ));
        let status = parsed(&started)["connectInstanceOnboardingJobStatus"].clone();
        assert_eq!(started.status, 200);
        assert_eq!(status["connectInstanceId"], json!("ci-1"));
        assert_eq!(status["status"], json!("SUCCEEDED"));

        let empty = dispatch_extract(&req(
            "PUT",
            "/connect-instance/ci-2/onboarding",
            r#"{}"#,
            acct,
        ));
        assert_eq!(empty.status, 400);
        assert_eq!(err_code(&empty), "ValidationException");
        assert_eq!(
            err_message(&empty),
            "encryptionConfig is a required parameter"
        );

        let no_key = dispatch_extract(&req(
            "PUT",
            "/connect-instance/ci-2/onboarding",
            r#"{"encryptionConfig":{"enabled":true}}"#,
            acct,
        ));
        assert_eq!(
            err_message(&no_key),
            "keyArn is required when encryption is enabled"
        );

        // With custom keyArn the stored config carries it.
        let _ = dispatch_extract(&req(
            "PUT",
            "/connect-instance/ci-2/onboarding",
            r#"{"encryptionConfig":{"enabled":true,"keyArn":"arn:aws:kms:us-east-1:77:key/custom"}}"#,
            acct,
        ));
        let stored = dispatch_extract(&req("GET", "/connect-instance/ci-2/config", "", acct));
        let enc = parsed(&stored)["connectInstanceConfig"]["encryptionConfig"].clone();
        assert_eq!(enc["enabled"], json!(true));
        assert_eq!(enc["keyArn"], json!("arn:aws:kms:us-east-1:77:key/custom"));
    }

    #[test]
    fn tag_round_trip_and_missing_arn() {
        let acct = next_account();
        let created = dispatch_extract(&req(
            "PUT",
            "/campaigns",
            r#"{"name":"t","connectInstanceId":"ci","dialerConfig":{},"outboundCallConfig":{}}"#,
            acct,
        ));
        let arn_raw = parsed(&created)["arn"].as_str().unwrap().to_string();
        // botocore percent-encodes the ARN path segment.
        let arn_path = urlencoding::encode(&arn_raw);

        let listed = dispatch_extract(&req("GET", &format!("/tags/{arn_path}"), "", acct));
        assert_eq!(parsed(&listed)["tags"], json!({}));

        dispatch_extract(&req(
            "POST",
            &format!("/tags/{arn_path}"),
            r#"{"tags":{"env":"test","team":"dev"}}"#,
            acct,
        ));
        let listed = dispatch_extract(&req("GET", &format!("/tags/{arn_path}"), "", acct));
        assert_eq!(
            parsed(&listed)["tags"],
            json!({"env": "test", "team": "dev"})
        );

        let _ = dispatch_extract(&query_req(
            "DELETE",
            &format!("/tags/{arn_path}"),
            "tagKeys=team",
            acct,
        ));
        let listed = dispatch_extract(&req("GET", &format!("/tags/{arn_path}"), "", acct));
        assert_eq!(parsed(&listed)["tags"], json!({"env": "test"}));

        let stray = dispatch_extract(&req(
            "GET",
            "/tags/arn:aws:connectcampaigns:missing",
            "",
            acct,
        ));
        assert_eq!(stray.status, 404);
        assert_eq!(err_code(&stray), "ResourceNotFoundException");
        assert!(
            err_message(&stray).starts_with("Resource arn:aws:connectcampaigns:missing not found")
        );
    }

    #[test]
    fn fork_absent_operations_answer_501() {
        let acct = next_account();
        let stubs: [(&str, &str, &str, &str); 8] = [
            ("POST", "/campaigns-state", "get_campaign_state_batch", ""),
            ("POST", "/campaigns/camp-1/name", "update_campaign_name", ""),
            (
                "POST",
                "/campaigns/camp-1/dialer-config",
                "update_campaign_dialer_config",
                "",
            ),
            (
                "POST",
                "/campaigns/camp-1/outbound-call-config",
                "update_campaign_outbound_call_config",
                "",
            ),
            (
                "PUT",
                "/campaigns/camp-1/dial-requests",
                "put_dial_request_batch",
                "",
            ),
            (
                "DELETE",
                "/connect-instance/inst-1/config",
                "delete_connect_instance_config",
                "",
            ),
            (
                "DELETE",
                "/connect-instance/inst-1/onboarding",
                "delete_instance_onboarding_job",
                "",
            ),
            (
                "GET",
                "/connect-instance/inst-1/onboarding",
                "get_instance_onboarding_job_status",
                "",
            ),
        ];
        for (method, path, action, _) in stubs {
            let resp = dispatch_extract(&req(method, path, "", acct));
            assert_eq!(resp.status, 501, "{method} {path}");
            assert_eq!(err_code(&resp), "NotImplemented");
            assert_eq!(
                err_message(&resp),
                format!("The {action} action has not been implemented")
            );
        }
    }

    #[test]
    fn unroutable_paths_answer_moto_404() {
        let acct = next_account();
        for (method, path) in [
            ("GET", "/campaigns"),
            ("PATCH", "/campaigns/x"),
            ("GET", "/campaigns-unknown"),
            ("GET", "/connect-instance/inst-1/other"),
            ("PUT", "/awspath/nothing"),
        ] {
            let resp = dispatch_extract(&req(method, path, "", acct));
            assert_eq!(resp.status, 404, "{method} {path}");
            assert!(resp.body.is_empty(), "{method} {path}");
        }
    }
}
