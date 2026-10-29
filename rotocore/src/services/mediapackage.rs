//! MediaPackage backend (REST-JSON).
//!
//! Port of moto's MediaPackage: channels, origin endpoints and harvest
//! jobs. Wire shapes mirror moto's `to_dict` output. Ops moto does not
//! route (tags, configureLogs, rotate*) are implemented from the botocore
//! REST bindings (PUT /channels/{id}/configure_logs, PUT /channels/{id}/credentials,
//! PUT /channels/{id}/ingest_endpoints/{ep}/credentials, tags under /tags/{arn}).

use crate::proto::{json_error, parse_json_body, query_map, url_decode};
use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Value};

/// Insertion-ordered resource list: (id, record). Keeps list output in
/// creation order like moto's OrderedDict backends.
type Records = Vec<(String, Value)>;

#[derive(Default)]
struct State {
    channels: Records,
    origin_endpoints: Records,
    harvest_jobs: Records,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("mediapackage"));
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

fn not_found(what: &str) -> Response {
    json_error(400, "NotFoundException", &format!("{what} not found"))
}

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "mediapackage" {
        return None;
    }
    let method = req.method.as_str();
    let region = req.region.clone();
    let path = req.path.trim_end_matches('/');
    let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();

    let slot = state();
    let mut st = slot.write();
    let st = st.entry((req.account, req.region.clone())).or_default();

    route_method(method, region, st, req, &segments)
}

fn route_method(
    method: &str,
    region: String,
    st: &mut State,
    req: &Request,
    segments: &[&str],
) -> Option<Response> {
    // moto binds ids as `[^/.]+`; a segment containing '.' fails the bind and
    // the sidecar 501s — fall through to the same outcome here. Tag ARN
    // segments are checked raw (botocore leaves '.' unescaped in ARNs).
    if segments.iter().any(|s| s.contains('.')) {
        return None;
    }
    match segments {
        ["channels"] | ["channels", ""] => match method {
            "POST" => Some(create_channel(st, &req.body, &region)),
            "GET" => Some(json_list("channels", &st.channels)),
            _ => None,
        },
        ["channels", id] => match method {
            "GET" => Some(get_channel(st, id)),
            "PUT" => Some(update_channel(st, id, &parse_json_body(&req.body))),
            "DELETE" => Some(delete_channel(st, id)),
            _ => None,
        },
        ["channels", id, "configure_logs"] if method == "PUT" => {
            Some(configure_logs(st, id, &parse_json_body(&req.body)))
        }
        ["channels", id, "credentials"] if method == "PUT" => {
            // RotateChannelCredentials: re-renders the channel (fresh ingest
            // credentials are not modeled; moto tracks none).
            Some(get_channel(st, id))
        }
        ["channels", id, "ingest_endpoints", _ep, "credentials"] if method == "PUT" => {
            // RotateIngestEndpointCredentials: missing channel → NotFound.
            Some(get_channel(st, id))
        }
        ["origin_endpoints"] | ["origin_endpoints", ""] => match method {
            "POST" => Some(create_origin_endpoint(
                st,
                &parse_json_body(&req.body),
                &region,
            )),
            "GET" => Some(json_list("originEndpoints", &st.origin_endpoints)),
            _ => None,
        },
        ["origin_endpoints", id] => match method {
            "GET" => Some(get_origin_endpoint(st, id)),
            "PUT" => Some(update_origin_endpoint(st, id, &parse_json_body(&req.body))),
            "DELETE" => Some(delete_origin_endpoint(st, id)),
            _ => None,
        },
        ["harvest_jobs"] | ["harvest_jobs", ""] => match method {
            "POST" => Some(create_harvest_job(st, &parse_json_body(&req.body), &region)),
            "GET" => Some(json_list("harvestJobs", &st.harvest_jobs)),
            _ => None,
        },
        ["harvest_jobs", id] if method == "GET" => Some(get_harvest_job(st, id)),
        ["tags", arn_seg] => {
            let arn = url_decode(arn_seg);
            match (method, index_by_arn(st, &arn)) {
                ("GET", Some(target)) => {
                    let tags = target.tags(st).as_object().cloned().unwrap_or_default();
                    Some(ok_json(&json!({ "tags": tags })))
                }
                ("POST", Some(target)) => {
                    let p = parse_json_body(&req.body);
                    let record = target.record_mut(st);
                    if record["tags"].is_null() {
                        record["tags"] = json!({});
                    }
                    if let (Some(dest), Some(src)) =
                        (record["tags"].as_object_mut(), p["tags"].as_object())
                    {
                        for (k, v) in src {
                            dest.insert(k.clone(), v.clone());
                        }
                    }
                    Some(Response::new(204, "application/json", ""))
                }
                ("DELETE", Some(target)) => {
                    let keys = query_map(&req.query_string)
                        .get("tagKeys")
                        .cloned()
                        .unwrap_or_default();
                    let record = target.record_mut(st);
                    if let Some(tags) = record["tags"].as_object_mut() {
                        for key in keys {
                            tags.remove(&key);
                        }
                    }
                    Some(Response::new(204, "application/json", ""))
                }
                ("GET" | "POST" | "DELETE", None) => {
                    Some(not_found(&format!("resource with arn={arn}")))
                }
                _ => None,
            }
        }
        _ => None,
    }
}

fn ok_json(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

fn json_list(key: &str, records: &Records) -> Response {
    let items: Vec<Value> = records.iter().map(|(_, v)| v.clone()).collect();
    ok_json(&json!({ key: items }))
}

fn create_channel(st: &mut State, body: &[u8], region: &str) -> Response {
    let p = parse_json_body(body);
    let id = p["id"].as_str().unwrap_or_default().to_string();
    let channel = json!({
        "arn": format!("arn:{}:mediapackage:channel:{}", partition(region), id),
        "id": id,
        "description": p["description"].clone(),
        "tags": p["tags"].clone(),
    });
    st.channels.retain(|(k, _)| *k != id);
    st.channels.push((id.clone(), channel.clone()));
    ok_json(&channel)
}

fn find<'a>(records: &'a Records, id: &str) -> Option<&'a Value> {
    records.iter().find(|(k, _)| *k == id).map(|(_, v)| v)
}

fn get_channel(st: &State, id: &str) -> Response {
    match find(&st.channels, id) {
        Some(c) => ok_json(c),
        None => not_found(&format!("channel with id={id}")),
    }
}

fn update_channel(st: &mut State, id: &str, p: &Value) -> Response {
    match st.channels.iter_mut().find(|(k, _)| *k == id) {
        Some((_, c)) => {
            c["description"] = p["description"].clone();
            ok_json(c)
        }
        None => not_found(&format!("channel with id={id}")),
    }
}

fn delete_channel(st: &mut State, id: &str) -> Response {
    match find(&st.channels, id) {
        Some(c) => {
            let v = c.clone();
            st.channels.retain(|(k, _)| *k != id);
            ok_json(&v)
        }
        None => not_found(&format!("channel with id={id}")),
    }
}

fn create_origin_endpoint(st: &mut State, p: &Value, region: &str) -> Response {
    let id = p["id"].as_str().unwrap_or_default().to_string();
    let endpoint = json!({
        "arn": format!("arn:{}:mediapackage:origin_endpoint:{}", partition(region), id),
        "authorization": p["authorization"].clone(),
        "channelId": p["channelId"].clone(),
        "cmafPackage": p["cmafPackage"].clone(),
        "dashPackage": p["dashPackage"].clone(),
        "description": p["description"].clone(),
        "hlsPackage": p["hlsPackage"].clone(),
        "id": id,
        "manifestName": p["manifestName"].clone(),
        "mssPackage": p["mssPackage"].clone(),
        "origination": p["origination"].clone(),
        "startoverWindowSeconds": p["startoverWindowSeconds"].clone(),
        "tags": p["tags"].clone(),
        "timeDelaySeconds": p["timeDelaySeconds"].clone(),
        "url": format!(
            "https://origin-endpoint.mediapackage.{region}.amazonaws.com/{id}"
        ),
        "whitelist": p["whitelist"].clone(),
    });
    st.origin_endpoints.retain(|(k, _)| *k != id);
    st.origin_endpoints.push((id.clone(), endpoint.clone()));
    ok_json(&endpoint)
}

fn get_origin_endpoint(st: &State, id: &str) -> Response {
    match find(&st.origin_endpoints, id) {
        Some(ep) => ok_json(ep),
        None => not_found(&format!("origin endpoint with id={id}")),
    }
}

fn update_origin_endpoint(st: &mut State, id: &str, p: &Value) -> Response {
    match st.origin_endpoints.iter_mut().find(|(k, _)| *k == id) {
        Some((_, ep)) => {
            for key in [
                "authorization",
                "cmafPackage",
                "dashPackage",
                "description",
                "hlsPackage",
                "manifestName",
                "mssPackage",
                "origination",
                "startoverWindowSeconds",
                "timeDelaySeconds",
                "whitelist",
            ] {
                ep[key] = p[key].clone();
            }
            ok_json(ep)
        }
        None => not_found(&format!("origin endpoint with id={id}")),
    }
}

fn delete_origin_endpoint(st: &mut State, id: &str) -> Response {
    match find(&st.origin_endpoints, id) {
        Some(ep) => {
            let v = ep.clone();
            st.origin_endpoints.retain(|(k, _)| *k != id);
            ok_json(&v)
        }
        None => not_found(&format!("origin endpoint with id={id}")),
    }
}

fn create_harvest_job(st: &mut State, p: &Value, region: &str) -> Response {
    let id = p["id"].as_str().unwrap_or_default().to_string();
    let origin_endpoint_id = p["originEndpointId"].as_str().unwrap_or_default();
    let Some(channel_id) = find(&st.origin_endpoints, origin_endpoint_id)
        .and_then(|ep| ep["channelId"].as_str())
        .map(str::to_string)
    else {
        return not_found(&format!("origin endpoint with id={origin_endpoint_id}"));
    };
    let job = json!({
        "id": id,
        "arn": format!("arn:{}:mediapackage:harvest_job:{}", partition(region), id),
        "channelId": channel_id,
        "startTime": p["startTime"].clone(),
        "endTime": p["endTime"].clone(),
        "s3Destination": p["s3Destination"].clone(),
        "originEndpointId": origin_endpoint_id,
        "status": "IN_PROGRESS",
        "createdAt": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    });
    st.harvest_jobs.retain(|(k, _)| *k != id);
    st.harvest_jobs.push((id.clone(), job.clone()));
    ok_json(&job)
}

fn get_harvest_job(st: &State, id: &str) -> Response {
    match find(&st.harvest_jobs, id) {
        Some(j) => ok_json(j),
        None => not_found(&format!("harvest job with id={id}")),
    }
}

/// ConfigureLogs — stores the log-config members on the channel and re-renders
/// it (wire: PUT /channels/{id}/configure_logs).
fn configure_logs(st: &mut State, id: &str, p: &Value) -> Response {
    match st.channels.iter_mut().find(|(k, _)| *k == id) {
        Some((_, c)) => {
            c["ingressAccessLogs"] = p["ingressAccessLogs"].clone();
            c["egressAccessLogs"] = p["egressAccessLogs"].clone();
            ok_json(c)
        }
        None => not_found(&format!("channel with id={id}")),
    }
}

/// Position of a resource by ARN; tag ops apply to channels and origin
/// endpoints (the taggable resources in moto's model).
enum TagTarget {
    Channel(usize),
    OriginEndpoint(usize),
}

impl TagTarget {
    fn tags<'a>(&self, st: &'a State) -> &'a Value {
        match self {
            TagTarget::Channel(i) => &st.channels[*i].1["tags"],
            TagTarget::OriginEndpoint(i) => &st.origin_endpoints[*i].1["tags"],
        }
    }

    fn record_mut<'a>(&self, st: &'a mut State) -> &'a mut Value {
        match self {
            TagTarget::Channel(i) => &mut st.channels[*i].1,
            TagTarget::OriginEndpoint(i) => &mut st.origin_endpoints[*i].1,
        }
    }
}

fn index_by_arn(st: &State, arn: &str) -> Option<TagTarget> {
    st.channels
        .iter()
        .position(|(_, v)| v["arn"].as_str() == Some(arn))
        .map(TagTarget::Channel)
        .or_else(|| {
            st.origin_endpoints
                .iter()
                .position(|(_, v)| v["arn"].as_str() == Some(arn))
                .map(TagTarget::OriginEndpoint)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn req(method: &str, path: &str, body: Value) -> Request {
        let (path, query_string) = match path.split_once('?') {
            Some((p, q)) => (p.to_string(), q.to_string()),
            None => (path.to_string(), String::new()),
        };
        Request {
            service: "mediapackage".into(),
            operation: String::new(),
            method: method.into(),
            path,
            query_string,
            headers: std::collections::HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap_or_default()),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    fn body(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap_or(Value::Null)
    }

    // Fresh scope per test: unique account keeps the shared slot disjoint.
    fn scope() -> u64 {
        uuid::Uuid::new_v4().as_u128() as u64
    }

    #[test]
    fn channel_lifecycle() {
        let acct = scope();
        let mut r = req(
            "POST",
            "/channels",
            json!({"id": "ch1", "description": "d"}),
        );
        r.account = acct;
        let resp = dispatch(&r).unwrap();
        assert_eq!(resp.status, 200);
        let b = body(&resp);
        assert_eq!(b["arn"], "arn:aws:mediapackage:channel:ch1");
        assert_eq!(b["id"], "ch1");

        // Describe
        r = req("GET", "/channels/ch1", json!({}));
        r.account = acct;
        assert_eq!(body(&dispatch(&r).unwrap())["id"], "ch1");

        // List
        r = req("GET", "/channels", json!({}));
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(b["channels"][0]["id"], "ch1");

        // Update description
        r = req("PUT", "/channels/ch1", json!({"description": "new"}));
        r.account = acct;
        assert_eq!(body(&dispatch(&r).unwrap())["description"], "new");

        // Delete removes it
        r = req("DELETE", "/channels/ch1", json!({}));
        r.account = acct;
        assert_eq!(dispatch(&r).unwrap().status, 200);
        r = req("GET", "/channels/ch1", json!({}));
        r.account = acct;
        let resp = dispatch(&r).unwrap();
        assert_eq!(resp.status, 400);
        assert_eq!(body(&resp)["__type"], "NotFoundException");
    }

    #[test]
    fn origin_endpoint_and_harvest_flow() {
        let acct = scope();
        let mut r = req("POST", "/channels", json!({"id": "chA"}));
        r.account = acct;
        dispatch(&r).unwrap();

        r = req(
            "POST",
            "/origin_endpoints",
            json!({"id": "epA", "channelId": "chA", "hlsPackage": {}}),
        );
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(b["channelId"], "chA");
        assert!(b["url"]
            .as_str()
            .unwrap()
            .contains("origin-endpoint.mediapackage.us-east-1.amazon"));

        // Harvest job against the endpoint
        r = req(
            "POST",
            "/harvest_jobs",
            json!({"id": "hjA", "originEndpointId": "epA", "startTime": "s", "endTime": "e"}),
        );
        r.account = acct;
        let resp = dispatch(&r).unwrap();
        let b = body(&resp);
        assert_eq!(b["channelId"], "chA");
        assert_eq!(b["status"], "IN_PROGRESS");
        assert_eq!(b["originEndpointId"], "epA");

        // Harvest job against a missing endpoint -> channel id missing
        r = req(
            "POST",
            "/harvest_jobs",
            json!({"id": "hjB", "originEndpointId": "nope"}),
        );
        r.account = acct;
        let resp = dispatch(&r).unwrap();
        assert_eq!(resp.status, 400);

        // Describe missing harvest job
        r = req("GET", "/harvest_jobs/no-such", json!({}));
        r.account = acct;
        let resp = dispatch(&r).unwrap();
        assert_eq!(body(&resp)["__type"], "NotFoundException");

        // Lists include created records
        r = req("GET", "/harvest_jobs", json!({}));
        r.account = acct;
        assert_eq!(body(&dispatch(&r).unwrap())["harvestJobs"][0]["id"], "hjA");
    }

    #[test]
    fn unimplemented_ops_fall_through() {
        let acct = scope();
        // Known root with an unrouted method -> None (caller 501s).
        let mut r = req("POST", "/channels/ch1", json!({}));
        r.account = acct;
        assert!(dispatch(&r).is_none());
        // Non-existent roots also route nowhere.
        r.path = "/channels/ch1/ingest_endpoints/e/rotate_credentials".into();
        assert!(dispatch(&r).is_none());
        r.path = "/no-such-resource".into();
        assert!(dispatch(&r).is_none());
    }

    #[test]
    fn segment_binding() {
        // moto rejects '.' ids (bind failure -> caller 501s).
        assert!(dispatch(&req("GET", "/channels/ch.1", json!({}))).is_none());
        assert!(dispatch(&req("GET", "/", json!({}))).is_none());
        assert!(dispatch(&req("GET", "/channels", json!({}))).is_some());
    }

    #[test]
    fn configure_logs_and_rotate() {
        let acct = scope();
        let mut r = req(
            "POST",
            "/channels",
            json!({"id": "chC", "tags": {"env": "test"}}),
        );
        r.account = acct;
        dispatch(&r).unwrap();

        // ConfigureLogs updates and re-renders the channel.
        r = req(
            "PUT",
            "/channels/chC/configure_logs",
            json!({"ingressAccessLogs": {"logGroupName": "g"}}),
        );
        r.account = acct;
        let resp = dispatch(&r).unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(body(&resp)["id"], "chC");
        assert_eq!(body(&resp)["ingressAccessLogs"]["logGroupName"], "g");

        r = req("PUT", "/channels/no-such/configure_logs", json!({}));
        r.account = acct;
        assert_eq!(dispatch(&r).unwrap().status, 400);

        // RotateChannelCredentials re-renders the channel.
        r = req("PUT", "/channels/chC/credentials", json!({}));
        r.account = acct;
        let resp = dispatch(&r).unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(body(&resp)["id"], "chC");

        r = req("PUT", "/channels/no-such/credentials", json!({}));
        r.account = acct;
        assert_eq!(dispatch(&r).unwrap().status, 400);

        // RotateIngestEndpointCredentials: missing channel -> NotFound.
        r = req(
            "PUT",
            "/channels/no-such/ingest_endpoints/ep/credentials",
            json!({}),
        );
        r.account = acct;
        let resp = dispatch(&r).unwrap();
        assert_eq!(resp.status, 400);
        assert_eq!(body(&resp)["__type"], "NotFoundException");

        r = req(
            "PUT",
            "/channels/chC/ingest_endpoints/ep/credentials",
            json!({}),
        );
        r.account = acct;
        assert_eq!(dispatch(&r).unwrap().status, 200);
    }

    #[test]
    fn tag_ops_round_trip() {
        let acct = scope();
        let mut r = req(
            "POST",
            "/channels",
            json!({"id": "chT", "tags": {"env": "test"}}),
        );
        r.account = acct;
        let created = body(&dispatch(&r).unwrap());
        let arn = created["arn"].as_str().unwrap().to_string();
        // botocore percent-encodes ':' in the URI (dots stay raw).
        let encoded = arn.replace(':', "%3A");
        assert!(encoded.find('.').is_none());

        // ListTagsForResource
        let mut r = req("GET", &format!("/tags/{encoded}"), json!({}));
        r.account = acct;
        assert_eq!(body(&dispatch(&r).unwrap())["tags"]["env"], "test");

        // TagResource merges (204 empty).
        r = req(
            "POST",
            &format!("/tags/{encoded}"),
            json!({"tags": {"stage": "prod"}}),
        );
        r.account = acct;
        let resp = dispatch(&r).unwrap();
        assert_eq!(resp.status, 204);

        r = req("GET", &format!("/tags/{encoded}"), json!({}));
        r.account = acct;
        let tags = body(&dispatch(&r).unwrap())["tags"].clone();
        assert_eq!(tags["env"], "test");
        assert_eq!(tags["stage"], "prod");

        // UntagResource removes query-listed keys (204 empty).
        r = req("DELETE", &format!("/tags/{encoded}?tagKeys=env"), json!({}));
        r.account = acct;
        assert_eq!(dispatch(&r).unwrap().status, 204);

        r = req("GET", &format!("/tags/{encoded}"), json!({}));
        r.account = acct;
        let tags = body(&dispatch(&r).unwrap())["tags"].clone();
        assert!(tags.get("env").is_none());
        assert_eq!(tags["stage"], "prod");

        // Unknown arn -> NotFound.
        r = req(
            "GET",
            "/tags/arn%3Aaws%3Amediapackage%3Achannel%3Anope",
            json!({}),
        );
        r.account = acct;
        let resp = dispatch(&r).unwrap();
        assert_eq!(resp.status, 400);
        assert_eq!(body(&resp)["__type"], "NotFoundException");
    }
}
