//! MediaPackage backend (REST-JSON).
//!
//! Port of moto's MediaPackage: channels, origin endpoints and harvest
//! jobs. Wire shapes mirror moto's `to_dict` output; ops moto does not
//! route (tags, configureLogs, rotate*) fall through to the 501 like the
//! sidecar's bind failure.

use crate::proto::{json_error, parse_json_body};
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
    // moto binds `[^/.]+` for ids; an id containing a dot fails the bind and
    // the sidecar 501s — fall through to the same outcome here.
    let (root, sub) = path_parts(&req.path)?;
    let method = req.method.as_str();
    let region = req.region.clone();

    let slot = state();
    let mut st = slot.write();
    let st = st.entry((req.account, req.region.clone())).or_default();

    match (method, root, sub) {
        (_, "channels", None) | (_, "channels", Some("")) => match method {
            "POST" => Some(create_channel(st, &req.body, &region)),
            "GET" => Some(json_list("channels", &st.channels)),
            _ => None,
        },
        ("GET" | "PUT" | "DELETE", "channels", Some(id)) => match method {
            "GET" => Some(get_channel(st, id)),
            "PUT" => Some(update_channel(st, id, &parse_json_body(&req.body))),
            "DELETE" => Some(delete_channel(st, id)),
            _ => None,
        },
        (_, "origin_endpoints", None) | (_, "origin_endpoints", Some("")) => match method {
            "POST" => Some(create_origin_endpoint(
                st,
                &parse_json_body(&req.body),
                &region,
            )),
            "GET" => Some(json_list("originEndpoints", &st.origin_endpoints)),
            _ => None,
        },
        ("GET" | "PUT" | "DELETE", "origin_endpoints", Some(id)) => match method {
            "GET" => Some(get_origin_endpoint(st, id)),
            "PUT" => Some(update_origin_endpoint(st, id, &parse_json_body(&req.body))),
            "DELETE" => Some(delete_origin_endpoint(st, id)),
            _ => None,
        },
        (_, "harvest_jobs", None) | (_, "harvest_jobs", Some("")) => match method {
            "POST" => Some(create_harvest_job(st, &parse_json_body(&req.body), &region)),
            "GET" => Some(json_list("harvestJobs", &st.harvest_jobs)),
            _ => None,
        },
        ("GET", "harvest_jobs", Some(id)) => Some(get_harvest_job(st, id)),
        _ => None,
    }
}

/// Split "/a/b/c" into root "a" and the remainder after the second slash.
/// A remainder containing "." fails moto's `[^/.]+` path binding and the
/// sidecar 501s, so those also route nowhere here.
/// Returns None when the path does not start with "/<root>".
fn path_parts(path: &str) -> Option<(&str, Option<&str>)> {
    let rest = path.strip_prefix('/')?;
    let (root, sub) = match rest.find('/') {
        Some(i) => (&rest[..i], Some(&rest[i + 1..])),
        None => (rest, None),
    };
    if root.is_empty() {
        return None;
    }
    match sub {
        None => Some((root, None)),
        Some(s) => {
            if s.is_empty() {
                // "/channels/" still binds the collection (strict_slashes).
                Some((root, Some(s)))
            } else if s.contains('/') || s.contains('.') {
                // moto binds ids as one segment `[^/.]+`; anything else fails
                // the route bind and the sidecar 501s — route nowhere here.
                None
            } else {
                Some((root, Some(s)))
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn req(method: &str, path: &str, body: Value) -> Request {
        Request {
            service: "mediapackage".into(),
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
    fn path_splitting() {
        assert_eq!(path_parts("/channels"), Some(("channels", None)));
        assert_eq!(path_parts("/channels/"), Some(("channels", Some(""))));
        assert_eq!(
            path_parts("/channels/ch-1"),
            Some(("channels", Some("ch-1")))
        );
        assert!(path_parts("/channels/ch.1").is_none()); // moto rejects '.' ids
        assert!(dispatch(&req("GET", "/", json!({}))).is_none());
    }
}
