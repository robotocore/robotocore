//! Unit tests for the managedblockchain backend, one family per op group.

use super::*;
use bytes::Bytes;

struct Ctx(u64);

impl Ctx {
    fn req(&self, method: &str, path: &str, body: Value) -> Request {
        let (path_part, query) = match path.split_once('?') {
            Some((p, q)) => (p.to_string(), q.to_string()),
            None => (path.to_string(), String::new()),
        };
        Request {
            service: "managedblockchain".into(),
            operation: String::new(),
            method: method.into(),
            path: path_part,
            query_string: query,
            headers: HashMap::from([
                ("content-type".to_string(), "application/json".into()),
                ("x-robotocore-path".to_string(), path.into()),
            ]),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            account: self.0,
            region: "us-east-1".into(),
        }
    }
}

fn fresh_ctx() -> Ctx {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(121111111110);
    Ctx(NEXT.fetch_add(1, Ordering::SeqCst))
}

fn member_config(name: &str) -> Value {
    json!({
        "Name": name,
        "Description": "Test member",
        "FrameworkConfiguration": {"Fabric": {"AdminUsername": "admin", "AdminPassword": "Password123!"}},
        "LogPublishingConfiguration": {"Fabric": {"CaLogs": {"Cloudwatch": {"Enabled": false}}}},
    })
}

fn network_payload(ctx: &Ctx, name: &str) -> Request {
    let mut req = ctx.req(
        "POST",
        "/networks",
        json!({
            "ClientRequestToken": "tok",
            "Name": name,
            "Framework": "HYPERLEDGER_FABRIC",
            "FrameworkVersion": "1.2",
            "FrameworkConfiguration": {"Fabric": {"Edition": "STARTER"}},
            "VotingPolicy": {
                "ApprovalThresholdPolicy": {
                    "ThresholdPercentage": 50,
                    "ProposalDurationInHours": 24,
                    "ThresholdComparator": "GREATER_THAN",
                }
            },
            "MemberConfiguration": member_config("m1"),
        }),
    );
    req.account = ctx.0;
    req
}

fn create_network(ctx: &Ctx) -> (String, String) {
    let resp = dispatch(&network_payload(ctx, "test-network"));
    assert_eq!(resp.status, 200);
    let v: Value = serde_json::from_slice(&resp.body).unwrap();
    (
        v["NetworkId"].as_str().unwrap().into(),
        v["MemberId"].as_str().unwrap().into(),
    )
}

fn parsed(resp: &Response) -> Value {
    if resp.body.is_empty() {
        return json!({});
    }
    serde_json::from_slice(&resp.body).unwrap()
}

#[test]
fn create_network_returns_ids_and_lists_summary() {
    let ctx = fresh_ctx();
    let (nid, mid) = create_network(&ctx);
    assert!(nid.starts_with("n-"));
    assert!(mid.starts_with("m-"));

    let list = parsed(&dispatch(&ctx.req("GET", "/networks", json!({}))));
    let networks = list["Networks"].as_array().unwrap();
    let found = networks.iter().find(|n| n["Id"] == nid).unwrap();
    assert_eq!(found["Name"], "test-network".to_string());
    assert!(found.get("Votes").is_none());

    let got = parsed(&dispatch(&ctx.req(
        "GET",
        &format!("/networks/{nid}"),
        json!({}),
    )));
    let net = &got["Network"];
    assert_eq!(net["Id"], nid);
    assert_eq!(net["Framework"], "HYPERLEDGER_FABRIC");
    assert_eq!(net["FrameworkAttributes"]["Fabric"]["Edition"], "STARTER");
    let endpoint = net["FrameworkAttributes"]["Fabric"]["OrderingServiceEndpoint"]
        .as_str()
        .unwrap();
    assert!(endpoint.contains(&nid.to_lowercase()));
}

#[test]
fn create_network_rejects_bad_framework() {
    let ctx = fresh_ctx();
    let mut payload = network_payload(&ctx, "bad");
    let mut body: serde_json::Map<String, Value> = serde_json::from_slice(&payload.body).unwrap();
    body.insert("Framework".into(), Value::String("ETHEREUM".into()));
    payload.body = Bytes::from(serde_json::to_vec(&body).unwrap());
    let resp = dispatch(&payload);
    assert_eq!(resp.status, 400);
    let err = parsed(&resp);
    assert!(err["message"]
        .as_str()
        .unwrap()
        .contains("BadRequestException"));
}

#[test]
fn member_lifecycle() {
    let ctx = fresh_ctx();
    let (nid, mid) = create_network(&ctx);

    let got = parsed(&dispatch(&ctx.req(
        "GET",
        &format!("/networks/{nid}/members/{mid}"),
        json!({}),
    )));
    let m = &got["Member"];
    assert_eq!(m["Id"], mid);
    assert_eq!(m["Name"], "m1");
    assert_eq!(m["FrameworkAttributes"]["Fabric"]["AdminUsername"], "admin");

    let list = parsed(&dispatch(&ctx.req(
        "GET",
        &format!("/networks/{nid}/members"),
        json!({}),
    )));
    assert_eq!(list["Members"].as_array().unwrap().len(), 1);

    // Deleting then getting must 404 like moto.
    let del = dispatch(&ctx.req(
        "DELETE",
        &format!("/networks/{nid}/members/{mid}"),
        json!({}),
    ));
    assert_eq!(del.status, 200);
    let gone = dispatch(&ctx.req("GET", &format!("/networks/{nid}/members/{mid}"), json!({})));
    assert_eq!(gone.status, 404);
}

#[test]
fn proposal_flow_mints_invitation() {
    let ctx = fresh_ctx();
    let (nid, mid) = create_network(&ctx);
    let pr = parsed(&dispatch(&ctx.req(
        "POST",
        &format!("/networks/{nid}/proposals"),
        json!({"MemberId": mid, "Actions": {"Invitations": [{"Principal": "123456789012"}]}}),
    )));
    let pid = pr["ProposalId"].as_str().unwrap().to_string();

    let got = parsed(&dispatch(&ctx.req(
        "GET",
        &format!("/networks/{nid}/proposals/{pid}"),
        json!({}),
    )));
    let p = &got["Proposal"];
    assert_eq!(p["ProposalId"], pid);
    assert_eq!(p["Status"], "IN_PROGRESS");
    assert_eq!(p["OutstandingVoteCount"], 1);

    let before = parsed(&dispatch(&ctx.req("GET", "/invitations", json!({}))));
    let before_count = before["Invitations"].as_array().unwrap().len();

    let vote = dispatch(&ctx.req(
        "POST",
        &format!("/networks/{nid}/proposals/{pid}/votes"),
        json!({"VoterMemberId": mid, "Vote": "YES"}),
    ));
    assert_eq!(vote.status, 200);

    let props = parsed(&dispatch(&ctx.req(
        "GET",
        &format!("/networks/{nid}/proposals"),
        json!({}),
    )));
    assert_eq!(props["Proposals"][0]["Status"], "APPROVED");

    let votes = parsed(&dispatch(&ctx.req(
        "GET",
        &format!("/networks/{nid}/proposals/{pid}/votes"),
        json!({}),
    )));
    let vs = votes["ProposalVotes"].as_array().unwrap();
    assert_eq!(vs.len(), 1);
    assert_eq!(vs[0]["Vote"], "YES");
    assert_eq!(vs[0]["MemberId"], mid);

    let after = parsed(&dispatch(&ctx.req("GET", "/invitations", json!({}))));
    let after_list = after["Invitations"].as_array().unwrap();
    assert_eq!(after_list.len(), before_count + 1);
    let pending = after_list.last().unwrap();
    assert_eq!(pending["Status"], "PENDING");
    assert_eq!(pending["NetworkSummary"]["Id"], nid);

    // Double-vote on a decided proposal is invalid (moto order: status is
    // checked before the already-voted bookkeeping).
    let again = dispatch(&ctx.req(
        "POST",
        &format!("/networks/{nid}/proposals/{pid}/votes"),
        json!({"VoterMemberId": mid, "Vote": "NO"}),
    ));
    assert_eq!(again.status, 400);
}

#[test]
fn node_crud_and_not_found() {
    let ctx = fresh_ctx();
    let (nid, mid) = create_network(&ctx);
    let node = parsed(&dispatch(&ctx.req(
        "POST",
        &format!("/networks/{nid}/nodes"),
        json!({
            "MemberId": mid,
            "NodeConfiguration": {
                "InstanceType": "bc.t3.small",
                "AvailabilityZone": "us-east-1a",
                "LogPublishingConfiguration": {"Fabric": {"PeerLogs": {"Cloudwatch": {"Enabled": false}}}},
            },
        }),
    )));
    let ndid = node["NodeId"].as_str().unwrap().to_string();

    let list = parsed(&dispatch(&ctx.req(
        "GET",
        &format!("/networks/{nid}/nodes?memberId={mid}"),
        json!({}),
    )));
    assert_eq!(list["Nodes"].as_array().unwrap().len(), 1);

    let got = parsed(&dispatch(&ctx.req(
        "GET",
        &format!("/networks/{nid}/nodes/{ndid}?memberId={mid}"),
        json!({}),
    )));
    let n = &got["Node"];
    assert_eq!(n["Id"], ndid);
    assert_eq!(n["InstanceType"], "bc.t3.small");
    let peer = n["FrameworkAttributes"]["Fabric"]["PeerEndpoint"]
        .as_str()
        .unwrap();
    assert!(peer.starts_with(&ndid.to_lowercase()));
    assert!(peer.ends_with("amazonaws.com:30003"));

    // Unknown member id → ResourceNotFoundException.
    let nf = dispatch(&ctx.req(
        "GET",
        &format!("/networks/{nid}/nodes?memberId=m-UNKNOWN"),
        json!({}),
    ));
    assert_eq!(nf.status, 404);

    let del = dispatch(&ctx.req(
        "DELETE",
        &format!("/networks/{nid}/nodes/{ndid}?memberId={mid}"),
        json!({}),
    ));
    assert_eq!(del.status, 200);
    let gone = dispatch(&ctx.req(
        "GET",
        &format!("/networks/{nid}/nodes/{ndid}?memberId={mid}"),
        json!({}),
    ));
    assert_eq!(gone.status, 404);
}

#[test]
fn invalid_inputs_surface_moto_errors() {
    let ctx = fresh_ctx();
    let (nid, _mid) = create_network(&ctx);
    // Unknown member on CreateNode → ResourceNotFoundException (moto order).
    let resp = dispatch(&ctx.req(
        "POST",
        &format!("/networks/{nid}/nodes"),
        json!({
            "MemberId": "m-NOPE",
            "NodeConfiguration": {"InstanceType": "bc.t3.large", "AvailabilityZone": "us-east-1a"},
        }),
    ));
    assert_eq!(resp.status, 404);
}

#[test]
fn reject_invitation_missing_is_404() {
    let ctx = fresh_ctx();
    let resp = dispatch(&ctx.req("DELETE", "/invitations/inv-doesnotexist12345", json!({})));
    assert_eq!(resp.status, 404);
}

#[test]
fn accessors_and_tags() {
    let ctx = fresh_ctx();
    let created = parsed(&dispatch(&ctx.req(
        "POST",
        "/accessors",
        json!({"AccessorType": "BILLING_TOKEN", "Tags": {"Env": "test"}}),
    )));
    let aid = created["AccessorId"].as_str().unwrap().to_string();
    assert!(created["BillingToken"].as_str().is_some());

    let got = parsed(&dispatch(&ctx.req(
        "GET",
        &format!("/accessors/{aid}"),
        json!({}),
    )));
    let acc = &got["Accessor"];
    assert_eq!(acc["Id"], aid);
    assert_eq!(acc["Status"], "AVAILABLE");
    assert_eq!(acc["Tags"]["Env"], "test");
    assert!(acc["Arn"]
        .as_str()
        .unwrap()
        .ends_with(&format!("accessors/{aid}")));

    let arn = acc["Arn"].as_str().unwrap().to_string();
    let list = parsed(&dispatch(&ctx.req("GET", "/accessors", json!({}))));
    assert!(list["Accessors"]
        .as_array()
        .unwrap()
        .iter()
        .any(|a| a["Id"] == aid));

    let tagged = dispatch(&ctx.req(
        "POST",
        &format!("/tags/{arn}"),
        json!({"Tags": {"NewTag": "val"}}),
    ));
    assert_eq!(tagged.status, 200);
    let tags = parsed(&dispatch(&ctx.req(
        "GET",
        &format!("/tags/{arn}"),
        json!({}),
    )));
    assert_eq!(tags["Tags"]["NewTag"], "val");
    let untagged = dispatch(&ctx.req("DELETE", &format!("/tags/{arn}?tagKeys=Env"), json!({})));
    assert_eq!(untagged.status, 200);
    let tags2 = parsed(&dispatch(&ctx.req(
        "GET",
        &format!("/tags/{arn}"),
        json!({}),
    )));
    assert!(tags2["Tags"].get("Env").is_none());

    let del = dispatch(&ctx.req("DELETE", &format!("/accessors/{aid}"), json!({})));
    assert_eq!(del.status, 200);
    let list2 = parsed(&dispatch(&ctx.req("GET", "/accessors", json!({}))));
    assert!(!list2["Accessors"]
        .as_array()
        .unwrap()
        .iter()
        .any(|a| a["Id"] == aid));

    let missing = dispatch(&ctx.req("GET", "/accessors/nonexistent", json!({})));
    assert_eq!(missing.status, 404);
    let err = parsed(&missing);
    assert!(err["message"]
        .as_str()
        .unwrap()
        .contains("ResourceNotFoundException"));
}
