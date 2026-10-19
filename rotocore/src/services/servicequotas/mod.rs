//! AWS Service Quotas backend — moto-equivalent (json protocol).
//!
//! Replaces the moto sidecar for `service-quotas`
//! (`X-Amz-Target: ServiceQuotasV20190624.<Op>`; the gateway router's target
//! map arrives here under the name `servicequotas`, the signing scope's
//! alias is `service-quotas`, so both spellings are served). State is
//! per-(account, region) like every backend, but Service Quotas carries no
//! mutable state: moto's `ServiceQuotasBackend` is pure table lookup, so the
//! backend is stateless and responses are identical across scopes.
//!
//! moto's surface is exactly two operations (moto/servicequotas/
//! responses.py): `list_aws_default_service_quotas` and `get_service_quota`,
//! both backed by a static 25-row VPC default-quotas table
//! (`resources/default_quotas/vpc.py`) — including its hardcoded
//! `eu-west-1` QuotaArns. Unknown ServiceCode/QuotaCode answers moto's
//! `NoSuchResourceException` (400). Every other botocore operation falls
//! through moto's `call_action` to `NotImplementedError`: HTTP 501
//! `NotImplemented`, reproduced verbatim.
//!
//! Contract: botocore's service-quotas model (26 operations) and
//! tests/compatibility/test_service_quotas_compat.py.

use crate::proto::{json_error, parse_json_body};
use crate::{Request, Response};
use serde_json::{json, Value};

// ---------------------------------------------------------------------------
// moto's static VPC default-quotas table
// ---------------------------------------------------------------------------

/// (`QuotaCode`, `QuotaName`, Adjustable, Value) — moto's
/// `VPC_DEFAULT_QUOTAS` verbatim. Every row shares `"ServiceCode": "vpc"`,
/// `"ServiceName": "Amazon Virtual Private Cloud (Amazon VPC)"`,
/// `"Unit": "None"`, `"GlobalQuota": false`, and a `eu-west-1` QuotaArn.
const VPC_DEFAULT_QUOTAS: &[(&str, &str, bool, f64)] = &[
    (
        "L-7E9ECCDB",
        "Active VPC peering connections per VPC",
        true,
        50.0,
    ),
    (
        "L-3248932A",
        "Characters per VPC endpoint policy",
        false,
        20480.0,
    ),
    (
        "L-45FE3B85",
        "Egress-only internet gateways per Region",
        true,
        5.0,
    ),
    ("L-1B52E74A", "Gateway VPC endpoints per Region", true, 20.0),
    ("L-83CA0A9D", "IPv4 CIDR blocks per VPC", true, 5.0),
    ("L-085A6257", "IPv6 CIDR blocks per VPC", true, 5.0),
    (
        "L-0EA8095F",
        "Inbound or outbound rules per security group",
        true,
        60.0,
    ),
    ("L-29B6F2EB", "Interface VPC endpoints per VPC", true, 50.0),
    ("L-A4707A72", "Internet gateways per Region", true, 5.0),
    (
        "L-FE5A380F",
        "NAT gateways per Availability Zone",
        true,
        5.0,
    ),
    ("L-B4A6D682", "Network ACLs per VPC", true, 200.0),
    ("L-BB24F6E5", "Network Address Usage", true, 64000.0),
    ("L-DF5E4CA3", "Network interfaces per Region", true, 5000.0),
    (
        "L-DC9F7029",
        "Outstanding VPC peering connection requests",
        true,
        25.0,
    ),
    ("L-2C462E13", "Participant accounts per VPC", true, 100.0),
    ("L-CD17FD4B", "Peered Network Address Usage", true, 128000.0),
    ("L-589F43AA", "Route tables per VPC", true, 200.0),
    ("L-93826ACB", "Routes per route table", true, 50.0),
    ("L-2AEEBF1A", "Rules per network ACL", true, 20.0),
    (
        "L-2AFB9258",
        "Security groups per network interface",
        true,
        5.0,
    ),
    ("L-407747CB", "Subnets per VPC", true, 200.0),
    (
        "L-44499CD2",
        "Subnets that can be shared with an account",
        true,
        100.0,
    ),
    (
        "L-8312C5BB",
        "VPC peering connection request expiry hours",
        false,
        168.0,
    ),
    ("L-E79EC296", "VPC security groups per Region", true, 2500.0),
    ("L-F678F1CE", "VPCs per Region", true, 5.0),
];

/// moto's quota wire shape (moto/servicequotas/models.py rows as served).
fn quota_record(code: &str, name: &str, adjustable: bool, value: f64) -> Value {
    json!({
        "Adjustable": adjustable,
        "GlobalQuota": false,
        "QuotaArn": format!("arn:aws:servicequotas:eu-west-1::vpc/{code}"),
        "QuotaCode": code,
        "QuotaName": name,
        "ServiceCode": "vpc",
        "ServiceName": "Amazon Virtual Private Cloud (Amazon VPC)",
        "Unit": "None",
        "Value": value,
    })
}

/// moto's `NoSuchResource` — raised for an unknown ServiceCode or the
/// `get_*` miss inside a known one (moto/servicequotas/exceptions.py).
fn not_found() -> Response {
    json_error(
        400,
        "NoSuchResourceException",
        "This service is not available in the current Region. Choose a different Region or a different service.",
    )
}

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    // The router's `ServiceQuotasV20190624` target map yields
    // `servicequotas`; the signing-scope alias resolves to `service-quotas`.
    if !matches!(req.service.as_str(), "servicequotas" | "service-quotas") {
        return None;
    }
    Some(route(req))
}

fn route(req: &Request) -> Response {
    let body = parse_json_body(&req.body);
    match req.operation.as_str() {
        "ListAWSDefaultServiceQuotas" => {
            // moto: `str(params.get("ServiceCode"))` — absent becomes "None",
            // which misses the table like any other unknown code. Pagination
            // is not implemented in moto; the full list always returns.
            let service_code = body
                .get("ServiceCode")
                .and_then(Value::as_str)
                .unwrap_or("None");
            if service_code != "vpc" {
                return not_found();
            }
            let quotas: Vec<Value> = VPC_DEFAULT_QUOTAS
                .iter()
                .map(|(code, name, adjustable, value)| {
                    quota_record(code, name, *adjustable, *value)
                })
                .collect();
            ok(json!({ "Quotas": quotas }))
        }
        "GetServiceQuota" => {
            let service_code = body
                .get("ServiceCode")
                .and_then(Value::as_str)
                .unwrap_or("None");
            let quota_code = body
                .get("QuotaCode")
                .and_then(Value::as_str)
                .unwrap_or("None");
            if service_code == "vpc" {
                if let Some((code, name, adjustable, value)) = VPC_DEFAULT_QUOTAS
                    .iter()
                    .find(|(code, ..)| *code == quota_code)
                {
                    return ok(json!({ "Quota": quota_record(code, name, *adjustable, *value) }));
                }
            }
            not_found()
        }
        other => json_error(
            501,
            "NotImplemented",
            &format!("The {other} action has not been implemented"),
        ),
    }
}

fn ok(body: Value) -> Response {
    Response::json(200, serde_json::to_vec(&body).unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn req(op: &str, body: &Value) -> Request {
        Request {
            service: "servicequotas".into(),
            operation: op.into(),
            method: "POST".into(),
            path: "/".into(),
            query_string: String::new(),
            headers: Default::default(),
            body: bytes::Bytes::from(serde_json::to_vec(body).unwrap()),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    #[test]
    fn list_vpc_default_quotas() {
        let resp = dispatch(&req(
            "ListAWSDefaultServiceQuotas",
            &json!({"ServiceCode": "vpc"}),
        ))
        .unwrap();
        assert_eq!(resp.status, 200);
        let v: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(v["Quotas"].as_array().unwrap().len(), 25);
        let first = &v["Quotas"][0];
        assert_eq!(first["QuotaCode"], "L-7E9ECCDB");
        assert_eq!(first["QuotaName"], "Active VPC peering connections per VPC");
        assert_eq!(
            first["QuotaArn"],
            "arn:aws:servicequotas:eu-west-1::vpc/L-7E9ECCDB"
        );
    }

    #[test]
    fn list_unknown_service_code() {
        let resp = dispatch(&req(
            "ListAWSDefaultServiceQuotas",
            &json!({"ServiceCode": "unknown-svc"}),
        ))
        .unwrap();
        assert_eq!(resp.status, 400);
        let v: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(v["__type"], "NoSuchResourceException");
    }

    #[test]
    fn get_vpc_quota() {
        let resp = dispatch(&req(
            "GetServiceQuota",
            &json!({"ServiceCode": "vpc", "QuotaCode": "L-7E9ECCDB"}),
        ))
        .unwrap();
        assert_eq!(resp.status, 200);
        let v: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(v["Quota"]["QuotaCode"], "L-7E9ECCDB");
        assert_eq!(v["Quota"]["Value"], 50.0);
    }

    #[test]
    fn get_unknown_quota_code() {
        let resp = dispatch(&req(
            "GetServiceQuota",
            &json!({"ServiceCode": "vpc", "QuotaCode": "L-UNKNOWN"}),
        ))
        .unwrap();
        assert_eq!(resp.status, 400);
        let v: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(v["__type"], "NoSuchResourceException");
    }

    #[test]
    fn unimplemented_actions_answer_motos_501() {
        // Every botocore op outside moto's two-handler surface: moto's
        // `call_action` raises NotImplementedError -> 501 NotImplemented.
        for op in ["ListServices", "ListServiceQuotas", "TagResource"] {
            let resp = dispatch(&req(op, &json!({}))).unwrap();
            assert_eq!(resp.status, 501);
            let v: Value = serde_json::from_slice(&resp.body).unwrap();
            assert_eq!(v["__type"], "NotImplemented");
            assert_eq!(
                v["message"],
                format!("The {op} action has not been implemented")
            );
        }
    }

    #[test]
    fn other_services_are_not_claimed() {
        let mut not_ours = req("GetServiceQuota", &json!({}));
        not_ours.service = "shield".into();
        assert!(dispatch(&not_ours).is_none());
    }
}
