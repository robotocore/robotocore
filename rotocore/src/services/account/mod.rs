//! AWS Account Management backend (REST-JSON, one URI per operation).
//!
//! Port of moto's `account` backend: alternate contacts, the main contact
//! information block, and the region opt-in registry. Wire shapes mirror
//! botocore's `account-2021-02-01` output shapes — note that moto's
//! serializer only emits members defined by the shape, so the dataclass's
//! `AccountType` never reaches the wire for GetAccountInformation. Every
//! moto error renders as HTTP 400 `{"__type": ..., "message": ...}`
//! (moto's `JsonRESTError`), keeping `AccessDeniedException` /
//! `ResourceNotFoundException` codes intact.
//!
//! State is per-account per-partition, exactly like moto's
//! `account_backends[account_id][partition]`: the region only selects the
//! partition, and an explicit `AccountId` parameter shards the state the
//! same way moto's BackendDict does.
//!
//! Moto surface boundary, kept faithful: `PutAccountName`,
//! `StartPrimaryEmailUpdate` and `AcceptPrimaryEmailUpdate` have no route
//! in moto's `account/urls.py` (checked against the deployed fork), so
//! botocore names for them answer the wire `NotImplemented` (501).

use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Value};

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

/// moto's `ALLOWED_CONTACT_TYPES`.
const ALLOWED_CONTACT_TYPES: &[&str] = &["SECURITY", "OPERATIONS", "BILLING"];

/// moto's `models._DEFAULT_REGIONS`: standard regions default ENABLED,
/// opt-in regions DISABLED, kept in moto's insertion order.
const DEFAULT_REGIONS: &[(&str, &str)] = &[
    ("us-east-1", "ENABLED"),
    ("us-east-2", "ENABLED"),
    ("us-west-1", "ENABLED"),
    ("us-west-2", "ENABLED"),
    ("ap-south-1", "ENABLED"),
    ("ap-northeast-1", "ENABLED"),
    ("ap-northeast-2", "ENABLED"),
    ("ap-northeast-3", "ENABLED"),
    ("ap-southeast-1", "ENABLED"),
    ("ap-southeast-2", "ENABLED"),
    ("ca-central-1", "ENABLED"),
    ("eu-central-1", "ENABLED"),
    ("eu-west-1", "ENABLED"),
    ("eu-west-2", "ENABLED"),
    ("eu-west-3", "ENABLED"),
    ("eu-north-1", "ENABLED"),
    ("sa-east-1", "ENABLED"),
    // Opt-in regions
    ("ap-east-1", "DISABLED"),
    ("ap-south-2", "DISABLED"),
    ("ap-southeast-3", "DISABLED"),
    ("ap-southeast-4", "DISABLED"),
    ("ca-west-1", "DISABLED"),
    ("eu-central-2", "DISABLED"),
    ("eu-south-1", "DISABLED"),
    ("eu-south-2", "DISABLED"),
    ("il-central-1", "DISABLED"),
    ("me-central-1", "DISABLED"),
    ("me-south-1", "DISABLED"),
    ("mx-central-1", "DISABLED"),
];

#[derive(Default)]
struct State {
    /// The account id this partition was first created for (moto echoes the
    /// backend's `account_id`, i.e. the requested one, in GetAccountInformation).
    account_id: String,
    alternate_contacts: Vec<AlternateContact>,
    /// `None` until `PutContactInformation` — moto serves a canned default
    /// (without `StateOrRegion`) before that.
    contact_information: Option<Value>,
    region_opt_status: Vec<(String, String)>,
}

struct AlternateContact {
    alternate_contact_type: String,
    title: String,
    name: String,
    email_address: String,
    phone_number: String,
}

impl AlternateContact {
    /// botocore's `AlternateContact` shape members, PascalCase.
    fn describe(&self) -> Value {
        json!({
            "AlternateContactType": self.alternate_contact_type,
            "Title": self.title,
            "Name": self.name,
            "EmailAddress": self.email_address,
            "PhoneNumber": self.phone_number,
        })
    }
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("account"));
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

/// moto's `UnknownContactType` — note the operation is hard-coded to
/// `GetAlternateContact` in moto's `_get_contact_type`, for every op.
fn unknown_contact_type(caller_arn: &str) -> Response {
    error(
        400,
        "AccessDeniedException",
        &format!(
            "User: {caller_arn} is not authorized to perform: account:GetAlternateContact \
             (You specified an invalid Alternate Contact type.)"
        ),
    )
}

/// moto's `UnspecifiedContactType`.
fn unspecified_contact_type() -> Response {
    error(
        400,
        "ResourceNotFoundException",
        "No contact of the inputted alternate contact type found.",
    )
}

/// moto's default STS caller arn (`sts/models.py::get_caller_identity`
/// fallback), which is what the error message embeds.
fn caller_arn(region: &str, account: &str) -> String {
    format!("arn:{}:sts::{}:user/moto", partition(region), account)
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

// ---------------------------------------------------------------------------
// Operation resolution: (method, requestUri) -> operation, per botocore's
// account-2021-02-01 model. moto only routes these twelve.
// ---------------------------------------------------------------------------

const RULES: &[(&str, &str, &str)] = &[
    ("POST", "deleteAlternateContact", "DeleteAlternateContact"),
    ("POST", "enableRegion", "EnableRegion"),
    ("POST", "disableRegion", "DisableRegion"),
    ("POST", "getAccountInformation", "GetAccountInformation"),
    ("POST", "getAlternateContact", "GetAlternateContact"),
    ("POST", "getContactInformation", "GetContactInformation"),
    (
        "POST",
        "getGovCloudAccountInformation",
        "GetGovCloudAccountInformation",
    ),
    ("POST", "getPrimaryEmail", "GetPrimaryEmail"),
    ("POST", "getRegionOptStatus", "GetRegionOptStatus"),
    ("POST", "listRegions", "ListRegions"),
    ("POST", "putAlternateContact", "PutAlternateContact"),
    ("POST", "putContactInformation", "PutContactInformation"),
];

fn wire_operation(req: &Request) -> Option<&'static str> {
    let path = req.path.trim_start_matches('/').trim_end_matches('/');
    RULES
        .iter()
        .find(|(method, uri, _)| *method == req.method && *uri == path)
        .map(|(_, _, op)| *op)
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "account" {
        return None;
    }
    let Some(op) = wire_operation(req) else {
        // botocore knows 15 operations; moto's urls.py routes 12. The
        // remaining three (PutAccountName, StartPrimaryEmailUpdate,
        // AcceptPrimaryEmailUpdate) — and any unknown path — answer the
        // generic 501, mirroring the moto-era probe status.
        let hint = if req.operation.is_empty() {
            req.path.trim_start_matches('/').trim_end_matches('/')
        } else {
            req.operation.as_str()
        };
        let e = json!({
            "__type": "NotImplemented",
            "message": format!("The {hint} action has not been implemented"),
        });
        return Some(Response::json(
            501,
            serde_json::to_vec(&e).unwrap_or_default(),
        ));
    };
    let body = crate::proto::parse_json_body(&req.body);
    let slot = state();
    let mut st = slot.write();

    // moto: `account_backends[AccountId param or current_account][partition]`.
    // The backend's account id string drives the echoed AccountId; the slot
    // key folds the region down to its partition, as moto's BackendDict does.
    let current = req.account.to_string();
    let requested = param_str(&body, "AccountId").unwrap_or(&current);
    let key_account = requested.parse::<u64>().unwrap_or(req.account);
    let key = (key_account, partition(&req.region).to_string());

    // The contact-type check runs before backend creation in moto.
    if matches!(
        op,
        "PutAlternateContact" | "GetAlternateContact" | "DeleteAlternateContact"
    ) && !ALLOWED_CONTACT_TYPES.contains(&param_str(&body, "AlternateContactType").unwrap_or(""))
    {
        return Some(unknown_contact_type(&caller_arn(&req.region, &current)));
    }

    let s = st.entry(key).or_insert_with(|| State {
        account_id: requested.to_string(),
        alternate_contacts: Vec::new(),
        contact_information: None,
        region_opt_status: DEFAULT_REGIONS
            .iter()
            .map(|(r, v)| ((*r).to_string(), (*v).to_string()))
            .collect(),
    });
    Some(handle(s, op, &body, req))
}

fn handle(st: &mut State, op: &str, p: &Value, req: &Request) -> Response {
    match op {
        // ---------------------------------------------------------------
        // Alternate contacts
        // ---------------------------------------------------------------
        "PutAlternateContact" => {
            let contact_type = param_str(p, "AlternateContactType").unwrap_or_default();
            if let Some(existing) = st
                .alternate_contacts
                .iter_mut()
                .find(|c| c.alternate_contact_type == contact_type)
            {
                existing.title = param_str(p, "Title").unwrap_or_default().to_string();
                existing.name = param_str(p, "Name").unwrap_or_default().to_string();
                existing.email_address =
                    param_str(p, "EmailAddress").unwrap_or_default().to_string();
                existing.phone_number = param_str(p, "PhoneNumber").unwrap_or_default().to_string();
            } else {
                st.alternate_contacts.push(AlternateContact {
                    alternate_contact_type: contact_type.to_string(),
                    title: param_str(p, "Title").unwrap_or_default().to_string(),
                    name: param_str(p, "Name").unwrap_or_default().to_string(),
                    email_address: param_str(p, "EmailAddress").unwrap_or_default().to_string(),
                    phone_number: param_str(p, "PhoneNumber").unwrap_or_default().to_string(),
                });
            }
            ok_empty()
        }
        "GetAlternateContact" => {
            let contact_type = param_str(p, "AlternateContactType").unwrap_or_default();
            match st
                .alternate_contacts
                .iter()
                .find(|c| c.alternate_contact_type == contact_type)
            {
                Some(contact) => ok(&json!({"AlternateContact": contact.describe()})),
                None => unspecified_contact_type(),
            }
        }
        "DeleteAlternateContact" => {
            let contact_type = param_str(p, "AlternateContactType").unwrap_or_default();
            st.alternate_contacts
                .retain(|c| c.alternate_contact_type != contact_type);
            ok_empty()
        }

        // ---------------------------------------------------------------
        // Account & contact information
        // ---------------------------------------------------------------
        "GetAccountInformation" => {
            let id = &st.account_id;
            ok(&json!({
                "AccountId": id,
                "AccountName": format!("Account {id}"),
            }))
        }
        "GetContactInformation" => {
            let ci = st
                .contact_information
                .clone()
                .unwrap_or_else(canned_contact_information);
            ok(&json!({"ContactInformation": ci}))
        }
        "PutContactInformation" => {
            let ci = p
                .get("ContactInformation")
                .cloned()
                .unwrap_or_else(Value::default);
            let get = |key: &str| {
                ci.get(key)
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string()
            };
            st.contact_information = Some(json!({
                "FullName": get("FullName"),
                "AddressLine1": get("AddressLine1"),
                "City": get("City"),
                "StateOrRegion": get("StateOrRegion"),
                "PostalCode": get("PostalCode"),
                "CountryCode": if ci.get("CountryCode").is_some() {
                    get("CountryCode")
                } else {
                    "US".to_string()
                },
                "PhoneNumber": get("PhoneNumber"),
            }));
            ok_empty()
        }
        "GetGovCloudAccountInformation" => {
            // moto: GovCloud pairing is not simulated; return empty.
            ok(&json!({}))
        }
        "GetPrimaryEmail" => {
            ok(&json!({"PrimaryEmail": format!("account-{}@example.com", st.account_id)}))
        }

        // ---------------------------------------------------------------
        // Region opt-in registry
        // ---------------------------------------------------------------
        "GetRegionOptStatus" => {
            let region_name = param_str(p, "RegionName").unwrap_or(&req.region);
            let opt_status = region_opt_status(st, region_name);
            ok(&json!({"RegionName": region_name, "RegionOptStatus": opt_status}))
        }
        "EnableRegion" => {
            set_region_opt_status(
                st,
                param_str(p, "RegionName").unwrap_or(&req.region),
                "ENABLED",
            );
            ok_empty()
        }
        "DisableRegion" => {
            set_region_opt_status(
                st,
                param_str(p, "RegionName").unwrap_or(&req.region),
                "DISABLED",
            );
            ok_empty()
        }
        "ListRegions" => {
            let filter = p
                .get("RegionOptStatusContains")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .map(|v| v.as_str().unwrap_or_default())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let regions: Vec<Value> = st
                .region_opt_status
                .iter()
                .filter(|(_, status)| filter.is_empty() || filter.contains(&status.as_str()))
                .map(|(name, status)| json!({"RegionName": name, "RegionOptStatus": status}))
                .collect();
            ok(&json!({"Regions": regions}))
        }
        _ => ok_empty(),
    }
}

/// moto defaults an unknown region to ENABLED.
fn region_opt_status(st: &State, region_name: &str) -> String {
    st.region_opt_status
        .iter()
        .find(|(name, _)| name == region_name)
        .map(|(_, status)| status.clone())
        .unwrap_or_else(|| "ENABLED".to_string())
}

/// moto's dict-assignment semantics: update in place (keeps order) or append.
fn set_region_opt_status(st: &mut State, region_name: &str, status: &str) {
    if let Some(entry) = st
        .region_opt_status
        .iter_mut()
        .find(|(name, _)| name == region_name)
    {
        entry.1 = status.to_string();
    } else {
        st.region_opt_status
            .push((region_name.to_string(), status.to_string()));
    }
}

/// moto's default (never-configured) contact information block — no
/// `StateOrRegion` member, country defaults US.
fn canned_contact_information() -> Value {
    json!({
        "FullName": "",
        "AddressLine1": "",
        "City": "",
        "PostalCode": "",
        "CountryCode": "US",
        "PhoneNumber": "",
    })
}

fn param_str<'a>(p: &'a Value, key: &str) -> Option<&'a str> {
    p.get(key).and_then(Value::as_str)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::atomic::{AtomicU64, Ordering};

    static ACCOUNTS: AtomicU64 = AtomicU64::new(240_000_000_001);

    /// Every test takes a fresh account so the shared store stays isolated.
    fn next_account() -> u64 {
        ACCOUNTS.fetch_add(1, Ordering::SeqCst)
    }

    fn send(account: u64, path: &str, body: Value) -> (u16, Value) {
        let request = Request {
            service: "account".to_string(),
            operation: String::new(),
            method: "POST".to_string(),
            path: path.to_string(),
            query_string: String::new(),
            headers: std::collections::HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            account,
            region: "us-east-1".to_string(),
        };
        let resp = dispatch(&request).expect("account dispatch");
        (
            resp.status,
            serde_json::from_slice(&resp.body).unwrap_or(json!({})),
        )
    }

    #[test]
    fn alternate_contact_lifecycle() {
        let account = next_account();

        let (status, body) = send(
            account,
            "/getAlternateContact",
            json!({"AlternateContactType": "BILLING"}),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ResourceNotFoundException");

        let (status, _) = send(
            account,
            "/putAlternateContact",
            json!({
                "AlternateContactType": "BILLING",
                "Name": "Test Test",
                "EmailAddress": "test@test.com",
                "PhoneNumber": "000000000000",
                "Title": "Dr. Test",
            }),
        );
        assert_eq!(status, 200);

        let (status, body) = send(
            account,
            "/getAlternateContact",
            json!({"AlternateContactType": "BILLING"}),
        );
        assert_eq!(status, 200);
        let contact = &body["AlternateContact"];
        assert_eq!(contact["AlternateContactType"], "BILLING");
        assert_eq!(contact["Name"], "Test Test");
        assert_eq!(contact["EmailAddress"], "test@test.com");
        assert_eq!(contact["Title"], "Dr. Test");

        let (status, _) = send(
            account,
            "/deleteAlternateContact",
            json!({"AlternateContactType": "BILLING"}),
        );
        assert_eq!(status, 200);

        let (status, body) = send(
            account,
            "/getAlternateContact",
            json!({"AlternateContactType": "BILLING"}),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn invalid_contact_type_is_access_denied() {
        let account = next_account();
        let (status, body) = send(
            account,
            "/getAlternateContact",
            json!({"AlternateContactType": "UNKNOWN"}),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "AccessDeniedException");
        assert_eq!(
            body["message"],
            format!(
                "User: arn:aws:sts::{account}:user/moto is not authorized to perform: \
                 account:GetAlternateContact (You specified an invalid Alternate Contact type.)"
            )
        );
    }

    #[test]
    fn explicit_account_id_shards_state() {
        let account = next_account();
        send(
            account,
            "/putAlternateContact",
            json!({
                "AccountId": "111111111111",
                "AlternateContactType": "SECURITY",
                "Name": "In 111111111111",
                "EmailAddress": "diff@test.com",
                "PhoneNumber": "000000000000",
                "Title": "X",
            }),
        );
        let (_, body) = send(
            account,
            "/getAlternateContact",
            json!({"AlternateContactType": "SECURITY"}),
        );
        assert_eq!(body["__type"], "ResourceNotFoundException");
        let (_, body) = send(
            account,
            "/getAlternateContact",
            json!({"AccountId": "111111111111", "AlternateContactType": "SECURITY"}),
        );
        assert_eq!(body["AlternateContact"]["Name"], "In 111111111111");
    }

    #[test]
    fn account_information_roundtrip() {
        let account = next_account();
        let (_, body) = send(account, "/getAccountInformation", json!({}));
        assert_eq!(body["AccountId"], account.to_string());
        assert_eq!(body["AccountName"], format!("Account {account}"));
    }

    #[test]
    fn contact_information_canned_default_then_put() {
        let account = next_account();
        let (_, body) = send(account, "/getContactInformation", json!({}));
        assert_eq!(body["ContactInformation"]["CountryCode"], "US");
        assert!(body["ContactInformation"].get("StateOrRegion").is_none());

        send(
            account,
            "/putContactInformation",
            json!({"ContactInformation": {
                "FullName": "Test User",
                "AddressLine1": "1 Main St",
                "City": "Testville",
                "StateOrRegion": "CA",
                "PostalCode": "12345",
                "CountryCode": "US",
                "PhoneNumber": "+15555555555",
            }}),
        );
        let (_, body) = send(account, "/getContactInformation", json!({}));
        assert_eq!(body["ContactInformation"]["FullName"], "Test User");
        assert_eq!(body["ContactInformation"]["City"], "Testville");
    }

    #[test]
    fn region_opt_status_flow() {
        let account = next_account();
        let (_, body) = send(
            account,
            "/getRegionOptStatus",
            json!({"RegionName": "ap-south-2"}),
        );
        assert_eq!(body["RegionOptStatus"], "DISABLED");

        let (_, body) = send(
            account,
            "/enableRegion",
            json!({"RegionName": "ap-south-2"}),
        );
        assert_eq!(body, json!({}));
        let (_, body) = send(
            account,
            "/getRegionOptStatus",
            json!({"RegionName": "ap-south-2"}),
        );
        assert_eq!(body["RegionOptStatus"], "ENABLED");

        let (_, body) = send(
            account,
            "/disableRegion",
            json!({"RegionName": "ap-south-2"}),
        );
        assert_eq!(body, json!({}));

        let (_, body) = send(
            account,
            "/listRegions",
            json!({"RegionOptStatusContains": ["DISABLED"]}),
        );
        let regions = body["Regions"].as_array().unwrap();
        assert!(!regions.is_empty());
        assert!(regions.iter().all(|r| r["RegionOptStatus"] == "DISABLED"));

        // Unknown regions default ENABLED (moto's dict get-with-default).
        let (_, body) = send(
            account,
            "/getRegionOptStatus",
            json!({"RegionName": "not-a-region"}),
        );
        assert_eq!(body["RegionOptStatus"], "ENABLED");
    }

    #[test]
    fn unmapped_operations_answer_501() {
        let account = next_account();
        for path in ["/putAccountName", "/startPrimaryEmailUpdate"] {
            let (status, body) = send(account, path, json!({}));
            assert_eq!(status, 501, "{path}");
            assert_eq!(body["__type"], "NotImplemented");
        }
    }

    #[test]
    fn state_is_partition_scoped_like_moto() {
        // moto keys account_backends[account][partition] — us-east-1 and
        // eu-west-1 share one backend for partition aws.
        let account = next_account();
        send(
            account,
            "/putAlternateContact",
            json!({
                "AlternateContactType": "OPERATIONS",
                "Name": "Global Ops",
                "EmailAddress": "ops@test.com",
                "PhoneNumber": "0",
                "Title": "T",
            }),
        );
        let request = Request {
            service: "account".to_string(),
            operation: String::new(),
            method: "POST".to_string(),
            path: "/getAlternateContact".to_string(),
            query_string: String::new(),
            headers: std::collections::HashMap::new(),
            body: Bytes::from(
                serde_json::to_vec(&json!({"AlternateContactType": "OPERATIONS"})).unwrap(),
            ),
            account,
            region: "eu-west-1".to_string(),
        };
        let resp = dispatch(&request).expect("account dispatch");
        assert_eq!(resp.status, 200);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["AlternateContact"]["Name"], "Global Ops");
    }
}
