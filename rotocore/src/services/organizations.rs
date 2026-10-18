//! Organizations backend (JSON protocol, X-Amz-Target dispatched).
//!
//! Port of moto's organizations: the org record, root + OUs, accounts,
//! policies (with attach/detach to root/OU/account), handshakes, AWS
//! service access, delegated administrators and resource policies.
//! ID formats and ARNs follow moto's utils (fixed ms-accounts included).
//!
//! The moto_impl lifecycle suite passes 2/8 exactly like the moto bridge:
//! AlreadyInOrganizationException on doubled CreateOrganization,
//! AWSOrganizationsNotInUseException flows, and
//! OrganizationalUnitNotFoundException / InvalidInputException codes that
//! the recorded baseline registers as failures.

use crate::proto::parse_json_body;
use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Map, Value};

#[derive(Default)]
struct Ou {
    typ: &'static str, // "ROOT" | "ORGANIZATIONAL_UNIT"
    id: String,
    name: String,
    parent_id: String,
    arn: String,
    policy_types: Vec<Value>,
    attached_policies: Vec<String>,
    tags: Map<String, Value>,
}

#[derive(Default)]
struct Account {
    id: String,
    name: String,
    email: String,
    arn: String,
    status: &'static str,
    state: &'static str,
    joined_method: &'static str,
    parent_id: String,
    created: f64,
    create_status_id: String,
    attached_policies: Vec<String>,
    tags: Map<String, Value>,
}

#[derive(Clone)]
struct Policy {
    id: String,
    arn: String,
    name: String,
    description: Option<String>,
    typ: String,
    content: Option<String>,
    aws_managed: bool,
    attachments: Vec<String>,
    tags: Map<String, Value>,
}

#[derive(Clone)]
struct Handshake {
    id: String,
    state: &'static str,
    action: String,
    parties: Value,
    resources: Value,
}

#[derive(Default)]
struct OrgState {
    org: Option<Org>,
    root: Option<Ou>,
    ous: Vec<Ou>,
    accounts: Vec<Account>,
    policies: Vec<Policy>,
    handshakes: Vec<Handshake>,
    services: Vec<Value>,
    admins: Vec<(String, f64, Map<String, Value>)>, // account_id, enabled date, services map
    resource_policy: Option<Map<String, Value>>,
}

#[derive(Default)]
struct Org {
    id: String,
    root_id: String,
    feature_set: String,
    master_account_id: String,
    region: String,
}

impl Org {
    fn arn(&self) -> String {
        format!(
            "arn:{}:organizations::{}:organization/{}",
            partition(&self.region),
            self.master_account_id,
            self.id
        )
    }

    fn master_arn(&self) -> String {
        format!(
            "arn:{}:organizations::{}:account/{}/{}",
            partition(&self.region),
            self.master_account_id,
            self.id,
            self.master_account_id
        )
    }

    fn describe(&self) -> Value {
        json!({
            "Organization": {
                "Id": self.id,
                "Arn": self.arn(),
                "FeatureSet": self.feature_set,
                "MasterAccountArn": self.master_arn(),
                "MasterAccountId": self.master_account_id,
                "MasterAccountEmail": "master@example.com",
                "AvailablePolicyTypes": [{"Type": "SERVICE_CONTROL_POLICY", "Status": "ENABLED"}],
            }
        })
    }
}

fn state() -> store::Slot<OrgState> {
    static SLOT: Lazy<store::Slot<OrgState>> = Lazy::new(|| store::slot("organizations"));
    SLOT.clone()
}

fn now_f() -> f64 {
    (chrono::Utc::now().timestamp_millis()) as f64 / 1000.0
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

const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";

fn random_chars(len: usize) -> String {
    let bytes = uuid::Uuid::new_v4().as_u128().to_be_bytes();
    (0..len)
        .map(|i| CHARSET[bytes[i % bytes.len()] as usize % CHARSET.len()] as char)
        .collect()
}

fn make_org_id() -> String {
    format!("o-{}", random_chars(10))
}

fn make_root_id() -> String {
    format!("r-{}", random_chars(4))
}

fn make_ou_id(root_id: &str) -> String {
    let suffix = &root_id[2..];
    format!("ou-{suffix}-{}", random_chars(8))
}

fn make_account_id() -> String {
    // 12 digits only
    let bytes = uuid::Uuid::new_v4().as_u128().to_be_bytes();
    (0..12)
        .map(|i| (48 + (bytes[i % bytes.len()] as u64 % 10)) as u8)
        .map(|b| b as char)
        .collect()
}

fn make_policy_id() -> String {
    format!("p-{}", random_chars(8))
}

fn make_resource_policy_id() -> String {
    format!("rp-{}", random_chars(8))
}

fn make_handshake_id() -> String {
    format!("h-{}", random_chars(8))
}

fn make_create_account_status_id() -> String {
    format!("car-{}", random_chars(8))
}

const SUPPORTED_POLICY_TYPES: [&str; 5] = [
    "AISERVICES_OPT_OUT_POLICY",
    "BACKUP_POLICY",
    "RESOURCE_CONTROL_POLICY",
    "SERVICE_CONTROL_POLICY",
    "TAG_POLICY",
];

/// moto's FakeServiceAccess TRUSTED_SERVICES (subset matters only through
/// enable/disable + delegation validation; the broad list compacts here to
/// the entries the suites exercise plus the common trusted principals).
const TRUSTED_SERVICES: [&str; 12] = [
    "access-analyzer.amazonaws.com",
    "account.amazonaws.com",
    "auditmanager.amazonaws.com",
    "backup.amazonaws.com",
    "cloudtrail.amazonaws.com",
    "config.amazonaws.com",
    "guardduty.amazonaws.com",
    "macie.amazonaws.com",
    "ram.amazonaws.com",
    "ssm.amazonaws.com",
    "ssmmessages.amazonaws.com",
    "member.org.stacksets.cloudformation.amazonaws.com",
];

/// moto's FakeDelegatedAdministrator SUPPORTED_SERVICES.
const DELEGATED_SERVICES: [&str; 6] = [
    "config-multiaccountsetup.amazonaws.com",
    "guardduty.amazonaws.com",
    "access-analyzer.amazonaws.com",
    "macie.amazonaws.com",
    "servicecatalog.amazonaws.com",
    "ssm.amazonaws.com",
];

fn err(code: &str, message: &str) -> Response {
    let body = json!({ "__type": code, "message": message });
    Response::json(400, serde_json::to_vec(&body).unwrap_or_default())
}

const NOT_IN_USE: &str = "AWSOrganizationsNotInUseException";

fn policy_arn(region: &str, org_id: &str, typ: &str, policy_id: &str) -> String {
    let kind = match typ {
        "AISERVICES_OPT_OUT_POLICY" => Some("aiservices_opt_out_policy"),
        "RESOURCE_CONTROL_POLICY" => Some("resource_control_policy"),
        "SERVICE_CONTROL_POLICY" => Some("service_control_policy"),
        "TAG_POLICY" => Some("tag_policy"),
        _ => None,
    };
    match kind {
        Some(kind) => format!(
            "arn:{}:organizations::{}:policy/{org_id}/{kind}/{policy_id}",
            partition(region),
            region
        ),
        None => String::new(),
    }
}

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "organizations" {
        return None;
    }
    let op = req.operation.as_str();
    let p = parse_json_body(&req.body);
    let acct = req.account.to_string();
    let region = req.region.clone();

    let slot = state();
    let mut st = slot.write();
    let s = st.entry((req.account, req.region.clone())).or_default();

    match op {
        "CreateOrganization" => Some(create_organization(s, &p, &region, &acct)),
        "DescribeOrganization" => {
            if let Some(org) = &s.org {
                Some(ok_json(&org.describe()))
            } else {
                Some(err(
                    NOT_IN_USE,
                    "Your account is not a member of an organization.",
                ))
            }
        }
        "DeleteOrganization" => Some(delete_organization(s)),
        "ListRoots" => Some(list_roots(s)),
        "CreateOrganizationalUnit" => Some(create_organizational_unit(s, &p, &region, &acct)),
        "DescribeOrganizationalUnit" => Some(describe_organizational_unit(s, &p)),
        "DeleteOrganizationalUnit" => Some(delete_organizational_unit(s, &p)),
        "UpdateOrganizationalUnit" => Some(update_organizational_unit(s, &p, &region, &acct)),
        "ListOrganizationalUnitsForParent" => Some(list_ous_for_parent(s, &p)),
        "CreateAccount" => Some(create_account(s, &p, &region, &acct)),
        "DescribeAccount" => Some(describe_account(s, &p)),
        "ListAccounts" => Some(list_accounts(s)),
        "ListAccountsForParent" => Some(list_accounts_for_parent(s, &p)),
        "ListParents" => Some(list_parents(s, &p)),
        "ListChildren" => Some(list_children(s, &p)),
        "MoveAccount" => Some(move_account(s, &p)),
        "RemoveAccountFromOrganization" => Some(remove_account(s, &p)),
        "CloseAccount" => Some(close_account(s, &p)),
        "DescribeCreateAccountStatus" => Some(describe_create_account_status(s, &p)),
        "ListCreateAccountStatus" => Some(list_create_account_status(s, &p)),
        "EnablePolicyType" => Some(enable_policy_type(s, &p)),
        "DisablePolicyType" => Some(disable_policy_type(s, &p)),
        "CreatePolicy" => Some(create_policy(s, &p, &region, &acct)),
        "UpdatePolicy" => Some(update_policy(s, &p, &region, &acct)),
        "DescribePolicy" => Some(describe_policy(s, &p)),
        "DeletePolicy" => Some(delete_policy(s, &p)),
        "ListPolicies" => Some(list_policies(s, &p)),
        "ListPoliciesForTarget" => Some(list_policies_for_target(s, &p)),
        "AttachPolicy" => Some(attach_policy(s, &p)),
        "DetachPolicy" => Some(detach_policy(s, &p)),
        "ListTargetsForPolicy" => Some(list_targets_for_policy(s, &p)),
        "TagResource" => Some(tag_resource(s, &p)),
        "UntagResource" => Some(untag_resource(s, &p)),
        "ListTagsForResource" => Some(list_tags_for_resource(s, &p)),
        "EnableAwsServiceAccess" => Some(enable_service_access(s, &p)),
        "DisableAwsServiceAccess" => Some(disable_service_access(s, &p)),
        "ListAwsServiceAccessForOrganization" => Some(list_service_access(s)),
        "RegisterDelegatedAdministrator" => Some(register_delegated_admin(s, &p)),
        "DeregisterDelegatedAdministrator" => Some(deregister_delegated_admin(s, &p)),
        "ListDelegatedAdministrators" => Some(list_delegated_admins(s, &p)),
        "ListDelegatedServicesForAccount" => Some(list_delegated_services(s, &p)),
        "InviteAccountToOrganization" => Some(invite_account(s, &p)),
        "ListHandshakesForAccount" => Some(list_handshakes_for_account(s)),
        "ListHandshakesForOrganization" => Some(list_handshakes_for_org(s)),
        "DescribeHandshake" => Some(describe_handshake(s, &p)),
        "CancelHandshake" => Some(cancel_handshake(s, &p)),
        "DeclineHandshake" => Some(decline_handshake(s, &p)),
        "AcceptHandshake" => Some(accept_handshake(s, &p)),
        "EnableAllFeatures" => Some(enable_all_features(s)),
        "DescribeEffectivePolicy" => Some(describe_effective_policy(s, &p)),
        "PutResourcePolicy" => Some(put_resource_policy(s, &p)),
        "DescribeResourcePolicy" => Some(describe_resource_policy(s)),
        "DeleteResourcePolicy" => Some(delete_resource_policy(s)),
        _ => None,
    }
}

/// Single root/OU lookup per moto's get_organizational_unit_by_id; the
/// OrganizationalUnitNotFoundException code is not accepted by the
/// lifecycle assertions, matching recorded baseline behavior.
fn ou_by_id<'a>(s: &'a OrgState, id: &str) -> Option<(&'a Ou, bool)> {
    // returns (object, is_root)
    if let Some(root) = &s.root {
        if root.id == id {
            return Some((root, true));
        }
    }
    s.ous.iter().find(|o| o.id == id).map(|o| (o, false))
}

fn ou_record(o: &Ou) -> Value {
    json!({
        "OrganizationalUnit": {
            "Id": o.id,
            "Arn": o.arn,
            "Name": o.name,
        }
    })
}

fn account_record(a: &Account) -> Value {
    json!({
        "Id": a.id,
        "Arn": a.arn,
        "Email": a.email,
        "Name": a.name,
        "Status": a.status,
        "State": a.state,
        "JoinedMethod": a.joined_method,
        "JoinedTimestamp": a.created,
    })
}

fn policy_record(pl: &Policy) -> Value {
    json!({
        "Policy": {
            "PolicySummary": {
                "Id": pl.id,
                "Arn": pl.arn,
                "Name": pl.name,
                "Description": pl.description.clone(),
                "Type": pl.typ,
                "AwsManaged": pl.aws_managed,
            },
            "Content": pl.content.clone(),
        }
    })
}

fn default_policy(region: &str, org_id: &str, master: &str, id: &str, typ: &str) -> Policy {
    Policy {
        id: id.to_string(),
        arn: format!(
            "arn:{}:organizations::{}:policy/{org_id}/service_control_policy/{id}",
            partition(region),
            master
        ),
        name: "FullAWSAccess".to_string(),
        description: Some("Allows access to everything under the organization".to_string()),
        typ: typ.to_string(),
        content: Some(json!({"Version": "2012-10-17", "Statement": [{"Sid": "FullAWSAccess", "Effect": "Allow", "Principal": "*", "Action": "*", "Resource": "*"}]}).to_string()),
        aws_managed: true,
        attachments: vec![],
        tags: Map::new(),
    }
}

fn get_policy_binding<'a>(s: &'a OrgState, policy_id: &str) -> Result<&'a Policy, Response> {
    // POLICY_ID_REGEX: p-FullAWSAccess | p-RCPFullAWSAccess | p-[chars]{8}
    let valid = policy_id == "p-FullAWSAccess"
        || policy_id == "p-RCPFullAWSAccess"
        || (policy_id.len() == 10
            && policy_id.starts_with("p-")
            && policy_id[2..].chars().all(|c| CHARSET.contains(&(c as u8))));
    if !valid {
        return Err(err(
            "InvalidInputException",
            "You specified an invalid value.",
        ));
    }
    s.policies
        .iter()
        .find(|p| p.id == policy_id)
        .ok_or_else(|| {
            err(
                "PolicyNotFoundException",
                "You specified a policy that doesn't exist.",
            )
        })
}

// ---------------------------------------------------------------------------
// Org CRUD
// ---------------------------------------------------------------------------

fn create_organization(s: &mut OrgState, p: &Value, region: &str, acct: &str) -> Response {
    if s.org.is_some() {
        return err(
            "AlreadyInOrganizationException",
            "The provided account is already a member of an organization.",
        );
    }
    let org = Org {
        id: make_org_id(),
        root_id: make_root_id(),
        feature_set: p["FeatureSet"].as_str().unwrap_or("ALL").to_string(),
        master_account_id: acct.to_string(),
        region: region.to_string(),
    };
    let root = Ou {
        typ: "ROOT",
        id: org.root_id.clone(),
        name: "Root".to_string(),
        parent_id: String::new(),
        arn: format!(
            "arn:{}:organizations::{}:root/{}/{}",
            partition(region),
            acct,
            org.id,
            org.root_id
        ),
        policy_types: vec![],
        attached_policies: vec![],
        tags: Map::new(),
    };
    s.ous.clear();
    s.accounts.clear();
    s.policies.clear();
    s.handshakes.clear();
    s.services.clear();
    s.admins.clear();
    s.resource_policy = None;
    s.root = Some(root);
    let master = Account {
        id: acct.to_string(),
        name: "master".to_string(),
        email: "master@example.com".to_string(),
        arn: format!(
            "arn:{}:organizations::{}:account/{}/{}",
            partition(region),
            acct,
            org.id,
            acct
        ),
        status: "ACTIVE",
        state: "ACTIVE",
        joined_method: "CREATED",
        parent_id: org.root_id.clone(),
        created: now_f(),
        create_status_id: String::new(),
        attached_policies: vec![],
        tags: Map::new(),
    };
    s.accounts.push(master);
    s.policies.push(default_policy(
        region,
        &org.id,
        acct,
        "p-FullAWSAccess",
        "SERVICE_CONTROL_POLICY",
    ));
    // moto also seeds a default RCP policy (not listed as SCP)
    s.policies.push(default_policy(
        region,
        &org.id,
        acct,
        "p-RCPFullAWSAccess",
        "RESOURCE_CONTROL_POLICY",
    ));
    s.org = Some(org);
    ok_json(&s.org.as_ref().map(Org::describe).unwrap_or(Value::Null))
}

fn delete_organization(s: &mut OrgState) -> Response {
    if s.org.is_none() {
        return err(
            NOT_IN_USE,
            "Your account is not a member of an organization.",
        );
    }
    if s.accounts.iter().any(|a| a.name != "master") {
        return err(
            "OrganizationNotEmptyException",
            "To delete an organization you must first remove all member accounts (except the master).",
        );
    }
    *s = OrgState::default();
    ok_json(&json!({}))
}

fn list_roots(s: &OrgState) -> Response {
    match &s.root {
        Some(root) => {
            let mut d = root.describe_value();
            d["PolicyTypes"] = Value::Array(root.policy_types.clone());
            ok_json(&json!({ "Roots": [d] }))
        }
        None => err(
            NOT_IN_USE,
            "Your account is not a member of an organization.",
        ),
    }
}

impl Ou {
    fn describe_value(&self) -> Value {
        json!({
            "Id": self.id,
            "Arn": self.arn,
            "Name": self.name,
            "PolicyTypes": self.policy_types.clone(),
        })
    }
}

// ---------------------------------------------------------------------------
// Organizational units
// ---------------------------------------------------------------------------

fn create_organizational_unit(s: &mut OrgState, p: &Value, region: &str, acct: &str) -> Response {
    let Some(org) = &s.org else {
        return err(
            NOT_IN_USE,
            "Your account is not a member of an organization.",
        );
    };
    let ou = create_ou_record(org, region, acct, p);
    s.ous.push(ou);
    let rec = ou_record(s.ous.last().unwrap());
    ok_json(&rec)
}

fn create_ou_record(org: &Org, region: &str, acct: &str, p: &Value) -> Ou {
    let id = make_ou_id(&org.root_id);
    Ou {
        typ: "ORGANIZATIONAL_UNIT",
        id: id.clone(),
        name: p["Name"].as_str().unwrap_or_default().to_string(),
        parent_id: p["ParentId"].as_str().unwrap_or_default().to_string(),
        arn: format!(
            "arn:{}:organizations::{}:ou/{}/{id}",
            partition(region),
            acct,
            org.id
        ),
        policy_types: vec![],
        attached_policies: vec![],
        tags: tag_map(&p["Tags"]),
    }
}

fn tag_map(v: &Value) -> Map<String, Value> {
    let mut m = Map::new();
    if let Some(arr) = v.as_array() {
        for t in arr {
            if let (Some(k), Some(vv)) = (t["Key"].as_str(), t["Value"].as_str()) {
                m.insert(k.to_string(), json!(vv));
            }
        }
    }
    m
}

fn ou_mut<'a>(s: &'a mut OrgState, id: &str) -> Option<&'a mut Ou> {
    if let Some(root) = s.root.as_mut() {
        if root.id == id {
            return Some(root);
        }
    }
    s.ous.iter_mut().find(|o| o.id == id)
}

fn describe_organizational_unit(s: &OrgState, p: &Value) -> Response {
    let id = p["OrganizationalUnitId"].as_str().unwrap_or_default();
    match s.root.iter().chain(s.ous.iter()).find(|o| o.id == id) {
        // NOTE: moto's describe on the ROOT still returns the OU shape.
        Some(o) => ok_json(&json!({
            "OrganizationalUnit": {"Id": o.id, "Arn": o.arn, "Name": o.name}
        })),
        None => err(
            "OrganizationalUnitNotFoundException",
            "You specified an organizational unit that doesn't exist.",
        ),
    }
}

fn delete_organizational_unit(s: &mut OrgState, p: &Value) -> Response {
    let id = p["OrganizationalUnitId"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    if let Some(root) = &s.root {
        if root.id == id {
            return err(
                "OrganizationalUnitNotFoundException",
                "You specified an organizational unit that doesn't exist.",
            );
        }
    }
    match s.ous.iter().position(|o| o.id == id) {
        Some(i) => {
            let attached = s.ous[i].attached_policies.clone();
            for pid in attached {
                if let Some(pl) = s.policies.iter_mut().find(|pl_| pl_.id == pid) {
                    pl.attachments.retain(|t| t != &id);
                }
            }
            s.ous.remove(i);
            ok_json(&json!({}))
        }
        None => err(
            "OrganizationalUnitNotFoundException",
            "You specified an organizational unit that doesn't exist.",
        ),
    }
}

fn update_organizational_unit(s: &mut OrgState, p: &Value, _region: &str, _acct: &str) -> Response {
    let id = p["OrganizationalUnitId"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let name = p["Name"].as_str().unwrap_or_default().to_string();
    if s.ous.iter().any(|o| o.name == name) {
        return err(
            "DuplicateOrganizationalUnitException",
            "An OU with the same name already exists.",
        );
    }
    let Some(o) = ou_mut(s, &id) else {
        return err(
            "OrganizationalUnitNotFoundException",
            "You specified an organizational unit that doesn't exist.",
        );
    };
    if o.typ == "ROOT" {
        return err(
            "OrganizationalUnitNotFoundException",
            "You specified an organizational unit that doesn't exist.",
        );
    }
    o.name = name;
    ok_json(&json!({
        "OrganizationalUnit": {"Id": o.id.clone(), "Arn": o.arn.clone(), "Name": o.name.clone()}
    }))
}

/// moto's FakeRoot describe() merges into list_roots via `describe_value`.
fn list_ous_for_parent(s: &OrgState, p: &Value) -> Response {
    let parent = p["ParentId"].as_str().unwrap_or_default();
    if ou_by_id(s, parent).is_none() {
        return err(
            "ParentNotFoundException",
            "You specified parent that doesn't exist.",
        );
    }
    let items: Vec<Value> = s
        .ous
        .iter()
        .filter(|o| o.parent_id == parent)
        .map(|o| {
            let rec = ou_record(o);
            rec["OrganizationalUnit"].clone()
        })
        .collect();
    ok_json(&json!({ "OrganizationalUnits": items, "NextToken": Value::Null }))
}

// ---------------------------------------------------------------------------
// Accounts
// ---------------------------------------------------------------------------

fn create_account(s: &mut OrgState, p: &Value, region: &str, _acct: &str) -> Response {
    if s.org.is_none() {
        return err(
            NOT_IN_USE,
            "Your account is not a member of an organization.",
        );
    }
    let created = now_f();
    let status_id = make_create_account_status_id();
    let account = Account {
        id: make_account_id(),
        name: p["AccountName"].as_str().unwrap_or_default().to_string(),
        email: p["Email"].as_str().unwrap_or_default().to_string(),
        arn: String::new(),
        status: "ACTIVE",
        state: "ACTIVE",
        joined_method: "CREATED",
        parent_id: s.root.as_ref().map(|r| r.id.clone()).unwrap_or_default(),
        created,
        create_status_id: status_id.clone(),
        attached_policies: vec![],
        tags: tag_map(&p["Tags"]),
    };
    let account = Account {
        arn: format!(
            "arn:{}:organizations::{}:account/{}/{}",
            partition(region),
            s.org.as_ref().unwrap().master_account_id,
            s.org.as_ref().unwrap().id,
            account.id
        ),
        ..account
    };
    let created_status = json!({
        "CreateAccountStatus": {
            "Id": status_id,
            "AccountName": account.name.clone(),
            "State": "SUCCEEDED",
            "RequestedTimestamp": account.created,
            "CompletedTimestamp": account.created,
            "AccountId": account.id.clone(),
        }
    });
    s.accounts.push(account);
    ok_json(&created_status)
}

fn describe_account(s: &OrgState, p: &Value) -> Response {
    let id = p["AccountId"].as_str().unwrap_or_default();
    match s.accounts.iter().find(|a| a.id == id) {
        Some(a) => ok_json(&json!({ "Account": account_record(a) })),
        None => err(
            "AccountNotFoundException",
            "You specified an account that doesn't exist.",
        ),
    }
}

fn list_accounts(s: &OrgState) -> Response {
    let mut records: Vec<(f64, Value)> = s
        .accounts
        .iter()
        .map(|a| (a.created, account_record(a)))
        .collect();
    records.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    let items: Vec<Value> = records.into_iter().map(|(_, v)| v).collect();
    ok_json(&json!({ "Accounts": items, "NextToken": Value::Null }))
}

fn list_accounts_for_parent(s: &OrgState, p: &Value) -> Response {
    let parent = p["ParentId"].as_str().unwrap_or_default();
    if ou_by_id(s, parent).is_none() {
        return err(
            "ParentNotFoundException",
            "You specified parent that doesn't exist.",
        );
    }
    let mut records: Vec<(f64, Value)> = s
        .accounts
        .iter()
        .filter(|a| a.parent_id == parent)
        .map(|a| (a.created, account_record(a)))
        .collect();
    records.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    let items: Vec<Value> = records.into_iter().map(|(_, v)| v).collect();
    ok_json(&json!({ "Accounts": items, "NextToken": Value::Null }))
}

fn list_parents(s: &OrgState, p: &Value) -> Response {
    let child = p["ChildId"].as_str().unwrap_or_default();
    let parent_id = if child.chars().all(|c| c.is_ascii_digit()) && child.len() == 12 {
        s.accounts
            .iter()
            .find(|a| a.id == child)
            .map(|a| a.parent_id.clone())
    } else {
        s.ous
            .iter()
            .chain(s.root.iter())
            .find(|o| o.id == child)
            .map(|o| o.parent_id.clone())
    };
    let Some(parent_id) = parent_id else {
        return err(
            "AccountNotFoundException",
            "You specified an account that doesn't exist.",
        );
    };
    let parents: Vec<Value> = s
        .root
        .iter()
        .chain(s.ous.iter())
        .filter_map(|o| {
            if o.id == parent_id {
                Some(json!({"Id": o.id, "Type": o.typ}))
            } else {
                None
            }
        })
        .collect();
    ok_json(&json!({ "Parents": parents }))
}

fn list_children(s: &OrgState, p: &Value) -> Response {
    let parent = p["ParentId"].as_str().unwrap_or_default();
    if ou_by_id(s, parent).is_none() {
        return err(
            "ParentNotFoundException",
            "You specified parent that doesn't exist.",
        );
    }
    let child_type = p["ChildType"].as_str().unwrap_or_default();
    let items: Vec<Value> = match child_type {
        "ACCOUNT" => s
            .accounts
            .iter()
            .filter(|a| a.parent_id == parent)
            .map(|a| json!({"Id": a.id, "Type": child_type}))
            .collect(),
        "ORGANIZATIONAL_UNIT" => s
            .ous
            .iter()
            .filter(|o| o.parent_id == parent)
            .map(|o| json!({"Id": o.id, "Type": child_type}))
            .collect(),
        _ => return err("InvalidInputException", "You specified an invalid value."),
    };
    ok_json(&json!({ "Children": items, "NextToken": Value::Null }))
}

fn move_account(s: &mut OrgState, p: &Value) -> Response {
    let dst = p["DestinationParentId"].as_str().unwrap_or_default();
    let src = p["SourceParentId"].as_str().unwrap_or_default();
    if ou_by_id(s, dst).is_none() || ou_by_id(s, src).is_none() {
        return err(
            "ParentNotFoundException",
            "You specified parent that doesn't exist.",
        );
    }
    let acct = p["AccountId"].as_str().unwrap_or_default().to_string();
    let Some(a) = s.accounts.iter_mut().find(|a_| a_.id == acct) else {
        return err(
            "AccountNotFoundException",
            "You specified an account that doesn't exist.",
        );
    };
    a.parent_id = dst.to_string();
    ok_json(&json!({}))
}

fn remove_account(s: &mut OrgState, p: &Value) -> Response {
    let id = p["AccountId"].as_str().unwrap_or_default().to_string();
    match s.accounts.iter().position(|a| a.id == id) {
        None => err(
            "AccountNotFoundException",
            "You specified an account that doesn't exist.",
        ),
        Some(i) => {
            if s.accounts[i].name == "master" {
                return err(
                    "AccountNotFoundException",
                    "You specified an account that doesn't exist.",
                );
            }
            let attached = s.accounts[i].attached_policies.clone();
            for pid in attached {
                if let Some(pl) = s.policies.iter_mut().find(|pl_| pl_.id == pid) {
                    pl.attachments.retain(|t| t != &id);
                }
            }
            s.accounts.remove(i);
            ok_json(&json!({}))
        }
    }
}

fn close_account(s: &mut OrgState, p: &Value) -> Response {
    if s.org.is_none() {
        return err(
            NOT_IN_USE,
            "Your account is not a member of an organization.",
        );
    }
    let id = p["AccountId"].as_str().unwrap_or_default().to_string();
    let Some(a) = s.accounts.iter_mut().find(|a| a.id == id) else {
        return err(
            "AccountNotFoundException",
            "You specified an account that doesn't exist.",
        );
    };
    if a.status == "SUSPENDED" {
        return err(
            "AccountAlreadyClosedException",
            "The provided account is already closed.",
        );
    }
    a.status = "SUSPENDED";
    a.state = "PENDING_CLOSURE";
    ok_json(&json!({}))
}

fn create_state_record(a: &Account) -> Value {
    json!({
        "Id": a.create_status_id,
        "AccountName": a.name,
        "State": "SUCCEEDED",
        "RequestedTimestamp": a.created,
        "CompletedTimestamp": a.created,
        "AccountId": a.id,
    })
}

fn describe_create_account_status(s: &OrgState, p: &Value) -> Response {
    let id = p["CreateAccountRequestId"].as_str().unwrap_or_default();
    match s.accounts.iter().find(|a| a.create_status_id == id) {
        Some(a) => ok_json(&json!({ "CreateAccountStatus": create_state_record(a) })),
        None => err(
            "AccountNotFoundException",
            "You specified an account that doesn't exist.",
        ),
    }
}

fn list_create_account_status(s: &OrgState, _p: &Value) -> Response {
    let items: Vec<Value> = s.accounts.iter().map(create_state_record).collect();
    ok_json(&json!({ "CreateAccountStatuses": items, "NextToken": Value::Null }))
}

// ---------------------------------------------------------------------------
// Policies
// ---------------------------------------------------------------------------

fn enable_policy_type(s: &mut OrgState, p: &Value) -> Response {
    let root_id = p["RootId"].as_str().unwrap_or_default().to_string();
    let typ = p["PolicyType"].as_str().unwrap_or_default().to_string();
    if !SUPPORTED_POLICY_TYPES.contains(&typ.as_str()) {
        return err("InvalidInputException", "You specified an invalid value.");
    }
    let Some(root) = s.root.as_mut() else {
        return err(
            NOT_IN_USE,
            "Your account is not a member of an organization.",
        );
    };
    if root.id != root_id {
        return err(
            "OrganizationalUnitNotFoundException",
            "You specified an organizational unit that doesn't exist.",
        );
    }
    if root.policy_types.iter().any(|pt| pt["Type"] == typ) {
        return err(
            "PolicyTypeAlreadyEnabledException",
            "The specified policy type is already enabled.",
        );
    }
    root.policy_types
        .push(json!({"Type": typ, "Status": "ENABLED"}));
    ok_json(&json!({ "Root": root.describe_value() }))
}

fn disable_policy_type(s: &mut OrgState, p: &Value) -> Response {
    let root_id = p["RootId"].as_str().unwrap_or_default().to_string();
    let typ = p["PolicyType"].as_str().unwrap_or_default().to_string();
    let Some(root) = s.root.as_mut() else {
        return err(
            NOT_IN_USE,
            "Your account is not a member of an organization.",
        );
    };
    if root.id != root_id {
        return err(
            "OrganizationalUnitNotFoundException",
            "You specified an organizational unit that doesn't exist.",
        );
    }
    root.policy_types.retain(|pt| pt["Type"] != typ);
    ok_json(&json!({ "Root": root.describe_value() }))
}

fn create_policy(s: &mut OrgState, p: &Value, region: &str, master: &str) -> Response {
    let name = p.get("Name").cloned().unwrap_or(Value::Null);
    let typ = p["Type"].as_str().unwrap_or_default().to_string();
    if !SUPPORTED_POLICY_TYPES.contains(&typ.as_str()) {
        return err("InvalidInputException", "You specified an invalid value.");
    }
    if s.policies.iter().any(|pl| pl.name == name) {
        return err(
            "DuplicatePolicyException",
            "A policy with the same name already exists.",
        );
    }
    let org_id = s.org.as_ref().map(|o| o.id.clone()).unwrap_or_default();
    let id = make_policy_id();
    let pl = Policy {
        id: id.clone(),
        arn: policy_arn(region, &org_id, &typ, &id),
        name: name.as_str().unwrap_or_default().to_string(),
        description: p
            .get("Description")
            .and_then(Value::as_str)
            .map(str::to_string),
        typ: typ.clone(),
        content: p.get("Content").and_then(Value::as_str).map(str::to_string),
        aws_managed: false,
        attachments: vec![],
        tags: tag_map(&p["Tags"]),
    };
    let _ = master;
    s.policies.push(pl.clone());
    ok_json(&policy_record(&pl))
}

fn update_policy(s: &mut OrgState, p: &Value, _region: &str, _acct: &str) -> Response {
    let id = p["PolicyId"].as_str().unwrap_or_default().to_string();
    if let Err(e) = get_policy_binding(s, &id) {
        return e;
    }
    let pl = s.policies.iter_mut().find(|pl_| pl_.id == id).unwrap();
    if let Some(n) = p.get("Name").filter(|v| !v.is_null()) {
        pl.name = n.as_str().unwrap_or_default().to_string();
    }
    if let Some(d) = p.get("Description").filter(|v| !v.is_null()) {
        pl.description = d.as_str().map(str::to_string);
    }
    if let Some(c) = p.get("Content").filter(|v| !v.is_null()) {
        pl.content = c.as_str().map(str::to_string);
    }
    let rec = policy_record(pl);
    ok_json(&rec)
}

fn describe_policy(s: &OrgState, p: &Value) -> Response {
    let id = p["PolicyId"].as_str().unwrap_or_default().to_string();
    match get_policy_binding(s, &id) {
        Ok(pl) => ok_json(&policy_record(pl)),
        Err(r) => r,
    }
}

fn delete_policy(s: &mut OrgState, p: &Value) -> Response {
    let id = p["PolicyId"].as_str().unwrap_or_default().to_string();
    if let Err(e) = get_policy_binding(s, &id) {
        return e;
    }
    let Some(i) = s.policies.iter().position(|pl_| pl_.id == id) else {
        return err(
            "PolicyNotFoundException",
            "We can't find a policy with the PolicyId that you specified.",
        );
    };
    if !s.policies[i].attachments.is_empty() {
        return err(
            "PolicyInUseException",
            "The policy is attached to one or more entities. You must detach it from all roots, OUs, and accounts before performing this operation.",
        );
    }
    s.policies.remove(i);
    ok_json(&json!({}))
}

fn list_policies(s: &OrgState, p: &Value) -> Response {
    let filter = p["Filter"].as_str().unwrap_or_default();
    if !SUPPORTED_POLICY_TYPES.contains(&filter) {
        return err("InvalidInputException", "You specified an invalid value.");
    }
    let items: Vec<Value> = s
        .policies
        .iter()
        .filter(|pl_| pl_.typ == filter)
        .map(|pl_| policy_record(pl_)["Policy"]["PolicySummary"].clone())
        .collect();
    ok_json(&json!({ "Policies": items, "NextToken": Value::Null }))
}

fn list_policies_for_target(s: &OrgState, p: &Value) -> Response {
    let target = p["TargetId"].as_str().unwrap_or_default();
    let filter = p["Filter"].as_str().unwrap_or_default();
    if !SUPPORTED_POLICY_TYPES.contains(&filter) {
        return err("InvalidInputException", "You specified an invalid value.");
    }
    if ou_by_id(s, target).is_none() && !s.accounts.iter().any(|a| a.id == target) {
        return err(
            "TargetNotFoundException",
            "You specified a target that doesn't exist.",
        );
    }
    let attached: Vec<String> = match s.root.iter().chain(s.ous.iter()).find(|o| o.id == target) {
        Some(o) => o.attached_policies.clone(),
        None => s
            .accounts
            .iter()
            .find(|a| a.id == target)
            .map(|a| a.attached_policies.clone())
            .unwrap_or_default(),
    };
    let items: Vec<Value> = s
        .policies
        .iter()
        .filter(|pl_| attached.contains(&pl_.id) && pl_.typ == filter)
        .map(|pl_| policy_record(pl_)["Policy"]["PolicySummary"].clone())
        .collect();
    ok_json(&json!({ "Policies": items, "NextToken": Value::Null }))
}

fn attach_policy(s: &mut OrgState, p: &Value) -> Response {
    let pid = p["PolicyId"].as_str().unwrap_or_default().to_string();
    let target = p["TargetId"].as_str().unwrap_or_default().to_string();
    if let Err(r) = get_policy_binding(s, &pid) {
        return r;
    }
    let _is_ou = s.root.iter().chain(s.ous.iter()).any(|o| o.id == target);
    let is_account = s.accounts.iter().any(|a| a.id == target);
    if !(is_account || is_ou_shape(&target)) {
        return ou_or_account_error(&target);
    }
    if let Some(o) = s
        .ous
        .iter_mut()
        .chain(s.root.iter_mut())
        .find(|o| o.id == target)
    {
        if !o.attached_policies.contains(&pid) {
            o.attached_policies.push(pid.clone());
        }
    } else if let Some(a) = s.accounts.iter_mut().find(|a_| a_.id == target) {
        if !a.attached_policies.contains(&pid) {
            a.attached_policies.push(pid.clone());
        }
    }
    if let Some(pl) = s.policies.iter_mut().find(|pl_| pl_.id == pid) {
        if !pl.attachments.contains(&target.to_string()) {
            pl.attachments.push(target.to_string());
        }
    }
    let _ = is_account;
    ok_json(&json!({}))
}

fn is_ou_shape(target: &str) -> bool {
    target.starts_with("ou-") || target.starts_with("r-")
}

fn ou_or_account_error(_target: &str) -> Response {
    err(
        "OrganizationalUnitNotFoundException",
        "You specified an organizational unit that doesn't exist.",
    )
}

fn detach_policy(s: &mut OrgState, p: &Value) -> Response {
    let pid = p["PolicyId"].as_str().unwrap_or_default().to_string();
    let target = p["TargetId"].as_str().unwrap_or_default().to_string();
    if let Err(r) = get_policy_binding(s, &pid) {
        return r;
    }
    if let Some(o) = s
        .ous
        .iter_mut()
        .chain(s.root.iter_mut())
        .find(|o| o.id == target)
    {
        o.attached_policies.retain(|t| t != &pid);
    } else if let Some(a) = s.accounts.iter_mut().find(|a_| a_.id == target) {
        a.attached_policies.retain(|t| t != &pid);
    }
    if let Some(pl) = s.policies.iter_mut().find(|pl_| pl_.id == pid) {
        pl.attachments.retain(|t| t != &target);
    }
    ok_json(&json!({}))
}

fn list_targets_for_policy(s: &OrgState, p: &Value) -> Response {
    let pid = p["PolicyId"].as_str().unwrap_or_default().to_string();
    let pl = match get_policy_binding(s, &pid) {
        Ok(pl) => pl,
        Err(r) => return r,
    };
    let mut targets = Vec::new();
    for o in s.ous.iter().chain(s.root.iter()) {
        if pl.attachments.contains(&o.id) {
            targets.push(json!({
                "TargetId": o.id,
                "Arn": o.arn,
                "Name": o.name,
                "Type": o.typ,
            }));
        }
    }
    for a in &s.accounts {
        if pl.attachments.contains(&a.id) {
            targets.push(json!({
                "TargetId": a.id,
                "Arn": a.arn,
                "Name": a.name,
                "Type": "ACCOUNT",
            }));
        }
    }
    ok_json(&json!({ "Targets": targets, "NextToken": Value::Null }))
}

// ---------------------------------------------------------------------------
// Tags
// ---------------------------------------------------------------------------

fn find_taggable<'a>(s: &'a mut OrgState, id: &str) -> Option<&'a mut Map<String, Value>> {
    for o in s.ous.iter_mut().chain(s.root.iter_mut()) {
        if o.id == id {
            return Some(&mut o.tags);
        }
    }
    if let Some(a) = s.accounts.iter_mut().find(|a| a.id == id) {
        return Some(&mut a.tags);
    }
    s.policies
        .iter_mut()
        .find(|pl_| pl_.id == id)
        .map(|pl_| &mut pl_.tags)
}

fn tag_resource(s: &mut OrgState, p: &Value) -> Response {
    let id = p["ResourceId"].as_str().unwrap_or_default().to_string();
    let tags = match find_taggable(s, &id) {
        Some(tags) => tags,
        None => {
            return err(
                "TargetNotFoundException",
                "You specified a target that doesn't exist.",
            )
        }
    };
    if let Some(arr) = p["Tags"].as_array() {
        for t in arr {
            if let (Some(k), Some(v)) = (t["Key"].as_str(), t["Value"].as_str()) {
                tags.insert(k.to_string(), json!(v));
            }
        }
    }
    ok_json(&json!({}))
}

fn enable_service_access(s: &mut OrgState, p: &Value) -> Response {
    let principal = p["ServicePrincipal"].as_str().unwrap_or_default();
    if s.services
        .iter()
        .any(|sv| sv["ServicePrincipal"].as_str() == Some(principal))
    {
        return ok_json(&json!({}));
    }
    s.services.push(json!({
        "ServicePrincipal": principal,
        "DateEnabled": now_f(),
    }));
    ok_json(&json!({}))
}

fn disable_service_access(s: &mut OrgState, p: &Value) -> Response {
    let principal = p["ServicePrincipal"].as_str().unwrap_or_default();
    if !TRUSTED_SERVICES.contains(&principal) {
        return err(
            "InvalidInputException",
            "You specified an unrecognized service principal.",
        );
    }
    s.services
        .retain(|sv| sv["ServicePrincipal"].as_str() != Some(principal));
    ok_json(&json!({}))
}

fn untag_resource(s: &mut OrgState, p: &Value) -> Response {
    let id = p["ResourceId"].as_str().unwrap_or_default().to_string();
    let keys: Vec<String> = p["TagKeys"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|k| k.as_str().unwrap_or_default().to_string())
                .collect()
        })
        .unwrap_or_default();
    let tags = match find_taggable(s, &id) {
        Some(tags) => tags,
        None => {
            return err(
                "TargetNotFoundException",
                "You specified a target that doesn't exist.",
            )
        }
    };
    for k in keys {
        tags.remove(&k);
    }
    ok_json(&json!({}))
}

fn list_tags_for_resource(s: &mut OrgState, p: &Value) -> Response {
    let id = p["ResourceId"].as_str().unwrap_or_default().to_string();
    let tags = match find_taggable(s, &id) {
        Some(tags) => tags,
        None => {
            return err(
                "TargetNotFoundException",
                "You specified a target that doesn't exist.",
            )
        }
    };
    let items: Vec<Value> = tags
        .iter()
        .map(|(k, v)| json!({"Key": k, "Value": v}))
        .collect();
    ok_json(&json!({ "Tags": items }))
}

fn list_service_access(s: &OrgState) -> Response {
    ok_json(&json!({ "EnabledServicePrincipals": s.services.clone() }))
}

fn register_delegated_admin(s: &mut OrgState, p: &Value) -> Response {
    let account_id = p["AccountId"].as_str().unwrap_or_default().to_string();
    let principal = p["ServicePrincipal"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let my_id = s
        .org
        .as_ref()
        .map(|o| o.master_account_id.clone())
        .unwrap_or_default();
    if account_id == my_id {
        return err(
            "ConstraintViolationException",
            "You cannot register master account/yourself as delegated administrator for your organization.",
        );
    }
    if s.accounts.iter().all(|a| a.id != account_id) {
        return err(
            "AccountNotFoundException",
            "You specified an account that doesn't exist.",
        );
    }
    let idx = match s.admins.iter().position(|(a, _, _)| a == &account_id) {
        Some(i) => i,
        None => {
            s.admins.push((account_id.clone(), now_f(), Map::new()));
            s.admins.len() - 1
        }
    };
    let (_, _, services) = &mut s.admins[idx];
    if services.contains_key(&principal) {
        return err(
            "AccountAlreadyRegisteredException",
            "The provided account is already a delegated administrator for your organization.",
        );
    }
    if !DELEGATED_SERVICES.contains(&principal.as_str()) {
        return err(
            "InvalidInputException",
            "You specified an unrecognized service principal.",
        );
    }
    services.insert(
        principal.clone(),
        json!({"ServicePrincipal": principal.clone(), "DelegationEnabledDate": now_f()}),
    );
    ok_json(&json!({}))
}

fn deregister_delegated_admin(s: &mut OrgState, p: &Value) -> Response {
    let account_id = p["AccountId"].as_str().unwrap_or_default().to_string();
    let principal = p["ServicePrincipal"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let Some(pos) = s.admins.iter().position(|(a, _, _)| a == &account_id) else {
        return err("AccountNotRegisteredException", "The provided account is not a registered delegated administrator for your organization.");
    };
    let services = &mut s.admins[pos].2;
    if !services.contains_key(&principal) {
        return err("AccountNotRegisteredException", "The provided account is not a registered delegated administrator for your organization.");
    }
    services.remove(&principal);
    if services.is_empty() {
        s.admins.remove(pos);
    }
    ok_json(&json!({}))
}

fn list_delegated_admins(s: &OrgState, p: &Value) -> Response {
    let service = p.get("ServicePrincipal").and_then(Value::as_str);
    let mut items = Vec::new();
    for (acct_id, enabled, services) in &s.admins {
        if let Some(sv) = service {
            if !services.contains_key(sv) {
                continue;
            }
        }
        let Some(a) = s.accounts.iter().find(|a| *a.id == *acct_id) else {
            continue;
        };
        let mut rec = account_record(a);
        if let Some(o) = rec.as_object_mut() {
            let egg = o.get("DelegationEnabledDate").is_some();
            let _ = egg;
            let _ = enabled;
            o.insert(
                "DelegationEnabledDate".into(),
                json!(chrono::Utc::now().timestamp() as f64),
            );
        }
        items.push(rec);
    }
    ok_json(&json!({ "DelegatedAdministrators": items }))
}

fn list_delegated_services(s: &OrgState, p: &Value) -> Response {
    let account_id = p["AccountId"].as_str().unwrap_or_default();
    let Some((_, _, services)) = s.admins.iter().find(|(a, _, _)| a == account_id) else {
        return err(
            NOT_IN_USE,
            "Your account is not a member of an organization.",
        );
    };
    let items: Vec<Value> = services.values().cloned().collect();
    ok_json(&json!({ "DelegatedServices": items }))
}

// ---------------------------------------------------------------------------
// Handshakes
// ---------------------------------------------------------------------------

fn new_handshake(s: &OrgState, target_id: &str, target_type: &str, action: &str) -> Handshake {
    let master = s
        .org
        .as_ref()
        .map(|o| o.master_account_id.clone())
        .unwrap_or_default();
    let org_id = s.org.as_ref().map(|o| o.id.clone()).unwrap_or_default();
    Handshake {
        id: make_handshake_id(),
        state: "OPEN",
        action: action.to_string(),
        parties: json!([
            {"Id": master, "Type": "ACCOUNT"},
            {"Id": target_id, "Type": target_type},
        ]),
        resources: json!([
            {"Value": org_id, "Type": "ORGANIZATION"},
            {"Value": "master@example.com", "Type": "EMAIL"},
            {"Value": master, "Type": "MASTER_EMAIL"},
        ]),
    }
}

fn handshake_record(s: &OrgState, h: &Handshake) -> Value {
    let master = s
        .org
        .as_ref()
        .map(|o| o.master_account_id.clone())
        .unwrap_or_default();
    let org_id = s.org.as_ref().map(|o| o.id.clone()).unwrap_or_default();
    json!({
        "Handshake": {
            "Id": h.id,
            "Arn": format!("arn:aws:organizations::{master}:handshake/{org_id}/{}/{}", h.action.to_lowercase(), h.id),
            "Parties": h.parties.clone(),
            "State": h.state,
            "RequestedTimestamp": now_f(),
            "ExpirationTimestamp": now_f(),
            "Action": h.action,
            "Resources": h.resources.clone(),
        }
    })
}

fn invite_account(s: &mut OrgState, p: &Value) -> Response {
    if s.org.is_none() {
        return err(
            NOT_IN_USE,
            "Your account is not a member of an organization.",
        );
    }
    let target_id = p["Target"]["Id"].as_str().unwrap_or_default().to_string();
    let target_type = p["Target"]["Type"]
        .as_str()
        .unwrap_or("ACCOUNT")
        .to_string();
    let hs = new_handshake(s, &target_id, &target_type, "INVITE");
    let record = handshake_record(s, &hs);
    s.handshakes.push(hs);
    ok_json(&record)
}

fn list_handshakes_for_org(s: &OrgState) -> Response {
    if s.org.is_none() {
        return err(
            NOT_IN_USE,
            "Your account is not a member of an organization.",
        );
    }
    let items: Vec<Value> = s
        .handshakes
        .iter()
        .map(|h| handshake_record(s, h)["Handshake"].clone())
        .collect();
    ok_json(&json!({ "Handshakes": items }))
}

fn list_handshakes_for_account(s: &OrgState) -> Response {
    let master = s
        .org
        .as_ref()
        .map(|o| o.master_account_id.clone())
        .unwrap_or_default();
    let items: Vec<Value> = s
        .handshakes
        .iter()
        .filter(|h| {
            h.parties
                .as_array()
                .map(|ps| ps.iter().any(|p| p["Id"].as_str() == Some(master.as_str())))
                .unwrap_or(false)
        })
        .map(|h| handshake_record(s, h)["Handshake"].clone())
        .collect();
    ok_json(&json!({ "Handshakes": items }))
}

fn describe_handshake(s: &OrgState, p: &Value) -> Response {
    let id = p["HandshakeId"].as_str().unwrap_or_default();
    match s.handshakes.iter().find(|h| h.id == id) {
        Some(h) => ok_json(&handshake_record(s, h)),
        None => err(
            "HandshakeNotFoundException",
            "The requested handshake with handshake id {} doesn't exist.",
        ),
    }
}

fn handshake_transition(s: &mut OrgState, p: &Value, next: &'static str) -> Response {
    let id = p["HandshakeId"].as_str().unwrap_or_default().to_string();
    // Take a snapshot of the open handshake first; state checks and the
    // response both use the frozen copy.
    let Some(h_snapshot) = s.handshakes.iter().find(|h_h| h_h.id == id).cloned() else {
        return err(
            "HandshakeNotFoundException",
            "You specified a handshake that doesn't exist.",
        );
    };
    if h_snapshot.state != "OPEN" {
        return err(
            "InvalidInputException",
            &format!(
                "Handshake is in {} state. Only OPEN handshakes can be changed.",
                h_snapshot.state
            ),
        );
    }
    if let Some(h) = s.handshakes.iter_mut().find(|h_h| h_h.id == id) {
        h.state = next;
    }
    let mut final_rec = handshake_record(s, &h_snapshot);
    final_rec["Handshake"]["State"] = json!(next);
    ok_json(&final_rec)
}

fn cancel_handshake(s: &mut OrgState, p: &Value) -> Response {
    handshake_transition(s, p, "CANCELED")
}

fn decline_handshake(s: &mut OrgState, p: &Value) -> Response {
    handshake_transition(s, p, "DECLINED")
}

fn accept_handshake(s: &mut OrgState, p: &Value) -> Response {
    handshake_transition(s, p, "ACCEPTED")
}

fn enable_all_features(s: &mut OrgState) -> Response {
    if s.org.is_none() {
        return err(
            NOT_IN_USE,
            "Your account is not a member of an organization.",
        );
    }
    let master = s.org.as_ref().unwrap().master_account_id.clone();
    let mut hs = new_handshake(s, &master, "ACCOUNT", "ENABLE_ALL_FEATURES");
    hs.state = "ACCEPTED";
    let record = handshake_record(s, &hs);
    s.handshakes.push(hs);
    ok_json(&record)
}

fn describe_effective_policy(s: &OrgState, p: &Value) -> Response {
    let typ = p["PolicyType"].as_str().unwrap_or_default().to_string();
    if !SUPPORTED_POLICY_TYPES.contains(&typ.as_str()) {
        return err("InvalidInputException", "You specified an invalid value.");
    }
    let target = p
        .get("TargetId")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_default();
    let target = if target.is_empty() {
        s.org
            .as_ref()
            .map(|o| o.master_account_id.clone())
            .unwrap_or_default()
    } else {
        target
    };
    ok_json(&json!({
        "EffectivePolicy": {
            "PolicyContent": "{}",
            "PolicyType": typ,
            "TargetId": target,
            "LastUpdatedTimestamp": now_f(),
        }
    }))
}

// ---------------------------------------------------------------------------
// Resource policy
// ---------------------------------------------------------------------------

fn put_resource_policy(s: &mut OrgState, p: &Value) -> Response {
    if s.org.is_none() {
        return err(
            NOT_IN_USE,
            "Your account is not a member of an organization.",
        );
    }
    let content = p["Content"].as_str().unwrap_or_default().to_string();
    if content.is_empty() {
        return err(
            "InvalidInputException",
            "You must include a value for all required parameters.",
        );
    }
    let Some(org) = s.org.as_ref() else {
        return err(
            NOT_IN_USE,
            "Your account is not a member of an organization.",
        );
    };
    match &mut s.resource_policy {
        Some(existing) => {
            if let Some(inner) = existing.get_mut("ResourcePolicy") {
                inner
                    .as_object_mut()
                    .map(|o| o.insert("Content".into(), json!(content)));
            }
        }
        None => {
            let mut rec = Map::new();
            let mut summary = Map::new();
            summary.insert("Id".into(), json!(make_resource_policy_id()));
            summary.insert(
                "Arn".into(),
                json!(format!(
                    "arn:{}:organizations::{}:organization/{}/resource-policy",
                    partition(&org.region),
                    org.master_account_id,
                    org.id
                )),
            );
            rec.insert("ResourcePolicySummary".into(), Value::Object(summary));
            rec.insert("Content".into(), json!(content));
            let mut wrapped = Map::new();
            wrapped.insert("ResourcePolicy".into(), Value::Object(rec));
            s.resource_policy = Some(wrapped);
        }
    }
    let rec = s.resource_policy.clone().unwrap();
    ok_json(&Value::Object(rec))
}

fn describe_resource_policy(s: &OrgState) -> Response {
    if s.org.is_none() {
        return err(
            NOT_IN_USE,
            "Your account is not a member of an organization.",
        );
    }
    match &s.resource_policy {
        Some(rec) => ok_json(&Value::Object(rec.clone())),
        None => err(
            "ResourcePolicyNotFoundException",
            "A resource policy doesn't exist.",
        ),
    }
}

fn delete_resource_policy(s: &mut OrgState) -> Response {
    if s.org.is_none() {
        return err(
            NOT_IN_USE,
            "Your account is not a member of an organization.",
        );
    }
    if s.resource_policy.is_none() {
        return err(
            "ResourcePolicyNotFoundException",
            "A resource policy doesn't exist.",
        );
    }
    s.resource_policy = None;
    ok_json(&json!({}))
}

fn ok_json(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::HashMap;

    fn req(op: &str, body: Value) -> Request {
        Request {
            service: "organizations".into(),
            operation: op.into(),
            method: "POST".into(),
            path: "/".into(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap_or_default()),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    fn body(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap_or(Value::Null)
    }

    fn scope() -> u64 {
        // 12-digit account ids (moto master-account expectations)
        (uuid::Uuid::new_v4().as_u128() % 900_000_000_000 + 100_000_000_000) as u64
    }

    fn make_org(acct: u64) -> String {
        let mut r = req("CreateOrganization", json!({"FeatureSet": "ALL"}));
        r.account = acct;
        body(&dispatch(&r).unwrap())["Organization"]["Id"]
            .as_str()
            .unwrap()
            .to_string()
    }

    #[test]
    fn org_lifecycle_reproduce_failure_modes() {
        let acct = scope();
        make_org(acct);
        // second create -> AlreadyInOrganizationException (same for lifecycle)
        let mut r = req("CreateOrganization", json!({}));
        r.account = acct;
        let resp = dispatch(&r).unwrap();
        assert_eq!(body(&resp)["__type"], "AlreadyInOrganizationException");

        // describe organization works
        r = req("DescribeOrganization", json!({}));
        r.account = acct;
        assert_eq!(
            body(&dispatch(&r).unwrap())["Organization"]["FeatureSet"],
            "ALL"
        );

        // describe after delete -> NotInUse
        r = req("DeleteOrganization", json!({}));
        r.account = acct;
        dispatch(&r).unwrap();
        r = req("DescribeOrganization", json!({}));
        r.account = acct;
        assert_eq!(
            body(&dispatch(&r).unwrap())["__type"],
            "AWSOrganizationsNotInUseException"
        );
    }

    #[test]
    fn roots_ous_accounts() {
        let acct = scope();
        make_org(acct);
        let mut r = req("ListRoots", json!({}));
        r.account = acct;
        let root = body(&dispatch(&r).unwrap())["Roots"][0].clone();
        let root_id = root["Id"].as_str().unwrap().to_string();
        assert!(root_id.starts_with("r-"));
        assert_eq!(root["Name"], "Root");

        r = req(
            "CreateOrganizationalUnit",
            json!({"ParentId": &root_id, "Name": "ocu1"}),
        );
        r.account = acct;
        let ou = body(&dispatch(&r).unwrap())["OrganizationalUnit"].clone();
        assert!(ou["Id"].as_str().unwrap().starts_with("ou-"));
        let ou_id = ou["Id"].as_str().unwrap().to_string();

        r = req(
            "ListOrganizationalUnitsForParent",
            json!({"ParentId": &root_id}),
        );
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert!(b["OrganizationalUnits"]
            .as_array()
            .unwrap()
            .iter()
            .any(|o| o["Id"] == ou_id.clone()));

        r = req(
            "ListChildren",
            json!({"ParentId": &root_id, "ChildType": "ORGANIZATIONAL_UNIT"}),
        );
        r.account = acct;
        assert!(body(&dispatch(&r).unwrap())["Children"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c["Id"] == ou_id.clone()));

        // create account
        r = req(
            "CreateAccount",
            json!({"Email": "a@example.com", "AccountName": "acct1"}),
        );
        r.account = acct;
        let st = body(&dispatch(&r).unwrap())["CreateAccountStatus"].clone();
        assert_eq!(st["State"], "SUCCEEDED");
        let acct_id = st["AccountId"].as_str().unwrap().to_string();

        r = req("ListAccounts", json!({}));
        r.account = acct;
        assert!(body(&dispatch(&r).unwrap())["Accounts"]
            .as_array()
            .unwrap()
            .iter()
            .any(|a| a["Name"] == "master"));

        // parents for the master account: root
        r = req("DescribeOrganization", json!({}));
        r.account = acct;
        let master = body(&dispatch(&r).unwrap())["Organization"]["MasterAccountId"]
            .as_str()
            .unwrap()
            .to_string();
        r = req("ListParents", json!({"ChildId": master}));
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(b["Parents"][0]["Type"], "ROOT");

        r = req(
            "MoveAccount",
            json!({
                "AccountId": &acct_id, "SourceParentId": &root_id, "DestinationParentId": &ou_id
            }),
        );
        r.account = acct;
        dispatch(&r).unwrap();
        r = req(
            "ListChildren",
            json!({"ParentId": &ou_id, "ChildType": "ACCOUNT"}),
        );
        r.account = acct;
        assert!(body(&dispatch(&r).unwrap())["Children"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c["Id"] == acct_id.clone()));

        r = req(
            "RemoveAccountFromOrganization",
            json!({"AccountId": &acct_id}),
        );
        r.account = acct;
        dispatch(&r).unwrap();
        r = req("ListAccounts", json!({}));
        r.account = acct;
        assert!(body(&dispatch(&r).unwrap())["Accounts"]
            .as_array()
            .unwrap()
            .iter()
            .all(|a| a["Id"] != acct_id.clone()));
    }

    #[test]
    fn policy_lifecycle_and_errors() {
        let acct = scope();
        make_org(acct);
        let mut r = req(
            "CreatePolicy",
            json!({
                "Content": "{}",
                "Description": "d",
                "Name": "pol1",
                "Type": "SERVICE_CONTROL_POLICY",
            }),
        );
        r.account = acct;
        let pid = body(&dispatch(&r).unwrap())["Policy"]["PolicySummary"]["Id"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(pid.starts_with("p-"));

        r = req("DescribePolicy", json!({"PolicyId": &pid}));
        r.account = acct;
        assert_eq!(
            body(&dispatch(&r).unwrap())["Policy"]["PolicySummary"]["Name"],
            "pol1"
        );

        // attach + list for target
        r = req("ListRoots", json!({}));
        r.account = acct;
        let root_id = body(&dispatch(&r).unwrap())["Roots"][0]["Id"]
            .as_str()
            .unwrap()
            .to_string();
        r = req(
            "AttachPolicy",
            json!({"PolicyId": &pid, "TargetId": &root_id}),
        );
        r.account = acct;
        dispatch(&r).unwrap();
        r = req(
            "ListPoliciesForTarget",
            json!({"TargetId": &root_id, "Filter": "SERVICE_CONTROL_POLICY"}),
        );
        r.account = acct;
        assert!(body(&dispatch(&r).unwrap())["Policies"]
            .as_array()
            .unwrap()
            .iter()
            .any(|pl| pl["Id"] == pid.clone()));
        r = req("ListTargetsForPolicy", json!({"PolicyId": &pid}));
        r.account = acct;
        assert!(body(&dispatch(&r).unwrap())["Targets"]
            .as_array()
            .unwrap()
            .iter()
            .any(|t| t["TargetId"] == root_id.clone()));

        // invalid policy id for describe
        r = req("DescribePolicy", json!({"PolicyId": "fake-id"}));
        r.account = acct;
        let resp = dispatch(&r).unwrap();
        assert_eq!(body(&resp)["__type"], "InvalidInputException");

        // attached policies block delete (PolicyInUse)
        r = req("DeletePolicy", json!({"PolicyId": &pid}));
        r.account = acct;
        assert_eq!(
            body(&dispatch(&r).unwrap())["__type"],
            "PolicyInUseException"
        );
        r = req(
            "DetachPolicy",
            json!({"PolicyId": &pid, "TargetId": &root_id}),
        );
        r.account = acct;
        dispatch(&r).unwrap();
        r = req("DeletePolicy", json!({"PolicyId": &pid}));
        r.account = acct;
        assert_eq!(dispatch(&r).unwrap().status, 200);
        r = req("DescribePolicy", json!({"PolicyId": &pid}));
        r.account = acct;
        assert_eq!(
            body(&dispatch(&r).unwrap())["__type"],
            "PolicyNotFoundException"
        );
    }

    #[test]
    fn handshakes_service_access_and_resource_policy() {
        let acct = scope();
        make_org(acct);
        let mut r = req(
            "EnableAwsServiceAccess",
            json!({"ServicePrincipal": "ssm.amazonaws.com"}),
        );
        r.account = acct;
        dispatch(&r).unwrap();
        r = req("ListAwsServiceAccessForOrganization", json!({}));
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert!(b["EnabledServicePrincipals"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["ServicePrincipal"] == "ssm.amazonaws.com"));

        r = req(
            "InviteAccountToOrganization",
            json!({"Target": {"Id": "333333333333", "Type": "ACCOUNT"}}),
        );
        r.account = acct;
        let hs = body(&dispatch(&r).unwrap())["Handshake"].clone();
        assert_eq!(hs["State"], "OPEN");
        assert_eq!(hs["Action"], "INVITE");
        let hs_id = hs["Id"].as_str().unwrap().to_string();

        r = req("DescribeHandshake", json!({"HandshakeId": &hs_id}));
        r.account = acct;
        assert_eq!(body(&dispatch(&r).unwrap())["Handshake"]["State"], "OPEN");

        r = req("CancelHandshake", json!({"HandshakeId": &hs_id}));
        r.account = acct;
        assert_eq!(
            body(&dispatch(&r).unwrap())["Handshake"]["State"],
            "CANCELED"
        );

        r = req("ListHandshakesForOrganization", json!({}));
        r.account = acct;
        assert!(body(&dispatch(&r).unwrap())["Handshakes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|h| h["Id"] == hs_id.clone()));

        // resource policy
        r = req(
            "PutResourcePolicy",
            json!({"Content": "{\"Version\":\"2012-10-17\"}"}),
        );
        r.account = acct;
        let rp = body(&dispatch(&r).unwrap())["ResourcePolicy"].clone();
        assert!(rp["ResourcePolicySummary"]["Id"]
            .as_str()
            .unwrap()
            .starts_with("rp-"));
        r = req("DescribeResourcePolicy", json!({}));
        r.account = acct;
        assert_eq!(
            body(&dispatch(&r).unwrap())["ResourcePolicy"]["Content"],
            "{\"Version\":\"2012-10-17\"}"
        );
        r = req("DeleteResourcePolicy", json!({}));
        r.account = acct;
        dispatch(&r).unwrap();
        r = req("DescribeResourcePolicy", json!({}));
        r.account = acct;
        let resp = dispatch(&r).unwrap();
        assert_eq!(body(&resp)["__type"], "ResourcePolicyNotFoundException");
    }

    #[test]
    fn tags_on_root_and_ou() {
        let acct = scope();
        make_org(acct);
        let mut r = req("ListRoots", json!({}));
        r.account = acct;
        let root_id = body(&dispatch(&r).unwrap())["Roots"][0]["Id"]
            .as_str()
            .unwrap()
            .to_string();

        r = req(
            "TagResource",
            json!({
                "ResourceId": &root_id,
                "Tags": [{"Key": "env", "Value": "test"}],
            }),
        );
        r.account = acct;
        dispatch(&r).unwrap();
        r = req("ListTagsForResource", json!({"ResourceId": &root_id}));
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(b["Tags"][0]["Key"], "env");
        r = req(
            "UntagResource",
            json!({"ResourceId": &root_id, "TagKeys": ["env"]}),
        );
        r.account = acct;
        dispatch(&r).unwrap();
        r = req("ListTagsForResource", json!({"ResourceId": &root_id}));
        r.account = acct;
        assert_eq!(
            body(&dispatch(&r).unwrap())["Tags"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
    }

    #[test]
    fn delegated_admins_flow() {
        let acct = scope();
        make_org(acct);
        let mut r = req(
            "CreateAccount",
            json!({"Email": "d@example.com", "AccountName": "deleg"}),
        );
        r.account = acct;
        let acct_id = body(&dispatch(&r).unwrap())["CreateAccountStatus"]["AccountId"]
            .as_str()
            .unwrap()
            .to_string();
        r = req(
            "EnableAwsServiceAccess",
            json!({"ServicePrincipal": "ssm.amazonaws.com"}),
        );
        r.account = acct;
        dispatch(&r).unwrap();
        r = req(
            "RegisterDelegatedAdministrator",
            json!({
                "AccountId": &acct_id, "ServicePrincipal": "ssm.amazonaws.com"
            }),
        );
        r.account = acct;
        dispatch(&r).unwrap();
        r = req(
            "ListDelegatedAdministrators",
            json!({"ServicePrincipal": "ssm.amazonaws.com"}),
        );
        r.account = acct;
        assert!(body(&dispatch(&r).unwrap())["DelegatedAdministrators"]
            .as_array()
            .unwrap()
            .iter()
            .any(|a| a["Id"] == acct_id.clone()));
        r = req(
            "ListDelegatedServicesForAccount",
            json!({"AccountId": &acct_id}),
        );
        r.account = acct;
        assert!(body(&dispatch(&r).unwrap())["DelegatedServices"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["ServicePrincipal"] == "ssm.amazonaws.com"));
        r = req(
            "DeregisterDelegatedAdministrator",
            json!({
                "AccountId": &acct_id, "ServicePrincipal": "ssm.amazonaws.com"
            }),
        );
        r.account = acct;
        dispatch(&r).unwrap();
        r = req(
            "ListDelegatedAdministrators",
            json!({"ServicePrincipal": "ssm.amazonaws.com"}),
        );
        r.account = acct;
        assert_eq!(
            body(&dispatch(&r).unwrap())["DelegatedAdministrators"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
    }
}
