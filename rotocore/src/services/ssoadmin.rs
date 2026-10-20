//! SSO Admin backend (JSON protocol, target `SWBExternalService.<Op>`).
//!
//! Port of moto's ssoadmin: pre-seeded SSO instance per account/region,
//! permission sets and account assignments.
//!
//! moto fork quirks kept verbatim: `UpdatePermissionSet` overwrites the stored
//! description/relay state with nulls when the client omits them and resets
//! `SessionDuration` to 3600 (moto reads the params with those defaults);
//! `ListPermissionSetsProvisionedToAccount` reads the instance provisioning
//! record rather than every permission set; managed-policy attachments are
//! validated against moto's `aws_managed_policies` dataset (embedded as
//! `ssoadmin_data/aws_managed_policies.json`) and both managed and
//! customer-managed attachments share the same limit of 20 per permission set;
//! `DeleteAccountAssignment` raises `ResourceNotFoundException` when nothing
//! matched and the deleted assignment keeps its request id for the
//! `DescribeAccountAssignmentDeletionStatus` lookup.

use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Value};
use std::collections::HashMap;

const MAX_MANAGED_POLICIES_PER_PERMISSION_SET: i64 = 20;

#[derive(Default)]
struct State {
    /// Seeded lazily on first use, exactly one instance like moto.
    instances: Vec<Value>,
    /// Permission sets in moto's list order (moto re-appends on update).
    permission_sets: Vec<PsRecord>,
    /// Active assignments, moto's `AccountAssignment` shape.
    assignments: Vec<Assignment>,
    /// moto's `deleted_account_assignments`, for the DeletionStatus lookup.
    deleted_assignments: Vec<Assignment>,
    /// moto's `Instance.provisioned_permission_sets`: instance arn -> ps arns.
    provisioned: Vec<(String, Vec<String>)>,
    /// Untagged-but-recorded tags: arn -> {Key: Value}.
    tags: HashMap<String, Value>,
}

/// moto's `PermissionSet` record: the wire summary body plus the mutable
/// policy state the fork tracks on the object.
struct PsRecord {
    instance_arn: String,
    /// Wire summary (Name/Description/PermissionSetArn/SessionDuration/
    /// RelayState/CreatedDate) exactly as moto's to_json renders it.
    body: Value,
    /// moto's `inline_policy` — stored raw (absent `InlinePolicy` reads null).
    inline_policy: Value,
    /// (arn, name) — moto's ManagedPolicy entries.
    managed_policies: Vec<(String, String)>,
    /// (name, path) — moto's CustomerManagedPolicy entries; the path defaults
    /// to "/" and explicit-null paths never match (moto's dict semantics).
    customer_policies: Vec<(String, Option<String>)>,
    /// moto's `total_managed_policies_attached` (both policy kinds).
    attached_policies: i64,
}

/// moto's `AccountAssignment` record.
struct Assignment {
    request_id: String,
    instance_arn: String,
    target_id: Value,
    target_type: Value,
    permission_set_arn: Value,
    principal_type: Value,
    principal_id: Value,
    created_date: f64,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("ssoadmin"));
    SLOT.clone()
}

fn ok(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

/// moto's `utcnow().timestamp()` (float epoch seconds).
fn epoch() -> f64 {
    chrono::Utc::now().timestamp_millis() as f64 / 1000.0
}

/// moto's get_random_string: lowercase letters + digits.
fn random_string(len: usize) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let bytes = uuid::Uuid::new_v4().as_bytes().to_vec();
    (0..len)
        .map(|i| ALPHABET[bytes[i % bytes.len()] as usize % ALPHABET.len()] as char)
        .collect()
}

fn ps_arn(instance_arn: &str) -> String {
    format!("{instance_arn}/ps-{}", random_string(16))
}

fn ps_json(
    name: Option<&str>,
    description: Option<&str>,
    arn: &str,
    session_duration: Value,
    relay_state: Value,
) -> Value {
    json!({
        "Name": name,
        "Description": description,
        "PermissionSetArn": arn,
        "SessionDuration": session_duration,
        "RelayState": relay_state,
    })
}

fn with_default_instance(st: &mut State, account: u64) {
    if st.instances.is_empty() {
        st.instances.push(json!({
            "CreatedDate": epoch(),
            "IdentityStoreId": format!("d-{}", random_string(10)),
            "InstanceArn": format!("arn:aws:sso:::instance/ssoins-{}", random_string(16)),
            "Name": Value::Null,
            "OwnerAccountId": account.to_string(),
            "Status": "ACTIVE",
        }));
    }
}

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "ssoadmin" {
        return None;
    }
    let body = crate::proto::parse_json_body(&req.body);
    let slot = state();
    let mut st = slot.write();
    let b = st.entry((req.account, req.region.clone())).or_default();
    with_default_instance(b, req.account);
    Some(handle(b, &body, req))
}

/// moto's JsonRESTError: HTTP 400 + `{"__type": <error_type>, "message": ...}`.
fn error_response(error_type: &str, message: &str) -> Response {
    let e = json!({
        "__type": error_type,
        "message": message,
    });
    Response::json(400, serde_json::to_vec(&e).unwrap_or_default())
}

fn rnf(message: &str) -> Response {
    error_response("ResourceNotFoundException", message)
}

fn conflict(message: &str) -> Response {
    error_response("ConflictException", message)
}

fn quota_exceeded(message: &str) -> Response {
    error_response("ServiceQuotaExceededException", message)
}

/// moto's dict lookups that crash into a plain 500 (unreachable through boto3 —
/// the members are required shapes): mirror the 500 without the crash.
fn moto_internal_error() -> Response {
    let e = json!({
        "__type": "InternalError",
        "message": "InternalError",
    });
    Response::json(500, serde_json::to_vec(&e).unwrap_or_default())
}

/// moto's `_find_permission_set`: the tail of the ARN after the last "/".
fn ps_id(permission_set_arn: &str) -> String {
    permission_set_arn
        .split('/')
        .next_back()
        .unwrap_or("")
        .to_string()
}

fn find_ps(st: &State, instance_arn: &str, permission_set_arn: &str) -> Option<usize> {
    st.permission_sets.iter().position(|p| {
        p.instance_arn == instance_arn && p.body["PermissionSetArn"] == json!(permission_set_arn)
    })
}

/// moto's `_find_permission_set` miss message.
fn ps_not_found(permission_set_arn: &str) -> Response {
    rnf(&format!(
        "Could not find PermissionSet with id {}",
        ps_id(permission_set_arn)
    ))
}

/// moto's `Instance.provisioned_permission_sets` for `instance_arn`.
fn provisioned_for<'a>(st: &'a State, instance_arn: &str) -> Option<&'a Vec<String>> {
    st.provisioned
        .iter()
        .find(|(i, _)| i == instance_arn)
        .map(|(_, list)| list)
}

/// moto's `get_partition` for `_find_managed_policy`'s expected ARN.
fn partition(region: &str) -> &'static str {
    if region.starts_with("cn-") {
        "aws-cn"
    } else if region.starts_with("us-gov-") {
        "aws-us-gov"
    } else if region.starts_with("us-iso-") {
        "aws-iso"
    } else if region.starts_with("us-isob-") {
        "aws-iso-b"
    } else if region.starts_with("eusc-") {
        "aws-eusc"
    } else {
        "aws"
    }
}

/// moto's `_find_managed_policy`: resolve the ARN against the managed-policy
/// dataset, requiring the exact `arn:<partition>:iam::aws:policy<path><name>`.
/// Returns the policy name on match.
fn find_managed_policy(region: &str, managed_policy_arn: &str) -> Option<String> {
    static POLICIES: Lazy<HashMap<String, String>> = Lazy::new(|| {
        // name -> path, from moto's iam `aws_managed_policies` dataset.
        serde_json::from_str(include_str!("ssoadmin_data/aws_managed_policies.json"))
            .unwrap_or_default()
    });
    let policy_name = managed_policy_arn.split('/').next_back().unwrap_or("");
    let path = POLICIES.get(policy_name)?;
    let expected = format!(
        "arn:{}:iam::aws:policy{path}{policy_name}",
        partition(region)
    );
    (managed_policy_arn == expected).then(|| policy_name.to_string())
}

/// Read a body member the way moto does: absent key == null.
fn param(body: &Value, key: &str) -> Value {
    body.get(key).cloned().unwrap_or(Value::Null)
}

/// String member read: absent key reads "".
fn body_str<'a>(body: &'a Value, key: &str) -> &'a str {
    body.get(key).and_then(Value::as_str).unwrap_or("")
}

/// `CustomerManagedPolicyReference.Path`: absent key defaults to "/", an
/// explicit null or non-string never matches (moto's `.get("Path", "/")`).
fn reference_path(reference: &Value) -> Option<String> {
    match reference.get("Path") {
        None => Some("/".to_string()),
        Some(Value::String(s)) => Some(s.clone()),
        Some(_) => None,
    }
}

/// Python f-string rendering of the optional path in error messages.
fn wire_str(v: Option<&str>) -> String {
    match v {
        Some(s) => s.to_string(),
        None => "None".to_string(),
    }
}

/// moto's `AccountAssignment.to_json(include_creation_date,
/// include_request_id)` plus the handler-injected Status.
fn assignment_json(a: &Assignment, status: Option<&str>) -> Value {
    let mut summary = json!({
        "TargetId": a.target_id,
        "TargetType": a.target_type,
        "PermissionSetArn": a.permission_set_arn,
        "PrincipalType": a.principal_type,
        "PrincipalId": a.principal_id,
        "CreatedDate": a.created_date,
        "RequestId": a.request_id,
    });
    if let Some(s) = status {
        summary["Status"] = json!(s);
    }
    summary
}

fn handle(st: &mut State, p: &Value, req: &Request) -> Response {
    match req.operation.as_str() {
        "ListInstances" => ok(&json!({"Instances": st.instances})),
        "UpdateInstance" => {
            // moto loops and updates matching instances only; an unknown ARN
            // is a silent no-op.
            let arn = param(p, "InstanceArn");
            let name = param(p, "Name");
            for instance in &mut st.instances {
                if instance["InstanceArn"] == arn {
                    instance["Name"] = name.clone();
                }
            }
            ok(&json!({}))
        }
        "CreatePermissionSet" => {
            let instance_arn = body_str(p, "InstanceArn").to_string();
            let arn = ps_arn(&instance_arn);
            let mut body = ps_json(
                p.get("Name").and_then(Value::as_str),
                p.get("Description").and_then(Value::as_str),
                &arn,
                param(p, "SessionDuration"),
                param(p, "RelayState"),
            );
            body["CreatedDate"] = json!(epoch());
            if let Some(tags) = p.get("Tags") {
                store_tags(st, &arn, tags);
            }
            st.permission_sets.push(PsRecord {
                instance_arn,
                body: body.clone(),
                // moto defaults the object's inline_policy to "".
                inline_policy: json!(""),
                managed_policies: Vec::new(),
                customer_policies: Vec::new(),
                attached_policies: 0,
            });
            ok(&json!({"PermissionSet": body}))
        }
        "DescribePermissionSet" => {
            let instance_arn = body_str(p, "InstanceArn");
            let ps = body_str(p, "PermissionSetArn");
            match find_ps(st, instance_arn, ps) {
                Some(i) => ok(&json!({"PermissionSet": st.permission_sets[i].body})),
                None => ps_not_found(ps),
            }
        }
        "ListPermissionSets" => {
            let instance_arn = body_str(p, "InstanceArn");
            let arns: Vec<Value> = st
                .permission_sets
                .iter()
                .filter(|ps| ps.instance_arn == instance_arn)
                .map(|ps| ps.body["PermissionSetArn"].clone())
                .collect();
            ok(&json!({"PermissionSets": arns, "NextToken": Value::Null}))
        }
        "ListPermissionSetsProvisionedToAccount" => {
            // moto reads the instance provisioning record and ignores
            // AccountId/ProvisioningStatus/pagination.
            let instance_arn = body_str(p, "InstanceArn");
            let arns = provisioned_for(st, instance_arn)
                .map(|list| list.iter().map(|p| json!(p)).collect::<Vec<Value>>())
                .unwrap_or_default();
            ok(&json!({"PermissionSets": arns}))
        }
        "UpdatePermissionSet" => {
            let instance_arn = body_str(p, "InstanceArn");
            let ps = body_str(p, "PermissionSetArn");
            match find_ps(st, instance_arn, ps) {
                Some(i) => {
                    // moto overwrites unconditionally: absent Description and
                    // RelayState read null, SessionDuration defaults to 3600.
                    let rec = &mut st.permission_sets[i];
                    rec.body["Description"] = param(p, "Description");
                    rec.body["SessionDuration"] = p
                        .get("SessionDuration")
                        .cloned()
                        .unwrap_or_else(|| json!(3600));
                    rec.body["RelayState"] = param(p, "RelayState");
                    // moto removes the record and re-appends it.
                    let rec = st.permission_sets.remove(i);
                    st.permission_sets.push(rec);
                    ok(&json!({}))
                }
                None => ps_not_found(ps),
            }
        }
        "DeletePermissionSet" => {
            let instance_arn = body_str(p, "InstanceArn");
            let ps = body_str(p, "PermissionSetArn");
            match find_ps(st, instance_arn, ps) {
                Some(i) => {
                    st.permission_sets.remove(i);
                    for (_, list) in &mut st.provisioned {
                        list.retain(|a| a.as_str() != ps);
                    }
                    ok(&json!({}))
                }
                None => ps_not_found(ps),
            }
        }
        "PutInlinePolicyToPermissionSet" => {
            let instance_arn = body_str(p, "InstanceArn");
            let ps = body_str(p, "PermissionSetArn");
            match find_ps(st, instance_arn, ps) {
                Some(i) => {
                    st.permission_sets[i].inline_policy = param(p, "InlinePolicy");
                    ok(&json!({}))
                }
                None => ps_not_found(ps),
            }
        }
        "GetInlinePolicyForPermissionSet" => {
            let instance_arn = body_str(p, "InstanceArn");
            let ps = body_str(p, "PermissionSetArn");
            match find_ps(st, instance_arn, ps) {
                Some(i) => ok(&json!({"InlinePolicy": st.permission_sets[i].inline_policy})),
                None => ps_not_found(ps),
            }
        }
        "DeleteInlinePolicyFromPermissionSet" => {
            let instance_arn = body_str(p, "InstanceArn");
            let ps = body_str(p, "PermissionSetArn");
            match find_ps(st, instance_arn, ps) {
                Some(i) => {
                    st.permission_sets[i].inline_policy = json!("");
                    ok(&json!({}))
                }
                None => ps_not_found(ps),
            }
        }
        "AttachManagedPolicyToPermissionSet" => {
            let instance_arn = body_str(p, "InstanceArn");
            let ps = body_str(p, "PermissionSetArn");
            let policy_arn = body_str(p, "ManagedPolicyArn");
            match find_ps(st, instance_arn, ps) {
                Some(i) => match find_managed_policy(&req.region, policy_arn) {
                    Some(name) => {
                        let rec = &mut st.permission_sets[i];
                        if rec.managed_policies.iter().any(|(a, _)| a == policy_arn) {
                            return conflict(&format!(
                                "Permission set with id {} already has a typed link attachment to a manged policy with {policy_arn}",
                                ps_id(ps)
                            ));
                        }
                        if rec.attached_policies >= MAX_MANAGED_POLICIES_PER_PERMISSION_SET {
                            return quota_exceeded(&format!(
                                "You have exceeded AWS SSO limits. Cannot create ManagedPolicy more than {MAX_MANAGED_POLICIES_PER_PERMISSION_SET} for id {}. Please refer to https://docs.aws.amazon.com/singlesignon/latest/userguide/limits.html",
                                ps_id(ps)
                            ));
                        }
                        rec.managed_policies.push((policy_arn.to_string(), name));
                        rec.attached_policies += 1;
                        ok(&json!({}))
                    }
                    None => rnf(&format!("Policy does not exist with ARN: {policy_arn}")),
                },
                None => ps_not_found(ps),
            }
        }
        "ListManagedPoliciesInPermissionSet" => {
            let instance_arn = body_str(p, "InstanceArn");
            let ps = body_str(p, "PermissionSetArn");
            match find_ps(st, instance_arn, ps) {
                Some(i) => {
                    let list: Vec<Value> = st.permission_sets[i]
                        .managed_policies
                        .iter()
                        .map(|(arn, name)| json!({"Arn": arn, "Name": name}))
                        .collect();
                    ok(&json!({"AttachedManagedPolicies": list, "NextToken": Value::Null}))
                }
                None => ps_not_found(ps),
            }
        }
        "DetachManagedPolicyFromPermissionSet" => {
            let instance_arn = body_str(p, "InstanceArn");
            let ps = body_str(p, "PermissionSetArn");
            let policy_arn = body_str(p, "ManagedPolicyArn");
            match find_ps(st, instance_arn, ps) {
                Some(i) => {
                    let rec = &mut st.permission_sets[i];
                    match rec
                        .managed_policies
                        .iter()
                        .position(|(a, _)| a == policy_arn)
                    {
                        Some(j) => {
                            rec.managed_policies.remove(j);
                            rec.attached_policies -= 1;
                            ok(&json!({}))
                        }
                        None => rnf(&format!(
                            "Could not find ManagedPolicy with arn {policy_arn}"
                        )),
                    }
                }
                None => ps_not_found(ps),
            }
        }
        "AttachCustomerManagedPolicyReferenceToPermissionSet" => {
            let instance_arn = body_str(p, "InstanceArn");
            let ps = body_str(p, "PermissionSetArn");
            let reference = param(p, "CustomerManagedPolicyReference");
            match find_ps(st, instance_arn, ps) {
                Some(i) => {
                    if !reference.is_object()
                        || reference.get("Name").and_then(Value::as_str).is_none()
                    {
                        // moto indexes ["Name"] — a missing key is a plain 500.
                        return moto_internal_error();
                    }
                    let name = reference["Name"].as_str().unwrap_or("");
                    let path = reference_path(&reference);
                    let path_ref = path.as_deref();
                    let rec = &mut st.permission_sets[i];
                    if rec
                        .customer_policies
                        .iter()
                        .any(|(n, pt)| n == name && pt.as_deref() == path_ref)
                    {
                        return conflict(&format!(
                            "Given customer managed policy with name: {name}  and path {} already attached",
                            wire_str(path_ref)
                        ));
                    }
                    if rec.attached_policies >= MAX_MANAGED_POLICIES_PER_PERMISSION_SET {
                        return quota_exceeded(&format!(
                            "Cannot attach managed policy: number of attached managed policies is already at maximum {MAX_MANAGED_POLICIES_PER_PERMISSION_SET}"
                        ));
                    }
                    rec.customer_policies.push((name.to_string(), path));
                    rec.attached_policies += 1;
                    ok(&json!({}))
                }
                None => ps_not_found(ps),
            }
        }
        "ListCustomerManagedPolicyReferencesInPermissionSet" => {
            let instance_arn = body_str(p, "InstanceArn");
            let ps = body_str(p, "PermissionSetArn");
            match find_ps(st, instance_arn, ps) {
                Some(i) => {
                    let list: Vec<Value> = st.permission_sets[i]
                        .customer_policies
                        .iter()
                        .map(|(name, path)| json!({"Name": name, "Path": path}))
                        .collect();
                    ok(&json!({"CustomerManagedPolicyReferences": list, "NextToken": Value::Null}))
                }
                None => ps_not_found(ps),
            }
        }
        "DetachCustomerManagedPolicyReferenceFromPermissionSet" => {
            let instance_arn = body_str(p, "InstanceArn");
            let ps = body_str(p, "PermissionSetArn");
            let reference = param(p, "CustomerManagedPolicyReference");
            match find_ps(st, instance_arn, ps) {
                Some(i) => {
                    if !reference.is_object()
                        || reference.get("Name").and_then(Value::as_str).is_none()
                    {
                        return moto_internal_error();
                    }
                    let name = reference["Name"].as_str().unwrap_or("");
                    let path = reference_path(&reference);
                    let path_ref = path.as_deref();
                    let rec = &mut st.permission_sets[i];
                    match rec
                        .customer_policies
                        .iter()
                        .position(|(n, pt)| n == name && pt.as_deref() == path_ref)
                    {
                        Some(j) => {
                            rec.customer_policies.remove(j);
                            rec.attached_policies -= 1;
                            ok(&json!({}))
                        }
                        None => rnf(&format!(
                            "Given managed policy with name: {name}  and path {} does not exist on PermissionSet",
                            wire_str(path_ref)
                        )),
                    }
                }
                None => ps_not_found(ps),
            }
        }
        "DescribeAccountAssignmentCreationStatus" => {
            let request_id = body_str(p, "AccountAssignmentCreationRequestId");
            match st.assignments.iter().find(|a| a.request_id == request_id) {
                Some(a) => ok(&json!({
                    "AccountAssignmentCreationStatus": assignment_json(a, Some("SUCCEEDED"))
                })),
                None => rnf(""),
            }
        }
        "DescribeAccountAssignmentDeletionStatus" => {
            let request_id = body_str(p, "AccountAssignmentDeletionRequestId");
            match st
                .deleted_assignments
                .iter()
                .find(|a| a.request_id == request_id)
            {
                Some(a) => ok(&json!({
                    "AccountAssignmentDeletionStatus": assignment_json(a, Some("SUCCEEDED"))
                })),
                None => rnf(""),
            }
        }
        "CreateAccountAssignment" => {
            let assignment = Assignment {
                request_id: uuid::Uuid::new_v4().to_string(),
                instance_arn: body_str(p, "InstanceArn").to_string(),
                target_id: param(p, "TargetId"),
                target_type: param(p, "TargetType"),
                permission_set_arn: param(p, "PermissionSetArn"),
                principal_type: param(p, "PrincipalType"),
                principal_id: param(p, "PrincipalId"),
                created_date: epoch(),
            };
            let summary = assignment_json(&assignment, Some("SUCCEEDED"));
            st.assignments.push(assignment);
            ok(&json!({"AccountAssignmentCreationStatus": summary}))
        }
        "DeleteAccountAssignment" => {
            // moto matches all six parameters; a miss raises
            // ResourceNotFoundException with the empty default message.
            let instance_arn = body_str(p, "InstanceArn");
            let target_id = param(p, "TargetId");
            let target_type = param(p, "TargetType");
            let permission_set_arn = param(p, "PermissionSetArn");
            let principal_type = param(p, "PrincipalType");
            let principal_id = param(p, "PrincipalId");
            let idx = st.assignments.iter().position(|a| {
                a.instance_arn == instance_arn
                    && a.target_id == target_id
                    && a.target_type == target_type
                    && a.permission_set_arn == permission_set_arn
                    && a.principal_type == principal_type
                    && a.principal_id == principal_id
            });
            match idx {
                Some(i) => {
                    let assignment = st.assignments.remove(i);
                    let summary = assignment_json(&assignment, Some("SUCCEEDED"));
                    st.deleted_assignments.push(assignment);
                    ok(&json!({"AccountAssignmentDeletionStatus": summary}))
                }
                None => rnf(""),
            }
        }
        "ListAccountAssignments" => {
            let account_id = param(p, "AccountId");
            let ps = param(p, "PermissionSetArn");
            let out: Vec<Value> = st
                .assignments
                .iter()
                .filter(|a| a.target_id == account_id && a.permission_set_arn == ps)
                .map(|a| {
                    json!({
                        "AccountId": a.target_id,
                        "PermissionSetArn": a.permission_set_arn,
                        "PrincipalType": a.principal_type,
                        "PrincipalId": a.principal_id,
                    })
                })
                .collect();
            ok(&json!({"AccountAssignments": out, "NextToken": Value::Null}))
        }
        "ListAccountAssignmentsForPrincipal" => {
            let out: Vec<Value> = st
                .assignments
                .iter()
                .map(|a| {
                    json!({
                        "AccountId": a.target_id,
                        "PermissionSetArn": a.permission_set_arn,
                        "PrincipalId": a.principal_id,
                        "PrincipalType": a.principal_type,
                    })
                })
                .collect();
            ok(&json!({"AccountAssignments": out, "NextToken": Value::Null}))
        }
        "ListTagsForResource" => {
            let arn = body_str(p, "ResourceARN");
            let tags = st.tags.get(arn).cloned().unwrap_or(json!({}));
            let list: Vec<Value> = tags
                .as_object()
                .map(|m| {
                    m.iter()
                        .map(|(k, v)| json!({"Key": k, "Value": v}))
                        .collect()
                })
                .unwrap_or_default();
            ok(&json!({"Tags": list}))
        }
        "TagResource" => {
            let arn = body_str(p, "ResourceARN");
            if let Some(tags) = p.get("Tags") {
                store_tags(st, arn, tags);
            }
            ok(&json!({}))
        }
        "UntagResource" => {
            let arn = body_str(p, "ResourceARN");
            if let Some(keys) = p.get("TagKeys").and_then(Value::as_array) {
                if let Some(entry) = st.tags.get_mut(arn) {
                    for k in keys {
                        entry
                            .as_object_mut()
                            .unwrap()
                            .remove(k.as_str().unwrap_or(""));
                    }
                }
            }
            ok(&json!({}))
        }
        "ProvisionPermissionSet" => {
            let instance_arn = body_str(p, "InstanceArn");
            let ps = body_str(p, "PermissionSetArn");
            match find_ps(st, instance_arn, ps) {
                Some(i) => {
                    let arn = st.permission_sets[i].body["PermissionSetArn"]
                        .as_str()
                        .unwrap_or("")
                        .to_string();
                    match st.provisioned.iter_mut().find(|(i, _)| i == instance_arn) {
                        Some((_, list)) => list.push(arn),
                        None => st.provisioned.push((instance_arn.to_string(), vec![arn])),
                    }
                    // moto's handler builds the status itself: TargetType and
                    // TargetId are ignored — the permission set is provisioned
                    // to the caller's account.
                    ok(&json!({
                        "PermissionSetProvisioningStatus": {
                            "AccountId": req.account.to_string(),
                            "CreatedDate": epoch(),
                            "PermissionSetArn": ps,
                            "RequestId": uuid::Uuid::new_v4().to_string(),
                            "Status": "SUCCEEDED",
                        }
                    }))
                }
                None => ps_not_found(ps),
            }
        }
        "ListAccountsForProvisionedPermissionSet" => {
            let instance_arn = body_str(p, "InstanceArn");
            let ps = body_str(p, "PermissionSetArn");
            let provisioned =
                provisioned_for(st, instance_arn).is_some_and(|list| list.iter().any(|a| a == ps));
            let ids = if provisioned {
                vec![req.account.to_string()]
            } else {
                Vec::new()
            };
            ok(&json!({"AccountIds": ids}))
        }
        _ => {
            let e = json!({
                "__type": "NotImplemented",
                "message": format!("The {} action has not been implemented", req.operation),
            });
            Response::json(501, serde_json::to_vec(&e).unwrap_or_default())
        }
    }
}

/// moto's tag store: {Key: Value} per resource ARN (twin extension; moto's
/// ssoadmin responses carry no tag handlers).
fn store_tags(st: &mut State, arn: &str, tags: &Value) {
    let entry = st.tags.entry(arn.to_string()).or_insert_with(|| json!({}));
    for tag in tags.as_array().unwrap_or(&vec![]) {
        let key = tag.get("Key").and_then(Value::as_str).unwrap_or("");
        if key.is_empty() {
            continue;
        }
        entry.as_object_mut().unwrap().insert(
            key.to_string(),
            tag.get("Value").cloned().unwrap_or(Value::Null),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn req(op: &str) -> Request {
        Request {
            service: "ssoadmin".into(),
            operation: op.into(),
            method: "POST".into(),
            path: "/".into(),
            query_string: String::new(),
            headers: Default::default(),
            body: bytes::Bytes::new(),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    /// Every test takes a fresh account so the shared store stays isolated.
    fn fresh_state(account: u64) -> State {
        let mut st = State::default();
        with_default_instance(&mut st, account);
        st
    }

    fn body(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap()
    }

    fn instance_arn(st: &State) -> String {
        st.instances[0]["InstanceArn"].as_str().unwrap().to_string()
    }

    fn create_permission_set(st: &mut State, name: &str) -> String {
        let inst = instance_arn(st);
        let resp = handle(
            st,
            &json!({"Name": name, "InstanceArn": &inst}),
            &req("CreatePermissionSet"),
        );
        body(&resp)["PermissionSet"]["PermissionSetArn"]
            .as_str()
            .unwrap()
            .to_string()
    }

    /// The dataset (name -> path) parsed from the embedded JSON.
    fn dataset() -> &'static HashMap<String, String> {
        static POLICIES: Lazy<HashMap<String, String>> = Lazy::new(|| {
            serde_json::from_str(include_str!("ssoadmin_data/aws_managed_policies.json"))
                .unwrap_or_default()
        });
        &POLICIES
    }

    /// The exact Amazon-managed-policy ARN for `name` (dataset paths vary).
    fn policy_arn(name: &str) -> String {
        match dataset().get(name) {
            Some(path) => format!("arn:aws:iam::aws:policy{path}{name}"),
            None => format!("arn:aws:iam::aws:policy/{name}"),
        }
    }

    fn attach_managed(st: &mut State, inst: &str, ps: &str, policy: &str) -> Response {
        handle(
            st,
            &json!({
                "InstanceArn": inst,
                "PermissionSetArn": ps,
                "ManagedPolicyArn": policy
            }),
            &req("AttachManagedPolicyToPermissionSet"),
        )
    }

    fn list_managed(st: &mut State, inst: &str, ps: &str) -> Value {
        body(&handle(
            st,
            &json!({"InstanceArn": inst, "PermissionSetArn": ps}),
            &req("ListManagedPoliciesInPermissionSet"),
        ))
    }

    fn attach_customer(
        st: &mut State,
        inst: &str,
        ps: &str,
        name: &str,
        path: Option<&str>,
    ) -> Response {
        let mut reference = json!({"Name": name});
        if let Some(p) = path {
            reference["Path"] = json!(p);
        }
        handle(
            st,
            &json!({
                "InstanceArn": inst,
                "PermissionSetArn": ps,
                "CustomerManagedPolicyReference": reference
            }),
            &req("AttachCustomerManagedPolicyReferenceToPermissionSet"),
        )
    }

    #[test]
    fn instance_and_permission_sets() {
        let st = &mut fresh_state(111_000_000_001);
        let listed = body(&handle(st, &json!({}), &req("ListInstances")));
        assert_eq!(listed["Instances"][0]["Status"], "ACTIVE");
        let arn = instance_arn(st);

        let created = body(&handle(
            st,
            &json!({"Name": "ps", "InstanceArn": &arn, "Description": "d"}),
            &req("CreatePermissionSet"),
        ));
        let ps = created["PermissionSet"]["PermissionSetArn"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(ps.starts_with(&arn));

        let listed = body(&handle(
            st,
            &json!({"InstanceArn": &arn}),
            &req("ListPermissionSets"),
        ));
        assert_eq!(listed["PermissionSets"].as_array().unwrap().len(), 1);

        let assignment = body(&handle(
            st,
            &json!({
                "InstanceArn": &arn,
                "PermissionSetArn": &ps,
                "PrincipalId": "user-1",
                "PrincipalType": "USER",
                "TargetId": "123456789012",
                "TargetType": "AWS_ACCOUNT",
            }),
            &req("CreateAccountAssignment"),
        ));
        assert_eq!(
            assignment["AccountAssignmentCreationStatus"]["Status"],
            "SUCCEEDED"
        );

        handle(
            st,
            &json!({"InstanceArn": &arn, "PermissionSetArn": &ps}),
            &req("DeletePermissionSet"),
        );
        assert_eq!(st.permission_sets.len(), 0);
    }

    #[test]
    fn describe_and_update_permission_set() {
        let st = &mut fresh_state(111_000_000_002);
        let ps = create_permission_set(st, "ps");
        let inst = instance_arn(st);

        let described = body(&handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": &ps}),
            &req("DescribePermissionSet"),
        ));
        assert_eq!(described["PermissionSet"]["Name"], "ps");
        assert!(described["PermissionSet"]["CreatedDate"].is_number());

        // moto overwrites unconditionally on update: an omitted description
        // reads null and SessionDuration resets to 3600.
        handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": &ps, "SessionDuration": 900}),
            &req("UpdatePermissionSet"),
        );
        let described = body(&handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": &ps}),
            &req("DescribePermissionSet"),
        ));
        assert_eq!(described["PermissionSet"]["Description"], Value::Null);
        assert_eq!(described["PermissionSet"]["SessionDuration"], 900);
        assert_eq!(described["PermissionSet"]["RelayState"], Value::Null);

        // Unknown permission sets raise moto's message with the ARN tail.
        let missing = handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": format!("{inst}/ps-unknown")}),
            &req("DescribePermissionSet"),
        );
        assert_eq!(missing.status, 400);
        let e = body(&missing);
        assert_eq!(e["__type"], "ResourceNotFoundException");
        assert_eq!(
            e["message"],
            "Could not find PermissionSet with id ps-unknown"
        );
    }

    #[test]
    fn inline_policy_roundtrip() {
        let st = &mut fresh_state(111_000_000_003);
        let ps = create_permission_set(st, "ps");
        let inst = instance_arn(st);

        // moto defaults the inline policy to "".
        let got = body(&handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": &ps}),
            &req("GetInlinePolicyForPermissionSet"),
        ));
        assert_eq!(got["InlinePolicy"], "");

        let put = body(&handle(
            st,
            &json!({
                "InstanceArn": &inst,
                "PermissionSetArn": &ps,
                "InlinePolicy": "{\"Version\":\"2012-10-17\"}"
            }),
            &req("PutInlinePolicyToPermissionSet"),
        ));
        assert!(put.is_object());
        let got = body(&handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": &ps}),
            &req("GetInlinePolicyForPermissionSet"),
        ));
        assert_eq!(got["InlinePolicy"], "{\"Version\":\"2012-10-17\"}");

        // delete resets to "".
        handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": &ps}),
            &req("DeleteInlinePolicyFromPermissionSet"),
        );
        let got = body(&handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": &ps}),
            &req("GetInlinePolicyForPermissionSet"),
        ));
        assert_eq!(got["InlinePolicy"], "");

        // unknown permission set → moto's message with the ARN tail.
        let missing = handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": format!("{inst}/ps-unknown")}),
            &req("GetInlinePolicyForPermissionSet"),
        );
        assert_eq!(missing.status, 400);
        let e = body(&missing);
        assert_eq!(e["__type"], "ResourceNotFoundException");
        assert_eq!(
            e["message"],
            "Could not find PermissionSet with id ps-unknown"
        );
    }

    #[test]
    fn managed_policies_roundtrip() {
        let st = &mut fresh_state(111_000_000_004);
        let ps = create_permission_set(st, "ps");
        let inst = instance_arn(st);

        // A real AWS managed policy attaches.
        let arn = "arn:aws:iam::aws:policy/AdministratorAccess";
        assert_eq!(attach_managed(st, &inst, &ps, arn).status, 200);
        let list = list_managed(st, &inst, &ps);
        let policies = list["AttachedManagedPolicies"].as_array().unwrap();
        assert_eq!(policies.len(), 1);
        assert_eq!(policies[0]["Name"], "AdministratorAccess");
        assert_eq!(policies[0]["Arn"], arn);

        // A second attach of the same policy conflicts (moto's message).
        let dup = attach_managed(st, &inst, &ps, arn);
        assert_eq!(dup.status, 400);
        let e = body(&dup);
        assert_eq!(e["__type"], "ConflictException");
        assert_eq!(
            e["message"],
            format!(
                "Permission set with id {} already has a typed link attachment to a manged policy with {arn}",
                ps_id(&ps)
            )
        );

        // Detaching an un-attached policy → moto's message.
        let missing = attach_detached(st, &inst, &ps);
        assert_eq!(missing.status, 400);
        let e = body(&missing);
        assert_eq!(e["__type"], "ResourceNotFoundException");
        assert_eq!(
            e["message"],
            "Could not find ManagedPolicy with arn arn:aws:iam::aws:policy/ViewOnlyAccess"
        );

        // Unknown policy names fail validation (moto's message).
        let bogus = attach_managed(st, &inst, &ps, "arn:aws:iam::aws:policy/NoSuchPolicy");
        assert_eq!(bogus.status, 400);
        let e = body(&bogus);
        assert_eq!(
            e["message"],
            "Policy does not exist with ARN: arn:aws:iam::aws:policy/NoSuchPolicy"
        );

        // Detach the attached policy and the list empties.
        let detached = handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": &ps, "ManagedPolicyArn": arn}),
            &req("DetachManagedPolicyFromPermissionSet"),
        );
        assert_eq!(detached.status, 200);
        let list = list_managed(st, &inst, &ps);
        assert_eq!(list["AttachedManagedPolicies"].as_array().unwrap().len(), 0);
    }

    fn attach_detached(st: &mut State, inst: &str, ps: &str) -> Response {
        handle(
            st,
            &json!({
                "InstanceArn": inst,
                "PermissionSetArn": ps,
                "ManagedPolicyArn": "arn:aws:iam::aws:policy/ViewOnlyAccess"
            }),
            &req("DetachManagedPolicyFromPermissionSet"),
        )
    }

    #[test]
    fn managed_policy_quota() {
        let st = &mut fresh_state(111_000_000_005);
        let ps = create_permission_set(st, "ps");
        let inst = instance_arn(st);

        // Names from the embedded dataset, sorted for determinism.
        let names: Vec<String> = {
            let mut names: Vec<String> = dataset().keys().cloned().collect();
            names.sort();
            names.truncate(21);
            names
        };
        for name in &names[..20] {
            let arn = policy_arn(name);
            let resp = attach_managed(st, &inst, &ps, &arn);
            assert_eq!(resp.status, 200, "attach {arn}");
        }
        // The 21st attach raises ServiceQuotaExceededException.
        let overrun = attach_managed(st, &inst, &ps, &policy_arn(&names[20]));
        assert_eq!(overrun.status, 400);
        let e = body(&overrun);
        assert_eq!(e["__type"], "ServiceQuotaExceededException");
        assert_eq!(
            e["message"],
            format!(
                "You have exceeded AWS SSO limits. Cannot create ManagedPolicy more than 20 for id {}. Please refer to https://docs.aws.amazon.com/singlesignon/latest/userguide/limits.html",
                ps_id(&ps)
            )
        );
    }

    #[test]
    fn customer_managed_policies_roundtrip() {
        let st = &mut fresh_state(111_000_000_006);
        let ps = create_permission_set(st, "ps");
        let inst = instance_arn(st);

        // attach without Path — moto defaults the path to "/".
        assert_eq!(
            attach_customer(st, &inst, &ps, "TeamPolicy", None).status,
            200
        );
        // A second reference with an explicit path is a distinct policy.
        assert_eq!(
            attach_customer(st, &inst, &ps, "TeamPolicy", Some("/team/")).status,
            200
        );
        let list = body(&handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": &ps}),
            &req("ListCustomerManagedPolicyReferencesInPermissionSet"),
        ));
        let references = list["CustomerManagedPolicyReferences"].as_array().unwrap();
        assert_eq!(references.len(), 2);
        assert_eq!(references[0]["Path"], "/");
        assert_eq!(references[1]["Path"], "/team/");

        // duplicate attach of the default-path policy conflicts.
        let dup = attach_customer(st, &inst, &ps, "TeamPolicy", None);
        assert_eq!(dup.status, 400);
        let e = body(&dup);
        assert_eq!(e["__type"], "ConflictException");
        assert_eq!(
            e["message"],
            "Given customer managed policy with name: TeamPolicy  and path / already attached"
        );

        // detach without Path hits the default path.
        let detached = handle(
            st,
            &json!({
                "InstanceArn": &inst,
                "PermissionSetArn": &ps,
                "CustomerManagedPolicyReference": {"Name": "TeamPolicy"}
            }),
            &req("DetachCustomerManagedPolicyReferenceFromPermissionSet"),
        );
        assert_eq!(detached.status, 200);

        // detaching again → moto's message.
        let missing = handle(
            st,
            &json!({
                "InstanceArn": &inst,
                "PermissionSetArn": &ps,
                "CustomerManagedPolicyReference": {"Name": "TeamPolicy"}
            }),
            &req("DetachCustomerManagedPolicyReferenceFromPermissionSet"),
        );
        assert_eq!(missing.status, 400);
        let e = body(&missing);
        assert_eq!(e["__type"], "ResourceNotFoundException");
        assert_eq!(
            e["message"],
            "Given managed policy with name: TeamPolicy  and path / does not exist on PermissionSet"
        );
    }

    #[test]
    fn account_assignment_statuses() {
        let st = &mut fresh_state(111_000_000_007);
        let ps = create_permission_set(st, "ps");
        let inst = instance_arn(st);
        let params = json!({
            "InstanceArn": &inst,
            "PermissionSetArn": &ps,
            "PrincipalId": "user-1",
            "PrincipalType": "USER",
            "TargetId": "123456789012",
            "TargetType": "AWS_ACCOUNT",
        });

        let created = body(&handle(st, &params, &req("CreateAccountAssignment")));
        let status = &created["AccountAssignmentCreationStatus"];
        assert_eq!(status["Status"], "SUCCEEDED");
        let request_id = status["RequestId"].as_str().unwrap().to_string();
        assert!(status["CreatedDate"].is_number());

        // the stored request id resolves through the Describe op.
        let described = body(&handle(
            st,
            &json!({"InstanceArn": &inst, "AccountAssignmentCreationRequestId": &request_id}),
            &req("DescribeAccountAssignmentCreationStatus"),
        ));
        assert_eq!(
            described["AccountAssignmentCreationStatus"]["Status"],
            "SUCCEEDED"
        );
        assert_eq!(
            described["AccountAssignmentCreationStatus"]["PrincipalId"],
            "user-1"
        );

        // unknown request ids → moto's empty-message ResourceNotFoundException.
        let missing = handle(
            st,
            &json!({"InstanceArn": &inst, "AccountAssignmentCreationRequestId": "nope"}),
            &req("DescribeAccountAssignmentCreationStatus"),
        );
        assert_eq!(missing.status, 400);
        let e = body(&missing);
        assert_eq!(e["__type"], "ResourceNotFoundException");
        assert_eq!(e["message"], "");

        // delete reports the deletion status and archives the request id.
        let deleted = body(&handle(st, &params, &req("DeleteAccountAssignment")));
        assert_eq!(
            deleted["AccountAssignmentDeletionStatus"]["Status"],
            "SUCCEEDED"
        );
        let del_request_id = deleted["AccountAssignmentDeletionStatus"]["RequestId"]
            .as_str()
            .unwrap()
            .to_string();
        let described = body(&handle(
            st,
            &json!({"InstanceArn": &inst, "AccountAssignmentDeletionRequestId": &del_request_id}),
            &req("DescribeAccountAssignmentDeletionStatus"),
        ));
        assert_eq!(
            described["AccountAssignmentDeletionStatus"]["Status"],
            "SUCCEEDED"
        );
        assert_eq!(
            described["AccountAssignmentDeletionStatus"]["TargetId"],
            "123456789012"
        );

        // deleting again — nothing matched → ResourceNotFoundException.
        let missing = handle(st, &params, &req("DeleteAccountAssignment"));
        assert_eq!(missing.status, 400);
        let e = body(&missing);
        assert_eq!(e["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn provisioning_records() {
        let st = &mut fresh_state(111_000_000_008);
        let ps = create_permission_set(st, "ps");
        let inst = instance_arn(st);

        // moto ignores TargetId/TargetType and provisions to the caller.
        let provisioned = body(&handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": &ps, "TargetId": "ignored"}),
            &req("ProvisionPermissionSet"),
        ));
        let status = &provisioned["PermissionSetProvisioningStatus"];
        assert_eq!(status["Status"], "SUCCEEDED");
        assert_eq!(status["PermissionSetArn"], ps);
        assert_eq!(
            status["AccountId"],
            req("ListInstances").account.to_string()
        );

        let listed = body(&handle(
            st,
            &json!({"InstanceArn": &inst}),
            &req("ListPermissionSetsProvisionedToAccount"),
        ));
        assert_eq!(listed["PermissionSets"].as_array().unwrap().len(), 1);

        let accounts = body(&handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": &ps}),
            &req("ListAccountsForProvisionedPermissionSet"),
        ));
        assert_eq!(
            accounts["AccountIds"],
            json!([req("ListInstances").account.to_string()])
        );

        // an unprovisioned permission set lists no accounts.
        let ps2 = create_permission_set(st, "ps2");
        let accounts = body(&handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": &ps2}),
            &req("ListAccountsForProvisionedPermissionSet"),
        ));
        assert_eq!(accounts["AccountIds"].as_array().unwrap().len(), 0);

        // deleting removes the provisioning record.
        handle(
            st,
            &json!({"InstanceArn": &inst, "PermissionSetArn": &ps}),
            &req("DeletePermissionSet"),
        );
        let listed = body(&handle(
            st,
            &json!({"InstanceArn": &inst}),
            &req("ListPermissionSetsProvisionedToAccount"),
        ));
        assert_eq!(listed["PermissionSets"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn update_instance() {
        let st = &mut fresh_state(111_000_000_009);
        let inst = instance_arn(st);

        handle(
            st,
            &json!({"InstanceArn": &inst, "Name": "renamed"}),
            &req("UpdateInstance"),
        );
        let listed = body(&handle(st, &json!({}), &req("ListInstances")));
        assert_eq!(listed["Instances"][0]["Name"], "renamed");

        // unknown instance arn → silent no-op, no error.
        handle(
            st,
            &json!({"InstanceArn": "arn:aws:sso:::instance/ssoins-none", "Name": "x"}),
            &req("UpdateInstance"),
        );
        let listed = body(&handle(st, &json!({}), &req("ListInstances")));
        assert_eq!(listed["Instances"][0]["Name"], "renamed");
    }
}
