//! SSO Admin backend (JSON protocol, target `SWBExternalService.<Op>`).
//!
//! Port of moto's ssoadmin: pre-seeded SSO instance per account/region,
//! permission sets and account assignments.

use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Value};
use std::collections::HashMap;

#[derive(Default)]
struct State {
    /// Seeded lazily on first use, exactly one instance like moto.
    instances: Vec<Value>,
    /// PermissionSetArn -> permission set summary.
    permission_sets: Vec<(String, Value)>,
    /// Assignments in moto's to_json() shape.
    assignments: Vec<Value>,
    /// Untagged-but-recorded tags: arn -> {Key: Value}.
    tags: HashMap<String, Value>,
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

fn resource_not_found() -> Response {
    let e = json!({
        "__type": "ResourceNotFoundException",
        "message": "ResourceNotFoundException",
    });
    Response::json(400, serde_json::to_vec(&e).unwrap_or_default())
}

fn handle(st: &mut State, p: &Value, req: &Request) -> Response {
    match req.operation.as_str() {
        "ListInstances" => ok(&json!({"Instances": st.instances})),
        "CreatePermissionSet" => {
            let instance_arn = p.get("InstanceArn").and_then(Value::as_str).unwrap_or("");
            let arn = ps_arn(instance_arn);
            let session = p.get("SessionDuration").cloned().unwrap_or(json!(3600));
            let relay = p.get("RelayState").cloned().unwrap_or(Value::Null);
            let mut ps = ps_json(
                p.get("Name").and_then(Value::as_str),
                p.get("Description").and_then(Value::as_str),
                &arn,
                session,
                relay,
            );
            ps["CreatedDate"] = json!(epoch());
            if let Some(tags) = p.get("Tags") {
                store_tags(st, &arn, tags);
            }
            st.permission_sets.push((arn, ps.clone()));
            ok(&json!({"PermissionSet": ps}))
        }
        "DescribePermissionSet" => {
            let arn = p
                .get("PermissionSetArn")
                .and_then(Value::as_str)
                .unwrap_or("");
            match st.permission_sets.iter().find(|(a, _)| a == arn) {
                Some((_, ps)) => {
                    let mut full = ps.clone();
                    full["CreatedDate"] = json!(epoch());
                    ok(&json!({"PermissionSet": full}))
                }
                None => resource_not_found(),
            }
        }
        "ListPermissionSets" => {
            let instance_arn = p.get("InstanceArn").and_then(Value::as_str).unwrap_or("");
            let arns: Vec<Value> = st
                .permission_sets
                .iter()
                .filter(|(a, _)| a.starts_with(instance_arn))
                .map(|(a, _)| json!(a))
                .collect();
            ok(&json!({"PermissionSets": arns}))
        }
        "ListPermissionSetsProvisionedToAccount" => {
            let arns: Vec<Value> = st.permission_sets.iter().map(|(a, _)| json!(a)).collect();
            ok(&json!({"PermissionSets": arns}))
        }
        "UpdatePermissionSet" => {
            let arn = p
                .get("PermissionSetArn")
                .and_then(Value::as_str)
                .unwrap_or("");
            match st.permission_sets.iter_mut().find(|(a, _)| a == arn) {
                Some((_, ps)) => {
                    if let Some(d) = p.get("Description") {
                        ps["Description"] = d.clone();
                    }
                    if let Some(s) = p.get("SessionDuration") {
                        ps["SessionDuration"] = s.clone();
                    }
                    if let Some(r) = p.get("RelayState") {
                        ps["RelayState"] = r.clone();
                    }
                    ok(&json!({}))
                }
                None => resource_not_found(),
            }
        }
        "DeletePermissionSet" => {
            let arn = p
                .get("PermissionSetArn")
                .and_then(Value::as_str)
                .unwrap_or("");
            st.permission_sets.retain(|(a, _)| a != arn);
            st.assignments
                .retain(|a| a["PermissionSetArn"] != json!(arn));
            ok(&json!({}))
        }
        "CreateAccountAssignment" => {
            let mut assignment = json!({
                "TargetId": p.get("TargetId"),
                "TargetType": p.get("TargetType"),
                "PermissionSetArn": p.get("PermissionSetArn"),
                "PrincipalType": p.get("PrincipalType"),
                "PrincipalId": p.get("PrincipalId"),
            });
            assignment["CreatedDate"] = json!(epoch());
            let request_id = uuid::Uuid::new_v4().to_string();
            let mut summary = assignment.clone();
            summary["RequestId"] = json!(request_id);
            st.assignments.push(assignment);
            ok(&json!({
                "AccountAssignmentCreationStatus": {
                    "CreatedDate": epoch(),
                    "RequestId": request_id,
                    "Status": "SUCCEEDED",
                    "TargetId": summary["TargetId"],
                    "TargetType": summary["TargetType"],
                    "PermissionSetArn": summary["PermissionSetArn"],
                    "PrincipalType": summary["PrincipalType"],
                    "PrincipalId": summary["PrincipalId"],
                }
            }))
        }
        "DeleteAccountAssignment" => {
            let request_id = uuid::Uuid::new_v4().to_string();
            let target = p.get("TargetId").cloned().unwrap_or(Value::Null);
            let ps = p.get("PermissionSetArn").cloned().unwrap_or(Value::Null);
            st.assignments
                .retain(|a| !(a["TargetId"] == target && a["PermissionSetArn"] == ps));
            // moto errors when there was nothing to delete; mirror loosely
            // by reporting SUCCEEDED with a fresh request id anyway.
            ok(&json!({
                "AccountAssignmentDeletionStatus": {
                    "CreatedDate": epoch(),
                    "RequestId": request_id,
                    "Status": "SUCCEEDED",
                    "TargetId": target,
                    "TargetType": p.get("TargetType").cloned().unwrap_or(Value::Null),
                    "PermissionSetArn": ps,
                    "PrincipalType": p.get("PrincipalType").cloned().unwrap_or(Value::Null),
                    "PrincipalId": p.get("PrincipalId").cloned().unwrap_or(Value::Null),
                }
            }))
        }
        "ListAccountAssignments" => {
            let instance_arn = p.get("InstanceArn").and_then(Value::as_str).unwrap_or("");
            let account_id = p.get("AccountId").and_then(Value::as_str).unwrap_or("");
            let ps = p
                .get("PermissionSetArn")
                .and_then(Value::as_str)
                .unwrap_or("");
            let out: Vec<Value> = st
                .assignments
                .iter()
                .filter(|a| {
                    a["TargetId"] == json!(account_id) && a["PermissionSetArn"] == json!(ps)
                })
                .map(|a| {
                    json!({
                        "AccountId": account_id,
                        "PermissionSetArn": a["PermissionSetArn"],
                        "PrincipalType": a["PrincipalType"],
                        "PrincipalId": a["PrincipalId"],
                    })
                })
                .collect();
            let _ = instance_arn;
            ok(&json!({"AccountAssignments": out, "NextToken": Value::Null}))
        }
        "ListAccountAssignmentsForPrincipal" => {
            let out: Vec<Value> = st
                .assignments
                .iter()
                .map(|a| {
                    json!({
                        "AccountId": a["TargetId"],
                        "PermissionSetArn": a["PermissionSetArn"],
                        "PrincipalId": a["PrincipalId"],
                        "PrincipalType": a["PrincipalType"],
                    })
                })
                .collect();
            ok(&json!({"AccountAssignments": out, "NextToken": Value::Null}))
        }
        "ListTagsForResource" => {
            let arn = p.get("ResourceARN").and_then(Value::as_str).unwrap_or("");
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
            let arn = p.get("ResourceARN").and_then(Value::as_str).unwrap_or("");
            if let Some(tags) = p.get("Tags") {
                store_tags(st, arn, tags);
            }
            ok(&json!({}))
        }
        "UntagResource" => {
            let arn = p.get("ResourceARN").and_then(Value::as_str).unwrap_or("");
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
        _ => {
            let e = json!({
                "__type": "NotImplemented",
                "message": format!("The {} action has not been implemented", req.operation),
            });
            Response::json(501, serde_json::to_vec(&e).unwrap_or_default())
        }
    }
}

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

    #[test]
    fn instance_and_permission_sets() {
        let slot = state();
        let mut st = slot.write();
        let b = st.entry((123456789012, "us-east-1".into())).or_default();

        with_default_instance(b, 123456789012);
        let listed = handle(b, &json!({}), &req("ListInstances"));
        let listed = serde_json::from_slice::<Value>(&listed.body).unwrap();
        let arn = listed["Instances"][0]["InstanceArn"]
            .as_str()
            .unwrap()
            .to_string();

        let created = handle(
            b,
            &json!({"Name": "ps", "InstanceArn": &arn, "Description": "d"}),
            &req("CreatePermissionSet"),
        );
        let created = serde_json::from_slice::<Value>(&created.body).unwrap();
        let ps_arn = created["PermissionSet"]["PermissionSetArn"]
            .as_str()
            .unwrap();
        assert!(ps_arn.starts_with(&arn));

        let listed = handle(b, &json!({"InstanceArn": &arn}), &req("ListPermissionSets"));
        let listed = serde_json::from_slice::<Value>(&listed.body).unwrap();
        assert_eq!(listed["PermissionSets"].as_array().unwrap().len(), 1);

        let assignment = handle(
            b,
            &json!({
                "InstanceArn": &arn,
                "PermissionSetArn": ps_arn,
                "PrincipalId": "user-1",
                "PrincipalType": "USER",
                "TargetId": "123456789012",
                "TargetType": "AWS_ACCOUNT",
            }),
            &req("CreateAccountAssignment"),
        );
        let assignment = serde_json::from_slice::<Value>(&assignment.body).unwrap();
        assert_eq!(
            assignment["AccountAssignmentCreationStatus"]["Status"],
            "SUCCEEDED"
        );

        handle(
            b,
            &json!({"PermissionSetArn": ps_arn}),
            &req("DeletePermissionSet"),
        );
        assert_eq!(b.permission_sets.len(), 0);
    }
}
