//! IdentityStore backend (JSON protocol, X-Amz-Target dispatched).
//!
//! Port of moto's identity-store: users, groups and group memberships
//! held per (account, region, IdentityStoreId-stores). Wire shapes and
//! validation errors mirror moto's models/exceptions.

use crate::proto::parse_json_body;
use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Map, Value};
use std::collections::HashMap;

#[derive(Default)]
struct StoreData {
    groups: Vec<(String, Value)>,      // (GroupId, group record)
    users: Vec<(String, Value)>,       // (UserId, user record)
    memberships: Vec<(String, Value)>, // (MembershipId, membership record)
}

#[derive(Default)]
struct Backend {
    stores: HashMap<String, StoreData>,
}

fn state() -> store::Slot<Backend> {
    static SLOT: Lazy<store::Slot<Backend>> = Lazy::new(|| store::slot("identitystore"));
    SLOT.clone()
}

const REQUEST_ID: &str = "178936da-50ad-4d58-8871-22d9979e8658example";

/// moto's IdentityStoreError JSON body: `__type` + Message + ResourceType
/// + Reason, always status 400.
fn ids_error(
    code: &str,
    message: &str,
    resource_type: Option<&str>,
    reason: Option<&str>,
) -> Response {
    let body = json!({
        "__type": code,
        "RequestId": REQUEST_ID,
        "Message": message,
        "ResourceType": resource_type,
        "Reason": reason,
    });
    Response::json(400, serde_json::to_vec(&body).unwrap_or_default())
}

fn not_found(message: &str, resource_type: &str) -> Response {
    ids_error(
        "ResourceNotFoundException",
        message,
        Some(resource_type),
        None,
    )
}

fn validation(message: &str) -> Response {
    ids_error("ValidationException", message, None, None)
}

fn conflict(message: &str, reason: &str) -> Response {
    ids_error("ConflictException", message, None, Some(reason))
}

fn ok_json(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

pub fn dispatch(req: &Request) -> Option<Response> {
    let p = parse_json_body(&req.body);
    let slot = state();
    let mut st = slot.write();
    let b = st.entry((req.account, req.region.clone())).or_default();
    handle(req.operation.as_str(), b, &p)
}

fn handle(op: &str, b: &mut Backend, p: &Value) -> Option<Response> {
    match op {
        "CreateGroup" => Some(create_group(b, p)),
        "CreateGroupMembership" => Some(create_group_membership(b, p)),
        "CreateUser" => Some(create_user(b, p)),
        "DeleteGroup" => Some(delete_group(b, p)),
        "DeleteGroupMembership" => Some(delete_group_membership(b, p)),
        "DeleteUser" => Some(delete_user(b, p)),
        "DescribeGroup" => Some(describe_group(b, p)),
        "DescribeGroupMembership" => Some(describe_group_membership(b, p)),
        "DescribeUser" => Some(describe_user(b, p)),
        "GetGroupId" => Some(get_group_id(b, p)),
        "GetGroupMembershipId" => Some(get_group_membership_id(b, p)),
        "GetUserId" => Some(get_user_id(b, p)),
        "IsMemberInGroups" => Some(is_member_in_groups(b, p)),
        "ListGroupMemberships" => Some(list_group_memberships(b, p)),
        "ListGroupMembershipsForMember" => Some(list_group_memberships_for_member(b, p)),
        "ListGroups" => Some(list_groups(b, p)),
        "ListUsers" => Some(list_users(b, p)),
        "UpdateGroup" => Some(update_group(b, p)),
        "UpdateUser" => Some(update_user(b, p)),
        _ => None,
    }
}

fn store<'a>(b: &'a mut Backend, store_id: &str) -> &'a mut StoreData {
    b.stores.entry(store_id.to_string()).or_default()
}

fn find<'a>(records: &'a [(String, Value)], id: &str) -> Option<&'a Value> {
    records.iter().find(|(k, _)| *k == id).map(|(_, v)| v)
}

fn records_with(records: &[(String, Value)], pred: impl Fn(&Value) -> bool + Copy) -> Vec<Value> {
    records
        .iter()
        .filter(|(_, v)| pred(v))
        .map(|(_, v)| v.clone())
        .collect()
}

/// Normalize a `Name` member the way moto's NamedTuple renders it: the six
/// canonical fields with nulls where the caller omitted them.
fn name_record(v: &Value) -> Value {
    json!({
        "Formatted": v["Formatted"].clone(),
        "FamilyName": v["FamilyName"].clone(),
        "GivenName": v["GivenName"].clone(),
        "MiddleName": v["MiddleName"].clone(),
        "HonorificPrefix": v["HonorificPrefix"].clone(),
        "HonorificSuffix": v["HonorificSuffix"].clone(),
    })
}

fn create_group(b: &mut Backend, p: &Value) -> Response {
    let store_id = p["IdentityStoreId"].as_str().unwrap_or_default();
    let display_name = p["DisplayName"].as_str().unwrap_or_default();
    let st = store(b, store_id);
    if st
        .groups
        .iter()
        .any(|(_, g)| g["DisplayName"].as_str() == Some(display_name))
    {
        return conflict(
            "Duplicate GroupDisplayName",
            "UNIQUENESS_CONSTRAINT_VIOLATION",
        );
    }
    let group_id = uuid::Uuid::new_v4().to_string();
    st.groups.push((
        group_id.clone(),
        json!({
            "GroupId": group_id,
            "DisplayName": display_name,
            "ExternalIds": [],
            "Description": p["Description"].clone(),
            "IdentityStoreId": store_id,
        }),
    ));
    ok_json(&json!({"GroupId": group_id, "IdentityStoreId": store_id}))
}

fn describe_group(b: &mut Backend, p: &Value) -> Response {
    let store_id = p["IdentityStoreId"].as_str().unwrap_or_default();
    let group_id = p["GroupId"].as_str().unwrap_or_default();
    let st = store(b, store_id);
    match find(&st.groups, group_id).cloned() {
        Some(g) => ok_json(&g),
        None => not_found("GROUP not found.", "GROUP"),
    }
}

fn delete_group(b: &mut Backend, p: &Value) -> Response {
    let store_id = p["IdentityStoreId"].as_str().unwrap_or_default();
    let group_id = p["GroupId"].as_str().unwrap_or_default().to_string();
    store(b, store_id).groups.retain(|(k, _)| *k != group_id);
    ok_json(&json!({}))
}

fn create_user(b: &mut Backend, p: &Value) -> Response {
    let store_id = p["IdentityStoreId"].as_str().unwrap_or_default();
    let user_name = p["UserName"].as_str().unwrap_or_default().to_string();

    if user_name.is_empty() {
        return validation("userName is a required attribute");
    }
    // Required-attribute validation mirrors __validate_create_user.
    let mut missing: Vec<String> = Vec::new();
    if p["DisplayName"].is_null() {
        missing.push("displayname".into());
    }
    if p["Name"].is_null() {
        missing.push("name".into());
    } else {
        if p["Name"]["GivenName"].is_null() {
            missing.push("givenname".into());
        }
        if p["Name"]["FamilyName"].is_null() {
            missing.push("familyname".into());
        }
    }
    if !missing.is_empty() {
        let msg = missing
            .iter()
            .map(|att| format!("{att}: The attribute {att} is required"))
            .collect::<Vec<_>>()
            .join(", ");
        return validation(&msg);
    }

    let st = store(b, store_id);
    if st
        .users
        .iter()
        .any(|(_, u)| u["UserName"].as_str() == Some(user_name.as_str()))
    {
        return conflict("Duplicate UserName", "UNIQUENESS_CONSTRAINT_VIOLATION");
    }

    let user_id = uuid::Uuid::new_v4().to_string();
    let user = json!({
        "UserId": user_id,
        "IdentityStoreId": store_id,
        "UserName": user_name,
        "Name": name_record(&p["Name"]),
        "DisplayName": p["DisplayName"].clone(),
        "NickName": p["NickName"].clone(),
        "ProfileUrl": p["ProfileUrl"].clone(),
        "Emails": p["Emails"].clone(),
        "Addresses": p["Addresses"].clone(),
        "PhoneNumbers": p["PhoneNumbers"].clone(),
        "UserType": p["UserType"].clone(),
        "Title": p["Title"].clone(),
        "PreferredLanguage": p["PreferredLanguage"].clone(),
        "Locale": p["Locale"].clone(),
        "Timezone": p["Timezone"].clone(),
    });
    st.users.push((user_id.clone(), user.clone()));
    ok_json(&json!({"UserId": user_id, "IdentityStoreId": store_id}))
}

fn describe_user(b: &mut Backend, p: &Value) -> Response {
    let store_id = p["IdentityStoreId"].as_str().unwrap_or_default();
    let user_id = p["UserId"].as_str().unwrap_or_default();
    let st = store(b, store_id);
    match find(&st.users, user_id).cloned() {
        Some(u) => ok_json(&u),
        None => not_found("USER not found.", "USER"),
    }
}

fn delete_user(b: &mut Backend, p: &Value) -> Response {
    let store_id = p["IdentityStoreId"].as_str().unwrap_or_default();
    let user_id = p["UserId"].as_str().unwrap_or_default().to_string();
    store(b, store_id).users.retain(|(k, _)| *k != user_id);
    ok_json(&json!({}))
}

fn create_group_membership(b: &mut Backend, p: &Value) -> Response {
    let store_id = p["IdentityStoreId"].as_str().unwrap_or_default();
    let group_id = p["GroupId"].as_str().unwrap_or_default().to_string();
    let user_id = p["MemberId"]["UserId"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let st = store(b, store_id);
    if st.users.iter().all(|(k, _)| k.as_str() != user_id.as_str()) {
        return not_found("Member does not exist", "USER");
    }
    if st
        .groups
        .iter()
        .all(|(k, _)| k.as_str() != group_id.as_str())
    {
        return not_found("Group does not exist", "GROUP");
    }
    let membership_id = uuid::Uuid::new_v4().to_string();
    st.memberships.push((
        membership_id.clone(),
        json!({
            "IdentityStoreId": store_id,
            "MembershipId": membership_id,
            "GroupId": group_id,
            "MemberId": {"UserId": user_id},
        }),
    ));
    ok_json(&json!({"MembershipId": membership_id, "IdentityStoreId": store_id}))
}

fn describe_group_membership(b: &mut Backend, p: &Value) -> Response {
    let store_id = p["IdentityStoreId"].as_str().unwrap_or_default();
    let membership_id = p["MembershipId"].as_str().unwrap_or_default();
    let st = store(b, store_id);
    match find(&st.memberships, membership_id).cloned() {
        Some(m) => ok_json(&m),
        None => not_found("MEMBERSHIP not found.", "GROUP_MEMBERSHIP"),
    }
}

fn delete_group_membership(b: &mut Backend, p: &Value) -> Response {
    let store_id = p["IdentityStoreId"].as_str().unwrap_or_default();
    let membership_id = p["MembershipId"].as_str().unwrap_or_default().to_string();
    store(b, store_id)
        .memberships
        .retain(|(k, _)| *k != membership_id);
    ok_json(&json!({}))
}

fn list_group_memberships(b: &mut Backend, p: &Value) -> Response {
    let store_id = p["IdentityStoreId"].as_str().unwrap_or_default();
    let group_id = p["GroupId"].as_str().unwrap_or_default();
    let st = store(b, store_id);
    let items = records_with(&st.memberships, |m| m["GroupId"].as_str() == Some(group_id));
    ok_json(&json!({"GroupMemberships": items, "NextToken": Value::Null}))
}

fn list_group_memberships_for_member(b: &mut Backend, p: &Value) -> Response {
    let store_id = p["IdentityStoreId"].as_str().unwrap_or_default();
    let user_id = p["MemberId"]["UserId"].as_str().unwrap_or_default();
    let st = store(b, store_id);
    let items = records_with(&st.memberships, |m| {
        m["MemberId"]["UserId"].as_str() == Some(user_id)
    });
    ok_json(&json!({"GroupMemberships": items, "NextToken": Value::Null}))
}

fn list_groups(b: &mut Backend, p: &Value) -> Response {
    let store_id = p["IdentityStoreId"].as_str().unwrap_or_default();
    let st = store(b, store_id);
    let items = match p["Filters"][0]["AttributePath"].as_str() {
        Some("DisplayName") => {
            let want = p["Filters"][0]["AttributeValue"]
                .as_str()
                .unwrap_or_default();
            records_with(&st.groups, |g| g["DisplayName"].as_str() == Some(want))
        }
        _ => records_with(&st.groups, |_| true),
    };
    ok_json(&json!({"Groups": items, "NextToken": Value::Null}))
}

fn list_users(b: &mut Backend, p: &Value) -> Response {
    let store_id = p["IdentityStoreId"].as_str().unwrap_or_default();
    let st = store(b, store_id);
    let items = match p["Filters"][0]["AttributePath"].as_str() {
        Some("UserName") => {
            let want = p["Filters"][0]["AttributeValue"]
                .as_str()
                .unwrap_or_default();
            records_with(&st.users, |u| u["UserName"].as_str() == Some(want))
        }
        _ => records_with(&st.users, |_| true),
    };
    ok_json(&json!({"Users": items, "NextToken": Value::Null}))
}

fn update_group(b: &mut Backend, p: &Value) -> Response {
    let store_id = p["IdentityStoreId"].as_str().unwrap_or_default();
    let group_id = p["GroupId"].as_str().unwrap_or_default().to_string();
    let st = store(b, store_id);
    let Some(idx) = st.groups.iter().position(|(k, _)| *k == group_id) else {
        return not_found("GROUP not found.", "GROUP");
    };
    let mut g = st.groups[idx].1.clone();
    if let Some(ops) = p["Operations"].as_array() {
        for op in ops {
            let path = op["AttributePath"]
                .as_str()
                .unwrap_or_default()
                .to_lowercase();
            match path.as_str() {
                "displayname" => g["DisplayName"] = op["AttributeValue"].clone(),
                "description" => g["Description"] = op["AttributeValue"].clone(),
                _ => {}
            }
        }
    }
    st.groups[idx].1 = g;
    ok_json(&json!({}))
}

fn update_user(b: &mut Backend, p: &Value) -> Response {
    let store_id = p["IdentityStoreId"].as_str().unwrap_or_default();
    let user_id = p["UserId"].as_str().unwrap_or_default().to_string();
    let st = store(b, store_id);
    let Some(idx) = st.users.iter().position(|(k, _)| *k == user_id) else {
        return not_found("USER not found.", "USER");
    };
    let mut u: Map<String, Value> = st.users[idx].1.as_object().cloned().unwrap_or_default();
    if let Some(ops) = p["Operations"].as_array() {
        for op in ops {
            let path = op["AttributePath"].as_str().unwrap_or_default();
            if let Some(key) = u
                .keys()
                .find(|k| k.to_lowercase() == path.to_lowercase())
                .cloned()
            {
                u.insert(key, op["AttributeValue"].clone());
            }
        }
    }
    if u.get("Name").map(Value::is_object).unwrap_or(false) {
        let name = name_record(u.get("Name").unwrap_or(&Value::Null));
        u.insert("Name".into(), name);
    }
    st.users[idx].1 = Value::Object(u);
    ok_json(&json!({}))
}

fn get_group_id(b: &mut Backend, p: &Value) -> Response {
    let store_id = p["IdentityStoreId"].as_str().unwrap_or_default();
    let st = store(b, store_id);
    let unique = &p["AlternateIdentifier"]["UniqueAttribute"];
    if let Some(path) = unique["AttributePath"].as_str() {
        if path.to_lowercase() == "displayname" {
            let value = unique["AttributeValue"].as_str().unwrap_or_default();
            for (_, g) in st.groups.iter() {
                if g["DisplayName"].as_str() == Some(value) {
                    return ok_json(&json!({
                        "GroupId": g["GroupId"],
                        "IdentityStoreId": store_id,
                    }));
                }
            }
        }
    }
    not_found("GROUP not found.", "GROUP")
}

fn get_user_id(b: &mut Backend, p: &Value) -> Response {
    let store_id = p["IdentityStoreId"].as_str().unwrap_or_default();
    let st = store(b, store_id);
    let unique = &p["AlternateIdentifier"]["UniqueAttribute"];
    if let Some(path) = unique["AttributePath"].as_str() {
        let value = unique["AttributeValue"].as_str().unwrap_or_default();
        if value.is_empty() {
            return validation("attribute value cannot be empty or null for attribute path");
        }
        let path = path.to_lowercase();
        if path != "username" && path != "emails.value" {
            return validation(&format!("The attribute {path} is not a unique attribute"));
        }
        for (_, u) in st.users.iter() {
            if path == "username" && u["UserName"].as_str() == Some(value) {
                return ok_json(&json!({"UserId": u["UserId"], "IdentityStoreId": store_id}));
            }
            if path == "emails.value" {
                if let Some(emails) = u["Emails"].as_array() {
                    for email in emails {
                        if email["Value"].as_str() == Some(value) {
                            return ok_json(&json!({
                                "UserId": u["UserId"],
                                "IdentityStoreId": store_id,
                            }));
                        }
                    }
                }
            }
        }
    }
    not_found("USER not found.", "USER")
}

fn get_group_membership_id(b: &mut Backend, p: &Value) -> Response {
    let store_id = p["IdentityStoreId"].as_str().unwrap_or_default();
    let group_id = p["GroupId"].as_str().unwrap_or_default();
    let user_id = p["MemberId"]["UserId"].as_str().unwrap_or_default();
    let st = store(b, store_id);
    for (mid, m) in st.memberships.iter() {
        if m["GroupId"].as_str() == Some(group_id)
            && m["MemberId"]["UserId"].as_str() == Some(user_id)
        {
            return ok_json(&json!({"MembershipId": mid, "IdentityStoreId": store_id}));
        }
    }
    not_found("MEMBERSHIP not found.", "GROUP_MEMBERSHIP")
}

fn is_member_in_groups(b: &mut Backend, p: &Value) -> Response {
    let store_id = p["IdentityStoreId"].as_str().unwrap_or_default();
    let user_id = p["MemberId"]["UserId"].as_str().unwrap_or_default();
    let st = store(b, store_id);
    let member_groups: Vec<&str> = st
        .memberships
        .iter()
        .filter(|(_, m)| m["MemberId"]["UserId"].as_str() == Some(user_id))
        .map(|(_, m)| m["GroupId"].as_str().unwrap_or_default())
        .collect();
    let results: Vec<Value> = p["GroupIds"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|gid| {
                    json!({
                        "GroupId": gid,
                        "MemberId": p["MemberId"],
                        "MembershipExists": member_groups.contains(&gid.as_str().unwrap_or_default()),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    ok_json(&json!({"Results": results}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn req(op: &str, body: Value) -> Request {
        Request {
            service: "identitystore".into(),
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
        uuid::Uuid::new_v4().as_u128() as u64
    }

    fn user_body(username: &str) -> Value {
        json!({
            "IdentityStoreId": "d-1234567890",
            "UserName": username,
            "DisplayName": "Test User",
            "Name": {"GivenName": "Test", "FamilyName": "User"},
        })
    }

    #[test]
    fn user_crud() {
        let acct = scope();
        let mut r = req("CreateUser", user_body("u1"));
        r.account = acct;
        let resp = dispatch(&r).unwrap();
        assert_eq!(resp.status, 200);
        let b = body(&resp);
        assert_eq!(b["IdentityStoreId"], "d-1234567890");
        let uid = b["UserId"].as_str().unwrap().to_string();

        r = req(
            "DescribeUser",
            json!({"IdentityStoreId": "d-1234567890", "UserId": uid}),
        );
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(b["UserName"], "u1");
        assert_eq!(b["DisplayName"], "Test User");
        assert_eq!(b["Name"]["GivenName"], "Test");

        r = req("ListUsers", json!({"IdentityStoreId": "d-1234567890"}));
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert!(b["Users"]
            .as_array()
            .unwrap()
            .iter()
            .any(|u| u["UserName"] == "u1"));

        r = req(
            "DeleteUser",
            json!({"IdentityStoreId": "d-1234567890", "UserId": uid}),
        );
        r.account = acct;
        assert_eq!(dispatch(&r).unwrap().status, 200);
        r = req(
            "DescribeUser",
            json!({"IdentityStoreId": "d-1234567890", "UserId": uid}),
        );
        r.account = acct;
        let resp = dispatch(&r).unwrap();
        assert_eq!(body(&resp)["__type"], "ResourceNotFoundException");
        assert_eq!(resp.status, 400);
    }

    #[test]
    fn user_validation_errors() {
        let acct = scope();
        let mut r = req(
            "CreateUser",
            json!({
                "IdentityStoreId": "d-1",
                "UserName": "x",
                "Name": {"GivenName": "T", "FamilyName": "U"}
            }),
        );
        r.account = acct;
        let resp = dispatch(&r).unwrap();
        assert_eq!(resp.status, 400);
        let b = body(&resp);
        assert_eq!(b["__type"], "ValidationException");
        assert_eq!(
            b["Message"],
            "displayname: The attribute displayname is required"
        );

        r = req("CreateUser", user_body("dup"));
        r.account = acct;
        assert_eq!(dispatch(&r).unwrap().status, 200);
        r = req("CreateUser", user_body("dup"));
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(b["__type"], "ConflictException");
        assert_eq!(b["Reason"], "UNIQUENESS_CONSTRAINT_VIOLATION");
    }

    #[test]
    fn group_crud_and_membership() {
        let acct = scope();
        let store_id = "d-1234567890";
        let mut r = req(
            "CreateGroup",
            json!({"IdentityStoreId": store_id, "DisplayName": "grp", "Description": "d"}),
        );
        r.account = acct;
        let gid = body(&dispatch(&r).unwrap())["GroupId"]
            .as_str()
            .unwrap()
            .to_string();

        r = req("CreateUser", user_body("m1"));
        r.account = acct;
        let uid = body(&dispatch(&r).unwrap())["UserId"]
            .as_str()
            .unwrap()
            .to_string();

        r = req(
            "CreateGroupMembership",
            json!({"IdentityStoreId": store_id, "GroupId": gid, "MemberId": {"UserId": uid}}),
        );
        r.account = acct;
        let mid = body(&dispatch(&r).unwrap())["MembershipId"]
            .as_str()
            .unwrap()
            .to_string();

        r = req(
            "DescribeGroupMembership",
            json!({"IdentityStoreId": store_id, "MembershipId": mid}),
        );
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(b["GroupId"].as_str().unwrap(), gid.as_str());
        assert_eq!(b["MemberId"]["UserId"].as_str().unwrap(), uid.as_str());

        r = req(
            "UpdateGroup",
            json!({"IdentityStoreId": store_id, "GroupId": gid, "Operations": [
                {"AttributePath": "DisplayName", "AttributeValue": "renamed"}
            ]}),
        );
        r.account = acct;
        assert_eq!(dispatch(&r).unwrap().status, 200);
        r = req(
            "DescribeGroup",
            json!({"IdentityStoreId": store_id, "GroupId": gid}),
        );
        r.account = acct;
        assert_eq!(body(&dispatch(&r).unwrap())["DisplayName"], "renamed");

        r = req(
            "ListGroupMemberships",
            json!({"IdentityStoreId": store_id, "GroupId": gid}),
        );
        r.account = acct;
        assert!(body(&dispatch(&r).unwrap())["GroupMemberships"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["MembershipId"].as_str() == Some(mid.as_str())));

        r = req(
            "GetGroupMembershipId",
            json!({"IdentityStoreId": store_id, "GroupId": gid, "MemberId": {"UserId": uid}}),
        );
        r.account = acct;
        assert_eq!(
            body(&dispatch(&r).unwrap())["MembershipId"]
                .as_str()
                .unwrap(),
            mid.as_str()
        );

        r = req(
            "DescribeGroupMembership",
            json!({"IdentityStoreId": store_id, "MembershipId": "fake"}),
        );
        r.account = acct;
        assert_eq!(
            body(&dispatch(&r).unwrap())["__type"],
            "ResourceNotFoundException"
        );

        r = req(
            "DeleteGroupMembership",
            json!({"IdentityStoreId": store_id, "MembershipId": mid}),
        );
        r.account = acct;
        assert_eq!(dispatch(&r).unwrap().status, 200);
    }

    #[test]
    fn alternate_identifier_lookups() {
        let acct = scope();
        let mut r = req("CreateUser", user_body("lookup"));
        r.account = acct;
        let uid = body(&dispatch(&r).unwrap())["UserId"]
            .as_str()
            .unwrap()
            .to_string();

        r = req(
            "GetUserId",
            json!({"IdentityStoreId": "d-1234567890",
                   "AlternateIdentifier": {"UniqueAttribute": {
                       "AttributePath": "userName", "AttributeValue": "lookup"}}}),
        );
        r.account = acct;
        assert_eq!(
            body(&dispatch(&r).unwrap())["UserId"].as_str().unwrap(),
            uid.as_str()
        );

        r = req(
            "GetUserId",
            json!({"IdentityStoreId": "d-1234567890",
                   "AlternateIdentifier": {"UniqueAttribute": {
                       "AttributePath": "title", "AttributeValue": "boss"}}}),
        );
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(b["__type"], "ValidationException");
        assert_eq!(
            b["Message"],
            "The attribute title is not a unique attribute"
        );

        r = req(
            "CreateGroup",
            json!({"IdentityStoreId": "d-1234567890", "DisplayName": "lookgrp"}),
        );
        r.account = acct;
        let gid = body(&dispatch(&r).unwrap())["GroupId"]
            .as_str()
            .unwrap()
            .to_string();

        r = req(
            "GetGroupId",
            json!({"IdentityStoreId": "d-1234567890",
                   "AlternateIdentifier": {"UniqueAttribute": {
                       "AttributePath": "displayName", "AttributeValue": "lookgrp"}}}),
        );
        r.account = acct;
        assert_eq!(
            body(&dispatch(&r).unwrap())["GroupId"].as_str().unwrap(),
            gid.as_str()
        );

        r = req(
            "GetGroupId",
            json!({"IdentityStoreId": "d-1234567890",
                   "AlternateIdentifier": {"UniqueAttribute": {
                       "AttributePath": "displayName", "AttributeValue": "missing"}}}),
        );
        r.account = acct;
        assert_eq!(
            body(&dispatch(&r).unwrap())["__type"],
            "ResourceNotFoundException"
        );
    }

    #[test]
    fn delete_then_describe_not_found() {
        let acct = scope();
        let mut r = req(
            "CreateGroup",
            json!({"IdentityStoreId": "d-1234567890", "DisplayName": "gone"}),
        );
        r.account = acct;
        let gid = body(&dispatch(&r).unwrap())["GroupId"]
            .as_str()
            .unwrap()
            .to_string();
        r = req(
            "DeleteGroup",
            json!({"IdentityStoreId": "d-1234567890", "GroupId": gid}),
        );
        r.account = acct;
        dispatch(&r).unwrap();
        r = req(
            "DescribeGroup",
            json!({"IdentityStoreId": "d-1234567890", "GroupId": gid}),
        );
        r.account = acct;
        let resp = dispatch(&r).unwrap();
        assert_eq!(resp.status, 400);
        let b = body(&resp);
        assert_eq!(b["__type"], "ResourceNotFoundException");
        assert_eq!(b["Message"], "GROUP not found.");
    }

    #[test]
    fn membership_member_must_exist() {
        let acct = scope();
        let mut r = req(
            "CreateGroup",
            json!({"IdentityStoreId": "d-1", "DisplayName": "g"}),
        );
        r.account = acct;
        let gid = body(&dispatch(&r).unwrap())["GroupId"]
            .as_str()
            .unwrap()
            .to_string();
        r = req(
            "CreateGroupMembership",
            json!({"IdentityStoreId": "d-1", "GroupId": gid, "MemberId": {"UserId": "nope"}}),
        );
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(b["__type"], "ResourceNotFoundException");
        assert_eq!(b["Message"], "Member does not exist");
    }
}
