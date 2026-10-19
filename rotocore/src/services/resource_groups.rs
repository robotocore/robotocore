//! AWS Resource Groups backend — mirrors moto's `resourcegroups` engine plus
//! the py gateway's intercepts for the ops moto lacks (GetAccountSettings,
//! ListGroupResources, UpdateAccountSettings).
//!
//! Group state is (account, region)-scoped and name-keyed. ARNs follow moto's
//! shape exactly (`arn:aws:resource-groups:us-west-1:<account>:group/<name>` —
//! moto hardcodes us-west-1), tags live under /resources/{arn}/tags REST paths
//! with a percent-encoded ARN, and list_group_resources always answers with
//! an empty resource set like the py provider does.

use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Map, Value};
use std::collections::HashMap;

static SLOT: Lazy<store::Slot<RgState>> = Lazy::new(|| store::slot::<RgState>("resource-groups"));

#[derive(Default)]
struct RgState {
    groups: HashMap<String, Group>,
    region: String,
    account: String,
}

struct Group {
    name: String,
    description: String,
    arn: String,
    resource_query: Value,
    tags: Map<String, Value>,
    configuration: Option<Value>,
}

pub fn dispatch(req: &Request) -> Response {
    let params = crate::proto::parse_json_body(&req.body);
    let mut guard = SLOT.write();
    let st = guard.entry((req.account, req.region.clone())).or_default();
    if st.region.is_empty() {
        st.region = req.region.clone();
        st.account = req.account.to_string();
    }
    let path = req.path.trim_start_matches('/');
    match (req.method.as_str(), path) {
        ("POST", "groups") => st.create_group(&params),
        ("POST", "delete-group") => st.delete_group(&params),
        ("POST", "get-group") => st.get_group(&params, "GroupName"),
        ("POST", "update-group") => st.update_group(&params),
        ("POST", "groups-list") => st.list_groups(),
        ("POST", "update-group-query") => st.update_group_query(&params),
        ("POST", "get-group-query") => st.get_group_query(&params, "GroupName"),
        ("POST", "get-group-configuration") => st.group_configuration(&params, "Group"),
        ("POST", "put-group-configuration") => st.put_group_configuration(&params),
        ("POST", "list-group-resources") => st.list_group_resources(),
        ("POST", "list-tag-sync-tasks") => Response::json_status(200, &json!({"TagSyncTasks": []})),
        ("POST", "get-account-settings") => st.get_account_settings(),
        ("POST", "update-account-settings") => st.update_account_settings(&params),
        ("GET", p) if let Some(arn) = tags_path_arn(p) => st.get_tags(arn),
        ("PUT", p) if let Some(arn) = tags_path_arn(p) => st.tag(arn, &params),
        ("PATCH", p) if let Some(arn) = tags_path_arn(p) => st.untag(arn, &params),
        _ => crate::proto::json_error(
            501,
            "NotImplemented",
            &format!("The {} action has not been implemented", req.operation),
        ),
    }
}

fn params_str(params: &Value, key: &str) -> String {
    params
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

/// Prefer `key`, fall back to `alt` when empty (moto's `A or B` params).
fn param_ident(params: &Value, key: &str, alt: &str) -> String {
    let v = params_str(params, key);
    if v.is_empty() {
        params_str(params, alt)
    } else {
        v
    }
}

/// Extract the percent-encoded ARN from `/resources/{arn}/tags/:suffix`.
fn tags_path_arn(path: &str) -> Option<String> {
    let body = path.strip_prefix("resources/")?;
    let body = body.strip_suffix("/tags")?;
    Some(crate::proto::url_decode(body))
}

/// Several ops accept either `GroupName` or a group ARN — moto splits the
/// name off the ARN when that's what it received.
fn name_from_identifier(identifier: &str) -> String {
    if identifier.contains(":group/") {
        identifier
            .rsplit(":group/")
            .next()
            .unwrap_or("")
            .to_string()
    } else {
        identifier.to_string()
    }
}

impl RgState {
    fn by_identifier(&self, identifier: &str) -> Option<&Group> {
        let name = name_from_identifier(identifier);
        if self.groups.contains_key(&name) {
            return self.groups.get(&name);
        }
        self.groups.values().find(|g| g.arn == identifier)
    }

    fn by_identifier_mut(&mut self, identifier: &str) -> Option<&mut Group> {
        let key = self.by_identifier(identifier).map(|g| g.name.clone())?;
        self.groups.get_mut(&key)
    }

    fn get_group(&self, params: &Value, key: &str) -> Response {
        let ident = params_str(params, key);
        match self.by_identifier(&ident) {
            Some(g) => Response::json_status(200, &group_body(g)),
            None => not_found(),
        }
    }

    fn create_group(&mut self, params: &Value) -> Response {
        let name = params_str(params, "Name");
        let description = params_str(params, "Description");
        let rquery = params.get("ResourceQuery").cloned().unwrap_or(Value::Null);
        let tags = params.get("Tags").cloned().unwrap_or(json!({}));
        let configuration = params.get("Configuration").cloned();

        // Validation order mirrors moto's FakeResourceGroup + backend checks.
        if description.len() > 511 {
            return bad_request(&format!(
                "'{}' at 'description' failed to satisfy constraint: Member must have length less than or equal to 512",
                description
            ));
        }
        if name.is_empty() || name.len() > 128 {
            return bad_request(&format!(
                "'{}' at 'name' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_\\.-]+",
                name
            ));
        }
        if self.groups.contains_key(&name) {
            return bad_request("Cannot create group: group already exists");
        }
        if name.to_uppercase().starts_with("AWS") {
            return bad_request("Group name must not start with 'AWS'");
        }
        let arn = format!(
            "arn:aws:resource-groups:us-west-1:{account}:group/{name}",
            account = self.account
        );
        let group = Group {
            name: name.clone(),
            description: description.clone(),
            arn,
            resource_query: rquery.clone(),
            tags: tags.as_object().cloned().unwrap_or_default(),
            configuration: configuration.clone(),
        };
        let out = json!({
            "Group": {
                "GroupArn": group.arn,
                "Name": name,
                "Description": description,
            },
            "ResourceQuery": rquery,
            "Tags": tags,
            "GroupConfiguration": {"Configuration": configuration},
        });
        self.groups.insert(group.name.clone(), group);
        Response::json_status(200, &out)
    }

    fn delete_group(&mut self, params: &Value) -> Response {
        let ident = param_ident(params, "GroupName", "Group");
        let name = name_from_identifier(&ident);
        match self.groups.remove(&name) {
            Some(g) => Response::json_status(
                200,
                &json!({
                    "Group": {"GroupArn": g.arn, "Name": g.name, "Description": g.description},
                }),
            ),
            None => not_found(),
        }
    }

    fn list_groups(&self) -> Response {
        let mut identifiers = Vec::new();
        let mut groups = Vec::new();
        for g in self.groups.values() {
            identifiers.push(json!({"GroupName": g.name, "GroupArn": g.arn}));
            groups.push(json!({
                "GroupArn": g.arn,
                "Name": g.name,
                "Description": g.description,
            }));
        }
        Response::json_status(
            200,
            &json!({
                "GroupIdentifiers": identifiers,
                "Groups": groups,
                "NextToken": Value::Null,
            }),
        )
    }

    fn update_group(&mut self, params: &Value) -> Response {
        let ident = param_ident(params, "GroupName", "Group");
        let description = params
            .get("Description")
            .and_then(Value::as_str)
            .unwrap_or("");
        if description.len() > 511 {
            return bad_request(
                "1 validation error detected: Value 'description' failed to satisfy constraint",
            );
        }
        match self.by_identifier_mut(&ident) {
            Some(g) => {
                g.description = description.to_string();
                Response::json_status(200, &group_body(g))
            }
            None => not_found(),
        }
    }

    fn update_group_query(&mut self, params: &Value) -> Response {
        let ident = param_ident(params, "GroupName", "Group");
        let rquery = params.get("ResourceQuery").cloned().unwrap_or(Value::Null);
        match self.by_identifier_mut(&ident) {
            Some(g) => {
                g.resource_query = rquery.clone();
                Response::json_status(
                    200,
                    &json!({"GroupQuery": {"GroupName": g.name, "ResourceQuery": rquery}}),
                )
            }
            None => not_found(),
        }
    }

    fn get_group_query(&self, params: &Value, key: &str) -> Response {
        let ident = params_str(params, key);
        match self.by_identifier(&ident) {
            Some(g) => Response::json_status(
                200,
                &json!({"GroupQuery": {"GroupName": g.name, "ResourceQuery": g.resource_query}}),
            ),
            None => not_found(),
        }
    }

    fn get_tags(&self, arn: String) -> Response {
        match self.by_identifier(&arn) {
            Some(g) => Response::json_status(200, &json!({"Arn": arn, "Tags": g.tags})),
            None => not_found(),
        }
    }

    fn tag(&mut self, arn: String, params: &Value) -> Response {
        let mut echo = Map::new();
        if let Some(new_tags) = params.get("Tags").and_then(Value::as_object) {
            for (k, v) in new_tags {
                echo.insert(k.clone(), v.clone());
            }
        }
        match self.by_identifier_mut(&arn) {
            Some(g) => {
                g.tags.extend(echo.clone());
                Response::json_status(200, &json!({"Arn": arn, "Tags": echo}))
            }
            None => not_found(),
        }
    }

    fn untag(&mut self, arn: String, params: &Value) -> Response {
        let keys: Vec<String> = params
            .get("Keys")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(String::from)
                    .collect()
            })
            .unwrap_or_default();
        match self.by_identifier_mut(&arn) {
            Some(g) => {
                for k in &keys {
                    g.tags.remove(k);
                }
                Response::json_status(200, &json!({"Arn": arn, "Keys": keys}))
            }
            None => not_found(),
        }
    }

    fn group_configuration(&self, params: &Value, key: &str) -> Response {
        let ident = params_str(params, key);
        match self.by_identifier(&ident) {
            Some(g) => Response::json_status(
                200,
                &json!({"GroupConfiguration": {"Configuration": g.configuration.clone().unwrap_or(json!([]))}}),
            ),
            None => not_found(),
        }
    }

    fn put_group_configuration(&mut self, params: &Value) -> Response {
        let ident = params_str(params, "Group");
        let configuration = params.get("Configuration").cloned().unwrap_or(json!([]));
        match self.by_identifier_mut(&ident) {
            Some(g) => {
                g.configuration = Some(configuration.clone());
                Response::json_status(
                    200,
                    &json!({"GroupConfiguration": {"Configuration": configuration}}),
                )
            }
            None => not_found(),
        }
    }

    /// Mirrors the py provider: the group is looked up for effect but the
    /// answer is always an empty resource set (moto has no ListGroupResources).
    fn list_group_resources(&self) -> Response {
        Response::json_status(200, &json!({"ResourceIdentifiers": [], "QueryErrors": []}))
    }

    fn get_account_settings(&self) -> Response {
        Response::json_status(
            200,
            &json!({"AccountSettings": {
                "GroupLifecycleEventsDesiredStatus": "INACTIVE",
                "GroupLifecycleEventsStatus": "INACTIVE",
                "GroupLifecycleEventsStatusMessage": "",
            }}),
        )
    }

    fn update_account_settings(&self, params: &Value) -> Response {
        let desired = params
            .get("GroupLifecycleEventsDesiredStatus")
            .cloned()
            .unwrap_or(json!("INACTIVE"));
        Response::json_status(
            200,
            &json!({"AccountSettings": {
                "GroupLifecycleEventsDesiredStatus": desired,
                "GroupLifecycleEventsStatus": desired,
                "GroupLifecycleEventsStatusMessage": "",
            }}),
        )
    }
}

fn group_body(g: &Group) -> Value {
    json!({
        "Group": {
            "GroupArn": g.arn,
            "Name": g.name,
            "Description": g.description,
        }
    })
}

fn not_found() -> Response {
    crate::proto::json_error(
        404,
        "NotFoundException",
        "One or more of the specified resources don't exist.",
    )
}

fn bad_request(message: &str) -> Response {
    crate::proto::json_error(400, "BadRequestException", message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn req(method: &str, path: &str, body: Value) -> Request {
        reqacct(method, path, body, 123456789031)
    }

    fn reqacct(method: &str, path: &str, body: Value, account: u64) -> Request {
        Request {
            service: "resource-groups".into(),
            operation: String::new(),
            method: method.into(),
            path: path.into(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            account,
            region: "us-east-1".into(),
        }
    }

    #[test]
    fn create_get_list_delete_group() {
        let create = dispatch(&req(
            "POST",
            "/groups",
            json!({
                "Name": "grp",
                "Description": "d",
                "ResourceQuery": {"Type": "TAG_FILTERS_1_0", "Query": "{}"},
            }),
        ));
        assert_eq!(create.status, 200);
        let body: Value = serde_json::from_slice(&create.body).unwrap();
        let arn = body["Group"]["GroupArn"].as_str().unwrap().to_string();
        assert!(arn.contains("group/grp") && arn.contains("resource-groups"));

        let get = dispatch(&req("POST", "/get-group", json!({"GroupName": "grp"})));
        assert_eq!(get.status, 200);

        let list: Value =
            serde_json::from_slice(&dispatch(&req("POST", "/groups-list", json!({}))).body)
                .unwrap();
        assert!(list["GroupIdentifiers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|g| g["GroupName"] == "grp"));

        let del = dispatch(&req("POST", "/delete-group", json!({"GroupName": "grp"})));
        assert_eq!(del.status, 200);
        let miss = dispatch(&req("POST", "/get-group", json!({"GroupName": "grp"})));
        assert_eq!(miss.status, 404);
    }

    #[test]
    fn tags_via_resource_path() {
        let _ = dispatch(&req(
            "POST",
            "/groups",
            json!({"Name": "second", "Tags": {"env": "staging"}}),
        ));
        let arn = "arn:aws:resource-groups:us-west-1:123456789012:group/second".to_string();
        let encoded = urlencoding::encode(&arn).into_owned();
        let get = dispatch(&req(
            "GET",
            &format!("/resources/{encoded}/tags"),
            json!({}),
        ));
        let body: Value = serde_json::from_slice(&get.body).unwrap();
        assert_eq!(body["Tags"]["env"], "staging");

        let tag = dispatch(&req(
            "PUT",
            &format!("/resources/{encoded}/tags"),
            json!({"Tags": {"new": "value"}}),
        ));
        assert_eq!(tag.status, 200);

        let untag = dispatch(&req(
            "PATCH",
            &format!("/resources/{encoded}/tags"),
            json!({"Keys": ["env"]}),
        ));
        assert_eq!(untag.status, 200);
        let after: Value = serde_json::from_slice(
            &dispatch(&req(
                "GET",
                &format!("/resources/{encoded}/tags"),
                json!({}),
            ))
            .body,
        )
        .unwrap();
        assert!(after["Tags"].get("env").is_none());
        assert_eq!(after["Tags"]["new"], "value");
    }

    #[test]
    fn account_settings_round_trip() {
        let resp: Value = serde_json::from_slice(
            &dispatch(&req("POST", "/get-account-settings", json!({}))).body,
        )
        .unwrap();
        assert_eq!(
            resp["AccountSettings"]["GroupLifecycleEventsDesiredStatus"],
            "INACTIVE"
        );

        let upd: Value = serde_json::from_slice(
            &dispatch(&req(
                "POST",
                "/update-account-settings",
                json!({"GroupLifecycleEventsDesiredStatus": "ACTIVE"}),
            ))
            .body,
        )
        .unwrap();
        assert_eq!(
            upd["AccountSettings"]["GroupLifecycleEventsStatus"],
            "ACTIVE"
        );
    }

    #[test]
    fn list_group_resources_is_empty_like_py() {
        let resp: Value = serde_json::from_slice(
            &dispatch(&req("POST", "/list-group-resources", json!({"Group": "x"}))).body,
        )
        .unwrap();
        assert_eq!(resp["ResourceIdentifiers"], json!([]));
        assert_eq!(resp["QueryErrors"], json!([]));
    }
}
