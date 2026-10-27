//! Cognito Identity Pools backend (JSON protocol, target
//! `...AWSCognitoIdentityService.<Op>`).
//!
//! Port of moto's cognitoidentity: identity pool CRUD plus the ID/token
//! endpoints, per (account, region).

use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Value};

#[derive(Default)]
struct State {
    /// Identity pool records, in moto's `to_json()` shape.
    pools: Vec<Value>,
    /// IdentityPoolId -> IdentityId entries created by GetId.
    pool_identities: Vec<(String, Value)>,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("cognitoidentity"));
    SLOT.clone()
}

fn random_identity_id(region: &str) -> String {
    format!("{}:{}", region, uuid::Uuid::new_v4())
}

fn ok(v: Value) -> Response {
    Response::json(200, serde_json::to_vec(&v).unwrap_or_default())
}

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "cognitoidentity" {
        return None;
    }
    let body = crate::proto::parse_json_body(&req.body);
    let slot = state();
    let mut st = slot.write();
    let b = st.entry((req.account, req.region.clone())).or_default();
    Some(handle(req, b, &body, &req.region))
}

fn str_field<'a>(p: &'a Value, key: &str) -> Option<&'a str> {
    p.get(key).and_then(Value::as_str)
}

impl State {
    fn find(&self, id: &str) -> Option<&Value> {
        self.pools
            .iter()
            .find(|pv| pv.get("IdentityPoolId").and_then(Value::as_str) == Some(id))
    }

    fn find_mut(&mut self, id: &str) -> Option<&mut Value> {
        self.pools
            .iter_mut()
            .find(|pv| pv.get("IdentityPoolId").and_then(Value::as_str) == Some(id))
    }
}

/// Pool JSON exactly as moto's `CognitoIdentityPool.to_json()` builds it.
#[allow(clippy::too_many_arguments)]
fn pool_json(
    name: &str,
    pool_id: &str,
    allow_unauth: Value,
    login_providers: Value,
    developer_provider_name: Value,
    open_id_arns: Value,
    identity_providers: Value,
    saml_arns: Value,
    tags: Value,
) -> Value {
    json!({
        "IdentityPoolId": pool_id,
        "IdentityPoolName": name,
        "AllowUnauthenticatedIdentities": allow_unauth,
        "SupportedLoginProviders": login_providers,
        "DeveloperProviderName": developer_provider_name,
        "OpenIdConnectProviderARNs": open_id_arns,
        "CognitoIdentityProviders": identity_providers,
        "SamlProviderARNs": saml_arns,
        "IdentityPoolTags": tags,
    })
}

/// moto trusts whatever arrived for `AllowUnauthenticatedIdentities`; the
/// client sends a bool and the suite asserts the round-trip, but moto's
/// JSON-to-string coercion (`str(True)`) is also mirrored as-is.
fn allow_unauth(v: &Value) -> Value {
    match v {
        Value::Bool(b) => json!(b),
        Value::String(s) => match s.as_str() {
            "true" | "True" => json!(true),
            "false" | "False" => json!(false),
            other => json!(other),
        },
        Value::Null => json!("None"),
        Value::Number(n) => json!(n),
        v => json!(v),
    }
}

fn handle(req: &Request, st: &mut State, p: &Value, region: &str) -> Response {
    match req.operation.as_str() {
        "CreateIdentityPool" => {
            let name = match str_field(p, "IdentityPoolName") {
                Some(n) => n,
                None => return missing_param("IdentityPoolName"),
            };
            if !name
                .chars()
                .all(|c| c.is_alphanumeric() || " \t+=,.@-_".contains(c))
            {
                return invalid_name(name);
            }
            let pool_id = random_identity_id(region);
            let pool = pool_json(
                name,
                &pool_id,
                allow_unauth(
                    p.get("AllowUnauthenticatedIdentities")
                        .unwrap_or(&json!("")),
                ),
                p.get("SupportedLoginProviders")
                    .cloned()
                    .unwrap_or(json!({})),
                p.get("DeveloperProviderName").cloned().unwrap_or(json!("")),
                p.get("OpenIdConnectProviderARNs")
                    .cloned()
                    .unwrap_or(json!([])),
                p.get("CognitoIdentityProviders")
                    .cloned()
                    .unwrap_or(json!([])),
                p.get("SamlProviderARNs").cloned().unwrap_or(json!([])),
                p.get("IdentityPoolTags").cloned().unwrap_or(json!({})),
            );
            st.pool_identities
                .push((pool_id.clone(), json!({"IdentityId": &pool_id})));
            st.pools.push(pool.clone());
            ok(pool)
        }
        "UpdateIdentityPool" => {
            let Some(id) = str_field(p, "IdentityPoolId") else {
                return resource_not_found("null");
            };
            let Some(pool) = st.find_mut(id) else {
                return resource_not_found(id);
            };
            if let Some(n) = str_field(p, "IdentityPoolName") {
                pool["IdentityPoolName"] = json!(n);
            }
            if p.get("AllowUnauthenticatedIdentities").is_some() {
                pool["AllowUnauthenticatedIdentities"] =
                    allow_unauth(&p["AllowUnauthenticatedIdentities"]);
            }
            for (key, path) in [
                ("SupportedLoginProviders", "SupportedLoginProviders"),
                ("DeveloperProviderName", "DeveloperProviderName"),
                ("OpenIdConnectProviderARNs", "OpenIdConnectProviderARNs"),
                ("CognitoIdentityProviders", "CognitoIdentityProviders"),
                ("SamlProviderARNs", "SamlProviderARNs"),
            ] {
                if let Some(v) = p.get(key) {
                    pool[path] = v.clone();
                }
            }
            if let Some(Value::Object(tags)) = p.get("IdentityPoolTags") {
                if !tags.is_empty() {
                    pool["IdentityPoolTags"] = p["IdentityPoolTags"].clone();
                }
            }
            ok(pool.clone())
        }
        "DescribeIdentityPool" => match str_field(p, "IdentityPoolId").and_then(|id| st.find(id)) {
            Some(pool) => ok(pool.clone()),
            None => resource_not_found(str_field(p, "IdentityPoolId").unwrap_or("?")),
        },
        "DeleteIdentityPool" => {
            if let Some(id) = str_field(p, "IdentityPoolId") {
                st.pools
                    .retain(|pv| pv.get("IdentityPoolId").and_then(Value::as_str) != Some(id));
                st.pool_identities.retain(|(pid, _)| pid != id);
            }
            ok(json!({}))
        }
        "GetId" => {
            let pool_id = str_field(p, "IdentityPoolId").unwrap_or("");
            let identity_id = random_identity_id(region);
            let entry = json!({"IdentityId": identity_id});
            st.pool_identities
                .push((pool_id.to_string(), entry.clone()));
            ok(entry)
        }
        "ListIdentities" => {
            let pool_id = str_field(p, "IdentityPoolId").unwrap_or("");
            let identities: Vec<Value> = st
                .pool_identities
                .iter()
                .filter(|(pid, _)| pid == pool_id)
                .map(|(_, v)| v.clone())
                .collect();
            ok(json!({
                "IdentityPoolId": pool_id,
                "Identities": identities,
            }))
        }
        "GetCredentialsForIdentity" => {
            let identity_id = str_field(p, "IdentityId").unwrap_or("");
            ok(json!({
                "Credentials": {
                    "AccessKeyId": "TESTACCESSKEY12345",
                    // moto: utcnow()+90s as a float epoch
                    "Expiration": crate::proto::unix_in(90.0),
                    "SecretKey": "ABCSECRETKEY",
                    "SessionToken": "ABC12345",
                },
                "IdentityId": identity_id,
            }))
        }
        "GetOpenIdTokenForDeveloperIdentity" | "GetOpenIdToken" => {
            let identity_id = match str_field(p, "IdentityId") {
                Some(id) if !id.is_empty() => id.to_string(),
                _ => random_identity_id(region),
            };
            let token = random_identity_id(region);
            ok(json!({"IdentityId": identity_id, "Token": token}))
        }
        "ListIdentityPools" => {
            let pools: Vec<Value> = st.pools.clone();
            ok(json!({"IdentityPools": pools}))
        }
        _ => not_handled(),
    }
}

fn resource_not_found(id: &str) -> Response {
    let body = json!({
        "__type": "ResourceNotFoundException",
        "message": format!("Resource: {id} not found"),
    });
    Response::json(400, serde_json::to_vec(&body).unwrap_or_default())
}

fn missing_param(name: &str) -> Response {
    Response::json(
        400,
        serde_json::to_vec(&json!({
            "__type": "InvalidParameterException",
            "message": format!("{name} is required"),
        }))
        .unwrap_or_default(),
    )
}

fn invalid_name(name: &str) -> Response {
    // botocore's cognito-identity model carries InvalidParameterException —
    // ValidationException is not modeled for the service.
    Response::json(
        400,
        serde_json::to_vec(&json!({
            "__type": "InvalidParameterException",
            "message": format!(
                "1 validation error detected: Value '{name}' at 'identityPoolName' failed to satisfy constraint: Member must satisfy regular expression pattern: [\\w\\s+=,.@-]+"
            ),
        }))
        .unwrap_or_default(),
    )
}

/// Ops moto does not implement; the gateway falls through to the sidecar.
fn not_handled() -> Response {
    let body = json!({
        "__type": "NotImplemented",
        "message": "cognitoidentity: not implemented",
    });
    Response::json(501, serde_json::to_vec(&body).unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn req(op: &str) -> Request {
        Request {
            service: "cognitoidentity".into(),
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
    fn create_describe_delete_roundtrip() {
        let slot = state();
        let mut st = slot.write();
        let b = st.entry((123456789012, "us-east-1".into())).or_default();
        let p = json!({
            "IdentityPoolName": "pool-1",
            "AllowUnauthenticatedIdentities": true,
        });
        let created = handle(&req("CreateIdentityPool"), b, &p, "us-east-1");
        assert_eq!(created.status, 200);
        let pool_id = serde_json::from_slice::<Value>(&created.body).unwrap()["IdentityPoolId"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(pool_id.starts_with("us-east-1:"));

        let described = handle(
            &req("DescribeIdentityPool"),
            b,
            &json!({"IdentityPoolId": &pool_id}),
            "us-east-1",
        );
        let described = serde_json::from_slice::<Value>(&described.body).unwrap();
        assert_eq!(described["IdentityPoolName"], "pool-1");

        let listed = handle(&req("ListIdentityPools"), b, &json!({}), "us-east-1");
        let listed = serde_json::from_slice::<Value>(&listed.body).unwrap();
        assert_eq!(listed["IdentityPools"].as_array().unwrap().len(), 1);

        handle(
            &req("DeleteIdentityPool"),
            b,
            &json!({"IdentityPoolId": &pool_id}),
            "us-east-1",
        );
        assert!(b.find(&pool_id).is_none());
    }

    #[test]
    fn invalid_pool_name_rejected() {
        let slot = state();
        let mut st = slot.write();
        let b = st.entry((123456789012, "us-east-1".into())).or_default();
        let resp = handle(
            &req("CreateIdentityPool"),
            b,
            &json!({"IdentityPoolName": "bad/name", "AllowUnauthenticatedIdentities": true}),
            "us-east-1",
        );
        assert_eq!(resp.status, 400);
        let v: Value = serde_json::from_slice(&resp.body).unwrap();
        // canon: cognito-identity models InvalidParameterException, never
        // ValidationException
        assert_eq!(v["__type"], "InvalidParameterException");
        assert!(v["message"]
            .as_str()
            .unwrap()
            .contains("failed to satisfy constraint"));
    }
}
