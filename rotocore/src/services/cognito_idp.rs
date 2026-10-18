//! Cognito Identity Provider backend (JSON protocol, X-Amz-Target).
//!
//! Port of robotocore's native python provider
//! (`src/robotocore/services/cognito/provider.py`): pools, clients, users,
//! groups, auth flows with structural JWT tokens, pool domains and tags.
//! Operations the python provider delegates to moto (identity providers,
//! resource servers, terms) follow moto's backend semantics; ops neither
//! implements (managed login branding, user import jobs) dispatch `None`
//! exactly like the sidecar's NotImplemented fall-through.

use crate::proto::parse_json_body;
use crate::store;
use crate::{Request, Response};
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use base64::Engine;
use once_cell::sync::Lazy;
use serde_json::{json, Map, Value};

#[derive(Default)]
struct Pool {
    /// The stored user pool record (native shape).
    pool: Value,
    /// (username, user record) — identity = python provider's dict.
    users: Vec<(String, Value)>,
    /// (client id, client record).
    clients: Vec<(String, Value)>,
    /// (group name, group record).
    groups: Vec<(String, Value)>,
    /// username -> member group names.
    user_groups: Map<String, Value>,
    /// provider name -> moto identity-provider record (extended to_json).
    idps: Map<String, Value>,
    /// (Identifier, moto resource-server record).
    resource_servers: Vec<(String, Value)>,
    /// (TermsId, moto terms record).
    terms: Vec<(String, Value)>,
}

#[derive(Default)]
struct State {
    /// (pool id, pool) — insertion order like the python provider's dict.
    pools: Vec<(String, Pool)>,
    /// (domain, pool id).
    domains: Vec<(String, String)>,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("cognito-idp"));
    SLOT.clone()
}

fn now() -> f64 {
    (chrono::Utc::now().timestamp_millis()) as f64 / 1000.0
}

fn err(code: &str, message: &str) -> Response {
    json_err(400, code, message)
}

fn err_status(code: &str, message: &str, status: u16) -> Response {
    json_err(status, code, message)
}

fn json_err(status: u16, code: &str, message: &str) -> Response {
    let body = json!({ "__type": code, "message": message });
    Response::json(status, serde_json::to_vec(&body).unwrap_or_default())
}

fn ok_json(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

fn pool_missing(pool_id: &str) -> Response {
    err_status(
        "ResourceNotFoundException",
        &format!("User pool {pool_id} does not exist."),
        404,
    )
}

fn pool_not_found_msg(pool_id: &str) -> Response {
    err(
        "ResourceNotFoundException",
        &format!("User pool {pool_id} not found"),
    )
}

fn user_missing(username: &str) -> Response {
    err_status(
        "UserNotFoundException",
        &format!("User {username} does not exist."),
        404,
    )
}

fn user_missing_simple(username: &str) -> Response {
    err(
        "UserNotFoundException",
        &format!("User {username} not found"),
    )
}

fn group_missing(group_name: &str) -> Response {
    err_status(
        "ResourceNotFoundException",
        &format!("Group {group_name} does not exist."),
        404,
    )
}

fn group_not_found_msg(group_name: &str) -> Response {
    err(
        "ResourceNotFoundException",
        &format!("Group {group_name} not found"),
    )
}

fn client_missing(client_id: &str) -> Response {
    err_status(
        "ResourceNotFoundException",
        &format!("Client {client_id} does not exist."),
        404,
    )
}

/// `str(uuid.uuid4())[:n]` (dashes included, like the native provider).
fn uuid_prefix(n: usize) -> String {
    let u = uuid::Uuid::new_v4().to_string();
    u[..n.min(u.len())].to_string()
}

/// Structural fake JWT: unsigned payload with a fake signature (matches
/// the native provider's `_generate_jwt`, which `GetUser` decodes back).
fn jwt(issuer: &str, sub: &str, audience: &str, token_use: &str, extra: Option<&Value>) -> String {
    let now = chrono::Utc::now().timestamp();
    let mut payload = json!({
        "sub": sub,
        "iss": issuer,
        "aud": audience,
        "token_use": token_use,
        "exp": now + 3600,
        "iat": now,
        "auth_time": now,
    });
    if let (Some(e), Some(m)) = (extra, payload.as_object_mut()) {
        for (k, v) in e.as_object().into_iter().flatten() {
            m.insert(k.clone(), v.clone());
        }
    }
    let enc = |d: &Value| B64.encode(serde_json::to_vec(d).unwrap_or_default());
    let sig = B64.encode(b"fake-signature");
    format!(
        "{}.{}.{}",
        enc(&json!({"alg": "RS256", "typ": "JWT"})),
        enc(&payload),
        sig
    )
}

/// Decode a token the way `_user_from_token` does (b64url payload).
fn token_payload(access_token: &str) -> Option<Value> {
    let part = access_token.split('.').nth(1)?;
    let padded = match part.len() % 4 {
        2 => format!("{part}=="),
        3 => format!("{part}="),
        _ => part.to_string(),
    };
    // Tokens are minted unpadded; decoding tolerates padding, so route
    // through the padded URL-safe engine once normalized.
    base64::engine::general_purpose::URL_SAFE
        .decode(padded.as_bytes())
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
}

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "cognito-idp" {
        return None;
    }
    let op = req.operation.as_str();
    if op.is_empty() {
        return None;
    }
    let p = parse_json_body(&req.body);
    let region = req.region.clone();
    let acct = req.account.to_string();

    let slot = state();
    let mut st = slot.write();
    let s = st.entry((req.account, req.region.clone())).or_default();

    match op {
        "CreateUserPool" => Some(create_user_pool(s, &p, &region, &acct)),
        "DescribeUserPool" => Some(describe_user_pool(s, &p)),
        "DeleteUserPool" => Some(delete_user_pool(s, &p)),
        "ListUserPools" => Some(list_user_pools(s, &p)),
        "UpdateUserPool" => Some(update_user_pool(s, &p)),
        "AddCustomAttributes" => Some(add_custom_attributes(s, &p)),
        "GetUserPoolMfaConfig" => Some(get_user_pool_mfa_config(s, &p)),
        "SetUserPoolMfaConfig" => Some(set_user_pool_mfa_config(s, &p)),
        "CreateUserPoolClient" => Some(create_user_pool_client(s, &p)),
        "DescribeUserPoolClient" => Some(describe_user_pool_client(s, &p)),
        "DeleteUserPoolClient" => Some(delete_user_pool_client(s, &p)),
        "UpdateUserPoolClient" => Some(update_user_pool_client(s, &p)),
        "ListUserPoolClients" => Some(list_user_pool_clients(s, &p)),
        "SignUp" => Some(sign_up(s, &p)),
        "AdminConfirmSignUp" | "ConfirmSignUp" => Some(confirm_sign_up(s, &p, op)),
        "InitiateAuth" => Some(initiate_auth(s, &p, &region)),
        "AdminInitiateAuth" => Some(admin_initiate_auth(s, &p, &region)),
        "RespondToAuthChallenge" => Some(respond_to_auth_challenge(s, &p, &region)),
        "GetUser" => Some(get_user(s, &p)),
        "AdminGetUser" => Some(admin_get_user(s, &p)),
        "AdminCreateUser" => Some(admin_create_user(s, &p)),
        "AdminDisableUser" => Some(admin_toggle_user(s, &p, false)),
        "AdminEnableUser" => Some(admin_toggle_user(s, &p, true)),
        "AdminDeleteUser" => Some(admin_delete_user(s, &p)),
        "ForgotPassword" => Some(forgot_password(s, &p)),
        "ConfirmForgotPassword" => Some(confirm_forgot_password(s, &p)),
        "ChangePassword" => Some(change_password(s, &p)),
        "AdminSetUserPassword" => Some(admin_set_user_password(s, &p)),
        "ListUsers" => Some(list_users(s, &p)),
        "AdminUpdateUserAttributes" => Some(admin_update_user_attributes(s, &p)),
        "AdminDeleteUserAttributes" => Some(admin_delete_user_attributes(s, &p)),
        "AdminResetUserPassword" => Some(admin_reset_user_password(s, &p)),
        "AdminSetUserMFAPreference" => Some(admin_set_user_mfa_preference(s, &p)),
        "AdminUserGlobalSignOut" => Some(admin_user_global_sign_out(s, &p)),
        "GlobalSignOut" => Some(global_sign_out(s, &p)),
        "UpdateUserAttributes" => Some(update_user_attributes(s, &p)),
        "CreateGroup" => Some(create_group(s, &p)),
        "GetGroup" => Some(get_group(s, &p)),
        "DeleteGroup" => Some(delete_group(s, &p)),
        "ListGroups" => Some(list_groups(s, &p)),
        "UpdateGroup" => Some(update_group(s, &p)),
        "AdminAddUserToGroup" => Some(admin_add_user_to_group(s, &p)),
        "AdminRemoveUserFromGroup" => Some(admin_remove_user_from_group(s, &p)),
        "AdminListGroupsForUser" => Some(admin_list_groups_for_user(s, &p)),
        "ListUsersInGroup" => Some(list_users_in_group(s, &p)),
        "TagResource" => Some(tag_resource(s, &p)),
        "UntagResource" => Some(untag_resource(s, &p)),
        "ListTagsForResource" => Some(list_tags_for_resource(s, &p)),
        "CreateUserPoolDomain" => Some(create_user_pool_domain(s, &p)),
        "DescribeUserPoolDomain" => Some(describe_user_pool_domain(s, &p, &region, &acct)),
        "UpdateUserPoolDomain" => Some(update_user_pool_domain(s, &p)),
        "DeleteUserPoolDomain" => Some(delete_user_pool_domain(s, &p)),
        // moto-fallback ops
        "CreateIdentityProvider" => Some(create_identity_provider(s, &p)),
        "DescribeIdentityProvider" => Some(describe_identity_provider(s, &p)),
        "UpdateIdentityProvider" => Some(update_identity_provider(s, &p)),
        "DeleteIdentityProvider" => Some(delete_identity_provider(s, &p)),
        "ListIdentityProviders" => Some(list_identity_providers(s, &p)),
        "CreateResourceServer" => Some(create_resource_server(s, &p)),
        "DescribeResourceServer" => Some(describe_resource_server(s, &p)),
        "ListResourceServers" => Some(list_resource_servers(s, &p)),
        "CreateTerms" => Some(create_terms(s, &p)),
        "DescribeTerms" => Some(describe_terms(s, &p)),
        "UpdateTerms" => Some(update_terms(s, &p)),
        "DeleteTerms" => Some(delete_terms(s, &p)),
        "ListTerms" => Some(list_terms(s, &p)),
        _ => None,
    }
}

fn pool_ref<'a>(s: &'a State, pool_id: &str) -> Option<&'a Pool> {
    s.pools.iter().find(|(k, _)| *k == pool_id).map(|(_, v)| v)
}

/// `require_pool`/`_require_pool` — missing pools are a 404.
fn pool_mut<'a>(s: &'a mut State, pool_id: &str) -> Option<&'a mut Pool> {
    s.pools
        .iter_mut()
        .find(|(k, _)| *k == pool_id)
        .map(|(_, v)| v)
}

fn need_pool<'a>(s: &'a mut State, pool_id: &str) -> Result<&'a mut Pool, Response> {
    pool_mut(s, pool_id).ok_or_else(|| pool_missing(pool_id))
}

fn need_pool_ref<'a>(s: &'a State, pool_id: &str) -> Result<&'a Pool, Response> {
    pool_ref(s, pool_id).ok_or_else(|| pool_missing(pool_id))
}

fn find_pool_by_client<'a>(s: &'a State, client_id: &str) -> Option<&'a Pool> {
    s.pools
        .iter()
        .find(|(_, p)| p.clients.iter().any(|(k, _)| *k == client_id))
        .map(|(_, v)| v)
}

// ---------------------------------------------------------------------------
// User pools
// ---------------------------------------------------------------------------

fn create_user_pool(s: &mut State, p: &Value, region: &str, account: &str) -> Response {
    let pool_name = p["PoolName"].as_str().unwrap_or_default();
    if pool_name.is_empty() {
        return err("InvalidParameterException", "PoolName is required.");
    }
    let pool_id = format!("{region}_{}", uuid_prefix(8));
    let mut pool = json!({
        "Id": pool_id,
        "Name": pool_name,
        "Arn": format!("arn:aws:cognito-idp:{region}:{account}:userpool/{pool_id}"),
        "CreationDate": now(),
        "LastModifiedDate": now(),
        "Status": "Enabled",
        "Policies": if p["Policies"].is_null() { json!({}) } else { p["Policies"].clone() },
        "LambdaConfig": if p["LambdaConfig"].is_null() { json!({}) } else { p["LambdaConfig"].clone() },
        "AutoVerifiedAttributes": if p["AutoVerifiedAttributes"].is_null() { json!([]) } else { p["AutoVerifiedAttributes"].clone() },
        "Schema": if p["Schema"].is_null() { json!([]) } else { p["Schema"].clone() },
        "MfaConfiguration": if p["MfaConfiguration"].is_null() { json!("OFF") } else { p["MfaConfiguration"].clone() },
    });
    for key in [
        "EmailConfiguration",
        "SmsConfiguration",
        "AdminCreateUserConfig",
        "UsernameAttributes",
    ] {
        if let Some(v) = p.get(key).filter(|v| !v.is_null()) {
            pool[key] = v.clone();
        }
    }
    s.pools.push((
        pool_id.clone(),
        Pool {
            pool,
            ..Default::default()
        },
    ));
    let stored = pool_ref(s, &pool_id).unwrap().pool.clone();
    ok_json(&json!({ "UserPool": stored }))
}

/// Body of DescribeUserPool / Create response: pool with Schema renamed to
/// SchemaAttributes (the describe path only; create keeps "Schema").
fn user_pool_json(p: &Value, rename_schema: bool) -> Value {
    let mut result = p.clone();
    if rename_schema {
        if let Some(obj) = result.as_object_mut() {
            if let Some(schema) = obj.remove("Schema") {
                obj.insert("SchemaAttributes".into(), schema);
            }
        }
    }
    result
}

fn describe_user_pool(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    let Some(pl) = pool_mut(s, &pool_id) else {
        return pool_missing(&pool_id);
    };
    ok_json(&json!({ "UserPool": user_pool_json(&pl.pool, true) }))
}

fn delete_user_pool(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    let Some(idx) = s.pools.iter().position(|(k, _)| *k == pool_id) else {
        return pool_missing(&pool_id);
    };
    s.pools.remove(idx);
    s.domains.retain(|(_, pid)| *pid != pool_id);
    ok_json(&json!({}))
}

fn list_user_pools(s: &State, p: &Value) -> Response {
    let max_results = p["MaxResults"].as_i64().unwrap_or(60).max(0) as usize;
    let pools: Vec<Value> = s
        .pools
        .iter()
        .take(max_results)
        .map(|(_, pl)| {
            json!({
                "Id": pl.pool["Id"].clone(),
                "Name": pl.pool["Name"].clone(),
                "Status": pl.pool["Status"].clone(),
                "CreationDate": pl.pool["CreationDate"].clone(),
                "LastModifiedDate": pl.pool["LastModifiedDate"].clone(),
            })
        })
        .collect();
    ok_json(&json!({ "UserPools": pools }))
}

fn update_user_pool(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    let Some(pl) = pool_mut(s, &pool_id) else {
        return pool_not_found_msg(&pool_id);
    };
    for key in [
        "Policies",
        "LambdaConfig",
        "AutoVerifiedAttributes",
        "MfaConfiguration",
        "EmailConfiguration",
        "SmsConfiguration",
        "AdminCreateUserConfig",
    ] {
        if let Some(v) = p.get(key).filter(|v| !v.is_null()) {
            pl.pool[key] = v.clone();
        }
    }
    pl.pool["LastModifiedDate"] = json!(now());
    ok_json(&json!({}))
}

fn add_custom_attributes(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    let Some(pl) = pool_mut(s, &pool_id) else {
        return pool_not_found_msg(&pool_id);
    };
    let mut schema = pl.pool["Schema"].as_array().cloned().unwrap_or_default();
    if let Some(attrs) = p["CustomAttributes"].as_array() {
        for attr in attrs {
            let base = attr["Name"].as_str().unwrap_or_default();
            let name = if base.starts_with("custom:") {
                base.to_string()
            } else {
                format!("custom:{base}")
            };
            schema.push(json!({
                "Name": name,
                "AttributeDataType": attr["AttributeDataType"].clone(),
                "Mutable": attr["Mutable"].clone(),
                "Required": false,
            }));
        }
    }
    pl.pool["Schema"] = Value::Array(schema);
    ok_json(&json!({}))
}

fn get_user_pool_mfa_config(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    let Some(pl) = pool_mut(s, &pool_id) else {
        return pool_not_found_msg(&pool_id);
    };
    let mut result = Map::new();
    result.insert(
        "MfaConfiguration".into(),
        pl.pool["MfaConfiguration"].clone(),
    );
    for key in ["SmsMfaConfiguration", "SoftwareTokenMfaConfiguration"] {
        if let Some(v) = pl.pool.get(key) {
            if !v.is_null() {
                result.insert(key.into(), v.clone());
            }
        }
    }
    ok_json(&Value::Object(result))
}

fn set_user_pool_mfa_config(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    let Some(pl) = pool_mut(s, &pool_id) else {
        return pool_not_found_msg(&pool_id);
    };
    let mfa_config = p["MfaConfiguration"].as_str().unwrap_or("OFF");
    pl.pool["MfaConfiguration"] = json!(mfa_config);
    let mut result = Map::new();
    result.insert("MfaConfiguration".into(), json!(mfa_config));
    for key in ["SmsMfaConfiguration", "SoftwareTokenMfaConfiguration"] {
        if let Some(v) = p.get(key).filter(|v| !v.is_null()) {
            pl.pool[key] = v.clone();
            result.insert(key.into(), v.clone());
        }
    }
    ok_json(&Value::Object(result))
}

// ---------------------------------------------------------------------------
// User pool clients
// ---------------------------------------------------------------------------

fn new_client(p: &Value, pool_id: &str) -> Value {
    let mut client_id = uuid::Uuid::new_v4().to_string().replace('-', "");
    client_id.truncate(26);
    let mut client = json!({
        "ClientId": client_id,
        "ClientName": p["ClientName"].clone(),
        "UserPoolId": pool_id,
        "CreationDate": now(),
        "LastModifiedDate": now(),
        "ExplicitAuthFlows": if p["ExplicitAuthFlows"].is_null() { json!([]) } else { p["ExplicitAuthFlows"].clone() },
        "AllowedOAuthFlows": if p["AllowedOAuthFlows"].is_null() { json!([]) } else { p["AllowedOAuthFlows"].clone() },
        "AllowedOAuthScopes": if p["AllowedOAuthScopes"].is_null() { json!([]) } else { p["AllowedOAuthScopes"].clone() },
        "CallbackURLs": if p["CallbackURLs"].is_null() { json!([]) } else { p["CallbackURLs"].clone() },
        "LogoutURLs": if p["LogoutURLs"].is_null() { json!([]) } else { p["LogoutURLs"].clone() },
        "DefaultRedirectURI": if p["DefaultRedirectURI"].is_null() { json!("") } else { p["DefaultRedirectURI"].clone() },
        "ReadAttributes": if p["ReadAttributes"].is_null() { json!([]) } else { p["ReadAttributes"].clone() },
        "WriteAttributes": if p["WriteAttributes"].is_null() { json!([]) } else { p["WriteAttributes"].clone() },
        "SupportedIdentityProviders": if p["SupportedIdentityProviders"].is_null() { json!([]) } else { p["SupportedIdentityProviders"].clone() },
        "AllowedOAuthFlowsUserPoolClient": p["AllowedOAuthFlowsUserPoolClient"].clone(),
        "TokenValidityUnits": if p["TokenValidityUnits"].is_null() { json!({}) } else { p["TokenValidityUnits"].clone() },
        "AccessTokenValidity": p["AccessTokenValidity"].clone(),
        "IdTokenValidity": p["IdTokenValidity"].clone(),
        "RefreshTokenValidity": p["RefreshTokenValidity"].clone(),
    });
    if p["GenerateSecret"].as_bool().unwrap_or(false) {
        client["ClientSecret"] = json!(uuid::Uuid::new_v4().to_string());
    }
    client
}

fn create_user_pool_client(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    if let Err(e) = need_pool(s, &pool_id) {
        return e;
    }
    let client = new_client(p, &pool_id);
    let cid = client["ClientId"].as_str().unwrap_or_default().to_string();
    pool_mut(s, &pool_id)
        .unwrap()
        .clients
        .push((cid, client.clone()));
    ok_json(&json!({ "UserPoolClient": client }))
}

fn describe_user_pool_client(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    let client_id = p["ClientId"].as_str().unwrap_or_default().to_string();
    if let Err(e) = need_pool(s, &pool_id) {
        return e;
    }
    let pl = pool_ref(s, &pool_id).unwrap();
    match pl.clients.iter().find(|(k, _)| *k == client_id) {
        Some((_, c)) => ok_json(&json!({ "UserPoolClient": c })),
        None => client_missing(&client_id),
    }
}

fn delete_user_pool_client(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    let client_id = p["ClientId"].as_str().unwrap_or_default().to_string();
    if let Err(e) = need_pool(s, &pool_id) {
        return e;
    }
    let pl = pool_mut(s, &pool_id).unwrap();
    if pl.clients.iter().all(|(k, _)| *k != client_id) {
        return client_missing(&client_id);
    }
    pl.clients.retain(|(k, _)| *k != client_id);
    ok_json(&json!({}))
}

fn update_user_pool_client(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    let client_id = p["ClientId"].as_str().unwrap_or_default().to_string();
    if let Err(e) = need_pool(s, &pool_id) {
        return e;
    }
    const UPDATABLE: [&str; 15] = [
        "ClientName",
        "ExplicitAuthFlows",
        "AllowedOAuthFlows",
        "AllowedOAuthScopes",
        "CallbackURLs",
        "LogoutURLs",
        "DefaultRedirectURI",
        "ReadAttributes",
        "WriteAttributes",
        "SupportedIdentityProviders",
        "AllowedOAuthFlowsUserPoolClient",
        "TokenValidityUnits",
        "AccessTokenValidity",
        "IdTokenValidity",
        "RefreshTokenValidity",
    ];
    let client = {
        let pl = pool_mut(s, &pool_id).unwrap();
        let Some((_, c)) = pl.clients.iter_mut().find(|(k, _)| *k == client_id) else {
            return client_missing(&client_id);
        };
        for key in UPDATABLE {
            if let Some(v) = p.get(key).filter(|v| !v.is_null()) {
                c[key] = v.clone();
            }
        }
        c["LastModifiedDate"] = json!(now());
        c.clone()
    };
    ok_json(&json!({ "UserPoolClient": client }))
}

fn list_user_pool_clients(s: &State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default();
    let pl = match need_pool_ref(s, pool_id) {
        Ok(pl) => pl,
        Err(e) => return e,
    };
    let clients: Vec<Value> = pl
        .clients
        .iter()
        .map(|(_, c)| {
            json!({
                "ClientId": c["ClientId"].clone(),
                "ClientName": c["ClientName"].clone(),
                "UserPoolId": c["UserPoolId"].clone(),
            })
        })
        .collect();
    ok_json(&json!({ "UserPoolClients": clients }))
}

// ---------------------------------------------------------------------------
// Users & auth
// ---------------------------------------------------------------------------

/// `_user_from_token`: find (user record, pool id) by JWT sub claim.
fn user_from_token<'a>(
    s: &'a mut State,
    access_token: &str,
) -> Result<(&'a mut Value, String), Response> {
    let Some(payload) = token_payload(access_token) else {
        return Err(err("NotAuthorizedException", "Invalid access token."));
    };
    let sub = payload["sub"].as_str().unwrap_or_default().to_string();
    for (_, pl) in s.pools.iter_mut() {
        for (_uname, user) in pl.users.iter_mut() {
            if user["UserSub"].as_str() == Some(sub.as_str()) {
                return Ok((user, pl.pool["Id"].as_str().unwrap_or_default().to_string()));
            }
        }
    }
    Err(err("NotAuthorizedException", "Invalid access token."))
}

fn sign_up(s: &mut State, p: &Value) -> Response {
    let client_id = p["ClientId"].as_str().unwrap_or_default().to_string();
    let pool_id = match find_pool_by_client(s, &client_id) {
        Some(pool) => pool.pool["Id"].as_str().unwrap_or_default().to_string(),
        None => return client_missing(&client_id),
    };
    let username = p["Username"].as_str().unwrap_or_default().to_string();
    let user_sub = uuid::Uuid::new_v4().to_string();
    {
        let pl = pool_ref(s, &pool_id).unwrap();
        if pl.users.iter().any(|(k, _)| *k == username) {
            return err(
                "UsernameExistsException",
                &format!("User {username} already exists."),
            );
        }
    }
    let user = json!({
        "Username": username,
        "UserSub": user_sub,
        "Password": p["Password"].clone(),
        "Enabled": true,
        "UserStatus": "UNCONFIRMED",
        "Attributes": if p["UserAttributes"].is_null() { json!([]) } else { p["UserAttributes"].clone() },
        "CreationDate": now(),
        "LastModifiedDate": now(),
        "MFAOptions": [],
    });
    pool_mut(s, &pool_id).unwrap().users.push((username, user));
    ok_json(&json!({ "UserConfirmed": false, "UserSub": user_sub }))
}

fn confirm_sign_up(s: &mut State, p: &Value, _op: &str) -> Response {
    // AdminConfirmSignUp keys on UserPoolId; ConfirmSignUp on ClientId.
    let (pool_id, username) = if p["UserPoolId"].is_null() {
        let client_id = p["ClientId"].as_str().unwrap_or_default().to_string();
        let pool_id = match find_pool_by_client(s, &client_id) {
            Some(pool) => pool.pool["Id"].as_str().unwrap_or_default().to_string(),
            None => return client_missing(&client_id),
        };
        (
            pool_id,
            p["Username"].as_str().unwrap_or_default().to_string(),
        )
    } else {
        (
            p["UserPoolId"].as_str().unwrap_or_default().to_string(),
            p["Username"].as_str().unwrap_or_default().to_string(),
        )
    };
    if let Err(e) = need_pool(s, &pool_id) {
        return e;
    }
    let pl = pool_mut(s, &pool_id).unwrap();
    let Some(user) = pl
        .users
        .iter_mut()
        .find(|(k, _)| *k == username)
        .map(|(_, v)| v)
    else {
        return user_missing(&username);
    };
    user["UserStatus"] = json!("CONFIRMED");
    user["LastModifiedDate"] = json!(now());
    ok_json(&json!({}))
}

fn issue_tokens(issuer: &str, sub: &str, client_id: &str, username: &str) -> Value {
    json!({
        "AccessToken": jwt(issuer, sub, client_id, "access", None),
        "IdToken": jwt(
            issuer,
            sub,
            client_id,
            "id",
            Some(&json!({ "cognito:username": username })),
        ),
        "RefreshToken": uuid::Uuid::new_v4().to_string(),
        "TokenType": "Bearer",
        "ExpiresIn": 3600,
    })
}

fn pool_region(pool_id: &str) -> &str {
    match pool_id.split_once('_') {
        Some((r, _)) => r,
        None => "us-east-1",
    }
}

fn authenticate_user(
    s: &State,
    pool_id: &str,
    client_id: &str,
    username: &str,
    password: &str,
) -> Response {
    let Some(pl) = pool_ref(s, pool_id) else {
        return err("NotAuthorizedException", "Incorrect username or password.");
    };
    let Some(user) = pl
        .users
        .iter()
        .find(|(k, _)| *k == username)
        .map(|(_, v)| v)
    else {
        return err("NotAuthorizedException", "Incorrect username or password.");
    };
    if user["Password"].as_str().unwrap_or_default() != password {
        return err("NotAuthorizedException", "Incorrect username or password.");
    }
    if user["UserStatus"].as_str() == Some("UNCONFIRMED") {
        return err("UserNotConfirmedException", "User is not confirmed.");
    }
    if user["UserStatus"].as_str() == Some("FORCE_CHANGE_PASSWORD") {
        return ok_json(&json!({
            "ChallengeName": "NEW_PASSWORD_REQUIRED",
            "ChallengeParameters": {
                "USER_ID_FOR_SRP": username,
                "requiredAttributes": "[]",
            },
            "Session": uuid::Uuid::new_v4().to_string(),
        }));
    }
    let issuer = format!(
        "https://cognito-idp.{}.amazonaws.com/{pool_id}",
        pool_region(pool_id)
    );
    let sub = user["UserSub"].as_str().unwrap_or_default().to_string();
    ok_json(&json!({
        "AuthenticationResult": issue_tokens(&issuer, &sub, client_id, username),
        "ChallengeParameters": {},
    }))
}

fn initiate_auth(s: &mut State, p: &Value, _region: &str) -> Response {
    let auth_flow = p["AuthFlow"].as_str().unwrap_or_default();
    let client_id = p["ClientId"].as_str().unwrap_or_default().to_string();
    if auth_flow != "USER_PASSWORD_AUTH" {
        return err(
            "InvalidParameterException",
            &format!("Unsupported auth flow: {auth_flow}"),
        );
    }
    let pool_id = match find_pool_by_client(s, &client_id) {
        Some(pool) => pool.pool["Id"].as_str().unwrap_or_default().to_string(),
        None => return client_missing(&client_id),
    };
    authenticate_user(
        s,
        &pool_id,
        &client_id,
        p["AuthParameters"]["USERNAME"].as_str().unwrap_or_default(),
        p["AuthParameters"]["PASSWORD"].as_str().unwrap_or_default(),
    )
}

fn admin_initiate_auth(s: &mut State, p: &Value, _region: &str) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    let client_id = p["ClientId"].as_str().unwrap_or_default().to_string();
    let auth_flow = p["AuthFlow"].as_str().unwrap_or_default();
    if let Err(e) = need_pool(s, &pool_id) {
        return e;
    }
    if !matches!(auth_flow, "ADMIN_USER_PASSWORD_AUTH" | "USER_PASSWORD_AUTH") {
        return err(
            "InvalidParameterException",
            &format!("Unsupported auth flow: {auth_flow}"),
        );
    }
    authenticate_user(
        s,
        &pool_id,
        &client_id,
        p["AuthParameters"]["USERNAME"].as_str().unwrap_or_default(),
        p["AuthParameters"]["PASSWORD"].as_str().unwrap_or_default(),
    )
}

fn respond_to_auth_challenge(s: &mut State, p: &Value, _region: &str) -> Response {
    let challenge = p["ChallengeName"].as_str().unwrap_or_default();
    let client_id = p["ClientId"].as_str().unwrap_or_default().to_string();
    if challenge != "NEW_PASSWORD_REQUIRED" {
        return err(
            "InvalidParameterException",
            &format!("Unsupported challenge: {challenge}"),
        );
    }
    let pool_id = match find_pool_by_client(s, &client_id) {
        Some(pool) => pool.pool["Id"].as_str().unwrap_or_default().to_string(),
        None => return client_missing(&client_id),
    };
    let username = p["ChallengeResponses"]["USERNAME"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let new_password = p["ChallengeResponses"]["NEW_PASSWORD"]
        .as_str()
        .unwrap_or_default();
    let (sub, issuer) = {
        let pl = pool_mut(s, &pool_id).unwrap();
        let Some(user) = pl
            .users
            .iter_mut()
            .find(|(k, _)| *k == username)
            .map(|(_, v)| v)
        else {
            return user_missing(&username);
        };
        user["Password"] = json!(new_password);
        user["UserStatus"] = json!("CONFIRMED");
        user["LastModifiedDate"] = json!(now());
        let sub = user["UserSub"].as_str().unwrap_or_default().to_string();
        let issuer = format!(
            "https://cognito-idp.{}.amazonaws.com/{pool_id}",
            pool_region(&pool_id)
        );
        (sub, issuer)
    };
    ok_json(&json!({
        "AuthenticationResult": issue_tokens(&issuer, &sub, &client_id, &username),
        "ChallengeParameters": {},
    }))
}

fn get_user(s: &State, p: &Value) -> Response {
    let access_token = p["AccessToken"].as_str().unwrap_or_default();
    if access_token.is_empty() {
        return err("NotAuthorizedException", "Missing access token.");
    }
    let Some(payload) = token_payload(access_token) else {
        return err("NotAuthorizedException", "Invalid access token.");
    };
    let sub = payload["sub"].as_str().unwrap_or_default().to_string();
    for (_, pl) in s.pools.iter() {
        for (username, user) in pl.users.iter() {
            if user["UserSub"].as_str() == Some(sub.as_str()) {
                return ok_json(&json!({
                    "Username": username,
                    "UserAttributes": user["Attributes"].clone(),
                    "UserCreateDate": user["CreationDate"].clone(),
                    "UserLastModifiedDate": user["LastModifiedDate"].clone(),
                    "Enabled": user["Enabled"].clone(),
                    "UserStatus": user["UserStatus"].clone(),
                }));
            }
        }
    }
    err("UserNotFoundException", "User not found.")
}

fn admin_user_json(user: &Value) -> Value {
    json!({
        "Username": user["Username"].clone(),
        "UserAttributes": user["Attributes"].clone(),
        "UserCreateDate": user["CreationDate"].clone(),
        "UserLastModifiedDate": user["LastModifiedDate"].clone(),
        "Enabled": user["Enabled"].clone(),
        "UserStatus": user["UserStatus"].clone(),
    })
}

fn admin_get_user(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    let username = p["Username"].as_str().unwrap_or_default().to_string();
    if let Err(e) = need_pool(s, &pool_id) {
        return e;
    }
    let pl = pool_ref(s, &pool_id).unwrap();
    match pl
        .users
        .iter()
        .find(|(k, _)| *k == username)
        .map(|(_, v)| v)
    {
        Some(u) => ok_json(&admin_user_json(u)),
        None => user_missing(&username),
    }
}

fn admin_create_user(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    let username = p["Username"].as_str().unwrap_or_default().to_string();
    if let Err(e) = need_pool(s, &pool_id) {
        return e;
    }
    let temp_password = p["TemporaryPassword"]
        .as_str()
        .unwrap_or("TempPass1!")
        .to_string();
    let user_sub = uuid::Uuid::new_v4().to_string();
    let created = now();
    {
        let pl = pool_ref(s, &pool_id).unwrap();
        if pl.users.iter().any(|(k, _)| *k == username) {
            return err(
                "UsernameExistsException",
                &format!("User {username} already exists."),
            );
        }
    }
    let user = json!({
        "Username": username,
        "UserSub": user_sub,
        "Password": temp_password,
        "Enabled": true,
        "UserStatus": "FORCE_CHANGE_PASSWORD",
        "Attributes": if p["UserAttributes"].is_null() { json!([]) } else { p["UserAttributes"].clone() },
        "CreationDate": created,
        "LastModifiedDate": created,
        "MFAOptions": [],
    });
    pool_mut(s, &pool_id)
        .unwrap()
        .users
        .push((username.clone(), user));
    ok_json(&json!({
        "User": {
            "Username": username,
            "Attributes": p["UserAttributes"].clone(),
            "UserCreateDate": created,
            "UserLastModifiedDate": created,
            "Enabled": true,
            "UserStatus": "FORCE_CHANGE_PASSWORD",
        }
    }))
}

fn admin_toggle_user(s: &mut State, p: &Value, enable: bool) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    let username = p["Username"].as_str().unwrap_or_default().to_string();
    if let Err(e) = need_pool(s, &pool_id) {
        return e;
    }
    let pl = pool_mut(s, &pool_id).unwrap();
    let Some(user) = pl
        .users
        .iter_mut()
        .find(|(k, _)| *k == username)
        .map(|(_, v)| v)
    else {
        return user_missing(&username);
    };
    user["Enabled"] = json!(enable);
    user["LastModifiedDate"] = json!(now());
    ok_json(&json!({}))
}

fn admin_delete_user(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    let username = p["Username"].as_str().unwrap_or_default().to_string();
    if let Err(e) = need_pool(s, &pool_id) {
        return e;
    }
    let pl = pool_mut(s, &pool_id).unwrap();
    if pl.users.iter().all(|(k, _)| *k != username) {
        return user_missing(&username);
    }
    pl.users.retain(|(k, _)| *k != username);
    pl.user_groups.remove(&username);
    ok_json(&json!({}))
}

fn forgot_password(s: &mut State, p: &Value) -> Response {
    let client_id = p["ClientId"].as_str().unwrap_or_default().to_string();
    let username = p["Username"].as_str().unwrap_or_default().to_string();
    let pool_id = match find_pool_by_client(s, &client_id) {
        Some(pool) => pool.pool["Id"].as_str().unwrap_or_default().to_string(),
        None => return client_missing(&client_id),
    };
    let pl = pool_ref(s, &pool_id).unwrap();
    if pl.users.iter().all(|(k, _)| *k != username) {
        return user_missing(&username);
    }
    ok_json(&json!({
        "CodeDeliveryDetails": {
            "Destination": "***",
            "DeliveryMedium": "EMAIL",
            "AttributeName": "email",
        }
    }))
}

fn confirm_forgot_password(s: &mut State, p: &Value) -> Response {
    let client_id = p["ClientId"].as_str().unwrap_or_default().to_string();
    let username = p["Username"].as_str().unwrap_or_default().to_string();
    let pool_id = match find_pool_by_client(s, &client_id) {
        Some(pool) => pool.pool["Id"].as_str().unwrap_or_default().to_string(),
        None => return client_missing(&client_id),
    };
    let pl = pool_mut(s, &pool_id).unwrap();
    let Some(user) = pl
        .users
        .iter_mut()
        .find(|(k, _)| *k == username)
        .map(|(_, v)| v)
    else {
        return user_missing(&username);
    };
    user["Password"] = p["Password"].clone();
    user["LastModifiedDate"] = json!(now());
    ok_json(&json!({}))
}

fn change_password(s: &mut State, p: &Value) -> Response {
    let access_token = p["AccessToken"].as_str().unwrap_or_default();
    if access_token.is_empty() {
        return err("NotAuthorizedException", "Missing access token.");
    }
    let (user, _) = match user_from_token(s, access_token) {
        Ok(v) => v,
        Err(e) => return e,
    };
    if user["Password"].as_str().unwrap_or_default()
        != p["PreviousPassword"].as_str().unwrap_or_default()
    {
        return err("NotAuthorizedException", "Incorrect password.");
    }
    user["Password"] = p["ProposedPassword"].clone();
    user["LastModifiedDate"] = json!(now());
    ok_json(&json!({}))
}

fn admin_set_user_password(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    let username = p["Username"].as_str().unwrap_or_default().to_string();
    if let Err(e) = need_pool(s, &pool_id) {
        return e;
    }
    let permanent = p["Permanent"].as_bool().unwrap_or(false);
    let pl = pool_mut(s, &pool_id).unwrap();
    let Some(user) = pl
        .users
        .iter_mut()
        .find(|(k, _)| *k == username)
        .map(|(_, v)| v)
    else {
        return user_missing(&username);
    };
    user["Password"] = p["Password"].clone();
    if permanent {
        user["UserStatus"] = json!("CONFIRMED");
    }
    user["LastModifiedDate"] = json!(now());
    ok_json(&json!({}))
}

/// Simple filter `attr = "value"` / `attr ^= "value"` per native provider.
fn filter_users<'a>(users: &'a [(String, Value)], filter: &str) -> Vec<&'a Value> {
    let parts: Vec<&str> = filter.split_whitespace().collect();
    if parts.len() < 3 {
        return users.iter().map(|(_, v)| v).collect();
    }
    let attr_name = parts[0];
    let operator = parts[1];
    let value = parts[2..].join(" ");
    let value = value.trim_matches('"').trim_matches('\'');
    users
        .iter()
        .filter(|(_, u)| {
            let user_val = if attr_name == "username" {
                u["Username"].as_str().unwrap_or_default().to_string()
            } else {
                u["Attributes"]
                    .as_array()
                    .map(|attrs| {
                        attrs
                            .iter()
                            .find(|a| a["Name"].as_str() == Some(attr_name))
                            .and_then(|a| a["Value"].as_str().map(str::to_string))
                            .unwrap_or_default()
                    })
                    .unwrap_or_default()
            };
            match operator {
                "=" => user_val == value,
                "^=" => user_val.starts_with(value),
                _ => false,
            }
        })
        .map(|(_, v)| v)
        .collect()
}

fn list_users(s: &State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default();
    let pl = match need_pool_ref(s, pool_id) {
        Ok(pl) => pl,
        Err(e) => return e,
    };
    let matched = match p["Filter"].as_str() {
        Some(f) if !f.is_empty() => filter_users(&pl.users, f),
        _ => pl.users.iter().map(|(_, v)| v).collect(),
    };
    let users: Vec<Value> = matched
        .into_iter()
        .map(|u| {
            json!({
                "Username": u["Username"].clone(),
                "Attributes": u["Attributes"].clone(),
                "UserCreateDate": u["CreationDate"].clone(),
                "UserLastModifiedDate": u["LastModifiedDate"].clone(),
                "Enabled": u["Enabled"].clone(),
                "UserStatus": u["UserStatus"].clone(),
            })
        })
        .collect();
    ok_json(&json!({ "Users": users }))
}

fn admin_update_user_attributes(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    let username = p["Username"].as_str().unwrap_or_default().to_string();
    if let Err(e) = need_pool(s, &pool_id) {
        return e;
    }
    let pl = pool_mut(s, &pool_id).unwrap();
    let Some(user) = pl
        .users
        .iter_mut()
        .find(|(k, _)| *k == username)
        .map(|(_, v)| v)
    else {
        return user_missing_simple(&username);
    };
    let mut existing: Map<String, Value> = user["Attributes"]
        .as_array()
        .map(|attrs| {
            attrs
                .iter()
                .filter_map(|a| Some((a["Name"].as_str()?.to_string(), a["Value"].clone())))
                .collect()
        })
        .unwrap_or_default();
    if let Some(new_attrs) = p["UserAttributes"].as_array() {
        for a in new_attrs {
            existing.insert(
                a["Name"].as_str().unwrap_or_default().to_string(),
                a["Value"].clone(),
            );
        }
    }
    user["Attributes"] = Value::Array(
        existing
            .into_iter()
            .map(|(n, v)| json!({"Name": n, "Value": v}))
            .collect(),
    );
    user["LastModifiedDate"] = json!(now());
    ok_json(&json!({}))
}

fn admin_delete_user_attributes(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    let username = p["Username"].as_str().unwrap_or_default().to_string();
    if let Err(e) = need_pool(s, &pool_id) {
        return e;
    }
    let names: Vec<String> = p["UserAttributeNames"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|v| v.as_str().unwrap_or_default().to_string())
                .collect()
        })
        .unwrap_or_default();
    let pl = pool_mut(s, &pool_id).unwrap();
    let Some(user) = pl
        .users
        .iter_mut()
        .find(|(k, _)| *k == username)
        .map(|(_, v)| v)
    else {
        return user_missing(&username);
    };
    if let Some(attrs) = user["Attributes"].as_array_mut() {
        attrs.retain(|a| !names.contains(&a["Name"].as_str().unwrap_or_default().to_string()));
    }
    user["LastModifiedDate"] = json!(now());
    ok_json(&json!({}))
}

fn admin_reset_user_password(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    let username = p["Username"].as_str().unwrap_or_default().to_string();
    if let Err(e) = need_pool(s, &pool_id) {
        return e;
    }
    let pl = pool_mut(s, &pool_id).unwrap();
    let Some(user) = pl
        .users
        .iter_mut()
        .find(|(k, _)| *k == username)
        .map(|(_, v)| v)
    else {
        return user_missing(&username);
    };
    user["UserStatus"] = json!("RESET_REQUIRED");
    user["LastModifiedDate"] = json!(now());
    ok_json(&json!({}))
}

fn admin_set_user_mfa_preference(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    let username = p["Username"].as_str().unwrap_or_default().to_string();
    if let Err(e) = need_pool(s, &pool_id) {
        return e;
    }
    let pl = pool_mut(s, &pool_id).unwrap();
    let Some(user) = pl
        .users
        .iter_mut()
        .find(|(k, _)| *k == username)
        .map(|(_, v)| v)
    else {
        return user_missing(&username);
    };
    if let Some(v) = p.get("SMSMfaSettings").filter(|v| !v.is_null()) {
        user["SMSMfaSettings"] = v.clone();
    }
    if let Some(v) = p.get("SoftwareTokenMfaSettings").filter(|v| !v.is_null()) {
        user["SoftwareTokenMfaSettings"] = v.clone();
    }
    user["LastModifiedDate"] = json!(now());
    ok_json(&json!({}))
}

fn admin_user_global_sign_out(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    let username = p["Username"].as_str().unwrap_or_default().to_string();
    if let Err(e) = need_pool(s, &pool_id) {
        return e;
    }
    let pl = pool_ref(s, &pool_id).unwrap();
    if pl.users.iter().all(|(k, _)| *k != username) {
        return user_missing(&username);
    }
    ok_json(&json!({}))
}

fn global_sign_out(s: &mut State, p: &Value) -> Response {
    let access_token = p["AccessToken"].as_str().unwrap_or_default();
    if access_token.is_empty() {
        return err("NotAuthorizedException", "Missing access token.");
    }
    match user_from_token(s, access_token) {
        Ok(_) => ok_json(&json!({})),
        Err(e) => e,
    }
}

fn update_user_attributes(s: &mut State, p: &Value) -> Response {
    let access_token = p["AccessToken"].as_str().unwrap_or_default();
    if access_token.is_empty() {
        return err("NotAuthorizedException", "Missing access token.");
    }
    let new_attrs: Vec<Value> = p["UserAttributes"].as_array().cloned().unwrap_or_default();
    let update_names: Vec<String> = new_attrs
        .iter()
        .map(|a| a["Name"].as_str().unwrap_or_default().to_string())
        .collect();
    let (user, _) = match user_from_token(s, access_token) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let mut merged: Vec<Value> = user["Attributes"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|a| !update_names.contains(&a["Name"].as_str().unwrap_or_default().to_string()))
        .collect();
    merged.extend(new_attrs);
    user["Attributes"] = Value::Array(merged);
    user["LastModifiedDate"] = json!(now());
    ok_json(&json!({}))
}

// ---------------------------------------------------------------------------
// Groups
// ---------------------------------------------------------------------------

fn create_group(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    let group_name = p["GroupName"].as_str().unwrap_or_default().to_string();
    if let Err(e) = need_pool(s, &pool_id) {
        return e;
    }
    let pl = pool_mut(s, &pool_id).unwrap();
    if pl.groups.iter().any(|(k, _)| *k == group_name) {
        return err(
            "GroupExistsException",
            &format!("Group {group_name} already exists."),
        );
    }
    let group = json!({
        "GroupName": group_name,
        "UserPoolId": pool_id,
        "Description": p["Description"].as_str().unwrap_or(""),
        "RoleArn": p["RoleArn"].as_str().unwrap_or(""),
        "Precedence": p["Precedence"].clone(),
        "CreationDate": now(),
        "LastModifiedDate": now(),
    });
    pl.groups.push((group_name.clone(), group.clone()));
    ok_json(&json!({ "Group": group }))
}

fn get_group_resp(s: &State, pool_id: &str, group_name: &str) -> Option<Value> {
    let pl = pool_ref(s, pool_id)?;
    pl.groups
        .iter()
        .find(|(k, _)| *k == group_name)
        .map(|(_, v)| v.clone())
}

fn get_group(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    let group_name = p["GroupName"].as_str().unwrap_or_default().to_string();
    if let Err(e) = need_pool(s, &pool_id) {
        return e;
    }
    match get_group_resp(s, &pool_id, &group_name) {
        Some(g) => ok_json(&json!({ "Group": g })),
        None => group_missing(&group_name),
    }
}

fn delete_group(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    let group_name = p["GroupName"].as_str().unwrap_or_default().to_string();
    if let Err(e) = need_pool(s, &pool_id) {
        return e;
    }
    let pl = pool_mut(s, &pool_id).unwrap();
    if pl.groups.iter().all(|(k, _)| *k != group_name) {
        return group_missing(&group_name);
    }
    pl.groups.retain(|(k, _)| *k != group_name);
    for members in pl.user_groups.values_mut() {
        if let Some(arr) = members.as_array_mut() {
            arr.retain(|g| g.as_str() != Some(group_name.as_str()));
        }
    }
    ok_json(&json!({}))
}

fn list_groups(s: &State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default();
    let pl = match need_pool_ref(s, pool_id) {
        Ok(pl) => pl,
        Err(e) => return e,
    };
    let groups: Vec<Value> = pl.groups.iter().map(|(_, v)| v.clone()).collect();
    ok_json(&json!({ "Groups": groups }))
}

fn update_group(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    let group_name = p["GroupName"].as_str().unwrap_or_default().to_string();
    let Some(pl) = pool_mut(s, &pool_id) else {
        return group_not_found_msg(&group_name);
    };
    let Some(group) = pl
        .groups
        .iter_mut()
        .find(|(k, _)| *k == group_name)
        .map(|(_, v)| v)
    else {
        return group_not_found_msg(&group_name);
    };
    for key in ["Description", "RoleArn", "Precedence"] {
        if let Some(v) = p.get(key).filter(|v| !v.is_null()) {
            group[key] = v.clone();
        }
    }
    group["LastModifiedDate"] = json!(now());
    let g = group.clone();
    ok_json(&json!({ "Group": g }))
}

fn admin_add_user_to_group(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    let username = p["Username"].as_str().unwrap_or_default().to_string();
    let group_name = p["GroupName"].as_str().unwrap_or_default().to_string();
    if let Err(e) = need_pool(s, &pool_id) {
        return e;
    }
    let pl = pool_mut(s, &pool_id).unwrap();
    if pl.users.iter().all(|(k, _)| *k != username) {
        return user_missing(&username);
    }
    if pl.groups.iter().all(|(k, _)| *k != group_name) {
        return group_missing(&group_name);
    }
    let members = pl.user_groups.entry(username).or_insert_with(|| json!([]));
    if let Some(arr) = members.as_array_mut() {
        if !arr.iter().any(|g| g.as_str() == Some(group_name.as_str())) {
            arr.push(json!(group_name));
        }
    }
    ok_json(&json!({}))
}

fn admin_remove_user_from_group(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    let username = p["Username"].as_str().unwrap_or_default().to_string();
    let group_name = p["GroupName"].as_str().unwrap_or_default().to_string();
    if let Err(e) = need_pool(s, &pool_id) {
        return e;
    }
    let pl = pool_mut(s, &pool_id).unwrap();
    if pl.users.iter().all(|(k, _)| *k != username) {
        return user_missing(&username);
    }
    if let Some(members) = pl.user_groups.get_mut(&username) {
        if let Some(arr) = members.as_array_mut() {
            arr.retain(|g| g.as_str() != Some(group_name.as_str()));
        }
    }
    ok_json(&json!({}))
}

fn admin_list_groups_for_user(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    let username = p["Username"].as_str().unwrap_or_default().to_string();
    if let Err(e) = need_pool(s, &pool_id) {
        return e;
    }
    let pl = pool_ref(s, &pool_id).unwrap();
    if pl.users.iter().all(|(k, _)| *k != username) {
        return user_missing(&username);
    }
    let member_names = pl
        .user_groups
        .get(&username)
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let groups: Vec<Value> = member_names
        .iter()
        .filter_map(|g| {
            pl.groups
                .iter()
                .find(|(k, _)| Some(k.as_str()) == g.as_str())
                .map(|(_, v)| v.clone())
        })
        .collect();
    ok_json(&json!({ "Groups": groups }))
}

fn list_users_in_group(s: &State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default();
    let group_name = p["GroupName"].as_str().unwrap_or_default();
    let Some(pl) = pool_ref(s, pool_id) else {
        return group_not_found_msg(group_name);
    };
    if pl.groups.iter().all(|(k, _)| k != group_name) {
        return group_not_found_msg(group_name);
    }
    let users: Vec<Value> = pl
        .user_groups
        .iter()
        .filter(|(_, groups)| {
            groups
                .as_array()
                .map(|a| a.iter().any(|g| g.as_str() == Some(group_name)))
                .unwrap_or(false)
        })
        .filter_map(|(u, _)| {
            pl.users
                .iter()
                .find(|(k, _)| *k == *u)
                .map(|(_, v)| v.clone())
        })
        .collect();
    ok_json(&json!({ "Users": users }))
}

// ---------------------------------------------------------------------------
// Tags (pool ARNs)
// ---------------------------------------------------------------------------

fn pool_id_from_arn(arn: &str) -> String {
    if let Some(i) = arn.find("/userpool/") {
        return arn[i + "/userpool/".len()..].to_string();
    }
    if let Some(i) = arn.rfind('/') {
        return arn[i + 1..].to_string();
    }
    arn.to_string()
}

fn tag_resource(s: &mut State, p: &Value) -> Response {
    let pool_id = pool_id_from_arn(p["ResourceArn"].as_str().unwrap_or_default());
    let incoming = p["Tags"].as_object().cloned().unwrap_or_default();
    let pl = match need_pool(s, &pool_id) {
        Ok(pl) => pl,
        Err(e) => return e,
    };
    let obj = pl.pool.as_object_mut().unwrap();
    let tags = obj
        .entry("Tags")
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .unwrap();
    for (k, v) in incoming {
        tags.insert(k, v);
    }
    ok_json(&json!({}))
}

fn untag_resource(s: &mut State, p: &Value) -> Response {
    let pool_id = pool_id_from_arn(p["ResourceArn"].as_str().unwrap_or_default());
    let keys: Vec<String> = p["TagKeys"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|v| v.as_str().unwrap_or_default().to_string())
                .collect()
        })
        .unwrap_or_default();
    let pl = match need_pool(s, &pool_id) {
        Ok(pl) => pl,
        Err(e) => return e,
    };
    if let Some(tags) = pl
        .pool
        .as_object_mut()
        .unwrap()
        .get_mut("Tags")
        .and_then(Value::as_object_mut)
    {
        for k in keys {
            tags.remove(&k);
        }
    }
    ok_json(&json!({}))
}

fn list_tags_for_resource(s: &mut State, p: &Value) -> Response {
    let pool_id = pool_id_from_arn(p["ResourceArn"].as_str().unwrap_or_default());
    let pl = match need_pool(s, &pool_id) {
        Ok(pl) => pl,
        Err(e) => return e,
    };
    let tags = pl
        .pool
        .as_object()
        .and_then(|o| o.get("Tags"))
        .cloned()
        .unwrap_or_else(|| json!({}));
    ok_json(&json!({ "Tags": tags }))
}

// ---------------------------------------------------------------------------
// User pool domains
// ---------------------------------------------------------------------------

fn create_user_pool_domain(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    let domain = p["Domain"].as_str().unwrap_or_default().to_string();
    if let Err(e) = need_pool(s, &pool_id) {
        return e;
    }
    if domain.is_empty() {
        return err("InvalidParameterException", "Domain is required.");
    }
    if s.domains.iter().any(|(d, _)| *d == domain) {
        return err(
            "InvalidParameterException",
            &format!("Domain {domain} is already associated with a user pool."),
        );
    }
    s.domains.push((domain.clone(), pool_id.clone()));
    pool_mut(s, &pool_id).unwrap().pool["Domain"] = json!(domain);
    ok_json(&json!({}))
}

fn describe_user_pool_domain(s: &State, p: &Value, region: &str, account: &str) -> Response {
    let domain = p["Domain"].as_str().unwrap_or_default();
    match s.domains.iter().find(|(d, _)| d.as_str() == domain) {
        None => ok_json(&json!({ "DomainDescription": {} })),
        Some((_, pool_id)) => ok_json(&json!({
            "DomainDescription": {
                "UserPoolId": pool_id,
                "AWSAccountId": account,
                "Domain": domain,
                "Status": "ACTIVE",
                "CloudFrontDistribution": format!("d{}.cloudfront.net", uuid_prefix(13)),
                "S3Bucket": format!("aws-cognito-idp-{region}"),
                "Version": "20241001",
            }
        })),
    }
}

fn update_user_pool_domain(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    let domain = p["Domain"].as_str().unwrap_or_default().to_string();
    if let Err(e) = need_pool(s, &pool_id) {
        return e;
    }
    if s.domains.iter().all(|(d, _)| *d != domain) {
        return err_status("ResourceNotFoundException", &domain, 404);
    }
    pool_mut(s, &pool_id).unwrap().pool["CustomDomainConfig"] = p["CustomDomainConfig"].clone();
    ok_json(&json!({
        "CloudFrontDomain": format!("d{}.cloudfront.net", uuid_prefix(13)),
    }))
}

fn delete_user_pool_domain(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    let domain = p["Domain"].as_str().unwrap_or_default().to_string();
    if let Err(e) = need_pool(s, &pool_id) {
        return e;
    }
    if let Some(i) = s.domains.iter().position(|(d, _)| *d == domain) {
        s.domains.remove(i);
        if let Some(pl) = pool_mut(s, &pool_id) {
            pl.pool.as_object_mut().unwrap().remove("Domain");
        }
    }
    ok_json(&json!({}))
}

// ---------------------------------------------------------------------------
// moto-fallback: identity providers / resource servers / terms
// ---------------------------------------------------------------------------

fn idp_record(cfg: &Value) -> Value {
    // moto to_json(extended=True): base + extended config
    let mut rec = json!({
        "ProviderName": cfg["ProviderName"].clone(),
        "ProviderType": cfg["ProviderType"].clone(),
        "CreationDate": cfg["CreationDate"].clone(),
        "LastModifiedDate": cfg["LastModifiedDate"].clone(),
    });
    if let (Some(obj), Some(ext)) = (rec.as_object_mut(), cfg.as_object()) {
        for (k, v) in ext {
            if k != "ProviderName" && k != "ProviderType" {
                obj.insert(k.clone(), v.clone());
            }
        }
    }
    rec
}

fn idp_base(cfg: &Value) -> Value {
    json!({
        "ProviderName": cfg["ProviderName"].clone(),
        "ProviderType": cfg["ProviderType"].clone(),
        "CreationDate": cfg["CreationDate"].clone(),
        "LastModifiedDate": cfg["LastModifiedDate"].clone(),
    })
}

fn create_identity_provider(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    let name = p["ProviderName"].as_str().unwrap_or_default().to_string();
    if let Err(e) = need_pool(s, &pool_id) {
        return e;
    }
    let mut cfg = p.as_object().cloned().unwrap_or_default();
    cfg.remove("UserPoolId");
    cfg.remove("ProviderName");
    cfg.insert("CreationDate".into(), json!(now()));
    cfg.insert("LastModifiedDate".into(), json!(now()));
    if !cfg.contains_key("AttributeMapping") {
        cfg.insert("AttributeMapping".into(), json!({ "username": "sub" }));
    }
    let pl = pool_mut(s, &pool_id).unwrap();
    let mut store_cfg = Map::new();
    store_cfg.insert("ProviderName".into(), json!(name.clone()));
    store_cfg.insert("ProviderType".into(), p["ProviderType"].clone());
    for (k, v) in &cfg {
        if k != "ProviderName" && k != "ProviderType" {
            store_cfg.insert(k.clone(), v.clone());
        }
    }
    pl.idps
        .insert(name.clone(), Value::Object(store_cfg.clone()));
    // record = base(ProviderName, ProviderType, dates) + extended config
    let mut rec = json!({
        "ProviderName": name,
        "ProviderType": p["ProviderType"].clone(),
        "CreationDate": cfg["CreationDate"].clone(),
        "LastModifiedDate": cfg["LastModifiedDate"].clone(),
    });
    if let (Some(obj), Some(ext)) = (
        rec.as_object_mut(),
        Value::Object(store_cfg.clone()).as_object(),
    ) {
        for (k, v) in ext {
            if k != "ProviderName" && k != "ProviderType" {
                obj.insert(k.clone(), v.clone());
            }
        }
    }
    ok_json(&json!({ "IdentityProvider": rec }))
}

fn idp_store_config<'a>(pl: &'a Pool, name: &str) -> Option<&'a Value> {
    pl.idps.get(name)
}

fn describe_identity_provider(s: &State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default();
    let name = p["ProviderName"].as_str().unwrap_or_default();
    let Some(pl) = pool_ref(s, pool_id) else {
        return pool_missing(pool_id);
    };
    match idp_store_config(pl, name) {
        Some(cfg) => ok_json(&json!({ "IdentityProvider": idp_record(cfg) })),
        None => err_status("ResourceNotFoundException", name, 404),
    }
}

fn update_identity_provider(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    let name = p["ProviderName"].as_str().unwrap_or_default().to_string();
    if let Err(e) = need_pool(s, &pool_id) {
        return e;
    }
    let pl = pool_mut(s, &pool_id).unwrap();
    let Some(cfg) = pl.idps.get_mut(&name) else {
        return err_status("ResourceNotFoundException", &name, 404);
    };
    if let (Some(obj), Some(patch)) = (cfg.as_object_mut(), p.as_object()) {
        for (k, v) in patch {
            if k != "UserPoolId" && k != "ProviderName" && k != "ProviderType" {
                obj.insert(k.clone(), v.clone());
            }
        }
    }
    ok_json(&json!({ "IdentityProvider": idp_record(cfg) }))
}

fn delete_identity_provider(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    let name = p["ProviderName"].as_str().unwrap_or_default().to_string();
    if let Err(e) = need_pool(s, &pool_id) {
        return e;
    }
    let pl = pool_mut(s, &pool_id).unwrap();
    if !pl.idps.contains_key(&name) {
        return err_status("ResourceNotFoundException", &name, 404);
    }
    pl.idps.remove(&name);
    ok_json(&json!({}))
}

fn list_identity_providers(s: &State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default();
    let pl = match need_pool_ref(s, pool_id) {
        Ok(pl) => pl,
        Err(e) => return e,
    };
    let providers: Vec<Value> = pl.idps.values().map(idp_base).collect();
    ok_json(&json!({ "Providers": providers }))
}

fn create_resource_server(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    let identifier = p["Identifier"].as_str().unwrap_or_default().to_string();
    if let Err(e) = need_pool(s, &pool_id) {
        return e;
    }
    let pl = pool_mut(s, &pool_id).unwrap();
    if pl.resource_servers.iter().any(|(k, _)| *k == identifier) {
        return err(
            "InvalidParameterException",
            &format!("{identifier} already exists in user pool {pool_id}."),
        );
    }
    let mut record = json!({
        "UserPoolId": pool_id,
        "Identifier": identifier,
        "Name": p["Name"].clone(),
    });
    if p["Scopes"]
        .as_array()
        .map(|a| !a.is_empty())
        .unwrap_or(false)
    {
        record["Scopes"] = p["Scopes"].clone();
    }
    pl.resource_servers
        .push((identifier.clone(), record.clone()));
    ok_json(&json!({ "ResourceServer": record }))
}

fn describe_resource_server(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    if let Err(e) = need_pool(s, &pool_id) {
        return e;
    }
    let identifier = p["Identifier"].as_str().unwrap_or_default().to_string();
    let pl = pool_mut(s, &pool_id).unwrap();
    match pl.resource_servers.iter().find(|(k, _)| *k == identifier) {
        Some((_, v)) => ok_json(&json!({ "ResourceServer": v })),
        None => err(
            "ResourceNotFoundException",
            &format!("Resource server {identifier} does not exist."),
        ),
    }
}

fn list_resource_servers(s: &State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default();
    let pl = match need_pool_ref(s, pool_id) {
        Ok(pl) => pl,
        Err(e) => return e,
    };
    let servers: Vec<Value> = pl.resource_servers.iter().map(|(_, v)| v.clone()).collect();
    ok_json(&json!({ "ResourceServers": servers }))
}

fn create_terms(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    if let Err(e) = need_pool(s, &pool_id) {
        return e;
    }
    let terms_id = uuid::Uuid::new_v4().to_string();
    let record = json!({
        "TermsId": terms_id,
        "TermsName": p["TermsName"].clone(),
        "UserPoolId": pool_id.clone(),
        "ClientId": p["ClientId"].clone(),
        "TermsSource": p["TermsSource"].clone(),
        "TermsVersion": 1,
        "Enforcement": p["Enforcement"].clone(),
        "CreationDate": now(),
        "LastModifiedDate": now(),
        "Status": "ACTIVE",
    });
    pool_mut(s, &pool_id)
        .unwrap()
        .terms
        .push((terms_id, record.clone()));
    ok_json(&json!({ "Terms": record }))
}

fn describe_terms(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    if let Err(e) = need_pool(s, &pool_id) {
        return e;
    }
    let terms_id = p["TermsId"].as_str().unwrap_or_default().to_string();
    let pl = pool_mut(s, &pool_id).unwrap();
    match pl.terms.iter().find(|(k, _)| *k == terms_id) {
        Some((_, v)) => ok_json(&json!({ "Terms": v })),
        None => err(
            "ResourceNotFoundException",
            &format!("Terms {terms_id} does not exist."),
        ),
    }
}

fn update_terms(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    if let Err(e) = need_pool(s, &pool_id) {
        return e;
    }
    let terms_id = p["TermsId"].as_str().unwrap_or_default().to_string();
    let pl = pool_mut(s, &pool_id).unwrap();
    let Some(t) = pl
        .terms
        .iter_mut()
        .find(|(k, _)| *k == terms_id)
        .map(|(_, v)| v)
    else {
        return err(
            "ResourceNotFoundException",
            &format!("Terms {terms_id} does not exist."),
        );
    };
    if let Some(v) = p.get("TermsSource").filter(|v| !v.is_null()) {
        t["TermsSource"] = v.clone();
    }
    if let Some(v) = p.get("Enforcement").filter(|v| !v.is_null()) {
        t["Enforcement"] = v.clone();
    }
    let version = t["TermsVersion"].as_i64().unwrap_or(1) + 1;
    t["TermsVersion"] = json!(version);
    t["LastModifiedDate"] = json!(now());
    let rec = t.clone();
    ok_json(&json!({ "Terms": rec }))
}

fn delete_terms(s: &mut State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default().to_string();
    if let Err(e) = need_pool(s, &pool_id) {
        return e;
    }
    let terms_id = p["TermsId"].as_str().unwrap_or_default().to_string();
    let pl = pool_mut(s, &pool_id).unwrap();
    if pl.terms.iter().all(|(k, _)| *k != terms_id) {
        return err(
            "ResourceNotFoundException",
            &format!("Terms {terms_id} does not exist."),
        );
    }
    pl.terms.retain(|(k, _)| *k != terms_id);
    ok_json(&json!({}))
}

fn list_terms(s: &State, p: &Value) -> Response {
    let pool_id = p["UserPoolId"].as_str().unwrap_or_default();
    let pl = match need_pool_ref(s, pool_id) {
        Ok(pl) => pl,
        Err(e) => return e,
    };
    let items: Vec<Value> = pl.terms.iter().map(|(_, v)| v.clone()).collect();
    ok_json(&json!({ "Terms": items }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::HashMap;

    fn req(op: &str, body: Value) -> Request {
        Request {
            service: "cognito-idp".into(),
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

    fn make_pool(acct: u64) -> String {
        let mut r = req("CreateUserPool", json!({"PoolName": "p"}));
        r.account = acct;
        body(&dispatch(&r).unwrap())["UserPool"]["Id"]
            .as_str()
            .unwrap()
            .to_string()
    }

    #[test]
    fn pool_lifecycle() {
        let acct = scope();
        let pool_id = make_pool(acct);
        let mut r = req("DescribeUserPool", json!({"UserPoolId": &pool_id}));
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(b["UserPool"]["Id"].as_str().unwrap(), pool_id.as_str());
        assert!(b["UserPool"]["Arn"]
            .as_str()
            .unwrap()
            .starts_with("arn:aws:cognito-idp:"));

        r = req("ListUserPools", json!({"MaxResults": 60}));
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert!(b["UserPools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["Id"] == pool_id.clone()));

        r = req("DeleteUserPool", json!({"UserPoolId": &pool_id}));
        r.account = acct;
        assert_eq!(dispatch(&r).unwrap().status, 200);
        r = req("DescribeUserPool", json!({"UserPoolId": &pool_id}));
        r.account = acct;
        let resp = dispatch(&r).unwrap();
        assert_eq!(resp.status, 404);
        assert_eq!(body(&resp)["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn mfa_config_round_trip() {
        let acct = scope();
        let pool_id = make_pool(acct);
        let mut r = req(
            "SetUserPoolMfaConfig",
            json!({
                "UserPoolId": &pool_id,
                "MfaConfiguration": "ON",
                "SoftwareTokenMfaConfiguration": {"Enabled": true},
            }),
        );
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(b["MfaConfiguration"], "ON");
        assert_eq!(b["SoftwareTokenMfaConfiguration"]["Enabled"], true);

        r = req("GetUserPoolMfaConfig", json!({"UserPoolId": &pool_id}));
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(b["MfaConfiguration"], "ON");
    }

    #[test]
    fn user_and_group_flow() {
        let acct = scope();
        let pool_id = make_pool(acct);
        let mut r = req(
            "AdminCreateUser",
            json!({"UserPoolId": &pool_id, "Username": "u1", "TemporaryPassword": "Temp1!"}),
        );
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(b["User"]["UserStatus"], "FORCE_CHANGE_PASSWORD");
        assert_eq!(b["User"]["Enabled"], true);

        r = req(
            "AdminGetUser",
            json!({"UserPoolId": &pool_id, "Username": "u1"}),
        );
        r.account = acct;
        assert_eq!(
            body(&dispatch(&r).unwrap())["UserStatus"],
            "FORCE_CHANGE_PASSWORD"
        );

        r = req(
            "AdminSetUserPassword",
            json!({"UserPoolId": &pool_id, "Username": "u1", "Password": "New1!", "Permanent": true}),
        );
        r.account = acct;
        dispatch(&r).unwrap();

        r = req(
            "AdminGetUser",
            json!({"UserPoolId": &pool_id, "Username": "u1"}),
        );
        r.account = acct;
        assert_eq!(body(&dispatch(&r).unwrap())["UserStatus"], "CONFIRMED");

        r = req(
            "CreateGroup",
            json!({"UserPoolId": &pool_id, "GroupName": "g1"}),
        );
        r.account = acct;
        assert_eq!(body(&dispatch(&r).unwrap())["Group"]["GroupName"], "g1");

        r = req(
            "AdminAddUserToGroup",
            json!({"UserPoolId": &pool_id, "Username": "u1", "GroupName": "g1"}),
        );
        r.account = acct;
        assert_eq!(dispatch(&r).unwrap().status, 200);

        r = req(
            "AdminListGroupsForUser",
            json!({"UserPoolId": &pool_id, "Username": "u1"}),
        );
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(b["Groups"][0]["GroupName"], "g1");

        r = req(
            "ListUsersInGroup",
            json!({"UserPoolId": &pool_id, "GroupName": "g1"}),
        );
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(b["Users"][0]["Username"], "u1");

        // missing user 404
        r = req(
            "AdminGetUser",
            json!({"UserPoolId": &pool_id, "Username": "nope"}),
        );
        r.account = acct;
        assert_eq!(dispatch(&r).unwrap().status, 404);
    }

    #[test]
    fn auth_flow_and_token_roundtrip() {
        let acct = scope();
        let pool_id = make_pool(acct);
        let mut r = req(
            "CreateUserPoolClient",
            json!({"UserPoolId": &pool_id, "ClientName": "c1"}),
        );
        r.account = acct;
        let client_id = body(&dispatch(&r).unwrap())["UserPoolClient"]["ClientId"]
            .as_str()
            .unwrap()
            .to_string();

        r = req(
            "AdminCreateUser",
            json!({"UserPoolId": &pool_id, "Username": "au", "TemporaryPassword": "Temp1!"}),
        );
        r.account = acct;
        dispatch(&r).unwrap();

        r = req(
            "AdminInitiateAuth",
            json!({
                "UserPoolId": &pool_id,
                "ClientId": &client_id,
                "AuthFlow": "ADMIN_USER_PASSWORD_AUTH",
                "AuthParameters": {"USERNAME": "au", "PASSWORD": "Temp1!"},
            }),
        );
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(b["ChallengeName"], "NEW_PASSWORD_REQUIRED");
        let session = b["Session"].as_str().unwrap().to_string();

        r = req(
            "RespondToAuthChallenge",
            json!({
                "ClientId": &client_id,
                "ChallengeName": "NEW_PASSWORD_REQUIRED",
                "Session": &session,
                "ChallengeResponses": {"USERNAME": "au", "NEW_PASSWORD": "New1!"},
            }),
        );
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        let access = b["AuthenticationResult"]["AccessToken"]
            .as_str()
            .unwrap()
            .to_string();
        assert_ne!(access, "");

        // GetUser by decoding the minted token finds the user
        r = req("GetUser", json!({"AccessToken": access}));
        r.account = acct;
        let gu = dispatch(&r).unwrap();
        assert_eq!(body(&gu)["Username"], "au");

        // a bogus token is NotAuthorized
        r = req("GetUser", json!({"AccessToken": "fake-access-token"}));
        r.account = acct;
        assert_eq!(
            body(&dispatch(&r).unwrap())["__type"],
            "NotAuthorizedException"
        );
    }

    #[test]
    fn identity_providers_resource_servers_terms() {
        let acct = scope();
        let pool_id = make_pool(acct);
        let mut r = req(
            "CreateIdentityProvider",
            json!({
                "UserPoolId": &pool_id,
                "ProviderName": "OIDC",
                "ProviderType": "OIDC",
                "ProviderDetails": {"client_id": "old"},
            }),
        );
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(b["IdentityProvider"]["ProviderType"], "OIDC");
        assert_eq!(b["IdentityProvider"]["ProviderDetails"]["client_id"], "old");

        r = req(
            "ListIdentityProviders",
            json!({"UserPoolId": &pool_id, "MaxResults": 10}),
        );
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(b["Providers"][0]["ProviderName"], "OIDC");
        // list returns base json only (no ProviderDetails)
        assert!(b["Providers"][0].get("ProviderDetails").is_none());

        r = req(
            "DescribeIdentityProvider",
            json!({"UserPoolId": &pool_id, "ProviderName": "OIDC"}),
        );
        r.account = acct;
        assert_eq!(
            body(&dispatch(&r).unwrap())["IdentityProvider"]["ProviderDetails"]["client_id"],
            "old"
        );

        r = req(
            "UpdateIdentityProvider",
            json!({
                "UserPoolId": &pool_id,
                "ProviderName": "OIDC",
                "ProviderDetails": {"client_id": "new"},
            }),
        );
        r.account = acct;
        assert_eq!(
            body(&dispatch(&r).unwrap())["IdentityProvider"]["ProviderDetails"]["client_id"],
            "new"
        );

        r = req(
            "DeleteIdentityProvider",
            json!({"UserPoolId": &pool_id, "ProviderName": "OIDC"}),
        );
        r.account = acct;
        assert_eq!(dispatch(&r).unwrap().status, 200);
        r = req(
            "DescribeIdentityProvider",
            json!({"UserPoolId": &pool_id, "ProviderName": "OIDC"}),
        );
        r.account = acct;
        assert_eq!(dispatch(&r).unwrap().status, 404);

        // resource server
        r = req(
            "CreateResourceServer",
            json!({
                "UserPoolId": &pool_id,
                "Identifier": "https://api.example.com",
                "Name": "API",
            }),
        );
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(b["ResourceServer"]["Identifier"], "https://api.example.com");

        r = req(
            "DescribeResourceServer",
            json!({"UserPoolId": &pool_id, "Identifier": "https://api.example.com"}),
        );
        r.account = acct;
        assert_eq!(
            body(&dispatch(&r).unwrap())["ResourceServer"]["Name"],
            "API"
        );

        // terms
        r = req(
            "CreateTerms",
            json!({"UserPoolId": &pool_id, "ClientId": "c", "TermsName": "t", "TermsSource": "LINK", "Enforcement": "NONE"}),
        );
        r.account = acct;
        let terms_id = body(&dispatch(&r).unwrap())["Terms"]["TermsId"]
            .as_str()
            .unwrap()
            .to_string();
        r = req(
            "DescribeTerms",
            json!({"UserPoolId": &pool_id, "TermsId": &terms_id}),
        );
        r.account = acct;
        assert_eq!(body(&dispatch(&r).unwrap())["Terms"]["Status"], "ACTIVE");

        r = req(
            "DeleteTerms",
            json!({"UserPoolId": &pool_id, "TermsId": &terms_id}),
        );
        r.account = acct;
        assert_eq!(dispatch(&r).unwrap().status, 200);
        r = req(
            "DescribeTerms",
            json!({"UserPoolId": &pool_id, "TermsId": &terms_id}),
        );
        r.account = acct;
        let resp = dispatch(&r).unwrap();
        assert_eq!(resp.status, 400);
        assert_eq!(body(&resp)["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn tagging() {
        let acct = scope();
        let pool_id = make_pool(acct);
        let arn = {
            let mut r = req("DescribeUserPool", json!({"UserPoolId": &pool_id}));
            r.account = acct;
            body(&dispatch(&r).unwrap())["UserPool"]["Arn"]
                .as_str()
                .unwrap()
                .to_string()
        };
        let mut r = req(
            "TagResource",
            json!({"ResourceArn": &arn, "Tags": {"env": "t"}}),
        );
        r.account = acct;
        assert_eq!(dispatch(&r).unwrap().status, 200);
        r = req("ListTagsForResource", json!({"ResourceArn": &arn}));
        r.account = acct;
        assert_eq!(body(&dispatch(&r).unwrap())["Tags"]["env"], "t");
        r = req(
            "UntagResource",
            json!({"ResourceArn": &arn, "TagKeys": ["env"]}),
        );
        r.account = acct;
        dispatch(&r).unwrap();
        r = req("ListTagsForResource", json!({"ResourceArn": &arn}));
        r.account = acct;
        assert!(body(&dispatch(&r).unwrap())["Tags"].get("env").is_none());
    }

    #[test]
    fn domains_lifecycle() {
        let acct = scope();
        let pool_id = make_pool(acct);
        let mut r = req(
            "CreateUserPoolDomain",
            json!({"UserPoolId": &pool_id, "Domain": "test-string"}),
        );
        r.account = acct;
        assert_eq!(dispatch(&r).unwrap().status, 200);

        r = req("DescribeUserPoolDomain", json!({"Domain": "test-string"}));
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(b["DomainDescription"]["Domain"], "test-string");
        assert_eq!(b["DomainDescription"]["Status"], "ACTIVE");

        r = req(
            "DeleteUserPoolDomain",
            json!({"UserPoolId": &pool_id, "Domain": "test-string"}),
        );
        r.account = acct;
        assert_eq!(dispatch(&r).unwrap().status, 200);

        r = req("DescribeUserPoolDomain", json!({"Domain": "test-string"}));
        r.account = acct;
        // moto + native both return an EMPTY description (no error) — the same
        // outcome the compatibility suite records against the bridge.
        assert_eq!(
            body(&dispatch(&r).unwrap())["DomainDescription"]
                .as_object()
                .unwrap()
                .len(),
            0
        );

        // duplicate domain creation errors
        let pool2 = make_pool(acct);
        r = req(
            "CreateUserPoolDomain",
            json!({"UserPoolId": &pool2, "Domain": "test-string"}),
        );
        r.account = acct;
        assert_eq!(dispatch(&r).unwrap().status, 200);
        r = req(
            "CreateUserPoolDomain",
            json!({"UserPoolId": &pool2, "Domain": "test-string"}),
        );
        r.account = acct;
        assert_eq!(
            body(&dispatch(&r).unwrap())["__type"],
            "InvalidParameterException"
        );
    }
}
