//! Greengrass backend (REST-JSON, `/greengrass/...` paths).
//!
//! Port of moto's `GreengrassBackend` per-(account, region): seven
//! definition kinds (cores/devices/functions/resources/connectors/loggers/
//! subscriptions) with versions, groups + group versions + deployments,
//! group/account roles, certificate-authority surfaces, tags, connectivity
//! info, thing runtime configurations, bulk deployments and software
//! update jobs.
//!
//! The two ops moto never supported (`$reset` deployments and `$stop` bulk
//! deployments) keep returning NotImplemented — the suites pin that.
//! Gap ops the moto bridge could not route (cert/bulk/thing/software
//! surfaces) answer 200 like the Python ground-truth provider does.

use crate::store;
use crate::{Request, Response};
use serde_json::{json, Value};
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

fn json_resp(status: u16, body: &Value) -> Response {
    Response::new(
        status,
        "application/json",
        serde_json::to_vec(body).unwrap_or_default(),
    )
}

fn ok_json(body: &Value) -> Response {
    json_resp(200, body)
}

fn created(body: &Value) -> Response {
    // Greengrass creations answer 201 (moto's ("status": 201) responses).
    json_resp(201, body)
}

fn gg_error(code: &str, message: &str) -> Response {
    json_resp(400, &json!({ "__type": code, "message": message }))
}

fn not_implemented() -> Response {
    gg_error("NotImplemented", "This operation has not been implemented")
}

/// moto's iso_8601_datetime_with_milliseconds ("...T...msZ").
fn iso_ts() -> String {
    let now = chrono::Utc::now();
    format!("{}Z", now.format("%Y-%m-%dT%H:%M:%S%.3f"))
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Plural {
    Cores,
    Devices,
    Functions,
    Resources,
    Subscriptions,
    Connectors,
    Loggers,
}

impl Plural {
    fn from_path(kind: &str) -> Option<Plural> {
        match kind {
            "cores" => Some(Plural::Cores),
            "devices" => Some(Plural::Devices),
            "functions" => Some(Plural::Functions),
            "resources" => Some(Plural::Resources),
            "subscriptions" => Some(Plural::Subscriptions),
            "connectors" => Some(Plural::Connectors),
            "loggers" => Some(Plural::Loggers),
            _ => None,
        }
    }

    fn collection(self) -> &'static str {
        match self {
            Plural::Cores => "cores",
            Plural::Devices => "devices",
            Plural::Functions => "functions",
            Plural::Resources => "resources",
            Plural::Subscriptions => "subscriptions",
            Plural::Connectors => "connectors",
            Plural::Loggers => "loggers",
        }
    }
}

#[derive(Clone)]
struct Definition {
    id: String,
    name: String,
    arn: String,
    created_at: String,
    latest_version: String,
    latest_version_arn: String,
}

#[derive(Clone)]
struct Version {
    def_id: String,
    arn: String,
    version: String,
    created_at: String,
    /// raw InitialVersion payload echoed verbatim ({"Cores": [...]} etc.)
    definition: Value,
}

struct Group {
    id: String,
    name: String,
    arn: String,
    created_at: String,
    latest_version: String,
    latest_version_arn: String,
}

#[derive(Clone)]
struct GroupVersion {
    group_id: String,
    arn: String,
    version: String,
    created_at: String,
    definition: Value,
}

struct Deployment {
    id: String,
    arn: String,
    group_arn: String,
    deployment_type: String,
    created_at: String,
}

#[derive(Default)]
pub struct GreengrassState {
    definitions: BTreeMap<Plural, BTreeMap<String, Definition>>,
    versions: BTreeMap<Plural, BTreeMap<String, Version>>,
    groups: BTreeMap<String, Group>,
    group_versions: BTreeMap<String, BTreeMap<String, GroupVersion>>,
    deployments: BTreeMap<String, Deployment>,
    deployments_by_group: BTreeMap<String, Vec<String>>,
    group_roles: BTreeMap<String, String>,
    service_role: String,
    /// arn → tag map
    tags: BTreeMap<String, BTreeMap<String, Option<String>>>,
}

fn with_state<T>(account: u64, region: &str, f: impl FnOnce(&mut GreengrassState) -> T) -> T {
    let slot = store::slot::<GreengrassState>("greengrass");
    let mut map = slot.write();
    f(map.entry((account, region.to_string())).or_default())
}

fn uuid() -> String {
    uuid::Uuid::new_v4().to_string()
}

// ---------------------------------------------------------------------------
// Shapes
// ---------------------------------------------------------------------------

fn definition_json(defn: &Definition) -> Value {
    json!({
        "Arn": defn.arn,
        "CreationTimestamp": defn.created_at,
        "Id": defn.id,
        "LastUpdatedTimestamp": defn.created_at,
        "LatestVersion": defn.latest_version,
        "LatestVersionArn": defn.latest_version_arn,
        "Name": defn.name,
    })
}

fn version_json_short(v: &Version) -> Value {
    json!({
        "Arn": v.arn,
        "CreationTimestamp": v.created_at,
        "Id": v.def_id,
        "Version": v.version,
    })
}

fn version_json_detail(v: &Version) -> Value {
    json!({
        "Arn": v.arn,
        "CreationTimestamp": v.created_at,
        "Id": v.def_id,
        "Definition": v.definition,
        "Version": v.version,
    })
}

fn group_json(group: &Group) -> Value {
    json!({
        "Arn": group.arn,
        "CreationTimestamp": group.created_at,
        "Id": group.id,
        "LastUpdatedTimestamp": group.created_at,
        "LatestVersion": group.latest_version,
        "LatestVersionArn": group.latest_version_arn,
        "Name": group.name,
    })
}

fn group_version_json_detail(v: &GroupVersion) -> Value {
    json!({
        "Arn": v.arn,
        "CreationTimestamp": v.created_at,
        "Id": v.group_id,
        "Definition": v.definition,
        "Version": v.version,
    })
}

fn deployment_json(d: &Deployment) -> Value {
    json!({
        "CreatedAt": d.created_at,
        "DeploymentArn": d.arn,
        "DeploymentId": d.id,
        "DeploymentType": d.deployment_type,
        "GroupArn": d.group_arn,
    })
}

fn deployment_status_json(d: &Deployment) -> Value {
    json!({
        "DeploymentStatus": "InProgress",
        "DeploymentType": d.deployment_type,
        "UpdatedAt": iso_ts(),
    })
}

// ---------------------------------------------------------------------------
// Definitions — shared engine over the seven kinds
// ---------------------------------------------------------------------------

fn definition_arn(kind: Plural, def_id: &str, account: u64, region: &str) -> String {
    format!(
        "arn:aws:greengrass:{region}:{account}:greengrass/definition/{}/{}",
        kind.collection(),
        def_id
    )
}

fn version_arn_for(
    kind: Plural,
    def_id: &str,
    version: &str,
    account: u64,
    region: &str,
) -> String {
    format!(
        "arn:aws:greengrass:{region}:{account}:/greengrass/definition/{}/{}/versions/{}",
        kind.collection(),
        def_id,
        version
    )
}

fn list_definitions(state: &mut GreengrassState, kind: Plural) -> Response {
    let empty = BTreeMap::new();
    let definitions = state.definitions.get(&kind).unwrap_or(&empty);
    ok_json(&json!({
        "Definitions": definitions.values().map(definition_json).collect::<Vec<_>>()
    }))
}

fn create_definition(
    state: &mut GreengrassState,
    kind: Plural,
    body: &Value,
    account: u64,
    region: &str,
) -> Response {
    let name = body
        .get("Name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let id = uuid();
    let arn = definition_arn(kind, &id, account, region);
    let mut defn = Definition {
        id: id.clone(),
        name,
        arn: arn.clone(),
        created_at: iso_ts(),
        latest_version: String::new(),
        latest_version_arn: String::new(),
    };
    // InitialVersion (when present) creates version 1 immediately.
    let fresh_version = body
        .get("InitialVersion")
        .filter(|p| p.as_object().is_some_and(|o| !o.is_empty()))
        .map(|payload| {
            let version_id = uuid();
            let arn_v = version_arn_for(kind, &id, &version_id, account, region);
            let record = Version {
                def_id: id.clone(),
                arn: arn_v.clone(),
                version: version_id.clone(),
                created_at: iso_ts(),
                definition: payload.clone(),
            };
            state
                .versions
                .entry(kind)
                .or_default()
                .insert(version_id.clone(), record);
            (version_id, arn_v)
        });
    if let Some((ref vid, ref varn)) = fresh_version {
        defn.latest_version = vid.clone();
        defn.latest_version_arn = varn.clone();
    }
    state
        .definitions
        .entry(kind)
        .or_default()
        .insert(id.clone(), defn.clone());
    let mut view = definition_json(&defn);
    if let Some((vid, varn)) = fresh_version {
        if let Some(obj) = view.as_object_mut() {
            obj.insert("LatestVersion".to_string(), json!(vid));
            obj.insert("LatestVersionArn".to_string(), json!(varn));
        }
    }
    created(&view)
}

fn get_definition(state: &GreengrassState, kind: Plural, def_id: &str) -> Response {
    match state
        .definitions
        .get(&kind)
        .and_then(|defs| defs.get(def_id))
    {
        Some(defn) => ok_json(&definition_json(defn)),
        None => gg_error(
            "IdNotFoundException",
            &format!("That definition does not exist: {def_id}"),
        ),
    }
}

fn update_definition(
    state: &mut GreengrassState,
    kind: Plural,
    def_id: &str,
    body: &Value,
) -> Response {
    let name = body.get("Name").and_then(Value::as_str).unwrap_or_default();
    let Some(defn) = state
        .definitions
        .get_mut(&kind)
        .and_then(|defs| defs.get_mut(def_id))
    else {
        return gg_error(
            "IdNotFoundException",
            &format!("That definition does not exist: {def_id}"),
        );
    };
    defn.name = name.to_string();
    ok_json(&json!({}))
}

fn delete_definition(
    state: &mut GreengrassState,
    kind: Plural,
    def_id: &str,
    account: u64,
    region: &str,
) -> Response {
    let removed = state
        .definitions
        .get_mut(&kind)
        .and_then(|defs| defs.remove(def_id))
        .is_some();
    if !removed {
        return gg_error(
            "IdNotFoundException",
            &format!("That definition does not exist: {def_id}"),
        );
    }
    if let Some(vs) = state.versions.get_mut(&kind) {
        vs.retain(|_, v| v.def_id != def_id);
    }
    let arn = definition_arn(kind, def_id, account, region);
    state.tags.remove(&arn);
    ok_json(&json!({}))
}

fn create_definition_version(
    state: &mut GreengrassState,
    kind: Plural,
    def_id: &str,
    body: &Value,
    account: u64,
    region: &str,
) -> Response {
    let exists = state
        .definitions
        .get(&kind)
        .is_some_and(|defs| defs.contains_key(def_id));
    if !exists {
        return gg_error(
            "IdNotFoundException",
            &format!("That definition does not exist: {def_id}"),
        );
    }
    let mut defn = state.definitions[&kind][def_id].clone();
    let version_id = uuid();
    let arn_v = version_arn_for(kind, def_id, &version_id, account, region);
    let record = Version {
        def_id: def_id.to_string(),
        arn: arn_v.clone(),
        version: version_id.clone(),
        created_at: iso_ts(),
        // moto stores the raw version payload ({"Cores": [...]}, ...).
        definition: body.clone(),
    };
    let view = version_json_short(&record);
    state
        .versions
        .entry(kind)
        .or_default()
        .insert(version_id, record);
    defn.latest_version = view["Version"].as_str().unwrap_or_default().to_string();
    defn.latest_version_arn = view["Arn"].as_str().unwrap_or_default().to_string();
    state
        .definitions
        .entry(kind)
        .or_default()
        .insert(def_id.to_string(), defn);
    created(&view)
}

fn list_definition_versions(state: &GreengrassState, kind: Plural, def_id: &str) -> Response {
    let exists = state
        .definitions
        .get(&kind)
        .is_some_and(|defs| defs.contains_key(def_id));
    if !exists {
        return gg_error(
            "IdNotFoundException",
            &format!("That definition does not exist: {def_id}"),
        );
    }
    let versions: Vec<Value> = state
        .versions
        .get(&kind)
        .map(|vs| {
            vs.values()
                .filter(|v| v.def_id == def_id)
                .map(version_json_short)
                .collect()
        })
        .unwrap_or_default();
    ok_json(&json!({ "Versions": versions }))
}

fn get_definition_version(
    state: &GreengrassState,
    kind: Plural,
    def_id: &str,
    version_id: &str,
) -> Response {
    match state
        .versions
        .get(&kind)
        .and_then(|vs| vs.get(version_id))
        .filter(|v| v.def_id == def_id)
    {
        Some(v) => ok_json(&version_json_detail(v)),
        None => gg_error(
            "IdNotFoundException",
            &format!("That version does not exist: {version_id}"),
        ),
    }
}

fn definition_routes(
    state: &mut GreengrassState,
    method: &str,
    kind: Plural,
    rest: &[&str],
    body: &Value,
    account: u64,
    region: &str,
) -> Response {
    match (method, rest) {
        ("GET", []) => list_definitions(state, kind),
        ("POST", []) => create_definition(state, kind, body, account, region),
        ("GET", [def_id]) => get_definition(state, kind, def_id),
        ("PUT", [def_id]) => update_definition(state, kind, def_id, body),
        ("DELETE", [def_id]) => delete_definition(state, kind, def_id, account, region),
        ("POST", [def_id, "versions"]) => {
            create_definition_version(state, kind, def_id, body, account, region)
        }
        ("GET", [def_id, "versions"]) => list_definition_versions(state, kind, def_id),
        ("GET", [def_id, "versions", version_id]) => {
            get_definition_version(state, kind, def_id, version_id)
        }
        _ => not_implemented(),
    }
}

// ---------------------------------------------------------------------------
// Groups / versions / deployments / roles
// ---------------------------------------------------------------------------

fn group_arn(account: u64, region: &str, group_id: &str) -> String {
    format!("arn:aws:greengrass:{region}:{account}:greengrass/groups/{group_id}")
}

fn group_version_arn(account: u64, region: &str, group_id: &str, version: &str) -> String {
    format!(
        "arn:aws:greengrass:{region}:{account}:/greengrass/groups/{group_id}/versions/{version}"
    )
}

fn list_groups(state: &mut GreengrassState) -> Response {
    ok_json(&json!({ "Groups": state.groups.values().map(group_json).collect::<Vec<_>>() }))
}

/// Insert a version for `group_id`; returns (version id, arn) when created.
fn create_group_version_inner(
    state: &mut GreengrassState,
    group_id: &str,
    payload: Option<&Value>,
    account: u64,
    region: &str,
) -> Option<(String, String)> {
    let payload = payload.filter(|p| p.as_object().is_some_and(|o| !o.is_empty()))?;
    let version_id = uuid();
    let arn_v = group_version_arn(account, region, group_id, &version_id);
    let record = GroupVersion {
        group_id: group_id.to_string(),
        arn: arn_v.clone(),
        version: version_id.clone(),
        created_at: iso_ts(),
        definition: payload.clone(),
    };
    state
        .group_versions
        .entry(group_id.to_string())
        .or_default()
        .insert(version_id.clone(), record);
    Some((version_id, arn_v))
}

fn create_group(state: &mut GreengrassState, body: &Value, account: u64, region: &str) -> Response {
    let name = body
        .get("Name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let id = uuid();
    let arn = group_arn(account, region, &id);
    let mut group = Group {
        id: id.clone(),
        name,
        arn,
        created_at: iso_ts(),
        latest_version: String::new(),
        latest_version_arn: String::new(),
    };
    if let Some((vid, varn)) =
        create_group_version_inner(state, &id, body.get("InitialVersion"), account, region)
    {
        group.latest_version = vid;
        group.latest_version_arn = varn;
    }
    state.groups.insert(id.clone(), group);
    let group = &state.groups[&id];
    created(&group_json(group))
}

fn get_group(state: &mut GreengrassState, group_id: &str) -> Response {
    match state.groups.get(group_id) {
        Some(group) => ok_json(&group_json(group)),
        None => gg_error("IdNotFoundException", "That group id does not exist."),
    }
}

fn delete_group(state: &mut GreengrassState, group_id: &str) -> Response {
    if state.groups.remove(group_id).is_none() {
        return gg_error("IdNotFoundException", "That group does not exist.");
    }
    state.group_versions.remove(group_id);
    ok_json(&json!({}))
}

fn update_group(state: &mut GreengrassState, group_id: &str, body: &Value) -> Response {
    let name = body.get("Name").and_then(Value::as_str).unwrap_or_default();
    let Some(group) = state.groups.get_mut(group_id) else {
        return gg_error("IdNotFoundException", "That group does not exist.");
    };
    group.name = name.to_string();
    ok_json(&json!({}))
}

fn op_group_versions(
    state: &mut GreengrassState,
    method: &str,
    group_id: &str,
    version_id: Option<&str>,
    body: &Value,
    account: u64,
    region: &str,
) -> Response {
    if !state.groups.contains_key(group_id) {
        return gg_error("IdNotFoundException", "That group does not exist.");
    }
    match (method, version_id) {
        ("POST", None) => {
            match create_group_version_inner(state, group_id, Some(body), account, region) {
                Some((vid, varn)) => {
                    if let Some(group) = state.groups.get_mut(group_id) {
                        group.latest_version = vid.clone();
                        group.latest_version_arn = varn.clone();
                    }
                    created(&json!({
                        "Arn": varn,
                        "CreationTimestamp": iso_ts(),
                        "Id": group_id,
                        "Version": vid,
                    }))
                }
                None => created(&json!({})),
            }
        }
        ("GET", None) => {
            let versions: Vec<Value> = state
                .group_versions
                .get(group_id)
                .map(|vs| {
                    vs.values()
                        .map(|v| {
                            json!({
                                "Arn": v.arn,
                                "CreationTimestamp": v.created_at,
                                "Id": v.group_id,
                                "Version": v.version,
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();
            ok_json(&json!({ "Versions": versions }))
        }
        ("GET", Some(vid)) => match state
            .group_versions
            .get(group_id)
            .and_then(|vs| vs.get(vid))
        {
            Some(v) => ok_json(&group_version_json_detail(v)),
            None => gg_error(
                "VersionNotFoundException",
                &format!("Version {vid} of Group Definition {group_id} does not exist."),
            ),
        },
        _ => not_implemented(),
    }
}

// --- deployments -------------------------------------------------------------

fn deployment_arn(account: u64, region: &str, group_id: &str, id: &str) -> String {
    format!("arn:aws:greengrass:{region}:{account}:/greengrass/groups/{group_id}/deployments/{id}")
}

fn op_group_deployments(
    state: &mut GreengrassState,
    method: &str,
    group_id: &str,
    deployment_id: Option<&str>,
    body: &Value,
    account: u64,
    region: &str,
) -> Response {
    if !state.groups.contains_key(group_id) {
        return gg_error("IdNotFoundException", "That group does not exist.");
    }
    match (method, deployment_id) {
        ("POST", None) => {
            let deployment_type = body
                .get("DeploymentType")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            const VALID_TYPES: [&str; 4] = [
                "NewDeployment",
                "Redeployment",
                "ResetDeployment",
                "ForceResetDeployment",
            ];
            if !VALID_TYPES.contains(&deployment_type.as_str()) {
                return gg_error(
                    "InvalidInputException",
                    &format!(
                        "That deployment type is not valid.  Please specify one of the following types: {{{}}}.",
                        VALID_TYPES.join(",")
                    ),
                );
            }
            let id = uuid();
            let arn = deployment_arn(account, region, group_id, &id);
            let group_arn_value = group_arn(account, region, group_id);
            let deployment = Deployment {
                id: id.clone(),
                arn: arn.clone(),
                group_arn: group_arn_value.clone(),
                deployment_type: deployment_type.clone(),
                created_at: iso_ts(),
            };
            state.deployments.insert(deployment.id.clone(), deployment);
            state
                .deployments_by_group
                .entry(group_id.to_string())
                .or_default()
                .push(id.clone());
            ok_json(&json!({
                "DeploymentArn": arn,
                "DeploymentId": id,
                "DeploymentType": deployment_type,
                "GroupArn": group_arn_value,
            }))
        }
        ("GET", None) => {
            let empty: Vec<String> = Vec::new();
            let deployments: Vec<Value> = state
                .deployments_by_group
                .get(group_id)
                .unwrap_or(&empty)
                .iter()
                .filter_map(|id| state.deployments.get(id))
                .map(deployment_json)
                .collect();
            ok_json(&json!({ "Deployments": deployments }))
        }
        ("GET", Some(deployment_id)) => match state.deployments.get(deployment_id) {
            Some(deployment) => ok_json(&deployment_status_json(deployment)),
            None => gg_error("IdNotFoundException", "That deployment does not exist."),
        },
        _ => not_implemented(),
    }
}

// --- roles -------------------------------------------------------------------

fn op_group_role(
    state: &mut GreengrassState,
    method: &str,
    group_id: &str,
    body: &Value,
) -> Response {
    match method {
        "PUT" => {
            let role_arn = body
                .get("RoleArn")
                .and_then(Value::as_str)
                .unwrap_or_default();
            state
                .group_roles
                .insert(group_id.to_string(), role_arn.to_string());
            ok_json(&json!({ "AssociatedAt": iso_ts() }))
        }
        "GET" => match state.group_roles.get(group_id).cloned() {
            Some(role_arn) => ok_json(&json!({ "AssociatedAt": iso_ts(), "RoleArn": role_arn })),
            // moto's missing-role row answers literal code "404" with the
            // IAM-attachment ask (moto/greengrass/models.py
            // get_associated_role) — live-verified.
            None => gg_error(
                "404",
                "You need to attach an IAM role to this deployment group.",
            ),
        },
        "DELETE" => {
            state.group_roles.remove(group_id);
            ok_json(&json!({ "DisassociatedAt": iso_ts() }))
        }
        _ => not_implemented(),
    }
}

fn op_service_role(state: &mut GreengrassState, method: &str, body: &Value) -> Response {
    match method {
        "PUT" => {
            state.service_role = body
                .get("RoleArn")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            ok_json(&json!({}))
        }
        "GET" => {
            // The gap suite only requires the key; reuse the stored value.
            ok_json(&json!({ "RoleArn": state.service_role.clone() }))
        }
        "DELETE" => {
            state.service_role.clear();
            ok_json(&json!({}))
        }
        _ => not_implemented(),
    }
}

// --- tags --------------------------------------------------------------------

/// Percent-decode a path or query component.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                if let (Some(h), Some(l)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                    out.push(h * 16 + l);
                    i += 3;
                    continue;
                }
                out.push(b'%');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn gg_arn_from_tags_path(path: &str) -> String {
    match path.rfind("tags/") {
        Some(idx) => percent_decode(&path[idx + 5..]),
        None => String::new(),
    }
}

fn op_gg_tags(state: &mut GreengrassState, method: &str, path: &str, body: &Value) -> Response {
    let arn = gg_arn_from_tags_path(path);
    match method {
        "GET" => {
            let obj: serde_json::Map<String, Value> = state
                .tags
                .get(&arn)
                .map(|m| {
                    m.iter()
                        .map(|(k, v)| {
                            (
                                k.clone(),
                                v.clone().map(Value::String).unwrap_or(Value::Null),
                            )
                        })
                        .collect()
                })
                .unwrap_or_default();
            ok_json(&json!({ "tags": obj }))
        }
        "POST" => {
            if let Some(Value::Object(map)) = body.get("tags") {
                let entry = state.tags.entry(arn).or_default();
                for (k, v) in map {
                    entry.insert(k.clone(), v.as_str().map(String::from));
                }
            }
            ok_json(&json!({}))
        }
        "DELETE" => {
            if let Some(keys) = body.get("TagKeys").and_then(Value::as_array) {
                if let Some(entry) = state.tags.get_mut(&arn) {
                    for key in keys.iter().filter_map(Value::as_str) {
                        entry.remove(key);
                    }
                }
            }
            ok_json(&json!({}))
        }
        _ => not_implemented(),
    }
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Response {
    let body: Value = if req.body.is_empty() {
        json!({})
    } else {
        serde_json::from_slice(&req.body).unwrap_or_else(|_| json!({}))
    };
    if req.path.contains("/tags/") {
        return with_state(req.account, &req.region, |state| {
            op_gg_tags(state, req.method.as_str(), &req.path, &body)
        });
    }
    let path = req.path.trim_matches('/');
    let segments: Vec<&str> = if path.is_empty() {
        Vec::new()
    } else {
        path.split('/').collect()
    };
    with_state(req.account, &req.region, |state| {
        route(
            state,
            req.method.as_str(),
            &segments,
            &body,
            req.account,
            &req.region,
        )
    })
}

fn route(
    state: &mut GreengrassState,
    method: &str,
    segments: &[&str],
    body: &Value,
    account: u64,
    region: &str,
) -> Response {
    match segments {
        ["greengrass", "definition", kind] => match Plural::from_path(kind) {
            Some(kind) => definition_routes(state, method, kind, &[], body, account, region),
            None => not_implemented(),
        },
        ["greengrass", "definition", kind, def_id] => match Plural::from_path(kind) {
            Some(kind) => definition_routes(state, method, kind, &[def_id], body, account, region),
            None => not_implemented(),
        },
        ["greengrass", "definition", kind, def_id, "versions"] => match Plural::from_path(kind) {
            Some(kind) => definition_routes(
                state,
                method,
                kind,
                &[def_id, "versions"],
                body,
                account,
                region,
            ),
            None => not_implemented(),
        },
        ["greengrass", "definition", kind, def_id, "versions", version_id] => {
            match Plural::from_path(kind) {
                Some(kind) => definition_routes(
                    state,
                    method,
                    kind,
                    &[def_id, "versions", version_id],
                    body,
                    account,
                    region,
                ),
                None => not_implemented(),
            }
        }
        ["greengrass", "groups"] => match method {
            "POST" => create_group(state, body, account, region),
            "GET" => list_groups(state),
            _ => not_implemented(),
        },
        ["greengrass", "groups", group_id] => match method {
            "GET" => get_group(state, group_id),
            "PUT" => update_group(state, group_id, body),
            "DELETE" => delete_group(state, group_id),
            _ => not_implemented(),
        },
        ["greengrass", "groups", group_id, "versions"] => {
            op_group_versions(state, method, group_id, None, body, account, region)
        }
        ["greengrass", "groups", group_id, "versions", version_id] => op_group_versions(
            state,
            method,
            group_id,
            Some(version_id),
            body,
            account,
            region,
        ),
        ["greengrass", "groups", group_id, "role"] => op_group_role(state, method, group_id, body),
        ["greengrass", "groups", group_id, "deployments"] => {
            op_group_deployments(state, method, group_id, None, body, account, region)
        }
        ["greengrass", "groups", group_id, "deployments", deployment_id, "status"] => {
            op_group_deployments(
                state,
                method,
                group_id,
                Some(deployment_id),
                body,
                account,
                region,
            )
        }
        ["greengrass", "groups", _group_id, "deployments", "$reset"] => not_implemented(),
        ["greengrass", "groups", group_id, "certificateauthorities"] => {
            if method == "POST" {
                return ok_json(&json!({
                    "GroupCertificateAuthorityArn": format!(
                        "arn:aws:greengrass:{region}:{account}:greengrass/groups/{group_id}/certificateauthorities/{}",
                        uuid()
                    )
                }));
            }
            ok_json(&json!({ "GroupCertificateAuthorities": [] }))
        }
        ["greengrass", "groups", group_id, "certificateauthorities", ca_id] => ok_json(&json!({
            "GroupCertificateAuthorityArn": format!(
                "arn:aws:greengrass:{region}:{account}:greengrass/groups/{group_id}/certificateauthorities/{ca_id}"
            ),
            "GroupCertificateAuthorityId": ca_id,
            "PemEncodedCertificate": "",
        })),
        ["greengrass", "groups", group_id, "certificateauthorities", "configuration", "expiry"] => {
            let _ = (group_id, method);
            ok_json(&json!({ "CertificateExpiryCheckConfiguration": {} }))
        }
        ["greengrass", "servicerole"] => op_service_role(state, method, body),
        ["greengrass", "things", thing_name, "connectivityInfo"] => match method {
            "GET" => ok_json(&json!({ "ConnectivityInfo": [], "Message": "200 OK" })),
            "PUT" => {
                let _ = thing_name;
                ok_json(&json!({}))
            }
            _ => not_implemented(),
        },
        ["greengrass", "things", thing_name, "runtimeconfig"] => {
            let _ = thing_name;
            match method {
                "GET" => ok_json(&json!({ "RuntimeConfiguration": {} })),
                "PUT" => ok_json(&json!({})),
                _ => not_implemented(),
            }
        }
        ["greengrass", "bulk", "deployments"] => match method {
            "POST" => {
                let id = uuid();
                ok_json(&json!({
                    "BulkDeploymentArn": format!(
                        "arn:aws:greengrass:{region}:{account}:greengrass/bulk/deployments/{id}"
                    ),
                    "BulkDeploymentId": id,
                }))
            }
            "GET" => ok_json(&json!({ "BulkDeployments": [] })),
            _ => not_implemented(),
        },
        ["greengrass", "bulk", "deployments", bulk_id, "status"] => {
            let _ = bulk_id;
            ok_json(&json!({}))
        }
        ["greengrass", "bulk", "deployments", bulk_id, "detailed-reports"] => {
            let _ = bulk_id;
            ok_json(&json!({}))
        }
        ["greengrass", "bulk", "deployments", _bulk_id, "$stop"] => not_implemented(),
        ["greengrass", "updates"] => match method {
            "POST" => {
                let job_id = uuid();
                ok_json(&json!({
                    "IotJobArn": format!("arn:aws:iot:{region}:{account}:job/{job_id}"),
                    "IotJobId": job_id,
                }))
            }
            _ => not_implemented(),
        },
        _ => not_implemented(),
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const ACCOUNT: u64 = 123456789012;
    const REGION: &str = "us-east-1";

    fn run(method: &str, path: &str, body: Value) -> Response {
        dispatch(&Request {
            service: "greengrass".to_string(),
            operation: String::new(),
            method: method.to_string(),
            path: path.to_string(),
            query_string: String::new(),
            headers: std::collections::HashMap::new(),
            body: serde_json::to_vec(&body).unwrap_or_default().into(),
            account: ACCOUNT,
            region: REGION.to_string(),
        })
    }

    fn body_of(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap_or(Value::Null)
    }

    #[test]
    fn core_definition_lifecycle() {
        let created = body_of(&run(
            "POST",
            "/greengrass/definition/cores",
            json!({
                "Name": "cd-1",
                "InitialVersion": {"Cores": [{"Id": "core1"}]},
            }),
        ));
        assert_eq!(created["Name"], "cd-1");
        assert!(created["Id"].is_string());
        assert!(created["Arn"]
            .as_str()
            .unwrap()
            .contains("greengrass/definition/cores"));
        let def_id = created["Id"].as_str().unwrap().to_string();
        let version_id = created["LatestVersion"].as_str().unwrap().to_string();

        let got = body_of(&run(
            "GET",
            &format!("/greengrass/definition/cores/{def_id}"),
            json!({}),
        ));
        assert_eq!(got["Id"], def_id.as_str());

        let listed = body_of(&run("GET", "/greengrass/definition/cores", json!({})));
        assert!(listed["Definitions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|d| d["Id"] == def_id.as_str()));

        let version = body_of(&run(
            "GET",
            &format!("/greengrass/definition/cores/{def_id}/versions/{version_id}"),
            json!({}),
        ));
        assert!(version["Definition"]["Cores"].is_array());

        run(
            "PUT",
            &format!("/greengrass/definition/cores/{def_id}"),
            json!({"Name": "renamed"}),
        );
        let got = body_of(&run(
            "GET",
            &format!("/greengrass/definition/cores/{def_id}"),
            json!({}),
        ));
        assert_eq!(got["Name"], "renamed");

        run(
            "DELETE",
            &format!("/greengrass/definition/cores/{def_id}"),
            json!({}),
        );
        let gone = run(
            "GET",
            &format!("/greengrass/definition/cores/{def_id}"),
            json!({}),
        );
        assert!(String::from_utf8_lossy(&gone.body).contains("IdNotFoundException"));
    }

    #[test]
    fn group_and_version_lifecycle() {
        let group = body_of(&run(
            "POST",
            "/greengrass/groups",
            json!({"Name": "group-1"}),
        ));
        assert_eq!(group["Name"], "group-1");
        let group_id = group["Id"].as_str().unwrap().to_string();

        run(
            "PUT",
            &format!("/greengrass/groups/{group_id}"),
            json!({"Name": "renamed-group"}),
        );
        let got = body_of(&run(
            "GET",
            &format!("/greengrass/groups/{group_id}"),
            json!({}),
        ));
        assert_eq!(got["Name"], "renamed-group");

        let version = body_of(&run(
            "POST",
            &format!("/greengrass/groups/{group_id}/versions"),
            json!({"CoreDefinitionVersionArn": "arn:aws:greengrass:us-east-1:123456789012:/greengrass/definition/cores/x/versions/y"}),
        ));
        let vid = version["Version"].as_str().unwrap().to_string();
        let detail = body_of(&run(
            "GET",
            &format!("/greengrass/groups/{group_id}/versions/{vid}"),
            json!({}),
        ));
        assert!(detail["Definition"]["CoreDefinitionVersionArn"].is_string());

        run(
            "DELETE",
            &format!("/greengrass/groups/{group_id}"),
            json!({}),
        );
        let gone = run("GET", &format!("/greengrass/groups/{group_id}"), json!({}));
        assert!(String::from_utf8_lossy(&gone.body).contains("IdNotFoundException"));
    }

    #[test]
    fn deployments_lifecycle() {
        let group = body_of(&run(
            "POST",
            "/greengrass/groups",
            json!({"Name": "dep-group"}),
        ));
        let group_id = group["Id"].as_str().unwrap().to_string();
        let resp = run(
            "POST",
            &format!("/greengrass/groups/{group_id}/deployments"),
            json!({"DeploymentType": "NewDeployment", "GroupVersionId": "v1"}),
        );
        let deployment_id = body_of(&resp)["DeploymentId"].as_str().unwrap().to_string();
        let status = body_of(&run(
            "GET",
            &format!("/greengrass/groups/{group_id}/deployments/{deployment_id}/status"),
            json!({}),
        ));
        assert_eq!(status["DeploymentStatus"], "InProgress");
        assert_eq!(status["DeploymentType"], "NewDeployment");
    }

    #[test]
    fn reset_deployments_stays_not_implemented() {
        let group = body_of(&run(
            "POST",
            "/greengrass/groups",
            json!({"Name": "reset-group"}),
        ));
        let group_id = group["Id"].as_str().unwrap().to_string();
        let resp = run(
            "POST",
            &format!("/greengrass/groups/{group_id}/deployments/$reset"),
            json!({}),
        );
        assert!(String::from_utf8_lossy(&resp.body).contains("NotImplemented"));
    }

    #[test]
    fn tags_roundtrip() {
        let arn = "arn:aws:greengrass:us-east-1:123456789012:groups/fake";
        run(
            "POST",
            &format!("/tags/{arn}"),
            json!({"tags": {"env": "test"}}),
        );
        let listed = body_of(&run("GET", &format!("/tags/{arn}"), json!({})));
        assert_eq!(listed["tags"]["env"], "test");
        run(
            "DELETE",
            &format!("/tags/{arn}"),
            json!({"TagKeys": ["env"]}),
        );
        let listed = body_of(&run("GET", &format!("/tags/{arn}"), json!({})));
        assert!(listed["tags"].as_object().unwrap().is_empty());
    }
}
