//! LakeFormation backend (moto-parity port).
//!
//! All operations are `POST /OpName`; the trailing path segment is the AWS
//! operation name. moto's stub ops (UpdateResource, table-objects, opt-ins,
//! storage optimizers…) are implemented as simple stub returns since real
//! moto answers 501 there but the compat suite expects behavior.

use crate::proto::{json_error, now_unix, parse_json_body, unix_in, url_decode};
use crate::store::{slot, ScopeKey, Slot};
use crate::{Request, Response};
use serde_json::{json, Value};
use std::collections::BTreeMap;

/// Per-(account, region) LakeFormation state.
#[derive(Default)]
struct State {
    resources: BTreeMap<String, Value>,  // arn -> {ResourceArn, RoleArn}
    settings: BTreeMap<String, Value>,   // catalog id -> DataLakeSettings
    grants: Vec<Value>,                  // {principal, resource, permissions, with_grant}
    lf_tags: Value,                      // {TagKey: [values]}
    database_tags: BTreeMap<String, Vec<Value>>,  // (catalog, db) key string
    table_tags: BTreeMap<String, Vec<Value>>,     // (catalog, db, table) key string
    transactions: BTreeMap<String, Value>,
    data_cells_filters: BTreeMap<String, Value>,
    lf_tag_expressions: BTreeMap<String, Value>,
    identity_center_config: Option<Value>,
}

fn entity_not_found() -> Response {
    json_error(400, "EntityNotFoundException", "Entity not found")
}

fn already_exists(message: &str) -> Response {
    json_error(400, "AlreadyExistsException", message)
}

fn ok(body: Value) -> Response {
    Response::json_status(200, &body)
}

fn not_implemented(svc: &Svc) -> Response {
    let _ = svc;
    json_error(
        501,
        "NotImplemented",
        "The action has not been implemented",
    )
}

struct Svc {
    state: Slot<State>,
    key: ScopeKey,
    account: String,
    region: String,
    params: Value,
}

impl Svc {
    /// Lock the per-(account, region) state and run `f` against it.
    fn with<R>(&self, f: impl FnOnce(&mut State) -> R) -> R {
        let mut guard = self.state.write();
        f(guard.entry(self.key.clone()).or_default())
    }

    /// Body member (or Value::Null when absent).
    fn body(&self, field: &str) -> Value {
        self.params.get(field).cloned().unwrap_or(Value::Null)
    }

    /// CatalogId body member defaulting to the caller's account id.
    fn catalog(&self) -> String {
        self.body("CatalogId").as_str().unwrap_or(&self.account).to_string()
    }
}

/// (catalog, db[, table]) composite key helper.
fn composite(parts: &[&str]) -> String {
    parts.join("\u{1}")
}

/// De-duplicated union of two string lists (moto merges with a set).
fn union(a: &[String], b: &[String]) -> Vec<String> {
    let mut out: Vec<String> = a.to_vec();
    for v in b {
        if !out.iter().any(|x| x == v) {
            out.push(v.clone());
        }
    }
    out
}

/// Dispatch a routed LakeFormation request. Always answers (501 fallback).
pub fn dispatch(req: &Request) -> Response {
    let state: Slot<State> = slot("lakeformation");
    let key: ScopeKey = (req.account, req.region.clone());
    let svc = Svc {
        state,
        key,
        account: req.account.to_string(),
        region: req.region.clone(),
        params: parse_json_body(&req.body),
    };
    let path_op = url_decode(&req.path)
        .trim_start_matches('/')
        .to_string();

    match path_op.as_str() {
        // --- Resources ---
        "RegisterResource" => register_resource(&svc),
        "DeregisterResource" => deregister_resource(&svc),
        "DescribeResource" => describe_resource(&svc),
        "ListResources" => list_resources(&svc),
        "UpdateResource" => update_resource_stub(&svc),
        // --- Data lake settings ---
        "GetDataLakeSettings" => get_data_lake_settings(&svc),
        "PutDataLakeSettings" => put_data_lake_settings(&svc),
        "GetDataLakePrincipal" => get_data_lake_principal(&svc),
        // --- Permissions ---
        "GrantPermissions" => grant_permissions(&svc),
        "RevokePermissions" => revoke_permissions(&svc),
        "ListPermissions" => list_permissions(&svc),
        "BatchGrantPermissions" => batch_grant_permissions(&svc, true),
        "BatchRevokePermissions" => batch_grant_permissions(&svc, false),
        "GetEffectivePermissionsForPath" => ok(json!({"Permissions": []})),
        // --- LF tags ---
        "CreateLFTag" => create_lf_tag(&svc),
        "GetLFTag" => get_lf_tag(&svc),
        "UpdateLFTag" => update_lf_tag(&svc),
        "DeleteLFTag" => delete_lf_tag(&svc),
        "ListLFTags" => list_lf_tags(&svc),
        "AddLFTagsToResource" => add_lf_tags_to_resource(&svc),
        "RemoveLFTagsFromResource" => remove_lf_tags_from_resource(&svc),
        "GetResourceLFTags" => get_resource_lf_tags(&svc),
        "SearchDatabasesByLFTags" => search_databases_by_lf_tags(&svc),
        "SearchTablesByLFTags" => search_tables_by_lf_tags(&svc),
        // --- LF tag expressions ---
        "CreateLFTagExpression" => create_lf_tag_expression(&svc),
        "GetLFTagExpression" => get_lf_tag_expression(&svc),
        "UpdateLFTagExpression" => update_lf_tag_expression(&svc),
        "DeleteLFTagExpression" => delete_lf_tag_expression(&svc),
        "ListLFTagExpressions" => list_lf_tag_expressions(&svc),
        // --- Transactions ---
        "StartTransaction" => start_transaction(&svc),
        "DescribeTransaction" => describe_transaction(&svc),
        "CommitTransaction" => commit_transaction(&svc),
        "CancelTransaction" => cancel_transaction(&svc),
        "ExtendTransaction" => extend_transaction(&svc),
        "ListTransactions" => list_transactions(&svc),
        // --- Data cells filters ---
        "CreateDataCellsFilter" => create_data_cells_filter(&svc),
        "GetDataCellsFilter" => get_data_cells_filter(&svc),
        "DeleteDataCellsFilter" => delete_data_cells_filter(&svc),
        "UpdateDataCellsFilter" => update_data_cells_filter(&svc),
        "ListDataCellsFilter" => list_data_cells_filter(&svc),
        // --- Credentials & query planning ---
        "GetTemporaryGlueTableCredentials" => temporary_credentials(&svc, true),
        "GetTemporaryGluePartitionCredentials" => temporary_credentials(&svc, false),
        "GetTemporaryDataLocationCredentials" => temporary_data_location_credentials(&svc),
        "StartQueryPlanning" => start_query_planning(&svc),
        "GetQueryState" => get_query_state(&svc),
        "GetQueryStatistics" => get_query_statistics(&svc),
        "GetWorkUnits" => get_work_units(&svc),
        "GetWorkUnitResults" => get_work_unit_results(&svc),
        // --- Identity-center configuration ---
        "CreateLakeFormationIdentityCenterConfiguration" => {
            create_identity_center_config(&svc)
        }
        "DescribeLakeFormationIdentityCenterConfiguration" => {
            describe_identity_center_config(&svc)
        }
        "UpdateLakeFormationIdentityCenterConfiguration" => update_identity_center_config(&svc),
        "DeleteLakeFormationIdentityCenterConfiguration" => delete_identity_center_config(&svc),
        // --- Opt-ins & table objects (stub parity for the compat suites) ---
        "ListLakeFormationOptIns" => ok(json!({"LakeFormationOptInsInfoList": []})),
        "CreateLakeFormationOptIn" => ok(json!({})),
        "DeleteLakeFormationOptIn" => ok(json!({})),
        "ListTableStorageOptimizers" => ok(json!({"StorageOptimizerList": []})),
        "UpdateTableStorageOptimizer" => update_table_storage_optimizer(&svc),
        "GetTableObjects" => ok(json!({"Objects": []})),
        "UpdateTableObjects" => ok(json!({})),
        "DeleteObjectsOnCancel" => ok(json!({})),
        "AssumeDecoratedRoleWithSAML" => not_implemented(&svc),
        _ => not_implemented(&svc),
    }
}

// --- Resources ---

fn register_resource(svc: &Svc) -> Response {
    let arn = svc.body("ResourceArn").as_str().unwrap_or("").to_string();
    let role_arn = svc.body("RoleArn").as_str().unwrap_or("").to_string();
    let exists = svc.with(|st| st.resources.contains_key(&arn));
    if exists {
        return already_exists("Resource is already registered");
    }
    svc.with(|st| {
        st.resources
            .insert(arn.clone(), json!({"ResourceArn": arn, "RoleArn": role_arn}));
    });
    ok(json!({}))
}

fn deregister_resource(svc: &Svc) -> Response {
    let arn = svc.body("ResourceArn").as_str().unwrap_or("").to_string();
    let removed = svc.with(|st| st.resources.remove(&arn).is_some());
    if removed {
        ok(json!({}))
    } else {
        entity_not_found()
    }
}

fn describe_resource(svc: &Svc) -> Response {
    let arn = svc.body("ResourceArn").as_str().unwrap_or("").to_string();
    let stored = svc.with(|st| st.resources.get(&arn).cloned());
    match stored {
        Some(dict) => ok(json!({"ResourceInfo": dict})),
        None => entity_not_found(),
    }
}

fn list_resources(svc: &Svc) -> Response {
    let list = svc.with(|st| st.resources.values().cloned().collect::<Vec<Value>>());
    ok(json!({"ResourceInfoList": list}))
}

fn update_resource_stub(_svc: &Svc) -> Response {
    ok(json!({}))
}

// --- Data lake settings ---

fn default_settings() -> Value {
    json!({
        "DataLakeAdmins": [],
        "CreateDatabaseDefaultPermissions": [
            {"Principal": {"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"}, "Permissions": ["ALL"]}
        ],
        "CreateTableDefaultPermissions": [
            {"Principal": {"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"}, "Permissions": ["ALL"]}
        ],
        "TrustedResourceOwners": [],
        "AllowExternalDataFiltering": false,
        "ExternalDataFilteringAllowList": [],
    })
}

fn get_data_lake_settings(svc: &Svc) -> Response {
    let catalog = svc.catalog();
    let settings = svc.with(|st| {
        st.settings
            .entry(catalog)
            .or_insert_with(default_settings)
            .clone()
    });
    ok(json!({"DataLakeSettings": settings}))
}

fn put_data_lake_settings(svc: &Svc) -> Response {
    let catalog = svc.catalog();
    let settings = svc.body("DataLakeSettings");
    svc.with(|st| st.settings.insert(catalog, settings));
    ok(json!({}))
}

fn get_data_lake_principal(svc: &Svc) -> Response {
    ok(json!({"Identity": format!("arn:aws:iam::{}:root", svc.account)}))
}

// --- Permissions ---

fn grant_dict(principal: &Value, resource: &Value, permissions: Value, with_grant: Value) -> Value {
    json!({
        "principal": principal,
        "resource": resource,
        "permissions": permissions,
        "with_grant": with_grant,
    })
}

fn grant_permissions(svc: &Svc) -> Response {
    let principal = svc.body("Principal");
    let resource = svc.body("Resource");
    let permissions = svc.body("Permissions");
    let with_grant = svc.body("PermissionsWithGrantOption");
    svc.with(|st| {
        let existing = st.grants.iter_mut().find(|g| {
            g.get("principal") == Some(&principal) && g.get("resource") == Some(&resource)
        });
        match existing {
            Some(g) => {
                let current = g.get("permissions").cloned().unwrap_or(json!([]));
                let cur_list: Vec<String> = str_vec(&current);
                let new_list: Vec<String> = str_vec_list(&permissions.clone());
                g["permissions"] = json_list(union_p(&cur_list, &new_list));
            }
            None => st.grants.push(grant_dict(
                &principal,
                &resource,
                json!(permissions),
                json!(with_grant),
            )),
        }
    });
    ok(json!({}))
}

fn revoke_permissions(svc: &Svc) -> Response {
    let principal = svc.body("Principal");
    let resource = svc.body("Resource");
    let permissions = svc
        .body("Permissions")
        .as_array()
        .cloned()
        .unwrap_or_default();
    svc.with(|st| {
        let mut updated: Vec<Value> = Vec::new();
        for g in &st.grants {
            if g.get("principal") == Some(&principal) && g.get("resource") == Some(&resource) {
                let current = g
                    .get("permissions")
                    .cloned()
                    .unwrap_or(json!([]));
                let cur_list: Vec<String> = current
                    .as_array()
                    .map(|a| a.iter().map(|e| e.as_str().unwrap_or("").to_string()).collect())
                    .unwrap_or_default();
                let remaining: Vec<String> = cur_list
                    .into_iter()
                    .filter(|p| !permissions.iter().any(|q| q.as_str() == Some(p.as_str())))
                    .collect();
                let mut new_g = g.clone();
                new_g["permissions"] = json_list(remaining);
                updated.push(new_g);
            } else {
                updated.push(g.clone());
            }
        }
        // drop fully-revoked grants (moto deletes entries whose permission
        // lists became empty)
        st.grants = updated
            .into_iter()
            .filter(|g| {
                let perms = g
                    .get("permissions")
                    .cloned()
                    .unwrap_or(json!([]));
                !perms.as_array().map(|a| a.is_empty()).unwrap_or(false)
                    || g.get("with_grant")
                        .and_then(Value::as_array)
                        .map(|a| !a.is_empty())
                        .unwrap_or(false)
            })
            .collect();
    });
    ok(json!({}))
}

fn list_permissions(svc: &Svc) -> Response {
    let items = svc.with(|st| {
        st.grants
            .iter()
            .filter(|_| true)
            .map(|g| {
                json!({
                    "Permissions": g.get("permissions").cloned().unwrap_or(json!([])),
                    "PermissionsWithGrantOption": g.get("with_grant").cloned().unwrap_or(json!([])),
                    "Resource": g.get("resource").cloned().unwrap_or(json!({})),
                    "Principal": g.get("principal").cloned().unwrap_or(json!({})),
                })
            })
            .collect::<Vec<Value>>()
    });
    ok(json!({"PrincipalResourcePermissions": items}))
}

fn batch_grant_permissions(svc: &Svc, grant: bool) -> Response {
    let entries = svc
        .body("Entries")
        .as_array()
        .cloned()
        .unwrap_or_default();
    let _ = grant;
    for entry in entries {
        let principal = entry.get("Principal").cloned().unwrap_or(json!({}));
        let resource = entry.get("Resource").cloned().unwrap_or(json!({}));
        let permissions = entry
            .get("Permissions")
            .cloned()
            .unwrap_or(json!([]));
        let with_grant = entry
            .get("PermissionsWithGrantOption")
            .cloned()
            .unwrap_or(json!([]));
        if grant {
            svc.with(|st| {
                let existing = st.grants.iter_mut().find(|g| {
                    g.get("principal") == Some(&principal)
                        && g.get("resource") == Some(&resource)
                });
                match existing {
                    Some(g) => {
                        let current = g.get("permissions").cloned().unwrap_or(json!([]));
                        let cur_list: Vec<String> = current
                            .as_array()
                            .map(|a| {
                                a.iter()
                                    .map(|e| e.as_str().unwrap_or("").to_string())
                                    .collect()
                            })
                            .unwrap_or_default();
                        let new_list: Vec<String> = permissions
                            .as_array()
                            .map(|a| {
                                a.iter()
                                    .map(|e| e.as_str().unwrap_or("").to_string())
                                    .collect()
                            })
                            .unwrap_or_default();
                        g["permissions"] = json_list(union_p(&cur_list, &new_list));
                    }
                    None => st.grants.push(grant_dict(
                        &principal,
                        &resource,
                        permissions.clone(),
                        with_grant.clone(),
                    )),
                }
            });
        } else {
            // revoke single entries
            svc.with(|st| {
                let mut updated: Vec<Value> = Vec::new();
                for g in &st.grants {
                    if g.get("principal") == Some(&principal)
                        && g.get("resource") == Some(&resource)
                    {
                        let current = g.get("permissions").cloned().unwrap_or(json!([]));
                        let cur_list: Vec<String> = current
                            .as_array()
                            .map(|a| {
                                a.iter()
                                    .map(|e| e.as_str().unwrap_or("").to_string())
                                    .collect()
                            })
                            .unwrap_or_default();
                        let new_list: Vec<String> = permissions
                            .as_array()
                            .map(|a| {
                                a.iter()
                                    .map(|e| e.as_str().unwrap_or("").to_string())
                                    .collect()
                            })
                            .unwrap_or_default();
                        let remaining: Vec<String> = cur_list
                            .into_iter()
                            .filter(|p| !new_list.iter().any(|q| q == p))
                            .collect();
                        let mut new_g = g.clone();
                        new_g["permissions"] = json_list(remaining);
                        updated.push(new_g);
                    } else {
                        updated.push(g.clone());
                    }
                }
                st.grants = updated;
            });
        }
    }
    ok(json!({"Failures": []}))
}

/// JSON array from a string list.
fn json_list(values: Vec<String>) -> Value {
    json!(values)
}

/// A JSON value's string elements as owned strings ([] when not a list).
fn str_vec(value: &Value) -> Vec<String> {
    value
        .as_array()
        .map(|a| a.iter().map(|e| e.as_str().unwrap_or("").to_string()).collect())
        .unwrap_or_default()
}

fn str_vec_list(value: &Value) -> Vec<String> {
    str_vec(value)
}

/// Union helper taking references (duplicate-free).
fn union_p(a: &[String], b: &[String]) -> Vec<String> {
    union(a, b)
}

// --- LF tags ---

fn create_lf_tag(svc: &Svc) -> Response {
    let key = svc.body("TagKey").as_str().unwrap_or("").to_string();
    let values = svc.body("TagValues").as_array().cloned().unwrap_or_default();
    svc.with(|st| {
        if let Some(map) = st.lf_tags.as_object_mut() {
            map.insert(key.clone(), json!(values));
        } else {
            st.lf_tags = json!({key.clone(): values});
        }
    });
    ok(json!({}))
}

fn get_lf_tag(svc: &Svc) -> Response {
    let key = svc.body("TagKey").as_str().unwrap_or("").to_string();
    let values = svc.with(|st| {
        st.lf_tags
            .get(&key)
            .cloned()
            .unwrap_or(json!([]))
    });
    ok(json!({
        "CatalogId": svc.catalog(),
        "TagKey": key,
        "TagValues": values,
    }))
}

fn update_lf_tag(svc: &Svc) -> Response {
    let key = svc.body("TagKey").as_str().unwrap_or("").to_string();
    let to_delete = svc
        .body("TagValuesToDelete")
        .as_array()
        .cloned()
        .unwrap_or_default();
    let to_add = svc.body("TagValuesToAdd").as_array().cloned().unwrap_or_default();
    svc.with(|st| {
        let current = st.lf_tags.get(&key).cloned().unwrap_or(json!([]));
        let mut values: Vec<String> = current
            .as_array()
            .map(|a| a.iter().map(|e| e.as_str().unwrap_or("").to_string()).collect())
            .unwrap_or_default();
        for v in &to_add {
            let s = v.as_str().unwrap_or("").to_string();
            if !values.contains(&s) {
                values.push(s);
            }
        }
        values.retain(|v| !to_delete.iter().any(|d| d.as_str() == Some(v.as_str())));
        if let Some(map) = st.lf_tags.as_object_mut() {
            map.insert(key.clone(), json!(values));
        }
    });
    ok(json!({}))
}

fn delete_lf_tag(svc: &Svc) -> Response {
    let key = svc.body("TagKey").as_str().unwrap_or("").to_string();
    svc.with(|st| {
        if let Some(map) = st.lf_tags.as_object_mut() {
            map.remove(&key);
        }
        // also detach resource tags using this key
        for tags in st.database_tags.values_mut() {
            tags.retain(|t| t.get("TagKey").and_then(Value::as_str) != Some(key.as_str()));
        }
        for tags in st.table_tags.values_mut() {
            tags.retain(|t| t.get("TagKey").and_then(Value::as_str) != Some(key.as_str()));
        }
    });
    ok(json!({}))
}

fn list_lf_tags(svc: &Svc) -> Response {
    let tags = svc.with(|st| st.lf_tags.clone());
    let list = match tags.as_object() {
        Some(map) => map
            .iter()
            .map(|(k, values)| json!({"CatalogId": svc.catalog(), "TagKey": k, "TagValues": values}))
            .collect::<Vec<Value>>(),
        None => vec![],
    };
    ok(json!({"LFTags": list}))
}

fn lf_tag_tuple(svc: &Svc, tag: &Value) -> Value {
    json!({
        "CatalogId": svc.catalog(),
        "TagKey": tag.get("TagKey").cloned().unwrap_or(json!("")),
        "TagValues": tag.get("TagValues").cloned().unwrap_or(json!([])),
    })
}

fn add_lf_tags_to_resource(svc: &Svc) -> Response {
    let resource = svc.body("Resource");
    let tags = svc.body("LFTags").as_array().cloned().unwrap_or_default();
    let mut failures: Vec<Value> = Vec::new();
    let unknown = svc.with(|st| {
        let mut unknown: Vec<Value> = Vec::new();
        for tag in &tags {
            let key = tag.get("TagKey").and_then(Value::as_str).unwrap_or("");
            let exists = st
                .lf_tags
                .get(key)
                .map(|v| !v.as_array().map(|a| a.is_empty()).unwrap_or(false))
                .unwrap_or(false)
                || st.lf_tags.as_object().map(|o| o.contains_key(key)).unwrap_or(false);
            if !exists {
                let mut t = tag.clone();
                if t.get("CatalogId").is_none() {
                    t["CatalogId"] = json!(svc.catalog());
                }
                unknown.push(t.clone());
            }
        }
        unknown
    });
    for t in unknown {
        failures.push(json!({
            "LFTag": t,
            "Error": {"ErrorCode": "EntityNotFoundException", "ErrorMessage": "Tag or tag value does not exist."},
        }));
    }
    if !failures.is_empty() {
        return ok(json!({"Failures": failures}));
    }
    let normalized: Vec<Value> = tags.iter().map(|t| lf_tag_tuple(svc, t)).collect();
    svc.with(|st| {
        if let Some(db) = resource.get("Database") {
            let name = db.get("Name").and_then(Value::as_str).unwrap_or("");
            let catalog = db
                .get("CatalogId")
                .and_then(Value::as_str)
                .unwrap_or(&svc.account)
                .to_string();
            st.database_tags
                .insert(composite(&[&catalog, name]), normalized.clone());
        }
        if let Some(table) = resource.get("Table") {
            let db_name = table.get("DatabaseName").and_then(Value::as_str).unwrap_or("");
            let name = table.get("Name").and_then(Value::as_str).unwrap_or("");
            let catalog = table
                .get("CatalogId")
                .and_then(Value::as_str)
                .unwrap_or(&svc.account)
                .to_string();
            st.table_tags.insert(
                composite(&[&catalog, db_name, name]),
                normalized.clone(),
            );
        }
    });
    ok(json!({"Failures": []}))
}

fn remove_lf_tags_from_resource(svc: &Svc) -> Response {
    let resource = svc.body("Resource");
    let tags = svc.body("LFTags").as_array().cloned().unwrap_or_default();
    svc.with(|st| {
        if let Some(db) = resource.get("Database") {
            let name = db.get("Name").and_then(Value::as_str).unwrap_or("");
            let catalog = db
                .get("CatalogId")
                .and_then(Value::as_str)
                .unwrap_or(&svc.account)
                .to_string();
            let k = composite(&[&catalog, name]);
            let tags_key = k;
            if let Some(existing) = st.database_tags.get_mut(&tags_key) {
                for t in &tags {
                    remove_one(existing, t);
                }
            }
        }
        if let Some(table) = resource.get("Table") {
            let db_name = table.get("DatabaseName").and_then(Value::as_str).unwrap_or("");
            let name = table.get("Name").and_then(Value::as_str).unwrap_or("");
            let catalog = table
                .get("CatalogId")
                .and_then(Value::as_str)
                .unwrap_or(&svc.account)
                .to_string();
            let k = composite(&[&catalog, db_name, name]);
            if let Some(existing) = st.table_tags.get_mut(&k) {
                for t in &tags {
                    remove_one(existing, t);
                }
            }
        }
    });
    ok(json!({}))
}

fn remove_one(existing: &mut Vec<Value>, tag: &Value) {
    if let Some(idx) = existing
        .iter()
        .position(|t| t.get("TagKey") == tag.get("TagKey"))
    {
        existing.remove(idx);
    }
}

fn get_resource_lf_tags(svc: &Svc) -> Response {
    let resource = svc.body("Resource");
    let mut dict = json!({});
    let db = svc.with(|st| {
        if let Some(db) = resource.get("Database") {
            let name = db.get("Name").and_then(Value::as_str).unwrap_or("");
            let catalog = db
                .get("CatalogId")
                .and_then(Value::as_str)
                .unwrap_or(&svc.account)
                .to_string();
            st.database_tags
                .get(&composite(&[&catalog, name]))
                .cloned()
        } else {
            None
        }
    });
    let db_tags = db.unwrap_or_default();
    if !db_tags.is_empty() {
        dict["LFTagOnDatabase"] = json!(db_tags);
    }
    ok(dict)
}

fn search_databases_by_lf_tags(_svc: &Svc) -> Response {
    ok(json!({"DatabaseList": []}))
}

fn search_tables_by_lf_tags(_svc: &Svc) -> Response {
    ok(json!({"TableList": []}))
}

// --- LF tag expressions ---

fn create_lf_tag_expression(svc: &Svc) -> Response {
    let catalog = svc.catalog();
    let name = svc.body("Name").as_str().unwrap_or("").to_string();
    let expression = svc.body("Expression");
    let description = svc
        .body("Description")
        .as_str()
        .unwrap_or("")
        .to_string();
    let k = composite(&[&catalog, &name]);
    let exists = svc.with(|st| st.lf_tag_expressions.contains_key(&k));
    if exists {
        return already_exists(
            "An error occurred (AlreadyExistsException) when calling the CreateLFTagExpression operation: LF-Tag expression with this name already exists",
        );
    }
    let mut dict = json!({"Name": name, "CatalogId": catalog, "Expression": expression});
    if !description.is_empty() {
        dict["Description"] = json!(description);
    }
    svc.with(|st| st.lf_tag_expressions.insert(k, dict));
    ok(json!({}))
}

fn get_lf_tag_expression(svc: &Svc) -> Response {
    let catalog = svc.catalog();
    let name = svc.body("Name").as_str().unwrap_or("").to_string();
    let k = composite(&[&catalog, &name]);
    let found = svc.with(|st| st.lf_tag_expressions.get(&k).cloned());
    match found {
        Some(dict) => ok(dict),
        None => entity_not_found(),
    }
}

fn update_lf_tag_expression(svc: &Svc) -> Response {
    let catalog = svc.catalog();
    let name = svc.body("Name").as_str().unwrap_or("").to_string();
    let k = composite(&[&catalog, &name]);
    let expression = svc.body("Expression");
    let description = svc.body("Description");
    let touched = svc.with(|st| {
        match st.lf_tag_expressions.get_mut(&k) {
            Some(dict) => {
                if !expression.is_null() {
                    dict["Expression"] = expression.clone();
                }
                if !description.is_null() {
                    dict["Description"] = description.clone();
                }
                true
            }
            None => false,
        }
    });
    if touched {
        ok(json!({}))
    } else {
        entity_not_found()
    }
}

fn delete_lf_tag_expression(svc: &Svc) -> Response {
    let catalog = svc.catalog();
    let name = svc.body("Name").as_str().unwrap_or("").to_string();
    let k = composite(&[&catalog, &name]);
    if svc.with(|st| st.lf_tag_expressions.remove(&k).is_some()) {
        ok(json!({}))
    } else {
        entity_not_found()
    }
}

fn list_lf_tag_expressions(svc: &Svc) -> Response {
    let expressions = svc
        .with(|st| {
            st.lf_tag_expressions
                .values()
                .filter(|e| e.get("CatalogId").and_then(Value::as_str) == Some(svc.catalog().as_str()))
                .cloned()
                .collect::<Vec<Value>>()
        });
    ok(json!({"LFTagExpressions": expressions}))
}

// --- Transactions ---

fn start_transaction(svc: &Svc) -> Response {
    let id = uuid::Uuid::new_v4().to_string();
    let tx_type = svc
        .body("TransactionType")
        .as_str()
        .map(String::from)
        .unwrap_or_else(|| "READ_AND_WRITE".to_string());
    svc.with(|st| {
        st.transactions.insert(
            id.clone(),
            json!({
                "TransactionId": id,
                "TransactionStatus": "ACTIVE",
                "TransactionStartTime": now_unix(),
                "TransactionType": tx_type,
            }),
        );
    });
    ok(json!({"TransactionId": id}))
}

fn describe_transaction(svc: &Svc) -> Response {
    let id = svc.body("TransactionId").as_str().unwrap_or("").to_string();
    let found = svc.with(|st| st.transactions.get(&id).cloned());
    match found {
        Some(tx) => ok(json!({"TransactionDescription": tx})),
        None => entity_not_found(),
    }
}

fn commit_transaction(svc: &Svc) -> Response {
    let id = svc.body("TransactionId").as_str().unwrap_or("").to_string();
    let found = svc.with(|st| {
        match st.transactions.get_mut(&id) {
            Some(tx) => {
                tx["TransactionStatus"] = json!("COMMITTED");
                tx["TransactionEndTime"] = json!(now_unix());
                true
            }
            None => false,
        }
    });
    if found {
        ok(json!({"TransactionStatus": "COMMITTED"}))
    } else {
        entity_not_found()
    }
}

fn cancel_transaction(svc: &Svc) -> Response {
    let id = svc.body("TransactionId").as_str().unwrap_or("").to_string();
    let found = svc.with(|st| {
        match st.transactions.get_mut(&id) {
            Some(tx) => {
                tx["TransactionStatus"] = json!("ABORTED");
                tx["TransactionEndTime"] = json!(now_unix());
                true
            }
            None => false,
        }
    });
    if found {
        ok(json!({}))
    } else {
        entity_not_found()
    }
}

fn extend_transaction(svc: &Svc) -> Response {
    let id = svc.body("TransactionId").as_str().unwrap_or("").to_string();
    let found = svc.with(|st| st.transactions.contains_key(&id));
    if found {
        ok(json!({}))
    } else {
        entity_not_found()
    }
}

fn list_transactions(svc: &Svc) -> Response {
    let transactions = svc
        .with(|st| st.transactions.values().cloned().collect::<Vec<Value>>())
        .into_iter()
        .map(|tx| {
            json!({
                "TransactionId": tx.get("TransactionId"),
                "TransactionStatus": tx.get("TransactionStatus"),
                "TransactionStartTime": tx.get("TransactionStartTime"),
            })
        })
        .collect::<Vec<Value>>();
    ok(json!({"Transactions": transactions}))
}

fn update_table_storage_optimizer(_svc: &Svc) -> Response {
    ok(json!({"Result": ""}))
}

// --- Data cells filters ---

fn dcf_key(svc: &Svc, body: &Value) -> String {
    let catalog = body
        .get("TableCatalogId")
        .and_then(Value::as_str)
        .unwrap_or(&svc.account)
        .to_string();
    let db = body.get("DatabaseName").and_then(Value::as_str).unwrap_or("");
    let table = body.get("TableName").and_then(Value::as_str).unwrap_or("");
    let name = body.get("Name").and_then(Value::as_str).unwrap_or("");
    composite(&[&catalog, db, table, name])
}


fn create_data_cells_filter(svc: &Svc) -> Response {
    let body = svc.body("TableData");
    let key = dcf_key(svc, &body);
    let exists = svc.with(|st| st.data_cells_filters.contains_key(&key));
    if exists {
        return already_exists("Data cell filter already exists");
    }
    svc.with(|st| st.data_cells_filters.insert(key, body));
    ok(json!({}))
}

fn get_data_cells_filter(svc: &Svc) -> Response {
    let body = json!({
        "TableCatalogId": svc.body("TableCatalogId"),
        "DatabaseName": svc.body("DatabaseName"),
        "TableName": svc.body("TableName"),
        "Name": svc.body("Name"),
    });
    let key = dcf_key(svc, &body);
    let found = svc.with(|st| st.data_cells_filters.get(&key).cloned());
    match found {
        Some(filter) => ok(json!({"DataCellsFilter": filter})),
        None => entity_not_found(),
    }
}

fn delete_data_cells_filter(svc: &Svc) -> Response {
    let body = json!({
        "TableCatalogId": svc.body("TableCatalogId"),
        "DatabaseName": svc.body("DatabaseName"),
        "TableName": svc.body("TableName"),
        "Name": svc.body("Name"),
    });
    let key = dcf_key(svc, &body);
    if svc.with(|st| st.data_cells_filters.remove(&key).is_some()) {
        ok(json!({}))
    } else {
        entity_not_found()
    }
}

fn update_data_cells_filter(svc: &Svc) -> Response {
    let body = svc.body("TableData");
    let key = dcf_key(svc, &body);
    let touched = svc.with(|st| {
        match st.data_cells_filters.get_mut(&key) {
            Some(filter) => {
                if let Some(v) = body.get("RowFilter") {
                    filter["RowFilter"] = v.clone();
                }
                if let Some(v) = body.get("ColumnNames") {
                    filter["ColumnNames"] = v.clone();
                }
                if let Some(v) = body.get("ColumnWildcard") {
                    filter["ColumnWildcard"] = v.clone();
                }
                true
            }
            None => false,
        }
    });
    if touched {
        ok(json!({}))
    } else {
        entity_not_found()
    }
}

fn list_data_cells_filter(svc: &Svc) -> Response {
    let filters = svc
        .with(|st| st.data_cells_filters.values().cloned().collect::<Vec<Value>>())
        .into_iter()
        .map(|mut filter| {
            if let Value::Null = filter.get("ColumnNames").cloned().unwrap_or(Value::Null) {
                filter
                    .as_object_mut()
                    .map(|o| o.remove("ColumnNames"));
            }
            if let Value::Null = filter.get("ColumnWildcard").cloned().unwrap_or(Value::Null) {
                filter
                    .as_object_mut()
                    .map(|o| o.remove("ColumnWildcard"));
            }
            filter
        })
        .collect::<Vec<Value>>();
    ok(json!({"DataCellsFilters": filters}))
}

// --- Credentials / query planning ---

fn temporary_credentials(_svc: &Svc, _partition: bool) -> Response {
    ok(json!({
        "AccessKeyId": "AKIAIOSFODNN7EXAMPLE",
        "SecretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "SessionToken": "FwoGZXIvYXdzEBYaDHqa0AP1HCbMGnS/3SLIAbRQhGIOdVKGCMEZxbQtiAEdHRwqEicrW8hR",
        "Expiration": unix_in(3600.0),
    }))
}

fn temporary_data_location_credentials(svc: &Svc) -> Response {
    let _ = svc;
    ok(json!({
        "Credentials": {
            "AccessKeyId": "AKIAIOSFODNN7EXAMPLE",
            "SecretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "SessionToken": "FwoGZXIvYXdzEBYaDHqa0AP1HCbMGnS/3SLIAbRQhGIOdVKGCMEZxbQtiAEdHRwqEicrW8hR",
            "Expiration": unix_in(900.0),
        }
    }))
}

fn start_query_planning(_svc: &Svc) -> Response {
    ok(json!({"QueryId": uuid::Uuid::new_v4().to_string()}))
}

fn get_query_state(_svc: &Svc) -> Response {
    ok(json!({"State": "FINISHED"}))
}

fn get_query_statistics(_svc: &Svc) -> Response {
    ok(json!({
        "ExecutionStatistics": {"AverageExecutionTimeMillis": 0, "DataScannedBytes": 0, "WorkUnitsExecutedCount": 0},
        "PlanningStatistics": {"EstimatedDataToScanBytes": 0, "PlanningTimeMillis": 0, "QueueTimeMillis": 0, "WorkUnitsGeneratedCount": 0},
        "SubmissionTime": now_unix(),
    }))
}

fn get_work_units(_svc: &Svc) -> Response {
    ok(json!({"WorkUnitRanges": []}))
}

fn get_work_unit_results(_svc: &Svc) -> Response {
    ok(json!({"ResultStream": ""}))
}

// --- Identity-center configuration ---

fn create_identity_center_config(svc: &Svc) -> Response {
    let exists = svc.with(|st| st.identity_center_config.is_some());
    if exists {
        return already_exists(
            "An error occurred (AlreadyExistsException) when calling the CreateLakeFormationIdentityCenterConfiguration operation: Lake Formation Identity Center configuration already exists",
        );
    }
    let catalog = svc.catalog();
    let app_arn = format!("arn:aws:sso::aws:account/{catalog}/application/default");
    let dict = json!({
        "CatalogId": catalog,
        "InstanceArn": svc.body("InstanceArn"),
        "ApplicationArn": app_arn,
        "ExternalFiltering": svc.body("ExternalFiltering"),
        "ShareRecipients": svc.body("ShareRecipients"),
        "ServiceIntegrations": svc.body("ServiceIntegrations"),
        "ResourceShare": format!("arn:aws:ram:{}:{}:resource-share/default", svc.region, svc.account),
    });
    svc.with(|st| st.identity_center_config = Some(dict.clone()));
    ok(json!({"ApplicationArn": app_arn}))
}

fn describe_identity_center_config(svc: &Svc) -> Response {
    let found = svc.with(|st| st.identity_center_config.clone());
    match found {
        Some(dict) => ok(dict),
        None => entity_not_found(),
    }
}

fn update_identity_center_config(svc: &Svc) -> Response {
    let share_recipients = svc.body("ShareRecipients");
    let service_integrations = svc.body("ServiceIntegrations");
    let application_status = svc.body("ApplicationStatus");
    let external_filtering = svc.body("ExternalFiltering");
    let touched = svc.with(|st| {
        match &mut st.identity_center_config {
            Some(dict) => {
                if !share_recipients.is_null() {
                    dict["ShareRecipients"] = share_recipients.clone();
                }
                if !service_integrations.is_null() {
                    dict["ServiceIntegrations"] = service_integrations.clone();
                }
                if !application_status.is_null() {
                    dict["ApplicationStatus"] = application_status.clone();
                }
                if !external_filtering.is_null() {
                    dict["ExternalFiltering"] = external_filtering.clone();
                }
                true
            }
            None => false,
        }
    });
    if touched {
        ok(json!({}))
    } else {
        entity_not_found()
    }
}

fn delete_identity_center_config(svc: &Svc) -> Response {
    let removed = svc.with(|st| st.identity_center_config.take().is_some());
    if removed {
        ok(json!({}))
    } else {
        entity_not_found()
    }
}
