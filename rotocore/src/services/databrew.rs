//! DataBrew backend (REST-JSON).
//!
//! Port of moto's databrew: recipes (versioned), rulesets, datasets,
//! projects, schedules and profile/recipe jobs. Records mirror moto's
//! `as_dict` shapes — PascalCase keys; "CreateDate" as a millisecond
//! string for datasets/rulesets/recipes/jobs but as floats for projects
//! and schedules; None-valued job members are dropped. Ruleset lookups
//! fail with EntityNotFoundException, everything else ResourceNotFound.

use crate::proto::{json_error, parse_json_body};
use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Map, Value};

#[derive(Default)]
struct State {
    recipes: Vec<(String, Recipe)>,
    rulesets: Vec<(String, Value)>,
    datasets: Vec<(String, Value)>,
    projects: Vec<(String, Value)>,
    schedules: Vec<(String, Value)>,
    jobs: Vec<(String, Job)>,
}

struct Recipe {
    versions: Vec<(f64, Value)>,
    latest_working: f64,
    latest_published: Option<f64>,
}

#[derive(Default)]
struct Job {
    job_type: &'static str,
    /// subclass attrs stored under their PascalCase output names.
    local: Map<String, Value>,
    dataset_name: Option<Value>,
    encryption_mode: Option<Value>,
    log_subscription: Option<Value>,
    max_capacity: Option<Value>,
    max_retries: Option<Value>,
    role_arn: Option<Value>,
    tags: Value,
    created: String,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("databrew"));
    SLOT.clone()
}

const LATEST_WORKING: &str = "LATEST_WORKING";
const LATEST_PUBLISHED: &str = "LATEST_PUBLISHED";

/// Wire members consumed by UpdateProfileJob / UpdateRecipeJob (moto
/// setattr's every update kwarg, wiping absent ones to None).
const PROFILE_UPDATE_MEMBERS: [&str; 3] = [
    "OutputLocation",
    "Configuration",
    "ValidationConfigurations",
];
const RECIPE_UPDATE_MEMBERS: [&str; 5] = [
    "DatabaseOutputs",
    "DataCatalogOutputs",
    "Outputs",
    "ProjectName",
    "RecipeReference",
];

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

fn now_ms_str() -> String {
    format!(
        "{:.3}",
        (chrono::Utc::now().timestamp_millis() as f64) / 1000.0
    )
}

fn now_f() -> f64 {
    (chrono::Utc::now().timestamp_millis()) as f64 / 1000.0
}

fn not_found(message: &str) -> Response {
    json_error(404, "ResourceNotFoundException", message)
}

fn general_missing() -> Response {
    not_found("One or more resources can't be found.")
}

fn validation(message: &str) -> Response {
    json_error(400, "ValidationException", message)
}

fn conflict(message: &str) -> Response {
    json_error(409, "ConflictException", message)
}

fn ruleset_not_found(name: &str) -> Response {
    json_error(
        400,
        "EntityNotFoundException",
        &format!("Ruleset {name} not found."),
    )
}

fn ok_json(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

fn segments(path: &str) -> Vec<&str> {
    path.split('/')
        .filter(|s| !s.is_empty())
        .map(|s| s.split('?').next().unwrap_or(s))
        .collect()
}

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "databrew" {
        return None;
    }
    let segs = segments(&req.path);
    if segs.is_empty() {
        return None;
    }
    let method = req.method.as_str();
    let body = parse_json_body(&req.body);

    let slot = state();
    let mut st = slot.write();
    let s = st.entry((req.account, req.region.clone())).or_default();
    let region = &req.region;
    let account = req.account.to_string();

    let head = segs[0];
    let res = match (method, head, segs.len()) {
        // ---- datasets (full record responses) ----
        ("POST", "datasets", 1) => create_dataset(s, &body, region, &account),
        ("GET", "datasets", 1) => list_all("Datasets", &s.datasets),
        ("GET", "datasets", 2) => look_record(&s.datasets, segs[1], general_missing()),
        ("PUT", "datasets", 2) => update_dataset(s, segs[1], &body, region, &account),
        ("DELETE", "datasets", 2) => delete_general(&mut s.datasets, segs[1]),
        // ---- projects ----
        ("POST", "projects", 1) => create_project(s, &body, region, &account),
        ("GET", "projects", 1) => list_all("Projects", &s.projects),
        ("GET", "projects", 2) => look_record(&s.projects, segs[1], general_missing()),
        ("PUT", "projects", 2) => update_project(s, segs[1], &body),
        ("DELETE", "projects", 2) => delete_general(&mut s.projects, segs[1]),
        // ---- schedules ----
        ("POST", "schedules", 1) => create_schedule(s, &body, region, &account),
        ("GET", "schedules", 1) => list_all("Schedules", &s.schedules),
        ("GET", "schedules", 2) => look_record(&s.schedules, segs[1], general_missing()),
        ("PUT", "schedules", 2) => update_schedule(s, segs[1], &body),
        ("DELETE", "schedules", 2) => delete_general(&mut s.schedules, segs[1]),
        // ---- rulesets ----
        ("POST", "rulesets", 1) => create_ruleset(s, &body),
        ("GET", "rulesets", 1) => list_all("Rulesets", &s.rulesets),
        ("GET", "rulesets", 2) => look_record(&s.rulesets, segs[1], ruleset_not_found(segs[1])),
        ("PUT", "rulesets", 2) => update_ruleset(s, segs[1], &body),
        ("DELETE", "rulesets", 2) => delete_ruleset(s, segs[1]),
        // ---- recipes ----
        ("POST", "recipes", 1) => create_recipe(s, &body),
        ("GET", "recipes", 1) => list_recipes(s, &req.query_string),
        ("GET", "recipes", 2) => describe_recipe(s, segs[1], req.query_string.as_str()),
        ("PUT", "recipes", 2) => update_recipe(s, segs[1], &body),
        ("POST", "recipes", 3) if segs[2] == "publishRecipe" => publish_recipe(s, segs[1]),
        ("DELETE", "recipes", 4) if segs[2] == "recipeVersion" => {
            delete_recipe_version(s, segs[1], segs[3])
        }
        ("GET", "recipeVersions", 1) => list_recipe_versions(s, req.query_string.as_str()),
        // ---- jobs ----
        ("GET", "jobs", 1) => list_jobs(s, req.query_string.as_str(), region, &account),
        ("GET", "jobs", 2) => describe_job(s, segs[1], region, &account),
        ("DELETE", "jobs", 2) => delete_job(s, segs[1]),
        ("POST", "profileJobs", 1) => create_job(s, &body, region, &account, "PROFILE"),
        ("POST", "recipeJobs", 1) => create_job(s, &body, region, &account, "RECIPE"),
        ("PUT", "profileJobs", 2) => update_job(s, segs[1], &body, region, &account, "PROFILE"),
        ("PUT", "recipeJobs", 2) => update_job(s, segs[1], &body, region, &account, "RECIPE"),
        _ => return None,
    };
    Some(res)
}

fn look_record(records: &[(String, Value)], name: &str, missing: Response) -> Response {
    match records.iter().find(|(k, _)| *k == name) {
        Some((_, v)) => ok_json(v),
        None => missing,
    }
}

fn list_all(key: &str, records: &[(String, Value)]) -> Response {
    let items: Vec<Value> = records.iter().map(|(_, v)| v.clone()).collect();
    ok_json(&json!({ key: items, "NextToken": Value::Null }))
}

fn delete_general(records: &mut Vec<(String, Value)>, name: &str) -> Response {
    if !records.iter().any(|(k, _)| *k == name) {
        return general_missing();
    }
    records.retain(|(k, _)| *k != name);
    ok_json(&json!({ "Name": name }))
}

// ---------------------------------------------------------------------------
// Datasets / projects / schedules / rulesets (Value records)
// ---------------------------------------------------------------------------

fn create_dataset(s: &mut State, p: &Value, region: &str, account: &str) -> Response {
    let name = p["Name"].as_str().unwrap_or_default().to_string();
    if s.datasets.iter().any(|(k, _)| *k == name) {
        return json_error(
            400,
            "AlreadyExistsException",
            &format!("{name} already exists."),
        );
    }
    let record = json!({
        "Name": name,
        "Format": p["Format"].clone(),
        "FormatOptions": p["FormatOptions"].clone(),
        "Input": p["Input"].clone(),
        "PathOptions": p["PathOptions"].clone(),
        "CreateDate": now_ms_str(),
        "Tags": p["Tags"].clone(),
        "ResourceArn": format!(
            "arn:{}:databrew:{region}:{account}:dataset/{name}",
            partition(region)
        ),
    });
    s.datasets.push((name, record.clone()));
    ok_json(&record)
}

fn update_dataset(s: &mut State, name: &str, p: &Value, region: &str, account: &str) -> Response {
    let Some(rec) = s
        .datasets
        .iter_mut()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v)
    else {
        return general_missing();
    };
    for key in ["Format", "FormatOptions", "Input", "PathOptions", "Tags"] {
        if let Some(v) = p.get(key).filter(|v| !v.is_null()) {
            rec[key] = v.clone();
        }
    }
    rec["ResourceArn"] = json!(format!(
        "arn:{}:databrew:{region}:{account}:dataset/{name}",
        partition(region)
    ));
    ok_json(rec)
}

fn create_ruleset(s: &mut State, p: &Value) -> Response {
    let name = p["Name"].as_str().unwrap_or_default().to_string();
    if s.rulesets.iter().any(|(k, _)| *k == name) {
        return json_error(400, "AlreadyExistsException", "Ruleset already exists.");
    }
    let record = json!({
        "Name": name,
        "Rules": p["Rules"].clone(),
        "Description": p["Description"].clone(),
        "TargetArn": p["TargetArn"].clone(),
        "CreateDate": now_ms_str(),
        "Tags": p["Tags"].clone(),
    });
    s.rulesets.push((name, record.clone()));
    ok_json(&record)
}

fn update_ruleset(s: &mut State, name: &str, p: &Value) -> Response {
    let Some(rec) = s
        .rulesets
        .iter_mut()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v)
    else {
        return ruleset_not_found(name);
    };
    for key in ["Description", "Rules", "Tags"] {
        if let Some(v) = p.get(key).filter(|v| !v.is_null()) {
            rec[key] = v.clone();
        }
    }
    ok_json(rec)
}

fn delete_ruleset(s: &mut State, name: &str) -> Response {
    if !s.rulesets.iter().any(|(k, _)| *k == name) {
        return json_error(
            400,
            "EntityNotFoundException",
            &format!("Ruleset {name} not found."),
        );
    }
    s.rulesets.retain(|(k, _)| *k != name);
    ok_json(&json!({ "Name": name }))
}

fn create_schedule(s: &mut State, p: &Value, region: &str, account: &str) -> Response {
    let name = p["Name"].as_str().unwrap_or_default().to_string();
    if s.schedules.iter().any(|(k, _)| *k == name) {
        return conflict(&format!("The schedule {name} already exists."));
    }
    let created = now_f();
    let record = json!({
        "Name": name,
        "CronExpression": p["CronExpression"].clone(),
        "JobNames": if p["JobNames"].is_null() { json!([]) } else { p["JobNames"].clone() },
        "Tags": p["Tags"].clone(),
        "CreateDate": created,
        "LastModifiedDate": created,
        "ResourceArn": format!(
            "arn:{}:databrew:{region}:{account}:schedule/{name}",
            partition(region)
        ),
    });
    s.schedules.push((name, record));
    ok_json(&json!({ "Name": p["Name"].clone() }))
}

fn update_schedule(s: &mut State, name: &str, p: &Value) -> Response {
    let Some(rec) = s
        .schedules
        .iter_mut()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v)
    else {
        return general_missing();
    };
    if let Some(v) = p.get("CronExpression").filter(|v| !v.is_null()) {
        rec["CronExpression"] = v.clone();
    }
    if let Some(v) = p.get("JobNames").filter(|v| !v.is_null()) {
        rec["JobNames"] = v.clone();
    }
    rec["LastModifiedDate"] = json!(now_f());
    ok_json(&json!({ "Name": name }))
}

fn create_project(s: &mut State, p: &Value, region: &str, account: &str) -> Response {
    let name = p["Name"].as_str().unwrap_or_default().to_string();
    if s.projects.iter().any(|(k, _)| *k == name) {
        return conflict(&format!("The project {name} already exists."));
    }
    let created = now_f();
    let mut record = json!({
        "Name": name,
        "DatasetName": p["DatasetName"].clone(),
        "RecipeName": p["RecipeName"].clone(),
        "RoleArn": p["RoleArn"].clone(),
        "CreateDate": created,
        "LastModifiedDate": created,
        "Tags": p["Tags"].clone(),
        "ResourceArn": format!(
            "arn:{}:databrew:{region}:{account}:project/{name}",
            partition(region)
        ),
    });
    if p["Sample"]
        .as_object()
        .map(|m| !m.is_empty())
        .unwrap_or(false)
    {
        if let Some(s0) = p.get("Sample") {
            record["Sample"] = s0.clone();
        }
    }
    s.projects.push((name, record));
    ok_json(&json!({ "Name": p["Name"].clone() }))
}

fn update_project(s: &mut State, name: &str, p: &Value) -> Response {
    let Some(rec) = s
        .projects
        .iter_mut()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v)
    else {
        return general_missing();
    };
    if let Some(v) = p.get("RoleArn").filter(|v| !v.is_null()) {
        rec["RoleArn"] = v.clone();
    }
    if let Some(v) = p.get("Sample").filter(|v| !v.is_null()) {
        rec["Sample"] = v.clone();
    }
    rec["LastModifiedDate"] = json!(now_f());
    let last = rec["LastModifiedDate"].clone();
    ok_json(&json!({ "Name": name, "LastModifiedDate": last }))
}

// ---------------------------------------------------------------------------
// Recipes
// ---------------------------------------------------------------------------

fn ver_str(v: f64) -> String {
    format!("{v:.1}")
}

fn recipe_version_value(recipe: &Recipe, v: f64) -> Option<Value> {
    recipe
        .versions
        .iter()
        .find(|(vv, _)| *vv == v)
        .map(|(_, rec)| {
            let mut out = rec.clone();
            out["RecipeVersion"] = json!(ver_str(v));
            out
        })
}

fn create_recipe(s: &mut State, p: &Value) -> Response {
    let name = p["Name"].as_str().unwrap_or_default().to_string();
    if s.recipes.iter().any(|(k, _)| *k == name) {
        return conflict(&format!("The recipe {name} already exists"));
    }
    let record = recipe_version_record(p, name.as_str(), 0.1, true);
    s.recipes.push((
        name,
        Recipe {
            versions: vec![(0.1, record.clone())],
            latest_working: 0.1,
            latest_published: None,
        },
    ));
    ok_json(&record)
}

/// Build a FakeRecipeVersion.as_dict()-shaped record.
fn recipe_version_record(source: &Value, name: &str, version: f64, fresh: bool) -> Value {
    let _ = fresh;
    json!({
        "Name": name,
        "Steps": source.get("Steps").cloned().unwrap_or(Value::Null),
        "Description": source.get("Description").cloned().unwrap_or(Value::Null),
        "CreateDate": now_ms_str(),
        "Tags": source.get("Tags").cloned().unwrap_or_else(|| json!({})),
        "RecipeVersion": ver_str(version),
    })
}

fn list_recipes(s: &State, query: &str) -> Response {
    let version_param = query_of(query, "recipeVersion");
    let items: Vec<Value> = s
        .recipes
        .iter()
        .filter_map(|(_, recipe)| {
            match version_param.as_deref() {
                Some(LATEST_WORKING) => {
                    let v = recipe.latest_working;
                    recipe_version_value(recipe, v)
                }
                // None and LATEST_PUBLISHED both mean latest published
                _ => {
                    let v = recipe.latest_published?;
                    recipe_version_value(recipe, v)
                }
            }
        })
        .collect();
    ok_json(&json!({ "Recipes": items, "NextToken": Value::Null }))
}

/// First value of a query param (percent chars aside — names are simple).
fn query_of(query: &str, key: &str) -> Option<String> {
    for pair in query.split('&') {
        let mut it = pair.splitn(2, '=');
        if it.next()? == key {
            return Some(it.next().unwrap_or("").to_string());
        }
    }
    None
}

fn describe_recipe(s: &State, name: &str, query: &str) -> Response {
    let version_param = query_of(query, "recipeVersion");
    let effective = match &version_param {
        None => LATEST_PUBLISHED,
        Some(vp) => {
            if !(vp.len() <= 16
                && (vp.parse::<f64>().is_ok() || *vp == LATEST_PUBLISHED || *vp == LATEST_WORKING))
            {
                return validation(&format!("Recipe {name} version {vp} isn't valid."));
            }
            vp
        }
    }
    .to_string();
    let Some((_, recipe)) = s.recipes.iter().find(|(k, _)| *k == name) else {
        return not_found(&format!(
            "The recipe {name} for version {effective} wasn't found."
        ));
    };
    let found = if effective == LATEST_WORKING {
        recipe_version_value(recipe, recipe.latest_working)
    } else if effective == LATEST_PUBLISHED {
        recipe
            .latest_published
            .and_then(|v| recipe_version_value(recipe, v))
    } else {
        version_param
            .and_then(|vp| vp.parse::<f64>().ok())
            .and_then(|f| {
                recipe
                    .versions
                    .iter()
                    .find(|(v, _)| *v == f)
                    .map(|(v, rec)| {
                        let mut out = rec.clone();
                        out["RecipeVersion"] = json!(ver_str(*v));
                        out
                    })
            })
    };
    match found {
        Some(rec) => ok_json(&rec),
        None => not_found(&format!(
            "The recipe {name} for version {effective} wasn't found."
        )),
    }
}

fn update_recipe(s: &mut State, name: &str, p: &Value) -> Response {
    let Some(rec) = s
        .recipes
        .iter_mut()
        .find(|(k, _)| k == name)
        .map(|(_, r)| r)
    else {
        return not_found(&format!("The recipe {name} wasn't found"));
    };
    let working = rec.latest_working;
    let Some(vrec) = rec
        .versions
        .iter_mut()
        .find(|(v, _)| *v == working)
        .map(|(_, r)| r)
    else {
        return not_found(&format!("The recipe {name} wasn't found"));
    };
    if let Some(v) = p.get("Description").filter(|v| !v.is_null()) {
        vrec["Description"] = v.clone();
    }
    if let Some(v) = p.get("Steps").filter(|v| !v.is_null()) {
        vrec["Steps"] = v.clone();
    }
    ok_json(&json!({ "Name": name }))
}

fn publish_recipe(s: &mut State, name: &str) -> Response {
    let Some(rec) = s
        .recipes
        .iter_mut()
        .find(|(k, _)| k == name)
        .map(|(_, r)| r)
    else {
        return not_found(&format!("Recipe {name} wasn't found"));
    };
    let working_v = rec.latest_working;
    let published_v = working_v.ceil();
    let idx = rec
        .versions
        .iter()
        .position(|(v, _)| *v == working_v)
        .unwrap_or(0);
    let mut published = rec.versions[idx].1.clone();
    published["RecipeVersion"] = json!(ver_str(published_v));
    rec.versions.remove(idx);
    rec.versions.push((published_v, published.clone()));
    let new_working = published_v + 0.1;
    let mut working_rec = published.clone();
    working_rec["CreateDate"] = json!(now_ms_str());
    working_rec["RecipeVersion"] = json!(ver_str(new_working));
    rec.versions.push((new_working, working_rec));
    rec.latest_working = new_working;
    rec.latest_published = Some(published_v);
    ok_json(&json!({ "Name": name }))
}

fn delete_recipe_version(s: &mut State, name: &str, version: &str) -> Response {
    // version validity: "LATEST_WORKING" or a float string
    let Ok(ver) = version.parse::<f64>() else {
        if version == LATEST_WORKING {
            let Some(rec) = s
                .recipes
                .iter_mut()
                .find(|(k, _)| k == name)
                .map(|(_, r)| r)
            else {
                return not_found(&format!("The recipe {name} wasn't found"));
            };
            if rec.latest_published.is_some() {
                return validation(&format!(
                    "Recipe version {version} is not allowed to be deleted"
                ));
            }
            // only the working version exists -> delete the whole recipe
            s.recipes.retain(|(k, _)| k != name);
            return ok_json(&json!({ "Name": name, "RecipeVersion": version }));
        }
        return validation(&format!("Recipe {name} version {version} is invalid."));
    };
    let Some(rec) = s
        .recipes
        .iter_mut()
        .find(|(k, _)| k == name)
        .map(|(_, r)| r)
    else {
        return not_found(&format!("The recipe {name} wasn't found"));
    };
    if rec.versions.iter().all(|(v, _)| *v != ver) {
        return not_found(&format!(
            "The recipe {name} version {version} wasn't found."
        ));
    }
    rec.versions.retain(|(v, _)| *v != ver);
    if rec.latest_published == Some(ver) {
        rec.latest_published = None;
    }
    ok_json(&json!({ "Name": name, "RecipeVersion": version }))
}

fn list_recipe_versions(s: &State, query: &str) -> Response {
    let name = query_of(query, "name").unwrap_or_default();
    let Some((_, recipe)) = s.recipes.iter().find(|(k, _)| *k == name) else {
        return ok_json(&json!({ "Recipes": [], "NextToken": Value::Null }));
    };
    let items: Vec<Value> = recipe
        .versions
        .iter()
        .filter(|(v, _)| *v != recipe.latest_working)
        .filter_map(|(v, _)| recipe_version_value(recipe, *v))
        .collect();
    ok_json(&json!({ "Recipes": items, "NextToken": Value::Null }))
}

// ---------------------------------------------------------------------------
// Jobs
// ---------------------------------------------------------------------------

fn job_record(job: &Job, name: &str, region: &str, account: &str) -> Value {
    let mut dict = json!({
        "Name": name,
        "AccountId": account,
        "CreateDate": job.created.clone(),
        "DatasetName": job.dataset_name.clone(),
        "EncryptionMode": job.encryption_mode.clone(),
        "Tags": if job.tags.is_null() { json!({}) } else { job.tags.clone() },
        "LogSubscription": job.log_subscription.clone(),
        "MaxCapacity": job.max_capacity.clone(),
        "MaxRetries": job.max_retries.clone(),
        "ResourceArn": format!(
            "arn:{}:databrew:{region}:{account}:job/{name}",
            partition(region)
        ),
        "RoleArn": job.role_arn.clone(),
        "Type": job.job_type,
    });
    if let Some(obj) = dict.as_object_mut() {
        for (k, v) in &job.local {
            obj.insert(k.clone(), v.clone());
        }
        obj.retain(|_, v| !v.is_null());
    }
    dict
}

fn validate_job_inputs(p: &Value) -> Option<Response> {
    if let Some(mode) = p.get("EncryptionMode").filter(|v| !v.is_null()) {
        if !matches!(mode.as_str(), Some("SSE-S3") | Some("SSE-KMS")) {
            return Some(validation(&format!(
                "1 validation error detected: Value '{}' at 'encryptionMode' failed to satisfy constraint: Member must satisfy enum value set: [SSE-S3, SSE-KMS]",
                mode.as_str().unwrap_or_default()
            )));
        }
    }
    if let Some(sub) = p.get("LogSubscription").filter(|v| !v.is_null()) {
        if !matches!(sub.as_str(), Some("ENABLE") | Some("DISABLE")) {
            return Some(validation(&format!(
                "1 validation error detected: Value '{}' at 'logSubscription' failed to satisfy constraint: Member must satisfy enum value set: [ENABLE, DISABLE]",
                sub.as_str().unwrap_or_default()
            )));
        }
    }
    None
}

fn create_job(
    s: &mut State,
    p: &Value,
    region: &str,
    account: &str,
    job_type: &'static str,
) -> Response {
    let name = p["Name"].as_str().unwrap_or_default().to_string();
    if let Some((_, existing)) = s.jobs.iter().find(|(k, _)| k == &name) {
        return conflict(&format!(
            "The job {name} {} job already exists.",
            existing.job_type.to_lowercase()
        ));
    }
    if let Some(err) = validate_job_inputs(p) {
        return err;
    }
    let members: &[&str] = if job_type == "PROFILE" {
        &PROFILE_UPDATE_MEMBERS
    } else {
        &RECIPE_UPDATE_MEMBERS
    };
    let local: Map<String, Value> = members
        .iter()
        .filter_map(|k| {
            p.get(*k)
                .filter(|v| !v.is_null())
                .map(|v| ((*k).to_string(), v.clone()))
        })
        .collect();
    let job = Job {
        job_type,
        local,
        dataset_name: p.get("DatasetName").filter(|v| !v.is_null()).cloned(),
        encryption_mode: p.get("EncryptionMode").filter(|v| !v.is_null()).cloned(),
        log_subscription: p.get("LogSubscription").filter(|v| !v.is_null()).cloned(),
        max_capacity: p.get("MaxCapacity").filter(|v| !v.is_null()).cloned(),
        max_retries: p.get("MaxRetries").filter(|v| !v.is_null()).cloned(),
        role_arn: p.get("RoleArn").filter(|v| !v.is_null()).cloned(),
        tags: p
            .get("Tags")
            .filter(|v| !v.is_null())
            .cloned()
            .unwrap_or(json!({})),
        created: now_ms_str(),
    };
    let record = job_record(&job, &name, region, account);
    s.jobs.push((name.clone(), job));
    ok_json(&record)
}

fn update_job(
    s: &mut State,
    name: &str,
    p: &Value,
    region: &str,
    account: &str,
    job_type: &'static str,
) -> Response {
    let Some((_, job)) = s.jobs.iter_mut().find(|(k, _)| k == name) else {
        return not_found(&format!("The job {name} wasn't found"));
    };
    // moto setattr's every update kwarg — absent ones wipe to None.
    job.encryption_mode = p.get("EncryptionMode").filter(|v| !v.is_null()).cloned();
    job.log_subscription = p.get("LogSubscription").filter(|v| !v.is_null()).cloned();
    job.max_capacity = p.get("MaxCapacity").filter(|v| !v.is_null()).cloned();
    job.max_retries = p.get("MaxRetries").filter(|v| !v.is_null()).cloned();
    job.role_arn = p.get("RoleArn").filter(|v| !v.is_null()).cloned();
    let members: &[&str] = if job_type == "PROFILE" {
        &PROFILE_UPDATE_MEMBERS
    } else {
        &RECIPE_UPDATE_MEMBERS
    };
    for key in members {
        match p.get(*key).filter(|v| !v.is_null()) {
            Some(v) => job.local.insert((*key).to_string(), v.clone()),
            None => job.local.remove(*key),
        };
    }
    let record = job_record(job, name, region, account);
    ok_json(&record)
}

fn describe_job(s: &State, name: &str, region: &str, account: &str) -> Response {
    let Some((n, job)) = s.jobs.iter().find(|(k, _)| *k == name) else {
        return not_found(&format!("Job {name} wasn't found."));
    };
    ok_json(&job_record(job, n, region, account))
}

fn delete_job(s: &mut State, name: &str) -> Response {
    let Some(idx) = s.jobs.iter().position(|(k, _)| k == name) else {
        return not_found(&format!("The job {name} wasn't found."));
    };
    s.jobs.remove(idx);
    ok_json(&json!({ "Name": name }))
}

fn list_jobs(s: &State, query: &str, region: &str, account: &str) -> Response {
    let dataset_filter: Option<String> = query_of(query, "datasetName");
    let project_filter = query_of(query, "projectName");
    let items: Vec<Value> = s
        .jobs
        .iter()
        .filter(|(_name, job)| {
            dataset_filter
                .as_deref()
                .map(|want| {
                    job.dataset_name
                        .as_ref()
                        .and_then(Value::as_str)
                        .map(|dn| dn == want)
                        .unwrap_or(false)
                })
                .unwrap_or(true)
                && project_filter
                    .as_deref()
                    .map(|want| {
                        job.local
                            .get("ProjectName")
                            .and_then(Value::as_str)
                            .map(|pn| pn == want)
                            .unwrap_or(false)
                    })
                    .unwrap_or(true)
        })
        .map(|(n, j)| job_record(j, n, region, account))
        .collect();
    ok_json(&json!({ "Jobs": items, "NextToken": Value::Null }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::HashMap;

    fn req(method: &str, path: &str, body: Value) -> Request {
        let (p, q) = match path.split_once('?') {
            Some((a, b)) => (a.to_string(), b.to_string()),
            None => (path.to_string(), String::new()),
        };
        Request {
            service: "databrew".into(),
            operation: String::new(),
            method: method.into(),
            path: p,
            query_string: q,
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

    fn steps() -> Value {
        json!([{"Action": {"Operation": "UPPER_CASE", "Parameters": {"sourceColumn": "col1"}}}])
    }

    #[test]
    fn dataset_crud_not_found() {
        let acct = scope();
        let mut r = req(
            "POST",
            "/datasets",
            json!({"Name": "ds1", "Input": {"k": "v"}}),
        );
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(b["Name"], "ds1");

        assert!(b["CreateDate"].is_string());

        r = req("GET", "/datasets/ds1", json!({}));
        r.account = acct;
        assert_eq!(body(&dispatch(&r).unwrap())["Name"], "ds1");

        r = req("PUT", "/datasets/ds1", json!({"Format": "CSV"}));
        r.account = acct;
        assert_eq!(body(&dispatch(&r).unwrap())["Format"], "CSV");

        r = req("GET", "/datasets", json!({}));
        r.account = acct;
        assert_eq!(body(&dispatch(&r).unwrap())["Datasets"][0]["Name"], "ds1");

        r = req("DELETE", "/datasets/ds1", json!({}));
        r.account = acct;
        assert_eq!(body(&dispatch(&r).unwrap())["Name"], "ds1");
        r = req("DELETE", "/datasets/ds1", json!({}));
        r.account = acct;
        assert_eq!(dispatch(&r).unwrap().status, 404);
        assert_eq!(
            body(&dispatch(&r).unwrap())["__type"],
            "ResourceNotFoundException"
        );
    }

    #[test]
    fn recipe_lifecycle() {
        let acct = scope();
        let mut r = req(
            "POST",
            "/recipes",
            json!({"Name": "r1", "Steps": steps(), "Description": "d"}),
        );
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(b["RecipeVersion"], "0.1");
        assert_eq!(b["Name"], "r1");

        // dup -> 409
        r = req("POST", "/recipes", json!({"Name": "r1"}));
        r.account = acct;
        let resp = dispatch(&r).unwrap();
        assert_eq!(resp.status, 409);

        r = req(
            "PUT",
            "/recipes/r1",
            json!({"Steps": steps(), "Description": "up"}),
        );
        r.account = acct;
        assert_eq!(body(&dispatch(&r).unwrap())["Name"], "r1");

        // publish: creates published 1.0 + working 1.1
        r = req("POST", "/recipes/r1/publishRecipe", json!({}));
        r.account = acct;
        assert_eq!(body(&dispatch(&r).unwrap())["Name"], "r1");

        r = req("GET", "/recipes/r1", json!({}));
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(b["RecipeVersion"], "1.0");

        r = req("GET", "/recipes/r1?recipeVersion=LATEST_WORKING", json!({}));
        r.account = acct;
        assert_eq!(body(&dispatch(&r).unwrap())["RecipeVersion"], "1.1");

        r = req("GET", "/recipeVersions?name=r1", json!({}));
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        // after one publish: published 1.0 stays in the versions list
        assert_eq!(b["Recipes"][0]["RecipeVersion"], "1.0");

        // delete published version 1.0
        r = req("DELETE", "/recipes/r1/recipeVersion/1.0", json!({}));
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(b["RecipeVersion"], "1.0");
    }

    #[test]
    fn recipe_delete_latest_working() {
        let acct = scope();
        let mut r = req("POST", "/recipes", json!({"Name": "r2", "Steps": steps()}));
        r.account = acct;
        dispatch(&r).unwrap();
        r = req(
            "DELETE",
            "/recipes/r2/recipeVersion/LATEST_WORKING",
            json!({}),
        );
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(b["Name"], "r2");
        assert_eq!(b["RecipeVersion"], "LATEST_WORKING");

        // published recipes cannot drop LATEST_WORKING
        r = req("POST", "/recipes", json!({"Name": "r3", "Steps": steps()}));
        r.account = acct;
        dispatch(&r).unwrap();
        r = req("POST", "/recipes/r3/publishRecipe", json!({}));
        r.account = acct;
        assert_eq!(dispatch(&r).unwrap().status, 200);
    }

    #[test]
    fn jobs_and_schedules_projects() {
        let acct = scope();
        let mut r = req(
            "POST",
            "/recipeJobs",
            json!({"Name": "j1", "RoleArn": "arn:aws:iam::123456789012:role/t"}),
        );
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(b["Type"], "RECIPE");
        assert_eq!(b["AccountId"], acct.to_string());
        assert!(b["ResourceArn"].as_str().unwrap().ends_with(":job/j1"));

        r = req("GET", "/jobs/j1", json!({}));
        r.account = acct;
        assert_eq!(body(&dispatch(&r).unwrap())["Name"], "j1");

        r = req("DELETE", "/jobs/j1", json!({}));
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(b["Name"], "j1");
        r = req("GET", "/jobs/j1", json!({}));
        r.account = acct;
        assert_eq!(dispatch(&r).unwrap().status, 404);

        // schedule
        r = req(
            "POST",
            "/schedules",
            json!({"Name": "s1", "CronExpression": "* * * * ?"}),
        );
        r.account = acct;
        assert_eq!(body(&dispatch(&r).unwrap())["Name"], "s1");
        r = req("GET", "/schedules/s1", json!({}));
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(b["CronExpression"], "* * * * ?");
        assert!(b["ResourceArn"].as_str().unwrap().ends_with(":schedule/s1"));
        r = req(
            "PUT",
            "/schedules/s1",
            json!({"CronExpression": "0 0 * ? * *"}),
        );
        r.account = acct;
        assert_eq!(body(&dispatch(&r).unwrap())["Name"], "s1");

        // ruleset errors use EntityNotFound
        r = req("GET", "/rulesets/nope", json!({}));
        r.account = acct;
        let resp = dispatch(&r).unwrap();
        assert_eq!(body(&resp)["__type"], "EntityNotFoundException");
        assert_eq!(resp.status, 400);
    }

    #[test]
    fn ruleset_crud() {
        let acct = scope();
        let mut r = req(
            "POST",
            "/rulesets",
            json!({
                "Name": "rs1",
                "TargetArn": "arn:aws:databrew:us-east-1:123456789012:dataset/dummy",
                "Rules": [{"Name": "rule1", "Disabled": false}],
            }),
        );
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(b["Name"], "rs1");
        assert!(b["Rules"].as_array().unwrap().len() == 1);

        r = req("PUT", "/rulesets/rs1", json!({"Rules": []}));
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert!(b["Rules"].as_array().unwrap().is_empty());

        r = req("DELETE", "/rulesets/rs1", json!({}));
        r.account = acct;
        assert_eq!(body(&dispatch(&r).unwrap())["Name"], "rs1");
        r = req("DELETE", "/rulesets/rs1", json!({}));
        r.account = acct;
        assert_eq!(
            body(&dispatch(&r).unwrap())["__type"],
            "EntityNotFoundException"
        );
    }
}
