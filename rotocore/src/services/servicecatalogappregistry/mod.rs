//! AWS Service Catalog App Registry (`servicecatalog-appregistry`) — ported
//! from the moto fork's `servicecatalogappregistry` module.
//!
//! Moto surface (`moto/servicecatalogappregistry/`): CreateApplication,
//! ListApplications, AssociateResource, ListAssociatedResources,
//! PutConfiguration, GetConfiguration. Every other operation in the botocore
//! model (attribute groups, GetApplication, the tags triad, SyncResource, ...)
//! has no handler in moto's module either and answers the shared 501
//! NotImplemented error.
//!
//! Requests arrive labeled `servicecatalog`: botocore signs appregistry calls
//! with the Service Catalog signing name, so the services arm consults this
//! module by URI family and falls through to the JSON Service Catalog
//! backend for everything else (the two URI families are disjoint).
//!
//! moto quirks kept verbatim:
//! - `application.tags` in every response is `{}` — moto stores create-time
//!   tags on its side-channel TaggingService, which this service's API
//!   never exposes.
//! - the RESOURCE_TAG_VALUE associate fabricates a resource group whose ARN
//!   hardcodes `us-west-1` as the region (moto's `FakeResourceGroup`).
//! - `creationTime`/`lastUpdateTime` use `str(datetime.now())`, the
//!   space-separated form botocore's dateutil parser accepts.
//! - `AssociateResource` echoes the raw input back in `resourceArn`, even
//!   when the resource normalizes to a resource-group ARN in the listing.

use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Value};

static SLOT: Lazy<crate::store::Slot<AppRegistryState>> =
    Lazy::new(|| crate::store::slot::<AppRegistryState>("servicecatalogappregistry"));

#[derive(Default)]
struct AppRegistryState {
    /// Applications in creation order, like moto's insertion-ordered dict.
    applications: Vec<Application>,
    /// moto initializes `{"tagQueryConfiguration": {}}`; PutConfiguration
    /// replaces the whole value.
    configuration: Value,
}

struct Application {
    id: String,
    arn: String,
    name: Value,
    description: Value,
    creation_time: String,
    /// Associated resources in attach order, keyed by the resolved `arn`
    /// (re-associating replaces, like moto's dict assignment).
    resources: Vec<AssociatedResource>,
}

struct AssociatedResource {
    name: String,
    arn: String,
    resource_type: String,
    options: Vec<String>,
    /// `resourceDetails.tagValue`, only for RESOURCE_TAG_VALUE.
    tag_value: Option<String>,
}

/// moto's application lookup: by arn (dict key), then id, then name.
fn app_matches(app: &Application, search: &str) -> bool {
    app.arn == search
        || app.id == search
        || app.name.as_str().map(|n| n == search).unwrap_or_default()
}

impl Application {
    fn to_json(&self) -> Value {
        json!({
            "id": self.id,
            "arn": self.arn,
            "name": self.name,
            "description": self.description,
            "creationTime": self.creation_time,
            "lastUpdateTime": self.creation_time,
            "tags": {},
            "applicationTag": {"awsApplication": self.arn},
        })
    }
}

impl AssociatedResource {
    fn to_json(&self) -> Value {
        let mut out = json!({
            "name": self.name,
            "arn": self.arn,
            "resourceType": self.resource_type,
            "options": self.options,
        });
        if self.resource_type == "RESOURCE_TAG_VALUE" {
            out["resourceDetails"] = json!({"tagValue": self.tag_value});
        }
        out
    }
}

pub fn dispatch(req: &Request) -> Option<Response> {
    let path = req.path.trim_end_matches('/');
    if !is_appregistry_uri(path) {
        return None;
    }
    let params = crate::proto::parse_json_body(&req.body);
    let slot = SLOT.clone();
    let mut guard = slot.write();
    let st = guard.entry((req.account, req.region.clone())).or_default();
    if st.configuration.is_null() {
        st.configuration = json!({"tagQueryConfiguration": {}});
    }

    // moto's urls.py matches the application segment greedily — an
    // application may be addressed by ARN, and ARNs contain slashes:
    // `^/applications/(?P<application>.+)/resources...`. Reproduce the two
    // URL patterns with the same numeric groups instead of splitting on '/'.
    static RESOURCES_WITH: Lazy<regex::Regex> =
        Lazy::new(|| regex::Regex::new(r"^/applications/(.+)/resources/([^/]+)/(.+)$").unwrap());
    static RESOURCES_ONLY: Lazy<regex::Regex> =
        Lazy::new(|| regex::Regex::new(r"^/applications/(.+)/resources$").unwrap());

    let rel = path.trim_start_matches('/');
    let segments_for_501: Vec<&str> = {
        // Single-segment-shaped view for the op-name table; greedy matches
        // below collapse the ARN into one segment, matching moto's naming.
        match RESOURCES_WITH.captures(path) {
            Some(c) => vec![
                "applications",
                c.get(1).unwrap().as_str(),
                "resources",
                c.get(2).unwrap().as_str(),
                c.get(3).unwrap().as_str(),
            ],
            None => rel.split('/').take(3).collect(),
        }
    };

    Some(match (req.method.as_str(), rel) {
        ("POST", "applications") => op_create_application(st, &params, req),
        ("GET", "applications") => op_list_applications(st),
        ("PUT", "configuration") => op_put_configuration(st, &params),
        ("GET", "configuration") => op_get_configuration(&st.configuration),
        (method, _) if rel.starts_with("applications/") => {
            // Trailing-slash trimming already applied; the greedy match on
            // the `/`-trimmed path mirrors moto's decodes.
            let greedy_path = path.to_string();
            if let Some(c) = RESOURCES_WITH.captures(&greedy_path) {
                // /applications/.+/resources/TYPE(.+)$ — moto routes this
                // shape to every method; responses.associate_resource owns
                // the PUT, everything else 501s.
                let application = c.get(1).unwrap().as_str();
                let resource_type = c.get(2).unwrap().as_str();
                let resource = c.get(3).unwrap().as_str();
                match method {
                    "PUT" => op_associate_resource(
                        st,
                        req,
                        application,
                        resource_type,
                        &[resource],
                        &params,
                    ),
                    _ => not_implemented(req, &segments_for_501),
                }
            } else if let Some(c) = RESOURCES_ONLY.captures(&greedy_path) {
                let application = c.get(1).unwrap().as_str();
                match method {
                    "GET" => op_list_associated_resources(st, application),
                    _ => not_implemented(req, &segments_for_501),
                }
            } else {
                not_implemented(req, &segments_for_501)
            }
        }
        _ => not_implemented(req, &segments_for_501),
    })
}

/// The URI families the `servicecatalog-appregistry` botocore model targets.
fn is_appregistry_uri(path: &str) -> bool {
    let trimmed = path.trim_start_matches('/');
    matches!(trimmed, "configuration")
        || trimmed.starts_with("applications")
        || trimmed.starts_with("attribute-groups")
        || trimmed.starts_with("sync/")
        || trimmed.starts_with("tags/")
}

fn bad(status: u16, code: &str, message: &str) -> Response {
    crate::proto::json_error(status, code, message)
}

/// moto raises `NotImplementedError("The {snake_action} action has not been
/// implemented")` for every modeled URI its responses class does not own.
fn not_implemented(req: &Request, segments: &[&str]) -> Response {
    let action = unimplemented_action(req.method.as_str(), segments);
    if action.is_empty() {
        return bad(
            501,
            "NotImplemented",
            &format!("The {} action has not been implemented", req.operation),
        );
    }
    bad(
        501,
        "NotImplemented",
        &format!("The {action} action has not been implemented"),
    )
}

/// URI → botocore operation name (snake_case, as in moto's error message).
fn unimplemented_action(method: &str, segments: &[&str]) -> &'static str {
    match (method, segments) {
        ("POST", ["applications", _, "attribute-groups", ..]) => "associate_attribute_group",
        ("DELETE", ["applications", _, "attribute-groups", ..]) => "disassociate_attribute_group",
        ("GET", ["applications", _, "attribute-groups"]) => "list_associated_attribute_groups",
        ("GET", ["applications", _, "attribute-group-details"]) => {
            "list_attribute_groups_for_application"
        }
        ("GET", ["applications", _, "resources", ..]) => "get_associated_resource",
        ("DELETE", ["applications", _, "resources", ..]) => "disassociate_resource",
        ("DELETE", ["applications", ..]) => "delete_application",
        ("PATCH", ["applications", ..]) => "update_application",
        ("GET", ["applications", ..]) => "get_application",
        ("POST", ["attribute-groups"]) => "create_attribute_group",
        ("GET", ["attribute-groups"]) => "list_attribute_groups",
        ("PATCH", ["attribute-groups", ..]) => "update_attribute_group",
        ("DELETE", ["attribute-groups", ..]) => "delete_attribute_group",
        ("GET", ["tags", ..]) => "list_tags_for_resource",
        ("POST", ["tags", ..]) => "tag_resource",
        ("DELETE", ["tags", ..]) => "untag_resource",
        ("POST", ["sync", ..]) => "sync_resource",
        _ => "",
    }
}

/// moto: `str(datetime.datetime.now())`.
fn now_str() -> String {
    chrono::Local::now()
        .format("%Y-%m-%d %H:%M:%S%.6f")
        .to_string()
}

/// moto `mock_random.get_random_string(27, include_digits=True,
/// lower_case=True)` — lowercase letters plus digits.
fn random_id() -> String {
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let bytes = uuid::Uuid::new_v4().as_bytes().to_vec();
    (0..27)
        .map(|i| CHARSET[bytes[i % bytes.len()] as usize % CHARSET.len()] as char)
        .collect()
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

fn op_create_application(st: &mut AppRegistryState, params: &Value, req: &Request) -> Response {
    let name = params.get("name").cloned().unwrap_or(Value::Null);
    let description = params.get("description").cloned().unwrap_or(Value::Null);
    // Create-time tags go to moto's TaggingService side channel; nothing in
    // the service's implemented surface exposes them again.
    let id = random_id();
    let arn = format!(
        "arn:{}:servicecatalog:{}:{}:applications/{id}",
        partition(&req.region),
        req.region,
        req.account
    );
    let time = now_str();
    let app = Application {
        id: id.clone(),
        arn: arn.clone(),
        name,
        description,
        creation_time: time.clone(),
        resources: Vec::new(),
    };
    let out = app.to_json();
    st.applications.push(app);
    Response::json_status(200, &json!({"application": out}))
}

fn op_list_applications(st: &AppRegistryState) -> Response {
    let apps: Vec<Value> = st.applications.iter().map(Application::to_json).collect();
    Response::json_status(200, &json!({"applications": apps}))
}

fn op_associate_resource(
    st: &mut AppRegistryState,
    req: &Request,
    application_path: &str,
    resource_type_path: &str,
    resource_rest: &[&str],
    params: &Value,
) -> Response {
    // moto decodes the path variables; the greedy `resource` segment may
    // carry unencoded path characters of its own.
    let application = crate::proto::url_decode(application_path);
    let resource_type = crate::proto::url_decode(resource_type_path);
    let resource = crate::proto::url_decode(&resource_rest.join("/"));
    let Some(app) = st
        .applications
        .iter_mut()
        .find(|app| app_matches(app, &application))
    else {
        // moto: _find_app_by_any_value raises ValidationException
        return bad(
            400,
            "ValidationException",
            "The request has invalid or missing parameters.",
        );
    };
    let options: Vec<String> = match params.get("options").filter(|v| !v.is_null()) {
        Some(list) => list
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(String::from)
                    .collect()
            })
            .unwrap_or_default(),
        None => Vec::new(),
    };
    let (name, arn, tag_value) = match resource_type.as_str() {
        "CFN_STACK" => {
            // moto: an exact stack ARN extracts the stack name; anything
            // else is treated as a bare stack name.
            let name = if cfn_stack_arn_matches(&resource) {
                resource.split('/').nth(1).unwrap_or(&resource).to_string()
            } else {
                resource.clone()
            };
            let exists = crate::services::cloudformation::store::with_store(
                req.account,
                &req.region,
                |cf| cf.find_stack(&name).is_some(),
            );
            if !exists {
                return bad(
                    400,
                    "ResourceNotFoundException",
                    &format!("No CloudFormation stack called '{name}' found"),
                );
            }
            (name, resource.clone(), None)
        }
        "RESOURCE_TAG_VALUE" => {
            let app_name = app.name.as_str().unwrap_or_default().to_string();
            let name = format!("AWS_AppRegistry_AppTag_{}-{app_name}", req.account);
            // moto's FakeResourceGroup hardcodes us-west-1 in the ARN.
            let arn = format!(
                "arn:{}:resource-groups:us-west-1:{}:group/{name}",
                partition(&req.region),
                req.account
            );
            (name, arn, Some(resource.clone()))
        }
        _ => {
            return bad(
                400,
                "ValidationException",
                "The request has invalid or missing parameters.",
            )
        }
    };
    let new_resource = AssociatedResource {
        name,
        arn: arn.clone(),
        resource_type,
        options: options.clone(),
        tag_value,
    };
    let out = json!({
        "applicationArn": app.arn,
        "resourceArn": resource,
        "options": options,
    });
    if let Some(existing) = app.resources.iter_mut().find(|r| r.arn == new_resource.arn) {
        *existing = new_resource;
    } else {
        app.resources.push(new_resource);
    }
    Response::json_status(200, &out)
}

fn op_list_associated_resources(st: &AppRegistryState, application: &str) -> Response {
    // moto decodes the path variable; it is usually an arn and never had
    // characters worth decoding, but mirror the behavior anyway.
    let application = crate::proto::url_decode(application);
    let Some(app) = st
        .applications
        .iter()
        .find(|app| app_matches(app, &application))
    else {
        return bad(
            400,
            "ValidationException",
            "The request has invalid or missing parameters.",
        );
    };
    let resources: Vec<Value> = app
        .resources
        .iter()
        .map(AssociatedResource::to_json)
        .collect();
    Response::json_status(200, &json!({"resources": resources}))
}

fn op_put_configuration(st: &mut AppRegistryState, params: &Value) -> Response {
    let configuration = params.get("configuration").cloned().unwrap_or(Value::Null);
    st.configuration = configuration;
    // moto's put_configuration returns None: a bare 200 with an empty body.
    Response::new(200, "text/html", b"")
}

fn op_get_configuration(configuration: &Value) -> Response {
    Response::json_status(200, &json!({"configuration": configuration}))
}

/// moto's stack-ARN shape test (kept as a literal translation of the fork's
/// regex); anything else is treated as a bare stack name.
fn cfn_stack_arn_matches(resource: &str) -> bool {
    use once_cell::sync::Lazy;
    static RE: Lazy<regex::Regex> = Lazy::new(|| regex::Regex::new(CFN_STACK_ARN_RE).unwrap());
    RE.is_match(resource)
}

const CFN_STACK_ARN_RE: &str = r"^arn:aws:cloudformation:(us(-gov)?|ap|ca|cn|eu|sa)-(central|(north|south)?(east|west)?)-\d:\d{12}:stack/\w[a-zA-Z0-9\-]{0,127}/[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}$";

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap as StdMap;

    fn request(method: &str, path: &str, body: Value) -> Request {
        request_as(123456789012, method, path, body)
    }

    fn request_as(account: u64, method: &str, path: &str, body: Value) -> Request {
        Request {
            service: "servicecatalog".into(),
            operation: String::new(),
            method: method.into(),
            path: path.into(),
            query_string: String::new(),
            headers: StdMap::new(),
            body: serde_json::to_vec(&body).unwrap().into(),
            account,
            region: "us-east-1".into(),
        }
    }

    fn body_of(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap()
    }

    #[test]
    fn create_then_list_round_trip() {
        // Unique account so the shared SLOT keeps this count deterministic
        // across the whole lib-test process.
        const ACCOUNT: u64 = 517030001;
        let post = request_as(
            ACCOUNT,
            "POST",
            "/applications",
            json!({"name": "app-1", "description": "d1"}),
        );
        let r = dispatch(&post).unwrap().body;
        let v: Value = serde_json::from_slice(&r).unwrap();
        let app = &v["application"];
        assert_eq!(app["name"], "app-1");
        assert_eq!(app["description"], "d1");
        assert_eq!(app["tags"], json!({}));
        assert_eq!(
            app["applicationTag"]["awsApplication"],
            format!(
                "arn:aws:servicecatalog:us-east-1:{ACCOUNT}:applications/{}",
                app["id"].as_str().unwrap()
            )
        );
        assert!(app["arn"].as_str().unwrap().contains(":servicecatalog:"));

        let r = dispatch(&request_as(ACCOUNT, "GET", "/applications", json!({}))).unwrap();
        let v = body_of(&r);
        assert_eq!(v["applications"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn create_defaults_description_to_null() {
        let r = dispatch(&request("POST", "/applications", json!({"name": "bare"}))).unwrap();
        let v: Value = serde_json::from_slice(&r.body).unwrap();
        assert!(v["application"]["description"].is_null());
        assert!(v["application"]["tags"].as_object().unwrap().is_empty());
    }

    #[test]
    fn associate_tag_value_resource() {
        dispatch(&request_as(
            222222222222,
            "POST",
            "/applications",
            json!({"name": "tagged", "description": ""}),
        ))
        .unwrap();
        let r = dispatch(&request_as(
            222222222222,
            "PUT",
            "/applications/tagged/resources/RESOURCE_TAG_VALUE/env%3Aprod",
            json!({}),
        ))
        .unwrap();
        assert_eq!(r.status, 200);
        let v = body_of(&r);
        assert_eq!(v["resourceArn"], "env:prod");
        assert_eq!(v["options"], json!([]));

        let r = dispatch(&request_as(
            222222222222,
            "GET",
            "/applications/tagged/resources",
            json!({}),
        ))
        .unwrap();
        let v = body_of(&r);
        let res = &v["resources"][0];
        assert_eq!(res["resourceType"], "RESOURCE_TAG_VALUE");
        assert_eq!(
            res["name"],
            "AWS_AppRegistry_AppTag_222222222222-tagged".to_string()
        );
        assert_eq!(res["resourceDetails"]["tagValue"], "env:prod");
    }

    #[test]
    fn associate_unknown_type_is_validation_error() {
        dispatch(&request("POST", "/applications", json!({"name": "vex"}))).unwrap();
        let r = dispatch(&request(
            "PUT",
            "/applications/vex/resources/SQS_QUEUE/queue-1",
            json!({}),
        ))
        .unwrap();
        assert_eq!(r.status, 400);
        let v = body_of(&r);
        assert_eq!(v["__type"], "ValidationException");
    }

    #[test]
    fn associate_missing_cfn_stack_is_resource_not_found() {
        dispatch(&request("POST", "/applications", json!({"name": "cfny"}))).unwrap();
        let r = dispatch(&request(
            "PUT",
            "/applications/cfny/resources/CFN_STACK/arn:aws:cloudformation:us-east-1:123456789012:stack/absent/0a0a0a0a-0b0b-4c4c-8d8d-0e0e0e0e0e0e",
            json!({}),
        ))
        .unwrap();
        assert_eq!(r.status, 400);
        let v = body_of(&r);
        assert_eq!(v["__type"], "ResourceNotFoundException");
        assert_eq!(
            v["message"],
            "No CloudFormation stack called 'absent' found"
        );
    }

    #[test]
    fn unknown_application_is_validation_error() {
        let r = dispatch(&request(
            "GET",
            "/applications/whoknows/resources",
            json!({}),
        ))
        .unwrap();
        assert_eq!(r.status, 400);
        let v = body_of(&r);
        assert_eq!(v["__type"], "ValidationException");

        let r = dispatch(&request(
            "PUT",
            "/applications/whoknows/resources/RESOURCE_TAG_VALUE/t",
            json!({}),
        ))
        .unwrap();
        assert_eq!(r.status, 400);
    }

    #[test]
    fn configuration_round_trip() {
        let r = dispatch(&request("GET", "/configuration", json!({}))).unwrap();
        let v = body_of(&r);
        assert_eq!(v["configuration"], json!({"tagQueryConfiguration": {}}));

        let r = dispatch(&request(
            "PUT",
            "/configuration",
            json!({"configuration": {"tagQueryConfiguration": {"tagKey": "createdBy"}}}),
        ))
        .unwrap();
        assert_eq!(r.status, 200);

        let r = dispatch(&request("GET", "/configuration", json!({}))).unwrap();
        let v = body_of(&r);
        assert_eq!(
            v["configuration"],
            json!({"tagQueryConfiguration": {"tagKey": "createdBy"}})
        );
    }

    #[test]
    fn find_app_by_id_arn_and_name() {
        let r = dispatch(&request("POST", "/applications", json!({"name": "byid"}))).unwrap();
        let v: Value = serde_json::from_slice(&r.body).unwrap();
        let id = v["application"]["id"].as_str().unwrap().to_string();
        let arn = v["application"]["arn"].as_str().unwrap().to_string();

        for search in [&arn, &id, &String::from("byid")] {
            let r = dispatch(&request(
                "GET",
                &format!("/applications/{search}/resources"),
                json!({}),
            ))
            .unwrap();
            assert_eq!(r.status, 200, "lookup by {search} failed");
        }
    }

    #[test]
    fn unimplemented_ops_answer_501_verbatim() {
        let r = dispatch(&request(
            "GET",
            "/applications/some-arn/attribute-groups",
            json!({}),
        ))
        .unwrap();
        assert_eq!(r.status, 501);
        let v: Value = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(v["__type"], "NotImplemented");
        assert_eq!(
            v["message"],
            "The list_associated_attribute_groups action has not been implemented"
        );

        let r = dispatch(&request("DELETE", "/applications/app-1", json!({}))).unwrap();
        assert_eq!(r.status, 501);
        let v: Value = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(
            v["message"],
            "The delete_application action has not been implemented"
        );

        let r = dispatch(&request("POST", "/attribute-groups", json!({}))).unwrap();
        assert_eq!(r.status, 501);
        let v: Value = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(
            v["message"],
            "The create_attribute_group action has not been implemented"
        );

        let r = dispatch(&request("GET", "/tags/arn:whatever", json!({}))).unwrap();
        assert_eq!(r.status, 501);
        let v: Value = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(
            v["message"],
            "The list_tags_for_resource action has not been implemented"
        );
    }

    #[test]
    fn non_appregistry_paths_pass_through() {
        // Service Catalog's JSON surface owns the rest of the signing scope.
        assert!(dispatch(&request("POST", "/", json!({}))).is_none());
        assert!(dispatch(&request("GET", "/other", json!({}))).is_none());
    }
}
