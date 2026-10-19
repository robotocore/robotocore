//! AWS CloudDirectory backend (REST-JSON, one URI per operation).
//!
//! Port of moto's `clouddirectory`: development/published/applied schemas,
//! directories, facets, typed-link facets, objects (parent/child link maps),
//! indices, typed links, policy attachments, tags and the Batch Read/Write
//! work queues. Wire shapes mirror moto's PascalCase JSON keys; every moto
//! error renders as HTTP 400 with an `x-amzn-ErrorType` header plus the
//! exception's own JSON body (moto's clouddirectory `JsonRESTError`
//! subclasses set the body, regardless of the botocore model status code).
//!
//! ARN parameters (DirectoryArn/SchemaArn/…) arrive in the
//! `x-amz-data-partition` header per botocore's model, and with the
//! gateway keeping no REST op table for clouddirectory the operation is
//! resolved from the HTTP method + botocore requestUri suffix.

use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

/// One directory: its objects plus every derived link/policy/index
/// bookkeeping map moto's `Directory` keeps.
struct Directory {
    name: String,
    schema_arn: String,
    directory_arn: String,
    state: &'static str,
    creation_date_time: String,
    object_identifier: String,
    /// Applied schema ARNs (populated via ApplySchema / upgrades).
    applied_schemas: Vec<String>,
    root_object_id: String,
    /// object identifier -> object
    objects: BTreeMap<String, DirectoryObject>,
    /// parent id -> {link name -> child id}
    children: BTreeMap<String, BTreeMap<String, String>>,
    /// child id -> {parent id -> link name}
    parents: BTreeMap<String, BTreeMap<String, String>>,
    /// policy object id -> attached object ids
    policy_attachments: BTreeMap<String, Vec<String>>,
    /// object id -> attached policy ids
    object_policies: BTreeMap<String, Vec<String>>,
    /// index object id -> index info
    indices: BTreeMap<String, IndexInfo>,
    /// object id -> attached index ids
    object_indices: BTreeMap<String, Vec<String>>,
    typed_links: Vec<TypedLinkInstance>,
}

struct DirectoryObject {
    object_identifier: String,
    schema_facets: Vec<Value>,
    attributes: Vec<Value>,
}

#[derive(Clone)]
struct Facet {
    name: String,
    attributes: Vec<Value>,
    object_type: String,
    facet_style: String,
}

#[derive(Clone)]
struct TypedLinkFacet {
    attributes: Vec<Value>,
    identity_attribute_order: Vec<String>,
}

/// moto's `SchemaInfo`: facet definitions, JSON doc and name per schema ARN
/// (schema_arn rides along; `IsUnique` is accepted but never consulted, as
/// moto keeps it without reading).
struct IndexInfo {
    ordered_indexed_attribute_list: Vec<Value>,
    attached_objects: Vec<String>,
}

struct TypedLinkInstance {
    source_object_id: String,
    target_object_id: String,
    typed_link_facet: Value,
    attributes: Vec<Value>,
}

/// Internal error carrier mirroring moto's clouddirectory `JsonRESTError`
/// subclasses: HTTP 400, `x-amzn-ErrorType` header, exception body.
struct CdError {
    code: &'static str,
    body: Value,
}

#[derive(Default)]
struct State {
    /// directory arn -> directory
    directories: BTreeMap<String, Directory>,
    development: Vec<String>,
    published: Vec<String>,
    applied: Vec<String>,
    /// schema arn -> schema info
    schema_data: BTreeMap<String, SchemaInfo>,
    /// TaggingService: arn -> ordered key/value tags
    tags: HashMap<String, Vec<(String, String)>>,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("clouddirectory"));
    SLOT.clone()
}

// ---------------------------------------------------------------------------
// Wire helpers
// ---------------------------------------------------------------------------

fn ok(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

impl CdError {
    fn wire(&self) -> Response {
        let mut resp = Response::json(400, serde_json::to_vec(&self.body).unwrap_or_default());
        resp.headers
            .push(("x-amzn-ErrorType".to_string(), format!("{}:", self.code)));
        resp
    }

    /// moto's batch frames: `{"ExceptionResponse": {"Type", "Message"}}`,
    /// with Message equal to what `str(exc)` surfaces.
    fn batch_frame(&self) -> Value {
        json!({
            "ExceptionResponse": {
                "Type": self.code,
                "Message": self.body.get("Message").and_then(Value::as_str).unwrap_or_default(),
            }
        })
    }
}

fn not_found(resource_id: impl std::fmt::Display) -> CdError {
    CdError {
        code: "ResourceNotFoundException",
        body: json!({
            "ResourceId": resource_id.to_string(),
            "Message": "Resource not found",
        }),
    }
}

fn invalid_arn(resource_id: impl std::fmt::Display) -> CdError {
    CdError {
        code: "InvalidArnException",
        body: json!({
            "ResourceId": resource_id.to_string(),
            "Message": "Invalid Arn",
        }),
    }
}

fn schema_already_published(schema_arn: impl std::fmt::Display) -> CdError {
    CdError {
        code: "SchemaAlreadyPublishedException",
        body: json!({
            "SchemaArn": schema_arn.to_string(),
            "Message": "Schema already published",
        }),
    }
}

/// The timestamp format moto emits (`str(datetime)`: no timezone suffix).
fn now_str() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%d %H:%M:%S%.6f")
        .to_string()
}

/// moto's object identifiers: uuid4 hex without dashes.
fn object_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

fn param_str<'a>(p: &'a Value, key: &str) -> Option<&'a str> {
    p.get(key).and_then(Value::as_str)
}

fn param_str_or(p: &Value, key: &str, default: &str) -> String {
    param_str(p, key).unwrap_or(default).to_string()
}

fn param_list(p: &Value, key: &str) -> Vec<Value> {
    p.get(key)
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

fn param_i64(p: &Value, key: &str) -> Option<i64> {
    p.get(key).and_then(Value::as_i64)
}

fn partition(region: &str) -> &'static str {
    for (prefix, name) in [
        ("cn-", "aws-cn"),
        ("us-gov-", "aws-us-gov"),
        ("us-iso-", "aws-iso"),
        ("us-isob-", "aws-iso-b"),
    ] {
        if region.starts_with(prefix) {
            return name;
        }
    }
    "aws"
}

/// moto's @paginate slicing with limit_default 100; token is a resume index.
fn paginate<T: Clone>(
    items: &[T],
    next_token: Option<&str>,
    max_results: Option<i64>,
) -> (Vec<T>, Value) {
    let limit = if max_results.is_none_or(|m| m <= 0) {
        100
    } else {
        max_results.unwrap_or(100) as usize
    };
    let start: usize = next_token
        .and_then(|t| t.parse().ok())
        .unwrap_or(0)
        .min(items.len());
    let end = (start + limit).min(items.len());
    let next = if end < items.len() {
        json!(end.to_string())
    } else {
        Value::Null
    };
    (items[start..end].to_vec(), next)
}

// ---------------------------------------------------------------------------
// Field constructors / serializers (moto's model classes)
// ---------------------------------------------------------------------------

/// moto's `SchemaInfo`: facet definitions, JSON doc and name per schema ARN.
struct SchemaInfo {
    facets: BTreeMap<String, Facet>,
    typed_link_facets: BTreeMap<String, TypedLinkFacet>,
    document: String,
    name: String,
}

impl SchemaInfo {
    fn new(schema_arn: &str) -> Self {
        let name = if schema_arn.contains('/') {
            schema_arn
                .rsplit('/')
                .next()
                .unwrap_or_default()
                .to_string()
        } else {
            String::new()
        };
        Self {
            facets: BTreeMap::new(),
            typed_link_facets: BTreeMap::new(),
            document: "{}".into(),
            name,
        }
    }
}

impl Facet {
    /// moto's `Facet.to_dict()`.
    fn to_dict(&self) -> Value {
        json!({
            "Name": self.name,
            "ObjectType": self.object_type,
            "FacetStyle": self.facet_style,
        })
    }
}

impl Directory {
    fn new(region: &str, account: u64, name: String, schema_arn: &str) -> Self {
        let directory_arn = format!(
            "arn:{}:{region}:{account}:directory/{name}",
            partition(region)
        );
        let root_id = object_id();
        let root_obj = DirectoryObject {
            object_identifier: root_id.clone(),
            schema_facets: Vec::new(),
            attributes: Vec::new(),
        };
        let mut objects = BTreeMap::new();
        objects.insert(root_id.clone(), root_obj);
        Self {
            object_identifier: format!("directory-{name}"),
            applied_schemas: Vec::new(),
            schema_arn: schema_arn.to_string(),
            directory_arn,
            state: "ENABLED",
            creation_date_time: now_str(),
            root_object_id: root_id,
            name,
            objects,
            children: BTreeMap::new(),
            parents: BTreeMap::new(),
            policy_attachments: BTreeMap::new(),
            object_policies: BTreeMap::new(),
            indices: BTreeMap::new(),
            object_indices: BTreeMap::new(),
            typed_links: Vec::new(),
        }
    }

    /// moto's `Directory.to_dict()` (`CreationDateTime` is `str(datetime)`).
    fn to_dict(&self) -> Value {
        json!({
            "Name": self.name,
            "SchemaArn": self.schema_arn,
            "DirectoryArn": self.directory_arn,
            "State": self.state,
            "CreationDateTime": self.creation_date_time,
            "ObjectIdentifier": self.object_identifier,
        })
    }
}

// ---------------------------------------------------------------------------
// State lookups (moto's backend helpers)
// ---------------------------------------------------------------------------

fn get_directory<'a>(st: &'a State, directory_arn: &str) -> Result<&'a Directory, CdError> {
    st.directories
        .get(directory_arn)
        .ok_or_else(|| not_found(directory_arn))
}

fn get_directory_mut<'a>(
    st: &'a mut State,
    directory_arn: &str,
) -> Result<&'a mut Directory, CdError> {
    st.directories
        .get_mut(directory_arn)
        .ok_or_else(|| not_found(directory_arn))
}

fn find_schema_arn(st: &State, schema_arn: &str) -> Result<(), CdError> {
    for state_arns in [&st.development, &st.published, &st.applied] {
        if state_arns.iter().any(|a| a == schema_arn) {
            return Ok(());
        }
    }
    Err(not_found(schema_arn))
}

/// moto's `_get_schema_info`: facet container created lazily for any ARN.
fn get_schema_info_mut<'a>(st: &'a mut State, schema_arn: &str) -> &'a mut SchemaInfo {
    st.schema_data
        .entry(schema_arn.to_string())
        .or_insert_with(|| SchemaInfo::new(schema_arn))
}

/// moto's `resolve_object_reference`: `$<id>` for ids, `/` for the root, or a
/// `link/link` path from the root.
fn resolve_object_reference(dir: &Directory, reference: &Value) -> Result<String, CdError> {
    let selector = reference
        .get("Selector")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if selector.is_empty() {
        return Err(not_found("ObjectReference must have a Selector"));
    }
    if let Some(id) = selector.strip_prefix('$') {
        return if dir.objects.contains_key(id) {
            Ok(id.to_string())
        } else {
            Err(not_found(id))
        };
    }
    if selector == "/" {
        return Ok(dir.root_object_id.clone());
    }
    let mut current = dir.root_object_id.clone();
    for part in selector.trim_matches('/').split('/') {
        let next = dir
            .children
            .get(&current)
            .and_then(|children| children.get(part))
            .cloned();
        match next {
            Some(child) => current = child,
            None => return Err(not_found(selector)),
        }
    }
    Ok(current)
}

/// Record a parent/child link (moto's children/parents bookkeeping).
fn link(parent_id: &str, child_id: &str, link_name: &str, dir: &mut Directory) {
    dir.children
        .entry(parent_id.to_string())
        .or_default()
        .insert(link_name.to_string(), child_id.to_string());
    dir.parents
        .entry(child_id.to_string())
        .or_default()
        .insert(parent_id.to_string(), link_name.to_string());
}

/// moto's `_collect_paths`: every root->leaf path that leads to `obj_id`.
fn collect_paths(dir: &Directory, obj_id: &str, current_path: &[String], result: &mut Vec<Value>) {
    let Some(parents) = dir.parents.get(obj_id) else {
        let mut segs = current_path.to_vec();
        segs.reverse();
        let path = format!("/{}", segs.join("/"));
        result.push(json!({
            "Path": path,
            "ObjectIdentifiers": [obj_id],
        }));
        return;
    };
    for (parent_id, link_name) in parents {
        let mut next_path = current_path.to_vec();
        next_path.push(link_name.clone());
        collect_paths(dir, parent_id, &next_path, result);
    }
}

/// Position of the typed-link instance a specifier resolves to, if any.
fn typed_link_position(
    dir: &Directory,
    typed_link_specifier: &Value,
) -> Result<Option<usize>, CdError> {
    let source_ref = typed_link_specifier
        .get("SourceObjectReference")
        .cloned()
        .unwrap_or(json!({}));
    let target_ref = typed_link_specifier
        .get("TargetObjectReference")
        .cloned()
        .unwrap_or(json!({}));
    let facet_name = typed_link_specifier
        .get("TypedLinkFacet")
        .and_then(|f| f.get("TypedLinkName"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let source_id = resolve_object_reference(dir, &source_ref)?;
    let target_id = resolve_object_reference(dir, &target_ref)?;
    Ok(dir.typed_links.iter().position(|tl| {
        tl.source_object_id == source_id
            && tl.target_object_id == target_id
            && tl
                .typed_link_facet
                .get("TypedLinkName")
                .and_then(Value::as_str)
                == Some(facet_name.as_str())
    }))
}

/// moto's TypedLinkInstance `to_specifier()` for wire emission.
fn link_specifier(tl: &TypedLinkInstance) -> Value {
    json!({
        "TypedLinkFacet": tl.typed_link_facet.clone(),
        "SourceObjectReference": {"Selector": format!("${}", tl.source_object_id)},
        "TargetObjectReference": {"Selector": format!("${}", tl.target_object_id)},
        "IdentityAttributeValues": tl.attributes.clone(),
    })
}

/// Deep-copy a schema's info under a new ARN (moto's `copy.deepcopy` branch).
fn copy_schema_data(st: &mut State, from_arn: &str, to_arn: &str) {
    if let Some(info) = st.schema_data.get(from_arn) {
        st.schema_data.insert(
            to_arn.to_string(),
            SchemaInfo {
                facets: info.facets.clone(),
                typed_link_facets: info.typed_link_facets.clone(),
                document: info.document.clone(),
                name: info.name.clone(),
            },
        );
    }
}

/// moto's TaggingService with list-of-{Key,Value} wire flavor.
impl State {
    fn tag_resource(&mut self, arn: &str, tags: &[Value]) {
        let entry = self.tags.entry(arn.to_string()).or_default();
        for tag in tags {
            let key = tag
                .get("Key")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let value = tag
                .get("Value")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            if let Some(pos) = entry.iter().position(|(k, _)| *k == key) {
                entry[pos].1 = value;
            } else {
                entry.push((key, value));
            }
        }
    }

    fn untag_resource(&mut self, arn: &str, tag_keys: &[Value]) {
        let names: Vec<String> = tag_keys
            .iter()
            .map(|k| {
                k.as_str()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| k.to_string())
            })
            .collect();
        if let Some(entry) = self.tags.get_mut(arn) {
            entry.retain(|(k, _)| !names.contains(k));
        }
    }

    /// moto's `TaggingService.list_tags_for_resource(arn)`.
    fn list_tags(&self, arn: &str) -> Vec<Value> {
        self.tags
            .get(arn)
            .map(|entry| {
                entry
                    .iter()
                    .map(|(k, v)| json!({"Key": k.clone(), "Value": v.clone()}))
                    .collect()
            })
            .unwrap_or_default()
    }
}

// ---------------------------------------------------------------------------
// Attribute updates shared by facet / typed-link-facet mutation paths
// ---------------------------------------------------------------------------

/// `AttributeUpdates` entries: [(action, attribute object)].
fn attribute_updates(updates: &Value) -> Vec<(String, Value)> {
    updates
        .as_array()
        .map(|arr| {
            arr.iter()
                .map(|u| {
                    (
                        u.get("Action")
                            .and_then(Value::as_str)
                            .unwrap_or("CREATE_OR_UPDATE")
                            .to_string(),
                        u.get("Attribute").cloned().unwrap_or(json!({})),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

/// moto's Facet/TypedLinkFacet `AttributeUpdates` application.
fn apply_facet_attribute_updates(attributes: &mut Vec<Value>, updates: &Value) {
    for (action, attr) in attribute_updates(updates) {
        let attr_name = attr
            .get("Name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if action == "DELETE" && !attr_name.is_empty() {
            attributes
                .retain(|a| a.get("Name").and_then(Value::as_str) != Some(attr_name.as_str()));
        } else {
            attributes
                .retain(|a| a.get("Name").and_then(Value::as_str) != Some(attr_name.as_str()));
            attributes.push(attr);
        }
    }
}

// ---------------------------------------------------------------------------
// Core object/policy/index/typed-link operations (moto's backend methods).
// Shared between the top-level handlers and the Batch work queues.
// ---------------------------------------------------------------------------

fn core_create_object(
    dir: &mut Directory,
    schema_facets: Vec<Value>,
    object_attribute_list: Option<Vec<Value>>,
    parent_reference: Option<&Value>,
    link_name: Option<&str>,
) -> String {
    let obj_id = object_id();
    dir.objects.insert(
        obj_id.clone(),
        DirectoryObject {
            object_identifier: obj_id.clone(),
            schema_facets,
            attributes: object_attribute_list.unwrap_or_default(),
        },
    );
    if let (Some(parent), Some(link_name)) = (parent_reference, link_name) {
        if let Ok(parent_id) = resolve_object_reference(dir, parent) {
            link(&parent_id, &obj_id, link_name, dir);
        }
    }
    obj_id
}

/// Pairs of (Value, Value) into a BTreeMap with string keys (wire map shape).
fn pairs_to_map(pairs: Vec<(Value, Value)>) -> Map<String, Value> {
    let mut out = Map::new();
    for (k, v) in pairs {
        let key = k
            .as_str()
            .map(|s| s.to_string())
            .unwrap_or_else(|| k.to_string());
        out.insert(key, v);
    }
    out
}

/// moto's `delete_object`: unlink from every parent, then remove the object.
fn core_delete_object(dir: &mut Directory, reference: &Value) -> Result<(), CdError> {
    let obj_id = resolve_object_reference(dir, reference)?;
    if let Some(parents) = dir.parents.get(&obj_id) {
        for parent_id in parents.keys() {
            if let Some(children) = dir.children.get_mut(parent_id) {
                children.retain(|_, child| *child != obj_id);
            }
        }
    }
    dir.parents.remove(&obj_id);
    dir.children.remove(&obj_id);
    dir.objects.remove(&obj_id);
    Ok(())
}

fn core_attach_object(
    dir: &mut Directory,
    parent_reference: &Value,
    child_reference: &Value,
    link_name: &str,
) -> Result<String, CdError> {
    let parent_id = resolve_object_reference(dir, parent_reference)?;
    let child_id = resolve_object_reference(dir, child_reference)?;
    link(&parent_id, &child_id, link_name, dir);
    Ok(child_id)
}

fn core_detach_object(
    dir: &mut Directory,
    parent_reference: &Value,
    link_name: &str,
) -> Result<String, CdError> {
    let parent_id = resolve_object_reference(dir, parent_reference)?;
    let mut child_id = String::new();
    if let Some(children) = dir.children.get_mut(&parent_id) {
        if let Some(child) = children.remove(link_name) {
            child_id = child;
        }
    }
    if !child_id.is_empty() {
        if let Some(parents) = dir.parents.get_mut(&child_id) {
            parents.remove(&parent_id);
        }
    }
    Ok(child_id)
}

fn core_attach_policy(
    dir: &mut Directory,
    policy_reference: &Value,
    object_reference: &Value,
) -> Result<(), CdError> {
    let policy_id = resolve_object_reference(dir, policy_reference)?;
    let obj_id = resolve_object_reference(dir, object_reference)?;
    let policy_entry = dir.policy_attachments.entry(policy_id.clone()).or_default();
    if !policy_entry.contains(&obj_id) {
        policy_entry.push(obj_id.clone());
    }
    let object_entry = dir.object_policies.entry(obj_id).or_default();
    if !object_entry.contains(&policy_id) {
        object_entry.push(policy_id);
    }
    Ok(())
}

fn core_detach_policy(
    dir: &mut Directory,
    policy_reference: &Value,
    object_reference: &Value,
) -> Result<(), CdError> {
    let policy_id = resolve_object_reference(dir, policy_reference)?;
    let obj_id = resolve_object_reference(dir, object_reference)?;
    if let Some(targets) = dir.policy_attachments.get_mut(&policy_id) {
        targets.retain(|t| *t != obj_id);
    }
    if let Some(policies) = dir.object_policies.get_mut(&obj_id) {
        policies.retain(|p| *p != policy_id);
    }
    Ok(())
}

fn core_create_index(
    dir: &mut Directory,
    ordered_indexed_attribute_list: Vec<Value>,
    _is_unique: bool,
    parent_reference: Option<&Value>,
    link_name: Option<&str>,
) -> String {
    let obj_id = object_id();
    dir.objects.insert(
        obj_id.clone(),
        DirectoryObject {
            object_identifier: obj_id.clone(),
            schema_facets: Vec::new(),
            attributes: Vec::new(),
        },
    );
    dir.indices.insert(
        obj_id.clone(),
        IndexInfo {
            ordered_indexed_attribute_list,
            attached_objects: Vec::new(),
        },
    );
    if let (Some(parent), Some(link_name)) = (parent_reference, link_name) {
        if let Ok(parent_id) = resolve_object_reference(dir, parent) {
            link(&parent_id, &obj_id, link_name, dir);
        }
    }
    obj_id
}

fn core_attach_to_index(
    dir: &mut Directory,
    index_reference: &Value,
    target_reference: &Value,
) -> Result<String, CdError> {
    let index_id = resolve_object_reference(dir, index_reference)?;
    let target_id = resolve_object_reference(dir, target_reference)?;
    if !dir.indices.contains_key(&index_id) {
        return Err(not_found(index_id));
    }
    if let Some(idx) = dir.indices.get_mut(&index_id) {
        if !idx.attached_objects.contains(&target_id) {
            idx.attached_objects.push(target_id.clone());
        }
    }
    let target_entry = dir.object_indices.entry(target_id.clone()).or_default();
    if !target_entry.contains(&index_id) {
        target_entry.push(index_id);
    }
    Ok(target_id)
}

fn core_detach_from_index(
    dir: &mut Directory,
    index_reference: &Value,
    target_reference: &Value,
) -> Result<String, CdError> {
    let index_id = resolve_object_reference(dir, index_reference)?;
    let target_id = resolve_object_reference(dir, target_reference)?;
    if let Some(idx) = dir.indices.get_mut(&index_id) {
        if let Some(pos) = idx.attached_objects.iter().position(|t| *t == target_id) {
            idx.attached_objects.remove(pos);
        }
    }
    if let Some(indices) = dir.object_indices.get_mut(&target_id) {
        if let Some(pos) = indices.iter().position(|i| *i == index_id) {
            indices.remove(pos);
        }
    }
    Ok(target_id)
}

/// moto's index attachment shape (IndexedAttributes + ObjectIdentifier).
fn index_attachments(
    dir: &Directory,
    index_ids: &[String],
    attribute_list: Vec<Value>,
) -> Vec<Value> {
    index_ids
        .iter()
        .filter(|id| dir.indices.contains_key(*id))
        .map(|id| {
            json!({
                "IndexedAttributes": attribute_list.clone(),
                "ObjectIdentifier": id.clone(),
            })
        })
        .collect()
}

fn core_attach_typed_link(
    dir: &mut Directory,
    source_object_reference: &Value,
    target_object_reference: &Value,
    typed_link_facet: Value,
    attributes: Vec<Value>,
) -> Result<Value, CdError> {
    let source_id = resolve_object_reference(dir, source_object_reference)?;
    let target_id = resolve_object_reference(dir, target_object_reference)?;
    let instance = TypedLinkInstance {
        source_object_id: source_id,
        target_object_id: target_id,
        typed_link_facet,
        attributes: attributes.clone(),
    };
    let specifier = json!({
        "TypedLinkFacet": instance.typed_link_facet.clone(),
        "SourceObjectReference": {"Selector": format!("${}", instance.source_object_id)},
        "TargetObjectReference": {"Selector": format!("${}", instance.target_object_id)},
        "IdentityAttributeValues": attributes,
    });
    dir.typed_links.push(instance);
    Ok(specifier)
}

fn core_detach_typed_link(
    dir: &mut Directory,
    typed_link_specifier: &Value,
) -> Result<(), CdError> {
    let source_ref = typed_link_specifier
        .get("SourceObjectReference")
        .cloned()
        .unwrap_or(json!({}));
    let target_ref = typed_link_specifier
        .get("TargetObjectReference")
        .cloned()
        .unwrap_or(json!({}));
    let facet_name = typed_link_specifier
        .get("TypedLinkFacet")
        .and_then(|f| f.get("TypedLinkName"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let source_id = resolve_object_reference(dir, &source_ref)?;
    let target_id = resolve_object_reference(dir, &target_ref)?;
    dir.typed_links.retain(|tl| {
        !(tl.source_object_id == source_id
            && tl.target_object_id == target_id
            && tl
                .typed_link_facet
                .get("TypedLinkName")
                .and_then(Value::as_str)
                == Some(facet_name.as_str()))
    });
    Ok(())
}

fn core_get_link_attributes(
    dir: &Directory,
    typed_link_specifier: &Value,
    attribute_names: &[Value],
) -> Result<Vec<Value>, CdError> {
    let names: Vec<String> = attribute_names
        .iter()
        .map(|v| v.as_str().unwrap_or_default().to_string())
        .collect();
    let Some(i) = typed_link_position(dir, typed_link_specifier)? else {
        return Ok(Vec::new());
    };
    let tl = &dir.typed_links[i];
    if names.is_empty() {
        Ok(tl.attributes.clone())
    } else {
        Ok(tl
            .attributes
            .iter()
            .filter(|a| {
                let name = a
                    .get("AttributeName")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                names.iter().any(|n| n.as_str() == name)
            })
            .cloned()
            .collect())
    }
}

fn core_update_link_attributes(
    dir: &mut Directory,
    typed_link_specifier: &Value,
    updates: &[Value],
) -> Result<(), CdError> {
    let Some(i) = typed_link_position(dir, typed_link_specifier)? else {
        return Ok(());
    };
    let tl = &mut dir.typed_links[i];
    for update in updates {
        let action = update.get("AttributeAction").cloned().unwrap_or(json!({}));
        let attr_key = update.get("AttributeKey").cloned().unwrap_or(json!({}));
        let action_type = action
            .get("AttributeActionType")
            .and_then(Value::as_str)
            .unwrap_or("CREATE_OR_UPDATE");
        let attr_name = attr_key
            .get("Name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if action_type == "DELETE" && !attr_name.is_empty() {
            tl.attributes.retain(|a| {
                a.get("AttributeName").and_then(Value::as_str) != Some(attr_name.as_str())
            });
        } else {
            tl.attributes.retain(|a| {
                a.get("AttributeName").and_then(Value::as_str) != Some(attr_name.as_str())
            });
            tl.attributes.push(json!({
                "AttributeName": attr_name,
                "Value": action.get("AttributeUpdateValue").cloned().unwrap_or(json!({})),
            }));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Operation resolution: (method, requestUri) -> operation, per botocore's
// clouddirectory-2017-01-11 model (paths stored without the version prefix).
// ---------------------------------------------------------------------------

const RULES: &[(&str, &str, &str)] = &[
    ("PUT", "object/facets", "AddFacetToObject"),
    ("PUT", "schema/apply", "ApplySchema"),
    ("PUT", "object/attach", "AttachObject"),
    ("PUT", "policy/attach", "AttachPolicy"),
    ("PUT", "index/attach", "AttachToIndex"),
    ("PUT", "typedlink/attach", "AttachTypedLink"),
    ("POST", "batchread", "BatchRead"),
    ("PUT", "batchwrite", "BatchWrite"),
    ("PUT", "directory/create", "CreateDirectory"),
    ("PUT", "facet/create", "CreateFacet"),
    ("PUT", "index", "CreateIndex"),
    ("PUT", "object", "CreateObject"),
    ("PUT", "schema/create", "CreateSchema"),
    ("PUT", "typedlink/facet/create", "CreateTypedLinkFacet"),
    ("PUT", "directory", "DeleteDirectory"),
    ("PUT", "facet/delete", "DeleteFacet"),
    ("PUT", "object/delete", "DeleteObject"),
    ("PUT", "schema", "DeleteSchema"),
    ("PUT", "typedlink/facet/delete", "DeleteTypedLinkFacet"),
    ("PUT", "index/detach", "DetachFromIndex"),
    ("PUT", "object/detach", "DetachObject"),
    ("PUT", "policy/detach", "DetachPolicy"),
    ("PUT", "typedlink/detach", "DetachTypedLink"),
    ("PUT", "directory/disable", "DisableDirectory"),
    ("PUT", "directory/enable", "EnableDirectory"),
    ("POST", "schema/getappliedschema", "GetAppliedSchemaVersion"),
    ("POST", "directory/get", "GetDirectory"),
    ("POST", "facet", "GetFacet"),
    ("POST", "typedlink/attributes/get", "GetLinkAttributes"),
    ("POST", "object/attributes/get", "GetObjectAttributes"),
    ("POST", "object/information", "GetObjectInformation"),
    ("POST", "schema/json", "GetSchemaAsJson"),
    (
        "POST",
        "typedlink/facet/get",
        "GetTypedLinkFacetInformation",
    ),
    ("POST", "schema/applied", "ListAppliedSchemaArns"),
    ("POST", "object/indices", "ListAttachedIndices"),
    ("POST", "schema/development", "ListDevelopmentSchemaArns"),
    ("POST", "directory/list", "ListDirectories"),
    ("POST", "facet/attributes", "ListFacetAttributes"),
    ("POST", "facet/list", "ListFacetNames"),
    ("POST", "typedlink/incoming", "ListIncomingTypedLinks"),
    ("POST", "index/targets", "ListIndex"),
    ("POST", "schema/managed", "ListManagedSchemaArns"),
    ("POST", "object/attributes", "ListObjectAttributes"),
    ("POST", "object/children", "ListObjectChildren"),
    ("POST", "object/parentpaths", "ListObjectParentPaths"),
    ("POST", "object/parent", "ListObjectParents"),
    ("POST", "object/policy", "ListObjectPolicies"),
    ("POST", "typedlink/outgoing", "ListOutgoingTypedLinks"),
    ("POST", "policy/attachment", "ListPolicyAttachments"),
    ("POST", "schema/published", "ListPublishedSchemaArns"),
    ("POST", "tags", "ListTagsForResource"),
    (
        "POST",
        "typedlink/facet/attributes",
        "ListTypedLinkFacetAttributes",
    ),
    ("POST", "typedlink/facet/list", "ListTypedLinkFacetNames"),
    ("POST", "policy/lookup", "LookupPolicy"),
    ("PUT", "schema/publish", "PublishSchema"),
    ("PUT", "schema/json", "PutSchemaFromJson"),
    ("PUT", "object/facets/delete", "RemoveFacetFromObject"),
    ("PUT", "tags/add", "TagResource"),
    ("PUT", "tags/remove", "UntagResource"),
    ("PUT", "facet", "UpdateFacet"),
    (
        "POST",
        "typedlink/attributes/update",
        "UpdateLinkAttributes",
    ),
    ("PUT", "object/update", "UpdateObjectAttributes"),
    ("PUT", "schema/update", "UpdateSchema"),
    ("PUT", "typedlink/facet", "UpdateTypedLinkFacet"),
    ("PUT", "schema/upgradeapplied", "UpgradeAppliedSchema"),
    ("PUT", "schema/upgradepublished", "UpgradePublishedSchema"),
];

fn wire_operation(req: &Request) -> Option<&'static str> {
    let path = req.path.trim_start_matches('/').trim_end_matches('/');
    let tail = path.strip_prefix("amazonclouddirectory/2017-01-11/")?;
    RULES
        .iter()
        .find(|(method, uri, _)| *method == req.method && *uri == tail)
        .map(|(_, _, op)| *op)
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "clouddirectory" {
        return None;
    }
    let Some(op) = wire_operation(req) else {
        return Some(wire_not_implemented());
    };
    let body = crate::proto::parse_json_body(&req.body);
    let slot = state();
    let mut st = slot.write();
    let s = st.entry((req.account, req.region.clone())).or_default();
    Some(handle(s, op, &body, req))
}

/// Wire NotImplemented for paths that match no clouddirectory operation.
fn wire_not_implemented() -> Response {
    let body = json!({
        "__type": "NotImplemented",
        "message": "The request has not been implemented",
    });
    let mut resp = Response::json(501, serde_json::to_vec(&body).unwrap_or_default());
    resp.headers.push((
        "x-amzn-ErrorType".to_string(),
        "NotImplemented:".to_string(),
    ));
    resp
}

/// The `x-amz-data-partition` header carries the ARN input for most ops.
fn data_partition(req: &Request) -> String {
    req.headers
        .get("x-amz-data-partition")
        .cloned()
        .unwrap_or_default()
}

/// Resolve `p["ObjectReference"]` and return the target object.
fn resolve_object_at<'a>(dir: &'a Directory, p: &Value) -> Result<&'a DirectoryObject, CdError> {
    let reference = p.get("ObjectReference").cloned().unwrap_or(json!({}));
    let obj_id = resolve_object_reference(dir, &reference)?;
    Ok(&dir.objects[&obj_id])
}

/// Resolve `p["ObjectReference"]` inside a directory to an object id.
fn referenced_id(dir: &Directory, p: &Value) -> Result<String, CdError> {
    let reference = p.get("ObjectReference").cloned().unwrap_or(json!({}));
    resolve_object_reference(dir, &reference)
}

#[allow(clippy::too_many_lines)]
fn handle(st: &mut State, op: &str, p: &Value, req: &Request) -> Response {
    let region = req.region.clone();
    let account = req.account;
    let arn_header = data_partition(req);

    match op {
        // ---------------------------------------------------------------
        // Directory operations
        // ---------------------------------------------------------------
        "CreateDirectory" => {
            let name = param_str(p, "Name").unwrap_or_default().to_string();
            let dir = Directory::new(&region, account, name.clone(), &arn_header);
            let resp = json!({
                "DirectoryArn": dir.directory_arn,
                "Name": name,
                "ObjectIdentifier": dir.object_identifier,
                "AppliedSchemaArn": dir.schema_arn,
            });
            st.directories.insert(dir.directory_arn.clone(), dir);
            ok(&resp)
        }
        "DeleteDirectory" => match st.directories.remove(&arn_header) {
            Some(dir) => ok(&json!({"DirectoryArn": dir.directory_arn})),
            None => not_found(&arn_header).wire(),
        },
        "DisableDirectory" => match get_directory_mut(st, &arn_header) {
            Ok(dir) => {
                dir.state = "DISABLED";
                ok(&json!({"DirectoryArn": arn_header}))
            }
            Err(e) => e.wire(),
        },
        "EnableDirectory" => match get_directory_mut(st, &arn_header) {
            Ok(dir) => {
                dir.state = "ENABLED";
                ok(&json!({"DirectoryArn": arn_header}))
            }
            Err(e) => e.wire(),
        },
        "GetDirectory" => match get_directory(st, &arn_header) {
            Ok(dir) => ok(&json!({"Directory": dir.to_dict()})),
            // moto raises InvalidArnException here (not ResourceNotFound).
            Err(_) => invalid_arn(&arn_header).wire(),
        },
        "ListDirectories" => {
            let state_filter = param_str(p, "state");
            let items: Vec<Value> = st
                .directories
                .values()
                .filter(|d| state_filter.is_none_or(|state| d.state == state))
                .map(|d| d.to_dict())
                .collect();
            let (page, next) = paginate(
                &items,
                param_str(p, "NextToken"),
                param_i64(p, "MaxResults"),
            );
            ok(&json!({"Directories": page, "NextToken": next}))
        }

        // ---------------------------------------------------------------
        // Schema operations
        // ---------------------------------------------------------------
        "CreateSchema" => {
            let name = param_str(p, "Name").unwrap_or_default().to_string();
            let schema_arn = format!(
                "arn:{}:{region}:{account}:schema/development/{name}",
                partition(&region)
            );
            if !st.development.contains(&schema_arn) {
                st.development.push(schema_arn.clone());
            }
            get_schema_info_mut(st, &schema_arn).name = name;
            ok(&json!({"SchemaArn": schema_arn}))
        }
        "DeleteSchema" => {
            let dev_pos = st.development.iter().position(|a| *a == arn_header);
            let pub_pos = st.published.iter().position(|a| *a == arn_header);
            match (dev_pos, pub_pos) {
                (Some(i), _) => {
                    st.development.remove(i);
                    st.schema_data.remove(&arn_header);
                    ok(&json!({"SchemaArn": arn_header}))
                }
                (None, Some(i)) => {
                    st.published.remove(i);
                    st.schema_data.remove(&arn_header);
                    ok(&json!({"SchemaArn": arn_header}))
                }
                (None, None) => not_found(&arn_header).wire(),
            }
        }
        "PublishSchema" => {
            let dev_arn = arn_header;
            let name = param_str(p, "Name").unwrap_or_default();
            let version = param_str(p, "Version").unwrap_or("None");
            let minor_version = param_str(p, "MinorVersion").unwrap_or("None");
            let pub_arn = format!(
                "arn:{}:{region}:{account}:schema/published/{name}/{version}/{minor_version}",
                partition(&region),
            );
            if st.published.contains(&dev_arn) {
                return schema_already_published(dev_arn).wire();
            }
            match st.development.iter().position(|a| *a == dev_arn) {
                Some(i) => {
                    st.development.remove(i);
                }
                None => return not_found(dev_arn).wire(),
            }
            st.published.push(pub_arn.clone());
            copy_schema_data(st, &dev_arn, &pub_arn);
            ok(&json!({"PublishedSchemaArn": pub_arn}))
        }
        "ApplySchema" => {
            let dir_arn = arn_header;
            let published = param_str(p, "PublishedSchemaArn")
                .unwrap_or_default()
                .to_string();
            // moto checks the directory first, then the published schema.
            if let Err(e) = get_directory(st, &dir_arn) {
                return e.wire();
            }
            if !st.published.contains(&published) {
                return not_found(&published).wire();
            }
            let applied = published.replace("/published/", "/applied/");
            if !st.applied.contains(&applied) {
                st.applied.push(applied.clone());
            }
            let dir = st.directories.get_mut(&dir_arn).unwrap();
            dir.schema_arn = published.clone();
            if !dir.applied_schemas.contains(&applied) {
                dir.applied_schemas.push(applied.clone());
            }
            copy_schema_data(st, &published, &applied);
            ok(&json!({"AppliedSchemaArn": published, "DirectoryArn": dir_arn}))
        }
        "UpdateSchema" => {
            if let Err(e) = find_schema_arn(st, &arn_header) {
                return e.wire();
            }
            let name = param_str(p, "Name").unwrap_or_default().to_string();
            get_schema_info_mut(st, &arn_header).name = name;
            ok(&json!({"SchemaArn": arn_header}))
        }
        "GetSchemaAsJson" => {
            if let Err(e) = find_schema_arn(st, &arn_header) {
                return e.wire();
            }
            let info = get_schema_info_mut(st, &arn_header);
            ok(&json!({"Name": info.name.clone(), "Document": info.document.clone()}))
        }
        "PutSchemaFromJson" => {
            if let Err(e) = find_schema_arn(st, &arn_header) {
                return e.wire();
            }
            let document = param_str(p, "Document").unwrap_or_default().to_string();
            // moto swallows malformed JSON; well-formed "facets" entries
            // become facets.
            if let Ok(doc) = serde_json::from_str::<Value>(&document) {
                if let Some(facets) = doc.get("facets").and_then(Value::as_object) {
                    for (fname, fdata) in facets {
                        let info = get_schema_info_mut(st, &arn_header);
                        info.facets.insert(
                            fname.clone(),
                            Facet {
                                name: fname.clone(),
                                attributes: fdata
                                    .get("attributes")
                                    .and_then(Value::as_array)
                                    .cloned()
                                    .unwrap_or_default(),
                                object_type: fdata
                                    .get("objectType")
                                    .and_then(Value::as_str)
                                    .unwrap_or("NODE")
                                    .to_string(),
                                facet_style: fdata
                                    .get("facetStyle")
                                    .and_then(Value::as_str)
                                    .unwrap_or("STATIC")
                                    .to_string(),
                            },
                        );
                    }
                }
            }
            get_schema_info_mut(st, &arn_header).document = document;
            ok(&json!({"Arn": arn_header}))
        }
        "ListDevelopmentSchemaArns" => {
            let items: Vec<Value> = st.development.iter().cloned().map(Value::String).collect();
            let (page, next) = paginate(
                &items,
                param_str(p, "NextToken"),
                param_i64(p, "MaxResults"),
            );
            ok(&json!({"SchemaArns": page, "NextToken": next}))
        }
        "ListPublishedSchemaArns" => {
            let items: Vec<Value> = st.published.iter().cloned().map(Value::String).collect();
            let (page, next) = paginate(
                &items,
                param_str(p, "NextToken"),
                param_i64(p, "MaxResults"),
            );
            ok(&json!({"SchemaArns": page, "NextToken": next}))
        }
        "ListAppliedSchemaArns" => {
            let dir_arn = param_str(p, "DirectoryArn").unwrap_or_default().to_string();
            let list = match get_directory(st, &dir_arn) {
                Ok(d) => d.applied_schemas.clone(),
                Err(e) => return e.wire(),
            };
            let items: Vec<Value> = list.into_iter().map(Value::String).collect();
            let (page, next) = paginate(
                &items,
                param_str(p, "NextToken"),
                param_i64(p, "MaxResults"),
            );
            ok(&json!({"SchemaArns": page, "NextToken": next}))
        }
        "ListManagedSchemaArns" => {
            // moto: managed schema ARNs are always an empty list.
            let items: Vec<Value> = Vec::new();
            let (page, next) = paginate(
                &items,
                param_str(p, "NextToken"),
                param_i64(p, "MaxResults"),
            );
            ok(&json!({"SchemaArns": page, "NextToken": next}))
        }
        "GetAppliedSchemaVersion" => {
            let schema_arn = param_str(p, "SchemaArn").unwrap_or_default().to_string();
            if st.applied.contains(&schema_arn) {
                return ok(&json!({"AppliedSchemaArn": schema_arn}));
            }
            let prefix = schema_arn
                .rsplit_once('/')
                .map_or(schema_arn.as_str(), |(a, _)| a)
                .to_string();
            for arn in st.applied.iter() {
                if arn.contains(&schema_arn) || arn.starts_with(&prefix) {
                    return ok(&json!({"AppliedSchemaArn": arn.clone()}));
                }
            }
            not_found(&schema_arn).wire()
        }
        "UpgradeAppliedSchema" => {
            let published = param_str(p, "PublishedSchemaArn")
                .unwrap_or_default()
                .to_string();
            let dir_arn = param_str(p, "DirectoryArn").unwrap_or_default().to_string();
            let dry_run = p.get("DryRun").and_then(Value::as_bool).unwrap_or(false);
            // moto: directory lookup first, then the published-schema check.
            if let Err(e) = get_directory(st, &dir_arn) {
                return e.wire();
            }
            if !st.published.contains(&published) {
                return not_found(&published).wire();
            }
            let applied = published.replace("/published/", "/applied/");
            if !dry_run {
                if !st.applied.contains(&applied) {
                    st.applied.push(applied.clone());
                }
                let dir = st.directories.get_mut(&dir_arn).unwrap();
                if !dir.applied_schemas.contains(&applied) {
                    dir.applied_schemas.push(applied.clone());
                }
                copy_schema_data(st, &published, &applied);
            }
            ok(&json!({"UpgradedSchemaArn": applied, "DirectoryArn": dir_arn}))
        }
        "UpgradePublishedSchema" => {
            let dev_arn = param_str(p, "DevelopmentSchemaArn")
                .unwrap_or_default()
                .to_string();
            let pub_arn = param_str(p, "PublishedSchemaArn")
                .unwrap_or_default()
                .to_string();
            let dry_run = p.get("DryRun").and_then(Value::as_bool).unwrap_or(false);
            if !st.development.contains(&dev_arn) {
                return not_found(&dev_arn).wire();
            }
            if !st.published.contains(&pub_arn) {
                return not_found(&pub_arn).wire();
            }
            if !dry_run {
                copy_schema_data(st, &dev_arn, &pub_arn);
            }
            ok(&json!({"UpgradedSchemaArn": pub_arn}))
        }

        // ---------------------------------------------------------------
        // Facet operations
        // ---------------------------------------------------------------
        "CreateFacet" => {
            if let Err(e) = find_schema_arn(st, &arn_header) {
                return e.wire();
            }
            let name = param_str(p, "Name").unwrap_or_default().to_string();
            let attributes = param_list(p, "Attributes");
            let object_type = param_str_or(p, "ObjectType", "NODE");
            let facet_style = param_str_or(p, "FacetStyle", "STATIC");
            get_schema_info_mut(st, &arn_header).facets.insert(
                name.clone(),
                Facet {
                    name,
                    attributes,
                    object_type,
                    facet_style,
                },
            );
            ok(&json!({}))
        }
        "DeleteFacet" => {
            let name = param_str(p, "Name").unwrap_or_default().to_string();
            if let Err(e) = find_schema_arn(st, &arn_header) {
                return e.wire();
            }
            match get_schema_info_mut(st, &arn_header).facets.remove(&name) {
                Some(_) => ok(&json!({})),
                None => not_found(name).wire(),
            }
        }
        "GetFacet" => {
            let name = param_str(p, "Name").unwrap_or_default().to_string();
            if let Err(e) = find_schema_arn(st, &arn_header) {
                return e.wire();
            }
            match get_schema_info_mut(st, &arn_header).facets.get(&name) {
                Some(f) => ok(&json!({"Facet": f.to_dict()})),
                None => not_found(name).wire(),
            }
        }
        "UpdateFacet" => {
            let name = param_str(p, "Name").unwrap_or_default().to_string();
            if let Err(e) = find_schema_arn(st, &arn_header) {
                return e.wire();
            }
            let Some(facet) = get_schema_info_mut(st, &arn_header).facets.get_mut(&name) else {
                return not_found(name).wire();
            };
            if let Some(object_type) = param_str(p, "ObjectType") {
                facet.object_type = object_type.to_string();
            }
            if p.get("AttributeUpdates")
                .and_then(Value::as_array)
                .is_some_and(|a| !a.is_empty())
            {
                if let Some(updates) = p.get("AttributeUpdates") {
                    apply_facet_attribute_updates(&mut facet.attributes, updates);
                }
            }
            ok(&json!({}))
        }
        "ListFacetNames" => {
            if let Err(e) = find_schema_arn(st, &arn_header) {
                return e.wire();
            }
            let items: Vec<Value> = get_schema_info_mut(st, &arn_header)
                .facets
                .keys()
                .cloned()
                .map(Value::String)
                .collect();
            let (page, next) = paginate(
                &items,
                param_str(p, "NextToken"),
                param_i64(p, "MaxResults"),
            );
            ok(&json!({"FacetNames": page, "NextToken": next}))
        }
        "ListFacetAttributes" => {
            let name = param_str(p, "Name").unwrap_or_default().to_string();
            if let Err(e) = find_schema_arn(st, &arn_header) {
                return e.wire();
            }
            let attrs = match get_schema_info_mut(st, &arn_header).facets.get(&name) {
                Some(f) => f.attributes.clone(),
                None => return not_found(name).wire(),
            };
            let (page, next) = paginate(
                &attrs,
                param_str(p, "NextToken"),
                param_i64(p, "MaxResults"),
            );
            ok(&json!({"Attributes": page, "NextToken": next}))
        }

        // ---------------------------------------------------------------
        // TypedLink facet operations
        // ---------------------------------------------------------------
        "CreateTypedLinkFacet" => {
            if let Err(e) = find_schema_arn(st, &arn_header) {
                return e.wire();
            }
            let facet = p.get("Facet").cloned().unwrap_or(json!({}));
            let name = facet
                .get("Name")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            get_schema_info_mut(st, &arn_header)
                .typed_link_facets
                .insert(
                    name.clone(),
                    TypedLinkFacet {
                        attributes: facet
                            .get("Attributes")
                            .and_then(Value::as_array)
                            .cloned()
                            .unwrap_or_default(),
                        identity_attribute_order: facet
                            .get("IdentityAttributeOrder")
                            .and_then(Value::as_array)
                            .map(|a| {
                                a.iter()
                                    .map(|v| v.as_str().unwrap_or_default().to_string())
                                    .collect()
                            })
                            .unwrap_or_default(),
                    },
                );
            ok(&json!({}))
        }
        "DeleteTypedLinkFacet" => {
            let name = param_str(p, "Name").unwrap_or_default().to_string();
            if let Err(e) = find_schema_arn(st, &arn_header) {
                return e.wire();
            }
            match get_schema_info_mut(st, &arn_header)
                .typed_link_facets
                .remove(&name)
            {
                Some(_) => ok(&json!({})),
                None => not_found(name).wire(),
            }
        }
        "GetTypedLinkFacetInformation" => {
            let name = param_str(p, "Name").unwrap_or_default().to_string();
            if let Err(e) = find_schema_arn(st, &arn_header) {
                return e.wire();
            }
            match get_schema_info_mut(st, &arn_header)
                .typed_link_facets
                .get(&name)
            {
                Some(f) => ok(&json!({"IdentityAttributeOrder": f.identity_attribute_order})),
                None => not_found(name).wire(),
            }
        }
        "UpdateTypedLinkFacet" => {
            let name = param_str(p, "Name").unwrap_or_default().to_string();
            if let Err(e) = find_schema_arn(st, &arn_header) {
                return e.wire();
            }
            let Some(facet) = get_schema_info_mut(st, &arn_header)
                .typed_link_facets
                .get_mut(&name)
            else {
                return not_found(name).wire();
            };
            if let Some(order) = p.get("IdentityAttributeOrder").and_then(Value::as_array) {
                facet.identity_attribute_order = order
                    .iter()
                    .map(|v| v.as_str().unwrap_or_default().to_string())
                    .collect();
            }
            if p.get("AttributeUpdates")
                .and_then(Value::as_array)
                .is_some_and(|a| !a.is_empty())
            {
                if let Some(updates) = p.get("AttributeUpdates") {
                    apply_facet_attribute_updates(&mut facet.attributes, updates);
                }
            }
            ok(&json!({}))
        }
        "ListTypedLinkFacetNames" => {
            if let Err(e) = find_schema_arn(st, &arn_header) {
                return e.wire();
            }
            let items: Vec<Value> = get_schema_info_mut(st, &arn_header)
                .typed_link_facets
                .keys()
                .cloned()
                .map(Value::String)
                .collect();
            let (page, next) = paginate(
                &items,
                param_str(p, "NextToken"),
                param_i64(p, "MaxResults"),
            );
            ok(&json!({"FacetNames": page, "NextToken": next}))
        }
        "ListTypedLinkFacetAttributes" => {
            let name = param_str(p, "Name").unwrap_or_default().to_string();
            if let Err(e) = find_schema_arn(st, &arn_header) {
                return e.wire();
            }
            let attrs = match get_schema_info_mut(st, &arn_header)
                .typed_link_facets
                .get(&name)
            {
                Some(f) => f.attributes.clone(),
                None => return not_found(name).wire(),
            };
            let (page, next) = paginate(
                &attrs,
                param_str(p, "NextToken"),
                param_i64(p, "MaxResults"),
            );
            ok(&json!({"Attributes": page, "NextToken": next}))
        }
        _ => directory_ops(st, op, p, req),
    }
}

/// Directory-scoped operations: objects, links, policies, indices, typed
/// links, tags and the batch work queues. The DirectoryArn arrives via the
/// `x-amz-data-partition` header.
#[allow(clippy::too_many_lines)]
fn directory_ops(st: &mut State, op: &str, p: &Value, req: &Request) -> Response {
    let dir_arn = data_partition(req);

    match op {
        // ---------------------------------------------------------------
        // Object operations
        // ---------------------------------------------------------------
        "CreateObject" => {
            let dir = match get_directory_mut(st, &dir_arn) {
                Ok(d) => d,
                Err(e) => return e.wire(),
            };
            let obj_id = core_create_object(
                dir,
                param_list(p, "SchemaFacets"),
                p.get("ObjectAttributeList")
                    .and_then(Value::as_array)
                    .cloned(),
                p.get("ParentReference"),
                param_str(p, "LinkName"),
            );
            ok(&json!({"ObjectIdentifier": obj_id}))
        }
        "DeleteObject" => {
            let dir = match get_directory_mut(st, &dir_arn) {
                Ok(d) => d,
                Err(e) => return e.wire(),
            };
            let obj_ref = p.get("ObjectReference").cloned().unwrap_or(json!({}));
            match core_delete_object(dir, &obj_ref) {
                Ok(()) => ok(&json!({})),
                Err(e) => e.wire(),
            }
        }
        "GetObjectInformation" => {
            let dir = match get_directory(st, &dir_arn) {
                Ok(d) => d,
                Err(e) => return e.wire(),
            };
            match resolve_object_at(dir, p) {
                Ok(obj) => ok(&json!({
                    "SchemaFacets": obj.schema_facets.clone(),
                    "ObjectIdentifier": obj.object_identifier.clone(),
                })),
                Err(e) => e.wire(),
            }
        }
        "GetObjectAttributes" => {
            let dir = match get_directory(st, &dir_arn) {
                Ok(d) => d,
                Err(e) => return e.wire(),
            };
            match resolve_object_at(dir, p) {
                Ok(obj) => {
                    let names: Vec<String> = param_list(p, "AttributeNames")
                        .iter()
                        .map(|v| v.as_str().unwrap_or_default().to_string())
                        .collect();
                    let attrs: Vec<Value> = obj
                        .attributes
                        .iter()
                        .filter(|a| attr_key_name_matches(a, &names))
                        .cloned()
                        .collect();
                    ok(&json!({"Attributes": attrs}))
                }
                Err(e) => e.wire(),
            }
        }
        "UpdateObjectAttributes" => {
            let dir = match get_directory_mut(st, &dir_arn) {
                Ok(d) => d,
                Err(e) => return e.wire(),
            };
            let obj_id = match referenced_id(dir, p) {
                Ok(id) => id,
                Err(e) => return e.wire(),
            };
            let attribute_updates = p.get("AttributeUpdates").cloned().unwrap_or(json!([]));
            let obj = dir.objects.get_mut(&obj_id).unwrap();
            for update in attribute_updates.as_array().cloned().unwrap_or_default() {
                let action = update
                    .get("ObjectAttributeAction")
                    .cloned()
                    .unwrap_or(json!({}));
                let attr_key = update
                    .get("ObjectAttributeKey")
                    .cloned()
                    .unwrap_or(json!({}));
                let action_type = action
                    .get("ObjectAttributeActionType")
                    .and_then(Value::as_str)
                    .unwrap_or("CREATE_OR_UPDATE");
                let attr_name = attr_key
                    .get("Name")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                if action_type == "DELETE" && !attr_name.is_empty() {
                    obj.attributes
                        .retain(|a| !attr_key_name_matches(a, std::slice::from_ref(&attr_name)));
                } else {
                    obj.attributes
                        .retain(|a| !attr_key_name_matches(a, std::slice::from_ref(&attr_name)));
                    obj.attributes.push(json!({
                        "Key": attr_key,
                        "Value": action
                            .get("ObjectAttributeUpdateValue")
                            .cloned()
                            .unwrap_or(json!({})),
                    }));
                }
            }
            ok(&json!({"ObjectIdentifier": obj_id}))
        }
        "AddFacetToObject" => {
            let dir = match get_directory_mut(st, &dir_arn) {
                Ok(d) => d,
                Err(e) => return e.wire(),
            };
            let obj_id = match referenced_id(dir, p) {
                Ok(id) => id,
                Err(e) => return e.wire(),
            };
            let obj = dir.objects.get_mut(&obj_id).unwrap();
            if let Some(facet) = p.get("SchemaFacet") {
                obj.schema_facets.push(facet.clone());
            }
            if let Some(attrs) = p.get("ObjectAttributeList").and_then(Value::as_array) {
                obj.attributes.extend(attrs.iter().cloned());
            }
            ok(&json!({}))
        }
        "RemoveFacetFromObject" => {
            let dir = match get_directory_mut(st, &dir_arn) {
                Ok(d) => d,
                Err(e) => return e.wire(),
            };
            let obj_id = match referenced_id(dir, p) {
                Ok(id) => id,
                Err(e) => return e.wire(),
            };
            let facet_name = p
                .get("SchemaFacet")
                .and_then(|f| f.get("FacetName"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let facet_schema = p
                .get("SchemaFacet")
                .and_then(|f| f.get("SchemaArn"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let obj = dir.objects.get_mut(&obj_id).unwrap();
            obj.schema_facets.retain(|sf| {
                !(sf.get("FacetName").and_then(Value::as_str) == Some(facet_name.as_str())
                    && sf.get("SchemaArn").and_then(Value::as_str) == Some(facet_schema.as_str()))
            });
            ok(&json!({}))
        }
        "ListObjectAttributes" => {
            let dir = match get_directory(st, &dir_arn) {
                Ok(d) => d,
                Err(e) => return e.wire(),
            };
            let obj = match resolve_object_at(dir, p) {
                Ok(o) => o,
                Err(e) => return e.wire(),
            };
            let attrs = obj.attributes.clone();
            let (page, next) = paginate(
                &attrs,
                param_str(p, "NextToken"),
                param_i64(p, "MaxResults"),
            );
            ok(&json!({"Attributes": page, "NextToken": next}))
        }
        "ListObjectChildren" => {
            let dir = match get_directory(st, &dir_arn) {
                Ok(d) => d,
                Err(e) => return e.wire(),
            };
            let obj_id = match referenced_id(dir, p) {
                Ok(id) => id,
                Err(e) => return e.wire(),
            };
            let children: Vec<(Value, Value)> = dir
                .children
                .get(&obj_id)
                .map(|c| {
                    c.iter()
                        .map(|(link, child)| (json!(link.clone()), json!(child.clone())))
                        .collect()
                })
                .unwrap_or_default();
            let (page, next) = paginate(
                &children,
                param_str(p, "NextToken"),
                param_i64(p, "MaxResults"),
            );
            ok(&json!({"Children": pairs_to_map(page), "NextToken": next}))
        }
        "ListObjectParents" => {
            let dir = match get_directory(st, &dir_arn) {
                Ok(d) => d,
                Err(e) => return e.wire(),
            };
            let obj_id = match referenced_id(dir, p) {
                Ok(id) => id,
                Err(e) => return e.wire(),
            };
            let parents: Vec<(Value, Value)> = dir
                .parents
                .get(&obj_id)
                .map(|c| {
                    c.iter()
                        .map(|(parent, link)| (json!(parent.clone()), json!(link.clone())))
                        .collect()
                })
                .unwrap_or_default();
            let (page, next) = paginate(
                &parents,
                param_str(p, "NextToken"),
                param_i64(p, "MaxResults"),
            );
            ok(&json!({"Parents": pairs_to_map(page), "NextToken": next}))
        }
        "ListObjectParentPaths" => {
            let dir = match get_directory(st, &dir_arn) {
                Ok(d) => d,
                Err(e) => return e.wire(),
            };
            let obj_id = match referenced_id(dir, p) {
                Ok(id) => id,
                Err(e) => return e.wire(),
            };
            let mut paths = Vec::new();
            collect_paths(dir, &obj_id, &[], &mut paths);
            let (page, next) = paginate(
                &paths,
                param_str(p, "NextToken"),
                param_i64(p, "MaxResults"),
            );
            ok(&json!({"PathToObjectIdentifiersList": page, "NextToken": next}))
        }
        "ListObjectPolicies" => {
            let dir = match get_directory(st, &dir_arn) {
                Ok(d) => d,
                Err(e) => return e.wire(),
            };
            let obj_id = match referenced_id(dir, p) {
                Ok(id) => id,
                Err(e) => return e.wire(),
            };
            let policies: Vec<Value> = dir
                .object_policies
                .get(&obj_id)
                .map(|v| v.iter().cloned().map(Value::String).collect())
                .unwrap_or_default();
            let (page, next) = paginate(
                &policies,
                param_str(p, "NextToken"),
                param_i64(p, "MaxResults"),
            );
            ok(&json!({"AttachedPolicyIds": page, "NextToken": next}))
        }

        // ---------------------------------------------------------------
        // Attach / detach object links
        // ---------------------------------------------------------------
        "AttachObject" => {
            let dir = match get_directory_mut(st, &dir_arn) {
                Ok(d) => d,
                Err(e) => return e.wire(),
            };
            match core_attach_object(
                dir,
                &p.get("ParentReference").cloned().unwrap_or(json!({})),
                &p.get("ChildReference").cloned().unwrap_or(json!({})),
                param_str(p, "LinkName").unwrap_or_default(),
            ) {
                Ok(child_id) => ok(&json!({"AttachedObjectIdentifier": child_id})),
                Err(e) => e.wire(),
            }
        }
        "DetachObject" => {
            let dir = match get_directory_mut(st, &dir_arn) {
                Ok(d) => d,
                Err(e) => return e.wire(),
            };
            match core_detach_object(
                dir,
                &p.get("ParentReference").cloned().unwrap_or(json!({})),
                param_str(p, "LinkName").unwrap_or_default(),
            ) {
                Ok(child_id) => ok(&json!({"DetachedObjectIdentifier": child_id})),
                Err(e) => e.wire(),
            }
        }

        // ---------------------------------------------------------------
        // Policy operations
        // ---------------------------------------------------------------
        "AttachPolicy" => {
            let dir = match get_directory_mut(st, &dir_arn) {
                Ok(d) => d,
                Err(e) => return e.wire(),
            };
            match core_attach_policy(
                dir,
                &p.get("PolicyReference").cloned().unwrap_or(json!({})),
                &p.get("ObjectReference").cloned().unwrap_or(json!({})),
            ) {
                Ok(()) => ok(&json!({})),
                Err(e) => e.wire(),
            }
        }
        "DetachPolicy" => {
            let dir = match get_directory_mut(st, &dir_arn) {
                Ok(d) => d,
                Err(e) => return e.wire(),
            };
            match core_detach_policy(
                dir,
                &p.get("PolicyReference").cloned().unwrap_or(json!({})),
                &p.get("ObjectReference").cloned().unwrap_or(json!({})),
            ) {
                Ok(()) => ok(&json!({})),
                Err(e) => e.wire(),
            }
        }
        "ListPolicyAttachments" => {
            let dir = match get_directory(st, &dir_arn) {
                Ok(d) => d,
                Err(e) => return e.wire(),
            };
            let policy_id = match resolve_object_reference(
                dir,
                &p.get("PolicyReference").cloned().unwrap_or(json!({})),
            ) {
                Ok(id) => id,
                Err(e) => return e.wire(),
            };
            let items: Vec<Value> = dir
                .policy_attachments
                .get(&policy_id)
                .map(|v| v.iter().cloned().map(Value::String).collect())
                .unwrap_or_default();
            let (page, next) = paginate(
                &items,
                param_str(p, "NextToken"),
                param_i64(p, "MaxResults"),
            );
            ok(&json!({"ObjectIdentifiers": page, "NextToken": next}))
        }
        "LookupPolicy" => {
            let dir = match get_directory(st, &dir_arn) {
                Ok(d) => d,
                Err(e) => return e.wire(),
            };
            let obj_id = match referenced_id(dir, p) {
                Ok(id) => id,
                Err(e) => return e.wire(),
            };
            let result: Vec<Value> = dir
                .object_policies
                .get(&obj_id)
                .map(|policies| {
                    policies
                        .iter()
                        .map(|policy_id| {
                            json!({
                                "PolicyId": policy_id.clone(),
                                "PolicyType": "POLICY",
                                "ObjectIdentifier": obj_id.clone(),
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();
            ok(&json!({"PolicyToPathList": result, "NextToken": Value::Null}))
        }

        // ---------------------------------------------------------------
        // Index operations
        // ---------------------------------------------------------------
        "CreateIndex" => {
            let dir = match get_directory_mut(st, &dir_arn) {
                Ok(d) => d,
                Err(e) => return e.wire(),
            };
            let obj_id = core_create_index(
                dir,
                param_list(p, "OrderedIndexedAttributeList"),
                p.get("IsUnique").and_then(Value::as_bool).unwrap_or(false),
                p.get("ParentReference"),
                param_str(p, "LinkName"),
            );
            ok(&json!({"ObjectIdentifier": obj_id}))
        }
        "AttachToIndex" => {
            let dir = match get_directory_mut(st, &dir_arn) {
                Ok(d) => d,
                Err(e) => return e.wire(),
            };
            match core_attach_to_index(
                dir,
                &p.get("IndexReference").cloned().unwrap_or(json!({})),
                &p.get("TargetReference").cloned().unwrap_or(json!({})),
            ) {
                Ok(target_id) => ok(&json!({"AttachedObjectIdentifier": target_id})),
                Err(e) => e.wire(),
            }
        }
        "DetachFromIndex" => {
            let dir = match get_directory_mut(st, &dir_arn) {
                Ok(d) => d,
                Err(e) => return e.wire(),
            };
            match core_detach_from_index(
                dir,
                &p.get("IndexReference").cloned().unwrap_or(json!({})),
                &p.get("TargetReference").cloned().unwrap_or(json!({})),
            ) {
                Ok(target_id) => ok(&json!({"DetachedObjectIdentifier": target_id})),
                Err(e) => e.wire(),
            }
        }
        "ListIndex" => {
            let dir = match get_directory(st, &dir_arn) {
                Ok(d) => d,
                Err(e) => return e.wire(),
            };
            let index_id = match resolve_object_reference(
                dir,
                &p.get("IndexReference").cloned().unwrap_or(json!({})),
            ) {
                Ok(id) => id,
                Err(e) => return e.wire(),
            };
            let Some(idx) = dir.indices.get(&index_id) else {
                return not_found(index_id).wire();
            };
            let items: Vec<Value> = idx
                .attached_objects
                .iter()
                .map(|oid| {
                    json!({
                        "IndexedAttributes": idx.ordered_indexed_attribute_list.clone(),
                        "ObjectIdentifier": oid.clone(),
                    })
                })
                .collect();
            let (page, next) = paginate(
                &items,
                param_str(p, "NextToken"),
                param_i64(p, "MaxResults"),
            );
            ok(&json!({"IndexAttachments": page, "NextToken": next}))
        }
        "ListAttachedIndices" => {
            let dir = match get_directory(st, &dir_arn) {
                Ok(d) => d,
                Err(e) => return e.wire(),
            };
            let target_id = match resolve_object_reference(
                dir,
                &p.get("TargetReference").cloned().unwrap_or(json!({})),
            ) {
                Ok(id) => id,
                Err(e) => return e.wire(),
            };
            let items: Vec<Value> = dir
                .object_indices
                .get(&target_id)
                .map(|index_ids| index_attachments(dir, index_ids, Vec::new()))
                .unwrap_or_default();
            let (page, next) = paginate(
                &items,
                param_str(p, "NextToken"),
                param_i64(p, "MaxResults"),
            );
            ok(&json!({"IndexAttachments": page, "NextToken": next}))
        }

        // ---------------------------------------------------------------
        // Typed-link instance operations
        // ---------------------------------------------------------------
        "AttachTypedLink" => {
            let dir = match get_directory_mut(st, &dir_arn) {
                Ok(d) => d,
                Err(e) => return e.wire(),
            };
            match core_attach_typed_link(
                dir,
                &p.get("SourceObjectReference").cloned().unwrap_or(json!({})),
                &p.get("TargetObjectReference").cloned().unwrap_or(json!({})),
                p.get("TypedLinkFacet").cloned().unwrap_or(json!({})),
                param_list(p, "Attributes"),
            ) {
                Ok(specifier) => ok(&json!({"TypedLinkSpecifier": specifier})),
                Err(e) => e.wire(),
            }
        }
        "DetachTypedLink" => {
            let dir = match get_directory_mut(st, &dir_arn) {
                Ok(d) => d,
                Err(e) => return e.wire(),
            };
            match core_detach_typed_link(
                dir,
                &p.get("TypedLinkSpecifier").cloned().unwrap_or(json!({})),
            ) {
                Ok(()) => ok(&json!({})),
                Err(e) => e.wire(),
            }
        }
        "GetLinkAttributes" => {
            let dir = match get_directory(st, &dir_arn) {
                Ok(d) => d,
                Err(e) => return e.wire(),
            };
            match core_get_link_attributes(
                dir,
                &p.get("TypedLinkSpecifier").cloned().unwrap_or(json!({})),
                &param_list(p, "AttributeNames"),
            ) {
                Ok(attrs) => ok(&json!({"Attributes": attrs})),
                Err(e) => e.wire(),
            }
        }
        "UpdateLinkAttributes" => {
            let dir = match get_directory_mut(st, &dir_arn) {
                Ok(d) => d,
                Err(e) => return e.wire(),
            };
            match core_update_link_attributes(
                dir,
                &p.get("TypedLinkSpecifier").cloned().unwrap_or(json!({})),
                &param_list(p, "AttributeUpdates"),
            ) {
                Ok(()) => ok(&json!({})),
                Err(e) => e.wire(),
            }
        }
        "ListIncomingTypedLinks" => {
            let dir = match get_directory(st, &dir_arn) {
                Ok(d) => d,
                Err(e) => return e.wire(),
            };
            let obj_id = match referenced_id(dir, p) {
                Ok(id) => id,
                Err(e) => return e.wire(),
            };
            let items: Vec<Value> = dir
                .typed_links
                .iter()
                .filter(|tl| tl.target_object_id == obj_id)
                .map(link_specifier)
                .collect();
            let (page, next) = paginate(
                &items,
                param_str(p, "NextToken"),
                param_i64(p, "MaxResults"),
            );
            ok(&json!({"LinkSpecifiers": page, "NextToken": next}))
        }
        "ListOutgoingTypedLinks" => {
            let dir = match get_directory(st, &dir_arn) {
                Ok(d) => d,
                Err(e) => return e.wire(),
            };
            let obj_id = match referenced_id(dir, p) {
                Ok(id) => id,
                Err(e) => return e.wire(),
            };
            let items: Vec<Value> = dir
                .typed_links
                .iter()
                .filter(|tl| tl.source_object_id == obj_id)
                .map(link_specifier)
                .collect();
            let (page, next) = paginate(
                &items,
                param_str(p, "NextToken"),
                param_i64(p, "MaxResults"),
            );
            ok(&json!({"TypedLinkSpecifiers": page, "NextToken": next}))
        }

        // ---------------------------------------------------------------
        // Tag operations
        // ---------------------------------------------------------------
        "TagResource" => {
            let resource_arn = param_str(p, "ResourceArn").unwrap_or_default().to_string();
            let tags = param_list(p, "Tags");
            st.tag_resource(&resource_arn, &tags);
            ok(&json!({}))
        }
        "UntagResource" => {
            let resource_arn = param_str(p, "ResourceArn").unwrap_or_default().to_string();
            let keys = match p.get("TagKeys") {
                Some(Value::Array(a)) => a.clone(),
                Some(other) => vec![other.clone()],
                None => Vec::new(),
            };
            st.untag_resource(&resource_arn, &keys);
            ok(&json!({}))
        }
        "ListTagsForResource" => {
            let resource_arn = param_str(p, "ResourceArn").unwrap_or_default().to_string();
            ok(&json!({"Tags": st.list_tags(&resource_arn), "NextToken": Value::Null}))
        }

        // ---------------------------------------------------------------
        // Batch operations
        // ---------------------------------------------------------------
        "BatchRead" => {
            let ops = param_list(p, "Operations");
            let dir = match get_directory(st, &dir_arn) {
                Ok(d) => d,
                Err(e) => return e.wire(),
            };
            let responses: Vec<Value> = ops
                .iter()
                .map(|op| match batch_read_op(dir, op) {
                    Ok(inner) => json!({"SuccessfulResponse": inner}),
                    Err(e) => e.batch_frame(),
                })
                .collect();
            ok(&json!({"Responses": responses}))
        }
        "BatchWrite" => {
            let ops = param_list(p, "Operations");
            let dir = match get_directory_mut(st, &dir_arn) {
                Ok(d) => d,
                Err(e) => return e.wire(),
            };
            let mut responses = Vec::new();
            for op in &ops {
                let frame = match batch_write_op(dir, op) {
                    Ok(inner) => json!({"SuccessfulResponse": inner}),
                    Err(e) => e.batch_frame(),
                };
                responses.push(frame);
            }
            ok(&json!({"Responses": responses}))
        }
        _ => unreachable!("wire_operation only returns RULES entries"),
    }
}

/// True when an attribute entry's `Key.Name` appears in `names`.
fn attr_key_name_matches(attr: &Value, names: &[String]) -> bool {
    attr.get("Key")
        .and_then(|k| k.get("Name"))
        .and_then(Value::as_str)
        .map(|name| names.contains(&name.to_string()))
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Batch work queues (moto's _execute_batch_read_op / _execute_batch_write_op)
// ---------------------------------------------------------------------------

fn batch_read_op(dir: &Directory, op: &Value) -> Result<Value, CdError> {
    if let Some(params) = op.get("ListObjectAttributes") {
        let reference = params.get("ObjectReference").cloned().unwrap_or(json!({}));
        let obj_id = resolve_object_reference(dir, &reference)?;
        let attrs = dir
            .objects
            .get(&obj_id)
            .map(|o| o.attributes.clone())
            .unwrap_or_default();
        return Ok(json!({"ListObjectAttributes": {"Attributes": attrs}}));
    }
    if let Some(params) = op.get("ListObjectChildren") {
        let reference = params.get("ObjectReference").cloned().unwrap_or(json!({}));
        let obj_id = resolve_object_reference(dir, &reference)?;
        let children = dir.children.get(&obj_id).cloned().unwrap_or_default();
        return Ok(json!({"ListObjectChildren": {"Children": children}}));
    }
    if let Some(params) = op.get("GetObjectInformation") {
        let reference = params.get("ObjectReference").cloned().unwrap_or(json!({}));
        let obj_id = resolve_object_reference(dir, &reference)?;
        let obj = &dir.objects[&obj_id];
        return Ok(json!({
            "GetObjectInformation": {
                "SchemaFacets": obj.schema_facets.clone(),
                "ObjectIdentifier": obj.object_identifier.clone(),
            }
        }));
    }
    if let Some(params) = op.get("GetObjectAttributes") {
        let reference = params.get("ObjectReference").cloned().unwrap_or(json!({}));
        let names: Vec<String> = params
            .get("AttributeNames")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .map(|v| v.as_str().unwrap_or_default().to_string())
                    .collect()
            })
            .unwrap_or_default();
        let obj_id = resolve_object_reference(dir, &reference)?;
        let attrs: Vec<Value> = dir.objects[&obj_id]
            .attributes
            .iter()
            .filter(|a| attr_key_name_matches(a, &names))
            .cloned()
            .collect();
        return Ok(json!({"GetObjectAttributes": {"Attributes": attrs}}));
    }
    if let Some(params) = op.get("ListObjectParents") {
        let reference = params.get("ObjectReference").cloned().unwrap_or(json!({}));
        let obj_id = resolve_object_reference(dir, &reference)?;
        let parents = dir.parents.get(&obj_id).cloned().unwrap_or_default();
        return Ok(json!({"ListObjectParents": {"Parents": parents}}));
    }
    if let Some(params) = op.get("ListObjectPolicies") {
        let reference = params.get("ObjectReference").cloned().unwrap_or(json!({}));
        let obj_id = resolve_object_reference(dir, &reference)?;
        let policies = dir
            .object_policies
            .get(&obj_id)
            .cloned()
            .unwrap_or_default();
        return Ok(json!({"ListObjectPolicies": {"AttachedPolicyIds": policies}}));
    }
    if let Some(params) = op.get("ListIncomingTypedLinks") {
        let reference = params.get("ObjectReference").cloned().unwrap_or(json!({}));
        let obj_id = resolve_object_reference(dir, &reference)?;
        let links: Vec<Value> = dir
            .typed_links
            .iter()
            .filter(|tl| tl.target_object_id == obj_id)
            .map(link_specifier)
            .collect();
        return Ok(json!({"ListIncomingTypedLinks": {"LinkSpecifiers": links}}));
    }
    if let Some(params) = op.get("ListOutgoingTypedLinks") {
        let reference = params.get("ObjectReference").cloned().unwrap_or(json!({}));
        let obj_id = resolve_object_reference(dir, &reference)?;
        let links: Vec<Value> = dir
            .typed_links
            .iter()
            .filter(|tl| tl.source_object_id == obj_id)
            .map(link_specifier)
            .collect();
        return Ok(json!({"ListOutgoingTypedLinks": {"TypedLinkSpecifiers": links}}));
    }
    if let Some(params) = op.get("ListIndex") {
        let index_ref = params.get("IndexReference").cloned().unwrap_or(json!({}));
        let index_id = resolve_object_reference(dir, &index_ref)?;
        let attachments: Vec<Value> = match dir.indices.get(&index_id) {
            Some(idx) => idx
                .attached_objects
                .iter()
                .map(|oid| {
                    json!({
                        "IndexedAttributes": idx.ordered_indexed_attribute_list.clone(),
                        "ObjectIdentifier": oid.clone(),
                    })
                })
                .collect(),
            None => Vec::new(),
        };
        return Ok(json!({"ListIndex": {"IndexAttachments": attachments}}));
    }
    if let Some(params) = op.get("ListPolicyAttachments") {
        let policy_ref = params.get("PolicyReference").cloned().unwrap_or(json!({}));
        let policy_id = resolve_object_reference(dir, &policy_ref)?;
        let items = dir
            .policy_attachments
            .get(&policy_id)
            .cloned()
            .unwrap_or_default();
        return Ok(json!({"ListPolicyAttachments": {"ObjectIdentifiers": items}}));
    }
    if let Some(params) = op.get("LookupPolicy") {
        let reference = params.get("ObjectReference").cloned().unwrap_or(json!({}));
        let obj_id = resolve_object_reference(dir, &reference)?;
        let result: Vec<Value> = dir
            .object_policies
            .get(&obj_id)
            .map(|policies| {
                policies
                    .iter()
                    .map(|policy_id| {
                        json!({
                            "PolicyId": policy_id.clone(),
                            "PolicyType": "POLICY",
                            "ObjectIdentifier": obj_id.clone(),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        return Ok(json!({"LookupPolicy": {"PolicyToPathList": result}}));
    }
    if let Some(params) = op.get("ListObjectParentPaths") {
        let reference = params.get("ObjectReference").cloned().unwrap_or(json!({}));
        let obj_id = resolve_object_reference(dir, &reference)?;
        let mut paths = Vec::new();
        collect_paths(dir, &obj_id, &[], &mut paths);
        return Ok(json!({
            "ListObjectParentPaths": {"PathToObjectIdentifiersList": paths}
        }));
    }
    if let Some(params) = op.get("GetLinkAttributes") {
        let specifier = params
            .get("TypedLinkSpecifier")
            .cloned()
            .unwrap_or(json!({}));
        let names: Vec<String> = params
            .get("AttributeNames")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .map(|v| v.as_str().unwrap_or_default().to_string())
                    .collect()
            })
            .unwrap_or_default();
        let attrs: Vec<Value> = match typed_link_position(dir, &specifier)? {
            Some(i) => {
                let tl = &dir.typed_links[i];
                if names.is_empty() {
                    tl.attributes.clone()
                } else {
                    tl.attributes
                        .iter()
                        .filter(|a| {
                            let name = a
                                .get("AttributeName")
                                .and_then(Value::as_str)
                                .unwrap_or_default();
                            names.contains(&name.to_string())
                        })
                        .cloned()
                        .collect()
                }
            }
            None => Vec::new(),
        };
        return Ok(json!({"GetLinkAttributes": {"Attributes": attrs}}));
    }
    if let Some(params) = op.get("ListAttachedIndices") {
        let target_ref = params.get("TargetReference").cloned().unwrap_or(json!({}));
        let target_id = resolve_object_reference(dir, &target_ref)?;
        let index_ids = dir
            .object_indices
            .get(&target_id)
            .cloned()
            .unwrap_or_default();
        let attachments = index_attachments(dir, &index_ids, Vec::new());
        return Ok(json!({"ListAttachedIndices": {"IndexAttachments": attachments}}));
    }
    Err(CdError {
        code: "NotImplemented",
        body: json!({"Message": "Batch read operation not supported"}),
    })
}

#[allow(clippy::too_many_lines)]
fn batch_write_op(dir: &mut Directory, op: &Value) -> Result<Value, CdError> {
    if let Some(params) = op.get("CreateObject") {
        // moto's batch path reads the singular "SchemaFacet" quirk key.
        let obj_id = core_create_object(
            dir,
            params
                .get("SchemaFacet")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default(),
            params
                .get("ObjectAttributeList")
                .and_then(Value::as_array)
                .cloned(),
            params.get("ParentReference"),
            param_str(params, "LinkName"),
        );
        return Ok(json!({"CreateObject": {"ObjectIdentifier": obj_id}}));
    }
    if let Some(params) = op.get("AttachObject") {
        let child_id = core_attach_object(
            dir,
            &params.get("ParentReference").cloned().unwrap_or(json!({})),
            &params.get("ChildReference").cloned().unwrap_or(json!({})),
            param_str(params, "LinkName").unwrap_or_default(),
        )?;
        return Ok(json!({"AttachObject": {"AttachedObjectIdentifier": child_id}}));
    }
    if let Some(params) = op.get("DetachObject") {
        let child_id = core_detach_object(
            dir,
            &params.get("ParentReference").cloned().unwrap_or(json!({})),
            param_str(params, "LinkName").unwrap_or_default(),
        )?;
        return Ok(json!({"DetachObject": {"DetachedObjectIdentifier": child_id}}));
    }
    if let Some(params) = op.get("UpdateObjectAttributes") {
        let reference = params.get("ObjectReference").cloned().unwrap_or(json!({}));
        let obj_id = resolve_object_reference(dir, &reference)?;
        let updates = params
            .get("AttributeUpdates")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        {
            let obj = dir.objects.get_mut(&obj_id).unwrap();
            for update in updates {
                let action = update
                    .get("ObjectAttributeAction")
                    .cloned()
                    .unwrap_or(json!({}));
                let attr_key = update
                    .get("ObjectAttributeKey")
                    .cloned()
                    .unwrap_or(json!({}));
                let action_type = action
                    .get("ObjectAttributeActionType")
                    .and_then(Value::as_str)
                    .unwrap_or("CREATE_OR_UPDATE");
                let attr_name = attr_key
                    .get("Name")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                if action_type == "DELETE" && !attr_name.is_empty() {
                    obj.attributes.retain(|a| {
                        a.get("Key")
                            .and_then(|k| k.get("Name"))
                            .and_then(Value::as_str)
                            != Some(attr_name.as_str())
                    });
                } else {
                    obj.attributes.retain(|a| {
                        a.get("Key")
                            .and_then(|k| k.get("Name"))
                            .and_then(Value::as_str)
                            != Some(attr_name.as_str())
                    });
                    obj.attributes.push(json!({
                        "Key": attr_key,
                        "Value": action
                            .get("ObjectAttributeUpdateValue")
                            .cloned()
                            .unwrap_or(json!({})),
                    }));
                }
            }
        }
        return Ok(json!({"UpdateObjectAttributes": {"ObjectIdentifier": obj_id}}));
    }
    if let Some(params) = op.get("DeleteObject") {
        let reference = params.get("ObjectReference").cloned().unwrap_or(json!({}));
        let obj_id = resolve_object_reference(dir, &reference)?;
        if let Some(parents) = dir.parents.get(&obj_id) {
            for parent_id in parents.keys() {
                if let Some(children) = dir.children.get_mut(parent_id) {
                    children.retain(|_, child| *child != obj_id);
                }
            }
        }
        dir.parents.remove(&obj_id);
        dir.children.remove(&obj_id);
        dir.objects.remove(&obj_id);
        return Ok(json!({"DeleteObject": {}}));
    }
    if let Some(params) = op.get("AddFacetToObject") {
        let reference = params.get("ObjectReference").cloned().unwrap_or(json!({}));
        let obj_id = resolve_object_reference(dir, &reference)?;
        let obj = dir.objects.get_mut(&obj_id).unwrap();
        if let Some(facet) = params.get("SchemaFacet") {
            obj.schema_facets.push(facet.clone());
        }
        if let Some(attrs) = params.get("ObjectAttributeList").and_then(Value::as_array) {
            obj.attributes.extend(attrs.iter().cloned());
        }
        return Ok(json!({"AddFacetToObject": {}}));
    }
    if let Some(params) = op.get("RemoveFacetFromObject") {
        let reference = params.get("ObjectReference").cloned().unwrap_or(json!({}));
        let obj_id = resolve_object_reference(dir, &reference)?;
        let facet_name = params
            .get("SchemaFacet")
            .and_then(|f| f.get("FacetName"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let facet_schema = params
            .get("SchemaFacet")
            .and_then(|f| f.get("SchemaArn"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let obj = dir.objects.get_mut(&obj_id).unwrap();
        obj.schema_facets.retain(|sf| {
            !(sf.get("FacetName").and_then(Value::as_str) == Some(facet_name.as_str())
                && sf.get("SchemaArn").and_then(Value::as_str) == Some(facet_schema.as_str()))
        });
        return Ok(json!({"RemoveFacetFromObject": {}}));
    }
    if let Some(params) = op.get("AttachPolicy") {
        core_attach_policy(
            dir,
            &params.get("PolicyReference").cloned().unwrap_or(json!({})),
            &params.get("ObjectReference").cloned().unwrap_or(json!({})),
        )?;
        return Ok(json!({"AttachPolicy": {}}));
    }
    if let Some(params) = op.get("DetachPolicy") {
        core_detach_policy(
            dir,
            &params.get("PolicyReference").cloned().unwrap_or(json!({})),
            &params.get("ObjectReference").cloned().unwrap_or(json!({})),
        )?;
        return Ok(json!({"DetachPolicy": {}}));
    }
    if let Some(params) = op.get("CreateIndex") {
        let obj_id = core_create_index(
            dir,
            params
                .get("OrderedIndexedAttributeList")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default(),
            params
                .get("IsUnique")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            params.get("ParentReference"),
            param_str(params, "LinkName"),
        );
        return Ok(json!({"CreateIndex": {"ObjectIdentifier": obj_id}}));
    }
    if let Some(params) = op.get("AttachToIndex") {
        let target_id = core_attach_to_index(
            dir,
            &params.get("IndexReference").cloned().unwrap_or(json!({})),
            &params.get("TargetReference").cloned().unwrap_or(json!({})),
        )?;
        return Ok(json!({"AttachToIndex": {"AttachedObjectIdentifier": target_id}}));
    }
    if let Some(params) = op.get("DetachFromIndex") {
        let target_id = core_detach_from_index(
            dir,
            &params.get("IndexReference").cloned().unwrap_or(json!({})),
            &params.get("TargetReference").cloned().unwrap_or(json!({})),
        )?;
        return Ok(json!({"DetachFromIndex": {"DetachedObjectIdentifier": target_id}}));
    }
    if let Some(params) = op.get("AttachTypedLink") {
        let specifier = core_attach_typed_link(
            dir,
            &params
                .get("SourceObjectReference")
                .cloned()
                .unwrap_or(json!({})),
            &params
                .get("TargetObjectReference")
                .cloned()
                .unwrap_or(json!({})),
            params.get("TypedLinkFacet").cloned().unwrap_or(json!({})),
            params
                .get("Attributes")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default(),
        )?;
        return Ok(json!({"AttachTypedLink": {"TypedLinkSpecifier": specifier}}));
    }
    if let Some(params) = op.get("DetachTypedLink") {
        core_detach_typed_link(
            dir,
            &params
                .get("TypedLinkSpecifier")
                .cloned()
                .unwrap_or(json!({})),
        )?;
        return Ok(json!({"DetachTypedLink": {}}));
    }
    if let Some(params) = op.get("UpdateLinkAttributes") {
        core_update_link_attributes(
            dir,
            &params
                .get("TypedLinkSpecifier")
                .cloned()
                .unwrap_or(json!({})),
            &params
                .get("AttributeUpdates")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default(),
        )?;
        return Ok(json!({"UpdateLinkAttributes": {}}));
    }
    Err(CdError {
        code: "NotImplemented",
        body: json!({"Message": "Batch write operation not supported"}),
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = "/amazonclouddirectory/2017-01-11";

    fn req(method: &str, path: &str, body: &Value, arn: Option<&str>) -> Request {
        let mut headers = HashMap::new();
        if let Some(arn) = arn {
            headers.insert("x-amz-data-partition".to_string(), arn.to_string());
        }
        Request {
            service: "clouddirectory".into(),
            operation: String::new(),
            method: method.into(),
            path: format!("{BASE}{path}"),
            query_string: String::new(),
            headers,
            body: bytes::Bytes::from(serde_json::to_vec(body).unwrap()),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    fn dispatch_ok(method: &str, path: &str, body: &Value, arn: Option<&str>) -> Value {
        let resp = dispatch(&req(method, path, body, arn)).unwrap();
        assert_eq!(
            resp.status,
            200,
            "expected 200, got {:?}",
            String::from_utf8_lossy(&resp.body)
        );
        serde_json::from_slice(&resp.body).unwrap()
    }

    /// Create + publish a fresh schema; returns (dev_arn, pub_arn).
    /// (Publishing removes the dev ARN from the development list, like moto.)
    fn published(name: &str) -> (String, String) {
        let resp = dispatch_ok("PUT", "/schema/create", &json!({"Name": name}), None);
        let dev_arn = resp["SchemaArn"].as_str().unwrap().to_string();
        let resp = dispatch_ok(
            "PUT",
            "/schema/publish",
            &json!({"Version": "v1"}),
            Some(&dev_arn),
        );
        let pub_arn = resp["PublishedSchemaArn"].as_str().unwrap().to_string();
        (dev_arn, pub_arn)
    }

    #[test]
    fn schema_lifecycle() {
        let resp = dispatch_ok("PUT", "/schema/create", &json!({"Name": "lifec"}), None);
        let arn = resp["SchemaArn"].as_str().unwrap().to_string();
        assert!(arn.contains(":schema/development/lifec"));

        let resp = dispatch_ok("POST", "/schema/development", &json!({}), None);
        assert!(resp["SchemaArns"]
            .as_array()
            .unwrap()
            .iter()
            .any(|a| *a == json!(arn)));

        let resp = dispatch_ok("PUT", "/schema", &json!({}), Some(&arn));
        assert_eq!(resp["SchemaArn"], json!(arn));

        let resp = dispatch_ok("POST", "/schema/development", &json!({}), None);
        assert!(resp["SchemaArns"]
            .as_array()
            .unwrap()
            .iter()
            .all(|a| *a != json!(arn)));
    }

    #[test]
    fn directory_and_object_roundtrip() {
        let (_, pub_arn) = published("roundtrip");
        let resp = dispatch_ok(
            "PUT",
            "/directory/create",
            &json!({"Name": "dir-a"}),
            Some(&pub_arn),
        );
        let dir_arn = resp["DirectoryArn"].as_str().unwrap().to_string();
        assert!(resp["AppliedSchemaArn"].is_string());

        // Root object info through the "/" selector.
        let resp = dispatch_ok(
            "POST",
            "/object/information",
            &json!({"ObjectReference": {"Selector": "/"}}),
            Some(&dir_arn),
        );
        assert!(resp["ObjectIdentifier"].is_string());

        let resp = dispatch_ok(
            "PUT",
            "/object",
            &json!({"SchemaFacets": []}),
            Some(&dir_arn),
        );
        let child = resp["ObjectIdentifier"].as_str().unwrap().to_string();

        let resp = dispatch_ok(
            "PUT",
            "/object/attach",
            &json!({
                "ParentReference": {"Selector": "/"},
                "ChildReference": {"Selector": format!("${child}")},
                "LinkName": "l1",
            }),
            Some(&dir_arn),
        );
        assert_eq!(resp["AttachedObjectIdentifier"], json!(child));

        let resp = dispatch_ok(
            "POST",
            "/object/children",
            &json!({"ObjectReference": {"Selector": "/"}}),
            Some(&dir_arn),
        );
        assert_eq!(resp["Children"]["l1"], json!(child));

        let resp = dispatch_ok(
            "PUT",
            "/object/detach",
            &json!({
                "ParentReference": {"Selector": "/"},
                "LinkName": "l1",
            }),
            Some(&dir_arn),
        );
        assert_eq!(resp["DetachedObjectIdentifier"], json!(child));

        dispatch_ok(
            "PUT",
            "/object/delete",
            &json!({"ObjectReference": {"Selector": format!("${child}")}}),
            Some(&dir_arn),
        );
    }

    #[test]
    fn get_directory_invalid_arn_shape() {
        let resp = dispatch(&req("POST", "/directory/get", &json!({}), Some("arn:nada"))).unwrap();
        assert_eq!(resp.status, 400);
        let errtype = resp
            .headers
            .iter()
            .find(|(k, _)| k == "x-amzn-ErrorType")
            .map(|(_, v)| v.clone())
            .unwrap();
        assert!(errtype.starts_with("InvalidArnException"));
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Message"], json!("Invalid Arn"));
    }

    #[test]
    fn facet_roundtrip() {
        // Facet ops target a development schema (publishing the dev ARN
        // would detach it, like moto).
        let resp = dispatch_ok(
            "PUT",
            "/schema/create",
            &json!({"Name": "facet-host"}),
            None,
        );
        let dev_arn = resp["SchemaArn"].as_str().unwrap().to_string();
        dispatch_ok(
            "PUT",
            "/facet/create",
            &json!({
                "Name": "FacetX",
                "Attributes": [
                    {"Name": "attr1", "AttributeDefinition": {"Type": "STRING"}},
                ],
                "ObjectType": "NODE",
            }),
            Some(&dev_arn),
        );
        let resp = dispatch_ok("POST", "/facet", &json!({"Name": "FacetX"}), Some(&dev_arn));
        assert_eq!(resp["Facet"]["Name"], json!("FacetX"));
        assert_eq!(resp["Facet"]["ObjectType"], json!("NODE"));
        assert_eq!(resp["Facet"]["FacetStyle"], json!("STATIC"));

        let resp = dispatch_ok(
            "POST",
            "/facet/attributes",
            &json!({"Name": "FacetX"}),
            Some(&dev_arn),
        );
        let names: Vec<&str> = resp["Attributes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a["Name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"attr1"));

        dispatch_ok(
            "PUT",
            "/facet/delete",
            &json!({"Name": "FacetX"}),
            Some(&dev_arn),
        );
        let resp = dispatch_ok("POST", "/facet/list", &json!({}), Some(&dev_arn));
        assert!(resp["FacetNames"]
            .as_array()
            .unwrap()
            .iter()
            .all(|n| *n != json!("FacetX")));
    }

    #[test]
    fn typed_link_facet_roundtrip() {
        // Typed-link facet ops target a development schema too.
        let resp = dispatch_ok(
            "PUT",
            "/schema/create",
            &json!({"Name": "tlink-host"}),
            None,
        );
        let dev_arn = resp["SchemaArn"].as_str().unwrap().to_string();
        dispatch_ok(
            "PUT",
            "/typedlink/facet/create",
            &json!({"Facet": {
                "Name": "TLX",
                "Attributes": [
                    {"Name": "linkattr", "Type": "STRING", "RequiredBehavior": "REQUIRED_ALWAYS"},
                ],
                "IdentityAttributeOrder": ["linkattr"],
            }}),
            Some(&dev_arn),
        );
        let resp = dispatch_ok(
            "POST",
            "/typedlink/facet/get",
            &json!({"Name": "TLX"}),
            Some(&dev_arn),
        );
        assert_eq!(resp["IdentityAttributeOrder"], json!(["linkattr"]));

        let resp = dispatch_ok(
            "POST",
            "/typedlink/facet/attributes",
            &json!({"Name": "TLX"}),
            Some(&dev_arn),
        );
        assert_eq!(resp["Attributes"][0]["Name"], json!("linkattr"));

        dispatch_ok(
            "PUT",
            "/typedlink/facet/delete",
            &json!({"Name": "TLX"}),
            Some(&dev_arn),
        );
        let resp = dispatch_ok("POST", "/typedlink/facet/list", &json!({}), Some(&dev_arn));
        assert!(resp["FacetNames"]
            .as_array()
            .unwrap()
            .iter()
            .all(|n| *n != json!("TLX")));
    }

    #[test]
    fn tag_roundtrip() {
        let resp = dispatch_ok("PUT", "/schema/create", &json!({"Name": "taggable"}), None);
        let arn = resp["SchemaArn"].as_str().unwrap().to_string();

        dispatch_ok(
            "PUT",
            "/tags/add",
            &json!({"ResourceArn": arn, "Tags": [{"Key": "env", "Value": "test"}]}),
            None,
        );
        let resp = dispatch_ok("POST", "/tags", &json!({"ResourceArn": arn}), None);
        assert_eq!(resp["Tags"][0]["Key"], json!("env"));
        assert_eq!(resp["Tags"][0]["Value"], json!("test"));

        dispatch_ok(
            "PUT",
            "/tags/remove",
            &json!({"ResourceArn": arn, "TagKeys": ["env"]}),
            None,
        );
        let resp = dispatch_ok("POST", "/tags", &json!({"ResourceArn": arn}), None);
        assert!(resp["Tags"].as_array().unwrap().is_empty());
    }

    #[test]
    fn batch_read_and_write() {
        let (_, pub_arn) = published("batched");
        let resp = dispatch_ok(
            "PUT",
            "/directory/create",
            &json!({"Name": "batch-dir"}),
            Some(&pub_arn),
        );
        let dir_arn = resp["DirectoryArn"].as_str().unwrap().to_string();

        let resp = dispatch_ok(
            "POST",
            "/batchread",
            &json!({"Operations": [
                {"ListObjectChildren": {"ObjectReference": {"Selector": "/"}}},
            ]}),
            Some(&dir_arn),
        );
        assert_eq!(resp["Responses"].as_array().unwrap().len(), 1);
        assert!(resp["Responses"][0].get("SuccessfulResponse").is_some());

        let resp = dispatch_ok(
            "PUT",
            "/batchwrite",
            &json!({"Operations": []}),
            Some(&dir_arn),
        );
        assert!(resp["Responses"].as_array().unwrap().is_empty());
    }

    #[test]
    fn unknown_path_is_not_implemented() {
        let resp = dispatch(&req("PUT", "/nonexistent/op", &json!({}), None)).unwrap();
        assert_eq!(resp.status, 501);
    }
}
