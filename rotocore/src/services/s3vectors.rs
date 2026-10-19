//! S3 Vectors backend (REST-JSON; one flat POST path per operation).
//!
//! Port of moto's s3vectors backend — the deployed moto fork's surface:
//! vector-bucket and index CRUD, bucket policies and vector
//! put/get/list/delete. Wire bodies mirror moto's shape-driven serializer
//! exactly: fields the moto models do not carry (`creationTime`, index
//! `encryptionConfiguration`/`metadataConfiguration`) stay omitted, the
//! default bucket encryption is `{"sseType": "AES256"}`, and the vector
//! payload echoes `key`/`data`/`metadata` per the `returnData` /
//! `returnMetadata` flags.
//!
//! `TagResource`, `UntagResource`, `ListTagsForResource` and `QueryVectors`
//! have no route or handler in the deployed moto fork (`urls.py`), so they
//! are out of moto's surface: for them (and any other path) `dispatch`
//! returns `None` and the gateway answers the generic 501.

use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Value};
use std::collections::HashMap;

#[derive(Default)]
struct State {
    /// Creation-ordered buckets; moto keys them by bucket ARN.
    buckets: Vec<Bucket>,
}

struct Bucket {
    name: String,
    arn: String,
    /// Botocore `encryptionConfiguration` body; moto defaults `{"sseType": "AES256"}`.
    encryption: Value,
    policy: Option<String>,
    /// Creation-ordered indexes (replaced in place on re-create), mirroring
    /// moto's index dict keyed by index ARN.
    indexes: Vec<IndexRec>,
}

struct IndexRec {
    bucket_name: String,
    name: String,
    arn: String,
    data_type: String,
    dimension: i64,
    distance_metric: String,
    /// Insertion-ordered (key, vector-body) pairs, mirroring moto's vectors dict.
    vectors: Vec<(String, Value)>,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("s3vectors"));
    SLOT.clone()
}

#[derive(Clone, Copy)]
enum Op {
    CreateVectorBucket,
    DeleteVectorBucket,
    GetVectorBucket,
    ListVectorBuckets,
    CreateIndex,
    DeleteIndex,
    GetIndex,
    ListIndexes,
    PutVectorBucketPolicy,
    GetVectorBucketPolicy,
    DeleteVectorBucketPolicy,
    PutVectors,
    GetVectors,
    ListVectors,
    DeleteVectors,
}

static ROUTED_BY_PATH: Lazy<HashMap<&'static str, Op>> = Lazy::new(|| {
    let mut m = HashMap::new();
    for (path, op) in [
        ("/CreateVectorBucket", Op::CreateVectorBucket),
        ("/DeleteVectorBucket", Op::DeleteVectorBucket),
        ("/GetVectorBucket", Op::GetVectorBucket),
        ("/ListVectorBuckets", Op::ListVectorBuckets),
        ("/CreateIndex", Op::CreateIndex),
        ("/DeleteIndex", Op::DeleteIndex),
        ("/GetIndex", Op::GetIndex),
        ("/ListIndexes", Op::ListIndexes),
        ("/PutVectorBucketPolicy", Op::PutVectorBucketPolicy),
        ("/GetVectorBucketPolicy", Op::GetVectorBucketPolicy),
        ("/DeleteVectorBucketPolicy", Op::DeleteVectorBucketPolicy),
        ("/PutVectors", Op::PutVectors),
        ("/GetVectors", Op::GetVectors),
        ("/ListVectors", Op::ListVectors),
        ("/DeleteVectors", Op::DeleteVectors),
    ] {
        m.insert(path, op);
    }
    m
});

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "s3vectors" {
        return None;
    }
    // Every routed operation is a POST to its own flat path; tag endpoints
    // (`/tags/{resourceArn}`) and QueryVectors are unrouted in the deployed
    // moto fork — falling through returns None and the gateway's 501.
    let op = *ROUTED_BY_PATH.get(req.path.as_str())?;
    if req.method != "POST" {
        return None;
    }
    let body = crate::proto::parse_json_body(&req.body);
    let slot = state();
    let mut st = slot.write();
    let b = st.entry((req.account, req.region.clone())).or_default();
    Some(handle(op, req.region.as_str(), req.account, b, &body))
}

fn empty_ok() -> Response {
    Response::json(200, br#"{}"#.to_vec())
}

fn json_ok(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

fn validation(msg: impl Into<String>) -> Response {
    crate::proto::json_error(400, "ValidationException", msg.into().as_str())
}

fn conflict(msg: &str) -> Response {
    crate::proto::json_error(409, "ConflictException", msg)
}

fn bucket_not_found() -> Response {
    crate::proto::json_error(
        404,
        "NotFoundException",
        "The specified vector bucket could not be found",
    )
}

fn index_not_found() -> Response {
    crate::proto::json_error(
        404,
        "NotFoundException",
        "The specified index could not be found",
    )
}

fn policy_not_found() -> Response {
    crate::proto::json_error(
        404,
        "NotFoundException",
        "The specified vector bucket policy could not be found",
    )
}

fn handle(op: Op, region: &str, account: u64, b: &mut State, p: &Value) -> Response {
    match op {
        Op::CreateVectorBucket => create_vector_bucket(b, region, account, p),
        Op::DeleteVectorBucket => delete_vector_bucket(b, p),
        Op::GetVectorBucket => get_vector_bucket(b, p),
        Op::ListVectorBuckets => list_vector_buckets(b, p),
        Op::CreateIndex => create_index(b, p),
        Op::DeleteIndex => delete_index(b, p),
        Op::GetIndex => get_index(b, p),
        Op::ListIndexes => list_indexes(b, p),
        Op::PutVectorBucketPolicy => put_vector_bucket_policy(b, p),
        Op::GetVectorBucketPolicy => get_vector_bucket_policy(b, p),
        Op::DeleteVectorBucketPolicy => delete_vector_bucket_policy(b, p),
        Op::PutVectors => put_vectors(b, p),
        Op::GetVectors => get_vectors(b, p),
        Op::ListVectors => list_vectors(b, p),
        Op::DeleteVectors => delete_vectors(b, p),
    }
}

impl State {
    /// moto `get_vector_bucket`: a matching name wins, then ARN.
    fn bucket_pos(&self, name: &str, arn: &str) -> Option<usize> {
        if !name.is_empty() {
            if let Some(pos) = self.buckets.iter().position(|b| b.name == name) {
                return Some(pos);
            }
        }
        if !arn.is_empty() {
            if let Some(pos) = self.buckets.iter().position(|b| b.arn == arn) {
                return Some(pos);
            }
        }
        None
    }

    /// Parse an index ARN (`...:bucket/<name>/index/<name>`), mirroring
    /// moto's `parse_arn(index_arn).resource_id.split("/")` unpack.
    fn arn_names(index_arn: &str) -> Option<(String, String)> {
        let resource = index_arn.splitn(6, ':').nth(5)?;
        let rest = resource.strip_prefix("bucket/")?;
        let mut parts = rest.splitn(3, '/');
        let bucket = parts.next()?;
        let mid = parts.next()?;
        let index = parts.next()?;
        if mid != "index" || bucket.is_empty() || index.is_empty() {
            return None;
        }
        Some((bucket.to_string(), index.to_string()))
    }

    /// moto `get_index`: an ARN overrides name params; a missing bucket
    /// collapses to `IndexNotFound` like moto's swallowed bucket miss.
    fn index_entry(
        &self,
        bucket_name: &str,
        index_name: &str,
        index_arn: &str,
    ) -> Option<(usize, usize)> {
        let (bname, iname) = if index_arn.is_empty() {
            (bucket_name.to_string(), index_name.to_string())
        } else {
            Self::arn_names(index_arn)?
        };
        let bpos = self.buckets.iter().position(|b| b.name == bname)?;
        let ipos = self.buckets[bpos]
            .indexes
            .iter()
            .position(|ix| ix.name == iname)?;
        Some((bpos, ipos))
    }

    /// moto `_validate_index_params`: either `indexArn` alone or the full
    /// (vectorBucketName, indexName) pair; returns `Some(err)` on violation.
    fn validate_index_params(&self, p: &Value) -> Option<Response> {
        let s = |k: &str| p.get(k).and_then(Value::as_str).unwrap_or("");
        let (bucket_name, index_name, index_arn) =
            (s("vectorBucketName"), s("indexName"), s("indexArn"));
        let valid = (!bucket_name.is_empty() && !index_name.is_empty() && index_arn.is_empty())
            || (!index_arn.is_empty() && bucket_name.is_empty() && index_name.is_empty());
        if valid {
            None
        } else {
            Some(validation(
                "Must specify either indexArn or both vectorBucketName and indexName",
            ))
        }
    }
}

fn create_vector_bucket(b: &mut State, region: &str, account: u64, p: &Value) -> Response {
    let name = p
        .get("vectorBucketName")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    // moto: 2 < len(name) < 64, reported with the exact AWS wording.
    if !(3..64).contains(&name.chars().count()) {
        return validation(format!(
            "1 validation error detected. Value with length {} at '/vectorBucketName' failed to satisfy constraint: Member must have length between 3 and 63, inclusive",
            name.chars().count()
        ));
    }
    // moto's obvious-invalid list it could not verify an official regex for.
    if name.contains('&') || name.contains('_') {
        return validation("Invalid vector bucket name");
    }
    let arn = format!("arn:aws:s3vectors:{region}:{account}:bucket/{name}");
    if b.buckets.iter().any(|x| x.arn == arn) {
        return conflict("A vector bucket with the specified name already exists");
    }
    // moto: `encryption_configuration or {"sseType": "AES256"}` — an empty
    // (falsy) or absent body yields the default.
    let encryption = match p.get("encryptionConfiguration") {
        Some(v) if v.as_object().map(|o| !o.is_empty()).unwrap_or(false) => v.clone(),
        _ => json!({"sseType": "AES256"}),
    };
    b.buckets.push(Bucket {
        name,
        arn,
        encryption,
        policy: None,
        indexes: Vec::new(),
    });
    empty_ok()
}

fn delete_vector_bucket(b: &mut State, p: &Value) -> Response {
    let name = p
        .get("vectorBucketName")
        .and_then(Value::as_str)
        .unwrap_or("");
    // moto passes only the name; an ARN-only request misses and 404s.
    let pos = match b.bucket_pos(name, "") {
        Some(pos) => pos,
        None => return bucket_not_found(),
    };
    if !b.buckets[pos].indexes.is_empty() {
        return conflict("The specified vector bucket is not empty");
    }
    b.buckets.remove(pos);
    empty_ok()
}

fn get_vector_bucket(b: &State, p: &Value) -> Response {
    let name = p
        .get("vectorBucketName")
        .and_then(Value::as_str)
        .unwrap_or("");
    let arn = p
        .get("vectorBucketArn")
        .and_then(Value::as_str)
        .unwrap_or("");
    let Some(pos) = b.bucket_pos(name, arn) else {
        return bucket_not_found();
    };
    let bucket = &b.buckets[pos];
    json_ok(&json!({
        "vectorBucket": {
            "vectorBucketName": bucket.name,
            "vectorBucketArn": bucket.arn,
            "encryptionConfiguration": bucket.encryption.clone(),
        }
    }))
}

fn list_vector_buckets(b: &State, p: &Value) -> Response {
    let prefix = p.get("prefix").and_then(Value::as_str).unwrap_or("");
    let list: Vec<Value> = b
        .buckets
        .iter()
        .filter(|bk| prefix.is_empty() || bk.name.starts_with(prefix))
        .map(|bk| json!({"vectorBucketName": bk.name, "vectorBucketArn": bk.arn}))
        .collect();
    json_ok(&json!({"vectorBuckets": list}))
}

fn create_index(b: &mut State, p: &Value) -> Response {
    let s = |k: &str| p.get(k).and_then(Value::as_str).unwrap_or("");
    let (bucket_name, bucket_arn, index_name) =
        (s("vectorBucketName"), s("vectorBucketArn"), s("indexName"));
    let data_type = s("dataType");
    let distance_metric = s("distanceMetric");
    // moto validates dataType, then dimension, then distanceMetric — but a
    // missing (non-required client-side) value reaches the enum/length
    // checks as absent, which we fold into dimension 0.
    if data_type != "float32" {
        return validation(
            "1 validation error detected. Value at '/dataType' failed to satisfy constraint: Member must satisfy enum value set: [float32]",
        );
    }
    let dimension = p.get("dimension").and_then(Value::as_i64).unwrap_or(0);
    if !(1..4096).contains(&dimension) {
        return validation(
            "1 validation error detected. Value at '/dimension' failed to satisfy constraint: Member must be between 1 and 4096, inclusive",
        );
    }
    if distance_metric != "euclidean" && distance_metric != "cosine" {
        return validation(
            "1 validation error detected. Value at '/distanceMetric' failed to satisfy constraint: Member must satisfy enum value set: [euclidean, cosine]",
        );
    }
    let Some(pos) = b.bucket_pos(bucket_name, bucket_arn) else {
        return bucket_not_found();
    };
    let arn = format!("{}/index/{index_name}", b.buckets[pos].arn);
    let index = IndexRec {
        bucket_name: b.buckets[pos].name.clone(),
        name: index_name.to_string(),
        arn,
        data_type: data_type.to_string(),
        dimension,
        distance_metric: distance_metric.to_string(),
        vectors: Vec::new(),
    };
    // moto keys indexes by ARN: a repeat replaces in place.
    match b.buckets[pos]
        .indexes
        .iter()
        .position(|ix| ix.arn == index.arn)
    {
        Some(ipos) => b.buckets[pos].indexes[ipos] = index,
        None => b.buckets[pos].indexes.push(index),
    }
    empty_ok()
}

fn delete_index(b: &mut State, p: &Value) -> Response {
    let index_arn = p.get("indexArn").and_then(Value::as_str).unwrap_or("");
    let index_name = p.get("indexName").and_then(Value::as_str).unwrap_or("");
    let bucket_name = p
        .get("vectorBucketName")
        .and_then(Value::as_str)
        .unwrap_or("");
    let Some((bpos, ipos)) = b.index_entry(bucket_name, index_name, index_arn) else {
        return index_not_found();
    };
    b.buckets[bpos].indexes.remove(ipos);
    empty_ok()
}

fn get_index(b: &State, p: &Value) -> Response {
    let index_arn = p.get("indexArn").and_then(Value::as_str).unwrap_or("");
    let bucket_name = p
        .get("vectorBucketName")
        .and_then(Value::as_str)
        .unwrap_or("");
    if !bucket_name.is_empty() && !index_arn.is_empty() {
        return validation("Must specify either indexArn or both vectorBucketName and indexName");
    }
    let index_name = p.get("indexName").and_then(Value::as_str).unwrap_or("");
    let Some((bpos, ipos)) = b.index_entry(bucket_name, index_name, index_arn) else {
        return index_not_found();
    };
    let ix = &b.buckets[bpos].indexes[ipos];
    json_ok(&json!({
        "index": {
            "vectorBucketName": ix.bucket_name,
            "indexName": ix.name,
            "indexArn": ix.arn,
            "dataType": ix.data_type,
            "dimension": ix.dimension,
            "distanceMetric": ix.distance_metric,
        }
    }))
}

fn list_indexes(b: &State, p: &Value) -> Response {
    let s = |k: &str| p.get(k).and_then(Value::as_str).unwrap_or("");
    let (bucket_name, bucket_arn) = (s("vectorBucketName"), s("vectorBucketArn"));
    if !bucket_name.is_empty() && !bucket_arn.is_empty() {
        return validation("Must specify either vectorBucketName or vectorBucketArn but not both");
    }
    let Some(pos) = b.bucket_pos(bucket_name, bucket_arn) else {
        return bucket_not_found();
    };
    let list: Vec<Value> = b.buckets[pos]
        .indexes
        .iter()
        .map(|ix| {
            json!({
                "vectorBucketName": ix.bucket_name,
                "indexName": ix.name,
                "indexArn": ix.arn,
            })
        })
        .collect();
    json_ok(&json!({"indexes": list}))
}

fn put_vector_bucket_policy(b: &mut State, p: &Value) -> Response {
    let s = |k: &str| p.get(k).and_then(Value::as_str).unwrap_or("");
    let (bucket_name, bucket_arn) = (s("vectorBucketName"), s("vectorBucketArn"));
    if !bucket_name.is_empty() && !bucket_arn.is_empty() {
        return validation("Must specify either vectorBucketName or vectorBucketArn but not both");
    }
    let Some(pos) = b.bucket_pos(bucket_name, bucket_arn) else {
        return bucket_not_found();
    };
    b.buckets[pos].policy = p.get("policy").and_then(Value::as_str).map(String::from);
    empty_ok()
}

fn get_vector_bucket_policy(b: &State, p: &Value) -> Response {
    let s = |k: &str| p.get(k).and_then(Value::as_str).unwrap_or("");
    let (bucket_name, bucket_arn) = (s("vectorBucketName"), s("vectorBucketArn"));
    if !bucket_name.is_empty() && !bucket_arn.is_empty() {
        return validation("Must specify either vectorBucketName or vectorBucketArn but not both");
    }
    let Some(pos) = b.bucket_pos(bucket_name, bucket_arn) else {
        return bucket_not_found();
    };
    let Some(policy) = b.buckets[pos].policy.as_deref() else {
        return policy_not_found();
    };
    json_ok(&json!({ "policy": policy }))
}

fn delete_vector_bucket_policy(b: &mut State, p: &Value) -> Response {
    let s = |k: &str| p.get(k).and_then(Value::as_str).unwrap_or("");
    let (bucket_name, bucket_arn) = (s("vectorBucketName"), s("vectorBucketArn"));
    if !bucket_name.is_empty() && !bucket_arn.is_empty() {
        return validation("Must specify either vectorBucketName or vectorBucketArn but not both");
    }
    let Some(pos) = b.bucket_pos(bucket_name, bucket_arn) else {
        return bucket_not_found();
    };
    b.buckets[pos].policy = None;
    empty_ok()
}

/// moto `Vector.to_dict`: `key` always, `data`/`metadata` per the flags;
/// moto's `from_dict` defaults absent metadata to `{}`.
fn vector_out(v: &Value, return_data: bool, return_metadata: bool) -> Value {
    let mut o = json!({"key": v.get("key").cloned().unwrap_or(Value::Null)});
    if return_data {
        o["data"] = v.get("data").cloned().unwrap_or(json!({}));
    }
    if return_metadata {
        o["metadata"] = v.get("metadata").cloned().unwrap_or(json!({}));
    }
    o
}

fn put_vectors(b: &mut State, p: &Value) -> Response {
    if let Some(err) = b.validate_index_params(p) {
        return err;
    }
    let index_arn = p.get("indexArn").and_then(Value::as_str).unwrap_or("");
    let index_name = p.get("indexName").and_then(Value::as_str).unwrap_or("");
    let bucket_name = p
        .get("vectorBucketName")
        .and_then(Value::as_str)
        .unwrap_or("");
    let Some((bpos, ipos)) = b.index_entry(bucket_name, index_name, index_arn) else {
        return index_not_found();
    };
    let target_dimension = b.buckets[bpos].indexes[ipos].dimension;
    let empty: Vec<Value> = Vec::new();
    let vectors = p.get("vectors").and_then(Value::as_array).unwrap_or(&empty);
    // moto validates every vector's dimension before storing any of them.
    for v in vectors {
        let key = v.get("key").and_then(Value::as_str).unwrap_or("");
        let provided = v
            .get("data")
            .and_then(|d| d.get("float32"))
            .and_then(Value::as_array)
            .map(|a| a.len())
            .unwrap_or(0) as i64;
        if provided != target_dimension {
            return validation(format!(
                "Invalid record for key '{key}': vector must have length {target_dimension}, but has length {provided}",
            ));
        }
    }
    for v in vectors {
        let key = v
            .get("key")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let stored = &mut b.buckets[bpos].indexes[ipos].vectors;
        match stored.iter().position(|(k, _)| *k == key) {
            // moto keys vectors by key: a repeat replaces in place.
            Some(pos) => stored[pos] = (key.clone(), v.clone()),
            None => stored.push((key, v.clone())),
        }
    }
    empty_ok()
}

fn get_vectors(b: &State, p: &Value) -> Response {
    if let Some(err) = b.validate_index_params(p) {
        return err;
    }
    let index_arn = p.get("indexArn").and_then(Value::as_str).unwrap_or("");
    let index_name = p.get("indexName").and_then(Value::as_str).unwrap_or("");
    let bucket_name = p
        .get("vectorBucketName")
        .and_then(Value::as_str)
        .unwrap_or("");
    let Some((bpos, ipos)) = b.index_entry(bucket_name, index_name, index_arn) else {
        return index_not_found();
    };
    let return_data = p
        .get("returnData")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let return_metadata = p
        .get("returnMetadata")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let keys: Vec<&str> = p
        .get("keys")
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    let list: Vec<Value> = b.buckets[bpos].indexes[ipos]
        .vectors
        .iter()
        .filter(|(k, _)| keys.contains(&k.as_str()))
        .map(|(_, v)| vector_out(v, return_data, return_metadata))
        .collect();
    json_ok(&json!({"vectors": list}))
}

fn list_vectors(b: &State, p: &Value) -> Response {
    if let Some(err) = b.validate_index_params(p) {
        return err;
    }
    let index_arn = p.get("indexArn").and_then(Value::as_str).unwrap_or("");
    let index_name = p.get("indexName").and_then(Value::as_str).unwrap_or("");
    let bucket_name = p
        .get("vectorBucketName")
        .and_then(Value::as_str)
        .unwrap_or("");
    let Some((bpos, ipos)) = b.index_entry(bucket_name, index_name, index_arn) else {
        return index_not_found();
    };
    let return_data = p
        .get("returnData")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let return_metadata = p
        .get("returnMetadata")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let list: Vec<Value> = b.buckets[bpos].indexes[ipos]
        .vectors
        .iter()
        .map(|(_, v)| vector_out(v, return_data, return_metadata))
        .collect();
    json_ok(&json!({"vectors": list}))
}

fn delete_vectors(b: &mut State, p: &Value) -> Response {
    if let Some(err) = b.validate_index_params(p) {
        return err;
    }
    let index_arn = p.get("indexArn").and_then(Value::as_str).unwrap_or("");
    let index_name = p.get("indexName").and_then(Value::as_str).unwrap_or("");
    let bucket_name = p
        .get("vectorBucketName")
        .and_then(Value::as_str)
        .unwrap_or("");
    let Some((bpos, ipos)) = b.index_entry(bucket_name, index_name, index_arn) else {
        return index_not_found();
    };
    let keys: Vec<&str> = p
        .get("keys")
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    b.buckets[bpos].indexes[ipos]
        .vectors
        .retain(|(k, _)| !keys.contains(&k.as_str()));
    empty_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn req(method: &str, path: &str, body: Value) -> Request {
        Request {
            service: "s3vectors".into(),
            operation: String::new(),
            method: method.into(),
            path: path.into(),
            query_string: String::new(),
            headers: Default::default(),
            body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap()),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    fn first_json(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap()
    }

    #[test]
    fn bucket_lifecycle() {
        let slot = state();
        let mut st = slot.write();
        let b = st.entry((123456789012, "us-east-1".into())).or_default();
        b.buckets.clear();

        let r = handle(
            Op::CreateVectorBucket,
            "us-east-1",
            123456789012,
            b,
            &json!({"vectorBucketName": "vbx"}),
        );
        assert_eq!(r.status, 200);

        let got = first_json(&handle(
            Op::GetVectorBucket,
            "us-east-1",
            123456789012,
            b,
            &json!({"vectorBucketName": "vbx"}),
        ));
        assert_eq!(got["vectorBucket"]["vectorBucketName"], "vbx");
        assert_eq!(
            got["vectorBucket"]["vectorBucketArn"],
            "arn:aws:s3vectors:us-east-1:123456789012:bucket/vbx"
        );
        // moto carries no creationTime on the model — omitted on the wire.
        assert!(got["vectorBucket"].get("creationTime").is_none());

        let listed = first_json(&handle(
            Op::ListVectorBuckets,
            "us-east-1",
            123456789012,
            b,
            &json!({}),
        ));
        assert_eq!(listed["vectorBuckets"][0]["vectorBucketName"], "vbx");

        let dupe = handle(
            Op::CreateVectorBucket,
            "us-east-1",
            123456789012,
            b,
            &json!({"vectorBucketName": "vbx"}),
        );
        assert_eq!(dupe.status, 409);

        let short = handle(
            Op::CreateVectorBucket,
            "us-east-1",
            123456789012,
            b,
            &json!({"vectorBucketName": "no"}),
        );
        assert_eq!(short.status, 400);
        assert!(first_json(&short)["message"]
            .as_str()
            .unwrap()
            .contains("between 3 and 63"));

        // Not-empty conflict, then delete.
        create_index(
            b,
            &json!({
                "vectorBucketName": "vbx", "indexName": "i",
                "dataType": "float32", "dimension": 2, "distanceMetric": "cosine",
            }),
        );
        let not_empty = delete_vector_bucket(b, &json!({"vectorBucketName": "vbx"}));
        assert_eq!(not_empty.status, 409);
        delete_index(b, &json!({"vectorBucketName": "vbx", "indexName": "i"}));
        let gone = handle(
            Op::DeleteVectorBucket,
            "us-east-1",
            123456789012,
            b,
            &json!({"vectorBucketName": "vbx"}),
        );
        assert_eq!(gone.status, 200);
        assert_eq!(b.buckets.len(), 0);
    }

    #[test]
    fn index_and_vectors_roundtrip() {
        let slot = state();
        let mut st = slot.write();
        let b = st.entry((123456789012, "us-east-1".into())).or_default();
        b.buckets.clear();
        create_vector_bucket(
            b,
            "us-east-1",
            123456789012,
            &json!({"vectorBucketName": "vbx"}),
        );

        let created = create_index(
            b,
            &json!({
                "vectorBucketName": "vbx", "indexName": "i",
                "dataType": "float32", "dimension": 4, "distanceMetric": "euclidean",
            }),
        );
        assert_eq!(created.status, 200);
        let got = first_json(&get_index(
            b,
            &json!({"vectorBucketName": "vbx", "indexName": "i"}),
        ));
        assert_eq!(got["index"]["dimension"], 4);
        assert_eq!(got["index"]["distanceMetric"], "euclidean");

        let bad_dim = put_vectors(
            b,
            &json!({
                "vectorBucketName": "vbx", "indexName": "i",
                "vectors": [{"key": "v1", "data": {"float32": [1.0, 2.0, 3.0]}}],
            }),
        );
        assert_eq!(bad_dim.status, 400);
        assert!(first_json(&bad_dim)["message"]
            .as_str()
            .unwrap()
            .contains("must have length 4"));

        put_vectors(
            b,
            &json!({
                "vectorBucketName": "vbx", "indexName": "i",
                "vectors": [
                    {"key": "v1", "data": {"float32": [1.0, 2.0, 3.0, 4.0]}, "metadata": {"a": "b"}},
                    {"key": "v2", "data": {"float32": [5.0, 6.0, 7.0, 8.0]}},
                ],
            }),
        );
        let got = first_json(&get_vectors(
            b,
            &json!({
                "vectorBucketName": "vbx", "indexName": "i", "keys": ["v1", "v2"],
                "returnData": true, "returnMetadata": true,
            }),
        ));
        assert_eq!(
            got["vectors"][0]["data"]["float32"],
            json!([1.0, 2.0, 3.0, 4.0])
        );
        assert_eq!(got["vectors"][0]["metadata"], json!({"a": "b"}));
        assert_eq!(got["vectors"][1]["metadata"], json!({}));

        delete_vectors(
            b,
            &json!({
                "vectorBucketName": "vbx", "indexName": "i", "keys": ["v1"],
            }),
        );
        let listed = first_json(&list_vectors(
            b,
            &json!({
                "vectorBucketName": "vbx", "indexName": "i",
            }),
        ));
        // Empty index lists still answer with the required `vectors` member.
        assert_eq!(listed["vectors"].as_array().unwrap().len(), 1);
        assert_eq!(listed["vectors"][0]["key"], "v2");
        // Without returnData the vector entry carries only the key.
        assert!(listed["vectors"][0].get("data").is_none());
    }

    #[test]
    fn policy_arn_validation_index() {
        let slot = state();
        let mut st = slot.write();
        let b = st.entry((123456789012, "us-east-1".into())).or_default();
        b.buckets.clear();
        create_vector_bucket(
            b,
            "us-east-1",
            123456789012,
            &json!({"vectorBucketName": "vbx"}),
        );

        // Policy roundtrip and not-found surfaces.
        put_vector_bucket_policy(b, &json!({"vectorBucketName": "vbx", "policy": "{}"}));
        let got = first_json(&get_vector_bucket_policy(
            b,
            &json!({"vectorBucketName": "vbx"}),
        ));
        assert_eq!(got["policy"], "{}");
        let missing = get_vector_bucket_policy(b, &json!({"vectorBucketName": "nope"}));
        assert_eq!(missing.status, 404);
        let no_policy = get_vector_bucket_policy(
            b,
            &json!({"vectorBucketArn":
            "arn:aws:s3vectors:us-east-1:123456789012:bucket/nope"}),
        );
        assert_eq!(no_policy.status, 404);
        delete_vector_bucket_policy(b, &json!({"vectorBucketName": "vbx"}));
        let empty = get_vector_bucket_policy(b, &json!({"vectorBucketName": "vbx"}));
        assert_eq!(empty.status, 404);

        // name+arn together validate; index ARN parsing resolves the index.
        let both = handle(
            Op::DeleteVectorBucketPolicy,
            "us-east-1",
            123456789012,
            b,
            &json!({"vectorBucketName": "vbx", "vectorBucketArn": "arn:aws:s3vectors:us-east-1:123456789012:bucket/vbx"}),
        );
        assert_eq!(both.status, 400);
        create_index(
            b,
            &json!({
                "vectorBucketName": "vbx", "indexName": "i",
                "dataType": "float32", "dimension": 1, "distanceMetric": "cosine",
            }),
        );
        let by_arn = first_json(&get_index(
            b,
            &json!({"indexArn":
            "arn:aws:s3vectors:us-east-1:123456789012:bucket/vbx/index/i"}),
        ));
        assert_eq!(by_arn["index"]["indexName"], "i");
        assert_eq!(by_arn["index"]["vectorBucketName"], "vbx");
    }

    #[test]
    fn unrouted_operations_fall_through() {
        assert!(dispatch(&req(
            "GET",
            "/tags/arn:aws:s3vectors:us-east-1:123456789012:bucket/vbx",
            json!({})
        ))
        .is_none());
        assert!(dispatch(&req("POST", "/QueryVectors", json!({}))).is_none());
        assert!(dispatch(&req("POST", "/UnknownOp", json!({}))).is_none());
    }
}
