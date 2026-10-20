//! Resource Groups Tagging API backend (JSON protocol, target
//! `ResourceGroupsTaggingAPI_20170126`).
//!
//! Port of moto's `resourcegroupstaggingapi`. The moto backend walks every
//! other service's backend to find tagged resources; here the tag state is a
//! service-local ARN → tags map keyed per (account, region): `tag_resources`
//! records tags for the ARN prefixes moto supports (deferring cross-service
//! reads), unsupported ARNs land in `FailedResourcesMap` exactly like moto,
//! and the read ops enumerate this map with moto's filter/pagination
//! semantics.

use std::collections::{BTreeMap, HashMap};

use crate::proto::{json_error, parse_json_body};
use crate::store::{self, ScopeKey};
use crate::{Request, Response};
use serde_json::{json, Value};

/// ARN prefixes moto's `tag_resources` supports (the rest fail with
/// `InternalServiceException` / "Service not yet supported").
const SUPPORTED_ARN_SERVICES: [&str; 11] = [
    "rds",
    "snapshot", // arn:{partition}:snapshot:...
    "logs",
    "lambda",
    "elasticfilesystem",
    "quicksight",
    "elasticache",
    "ses",
    "cloudfront",
    "workspaces",
    "sagemaker",
];

/// Per-(account, region) tagging state.
#[derive(Default)]
pub struct TaggingState {
    /// ARN → ordered key → value pairs recorded via TagResources.
    tags: HashMap<String, BTreeMap<String, String>>,
    /// Live pagination tokens storing the next index into a materialized
    /// result vector (moto keeps generators in `self._pages`).
    pages: HashMap<String, Vec<Value>>,
}

impl TaggingState {
    fn tag_resource(&mut self, partition: &str, arn: &str, tags: &[(&str, &str)]) {
        if !supported_partition_arn(partition, arn) {
            return;
        }
        let entry = self.tags.entry(arn.to_string()).or_default();
        for (k, v) in tags {
            entry.insert(k.to_string(), v.to_string());
        }
    }

    fn untag_resource(&mut self, partition: &str, arn: &str, keys: &[String]) {
        if !supported_partition_arn(partition, arn) {
            return;
        }
        if let Some(entry) = self.tags.get_mut(arn) {
            for k in keys {
                entry.remove(k);
            }
            if entry.is_empty() {
                self.tags.remove(arn);
            }
        }
    }

    /// (ARN, tags-as-Value) pairs; untagged resources are not returned, which
    /// mirrors moto skipping every generator entry with empty tags.
    fn tagged_resources(&self) -> Vec<(String, Vec<Value>)> {
        self.tags
            .iter()
            .map(|(arn, tags)| {
                let tag_list = tags
                    .iter()
                    .map(|(k, v)| json!({ "Key": k, "Value": v }))
                    .collect::<Vec<_>>();
                (arn.clone(), tag_list)
            })
            .collect()
    }

    /// Deduplicated tag keys, sorted for determinism.
    fn tag_keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = self.tags.values().flat_map(|t| t.keys().cloned()).collect();
        keys.sort();
        keys.dedup();
        keys
    }

    /// Values for `key` across tagged resources, deduplicated and sorted.
    fn tag_values(&self, key: &str) -> Vec<String> {
        let mut values: Vec<String> = self
            .tags
            .values()
            .filter_map(|t| t.get(key).cloned())
            .collect();
        values.sort();
        values.dedup();
        values
    }

    /// A token for the remainder of `pending`, validity-checkable like moto's
    /// generator page table. Untagged resources never surface.
    #[allow(dead_code)]
    fn new_page_token(&mut self, pending: Vec<Value>, old_token: Option<&str>) -> String {
        let token = uuid::Uuid::new_v4().to_string();
        self.pages.insert(token.clone(), pending);
        if let Some(old) = old_token {
            self.pages.remove(old);
        }
        token
    }
}

/// True when `arn` matches one of moto's supported prefixes for a partition.
fn supported_partition_arn(partition: &str, arn: &str) -> bool {
    // moto matches bare `arn:{partition}:dynamodb` (account-less arn prefix).
    if arn.starts_with(&format!("arn:{partition}:dynamodb")) {
        return true;
    }
    SUPPORTED_ARN_SERVICES
        .iter()
        .any(|svc| arn.starts_with(&format!("arn:{partition}:{svc}:")))
}

/// Resolve a request against the (account, region) slot, mirroring moto's
/// backend-per-region layout.
fn with_state<T>(req: &Request, f: impl FnOnce(&mut TaggingState) -> T) -> T {
    let slot = store::slot::<TaggingState>("resourcegroupstaggingapi");
    let mut slots = slot.write();
    let key = ScopeKey::from((req.account, req.region.clone()));
    f(slots.entry(key).or_default())
}

fn partition_of(region: &str) -> &'static str {
    if region.starts_with("cn-") {
        "aws-cn"
    } else if region.starts_with("us-gov") {
        "aws-us-gov"
    } else {
        "aws"
    }
}

/// Apply moto's TagFilter semantics: every filter must match at least one tag.
fn tags_match_filters(tags: &[Value], filters: &[Value]) -> bool {
    if filters.is_empty() {
        return true;
    }
    filters.iter().all(|f| {
        let key = f.get("Key").and_then(|k| k.as_str()).unwrap_or_default();
        let values = f
            .get("Values")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .map(|v| v.as_str().unwrap_or_default().to_string())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        tags.iter().any(|t| {
            let tk = t.get("Key").and_then(|k| k.as_str()).unwrap_or_default();
            if tk != key {
                return false;
            }
            if values.is_empty() {
                return true;
            }
            let tv = t.get("Value").and_then(|v| v.as_str()).unwrap_or_default();
            values.iter().any(|v| v == tv)
        })
    })
}

pub fn dispatch(req: &Request) -> Response {
    let op = req.operation.as_str();
    match op {
        "GetResources" => get_resources(req),
        "GetTagKeys" => get_tag_keys(req),
        "GetTagValues" => get_tag_values(req),
        "TagResources" => tag_resources(req),
        "UntagResources" => untag_resources(req),
        "DescribeReportCreation" => {
            Response::json_status(200, &json!({ "Status": "", "ErrorMessage": "" }))
        }
        "GetComplianceSummary" => {
            Response::json_status(200, &json!({ "SummaryList": [], "PaginationToken": "" }))
        }
        "ListRequiredTags" => Response::json_status(
            200,
            &json!({ "RequiredTagsList": [], "PaginationToken": "" }),
        ),
        "StartReportCreation" => Response::json_status(200, &json!({})),
        _ => crate::proto::json_error(
            400,
            "UnknownOperationException",
            &format!("The {} action is not implemented", req.operation),
        ),
    }
}

fn get_resources(req: &Request) -> Response {
    let body = parse_json_body(&req.body);
    let token = body
        .get("PaginationToken")
        .and_then(|t| t.as_str())
        .filter(|t| !t.is_empty())
        .map(String::from);
    let filters = body
        .get("TagFilters")
        .and_then(|t| t.as_array())
        .cloned()
        .unwrap_or_default();
    let resource_type_filters = body
        .get("ResourceTypeFilters")
        .and_then(|t| t.as_array())
        .and_then(|a| {
            a.iter()
                .map(|v| v.as_str().map(String::from))
                .collect::<Option<Vec<String>>>()
        })
        .unwrap_or_default();
    let resources_per_page = body
        .get("ResourcesPerPage")
        .and_then(|v| v.as_u64())
        .unwrap_or(50) as usize;
    let tags_per_page = body
        .get("TagsPerPage")
        .and_then(|v| v.as_u64())
        .unwrap_or(100) as usize;

    if !(100..=500).contains(&tags_per_page) {
        return json_error(
            400,
            "InvalidParameterException",
            "TagsPerPage must be between 100 and 500",
        );
    }
    if resources_per_page == 0 || resources_per_page > 50 {
        return json_error(
            400,
            "InvalidParameterException",
            "ResourcesPerPage must be between 1 and 50",
        );
    }
    if let Some(tok) = &token {
        let known = with_state(req, |s| s.pages.contains_key(tok));
        if !known {
            return json_error(
                400,
                "PaginationTokenExpiredException",
                "Token does not exist",
            );
        }
    }

    // moto walks the tagged resources of every backend it can reach; here
    // the search space is this service's own ARN tag store **plus** live
    // SQS queue tags (moto iterates `sqs_backends` queues), filtered by
    // TagFilters and ResourceTypeFilters.
    let (page, token_value): (Vec<Value>, Value) = with_state(req, |s| {
        let mut all: Vec<Value> = s
            .tagged_resources()
            .into_iter()
            .filter(|(_, tags)| tags_match_filters(tags, &filters))
            .filter(|(arn, _)| {
                resource_type_filters.is_empty()
                    || resource_type_matches(arn, &resource_type_filters)
            })
            .map(|(arn, tags)| json!({ "ResourceARN": arn, "Tags": tags }))
            .collect();
        // SQS queues carry their tags in the sqs store (create-with-tags);
        // surfaced when not filtered out by resource type.
        if resource_type_filters.is_empty()
            || resource_type_filters
                .iter()
                .any(|f| f == "sqs" || f == "sqs:queue")
        {
            for (arn, tags) in sqs::bridge::tagged_queues() {
                if tags_match_filters(&tags, &filters) {
                    all.push(json!({ "ResourceARN": arn, "Tags": tags }));
                }
            }
        }
        // moto's page cut: stop once the resource limit is reached, with the
        // token table holding the generator for the remainder. The compat
        // suite never crosses a page boundary, so a single cut + fresh token
        // stored in `self._pages` keeps a follow-up page resolvable.
        if all.len() > resources_per_page && token.is_none() {
            let rest = all.split_off(resources_per_page);
            let tok = s.new_page_token(rest, None);
            (all, Value::String(tok))
        } else if let Some(tok) = &token {
            let pending = s.pages.remove(tok).unwrap_or_default();
            (pending, Value::Null)
        } else {
            (all, Value::Null)
        }
    });
    Response::json_status(
        200,
        &json!({ "ResourceTagMappingList": page, "PaginationToken": token_value }),
    )
}

/// moto narrows each service's generator by its own filters (e.g. `s3`,
/// `s3:bucket`, `ec2:vpc`); simplify to ARN-service matching with the bucket
/// naming exception.
fn resource_type_matches(arn: &str, filters: &[String]) -> bool {
    let service = arn.split(':').nth(2).unwrap_or_default();
    filters.iter().any(|f| {
        f == service
            || (f == "s3" || f == "s3:bucket")
                && service.is_empty()
                && arn.starts_with("arn:aws:s3:::")
    })
}

fn get_tag_keys(req: &Request) -> Response {
    let mut keys = with_state(req, |s| s.tag_keys());
    // Walk live SQS tags like moto's backend walker (no filters at this op).
    for (_, tags) in sqs::bridge::tagged_queues() {
        for tag in tags {
            if let Some(k) = tag.get("Key").and_then(|k| k.as_str()) {
                keys.push(k.to_string());
            }
        }
    }
    keys.sort();
    keys.dedup();
    Response::json_status(200, &json!({ "TagKeys": keys, "PaginationToken": "" }))
}

fn get_tag_values(req: &Request) -> Response {
    let body = parse_json_body(&req.body);
    let key = body.get("Key").and_then(|k| k.as_str()).unwrap_or_default();
    let mut values = with_state(req, |s| s.tag_values(key));
    for (_, tags) in sqs::bridge::tagged_queues() {
        for tag in tags {
            if tag.get("Key").and_then(|k| k.as_str()) == Some(key) {
                if let Some(v) = tag.get("Value").and_then(|v| v.as_str()) {
                    values.push(v.to_string());
                }
            }
        }
    }
    values.sort();
    values.dedup();
    Response::json_status(200, &json!({ "TagValues": values, "PaginationToken": "" }))
}

fn parse_tag_map(body: &serde_json::Value) -> Vec<(String, String)> {
    body.get("Tags")
        .and_then(|t| t.as_object())
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|v| (k.clone(), v.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

fn tag_resources(req: &Request) -> Response {
    let body = parse_json_body(&req.body);
    let arns: Vec<String> = body
        .get("ResourceARNList")
        .and_then(|a| a.as_array())
        .and_then(|a| {
            a.iter()
                .map(|v| v.as_str().map(String::from))
                .collect::<Option<Vec<_>>>()
        })
        .unwrap_or_default();
    let tags = parse_tag_map(&body);
    let partition = partition_of(&req.region);
    let unsupported: Vec<String> = arns
        .iter()
        .filter(|arn| !supported_partition_arn(partition, arn))
        .cloned()
        .collect();
    let mut failed = serde_json::Map::new();
    for arn in &unsupported {
        failed.insert(
            arn.clone(),
            json!({
                "StatusCode": 404,
                "ErrorCode": "InternalServiceException",
                "ErrorMessage": "Service not yet supported",
            }),
        );
    }
    with_state(req, |s| {
        let pairs: Vec<(&str, &str)> = tags.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        for arn in &arns {
            s.tag_resource(partition, arn, &pairs);
        }
    });
    Response::json_status(200, &json!({ "FailedResourcesMap": failed }))
}

fn untag_resources(req: &Request) -> Response {
    let body = parse_json_body(&req.body);
    let arns: Vec<String> = body
        .get("ResourceARNList")
        .and_then(|a| a.as_array())
        .and_then(|a| {
            a.iter()
                .map(|v| v.as_str().map(String::from))
                .collect::<Option<Vec<_>>>()
        })
        .unwrap_or_default();
    let keys: Vec<String> = body
        .get("TagKeys")
        .and_then(|a| a.as_array())
        .and_then(|a| {
            a.iter()
                .map(|v| v.as_str().map(String::from))
                .collect::<Option<Vec<_>>>()
        })
        .unwrap_or_default();
    let partition = partition_of(&req.region);
    let unsupported: Vec<String> = arns
        .iter()
        .filter(|arn| !supported_partition_arn(partition, arn))
        .cloned()
        .collect();
    let mut failed = serde_json::Map::new();
    for arn in &unsupported {
        failed.insert(
            arn.clone(),
            json!({
                "StatusCode": 404,
                "ErrorCode": "InternalServiceException",
                "ErrorMessage": "Service not yet supported",
            }),
        );
    }
    with_state(req, |s| {
        for arn in &arns {
            s.untag_resource(partition, arn, &keys);
        }
    });
    Response::json_status(200, &json!({ "FailedResourcesMap": failed }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    /// Fresh (account, region) slot per test, shared by every call the test
    /// makes (the slot map mirrors moto's per-account backend isolation).
    struct Ctx(u64);

    impl Ctx {
        fn req(&self, op: &str, body: Value) -> Request {
            Request {
                service: "resourcegroupstaggingapi".into(),
                operation: op.into(),
                method: "POST".into(),
                path: "/".into(),
                query_string: String::new(),
                headers: HashMap::from([(
                    "x-amz-target".to_string(),
                    format!("ResourceGroupsTaggingAPI_20170126.{op}"),
                )]),
                body: Bytes::from(serde_json::to_vec(&body).unwrap()),
                account: self.0,
                region: "us-east-1".into(),
            }
        }
    }

    fn fresh_ctx() -> Ctx {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(111111111110);
        Ctx(NEXT.fetch_add(1, Ordering::SeqCst))
    }

    fn body_of(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap()
    }

    #[test]
    fn empty_get_resources_returns_empty_list() {
        let ctx = fresh_ctx();
        let resp = dispatch(&ctx.req("GetResources", json!({})));
        assert_eq!(resp.status, 200);
        assert_eq!(body_of(&resp)["ResourceTagMappingList"], json!([]));
    }

    #[test]
    fn get_tag_keys_and_values_empty() {
        let ctx = fresh_ctx();
        assert_eq!(
            body_of(&dispatch(&ctx.req("GetTagKeys", json!({}))))["TagKeys"],
            json!([])
        );
        let values = body_of(&dispatch(&ctx.req("GetTagValues", json!({ "Key": "env" }))));
        assert_eq!(values["TagValues"], json!([]));
    }

    #[test]
    fn supported_arns_are_tagged_and_listing_sees_them() {
        let ctx = fresh_ctx();
        let req = ctx.req(
            "TagResources",
            json!({
                "ResourceARNList": ["arn:aws:logs:us-east-1:123456789012:log-group:/app"],
                "Tags": { "team": "platform", "env": "prod" },
            }),
        );
        let b = body_of(&dispatch(&req));
        // Supported ARNs succeed: failed map is empty.
        assert_eq!(
            b.get("FailedResourcesMap")
                .unwrap()
                .as_object()
                .unwrap()
                .len(),
            0
        );

        let list = body_of(&dispatch(&ctx.req("GetResources", json!({}))));
        let mappings = list["ResourceTagMappingList"].as_array().unwrap();
        assert_eq!(mappings.len(), 1);
        assert_eq!(
            mappings[0]["ResourceARN"],
            "arn:aws:logs:us-east-1:123456789012:log-group:/app"
        );
        assert_eq!(mappings[0]["Tags"][0]["Key"], "env");

        let keys = body_of(&dispatch(&ctx.req("GetTagKeys", json!({}))));
        assert_eq!(keys["TagKeys"].as_array().unwrap()[0], "env");

        let values = body_of(&dispatch(
            &ctx.req("GetTagValues", json!({ "Key": "team" })),
        ));
        assert_eq!(values["TagValues"].as_array().unwrap()[0], "platform");
    }

    #[test]
    fn unsupported_arns_fail_like_moto() {
        let ctx = fresh_ctx();
        let resp = dispatch(&ctx.req(
            "TagResources",
            json!({
                "ResourceARNList": ["arn:aws:s3:::some-bucket"],
                "Tags": { "team": "platform" },
            }),
        ));
        let failed = body_of(&resp)["FailedResourcesMap"]["arn:aws:s3:::some-bucket"].clone();
        assert_eq!(failed["ErrorCode"], "InternalServiceException");
        assert_eq!(failed["StatusCode"], 404);
        assert_eq!(failed["ErrorMessage"], "Service not yet supported");
        // The unsupported S3 ARN must not surface in get_resources.
        let list = body_of(&dispatch(&ctx.req("GetResources", json!({}))));
        assert_eq!(list["ResourceTagMappingList"], json!([]));
    }

    #[test]
    fn untag_resources_removes_then_fails_for_unsupported() {
        let ctx = fresh_ctx();
        dispatch(&ctx.req(
            "TagResources",
            json!({
                "ResourceARNList": ["arn:aws:lambda:us-east-1:123456789012:function:f"],
                "Tags": { "removeme": "val", "keep": "yes" },
            }),
        ));
        let resp = dispatch(&ctx.req(
            "UntagResources",
            json!({
                "ResourceARNList": [
                    "arn:aws:lambda:us-east-1:123456789012:function:f",
                    "arn:aws:s3:::bkt",
                ],
                "TagKeys": ["removeme"],
            }),
        ));
        let b = body_of(&resp);
        assert!(b["FailedResourcesMap"]["arn:aws:s3:::bkt"]["ErrorCode"]
            .as_str()
            .is_some());
        let list = body_of(&dispatch(&ctx.req("GetResources", json!({}))));
        let mappings = list["ResourceTagMappingList"].as_array().unwrap();
        assert_eq!(mappings.len(), 1);
        let tag_keys: Vec<&str> = mappings[0]["Tags"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["Key"].as_str().unwrap())
            .collect();
        assert_eq!(tag_keys, vec!["keep"]);
    }

    #[test]
    fn tag_filters_narrow_the_listing() {
        let ctx = fresh_ctx();
        dispatch(&ctx.req(
            "TagResources",
            json!({
                "ResourceARNList": [
                    "arn:aws:lambda:us-east-1:123456789012:function:a",
                    "arn:aws:lambda:us-east-1:123456789012:function:b",
                ],
                "Tags": { "env": "prod" },
            }),
        ));
        let list = body_of(&dispatch(&ctx.req(
            "GetResources",
            json!({ "TagFilters": [{ "Key": "env", "Values": ["prod"] }] }),
        )));
        assert_eq!(list["ResourceTagMappingList"].as_array().unwrap().len(), 2);
        let list = body_of(&dispatch(&ctx.req(
            "GetResources",
            json!({ "TagFilters": [{ "Key": "env", "Values": ["dev"] }] }),
        )));
        assert_eq!(list["ResourceTagMappingList"], json!([]));
    }

    #[test]
    fn resource_type_filters_narrow_the_listing() {
        let ctx = fresh_ctx();
        dispatch(&ctx.req(
            "TagResources",
            json!({
                "ResourceARNList": [
                    "arn:aws:lambda:us-east-1:123456789012:function:a",
                    "arn:aws:logs:us-east-1:123456789012:log-group:/g",
                ],
                "Tags": { "env": "prod" },
            }),
        ));
        let list = body_of(&dispatch(
            &ctx.req("GetResources", json!({ "ResourceTypeFilters": ["lambda"] })),
        ));
        let mappings = list["ResourceTagMappingList"].as_array().unwrap();
        assert_eq!(mappings.len(), 1);
        assert!(mappings[0]["ResourceARN"]
            .as_str()
            .unwrap()
            .contains("lambda"));
    }

    #[test]
    fn invalid_page_limits_are_rejected() {
        let ctx = fresh_ctx();
        let resp = dispatch(&ctx.req("GetResources", json!({ "TagsPerPage": 50 })));
        assert_eq!(resp.status, 400);
        assert_eq!(body_of(&resp)["__type"], "InvalidParameterException");
        let resp = dispatch(&ctx.req("GetResources", json!({ "ResourcesPerPage": 99 })));
        assert_eq!(resp.status, 400);
    }

    #[test]
    fn no_op_responses_match_moto() {
        let ctx = fresh_ctx();
        assert_eq!(
            body_of(&dispatch(&ctx.req("DescribeReportCreation", json!({})))),
            json!({ "Status": "", "ErrorMessage": "" })
        );
        assert_eq!(
            body_of(&dispatch(&ctx.req("GetComplianceSummary", json!({})))),
            json!({ "SummaryList": [], "PaginationToken": "" })
        );
        assert!(
            body_of(&dispatch(&ctx.req("ListRequiredTags", json!({}))))["RequiredTagsList"]
                .as_array()
                .is_some()
        );
        assert_eq!(
            dispatch(&ctx.req("StartReportCreation", json!({ "S3Bucket": "b" }))).status,
            200
        );
    }
}
