//! ECR operation handler.
//!
//! Mirrors the behaviour of `src/robotocore/services/ecr/provider.py`
//! (which forwards to Moto) plus Moto's `ecr` backend itself, so the Rust twin
//! produces the same shapes the Python ground truth produces.

use base64::Engine;
use chrono::Utc;
use parking_lot::RwLock;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

use crate::models::{EcrImage, EcrState};
use crate::protocol::{AwsRequest, AwsResponse};

pub struct EcrHandler {
    pub(crate) state: RwLock<HashMap<(u64, String), EcrState>>,
}

impl Default for EcrHandler {
    fn default() -> Self {
        Self {
            state: RwLock::new(HashMap::new()),
        }
    }
}

impl EcrHandler {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn get_state(&self, account: u64, region: &str) -> EcrState {
        let mut states = self.state.write();
        states
            .entry((account, region.to_string()))
            .or_default()
            .clone()
    }

    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        match req.operation.as_str() {
            "CreateRepository" => self.create_repository(&req),
            "DeleteRepository" => self.delete_repository(&req),
            "DescribeRepositories" => self.describe_repositories(&req),
            "ListImages" => self.list_images(&req),
            "PutImage" => self.put_image(&req),
            "DescribeImages" => self.describe_images(&req),
            "BatchGetImage" => self.batch_get_image(&req),
            "BatchDeleteImage" => self.batch_delete_image(&req),
            "GetAuthorizationToken" => self.get_authorization_token(&req),
            "ListTagsForResource" => self.list_tags(&req),
            "TagResource" => self.tag_resource(&req),
            "UntagResource" => self.untag_resource(&req),
            "GetRepositoryPolicy" => self.get_repository_policy(&req),
            "SetRepositoryPolicy" => self.set_repository_policy(&req),
            "DeleteRepositoryPolicy" => self.delete_repository_policy(&req),
            "StartImageScan" => self.start_image_scan(&req),
            "DescribeImageScanFindings" => self.describe_scan_findings(&req),
            "DescribeImageSigningStatus" => {
                AwsResponse::json(200, json!({ "signingConfiguration": Value::Null }))
            }
            "PutImageScanningConfiguration" => self.put_image_scanning_config(&req),
            "PutImageTagMutability" => self.put_image_tag_mutability(&req),
            "BatchCheckLayerAvailability" => self.batch_check_layer(&req),
            "DescribeRegistry" => self.describe_registry(&req),
            "PutLifecyclePolicy" => self.put_lifecycle_policy(&req),
            "GetLifecyclePolicy" => self.get_lifecycle_policy(&req),
            "DeleteLifecyclePolicy" => self.delete_lifecycle_policy(&req),
            "StartLifecyclePolicyPreview" => self.start_lifecycle_policy_preview(&req),
            "GetLifecyclePolicyPreview" => self.get_lifecycle_policy_preview(&req),
            "PutRegistryPolicy" => self.put_registry_policy(&req),
            "GetRegistryPolicy" => self.get_registry_policy(&req),
            "DeleteRegistryPolicy" => self.delete_registry_policy(&req),
            "GetRegistryScanningConfiguration" => self.get_registry_scanning(&req),
            "PutRegistryScanningConfiguration" => self.put_registry_scanning(&req),
            "BatchGetRepositoryScanningConfiguration" => self.batch_get_scanning_config(&req),
            "PutReplicationConfiguration" => self.put_replication_configuration(&req),
            "GetSigningConfiguration" => self.get_signing_configuration(&req),
            "CreatePullThroughCacheRule" => self.create_pull_through_cache_rule(&req),
            "DeletePullThroughCacheRule" => self.delete_pull_through_cache_rule(&req),
            "UpdatePullThroughCacheRule" => self.update_pull_through_cache_rule(&req),
            "DescribePullThroughCacheRules" => self.describe_pull_through_cache_rules(&req),
            "ValidatePullThroughCacheRule" => self.validate_pull_through_cache_rule(&req),
            "CreateRepositoryCreationTemplate" => self.create_repository_creation_template(&req),
            "DeleteRepositoryCreationTemplate" => self.delete_repository_creation_template(&req),
            "UpdateRepositoryCreationTemplate" => self.update_repository_creation_template(&req),
            "DescribeRepositoryCreationTemplates" => {
                self.describe_repository_creation_templates(&req)
            }
            "GetAccountSetting" => self.get_account_setting(&req),
            "PutAccountSetting" => self.put_account_setting(&req),
            "DescribeImageReplicationStatus" => self.describe_image_replication_status(&req),
            other => AwsResponse::error(
                400,
                "InvalidAction",
                &format!("Could not find operation {}", other),
            ),
        }
    }

    // ------------------------------------------------------------------
    // Repository CRUD
    // ------------------------------------------------------------------

    fn create_repository(&self, req: &AwsRequest) -> AwsResponse {
        let name = match req.params.get("repositoryName").and_then(|v| v.as_str()) {
            Some(n) if !n.is_empty() => n.to_string(),
            _ => {
                return AwsResponse::error(
                    400,
                    "InvalidParameterException",
                    "Invalid parameter at 'repositoryName' failed to satisfy constraint",
                )
            }
        };
        let state = self.get_state(req.account, &req.region);
        let mut repos = state.repositories.write();
        if repos.contains_key(&name) {
            return AwsResponse::error(
                400,
                "RepositoryAlreadyExistsException",
                &format!(
                    "The repository with name '{}' already exists in the registry with id '{}'",
                    name, req.account
                ),
            );
        }
        let repo = json!({
            "repositoryArn": format!("arn:aws:ecr:{}:{}:repository/{}", req.region, req.account, name),
            "registryId": req.account.to_string(),
            "repositoryName": name,
            "repositoryUri": format!("{}.dkr.ecr.{}.amazonaws.com/{}", req.account, req.region, name),
            "createdAt": now_epoch(),
            "imageTagMutability": req.params
                .get("imageTagMutability")
                .and_then(|v| v.as_str()).unwrap_or("MUTABLE"),
            "imageScanningConfiguration": scan_on_push_config(req.params.get("imageScanningConfiguration")),
            "encryptionConfiguration": req.params
                .get("encryptionConfiguration")
                .filter(|c| !c.is_null())
                .cloned()
                .unwrap_or(json!({"encryptionType": "AES256"})),
        });
        if let Some(tags) = req.params.get("tags").and_then(|v| v.as_array()).cloned() {
            if !tags.is_empty() {
                state.tags.write().insert(
                    repo["repositoryArn"]
                        .as_str()
                        .unwrap_or_default()
                        .to_string(),
                    tags,
                );
            }
        }
        repos.insert(name.clone(), repo.clone());
        AwsResponse::json(200, json!({ "repository": repo }))
    }

    fn delete_repository(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("repositoryName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let force = req
            .params
            .get("force")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let state = self.get_state(req.account, &req.region);
        {
            let repos = state.repositories.read();
            if !repos.contains_key(name) {
                return repo_not_found(req, name);
            }
        }
        if !force {
            let has_images = state
                .images
                .read()
                .get(name)
                .map(|v| !v.is_empty())
                .unwrap_or(false);
            if has_images {
                return AwsResponse::error(400, "RepositoryNotEmptyException",
                    &format!("The repository with name '{}' cannot be deleted as it still contains images",
                        name));
            }
        }
        // A concurrent DeleteRepository can remove the entry between the
        // existence check above and this lock — handle absence instead of
        // panicking.
        let repo = match state.repositories.write().remove(name) {
            Some(repo) => repo,
            None => return repo_not_found(req, name),
        };
        state.images.write().remove(name);
        state.repository_policies.write().remove(name);
        state.lifecycle_policies.write().remove(name);
        state.scanning_configs.write().remove(name);
        state
            .tags
            .write()
            .remove(repo["repositoryArn"].as_str().unwrap_or_default());
        AwsResponse::json(200, json!({ "repository": repo }))
    }

    fn describe_repositories(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let repos = state.repositories.read();
        let names: Vec<String> = req
            .params
            .get("repositoryNames")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        for n in &names {
            if !repos.contains_key(n) {
                return repo_not_found(req, n);
            }
        }

        // Sorted for deterministic pagination; moto has no pagination but the
        // Python provider layers index-based `nextToken` on top.
        let mut all: Vec<Value> = repos.values().cloned().collect();
        all.sort_by(|a, b| {
            let na = a
                .get("repositoryName")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let nb = b
                .get("repositoryName")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            na.cmp(nb)
        });
        if !names.is_empty() {
            all.retain(|r| {
                names.iter().any(|n| {
                    n == r
                        .get("repositoryName")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                })
            });
        }

        let max_results = req
            .params
            .get("maxResults")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as usize;
        let next_token = req
            .params
            .get("nextToken")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let start_idx: usize = next_token.parse().unwrap_or(0).min(all.len());
        let remaining = &all[start_idx..];
        if max_results > 0 && remaining.len() > max_results {
            let page: Vec<Value> = remaining[..max_results].to_vec();
            AwsResponse::json(
                200,
                json!({
                    "repositories": page,
                    "nextToken": (start_idx + max_results).to_string(),
                }),
            )
        } else {
            AwsResponse::json(200, json!({ "repositories": remaining, "failures": [] }))
        }
    }

    // ------------------------------------------------------------------
    // Images
    // ------------------------------------------------------------------

    fn require_repository(&self, req: &AwsRequest, name: &str) -> Result<EcrState, AwsResponse> {
        let state = self.get_state(req.account, &req.region);
        if state.repositories.read().contains_key(name) {
            Ok(state)
        } else {
            Err(repo_not_found(req, name))
        }
    }

    fn put_image(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("repositoryName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let manifest = req
            .params
            .get("imageManifest")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let tag: Option<String> = req
            .params
            .get("imageTag")
            .and_then(|v| v.as_str())
            .map(String::from);
        let state = match self.require_repository(req, name) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let parsed: Value = match serde_json::from_str(manifest) {
            Ok(v) => v,
            Err(_) => return AwsResponse::error(400, "InvalidParameterException",
                "Invalid parameter at 'ImageManifest' failed to satisfy constraint: 'Invalid JSON syntax'"),
        };
        let media_type = req
            .params
            .get("imageManifestMediaType")
            .and_then(|v| v.as_str())
            .map(String::from)
            .or_else(|| {
                parsed
                    .get("mediaType")
                    .and_then(|v| v.as_str())
                    .map(String::from)
            })
            .unwrap_or_default();
        let size = parsed
            .get("config")
            .and_then(|c| c.get("size"))
            .and_then(|v| v.as_i64());
        // moto derives the digest from the joined layer digests of the manifest.
        let digest = req
            .params
            .get("imageDigest")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| {
                let joined = parsed
                    .get("layers")
                    .and_then(|l| l.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|l| l.get("digest").and_then(|d| d.as_str()))
                            .collect::<Vec<&str>>()
                            .join("")
                    })
                    .unwrap_or_default();
                let mut hasher = Sha256::new();
                hasher.update(joined.as_bytes());
                format!("sha256:{}", hex::encode(hasher.finalize()))
            });

        let mut images = state.images.write();
        let repo_images = images.entry(name.to_string()).or_insert_with(Vec::new);

        // Two images are "the same" when their manifest strings match (moto).
        let existing_manifest_idx = repo_images.iter().position(|img| img.manifest == manifest);
        let tag_in_use_idx = match tag.as_deref() {
            Some(t) => repo_images
                .iter()
                .position(|img| img.tags.contains(&t.to_string())),
            None => None,
        };

        let repo_immutable = state
            .repositories
            .read()
            .get(name)
            .and_then(|r| r.get("imageTagMutability"))
            .and_then(|m| m.as_str())
            == Some("IMMUTABLE");

        match existing_manifest_idx {
            Some(idx) => {
                if let Some(t) = &tag {
                    if repo_images[idx].tags.contains(t) {
                        return AwsResponse::error(400, "ImageAlreadyExistsException",
                            &format!("Image with tag '{}' already exists in repository '{}' and cannot be re-tagged",
                                t, name));
                    }
                    // moto deletes the tag elsewhere (batch_delete_image) and
                    // moves it onto the image carrying this manifest.
                    if let Some(other_idx) = tag_in_use_idx.filter(|&j| j != idx) {
                        repo_images[other_idx].tags.retain(|x| x != t);
                    }
                    repo_images[idx].tags.push(t.clone());
                }
                let resp = image_summary(req, &repo_images[idx]);
                AwsResponse::json(200, json!({ "image": resp }))
            }
            None => {
                if repo_immutable && tag_in_use_idx.is_some() {
                    return AwsResponse::error(400, "ImageAlreadyExistsException",
                        &format!("Image with tag '{}' already exists in repository '{}' and cannot be re-tagged",
                            tag.clone().unwrap_or_default(), name));
                }
                if let Some(t) = &tag {
                    if let Some(j) = tag_in_use_idx {
                        let other = &mut repo_images[j];
                        other.tags.retain(|x| x != t);
                    }
                }
                let img = EcrImage {
                    digest,
                    tags: tag.clone().into_iter().collect(),
                    manifest: manifest.to_string(),
                    manifest_media_type: media_type,
                    size_in_bytes: size,
                    pushed_at: Utc::now().timestamp(),
                    scanned_at: None,
                };
                let resp = image_summary(req, &img);
                repo_images.push(img);
                AwsResponse::json(200, json!({ "image": resp }))
            }
        }
    }

    fn list_images(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("repositoryName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = match self.require_repository(req, name) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let repo_images = images_of(&state, name);
        let mut ids: Vec<Value> = Vec::new();
        for img in &repo_images {
            if img.tags.is_empty() {
                ids.push(json!({ "imageDigest": img.digest, "imageTag": Value::Null }));
            } else {
                for t in &img.tags {
                    ids.push(json!({ "imageDigest": img.digest, "imageTag": t }));
                }
            }
        }
        AwsResponse::json(200, json!({ "imageIds": ids }))
    }

    fn describe_images(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("repositoryName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = match self.require_repository(req, name) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let repo_images = images_of(&state, name);
        let image_ids = req
            .params
            .get("imageIds")
            .and_then(|v| v.as_array())
            .cloned();
        let selected: Vec<&EcrImage> = match image_ids {
            None => repo_images.iter().collect(),
            Some(ids) => {
                let mut out = Vec::new();
                for id in &ids {
                    let tag = id.get("imageTag").and_then(|v| v.as_str());
                    let digest = id.get("imageDigest").and_then(|v| v.as_str());
                    match repo_images.iter().find(|img| {
                        (tag.map(|t| img.tags.iter().any(|x| x == t)).unwrap_or(true))
                            && (digest.map(|d| img.digest == d).unwrap_or(true))
                            && (tag.is_some() || digest.is_some())
                    }) {
                        Some(img) => out.push(img),
                        None => return image_not_found(digest, tag),
                    }
                }
                out
            }
        };
        let details: Vec<Value> = selected
            .iter()
            .map(|img| self.image_detail(req, img))
            .collect();
        AwsResponse::json(200, json!({ "imageDetails": details }))
    }

    fn batch_get_image(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("repositoryName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = match self.require_repository(req, name) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let repo_images = images_of(&state, name);
        let mut out_images: Vec<Value> = Vec::new();
        let mut failures: Vec<Value> = Vec::new();
        for id in req
            .params
            .get("imageIds")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
        {
            let tag = id.get("imageTag").and_then(|v| v.as_str());
            let digest = id.get("imageDigest").and_then(|v| v.as_str());
            let matches: Vec<&EcrImage> = repo_images
                .iter()
                .filter(|img| {
                    (digest.map(|d| img.digest == d).unwrap_or(false))
                        || (tag
                            .map(|t| img.tags.iter().any(|x| x == t))
                            .unwrap_or(false))
                })
                .collect();
            if matches.is_empty() {
                let mut image_id = json!({});
                if let Some(t) = tag {
                    image_id["imageTag"] = json!(t);
                }
                failures.push(json!({
                    "imageId": image_id,
                    "failureCode": "ImageNotFound",
                    "failureReason": "Requested image not found",
                }));
            } else {
                for img in matches {
                    out_images.push(json!({
                        "registryId": req.account.to_string(),
                        "repositoryName": name,
                        "imageId": image_id_shape(img),
                        "imageManifest": img.manifest,
                        "imageManifestMediaType": img.manifest_media_type,
                    }));
                }
            }
        }
        AwsResponse::json(200, json!({ "images": out_images, "failures": failures }))
    }

    fn batch_delete_image(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("repositoryName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = match self.require_repository(req, name) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let mut deleted: Vec<Value> = Vec::new();
        let mut failures: Vec<Value> = Vec::new();
        {
            let mut images = state.images.write();
            let repo_images = images.entry(name.to_string()).or_insert_with(Vec::new);
            for id in req
                .params
                .get("imageIds")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default()
            {
                let tag = id
                    .get("imageTag")
                    .and_then(|v| v.as_str())
                    .map(String::from);
                let digest = id
                    .get("imageDigest")
                    .and_then(|v| v.as_str())
                    .map(String::from);
                if tag.is_none() && digest.is_none() {
                    failures.push(json!({
                        "imageId": {},
                        "failureCode": "MissingDigestAndTag",
                        "failureReason": "Invalid request parameters: both tag and digest cannot be null",
                    }));
                    continue;
                }
                if let Some(d) = &digest {
                    if !is_valid_digest(d) {
                        failures.push(json!({
                            "imageId": { "imageDigest": d },
                            "failureCode": "InvalidImageDigest",
                            "failureReason": "Invalid request parameters: image digest should satisfy the regex '[a-zA-Z0-9-_+.]+:[a-fA-F0-9]+'",
                        }));
                        continue;
                    }
                }
                if let Some(d) = &digest {
                    // Digest matching always deletes the image in full.
                    match repo_images.iter().position(|img| &img.digest == d) {
                        Some(idx) => {
                            let img = repo_images.remove(idx);
                            if img.tags.is_empty() {
                                deleted.push(
                                    json!({ "imageDigest": img.digest, "imageTag": Value::Null }),
                                );
                            } else {
                                for t in img.tags.iter().rev() {
                                    deleted
                                        .push(json!({ "imageDigest": img.digest, "imageTag": t }));
                                }
                            }
                        }
                        None => failures.push(image_not_found_payload(Some(d.clone()), None)),
                    }
                } else {
                    // Tag matching may leave sibling tags behind.
                    match repo_images
                        .iter()
                        .position(|img| img.tags.contains(tag.as_ref().unwrap()))
                    {
                        Some(idx) => {
                            let t = tag.clone().unwrap();
                            deleted.push(
                                json!({ "imageDigest": repo_images[idx].digest, "imageTag": t }),
                            );
                            repo_images[idx].tags.retain(|x| *x != t);
                            if repo_images[idx].tags.is_empty() {
                                repo_images.remove(idx);
                            }
                        }
                        None => failures.push(image_not_found_payload(None, tag)),
                    }
                }
            }
        }
        AwsResponse::json(200, json!({ "imageIds": deleted, "failures": failures }))
    }

    // ------------------------------------------------------------------
    // Layer API
    // ------------------------------------------------------------------

    fn batch_check_layer(&self, req: &AwsRequest) -> AwsResponse {
        let repo_name = req
            .params
            .get("repositoryName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let digests = req
            .params
            .get("layerDigests")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let layers: Vec<Value> = digests
            .iter()
            .map(|d| {
                json!({
                    "layerDigest": d,
                    "layerAvailability": "UNAVAILABLE",
                    "repositoryName": repo_name,
                    "registryId": req.account.to_string(),
                })
            })
            .collect();
        AwsResponse::json(200, json!({ "layers": layers, "failures": [] }))
    }

    fn get_authorization_token(&self, req: &AwsRequest) -> AwsResponse {
        let registry_ids: Vec<String> = req
            .params
            .get("registryIds")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .filter(|v: &Vec<String>| !v.is_empty())
            .unwrap_or_else(|| vec![req.account.to_string()]);
        let auth_data: Vec<Value> = registry_ids
            .iter()
            .map(|id| {
                let token = base64::engine::general_purpose::STANDARD
                    .encode(format!("AWS:{}-auth-token", id));
                json!({
                    "authorizationToken": token,
                    "expiresAt": 1420070400i64,
                    "proxyEndpoint": format!("https://{}.dkr.ecr.{}.amazonaws.com", id, req.region),
                })
            })
            .collect();
        AwsResponse::json(200, json!({ "authorizationData": auth_data }))
    }

    // ------------------------------------------------------------------
    // Tags
    // ------------------------------------------------------------------

    fn list_tags(&self, req: &AwsRequest) -> AwsResponse {
        let (arn, repo_name) = match parse_repo_arn(
            req.params.get("resourceArn").and_then(|v| v.as_str()),
        ) {
            Some(v) => v,
            None => return AwsResponse::error(
                400,
                "InvalidParameterException",
                "Invalid parameter at 'resourceArn' failed to satisfy constraint: 'Invalid ARN'",
            ),
        };
        let state = match self.require_repository(req, &repo_name) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let tags = state.tags.read().get(arn).cloned().unwrap_or_default();
        AwsResponse::json(200, json!({ "tags": tags }))
    }

    fn tag_resource(&self, req: &AwsRequest) -> AwsResponse {
        let (arn, repo_name) = match parse_repo_arn(
            req.params.get("resourceArn").and_then(|v| v.as_str()),
        ) {
            Some(v) => v,
            None => return AwsResponse::error(
                400,
                "InvalidParameterException",
                "Invalid parameter at 'resourceArn' failed to satisfy constraint: 'Invalid ARN'",
            ),
        };
        let state = match self.require_repository(req, &repo_name) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let tags: Vec<Value> = req
            .params
            .get("tags")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let mut all_tags = state.tags.write();
        let entry = all_tags.entry(arn.to_string()).or_insert_with(Vec::new);
        for tag in tags {
            let key = tag.get("Key").and_then(|k| k.as_str()).unwrap_or_default();
            if let Some(slot) = entry
                .iter_mut()
                .find(|t| t.get("Key").and_then(|k| k.as_str()) == Some(key))
            {
                *slot = tag;
            } else {
                entry.push(tag);
            }
        }
        AwsResponse::json(200, json!({}))
    }

    fn untag_resource(&self, req: &AwsRequest) -> AwsResponse {
        let (arn, repo_name) = match parse_repo_arn(
            req.params.get("resourceArn").and_then(|v| v.as_str()),
        ) {
            Some(v) => v,
            None => return AwsResponse::error(
                400,
                "InvalidParameterException",
                "Invalid parameter at 'resourceArn' failed to satisfy constraint: 'Invalid ARN'",
            ),
        };
        let state = match self.require_repository(req, &repo_name) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let keys: Vec<String> = req
            .params
            .get("tagKeys")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let mut all_tags = state.tags.write();
        if let Some(tags) = all_tags.get_mut(arn) {
            tags.retain(|t| {
                t.get("Key")
                    .and_then(|k| k.as_str())
                    .map(|k| !keys.iter().any(|x| x == k))
                    .unwrap_or(true)
            });
        }
        AwsResponse::json(200, json!({}))
    }

    // ------------------------------------------------------------------
    // Repository-level configuration
    // ------------------------------------------------------------------

    fn put_image_scanning_config(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("repositoryName")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let config = scan_on_push_config(req.params.get("imageScanningConfiguration"));
        let state = match self.require_repository(req, &name) {
            Ok(s) => s,
            Err(e) => return e,
        };
        if let Some(repo) = state.repositories.write().get_mut(&name) {
            repo["imageScanningConfiguration"] = config.clone();
        }
        AwsResponse::json(
            200,
            json!({
                "registryId": req.account.to_string(),
                "repositoryName": name,
                "imageScanningConfiguration": config,
            }),
        )
    }

    fn put_image_tag_mutability(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("repositoryName")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let mutability = req
            .params
            .get("imageTagMutability")
            .and_then(|v| v.as_str())
            .unwrap_or("MUTABLE");
        let state = match self.require_repository(req, &name) {
            Ok(s) => s,
            Err(e) => return e,
        };
        if let Some(repo) = state.repositories.write().get_mut(&name) {
            repo["imageTagMutability"] = json!(mutability);
        }
        AwsResponse::json(
            200,
            json!({
                "registryId": req.account.to_string(),
                "repositoryName": name,
                "imageTagMutability": mutability,
            }),
        )
    }

    fn get_repository_policy(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("repositoryName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = match self.require_repository(req, name) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let found_policy = state.repository_policies.read().get(name).cloned();
        match found_policy {
            Some(policy) => AwsResponse::json(200, json!({
                "registryId": req.account.to_string(),
                "repositoryName": name,
                "policyText": policy,
            })),
            None => AwsResponse::error(400, "RepositoryPolicyNotFoundException",
                &format!("Repository policy does not exist for the repository with name '{}' in the registry with id '{}'",
                    name, req.account)),
        }
    }

    fn set_repository_policy(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("repositoryName")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let policy = req
            .params
            .get("policyText")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let state = match self.require_repository(req, &name) {
            Ok(s) => s,
            Err(e) => return e,
        };
        state
            .repository_policies
            .write()
            .insert(name.clone(), policy.clone());
        AwsResponse::json(
            200,
            json!({
                "registryId": req.account.to_string(),
                "repositoryName": name,
                "policyText": policy,
            }),
        )
    }

    fn delete_repository_policy(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("repositoryName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = match self.require_repository(req, name) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let removed_policy = state.repository_policies.write().remove(name);
        match removed_policy {
            Some(policy) => AwsResponse::json(200, json!({
                "registryId": req.account.to_string(),
                "repositoryName": name,
                "policyText": policy,
            })),
            None => AwsResponse::error(400, "RepositoryPolicyNotFoundException",
                &format!("Repository policy does not exist for the repository with name '{}' in the registry with id '{}'",
                    name, req.account)),
        }
    }

    // ------------------------------------------------------------------
    // Lifecycle policies
    // ------------------------------------------------------------------

    fn put_lifecycle_policy(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("repositoryName")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let policy = req
            .params
            .get("lifecyclePolicyText")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let state = match self.require_repository(req, &name) {
            Ok(s) => s,
            Err(e) => return e,
        };
        state
            .lifecycle_policies
            .write()
            .insert(name.clone(), policy.clone());
        AwsResponse::json(
            200,
            json!({
                "registryId": req.account.to_string(),
                "repositoryName": name,
                "lifecyclePolicyText": policy,
            }),
        )
    }

    fn get_lifecycle_policy(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("repositoryName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = match self.require_repository(req, name) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let found_policy = state.lifecycle_policies.read().get(name).cloned();
        match found_policy {
            Some(policy) => AwsResponse::json(200, json!({
                "registryId": req.account.to_string(),
                "repositoryName": name,
                "lifecyclePolicyText": policy,
                "lastEvaluatedAt": Utc::now().timestamp(),
            })),
            None => AwsResponse::error(400, "LifecyclePolicyNotFoundException",
                &format!("Lifecycle policy does not exist for the repository with name '{}' in the registry with id '{}'",
                    name, req.account)),
        }
    }

    fn delete_lifecycle_policy(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("repositoryName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = match self.require_repository(req, name) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let removed_policy = state.lifecycle_policies.write().remove(name);
        match removed_policy {
            Some(policy) => AwsResponse::json(200, json!({
                "registryId": req.account.to_string(),
                "repositoryName": name,
                "lifecyclePolicyText": policy,
                "lastEvaluatedAt": Utc::now().timestamp(),
            })),
            None => AwsResponse::error(400, "LifecyclePolicyNotFoundException",
                &format!("Lifecycle policy does not exist for the repository with name '{}' in the registry with id '{}'",
                    name, req.account)),
        }
    }

    fn start_lifecycle_policy_preview(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("repositoryName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = match self.require_repository(req, name) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let policy = req
            .params
            .get("lifecyclePolicyText")
            .and_then(|v| v.as_str())
            .map(String::from)
            .or_else(|| state.lifecycle_policies.read().get(name).cloned());
        match policy {
            Some(p) => AwsResponse::json(200, json!({
                "registryId": req.account.to_string(),
                "repositoryName": name,
                "lifecyclePolicyText": p,
                "status": "IN_PROGRESS",
            })),
            None => AwsResponse::error(400, "LifecyclePolicyNotFoundException",
                &format!("Lifecycle policy does not exist for the repository with name '{}' in the registry with id '{}'",
                    name, req.account)),
        }
    }

    fn get_lifecycle_policy_preview(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("repositoryName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = match self.require_repository(req, name) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let found_policy = state.lifecycle_policies.read().get(name).cloned();
        match found_policy {
            Some(p) => AwsResponse::json(200, json!({
                "registryId": req.account.to_string(),
                "repositoryName": name,
                "lifecyclePolicyText": p,
                "status": "COMPLETE",
                "previewResults": [],
            })),
            None => AwsResponse::error(400, "LifecyclePolicyNotFoundException",
                &format!("Lifecycle policy does not exist for the repository with name '{}' in the registry with id '{}'",
                    name, req.account)),
        }
    }

    // ------------------------------------------------------------------
    // Registry-level operations
    // ------------------------------------------------------------------

    fn describe_registry(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let replication = state.replication_configuration.read().clone();
        AwsResponse::json(
            200,
            json!({
                "registryId": req.account.to_string(),
                "replicationConfiguration": replication,
            }),
        )
    }

    fn put_registry_policy(&self, req: &AwsRequest) -> AwsResponse {
        let policy = req
            .params
            .get("policyText")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let state = self.get_state(req.account, &req.region);
        *state.registry_policy.write() = Some(policy.clone());
        AwsResponse::json(
            200,
            json!({
                "registryId": req.account.to_string(),
                "policyText": policy,
            }),
        )
    }

    fn get_registry_policy(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let found_policy = state.registry_policy.read().clone();
        match found_policy {
            Some(policy) => AwsResponse::json(
                200,
                json!({
                    "registryId": req.account.to_string(),
                    "policyText": policy,
                }),
            ),
            None => AwsResponse::error(
                400,
                "RegistryPolicyNotFoundException",
                &format!(
                    "Registry policy does not exist in the registry with id '{}'",
                    req.account
                ),
            ),
        }
    }

    fn delete_registry_policy(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let removed_policy = state.registry_policy.write().take();
        match removed_policy {
            Some(policy) => AwsResponse::json(
                200,
                json!({
                    "registryId": req.account.to_string(),
                    "policyText": policy,
                }),
            ),
            None => AwsResponse::error(
                400,
                "RegistryPolicyNotFoundException",
                &format!(
                    "Registry policy does not exist in the registry with id '{}'",
                    req.account
                ),
            ),
        }
    }

    fn get_registry_scanning(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let config = state.registry_scanning_configuration.read().clone();
        AwsResponse::json(
            200,
            json!({
                "registryId": req.account.to_string(),
                "scanningConfiguration": config,
            }),
        )
    }

    fn put_registry_scanning(&self, req: &AwsRequest) -> AwsResponse {
        let scan_type = req
            .params
            .get("scanType")
            .and_then(|v| v.as_str())
            .unwrap_or("BASIC");
        let rules = req.params.get("rules").cloned().unwrap_or(json!([]));
        let state = self.get_state(req.account, &req.region);
        {
            let mut reg_cfg = state.registry_scanning_configuration.write();
            *reg_cfg = json!({ "scanType": scan_type, "rules": rules.clone() });
            // Reset per-repository scanFrequency, then apply matching rules.
            let mut configs = state.scanning_configs.write();
            for cfg in configs.values_mut() {
                cfg["scanFrequency"] = json!("MANUAL");
                cfg["appliedScanFilters"] = json!([]);
            }
            if let Some(rules_arr) = rules.as_array() {
                for rule in rules_arr {
                    let freq = rule
                        .get("scanFrequency")
                        .and_then(|v| v.as_str())
                        .unwrap_or("MANUAL");
                    for repo_filter in rule
                        .get("repositoryFilters")
                        .and_then(|f| f.as_array())
                        .cloned()
                        .unwrap_or_default()
                    {
                        let filter = repo_filter
                            .get("filter")
                            .and_then(|f| f.as_str())
                            .unwrap_or_default();
                        for (repo, cfg) in configs.iter_mut() {
                            if repo_filter_matches(filter, repo) {
                                cfg["scanFrequency"] = json!(freq);
                                cfg["appliedScanFilters"] = json!([repo_filter]);
                            }
                        }
                    }
                }
            }
        }
        AwsResponse::json(
            200,
            json!({
                "registryScanningConfiguration": { "scanType": scan_type, "rules": rules },
            }),
        )
    }

    fn batch_get_scanning_config(&self, req: &AwsRequest) -> AwsResponse {
        let names: Vec<String> = req
            .params
            .get("repositoryNames")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let mut configs: Vec<Value> = Vec::new();
        let mut failures: Vec<Value> = Vec::new();
        for n in &names {
            if state.repositories.read().contains_key(n) {
                configs.push(scanning_config(
                    req,
                    n,
                    state.scanning_configs.read().get(n).cloned(),
                ));
            } else {
                failures.push(json!({
                    "repositoryName": n,
                    "failureCode": "REPOSITORY_NOT_FOUND",
                    "failureReason": "REPOSITORY_NOT_FOUND",
                }));
            }
        }
        AwsResponse::json(
            200,
            json!({ "scanningConfigurations": configs, "failures": failures }),
        )
    }

    fn put_replication_configuration(&self, req: &AwsRequest) -> AwsResponse {
        let config = req
            .params
            .get("replicationConfiguration")
            .cloned()
            .unwrap_or(json!({"rules": []}));
        if let Some(rules) = config.get("rules").and_then(|r| r.as_array()) {
            if rules.len() > 1 {
                return AwsResponse::error(400, "ValidationException", "This feature is disabled");
            }
        }
        let state = self.get_state(req.account, &req.region);
        *state.replication_configuration.write() = config.clone();
        AwsResponse::json(200, json!({ "replicationConfiguration": config }))
    }

    fn get_signing_configuration(&self, req: &AwsRequest) -> AwsResponse {
        AwsResponse::json(
            200,
            json!({
                "registryId": req.account.to_string(),
                "signingConfiguration": { "rules": [] },
            }),
        )
    }

    // ------------------------------------------------------------------
    // Pull-through cache rules
    // ------------------------------------------------------------------

    fn create_pull_through_cache_rule(&self, req: &AwsRequest) -> AwsResponse {
        let prefix = req
            .params
            .get("ecrRepositoryPrefix")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let url = req
            .params
            .get("upstreamRegistryUrl")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let mut rules = state.pull_through_cache_rules.write();
        if rules.contains_key(&prefix) {
            return AwsResponse::error(400, "PullThroughCacheRuleAlreadyExistsException",
                &format!("The operation failed because the pull through cache rule with prefix {} already exists",
                    prefix));
        }
        let rule = json!({
            "ecrRepositoryPrefix": prefix,
            "upstreamRegistryUrl": url,
            "upstreamRegistry": req.params.get("upstreamRegistry").and_then(|v| v.as_str()).unwrap_or(""),
            "credentialArn": req.params.get("credentialArn").and_then(|v| v.as_str()).unwrap_or(""),
            "registryId": req.account.to_string(),
            "createdAt": now_epoch(),
        });
        rules.insert(prefix, rule.clone());
        AwsResponse::json(200, rule)
    }

    fn delete_pull_through_cache_rule(&self, req: &AwsRequest) -> AwsResponse {
        let prefix = req
            .params
            .get("ecrRepositoryPrefix")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let mut rules = state.pull_through_cache_rules.write();
        match rules.remove(prefix) {
            Some(rule) => AwsResponse::json(200, rule),
            None => AwsResponse::error(400, "PullThroughCacheRuleNotFoundException",
                &format!("The requested operation failed because the pull through cache rule with prefix {} was not found",
                    prefix)),
        }
    }

    fn update_pull_through_cache_rule(&self, req: &AwsRequest) -> AwsResponse {
        let prefix = req
            .params
            .get("ecrRepositoryPrefix")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let mut rules = state.pull_through_cache_rules.write();
        match rules.get_mut(prefix.as_str()) {
            Some(rule) => {
                if let Some(arn) = req.params.get("credentialArn").and_then(|v| v.as_str()) {
                    if !arn.is_empty() {
                        rule["credentialArn"] = json!(arn);
                    }
                }
                let updated = now_epoch();
                rule["updatedAt"] = json!(updated);
                AwsResponse::json(200, json!({
                    "ecrRepositoryPrefix": prefix,
                    "registryId": req.account.to_string(),
                    "updatedAt": updated,
                    "credentialArn": rule["credentialArn"].clone(),
                }))
            }
            None => AwsResponse::error(400, "PullThroughCacheRuleNotFoundException",
                &format!("The requested operation failed because the pull through cache rule with prefix {} was not found",
                    prefix)),
        }
    }

    fn describe_pull_through_cache_rules(&self, req: &AwsRequest) -> AwsResponse {
        let prefixes: Vec<String> = req
            .params
            .get("ecrRepositoryPrefixes")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let mut all: Vec<Value> = state
            .pull_through_cache_rules
            .read()
            .values()
            .cloned()
            .collect();
        all.sort_by(|a, b| {
            let ka = a
                .get("ecrRepositoryPrefix")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let kb = b
                .get("ecrRepositoryPrefix")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            ka.cmp(kb)
        });
        if !prefixes.is_empty() {
            all.retain(|r| {
                prefixes.iter().any(|p| {
                    p == r
                        .get("ecrRepositoryPrefix")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                })
            });
        }
        AwsResponse::json(200, json!({ "pullThroughCacheRules": all }))
    }

    fn validate_pull_through_cache_rule(&self, req: &AwsRequest) -> AwsResponse {
        let prefix = req
            .params
            .get("ecrRepositoryPrefix")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let rules = state.pull_through_cache_rules.read();
        match rules.get(prefix) {
            Some(rule) => AwsResponse::json(
                200,
                json!({
                    "ecrRepositoryPrefix": prefix,
                    "registryId": req.account.to_string(),
                    "upstreamRegistryUrl": rule.get("upstreamRegistryUrl").cloned().unwrap_or(json!("")),
                    "credentialArn": rule.get("credentialArn").cloned().unwrap_or(json!("")),
                    "isValid": true,
                    "failure": "",
                }),
            ),
            None => {
                let prefix = prefix.to_string();
                drop(rules);
                AwsResponse::error(400, "PullThroughCacheRuleNotFoundException",
                    &format!("The requested operation failed because the pull through cache rule with prefix {} was not found",
                        prefix))
            }
        }
    }

    // ------------------------------------------------------------------
    // Repository creation templates
    // ------------------------------------------------------------------

    fn create_repository_creation_template(&self, req: &AwsRequest) -> AwsResponse {
        let prefix = req
            .params
            .get("prefix")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let mut templates = state.repository_creation_templates.write();
        if templates.contains_key(&prefix) {
            return AwsResponse::error(400, "TemplateAlreadyExistsException",
                &format!("The requested operation failed because the template with prefix {} already exists",
                    prefix));
        }
        let template = json!({
            "prefix": prefix,
            "description": req.params.get("description").and_then(|v| v.as_str()).unwrap_or(""),
            "encryptionConfiguration": req.params.get("encryptionConfiguration")
                .filter(|c| !c.is_null()).cloned().unwrap_or(json!({"encryptionType": "AES256"})),
            "resourceTags": req.params.get("resourceTags").cloned().unwrap_or(json!([])),
            "imageTagMutability": req.params.get("imageTagMutability").and_then(|v| v.as_str()).unwrap_or("MUTABLE"),
            "repositoryPolicy": req.params.get("repositoryPolicy").and_then(|v| v.as_str()).unwrap_or(""),
            "lifecyclePolicy": req.params.get("lifecyclePolicy").and_then(|v| v.as_str()).unwrap_or(""),
            "appliedFor": req.params.get("appliedFor").cloned().unwrap_or(json!([])),
            "registryId": req.account.to_string(),
            "createdAt": now_epoch(),
            "updatedAt": now_epoch(),
        });
        templates.insert(prefix, template.clone());
        AwsResponse::json(
            200,
            json!({
                "registryId": req.account.to_string(),
                "repositoryCreationTemplate": template,
            }),
        )
    }

    fn delete_repository_creation_template(&self, req: &AwsRequest) -> AwsResponse {
        let prefix = req
            .params
            .get("prefix")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let mut templates = state.repository_creation_templates.write();
        match templates.remove(prefix) {
            Some(template) => AwsResponse::json(200, json!({
                "registryId": req.account.to_string(),
                "repositoryCreationTemplate": template,
            })),
            None => AwsResponse::error(400, "TemplateNotFoundException",
                &format!("The requested operation failed because the template with prefix {} was not found",
                    prefix)),
        }
    }

    fn update_repository_creation_template(&self, req: &AwsRequest) -> AwsResponse {
        let prefix = req
            .params
            .get("prefix")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let mut templates = state.repository_creation_templates.write();
        match templates.get_mut(prefix) {
            Some(template) => {
                for (param, field) in [
                    ("description", "description"),
                    ("resourceTags", "resourceTags"),
                    ("imageTagMutability", "imageTagMutability"),
                    ("repositoryPolicy", "repositoryPolicy"),
                    ("lifecyclePolicy", "lifecyclePolicy"),
                    ("appliedFor", "appliedFor"),
                ] {
                    if let Some(v) = req.params.get(param) {
                        template[field] = v.clone();
                    }
                }
                if let Some(enc) = req.params.get("encryptionConfiguration") {
                    if !enc.is_null() {
                        template["encryptionConfiguration"] = enc.clone();
                    }
                }
                template["updatedAt"] = json!(now_epoch());
                let updated = template.clone();
                AwsResponse::json(200, json!({
                    "registryId": req.account.to_string(),
                    "repositoryCreationTemplate": updated,
                }))
            }
            None => AwsResponse::error(400, "TemplateNotFoundException",
                &format!("The requested operation failed because the template with prefix {} was not found",
                    prefix)),
        }
    }

    fn describe_repository_creation_templates(&self, req: &AwsRequest) -> AwsResponse {
        let prefixes: Vec<String> = req
            .params
            .get("prefixes")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let mut all: Vec<Value> = state
            .repository_creation_templates
            .read()
            .values()
            .cloned()
            .collect();
        all.sort_by(|a, b| {
            let ka = a.get("prefix").and_then(|v| v.as_str()).unwrap_or("");
            let kb = b.get("prefix").and_then(|v| v.as_str()).unwrap_or("");
            ka.cmp(kb)
        });
        if !prefixes.is_empty() {
            all.retain(|t| {
                prefixes
                    .iter()
                    .any(|p| p == t.get("prefix").and_then(|v| v.as_str()).unwrap_or(""))
            });
        }
        AwsResponse::json(
            200,
            json!({
                "registryId": req.account.to_string(),
                "repositoryCreationTemplates": all,
            }),
        )
    }

    // ------------------------------------------------------------------
    // Account settings
    // ------------------------------------------------------------------

    fn get_account_setting(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let value = state
            .account_settings
            .read()
            .get(name)
            .cloned()
            .unwrap_or_default();
        AwsResponse::json(200, json!({ "name": name, "value": value }))
    }

    fn put_account_setting(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let value = req
            .params
            .get("value")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        state
            .account_settings
            .write()
            .insert(name.to_string(), value.to_string());
        AwsResponse::json(200, json!({ "name": name, "value": value }))
    }

    // ------------------------------------------------------------------
    // Image scans & replication status
    // ------------------------------------------------------------------

    fn find_image(images: &[EcrImage], req_image_id: &Value) -> Option<usize> {
        let tag = req_image_id.get("imageTag").and_then(|v| v.as_str());
        let digest = req_image_id.get("imageDigest").and_then(|v| v.as_str());
        images.iter().position(|img| {
            (tag.map(|t| img.tags.iter().any(|x| x == t)).unwrap_or(true))
                && (digest.map(|d| img.digest == d).unwrap_or(true))
                && (tag.is_some() || digest.is_some())
        })
    }

    fn start_image_scan(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("repositoryName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = match self.require_repository(req, name) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let image_id = req.params.get("imageId").cloned().unwrap_or(json!({}));
        let mut images = state.images.write();
        let repo_images = images.entry(name.to_string()).or_insert_with(Vec::new);
        match Self::find_image(repo_images, &image_id) {
            Some(idx) => {
                let today = Utc::now()
                    .date_naive()
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
                    .and_utc()
                    .timestamp();
                if repo_images[idx].scanned_at == Some(today) {
                    return AwsResponse::error(
                        400,
                        "LimitExceededException",
                        "The requested operation exceeds the service quota for scanning images",
                    );
                }
                repo_images[idx].scanned_at = Some(today);
                AwsResponse::json(
                    200,
                    json!({
                        "registryId": req.account.to_string(),
                        "repositoryName": name,
                        "imageId": image_id_shape(&repo_images[idx]),
                        "imageScanStatus": { "status": "IN_PROGRESS" },
                    }),
                )
            }
            None => image_not_found(
                image_id.get("imageDigest").and_then(|v| v.as_str()),
                image_id.get("imageTag").and_then(|v| v.as_str()),
            ),
        }
    }

    fn describe_scan_findings(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("repositoryName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = match self.require_repository(req, name) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let image_id = req.params.get("imageId").cloned().unwrap_or(json!({}));
        let images = state.images.read();
        let repo_images = images.get(name).cloned().unwrap_or_default();
        match Self::find_image(&repo_images, &image_id) {
            Some(idx) if repo_images[idx].scanned_at.is_some() => {
                AwsResponse::json(200, json!({
                    "registryId": req.account.to_string(),
                    "repositoryName": name,
                    "imageId": image_id_shape(&repo_images[idx]),
                    "imageScanStatus": {
                        "status": "COMPLETE",
                        "description": "The scan was completed successfully.",
                    },
                    "imageScanFindings": {
                        "imageScanCompletedAt": iso_8601_now(),
                        "vulnerabilitySourceUpdatedAt": iso_8601_now(),
                        "findings": [{
                            "name": "CVE-9999-9999",
                            "uri": "https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-9999-9999",
                            "severity": "HIGH",
                            "attributes": [
                                {"key": "package_version", "value": "9.9.9"},
                                {"key": "package_name", "value": "moto_fake"},
                                {"key": "CVSS2_VECTOR", "value": "AV:N/AC:L/Au:N/C:P/I:P/A:P"},
                                {"key": "CVSS2_SCORE", "value": "7.5"},
                            ],
                        }],
                        "findingSeverityCounts": { "HIGH": 1 },
                    },
                }))
            }
            Some(_) => AwsResponse::error(400, "ScanNotFoundException",
                &format!("Image scan does not exist for image with id {{imageDigest:'{}', imageTag:'{}'}} in repository '{}' in registry '{}'",
                    image_id.get("imageDigest").and_then(|v| v.as_str()).unwrap_or("null"),
                    image_id.get("imageTag").and_then(|v| v.as_str()).unwrap_or("null"),
                    name, req.account)),
            None => image_not_found(
                image_id.get("imageDigest").and_then(|v| v.as_str()),
                image_id.get("imageTag").and_then(|v| v.as_str())),
        }
    }

    fn describe_image_replication_status(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("repositoryName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = match self.require_repository(req, name) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let image_id = req.params.get("imageId").cloned().unwrap_or(json!({}));
        let images = state.images.read();
        let repo_images = images.get(name).cloned().unwrap_or_default();
        match Self::find_image(&repo_images, &image_id) {
            Some(idx) => AwsResponse::json(
                200,
                json!({
                    "repositoryName": name,
                    "imageId": image_id_shape(&repo_images[idx]),
                    "replicationStatuses": [],
                }),
            ),
            None => image_not_found(
                image_id.get("imageDigest").and_then(|v| v.as_str()),
                image_id.get("imageTag").and_then(|v| v.as_str()),
            ),
        }
    }

    fn image_detail(&self, req: &AwsRequest, img: &EcrImage) -> Value {
        let mut detail = json!({
            "registryId": req.account.to_string(),
            "repositoryName": req.params.get("repositoryName").and_then(|v| v.as_str()).unwrap_or(""),
            "imageDigest": img.digest,
            "imageSizeInBytes": img.size_in_bytes,
            "imagePushedAt": img.pushed_at,
            "imageManifestMediaType": img.manifest_media_type,
        });
        if !img.tags.is_empty() {
            detail["imageTags"] = json!(img.tags);
        }
        detail
    }
}

// ----------------------------------------------------------------------
// Local helpers
// ----------------------------------------------------------------------

fn now_epoch() -> f64 {
    Utc::now().timestamp_millis() as f64 / 1000.0
}

fn iso_8601_now() -> String {
    Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

fn is_valid_digest(d: &str) -> bool {
    let (scheme, hex_part) = match d.split_once(':') {
        Some(v) => v,
        None => return false,
    };
    !scheme.is_empty()
        && scheme
            .chars()
            .all(|c| "0123456789abcdefABCDEF_+.-".contains(c))
        && hex_part.len() == 64
        && hex_part.chars().all(|c| c.is_ascii_hexdigit())
}

fn parse_repo_arn(value: Option<&str>) -> Option<(&str, String)> {
    let arn = value?;
    let repo_name = arn.split("repository/").nth(1)?;
    if arn.starts_with("arn:") {
        Some((arn, repo_name.to_string()))
    } else {
        None
    }
}

fn repo_not_found(req: &AwsRequest, name: &str) -> AwsResponse {
    AwsResponse::error(
        400,
        "RepositoryNotFoundException",
        &format!(
            "The repository with name '{}' does not exist in the registry with id '{}'",
            name, req.account
        ),
    )
}

fn image_not_found(digest: Option<&str>, tag: Option<&str>) -> AwsResponse {
    AwsResponse::error(
        400,
        "ImageNotFoundException",
        &format!(
            "Image with id {{imageDigest:'{}', imageTag:'{}'}} not found",
            digest.unwrap_or("null"),
            tag.unwrap_or("null")
        ),
    )
}

fn image_not_found_payload(digest: Option<String>, tag: Option<String>) -> Value {
    let mut image_id = json!({});
    if let Some(d) = digest {
        image_id["imageDigest"] = json!(d);
    }
    if let Some(t) = tag {
        image_id["imageTag"] = json!(t);
    }
    json!({
        "imageId": image_id,
        "failureCode": "ImageNotFound",
        "failureReason": "Requested image not found",
    })
}

/// The primary `imageId` pair in moto: `imageTag` is the image's current tag.
fn image_id_shape(img: &EcrImage) -> Value {
    json!({
        "imageDigest": img.digest,
        "imageTag": img.tags.last().cloned().unwrap_or_default(),
    })
}

/// `scanOnPush` wrapper; the inner value only.
fn scan_on_push_config(param: Option<&Value>) -> Value {
    let scan = param
        .and_then(|v| v.get("scanOnPush"))
        .cloned()
        .unwrap_or(json!(false));
    json!({ "scanOnPush": scan })
}

fn image_summary(req: &AwsRequest, img: &EcrImage) -> Value {
    json!({
        "registryId": req.account.to_string(),
        "repositoryName": req.params.get("repositoryName").and_then(|v| v.as_str()).unwrap_or(""),
        "imageId": {
            "imageDigest": img.digest,
            "imageTag": img.tags.last().cloned().unwrap_or_default(),
        },
        "imageManifest": img.manifest,
        "imageManifestMediaType": img.manifest_media_type,
    })
}

fn images_of(state: &EcrState, name: &str) -> Vec<EcrImage> {
    state.images.read().get(name).cloned().unwrap_or_default()
}

fn scanning_config(req: &AwsRequest, name: &str, existing: Option<Value>) -> Value {
    let mut cfg = existing.unwrap_or(json!({}));
    if cfg.get("scanFrequency").is_none() {
        cfg["scanFrequency"] = json!("MANUAL");
    }
    if cfg.get("appliedScanFilters").is_none() {
        cfg["appliedScanFilters"] = json!([]);
    }
    json!({
        "repositoryArn": format!("arn:aws:ecr:{}:{}:repository/{}", req.region, req.account, name),
        "repositoryName": name,
        "scanOnPush": cfg.get("scanOnPush").cloned().unwrap_or(json!(false)),
        "scanFrequency": cfg["scanFrequency"],
        "appliedScanFilters": cfg["appliedScanFilters"],
    })
}

/// moto's `_match_repository_filter`: substring match or `*`-wildcard glob.
fn repo_filter_matches(filter: &str, repo: &str) -> bool {
    if repo.contains(filter) {
        return true;
    }
    if filter.contains('*') {
        let (prefix, suffix) = filter.split_once('*').unwrap_or((filter, ""));
        return repo.starts_with(prefix) && repo.ends_with(suffix);
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use serde_json::json;

    fn make_req(operation: &str, params: Value) -> AwsRequest {
        AwsRequest {
            service: "ecr".to_string(),
            operation: operation.to_string(),
            account: 123456789012,
            region: "us-east-1".to_string(),
            params,
            body: Bytes::new(),
        }
    }

    fn manifest(tag_seed: &str) -> String {
        let digest = format!("sha256:{}", tag_seed);
        json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
            "config": {"mediaType": "a", "size": 7023, "digest": digest},
            "layers": [],
        })
        .to_string()
    }

    #[test]
    fn repo_crud_and_errors() {
        let h = EcrHandler::new();
        assert_eq!(
            h.handle(make_req("CreateRepository", json!({"repositoryName": "a"})))
                .status,
            200
        );
        let dup = h.handle(make_req("CreateRepository", json!({"repositoryName": "a"})));
        assert!(dup.body.contains("RepositoryAlreadyExistsException"));
        let missing = h.handle(make_req(
            "DescribeRepositories",
            json!({"repositoryNames": ["nope"]}),
        ));
        assert!(missing.body.contains("RepositoryNotFoundException"));
    }

    #[test]
    fn lifecycle_policy_paths() {
        let h = EcrHandler::new();
        h.handle(make_req("CreateRepository", json!({"repositoryName": "a"})));
        let none = h.handle(make_req(
            "GetLifecyclePolicy",
            json!({"repositoryName": "a"}),
        ));
        assert!(none.body.contains("LifecyclePolicyNotFoundException"));
        h.handle(make_req(
            "PutLifecyclePolicy",
            json!({"repositoryName": "a", "lifecyclePolicyText": "{}"}),
        ));
        let got = h.handle(make_req(
            "GetLifecyclePolicy",
            json!({"repositoryName": "a"}),
        ));
        assert!(got.body.contains("lifecyclePolicyText"));
    }

    #[test]
    fn put_list_describe_images() {
        let h = EcrHandler::new();
        h.handle(make_req("CreateRepository", json!({"repositoryName": "r"})));
        let put = h.handle(make_req(
            "PutImage",
            json!({
                "repositoryName": "r", "imageManifest": manifest("seed"), "imageTag": "t1"
            }),
        ));
        assert_eq!(put.status, 200);
        let describe = h.handle(make_req("DescribeImages", json!({"repositoryName": "r"})));
        assert!(describe.body.contains("imageTags"));
        let batch = h.handle(make_req(
            "BatchGetImage",
            json!({
                "repositoryName": "r", "imageIds": [{"imageTag": "t1"}]
            }),
        ));
        assert!(batch.body.contains("bg") || batch.body.contains("imageId"));
        let missing = h.handle(make_req(
            "BatchGetImage",
            json!({
                "repositoryName": "r", "imageIds": [{"imageTag": "nope"}]
            }),
        ));
        assert!(missing.body.contains("ImageNotFound"));
    }
}
