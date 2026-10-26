//! ECS operation handler.
//!
//! Mirrors `src/robotocore/services/ecs/provider.py` (the Python native
//! provider) for the operations it serves, and Moto's `ecs` backend for the
//! rest, so the Rust twin produces the same shapes the ground truth produces.

use parking_lot::RwLock;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::UNIX_EPOCH;

use crate::models::EcsState;
use crate::protocol::{AwsRequest, AwsResponse};

/// Valid account setting names (moto's list).
const ACCOUNT_SETTING_NAMES: &[&str] = &[
    "serviceLongArnFormat",
    "taskLongArnFormat",
    "containerInstanceLongArnFormat",
    "containerLongArnFormat",
    "awsvpcTrunking",
    "containerInsights",
    "dualStackIPv6",
];

pub struct EcsHandler {
    pub(crate) state: RwLock<HashMap<(u64, String), EcsState>>,
}

impl Default for EcsHandler {
    fn default() -> Self {
        Self {
            state: RwLock::new(HashMap::new()),
        }
    }
}

impl EcsHandler {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn get_state(&self, account: u64, region: &str) -> EcsState {
        let mut states = self.state.write();
        let state = states
            .entry((account, region.to_string()))
            .or_default()
            .clone();
        // Expose the live state to other crates via the bridge.
        crate::bridge::register_state(account, region, &state);
        state
    }

    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        match req.operation.as_str() {
            "CreateCluster" => self.create_cluster(&req),
            "DeleteCluster" => self.delete_cluster(&req),
            "DescribeClusters" => self.describe_clusters(&req),
            "ListClusters" => self.list_clusters(&req),
            "UpdateCluster" => self.update_cluster(&req),
            "UpdateClusterSettings" => self.update_cluster_settings(&req),
            "PutClusterCapacityProviders" => self.put_cluster_capacity_providers(&req),
            "CreateCapacityProvider" => self.create_capacity_provider(&req),
            "DescribeCapacityProviders" => self.describe_capacity_providers(&req),
            "UpdateCapacityProvider" => self.update_capacity_provider(&req),
            "DeleteCapacityProvider" => self.delete_capacity_provider(&req),
            "RegisterTaskDefinition" => self.register_task_definition(&req),
            "DescribeTaskDefinition" => self.describe_task_definition(&req),
            "ListTaskDefinitions" => self.list_task_definitions(&req),
            "ListTaskDefinitionFamilies" => self.list_task_definition_families(&req),
            "DeregisterTaskDefinition" => self.deregister_task_definition(&req),
            "DeleteTaskDefinitions" => self.delete_task_definitions(&req),
            "CreateService" => self.create_service(&req),
            "DeleteService" => self.delete_service(&req),
            "DescribeServices" => self.describe_services(&req),
            "ListServices" => self.list_services(&req),
            "UpdateService" => self.update_service(&req),
            "UpdateServicePrimaryTaskSet" => self.update_service_primary_task_set(&req),
            "RunTask" => self.run_task(&req),
            "StartTask" => self.start_task(&req),
            "StopTask" => self.stop_task(&req),
            "DescribeTasks" => self.describe_tasks(&req),
            "ListTasks" => self.list_tasks(&req),
            "RegisterContainerInstance" => self.register_container_instance(&req),
            "DeregisterContainerInstance" => self.deregister_container_instance(&req),
            "DescribeContainerInstances" => self.describe_container_instances(&req),
            "ListContainerInstances" => self.list_container_instances(&req),
            "UpdateContainerInstancesState" => self.update_container_instances_state(&req),
            "UpdateContainerAgent" => self.update_container_agent(&req),
            "CreateTaskSet" => self.create_task_set(&req),
            "DescribeTaskSets" => self.describe_task_sets(&req),
            "UpdateTaskSet" => self.update_task_set(&req),
            "DeleteTaskSet" => self.delete_task_set(&req),
            "PutAttributes" => self.put_attributes(&req),
            "DeleteAttributes" => self.delete_attributes(&req),
            "ListAttributes" => self.list_attributes(&req),
            "ListAccountSettings" => self.list_account_settings(&req),
            "PutAccountSetting" => self.put_account_setting(&req),
            "PutAccountSettingDefault" => self.put_account_setting_default(&req),
            "DeleteAccountSetting" => self.delete_account_setting(&req),
            "TagResource" => self.tag_resource(&req),
            "UntagResource" => self.untag_resource(&req),
            "ListTagsForResource" => self.list_tags(&req),
            "ListServiceDeployments" => self.list_service_deployments(&req),
            "DescribeServiceDeployments" => self.describe_service_deployments(&req),
            "DescribeServiceRevisions" => self.describe_service_revisions(&req),
            "SubmitContainerStateChange"
            | "SubmitTaskStateChange"
            | "SubmitAttachmentStateChanges" => {
                AwsResponse::json(200, json!({ "acknowledgment": "ACCEPT" }))
            }
            "ExecuteCommand" => self.execute_command(&req),
            "GetTaskProtection" => self.get_task_protection(&req),
            "UpdateTaskProtection" => self.update_task_protection(&req),
            "ListServicesByNamespace" => AwsResponse::json(200, json!({ "serviceArns": [] })),
            "StopServiceDeployment" => AwsResponse::json(200, json!({})),
            "CreateExpressGatewayService"
            | "DeleteExpressGatewayService"
            | "DescribeExpressGatewayService"
            | "UpdateExpressGatewayService" => AwsResponse::json(200, json!({})),
            "DiscoverPollEndpoint" => AwsResponse::json(
                200,
                json!({
                    "endpoint": "http://localhost:4567",
                    "agentArgs": [],
                    "agentConfig": {},
                }),
            ),
            other => AwsResponse::error(
                400,
                "InvalidAction",
                &format!("Could not find operation {}", other),
            ),
        }
    }

    // ------------------------------------------------------------------
    // Clusters
    // ------------------------------------------------------------------

    fn create_cluster(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("clusterName")
            .and_then(|v| v.as_str())
            .unwrap_or("default")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let settings: Vec<Value> = req
            .params
            .get("settings")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let tags: Vec<Value> = req
            .params
            .get("tags")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let providers: Vec<Value> = req
            .params
            .get("capacityProviders")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let arn = format!(
            "arn:aws:ecs:{}:{}:cluster/{}",
            req.region, req.account, name
        );
        let cluster = json!({
            "clusterArn": arn.clone(),
            "clusterName": name,
            "status": "ACTIVE",
            "registeredContainerInstancesCount": 0,
            "runningTasksCount": 0,
            "pendingTasksCount": 0,
            "activeServicesCount": 0,
            "settings": settings,
            "capacityProviders": providers,
            "tags": tags.clone(),
        });
        if !tags.is_empty() {
            state.tags.write().insert(arn, json!(tags));
        }
        state.clusters.write().insert(name, cluster.clone());
        AwsResponse::json(200, json!({ "cluster": cluster }))
    }

    fn delete_cluster(&self, req: &AwsRequest) -> AwsResponse {
        let name = resolve_name(
            req.params
                .get("cluster")
                .and_then(|v| v.as_str())
                .unwrap_or_default(),
        );
        let state = self.get_state(req.account, &req.region);
        let cluster = state.clusters.read().get(name).cloned();
        let cluster = match cluster {
            Some(c) => c,
            None => return cluster_not_found(name),
        };
        let arn = cluster["clusterArn"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        // Cascade: remove child services/tasks and their tags, then the cluster's.
        {
            let mut services = state.services.write();
            if let Some(svcs) = services.remove(name) {
                let mut tags = state.tags.write();
                for svc in svcs.values() {
                    tags.remove(
                        svc.get("serviceArn")
                            .and_then(|a| a.as_str())
                            .unwrap_or_default(),
                    );
                }
            }
        }
        {
            let mut tasks = state.tasks.write();
            if let Some(tsk) = tasks.remove(name) {
                let mut tags = state.tags.write();
                for task in tsk.values() {
                    tags.remove(
                        task.get("taskArn")
                            .and_then(|a| a.as_str())
                            .unwrap_or_default(),
                    );
                }
            }
        }
        state.tags.write().remove(&arn);
        let mut cluster = cluster;
        cluster["status"] = json!("INACTIVE");
        state.clusters.write().remove(name);
        AwsResponse::json(200, json!({ "cluster": cluster }))
    }

    fn describe_clusters(&self, req: &AwsRequest) -> AwsResponse {
        let ids: Vec<String> = req
            .params
            .get("clusters")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let mut found: Vec<Value> = Vec::new();
        let mut failures: Vec<Value> = Vec::new();
        let clusters = state.clusters.read();
        for cid in ids {
            let name = resolve_name(&cid);
            match clusters.get(name) {
                Some(cluster) => {
                    let mut c = cluster.clone();
                    c["activeServicesCount"] = json!(state
                        .services
                        .read()
                        .get(name)
                        .map(|m| m.len())
                        .unwrap_or(0));
                    c["runningTasksCount"] = json!(state
                        .tasks
                        .read()
                        .get(name)
                        .map(|m| m
                            .values()
                            .filter(|t| t["lastStatus"] == json!("RUNNING"))
                            .count())
                        .unwrap_or(0));
                    found.push(c);
                }
                None => failures.push(json!({ "arn": cid, "reason": "MISSING" })),
            }
        }
        AwsResponse::json(200, json!({ "clusters": found, "failures": failures }))
    }

    fn list_clusters(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let arns: Vec<Value> = state
            .clusters
            .read()
            .values()
            .map(|c| c["clusterArn"].clone())
            .collect();
        AwsResponse::json(200, json!({ "clusterArns": arns }))
    }

    fn update_cluster_common(&self, req: &AwsRequest, apply_settings: bool) -> AwsResponse {
        let name = resolve_name(
            req.params
                .get("cluster")
                .and_then(|v| v.as_str())
                .unwrap_or_default(),
        );
        let state = self.get_state(req.account, &req.region);
        let updated = {
            let mut clusters = state.clusters.write();
            // Match under the write lock: a concurrent DeleteCluster could
            // remove the entry between a check and this lookup.
            let Some(cluster) = clusters.get_mut(name) else {
                return cluster_not_found(name);
            };
            if apply_settings {
                if let Some(settings) = req.params.get("settings") {
                    cluster["settings"] = settings.clone();
                }
                if let Some(configuration) = req.params.get("configuration") {
                    cluster["configuration"] = configuration.clone();
                }
            } else {
                cluster["settings"] = req.params.get("settings").cloned().unwrap_or(json!([]));
            }
            cluster.clone()
        };
        AwsResponse::json(200, json!({ "cluster": updated }))
    }

    fn update_cluster(&self, req: &AwsRequest) -> AwsResponse {
        self.update_cluster_common(req, true)
    }

    fn update_cluster_settings(&self, req: &AwsRequest) -> AwsResponse {
        self.update_cluster_common(req, true)
    }

    fn put_cluster_capacity_providers(&self, req: &AwsRequest) -> AwsResponse {
        let name = resolve_name(
            req.params
                .get("cluster")
                .and_then(|v| v.as_str())
                .unwrap_or_default(),
        );
        let state = self.get_state(req.account, &req.region);
        let updated = {
            let providers = req
                .params
                .get("capacityProviders")
                .cloned()
                .unwrap_or(json!([]));
            let strategy = req
                .params
                .get("defaultCapacityProviderStrategy")
                .cloned()
                .unwrap_or(json!([]));
            let mut clusters = state.clusters.write();
            // Match under the write lock: a concurrent DeleteCluster could
            // remove the entry between a check and this lookup.
            let Some(cluster) = clusters.get_mut(name) else {
                return cluster_not_found(name);
            };
            cluster["capacityProviders"] = providers;
            cluster["defaultCapacityProviderStrategy"] = strategy;
            cluster.clone()
        };
        AwsResponse::json(200, json!({ "cluster": updated }))
    }

    // ------------------------------------------------------------------
    // Capacity providers
    // ------------------------------------------------------------------

    fn create_capacity_provider(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let mut asg = req
            .params
            .get("autoScalingGroupProvider")
            .cloned()
            .unwrap_or(json!({}));
        normalize_asg(&mut asg);
        let cp = json!({
            "capacityProviderArn": format!("arn:aws:ecs:{}:{}:capacity-provider/{}",
                req.region, req.account, name),
            "name": name,
            "status": "ACTIVE",
            "autoScalingGroupProvider": asg,
        });
        state.capacity_providers.write().insert(name, cp.clone());
        AwsResponse::json(200, json!({ "capacityProvider": cp }))
    }

    fn describe_capacity_providers(&self, req: &AwsRequest) -> AwsResponse {
        let names: Vec<String> = req
            .params
            .get("capacityProviders")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let mut found: Vec<Value> = Vec::new();
        let mut failures: Vec<Value> = Vec::new();
        let cps = state.capacity_providers.read();
        let names = if names.is_empty() {
            cps.keys().cloned().collect::<Vec<_>>()
        } else {
            names
        };
        for n in names {
            match cps.get(&n) {
                Some(cp) => found.push(cp.clone()),
                None => failures.push(json!({
                    "arn": format!("arn:aws:ecs:{}:{}:capacity-provider/{}", req.region, req.account, n),
                    "reason": "MISSING",
                })),
            }
        }
        AwsResponse::json(
            200,
            json!({ "capacityProviders": found, "failures": failures }),
        )
    }

    fn delete_capacity_provider(&self, req: &AwsRequest) -> AwsResponse {
        let reference = req
            .params
            .get("capacityProvider")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let name = resolve_name(reference);
        let state = self.get_state(req.account, &req.region);
        let mut cps = state.capacity_providers.write();
        match cps.get(name).cloned() {
            Some(cp) => {
                cps.remove(name);
                let _ = reference;
                AwsResponse::json(200, json!({ "capacityProvider": cp }))
            }
            None => AwsResponse::error(
                400,
                "CapacityProviderNotFoundException",
                &format!(
                    "The specified capacity provider {} was not found",
                    reference
                ),
            ),
        }
    }

    fn update_capacity_provider(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let asg = req
            .params
            .get("autoScalingGroupProvider")
            .cloned()
            .unwrap_or(json!({}));
        let updated = {
            let mut cps = state.capacity_providers.write();
            // Match under the write lock: a concurrent
            // DeleteCapacityProvider could remove the entry between a check
            // and this lookup.
            let Some(cp) = cps.get_mut(name) else {
                return AwsResponse::error(
                    400,
                    "CapacityProviderNotFoundException",
                    &format!("The specified capacity provider {} was not found", name),
                );
            };
            merge_asg(cp, &asg);
            cp.clone()
        };
        AwsResponse::json(200, json!({ "capacityProvider": updated }))
    }

    // ------------------------------------------------------------------
    // Task definitions
    // ------------------------------------------------------------------

    fn register_task_definition(&self, req: &AwsRequest) -> AwsResponse {
        let family = req
            .params
            .get("family")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        if family.is_empty() {
            return AwsResponse::error(400, "ClientException", "family is required.");
        }
        let state = self.get_state(req.account, &req.region);
        let td_arn;
        let tags: Vec<Value>;
        let td = {
            let mut tds = state.task_definitions.write();
            let revisions = tds.entry(family.to_string()).or_default();
            let revision = revisions.len() + 1;
            td_arn = format!(
                "arn:aws:ecs:{}:{}:task-definition/{}:{}",
                req.region, req.account, family, revision
            );
            tags = req
                .params
                .get("tags")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            let td = json!({
                "taskDefinitionArn": td_arn.clone(),
                "family": family,
                "revision": revision,
                "status": "ACTIVE",
                "containerDefinitions": req.params.get("containerDefinitions").cloned().unwrap_or(json!([])),
                "cpu": req.params.get("cpu").and_then(|v| v.as_str()).unwrap_or("256"),
                "memory": req.params.get("memory").and_then(|v| v.as_str()).unwrap_or("512"),
                "networkMode": req.params.get("networkMode").and_then(|v| v.as_str()).unwrap_or("awsvpc"),
                "requiresCompatibilities": req.params.get("requiresCompatibilities")
                    .cloned().unwrap_or(json!(["FARGATE"])),
                "executionRoleArn": req.params.get("executionRoleArn").and_then(|v| v.as_str()).unwrap_or(""),
                "taskRoleArn": req.params.get("taskRoleArn").and_then(|v| v.as_str()).unwrap_or(""),
                "volumes": req.params.get("volumes").cloned().unwrap_or(json!([])),
                "tags": tags.clone(),
            });
            revisions.push(td.clone());
            td
        };
        if !tags.is_empty() {
            state.tags.write().insert(td_arn, json!(tags));
        }
        AwsResponse::json(
            200,
            json!({
                "taskDefinition": td.clone(),
                "tags": td["tags"].clone(),
            }),
        )
    }

    fn describe_task_definition(&self, req: &AwsRequest) -> AwsResponse {
        let td_ref = req
            .params
            .get("taskDefinition")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        match resolve_task_definition(&state, td_ref) {
            Some(td) => AwsResponse::json(
                200,
                json!({
                    "taskDefinition": td.clone(),
                    "tags": td.get("tags").cloned().unwrap_or(json!([])),
                }),
            ),
            None => AwsResponse::error(
                400,
                "ClientException",
                &format!("Unable to describe task definition {}.", td_ref),
            ),
        }
    }

    fn list_task_definitions(&self, req: &AwsRequest) -> AwsResponse {
        let prefix = req
            .params
            .get("familyPrefix")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let status = req
            .params
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("ACTIVE");
        let state = self.get_state(req.account, &req.region);
        let tds = state.task_definitions.read();
        let mut arns: Vec<String> = Vec::new();
        for (family, revisions) in tds.iter() {
            if !prefix.is_empty() && !family.starts_with(prefix) {
                continue;
            }
            for td in revisions {
                if td.get("status").and_then(|v| v.as_str()) == Some(status) {
                    arns.push(
                        td["taskDefinitionArn"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string(),
                    );
                }
            }
        }
        arns.sort();
        AwsResponse::json(200, json!({ "taskDefinitionArns": arns }))
    }

    fn list_task_definition_families(&self, req: &AwsRequest) -> AwsResponse {
        let prefix = req
            .params
            .get("familyPrefix")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let status = req
            .params
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("ACTIVE");
        let state = self.get_state(req.account, &req.region);
        let tds = state.task_definitions.read();
        let mut families: Vec<String> = Vec::new();
        for (family, revisions) in tds.iter() {
            if !prefix.is_empty() && !family.starts_with(prefix) {
                continue;
            }
            let has_status = |want: &str| {
                revisions
                    .iter()
                    .any(|td| td.get("status").and_then(|v| v.as_str()) == Some(want))
            };
            match status {
                "ACTIVE" => {
                    if has_status("ACTIVE") {
                        families.push(family.clone());
                    }
                }
                "INACTIVE" => {
                    if has_status("INACTIVE") {
                        families.push(family.clone());
                    }
                }
                _ => families.push(family.clone()),
            }
        }
        families.sort();
        families.dedup();
        AwsResponse::json(200, json!({ "families": families }))
    }

    fn deregister_task_definition(&self, req: &AwsRequest) -> AwsResponse {
        let td_ref = req
            .params
            .get("taskDefinition")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let found = resolve_task_definition(&state, td_ref);
        let td = match found {
            None => {
                return AwsResponse::error(
                    400,
                    "ClientException",
                    &format!("Unable to deregister task definition {}.", td_ref),
                )
            }
            Some(td) => td,
        };
        let family = td["family"].as_str().unwrap_or_default().to_string();
        let revision = td["revision"].as_i64().unwrap_or(0);
        {
            let mut tds = state.task_definitions.write();
            if let Some(revisions) = tds.get_mut(&family) {
                for rev in revisions.iter_mut() {
                    if rev["revision"].as_i64() == Some(revision) {
                        rev["status"] = json!("INACTIVE");
                    }
                }
            }
        }
        let mut updated = td;
        updated["status"] = json!("INACTIVE");
        AwsResponse::json(200, json!({ "taskDefinition": updated }))
    }

    fn delete_task_definitions(&self, req: &AwsRequest) -> AwsResponse {
        let refs: Vec<String> = req
            .params
            .get("taskDefinitions")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let mut deleted: Vec<Value> = Vec::new();
        let mut failures: Vec<Value> = Vec::new();
        for td_ref in &refs {
            let found = resolve_task_definition(&state, td_ref);
            match found {
                Some(td) => {
                    let family = td["family"].as_str().unwrap_or_default().to_string();
                    let revision = td["revision"].as_i64().unwrap_or(0);
                    let mut tds = state.task_definitions.write();
                    if let Some(revisions) = tds.get_mut(&family) {
                        for rev in revisions.iter_mut() {
                            if rev["revision"].as_i64() == Some(revision) {
                                rev["status"] = json!("DELETE_IN_PROGRESS");
                            }
                        }
                    }
                    let mut updated = td;
                    updated["status"] = json!("DELETE_IN_PROGRESS");
                    deleted.push(updated);
                }
                None => failures.push(json!({
                    "arn": td_ref,
                    "reason": "The specified task definition does not exist. Specify a valid account, family, revision and try again.",
                })),
            }
        }
        AwsResponse::json(
            200,
            json!({ "taskDefinitions": deleted, "failures": failures }),
        )
    }

    // ------------------------------------------------------------------
    // Services
    // ------------------------------------------------------------------

    fn create_service(&self, req: &AwsRequest) -> AwsResponse {
        let cluster_name = resolve_name(
            req.params
                .get("cluster")
                .map(|v| v.as_str().unwrap_or("default"))
                .unwrap_or("default"),
        );
        let state = self.get_state(req.account, &req.region);
        if !state.clusters.read().contains_key(cluster_name) {
            return cluster_not_found(cluster_name);
        }
        let svc_name = req
            .params
            .get("serviceName")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        if svc_name.is_empty() {
            return AwsResponse::error(400, "ClientException", "serviceName is required.");
        }
        let now = now_f64();
        let dep_id = format!(
            "ecs-svc/{}",
            &uuid::Uuid::new_v4().simple().to_string()[..20]
        );
        let task_definition = req
            .params
            .get("taskDefinition")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let desired_count = req.params.get("desiredCount").cloned().unwrap_or(json!(1));
        // moto 5.2.0 computes the rollout state at service creation: the
        // primary deployment reports COMPLETED once its runningCount reaches
        // desiredCount, IN_PROGRESS while tasks are still coming up. The
        // deployment created below starts at runningCount 0.
        let running_count = 0i64;
        let (rollout_state, rollout_state_reason) =
            if running_count >= desired_count.as_i64().unwrap_or(0) {
                ("COMPLETED", format!("ECS deployment {dep_id} completed."))
            } else {
                (
                    "IN_PROGRESS",
                    format!("ECS deployment {dep_id} in progress."),
                )
            };
        let svc_arn = format!(
            "arn:aws:ecs:{}:{}:service/{}/{}",
            req.region, req.account, cluster_name, svc_name
        );
        let clusters = state.clusters.read();
        let cluster_arn = clusters
            .get(cluster_name)
            .and_then(|c| c.get("clusterArn"))
            .cloned()
            .unwrap_or(Value::Null);
        drop(clusters);
        let tags: Vec<Value> = req
            .params
            .get("tags")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let service = json!({
            "serviceArn": svc_arn.clone(),
            "serviceName": svc_name,
            "clusterArn": cluster_arn,
            "taskDefinition": task_definition,
            "desiredCount": desired_count.clone(),
            "runningCount": 0,
            "pendingCount": 0,
            "status": "ACTIVE",
            "launchType": req.params.get("launchType").and_then(|v| v.as_str()).unwrap_or("FARGATE"),
            "networkConfiguration": req.params.get("networkConfiguration").cloned().unwrap_or(json!({})),
            "loadBalancers": req.params.get("loadBalancers").cloned().unwrap_or(json!([])),
            "deployments": [{
                "id": dep_id,
                "status": "PRIMARY",
                "taskDefinition": task_definition,
                "desiredCount": desired_count,
                "runningCount": 0,
                "pendingCount": 0,
                "failedTasks": env_failed_tasks(),
                "rolloutState": rollout_state,
                "rolloutStateReason": rollout_state_reason,
                "createdAt": now,
                "updatedAt": now,
            }],
            "createdAt": now,
            "tags": tags.clone(),
        });
        {
            let mut services = state.services.write();
            let cluster_services = services.entry(cluster_name.to_string()).or_default();
            if cluster_services.contains_key(svc_name) {
                return AwsResponse::error(
                    400,
                    "ClientException",
                    &format!("Service {} already exists.", svc_name),
                );
            }
            cluster_services.insert(svc_name.to_string(), service.clone());
        }
        if !tags.is_empty() {
            state.tags.write().insert(svc_arn, json!(tags));
        }
        AwsResponse::json(200, json!({ "service": service }))
    }

    fn delete_service(&self, req: &AwsRequest) -> AwsResponse {
        let cluster_name = resolve_name(
            req.params
                .get("cluster")
                .map(|v| v.as_str().unwrap_or("default"))
                .unwrap_or("default"),
        );
        let svc_name = resolve_name(
            req.params
                .get("service")
                .and_then(|v| v.as_str())
                .unwrap_or_default(),
        );
        let state = self.get_state(req.account, &req.region);
        let mut services = state.services.write();
        let cluster_services = services.get_mut(cluster_name);
        let lookup = cluster_services.and_then(|m| m.get_mut(svc_name));
        let Some(service) = lookup else {
            return service_not_found(svc_name);
        };
        service["status"] = json!("INACTIVE");
        service["desiredCount"] = json!(0);
        let arn = service["serviceArn"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let updated = service.clone();
        if let Some(map) = services.get_mut(cluster_name) {
            map.remove(svc_name);
        }
        drop(services);
        state.tags.write().remove(&arn);
        AwsResponse::json(200, json!({ "service": updated }))
    }

    fn describe_services(&self, req: &AwsRequest) -> AwsResponse {
        let cluster_name = resolve_name(
            req.params
                .get("cluster")
                .map(|v| v.as_str().unwrap_or("default"))
                .unwrap_or("default"),
        );
        let ids: Vec<String> = req
            .params
            .get("services")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let mut found: Vec<Value> = Vec::new();
        let mut failures: Vec<Value> = Vec::new();
        let services = state.services.read();
        let cluster_services = services.get(cluster_name);
        for sid in &ids {
            let lookup = cluster_services
                .map(|m| {
                    m.get(resolve_name(sid))
                        .cloned()
                        .or_else(|| m.get(sid).cloned())
                })
                .unwrap_or(None);
            match lookup {
                Some(svc) => found.push(svc),
                None => failures.push(json!({ "arn": sid, "reason": "MISSING" })),
            }
        }
        AwsResponse::json(200, json!({ "services": found, "failures": failures }))
    }

    fn list_services(&self, req: &AwsRequest) -> AwsResponse {
        let cluster_name = resolve_name(
            req.params
                .get("cluster")
                .map(|v| v.as_str().unwrap_or("default"))
                .unwrap_or("default"),
        );
        let state = self.get_state(req.account, &req.region);
        let arns: Vec<String> = state
            .services
            .read()
            .get(cluster_name)
            .map(|m| {
                m.values()
                    .map(|s| s["serviceArn"].as_str().unwrap_or_default().to_string())
                    .collect()
            })
            .unwrap_or_default();
        AwsResponse::json(200, json!({ "serviceArns": arns }))
    }

    fn update_service(&self, req: &AwsRequest) -> AwsResponse {
        let cluster_name = resolve_name(
            req.params
                .get("cluster")
                .map(|v| v.as_str().unwrap_or("default"))
                .unwrap_or("default"),
        );
        let svc_name = resolve_name(
            req.params
                .get("service")
                .and_then(|v| v.as_str())
                .unwrap_or_default(),
        );
        let state = self.get_state(req.account, &req.region);
        let mut services = state.services.write();
        let lookup = services
            .get_mut(cluster_name)
            .and_then(|m| m.get_mut(svc_name));
        let Some(service) = lookup else {
            return service_not_found(svc_name);
        };
        if let Some(count) = req.params.get("desiredCount") {
            service["desiredCount"] = count.clone();
        }
        if let Some(td) = req.params.get("taskDefinition") {
            service["taskDefinition"] = td.clone();
        }
        let updated = service.clone();
        AwsResponse::json(200, json!({ "service": updated }))
    }

    fn update_service_primary_task_set(&self, req: &AwsRequest) -> AwsResponse {
        let cluster_name = resolve_name(
            req.params
                .get("cluster")
                .and_then(|v| v.as_str())
                .unwrap_or_default(),
        );
        let state = self.get_state(req.account, &req.region);
        if !state.clusters.read().contains_key(cluster_name) {
            return cluster_not_found(cluster_name);
        }
        let svc_name = resolve_name(
            req.params
                .get("service")
                .and_then(|v| v.as_str())
                .unwrap_or_default(),
        );
        let exists = state
            .services
            .read()
            .get(cluster_name)
            .map(|m| m.contains_key(svc_name))
            .unwrap_or(false);
        if !exists {
            return service_not_found(svc_name);
        }
        AwsResponse::error(
            400,
            "InvalidParameterException",
            &format!(
                "Task set {} not found",
                req.params
                    .get("primaryTaskSet")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
            ),
        )
    }

    // ------------------------------------------------------------------
    // Tasks
    // ------------------------------------------------------------------

    fn run_task(&self, req: &AwsRequest) -> AwsResponse {
        let cluster_name = resolve_name(
            req.params
                .get("cluster")
                .map(|v| v.as_str().unwrap_or("default"))
                .unwrap_or("default"),
        );
        // moto 5.2.3: an explicit capacity provider strategy cannot be
        // combined with a launch type.
        let launch_type_param = req.params.get("launchType").and_then(|v| v.as_str());
        let explicit_strategy = req
            .params
            .get("capacityProviderStrategy")
            .and_then(|v| v.as_array())
            .filter(|a| !a.is_empty());
        let has_launch_type = launch_type_param.is_some_and(|t| !t.is_empty());
        if has_launch_type && explicit_strategy.is_some() {
            return AwsResponse::error(
                400,
                "InvalidParameterException",
                "Specify either Launch Type or Capacity Provider Strategy, but not both.",
            );
        }
        let state = self.get_state(req.account, &req.region);
        if !state.clusters.read().contains_key(cluster_name) {
            return cluster_not_found(cluster_name);
        }
        let td_ref = req
            .params
            .get("taskDefinition")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let td = match resolve_task_definition(&state, td_ref) {
            Some(td) => td,
            None => {
                return AwsResponse::error(
                    400,
                    "ClientException",
                    &format!("Task definition {} not found.", td_ref),
                )
            }
        };
        let count = req
            .params
            .get("count")
            .and_then(|v| v.as_i64())
            .unwrap_or(1);
        let now = now_f64();
        let clusters = state.clusters.read();
        let cluster = clusters.get(cluster_name);
        let cluster_arn = cluster
            .and_then(|c| c.get("clusterArn"))
            .cloned()
            .unwrap_or(Value::Null);
        // moto 5.2.3: with neither an explicit strategy nor a launch type, the
        // cluster's default capacity provider strategy applies.
        let cluster_default = if !has_launch_type {
            cluster
                .and_then(|c| c.get("defaultCapacityProviderStrategy"))
                .and_then(|v| v.as_array())
                .filter(|a| !a.is_empty())
        } else {
            None
        };
        let strategy = explicit_strategy.or(cluster_default);
        let capacity_provider_name = strategy
            .and_then(|items| items.first())
            .and_then(|item| item.get("capacityProvider"))
            .and_then(|v| v.as_str())
            .map(String::from);
        drop(clusters);
        let containers: Vec<Value> = td
            .get("containerDefinitions")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(|cd| {
                let mut entry = json!({
                    "containerArn": format!("arn:aws:ecs:{}:{}:container/{}",
                        req.region, req.account, uuid::Uuid::new_v4()),
                    "name": cd.get("name").and_then(|v| v.as_str()).unwrap_or(""),
                    "image": cd.get("image").and_then(|v| v.as_str()).unwrap_or(""),
                    "lastStatus": "RUNNING",
                });
                if let Some(ports) = cd.get("portMappings") {
                    entry["portMappings"] = ports.clone();
                }
                entry
            })
            .collect();
        let mut tasks: Vec<Value> = Vec::new();
        let mut inserted: Vec<(String, Value)> = Vec::new();
        let task_tags: Vec<Value> = req
            .params
            .get("tags")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        for _ in 0..count {
            let task_id = uuid::Uuid::new_v4().to_string();
            let task_arn = format!(
                "arn:aws:ecs:{}:{}:task/{}/{}",
                req.region, req.account, cluster_name, task_id
            );
            if !task_tags.is_empty() {
                state
                    .tags
                    .write()
                    .insert(task_arn.clone(), json!(task_tags.clone()));
            }
            let mut task = json!({
                "taskArn": task_arn.clone(),
                "taskDefinitionArn": td["taskDefinitionArn"].clone(),
                "clusterArn": cluster_arn,
                "lastStatus": "RUNNING",
                "desiredStatus": "RUNNING",
                "cpu": td.get("cpu").and_then(|v| v.as_str()).unwrap_or("256"),
                "memory": td.get("memory").and_then(|v| v.as_str()).unwrap_or("512"),
                "launchType": req.params.get("launchType").and_then(|v| v.as_str()).unwrap_or("FARGATE"),
                "startedAt": now,
                "createdAt": now,
                "containers": containers.clone(),
                "tags": [],
                "overrides": req.params.get("overrides").cloned().unwrap_or(json!({})),
            });
            // moto 5.2.3 surfaces the strategy's primary provider as the Task
            // model's capacityProviderName member.
            if let Some(provider) = &capacity_provider_name {
                task["capacityProviderName"] = json!(provider);
            }
            tasks.push(task.clone());
            inserted.push((task_id, task));
        }
        state
            .tasks
            .write()
            .entry(cluster_name.to_string())
            .or_default()
            .extend(inserted);
        AwsResponse::json(200, json!({ "tasks": tasks, "failures": [] }))
    }

    fn start_task(&self, req: &AwsRequest) -> AwsResponse {
        let cluster_name = resolve_name(
            req.params
                .get("cluster")
                .and_then(|v| v.as_str())
                .unwrap_or_default(),
        );
        let state = self.get_state(req.account, &req.region);
        if !state.clusters.read().contains_key(cluster_name) {
            return cluster_not_found(cluster_name);
        }
        let td_ref = req
            .params
            .get("taskDefinition")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let td = match resolve_task_definition(&state, td_ref) {
            Some(td) => td,
            None => {
                return AwsResponse::error(
                    400,
                    "ClientException",
                    &format!("Task definition {} not found.", td_ref),
                )
            }
        };
        let now = now_f64();
        let task_id = uuid::Uuid::new_v4().to_string();
        let task_arn = format!(
            "arn:aws:ecs:{}:{}:task/{}/{}",
            req.region, req.account, cluster_name, task_id
        );
        let task = json!({
            "taskArn": task_arn.clone(),
            "taskDefinitionArn": td["taskDefinitionArn"].clone(),
            "clusterArn": format!("arn:aws:ecs:{}:{}:cluster/{}", req.region, req.account, cluster_name),
            "lastStatus": "RUNNING",
            "desiredStatus": "RUNNING",
            "createdAt": now,
            "startedAt": now,
            "tags": [],
        });
        state
            .tasks
            .write()
            .entry(cluster_name.to_string())
            .or_default()
            .insert(task_id, task.clone());
        AwsResponse::json(200, json!({ "tasks": [task], "failures": [] }))
    }

    fn stop_task(&self, req: &AwsRequest) -> AwsResponse {
        let cluster_name = resolve_name(
            req.params
                .get("cluster")
                .map(|v| v.as_str().unwrap_or("default"))
                .unwrap_or("default"),
        );
        let task_ref = req
            .params
            .get("task")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let task_id = resolve_name(task_ref);
        let state = self.get_state(req.account, &req.region);
        let mut tasks = state.tasks.write();
        let lookup = tasks.get_mut(cluster_name).and_then(|m| m.get_mut(task_id));
        let Some(task) = lookup else {
            return AwsResponse::error(
                400,
                "InvalidParameterException",
                &format!("Task {} not found.", task_ref),
            );
        };
        task["lastStatus"] = json!("STOPPED");
        task["desiredStatus"] = json!("STOPPED");
        task["stoppedAt"] = json!(now_f64());
        task["stoppedReason"] = json!(req
            .params
            .get("reason")
            .and_then(|v| v.as_str())
            .unwrap_or("Task stopped by user"));
        let updated = task.clone();
        AwsResponse::json(200, json!({ "task": updated }))
    }

    fn describe_tasks(&self, req: &AwsRequest) -> AwsResponse {
        let cluster_name = resolve_name(
            req.params
                .get("cluster")
                .map(|v| v.as_str().unwrap_or("default"))
                .unwrap_or("default"),
        );
        let ids: Vec<String> = req
            .params
            .get("tasks")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let mut found: Vec<Value> = Vec::new();
        let mut failures: Vec<Value> = Vec::new();
        let tasks = state.tasks.read();
        let cluster_tasks = tasks.get(cluster_name);
        for tid in &ids {
            let lookup = cluster_tasks
                .map(|m| {
                    m.get(resolve_name(tid)).cloned().or_else(|| {
                        m.get(tid).cloned().or_else(|| {
                            m.values()
                                .find(|t| {
                                    t.get("taskArn").and_then(|a| a.as_str()) == Some(tid.as_str())
                                })
                                .cloned()
                        })
                    })
                })
                .unwrap_or(None);
            match lookup {
                Some(t) => found.push(t),
                None => failures.push(json!({ "arn": tid, "reason": "MISSING" })),
            }
        }
        AwsResponse::json(200, json!({ "tasks": found, "failures": failures }))
    }

    fn list_tasks(&self, req: &AwsRequest) -> AwsResponse {
        let cluster_name = resolve_name(
            req.params
                .get("cluster")
                .map(|v| v.as_str().unwrap_or("default"))
                .unwrap_or("default"),
        );
        let status_filter = req
            .params
            .get("desiredStatus")
            .and_then(|v| v.as_str())
            .unwrap_or("RUNNING");
        let state = self.get_state(req.account, &req.region);
        let arns: Vec<String> = state
            .tasks
            .read()
            .get(cluster_name)
            .map(|m| {
                m.values()
                    .filter(|t| {
                        t.get("desiredStatus").and_then(|v| v.as_str()) == Some(status_filter)
                    })
                    .map(|t| t["taskArn"].as_str().unwrap_or_default().to_string())
                    .collect()
            })
            .unwrap_or_default();
        AwsResponse::json(200, json!({ "taskArns": arns }))
    }

    // ------------------------------------------------------------------
    // Container instances
    // ------------------------------------------------------------------

    fn register_container_instance(&self, req: &AwsRequest) -> AwsResponse {
        let cluster_name = resolve_name(
            req.params
                .get("cluster")
                .map(|v| v.as_str().unwrap_or("default"))
                .unwrap_or("default"),
        );
        let state = self.get_state(req.account, &req.region);
        if !state.clusters.read().contains_key(cluster_name) {
            return cluster_not_found(cluster_name);
        }
        let doc = req
            .params
            .get("instanceIdentityDocument")
            .and_then(|v| v.as_str())
            .and_then(|s| serde_json::from_str::<Value>(s).ok())
            .unwrap_or_else(|| json!({}));
        let ec2_instance_id = doc
            .get("instanceId")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| format!("i-{}", uuid::Uuid::new_v4().simple()));
        let ci_id = uuid::Uuid::new_v4().to_string();
        let ci_arn = format!(
            "arn:aws:ecs:{}:{}:container-instance/{}/{}",
            req.region, req.account, cluster_name, ci_id
        );
        let tags: Vec<Value> = req
            .params
            .get("tags")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let ci = json!({
            "containerInstanceArn": ci_arn.clone(),
            "ec2InstanceId": ec2_instance_id,
            "status": "ACTIVE",
            "registeredResources": [
                { "name": "CPU", "type": "INTEGER", "integerValue": 2048 },
                { "name": "MEMORY", "type": "INTEGER", "integerValue": 3768 },
            ],
            "remainingResources": [
                { "name": "CPU", "type": "INTEGER", "integerValue": 2048 },
                { "name": "MEMORY", "type": "INTEGER", "integerValue": 3768 },
            ],
            "runningTasksCount": 0,
            "pendingTasksCount": 0,
            "agentConnected": true,
            "attributes": req.params.get("attributes").cloned().unwrap_or(json!([])),
            "tags": tags.clone(),
        });
        state
            .container_instances
            .write()
            .entry(cluster_name.to_string())
            .or_default()
            .insert(ci_id, ci.clone());
        if let Some(cluster) = state.clusters.write().get_mut(cluster_name) {
            let current = cluster["registeredContainerInstancesCount"]
                .as_i64()
                .unwrap_or(0);
            cluster["registeredContainerInstancesCount"] = json!(current + 1);
        }
        if !tags.is_empty() {
            state.tags.write().insert(ci_arn, json!(tags));
        }
        AwsResponse::json(200, json!({ "containerInstance": ci }))
    }

    fn deregister_container_instance(&self, req: &AwsRequest) -> AwsResponse {
        let cluster_name = resolve_name(
            req.params
                .get("cluster")
                .map(|v| v.as_str().unwrap_or("default"))
                .unwrap_or("default"),
        );
        let state = self.get_state(req.account, &req.region);
        if !state.clusters.read().contains_key(cluster_name) {
            return cluster_not_found(cluster_name);
        }
        let ci_ref = req
            .params
            .get("containerInstance")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let ci_id = resolve_name(ci_ref);
        let mut cis = state.container_instances.write();
        let removed = cis.get_mut(cluster_name).and_then(|m| m.remove(ci_id));
        let Some(mut ci) = removed else {
            return AwsResponse::error(
                400,
                "InvalidParameterException",
                &format!("Container instance {} not found.", ci_ref),
            );
        };
        let arn = ci["containerInstanceArn"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        ci["status"] = json!("INACTIVE");
        ci["agentConnected"] = json!(false);
        let updated = ci;
        drop(cis);
        state.tags.write().remove(&arn);
        if let Some(cluster) = state.clusters.write().get_mut(cluster_name) {
            let current = cluster["registeredContainerInstancesCount"]
                .as_i64()
                .unwrap_or(0);
            cluster["registeredContainerInstancesCount"] = json!(current - 1);
        }
        AwsResponse::json(200, json!({ "containerInstance": updated }))
    }

    fn describe_container_instances(&self, req: &AwsRequest) -> AwsResponse {
        let cluster_name = resolve_name(
            req.params
                .get("cluster")
                .map(|v| v.as_str().unwrap_or("default"))
                .unwrap_or("default"),
        );
        let refs: Vec<String> = req
            .params
            .get("containerInstances")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let mut found: Vec<Value> = Vec::new();
        let mut failures: Vec<Value> = Vec::new();
        let cis = state.container_instances.read();
        let cluster_cis = cis.get(cluster_name);
        for reference in &refs {
            let lookup = cluster_cis
                .map(|m| {
                    m.get(resolve_name(reference)).cloned().or_else(|| {
                        m.iter()
                            .find(|(_, t)| {
                                t.get("containerInstanceArn").and_then(|a| a.as_str())
                                    == Some(reference.as_str())
                            })
                            .map(|(_, t)| t.clone())
                    })
                })
                .unwrap_or(None);
            match lookup {
                Some(c) => found.push(c),
                None => failures.push(json!({ "arn": reference, "reason": "MISSING" })),
            }
        }
        AwsResponse::json(
            200,
            json!({ "containerInstances": found, "failures": failures }),
        )
    }

    fn list_container_instances(&self, req: &AwsRequest) -> AwsResponse {
        let cluster_name = resolve_name(
            req.params
                .get("cluster")
                .map(|v| v.as_str().unwrap_or("default"))
                .unwrap_or("default"),
        );
        let state = self.get_state(req.account, &req.region);
        let arns: Vec<String> = state
            .container_instances
            .read()
            .get(cluster_name)
            .map(|m| {
                m.values()
                    .map(|ci| {
                        ci["containerInstanceArn"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string()
                    })
                    .collect()
            })
            .unwrap_or_default();
        AwsResponse::json(200, json!({ "containerInstanceArns": arns }))
    }

    fn update_container_instances_state(&self, req: &AwsRequest) -> AwsResponse {
        let cluster_name = resolve_name(
            req.params
                .get("cluster")
                .map(|v| v.as_str().unwrap_or("default"))
                .unwrap_or("default"),
        );
        let state = self.get_state(req.account, &req.region);
        if !state.clusters.read().contains_key(cluster_name) {
            return cluster_not_found(cluster_name);
        }
        let new_status = req
            .params
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("ACTIVE");
        let mut found: Vec<Value> = Vec::new();
        let mut failures: Vec<Value> = Vec::new();
        {
            let mut cis = state.container_instances.write();
            let cluster_cis = cis.get_mut(cluster_name);
            if let Some(map) = cluster_cis {
                for reference in req
                    .params
                    .get("containerInstances")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default()
                {
                    let r = reference.as_str().unwrap_or_default().to_string();
                    match map.get_mut(resolve_name(&r)) {
                        Some(ci) => {
                            ci["status"] = json!(new_status);
                            found.push(ci.clone());
                        }
                        None => failures.push(json!({ "arn": reference, "reason": "MISSING" })),
                    }
                }
            }
        }
        AwsResponse::json(
            200,
            json!({ "containerInstances": found, "failures": failures }),
        )
    }

    fn update_container_agent(&self, req: &AwsRequest) -> AwsResponse {
        let cluster_name = resolve_name(
            req.params
                .get("cluster")
                .and_then(|v| v.as_str())
                .unwrap_or_default(),
        );
        let state = self.get_state(req.account, &req.region);
        if !state.clusters.read().contains_key(cluster_name) {
            return cluster_not_found(cluster_name);
        }
        let ci_ref = req
            .params
            .get("containerInstance")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let ci_id = resolve_name(ci_ref);
        let found = state
            .container_instances
            .read()
            .get(cluster_name)
            .and_then(|m| m.get(ci_id))
            .cloned();
        match found {
            Some(ci) => AwsResponse::json(200, json!({ "containerInstance": ci })),
            None => AwsResponse::error(
                400,
                "InvalidParameterException",
                "The referenced container instance is not found.",
            ),
        }
    }

    // ------------------------------------------------------------------
    // Task sets
    // ------------------------------------------------------------------

    fn create_task_set(&self, req: &AwsRequest) -> AwsResponse {
        let cluster_name = resolve_name(
            req.params
                .get("cluster")
                .and_then(|v| v.as_str())
                .unwrap_or_default(),
        );
        let state = self.get_state(req.account, &req.region);
        if !state.clusters.read().contains_key(cluster_name) {
            return cluster_not_found(cluster_name);
        }
        let svc_name = resolve_name(
            req.params
                .get("service")
                .and_then(|v| v.as_str())
                .unwrap_or_default(),
        );
        let svc = state
            .services
            .read()
            .get(cluster_name)
            .and_then(|m| m.get(svc_name))
            .cloned();
        let Some(svc) = svc else {
            return service_not_found(svc_name);
        };
        let svc_arn = svc["serviceArn"].as_str().unwrap_or_default().to_string();
        let ts_id = uuid::Uuid::new_v4().to_string();
        let ts_arn = format!(
            "arn:aws:ecs:{}:{}:task-set/{}/{}/{}",
            req.region, req.account, cluster_name, svc_name, ts_id
        );
        let tags: Vec<Value> = req
            .params
            .get("tags")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let task_set = json!({
            "id": ts_id,
            "taskSetArn": ts_arn.clone(),
            "serviceArn": svc_arn.clone(),
            "clusterArn": format!("arn:aws:ecs:{}:{}:cluster/{}", req.region, req.account, cluster_name),
            "taskDefinition": req.params.get("taskDefinition").cloned().unwrap_or(json!("")),
            "status": "ACTIVE",
            "computedDesiredCount": 0,
            "pendingCount": 0,
            "runningCount": 0,
            "stabilityStatus": "STEADY_STATE",
            "scale": req.params.get("scale").cloned()
                .unwrap_or(json!({ "value": 100.0, "unit": "PERCENT" })),
            "launchType": req.params.get("launchType").and_then(|v| v.as_str()).unwrap_or("FARGATE"),
            "networkConfiguration": req.params.get("networkConfiguration").cloned().unwrap_or(json!({})),
            "loadBalancers": req.params.get("loadBalancers").cloned().unwrap_or(json!([])),
            "tags": tags.clone(),
            "createdAt": now_f64(),
        });
        state
            .task_sets
            .write()
            .entry(svc_arn)
            .or_default()
            .insert(ts_id, task_set.clone());
        if !tags.is_empty() {
            state.tags.write().insert(ts_arn, json!(tags));
        }
        AwsResponse::json(200, json!({ "taskSet": task_set }))
    }

    fn describe_task_sets(&self, req: &AwsRequest) -> AwsResponse {
        let cluster_name = resolve_name(
            req.params
                .get("cluster")
                .and_then(|v| v.as_str())
                .unwrap_or_default(),
        );
        let svc_name = resolve_name(
            req.params
                .get("service")
                .and_then(|v| v.as_str())
                .unwrap_or_default(),
        );
        let state = self.get_state(req.account, &req.region);
        let svc = state
            .services
            .read()
            .get(cluster_name)
            .and_then(|m| m.get(svc_name))
            .cloned();
        let Some(svc) = svc else {
            return service_not_found(svc_name);
        };
        let svc_arn = svc["serviceArn"].as_str().unwrap_or_default().to_string();
        let ts_map = state
            .task_sets
            .read()
            .get(&svc_arn)
            .cloned()
            .unwrap_or_default();
        let refs: Vec<String> = req
            .params
            .get("taskSets")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let found: Vec<Value> = if refs.is_empty() {
            ts_map.values().cloned().collect()
        } else {
            refs.iter()
                .filter_map(|r| ts_map.get(resolve_name(r)).cloned())
                .collect()
        };
        AwsResponse::json(200, json!({ "taskSets": found }))
    }

    fn update_task_set(&self, req: &AwsRequest) -> AwsResponse {
        let cluster_name = resolve_name(
            req.params
                .get("cluster")
                .and_then(|v| v.as_str())
                .unwrap_or_default(),
        );
        let svc_name = resolve_name(
            req.params
                .get("service")
                .and_then(|v| v.as_str())
                .unwrap_or_default(),
        );
        let ts_ref = req
            .params
            .get("taskSet")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let svc = state
            .services
            .read()
            .get(cluster_name)
            .and_then(|m| m.get(svc_name))
            .cloned();
        let Some(svc) = svc else {
            return service_not_found(svc_name);
        };
        let svc_arn = svc["serviceArn"].as_str().unwrap_or_default().to_string();
        let mut ts_maps = state.task_sets.write();
        let ts = ts_maps
            .get_mut(&svc_arn)
            .and_then(|m| m.get_mut(resolve_name(ts_ref)));
        let Some(ts_json) = ts else {
            return AwsResponse::error(
                400,
                "InvalidParameterException",
                &format!("Task set {} not found.", ts_ref),
            );
        };
        if let Some(scale) = req.params.get("scale") {
            ts_json["scale"] = scale.clone();
        }
        let updated = ts_json.clone();
        AwsResponse::json(200, json!({ "taskSet": updated }))
    }

    fn delete_task_set(&self, req: &AwsRequest) -> AwsResponse {
        let cluster_name = resolve_name(
            req.params
                .get("cluster")
                .and_then(|v| v.as_str())
                .unwrap_or_default(),
        );
        let svc_name = resolve_name(
            req.params
                .get("service")
                .and_then(|v| v.as_str())
                .unwrap_or_default(),
        );
        let ts_ref = req
            .params
            .get("taskSet")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let svc = state
            .services
            .read()
            .get(cluster_name)
            .and_then(|m| m.get(svc_name))
            .cloned();
        let Some(svc) = svc else {
            return service_not_found(svc_name);
        };
        let svc_arn = svc["serviceArn"].as_str().unwrap_or_default().to_string();
        let mut ts_maps = state.task_sets.write();
        let removed = ts_maps
            .get_mut(&svc_arn)
            .and_then(|m| m.remove(resolve_name(ts_ref)));
        let Some(mut ts) = removed else {
            return AwsResponse::error(
                400,
                "InvalidParameterException",
                &format!("Task set {} not found.", ts_ref),
            );
        };
        ts["status"] = json!("INACTIVE");
        let updated = ts;
        let arn = updated["taskSetArn"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        drop(ts_maps);
        state.tags.write().remove(&arn);
        AwsResponse::json(200, json!({ "taskSet": updated }))
    }

    // ------------------------------------------------------------------
    // Attributes
    // ------------------------------------------------------------------

    fn put_attributes(&self, req: &AwsRequest) -> AwsResponse {
        let cluster_name = resolve_name(
            req.params
                .get("cluster")
                .map(|v| v.as_str().unwrap_or("default"))
                .unwrap_or("default"),
        );
        let state = self.get_state(req.account, &req.region);
        if !state.clusters.read().contains_key(cluster_name) {
            return cluster_not_found(cluster_name);
        }
        let attrs: Vec<Value> = req
            .params
            .get("attributes")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let mut all = state.attributes.write();
        let existing = all.entry(cluster_name.to_string()).or_default();
        for attr in &attrs {
            let key = attr_key(attr);
            existing.retain(|a| attr_key(a) != key);
            existing.push(attr.clone());
        }
        let attributes = req.params.get("attributes").cloned().unwrap_or(json!([]));
        AwsResponse::json(200, json!({ "attributes": attributes }))
    }

    fn delete_attributes(&self, req: &AwsRequest) -> AwsResponse {
        let cluster_name = resolve_name(
            req.params
                .get("cluster")
                .map(|v| v.as_str().unwrap_or("default"))
                .unwrap_or("default"),
        );
        let state = self.get_state(req.account, &req.region);
        if !state.clusters.read().contains_key(cluster_name) {
            return cluster_not_found(cluster_name);
        }
        let attrs_to_del: Vec<Value> = req
            .params
            .get("attributes")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        {
            let mut all = state.attributes.write();
            let existing = all.entry(cluster_name.to_string()).or_default();
            for attr in &attrs_to_del {
                let key = attr_key(attr);
                existing.retain(|a| attr_key(a) != key);
            }
        }
        let attributes = req.params.get("attributes").cloned().unwrap_or(json!([]));
        AwsResponse::json(200, json!({ "attributes": attributes }))
    }

    fn list_attributes(&self, req: &AwsRequest) -> AwsResponse {
        let cluster_name = resolve_name(
            req.params
                .get("cluster")
                .map(|v| v.as_str().unwrap_or("default"))
                .unwrap_or("default"),
        );
        let target_type = req
            .params
            .get("targetType")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let name_filter = req.params.get("attributeName").and_then(|v| v.as_str());
        let state = self.get_state(req.account, &req.region);
        let filtered: Vec<Value> = state
            .attributes
            .read()
            .get(cluster_name)
            .map(|attrs| {
                attrs
                    .iter()
                    .filter(|a| {
                        let t = a.get("targetType").and_then(|v| v.as_str()).unwrap_or("");
                        target_type.is_empty() || t == target_type
                    })
                    .filter(|a| {
                        name_filter
                            .map(|n| a.get("name").and_then(|v| v.as_str()) == Some(n))
                            .unwrap_or(true)
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        AwsResponse::json(200, json!({ "attributes": filtered }))
    }

    // ------------------------------------------------------------------
    // Account settings
    // ------------------------------------------------------------------

    fn list_account_settings(&self, req: &AwsRequest) -> AwsResponse {
        let name = req.params.get("name").and_then(|v| v.as_str());
        let state = self.get_state(req.account, &req.region);
        let mut settings_list: Vec<Value> = state
            .account_settings
            .read()
            .values()
            .filter(|s| {
                name.map(|n| s.get("name").and_then(|v| v.as_str()) == Some(n))
                    .unwrap_or(true)
            })
            .cloned()
            .collect();
        settings_list.sort_by(|a, b| {
            let ka = a.get("name").and_then(|v| v.as_str()).unwrap_or("");
            let kb = b.get("name").and_then(|v| v.as_str()).unwrap_or("");
            ka.cmp(kb)
        });
        AwsResponse::json(200, json!({ "settings": settings_list }))
    }

    fn put_account_setting(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let value = req
            .params
            .get("value")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let setting = json!({ "name": name, "value": value });
        state.account_settings.write().insert(name, setting.clone());
        AwsResponse::json(200, json!({ "setting": setting }))
    }

    fn put_account_setting_default(&self, req: &AwsRequest) -> AwsResponse {
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
        if !ACCOUNT_SETTING_NAMES.contains(&name) {
            return AwsResponse::error(
                400,
                "InvalidParameterException",
                &format!("Unknown account setting {}", name),
            );
        }
        let state = self.get_state(req.account, &req.region);
        let setting = json!({ "name": name, "value": value });
        let mut settings = state.account_settings.write();
        // moto: stored under `default:{name}`, and becomes the effective
        // setting when it is not set per-user yet.
        settings.insert(format!("default:{}", name), setting.clone());
        let user_has = settings
            .values()
            .any(|s| s.get("name").and_then(|v| v.as_str()) == Some(name));
        if !user_has {
            settings.insert(name.to_string(), setting.clone());
        }
        AwsResponse::json(200, json!({ "setting": setting }))
    }

    fn delete_account_setting(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let mut settings = state.account_settings.write();
        settings.retain(|key, _| key != name && key != &format!("default:{}", name));
        AwsResponse::json(200, json!({}))
    }

    // ------------------------------------------------------------------
    // Tags
    // ------------------------------------------------------------------

    fn tag_resource(&self, req: &AwsRequest) -> AwsResponse {
        let arn = req
            .params
            .get("resourceArn")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let tags: Vec<Value> = req
            .params
            .get("tags")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        {
            let mut all_tags = state.tags.write();
            let entry = all_tags.entry(arn.clone()).or_insert_with(|| json!([]));
            for tag in tags {
                let key = tag.get("key").and_then(|k| k.as_str()).unwrap_or_default();
                if let Some(arr) = entry.as_array_mut() {
                    match arr
                        .iter_mut()
                        .find(|t| t.get("key").and_then(|k| k.as_str()) == Some(key))
                    {
                        Some(slot) => *slot = tag,
                        None => arr.push(tag),
                    }
                }
            }
        }
        sync_inline_tags(&state, &arn);
        AwsResponse::json(200, json!({}))
    }

    fn untag_resource(&self, req: &AwsRequest) -> AwsResponse {
        let arn = req
            .params
            .get("resourceArn")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
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
        let state = self.get_state(req.account, &req.region);
        {
            let mut all_tags = state.tags.write();
            if let Some(arr) = all_tags
                .get_mut(arn.as_str())
                .and_then(|v| v.as_array_mut())
            {
                arr.retain(|t| {
                    t.get("key")
                        .and_then(|k| k.as_str())
                        .map(|k| !keys.iter().any(|x| x == k))
                        .unwrap_or(true)
                });
            }
        }
        sync_inline_tags(&state, &arn);
        AwsResponse::json(200, json!({}))
    }

    fn list_tags(&self, req: &AwsRequest) -> AwsResponse {
        let arn = req
            .params
            .get("resourceArn")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let tags = state.tags.read().get(arn).cloned().unwrap_or_default();
        AwsResponse::json(200, json!({ "tags": tags }))
    }

    // ------------------------------------------------------------------
    // Service deployments & revisions
    // ------------------------------------------------------------------

    fn list_service_deployments(&self, req: &AwsRequest) -> AwsResponse {
        let cluster_name = resolve_name(
            req.params
                .get("cluster")
                .and_then(|v| v.as_str())
                .unwrap_or_default(),
        );
        let state = self.get_state(req.account, &req.region);
        if !state.clusters.read().contains_key(cluster_name) {
            return cluster_not_found(cluster_name);
        }
        let svc_name = resolve_name(
            req.params
                .get("service")
                .and_then(|v| v.as_str())
                .unwrap_or_default(),
        );
        let svc = state
            .services
            .read()
            .get(cluster_name)
            .and_then(|m| m.get(svc_name))
            .cloned();
        let Some(svc) = svc else {
            return service_not_found(svc_name);
        };
        let status_filter = req.params.get("status").and_then(|v| v.as_str());
        let mut deployments = Vec::new();
        for dep in svc
            .get("deployments")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
        {
            let status = dep
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("PRIMARY")
                .to_string();
            if let Some(want) = status_filter {
                if want != status {
                    continue;
                }
            }
            let svc_arn = svc["serviceArn"].as_str().unwrap_or_default();
            let svc_path = svc_arn.split("service/").nth(1).unwrap_or(svc_name);
            deployments.push(json!({
                "serviceDeploymentArn": format!("arn:aws:ecs:{}:{}:service-deployment/{}/{}",
                    req.region, req.account, svc_path,
                    dep.get("id").and_then(|d| d.as_str()).unwrap_or("")),
                "serviceArn": svc_arn,
                "clusterArn": svc["clusterArn"].clone(),
                "status": status,
                "createdAt": dep.get("createdAt").cloned().unwrap_or(Value::Null),
                "targetServiceRevisionArn": format!("arn:aws:ecs:{}:{}:service-revision/{}:1",
                    req.region, req.account, svc_path),
            }));
        }
        AwsResponse::json(200, json!({ "serviceDeployments": deployments }))
    }

    fn describe_service_deployments(&self, req: &AwsRequest) -> AwsResponse {
        let dep_arns: Vec<String> = req
            .params
            .get("serviceDeploymentArns")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let mut results: Vec<Value> = Vec::new();
        let mut failures: Vec<Value> = Vec::new();
        let services = state.services.read();
        for dep_arn in &dep_arns {
            let mut found = false;
            for (cluster_name, svc_map) in services.iter() {
                if found {
                    break;
                }
                for (svc_name, svc) in svc_map.iter() {
                    let mut matched: Option<Value> = None;
                    for dep in svc
                        .get("deployments")
                        .and_then(|v| v.as_array())
                        .cloned()
                        .unwrap_or_default()
                    {
                        let expected = format!(
                            "arn:aws:ecs:{}:{}:service-deployment/{}/{}/{}",
                            req.region,
                            req.account,
                            cluster_name,
                            svc_name,
                            dep.get("id").and_then(|d| d.as_str()).unwrap_or("")
                        );
                        if expected == *dep_arn {
                            matched = Some(json!({
                                "serviceDeploymentArn": expected,
                                "serviceArn": svc["serviceArn"].clone(),
                                "clusterArn": svc["clusterArn"].clone(),
                                "status": dep.get("status").cloned().unwrap_or(json!("PRIMARY")),
                                "taskDefinition": dep.get("taskDefinition").cloned().unwrap_or(json!("")),
                                "desiredCount": dep.get("desiredCount").cloned().unwrap_or(json!(0)),
                                "runningCount": dep.get("runningCount").cloned().unwrap_or(json!(0)),
                                "createdAt": dep.get("createdAt").cloned().unwrap_or(Value::Null),
                                "updatedAt": dep.get("updatedAt").cloned().unwrap_or(Value::Null),
                            }));
                            break;
                        }
                    }
                    if let Some(m) = matched {
                        results.push(m);
                        found = true;
                        break;
                    }
                }
            }
            if !found {
                failures.push(json!({
                    "arn": dep_arn,
                    "reason": "SERVICE_DEPLOYMENT_NOT_FOUND",
                }));
            }
        }
        AwsResponse::json(
            200,
            json!({ "serviceDeployments": results, "failures": failures }),
        )
    }

    fn describe_service_revisions(&self, req: &AwsRequest) -> AwsResponse {
        let rev_arns: Vec<String> = req
            .params
            .get("serviceRevisionArns")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let mut results: Vec<Value> = Vec::new();
        let mut failures: Vec<Value> = Vec::new();
        let services = state.services.read();
        for rev_arn in &rev_arns {
            // arn:aws:ecs:{region}:{account}:service-revision/{cluster}/{svc}:{n}
            let path = rev_arn.split("service-revision/").nth(1);
            let match_info = path.and_then(|rest| {
                let (revision_path, _) = rest.split_once(':')?;
                let mut parts = revision_path.split('/');
                let cluster = parts.next()?;
                let svc = parts.next()?;
                Some((cluster.to_string(), svc.to_string()))
            });
            let mut found = false;
            if let Some((cluster_name, svc_name)) = match_info {
                if let Some((_, svc)) = services
                    .get(&cluster_name)
                    .and_then(|m| m.get_key_value(&svc_name))
                {
                    results.push(json!({
                        "serviceRevisionArn": rev_arn,
                        "serviceArn": svc["serviceArn"].clone(),
                        "clusterArn": svc["clusterArn"].clone(),
                        "taskDefinition": svc.get("taskDefinition").cloned().unwrap_or(json!("")),
                        "desiredCount": svc.get("desiredCount").cloned().unwrap_or(json!(0)),
                        "runningCount": svc.get("runningCount").cloned().unwrap_or(json!(0)),
                        "createdAt": svc.get("createdAt").cloned().unwrap_or(Value::Null),
                    }));
                    found = true;
                }
            }
            if !found {
                failures.push(json!({
                    "arn": rev_arn,
                    "reason": "SERVICE_REVISION_NOT_FOUND",
                }));
            }
        }
        AwsResponse::json(
            200,
            json!({ "serviceRevisions": results, "failures": failures }),
        )
    }

    // ------------------------------------------------------------------
    // Agent & protection
    // ------------------------------------------------------------------

    fn execute_command(&self, req: &AwsRequest) -> AwsResponse {
        let cluster_name = resolve_name(
            req.params
                .get("cluster")
                .and_then(|v| v.as_str())
                .unwrap_or_default(),
        );
        let state = self.get_state(req.account, &req.region);
        if !state.clusters.read().contains_key(cluster_name) {
            return cluster_not_found(cluster_name);
        }
        let task_ref = req
            .params
            .get("task")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let found = state.tasks.read().get(cluster_name).and_then(|m| {
            m.get(resolve_name(task_ref))
                .cloned()
                .or_else(|| m.get(task_ref).cloned())
                .or_else(|| {
                    m.values()
                        .find(|t| t.get("taskArn").and_then(|a| a.as_str()) == Some(task_ref))
                        .cloned()
                })
        });
        let Some(task) = found else {
            return AwsResponse::error(
                400,
                "InvalidParameterException",
                &format!("The task was not found: {}", task_ref),
            );
        };
        AwsResponse::json(
            200,
            json!({
                "clusterArn": format!("arn:aws:ecs:{}:{}:cluster/{}", req.region, req.account, cluster_name),
                "containerArn": format!("arn:aws:ecs:{}:{}:container/{}", req.region, req.account, uuid::Uuid::new_v4()),
                "containerName": req.params.get("container").and_then(|v| v.as_str()).unwrap_or(""),
                "interactive": req.params.get("interactive").cloned().unwrap_or(json!(false)),
                "session": {
                    "sessionId": uuid::Uuid::new_v4().to_string(),
                    "streamUrl": format!("wss://ssmmessages.{}.amazonaws.com/v1/data-channel/{}",
                        req.region, uuid::Uuid::new_v4()),
                    "tokenValue": uuid::Uuid::new_v4().to_string(),
                },
                "taskArn": task["taskArn"].clone(),
            }),
        )
    }

    fn get_task_protection(&self, req: &AwsRequest) -> AwsResponse {
        let cluster_name = resolve_name(
            req.params
                .get("cluster")
                .and_then(|v| v.as_str())
                .unwrap_or_default(),
        );
        let state = self.get_state(req.account, &req.region);
        if !state.clusters.read().contains_key(cluster_name) {
            return cluster_not_found(cluster_name);
        }
        let task_refs: Vec<String> = req
            .params
            .get("tasks")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let mut protected: Vec<Value> = Vec::new();
        let mut failures: Vec<Value> = Vec::new();
        let tasks = state.tasks.read();
        let cluster_tasks = tasks.get(cluster_name);
        if task_refs.is_empty() {
            for task in cluster_tasks.iter().flat_map(|m| m.values()) {
                protected.push(json!({
                    "taskArn": task["taskArn"].clone(),
                    "protectionEnabled": false,
                }));
            }
        } else {
            for task_ref in task_refs {
                let short = resolve_name(&task_ref);
                let found = cluster_tasks
                    .and_then(|m| m.get(short).cloned().or_else(|| m.get(&task_ref).cloned()))
                    .map(|t| {
                        json!({
                            "taskArn": t["taskArn"].clone(),
                            "protectionEnabled": false,
                        })
                    });
                match found {
                    Some(v) => protected.push(v),
                    None => failures.push(json!({
                        "arn": task_ref.clone(),
                        "reason": "TASK_NOT_FOUND",
                        "detail": format!("The referenced task was not found: {}", task_ref),
                    })),
                }
            }
        }
        AwsResponse::json(
            200,
            json!({ "protectedTasks": protected, "failures": failures }),
        )
    }

    fn update_task_protection(&self, req: &AwsRequest) -> AwsResponse {
        let cluster_name = resolve_name(
            req.params
                .get("cluster")
                .and_then(|v| v.as_str())
                .unwrap_or_default(),
        );
        let state = self.get_state(req.account, &req.region);
        if !state.clusters.read().contains_key(cluster_name) {
            return cluster_not_found(cluster_name);
        }
        let task_refs: Vec<String> = req
            .params
            .get("tasks")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let enabled = req
            .params
            .get("protectionEnabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let mut protected: Vec<Value> = Vec::new();
        for task_ref in task_refs {
            protected.push(json!({
                "taskArn": task_ref,
                "protectionEnabled": enabled,
            }));
        }
        AwsResponse::json(200, json!({ "protectedTasks": protected, "failures": [] }))
    }
}

// ----------------------------------------------------------------------
// Local helpers
// ----------------------------------------------------------------------

/// `arn:.../name` or a plain name → name (ground truth takes the last `/`)
/// segment.
fn resolve_name(value: &str) -> &str {
    value.split('/').next_back().unwrap_or(value)
}

/// Python `time.time()`-style epoch seconds with sub-second precision.
fn now_f64() -> f64 {
    UNIX_EPOCH.elapsed().map(|d| d.as_secs_f64()).unwrap_or(0.0)
}

/// `MOTO_ECS_SERVICE_FAILED_TASKS` (moto 5.2.0): failedTasks reported in the
/// service's primary deployment; non-numeric or negative values fall back to 0.
fn env_failed_tasks() -> i64 {
    std::env::var("MOTO_ECS_SERVICE_FAILED_TASKS")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .map_or(0, |v| v.max(0))
}

fn cluster_not_found(name: &str) -> AwsResponse {
    AwsResponse::error(
        400,
        "ClusterNotFoundException",
        &format!("Cluster {} not found.", name),
    )
}

fn service_not_found(name: &str) -> AwsResponse {
    AwsResponse::error(
        400,
        "ServiceNotFoundException",
        &format!("Service {} not found.", name),
    )
}

/// Resolve a task-definition reference: family, `family:revision`, or ARN.
/// Returns the stored revision (latest ACTIVE revision for a bare family).
fn resolve_task_definition(state: &EcsState, td_ref: &str) -> Option<Value> {
    let parts = if td_ref.starts_with("arn:") {
        td_ref.split('/').next_back().unwrap_or(td_ref)
    } else {
        td_ref
    };
    let tds = state.task_definitions.read();
    if let Some((family, rev_str)) = parts.rsplit_once(':') {
        if let Ok(rev) = rev_str.parse::<i64>() {
            return tds.get(family).and_then(|revisions| {
                revisions
                    .iter()
                    .find(|td| td["revision"].as_i64() == Some(rev))
                    .cloned()
            });
        }
        return None;
    }
    tds.get(parts).and_then(|revisions| {
        revisions
            .iter()
            .rev()
            .find(|td| td["status"] == json!("ACTIVE"))
            .cloned()
    })
}

/// Attribute identity tuple (name, targetId, targetType) used by
/// put/delete attribute semantics.
fn attr_key(attr: &Value) -> (String, String, String) {
    (
        attr.get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        attr.get("targetId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        attr.get("targetType")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
    )
}

/// Keep inline `tags` on cluster/service/task/task-definition resources in
/// sync with the tag store (mirrors `_find_resource_by_arn` in Python).
fn sync_inline_tags(state: &EcsState, arn: &str) {
    let tags_json = state.tags.read().get(arn).cloned().unwrap_or(json!([]));
    // Cluster.
    {
        let mut clusters = state.clusters.write();
        for cluster in clusters.values_mut() {
            if cluster.get("clusterArn").and_then(|a| a.as_str()) == Some(arn) {
                cluster["tags"] = tags_json.clone();
                return;
            }
        }
    }
    // Services.
    {
        let mut services = state.services.write();
        for cluster_services in services.values_mut() {
            for svc in cluster_services.values_mut() {
                if svc.get("serviceArn").and_then(|a| a.as_str()) == Some(arn) {
                    svc["tags"] = tags_json.clone();
                    return;
                }
            }
        }
    }
    // Tasks.
    {
        let mut tasks = state.tasks.write();
        for cluster_tasks in tasks.values_mut() {
            for task in cluster_tasks.values_mut() {
                if task.get("taskArn").and_then(|a| a.as_str()) == Some(arn) {
                    task["tags"] = tags_json.clone();
                    return;
                }
            }
        }
    }
    // Task definitions.
    {
        let mut tds = state.task_definitions.write();
        for revisions in tds.values_mut() {
            for td in revisions.iter_mut() {
                if td.get("taskDefinitionArn").and_then(|a| a.as_str()) == Some(arn) {
                    td["tags"] = tags_json.clone();
                    return;
                }
            }
        }
    }
}

/// moto's `CapacityProvider.update`: merge managedScaling member-wise, plus
/// optional managedTerminationProtection switch.
fn merge_asg(provider: &mut Value, asg: &Value) {
    if let Some(mtp) = asg.get("managedTerminationProtection").cloned() {
        provider["autoScalingGroupProvider"]["managedTerminationProtection"] = mtp;
    }
    if let Some(scaling_props) = asg
        .get("managedScaling")
        .and_then(|m| m.as_object())
        .cloned()
    {
        for (key, value) in scaling_props {
            provider["autoScalingGroupProvider"]["managedScaling"][key.as_str()] = value;
        }
    }
}

/// moto's `_prepare_asg_provider` defaults for the ASG provider.
fn normalize_asg(asg: &mut Value) {
    if asg.get("managedScaling").is_none() {
        asg["managedScaling"] = json!({});
    }
    if let Some(scaling) = asg.get_mut("managedScaling") {
        if scaling.get("instanceWarmupPeriod").is_none() {
            scaling["instanceWarmupPeriod"] = json!(300);
        }
        if scaling.get("minimumScalingStepSize").is_none() {
            scaling["minimumScalingStepSize"] = json!(1);
        }
        if scaling.get("maximumScalingStepSize").is_none() {
            scaling["maximumScalingStepSize"] = json!(10000);
        }
        if scaling.get("targetCapacity").is_none() {
            scaling["targetCapacity"] = json!(100);
        }
        if scaling.get("status").is_none() {
            scaling["status"] = json!("DISABLED");
        }
    }
    if asg.get("managedTerminationProtection").is_none() {
        asg["managedTerminationProtection"] = json!("DISABLED");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use serde_json::json;

    fn make_req(operation: &str, params: Value) -> AwsRequest {
        AwsRequest {
            service: "ecs".to_string(),
            operation: operation.to_string(),
            account: 123456789012,
            region: "us-east-1".to_string(),
            params,
            body: Bytes::new(),
        }
    }

    #[test]
    fn cluster_crud_and_errors() {
        let h = EcsHandler::new();
        let created = h.handle(make_req("CreateCluster", json!({"clusterName": "c1"})));
        assert_eq!(created.status, 200);
        let updated = h.handle(make_req("UpdateCluster", json!({"cluster": "nope"})));
        assert!(updated.body.contains("ClusterNotFoundException"));
        let deleted = h.handle(make_req("DeleteCluster", json!({"cluster": "c1"})));
        assert!(deleted.body.contains("INACTIVE"));
    }

    #[test]
    fn task_definition_roundtrip() {
        let h = EcsHandler::new();
        let reg = h.handle(make_req(
            "RegisterTaskDefinition",
            json!({
                "family": "fam",
                "containerDefinitions": [{"name": "app", "image": "nginx", "memory": 128}],
            }),
        ));
        assert!(reg.body.contains("\"revision\":1"));
        let dereg = h.handle(make_req(
            "DeregisterTaskDefinition",
            json!({
                "taskDefinition": "fam:1"
            }),
        ));
        assert!(dereg.body.contains("INACTIVE"));
    }

    #[test]
    fn service_lifecycle() {
        let h = EcsHandler::new();
        h.handle(make_req("CreateCluster", json!({"clusterName": "cls"})));
        let err = h.handle(make_req(
            "CreateService",
            json!({
                "cluster": "cls", "serviceName": "s1"
            }),
        ));
        assert!(err.body.contains("serviceArn"));
        let listed = h.handle(make_req("ListServices", json!({"cluster": "cls"})));
        assert!(listed.body.contains("service/cls/s1"));
        let deleted = h.handle(make_req(
            "DeleteService",
            json!({
                "cluster": "cls", "service": "s1", "force": true
            }),
        ));
        assert!(deleted.body.contains("INACTIVE"));
    }

    // ------------------------------------------------------------------
    // moto 5.2.0: deployment rollout members on service responses
    // ------------------------------------------------------------------

    fn setup_cluster_and_task_family(h: &EcsHandler, cluster: &str, family: &str) {
        h.handle(make_req("CreateCluster", json!({"clusterName": cluster})));
        h.handle(make_req(
            "RegisterTaskDefinition",
            json!({
                "family": family,
                "containerDefinitions": [{"name": "app", "image": "nginx", "memory": 128}],
            }),
        ));
    }

    #[test]
    fn describe_services_rollout_state() {
        let h = EcsHandler::new();
        setup_cluster_and_task_family(&h, "cls", "fam");
        let create = h.handle(make_req(
            "CreateService",
            json!({
                "cluster": "cls", "serviceName": "s1", "desiredCount": 1,
                "taskDefinition": "fam:1",
            }),
        ));
        assert_eq!(create.status, 200);
        let created: Value = serde_json::from_str(&create.body).unwrap();
        let dep = created["service"]["deployments"][0].clone();
        // runningCount 0 < desiredCount 1 -> rollout still in progress.
        assert_eq!(dep["rolloutState"], json!("IN_PROGRESS"));
        assert_eq!(
            dep["rolloutStateReason"],
            json!(format!(
                "ECS deployment {} in progress.",
                dep["id"].as_str().unwrap()
            ))
        );

        // A zero desired count is already complete; the describe renderer
        // echoes the stored members.
        h.handle(make_req(
            "CreateService",
            json!({
                "cluster": "cls", "serviceName": "s0", "desiredCount": 0,
                "taskDefinition": "fam:1",
            }),
        ));
        let described = h.handle(make_req(
            "DescribeServices",
            json!({"cluster": "cls", "services": ["s0"]}),
        ));
        assert_eq!(described.status, 200);
        let described: Value = serde_json::from_str(&described.body).unwrap();
        let dep0 = described["services"][0]["deployments"][0].clone();
        assert_eq!(dep0["rolloutState"], json!("COMPLETED"));
        assert_eq!(
            dep0["rolloutStateReason"],
            json!(format!(
                "ECS deployment {} completed.",
                dep0["id"].as_str().unwrap()
            ))
        );
    }

    #[test]
    fn describe_services_rollout_failed_tasks() {
        // MOTO_ECS_SERVICE_FAILED_TASKS unset -> failedTasks defaults to 0.
        let h = EcsHandler::new();
        setup_cluster_and_task_family(&h, "cls", "fam");
        h.handle(make_req(
            "CreateService",
            json!({
                "cluster": "cls", "serviceName": "s1", "desiredCount": 1,
                "taskDefinition": "fam:1",
            }),
        ));
        let described = h.handle(make_req(
            "DescribeServices",
            json!({"cluster": "cls", "services": ["s1"]}),
        ));
        let described: Value = serde_json::from_str(&described.body).unwrap();
        assert_eq!(
            described["services"][0]["deployments"][0]["failedTasks"],
            json!(0)
        );
    }

    // ------------------------------------------------------------------
    // moto 5.2.3: run_task capacityProviderStrategy
    // ------------------------------------------------------------------

    #[test]
    fn run_task_capacity_provider_strategy() {
        let h = EcsHandler::new();
        setup_cluster_and_task_family(&h, "cls", "fam");
        let run = h.handle(make_req(
            "RunTask",
            json!({
                "cluster": "cls",
                "taskDefinition": "fam:1",
                "capacityProviderStrategy": [{"capacityProvider": "FARGATE_SPOT", "weight": 1}],
            }),
        ));
        assert_eq!(run.status, 200);
        let run: Value = serde_json::from_str(&run.body).unwrap();
        assert_eq!(
            run["tasks"][0]["capacityProviderName"],
            json!("FARGATE_SPOT")
        );

        // The stored member is echoed by describe_tasks.
        let arn = run["tasks"][0]["taskArn"].as_str().unwrap().to_string();
        let described = h.handle(make_req(
            "DescribeTasks",
            json!({"cluster": "cls", "tasks": [arn]}),
        ));
        let described: Value = serde_json::from_str(&described.body).unwrap();
        assert_eq!(
            described["tasks"][0]["capacityProviderName"],
            json!("FARGATE_SPOT")
        );
    }

    #[test]
    fn run_task_capacity_provider_launch_type_conflict() {
        let h = EcsHandler::new();
        setup_cluster_and_task_family(&h, "cls", "fam");
        let both = h.handle(make_req(
            "RunTask",
            json!({
                "cluster": "cls",
                "taskDefinition": "fam:1",
                "launchType": "FARGATE",
                "capacityProviderStrategy": [{"capacityProvider": "FARGATE_SPOT"}],
            }),
        ));
        assert_eq!(both.status, 400);
        let body: Value = serde_json::from_str(&both.body).unwrap();
        assert_eq!(body["__type"], json!("InvalidParameterException"));
        assert_eq!(
            body["message"],
            json!("Specify either Launch Type or Capacity Provider Strategy, but not both.")
        );
    }

    #[test]
    fn run_task_capacity_provider_cluster_default() {
        let h = EcsHandler::new();
        setup_cluster_and_task_family(&h, "cls", "fam");
        h.handle(make_req(
            "PutClusterCapacityProviders",
            json!({
                "cluster": "cls",
                "capacityProviders": ["FARGATE"],
                "defaultCapacityProviderStrategy":
                    [{"capacityProvider": "FARGATE", "weight": 1, "base": 1}],
            }),
        ));
        let run = h.handle(make_req(
            "RunTask",
            json!({
                "cluster": "cls",
                "taskDefinition": "fam:1",
            }),
        ));
        assert_eq!(run.status, 200);
        let run: Value = serde_json::from_str(&run.body).unwrap();
        assert_eq!(run["tasks"][0]["capacityProviderName"], json!("FARGATE"));

        // A strategy on the task wins over the cluster default.
        let run = h.handle(make_req(
            "RunTask",
            json!({
                "cluster": "cls",
                "taskDefinition": "fam:1",
                "capacityProviderStrategy": [{"capacityProvider": "EC2_PROVIDER"}],
            }),
        ));
        let run: Value = serde_json::from_str(&run.body).unwrap();
        assert_eq!(
            run["tasks"][0]["capacityProviderName"],
            json!("EC2_PROVIDER")
        );
    }
}
