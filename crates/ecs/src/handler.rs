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
            "DiscoverPollEndpoint" => self.discover_poll_endpoint(),
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
        // moto's Cluster model carries the cluster-default strategy so
        // run_task can fall back to it when neither a launch type nor an
        // explicit strategy is given (fork ecs/models.py run_task).
        let default_strategy: Vec<Value> = req
            .params
            .get("defaultCapacityProviderStrategy")
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
            "defaultCapacityProviderStrategy": default_strategy,
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
        // Real AWS: no clusters parameter (or empty) describes ALL clusters;
        // moto models the default cluster only. "Return everything when
        // nothing was named" — the empty-200 masked the whole cluster list.
        let ids = if ids.is_empty() {
            state
                .clusters
                .read()
                .keys()
                .cloned()
                .collect::<Vec<String>>()
        } else {
            ids
        };
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
                // moto's ClusterFailure synthesizes the typed cluster ARN
                // for the missing name (ecs/models.py:831-836) instead of
                // echoing the raw request reference.
                None => failures.push(json!({
                    "arn": format!("arn:aws:ecs:{}:{}:cluster/{}", req.region, req.account, resolve_name(&cid)),
                    "reason": "MISSING",
                })),
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
                // moto's CapacityProviderFailure spells the resource type
                // with an underscore in the failure ARN
                // (ecs/models.py:466-469): capacity_provider/{name}.
                None => failures.push(json!({
                    "arn": format!("arn:aws:ecs:{}:{}:capacity_provider/{}", req.region, req.account, n),
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

    /// moto's `register_task_definition` (ecs/models.py:1112-1199): fields
    /// are stored as passed (no invented cpu "256"/memory "512"/networkMode
    /// "awsvpc"/requiresCompatibilities; absent task/execution roles are
    /// omitted), `compatibilities` is computed (["EC2"] default — the whole
    /// ["EC2","FARGATE"] bucket for any other input), networkMode follows
    /// moto's rule (FARGATE → "awsvpc", absent → "bridge"), container
    /// definitions get moto's default members, the revision is
    /// max(revision)+1, and the FARGATE pidMode + container-def checks run
    /// in moto's order.
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
        let requires: Option<Vec<String>> = req
            .params
            .get("requiresCompatibilities")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str().map(String::from))
                    .collect()
            });
        let pid_mode = req
            .params
            .get("pidMode")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let fargate_required = requires
            .as_ref()
            .map(|r| r.iter().any(|c| c == "FARGATE"))
            .unwrap_or(false);
        // moto (models.py:1132-1137): the FARGATE pidMode check runs first.
        if fargate_required && !pid_mode.is_empty() && pid_mode != "task" {
            return AwsResponse::error(
                400,
                "ClientException",
                &format!(
                    "Tasks using the Fargate launch type do not support pidMode '{pid_mode}'. The supported value for pidMode is 'task'."
                ),
            );
        }
        let top_memory = req.params.get("memory").and_then(|v| v.as_str());
        let container_definitions: Vec<Value> = req
            .params
            .get("containerDefinitions")
            .and_then(|v| v.as_array())
            .map(|arr| arr.as_slice())
            .unwrap_or(&[])
            .iter()
            .map(|cd| {
                // moto's default_container_definition (models.py:159-171):
                // fields the request left out fall back to these.
                let mut full = json!({
                    "cpu": 0,
                    "portMappings": [],
                    "essential": true,
                    "environment": [],
                    "mountPoints": [],
                    "volumesFrom": [],
                });
                if let (Some(slot), Some(passed)) = (full.as_object_mut(), cd.as_object()) {
                    for (k, v) in passed {
                        slot.insert(k.clone(), v.clone());
                    }
                }
                full
            })
            .collect();
        for cd in &container_definitions {
            // moto's _validate_container_defs (models.py:1174-1199); the
            // capitalized variants only arrive from CloudFormation, which
            // never reaches this handler.
            let has = |keys: &[&str]| {
                keys.iter()
                    .any(|k| cd.get(*k).map(|v| !v.is_null()).unwrap_or(false))
            };
            if !has(&["name"]) {
                return AwsResponse::error(
                    400,
                    "ClientException",
                    "Container.name should not be null or empty.",
                );
            }
            let name = cd["name"].as_str().unwrap_or_default().to_string();
            if !has(&["image"]) {
                return AwsResponse::error(
                    400,
                    "ClientException",
                    "Container.image should not be null or empty.",
                );
            }
            if !has(&["memory", "memoryReservation"]) && top_memory.is_none() {
                return AwsResponse::error(
                    400,
                    "ClientException",
                    &format!(
                        "Invalid setting for container '{name}'. At least one of 'memory' or 'memoryReservation' must be specified."
                    ),
                );
            }
        }
        let td_arn;
        let tags: Vec<Value>;
        let td = {
            let mut tds = state.task_definitions.write();
            // moto numbers revisions via _get_last_task_definition_revision_id
            // (max revision) + 1 — len()+1 duplicates ids once a delete
            // removed a middle revision.
            let revisions = tds.entry(family.to_string()).or_default();
            let revision =
                revisions
                    .iter()
                    .map(|td| td["revision"].as_i64().unwrap_or(0))
                    .max()
                    .unwrap_or(0)
                    + 1;
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
            // compatibilities/networkMode: TaskDefinition.__init__
            // (models.py:186-196).
            let compatibilities: Value = if requires
                .as_ref()
                .map(|r| r.is_empty() || r == &["EC2"])
                .unwrap_or(true)
            {
                json!(["EC2"])
            } else {
                json!(["EC2", "FARGATE"])
            };
            let fargate_compat = compatibilities
                .as_array()
                .map(|c| c.iter().any(|c| c == &json!("FARGATE")))
                .unwrap_or(false);
            let mut td = json!({
                "taskDefinitionArn": td_arn.clone(),
                "family": family,
                "revision": revision,
                "compatibilities": compatibilities,
                "status": "ACTIVE",
                "containerDefinitions": container_definitions,
                "volumes": req.params.get("volumes").cloned().unwrap_or(json!([])),
                "placementConstraints": req.params.get("placementConstraints").cloned().unwrap_or(json!([])),
            });
            if fargate_compat {
                td["networkMode"] = json!("awsvpc");
            } else if let Some(nm) = req.params.get("networkMode").and_then(|v| v.as_str()) {
                td["networkMode"] = json!(nm);
            } else {
                td["networkMode"] = json!("bridge");
            }
            if let Some(cpu) = req.params.get("cpu").and_then(|v| v.as_str()) {
                td["cpu"] = json!(cpu);
            }
            if let Some(memory) = req.params.get("memory").and_then(|v| v.as_str()) {
                td["memory"] = json!(memory);
            }
            if let Some(task_role) = req.params.get("taskRoleArn").and_then(|v| v.as_str()) {
                td["taskRoleArn"] = json!(task_role);
            }
            if let Some(exec_role) =
                req.params.get("executionRoleArn").and_then(|v| v.as_str())
            {
                td["executionRoleArn"] = json!(exec_role);
            }
            if let Some(rc) = req.params.get("requiresCompatibilities") {
                td["requiresCompatibilities"] = rc.clone();
            }
            td["tags"] = json!(tags.clone());
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
            // moto's TaskDefinitionNotFoundException (ecs/exceptions.py:13-18)
            // — the message does NOT interpolate the reference.
            None => AwsResponse::error(400, "ClientException", "Unable to describe task definition."),
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

    /// moto's `deregister_task_definition` (ecs/models.py:1218-1238): the
    /// reference MUST be `family:revision` — a bare family raises
    /// `RevisionNotFoundException` ("Revision is missing."), a non-numeric
    /// revision `InvalidParameterException`, and a missing pair the
    /// `TaskDefinitionNotFoundException` text ("Unable to describe task
    /// definition."). A hit flips the stored revision INACTIVE in place
    /// (the twin's comment at resolve_task_definition used to decode the
    /// status handling — moto keeps INACTIVE rows discoverable).
    fn deregister_task_definition(&self, req: &AwsRequest) -> AwsResponse {
        let td_ref = req
            .params
            .get("taskDefinition")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let name = td_ref.split('/').next_back().unwrap_or(td_ref);
        let Some((family, revision)) = name.split_once(':') else {
            return AwsResponse::error(400, "ClientException", "Revision is missing.");
        };
        let Ok(revision) = revision.parse::<i64>() else {
            return AwsResponse::error(
                400,
                "InvalidParameterException",
                &format!("Invalid revision number. Number: {revision}"),
            );
        };
        let hit = state
            .task_definitions
            .read()
            .get(family)
            .and_then(|revisions| {
                revisions
                    .iter()
                    .find(|td| td["revision"].as_i64() == Some(revision))
                    .cloned()
            });
        let Some(mut td) = hit else {
            return AwsResponse::error(400, "ClientException", "Unable to describe task definition.");
        };
        {
            let mut tds = state.task_definitions.write();
            if let Some(revisions) = tds.get_mut(family) {
                for rev_td in revisions.iter_mut() {
                    if rev_td["revision"].as_i64() == Some(revision) {
                        rev_td["status"] = json!("INACTIVE");
                    }
                }
            }
        }
        td["status"] = json!("INACTIVE");
        AwsResponse::json(200, json!({ "taskDefinition": td }))
    }

    /// moto's `delete_task_definitions` (ecs/models.py:2350-2409): empty and
    /// >10 lists are client errors; every reference must be `family:rev`;
    /// an ACTIVE definition is refused with the still-in-ACTIVE failure row;
    /// a hit is REMOVED from the store (not just marked) and returned with
    /// status DELETE_IN_PROGRESS; failures echo the raw reference with
    /// moto's reason text (its DeleteTaskDefinitionFailure row shape).
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
        if refs.is_empty() {
            return AwsResponse::error(
                400,
                "InvalidParameterException",
                "size of TaskDefinition references list must be greater than 0",
            );
        }
        if refs.len() > 10 {
            return AwsResponse::error(
                400,
                "InvalidParameterException",
                "Size of TaskDefinition references list cannot exceed 10",
            );
        }
        let state = self.get_state(req.account, &req.region);
        let mut deleted: Vec<Value> = Vec::new();
        let mut failures: Vec<Value> = Vec::new();
        for td_ref in &refs {
            let name = td_ref.split('/').next_back().unwrap_or(td_ref);
            let parsed = name
                .split_once(':')
                .and_then(|(f, r)| r.parse::<i64>().ok().map(|rev| (f, rev)));
            let Some((family, revision)) = parsed else {
                // moto bare-families this row ("…identifier is invalid…");
                // its unguarded int() on a non-numeric revision would be a
                // crash there — answer the same invalid-identifier row.
                failures.push(json!({
                    "arn": td_ref,
                    "reason": "The specified task definition identifier is invalid. Specify a valid name or ARN and try again.",
                }));
                continue;
            };
            let hit = state
                .task_definitions
                .read()
                .get(family)
                .and_then(|revisions| {
                    revisions
                        .iter()
                        .find(|td| td["revision"].as_i64() == Some(revision))
                        .cloned()
                });
            match hit {
                Some(td) if td["status"] == json!("ACTIVE") => failures.push(json!({
                    "arn": td_ref,
                    "reason": "The specified task definition is still in ACTIVE status. Please deregister the target and try again.",
                })),
                Some(td) => {
                    {
                        // moto `del self.task_definitions[family][revision]`
                        // — the row leaves the store entirely.
                        let mut tds = state.task_definitions.write();
                        if let Some(revisions) = tds.get_mut(family) {
                            revisions.retain(|row| row["revision"].as_i64() != Some(revision));
                        }
                    }
                    let arn = td["taskDefinitionArn"]
                        .as_str()
                        .unwrap_or_default()
                        .to_string();
                    state.tags.write().remove(&arn);
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
        // moto's create_service launch-type handling (ecs/models.py:
        // 1622-1625): absent → "EC2", anything outside [EC2, FARGATE] is a
        // ClientException ("launch type should be one of [EC2,FARGATE]").
        let launch_type = req
            .params
            .get("launchType")
            .and_then(|v| v.as_str())
            .filter(|t| !t.is_empty())
            .unwrap_or("EC2");
        if launch_type != "EC2" && launch_type != "FARGATE" {
            return AwsResponse::error(
                400,
                "ClientException",
                "launch type should be one of [EC2,FARGATE]",
            );
        }
        let task_definition = req
            .params
            .get("taskDefinition")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        // moto 5.2.0: desiredCount is optional and defaults to 0 (fork
        // ecs/models.py create_service), so a service created without one
        // reports its rollout COMPLETED rather than IN_PROGRESS.
        let desired_count = req.params.get("desiredCount").cloned().unwrap_or(json!(0));
        // moto 5.2.0 computes the rollout state at service creation: the
        // primary deployment reports COMPLETED once its runningCount reaches
        // desiredCount, IN_PROGRESS while tasks are still coming up. moto
        // seeds that runningCount from MOTO_ECS_SERVICE_RUNNING (fork
        // ecs/models.py Service.__init__), with pendingCount the remainder.
        let running_count = env_running_tasks();
        let pending_count = desired_count.as_i64().unwrap_or(0) - running_count;
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
            "runningCount": running_count,
            "pendingCount": pending_count,
            "status": "ACTIVE",
            "launchType": launch_type,
            "networkConfiguration": req.params.get("networkConfiguration").cloned().unwrap_or(json!({})),
            "loadBalancers": req.params.get("loadBalancers").cloned().unwrap_or(json!([])),
            "deployments": [{
                "id": dep_id,
                "status": "PRIMARY",
                "taskDefinition": task_definition,
                "desiredCount": desired_count,
                "runningCount": running_count,
                "pendingCount": pending_count,
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
                // moto's describe_services failure ARN (ecs/models.py:
                // 1694-1698): the input as-is when it is already an ARN,
                // otherwise the synthesized typed service ARN.
                None => failures.push(json!({
                    "arn": if sid.starts_with("arn:") {
                        sid.clone()
                    } else {
                        format!(
                            "arn:aws:ecs:{}:{}:service/{}",
                            req.region,
                            req.account,
                            resolve_name(sid)
                        )
                    },
                    "reason": "MISSING",
                })),
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
            // moto's update syncs the service-level counts (models.py:
            // 1721-1735) — running=desired, pending=0 — so a scaled service
            // no longer reports runningCount 0 IN_PROGRESS forever. The
            // DEPLOYMENT row is left stale on purpose: moto never rewrites
            // deployments in update_service either, and botocore's
            // Deployment member is `status` (not `lastStatus`) — the old
            // `lastStatus == "PRIMARY"` gate never matched anything and
            // rewrote nothing (dead code, member name wrong).
            service["runningCount"] = count.clone();
            service["pendingCount"] = json!(0);
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
        // moto 5.2.4 (models.py:2293-2313): resolve the task set against the
        // service's set, promote it PRIMARY, demote every other one back to
        // ACTIVE, and sync the service's task definition + load balancers
        // onto the promoted set's values. Success is the mainline; the
        // declared TaskSetNotFoundException family (ClientException:
        // "The specified task set does not exist.") fires only when the
        // row is missing — the old unconditional 400 made the op
        // unreachable.
        let ts_ref = resolve_name(
            req.params
                .get("primaryTaskSet")
                .and_then(|v| v.as_str())
                .unwrap_or_default(),
        )
        .to_string();
        let task_set_not_found = || {
            AwsResponse::error(
                400,
                "ClientException",
                "The specified task set does not exist.",
            )
        };
        {
            let mut ts_maps = state.task_sets.write();
            let Some(ts_map) = ts_maps.get_mut(&svc_arn) else {
                return task_set_not_found();
            };
            if !ts_map.contains_key(&ts_ref) {
                return task_set_not_found();
            }
            for (id, ts) in ts_map.iter_mut() {
                ts["status"] = if id == &ts_ref {
                    json!("PRIMARY")
                } else {
                    json!("ACTIVE")
                };
            }
            let promoted = ts_map.get(&ts_ref).cloned().unwrap_or_default();
            if let Some(svc_obj) = state
                .services
                .write()
                .get_mut(cluster_name)
                .and_then(|m| m.get_mut(svc_name))
            {
                svc_obj["taskDefinition"] = promoted["taskDefinition"].clone();
                svc_obj["loadBalancers"] = promoted["loadBalancers"].clone();
            }
            AwsResponse::json(200, json!({ "taskSet": promoted }))
        }
    }

    // ------------------------------------------------------------------
    // Tasks
    // ------------------------------------------------------------------

    /// moto's `run_task` (ecs/models.py:1240-1359): the launchType
    /// vocabulary check, the launchType/strategy conflict, the cluster
    /// lookup, the task-definition resolution (moto's family resolution —
    /// "Unable to describe task definition."), the cluster default strategy
    /// fallback, and placement: FARGATE/FARGATE_SPOT (or the FARGATE launch
    /// type) tasks need no instance; any other capacity provider needs an
    /// ACTIVE container instance in the cluster — moto raises
    /// `No instances found in cluster …` there (a crash page in moto); the
    /// twin answers real-AWS's RunTask failure row (200, tasks=[], a
    /// RESOURCE:TASK failure citing moto's sentence in detail). startedBy
    /// and group are stored on the task like moto; launchType defaults to
    /// "" (moto stores `launch_type or ""`; it does NOT default FARGATE).
    fn run_task(&self, req: &AwsRequest) -> AwsResponse {
        let cluster_name = resolve_name(
            req.params
                .get("cluster")
                .map(|v| v.as_str().unwrap_or("default"))
                .unwrap_or("default"),
        );
        // moto's check order (models.py:1254-1262): vocabulary first, then
        // the conflict.
        let launch_type_param = req
            .params
            .get("launchType")
            .and_then(|v| v.as_str())
            .filter(|t| !t.is_empty());
        if let Some(t) = launch_type_param {
            if t != "EC2" && t != "FARGATE" && t != "EXTERNAL" {
                return AwsResponse::error(
                    400,
                    "InvalidParameterException",
                    "launch type should be one of [EC2,FARGATE,EXTERNAL]",
                );
            }
        }
        let explicit_strategy = req
            .params
            .get("capacityProviderStrategy")
            .and_then(|v| v.as_array())
            .filter(|a| !a.is_empty());
        let has_launch_type = launch_type_param.is_some();
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
            // moto surfaces its TaskDefinitionNotFoundException here
            // (models.py:1281 → exceptions.py:13-18).
            None => {
                return AwsResponse::error(
                    400,
                    "ClientException",
                    "Unable to describe task definition.",
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
        // FARGATE/FARGATE_SPOT are managed providers that need no container
        // instance (models.py:1274-1279).
        let use_fargate = launch_type_param == Some("FARGATE")
            || capacity_provider_name
                .as_deref()
                .map(|c| c == "FARGATE" || c == "FARGATE_SPOT")
                .unwrap_or(false);
        // moto places EC2 runs onto the cluster's ACTIVE container
        // instances and raises `No instances found in cluster …` when there
        // is none (models.py:1313-1317). That raise is a moto crash page —
        // the twin answers real-AWS's RunTask failure row instead (200, no
        // tasks, one RESOURCE:TASK failure carrying moto's sentence).
        let active_instances: Vec<String> = if use_fargate {
            Vec::new()
        } else {
            state
                .container_instances
                .read()
                .get(cluster_name)
                .map(|m| {
                    let mut arns: Vec<String> = m
                        .values()
                        .filter(|ci| {
                            ci.get("status").and_then(|v| v.as_str()) == Some("ACTIVE")
                        })
                        .filter_map(|ci| {
                            ci["containerInstanceArn"].as_str().map(String::from)
                        })
                        .collect();
                    arns.sort();
                    arns
                })
                .unwrap_or_default()
        };
        if !use_fargate && active_instances.is_empty() {
            return AwsResponse::json(
                200,
                json!({
                    "tasks": [],
                    "failures": [{
                        "arn": td["taskDefinitionArn"].clone(),
                        "reason": "RESOURCE:TASK",
                        "detail": format!("No instances found in cluster {cluster_name}."),
                    }]
                }),
            );
        }
        // moto's Task.__init__ (models.py:358-362): an awsvpc task needs a
        // network configuration.
        if td.get("networkMode").and_then(|v| v.as_str()) == Some("awsvpc")
            && req.params.get("networkConfiguration").is_none()
        {
            return AwsResponse::error(
                400,
                "InvalidParameterException",
                "Network Configuration must be provided when networkMode 'awsvpc' is specified.",
            );
        }
        let started_by = req
            .params
            .get("startedBy")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        // moto (models.py:1282-1284): group defaults to family:{family},
        // otherwise the explicit `group` parameter.
        let group = req
            .params
            .get("group")
            .and_then(|v| v.as_str())
            .filter(|g| !g.is_empty())
            .map(String::from)
            .unwrap_or_else(|| format!("family:{}", td["family"].as_str().unwrap_or_default()));
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
        let mut task_index: i64 = 0;
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
                // moto's Task model carries NO cpu/memory members — the twin
                // used to invent "256"/"512" here from the old task-def
                // defaults.
                "launchType": launch_type_param.unwrap_or(""),
                "startedAt": now,
                "createdAt": now,
                "containers": containers.clone(),
                "tags": task_tags.clone(),
                "overrides": req.params.get("overrides").cloned().unwrap_or(json!({})),
                "startedBy": started_by.clone(),
                "group": group.clone(),
            });
            // moto 5.2.3 surfaces the strategy's primary provider as the Task
            // model's capacityProviderName member.
            if let Some(provider) = &capacity_provider_name {
                task["capacityProviderName"] = json!(provider);
            }
            // EC2 placements carry the instance they landed on (moto's
            // container_instance_arn; placed round-robin — moto's
            // resource-check placement is not modeled).
            if !active_instances.is_empty() {
                task["containerInstanceArn"] = json!(
                    &active_instances[(task_index % active_instances.len() as i64) as usize]
                );
            }
            task_index += 1;
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

    /// moto's `start_task` (ecs/models.py:1439-1485): the empty
    /// containerInstances list is a ClientException ("Container Instances
    /// cannot be empty."), each reference resolves against the cluster's
    /// instance map, and the created task carries containerInstanceArn,
    /// startedBy and group as moto stores them. moto's unknown instance
    /// KeyErrors into a crash; the twin answers moto's
    /// ContainerInstanceFailure row for those instead.
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
                    "Unable to describe task definition.",
                )
            }
        };
        // moto (models.py:1458-1459).
        let instance_refs: Vec<String> = req
            .params
            .get("containerInstances")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        if instance_refs.is_empty() {
            return AwsResponse::error(
                400,
                "ClientException",
                "Container Instances cannot be empty.",
            );
        }
        // moto (models.py:1282-1284): group defaults to family:{family}.
        let group = req
            .params
            .get("group")
            .and_then(|v| v.as_str())
            .filter(|g| !g.is_empty())
            .map(String::from)
            .unwrap_or_else(|| format!("family:{}", td["family"].as_str().unwrap_or_default()));
        let started_by = req
            .params
            .get("startedBy")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let now = now_f64();
        let mut tasks: Vec<Value> = Vec::new();
        let mut failures: Vec<Value> = Vec::new();
        {
            let mut cis = state.container_instances.write();
            let cluster_cis = cis.entry(cluster_name.to_string()).or_default();
            for reference in &instance_refs {
                let ci_id = resolve_name(reference).to_string();
                let Some(ci) = cluster_cis.get_mut(&ci_id) else {
                    // moto's ContainerInstanceFailure("MISSING", id, …)!
                    failures.push(json!({
                        "arn": format!(
                            "arn:aws:ecs:{}:{}:container-instance/{}",
                            req.region, req.account, ci_id
                        ),
                        "reason": "MISSING",
                    }));
                    continue;
                };
                let ci_arn = ci["containerInstanceArn"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string();
                let task_id = uuid::Uuid::new_v4().to_string();
                let task_arn = format!(
                    "arn:aws:ecs:{}:{}:task/{}/{}",
                    req.region, req.account, cluster_name, task_id
                );
                let task = json!({
                    "taskArn": task_arn.clone(),
                    "taskDefinitionArn": td["taskDefinitionArn"].clone(),
                    "clusterArn": format!("arn:aws:ecs:{}:{}:cluster/{}", req.region, req.account, cluster_name),
                    "containerInstanceArn": ci_arn.clone(),
                    "lastStatus": "RUNNING",
                    "desiredStatus": "RUNNING",
                    "createdAt": now,
                    "startedAt": now,
                    "tags": [],
                    "startedBy": started_by.clone(),
                    "group": group.clone(),
                });
                state
                    .tasks
                    .write()
                    .entry(cluster_name.to_string())
                    .or_default()
                    .insert(task_id, task.clone());
                let running = ci.get("runningTasksCount").and_then(|v| v.as_i64()).unwrap_or(0);
                ci["runningTasksCount"] = json!(running + 1);
                tasks.push(task);
            }
        }
        AwsResponse::json(200, json!({ "tasks": tasks, "failures": failures }))
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

    /// moto's `describe_tasks` (ecs/models.py:1487-1494): the cluster must
    /// exist and an empty task list is
    /// InvalidParameterException("Tasks cannot be empty.").
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
        if !state.clusters.read().contains_key(cluster_name) {
            return cluster_not_found(cluster_name);
        }
        if ids.is_empty() {
            return AwsResponse::error(400, "InvalidParameterException", "Tasks cannot be empty.");
        }
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

    /// moto's `list_tasks` (ecs/models.py:1509-1561): the containerInstance
    /// (substring), family (exact, against ACTIVE task-definition ARNs),
    /// startedBy (equality) and desiredStatus (only when the request names
    /// one — moto does NOT default to RUNNING; it returns every status)
    /// filters, all optional.
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
            .filter(|s| !s.is_empty());
        let container_instance = req
            .params
            .get("containerInstance")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty());
        let family = req
            .params
            .get("family")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty());
        let started_by = req
            .params
            .get("startedBy")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty());
        let state = self.get_state(req.account, &req.region);
        // moto's family filter narrows to the ACTIVE revisions of the exact
        // family (models.py:1537-1544 via list_task_definitions, whose
        // default status is ACTIVE).
        let family_arns: Vec<String> = family
            .map(|fam| {
                let mut arns = Vec::new();
                let tds = state.task_definitions.read();
                let Some(revisions) = tds.get(fam) else {
                    return arns;
                };
                for td in revisions {
                    if td["status"] == json!("ACTIVE") {
                        arns.push(td["taskDefinitionArn"].as_str().unwrap_or_default().to_string());
                    }
                }
                arns
            })
            .unwrap_or_default();
        let arns: Vec<String> = state
            .tasks
            .read()
            .get(cluster_name)
            .map(|m| {
                m.values()
                    .filter(|t| {
                        status_filter
                            .map(|want| t.get("desiredStatus").and_then(|v| v.as_str()) == Some(want))
                            .unwrap_or(true)
                    })
                    .filter(|t| {
                        container_instance
                            .map(|ci| {
                                t.get("containerInstanceArn")
                                    .and_then(|v| v.as_str())
                                    .map(|arn| arn.contains(ci))
                                    .unwrap_or(false)
                            })
                            .unwrap_or(true)
                    })
                    .filter(|t| {
                        family
                            .map(|_| {
                                family_arns.contains(
                                    &t["taskDefinitionArn"].as_str().unwrap_or_default().to_string(),
                                )
                            })
                            .unwrap_or(true)
                    })
                    .filter(|t| {
                        started_by
                            .map(|by| t.get("startedBy").and_then(|v| v.as_str()) == Some(by))
                            .unwrap_or(true)
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
        // moto's deregister guard (ecs/models.py:1901-1904): without
        // `force`, an instance still running tasks is refused with
        // InvalidParameterException "Found running tasks on the instance.".
        let running: i64 = state
            .tasks
            .read()
            .get(cluster_name)
            .map(|m| {
                m.values()
                    .filter(|t| {
                        t.get("containerInstanceArn")
                            .and_then(|v| v.as_str())
                            .map(|arn| arn.ends_with(ci_id))
                            .unwrap_or(false)
                    })
                    .filter(|t| t["desiredStatus"] == json!("RUNNING"))
                    .count() as i64
            })
            .unwrap_or(0);
        let mut cis = state.container_instances.write();
        if !req
            .params
            .get("force")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
            && running > 0
        {
            return AwsResponse::error(
                400,
                "InvalidParameterException",
                "Found running tasks on the instance.",
            );
        }
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

    /// moto's `describe_container_instances` (ecs/models.py:1802-1828): the
    /// cluster must exist, an empty list is a ClientException ("Container
    /// Instances cannot be empty."), and missing ids answer moto's
    /// ContainerInstanceFailure rows — a synthesized typed ARN (the raw
    /// reference used to ride through verbatim).
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
        if !state.clusters.read().contains_key(cluster_name) {
            return cluster_not_found(cluster_name);
        }
        if refs.is_empty() {
            return AwsResponse::error(
                400,
                "ClientException",
                "Container Instances cannot be empty.",
            );
        }
        let mut found: Vec<Value> = Vec::new();
        let mut failures: Vec<Value> = Vec::new();
        let cis = state.container_instances.read();
        let cluster_cis = cis.get(cluster_name);
        for reference in &refs {
            let ci_id = resolve_name(reference);
            let lookup = cluster_cis
                .map(|m| {
                    m.get(ci_id).cloned().or_else(|| {
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
                None => failures.push(json!({
                    "arn": format!("arn:aws:ecs:{}:{}:container-instance/{}", req.region, req.account, ci_id),
                    "reason": "MISSING",
                })),
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
        // moto sorts the ARNs (ecs/models.py:1800).
        let mut arns: Vec<String> = state
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
        arns.sort();
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
        // moto (ecs/models.py:1835-1839): the status is uppercased and
        // validated against [ACTIVE, DRAINING].
        let new_status = req
            .params
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("ACTIVE")
            .to_uppercase();
        if new_status != "ACTIVE" && new_status != "DRAINING" {
            return AwsResponse::error(
                400,
                "InvalidParameterException",
                "Container instance status should be one of [ACTIVE, DRAINING]",
            );
        }
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
                        None => failures.push(json!({
                            "arn": format!(
                                "arn:aws:ecs:{}:{}:container-instance/{}",
                                req.region,
                                req.account,
                                resolve_name(&r)
                            ),
                            "reason": "MISSING",
                        })),
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
        // moto (ecs/models.py:2170-2172): launchType absent → "EC2",
        // anything outside [EC2, FARGATE] is a ClientException.
        let launch_type = req
            .params
            .get("launchType")
            .and_then(|v| v.as_str())
            .filter(|t| !t.is_empty())
            .unwrap_or("EC2");
        if launch_type != "EC2" && launch_type != "FARGATE" {
            return AwsResponse::error(
                400,
                "ClientException",
                "launch type should be one of [EC2,FARGATE]",
            );
        }
        // moto resolves the task definition via describe_task_definition
        // (models.py:2199-2202) — the stored taskDefinition is the
        // versioned ARN, and an unknown reference is a ClientException
        // ("Unable to describe task definition.").
        let td_ref = req
            .params
            .get("taskDefinition")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let resolved = resolve_task_definition(&state, td_ref);
        let Some(td) = resolved else {
            return AwsResponse::error(400, "ClientException", "Unable to describe task definition.");
        };
        let task_def_arn = td["taskDefinitionArn"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let task_set = json!({
            "id": ts_id,
            "taskSetArn": ts_arn.clone(),
            "serviceArn": svc_arn.clone(),
            "clusterArn": format!("arn:aws:ecs:{}:{}:cluster/{}", req.region, req.account, cluster_name),
            "taskDefinition": task_def_arn,
            "status": "ACTIVE",
            "pendingCount": 0,
            "runningCount": 0,
            "stabilityStatus": "STEADY_STATE",
            "scale": req.params.get("scale").cloned()
                .unwrap_or(json!({ "value": 100.0, "unit": "PERCENT" })),
            "launchType": launch_type,
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

    /// moto's attribute target checks (ecs/models.py _put_attribute
    /// 1939-1973): with a `targetId` (no `targetType`) the reference is a
    /// container-instance ARN resolved against the cluster's instances;
    /// with a `targetType` it MUST be "container-instance" and the id is
    /// used raw; anything unresolved raises TargetNotFoundException
    /// ("Could not find {targetId}"). Attributes carry no task-definition
    /// targets — the target surface is container instances only.
    fn validate_attribute_target(&self, state: &EcsState, cluster_name: &str, attr: &Value) -> Option<AwsResponse> {
        let target_id = attr.get("targetId").and_then(|v| v.as_str());
        let target_type = attr.get("targetType").and_then(|v| v.as_str());
        let not_found = |id: &str| {
            AwsResponse::error(
                400,
                "TargetNotFoundException",
                &format!("Could not find {id}"),
            )
        };
        match (target_id, target_type) {
            (None, None) => None,
            (Some(id), None) => {
                let arn = id.rsplit('/').next().unwrap_or(id);
                if state
                    .container_instances
                    .read()
                    .get(cluster_name)
                    .map(|m| m.contains_key(arn))
                    .unwrap_or(false)
                {
                    None
                } else {
                    Some(not_found(id))
                }
            }
            (id_opt, Some(kind)) => {
                if kind != "container-instance" {
                    return Some(not_found(id_opt.unwrap_or_default()));
                }
                match id_opt {
                    Some(id) => {
                        // moto indexes the raw id (models.py:1967); real AWS
                        // accepts full ARNs here too — resolve the ARN's
                        // last segment the same way the no-targetType branch
                        // does (a benign superset over moto's uuid-only
                        // lookup).
                        let key = id.rsplit('/').next().unwrap_or(id);
                        if state
                            .container_instances
                            .read()
                            .get(cluster_name)
                            .map(|m| m.contains_key(key))
                            .unwrap_or(false)
                        {
                            None
                        } else {
                            Some(not_found(id))
                        }
                    }
                    None => Some(not_found("")),
                }
            }
        }
    }

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
            .map(|arr| arr.as_slice())
            .unwrap_or(&[])
            .to_vec();
        for attr in &attrs {
            if let Some(err) = self.validate_attribute_target(&state, cluster_name, attr) {
                return err;
            }
        }
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
            .map(|arr| arr.as_slice())
            .unwrap_or(&[])
            .to_vec();
        for attr in &attrs_to_del {
            if let Some(err) = self.validate_attribute_target(&state, cluster_name, attr) {
                return err;
            }
        }
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

    /// moto's `list_attributes` (ecs/models.py:1975-1986): targetType must
    /// be "container-instance" (absent or anything else is
    /// InvalidParameterException "targetType must be container-instance").
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
        if target_type != "container-instance" {
            return AwsResponse::error(
                400,
                "InvalidParameterException",
                "targetType must be container-instance",
            );
        }
        let name_filter = req.params.get("attributeName").and_then(|v| v.as_str());
        let state = self.get_state(req.account, &req.region);
        // moto lists attributes stored against container instances and
        // renders {name, targetId[, value]} — no targetType member at all
        // (models.py:1998-2011 + the responses transform); target
        // validation meanwhile guarantees the entries are
        // container-instance targets.
        let filtered: Vec<Value> = state
            .attributes
            .read()
            .get(cluster_name)
            .map(|attrs| {
                attrs
                    .iter()
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
        // moto raises UnknownAccountSettingException — a Client-side
        // InvalidParameterException with the literal one-line member list
        // (ecs/exceptions.py:51-55, models.py:2315-2334) — for an unknown
        // name on the list path.
        if let Some(n) = name {
            if !ACCOUNT_SETTING_NAMES.contains(&n) {
                return AwsResponse::error(
                    400,
                    "InvalidParameterException",
                    &unknown_account_setting_message(),
                );
            }
        }
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
        // Same moto oracle as list_account_settings:
        // UnknownAccountSettingException text (moto has no
        // PutAccountSettingDefault route; the twin carries it because real
        // AWS does, so the literalUnknownAccountSettingException member
        // list is the only modeled surface for the error shape).
        if !ACCOUNT_SETTING_NAMES.contains(&name) {
            return AwsResponse::error(
                400,
                "InvalidParameterException",
                &unknown_account_setting_message(),
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

    /// GetTaskProtection with durable state: the stored
    /// UpdateTaskProtection flag is returned (keyed by the task's ARN) —
    /// the pair used to be a tautology (update echoed the request, get
    /// always answered false). moto 5.2.4 does not implement these list
    /// members at all, so the protection surface is stored per task-ARN
    /// like real AWS and verified by the round-trip test below.
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
        let protection = state.task_protection.read();
        let cluster_tasks = tasks.get(cluster_name);
        if task_refs.is_empty() {
            for task in cluster_tasks.iter().flat_map(|m| m.values()) {
                protected.push(json!({
                    "taskArn": task["taskArn"].clone(),
                    "protectionEnabled": protection
                        .get(task["taskArn"].as_str().unwrap_or_default())
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false),
                }));
            }
        } else {
            for task_ref in task_refs {
                let short = resolve_name(&task_ref);
                let found = cluster_tasks
                    .and_then(|m| m.get(short).cloned().or_else(|| m.get(&task_ref).cloned()))
                    .map(|t| {
                        let arn = t["taskArn"].as_str().unwrap_or_default().to_string();
                        json!({
                            "taskArn": arn,
                            "protectionEnabled": protection
                                .get(&arn)
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false),
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

    /// UpdateTaskProtection stores the flag per task ARN (the returned
    /// entries cite the STORED task ARN, not the raw request reference)
    /// and reports missing tasks in the failures list, matching the
    /// get-side shape.
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
        let mut failures: Vec<Value> = Vec::new();
        let stored: Vec<(String, String)> = task_refs
            .iter()
            .filter_map(|r| {
                let short = resolve_name(r);
                state
                    .tasks
                    .read()
                    .get(cluster_name)
                    .and_then(|m| m.get(short).cloned().or_else(|| m.get(r).cloned()))
                    .map(|t| (r.clone(), t["taskArn"].as_str().unwrap_or_default().to_string()))
            })
            .collect();
        for (_task_ref, arn) in stored {
            state.task_protection.write().insert(arn.clone(), json!(enabled));
            protected.push(json!({
                "taskArn": arn,
                "protectionEnabled": enabled,
            }));
        }
        for task_ref in &task_refs {
            let short = resolve_name(task_ref);
            let exists = state.tasks.read().get(cluster_name).map(|m| {
                m.contains_key(short)
                    || m.values().any(|t| {
                        t.get("taskArn").and_then(|a| a.as_str()) == Some(task_ref.as_str())
                    })
            });
            if exists != Some(true) {
                failures.push(json!({
                    "arn": task_ref.clone(),
                    "reason": "TASK_NOT_FOUND",
                    "detail": format!("The referenced task was not found: {}", task_ref),
                }));
            }
        }
        AwsResponse::json(
            200,
            json!({ "protectedTasks": protected, "failures": failures }),
        )
    }

    /// The agent poll endpoint. moto's stub (ecs/responses.py:453-458)
    /// answers static `http://localhost`; the twin follows the repo's
    /// no-portability rule and reports the port the server is actually
    /// reachable on: `GATEWAY_PORT` (the env other crates build gateway
    /// URLs from, crates/lambda/src/docker_runtime.rs), then
    /// `ROBOTOCORE_PORT`/`RUST_PORT`, falling back to 4566 with a log line
    /// when none is set.
    fn discover_poll_endpoint(&self) -> AwsResponse {
        let endpoint = poll_endpoint_value();
        AwsResponse::json(
            200,
            json!({
                "endpoint": endpoint,
                "telemetryEndpoint": endpoint,
            }),
        )
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

/// DiscoverPollEndpoint's base URL: env-derived gateway port, logging the
/// fallback when no port env is set (the repo's portability rule).
fn poll_endpoint_value() -> String {
    match std::env::var("GATEWAY_PORT")
        .ok()
        .filter(|v| !v.is_empty())
        .or_else(|| {
            std::env::var("ROBOTOCORE_PORT")
                .ok()
                .filter(|v| !v.is_empty())
        })
        .or_else(|| std::env::var("RUST_PORT").ok().filter(|v| !v.is_empty()))
    {
        Some(port) => format!("http://localhost:{port}"),
        None => {
            eprintln!(
                "[ecs] DiscoverPollEndpoint: GATEWAY_PORT/ROBOTOCORE_PORT/RUST_PORT unset; \
                 falling back to http://localhost:4566"
            );
            "http://localhost:4566".to_string()
        }
    }
}

/// Python `time.time()`-style epoch seconds with sub-second precision.
fn now_f64() -> f64 {
    UNIX_EPOCH.elapsed().map(|d| d.as_secs_f64()).unwrap_or(0.0)
}

/// moto's env-var count parsing (fork ecs/models.py Service.__init__):
/// `max(int(value), 0)` with a ValueError falling back to 0 — negative
/// values are not allowed. Python's int() accepts surrounding whitespace,
/// so trim before parsing.
fn parse_env_count(value: Option<&str>) -> i64 {
    value
        .and_then(|v| v.trim().parse::<i64>().ok())
        .map_or(0, |v| v.max(0))
}

/// `MOTO_ECS_SERVICE_FAILED_TASKS` (moto 5.2.0): failedTasks reported in the
/// service's primary deployment; non-numeric or negative values fall back to 0.
fn env_failed_tasks() -> i64 {
    parse_env_count(
        std::env::var("MOTO_ECS_SERVICE_FAILED_TASKS")
            .ok()
            .as_deref(),
    )
}

/// `MOTO_ECS_SERVICE_RUNNING` (moto 5.2.0, fork ecs/models.py:515): the
/// number of running tasks the primary deployment reports; pendingCount and
/// the rollout state follow from it.
fn env_running_tasks() -> i64 {
    parse_env_count(std::env::var("MOTO_ECS_SERVICE_RUNNING").ok().as_deref())
}

fn cluster_not_found(name: &str) -> AwsResponse {
    AwsResponse::error(
        400,
        "ClusterNotFoundException",
        &format!("Cluster {} not found.", name),
    )
}

/// moto's UnknownAccountSettingException literal (ecs/exceptions.py:51-55).
fn unknown_account_setting_message() -> String {
    "unknown should be one of [serviceLongArnFormat,taskLongArnFormat,containerInstanceLongArnFormat,containerLongArnFormat,awsvpcTrunking,containerInsights,dualStackIPv6]".to_string()
}

fn service_not_found(name: &str) -> AwsResponse {
    AwsResponse::error(
        400,
        "ServiceNotFoundException",
        &format!("Service {} not found.", name),
    )
}

/// Resolve a task-definition reference: family, `family:revision`, or ARN.
///
/// moto's family resolution (fork ecs/models.py
/// `_get_last_task_definition_revision_id`, via
/// `describe_task_definition`): a bare family resolves to the LATEST
/// revision by number REGARDLESS of status — deregistered (INACTIVE) ones
/// are discoverable, so a bare family keeps resolving after a
/// deregister. A `family:rev` reference needs that exact revision, any
/// status.
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
            .max_by_key(|td| td["revision"].as_i64().unwrap_or(0))
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

    /// moto 5.2.4 (models.py:2293-2313): a promote call succeeds, flips
    /// statuses (PRIMARY vs ACTIVE), syncs the service placement, and its
    /// missing-row error is the ClientException-shape 400.
    #[test]
    fn update_service_primary_task_set_promotes() {
        let h = EcsHandler::new();
        h.handle(make_req("CreateCluster", json!({"clusterName": "pc"})));
        h.handle(make_req(
            "RegisterTaskDefinition",
            json!({
                "family": "p-fam",
                "containerDefinitions": [{"name": "app", "image": "nginx", "memory": 128}],
            }),
        ));
        h.handle(make_req(
            "CreateService",
            json!({"cluster": "pc", "serviceName": "ps", "taskDefinition": "p-fam"}),
        ));
        let create1 = serde_json::from_str::<Value>(
            &h.handle(make_req(
                "CreateTaskSet",
                json!({"cluster": "pc", "service": "ps", "taskDefinition": "p-fam"}),
            ))
            .body,
        )
        .unwrap();
        let created = serde_json::from_str::<Value>(
            &h.handle(make_req(
                "CreateTaskSet",
                json!({"cluster": "pc", "service": "ps", "taskDefinition": "p-fam"}),
            ))
            .body,
        )
        .unwrap();
        let second_arn = created["taskSet"]["taskSetArn"]
            .as_str()
            .expect("second task set")
            .to_string();
        assert_ne!(
            create1["taskSet"]["taskSetArn"], created["taskSet"]["taskSetArn"],
            "two distinct task sets"
        );

        // Promote the second: 200 with the taskSet echo.
        let promote = serde_json::from_str::<Value>(
            &h.handle(make_req(
                "UpdateServicePrimaryTaskSet",
                json!({"cluster": "pc", "service": "ps", "primaryTaskSet": second_arn}),
            ))
            .body,
        )
        .unwrap();
        assert_eq!(promote["taskSet"]["status"], json!("PRIMARY"));

        // DescribeTaskSets shows the promoted one as PRIMARY and the other
        // demoted back to ACTIVE; the service placement follows the winner.
        let sets = serde_json::from_str::<Value>(
            &h.handle(make_req(
                "DescribeTaskSets",
                json!({"cluster": "pc", "service": "ps"}),
            ))
            .body,
        )
        .unwrap();
        let statuses: Vec<(&str, &str)> = sets["taskSets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| {
                (
                    t["taskSetArn"].as_str().unwrap(),
                    t["status"].as_str().unwrap(),
                )
            })
            .collect();
        assert!(
            statuses.contains(&(second_arn.as_str(), "PRIMARY")),
            "promoted is PRIMARY: {sets}"
        );
        assert_eq!(
            statuses.iter().filter(|(_, s)| *s == "ACTIVE").count(),
            1,
            "the other set demotes to ACTIVE"
        );

        // Missing task set → ClientException 400 (TaskSetNotFound family).
        let miss = h.handle(make_req(
            "UpdateServicePrimaryTaskSet",
            json!({"cluster": "pc", "service": "ps", "primaryTaskSet": "arn:aws:ecs:us-east-1:123456789012:task-set/pc/ps/nope"}),
        ));
        assert_eq!(miss.status, 400);
        assert!(miss.body.contains("ClientException"));
        assert!(miss.body.contains("The specified task set does not exist."));
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
    fn create_service_defaults_desired_count_to_zero() {
        // moto 5.2.0 create_service: desiredCount is optional and defaults
        // to 0, so the primary deployment is already COMPLETED.
        let h = EcsHandler::new();
        setup_cluster_and_task_family(&h, "cls", "fam");
        let create = h.handle(make_req(
            "CreateService",
            json!({"cluster": "cls", "serviceName": "s0", "taskDefinition": "fam:1"}),
        ));
        let created: Value = serde_json::from_str(&create.body).unwrap();
        let svc = &created["service"];
        assert_eq!(svc["desiredCount"], json!(0));
        assert_eq!(svc["runningCount"], json!(0));
        assert_eq!(svc["pendingCount"], json!(0));
        let dep = &svc["deployments"][0];
        assert_eq!(dep["desiredCount"], json!(0));
        assert_eq!(dep["rolloutState"], json!("COMPLETED"));
        assert_eq!(
            dep["rolloutStateReason"],
            json!(format!(
                "ECS deployment {} completed.",
                dep["id"].as_str().unwrap()
            ))
        );
    }

    #[test]
    fn env_count_parsing_matches_moto() {
        // moto's `max(int(value), 0)` with a ValueError caught: absent,
        // empty and non-numeric values fall back to 0, negatives clamp.
        assert_eq!(parse_env_count(None), 0);
        assert_eq!(parse_env_count(Some("")), 0);
        assert_eq!(parse_env_count(Some("0")), 0);
        assert_eq!(parse_env_count(Some("3")), 3);
        assert_eq!(parse_env_count(Some("+3")), 3);
        assert_eq!(parse_env_count(Some("-5")), 0);
        assert_eq!(parse_env_count(Some("running")), 0);
        // Python's int(' 2 ') accepts surrounding whitespace.
        assert_eq!(parse_env_count(Some(" 2 ")), 2);
        assert_eq!(parse_env_count(Some("\t2\n")), 2);
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

        // A strategy on the task wins over the cluster default — and a
        // non-FARGATE placement needs a registered ACTIVE container
        // instance (moto raises `No instances found in cluster …` per
        // models.py:1313-1317; the twin answers a RESOURCE:TASK failure row
        // when there is none).
        let run = h.handle(make_req(
            "RunTask",
            json!({
                "cluster": "cls",
                "taskDefinition": "fam:1",
                "capacityProviderStrategy": [{"capacityProvider": "EC2_PROVIDER"}],
            }),
        ));
        assert_eq!(run.status, 200);
        let run: Value = serde_json::from_str(&run.body).unwrap();
        assert!(
            run["tasks"].as_array().map(|t| t.is_empty()).unwrap_or(true),
            "no instances -> no placed tasks: {run}"
        );
        assert_eq!(
            run["failures"][0]["reason"], json!("RESOURCE:TASK"),
            "moto's missing-instances raise becomes the failure row: {run}"
        );
        h.handle(make_req(
            "RegisterContainerInstance",
            json!({"cluster": "cls", "instanceIdentityDocument":
                "{\"region\": \"us-east-1\", \"instanceId\": \"i-abc\"}" }),
        ));
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
        assert!(
            run["tasks"][0]["containerInstanceArn"].is_string(),
            "the placed task cites its instance: {run}"
        );
    }

    #[test]
    fn run_task_capacity_provider_cluster_default_v2() {
        // moto's Cluster model stores defaultCapacityProviderStrategy from
        // CreateCluster itself (fork ecs/models.py Cluster.__init__), so the
        // run_task fallback works without a PutClusterCapacityProviders call
        // and DescribeClusters echoes the member.
        let h = EcsHandler::new();
        setup_cluster_and_task_family(&h, "cds", "fam");
        let created = h.handle(make_req(
            "CreateCluster",
            json!({
                "clusterName": "cds",
                "capacityProviders": ["FARGATE", "FARGATE_SPOT"],
                "defaultCapacityProviderStrategy":
                    [{"capacityProvider": "FARGATE", "weight": 1, "base": 1}],
            }),
        ));
        let created: Value = serde_json::from_str(&created.body).unwrap();
        assert_eq!(
            created["cluster"]["defaultCapacityProviderStrategy"][0]["capacityProvider"],
            json!("FARGATE")
        );

        let run = h.handle(make_req(
            "RunTask",
            json!({
                "cluster": "cds",
                "taskDefinition": "fam:1",
            }),
        ));
        assert_eq!(run.status, 200);
        let run: Value = serde_json::from_str(&run.body).unwrap();
        assert_eq!(run["tasks"][0]["capacityProviderName"], json!("FARGATE"));

        let described = h.handle(make_req("DescribeClusters", json!({"clusters": ["cds"]})));
        let described: Value = serde_json::from_str(&described.body).unwrap();
        assert_eq!(
            described["clusters"][0]["defaultCapacityProviderStrategy"][0]["capacityProvider"],
            json!("FARGATE")
        );
    }

    // ------------------------------------------------------------------
    // moto parity: task-definition lifecycle guards (ecs/models.py)
    // ------------------------------------------------------------------

    /// moto's delete_task_definitions (models.py:2350-2409): empty/oversize
    /// lists are client errors, an ACTIVE definition is refused with the
    /// still-in-ACTIVE failure row, a deregistered one is REMOVED from the
    /// store (not just marked), and a bare identifier lands an
    /// invalid-identifier failure row.
    #[test]
    fn delete_task_definitions_moto_branches() {
        let h = EcsHandler::new();
        let empty = h.handle(make_req("DeleteTaskDefinitions", json!({"taskDefinitions": []})));
        assert_eq!(empty.status, 400);
        assert_eq!(
            serde_json::from_str::<Value>(&empty.body).unwrap()["message"],
            json!("size of TaskDefinition references list must be greater than 0")
        );
        let big = h.handle(make_req(
            "DeleteTaskDefinitions",
            json!({"taskDefinitions": (0..11).map(|i| format!("f:{i}")).collect::<Vec<_>>()}),
        ));
        assert_eq!(big.status, 400);
        assert_eq!(
            serde_json::from_str::<Value>(&big.body).unwrap()["message"],
            json!("Size of TaskDefinition references list cannot exceed 10")
        );

        h.handle(make_req(
            "RegisterTaskDefinition",
            json!({
                "family": "del-fam",
                "containerDefinitions": [{"name": "app", "image": "nginx", "memory": 128}],
            }),
        ));
        // ACTIVE → refusal row (and the row stays in the store).
        let active = serde_json::from_str::<Value>(
            &h.handle(make_req(
                "DeleteTaskDefinitions",
                json!({"taskDefinitions": ["del-fam:1"]}),
            ))
            .body,
        )
        .unwrap();
        assert_eq!(active["taskDefinitions"].as_array().unwrap().len(), 0);
        assert_eq!(
            active["failures"][0]["reason"],
            json!("The specified task definition is still in ACTIVE status. Please deregister the target and try again.")
        );
        h.handle(make_req(
            "DeregisterTaskDefinition",
            json!({"taskDefinition": "del-fam:1"}),
        ));
        // INACTIVE → actually removed now, plus a bogus bare identifier row.
        let deleted = serde_json::from_str::<Value>(
            &h.handle(make_req(
                "DeleteTaskDefinitions",
                json!({"taskDefinitions": ["del-fam:1", "del-fam"]}),
            ))
            .body,
        )
        .unwrap();
        assert_eq!(
            deleted["taskDefinitions"][0]["status"],
            json!("DELETE_IN_PROGRESS")
        );
        assert_eq!(
            deleted["failures"][0]["reason"],
            json!("The specified task definition identifier is invalid. Specify a valid name or ARN and try again.")
        );
        // The revision left the store: describing it now errors with moto's
        // TaskDefinitionNotFoundException text.
        let gone = h.handle(make_req(
            "DescribeTaskDefinition",
            json!({"taskDefinition": "del-fam:1"}),
        ));
        assert_eq!(gone.status, 400);
        let parsed: Value = serde_json::from_str(&gone.body).unwrap();
        assert_eq!(parsed["__type"], json!("ClientException"));
        assert_eq!(
            parsed["message"],
            json!("Unable to describe task definition.")
        );
    }

    /// moto's deregister (models.py:1218-1238): a bare family answers
    /// "Revision is missing." and INACTIVE rows remain discoverable by
    /// bare family (moto's max-revision resolve,describe).
    #[test]
    fn deregister_and_bare_family_resolution() {
        let h = EcsHandler::new();
        let bare = h.handle(make_req(
            "DeregisterTaskDefinition",
            json!({"taskDefinition": "fam"}),
        ));
        assert_eq!(bare.status, 400);
        let parsed: Value = serde_json::from_str(&bare.body).unwrap();
        assert_eq!(parsed["__type"], json!("ClientException"));
        assert_eq!(parsed["message"], json!("Revision is missing."));

        h.handle(make_req(
            "RegisterTaskDefinition",
            json!({
                "family": "res-fam",
                "containerDefinitions": [{"name": "app", "image": "nginx", "memory": 128}],
            }),
        ));
        h.handle(make_req(
            "DeregisterTaskDefinition",
            json!({"taskDefinition": "res-fam:1"}),
        ));
        // moto's family resolution is max-revision-across-STATUS: the
        // deregistered revision still resolves by bare family (the old
        // resolve only ever found ACTIVE ones and errored instead).
        let by_family = serde_json::from_str::<Value>(
            &h.handle(make_req(
                "DescribeTaskDefinition",
                json!({"taskDefinition": "res-fam"}),
            ))
            .body,
        )
        .unwrap();
        assert_eq!(by_family["taskDefinition"]["status"], json!("INACTIVE"));
    }

    /// moto's launch-type validation: CreateService defaults EC2 and
    /// errors outside [EC2, FARGATE]; RunTask validates
    /// [EC2, FARGATE, EXTERNAL] (ecs/models.py:1254-1257, 1622-1625).
    #[test]
    fn launch_type_defaults_and_validation() {
        let h = EcsHandler::new();
        h.handle(make_req("CreateCluster", json!({"clusterName": "ltn"})));
        h.handle(make_req(
            "RegisterTaskDefinition",
            json!({
                "family": "lt-fam",
                "containerDefinitions": [{"name": "app", "image": "nginx", "memory": 128}],
            }),
        ));
        let bogus = h.handle(make_req(
            "CreateService",
            json!({
                "cluster": "ltn", "serviceName": "s", "taskDefinition": "lt-fam",
                "launchType": "EXTERNAL"
            }),
        ));
        assert_eq!(bogus.status, 400);
        let parsed: Value = serde_json::from_str(&bogus.body).unwrap();
        assert_eq!(parsed["__type"], json!("ClientException"));
        assert_eq!(
            parsed["message"],
            json!("launch type should be one of [EC2,FARGATE]")
        );
        // Absent launchType defaults to EC2 (not FARGATE).
        let created = serde_json::from_str::<Value>(
            &h.handle(make_req(
                "CreateService",
                json!({"cluster": "ltn", "serviceName": "s", "taskDefinition": "lt-fam"}),
            ))
            .body,
        )
        .unwrap();
        assert_eq!(created["service"]["launchType"], json!("EC2"));

        let bogus_run = h.handle(make_req(
            "RunTask",
            json!({
                "cluster": "ltn", "taskDefinition": "lt-fam", "launchType": "SLURP"
            }),
        ));
        assert_eq!(bogus_run.status, 400);
        let parsed: Value = serde_json::from_str(&bogus_run.body).unwrap();
        assert_eq!(
            parsed["message"],
            json!("launch type should be one of [EC2,FARGATE,EXTERNAL]")
        );
    }

    /// moto's register pass-through (models.py:131-217): fields the caller
    /// left out carry no invented values, compatibilities/networkMode are
    /// computed, container definitions get the default members, and the
    /// FARGATE pidMode + name/image/memory checks fire.
    #[test]
    fn register_task_definition_stores_what_was_passed() {
        let h = EcsHandler::new();
        let reg = serde_json::from_str::<Value>(
            &h.handle(make_req(
                "RegisterTaskDefinition",
                json!({
                    "family": "pass-fam",
                    "containerDefinitions": [{"name": "app", "image": "nginx", "memory": 128}],
                }),
            ))
            .body,
        )
        .unwrap();
        let td = &reg["taskDefinition"];
        assert!(td.get("cpu").is_none(), "no invented cpu: {td}");
        assert!(td.get("memory").is_none(), "no invented memory: {td}");
        assert!(td.get("requiresCompatibilities").is_none());
        assert_eq!(td["compatibilities"], json!(["EC2"]));
        assert_eq!(td["networkMode"], json!("bridge"));
        let cd = &td["containerDefinitions"][0];
        // moto's default_container_definition (models.py:159-166).
        assert_eq!(cd["cpu"], json!(0));
        assert_eq!(cd["essential"], json!(true));
        assert_eq!(cd["portMappings"], json!([]));

        // FARGATE requires + pidMode → moto's exact text.
        let pid = h.handle(make_req(
            "RegisterTaskDefinition",
            json!({
                "family": "pid-fam",
                "containerDefinitions": [{"name": "app", "image": "nginx", "memory": 128}],
                "requiresCompatibilities": ["FARGATE"],
                "pidMode": "host"
            }),
        ));
        let parsed: Value = serde_json::from_str(&pid.body).unwrap();
        assert_eq!(
            parsed["message"],
            json!("Tasks using the Fargate launch type do not support pidMode 'host'. The supported value for pidMode is 'task'.")
        );
        // FARGATE-compat + explicit name/image but NO memory → memory error.
        let mem = h.handle(make_req(
            "RegisterTaskDefinition",
            json!({
                "family": "mem-fam",
                "containerDefinitions": [{"name": "app", "image": "nginx"}],
                "requiresCompatibilities": ["FARGATE"]
            }),
        ));
        let parsed: Value = serde_json::from_str(&mem.body).unwrap();
        assert_eq!(
            parsed["message"],
            json!("Invalid setting for container 'app'. At least one of 'memory' or 'memoryReservation' must be specified.")
        );
        // Missing image → moto's TaskDefinitionMissingPropertyError text.
        let img = h.handle(make_req(
            "RegisterTaskDefinition",
            json!({
                "family": "img-fam",
                "containerDefinitions": [{"name": "app", "memory": 64}]
            }),
        ));
        let parsed: Value = serde_json::from_str(&img.body).unwrap();
        assert_eq!(
            parsed["message"],
            json!("Container.image should not be null or empty.")
        );
    }

    /// run_task stores startedBy/group (moto Stores both, models.py:1302)
    /// and list_tasks honors the filters with NO RUNNING default
    /// (models.py:1509-1561) — stopped tasks are listed unless desiredStatus
    /// is named.
    #[test]
    fn run_task_stores_references_and_list_filters() {
        let h = EcsHandler::new();
        h.handle(make_req("CreateCluster", json!({"clusterName": "fl"})));
        h.handle(make_req(
            "RegisterTaskDefinition",
            json!({
                "family": "fl-fam",
                "containerDefinitions": [{"name": "app", "image": "nginx", "memory": 128}],
            }),
        ));
        let run = serde_json::from_str::<Value>(
            &h.handle(make_req(
                "RunTask",
                json!({
                    "cluster": "fl", "taskDefinition": "fl-fam",
                    "launchType": "FARGATE", "startedBy": "jenkins"
                }),
            ))
            .body,
        )
        .unwrap();
        assert_eq!(run["tasks"][0]["startedBy"], json!("jenkins"));
        assert_eq!(run["tasks"][0]["group"], json!("family:fl-fam"));
        assert_eq!(run["tasks"][0]["launchType"], json!("FARGATE"));
        // moto invents no cpu/memory on the task from the old defaults.
        assert!(run["tasks"][0].get("cpu").is_none());

        let arn = run["tasks"][0]["taskArn"].as_str().unwrap().to_string();
        h.handle(make_req(
            "StopTask",
            json!({"cluster": "fl", "task": arn, "reason": "done"}),
        ));
        // moto: no desiredStatus filter → stopped tasks are listed.
        let all = serde_json::from_str::<Value>(
            &h.handle(make_req("ListTasks", json!({"cluster": "fl"}))).body,
        )
        .unwrap();
        assert_eq!(all["taskArns"].as_array().unwrap().len(), 1);
        let stopped = serde_json::from_str::<Value>(
            &h.handle(make_req(
                "ListTasks",
                json!({"cluster": "fl", "desiredStatus": "STOPPED"}),
            ))
            .body,
        )
        .unwrap();
        assert_eq!(stopped["taskArns"].as_array().unwrap().len(), 1);
        let started_by = serde_json::from_str::<Value>(
            &h.handle(make_req(
                "ListTasks",
                json!({"cluster": "fl", "startedBy": "jenkins"}),
            ))
            .body,
        )
        .unwrap();
        assert_eq!(started_by["taskArns"].as_array().unwrap().len(), 1);
        let none = serde_json::from_str::<Value>(
            &h.handle(make_req(
                "ListTasks",
                json!({"cluster": "fl", "startedBy": "nobody"}),
            ))
            .body,
        )
        .unwrap();
        assert_eq!(none["taskArns"].as_array().unwrap().len(), 0);
    }

    /// UpdateTaskProtection stores the flag; GetTaskProtection reads it
    /// back (the pair used to be a tautology). moto 5.2.4 has no models
    /// here — real-AWS semantics keyed by the stored task ARN.
    #[test]
    fn task_protection_round_trip() {
        let h = EcsHandler::new();
        h.handle(make_req("CreateCluster", json!({"clusterName": "tp"})));
        h.handle(make_req(
            "RegisterTaskDefinition",
            json!({
                "family": "tp-fam",
                "containerDefinitions": [{"name": "app", "image": "nginx", "memory": 128}],
            }),
        ));
        let run = serde_json::from_str::<Value>(
            &h.handle(make_req(
                "RunTask",
                json!({"cluster": "tp", "taskDefinition": "tp-fam", "launchType": "FARGATE"}),
            ))
            .body,
        )
        .unwrap();
        let arn = run["tasks"][0]["taskArn"].as_str().unwrap().to_string();
        let up = serde_json::from_str::<Value>(
            &h.handle(make_req(
                "UpdateTaskProtection",
                json!({"cluster": "tp", "tasks": [arn.clone()], "protectionEnabled": true}),
            ))
            .body,
        )
        .unwrap();
        assert_eq!(up["protectedTasks"][0]["taskArn"], json!(arn.clone()));
        assert_eq!(up["protectedTasks"][0]["protectionEnabled"], json!(true));
        let got = serde_json::from_str::<Value>(
            &h.handle(make_req(
                "GetTaskProtection",
                json!({"cluster": "tp", "tasks": [arn.clone()]}),
            ))
            .body,
        )
        .unwrap();
        assert_eq!(got["protectedTasks"][0]["protectionEnabled"], json!(true));
        // Off again.
        h.handle(make_req(
            "UpdateTaskProtection",
            json!({"cluster": "tp", "tasks": [arn.clone()], "protectionEnabled": false}),
        ));
        let got = serde_json::from_str::<Value>(
            &h.handle(make_req(
                "GetTaskProtection",
                json!({"cluster": "tp", "tasks": [arn]}),
            ))
            .body,
        )
        .unwrap();
        assert_eq!(got["protectedTasks"][0]["protectionEnabled"], json!(false));
        // Unknown task → failure row, not an echo.
        let miss = serde_json::from_str::<Value>(
            &h.handle(make_req(
                "UpdateTaskProtection",
                json!({"cluster": "tp", "tasks": ["arn:aws:ecs:us-east-1:123456789012:task/tp/nope"], "protectionEnabled": true}),
            ))
            .body,
        )
        .unwrap();
        assert_eq!(miss["protectedTasks"].as_array().unwrap().len(), 0);
        assert_eq!(miss["failures"][0]["reason"], json!("TASK_NOT_FOUND"));
    }

    /// DiscoverPollEndpoint answers the env-derived gateway port (no
    /// portability constants).
    #[test]
    fn discover_poll_endpoint_follows_gateway_port() {
        std::env::set_var("GATEWAY_PORT", "4583");
        let h = EcsHandler::new();
        let resp = serde_json::from_str::<Value>(
            &h.handle(make_req("DiscoverPollEndpoint", json!({}))).body,
        )
        .unwrap();
        assert_eq!(resp["endpoint"], json!("http://localhost:4583"));
        assert_eq!(resp["telemetryEndpoint"], json!("http://localhost:4583"));
        std::env::remove_var("GATEWAY_PORT");
    }

    /// The attribute target must be a container instance in the cluster;
    /// list_attributes demands targetType=container-instance (moto
    /// models.py:1939-1986).
    #[test]
    fn attributes_target_validation() {
        let h = EcsHandler::new();
        h.handle(make_req("CreateCluster", json!({"clusterName": "attr"})));
        // bogus targetId with targetType → TargetNotFoundException.
        let bogus = h.handle(make_req(
            "PutAttributes",
            json!({
                "cluster": "attr",
                "attributes": [{"name": "env", "value": "prod",
                    "targetType": "container-instance", "targetId": "fake-ci"}]
            }),
        ));
        assert_eq!(bogus.status, 400);
        let parsed: Value = serde_json::from_str(&bogus.body).unwrap();
        assert_eq!(parsed["__type"], json!("TargetNotFoundException"));
        assert_eq!(parsed["message"], json!("Could not find fake-ci"));

        // list_attributes requires the targetType.
        let no_type = h.handle(make_req(
            "ListAttributes",
            json!({"cluster": "attr"}),
        ));
        assert_eq!(no_type.status, 400);
        assert_eq!(
            serde_json::from_str::<Value>(&no_type.body).unwrap()["message"],
            json!("targetType must be container-instance")
        );

        // A real instance accepts the attribute and lists it back.
        let reg = serde_json::from_str::<Value>(
            &h.handle(make_req(
                "RegisterContainerInstance",
                json!({"cluster": "attr", "instanceIdentityDocument":
                    "{\"region\": \"us-east-1\", \"instanceId\": \"i-a1\"}" }),
            ))
            .body,
        )
        .unwrap();
        let ci_arn = reg["containerInstance"]["containerInstanceArn"]
            .as_str()
            .unwrap()
            .to_string();
        let ok = h.handle(make_req(
            "PutAttributes",
            json!({
                "cluster": "attr",
                "attributes": [{"name": "env", "value": "prod", "targetId": ci_arn.clone()}]
            }),
        ));
        assert_eq!(ok.status, 200);
        let listed = serde_json::from_str::<Value>(
            &h.handle(make_req(
                "ListAttributes",
                json!({"cluster": "attr", "targetType": "container-instance"}),
            ))
            .body,
        )
        .unwrap();
        assert_eq!(
            listed["attributes"][0]["targetId"], json!(ci_arn),
            "moto lists the ARN-typed target: {listed}"
        );
    }

    /// moto's deregister guard (models.py:1901-1904): running tasks on the
    /// instance refuse the deregister without force.
    #[test]
    fn deregister_container_instance_running_guard() {
        let h = EcsHandler::new();
        h.handle(make_req("CreateCluster", json!({"clusterName": "dci"})));
        h.handle(make_req(
            "RegisterTaskDefinition",
            json!({
                "family": "dci-fam",
                "containerDefinitions": [{"name": "app", "image": "nginx", "memory": 128}],
            }),
        ));
        let reg = serde_json::from_str::<Value>(
            &h.handle(make_req(
                "RegisterContainerInstance",
                json!({"cluster": "dci", "instanceIdentityDocument":
                    "{\"region\": \"us-east-1\", \"instanceId\": \"i-dci\"}" }),
            ))
            .body,
        )
        .unwrap();
        let ci_arn = reg["containerInstance"]["containerInstanceArn"]
            .as_str()
            .unwrap()
            .to_string();
        h.handle(make_req(
            "StartTask",
            json!({"cluster": "dci", "taskDefinition": "dci-fam", "containerInstances": [ci_arn.clone()]}),
        ));
        let refused = h.handle(make_req(
            "DeregisterContainerInstance",
            json!({"cluster": "dci", "containerInstance": ci_arn.clone()}),
        ));
        assert_eq!(refused.status, 400);
        assert_eq!(
            serde_json::from_str::<Value>(&refused.body).unwrap()["message"],
            json!("Found running tasks on the instance.")
        );
        // force=true goes through (moto models.py:1907-1912).
        let forced = h.handle(make_req(
            "DeregisterContainerInstance",
            json!({"cluster": "dci", "containerInstance": ci_arn, "force": true}),
        ));
        assert_eq!(forced.status, 200);
    }

    /// The failures list carries moto's synthesized typed ARNs
    /// (ClusterFailure / the service-ARN rule / ContainerInstanceFailure).
    #[test]
    fn failure_rows_use_typed_arns() {
        let h = EcsHandler::new();
        let dc = h.handle(make_req(
            "DescribeClusters",
            json!({"clusters": ["ghost"]}),
        ));
        let parsed: Value = serde_json::from_str(&dc.body).unwrap();
        assert_eq!(parsed["failures"][0]["reason"], json!("MISSING"));
        assert_eq!(
            parsed["failures"][0]["arn"],
            json!("arn:aws:ecs:us-east-1:123456789012:cluster/ghost"),
            "moto's ClusterFailure synthesizes the typed cluster ARN: {parsed}"
        );
        let ds = h.handle(make_req(
            "DescribeServices",
            json!({"cluster": "c", "services": ["missing-svc"]}),
        ));
        let parsed: Value = serde_json::from_str(&ds.body).unwrap();
        assert_eq!(
            parsed["failures"][0]["arn"],
            json!("arn:aws:ecs:us-east-1:123456789012:service/missing-svc")
        );
        h.handle(make_req("CreateCluster", json!({"clusterName": "c"})));
        let dci = h.handle(make_req(
            "DescribeContainerInstances",
            json!({"cluster": "c", "containerInstances": ["arn:aws:ecs:us-east-1:123456789012:container-instance/c/ghost"]}),
        ));
        let parsed: Value = serde_json::from_str(&dci.body).unwrap();
        assert_eq!(
            parsed["failures"][0]["arn"],
            json!("arn:aws:ecs:us-east-1:123456789012:container-instance/ghost")
        );
        // The service-name ARN rule (models.py:1694-1698): an input that IS
        // an ARN rides through verbatim.
        let ds2 = h.handle(make_req(
            "DescribeServices",
            json!({"cluster": "c", "services": ["arn:aws:ecs:eu-west-1:111111111111:service/x/y"]}),
        ));
        let parsed: Value = serde_json::from_str(&ds2.body).unwrap();
        assert_eq!(
            parsed["failures"][0]["arn"],
            json!("arn:aws:ecs:eu-west-1:111111111111:service/x/y")
        );
    }
}
