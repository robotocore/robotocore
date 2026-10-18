//! SSM operation handler (mirrors moto's ssm response semantics).

use parking_lot::RwLock;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::models::{
    fmt_dt, hex_id, uuid_str, Activation, Association, AutomationExecution, Command, Document,
    Documents, MaintenanceWindow, MwTarget, MwTask, OpsItem, OpsMetadata, ParamVersion, Parameter,
    PatchBaseline, ResourceDataSync, SsmState, PARAMETER_BY_PATH_MAX_RESULTS,
};
use crate::protocol::{AwsRequest, AwsResponse};

pub struct SsmHandler {
    state: RwLock<BTreeMap<(u64, String), SsmState>>,
}

impl Default for SsmHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl SsmHandler {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(BTreeMap::new()),
        }
    }

    fn get_state(&self, account: u64, region: &str) -> SsmState {
        let mut states = self.state.write();
        states
            .entry((account, region.to_string()))
            .or_default()
            .clone()
    }

    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        let op = req.operation.as_str();
        match op {
            // ---- Parameters ----
            "PutParameter" => self.put_parameter(&req),
            "GetParameter" => self.get_parameter(&req),
            "GetParameters" => self.get_parameters(&req),
            "GetParametersByPath" => self.get_parameters_by_path(&req),
            "DescribeParameters" => self.describe_parameters(&req),
            "GetParameterHistory" => self.get_parameter_history(&req),
            "LabelParameterVersion" => self.label_parameter_version(&req),
            "UnlabelParameterVersion" => self.unlabel_parameter_version(&req),
            "DeleteParameter" => self.delete_parameter(&req),
            "DeleteParameters" => self.delete_parameters(&req),
            "ListTagsForResource" => self.list_tags_for_resource(&req),
            "AddTagsToResource" => self.add_tags_to_resource(&req),
            "RemoveTagsFromResource" => self.remove_tags_from_resource(&req),
            // ---- Documents ----
            "CreateDocument" => self.create_document(&req),
            "DescribeDocument" => self.describe_document(&req),
            "GetDocument" => self.get_document(&req),
            "DeleteDocument" => self.delete_document(&req),
            "UpdateDocument" => self.update_document(&req),
            "UpdateDocumentDefaultVersion" => self.update_document_default_version(&req),
            "ListDocuments" => self.list_documents(&req),
            "DescribeDocumentPermission" => self.describe_document_permission(&req),
            "ModifyDocumentPermission" => self.modify_document_permission(&req),
            "ListDocumentVersions" => self.list_document_versions(&req),
            "ListDocumentMetadataHistory" => self.list_document_metadata_history(&req),
            "UpdateDocumentMetadata" => self.update_document_metadata(&req),
            // ---- Commands ----
            "SendCommand" => self.send_command(&req),
            "ListCommands" => self.list_commands(&req),
            "GetCommandInvocation" => self.get_command_invocation(&req),
            "ListCommandInvocations" => self.list_command_invocations(&req),
            // ---- Maintenance windows ----
            "CreateMaintenanceWindow" => self.create_maintenance_window(&req),
            "GetMaintenanceWindow" => self.get_maintenance_window(&req),
            "DescribeMaintenanceWindows" => self.describe_maintenance_windows(&req),
            "DeleteMaintenanceWindow" => self.delete_maintenance_window(&req),
            "RegisterTargetWithMaintenanceWindow" => self.register_mw_target(&req),
            "DescribeMaintenanceWindowTargets" => self.describe_mw_targets(&req),
            "DeregisterTargetFromMaintenanceWindow" => self.deregister_mw_target(&req),
            "RegisterTaskWithMaintenanceWindow" => self.register_mw_task(&req),
            "DescribeMaintenanceWindowTasks" => self.describe_mw_tasks(&req),
            "DeregisterTaskFromMaintenanceWindow" => self.deregister_mw_task(&req),
            "UpdateMaintenanceWindowTarget" => self.update_mw_target(&req),
            "UpdateMaintenanceWindowTask" => self.update_mw_task(&req),
            "DescribeMaintenanceWindowsForTarget" => self.describe_mw_for_target(&req),
            // ---- Patch baselines ----
            "CreatePatchBaseline" => self.create_patch_baseline(&req),
            "GetPatchBaseline" => self.get_patch_baseline(&req),
            "DescribePatchBaselines" => self.describe_patch_baselines(&req),
            "DeletePatchBaseline" => self.delete_patch_baseline(&req),
            "UpdatePatchBaseline" => AwsResponse::error(
                400,
                "NotImplemented",
                "The update_patch_baseline action has not been implemented",
            ),
            "RegisterPatchBaselineForPatchGroup" => self.register_pb_for_patch_group(&req),
            "DeregisterPatchBaselineForPatchGroup" => self.deregister_pb_for_patch_group(&req),
            "GetPatchBaselineForPatchGroup" => self.get_pb_for_patch_group(&req),
            "GetDefaultPatchBaseline" => self.get_default_patch_baseline(&req),
            "RegisterDefaultPatchBaseline" => {
                let id = req
                    .params
                    .get("BaselineId")
                    .and_then(|v| v.as_str())
                    .unwrap_or("pb-00000000000000000");
                AwsResponse::json(200, json!({ "BaselineId": id }))
            }
            "DescribePatchGroupState" => self.describe_patch_group_state(&req),
            // ---- Associations ----
            "CreateAssociation" => self.create_association(&req),
            "CreateAssociationBatch" => AwsResponse::json(
                200,
                json!({
                    "Successful": [], "Failed": []
                }),
            ),
            "DescribeAssociation" => self.describe_association(&req),
            "ListAssociations" => self.list_associations(&req),
            "UpdateAssociation" => self.update_association(&req),
            "DeleteAssociation" => self.delete_association(&req),
            "ListAssociationVersions" => self.list_association_versions(&req),
            "UpdateAssociationStatus" => self.update_association_status(&req),
            "DescribeAssociationExecutions" => self.describe_association_executions(&req),
            "DescribeAssociationExecutionTargets" => {
                self.describe_association_execution_targets(&req)
            }
            "DescribeEffectiveInstanceAssociations" => {
                self.describe_effective_instance_associations(&req)
            }
            "DescribeInstanceAssociationsStatus" => {
                self.describe_instance_associations_status(&req)
            }
            "StartAssociationsOnce" => {
                AwsResponse::error(400, "DoesNotExistException", "Association does not exist")
            }
            // ---- Ops items ----
            "CreateOpsItem" => self.create_ops_item(&req),
            "GetOpsItem" => self.get_ops_item(&req),
            "UpdateOpsItem" => self.update_ops_item(&req),
            "DeleteOpsItem" => self.delete_ops_item(&req),
            "DescribeOpsItems" => self.describe_ops_items(&req),
            "GetOpsSummary" => self.get_ops_summary(&req),
            "ListOpsItemEvents" => AwsResponse::json(200, json!({ "Summaries": [] })),
            "ListOpsItemRelatedItems" => self.list_ops_item_related_items(&req),
            "AssociateOpsItemRelatedItem" => self.associate_ops_item_related_item(&req),
            "DisassociateOpsItemRelatedItem" => self.disassociate_ops_item_related_item(&req),
            // ---- Ops metadata ----
            "CreateOpsMetadata" => self.create_ops_metadata(&req),
            "GetOpsMetadata" => self.get_ops_metadata(&req),
            "UpdateOpsMetadata" => self.update_ops_metadata(&req),
            "DeleteOpsMetadata" => self.delete_ops_metadata(&req),
            "ListOpsMetadata" => self.list_ops_metadata(&req),
            // ---- Activations ----
            "CreateActivation" => self.create_activation(&req),
            "DescribeActivations" => self.describe_activations(&req),
            "DeleteActivation" => self.delete_activation(&req),
            // ---- Automation ----
            "StartAutomationExecution" => self.start_automation_execution(&req),
            "DescribeAutomationExecutions" => self.describe_automation_executions(&req),
            "GetAutomationExecution" => self.get_automation_execution(&req),
            "StopAutomationExecution" => self.stop_automation_execution(&req),
            "StartChangeRequestExecution" => self.start_change_request_execution(&req),
            // ---- Sessions ----
            "StartSession" => self.start_session(&req),
            "ResumeSession" => self.resume_session(&req),
            "TerminateSession" => self.terminate_session(&req),
            // ---- Service settings ----
            "GetServiceSetting" => self.get_service_setting(&req),
            "UpdateServiceSetting" => AwsResponse::json(200, json!({})),
            "ResetServiceSetting" => self.get_service_setting(&req),
            // ---- Resource data sync ----
            "CreateResourceDataSync" => self.create_resource_data_sync(&req),
            "ListResourceDataSync" => self.list_resource_data_sync(&req),
            "DeleteResourceDataSync" => self.delete_resource_data_sync(&req),
            "UpdateResourceDataSync" => self.update_resource_data_sync(&req),
            // ---- Resource policies ----
            "GetResourcePolicies" => self.get_resource_policies(&req),
            "PutResourcePolicy" => self.put_resource_policy(&req),
            "DeleteResourcePolicy" => self.delete_resource_policy(&req),
            // ---- Inventory / compliance ----
            "GetInventory" => AwsResponse::json(200, json!({ "Entities": [] })),
            "GetInventorySchema" => AwsResponse::json(200, json!({ "Schemas": [] })),
            "DeleteInventory" => AwsResponse::json(
                200,
                json!({
                    "DeletionId": uuid_str(),
                    "TypeName": req.params.get("TypeName").cloned().unwrap_or(Value::Null),
                    "DeletionSummary": { "TotalCount": 0, "RemainingCount": 0, "SummaryItems": [] }
                }),
            ),
            "ListInventoryEntries" => AwsResponse::json(
                200,
                json!({
                    "TypeName": req.params.get("TypeName").cloned().unwrap_or(Value::Null),
                    "InstanceId": req.params.get("InstanceId").cloned().unwrap_or(Value::Null),
                    "SchemaVersion": "1.0",
                    "CaptureTime": fmt_dt(&chrono::Utc::now()),
                    "Entries": []
                }),
            ),
            "DescribeInstancePatches" => AwsResponse::json(200, json!({ "Patches": [] })),
            "PutInventory" => AwsResponse::json(
                200,
                json!({ "Message": "Inventory data has been successfully submitted" }),
            ),
            "ListComplianceItems" => AwsResponse::json(200, json!({ "ComplianceItems": [] })),
            "ListComplianceSummaries" => {
                AwsResponse::json(200, json!({ "ComplianceSummaryItems": [] }))
            }
            "ListResourceComplianceSummaries" => {
                AwsResponse::json(200, json!({ "ResourceComplianceSummaryItems": [] }))
            }
            "PutComplianceItems" => AwsResponse::json(200, json!({})),
            // ---- Instance / nodes / misc describe ----
            "DescribeInstanceInformation" => {
                AwsResponse::json(200, json!({ "InstanceInformationList": [] }))
            }
            "DescribeInstanceProperties" => {
                AwsResponse::json(200, json!({ "InstanceProperties": [] }))
            }
            "ListNodes" => AwsResponse::json(200, json!({ "Nodes": [] })),
            "ListNodesSummary" => AwsResponse::json(200, json!({ "Summary": [] })),
            "DescribePatchGroups" => AwsResponse::json(200, json!({ "Mappings": [] })),
            "DescribePatchProperties" => AwsResponse::json(200, json!({ "Properties": [] })),
            "DescribeAvailablePatches" => AwsResponse::json(200, json!({ "Patches": [] })),
            "DescribeEffectivePatchesForPatchBaseline" => {
                AwsResponse::json(200, json!({ "EffectivePatches": [] }))
            }
            "DescribeInventoryDeletions" => {
                AwsResponse::json(200, json!({ "InventoryDeletions": [] }))
            }
            "DescribeMaintenanceWindowSchedule" => {
                AwsResponse::json(200, json!({ "ScheduledWindowExecutions": [] }))
            }
            "DescribeMaintenanceWindowExecutions" => {
                AwsResponse::json(200, json!({ "WindowExecutions": [] }))
            }
            "DescribeInstancePatchStates" => self.describe_instance_patch_states(&req),
            "DescribeInstancePatchStatesForPatchGroup" => {
                AwsResponse::json(200, json!({ "InstancePatchStates": [] }))
            }
            "DescribeSessions" => AwsResponse::json(200, json!({ "Sessions": [] })),
            "GetConnectionStatus" => AwsResponse::json(
                200,
                json!({
                    "Target": req.params.get("Target").cloned().unwrap_or(Value::Null),
                    "Status": "notconnected"
                }),
            ),
            "GetCalendarState" => AwsResponse::json(
                200,
                json!({
                    "State": "OPEN",
                    "AtTime": chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6f").to_string(),
                    "NextTransitionTime": ""
                }),
            ),
            // ---- Access requests / previews ----
            "GetAccessToken" => AwsResponse::json(
                200,
                json!({
                    "AccessRequestStatus": "Approved",
                    "Credentials": {
                        "AccessKeyId": "AKIAIOSFODNN7EXAMPLE",
                        "SecretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                        "SessionToken": uuid_str(),
                        "ExpirationTime": fmt_dt(&chrono::Utc::now())
                    }
                }),
            ),
            "StartAccessRequest" => {
                AwsResponse::json(200, json!({ "AccessRequestId": uuid_str() }))
            }
            "StartExecutionPreview" => {
                AwsResponse::json(200, json!({ "ExecutionPreviewId": uuid_str() }))
            }
            // ---- Deregister managed instance (exists but rejects unknown ids) ----
            "DeregisterManagedInstance" => {
                AwsResponse::error(400, "InvalidInstanceId", "Invalid managed instance id")
            }
            "UpdateManagedInstanceRole" => AwsResponse::json(200, json!({})),
            "CancelCommand" => AwsResponse::error(400, "InvalidCommandId", "Invalid command id."),
            other => AwsResponse::error(
                400,
                "InvalidParameterException",
                &format!("The operation {other} is not implemented"),
            ),
        }
    }

    // -----------------------------------------------------------------------
    // Parameters
    // -----------------------------------------------------------------------

    fn put_parameter(&self, req: &AwsRequest) -> AwsResponse {
        let name = match req.params.get("Name").and_then(|v| v.as_str()) {
            Some(n) if !n.is_empty() => n.to_string(),
            _ => return err_validation("Parameter name is required"),
        };
        let state = self.get_state(req.account, &req.region);
        let name = SsmState::normalize_name(&name);

        let value = req
            .params
            .get("Value")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let parameter_type = req
            .params
            .get("Type")
            .and_then(|v| v.as_str())
            .map(String::from);
        let description = req
            .params
            .get("Description")
            .and_then(|v| v.as_str())
            .map(String::from);
        let allowed_pattern = req
            .params
            .get("AllowedPattern")
            .and_then(|v| v.as_str())
            .map(String::from);
        let keyid = req
            .params
            .get("KeyId")
            .and_then(|v| v.as_str())
            .map(String::from);
        let data_type = req
            .params
            .get("DataType")
            .and_then(|v| v.as_str())
            .map(String::from);
        let tier = req
            .params
            .get("Tier")
            .and_then(|v| v.as_str())
            .map(String::from);
        let policies = req.params.get("Policies").cloned();
        let overwrite = req
            .params
            .get("Overwrite")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let tags = req.params.get("Tags").cloned();

        if value.is_empty() {
            return err_validation(
                "1 validation error detected: Value '' at 'value' failed to satisfy constraint: Member must have length greater than or equal to 1.");
        }
        let has_tags = tags
            .as_ref()
            .map(|t| t.as_array().map(|a| !a.is_empty()).unwrap_or(false))
            .unwrap_or(false);
        if overwrite && has_tags {
            return err_validation(
                "Invalid request: tags and overwrite can't be used together. To create a parameter with tags, please remove overwrite flag. To update tags for an existing parameter, please use AddTagsToResource or RemoveTagsFromResource.");
        }

        if let Some(existing) = state.get_parameter(&name) {
            if !overwrite {
                return AwsResponse::error(
                    400,
                    "ParameterAlreadyExists",
                    &format!("Parameter {name} already exists"),
                );
            }
            let previous = existing.current().expect("parameter without versions");
            let new_version = previous.version + 1;
            // Type/fields are preserved from the previous version when absent.
            let parameter_type = parameter_type
                .clone()
                .unwrap_or_else(|| previous.parameter_type.clone());
            let description = description.clone().or(previous.description.clone());
            let allowed_pattern = allowed_pattern.clone().or(previous.allowed_pattern.clone());
            let keyid = keyid.clone().or(previous.keyid.clone());
            let data_type = data_type
                .clone()
                .or_else(|| Some(previous.data_type.clone()));
            let tier = tier.clone().or_else(|| Some(previous.tier.clone()));
            let policies = policies.clone().or(previous.policies.clone());
            let stored_value = if parameter_type == "SecureString" {
                let key = keyid.clone().unwrap_or_else(|| "alias/aws/ssm".into());
                format!("kms:{key}:{value}")
            } else {
                value.clone()
            };
            let version = ParamVersion {
                version: new_version,
                value: stored_value,
                parameter_type,
                data_type: data_type.unwrap_or_else(|| "text".into()),
                description,
                allowed_pattern,
                keyid,
                tier: tier.clone().unwrap_or_else(|| "Standard".into()),
                policies,
                labels: Vec::new(),
                last_modified: chrono::Utc::now(),
            };
            existing.push_version(version);
            let tier_out = tier.unwrap_or_else(|| "Standard".to_string());
            return AwsResponse::json(200, json!({ "Version": new_version, "Tier": tier_out }));
        }

        // Creating a new parameter.
        let Some(param_type) = parameter_type.clone() else {
            return err_validation("A parameter type is required when you create a parameter.");
        };
        if !matches!(
            param_type.as_str(),
            "String" | "StringList" | "SecureString"
        ) {
            return err_validation(&format!(
                "1 validation error detected: Value '{param_type}' at 'type' failed to satisfy constraint: Member must satisfy enum value set: [SecureString, StringList, String]"));
        }
        let data_type = data_type.clone().unwrap_or_else(|| "text".into());
        if !matches!(data_type.as_str(), "text" | "aws:ec2:image") {
            return err_validation(&format!(
                "The following data type is not supported: {data_type} (Data type names are all lowercase.)"));
        }
        // SecureString values are stored encrypted.
        let keyid = if param_type == "SecureString" {
            Some(keyid.clone().unwrap_or_else(|| "alias/aws/ssm".into()))
        } else {
            None
        };
        let stored = if param_type == "SecureString" {
            format!("kms:{}:{}", keyid.clone().unwrap_or_default(), value)
        } else {
            value.clone()
        };
        let param = Arc::new(Parameter::new(&name));
        param.push_version(ParamVersion {
            version: 1,
            value: stored,
            parameter_type: param_type,
            data_type,
            description,
            allowed_pattern,
            keyid: keyid.clone(),
            tier: tier.clone().unwrap_or_else(|| "Standard".into()),
            policies,
            labels: Vec::new(),
            last_modified: chrono::Utc::now(),
        });
        state.parameters.write().insert(name.clone(), param);

        if let Some(list) = tags.as_ref().and_then(|t| t.as_array()).cloned() {
            for tag in list {
                if let (Some(k), Some(v)) = (
                    tag.get("Key").and_then(|v| v.as_str()),
                    tag.get("Value").and_then(|v| v.as_str()),
                ) {
                    state.add_tag("Parameter", &name, k, v);
                }
            }
        }
        let tier_out = tier.unwrap_or_else(|| "Standard".to_string());
        AwsResponse::json(200, json!({ "Version": 1, "Tier": tier_out }))
    }

    fn get_parameter(&self, req: &AwsRequest) -> AwsResponse {
        let raw = req
            .params
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let name = strip_arn_prefix(raw, &req.region, req.account);
        let with_decryption = bool_of(req.params.get("WithDecryption"));
        let state = self.get_state(req.account, &req.region);
        match state.resolve_parameter(&name) {
            Ok(Some((param, version))) => {
                let resp = version.response_object(&param.name, with_decryption, Some(&req.region), req.account);
                AwsResponse::json(200, json!({ "Parameter": resp }))
            }
            Ok(None) => AwsResponse::error(400, "ParameterNotFound",
                &format!("Parameter {name} not found.")),
            Err(_) => AwsResponse::error(400, "ParameterVersionNotFound",
                &format!("Systems Manager could not find version {} of {}. Verify the version and try again.",
                    name.rsplit(':').next().unwrap_or(""),
                    name.rsplit_once(':').map(|x| x.0).unwrap_or(&name))),
        }
    }

    fn get_parameters(&self, req: &AwsRequest) -> AwsResponse {
        let names: Vec<String> = req
            .params
            .get("Names")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let with_decryption = bool_of(req.params.get("WithDecryption"));
        let state = self.get_state(req.account, &req.region);

        let mut found = Vec::new();
        let mut invalid = Vec::new();
        for name in &names {
            match state.resolve_parameter(&strip_arn_prefix(name, &req.region, req.account)) {
                Ok(Some((p, v))) => {
                    found.push(v.response_object(
                        &p.name,
                        with_decryption,
                        Some(&req.region),
                        req.account,
                    ));
                }
                Ok(None) | Err(_) => invalid.push(name.clone()),
            }
        }
        AwsResponse::json(
            200,
            json!({ "Parameters": found, "InvalidParameters": invalid }),
        )
    }

    fn delete_parameter(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let state = self.get_state(req.account, &req.region);
        let normalized = SsmState::normalize_name(name);
        if state.parameters.write().remove(&normalized).is_none() {
            return AwsResponse::error(
                400,
                "ParameterNotFound",
                &format!("Parameter {name} not found."),
            );
        }
        state
            .resource_tags
            .write()
            .get_mut("Parameter")
            .map(|m| m.remove(&normalized));
        AwsResponse::json(200, json!({}))
    }

    fn delete_parameters(&self, req: &AwsRequest) -> AwsResponse {
        let names: Vec<String> = req
            .params
            .get("Names")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let mut deleted = Vec::new();
        let mut invalid = Vec::new();
        {
            let mut params = state.parameters.write();
            for name in &names {
                let normalized = SsmState::normalize_name(name);
                if params.remove(&normalized).is_some() {
                    deleted.push(name.clone());
                } else {
                    invalid.push(name.clone());
                }
            }
        }
        AwsResponse::json(
            200,
            json!({ "DeletedParameters": deleted, "InvalidParameters": invalid }),
        )
    }

    fn get_parameters_by_path(&self, req: &AwsRequest) -> AwsResponse {
        let path = req
            .params
            .get("Path")
            .and_then(|v| v.as_str())
            .unwrap_or("/")
            .to_string();
        let recursive = bool_of(req.params.get("Recursive"));
        let with_decryption = bool_of(req.params.get("WithDecryption"));
        let filters = req
            .params
            .get("ParameterFilters")
            .and_then(|v| v.as_array())
            .cloned();
        let max_results = req
            .params
            .get("MaxResults")
            .and_then(|v| v.as_i64())
            .unwrap_or(10);
        if max_results > PARAMETER_BY_PATH_MAX_RESULTS {
            return err_validation(&format!(
                "1 validation error detected: Value {max_results} at 'maxResults' failed to satisfy constraint: Member must have value less than or equal to {PARAMETER_BY_PATH_MAX_RESULTS}"));
        }
        let token = req
            .params
            .get("NextToken")
            .and_then(|v| v.as_str())
            .and_then(|t| t.parse::<i64>().ok())
            .unwrap_or(0);
        let state = self.get_state(req.account, &req.region);

        let mut prefix = path.trim_end_matches('/').to_string();
        prefix.push('/');

        let entries = state.parameters.read();
        let mut result = Vec::new();
        for (name, param) in entries.iter() {
            // Use strip_prefix so a multi-byte first character can never split
            // a UTF-8 boundary (byte-index slicing would panic), and accept
            // both "/name" and bare "name" for the root path.
            let rest = if path == "/" {
                Some(name.strip_prefix('/').unwrap_or(name))
            } else {
                name.strip_prefix(&prefix)
            };
            let rest = match rest {
                Some(r) => r,
                None => continue,
            };
            if !recursive && rest.contains('/') {
                continue;
            }
            let Some(version) = param.current() else {
                continue;
            };
            if !matches_parameter_filters(&version, name, filters.as_ref()) {
                continue;
            }
            result.push(version.response_object(
                name,
                with_decryption,
                Some(&req.region),
                req.account,
            ));
        }
        let (page, next_token) = page_slice(&result, token, max_results);
        AwsResponse::json(200, json!({ "Parameters": page, "NextToken": next_token }))
    }

    fn describe_parameters(&self, req: &AwsRequest) -> AwsResponse {
        let filters = req
            .params
            .get("ParameterFilters")
            .and_then(|v| v.as_array())
            .cloned();
        let max_results = req
            .params
            .get("MaxResults")
            .and_then(|v| v.as_i64())
            .unwrap_or(10)
            .clamp(1, 50);
        let token = req
            .params
            .get("NextToken")
            .and_then(|v| v.as_str())
            .and_then(|t| t.parse::<i64>().ok())
            .unwrap_or(0);
        let state = self.get_state(req.account, &req.region);

        let entries = state.parameters.read();
        let mut result = Vec::new();
        for (name, param) in entries.iter() {
            let Some(version) = param.current() else {
                continue;
            };
            if !matches_parameter_filters(&version, name, filters.as_ref()) {
                continue;
            }
            result.push(version.describe_response_object(name, false, false, req.account));
        }
        let (page, next_token) = page_slice(&result, token, max_results);
        AwsResponse::json(200, json!({ "Parameters": page, "NextToken": next_token }))
    }

    fn get_parameter_history(&self, req: &AwsRequest) -> AwsResponse {
        let raw = req
            .params
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let name = SsmState::normalize_name(&raw);
        let with_decryption = bool_of(req.params.get("WithDecryption"));
        let max_results = req
            .params
            .get("MaxResults")
            .and_then(|v| v.as_i64())
            .unwrap_or(50);
        if max_results > crate::models::PARAMETER_HISTORY_MAX_RESULTS {
            return err_validation(&format!(
                "1 validation error detected: Value '{max_results}' at 'maxResults' failed to satisfy constraint: Member must have value less than or equal to 50."));
        }
        let token = req
            .params
            .get("NextToken")
            .and_then(|v| v.as_str())
            .and_then(|t| t.parse::<i64>().ok())
            .unwrap_or(0);
        let state = self.get_state(req.account, &req.region);

        let Some(param) = state.get_parameter(&name) else {
            return AwsResponse::error(
                400,
                "ParameterNotFound",
                &format!("Parameter {raw} not found."),
            );
        };
        let history = param.versions.read().clone();
        let list: Vec<Value> = history
            .iter()
            .map(|v| v.describe_response_object(&param.name, with_decryption, true, req.account))
            .collect();
        let (page, next_token) = page_slice(&list, token, max_results);
        AwsResponse::json(200, json!({ "Parameters": page, "NextToken": next_token }))
    }

    fn label_parameter_version(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let version_number = req.params.get("ParameterVersion").and_then(|v| v.as_u64());
        let labels: Vec<String> = req
            .params
            .get("Labels")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);

        let Some(param) = state.get_parameter(&name) else {
            return AwsResponse::error(
                400,
                "ParameterNotFound",
                &format!("Parameter {name} not found."),
            );
        };
        let target_version =
            version_number.unwrap_or_else(|| param.current().map(|v| v.version).unwrap_or(1));
        let existing = param.version(target_version);
        let Some(_) = existing else {
            return AwsResponse::error(400, "ParameterVersionNotFound",
                &format!("Systems Manager could not find version {target_version} of {name}. Verify the version and try again."));
        };
        let (valid_labels, invalid_labels): (Vec<String>, Vec<String>) =
            labels.into_iter().partition(|l| {
                !(l.starts_with("aws")
                    || l.starts_with("ssm")
                    || l.chars()
                        .next()
                        .map(|c| c.is_ascii_digit())
                        .unwrap_or(false)
                    || !l
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-'))
            });
        let mut versions = param.versions.write();
        for v in versions.iter_mut() {
            if v.version == target_version {
                for l in &valid_labels {
                    if !v.labels.contains(l) {
                        v.labels.push(l.clone());
                    }
                }
            } else {
                v.labels.retain(|l| !valid_labels.contains(l));
            }
        }
        AwsResponse::json(
            200,
            json!({ "InvalidLabels": invalid_labels, "ParameterVersion": target_version }),
        )
    }

    fn unlabel_parameter_version(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let version_number = req.params.get("ParameterVersion").and_then(|v| v.as_u64());
        let labels: Vec<String> = req
            .params
            .get("Labels")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);

        let Some(param) = state.get_parameter(&name) else {
            return AwsResponse::error(
                400,
                "ParameterNotFound",
                &format!("Parameter {name} not found."),
            );
        };
        let Some(version_number) = version_number else {
            return err_validation("1 validation error detected: Value 'null' at 'parameterVersion' failed to satisfy constraint: Member must not be null");
        };
        let found = param.version(version_number);
        if found.is_none() {
            return AwsResponse::error(400, "ParameterVersionNotFound",
                &format!("Systems Manager could not find version {version_number} of {name}. Verify the version and try again."));
        }
        let mut removed = Vec::new();
        let mut invalid = Vec::new();
        for v in param.versions.write().iter_mut() {
            if v.version == version_number {
                for l in &labels {
                    if let Some(idx) = v.labels.iter().position(|x| x == l) {
                        v.labels.remove(idx);
                        removed.push(l.clone());
                    } else {
                        invalid.push(l.clone());
                    }
                }
            }
        }
        AwsResponse::json(
            200,
            json!({ "RemovedLabels": removed, "InvalidLabels": invalid }),
        )
    }

    // ---- tags (parameter / document / maintenance window / patch baseline) ----

    fn resource_ref(&self, req: &AwsRequest) -> Result<(SsmState, String, String), AwsResponse> {
        let rtype = req
            .params
            .get("ResourceType")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let rid = req
            .params
            .get("ResourceId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if rtype.is_empty() {
            return Err(AwsResponse::error(
                400,
                "InvalidResourceType",
                "Invalid Resource Type",
            ));
        }
        if rid.is_empty() {
            return Err(AwsResponse::error(
                400,
                "InvalidResourceId",
                "Invalid Resource Id",
            ));
        }
        let state = self.get_state(req.account, &req.region);
        let normalized = match rtype.as_str() {
            "Parameter" => {
                let normalized = SsmState::normalize_name(&rid);
                if state.get_parameter(&normalized).is_none() {
                    return Err(AwsResponse::error(
                        400,
                        "InvalidResourceId",
                        "Invalid Resource Id",
                    ));
                }
                normalized
            }
            "Document" => {
                if !state.documents.read().contains_key(&rid) {
                    return Err(AwsResponse::error(
                        400,
                        "InvalidResourceId",
                        "Invalid Resource Id",
                    ));
                }
                rid.clone()
            }
            "MaintenanceWindow" => {
                if !state.windows.read().contains_key(&rid) {
                    return Err(AwsResponse::error(
                        400,
                        "InvalidResourceId",
                        "Invalid Resource Id",
                    ));
                }
                rid.clone()
            }
            "ManagedInstance" | "PatchBaseline" | "OpsItem" | "OpsMetadata" => {
                return Err(AwsResponse::error(
                    400,
                    "InvalidResourceId",
                    "Invalid Resource Id",
                ))
            }
            _ => {
                return Err(AwsResponse::error(
                    400,
                    "InvalidResourceType",
                    "Invalid Resource Type",
                ))
            }
        };
        Ok((state, rtype, normalized))
    }

    fn add_tags_to_resource(&self, req: &AwsRequest) -> AwsResponse {
        let (state, rtype, rid) = match self.resource_ref(req) {
            Ok((s, t, r)) => (s, t, r),
            Err(e) => return e,
        };
        if let Some(list) = req.params.get("Tags").and_then(|v| v.as_array()).cloned() {
            for tag in list {
                if let (Some(k), Some(v)) = (
                    tag.get("Key").and_then(|v| v.as_str()),
                    tag.get("Value").and_then(|v| v.as_str()),
                ) {
                    state.add_tag(&rtype, &rid, k, v);
                }
            }
        }
        AwsResponse::json(200, json!({}))
    }

    fn remove_tags_from_resource(&self, req: &AwsRequest) -> AwsResponse {
        let (state, rtype, rid) = match self.resource_ref(req) {
            Ok(v) => v,
            Err(e) => return e,
        };
        let keys: Vec<String> = req
            .params
            .get("TagKeys")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        {
            let mut tags = state.resource_tags.write();
            if let Some(map) = tags.get_mut(&rtype).and_then(|t| t.get_mut(&rid)) {
                for k in &keys {
                    map.remove(k);
                }
            }
        }
        AwsResponse::json(200, json!({}))
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> AwsResponse {
        let (state, _rtype, rid) = match self.resource_ref(req) {
            Ok(v) => v,
            Err(e) => return e,
        };
        let tag_list = state.list_tags(&_rtype, &rid);
        AwsResponse::json(200, json!({ "TagList": tag_list }))
    }

    // -----------------------------------------------------------------------
    // Documents
    // -----------------------------------------------------------------------

    fn create_document(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let content = req
            .params
            .get("Content")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let document_type = req
            .params
            .get("DocumentType")
            .and_then(|v| v.as_str())
            .unwrap_or("Command")
            .to_string();
        let document_format = req
            .params
            .get("DocumentFormat")
            .and_then(|v| v.as_str())
            .unwrap_or("JSON")
            .to_string();
        let state = self.get_state(req.account, &req.region);

        let (content_json, schema_version, description, parameter_list) =
            match Document::parse_content(&content, &document_format) {
                Ok(v) => v,
                Err(_) => {
                    return AwsResponse::error(
                        400,
                        "InvalidDocumentContent",
                        "The content for the document is not valid.",
                    )
                }
            };
        let mut docs = state.documents.write();
        if docs.contains_key(&name) {
            return AwsResponse::error(
                400,
                "DocumentAlreadyExists",
                "The specified document already exists.",
            );
        }
        let doc = Arc::new(Document {
            owner: req.account,
            name: name.clone(),
            version: "1".into(),
            version_name: req
                .params
                .get("VersionName")
                .and_then(|v| v.as_str())
                .map(String::from),
            content: content.clone(),
            content_json,
            document_type,
            document_format,
            requires: req.params.get("Requires").cloned(),
            attachments: req.params.get("Attachments").cloned(),
            target_type: req
                .params
                .get("TargetType")
                .and_then(|v| v.as_str())
                .map(String::from),
            status: "Active".to_string(),
            description,
            schema_version,
            parameter_list,
            created: chrono::Utc::now(),
        });
        docs.insert(
            name.clone(),
            Arc::new(RwLock::new(Documents {
                versions: BTreeMap::from([("1".to_string(), doc.clone())]),
                default_version: "1".to_string(),
                latest_version: "1".to_string(),
                permissions: RwLock::new(Vec::new()),
            })),
        );
        drop(docs);

        let mut tags = json!([]);
        if let Some(list) = req.params.get("Tags").and_then(|v| v.as_array()).cloned() {
            for tag in list {
                if let (Some(k), Some(v)) = (
                    tag.get("Key").and_then(|x| x.as_str()),
                    tag.get("Value").and_then(|x| x.as_str()),
                ) {
                    state.add_tag("Document", &name, k, v);
                }
            }
            tags = state.document_tags(&name);
        }
        AwsResponse::json(
            200,
            json!({ "DocumentDescription": doc.describe(Some(tags)) }),
        )
    }

    fn documents_of(
        &self,
        req: &AwsRequest,
        name: &str,
    ) -> Result<(SsmState, Arc<RwLock<Documents>>), AwsResponse> {
        let state = self.get_state(req.account, &req.region);
        let found = {
            let docs = state.documents.read();
            docs.get(name).cloned()
        };
        found
            .ok_or_else(|| {
                AwsResponse::error(
                    400,
                    "InvalidDocument",
                    "The specified document does not exist.",
                )
            })
            .map(|d| (state, d))
    }

    fn describe_document(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let (state, docs) = match self.documents_of(req, &name) {
            Ok(v) => v,
            Err(e) => return e,
        };
        let container = docs.read();
        let doc = container.versions.get(&container.default_version).cloned();
        let Some(doc) = doc else {
            return AwsResponse::error(
                400,
                "InvalidDocument",
                "The specified document does not exist.",
            );
        };
        let tags = state.document_tags(&name);
        let mut description = doc.describe(Some(tags));
        description["LatestVersion"] = json!(container.latest_version.clone());
        description["DefaultVersion"] = json!(container.default_version.clone());
        AwsResponse::json(200, json!({ "Document": description }))
    }

    fn get_document(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let document_format = req
            .params
            .get("DocumentFormat")
            .and_then(|v| v.as_str())
            .unwrap_or("JSON")
            .to_string();
        let (_, docs) = match self.documents_of(req, &name) {
            Ok(v) => v,
            Err(e) => return e,
        };
        let container = docs.read();
        let doc = container.versions.get(&container.default_version).cloned();
        let Some(doc) = doc else {
            return AwsResponse::error(
                400,
                "InvalidDocument",
                "The specified document does not exist.",
            );
        };
        AwsResponse::json(200, doc.get_document(&document_format))
    }

    fn delete_document(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        if !state.documents.read().contains_key(&name) {
            return AwsResponse::error(
                400,
                "InvalidDocument",
                "The specified document does not exist.",
            );
        }
        let is_shared = state
            .documents
            .read()
            .get(&name)
            .map(|d| !d.read().permissions.read().is_empty())
            .unwrap_or(false);
        if is_shared {
            return AwsResponse::error(
                400,
                "InvalidDocumentOperation",
                "Must unshare document first before delete",
            );
        }
        state.documents.write().remove(&name);
        state
            .resource_tags
            .write()
            .get_mut("Document")
            .map(|m| m.remove(&name));
        AwsResponse::json(200, json!({}))
    }

    fn update_document(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let content = req
            .params
            .get("Content")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let document_version = req
            .params
            .get("DocumentVersion")
            .and_then(|v| v.as_str())
            .unwrap_or("$LATEST")
            .to_string();
        let document_format = req
            .params
            .get("DocumentFormat")
            .and_then(|v| v.as_str())
            .unwrap_or("JSON")
            .to_string();
        let (state, docs) = match self.documents_of(req, &name) {
            Ok(v) => v,
            Err(e) => return e,
        };
        let (content_json, schema_version, description, parameter_list) =
            match Document::parse_content(&content, &document_format) {
                Ok(v) => v,
                Err(_) => {
                    return AwsResponse::error(
                        400,
                        "InvalidDocumentContent",
                        "The content for the document is not valid.",
                    )
                }
            };
        let mut container = docs.write();
        if container.latest_version != document_version && document_version != "$LATEST" {
            return AwsResponse::error(
                400,
                "InvalidDocumentVersion",
                "The document version is not valid or does not exist.",
            );
        }
        let old = container
            .versions
            .get(&container.default_version)
            .cloned()
            .unwrap();
        let new_version = (container.latest_version.parse::<u64>().unwrap_or(0) + 1).to_string();
        for existing in container.versions.values() {
            if existing.content == content {
                return AwsResponse::error(400, "DuplicateDocumentContent",
                    "The content of the association document matches another document. Change the content of the document and try again.");
            }
        }
        let new_doc = Arc::new(Document {
            owner: req.account,
            name: name.clone(),
            version: new_version.clone(),
            version_name: req
                .params
                .get("VersionName")
                .and_then(|v| v.as_str())
                .map(String::from),
            content: content.clone(),
            content_json,
            document_type: old.document_type.clone(),
            document_format,
            requires: old.requires.clone(),
            attachments: req.params.get("Attachments").cloned(),
            target_type: req
                .params
                .get("TargetType")
                .and_then(|v| v.as_str())
                .map(String::from),
            status: "Active".to_string(),
            description,
            schema_version,
            parameter_list,
            created: old.created,
        });
        container.versions.insert(new_version.clone(), new_doc);
        container.latest_version = new_version.clone();
        let tags = state.document_tags(&name);
        let doc = container.versions.get(&new_version).cloned().unwrap();
        let mut description = doc.describe(Some(tags));
        description["LatestVersion"] = json!(container.latest_version.clone());
        description["DefaultVersion"] = json!(container.default_version.clone());
        AwsResponse::json(200, json!({ "DocumentDescription": description }))
    }

    fn update_document_default_version(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let document_version = req
            .params
            .get("DocumentVersion")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let (_, docs) = match self.documents_of(req, &name) {
            Ok(v) => v,
            Err(e) => return e,
        };
        let mut container = docs.write();
        if !container.versions.contains_key(&document_version) {
            return AwsResponse::error(
                400,
                "InvalidDocument",
                "The specified document does not exist.",
            );
        }
        container.default_version = document_version.clone();
        AwsResponse::json(
            200,
            json!({
                "Description": { "Name": name, "DefaultVersion": document_version }
            }),
        )
    }

    fn list_documents(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let filters = req
            .params
            .get("Filters")
            .and_then(|v| v.as_array())
            .cloned();
        let max_results = req
            .params
            .get("MaxResults")
            .and_then(|v| v.as_i64())
            .unwrap_or(10);
        let token = req
            .params
            .get("NextToken")
            .and_then(|v| v.as_str())
            .and_then(|t| t.parse::<i64>().ok())
            .unwrap_or(0);

        let docs_map = state.documents.read();
        let mut result = Vec::new();
        for (name, container) in docs_map.iter() {
            let c = container.read();
            let Some(doc) = c.versions.get(&c.default_version).cloned() else {
                continue;
            };
            if let Some(filters) = &filters {
                let mut matched = true;
                for f in filters {
                    let key = f.get("Key").and_then(|v| v.as_str()).unwrap_or("");
                    let values: Vec<String> = f
                        .get("Values")
                        .and_then(|v| v.as_array())
                        .map(|a| {
                            a.iter()
                                .filter_map(|v| v.as_str().map(String::from))
                                .collect()
                        })
                        .unwrap_or_default();
                    match key {
                        "Name" => {
                            if !values.is_empty() && !values.contains(name) {
                                matched = false;
                            }
                        }
                        "Owner" => {
                            let owner_self =
                                values.iter().any(|v| v == "Self" || v == "123456789012");
                            if !owner_self {
                                matched = false;
                            }
                        }
                        _ => {}
                    }
                    if !matched {
                        break;
                    }
                }
                if !matched {
                    continue;
                }
            }
            result.push(doc.list_describe(Some(state.document_tags(name))));
        }
        drop(docs_map);
        let (page, next_token) = page_slice(&result, token, max_results);
        AwsResponse::json(
            200,
            json!({ "DocumentIdentifiers": page, "NextToken": next_token }),
        )
    }

    fn describe_document_permission(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let (_, docs) = match self.documents_of(req, &name) {
            Ok(v) => v,
            Err(e) => return e,
        };
        let container = docs.read();
        let mut perms = container.permissions.read().clone();
        perms.sort_by_key(|(_, _, created)| *created);
        let account_ids: Vec<&String> = perms.iter().map(|(a, _, _)| a).collect();
        let sharing: Vec<Value> = perms
            .iter()
            .map(|(a, v, _)| json!({ "AccountId": a, "SharedDocumentVersion": v }))
            .collect();
        AwsResponse::json(
            200,
            json!({ "AccountIds": account_ids, "AccountSharingInfoList": sharing }),
        )
    }

    fn modify_document_permission(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let shared_version = req
            .params
            .get("SharedDocumentVersion")
            .and_then(|v| v.as_str())
            .map(String::from);
        let permission_type = req
            .params
            .get("PermissionType")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if let Some(v) = &shared_version {
            if !["$LATEST", "$DEFAULT", "$ALL"].contains(&v.as_str()) {
                return err_validation(&format!(
                    "Value '{v}' at 'sharedDocumentVersion' failed to satisfy constraint: Member must satisfy regular expression pattern: ([$]LATEST|[$]DEFAULT|[$]ALL)."));
            }
        }
        let sanitize = |list: Option<Vec<Value>>| -> Result<Vec<String>, AwsResponse> {
            let mut out = Vec::new();
            for v in list.unwrap_or_default() {
                let Some(s) = v.as_str() else { continue };
                let lower = s.to_lowercase();
                if lower != "all"
                    && !(lower.len() == 12 && lower.chars().all(|c| c.is_ascii_digit()))
                {
                    return Err(err_validation(&format!(
                        "Value '[{s}]' failed to satisfy constraint: Member must satisfy regular expression pattern: (all|[0-9]{{12}}).")));
                }
                out.push(lower);
            }
            Ok(out)
        };
        let to_add = sanitize(
            req.params
                .get("AccountIdsToAdd")
                .and_then(|v| v.as_array())
                .cloned(),
        );
        let to_remove = sanitize(
            req.params
                .get("AccountIdsToRemove")
                .and_then(|v| v.as_array())
                .cloned(),
        );
        let to_add = match to_add {
            Ok(v) => v,
            Err(e) => return e,
        };
        let to_remove = match to_remove {
            Ok(v) => v,
            Err(e) => return e,
        };
        if to_add.len() > 1 && to_add.iter().any(|a| a == "all") {
            return AwsResponse::error(
                400,
                "DocumentPermissionLimit",
                "Accounts can either be all or a group of AWS accounts",
            );
        }
        if to_remove.len() > 1 && to_remove.iter().any(|a| a == "all") {
            return AwsResponse::error(
                400,
                "DocumentPermissionLimit",
                "Accounts can either be all or a group of AWS accounts",
            );
        }
        if permission_type != "Share" {
            return AwsResponse::error(400, "InvalidPermissionType",
                &format!("Value '{permission_type}' at 'permissionType' failed to satisfy constraint: Member must satisfy enum value set: [Share]."));
        }
        let (_, docs) = match self.documents_of(req, &name) {
            Ok(v) => v,
            Err(e) => return e,
        };
        let container = docs.write();
        let now = chrono::Utc::now();
        let mut perms = container.permissions.write();
        if !to_add.is_empty() {
            if to_add.iter().any(|a| a == "all") {
                perms.clear();
            } else {
                perms.retain(|(a, _, _)| a != "all");
            }
            for a in &to_add {
                perms.retain(|(existing, _, _)| existing != a);
                perms.push((
                    a.clone(),
                    shared_version.clone().unwrap_or_else(|| "$DEFAULT".into()),
                    now,
                ));
            }
        }
        if !to_remove.is_empty() {
            if to_remove.iter().any(|a| a == "all") {
                perms.clear();
            } else {
                for a in &to_remove {
                    perms.retain(|(existing, _, _)| existing != a);
                }
            }
        }
        AwsResponse::json(200, json!({}))
    }

    fn list_document_versions(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let (_, docs) = match self.documents_of(req, &name) {
            Ok(v) => v,
            Err(e) => return e,
        };
        let container = docs.read();
        let mut versions: Vec<Value> = Vec::new();
        for (version, doc) in container.versions.iter() {
            let mut entry = json!({
                "Name": doc.name,
                "DocumentVersion": version,
                "CreatedDate": fmt_dt(&doc.created),
                "IsDefaultVersion": version == &container.default_version,
                "DocumentFormat": doc.document_format,
                "Status": doc.status,
            });
            if let Some(vn) = &doc.version_name {
                entry["VersionName"] = json!(vn);
            }
            versions.push(entry);
        }
        AwsResponse::json(200, json!({ "DocumentVersions": versions }))
    }

    fn list_document_metadata_history(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let (_, docs) = match self.documents_of(req, &name) {
            Ok(v) => v,
            Err(e) => return e,
        };
        let container = docs.read();
        let doc = container.versions.get(&container.default_version).cloned();
        let Some(doc) = doc else {
            return AwsResponse::error(
                400,
                "InvalidDocument",
                "The specified document does not exist.",
            );
        };
        AwsResponse::json(
            200,
            json!({
                "Name": doc.name,
                "DocumentVersion": doc.version,
                "Author": "123456789012",
                "Metadata": {}
            }),
        )
    }

    fn update_document_metadata(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        if !state.documents.read().contains_key(&name) {
            return AwsResponse::error(400, "InvalidDocument", &name);
        }
        AwsResponse::json(200, json!({}))
    }

    // -----------------------------------------------------------------------
    // Commands
    // -----------------------------------------------------------------------

    fn send_command(&self, req: &AwsRequest) -> AwsResponse {
        let document_name = req
            .params
            .get("DocumentName")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let mut instance_ids: Vec<String> = req
            .params
            .get("InstanceIds")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let targets = req
            .params
            .get("Targets")
            .cloned()
            .unwrap_or_else(|| json!([]));
        if let Some(list) = targets.as_array() {
            for t in list {
                let key = t
                    .get("Key")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_lowercase();
                if key == "instanceids" {
                    if let Some(values) = t.get("Values").and_then(|v| v.as_array()) {
                        for v in values.iter().filter_map(|x| x.as_str()) {
                            if !instance_ids.iter().any(|s| s == v) {
                                instance_ids.push(v.to_string());
                            }
                        }
                    }
                }
            }
        }
        let timeout = req
            .params
            .get("TimeoutSeconds")
            .and_then(|v| v.as_i64())
            .unwrap_or(3600);
        let command = Command {
            command_id: uuid_str(),
            comment: req
                .params
                .get("Comment")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            document_name,
            max_concurrency: req
                .params
                .get("MaxConcurrency")
                .and_then(|v| v.as_str())
                .unwrap_or("50")
                .to_string(),
            max_errors: req
                .params
                .get("MaxErrors")
                .and_then(|v| v.as_str())
                .unwrap_or("0")
                .to_string(),
            notification_config: req
                .params
                .get("NotificationConfig")
                .cloned()
                .unwrap_or_else(|| {
                    json!({
                        "NotificationArn": "string",
                        "NotificationEvents": ["Success"],
                        "NotificationType": "Command"
                    })
                }),
            output_s3_bucket_name: req
                .params
                .get("OutputS3BucketName")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            output_s3_key_prefix: req
                .params
                .get("OutputS3KeyPrefix")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            output_s3_region: req
                .params
                .get("OutputS3Region")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            parameters: req
                .params
                .get("Parameters")
                .cloned()
                .unwrap_or_else(|| json!({})),
            service_role_arn: req
                .params
                .get("ServiceRoleArn")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            targets,
            instance_ids: instance_ids.clone(),
            requested_date_time: chrono::Utc::now(),
            timeout_seconds: if timeout <= 0 { 3600 } else { timeout },
        };
        let command = Arc::new(command);
        self.get_state(req.account, &req.region)
            .commands
            .write()
            .push(command.clone());
        AwsResponse::json(200, json!({ "Command": command.response_object() }))
    }

    fn command_by_id(&self, state: &SsmState, command_id: &str) -> Option<Arc<Command>> {
        state
            .commands
            .read()
            .iter()
            .find(|c| c.command_id == command_id)
            .cloned()
    }

    fn list_commands(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let command_id = req.params.get("CommandId").and_then(|v| v.as_str());
        let instance_id = req.params.get("InstanceId").and_then(|v| v.as_str());
        let commands: Vec<Arc<Command>> = if let Some(cid) = command_id {
            match self.command_by_id(&state, cid) {
                Some(c) => vec![c],
                None => return AwsResponse::error(400, "InvalidCommandId", "Invalid command id."),
            }
        } else if let Some(iid) = instance_id {
            state
                .commands
                .read()
                .iter()
                .filter(|c| c.instance_ids.iter().any(|i| i == iid))
                .cloned()
                .collect()
        } else {
            state.commands.read().clone()
        };
        let list: Vec<Value> = commands.iter().map(|c| c.response_object()).collect();
        AwsResponse::json(200, json!({ "Commands": list }))
    }

    fn get_command_invocation(&self, req: &AwsRequest) -> AwsResponse {
        let command_id = req
            .params
            .get("CommandId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let instance_id = req
            .params
            .get("InstanceId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let state = self.get_state(req.account, &req.region);
        let Some(command) = self.command_by_id(&state, command_id) else {
            return AwsResponse::error(400, "InvalidCommandId", "Invalid command id.");
        };
        if !command.instance_ids.iter().any(|i| i == instance_id) {
            return AwsResponse::error(400, "InvocationDoesNotExist",
                "An error occurred (InvocationDoesNotExist) when calling the GetCommandInvocation operation");
        }
        AwsResponse::json(200, command.invocation_response(instance_id))
    }

    fn list_command_invocations(&self, _req: &AwsRequest) -> AwsResponse {
        AwsResponse::json(200, json!({ "CommandInvocations": [] }))
    }

    // -----------------------------------------------------------------------
    // Maintenance windows
    // -----------------------------------------------------------------------

    fn create_maintenance_window(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let window = Arc::new(MaintenanceWindow {
            id: MaintenanceWindow::generate_id(),
            name: req
                .params
                .get("Name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            description: req
                .params
                .get("Description")
                .and_then(|v| v.as_str())
                .map(String::from),
            duration: req.params.get("Duration").and_then(|v| v.as_i64()),
            cutoff: req.params.get("Cutoff").and_then(|v| v.as_i64()),
            schedule: req
                .params
                .get("Schedule")
                .and_then(|v| v.as_str())
                .map(String::from),
            schedule_timezone: req
                .params
                .get("ScheduleTimezone")
                .and_then(|v| v.as_str())
                .map(String::from),
            schedule_offset: req.params.get("ScheduleOffset").and_then(|v| v.as_i64()),
            start_date: req
                .params
                .get("StartDate")
                .and_then(|v| v.as_str())
                .map(String::from),
            end_date: req
                .params
                .get("EndDate")
                .and_then(|v| v.as_str())
                .map(String::from),
            targets: RwLock::new(BTreeMap::new()),
            tasks: RwLock::new(BTreeMap::new()),
        });
        let wid = window.id.clone();
        state.windows.write().insert(wid.clone(), window.clone());
        if let Some(list) = req.params.get("Tags").and_then(|v| v.as_array()).cloned() {
            for tag in list {
                if let (Some(k), Some(v)) = (
                    tag.get("Key").and_then(|v| v.as_str()),
                    tag.get("Value").and_then(|v| v.as_str()),
                ) {
                    state.add_tag("MaintenanceWindow", &wid, k, v);
                }
            }
        }
        AwsResponse::json(200, json!({ "WindowId": wid }))
    }

    fn window_by_id(
        &self,
        state: &SsmState,
        window_id: &str,
    ) -> Result<Arc<MaintenanceWindow>, AwsResponse> {
        state.windows.read().get(window_id).cloned().ok_or_else(|| {
            AwsResponse::error(
                400,
                "DoesNotExistException",
                &format!("Maintenance window {window_id} does not exist"),
            )
        })
    }

    fn get_maintenance_window(&self, req: &AwsRequest) -> AwsResponse {
        let wid = req
            .params
            .get("WindowId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let state = self.get_state(req.account, &req.region);
        let w = match self.window_by_id(&state, wid) {
            Ok(w) => w,
            Err(e) => return e,
        };
        AwsResponse::json(200, w.to_json())
    }

    fn describe_maintenance_windows(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let filters = req
            .params
            .get("Filters")
            .and_then(|v| v.as_array())
            .cloned();
        let map = state.windows.read();
        let mut windows = Vec::new();
        for w in map.values() {
            if let Some(filters) = &filters {
                let mut ok = true;
                for f in filters {
                    if f.get("Key").and_then(|v| v.as_str()) == Some("Name") {
                        let values: Vec<&str> = f
                            .get("Values")
                            .and_then(|v| v.as_array())
                            .map(|a| a.iter().filter_map(|x| x.as_str()).collect())
                            .unwrap_or_default();
                        if !values.contains(&w.name.as_str()) {
                            ok = false;
                            break;
                        }
                    }
                }
                if !ok {
                    continue;
                }
            }
            windows.push(w.to_json());
        }
        AwsResponse::json(200, json!({ "WindowIdentities": windows }))
    }

    fn delete_maintenance_window(&self, req: &AwsRequest) -> AwsResponse {
        let wid = req
            .params
            .get("WindowId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        if state.windows.write().remove(&wid).is_none() {
            return AwsResponse::error(
                400,
                "DoesNotExistException",
                &format!("Maintenance window {wid} does not exist"),
            );
        }
        AwsResponse::json(200, json!({}))
    }

    fn register_mw_target(&self, req: &AwsRequest) -> AwsResponse {
        let wid = req
            .params
            .get("WindowId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let window = match self.window_by_id(&state, &wid) {
            Ok(w) => w,
            Err(e) => return e,
        };
        let target = MwTarget {
            window_id: wid.clone(),
            window_target_id: uuid_str(),
            resource_type: req
                .params
                .get("ResourceType")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            targets: req
                .params
                .get("Targets")
                .cloned()
                .unwrap_or_else(|| json!([])),
            name: req
                .params
                .get("Name")
                .and_then(|v| v.as_str())
                .map(String::from),
            description: req
                .params
                .get("Description")
                .and_then(|v| v.as_str())
                .map(String::from),
            owner_information: req
                .params
                .get("OwnerInformation")
                .and_then(|v| v.as_str())
                .map(String::from),
        };
        let tid = target.window_target_id.clone();
        window.targets.write().insert(tid.clone(), target);
        AwsResponse::json(200, json!({ "WindowTargetId": tid }))
    }

    fn describe_mw_targets(&self, req: &AwsRequest) -> AwsResponse {
        let wid = req
            .params
            .get("WindowId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let state = self.get_state(req.account, &req.region);
        let window = match self.window_by_id(&state, wid) {
            Ok(w) => w,
            Err(e) => return e,
        };
        let targets: Vec<Value> = window
            .targets
            .read()
            .values()
            .map(|t| t.to_json())
            .collect();
        AwsResponse::json(200, json!({ "Targets": targets }))
    }

    fn deregister_mw_target(&self, req: &AwsRequest) -> AwsResponse {
        let wid = req
            .params
            .get("WindowId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let tid = req
            .params
            .get("WindowTargetId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let window = match self.window_by_id(&state, &wid) {
            Ok(w) => w,
            Err(e) => return e,
        };
        if window.targets.write().remove(&tid).is_none() {
            return AwsResponse::error(
                400,
                "DoesNotExistException",
                &format!("Maintenance window {tid} does not exist"),
            );
        }
        AwsResponse::json(200, json!({}))
    }

    fn register_mw_task(&self, req: &AwsRequest) -> AwsResponse {
        let wid = req
            .params
            .get("WindowId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let window = match self.window_by_id(&state, &wid) {
            Ok(w) => w,
            Err(e) => return e,
        };
        let task = MwTask {
            window_id: wid.clone(),
            window_task_id: uuid_str(),
            task_type: req
                .params
                .get("TaskType")
                .and_then(|v| v.as_str())
                .map(String::from),
            task_arn: req
                .params
                .get("TaskArn")
                .and_then(|v| v.as_str())
                .map(String::from),
            service_role_arn: req
                .params
                .get("ServiceRoleArn")
                .and_then(|v| v.as_str())
                .map(String::from),
            task_parameters: req.params.get("TaskParameters").cloned(),
            task_invocation_parameters: req.params.get("TaskInvocationParameters").cloned(),
            priority: req.params.get("Priority").and_then(|v| v.as_i64()),
            max_concurrency: req
                .params
                .get("MaxConcurrency")
                .and_then(|v| v.as_str())
                .map(String::from),
            max_errors: req
                .params
                .get("MaxErrors")
                .and_then(|v| v.as_str())
                .map(String::from),
            logging_info: req.params.get("LoggingInfo").cloned(),
            name: req
                .params
                .get("Name")
                .and_then(|v| v.as_str())
                .map(String::from),
            description: req
                .params
                .get("Description")
                .and_then(|v| v.as_str())
                .map(String::from),
            targets: req.params.get("Targets").cloned(),
            cutoff_behavior: req
                .params
                .get("CutoffBehavior")
                .and_then(|v| v.as_str())
                .map(String::from),
            alarm_configurations: req.params.get("AlarmConfigurations").cloned(),
        };
        let tid = task.window_task_id.clone();
        window.tasks.write().insert(tid.clone(), task);
        AwsResponse::json(200, json!({ "WindowTaskId": tid }))
    }

    fn describe_mw_tasks(&self, req: &AwsRequest) -> AwsResponse {
        let wid = req
            .params
            .get("WindowId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let state = self.get_state(req.account, &req.region);
        let window = match self.window_by_id(&state, wid) {
            Ok(w) => w,
            Err(e) => return e,
        };
        let tasks: Vec<Value> = window.tasks.read().values().map(|t| t.to_json()).collect();
        AwsResponse::json(200, json!({ "Tasks": tasks }))
    }

    fn deregister_mw_task(&self, req: &AwsRequest) -> AwsResponse {
        let wid = req
            .params
            .get("WindowId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let tid = req
            .params
            .get("WindowTaskId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let window = match self.window_by_id(&state, &wid) {
            Ok(w) => w,
            Err(e) => return e,
        };
        if window.tasks.write().remove(&tid).is_none() {
            return AwsResponse::error(
                400,
                "DoesNotExistException",
                &format!("Maintenance window {tid} does not exist"),
            );
        }
        AwsResponse::json(200, json!({}))
    }

    fn update_mw_target(&self, req: &AwsRequest) -> AwsResponse {
        let wid = req
            .params
            .get("WindowId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let tid = req
            .params
            .get("WindowTargetId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        if state.windows.read().get(&wid).is_none() {
            return AwsResponse::error(
                400,
                "DoesNotExistException",
                &format!("Maintenance window {wid} does not exist"),
            );
        }
        let window = self.window_by_id(&state, &wid).ok().unwrap();
        let mut targets = window.targets.write();
        let Some(target) = targets.get_mut(&tid) else {
            return AwsResponse::error(
                400,
                "DoesNotExistException",
                &format!("Maintenance window {tid} does not exist"),
            );
        };
        if let Some(t) = req.params.get("Targets") {
            target.targets = t.clone();
        }
        if let Some(v) = req.params.get("OwnerInformation").and_then(|v| v.as_str()) {
            target.owner_information = Some(v.to_string());
        }
        if let Some(v) = req.params.get("Name").and_then(|v| v.as_str()) {
            target.name = Some(v.to_string());
        }
        if let Some(v) = req.params.get("Description").and_then(|v| v.as_str()) {
            target.description = Some(v.to_string());
        }
        let json = target.to_json();
        AwsResponse::json(200, json)
    }

    fn update_mw_task(&self, req: &AwsRequest) -> AwsResponse {
        let wid = req
            .params
            .get("WindowId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let tid = req
            .params
            .get("WindowTaskId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let window = match self.window_by_id(&state, &wid) {
            Ok(w) => w,
            Err(e) => return e,
        };
        let mut tasks = window.tasks.write();
        let Some(task) = tasks.get_mut(&tid) else {
            return AwsResponse::error(
                400,
                "DoesNotExistException",
                &format!("Maintenance window {tid} does not exist"),
            );
        };
        if let Some(v) = req.params.get("Targets") {
            task.targets = Some(v.clone());
        }
        if let Some(v) = req.params.get("TaskArn").and_then(|v| v.as_str()) {
            task.task_arn = Some(v.to_string());
        }
        if let Some(v) = req.params.get("ServiceRoleArn").and_then(|v| v.as_str()) {
            task.service_role_arn = Some(v.to_string());
        }
        if let Some(v) = req.params.get("TaskParameters") {
            task.task_parameters = Some(v.clone());
        }
        if let Some(v) = req.params.get("TaskInvocationParameters") {
            task.task_invocation_parameters = Some(v.clone());
        }
        if let Some(v) = req.params.get("Priority").and_then(|v| v.as_i64()) {
            task.priority = Some(v);
        }
        if let Some(v) = req.params.get("MaxConcurrency").and_then(|v| v.as_str()) {
            task.max_concurrency = Some(v.to_string());
        }
        if let Some(v) = req.params.get("MaxErrors").and_then(|v| v.as_str()) {
            task.max_errors = Some(v.to_string());
        }
        if let Some(v) = req.params.get("LoggingInfo") {
            task.logging_info = Some(v.clone());
        }
        if let Some(v) = req.params.get("Name").and_then(|v| v.as_str()) {
            task.name = Some(v.to_string());
        }
        if let Some(v) = req.params.get("Description").and_then(|v| v.as_str()) {
            task.description = Some(v.to_string());
        }
        if let Some(v) = req.params.get("CutoffBehavior").and_then(|v| v.as_str()) {
            task.cutoff_behavior = Some(v.to_string());
        }
        if let Some(v) = req.params.get("AlarmConfigurations") {
            task.alarm_configurations = Some(v.clone());
        }
        AwsResponse::json(200, task.to_json())
    }

    fn describe_mw_for_target(&self, req: &AwsRequest) -> AwsResponse {
        let resource_type = req
            .params
            .get("ResourceType")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let mut windows = Vec::new();
        for w in state.windows.read().values() {
            for t in w.targets.read().values() {
                if t.resource_type == resource_type {
                    windows.push(json!({ "WindowId": w.id, "Name": w.name }));
                    break;
                }
            }
        }
        AwsResponse::json(200, json!({ "WindowIdentities": windows }))
    }

    // -----------------------------------------------------------------------
    // Patch baselines
    // -----------------------------------------------------------------------

    fn create_patch_baseline(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let baseline = Arc::new(PatchBaseline {
            id: PatchBaseline::generate_id(),
            name: req
                .params
                .get("Name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            operating_system: req
                .params
                .get("OperatingSystem")
                .and_then(|v| v.as_str())
                .map(String::from),
            global_filters: req.params.get("GlobalFilters").cloned(),
            approval_rules: req.params.get("ApprovalRules").cloned(),
            approved_patches: req.params.get("ApprovedPatches").cloned(),
            approved_patches_compliance_level: req
                .params
                .get("ApprovedPatchesComplianceLevel")
                .and_then(|v| v.as_str())
                .map(String::from),
            approved_patches_enable_non_security: req
                .params
                .get("ApprovedPatchesEnableNonSecurity")
                .and_then(|v| v.as_bool()),
            rejected_patches: req.params.get("RejectedPatches").cloned(),
            rejected_patches_action: req
                .params
                .get("RejectedPatchesAction")
                .and_then(|v| v.as_str())
                .map(String::from),
            description: req
                .params
                .get("Description")
                .and_then(|v| v.as_str())
                .map(String::from),
            sources: req.params.get("Sources").cloned(),
            default_baseline: false,
            created: chrono::Utc::now(),
        });
        let id = baseline.id.clone();
        state.baselines.write().insert(id.clone(), baseline);
        AwsResponse::json(200, json!({ "BaselineId": id }))
    }

    fn get_patch_baseline(&self, req: &AwsRequest) -> AwsResponse {
        let bid = req
            .params
            .get("BaselineId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let baseline = state.baselines.read().get(&bid).cloned();
        let Some(baseline) = baseline else {
            return AwsResponse::error(400, "DoesNotExistException", "Baseline does not exist");
        };
        let mut result = baseline.to_json();
        if let Some(obj) = result.as_object_mut() {
            let name = obj.remove("BaselineName").unwrap_or(Value::Null);
            let description = obj.remove("BaselineDescription").unwrap_or(Value::Null);
            if !name.is_null() {
                obj.insert("Name".to_string(), name);
            }
            if !description.is_null() {
                obj.insert("Description".to_string(), description);
            }
        }
        let patch_groups: Vec<String> = state
            .patch_groups
            .read()
            .iter()
            .filter(|(_, assoc)| assoc.read().values().any(|v| v == &bid))
            .map(|(name, _)| name.clone())
            .collect();
        result["CreatedDate"] = json!(fmt_dt(&baseline.created));
        result["ModifiedDate"] = json!(fmt_dt(&chrono::Utc::now()));
        result["PatchGroups"] = json!(patch_groups);
        AwsResponse::json(200, result)
    }

    fn describe_patch_baselines(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let filters = req
            .params
            .get("Filters")
            .and_then(|v| v.as_array())
            .cloned();
        let mut baselines = Vec::new();
        for b in state.baselines.read().values() {
            if let Some(filters) = &filters {
                let mut ok = true;
                for f in filters {
                    if f.get("Key").and_then(|v| v.as_str()) == Some("NAME_PREFIX") {
                        let values: Vec<&str> = f
                            .get("Values")
                            .and_then(|v| v.as_array())
                            .map(|a| a.iter().filter_map(|x| x.as_str()).collect())
                            .unwrap_or_default();
                        if !values.contains(&b.name.as_str()) {
                            ok = false;
                            break;
                        }
                    }
                }
                if !ok {
                    continue;
                }
            }
            baselines.push(b.to_json());
        }
        AwsResponse::json(200, json!({ "BaselineIdentities": baselines }))
    }

    fn delete_patch_baseline(&self, req: &AwsRequest) -> AwsResponse {
        let bid = req
            .params
            .get("BaselineId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        if state.baselines.write().remove(&bid).is_none() {
            return AwsResponse::error(400, "DoesNotExistException", "Baseline does not exist");
        }
        AwsResponse::json(200, json!({}))
    }

    fn register_pb_for_patch_group(&self, req: &AwsRequest) -> AwsResponse {
        let bid = req
            .params
            .get("BaselineId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let patch_group = req
            .params
            .get("PatchGroup")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let os = state
            .baselines
            .read()
            .get(&bid)
            .map(|b| b.operating_system.clone());
        let Some(os) = os else {
            return AwsResponse::error(
                400,
                "DoesNotExistException",
                &format!("{bid} does not exist"),
            );
        };
        let mut groups = state.patch_groups.write();
        let entry = groups
            .entry(patch_group.clone())
            .or_insert_with(|| Arc::new(RwLock::new(BTreeMap::new())));
        entry.write().insert(os, bid.clone());
        AwsResponse::json(200, json!({ "BaselineId": bid, "PatchGroup": patch_group }))
    }

    fn deregister_pb_for_patch_group(&self, req: &AwsRequest) -> AwsResponse {
        let bid = req
            .params
            .get("BaselineId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let patch_group = req
            .params
            .get("PatchGroup")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        if let Some(assoc) = state.patch_groups.read().get(&patch_group).cloned() {
            let Some(b) = state.baselines.read().get(&bid).cloned() else {
                return AwsResponse::error(400, "DoesNotExistException", "Baseline does not exist");
            };
            assoc
                .write()
                .insert(b.operating_system.clone(), bid.clone());
        }
        AwsResponse::json(200, json!({ "BaselineId": bid, "PatchGroup": patch_group }))
    }

    fn get_pb_for_patch_group(&self, req: &AwsRequest) -> AwsResponse {
        let patch_group = req
            .params
            .get("PatchGroup")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let operating_system = req
            .params
            .get("OperatingSystem")
            .and_then(|v| v.as_str())
            .unwrap_or("WINDOWS")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let baseline_id = state
            .patch_groups
            .read()
            .get(&patch_group)
            .cloned()
            .and_then(|assoc| assoc.read().get(&Some(operating_system.clone())).cloned())
            .unwrap_or_else(|| {
                "arn:aws:ssm:us-east-1:075727635805:patchbaseline/pb-09ca3fb51f0412ec3".to_string()
            });
        AwsResponse::json(
            200,
            json!({
                "BaselineId": baseline_id, "PatchGroup": patch_group, "OperatingSystem": operating_system
            }),
        )
    }

    fn get_default_patch_baseline(&self, req: &AwsRequest) -> AwsResponse {
        let os = req
            .params
            .get("OperatingSystem")
            .and_then(|v| v.as_str())
            .unwrap_or("WINDOWS")
            .to_string();
        let short = os.to_lowercase().chars().take(8).collect::<String>();
        AwsResponse::json(
            200,
            json!({
                "BaselineId": format!("pb-{short}0000000000000"),
                "OperatingSystem": os
            }),
        )
    }

    fn describe_patch_group_state(&self, _req: &AwsRequest) -> AwsResponse {
        AwsResponse::json(
            200,
            json!({
                "Instances": 0,
                "InstancesWithInstalledPatches": 0,
                "InstancesWithInstalledOtherPatches": 0,
                "InstancesWithInstalledRejectedPatches": 0,
                "InstancesWithMissingPatches": 0,
                "InstancesWithFailedPatches": 0,
                "InstancesWithNotApplicablePatches": 0,
                "InstancesWithInstalledPendingRebootPatches": 0,
                "InstancesWithCriticalNonCompliantPatches": 0,
                "InstancesWithSecurityNonCompliantPatches": 0,
                "InstancesWithOtherNonCompliantPatches": 0
            }),
        )
    }

    // -----------------------------------------------------------------------
    // Associations
    // -----------------------------------------------------------------------

    fn create_association(&self, req: &AwsRequest) -> AwsResponse {
        let now = chrono::Utc::now();
        let assoc = Arc::new(RwLock::new(Association {
            association_id: uuid_str(),
            name: req
                .params
                .get("Name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            instance_id: req
                .params
                .get("InstanceId")
                .and_then(|v| v.as_str())
                .map(String::from),
            targets: req.params.get("Targets").cloned(),
            parameters: req.params.get("Parameters").cloned(),
            schedule_expression: req
                .params
                .get("ScheduleExpression")
                .and_then(|v| v.as_str())
                .map(String::from),
            output_location: req.params.get("OutputLocation").cloned(),
            association_name: req
                .params
                .get("AssociationName")
                .and_then(|v| v.as_str())
                .map(String::from),
            document_version: req
                .params
                .get("DocumentVersion")
                .and_then(|v| v.as_str())
                .unwrap_or("$DEFAULT")
                .to_string(),
            max_errors: req
                .params
                .get("MaxErrors")
                .and_then(|v| v.as_str())
                .map(String::from),
            max_concurrency: req
                .params
                .get("MaxConcurrency")
                .and_then(|v| v.as_str())
                .map(String::from),
            compliance_severity: req
                .params
                .get("ComplianceSeverity")
                .and_then(|v| v.as_str())
                .unwrap_or("UNSPECIFIED")
                .to_string(),
            apply_only_at_cron_interval: bool_of(req.params.get("ApplyOnlyAtCronInterval")),
            association_version: "1".to_string(),
            date: now,
            last_update_association_date: now,
            last_execution_date: now,
            status: "Success".to_string(),
        }));
        let id = assoc.read().association_id.clone();
        self.get_state(req.account, &req.region)
            .associations
            .write()
            .insert(id.clone(), assoc);
        self.describe_association_internal(req.account, &req.region, &id)
    }

    fn describe_association_internal(&self, account: u64, region: &str, id: &str) -> AwsResponse {
        let state = self.get_state(account, region);
        let found = {
            let associations = state.associations.read();
            associations.get(id).cloned()
        };
        match found {
            Some(assoc) => {
                let description = assoc.read().describe();
                AwsResponse::json(200, json!({ "AssociationDescription": description }))
            }
            None => AwsResponse::error(
                400,
                "DoesNotExistException",
                &format!("Association {id} does not exist"),
            ),
        }
    }

    fn describe_association(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        if let Some(id) = req.params.get("AssociationId").and_then(|v| v.as_str()) {
            return self.describe_association_internal(req.account, &req.region, id);
        }
        let name = req
            .params
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let instance_id = req.params.get("InstanceId").and_then(|v| v.as_str());
        for assoc in state.associations.read().values() {
            let guard = assoc.read();
            if guard.name == name {
                if let Some(iid) = instance_id {
                    if guard.instance_id.as_deref() != Some(iid) {
                        continue;
                    }
                }
                return AwsResponse::json(
                    200,
                    json!({ "AssociationDescription": guard.describe() }),
                );
            }
        }
        AwsResponse::error(
            400,
            "DoesNotExistException",
            &format!("{name} does not exist"),
        )
    }

    fn list_associations(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let list: Vec<Value> = state
            .associations
            .read()
            .values()
            .map(|a| a.read().to_json())
            .collect();
        AwsResponse::json(200, json!({ "Associations": list }))
    }

    fn update_association(&self, req: &AwsRequest) -> AwsResponse {
        let id = req
            .params
            .get("AssociationId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let assoc = state.associations.read().get(&id).cloned();
        let Some(assoc) = assoc else {
            return AwsResponse::error(
                400,
                "DoesNotExistException",
                &format!("{id} does not exist"),
            );
        };
        let mut a = assoc.write();
        if let Some(v) = req.params.get("Parameters") {
            a.parameters = Some(v.clone());
        }
        if let Some(v) = req
            .params
            .get("ScheduleExpression")
            .and_then(|x| x.as_str())
        {
            a.schedule_expression = Some(v.to_string());
        }
        if let Some(v) = req.params.get("OutputLocation") {
            a.output_location = Some(v.clone());
        }
        if let Some(v) = req.params.get("AssociationName").and_then(|x| x.as_str()) {
            a.association_name = Some(v.to_string());
        }
        if let Some(v) = req.params.get("DocumentVersion").and_then(|x| x.as_str()) {
            a.document_version = v.to_string();
        }
        if let Some(v) = req.params.get("MaxErrors").and_then(|x| x.as_str()) {
            a.max_errors = Some(v.to_string());
        }
        if let Some(v) = req.params.get("MaxConcurrency").and_then(|x| x.as_str()) {
            a.max_concurrency = Some(v.to_string());
        }
        if let Some(v) = req
            .params
            .get("ComplianceSeverity")
            .and_then(|x| x.as_str())
        {
            a.compliance_severity = v.to_string();
        }
        if let Some(v) = req.params.get("Name").and_then(|x| x.as_str()) {
            a.name = v.to_string();
        }
        if let Some(v) = req.params.get("Targets") {
            a.targets = Some(v.clone());
        }
        a.association_version = (a.association_version.parse::<u64>().unwrap_or(0) + 1).to_string();
        a.last_update_association_date = chrono::Utc::now();
        AwsResponse::json(200, json!({ "AssociationDescription": a.describe() }))
    }

    fn delete_association(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        if let Some(id) = req.params.get("AssociationId").and_then(|v| v.as_str()) {
            if state.associations.write().remove(id).is_none() {
                return AwsResponse::error(
                    400,
                    "DoesNotExistException",
                    &format!("{id} does not exist"),
                );
            }
            return AwsResponse::json(200, json!({}));
        }
        if let Some(name) = req.params.get("Name").and_then(|v| v.as_str()) {
            let to_remove = state
                .associations
                .read()
                .iter()
                .find(|(_, a)| a.read().name == name)
                .map(|(k, _)| k.clone());
            match to_remove {
                Some(k) => {
                    state.associations.write().remove(&k);
                    AwsResponse::json(200, json!({}))
                }
                None => AwsResponse::error(
                    400,
                    "DoesNotExistException",
                    &format!("{name} does not exist"),
                ),
            }
        } else {
            err_validation("Name or AssociationId must be specified")
        }
    }

    fn list_association_versions(&self, req: &AwsRequest) -> AwsResponse {
        let id = req
            .params
            .get("AssociationId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let assoc = state.associations.read().get(&id).cloned();
        let Some(assoc) = assoc else {
            return AwsResponse::error(
                400,
                "DoesNotExistException",
                &format!("{id} does not exist"),
            );
        };
        let a = assoc.read();
        let count = a.association_version.parse::<u64>().unwrap_or(1);
        let versions: Vec<Value> = (1..=count)
            .map(|v| {
                json!({
                    "AssociationId": a.association_id,
                    "AssociationVersion": v.to_string(),
                    "Name": a.name.clone(),
                    "DocumentVersion": a.document_version.clone(),
                    "Targets": a.targets_value(),
                    "Parameters": a.parameters_value(),
                    "CreatedDate": fmt_dt(&a.date),
                })
            })
            .collect();
        AwsResponse::json(200, json!({ "AssociationVersions": versions }))
    }

    fn update_association_status(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let instance_id = req
            .params
            .get("InstanceId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        for assoc in state.associations.read().values() {
            let mut a = assoc.write();
            if a.name == name && a.instance_id.as_deref() == Some(instance_id.as_str()) {
                a.status = req
                    .params
                    .get("AssociationStatus")
                    .and_then(|s| s.get("Name"))
                    .and_then(|n| n.as_str())
                    .unwrap_or("Success")
                    .to_string();
                a.last_update_association_date = chrono::Utc::now();
                return AwsResponse::json(200, json!({ "AssociationDescription": a.describe() }));
            }
        }
        AwsResponse::error(
            400,
            "DoesNotExistException",
            &format!("{name}-{instance_id} does not exist"),
        )
    }

    fn describe_association_executions(&self, req: &AwsRequest) -> AwsResponse {
        let id = req
            .params
            .get("AssociationId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let assoc = state.associations.read().get(&id).cloned();
        let Some(assoc) = assoc else {
            return AwsResponse::error(
                400,
                "DoesNotExistException",
                &format!("{id} does not exist"),
            );
        };
        let a = assoc.read();
        AwsResponse::json(
            200,
            json!({ "AssociationExecutions": [{
            "AssociationId": id,
            "AssociationVersion": a.association_version.clone(),
            "ExecutionId": uuid_str(),
            "Status": "Success",
            "DetailedStatus": "Success",
            "CreatedTime": fmt_dt(&a.last_execution_date),
            "ResourceCountByStatus": "{\"Success\": 1}",
        }] }),
        )
    }

    fn describe_association_execution_targets(&self, req: &AwsRequest) -> AwsResponse {
        let id = req
            .params
            .get("AssociationId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        if !state.associations.read().contains_key(&id) {
            return AwsResponse::error(
                400,
                "DoesNotExistException",
                &format!("{id} does not exist"),
            );
        }
        AwsResponse::json(200, json!({ "AssociationExecutionTargets": [] }))
    }

    fn describe_effective_instance_associations(&self, req: &AwsRequest) -> AwsResponse {
        let instance_id = req
            .params
            .get("InstanceId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let mut results = Vec::new();
        for assoc in state.associations.read().values() {
            let a = assoc.read();
            let target_match = a
                .targets_value()
                .as_array()
                .map(|list| {
                    list.iter().any(|t| {
                        t.get("Key").and_then(|v| v.as_str()) == Some("InstanceIds")
                            && t.get("Values")
                                .and_then(|v| v.as_array())
                                .map(|vs| {
                                    vs.iter()
                                        .filter_map(|x| x.as_str())
                                        .any(|s| s == instance_id)
                                })
                                .unwrap_or(false)
                    })
                })
                .unwrap_or(false);
            if a.instance_id.as_deref() == Some(instance_id.as_str()) || target_match {
                results.push(json!({
                    "AssociationId": a.association_id,
                    "InstanceId": instance_id,
                    "Content": a.name.clone(),
                    "AssociationVersion": a.association_version.clone(),
                }));
            }
        }
        AwsResponse::json(200, json!({ "Associations": results }))
    }

    fn describe_instance_associations_status(&self, req: &AwsRequest) -> AwsResponse {
        let instance_id = req
            .params
            .get("InstanceId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let mut results = Vec::new();
        for assoc in state.associations.read().values() {
            let a = assoc.read();
            let target_match = a
                .targets_value()
                .as_array()
                .map(|list| {
                    list.iter().any(|t| {
                        t.get("Key").and_then(|v| v.as_str()) == Some("InstanceIds")
                            && t.get("Values")
                                .and_then(|v| v.as_array())
                                .map(|vs| {
                                    vs.iter()
                                        .filter_map(|x| x.as_str())
                                        .any(|s| s == instance_id)
                                })
                                .unwrap_or(false)
                    })
                })
                .unwrap_or(false);
            if a.instance_id.as_deref() == Some(instance_id.as_str()) || target_match {
                results.push(json!({
                    "AssociationId": a.association_id,
                    "Name": a.name.clone(),
                    "DocumentVersion": a.document_version.clone(),
                    "AssociationVersion": a.association_version.clone(),
                    "InstanceId": instance_id,
                    "ExecutionDate": fmt_dt(&a.last_execution_date),
                    "ExecutionSummary": a.status.clone(),
                    "Status": a.status.clone(),
                    "DetailedStatus": "Success",
                    "OutputUrl": {},
                }));
            }
        }
        AwsResponse::json(200, json!({ "InstanceAssociationStatusInfos": results }))
    }

    fn describe_instance_patch_states(&self, req: &AwsRequest) -> AwsResponse {
        let ids: Vec<String> = req
            .params
            .get("InstanceIds")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let now = chrono::Utc::now();
        let states: Vec<Value> = ids
            .iter()
            .map(|iid| {
                json!({
                    "InstanceId": iid,
                    "PatchGroup": "",
                    "BaselineId": "",
                    "OperationStartTime": fmt_dt(&now),
                    "OperationEndTime": fmt_dt(&now),
                    "Operation": "Scan",
                    "InstalledCount": 0,
                    "InstalledOtherCount": 0,
                    "MissingCount": 0,
                    "FailedCount": 0,
                    "NotApplicableCount": 0,
                })
            })
            .collect();
        AwsResponse::json(200, json!({ "InstancePatchStates": states }))
    }

    // -----------------------------------------------------------------------
    // Ops items
    // -----------------------------------------------------------------------

    fn create_ops_item(&self, req: &AwsRequest) -> AwsResponse {
        let title = req
            .params
            .get("Title")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let source = req
            .params
            .get("Source")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if title.is_empty() || source.is_empty() {
            return err_validation("Title and Source are required");
        }
        let id = format!("oi-{}", hex_id(12));
        let now = chrono::Utc::now();
        let item = Arc::new(RwLock::new(OpsItem {
            ops_item_id: id.clone(),
            title,
            description: req
                .params
                .get("Description")
                .and_then(|v| v.as_str())
                .map(String::from),
            source,
            status: "Open".to_string(),
            priority: req.params.get("Priority").and_then(|v| v.as_i64()),
            category: req
                .params
                .get("Category")
                .and_then(|v| v.as_str())
                .map(String::from),
            severity: req
                .params
                .get("Severity")
                .and_then(|v| v.as_str())
                .map(String::from),
            operational_data: req.params.get("OperationalData").cloned(),
            notifications: req.params.get("Notifications").cloned(),
            created_time: now,
            last_modified_time: now,
            version: "1".to_string(),
            related_items: RwLock::new(Vec::new()),
        }));
        self.get_state(req.account, &req.region)
            .ops_items
            .write()
            .insert(id.clone(), item);
        AwsResponse::json(
            200,
            json!({
                "OpsItemId": id,
                "OpsItemArn": format!("arn:aws:ssm:{}:{}:opsitem/{}", req.region, req.account, id)
            }),
        )
    }

    fn get_ops_item(&self, req: &AwsRequest) -> AwsResponse {
        let id = req
            .params
            .get("OpsItemId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let Some(item) = state.ops_items.read().get(&id).cloned() else {
            return AwsResponse::error(
                400,
                "DoesNotExistException",
                &format!("{id} does not exist"),
            );
        };
        AwsResponse::json(
            200,
            json!({ "OpsItem": item.read().to_json(req.account, &req.region) }),
        )
    }

    fn update_ops_item(&self, req: &AwsRequest) -> AwsResponse {
        let id = req
            .params
            .get("OpsItemId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let Some(item) = state.ops_items.read().get(&id).cloned() else {
            return AwsResponse::error(
                400,
                "DoesNotExistException",
                &format!("{id} does not exist"),
            );
        };
        {
            let mut item = item.write();
            if let Some(v) = req.params.get("Title").and_then(|x| x.as_str()) {
                item.title = v.to_string();
            }
            if let Some(v) = req.params.get("Description").and_then(|x| x.as_str()) {
                item.description = Some(v.to_string());
            }
            if let Some(v) = req.params.get("Status").and_then(|x| x.as_str()) {
                item.status = v.to_string();
            }
            if let Some(v) = req.params.get("Priority").and_then(|x| x.as_i64()) {
                item.priority = Some(v);
            }
            if let Some(v) = req.params.get("Category").and_then(|x| x.as_str()) {
                item.category = Some(v.to_string());
            }
            if let Some(v) = req.params.get("Severity").and_then(|x| x.as_str()) {
                item.severity = Some(v.to_string());
            }
            if let Some(v) = req.params.get("OperationalData") {
                item.operational_data = v.clone().into();
            }
            if let Some(v) = req.params.get("Notifications") {
                item.notifications = Some(v.clone());
            }
            item.last_modified_time = chrono::Utc::now();
            item.version = (item.version.parse::<u64>().unwrap_or(0) + 1).to_string();
        }
        AwsResponse::json(200, json!({}))
    }

    fn delete_ops_item(&self, req: &AwsRequest) -> AwsResponse {
        let id = req
            .params
            .get("OpsItemId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        self.get_state(req.account, &req.region)
            .ops_items
            .write()
            .remove(&id);
        AwsResponse::json(200, json!({}))
    }

    fn describe_ops_items(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let summaries: Vec<Value> = state
            .ops_items
            .read()
            .values()
            .map(|i| i.read().summary(req.account, &req.region))
            .collect();
        AwsResponse::json(200, json!({ "OpsItemSummaries": summaries }))
    }

    fn get_ops_summary(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let entities: Vec<Value> = state.ops_items.read().values()
            .map(|i| json!({ "Id": i.read().ops_item_id, "Data": { "AWS:OpsItem": { "Content": [i.read().summary(req.account, &req.region)], "Count": 1 } } }))
            .collect();
        AwsResponse::json(200, json!({ "Entities": entities }))
    }

    fn list_ops_item_related_items(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let mut ops_item_id = None;
        if let Some(filters) = req.params.get("Filters").and_then(|v| v.as_array()) {
            for f in filters {
                if f.get("Key").and_then(|v| v.as_str()) == Some("OpsItemId") {
                    ops_item_id = f
                        .get("Values")
                        .and_then(|v| v.as_array())
                        .and_then(|vals| vals.first())
                        .and_then(|v| v.as_str())
                        .map(String::from);
                }
            }
        }
        if ops_item_id.is_none() {
            ops_item_id = req
                .params
                .get("OpsItemId")
                .and_then(|v| v.as_str())
                .map(String::from);
        }
        match ops_item_id {
            Some(oid) => {
                let item = state.ops_items.read().get(&oid).cloned();
                let Some(item) = item else {
                    return AwsResponse::error(
                        400,
                        "DoesNotExistException",
                        &format!("{oid} does not exist"),
                    );
                };
                let items: Vec<Value> = item.read().related_items.read().clone();
                AwsResponse::json(200, json!({ "Summaries": items }))
            }
            None => {
                let mut all = Vec::new();
                for item in state.ops_items.read().values() {
                    all.extend(item.read().related_items.read().clone());
                }
                AwsResponse::json(200, json!({ "Summaries": all }))
            }
        }
    }

    fn associate_ops_item_related_item(&self, req: &AwsRequest) -> AwsResponse {
        let oid = req
            .params
            .get("OpsItemId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let Some(item) = state.ops_items.read().get(&oid).cloned() else {
            return AwsResponse::error(
                400,
                "DoesNotExistException",
                &format!("{oid} does not exist"),
            );
        };
        let association_id = uuid_str();
        let now = chrono::Utc::now();
        item.read().related_items.write().push(json!({
            "AssociationId": association_id.clone(),
            "OpsItemId": oid,
            "AssociationType": req.params.get("AssociationType").cloned().unwrap_or(Value::Null),
            "ResourceType": req.params.get("ResourceType").cloned().unwrap_or(Value::Null),
            "ResourceUri": req.params.get("ResourceUri").cloned().unwrap_or(Value::Null),
            "CreatedBy": iam_root(req.account),
            "CreatedTime": fmt_dt(&now),
            "LastModifiedBy": iam_root(req.account),
            "LastModifiedTime": fmt_dt(&now),
        }));
        AwsResponse::json(200, json!({ "AssociationId": association_id }))
    }

    fn disassociate_ops_item_related_item(&self, req: &AwsRequest) -> AwsResponse {
        let oid = req
            .params
            .get("OpsItemId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let aid = req
            .params
            .get("AssociationId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let Some(item) = state.ops_items.read().get(&oid).cloned() else {
            return AwsResponse::error(
                400,
                "DoesNotExistException",
                &format!("{oid} does not exist"),
            );
        };
        item.read()
            .related_items
            .write()
            .retain(|ri| ri.get("AssociationId").and_then(|v| v.as_str()) != Some(aid.as_str()));
        AwsResponse::json(200, json!({}))
    }

    // -----------------------------------------------------------------------
    // Ops metadata
    // -----------------------------------------------------------------------

    fn ops_metadata_by_arn(
        &self,
        state: &SsmState,
        arn: &str,
        account: u64,
        region: &str,
    ) -> Option<Arc<RwLock<OpsMetadata>>> {
        state
            .ops_metadata
            .read()
            .values()
            .find(|m| m.read().arn(account, region) == arn)
            .cloned()
    }

    fn create_ops_metadata(&self, req: &AwsRequest) -> AwsResponse {
        let resource_id = req
            .params
            .get("ResourceId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if resource_id.is_empty() {
            return err_validation("ResourceId is required");
        }
        let state = self.get_state(req.account, &req.region);
        if state.ops_metadata.read().contains_key(&resource_id) {
            return err_validation(&format!(
                "An OpsMetadata entry already exists for ResourceId: {resource_id}."
            ));
        }
        let meta = Arc::new(RwLock::new(OpsMetadata {
            resource_id: resource_id.clone(),
            metadata: RwLock::new(
                req.params
                    .get("Metadata")
                    .cloned()
                    .unwrap_or_else(|| json!({})),
            ),
            created: chrono::Utc::now(),
            last_modified: RwLock::new(chrono::Utc::now()),
        }));
        state
            .ops_metadata
            .write()
            .insert(resource_id.clone(), meta.clone());
        AwsResponse::json(
            200,
            json!({ "OpsMetadataArn": meta.read().arn(req.account, &req.region) }),
        )
    }

    fn get_ops_metadata(&self, req: &AwsRequest) -> AwsResponse {
        let arn = req
            .params
            .get("OpsMetadataArn")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let Some(meta) = self.ops_metadata_by_arn(&state, &arn, req.account, &req.region) else {
            return AwsResponse::error(
                400,
                "DoesNotExistException",
                &format!("Maintenance window {arn} does not exist"),
            );
        };
        let m = meta.read();
        AwsResponse::json(
            200,
            json!({
                "ResourceId": m.resource_id,
                "Metadata": *m.metadata.read(),
                "OpsMetadataArn": arn,
            }),
        )
    }

    fn update_ops_metadata(&self, req: &AwsRequest) -> AwsResponse {
        let arn = req
            .params
            .get("OpsMetadataArn")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let Some(meta) = self.ops_metadata_by_arn(&state, &arn, req.account, &req.region) else {
            return AwsResponse::error(
                400,
                "DoesNotExistException",
                &format!("Maintenance window {arn} does not exist"),
            );
        };
        if let Some(update) = req
            .params
            .get("MetadataToUpdate")
            .and_then(|v| v.as_object())
        {
            let m = meta.read();
            let mut metadata = m.metadata.write();
            if let Some(obj) = metadata.as_object_mut() {
                for (k, v) in update {
                    obj.insert(k.clone(), v.clone());
                }
            }
        }
        if let Some(keys) = req.params.get("KeysToDelete").and_then(|v| v.as_array()) {
            let m = meta.read();
            let mut metadata = m.metadata.write();
            if let Some(obj) = metadata.as_object_mut() {
                for k in keys.iter().filter_map(|x| x.as_str()) {
                    obj.remove(k);
                }
            }
        }
        *meta.read().last_modified.write() = chrono::Utc::now();
        AwsResponse::json(200, json!({ "OpsMetadataArn": arn }))
    }

    fn delete_ops_metadata(&self, req: &AwsRequest) -> AwsResponse {
        let arn = req
            .params
            .get("OpsMetadataArn")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let Some(meta) = self.ops_metadata_by_arn(&state, &arn, req.account, &req.region) else {
            return AwsResponse::error(
                400,
                "DoesNotExistException",
                &format!("Maintenance window {arn} does not exist"),
            );
        };
        let resource_id = meta.read().resource_id.clone();
        state.ops_metadata.write().remove(&resource_id);
        AwsResponse::json(200, json!({}))
    }

    fn list_ops_metadata(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let list: Vec<Value> = state
            .ops_metadata
            .read()
            .values()
            .map(|m| {
                let m = m.read();
                json!({
                    "ResourceId": m.resource_id,
                    "OpsMetadataArn": m.arn(req.account, &req.region),
                    "CreationDate": fmt_dt(&m.created),
                    "LastModifiedDate": fmt_dt(&m.last_modified.read()),
                    "LastModifiedUser": format!("arn:aws:iam::{}:root", req.account),
                })
            })
            .collect();
        AwsResponse::json(200, json!({ "OpsMetadataList": list }))
    }

    // -----------------------------------------------------------------------
    // Activations
    // -----------------------------------------------------------------------

    fn create_activation(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let now = chrono::Utc::now();
        let id = format!("act-{}", hex_id(17));
        let expiration = req
            .params
            .get("ExpirationDate")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| fmt_dt(&(now + chrono::Duration::hours(24))));
        let activation = Arc::new(Activation {
            activation_id: id.clone(),
            iam_role: req
                .params
                .get("IamRole")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            description: req
                .params
                .get("Description")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            default_instance_name: req
                .params
                .get("DefaultInstanceName")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            registration_limit: req
                .params
                .get("RegistrationLimit")
                .and_then(|v| v.as_i64())
                .unwrap_or(1),
            registrations_count: 0,
            expired: false,
            expiration_date: expiration,
            created: now,
        });
        state.activations.write().insert(id.clone(), activation);
        AwsResponse::json(200, json!({ "ActivationId": id, "ActivationCode": id }))
    }

    fn describe_activations(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let list: Vec<Value> = state
            .activations
            .read()
            .values()
            .map(|a| a.to_json())
            .collect();
        AwsResponse::json(200, json!({ "ActivationList": list }))
    }

    fn delete_activation(&self, req: &AwsRequest) -> AwsResponse {
        let id = req
            .params
            .get("ActivationId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        if state.activations.write().remove(&id).is_none() {
            return AwsResponse::error(
                400,
                "DoesNotExistException",
                &format!("{id} does not exist"),
            );
        }
        AwsResponse::json(200, json!({}))
    }

    // -----------------------------------------------------------------------
    // Automation
    // -----------------------------------------------------------------------

    fn start_automation_execution(&self, req: &AwsRequest) -> AwsResponse {
        let execution = Arc::new(AutomationExecution {
            automation_execution_id: uuid_str(),
            document_name: req
                .params
                .get("DocumentName")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            document_version: req
                .params
                .get("DocumentVersion")
                .and_then(|v| v.as_str())
                .unwrap_or("1")
                .to_string(),
            parameters: req.params.get("Parameters").cloned(),
            target_parameter_name: req
                .params
                .get("TargetParameterName")
                .and_then(|v| v.as_str())
                .map(String::from),
            targets: req.params.get("Targets").cloned(),
            mode: req
                .params
                .get("Mode")
                .and_then(|v| v.as_str())
                .unwrap_or("Auto")
                .to_string(),
            max_concurrency: req
                .params
                .get("MaxConcurrency")
                .and_then(|v| v.as_str())
                .map(String::from),
            max_errors: req
                .params
                .get("MaxErrors")
                .and_then(|v| v.as_str())
                .map(String::from),
            automation_execution_status: "InProgress".to_string(),
            execution_start_time: chrono::Utc::now(),
            execution_end_time: None,
            executed_by: iam_root(req.account),
        });
        let id = execution.automation_execution_id.clone();
        self.get_state(req.account, &req.region)
            .automation_executions
            .write()
            .insert(id.clone(), execution);
        AwsResponse::json(200, json!({ "AutomationExecutionId": id }))
    }

    fn start_change_request_execution(&self, req: &AwsRequest) -> AwsResponse {
        let execution = Arc::new(AutomationExecution {
            automation_execution_id: uuid_str(),
            document_name: req
                .params
                .get("DocumentName")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            document_version: req
                .params
                .get("DocumentVersion")
                .and_then(|v| v.as_str())
                .unwrap_or("1")
                .to_string(),
            parameters: req.params.get("Parameters").cloned(),
            target_parameter_name: None,
            targets: None,
            mode: "Auto".to_string(),
            max_concurrency: None,
            max_errors: None,
            automation_execution_status: "Pending".to_string(),
            execution_start_time: chrono::Utc::now(),
            execution_end_time: None,
            executed_by: iam_root(req.account),
        });
        let id = execution.automation_execution_id.clone();
        self.get_state(req.account, &req.region)
            .automation_executions
            .write()
            .insert(id.clone(), execution);
        AwsResponse::json(200, json!({ "AutomationExecutionId": id }))
    }

    fn describe_automation_executions(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let list: Vec<Value> = state
            .automation_executions
            .read()
            .values()
            .map(|e| e.to_json())
            .collect();
        AwsResponse::json(200, json!({ "AutomationExecutionMetadataList": list }))
    }

    fn get_automation_execution(&self, req: &AwsRequest) -> AwsResponse {
        let id = req
            .params
            .get("AutomationExecutionId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let Some(execution) = state.automation_executions.read().get(&id).cloned() else {
            return AwsResponse::error(
                400,
                "DoesNotExistException",
                &format!("{id} does not exist"),
            );
        };
        AwsResponse::json(200, json!({ "AutomationExecution": execution.to_json() }))
    }

    fn stop_automation_execution(&self, req: &AwsRequest) -> AwsResponse {
        let id = req
            .params
            .get("AutomationExecutionId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let Some(_) = state.automation_executions.read().get(&id).cloned() else {
            return AwsResponse::error(
                400,
                "DoesNotExistException",
                &format!("{id} does not exist"),
            );
        };
        // Status transitions are recorded but the backend stores immutable executions.
        AwsResponse::json(200, json!({}))
    }

    // -----------------------------------------------------------------------
    // Sessions
    // -----------------------------------------------------------------------

    fn start_session(&self, req: &AwsRequest) -> AwsResponse {
        let session_id = format!("{}-{}", req.account, uuid_str());
        AwsResponse::json(
            200,
            json!({
                "SessionId": session_id,
                "TokenValue": uuid_str(),
                "StreamUrl": format!("wss://ssmmessages.{}.amazonaws.com/v1/data-channel/{}", req.region, session_id)
            }),
        )
    }

    fn resume_session(&self, req: &AwsRequest) -> AwsResponse {
        let session_id = req
            .params
            .get("SessionId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        AwsResponse::json(
            200,
            json!({
                "SessionId": session_id,
                "TokenValue": uuid_str(),
                "StreamUrl": format!("wss://ssmmessages.{}.amazonaws.com/v1/data-channel/{}", req.region, session_id)
            }),
        )
    }

    fn terminate_session(&self, req: &AwsRequest) -> AwsResponse {
        let session_id = req
            .params
            .get("SessionId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        AwsResponse::json(200, json!({ "SessionId": session_id }))
    }

    // -----------------------------------------------------------------------
    // Service settings
    // -----------------------------------------------------------------------

    fn get_service_setting(&self, req: &AwsRequest) -> AwsResponse {
        let setting_id = req
            .params
            .get("SettingId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        AwsResponse::json(
            200,
            json!({
                "ServiceSetting": {
                    "SettingId": setting_id,
                    "SettingValue": "",
                    "LastModifiedDate": "1970-01-01 00:00:00+00:00",
                    "LastModifiedUser": "",
                    "ARN": format!("arn:aws:ssm:{}:{}:servicesetting/{}", req.region, req.account, setting_id),
                    "Status": "Default"
                }
            }),
        )
    }

    // -----------------------------------------------------------------------
    // Resource data sync
    // -----------------------------------------------------------------------

    fn create_resource_data_sync(&self, req: &AwsRequest) -> AwsResponse {
        let sync_name = req
            .params
            .get("SyncName")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        if state.resource_data_syncs.read().contains_key(&sync_name) {
            return err_validation(&format!("A sync with the name {sync_name} already exists."));
        }
        let now = chrono::Utc::now();
        let sync = Arc::new(RwLock::new(ResourceDataSync {
            sync_name: sync_name.clone(),
            s3_destination: req.params.get("S3Destination").cloned(),
            sync_type: req
                .params
                .get("SyncType")
                .and_then(|v| v.as_str())
                .unwrap_or("SyncToDestination")
                .to_string(),
            sync_source: req.params.get("SyncSource").cloned(),
            sync_created_time: now,
            last_sync_time: now,
            last_successful_sync_time: now,
            sync_last_modified_time: now,
            last_status: "Successful".to_string(),
        }));
        state.resource_data_syncs.write().insert(sync_name, sync);
        AwsResponse::json(200, json!({}))
    }

    fn list_resource_data_sync(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let items: Vec<Value> = state
            .resource_data_syncs
            .read()
            .values()
            .map(|s| s.read().to_json())
            .collect();
        AwsResponse::json(200, json!({ "ResourceDataSyncItems": items }))
    }

    fn delete_resource_data_sync(&self, req: &AwsRequest) -> AwsResponse {
        let sync_name = req
            .params
            .get("SyncName")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        if state
            .resource_data_syncs
            .write()
            .remove(&sync_name)
            .is_none()
        {
            return AwsResponse::error(
                400,
                "DoesNotExistException",
                &format!("{sync_name} does not exist"),
            );
        }
        AwsResponse::json(200, json!({}))
    }

    fn update_resource_data_sync(&self, req: &AwsRequest) -> AwsResponse {
        let sync_name = req
            .params
            .get("SyncName")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let Some(sync) = state.resource_data_syncs.read().get(&sync_name).cloned() else {
            return AwsResponse::error(
                400,
                "DoesNotExistException",
                &format!("{sync_name} does not exist"),
            );
        };
        if let Some(source) = req.params.get("SyncSource") {
            sync.write().sync_source = Some(source.clone());
        }
        AwsResponse::json(200, json!({}))
    }

    // -----------------------------------------------------------------------
    // Resource policies
    // -----------------------------------------------------------------------

    fn get_resource_policies(&self, req: &AwsRequest) -> AwsResponse {
        let arn = req
            .params
            .get("ResourceArn")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let read_policies = state.resource_policies.read();
        let policies = match read_policies.get(&arn) {
            Some(policy) => vec![policy.read().clone()],
            None => vec![],
        };
        AwsResponse::json(200, json!({ "Policies": policies }))
    }

    fn put_resource_policy(&self, req: &AwsRequest) -> AwsResponse {
        let arn = req
            .params
            .get("ResourceArn")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let policy = req
            .params
            .get("Policy")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let policy_id = req
            .params
            .get("PolicyId")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(uuid_str);
        let policy_hash = crate::models::sha256_hex(&policy);
        let stored = json!({
            "PolicyId": policy_id.clone(),
            "PolicyHash": policy_hash,
            "Policy": policy
        });
        let state = self.get_state(req.account, &req.region);
        state
            .resource_policies
            .write()
            .insert(arn, RwLock::new(stored.clone()));
        AwsResponse::json(
            200,
            json!({ "PolicyId": policy_id, "PolicyHash": policy_hash }),
        )
    }

    fn delete_resource_policy(&self, req: &AwsRequest) -> AwsResponse {
        let arn = req
            .params
            .get("ResourceArn")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let policy_id = req
            .params
            .get("PolicyId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let state = self.get_state(req.account, &req.region);
        let mut policies = state.resource_policies.write();
        let exists = policies
            .get(&arn)
            .map(|p| p.read().get("PolicyId").and_then(|v| v.as_str()) == Some(policy_id))
            .unwrap_or(false);
        if exists {
            policies.remove(&arn);
            AwsResponse::json(200, json!({}))
        } else {
            AwsResponse::error(
                400,
                "DoesNotExistException",
                &format!("{arn} does not exist"),
            )
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn iam_root(account: u64) -> String {
    format!("arn:aws:iam::{account}:root")
}

fn bool_of(v: Option<&Value>) -> bool {
    matches!(v, Some(Value::Bool(true)))
        || v.and_then(|x| x.as_str())
            .map(|s| s == "true")
            .unwrap_or(false)
}

/// moto-style page slicing: returns a page and the next-token value.
fn page_slice(values: &[Value], token: i64, max_results: i64) -> (Vec<Value>, Value) {
    let end = (token + max_results).max(0) as usize;
    let start = (token.max(0)) as usize;
    let page: Vec<Value> = values
        .iter()
        .skip(start)
        .take(end.saturating_sub(start))
        .cloned()
        .collect();
    let next_token = if page.len() == max_results.max(0) as usize {
        json!(end.to_string())
    } else {
        Value::Null
    };
    (page, next_token)
}

/// moto's `_match_filters` subset for parameter filtering.
fn matches_parameter_filters(
    version: &ParamVersion,
    name: &str,
    filters: Option<&Vec<Value>>,
) -> bool {
    let Some(filters) = filters else { return true };
    let norm = |v: &str| format!("/{}", v.trim_start_matches('/'));
    for f in filters {
        let key = f.get("Key").and_then(|v| v.as_str()).unwrap_or("");
        let option = if key == "Path" {
            f.get("Option")
                .and_then(|v| v.as_str())
                .unwrap_or("OneLevel")
        } else {
            f.get("Option").and_then(|v| v.as_str()).unwrap_or("Equals")
        };
        let values: Vec<String> = f
            .get("Values")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let matched = match key {
            "KeyId" => match &version.keyid {
                Some(k) => values.is_empty() || values.iter().any(|v| k == v),
                None => false,
            },
            "Name" => {
                if option == "Contains" {
                    let name_norm = name.trim_start_matches('/');
                    values
                        .iter()
                        .any(|v| name_norm.contains(v.trim_start_matches('/')))
                } else if option == "BeginsWith" {
                    let what = norm(name);
                    values.iter().any(|v| what.starts_with(&norm(v)))
                } else {
                    let what = norm(name);
                    values.iter().any(|v| what == norm(v))
                }
            }
            "Path" => {
                let what = norm(name);
                match option {
                    "OneLevel" => values.iter().any(|v| {
                        let v = norm(v);
                        v == "/"
                            || (what.starts_with(&format!("{v}/"))
                                && what[v.len()..].matches('/').count() == 1)
                    }),
                    "Recursive" => values.iter().any(|v| {
                        let v = norm(v);
                        v == "/" || what.starts_with(&format!("{v}/"))
                    }),
                    _ => values.iter().any(|v| what == norm(v)),
                }
            }
            "Type" => values.is_empty() || values.contains(&version.parameter_type),
            "Tier" => values.is_empty() || values.contains(&version.tier),
            "Label" => {
                !version.labels.is_empty()
                    && version.labels.iter().all(|l| values.iter().any(|v| v == l))
            }
            _ => true,
        };
        if !matched {
            return false;
        }
    }
    true
}

fn strip_arn_prefix(raw: &str, region: &str, account: u64) -> String {
    let prefix = format!("arn:aws:ssm:{region}:{account}:parameter");
    match raw.strip_prefix(&prefix) {
        Some(rest) => rest.to_string(),
        None => raw.to_string(),
    }
}

fn err_validation(message: &str) -> AwsResponse {
    AwsResponse::error(400, "ValidationException", message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use serde_json::json;

    fn make_req(operation: &str, params: Value) -> AwsRequest {
        AwsRequest {
            service: "ssm".to_string(),
            operation: operation.to_string(),
            account: 123456789012,
            region: "us-east-1".to_string(),
            params,
            body: Bytes::new(),
        }
    }

    fn body(resp: &AwsResponse) -> Value {
        serde_json::from_str(&resp.body).unwrap()
    }

    #[test]
    fn test_put_and_get_parameter() {
        let handler = SsmHandler::default();
        handler.handle(make_req(
            "PutParameter",
            json!({
                "Name": "/app/env", "Value": "production", "Type": "String"
            }),
        ));
        let resp = handler.handle(make_req("GetParameter", json!({ "Name": "/app/env" })));
        assert_eq!(resp.status, 200);
        assert_eq!(body(&resp)["Parameter"]["Value"], "production");
        assert_eq!(body(&resp)["Parameter"]["DataType"], "text");
    }

    #[test]
    fn test_parameter_overwrite_versions_and_history() {
        let handler = SsmHandler::default();
        handler.handle(make_req(
            "PutParameter",
            json!({
                "Name": "/ow", "Value": "v1", "Type": "String"
            }),
        ));
        let dup = handler.handle(make_req(
            "PutParameter",
            json!({
                "Name": "/ow", "Value": "again", "Type": "String"
            }),
        ));
        assert_eq!(dup.status, 400);
        assert_eq!(body(&dup)["__type"], "ParameterAlreadyExists");
        handler.handle(make_req(
            "PutParameter",
            json!({
                "Name": "/ow", "Value": "v2", "Type": "String", "Overwrite": true
            }),
        ));
        let resp = handler.handle(make_req("GetParameter", json!({ "Name": "/ow" })));
        assert_eq!(body(&resp)["Parameter"]["Version"], 2);
        let history = handler.handle(make_req("GetParameterHistory", json!({ "Name": "/ow" })));
        let versions: Vec<u64> = body(&history)["Parameters"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["Version"].as_u64().unwrap())
            .collect();
        assert_eq!(versions, vec![1, 2]);
    }

    #[test]
    fn test_get_parameter_version_selector() {
        let handler = SsmHandler::default();
        handler.handle(make_req(
            "PutParameter",
            json!({ "Name": "/vsel", "Value": "v1", "Type": "String" }),
        ));
        handler.handle(make_req(
            "PutParameter",
            json!({ "Name": "/vsel", "Value": "v2", "Type": "String", "Overwrite": true }),
        ));
        let resp = handler.handle(make_req("GetParameter", json!({ "Name": "/vsel:1" })));
        assert_eq!(body(&resp)["Parameter"]["Value"], "v1");
    }

    #[test]
    fn test_get_parameters_by_path_recursive() {
        let handler = SsmHandler::default();
        handler.handle(make_req(
            "PutParameter",
            json!({ "Name": "/rec/a/b", "Value": "1", "Type": "String" }),
        ));
        handler.handle(make_req(
            "PutParameter",
            json!({ "Name": "/rec/a/c/d", "Value": "2", "Type": "String" }),
        ));
        let resp = handler.handle(make_req(
            "GetParametersByPath",
            json!({ "Path": "/rec/a", "Recursive": true }),
        ));
        assert_eq!(body(&resp)["Parameters"].as_array().unwrap().len(), 2);
        let resp = handler.handle(make_req("GetParametersByPath", json!({ "Path": "/rec/a" })));
        assert_eq!(body(&resp)["Parameters"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn test_document_lifecycle() {
        let handler = SsmHandler::default();
        let content = r#"{"schemaVersion":"2.2","description":"d","mainSteps":[]}"#;
        handler.handle(make_req("CreateDocument", json!({
            "Name": "doc", "Content": content, "DocumentType": "Command", "DocumentFormat": "JSON"
        })));
        let get = handler.handle(make_req("GetDocument", json!({ "Name": "doc" })));
        assert_eq!(body(&get)["Name"], "doc");
        assert_eq!(body(&get)["DocumentVersion"], "1");
        let upd = handler.handle(make_req(
            "UpdateDocument",
            json!({
                "Name": "doc", "Content": content, "DocumentVersion": "$LATEST"
            }),
        ));
        assert_eq!(upd.status, 400); // DuplicateDocumentContent
        let upd = handler.handle(make_req("UpdateDocument", json!({
            "Name": "doc", "Content": content.replace("\"d\"", "\"d2\""), "DocumentVersion": "$LATEST"
        })));
        assert_eq!(body(&upd)["DocumentDescription"]["DocumentVersion"], "2");
        handler.handle(make_req(
            "UpdateDocumentDefaultVersion",
            json!({ "Name": "doc", "DocumentVersion": "2" }),
        ));
        let desc = handler.handle(make_req("DescribeDocument", json!({ "Name": "doc" })));
        assert_eq!(body(&desc)["Document"]["DefaultVersion"], "2");
        let versions = handler.handle(make_req("ListDocumentVersions", json!({ "Name": "doc" })));
        assert_eq!(
            body(&versions)["DocumentVersions"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn test_maintenance_window_crud() {
        let handler = SsmHandler::default();
        let resp = handler.handle(make_req(
            "CreateMaintenanceWindow",
            json!({
                "Name": "mw", "Schedule": "rate(1 day)", "Duration": 2, "Cutoff": 1,
                "AllowUnassociatedTargets": true
            }),
        ));
        let wid = body(&resp)["WindowId"].as_str().unwrap().to_string();
        assert!(wid.starts_with("mw-"));
        let got = handler.handle(make_req(
            "GetMaintenanceWindow",
            json!({ "WindowId": &wid }),
        ));
        assert_eq!(body(&got)["Name"], "mw");
        assert_eq!(body(&got)["Duration"], 2);
        let target = handler.handle(make_req(
            "RegisterTargetWithMaintenanceWindow",
            json!({
                "WindowId": wid, "ResourceType": "INSTANCE",
                "Targets": [{"Key": "tag:E", "Values": ["p"]}]
            }),
        ));
        let target_id = body(&target)["WindowTargetId"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(!target_id.is_empty());
        let desc = handler.handle(make_req("DescribeMaintenanceWindows", json!({})));
        assert_eq!(body(&desc)["WindowIdentities"].as_array().unwrap().len(), 1);
        let gone = handler.handle(make_req(
            "DeleteMaintenanceWindow",
            json!({ "WindowId": wid }),
        ));
        assert_eq!(gone.status, 200);
        let missing = handler.handle(make_req("GetMaintenanceWindow", json!({ "WindowId": wid })));
        assert_eq!(body(&missing)["__type"], "DoesNotExistException");
    }

    #[test]
    fn test_association_and_ops_item() {
        let handler = SsmHandler::default();
        let resp = handler.handle(make_req(
            "CreateAssociation",
            json!({
                "Name": "AWS-RunShellScript",
                "Targets": [{"Key": "instanceids", "Values": ["i-0"]}]
            }),
        ));
        let aid = body(&resp)["AssociationDescription"]["AssociationId"]
            .as_str()
            .unwrap()
            .to_string();
        let desc = handler.handle(make_req(
            "DescribeAssociation",
            json!({ "AssociationId": &aid }),
        ));
        assert_eq!(
            body(&desc)["AssociationDescription"]["Name"],
            "AWS-RunShellScript"
        );
        assert!(
            body(&desc)["AssociationDescription"]["LastUpdateAssociationDate"]
                .as_str()
                .is_some()
        );

        let created = handler.handle(make_req(
            "CreateOpsItem",
            json!({
                "Title": "t", "Source": "s", "Description": "dd", "Priority": 3
            }),
        ));
        let oid = body(&created)["OpsItemId"].as_str().unwrap().to_string();
        assert!(oid.starts_with("oi-"));
        let got = handler.handle(make_req("GetOpsItem", json!({ "OpsItemId": &oid })));
        assert_eq!(body(&got)["OpsItem"]["Status"], "Open");
        handler.handle(make_req(
            "UpdateOpsItem",
            json!({ "OpsItemId": &oid, "Status": "Resolved" }),
        ));
        let got = handler.handle(make_req("GetOpsItem", json!({ "OpsItemId": &oid })));
        assert_eq!(body(&got)["OpsItem"]["Status"], "Resolved");
    }

    #[test]
    fn test_patch_baseline_and_commands() {
        let handler = SsmHandler::default();
        let resp = handler.handle(make_req(
            "CreatePatchBaseline",
            json!({
                "Name": "pb", "Description": "desc", "OperatingSystem": "AMAZON_LINUX_2"
            }),
        ));
        let bid = body(&resp)["BaselineId"].as_str().unwrap().to_string();
        assert!(bid.starts_with("pb-") && bid.len() == 20);
        let got = handler.handle(make_req("GetPatchBaseline", json!({ "BaselineId": &bid })));
        assert_eq!(body(&got)["Name"], "pb");
        assert_eq!(body(&got)["Description"], "desc");

        let sent = handler.handle(make_req(
            "SendCommand",
            json!({
                "DocumentName": "AWS-RunShellScript",
                "Parameters": {"commands": ["echo hello"]},
                "Targets": [{"Key": "instanceids", "Values": ["i-1"]}],
                "Comment": "cc"
            }),
        ));
        let cid = body(&sent)["Command"]["CommandId"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(body(&sent)["Command"]["Comment"], "cc");
        assert_eq!(body(&sent)["Command"]["InstanceIds"], json!(["i-1"]));
        let listed = handler.handle(make_req("ListCommands", json!({ "CommandId": &cid })));
        assert_eq!(body(&listed)["Commands"].as_array().unwrap().len(), 1);
    }
}
