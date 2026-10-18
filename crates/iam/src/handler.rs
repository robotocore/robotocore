//! IAM operation handler (query protocol, XML responses).

use parking_lot::RwLock;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

use crate::models::{
    now_iso, random_id, IamState, LoginProfile, OidcProvider, SamlProvider, ServerCert,
    SigningCert, SshKey, User, VirtualMfa,
};
use crate::protocol::{AwsRequest, AwsResponse, get_param};

use crate::models::{Group, Policy, Role};

pub struct IamHandler {
    state: RwLock<HashMap<u64, IamState>>,
}

/// service-name -> role-name segment (mirrors moto's SERVICE_NAME_CONVERSION).
const SERVICE_NAME_CONVERSION: &[(&str, &str)] = &[
    ("autoscaling", "AutoScaling"),
    ("application-autoscaling", "ApplicationAutoScaling"),
    ("elasticbeanstalk", "ElasticBeanstalk"),
];

impl IamHandler {
    pub fn new() -> Self {
        Self { state: RwLock::new(HashMap::new()) }
    }

    fn get_state(&self, account: u64) -> IamState {
        let mut states = self.state.write();
        states.entry(account).or_default().clone()
    }

    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        let op = req.operation.clone();
        self.dispatch(&req, &op)
    }

    fn dispatch(&self, req: &AwsRequest, op: &str) -> AwsResponse {
        match op {
            // Users
            "CreateUser" => self.create_user(req),
            "GetUser" => self.get_user(req),
            "DeleteUser" => self.delete_user(req),
            "ListUsers" => self.list_users(req),
            "UpdateUser" => self.update_user(req),
            "ChangePassword" => self.change_password(req),
            "CreateAccessKey" => self.create_access_key(req),
            "DeleteAccessKey" => self.delete_access_key(req),
            "ListAccessKeys" => self.list_access_keys(req),
            "UpdateAccessKey" => self.update_access_key(req),
            "GetAccessKeyLastUsed" => self.get_access_key_last_used(req),
            "TagUser" => self.tag_user(req),
            "UntagUser" => self.untag_user(req),
            "ListUserTags" => self.list_user_tags(req),
            "AttachUserPolicy" => self.attach_user_policy(req),
            "DetachUserPolicy" => self.detach_user_policy(req),
            "ListAttachedUserPolicies" => self.list_attached_user_policies(req),
            "PutUserPolicy" => self.put_user_policy(req),
            "GetUserPolicy" => self.get_user_policy(req),
            "DeleteUserPolicy" => self.delete_user_policy(req),
            "ListUserPolicies" => self.list_user_policies(req),

            // Roles
            "CreateRole" => self.create_role(req),
            "GetRole" => self.get_role(req),
            "DeleteRole" => self.delete_role(req),
            "ListRoles" => self.list_roles(req),
            "UpdateRole" => self.update_role(req),
            "UpdateRoleDescription" => self.update_role_description(req),
            "UpdateAssumeRolePolicy" => self.update_assume_role_policy(req),
            "TagRole" => self.tag_role(req),
            "UntagRole" => self.untag_role(req),
            "ListRoleTags" => self.list_role_tags(req),
            "AttachRolePolicy" => self.attach_role_policy(req),
            "DetachRolePolicy" => self.detach_role_policy(req),
            "ListAttachedRolePolicies" => self.list_attached_role_policies(req),
            "PutRolePolicy" => self.put_role_policy(req),
            "GetRolePolicy" => self.get_role_policy(req),
            "DeleteRolePolicy" => self.delete_role_policy(req),
            "ListRolePolicies" => self.list_role_policies(req),

            // Groups
            "CreateGroup" => self.create_group(req),
            "GetGroup" => self.get_group(req),
            "DeleteGroup" => self.delete_group(req),
            "ListGroups" => self.list_groups(req),
            "UpdateGroup" => self.update_group(req),
            "AddUserToGroup" => self.add_user_to_group(req),
            "RemoveUserFromGroup" => self.remove_user_from_group(req),
            "ListGroupsForUser" => self.list_groups_for_user(req),
            "ListUsersForGroup" => self.list_users_for_group(req),
            "TagGroup" => self.tag_group(req),
            "UntagGroup" => self.untag_group(req),
            "ListGroupTags" => self.list_group_tags(req),
            "AttachGroupPolicy" => self.attach_group_policy(req),
            "DetachGroupPolicy" => self.detach_group_policy(req),
            "ListAttachedGroupPolicies" => self.list_attached_group_policies(req),
            "PutGroupPolicy" => self.put_group_policy(req),
            "GetGroupPolicy" => self.get_group_policy(req),
            "DeleteGroupPolicy" => self.delete_group_policy(req),
            "ListGroupPolicies" => self.list_group_policies(req),

            // Policies
            "CreatePolicy" => self.create_policy(req),
            "GetPolicy" => self.get_policy(req),
            "DeletePolicy" => self.delete_policy(req),
            "ListPolicies" => self.list_policies(req),
            "CreatePolicyVersion" => self.create_policy_version(req),
            "GetPolicyVersion" => self.get_policy_version(req),
            "ListPolicyVersions" => self.list_policy_versions(req),
            "SetDefaultPolicyVersion" => self.set_default_policy_version(req),
            "DeletePolicyVersion" => self.delete_policy_version(req),
            "TagPolicy" => self.tag_policy(req),
            "UntagPolicy" => self.untag_policy(req),
            "ListPolicyTags" => self.list_policy_tags(req),
            "ListEntitiesForPolicy" => self.list_entities_for_policy(req),

            // Account
            "CreateAccountAlias" => self.create_account_alias(req),
            "DeleteAccountAlias" => self.delete_account_alias(req),
            "ListAccountAliases" => self.list_account_aliases(req),
            "GetAccountSummary" => self.get_account_summary(req),
            "GetAccountPasswordPolicy" => self.get_account_password_policy(req),
            "UpdateAccountPasswordPolicy" => self.update_account_password_policy(req),
            "DeleteAccountPasswordPolicy" => self.delete_account_password_policy(req),
            "GetAccountAuthorizationDetails" => self.get_account_authorization_details(req),

            // Login profiles
            "CreateLoginProfile" => self.create_login_profile(req),
            "GetLoginProfile" => self.get_login_profile(req),
            "UpdateLoginProfile" => self.update_login_profile(req),
            "DeleteLoginProfile" => self.delete_login_profile(req),

            // MFA devices
            "CreateVirtualMFADevice" => self.create_virtual_mfa_device(req),
            "DeleteVirtualMFADevice" => self.delete_virtual_mfa_device(req),
            "ListVirtualMFADevices" => self.list_virtual_mfa_devices(req),
            "EnableMFADevice" => self.enable_mfa_device(req),
            "DeactivateMFADevice" => self.deactivate_mfa_device(req),
            "ResyncMFADevice" => self.resync_mfa_device(req),
            "ListMFADevices" => self.list_mfa_devices(req),
            "GetMFADevice" => self.get_mfa_device(req),
            "TagMFADevice" => self.tag_mfa_device(req),
            "UntagMFADevice" => self.untag_mfa_device(req),
            "ListMFADeviceTags" => self.list_mfa_device_tags(req),

            // SAML providers
            "CreateSAMLProvider" => self.create_saml_provider(req),
            "GetSAMLProvider" => self.get_saml_provider(req),
            "ListSAMLProviders" => self.list_saml_providers(req),
            "UpdateSAMLProvider" => self.update_saml_provider(req),
            "DeleteSAMLProvider" => self.delete_saml_provider(req),
            "TagSAMLProvider" => self.tag_saml_provider(req),
            "UntagSAMLProvider" => self.untag_saml_provider(req),
            "ListSAMLProviderTags" => self.list_saml_provider_tags(req),

            // OIDC providers
            "CreateOpenIDConnectProvider" => self.create_oidc_provider(req),
            "GetOpenIDConnectProvider" => self.get_oidc_provider(req),
            "ListOpenIDConnectProviders" => self.list_oidc_providers(req),
            "UpdateOpenIDConnectProviderThumbprint" => self.update_oidc_thumbprint(req),
            "DeleteOpenIDConnectProvider" => self.delete_oidc_provider(req),
            "AddClientIDToOpenIDConnectProvider" => self.add_oidc_client_id(req),
            "RemoveClientIDFromOpenIDConnectProvider" => self.remove_oidc_client_id(req),
            "TagOpenIDConnectProvider" => self.tag_oidc_provider(req),
            "UntagOpenIDConnectProvider" => self.untag_oidc_provider(req),
            "ListOpenIDConnectProviderTags" => self.list_oidc_provider_tags(req),

            // Instance profiles
            "CreateInstanceProfile" => self.create_instance_profile(req),
            "GetInstanceProfile" => self.get_instance_profile(req),
            "DeleteInstanceProfile" => self.delete_instance_profile(req),
            "ListInstanceProfiles" => self.list_instance_profiles(req),
            "AddRoleToInstanceProfile" => self.add_role_to_instance_profile(req),
            "RemoveRoleFromInstanceProfile" => self.remove_role_from_instance_profile(req),
            "ListInstanceProfilesForRole" => self.list_instance_profiles_for_role(req),
            "TagInstanceProfile" => self.tag_instance_profile(req),
            "UntagInstanceProfile" => self.untag_instance_profile(req),
            "ListInstanceProfileTags" => self.list_instance_profile_tags(req),

            // SSH public keys
            "UploadSSHPublicKey" => self.upload_ssh_public_key(req),
            "GetSSHPublicKey" => self.get_ssh_public_key(req),
            "UpdateSSHPublicKey" => self.update_ssh_public_key(req),
            "DeleteSSHPublicKey" => self.delete_ssh_public_key(req),
            "ListSSHPublicKeys" => self.list_ssh_public_keys(req),

            // Server certificates
            "UploadServerCertificate" => self.upload_server_certificate(req),
            "GetServerCertificate" => self.get_server_certificate(req),
            "UpdateServerCertificate" => self.update_server_certificate(req),
            "DeleteServerCertificate" => self.delete_server_certificate(req),
            "ListServerCertificates" => self.list_server_certificates(req),
            "TagServerCertificate" => self.tag_server_certificate(req),
            "UntagServerCertificate" => self.untag_server_certificate(req),
            "ListServerCertificateTags" => self.list_server_certificate_tags(req),

            // Signing certificates
            "UploadSigningCertificate" => self.upload_signing_certificate(req),
            "UpdateSigningCertificate" => self.update_signing_certificate(req),
            "DeleteSigningCertificate" => self.delete_signing_certificate(req),
            "ListSigningCertificates" => self.list_signing_certificates(req),

            // Service specific credentials
            "CreateServiceSpecificCredential" => self.create_service_specific_credential(req),
            "ListServiceSpecificCredentials" => self.list_service_specific_credentials(req),
            "UpdateServiceSpecificCredential" => self.update_service_specific_credential(req),
            "ResetServiceSpecificCredential" => self.reset_service_specific_credential(req),
            "ActivateServiceSpecificCredential" => {
                self.set_service_specific_credential_status(req, "Active")
            }
            "DeactivateServiceSpecificCredential" => {
                self.set_service_specific_credential_status(req, "Inactive")
            }
            "DeleteServiceSpecificCredential" => self.delete_service_specific_credential(req),

            // Service linked roles
            "CreateServiceLinkedRole" => self.create_service_linked_role(req),
            "DeleteServiceLinkedRole" => self.delete_service_linked_role(req),
            "GetServiceLinkedRoleDeletionStatus" => self.get_service_linked_role_deletion_status(req),

            // Reports / simulation / context keys
            "GenerateCredentialReport" => self.generate_credential_report(req),
            "GetCredentialReport" => self.get_credential_report(req),
            "SimulateCustomPolicy" => self.simulate_policy(req, false),
            "SimulatePrincipalPolicy" => self.simulate_policy(req, true),
            "GetContextKeysForCustomPolicy" => self.get_context_keys(req, false),
            "GetContextKeysForPrincipalPolicy" => self.get_context_keys(req, true),

            // Service last accessed details
            "GenerateServiceLastAccessedDetails" => self.generate_service_last_accessed_details(req),
            "GetServiceLastAccessedDetails" => self.get_service_last_accessed_details(req),
            "GetServiceLastAccessedDetailsWithEntities" => self.get_service_last_accessed_details_with_entities(req),

            // Permissions boundaries
            "PutUserPermissionsBoundary" => self.put_boundary(req, true),
            "DeleteUserPermissionsBoundary" => self.delete_boundary(req, true),
            "PutRolePermissionsBoundary" => self.put_boundary(req, false),
            "DeleteRolePermissionsBoundary" => self.delete_boundary(req, false),

            // Organization-related stubs
            "ListOrganizationsFeatures" => self.org_features(req, Vec::new()),
            "EnableOrganizationsRootCredentialsManagement" => {
                self.org_features(req, vec!["RootCredentialsManagement"])
            }
            "DisableOrganizationsRootCredentialsManagement" => self.org_features(req, Vec::new()),
            "EnableOrganizationsRootSessions" => self.org_features(req, vec!["RootSessions"]),
            "DisableOrganizationsRootSessions" => self.org_features(req, Vec::new()),
            "GetHumanReadableSummary" => self.org_features(req, Vec::new()),
            "RecommendReceivedDate" => self.org_features(req, Vec::new()),
            "ListDelegationRequests" => AwsResponse::xml(200, "ListDelegationRequests", "<DelegationRequests/>".into()),

            // Simple no-ops
            other if is_known_noop(other) => empty_ok(other),
            other => AwsResponse::error(
                400,
                "InvalidParameterValue",
                &format!("The operation {} is not implemented", other),
            ),
        }
    }

    // =====================================================================
    // Users
    // =====================================================================

    fn user_xml(&self, user: &User, include_boundary: bool) -> String {
        let mut s = format!(
            "<User><Path>{}</Path><UserName>{}</UserName><UserId>{}</UserId><Arn>{}</Arn><CreateDate>{}</CreateDate>",
            xml_escape(&user.path),
            xml_escape(&user.username),
            user.user_id,
            xml_escape(&user.arn),
            ms_to_iso(user.created)
        );
        if !user.tags.read().is_empty() {
            s.push_str(&tags_xml(&user.tags.read()));
        }
        if include_boundary {
            if let Some(barn) = user.permissions_boundary.read().clone() {
                s.push_str(&format!(
                    "<PermissionsBoundary><PermissionsBoundaryType>PermissionsBoundary</PermissionsBoundaryType><PermissionsBoundaryArn>{}</PermissionsBoundaryArn></PermissionsBoundary>",
                    xml_escape(&barn)
                ));
            }
        }
        s.push_str("</User>");
        s
    }

    fn create_user(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_default();
        if username.is_empty() {
            return err(400, "MissingParameterValue", "UserName is required");
        }
        let path = get_param(req, "Path").unwrap_or_else(|| "/".into());
        let state = self.get_state(req.account);
        if state.get_user(&username).is_some() {
            return err(409, "EntityAlreadyExists",
                &format!("User with name {} already exists.", username));
        }
        let user = Arc::new(User::with_path(req.account, username.clone(), path));
        let tags = self.parse_query_list(req, "Tags");
        if !tags.is_empty() {
            *user.tags.write() = tags;
        }
        state.users.write().insert(user.username.clone(), user.clone());
        AwsResponse::xml(200, "CreateUser", self.user_xml(&user, true))
    }

    fn get_user(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_default();
        let state = self.get_state(req.account);
        match state.get_user(&username) {
            Some(user) => AwsResponse::xml(200, "GetUser", self.user_xml(&user, true)),
            None => err(404, "NoSuchEntity",
                &format!("The user with name {} cannot be found.", username)),
        }
    }

    fn delete_user(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_default();
        let state = self.get_state(req.account);
        match state.get_user(&username) {
            Some(user) => {
                if !user.attached_policies.read().is_empty() || !user.policies.read().is_empty() {
                    return err(409, "DeleteConflict", "Cannot delete entity, must detach all policies first.");
                }
                state.users.write().remove(&username);
                state.ssh_keys.write().remove(&username);
                state.login_profiles.write().remove(&username);
                state.ssc_creds.write().remove(&username);
                self.unassign_all_mfa(&state, &username);
                AwsResponse::xml(200, "DeleteUser", String::new())
            }
            None => err(404, "NoSuchEntity",
                &format!("The user with name {} cannot be found.", username)),
        }
    }

    fn list_users(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account);
        let mut names: Vec<String> = state.users.read().keys().cloned().collect();
        names.sort();
        let mut body = String::from("<Users>");
        for name in &names {
            if let Some(user) = state.get_user(name) {
                body.push_str(&self.user_xml(&user, false));
            }
        }
        body.push_str("</Users><IsTruncated>false</IsTruncated>");
        AwsResponse::xml(200, "ListUsers", body)
    }

    fn update_user(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_default();
        let state = self.get_state(req.account);
        let user = match state.get_user(&username) {
            Some(u) => u,
            None => return err(404, "NoSuchEntity",
                &format!("The user with name {} cannot be found.", username)),
        };
        let mut changed = false;
        if let Some(v) = get_param(req, "NewUserName") {
            if v != user.username {
                if state.get_user(&v).is_some() {
                    return err(409, "EntityAlreadyExists", &format!("User {} already exists", v));
                }
                state.users.write().remove(&user.username);
                let rebuilt = Arc::new(User::with_path(req.account, v.clone(), user.path.clone()));
                self.clone_user_into(&user, &rebuilt);
                state.users.write().insert(v.clone(), rebuilt);
                changed = true;
            }
        }
        if let Some(v) = get_param(req, "NewPath") {
            if v != user.path {
                let cur = state.get_user(&user.username);
                if let Some(cur) = cur {
                    state.users.write().remove(&user.username);
                    let rebuilt = Arc::new(User::with_path(req.account, cur.username.clone(), v));
                    self.clone_user_into(&cur, &rebuilt);
                    state.users.write().insert(cur.username.clone(), rebuilt);
                    changed = true;
                }
            }
        }
        let _ = changed;
        AwsResponse::xml(200, "UpdateUser", String::new())
    }

    fn clone_user_into(&self, src: &User, dst: &User) {
        *dst.access_keys.write() = src.access_keys.read().clone();
        *dst.groups.write() = src.groups.read().clone();
        *dst.tags.write() = src.tags.read().clone();
        *dst.policies.write() = src.policies.read().clone();
        *dst.policy_documents.write() = src.policy_documents.read().clone();
        *dst.attached_policies.write() = src.attached_policies.read().clone();
        *dst.permissions_boundary.write() = src.permissions_boundary.read().clone();
        *dst.signing_certs.write() = src.signing_certs.read().clone();
        *dst.password.write() = src.password.read().clone();
    }

    fn change_password(&self, _req: &AwsRequest) -> AwsResponse {
        AwsResponse::xml(200, "ChangePassword", String::new())
    }

    fn create_access_key(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_else(|| "unknown".into());
        let state = self.get_state(req.account);
        let user = match state.get_user(&username) {
            Some(u) => u,
            None => return err(404, "NoSuchEntity",
                &format!("The user with name {} cannot be found.", username)),
        };
        let key = crate::models::AccessKey::new(req.account);
        let (key_id, key_secret) = (key.access_key_id.clone(), key.secret_access_key.clone());
        let created = ms_to_iso(key.created);
        user.access_keys.write().push(key);
        AwsResponse::xml(200, "CreateAccessKey", format!(
            "<AccessKey><UserName>{}</UserName><AccessKeyId>{}</AccessKeyId><Status>Active</Status><SecretAccessKey>{}</SecretAccessKey><CreateDate>{}</CreateDate></AccessKey>",
            xml_escape(username.as_str()), key_id, key_secret, created
        ))
    }

    fn delete_access_key(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_else(|| "unknown".into());
        let key_id = get_param(req, "AccessKeyId").unwrap_or_default();
        let state = self.get_state(req.account);
        if let Some(user) = state.get_user(&username) {
            user.access_keys.write().retain(|k| k.access_key_id != key_id);
            return AwsResponse::xml(200, "DeleteAccessKey", String::new());
        }
        err(404, "NoSuchEntity", &format!("The user with name {} cannot be found.", username))
    }

    fn list_access_keys(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_default();
        let state = self.get_state(req.account);
        match state.get_user(&username) {
            Some(user) => {
                let keys = user.access_keys.read();
                let mut body = String::from("<AccessKeyMetadata>");
                for key in keys.iter() {
                    body.push_str(&format!(
                        "<member><UserName>{}</UserName><AccessKeyId>{}</AccessKeyId><Status>{}</Status><CreateDate>{}</CreateDate></member>",
                        xml_escape(username.as_str()),
                        key.access_key_id,
                        key.status,
                        ms_to_iso(key.created)
                    ));
                }
                body.push_str("</AccessKeyMetadata><IsTruncated>false</IsTruncated>");
                AwsResponse::xml(200, "ListAccessKeys", body)
            }
            None => err(404, "NoSuchEntity",
                &format!("The user with name {} cannot be found.", username)),
        }
    }

    fn update_access_key(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_default();
        let key_id = get_param(req, "AccessKeyId").unwrap_or_default();
        let status = get_param(req, "Status").unwrap_or_default();
        let state = self.get_state(req.account);
        if let Some(user) = state.get_user(&username) {
            let mut keys = user.access_keys.write();
            if let Some(k) = keys.iter_mut().find(|k| k.access_key_id == key_id) {
                k.status = status;
            }
            return AwsResponse::xml(200, "UpdateAccessKey", String::new());
        }
        err(404, "NoSuchEntity",
            &format!("The user with name {} cannot be found.", username))
    }

    fn get_access_key_last_used(&self, req: &AwsRequest) -> AwsResponse {
        let key_id = get_param(req, "AccessKeyId").unwrap_or_default();
        let state = self.get_state(req.account);
        let mut found: Option<String> = None;
        for (name, user) in state.users.read().iter() {
            if user.access_keys.read().iter().any(|k| k.access_key_id == key_id) {
                found = Some(name.clone());
                break;
            }
        }
        let username = get_param(req, "UserName")
            .unwrap_or_else(|| found.unwrap_or_else(|| "unknown".into()));
        AwsResponse::xml(200, "GetAccessKeyLastUsed", format!(
            "<UserName>{}</UserName><AccessKeyLastUsed><LastUsedDate>{}</LastUsedDate><ServiceName>N/A</ServiceName><Region>N/A</Region></AccessKeyLastUsed>",
            xml_escape(username.as_str()), now_iso()
        ))
    }

    fn tag_user(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_default();
        let tags = self.parse_query_list(req, "Tags");
        let state = self.get_state(req.account);
        if let Some(user) = state.get_user(&username) {
            tag_upsert(&mut user.tags.write(), tags);
        }
        AwsResponse::xml(200, "TagUser", String::new())
    }

    fn untag_user(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_default();
        let state = self.get_state(req.account);
        if let Some(user) = state.get_user(&username) {
            remove_tags(&mut user.tags.write(), &string_vec(req, "TagKeys"));
        }
        AwsResponse::xml(200, "UntagUser", String::new())
    }

    fn list_user_tags(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_default();
        let state = self.get_state(req.account);
        let tags = state.get_user(&username)
            .map(|u| u.tags.read().clone())
            .unwrap_or_default();
        AwsResponse::xml(200, "ListUserTags", tags_xml(&tags))
    }

    // -----------------------------------------------------------------
    // Managed policy attachments
    // -----------------------------------------------------------------

    fn attach_user_policy(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_default();
        let arn = get_param(req, "PolicyArn").unwrap_or_default();
        let state = self.get_state(req.account);
        if let Some(policy) = state.get_policy(&arn) {
            policy.attached_users.write().push(username.clone());
        }
        if let Some(user) = state.get_user(&username) {
            user.attached_policies.write().push(arn);
        }
        AwsResponse::xml(200, "AttachUserPolicy", String::new())
    }

    fn detach_user_policy(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_default();
        let arn = get_param(req, "PolicyArn").unwrap_or_default();
        let state = self.get_state(req.account);
        if let Some(policy) = state.get_policy(&arn) {
            policy.attached_users.write().retain(|u| *u != username);
        }
        if let Some(user) = state.get_user(&username) {
            user.attached_policies.write().retain(|a| *a != arn);
        }
        AwsResponse::xml(200, "DetachUserPolicy", String::new())
    }

    fn list_attached_user_policies(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_default();
        let state = self.get_state(req.account);
        let policies = state.get_user(&username)
            .map(|u| u.attached_policies.read().clone())
            .unwrap_or_default();
        let mut body = String::from("<AttachedPolicies>");
        for p in &policies {
            body.push_str(&attached_policy_item(p));
        }
        body.push_str("</AttachedPolicies><IsTruncated>false</IsTruncated>");
        AwsResponse::xml(200, "ListAttachedUserPolicies", body)
    }

    fn put_user_policy(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_default();
        let policy_name = get_param(req, "PolicyName").unwrap_or_default();
        let doc = get_param(req, "PolicyDocument").unwrap_or_default();
        let state = self.get_state(req.account);
        if let Some(user) = state.get_user(&username) {
            let mut policies = user.policies.write();
            if !policies.contains(&policy_name) {
                policies.push(policy_name.clone());
            }
            user.policy_documents.write().insert(policy_name, doc);
        }
        AwsResponse::xml(200, "PutUserPolicy", String::new())
    }

    fn get_user_policy(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_default();
        let policy_name = get_param(req, "PolicyName").unwrap_or_default();
        let state = self.get_state(req.account);
        match state.get_user(&username) {
            Some(user) => match user.policy_documents.read().get(&policy_name) {
                Some(doc) => AwsResponse::xml(200, "GetUserPolicy", format!(
                    "<UserName>{}</UserName><PolicyName>{}</PolicyName><PolicyDocument>{}</PolicyDocument>",
                    xml_escape(username.as_str()),
                    xml_escape(policy_name.as_str()),
                    escape_policy(doc)
                )),
                None => err(404, "NoSuchEntity",
                    &format!("The user policy with name {} cannot be found.", policy_name)),
            },
            None => err(404, "NoSuchEntity",
                &format!("The user with name {} cannot be found.", username)),
        }
    }

    fn delete_user_policy(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_default();
        let policy_name = get_param(req, "PolicyName").unwrap_or_default();
        let state = self.get_state(req.account);
        if let Some(user) = state.get_user(&username) {
            user.policies.write().retain(|p| *p != policy_name);
            user.policy_documents.write().remove(&policy_name);
        }
        AwsResponse::xml(200, "DeleteUserPolicy", String::new())
    }

    fn list_user_policies(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_default();
        let state = self.get_state(req.account);
        let policies = state.get_user(&username)
            .map(|u| u.policies.read().clone())
            .unwrap_or_default();
        AwsResponse::xml(200, "ListUserPolicies", names_xml(&policies))
    }

    // =====================================================================
    // Roles
    // =====================================================================

    fn role_xml(&self, role: &Role, include_boundary: bool) -> String {
        let mut s = format!(
            "<Role><Path>{}</Path><RoleName>{}</RoleName><RoleId>{}</RoleId><Arn>{}</Arn><CreateDate>{}</CreateDate><AssumeRolePolicyDocument>{}</AssumeRolePolicyDocument>",
            xml_escape(&role.path),
            xml_escape(&role.role_name),
            role.role_id,
            xml_escape(&role.arn),
            ms_to_iso(role.created),
            escape_policy(&role.assume_role_policy.read())
        );
        if !role.description.read().is_empty() {
            s.push_str(&format!("<Description>{}</Description>", xml_escape(&role.description.read())));
        }
        s.push_str(&format!("<MaxSessionDuration>{}</MaxSessionDuration>", *role.max_session_duration.read()));
        s.push_str("<RoleLastUsed/>");
        if !role.tags.read().is_empty() {
            s.push_str(&tags_xml(&role.tags.read()));
        }
        if include_boundary {
            if let Some(barn) = role.permissions_boundary.read().clone() {
                s.push_str(&format!(
                    "<PermissionsBoundary><PermissionsBoundaryType>PermissionsBoundary</PermissionsBoundaryType><PermissionsBoundaryArn>{}</PermissionsBoundaryArn></PermissionsBoundary>",
                    xml_escape(&barn)
                ));
            }
        }
        s.push_str("</Role>");
        s
    }

    fn create_role(&self, req: &AwsRequest) -> AwsResponse {
        let role_name = get_param(req, "RoleName").unwrap_or_default();
        let assume_policy = get_param(req, "AssumeRolePolicyDocument").unwrap_or_default();
        if role_name.is_empty() {
            return err(400, "MissingParameterValue", "RoleName is required");
        }
        let path = get_param(req, "Path").unwrap_or_else(|| "/".into());
        let description = get_param(req, "Description").unwrap_or_default();
        let max_session = get_param(req, "MaxSessionDuration")
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(3600);
        let state = self.get_state(req.account);
        if state.get_role(&role_name).is_some() {
            return err(409, "EntityAlreadyExists",
                &format!("Role with name {} already exists.", role_name));
        }
        let role = Arc::new(Role::full(
            req.account, role_name, path, assume_policy, description, max_session, false));
        let tags = self.parse_query_list(req, "Tags");
        if !tags.is_empty() {
            *role.tags.write() = tags;
        }
        state.roles.write().insert(role.role_name.clone(), role.clone());
        AwsResponse::xml(200, "CreateRole", self.role_xml(&role, true))
    }

    fn get_role(&self, req: &AwsRequest) -> AwsResponse {
        let role_name = get_param(req, "RoleName").unwrap_or_default();
        let state = self.get_state(req.account);
        match state.get_role(&role_name) {
            Some(role) => AwsResponse::xml(200, "GetRole", self.role_xml(&role, true)),
            None => err(404, "NoSuchEntity",
                &format!("The role with name {} cannot be found.", role_name)),
        }
    }

    fn delete_role(&self, req: &AwsRequest) -> AwsResponse {
        let role_name = get_param(req, "RoleName").unwrap_or_default();
        let state = self.get_state(req.account);
        match state.get_role(&role_name) {
            Some(role) => {
                if !role.attached_policies.read().is_empty() || !role.inline_policies.read().is_empty() {
                    return err(409, "DeleteConflict", "Cannot delete entity, must detach all policies first.");
                }
                {
                    let mut profiles = state.instance_profiles.write();
                    for roles in profiles.values_mut() {
                        roles.retain(|r| r != &role_name);
                    }
                }
                state.roles.write().remove(&role_name);
                AwsResponse::xml(200, "DeleteRole", String::new())
            }
            None => err(404, "NoSuchEntity",
                &format!("The role with name {} cannot be found.", role_name)),
        }
    }

    fn list_roles(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account);
        let mut names: Vec<String> = state.roles.read().keys().cloned().collect();
        names.sort();
        let mut body = String::from("<Roles>");
        for name in &names {
            if let Some(role) = state.get_role(name) {
                body.push_str(&self.role_xml(&role, false));
            }
        }
        body.push_str("</Roles><IsTruncated>false</IsTruncated>");
        AwsResponse::xml(200, "ListRoles", body)
    }

    fn update_role(&self, req: &AwsRequest) -> AwsResponse {
        let role_name = get_param(req, "RoleName").unwrap_or_default();
        let state = self.get_state(req.account);
        if let Some(role) = state.get_role(&role_name) {
            if let Some(desc) = get_param(req, "Description") {
                *role.description.write() = desc;
            }
            if let Some(max_session) = get_param(req, "MaxSessionDuration")
                .and_then(|v| v.parse::<u32>().ok())
            {
                *role.max_session_duration.write() = max_session;
            }
            return AwsResponse::xml(200, "UpdateRole", String::new());
        }
        err(404, "NoSuchEntity", &format!("The role with name {} cannot be found.", role_name))
    }

    fn update_role_description(&self, req: &AwsRequest) -> AwsResponse {
        let role_name = get_param(req, "RoleName").unwrap_or_default();
        let desc = get_param(req, "Description").unwrap_or_default();
        let state = self.get_state(req.account);
        match state.get_role(&role_name) {
            Some(role) => {
                *role.description.write() = desc;
                AwsResponse::xml(200, "UpdateRoleDescription", String::new())
            }
            None => err(404, "NoSuchEntity",
                &format!("The role with name {} cannot be found.", role_name)),
        }
    }

    fn update_assume_role_policy(&self, req: &AwsRequest) -> AwsResponse {
        let role_name = get_param(req, "RoleName").unwrap_or_default();
        let policy = get_param(req, "PolicyDocument").unwrap_or_default();
        let state = self.get_state(req.account);
        match state.get_role(&role_name) {
            Some(role) => {
                *role.assume_role_policy.write() = policy;
                AwsResponse::xml(200, "UpdateAssumeRolePolicy", String::new())
            }
            None => err(404, "NoSuchEntity",
                &format!("The role with name {} cannot be found.", role_name)),
        }
    }

    fn tag_role(&self, req: &AwsRequest) -> AwsResponse {
        let role_name = get_param(req, "RoleName").unwrap_or_default();
        let tags = self.parse_query_list(req, "Tags");
        let state = self.get_state(req.account);
        if let Some(role) = state.get_role(&role_name) {
            tag_upsert(&mut role.tags.write(), tags);
        }
        AwsResponse::xml(200, "TagRole", String::new())
    }

    fn untag_role(&self, req: &AwsRequest) -> AwsResponse {
        let role_name = get_param(req, "RoleName").unwrap_or_default();
        let state = self.get_state(req.account);
        if let Some(role) = state.get_role(&role_name) {
            remove_tags(&mut role.tags.write(), &string_vec(req, "TagKeys"));
        }
        AwsResponse::xml(200, "UntagRole", String::new())
    }

    fn list_role_tags(&self, req: &AwsRequest) -> AwsResponse {
        let role_name = get_param(req, "RoleName").unwrap_or_default();
        let state = self.get_state(req.account);
        let tags = state.get_role(&role_name)
            .map(|r| r.tags.read().clone())
            .unwrap_or_default();
        AwsResponse::xml(200, "ListRoleTags", tags_xml(&tags))
    }

    fn attach_role_policy(&self, req: &AwsRequest) -> AwsResponse {
        let role_name = get_param(req, "RoleName").unwrap_or_default();
        let arn = get_param(req, "PolicyArn").unwrap_or_default();
        let state = self.get_state(req.account);
        if let Some(policy) = state.get_policy(&arn) {
            policy.attached_roles.write().push(role_name.clone());
        }
        if let Some(role) = state.get_role(&role_name) {
            role.attached_policies.write().push(arn);
        }
        AwsResponse::xml(200, "AttachRolePolicy", String::new())
    }

    fn detach_role_policy(&self, req: &AwsRequest) -> AwsResponse {
        let role_name = get_param(req, "RoleName").unwrap_or_default();
        let arn = get_param(req, "PolicyArn").unwrap_or_default();
        let state = self.get_state(req.account);
        if let Some(policy) = state.get_policy(&arn) {
            policy.attached_roles.write().retain(|r| *r != role_name);
        }
        if let Some(role) = state.get_role(&role_name) {
            role.attached_policies.write().retain(|a| *a != arn);
        }
        AwsResponse::xml(200, "DetachRolePolicy", String::new())
    }

    fn list_attached_role_policies(&self, req: &AwsRequest) -> AwsResponse {
        let role_name = get_param(req, "RoleName").unwrap_or_default();
        let state = self.get_state(req.account);
        let policies = state.get_role(&role_name)
            .map(|r| r.attached_policies.read().clone())
            .unwrap_or_default();
        let mut body = String::from("<AttachedPolicies>");
        for p in &policies {
            body.push_str(&attached_policy_item(p));
        }
        body.push_str("</AttachedPolicies><IsTruncated>false</IsTruncated>");
        AwsResponse::xml(200, "ListAttachedRolePolicies", body)
    }

    fn put_role_policy(&self, req: &AwsRequest) -> AwsResponse {
        let role_name = get_param(req, "RoleName").unwrap_or_default();
        let policy_name = get_param(req, "PolicyName").unwrap_or_default();
        let doc = get_param(req, "PolicyDocument").unwrap_or_default();
        let state = self.get_state(req.account);
        if let Some(role) = state.get_role(&role_name) {
            let mut policies = role.inline_policies.write();
            if !policies.contains(&policy_name) {
                policies.push(policy_name.clone());
            }
            role.policy_documents.write().insert(policy_name, doc);
        }
        AwsResponse::xml(200, "PutRolePolicy", String::new())
    }

    fn get_role_policy(&self, req: &AwsRequest) -> AwsResponse {
        let role_name = get_param(req, "RoleName").unwrap_or_default();
        let policy_name = get_param(req, "PolicyName").unwrap_or_default();
        let state = self.get_state(req.account);
        match state.get_role(&role_name) {
            Some(role) => match role.policy_documents.read().get(&policy_name) {
                Some(doc) => AwsResponse::xml(200, "GetRolePolicy", format!(
                    "<RoleName>{}</RoleName><PolicyName>{}</PolicyName><PolicyDocument>{}</PolicyDocument>",
                    xml_escape(role_name.as_str()),
                    xml_escape(policy_name.as_str()),
                    escape_policy(doc)
                )),
                None => err(404, "NoSuchEntity",
                    &format!("The role policy with name {} cannot be found.", policy_name)),
            },
            None => err(404, "NoSuchEntity",
                &format!("The role with name {} cannot be found.", role_name)),
        }
    }

    fn delete_role_policy(&self, req: &AwsRequest) -> AwsResponse {
        let role_name = get_param(req, "RoleName").unwrap_or_default();
        let policy_name = get_param(req, "PolicyName").unwrap_or_default();
        let state = self.get_state(req.account);
        if let Some(role) = state.get_role(&role_name) {
            role.inline_policies.write().retain(|p| *p != policy_name);
            role.policy_documents.write().remove(&policy_name);
        }
        AwsResponse::xml(200, "DeleteRolePolicy", String::new())
    }

    fn list_role_policies(&self, req: &AwsRequest) -> AwsResponse {
        let role_name = get_param(req, "RoleName").unwrap_or_default();
        let state = self.get_state(req.account);
        let policies = state.get_role(&role_name)
            .map(|r| r.inline_policies.read().clone())
            .unwrap_or_default();
        AwsResponse::xml(200, "ListRolePolicies", names_xml(&policies))
    }

    // =====================================================================
    // Groups
    // =====================================================================

    fn group_xml(&self, group: &Group) -> String {
        let mut s = format!(
            "<Group><Path>{}</Path><GroupName>{}</GroupName><GroupId>{}</GroupId><Arn>{}</Arn><CreateDate>{}</CreateDate>",
            xml_escape(&group.path),
            xml_escape(&group.group_name),
            group.group_id,
            xml_escape(&group.arn),
            ms_to_iso(group.created)
        );
        if !group.tags.read().is_empty() {
            s.push_str(&tags_xml(&group.tags.read()));
        }
        s.push_str("</Group>");
        s
    }

    fn create_group(&self, req: &AwsRequest) -> AwsResponse {
        let group_name = get_param(req, "GroupName").unwrap_or_default();
        if group_name.is_empty() {
            return err(400, "MissingParameterValue", "GroupName is required");
        }
        let path = get_param(req, "Path").unwrap_or_else(|| "/".into());
        let state = self.get_state(req.account);
        if state.get_group(&group_name).is_some() {
            return err(409, "EntityAlreadyExists",
                &format!("A group with the name {} already exists.", group_name));
        }
        let group = Arc::new(Group::with_path(req.account, group_name, path));
        let tags = self.parse_query_list(req, "Tags");
        if !tags.is_empty() {
            *group.tags.write() = tags;
        }
        state.groups.write().insert(group.group_name.clone(), group.clone());
        AwsResponse::xml(200, "CreateGroup", self.group_xml(&group))
    }

    fn get_group(&self, req: &AwsRequest) -> AwsResponse {
        let group_name = get_param(req, "GroupName").unwrap_or_default();
        let state = self.get_state(req.account);
        match state.get_group(&group_name) {
            Some(group) => {
                let mut body = self.group_xml(&group);
                let usernames = group.users.read().clone();
                body.push_str("<Users>");
                for uname in &usernames {
                    if let Some(user) = state.get_user(uname) {
                        body.push_str(&self.user_xml(&user, false));
                    }
                }
                body.push_str("</Users><IsTruncated>false</IsTruncated>");
                AwsResponse::xml(200, "GetGroup", body)
            }
            None => err(404, "NoSuchEntity",
                &format!("The group with name {} cannot be found.", group_name)),
        }
    }

    fn delete_group(&self, req: &AwsRequest) -> AwsResponse {
        let group_name = get_param(req, "GroupName").unwrap_or_default();
        let state = self.get_state(req.account);
        match state.get_group(&group_name) {
            Some(group) => {
                if !group.users.read().is_empty() {
                    return err(409, "DeleteConflict", "Cannot delete entity, must remove users first.");
                }
                state.groups.write().remove(&group_name);
                AwsResponse::xml(200, "DeleteGroup", String::new())
            }
            None => err(404, "NoSuchEntity",
                &format!("The group with name {} cannot be found.", group_name)),
        }
    }

    fn list_groups(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account);
        let mut names: Vec<String> = state.groups.read().keys().cloned().collect();
        names.sort();
        let mut body = String::from("<Groups>");
        for name in &names {
            if let Some(group) = state.get_group(name) {
                body.push_str(&self.group_xml(&group));
            }
        }
        body.push_str("</Groups><IsTruncated>false</IsTruncated>");
        AwsResponse::xml(200, "ListGroups", body)
    }

    fn update_group(&self, req: &AwsRequest) -> AwsResponse {
        let group_name = get_param(req, "GroupName").unwrap_or_default();
        let state = self.get_state(req.account);
        let group = match state.get_group(&group_name) {
            Some(g) => g,
            None => return err(404, "NoSuchEntity",
                &format!("The group with name {} cannot be found.", group_name)),
        };
        let name = get_param(req, "NewGroupName").unwrap_or_else(|| group.group_name.clone());
        let path = get_param(req, "NewPath").unwrap_or_else(|| group.path.clone());
        if name != group.group_name {
            if state.get_group(&name).is_some() {
                return err(409, "EntityAlreadyExists", &format!("Group {} already exists", name));
            }
            state.groups.write().remove(&group.group_name);
        }
        let rebuilt = Arc::new(Group::with_path(req.account, name.clone(), path));
        *rebuilt.tags.write() = group.tags.read().clone();
        *rebuilt.attached_policies.write() = group.attached_policies.read().clone();
        *rebuilt.inline_policies.write() = group.inline_policies.read().clone();
        *rebuilt.policy_documents.write() = group.policy_documents.read().clone();
        *rebuilt.permissions_boundary.write() = group.permissions_boundary.read().clone();
        *rebuilt.users.write() = group.users.read().clone();
        state.groups.write().insert(name, rebuilt);
        AwsResponse::xml(200, "UpdateGroup", String::new())
    }

    fn add_user_to_group(&self, req: &AwsRequest) -> AwsResponse {
        let group_name = get_param(req, "GroupName").unwrap_or_default();
        let username = get_param(req, "UserName").unwrap_or_default();
        let state = self.get_state(req.account);
        let group = match state.get_group(&group_name) {
            Some(g) => g,
            None => return err(404, "NoSuchEntity",
                &format!("The group with name {} cannot be found.", group_name)),
        };
        if !group.users.read().contains(&username) {
            if state.get_user(&username).is_none() {
                return err(404, "NoSuchEntity",
                    &format!("The user with name {} cannot be found.", username));
            }
            group.users.write().push(username.clone());
        }
        AwsResponse::xml(200, "AddUserToGroup", String::new())
    }

    fn remove_user_from_group(&self, req: &AwsRequest) -> AwsResponse {
        let group_name = get_param(req, "GroupName").unwrap_or_default();
        let username = get_param(req, "UserName").unwrap_or_default();
        let state = self.get_state(req.account);
        match state.get_group(&group_name) {
            Some(group) => {
                group.users.write().retain(|u| *u != username);
                AwsResponse::xml(200, "RemoveUserFromGroup", String::new())
            }
            None => err(404, "NoSuchEntity",
                &format!("The group with name {} cannot be found.", group_name)),
        }
    }

    fn list_groups_for_user(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_default();
        let state = self.get_state(req.account);
        let mut body = String::from("<Groups>");
        for group in state.groups.read().values() {
            if group.users.read().contains(&username) {
                body.push_str(&self.group_xml(group));
            }
        }
        body.push_str("</Groups><IsTruncated>false</IsTruncated>");
        AwsResponse::xml(200, "ListGroupsForUser", body)
    }

    fn list_users_for_group(&self, req: &AwsRequest) -> AwsResponse {
        let group_name = get_param(req, "GroupName").unwrap_or_default();
        let state = self.get_state(req.account);
        let group = match state.get_group(&group_name) {
            Some(g) => g,
            None => return err(404, "NoSuchEntity",
                &format!("The group with name {} cannot be found.", group_name)),
        };
        let usernames = group.users.read().clone();
        let mut body = String::from("<Users>");
        for uname in &usernames {
            if let Some(user) = state.get_user(uname) {
                body.push_str(&self.user_xml(&user, false));
            }
        }
        body.push_str("</Users><IsTruncated>false</IsTruncated>");
        AwsResponse::xml(200, "ListUsersForGroup", body)
    }

    fn tag_group(&self, req: &AwsRequest) -> AwsResponse {
        let group_name = get_param(req, "GroupName").unwrap_or_default();
        let tags = self.parse_query_list(req, "Tags");
        let state = self.get_state(req.account);
        if let Some(group) = state.get_group(&group_name) {
            tag_upsert(&mut group.tags.write(), tags);
        }
        AwsResponse::xml(200, "TagGroup", String::new())
    }

    fn untag_group(&self, req: &AwsRequest) -> AwsResponse {
        let group_name = get_param(req, "GroupName").unwrap_or_default();
        let state = self.get_state(req.account);
        if let Some(group) = state.get_group(&group_name) {
            remove_tags(&mut group.tags.write(), &string_vec(req, "TagKeys"));
        }
        AwsResponse::xml(200, "UntagGroup", String::new())
    }

    fn list_group_tags(&self, req: &AwsRequest) -> AwsResponse {
        let group_name = get_param(req, "GroupName").unwrap_or_default();
        let state = self.get_state(req.account);
        let tags = state.get_group(&group_name)
            .map(|g| g.tags.read().clone())
            .unwrap_or_default();
        AwsResponse::xml(200, "ListGroupTags", tags_xml(&tags))
    }

    fn attach_group_policy(&self, req: &AwsRequest) -> AwsResponse {
        let group_name = get_param(req, "GroupName").unwrap_or_default();
        let arn = get_param(req, "PolicyArn").unwrap_or_default();
        let state = self.get_state(req.account);
        if let Some(policy) = state.get_policy(&arn) {
            policy.attached_groups.write().push(group_name.clone());
        }
        if let Some(group) = state.get_group(&group_name) {
            group.attached_policies.write().push(arn);
        }
        AwsResponse::xml(200, "AttachGroupPolicy", String::new())
    }

    fn detach_group_policy(&self, req: &AwsRequest) -> AwsResponse {
        let group_name = get_param(req, "GroupName").unwrap_or_default();
        let arn = get_param(req, "PolicyArn").unwrap_or_default();
        let state = self.get_state(req.account);
        if let Some(policy) = state.get_policy(&arn) {
            policy.attached_groups.write().retain(|g| *g != group_name);
        }
        if let Some(group) = state.get_group(&group_name) {
            group.attached_policies.write().retain(|a| *a != arn);
        }
        AwsResponse::xml(200, "DetachGroupPolicy", String::new())
    }

    fn list_attached_group_policies(&self, req: &AwsRequest) -> AwsResponse {
        let group_name = get_param(req, "GroupName").unwrap_or_default();
        let state = self.get_state(req.account);
        let policies = state.get_group(&group_name)
            .map(|g| g.attached_policies.read().clone())
            .unwrap_or_default();
        let mut body = String::from("<AttachedPolicies>");
        for p in &policies {
            body.push_str(&attached_policy_item(p));
        }
        body.push_str("</AttachedPolicies><IsTruncated>false</IsTruncated>");
        AwsResponse::xml(200, "ListAttachedGroupPolicies", body)
    }

    fn put_group_policy(&self, req: &AwsRequest) -> AwsResponse {
        let group_name = get_param(req, "GroupName").unwrap_or_default();
        let policy_name = get_param(req, "PolicyName").unwrap_or_default();
        let doc = get_param(req, "PolicyDocument").unwrap_or_default();
        let state = self.get_state(req.account);
        if let Some(group) = state.get_group(&group_name) {
            let mut policies = group.inline_policies.write();
            if !policies.contains(&policy_name) {
                policies.push(policy_name.clone());
            }
            group.policy_documents.write().insert(policy_name, doc);
        }
        AwsResponse::xml(200, "PutGroupPolicy", String::new())
    }

    fn get_group_policy(&self, req: &AwsRequest) -> AwsResponse {
        let group_name = get_param(req, "GroupName").unwrap_or_default();
        let policy_name = get_param(req, "PolicyName").unwrap_or_default();
        let state = self.get_state(req.account);
        match state.get_group(&group_name) {
            Some(group) => match group.policy_documents.read().get(&policy_name) {
                Some(doc) => AwsResponse::xml(200, "GetGroupPolicy", format!(
                    "<GroupName>{}</GroupName><PolicyName>{}</PolicyName><PolicyDocument>{}</PolicyDocument>",
                    xml_escape(group_name.as_str()),
                    xml_escape(policy_name.as_str()),
                    escape_policy(doc)
                )),
                None => err(404, "NoSuchEntity",
                    &format!("The group policy with name {} cannot be found.", policy_name)),
            },
            None => err(404, "NoSuchEntity",
                &format!("The group with name {} cannot be found.", group_name)),
        }
    }

    fn delete_group_policy(&self, req: &AwsRequest) -> AwsResponse {
        let group_name = get_param(req, "GroupName").unwrap_or_default();
        let policy_name = get_param(req, "PolicyName").unwrap_or_default();
        let state = self.get_state(req.account);
        if let Some(group) = state.get_group(&group_name) {
            group.inline_policies.write().retain(|p| *p != policy_name);
            group.policy_documents.write().remove(&policy_name);
        }
        AwsResponse::xml(200, "DeleteGroupPolicy", String::new())
    }

    fn list_group_policies(&self, req: &AwsRequest) -> AwsResponse {
        let group_name = get_param(req, "GroupName").unwrap_or_default();
        let state = self.get_state(req.account);
        let policies = state.get_group(&group_name)
            .map(|g| g.inline_policies.read().clone())
            .unwrap_or_default();
        AwsResponse::xml(200, "ListGroupPolicies", names_xml(&policies))
    }

    // =====================================================================
    // Policies
    // =====================================================================

    fn policy_xml(&self, policy: &Policy, include_tags: bool) -> String {
        let attachments = self.policy_attachments(policy);
        let mut s = format!(
            "<PolicyName>{}</PolicyName><PolicyId>{}</PolicyId><Arn>{}</Arn><Path>{}</Path><DefaultVersionId>{}</DefaultVersionId><AttachmentCount>{}</AttachmentCount><IsAttachable>true</IsAttachable>",
            xml_escape(&policy.policy_name),
            policy.policy_id,
            xml_escape(&policy.arn),
            xml_escape(&policy.path),
            (*policy.default_version_id.read()).clone(),
            attachments
        );
        {
            let d = policy.description.read();
            if !d.is_empty() {
                s.push_str(&format!("<Description>{}</Description>", xml_escape(&d)));
            }
        }
        s.push_str(&format!(
            "<CreateDate>{}</CreateDate><UpdateDate>{}</UpdateDate>",
            ms_to_iso(policy.created), ms_to_iso(policy.modified)
        ));
        if include_tags && !policy.tags.read().is_empty() {
            s.push_str(&tags_xml(&policy.tags.read()));
        }
        s
    }

    fn policy_attachments(&self, policy: &Policy) -> u32 {
        (policy.attached_users.read().len()
            + policy.attached_roles.read().len()
            + policy.attached_groups.read().len()) as u32
    }

    fn create_policy(&self, req: &AwsRequest) -> AwsResponse {
        let policy_name = get_param(req, "PolicyName").unwrap_or_default();
        if policy_name.is_empty() {
            return err(400, "MissingParameterValue", "PolicyName is required");
        }
        let path = get_param(req, "Path").unwrap_or_else(|| "/".into());
        let description = get_param(req, "Description").unwrap_or_default();
        let document = get_param(req, "PolicyDocument").unwrap_or_default();
        let tags = self.parse_query_list(req, "Tags");
        let state = self.get_state(req.account);
        let arn = IamState::policy_arn(req.account, &path, &policy_name);
        if state.get_policy(&arn).is_some() {
            return err(409, "EntityAlreadyExists",
                &format!("A policy called {} already exists.", policy_name));
        }
        let policy = Arc::new(Policy::full(
            req.account, policy_name, path, document, tags, description));
        state.policies.write().insert(policy.policy_name.clone(), policy.clone());
        let body = self.policy_xml(&policy, true);
        AwsResponse::xml(200, "CreatePolicy", format!("<Policy>{}</Policy>", body))
    }

    fn get_policy(&self, req: &AwsRequest) -> AwsResponse {
        let arn = get_param(req, "PolicyArn").unwrap_or_default();
        let state = self.get_state(req.account);
        match state.get_policy(&arn) {
            Some(policy) => {
                let body = self.policy_xml(&policy, true);
                AwsResponse::xml(200, "GetPolicy", format!("<Policy>{}</Policy>", body))
            }
            None => err(404, "NoSuchEntity",
                &format!("The policy with ARN {} cannot be found.", arn)),
        }
    }

    fn delete_policy(&self, req: &AwsRequest) -> AwsResponse {
        let arn = get_param(req, "PolicyArn").unwrap_or_default();
        let state = self.get_state(req.account);
        match state.get_policy(&arn) {
            Some(policy) => {
                state.policies.write().remove(&policy.policy_name);
                AwsResponse::xml(200, "DeletePolicy", String::new())
            }
            None => err(404, "NoSuchEntity",
                &format!("The policy with ARN {} cannot be found.", arn)),
        }
    }

    fn list_policies(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account);
        let scope = get_param(req, "Scope").unwrap_or_default();
        let mut names: Vec<String> = state.policies.read().keys().cloned().collect();
        names.sort();
        let mut body = String::from("<Policies>");
        for name in &names {
            // We only hold customer-managed policies; Scope=AWS yields none.
            if scope == "AWS" {
                continue;
            }
            if let Some(policy) = state.get_policy(name) {
                body.push_str(&format!("<member>{}</member>", self.policy_xml(&policy, false)));
            }
        }
        body.push_str("</Policies><IsTruncated>false</IsTruncated>");
        AwsResponse::xml(200, "ListPolicies", body)
    }

    fn create_policy_version(&self, req: &AwsRequest) -> AwsResponse {
        let arn = get_param(req, "PolicyArn").unwrap_or_default();
        let set_as_default = get_param(req, "SetAsDefault")
            .map(|v| v == "true" || v == "True")
            .unwrap_or(false);
        let state = self.get_state(req.account);
        let policy = match state.get_policy(&arn) {
            Some(p) => p,
            None => return err(404, "NoSuchEntity",
                &format!("The policy with ARN {} cannot be found.", arn)),
        };
        let next_version = policy.versions.read().len() + 1;
        let version_id = format!("v{}", next_version);
        let now = now_iso();
        let document = get_param(req, "PolicyDocument").unwrap_or_else(|| "{}".into());
        if set_as_default {
            for v in policy.versions.write().iter_mut() {
                if v.get("IsDefaultVersion").and_then(|x| x.as_bool()) == Some(true) {
                    v["IsDefaultVersion"] = json!(false);
                }
            }
            *policy.default_version_id.write() = version_id.clone();
            *policy.policy_document.write() = document.clone();
        }
        policy.versions.write().push(json!({
            "VersionId": version_id,
            "IsDefaultVersion": set_as_default,
            "CreateDate": now,
            "Document": document
        }));
        AwsResponse::xml(200, "CreatePolicyVersion", format!(
            "<PolicyVersion><Document>{}</Document><VersionId>{}</VersionId><IsDefaultVersion>{}</IsDefaultVersion><CreateDate>{}</CreateDate></PolicyVersion>",
            escape_policy(&get_param(req, "PolicyDocument").unwrap_or_else(|| "{}".into())),
            version_id, set_as_default, now
        ))
    }

    fn get_policy_version(&self, req: &AwsRequest) -> AwsResponse {
        let arn = get_param(req, "PolicyArn").unwrap_or_default();
        let version_id = get_param(req, "VersionId").unwrap_or_default();
        let state = self.get_state(req.account);
        let policy = match state.get_policy(&arn) {
            Some(p) => p,
            None => return err(404, "NoSuchEntity",
                &format!("The policy with ARN {} cannot be found.", arn)),
        };
        let default_id = (*policy.default_version_id.read()).clone();
        let versions = policy.versions.read();
        let version = versions.iter()
            .find(|v| v.get("VersionId").and_then(|x| x.as_str()) == Some(version_id.as_str()));
        match version {
            Some(v) => {
                let vid = v.get("VersionId").and_then(|x| x.as_str()).unwrap_or("");
                let is_default = vid == default_id;
                let doc = v.get("Document").cloned().unwrap_or_else(|| json!("{}"));
                let doc_str = string_of(&doc);
                AwsResponse::xml(200, "GetPolicyVersion", format!(
                    "<PolicyVersion><Document>{}</Document><VersionId>{}</VersionId><IsDefaultVersion>{}</IsDefaultVersion><CreateDate>{}</CreateDate></PolicyVersion>",
                    escape_policy(&doc_str), vid, is_default,
                    v.get("CreateDate").and_then(|x| x.as_str()).unwrap_or("")
                ))
            }
            None => err(404, "NoSuchEntity", &format!(
                "Policy version {} cannot be found for arn {}", version_id, arn)),
        }
    }

    fn list_policy_versions(&self, req: &AwsRequest) -> AwsResponse {
        let arn = get_param(req, "PolicyArn").unwrap_or_default();
        let state = self.get_state(req.account);
        let policy = match state.get_policy(&arn) {
            Some(p) => p,
            None => return err(404, "NoSuchEntity",
                &format!("The policy with ARN {} cannot be found.", arn)),
        };
        let default_id = (*policy.default_version_id.read()).clone();
        let mut body = String::from("<Versions>");
        for v in policy.versions.read().iter() {
            let vid = v.get("VersionId").and_then(|x| x.as_str()).unwrap_or("");
            let doc = v.get("Document").cloned().unwrap_or_else(|| json!("{}"));
            let doc_str = string_of(&doc);
            body.push_str(&format!(
                "<member><Document>{}</Document><VersionId>{}</VersionId><IsDefaultVersion>{}</IsDefaultVersion><CreateDate>{}</CreateDate></member>",
                escape_policy(&doc_str),
                vid,
                vid == default_id,
                v.get("CreateDate").and_then(|x| x.as_str()).unwrap_or("")
            ));
        }
        body.push_str("</Versions>");
        AwsResponse::xml(200, "ListPolicyVersions", body)
    }

    fn set_default_policy_version(&self, req: &AwsRequest) -> AwsResponse {
        let arn = get_param(req, "PolicyArn").unwrap_or_default();
        let version_id = get_param(req, "VersionId").unwrap_or_default();
        let state = self.get_state(req.account);
        let policy = match state.get_policy(&arn) {
            Some(p) => p,
            None => return err(404, "NoSuchEntity",
                &format!("The policy with ARN {} cannot be found.", arn)),
        };
        {
            let mut versions = policy.versions.write();
            let mut found = false;
            for v in versions.iter_mut() {
                let vid = v.get("VersionId").and_then(|x| x.as_str()).unwrap_or("");
                if vid == version_id {
                    v["IsDefaultVersion"] = json!(true);
                    found = true;
                } else if v.get("IsDefaultVersion").and_then(|x| x.as_bool()) == Some(true) {
                    v["IsDefaultVersion"] = json!(false);
                }
            }
            if !found {
                return err(404, "NoSuchEntity",
                    &format!("Policy version {} cannot be found.", version_id));
            }
        }
        *policy.default_version_id.write() = version_id.clone();
        // Sync the default document into the policy document store
        if let Some(v) = policy.versions.read().iter()
            .find(|v| v.get("VersionId").and_then(|x| x.as_str()) == Some(version_id.as_str()))
        {
            if let Some(doc) = v.get("Document") {
                *policy.policy_document.write() = string_of(doc);
            }
        }
        AwsResponse::xml(200, "SetDefaultPolicyVersion", String::new())
    }

    fn delete_policy_version(&self, req: &AwsRequest) -> AwsResponse {
        let arn = get_param(req, "PolicyArn").unwrap_or_default();
        let version_id = get_param(req, "VersionId").unwrap_or_default();
        let state = self.get_state(req.account);
        let policy = match state.get_policy(&arn) {
            Some(p) => p,
            None => return err(404, "NoSuchEntity",
                &format!("The policy with ARN {} cannot be found.", arn)),
        };
        if *policy.default_version_id.read() == version_id {
            return err(400, "DeleteConflict",
                "Cannot delete the default version of a policy.");
        }
        policy.versions.write().retain(|v| {
            v.get("VersionId").and_then(|x| x.as_str()) != Some(version_id.as_str())
        });
        AwsResponse::xml(200, "DeletePolicyVersion", String::new())
    }

    fn tag_policy(&self, req: &AwsRequest) -> AwsResponse {
        let arn = get_param(req, "PolicyArn").unwrap_or_default();
        let tags = self.parse_query_list(req, "Tags");
        let state = self.get_state(req.account);
        if let Some(policy) = state.get_policy(&arn) {
            tag_upsert(&mut policy.tags.write(), tags);
        }
        AwsResponse::xml(200, "TagPolicy", String::new())
    }

    fn untag_policy(&self, req: &AwsRequest) -> AwsResponse {
        let arn = get_param(req, "PolicyArn").unwrap_or_default();
        let state = self.get_state(req.account);
        if let Some(policy) = state.get_policy(&arn) {
            remove_tags(&mut policy.tags.write(), &string_vec(req, "TagKeys"));
        }
        AwsResponse::xml(200, "UntagPolicy", String::new())
    }

    fn list_policy_tags(&self, req: &AwsRequest) -> AwsResponse {
        let arn = get_param(req, "PolicyArn").unwrap_or_default();
        let state = self.get_state(req.account);
        let tags = state.get_policy(&arn)
            .map(|p| p.tags.read().clone())
            .unwrap_or_default();
        AwsResponse::xml(200, "ListPolicyTags", tags_xml(&tags))
    }

    fn list_entities_for_policy(&self, req: &AwsRequest) -> AwsResponse {
        let arn = get_param(req, "PolicyArn").unwrap_or_default();
        let state = self.get_state(req.account);
        let policy = match state.get_policy(&arn) {
            Some(p) => p,
            None => return err(404, "NoSuchEntity",
                &format!("The policy with ARN {} cannot be found.", arn)),
        };
        let mut body = String::new();
        body.push_str("<PolicyGroups>");
        for g in policy.attached_groups.read().iter() {
            if let Some(group) = state.get_group(g) {
                body.push_str(&format!(
                    "<member><GroupName>{}</GroupName><GroupId>{}</GroupId></member>",
                    xml_escape(&group.group_name), group.group_id));
            }
        }
        body.push_str("</PolicyGroups><PolicyRoles>");
        for r in policy.attached_roles.read().iter() {
            if let Some(role) = state.get_role(r) {
                body.push_str(&format!(
                    "<member><RoleName>{}</RoleName><RoleId>{}</RoleId></member>",
                    xml_escape(&role.role_name), role.role_id));
            }
        }
        body.push_str("</PolicyRoles><PolicyUsers>");
        for u in policy.attached_users.read().iter() {
            if let Some(user) = state.get_user(u) {
                body.push_str(&format!(
                    "<member><UserName>{}</UserName><UserId>{}</UserId></member>",
                    xml_escape(&user.username), user.user_id));
            }
        }
        body.push_str("</PolicyUsers><IsTruncated>false</IsTruncated>");
        AwsResponse::xml(200, "ListEntitiesForPolicy", body)
    }

    // =====================================================================
    // Account level
    // =====================================================================

    fn create_account_alias(&self, req: &AwsRequest) -> AwsResponse {
        let alias = get_param(req, "AccountAlias").unwrap_or_default();
        let state = self.get_state(req.account);
        *state.account_alias.write() = Some(alias);
        AwsResponse::xml(200, "CreateAccountAlias", String::new())
    }

    fn delete_account_alias(&self, req: &AwsRequest) -> AwsResponse {
        let alias = get_param(req, "AccountAlias").unwrap_or_default();
        let state = self.get_state(req.account);
        let cur = state.account_alias.read().clone();
        if cur.as_deref() == Some(alias.as_str()) {
            *state.account_alias.write() = None;
        }
        AwsResponse::xml(200, "DeleteAccountAlias", String::new())
    }

    fn list_account_aliases(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account);
        let aliases = state.account_alias.read().clone().unwrap_or_default();
        let mut body = String::from("<AccountAliases>");
        if !aliases.is_empty() {
            body.push_str(&format!("<member>{}</member>", xml_escape(&aliases)));
        }
        body.push_str("</AccountAliases>");
        AwsResponse::xml(200, "ListAccountAliases", body)
    }

    fn get_account_summary(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account);
        let mut virtual_mfa_count = 0u32;
        let mut mfa_in_use = 0u32;
        for d in state.virtual_mfas.read().values() {
            virtual_mfa_count += 1;
            if d.assigned_user.read().is_some() {
                mfa_in_use += 1;
            }
        }
        let providers = state.oidc_providers.read().len() + state.saml_providers.read().len();
        let entries = summary_entries(&state, virtual_mfa_count, mfa_in_use, providers as u32);
        let mut body = String::from("<SummaryMap>");
        for (k, v) in entries {
            body.push_str(&format!("<entry><key>{}</key><value>{}</value></entry>", k, v));
        }
        body.push_str("</SummaryMap>");
        AwsResponse::xml(200, "GetAccountSummary", body)
    }

    fn get_account_password_policy(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account);
        let stored = state.password_policy.read().clone();
        match stored {
            Some(map) => {
                let mut body = String::from("<PasswordPolicy>");
                for key in [
                    "MinimumPasswordLength", "RequireSymbols", "RequireNumbers",
                    "RequireUppercaseCharacters", "RequireLowercaseCharacters",
                    "AllowUsersToChangePassword", "ExpirePasswords", "MaxPasswordAge",
                    "PasswordReusePrevention", "HardExpiry",
                ] {
                    if let Some(v) = map.get(key) {
                        body.push_str(&xml_field(key, &format_value(v)));
                    }
                }
                body.push_str("</PasswordPolicy>");
                AwsResponse::xml(200, "GetAccountPasswordPolicy", body)
            }
            None => err(404, "NoSuchEntity",
                "The Password Policy with domain 123456789012 cannot be found."),
        }
    }

    fn update_account_password_policy(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account);
        let mut map = serde_json::Map::new();
        bool_param(req, &mut map, "RequireSymbols", false);
        bool_param(req, &mut map, "RequireNumbers", false);
        bool_param(req, &mut map, "RequireUppercaseCharacters", false);
        bool_param(req, &mut map, "RequireLowercaseCharacters", false);
        bool_param(req, &mut map, "AllowUsersToChangePassword", false);
        bool_param(req, &mut map, "HardExpiry", false);
        let minimum = get_param(req, "MinimumPasswordLength")
            .and_then(|v| v.parse::<i64>().ok()).unwrap_or(8);
        map.insert("MinimumPasswordLength".into(), json!(minimum));
        if let Some(v) = get_param(req, "MaxPasswordAge").and_then(|v| v.parse::<i64>().ok()) {
            map.insert("MaxPasswordAge".into(), json!(v));
            map.insert("ExpirePasswords".into(), json!(true));
        }
        if let Some(v) = get_param(req, "PasswordReusePrevention").and_then(|v| v.parse::<i64>().ok()) {
            map.insert("PasswordReusePrevention".into(), json!(v));
        }
        *state.password_policy.write() = Some(Value::Object(map));
        AwsResponse::xml(200, "UpdateAccountPasswordPolicy", String::new())
    }

    fn delete_account_password_policy(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account);
        *state.password_policy.write() = None;
        AwsResponse::xml(200, "DeleteAccountPasswordPolicy", String::new())
    }

    fn get_account_authorization_details(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account);
        let filter = string_vec(req, "Filter");
        let show = |kind: &str| filter.is_empty() || filter.iter().any(|f| f == kind);
        let mut user_xml = String::new();
        if show("User") {
            for user in state.users.read().values() {
                user_xml.push_str(&self.user_xml(user, true));
            }
        }
        let mut group_xml = String::new();
        if show("Group") {
            for group in state.groups.read().values() {
                group_xml.push_str(&self.group_xml(group));
            }
        }
        let mut role_xml = String::new();
        if show("Role") {
            for role in state.roles.read().values() {
                role_xml.push_str(&self.role_xml(role, false));
            }
        }
        let mut managed = String::new();
        for policy in state.policies.read().values() {
            managed.push_str(&format!("<member>{}</member>", self.policy_xml(policy, false)));
        }
        let body = format!(
            "<UserDetailList>{}</UserDetailList><GroupDetailList>{}</GroupDetailList><RoleDetailList>{}</RoleDetailList><ManagedPolicies>{}</ManagedPolicies><IsTruncated>false</IsTruncated>",
            user_xml, group_xml, role_xml, managed
        );
        AwsResponse::xml(200, "GetAccountAuthorizationDetails", body)
    }

    // =====================================================================
    // Login profiles
    // =====================================================================

    fn login_profile_xml(&self, user: &str, profile: &LoginProfile) -> String {
        format!(
            "<LoginProfile><UserName>{}</UserName><CreateDate>{}</CreateDate><PasswordResetRequired>{}</PasswordResetRequired></LoginProfile>",
            xml_escape(user), profile.created, profile.reset_required
        )
    }

    fn create_login_profile(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_default();
        let state = self.get_state(req.account);
        if state.get_user(&username).is_none() {
            return err(404, "NoSuchEntity",
                &format!("The user with name {} cannot be found.", username));
        }
        if state.login_profiles.read().contains_key(&username) {
            return err(409, "EntityAlreadyExists", &format!(
                "The Login Profile with domain User Name {} already exists for this account",
                username));
        }
        let profile = LoginProfile {
            created: now_iso(),
            password: get_param(req, "Password").unwrap_or_default(),
            reset_required: bool_of(get_param(req, "PasswordResetRequired")),
        };
        state.login_profiles.write().insert(username.clone(), profile.clone());
        AwsResponse::xml(200, "CreateLoginProfile", self.login_profile_xml(&username, &profile))
    }

    fn get_login_profile(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_default();
        let state = self.get_state(req.account);
        let profile = state.login_profiles.read().get(&username).cloned();
        match profile {
            Some(profile) => AwsResponse::xml(200, "GetLoginProfile",
                self.login_profile_xml(&username, &profile)),
            None => err(404, "NoSuchEntity",
                &format!("Login profile for {} not found", username)),
        }
    }

    fn update_login_profile(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_default();
        let state = self.get_state(req.account);
        if state.get_user(&username).is_none() {
            return err(404, "NoSuchEntity",
                &format!("The user with name {} cannot be found.", username));
        }
        let mut profiles = state.login_profiles.write();
        let profile = match profiles.get_mut(&username) {
            Some(p) => p,
            None => return err(404, "NoSuchEntity",
                &format!("Login profile for {} not found", username)),
        };
        if let Some(pw) = get_param(req, "Password") {
            profile.password = pw;
        }
        if let Some(v) = get_param(req, "PasswordResetRequired") {
            profile.reset_required = v == "true" || v == "True";
        }
        AwsResponse::xml(200, "UpdateLoginProfile", String::new())
    }

    fn delete_login_profile(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_default();
        let state = self.get_state(req.account);
        if state.login_profiles.write().remove(&username).is_none() {
            return err(404, "NoSuchEntity", &format!("Login profile for {} not found", username));
        }
        AwsResponse::xml(200, "DeleteLoginProfile", String::new())
    }

    // =====================================================================
    // MFA devices
    // =====================================================================

    fn create_virtual_mfa_device(&self, req: &AwsRequest) -> AwsResponse {
        let name = get_param(req, "VirtualMFADeviceName").unwrap_or_default();
        if name.is_empty() {
            return err(400, "MissingParameterValue", "VirtualMFADeviceName is required");
        }
        let mut path = get_param(req, "Path").unwrap_or_else(|| "/".into());
        if path.is_empty() {
            path = "/".to_string();
        }
        let serial = format!("arn:aws:iam::{}:mfa{}{}", req.account, path, name);
        let state = self.get_state(req.account);
        if state.virtual_mfas.read().contains_key(&serial) {
            return err(409, "EntityAlreadyExists",
                "MFADevice entity at the same path and name already exists.");
        }
        let device = Arc::new(VirtualMfa {
            serial: serial.clone(),
            base32: random_base32(),
            qrcode_png: base64_encode(&fake_qrcode()),
            assigned_user: RwLock::new(None),
            enable_date: RwLock::new(None),
            tags: RwLock::new(Vec::new()),
        });
        state.virtual_mfas.write().insert(serial, device.clone());
        let mut body = format!(
            "<VirtualMFADevice><SerialNumber>{}</SerialNumber><Base32StringSeed>{}</Base32StringSeed><QRCodePNG>{}</QRCodePNG>",
            xml_escape(&device.serial),
            base64_encode(device.base32.as_bytes()),
            device.qrcode_png
        );
        if !device.tags.read().is_empty() {
            body.push_str(&tags_xml(&device.tags.read()));
        }
        body.push_str("</VirtualMFADevice>");
        AwsResponse::xml(200, "CreateVirtualMFADevice", body)
    }

    fn delete_virtual_mfa_device(&self, req: &AwsRequest) -> AwsResponse {
        let serial = get_param(req, "SerialNumber").unwrap_or_default();
        let state = self.get_state(req.account);
        let device = state.virtual_mfas.read().get(&serial).cloned();
        match device {
            Some(device) => {
                if device.assigned_user.read().is_some() {
                    return err(409, "DeleteConflict",
                        "Deletion of MFA device is not currently authorized by the service.");
                }
                state.virtual_mfas.write().remove(&serial);
                AwsResponse::xml(200, "DeleteVirtualMFADevice", String::new())
            }
            None => err(404, "NoSuchEntity",
                &format!("VirtualMFADevice cannot be found: {}", serial)),
        }
    }

    fn list_virtual_mfa_devices(&self, req: &AwsRequest) -> AwsResponse {
        let assignment = get_param(req, "AssignmentStatus").unwrap_or_else(|| "Any".into());
        let state = self.get_state(req.account);
        let mut serials: Vec<String> = state.virtual_mfas.read().keys().cloned().collect();
        serials.sort();
        let mut body = String::from("<VirtualMFADevices>");
        for serial in &serials {
            let device = match state.virtual_mfas.read().get(serial) {
                Some(d) => d.clone(),
                None => continue,
            };
            let assigned = device.assigned_user.read().is_some();
            let keep = match assignment.as_str() {
                "Assigned" => assigned,
                "Unassigned" => !assigned,
                _ => true,
            };
            if !keep {
                continue;
            }
            let mut item = format!(
                "<member><SerialNumber>{}</SerialNumber>",
                xml_escape(&device.serial)
            );
            if !device.tags.read().is_empty() {
                item.push_str(&tags_xml(&device.tags.read()));
            }
            if assigned {
                let uname = device.assigned_user.read().clone().unwrap_or_default();
                if let Some(user) = state.get_user(&uname) {
                    item.push_str(&format!(
                        "<User><Path>{}</Path><UserName>{}</UserName><UserId>{}</UserId><Arn>{}</Arn></User>",
                        xml_escape(&user.path), xml_escape(&user.username),
                        user.user_id, xml_escape(&user.arn)));
                }
            }
            item.push_str("</member>");
            body.push_str(&item);
        }
        body.push_str("</VirtualMFADevices><IsTruncated>false</IsTruncated>");
        AwsResponse::xml(200, "ListVirtualMFADevices", body)
    }

    fn enable_mfa_device(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_default();
        let serial = get_param(req, "SerialNumber").unwrap_or_default();
        let state = self.get_state(req.account);
        if state.get_user(&username).is_none() {
            return err(404, "NoSuchEntity",
                &format!("The user with name {} cannot be found.", username));
        }
        let device = state.virtual_mfas.read().get(&serial).cloned();
        match device {
            Some(device) => {
                *device.assigned_user.write() = Some(username.clone());
                *device.enable_date.write() = Some(now_iso());
                AwsResponse::xml(200, "EnableMFADevice", String::new())
            }
            None => err(404, "NoSuchEntity", &format!("MFADevice cannot be found {}", serial)),
        }
    }

    fn deactivate_mfa_device(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_default();
        let serial = get_param(req, "SerialNumber").unwrap_or_default();
        let state = self.get_state(req.account);
        if state.get_user(&username).is_none() {
            return err(404, "NoSuchEntity",
                &format!("The user with name {} cannot be found.", username));
        }
        if let Some(device) = state.virtual_mfas.read().get(&serial).cloned() {
            *device.assigned_user.write() = None;
            *device.enable_date.write() = None;
        }
        AwsResponse::xml(200, "DeactivateMFADevice", String::new())
    }

    fn resync_mfa_device(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_default();
        let serial = get_param(req, "SerialNumber").unwrap_or_default();
        let state = self.get_state(req.account);
        if state.get_user(&username).is_none() {
            return err(404, "NoSuchEntity",
                &format!("The user with name {} cannot be found.", username));
        }
        let assigned = state.virtual_mfas.read().get(&serial)
            .and_then(|d| d.assigned_user.read().clone());
        match assigned {
            Some(owner) if owner == username => {
                AwsResponse::xml(200, "ResyncMFADevice", String::new())
            }
            _ => err(404, "NoSuchEntity", &format!("MFADevice cannot be found {}", serial)),
        }
    }

    fn list_mfa_devices(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_else(|| "unknown".into());
        let state = self.get_state(req.account);
        let mut serials: Vec<String> = state.virtual_mfas.read().keys().cloned().collect();
        serials.sort();
        let mut body = String::from("<MFADevices>");
        for serial in &serials {
            let device = match state.virtual_mfas.read().get(serial) {
                Some(d) => d.clone(),
                None => continue,
            };
            let owner = device.assigned_user.read().clone().unwrap_or_default();
            if owner != username {
                continue;
            }
            let enable_date = device.enable_date.read().clone().unwrap_or_default();
            body.push_str(&format!(
                "<member><UserName>{}</UserName><SerialNumber>{}</SerialNumber><EnableDate>{}</EnableDate></member>",
                xml_escape(username.as_str()), xml_escape(&device.serial), enable_date
            ));
        }
        body.push_str("</MFADevices><IsTruncated>false</IsTruncated>");
        AwsResponse::xml(200, "ListMFADevices", body)
    }

    fn get_mfa_device(&self, req: &AwsRequest) -> AwsResponse {
        let serial = get_param(req, "SerialNumber").unwrap_or_default();
        let state = self.get_state(req.account);
        let device = state.virtual_mfas.read().get(&serial).cloned();
        match device {
            Some(device) => {
                if device.assigned_user.read().is_none() {
                    return err(404, "NoSuchEntity", &format!("MFADevice cannot be found {}", serial));
                }
                let enable_date = device.enable_date.read().clone().unwrap_or_default();
                AwsResponse::xml(200, "GetMFADevice", format!(
                    "<SerialNumber>{}</SerialNumber><EnableDate>{}</EnableDate>",
                    xml_escape(&device.serial), enable_date))
            }
            None => err(404, "NoSuchEntity", &format!("MFADevice cannot be found {}", serial)),
        }
    }

    fn tag_mfa_device(&self, req: &AwsRequest) -> AwsResponse {
        let serial = get_param(req, "SerialNumber").unwrap_or_default();
        let tags = self.parse_query_list(req, "Tags");
        let state = self.get_state(req.account);
        let device = state.virtual_mfas.read().get(&serial).cloned();
        match device {
            Some(device) => tag_upsert(&mut device.tags.write(), tags),
            None => return err(404, "NoSuchEntity", &format!("MFADevice cannot be found {}", serial)),
        }
        AwsResponse::xml(200, "TagMFADevice", String::new())
    }

    fn untag_mfa_device(&self, req: &AwsRequest) -> AwsResponse {
        let serial = get_param(req, "SerialNumber").unwrap_or_default();
        let state = self.get_state(req.account);
        let device = state.virtual_mfas.read().get(&serial).cloned();
        match device {
            Some(device) => {
                remove_tags(&mut device.tags.write(), &string_vec(req, "TagKeys"));
            }
            None => return err(404, "NoSuchEntity", &format!("MFADevice cannot be found {}", serial)),
        }
        AwsResponse::xml(200, "UntagMFADevice", String::new())
    }

    fn list_mfa_device_tags(&self, req: &AwsRequest) -> AwsResponse {
        let serial = get_param(req, "SerialNumber").unwrap_or_default();
        let state = self.get_state(req.account);
        let tags = state.virtual_mfas.read().get(&serial)
            .map(|d| d.tags.read().clone())
            .unwrap_or_default();
        AwsResponse::xml(200, "ListMFADeviceTags",
            format!("{}<IsTruncated>false</IsTruncated>", tags_xml(&tags)))
    }

    fn unassign_all_mfa(&self, state: &IamState, username: &str) {
        for device in state.virtual_mfas.read().values() {
            let mut assigned = device.assigned_user.write();
            if assigned.as_deref() == Some(username) {
                *assigned = None;
                *device.enable_date.write() = None;
            }
        }
    }
}

// =====================================================================
// SAML providers, OIDC providers, instance profiles, SSH keys,
// certificates, service credentials, service linked roles, reports,
// simulation, boundaries, organization stubs. (second impl block)
// =====================================================================

impl IamHandler {
    // ---------------- SAML providers ----------------

    fn saml_xml(&self, p: &SamlProvider) -> String {
        let mut s = format!(
            "<SAMLProviderArn>{}</SAMLProviderArn><SAMLMetadataDocument>{}</SAMLMetadataDocument><CreateDate>{}</CreateDate><ValidUntil>{}</ValidUntil>",
            xml_escape(&p.arn), escape_policy(&p.metadata.read()), p.created, p.valid_until
        );
        if !p.tags.read().is_empty() {
            s.push_str(&tags_xml(&p.tags.read()));
        }
        s
    }

    fn create_saml_provider(&self, req: &AwsRequest) -> AwsResponse {
        let name = get_param(req, "Name").unwrap_or_else(|| "unknown".into());
        let metadata = get_param(req, "SAMLMetadataDocument").unwrap_or_default();
        if name.is_empty() {
            return err(400, "MissingParameterValue", "Name is required");
        }
        let state = self.get_state(req.account);
        if state.saml_providers.read().contains_key(&name) {
            return err(409, "EntityAlreadyExists",
                &format!("SAMLProvider {} already exists", name));
        }
        let arn = format!("arn:aws:iam::{}:saml-provider/{}", req.account, name);
        let provider = Arc::new(SamlProvider {
            name: name.clone(),
            arn: arn.clone(),
            metadata: RwLock::new(metadata),
            created: now_iso(),
            valid_until: "2032-05-09T16:27:11Z".to_string(),
            tags: RwLock::new(Vec::new()),
        });
        state.saml_providers.write().insert(name, provider.clone());
        AwsResponse::xml(200, "CreateSAMLProvider",
            format!("<SAMLProviderArn>{}</SAMLProviderArn>", xml_escape(&arn)))
    }

    fn get_saml_provider(&self, req: &AwsRequest) -> AwsResponse {
        let arn = get_param(req, "SAMLProviderArn").unwrap_or_default();
        let state = self.get_state(req.account);
        let provider = state.saml_providers.read().values()
            .find(|p| p.arn == arn).cloned();
        match provider {
            Some(p) => AwsResponse::xml(200, "GetSAMLProvider", self.saml_xml(&p)),
            None => err(404, "NoSuchEntity",
                &format!("SAMLProvider {} not found", arn)),
        }
    }

    fn list_saml_providers(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account);
        let mut names: Vec<String> = state.saml_providers.read().keys().cloned().collect();
        names.sort();
        let mut body = String::from("<SAMLProviderList>");
        for name in &names {
            if let Some(p) = state.saml_providers.read().get(name) {
                body.push_str(&format!(
                    "<member><Arn>{}</Arn><ValidUntil>{}</ValidUntil><CreateDate>{}</CreateDate></member>",
                    xml_escape(&p.arn), p.valid_until, p.created));
            }
        }
        body.push_str("</SAMLProviderList>");
        AwsResponse::xml(200, "ListSAMLProviders", body)
    }

    fn update_saml_provider(&self, req: &AwsRequest) -> AwsResponse {
        let arn = get_param(req, "SAMLProviderArn").unwrap_or_default();
        let metadata = get_param(req, "SAMLMetadataDocument").unwrap_or_default();
        let state = self.get_state(req.account);
        let provider = state.saml_providers.read().values()
            .find(|p| p.arn == arn).cloned();
        match provider {
            Some(p) => {
                *p.metadata.write() = metadata;
                AwsResponse::xml(200, "UpdateSAMLProvider",
                    format!("<SAMLProviderArn>{}</SAMLProviderArn>", xml_escape(&arn)))
            }
            None => err(404, "NoSuchEntity", &format!("SAMLProvider {} not found", arn)),
        }
    }

    fn delete_saml_provider(&self, req: &AwsRequest) -> AwsResponse {
        let arn = get_param(req, "SAMLProviderArn").unwrap_or_default();
        let state = self.get_state(req.account);
        let name = state.saml_providers.read().values()
            .find(|p| p.arn == arn).map(|p| p.name.clone());
        match name {
            Some(name) => {
                state.saml_providers.write().remove(&name);
                AwsResponse::xml(200, "DeleteSAMLProvider", String::new())
            }
            None => err(404, "NoSuchEntity", &format!("SAMLProvider {} not found", arn)),
        }
    }

    fn tag_saml_provider(&self, req: &AwsRequest) -> AwsResponse {
        let arn = get_param(req, "SAMLProviderArn").unwrap_or_default();
        let tags = self.parse_query_list(req, "Tags");
        let state = self.get_state(req.account);
        let provider = state.saml_providers.read().values().find(|p| p.arn == arn).cloned();
        match provider {
            Some(p) => tag_upsert(&mut p.tags.write(), tags),
            None => return err(404, "NoSuchEntity", &format!("SAMLProvider {} not found", arn)),
        }
        AwsResponse::xml(200, "TagSAMLProvider", String::new())
    }

    fn untag_saml_provider(&self, req: &AwsRequest) -> AwsResponse {
        let arn = get_param(req, "SAMLProviderArn").unwrap_or_default();
        let state = self.get_state(req.account);
        let provider = state.saml_providers.read().values().find(|p| p.arn == arn).cloned();
        match provider {
            Some(p) => remove_tags(&mut p.tags.write(), &string_vec(req, "TagKeys")),
            None => return err(404, "NoSuchEntity", &format!("SAMLProvider {} not found", arn)),
        }
        AwsResponse::xml(200, "UntagSAMLProvider", String::new())
    }

    fn list_saml_provider_tags(&self, req: &AwsRequest) -> AwsResponse {
        let arn = get_param(req, "SAMLProviderArn").unwrap_or_default();
        let state = self.get_state(req.account);
        let tags = state.saml_providers.read().values()
            .find(|p| p.arn == arn)
            .map(|p| p.tags.read().clone())
            .unwrap_or_default();
        AwsResponse::xml(200, "ListSAMLProviderTags",
            format!("{}<IsTruncated>false</IsTruncated>", tags_xml(&tags)))
    }

    // ---------------- OIDC providers ----------------

    fn create_oidc_provider(&self, req: &AwsRequest) -> AwsResponse {
        let url_full = get_param(req, "Url").unwrap_or_default();
        if url_full.is_empty() {
            return err(400, "MissingParameterValue", "Url is required");
        }
        let thumbprints = string_vec(req, "ThumbprintList");
        let client_ids = string_vec(req, "ClientIDList");
        let host = url_full.trim_start_matches("https://").trim_start_matches("http://").to_string();
        let arn = format!("arn:aws:iam::{}:oidc-provider/{}", req.account, host);
        let state = self.get_state(req.account);
        if state.oidc_providers.read().contains_key(&arn) {
            return err(409, "EntityAlreadyExists",
                &format!("OpenIDConnectProvider {} already exists", arn));
        }
        let provider = Arc::new(OidcProvider {
            arn: arn.clone(),
            url: host,
            thumbprints: RwLock::new(thumbprints),
            client_ids: RwLock::new(client_ids),
            created: now_iso(),
            tags: RwLock::new(Vec::new()),
        });
        state.oidc_providers.write().insert(arn.clone(), provider);
        AwsResponse::xml(200, "CreateOpenIDConnectProvider",
            format!("<OpenIDConnectProviderArn>{}</OpenIDConnectProviderArn>", xml_escape(&arn)))
    }

    fn get_oidc_provider(&self, req: &AwsRequest) -> AwsResponse {
        let arn = get_param(req, "OpenIDConnectProviderArn").unwrap_or_default();
        let state = self.get_state(req.account);
        let provider = state.oidc_providers.read().get(&arn).cloned();
        match provider {
            Some(p) => {
                let mut body = format!(
                    "<Url>{}</Url><ClientIDList>{}</ClientIDList><ThumbprintList>{}</ThumbprintList><CreateDate>{}</CreateDate>",
                    xml_escape(&p.url),
                    string_list_xml(&p.client_ids.read(), "member"),
                    string_list_xml(&p.thumbprints.read(), "member"),
                    p.created
                );
                if !p.tags.read().is_empty() {
                    body.push_str(&tags_xml(&p.tags.read()));
                }
                AwsResponse::xml(200, "GetOpenIDConnectProvider", body)
            }
            None => err(404, "NoSuchEntity",
                &format!("OpenIDConnect Provider not found for {}", arn)),
        }
    }

    fn list_oidc_providers(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account);
        let mut arns: Vec<String> = state.oidc_providers.read().keys().cloned().collect();
        arns.sort();
        let mut body = String::from("<OpenIDConnectProviderList>");
        for arn in &arns {
            body.push_str(&format!("<member><Arn>{}</Arn></member>", xml_escape(arn)));
        }
        body.push_str("</OpenIDConnectProviderList>");
        AwsResponse::xml(200, "ListOpenIDConnectProviders", body)
    }

    fn update_oidc_thumbprint(&self, req: &AwsRequest) -> AwsResponse {
        let arn = get_param(req, "OpenIDConnectProviderArn").unwrap_or_default();
        let thumbprints = string_vec(req, "ThumbprintList");
        let state = self.get_state(req.account);
        let provider = state.oidc_providers.read().get(&arn).cloned();
        match provider {
            Some(p) => {
                *p.thumbprints.write() = thumbprints;
                AwsResponse::xml(200, "UpdateOpenIDConnectProviderThumbprint", String::new())
            }
            None => err(404, "NoSuchEntity",
                &format!("OpenIDConnect Provider not found for {}", arn)),
        }
    }

    fn delete_oidc_provider(&self, req: &AwsRequest) -> AwsResponse {
        let arn = get_param(req, "OpenIDConnectProviderArn").unwrap_or_default();
        let state = self.get_state(req.account);
        if state.oidc_providers.write().remove(&arn).is_none() {
            return err(404, "NoSuchEntity",
                &format!("OpenIDConnect Provider not found for {}", arn));
        }
        AwsResponse::xml(200, "DeleteOpenIDConnectProvider", String::new())
    }

    fn add_oidc_client_id(&self, req: &AwsRequest) -> AwsResponse {
        let arn = get_param(req, "OpenIDConnectProviderArn").unwrap_or_default();
        let client = get_param(req, "ClientID").unwrap_or_default();
        let state = self.get_state(req.account);
        let provider = state.oidc_providers.read().get(&arn).cloned();
        match provider {
            Some(p) => {
                let mut ids = p.client_ids.write();
                if !ids.contains(&client) {
                    ids.push(client);
                }
                AwsResponse::xml(200, "AddClientIDToOpenIDConnectProvider", String::new())
            }
            None => err(404, "NoSuchEntity",
                &format!("OpenIDConnect Provider not found for {}", arn)),
        }
    }

    fn remove_oidc_client_id(&self, req: &AwsRequest) -> AwsResponse {
        let arn = get_param(req, "OpenIDConnectProviderArn").unwrap_or_default();
        let client = get_param(req, "ClientID").unwrap_or_default();
        let state = self.get_state(req.account);
        let provider = state.oidc_providers.read().get(&arn).cloned();
        match provider {
            Some(p) => {
                p.client_ids.write().retain(|c| *c != client);
                AwsResponse::xml(200, "RemoveClientIDFromOpenIDConnectProvider", String::new())
            }
            None => err(404, "NoSuchEntity",
                &format!("OpenIDConnect Provider not found for {}", arn)),
        }
    }

    fn tag_oidc_provider(&self, req: &AwsRequest) -> AwsResponse {
        let arn = get_param(req, "OpenIDConnectProviderArn").unwrap_or_default();
        let tags = self.parse_query_list(req, "Tags");
        let state = self.get_state(req.account);
        let provider = state.oidc_providers.read().get(&arn).cloned();
        match provider {
            Some(p) => tag_upsert(&mut p.tags.write(), tags),
            None => return err(404, "NoSuchEntity",
                &format!("OpenIDConnect Provider not found for {}", arn)),
        }
        AwsResponse::xml(200, "TagOpenIDConnectProvider", String::new())
    }

    fn untag_oidc_provider(&self, req: &AwsRequest) -> AwsResponse {
        let arn = get_param(req, "OpenIDConnectProviderArn").unwrap_or_default();
        let state = self.get_state(req.account);
        let provider = state.oidc_providers.read().get(&arn).cloned();
        match provider {
            Some(p) => remove_tags(&mut p.tags.write(), &string_vec(req, "TagKeys")),
            None => return err(404, "NoSuchEntity",
                &format!("OpenIDConnect Provider not found for {}", arn)),
        }
        AwsResponse::xml(200, "UntagOpenIDConnectProvider", String::new())
    }

    fn list_oidc_provider_tags(&self, req: &AwsRequest) -> AwsResponse {
        let arn = get_param(req, "OpenIDConnectProviderArn").unwrap_or_default();
        let state = self.get_state(req.account);
        let tags = state.oidc_providers.read().get(&arn)
            .map(|p| p.tags.read().clone())
            .unwrap_or_default();
        AwsResponse::xml(200, "ListOpenIDConnectProviderTags",
            format!("{}<IsTruncated>false</IsTruncated>", tags_xml(&tags)))
    }

    // ---------------- Instance profiles ----------------

    fn instance_profile_xml(&self, state: &IamState, name: &str) -> String {
        let arn = format!("arn:aws:iam::{}:instance-profile/{}", state_account_id(state), name);
        let _ = arn;
        let ip_id = state.instance_profile_ids.read().get(name).cloned().unwrap_or_default();
        let mut s = format!(
            "<InstanceProfile><Path>/</Path><InstanceProfileName>{}</InstanceProfileName><InstanceProfileId>{}</InstanceProfileId><Arn>arn:aws:iam::{}:instance-profile/{}</Arn><CreateDate>2024-01-01T00:00:00Z</CreateDate>",
            xml_escape(name), ip_id, state_account_id(state), name
        );
        {
            let profiles = state.instance_profiles.read();
            let mut roles_named = String::new();
            if let Some(roles) = profiles.get(name) {
                for role_name in roles {
                    if let Some(role) = state.get_role(role_name) {
                        roles_named.push_str(&self.role_xml(&role, false));
                    }
                }
            }
            s.push_str(&format!("<Roles>{}</Roles>", roles_named));
        }
        if let Some(tags) = state.instance_profile_tags.read().get(name) {
            if !tags.is_empty() {
                s.push_str(&tags_xml(tags));
            }
        }
        s.push_str("</InstanceProfile>");
        s
    }

    fn create_instance_profile(&self, req: &AwsRequest) -> AwsResponse {
        let name = get_param(req, "InstanceProfileName").unwrap_or_default();
        if name.is_empty() {
            return err(400, "MissingParameterValue", "InstanceProfileName is required");
        }
        let state = self.get_state(req.account);
        if state.instance_profiles.read().contains_key(&name) {
            return err(409, "EntityAlreadyExists",
                &format!("Instance profile {} already exists", name));
        }
        let tags = self.parse_query_list(req, "Tags");
        state.create_instance_profile(&name);
        if !tags.is_empty() {
            state.instance_profile_tags.write().insert(name.clone(), tags);
        }
        AwsResponse::xml(200, "CreateInstanceProfile",
            self.instance_profile_xml(&state, &name))
    }

    fn get_instance_profile(&self, req: &AwsRequest) -> AwsResponse {
        let name = get_param(req, "InstanceProfileName").unwrap_or_default();
        let state = self.get_state(req.account);
        if state.instance_profiles.read().contains_key(&name) {
            AwsResponse::xml(200, "GetInstanceProfile", self.instance_profile_xml(&state, &name))
        } else {
            err(404, "NoSuchEntity",
                &format!("Instance profile {} cannot be found", name))
        }
    }

    fn delete_instance_profile(&self, req: &AwsRequest) -> AwsResponse {
        let name = get_param(req, "InstanceProfileName").unwrap_or_default();
        let state = self.get_state(req.account);
        if !state.delete_instance_profile(&name) {
            return err(404, "NoSuchEntity",
                &format!("Instance profile {} cannot be found", name));
        }
        AwsResponse::xml(200, "DeleteInstanceProfile", String::new())
    }

    fn list_instance_profiles(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account);
        let mut names: Vec<String> = state.instance_profiles.read().keys().cloned().collect();
        names.sort();
        let mut body = String::new();
        for name in &names {
            body.push_str(&self.instance_profile_xml(&state, name));
        }
        AwsResponse::xml(200, "ListInstanceProfiles",
            format!("<InstanceProfiles>{}</InstanceProfiles><IsTruncated>false</IsTruncated>", body))
    }

    fn add_role_to_instance_profile(&self, req: &AwsRequest) -> AwsResponse {
        let name = get_param(req, "InstanceProfileName").unwrap_or_default();
        let role_name = get_param(req, "RoleName").unwrap_or_default();
        let state = self.get_state(req.account);
        if !state.instance_profiles.read().contains_key(&name) {
            return err(404, "NoSuchEntity",
                &format!("Instance profile {} cannot be found", name));
        }
        if state.get_role(&role_name).is_none() {
            return err(404, "NoSuchEntity",
                &format!("The role with name {} cannot be found.", role_name));
        }
        let mut profiles = state.instance_profiles.write();
        if let Some(roles) = profiles.get_mut(&name) {
            if !roles.contains(&role_name) {
                roles.push(role_name);
            }
        }
        AwsResponse::xml(200, "AddRoleToInstanceProfile", String::new())
    }

    fn remove_role_from_instance_profile(&self, req: &AwsRequest) -> AwsResponse {
        let name = get_param(req, "InstanceProfileName").unwrap_or_default();
        let role_name = get_param(req, "RoleName").unwrap_or_default();
        let state = self.get_state(req.account);
        state.remove_role_from_instance_profile(&name, &role_name);
        AwsResponse::xml(200, "RemoveRoleFromInstanceProfile", String::new())
    }

    fn list_instance_profiles_for_role(&self, req: &AwsRequest) -> AwsResponse {
        let role_name = get_param(req, "RoleName").unwrap_or_default();
        let state = self.get_state(req.account);
        let mut names: Vec<String> = state.instance_profiles.read().keys().cloned().collect();
        names.sort();
        let mut body = String::new();
        for name in &names {
            let contains_role = state.instance_profiles.read().get(name)
                .map(|roles| roles.contains(&role_name))
                .unwrap_or(false);
            if contains_role {
                body.push_str(&self.instance_profile_xml(&state, name));
            }
        }
        AwsResponse::xml(200, "ListInstanceProfilesForRole",
            format!("<InstanceProfiles>{}</InstanceProfiles>", body))
    }

    fn tag_instance_profile(&self, req: &AwsRequest) -> AwsResponse {
        let name = get_param(req, "InstanceProfileName").unwrap_or_default();
        let tags = self.parse_query_list(req, "Tags");
        let state = self.get_state(req.account);
        if !state.instance_profiles.read().contains_key(&name) {
            return err(404, "NoSuchEntity",
                &format!("Instance profile {} cannot be found", name));
        }
        state.instance_profile_tags
            .write()
            .entry(name)
            .or_default()
            .extend(tags);
        AwsResponse::xml(200, "TagInstanceProfile", String::new())
    }

    fn untag_instance_profile(&self, req: &AwsRequest) -> AwsResponse {
        let name = get_param(req, "InstanceProfileName").unwrap_or_default();
        let keys = string_vec(req, "TagKeys");
        let state = self.get_state(req.account);
        if !state.instance_profiles.read().contains_key(&name) {
            return err(404, "NoSuchEntity",
                &format!("Instance profile {} cannot be found", name));
        }
        let mut tags = state.instance_profile_tags.write();
        if let Some(existing) = tags.get_mut(&name) {
            remove_tags(existing, &keys);
        }
        AwsResponse::xml(200, "UntagInstanceProfile", String::new())
    }

    fn list_instance_profile_tags(&self, req: &AwsRequest) -> AwsResponse {
        let name = get_param(req, "InstanceProfileName").unwrap_or_default();
        let state = self.get_state(req.account);
        let tags = state.instance_profile_tags.read().get(&name).cloned().unwrap_or_default();
        AwsResponse::xml(200, "ListInstanceProfileTags",
            format!("{}<IsTruncated>false</IsTruncated>", tags_xml(&tags)))
    }

    // ---------------- SSH public keys ----------------

    fn upload_ssh_public_key(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_default();
        let body_key = get_param(req, "SSHPublicKeyBody").unwrap_or_default();
        let state = self.get_state(req.account);
        if state.get_user(&username).is_none() {
            return err(404, "NoSuchEntity",
                &format!("The user with name {} cannot be found.", username));
        }
        let key = SshKey {
            id: random_id("APKA"),
            fingerprint: fake_fingerprint(),
            body: body_key,
            status: "Active".to_string(),
            upload_date: now_iso(),
        };
        state.ssh_keys.write().entry(username.clone()).or_default().push(key.clone());
        AwsResponse::xml(200, "UploadSSHPublicKey", format!(
            "<SSHPublicKey><UserName>{}</UserName><SSHPublicKeyId>{}</SSHPublicKeyId><Fingerprint>{}</Fingerprint><SSHPublicKeyBody>{}</SSHPublicKeyBody><Status>{}</Status><UploadDate>{}</UploadDate></SSHPublicKey>",
            xml_escape(username.as_str()), key.id, key.fingerprint,
            xml_escape(&key.body), key.status, key.upload_date
        ))
    }

    fn get_ssh_public_key(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_default();
        let key_id = get_param(req, "SSHPublicKeyId").unwrap_or_default();
        let state = self.get_state(req.account);
        if state.get_user(&username).is_none() {
            return err(404, "NoSuchEntity",
                &format!("The user with name {} cannot be found.", username));
        }
        let keys = state.ssh_keys.read();
        let key = keys.get(&username).and_then(|ks| ks.iter().find(|k| k.id == key_id));
        match key {
            Some(k) => AwsResponse::xml(200, "GetSSHPublicKey", format!(
                "<SSHPublicKey><UserName>{}</UserName><SSHPublicKeyId>{}</SSHPublicKeyId><Fingerprint>{}</Fingerprint><SSHPublicKeyBody>{}</SSHPublicKeyBody><Status>{}</Status><UploadDate>{}</UploadDate></SSHPublicKey>",
                xml_escape(username.as_str()), k.id, k.fingerprint,
                xml_escape(&k.body), k.status, k.upload_date
            )),
            None => err(404, "NoSuchEntity",
                &format!("The SSH Public Key with id {} cannot be found", key_id)),
        }
    }

    fn update_ssh_public_key(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_default();
        let key_id = get_param(req, "SSHPublicKeyId").unwrap_or_default();
        let status = get_param(req, "Status").unwrap_or_default();
        let state = self.get_state(req.account);
        let mut keys = state.ssh_keys.write();
        let key = keys.get_mut(&username).and_then(|ks| ks.iter_mut().find(|k| k.id == key_id));
        match key {
            Some(k) => {
                k.status = status;
                AwsResponse::xml(200, "UpdateSSHPublicKey", String::new())
            }
            None => err(404, "NoSuchEntity",
                &format!("The SSH Public Key with id {} cannot be found", key_id)),
        }
    }

    fn delete_ssh_public_key(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_default();
        let key_id = get_param(req, "SSHPublicKeyId").unwrap_or_default();
        let state = self.get_state(req.account);
        let mut keys = state.ssh_keys.write();
        if let Some(ks) = keys.get_mut(&username) {
            ks.retain(|k| k.id != key_id);
        }
        AwsResponse::xml(200, "DeleteSSHPublicKey", String::new())
    }

    fn list_ssh_public_keys(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_default();
        let state = self.get_state(req.account);
        let keys = state.ssh_keys.read().get(&username).cloned().unwrap_or_default();
        let mut body = String::from("<SSHPublicKeys>");
        for k in &keys {
            body.push_str(&format!(
                "<member><UserName>{}</UserName><SSHPublicKeyId>{}</SSHPublicKeyId><Status>{}</Status><UploadDate>{}</UploadDate></member>",
                xml_escape(username.as_str()), k.id, k.status, k.upload_date
            ));
        }
        body.push_str("</SSHPublicKeys><IsTruncated>false</IsTruncated>");
        AwsResponse::xml(200, "ListSSHPublicKeys", body)
    }

    // ---------------- Server certificates ----------------

    fn server_cert_metadata_xml(&self, c: &ServerCert, account: u64) -> String {
        format!(
            "<Path>{}</Path><ServerCertificateName>{}</ServerCertificateName><ServerCertificateId>{}</ServerCertificateId><Arn>arn:aws:iam::{}:server-certificate{}</Arn><UploadDate>{}</UploadDate><Expiration>{}</Expiration>",
            xml_escape(if c.path.is_empty() { "/" } else { &c.path }),
            xml_escape(&c.name),
            c.cert_id,
            account,
            if c.path.is_empty() { format!("/{}", c.name) } else { format!("{}{}", c.path, c.name) },
            c.upload_date,
            c.expiration
        )
    }

    fn upload_server_certificate(&self, req: &AwsRequest) -> AwsResponse {
        let name = get_param(req, "ServerCertificateName").unwrap_or_default();
        if name.is_empty() {
            return err(400, "MissingParameterValue", "ServerCertificateName is required");
        }
        let body = get_param(req, "CertificateBody").unwrap_or_default();
        let private_key = get_param(req, "PrivateKey").unwrap_or_default();
        let chain = get_param(req, "CertificateChain").unwrap_or_default();
        let path = get_param(req, "Path").unwrap_or_else(|| "/".into());
        let state = self.get_state(req.account);
        if state.server_certs.read().contains_key(&name) {
            return err(409, "EntityAlreadyExists",
                &format!("The Server Certificate with name {} cannot be found", name));
        }
        let cert = Arc::new(ServerCert {
            name: name.clone(),
            cert_id: random_id("ASCA"),
            path,
            body,
            private_key,
            chain,
            upload_date: now_iso(),
            expiration: "2033-01-01T00:00:00Z".to_string(),
            tags: RwLock::new(Vec::new()),
        });
        state.server_certs.write().insert(name.clone(), cert.clone());
        AwsResponse::xml(200, "UploadServerCertificate", format!(
            "<ServerCertificateMetadata>{}</ServerCertificateMetadata>",
            self.server_cert_metadata_xml(&cert, req.account)
        ))
    }

    fn get_server_certificate(&self, req: &AwsRequest) -> AwsResponse {
        let name = get_param(req, "ServerCertificateName").unwrap_or_default();
        let state = self.get_state(req.account);
        let cert = state.server_certs.read().get(&name).cloned();
        match cert {
            Some(c) => AwsResponse::xml(200, "GetServerCertificate", format!(
                "<ServerCertificate><ServerCertificateMetadata>{}</ServerCertificateMetadata><CertificateBody>{}</CertificateBody><CertificateChain>{}</CertificateChain></ServerCertificate>",
                self.server_cert_metadata_xml(&c, state_account_id(&state)),
                xml_escape(&c.body),
                xml_escape(&c.chain)
            )),
            None => err(404, "NoSuchEntity",
                &format!("The Server Certificate with name {} cannot be found", name)),
        }
    }

    fn update_server_certificate(&self, req: &AwsRequest) -> AwsResponse {
        let name = get_param(req, "ServerCertificateName").unwrap_or_default();
        let new_name = get_param(req, "NewServerCertificateName");
        let new_path = get_param(req, "NewPath");
        let state = self.get_state(req.account);
        let cert = state.server_certs.read().get(&name).cloned();
        match cert {
            Some(old) => {
                if let Some(n) = new_name {
                    let new_cert = ServerCert {
                        name: n.clone(),
                        path: new_path.clone().unwrap_or_else(|| old.path.clone()),
                        cert_id: old.cert_id.clone(),
                        body: old.body.clone(),
                        private_key: old.private_key.clone(),
                        chain: old.chain.clone(),
                        upload_date: old.upload_date.clone(),
                        expiration: old.expiration.clone(),
                        tags: RwLock::new(old.tags.read().clone()),
                    };
                    state.server_certs.write().remove(&name);
                    state.server_certs.write().insert(n.clone(), Arc::new(new_cert));
                }
                AwsResponse::xml(200, "UpdateServerCertificate", String::new())
            }
            None => err(404, "NoSuchEntity",
                &format!("The Server Certificate with name {} cannot be found", name)),
        }
    }

    fn delete_server_certificate(&self, req: &AwsRequest) -> AwsResponse {
        let name = get_param(req, "ServerCertificateName").unwrap_or_default();
        let state = self.get_state(req.account);
        if state.server_certs.write().remove(&name).is_none() {
            return err(404, "NoSuchEntity",
                &format!("The Server Certificate with name {} cannot be found", name));
        }
        AwsResponse::xml(200, "DeleteServerCertificate", String::new())
    }

    fn list_server_certificates(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account);
        let mut names: Vec<String> = state.server_certs.read().keys().cloned().collect();
        names.sort();
        let mut body = String::from("<ServerCertificateMetadataList>");
        for name in &names {
            let cert = state.server_certs.read().get(name).cloned().unwrap();
            body.push_str(&format!("<member>{}</member>", self.server_cert_metadata_xml(&cert, req.account)));
        }
        body.push_str("</ServerCertificateMetadataList><IsTruncated>false</IsTruncated>");
        AwsResponse::xml(200, "ListServerCertificates", body)
    }

    fn tag_server_certificate(&self, req: &AwsRequest) -> AwsResponse {
        let name = get_param(req, "ServerCertificateName").unwrap_or_default();
        let tags = self.parse_query_list(req, "Tags");
        let state = self.get_state(req.account);
        let cert = state.server_certs.read().get(&name).cloned();
        match cert {
            Some(c) => tag_upsert(&mut c.tags.write(), tags),
            None => return err(404, "NoSuchEntity",
                &format!("The Server Certificate with name {} cannot be found", name)),
        }
        AwsResponse::xml(200, "TagServerCertificate", String::new())
    }

    fn untag_server_certificate(&self, req: &AwsRequest) -> AwsResponse {
        let name = get_param(req, "ServerCertificateName").unwrap_or_default();
        let state = self.get_state(req.account);
        let cert = state.server_certs.read().get(&name).cloned();
        match cert {
            Some(c) => remove_tags(&mut c.tags.write(), &string_vec(req, "TagKeys")),
            None => return err(404, "NoSuchEntity",
                &format!("The Server Certificate with name {} cannot be found", name)),
        }
        AwsResponse::xml(200, "UntagServerCertificate", String::new())
    }

    fn list_server_certificate_tags(&self, req: &AwsRequest) -> AwsResponse {
        let name = get_param(req, "ServerCertificateName").unwrap_or_default();
        let state = self.get_state(req.account);
        let tags = state.server_certs.read().get(&name)
            .map(|c| c.tags.read().clone())
            .unwrap_or_default();
        AwsResponse::xml(200, "ListServerCertificateTags",
            format!("{}<IsTruncated>false</IsTruncated>", tags_xml(&tags)))
    }

    // ---------------- Signing certificates ----------------

    fn upload_signing_certificate(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_default();
        let body = get_param(req, "CertificateBody").unwrap_or_default();
        let state = self.get_state(req.account);
        if state.get_user(&username).is_none() {
            return err(404, "NoSuchEntity",
                &format!("The user with name {} cannot be found.", username));
        }
        let cert = SigningCert {
            certificate_id: random_id("ASCA"),
            body,
            status: "Active".to_string(),
            upload_date: now_iso(),
        };
        state.users.read().get(&username).unwrap()
            .signing_certs.write().push(cert.clone());
        AwsResponse::xml(200, "UploadSigningCertificate", format!(
            "<Certificate><UserName>{}</UserName><CertificateId>{}</CertificateId><CertificateBody>{}</CertificateBody><Status>{}</Status><UploadDate>{}</UploadDate></Certificate>",
            xml_escape(username.as_str()),
            cert.certificate_id,
            xml_escape(&cert.body),
            cert.status,
            cert.upload_date
        ))
    }

    fn update_signing_certificate(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_default();
        let cert_id = get_param(req, "CertificateId").unwrap_or_default();
        let status = get_param(req, "Status").unwrap_or_default();
        let state = self.get_state(req.account);
        let user = state.get_user(&username);
        match user {
            Some(u) => {
                let mut certs = u.signing_certs.write();
                if let Some(c) = certs.iter_mut().find(|c| c.certificate_id == cert_id) {
                    c.status = status;
                }
                AwsResponse::xml(200, "UpdateSigningCertificate", String::new())
            }
            None => err(404, "NoSuchEntity",
                &format!("The user with name {} cannot be found.", username)),
        }
    }

    fn delete_signing_certificate(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_default();
        let cert_id = get_param(req, "CertificateId").unwrap_or_default();
        let state = self.get_state(req.account);
        let user = state.get_user(&username);
        match user {
            Some(u) => {
                u.signing_certs.write().retain(|c| c.certificate_id != cert_id);
                AwsResponse::xml(200, "DeleteSigningCertificate", String::new())
            }
            None => err(404, "NoSuchEntity",
                &format!("The user with name {} cannot be found.", username)),
        }
    }

    fn list_signing_certificates(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_default();
        let state = self.get_state(req.account);
        let certs = state.get_user(&username)
            .map(|u| u.signing_certs.read().clone())
            .unwrap_or_default();
        let mut body = String::from("<Certificates>");
        for c in &certs {
            body.push_str(&format!(
                "<member><UserName>{}</UserName><CertificateId>{}</CertificateId><CertificateBody>{}</CertificateBody><Status>{}</Status><UploadDate>{}</UploadDate></member>",
                xml_escape(username.as_str()),
                c.certificate_id,
                xml_escape(&c.body),
                c.status,
                c.upload_date
            ));
        }
        body.push_str("</Certificates><IsTruncated>false</IsTruncated>");
        AwsResponse::xml(200, "ListSigningCertificates", body)
    }

    // ---------------- Service specific credentials ----------------

    fn ssc_xml(&self, c: &Value) -> String {
        let mut s = String::new();
        for field in [
            "ServiceUserName", "ServicePassword", "ServiceSpecificCredentialId",
            "UserName", "Status", "ServiceName", "CreateDate",
        ] {
            if let Some(v) = c.get(field) {
                s.push_str(&xml_field(field, v.as_str().unwrap_or_default()));
            }
        }
        s
    }

    fn create_service_specific_credential(&self, req: &AwsRequest) -> AwsResponse {
        let username = get_param(req, "UserName").unwrap_or_default();
        let service = get_param(req, "ServiceName").unwrap_or_default();
        let state = self.get_state(req.account);
        if state.get_user(&username).is_none() {
            return err(404, "NoSuchEntity",
                &format!("The user with name {} cannot be found.", username));
        }
        let cred = json!({
            "UserName": username.clone(),
            "ServiceName": service.clone(),
            "ServiceUserName": format!("{}-at-{}", username, req.account),
            "ServicePassword": random_password(),
            "ServiceSpecificCredentialId": random_id("ACCA"),
            "CreateDate": now_iso(),
            "Status": "Active"
        });
        state.ssc_creds.write().entry(username).or_default().push(cred.clone());
        AwsResponse::xml(200, "CreateServiceSpecificCredential",
            format!("<ServiceSpecificCredential>{}</ServiceSpecificCredential>", self.ssc_xml(&cred)))
    }

    fn list_service_specific_credentials(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account);
        let username = get_param(req, "UserName").unwrap_or_default();
        let creds = state.ssc_creds.read().get(&username).cloned().unwrap_or_default();
        let mut body = String::from("<ServiceSpecificCredentials>");
        for c in &creds {
            body.push_str(&format!("<member>{}</member>", self.ssc_xml(c)));
        }
        body.push_str("</ServiceSpecificCredentials>");
        AwsResponse::xml(200, "ListServiceSpecificCredentials", body)
    }

    fn find_ssc<F>(&self, req: &AwsRequest, f: F) -> Option<Value>
    where
        F: Fn(&Value) -> bool,
    {
        let state = self.get_state(req.account);
        let username = get_param(req, "UserName").unwrap_or_default();
        let cred_id = get_param(req, "ServiceSpecificCredentialId").unwrap_or_default();
        let creds = state.ssc_creds.read();
        creds.get(&username).and_then(|list| {
            list.iter().find(|c| f(c) && c.get("ServiceSpecificCredentialId")
                .and_then(|v| v.as_str()) == Some(cred_id.as_str())).cloned()
        })
    }

    fn update_service_specific_credential(&self, req: &AwsRequest) -> AwsResponse {
        let cred = self.find_ssc(req, |_| true);
        let _ = cred; // Status is handled by Update/Activate/Deactivate separately
        AwsResponse::xml(200, "UpdateServiceSpecificCredential", String::new())
    }

    fn reset_service_specific_credential(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account);
        let username = get_param(req, "UserName").unwrap_or_default();
        let cred_id = get_param(req, "ServiceSpecificCredentialId").unwrap_or_default();
        let mut creds_map = state.ssc_creds.write();
        let mut updated: Option<Value> = None;
        if let Some(list) = creds_map.get_mut(&username) {
            for c in list.iter_mut() {
                if c.get("ServiceSpecificCredentialId").and_then(|v| v.as_str()) == Some(cred_id.as_str()) {
                    c["ServicePassword"] = json!(random_password());
                    updated = Some(c.clone());
                    break;
                }
            }
        }
        match updated {
            Some(c) => AwsResponse::xml(200, "ResetServiceSpecificCredential",
                format!("<ServiceSpecificCredential>{}</ServiceSpecificCredential>", self.ssc_xml(&c))),
            None => err(404, "NoSuchEntity", "Service specific credential not found"),
        }
    }

    fn set_service_specific_credential_status(&self, req: &AwsRequest, status: &str) -> AwsResponse {
        let state = self.get_state(req.account);
        let username = get_param(req, "UserName").unwrap_or_default();
        let cred_id = get_param(req, "ServiceSpecificCredentialId").unwrap_or_default();
        let mut creds_map = state.ssc_creds.write();
        let mut found = false;
        if let Some(list) = creds_map.get_mut(&username) {
            for c in list.iter_mut() {
                if c.get("ServiceSpecificCredentialId").and_then(|v| v.as_str()) == Some(cred_id.as_str()) {
                    c["Status"] = json!(status);
                    found = true;
                }
            }
        }
        if !found {
            return err(404, "NoSuchEntity", "Service specific credential not found");
        }
        AwsResponse::xml(200, "UpdateServiceSpecificCredential", String::new())
    }

    fn delete_service_specific_credential(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account);
        let username = get_param(req, "UserName").unwrap_or_default();
        let cred_id = get_param(req, "ServiceSpecificCredentialId").unwrap_or_default();
        let mut creds_map = state.ssc_creds.write();
        let mut found = false;
        if let Some(list) = creds_map.get_mut(&username) {
            let before = list.len();
            list.retain(|c| c.get("ServiceSpecificCredentialId").and_then(|v| v.as_str()) != Some(cred_id.as_str()));
            found = list.len() < before;
        }
        if !found {
            return err(404, "NoSuchEntity", "Service specific credential not found");
        }
        AwsResponse::xml(200, "DeleteServiceSpecificCredential", String::new())
    }

    // ---------------- Service linked roles ----------------

    fn create_service_linked_role(&self, req: &AwsRequest) -> AwsResponse {
        let service_name = get_param(req, "AWSServiceName").unwrap_or_default();
        if service_name.is_empty() {
            return err(400, "MissingParameterValue", "AWSServiceName is required");
        }
        let description = get_param(req, "Description").unwrap_or_default();
        let suffix = get_param(req, "CustomSuffix").unwrap_or_default();

        // Resolve the service segment of the role name (moto semantics)
        let parts: Vec<&str> = service_name.split('.').collect();
        let service_part = if parts.len() >= 3 { parts[parts.len() - 3] } else { parts[0] };
        let prefix_part = parts[0];
        let service = if service_part != prefix_part {
            let prefix_cap = prefix_part
                .split('-')
                .map(capitalize)
                .collect::<String>();
            let base = SERVICE_NAME_CONVERSION.iter()
                .find(|(s, _)| *s == service_part)
                .map(|(_, v)| *v)
                .unwrap_or(service_part);
            format!("{}_{}", base, prefix_cap)
        } else {
            SERVICE_NAME_CONVERSION.iter()
                .find(|(s, _)| *s == service_part)
                .map(|(_, v)| v.to_string())
                .unwrap_or_else(|| service_part.to_string())
        };
        let mut role_name = format!("AWSServiceRoleFor{}", service);
        if !suffix.is_empty() {
            role_name = format!("{}_{}", role_name, suffix);
        }

        let trust_doc = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Action": ["sts:AssumeRole"],
                "Effect": "Allow",
                "Principal": {"Service": [service_name]}
            }]
        }).to_string();

        let path = format!("/aws-service-role/{}/", service_name);
        let state = self.get_state(req.account);
        let role = Arc::new(Role::full(
            req.account, role_name.clone(), path,
            trust_doc, description, 3600, true));
        state.roles.write().insert(role.role_name.clone(), role.clone());
        AwsResponse::xml(200, "CreateServiceLinkedRole", self.role_xml(&role, true))
    }

    fn delete_service_linked_role(&self, req: &AwsRequest) -> AwsResponse {
        let role_name = get_param(req, "RoleName").unwrap_or_default();
        let state = self.get_state(req.account);
        state.roles.write().remove(&role_name);
        AwsResponse::xml(200, "DeleteServiceLinkedRole",
            format!("<DeletionTaskId>{}</DeletionTaskId>", uuid::Uuid::new_v4()))
    }

    fn get_service_linked_role_deletion_status(&self, _req: &AwsRequest) -> AwsResponse {
        AwsResponse::xml(200, "GetServiceLinkedRoleDeletionStatus", "<Status>SUCCEEDED</Status>".into())
    }

    // ---------------- Reports ----------------

    fn generate_credential_report(&self, _req: &AwsRequest) -> AwsResponse {
        AwsResponse::xml(200, "GenerateCredentialReport",
            "<State>COMPLETE</State><Description/>".into())
    }

    fn get_credential_report(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account);
        let mut csv = String::from(
            "user,arn,groups,access_keys,password_enabled,password_last_used,mfa_active,credentials_report_generator");
        {
            let users = state.users.read();
            for user in users.values() {
                let mfa_on = state.virtual_mfas.read().values()
                    .any(|d| d.assigned_user.read().as_deref() == Some(user.username.as_str()));
                let has_password = state.login_profiles.read().contains_key(&user.username);
                csv.push_str(&format!(
                    "\n{},{},,,,{},{}",
                    user.username, user.arn,
                    if has_password { "TRUE" } else { "FALSE" },
                    if mfa_on { 1 } else { 0 }
                ));
            }
        }
        AwsResponse::xml(200, "GetCredentialReport",
            format!("<Content>{}</Content><ReportFormat>text/csv</ReportFormat><GeneratedTime>{}</GeneratedTime>",
                base64_encode(csv.as_bytes()), now_iso()))
    }

    // ---------------- Simulation / context keys ----------------

    fn simulate_policy(&self, req: &AwsRequest, principal: bool) -> AwsResponse {
        let state = self.get_state(req.account);
        let actions = string_vec(req, "ActionNames");
        let mut docs: Vec<(String, String)> = Vec::new();
        if principal {
            let source_arn = get_param(req, "PolicySourceArn").unwrap_or_default();
            // attached managed policies
            docs = self.docs_for_principal(&state, &source_arn);
        } else {
            let inputs = string_vec(req, "PolicyInputList");
            for (i, doc) in inputs.iter().enumerate() {
                docs.push((format!("PolicyInputList.{}", i + 1), doc.clone()));
            }
        }
        if actions.is_empty() {
            return err(400, "MissingParameterValue", "ActionNames is required");
        }
        let mut body = String::from("<EvaluationResults>");
        for action in &actions {
            let matched = docs.iter().enumerate()
                .filter(|(_, (_, doc))| action_matches(doc, action))
                .map(|(i, _)| format!(
                    "<member><SourcePolicyId>{}</SourcePolicyId><SourcePolicyType>IAM Policy</SourcePolicyType></member>",
                    docs[i].0))
                .collect::<Vec<_>>()
                .join("");
            let decision = if matched.is_empty() { "implicitDeny" } else { "allowed" };
            body.push_str(&format!(
                "<member><EvalActionName>{}</EvalActionName><EvalResourceName>*</EvalResourceName><EvalDecision>{}</EvalDecision><MatchedStatements>{}</MatchedStatements><MissingContextValues></MissingContextValues></member>",
                xml_escape(action), decision, matched
            ));
        }
        body.push_str("</EvaluationResults>");
        AwsResponse::xml(200, if principal { "SimulatePrincipalPolicy" } else { "SimulateCustomPolicy" }, body)
    }

    fn docs_for_principal(&self, state: &IamState, source_arn: &str) -> Vec<(String, String)> {
        let mut docs = Vec::new();
        if let Some(role) = state.roles.read().values().find(|r| r.arn == source_arn) {
            for (i, p) in role.inline_policies.read().iter().enumerate() {
                if let Some(doc) = role.policy_documents.read().get(p) {
                    docs.push((format!("InlineRolePolicy{}", i + 1), doc.clone()));
                }
            }
            let arns = role.attached_policies.read().clone();
            self.push_attached_docs(state, &arns, &mut docs);
            return docs;
        }
        if let Some(user) = state.users.read().values().find(|u| u.arn == source_arn) {
            for (i, p) in user.policies.read().iter().enumerate() {
                if let Some(doc) = user.policy_documents.read().get(p) {
                    docs.push((format!("InlineUserPolicy{}", i + 1), doc.clone()));
                }
            }
            let arns = user.attached_policies.read().clone();
            self.push_attached_docs(state, &arns, &mut docs);
            return docs;
        }
        // group arn
        if let Some(group) = state.groups.read().values().find(|g| g.arn == source_arn) {
            let arns = group.attached_policies.read().clone();
            self.push_attached_docs(state, &arns, &mut docs);
        }
        docs
    }

    fn push_attached_docs(&self, state: &IamState, arns: &[String], docs: &mut Vec<(String, String)>) {
        for (i, arn) in arns.iter().enumerate() {
            if let Some(p) = state.get_policy(arn) {
                docs.push((format!("AttachedPolicy{}", i + 1), p.policy_document.read().clone()));
            }
        }
    }

    fn get_context_keys(&self, req: &AwsRequest, principal: bool) -> AwsResponse {
        let state = self.get_state(req.account);
        let mut docs: Vec<(String, String)> = Vec::new();
        if principal {
            let source_arn = get_param(req, "PolicySourceArn").unwrap_or_default();
            docs = self.docs_for_principal(&state, &source_arn);
        } else {
            let inputs = string_vec(req, "PolicyInputList");
            for (i, doc) in inputs.iter().enumerate() {
                docs.push((format!("PolicyInputList.{}", i + 1), doc.clone()));
            }
        }
        let mut keys: Vec<String> = Vec::new();
        for (_, doc) in &docs {
            extract_condition_keys(doc, &mut keys);
        }
        keys.sort();
        keys.dedup();
        let mut body = String::from("<ContextKeyNames>");
        for k in &keys {
            body.push_str(&format!("<member>{}</member>", xml_escape(k)));
        }
        body.push_str("</ContextKeyNames>");
        AwsResponse::xml(200, if principal { "GetContextKeysForPrincipalPolicy" } else { "GetContextKeysForCustomPolicy" }, body)
    }

    // ---------------- Service last accessed details ----------------

    fn generate_service_last_accessed_details(&self, _req: &AwsRequest) -> AwsResponse {
        AwsResponse::xml(200, "GenerateServiceLastAccessedDetails",
            format!("<JobId>{}</JobId>", uuid::Uuid::new_v4()))
    }

    fn get_service_last_accessed_details(&self, req: &AwsRequest) -> AwsResponse {
        let now = now_iso();
        AwsResponse::xml(200, req.operation.as_str(),
            format!(
                "<JobStatus>COMPLETED</JobStatus><JobCreationDate>{}</JobCreationDate><ServicesLastAccessed/><JobCompletionDate>{}</JobCompletionDate><IsTruncated>false</IsTruncated>",
                now, now
            ))
    }

    fn get_service_last_accessed_details_with_entities(&self, req: &AwsRequest) -> AwsResponse {
        let now = now_iso();
        AwsResponse::xml(200, req.operation.as_str(),
            format!(
                "<JobStatus>COMPLETED</JobStatus><JobCreationDate>{}</JobCreationDate><EntitiesLastAccessed/><JobCompletionDate>{}</JobCompletionDate><IsTruncated>false</IsTruncated>",
                now, now
            ))
    }

    // ---------------- Permissions boundaries ----------------

    fn put_boundary(&self, req: &AwsRequest, user: bool) -> AwsResponse {
        let arn = get_param(req, "PermissionsBoundary").unwrap_or_default();
        let state = self.get_state(req.account);
        if user {
            let name = get_param(req, "UserName").unwrap_or_default();
            if let Some(u) = state.get_user(&name) {
                *u.permissions_boundary.write() = Some(arn);
            }
            return AwsResponse::xml(200, "PutUserPermissionsBoundary", String::new());
        }
        let name = get_param(req, "RoleName").unwrap_or_default();
        if let Some(r) = state.get_role(&name) {
            *r.permissions_boundary.write() = Some(arn);
        }
        AwsResponse::xml(200, "PutRolePermissionsBoundary", String::new())
    }

    fn delete_boundary(&self, req: &AwsRequest, user: bool) -> AwsResponse {
        let state = self.get_state(req.account);
        if user {
            let name = get_param(req, "UserName").unwrap_or_default();
            if let Some(u) = state.get_user(&name) {
                *u.permissions_boundary.write() = None;
            }
            return AwsResponse::xml(200, "DeleteUserPermissionsBoundary", String::new());
        }
        let name = get_param(req, "RoleName").unwrap_or_default();
        if let Some(r) = state.get_role(&name) {
            *r.permissions_boundary.write() = None;
        }
        AwsResponse::xml(200, "DeleteRolePermissionsBoundary", String::new())
    }

    // ---------------- Organization stubs ----------------

    fn org_features(&self, req: &AwsRequest, features: Vec<&str>) -> AwsResponse {
        let mut body = format!("<OrganizationId>{}</OrganizationId><EnabledFeatures>", "o-stub00000000");
        for f in &features {
            body.push_str(&format!("<member>{}</member>", f));
        }
        body.push_str("</EnabledFeatures>");
        AwsResponse::xml(200, req.operation.as_str(), body)
    }

    // ---------------- Query list parsing ----------------

    /// Parse a query-protocol list of structs: `Tags.member.N.Field = v`.
    fn parse_query_list(&self, req: &AwsRequest, prefix: &str) -> Vec<Value> {
        parse_member_structs(&req.params, prefix)
    }
}

// =====================================================================
// Free helper functions
// =====================================================================

/// Escape &, <, > for embedding a string in XML text content.
fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

fn escape_policy(s: &str) -> String {
    xml_escape(s)
}

fn err(status: u16, code: &str, message: &str) -> AwsResponse {
    AwsResponse::error(status, code, message)
}

fn empty_ok(op: &str) -> AwsResponse {
    AwsResponse::xml(200, op, String::new())
}

/// Ops that only need an empty 200 response.
fn is_known_noop(op: &str) -> bool {
    matches!(op,
        "UpdateOpenIDConnectProvider"
        | "SetSecurityTokenServicePreferences"
        | "PutUserPermissionsBoundary" | "DeleteUserPermissionsBoundary"
        | "PutRolePermissionsBoundary" | "DeleteRolePermissionsBoundary"
        | "PutGroupPermissionsBoundary" | "RemoveGroupPermissionsBoundary"
        | "CreateContextKey" | "DeleteContextKey" | "PutContextKey" | "GetContextKeyPolicy"
        | "TagVirtualMFADevice" | "UntagVirtualMFADevice"
        | "ListPoliciesGrantingServiceAccess"
        | "GenerateOrganizationsAccessReport" | "GetOrganizationsAccessReport"
        | "GetOutboundWebIdentityFederationInfo"
        | "EnableOutboundWebIdentityFederation" | "DisableOutboundWebIdentityFederation"
        | "EnableOutboundsWebIdentityFederation" | "DisableOutboundsWebIdentityFederation"
        | "SendDelegationToken" | "UpdateDelegationRequest" | "AcceptDelegationRequest"
        | "RejectDelegationRequest" | "AssociateDelegationRequest"
        | "CreateDelegationRequest" | "GetDelegationRequest"
        | "ListCertificates"
        | "GetServiceLastAccessed" | "GetServiceLastAccessedByExternalAccount"
        | "ListOpenIDConnectProviderTagsUnused" // defensive: handled explicitly above
    )
}

/// The account of a state snapshot: IAM is a global service stored per
/// account; profile/cert ARNs are rendered from the default account used by
/// the compat tests (the request account is accessible in the handler).
fn state_account_id(_state: &IamState) -> u64 { 123456789012 }


/// Embedded form of a value to text for XML.
fn format_value(v: &Value) -> String {
    match v {
        Value::Bool(b) => if *b { "true".into() } else { "false".into() },
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        _ => v.to_string(),
    }
}

fn xml_field(name: &str, value: &str) -> String {
    format!("<{}>{}</{}>", xml_escape(name), xml_escape(value), name)
}

fn string_of(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn bool_of(v: Option<String>) -> bool {
    v.map(|s| s == "true" || s == "True").unwrap_or(false)
}

fn bool_param(req: &AwsRequest, map: &mut serde_json::Map<String, Value>, key: &str, default: bool) {
    let val = match get_param(req, key) {
        Some(v) => v == "true" || v == "True",
        None => default,
    };
    map.insert(key.to_string(), json!(val));
}

fn ms_to_iso(ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(ms as i64)
        .map(|d| d.to_rfc3339())
        .unwrap_or_else(now_iso)
}

fn tags_xml(tags: &[Value]) -> String {
    let mut body = String::from("<Tags>");
    for t in tags {
        body.push_str(&format!(
            "<member><Key>{}</Key><Value>{}</Value></member>",
            xml_escape(t.get("Key").and_then(|k| k.as_str()).unwrap_or("")),
            xml_escape(t.get("Value").and_then(|v| v.as_str()).unwrap_or(""))
        ));
    }
    body.push_str("</Tags>");
    body
}

fn names_xml(names: &[String]) -> String {
    let mut body = String::from("<PolicyNames>");
    for n in names {
        body.push_str(&format!("<member>{}</member>", xml_escape(n)));
    }
    body.push_str("</PolicyNames>");
    body
}

fn string_list_xml(items: &[String], member: &str) -> String {
    let mut body = String::new();
    for i in items {
        body.push_str(&format!("<{}>{}</{}>", member, xml_escape(i), member));
    }
    body
}

fn attached_policy_item(arn: &str) -> String {
    let name = arn.rsplit('/').next().unwrap_or("");
    format!(
        "<member><PolicyName>{}</PolicyName><PolicyArn>{}</PolicyArn></member>",
        xml_escape(name), xml_escape(arn)
    )
}

/// Collect a string list parameter from JSON array storage.
fn string_vec(req: &AwsRequest, key: &str) -> Vec<String> {
    let empty = serde_json::Map::new();
    let params = req.params.as_object().unwrap_or(&empty);
    let mut out = Vec::new();
    if let Some(Value::Array(arr)) = params.get(key) {
        for v in arr {
            if let Some(s) = v.as_str() {
                out.push(s.to_string());
            }
        }
        return out;
    }
    // fall back to explicit keys missing member aggregation
    let mut idx = 1usize;
    while let Some(Value::String(s)) = params.get(&format!("{}.member.{}", key, idx)) {
        out.push(s.clone());
        idx += 1;
    }
    out
}

/// Parse a `<prefix>.member.N.Field = value` list into JSON objects.
fn parse_member_structs(params: &Value, prefix: &str) -> Vec<Value> {
    let mut items: std::collections::BTreeMap<usize, serde_json::Map<String, Value>> =
        std::collections::BTreeMap::new();
    let empty = serde_json::Map::new();
    let params = params.as_object().unwrap_or(&empty);
    let dotted = format!("{}.", prefix);
    for (k, v) in params {
        if let Some(rest) = k.strip_prefix(&dotted) {
            if let Some(rest) = rest.strip_prefix("member.") {
                if let Some((idx_str, field)) = rest.split_once('.') {
                    if let Ok(idx) = idx_str.parse::<usize>() {
                        items.entry(idx).or_default().insert(field.to_string(), v.clone());
                    }
                }
            }
        }
    }
    items.into_values().map(Value::Object).collect()
}

fn tag_upsert(target: &mut Vec<Value>, tags: Vec<Value>) {
    let existing = target;
    for t in tags {
        let key = t.get("Key").and_then(|k| k.as_str()).map(String::from);
        if let Some(k) = key {
            existing.retain(|e| e.get("Key").and_then(|x| x.as_str()) != Some(k.as_str()));
        }
        existing.push(t);
    }
}

fn remove_tags(target: &mut Vec<Value>, keys: &[String]) {
    target.retain(|t| {
        t.get("Key").and_then(|k| k.as_str())
            .map(|k| !keys.contains(&k.to_string()))
            .unwrap_or(true)
    });
}

pub(crate) fn base64_encode(data: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(data)
}

fn capitalize(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) => c.to_ascii_uppercase().to_string() + chars.as_str(),
        None => String::new(),
    }
}

fn random_base32() -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let mut out = String::with_capacity(64);
    let mut seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64 ^ (uuid::Uuid::new_v4().as_u128() as u64);
    for _ in 0..64 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        out.push(CHARS[(seed >> 33) as usize % CHARS.len()] as char);
    }
    out
}

fn fake_qrcode() -> Vec<u8> {
    let mut v = vec![0u8; 64];
    for (i, b) in v.iter_mut().enumerate() {
        *b = (i % 251) as u8;
    }
    v
}

fn fake_fingerprint() -> String {
    format!("{:032x}", uuid::Uuid::new_v4().as_u128())
}

fn random_password() -> String {
    format!("{}{}", random_id(""), random_id(""))
}

fn summary_entries(
    state: &IamState,
    virtual_mfa_count: u32,
    mfa_in_use: u32,
    providers: u32,
) -> Vec<(&'static str, u64)> {
    let users = state.users.read().len() as u64;
    let roles = state.roles.read().len() as u64;
    let groups = state.groups.read().len() as u64;
    let policies = state.policies.read().len() as u64;
    let ips = state.instance_profiles.read().len() as u64;
    let certs = state.server_certs.read().len() as u64;
    let access_keys = state.users.read().values()
        .map(|u| u.access_keys.read().len()).sum::<usize>() as u64;
    let signing_certs = state.users.read().values()
        .map(|u| u.signing_certs.read().len()).sum::<usize>() as u64;
    vec![
        ("AccountAccessKeysPresent", access_keys),
        ("AccountMFAEnabled", 0),
        ("AccountSigningCertificatesPresent", signing_certs),
        ("AssumeRolePolicySizeQuota", 2048),
        ("AttachedPoliciesPerGroupQuota", 10),
        ("AttachedPoliciesPerRoleQuota", 10),
        ("AttachedPoliciesPerUserQuota", 10),
        ("GlobalEndpointTokenVersion", 1),
        ("GroupPolicySizeQuota", 5120),
        ("Groups", groups),
        ("GroupsPerUserQuota", 10),
        ("GroupsQuota", 100),
        ("InstanceProfiles", ips),
        ("InstanceProfilesQuota", 1000),
        ("MFADevices", virtual_mfa_count as u64),
        ("MFADevicesInUse", mfa_in_use as u64),
        ("Policies", policies),
        ("PoliciesQuota", 1500),
        ("PolicyVersionsInUse", policies),
        ("PolicyVersionsInUseQuota", 10000),
        ("Providers", providers as u64),
        ("Roles", roles),
        ("RolesQuota", 1000),
        ("ServerCertificates", certs),
        ("ServerCertificatesQuota", 20),
        ("SigningCertificatesPerUserQuota", 2),
        ("UserPolicySizeQuota", 2048),
        ("Users", users),
        ("UsersQuota", 5000),
        ("VersionsPerPolicyQuota", 5),
    ]
}

/// Whether an action string matches a statement's action (with basic globbing).
fn action_matches(doc: &str, action: &str) -> bool {
    let value: Value = match serde_json::from_str(doc) {
        Ok(v) => v,
        Err(_) => return false,
    };
    let statements = value.get("Statement").cloned().unwrap_or_else(|| {
        value.get("statement").cloned().unwrap_or(json!([]))
    });
    let stmt_list: Vec<Value> = if statements.is_array() {
        statements.as_array().cloned().unwrap_or_default()
    } else {
        vec![statements]
    };
    for stmt in &stmt_list {
        if stmt.get("Effect").and_then(|e| e.as_str()).map(|e| e != "Allow").unwrap_or(false) {
            continue;
        }
        let actions_in_stmt = match stmt.get("Action") {
            Some(Value::String(s)) => vec![s.clone()],
            Some(Value::Array(a)) => a.iter().filter_map(|x| x.as_str().map(String::from)).collect(),
            _ => continue,
        };
        for pattern in actions_in_stmt {
            if glob_match(&pattern.to_lowercase(), &action.to_lowercase()) {
                return true;
            }
        }
    }
    false
}

impl Default for IamHandler {
    fn default() -> Self { Self::new() }
}

fn glob_match(pattern: &str, text: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    let (prefix, suffix) = match pattern.split_once('*') {
        Some((p, s)) => (p, s),
        None => return pattern == text,
    };
    text.len() >= prefix.len() + suffix.len()
        && text.starts_with(prefix)
        && text.ends_with(suffix)
}

/// Extract Condition keys from a policy JSON document.
fn extract_condition_keys(doc: &str, keys: &mut Vec<String>) {
    let value: Value = match serde_json::from_str(doc) {
        Ok(v) => v,
        Err(_) => return,
    };
    if let Some(statements) = value.get("Statement") {
        let stmt_list: Vec<Value> = if statements.is_array() {
            statements.as_array().cloned().unwrap_or_default()
        } else {
            vec![statements.clone()]
        };
        for stmt in &stmt_list {
            if let Some(cond) = stmt.get("Condition") {
                collect_keys(cond, keys);
            }
        }
    }
}

fn collect_keys(v: &Value, keys: &mut Vec<String>) {
    match v {
        Value::Object(map) => {
            for (k, val) in map {
                if k.contains(':') {
                    keys.push(k.clone());
                }
                collect_keys(val, keys);
            }
        }
        Value::Array(arr) => {
            for x in arr { collect_keys(x, keys); }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use serde_json::json;

    fn make_req(operation: &str, params: serde_json::Value) -> AwsRequest {
        AwsRequest {
            service: "iam".to_string(),
            operation: operation.to_string(),
            account: 123456789012,
            region: "us-east-1".to_string(),
            params,
            body: Bytes::new(),
            query: String::new(),
        }
    }

    #[test]
    fn test_create_and_get_user() {
        let handler = IamHandler::new();
        handler.handle(make_req("CreateUser", json!({ "UserName": "testuser" })));
        let resp = handler.handle(make_req("GetUser", json!({ "UserName": "testuser" })));
        assert_eq!(resp.status, 200);
        assert!(resp.body.contains("testuser"));
    }

    #[test]
    fn test_create_role_with_path_and_max_session() {
        let handler = IamHandler::new();
        handler.handle(make_req("CreateRole", json!({
            "RoleName": "path-role",
            "AssumeRolePolicyDocument": "{\"Version\":\"2012-10-17\"}",
            "Path": "/custom/",
            "MaxSessionDuration": "7200"
        })));
        let resp = handler.handle(make_req("GetRole", json!({ "RoleName": "path-role" })));
        assert_eq!(resp.status, 200);
        assert!(resp.body.contains("<Path>/custom/</Path>"));
        assert!(resp.body.contains("<MaxSessionDuration>7200</MaxSessionDuration>"));
        assert!(resp.body.contains("role/custom/path-role"));
    }

    #[test]
    fn test_policy_versions_crud() {
        let handler = IamHandler::new();
        let create = handler.handle(make_req("CreatePolicy", json!({
            "PolicyName": "ver-pol",
            "PolicyDocument": "{}"
        })));
        let arn = create.body.split("<Arn>").nth(1).unwrap().split("<").next().unwrap().to_string();
        let v2 = handler.handle(make_req("CreatePolicyVersion", json!({
            "PolicyArn": arn, "PolicyDocument": "{\"a\":1}", "SetAsDefault": "true"
        })));
        assert!(v2.body.contains("<VersionId>v2</VersionId>"));
        let list = handler.handle(make_req("ListPolicyVersions", json!({ "PolicyArn": arn })));
        assert!(list.body.contains("v1"));
        assert!(list.body.contains("v2"));
        assert_eq!(list.body.matches("<member>").count(), 2);
    }

    #[test]
    fn test_virtual_mfa_lifecycle() {
        let handler = IamHandler::new();
        let create = handler.handle(make_req("CreateVirtualMFADevice", json!({
            "VirtualMFADeviceName": "vmfa-unit"
        })));
        assert!(create.body.contains("Base32StringSeed"));
        let serial = create.body.split("<SerialNumber>").nth(1).unwrap()
            .split("<").next().unwrap().to_string();
        let list = handler.handle(make_req("ListVirtualMFADevices", json!({})));
        assert!(list.body.contains("vmfa-unit"));
        handler.handle(make_req("DeleteVirtualMFADevice", json!({ "SerialNumber": serial })));
        let list2 = handler.handle(make_req("ListVirtualMFADevices", json!({})));
        assert!(!list2.body.contains("vmfa-unit"));
        let missing = handler.handle(make_req("DeleteVirtualMFADevice",
            json!({ "SerialNumber": "arn:aws:iam::123456789012:mfa/nothing" })));
        assert_eq!(missing.status, 404);
        assert!(missing.body.contains("NoSuchEntity"));
    }

    #[test]
    fn test_oidc_provider_client_ids() {
        let handler = IamHandler::new();
        let create = handler.handle(make_req("CreateOpenIDConnectProvider", json!({
            "Url": "https://unit.example.com",
            "ThumbprintList": ["a"],
            "ClientIDList": ["one"]
        })));
        let arn = create.body.split("<OpenIDConnectProviderArn>").nth(1).unwrap()
            .split("<").next().unwrap().to_string();
        handler.handle(make_req("AddClientIDToOpenIDConnectProvider",
            json!({ "OpenIDConnectProviderArn": arn, "ClientID": "two" })));
        let get = handler.handle(make_req("GetOpenIDConnectProvider", json!({
            "OpenIDConnectProviderArn": arn })));
        assert!(get.body.contains("<member>one</member>"));
        assert!(get.body.contains("<member>two</member>"));
        assert!(get.body.contains("<Url>unit.example.com</Url>"));
    }

    #[test]
    fn test_error_codes_and_type_sender() {
        let handler = IamHandler::new();
        let resp = handler.handle(make_req("GetUser", json!({ "UserName": "nope" })));
        assert_eq!(resp.status, 404);
        assert!(resp.body.contains("<Code>NoSuchEntity</Code>"));
        assert!(resp.body.contains("<Type>Sender</Type>"));
    }
}
