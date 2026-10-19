//! AWS WorkSpaces backend — moto-equivalent (json protocol).
//!
//! Replaces the moto sidecar for `workspaces` (`X-Amz-Target:
//! WorkspacesService.<Op>` on `application/x-amz-json-1.1`). State is
//! per-(account, region), mirroring moto's `WorkSpacesBackend`
//! (moto/workspaces): workspaces, registered directories, images, IP
//! groups, connection aliases, bundles, connect-client add-ins, account
//! links, pools, tags stored on each resource (routed by id prefix), and
//! the account-level tenancy settings. moto reads its Directory Service
//! and EC2 backends when registering a directory; those in-process
//! backends do not exist here, so `RegisterWorkspaceDirectory` stores the
//! directory record on its own (ds/EC2 details become synthetic defaults).
//!
//! Contract: probes/workspaces.json (per-operation moto-era behavior) and
//! tests/compatibility/test_workspaces_compat.py. moto's unguarded dict
//! indexes (`self.workspace_directories[id]`) crash with a 500
//! `InternalError` on unknown ids — kept as recorded in the probe for
//! DeregisterWorkspaceDirectory, ModifyClientProperties,
//! ModifySelfservicePermissions and UpdateWorkspaceImagePermission;
//! everything else raises moto's typed `ResourceNotFoundException` /
//! `ValidationException`.

use crate::proto::{json_error, now_unix, parse_json_body};
use crate::store::{self, ScopeKey};
use crate::{Request, Response};
use parking_lot::RwLock;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

type Stores = Arc<RwLock<HashMap<ScopeKey, WorkspacesStore>>>;

fn stores() -> Stores {
    store::slot::<WorkspacesStore>("workspaces")
}

/// Per-(account, region) in-memory state for Amazon WorkSpaces.
#[derive(Default)]
struct WorkspacesStore {
    /// workspace id -> workspace
    workspaces: BTreeMap<String, Workspace>,
    /// directory id -> registered directory
    workspace_directories: BTreeMap<String, WorkSpaceDirectory>,
    /// image id -> workspace image
    workspace_images: BTreeMap<String, WorkspaceImage>,
    /// ip group id -> ip group
    ip_groups: BTreeMap<String, IpGroup>,
    /// alias id -> connection alias
    connection_aliases: BTreeMap<String, ConnectionAlias>,
    /// bundle id -> workspace bundle
    workspace_bundles: BTreeMap<String, WorkspaceBundle>,
    /// add-in id -> connect client add-in
    connect_client_add_ins: BTreeMap<String, ConnectClientAddIn>,
    /// link id -> account link
    account_links: BTreeMap<String, AccountLink>,
    /// pool id -> workspaces pool
    workspaces_pools: BTreeMap<String, WorkspacesPool>,
    /// Account-level settings
    dedicated_tenancy_support: String,
    dedicated_tenancy_management_cidr_range: String,
    account_modifications: Vec<Value>,
}

fn with_store(req: &Request, f: impl FnOnce(&mut WorkspacesStore) -> Response) -> Response {
    let slot = stores();
    let mut map = slot.write();
    let st = map.entry((req.account, req.region.clone())).or_default();
    f(st)
}

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

struct Workspace {
    workspace_id: String,
    directory_id: String,
    bundle_id: String,
    user_name: String,
    state: String,
    error_code: String,
    error_message: String,
    volume_encryption_key: String,
    user_volume_encryption_enabled: Value,
    root_volume_encryption_enabled: Value,
    workspace_properties: Value,
    tags: Vec<Value>,
    data_replication_settings: Value,
}

impl Workspace {
    /// moto's `Workspace.to_dict_pending()`: falsy members drop off the wire.
    fn to_dict_pending(&self) -> Value {
        drop_falsy(json!({
            "WorkspaceId": self.workspace_id,
            "DirectoryId": self.directory_id,
            "UserName": self.user_name,
            "IpAddress": "",
            "State": self.state,
            "BundleId": self.bundle_id,
            "SubnetId": "",
            "ErrorMessage": self.error_message,
            "ErrorCode": self.error_code,
            "ComputerName": "",
            "VolumeEncryptionKey": self.volume_encryption_key,
            "UserVolumeEncryptionEnabled": self.user_volume_encryption_enabled,
            "RootVolumeEncryptionEnabled": self.root_volume_encryption_enabled,
            "WorkspaceProperties": self.workspace_properties,
            "ModificationStates": [],
            "RelatedWorkspaces": [],
            "DataReplicationSettings": self.data_replication_settings,
            "StandbyWorkspacesProperties": [],
        }))
    }

    /// moto's `Workspace.to_dict_failed()` plus its recursive falsy-drop.
    fn to_dict_failed(&self) -> Value {
        filter_empty_values(&json!({
            "WorkspaceRequest": {
                "DirectoryId": self.directory_id,
                "UserName": self.user_name,
                "BundleId": self.bundle_id,
                "SubnetId": "",
                "VolumeEncryptionKey": self.volume_encryption_key,
                "UserVolumeEncryptionEnabled": self.user_volume_encryption_enabled,
                "RootVolumeEncryptionEnabled": self.root_volume_encryption_enabled,
                "WorkspaceProperties": self.workspace_properties,
                "Tags": self.tags,
            },
            "ErrorCode": self.error_code,
            "ErrorMessage": self.error_message,
        }))
    }
}

/// One registered directory with moto's property defaults.
struct WorkSpaceDirectory {
    directory_id: String,
    alias: String,
    directory_name: String,
    launch_time: f64,
    registration_code: String,
    subnet_ids: Vec<String>,
    dns_ip_addresses: Vec<String>,
    directory_type: String,
    workspace_security_group_id: String,
    workspace_creation_properties: Value,
    ip_group_ids: Vec<String>,
    workspace_access_properties: Value,
    self_service_permissions: Value,
    saml_properties: Value,
    certificate_based_auth_properties: Value,
    client_properties: Value,
    /// None until the first ImportClientBranding (moto's absent attribute).
    client_branding: Option<Value>,
    streaming_properties: Value,
    endpoint_encryption_mode: String,
    tenancy: String,
    tags: Vec<Value>,
    account: u64,
}

impl WorkSpaceDirectory {
    #[allow(clippy::too_many_arguments)]
    fn new(
        account: u64,
        directory_id: &str,
        subnet_ids: Vec<String>,
        enable_self_service: bool,
        tenancy: Option<&str>,
        tags: Vec<Value>,
    ) -> Self {
        // Self-service permissions default to ENABLED only for restarts;
        // the rest flip on when the directory registered with
        // EnableSelfService.
        let mode = if enable_self_service {
            "ENABLED"
        } else {
            "DISABLED"
        };
        WorkSpaceDirectory {
            directory_id: directory_id.to_string(),
            alias: String::new(),
            directory_name: String::new(),
            launch_time: now_unix(),
            registration_code: format!("SLiad+{}", random_string(6).to_uppercase()),
            subnet_ids,
            dns_ip_addresses: Vec::new(),
            directory_type: "SIMPLE_AD".to_string(),
            workspace_security_group_id: format!("sg-{}", short_hex(9)),
            workspace_creation_properties: json!({
                "EnableInternetAccess": false,
                "DefaultOu": "",
                "CustomSecurityGroupId": "",
                "UserEnabledAsLocalAdministrator": true,
                "EnableMaintenanceMode": true,
            }),
            ip_group_ids: Vec::new(),
            workspace_access_properties: json!({
                "DeviceTypeWindows": "ALLOW",
                "DeviceTypeOsx": "ALLOW",
                "DeviceTypeWeb": "DENY",
                "DeviceTypeIos": "ALLOW",
                "DeviceTypeAndroid": "ALLOW",
                "DeviceTypeChromeOs": "ALLOW",
                "DeviceTypeZeroClient": "ALLOW",
                "DeviceTypeLinux": "DENY",
            }),
            self_service_permissions: json!({
                "RestartWorkspace": "ENABLED",
                "IncreaseVolumeSize": mode,
                "ChangeComputeType": mode,
                "SwitchRunningMode": mode,
                "RebuildWorkspace": mode,
            }),
            saml_properties: json!({
                "Status": "DISABLED",
                "UserAccessUrl": "",
                "RelayStateParameterName": "RelayState",
            }),
            certificate_based_auth_properties: json!({ "Status": "DISABLED" }),
            client_properties: json!({
                "ReconnectEnabled": "ENABLED",
                "LogUploadEnabled": "ENABLED",
            }),
            client_branding: None,
            streaming_properties: json!({}),
            endpoint_encryption_mode: String::new(),
            tenancy: match tenancy {
                Some(t) if !t.is_empty() => t.to_string(),
                _ => "SHARED".to_string(),
            },
            tags,
            account,
        }
    }

    /// moto's `WorkSpaceDirectory.to_dict()` — note its lowercased
    /// `ipGroupIds` and `SelfservicePermissions` casings.
    fn to_dict(&self) -> Value {
        drop_falsy(json!({
            "DirectoryId": self.directory_id,
            "Alias": self.alias,
            "DirectoryName": self.directory_name,
            "RegistrationCode": self.registration_code,
            "SubnetIds": self.subnet_ids,
            "DnsIpAddresses": self.dns_ip_addresses,
            "CustomerUserName": "Administrator",
            "IamRoleId": format!("arn:aws:iam::{}:role/workspaces_DefaultRole", self.account),
            "DirectoryType": self.directory_type,
            "WorkspaceSecurityGroupId": self.workspace_security_group_id,
            "State": "REGISTERED",
            "WorkspaceCreationProperties": self.workspace_creation_properties,
            "ipGroupIds": self.ip_group_ids,
            "WorkspaceAccessProperties": self.workspace_access_properties,
            "Tenancy": self.tenancy,
            "SelfservicePermissions": self.self_service_permissions,
            "SamlProperties": self.saml_properties,
            "CertificateBasedAuthProperties": self.certificate_based_auth_properties,
        }))
    }
}

struct WorkspaceImage {
    image_id: String,
    name: String,
    description: String,
    state: String,
    required_tenancy: String,
    created: f64,
    owner_account: String,
    image_permissions: Vec<Value>,
    tags: Vec<Value>,
}

impl WorkspaceImage {
    /// moto's `WorkspaceImage.to_dict()`.
    fn to_dict(&self) -> Value {
        drop_falsy(json!({
            "ImageId": self.image_id,
            "Name": self.name,
            "Description": self.description,
            "OperatingSystem": {},
            "State": self.state,
            "RequiredTenancy": self.required_tenancy,
            "Created": self.created,
            "OwnerAccountId": self.owner_account,
        }))
    }

    /// moto's `WorkspaceImage.to_desc_dict()`: the four extras are added
    /// whenever not None, so empty strings/lists still appear.
    fn to_desc_dict(&self) -> Value {
        let mut out = self.to_dict();
        let obj = out.as_object_mut().expect("object");
        obj.insert("ErrorCode".into(), json!(""));
        obj.insert("ErrorMessage".into(), json!(""));
        obj.insert(
            "Updates".into(),
            json!({
                "UpdateAvailable": false,
                "Description": "This WorkSpace image does not have updates available",
            }),
        );
        obj.insert("ErrorDetails".into(), json!([]));
        out
    }
}

struct IpGroup {
    group_id: String,
    group_name: String,
    group_desc: String,
    user_rules: Vec<Value>,
    tags: Vec<Value>,
}

impl IpGroup {
    fn to_dict(&self) -> Value {
        json!({
            "groupId": self.group_id,
            "groupName": self.group_name,
            "groupDesc": self.group_desc,
            "userRules": self.user_rules,
        })
    }
}

struct ConnectionAlias {
    alias_id: String,
    connection_string: String,
    state: String,
    owner_account_id: String,
    associations: Vec<Value>,
    permissions: Vec<Value>,
    tags: Vec<Value>,
}

impl ConnectionAlias {
    /// moto's `ConnectionAlias.to_dict()` drops only None, not empties.
    fn to_dict(&self) -> Value {
        json!({
            "AliasId": self.alias_id,
            "ConnectionString": self.connection_string,
            "State": self.state,
            "OwnerAccountId": self.owner_account_id,
            "Associations": self.associations,
        })
    }
}

struct WorkspaceBundle {
    bundle_id: String,
    name: String,
    description: String,
    image_id: String,
    compute_type: Value,
    user_storage: Value,
    root_storage: Value,
    owner: String,
    state: String,
    creation_time: f64,
    last_updated_time: f64,
    tags: Vec<Value>,
}

impl WorkspaceBundle {
    fn to_dict(&self) -> Value {
        drop_falsy(json!({
            "BundleId": self.bundle_id,
            "Name": self.name,
            "Owner": self.owner,
            "Description": self.description,
            "ImageId": self.image_id,
            "ComputeType": self.compute_type,
            "UserStorage": self.user_storage,
            "RootStorage": self.root_storage,
            "State": self.state,
            "CreationTime": self.creation_time,
            "LastUpdatedTime": self.last_updated_time,
        }))
    }
}

struct ConnectClientAddIn {
    add_in_id: String,
    resource_id: String,
    name: String,
    url: String,
}

impl ConnectClientAddIn {
    fn to_dict(&self) -> Value {
        json!({
            "AddInId": self.add_in_id,
            "ResourceId": self.resource_id,
            "Name": self.name,
            "URL": self.url,
        })
    }
}

struct AccountLink {
    link_id: String,
    link_status: String,
    source_account_id: String,
    target_account_id: String,
}

impl AccountLink {
    fn to_dict(&self) -> Value {
        json!({
            "AccountLinkId": self.link_id,
            "AccountLinkStatus": self.link_status,
            "SourceAccountId": self.source_account_id,
            "TargetAccountId": self.target_account_id,
        })
    }
}

struct WorkspacesPool {
    pool_id: String,
    pool_name: String,
    description: String,
    bundle_id: String,
    directory_id: String,
    capacity: Option<Value>,
    application_settings: Option<Value>,
    timeout_settings: Option<Value>,
    running_mode: String,
    state: String,
    created_at: f64,
    pool_arn: String,
    tags: Vec<Value>,
}

impl WorkspacesPool {
    fn to_dict(&self) -> Value {
        let desired = self
            .capacity
            .as_ref()
            .and_then(|c| c.get("DesiredUserSessions"))
            .cloned()
            .unwrap_or(json!(1));
        let mut out = json!({
            "PoolId": self.pool_id,
            "PoolArn": self.pool_arn,
            "CapacityStatus": {
                "AvailableUserSessions": desired,
                "DesiredUserSessions": desired,
                "ActualUserSessions": 0,
                "ActiveUserSessions": 0,
            },
            "PoolName": self.pool_name,
            "Description": self.description,
            "State": self.state,
            "CreatedAt": self.created_at,
            "BundleId": self.bundle_id,
            "DirectoryId": self.directory_id,
            "Errors": [],
        });
        if let Some(cfg) = &self.application_settings {
            out["ApplicationSettings"] = cfg.clone();
        }
        if let Some(cfg) = &self.timeout_settings {
            out["TimeoutSettings"] = cfg.clone();
        }
        out
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// moto's `get_random_hex(n)` shape: lowercase hex.
fn short_hex(n: usize) -> String {
    let hex = Uuid::new_v4().simple().to_string();
    hex[..n].to_string()
}

/// moto's `get_random_string` — WorkSpaces registration codes upper-case it.
fn random_string(n: usize) -> String {
    short_hex(n).to_uppercase()
}

/// Python truthiness for moto's `if v` response filters.
fn py_truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
        Value::String(s) => !s.is_empty(),
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(true),
    }
}

/// moto's `{k: v for k, v in dct.items() if v}` one-level falsy-drop.
fn drop_falsy(value: Value) -> Value {
    match value {
        Value::Object(obj) => {
            let mut obj = obj;
            obj.retain(|_, v| py_truthy(v));
            Value::Object(obj)
        }
        other => other,
    }
}

/// moto's `Workspace.filter_empty_values` — recursive falsy-drop.
fn filter_empty_values(value: &Value) -> Value {
    match value {
        Value::Object(obj) => Value::Object(
            obj.iter()
                .filter(|(_, v)| py_truthy(v))
                .map(|(k, v)| (k.clone(), filter_empty_values(v)))
                .collect(),
        ),
        other => other.clone(),
    }
}

fn as_str<'a>(body: &'a Value, key: &str) -> &'a str {
    body.get(key).and_then(Value::as_str).unwrap_or("")
}

fn as_str_list(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|list| {
            list.iter()
                .filter_map(Value::as_str)
                .map(String::from)
                .collect()
        })
        .unwrap_or_default()
}

fn as_value_list(value: Option<&Value>) -> Vec<Value> {
    value
        .and_then(Value::as_array)
        .map(|l| l.to_vec())
        .unwrap_or_default()
}

/// moto's truthiness on optional lists: only a non-empty list filters.
fn truthy_str_list(value: Option<&Value>) -> Option<Vec<String>> {
    let list = as_str_list(value);
    if list.is_empty() {
        None
    } else {
        Some(list)
    }
}

/// moto's directory-id regex `^d-[0-9a-f]{10}$`.
fn is_directory_id(value: &str) -> bool {
    match value.strip_prefix("d-") {
        Some(id) => {
            id.len() == 10
                && id
                    .chars()
                    .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c))
        }
        None => false,
    }
}

/// moto's image-id regex `^wsi-[0-9a-z]{9}$`.
fn is_image_id(value: &str) -> bool {
    match value.strip_prefix("wsi-") {
        Some(id) => {
            id.len() == 9
                && id
                    .chars()
                    .all(|c| c.is_ascii_digit() || c.is_ascii_lowercase())
        }
        None => false,
    }
}

/// The id prefixes moto routes tags by.
fn has_resource_prefix(id: &str) -> bool {
    ["d-", "ws-", "wsi-", "wsipg-", "wsca-", "wsb-", "wsp-"]
        .iter()
        .any(|p| id.starts_with(p))
}

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "workspaces" {
        return None;
    }
    Some(with_store(req, |st| route(req, st)))
}

fn route(req: &Request, st: &mut WorkspacesStore) -> Response {
    let body = parse_json_body(&req.body);
    match req.operation.as_str() {
        // --- workspaces ------------------------------------------------------
        "CreateWorkspaces" => create_workspaces(st, &body),
        "DescribeWorkspaces" => describe_workspaces(st, &body),
        "TerminateWorkspaces" => terminate_workspaces(st, &body),
        "StartWorkspaces" => state_workspaces(st, &body, "StartWorkspaceRequests", "AVAILABLE"),
        "StopWorkspaces" => state_workspaces(st, &body, "StopWorkspaceRequests", "STOPPED"),
        // moto leaves rebooted/rebuilt workspaces AVAILABLE.
        "RebootWorkspaces" => state_workspaces(st, &body, "RebootWorkspaceRequests", ""),
        "RebuildWorkspaces" => state_workspaces(st, &body, "RebuildWorkspaceRequests", ""),
        "ModifyWorkspaceProperties" => modify_workspace_properties(st, &body),
        "ModifyWorkspaceState" => modify_workspace_state(st, &body),
        "RestoreWorkspace" => restore_workspace(st, &body),
        "MigrateWorkspace" => migrate_workspace(st, &body),
        "DescribeWorkspacesConnectionStatus" => describe_workspaces_connection_status(st, &body),
        "DescribeWorkspaceSnapshots" => describe_workspace_snapshots(st, &body),
        "CreateStandbyWorkspaces" => create_standby_workspaces(st, &body),
        // --- directories -------------------------------------------------------
        "DescribeWorkspaceDirectories" => describe_workspace_directories(st, &body),
        "RegisterWorkspaceDirectory" => register_workspace_directory(req, st, &body),
        "DeregisterWorkspaceDirectory" => deregister_workspace_directory(st, &body),
        "ModifyWorkspaceCreationProperties" => modify_workspace_creation_properties(st, &body),
        "ModifyWorkspaceAccessProperties" => modify_workspace_access_properties(st, &body),
        "ModifySamlProperties" => modify_saml_properties(st, &body),
        "ModifyCertificateBasedAuthProperties" => {
            modify_certificate_based_auth_properties(st, &body)
        }
        "ModifyStreamingProperties" => modify_streaming_properties(st, &body),
        "ModifyEndpointEncryptionMode" => modify_endpoint_encryption_mode(st, &body),
        "ModifySelfservicePermissions" => modify_selfservice_permissions(st, &body),
        "ModifyClientProperties" => modify_client_properties(st, &body),
        "DescribeClientProperties" => describe_client_properties(st, &body),
        // --- tags ---------------------------------------------------------------
        "CreateTags" => create_tags(st, &body),
        "DeleteTags" => delete_tags(st, &body),
        "DescribeTags" => describe_tags(st, &body),
        // --- images ---------------------------------------------------------------
        "CreateWorkspaceImage" => create_workspace_image(req, st, &body),
        "DescribeWorkspaceImages" => describe_workspace_images(req, st, &body),
        "UpdateWorkspaceImagePermission" => update_workspace_image_permission(st, &body),
        "DescribeWorkspaceImagePermissions" => describe_workspace_image_permissions(st, &body),
        "CopyWorkspaceImage" => copy_workspace_image(req, st, &body),
        "DeleteWorkspaceImage" => delete_workspace_image(st, &body),
        "CreateUpdatedWorkspaceImage" => create_updated_workspace_image(req, st, &body),
        "ImportWorkspaceImage" => import_workspace_image(req, st, &body),
        "ImportCustomWorkspaceImage" => import_custom_workspace_image(req, st, &body),
        "DescribeCustomWorkspaceImageImport" => describe_custom_workspace_image_import(st, &body),
        // --- bundles -----------------------------------------------------------
        "CreateWorkspaceBundle" => create_workspace_bundle(req, st, &body),
        "DescribeWorkspaceBundles" => describe_workspace_bundles(st, &body),
        "DeleteWorkspaceBundle" => delete_workspace_bundle(st, &body),
        "UpdateWorkspaceBundle" => update_workspace_bundle(st, &body),
        // --- ip groups -----------------------------------------------------------
        "CreateIpGroup" => create_ip_group(st, &body),
        "DescribeIpGroups" => describe_ip_groups(st, &body),
        "DeleteIpGroup" => delete_ip_group(st, &body),
        "AuthorizeIpRules" => authorize_ip_rules(st, &body),
        "RevokeIpRules" => revoke_ip_rules(st, &body),
        "UpdateRulesOfIpGroup" => update_rules_of_ip_group(st, &body),
        "AssociateIpGroups" => associate_ip_groups(st, &body),
        "DisassociateIpGroups" => disassociate_ip_groups(st, &body),
        // --- connection aliases ----------------------------------------------------
        "CreateConnectionAlias" => create_connection_alias(req, st, &body),
        "DescribeConnectionAliases" => describe_connection_aliases(st, &body),
        "DeleteConnectionAlias" => delete_connection_alias(st, &body),
        "AssociateConnectionAlias" => associate_connection_alias(req, st, &body),
        "DisassociateConnectionAlias" => disassociate_connection_alias(st, &body),
        "DescribeConnectionAliasPermissions" => describe_connection_alias_permissions(st, &body),
        "UpdateConnectionAliasPermission" => update_connection_alias_permission(st, &body),
        // --- connect client add-ins -------------------------------------------------
        "CreateConnectClientAddIn" => create_connect_client_add_in(st, &body),
        "DescribeConnectClientAddIns" => describe_connect_client_add_ins(st, &body),
        "DeleteConnectClientAddIn" => delete_connect_client_add_in(st, &body),
        "UpdateConnectClientAddIn" => update_connect_client_add_in(st, &body),
        // --- account links --------------------------------------------------------------
        "CreateAccountLinkInvitation" => create_account_link_invitation(req, st, &body),
        "AcceptAccountLinkInvitation" => transition_account_link(st, &body, "LINKED"),
        "RejectAccountLinkInvitation" => transition_account_link(st, &body, "REJECTED"),
        "DeleteAccountLinkInvitation" => delete_account_link_invitation(st, &body),
        "GetAccountLink" => get_account_link(st, &body),
        "ListAccountLinks" => list_account_links(st, &body),
        // --- pools -----------------------------------------------------------------------
        "CreateWorkspacesPool" => create_workspaces_pool(req, st, &body),
        "DescribeWorkspacesPools" => describe_workspaces_pools(st, &body),
        "UpdateWorkspacesPool" => update_workspaces_pool(st, &body),
        "TerminateWorkspacesPool" => terminate_workspaces_pool(st, &body),
        "StartWorkspacesPool" => pool_lifecycle(st, &body, "AVAILABLE"),
        "StopWorkspacesPool" => pool_lifecycle(st, &body, "STOPPED"),
        "DescribeWorkspacesPoolSessions" => describe_workspaces_pool_sessions(st, &body),
        "TerminateWorkspacesPoolSession" => terminate_workspaces_pool_session(),
        // --- account -----------------------------------------------------------------------
        "DescribeAccount" => describe_account(st),
        "ModifyAccount" => modify_account(st, &body),
        "DescribeAccountModifications" => describe_account_modifications(st),
        // --- client branding ------------------------------------------------------------
        "ImportClientBranding" => import_client_branding(st, &body),
        "DescribeClientBranding" => describe_client_branding(st, &body),
        "DeleteClientBranding" => delete_client_branding(st, &body),
        // --- application associations (moto stubs) ----------------------------------
        "AssociateWorkspaceApplication" => associate_workspace_application(st, &body, "COMPLETED"),
        "DisassociateWorkspaceApplication" => associate_workspace_application(st, &body, "REMOVED"),
        "DeployWorkspaceApplications" => ok(json!({ "Deployment": { "Associations": [] } })),
        "DescribeApplicationAssociations" => ok(json!({ "Associations": [] })),
        "DescribeApplications" => ok(json!({ "Applications": [] })),
        "DescribeBundleAssociations" => ok(json!({ "Associations": [] })),
        "DescribeImageAssociations" => ok(json!({ "Associations": [] })),
        "DescribeWorkspaceAssociations" => ok(json!({ "Associations": [] })),
        // --- management cidr ranges --------------------------------------------------
        "ListAvailableManagementCidrRanges" => ok(json!({
            "ManagementCidrRanges": [as_str(&body, "ManagementCidrRangeConstraint")]
        })),
        other => json_error(
            501,
            "NotImplemented",
            &format!("The {other} action has not been implemented"),
        ),
    }
}

// ---------------------------------------------------------------------------
// Workspaces
// ---------------------------------------------------------------------------

fn create_workspaces(st: &mut WorkspacesStore, body: &Value) -> Response {
    let mut failed_requests: Vec<Value> = Vec::new();
    let mut pending_requests: Vec<Value> = Vec::new();

    for ws in as_value_list(body.get("Workspaces")) {
        // moto validates the directory id before anything else.
        let directory_id = ws
            .get("DirectoryId")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if !is_directory_id(&directory_id) {
            return validation_ex(&format!(
                "The Directory ID {directory_id} in the request is invalid."
            ));
        }

        // FailedRequests are produced when the directory is unknown.
        let mut error_code = String::new();
        let mut error_msg = String::new();
        if !st.workspace_directories.contains_key(&directory_id) {
            error_code = "ResourceNotFound.Directory".to_string();
            error_msg =
                "The specified directory could not be found in the specified region.".to_string();
        }

        // RunningMode resolution plus the AutoStopTimeout failures.
        let request_properties = ws.get("WorkspaceProperties").cloned().unwrap_or(json!({}));
        let mut running_mode = "ALWAYS_ON".to_string();
        if py_truthy(&request_properties) {
            if let Some(mode) = request_properties
                .get("RunningMode")
                .and_then(Value::as_str)
            {
                if !mode.is_empty() {
                    running_mode = mode.to_string();
                }
            }
            let auto_stop_timeout = request_properties
                .get("RunningModeAutoStopTimeoutInMinutes")
                .map(py_truthy)
                .unwrap_or(false);
            if auto_stop_timeout && running_mode == "ALWAYS_ON" {
                error_code = "AutoStopTimeoutIsNotApplicableForAnAlwaysOnWorkspace".to_string();
                error_msg = "RunningModeAutoStopTimeoutInMinutes is not applicable for WorkSpace with running mode set to ALWAYS_ON.".to_string();
            }
            // Requests fail if AutoStopTimeout is given for a Manual one
            // (moto sets no message in that branch).
            if auto_stop_timeout && running_mode == "MANUAL" {
                error_code = "AutoStopTimeoutIsNotDefaultForManualWorkspace".to_string();
            }
        }

        // Stored WorkspaceProperties are the request's map when given
        // (with the resolved RunningMode), otherwise just RunningMode.
        let mut workspace_properties = request_properties;
        if py_truthy(&workspace_properties) {
            workspace_properties["RunningMode"] = json!(running_mode.clone());
        } else {
            workspace_properties = json!({ "RunningMode": running_mode });
        }

        let workspace = Workspace {
            workspace_id: format!("ws-{}", short_hex(9)),
            directory_id: directory_id.clone(),
            bundle_id: ws
                .get("BundleId")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            user_name: ws
                .get("UserName")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            state: "AVAILABLE".to_string(),
            error_code: error_code.clone(),
            error_message: error_msg,
            volume_encryption_key: ws
                .get("VolumeEncryptionKey")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            user_volume_encryption_enabled: ws
                .get("UserVolumeEncryptionEnabled")
                .cloned()
                .unwrap_or(json!("")),
            root_volume_encryption_enabled: ws
                .get("RootVolumeEncryptionEnabled")
                .cloned()
                .unwrap_or(json!("")),
            workspace_properties,
            tags: as_value_list(ws.get("Tags")),
            data_replication_settings: json!({}),
        };

        if error_code.is_empty() {
            pending_requests.push(workspace.to_dict_pending());
            let id = workspace.workspace_id.clone();
            st.workspaces.insert(id, workspace);
        } else {
            failed_requests.push(workspace.to_dict_failed());
        }
    }

    ok(json!({
        "FailedRequests": failed_requests,
        "PendingRequests": pending_requests,
    }))
}

fn describe_workspaces(st: &mut WorkspacesStore, body: &Value) -> Response {
    let workspace_ids = truthy_str_list(body.get("WorkspaceIds"));
    let directory_id = as_str(body, "DirectoryId");
    let user_name = as_str(body, "UserName");
    let bundle_id = as_str(body, "BundleId");

    // moto: only one of WorkspaceIds / DirectoryId / BundleId may be given.
    if (workspace_ids.is_some() && !directory_id.is_empty())
        || (!directory_id.is_empty() && !bundle_id.is_empty())
        || (workspace_ids.is_some() && !bundle_id.is_empty())
    {
        return invalid_params("An invalid number of parameters provided with DescribeWorkspaces. Only one of the following are allowed to be specified: BundleId, DirectoryId, WorkSpaceIds, Filters.");
    }
    // The directory is required when UserName is given.
    if !user_name.is_empty() && directory_id.is_empty() {
        return invalid_params("The DirectoryId parameter is required when UserName is used.");
    }

    let mut out: Vec<Value> = Vec::new();
    for ws in st.workspaces.values() {
        if let Some(ids) = &workspace_ids {
            if !ids.iter().any(|id| id == &ws.workspace_id) {
                continue;
            }
        }
        if !directory_id.is_empty() && ws.directory_id != directory_id {
            continue;
        }
        if !directory_id.is_empty() && !user_name.is_empty() && ws.user_name != user_name {
            continue;
        }
        if !bundle_id.is_empty() && ws.bundle_id != bundle_id {
            continue;
        }
        out.push(ws.to_dict_pending());
    }
    ok(json!({ "Workspaces": out }))
}

/// TerminateWorkspaces — moto reports `ErrorCode: "400"` for unknown ids.
fn terminate_workspaces(st: &mut WorkspacesStore, body: &Value) -> Response {
    let mut failed_requests: Vec<Value> = Vec::new();
    for entry in as_value_list(body.get("TerminateWorkspaceRequests")) {
        let workspace_id = entry
            .get("WorkspaceId")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if st.workspaces.remove(workspace_id).is_none() {
            failed_requests.push(json!({
                "WorkspaceId": workspace_id,
                "ErrorCode": "400",
                "ErrorMessage": format!("WorkSpace {workspace_id} could not be found."),
            }));
        }
    }
    ok(json!({ "FailedRequests": failed_requests }))
}

/// Start/Stop/Reboot/Rebuild share the same wire shape. moto reclamps the
/// state (Start→AVAILABLE, Stop→STOPPED) and leaves the other two alone;
/// unknown ids become FailedRequests entries.
fn state_workspaces(
    st: &mut WorkspacesStore,
    body: &Value,
    requests_key: &str,
    new_state: &str,
) -> Response {
    let mut failed_requests: Vec<Value> = Vec::new();
    for entry in as_value_list(body.get(requests_key)) {
        let workspace_id = entry
            .get("WorkspaceId")
            .and_then(Value::as_str)
            .unwrap_or_default();
        match st.workspaces.get_mut(workspace_id) {
            None => failed_requests.push(json!({
                "WorkspaceId": workspace_id,
                "ErrorCode": "ResourceNotFound.Workspace",
                "ErrorMessage": format!("WorkSpace {workspace_id} could not be found."),
            })),
            Some(ws) => {
                if !new_state.is_empty() {
                    ws.state = new_state.to_string();
                }
            }
        }
    }
    ok(json!({ "FailedRequests": failed_requests }))
}

fn modify_workspace_properties(st: &mut WorkspacesStore, body: &Value) -> Response {
    let workspace_id = as_str(body, "WorkspaceId");
    let Some(ws) = st.workspaces.get_mut(workspace_id) else {
        return not_found(&format!("The workspace {workspace_id} does not exist."));
    };
    if let Some(props) = body.get("WorkspaceProperties") {
        if let (Value::Object(dst), Value::Object(src)) = (&mut ws.workspace_properties, props) {
            for (k, v) in src {
                dst.insert(k.clone(), v.clone());
            }
        }
    }
    if let Some(mode) = body.get("DataReplication").and_then(Value::as_str) {
        if !mode.is_empty() {
            ws.data_replication_settings["DataReplication"] = json!(mode);
        }
    }
    ok_empty()
}

fn modify_workspace_state(st: &mut WorkspacesStore, body: &Value) -> Response {
    let workspace_id = as_str(body, "WorkspaceId");
    match st.workspaces.get_mut(workspace_id) {
        None => not_found(&format!("The workspace {workspace_id} does not exist.")),
        Some(ws) => {
            ws.state = as_str(body, "WorkspaceState").to_string();
            ok_empty()
        }
    }
}

fn restore_workspace(st: &mut WorkspacesStore, body: &Value) -> Response {
    let workspace_id = as_str(body, "WorkspaceId");
    if st.workspaces.contains_key(workspace_id) {
        // moto: the restored workspace stays AVAILABLE.
        ok_empty()
    } else {
        not_found(&format!("The workspace {workspace_id} does not exist."))
    }
}

fn migrate_workspace(st: &mut WorkspacesStore, body: &Value) -> Response {
    let source_workspace_id = as_str(body, "SourceWorkspaceId");
    if !st.workspaces.contains_key(source_workspace_id) {
        return not_found(&format!(
            "The workspace {source_workspace_id} does not exist."
        ));
    }
    // moto synthesizes the target id (without ever creating it).
    ok(json!({
        "SourceWorkspaceId": source_workspace_id,
        "TargetWorkspaceId": format!("ws-{}", short_hex(9)),
    }))
}

fn describe_workspaces_connection_status(st: &mut WorkspacesStore, body: &Value) -> Response {
    let ids = truthy_str_list(body.get("WorkspaceIds"));
    let statuses: Vec<Value> = st
        .workspaces
        .values()
        .filter(|ws| {
            ids.as_ref()
                .is_none_or(|list| list.iter().any(|id| id == &ws.workspace_id))
        })
        .map(|ws| {
            let now = now_unix();
            json!({
                "WorkspaceId": ws.workspace_id,
                "ConnectionState": "UNKNOWN",
                "ConnectionStateCheckTimestamp": now,
                "LastKnownUserConnectionTimestamp": now,
            })
        })
        .collect();
    ok(json!({ "WorkspacesConnectionStatus": statuses }))
}

fn describe_workspace_snapshots(st: &mut WorkspacesStore, body: &Value) -> Response {
    let workspace_id = as_str(body, "WorkspaceId");
    if !st.workspaces.contains_key(workspace_id) {
        return not_found(&format!("The workspace {workspace_id} does not exist."));
    }
    ok(json!({ "RebuildSnapshots": [], "RestoreSnapshots": [] }))
}

fn create_standby_workspaces(_st: &mut WorkspacesStore, body: &Value) -> Response {
    let mut pending: Vec<Value> = Vec::new();
    for sw in as_value_list(body.get("StandbyWorkspaces")) {
        pending.push(json!({
            "StandbyWorkspace": {
                "PrimaryWorkspaceId": sw
                    .get("PrimaryWorkspaceId")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
                "DirectoryId": sw
                    .get("DirectoryId")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            },
            "WorkspaceId": format!("ws-{}", short_hex(9)),
        }));
    }
    ok(json!({
        "FailedStandbyRequests": [],
        "PendingStandbyRequests": pending,
    }))
}

// ---------------------------------------------------------------------------
// Directories
// ---------------------------------------------------------------------------

fn describe_workspace_directories(st: &mut WorkspacesStore, body: &Value) -> Response {
    let directory_ids = truthy_str_list(body.get("DirectoryIds"));
    if let Some(ids) = &directory_ids {
        for id in ids {
            if !is_directory_id(id) {
                return validation_ex("The request is invalid.");
            }
        }
    }
    let mut directories: Vec<&WorkSpaceDirectory> = st.workspace_directories.values().collect();
    if let Some(ids) = &directory_ids {
        directories.retain(|d| ids.iter().any(|id| id == &d.directory_id));
    }
    directories.sort_by(|a, b| a.launch_time.partial_cmp(&b.launch_time).unwrap());
    ok(json!({
        "Directories": directories.iter().map(|d| d.to_dict()).collect::<Vec<_>>()
    }))
}

/// moto fetches the directory object from its Directory Service backend
/// (and creates an EC2 security group). Both in-process backends do not
/// exist here, so the directory is registered with synthetic defaults
/// (SimpleAD type, generated security group).
fn register_workspace_directory(req: &Request, st: &mut WorkspacesStore, body: &Value) -> Response {
    let directory = WorkSpaceDirectory::new(
        req.account,
        as_str(body, "DirectoryId"),
        as_str_list(body.get("SubnetIds")),
        py_truthy(body.get("EnableSelfService").unwrap_or(&Value::Null)),
        body.get("Tenancy").and_then(Value::as_str),
        as_value_list(body.get("Tags")),
    );
    st.workspace_directories
        .insert(directory.directory_id.clone(), directory);
    ok_empty()
}

fn deregister_workspace_directory(st: &mut WorkspacesStore, body: &Value) -> Response {
    let directory_id = as_str(body, "DirectoryId");
    if st.workspace_directories.remove(directory_id).is_some() {
        ok_empty()
    } else {
        // moto indexes `self.workspace_directories[directory_id]` — a
        // KeyError crash, recorded as InternalError in the probe.
        internal_error_key(directory_id)
    }
}

/// moto replaces the whole creation-properties map.
fn modify_workspace_creation_properties(st: &mut WorkspacesStore, body: &Value) -> Response {
    let resource_id = as_str(body, "ResourceId");
    match st.workspace_directories.get_mut(resource_id) {
        None => validation_ex("The request is invalid."),
        Some(dir) => {
            dir.workspace_creation_properties = body
                .get("WorkspaceCreationProperties")
                .cloned()
                .unwrap_or(Value::Null);
            ok_empty()
        }
    }
}

/// moto merges access properties into the directory's map.
fn modify_workspace_access_properties(st: &mut WorkspacesStore, body: &Value) -> Response {
    let resource_id = as_str(body, "ResourceId");
    let Some(dir) = st.workspace_directories.get_mut(resource_id) else {
        return not_found(&format!("The directory {resource_id} does not exist."));
    };
    if let (Some(dst), Some(Value::Object(src))) = (
        dir.workspace_access_properties.as_object_mut(),
        body.get("WorkspaceAccessProperties"),
    ) {
        for (k, v) in src {
            dst.insert(k.clone(), v.clone());
        }
    }
    ok_empty()
}

fn modify_saml_properties(st: &mut WorkspacesStore, body: &Value) -> Response {
    let resource_id = as_str(body, "ResourceId");
    let Some(dir) = st.workspace_directories.get_mut(resource_id) else {
        return not_found(&format!("The directory {resource_id} does not exist."));
    };
    let saml = body.get("SamlProperties");
    if saml.is_some_and(py_truthy) {
        if let (Value::Object(dst), Value::Object(src)) = (&mut dir.saml_properties, saml.unwrap())
        {
            for (k, v) in src {
                dst.insert(k.clone(), v.clone());
            }
        }
    }
    for prop in as_str_list(body.get("PropertiesToDelete")) {
        match prop.as_str() {
            "SAML_PROPERTIES_USER_ACCESS_URL" => {
                dir.saml_properties["UserAccessUrl"] = json!("");
            }
            "SAML_PROPERTIES_RELAY_STATE_PARAMETER_NAME" => {
                dir.saml_properties["RelayStateParameterName"] = json!("");
            }
            _ => {}
        }
    }
    ok_empty()
}

fn modify_certificate_based_auth_properties(st: &mut WorkspacesStore, body: &Value) -> Response {
    let resource_id = as_str(body, "ResourceId");
    let Some(dir) = st.workspace_directories.get_mut(resource_id) else {
        return not_found(&format!("The directory {resource_id} does not exist."));
    };
    let props = body.get("CertificateBasedAuthProperties");
    if props.is_some_and(py_truthy) {
        if let (Value::Object(dst), Value::Object(src)) =
            (&mut dir.certificate_based_auth_properties, props.unwrap())
        {
            for (k, v) in src {
                dst.insert(k.clone(), v.clone());
            }
        }
    }
    for prop in as_str_list(body.get("PropertiesToDelete")) {
        if prop == "CERTIFICATE_BASED_AUTH_PROPERTIES_CERTIFICATE_AUTHORITY_ARN" {
            if let Value::Object(obj) = &mut dir.certificate_based_auth_properties {
                obj.remove("CertificateAuthorityArn");
            }
        }
    }
    ok_empty()
}

fn modify_streaming_properties(st: &mut WorkspacesStore, body: &Value) -> Response {
    let resource_id = as_str(body, "ResourceId");
    let Some(dir) = st.workspace_directories.get_mut(resource_id) else {
        return not_found(&format!("The directory {resource_id} does not exist."));
    };
    if let Some(props) = body.get("StreamingProperties") {
        dir.streaming_properties = props.clone();
    }
    ok_empty()
}

fn modify_endpoint_encryption_mode(st: &mut WorkspacesStore, body: &Value) -> Response {
    let directory_id = as_str(body, "DirectoryId");
    match st.workspace_directories.get_mut(directory_id) {
        None => not_found(&format!("The directory {directory_id} does not exist.")),
        Some(dir) => {
            dir.endpoint_encryption_mode = as_str(body, "EndpointEncryptionMode").to_string();
            ok_empty()
        }
    }
}

fn modify_selfservice_permissions(st: &mut WorkspacesStore, body: &Value) -> Response {
    let resource_id = as_str(body, "ResourceId");
    match st.workspace_directories.get_mut(resource_id) {
        None => internal_error_key(resource_id),
        Some(dir) => {
            dir.self_service_permissions = body
                .get("SelfservicePermissions")
                .cloned()
                .unwrap_or(Value::Null);
            ok_empty()
        }
    }
}

fn modify_client_properties(st: &mut WorkspacesStore, body: &Value) -> Response {
    let resource_id = as_str(body, "ResourceId");
    match st.workspace_directories.get_mut(resource_id) {
        None => internal_error_key(resource_id),
        Some(dir) => {
            dir.client_properties = body.get("ClientProperties").cloned().unwrap_or(Value::Null);
            ok_empty()
        }
    }
}

fn describe_client_properties(st: &mut WorkspacesStore, body: &Value) -> Response {
    let resource_ids = as_str_list(body.get("ResourceIds"));
    let list: Vec<Value> = st
        .workspace_directories
        .values()
        .filter(|d| resource_ids.iter().any(|id| id == &d.directory_id))
        .map(|d| {
            json!({
                "ResourceId": d.directory_id,
                "ClientProperties": d.client_properties,
            })
        })
        .collect();
    ok(json!({ "ClientPropertiesList": list }))
}

// ---------------------------------------------------------------------------
// Tags (moto routes every resource type by id prefix)
// ---------------------------------------------------------------------------

fn tags_for_resource_mut<'a>(st: &'a mut WorkspacesStore, id: &str) -> Option<&'a mut Vec<Value>> {
    if id.starts_with("d-") {
        st.workspace_directories.get_mut(id).map(|d| &mut d.tags)
    } else if id.starts_with("ws-") {
        st.workspaces.get_mut(id).map(|w| &mut w.tags)
    } else if id.starts_with("wsi-") {
        st.workspace_images.get_mut(id).map(|i| &mut i.tags)
    } else if id.starts_with("wsipg-") {
        st.ip_groups.get_mut(id).map(|g| &mut g.tags)
    } else if id.starts_with("wsca-") {
        st.connection_aliases.get_mut(id).map(|a| &mut a.tags)
    } else if id.starts_with("wsb-") {
        st.workspace_bundles.get_mut(id).map(|b| &mut b.tags)
    } else if id.starts_with("wsp-") {
        st.workspaces_pools.get_mut(id).map(|p| &mut p.tags)
    } else {
        None
    }
}

fn tags_for_resource(st: &WorkspacesStore, id: &str) -> Vec<Value> {
    if id.starts_with("d-") {
        st.workspace_directories
            .get(id)
            .map(|d| d.tags.clone())
            .unwrap_or_default()
    } else if id.starts_with("ws-") {
        st.workspaces
            .get(id)
            .map(|w| w.tags.clone())
            .unwrap_or_default()
    } else if id.starts_with("wsi-") {
        st.workspace_images
            .get(id)
            .map(|i| i.tags.clone())
            .unwrap_or_default()
    } else if id.starts_with("wsipg-") {
        st.ip_groups
            .get(id)
            .map(|g| g.tags.clone())
            .unwrap_or_default()
    } else if id.starts_with("wsca-") {
        st.connection_aliases
            .get(id)
            .map(|a| a.tags.clone())
            .unwrap_or_default()
    } else if id.starts_with("wsb-") {
        st.workspace_bundles
            .get(id)
            .map(|b| b.tags.clone())
            .unwrap_or_default()
    } else if id.starts_with("wsp-") {
        st.workspaces_pools
            .get(id)
            .map(|p| p.tags.clone())
            .unwrap_or_default()
    } else {
        Vec::new()
    }
}

/// moto indexes the resource for the matching prefix — unknown ids with a
/// known prefix crash (`InternalError`); unknown prefixes fall through
/// every branch and answer `{}`.
fn create_tags(st: &mut WorkspacesStore, body: &Value) -> Response {
    let resource_id = as_str(body, "ResourceId");
    match tags_for_resource_mut(st, resource_id) {
        Some(tags) => {
            tags.extend(as_value_list(body.get("Tags")));
            ok_empty()
        }
        None => {
            if has_resource_prefix(resource_id) {
                internal_error_key(resource_id)
            } else {
                ok_empty()
            }
        }
    }
}

fn delete_tags(st: &mut WorkspacesStore, body: &Value) -> Response {
    let resource_id = as_str(body, "ResourceId");
    let tag_keys = as_str_list(body.get("TagKeys"));
    if let Some(tags) = tags_for_resource_mut(st, resource_id) {
        tags.retain(|t| !matches_with_keys(t, &tag_keys));
    }
    ok_empty()
}

fn describe_tags(st: &mut WorkspacesStore, body: &Value) -> Response {
    let resource_id = as_str(body, "ResourceId");
    // AWS answers empty tags (not an error) for nonexistent resources.
    ok(json!({ "TagList": tags_for_resource(st, resource_id) }))
}

fn matches_with_keys(tag: &Value, keys: &[String]) -> bool {
    tag.get("Key")
        .and_then(Value::as_str)
        .map(|k| keys.contains(&k.to_string()))
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Images
// ---------------------------------------------------------------------------

fn create_workspace_image(req: &Request, st: &mut WorkspacesStore, body: &Value) -> Response {
    let workspace_id = as_str(body, "WorkspaceId");
    if !st.workspaces.contains_key(workspace_id) {
        return not_found(
            "The specified WorkSpace cannot be found. Confirm that the workspace exists in your AWS account, and try again.",
        );
    }
    let name = as_str(body, "Name");
    if st.workspace_images.values().any(|i| i.name == name) {
        return already_exists(
            "A WorkSpace image with the same name exists in the destination Region. Provide a unique destination image name, and try again.",
        );
    }
    let image = new_workspace_image(req, name, as_str(body, "Description"), body);
    let out = image.to_dict();
    let id = image.image_id.clone();
    st.workspace_images.insert(id, image);
    ok(out)
}

fn new_workspace_image(
    account_holder: &Request,
    name: &str,
    description: &str,
    body: &Value,
) -> WorkspaceImage {
    WorkspaceImage {
        image_id: format!("wsi-{}", short_hex(9)),
        name: name.to_string(),
        description: description.to_string(),
        state: "AVAILABLE".to_string(),
        required_tenancy: "DEFAULT".to_string(),
        created: now_unix(),
        owner_account: account_holder.account.to_string(),
        image_permissions: Vec::new(),
        tags: as_value_list(body.get("Tags")),
    }
}

fn describe_workspace_images(req: &Request, st: &mut WorkspacesStore, body: &Value) -> Response {
    let image_ids = truthy_str_list(body.get("ImageIds"));
    let image_type = as_str(body, "ImageType");
    let account = req.account.to_string();
    let images: Vec<Value> = st
        .workspace_images
        .values()
        .filter(|i| {
            if image_type == "OWNED" {
                i.owner_account == account
            } else if image_type == "SHARED" {
                i.owner_account != account
            } else {
                true
            }
        })
        .filter(|i| {
            image_ids
                .as_ref()
                .is_none_or(|ids| ids.iter().any(|id| id == &i.image_id))
        })
        .map(WorkspaceImage::to_desc_dict)
        .collect();
    ok(json!({ "Images": images }))
}

fn update_workspace_image_permission(st: &mut WorkspacesStore, body: &Value) -> Response {
    let image_id = as_str(body, "ImageId");
    let Some(image) = st.workspace_images.get_mut(image_id) else {
        // moto indexes `self.workspace_images[image_id]` — crash as
        // recorded in the probe.
        return internal_error_key(image_id);
    };
    let shared_account_id = as_str(body, "SharedAccountId");
    let allow_copy_image = body.get("AllowCopyImage").and_then(Value::as_bool);
    let already = image
        .image_permissions
        .iter()
        .any(|p| p.get("SharedAccountId").and_then(Value::as_str) == Some(shared_account_id));
    if !already && allow_copy_image.unwrap_or(false) {
        image
            .image_permissions
            .push(json!({ "SharedAccountId": shared_account_id }));
    }
    if already && !allow_copy_image.unwrap_or(false) {
        image.image_permissions.retain(|p| {
            p.get("SharedAccountId").and_then(Value::as_str) != Some(shared_account_id)
        });
    }
    ok_empty()
}

fn describe_workspace_image_permissions(st: &mut WorkspacesStore, body: &Value) -> Response {
    let image_id = as_str(body, "ImageId");
    // moto validates the id shape before checking existence.
    if !is_image_id(image_id) {
        return validation_ex(&format!(
            "The Image ID {image_id} in the request is invalid"
        ));
    }
    let permissions = st
        .workspace_images
        .get(image_id)
        .map(|i| i.image_permissions.clone())
        .unwrap_or_default();
    ok(json!({
        "ImageId": image_id,
        "ImagePermissions": permissions,
    }))
}

fn copy_workspace_image(req: &Request, st: &mut WorkspacesStore, body: &Value) -> Response {
    // moto creates the copy without validating the source image.
    let image = new_workspace_image(req, as_str(body, "Name"), as_str(body, "Description"), body);
    let image_id = image.image_id.clone();
    st.workspace_images.insert(image_id.clone(), image);
    ok(json!({ "ImageId": image_id }))
}

fn delete_workspace_image(st: &mut WorkspacesStore, body: &Value) -> Response {
    // moto pops silently — unknown image ids are not an error.
    st.workspace_images.remove(as_str(body, "ImageId"));
    ok_empty()
}

fn create_updated_workspace_image(
    req: &Request,
    st: &mut WorkspacesStore,
    body: &Value,
) -> Response {
    let image = new_workspace_image(req, as_str(body, "Name"), as_str(body, "Description"), body);
    let image_id = image.image_id.clone();
    st.workspace_images.insert(image_id.clone(), image);
    ok(json!({ "ImageId": image_id }))
}

fn import_workspace_image(req: &Request, st: &mut WorkspacesStore, body: &Value) -> Response {
    let image = new_workspace_image(
        req,
        as_str(body, "ImageName"),
        as_str(body, "ImageDescription"),
        body,
    );
    let image_id = image.image_id.clone();
    st.workspace_images.insert(image_id.clone(), image);
    ok(json!({ "ImageId": image_id }))
}

fn import_custom_workspace_image(
    req: &Request,
    st: &mut WorkspacesStore,
    body: &Value,
) -> Response {
    let image = new_workspace_image(
        req,
        as_str(body, "ImageName"),
        as_str(body, "ImageDescription"),
        body,
    );
    let state = image.state.clone();
    let image_id = image.image_id.clone();
    st.workspace_images.insert(image_id.clone(), image);
    ok(json!({ "ImageId": image_id, "State": state }))
}

fn describe_custom_workspace_image_import(st: &mut WorkspacesStore, body: &Value) -> Response {
    let image_id = as_str(body, "ImageId");
    match st.workspace_images.get(image_id) {
        Some(image) => ok(json!({
            "ImageId": image.image_id,
            "State": image.state,
            "Created": image.created,
        })),
        None => not_found(&format!("The workspace image {image_id} does not exist.")),
    }
}

// ---------------------------------------------------------------------------
// Bundles
// ---------------------------------------------------------------------------

fn create_workspace_bundle(req: &Request, st: &mut WorkspacesStore, body: &Value) -> Response {
    let bundle = WorkspaceBundle {
        bundle_id: format!("wsb-{}", short_hex(9)),
        name: as_str(body, "BundleName").to_string(),
        description: as_str(body, "BundleDescription").to_string(),
        image_id: as_str(body, "ImageId").to_string(),
        compute_type: body.get("ComputeType").cloned().unwrap_or(json!({})),
        user_storage: body.get("UserStorage").cloned().unwrap_or(json!({})),
        root_storage: body.get("RootStorage").cloned().unwrap_or(json!({})),
        owner: req.account.to_string(),
        state: "AVAILABLE".to_string(),
        creation_time: now_unix(),
        last_updated_time: now_unix(),
        tags: as_value_list(body.get("Tags")),
    };
    let out = bundle.to_dict();
    let id = bundle.bundle_id.clone();
    st.workspace_bundles.insert(id, bundle);
    ok(json!({ "WorkspaceBundle": out }))
}

fn describe_workspace_bundles(st: &mut WorkspacesStore, body: &Value) -> Response {
    let bundle_ids = truthy_str_list(body.get("BundleIds"));
    let owner = as_str(body, "Owner");
    let bundles: Vec<Value> = st
        .workspace_bundles
        .values()
        .filter(|b| {
            bundle_ids
                .as_ref()
                .is_none_or(|ids| ids.iter().any(|id| id == &b.bundle_id))
        })
        .filter(|b| owner.is_empty() || b.owner == owner)
        .map(WorkspaceBundle::to_dict)
        .collect();
    ok(json!({ "Bundles": bundles }))
}

fn delete_workspace_bundle(st: &mut WorkspacesStore, body: &Value) -> Response {
    // moto: `if bundle_id and bundle_id in bundles` — otherwise silent.
    let bundle_id = as_str(body, "BundleId");
    if !bundle_id.is_empty() {
        st.workspace_bundles.remove(bundle_id);
    }
    ok_empty()
}

fn update_workspace_bundle(st: &mut WorkspacesStore, body: &Value) -> Response {
    let bundle_id = as_str(body, "BundleId");
    let image_id = as_str(body, "ImageId");
    match st.workspace_bundles.get_mut(bundle_id) {
        None => not_found(&format!("The workspace bundle {bundle_id} does not exist.")),
        Some(bundle) => {
            if !image_id.is_empty() {
                bundle.image_id = image_id.to_string();
                bundle.last_updated_time = now_unix();
            }
            ok_empty()
        }
    }
}

// ---------------------------------------------------------------------------
// IP groups
// ---------------------------------------------------------------------------

fn create_ip_group(st: &mut WorkspacesStore, body: &Value) -> Response {
    let group = IpGroup {
        group_id: format!("wsipg-{}", short_hex(9)),
        group_name: as_str(body, "GroupName").to_string(),
        group_desc: as_str(body, "GroupDesc").to_string(),
        user_rules: as_value_list(body.get("UserRules")),
        tags: as_value_list(body.get("Tags")),
    };
    let id = group.group_id.clone();
    st.ip_groups.insert(id.clone(), group);
    ok(json!({ "GroupId": id }))
}

fn describe_ip_groups(st: &mut WorkspacesStore, body: &Value) -> Response {
    let group_ids = truthy_str_list(body.get("GroupIds"));
    let groups: Vec<Value> = st
        .ip_groups
        .values()
        .filter(|g| {
            group_ids
                .as_ref()
                .is_none_or(|ids| ids.iter().any(|id| id == &g.group_id))
        })
        .map(IpGroup::to_dict)
        .collect();
    ok(json!({ "Result": groups }))
}

fn delete_ip_group(st: &mut WorkspacesStore, body: &Value) -> Response {
    let group_id = as_str(body, "GroupId");
    if st.ip_groups.contains_key(group_id) {
        // Also disassociate from any directory holding the group.
        for dir in st.workspace_directories.values_mut() {
            dir.ip_group_ids.retain(|id| id != group_id);
        }
        st.ip_groups.remove(group_id);
        ok_empty()
    } else {
        not_found(&format!("The IP group {group_id} does not exist."))
    }
}

fn authorize_ip_rules(st: &mut WorkspacesStore, body: &Value) -> Response {
    let group_id = as_str(body, "GroupId");
    match st.ip_groups.get_mut(group_id) {
        None => not_found(&format!("The IP group {group_id} does not exist.")),
        Some(group) => {
            group
                .user_rules
                .extend(as_value_list(body.get("UserRules")));
            ok_empty()
        }
    }
}

fn revoke_ip_rules(st: &mut WorkspacesStore, body: &Value) -> Response {
    let group_id = as_str(body, "GroupId");
    match st.ip_groups.get_mut(group_id) {
        None => not_found(&format!("The IP group {group_id} does not exist.")),
        Some(group) => {
            let revoked = as_str_list(body.get("UserRules"));
            group.user_rules.retain(|r| {
                !r.get("ipRule")
                    .and_then(Value::as_str)
                    .map(|rule| revoked.contains(&rule.to_string()))
                    .unwrap_or(false)
            });
            ok_empty()
        }
    }
}

fn update_rules_of_ip_group(st: &mut WorkspacesStore, body: &Value) -> Response {
    let group_id = as_str(body, "GroupId");
    match st.ip_groups.get_mut(group_id) {
        None => not_found(&format!("The IP group {group_id} does not exist.")),
        Some(group) => {
            // The new rules list replaces the stored one.
            group.user_rules = as_value_list(body.get("UserRules"));
            ok_empty()
        }
    }
}

fn associate_ip_groups(st: &mut WorkspacesStore, body: &Value) -> Response {
    let directory_id = as_str(body, "DirectoryId");
    let Some(dir) = st.workspace_directories.get_mut(directory_id) else {
        return not_found(&format!("The directory {directory_id} does not exist."));
    };
    for gid in as_str_list(body.get("GroupIds")) {
        if !st.ip_groups.contains_key(&gid) {
            return not_found(&format!("The IP group {gid} does not exist."));
        }
        if !dir.ip_group_ids.contains(&gid) {
            dir.ip_group_ids.push(gid);
        }
    }
    ok_empty()
}

fn disassociate_ip_groups(st: &mut WorkspacesStore, body: &Value) -> Response {
    let directory_id = as_str(body, "DirectoryId");
    let Some(dir) = st.workspace_directories.get_mut(directory_id) else {
        return not_found(&format!("The directory {directory_id} does not exist."));
    };
    let group_ids = as_str_list(body.get("GroupIds"));
    dir.ip_group_ids.retain(|id| !group_ids.contains(id));
    ok_empty()
}

// ---------------------------------------------------------------------------
// Connection aliases
// ---------------------------------------------------------------------------

fn create_connection_alias(req: &Request, st: &mut WorkspacesStore, body: &Value) -> Response {
    let alias = ConnectionAlias {
        alias_id: format!("wsca-{}", short_hex(9)),
        connection_string: as_str(body, "ConnectionString").to_string(),
        state: "CREATED".to_string(),
        owner_account_id: req.account.to_string(),
        associations: Vec::new(),
        permissions: Vec::new(),
        tags: as_value_list(body.get("Tags")),
    };
    let id = alias.alias_id.clone();
    st.connection_aliases.insert(id.clone(), alias);
    ok(json!({ "AliasId": id }))
}

fn describe_connection_aliases(st: &mut WorkspacesStore, body: &Value) -> Response {
    let alias_ids = truthy_str_list(body.get("AliasIds"));
    let resource_id = as_str(body, "ResourceId");
    let aliases: Vec<Value> = st
        .connection_aliases
        .values()
        .filter(|a| {
            alias_ids
                .as_ref()
                .is_none_or(|ids| ids.iter().any(|id| id == &a.alias_id))
        })
        .filter(|a| {
            resource_id.is_empty()
                || a.associations.iter().any(|assoc| {
                    assoc.get("ResourceId").and_then(Value::as_str) == Some(resource_id)
                })
        })
        .map(ConnectionAlias::to_dict)
        .collect();
    ok(json!({ "ConnectionAliases": aliases }))
}

fn delete_connection_alias(st: &mut WorkspacesStore, body: &Value) -> Response {
    let alias_id = as_str(body, "AliasId");
    if st.connection_aliases.remove(alias_id).is_some() {
        ok_empty()
    } else {
        not_found(&format!("The connection alias {alias_id} does not exist."))
    }
}

fn associate_connection_alias(req: &Request, st: &mut WorkspacesStore, body: &Value) -> Response {
    let alias_id = as_str(body, "AliasId");
    let Some(alias) = st.connection_aliases.get_mut(alias_id) else {
        return not_found(&format!("The connection alias {alias_id} does not exist."));
    };
    let connection_id = format!("wsci-{}", short_hex(9));
    alias.associations.push(json!({
        "AssociatedAccountId": req.account.to_string(),
        "ResourceId": as_str(body, "ResourceId"),
        "ConnectionIdentifier": connection_id,
        "AssociationStatus": "ASSOCIATED",
    }));
    ok(json!({ "ConnectionIdentifier": connection_id }))
}

fn disassociate_connection_alias(st: &mut WorkspacesStore, body: &Value) -> Response {
    let alias_id = as_str(body, "AliasId");
    match st.connection_aliases.get_mut(alias_id) {
        None => not_found(&format!("The connection alias {alias_id} does not exist.")),
        Some(alias) => {
            alias.associations.clear();
            ok_empty()
        }
    }
}

fn describe_connection_alias_permissions(st: &mut WorkspacesStore, body: &Value) -> Response {
    let alias_id = as_str(body, "AliasId");
    match st.connection_aliases.get(alias_id) {
        None => not_found(&format!("The connection alias {alias_id} does not exist.")),
        Some(alias) => ok(json!({
            "AliasId": alias_id,
            "ConnectionAliasPermissions": alias.permissions,
        })),
    }
}

fn update_connection_alias_permission(st: &mut WorkspacesStore, body: &Value) -> Response {
    let alias_id = as_str(body, "AliasId");
    let Some(alias) = st.connection_aliases.get_mut(alias_id) else {
        return not_found(&format!("The connection alias {alias_id} does not exist."));
    };
    let permission = body
        .get("ConnectionAliasPermission")
        .cloned()
        .unwrap_or(json!({}));
    let shared_account_id = permission
        .get("SharedAccountId")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let allow_association = permission
        .get("AllowAssociation")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if allow_association {
        let exists = alias
            .permissions
            .iter()
            .any(|p| p.get("SharedAccountId").and_then(Value::as_str) == Some(shared_account_id));
        if !exists {
            alias.permissions.push(permission);
        }
    } else {
        alias.permissions.retain(|p| {
            p.get("SharedAccountId").and_then(Value::as_str) != Some(shared_account_id)
        });
    }
    ok_empty()
}

// ---------------------------------------------------------------------------
// Connect client add-ins
// ---------------------------------------------------------------------------

fn create_connect_client_add_in(st: &mut WorkspacesStore, body: &Value) -> Response {
    // moto reuses the wsca- prefix here — kept as-is.
    let add_in = ConnectClientAddIn {
        add_in_id: format!("wsca-{}", short_hex(9)),
        resource_id: as_str(body, "ResourceId").to_string(),
        name: as_str(body, "Name").to_string(),
        url: as_str(body, "URL").to_string(),
    };
    let id = add_in.add_in_id.clone();
    st.connect_client_add_ins.insert(id.clone(), add_in);
    ok(json!({ "AddInId": id }))
}

fn describe_connect_client_add_ins(st: &mut WorkspacesStore, body: &Value) -> Response {
    let resource_id = as_str(body, "ResourceId");
    let add_ins: Vec<Value> = st
        .connect_client_add_ins
        .values()
        .filter(|a| a.resource_id == resource_id)
        .map(ConnectClientAddIn::to_dict)
        .collect();
    ok(json!({ "AddIns": add_ins }))
}

fn delete_connect_client_add_in(st: &mut WorkspacesStore, body: &Value) -> Response {
    // moto pops silently — no error for unknown add-ins.
    st.connect_client_add_ins.remove(as_str(body, "AddInId"));
    ok_empty()
}

fn update_connect_client_add_in(st: &mut WorkspacesStore, body: &Value) -> Response {
    let add_in_id = as_str(body, "AddInId");
    match st.connect_client_add_ins.get_mut(add_in_id) {
        None => not_found(&format!("The add-in {add_in_id} does not exist.")),
        Some(add_in) => {
            let name = as_str(body, "Name");
            if !name.is_empty() {
                add_in.name = name.to_string();
            }
            let url = as_str(body, "URL");
            if !url.is_empty() {
                add_in.url = url.to_string();
            }
            ok_empty()
        }
    }
}

// ---------------------------------------------------------------------------
// Account links
// ---------------------------------------------------------------------------

fn create_account_link_invitation(
    req: &Request,
    st: &mut WorkspacesStore,
    body: &Value,
) -> Response {
    let link = AccountLink {
        link_id: format!("wsal-{}", short_hex(9)),
        link_status: "PENDING_ACCEPTANCE".to_string(),
        source_account_id: req.account.to_string(),
        target_account_id: as_str(body, "TargetAccountId").to_string(),
    };
    let out = link.to_dict();
    let id = link.link_id.clone();
    st.account_links.insert(id, link);
    ok(json!({ "AccountLink": out }))
}

fn transition_account_link(st: &mut WorkspacesStore, body: &Value, status: &str) -> Response {
    let link_id = as_str(body, "LinkId");
    match st.account_links.get_mut(link_id) {
        None => not_found(&format!("The account link {link_id} does not exist.")),
        Some(link) => {
            link.link_status = status.to_string();
            ok(json!({ "AccountLink": link.to_dict() }))
        }
    }
}

fn delete_account_link_invitation(st: &mut WorkspacesStore, body: &Value) -> Response {
    let link_id = as_str(body, "LinkId");
    match st.account_links.remove(link_id) {
        None => not_found(&format!("The account link {link_id} does not exist.")),
        Some(link) => {
            let out = AccountLink {
                link_status: "DELETED".to_string(),
                ..link
            };
            ok(json!({ "AccountLink": out.to_dict() }))
        }
    }
}

fn get_account_link(st: &mut WorkspacesStore, body: &Value) -> Response {
    let link_id = as_str(body, "LinkId");
    if !link_id.is_empty() {
        return match st.account_links.get(link_id) {
            Some(link) => ok(json!({ "AccountLink": link.to_dict() })),
            None => not_found(&format!("The account link {link_id} does not exist.")),
        };
    }
    let linked_account_id = as_str(body, "LinkedAccountId");
    if !linked_account_id.is_empty() {
        for link in st.account_links.values() {
            if link.target_account_id == linked_account_id
                || link.source_account_id == linked_account_id
            {
                return ok(json!({ "AccountLink": link.to_dict() }));
            }
        }
    }
    not_found("The account link does not exist.")
}

fn list_account_links(st: &mut WorkspacesStore, body: &Value) -> Response {
    let status_filter = as_str_list(body.get("LinkStatusFilter"));
    let links: Vec<Value> = if status_filter.is_empty() {
        st.account_links
            .values()
            .map(AccountLink::to_dict)
            .collect()
    } else {
        st.account_links
            .values()
            .filter(|link| status_filter.contains(&link.link_status))
            .map(AccountLink::to_dict)
            .collect()
    };
    ok(json!({ "AccountLinks": links }))
}

// ---------------------------------------------------------------------------
// Pools
// ---------------------------------------------------------------------------

fn create_workspaces_pool(req: &Request, st: &mut WorkspacesStore, body: &Value) -> Response {
    let pool_id = format!("wsp-{}", short_hex(9));
    let pool = WorkspacesPool {
        pool_id: pool_id.clone(),
        pool_name: as_str(body, "PoolName").to_string(),
        description: as_str(body, "Description").to_string(),
        bundle_id: as_str(body, "BundleId").to_string(),
        directory_id: as_str(body, "DirectoryId").to_string(),
        capacity: body.get("Capacity").filter(|c| !c.is_null()).cloned(),
        application_settings: body
            .get("ApplicationSettings")
            .filter(|v| !v.is_null())
            .cloned(),
        timeout_settings: body
            .get("TimeoutSettings")
            .filter(|v| !v.is_null())
            .cloned(),
        running_mode: match body.get("RunningMode").and_then(Value::as_str) {
            Some(mode) if !mode.is_empty() => mode.to_string(),
            _ => "AUTO_STOP".to_string(),
        },
        state: "AVAILABLE".to_string(),
        created_at: now_unix(),
        pool_arn: format!(
            "arn:aws:workspaces:{}:{}:workspacespool/{pool_id}",
            req.region, req.account
        ),
        tags: as_value_list(body.get("Tags")),
    };
    let out = pool.to_dict();
    st.workspaces_pools.insert(pool_id, pool);
    ok(json!({ "WorkspacesPool": out }))
}

fn describe_workspaces_pools(st: &mut WorkspacesStore, body: &Value) -> Response {
    let pool_ids = truthy_str_list(body.get("PoolIds"));
    let pools: Vec<Value> = st
        .workspaces_pools
        .values()
        .filter(|p| {
            pool_ids
                .as_ref()
                .is_none_or(|ids| ids.iter().any(|id| id == &p.pool_id))
        })
        .map(WorkspacesPool::to_dict)
        .collect();
    ok(json!({ "WorkspacesPools": pools }))
}

fn update_workspaces_pool(st: &mut WorkspacesStore, body: &Value) -> Response {
    let pool_id = as_str(body, "PoolId");
    let Some(pool) = st.workspaces_pools.get_mut(pool_id) else {
        return not_found(&format!("The workspaces pool {pool_id} does not exist."));
    };
    // moto assigns each field that is present (not None).
    if let Some(v) = body.get("Description") {
        pool.description = v.as_str().unwrap_or_default().to_string();
    }
    if let Some(v) = body.get("BundleId").and_then(Value::as_str) {
        pool.bundle_id = v.to_string();
    }
    if let Some(v) = body.get("DirectoryId").and_then(Value::as_str) {
        pool.directory_id = v.to_string();
    }
    if let Some(v) = body.get("Capacity").filter(|c| !c.is_null()) {
        pool.capacity = Some(v.clone());
    }
    if let Some(v) = body.get("ApplicationSettings").filter(|v| !v.is_null()) {
        pool.application_settings = Some(v.clone());
    }
    if let Some(v) = body.get("TimeoutSettings").filter(|v| !v.is_null()) {
        pool.timeout_settings = Some(v.clone());
    }
    if let Some(v) = body.get("RunningMode").and_then(Value::as_str) {
        pool.running_mode = v.to_string();
    }
    ok(json!({ "WorkspacesPool": pool.to_dict() }))
}

fn terminate_workspaces_pool(st: &mut WorkspacesStore, body: &Value) -> Response {
    let pool_id = as_str(body, "PoolId");
    if st.workspaces_pools.remove(pool_id).is_some() {
        ok_empty()
    } else {
        not_found(&format!("The workspaces pool {pool_id} does not exist."))
    }
}

fn pool_lifecycle(st: &mut WorkspacesStore, body: &Value, state: &str) -> Response {
    let pool_id = as_str(body, "PoolId");
    match st.workspaces_pools.get_mut(pool_id) {
        None => not_found(&format!("The workspaces pool {pool_id} does not exist.")),
        Some(pool) => {
            pool.state = state.to_string();
            ok_empty()
        }
    }
}

fn describe_workspaces_pool_sessions(st: &mut WorkspacesStore, body: &Value) -> Response {
    let pool_id = as_str(body, "PoolId");
    if !st.workspaces_pools.contains_key(pool_id) {
        return not_found(&format!("The workspaces pool {pool_id} does not exist."));
    }
    // moto tracks no sessions, so the list is always empty.
    ok(json!({ "Sessions": [] }))
}

fn terminate_workspaces_pool_session() -> Response {
    // moto loops every pool and drops the session if found — no error path.
    ok_empty()
}

// ---------------------------------------------------------------------------
// Account
// ---------------------------------------------------------------------------

fn describe_account(st: &mut WorkspacesStore) -> Response {
    ok(json!({
        "DedicatedTenancySupport": if st.dedicated_tenancy_support.is_empty() {
            "DISABLED"
        } else {
            st.dedicated_tenancy_support.as_str()
        },
        "DedicatedTenancyManagementCidrRange": st.dedicated_tenancy_management_cidr_range,
    }))
}

fn modify_account(st: &mut WorkspacesStore, body: &Value) -> Response {
    let support = as_str(body, "DedicatedTenancySupport");
    if !support.is_empty() {
        st.dedicated_tenancy_support = support.to_string();
    }
    let cidr = as_str(body, "DedicatedTenancyManagementCidrRange");
    if !cidr.is_empty() {
        st.dedicated_tenancy_management_cidr_range = cidr.to_string();
    }
    st.account_modifications.push(json!({
        "ModificationState": "COMPLETED",
        "DedicatedTenancySupport": st.dedicated_tenancy_support,
        "DedicatedTenancyManagementCidrRange": st.dedicated_tenancy_management_cidr_range,
        "StartTime": now_unix(),
    }));
    ok_empty()
}

fn describe_account_modifications(st: &mut WorkspacesStore) -> Response {
    ok(json!({ "AccountModifications": st.account_modifications }))
}

// ---------------------------------------------------------------------------
// Client branding
// ---------------------------------------------------------------------------

/// moto merges the imported branding into the directory and echoes the
/// imported subset back.
fn import_client_branding(st: &mut WorkspacesStore, body: &Value) -> Response {
    let resource_id = as_str(body, "ResourceId");
    let Some(dir) = st.workspace_directories.get_mut(resource_id) else {
        return not_found(&format!("The directory {resource_id} does not exist."));
    };
    let mut imported = Map::new();
    for key in [
        "DeviceTypeWindows",
        "DeviceTypeOsx",
        "DeviceTypeAndroid",
        "DeviceTypeIos",
        "DeviceTypeLinux",
        "DeviceTypeWeb",
    ] {
        if let Some(v) = body.get(key) {
            imported.insert(key.to_string(), v.clone());
        }
    }
    let imported_value = Value::Object(imported);
    let merged = match dir.client_branding.take() {
        Some(existing) => merge_objects(existing, &imported_value),
        None => imported_value.clone(),
    };
    dir.client_branding = Some(merged);
    ok(imported_value)
}

fn describe_client_branding(st: &mut WorkspacesStore, body: &Value) -> Response {
    let resource_id = as_str(body, "ResourceId");
    match st.workspace_directories.get(resource_id) {
        None => not_found(&format!("The directory {resource_id} does not exist.")),
        Some(dir) => ok(dir.client_branding.clone().unwrap_or(json!({}))),
    }
}

fn delete_client_branding(st: &mut WorkspacesStore, body: &Value) -> Response {
    let resource_id = as_str(body, "ResourceId");
    let Some(dir) = st.workspace_directories.get_mut(resource_id) else {
        return not_found(&format!("The directory {resource_id} does not exist."));
    };
    for platform in as_str_list(body.get("Platforms")) {
        // "DeviceTypeWindows" and "Windows" both target DeviceTypeWindows.
        let key = format!("DeviceType{}", platform.replace("DeviceType", ""));
        if let Some(branding) = dir.client_branding.as_mut().and_then(Value::as_object_mut) {
            branding.remove(&key);
        }
    }
    ok_empty()
}

fn merge_objects(mut base: Value, over: &Value) -> Value {
    if let (Value::Object(dst), Value::Object(over)) = (&mut base, over) {
        for (k, v) in over {
            dst.insert(k.clone(), v.clone());
        }
    }
    base
}

// ---------------------------------------------------------------------------
// Application associations (moto stubs — nothing is stored)
// ---------------------------------------------------------------------------

fn associate_workspace_application(
    st: &mut WorkspacesStore,
    body: &Value,
    state: &str,
) -> Response {
    let _ = st; // moto validates nothing here
    ok(json!({
        "Association": {
            "WorkspaceId": as_str(body, "WorkspaceId"),
            "ApplicationId": as_str(body, "ApplicationId"),
            "State": state,
            "AssociatedResourceType": "APPLICATION",
        }
    }))
}

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

fn ok(body: Value) -> Response {
    Response::json(200, serde_json::to_vec(&body).unwrap_or_default())
}

fn ok_empty() -> Response {
    ok(json!({}))
}

fn not_found(message: &str) -> Response {
    json_error(400, "ResourceNotFoundException", message)
}

fn validation_ex(message: &str) -> Response {
    json_error(400, "ValidationException", message)
}

fn invalid_params(message: &str) -> Response {
    json_error(400, "InvalidParameterValuesException", message)
}

fn already_exists(message: &str) -> Response {
    json_error(400, "ResourceAlreadyExistsException", message)
}

/// moto's unguarded indexing crashes with KeyError('<key>') turned into a
/// 500 InternalError, as recorded in probes/workspaces.json.
fn internal_error_key(key: &str) -> Response {
    json_error(500, "InternalError", &format!("'{key}'"))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::atomic::{AtomicU64, Ordering};

    static ACCOUNTS: AtomicU64 = AtomicU64::new(200_000_000_001);

    /// Every test takes a fresh account so the shared store stays isolated.
    fn next_account() -> u64 {
        ACCOUNTS.fetch_add(1, Ordering::SeqCst)
    }

    fn send(account: u64, operation: &str, body: Value) -> (u16, Value) {
        let request = Request {
            service: "workspaces".to_string(),
            operation: operation.to_string(),
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            account,
            region: "us-east-1".to_string(),
        };
        let resp = dispatch(&request).expect("workspaces dispatch");
        (
            resp.status,
            serde_json::from_slice(&resp.body).unwrap_or(json!({})),
        )
    }

    /// Register a directory and create one workspace in it.
    fn seeded_workspace(account: u64) -> (String, String) {
        let (status, _) = send(
            account,
            "RegisterWorkspaceDirectory",
            json!({ "DirectoryId": "d-9267462133", "Tenancy": "SHARED" }),
        );
        assert_eq!(status, 200);
        let (status, body) = send(
            account,
            "CreateWorkspaces",
            json!({ "Workspaces": [{
                "DirectoryId": "d-9267462133",
                "UserName": "alice",
                "BundleId": "wsb-b1",
            }]}),
        );
        assert_eq!(status, 200);
        assert!(body["FailedRequests"].as_array().unwrap().is_empty());
        let id = body["PendingRequests"][0]["WorkspaceId"]
            .as_str()
            .expect("WorkspaceId")
            .to_string();
        ("d-9267462133".to_string(), id)
    }

    #[test]
    fn workspace_lifecycle() {
        let account = next_account();
        let (dir_id, ws_id) = seeded_workspace(account);

        let (status, body) = send(
            account,
            "DescribeWorkspaces",
            json!({ "WorkspaceIds": [ws_id] }),
        );
        assert_eq!(status, 200);
        let ws = &body["Workspaces"][0];
        assert_eq!(ws["WorkspaceId"], json!(ws_id));
        assert_eq!(ws["DirectoryId"], json!(dir_id));
        assert_eq!(ws["UserName"], "alice");
        assert_eq!(ws["State"], "AVAILABLE");
        assert_eq!(ws["WorkspaceProperties"]["RunningMode"], "ALWAYS_ON");

        let (status, body) = send(
            account,
            "TerminateWorkspaces",
            json!({ "TerminateWorkspaceRequests": [{ "WorkspaceId": ws_id }] }),
        );
        assert_eq!(status, 200);
        assert!(body["FailedRequests"].as_array().unwrap().is_empty());

        let (status, body) = send(
            account,
            "DescribeWorkspaces",
            json!({ "WorkspaceIds": [ws_id] }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Workspaces"], json!([]));
    }

    #[test]
    fn create_workspaces_failures() {
        let account = next_account();

        // Malformed directory id -> ValidationException.
        let (status, body) = send(
            account,
            "CreateWorkspaces",
            json!({ "Workspaces": [{"DirectoryId": "d-nope", "UserName": "u", "BundleId": "b"}] }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ValidationException");

        // Unknown-but-well-shaped directory -> FailedRequests entry.
        let (status, body) = send(
            account,
            "CreateWorkspaces",
            json!({ "Workspaces": [{"DirectoryId": "d-0000000000", "UserName": "u", "BundleId": "b"}] }),
        );
        assert_eq!(status, 200);
        let failed = &body["FailedRequests"][0];
        assert_eq!(failed["ErrorCode"], "ResourceNotFound.Directory");
        assert_eq!(failed["WorkspaceRequest"]["DirectoryId"], "d-0000000000");
        assert_eq!(body["PendingRequests"], json!([]));

        // AutoStopTimeout with ALWAYS_ON overrides the directory error.
        let (status, body) = send(
            account,
            "CreateWorkspaces",
            json!({ "Workspaces": [{
                "DirectoryId": "d-0000000000",
                "UserName": "u",
                "BundleId": "b",
                "WorkspaceProperties": {
                    "RunningMode": "ALWAYS_ON",
                    "RunningModeAutoStopTimeoutInMinutes": 60,
                },
            }]}),
        );
        assert_eq!(status, 200);
        assert_eq!(
            body["FailedRequests"][0]["ErrorCode"],
            "AutoStopTimeoutIsNotApplicableForAnAlwaysOnWorkspace"
        );
    }

    #[test]
    fn describe_workspaces_filters() {
        let account = next_account();

        let (status, body) = send(
            account,
            "DescribeWorkspaces",
            json!({ "WorkspaceIds": ["ws-nonexistent123"] }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Workspaces"], json!([]));

        // Combined filters are rejected.
        let (status, _) = send(
            account,
            "DescribeWorkspaces",
            json!({ "DirectoryId": "d-9267462133", "BundleId": "wsb-b1" }),
        );
        assert_eq!(status, 400);
        let (status, _) = send(
            account,
            "DescribeWorkspaces",
            json!({ "WorkspaceIds": ["ws-1"], "BundleId": "wsb-b1" }),
        );
        assert_eq!(status, 400);

        // UserName without DirectoryId is rejected.
        let (status, _) = send(
            account,
            "DescribeWorkspaces",
            json!({ "UserName": "alice" }),
        );
        assert_eq!(status, 400);

        // DirectoryId+UserName that does not match -> empty list.
        let (status, body) = send(
            account,
            "DescribeWorkspaces",
            json!({ "DirectoryId": "d-9267462133", "UserName": "nobody" }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Workspaces"], json!([]));
    }

    #[test]
    fn directory_registration_and_describe() {
        let account = next_account();

        let (status, body) = send(account, "DescribeWorkspaceDirectories", json!({}));
        assert_eq!(status, 200);
        assert_eq!(body["Directories"], json!([]));

        let (status, _) = send(
            account,
            "RegisterWorkspaceDirectory",
            json!({ "DirectoryId": "d-9267462133" }),
        );
        assert_eq!(status, 200);

        let (status, body) = send(account, "DescribeWorkspaceDirectories", json!({}));
        assert_eq!(status, 200);
        let dirs = body["Directories"].as_array().unwrap();
        let dir = &dirs[0];
        assert_eq!(dir["DirectoryId"], "d-9267462133");
        assert_eq!(dir["DirectoryType"], "SIMPLE_AD");
        assert_eq!(dir["State"], "REGISTERED");
        assert_eq!(dir["CustomerUserName"], "Administrator");
        assert!(dir["RegistrationCode"]
            .as_str()
            .unwrap()
            .starts_with("SLiad+"));
        // Empty defaults drop off the wire (moto's falsy filter).
        assert!(dir.get("Alias").is_none());
        assert!(dir.get("ipGroupIds").is_none());

        // Invalid directory id format -> ValidationException.
        let (status, body) = send(
            account,
            "DescribeWorkspaceDirectories",
            json!({ "DirectoryIds": ["d-nonexistent1234"] }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ValidationException");

        // Unknown id crashes just like moto's indexing.
        let (status, body) = send(
            account,
            "DeregisterWorkspaceDirectory",
            json!({ "DirectoryId": "d-nonexistent1234" }),
        );
        assert_eq!(status, 500);
        assert_eq!(body["__type"], "InternalError");

        // A registered directory deregisters cleanly.
        let (status, _) = send(
            account,
            "DeregisterWorkspaceDirectory",
            json!({ "DirectoryId": "d-9267462133" }),
        );
        assert_eq!(status, 200);
        let (status, body) = send(account, "DescribeWorkspaceDirectories", json!({}));
        assert_eq!(status, 200);
        assert_eq!(body["Directories"], json!([]));
    }

    #[test]
    fn modify_directory_properties() {
        let account = next_account();

        // Unknown directory -> moto's ValidationException / crash mirrors.
        let (status, body) = send(
            account,
            "ModifyWorkspaceCreationProperties",
            json!({"ResourceId": "d-0000000000", "WorkspaceCreationProperties": {"EnableInternetAccess": true}}),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ValidationException");

        for op in ["ModifyClientProperties", "ModifySelfservicePermissions"] {
            let (status, body) = send(
                account,
                op,
                json!({"ResourceId": "d-0000000000", "ClientProperties": {}}),
            );
            assert_eq!(status, 500, "{op}");
            assert_eq!(body["__type"], "InternalError");
        }

        let (_, _) = send(
            account,
            "RegisterWorkspaceDirectory",
            json!({ "DirectoryId": "d-9267462133" }),
        );

        let (status, _) = send(
            account,
            "ModifyWorkspaceCreationProperties",
            json!({"ResourceId": "d-9267462133", "WorkspaceCreationProperties": {"EnableInternetAccess": true}}),
        );
        assert_eq!(status, 200);

        let (status, _) = send(
            account,
            "ModifySelfservicePermissions",
            json!({"ResourceId": "d-9267462133", "SelfservicePermissions": {"RestartWorkspace": "ENABLED"}}),
        );
        assert_eq!(status, 200);

        let (status, _) = send(
            account,
            "ModifyWorkspaceAccessProperties",
            json!({"ResourceId": "d-9267462133", "WorkspaceAccessProperties": {"DeviceTypeLinux": "ALLOW"}}),
        );
        assert_eq!(status, 200);

        let (status, body) = send(
            account,
            "DescribeClientProperties",
            json!({ "ResourceIds": ["d-9267462133"] }),
        );
        assert_eq!(status, 200);
        assert_eq!(
            body["ClientPropertiesList"][0]["ResourceId"],
            "d-9267462133"
        );

        // An unmatched resource id answers an empty list.
        let (status, body) = send(
            account,
            "DescribeClientProperties",
            json!({ "ResourceIds": ["d-0000000000"] }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["ClientPropertiesList"], json!([]));

        // Client branding round-trip.
        let (status, body) = send(
            account,
            "ImportClientBranding",
            json!({"ResourceId": "d-9267462133", "DeviceTypeWindows": {"Logo": "abc"}}),
        );
        assert_eq!(status, 200);
        assert_eq!(body["DeviceTypeWindows"]["Logo"], "abc");
        let (status, body) = send(
            account,
            "DescribeClientBranding",
            json!({ "ResourceId": "d-9267462133" }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["DeviceTypeWindows"]["Logo"], "abc");
        let (status, _) = send(
            account,
            "DeleteClientBranding",
            json!({"ResourceId": "d-9267462133", "Platforms": ["DeviceTypeWindows"]}),
        );
        assert_eq!(status, 200);
        let (status, body) = send(
            account,
            "DescribeClientBranding",
            json!({ "ResourceId": "d-9267462133" }),
        );
        assert_eq!(status, 200);
        assert_eq!(body, json!({}));

        // Branding on an unknown directory -> ResourceNotFoundException.
        let (status, body) = send(
            account,
            "DescribeClientBranding",
            json!({ "ResourceId": "d-0000000000" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn ip_group_lifecycle() {
        let account = next_account();

        let (status, body) = send(
            account,
            "CreateIpGroup",
            json!({ "GroupName": "grp", "GroupDesc": "d", "UserRules": [{"ipRule": "10.0.0.0/8"}] }),
        );
        assert_eq!(status, 200);
        let group_id = body["GroupId"].as_str().unwrap().to_string();
        assert!(group_id.starts_with("wsipg-"));

        let (status, body) = send(
            account,
            "DescribeIpGroups",
            json!({ "GroupIds": [group_id] }),
        );
        assert_eq!(status, 200);
        let group = &body["Result"][0];
        // moto's IpGroup wire format uses lowercased keys.
        assert_eq!(group["groupId"], json!(group_id));
        assert_eq!(group["groupName"], "grp");
        assert_eq!(group["userRules"][0]["ipRule"], "10.0.0.0/8");

        // Errors on unknown groups.
        for (op, keys) in [
            (
                "AuthorizeIpRules",
                json!({"UserRules": [{"ipRule": "10.0.0.0/8"}]}),
            ),
            ("RevokeIpRules", json!({"UserRules": ["10.0.0.0/8"]})),
            (
                "UpdateRulesOfIpGroup",
                json!({"UserRules": [{"ipRule": "10.0.0.0/8"}]}),
            ),
            ("DeleteIpGroup", json!({})),
        ] {
            let mut body = keys;
            body["GroupId"] = json!("wsipg-nonexistent999");
            let (status, err) = send(account, op, body);
            assert_eq!(status, 400, "{op}");
            assert_eq!(err["__type"], "ResourceNotFoundException", "{op}");
        }

        // Change rules, then delete for real.
        let (status, _) = send(
            account,
            "AuthorizeIpRules",
            json!({"GroupId": group_id, "UserRules": [{"ipRule": "10.1.0.0/16"}]}),
        );
        assert_eq!(status, 200);
        let (status, _) = send(
            account,
            "RevokeIpRules",
            json!({"GroupId": group_id, "UserRules": ["10.0.0.0/8"]}),
        );
        assert_eq!(status, 200);
        let (_status, body) = send(
            account,
            "DescribeIpGroups",
            json!({ "GroupIds": [group_id] }),
        );
        assert_eq!(body["Result"][0]["userRules"].as_array().unwrap().len(), 1);
        let (status, _) = send(account, "DeleteIpGroup", json!({ "GroupId": group_id }));
        assert_eq!(status, 200);
        let (_status, body) = send(account, "DescribeIpGroups", json!({}));
        assert_eq!(body["Result"], json!([]));
    }

    #[test]
    fn connection_alias_lifecycle() {
        let account = next_account();

        let (status, body) = send(
            account,
            "CreateConnectionAlias",
            json!({ "ConnectionString": "alias-1.example.com" }),
        );
        assert_eq!(status, 200);
        let alias_id = body["AliasId"].as_str().unwrap().to_string();
        assert!(alias_id.starts_with("wsca-"));

        let (status, body) = send(
            account,
            "DescribeConnectionAliases",
            json!({ "AliasIds": [alias_id] }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["ConnectionAliases"][0]["State"], "CREATED");

        // Associate with a directory and describe by ResourceId.
        let (status, body) = send(
            account,
            "AssociateConnectionAlias",
            json!({"AliasId": alias_id, "ResourceId": "d-9267462133"}),
        );
        assert_eq!(status, 200);
        let conn = body["ConnectionIdentifier"].as_str().unwrap().to_string();
        assert!(conn.starts_with("wsci-"));
        let (status, body) = send(
            account,
            "DescribeConnectionAliases",
            json!({ "ResourceId": "d-9267462133" }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["ConnectionAliases"][0]["AliasId"], json!(alias_id));

        // Permissions may be granted and revoked.
        let (status, _) = send(
            account,
            "UpdateConnectionAliasPermission",
            json!({"AliasId": alias_id, "ConnectionAliasPermission": {
                "SharedAccountId": "222233334444", "AllowAssociation": true}}),
        );
        assert_eq!(status, 200);
        let (status, body) = send(
            account,
            "DescribeConnectionAliasPermissions",
            json!({ "AliasId": alias_id }),
        );
        assert_eq!(status, 200);
        assert_eq!(
            body["ConnectionAliasPermissions"][0]["SharedAccountId"],
            "222233334444"
        );
        let (status, _) = send(
            account,
            "UpdateConnectionAliasPermission",
            json!({"AliasId": alias_id, "ConnectionAliasPermission": {
                "SharedAccountId": "222233334444", "AllowAssociation": false}}),
        );
        assert_eq!(status, 200);

        // Unknown alias ids error as ResourceNotFoundException.
        for op in [
            "DeleteConnectionAlias",
            "AssociateConnectionAlias",
            "DisassociateConnectionAlias",
            "DescribeConnectionAliasPermissions",
            "UpdateConnectionAliasPermission",
        ] {
            let mut body = json!({"AliasId": "wsca-nonexistent999"});
            if op == "AssociateConnectionAlias" {
                body["ResourceId"] = json!("d-fake12345");
            }
            if op == "UpdateConnectionAliasPermission" {
                body["ConnectionAliasPermission"] =
                    json!({"SharedAccountId": "1", "AllowAssociation": true});
            }
            let (status, err) = send(account, op, body);
            assert_eq!(status, 400, "{op}");
            assert_eq!(err["__type"], "ResourceNotFoundException", "{op}");
        }

        // Disassociate clears the associations; delete removes the alias.
        let (status, _) = send(
            account,
            "DisassociateConnectionAlias",
            json!({ "AliasId": alias_id }),
        );
        assert_eq!(status, 200);
        let (_status, body) = send(
            account,
            "DescribeConnectionAliases",
            json!({ "ResourceId": "d-9267462133" }),
        );
        assert_eq!(body["ConnectionAliases"], json!([]));
        let (status, _) = send(
            account,
            "DeleteConnectionAlias",
            json!({"AliasId": alias_id}),
        );
        assert_eq!(status, 200);
    }

    #[test]
    fn workspace_image_operations() {
        let account = next_account();
        let (_dir_id, ws_id) = seeded_workspace(account);

        // Creating from a real workspace works; a fake name collides.
        let (status, body) = send(
            account,
            "CreateWorkspaceImage",
            json!({"Name": "img-1", "Description": "d", "WorkspaceId": ws_id}),
        );
        assert_eq!(status, 200);
        let image_id = body["ImageId"].as_str().unwrap().to_string();
        assert!(image_id.starts_with("wsi-"));
        assert_eq!(body["State"], "AVAILABLE");
        assert_eq!(body["OwnerAccountId"], account.to_string());

        let (status, body) = send(
            account,
            "CreateWorkspaceImage",
            json!({"Name": "img-1", "Description": "again", "WorkspaceId": ws_id}),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ResourceAlreadyExistsException");

        let (status, body) = send(
            account,
            "CreateWorkspaceImage",
            json!({"Name": "img-2", "WorkspaceId": "ws-nonexistent123"}),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ResourceNotFoundException");

        // Describe with ids and with OWNED filter.
        let (status, body) = send(
            account,
            "DescribeWorkspaceImages",
            json!({ "ImageIds": [image_id] }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Images"][0]["ImageId"], json!(image_id));
        assert!(body["Images"][0].get("Updates").is_some());
        let (status, body) = send(
            account,
            "DescribeWorkspaceImages",
            json!({ "ImageType": "OWNED" }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Images"].as_array().unwrap().len(), 1);

        // Permissions describe validates the id shape.
        let (status, body) = send(
            account,
            "DescribeWorkspaceImagePermissions",
            json!({ "ImageId": "wsi-nonexistent123" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ValidationException");

        let (status, body) = send(
            account,
            "DescribeWorkspaceImagePermissions",
            json!({ "ImageId": image_id }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["ImageId"], json!(image_id));

        // Sharing and revoking a permission mirrors moto's in-place edit.
        let (status, _) = send(
            account,
            "UpdateWorkspaceImagePermission",
            json!({"ImageId": image_id, "AllowCopyImage": true, "SharedAccountId": "333344445555"}),
        );
        assert_eq!(status, 200);
        let (_status, body) = send(
            account,
            "DescribeWorkspaceImagePermissions",
            json!({ "ImageId": image_id.clone() }),
        );
        assert_eq!(
            body["ImagePermissions"][0]["SharedAccountId"],
            "333344445555"
        );

        // Copy and delete.
        let (status, body) = send(
            account,
            "CopyWorkspaceImage",
            json!({"Name": "copy-1", "SourceImageId": image_id, "SourceRegion": "us-west-2"}),
        );
        assert_eq!(status, 200);
        assert!(body["ImageId"].as_str().unwrap().starts_with("wsi-"));
        let (status, _) = send(
            account,
            "DeleteWorkspaceImage",
            json!({"ImageId": image_id}),
        );
        assert_eq!(status, 200);
        let (_status, body) = send(
            account,
            "DescribeWorkspaceImages",
            json!({ "ImageIds": [image_id] }),
        );
        assert_eq!(body["Images"], json!([]));
    }

    #[test]
    fn workspace_bundle_operations() {
        let account = next_account();

        let (status, body) = send(
            account,
            "CreateWorkspaceBundle",
            json!({"BundleName": "b1", "BundleDescription": "d", "ImageId": "wsi-fake12345",
                   "ComputeType": {"Name": "VALUE"}, "UserStorage": {"Capacity": "10"}}),
        );
        assert_eq!(status, 200);
        let bundle_id = body["WorkspaceBundle"]["BundleId"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(bundle_id.starts_with("wsb-"));
        assert_eq!(body["WorkspaceBundle"]["State"], "AVAILABLE");
        assert_eq!(body["WorkspaceBundle"]["Owner"], account.to_string());

        let (status, body) = send(
            account,
            "DescribeWorkspaceBundles",
            json!({ "BundleIds": [bundle_id.clone()], "Owner": account.to_string() }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Bundles"].as_array().unwrap().len(), 1);

        // Unknown bundle update errors with moto's message.
        let (status, body) = send(
            account,
            "UpdateWorkspaceBundle",
            json!({ "BundleId": "wsb-nonexistent999", "ImageId": "wsi-fake12345" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ResourceNotFoundException");

        // Updating an existing bundle's image succeeds.
        let (status, _) = send(
            account,
            "UpdateWorkspaceBundle",
            json!({ "BundleId": bundle_id, "ImageId": "wsi-fake12345" }),
        );
        assert_eq!(status, 200);

        // Delete is silent even for unknown bundles.
        let (status, _) = send(
            account,
            "DeleteWorkspaceBundle",
            json!({ "BundleId": "wsb-fake12345" }),
        );
        assert_eq!(status, 200);
        let (status, _) = send(account, "DeleteWorkspaceBundle", json!({}));
        assert_eq!(status, 200);
    }

    #[test]
    fn account_link_operations() {
        let account = next_account();

        let (status, body) = send(
            account,
            "CreateAccountLinkInvitation",
            json!({ "TargetAccountId": "222233334444" }),
        );
        assert_eq!(status, 200);
        let link = &body["AccountLink"];
        let link_id = link["AccountLinkId"].as_str().unwrap().to_string();
        assert!(link_id.starts_with("wsal-"));
        assert_eq!(link["AccountLinkStatus"], "PENDING_ACCEPTANCE");
        assert_eq!(link["SourceAccountId"], account.to_string());

        // Unknown link -> ResourceNotFoundException.
        let (status, body) = send(
            account,
            "AcceptAccountLinkInvitation",
            json!({ "LinkId": "wsal-nonexistent999" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ResourceNotFoundException");

        // Accept flips the status; delete removes it.
        let (status, body) = send(
            account,
            "AcceptAccountLinkInvitation",
            json!({ "LinkId": link_id.clone() }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["AccountLink"]["AccountLinkStatus"], "LINKED");

        let (_status, body) = send(account, "ListAccountLinks", json!({}));
        assert_eq!(body["AccountLinks"].as_array().unwrap().len(), 1);

        let (status, body) = send(
            account,
            "GetAccountLink",
            json!({ "LinkId": link_id.clone() }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["AccountLink"]["AccountLinkStatus"], "LINKED");

        // Unknown ids answer "The account link does not exist."
        let (status, body) = send(
            account,
            "GetAccountLink",
            json!({ "LinkId": "wsal-nothing88888" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ResourceNotFoundException");

        let (status, _) = send(
            account,
            "DeleteAccountLinkInvitation",
            json!({ "LinkId": link_id.clone() }),
        );
        assert_eq!(status, 200);
        let (_status, body) = send(account, "ListAccountLinks", json!({}));
        assert_eq!(body["AccountLinks"], json!([]));
    }

    #[test]
    fn pool_operations() {
        let account = next_account();

        let (status, body) = send(
            account,
            "CreateWorkspacesPool",
            json!({"PoolName": "pool-1", "Description": "d", "BundleId": "wsb-fake12345",
                   "DirectoryId": "d-9267462133", "Capacity": {"DesiredUserSessions": 2}}),
        );
        assert_eq!(status, 200);
        let pool = &body["WorkspacesPool"];
        let pool_id = pool["PoolId"].as_str().unwrap().to_string();
        assert!(pool_id.starts_with("wsp-"));
        assert_eq!(pool["State"], "AVAILABLE");
        assert_eq!(
            pool["PoolArn"],
            json!(format!(
                "arn:aws:workspaces:us-east-1:{account}:workspacespool/{pool_id}"
            ))
        );
        assert_eq!(pool["CapacityStatus"]["DesiredUserSessions"], 2);

        let (status, body) = send(
            account,
            "DescribeWorkspacesPools",
            json!({ "PoolIds": [pool_id] }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["WorkspacesPools"][0]["PoolId"], json!(pool_id));

        let (status, _) = send(
            account,
            "UpdateWorkspacesPool",
            json!({"PoolId": pool_id, "Description": "updated", "Capacity": {"DesiredUserSessions": 3}}),
        );
        assert_eq!(status, 200);
        let (_status, body) = send(
            account,
            "DescribeWorkspacesPools",
            json!({ "PoolIds": [pool_id.clone()] }),
        );
        assert_eq!(body["WorkspacesPools"][0]["Description"], "updated");
        assert_eq!(
            body["WorkspacesPools"][0]["CapacityStatus"]["DesiredUserSessions"],
            3
        );

        // Unknown pool ids -> ResourceNotFoundException everywhere.
        for op in [
            "StartWorkspacesPool",
            "StopWorkspacesPool",
            "TerminateWorkspacesPool",
            "UpdateWorkspacesPool",
            "DescribeWorkspacesPoolSessions",
        ] {
            let mut body = json!({ "PoolId": "wsp-nonexistent999" });
            if op == "UpdateWorkspacesPool" {
                body["Description"] = json!("x");
            }
            let (status, err) = send(account, op, body);
            assert_eq!(status, 400, "{op}");
            assert_eq!(err["__type"], "ResourceNotFoundException", "{op}");
        }

        // Terminating a pool session answers 200 even for unknown sessions.
        let (status, _) = send(
            account,
            "TerminateWorkspacesPoolSession",
            json!({ "SessionId": "00000000-0000-0000-0000-000000000000" }),
        );
        assert_eq!(status, 200);

        // Lifecycle: stop then terminate.
        let (status, _) = send(
            account,
            "StopWorkspacesPool",
            json!({ "PoolId": pool_id.clone() }),
        );
        assert_eq!(status, 200);
        let (_status, body) = send(
            account,
            "DescribeWorkspacesPools",
            json!({ "PoolIds": [pool_id.clone()] }),
        );
        assert_eq!(body["WorkspacesPools"][0]["State"], "STOPPED");
        let (status, _) = send(
            account,
            "TerminateWorkspacesPool",
            json!({ "PoolId": pool_id }),
        );
        assert_eq!(status, 200);
    }

    #[test]
    fn account_management() {
        let account = next_account();

        let (status, body) = send(account, "DescribeAccount", json!({}));
        assert_eq!(status, 200);
        assert_eq!(body["DedicatedTenancySupport"], "DISABLED");

        let (status, _body) = send(
            account,
            "ModifyAccount",
            json!({
                "DedicatedTenancySupport": "ENABLED",
                "DedicatedTenancyManagementCidrRange": "10.0.0.0/16",
            }),
        );
        assert_eq!(status, 200);

        let (_status, body) = send(account, "DescribeAccount", json!({}));
        assert_eq!(body["DedicatedTenancySupport"], "ENABLED");
        assert_eq!(body["DedicatedTenancyManagementCidrRange"], "10.0.0.0/16");

        let (status, body) = send(account, "DescribeAccountModifications", json!({}));
        assert_eq!(status, 200);
        assert_eq!(
            body["AccountModifications"][0]["ModificationState"],
            "COMPLETED"
        );
        assert_eq!(
            body["AccountModifications"][0]["DedicatedTenancySupport"],
            "ENABLED"
        );
    }

    #[test]
    fn tag_operations() {
        let account = next_account();
        let (_, ws_id) = seeded_workspace(account);

        // Tags round-trip on a workspace.
        let (status, _) = send(
            account,
            "CreateTags",
            json!({"ResourceId": ws_id, "Tags": [{"Key": "env", "Value": "test"}]}),
        );
        assert_eq!(status, 200);
        let (status, body) = send(account, "DescribeTags", json!({ "ResourceId": ws_id }));
        assert_eq!(status, 200);
        assert_eq!(body["TagList"][0]["Key"], "env");
        assert_eq!(body["TagList"][0]["Value"], "test");

        // moto answers empty tags for unknown resources.
        let (status, body) = send(
            account,
            "DescribeTags",
            json!({ "ResourceId": "ws-fake12345" }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["TagList"], json!([]));

        // ...and {} for prefix-less ids (moto falls off the branch chain).
        let (status, _) = send(
            account,
            "CreateTags",
            json!({"ResourceId": "probe-test-value", "Tags": [{"Key": "k", "Value": "v"}]}),
        );
        assert_eq!(status, 200);

        // Known-prefix unknown id -> moto's InternalError crash.
        let (status, body) = send(
            account,
            "CreateTags",
            json!({"ResourceId": "ws-fake12345", "Tags": [{"Key": "k", "Value": "v"}]}),
        );
        assert_eq!(status, 500);
        assert_eq!(body["__type"], "InternalError");

        // DeleteTags succeeds everywhere; keys disappear.
        let (status, _) = send(
            account,
            "DeleteTags",
            json!({ "ResourceId": ws_id, "TagKeys": ["env"] }),
        );
        assert_eq!(status, 200);
        let (_status, body) = send(account, "DescribeTags", json!({ "ResourceId": ws_id }));
        assert_eq!(body["TagList"], json!([]));
    }

    #[test]
    fn workspace_state_and_modify_ops() {
        let account = next_account();
        let (_, ws_id) = seeded_workspace(account);

        // Unknown workspaces error.
        let (status, body) = send(
            account,
            "ModifyWorkspaceState",
            json!({ "WorkspaceId": "ws-nonexistent999", "WorkspaceState": "ADMIN_MAINTENANCE" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ResourceNotFoundException");

        let (_status, body) = send(
            account,
            "ModifyWorkspaceProperties",
            json!({"WorkspaceId": "ws-nonexistent999", "WorkspaceProperties": {"RunningMode": "AUTO_STOP"}}),
        );
        assert_eq!(body["__type"], "ResourceNotFoundException");

        for (op, key) in [
            ("StartWorkspaces", "StartWorkspaceRequests"),
            ("StopWorkspaces", "StopWorkspaceRequests"),
            ("RebootWorkspaces", "RebootWorkspaceRequests"),
            ("RebuildWorkspaces", "RebuildWorkspaceRequests"),
        ] {
            let (status, body) = send(
                account,
                op,
                json!({ key: [{ "WorkspaceId": "ws-nonexistent123" }] }),
            );
            assert_eq!(status, 200, "{op}");
            let failed = &body["FailedRequests"][0];
            assert_eq!(failed["WorkspaceId"], "ws-nonexistent123", "{op}");
            assert_eq!(failed["ErrorCode"], "ResourceNotFound.Workspace", "{op}");
        }

        // Terminate reports its own 400-style error code.
        let (_status, body) = send(
            account,
            "TerminateWorkspaces",
            json!({ "TerminateWorkspaceRequests": [{ "WorkspaceId": "ws-nonexistent123" }] }),
        );
        assert_eq!(body["FailedRequests"][0]["ErrorCode"], "400");

        // State changes land.
        let (status, _) = send(
            account,
            "ModifyWorkspaceState",
            json!({"WorkspaceId": ws_id, "WorkspaceState": "ADMIN_MAINTENANCE"}),
        );
        assert_eq!(status, 200);
        let (_status, body) = send(account, "DescribeWorkspaces", json!({}));
        assert_eq!(body["Workspaces"][0]["State"], "ADMIN_MAINTENANCE");

        // Snapshots: unknown errors, known empty.
        let (status, body) = send(
            account,
            "DescribeWorkspaceSnapshots",
            json!({ "WorkspaceId": ws_id }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["RebuildSnapshots"], json!([]));
        assert_eq!(body["RestoreSnapshots"], json!([]));

        // Migration targets a fresh id.
        let (status, body) = send(
            account,
            "MigrateWorkspace",
            json!({"SourceWorkspaceId": ws_id, "BundleId": "wsb-fake12345"}),
        );
        assert_eq!(status, 200);
        assert_eq!(body["SourceWorkspaceId"], json!(ws_id));
        assert!(body["TargetWorkspaceId"]
            .as_str()
            .unwrap()
            .starts_with("ws-"));

        // The connection status list only reports existing workspaces.
        let (_status, body) = send(account, "DescribeWorkspacesConnectionStatus", json!({}));
        assert_eq!(
            body["WorkspacesConnectionStatus"][0]["ConnectionState"],
            "UNKNOWN"
        );
    }

    #[test]
    fn association_and_misc_stubs() {
        let account = next_account();

        let (status, body) = send(
            account,
            "AssociateWorkspaceApplication",
            json!({"WorkspaceId": "ws-fake12345", "ApplicationId": "wsa-fake12345"}),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Association"]["State"], "COMPLETED");

        let (status, body) = send(
            account,
            "DisassociateWorkspaceApplication",
            json!({"WorkspaceId": "ws-fake12345", "ApplicationId": "wsa-fake12345"}),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Association"]["State"], "REMOVED");

        let (status, body) = send(
            account,
            "DeployWorkspaceApplications",
            json!({"WorkspaceId": "ws-fake12345"}),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Deployment"]["Associations"], json!([]));

        for (op, key) in [
            ("DescribeApplicationAssociations", "Associations"),
            ("DescribeBundleAssociations", "Associations"),
            ("DescribeImageAssociations", "Associations"),
            ("DescribeWorkspaceAssociations", "Associations"),
        ] {
            let (status, body) = send(account, op, json!({}));
            assert_eq!(status, 200, "{op}");
            assert_eq!(body[key], json!([]), "{op}");
        }

        let (status, body) = send(account, "DescribeApplications", json!({}));
        assert_eq!(status, 200);
        assert_eq!(body["Applications"], json!([]));

        let (status, body) = send(
            account,
            "ListAvailableManagementCidrRanges",
            json!({ "ManagementCidrRangeConstraint": "10.0.0.0/8" }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["ManagementCidrRanges"], json!(["10.0.0.0/8"]));

        // Connect client add-ins create, describe, fail-update, delete.
        let (status, body) = send(
            account,
            "CreateConnectClientAddIn",
            json!({"ResourceId": "d-fake12345", "Name": "addin", "URL": "https://example.com"}),
        );
        assert_eq!(status, 200);
        let add_in_id = body["AddInId"].as_str().unwrap().to_string();
        let (_status, body) = send(
            account,
            "DescribeConnectClientAddIns",
            json!({ "ResourceId": "d-fake12345" }),
        );
        assert_eq!(body["AddIns"][0]["AddInId"], json!(add_in_id));
        let (status, body) = send(
            account,
            "UpdateConnectClientAddIn",
            json!({"AddInId": "12345678-1234-1234-1234-123456789012", "ResourceId": "d-1234567890", "Name": "nope"}),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ResourceNotFoundException");
        let (status, _) = send(
            account,
            "DeleteConnectClientAddIn",
            json!({"AddInId": "12345678-1234-1234-1234-123456789012", "ResourceId": "d-1234567890"}),
        );
        assert_eq!(status, 200);

        // Standby workspaces echo pending entries.
        let (status, body) = send(
            account,
            "CreateStandbyWorkspaces",
            json!({"PrimaryRegion": "us-east-1", "StandbyWorkspaces": [{
                "PrimaryWorkspaceId": "ws-fake12345", "DirectoryId": "d-9267462133"}]}),
        );
        assert_eq!(status, 200);
        assert_eq!(
            body["PendingStandbyRequests"][0]["StandbyWorkspace"]["DirectoryId"],
            "d-9267462133"
        );
        assert_eq!(body["FailedStandbyRequests"], json!([]));

        // Images: custom import + describe.
        let (status, body) = send(
            account,
            "ImportCustomWorkspaceImage",
            json!({"ImageName": "custom-1", "ImageDescription": "d"}),
        );
        assert_eq!(status, 200);
        assert_eq!(body["State"], "AVAILABLE");
        let image_id = body["ImageId"].as_str().unwrap().to_string();
        let (status, body) = send(
            account,
            "DescribeCustomWorkspaceImageImport",
            json!({ "ImageId": image_id.clone() }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["ImageId"], json!(image_id));
        let (status, body) = send(
            account,
            "DescribeCustomWorkspaceImageImport",
            json!({ "ImageId": "wsi-fake12345" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ResourceNotFoundException");

        // Updated and imported images return fresh ids.
        let (_status, body) = send(
            account,
            "CreateUpdatedWorkspaceImage",
            json!({"Name": "updated-1", "SourceImageId": image_id, "Description": "d"}),
        );
        assert!(body["ImageId"].as_str().unwrap().starts_with("wsi-"));
        let (_status, body) = send(
            account,
            "ImportWorkspaceImage",
            json!({"Ec2ImageId": "ami-fake12345", "IngestionProcess": "BYOL_REGULAR", "ImageName": "imported-1"}),
        );
        assert!(body["ImageId"].as_str().unwrap().starts_with("wsi-"));
    }
}
