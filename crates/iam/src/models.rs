//! IAM in-memory state models.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

/// Build a resource ARN honoring the path (`arn:aws:iam::{acct}:kind{path}{name}`).
pub fn resource_arn(account: u64, kind: &str, path: &str, name: &str) -> String {
    let path = if path.is_empty() { "/" } else { path };
    if path == "/" {
        format!("arn:aws:iam::{}:{}/{}", account, kind, name)
    } else {
        format!("arn:aws:iam::{}:{}{}{}", account, kind, path, name)
    }
}

/// Generate a random uppercase ID with the given prefix (20 random chars).
pub fn random_id(prefix: &str) -> String {
    let mut chars: Vec<char> = prefix.chars().collect();
    for c in uuid::Uuid::new_v4().simple().to_string().chars().take(20) {
        let up = if c.is_ascii_digit() {
            (b'A' + (c as u8 - b'0')) as char
        } else {
            c.to_ascii_uppercase()
        };
        chars.push(up);
    }
    chars.into_iter().collect()
}

/// Current time as RFC3339.
pub fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339()
}

/// An IAM access key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessKey {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub status: String,
    pub created: u64,
}

impl AccessKey {
    pub fn new(_account: u64) -> Self {
        Self {
            access_key_id: random_id("AKIA"),
            secret_access_key: crate::handler::base64_encode(
                format!("{}", uuid::Uuid::new_v4()).as_bytes(),
            ),
            status: "Active".to_string(),
            created: chrono::Utc::now().timestamp_millis() as u64,
        }
    }
}

/// An IAM user.
#[derive(Debug, Serialize, Deserialize)]
pub struct User {
    pub user_id: String,
    pub username: String,
    pub arn: String,
    pub path: String,
    pub created: u64,
    #[serde(with = "crate::snapshot::rw")]
    pub password: RwLock<Option<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub access_keys: RwLock<Vec<AccessKey>>,
    #[serde(with = "crate::snapshot::rw")]
    pub groups: RwLock<Vec<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub tags: RwLock<Vec<serde_json::Value>>,
    #[serde(with = "crate::snapshot::rw")]
    pub policies: RwLock<Vec<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub policy_documents: RwLock<HashMap<String, String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub attached_policies: RwLock<Vec<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub permissions_boundary: RwLock<Option<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub signing_certs: RwLock<Vec<SigningCert>>,
}

impl User {
    pub fn new(account: u64, username: String) -> Self {
        Self::with_path(account, username, "/".to_string())
    }

    /// Identity-preserving rename: moto mutates `user.name` in place across
    /// UpdateUser, keeping `user_id` and the creation timestamp (models.py:
    /// 2691-2706) — only name/path/arn move; every state map rides along.
    pub fn renamed(&self, account: u64, username: String, path: String) -> Self {
        Self {
            user_id: self.user_id.clone(),
            username: username.clone(),
            arn: resource_arn(account, "user", &path, &username),
            path,
            created: self.created,
            password: RwLock::new(self.password.read().clone()),
            access_keys: RwLock::new(self.access_keys.read().clone()),
            groups: RwLock::new(self.groups.read().clone()),
            tags: RwLock::new(self.tags.read().clone()),
            policies: RwLock::new(self.policies.read().clone()),
            policy_documents: RwLock::new(self.policy_documents.read().clone()),
            attached_policies: RwLock::new(self.attached_policies.read().clone()),
            permissions_boundary: RwLock::new(self.permissions_boundary.read().clone()),
            signing_certs: RwLock::new(self.signing_certs.read().clone()),
        }
    }

    pub fn with_path(account: u64, username: String, path: String) -> Self {
        Self {
            user_id: random_id("AIDA"),
            arn: resource_arn(account, "user", &path, &username),
            username,
            path,
            created: chrono::Utc::now().timestamp_millis() as u64,
            password: RwLock::new(None),
            access_keys: RwLock::new(Vec::new()),
            groups: RwLock::new(Vec::new()),
            tags: RwLock::new(Vec::new()),
            policies: RwLock::new(Vec::new()),
            policy_documents: RwLock::new(HashMap::new()),
            attached_policies: RwLock::new(Vec::new()),
            permissions_boundary: RwLock::new(None),
            signing_certs: RwLock::new(Vec::new()),
        }
    }
}

/// An IAM role.
#[derive(Debug, Serialize, Deserialize)]
pub struct Role {
    pub role_id: String,
    pub role_name: String,
    pub arn: String,
    pub path: String,
    #[serde(with = "crate::snapshot::rw")]
    pub assume_role_policy: RwLock<String>,
    #[serde(with = "crate::snapshot::rw")]
    pub description: RwLock<String>,
    #[serde(with = "crate::snapshot::rw")]
    pub max_session_duration: RwLock<u32>,
    pub created: u64,
    #[serde(with = "crate::snapshot::rw")]
    pub tags: RwLock<Vec<serde_json::Value>>,
    #[serde(with = "crate::snapshot::rw")]
    pub attached_policies: RwLock<Vec<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub inline_policies: RwLock<Vec<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub policy_documents: RwLock<HashMap<String, String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub permissions_boundary: RwLock<Option<String>>,
    pub is_service_linked: bool,
}

impl Role {
    pub fn new(account: u64, role_name: String, assume_role_policy: String) -> Self {
        Self::full(
            account,
            role_name,
            "/".to_string(),
            assume_role_policy,
            String::new(),
            3600,
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn full(
        account: u64,
        role_name: String,
        path: String,
        assume_role_policy: String,
        description: String,
        max_session_duration: u32,
        is_service_linked: bool,
    ) -> Self {
        Self {
            role_id: random_id("AROA"),
            arn: resource_arn(account, "role", &path, &role_name),
            role_name,
            path,
            assume_role_policy: RwLock::new(assume_role_policy),
            description: RwLock::new(description),
            max_session_duration: RwLock::new(max_session_duration),
            created: chrono::Utc::now().timestamp_millis() as u64,
            tags: RwLock::new(Vec::new()),
            attached_policies: RwLock::new(Vec::new()),
            inline_policies: RwLock::new(Vec::new()),
            policy_documents: RwLock::new(HashMap::new()),
            permissions_boundary: RwLock::new(None),
            is_service_linked,
        }
    }
}

/// An IAM group.
#[derive(Debug, Serialize, Deserialize)]
pub struct Group {
    pub group_id: String,
    pub group_name: String,
    pub arn: String,
    pub path: String,
    pub created: u64,
    #[serde(with = "crate::snapshot::rw")]
    pub users: RwLock<Vec<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub tags: RwLock<Vec<serde_json::Value>>,
    #[serde(with = "crate::snapshot::rw")]
    pub attached_policies: RwLock<Vec<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub inline_policies: RwLock<Vec<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub policy_documents: RwLock<HashMap<String, String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub permissions_boundary: RwLock<Option<String>>,
}

impl Group {
    pub fn new(account: u64, group_name: String) -> Self {
        Self::with_path(account, group_name, "/".to_string())
    }

    pub fn with_path(account: u64, group_name: String, path: String) -> Self {
        Self {
            group_id: random_id("AGPA"),
            arn: resource_arn(account, "group", &path, &group_name),
            group_name,
            path,
            created: chrono::Utc::now().timestamp_millis() as u64,
            users: RwLock::new(Vec::new()),
            tags: RwLock::new(Vec::new()),
            attached_policies: RwLock::new(Vec::new()),
            inline_policies: RwLock::new(Vec::new()),
            policy_documents: RwLock::new(HashMap::new()),
            permissions_boundary: RwLock::new(None),
        }
    }
}

/// An IAM policy.
#[derive(Debug, Serialize, Deserialize)]
pub struct Policy {
    pub policy_id: String,
    pub policy_name: String,
    pub arn: String,
    pub path: String,
    #[serde(with = "crate::snapshot::rw")]
    pub default_version_id: RwLock<String>,
    #[serde(with = "crate::snapshot::rw")]
    pub description: RwLock<String>,
    pub created: u64,
    pub modified: u64,
    #[serde(with = "crate::snapshot::rw")]
    pub policy_document: RwLock<String>,
    #[serde(with = "crate::snapshot::rw")]
    pub tags: RwLock<Vec<serde_json::Value>>,
    #[serde(with = "crate::snapshot::rw")]
    pub versions: RwLock<Vec<serde_json::Value>>,
    #[serde(with = "crate::snapshot::rw")]
    pub attached_users: RwLock<Vec<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub attached_roles: RwLock<Vec<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub attached_groups: RwLock<Vec<String>>,
}

impl Policy {
    pub fn new(account: u64, policy_name: String, document: String) -> Self {
        Self::full(
            account,
            policy_name,
            "/".to_string(),
            document,
            Vec::new(),
            String::new(),
        )
    }

    pub fn full(
        account: u64,
        policy_name: String,
        path: String,
        document: String,
        tags: Vec<serde_json::Value>,
        description: String,
    ) -> Self {
        let now = chrono::Utc::now().timestamp_millis() as u64;
        let arn = IamState::policy_arn(account, &path, &policy_name);
        Self {
            policy_id: random_id("ANPA"),
            policy_name,
            arn,
            path,
            default_version_id: RwLock::new("v1".to_string()),
            description: RwLock::new(description),
            created: now,
            modified: now,
            policy_document: RwLock::new(document.clone()),
            tags: RwLock::new(tags),
            versions: RwLock::new(vec![json!({
                "VersionId": "v1",
                "IsDefaultVersion": true,
                "CreateDate": now_iso(),
                "Document": document
            })]),
            attached_users: RwLock::new(Vec::new()),
            attached_roles: RwLock::new(Vec::new()),
            attached_groups: RwLock::new(Vec::new()),
        }
    }
}

/// A signing certificate attached to a user.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SigningCert {
    pub certificate_id: String,
    pub body: String,
    pub status: String,
    pub upload_date: String,
}

/// An SSH public key of a user.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SshKey {
    pub id: String,
    pub fingerprint: String,
    pub body: String,
    pub status: String,
    pub upload_date: String,
}

/// A server certificate.
#[derive(Debug, Serialize, Deserialize)]
pub struct ServerCert {
    pub name: String,
    pub cert_id: String,
    pub path: String,
    pub body: String,
    pub private_key: String,
    pub chain: String,
    pub upload_date: String,
    pub expiration: String,
    #[serde(with = "crate::snapshot::rw")]
    pub tags: RwLock<Vec<serde_json::Value>>,
}

/// A virtual MFA device.
#[derive(Debug, Serialize, Deserialize)]
pub struct VirtualMfa {
    pub serial: String,
    pub base32: String,
    pub qrcode_png: String,
    #[serde(with = "crate::snapshot::rw")]
    pub assigned_user: RwLock<Option<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub enable_date: RwLock<Option<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub tags: RwLock<Vec<serde_json::Value>>,
}

/// A SAML provider.
#[derive(Debug, Serialize, Deserialize)]
pub struct SamlProvider {
    pub name: String,
    pub arn: String,
    #[serde(with = "crate::snapshot::rw")]
    pub metadata: RwLock<String>,
    pub created: String,
    pub valid_until: String,
    #[serde(with = "crate::snapshot::rw")]
    pub tags: RwLock<Vec<serde_json::Value>>,
}

/// An OpenID Connect provider.
#[derive(Debug, Serialize, Deserialize)]
pub struct OidcProvider {
    pub arn: String,
    pub url: String,
    #[serde(with = "crate::snapshot::rw")]
    pub thumbprints: RwLock<Vec<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub client_ids: RwLock<Vec<String>>,
    pub created: String,
    #[serde(with = "crate::snapshot::rw")]
    pub tags: RwLock<Vec<serde_json::Value>>,
}

/// A stored login profile.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginProfile {
    pub created: String,
    pub password: String,
    pub reset_required: bool,
}

/// A service specific credential.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceSpecificCredential {
    pub id: String,
    pub user: String,
    pub service: String,
    pub service_username: String,
    pub password: String,
    pub status: String,
    pub created: String,
}

/// The IAM state store (per account, global region).
#[derive(Clone, Serialize, Deserialize)]
pub struct IamState {
    #[serde(with = "crate::snapshot::arc_rw")]
    pub users: Arc<RwLock<HashMap<String, Arc<User>>>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub roles: Arc<RwLock<HashMap<String, Arc<Role>>>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub groups: Arc<RwLock<HashMap<String, Arc<Group>>>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub policies: Arc<RwLock<HashMap<String, Arc<Policy>>>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub account_alias: Arc<RwLock<Option<String>>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub password_policy: Arc<RwLock<Option<serde_json::Value>>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub instance_profiles: Arc<RwLock<HashMap<String, Vec<String>>>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub instance_profile_tags: Arc<RwLock<HashMap<String, Vec<serde_json::Value>>>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub instance_profile_ids: Arc<RwLock<HashMap<String, String>>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub virtual_mfas: Arc<RwLock<HashMap<String, Arc<VirtualMfa>>>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub saml_providers: Arc<RwLock<HashMap<String, Arc<SamlProvider>>>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub oidc_providers: Arc<RwLock<HashMap<String, Arc<OidcProvider>>>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub ssh_keys: Arc<RwLock<HashMap<String, Vec<SshKey>>>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub server_certs: Arc<RwLock<HashMap<String, Arc<ServerCert>>>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub login_profiles: Arc<RwLock<HashMap<String, LoginProfile>>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub ssc_creds: Arc<RwLock<HashMap<String, Vec<Value>>>>,
}

impl IamState {
    pub fn new() -> Self {
        Self {
            users: Arc::new(RwLock::new(HashMap::new())),
            roles: Arc::new(RwLock::new(HashMap::new())),
            groups: Arc::new(RwLock::new(HashMap::new())),
            policies: Arc::new(RwLock::new(HashMap::new())),
            account_alias: Arc::new(RwLock::new(None)),
            password_policy: Arc::new(RwLock::new(None)),
            instance_profiles: Arc::new(RwLock::new(HashMap::new())),
            instance_profile_tags: Arc::new(RwLock::new(HashMap::new())),
            instance_profile_ids: Arc::new(RwLock::new(HashMap::new())),
            virtual_mfas: Arc::new(RwLock::new(HashMap::new())),
            saml_providers: Arc::new(RwLock::new(HashMap::new())),
            oidc_providers: Arc::new(RwLock::new(HashMap::new())),
            ssh_keys: Arc::new(RwLock::new(HashMap::new())),
            server_certs: Arc::new(RwLock::new(HashMap::new())),
            login_profiles: Arc::new(RwLock::new(HashMap::new())),
            ssc_creds: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn get_user(&self, name: &str) -> Option<Arc<User>> {
        self.users.read().get(name).cloned()
    }

    pub fn get_role(&self, name: &str) -> Option<Arc<Role>> {
        self.roles.read().get(name).cloned()
    }

    pub fn get_group(&self, name: &str) -> Option<Arc<Group>> {
        self.groups.read().get(name).cloned()
    }

    pub fn get_policy(&self, arn_or_name: &str) -> Option<Arc<Policy>> {
        let policies = self.policies.read();
        if let Some(p) = policies.get(arn_or_name) {
            return Some(p.clone());
        }
        policies.values().find(|p| p.arn == arn_or_name).cloned()
    }

    /// Policy ARN: `arn:aws:iam::{acct}:policy{path-pruned}{name}`.
    pub fn policy_arn(account: u64, path: &str, name: &str) -> String {
        if path.is_empty() || path == "/" {
            format!("arn:aws:iam::{}:policy/{}", account, name)
        } else {
            format!("arn:aws:iam::{}:policy{}{}", account, path, name)
        }
    }

    pub fn attachments(&self, policy: &Policy) -> u32 {
        policy.attached_users.read().len() as u32
            + policy.attached_roles.read().len() as u32
            + policy.attached_groups.read().len() as u32
    }

    pub fn create_instance_profile(&self, name: &str) {
        self.instance_profiles
            .write()
            .entry(name.to_string())
            .or_default();
        self.instance_profile_ids
            .write()
            .entry(name.to_string())
            .or_insert_with(|| random_id("").chars().take(20).collect());
    }

    pub fn delete_instance_profile(&self, name: &str) -> bool {
        let removed = self.instance_profiles.write().remove(name).is_some();
        self.instance_profile_tags.write().remove(name);
        self.instance_profile_ids.write().remove(name);
        removed
    }

    pub fn add_role_to_instance_profile(&self, profile: &str, role: &str) {
        self.instance_profiles
            .write()
            .entry(profile.to_string())
            .or_default()
            .push(role.to_string());
    }

    pub fn remove_role_from_instance_profile(&self, profile: &str, role: &str) {
        if let Some(roles) = self.instance_profiles.write().get_mut(profile) {
            roles.retain(|r| r != role);
        }
    }
}

impl Default for IamState {
    fn default() -> Self {
        Self::new()
    }
}
