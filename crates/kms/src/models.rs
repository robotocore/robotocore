//! KMS in-memory state models.

use parking_lot::RwLock;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// A KMS key.
#[derive(Debug)]
pub struct Key {
    /// Raw KMS key id (uuid hex without dashes, or `mrk-<hex>` for multi-region keys).
    pub key_id: String,
    pub arn: String,
    pub owner: u64,
    pub region: String,
    pub key_state: RwLock<String>,
    pub key_usage: String,
    pub key_spec: String,
    pub description: RwLock<String>,
    pub enabled: RwLock<bool>,
    /// Key material for symmetric/HMAC keys (random bytes).
    pub key_material: RwLock<Vec<u8>>,
    pub created_ms: u64,
    pub origin: RwLock<String>,
    pub multi_region: RwLock<bool>,
    pub tags: RwLock<Vec<Value>>,
    pub rotation_enabled: RwLock<bool>,
    /// Timestamp (ms) of the last on-demand rotation, if any.
    pub last_rotation_ms: RwLock<Option<u64>>,
    /// Key policies by policy name (always at least `default`).
    pub policies: RwLock<HashMap<String, String>>,
    /// True once `ImportKeyMaterial` succeeded on an EXTERNAL-origin key.
    pub has_imported_material: RwLock<bool>,
    /// RSA private key for asymmetric RSA specs.
    pub rsa_key: RwLock<Option<rsa::RsaPrivateKey>>,
    /// EC secret key for ECC specs.
    pub ec_key: RwLock<Option<p256::SecretKey>>,
}

impl Key {
    pub fn new(account: u64, region: String, key_usage: &str, key_spec: &str) -> Self {
        let key_id = uuid::Uuid::new_v4().simple().to_string();
        Self::with_id(key_id, account, region, key_usage, key_spec)
    }

    pub fn new_multi_region(account: u64, region: String, key_usage: &str, key_spec: &str) -> Self {
        let key_id = format!("mrk-{}", uuid::Uuid::new_v4().simple());
        Self::with_id(key_id, account, region, key_usage, key_spec)
    }

    pub fn with_id(
        key_id: String,
        account: u64,
        region: String,
        key_usage: &str,
        key_spec: &str,
    ) -> Self {
        let material: Vec<u8> = (0..32).map(|_| rand::random::<u8>()).collect();
        Self {
            key_id: key_id.clone(),
            arn: format!("arn:aws:kms:{}:{}:key/{}", region, account, key_id),
            owner: account,
            region,
            key_state: RwLock::new("Enabled".to_string()),
            key_usage: key_usage.to_string(),
            key_spec: key_spec.to_string(),
            description: RwLock::new(String::new()),
            enabled: RwLock::new(true),
            key_material: RwLock::new(material),
            created_ms: chrono::Utc::now().timestamp_millis() as u64,
            origin: RwLock::new("AWS_KMS".to_string()),
            multi_region: RwLock::new(false),
            tags: RwLock::new(Vec::new()),
            rotation_enabled: RwLock::new(false),
            last_rotation_ms: RwLock::new(None),
            policies: RwLock::new(HashMap::new()),
            has_imported_material: RwLock::new(false),
            rsa_key: RwLock::new(None),
            ec_key: RwLock::new(None),
        }
    }

    /// True when the spec represents an RSA key pair.
    pub fn is_rsa(&self) -> bool {
        self.key_spec.starts_with("RSA_")
    }

    /// True when the spec represents an ECC key pair.
    pub fn is_ecc(&self) -> bool {
        self.key_spec.starts_with("ECC_")
    }

    /// Secondary key material: generate and store the asymmetric key pair.
    pub fn generate_key_material(&self) {
        if self.is_rsa() {
            let mut rng = rand::thread_rng();
            if let Ok(priv_key) = rsa::RsaPrivateKey::new(&mut rng, self.rsa_bits()) {
                *self.rsa_key.write() = Some(priv_key);
            }
        } else if self.is_ecc() {
            // P-256 is the only ECC spec moto-style emulators commonly serve here.
            let mut secret_bytes = [0u8; 32];
            for b in secret_bytes.iter_mut() {
                *b = rand::random::<u8>();
            }
            if let Ok(secret) = p256::SecretKey::from_slice(&secret_bytes) {
                *self.ec_key.write() = Some(secret);
            }
        }
    }

    /// RSA modulus bits for the key spec.
    fn rsa_bits(&self) -> usize {
        match self.key_spec.as_str() {
            "RSA_3072" => 3072,
            "RSA_4096" => 4096,
            _ => 2048,
        }
    }

    /// Algorithms supported by the key (mirrors AWS KeyMetadata fields).
    pub fn algorithms(&self) -> (Vec<&'static str>, Vec<&'static str>, Vec<&'static str>) {
        match (self.key_usage.as_str(), self.key_spec.as_str()) {
            ("ENCRYPT_DECRYPT", "SYMMETRIC_DEFAULT") => (
                vec!["SYMMETRIC_DEFAULT"],
                vec![],
                vec![],
            ),
            ("ENCRYPT_DECRYPT", s) if s.starts_with("RSA_") => (
                vec!["RSAES_OAEP_SHA_1", "RSAES_OAEP_SHA_256"],
                vec![],
                vec![],
            ),
            ("SIGN_VERIFY", s) if s.starts_with("RSA_") => (
                vec![],
                vec![
                    "RSASSA_PKCS1_V1_5_SHA_256",
                    "RSASSA_PKCS1_V1_5_SHA_384",
                    "RSASSA_PKCS1_V1_5_SHA_512",
                    "RSASSA_PSS_SHA_256",
                    "RSASSA_PSS_SHA_384",
                    "RSASSA_PSS_SHA_512",
                ],
                vec![],
            ),
            ("SIGN_VERIFY", "ECC_NIST_P256") => (vec![], vec!["ECDSA_SHA_256"], vec![]),
            ("SIGN_VERIFY", "ECC_NIST_P384") => (vec![], vec!["ECDSA_SHA_384"], vec![]),
            ("SIGN_VERIFY", "ECC_NIST_P521") => (vec![], vec!["ECDSA_SHA_512"], vec![]),
            ("SIGN_VERIFY", "ECC_SECG_P256K1") => (vec![], vec!["ECDSA_SHA_256"], vec![]),
            ("GENERATE_VERIFY_MAC", "HMAC_256") => (vec![], vec![], vec!["HMAC_SHA_256"]),
            ("GENERATE_VERIFY_MAC", "HMAC_384") => (vec![], vec![], vec!["HMAC_SHA_384"]),
            ("GENERATE_VERIFY_MAC", "HMAC_512") => (vec![], vec![], vec!["HMAC_SHA_512"]),
            _ => (vec![], vec![], vec![]),
        }
    }
}

/// A KMS grant.
#[derive(Debug, Clone)]
pub struct Grant {
    pub grant_id: String,
    pub grant_token: String,
    pub key_id: String,
    pub grantee_principal: String,
    pub retiring_principal: String,
    pub operations: Vec<String>,
    /// Raw Constraints JSON (optional).
    pub constraints: Option<Value>,
    pub name: String,
    pub issuing_account: String,
    pub creation_date_ms: u64,
}

/// A custom key store (CloudHSM-backed).
#[derive(Debug)]
pub struct CustomKeyStore {
    pub id: String,
    pub name: RwLock<String>,
    pub cloud_hsm_cluster_id: RwLock<String>,
    pub trust_anchor_certificate: String,
    pub key_store_password: String,
    pub connection_state: RwLock<String>,
    pub creation_date_ms: u64,
}

/// The KMS state store (per account+region).
#[derive(Clone)]
pub struct KmsState {
    pub keys: Arc<RwLock<HashMap<String, Arc<Key>>>>,
    pub aliases: Arc<RwLock<HashMap<String, String>>>,
    pub tags: Arc<RwLock<HashMap<String, serde_json::Map<String, Value>>>>,
    pub grants: Arc<RwLock<Vec<Grant>>>,
    pub custom_key_stores: Arc<RwLock<HashMap<String, Arc<CustomKeyStore>>>>,
}

impl KmsState {
    pub fn new() -> Self {
        Self {
            keys: Arc::new(RwLock::new(HashMap::new())),
            aliases: Arc::new(RwLock::new(HashMap::new())),
            tags: Arc::new(RwLock::new(HashMap::new())),
            grants: Arc::new(RwLock::new(Vec::new())),
            custom_key_stores: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn create_alias(&self, alias_name: &str, key_id: &str) {
        self.aliases.write().insert(alias_name.to_string(), key_id.to_string());
    }

    pub fn update_alias(&self, alias_name: &str, key_id: &str) {
        self.aliases.write().insert(alias_name.to_string(), key_id.to_string());
    }

    pub fn delete_alias(&self, alias_name: &str) {
        self.aliases.write().remove(alias_name);
    }

    pub fn list_aliases(&self) -> Vec<(String, String)> {
        let mut out: Vec<(String, String)> = self
            .aliases
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        out.sort();
        out
    }

    pub fn get_key(&self, id: &str) -> Option<Arc<Key>> {
        // Try direct lookup, then by ARN (`key/{id}` suffix accepted), then by alias
        let direct = if id.starts_with("arn:aws:kms:") {
            id.rsplit('/').next().unwrap_or(id)
        } else {
            id
        };
        let keys = self.keys.read();
        if let Some(k) = keys.get(direct) {
            return Some(k.clone());
        }
        if let Some(k) = keys.values().find(|k| k.arn == id) {
            return Some(k.clone());
        }
        // Try alias resolution
        let aliases = self.aliases.read();
        if let Some(target) = aliases.get(id) {
            if let Some(k) = keys.get(target) {
                return Some(k.clone());
            }
            return keys.values().find(|k| k.arn == *target).cloned();
        }
        None
    }

    pub fn put_key(&self, key: Arc<Key>) {
        self.keys.write().insert(key.key_id.clone(), key);
    }

    pub fn list_keys(&self) -> Vec<Arc<Key>> {
        let mut keys: Vec<Arc<Key>> = self.keys.read().values().cloned().collect();
        keys.sort_by(|a, b| a.key_id.cmp(&b.key_id));
        keys
    }

    pub fn delete_key(&self, id: &str) -> Option<Arc<Key>> {
        let key = self.get_key(id)?;
        self.keys.write().remove(&key.key_id)
    }
}

impl Default for KmsState {
    fn default() -> Self {
        Self::new()
    }
}
