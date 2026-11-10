//! S3 in-memory state models.

use chrono::Utc;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Version id assigned to objects in unversioned buckets (matches AWS).
pub const NULL_VERSION_ID: &str = "null";

/// Moto's canned owner identity, used in ACL/loose owner fields.
pub const OWNER_ID: &str = "75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a";
pub const OWNER_DISPLAY: &str = "webfile";

/// One stored revision of an object (a version row).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectVersion {
    pub version_id: String,
    pub seq: u64,
    #[serde(with = "crate::snapshot::b64")]
    pub data: Vec<u8>,
    pub content_type: String,
    pub metadata: HashMap<String, String>,
    pub etag: String,
    pub size: usize,
    pub last_modified: u64,
    pub storage_class: String,
    /// Tag set in STORE (insertion) order — moto's tagger renders
    /// `self.tags[arn].items()` in insertion order, so GET TagSet must not
    /// depend on HashMap iteration. Duplicates collapse at the wire-parse
    /// layer (first position, last value) before storage.
    #[serde(default, with = "crate::snapshot::ordered_tags")]
    pub tags: Vec<(String, String)>,
    pub legal_hold: Option<String>,
    #[serde(default)]
    pub retention: Option<(String, String)>,
    /// Restore state from RestoreObject — moto's `x-amz-restore` response
    /// dict entry; a GLACIER/DEEP_ARCHIVE object only serves normal reads
    /// while a completed restore marker is present.
    #[serde(default)]
    pub restore: Option<String>,
    /// moto's per-object checksum_value (models.py:184 + transfers on
    /// copy): stored ONLY when the upload carried a checksum-algorithm
    /// request header (CRC32 slice) — CopyObjectResult renders it
    /// conditionally (`if new_key.checksum_value`, responses.py:
    /// 2463-2470) and copies propagate it from the source unless
    /// overridden. CompleteMultipartUploadResult never renders a
    /// checksum (hard-coded Location/Bucket/Key/ETag, responses.py:
    /// 3209-3217).
    #[serde(default)]
    pub checksum_value: Option<(String, String)>,
}

impl ObjectVersion {
    pub fn null_version(data: Vec<u8>, content_type: String) -> Self {
        let size = data.len();
        let etag = md5_hex(&data);
        Self {
            version_id: NULL_VERSION_ID.to_string(),
            seq: 0,
            data,
            content_type,
            metadata: HashMap::new(),
            etag,
            size,
            last_modified: Utc::now().timestamp() as u64,
            storage_class: "STANDARD".to_string(),
            tags: Vec::new(),
            legal_hold: None,
            retention: None,
            restore: None,
            checksum_value: None,
        }
    }
}

/// A delete marker placeholder created by DeleteObject on versioned buckets.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteMarker {
    pub version_id: String,
    pub seq: u64,
    pub last_modified: u64,
}

/// All revisions of one object key, newest first within each list.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ObjectEntry {
    #[serde(default)]
    pub versions: Vec<ObjectVersion>,
    #[serde(default)]
    pub markers: Vec<DeleteMarker>,
}

impl ObjectEntry {
    pub fn is_empty(&self) -> bool {
        self.versions.is_empty() && self.markers.is_empty()
    }

    /// True when the newest record (version or marker) is a live version.
    pub fn is_live(&self) -> bool {
        match (self.versions.first(), self.markers.first()) {
            (Some(v), Some(m)) => v.seq > m.seq,
            (Some(_), None) => true,
            (None, _) => false,
        }
    }

    /// Find a stored record (version or marker) by version id.
    pub fn has_version_id(&self, id: &str) -> bool {
        self.versions.iter().any(|v| v.version_id == id)
            || self.markers.iter().any(|m| m.version_id == id)
    }
}

/// A notification target (queue, topic, or lambda) entry.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TargetConfig {
    pub arn: String,
    pub events: Vec<String>,
    pub prefix: Option<String>,
    pub suffix: Option<String>,
}

/// Parsed bucket notification configuration, used for event firing.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NotificationConfig {
    #[serde(default)]
    pub queues: Vec<TargetConfig>,
    #[serde(default)]
    pub topics: Vec<TargetConfig>,
    #[serde(default)]
    pub lambdas: Vec<TargetConfig>,
    #[serde(default)]
    pub eventbridge: bool,
}

/// A multipart upload in progress.
#[derive(Debug, Serialize, Deserialize)]
pub struct MultipartUpload {
    pub upload_id: String,
    pub key: String,
    pub initiated: u64,
    #[serde(with = "crate::snapshot::rw")]
    pub parts: parking_lot::RwLock<std::collections::BTreeMap<u32, MultipartPart>>,
    pub content_type: String,
    pub metadata: HashMap<String, String>,
    /// Storage class captured at CreateMultipartUpload (moto validates it
    /// there and the completed object inherits it).
    #[serde(default)]
    pub storage_class: String,
    /// x-amz-tagging captured (collapsed, first-value) at
    /// CreateMultipartUpload (moto responses.py:3137 stores it raw on the
    /// multipart); validated and applied at CompleteMultipartUpload
    /// (models.py:2744 routes through put_object_tagging) — the create
    /// itself does NOT validate.
    #[serde(default)]
    pub pending_tags: Vec<(String, String)>,
}

impl MultipartUpload {
    pub fn new(key: String, content_type: String) -> Self {
        Self {
            upload_id: uuid::Uuid::new_v4().simple().to_string(),
            key,
            initiated: Utc::now().timestamp() as u64,
            parts: parking_lot::RwLock::new(std::collections::BTreeMap::new()),
            content_type,
            storage_class: "STANDARD".to_string(),
            metadata: HashMap::new(),
            pending_tags: Vec::new(),
        }
    }
}

/// A single part of a multipart upload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartPart {
    pub part_number: u32,
    pub etag: String,
    pub size: usize,
    #[serde(with = "crate::snapshot::b64")]
    pub data: Vec<u8>,
    pub last_modified: u64,
}

/// A single S3 bucket with its configurations.
#[derive(Debug, Serialize, Deserialize)]
pub struct Bucket {
    pub name: String,
    pub region: String,
    pub created: u64,
    pub directory: bool,
    #[serde(with = "crate::snapshot::rw")]
    pub versioning: RwLock<Option<bool>>,
    /// moto stores PutBucketVersioning's `<Status>` verbatim (any [A-Za-z]+
    /// matches — models.py:2062-2066) and GetBucketVersioning echoes it;
    /// non-Enabled/Suspended strings are NOT versioned for feature
    /// purposes but must round-trip. None when never set (and for the
    /// feature branches the derived Option<bool> is the truth).
    #[serde(default, with = "crate::snapshot::rw")]
    pub versioning_status: RwLock<Option<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub policy: RwLock<Option<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub tags_xml: RwLock<Option<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub cors_xml: RwLock<Option<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub lifecycle_xml: RwLock<Option<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub website_xml: RwLock<Option<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub encryption_xml: RwLock<Option<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub logging_xml: RwLock<Option<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub accelerate_xml: RwLock<Option<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub request_payment_xml: RwLock<Option<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub ownership_xml: RwLock<Option<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub public_access_block_xml: RwLock<Option<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub object_lock_enabled: RwLock<bool>,
    #[serde(with = "crate::snapshot::rw")]
    pub object_lock_xml: RwLock<Option<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub replication_xml: RwLock<Option<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub analytics_config: RwLock<HashMap<String, String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub metrics_config: RwLock<HashMap<String, String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub inventory_config: RwLock<HashMap<String, String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub intelligent_tiering: RwLock<HashMap<String, String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub metadata_config: RwLock<Option<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub metadata_table_config: RwLock<Option<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub notification_xml: RwLock<Option<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub notification_config: RwLock<Option<NotificationConfig>>,
    #[serde(with = "crate::snapshot::rw")]
    pub objects: RwLock<HashMap<String, ObjectEntry>>,
    #[serde(with = "crate::snapshot::rw")]
    pub multipart_uploads: RwLock<HashMap<String, MultipartUpload>>,
    #[serde(with = "crate::snapshot::atomic_u64")]
    pub seq: AtomicU64,
    pub acl: String,
}

impl Bucket {
    pub fn new(name: String, region: String) -> Self {
        Self {
            name,
            region,
            created: Utc::now().timestamp() as u64,
            directory: false,
            versioning: RwLock::new(None),
            versioning_status: RwLock::new(None),
            policy: RwLock::new(None),
            tags_xml: RwLock::new(None),
            cors_xml: RwLock::new(None),
            lifecycle_xml: RwLock::new(None),
            website_xml: RwLock::new(None),
            encryption_xml: RwLock::new(None),
            logging_xml: RwLock::new(None),
            accelerate_xml: RwLock::new(None),
            request_payment_xml: RwLock::new(None),
            ownership_xml: RwLock::new(None),
            public_access_block_xml: RwLock::new(None),
            object_lock_enabled: RwLock::new(false),
            object_lock_xml: RwLock::new(None),
            replication_xml: RwLock::new(None),
            analytics_config: RwLock::new(HashMap::new()),
            metrics_config: RwLock::new(HashMap::new()),
            inventory_config: RwLock::new(HashMap::new()),
            intelligent_tiering: RwLock::new(HashMap::new()),
            metadata_config: RwLock::new(None),
            metadata_table_config: RwLock::new(None),
            notification_xml: RwLock::new(None),
            notification_config: RwLock::new(None),
            objects: RwLock::new(HashMap::new()),
            multipart_uploads: RwLock::new(HashMap::new()),
            seq: AtomicU64::new(0),
            acl: "private".to_string(),
        }
    }

    pub fn is_versioned(&self) -> bool {
        *self.versioning.read() == Some(true)
    }

    pub fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::SeqCst)
    }

    /// True when the bucket holds any object version or delete marker.
    pub fn has_content(&self) -> bool {
        self.objects.read().values().any(|e| !e.is_empty())
    }
}

/// The S3 state store (per account+region).
#[derive(Clone)]
pub struct S3State {
    pub buckets: Arc<RwLock<HashMap<String, Arc<Bucket>>>>,
}
impl S3State {
    pub fn new() -> Self {
        Self {
            buckets: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn get_bucket(&self, name: &str) -> Option<Arc<Bucket>> {
        self.buckets.read().get(name).cloned()
    }

    pub fn put_bucket(&self, bucket: Arc<Bucket>) {
        self.buckets.write().insert(bucket.name.clone(), bucket);
    }

    pub fn delete_bucket(&self, name: &str) -> Option<Arc<Bucket>> {
        self.buckets.write().remove(name)
    }

    pub fn list_buckets(&self) -> Vec<(String, u64)> {
        self.buckets
            .read()
            .iter()
            .map(|(name, b)| (name.clone(), b.created))
            .collect()
    }
}

impl Default for S3State {
    fn default() -> Self {
        Self::new()
    }
}

/// Compute the hexadecimal md5 digest of a byte slice (no quotes).
pub fn md5_hex(data: &[u8]) -> String {
    format!("{:x}", md5::compute(data))
}

/// Compute the multipart-composite ETag: `md5(md5s-binary-concat)-<count>`.
pub fn multipart_etag(part_etags: &[String], part_count: usize) -> String {
    let mut concat = Vec::new();
    for et in part_etags {
        concat.extend_from_slice(&hex::decode(et).unwrap_or_default());
    }
    let digest = md5_hex(&concat);
    format!("{}-{}", digest, part_count)
}
