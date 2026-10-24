//! AWS FSx backend — moto-equivalent (json protocol).
//!
//! Mirrors the deployed moto fork's fsx backend (`moto/fsx` models.py +
//! responses.py): per-(account, region) state for file systems, backups,
//! storage virtual machines, volumes, snapshots and data-repository
//! associations, plus moto's TaggingService. Python quirks the probe
//! recorded stay intact: `CreateFileSystem` with a bogus FileSystemType
//! crashes into a 500 `InternalError` ("Invalid FileSystemType: ..."),
//! `DeleteFileSystem` on an unknown id is the unguarded KeyError 500, ID
//! formats are moto's (`fs-`/`backup-` 8 hex, `svm-`/`fsvol-`/`fsvolsnap-`
//! 17 hex, `dra-` 9 hex) and file-system types validate against moto's
//! enum (`OPENZFS`, not `OPEN_ZFS`). Wave-12 additions (data-repository
//! tasks, file caches, S3 access-point attachments, backup copies and the
//! shared-VPC/SVM/file-system lifecycle helpers) are modeled straight from
//! botocore's shapes, keeping moto's ID formats. Only `UpdateVolume`,
//! `CopySnapshotAndUpdateVolume` and `RestoreVolumeFromSnapshot` — and the
//! generic stub sweep beyond it — still answer the generic 501
//! NotImplemented, as they do in moto.

use crate::proto::{json_error, now_unix, parse_json_body};
use crate::store::{self, ScopeKey};
use crate::{Request, Response};
use base64::Engine as _;
use parking_lot::RwLock;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

type Stores = Arc<RwLock<HashMap<ScopeKey, FsxStore>>>;

fn stores() -> Stores {
    store::slot::<FsxStore>("fsx")
}

/// Per-(account, region) in-memory state for Amazon FSx.
#[derive(Default)]
struct FsxStore {
    file_systems: BTreeMap<String, FileSystem>,
    backups: BTreeMap<String, Backup>,
    storage_virtual_machines: BTreeMap<String, StorageVirtualMachine>,
    volumes: BTreeMap<String, Volume>,
    snapshots: BTreeMap<String, Snapshot>,
    data_repository_associations: BTreeMap<String, DataRepositoryAssociation>,
    data_repository_tasks: BTreeMap<String, DataRepositoryTask>,
    file_caches: BTreeMap<String, FileCache>,
    /// attachment name -> S3 access point attachment
    s3_access_points: BTreeMap<String, S3AccessPointAttachment>,
    /// `UpdateSharedVpcConfiguration` override ("true" | "false").
    shared_vpc_route_table_updates: bool,
    /// ARN -> {key: value} — moto's TaggingService.
    tags: BTreeMap<String, BTreeMap<String, Option<String>>>,
}

fn with_store(req: &Request, f: impl FnOnce(&mut FsxStore) -> Response) -> Response {
    let slot = stores();
    let mut map = slot.write();
    let st = map.entry((req.account, req.region.clone())).or_default();
    f(st)
}

// ---------------------------------------------------------------------------
// Records — `to_dict` mirrors moto's member-dropping serialization
// ---------------------------------------------------------------------------

struct FileSystem {
    file_system_id: String,
    file_system_type: String,
    storage_capacity: Option<i64>,
    storage_type: Option<String>,
    subnet_ids: Vec<String>,
    security_group_ids: Vec<String>,
    dns_name: String,
    kms_key_id: Option<String>,
    resource_arn: String,
    windows_configuration: Option<Value>,
    lustre_configuration: Option<Value>,
    ontap_configuration: Option<Value>,
    open_zfs_configuration: Option<Value>,
    aliases: Vec<Value>,
}

impl FileSystem {
    /// moto's `FileSystem.to_dict()` drops falsy members (empty tags list,
    /// absent capacity, empty group list, ...).
    fn to_dict(&self, tags: Vec<Value>) -> Value {
        let mut out = json!({
            "FileSystemId": self.file_system_id,
            "FileSystemType": self.file_system_type,
            "StorageCapacity": self.storage_capacity,
            "StorageType": self.storage_type,
            "SubnetIds": self.subnet_ids,
            "SecurityGroupIds": self.security_group_ids,
            "Tags": tags,
            "DNSName": self.dns_name,
            "KmsKeyId": self.kms_key_id,
            "ResourceARN": self.resource_arn,
            "WindowsConfiguration": self.windows_configuration,
            "LustreConfiguration": self.lustre_configuration,
            "OntapConfiguration": self.ontap_configuration,
            "OpenZFSConfiguration": self.open_zfs_configuration,
        });
        strip_falsy(out.as_object_mut().expect("object"));
        out
    }
}

#[derive(Clone)]
struct Backup {
    backup_id: String,
    file_system_id: String,
    client_request_token: String,
    volume_id: Option<String>,
    resource_arn: String,
    lifecycle: String,
    typ: String,
    source_backup_id: Option<String>,
    source_backup_region: Option<String>,
    creation_time: f64,
}

impl Backup {
    /// moto's `Backup.to_dict()` drops `None` members but keeps empty lists.
    fn to_dict(&self, tags: Vec<Value>) -> Value {
        let mut out = json!({
            "BackupId": self.backup_id,
            "FileSystemId": self.file_system_id,
            "VolumeId": self.volume_id,
            "Lifecycle": self.lifecycle,
            "Type": self.typ,
            "SourceBackupId": self.source_backup_id,
            "SourceBackupRegion": self.source_backup_region,
            "CreationTime": self.creation_time,
            "Tags": tags,
            "ResourceARN": self.resource_arn,
            "ClientRequestToken": self.client_request_token,
        });
        strip_nulls(out.as_object_mut().expect("object"));
        out
    }
}

struct StorageVirtualMachine {
    storage_virtual_machine_id: String,
    file_system_id: String,
    name: Option<String>,
    active_directory_configuration: Option<Value>,
    root_volume_security_style: String,
    resource_arn: String,
    lifecycle: String,
    creation_time: f64,
}

impl StorageVirtualMachine {
    fn to_dict(&self, tags: Vec<Value>) -> Value {
        let mut out = json!({
            "StorageVirtualMachineId": self.storage_virtual_machine_id,
            "FileSystemId": self.file_system_id,
            "Name": self.name,
            "Lifecycle": self.lifecycle,
            "CreationTime": self.creation_time,
            "ResourceARN": self.resource_arn,
            "RootVolumeSecurityStyle": self.root_volume_security_style,
            "Tags": tags,
            "ActiveDirectoryConfiguration": self.active_directory_configuration,
        });
        strip_nulls(out.as_object_mut().expect("object"));
        out
    }
}

struct Volume {
    volume_id: String,
    volume_type: Option<String>,
    name: Option<String>,
    file_system_id: Option<String>,
    resource_arn: String,
    lifecycle: String,
    creation_time: f64,
    ontap_configuration: Option<Value>,
    open_zfs_configuration: Option<Value>,
}

impl Volume {
    fn to_dict(&self, tags: Vec<Value>) -> Value {
        let mut out = json!({
            "VolumeId": self.volume_id,
            "VolumeType": self.volume_type,
            "Name": self.name,
            "FileSystemId": self.file_system_id,
            "Lifecycle": self.lifecycle,
            "CreationTime": self.creation_time,
            "ResourceARN": self.resource_arn,
            "Tags": tags,
            "OntapConfiguration": self.ontap_configuration,
            "OpenZFSConfiguration": self.open_zfs_configuration,
        });
        strip_nulls(out.as_object_mut().expect("object"));
        out
    }
}

struct Snapshot {
    snapshot_id: String,
    name: Option<String>,
    volume_id: String,
    resource_arn: String,
    lifecycle: String,
    creation_time: f64,
}

impl Snapshot {
    fn to_dict(&self, tags: Vec<Value>) -> Value {
        let mut out = json!({
            "SnapshotId": self.snapshot_id,
            "Name": self.name,
            "VolumeId": self.volume_id,
            "Lifecycle": self.lifecycle,
            "CreationTime": self.creation_time,
            "ResourceARN": self.resource_arn,
            "Tags": tags,
        });
        strip_nulls(out.as_object_mut().expect("object"));
        out
    }
}

struct DataRepositoryAssociation {
    association_id: String,
    file_system_id: String,
    file_cache_id: Option<String>,
    file_system_path: Option<String>,
    data_repository_path: String,
    batch_import_meta_data_on_create: Option<bool>,
    imported_file_chunk_size: Option<i64>,
    s3: Option<Value>,
    resource_arn: String,
    lifecycle: String,
    creation_time: f64,
}

impl DataRepositoryAssociation {
    fn to_dict(&self, tags: Vec<Value>) -> Value {
        let mut out = json!({
            "AssociationId": self.association_id,
            "FileSystemId": if self.file_system_id.is_empty() { Value::Null } else { json!(self.file_system_id) },
            "FileCacheId": self.file_cache_id,
            "FileSystemPath": self.file_system_path,
            "DataRepositoryPath": self.data_repository_path,
            "BatchImportMetaDataOnCreate": self.batch_import_meta_data_on_create,
            "ImportedFileChunkSize": self.imported_file_chunk_size,
            "S3": self.s3,
            "Lifecycle": self.lifecycle,
            "CreationTime": self.creation_time,
            "ResourceARN": self.resource_arn,
            "Tags": tags,
        });
        strip_nulls(out.as_object_mut().expect("object"));
        out
    }
}

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "fsx" {
        return None;
    }
    // CopyBackup consults another region's backup store; answer it without
    // holding this scope's write lock (read-source first, then write-target).
    if req.operation == "CopyBackup" {
        return Some(copy_backup(req));
    }
    Some(with_store(req, |st| route(req, st)))
}

fn route(req: &Request, st: &mut FsxStore) -> Response {
    let body = parse_json_body(&req.body);
    let account = req.account;
    let region = req.region.clone();
    match req.operation.as_str() {
        // --- file systems -----------------------------------------------------
        "CreateFileSystem" => create_file_system(st, account, &region, &body),
        "DescribeFileSystems" => describe_file_systems(st, &body),
        "UpdateFileSystem" => update_file_system(st, &body),
        "DeleteFileSystem" => delete_file_system(st, &body),
        "CreateFileSystemFromBackup" => create_file_system_from_backup(st, account, &region, &body),
        // --- backups -------------------------------------------------------------
        "CreateBackup" => create_backup(st, account, &region, &body),
        "DeleteBackup" => delete_backup(st, &body),
        "DescribeBackups" => describe_backups(st, &body),
        // --- storage virtual machines --------------------------------------------
        "CreateStorageVirtualMachine" => {
            create_storage_virtual_machine(st, account, &region, &body)
        }
        "DeleteStorageVirtualMachine" => delete_storage_virtual_machine(st, &body),
        "DescribeStorageVirtualMachines" => describe_storage_virtual_machines(st, &body),
        // --- volumes ----------------------------------------------------------------
        "CreateVolume" => create_volume(st, account, &region, &body),
        "CreateVolumeFromBackup" => create_volume_from_backup(st, account, &region, &body),
        "DeleteVolume" => delete_volume(st, &body),
        "DescribeVolumes" => describe_volumes(st, &body),
        // --- snapshots ------------------------------------------------------------------
        "CreateSnapshot" => create_snapshot(st, account, &region, &body),
        "DeleteSnapshot" => delete_snapshot(st, &body),
        "UpdateSnapshot" => update_snapshot(st, &body),
        "DescribeSnapshots" => describe_snapshots(st, &body),
        // --- file system aliases ------------------------------------------------------------
        "AssociateFileSystemAliases" => associate_file_system_aliases(st, &body),
        "DisassociateFileSystemAliases" => disassociate_file_system_aliases(st, &body),
        "DescribeFileSystemAliases" => describe_file_system_aliases(st, &body),
        // --- data repository associations ------------------------------------------------------
        "CreateDataRepositoryAssociation" => {
            create_data_repository_association(st, account, &region, &body)
        }
        "UpdateDataRepositoryAssociation" => update_data_repository_association(st, &body),
        "DeleteDataRepositoryAssociation" => delete_data_repository_association(st, &body),
        "DescribeDataRepositoryAssociations" => describe_data_repository_associations(st, &body),
        // --- data repository tasks ---------------------------------------------------
        "CreateDataRepositoryTask" => create_data_repository_task(st, account, &region, &body),
        "CancelDataRepositoryTask" => cancel_data_repository_task(st, &body),
        "DescribeDataRepositoryTasks" => describe_data_repository_tasks(st, &body),
        // --- file caches ----------------------------------------------------------------
        "CreateFileCache" => create_file_cache(st, account, &region, &body),
        "DeleteFileCache" => delete_file_cache(st, &body),
        "UpdateFileCache" => update_file_cache(st, &body),
        "DescribeFileCaches" => describe_file_caches(st, &body),
        // --- S3 access point attachments ----------------------------------------------------
        "CreateAndAttachS3AccessPoint" => {
            create_and_attach_s3_access_point(st, account, &region, &body)
        }
        "DetachAndDeleteS3AccessPoint" => detach_and_delete_s3_access_point(st, &body),
        "DescribeS3AccessPointAttachments" => describe_s3_access_point_attachments(st, &body),
        // --- file system lifecycle helpers ---------------------------------------------------
        "ReleaseFileSystemNfsV3Locks" => release_file_system_nfs_v3_locks(st, &body),
        "StartMisconfiguredStateRecovery" => start_misconfigured_state_recovery(st, &body),
        "UpdateSharedVpcConfiguration" => update_shared_vpc_configuration(st, &body),
        "UpdateStorageVirtualMachine" => update_storage_virtual_machine(st, &body),
        // --- describe-only surfaces moto leaves empty ---------------------------------------------
        "DescribeSharedVpcConfiguration" => ok(json!({
            "EnableFsxRouteTableUpdatesFromParticipantAccounts": if st.shared_vpc_route_table_updates { "true" } else { "false" }
        })),
        // --- tags -------------------------------------------------------------------------------
        "TagResource" => {
            tag_insert(
                st,
                as_str(&body, "ResourceARN"),
                body.get("Tags").and_then(Value::as_array),
            );
            ok_empty()
        }
        "UntagResource" => {
            untag_resource(
                st,
                as_str(&body, "ResourceARN"),
                as_str_list(body.get("TagKeys")),
            );
            ok_empty()
        }
        "ListTagsForResource" => {
            let tags = tags_by_ref(st, as_str(&body, "ResourceARN"));
            ok(json!({ "Tags": tags }))
        }
        // --- moto-era gaps still minus a handler -----------------------------
        other => json_error(
            501,
            "NotImplemented",
            &format!("The {other} action has not been implemented"),
        ),
    }
}

// ---------------------------------------------------------------------------
// File systems
// ---------------------------------------------------------------------------

fn create_file_system(st: &mut FsxStore, account: u64, region: &str, body: &Value) -> Response {
    let file_system_type = py_str(body.get("FileSystemType"));
    if !valid_file_system_type(&file_system_type) {
        // moto's FileSystem.__init__ raises ValueError; the sidecar turns
        // it into a 500 with the raw message (probe: 500_error).
        return internal_error_500(&format!("Invalid FileSystemType: {file_system_type}"));
    }
    let file_system_id = hex_id("fs-", 8);
    let record = FileSystem {
        file_system_id: file_system_id.clone(),
        file_system_type,
        storage_capacity: body.get("StorageCapacity").and_then(Value::as_i64),
        storage_type: str_opt(body.get("StorageType")),
        subnet_ids: as_str_list(body.get("SubnetIds")),
        security_group_ids: as_str_list(body.get("SecurityGroupIds")),
        dns_name: format!("{file_system_id}.fsx.{region}.amazonaws.com"),
        kms_key_id: str_opt(body.get("KmsKeyId")),
        resource_arn: format!("arn:aws:fsx:{region}:{account}:file-system/{file_system_id}"),
        windows_configuration: body
            .get("WindowsConfiguration")
            .filter(|v| !v.is_null())
            .cloned(),
        lustre_configuration: body
            .get("LustreConfiguration")
            .filter(|v| !v.is_null())
            .cloned(),
        ontap_configuration: body
            .get("OntapConfiguration")
            .filter(|v| !v.is_null())
            .cloned(),
        open_zfs_configuration: body
            .get("OpenZFSConfiguration")
            .filter(|v| !v.is_null())
            .cloned(),
        aliases: Vec::new(),
    };
    let arn = record.resource_arn.clone();
    if let Some(tags) = non_empty_tags(body) {
        tag_insert(st, &arn, Some(tags));
    }
    let dict = record.to_dict(tags_by_ref(st, &arn));
    st.file_systems
        .insert(record.file_system_id.clone(), record);
    ok(json!({ "FileSystem": dict }))
}

/// moto's enum (note: `OPENZFS`, not `OPEN_ZFS`).
fn valid_file_system_type(file_system_type: &str) -> bool {
    matches!(file_system_type, "LUSTRE" | "ONTAP" | "OPENZFS" | "WINDOWS")
}

fn describe_file_systems(st: &mut FsxStore, body: &Value) -> Response {
    let ids = as_str_list(body.get("FileSystemIds"));
    let collected: Vec<&FileSystem> = if ids.is_empty() {
        st.file_systems.values().collect()
    } else {
        // moto silently skips unknown ids.
        st.file_systems
            .values()
            .filter(|fs| ids.contains(&fs.file_system_id))
            .collect()
    };
    let items: Vec<(String, Value)> = collected
        .into_iter()
        .map(|fs| {
            (
                fs.resource_arn.clone(),
                fs.to_dict(tags_by_ref(st, &fs.resource_arn)),
            )
        })
        .collect();
    let (page, next) = match paginate(items, max_results(body), token(body)) {
        Ok(result) => result,
        Err(resp) => return resp,
    };
    ok(json!({ "FileSystems": page, "NextToken": next }))
}

fn update_file_system(st: &mut FsxStore, body: &Value) -> Response {
    let id = as_str(body, "FileSystemId").to_string();
    let Some(fs) = st.file_systems.get(&id) else {
        return fs_not_found(&id);
    };
    let arn = fs.resource_arn.clone();
    let tags = tags_by_ref(st, &arn);
    let Some(fs) = st.file_systems.get_mut(&id) else {
        unreachable!("looked up one line above");
    };
    if let Some(capacity) = body.get("StorageCapacity").and_then(Value::as_i64) {
        fs.storage_capacity = Some(capacity);
    }
    if let Some(storage_type) = str_opt(body.get("StorageType")) {
        fs.storage_type = Some(storage_type);
    }
    merge_config(
        body.get("WindowsConfiguration"),
        &mut fs.windows_configuration,
    );
    merge_config(
        body.get("LustreConfiguration"),
        &mut fs.lustre_configuration,
    );
    merge_config(body.get("OntapConfiguration"), &mut fs.ontap_configuration);
    merge_config(
        body.get("OpenZFSConfiguration"),
        &mut fs.open_zfs_configuration,
    );
    let dict = fs.to_dict(tags);
    ok(json!({ "FileSystem": dict }))
}

/// moto's configuration merge: `{**(old or {}), **given}` — an object
/// merge; absent or non-object input (impossible through a real SDK) is a
/// no-op instead of the dict-unpacking TypeError.
fn merge_config(given: Option<&Value>, stored: &mut Option<Value>) {
    let Some(given) = given.filter(|v| v.is_object()) else {
        return;
    };
    let merged = match stored.take() {
        Some(Value::Object(mut old)) => {
            for (k, v) in given.as_object().expect("object") {
                old.insert(k.clone(), v.clone());
            }
            Value::Object(old)
        }
        _ => given.clone(),
    };
    *stored = Some(merged);
}

fn delete_file_system(st: &mut FsxStore, body: &Value) -> Response {
    let id = as_str(body, "FileSystemId").to_string();
    // moto indexes `self.file_systems[file_system_id]` unguarded: the
    // KeyError surfaces as a 500 whose message is the bare id.
    let Some(fs) = st.file_systems.get(&id) else {
        return internal_error_500(&format!("'{id}'"));
    };
    let fs_type = fs.file_system_type.clone();
    st.file_systems.remove(&id);
    let template = json!({ "FinalBackUpId": "", "FinalBackUpTags": [] });
    let mut out = json!({
        "FileSystemId": id,
        "Lifecycle": "DELETING",
        "WindowsResponse": Value::Null,
        "LustreResponse": Value::Null,
        "OpenZfsResponse": Value::Null,
    });
    match fs_type.as_str() {
        "WINDOWS" => out["WindowsResponse"] = template,
        "LUSTRE" => out["LustreResponse"] = template,
        "OPEN_ZFS" => out["OpenZfsResponse"] = template,
        _ => {}
    }
    ok(out)
}

// ---------------------------------------------------------------------------
// File system from backup — moto derives type/capacity/storage from the
// original file system when the request omits them
// ---------------------------------------------------------------------------

fn create_file_system_from_backup(
    st: &mut FsxStore,
    account: u64,
    region: &str,
    body: &Value,
) -> Response {
    let backup_id = as_str(body, "BackupId");
    let Some(backup) = st.backups.get(backup_id) else {
        return backup_not_found(backup_id);
    };
    let orig = st.file_systems.get(&backup.file_system_id);
    let file_system_type = orig
        .map(|fs| fs.file_system_type.clone())
        .unwrap_or_else(|| "LUSTRE".to_string());
    // python: `param or (orig.attr if orig else default)` — a truthy param
    // wins, else the original file system's value (even when None/0), else
    // the default.
    let storage_capacity: Option<i64> = match body
        .get("StorageCapacity")
        .and_then(Value::as_i64)
        .filter(|c| *c != 0)
    {
        Some(capacity) => Some(capacity),
        None => match orig {
            Some(fs) => fs.storage_capacity,
            None => Some(1200),
        },
    };
    let storage_type: Option<String> =
        match str_opt(body.get("StorageType")).filter(|s| !s.is_empty()) {
            Some(storage_type) => Some(storage_type),
            None => match orig {
                Some(fs) => fs.storage_type.clone(),
                None => Some("SSD".to_string()),
            },
        };
    let mut create_body = json!({
        "FileSystemType": file_system_type,
        "StorageCapacity": storage_capacity,
        "StorageType": storage_type,
        "SubnetIds": body.get("SubnetIds").filter(|v| v.is_array()).cloned().unwrap_or(json!([])),
        "OntapConfiguration": Value::Null,
    });
    for key in [
        "SecurityGroupIds",
        "Tags",
        "KmsKeyId",
        "WindowsConfiguration",
        "LustreConfiguration",
        "OpenZFSConfiguration",
    ] {
        if let Some(value) = body.get(key) {
            create_body[key] = value.clone();
        }
    }
    create_file_system(st, account, region, &create_body)
}

// ---------------------------------------------------------------------------
// Backups
// ---------------------------------------------------------------------------

fn create_backup(st: &mut FsxStore, account: u64, region: &str, body: &Value) -> Response {
    let file_system_id = as_str(body, "FileSystemId");
    if !st.file_systems.contains_key(file_system_id) {
        return resource_not_found(file_system_id);
    }
    let backup_id = hex_id("backup-", 8);
    let record = Backup {
        backup_id: backup_id.clone(),
        file_system_id: file_system_id.to_string(),
        client_request_token: str_opt(body.get("ClientRequestToken"))
            .unwrap_or_else(|| Uuid::new_v4().to_string()),
        volume_id: str_opt(body.get("VolumeId")),
        resource_arn: format!("arn:aws:fsx:{region}:{account}:backup/{backup_id}"),
        lifecycle: "CREATING".to_string(),
        typ: "USER_INITIATED".to_string(),
        source_backup_id: None,
        source_backup_region: None,
        creation_time: now_unix(),
    };
    let arn = record.resource_arn.clone();
    if let Some(tags) = non_empty_tags(body) {
        tag_insert(st, &arn, Some(tags));
    }
    let dict = record.to_dict(tags_by_ref(st, &arn));
    st.backups.insert(record.backup_id.clone(), record);
    ok(json!({ "Backup": dict }))
}

fn delete_backup(st: &mut FsxStore, body: &Value) -> Response {
    let id = as_str(body, "BackupId");
    if !st.backups.contains_key(id) {
        return resource_not_found(id);
    }
    st.backups.remove(id);
    ok(json!({ "BackupId": id, "Lifecycle": "DELETED" }))
}

fn describe_backups(st: &mut FsxStore, body: &Value) -> Response {
    let ids = as_str_list(body.get("BackupIds"));
    let collected: Vec<&Backup> = if ids.is_empty() {
        st.backups.values().collect()
    } else {
        st.backups
            .values()
            .filter(|b| ids.contains(&b.backup_id))
            .collect()
    };
    let filters = filter_map(body.get("Filters"));
    // moto consults seven attribute pairs; its Backup model only carries
    // file_system_id and volume_id, so the other five never match — which
    // is exactly what the `no_attr` lookup produces here.
    let filtered = filter_resources(
        collected,
        &filters,
        &[
            ("file-system-id", backup_attr_file_system_id),
            ("volume-id", backup_attr_volume_id),
            ("backup-type", no_attr),
            ("file-system-type", no_attr),
            ("data-repository-type", no_attr),
            ("file-cache-id", no_attr),
            ("file-cache-type", no_attr),
        ],
    );
    let items: Vec<(String, Value)> = filtered
        .into_iter()
        .map(|b| {
            (
                b.resource_arn.clone(),
                b.to_dict(tags_by_ref(st, &b.resource_arn)),
            )
        })
        .collect();
    let (page, next) = match paginate(items, max_results(body), token(body)) {
        Ok(result) => result,
        Err(resp) => return resp,
    };
    ok(json!({ "Backups": page, "NextToken": next }))
}

fn backup_attr_file_system_id(backup: &Backup) -> Option<String> {
    Some(backup.file_system_id.clone())
}

fn backup_attr_volume_id(backup: &Backup) -> Option<String> {
    backup.volume_id.clone()
}

// ---------------------------------------------------------------------------
// Storage virtual machines
// ---------------------------------------------------------------------------

fn create_storage_virtual_machine(
    st: &mut FsxStore,
    account: u64,
    region: &str,
    body: &Value,
) -> Response {
    let file_system_id = as_str(body, "FileSystemId");
    if !st.file_systems.contains_key(file_system_id) {
        return fs_not_found(file_system_id);
    }
    let svm_id = hex_id("svm-", 17);
    let record = StorageVirtualMachine {
        storage_virtual_machine_id: svm_id.clone(),
        file_system_id: file_system_id.to_string(),
        name: str_opt(body.get("Name")),
        active_directory_configuration: body
            .get("ActiveDirectoryConfiguration")
            .filter(|v| !v.is_null())
            .cloned(),
        root_volume_security_style: str_opt(body.get("RootVolumeSecurityStyle"))
            .unwrap_or_else(|| "UNIX".to_string()),
        resource_arn: format!(
            "arn:aws:fsx:{region}:{account}:storage-virtual-machine/{file_system_id}/{svm_id}"
        ),
        lifecycle: "CREATED".to_string(),
        creation_time: now_unix(),
    };
    let arn = record.resource_arn.clone();
    if let Some(tags) = non_empty_tags(body) {
        tag_insert(st, &arn, Some(tags));
    }
    let dict = record.to_dict(tags_by_ref(st, &arn));
    st.storage_virtual_machines
        .insert(record.storage_virtual_machine_id.clone(), record);
    ok(json!({ "StorageVirtualMachine": dict }))
}

fn delete_storage_virtual_machine(st: &mut FsxStore, body: &Value) -> Response {
    let id = as_str(body, "StorageVirtualMachineId");
    if !st.storage_virtual_machines.contains_key(id) {
        return svm_not_found(id);
    }
    st.storage_virtual_machines.remove(id);
    ok(json!({ "StorageVirtualMachineId": id, "Lifecycle": "DELETING" }))
}

fn describe_storage_virtual_machines(st: &mut FsxStore, body: &Value) -> Response {
    let ids = as_str_list(body.get("StorageVirtualMachineIds"));
    let collected: Vec<&StorageVirtualMachine> = if ids.is_empty() {
        st.storage_virtual_machines.values().collect()
    } else {
        // moto raises on the first unknown id — unlike the file-system
        // describe which silently skips.
        match ids
            .iter()
            .find(|id| !st.storage_virtual_machines.contains_key(*id))
        {
            Some(missing) => return svm_not_found(missing),
            None => st
                .storage_virtual_machines
                .values()
                .filter(|svm| ids.contains(&svm.storage_virtual_machine_id))
                .collect(),
        }
    };
    let filters = filter_map(body.get("Filters"));
    let filtered = filter_resources(
        collected,
        &filters,
        &[("file-system-id", svm_attr_file_system_id)],
    );
    let list: Vec<Value> = filtered
        .into_iter()
        .map(|svm| svm.to_dict(tags_by_ref(st, &svm.resource_arn)))
        .collect();
    ok(json!({ "StorageVirtualMachines": list, "NextToken": Value::Null }))
}

fn svm_attr_file_system_id(svm: &StorageVirtualMachine) -> Option<String> {
    Some(svm.file_system_id.clone())
}

// ---------------------------------------------------------------------------
// Volumes
// ---------------------------------------------------------------------------

fn create_volume(st: &mut FsxStore, account: u64, region: &str, body: &Value) -> Response {
    let ontap = body.get("OntapConfiguration").filter(|v| !v.is_null());
    let open_zfs = body.get("OpenZFSConfiguration").filter(|v| !v.is_null());
    // moto derives file_system_id from the configuration: ONTAP via the SVM
    // (only when that SVM exists), then OpenZFS's ParentVolumeId (no
    // validation, and an OpenZFS config overwrites an ONTAP result).
    let mut file_system_id = ontap
        .and_then(|c| c.get("StorageVirtualMachineId").and_then(Value::as_str))
        .and_then(|svm_id| st.storage_virtual_machines.get(svm_id))
        .map(|svm| svm.file_system_id.clone());
    if open_zfs.is_some() {
        file_system_id = open_zfs
            .and_then(|c| c.get("ParentVolumeId").and_then(Value::as_str))
            .map(String::from);
    }
    let volume_id = hex_id("fsvol-", 17);
    let record = Volume {
        volume_id: volume_id.clone(),
        volume_type: str_opt(body.get("VolumeType")),
        name: str_opt(body.get("Name")),
        file_system_id,
        resource_arn: format!("arn:aws:fsx:{region}:{account}:volume/{volume_id}"),
        lifecycle: "CREATED".to_string(),
        creation_time: now_unix(),
        ontap_configuration: ontap.cloned(),
        open_zfs_configuration: open_zfs.cloned(),
    };
    let arn = record.resource_arn.clone();
    if let Some(tags) = non_empty_tags(body) {
        tag_insert(st, &arn, Some(tags));
    }
    let dict = record.to_dict(tags_by_ref(st, &arn));
    st.volumes.insert(record.volume_id.clone(), record);
    ok(json!({ "Volume": dict }))
}

fn create_volume_from_backup(
    st: &mut FsxStore,
    account: u64,
    region: &str,
    body: &Value,
) -> Response {
    let backup_id = as_str(body, "BackupId");
    if !st.backups.contains_key(backup_id) {
        return backup_not_found(backup_id);
    }
    let create_body = json!({
        "VolumeType": "ONTAP",
        "Name": body.get("Name").cloned().unwrap_or(Value::Null),
        "OntapConfiguration": body.get("OntapConfiguration").cloned().unwrap_or(Value::Null),
        "Tags": body.get("Tags").cloned().unwrap_or(Value::Null),
    });
    create_volume(st, account, region, &create_body)
}

fn delete_volume(st: &mut FsxStore, body: &Value) -> Response {
    let id = as_str(body, "VolumeId");
    let Some(vol) = st.volumes.get(id) else {
        return volume_not_found(id);
    };
    let ontap = vol.volume_type.as_deref() == Some("ONTAP");
    st.volumes.remove(id);
    let mut out = json!({
        "VolumeId": id,
        "Lifecycle": "DELETING",
        "OntapResponse": Value::Null,
    });
    if ontap {
        out["OntapResponse"] = json!({ "FinalBackupId": Value::Null });
    }
    ok(out)
}

fn describe_volumes(st: &mut FsxStore, body: &Value) -> Response {
    let ids = as_str_list(body.get("VolumeIds"));
    let collected: Vec<&Volume> = if ids.is_empty() {
        st.volumes.values().collect()
    } else {
        match ids.iter().find(|id| !st.volumes.contains_key(*id)) {
            Some(missing) => return volume_not_found(missing),
            None => st
                .volumes
                .values()
                .filter(|v| ids.contains(&v.volume_id))
                .collect(),
        }
    };
    let filters = filter_map(body.get("Filters"));
    let filtered = filter_resources(
        collected,
        &filters,
        &[
            ("file-system-id", volume_attr_file_system_id),
            ("volume-type", volume_attr_type),
        ],
    );
    let list: Vec<Value> = filtered
        .into_iter()
        .map(|vol| vol.to_dict(tags_by_ref(st, &vol.resource_arn)))
        .collect();
    ok(json!({ "Volumes": list, "NextToken": Value::Null }))
}

fn volume_attr_file_system_id(volume: &Volume) -> Option<String> {
    volume.file_system_id.clone()
}

fn volume_attr_type(volume: &Volume) -> Option<String> {
    volume.volume_type.clone()
}

// ---------------------------------------------------------------------------
// Snapshots
// ---------------------------------------------------------------------------

fn create_snapshot(st: &mut FsxStore, account: u64, region: &str, body: &Value) -> Response {
    let volume_id = as_str(body, "VolumeId");
    if !st.volumes.contains_key(volume_id) {
        return volume_not_found(volume_id);
    }
    let snapshot_id = hex_id("fsvolsnap-", 17);
    let record = Snapshot {
        snapshot_id: snapshot_id.clone(),
        name: str_opt(body.get("Name")),
        volume_id: volume_id.to_string(),
        resource_arn: format!("arn:aws:fsx:{region}:{account}:snapshot/{snapshot_id}"),
        lifecycle: "AVAILABLE".to_string(),
        creation_time: now_unix(),
    };
    let arn = record.resource_arn.clone();
    if let Some(tags) = non_empty_tags(body) {
        tag_insert(st, &arn, Some(tags));
    }
    let dict = record.to_dict(tags_by_ref(st, &arn));
    st.snapshots.insert(record.snapshot_id.clone(), record);
    ok(json!({ "Snapshot": dict }))
}

fn delete_snapshot(st: &mut FsxStore, body: &Value) -> Response {
    let id = as_str(body, "SnapshotId");
    if !st.snapshots.contains_key(id) {
        return snapshot_not_found(id);
    }
    st.snapshots.remove(id);
    ok(json!({ "SnapshotId": id, "Lifecycle": "DELETING" }))
}

fn update_snapshot(st: &mut FsxStore, body: &Value) -> Response {
    let id = as_str(body, "SnapshotId").to_string();
    let Some(snap) = st.snapshots.get(&id) else {
        return snapshot_not_found(&id);
    };
    let arn = snap.resource_arn.clone();
    let tags = tags_by_ref(st, &arn);
    let Some(snap) = st.snapshots.get_mut(&id) else {
        unreachable!("looked up one line above");
    };
    snap.name = str_opt(body.get("Name"));
    let dict = snap.to_dict(tags);
    ok(json!({ "Snapshot": dict }))
}

fn describe_snapshots(st: &mut FsxStore, body: &Value) -> Response {
    let ids = as_str_list(body.get("SnapshotIds"));
    let collected: Vec<&Snapshot> = if ids.is_empty() {
        st.snapshots.values().collect()
    } else {
        match ids.iter().find(|id| !st.snapshots.contains_key(*id)) {
            Some(missing) => return snapshot_not_found(missing),
            None => st
                .snapshots
                .values()
                .filter(|s| ids.contains(&s.snapshot_id))
                .collect(),
        }
    };
    let filters = filter_map(body.get("Filters"));
    let filtered = filter_resources(
        collected,
        &filters,
        &[("volume-id", snapshot_attr_volume_id)],
    );
    let list: Vec<Value> = filtered
        .into_iter()
        .map(|snap| snap.to_dict(tags_by_ref(st, &snap.resource_arn)))
        .collect();
    ok(json!({ "Snapshots": list, "NextToken": Value::Null }))
}

fn snapshot_attr_volume_id(snapshot: &Snapshot) -> Option<String> {
    Some(snapshot.volume_id.clone())
}

// ---------------------------------------------------------------------------
// File system aliases
// ---------------------------------------------------------------------------

fn associate_file_system_aliases(st: &mut FsxStore, body: &Value) -> Response {
    let id = as_str(body, "FileSystemId");
    let aliases = as_str_list(body.get("Aliases"));
    let Some(fs) = st.file_systems.get_mut(id) else {
        return fs_not_found(id);
    };
    let mut entries = Vec::new();
    for alias in aliases {
        entries.push(json!({ "Name": alias, "Lifecycle": "AVAILABLE" }));
        if !fs
            .aliases
            .iter()
            .any(|a| a.get("Name").and_then(Value::as_str) == Some(alias.as_str()))
        {
            fs.aliases
                .push(json!({ "Name": alias, "Lifecycle": "AVAILABLE" }));
        }
    }
    ok(json!({ "Aliases": entries }))
}

fn disassociate_file_system_aliases(st: &mut FsxStore, body: &Value) -> Response {
    let id = as_str(body, "FileSystemId");
    let aliases = as_str_list(body.get("Aliases"));
    let Some(fs) = st.file_systems.get_mut(id) else {
        return fs_not_found(id);
    };
    let mut entries = Vec::new();
    for alias in aliases {
        fs.aliases
            .retain(|a| a.get("Name").and_then(Value::as_str) != Some(alias.as_str()));
        entries.push(json!({ "Name": alias, "Lifecycle": "DELETING" }));
    }
    ok(json!({ "Aliases": entries }))
}

fn describe_file_system_aliases(st: &mut FsxStore, body: &Value) -> Response {
    let id = as_str(body, "FileSystemId");
    let Some(fs) = st.file_systems.get(id) else {
        return fs_not_found(id);
    };
    ok(json!({ "Aliases": fs.aliases, "NextToken": Value::Null }))
}

// ---------------------------------------------------------------------------
// Data repository associations
// ---------------------------------------------------------------------------

fn create_data_repository_association(
    st: &mut FsxStore,
    account: u64,
    region: &str,
    body: &Value,
) -> Response {
    let file_cache_id = str_opt(body.get("FileCacheId")).filter(|s| !s.is_empty());
    if let Some(cache_id) = &file_cache_id {
        if !st.file_caches.contains_key(cache_id) {
            return file_cache_not_found(cache_id);
        }
    }
    let file_system_id = as_str(body, "FileSystemId");
    if file_system_id.is_empty() && file_cache_id.is_none() {
        return fs_not_found(file_system_id);
    }
    if !file_system_id.is_empty() && !st.file_systems.contains_key(file_system_id) {
        return fs_not_found(file_system_id);
    }
    let association_id = hex_id("dra-", 9);
    let resource_arn = if file_system_id.is_empty() {
        format!(
            "arn:aws:fsx:{region}:{account}:association/{}/{association_id}",
            file_cache_id.as_deref().unwrap_or_default()
        )
    } else {
        format!("arn:aws:fsx:{region}:{account}:association/{file_system_id}/{association_id}")
    };
    let record = DataRepositoryAssociation {
        association_id: association_id.clone(),
        file_system_id: file_system_id.to_string(),
        file_cache_id,
        file_system_path: str_opt(body.get("FileSystemPath")),
        data_repository_path: as_str(body, "DataRepositoryPath").to_string(),
        batch_import_meta_data_on_create: body
            .get("BatchImportMetaDataOnCreate")
            .and_then(Value::as_bool),
        imported_file_chunk_size: body.get("ImportedFileChunkSize").and_then(Value::as_i64),
        s3: body.get("S3").filter(|v| !v.is_null()).cloned(),
        resource_arn,
        lifecycle: "AVAILABLE".to_string(),
        creation_time: now_unix(),
    };
    let arn = record.resource_arn.clone();
    if let Some(tags) = non_empty_tags(body) {
        tag_insert(st, &arn, Some(tags));
    }
    let dict = record.to_dict(tags_by_ref(st, &arn));
    st.data_repository_associations
        .insert(record.association_id.clone(), record);
    ok(json!({ "Association": dict }))
}

fn update_data_repository_association(st: &mut FsxStore, body: &Value) -> Response {
    let id = as_str(body, "AssociationId").to_string();
    let Some(dra) = st.data_repository_associations.get(&id) else {
        return resource_not_found(&id);
    };
    let arn = dra.resource_arn.clone();
    let tags = tags_by_ref(st, &arn);
    let Some(dra) = st.data_repository_associations.get_mut(&id) else {
        unreachable!("looked up one line above");
    };
    if let Some(chunk) = body.get("ImportedFileChunkSize").and_then(Value::as_i64) {
        dra.imported_file_chunk_size = Some(chunk);
    }
    if let Some(s3) = body.get("S3").filter(|v| !v.is_null()) {
        dra.s3 = Some(s3.clone());
    }
    let dict = dra.to_dict(tags);
    ok(json!({ "Association": dict }))
}

fn delete_data_repository_association(st: &mut FsxStore, body: &Value) -> Response {
    let id = as_str(body, "AssociationId");
    if !st.data_repository_associations.contains_key(id) {
        return resource_not_found(id);
    }
    st.data_repository_associations.remove(id);
    let delete_data = body
        .get("DeleteDataInFileSystem")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    ok(json!({
        "AssociationId": id,
        "Lifecycle": "DELETING",
        "DeleteDataInFileSystem": delete_data,
    }))
}

fn describe_data_repository_associations(st: &mut FsxStore, body: &Value) -> Response {
    let ids = as_str_list(body.get("AssociationIds"));
    let collected: Vec<&DataRepositoryAssociation> = if ids.is_empty() {
        st.data_repository_associations.values().collect()
    } else {
        // moto silently skips unknown ids here.
        st.data_repository_associations
            .values()
            .filter(|a| ids.contains(&a.association_id))
            .collect()
    };
    let filters = filter_map(body.get("Filters"));
    // The second pair reads a data_repository_type attr moto's DRA model
    // never defines; `no_attr` keeps it "never matches" without the crash.
    let filtered = filter_resources(
        collected,
        &filters,
        &[
            ("file-system-id", dra_attr_file_system_id),
            ("data-repository-type", no_attr),
        ],
    );
    let list: Vec<Value> = filtered
        .into_iter()
        .map(|assoc| assoc.to_dict(tags_by_ref(st, &assoc.resource_arn)))
        .collect();
    ok(json!({ "Associations": list, "NextToken": Value::Null }))
}

fn dra_attr_file_system_id(assoc: &DataRepositoryAssociation) -> Option<String> {
    Some(assoc.file_system_id.clone())
}

/// Attr lookup for filters moto would crash on (the model attribute does
/// not exist): never matches.
fn no_attr<T>(_item: &T) -> Option<String> {
    None
}

// ---------------------------------------------------------------------------
// Data repository tasks, file caches, S3 access point attachments
// (botocore-modeled; ids follow moto's conventions where they exist)
// ---------------------------------------------------------------------------

struct DataRepositoryTask {
    task_id: String,
    lifecycle: String,
    typ: String,
    creation_time: f64,
    resource_arn: String,
    file_system_id: Option<String>,
    file_cache_id: Option<String>,
    paths: Vec<String>,
    report: Option<Value>,
    capacity_to_release: Option<i64>,
    release_configuration: Option<Value>,
}

impl DataRepositoryTask {
    fn to_dict(&self, tags: Vec<Value>) -> Value {
        let mut out = json!({
            "TaskId": self.task_id,
            "Lifecycle": self.lifecycle,
            "Type": self.typ,
            "CreationTime": self.creation_time,
            "ResourceARN": self.resource_arn,
            "Tags": tags,
            "FileSystemId": self.file_system_id,
            "FileCacheId": self.file_cache_id,
            "Paths": self.paths,
            "Status": {
                "TotalCount": 0,
                "SucceededCount": 0,
                "FailedCount": 0,
                "LastUpdatedTime": self.creation_time,
            },
            "Report": self.report,
            "CapacityToRelease": self.capacity_to_release,
            "ReleaseConfiguration": self.release_configuration,
        });
        strip_nulls(out.as_object_mut().expect("object"));
        out
    }
}

struct FileCache {
    file_cache_id: String,
    file_cache_type: String,
    file_cache_type_version: String,
    owner_id: u64,
    storage_capacity: i64,
    subnet_ids: Vec<String>,
    network_interface_ids: Vec<String>,
    dns_name: String,
    kms_key_id: Option<String>,
    resource_arn: String,
    creation_time: f64,
    copy_tags: Option<bool>,
    lustre_configuration: Option<Value>,
    association_ids: Vec<String>,
}

impl FileCache {
    fn to_dict(&self, tags: Vec<Value>) -> Value {
        let mut out = json!({
            "OwnerId": self.owner_id.to_string(),
            "CreationTime": self.creation_time,
            "FileCacheId": self.file_cache_id,
            "FileCacheType": self.file_cache_type,
            "FileCacheTypeVersion": self.file_cache_type_version,
            "Lifecycle": "AVAILABLE",
            "StorageCapacity": self.storage_capacity,
            "SubnetIds": self.subnet_ids,
            "NetworkInterfaceIds": self.network_interface_ids,
            "DNSName": self.dns_name,
            "KmsKeyId": self.kms_key_id,
            "ResourceARN": self.resource_arn,
            "CopyTagsToDataRepositoryAssociations": self.copy_tags,
            "LustreConfiguration": self.lustre_configuration,
            "DataRepositoryAssociationIds": self.association_ids,
            "Tags": tags,
        });
        strip_nulls(out.as_object_mut().expect("object"));
        out
    }
}

struct S3AccessPointAttachment {
    name: String,
    typ: String,
    lifecycle: String,
    creation_time: f64,
    /// OpenZFS/Ontap config (VolumeId + FileSystemIdentity) as given.
    configuration: Option<Value>,
    config_key: &'static str,
    resource_arn: String,
    alias: String,
}

impl S3AccessPointAttachment {
    fn to_dict(&self) -> Value {
        let mut out = json!({
            "Lifecycle": self.lifecycle,
            "CreationTime": self.creation_time,
            "Name": self.name,
            "Type": self.typ,
            "S3AccessPoint": {
                "ResourceARN": self.resource_arn,
                "Alias": self.alias,
            },
        });
        if let Some(config) = &self.configuration {
            out[self.config_key] = config.clone();
        }
        out
    }
}

// ---------------------------------------------------------------------------
// Wave-12 handlers
// ---------------------------------------------------------------------------

fn create_data_repository_task(
    st: &mut FsxStore,
    account: u64,
    region: &str,
    body: &Value,
) -> Response {
    let file_system_id = as_str(body, "FileSystemId");
    if !st.file_systems.contains_key(file_system_id) {
        return fs_not_found(file_system_id);
    }
    let task_id = hex_id("task-", 17);
    let typ = py_str(body.get("Type"));
    if !matches!(
        typ.as_str(),
        "EXPORT_TO_REPOSITORY"
            | "IMPORT_METADATA_FROM_REPOSITORY"
            | "RELEASE_DATA_FROM_FILESYSTEM"
            | "AUTO_RELEASE_DATA"
    ) {
        // botocore's BadRequest for enum violations.
        return json_error(
            400,
            "BadRequest",
            &format!("Invalid value '{typ}' for Type. Valid values are: EXPORT_TO_REPOSITORY, IMPORT_METADATA_FROM_REPOSITORY, RELEASE_DATA_FROM_FILESYSTEM, AUTO_RELEASE_DATA"),
        );
    }
    let record = DataRepositoryTask {
        task_id: task_id.clone(),
        lifecycle: "PENDING".to_string(),
        typ,
        creation_time: now_unix(),
        resource_arn: format!("arn:aws:fsx:{region}:{account}:task/{task_id}"),
        file_system_id: Some(file_system_id.to_string()),
        file_cache_id: None,
        paths: as_str_list(body.get("Paths")),
        report: body.get("Report").filter(|v| v.is_object()).cloned(),
        capacity_to_release: body.get("CapacityToRelease").and_then(Value::as_i64),
        release_configuration: body
            .get("ReleaseConfiguration")
            .filter(|v| v.is_object())
            .cloned(),
    };
    let arn = record.resource_arn.clone();
    if let Some(tags) = non_empty_tags(body) {
        tag_insert(st, &arn, Some(tags));
    }
    let dict = record.to_dict(tags_by_ref(st, &arn));
    st.data_repository_tasks.insert(task_id, record);
    ok(json!({ "DataRepositoryTask": dict }))
}

fn cancel_data_repository_task(st: &mut FsxStore, body: &Value) -> Response {
    let id = as_str(body, "TaskId");
    let Some(task) = st.data_repository_tasks.get_mut(id) else {
        return json_error(
            400,
            "DataRepositoryTaskNotFound",
            &format!("Data repository task '{id}' does not exist"),
        );
    };
    match task.lifecycle.as_str() {
        // Terminal tasks answer their declined-modification fault.
        "SUCCEEDED" | "FAILED" | "CANCELED" => {
            let lifecycle = task.lifecycle.clone();
            return json_error(
                400,
                "DataRepositoryTaskEnded",
                &format!(
                    "Data repository task has ended ({lifecycle}) and cannot be modified"
                ),
            );
        }
        // A queued task cancels outright; an executing one transitions.
        "PENDING" => task.lifecycle = "CANCELED".to_string(),
        _ => task.lifecycle = "CANCELING".to_string(),
    }
    ok(json!({ "TaskId": id, "Lifecycle": task.lifecycle.clone() }))
}

fn describe_data_repository_tasks(st: &mut FsxStore, body: &Value) -> Response {
    let ids = as_str_list(body.get("TaskIds"));
    let collected: Vec<&DataRepositoryTask> = if ids.is_empty() {
        st.data_repository_tasks.values().collect()
    } else {
        st.data_repository_tasks
            .values()
            .filter(|t| ids.contains(&t.task_id))
            .collect()
    };
    let filters = filter_map(body.get("Filters"));
    let filtered = filter_resources(
        collected,
        &filters,
        &[
            ("file-system-id", |t: &DataRepositoryTask| {
                t.file_system_id.clone()
            }),
            ("task-id", |t: &DataRepositoryTask| Some(t.task_id.clone())),
            ("task-lifecycle", |t: &DataRepositoryTask| {
                Some(t.lifecycle.clone())
            }),
            ("file-cache-id", |t: &DataRepositoryTask| {
                t.file_cache_id.clone()
            }),
        ],
    );
    let items: Vec<(String, Value)> = filtered
        .into_iter()
        .map(|t| (t.resource_arn.clone(), t.to_dict(tags_by_ref(st, &t.resource_arn))))
        .collect();
    let (page, next) = match paginate(items, max_results(body), token(body)) {
        Ok(result) => result,
        Err(resp) => return resp,
    };
    ok(json!({ "DataRepositoryTasks": page, "NextToken": next }))
}

fn create_file_cache(st: &mut FsxStore, account: u64, region: &str, body: &Value) -> Response {
    let cache_type = py_str(body.get("FileCacheType"));
    if cache_type != "LUSTRE" {
        return json_error(
            400,
            "BadRequest",
            &format!("Invalid value '{cache_type}' for FileCacheType. Valid values are: LUSTRE"),
        );
    }
    let cache_id = hex_id("fc-", 17);
    // The input's LustreConfiguration flows into the record; the record is
    // stored first so the cache-based associations can validate against it.
    let mut lustre = body
        .get("LustreConfiguration")
        .filter(|v| v.is_object())
        .cloned()
        .unwrap_or_else(|| json!({}));
    lustre["MountName"] = json!(hex_id("cache-", 8));
    let copy_tags = body
        .get("CopyTagsToDataRepositoryAssociations")
        .and_then(Value::as_bool);
    let record = FileCache {
        file_cache_id: cache_id.clone(),
        file_cache_type: cache_type,
        file_cache_type_version: as_str(body, "FileCacheTypeVersion").to_string(),
        owner_id: account,
        storage_capacity: body.get("StorageCapacity").and_then(Value::as_i64).unwrap_or(0),
        subnet_ids: as_str_list(body.get("SubnetIds")),
        network_interface_ids: vec![hex_id("eni-", 17)],
        dns_name: format!("{cache_id}.fsx.{region}.amazonaws.com"),
        kms_key_id: str_opt(body.get("KmsKeyId")),
        resource_arn: format!("arn:aws:fsx:{region}:{account}:file-cache/{cache_id}"),
        creation_time: now_unix(),
        copy_tags,
        lustre_configuration: Some(lustre),
        association_ids: Vec::new(),
    };
    let arn = record.resource_arn.clone();
    if let Some(tags) = non_empty_tags(body) {
        tag_insert(st, &arn, Some(tags));
    }
    st.file_caches.insert(cache_id.clone(), record);
    // Associations become real DataRepositoryAssociations on the cache.
    if let Some(assocs) = body.get("DataRepositoryAssociations").and_then(Value::as_array) {
        for assoc in assocs {
            let mut assoc_body = assoc.clone();
            assoc_body["FileCacheId"] = json!(&cache_id);
            let resp = create_data_repository_association(st, account, region, &assoc_body);
            if resp.status != 200 {
                return resp;
            }
            let parsed: Value = serde_json::from_slice(&resp.body).unwrap_or(json!({}));
            if let Some(id) = parsed["Association"]["AssociationId"].as_str() {
                if let Some(cache) = st.file_caches.get_mut(&cache_id) {
                    cache.association_ids.push(id.to_string());
                }
            }
        }
    }
    let dict = st.file_caches[&cache_id].to_dict(tags_by_ref(st, &arn));
    ok(json!({ "FileCache": dict }))
}

fn delete_file_cache(st: &mut FsxStore, body: &Value) -> Response {
    let id = as_str(body, "FileCacheId");
    match st.file_caches.remove(id) {
        Some(_) => ok(json!({ "FileCacheId": id, "Lifecycle": "DELETING" })),
        None => json_error(400, "FileCacheNotFound", &format!("File cache '{id}' does not exist")),
    }
}

fn update_file_cache(st: &mut FsxStore, body: &Value) -> Response {
    let id = as_str(body, "FileCacheId");
    let Some(cache) = st.file_caches.get(id) else {
        return json_error(400, "FileCacheNotFound", &format!("File cache '{id}' does not exist"));
    };
    let arn = cache.resource_arn.clone();
    let tags = tags_by_ref(st, &arn);
    let Some(cache) = st.file_caches.get_mut(id) else {
        unreachable!("looked up one line above");
    };
    merge_config(body.get("LustreConfiguration"), &mut cache.lustre_configuration);
    let dict = cache.to_dict(tags);
    ok(json!({ "FileCache": dict }))
}

fn describe_file_caches(st: &mut FsxStore, body: &Value) -> Response {
    let ids = as_str_list(body.get("FileCacheIds"));
    let items: Vec<Value> = st
        .file_caches
        .values()
        .filter(|c| ids.is_empty() || ids.contains(&c.file_cache_id))
        .map(|c| c.to_dict(tags_by_ref(st, &c.resource_arn)))
        .collect();
    ok(json!({ "FileCaches": items, "NextToken": Value::Null }))
}

fn create_and_attach_s3_access_point(
    st: &mut FsxStore,
    account: u64,
    region: &str,
    body: &Value,
) -> Response {
    let name = as_str(body, "Name");
    let typ = py_str(body.get("Type"));
    let (config_key, config) = match typ.as_str() {
        "OPENZFS" => (
            "OpenZFSConfiguration",
            body.get("OpenZFSConfiguration").filter(|v| v.is_object()),
        ),
        "ONTAP" => (
            "OntapConfiguration",
            body.get("OntapConfiguration").filter(|v| v.is_object()),
        ),
        other => {
            return json_error(
                400,
                "BadRequest",
                &format!("Invalid value '{other}' for Type. Valid values are: ONTAP, OPENZFS"),
            )
        }
    };
    let Some(config) = config else {
        return json_error(
            400,
            "BadRequest",
            &format!("{config_key} must be provided for a {typ} S3 access point"),
        );
    };
    let volume_id = str_opt(config.get("VolumeId"));
    if let Some(volume_id) = &volume_id {
        if !st.volumes.contains_key(volume_id) {
            return volume_not_found(volume_id);
        }
    }
    if st.s3_access_points.contains_key(name) {
        return json_error(
            400,
            "AccessPointAlreadyOwnedByYou",
            &format!("An S3 access point named {name} already exists"),
        );
    }
    let attachment = S3AccessPointAttachment {
        name: name.to_string(),
        typ: typ.clone(),
        lifecycle: "AVAILABLE".to_string(),
        creation_time: now_unix(),
        configuration: Some(config.clone()),
        config_key,
        // The attached S3 access point is an s3-namespace resource.
        resource_arn: format!("arn:aws:s3:{region}:{account}:accesspoint/{name}"),
        alias: format!("{name}-{account}-s3alias"),
    };
    st.s3_access_points.insert(name.to_string(), attachment);
    let stored = &st.s3_access_points[name];
    ok(json!({ "S3AccessPointAttachment": stored.to_dict() }))
}

fn detach_and_delete_s3_access_point(st: &mut FsxStore, body: &Value) -> Response {
    let name = as_str(body, "Name");
    match st.s3_access_points.remove(name) {
        Some(attachment) => ok(json!({ "Lifecycle": "DELETING", "Name": attachment.name })),
        None => json_error(
            400,
            "S3AccessPointAttachmentNotFound",
            &format!("No S3 access point attachment {name} found"),
        ),
    }
}

fn describe_s3_access_point_attachments(st: &mut FsxStore, body: &Value) -> Response {
    let names = as_str_list(body.get("S3AccessPointAttachmentIds"));
    let collected: Vec<&S3AccessPointAttachment> = if names.is_empty() {
        st.s3_access_points.values().collect()
    } else {
        st.s3_access_points
            .values()
            .filter(|a| names.contains(&a.name))
            .collect()
    };
    let items: Vec<(String, Value)> = collected
        .into_iter()
        .map(|a| (a.resource_arn.clone(), a.to_dict()))
        .collect();
    let (page, next) = match paginate(items, max_results(body), token(body)) {
        Ok(result) => result,
        Err(resp) => return resp,
    };
    ok(json!({ "S3AccessPointAttachments": page, "NextToken": next }))
}

fn release_file_system_nfs_v3_locks(st: &mut FsxStore, body: &Value) -> Response {
    let id = as_str(body, "FileSystemId");
    let Some(fs) = st.file_systems.get(id) else {
        return fs_not_found(id);
    };
    let arn = fs.resource_arn.clone();
    let dict = fs.to_dict(tags_by_ref(st, &arn));
    ok(json!({ "FileSystem": dict }))
}

fn start_misconfigured_state_recovery(st: &mut FsxStore, body: &Value) -> Response {
    release_file_system_nfs_v3_locks(st, body)
}

fn update_shared_vpc_configuration(st: &mut FsxStore, body: &Value) -> Response {
    let enabled = match str_opt(body.get("EnableFsxRouteTableUpdatesFromParticipantAccounts")) {
        Some(value) if value == "true" || value == "false" => value == "true",
        Some(value) => {
            return json_error(
                400,
                "BadRequest",
                &format!("Invalid value '{value}' for EnableFsxRouteTableUpdatesFromParticipantAccounts. Valid values are true or false"),
            )
        }
        // Absent flag keeps the current setting (moto's `or false`).
        None => false,
    };
    st.shared_vpc_route_table_updates = enabled;
    ok(json!({
        "EnableFsxRouteTableUpdatesFromParticipantAccounts": if enabled { "true" } else { "false" }
    }))
}

fn update_storage_virtual_machine(st: &mut FsxStore, body: &Value) -> Response {
    let id = as_str(body, "StorageVirtualMachineId");
    let Some(svm) = st.storage_virtual_machines.get(id) else {
        return svm_not_found(id);
    };
    let arn = svm.resource_arn.clone();
    let tags = tags_by_ref(st, &arn);
    let Some(svm) = st.storage_virtual_machines.get_mut(id) else {
        unreachable!("looked up one line above");
    };
    if let Some(config) = body.get("ActiveDirectoryConfiguration").filter(|v| v.is_object()) {
        // The op renders the stored record; the admin password and
        // SelfManaged updates ride inside ActiveDirectoryConfiguration.
        let merged = match svm.active_directory_configuration.take() {
            Some(Value::Object(mut old)) => {
                for (k, v) in config.as_object().expect("object") {
                    old.insert(k.clone(), v.clone());
                }
                Value::Object(old)
            }
            _ => config.clone(),
        };
        svm.active_directory_configuration = Some(merged);
    }
    let dict = svm.to_dict(tags);
    ok(json!({ "StorageVirtualMachine": dict }))
}

/// Copy a backup across (or within) scopes. The source is read without
/// holding the target scope's write lock (avoids nested map guards).
fn copy_backup(req: &Request) -> Response {
    let body = parse_json_body(&req.body);
    let region = req.region.clone();
    let source_region = str_opt(body.get("SourceRegion")).unwrap_or_else(|| region.clone());
    let source_backup_id = as_str(&body, "SourceBackupId").to_string();
    let account = req.account;
    // Drop the read guard before taking the write lock below.
    let (source, source_tags) = {
        let slot = stores();
        let map = slot.read();
        let Some(st) = map.get(&(req.account, source_region.clone())) else {
            return backup_not_found(&source_backup_id);
        };
        let Some(backup) = st.backups.get(&source_backup_id) else {
            return backup_not_found(&source_backup_id);
        };
        (backup.clone(), tags_by_ref(st, &backup.resource_arn))
    };
    if source.lifecycle == "COPYING" {
        // SourceBackupUnavailable for eligibility failures.
        return json_error(
            400,
            "SourceBackupUnavailable",
            &format!("The source backup {source_backup_id} is not available"),
        );
    }
    let copy_tags_flag = body.get("CopyTags").and_then(Value::as_bool).unwrap_or(false);
    let slot = stores();
    let mut map = slot.write();
    let st = map.entry((account, region.clone())).or_default();
    let backup_id = hex_id("backup-", 8);
    let record = Backup {
        backup_id: backup_id.clone(),
        file_system_id: source.file_system_id.clone(),
        client_request_token: str_opt(body.get("ClientRequestToken"))
            .unwrap_or_else(|| Uuid::new_v4().to_string()),
        volume_id: source.volume_id.clone(),
        resource_arn: format!("arn:aws:fsx:{region}:{account}:backup/{backup_id}"),
        lifecycle: "COPYING".to_string(),
        typ: source.typ.clone(),
        source_backup_id: Some(source_backup_id.clone()),
        source_backup_region: Some(source_region.clone()),
        creation_time: now_unix(),
    };
    let arn = record.resource_arn.clone();
    if copy_tags_flag {
        for tag in &source_tags {
            let key = tag["Key"].as_str().unwrap_or_default().to_string();
            let value = tag["Value"].as_str().map(String::from);
            if !key.is_empty() {
                st.tags.entry(arn.clone()).or_default().insert(key, value);
            }
        }
    }
    if let Some(tags) = non_empty_tags(&body) {
        tag_insert(st, &arn, Some(tags));
    }
    // The copy carries the source file system's snapshot view; when the
    // original is gone a bare OwnerId/FileSystemId shell renders instead
    // of crashing the lookup.
    let fs_dict = st
        .file_systems
        .get(&source.file_system_id)
        .map(|fs| fs.to_dict(tags_by_ref(st, &fs.resource_arn)))
        .unwrap_or_else(|| json!({ "FileSystemId": source.file_system_id, "OwnerId": account.to_string() }));
    let mut dict = record.to_dict(tags_by_ref(st, &arn));
    dict["FileSystem"] = fs_dict;
    st.backups.insert(backup_id.clone(), record);
    ok(json!({ "Backup": dict }))
}

// ---------------------------------------------------------------------------
// Tags (moto's TaggingService)
// ---------------------------------------------------------------------------

fn tags_by_ref(st: &FsxStore, arn: &str) -> Vec<Value> {
    st.tags
        .get(arn)
        .map(|row| {
            row.iter()
                .map(|(key, value)| json!({ "Key": key, "Value": tag_value(value.as_deref()) }))
                .collect()
        })
        .unwrap_or_default()
}

fn non_empty_tags(body: &Value) -> Option<&Vec<Value>> {
    body.get("Tags")
        .and_then(Value::as_array)
        .filter(|t| !t.is_empty())
}

fn tag_insert(st: &mut FsxStore, arn: &str, tags: Option<&Vec<Value>>) {
    let Some(tags) = tags.filter(|t| !t.is_empty()) else {
        return;
    };
    let row = st.tags.entry(arn.to_string()).or_default();
    for tag in tags {
        let Some(key) = tag.get("Key").and_then(Value::as_str) else {
            continue;
        };
        // A member without `Value` stores null, like moto's tagger.
        row.insert(
            key.to_string(),
            tag.get("Value").and_then(Value::as_str).map(String::from),
        );
    }
}

fn untag_resource(st: &mut FsxStore, arn: &str, keys: Vec<String>) {
    let Some(row) = st.tags.get_mut(arn) else {
        return;
    };
    for key in keys {
        row.remove(&key);
    }
}

fn tag_value(value: Option<&str>) -> Value {
    match value {
        Some(v) => json!(v),
        None => Value::Null,
    }
}

// ---------------------------------------------------------------------------
// moto's filter_resources / paginate
// ---------------------------------------------------------------------------

/// Attribute lookup fn passed with each filter pair.
type AttrFn<T> = fn(&T) -> Option<String>;

/// moto's `filter_resources`: only `{Name, Values}` pairs named here are
/// considered. A filter whose values list is empty (or null) drops every
/// resource, matching its `values = filters.get(name) or None` truthiness.
fn filter_resources<'a, T>(
    items: Vec<&'a T>,
    filters: &Map<String, Value>,
    pairs: &[(&str, AttrFn<T>)],
) -> Vec<&'a T> {
    items
        .into_iter()
        .filter(|item| {
            for (name, attr) in pairs {
                let Some(values) = filters.get(*name) else {
                    continue;
                };
                let Some(list) = values.as_array().filter(|l| !l.is_empty()) else {
                    return false;
                };
                if !attr(item).is_some_and(|v| list.iter().any(|x| x.as_str() == Some(v.as_str())))
                {
                    return false;
                }
            }
            true
        })
        .collect()
}

/// Fold the `{Name, Values}` filter list into the `{name: values}` map
/// shape `filter_resources` consumes.
fn filter_map(filters: Option<&Value>) -> Map<String, Value> {
    let mut map = Map::new();
    if let Some(list) = filters.and_then(Value::as_array) {
        for entry in list {
            if let Some(name) = entry.get("Name").and_then(Value::as_str) {
                let values = entry.get("Values").cloned().unwrap_or(Value::Null);
                map.insert(name.to_string(), values);
            }
        }
    }
    map
}

/// moto's `paginate` semantics for `unique_attribute: "resource_arn"`:
/// slice `[start, start + max_results)` off the ordered list; emit a next
/// token describing the first item AFTER the page when more remain.
fn paginate(
    items: Vec<(String, Value)>,
    max_results: Option<i64>,
    starting_token: Option<&str>,
) -> Result<(Vec<Value>, Value), Response> {
    let limit = match max_results {
        Some(n) if n > 0 => n as usize,
        // moto: falsy limit falls back to limit_default 2147483647.
        _ => 2_147_483_647,
    };
    let start = match starting_token {
        None => 0usize,
        Some(value) => {
            let attribute = decode_token(value)?;
            items
                .iter()
                .position(|(arn, _)| *arn == attribute)
                .ok_or_else(|| invalid_token("Resource not found!"))?
        }
    };
    let end = (start + limit).min(items.len());
    let page: Vec<Value> = items[start..end].iter().map(|(_, v)| v.clone()).collect();
    let next = if !page.is_empty() && end < items.len() {
        Value::String(encode_token(&items[end].0))
    } else {
        Value::Null
    };
    Ok((page, next))
}

fn max_results(body: &Value) -> Option<i64> {
    body.get("MaxResults").and_then(Value::as_i64)
}

fn token(body: &Value) -> Option<&str> {
    body.get("NextToken").and_then(Value::as_str)
}

fn encode_token(arn: &str) -> String {
    let doc = json!({ "uniqueAttributes": arn });
    base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(&doc).unwrap_or_default())
}

fn decode_token(token: &str) -> Result<String, Response> {
    base64::engine::general_purpose::STANDARD
        .decode(token.as_bytes())
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
        .and_then(|value| {
            value
                .get("uniqueAttributes")
                .and_then(Value::as_str)
                .map(String::from)
        })
        .ok_or_else(|| invalid_token("Invalid token"))
}

fn invalid_token(message: &str) -> Response {
    json_error(400, "InvalidToken", &format!("Invalid Token: {message}"))
}

// ---------------------------------------------------------------------------
// Small shared helpers
// ---------------------------------------------------------------------------

/// moto's fsx ID generation: `prefix-{first N hex of uuid4}`.
fn hex_id(prefix: &str, hex_chars: usize) -> String {
    let hex = Uuid::new_v4().simple().to_string();
    format!("{prefix}{}", &hex[..hex_chars])
}

/// A JSON scalar the way python would stringify it (`None` when
/// absent/null) — used for the FileSystemType validation message.
fn py_str(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Null) | None => "None".to_string(),
        Some(other) => other.to_string(),
    }
}

fn str_opt(value: Option<&Value>) -> Option<String> {
    value.and_then(Value::as_str).map(String::from)
}

/// Strip members python's `if v` would drop from FileSystem's dict.
fn strip_falsy(obj: &mut Map<String, Value>) {
    let falsy: Vec<String> = obj
        .iter()
        .filter(|(_, v)| !py_truthy(v))
        .map(|(k, _)| k.clone())
        .collect();
    for key in falsy {
        obj.remove(&key);
    }
}

/// Strip members python's `if v is not None` would drop.
fn strip_nulls(obj: &mut Map<String, Value>) {
    let nulls: Vec<String> = obj
        .iter()
        .filter(|(_, v)| v.is_null())
        .map(|(k, _)| k.clone())
        .collect();
    for key in nulls {
        obj.remove(&key);
    }
}

fn py_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(true),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

/// moto's crash-shaped 500: ValueError/KeyError into `InternalError`.
fn internal_error_500(message: &str) -> Response {
    json_error(500, "InternalError", message)
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

fn fs_not_found(id: &str) -> Response {
    json_error(
        400,
        "FileSystemNotFound",
        &format!("File system '{id}' does not exist."),
    )
}

fn backup_not_found(id: &str) -> Response {
    json_error(
        400,
        "BackupNotFound",
        &format!("Backup '{id}' does not exist."),
    )
}

fn volume_not_found(id: &str) -> Response {
    json_error(
        400,
        "VolumeNotFound",
        &format!("Volume '{id}' does not exist."),
    )
}

fn snapshot_not_found(id: &str) -> Response {
    json_error(
        400,
        "SnapshotNotFound",
        &format!("Snapshot '{id}' does not exist."),
    )
}

fn svm_not_found(id: &str) -> Response {
    json_error(
        400,
        "StorageVirtualMachineNotFound",
        &format!("Storage virtual machine '{id}' does not exist."),
    )
}

fn file_cache_not_found(id: &str) -> Response {
    json_error(
        400,
        "FileCacheNotFound",
        &format!("File cache '{id}' does not exist."),
    )
}

fn resource_not_found(id: &str) -> Response {
    json_error(
        400,
        "ResourceNotFoundException",
        &format!("FSx resource, {id} does not exist"),
    )
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
            service: "fsx".to_string(),
            operation: operation.to_string(),
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            account,
            region: "us-east-1".to_string(),
        };
        let resp = dispatch(&request).expect("fsx dispatch");
        (
            resp.status,
            serde_json::from_slice(&resp.body).unwrap_or(json!({})),
        )
    }

    fn make_file_system(account: u64, body: Value) -> (String, Value) {
        let (status, body) = send(account, "CreateFileSystem", body);
        assert_eq!(status, 200, "create fs failed: {body}");
        let id = body["FileSystem"]["FileSystemId"]
            .as_str()
            .expect("FileSystemId")
            .to_string();
        (id, body)
    }

    fn make_lustre_fs(account: u64) -> String {
        make_file_system(
            account,
            json!({
                "FileSystemType": "LUSTRE",
                "StorageCapacity": 1200,
                "SubnetIds": ["subnet-00000001"],
                "LustreConfiguration": {"DeploymentType": "SCRATCH_1"},
            }),
        )
        .0
    }

    #[test]
    fn file_system_lifecycle() {
        let account = next_account();
        let (fs_id, created) = make_file_system(
            account,
            json!({
                "FileSystemType": "LUSTRE",
                "StorageCapacity": 1200,
                "SubnetIds": ["subnet-00000001"],
                "SecurityGroupIds": [],
                "LustreConfiguration": {"DeploymentType": "SCRATCH_1"},
            }),
        );
        assert!(created["FileSystem"]["ResourceARN"]
            .as_str()
            .unwrap()
            .starts_with(&format!("arn:aws:fsx:us-east-1:{account}:file-system/")));
        // moto drops falsy members: empty group lists vanish from the wire.
        assert!(created["FileSystem"].get("SecurityGroupIds").is_none());
        assert!(created["FileSystem"].get("Tags").is_none());
        assert!(created["FileSystem"]["DNSName"]
            .as_str()
            .unwrap()
            .starts_with(&format!("{fs_id}.fsx.")));

        let (status, body) = send(
            account,
            "DescribeFileSystems",
            json!({ "FileSystemIds": [fs_id] }),
        );
        assert_eq!(status, 200);
        let systems = body["FileSystems"].as_array().expect("list");
        assert_eq!(systems.len(), 1);
        assert_eq!(systems[0]["FileSystemId"], json!(fs_id));
        assert_eq!(systems[0]["FileSystemType"], "LUSTRE");
        assert_eq!(systems[0]["StorageCapacity"], 1200);

        let (status, body) = send(
            account,
            "UpdateFileSystem",
            json!({
                "FileSystemId": fs_id,
                "LustreConfiguration": {"WeeklyMaintenanceStartTime": "1:00:00"},
            }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["FileSystem"]["FileSystemId"], json!(fs_id));
        // moto merges the configuration instead of replacing it.
        assert_eq!(
            body["FileSystem"]["LustreConfiguration"]["DeploymentType"],
            json!("SCRATCH_1")
        );
        assert_eq!(
            body["FileSystem"]["LustreConfiguration"]["WeeklyMaintenanceStartTime"],
            json!("1:00:00")
        );

        let (status, body) = send(
            account,
            "DeleteFileSystem",
            json!({ "FileSystemId": fs_id }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Lifecycle"], "DELETING");
        assert_eq!(body["LustreResponse"]["FinalBackUpId"], json!(""));
        assert!(body.get("WindowsResponse") == Some(&Value::Null));

        let (status, body) = send(
            account,
            "DescribeFileSystems",
            json!({ "FileSystemIds": [fs_id] }),
        );
        assert_eq!(status, 200);
        assert!(body["FileSystems"].as_array().unwrap().is_empty());
    }

    #[test]
    fn create_file_system_invalid_type_is_moto_500() {
        let account = next_account();
        let (status, body) = send(
            account,
            "CreateFileSystem",
            json!({ "FileSystemType": "probe-test-value" }),
        );
        assert_eq!(status, 500);
        assert_eq!(body["__type"], "InternalError");
        assert_eq!(body["message"], "Invalid FileSystemType: probe-test-value");

        // An absent FileSystemType stringifies like python's None.
        let (status, body) = send(account, "CreateFileSystem", json!({}));
        assert_eq!(status, 500);
        assert_eq!(body["message"], "Invalid FileSystemType: None");
    }

    #[test]
    fn delete_file_system_unknown_id_is_moto_500() {
        // moto indexes the map unguarded: the KeyError message is the id.
        let account = next_account();
        let (status, body) = send(
            account,
            "DeleteFileSystem",
            json!({ "FileSystemId": "probe-test-value" }),
        );
        assert_eq!(status, 500);
        assert_eq!(body["__type"], "InternalError");
        assert_eq!(body["message"], "'probe-test-value'");
    }

    #[test]
    fn describe_file_systems_skips_unknown_ids() {
        let account = next_account();
        let (status, body) = send(
            account,
            "DescribeFileSystems",
            json!({ "FileSystemIds": ["fs-does-not-exist"] }),
        );
        assert_eq!(status, 200);
        assert!(body["FileSystems"].as_array().unwrap().is_empty());
        assert!(body["NextToken"].is_null());
    }

    #[test]
    fn update_file_system_unknown_id() {
        let account = next_account();
        let (status, body) = send(
            account,
            "UpdateFileSystem",
            json!({ "FileSystemId": "fs-does-not-exist" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "FileSystemNotFound");
        assert_eq!(
            body["message"],
            "File system 'fs-does-not-exist' does not exist."
        );
    }

    #[test]
    fn backup_lifecycle() {
        let account = next_account();
        let fs_id = make_lustre_fs(account);

        let (status, body) = send(account, "CreateBackup", json!({ "FileSystemId": fs_id }));
        assert_eq!(status, 200);
        let backup = &body["Backup"];
        let backup_id = backup["BackupId"].as_str().unwrap().to_string();
        assert!(backup_id.starts_with("backup-"));
        assert_eq!(backup["Lifecycle"], "CREATING");
        // moto keeps empty tag lists on backups (`v is not None`).
        assert!(backup["Tags"].as_array().unwrap().is_empty());
        assert_eq!(backup["FileSystemId"], json!(fs_id));
        assert!(backup["ResourceARN"]
            .as_str()
            .unwrap()
            .starts_with(&format!("arn:aws:fsx:us-east-1:{account}:backup/")));
        // moto fills a token when the client omits one.
        assert!(backup["ClientRequestToken"].as_str().is_some());
        assert!(backup["CreationTime"].as_f64().is_some());

        let (status, body) = send(account, "DescribeBackups", json!({}));
        assert_eq!(status, 200);
        assert!(body["Backups"]
            .as_array()
            .unwrap()
            .iter()
            .any(|b| b["BackupId"] == json!(backup_id)));

        let (status, body) = send(
            account,
            "DescribeBackups",
            json!({ "BackupIds": [backup_id] }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Backups"].as_array().unwrap().len(), 1);

        let (status, body) = send(
            account,
            "DescribeBackups",
            json!({ "BackupIds": ["backup-does-not-exist"] }),
        );
        assert_eq!(status, 200);
        assert!(body["Backups"].as_array().unwrap().is_empty());

        let (status, body) = send(account, "DeleteBackup", json!({ "BackupId": backup_id }));
        assert_eq!(status, 200);
        assert_eq!(body["Lifecycle"], "DELETED");

        let (status, body) = send(account, "DeleteBackup", json!({ "BackupId": backup_id }));
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ResourceNotFoundException");

        // An unknown file system is ResourceNotFound, not a crash.
        let (status, body) = send(
            account,
            "CreateBackup",
            json!({ "FileSystemId": "fs-does-not-exist" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn file_system_from_backup() {
        let account = next_account();
        let fs_id = make_lustre_fs(account);
        let (_, body) = send(account, "CreateBackup", json!({ "FileSystemId": fs_id }));
        let backup_id = body["Backup"]["BackupId"].as_str().unwrap().to_string();

        let (status, body) = send(
            account,
            "CreateFileSystemFromBackup",
            json!({ "BackupId": backup_id, "SubnetIds": ["subnet-00000001"] }),
        );
        assert_eq!(status, 200);
        let new_fs = &body["FileSystem"];
        let new_fs_id = new_fs["FileSystemId"].as_str().unwrap().to_string();
        assert_eq!(new_fs["FileSystemType"], "LUSTRE");
        // moto defaults capacity/storage to the original file system's.
        assert_eq!(new_fs["StorageCapacity"], 1200);
        assert_ne!(new_fs_id, fs_id);

        let (status, body) = send(
            account,
            "CreateFileSystemFromBackup",
            json!({ "BackupId": "backup-does-not-exist" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "BackupNotFound");
        assert_eq!(
            body["message"],
            "Backup 'backup-does-not-exist' does not exist."
        );
    }

    #[test]
    fn svm_volume_snapshot_lifecycle() {
        let account = next_account();
        let (status, body) = send(
            account,
            "CreateFileSystem",
            json!({
                "FileSystemType": "ONTAP",
                "StorageCapacity": 1024,
                "SubnetIds": ["subnet-00000001"],
                "OntapConfiguration": {"DeploymentType": "SINGLE_AZ_1", "ThroughputCapacity": 128},
            }),
        );
        assert_eq!(status, 200, "ontap fs: {body}");
        let fs_id = body["FileSystem"]["FileSystemId"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(body["FileSystem"]["FileSystemType"], "ONTAP");

        let (status, body) = send(
            account,
            "CreateStorageVirtualMachine",
            json!({ "FileSystemId": fs_id, "Name": "test-svm" }),
        );
        assert_eq!(status, 200);
        let svm = &body["StorageVirtualMachine"];
        let svm_id = svm["StorageVirtualMachineId"].as_str().unwrap().to_string();
        assert!(svm_id.starts_with("svm-"));
        assert_eq!(svm["Name"], "test-svm");
        assert_eq!(svm["FileSystemId"], json!(fs_id));
        assert_eq!(svm["Lifecycle"], "CREATED");
        assert_eq!(svm["RootVolumeSecurityStyle"], "UNIX");

        let (status, body) = send(
            account,
            "DescribeStorageVirtualMachines",
            json!({ "StorageVirtualMachineIds": [svm_id] }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["StorageVirtualMachines"].as_array().unwrap().len(), 1);

        let (status, body) = send(
            account,
            "CreateVolume",
            json!({
                "VolumeType": "ONTAP",
                "Name": "test-vol",
                "OntapConfiguration": {
                    "SizeInMegabytes": 1024,
                    "StorageVirtualMachineId": svm_id,
                    "JunctionPath": "/vol1",
                },
            }),
        );
        assert_eq!(status, 200);
        let vol = &body["Volume"];
        let vol_id = vol["VolumeId"].as_str().unwrap().to_string();
        assert!(vol_id.starts_with("fsvol-"));
        // An existing SVM links the volume back to its file system.
        assert_eq!(vol["FileSystemId"], json!(fs_id));
        assert_eq!(vol["VolumeType"], "ONTAP");
        assert_eq!(vol["Lifecycle"], "CREATED");

        let (status, body) = send(
            account,
            "CreateSnapshot",
            json!({ "Name": "my-snap", "VolumeId": vol_id }),
        );
        assert_eq!(status, 200);
        let snap = &body["Snapshot"];
        let snap_id = snap["SnapshotId"].as_str().unwrap().to_string();
        assert!(snap_id.starts_with("fsvolsnap-"));
        assert_eq!(snap["VolumeId"], json!(vol_id));
        assert_eq!(snap["Lifecycle"], "AVAILABLE");

        let (status, body) = send(
            account,
            "UpdateSnapshot",
            json!({ "SnapshotId": snap_id, "Name": "renamed-snap" }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Snapshot"]["Name"], "renamed-snap");

        let (status, body) = send(
            account,
            "DescribeSnapshots",
            json!({ "SnapshotIds": [snap_id] }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Snapshots"][0]["Name"], "renamed-snap");

        let (status, _) = send(account, "DeleteSnapshot", json!({ "SnapshotId": snap_id }));
        assert_eq!(status, 200);
        let (status, body) = send(account, "DeleteVolume", json!({ "VolumeId": vol_id }));
        assert_eq!(status, 200);
        assert_eq!(body["Lifecycle"], "DELETING");
        assert_eq!(body["OntapResponse"]["FinalBackupId"], Value::Null);
        let (status, _) = send(
            account,
            "DeleteStorageVirtualMachine",
            json!({ "StorageVirtualMachineId": svm_id }),
        );
        assert_eq!(status, 200);
        let (status, _) = send(
            account,
            "DeleteFileSystem",
            json!({ "FileSystemId": fs_id }),
        );
        assert_eq!(status, 200);
    }

    #[test]
    fn volume_with_unknown_svm_has_no_file_system_link() {
        let account = next_account();
        let (status, body) = send(
            account,
            "CreateVolume",
            json!({
                "VolumeType": "ONTAP",
                "Name": "filtered-vol",
                "OntapConfiguration": {
                    "SizeInMegabytes": 512,
                    "StorageVirtualMachineId": "svm-0123456789abcdef0",
                    "JunctionPath": "/filt",
                },
            }),
        );
        assert_eq!(status, 200);
        let vol = &body["Volume"];
        assert_eq!(vol["Name"], "filtered-vol");
        assert!(vol.get("FileSystemId").is_none());

        let vol_id = vol["VolumeId"].as_str().unwrap().to_string();
        let (status, body) = send(account, "DescribeVolumes", json!({ "VolumeIds": [vol_id] }));
        assert_eq!(status, 200);
        assert_eq!(body["Volumes"].as_array().unwrap().len(), 1);
        assert_eq!(body["Volumes"][0]["Name"], "filtered-vol");

        let (status, body) = send(
            account,
            "DeleteVolume",
            json!({ "VolumeId": "fsvol-does-not-exist" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "VolumeNotFound");

        let (status, body) = send(
            account,
            "DescribeVolumes",
            json!({ "VolumeIds": ["fsvol-does-not-exist"] }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "VolumeNotFound");
    }

    #[test]
    fn snapshot_and_volume_not_found_paths() {
        let account = next_account();
        let (status, body) = send(
            account,
            "CreateSnapshot",
            json!({ "Name": "n", "VolumeId": "fsvol-does-not-exist" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "VolumeNotFound");

        let (status, body) = send(
            account,
            "DeleteSnapshot",
            json!({ "SnapshotId": "fsvolsnap-does-not-exist" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "SnapshotNotFound");

        let (status, body) = send(
            account,
            "UpdateSnapshot",
            json!({ "SnapshotId": "fsvolsnap-does-not-exist", "Name": "x" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "SnapshotNotFound");
    }

    #[test]
    fn svm_not_found_paths() {
        let account = next_account();
        let (status, body) = send(
            account,
            "CreateStorageVirtualMachine",
            json!({ "FileSystemId": "fs-does-not-exist", "Name": "svm" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "FileSystemNotFound");

        let (status, body) = send(
            account,
            "DeleteStorageVirtualMachine",
            json!({ "StorageVirtualMachineId": "svm-0123456789abcdef0" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "StorageVirtualMachineNotFound");

        let (status, body) = send(
            account,
            "DescribeStorageVirtualMachines",
            json!({ "StorageVirtualMachineIds": ["svm-0123456789abcdef0"] }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "StorageVirtualMachineNotFound");
    }

    #[test]
    fn alias_operations() {
        let account = next_account();
        let fs_id = make_lustre_fs(account);

        let (status, body) = send(
            account,
            "AssociateFileSystemAliases",
            json!({ "FileSystemId": fs_id, "Aliases": ["accounting.example.com"] }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Aliases"][0]["Name"], "accounting.example.com");
        assert_eq!(body["Aliases"][0]["Lifecycle"], "AVAILABLE");

        // Associating the same alias twice does not duplicate it.
        let (status, _) = send(
            account,
            "AssociateFileSystemAliases",
            json!({ "FileSystemId": fs_id, "Aliases": ["accounting.example.com"] }),
        );
        assert_eq!(status, 200);
        let (status, body) = send(
            account,
            "DescribeFileSystemAliases",
            json!({ "FileSystemId": fs_id }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Aliases"].as_array().unwrap().len(), 1);

        let (status, body) = send(
            account,
            "DisassociateFileSystemAliases",
            json!({ "FileSystemId": fs_id, "Aliases": ["accounting.example.com"] }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Aliases"][0]["Lifecycle"], "DELETING");

        let (status, body) = send(
            account,
            "DescribeFileSystemAliases",
            json!({ "FileSystemId": "fs-does-not-exist" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "FileSystemNotFound");
    }

    #[test]
    fn tag_operations() {
        let account = next_account();
        let (status, body) = send(
            account,
            "CreateFileSystem",
            json!({
                "FileSystemType": "LUSTRE",
                "StorageCapacity": 1200,
                "SubnetIds": ["subnet-1"],
                "Tags": [{"Key": "env", "Value": "test"}],
            }),
        );
        assert_eq!(status, 200);
        let arn = body["FileSystem"]["ResourceARN"]
            .as_str()
            .unwrap()
            .to_string();
        // Tags land on the create response through moto's tagger.
        assert_eq!(
            body["FileSystem"]["Tags"]
                .as_array()
                .unwrap()
                .iter()
                .find(|t| t["Key"] == json!("env"))
                .map(|t| t["Value"].clone()),
            Some(json!("test"))
        );

        let (status, _) = send(
            account,
            "TagResource",
            json!({ "ResourceARN": arn, "Tags": [{"Key": "team", "Value": "platform"}] }),
        );
        assert_eq!(status, 200);

        let (status, body) = send(
            account,
            "ListTagsForResource",
            json!({ "ResourceARN": arn }),
        );
        assert_eq!(status, 200);
        let tags = body["Tags"].as_array().unwrap();
        assert_eq!(tags.len(), 2);
        assert_eq!(tags[0]["Key"], json!("env"));
        assert_eq!(tags[1]["Value"], "platform");

        let (status, _) = send(
            account,
            "UntagResource",
            json!({ "ResourceARN": arn, "TagKeys": ["env"] }),
        );
        assert_eq!(status, 200);
        let (status, body) = send(
            account,
            "ListTagsForResource",
            json!({ "ResourceARN": arn }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Tags"].as_array().unwrap().len(), 1);
        assert_eq!(body["Tags"][0]["Key"], json!("team"));

        // moto does not validate the ARN against known resources here.
        let (status, body) = send(
            account,
            "ListTagsForResource",
            json!({ "ResourceARN": format!("arn:aws:fsx:us-east-1:{account}:file-system/fs-unknown") }),
        );
        assert_eq!(status, 200);
        assert!(body["Tags"].as_array().unwrap().is_empty());
    }

    #[test]
    fn data_repository_association_lifecycle() {
        let account = next_account();
        let (status, body) = send(
            account,
            "CreateFileSystem",
            json!({
                "FileSystemType": "LUSTRE",
                "StorageCapacity": 1200,
                "SubnetIds": ["subnet-1"],
                "LustreConfiguration": {"DeploymentType": "PERSISTENT_1"},
            }),
        );
        assert_eq!(status, 200);
        let fs_id = body["FileSystem"]["FileSystemId"]
            .as_str()
            .unwrap()
            .to_string();

        let (status, body) = send(
            account,
            "CreateDataRepositoryAssociation",
            json!({
                "FileSystemId": fs_id,
                "FileSystemPath": "/data",
                "DataRepositoryPath": "s3://my-bucket/prefix",
            }),
        );
        assert_eq!(status, 200);
        let assoc = &body["Association"];
        let assoc_id = assoc["AssociationId"].as_str().unwrap().to_string();
        assert!(assoc_id.starts_with("dra-"));
        assert_eq!(assoc["FileSystemId"], json!(fs_id));
        assert_eq!(assoc["FileSystemPath"], "/data");
        assert_eq!(assoc["DataRepositoryPath"], "s3://my-bucket/prefix");
        assert_eq!(assoc["Lifecycle"], "AVAILABLE");

        let (status, body) = send(
            account,
            "DescribeDataRepositoryAssociations",
            json!({ "AssociationIds": [assoc_id] }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Associations"].as_array().unwrap().len(), 1);
        // Unknown association ids are skipped, not errors.
        let (status, body) = send(
            account,
            "DescribeDataRepositoryAssociations",
            json!({ "AssociationIds": ["dra-does-not-exist"] }),
        );
        assert_eq!(status, 200);
        assert!(body["Associations"].as_array().unwrap().is_empty());

        let (status, body) = send(
            account,
            "UpdateDataRepositoryAssociation",
            json!({ "AssociationId": assoc_id }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Association"]["AssociationId"], json!(assoc_id));

        let (status, body) = send(
            account,
            "DeleteDataRepositoryAssociation",
            json!({"AssociationId": assoc_id, "DeleteDataInFileSystem": false}),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Lifecycle"], "DELETING");
        assert_eq!(body["DeleteDataInFileSystem"], false);

        let (status, body) = send(
            account,
            "UpdateDataRepositoryAssociation",
            json!({ "AssociationId": "dra-does-not-exist" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ResourceNotFoundException");

        let (status, body) = send(
            account,
            "CreateDataRepositoryAssociation",
            json!({"FileSystemId": "fs-does-not-exist", "DataRepositoryPath": "s3://b/p"}),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "FileSystemNotFound");
    }

    #[test]
    fn describe_only_surfaces_stay_empty() {
        let account = next_account();
        for op in [
            "DescribeDataRepositoryTasks",
            "DescribeFileCaches",
            "DescribeS3AccessPointAttachments",
        ] {
            let (status, body) = send(account, op, json!({}));
            assert_eq!(status, 200);
            assert!(!body.is_null());
        }
        let (status, body) = send(account, "DescribeSharedVpcConfiguration", json!({}));
        assert_eq!(status, 200);
        assert_eq!(
            body["EnableFsxRouteTableUpdatesFromParticipantAccounts"],
            json!("false")
        );
    }

    #[test]
    fn create_volume_from_backup_requires_existing_backup() {
        let account = next_account();
        let (status, body) = send(
            account,
            "CreateVolumeFromBackup",
            json!({
                "BackupId": "backup-does-not-exist",
                "Name": "test-vol",
                "OntapConfiguration": {"SizeInMegabytes": 1024},
            }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "BackupNotFound");
    }

    #[test]
    fn describe_file_systems_paginates() {
        let account = next_account();
        let first = make_lustre_fs(account);
        let second = make_lustre_fs(account);

        let (status, body) = send(account, "DescribeFileSystems", json!({ "MaxResults": 1 }));
        assert_eq!(status, 200);
        let systems = body["FileSystems"].as_array().unwrap();
        assert_eq!(systems.len(), 1);
        let seen = systems[0]["FileSystemId"].as_str().unwrap().to_string();
        let next_token = body["NextToken"].as_str().expect("token").to_string();

        let (status, body) = send(
            account,
            "DescribeFileSystems",
            json!({ "MaxResults": 1, "NextToken": next_token }),
        );
        assert_eq!(status, 200);
        let systems = body["FileSystems"].as_array().unwrap();
        assert_eq!(systems.len(), 1);
        let other = systems[0]["FileSystemId"].as_str().unwrap().to_string();
        // The token must advance: page 2 holds exactly the other file system.
        let both = [first, second];
        assert!(both.iter().all(|id| id == &seen || id == &other));
        assert_ne!(seen, other);
    }

    #[test]
    fn unimplemented_ops_answer_501_like_moto() {
        // The wave-12 conversions (data-repository tasks, file caches,
        // S3 access-point attachments, backup copies and the lifecycle
        // helpers) now answer real arms; only these moto gaps remain.
        let account = next_account();
        for op in [
            "CopySnapshotAndUpdateVolume",
            "RestoreVolumeFromSnapshot",
            "UpdateVolume",
        ] {
            let (status, body) = send(account, op, json!({}));
            assert_eq!(status, 501, "{op} must stay unimplemented");
            assert_eq!(body["__type"], "NotImplemented");
            assert_eq!(
                body["message"],
                format!("The {op} action has not been implemented")
            );
        }
    }

    #[test]
    fn data_repository_task_lifecycle() {
        let account = next_account();
        let fs_id = make_lustre_fs(account);

        let (status, body) = send(
            account,
            "CreateDataRepositoryTask",
            json!({
                "Type": "EXPORT_TO_REPOSITORY",
                "FileSystemId": fs_id,
                "Paths": ["/foo"],
                "Report": {"Enabled": true, "Format": "REPORT_CSV_20191124"},
                "Tags": [{"Key": "team", "Value": "core"}],
            }),
        );
        assert_eq!(status, 200);
        let task = &body["DataRepositoryTask"];
        let task_id = task["TaskId"].as_str().unwrap().to_string();
        assert!(task_id.starts_with("task-"));
        assert_eq!(task["Lifecycle"], "PENDING");
        assert_eq!(task["Type"], "EXPORT_TO_REPOSITORY");
        assert_eq!(task["FileSystemId"], json!(fs_id));
        assert!(task["ResourceARN"]
            .as_str()
            .unwrap()
            .ends_with(&format!(":task/{task_id}")));
        assert_eq!(task["Report"]["Enabled"], true);
        assert_eq!(task["Status"]["TotalCount"], 0);

        let (status, body) = send(account, "DescribeDataRepositoryTasks", json!({}));
        assert_eq!(status, 200);
        assert_eq!(body["DataRepositoryTasks"].as_array().unwrap().len(), 1);

        let (status, body) = send(account, "CancelDataRepositoryTask", json!({ "TaskId": task_id }));
        assert_eq!(status, 200);
        assert_eq!(body["Lifecycle"], "CANCELED");

        // Canceling an already-ended task is the declared fault.
        let (status, body) = send(account, "CancelDataRepositoryTask", json!({ "TaskId": task_id }));
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "DataRepositoryTaskEnded");

        let (status, body) = send(
            account,
            "CancelDataRepositoryTask",
            json!({ "TaskId": "task-does-not-exist" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "DataRepositoryTaskNotFound");

        let (status, body) = send(
            account,
            "CreateDataRepositoryTask",
            json!({
                "Type": "EXPORT_TO_REPOSITORY",
                "FileSystemId": "fs-does-not-exist",
                "Report": {"Enabled": false},
            }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "FileSystemNotFound");
    }

    #[test]
    fn file_cache_lifecycle() {
        let account = next_account();
        let (status, body) = send(
            account,
            "CreateFileCache",
            json!({
                "FileCacheType": "LUSTRE",
                "FileCacheTypeVersion": "2.12",
                "StorageCapacity": 4800,
                "SubnetIds": ["subnet-00000001"],
                "LustreConfiguration": {
                    "DeploymentType": "CACHE_1",
                    "PerUnitStorageThroughput": 500,
                    "MetadataConfiguration": {"StorageCapacity": 2400},
                },
            }),
        );
        assert_eq!(status, 200);
        let cache = &body["FileCache"];
        let cache_id = cache["FileCacheId"].as_str().unwrap().to_string();
        assert!(cache_id.starts_with("fc-"));
        assert_eq!(cache["FileCacheType"], "LUSTRE");
        assert_eq!(cache["StorageCapacity"], 4800);
        assert!(cache["ResourceARN"]
            .as_str()
            .unwrap()
            .ends_with(&format!(":file-cache/{cache_id}")));
        assert_eq!(cache["Lifecycle"], "AVAILABLE");
        assert!(cache["DNSName"].as_str().unwrap().starts_with(&cache_id));
        assert_eq!(
            cache["LustreConfiguration"]["MetadataConfiguration"]["StorageCapacity"],
            2400
        );

        let (status, body) = send(
            account,
            "UpdateFileCache",
            json!({
                "FileCacheId": cache_id,
                "LustreConfiguration": {"WeeklyMaintenanceStartTime": "1:00:00"},
            }),
        );
        assert_eq!(status, 200);
        assert_eq!(
            body["FileCache"]["LustreConfiguration"]["DeploymentType"],
            "CACHE_1"
        );
        assert_eq!(
            body["FileCache"]["LustreConfiguration"]["WeeklyMaintenanceStartTime"],
            "1:00:00"
        );

        let (status, body) = send(
            account,
            "DescribeFileCaches",
            json!({ "FileCacheIds": [cache_id] }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["FileCaches"].as_array().unwrap().len(), 1);

        let (status, body) = send(account, "DeleteFileCache", json!({ "FileCacheId": cache_id }));
        assert_eq!(status, 200);
        assert_eq!(body["Lifecycle"], "DELETING");

        let (status, body) = send(account, "DeleteFileCache", json!({ "FileCacheId": cache_id }));
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "FileCacheNotFound");

        let (status, body) = send(
            account,
            "CreateFileCache",
            json!({
                "FileCacheType": "CAFFEINE",
                "FileCacheTypeVersion": "2.12",
                "StorageCapacity": 4800,
                "SubnetIds": ["subnet-00000001"],
            }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "BadRequest");

        // Cache-based associations ride along on CreateFileCache.
        let (status, body) = send(
            account,
            "CreateFileCache",
            json!({
                "FileCacheType": "LUSTRE",
                "FileCacheTypeVersion": "2.12",
                "StorageCapacity": 4800,
                "SubnetIds": ["subnet-00000001"],
                "DataRepositoryAssociations": [
                    {"DataRepositoryPath": "s3://cache-source", "FileSystemPath": "/nope"}
                ],
            }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["FileCache"]["DataRepositoryAssociationIds"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn s3_access_point_attachments() {
        let account = next_account();
        let fs_id = make_lustre_fs(account);
        // Attach needs a real volume on the file system.
        let (status, body) = send(
            account,
            "CreateVolume",
            json!({
                "VolumeType": "OPENZFS",
                "Name": "vol0",
                "OpenZFSConfiguration": {"ParentVolumeId": fs_id, "StorageCapacity": 64},
            }),
        );
        assert_eq!(status, 200);
        let volume_id = body["Volume"]["VolumeId"].as_str().unwrap().to_string();

        let (status, body) = send(
            account,
            "CreateAndAttachS3AccessPoint",
            json!({
                "Name": "ap-share",
                "Type": "OPENZFS",
                "OpenZFSConfiguration": {
                    "VolumeId": volume_id,
                    "FileSystemIdentity": {"Type": "POSIX", "PosixUser": {"Uid": 1000, "Gid": 1000}},
                },
            }),
        );
        assert_eq!(status, 200);
        let attachment = &body["S3AccessPointAttachment"];
        assert_eq!(attachment["Name"], "ap-share");
        assert_eq!(attachment["Type"], "OPENZFS");
        assert_eq!(attachment["Lifecycle"], "AVAILABLE");
        assert_eq!(attachment["OpenZFSConfiguration"]["VolumeId"], json!(volume_id));
        assert!(attachment["S3AccessPoint"]["ResourceARN"]
            .as_str()
            .unwrap()
            .ends_with(":accesspoint/ap-share"));

        // Duplicate names are the declared AccessPointAlreadyOwnedByYou fault.
        let (status, body) = send(
            account,
            "CreateAndAttachS3AccessPoint",
            json!({
                "Name": "ap-share",
                "Type": "OPENZFS",
                "OpenZFSConfiguration": {
                    "VolumeId": volume_id,
                    "FileSystemIdentity": {"Type": "POSIX", "PosixUser": {"Uid": 1000, "Gid": 1000}},
                },
            }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "AccessPointAlreadyOwnedByYou");

        // A bogus volume answers VolumeNotFound.
        let (status, body) = send(
            account,
            "CreateAndAttachS3AccessPoint",
            json!({
                "Name": "ap-bogus",
                "Type": "OPENZFS",
                "OpenZFSConfiguration": {
                    "VolumeId": "fsvol-does-not-exist",
                    "FileSystemIdentity": {"Type": "POSIX", "PosixUser": {"Uid": 1, "Gid": 1}},
                },
            }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "VolumeNotFound");

        let (status, body) = send(
            account,
            "DescribeS3AccessPointAttachments",
            json!({}),
        );
        assert_eq!(status, 200);
        assert!(body["S3AccessPointAttachments"]
            .as_array()
            .unwrap()
            .iter()
            .any(|a| a["Name"] == json!("ap-share")));

        let (status, body) = send(
            account,
            "DetachAndDeleteS3AccessPoint",
            json!({ "Name": "ap-share" }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Lifecycle"], "DELETING");

        let (status, body) = send(
            account,
            "DetachAndDeleteS3AccessPoint",
            json!({ "Name": "ap-share" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "S3AccessPointAttachmentNotFound");
    }

    #[test]
    fn copy_backup_within_and_across_region() {
        let account = next_account();
        let fs_id = make_lustre_fs(account);
        let (_, body) = send(account, "CreateBackup", json!({ "FileSystemId": fs_id }));
        let source_id = body["Backup"]["BackupId"].as_str().unwrap().to_string();

        let (status, body) = send(
            account,
            "CopyBackup",
            json!({"SourceBackupId": source_id, "CopyTags": true}),
        );
        assert_eq!(status, 200);
        let backup = &body["Backup"];
        let copy_id = backup["BackupId"].as_str().unwrap().to_string();
        assert!(copy_id.starts_with("backup-"));
        assert_eq!(backup["Lifecycle"], "COPYING");
        assert_eq!(backup["Type"], "USER_INITIATED");
        assert_eq!(backup["SourceBackupId"], json!(source_id));
        assert_eq!(backup["FileSystem"]["FileSystemId"], json!(fs_id));

        let (status, body) = send(
            account,
            "CopyBackup",
            json!({"SourceBackupId": "backup-does-not-exist"}),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "BackupNotFound");
    }

    #[test]
    fn file_system_lifecycle_helpers() {
        let account = next_account();
        let fs_id = make_lustre_fs(account);

        let (status, body) = send(
            account,
            "ReleaseFileSystemNfsV3Locks",
            json!({"FileSystemId": fs_id}),
        );
        assert_eq!(status, 200);
        assert_eq!(body["FileSystem"]["FileSystemId"], json!(fs_id));

        let (status, body) = send(
            account,
            "ReleaseFileSystemNfsV3Locks",
            json!({"FileSystemId": "fs-does-not-exist"}),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "FileSystemNotFound");

        let (status, body) = send(
            account,
            "StartMisconfiguredStateRecovery",
            json!({"FileSystemId": fs_id}),
        );
        assert_eq!(status, 200);
        assert_eq!(body["FileSystem"]["FileSystemId"], json!(fs_id));

        let (status, body) = send(
            account,
            "UpdateSharedVpcConfiguration",
            json!({"EnableFsxRouteTableUpdatesFromParticipantAccounts": "true"}),
        );
        assert_eq!(status, 200);
        assert_eq!(
            body["EnableFsxRouteTableUpdatesFromParticipantAccounts"],
            "true"
        );
        let (status, body) = send(account, "DescribeSharedVpcConfiguration", json!({}));
        assert_eq!(status, 200);
        assert_eq!(
            body["EnableFsxRouteTableUpdatesFromParticipantAccounts"],
            "true"
        );

        // A wrong flag value answers BadRequest.
        let (status, body) = send(
            account,
            "UpdateSharedVpcConfiguration",
            json!({"EnableFsxRouteTableUpdatesFromParticipantAccounts": "yes"}),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "BadRequest");
    }

    #[test]
    fn update_storage_virtual_machine() {
        let account = next_account();
        let fs_id = make_lustre_fs(account);
        let (_, body) = send(
            account,
            "CreateStorageVirtualMachine",
            json!({"FileSystemId": fs_id, "Name": "svm0"}),
        );
        let svm_id = body["StorageVirtualMachine"]["StorageVirtualMachineId"]
            .as_str()
            .unwrap()
            .to_string();

        let (status, body) = send(
            account,
            "UpdateStorageVirtualMachine",
            json!({
                "StorageVirtualMachineId": svm_id,
                "ActiveDirectoryConfiguration": {
                    "SelfManagedActiveDirectoryConfiguration": {"UserName": "admin"}
                },
            }),
        );
        assert_eq!(status, 200);
        assert_eq!(
            body["StorageVirtualMachine"]["ActiveDirectoryConfiguration"]
                ["SelfManagedActiveDirectoryConfiguration"]["UserName"],
            json!("admin")
        );

        let (status, body) = send(
            account,
            "UpdateStorageVirtualMachine",
            json!({"StorageVirtualMachineId": "svm-does-not-exist"}),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "StorageVirtualMachineNotFound");
    }
}
