//! AWS EFS backend — port of moto's `efs` engine (file systems, access
//! points, mount targets, lifecycle/backup/file-system policies, tags under
//! resource-tags + legacy create-tags/delete-tags/tags paths, account
//! preferences and replication configurations).
//!
//! Scope note (moto cross-store): moto's create_mount_target validates the
//! SubnetId and security groups against its own EC2 backend and derives the
//! mount-target IP from the subnet CIDR. The rotocore gateway serves EC2
//! through the moto sidecar (no native EC2 store exists to consult), so this
//! backend records the subnet id verbatim, synthesizes the private IP, and
//! stores security group ids without the moto-side EC2 validation — the
//! compat suite exercises no EC2-dependent behavior beyond passing real
//! subnet/s/group ids. File-system fields otherwise mirror moto's info_json
//! exactly (CreationTime float, SizeInBytes triple with Timestamp, Name from
//! the `Name` tag, LifeCycleState "available", AvailabilityZone fields only
//! when one was requested). Status codes follow botocore's responseCode table.

use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Value};
use std::collections::HashMap;

static SLOT: Lazy<store::Slot<EfsState>> = Lazy::new(|| store::slot::<EfsState>("efs"));

fn new_hex(n: usize) -> String {
    uuid::Uuid::new_v4().simple().to_string()[..n].to_string()
}

fn now_unix() -> f64 {
    crate::proto::now_unix()
}

/// The three fields EFS echoes as a tag-shaped list; stored as sorted pairs.
type Tags = Vec<(String, String)>;

#[derive(Default)]
struct EfsState {
    creation_tokens: HashMap<String, String>, // token -> fsid
    file_systems: HashMap<String, FileSystem>,
    access_points: HashMap<String, AccessPoint>,
    mount_targets: HashMap<String, MountTarget>,
    mount_target_ips: u64,
    tags: HashMap<String, Tags>,
    account: String,
    region: String,
}

struct FileSystem {
    creation_token: String,
    file_system_id: String,
    file_system_arn: String,
    creation_time: f64,
    performance_mode: String,
    encrypted: bool,
    kms_key_id: Option<Value>,
    throughput_mode: String,
    provisioned_throughput: Option<Value>,
    availability_zone_name: Option<Value>,
    life_cycle_state: String,
    size_value: u64,
    lifecycle_policies: Value,
    file_system_policy: Option<String>,
    file_system_protection: Value,
    replication_configuration: Option<Value>,
    backup: bool,
}

struct MountTarget {
    file_system_id: String,
    subnet_id: String,
    ip_address: String,
    security_groups: Option<Value>,
    mount_target_id: String,
}

struct AccessPoint {
    access_point_id: String,
    access_point_arn: String,
    client_token: Option<Value>,
    file_system_id: String,
    posix_user: Value,
    root_directory: Value,
}

impl FileSystem {
    /// Mirror of moto's FileSystem.info_json: dunder-free instance fields
    /// camel-cased plus Tags/SizeInBytes/NumberOfMountTargets/Name.
    fn info_json(&self, tags: &Tags) -> Value {
        let security_in_ia = 0;
        let mut out = json!({
            "CreationToken": self.creation_token,
            "FileSystemId": self.file_system_id,
            "FileSystemArn": self.file_system_arn,
            "CreationTime": self.creation_time,
            "LifeCycleState": self.life_cycle_state,
            "PerformanceMode": self.performance_mode,
            "Encrypted": self.encrypted,
            "KmsKeyId": self.kms_key_id.clone().unwrap_or(Value::Null),
            "ThroughputMode": self.throughput_mode,
            "ProvisionedThroughputInMibps": self.provisioned_throughput.clone().unwrap_or(json!(0)),
            "OwnerId": "",
            "LifeCycleState2": Value::Null,
            "Tags": tags_json(tags),
            "SizeInBytes": {
                "Value": self.size_value,
                "ValueInIA": security_in_ia,
                "ValueInStandard": self.size_value,
                "Timestamp": now_unix(),
            },
            "LifecyclePolicies": self.lifecycle_policies,
            "FileSystemPolicy": self.file_system_policy.clone().map(Value::String).unwrap_or(Value::Null),
            "FileSystemProtection": self.file_system_protection,
            "Name": name_of(tags),
            "NumberOfMountTargets": 0,
            "ThroughputMode2": Value::Null,
        });
        if let Some(map) = out.as_object_mut() {
            map.remove("LifeCycleState2");
            map.remove("ThroughputMode2");
            if let Some(name) = null_if_absent(self.availability_zone_name.clone()) {
                map.insert("AvailabilityZoneName".into(), name);
            }
        }
        out
    }
}

fn null_if_absent(v: Option<Value>) -> Option<Value> {
    v.filter(|x| !x.is_null())
}

fn tags_json(tags: &Tags) -> Value {
    Value::Array(
        tags.iter()
            .map(|(k, v)| json!({"Key": k, "Value": v}))
            .collect(),
    )
}

fn name_of(tags: &Tags) -> String {
    tags.iter()
        .find(|(k, _)| k == "Name")
        .map(|(_, v)| v.clone())
        .unwrap_or_default()
}

fn fs_not_found(id: &str) -> Response {
    crate::proto::json_error(
        404,
        "FileSystemNotFound",
        &format!("File system '{id}' does not exist."),
    )
}

fn mount_target_not_found(id: &str) -> Response {
    crate::proto::json_error(
        404,
        "MountTargetNotFound",
        &format!("Mount target '{id}' does not exist."),
    )
}

fn access_point_not_found(id: &str) -> Response {
    crate::proto::json_error(
        404,
        "AccessPointNotFound",
        &format!("Access Point {id} does not exist."),
    )
}

fn policy_not_found() -> Response {
    crate::proto::json_error(404, "PolicyNotFound", "None")
}

pub fn dispatch(req: &Request) -> Response {
    let method = req.method.as_str();
    let path = req.path.trim_matches('/');
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    // GET filters live in the querystring; POST/PUT carry JSON bodies. Merge
    // query params over the body keys (moto merges both into request params).
    let mut params = crate::proto::parse_json_body(&req.body);
    if let Value::Object(map) = &mut params {
        for (key, values) in crate::proto::query_map(&req.query_string) {
            if let Some(single) = values.first() {
                map.insert(key, json!(single));
            }
        }
    } else {
        params = json!({});
    }

    let mut guard = SLOT.write();
    let st = guard.entry((req.account, req.region.clone())).or_default();
    if st.region.is_empty() {
        st.region = req.region.clone();
        st.account = req.account.to_string();
    }

    match segments.as_slice() {
        ["2015-02-01", "file-systems"] => match method {
            "POST" => st.create_file_system(&params),
            "GET" => st.describe_file_systems(&params),
            _ => unimplemented(req),
        },
        ["2015-02-01", "file-systems", "replication-configurations"] => {
            st.describe_replication_configurations(&params)
        }
        ["2015-02-01", "file-systems", id] => match method {
            "DELETE" => st.delete_file_system(id),
            "PUT" => st.update_file_system(id, &params),
            _ => unimplemented(req),
        },
        ["2015-02-01", "file-systems", id, "backup-policy"] => match method {
            "GET" => st.describe_backup_policy(id),
            "PUT" => st.put_backup_policy(id, &params),
            _ => unimplemented(req),
        },
        ["2015-02-01", "file-systems", id, "lifecycle-configuration"] => match method {
            "GET" => st.describe_lifecycle_configuration(id),
            "PUT" => st.put_lifecycle_configuration(id, &params),
            _ => unimplemented(req),
        },
        ["2015-02-01", "file-systems", id, "policy"] => match method {
            "GET" => st.describe_file_system_policy(id),
            "PUT" => st.put_file_system_policy(id, &params),
            "DELETE" => st.delete_file_system_policy(id),
            _ => unimplemented(req),
        },
        ["2015-02-01", "file-systems", id, "protection"] if method == "PUT" => {
            st.update_file_system_protection(id, &params)
        }
        ["2015-02-01", "file-systems", src, "replication-configuration"] => match method {
            "POST" => st.create_replication_configuration(src, &params),
            "DELETE" => st.delete_replication_configuration(src),
            _ => unimplemented(req),
        },
        ["2015-02-01", "create-tags", id] if method == "POST" => st.create_tags(id, &params),
        ["2015-02-01", "delete-tags", id] if method == "POST" => st.delete_tags(id, &params),
        ["2015-02-01", "tags", id] if method == "GET" => st.describe_tags(id),
        ["2015-02-01", "mount-targets"] => match method {
            "POST" => st.create_mount_target(&params),
            "GET" => st.describe_mount_targets(&params),
            _ => unimplemented(req),
        },
        ["2015-02-01", "mount-targets", id] if method == "DELETE" => st.delete_mount_target(id),
        ["2015-02-01", "mount-targets", id, "security-groups"] => match method {
            "GET" => st.describe_mount_target_security_groups(id),
            "PUT" => st.modify_mount_target_security_groups(id, &params),
            _ => unimplemented(req),
        },
        ["2015-02-01", "access-points"] => match method {
            "POST" => st.create_access_point(&params),
            "GET" => st.describe_access_points(&params),
            _ => unimplemented(req),
        },
        ["2015-02-01", "access-points", id] if method == "DELETE" => st.delete_access_point(id),
        ["2015-02-01", "resource-tags", id] => match method {
            "GET" => st.list_tags_for_resource(id),
            "POST" => st.tag_resource(id, &params),
            "DELETE" => {
                let keys: Vec<String> = crate::proto::query_map(&req.query_string)
                    .get("tagKeys")
                    .cloned()
                    .unwrap_or_default();
                st.untag_resource(id, &keys)
            }
            _ => unimplemented(req),
        },
        ["2015-02-01", "account-preferences"] => match method {
            "GET" => st.describe_account_preferences(),
            "PUT" => st.put_account_preferences(&params),
            _ => unimplemented(req),
        },
        _ => unimplemented(req),
    }
}

fn unimplemented(req: &Request) -> Response {
    crate::proto::json_error(
        501,
        "NotImplemented",
        &format!("The {} action has not been implemented", req.operation),
    )
}

impl EfsState {
    fn tags_of(&self, resource: &str) -> Tags {
        self.tags.get(resource).cloned().unwrap_or_default()
    }

    fn tag_add(&mut self, resource: &str, tags: &Value) {
        let Some(tags) = tags.as_array() else {
            return;
        };
        let entry = self.tags.entry(resource.to_string()).or_default();
        for t in tags {
            let key = t
                .get("Key")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let value = t
                .get("Value")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            match entry.iter_mut().find(|(k, _)| *k == key) {
                Some(slot) => slot.1 = value,
                None => entry.push((key, value)),
            }
        }
    }

    fn tag_remove(&mut self, resource: &str, keys: &[String]) {
        if let Some(entry) = self.tags.get_mut(resource) {
            entry.retain(|(k, _)| !keys.contains(k));
        }
    }

    fn drop_resource_tags(&mut self, resource: &str) {
        self.tags.remove(resource);
    }

    // ---- file systems ------------------------------------------------------

    fn create_file_system(&mut self, params: &Value) -> Response {
        let token = param_str(params, "CreationToken");
        if token.is_empty() {
            return crate::proto::json_error(400, "BadRequest", "No creation token given.");
        }
        if self.creation_tokens.contains_key(&token) {
            return crate::proto::json_error(
                409,
                "FileSystemAlreadyExists",
                &format!("File system with {token} already exists."),
            );
        }
        let fsid = format!("fs-{}", new_hex(11));
        let encrypted = params
            .get("Encrypted")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let fs = FileSystem {
            creation_token: token.clone(),
            file_system_id: fsid.clone(),
            file_system_arn: format!(
                "arn:aws:elasticfilesystem:{region}:{account}:file-system/{fsid}",
                region = self.region,
                account = self.account
            ),
            creation_time: now_unix(),
            performance_mode: params
                .get("PerformanceMode")
                .and_then(Value::as_str)
                .unwrap_or("generalPurpose")
                .to_string(),
            encrypted,
            kms_key_id: null_if_absent(params.get("KmsKeyId").cloned()),
            throughput_mode: params
                .get("ThroughputMode")
                .and_then(Value::as_str)
                .unwrap_or("bursting")
                .to_string(),
            provisioned_throughput: null_if_absent(
                params.get("ProvisionedThroughputInMibps").cloned(),
            ),
            availability_zone_name: null_if_absent(params.get("AvailabilityZoneName").cloned()),
            life_cycle_state: "available".to_string(),
            size_value: 0,
            lifecycle_policies: json!([]),
            file_system_policy: None,
            file_system_protection: json!({"ReplicationOverwriteProtection": "ENABLED"}),
            replication_configuration: None,
            backup: params
                .get("Backup")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        };
        self.creation_tokens.insert(token, fsid.clone());
        self.tag_add(&fsid, params.get("Tags").unwrap_or(&json!([])));
        let tags = self.tags_of(&fsid);
        let mut info = fs.info_json(&tags);
        info["OwnerId"] = json!(self.account);
        self.file_systems.insert(fsid, fs);
        Response::json_status(201, &info)
    }

    fn describe_file_systems(&self, params: &Value) -> Response {
        let token = params.get("CreationToken").and_then(Value::as_str);
        let fsid = params.get("FileSystemId").and_then(Value::as_str);
        if let (Some(token), Some(id)) = (token, fsid) {
            if !token.is_empty() && !id.is_empty() {
                return crate::proto::json_error(
                    400,
                    "BadRequest",
                    "Request cannot contain both a file system ID and a creation token.",
                );
            }
        }
        let mut corpus: Vec<&FileSystem> = Vec::new();
        if let Some(id) = fsid.filter(|id| !id.is_empty()) {
            match self.file_systems.get(id) {
                Some(fs) => corpus.push(fs),
                None => return fs_not_found(id),
            }
        } else if let Some(token) = token.filter(|t| !t.is_empty()) {
            corpus.extend(
                self.file_systems
                    .values()
                    .filter(|fs| fs.creation_token == token),
            );
        } else {
            corpus = self.file_systems.values().collect();
        }
        let file_systems: Vec<Value> = corpus
            .iter()
            .map(|fs| {
                let tags = self.tags_of(&fs.file_system_id);
                let mut info = fs.info_json(&tags);
                info["OwnerId"] = json!(self.account);
                info["NumberOfMountTargets"] =
                    json!(self.mount_targets_for(&fs.file_system_id).len());
                info
            })
            .collect();
        Response::json_status(200, &json!({"FileSystems": file_systems}))
    }

    fn update_file_system(&mut self, id: &str, params: &Value) -> Response {
        if !self.file_systems.contains_key(id) {
            return fs_not_found(id);
        }
        if let Some(mode) = params.get("ThroughputMode").and_then(Value::as_str) {
            self.file_systems.get_mut(id).unwrap().throughput_mode = mode.to_string();
        }
        if let Some(throughput) =
            null_if_absent(params.get("ProvisionedThroughputInMibps").cloned())
        {
            self.file_systems
                .get_mut(id)
                .unwrap()
                .provisioned_throughput = Some(throughput);
        }
        let tags = self.tags_of(id);
        let fs = self.file_systems.get(id).unwrap();
        let mut info = fs.info_json(&tags);
        info["OwnerId"] = json!(self.account);
        info["NumberOfMountTargets"] = json!(self.mount_targets_for(id).len());
        Response::json_status(202, &info)
    }

    fn delete_file_system(&mut self, id: &str) -> Response {
        if !self.file_systems.contains_key(id) {
            return fs_not_found(id);
        }
        if !self.mount_targets_for(id).is_empty() {
            return crate::proto::json_error(
                409,
                "FileSystemInUse",
                "Must delete all mount targets before deleting file system.",
            );
        }
        if let Some(fs) = self.file_systems.remove(id) {
            self.creation_tokens.remove(&fs.creation_token);
        }
        self.drop_resource_tags(id);
        Response::json_status(204, &json!({}))
    }

    // ---- mount targets -----------------------------------------------------

    fn create_mount_target(&mut self, params: &Value) -> Response {
        let fsid = param_str(params, "FileSystemId");
        if !self.file_systems.contains_key(&fsid) {
            return fs_not_found(&fsid);
        }
        let subnet_id = param_str(params, "SubnetId");
        if subnet_id.is_empty() {
            return crate::proto::json_error(
                404,
                "SubnetNotFound",
                "The subnetId provided does not exist in your VPC.",
            );
        }
        // moto validates the subnet + security groups against its EC2 store
        // and picks the IP from the subnet range; the gateway serves EC2 via
        // the sidecar (no native store to consult), so both are taken as
        // given and a plausible private IP is synthesized instead.
        self.mount_target_ips += 1;
        let octet = self.mount_target_ips % 254 + 1;
        let ip_address = params
            .get("IpAddress")
            .and_then(Value::as_str)
            .unwrap_or(&format!("10.0.1.{octet}"))
            .to_string();
        let mount_target_id = format!("fsmt-{}", new_hex(13));
        let security_groups = null_if_absent(params.get("SecurityGroups").cloned());
        self.mount_targets.insert(
            mount_target_id.clone(),
            MountTarget {
                file_system_id: fsid.clone(),
                subnet_id: subnet_id.clone(),
                ip_address: ip_address.clone(),
                security_groups: security_groups.clone(),
                mount_target_id: mount_target_id.clone(),
            },
        );
        Response::json_status(
            200,
            &json!({
                "FileSystemId": fsid,
                "SubnetId": subnet_id,
                "IpAddress": ip_address,
                "MountTargetId": mount_target_id,
                "SecurityGroups": security_groups.unwrap_or(json!([])),
                "OwnerId": self.account,
                "LifeCycleState": "available",
                "VpcId": Value::Null,
                "NetworkInterfaceId": Value::Null,
                "AvailabilityZoneId": Value::Null,
                "AvailabilityZoneName": Value::Null,
            }),
        )
    }

    fn mount_targets_for(&self, fsid: &str) -> Vec<&MountTarget> {
        self.mount_targets
            .values()
            .filter(|mt| mt.file_system_id == fsid)
            .collect()
    }

    fn describe_mount_targets(&self, params: &Value) -> Response {
        let fsid = param_str(params, "FileSystemId");
        let mtid = param_str(params, "MountTargetId");
        let apid = param_str(params, "AccessPointId");
        // AccessPointId → its file system's targets; else FileSystemId; else
        // MountTargetId; moto requires exactly one of the three.
        if let Some(ap_id) = Some(apid.clone()).filter(|v| !v.is_empty()) {
            let Some(ap) = self.access_points.get(&ap_id) else {
                if !fsid.is_empty() && !self.file_systems.contains_key(&fsid) {
                    return fs_not_found(&fsid);
                }
                return access_point_not_found(&ap_id);
            };
            let targets: Vec<Value> = self
                .mount_targets_for(&ap.file_system_id)
                .into_iter()
                .map(|mt| self.mount_target_json(mt))
                .collect();
            return Response::json_status(200, &json!({"MountTargets": targets}));
        }
        if !fsid.is_empty() {
            if !self.file_systems.contains_key(&fsid) {
                return fs_not_found(&fsid);
            }
            let targets: Vec<Value> = self
                .mount_targets_for(&fsid)
                .into_iter()
                .map(|mt| self.mount_target_json(mt))
                .collect();
            return Response::json_status(200, &json!({"MountTargets": targets}));
        }
        if !mtid.is_empty() {
            return match self.mount_targets.get(&mtid) {
                Some(mt) => Response::json_status(
                    200,
                    &json!({"MountTargets": [self.mount_target_json(mt)]}),
                ),
                None => mount_target_not_found(&mtid),
            };
        }
        crate::proto::json_error(
            400,
            "BadRequest",
            "Must specify exactly one mutually exclusive parameter.",
        )
    }

    fn mount_target_json(&self, mt: &MountTarget) -> Value {
        json!({
            "FileSystemId": mt.file_system_id,
            "SubnetId": mt.subnet_id,
            "IpAddress": mt.ip_address,
            "MountTargetId": mt.mount_target_id,
            "SecurityGroups": mt.security_groups.clone().unwrap_or(json!(null)),
            "OwnerId": self.account,
            "LifeCycleState": "available",
            "VpcId": Value::Null,
            "NetworkInterfaceId": Value::Null,
            "AvailabilityZoneId": Value::Null,
            "AvailabilityZoneName": Value::Null,
        })
    }

    fn delete_mount_target(&mut self, id: &str) -> Response {
        match self.mount_targets.remove(id) {
            Some(_) => Response::json_status(204, &json!({})),
            None => mount_target_not_found(id),
        }
    }

    fn describe_mount_target_security_groups(&self, id: &str) -> Response {
        match self.mount_targets.get(id) {
            Some(mt) => Response::json_status(
                200,
                &json!({"SecurityGroups": mt.security_groups.clone().unwrap_or(json!(null))}),
            ),
            None => mount_target_not_found(id),
        }
    }

    fn modify_mount_target_security_groups(&mut self, id: &str, params: &Value) -> Response {
        match self.mount_targets.get_mut(id) {
            Some(mt) => {
                mt.security_groups = null_if_absent(params.get("SecurityGroups").cloned());
                Response::json_status(204, &json!({}))
            }
            None => mount_target_not_found(id),
        }
    }

    // ---- access points -------------------------------------------------------

    fn create_access_point(&mut self, params: &Value) -> Response {
        let fsid = param_str(params, "FileSystemId");
        if !self.file_systems.contains_key(&fsid) {
            return fs_not_found(&fsid);
        }
        let access_point_id = format!("fsap-{}", new_hex(8));
        let access_point_arn = format!(
            "arn:aws:elasticfilesystem:{region}:{account}:access-point/{access_point_id}",
            region = self.region,
            account = self.account
        );
        let tags = params.get("Tags").cloned().unwrap_or(json!([]));
        self.tag_add(&access_point_id, &tags);
        let posix_user = params.get("PosixUser").cloned().unwrap_or(json!({}));
        let root_directory = null_if_absent(params.get("RootDirectory").cloned())
            .unwrap_or_else(|| json!({"Path": "/"}));
        let client_token = null_if_absent(params.get("ClientToken").cloned());
        self.access_points.insert(
            access_point_id.clone(),
            AccessPoint {
                access_point_id: access_point_id.clone(),
                access_point_arn: access_point_arn.clone(),
                client_token: client_token.clone(),
                file_system_id: fsid.clone(),
                posix_user: posix_user.clone(),
                root_directory: root_directory.clone(),
            },
        );
        let tags_stored = self.tags_of(&access_point_id);
        Response::json_status(
            200,
            &json!({
                "ClientToken": client_token.unwrap_or(Value::Null),
                "Name": name_of(&tags_stored),
                "Tags": tags_json(&tags_stored),
                "AccessPointId": access_point_id,
                "AccessPointArn": access_point_arn,
                "FileSystemId": fsid,
                "PosixUser": posix_user,
                "RootDirectory": root_directory,
                "OwnerId": self.account,
                "LifeCycleState": "available",
            }),
        )
    }

    fn describe_access_points(&self, params: &Value) -> Response {
        let apid = param_str(params, "AccessPointId");
        let fsid = param_str(params, "FileSystemId");
        let out: Vec<Value> = if !apid.is_empty() {
            match self
                .access_points
                .values()
                .find(|ap| ap.access_point_id == apid)
            {
                Some(ap) => vec![self.access_point_json(ap)],
                None => return access_point_not_found(&apid),
            }
        } else if !fsid.is_empty() {
            self.access_points
                .values()
                .filter(|ap| ap.file_system_id == fsid)
                .map(|ap| self.access_point_json(ap))
                .collect()
        } else {
            self.access_points
                .values()
                .map(|ap| self.access_point_json(ap))
                .collect()
        };
        Response::json_status(200, &json!({"AccessPoints": out}))
    }

    fn access_point_json(&self, ap: &AccessPoint) -> Value {
        let tags = self.tags_of(&ap.access_point_id);
        let name = name_of(&tags);
        json!({
            "ClientToken": ap.client_token.clone().unwrap_or(Value::Null),
            "Name": if name.is_empty() { Value::Null } else { json!(name) },
            "Tags": tags_json(&tags),
            "AccessPointId": ap.access_point_id,
            "AccessPointArn": ap.access_point_arn,
            "FileSystemId": ap.file_system_id,
            "PosixUser": ap.posix_user,
            "RootDirectory": ap.root_directory,
            "OwnerId": self.account,
            "LifeCycleState": "available",
        })
    }

    fn delete_access_point(&mut self, id: &str) -> Response {
        self.access_points.remove(id);
        self.drop_resource_tags(id);
        Response::json_status(204, &json!({}))
    }

    // ---- tags ---------------------------------------------------------------

    fn list_tags_for_resource(&self, id: &str) -> Response {
        Response::json_status(200, &json!({"Tags": tags_json(&self.tags_of(id))}))
    }

    fn tag_resource(&mut self, id: &str, params: &Value) -> Response {
        self.tag_add(id, params.get("Tags").unwrap_or(&json!(null)));
        Response::json_status(200, &json!({}))
    }

    fn untag_resource(&mut self, id: &str, keys: &[String]) -> Response {
        self.tag_remove(id, keys);
        Response::json_status(200, &json!({}))
    }

    fn create_tags(&mut self, id: &str, params: &Value) -> Response {
        if !self.file_systems.contains_key(id) {
            return fs_not_found(id);
        }
        self.tag_add(id, params.get("Tags").unwrap_or(&json!(null)));
        Response::json_status(204, &json!({}))
    }

    fn delete_tags(&mut self, id: &str, params: &Value) -> Response {
        if !self.file_systems.contains_key(id) {
            return fs_not_found(id);
        }
        let keys: Vec<String> = params
            .get("TagKeys")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(String::from)
                    .collect()
            })
            .unwrap_or_default();
        self.tag_remove(id, &keys);
        Response::json_status(204, &json!({}))
    }

    fn describe_tags(&self, id: &str) -> Response {
        if !self.file_systems.contains_key(id) {
            return fs_not_found(id);
        }
        Response::json_status(200, &json!({"Tags": tags_json(&self.tags_of(id))}))
    }

    // ---- policies ------------------------------------------------------------

    fn put_lifecycle_configuration(&mut self, id: &str, params: &Value) -> Response {
        let Some(fs) = self.file_systems.get_mut(id) else {
            return fs_not_found(id);
        };
        fs.lifecycle_policies =
            null_if_absent(params.get("LifecyclePolicies").cloned()).unwrap_or(json!(null));
        let policies = fs.lifecycle_policies.clone();
        Response::json_status(200, &json!({"LifecyclePolicies": policies}))
    }

    fn describe_lifecycle_configuration(&self, id: &str) -> Response {
        match self.file_systems.get(id) {
            Some(fs) => {
                Response::json_status(200, &json!({"LifecyclePolicies": fs.lifecycle_policies}))
            }
            None => fs_not_found(id),
        }
    }

    fn describe_backup_policy(&self, id: &str) -> Response {
        match self.file_systems.get(id) {
            None => fs_not_found(id),
            Some(fs) if fs.backup => {
                Response::json_status(200, &json!({"BackupPolicy": {"Status": "ENABLED"}}))
            }
            Some(_) => policy_not_found(),
        }
    }

    fn put_backup_policy(&mut self, id: &str, params: &Value) -> Response {
        let Some(fs) = self.file_systems.get_mut(id) else {
            return fs_not_found(id);
        };
        let status = params
            .get("BackupPolicy")
            .and_then(|b| b.get("Status"))
            .and_then(Value::as_str)
            .unwrap_or("ENABLED")
            .to_string();
        fs.backup = status == "ENABLED";
        Response::json_status(200, &json!({"BackupPolicy": {"Status": status}}))
    }

    fn describe_file_system_policy(&self, id: &str) -> Response {
        match self.file_systems.get(id) {
            None => fs_not_found(id),
            Some(fs) => match &fs.file_system_policy {
                Some(policy) => {
                    Response::json_status(200, &json!({"FileSystemId": id, "Policy": policy}))
                }
                None => policy_not_found(),
            },
        }
    }

    fn put_file_system_policy(&mut self, id: &str, params: &Value) -> Response {
        let Some(fs) = self.file_systems.get_mut(id) else {
            return fs_not_found(id);
        };
        let policy = param_str(params, "Policy");
        fs.file_system_policy = Some(policy.clone());
        Response::json_status(200, &json!({"FileSystemId": id, "Policy": policy}))
    }

    fn delete_file_system_policy(&mut self, id: &str) -> Response {
        let Some(fs) = self.file_systems.get_mut(id) else {
            return fs_not_found(id);
        };
        fs.file_system_policy = None;
        Response::json_status(200, &json!({}))
    }

    fn update_file_system_protection(&mut self, id: &str, params: &Value) -> Response {
        let Some(fs) = self.file_systems.get_mut(id) else {
            return fs_not_found(id);
        };
        if let Some(prot) = params.get("ReplicationOverwriteProtection") {
            fs.file_system_protection = json!({"ReplicationOverwriteProtection": prot});
        }
        Response::json_status(200, &json!(fs.file_system_protection.clone()))
    }

    // ---- account preferences --------------------------------------------------

    fn describe_account_preferences(&self) -> Response {
        Response::json_status(
            200,
            &json!({"ResourceIdPreference": {
                "ResourceIdType": "LONG_ID",
                "Resources": ["FILE_SYSTEM", "MOUNT_TARGET"],
            }}),
        )
    }

    fn put_account_preferences(&self, params: &Value) -> Response {
        let id_type = params
            .get("ResourceIdType")
            .and_then(Value::as_str)
            .unwrap_or("SHORT_ID");
        Response::json_status(
            200,
            &json!({"ResourceIdPreference": {
                "ResourceIdType": id_type,
                "Resources": ["FILE_SYSTEM", "MOUNT_TARGET"],
            }}),
        )
    }

    // ---- replication ------------------------------------------------------------

    fn describe_replication_configurations(&self, params: &Value) -> Response {
        let filter = param_str(params, "FileSystemId");
        let mut results = Vec::new();
        for fs in self.file_systems.values() {
            if !filter.is_empty() && fs.file_system_id != filter {
                continue;
            }
            if let Some(replication) = &fs.replication_configuration {
                results.push(replication.clone());
            }
        }
        Response::json_status(200, &json!({"Replications": results}))
    }

    fn create_replication_configuration(&mut self, src: &str, params: &Value) -> Response {
        let Some(fs) = self.file_systems.get_mut(src) else {
            return fs_not_found(src);
        };
        if fs.replication_configuration.is_some() {
            return crate::proto::json_error(
                409,
                "ReplicationAlreadyExists",
                &format!("Replication for {src} already exists."),
            );
        }
        let mut destinations = Vec::new();
        for dest in params
            .get("Destinations")
            .and_then(Value::as_array)
            .unwrap_or(&vec![])
        {
            destinations.push(json!({
                "FileSystemId": null_if_absent(dest.get("FileSystemId").cloned())
                    .unwrap_or(json!(format!("fs-{}", new_hex(11)))),
                "Region": dest.get("Region").cloned().unwrap_or(json!(self.region.clone())),
                "Status": "ENABLED",
                "LastReplicatedTimestamp": now_unix(),
                "OwnerId": self.account,
            }));
        }
        let arn = fs.file_system_arn.clone();
        let config = json!({
            "SourceFileSystemId": src,
            "SourceFileSystemRegion": self.region,
            "SourceFileSystemArn": arn.clone(),
            "OriginalSourceFileSystemArn": arn,
            "CreationTime": now_unix(),
            "Destinations": destinations,
            "SourceFileSystemOwnerId": self.account,
        });
        if let Some(fs) = self.file_systems.get_mut(src) {
            fs.replication_configuration = Some(config.clone());
        }
        Response::json_status(200, &config)
    }

    fn delete_replication_configuration(&mut self, src: &str) -> Response {
        match self.file_systems.get_mut(src) {
            None => fs_not_found(src),
            Some(fs) => {
                if fs.replication_configuration.take().is_none() {
                    return crate::proto::json_error(
                        404,
                        "ReplicationNotFound",
                        &format!("Replication for {src} does not exist."),
                    );
                }
                Response::json_status(204, &json!({}))
            }
        }
    }
}

fn param_str(params: &Value, key: &str) -> String {
    params
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::HashMap;

    // The state slot is process-global and tests run in parallel, so the
    // account id lives per-thread (each #[test] owns its thread).
    thread_local! {
        static TEST_ACCOUNT: std::cell::Cell<u64> = const { std::cell::Cell::new(123456789041) };
    }

    fn set_account(a: u64) {
        TEST_ACCOUNT.with(|t| t.set(a));
    }

    fn req(method: &str, path: &str, body: Value) -> Request {
        let (path, query_string) = match path.split_once('?') {
            Some((p, q)) => (p.to_string(), q.to_string()),
            None => (path.to_string(), String::new()),
        };
        Request {
            service: "efs".into(),
            operation: String::new(),
            method: method.into(),
            path,
            query_string,
            headers: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            account: TEST_ACCOUNT.with(|t| t.get()),
            region: "us-east-1".into(),
        }
    }

    fn json_body(resp: Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap()
    }

    #[test]
    fn file_system_lifecycle() {
        set_account(123456789042);
        let created = json_body(dispatch(&req(
            "POST",
            "/2015-02-01/file-systems",
            json!({
                "CreationToken": "tok-1",
                "Encrypted": true,
                "Tags": [{"Key": "Name", "Value": "tagged-fs"}],
            }),
        )));
        let fsid = created["FileSystemId"].as_str().unwrap().to_string();
        assert!(fsid.starts_with("fs-"));
        assert_eq!(created["Name"], "tagged-fs");
        assert_eq!(created["Encrypted"], true);
        assert_eq!(created["CreationToken"], "tok-1");
        assert_eq!(created["LifeCycleState"], "available");
        assert!(created["FileSystemArn"]
            .as_str()
            .unwrap()
            .starts_with("arn:aws:elasticfilesystem:"));
        assert!(created["SizeInBytes"].is_object());
        assert!(created["PerformanceMode"].is_string());

        let one = dispatch(&req(
            "GET",
            &format!("/2015-02-01/file-systems?FileSystemId={fsid}"),
            json!({}),
        ));
        assert_eq!(
            one.status,
            200,
            "describe body: {}",
            String::from_utf8_lossy(&one.body)
        );
        let one = json_body(one);
        assert_eq!(one["FileSystems"].as_array().unwrap().len(), 1);

        assert_eq!(
            dispatch(&req(
                "DELETE",
                &format!("/2015-02-01/file-systems/{fsid}"),
                json!({})
            ))
            .status,
            204
        );
        let miss = dispatch(&req(
            "DELETE",
            &format!("/2015-02-01/file-systems/{fsid}"),
            json!({}),
        ));
        assert_eq!(miss.status, 404);
        let err: Value = serde_json::from_slice(&miss.body).unwrap();
        assert_eq!(err["__type"], "FileSystemNotFound");
    }

    #[test]
    fn policies_and_lifecycle() {
        set_account(123456789043);
        dispatch(&req(
            "POST",
            "/2015-02-01/file-systems",
            json!({"CreationToken": "tok-2"}),
        ));
        let listed = json_body(dispatch(&req("GET", "/2015-02-01/file-systems", json!({}))));
        let fsid = listed["FileSystems"][0]["FileSystemId"]
            .as_str()
            .unwrap()
            .to_string();
        let prefix = format!("/2015-02-01/file-systems/{fsid}");

        let policy = json!({
            "Policy": r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*"}]}"#,
        });
        let put = dispatch(&req("PUT", &format!("{prefix}/policy"), policy));
        assert_eq!(put.status, 200);
        let desc = json_body(dispatch(&req(
            "GET",
            &format!("{prefix}/policy"),
            json!({}),
        )));
        assert!(desc["Policy"].as_str().unwrap().contains("2012-10-17"));
        assert_eq!(
            dispatch(&req("DELETE", &format!("{prefix}/policy"), json!({}))).status,
            200
        );
        let empty = dispatch(&req("GET", &format!("{prefix}/policy"), json!({})));
        let err: Value = serde_json::from_slice(&empty.body).unwrap();
        assert_eq!(err["__type"], "PolicyNotFound");

        let lc = json_body(dispatch(&req(
            "PUT",
            &format!("{prefix}/lifecycle-configuration"),
            json!({"LifecyclePolicies": [{"TransitionToIA": "AFTER_30_DAYS"}]}),
        )));
        assert_eq!(
            lc["LifecyclePolicies"][0]["TransitionToIA"],
            "AFTER_30_DAYS"
        );
        let got = json_body(dispatch(&req(
            "GET",
            &format!("{prefix}/lifecycle-configuration"),
            json!({}),
        )));
        assert_eq!(
            got["LifecyclePolicies"][0]["TransitionToIA"],
            "AFTER_60_DAYS".replace("60", "30")
        );

        assert_eq!(
            dispatch(&req("GET", &format!("{prefix}/backup-policy"), json!({}))).status,
            404
        );
        let backup = json_body(dispatch(&req(
            "PUT",
            &format!("{prefix}/backup-policy"),
            json!({"BackupPolicy": {"Status": "ENABLED"}}),
        )));
        assert_eq!(backup["BackupPolicy"]["Status"], "ENABLED");
        let described = json_body(dispatch(&req(
            "GET",
            &format!("{prefix}/backup-policy"),
            json!({}),
        )));
        assert_eq!(described["BackupPolicy"]["Status"], "ENABLED");
    }

    #[test]
    fn access_points_and_tags() {
        set_account(123456789044);
        dispatch(&req(
            "POST",
            "/2015-02-01/file-systems",
            json!({"CreationToken": "tok-3"}),
        ));
        let fsid = json_body(dispatch(&req("GET", "/2015-02-01/file-systems", json!({}))))
            ["FileSystems"][0]["FileSystemId"]
            .as_str()
            .unwrap()
            .to_string();

        let ap = json_body(dispatch(&req(
            "POST",
            "/2015-02-01/access-points",
            json!({
                "ClientToken": "ctok",
                "FileSystemId": fsid,
                "PosixUser": {"Uid": 1000, "Gid": 1000},
            }),
        )));
        let apid = ap["AccessPointId"].as_str().unwrap().to_string();
        assert!(apid.starts_with("fsap-"));
        assert_eq!(ap["PosixUser"]["Uid"], 1000);
        assert_eq!(ap["RootDirectory"]["Path"], "/");

        let matched = json_body(dispatch(&req(
            "GET",
            &format!("/2015-02-01/access-points?FileSystemId={fsid}"),
            json!({}),
        )));
        assert_eq!(matched["AccessPoints"].as_array().unwrap().len(), 1);
        assert_eq!(
            dispatch(&req(
                "DELETE",
                &format!("/2015-02-01/access-points/{apid}"),
                json!({})
            ))
            .status,
            204
        );

        dispatch(&req(
            "POST",
            &format!("/2015-02-01/resource-tags/{fsid}"),
            json!({"Tags": [{"Key": "Env", "Value": "prod"}]}),
        ));
        let tags = json_body(dispatch(&req(
            "GET",
            &format!("/2015-02-01/resource-tags/{fsid}"),
            json!({}),
        )));
        assert_eq!(tags["Tags"][0]["Key"], "Env");

        let describe = json_body(dispatch(&req(
            "GET",
            &format!("/2015-02-01/tags/{fsid}"),
            json!({}),
        )));
        assert_eq!(describe["Tags"][0]["Value"], "prod");
    }

    #[test]
    fn mount_targets_record_subnet_and_sg() {
        set_account(123456789045);
        dispatch(&req(
            "POST",
            "/2015-02-01/file-systems",
            json!({"CreationToken": "tok-4"}),
        ));
        let fsid = json_body(dispatch(&req("GET", "/2015-02-01/file-systems", json!({}))))
            ["FileSystems"][0]["FileSystemId"]
            .as_str()
            .unwrap()
            .to_string();

        let mt = json_body(dispatch(&req(
            "POST",
            "/2015-02-01/mount-targets",
            json!({
                "FileSystemId": fsid,
                "SubnetId": "subnet-1234",
                "SecurityGroups": ["sg-1"],
            }),
        )));
        let mtid = mt["MountTargetId"].as_str().unwrap().to_string();
        assert!(mtid.starts_with("fsmt-"));
        assert_eq!(mt["SubnetId"], "subnet-1234");
        assert!(mt["IpAddress"].is_string());
        assert_eq!(mt["FileSystemId"], fsid);

        dispatch(&req(
            "PUT",
            &format!("/2015-02-01/mount-targets/{mtid}/security-groups"),
            json!({"SecurityGroups": ["sg-2", "sg-3"]}),
        ));
        let sgs = json_body(dispatch(&req(
            "GET",
            &format!("/2015-02-01/mount-targets/{mtid}/security-groups"),
            json!({}),
        )));
        assert_eq!(sgs["SecurityGroups"][0], "sg-2");
        let listed = json_body(dispatch(&req(
            "GET",
            &format!("/2015-02-01/mount-targets?FileSystemId={fsid}"),
            json!({}),
        )));
        assert_eq!(listed["MountTargets"].as_array().unwrap().len(), 1);
        assert_eq!(
            dispatch(&req(
                "DELETE",
                &format!("/2015-02-01/mount-targets/{mtid}"),
                json!({})
            ))
            .status,
            204
        );
    }

    #[test]
    fn account_preferences_and_replication() {
        set_account(123456789046);
        let prefs = json_body(dispatch(&req(
            "GET",
            "/2015-02-01/account-preferences",
            json!({}),
        )));
        assert_eq!(prefs["ResourceIdPreference"]["ResourceIdType"], "LONG_ID");
        let put = json_body(dispatch(&req(
            "PUT",
            "/2015-02-01/account-preferences",
            json!({"ResourceIdType": "SHORT_ID"}),
        )));
        assert_eq!(put["ResourceIdPreference"]["ResourceIdType"], "SHORT_ID");

        let miss = dispatch(&req(
            "DELETE",
            "/2015-02-01/file-systems/fs-nonexistent/replication-configuration",
            json!({}),
        ));
        assert_eq!(miss.status, 404);
        let prot = dispatch(&req(
            "PUT",
            "/2015-02-01/file-systems/fs-nonexistent/protection",
            json!({"ReplicationOverwriteProtection": "ENABLED"}),
        ));
        assert_eq!(prot.status, 404);
    }
}
