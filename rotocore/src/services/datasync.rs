//! DataSync backend (JSON protocol, X-Amz-Target dispatched).
//!
//! Port of moto's datasync: agents, typed locations (S3/SMB/EFS/NFS/HDFS/
//! ObjectStorage/AzureBlob/FsxLustre/FsxOntap/FsxOpenZfs/FsxWindows),
//! tasks and the task-execution status machine. ARNs carry moto's fixed
//! 111222333444 account segment. Ops moto does not route (Tag*,
//! ListTaskExecutions, UpdateLocation*, UpdateTaskExecution) dispatch
//! None exactly like the sidecar's NotImplemented fall-through — they
//! are recorded baseline failures.

use crate::proto::{json_error, parse_json_body};
use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Map, Value};

#[derive(Default)]
struct Exec {
    task_arn: String,
    status: &'static str,
}

#[derive(Default)]
struct Task {
    source: String,
    destination: String,
    name: String,
    status: &'static str,
    current_execution: Option<String>,
    metadata: Map<String, Value>,
}

struct Location {
    uri: String,
    typ: &'static str,
    /// per-type describe members (everything except LocationArn/LocationUri)
    members: Map<String, Value>,
}

#[derive(Default)]
struct State {
    arn_counter: u64,
    agents: Vec<(String, Value)>,
    locations: Vec<(String, Location)>,
    tasks: Vec<(String, Task)>,
    task_executions: Vec<(String, Exec)>,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("datasync"));
    SLOT.clone()
}

/// moto hardcodes the fake account segment in datasync ARNs.
const ARN_ACCOUNT: &str = "111222333444";

/// Intermediate states -> SUCCESS progression (TaskExecution model).
const STATES: [&str; 4] = ["INITIALIZING", "PREPARING", "TRANSFERRING", "VERIFYING"];

fn partition(region: &str) -> &'static str {
    let prefixes = [
        ("cn-", "aws-cn"),
        ("us-gov-", "aws-us-gov"),
        ("us-iso-", "aws-iso"),
        ("us-isob-", "aws-iso-b"),
        ("eusc-", "aws-eusc"),
    ];
    for (prefix, name) in prefixes {
        if region.starts_with(prefix) {
            return name;
        }
    }
    "aws"
}

fn zfill(counter: u64) -> String {
    format!("{counter:017}")
}

fn arn_of(region: &str, kind: &str, label: &str, counter: u64) -> String {
    format!(
        "arn:{}:datasync:{region}:{ARN_ACCOUNT}:{kind}/{label}-{}",
        partition(region),
        zfill(counter)
    )
}

fn invalid_request(msg: &str) -> Response {
    json_error(400, "InvalidRequestException", msg)
}

fn ok_json(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "datasync" {
        return None;
    }
    let op = req.operation.as_str();
    let p = parse_json_body(&req.body);

    let slot = state();
    let mut st = slot.write();
    let s = st.entry((req.account, req.region.clone())).or_default();
    let region = req.region.clone();

    // ---- agent ops ----
    match op {
        "CreateAgent" => {
            s.arn_counter += 1;
            let arn = arn_of(&region, "agent", "agent", s.arn_counter);
            s.agents.push((
                arn.clone(),
                json!({"name": p["AgentName"].clone(), "status": "ONLINE"}),
            ));
            return Some(ok_json(&json!({ "AgentArn": arn })));
        }
        "DescribeAgent" => {
            let arn = p["AgentArn"].as_str().unwrap_or_default();
            return Some(match s.agents.iter().find(|(k, _)| k == arn) {
                Some((k, v)) => ok_json(&json!({
                    "AgentArn": k,
                    "Name": v["name"],
                    "Status": v["status"],
                })),
                None => invalid_request(&format!("Agent {arn} is not found.")),
            });
        }
        "ListAgents" => {
            let agents: Vec<Value> = s
                .agents
                .iter()
                .map(|(arn, v)| {
                    json!({"AgentArn": arn, "Name": v["name"].clone(), "Status": v["status"].clone()})
                })
                .collect();
            return Some(ok_json(&json!({ "Agents": agents })));
        }
        "UpdateAgent" => {
            let arn = p["AgentArn"].as_str().unwrap_or_default();
            return Some(match s.agents.iter_mut().find(|(k, _)| k == arn) {
                Some((_, v)) => {
                    if let Some(n) = p["Name"].as_str() {
                        v["name"] = json!(n);
                    }
                    ok_json(&json!({}))
                }
                None => invalid_request(&format!("Agent {arn} is not found.")),
            });
        }
        "DeleteAgent" => {
            let arn = p["AgentArn"].as_str().unwrap_or_default();
            if !s.agents.iter().any(|(k, _)| k == arn) {
                return Some(invalid_request(&format!("Agent {arn} is not found.")));
            }
            s.agents.retain(|(k, _)| k != arn);
            return Some(ok_json(&json!({})));
        }
        "ListLocations" => {
            let locations: Vec<Value> = s
                .locations
                .iter()
                .map(|(arn, loc)| json!({"LocationArn": arn, "LocationUri": loc.uri}))
                .collect();
            return Some(ok_json(&json!({ "Locations": locations })));
        }
        "DeleteLocation" => {
            let arn = p["LocationArn"].as_str().unwrap_or_default();
            if !s.locations.iter().any(|(k, _)| k == arn) {
                return Some(invalid_request("The request is not valid."));
            }
            s.locations.retain(|(k, _)| k != arn);
            return Some(ok_json(&json!({})));
        }
        "CreateTask" => {
            let src = p["SourceLocationArn"].as_str().unwrap_or_default();
            let dst = p["DestinationLocationArn"].as_str().unwrap_or_default();
            if !s.locations.iter().any(|(k, _)| k == src) {
                return Some(invalid_request(&format!("Location {src} not found.")));
            }
            if !s.locations.iter().any(|(k, _)| k == dst) {
                return Some(invalid_request(&format!("Location {dst} not found.")));
            }
            s.arn_counter += 1;
            let arn = arn_of(&region, "task", "task", s.arn_counter);
            let mut metadata = Map::new();
            for k in ["CloudWatchLogGroupArn", "Options", "Excludes", "Tags"] {
                metadata.insert(
                    k.to_string(),
                    p.get(k)
                        .filter(|v| !v.is_null())
                        .cloned()
                        .unwrap_or(Value::Null),
                );
            }
            s.tasks.push((
                arn.clone(),
                Task {
                    source: src.to_string(),
                    destination: dst.to_string(),
                    name: p["Name"].as_str().unwrap_or_default().to_string(),
                    status: "AVAILABLE",
                    current_execution: None,
                    metadata,
                },
            ));
            return Some(ok_json(&json!({ "TaskArn": arn })));
        }
        "DescribeTask" => {
            let arn = p["TaskArn"].as_str().unwrap_or_default();
            return Some(match s.tasks.iter().find(|(k, _)| k == arn) {
                Some((k, t)) => ok_json(&json!({
                    "TaskArn": k,
                    "Status": t.status,
                    "Name": t.name.clone(),
                    "CurrentTaskExecutionArn": t.current_execution.clone(),
                    "SourceLocationArn": t.source.clone(),
                    "DestinationLocationArn": t.destination.clone(),
                    "CloudWatchLogGroupArn": t.metadata.get("CloudWatchLogGroupArn").cloned().unwrap_or(Value::Null),
                    "Options": t.metadata.get("Options").cloned().unwrap_or(Value::Null),
                    "Excludes": t.metadata.get("Excludes").cloned().unwrap_or(Value::Null),
                })),
                None => invalid_request(&format!("Sync task {arn} is not found.")),
            });
        }
        "ListTasks" => {
            let tasks: Vec<Value> = s
                .tasks
                .iter()
                .map(|(arn, t)| json!({"Name": t.name.clone(), "Status": t.status, "TaskArn": arn}))
                .collect();
            return Some(ok_json(&json!({ "Tasks": tasks })));
        }
        "UpdateTask" => {
            let arn = p["TaskArn"].as_str().unwrap_or_default();
            return Some(match s.tasks.iter_mut().find(|(k, _)| k == arn) {
                Some((_, t)) => {
                    t.name = p["Name"].as_str().unwrap_or_default().to_string();
                    let mut metadata = Map::new();
                    for k in ["CloudWatchLogGroupArn", "Options", "Excludes", "Tags"] {
                        metadata.insert(
                            k.to_string(),
                            p.get(k)
                                .filter(|v| !v.is_null())
                                .cloned()
                                .unwrap_or(Value::Null),
                        );
                    }
                    t.metadata = metadata;
                    ok_json(&json!({}))
                }
                None => invalid_request(&format!("Sync task {arn} is not found.")),
            });
        }
        "DeleteTask" => {
            let arn = p["TaskArn"].as_str().unwrap_or_default();
            if !s.tasks.iter().any(|(k, _)| k == arn) {
                return Some(invalid_request("The request is not valid."));
            }
            s.tasks.retain(|(k, _)| k != arn);
            return Some(ok_json(&json!({})));
        }
        "StartTaskExecution" => {
            let arn = p["TaskArn"].as_str().unwrap_or_default();
            s.arn_counter += 1;
            let Some((_, task)) = s.tasks.iter_mut().find(|(k, _)| k == arn) else {
                return Some(invalid_request("Invalid request."));
            };
            if task.status != "AVAILABLE" {
                return Some(invalid_request("Invalid request."));
            }
            let exec_arn = format!("{arn}/execution/exec-{}", zfill(s.arn_counter));
            s.task_executions.push((
                exec_arn.clone(),
                Exec {
                    task_arn: arn.to_string(),
                    status: "INITIALIZING",
                },
            ));
            task.current_execution = Some(exec_arn.clone());
            task.status = "RUNNING";
            return Some(ok_json(&json!({ "TaskExecutionArn": exec_arn })));
        }
        "CancelTaskExecution" => {
            let arn = p["TaskExecutionArn"].as_str().unwrap_or_default();
            let Some((_, exec)) = s.task_executions.iter_mut().find(|(k, _)| k == arn) else {
                return Some(invalid_request(&format!("Sync task {arn} is not found.")));
            };
            if !STATES.contains(&exec.status) {
                return Some(invalid_request(&format!(
                    "Sync task cannot be cancelled in its current status: {}",
                    exec.status
                )));
            }
            let task_arn = exec.task_arn.clone();
            exec.status = "ERROR";
            if let Some((_, t)) = s.tasks.iter_mut().find(|(k, _)| *k == task_arn) {
                t.current_execution = None;
                t.status = "AVAILABLE";
            }
            return Some(ok_json(&json!({})));
        }
        "DescribeTaskExecution" => {
            let arn = p["TaskExecutionArn"].as_str().unwrap_or_default();
            let Some((_, exec)) = s.task_executions.iter_mut().find(|(k, _)| k == arn) else {
                return Some(invalid_request("The request is not valid."));
            };
            let status = exec.status;
            if status == "SUCCESS" {
                if let Some((_, t)) = s.tasks.iter_mut().find(|(a, _)| *a == exec.task_arn) {
                    t.status = "AVAILABLE";
                }
            } else if let Some(i) = STATES.iter().position(|x| *x == status) {
                exec.status = STATES
                    .iter()
                    .skip(i + 1)
                    .copied()
                    .next()
                    .unwrap_or("SUCCESS");
            }
            return Some(ok_json(&json!({"TaskExecutionArn": arn, "Status": status})));
        }
        _ => {}
    }

    // ---- location describe pairs ----
    match op {
        "DescribeLocationS3" => return Some(describe_location(s, &p["LocationArn"], "S3")),
        "DescribeLocationSmb" => return Some(describe_location(s, &p["LocationArn"], "SMB")),
        "DescribeLocationEfs" => return Some(describe_location(s, &p["LocationArn"], "EFS")),
        "DescribeLocationNfs" => return Some(describe_location(s, &p["LocationArn"], "NFS")),
        "DescribeLocationHdfs" => return Some(describe_location(s, &p["LocationArn"], "HDFS")),
        "DescribeLocationObjectStorage" => {
            return Some(describe_location(s, &p["LocationArn"], "ObjectStorage"))
        }
        "DescribeLocationAzureBlob" => {
            return Some(describe_location(s, &p["LocationArn"], "AzureBlob"))
        }
        "DescribeLocationFsxLustre" => {
            return Some(describe_location(s, &p["LocationArn"], "FsxLustre"))
        }
        "DescribeLocationFsxOntap" => {
            return Some(describe_location(s, &p["LocationArn"], "FsxOntap"))
        }
        "DescribeLocationFsxOpenZfs" => {
            return Some(describe_location(s, &p["LocationArn"], "FsxOpenZfs"))
        }
        "DescribeLocationFsxWindows" => {
            return Some(describe_location(s, &p["LocationArn"], "FsxWindows"))
        }
        _ => {}
    }

    // ---- location create pairs ----
    match op {
        "CreateLocationS3" => {
            let bucket = p["S3BucketArn"]
                .as_str()
                .unwrap_or_default()
                .split(':')
                .next_back()
                .unwrap_or_default();
            let mut els = vec!["s3:/".to_string(), bucket.to_string()];
            if let Some(v) = p.get("Subdirectory").filter(|v| !v.is_null()) {
                els.push(v.as_str().unwrap_or_default().to_string());
            }
            let uri = els.join("/");
            let mut members = Map::new();
            members.insert("S3Config".into(), p["S3Config"].clone());
            Some(new_location(s, &region, uri, "S3", members))
        }
        "CreateLocationSmb" => {
            let uri = [
                "smb:/".to_string(),
                p["ServerHostname"].as_str().unwrap_or_default().to_string(),
                p["Subdirectory"].as_str().unwrap_or_default().to_string(),
            ]
            .join("/");
            let mut members = Map::new();
            for k in ["AgentArns", "User", "Domain", "MountOptions"] {
                members.insert(k.into(), p.get(k).cloned().unwrap_or(Value::Null));
            }
            Some(new_location(s, &region, uri, "SMB", members))
        }
        "CreateLocationEfs" => {
            let fs_arn = p["EfsFilesystemArn"].as_str().unwrap_or_default();
            let sub = p["Subdirectory"].as_str().unwrap_or("/");
            let uri = format!(
                "efs://{}{sub}",
                fs_arn.split(':').next_back().unwrap_or_default()
            );
            let mut members = Map::new();
            members.insert("EfsFilesystemArn".into(), p["EfsFilesystemArn"].clone());
            members.insert("Ec2Config".into(), p["Ec2Config"].clone());
            Some(new_location(s, &region, uri, "EFS", members))
        }
        "CreateLocationNfs" => {
            let host = p["ServerHostname"].as_str().unwrap_or_default();
            let sub = p["Subdirectory"].as_str().unwrap_or_default();
            let uri = format!("nfs://{host}{sub}");
            let mut members = Map::new();
            members.insert("OnPremConfig".into(), p["OnPremConfig"].clone());
            members.insert("MountOptions".into(), p["MountOptions"].clone());
            Some(new_location(s, &region, uri, "NFS", members))
        }
        "CreateLocationHdfs" => {
            let sub = p["Subdirectory"].as_str().unwrap_or("/");
            let host = p["NameNodes"][0]["Hostname"]
                .as_str()
                .unwrap_or("localhost");
            let uri = format!("hdfs://{host}{sub}");
            let mut members = Map::new();
            members.insert("NameNodes".into(), p["NameNodes"].clone());
            members.insert(
                "AuthenticationType".into(),
                if p["AuthenticationType"].is_null() {
                    json!("SIMPLE")
                } else {
                    p["AuthenticationType"].clone()
                },
            );
            members.insert("AgentArns".into(), p["AgentArns"].clone());
            Some(new_location(s, &region, uri, "HDFS", members))
        }
        "CreateLocationObjectStorage" => {
            let host = p["ServerHostname"].as_str().unwrap_or_default();
            let bucket = p["BucketName"].as_str().unwrap_or_default();
            let sub = p["Subdirectory"].as_str().unwrap_or("/");
            let uri = format!("object-storage://{host}/{bucket}{sub}");
            let mut members = Map::new();
            members.insert("ServerHostname".into(), p["ServerHostname"].clone());
            members.insert(
                "ServerPort".into(),
                if p["ServerPort"].is_null() {
                    json!(443)
                } else {
                    p["ServerPort"].clone()
                },
            );
            members.insert(
                "ServerProtocol".into(),
                if p["ServerProtocol"].is_null() {
                    json!("HTTPS")
                } else {
                    p["ServerProtocol"].clone()
                },
            );
            members.insert("BucketName".into(), p["BucketName"].clone());
            members.insert("AgentArns".into(), p["AgentArns"].clone());
            Some(new_location(s, &region, uri, "ObjectStorage", members))
        }
        "CreateLocationAzureBlob" => {
            let container = p["ContainerUrl"].as_str().unwrap_or_default();
            let sub = p["Subdirectory"].as_str().unwrap_or("/");
            let uri = format!("{container}{sub}");
            let mut members = Map::new();
            members.insert("ContainerUrl".into(), p["ContainerUrl"].clone());
            members.insert(
                "AuthenticationType".into(),
                if p["AuthenticationType"].is_null() {
                    json!("SAS")
                } else {
                    p["AuthenticationType"].clone()
                },
            );
            members.insert("AgentArns".into(), p["AgentArns"].clone());
            Some(new_location(s, &region, uri, "AzureBlob", members))
        }
        "CreateLocationFsxLustre" => {
            let fs = p["FsxFilesystemArn"].as_str().unwrap_or_default();
            let sub = p["Subdirectory"].as_str().unwrap_or("/");
            let uri = format!(
                "fsxl://{}{sub}",
                fs.split(':').next_back().unwrap_or_default()
            );
            let mut members = Map::new();
            members.insert("FsxFilesystemArn".into(), p["FsxFilesystemArn"].clone());
            members.insert("SecurityGroupArns".into(), p["SecurityGroupArns"].clone());
            Some(new_location(s, &region, uri, "FsxLustre", members))
        }
        "CreateLocationFsxOntap" => {
            let svm = p["StorageVirtualMachineArn"].as_str().unwrap_or_default();
            let sub = p["Subdirectory"].as_str().unwrap_or("/");
            let uri = format!(
                "fsxo://{}{sub}",
                svm.split(':').next_back().unwrap_or_default()
            );
            let mut members = Map::new();
            members.insert(
                "StorageVirtualMachineArn".into(),
                p["StorageVirtualMachineArn"].clone(),
            );
            members.insert("Protocol".into(), p["Protocol"].clone());
            members.insert("SecurityGroupArns".into(), p["SecurityGroupArns"].clone());
            Some(new_location(s, &region, uri, "FsxOntap", members))
        }
        "CreateLocationFsxOpenZfs" => {
            let fs = p["FsxFilesystemArn"].as_str().unwrap_or_default();
            let sub = p["Subdirectory"].as_str().unwrap_or("/");
            let uri = format!(
                "fsxz://{}{sub}",
                fs.split(':').next_back().unwrap_or_default()
            );
            let mut members = Map::new();
            members.insert("FsxFilesystemArn".into(), p["FsxFilesystemArn"].clone());
            members.insert("Protocol".into(), p["Protocol"].clone());
            members.insert("SecurityGroupArns".into(), p["SecurityGroupArns"].clone());
            Some(new_location(s, &region, uri, "FsxOpenZfs", members))
        }
        "CreateLocationFsxWindows" => {
            let fs = p["FsxFilesystemArn"].as_str().unwrap_or_default();
            let sub = p["Subdirectory"].as_str().unwrap_or("/");
            let uri = format!(
                "fsxw://{}{sub}",
                fs.split(':').next_back().unwrap_or_default()
            );
            let mut members = Map::new();
            members.insert("FsxFilesystemArn".into(), p["FsxFilesystemArn"].clone());
            members.insert("SecurityGroupArns".into(), p["SecurityGroupArns"].clone());
            members.insert("User".into(), p["User"].clone());
            members.insert("Domain".into(), p["Domain"].clone());
            Some(new_location(s, &region, uri, "FsxWindows", members))
        }
        _ => None,
    }
}

fn describe_location(s: &mut State, arn_param: &Value, typ: &'static str) -> Response {
    let arn = arn_param.as_str().unwrap_or_default();
    let Some((k, loc)) = s.locations.iter().find(|(a, _)| a == arn) else {
        return invalid_request(&format!("Location {arn} is not found."));
    };
    if loc.typ != typ {
        return invalid_request(&format!("Invalid Location type: {}", loc.typ));
    }
    let mut out = Map::new();
    out.insert("LocationArn".into(), json!(k));
    out.insert("LocationUri".into(), json!(loc.uri.clone()));
    for (mem, val) in &loc.members {
        out.insert(mem.clone(), val.clone());
    }
    let v = Value::Object(out);
    ok_json(&v)
}

/// Register a location with its uri/type and per-type describe members.
fn new_location(
    s: &mut State,
    region: &str,
    uri: String,
    typ: &'static str,
    members: Map<String, Value>,
) -> Response {
    s.arn_counter += 1;
    let arn = arn_of(region, "location", "loc", s.arn_counter);
    s.locations
        .push((arn.clone(), Location { uri, typ, members }));
    ok_json(&json!({ "LocationArn": arn }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::HashMap;

    fn req(op: &str, body: Value) -> Request {
        Request {
            service: "datasync".into(),
            operation: op.into(),
            method: "POST".into(),
            path: "/".into(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap_or_default()),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    fn body(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap_or(Value::Null)
    }

    fn scope() -> u64 {
        uuid::Uuid::new_v4().as_u128() as u64
    }

    fn loc(acct: u64, name: &str) -> String {
        let mut r = req(
            "CreateLocationS3",
            json!({
                "S3BucketArn": format!("arn:aws:s3::{name}"),
                "S3Config": {"BucketAccessRoleArn": "arn:aws:iam::1:role/r"},
            }),
        );
        r.account = acct;
        body(&dispatch(&r).unwrap())["LocationArn"]
            .as_str()
            .unwrap()
            .to_string()
    }

    #[test]
    fn agent_lifecycle() {
        let acct = scope();
        let mut r = req("CreateAgent", json!({"AgentName": "a1"}));
        r.account = acct;
        let arn = body(&dispatch(&r).unwrap())["AgentArn"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(arn.starts_with("arn:aws:datasync:us-east-1:111222333444:agent/agent-"));
        assert!(arn.ends_with('1'));

        r = req("DescribeAgent", json!({"AgentArn": &arn}));
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(b["Status"], "ONLINE");
        assert_eq!(b["Name"], "a1");

        r = req("ListAgents", json!({}));
        r.account = acct;
        assert_eq!(
            body(&dispatch(&r).unwrap())["Agents"][0]["AgentArn"],
            arn.clone()
        );

        r = req("UpdateAgent", json!({"AgentArn": &arn, "Name": "renamed"}));
        r.account = acct;
        dispatch(&r).unwrap();
        r = req("DescribeAgent", json!({"AgentArn": &arn}));
        r.account = acct;
        assert_eq!(body(&dispatch(&r).unwrap())["Name"], "renamed");

        r = req("DeleteAgent", json!({"AgentArn": &arn}));
        r.account = acct;
        assert_eq!(dispatch(&r).unwrap().status, 200);
        r = req("DescribeAgent", json!({"AgentArn": &arn}));
        r.account = acct;
        let resp = dispatch(&r).unwrap();
        assert_eq!(body(&resp)["__type"], "InvalidRequestException");
    }

    #[test]
    fn location_types_round_trip() {
        let acct = scope();
        // S3 with subdirectory
        let mut r = req(
            "CreateLocationS3",
            json!({
                "S3BucketArn": "arn:aws:s3:::buck",
                "Subdirectory": "/pref",
                "S3Config": {"BucketAccessRoleArn": "arn:aws:iam::1:role/r"},
            }),
        );
        r.account = acct;
        let s3_arn = body(&dispatch(&r).unwrap())["LocationArn"]
            .as_str()
            .unwrap()
            .to_string();

        r = req("DescribeLocationS3", json!({"LocationArn": &s3_arn}));
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(b["LocationUri"], "s3://buck//pref"); // join semantics like moto

        // smb
        r = req(
            "CreateLocationSmb",
            json!({
                "ServerHostname": "smb.example.com",
                "Subdirectory": "/share",
                "User": "admin",
            }),
        );
        r.account = acct;
        let smb_arn = body(&dispatch(&r).unwrap())["LocationArn"]
            .as_str()
            .unwrap()
            .to_string();
        r = req("DescribeLocationSmb", json!({"LocationArn": &smb_arn}));
        r.account = acct;
        assert_eq!(
            body(&dispatch(&r).unwrap())["LocationUri"],
            "smb://smb.example.com//share"
        );

        // type mismatch error
        r = req("DescribeLocationS3", json!({"LocationArn": &smb_arn}));
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(b["__type"], "InvalidRequestException");
        assert!(b["message"]
            .as_str()
            .unwrap()
            .contains("Invalid Location type"));

        // list includes both
        r = req("ListLocations", json!({}));
        r.account = acct;
        assert_eq!(
            body(&dispatch(&r).unwrap())["Locations"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn task_and_execution_lifecycle() {
        let acct = scope();
        let src = loc(acct, "src");
        let dst = loc(acct, "dst");
        let mut r = req(
            "CreateTask",
            json!({
                "SourceLocationArn": &src,
                "DestinationLocationArn": &dst,
            }),
        );
        r.account = acct;
        let task_arn = body(&dispatch(&r).unwrap())["TaskArn"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(task_arn.contains(":task/task-"));

        r = req("DescribeTask", json!({"TaskArn": &task_arn}));
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert_eq!(b["Status"], "AVAILABLE");
        assert_eq!(b["SourceLocationArn"], src.clone());

        r = req(
            "UpdateTask",
            json!({"TaskArn": &task_arn, "Name": "renamed"}),
        );
        r.account = acct;
        dispatch(&r).unwrap();
        r = req("DescribeTask", json!({"TaskArn": &task_arn}));
        r.account = acct;
        assert_eq!(body(&dispatch(&r).unwrap())["Name"], "renamed");

        // start -> describe-execution -> cancel
        r = req("StartTaskExecution", json!({"TaskArn": &task_arn}));
        r.account = acct;
        let exec_arn = body(&dispatch(&r).unwrap())["TaskExecutionArn"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(exec_arn.contains("/execution/exec-"));

        r = req(
            "DescribeTaskExecution",
            json!({"TaskExecutionArn": &exec_arn}),
        );
        r.account = acct;
        assert_eq!(body(&dispatch(&r).unwrap())["Status"], "INITIALIZING");

        r = req(
            "CancelTaskExecution",
            json!({"TaskExecutionArn": &exec_arn}),
        );
        r.account = acct;
        assert_eq!(dispatch(&r).unwrap().status, 200);
        r = req(
            "CancelTaskExecution",
            json!({"TaskExecutionArn": &exec_arn}),
        );
        r.account = acct;
        let resp = dispatch(&r).unwrap();
        assert_eq!(resp.status, 400);
        assert!(body(&resp)["message"]
            .as_str()
            .unwrap()
            .contains("cannot be cancelled"));

        r = req("ListTasks", json!({}));
        r.account = acct;
        let b = body(&dispatch(&r).unwrap());
        assert!(b["Tasks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|t| t["TaskArn"] == task_arn.clone()));

        r = req("DeleteTask", json!({"TaskArn": &task_arn}));
        r.account = acct;
        dispatch(&r).unwrap();
        r = req("DescribeTask", json!({"TaskArn": &task_arn}));
        r.account = acct;
        assert_eq!(dispatch(&r).unwrap().status, 400);

        // location cycle
        r = req("DeleteLocation", json!({"LocationArn": &src}));
        r.account = acct;
        assert_eq!(dispatch(&r).unwrap().status, 200);
    }

    #[test]
    fn gap_ops_fall_through() {
        let acct = scope();
        // ops moto does not implement: tag/update-location/list-executions
        let mut r = req("TagResource", json!({"ResourceArn": "arn:x"}));
        r.account = acct;
        assert!(dispatch(&r).is_none());
        r = req("ListTaskExecutions", json!({}));
        r.account = acct;
        assert!(dispatch(&r).is_none());
        r = req("UpdateLocationS3", json!({"LocationArn": "arn:x"}));
        r.account = acct;
        assert!(dispatch(&r).is_none());
    }
}
