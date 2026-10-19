//! EBS Direct APIs backend (REST-JSON).
//!
//! Port of moto's ebs: snapshot lifecycle for direct block access —
//! StartSnapshot, PutSnapshotBlock, GetSnapshotBlock, CompleteSnapshot,
//! ListSnapshotBlocks, ListChangedBlocks.

use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Value};

#[derive(Default)]
struct State {
    /// SnapshotId -> {Status, VolumeSize, BlockSize, blocks{idx->checksum}}
    snapshots: Vec<Value>,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("ebs"));
    SLOT.clone()
}

fn ok(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "ebs" {
        return None;
    }
    let body = crate::proto::parse_json_body(&req.body);
    let slot = state();
    let mut st = slot.write();
    let b = st.entry((req.account, req.region.clone())).or_default();
    Some(handle(req, b, &body))
}

impl State {
    fn find_mut(&mut self, id: &str) -> Option<&mut Value> {
        self.snapshots
            .iter_mut()
            .find(|s| s["SnapshotId"] == json!(id))
    }
}

fn not_found(id: &str) -> Response {
    let e = json!({
        "__type": "ResourceNotFoundException",
        "message": format!("snapshot {id} not found"),
    });
    Response::json(404, serde_json::to_vec(&e).unwrap_or_default())
}

fn handle(req: &Request, st: &mut State, p: &Value) -> Response {
    let account = req.account.to_string();
    let path = req.path.as_str();
    let method = req.method.as_str();

    match method {
        "POST" if path == "/snapshots" => {
            let id = format!("snap-{}", uuid::Uuid::new_v4().simple());
            let snapshot = json!({
                "SnapshotId": id.clone(),
                "OwnerId": account,
                "Status": "pending",
                "StartTime": now_epoch(),
                "VolumeSize": p.get("VolumeSize").cloned().unwrap_or(json!(1)),
                "BlockSize": 512,
                "Tags": p.get("Tags").cloned().unwrap_or(json!([])),
                "Description": p.get("Description").cloned().unwrap_or(Value::Null),
            });
            st.snapshots.push(json!({
                "SnapshotId": id,
                "Status": "pending",
                "VolumeSize": p.get("VolumeSize").cloned().unwrap_or(json!(1)),
                "BlockSize": 512,
            }));
            ok(&snapshot)
        }
        "PUT" => snapshot_put(req, st, p),
        "GET" => snapshot_get(req, st),
        _ => ok(&json!({})),
    }
}

fn now_epoch() -> f64 {
    chrono::Utc::now().timestamp_millis() as f64 / 1000.0
}

/// PUT /snapshots/completion/{id}; PUT /snapshots/{id}/blocks/{idx}.
fn snapshot_put(req: &Request, st: &mut State, p: &Value) -> Response {
    let path = req.path.as_str();
    if let Some(id) = path
        .strip_prefix("/snapshots/completion/")
        .map(|s| s.trim_end_matches('/'))
    {
        return match st.find_mut(id) {
            Some(snapshot) => {
                snapshot["Status"] = json!("completed");
                let body = json!({"Status": "completed"});
                let mut resp = Response::json(202, serde_json::to_vec(&body).unwrap_or_default());
                resp.headers
                    .push(("Content-Type".into(), "application/json".into()));
                resp
            }
            None => not_found(id),
        };
    }
    if path.starts_with("/snapshots/") {
        // .../blocks/{idx} → snapshot id is the third segment from the end.
        let segs: Vec<&str> = path.trim_end_matches('/').split('/').collect();
        if segs.len() >= 4 {
            let idx = segs[segs.len() - 1];
            let id = segs[2];
            let checksum = header(req, "x-amz-checksum");
            let algorithm = header(req, "x-amz-checksum-algorithm");
            return match st.find_mut(id) {
                Some(snapshot) => {
                    let blocks = snapshot
                        .as_object_mut()
                        .unwrap()
                        .entry("blocks".to_string())
                        .or_insert(json!({}));
                    let _ = p;
                    blocks[idx] = json!({
                        "checksum": checksum,
                        "algorithm": algorithm,
                        "token": uuid::Uuid::new_v4(),
                    });
                    let mut resp = Response::json(201, b"{}".to_vec());
                    resp.headers.push(("x-amz-Checksum".into(), checksum));
                    resp.headers
                        .push(("x-amz-Checksum-Algorithm".into(), algorithm));
                    return resp;
                }
                None => not_found(id),
            };
        }
    }
    ok(&json!({}))
}

fn header(req: &Request, name: &str) -> String {
    req.headers
        .get(name)
        .cloned()
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string())
}

fn snapshot_get(req: &Request, st: &State) -> Response {
    let path = req.path.as_str();
    let segs: Vec<&str> = path.trim_end_matches('/').split('/').collect();
    if segs.len() >= 3 && path.contains("/changedblocks") {
        let id = segs[segs.len() - 2];
        let Some(s) = st.snapshots.iter().find(|s| s["SnapshotId"] == json!(id)) else {
            return not_found(id);
        };
        return ok(&json!({
            "ChangedBlocks": [],
            "VolumeSize": s["VolumeSize"],
            "BlockSize": s["BlockSize"],
        }));
    }
    if segs.len() >= 2 {
        let id = segs[segs.len() - 2];
        let Some(s) = st.snapshots.iter().find(|s| s["SnapshotId"] == json!(id)) else {
            return not_found(id);
        };
        let blocks: Vec<Value> = s["blocks"]
            .as_object()
            .map(|m| {
                m.keys()
                    .enumerate()
                    .map(|(i, k)| {
                        json!({"BlockIndex": k.parse::<u64>().unwrap_or(i as u64),
                               "BlockToken": s["blocks"][k]["token"]})
                    })
                    .collect()
            })
            .unwrap_or_default();
        return ok(&json!({
            "Blocks": blocks,
            "VolumeSize": s["VolumeSize"],
            "BlockSize": s["BlockSize"],
        }));
    }
    not_found("?")
}
