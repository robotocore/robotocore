//! API Gateway Management API (@connections) — ported from the py gateway
//! websocket management endpoints. Connections live in
//! (account, region)-scoped state; missing connections behave like the py
//! gateway: GET/POST → 410, DELETE → 204 silently.

use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::json;
use std::collections::HashMap;

static SLOT: Lazy<store::Slot<AgwMgmtState>> =
    Lazy::new(|| store::slot::<AgwMgmtState>("apigatewaymanagementapi"));

#[derive(Default)]
struct AgwMgmtState {
    connections: HashMap<String, Connection>,
}

struct Connection {
    created: i64,
}

pub fn dispatch(req: &Request) -> Response {
    let mut guard = SLOT.write();
    let st = guard.entry((req.account, req.region.clone())).or_default();
    let path = req.path.trim_end_matches('/');
    let Some((_prefix, connection_id)) = path.split_once("/@connections/") else {
        return crate::proto::json_error(
            501,
            "NotImplemented",
            &format!("The {} action has not been implemented", req.operation),
        );
    };
    // py gateway routes /@connections/{api_id}/{stage}/{connection_id}? No:
    // the compat suite uses /@connections/{id} directly.
    if req.method == "GET" {
        match st.connections.get(connection_id) {
            Some(conn) => {
                let _ = conn;
                Response::json_status(
                    200,
                    &json!({"ConnectionId": connection_id, "ConnectedAt": conn.created as u64}),
                )
            }
            None => Response::json_status(410, &json!({"message": "Connection not found"})),
        }
    } else if req.method == "DELETE" {
        st.connections.remove(connection_id);
        Response::new(204, "application/json", String::new())
    } else if req.method == "POST" {
        let body_bytes = req.body.to_vec();
        let ok = body_bytes.len() < 4 * 1024 * 1024;
        if ok && st.connections.contains_key(connection_id) {
            Response::new(200, "application/json", String::new())
        } else {
            Response::json_status(410, &json!({"message": "Connection not found"}))
        }
    } else {
        crate::proto::json_error(
            501,
            "NotImplemented",
            &format!("The {} action has not been implemented", req.operation),
        )
    }
}
