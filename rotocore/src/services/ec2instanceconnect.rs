//! EC2 Instance Connect backend (JSON protocol, target
//! `EC2InstanceConnect_20150115.SendSSHPublicKey`).
//!
//! moto always succeeds and returns a request id plus `Success: true`.

use crate::{Request, Response};
use serde_json::json;

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "ec2instanceconnect" {
        return None;
    }
    let _ = crate::proto::parse_json_body(&req.body);
    match req.operation.as_str() {
        "SendSSHPublicKey" | "SendSerialConsoleSSHPublicKey" => {
            let body = json!({
                "RequestId": format!("example-2a47-4c91-9700-{}", uuid::Uuid::new_v4()),
                "Success": true,
            });
            Some(Response::json(
                200,
                serde_json::to_vec(&body).unwrap_or_default(),
            ))
        }
        _ => {
            let body = json!({
                "__type": "NotImplemented",
                "message": format!("The {} action has not been implemented", req.operation),
            });
            Some(Response::json(
                501,
                serde_json::to_vec(&body).unwrap_or_default(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn req(op: &str) -> Request {
        Request {
            service: "ec2instanceconnect".into(),
            operation: op.into(),
            method: "POST".into(),
            path: "/".into(),
            query_string: String::new(),
            headers: Default::default(),
            body: bytes::Bytes::new(),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    #[test]
    fn send_ssh_public_key_succeeds() {
        let resp = dispatch(&req("SendSSHPublicKey")).unwrap();
        assert_eq!(resp.status, 200);
        let v: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(v["Success"], true);
        assert!(v["RequestId"].as_str().is_some());
    }
}
