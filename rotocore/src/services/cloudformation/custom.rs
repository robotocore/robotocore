//! Custom-resource arm: `AWS::CloudFormation::CustomResource` and
//! `Custom::*`.
//!
//! Ports moto `cloudformation/custom_model.py` (`CustomModel.create_from_cloudformation_json`):
//! a custom resource is a promise — the template's `ServiceToken` lambda
//! receives a moto-shaped invocation event, and the resource stays
//! `CREATE_IN_PROGRESS` until the lambda's cfnresponse shim calls back.
//! The shim PUTs `/{cloudformation}/{region}/cfnresponse?stack={stack_id}`
//! (moto `urls.py:7`), whose receiver (`responses.py:129-133`,
//! `process_cfn_response`) finalizes the resource record from the payload:
//! `Status=SUCCESS` stores `Data` (returned for every Fn::GetAtt,
//! `custom_model.py:140-146`) and, once no pending custom resource is left,
//! flips the stack to complete; `Status=FAILED` records a FAILURE resource
//! event with the payload reason.

use serde_json::{json, Map, Value};
use std::collections::HashMap;
use uuid::Uuid;

/// True for the two custom resource spellings (moto `parsing.py:277-278`
/// routes both `Custom::*` and the built-in to `CustomModel`).
pub fn is_custom_type(rtype: &str) -> bool {
    rtype == "AWS::CloudFormation::CustomResource" || rtype.starts_with("Custom::")
}

/// Required custom-resource property; moto reads it before anything else
/// (`custom_model.py:47`: `properties["ServiceToken"]`).
pub fn service_token(props: &Value) -> Result<String, String> {
    match props.get("ServiceToken").and_then(Value::as_str) {
        Some(t) if !t.is_empty() => Ok(t.to_string()),
        _ => Err("ServiceToken is required for custom resources".to_string()),
    }
}

/// Lambda function identifier from a ServiceToken (moto passes the string
/// straight to `lambda_backend.get_function`, which accepts a name or ARN —
/// but the loopback invoke path needs the plain function name).
pub fn service_token_fn_name(token: &str) -> &str {
    if let Some(rest) = token.strip_prefix("arn:aws:lambda:") {
        // arn:aws:lambda:{region}:{account}:function:{name}[:{qualifier}]
        rest.splitn(6, ':')
            .last()
            .unwrap_or(token)
            .split(':')
            .next()
            .unwrap_or(token)
    } else {
        token.split(':').next().unwrap_or(token)
    }
}

/// Default physical id while the lambda runs, shaped like moto's
/// generated resource name for unnamed types (`parsing.py:294-316`
/// `generate_resource_name` → `{stack}-{logical}-{12-char alnum}`).
pub fn moto_physical_name(stack_name: &str, logical_id: &str) -> String {
    const CHARS: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let mut buf = [0u8; 12];
    for b in &mut buf {
        let i = Uuid::new_v4().as_bytes()[usize::from(*b) % 16] % 36;
        *b = CHARS[usize::from(i)];
    }
    let suffix = String::from_utf8_lossy(&buf).to_string();
    format!("{stack_name}-{logical_id}-{suffix}")
}

/// The moto invocation event (`custom_model.py:76-87`): the fields the
/// moto test suite pins (`tests/test_cloudformation/
/// test_cloudformation_custom_resources.py:85-123`) plus a
/// `PhysicalResourceId` default carrying the armed physical id.
#[allow(clippy::too_many_arguments)]
pub fn build_event(
    request_type: &str,
    service_token: &str,
    response_url: &str,
    stack_id: &str,
    logical_id: &str,
    resource_type: &str,
    physical_id: &str,
    properties: &Value,
) -> Value {
    json!({
        "RequestType": request_type,
        "ServiceToken": service_token,
        "ResponseURL": response_url,
        "StackId": stack_id,
        "RequestId": Uuid::new_v4().to_string(),
        "LogicalResourceId": logical_id,
        "ResourceType": resource_type,
        "ResourceProperties": properties,
        "PhysicalResourceId": physical_id,
    })
}

/// The receiver path this module answers
/// (moto `urls.py:7`: `{0}/cloudformation_(?P<region>[^/]+)/cfnresponse$`).
pub fn cfnresponse_region(path: &str) -> Option<String> {
    let rest = path.strip_prefix('/')?;
    let rest = rest.strip_prefix("cloudformation_")?;
    let region = rest.split('/').next()?;
    if region.is_empty() || !rest.ends_with("/cfnresponse") {
        return None;
    }
    Some(region.to_string())
}

/// One cfnresponse payload — the shim's PUT body. moto accepts the keys
/// from the querystring first, then a JSON body (`core/responses.py`
/// `_get_param`); the AWS shim and the moto manual test both send JSON
/// (POST json / urllib PUT), so parse json then fall back to form pairs.
#[derive(Debug, Default)]
pub struct CfnResponsePayload {
    pub status: String,
    pub reason: String,
    pub physical_id: String,
    pub stack_id: String,
    pub logical_id: String,
    pub request_id: String,
    pub no_echo: bool,
    /// Callback `Data` — becomes every GetAtt attribute of the resource.
    pub data: HashMap<String, Value>,
}

impl CfnResponsePayload {
    pub fn from_json(body: &Value, stack_query: &str) -> Self {
        let mut out = Self {
            status: str_field(body, "Status").unwrap_or_default(),
            reason: str_field(body, "Reason").unwrap_or_default(),
            physical_id: str_field(body, "PhysicalResourceId").unwrap_or_default(),
            stack_id: str_field(body, "StackId").unwrap_or_default(),
            logical_id: str_field(body, "LogicalResourceId").unwrap_or_default(),
            request_id: str_field(body, "RequestId").unwrap_or_default(),
            no_echo: body.get("NoEcho").and_then(Value::as_bool).unwrap_or(false),
            data: HashMap::new(),
        };
        if let Some(data) = body.get("Data").and_then(Value::as_object) {
            for (k, v) in data {
                out.data.insert(k.clone(), v.clone());
            }
        }
        // moto also keys the stack off the ResponseURL query
        // (`custom_model.py:73-74` puts `?stack={stack_id}` there).
        if out.stack_id.is_empty() {
            out.stack_id = form_value(stack_query, "stack").unwrap_or_default();
        }
        out
    }

    pub fn from_body(body: &[u8], stack_query: &str) -> Self {
        if let Ok(v) = serde_json::from_slice::<Value>(body) {
            return Self::from_json(&v, stack_query);
        }
        // Form fallback: urlencoded params count twice (shim bodies with a
        // JSON content-type and none alike reach moto's receiver — moto's
        // querystring parse covers `Status=SUCCESS` bodies too).
        let text = String::from_utf8_lossy(body);
        Self::from_qs(&text, stack_query)
    }

    pub fn from_qs(qs: &str, stack_query: &str) -> Self {
        let mut body = Map::new();
        for pair in qs.split('&') {
            if pair.is_empty() {
                continue;
            }
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            match serde_json::from_str::<Value>(v).ok() {
                Some(v) => body.insert(form_decode(k), v),
                None => body.insert(form_decode(k), json!(form_decode(v))),
            };
        }
        Self::from_json(&Value::Object(body), stack_query)
    }
}

fn str_field(body: &Value, key: &str) -> Option<String> {
    body.get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(String::from)
}

/// `k=v&…` single-value lookup (querystring semantics, `+` → space).
fn form_value(qs: &str, key: &str) -> Option<String> {
    for pair in qs.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            if form_decode(k) == key {
                return Some(form_decode(v));
            }
        }
    }
    None
}

fn form_decode(s: &str) -> String {
    urldecode(s, true)
}

fn urldecode(s: &str, plus_is_space: bool) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("");
                match u8::from_str_radix(hex, 16) {
                    Ok(byte) => {
                        out.push(byte);
                        i += 3;
                    }
                    Err(_) => {
                        out.push(bytes[i]);
                        i += 1;
                    }
                }
            }
            b'+' if plus_is_space => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn event(base: &str, region: &str, stack_id: &str, logical: &str, props: Value) -> Value {
        build_event(
            "Create",
            "arn:aws:lambda:us-east-1:123456789012:function:shim",
            &response_url(base, region, stack_id),
            stack_id,
            logical,
            "Custom::Info",
            "stack1-CustomInfo-J37BTR",
            &props,
        )
    }

    // mirror::custom_response_url shape lives in mirror.rs; keep an identical
    // local copy for the event-shape test.
    fn response_url(base: &str, region: &str, stack_id: &str) -> String {
        format!("{base}/cloudformation_{region}/cfnresponse?stack={stack_id}")
    }

    #[test]
    fn custom_type_match() {
        assert!(is_custom_type("Custom::Info"));
        assert!(is_custom_type("AWS::CloudFormation::CustomResource"));
        assert!(!is_custom_type("AWS::SNS::Topic"));
        assert!(!is_custom_type("AWS::Custom::weird"));
        assert!(is_custom_type("Custom::")); // prefix match by design
        assert!(is_custom_type("Custom::!"));
    }

    #[test]
    fn event_shape_matches_moto() {
        let props = json!({"ServiceToken": "tok", "MyProperty": "stuff"});
        let ev = event(
            "http://127.0.0.1:4790",
            "eu-north-1",
            "arn:aws:cloudformation:eu-north-1:123456789012:stack/s1/xyz",
            "CustomInfo",
            props.clone(),
        );
        // moto event keys (custom_model.py:76-87) + PhysicalResourceId.
        let keys: Vec<&str> = ev.as_object().unwrap().keys().map(String::as_str).collect();
        for expected in [
            "RequestType",
            "ServiceToken",
            "ResponseURL",
            "StackId",
            "RequestId",
            "LogicalResourceId",
            "ResourceType",
            "ResourceProperties",
            "PhysicalResourceId",
        ] {
            assert!(keys.contains(&expected), "missing {expected}");
        }
        assert_eq!(ev["RequestType"], "Create");
        assert_eq!(ev["ResourceType"], "Custom::Info");
        assert_eq!(ev["LogicalResourceId"], "CustomInfo");
        assert_eq!(
            ev["ResponseURL"],
            "http://127.0.0.1:4790/cloudformation_eu-north-1/cfnresponse?stack=arn:aws:cloudformation:eu-north-1:123456789012:stack/s1/xyz"
        );
        assert_eq!(ev["ResourceProperties"], props);
        assert!(!ev["RequestId"].as_str().unwrap().is_empty());
    }

    #[test]
    fn service_token_arm_parse() {
        assert_eq!(
            service_token(&json!({"ServiceToken": "arn:aws:lambda:us-east-1:1:function:abc"}))
                .unwrap(),
            "arn:aws:lambda:us-east-1:1:function:abc"
        );
        assert_eq!(
            service_token_fn_name("arn:aws:lambda:us-east-1:1:function:abc"),
            "abc"
        );
        assert_eq!(service_token_fn_name("my-fn"), "my-fn");
        assert!(service_token(&json!({})).is_err());
        assert!(service_token(&json!({"ServiceToken": ""})).is_err());
    }

    #[test]
    fn physical_name_shape() {
        let name = moto_physical_name("stack1", "CustomInfo");
        let parts: Vec<&str> = name.split('-').collect();
        // {stack}-{logical}-{12 alnum uppercase}
        assert!(parts.len() >= 3);
        let suffix = parts.last().unwrap();
        assert_eq!(suffix.len(), 12);
        assert!(suffix
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit()));
    }

    #[test]
    fn cfnresponse_region_capture() {
        assert_eq!(
            cfnresponse_region("/cloudformation_eu-north-1/cfnresponse"),
            Some("eu-north-1".to_string())
        );
        assert_eq!(cfnresponse_region("/cloudformation_us-east-1/other"), None);
        assert_eq!(cfnresponse_region("/cfnresponse"), None);
    }

    #[test]
    fn payload_parse_json_body() {
        let p = CfnResponsePayload::from_json(
            &json!({
                "Status": "SUCCESS",
                "Reason": "ok",
                "PhysicalResourceId": "CustomResourceAbc",
                "StackId": "arn:aws:cloudformation:us-east-1:123:stack/s/1",
                "RequestId": "req",
                "LogicalResourceId": "CustomInfo",
                "NoEcho": false,
                "Data": {"info_value": "special value"},
            }),
            "",
        );
        assert_eq!(p.status, "SUCCESS");
        assert_eq!(p.physical_id, "CustomResourceAbc");
        assert_eq!(p.data.get("info_value"), Some(&json!("special value")));
        // StackId from body wins over the empty query.
        assert!(p.stack_id.contains("stack/s/1"));
    }

    #[test]
    fn payload_parse_form_body_and_query_fallback() {
        // Form-encoded body (AWS shim urlencoded shape).
        let body = b"Status=SUCCESS&StackId=arn%3Aaws%3Acloudformation%3Aus-east-1%3A5%3Astack%2Fs%2F1&LogicalResourceId=CustomInfo&Data=%7B%22a%22%3A1%7D";
        let p = CfnResponsePayload::from_body(body, "");
        assert_eq!(p.status, "SUCCESS");
        assert_eq!(p.logical_id, "CustomInfo");
        assert!(p.stack_id.ends_with("stack/s/1"));

        // Missing stack in body → the ?stack= query that moto's
        // ResponseURL carries.
        let p2 = CfnResponsePayload::from_json(
            &json!({"Status": "FAILED", "Reason": "boom"}),
            "stack=arn:aws:cloudformation:r:7:stack/s/2",
        );
        assert_eq!(p2.status, "FAILED");
        assert_eq!(p2.stack_id, "arn:aws:cloudformation:r:7:stack/s/2");
        assert!(p2.reason == "boom");
        // A FAILED payload carries no data.
        assert!(!p2.data.contains_key("x"));
    }
}
