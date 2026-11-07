//! Kinesis request/response types (json protocol).

use bytes::Bytes;
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct AwsRequest {
    pub service: String,
    pub operation: String,
    pub account: u64,
    pub region: String,
    pub params: Value,
    pub body: Bytes,
}

#[derive(Debug, Clone)]
pub struct AwsResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

impl AwsResponse {
    pub fn json(status: u16, body: Value) -> Self {
        Self {
            status,
            headers: vec![
                (
                    "Content-Type".to_string(),
                    "application/x-amz-json-1.0".to_string(),
                ),
                (
                    "x-amzn-RequestId".to_string(),
                    uuid::Uuid::new_v4().to_string(),
                ),
                ("server".to_string(), "robotocore".to_string()),
            ],
            body: serde_json::to_string(&body).unwrap_or_else(|_| "{}".to_string()),
        }
    }

    pub fn error(status: u16, code: &str, message: &str) -> Self {
        let body = serde_json::json!({ "__type": code, "message": message });
        Self {
            status,
            headers: vec![
                (
                    "Content-Type".to_string(),
                    "application/x-amz-json-1.0".to_string(),
                ),
                (
                    "x-amzn-RequestId".to_string(),
                    uuid::Uuid::new_v4().to_string(),
                ),
                ("server".to_string(), "robotocore".to_string()),
            ],
            body: serde_json::to_string(&body).unwrap_or_else(|_| "{}".to_string()),
        }
    }

    /// AWS query-protocol error envelope (`<ErrorResponse><Error>`), for
    /// form-encoded `Action=` requests (the same call shape reads as the
    /// JSON protocol via `X-Amz-Target`).
    pub fn query_error(status: u16, code: &str, message: &str) -> Self {
        let esc = |s: &str| {
            s.replace('&', "&amp;")
                .replace('<', "&lt;")
                .replace('>', "&gt;")
        };
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <ErrorResponse xmlns=\"http://monitoring.amazonaws.com/doc/2010-08-01/\">\
             <Error><Type>Sender</Type><Code>{code}</Code>\
             <Message>{message}</Message></Error>\
             <RequestId>{rid}</RequestId></ErrorResponse>",
            code = esc(code),
            message = esc(message),
            rid = uuid::Uuid::new_v4()
        );
        Self {
            status,
            headers: vec![
                ("Content-Type".to_string(), "text/xml".to_string()),
                (
                    "x-amzn-RequestId".to_string(),
                    uuid::Uuid::new_v4().to_string(),
                ),
                ("server".to_string(), "robotocore".to_string()),
            ],
            body,
        }
    }
}

/// Render a handler's JSON body as the CLASSIC query-protocol XML envelope
/// (`<{Op}Response><{Op}Result>…</{Op}Result></{Op}Response>`).
///
/// Real CloudWatch still serves the query protocol (POST
/// `Action=DescribeAlarms` form bodies — the terraform AWS provider's Go
/// SDK speaks it; botocore moved to Smithy RPC v2 CBOR, which is why the
/// compat suite never saw this). The wire mapping is mechanical: objects
/// map to child elements, arrays to repeated `<member>` children, scalars
/// to text; nulls are skipped.
fn xml_text_esc(v: &str) -> String {
    v.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

pub fn json_to_query_xml(operation: &str, value: &Value) -> String {
    fn esc(v: &str) -> String {
        xml_text_esc(v)
    }
    fn render(buf: &mut String, tag: &str, v: &Value) {
        match v {
            Value::Null => {}
            Value::Object(map) => {
                buf.push('<');
                buf.push_str(tag);
                buf.push('>');
                for (k, child) in map {
                    render(buf, k, child);
                }
                buf.push_str("</");
                buf.push_str(tag);
                buf.push('>');
            }
            Value::Array(items) => {
                buf.push('<');
                buf.push_str(tag);
                buf.push('>');
                for item in items {
                    render(buf, "member", item);
                }
                buf.push_str("</");
                buf.push_str(tag);
                buf.push('>');
            }
            other => {
                buf.push('<');
                buf.push_str(tag);
                buf.push('>');
                match other {
                    Value::String(s) => buf.push_str(&esc(s)),
                    _ => buf.push_str(&other.to_string()),
                }
                buf.push_str("</");
                buf.push_str(tag);
                buf.push('>');
            }
        }
    }
    let mut out = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    out.push_str(&format!("<{operation}Response>"));
    out.push_str(&format!("<{operation}Result>"));
    if let Value::Object(map) = value {
        for (k, child) in map {
            render(&mut out, k, child);
        }
    } else {
        render(&mut out, "Result", value);
    }
    out.push_str(&format!("</{operation}Result>"));
    out.push_str(&format!(
        "<ResponseMetadata><RequestId>{}</RequestId></ResponseMetadata>",
        uuid::Uuid::new_v4()
    ));
    out.push_str(&format!("</{operation}Response>"));
    out
}

/// Render an error as the query protocol's ErrorResponse envelope.
pub fn json_error_to_query_xml(value: &Value) -> Option<String> {
    let code = value.get("__type")?.as_str()?;
    // The crate's error codes arrive Service-prefixed
    // ("com.amazonaws.cloudwatch#Foo") or bare ("Foo") — take the tail.
    let code = code.rsplit('#').next().unwrap_or(code);
    let message = value.get("message").and_then(|m| m.as_str()).unwrap_or("");
    Some(format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <ErrorResponse xmlns=\"http://monitoring.amazonaws.com/doc/2010-08-01/\">\n\
         <Error>\n<Type>Sender</Type>\n<Code>{}</Code>\n\
         <Message>{}</Message>\n</Error>\n\
         <RequestId>{}</RequestId>\n</ErrorResponse>",
        xml_text_esc(code),
        xml_text_esc(message),
        uuid::Uuid::new_v4()
    ))
}

#[cfg(test)]
mod query_xml_tests {
    use super::*;

    #[test]
    fn renders_the_query_envelope_with_member_wrapping() {
        let v = serde_json::json!({
            "MetricAlarms": [{"AlarmName": "a", "Threshold": 80.0}],
            "NextToken": Value::Null
        });
        let xml = json_to_query_xml("DescribeAlarms", &v);
        assert!(xml.starts_with("<?xml version=\"1.0\""));
        assert!(xml.contains("<DescribeAlarmsResponse>"));
        assert!(xml.contains("<DescribeAlarmsResult>"));
        assert!(xml.contains("<MetricAlarms><member><AlarmName>a</AlarmName>"));
        assert!(xml.contains("<Threshold>80.0</Threshold>"));
        assert!(xml.contains("<ResponseMetadata><RequestId>"));
        // Null members are skipped, not rendered as <NextToken/>.
        assert!(!xml.contains("NextToken"));
    }

    #[test]
    fn errors_render_as_error_responses() {
        let xml = json_error_to_query_xml(
            &serde_json::json!({"__type": "com.amazonaws.cloudwatch#ResourceNotFound", "message": "nope <bad>"})
        )
        .unwrap();
        assert!(xml.contains("<Code>ResourceNotFound</Code>"));
        assert!(xml.contains("<Message>nope &lt;bad&gt;</Message>"));
    }
}
