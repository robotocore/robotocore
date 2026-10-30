//! AWS STS (Security Token Service) handler.
//!
//! Implements GetCallerIdentity, GetAccessKeyInfo, and other STS operations.

use crate::core::account::parse_account_from_key;
use crate::core::protocol::{ParsedRequest, ParsedResponse};
use http::StatusCode;
use serde_json::json;

/// Every operation the STS handler implements. The gateway consults this
/// list when an unsigned request lands on `/`: only a request naming one
/// of these operations belongs to STS; anything else must not be answered
/// with STS-shaped errors. Keep in sync with the match in
/// `handle_sts_request`.
pub const STS_KNOWN_OPS: &[&str] = &[
    "AssumeRole",
    "AssumeRoleWithSAML",
    "AssumeRoleWithWebIdentity",
    "AssumeRoot",
    "DecodeAWSAccountId",
    "DecodeAuthorizationMessage",
    "GetAccessKeyInfo",
    "GetAccessKeyLastUsed",
    "GetCallerIdentity",
    "GetDelegatedAccessToken",
    "GetFederationToken",
    "GetSessionToken",
    "GetWebIdentityToken",
];

/// True when the given action is an STS operation this build implements.
pub fn is_known_sts_op(action: &str) -> bool {
    STS_KNOWN_OPS.contains(&action)
}

/// Handle STS requests.
pub async fn handle_sts_request(
    req: &ParsedRequest,
) -> Result<ParsedResponse, Box<dyn std::error::Error>> {
    match req.operation.as_str() {
        "GetCallerIdentity" => handle_get_caller_identity(req),
        "GetAccessKeyInfo" => handle_get_access_key_info(req),
        "GetSessionToken" => handle_get_session_token(req),
        "GetFederationToken" => handle_get_federation_token(req),
        "AssumeRole" => handle_assume_role(req, "AssumeRole"),
        "AssumeRoleWithWebIdentity" => handle_assume_role(req, "AssumeRoleWithWebIdentity"),
        "AssumeRoleWithSAML" => handle_assume_role(req, "AssumeRoleWithSAML"),
        "AssumeRoot" => handle_assume_root(req),
        "GetDelegatedAccessToken" => handle_get_delegated_access_token(req),
        "GetWebIdentityToken" => handle_get_web_identity_token(req),
        "DecodeAuthorizationMessage" => handle_decode_auth_message(req),
        "DecodeAWSAccountId" => handle_decode_aws_account_id(req),
        "GetAccessKeyLastUsed" => handle_get_access_key_last_used(req),
        _ => Ok(ParsedResponse {
            status: StatusCode::NOT_FOUND,
            headers: {
                let mut h = std::collections::HashMap::new();
                h.insert("Content-Type".to_string(), "text/xml".to_string());
                h
            },
            body: json!({}),
            raw: Some(format!(
                r#"<?xml version="1.0" encoding="UTF-8"?>
<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
    <Error>
        <Type>Sender</Type>
        <Code>InvalidAction</Code>
        <Message>Unknown STS operation: {}</Message>
    </Error>
    <RequestId>00000000-0000-0000-0000-000000000000</RequestId>
</ErrorResponse>"#,
                req.operation
            )),
            raw_bytes: None,
        }),
    }
}

/// GetCallerIdentity operation.
///
/// Returns UserId, Account, and Arn based on the access key ID.
fn handle_get_caller_identity(
    req: &ParsedRequest,
) -> Result<ParsedResponse, Box<dyn std::error::Error>> {
    // Account is already parsed and available
    let account = req.account;

    // UserId from access key - format as AKIA... or similar
    let user_id = "AKIAIOSFODNN7EXAMPLE";

    // Build ARN
    let arn = format!("arn:aws:sts::{}:user/moto", account);

    let body = json!({
        "UserId": user_id,
        "Account": account.to_string(),
        "Arn": arn
    });

    Ok(ParsedResponse {
        status: StatusCode::OK,
        headers: {
            let mut h = std::collections::HashMap::new();
            h.insert("Content-Type".to_string(), "text/xml".to_string());
            h
        },
        body,
        raw: None,
        raw_bytes: None,
    })
}

/// GetAccessKeyInfo operation.
///
/// Returns the account ID for the given access key ID.
fn handle_get_access_key_info(
    req: &ParsedRequest,
) -> Result<ParsedResponse, Box<dyn std::error::Error>> {
    // Extract AccessKeyId from params
    let access_key_id = req
        .params
        .get("AccessKeyId")
        .and_then(|v| v.as_str())
        .ok_or("Missing AccessKeyId parameter")?;

    // Determine account from access key
    let account = parse_account_from_key(access_key_id);

    let body = json!({
        "Account": account.to_string()
    });

    Ok(ParsedResponse {
        status: StatusCode::OK,
        headers: {
            let mut h = std::collections::HashMap::new();
            h.insert("Content-Type".to_string(), "text/xml".to_string());
            h
        },
        body,
        raw: None,
        raw_bytes: None,
    })
}

/// A session expiry ~12h out from now, RFC3339 UTC — matches real STS
/// semantics (session credentials expire) and avoids stale hardcoded dates.
fn session_expiry() -> String {
    let t = chrono::Utc::now() + chrono::Duration::hours(12);
    t.format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

/// GetSessionToken operation.
fn handle_get_session_token(
    _req: &ParsedRequest,
) -> Result<ParsedResponse, Box<dyn std::error::Error>> {
    let body = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<GetSessionTokenResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
    <GetSessionTokenResult>
        <Credentials>
            <AccessKeyId>ASIAIOSFODNN7EXAMPLE</AccessKeyId>
            <SecretAccessKey>wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY</SecretAccessKey>
            <SessionToken>session-token-001</SessionToken>
            <Expiration>{}</Expiration>
        </Credentials>
    </GetSessionTokenResult>
    <ResponseMetadata>
        <RequestId>00000000-0000-0000-0000-000000000000</RequestId>
    </ResponseMetadata>
</GetSessionTokenResponse>"#,
        session_expiry()
    );
    Ok(xml_response(&body))
}

/// GetFederationToken operation.
fn handle_get_federation_token(
    req: &ParsedRequest,
) -> Result<ParsedResponse, Box<dyn std::error::Error>> {
    let account = req.account;
    let name = req
        .params
        .get("Name")
        .and_then(|v| v.as_str())
        .unwrap_or("federation-user");
    let body = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<GetFederationTokenResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
    <GetFederationTokenResult>
        <FederatedUser>
            <Arn>arn:aws:sts::{}:federated-user/{}</Arn>
            <FederatedUserId>{}/{}-1234567890</FederatedUserId>
        </FederatedUser>
        <Credentials>
            <AccessKeyId>ASIAIOSFODNN7EXAMPLE</AccessKeyId>
            <SecretAccessKey>wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY</SecretAccessKey>
            <SessionToken>session-token-001</SessionToken>
            <Expiration>{}</Expiration>
        </Credentials>
    </GetFederationTokenResult>
    <ResponseMetadata>
        <RequestId>00000000-0000-0000-0000-000000000000</RequestId>
    </ResponseMetadata>
</GetFederationTokenResponse>"#,
        account,
        xml_escape_text(name),
        account,
        xml_escape_text(name),
        session_expiry()
    );
    Ok(xml_response(&body))
}

/// AssumeRole / AssumeRoleWithWebIdentity / AssumeRoleWithSAML.
fn handle_assume_role(
    req: &ParsedRequest,
    op: &str,
) -> Result<ParsedResponse, Box<dyn std::error::Error>> {
    let account = req.account;
    let role_arn = req
        .params
        .get("RoleArn")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("arn:aws:iam::{}:role/lambda-role", account));
    let session_name = req
        .params
        .get("RoleSessionName")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| {
            format!(
                "session-{}",
                uuid::Uuid::new_v4()
                    .to_string()
                    .split('-')
                    .next()
                    .unwrap_or("0000")
            )
        });
    let role_name = role_arn.rsplit('/').next().unwrap_or(&role_arn);

    // Check policy size limit (2048 characters when packed)
    if let Some(policy) = req.params.get("Policy").and_then(|v| v.as_str()) {
        if policy.len() > 2048 {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Policy is too large",
            )));
        }
    }

    let arn = format!(
        "arn:aws:sts::{}:assumed-role/{}/{}",
        account, role_name, session_name
    );
    let role_id = format!("AROA{}:{}", account, session_name);
    let body = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<{}Response xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
    <{}Result>
        <Credentials>
            <AccessKeyId>ASIAIOSFODNN7EXAMPLE</AccessKeyId>
            <SecretAccessKey>wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY</SecretAccessKey>
            <SessionToken>session-token-001</SessionToken>
            <Expiration>{}</Expiration>
        </Credentials>
        <AssumedRoleUser>
            <Arn>{}</Arn>
            <AssumedRoleId>{}</AssumedRoleId>
        </AssumedRoleUser>
    </{}Result>
    <ResponseMetadata>
        <RequestId>00000000-0000-0000-0000-000000000000</RequestId>
    </ResponseMetadata>
</{}Response>"#,
        op,
        op,
        session_expiry(),
        xml_escape_text(&arn),
        xml_escape_text(&role_id),
        op,
        op
    );
    Ok(xml_response(&body))
}

/// AssumeRoot operation.
fn handle_assume_root(req: &ParsedRequest) -> Result<ParsedResponse, Box<dyn std::error::Error>> {
    let account = req.account;
    let arn = format!("arn:aws:sts::{}:assumed-role/Root/root", account);
    let body = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<AssumeRootResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
    <AssumeRootResult>
        <Credentials>
            <AccessKeyId>ASIAIOSFODNN7EXAMPLE</AccessKeyId>
            <SecretAccessKey>wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY</SecretAccessKey>
            <SessionToken>session-token-root</SessionToken>
            <Expiration>{}</Expiration>
        </Credentials>
        <AssumedRoleUser>
            <Arn>{}</Arn>
            <AssumedRoleId>AROA000000000000000000:root</AssumedRoleId>
        </AssumedRoleUser>
    </AssumeRootResult>
    <ResponseMetadata>
        <RequestId>00000000-0000-0000-0000-000000000000</RequestId>
    </ResponseMetadata>
</AssumeRootResponse>"#,
        session_expiry(),
        arn
    );
    Ok(xml_response(&body))
}

/// GetDelegatedAccessToken operation.
fn handle_get_delegated_access_token(
    req: &ParsedRequest,
) -> Result<ParsedResponse, Box<dyn std::error::Error>> {
    let _account = req.account;
    let body = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<GetDelegatedAccessTokenResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
    <GetDelegatedAccessTokenResult>
        <Credentials>
            <AccessKeyId>ASIADELEGATED0000001</AccessKeyId>
            <SecretAccessKey>delegated-secret-key-0000000000000000</SecretAccessKey>
            <SessionToken>delegated-session-token-0000000000000000</SessionToken>
            <Expiration>{}</Expiration>
        </Credentials>
    </GetDelegatedAccessTokenResult>
    <ResponseMetadata>
        <RequestId>00000000-0000-0000-0000-000000000000</RequestId>
    </ResponseMetadata>
</GetDelegatedAccessTokenResponse>"#,
        session_expiry()
    );
    Ok(xml_response(&body))
}

/// GetWebIdentityToken operation.
fn handle_get_web_identity_token(
    _req: &ParsedRequest,
) -> Result<ParsedResponse, Box<dyn std::error::Error>> {
    let body = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<GetWebIdentityTokenResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
    <GetWebIdentityTokenResult>
        <WebIdentityToken>eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c</WebIdentityToken>
        <Expiration>{}</Expiration>
    </GetWebIdentityTokenResult>
    <ResponseMetadata>
        <RequestId>00000000-0000-0000-0000-000000000000</RequestId>
    </ResponseMetadata>
</GetWebIdentityTokenResponse>"#,
        session_expiry()
    );
    Ok(xml_response(&body))
}

/// DecodeAuthorizationMessage operation.
fn handle_decode_auth_message(
    _req: &ParsedRequest,
) -> Result<ParsedResponse, Box<dyn std::error::Error>> {
    let body = r#"<?xml version="1.0" encoding="UTF-8"?>
<DecodeAuthorizationMessageResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
    <DecodeAuthorizationMessageResult>
        <DecodedMessage>{"account":"123456789012"}</DecodedMessage>
    </DecodeAuthorizationMessageResult>
    <ResponseMetadata>
        <RequestId>00000000-0000-0000-0000-000000000000</RequestId>
    </ResponseMetadata>
</DecodeAuthorizationMessageResponse>"#;
    Ok(xml_response(body))
}

/// DecodeAWSAccountId operation.
fn handle_decode_aws_account_id(
    _req: &ParsedRequest,
) -> Result<ParsedResponse, Box<dyn std::error::Error>> {
    let body = r#"<?xml version="1.0" encoding="UTF-8"?>
<DecodeAWSAccountIdResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
    <DecodeAWSAccountIdResult>
        <AccountId>123456789012</AccountId>
    </DecodeAWSAccountIdResult>
    <ResponseMetadata>
        <RequestId>00000000-0000-0000-0000-000000000000</RequestId>
    </ResponseMetadata>
</DecodeAWSAccountIdResponse>"#;
    Ok(xml_response(body))
}

/// GetAccessKeyLastUsed operation.
fn handle_get_access_key_last_used(
    _req: &ParsedRequest,
) -> Result<ParsedResponse, Box<dyn std::error::Error>> {
    let body = r#"<?xml version="1.0" encoding="UTF-8"?>
<GetAccessKeyLastUsedResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
    <GetAccessKeyLastUsedResult>
        <AccessKeyLastUsed>
            <LastUsedDate>2024-01-01T00:00:00Z</LastUsedDate>
            <ServiceName>sts</ServiceName>
            <Region>us-east-1</Region>
        </AccessKeyLastUsed>
    </GetAccessKeyLastUsedResult>
    <ResponseMetadata>
        <RequestId>00000000-0000-0000-0000-000000000000</RequestId>
    </ResponseMetadata>
</GetAccessKeyLastUsedResponse>"#;
    Ok(xml_response(body))
}

/// Helper: build a ParsedResponse from raw XML.

/// XML-escape a client-controlled value interpolated into the STS XML body
/// (rotocore::proto::xml_escape parity; `<`/`&` etc. would otherwise break
/// the response or inject sibling elements).
pub(crate) fn xml_escape_text(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn xml_response(body: &str) -> ParsedResponse {
    ParsedResponse {
        status: StatusCode::OK,
        headers: {
            let mut h = std::collections::HashMap::new();
            h.insert("Content-Type".to_string(), "text/xml".to_string());
            h
        },
        body: json!({}),
        raw: Some(body.to_string()),
        raw_bytes: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::collections::HashMap;

    #[test]
    fn test_get_caller_identity() {
        let params = HashMap::new();
        let req = ParsedRequest {
            service: "sts".to_string(),
            operation: "GetCallerIdentity".to_string(),
            params,
            body: bytes::Bytes::new(),
            region: "us-east-1".to_string(),
            account: 123456789012,
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
        };

        let resp = handle_get_caller_identity(&req).unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        if let Value::Object(obj) = &resp.body {
            assert!(obj.contains_key("UserId"));
            assert!(obj.contains_key("Account"));
            assert!(obj.contains_key("Arn"));
        } else {
            panic!("Expected JSON object response");
        }
    }

    #[test]
    fn test_get_access_key_info() {
        let mut params = HashMap::new();
        params.insert(
            "AccessKeyId".to_string(),
            Value::String("123456789012".to_string()),
        );

        let req = ParsedRequest {
            service: "sts".to_string(),
            operation: "GetAccessKeyInfo".to_string(),
            params,
            body: bytes::Bytes::new(),
            region: "us-east-1".to_string(),
            account: 123456789012,
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
        };

        let resp = handle_get_access_key_info(&req).unwrap();
        assert_eq!(resp.status, StatusCode::OK);
    }

    /// Drift guard: every entry in STS_KNOWN_OPS must be handled by
    /// `handle_sts_request` — an op listed here but missing from the match
    /// would resurrect the STS-shaped misparse the gateway veto was built
    /// to fix (unsigned callers would get `Unknown STS operation` XML).
    #[tokio::test]
    async fn known_ops_never_hit_the_invalid_action_arm() {
        for op in STS_KNOWN_OPS {
            let req = ParsedRequest {
                service: "sts".to_string(),
                operation: op.to_string(),
                params: HashMap::new(),
                body: bytes::Bytes::new(),
                region: "us-east-1".to_string(),
                account: 123456789012,
                method: "POST".to_string(),
                path: "/".to_string(),
                query_string: String::new(),
                headers: HashMap::new(),
            };
            // Parameter errors (Err / 4xx) are fine: they prove the
            // matched arm ran. The drift failure mode is the fallthrough
            // `_` arm answering 404 "Unknown STS operation".
            match handle_sts_request(&req).await {
                Ok(resp) => {
                    assert_ne!(
                        resp.status,
                        StatusCode::NOT_FOUND,
                        "{op} is in STS_KNOWN_OPS but has no handler arm"
                    )
                }
                Err(err) => {
                    let msg = err.to_string();
                    assert!(
                        !msg.contains("Unknown STS operation"),
                        "{op} reached the InvalidAction arm: {msg}"
                    );
                }
            }
        }
    }
}
