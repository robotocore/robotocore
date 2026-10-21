//! robotocore client bindings (Rust).
//!
//! Thin wrapper that aims `aws-sdk` clients at a running robotocore
//! twin. Language-agnostic behavior lives server-side; this crate only
//! wires credentials/endpoints. Sits alongside the Python, TypeScript,
//! and Go bindings under `bindings/`, mirroring
//! `../../python/robotocore_client`.
//!
//! Usage:
//!
//! ```no_run
//! let sqs = tokio::runtime::Runtime::new().unwrap().block_on(async {
//!     robotocore_client::session("sqs", None, None, "us-east-1")
//!         .await
//!         .unwrap() // talks to http://127.0.0.1:4566
//! });
//! # let _ = sqs;
//! ```

use std::env;

/// Plain HTTP endpoint for the twin: arg, `ROBOTOCORE_ENDPOINT`, default.
pub const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:4566";
/// Account id used when none is supplied: `ROBOTOCORE_ACCOUNT` or this.
pub const DEFAULT_ACCOUNT: &str = "123456789012";

/// Plain HTTP endpoint for the twin; trailing slashes trimmed.
pub fn endpoint_url(endpoint: Option<&str>) -> String {
    let url = endpoint
        .map(str::to_string)
        .or_else(|| env::var("ROBOTOCORE_ENDPOINT").ok())
        .unwrap_or_else(|| DEFAULT_ENDPOINT.to_string());
    url.trim_end_matches('/').to_string()
}

/// `(access_key, secret)` pair; a 12-digit access key IS the account id.
///
/// Falls back to `ROBOTOCORE_ACCOUNT`, then [`DEFAULT_ACCOUNT`]; the
/// secret is the static `"test"` the twin accepts for any account.
pub fn credentials(account: Option<&str>) -> (String, String) {
    let key = account
        .map(str::to_string)
        .or_else(|| env::var("ROBOTOCORE_ACCOUNT").ok())
        .unwrap_or_else(|| DEFAULT_ACCOUNT.to_string());
    let key = format!("{:0>12}", key);
    (key, "test".to_string())
}

/// An `aws-config` loader pre-wired for the twin: endpoint, region and
/// static credentials. Call `.load().await` yourself, or use the ready
/// [`session()`]/[`s3_client()`]/[`sqs_client()`] helpers instead.
pub fn session_config(
    account: Option<&str>,
    endpoint: Option<&str>,
    region: &str,
) -> aws_config::ConfigLoader {
    let (access_key, secret_key) = credentials(account);
    let credentials =
        aws_sdk_sqs::config::Credentials::new(access_key, secret_key, None, None, "robotocore");
    aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(region.to_owned()))
        .endpoint_url(endpoint_url(endpoint))
        .credentials_provider(credentials)
}

/// The full `SdkConfig` aimed at the twin.
pub async fn config(
    account: Option<&str>,
    endpoint: Option<&str>,
    region: &str,
) -> aws_config::SdkConfig {
    session_config(account, endpoint, region).load().await
}

/// Which ready-made service clients [crate] wires up.
pub enum Client {
    /// S3 client with path-style addressing baked in.
    S3(aws_sdk_s3::Client),
    /// SQS client for the twin.
    Sqs(aws_sdk_sqs::Client),
}

impl Client {
    /// The S3 client, if this session is for S3.
    pub fn as_s3(&self) -> Option<&aws_sdk_s3::Client> {
        match self {
            Client::S3(client) => Some(client),
            _ => None,
        }
    }

    /// The SQS client, if this session is for SQS.
    pub fn as_sqs(&self) -> Option<&aws_sdk_sqs::Client> {
        match self {
            Client::Sqs(client) => Some(client),
            _ => None,
        }
    }
}

/// Unknown `service` string passed to [`session()`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownServiceError(pub String);

impl std::fmt::Display for UnknownServiceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "robotocore: unknown service {:?}", self.0)
    }
}

impl std::error::Error for UnknownServiceError {}

/// A ready-made client for `service` ("s3" or "sqs") aimed at the twin.
///
/// Mirrors the Python binding's `session().client(service)`. Unknown
/// service names return [`UnknownServiceError`] — add the matching
/// `aws-sdk-*` crate and an arm below to extend.
pub async fn session(
    service: &str,
    account: Option<&str>,
    endpoint: Option<&str>,
    region: &str,
) -> Result<Client, UnknownServiceError> {
    match service {
        "s3" => Ok(Client::S3(s3_client(account, endpoint, region).await)),
        "sqs" => Ok(Client::Sqs(sqs_client(account, endpoint, region).await)),
        other => Err(UnknownServiceError(other.to_string())),
    }
}

/// An `aws-sdk-s3` client configured for the twin, with path-style
/// addressing forced (the twin serves buckets under `/bucket`, never a
/// virtual host).
pub async fn s3_client(account: Option<&str>, endpoint: Option<&str>, region: &str) -> aws_sdk_s3::Client {
    let conf = aws_sdk_s3::Config::from(&config(account, endpoint, region).await)
        .to_builder()
        .force_path_style(true)
        .build();
    aws_sdk_s3::Client::from_conf(conf)
}

/// An `aws-sdk-sqs` client configured for the twin.
pub async fn sqs_client(account: Option<&str>, endpoint: Option<&str>, region: &str) -> aws_sdk_sqs::Client {
    aws_sdk_sqs::Client::new(&config(account, endpoint, region).await)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_url_resolves_override_env_then_default() {
        assert_eq!(endpoint_url(Some("http://127.0.0.1:5000/")), "http://127.0.0.1:5000");
        assert_eq!(endpoint_url(None), DEFAULT_ENDPOINT);
        env::set_var("ROBOTOCORE_ENDPOINT", "http://127.0.0.1:5001");
        assert_eq!(endpoint_url(None), "http://127.0.0.1:5001");
        assert_eq!(endpoint_url(Some("x")), "x"); // override always wins
        env::remove_var("ROBOTOCORE_ENDPOINT");
    }

    #[test]
    fn credentials_zfill_accounts() {
        assert_eq!(credentials(Some("42")), ("000000000042".to_string(), "test".to_string()));
        assert_eq!(credentials(None).0, DEFAULT_ACCOUNT);
        env::set_var("ROBOTOCORE_ACCOUNT", "999999999999");
        assert_eq!(credentials(None).0, "999999999999");
        env::remove_var("ROBOTOCORE_ACCOUNT");
    }
}
