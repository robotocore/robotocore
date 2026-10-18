//! Moto bridge proxy - forwards non-native service requests to the Python sidecar.
//!
//! The Rust server handles native services (our crates) directly.
//! All other AWS service requests are proxied to the Python Moto sidecar
//! running on localhost.

use http::Method;
use std::collections::HashMap;
use std::sync::Arc;

use crate::core::protocol::{ParsedRequest, ParsedResponse};

/// The Moto sidecar proxy.
#[derive(Clone)]
pub struct MotoProxy {
    /// Base URL of the Python Moto sidecar (e.g., "http://127.0.0.1:4567").
    pub base_url: String,
    /// HTTP client for making proxy requests.
    pub client: reqwest::Client,
    /// Services that are handled natively (not proxied).
    pub native_services: Arc<std::collections::HashSet<String>>,
}

impl MotoProxy {
    pub fn new(base_url: String, native_services: Vec<String>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap_or_default();
        Self {
            base_url,
            client,
            native_services: Arc::new(native_services.into_iter().collect()),
        }
    }

    /// Check if a service is handled natively.
    pub fn is_native(&self, service: &str) -> bool {
        self.native_services.contains(service)
    }

    /// Proxy a request to the Moto sidecar.
    pub async fn forward(
        &self,
        req: &ParsedRequest,
    ) -> Result<ParsedResponse, Box<dyn std::error::Error>> {
        // Preserve the original path and query: moto handlers route on the
        // URL shape (rest-json/rest-xml services live at their REST paths,
        // e.g. POST /restapis for API Gateway; a bare "/" would 501).
        let mut url = format!("{}{}", self.base_url, req.path);
        if !req.query_string.is_empty() {
            url.push('?');
            url.push_str(&req.query_string);
        }
        // Forward the original path + query: REST-JSON services (appmesh,
        // amp, appsync, greengrass, ...) route on the request URI inside moto,
        // so posting to "/" would 501 every resource-path call. The original
        // path is what the moto sidecar's werkzeug routing table matches on.
        let url = format!("{}{}?{}", self.base_url, req.path, req.query_string);

        // Build the proxy request
        let mut proxy_req = self
            .client
            .request(
                Method::from_bytes(req.method.as_bytes()).unwrap_or(Method::POST),
                &url,
            )
            .header("x-robotocore-service", &req.service);

        // Forward relevant headers
        for (key, value) in &req.headers {
            let key_lower = key.to_lowercase();
            if key_lower.starts_with("x-amz-")
                || key_lower == "authorization"
                || key_lower == "content-type"
                || key_lower == "x-amz-target"
                || key_lower == "x-amz-date"
                || key_lower == "x-amz-security-token"
                || key_lower == "x-amz-content-sha256"
            {
                proxy_req = proxy_req.header(key.as_str(), value.as_str());
            }
        }

        // Set the body
        if !req.body.is_empty() {
            proxy_req = proxy_req.body(req.body.clone());
        }

        // Send the request
        let resp = proxy_req.send().await?;

        let status = resp.status();
        let headers_map = resp.headers().clone();
        let body_bytes = resp.bytes().await?;

        // Parse headers, dropping framing/hop-by-hop headers. The body may be
        // re-serialized downstream (e.g. JSON parsed and rebuilt in Rust), so a
        // forwarded Content-Length can mismatch the final bytes and stall
        // clients waiting for phantom bytes.
        const STRIP_HEADERS: [&str; 5] = [
            "content-length",
            "transfer-encoding",
            "keep-alive",
            "connection",
            "proxy-authenticate",
        ];
        let mut headers = HashMap::new();
        for (key, value) in headers_map.iter() {
            let key_lower = key.as_str().to_lowercase();
            if STRIP_HEADERS.contains(&key_lower.as_str()) {
                continue;
            }
            if let Ok(val) = value.to_str() {
                headers.insert(key.to_string(), val.to_string());
            }
        }

        // Check if it's JSON or raw
        let content_type = headers
            .get("content-type")
            .map(|s| s.as_str())
            .unwrap_or("application/json");

        if content_type.contains("json") {
            match serde_json::from_slice::<serde_json::Value>(&body_bytes) {
                Ok(body) => Ok(ParsedResponse {
                    status,
                    headers,
                    body,
                    raw: None,
                    raw_bytes: None,
                }),
                // Not actually JSON despite the header: never hand the client
                // a literal `null`, send the original bytes.
                Err(_) => Ok(ParsedResponse {
                    status,
                    headers,
                    body: serde_json::Value::Null,
                    raw: None,
                    raw_bytes: Some(body_bytes.to_vec()),
                }),
            }
        } else {
            // Bytes pass through unchanged (XML responses, binary octets).
            Ok(ParsedResponse {
                status,
                headers,
                body: serde_json::Value::Null,
                raw: None,
                raw_bytes: Some(body_bytes.to_vec()),
            })
        }
    }

    /// GET a config record from the sidecar (JSON), for the apigateway executor.
    pub async fn sidecar_get(
        &self,
        path: &str,
        region: &str,
    ) -> Result<SidecarResponse, Box<dyn std::error::Error>> {
        let url = format!("{}{}", self.base_url, path);
        let resp = self
            .client
            .get(&url)
            .header("x-robotocore-service", "apigateway")
            .header(
                "authorization",
                format!(
                    "AWS4-HMAC-SHA256 Credential=123456789012/20240101/{region}/apigateway/aws4_request"
                ),
            )
            .send()
            .await?;
        let status = resp.status();
        let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
        Ok(SidecarResponse { status, body })
    }
}

/// A parsed config response from the sidecar.
pub struct SidecarResponse {
    pub status: reqwest::StatusCode,
    pub body: serde_json::Value,
}
