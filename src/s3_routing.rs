//! S3 virtual-hosted-style routing.
//!
//! Port of `src/robotocore/gateway/s3_routing.py`.
//!
//! Parses Host headers to detect S3 virtual-hosted-style requests and
//! rewrites them to path-style for downstream handling.

use once_cell::sync::Lazy;
use parking_lot::RwLock;
use regex::Regex;
use std::env;
use std::sync::Arc;

/// Default hostname base for S3 virtual-hosted-style requests
const DEFAULT_S3_HOSTNAME: &str = "s3.localhost.robotocore.cloud";

/// Backwards-compatible alias for localstack.cloud hostnames
#[allow(dead_code)]
const S3_LOCALSTACK_HOSTNAME: &str = "s3.localhost.localstack.cloud";

/// Pre-compiled pattern for standard AWS S3 virtual-hosted requests
static VHOST_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"^(?P<bucket>[a-zA-Z0-9][a-zA-Z0-9.\-]{1,61}[a-zA-Z0-9])\.s3(?:\.(?P<region>[a-z]{2}-[a-z]+-\d+))?\.(?P<rest>.+?)(?::\d+)?$"
    ).unwrap()
});

/// Pre-compiled pattern for localstack.cloud backwards-compatible alias
static VHOST_LOCALSTACK_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"^(?P<bucket>[a-zA-Z0-9][a-zA-Z0-9.\-]{1,61}[a-zA-Z0-9])\.s3\.localhost\.localstack\.cloud(?::\d+)?$"
    ).unwrap()
});

/// Custom hostname pattern cache
struct CustomPatternCache {
    pattern: Regex,
    hostname: String,
}

static CUSTOM_PATTERN_CACHE: Lazy<Arc<RwLock<Option<CustomPatternCache>>>> =
    Lazy::new(|| Arc::new(RwLock::new(None)));

/// Return the configured S3 hostname base from environment or default
fn get_s3_hostname() -> String {
    env::var("S3_HOSTNAME").unwrap_or_else(|_| DEFAULT_S3_HOSTNAME.to_string())
}

/// Get or build the custom hostname pattern, caching it
fn get_custom_pattern() -> (Regex, String) {
    let hostname = get_s3_hostname();

    {
        let read_guard = CUSTOM_PATTERN_CACHE.read();
        if let Some(cache) = read_guard.as_ref() {
            if cache.hostname == hostname {
                return (cache.pattern.clone(), cache.hostname.clone());
            }
        }
    }

    let escaped = regex::escape(&hostname);
    let pattern = Regex::new(&format!(
        r"^(?P<bucket>[a-zA-Z0-9][a-zA-Z0-9.\-{{1,61}}][a-zA-Z0-9])\.{}(?::\d+)?$",
        escaped
    ))
    .unwrap();

    {
        let mut write_guard = CUSTOM_PATTERN_CACHE.write();
        *write_guard = Some(CustomPatternCache {
            pattern: pattern.clone(),
            hostname: hostname.clone(),
        });
    }

    (pattern, hostname)
}

/// S3 routing result
#[derive(Debug, Clone, PartialEq)]
pub struct S3VhostInfo {
    pub bucket: String,
    pub region: Option<String>,
}

/// Parse an S3 virtual-hosted-style Host header.
pub fn parse_s3_vhost(host: Option<&str>) -> Option<S3VhostInfo> {
    let host = host?;
    if host.is_empty() {
        return None;
    }

    let host_no_port = if let Some(idx) = host.rfind(':') {
        let after_colon = &host[idx + 1..];
        if after_colon.chars().all(|c| c.is_ascii_digit()) {
            &host[..idx]
        } else {
            host
        }
    } else {
        host
    };

    let (custom_re, _) = get_custom_pattern();
    if let Some(m) = custom_re.captures(host) {
        return Some(S3VhostInfo {
            bucket: m.name("bucket")?.as_str().to_string(),
            region: None,
        });
    }

    if let Some(m) = VHOST_LOCALSTACK_RE.captures(host) {
        return Some(S3VhostInfo {
            bucket: m.name("bucket")?.as_str().to_string(),
            region: None,
        });
    }

    if let Some(m) = VHOST_RE.captures(host) {
        let mut result = S3VhostInfo {
            bucket: m.name("bucket")?.as_str().to_string(),
            region: None,
        };

        if let Some(region) = m.name("region") {
            result.region = Some(region.as_str().to_string());
        } else {
            let rest = m.name("rest")?.as_str();
            if let Some(region_match) =
                Regex::new(r"(?:^|\.)((?:us|eu|ap|sa|ca|me|af|il)-[a-z]+-\d+)")
                    .unwrap()
                    .captures(rest)
            {
                result.region = Some(region_match.get(1)?.as_str().to_string());
            }
        }
        return Some(result);
    }

    if host_no_port.contains(".s3.") {
        let parts: Vec<&str> = host_no_port.splitn(2, ".s3.").collect();
        if !parts[0].is_empty() && !parts[0].starts_with('.') {
            let bucket = parts[0].to_string();
            let remainder = parts[1];

            let mut result = S3VhostInfo {
                bucket,
                region: None,
            };

            if let Some(region_match) =
                Regex::new(r"(?:^|\.)(us|eu|ap|sa|ca|me|af|il)(-[a-z]+-\d+)")
                    .unwrap()
                    .captures(remainder)
            {
                result.region = Some(format!(
                    "{}{}",
                    region_match.get(1)?.as_str(),
                    region_match.get(2)?.as_str()
                ));
            }
            return Some(result);
        }
    }

    if host_no_port.ends_with(".localhost") || host_no_port.contains(".localhost:") {
        let label = {
            let idx = host_no_port.find(".localhost")?;
            &host_no_port[..idx]
        };

        if !label.is_empty() && !label.contains('.') {
            return Some(S3VhostInfo {
                bucket: label.to_string(),
                region: None,
            });
        }
    }

    None
}

/// ASGI scope structure
#[derive(Debug, Clone)]
pub struct Scope {
    pub r#type: String,
    pub method: Option<String>,
    pub path: String,
    pub query_string: String,
    pub headers: Vec<(String, String)>,
}

/// Check if an ASGI scope represents an S3 virtual-hosted-style request.
pub fn is_s3_vhost_request(scope: &Scope) -> bool {
    if scope.r#type != "http" {
        return false;
    }

    for header in &scope.headers {
        if header.0 == "host" {
            return parse_s3_vhost(Some(&header.1)).is_some();
        }
    }
    false
}

/// Rewrite a virtual-hosted-style S3 request scope to path-style.
pub fn rewrite_vhost_to_path(scope: &Scope) -> Option<Scope> {
    let mut host = String::new();
    for header in &scope.headers {
        if header.0 == "host" {
            host = header.1.clone();
            break;
        }
    }

    if host.is_empty() {
        return None;
    }

    let parsed = parse_s3_vhost(Some(&host))?;
    let bucket = parsed.bucket;
    let original_path = &scope.path;

    let new_path = if original_path == "/" {
        format!("/{}", bucket)
    } else {
        format!("/{}{}", bucket, original_path)
    };

    let new_host = if host.len() > bucket.len() + 1 {
        host[bucket.len() + 1..].to_string()
    } else {
        host.clone()
    };

    let mut new_scope = scope.clone();
    new_scope.headers = scope
        .headers
        .iter()
        .map(|h| {
            if h.0 == "host" {
                ("host".to_string(), new_host.clone())
            } else {
                h.clone()
            }
        })
        .collect();

    new_scope.path = new_path;

    if !scope.query_string.is_empty() {
        new_scope.query_string = scope.query_string.clone();
    }

    Some(new_scope)
}

/// S3 routing configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct S3RoutingConfig {
    pub s3_hostname: String,
    pub virtual_hosted_style: bool,
    pub website_hostname: String,
    pub supported_patterns: Vec<String>,
}

/// Return the current S3 routing configuration
pub fn get_s3_routing_config() -> S3RoutingConfig {
    let hostname = get_s3_hostname();
    S3RoutingConfig {
        s3_hostname: hostname.clone(),
        virtual_hosted_style: true,
        website_hostname: format!("s3-website.{}", hostname),
        supported_patterns: vec![
            "<bucket>.s3.<hostname>".to_string(),
            "<bucket>.s3.<region>.amazonaws.com".to_string(),
            "<bucket>.s3.amazonaws.com".to_string(),
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reset_cache() {
        let mut cache = CUSTOM_PATTERN_CACHE.write();
        *cache = None;
    }

    #[test]
    fn test_default_hostname() {
        reset_cache();
        let result = parse_s3_vhost(Some("mybucket.s3.localhost.robotocore.cloud"));
        assert!(result.is_some());
        assert_eq!(result.unwrap().bucket, "mybucket");
    }

    #[test]
    fn test_aws_region_hostname() {
        reset_cache();
        let result = parse_s3_vhost(Some("mybucket.s3.us-east-1.amazonaws.com"));
        assert!(result.is_some());
        let info = result.unwrap();
        assert_eq!(info.bucket, "mybucket");
        assert_eq!(info.region, Some("us-east-1".to_string()));
    }

    #[test]
    fn test_aws_global_hostname() {
        reset_cache();
        let result = parse_s3_vhost(Some("mybucket.s3.amazonaws.com"));
        assert!(result.is_some());
        let info = result.unwrap();
        assert_eq!(info.bucket, "mybucket");
        assert_eq!(info.region, None);
    }

    #[test]
    fn test_non_s3_hostname_returns_none() {
        reset_cache();
        assert!(parse_s3_vhost(Some("example.com")).is_none());
        assert!(parse_s3_vhost(Some("api.example.com")).is_none());
        assert!(parse_s3_vhost(Some("localhost")).is_none());
    }

    #[test]
    fn test_empty_host_returns_none() {
        reset_cache();
        assert!(parse_s3_vhost(Some("")).is_none());
    }

    #[test]
    fn test_none_host_returns_none() {
        reset_cache();
        assert!(parse_s3_vhost(None).is_none());
    }

    #[test]
    fn test_host_with_port() {
        reset_cache();
        let result = parse_s3_vhost(Some("mybucket.s3.localhost.robotocore.cloud:4566"));
        assert!(result.is_some());
        assert_eq!(result.unwrap().bucket, "mybucket");
    }

    #[test]
    fn test_s3_express_localhost_host() {
        reset_cache();
        let result = parse_s3_vhost(Some("mybucket--use1-az1--x-s3.localhost:4566"));
        assert!(result.is_some());
        assert_eq!(result.unwrap().bucket, "mybucket--use1-az1--x-s3");
    }

    #[test]
    fn test_s3_object_lambda_route_token() {
        reset_cache();
        let result = parse_s3_vhost(Some("my-route-token.localhost:4566"));
        assert!(result.is_some());
        assert_eq!(result.unwrap().bucket, "my-route-token");
    }

    #[test]
    fn test_localstack_hostname_alias() {
        reset_cache();
        let result = parse_s3_vhost(Some("mybucket.s3.localhost.localstack.cloud"));
        assert!(result.is_some());
        assert_eq!(result.unwrap().bucket, "mybucket");
    }

    #[test]
    fn test_eu_west_region() {
        reset_cache();
        let result = parse_s3_vhost(Some("mybucket.s3.eu-west-1.amazonaws.com"));
        assert!(result.is_some());
        let info = result.unwrap();
        assert_eq!(info.bucket, "mybucket");
        assert_eq!(info.region, Some("eu-west-1".to_string()));
    }

    #[test]
    fn test_dualstack_hostname() {
        reset_cache();
        let result = parse_s3_vhost(Some("mybucket.s3.dualstack.us-east-1.amazonaws.com"));
        assert!(result.is_some());
        let info = result.unwrap();
        assert_eq!(info.bucket, "mybucket");
        assert_eq!(info.region, Some("us-east-1".to_string()));
    }

    #[test]
    fn test_bucket_with_dots() {
        reset_cache();
        let result = parse_s3_vhost(Some("my.bucket.name.s3.us-east-1.amazonaws.com"));
        assert!(result.is_some());
        assert_eq!(result.unwrap().bucket, "my.bucket.name");
    }

    #[test]
    fn test_bucket_with_hyphens() {
        reset_cache();
        let result = parse_s3_vhost(Some("my-test-bucket.s3.localhost.robotocore.cloud"));
        assert!(result.is_some());
        assert_eq!(result.unwrap().bucket, "my-test-bucket");
    }

    #[test]
    fn test_custom_s3_hostname_env() {
        reset_cache();
        env::set_var("S3_HOSTNAME", "s3.custom.local");
        let result = parse_s3_vhost(Some("testbucket.s3.custom.local"));
        assert!(result.is_some());
        assert_eq!(result.unwrap().bucket, "testbucket");
        env::remove_var("S3_HOSTNAME");
        reset_cache();
    }

    #[test]
    fn test_aws_regional_hostname() {
        reset_cache();
        let result = parse_s3_vhost(Some("mybucket.s3.us-west-2.amazonaws.com"));
        assert!(result.is_some());
        let info = result.unwrap();
        assert_eq!(info.bucket, "mybucket");
        assert_eq!(info.region, Some("us-west-2".to_string()));
    }

    #[test]
    fn test_sa_region() {
        reset_cache();
        let result = parse_s3_vhost(Some("mybucket.s3.sa-east-1.amazonaws.com"));
        assert!(result.is_some());
        let info = result.unwrap();
        assert_eq!(info.bucket, "mybucket");
        assert_eq!(info.region, Some("sa-east-1".to_string()));
    }

    #[test]
    fn test_ap_region() {
        reset_cache();
        let result = parse_s3_vhost(Some("data-bucket.s3.ap-southeast-2.amazonaws.com"));
        assert!(result.is_some());
        let info = result.unwrap();
        assert_eq!(info.bucket, "data-bucket");
        assert_eq!(info.region, Some("ap-southeast-2".to_string()));
    }

    #[test]
    fn test_all_numeric_bucket() {
        reset_cache();
        let result = parse_s3_vhost(Some("123456789.s3.localhost.robotocore.cloud"));
        assert!(result.is_some());
        assert_eq!(result.unwrap().bucket, "123456789");
    }

    #[test]
    fn test_mixed_case_bucket() {
        reset_cache();
        let result = parse_s3_vhost(Some("MyBucket.s3.localhost.robotocore.cloud"));
        assert!(result.is_some());
        assert_eq!(result.unwrap().bucket, "MyBucket");
    }

    #[test]
    fn test_rewrite_root_path() {
        reset_cache();
        let scope = Scope {
            r#type: "http".to_string(),
            method: Some("GET".to_string()),
            path: "/".to_string(),
            query_string: "".to_string(),
            headers: vec![
                (
                    "host".to_string(),
                    "mybucket.s3.localhost.robotocore.cloud".to_string(),
                ),
                ("content-type".to_string(), "text/plain".to_string()),
            ],
        };
        let result = rewrite_vhost_to_path(&scope);
        assert!(result.is_some());
        assert_eq!(result.unwrap().path, "/mybucket");
    }

    #[test]
    fn test_rewrite_key_path() {
        reset_cache();
        let scope = Scope {
            r#type: "http".to_string(),
            method: Some("GET".to_string()),
            path: "/key.txt".to_string(),
            query_string: "".to_string(),
            headers: vec![
                (
                    "host".to_string(),
                    "mybucket.s3.localhost.robotocore.cloud".to_string(),
                ),
                ("content-type".to_string(), "text/plain".to_string()),
            ],
        };
        let result = rewrite_vhost_to_path(&scope);
        assert!(result.is_some());
        assert_eq!(result.unwrap().path, "/mybucket/key.txt");
    }

    #[test]
    fn test_preserves_query_string() {
        reset_cache();
        let scope = Scope {
            r#type: "http".to_string(),
            method: Some("GET".to_string()),
            path: "/key.txt".to_string(),
            query_string: "versionId=123".to_string(),
            headers: vec![
                (
                    "host".to_string(),
                    "mybucket.s3.localhost.robotocore.cloud".to_string(),
                ),
                ("content-type".to_string(), "text/plain".to_string()),
            ],
        };
        let result = rewrite_vhost_to_path(&scope);
        assert!(result.is_some());
        let res = result.unwrap();
        assert_eq!(res.path, "/mybucket/key.txt");
        assert_eq!(res.query_string, "versionId=123");
    }

    #[test]
    fn test_non_s3_host_returns_none() {
        reset_cache();
        let scope = Scope {
            r#type: "http".to_string(),
            method: Some("GET".to_string()),
            path: "/key.txt".to_string(),
            query_string: "".to_string(),
            headers: vec![
                ("host".to_string(), "example.com".to_string()),
                ("content-type".to_string(), "text/plain".to_string()),
            ],
        };
        assert!(rewrite_vhost_to_path(&scope).is_none());
    }

    #[test]
    fn test_is_s3_vhost_request() {
        reset_cache();
        let scope = Scope {
            r#type: "http".to_string(),
            method: Some("GET".to_string()),
            path: "/".to_string(),
            query_string: "".to_string(),
            headers: vec![(
                "host".to_string(),
                "mybucket.s3.localhost.robotocore.cloud".to_string(),
            )],
        };
        assert!(is_s3_vhost_request(&scope));
    }

    #[test]
    fn test_is_not_s3_vhost_request() {
        reset_cache();
        let scope = Scope {
            r#type: "http".to_string(),
            method: Some("GET".to_string()),
            path: "/".to_string(),
            query_string: "".to_string(),
            headers: vec![("host".to_string(), "localhost:4566".to_string())],
        };
        assert!(!is_s3_vhost_request(&scope));
    }

    #[test]
    fn test_rewrite_preserves_headers() {
        reset_cache();
        let scope = Scope {
            r#type: "http".to_string(),
            method: Some("GET".to_string()),
            path: "/key.txt".to_string(),
            query_string: "".to_string(),
            headers: vec![
                (
                    "host".to_string(),
                    "mybucket.s3.localhost.robotocore.cloud".to_string(),
                ),
                ("content-type".to_string(), "text/plain".to_string()),
            ],
        };
        let result = rewrite_vhost_to_path(&scope);
        assert!(result.is_some());
        let res = result.unwrap();
        assert_eq!(res.path, "/mybucket/key.txt");
        let host_header = res.headers.iter().find(|h| h.0 == "host").unwrap();
        assert_eq!(host_header.1, "s3.localhost.robotocore.cloud");
        assert!(res
            .headers
            .iter()
            .any(|h| h.0 == "content-type" && h.1 == "text/plain"));
    }

    #[test]
    fn test_rewrite_aws_regional_host() {
        reset_cache();
        let scope = Scope {
            r#type: "http".to_string(),
            method: Some("GET".to_string()),
            path: "/data.csv".to_string(),
            query_string: "".to_string(),
            headers: vec![(
                "host".to_string(),
                "mybucket.s3.eu-west-1.amazonaws.com".to_string(),
            )],
        };
        let result = rewrite_vhost_to_path(&scope);
        assert!(result.is_some());
        assert_eq!(result.unwrap().path, "/mybucket/data.csv");
    }

    #[test]
    fn test_config_default() {
        reset_cache();
        env::remove_var("S3_HOSTNAME");
        let config = get_s3_routing_config();
        assert_eq!(config.s3_hostname, "s3.localhost.robotocore.cloud");
        assert!(config.virtual_hosted_style);
        assert_eq!(
            config.website_hostname,
            "s3-website.s3.localhost.robotocore.cloud"
        );
    }

    #[test]
    fn test_config_custom_hostname() {
        reset_cache();
        env::set_var("S3_HOSTNAME", "s3.mycompany.dev");
        let config = get_s3_routing_config();
        assert_eq!(config.s3_hostname, "s3.mycompany.dev");
        assert_eq!(config.website_hostname, "s3-website.s3.mycompany.dev");
        env::remove_var("S3_HOSTNAME");
        reset_cache();
    }

    #[test]
    fn test_deeply_nested_key() {
        reset_cache();
        let scope = Scope {
            r#type: "http".to_string(),
            method: Some("GET".to_string()),
            path: "/a/b/c/d/e/file.json".to_string(),
            query_string: "".to_string(),
            headers: vec![(
                "host".to_string(),
                "mybucket.s3.localhost.robotocore.cloud".to_string(),
            )],
        };
        let result = rewrite_vhost_to_path(&scope);
        assert!(result.is_some());
        assert_eq!(result.unwrap().path, "/mybucket/a/b/c/d/e/file.json");
    }

    #[test]
    fn test_key_with_special_chars() {
        reset_cache();
        let scope = Scope {
            r#type: "http".to_string(),
            method: Some("GET".to_string()),
            path: "/path%20with%20spaces/file.txt".to_string(),
            query_string: "".to_string(),
            headers: vec![(
                "host".to_string(),
                "mybucket.s3.localhost.robotocore.cloud".to_string(),
            )],
        };
        let result = rewrite_vhost_to_path(&scope);
        assert!(result.is_some());
        assert_eq!(
            result.unwrap().path,
            "/mybucket/path%20with%20spaces/file.txt"
        );
    }
}
