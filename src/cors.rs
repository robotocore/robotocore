//! CORS handling for the gateway.
//!
//! Port of `src/robotocore/gateway/cors.py`.
//!
//! Builds CORS response headers from environment-driven config and applies
//! S3 bucket CORS rules. Pure logic — the Starlette `Response` wrapping
//! (`build_preflight_response`) stays on the Python side.

use std::env;

/// Standard AWS request headers that should always be allowed
pub const DEFAULT_ALLOWED_HEADERS: [&str; 11] = [
    "Authorization",
    "Content-Type",
    "Content-MD5",
    "Cache-Control",
    "X-Amz-Content-Sha256",
    "X-Amz-Date",
    "X-Amz-Security-Token",
    "X-Amz-Target",
    "X-Amz-User-Agent",
    "X-Amzn-Authorization",
    "x-localstack-tgt",
];

/// Standard AWS response headers that should be exposed
pub const DEFAULT_EXPOSE_HEADERS: [&str; 8] = [
    "x-amz-request-id",
    "x-amz-id-2",
    "x-amz-version-id",
    "x-amz-delete-marker",
    "ETag",
    "x-amz-server-side-encryption",
    "x-amzn-RequestId",
    "x-amz-bucket-region",
];

pub const DEFAULT_ALLOWED_METHODS: [&str; 6] = ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD"];

pub const DEFAULT_MAX_AGE: &str = "86400";

/// CORS configuration loaded from environment variables.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CorsConfig {
    pub disable_cors_headers: bool,
    pub disable_cors_checks: bool,
    pub disable_custom_cors_s3: bool,
    pub disable_custom_cors_apigateway: bool,
    pub disable_preflight_processing: bool,
    pub allowed_headers: Vec<String>,
    pub expose_headers: Vec<String>,
    pub allowed_origins: Vec<String>,
    pub allowed_methods: Vec<String>,
}

impl CorsConfig {
    /// Build config from environment variables (mirrors `CORSConfig.from_environment`).
    pub fn from_env() -> Self {
        Self {
            disable_cors_headers: env::var("DISABLE_CORS_HEADERS").as_deref() == Ok("1"),
            disable_cors_checks: env::var("DISABLE_CORS_CHECKS").as_deref() == Ok("1"),
            disable_custom_cors_s3: env::var("DISABLE_CUSTOM_CORS_S3").as_deref() == Ok("1"),
            disable_custom_cors_apigateway: env::var("DISABLE_CUSTOM_CORS_APIGATEWAY").as_deref()
                == Ok("1"),
            disable_preflight_processing: env::var("DISABLE_PREFLIGHT_PROCESSING").as_deref()
                == Ok("1"),
            allowed_headers: DEFAULT_ALLOWED_HEADERS
                .iter()
                .map(|s| s.to_string())
                .chain(parse_csv(
                    &env::var("EXTRA_CORS_ALLOWED_HEADERS").unwrap_or_default(),
                ))
                .collect(),
            expose_headers: DEFAULT_EXPOSE_HEADERS
                .iter()
                .map(|s| s.to_string())
                .chain(parse_csv(
                    &env::var("EXTRA_CORS_EXPOSE_HEADERS").unwrap_or_default(),
                ))
                .collect(),
            allowed_origins: parse_csv(&env::var("EXTRA_CORS_ALLOWED_ORIGINS").unwrap_or_default()),
            allowed_methods: {
                let extra = parse_csv(&env::var("CORS_ALLOWED_METHODS").unwrap_or_default());
                if extra.is_empty() {
                    DEFAULT_ALLOWED_METHODS
                        .iter()
                        .map(|s| s.to_string())
                        .collect()
                } else {
                    extra
                }
            },
        }
    }
}

/// Parse a comma-separated string into a list of trimmed, non-empty strings.
pub fn parse_csv(value: &str) -> Vec<String> {
    if value.is_empty() {
        return Vec::new();
    }
    value
        .split(',')
        .map(|item| item.trim().to_string())
        .filter(|item| !item.is_empty())
        .collect()
}

/// Match `text` against an fnmatch-style `pattern` (Python `fnmatch` semantics,
/// POSIX case-sensitive):
/// - `*` matches any sequence (including empty)
/// - `?` matches exactly one character
/// - `[seq]` matches any character in `seq` (supports `a-z` ranges)
/// - `[!seq]` matches any character NOT in `seq`
/// - A `]` as the first member of a class is literal; an unclosed `[` is literal.
pub fn fnmatch(pattern: &str, text: &str) -> bool {
    fn match_here(pat: &[u8], txt: &[u8]) -> bool {
        match pat.first() {
            None => txt.is_empty(),
            Some(b'*') => {
                // Collapse consecutive '*'
                let mut i = 0;
                while i < pat.len() && pat[i] == b'*' {
                    i += 1;
                }
                let pat_rest = &pat[i..];
                if txt.is_empty() {
                    return match_here(pat_rest, txt);
                }
                match_here(pat_rest, txt) || match_here(pat, &txt[1..])
            }
            Some(b'?') => !txt.is_empty() && match_here(&pat[1..], &txt[1..]),
            Some(b'[') => {
                // Locate closing ']' (first ']' right after '[' or '[!' is a member)
                let mut j = 1;
                if j < pat.len() && pat[j] == b'!' {
                    j += 1;
                }
                if j < pat.len() && pat[j] == b']' {
                    j += 1;
                }
                while j < pat.len() && pat[j] != b']' {
                    j += 1;
                }
                if j >= pat.len() {
                    // No closing ']': treat '[' as a literal character
                    !txt.is_empty() && txt[0] == b'[' && match_here(&pat[1..], &txt[1..])
                } else if txt.is_empty() {
                    false
                } else {
                    let negated = pat[1] == b'!';
                    if class_contains(&pat[1..j], txt[0]) == negated {
                        false
                    } else {
                        match_here(&pat[j + 1..], &txt[1..])
                    }
                }
            }
            Some(c) => !txt.is_empty() && txt[0] == *c && match_here(&pat[1..], &txt[1..]),
        }
    }

    fn class_contains(class: &[u8], ch: u8) -> bool {
        let mut i = 0;
        // A ']' as the first member is literal
        if i < class.len() && class[i] == b']' {
            if class[i] == ch {
                return true;
            }
            i += 1;
        }
        let mut matched = false;
        while i < class.len() {
            // Range a-z (only when the dash has both neighbors)
            if i + 2 < class.len() && class[i + 1] == b'-' && class[i + 2] != b']' {
                let lo = class[i];
                let hi = class[i + 2];
                if lo <= ch && ch <= hi {
                    matched = true;
                }
                i += 3;
            } else {
                if class[i] == ch {
                    matched = true;
                }
                i += 1;
            }
        }
        matched
    }

    match_here(pattern.as_bytes(), text.as_bytes())
}

/// Check if an origin matches any of the allowed origin patterns.
pub fn origin_matches(origin: &str, patterns: &[String]) -> bool {
    for pattern in patterns {
        if pattern == "*" || pattern == origin {
            return true;
        }
        if fnmatch(pattern, origin) {
            return true;
        }
    }
    false
}

/// Check if a method matches any of the allowed methods (case-insensitive).
pub fn method_matches(method: &str, allowed_methods: &[String]) -> bool {
    let method_upper = method.to_ascii_uppercase();
    allowed_methods
        .iter()
        .any(|m| m == "*" || m.to_ascii_uppercase() == method_upper)
}

/// Determine which origin value to return in Access-Control-Allow-Origin.
pub fn resolve_origin(config: &CorsConfig, request_origin: Option<&str>) -> Option<String> {
    // No specific origins configured -> wildcard
    if config.allowed_origins.is_empty() {
        return Some("*".to_string());
    }

    // DISABLE_CORS_CHECKS -> accept anything
    if config.disable_cors_checks {
        return Some(request_origin.unwrap_or("*").to_string());
    }

    // Wildcard in the allowed list
    if config.allowed_origins.iter().any(|o| o == "*") {
        return Some("*".to_string());
    }

    // No origin in request -> not a CORS request
    let origin = request_origin?;

    if origin_matches(origin, &config.allowed_origins) {
        Some(origin.to_string())
    } else {
        None
    }
}

/// Build CORS response headers based on config and request origin.
///
/// Returns an empty vec if CORS headers are disabled or the origin is not
/// allowed.
pub fn build_cors_headers(
    config: &CorsConfig,
    request_origin: Option<&str>,
) -> Vec<(String, String)> {
    if config.disable_cors_headers {
        return Vec::new();
    }

    let origin_value = match resolve_origin(config, request_origin) {
        Some(v) => v,
        None => return Vec::new(),
    };

    let mut headers: Vec<(String, String)> = vec![
        ("Access-Control-Allow-Origin".into(), origin_value.clone()),
        (
            "Access-Control-Allow-Methods".into(),
            config.allowed_methods.join(", "),
        ),
        (
            "Access-Control-Allow-Headers".into(),
            config.allowed_headers.join(", "),
        ),
        (
            "Access-Control-Expose-Headers".into(),
            config.expose_headers.join(", "),
        ),
        ("Access-Control-Max-Age".into(), DEFAULT_MAX_AGE.to_string()),
    ];

    // Reflected a specific origin -> Vary: Origin
    if origin_value != "*" {
        headers.push(("Vary".into(), "Origin".into()));
    }

    headers
}

/// S3 CORS rule as stored by the S3 provider (parsed from bucket XML).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct S3CorsRule {
    pub allowed_origins: Vec<String>,
    pub allowed_methods: Vec<String>,
    pub allowed_headers: Vec<String>,
    pub expose_headers: Vec<String>,
    pub max_age_seconds: Option<i64>,
}

impl S3CorsRule {
    /// Build a rule from a list of list-items, tolerating missing/extra keys.
    ///
    /// The Python provider stores `AllowedOrigins`/`AllowedMethods`/... as
    /// *lists* of strings (not CSV), so accept both: a list of strings, or a
    /// comma-joined string.
    pub fn from_lists(
        allowed_origins: Vec<String>,
        allowed_methods: Vec<String>,
        allowed_headers: Vec<String>,
        expose_headers: Vec<String>,
        max_age_seconds: Option<String>,
    ) -> Self {
        Self {
            allowed_origins,
            allowed_methods,
            allowed_headers,
            expose_headers,
            max_age_seconds: max_age_seconds.and_then(|v| v.parse().ok()),
        }
    }
}

/// Apply S3 bucket CORS rules instead of default CORS.
///
/// `request_headers` is accepted for API compatibility (the Python version
/// does not use it either).
pub fn build_s3_cors_headers(
    rules: &[S3CorsRule],
    request_origin: Option<&str>,
    request_method: Option<&str>,
    _request_headers: Option<&str>,
) -> Vec<(String, String)> {
    let origin = match request_origin {
        Some(o) => o,
        None => return Vec::new(),
    };

    for rule in rules {
        if !origin_matches(origin, &rule.allowed_origins) {
            continue;
        }
        if let Some(m) = request_method {
            if !method_matches(m, &rule.allowed_methods) {
                continue;
            }
        }

        let mut headers: Vec<(String, String)> = Vec::new();
        headers.push(("Access-Control-Allow-Origin".into(), origin.to_string()));
        if !rule.allowed_methods.is_empty() {
            headers.push((
                "Access-Control-Allow-Methods".into(),
                rule.allowed_methods.join(", "),
            ));
        }
        if !rule.allowed_headers.is_empty() {
            headers.push((
                "Access-Control-Allow-Headers".into(),
                rule.allowed_headers.join(", "),
            ));
        }
        if !rule.expose_headers.is_empty() {
            headers.push((
                "Access-Control-Expose-Headers".into(),
                rule.expose_headers.join(", "),
            ));
        }
        if let Some(max_age) = rule.max_age_seconds {
            headers.push(("Access-Control-Max-Age".into(), max_age.to_string()));
        }
        headers.push(("Vary".into(), "Origin".into()));
        return headers;
    }

    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn config() -> CorsConfig {
        // Mirrors CORSConfig.from_environment() with an empty environment:
        // lists default to the standard header/method lists.
        CorsConfig {
            allowed_headers: DEFAULT_ALLOWED_HEADERS
                .iter()
                .map(|s| s.to_string())
                .collect(),
            expose_headers: DEFAULT_EXPOSE_HEADERS
                .iter()
                .map(|s| s.to_string())
                .collect(),
            allowed_methods: DEFAULT_ALLOWED_METHODS
                .iter()
                .map(|s| s.to_string())
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn test_parse_csv() {
        assert_eq!(parse_csv("a, b ,c"), vec!["a", "b", "c"]);
        assert_eq!(parse_csv(""), Vec::<String>::new());
        assert_eq!(parse_csv(",,a,, "), vec!["a"]);
        assert_eq!(parse_csv("  "), Vec::<String>::new());
    }

    #[test]
    fn test_fnmatch_star() {
        assert!(fnmatch("*", ""));
        assert!(fnmatch("*", "anything"));
        assert!(fnmatch("*.example.com", "sub.example.com"));
        assert!(!fnmatch("*.example.com", "example.com")); // literal '.' still must match
        assert!(!fnmatch("*.example.com", "example.org"));
        assert!(fnmatch("http://*", "http://a.b/c"));
        assert!(fnmatch("**", "a"));
        assert!(fnmatch("*a*", "ba"));
        assert!(!fnmatch("*a", "b"));
    }

    #[test]
    fn test_fnmatch_question() {
        assert!(fnmatch("a?c", "abc"));
        assert!(fnmatch("a?c", "axc"));
        assert!(!fnmatch("a?c", "ac"));
        assert!(!fnmatch("a?c", "abbc"));
        assert!(fnmatch("?", "x"));
        assert!(!fnmatch("?", ""));
    }

    #[test]
    fn test_fnmatch_classes() {
        assert!(fnmatch("[abc]x", "ax"));
        assert!(fnmatch("[abc]x", "cx"));
        assert!(!fnmatch("[abc]x", "dx"));
        assert!(fnmatch("[a-z]oo", "foo"));
        assert!(!fnmatch("[a-z]oo", "Foo"));
        assert!(fnmatch("[!abc]x", "dx"));
        assert!(!fnmatch("[!abc]x", "ax"));
        assert!(fnmatch("[]]x", "]x"));
        assert!(fnmatch("[a-]", "-"));
        assert!(fnmatch("[a-cx-z]*", "x1"));
    }

    #[test]
    fn test_fnmatch_unclosed_bracket() {
        // Unclosed '[' is a literal; the rest of the pattern still applies
        assert!(!fnmatch("[x", "["));
        assert!(fnmatch("[x", "[x"));
        assert!(fnmatch("a[b", "a[b"));
        assert!(!fnmatch("a[b", "a[x"));
        assert!(!fnmatch("a[b", "a["));
        assert!(!fnmatch("a[b", "ab"));
    }

    #[test]
    fn test_origin_matches() {
        let patterns = vec!["http://a.com".into(), "*.b.com".into()];
        assert!(origin_matches("http://a.com", &patterns));
        assert!(origin_matches("sub.b.com", &patterns));
        assert!(!origin_matches("http://c.com", &patterns));
        assert!(origin_matches("whatever", &[("*").to_string()]));
        assert!(!origin_matches("whatever", &[]));
    }

    #[test]
    fn test_method_matches() {
        let allowed = vec!["get".into(), "POST".into()];
        assert!(method_matches("GET", &allowed));
        assert!(method_matches("post", &allowed));
        assert!(!method_matches("DELETE", &allowed));
        assert!(method_matches("DELETE", &["*".to_string()]));
        assert!(!method_matches("DELETE", &[]));
    }

    #[test]
    fn test_resolve_origin_wildcard_mode() {
        assert_eq!(
            resolve_origin(&config(), Some("http://x.com")).as_deref(),
            Some("*")
        );
        assert_eq!(resolve_origin(&config(), None).as_deref(), Some("*"));
    }

    #[test]
    fn test_resolve_origin_allowed_list() {
        let mut c = config();
        c.allowed_origins = vec!["http://a.com".into(), "*.b.com".into()];
        assert_eq!(
            resolve_origin(&c, Some("http://a.com")).as_deref(),
            Some("http://a.com")
        );
        assert_eq!(
            resolve_origin(&c, Some("sub.b.com")).as_deref(),
            Some("sub.b.com")
        );
        assert_eq!(resolve_origin(&c, Some("http://evil.com")), None);
        assert_eq!(resolve_origin(&c, None), None);
    }

    #[test]
    fn test_resolve_origin_disable_checks() {
        let mut c = config();
        c.allowed_origins = vec!["http://a.com".into()];
        c.disable_cors_checks = true;
        assert_eq!(
            resolve_origin(&c, Some("http://not-listed.com")).as_deref(),
            Some("http://not-listed.com")
        );
        assert_eq!(resolve_origin(&c, None).as_deref(), Some("*"));
    }

    #[test]
    fn test_resolve_origin_explicit_wildcard() {
        let mut c = config();
        c.allowed_origins = vec!["*".into()];
        assert_eq!(
            resolve_origin(&c, Some("http://anything.com")).as_deref(),
            Some("*")
        );
    }

    #[test]
    fn test_build_cors_headers_disabled() {
        let mut c = config();
        c.disable_cors_headers = true;
        assert!(build_cors_headers(&c, Some("http://x.com")).is_empty());
    }

    #[test]
    fn test_build_cors_headers_wildcard() {
        let headers = build_cors_headers(&config(), Some("http://anything.com"));
        let map: HashMap<String, String> = headers.into_iter().collect();
        assert_eq!(map["Access-Control-Allow-Origin"], "*");
        assert!(!map.contains_key("Vary"));
        assert_eq!(map["Access-Control-Max-Age"], "86400");
        assert!(map["Access-Control-Allow-Methods"].contains("GET"));
    }

    #[test]
    fn test_build_cors_headers_reflected_origin() {
        let mut c = config();
        c.allowed_origins = vec!["http://trusted.com".into()];
        let headers = build_cors_headers(&c, Some("http://trusted.com"));
        let map: HashMap<String, String> = headers.into_iter().collect();
        assert_eq!(map["Access-Control-Allow-Origin"], "http://trusted.com");
        assert_eq!(map["Vary"], "Origin");
    }

    #[test]
    fn test_build_cors_headers_disallowed_origin() {
        let mut c = config();
        c.allowed_origins = vec!["http://trusted.com".into()];
        assert!(build_cors_headers(&c, Some("http://evil.com")).is_empty());
    }

    fn rule(origins: &[&str], methods: &[&str], max_age: Option<i64>) -> S3CorsRule {
        S3CorsRule {
            allowed_origins: origins.iter().map(|s| s.to_string()).collect(),
            allowed_methods: methods.iter().map(|s| s.to_string()).collect(),
            allowed_headers: vec!["*".to_string()],
            expose_headers: vec!["ETag".to_string()],
            max_age_seconds: max_age,
        }
    }

    #[test]
    fn test_s3_cors_no_origin() {
        assert!(
            build_s3_cors_headers(&[rule(&["*"], &["GET"], None)], None, Some("GET"), None)
                .is_empty()
        );
    }

    #[test]
    fn test_s3_cors_matching_rule() {
        let headers = build_s3_cors_headers(
            &[rule(
                &["http://a.com", "*.b.com"],
                &["GET", "POST"],
                Some(600),
            )],
            Some("sub.b.com"),
            Some("get"),
            None,
        );
        let map: HashMap<String, String> = headers.into_iter().collect();
        assert_eq!(map["Access-Control-Allow-Origin"], "sub.b.com");
        assert_eq!(map["Access-Control-Allow-Methods"], "GET, POST");
        assert_eq!(map["Access-Control-Allow-Headers"], "*");
        assert_eq!(map["Access-Control-Expose-Headers"], "ETag");
        assert_eq!(map["Access-Control-Max-Age"], "600");
        assert_eq!(map["Vary"], "Origin");
    }

    #[test]
    fn test_s3_cors_method_mismatch() {
        assert!(build_s3_cors_headers(
            &[rule(&["*"], &["GET"], None)],
            Some("http://a.com"),
            Some("DELETE"),
            None,
        )
        .is_empty());
    }

    #[test]
    fn test_s3_cors_origin_mismatch() {
        assert!(build_s3_cors_headers(
            &[rule(&["http://a.com"], &["GET"], None)],
            Some("http://other.com"),
            Some("GET"),
            None,
        )
        .is_empty());
    }

    #[test]
    fn test_s3_cors_first_matching_rule_wins() {
        let headers = build_s3_cors_headers(
            &[
                rule(&["nomatch.com"], &["GET"], None),
                rule(&["*"], &["*"], Some(30)),
            ],
            Some("http://a.com"),
            Some("POST"),
            None,
        );
        let map: HashMap<String, String> = headers.into_iter().collect();
        assert_eq!(map["Access-Control-Max-Age"], "30");
    }
}
