//! Robotocore Rust - High-performance AWS API mock server
//!
//! This module provides Rust implementations of robotocore components,
//! gradually replacing Python implementations for better performance.
//!
//! Currently ported components:
//! - S3 virtual-hosted routing (`s3_routing`)
//! - AWS service detection / request routing (`router`)
//! - CORS header handling (`cors`)

pub mod cors;
pub mod router;
pub mod s3_routing;

pub use cors::{
    build_cors_headers, build_s3_cors_headers, fnmatch, method_matches, origin_matches, parse_csv,
    resolve_origin, CorsConfig, S3CorsRule,
};
pub use router::{route_to_service, AwsRequest};
pub use s3_routing::{
    get_s3_routing_config, is_s3_vhost_request, parse_s3_vhost, rewrite_vhost_to_path,
    S3RoutingConfig, S3VhostInfo, Scope,
};

pub mod core;

#[cfg(test)]
mod tests {
    use super::*;

    fn hdr(k: &str, v: &str) -> (String, String) {
        (k.to_string(), v.to_string())
    }

    #[test]
    fn test_route_dynamodb_target() {
        let req = AwsRequest {
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: vec![hdr("x-amz-target", "DynamoDB_20120810.GetItem")],
        };
        assert_eq!(route_to_service(&req).as_deref(), Some("dynamodb"));
    }

    #[test]
    fn test_route_lambda_path() {
        let req = AwsRequest {
            method: "POST".to_string(),
            path: "/2015-03-31/functions".to_string(),
            query_string: String::new(),
            headers: vec![hdr(
                "authorization",
                "AWS4-HMAC-SHA256 Credential=AKIA/20240101/us-east-1/lambda/aws4_request",
            )],
        };
        assert_eq!(route_to_service(&req).as_deref(), Some("lambda"));
    }

    #[test]
    fn test_route_s3_host() {
        let req = AwsRequest {
            method: "GET".to_string(),
            path: "/my-bucket/key.txt".to_string(),
            query_string: String::new(),
            headers: vec![hdr("host", "my-bucket.s3.us-east-1.amazonaws.com")],
        };
        assert_eq!(route_to_service(&req).as_deref(), Some("s3"));
    }
}
