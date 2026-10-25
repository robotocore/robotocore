//! AWS service detection from incoming HTTP requests.
//!
//! Port of `src/robotocore/gateway/router.py`.
//!
//! Determines which AWS service a request targets by inspecting, in order:
//! 1. X-Amz-Target header (used by JSON protocol services)
//! 2. URL path patterns (e.g., /2015-03-31/functions for Lambda)
//! 3. Authorization header (credential scope contains service name)
//! 4. X-Amz-Credential query parameter (SigV4 presigned URLs)
//! 5. Host header (e.g., sqs.us-east-1.amazonaws.com)
//! 6. Query string Action parameter
//! 7. Body-based Action detection for unsigned requests
//! 8. Path-style S3 fallback for unsigned GET/HEAD requests

use once_cell::sync::Lazy;
use regex::Regex;
use std::collections::HashMap;

/// Minimal representation of an HTTP request, sufficient for service detection.
///
/// Field names mirror the Starlette `Request` attributes used by the Python
/// implementation (`request.headers`, `request.url.path`, `request.method`,
/// `request.query_params`). Owns its data so it can be built from PyO3
/// bindings, test helpers, or an HTTP server alike.
#[derive(Debug, Clone)]
pub struct AwsRequest {
    pub method: String,
    pub path: String,
    /// Raw query string without the leading `?` (may be empty).
    pub query_string: String,
    /// (name, value) header pairs; lookups are case-insensitive.
    pub headers: Vec<(String, String)>,
}

impl AwsRequest {
    /// Case-insensitive header lookup (first match wins, like HTTP).
    pub fn header(&self, name: &str) -> Option<&str> {
        let lower = name.to_ascii_lowercase();
        self.headers
            .iter()
            .find(|(k, _)| k.to_ascii_lowercase() == lower)
            .map(|(_, v)| v.as_str())
    }

    /// Parsed query parameters (first value per key, matching
    /// `request.query_params.get(...)` semantics).
    pub fn query_params(&self) -> HashMap<String, String> {
        let mut out = HashMap::new();
        for pair in self.query_string.split('&') {
            if pair.is_empty() {
                continue;
            }
            let (k, v) = match pair.find('=') {
                Some(i) => (&pair[..i], &pair[i + 1..]),
                None => (pair, ""),
            };
            // First occurrence wins, like Starlette's get().
            out.entry(percent_decode(k))
                .or_insert_with(|| percent_decode(v));
        }
        out
    }
}

/// Percent-decode a query-string component (also treats `+` as a space).
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hi = bytes[i + 1];
                let lo = bytes[i + 2];
                if let (Some(h), Some(l)) = (hex_val(hi), hex_val(lo)) {
                    out.push(h * 16 + l);
                    i += 3;
                    continue;
                }
                out.push(b'%');
                i += 1;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

// Map of X-Amz-Target prefixes to service names
static TARGET_PREFIX_MAP: Lazy<HashMap<&'static str, &'static str>> = Lazy::new(|| {
    let entries: [(&str, &str); 74] = [
        ("AWSCognitoIdentityProviderService", "cognito-idp"),
        ("AWSCognitoIdentityService", "cognitoidentity"),
        ("AWSStepFunctions", "stepfunctions"),
        ("AWSSupport", "support"),
        ("AmazonSSM", "ssm"),
        ("CertificateManager", "acm"),
        ("AmazonEC2ContainerRegistry", "ecr"),
        ("AmazonEC2ContainerServiceV20141113", "ecs"),
        ("CloudWatchEvents", "events"),
        ("DynamoDB", "dynamodb"),
        ("DynamoDBStreams", "dynamodbstreams"),
        ("Firehose", "firehose"),
        ("Kinesis", "kinesis"),
        ("Logs", "logs"),
        ("monitoring", "cloudwatch"),
        ("OvertureService", "support"),
        ("Route53Domains", "route53domains"),
        ("SageMaker", "sagemaker"),
        ("SecretManager", "secretsmanager"),
        ("secretsmanager", "secretsmanager"),
        ("StarlingDoveService", "config"),
        ("TrentService", "kms"),
        ("WorkspacesService", "workspaces"),
        ("ACMPrivateCA", "acmpca"),
        ("AWS242ServiceCatalogService", "servicecatalog"),
        ("AWSBudgetServiceGateway", "budgets"),
        ("AWSEC2InstanceConnectService", "ec2instanceconnect"),
        ("AWSGlue", "glue"),
        ("AWSIdentityStore", "identitystore"),
        ("AWSInsightsIndexService", "ce"),
        ("AWSMPMeteringService", "meteringmarketplace"),
        ("AWSOrganizationsV20161128", "organizations"),
        ("AWSShield_20160616", "shield"),
        ("AWSSimbaAPIService_v20180301", "fsx"),
        ("AWSWAF_20190729", "wafv2"),
        ("AmazonAthena", "athena"),
        ("AmazonDAXV3", "dax"),
        ("AmazonDMSv20160101", "dms"),
        ("AmazonForecast", "forecast"),
        ("AmazonMemoryDB", "memorydb"),
        ("AmazonPersonalize", "personalize"),
        ("AmazonTimestreamInfluxDB", "timestreaminfluxdb"),
        ("AnyScaleFrontendService", "applicationautoscaling"),
        ("BaldrApiService", "cloudhsmv2"),
        ("CodeBuild_20161006", "codebuild"),
        ("CodeCommit_20150413", "codecommit"),
        ("CodeDeploy_20141006", "codedeploy"),
        ("CodePipeline_20150709", "codepipeline"),
        ("Comprehend_20171127", "comprehend"),
        ("DataPipeline", "datapipeline"),
        ("DirectoryService_20150416", "ds"),
        ("ElasticMapReduce", "emr"),
        ("FmrsService", "datasync"),
        ("KinesisAnalytics_20180523", "kinesisanalyticsv2"),
        ("MediaStore_20170901", "mediastore"),
        ("NetworkFirewall_20201112", "networkfirewall"),
        ("OpenSearchServerless", "opensearchserverless"),
        ("RedshiftData", "redshiftdata"),
        ("RekognitionService", "rekognition"),
        ("Route53AutoNaming_v20170314", "servicediscovery"),
        ("Route53Domains_v20140515", "route53domains"),
        ("SWBExternalService", "ssoadmin"),
        ("ServiceQuotasV20190624", "servicequotas"),
        ("Textract", "textract"),
        // Note: Timestream query and write share the same target prefix.
        // We route to timestreamwrite by default; query ops are handled in
        // route_to_service().
        ("Timestream_20181101", "timestreamwrite"),
        ("TransferService", "transfer"),
        (
            "com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101",
            "cloudtrail",
        ),
        ("AmazonSQS", "sqs"),
        ("AWSEvents", "events"),
        ("GraniteServiceVersion20100801", "cloudwatch"),
        ("SimpleWorkflowService", "swf"),
        ("Route53Resolver", "route53resolver"),
        ("Transcribe", "transcribe"),
        (
            "ResourceGroupsTaggingAPI_20170126",
            "resourcegroupstaggingapi",
        ),
    ];
    entries.iter().map(|(k, v)| (*k, *v)).collect()
});

/// URL path patterns to service names (ordered; first match wins).
fn path_patterns() -> Vec<(Regex, &'static str)> {
    vec![
        (Regex::new(r"^/2014-11-13/functions").unwrap(), "lambda"),
        (Regex::new(r"^/2015-03-31/functions").unwrap(), "lambda"),
        (
            Regex::new(r"^/2021-\d{2}-\d{2}/functions/").unwrap(),
            "lambda",
        ),
        (Regex::new(r"^/2025-11-30/").unwrap(), "lambda"),
        (Regex::new(r"^/2025-12-01/").unwrap(), "lambda"),
        (Regex::new(r"^/2021-01-01/").unwrap(), "opensearch"),
        (Regex::new(r"^/restapis").unwrap(), "apigateway"),
        (Regex::new(r"^/v2/email/").unwrap(), "sesv2"),
        (Regex::new(r"^/v2/").unwrap(), "apigatewayv2"),
        (Regex::new(r"^/v20180820/").unwrap(), "s3control"),
        (Regex::new(r"^/2015-01-01/es/").unwrap(), "es"),
        (Regex::new(r"^/2013-04-01/").unwrap(), "route53"),
        (Regex::new(r"^/2014-11-13/").unwrap(), "logs"),
        (Regex::new(r"^/prod/v\d+/").unwrap(), "kafka"),
        (Regex::new(r"^/prod/").unwrap(), "medialive"),
        (Regex::new(r"^/v1/pipes").unwrap(), "pipes"),
        (Regex::new(r"^/v1/apis").unwrap(), "appsync"),
        (Regex::new(r"^/v1/create").unwrap(), "batch"),
        (Regex::new(r"^/v1/describe").unwrap(), "batch"),
        (Regex::new(r"^/v1/update").unwrap(), "batch"),
        (Regex::new(r"^/v1/delete").unwrap(), "batch"),
        (Regex::new(r"^/v1/register").unwrap(), "batch"),
        (Regex::new(r"^/v1/deregister").unwrap(), "batch"),
        (Regex::new(r"^/v1/submit").unwrap(), "batch"),
        (Regex::new(r"^/v1/list").unwrap(), "batch"),
        (Regex::new(r"^/v1/terminate").unwrap(), "batch"),
        (Regex::new(r"^/v1/cancel").unwrap(), "batch"),
        (Regex::new(r"^/v1/tags").unwrap(), "batch"),
        (Regex::new(r"^/v1/untag").unwrap(), "batch"),
        // Endpoint strategy path-style routes
        (Regex::new(r"^/queue/[a-z0-9-]+/\d+/").unwrap(), "sqs"),
        (
            Regex::new(r"^/opensearch/[a-z0-9-]+/[A-Za-z0-9-]+").unwrap(),
            "opensearch",
        ),
        // The cfnresponse receiver the CustomResource shim PUTs to (moto
        // urls.py:7 `{0}/cloudformation_(?P<region>[^/]+)/cfnresponse`) —
        // unsigned (no credentials ride the shim request), so it must
        // resolve by path before the auth-scope check.
        (
            Regex::new(r"^/cloudformation_[^/]+/cfnresponse").unwrap(),
            "cloudformation",
        ),
    ]
}

static PATH_PATTERNS: Lazy<Vec<(Regex, &'static str)>> = Lazy::new(path_patterns);

// Service name extracted from credential scope in Authorization header
static AUTH_SERVICE_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"Credential=[^/]+/\d{8}/[^/]+/([^/]+)/aws4_request").unwrap());

// Valid S3 bucket name: 3-63 chars, lowercase letters/digits/hyphens/dots
static S3_BUCKET_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^[a-z0-9][a-z0-9\-.]{1,61}[a-z0-9]$").unwrap());

// AWS credential scope service names that differ from Moto backend names
static SERVICE_NAME_ALIASES: Lazy<HashMap<&'static str, &'static str>> = Lazy::new(|| {
    let entries: [(&str, &str); 32] = [
        ("monitoring", "cloudwatch"),
        ("email", "ses"),
        ("states", "stepfunctions"),
        ("elasticmapreduce", "emr"),
        ("tagging", "resourcegroupstaggingapi"),
        ("acm-pca", "acmpca"),
        ("aoss", "opensearchserverless"),
        ("application-autoscaling", "applicationautoscaling"),
        ("aps", "amp"),
        ("aws-marketplace", "meteringmarketplace"),
        ("cloudhsm", "cloudhsmv2"),
        ("connect-campaigns", "connectcampaigns"),
        ("ec2-instance-connect", "ec2instanceconnect"),
        ("elasticfilesystem", "efs"),
        ("elasticloadbalancing", "elbv2"),
        ("emr-containers", "emrcontainers"),
        ("emr-serverless", "emrserverless"),
        ("kinesisanalytics", "kinesisanalyticsv2"),
        ("lex", "lexv2models"),
        ("mobiletargeting", "pinpoint"),
        ("network-firewall", "networkfirewall"),
        ("rds-data", "rdsdata"),
        ("redshift-data", "redshiftdata"),
        ("timestream", "timestreamwrite"),
        ("timestream-influxdb", "timestreaminfluxdb"),
        ("s3express", "s3"),
        ("s3-object-lambda", "s3"),
        ("vpc-lattice", "vpclattice"),
        ("workspaces-web", "workspacesweb"),
        ("sso", "ssoadmin"),
        ("execute-api", "apigatewaymanagementapi"),
        ("servicequotas", "service-quotas"),
    ];
    entries.iter().map(|(k, v)| (*k, *v)).collect()
});

// Timestream Query operations (vs Write ops which are the default)
static TIMESTREAM_QUERY_OPS: Lazy<std::collections::HashSet<&'static str>> = Lazy::new(|| {
    [
        "CancelQuery",
        "CreateScheduledQuery",
        "DeleteScheduledQuery",
        "DescribeAccountSettings",
        "DescribeScheduledQuery",
        "ExecuteScheduledQuery",
        "ListScheduledQueries",
        "PrepareQuery",
        "Query",
        "UpdateAccountSettings",
        "UpdateScheduledQuery",
    ]
    .into_iter()
    .collect()
});

/// Determine the target AWS service from request attributes.
///
/// Returns the service name, or `None` if it cannot be determined.
pub fn route_to_service(request: &AwsRequest) -> Option<String> {
    // 1. Check X-Amz-Target header (JSON protocol services like DynamoDB, KMS, etc.)
    let target = request.header("x-amz-target").unwrap_or("");
    if !target.is_empty() {
        // Target format is "ServiceName.Operation" or "ServiceName_Version.Operation"
        // A valid target MUST contain a dot with a non-empty operation after it.
        if let Some(last_dot) = target.rfind('.') {
            let operation = &target[last_dot + 1..];
            if !operation.is_empty() {
                // Everything before the last dot is the prefix
                let prefix = &target[..last_dot];
                // Strip version suffix (e.g., "DynamoDB_20120810" -> "DynamoDB")
                let base_prefix = prefix.split('_').next().unwrap_or(prefix);

                // Timestream query and write share the same target prefix — disambiguate by op
                if prefix == "Timestream_20181101" && TIMESTREAM_QUERY_OPS.contains(operation) {
                    return Some("timestreamquery".to_string());
                }

                if let Some(svc) = TARGET_PREFIX_MAP.get(prefix) {
                    return Some((*svc).to_string());
                }
                if let Some(svc) = TARGET_PREFIX_MAP.get(base_prefix) {
                    return Some((*svc).to_string());
                }
            }
        }
    }

    let path = &request.path;
    let query = request.query_params();

    // 2. Check URL path patterns (before auth, since some services share signing names)
    // /v2/apis is shared by appsync and apigatewayv2 — disambiguate by auth header
    if path.starts_with("/v2/apis") {
        let auth = request.header("authorization").unwrap_or("");
        if auth_service(auth) == Some("appsync") {
            return Some("appsync".to_string());
        }
        return Some("apigatewayv2".to_string());
    }

    // An exact `POST /tags` is IoT's TagResource wire surface — the caller's
    // signed scope decides (`iot` → IoT; nothing else serves this path, since
    // RGTA's operations ride POST / with X-Amz-Target). Unscoped form posts
    // fall back to resourcegroupstaggingapi's legacy `/tags` surface.
    if path == "/tags" {
        let authz = request.header("authorization").unwrap_or("");
        if let Some(scope) = auth_service(authz) {
            if scope.eq("iot") {
                return Some("iot".to_string());
            }
        }
        // Unscoped /tags keeps the legacy resourcegroupstaggingapi surface.
        return Some("resourcegroupstaggingapi".to_string());
    }
    for (pattern, service) in PATH_PATTERNS.iter() {
        if pattern.is_match(path) {
            // /v1/tags and /v1/untag are shared by Batch, AppSync, Kafka, MQ, and Pinpoint
            // — disambiguate via the service name in the auth credential scope
            if *service == "batch"
                && (path.starts_with("/v1/tags") || path.starts_with("/v1/untag"))
            {
                let auth = request.header("authorization").unwrap_or("");
                if let Some(auth_service) = auth_service(auth) {
                    let resolved = SERVICE_NAME_ALIASES
                        .get(auth_service)
                        .copied()
                        .unwrap_or(auth_service);
                    if matches!(resolved, "appsync" | "kafka" | "mq" | "pinpoint") {
                        return Some(resolved.to_string());
                    }
                }
            }
            return Some((*service).to_string());
        }
    }

    // 3. Check Authorization header for service name in credential scope
    let auth = request.header("authorization").unwrap_or("");
    if let Some(service) = auth_service(auth) {
        let resolved = SERVICE_NAME_ALIASES
            .get(service)
            .copied()
            .unwrap_or(service);
        // ELB Classic and ELBv2 share the signing name 'elasticloadbalancing'.
        // Disambiguate by the API Version query parameter.
        if resolved == "elbv2" && query.get("Version").map(String::as_str) == Some("2012-06-01") {
            return Some("elb".to_string());
        }
        return Some(resolved.to_string());
    }

    // 4. Check X-Amz-Credential query parameter (SigV4 presigned URLs)
    if let Some(credential) = query.get("X-Amz-Credential") {
        // Format: <access-key>/<date>/<region>/<service>/aws4_request
        let parts: Vec<&str> = credential.split('/').collect();
        if parts.len() >= 4 {
            let service = parts[3];
            if service.is_empty() {
                return None;
            }
            return Some(
                SERVICE_NAME_ALIASES
                    .get(service)
                    .copied()
                    .unwrap_or(service)
                    .to_string(),
            );
        }
    }

    // 4b. Check for SigV2 presigned URLs (AWSAccessKeyId + Signature)
    if query_nonempty(&query, "AWSAccessKeyId") && query_nonempty(&query, "Signature") {
        // SigV2 presigned URLs don't encode the service name.
        // Infer from path — S3 is the only service that commonly uses SigV2 presigned URLs.
        return Some("s3".to_string());
    }

    // 4c. Check for SigV2 Authorization header (AWS AKID:signature)
    if auth.starts_with("AWS ") && auth.contains(':') && !auth.contains("AWS4-HMAC-SHA256") {
        // SigV2 header-based auth doesn't encode the service name.
        // S3 is the only service that commonly uses SigV2 auth headers.
        return Some("s3".to_string());
    }

    // 5. Check Host header
    let host = request.header("host").unwrap_or("");
    if host.contains(".s3.") || host.starts_with("s3.") || host.starts_with("s3-") {
        return Some("s3".to_string());
    }
    // Virtual-hosted style with a plain localhost endpoint: mybucket.localhost:4566
    // Only match when the rest of the host is exactly "localhost". Requires no
    // Authorization header — signed requests are caught by step 3, and a non-SigV4
    // auth header (e.g. Bearer) should not silently route to S3.
    let host_no_port = host.split(':').next().unwrap_or(host);
    let (host_subdomain, host_rest) = match host_no_port.find('.') {
        Some(i) => (
            host_no_port[..i].to_string(),
            host_no_port[i + 1..].to_string(),
        ),
        None => (host_no_port.to_string(), String::new()),
    };
    if auth.is_empty() && host_rest == "localhost" && S3_BUCKET_RE.is_match(&host_subdomain) {
        return Some("s3".to_string());
    }
    // SQS endpoint strategy host patterns (robotocore.cloud primary, localstack.cloud alias)
    if host.contains(".queue.localhost.robotocore.cloud")
        || (host.starts_with("sqs.") && host.contains(".localhost.robotocore.cloud"))
        || host.contains(".queue.localhost.localstack.cloud")
        || (host.starts_with("sqs.") && host.contains(".localhost.localstack.cloud"))
    {
        return Some("sqs".to_string());
    }
    // OpenSearch endpoint strategy host patterns (robotocore.cloud primary, localstack.cloud alias)
    if host.contains(".opensearch.localhost.robotocore.cloud")
        || host.contains(".opensearch.localhost.localstack.cloud")
    {
        return Some("opensearch".to_string());
    }

    // 6. Query string action parameter (used by EC2, SQS, SNS, etc.)
    if query_nonempty(&query, "Action") {
        // These services use query protocol with Action parameter
        // The service is in the auth header which we already checked,
        // but as a fallback we can try common patterns
        if path.contains("Queue") || path.contains("queue") {
            return Some("sqs".to_string());
        }
        if path.contains("Topic") || path.contains("topic") {
            return Some("sns".to_string());
        }
    }

    // 7. Body-based Action detection for unsigned requests
    // Some STS operations (AssumeRoleWithWebIdentity, AssumeRoleWithSAML)
    // don't include an Authorization header.
    let content_type = request.header("content-type").unwrap_or("");
    if content_type.contains("x-www-form-urlencoded") && auth.is_empty() {
        return Some("sts".to_string());
    }

    // 8. Path-style S3 fallback for unsigned/anonymous GET/HEAD requests.
    // Handles: GET http://localhost:4566/bucket/key.json (public bucket access)
    // Restricted to GET/HEAD (the actual public-access shape) and no auth header
    // so that genuine typos and non-S3 services still get a clean 400.
    // Must come last — all other service patterns are more specific.
    if auth.is_empty()
        && (request.method == "GET" || request.method == "HEAD")
        && path != "/"
        && !path.starts_with("/_")
    {
        let bucket_candidate = path.trim_start_matches('/').split('/').next().unwrap_or("");
        if S3_BUCKET_RE.is_match(bucket_candidate) {
            return Some("s3".to_string());
        }
    }

    None
}

/// Extract the service name from the credential scope in an Authorization header.
fn auth_service(auth: &str) -> Option<&str> {
    AUTH_SERVICE_RE
        .captures(auth)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str())
}

/// True if the query param exists with a non-empty value (matches Python truthiness).
fn query_nonempty(query: &HashMap<String, String>, key: &str) -> bool {
    query.get(key).map(|v| !v.is_empty()).unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(method: &str, path: &str, query: &str, headers: &[(&str, &str)]) -> AwsRequest {
        AwsRequest {
            method: method.to_string(),
            path: path.to_string(),
            query_string: query.to_string(),
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    const SIGV4: &str =
        "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20240101/us-east-1/sqs/aws4_request, SignedHeaders=host, Signature=abc";

    // ---- X-Amz-Target routing ----

    #[test]
    fn target_dynamodb() {
        let r = req(
            "POST",
            "/",
            "",
            &[("X-Amz-Target", "DynamoDB_20120810.GetItem")],
        );
        assert_eq!(route_to_service(&r).as_deref(), Some("dynamodb"));
    }

    #[test]
    fn target_dynamodb_streams() {
        let r = req(
            "POST",
            "/",
            "",
            &[("X-Amz-Target", "DynamoDBStreams_20120810.GetShardIterator")],
        );
        assert_eq!(route_to_service(&r).as_deref(), Some("dynamodbstreams"));
    }

    #[test]
    fn target_kms() {
        let r = req(
            "POST",
            "/",
            "",
            &[("X-Amz-Target", "TrentService.CreateKey")],
        );
        assert_eq!(route_to_service(&r).as_deref(), Some("kms"));
    }

    #[test]
    fn target_cognito() {
        let r = req(
            "POST",
            "/",
            "",
            &[("X-Amz-Target", "AWSCognitoIdentityProviderService.SignUp")],
        );
        assert_eq!(route_to_service(&r).as_deref(), Some("cognito-idp"));
    }

    #[test]
    fn target_timestream_query_op() {
        let r = req(
            "POST",
            "/",
            "",
            &[("X-Amz-Target", "Timestream_20181101.Query")],
        );
        assert_eq!(route_to_service(&r).as_deref(), Some("timestreamquery"));
    }

    #[test]
    fn target_timestream_write_op() {
        let r = req(
            "POST",
            "/",
            "",
            &[("X-Amz-Target", "Timestream_20181101.WriteRecords")],
        );
        assert_eq!(route_to_service(&r).as_deref(), Some("timestreamwrite"));
    }

    #[test]
    fn target_no_dot_is_malformed() {
        let r = req("POST", "/", "", &[("X-Amz-Target", "DynamoDB")]);
        // No dot -> no operation -> falls through to other checks -> None
        assert_eq!(route_to_service(&r).as_deref(), None);
    }

    #[test]
    fn target_trailing_dot_is_malformed() {
        let r = req("POST", "/", "", &[("X-Amz-Target", "DynamoDB_20120810.")]);
        assert_eq!(route_to_service(&r).as_deref(), None);
    }

    #[test]
    fn target_dotted_cloudtrail() {
        let r = req(
            "POST",
            "/",
            "",
            &[(
                "X-Amz-Target",
                "com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.LookupEvents",
            )],
        );
        assert_eq!(route_to_service(&r).as_deref(), Some("cloudtrail"));
    }

    #[test]
    fn target_base_prefix_fallback() {
        // Versioned prefix not in map, but base prefix is
        let r = req(
            "POST",
            "/",
            "",
            &[("X-Amz-Target", "DynamoDB_20120810.PutItem")],
        );
        assert_eq!(route_to_service(&r).as_deref(), Some("dynamodb"));
    }

    // ---- Path routing ----

    #[test]
    fn path_lambda_2015() {
        let r = req("POST", "/2015-03-31/functions", "", &[]);
        assert_eq!(route_to_service(&r).as_deref(), Some("lambda"));
    }

    #[test]
    fn path_lambda_2021() {
        let r = req("GET", "/2021-04-19/functions/my-fn", "", &[]);
        assert_eq!(route_to_service(&r).as_deref(), Some("lambda"));
    }

    #[test]
    fn path_apigateway() {
        let r = req("GET", "/restapis", "", &[]);
        assert_eq!(route_to_service(&r).as_deref(), Some("apigateway"));
    }

    #[test]
    fn path_sesv2() {
        let r = req("POST", "/v2/email/outbound", "", &[]);
        assert_eq!(route_to_service(&r).as_deref(), Some("sesv2"));
    }

    #[test]
    fn path_apigatewayv2() {
        let r = req("GET", "/v2/apps", "", &[]);
        assert_eq!(route_to_service(&r).as_deref(), Some("apigatewayv2"));
    }

    #[test]
    fn path_s3control() {
        let r = req("GET", "/v20180820/account", "", &[]);
        assert_eq!(route_to_service(&r).as_deref(), Some("s3control"));
    }

    #[test]
    fn path_es() {
        let r = req("GET", "/2015-01-01/es/domain", "", &[]);
        assert_eq!(route_to_service(&r).as_deref(), Some("es"));
    }

    #[test]
    fn path_route53() {
        let r = req("GET", "/2013-04-01/hostedzone", "", &[]);
        assert_eq!(route_to_service(&r).as_deref(), Some("route53"));
    }

    #[test]
    fn path_logs() {
        let r = req("POST", "/2014-11-13/log-groups", "", &[]);
        assert_eq!(route_to_service(&r).as_deref(), Some("logs"));
    }

    #[test]
    fn path_tags_tagging_api() {
        // Unscoped GET /tags keeps the legacy resourcegroupstaggingapi
        // surface (a tagged caller with scope iot takes the IoT TagResource
        // route instead — see the /tags scope check above).
        let r = req("GET", "/tags", "", &[]);
        assert_eq!(
            route_to_service(&r).as_deref(),
            Some("resourcegroupstaggingapi")
        );
    }

    #[test]
    fn path_tags_iot_scope() {
        let r = req(
            "POST",
            "/tags",
            "",
            &[(
                "Authorization",
                "AWS4-HMAC-SHA256 Credential=123456789012/20260101/us-east-1/iot/aws4_request, SignedHeaders=host, Signature=abc",
            )],
        );
        assert_eq!(route_to_service(&r).as_deref(), Some("iot"));
    }

    #[test]
    fn path_kafka() {
        let r = req("POST", "/prod/v2/admin", "", &[]);
        assert_eq!(route_to_service(&r).as_deref(), Some("kafka"));
    }

    #[test]
    fn path_medialive() {
        let r = req("GET", "/prod/channels", "", &[]);
        assert_eq!(route_to_service(&r).as_deref(), Some("medialive"));
    }

    #[test]
    fn path_sqs_endpoint_strategy() {
        let r = req("GET", "/queue/my-queue/123456789012/messages", "", &[]);
        assert_eq!(route_to_service(&r).as_deref(), Some("sqs"));
    }

    #[test]
    fn path_opensearch_endpoint_strategy() {
        let r = req("GET", "/opensearch/my-domain/abc123", "", &[]);
        assert_eq!(route_to_service(&r).as_deref(), Some("opensearch"));
    }

    // ---- /v2/apis disambiguation ----

    #[test]
    fn v2_apis_appsync_auth() {
        let auth = "AWS4-HMAC-SHA256 Credential=AKIA/20240101/us-east-1/appsync/aws4_request";
        let r = req("GET", "/v2/apis", "", &[("authorization", auth)]);
        assert_eq!(route_to_service(&r).as_deref(), Some("appsync"));
    }

    #[test]
    fn v2_apis_default_apigatewayv2() {
        let r = req("GET", "/v2/apis", "", &[]);
        assert_eq!(route_to_service(&r).as_deref(), Some("apigatewayv2"));
    }

    // ---- Batch /v1/tags disambiguation ----

    #[test]
    fn batch_v1_tags_default() {
        let r = req("POST", "/v1/tags/job", "", &[]);
        assert_eq!(route_to_service(&r).as_deref(), Some("batch"));
    }

    #[test]
    fn v1_tags_appsync_auth() {
        let auth = "AWS4-HMAC-SHA256 Credential=AKIA/20240101/us-east-1/appsync/aws4_request";
        let r = req("POST", "/v1/tags/job", "", &[("authorization", auth)]);
        assert_eq!(route_to_service(&r).as_deref(), Some("appsync"));
    }

    #[test]
    fn v1_untag_pinpoint_auth() {
        let auth =
            "AWS4-HMAC-SHA256 Credential=AKIA/20240101/us-east-1/mobiletargeting/aws4_request";
        let r = req("POST", "/v1/untag/app", "", &[("authorization", auth)]);
        assert_eq!(route_to_service(&r).as_deref(), Some("pinpoint"));
    }

    // ---- Authorization credential scope ----

    #[test]
    fn auth_sqs() {
        let r = req(
            "POST",
            "/",
            "Action=SendMessage",
            &[("authorization", SIGV4)],
        );
        assert_eq!(route_to_service(&r).as_deref(), Some("sqs"));
    }

    #[test]
    fn auth_alias_monitoring_cloudwatch() {
        let auth = "AWS4-HMAC-SHA256 Credential=AKIA/20240101/us-east-1/monitoring/aws4_request";
        let r = req("POST", "/", "", &[("authorization", auth)]);
        assert_eq!(route_to_service(&r).as_deref(), Some("cloudwatch"));
    }

    #[test]
    fn auth_alias_states_stepfunctions() {
        let auth = "AWS4-HMAC-SHA256 Credential=AKIA/20240101/us-east-1/states/aws4_request";
        let r = req("POST", "/", "", &[("authorization", auth)]);
        assert_eq!(route_to_service(&r).as_deref(), Some("stepfunctions"));
    }

    #[test]
    fn auth_elbv2_default() {
        let auth =
            "AWS4-HMAC-SHA256 Credential=AKIA/20240101/us-east-1/elasticloadbalancing/aws4_request";
        let r = req(
            "POST",
            "/",
            "Action=DescribeLoadBalancers",
            &[("authorization", auth)],
        );
        assert_eq!(route_to_service(&r).as_deref(), Some("elbv2"));
    }

    #[test]
    fn auth_elb_classic_version() {
        let auth =
            "AWS4-HMAC-SHA256 Credential=AKIA/20240101/us-east-1/elasticloadbalancing/aws4_request";
        let r = req(
            "POST",
            "/",
            "Action=DescribeLoadBalancers&Version=2012-06-01",
            &[("authorization", auth)],
        );
        assert_eq!(route_to_service(&r).as_deref(), Some("elb"));
    }

    // ---- Host header S3 ----

    #[test]
    fn host_s3_virtual() {
        let r = req(
            "GET",
            "/key.txt",
            "",
            &[("host", "my-bucket.s3.us-east-1.amazonaws.com")],
        );
        assert_eq!(route_to_service(&r).as_deref(), Some("s3"));
    }

    #[test]
    fn host_s3_prefix() {
        let r = req(
            "GET",
            "/key.txt",
            "",
            &[("host", "s3.us-east-1.amazonaws.com")],
        );
        assert_eq!(route_to_service(&r).as_deref(), Some("s3"));
    }

    #[test]
    fn host_s3_website() {
        let r = req(
            "GET",
            "/key.txt",
            "",
            &[("host", "s3-website.us-east-1.amazonaws.com")],
        );
        assert_eq!(route_to_service(&r).as_deref(), Some("s3"));
    }

    #[test]
    fn host_localhost_bucket_s3() {
        // mybucket.localhost:4566 with no auth -> S3
        let r = req(
            "GET",
            "/key.txt",
            "",
            &[("host", "my-bucket.localhost:4566")],
        );
        assert_eq!(route_to_service(&r).as_deref(), Some("s3"));
    }

    #[test]
    fn host_localhost_bucket_with_auth_not_s3() {
        // Has auth -> caught by other checks (none match) -> not silently S3
        let r = req(
            "GET",
            "/key.txt",
            "",
            &[
                ("host", "my-bucket.localhost:4566"),
                ("authorization", SIGV4),
            ],
        );
        assert_eq!(route_to_service(&r).as_deref(), Some("sqs"));
    }

    // ---- Host header SQS / OpenSearch endpoint strategy ----

    #[test]
    fn host_sqs_endpoint() {
        let r = req(
            "GET",
            "/",
            "",
            &[("host", "my-queue.queue.localhost.robotocore.cloud")],
        );
        assert_eq!(route_to_service(&r).as_deref(), Some("sqs"));
    }

    #[test]
    fn host_opensearch_endpoint() {
        let r = req(
            "GET",
            "/",
            "",
            &[("host", "my-domain.opensearch.localhost.robotocore.cloud")],
        );
        assert_eq!(route_to_service(&r).as_deref(), Some("opensearch"));
    }

    // ---- SigV4 presigned (X-Amz-Credential) ----

    #[test]
    fn presigned_v4_s3() {
        let r = req(
            "GET",
            "/my-bucket/key.txt",
            "X-Amz-Credential=AKIA/20240101/us-east-1/s3/aws4_request",
            &[],
        );
        assert_eq!(route_to_service(&r).as_deref(), Some("s3"));
    }

    #[test]
    fn presigned_v4_alias_s3express() {
        let r = req(
            "GET",
            "/b--x-s3/key",
            "X-Amz-Credential=AKIA/20240101/us-east-1/s3express/aws4_request",
            &[],
        );
        assert_eq!(route_to_service(&r).as_deref(), Some("s3"));
    }

    // ---- SigV2 ----

    #[test]
    fn sigv2_presigned_s3() {
        let r = req(
            "GET",
            "/my-bucket/key.txt",
            "AWSAccessKeyId=AKIA&Signature=abc",
            &[],
        );
        assert_eq!(route_to_service(&r).as_deref(), Some("s3"));
    }

    #[test]
    fn sigv2_header_s3() {
        let r = req(
            "GET",
            "/key.txt",
            "",
            &[("authorization", "AWS AKIA:abcdef")],
        );
        assert_eq!(route_to_service(&r).as_deref(), Some("s3"));
    }

    // ---- STS unsigned form ----

    #[test]
    fn sts_unsigned_form() {
        let r = req(
            "POST",
            "/",
            "Action=GetCallerIdentity",
            &[("content-type", "application/x-www-form-urlencoded")],
        );
        assert_eq!(route_to_service(&r).as_deref(), Some("sts"));
    }

    // ---- Path-style S3 fallback ----

    #[test]
    fn path_style_s3_get() {
        let r = req("GET", "/my-bucket/key.json", "", &[]);
        assert_eq!(route_to_service(&r).as_deref(), Some("s3"));
    }

    #[test]
    fn path_style_s3_head() {
        let r = req("HEAD", "/my-bucket/key.json", "", &[]);
        assert_eq!(route_to_service(&r).as_deref(), Some("s3"));
    }

    #[test]
    fn path_style_s3_post_not_matched() {
        // POST is not GET/HEAD -> not the public bucket shape
        let r = req("POST", "/my-bucket/key.json", "", &[]);
        assert_eq!(route_to_service(&r).as_deref(), None);
    }

    #[test]
    fn root_path_not_s3() {
        let r = req("GET", "/", "", &[]);
        assert_eq!(route_to_service(&r).as_deref(), None);
    }

    #[test]
    fn internal_path_not_s3() {
        let r = req("GET", "/_robotocore/health", "", &[]);
        assert_eq!(route_to_service(&r).as_deref(), None);
    }

    // ---- Unknown ----

    #[test]
    fn unknown_request() {
        // "ab" is too short to be a bucket name; no other rule matches
        let r = req("GET", "/ab/cd", "", &[]);
        assert_eq!(route_to_service(&r).as_deref(), None);
    }

    #[test]
    fn bucket_like_path_falls_to_s3() {
        // Parity with Python: any path whose first segment looks like a bucket
        // name and has no auth routes to S3 as the last-resort fallback.
        let r = req("GET", "/some/random/path", "", &[]);
        assert_eq!(route_to_service(&r).as_deref(), Some("s3"));
    }

    #[test]
    fn percent_decode_basic() {
        assert_eq!(percent_decode("hello%20world"), "hello world");
        assert_eq!(percent_decode("a+b"), "a b");
        assert_eq!(percent_decode("100%"), "100%");
        assert_eq!(percent_decode("%41%42"), "AB");
    }
}
