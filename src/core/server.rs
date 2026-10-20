//! Axum-based HTTP server for the Robotocore Rust implementation.
//!
//! Implements catch-all routing, protocol detection, and service dispatch.

use axum::{
    extract::State,
    http::{HeaderMap, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Router,
};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};
use uuid::Uuid;

use crate::core::account::parse_account_from_key;
use crate::core::protocol::{
    extract_operation, parse_query_protocol, ParsedRequest, ParsedResponse,
};
use crate::core::services::sts;
use crate::router::{route_to_service, AwsRequest};
use http::header::HeaderValue;
use sqs::SqsHandler;

/// Service handler trait for AWS services.
pub trait ServiceHandler: Send + Sync {
    /// Handle a parsed request and return a response.
    fn handle_sync(
        &self,
        req: &ParsedRequest,
    ) -> Result<ParsedResponse, Box<dyn std::error::Error>>;
}

/// Synchronous wrapper for async STS handler
pub struct StsFunctionHandler;

impl ServiceHandler for StsFunctionHandler {
    fn handle_sync(
        &self,
        req: &ParsedRequest,
    ) -> Result<ParsedResponse, Box<dyn std::error::Error>> {
        // `handle_sts_request` is async but has no awaits; poll it to
        // completion on a local waker without touching the current runtime.
        use std::future::Future;
        use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

        fn noop_raw() -> RawWaker {
            fn clone(_: *const ()) -> RawWaker {
                RawWaker::new(null_ptr(), &VTABLE)
            }
            fn wakeup(_: *const ()) {}
            static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wakeup, wakeup, wakeup);
            RawWaker::new(null_ptr(), &VTABLE)
        }
        fn null_ptr() -> *const () {
            std::ptr::null()
        }

        let waker = unsafe { Waker::from_raw(noop_raw()) };
        let mut cx = Context::from_waker(&waker);
        let mut fut = std::pin::pin!(sts::handle_sts_request(req));
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(result) => result,
            Poll::Pending => Err("STS handler unexpectedly pending".into()),
        }
    }
}

/// Adapter that bridges the core `ParsedRequest`/`ParsedResponse` protocol to
/// the native SQS service crate.
pub struct SqsServiceHandler {
    inner: Arc<sqs::DefaultSqsHandler>,
}

impl SqsServiceHandler {
    fn to_sqs_request(req: &ParsedRequest) -> sqs::protocol::AwsRequest {
        let params: serde_json::Value = serde_json::to_value(&req.params)
            .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));
        sqs::protocol::AwsRequest {
            service: req.service.clone(),
            operation: req.operation.clone(),
            account: req.account,
            region: req.region.clone(),
            params,
            body: req.body.clone(),
        }
    }

    fn to_parsed_response(resp: sqs::protocol::AwsResponse) -> ParsedResponse {
        let mut headers = std::collections::HashMap::new();
        for (k, v) in resp.headers {
            headers.insert(k, v);
        }
        ParsedResponse {
            status: StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            headers,
            body: serde_json::Value::Null,
            raw: Some(resp.body),
            raw_bytes: None,
        }
    }
}

impl ServiceHandler for SqsServiceHandler {
    fn handle_sync(
        &self,
        req: &ParsedRequest,
    ) -> Result<ParsedResponse, Box<dyn std::error::Error>> {
        let sqs_req = Self::to_sqs_request(req);
        let resp = self.inner.handle(sqs_req);
        Ok(Self::to_parsed_response(resp))
    }
}

/// Adapter that bridges the core protocol to the native S3 service crate.
pub struct S3ServiceHandler {
    inner: Arc<s3::DefaultS3Handler>,
}

impl S3ServiceHandler {
    fn to_s3_request(
        req: &ParsedRequest,
        method: &str,
        query_string: &str,
    ) -> s3::protocol::AwsRequest {
        // Extract bucket and key from the path
        let path = &req.path;
        let path_parts: Vec<&str> = path.trim_start_matches('/').splitn(2, '/').collect();
        let bucket = path_parts
            .first()
            .map(|s| s.to_string())
            .filter(|s| !s.is_empty());
        let key = path_parts
            .get(1)
            .map(|s| s.to_string())
            .filter(|s| !s.is_empty());

        // Parse query params
        let mut query_params = std::collections::HashMap::new();
        for pair in query_string.split('&') {
            if let Some((k, v)) = pair.split_once('=') {
                query_params.insert(
                    urlencoding::decode(k).unwrap_or_default().into_owned(),
                    urlencoding::decode(v).unwrap_or_default().into_owned(),
                );
            }
        }

        // Use the headers map directly
        let headers = req.headers.clone();

        s3::protocol::AwsRequest {
            service: req.service.clone(),
            operation: req.operation.clone(),
            account: req.account,
            region: req.region.clone(),
            bucket,
            key,
            query_params,
            headers,
            method: method.to_string(),
            body: req.body.clone(),
            params: serde_json::to_value(&req.params).unwrap_or_default(),
        }
    }

    fn to_parsed_response(resp: s3::protocol::AwsResponse) -> ParsedResponse {
        let mut headers = std::collections::HashMap::new();
        for (k, v) in resp.headers {
            headers.insert(k, v);
        }
        // S3 bodies are arbitrary bytes; carry them losslessly via raw_bytes
        // (lossy UTF-8 re-encoding corrupted binary objects in transit).
        ParsedResponse {
            status: StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            headers,
            body: serde_json::Value::Null,
            raw: None,
            raw_bytes: Some(resp.body),
        }
    }
}

impl ServiceHandler for S3ServiceHandler {
    fn handle_sync(
        &self,
        req: &ParsedRequest,
    ) -> Result<ParsedResponse, Box<dyn std::error::Error>> {
        // Detect the S3 operation from method + path + query
        let mut query_params = std::collections::HashMap::new();
        for pair in req.query_string.split('&') {
            if pair.is_empty() {
                continue;
            }
            if let Some((k, v)) = pair.split_once('=') {
                query_params.insert(
                    urlencoding::decode(k).unwrap_or_default().into_owned(),
                    urlencoding::decode(v).unwrap_or_default().into_owned(),
                );
            } else {
                // Value-less query param (e.g. ?policy, ?cors, ?lifecycle)
                query_params.insert(
                    urlencoding::decode(pair).unwrap_or_default().into_owned(),
                    String::new(),
                );
            }
        }
        let operation =
            s3::handler::S3Handler::detect_s3_operation(&req.method, &req.path, &query_params)
                .unwrap_or_else(|| req.operation.clone());

        let mut s3_req = Self::to_s3_request(req, &req.method, &req.query_string);
        s3_req.operation = operation;
        let resp = self.inner.handle(s3_req);
        Ok(Self::to_parsed_response(resp))
    }
}

/// Adapter that bridges the core protocol to the native DynamoDB service crate.
pub struct DynamoDbServiceHandler {
    inner: Arc<dynamodb::DefaultDynamoDbHandler>,
}

impl DynamoDbServiceHandler {
    fn to_dynamo_req(req: &ParsedRequest) -> dynamodb::protocol::AwsRequest {
        let params = serde_json::to_value(&req.params).unwrap_or_default();
        dynamodb::protocol::AwsRequest {
            service: req.service.clone(),
            operation: req.operation.clone(),
            account: req.account,
            region: req.region.clone(),
            params,
            body: req.body.clone(),
        }
    }

    fn to_parsed_response(resp: dynamodb::protocol::AwsResponse) -> ParsedResponse {
        let mut headers = std::collections::HashMap::new();
        for (k, v) in resp.headers {
            headers.insert(k, v);
        }
        ParsedResponse {
            status: StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            headers,
            body: serde_json::Value::Null,
            raw: Some(resp.body),
            raw_bytes: None,
        }
    }
}

impl ServiceHandler for DynamoDbServiceHandler {
    fn handle_sync(
        &self,
        req: &ParsedRequest,
    ) -> Result<ParsedResponse, Box<dyn std::error::Error>> {
        let ddb_req = Self::to_dynamo_req(req);
        let resp = self.inner.handle(ddb_req);
        Ok(Self::to_parsed_response(resp))
    }
}

/// Adapter that bridges the core protocol to the native SNS service crate.
pub struct SnsServiceHandler {
    inner: Arc<sns::DefaultSnsHandler>,
}

impl SnsServiceHandler {
    fn to_sns_request(req: &ParsedRequest) -> sns::protocol::AwsRequest {
        let params = serde_json::to_value(&req.params).unwrap_or_default();
        sns::protocol::AwsRequest {
            service: req.service.clone(),
            operation: req.operation.clone(),
            account: req.account,
            region: req.region.clone(),
            params,
            body: req.body.clone(),
        }
    }

    fn to_parsed_response(resp: sns::protocol::AwsResponse) -> ParsedResponse {
        let mut headers = std::collections::HashMap::new();
        for (k, v) in resp.headers {
            headers.insert(k, v);
        }
        ParsedResponse {
            status: StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            headers,
            body: serde_json::Value::Null,
            raw: Some(resp.body),
            raw_bytes: None,
        }
    }
}

impl ServiceHandler for SnsServiceHandler {
    fn handle_sync(
        &self,
        req: &ParsedRequest,
    ) -> Result<ParsedResponse, Box<dyn std::error::Error>> {
        let sns_req = Self::to_sns_request(req);
        let resp = self.inner.handle(sns_req);
        Ok(Self::to_parsed_response(resp))
    }
}

/// Adapter that bridges the core protocol to the native Secrets Manager service crate.
pub struct SmServiceHandler {
    inner: Arc<secretsmanager::DefaultSecretsManagerHandler>,
}

impl SmServiceHandler {
    fn to_sm_request(req: &ParsedRequest) -> secretsmanager::protocol::AwsRequest {
        let params = serde_json::to_value(&req.params).unwrap_or_default();
        secretsmanager::protocol::AwsRequest {
            service: req.service.clone(),
            operation: req.operation.clone(),
            account: req.account,
            region: req.region.clone(),
            params,
            body: req.body.clone(),
        }
    }

    fn to_parsed_response(resp: secretsmanager::protocol::AwsResponse) -> ParsedResponse {
        let mut headers = std::collections::HashMap::new();
        for (k, v) in resp.headers {
            headers.insert(k, v);
        }
        ParsedResponse {
            status: StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            headers,
            body: serde_json::Value::Null,
            raw: Some(resp.body),
            raw_bytes: None,
        }
    }
}

impl ServiceHandler for SmServiceHandler {
    fn handle_sync(
        &self,
        req: &ParsedRequest,
    ) -> Result<ParsedResponse, Box<dyn std::error::Error>> {
        let sm_req = Self::to_sm_request(req);
        let resp = self.inner.handle(sm_req);
        Ok(Self::to_parsed_response(resp))
    }
}

/// Adapter for KMS service crate.
pub struct KmsServiceHandler {
    inner: Arc<kms::DefaultKmsHandler>,
}

impl ServiceHandler for KmsServiceHandler {
    fn handle_sync(
        &self,
        req: &ParsedRequest,
    ) -> Result<ParsedResponse, Box<dyn std::error::Error>> {
        let params = serde_json::to_value(&req.params).unwrap_or_default();
        let kms_req = kms::protocol::AwsRequest {
            service: req.service.clone(),
            operation: req.operation.clone(),
            account: req.account,
            region: req.region.clone(),
            params,
            body: req.body.clone(),
        };
        let resp = self.inner.handle(kms_req);
        let mut headers = std::collections::HashMap::new();
        for (k, v) in resp.headers {
            headers.insert(k, v);
        }
        Ok(ParsedResponse {
            status: StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            headers,
            body: serde_json::Value::Null,
            raw: Some(resp.body),
            raw_bytes: None,
        })
    }
}

/// Adapter for SSM service crate.
pub struct SsmServiceHandler {
    inner: Arc<ssm::DefaultSsmHandler>,
}

impl ServiceHandler for SsmServiceHandler {
    fn handle_sync(
        &self,
        req: &ParsedRequest,
    ) -> Result<ParsedResponse, Box<dyn std::error::Error>> {
        let params = serde_json::to_value(&req.params).unwrap_or_default();
        let ssm_req = ssm::protocol::AwsRequest {
            service: req.service.clone(),
            operation: req.operation.clone(),
            account: req.account,
            region: req.region.clone(),
            params,
            body: req.body.clone(),
        };
        let resp = self.inner.handle(ssm_req);
        let mut headers = std::collections::HashMap::new();
        for (k, v) in resp.headers {
            headers.insert(k, v);
        }
        Ok(ParsedResponse {
            status: StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            headers,
            body: serde_json::Value::Null,
            raw: Some(resp.body),
            raw_bytes: None,
        })
    }
}

/// Adapter for IAM service crate.
pub struct IamServiceHandler {
    inner: Arc<iam::DefaultIamHandler>,
}

impl ServiceHandler for IamServiceHandler {
    fn handle_sync(
        &self,
        req: &ParsedRequest,
    ) -> Result<ParsedResponse, Box<dyn std::error::Error>> {
        let params = serde_json::to_value(&req.params).unwrap_or_default();
        let iam_req = iam::protocol::AwsRequest {
            service: req.service.clone(),
            operation: req.operation.clone(),
            account: req.account,
            region: req.region.clone(),
            params,
            body: req.body.clone(),
            query: req.query_string.clone(),
        };
        let resp = self.inner.handle(iam_req);
        let mut headers = std::collections::HashMap::new();
        for (k, v) in resp.headers {
            headers.insert(k, v);
        }
        Ok(ParsedResponse {
            status: StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            headers,
            body: serde_json::Value::Null,
            raw: Some(resp.body),
            raw_bytes: None,
        })
    }
}

/// Adapter for Lambda service crate.
pub struct LambdaServiceHandler {
    inner: Arc<lambda::DefaultLambdaHandler>,
}

impl ServiceHandler for LambdaServiceHandler {
    fn handle_sync(
        &self,
        req: &ParsedRequest,
    ) -> Result<ParsedResponse, Box<dyn std::error::Error>> {
        let params = serde_json::to_value(&req.params).unwrap_or_default();
        let lambda_req = lambda::protocol::AwsRequest {
            service: req.service.clone(),
            operation: req.operation.clone(),
            account: req.account,
            region: req.region.clone(),
            params,
            body: req.body.clone(),
            method: req.method.clone(),
            path: req.path.clone(),
            query_string: req.query_string.clone(),
            headers: req.headers.clone(),
        };
        let resp = self.inner.handle(lambda_req);
        let mut headers = std::collections::HashMap::new();
        for (k, v) in resp.headers {
            headers.insert(k, v);
        }
        Ok(ParsedResponse {
            status: StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            headers,
            body: serde_json::Value::Null,
            raw: Some(resp.body),
            raw_bytes: None,
        })
    }
}

/// Adapter for CloudWatch Logs service crate.
pub struct LogsServiceHandler {
    inner: Arc<cloudwatch_logs::DefaultLogsHandler>,
}

impl ServiceHandler for LogsServiceHandler {
    fn handle_sync(
        &self,
        req: &ParsedRequest,
    ) -> Result<ParsedResponse, Box<dyn std::error::Error>> {
        let params = serde_json::to_value(&req.params).unwrap_or_default();
        let logs_req = cloudwatch_logs::protocol::AwsRequest {
            service: req.service.clone(),
            operation: req.operation.clone(),
            account: req.account,
            region: req.region.clone(),
            params,
            body: req.body.clone(),
        };
        let resp = self.inner.handle(logs_req);
        let mut headers = std::collections::HashMap::new();
        for (k, v) in resp.headers {
            headers.insert(k, v);
        }
        Ok(ParsedResponse {
            status: StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            headers,
            body: serde_json::Value::Null,
            raw: Some(resp.body),
            raw_bytes: None,
        })
    }
}

/// Adapter for EventBridge service crate.
pub struct EventsServiceHandler {
    inner: Arc<events::DefaultEventsHandler>,
}

impl ServiceHandler for EventsServiceHandler {
    fn handle_sync(
        &self,
        req: &ParsedRequest,
    ) -> Result<ParsedResponse, Box<dyn std::error::Error>> {
        let params = serde_json::to_value(&req.params).unwrap_or_default();
        let events_req = events::protocol::AwsRequest {
            service: req.service.clone(),
            operation: req.operation.clone(),
            account: req.account,
            region: req.region.clone(),
            params,
            body: req.body.clone(),
        };
        let resp = self.inner.handle(events_req);
        let mut headers = std::collections::HashMap::new();
        for (k, v) in resp.headers {
            headers.insert(k, v);
        }
        Ok(ParsedResponse {
            status: StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            headers,
            body: serde_json::Value::Null,
            raw: Some(resp.body),
            raw_bytes: None,
        })
    }
}

/// Adapter for Kinesis service crate.
pub struct KinesisServiceHandler {
    inner: Arc<kinesis::DefaultKinesisHandler>,
}

impl ServiceHandler for KinesisServiceHandler {
    fn handle_sync(
        &self,
        req: &ParsedRequest,
    ) -> Result<ParsedResponse, Box<dyn std::error::Error>> {
        let params = serde_json::to_value(&req.params).unwrap_or_default();
        let kinesis_req = kinesis::protocol::AwsRequest {
            service: req.service.clone(),
            operation: req.operation.clone(),
            account: req.account,
            region: req.region.clone(),
            params,
            body: req.body.clone(),
        };
        let resp = self.inner.handle(kinesis_req);
        let mut headers = std::collections::HashMap::new();
        for (k, v) in resp.headers {
            headers.insert(k, v);
        }
        Ok(ParsedResponse {
            status: StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            headers,
            body: serde_json::Value::Null,
            raw: Some(resp.body),
            raw_bytes: None,
        })
    }
}

/// Adapter for Firehose.
pub struct FirehoseServiceHandler {
    inner: Arc<firehose::DefaultFirehoseHandler>,
}
impl ServiceHandler for FirehoseServiceHandler {
    fn handle_sync(
        &self,
        req: &ParsedRequest,
    ) -> Result<ParsedResponse, Box<dyn std::error::Error>> {
        let params = serde_json::to_value(&req.params).unwrap_or_default();
        let r = firehose::protocol::AwsRequest {
            service: req.service.clone(),
            operation: req.operation.clone(),
            account: req.account,
            region: req.region.clone(),
            params,
            body: req.body.clone(),
        };
        let resp = self.inner.handle(r);
        let mut h = std::collections::HashMap::new();
        for (k, v) in resp.headers {
            h.insert(k, v);
        }
        Ok(ParsedResponse {
            status: StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            headers: h,
            body: serde_json::Value::Null,
            raw: Some(resp.body),
            raw_bytes: None,
        })
    }
}

/// Adapter for CloudWatch.
pub struct CloudWatchServiceHandler {
    inner: Arc<cloudwatch::DefaultCloudwatchHandler>,
}
impl ServiceHandler for CloudWatchServiceHandler {
    fn handle_sync(
        &self,
        req: &ParsedRequest,
    ) -> Result<ParsedResponse, Box<dyn std::error::Error>> {
        let params = serde_json::to_value(&req.params).unwrap_or_default();
        let r = cloudwatch::protocol::AwsRequest {
            service: req.service.clone(),
            operation: req.operation.clone(),
            account: req.account,
            region: req.region.clone(),
            params,
            body: req.body.clone(),
        };
        let resp = self.inner.handle(r);
        let mut h = std::collections::HashMap::new();
        for (k, v) in resp.headers {
            h.insert(k, v);
        }
        Ok(ParsedResponse {
            status: StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            headers: h,
            body: serde_json::Value::Null,
            raw: Some(resp.body),
            raw_bytes: None,
        })
    }
}

/// Adapter for ECR.
pub struct EcrServiceHandler {
    inner: Arc<ecr::DefaultEcrHandler>,
}
impl ServiceHandler for EcrServiceHandler {
    fn handle_sync(
        &self,
        req: &ParsedRequest,
    ) -> Result<ParsedResponse, Box<dyn std::error::Error>> {
        let params = serde_json::to_value(&req.params).unwrap_or_default();
        let r = ecr::protocol::AwsRequest {
            service: req.service.clone(),
            operation: req.operation.clone(),
            account: req.account,
            region: req.region.clone(),
            params,
            body: req.body.clone(),
        };
        let resp = self.inner.handle(r);
        let mut h = std::collections::HashMap::new();
        for (k, v) in resp.headers {
            h.insert(k, v);
        }
        Ok(ParsedResponse {
            status: StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            headers: h,
            body: serde_json::Value::Null,
            raw: Some(resp.body),
            raw_bytes: None,
        })
    }
}

/// Adapter for ECS.
pub struct EcsServiceHandler {
    inner: Arc<ecs::DefaultEcsHandler>,
}
impl ServiceHandler for EcsServiceHandler {
    fn handle_sync(
        &self,
        req: &ParsedRequest,
    ) -> Result<ParsedResponse, Box<dyn std::error::Error>> {
        let params = serde_json::to_value(&req.params).unwrap_or_default();
        let r = ecs::protocol::AwsRequest {
            service: req.service.clone(),
            operation: req.operation.clone(),
            account: req.account,
            region: req.region.clone(),
            params,
            body: req.body.clone(),
        };
        let resp = self.inner.handle(r);
        let mut h = std::collections::HashMap::new();
        for (k, v) in resp.headers {
            h.insert(k, v);
        }
        Ok(ParsedResponse {
            status: StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            headers: h,
            body: serde_json::Value::Null,
            raw: Some(resp.body),
            raw_bytes: None,
        })
    }
}

/// Adapter for Step Functions.
pub struct StepFunctionsServiceHandler {
    inner: Arc<stepfunctions::DefaultStepfunctionsHandler>,
}
impl ServiceHandler for StepFunctionsServiceHandler {
    fn handle_sync(
        &self,
        req: &ParsedRequest,
    ) -> Result<ParsedResponse, Box<dyn std::error::Error>> {
        let params = serde_json::to_value(&req.params).unwrap_or_default();
        let r = stepfunctions::protocol::AwsRequest {
            service: req.service.clone(),
            operation: req.operation.clone(),
            account: req.account,
            region: req.region.clone(),
            params,
            body: req.body.clone(),
        };
        let resp = self.inner.handle(r);
        let mut h = std::collections::HashMap::new();
        for (k, v) in resp.headers {
            h.insert(k, v);
        }
        Ok(ParsedResponse {
            status: StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            headers: h,
            body: serde_json::Value::Null,
            raw: Some(resp.body),
            raw_bytes: None,
        })
    }
}

/// Handler instances shared between the wire path and the snapshot registry.
#[derive(Clone)]
pub struct NativeHandlers {
    pub sqs: Arc<sqs::DefaultSqsHandler>,
    pub s3: Arc<s3::DefaultS3Handler>,
    pub dynamodb: Arc<dynamodb::DefaultDynamoDbHandler>,
    pub sns: Arc<sns::DefaultSnsHandler>,
    pub secretsmanager: Arc<secretsmanager::DefaultSecretsManagerHandler>,
    pub kms: Arc<kms::DefaultKmsHandler>,
    pub ssm: Arc<ssm::DefaultSsmHandler>,
    pub iam: Arc<iam::DefaultIamHandler>,
    pub lambda: Arc<lambda::DefaultLambdaHandler>,
    pub logs: Arc<cloudwatch_logs::DefaultLogsHandler>,
    pub events: Arc<events::DefaultEventsHandler>,
    pub kinesis: Arc<kinesis::DefaultKinesisHandler>,
    pub firehose: Arc<firehose::DefaultFirehoseHandler>,
    pub cloudwatch: Arc<cloudwatch::DefaultCloudwatchHandler>,
    pub ecr: Arc<ecr::DefaultEcrHandler>,
    pub ecs: Arc<ecs::DefaultEcsHandler>,
    pub stepfunctions: Arc<stepfunctions::DefaultStepfunctionsHandler>,
}

impl NativeHandlers {
    /// Invoke a lambda function with an API Gateway proxy event and shape the
    /// wire response (mirrors the python executor's proxy handling).
    pub fn invoke_lambda(
        &self,
        account: u64,
        region: &str,
        function_name: &str,
        payload: &serde_json::Value,
    ) -> axum::response::Response {
        use axum::response::IntoResponse;

        let anyhow = ("", 0u16);
        let _ = anyhow;
        let req = lambda::protocol::AwsRequest {
            service: "lambda".to_string(),
            operation: "Invoke".to_string(),
            account,
            region: region.to_string(),
            params: json!({
                "FunctionName": function_name,
                "Qualifier": "$LATEST",
                "Payload": payload.to_string(),
            }),
            // The lambda runtime takes the event from the raw request body
            // (boto's Invoke sends Payload as the body) — the empty body here
            // is what made proxy events arrive as {}.
            body: bytes::Bytes::from(payload.to_string()),
            method: "POST".to_string(),
            // The lambda handler re-derives the operation from method+path.
            path: format!("/2015-03-31/functions/{function_name}/invocations"),
            query_string: String::new(),
            headers: std::collections::HashMap::new(),
        };
        let resp = self.lambda.handle(req);
        let function_error = resp
            .headers
            .iter()
            .find(|(k, _)| k == "x-amz-function-error")
            .map(|(_, v)| v.clone())
            .unwrap_or_default();
        let body_text = resp.body.clone();
        let parsed: Value = serde_json::from_str(&body_text).unwrap_or(Value::Null);

        let (status, out_body) = if resp.status == 200 && function_error.is_empty() {
            match &parsed {
                Value::Object(obj) if obj.contains_key("statusCode") => {
                    let code = obj.get("statusCode").cloned().expect("checked above");
                    let code = code.as_u64().unwrap_or(200);
                    let out_body = match obj.get("body") {
                        Some(Value::String(text)) => text.clone(),
                        Some(other) => other.to_string(),
                        None => String::new(),
                    };
                    (code as u16, out_body)
                }
                _ => (200u16, body_text.clone()),
            }
        } else {
            (500, out500(&body_text))
        };

        (
            StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            axum::body::Body::from(out_body),
        )
            .into_response()
    }
}

fn out500(text: &str) -> String {
    json!({"message": text}).to_string()
}

/// Registry of service handlers.
pub struct ServiceRegistry {
    handlers: HashMap<String, Arc<dyn ServiceHandler>>,
    /// Shared native handler instances (used by the apigateway executor).
    pub native: NativeHandlers,
}

impl ServiceRegistry {
    /// Create a registry with built-in services, returning the shared
    /// native handler instances for the state-snapshot registry.
    pub fn new_with_handlers() -> (Self, NativeHandlers) {
        let handlers: NativeHandlers = NativeHandlers {
            sqs: Arc::new(sqs::DefaultSqsHandler::new()),
            s3: Arc::new(s3::DefaultS3Handler::new()),
            dynamodb: Arc::new(dynamodb::DefaultDynamoDbHandler::new()),
            sns: Arc::new(sns::DefaultSnsHandler::new()),
            secretsmanager: Arc::new(secretsmanager::DefaultSecretsManagerHandler::new()),
            kms: Arc::new(kms::DefaultKmsHandler::new()),
            ssm: Arc::new(ssm::DefaultSsmHandler::new()),
            iam: Arc::new(iam::DefaultIamHandler::new()),
            lambda: Arc::new(lambda::DefaultLambdaHandler::new()),
            logs: Arc::new(cloudwatch_logs::DefaultLogsHandler::new()),
            events: Arc::new(events::DefaultEventsHandler::new()),
            kinesis: Arc::new(kinesis::DefaultKinesisHandler::new()),
            firehose: Arc::new(firehose::DefaultFirehoseHandler::new()),
            cloudwatch: Arc::new(cloudwatch::DefaultCloudwatchHandler::new()),
            ecr: Arc::new(ecr::DefaultEcrHandler::new()),
            ecs: Arc::new(ecs::DefaultEcsHandler::new()),
            stepfunctions: Arc::new(stepfunctions::DefaultStepfunctionsHandler::new()),
        };

        let mut adapters: HashMap<String, Arc<dyn ServiceHandler>> = HashMap::new();

        // Register STS handler
        adapters.insert(
            "sts".to_string(),
            Arc::new(StsFunctionHandler) as Arc<dyn ServiceHandler>,
        );

        // Register native handlers (sharing instances with NativeHandlers)
        adapters.insert(
            "sqs".to_string(),
            Arc::new(SqsServiceHandler {
                inner: handlers.sqs.clone(),
            }) as Arc<dyn ServiceHandler>,
        );
        adapters.insert(
            "s3".to_string(),
            Arc::new(S3ServiceHandler {
                inner: handlers.s3.clone(),
            }) as Arc<dyn ServiceHandler>,
        );
        adapters.insert(
            "dynamodb".to_string(),
            Arc::new(DynamoDbServiceHandler {
                inner: handlers.dynamodb.clone(),
            }) as Arc<dyn ServiceHandler>,
        );
        adapters.insert(
            "sns".to_string(),
            Arc::new(SnsServiceHandler {
                inner: handlers.sns.clone(),
            }) as Arc<dyn ServiceHandler>,
        );
        adapters.insert(
            "secretsmanager".to_string(),
            Arc::new(SmServiceHandler {
                inner: handlers.secretsmanager.clone(),
            }) as Arc<dyn ServiceHandler>,
        );
        adapters.insert(
            "kms".to_string(),
            Arc::new(KmsServiceHandler {
                inner: handlers.kms.clone(),
            }) as Arc<dyn ServiceHandler>,
        );
        adapters.insert(
            "ssm".to_string(),
            Arc::new(SsmServiceHandler {
                inner: handlers.ssm.clone(),
            }) as Arc<dyn ServiceHandler>,
        );
        adapters.insert(
            "iam".to_string(),
            Arc::new(IamServiceHandler {
                inner: handlers.iam.clone(),
            }) as Arc<dyn ServiceHandler>,
        );
        adapters.insert(
            "lambda".to_string(),
            Arc::new(LambdaServiceHandler {
                inner: handlers.lambda.clone(),
            }) as Arc<dyn ServiceHandler>,
        );
        adapters.insert(
            "logs".to_string(),
            Arc::new(LogsServiceHandler {
                inner: handlers.logs.clone(),
            }) as Arc<dyn ServiceHandler>,
        );
        adapters.insert(
            "events".to_string(),
            Arc::new(EventsServiceHandler {
                inner: handlers.events.clone(),
            }) as Arc<dyn ServiceHandler>,
        );
        adapters.insert(
            "kinesis".to_string(),
            Arc::new(KinesisServiceHandler {
                inner: handlers.kinesis.clone(),
            }) as Arc<dyn ServiceHandler>,
        );
        adapters.insert(
            "firehose".to_string(),
            Arc::new(FirehoseServiceHandler {
                inner: handlers.firehose.clone(),
            }) as Arc<dyn ServiceHandler>,
        );
        adapters.insert(
            "cloudwatch".to_string(),
            Arc::new(CloudWatchServiceHandler {
                inner: handlers.cloudwatch.clone(),
            }) as Arc<dyn ServiceHandler>,
        );
        adapters.insert(
            "ecr".to_string(),
            Arc::new(EcrServiceHandler {
                inner: handlers.ecr.clone(),
            }) as Arc<dyn ServiceHandler>,
        );
        adapters.insert(
            "ecs".to_string(),
            Arc::new(EcsServiceHandler {
                inner: handlers.ecs.clone(),
            }) as Arc<dyn ServiceHandler>,
        );
        adapters.insert(
            "stepfunctions".to_string(),
            Arc::new(StepFunctionsServiceHandler {
                inner: handlers.stepfunctions.clone(),
            }) as Arc<dyn ServiceHandler>,
        );

        (
            Self {
                handlers: adapters,
                native: NativeHandlers {
                    sqs: handlers.sqs.clone(),
                    s3: handlers.s3.clone(),
                    dynamodb: handlers.dynamodb.clone(),
                    sns: handlers.sns.clone(),
                    secretsmanager: handlers.secretsmanager.clone(),
                    kms: handlers.kms.clone(),
                    ssm: handlers.ssm.clone(),
                    iam: handlers.iam.clone(),
                    lambda: handlers.lambda.clone(),
                    logs: handlers.logs.clone(),
                    events: handlers.events.clone(),
                    kinesis: handlers.kinesis.clone(),
                    firehose: handlers.firehose.clone(),
                    cloudwatch: handlers.cloudwatch.clone(),
                    ecr: handlers.ecr.clone(),
                    ecs: handlers.ecs.clone(),
                    stepfunctions: handlers.stepfunctions.clone(),
                },
            },
            handlers,
        )
    }

    /// Create a new registry with built-in services.
    pub fn new() -> Self {
        Self::new_with_handlers().0
    }

    /// Get a service handler by name.
    pub fn get(&self, service: &str) -> Option<Arc<dyn ServiceHandler>> {
        self.handlers.get(service).cloned()
    }
}

impl Default for ServiceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Application state shared across requests.
pub struct AppState {
    pub registry: ServiceRegistry,
    /// Central (service, snapshot surface) table for the state endpoints.
    pub snapshots: Arc<crate::core::state::SnapshotRegistry>,
    /// In-memory named snapshots (`/state/save` without `path`).
    pub named_snapshots: Arc<crate::core::state::NamedSnapshots>,
    /// Versioned snapshots (`/state/snapshots/save|load`).
    pub versioned_snapshots: Arc<crate::core::state::VersionedSnapshots>,
}

/// Catch-all handler for all AWS API requests.
pub async fn catch_all_handler(
    State(state): State<Arc<AppState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: axum::body::Body,
) -> impl IntoResponse {
    let req_start = std::time::Instant::now();
    let req_method = method.as_str().to_string();
    let req_path = uri.path().to_string();

    // Read the body
    let body_bytes = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            audit_out(
                None,
                None,
                &req_method,
                &req_path,
                400,
                req_start,
                "",
                "",
                Some(format!("Invalid body: {e}")),
            );
            return error_response(StatusCode::BAD_REQUEST, "Invalid body");
        }
    };

    // Deployed-API execution: /restapis/{api}/{stage}/_user_request_/{path}/
    // (the ground truth serves these with its own executor, not moto routes).
    if let Some((api_id, stage, proxy_path)) = crate::core::apigw_exec::match_execute(uri.path()) {
        // The audit call below needs the account/region rows that are
        // computed further down; derive them early here.
        let account =
            extract_account_from_request(&headers, uri.query().unwrap_or(""), &body_bytes);
        let account_id = account.to_string();
        let region_for_audit = extract_region_from_request(&headers)
            .or_else(|| extract_region_from_query(uri.query().unwrap_or("")))
            .unwrap_or_else(|| "us-east-1".to_string());
        let mut exec_headers: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        for (key, value) in headers.iter() {
            if let Ok(v) = value.to_str() {
                exec_headers.insert(key.as_str().to_lowercase(), v.to_string());
            }
        }
        let result = crate::core::apigw_exec::execute(
            &state.registry.native,
            &method,
            crate::core::apigw_exec::ExecuteTarget {
                api_id: &api_id,
                stage: &stage,
                proxy_path: &proxy_path,
            },
            account,
            region_for_audit.as_str(),
            uri.query().unwrap_or(""),
            &exec_headers,
            &body_bytes,
        )
        .await;
        audit_out(
            Some("apigateway"),
            None,
            &req_method,
            &req_path,
            result.status().as_u16(),
            req_start,
            &account_id,
            &region_for_audit,
            None,
        );
        return result;
    }

    // Lambda function URL HTTP surface: /lambda-url/{url-id}/{path…}
    // (served by its own executor like the deployed-API bridge above).
    if let Some(target) = crate::core::furl_exec::match_url(uri.path()) {
        let account =
            extract_account_from_request(&headers, uri.query().unwrap_or(""), &body_bytes);
        let account_id = account.to_string();
        let region_for_audit = extract_region_from_request(&headers)
            .or_else(|| extract_region_from_query(uri.query().unwrap_or("")))
            .unwrap_or_else(|| "us-east-1".to_string());
        let mut exec_headers: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        for (key, value) in headers.iter() {
            if let Ok(v) = value.to_str() {
                exec_headers.insert(key.as_str().to_lowercase(), v.to_string());
            }
        }
        let result = crate::core::furl_exec::execute(
            &state.registry.native,
            &method,
            target,
            account,
            &body_bytes,
            &exec_headers,
            uri.query().unwrap_or(""),
        );
        audit_out(
            Some("lambda"),
            None,
            &req_method,
            &req_path,
            result.status().as_u16(),
            req_start,
            &account_id,
            &region_for_audit,
            None,
        );
        return result;
    }

    // Determine service from URI and headers
    let service = match extract_service(&method, &uri, &headers, &body_bytes) {
        Ok(s) => refine_service_for_body(&s, &body_bytes),
        Err(e) => {
            audit_out(
                None,
                None,
                &req_method,
                &req_path,
                400,
                req_start,
                "",
                "",
                Some(format!("Could not determine service: {e}")),
            );
            return error_response(
                StatusCode::BAD_REQUEST,
                &format!("Could not determine service: {}", e),
            );
        }
    };

    // Add the path to headers for REST operation resolution
    let mut headers_with_path = headers.clone();
    if let Ok(path_val) = HeaderValue::from_str(uri.path()) {
        headers_with_path.insert("x-robotocore-path", path_val);
    }

    // Determine operation (may be empty for REST services like S3 where
    // the operation is derived from method + path + query params)
    let query_string = uri.query().unwrap_or("").to_string();
    let operation = extract_operation(
        &method,
        &headers_with_path,
        &body_bytes,
        &service,
        &query_string,
    )
    .unwrap_or_default();

    // Extract account from Authorization header or default
    let account = extract_account_from_request(&headers, uri.query().unwrap_or(""), &body_bytes);
    let region = extract_region_from_request(&headers)
        .or_else(|| extract_region_from_query(uri.query().unwrap_or("")))
        .unwrap_or_else(|| "us-east-1".to_string());
    let account_id = account.to_string();
    let region_for_audit = region.clone();

    // Chaos fault injection runs where the python twin's handler chain ran
    // it — right after context population, before service dispatch.
    if let Some(injected) = crate::core::chaos::intercept(
        &service,
        &operation,
        &method,
        uri.path(),
        &query_string,
        &region_for_audit,
    )
    .await
    {
        audit_out(
            Some(&service),
            opt_op(&injected.operation),
            &req_method,
            &req_path,
            injected.response.status().as_u16(),
            req_start,
            &account_id,
            &region_for_audit,
            Some(format!("Injected by chaos rule {}", injected.rule_id)),
        );
        return injected.response;
    }

    // Parse request parameters based on protocol
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let params = if content_type.contains("json") {
        crate::core::protocol::parse_json_protocol(&body_bytes).unwrap_or_default()
    } else {
        parse_query_protocol(&body_bytes).unwrap_or_default()
    };

    // Build headers map (lowercase keys)
    let mut header_map: HashMap<String, String> = HashMap::new();
    for (k, v) in headers.iter() {
        if let Ok(v) = v.to_str() {
            header_map.insert(k.as_str().to_lowercase(), v.to_string());
        }
    }

    // Create ParsedRequest
    let parsed_req = ParsedRequest {
        service: service.clone(),
        operation: operation.clone(),
        params,
        body: body_bytes.clone(),
        region,
        account,
        method: method.as_str().to_string(),
        path: uri.path().to_string(),
        query_string: uri.query().unwrap_or("").to_string(),
        headers: header_map,
    };

    // Get service handler
    let handler = state.registry.get(&service);
    crate::core::introspection::bump_requests(&service);
    let handler = match handler {
        Some(h) => h,
        None => {
            // Rotocore is the in-process moto replacement: services that
            // are not in the gateway core dispatch here. unported ones
            // fall through to the generic 501 NotImplemented below.
            // rotocore backends may block on sibling services (loopback
            // mirror calls), so run the sync dispatch on the blocking
            // thread pool — never on the async workers, which must stay
            // free to accept/serve those loopback calls.
            let rot_req = rotocore::Request {
                service: service.clone(),
                operation: parsed_req.operation.clone(),
                method: parsed_req.method.clone(),
                path: parsed_req.path.clone(),
                query_string: parsed_req.query_string.clone(),
                headers: parsed_req.headers.clone(),
                body: parsed_req.body.clone(),
                account: parsed_req.account,
                region: parsed_req.region.clone(),
            };
            let dispatch_result =
                tokio::task::spawn_blocking(move || rotocore::dispatch(&rot_req)).await;
            if let Ok(Some(resp)) = dispatch_result {
                audit_out(
                    Some(&service),
                    opt_op(&parsed_req.operation),
                    &req_method,
                    &req_path,
                    resp.status,
                    req_start,
                    &account_id,
                    &region_for_audit,
                    None,
                );
                let status =
                    StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
                let mut headers: HashMap<String, String> = HashMap::new();
                for (k, v) in resp.headers {
                    headers.insert(k, v);
                }
                let parsed = crate::core::protocol::ParsedResponse {
                    status,
                    headers,
                    body: serde_json::Value::Null,
                    raw: None,
                    raw_bytes: Some(resp.body),
                };
                return response_from_parsed(parsed, &service, &parsed_req.operation);
            }
            let msg = format!("Service {} not implemented", service);
            audit_out(
                Some(&service),
                opt_op(&operation),
                &req_method,
                &req_path,
                501,
                req_start,
                &account_id,
                &region_for_audit,
                Some(msg.clone()),
            );
            return error_response(StatusCode::NOT_IMPLEMENTED, &msg);
        }
    };

    // Handle request with a timeout to catch runaway tasks
    let handler = handler.clone();
    let parsed_req = Arc::new(parsed_req);
    let service = service.clone();
    let timeout_secs: u64 = 30;

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(timeout_secs),
        tokio::task::spawn_blocking(move || {
            let req = parsed_req.as_ref();
            match handler.handle_sync(req) {
                Ok(resp) => Ok((resp, req.operation.clone())),
                Err(e) => Err(e.to_string()),
            }
        }),
    )
    .await;

    match result {
        Ok(Ok(Ok((resp, op)))) => {
            audit_out(
                Some(&service),
                opt_op(&op),
                &req_method,
                &req_path,
                resp.status.as_u16(),
                req_start,
                &account_id,
                &region_for_audit,
                None,
            );
            response_from_parsed(resp, &service, &op)
        }
        Ok(Ok(Err(e))) => {
            audit_out(
                Some(&service),
                opt_op(&operation),
                &req_method,
                &req_path,
                500,
                req_start,
                &account_id,
                &region_for_audit,
                Some(e.clone()),
            );
            error_response(StatusCode::INTERNAL_SERVER_ERROR, &e)
        }
        Ok(Err(e)) => {
            let msg = format!("Handler task failed: {}", e);
            audit_out(
                Some(&service),
                opt_op(&operation),
                &req_method,
                &req_path,
                500,
                req_start,
                &account_id,
                &region_for_audit,
                Some(msg.clone()),
            );
            error_response(StatusCode::INTERNAL_SERVER_ERROR, &msg)
        }
        Err(_) => {
            tracing::warn!(
                "Request timed out after {}s: {} {}",
                timeout_secs,
                method,
                uri
            );
            audit_out(
                Some(&service),
                opt_op(&operation),
                &req_method,
                &req_path,
                504,
                req_start,
                &account_id,
                &region_for_audit,
                Some(format!("Request timed out after {timeout_secs}s")),
            );
            error_response(StatusCode::GATEWAY_TIMEOUT, "Request timed out")
        }
    }
}

/// Build an audit record for a completed request (Python audit-log shape).
#[allow(clippy::too_many_arguments)]
fn audit_out(
    service: Option<&str>,
    operation: Option<&str>,
    method: &str,
    path: &str,
    status: u16,
    start: std::time::Instant,
    account_id: &str,
    region: &str,
    error: Option<String>,
) {
    crate::core::introspection::record(crate::core::introspection::AuditEntry {
        timestamp: crate::core::introspection::epoch_now(),
        service: service.unwrap_or("unknown").to_string(),
        operation: operation.map(|s| s.to_string()),
        method: method.to_string(),
        path: path.to_string(),
        status_code: status,
        // ms with µs (3-decimal) precision, as Python's round(ms, 3)
        duration_ms: (start.elapsed().as_secs_f64() * 1.0e6).round() / 1.0e3,
        account_id: account_id.to_string(),
        region: region.to_string(),
        error,
    });
}

/// ELB Classic and ELBv2 share the signing name `elasticloadbalancing`.
/// The router disambiguates by the `Version=2012-06-01` query parameter, but
/// boto3 sends that version inside the urlencoded form body, so the gateway
/// consults it here before the elbv2 arm swallows the request.
fn refine_service_for_body(service: &str, body: &[u8]) -> String {
    if service != "elbv2" {
        return service.to_string();
    }
    let form = String::from_utf8_lossy(body);
    let has_version = form
        .split('&')
        .any(|pair| pair == "Version=2012-06-01" || pair.starts_with("Version=2012-06-01&"));
    if has_version {
        "elb".to_string()
    } else {
        service.to_string()
    }
}

fn opt_op(operation: &str) -> Option<&str> {
    (!operation.is_empty()).then_some(operation)
}

/// Health check endpoint
pub async fn health_handler() -> impl IntoResponse {
    let health = json!({
        "status": "running",
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_seconds": crate::core::introspection::uptime_seconds(),
        "services": registry_service_statuses()
    });

    (StatusCode::OK, axum::Json(health))
}

/// Names of every service with a native handler in this build.
pub const NATIVE_SERVICES: &[&str] = &[
    "sts",
    "sqs",
    "s3",
    "dynamodb",
    "sns",
    "secretsmanager",
    "kms",
    "ssm",
    "iam",
    "lambda",
    "logs",
    "events",
    "kinesis",
    "firehose",
    "cloudwatch",
    "ecr",
    "ecs",
    "stepfunctions",
];

/// Service names present in the gateway's own registry.
///
/// `service_is_native` is consulted once per registry entry on every
/// introspection request (~157 entries), and constructing a throwaway
/// `ServiceRegistry` — seventeen default handlers plus the adapter map — for
/// each probe dominated the health/services endpoints. The handler set is
/// fixed at compile time, so derive the names from the real constructed
/// table exactly once and reuse them; the introspection labels stay derived
/// from real handler tables rather than a hardcoded list.
fn gateway_native_service_names() -> &'static HashSet<String> {
    static NAMES: OnceLock<HashSet<String>> = OnceLock::new();
    NAMES.get_or_init(|| ServiceRegistry::new().handlers.keys().cloned().collect())
}

fn service_is_native(name: &str) -> bool {
    // The gateway's own handler table plus rotocore's armed backends. The
    // registry's static "type" column is stale for both directions, so
    // introspection derives availability from the real tables.
    gateway_native_service_names().contains(name) || rotocore::handles_service(name)
}

fn registry_service_statuses() -> serde_json::Map<String, serde_json::Value> {
    // Full sorted registry: every service reports status/type/request-count.
    // "type" is truthful: "native" when a Rust backend answers in-process,
    // "absent" when requests would get a 501 NotImplemented.
    let mut map = serde_json::Map::new();
    for (name, _stype, _protocol, _description) in crate::core::service_registry::SERVICE_REGISTRY {
        map.insert(
            name.to_string(),
            json!({
                "status": "running",
                "type": if service_is_native(name) { "native" } else { "absent" },
                "requests": crate::core::introspection::request_count(name),
            }),
        );
    }
    map
}

/// Runtime families report.
///
/// Field contract mirrors the Python endpoint (`available` sorted, `all`
/// equal to _ALL_RUNTIME_FAMILIES). Honesty rule: a family may only appear in
/// `available` when the Rust lambda handler can actually execute it — keep
/// this list truthful, not aspirational. Update when executors land in
/// `crates/lambda`.
pub async fn runtimes_handler() -> impl IntoResponse {
    let all = ["custom", "dotnet", "java", "nodejs", "python", "ruby"];
    let mut available: Vec<&str> = vec!["custom", "python"];
    available.sort_unstable();
    (
        StatusCode::OK,
        axum::Json(json!({ "available": available, "all": all })),
    )
}

/// Config endpoint
pub async fn config_handler() -> impl IntoResponse {
    let config = json!({
        "version": env!("CARGO_PKG_VERSION"),
        "debug": false,
        "specifications_dir": std::env::var("ROBOTOCORE_SPECS_DIR").unwrap_or_else(|_| "/opt/homebrew/lib/python3.14/site-packages/botocore/data".to_string())
    });

    (StatusCode::OK, axum::Json(config))
}

/// `POST /_robotocore/config` — runtime config updates, mirroring the
/// python twin's behavior verbatim: 403 unless the twin was launched with
/// `ENABLE_CONFIG_UPDATES=1`, 400 for empty/invalid/non-object bodies and
/// non-whitelisted keys, `{"status": "updated", "updated": {...}}` on
/// success.
///
/// Values are stored in the runtime-config override table; settings whose
/// consumers are not ported to the rust gateway yet are accepted but have
/// no consumer (see `core::runtime_config`).
pub async fn config_update_handler(body: axum::body::Bytes) -> Response {
    const DISABLED: &str = "Runtime config updates are disabled. Set ENABLE_CONFIG_UPDATES=1.";
    if !crate::core::runtime_config::updates_enabled() {
        return (
            StatusCode::FORBIDDEN,
            axum::Json(json!({ "error": DISABLED })),
        )
            .into_response();
    }
    if body.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(json!({ "error": "No settings provided" })),
        )
            .into_response();
    }
    let updates: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                axum::Json(json!({ "error": "Invalid JSON" })),
            )
                .into_response()
        }
    };
    let Some(updates) = updates.as_object() else {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(json!({ "error": "Expected JSON object" })),
        )
            .into_response();
    };
    let mut updated = serde_json::Map::new();
    for (key, value) in updates {
        // python twin echoes str(value): `True`/`None`/`['a']` shapes.
        let value_str = crate::core::runtime_config::python_repr(value);
        if !crate::core::runtime_config::MUTABLE_SETTINGS
            .iter()
            .any(|(k, _)| k == key)
        {
            return (
                StatusCode::BAD_REQUEST,
                axum::Json(
                    json!({ "error": format!("Setting {key} cannot be changed at runtime") }),
                ),
            )
                .into_response();
        }
        crate::core::runtime_config::set(key, value_str.clone());
        updated.insert(key.clone(), json!(value_str));
    }
    (
        StatusCode::OK,
        axum::Json(json!({ "status": "updated", "updated": updated })),
    )
        .into_response()
}

/// Audit endpoint: `?limit=N` recent call records.
pub async fn audit_handler(uri: Uri) -> impl IntoResponse {
    // Python: `?limit=N` (int required; anything non-numeric is a 400)
    let limit = match uri.query().and_then(|q| {
        q.split('&').find_map(|kv| {
            let (k, v) = kv.split_once('=')?;
            (k == "limit").then_some(v)
        })
    }) {
        Some(v) => match v.parse::<usize>() {
            Ok(n) => n,
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    axum::Json(json!({"error": "limit must be an integer"})),
                );
            }
        },
        None => 100,
    };
    let entries = crate::core::introspection::recent_entries(limit, None, None);
    (
        StatusCode::OK,
        axum::Json(json!({
            "entries": entries,
            "count": entries.len(),
        })),
    )
}

/// Per-service protocol listing: `GET /_robotocore/services`.
///
/// Mirrors the Python server: every registry entry with its
/// native/moto status, protocol, description and enabled flag,
/// sorted by name (the table is already sorted).
pub async fn services_handler() -> impl IntoResponse {
    let entries: Vec<serde_json::Value> = crate::core::service_registry::SERVICE_REGISTRY
        .iter()
        .map(|(name, _stype, protocol, description)| {
            let native = service_is_native(name);
            json!({
                "name": name,
                "status": if native { "native" } else { "absent" },
                "protocol": protocol,
                "description": description,
                "enabled": native,
            })
        })
        .collect();
    (StatusCode::OK, axum::Json(json!({ "services": entries })))
}

/// Error usage summary: `GET /_robotocore/usage/errors`.
pub async fn usage_errors_handler() -> impl IntoResponse {
    (
        StatusCode::OK,
        axum::Json(crate::core::introspection::error_summary()),
    )
}

/// Build the Axum router.
pub fn build_router(
    registry: ServiceRegistry,
    snapshots: Arc<crate::core::state::SnapshotRegistry>,
) -> Router {
    let state = Arc::new(AppState {
        registry,
        snapshots,
        named_snapshots: Arc::new(crate::core::state::NamedSnapshots::default()),
        versioned_snapshots: Arc::new(crate::core::state::VersionedSnapshots::default()),
    });

    Router::new()
        .route("/_robotocore/health", get(health_handler))
        .route(
            "/_robotocore/config",
            get(config_handler).post(config_update_handler),
        )
        .route("/_robotocore/audit", get(audit_handler))
        .route("/_robotocore/runtimes", get(runtimes_handler))
        .route("/_robotocore/services", get(services_handler))
        .route("/_robotocore/usage/errors", get(usage_errors_handler))
        // State save/load/reset (Cloud Pods-style snapshots)
        .route(
            "/_robotocore/state/save",
            get(state_save_handler).post(state_save_handler),
        )
        .route("/_robotocore/state/load", post(state_load_handler))
        .route("/_robotocore/state/reset", post(state_reset_handler))
        .route(
            "/_robotocore/state/snapshots",
            get(state_snapshots_list_handler),
        )
        .route(
            "/robotocore/state/snapshots",
            get(state_snapshots_list_handler),
        )
        .route(
            "/_robotocore/state/snapshots/save",
            post(state_snapshot_save_handler),
        )
        .route(
            "/_robotocore/state/snapshots/load",
            post(state_snapshot_load_handler),
        )
        .route("/_robotocore/state/export", get(state_export_handler))
        .route("/_robotocore/state/import", post(state_import_handler))
        // Chaos engineering fault-injection admin API
        // (python-era `/_robotocore/chaos/*` port)
        .route(
            "/_robotocore/chaos/rules",
            get(crate::core::chaos::chaos_list_rules).post(crate::core::chaos::chaos_add_rule),
        )
        .route(
            "/_robotocore/chaos/rules/clear",
            post(crate::core::chaos::chaos_clear_rules),
        )
        .route(
            "/_robotocore/chaos/rules/:rule_id",
            delete(crate::core::chaos::chaos_delete_rule),
        )
        .fallback(catch_all_handler)
        .with_state(state)
}

// ---------------------------------------------------------------------------
// State save/load endpoints
// ---------------------------------------------------------------------------

/// Parse the JSON request body for state endpoints ({} for empty bodies).
/// Small error type so the `Err` variant stays cheap to return inline.
type StateBodyError = (StatusCode, axum::Json<serde_json::Value>);

fn state_body(body: axum::body::Bytes) -> Result<serde_json::Value, StateBodyError> {
    if body.is_empty() {
        return Ok(serde_json::Value::Object(serde_json::Map::new()));
    }
    serde_json::from_slice::<serde_json::Value>(&body).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            axum::Json(json!({"error": format!("Invalid JSON: {e}")})),
        )
    })
}

/// Read a string parameter from body or query string.
fn body_str<'a>(
    params: &'a serde_json::Value,
    key: &str,
    query: Option<&'a str>,
) -> Option<String> {
    if let Some(v) = params.get(key) {
        if let Some(s) = v.as_str() {
            if !s.is_empty() {
                return Some(s.to_string());
            }
        }
    }
    query.and_then(|q| {
        q.split('&').find_map(|kv| {
            let (k, v) = kv.split_once('=')?;
            if k != key {
                return None;
            }
            let decoded = urlencoding::decode(v).ok()?.into_owned();
            (!decoded.is_empty()).then_some(decoded)
        })
    })
}

/// `services` filter: JSON array, comma-separated string, or query `?services=a,b`.
fn services_filter(params: &serde_json::Value, query: Option<&str>) -> Option<Vec<String>> {
    let from_param = params.get("services").cloned();
    let from_query = query.and_then(|q| {
        q.split('&').find_map(|kv| {
            let (k, v) = kv.split_once('=')?;
            (k == "services").then_some(v.to_string())
        })
    });
    let parsed = from_param
        .map(|v| match v {
            serde_json::Value::Array(items) => items
                .iter()
                .filter_map(|i| i.as_str().map(String::from))
                .collect::<Vec<String>>(),
            serde_json::Value::String(s) => s
                .split(',')
                .map(|x| x.trim().to_string())
                .filter(|x| !x.is_empty())
                .collect(),
            _ => Vec::new(),
        })
        .or_else(|| {
            from_query.map(|s| {
                s.split(',')
                    .map(|x| x.trim().to_string())
                    .filter(|x| !x.is_empty())
                    .collect::<Vec<_>>()
            })
        });
    parsed.filter(|v| !v.is_empty())
}

/// Save handler: `GET|POST /_robotocore/state/save`.
pub async fn state_save_handler(
    State(state): State<Arc<AppState>>,
    uri: Uri,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let params = match state_body(body) {
        Ok(b) => b,
        Err((status, body)) => return (status, body).into_response(),
    };
    let query = uri.query();
    let path = body_str(&params, "path", query);
    let name = body_str(&params, "name", query);
    let services = services_filter(&params, query);

    let (saved_names, state_value) = state.snapshots.snapshot(services.as_deref());

    match path {
        Some(dir) => {
            let value = json!({
                "version": "1.0",
                "saved_at": crate::core::introspection::epoch_now(),
                "name": name.clone(),
                "services": saved_names.clone(),
                "state": state_value,
            });
            match crate::core::state::save_to_path(&dir, name.as_deref(), &value) {
                Ok(file) => {
                    let shown_path = match name.as_deref() {
                        Some(_) => file,
                        None => dir,
                    };
                    (
                        StatusCode::OK,
                        axum::Json(json!({
                            "status": "saved",
                            "path": shown_path,
                            "services": saved_names,
                        })),
                    )
                        .into_response()
                }
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    axum::Json(json!({"error": format!("Could not save state: {e}")})),
                )
                    .into_response(),
            }
        }
        None => {
            // No path: keep an in-memory named snapshot.
            let snap_name = name.unwrap_or_else(|| "default".to_string());
            let Some(obj) = state_value.as_object() else {
                return error_response(StatusCode::INTERNAL_SERVER_ERROR, "snapshot failed");
            };
            state
                .named_snapshots
                .put(crate::core::state::NamedSnapshot {
                    name: snap_name.clone(),
                    created_at: crate::core::introspection::epoch_now(),
                    services: saved_names.clone(),
                    state: obj.clone(),
                });
            (
                StatusCode::OK,
                axum::Json(json!({
                    "status": "saved",
                    "path": format!("memory://{snap_name}"),
                    "services": saved_names,
                })),
            )
                .into_response()
        }
    }
}

/// Load handler: `POST /_robotocore/state/load`.
pub async fn state_load_handler(
    State(state): State<Arc<AppState>>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let params = match state_body(body) {
        Ok(b) => b,
        Err((status, body)) => return (status, body).into_response(),
    };
    let path = params
        .get("path")
        .and_then(Value::as_str)
        .map(String::from)
        .or_else(|| {
            crate::core::state::resolve_state_dir().map(|d| d.to_string_lossy().into_owned())
        });
    let name = params.get("name").and_then(Value::as_str).map(String::from);
    let services = services_filter(&params, None);

    // Source resolution order: path-based file, then in-memory name, then default.
    let loaded = match (&path, &name) {
        (Some(dir), file_name) => {
            crate::core::state::load_from_path(dir, file_name.as_deref()).map(|v| ("file", v))
        }
        (None, Some(n)) => state
            .named_snapshots
            .get(n)
            .map(|snap| ("memory", Value::Object(snap.state))),
        (None, None) => None,
    };

    let Some((source, value)) = loaded else {
        return (
            StatusCode::OK,
            axum::Json(json!({
                "status": "no_state_found",
                "path": path.clone().map(Value::from).unwrap_or(name.clone().map(Value::from).unwrap_or(Value::Null)),
            })),
        )
            .into_response();
    };

    state.snapshots.restore(&value, services.as_deref());
    let shown = match source {
        "file" => path.unwrap_or_default(),
        _ => format!("memory://{}", name.unwrap_or_default()),
    };
    (
        StatusCode::OK,
        axum::Json(json!({"status": "loaded", "path": shown})),
    )
        .into_response()
}

/// Reset handler: `POST /_robotocore/state/reset` clears every service state.
pub async fn state_reset_handler(
    State(state): State<Arc<AppState>>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let params = match state_body(body) {
        Ok(b) => b,
        Err((status, body)) => return (status, body).into_response(),
    };
    let services = services_filter(&params, None);
    state.snapshots.reset(services.as_deref());
    (StatusCode::OK, axum::Json(json!({"status": "reset"}))).into_response()
}

/// Snapshot listing: `GET /_robotocore/state/snapshots` (both spellings).
pub async fn state_snapshots_list_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let mut out: Vec<Value> = Vec::new();
    for snap in state.named_snapshots.list() {
        out.push(json!({
            "name": snap.name,
            "timestamp": snap.created_at,
            "saved_at": snap.created_at,
            "services": snap.services,
            "native_services": snap.services,
            "compressed": false,
        }));
    }
    for (name, latest, count) in state.versioned_snapshots.list() {
        out.push(json!({
            "name": name,
            "timestamp": latest.created_at,
            "saved_at": latest.created_at,
            "services": latest.services,
            "native_services": latest.services,
            "compressed": false,
            "versions": count,
        }));
    }
    (StatusCode::OK, axum::Json(json!({ "snapshots": out })))
}

/// Versioned save: `POST /_robotocore/state/snapshots/save`.
pub async fn state_snapshot_save_handler(
    State(state): State<Arc<AppState>>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let params = match state_body(body) {
        Ok(b) => b,
        Err((status, body)) => return (status, body).into_response(),
    };
    let Some(name) = params.get("name").and_then(Value::as_str).map(String::from) else {
        return error_response(StatusCode::BAD_REQUEST, "Snapshot name must not be empty");
    };
    let services = services_filter(&params, None);
    let (saved_names, state_value) = state.snapshots.snapshot(services.as_deref());
    let empty_map = serde_json::Map::new();
    let state_map = state_value.as_object().unwrap_or(&empty_map);
    let version = state
        .versioned_snapshots
        .save(&name, saved_names.clone(), state_map.clone());
    (
        StatusCode::OK,
        axum::Json(json!({
            "status": "saved",
            "name": name,
            "version": version,
            "services": saved_names,
        })),
    )
        .into_response()
}

/// Versioned load: `POST /_robotocore/state/snapshots/load`.
pub async fn state_snapshot_load_handler(
    State(state): State<Arc<AppState>>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let params = match state_body(body) {
        Ok(b) => b,
        Err((status, body)) => return (status, body).into_response(),
    };
    let Some(name) = params.get("name").and_then(Value::as_str).map(String::from) else {
        return error_response(StatusCode::BAD_REQUEST, "Snapshot name must not be empty");
    };
    let version = params.get("version").and_then(Value::as_u64);
    let Some(snap) = state.versioned_snapshots.load(&name, version) else {
        return (
            StatusCode::OK,
            axum::Json(json!({"status": "no_state_found", "name": name})),
        )
            .into_response();
    };
    state.snapshots.restore(&Value::Object(snap.state), None);
    (
        StatusCode::OK,
        axum::Json(json!({
            "status": "loaded",
            "name": name,
            "version": snap.version,
        })),
    )
        .into_response()
}

/// Export handler: `GET /_robotocore/state/export`.
pub async fn state_export_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let (_, state_value) = state.snapshots.snapshot(None);
    (
        StatusCode::OK,
        axum::Json(json!({
            "version": "1.0",
            "exported_at": crate::core::introspection::epoch_now(),
            "native_state": state_value,
        })),
    )
        .into_response()
}

/// Import handler: `POST /_robotocore/state/import`.
pub async fn state_import_handler(
    State(state): State<Arc<AppState>>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    if body.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(json!({"error": "No data provided"})),
        )
            .into_response();
    }
    let data = match state_body(body) {
        Ok(b) => b,
        Err((status, body)) => return (status, body).into_response(),
    };
    let native_state = data.get("native_state").cloned().unwrap_or(data);
    state.snapshots.restore(&native_state, None);
    (StatusCode::OK, axum::Json(json!({"status": "imported"}))).into_response()
}

/// Extract service name from URI, headers, or body.
///
/// Uses the full AWS service router first, falling back to the legacy
/// X-Amz-Target / path heuristics only when the router cannot decide.
fn extract_service(
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<String, String> {
    // Build a router request from method, URI, and headers
    let mut header_pairs: Vec<(String, String)> = headers
        .iter()
        .filter_map(|(k, v)| {
            let v = v.to_str().ok()?;
            Some((k.to_string(), v.to_string()))
        })
        .collect();
    // The full router checks the Host header for virtual-hosted services
    // (sqs.{region}.localhost.robotocore.cloud, S3 virtual hosts, etc.).
    // Axum strips Host from HeaderMap, so recover it from the authority.
    if !header_pairs
        .iter()
        .any(|(k, _)| k.eq_ignore_ascii_case("host"))
    {
        if let Some(authority) = uri.authority() {
            header_pairs.push(("host".to_string(), authority.as_str().to_string()));
        }
    }
    let router_req = AwsRequest {
        method: method.as_str().to_string(),
        path: uri.path().to_string(),
        query_string: uri.query().unwrap_or("").to_string(),
        headers: header_pairs,
    };
    if let Some(service) = route_to_service(&router_req) {
        // route_to_service defaults any unsigned form post on `/` to STS
        // (it cannot see the body). When the body names a concrete non-STS
        // operation, that default is a misroute: the client is talking to
        // some other service, and an STS-shaped InvalidAction error would
        // mislead the SDK. Veto the default and fall through to the
        // generic paths below instead. Signed requests keep faithful STS
        // behavior (the real endpoint answers InvalidAction XML for an
        // unknown operation).
        if !(service == "sts" && unsigned_non_sts_action(headers, uri, body).is_some()) {
            return Ok(service);
        }
    }

    // SimpleDB is botocore's only SigV2 service: the signature rides in the
    // form body, so there is no Authorization header for the credential-scope
    // check and route_to_service parks the request on the unsigned-STS
    // default, which the veto above then skips when the body names a non-STS
    // action — leaving sdb requests named by no service at all. sdb's
    // protocol pins Version=2009-04-15 in every request body; treat that
    // exact shape (unsigned form post carrying sdb's fixed API version) as
    // sdb so the rotocore backend owns the request.
    if body_is_sdb(headers, body) {
        return Ok("sdb".to_string());
    }

    // Fallback: X-Amz-Target header (contains "service.Operation")
    if let Some(target) = headers.get("X-Amz-Target") {
        if let Ok(target_str) = target.to_str() {
            if let Some(service) = target_str.split('.').next() {
                return Ok(service.to_lowercase());
            }
        }
    }

    // Fallback: path-based detection (e.g., /bucket/key for S3)
    let path = uri.path();
    if path == "/" && unsigned_non_sts_action(headers, uri, body).is_none() {
        // Default to sts for root path
        return Ok("sts".to_string());
    }

    // REST-path services (xray, cloudfront, bedrock family, arm paths) route by
    // credential scope only when the SDK signs with the exact service name;
    // scope "xray" lowercase etc. all handled by route_to_service. If we got
    // here, the request is likely signed for a service whose wire name equals
    // the first path segment. (Note: rotocore's dispatch is visited later with
    // whatever name lands; returning "" signals "unknown" and the gateway
    // returns a 501-style JSON error.)
    Err("request does not match a known AWS service endpoint".to_string())
}

/// A concrete AWS operation named by an *unsigned* request that is not an
/// STS operation, found in the `Action` form field or the query string.
/// `route_to_service` cannot see request bodies, so it parks every
/// unsigned form-bodied call on `/` with STS; when such a call names a
/// real non-STS action, answering with STS-shaped XML (e.g. "Unknown STS
/// operation") misleads SDKs into treating the twin as a broken STS
/// endpoint. Callers veto the STS default for these requests so the
/// generic gateway error paths can answer instead.
fn unsigned_non_sts_action(headers: &HeaderMap, uri: &Uri, body: &[u8]) -> Option<String> {
    if headers.get("Authorization").is_some() {
        return None;
    }
    let action_in = |encoded: &str| -> Option<String> {
        encoded.split('&').find_map(|pair| {
            let (k, v) = pair.split_once('=')?;
            (k == "Action" && !v.is_empty()).then(|| v.to_string())
        })
    };
    if let Some(action) = action_in(uri.query().unwrap_or("")) {
        return (!sts::is_known_sts_op(&action)).then_some(action);
    }
    // Only form posts may name an Action in the body — rule 7 of
    // route_to_service parks exactly these — so don't scan other body
    // types for stray "Action=" substrings.
    let form_body = headers
        .get("Content-Type")
        .and_then(|v| v.to_str().ok())
        .map(|t| t.to_ascii_lowercase().contains("x-www-form-urlencoded"))
        .unwrap_or(false);
    if form_body {
        if let Some(action) = action_in(&String::from_utf8_lossy(body)) {
            if !sts::is_known_sts_op(&action) {
                return Some(action);
            }
        }
    }
    None
}

/// True for an unsigned form post carrying SimpleDB's fixed API version
/// (2009-04-15) — the exact shape botocore's sdb client always emits: a
/// SigV2 POST form body (auth params inside the body, no Authorization
/// header). The sdb API version is unique to sdb among `Version=` form
/// fields, so this identifies only sdb requests.
fn body_is_sdb(headers: &HeaderMap, body: &[u8]) -> bool {
    if headers.get("Authorization").is_some() {
        return false;
    }
    let form_body = headers
        .get("Content-Type")
        .and_then(|v| v.to_str().ok())
        .map(|t| t.to_ascii_lowercase().contains("x-www-form-urlencoded"))
        .unwrap_or(false);
    form_body
        && std::str::from_utf8(body)
            .map(|b| b.contains("Version=2009-04-15"))
            .unwrap_or(false)
}

/// Extract account ID from Authorization header.
fn extract_account_from_request(headers: &HeaderMap, query_string: &str, body: &[u8]) -> u64 {
    // Explicit per-test override header wins over every credential-like
    // source (mirrors the python twin's x-moto-account-id priority).
    if let Some(account_header) = headers.get("X-Robotocore-Account") {
        if let Ok(account_str) = account_header.to_str() {
            if let Ok(account) = account_str.parse::<u64>() {
                return account;
            }
        }
    }

    // Try to get account from Authorization header
    if let Some(auth) = headers.get("Authorization") {
        if let Ok(auth_str) = auth.to_str() {
            // Look for Credential=AKIAIOSFODNN7EXAMPLE/...
            if let Some(cred_part) = auth_str.split("Credential=").nth(1) {
                if let Some(access_key) = cred_part.split('/').next() {
                    return parse_account_from_key(access_key);
                }
            }
        }
    }

    // Presigned requests carry credentials in the query string instead of
    // the Authorization header: X-Amz-Credential=KEY/DATE/REGION/SVC/aws4_request
    if let Some(cred) = query_string.split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=')?;
        if k != "X-Amz-Credential" {
            return None;
        }
        urlencoding::decode(v).ok().map(|c| c.into_owned())
    }) {
        if let Some(access_key) = cred.split('/').next() {
            return parse_account_from_key(access_key);
        }
    }

    // SigV2 bodies (SimpleDB is the only SigV2 service) carry the access
    // key in the form body: AWSAccessKeyId=KEY&... moto derives the account
    // the same way (moto/core/responses.py get_account_id_from), so the
    // documented per-account isolation holds for sdb too.
    if let Some(v) = String::from_utf8_lossy(body).split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=')?;
        (k == "AWSAccessKeyId" && !v.is_empty()).then(|| v.to_string())
    }) {
        let key = urlencoding::decode(&v).unwrap_or_default();
        if !key.is_empty() {
            return parse_account_from_key(&key);
        }
    }

    123456789012
}

/// Extract region from headers.
fn extract_region_from_query(query_string: &str) -> Option<String> {
    query_string.split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=')?;
        if k != "X-Amz-Credential" {
            return None;
        }
        let cred = urlencoding::decode(v).ok()?;
        cred.split('/').nth(2).map(|s| s.to_string())
    })
}

/// Extract region from headers.
fn extract_region_from_request(headers: &HeaderMap) -> Option<String> {
    if let Some(region) = headers.get("X-Robotocore-Region") {
        if let Ok(region_str) = region.to_str() {
            return Some(region_str.to_string());
        }
    }

    // Try Authorization header
    if let Some(auth) = headers.get("Authorization") {
        if let Ok(auth_str) = auth.to_str() {
            // Look for credential format: AKIA.../20230101/us-east-1/...
            if let Some(cred_part) = auth_str.split("Credential=").nth(1) {
                let parts: Vec<&str> = cred_part.split('/').collect();
                if parts.len() >= 3 {
                    return Some(parts[2].to_string());
                }
            }
        }
    }

    None
}

/// Convert ParsedResponse to Axum response with appropriate protocol encoding.
fn response_from_parsed(resp: ParsedResponse, service: &str, operation: &str) -> Response {
    use crate::core::protocol::serialize_query_response;

    let request_id = Uuid::new_v4().to_string();

    // Arbitrary bytes first (binary objects, event streams), then
    // pre-serialized string bodies, then protocol encoding.
    let body: axum::body::Body = if let Some(bytes) = resp.raw_bytes {
        axum::body::Body::from(bytes)
    } else if let Some(raw) = resp.raw {
        // Pre-serialized body: send verbatim (e.g. native JSON-protocol services).
        axum::body::Body::from(raw)
    } else {
        let body_str = if service == "sts" {
            serialize_query_response(&resp.body, operation, &request_id)
        } else {
            serde_json::to_string(&resp.body).unwrap_or_else(|_| "{}".to_string())
        };
        axum::body::Body::from(body_str)
    };

    let mut headers_map = axum::http::HeaderMap::new();
    for (key, value) in resp.headers {
        if let Ok(header_value) = HeaderValue::from_str(&value) {
            if let Ok(header_name) = axum::http::HeaderName::from_bytes(key.as_bytes()) {
                headers_map.insert(header_name, header_value);
            }
        }
    }
    headers_map.insert("server", HeaderValue::from_static("robotocore"));
    headers_map.insert(
        "x-robotocore-request-id",
        HeaderValue::from_str(&request_id).unwrap(),
    );

    let mut response = Response::builder().status(resp.status).body(body).unwrap();
    *response.headers_mut() = headers_map;
    response
}

/// Build an error response shaped like an AWS SDK error so clients parse it.
fn error_response(status: StatusCode, message: &str) -> Response {
    // The one error shape a twin-wide convention depends on: 501 means the
    // operation is genuinely absent (see AGENTS.md "Reading errors
    // correctly"), so it must carry the NotImplemented error code, not
    // InternalError — SDKs treat InternalError as "implemented and broken".
    let errortype = if status == StatusCode::NOT_IMPLEMENTED {
        "NotImplemented"
    } else {
        "InternalError"
    };
    let error = json!({
        "__type": errortype,
        "message": message,
    });

    (
        status,
        [("x-amzn-errortype", errortype.to_string())],
        axum::Json(error),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_service_from_target_header() {
        let mut headers = HeaderMap::new();
        headers.insert("X-Amz-Target", "STS.GetCallerIdentity".parse().unwrap());

        let uri = "/".parse().unwrap();
        let method = Method::POST;
        let service = extract_service(&method, &uri, &headers, b"").unwrap();
        assert_eq!(service, "sts");
    }

    #[test]
    fn test_extract_account_from_auth_header() {
        let mut headers = HeaderMap::new();
        headers.insert("Authorization", "AWS4-HMAC-SHA256 Credential=123456789012/20230101/us-east-1/sts/aws4_request, SignedHeaders=host;x-amz-date, Signature=xyz".parse().unwrap());

        let account = extract_account_from_request(&headers, "", b"");
        assert_eq!(account, 123456789012);
    }

    #[test]
    fn test_extract_account_from_presigned_query() {
        // X-Amz-Credential in the query string routes to that account too
        let headers = HeaderMap::new();
        let account = extract_account_from_request(&headers, "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=222222222222%2F20260101%2Fus-east-1%2Fs3%2Faws4_request", b"");
        assert_eq!(account, 222222222222);
    }

    #[test]
    fn test_extract_account_from_sigv2_body() {
        // SimpleDB is SigV2: AWSAccessKeyId rides the form body (moto's
        // get_account_id_from path), so per-account isolation holds.
        let headers = HeaderMap::new();
        let account = extract_account_from_request(
            &headers,
            "",
            b"Action=CreateDomain&AWSAccessKeyId=222222222222&Signature=x%3D",
        );
        assert_eq!(account, 222222222222);
    }

    #[test]
    fn test_registry_creation() {
        let registry = ServiceRegistry::new();
        assert!(registry.get("sts").is_some());
        assert!(registry.get("sqs").is_some());
    }

    #[test]
    fn test_extract_service_sqs_via_target_header() {
        let mut headers = HeaderMap::new();
        headers.insert("X-Amz-Target", "AmazonSQS.SendMessage".parse().unwrap());
        let uri = "/".parse().unwrap();
        let method = Method::POST;
        let service = extract_service(&method, &uri, &headers, b"").unwrap();
        assert_eq!(service, "sqs");
    }

    #[test]
    fn test_extract_service_sqs_via_auth_scope() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=123456789012/20260101/us-east-1/sqs/aws4_request, SignedHeaders=host, Signature=abc".parse().unwrap(),
        );
        let uri = "/".parse().unwrap();
        let method = Method::POST;
        let service = extract_service(&method, &uri, &headers, b"").unwrap();
        assert_eq!(service, "sqs");
    }

    #[test]
    fn test_extract_service_sqs_via_host_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "Host",
            "sqs.us-east-1.localhost.robotocore.cloud:4566"
                .parse()
                .unwrap(),
        );
        let uri = "/123456789012/my-queue".parse().unwrap();
        let method = Method::POST;
        let service = extract_service(&method, &uri, &headers, b"").unwrap();
        assert_eq!(service, "sqs");
    }

    #[test]
    fn test_extract_service_unsigned_unknown_action_is_not_sts() {
        // An unsigned form post naming a concrete non-STS operation must
        // not park on STS — the twin previously answered such requests
        // with STS-shaped "Unknown STS operation" XML that SDKs read as a
        // real STS failure.
        let mut headers = HeaderMap::new();
        headers.insert(
            "content-type",
            "application/x-www-form-urlencoded".parse().unwrap(),
        );
        let uri = "/".parse().unwrap();
        let method = Method::POST;
        let err = extract_service(
            &method,
            &uri,
            &headers,
            b"Action=CreateVault&Version=2012-06-15",
        )
        .unwrap_err();
        assert!(err.contains("request does not match a known AWS service endpoint"));
    }

    #[test]
    fn test_extract_service_unsigned_post_to_root_without_action_is_sts() {
        let headers = HeaderMap::new();
        let uri = "/".parse().unwrap();
        let method = Method::POST;
        let service = extract_service(&method, &uri, &headers, b"").unwrap();
        assert_eq!(service, "sts");
    }

    #[test]
    fn test_extract_service_unsigned_unknown_action_with_target() {
        // Same request, but carrying an X-Amz-Target: the target's service
        // prefix wins and the gateway degrades to the generic
        // NotImplemented path later ("Service robocorefake not
        // implemented") instead of an STS-shaped error.
        let mut headers = HeaderMap::new();
        headers.insert("X-Amz-Target", "RobocoreFake.NonExistent".parse().unwrap());
        let uri = "/".parse().unwrap();
        let method = Method::POST;
        let service = extract_service(&method, &uri, &headers, b"Action=Foo").unwrap();
        assert_eq!(service, "robocorefake");
    }

    #[test]
    fn test_extract_service_signed_sts_unknown_action_still_sts() {
        // A signed STS request with an unknown operation belongs to STS;
        // the handler keeps faithful InvalidAction behavior.
        let mut headers = HeaderMap::new();
        headers.insert(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=123456789012/20260101/us-east-1/sts/aws4_request, SignedHeaders=host, Signature=abc"
                .parse()
                .unwrap(),
        );
        let uri = "/".parse().unwrap();
        let method = Method::POST;
        let service = extract_service(&method, &uri, &headers, b"Action=NotARealOp").unwrap();
        assert_eq!(service, "sts");
    }

    #[test]
    fn test_error_response_501_carries_not_implemented() {
        let resp = error_response(StatusCode::NOT_IMPLEMENTED, "Service x not implemented");
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
        assert_eq!(
            resp.headers().get("x-amzn-errortype").unwrap(),
            "NotImplemented"
        );
        let other = error_response(StatusCode::SERVICE_UNAVAILABLE, "down");
        assert_eq!(
            other.headers().get("x-amzn-errortype").unwrap(),
            "InternalError"
        );
    }
}
