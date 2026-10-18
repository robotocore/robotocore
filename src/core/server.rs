//! Axum-based HTTP server for the Robotocore Rust implementation.
//!
//! Implements catch-all routing, protocol detection, and service dispatch.

use axum::{
    extract::State,
    http::{HeaderMap, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use http_body_util::BodyExt;
use once_cell::sync::Lazy;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
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
    inner: sqs::DefaultSqsHandler,
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
    inner: s3::DefaultS3Handler,
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
        ParsedResponse {
            status: StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            headers,
            body: serde_json::Value::Null,
            raw: Some(String::from_utf8_lossy(&resp.body).to_string()),
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
            if pair.is_empty() { continue; }
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
        let operation = s3::handler::S3Handler::detect_s3_operation(
            &req.method,
            &req.path,
            &query_params,
        ).unwrap_or_else(|| req.operation.clone());

        let mut s3_req = Self::to_s3_request(req, &req.method, &req.query_string);
        s3_req.operation = operation;
        let resp = self.inner.handle(s3_req);
        Ok(Self::to_parsed_response(resp))
    }
}

/// Adapter that bridges the core protocol to the native DynamoDB service crate.
pub struct DynamoDbServiceHandler {
    inner: dynamodb::DefaultDynamoDbHandler,
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
    inner: sns::DefaultSnsHandler,
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
    inner: secretsmanager::DefaultSecretsManagerHandler,
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
    inner: kms::DefaultKmsHandler,
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
        for (k, v) in resp.headers { headers.insert(k, v); }
        Ok(ParsedResponse {
            status: StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            headers,
            body: serde_json::Value::Null,
            raw: Some(resp.body),
        })
    }
}

/// Adapter for SSM service crate.
pub struct SsmServiceHandler {
    inner: ssm::DefaultSsmHandler,
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
        for (k, v) in resp.headers { headers.insert(k, v); }
        Ok(ParsedResponse {
            status: StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            headers,
            body: serde_json::Value::Null,
            raw: Some(resp.body),
        })
    }
}

/// Adapter for IAM service crate.
pub struct IamServiceHandler {
    inner: iam::DefaultIamHandler,
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
        for (k, v) in resp.headers { headers.insert(k, v); }
        Ok(ParsedResponse {
            status: StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            headers,
            body: serde_json::Value::Null,
            raw: Some(resp.body),
        })
    }
}

/// Adapter for Lambda service crate.
pub struct LambdaServiceHandler {
    inner: lambda::DefaultLambdaHandler,
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
        for (k, v) in resp.headers { headers.insert(k, v); }
        Ok(ParsedResponse {
            status: StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            headers,
            body: serde_json::Value::Null,
            raw: Some(resp.body),
        })
    }
}

/// Adapter for CloudWatch Logs service crate.
pub struct LogsServiceHandler {
    inner: cloudwatch_logs::DefaultLogsHandler,
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
        for (k, v) in resp.headers { headers.insert(k, v); }
        Ok(ParsedResponse {
            status: StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            headers,
            body: serde_json::Value::Null,
            raw: Some(resp.body),
        })
    }
}

/// Adapter for EventBridge service crate.
pub struct EventsServiceHandler {
    inner: events::DefaultEventsHandler,
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
        for (k, v) in resp.headers { headers.insert(k, v); }
        Ok(ParsedResponse {
            status: StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            headers,
            body: serde_json::Value::Null,
            raw: Some(resp.body),
        })
    }
}

/// Adapter for Kinesis service crate.
pub struct KinesisServiceHandler {
    inner: kinesis::DefaultKinesisHandler,
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
        for (k, v) in resp.headers { headers.insert(k, v); }
        Ok(ParsedResponse {
            status: StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            headers,
            body: serde_json::Value::Null,
            raw: Some(resp.body),
        })
    }
}

/// Adapter for Firehose.
pub struct FirehoseServiceHandler { inner: firehose::DefaultFirehoseHandler }
impl ServiceHandler for FirehoseServiceHandler {
    fn handle_sync(&self, req: &ParsedRequest) -> Result<ParsedResponse, Box<dyn std::error::Error>> {
        let params = serde_json::to_value(&req.params).unwrap_or_default();
        let r = firehose::protocol::AwsRequest { service: req.service.clone(), operation: req.operation.clone(), account: req.account, region: req.region.clone(), params, body: req.body.clone() };
        let resp = self.inner.handle(r);
        let mut h = std::collections::HashMap::new();
        for (k, v) in resp.headers { h.insert(k, v); }
        Ok(ParsedResponse { status: StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR), headers: h, body: serde_json::Value::Null, raw: Some(resp.body) })
    }
}

/// Adapter for CloudWatch.
pub struct CloudWatchServiceHandler { inner: cloudwatch::DefaultCloudwatchHandler }
impl ServiceHandler for CloudWatchServiceHandler {
    fn handle_sync(&self, req: &ParsedRequest) -> Result<ParsedResponse, Box<dyn std::error::Error>> {
        let params = serde_json::to_value(&req.params).unwrap_or_default();
        let r = cloudwatch::protocol::AwsRequest { service: req.service.clone(), operation: req.operation.clone(), account: req.account, region: req.region.clone(), params, body: req.body.clone() };
        let resp = self.inner.handle(r);
        let mut h = std::collections::HashMap::new();
        for (k, v) in resp.headers { h.insert(k, v); }
        Ok(ParsedResponse { status: StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR), headers: h, body: serde_json::Value::Null, raw: Some(resp.body) })
    }
}

/// Adapter for ECR.
pub struct EcrServiceHandler { inner: ecr::DefaultEcrHandler }
impl ServiceHandler for EcrServiceHandler {
    fn handle_sync(&self, req: &ParsedRequest) -> Result<ParsedResponse, Box<dyn std::error::Error>> {
        let params = serde_json::to_value(&req.params).unwrap_or_default();
        let r = ecr::protocol::AwsRequest { service: req.service.clone(), operation: req.operation.clone(), account: req.account, region: req.region.clone(), params, body: req.body.clone() };
        let resp = self.inner.handle(r);
        let mut h = std::collections::HashMap::new();
        for (k, v) in resp.headers { h.insert(k, v); }
        Ok(ParsedResponse { status: StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR), headers: h, body: serde_json::Value::Null, raw: Some(resp.body) })
    }
}

/// Adapter for ECS.
pub struct EcsServiceHandler { inner: ecs::DefaultEcsHandler }
impl ServiceHandler for EcsServiceHandler {
    fn handle_sync(&self, req: &ParsedRequest) -> Result<ParsedResponse, Box<dyn std::error::Error>> {
        let params = serde_json::to_value(&req.params).unwrap_or_default();
        let r = ecs::protocol::AwsRequest { service: req.service.clone(), operation: req.operation.clone(), account: req.account, region: req.region.clone(), params, body: req.body.clone() };
        let resp = self.inner.handle(r);
        let mut h = std::collections::HashMap::new();
        for (k, v) in resp.headers { h.insert(k, v); }
        Ok(ParsedResponse { status: StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR), headers: h, body: serde_json::Value::Null, raw: Some(resp.body) })
    }
}

/// Adapter for Step Functions.
pub struct StepFunctionsServiceHandler { inner: stepfunctions::DefaultStepfunctionsHandler }
impl ServiceHandler for StepFunctionsServiceHandler {
    fn handle_sync(&self, req: &ParsedRequest) -> Result<ParsedResponse, Box<dyn std::error::Error>> {
        let params = serde_json::to_value(&req.params).unwrap_or_default();
        let r = stepfunctions::protocol::AwsRequest { service: req.service.clone(), operation: req.operation.clone(), account: req.account, region: req.region.clone(), params, body: req.body.clone() };
        let resp = self.inner.handle(r);
        let mut h = std::collections::HashMap::new();
        for (k, v) in resp.headers { h.insert(k, v); }
        Ok(ParsedResponse { status: StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR), headers: h, body: serde_json::Value::Null, raw: Some(resp.body) })
    }
}

/// Registry of service handlers.
pub struct ServiceRegistry {
    handlers: HashMap<String, Arc<dyn ServiceHandler>>,
}

impl ServiceRegistry {
    /// Create a new registry with built-in services.
    pub fn new() -> Self {
        let mut handlers: HashMap<String, Arc<dyn ServiceHandler>> = HashMap::new();

        // Register STS handler
        handlers.insert(
            "sts".to_string(),
            Arc::new(StsFunctionHandler) as Arc<dyn ServiceHandler>,
        );

        // Register native SQS handler
        handlers.insert(
            "sqs".to_string(),
            Arc::new(SqsServiceHandler {
                inner: sqs::DefaultSqsHandler::new(),
            }) as Arc<dyn ServiceHandler>,
        );

        // Register native S3 handler
        handlers.insert(
            "s3".to_string(),
            Arc::new(S3ServiceHandler {
                inner: s3::DefaultS3Handler::new(),
            }) as Arc<dyn ServiceHandler>,
        );

        // Register native DynamoDB handler
        handlers.insert(
            "dynamodb".to_string(),
            Arc::new(DynamoDbServiceHandler {
                inner: dynamodb::DefaultDynamoDbHandler::new(),
            }) as Arc<dyn ServiceHandler>,
        );

        // Register native SNS handler
        handlers.insert(
            "sns".to_string(),
            Arc::new(SnsServiceHandler {
                inner: sns::DefaultSnsHandler::new(),
            }) as Arc<dyn ServiceHandler>,
        );

        // Register native Secrets Manager handler
        handlers.insert(
            "secretsmanager".to_string(),
            Arc::new(SmServiceHandler {
                inner: secretsmanager::DefaultSecretsManagerHandler::new(),
            }) as Arc<dyn ServiceHandler>,
        );

        // Register native KMS handler
        handlers.insert(
            "kms".to_string(),
            Arc::new(KmsServiceHandler {
                inner: kms::DefaultKmsHandler::new(),
            }) as Arc<dyn ServiceHandler>,
        );

        // Register native SSM handler
        handlers.insert(
            "ssm".to_string(),
            Arc::new(SsmServiceHandler {
                inner: ssm::DefaultSsmHandler::new(),
            }) as Arc<dyn ServiceHandler>,
        );

        // Register native IAM handler
        handlers.insert(
            "iam".to_string(),
            Arc::new(IamServiceHandler {
                inner: iam::DefaultIamHandler::new(),
            }) as Arc<dyn ServiceHandler>,
        );

        // Register native Lambda handler
        handlers.insert(
            "lambda".to_string(),
            Arc::new(LambdaServiceHandler {
                inner: lambda::DefaultLambdaHandler::new(),
            }) as Arc<dyn ServiceHandler>,
        );

        // Register native CloudWatch Logs handler
        handlers.insert(
            "logs".to_string(),
            Arc::new(LogsServiceHandler {
                inner: cloudwatch_logs::DefaultLogsHandler::new(),
            }) as Arc<dyn ServiceHandler>,
        );

        // Register native EventBridge handler
        handlers.insert(
            "events".to_string(),
            Arc::new(EventsServiceHandler {
                inner: events::DefaultEventsHandler::new(),
            }) as Arc<dyn ServiceHandler>,
        );

        // Register native Kinesis handler
        handlers.insert(
            "kinesis".to_string(),
            Arc::new(KinesisServiceHandler {
                inner: kinesis::DefaultKinesisHandler::new(),
            }) as Arc<dyn ServiceHandler>,
        );

        // Register native Firehose handler
        handlers.insert(
            "firehose".to_string(),
            Arc::new(FirehoseServiceHandler {
                inner: firehose::DefaultFirehoseHandler::new(),
            }) as Arc<dyn ServiceHandler>,
        );

        // Register native CloudWatch handler
        handlers.insert(
            "cloudwatch".to_string(),
            Arc::new(CloudWatchServiceHandler {
                inner: cloudwatch::DefaultCloudwatchHandler::new(),
            }) as Arc<dyn ServiceHandler>,
        );

        // Register native ECR handler
        handlers.insert(
            "ecr".to_string(),
            Arc::new(EcrServiceHandler {
                inner: ecr::DefaultEcrHandler::new(),
            }) as Arc<dyn ServiceHandler>,
        );

        // Register native ECS handler
        handlers.insert(
            "ecs".to_string(),
            Arc::new(EcsServiceHandler {
                inner: ecs::DefaultEcsHandler::new(),
            }) as Arc<dyn ServiceHandler>,
        );

        // Register native Step Functions handler
        handlers.insert(
            "stepfunctions".to_string(),
            Arc::new(StepFunctionsServiceHandler {
                inner: stepfunctions::DefaultStepfunctionsHandler::new(),
            }) as Arc<dyn ServiceHandler>,
        );

        Self { handlers }
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
    pub moto_proxy: Option<crate::core::proxy::MotoProxy>,
}

/// Catch-all handler for all AWS API requests.
pub async fn catch_all_handler(
    State(state): State<Arc<AppState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: axum::body::Body,
) -> impl IntoResponse {
    // Read the body
    let body_bytes = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(_) => return error_response(StatusCode::BAD_REQUEST, "Invalid body"),
    };

    // Determine service from URI and headers
    let service = match extract_service(&method, &uri, &headers) {
        Ok(s) => s,
        Err(e) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                &format!("Could not determine service: {}", e),
            )
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
    let operation = extract_operation(&method, &headers_with_path, &body_bytes, &service, &query_string)
        .unwrap_or_default();

    // Extract account from Authorization header or default
    let account = extract_account_from_request(&headers);
    let region = extract_region_from_request(&headers).unwrap_or_else(|| "us-east-1".to_string());

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
    bump_service_requests(&service);
    let handler = match handler {
        Some(h) => h,
        None => {
            // Fall back to Moto proxy for non-native services
            if let Some(proxy) = &state.moto_proxy {
                if !proxy.is_native(&service) {                    match proxy.forward(&parsed_req).await {
                        Ok(resp) => {
                            crate::core::introspection::record(
                                &service,
                                &parsed_req.operation,
                                resp.status.as_u16(),
                                account,
                                &parsed_req.region,
                            );
                            return response_from_parsed(resp, &service, &parsed_req.operation);
                        }
                        Err(e) => {
                            return error_response(
                                StatusCode::SERVICE_UNAVAILABLE,
                                &format!("Moto proxy failed: {}", e),
                            )
                        }
                    }
                }
            }
            return error_response(
                StatusCode::NOT_IMPLEMENTED,
                &format!("Service {} not implemented", service),
            )
        }
    };

    // Handle request with a timeout to catch runaway tasks
    let handler = handler.clone();
    let parsed_req = Arc::new(parsed_req);
    let parsed_req_for_log = Arc::clone(&parsed_req);
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
            crate::core::introspection::record(&service, &op, resp.status.as_u16(), account, &parsed_req_for_log.region);
            response_from_parsed(resp, &service, &op)
        }
        Ok(Ok(Err(e))) => {
            crate::core::introspection::record(&service, &operation, 500, account, &parsed_req_for_log.region);
            error_response(StatusCode::INTERNAL_SERVER_ERROR, &e)
        }
        Ok(Err(e)) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Handler task failed: {}", e),
        ),
        Err(_) => {
            tracing::warn!("Request timed out after {}s: {} {}", timeout_secs, method, uri);
            error_response(
                StatusCode::GATEWAY_TIMEOUT,
                "Request timed out",
            )
        }
    }
}

/// Health check endpoint
pub async fn health_handler() -> impl IntoResponse {
    let health = json!({
        "status": "running",
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_seconds": 0.0,
        "services": registry_service_statuses()
    });

    (StatusCode::OK, axum::Json(health))
}

/// Names of every service with a native handler in this build.
pub const NATIVE_SERVICES: &[&str] = &[
    "sts", "sqs", "s3", "dynamodb", "sns", "secretsmanager", "kms", "ssm",
    "iam", "lambda", "logs", "events", "kinesis", "firehose", "cloudwatch",
    "ecr", "ecs", "stepfunctions",
];

/// Per-service request counters, shared with the audit log.
pub static SERVICE_REQUESTS: Lazy<std::collections::HashMap<&'static str, AtomicU64>> = Lazy::new(|| {
    NATIVE_SERVICES
        .iter()
        .map(|s| (*s, AtomicU64::new(0)))
        .collect()
});

fn bump_service_requests(service: &str) {
    if let Some(counter) = SERVICE_REQUESTS.get(service) {
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

fn service_request_count(service: &str) -> u64 {
    SERVICE_REQUESTS
        .get(service)
        .map(|c| c.load(Ordering::Relaxed))
        .unwrap_or(0)
}

fn registry_service_statuses() -> serde_json::Map<String, serde_json::Value> {
    let mut map = serde_json::Map::new();
    for svc in NATIVE_SERVICES {
        map.insert(
            svc.to_string(),
            json!({
                "status": "running",
                "type": "native",
                "requests": service_request_count(svc),
            }),
        );
    }
    map
}

/// Runtime families report. The Rust lambda handler executes Python and
/// shell-based (custom) handlers in-process; other families are not available.
pub async fn runtimes_handler() -> impl IntoResponse {
    let all = ["python", "nodejs", "java", "ruby", "dotnet", "go", "custom"];
    let node_paths = ["/usr/bin/node", "/usr/local/bin/node"];
    let mut available: Vec<String> = vec!["python".into(), "custom".into()];
    if node_paths.iter().any(|p| std::path::Path::new(p).exists()) {
        available.push("nodejs".into());
    }
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

/// Audit endpoint: `?limit=N` recent call records.
pub async fn audit_handler(uri: Uri) -> impl IntoResponse {
    let mut limit = 100usize;
    if let Some(q) = uri.query() {
        for kv in q.split('&') {
            if let Some((k, v)) = kv.split_once('=') {
                if k == "limit" {
                    if let Ok(n) = v.parse::<usize>() {
                        limit = n.min(1000);
                    }
                }
            }
        }
    }
    let entries = crate::core::introspection::recent_entries(limit);
    (
        StatusCode::OK,
        axum::Json(json!({
            "entries": entries,
            "count": entries.len(),
        })),
    )
}

/// Per-service protocol listing: `GET /_robotocore/services`.
pub async fn services_handler() -> impl IntoResponse {
    let protocols: &[(&str, &str)] = &[
        ("sqs", "query"),
        ("sns", "query"),
        ("secretsmanager", "json"),
        ("kms", "json"),
        ("ssm", "json"),
        ("iam", "query"),
        ("lambda", "rest-json"),
        ("logs", "json"),
        ("events", "json"),
        ("kinesis", "json"),
        ("firehose", "json"),
        ("cloudwatch", "query"),
        ("ecr", "json"),
        ("ecs", "json"),
        ("stepfunctions", "json"),
        ("dynamodb", "json"),
        ("sts", "query"),
        ("s3", "rest-xml"),
    ];
    let entries: Vec<serde_json::Value> = protocols
        .iter()
        .map(|(name, protocol)| {
            json!({
                "name": name,
                "protocol": protocol,
                "description": name,
                "status": "running",
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
pub fn build_router(registry: ServiceRegistry, moto_proxy: Option<crate::core::proxy::MotoProxy>) -> Router {
    let state = Arc::new(AppState {
        registry,
        moto_proxy,
    });

    Router::new()
        .route("/_robotocore/health", get(health_handler))
        .route("/_robotocore/config", get(config_handler))
        .route("/_robotocore/audit", get(audit_handler))
        .route("/_robotocore/runtimes", get(runtimes_handler))
        .route("/_robotocore/services", get(services_handler))
        .route("/_robotocore/usage/errors", get(usage_errors_handler))
        .fallback(catch_all_handler)
        .with_state(state)
}

/// Extract service name from URI, headers, or body.
///
/// Uses the full AWS service router first, falling back to the legacy
/// X-Amz-Target / path heuristics only when the router cannot decide.
fn extract_service(method: &Method, uri: &Uri, headers: &HeaderMap) -> Result<String, String> {
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
        return Ok(service);
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
    if path.starts_with("/") {
        // Default to sts for root path
        if path == "/" {
            return Ok("sts".to_string());
        }
    }

    Err("Could not determine service".to_string())
}

/// Extract account ID from Authorization header.
fn extract_account_from_request(headers: &HeaderMap) -> u64 {
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

    // Try custom headers
    if let Some(account_header) = headers.get("X-Robotocore-Account") {
        if let Ok(account_str) = account_header.to_str() {
            if let Ok(account) = account_str.parse::<u64>() {
                return account;
            }
        }
    }

    123456789012
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

    let body_str = if let Some(raw) = resp.raw {
        // Pre-serialized body: send verbatim (e.g. native JSON-protocol services).
        raw
    } else if service == "sts" {
        serialize_query_response(&resp.body, operation, &request_id)
    } else {
        serde_json::to_string(&resp.body).unwrap_or_else(|_| "{}".to_string())
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
    headers_map.insert("x-robotocore-request-id", HeaderValue::from_str(&request_id).unwrap());

    (resp.status, headers_map, body_str).into_response()
}

/// Build an error response.
fn error_response(status: StatusCode, message: &str) -> Response {
    let error = json!({
        "error": message
    });

    (status, axum::Json(error)).into_response()
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
        let service = extract_service(&method, &uri, &headers).unwrap();
        assert_eq!(service, "sts");
    }

    #[test]
    fn test_extract_account_from_auth_header() {
        let mut headers = HeaderMap::new();
        headers.insert("Authorization", "AWS4-HMAC-SHA256 Credential=123456789012/20230101/us-east-1/sts/aws4_request, SignedHeaders=host;x-amz-date, Signature=xyz".parse().unwrap());

        let account = extract_account_from_request(&headers);
        assert_eq!(account, 123456789012);
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
        let service = extract_service(&method, &uri, &headers).unwrap();
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
        let service = extract_service(&method, &uri, &headers).unwrap();
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
        let service = extract_service(&method, &uri, &headers).unwrap();
        assert_eq!(service, "sqs");
    }
}
