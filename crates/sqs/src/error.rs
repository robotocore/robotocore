//! SQS error types and responses

use crate::protocol::AwsResponse;
use serde_json::json;

/// moto's unhandled-exception page (werkzeug plain 500): the recorded
/// wire shape for paths where the pinned moto fork crashes outright
/// (missing QueueName on CreateQueue / missing MessageBody on SendMessage,
/// missing ReceiptHandle on ChangeMessageVisibility, unknown actions).
const MOTO_CRASH_HTML: &str = "<!doctype html>\n\
     <html lang=en>\n\
     <title>500 Internal Server Error</title>\n\
     <h1>Internal Server Error</h1>\n\
     <p>The server encountered an internal error and was unable to complete your request. \
     Either the server is overloaded or there is an error in the application.</p>\n";

#[derive(Debug, Clone)]
pub enum SqsError {
    NonExistentQueue(String),
    InvalidAttributeName(String),
    ReceiptHandleIsInvalid(String),
    MessageNotInflight(String),
    InvalidParameterValue(String),
    MissingParameter(String),
    ResourceNotFound(String),
    EmptyBatchRequest(String),
    /// Crash-faithful row: moto escapes an unhandled Python error on these
    /// inputs (werkzeug plain HTML 500, no AWS error envelope).
    MotoCrash,
}

impl SqsError {
    pub fn code(&self) -> &str {
        match self {
            SqsError::NonExistentQueue(_) => "AWS.SimpleQueueService.NonExistentQueue",
            SqsError::InvalidAttributeName(_) => "InvalidAttributeName",
            SqsError::ReceiptHandleIsInvalid(_) => "ReceiptHandleIsInvalid",
            SqsError::MessageNotInflight(_) => "MessageNotInflight",
            SqsError::InvalidParameterValue(_) => "InvalidParameterValue",
            SqsError::MissingParameter(_) => "MissingParameter",
            SqsError::ResourceNotFound(_) => "ResourceNotFoundException",
            SqsError::EmptyBatchRequest(_) => "AWS.SimpleQueueService.EmptyBatchRequest",
            SqsError::MotoCrash => "InternalError",
        }
    }

    pub fn message(&self) -> &str {
        match self {
            SqsError::NonExistentQueue(msg) => msg,
            SqsError::InvalidAttributeName(msg) => msg,
            SqsError::ReceiptHandleIsInvalid(msg) => msg,
            SqsError::MessageNotInflight(msg) => msg,
            SqsError::InvalidParameterValue(msg) => msg,
            SqsError::MissingParameter(msg) => msg,
            SqsError::ResourceNotFound(msg) => msg,
            SqsError::EmptyBatchRequest(msg) => msg,
            SqsError::MotoCrash => "",
        }
    }

    pub fn status_code(&self) -> u16 {
        match self {
            // AWS query-protocol SQS errors are 400; NonExistentQueue included
            // (moto's RESTError defaults to 400).
            SqsError::NonExistentQueue(_) => 400,
            SqsError::InvalidAttributeName(_) => 400,
            SqsError::ReceiptHandleIsInvalid(_) => 400,
            SqsError::MessageNotInflight(_) => 400,
            SqsError::InvalidParameterValue(_) => 400,
            SqsError::MissingParameter(_) => 400,
            SqsError::ResourceNotFound(_) => 400,
            SqsError::EmptyBatchRequest(_) => 400,
            SqsError::MotoCrash => 500,
        }
    }

    /// Convert error to JSON response for modern AWS JSON protocol
    pub fn to_json_response(&self) -> AwsResponse {
        if matches!(self, SqsError::MotoCrash) {
            return AwsResponse {
                status: 500,
                headers: vec![(
                    "Content-Type".to_string(),
                    "text/html; charset=utf-8".to_string(),
                )],
                body: MOTO_CRASH_HTML.to_string(),
            };
        }
        let body = json!({
            "__type": self.code(),
            "message": self.message(),
        })
        .to_string();

        AwsResponse {
            status: self.status_code(),
            headers: vec![(
                "Content-Type".to_string(),
                "application/x-amz-json-1.0".to_string(),
            )],
            body,
        }
    }

    /// Convert error to XML response for query protocol
    pub fn to_xml_response(&self) -> AwsResponse {
        if matches!(self, SqsError::MotoCrash) {
            return AwsResponse {
                status: 500,
                headers: vec![(
                    "Content-Type".to_string(),
                    "text/html; charset=utf-8".to_string(),
                )],
                body: MOTO_CRASH_HTML.to_string(),
            };
        }
        let body = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ErrorResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/">
  <Error>
    <Type>Sender</Type>
    <Code>{}</Code>
    <Message>{}</Message>
  </Error>
  <RequestId>00000000-0000-0000-0000-000000000000</RequestId>
</ErrorResponse>"#,
            self.code(),
            self.message()
        );

        AwsResponse {
            status: self.status_code(),
            headers: vec![("Content-Type".to_string(), "text/xml".to_string())],
            body,
        }
    }
}
