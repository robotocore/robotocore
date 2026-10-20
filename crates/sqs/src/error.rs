//! SQS error types and responses

use crate::protocol::AwsResponse;
use serde_json::json;

#[derive(Debug, Clone)]
pub enum SqsError {
    NonExistentQueue(String),
    InvalidAttributeName(String),
    ReceiptHandleIsInvalid(String),
    MessageNotInflight(String),
    InvalidQueueName(String),
    MissingParameter(String),
    ValidationError(String),
}

impl SqsError {
    pub fn code(&self) -> &str {
        match self {
            SqsError::NonExistentQueue(_) => "AWS.SimpleQueueService.NonExistentQueue",
            SqsError::InvalidAttributeName(_) => "InvalidAttributeName",
            SqsError::ReceiptHandleIsInvalid(_) => "ReceiptHandleIsInvalid",
            SqsError::MessageNotInflight(_) => "MessageNotInflight",
            SqsError::InvalidQueueName(_) => "InvalidQueueName",
            SqsError::MissingParameter(_) => "MissingParameter",
            SqsError::ValidationError(_) => "ValidationException",
        }
    }

    pub fn message(&self) -> &str {
        match self {
            SqsError::NonExistentQueue(msg) => msg,
            SqsError::InvalidAttributeName(msg) => msg,
            SqsError::ReceiptHandleIsInvalid(msg) => msg,
            SqsError::MessageNotInflight(msg) => msg,
            SqsError::InvalidQueueName(msg) => msg,
            SqsError::MissingParameter(msg) => msg,
            SqsError::ValidationError(msg) => msg,
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
            SqsError::InvalidQueueName(_) => 400,
            SqsError::MissingParameter(_) => 400,
            SqsError::ValidationError(_) => 400,
        }
    }

    /// Convert error to JSON response for modern AWS JSON protocol
    pub fn to_json_response(&self) -> AwsResponse {
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
