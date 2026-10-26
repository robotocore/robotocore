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
    /// moto's `InvalidBatchEntryId` (moto/sqs/exceptions.py): a batch
    /// entry Id fails `^[\w-]{1,80}$`. The wire carries the
    /// service-prefixed code (AWS.SimpleQueueService.InvalidBatchEntryId),
    /// matching every other top-level SQS batch error.
    InvalidBatchEntryId,
    /// moto's `BatchEntryIdsNotDistinct`: carries the formatted
    /// "Id {id} repeated." message.
    BatchEntryIdsNotDistinct(String),
    /// moto's `TooManyEntriesInBatchRequest`: carries the formatted
    /// "Maximum number of entries per request are 10. You have sent {n}."
    TooManyEntriesInBatchRequest(String),
    /// moto's `BatchRequestTooLong` (moto/sqs/exceptions.py): the
    /// send_message_batch backend answers it when the summed entry bodies
    /// exceed MAXIMUM_MESSAGE_LENGTH (1048576; moto/sqs/constants.py).
    /// The wire code is service-prefixed — moto's
    /// service-2.moto-extras.json.gz overlay declares the shape's
    /// error.code as AWS.SimpleQueueService.BatchRequestTooLong
    /// (live-verified). The message carries moto's exact wording.
    BatchRequestTooLong(String),
    /// moto's `PurgeQueueInProgress` (moto/sqs/exceptions.py): a queue
    /// purged within the last 60 seconds blocks every further purge —
    /// regardless of which client issued the first one. The 403 status
    /// comes from moto's botocore overlay
    /// `moto/sqs/2012-11-05/service-2.moto-extras.json.gz`
    /// (`merge.shapes.PurgeQueueInProgress` declares error.httpStatusCode
    /// 403; live-verified) — the stock botocore SQS model declares the
    /// shape with no HTTP status at all, unlike what its name suggests.
    PurgeQueueInProgress(String),
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
            SqsError::InvalidBatchEntryId => "AWS.SimpleQueueService.InvalidBatchEntryId",
            SqsError::BatchEntryIdsNotDistinct(_) => {
                "AWS.SimpleQueueService.BatchEntryIdsNotDistinct"
            }
            SqsError::TooManyEntriesInBatchRequest(_) => {
                "AWS.SimpleQueueService.TooManyEntriesInBatchRequest"
            }
            SqsError::BatchRequestTooLong(_) => "AWS.SimpleQueueService.BatchRequestTooLong",
            SqsError::PurgeQueueInProgress(_) => "AWS.SimpleQueueService.PurgeQueueInProgress",
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
            SqsError::InvalidBatchEntryId => {
                "A batch entry id can only contain alphanumeric characters, hyphens and underscores. It can be at most 80 letters long."
            }
            SqsError::BatchEntryIdsNotDistinct(msg) => msg,
            SqsError::TooManyEntriesInBatchRequest(msg) => msg,
            SqsError::BatchRequestTooLong(msg) => msg,
            SqsError::PurgeQueueInProgress(msg) => msg,
            SqsError::MotoCrash => "",
        }
    }

    pub fn status_code(&self) -> u16 {
        match self {
            // AWS query-protocol SQS errors are 400 (moto's RESTError
            // default); PurgeQueueInProgress is the one documented 403.
            SqsError::NonExistentQueue(_) => 400,
            SqsError::InvalidAttributeName(_) => 400,
            SqsError::ReceiptHandleIsInvalid(_) => 400,
            SqsError::MessageNotInflight(_) => 400,
            SqsError::InvalidParameterValue(_) => 400,
            SqsError::MissingParameter(_) => 400,
            SqsError::ResourceNotFound(_) => 400,
            SqsError::EmptyBatchRequest(_) => 400,
            SqsError::InvalidBatchEntryId => 400,
            SqsError::BatchEntryIdsNotDistinct(_) => 400,
            SqsError::TooManyEntriesInBatchRequest(_) => 400,
            SqsError::BatchRequestTooLong(_) => 400,
            SqsError::PurgeQueueInProgress(_) => 403,
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
