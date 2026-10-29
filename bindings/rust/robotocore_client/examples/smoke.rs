//! Smoke test: drive SQS and S3 through this crate against a running
//! robotocore twin.
//!
//! ```text
//! ROBOTOCORE_ENDPOINT=http://127.0.0.1:5000 cargo run --example smoke
//! ```

use std::error::Error;

use aws_sdk_s3::primitives::ByteStream;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let endpoint = std::env::var("ROBOTOCORE_ENDPOINT").unwrap_or_else(|_| robotocore_client::DEFAULT_ENDPOINT.to_string());
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs()
        .to_string();
    println!("smoke: twin at {endpoint}");

    // SQS: create, send, receive, delete a queue.
    let client = robotocore_client::session("sqs", None, None, "us-east-1").await?;
    let sqs = client
        .as_sqs()
        .expect("session(\"sqs\") returned the SQS client");
    let queue_name = format!("robotocore-rs-smoke-{suffix}");
    let queue = sqs.create_queue().queue_name(&queue_name).send().await?;
    let queue_url = queue.queue_url.expect("CreateQueue returned a QueueUrl");
    assert!(queue_url.ends_with(&queue_name), "QueueUrl contains the queue name");

    sqs.send_message()
        .queue_url(&queue_url)
        .message_body("hello")
        .send()
        .await?;
    let received = sqs
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(1)
        .send()
        .await?;
    let messages = received.messages.unwrap_or_default();
    let bodies: Vec<_> = messages.iter().filter_map(|m| m.body.as_deref()).collect();
    assert_eq!(bodies, vec!["hello"], "receiveMessage saw the body");
    sqs.delete_message()
        .queue_url(&queue_url)
        .receipt_handle(
            messages
                .first()
                .and_then(|m| m.receipt_handle.clone())
                .expect("a receipt handle"),
        )
        .send()
        .await?;
    sqs.delete_queue().queue_url(&queue_url).send().await?;
    println!("smoke: sqs {queue_name} create/send/receive/delete OK");

    // S3: bucket + object round-trip through the path-style client.
    let s3client = robotocore_client::session("s3", None, None, "us-east-1").await?;
    let s3 = s3client
        .as_s3()
        .expect("session(\"s3\") returned the S3 client");
    let bucket = format!("robotocore-rs-smoke-{suffix}");
    s3.create_bucket().bucket(&bucket).send().await?; // us-east-1: no location constraint
    s3.put_object()
        .bucket(&bucket)
        .key("hello.txt")
        .body(ByteStream::from_static(b"hello"))
        .send()
        .await?;
    let fetched = s3.get_object().bucket(&bucket).key("hello.txt").send().await?;
    let body = fetched
        .body
        .collect()
        .await
        .expect("collect the GetObject body")
        .into_bytes();
    assert_eq!(body.as_ref(), b"hello", "getObject round-tripped the body");
    s3.delete_object().bucket(&bucket).key("hello.txt").send().await?;
    s3.delete_bucket().bucket(&bucket).send().await?;
    println!("smoke: s3 {bucket} put/get/delete OK");

    println!("smoke: PASS");
    Ok(())
}
