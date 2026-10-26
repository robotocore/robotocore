//! Unit and integration tests for SQS handler

use crate::bridge;
use crate::handler::DefaultSqsHandler;
use crate::models::{Queue, SqsStore};
use crate::protocol::{AwsRequest, SqsHandler};
use bytes::Bytes;
use chrono::Utc;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Collect thread results, capping the wait so a lock-order regression
/// (wedged worker) fails the test instead of hanging the harness.
fn recv_all_until(
    rx: &mpsc::Receiver<(u16, String)>,
    count: usize,
    cap: Duration,
) -> Vec<(u16, String)> {
    let deadline = Instant::now() + cap;
    let mut out = Vec::new();
    while out.len() < count {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match rx.recv_timeout(remaining) {
            Ok(v) => out.push(v),
            Err(_) => break,
        }
    }
    out
}

fn dlq_arn_of(queue_name: &str) -> String {
    format!("arn:aws:sqs:us-east-1:123456789012:{}", queue_name)
}

/// Drain up to `max` messages from `queue_url`, repeat-waiting until at
/// least `count` arrive or the poll budget runs out.
fn drain_until(handler: &DefaultSqsHandler, url: &str, count: usize) -> Vec<Value> {
    let mut seen = Vec::new();
    for _ in 0..20 {
        if seen.len() >= count {
            break;
        }
        let resp = handler.handle(make_request(
            "ReceiveMessage",
            json!({
                "QueueUrl": url,
                "MaxNumberOfMessages": 10,
                "WaitTimeSeconds": 1,
            }),
        ));
        let data: Value = serde_json::from_str(&resp.body).unwrap_or(Value::Null);
        seen.extend(
            data.get("Messages")
                .and_then(|m| m.as_array())
                .cloned()
                .unwrap_or_default(),
        );
    }
    seen
}

/// Mirrored StartMessageMoveTask calls (X->Y racing Y->X) must both finish
/// instead of deadlocking on the nested queue write locks; the harness
/// joins both threads under a 2s cap.
#[test]
fn test_mirrored_message_move_task_never_wedges() {
    let handler = Arc::new(DefaultSqsHandler::new());
    let name_a = "lock-move-a";
    let name_b = "lock-move-b";
    let arn_a = dlq_arn_of(name_a);
    let arn_b = dlq_arn_of(name_b);
    let mut urls = HashMap::new();
    for name in [name_a, name_b] {
        let resp = handler.handle(make_request("CreateQueue", json!({"QueueName": name})));
        assert_eq!(resp.status, 200);
        urls.insert(
            name.to_string(),
            serde_json::from_str::<Value>(&resp.body).unwrap()["QueueUrl"]
                .as_str()
                .unwrap()
                .to_string(),
        );
    }
    // Both queues carry a visible message each: either direction of the
    // mirrored pair has work to move, so both threads contend for both
    // queues' write locks.
    for (name, body) in [(name_a, "from-a"), (name_b, "from-b")] {
        let url = urls[name].clone();
        let resp = handler.handle(make_request(
            "SendMessage",
            json!({"QueueUrl": url, "MessageBody": body}),
        ));
        assert_eq!(resp.status, 200);
    }

    let (tx, rx) = mpsc::channel();
    for (source_arn, dest_arn) in [(&arn_a, &arn_b), (&arn_b, &arn_a)] {
        let handler = Arc::clone(&handler);
        let payload = json!({
            "SourceArn": source_arn,
            "DestinationArn": dest_arn,
        });
        let tx = tx.clone();
        thread::spawn(move || {
            let resp = handler.handle(make_request("StartMessageMoveTask", payload));
            let _ = tx.send((resp.status, resp.body));
        });
    }
    drop(tx);

    let results = recv_all_until(&rx, 2, Duration::from_secs(2));
    assert_eq!(
        results.len(),
        2,
        "mirrored StartMessageMoveTask deadlocked: both threads must finish"
    );
    for (status, _body) in &results {
        assert_eq!(*status, 200);
    }

    // No message was lost by the mirrored moves: both originals still
    // exist across the two queues (possibly re-homed with fresh ids).
    let mut total = 0;
    for url in urls.values() {
        let data = drain_until(&handler, url, 10);
        total += data.len();
    }
    assert_eq!(total, 2, "both messages survive mirrored move tasks");
}

/// Two hundred same-name CreateQueue calls issued concurrently must all
/// succeed and share one queue object (insert-if-absent), whose stored
/// messages stay exactly what was sent afterwards.
#[test]
fn test_create_queue_race_same_name_200_pairs() {
    let handler = Arc::new(DefaultSqsHandler::new());
    let name = "lock-create-race";
    let pairs = 200usize;
    let barrier = Arc::new(std::sync::Barrier::new(pairs));
    let (tx, rx) = mpsc::channel();
    for _ in 0..pairs {
        let handler = Arc::clone(&handler);
        let barrier = Arc::clone(&barrier);
        let tx = tx.clone();
        thread::spawn(move || {
            barrier.wait();
            let resp = handler.handle(make_request("CreateQueue", json!({"QueueName": name})));
            let parsed: Value = serde_json::from_str(&resp.body).unwrap_or(Value::Null);
            let _ = tx.send((
                resp.status,
                parsed["QueueUrl"].as_str().unwrap_or("").to_string(),
            ));
        });
    }
    drop(tx);
    let results = recv_all_until(&rx, pairs, Duration::from_secs(10));
    assert_eq!(results.len(), pairs, "every racing create must respond");
    for (status, _) in &results {
        assert_eq!(*status, 200);
    }
    let mut urls: Vec<&String> = results.iter().map(|(_, u)| u).collect();
    urls.sort();
    urls.dedup();
    assert_eq!(
        urls.len(),
        1,
        "all racing creates must converge on one QueueUrl"
    );

    // The store keeps exactly one queue behind that URL and it behaves as
    // the single live object: one send, one receive, nothing duplicated.
    let store_arc = handler
        .stores
        .read()
        .get(&(123456789012, "us-east-1".to_string()))
        .cloned()
        .unwrap();
    assert_eq!(
        store_arc
            .list_queues()
            .iter()
            .filter(|n| n.as_str() == name)
            .count(),
        1,
        "exactly one map entry survives the race"
    );
    let queue_url = &urls[0];
    handler.handle(make_request(
        "SendMessage",
        json!({"QueueUrl": queue_url, "MessageBody": "post-race"}),
    ));
    let data = drain_until(&handler, queue_url, 1);
    assert_eq!(data.len(), 1);
    assert_eq!(data[0]["Body"], "post-race");
}

/// Redrive decided out of the receive's lock scope double-moves messages
/// with VisibilityTimeout=0: two racing polls both bump receive_count and
/// both pass `receive_count > maxReceiveCount`. With the move inside the
/// receive's critical section, every exhausted message moves exactly once.
#[test]
fn test_redrive_double_move_visibility_zero() {
    let handler = Arc::new(DefaultSqsHandler::new());
    let src_name = "lock-redrive-src";
    let dl_name = "lock-redrive-dlq";

    let dl_resp = handler.handle(make_request("CreateQueue", json!({"QueueName": dl_name})));
    let dl_url: String = serde_json::from_str::<Value>(&dl_resp.body).unwrap()["QueueUrl"]
        .as_str()
        .unwrap()
        .into();
    let dl_arn = dlq_arn_of(dl_name);
    let src_resp = handler.handle(make_request("CreateQueue", json!({"QueueName": src_name})));
    let src_url: String = serde_json::from_str::<Value>(&src_resp.body).unwrap()["QueueUrl"]
        .as_str()
        .unwrap()
        .into();
    handler.handle(make_request(
        "SetQueueAttributes",
        json!({
            "QueueUrl": src_url,
            "Attributes": {
                "RedrivePolicy": json!({
                    "deadLetterTargetArn": dl_arn,
                    "maxReceiveCount": 1,
                })
                .to_string(),
            },
        }),
    ));

    let count = 20;
    for i in 0..count {
        let resp = handler.handle(make_request(
            "SendMessage",
            json!({"QueueUrl": src_url, "MessageBody": format!("m{}", i)}),
        ));
        assert_eq!(resp.status, 200);
    }

    // Two racing receive loops with VisibilityTimeout=0.
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let (tx, rx) = mpsc::channel();
    for _ in 0..2 {
        let handler = Arc::clone(&handler);
        let barrier = Arc::clone(&barrier);
        let src_url = src_url.clone();
        let tx = tx.clone();
        thread::spawn(move || {
            barrier.wait();
            let mut empties = 0;
            for _ in 0..60 {
                let resp = handler.handle(make_request(
                    "ReceiveMessage",
                    json!({
                        "QueueUrl": src_url,
                        "MaxNumberOfMessages": 10,
                        "VisibilityTimeout": 0,
                    }),
                ));
                let data: Value = serde_json::from_str(&resp.body).unwrap_or(Value::Null);
                let got = data
                    .get("Messages")
                    .and_then(|m| m.as_array())
                    .map(|a| !a.is_empty())
                    .unwrap_or(false);
                empties = if got { 0 } else { empties + 1 };
                if empties >= 2 {
                    break;
                }
            }
            let _ = tx.send(true);
        });
    }
    drop(tx);
    for _ in 0..2 {
        let remaining = Duration::from_secs(10);
        let _ = rx.recv_timeout(remaining);
    }

    // The DLQ holds each exhausted message exactly once.
    let dlq_msgs = drain_until(&handler, &dl_url, count);
    let mut ids: Vec<String> = dlq_msgs
        .iter()
        .map(|m| m["MessageId"].as_str().unwrap_or_default().to_string())
        .collect();
    assert_eq!(ids.len(), count, "every exhausted message moved to the DLQ");
    ids.sort();
    ids.dedup();
    assert_eq!(
        ids.len(),
        count,
        "every exhausted message must appear exactly once in the DLQ"
    );
    // The source is empty afterwards.
    let src_after = drain_until(&handler, &src_url, 1);
    assert!(src_after.is_empty(), "source must be fully redriven");
}

/// Delete-then-producer-loss: deliveries buffered by bridge producers for
/// a queue are dropped with the queue itself; a re-created queue of the
/// same name must not resurrect them, while the bridge keeps working after.
#[test]
fn test_delete_queue_clears_pending_before_recreate() {
    let handler = DefaultSqsHandler::new();
    let name = "lock-delete-pending";
    let create = handler.handle(make_request("CreateQueue", json!({"QueueName": name})));
    let url: String = serde_json::from_str::<Value>(&create.body).unwrap()["QueueUrl"]
        .as_str()
        .unwrap()
        .into();

    // Producer buffering (SNS/EventBridge fanout path) before the delete.
    for i in 0..3 {
        assert!(bridge::deliver_to_queue(
            123456789012,
            "us-east-1",
            name,
            &format!("lost-{}", i)
        ));
    }

    let del = handler.handle(make_request("DeleteQueue", json!({"QueueUrl": url})));
    assert_eq!(del.status, 200);

    // Same-name re-create yields a fresh empty queue: no stale buffered
    // delivery from the deleted queue's lifetime may survive.
    let create2 = handler.handle(make_request("CreateQueue", json!({"QueueName": name})));
    let url2: String = serde_json::from_str::<Value>(&create2.body).unwrap()["QueueUrl"]
        .as_str()
        .unwrap()
        .into();
    let data = drain_until(&handler, &url2, 3);
    assert!(
        data.is_empty(),
        "deleted queue's pending deliveries must be gone, got {:?}",
        data
    );

    // The bridge still buffers for the live queue after the delete.
    assert!(bridge::deliver_to_queue(
        123456789012,
        "us-east-1",
        name,
        "fresh-1"
    ));
    let data = drain_until(&handler, &url2, 1);
    assert_eq!(data.len(), 1);
    assert_eq!(data[0]["Body"], "fresh-1");
}

/// PurgeQueue drops buffered cross-service deliveries along with the
/// stored messages, in one critical section with the queue purge.
#[test]
fn test_purge_queue_clears_pending() {
    let handler = DefaultSqsHandler::new();
    let name = "lock-purge-pending";
    let create = handler.handle(make_request("CreateQueue", json!({"QueueName": name})));
    let url: String = serde_json::from_str::<Value>(&create.body).unwrap()["QueueUrl"]
        .as_str()
        .unwrap()
        .into();

    for i in 0..3 {
        assert!(bridge::deliver_to_queue(
            123456789012,
            "us-east-1",
            name,
            &format!("p{}", i)
        ));
    }
    let purge = handler.handle(make_request("PurgeQueue", json!({"QueueUrl": url})));
    assert_eq!(purge.status, 200);

    let data = drain_until(&handler, &url, 3);
    assert!(data.is_empty(), "purge must drop pending deliveries too");
}

/// Store-level: re-creating a same-name queue resolves to the live handle
/// instead of overwriting the registered entry.
#[test]
fn test_store_create_queue_is_insert_if_absent() {
    let store = SqsStore::new();
    let first = store.create_queue(Queue::new(
        "lock-create-identity".to_string(),
        "us-east-1".to_string(),
        "123456789012".to_string(),
        "http://queue-url-1".to_string(),
        "arn:aws:sqs:us-east-1:123456789012:lock-create-identity".to_string(),
    ));
    let second = store.create_queue(Queue::new(
        "lock-create-identity".to_string(),
        "us-east-1".to_string(),
        "123456789012".to_string(),
        "http://queue-url-2".to_string(),
        "arn:aws:sqs:us-east-1:123456789012:lock-create-identity".to_string(),
    ));

    assert!(
        Arc::ptr_eq(&first, &second),
        "racing same-name create must resolve to the live queue"
    );
    assert_eq!(
        first.read().url,
        "http://queue-url-1",
        "existing entry must survive the racing create"
    );
}

/// The dead-letter move task keeps its previous wire behavior: a task
/// against a source with a redrive-derived target moves the visible
/// messages and, with no target, reports FAILED.
#[test]
fn test_start_message_move_task_without_target_reports_failed() {
    let handler = DefaultSqsHandler::new();
    let name = "lock-move-failed";
    let arn = dlq_arn_of(name);
    let create = handler.handle(make_request("CreateQueue", json!({"QueueName": name})));
    assert_eq!(create.status, 200);

    let resp = handler.handle(make_request(
        "StartMessageMoveTask",
        json!({"SourceArn": arn}),
    ));
    assert_eq!(resp.status, 200);
    let handle: String = serde_json::from_str::<Value>(&resp.body).unwrap()["TaskHandle"]
        .as_str()
        .unwrap()
        .into();
    let task = serde_json::from_str::<Value>(
        &handler
            .handle(make_request(
                "ListMessageMoveTasks",
                json!({"SourceArn": arn}),
            ))
            .body,
    )
    .unwrap();
    let entry = &task["Results"][0];
    assert_eq!(entry["Status"], "FAILED");
    assert_eq!(entry["TaskHandle"], handle);
}

fn make_request(operation: &str, params: Value) -> AwsRequest {
    AwsRequest {
        service: "sqs".to_string(),
        operation: operation.to_string(),
        account: 123456789012,
        region: "us-east-1".to_string(),
        params,
        body: Bytes::new(),
    }
}

#[test]
fn test_create_queue() {
    let handler = DefaultSqsHandler::new();

    let req = make_request(
        "CreateQueue",
        json!({
            "QueueName": "test-queue"
        }),
    );

    let resp = handler.handle(req);

    assert_eq!(resp.status, 200);
    assert!(resp.body.contains("QueueUrl"));

    let parsed: Value = serde_json::from_str(&resp.body).expect("Invalid JSON");
    let url = parsed["QueueUrl"].as_str().expect("Missing QueueUrl");
    assert!(url.contains("test-queue"));
}

#[test]
fn test_create_queue_invalid_name_too_long() {
    let handler = DefaultSqsHandler::new();

    let long_name = "a".repeat(81);
    let req = make_request(
        "CreateQueue",
        json!({
            "QueueName": long_name
        }),
    );

    let resp = handler.handle(req);

    assert_eq!(resp.status, 400);
    // moto's InvalidParameterValue answer at 400 (code + fixed wording).
    assert!(resp.body.contains("InvalidParameterValue"));
    assert!(resp
        .body
        .contains("Can only include alphanumeric characters, hyphens, or underscores"));
}

#[test]
fn test_create_queue_invalid_name_special_chars() {
    let handler = DefaultSqsHandler::new();

    let req = make_request(
        "CreateQueue",
        json!({
            "QueueName": "queue@invalid"
        }),
    );

    let resp = handler.handle(req);

    assert_eq!(resp.status, 400);
}

#[test]
fn test_create_queue_name_matches_moto_setter_regex() {
    // moto's Queue.name setter: `^[a-zA-Z0-9_-]{1,80}$` or
    // `^[a-zA-Z0-9_-]{1,75}\.fifo$`. A `.` is legal only as the `.fifo`
    // suffix; non-ASCII alphanumerics are invalid everywhere.
    let handler = DefaultSqsHandler::new();
    let expect_ok = |name: &str| -> bool {
        handler
            .handle(make_request("CreateQueue", json!({ "QueueName": name })))
            .status
            == 200
    };

    assert!(expect_ok("plain-queue"));
    assert!(expect_ok("a"));
    assert!(expect_ok("a".repeat(80).as_str()));
    assert!(expect_ok("fifo-suffix.fifo"));
    let fifo_max = "a".repeat(75) + ".fifo";
    let fifo_over = "a".repeat(76) + ".fifo";
    assert!(expect_ok(&fifo_max));
    assert!(!expect_ok(&fifo_over));

    assert!(!expect_ok("my.queue"));
    assert!(!expect_ok("deadletter.queue"));
    assert!(!expect_ok("my-queue.fifo.fifo"));
    assert!(!expect_ok("café"));
    assert!(!expect_ok(".fifo"));
    assert!(!expect_ok(&fifo_over));
    assert!(!expect_ok("a".repeat(81).as_str()));
}

#[test]
fn test_send_message() {
    let handler = DefaultSqsHandler::new();

    // Create queue first
    let create_req = make_request(
        "CreateQueue",
        json!({
            "QueueName": "msg-queue"
        }),
    );
    let create_resp = handler.handle(create_req);
    assert_eq!(create_resp.status, 200);

    let queue_data: Value = serde_json::from_str(&create_resp.body).expect("Invalid JSON");
    let queue_url = queue_data["QueueUrl"].as_str().expect("Missing QueueUrl");

    // Send message
    let send_req = make_request(
        "SendMessage",
        json!({
            "QueueUrl": queue_url,
            "MessageBody": "hello world"
        }),
    );

    let send_resp = handler.handle(send_req);
    assert_eq!(send_resp.status, 200);

    let msg_data: Value = serde_json::from_str(&send_resp.body).expect("Invalid JSON");
    assert!(msg_data.get("MessageId").is_some());
    assert!(msg_data.get("MD5OfMessageBody").is_some());

    let md5 = msg_data["MD5OfMessageBody"].as_str().expect("Missing MD5");
    // MD5 of "hello world"
    assert_eq!(md5, "5eb63bbbe01eeed093cb22bb8f5acdc3");
}

#[test]
fn test_send_message_missing_queue() {
    let handler = DefaultSqsHandler::new();

    let send_req = make_request(
        "SendMessage",
        json!({
            "QueueUrl": "http://sqs.us-east-1.localhost.robotocore.cloud:4566/123456789012/nonexistent",
            "MessageBody": "test"
        }),
    );

    let resp = handler.handle(send_req);
    assert_eq!(resp.status, 400);
    assert!(resp.body.contains("NonExistentQueue"));
}

#[test]
fn test_receive_message() {
    let handler = DefaultSqsHandler::new();

    // Create queue
    let create_req = make_request(
        "CreateQueue",
        json!({
            "QueueName": "recv-queue"
        }),
    );
    let create_resp = handler.handle(create_req);
    assert_eq!(create_resp.status, 200);

    let queue_data: Value = serde_json::from_str(&create_resp.body).expect("Invalid JSON");
    let queue_url = queue_data["QueueUrl"]
        .as_str()
        .expect("Missing QueueUrl")
        .to_string();

    // Send message
    let send_req = make_request(
        "SendMessage",
        json!({
            "QueueUrl": &queue_url,
            "MessageBody": "test message"
        }),
    );
    let send_resp = handler.handle(send_req);
    assert_eq!(send_resp.status, 200);

    // Receive message
    let recv_req = make_request(
        "ReceiveMessage",
        json!({
            "QueueUrl": &queue_url,
            "MaxNumberOfMessages": 5,
            "VisibilityTimeout": 30
        }),
    );

    let recv_resp = handler.handle(recv_req);
    assert_eq!(recv_resp.status, 200);

    let recv_data: Value = serde_json::from_str(&recv_resp.body).expect("Invalid JSON");
    let messages = recv_data["Messages"]
        .as_array()
        .expect("Messages should be array");
    assert_eq!(messages.len(), 1);

    let msg = &messages[0];
    assert_eq!(msg["Body"].as_str().unwrap(), "test message");
    assert!(msg.get("MessageId").is_some());
    assert!(msg.get("ReceiptHandle").is_some());
    assert!(msg.get("MD5OfBody").is_some());
}

#[test]
fn test_receive_message_empty_queue() {
    let handler = DefaultSqsHandler::new();

    // Create queue
    let create_req = make_request(
        "CreateQueue",
        json!({
            "QueueName": "empty-queue"
        }),
    );
    let create_resp = handler.handle(create_req);
    assert_eq!(create_resp.status, 200);

    let queue_data: Value = serde_json::from_str(&create_resp.body).expect("Invalid JSON");
    let queue_url = queue_data["QueueUrl"].as_str().expect("Missing QueueUrl");

    // Receive from empty queue
    let recv_req = make_request(
        "ReceiveMessage",
        json!({
            "QueueUrl": queue_url,
            "MaxNumberOfMessages": 5
        }),
    );

    let recv_resp = handler.handle(recv_req);
    assert_eq!(recv_resp.status, 200);

    let recv_data: Value = serde_json::from_str(&recv_resp.body).expect("Invalid JSON");
    let messages = recv_data["Messages"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    assert_eq!(messages, 0);
}

#[test]
fn test_delete_message() {
    let handler = DefaultSqsHandler::new();

    // Create queue
    let create_req = make_request(
        "CreateQueue",
        json!({
            "QueueName": "del-queue"
        }),
    );
    let create_resp = handler.handle(create_req);
    let queue_data: Value = serde_json::from_str(&create_resp.body).expect("Invalid JSON");
    let queue_url = queue_data["QueueUrl"]
        .as_str()
        .expect("Missing QueueUrl")
        .to_string();

    // Send message
    let send_req = make_request(
        "SendMessage",
        json!({
            "QueueUrl": &queue_url,
            "MessageBody": "to be deleted"
        }),
    );
    handler.handle(send_req);

    // Receive message
    let recv_req = make_request(
        "ReceiveMessage",
        json!({
            "QueueUrl": &queue_url,
            "MaxNumberOfMessages": 1
        }),
    );
    let recv_resp = handler.handle(recv_req);
    let recv_data: Value = serde_json::from_str(&recv_resp.body).expect("Invalid JSON");
    let receipt_handle = recv_data["Messages"][0]["ReceiptHandle"]
        .as_str()
        .expect("Missing ReceiptHandle")
        .to_string();

    // Delete message
    let del_req = make_request(
        "DeleteMessage",
        json!({
            "QueueUrl": &queue_url,
            "ReceiptHandle": receipt_handle
        }),
    );
    let del_resp = handler.handle(del_req);
    assert_eq!(del_resp.status, 200);

    // Try to receive again - should be empty (no Messages key or empty list)
    let recv_req2 = make_request(
        "ReceiveMessage",
        json!({
            "QueueUrl": &queue_url,
            "MaxNumberOfMessages": 1
        }),
    );
    let recv_resp2 = handler.handle(recv_req2);
    let recv_data2: Value = serde_json::from_str(&recv_resp2.body).expect("Invalid JSON");
    let messages = recv_data2["Messages"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    assert_eq!(messages, 0);
}

#[test]
fn test_delete_message_invalid_handle() {
    let handler = DefaultSqsHandler::new();

    // Create queue
    let create_req = make_request(
        "CreateQueue",
        json!({
            "QueueName": "invalid-handle-queue"
        }),
    );
    let create_resp = handler.handle(create_req);
    let queue_data: Value = serde_json::from_str(&create_resp.body).expect("Invalid JSON");
    let queue_url = queue_data["QueueUrl"].as_str().expect("Missing QueueUrl");

    // Try to delete with invalid handle
    let del_req = make_request(
        "DeleteMessage",
        json!({
            "QueueUrl": queue_url,
            "ReceiptHandle": "invalid-handle"
        }),
    );
    let del_resp = handler.handle(del_req);
    assert_eq!(del_resp.status, 400);
    assert!(del_resp.body.contains("ReceiptHandleIsInvalid"));
}

#[test]
fn test_get_queue_url() {
    let handler = DefaultSqsHandler::new();

    // Create queue
    let create_req = make_request(
        "CreateQueue",
        json!({
            "QueueName": "named-queue"
        }),
    );
    let create_resp = handler.handle(create_req);
    let queue_data: Value = serde_json::from_str(&create_resp.body).expect("Invalid JSON");
    let expected_url = queue_data["QueueUrl"].as_str().expect("Missing QueueUrl");

    // Get queue URL
    let get_req = make_request(
        "GetQueueUrl",
        json!({
            "QueueName": "named-queue"
        }),
    );
    let get_resp = handler.handle(get_req);
    assert_eq!(get_resp.status, 200);

    let get_data: Value = serde_json::from_str(&get_resp.body).expect("Invalid JSON");
    let url = get_data["QueueUrl"].as_str().expect("Missing QueueUrl");
    assert_eq!(url, expected_url);
}

#[test]
fn test_list_queues() {
    let handler = DefaultSqsHandler::new();

    // Queues live in a process-global store shared with other services
    // (S3 notifications), so assert membership rather than exact counts.
    let create_req1 = make_request("CreateQueue", json!({"QueueName": "queue-a"}));
    let create_resp1 = handler.handle(create_req1);
    let created: Value = serde_json::from_str(&create_resp1.body).expect("Invalid JSON");
    let _expected_url = created["QueueUrl"].as_str().expect("Missing QueueUrl");
    handler.handle(make_request("CreateQueue", json!({"QueueName": "queue-b"})));

    let list_req = make_request("ListQueues", json!({}));
    let list_resp = handler.handle(list_req);
    assert_eq!(list_resp.status, 200);

    let list_data: Value = serde_json::from_str(&list_resp.body).expect("Invalid JSON");
    let urls = list_data["QueueUrls"]
        .as_array()
        .expect("QueueUrls should be array");
    // Stores are process-wide, so other tests running in this binary may
    // have created queues too — only assert on our own entries.
    let our_urls: Vec<&Value> = urls
        .iter()
        .filter(|u| {
            let s = u.as_str().unwrap_or("");
            s.ends_with("/queue-a") || s.ends_with("/queue-b")
        })
        .collect();
    assert_eq!(our_urls.len(), 2);
}

#[test]
fn test_delete_queue() {
    let handler = DefaultSqsHandler::new();

    // Create queue
    let create_req = make_request("CreateQueue", json!({"QueueName": "to-delete"}));
    let create_resp = handler.handle(create_req);
    let queue_data: Value = serde_json::from_str(&create_resp.body).expect("Invalid JSON");
    let queue_url = queue_data["QueueUrl"].as_str().expect("Missing QueueUrl");

    // Delete queue
    let del_req = make_request("DeleteQueue", json!({"QueueUrl": queue_url}));
    let del_resp = handler.handle(del_req);
    assert_eq!(del_resp.status, 200);

    // Verify it's gone
    let get_req = make_request("GetQueueUrl", json!({"QueueName": "to-delete"}));
    let get_resp = handler.handle(get_req);
    assert_eq!(get_resp.status, 400);
}

#[test]
fn test_get_queue_attributes() {
    let handler = DefaultSqsHandler::new();

    // Create queue
    let create_req = make_request("CreateQueue", json!({"QueueName": "attr-queue"}));
    let create_resp = handler.handle(create_req);
    let queue_data: Value = serde_json::from_str(&create_resp.body).expect("Invalid JSON");
    let queue_url = queue_data["QueueUrl"].as_str().expect("Missing QueueUrl");

    // Get attributes
    let get_req = make_request("GetQueueAttributes", json!({"QueueUrl": queue_url}));
    let get_resp = handler.handle(get_req);
    assert_eq!(get_resp.status, 200);

    let get_data: Value = serde_json::from_str(&get_resp.body).expect("Invalid JSON");
    let attrs = get_data["Attributes"]
        .as_object()
        .expect("Attributes should be object");

    assert!(attrs.contains_key("VisibilityTimeout"));
    assert!(attrs.contains_key("MessageRetentionPeriod"));
    assert!(attrs.contains_key("MaximumMessageSize"));
    assert!(attrs.contains_key("QueueArn"));
}

#[test]
fn test_create_queue_with_attributes() {
    let handler = DefaultSqsHandler::new();

    let req = make_request(
        "CreateQueue",
        json!({
            "QueueName": "attr-create-queue",
            "Attributes": {
                "VisibilityTimeout": "60",
                "DelaySeconds": "5",
                "ReceiveMessageWaitTimeSeconds": "10",
                "MaximumMessageSize": "1024",
                "MessageRetentionPeriod": "86400"
            }
        }),
    );

    let resp = handler.handle(req);
    assert_eq!(resp.status, 200);

    let parsed: Value = serde_json::from_str(&resp.body).expect("Invalid JSON");
    let url = parsed["QueueUrl"]
        .as_str()
        .expect("Missing QueueUrl")
        .to_string();

    // Verify attributes were set
    let get_req = make_request("GetQueueAttributes", json!({"QueueUrl": url}));
    let get_resp = handler.handle(get_req);
    let get_data: Value = serde_json::from_str(&get_resp.body).expect("Invalid JSON");
    let attrs = get_data["Attributes"]
        .as_object()
        .expect("Missing Attributes");
    assert_eq!(attrs.get("VisibilityTimeout").unwrap(), "60");
    assert_eq!(attrs.get("DelaySeconds").unwrap(), "5");
    assert_eq!(attrs.get("ReceiveMessageWaitTimeSeconds").unwrap(), "10");
    assert_eq!(attrs.get("MaximumMessageSize").unwrap(), "1024");
    assert_eq!(attrs.get("MessageRetentionPeriod").unwrap(), "86400");
}

#[test]
fn test_set_queue_attributes() {
    let handler = DefaultSqsHandler::new();

    // Create queue
    let create_req = make_request("CreateQueue", json!({"QueueName": "set-attr-queue"}));
    let create_resp = handler.handle(create_req);
    let queue_data: Value = serde_json::from_str(&create_resp.body).expect("Invalid JSON");
    let queue_url = queue_data["QueueUrl"]
        .as_str()
        .expect("Missing QueueUrl")
        .to_string();

    // Set attributes
    let set_req = make_request(
        "SetQueueAttributes",
        json!({
            "QueueUrl": &queue_url,
            "Attributes": {
                "VisibilityTimeout": "90",
                "DelaySeconds": "15"
            }
        }),
    );
    let set_resp = handler.handle(set_req);
    assert_eq!(set_resp.status, 200);

    // Verify
    let get_req = make_request("GetQueueAttributes", json!({"QueueUrl": &queue_url}));
    let get_resp = handler.handle(get_req);
    let get_data: Value = serde_json::from_str(&get_resp.body).expect("Invalid JSON");
    let attrs = get_data["Attributes"]
        .as_object()
        .expect("Missing Attributes");
    assert_eq!(attrs.get("VisibilityTimeout").unwrap(), "90");
    assert_eq!(attrs.get("DelaySeconds").unwrap(), "15");
}

#[test]
fn test_set_queue_attributes_nonexistent_queue() {
    let handler = DefaultSqsHandler::new();

    let set_req = make_request(
        "SetQueueAttributes",
        json!({
            "QueueUrl": "http://sqs.us-east-1.localhost.robotocore.cloud:4566/123456789012/no-such-queue",
            "Attributes": {"VisibilityTimeout": "90"}
        }),
    );
    let set_resp = handler.handle(set_req);
    assert_eq!(set_resp.status, 400);
    assert!(set_resp.body.contains("NonExistentQueue"));
}

#[test]
fn test_change_message_visibility() {
    let handler = DefaultSqsHandler::new();

    // Create queue
    let create_req = make_request("CreateQueue", json!({"QueueName": "cmv-queue"}));
    let create_resp = handler.handle(create_req);
    let queue_data: Value = serde_json::from_str(&create_resp.body).expect("Invalid JSON");
    let queue_url = queue_data["QueueUrl"]
        .as_str()
        .expect("Missing QueueUrl")
        .to_string();

    // Send message
    let send_req = make_request(
        "SendMessage",
        json!({"QueueUrl": &queue_url, "MessageBody": "cmv test"}),
    );
    handler.handle(send_req);

    // Receive message
    let recv_req = make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 1, "VisibilityTimeout": 30}),
    );
    let recv_resp = handler.handle(recv_req);
    let recv_data: Value = serde_json::from_str(&recv_resp.body).expect("Invalid JSON");
    let receipt_handle = recv_data["Messages"][0]["ReceiptHandle"]
        .as_str()
        .expect("Missing ReceiptHandle")
        .to_string();

    // Change visibility
    let cmv_req = make_request(
        "ChangeMessageVisibility",
        json!({
            "QueueUrl": &queue_url,
            "ReceiptHandle": receipt_handle,
            "VisibilityTimeout": 60
        }),
    );
    let cmv_resp = handler.handle(cmv_req);
    assert_eq!(cmv_resp.status, 200);
}

#[test]
fn test_change_message_visibility_invalid_handle() {
    let handler = DefaultSqsHandler::new();

    // Create queue
    let create_req = make_request("CreateQueue", json!({"QueueName": "cmv-invalid-queue"}));
    handler.handle(create_req);
    let create_resp = handler.handle(make_request(
        "CreateQueue",
        json!({"QueueName": "cmv-invalid-queue"}),
    ));
    let queue_data: Value = serde_json::from_str(&create_resp.body).expect("Invalid JSON");
    let queue_url = queue_data["QueueUrl"]
        .as_str()
        .expect("Missing QueueUrl")
        .to_string();

    // Change visibility with invalid handle
    let cmv_req = make_request(
        "ChangeMessageVisibility",
        json!({
            "QueueUrl": &queue_url,
            "ReceiptHandle": "bogus-handle",
            "VisibilityTimeout": 60
        }),
    );
    let cmv_resp = handler.handle(cmv_req);
    assert_eq!(cmv_resp.status, 400);
    assert!(cmv_resp.body.contains("ReceiptHandleIsInvalid"));
}

#[test]
fn test_purge_queue() {
    let handler = DefaultSqsHandler::new();

    // Create queue
    let create_req = make_request("CreateQueue", json!({"QueueName": "purge-queue"}));
    let create_resp = handler.handle(create_req);
    let queue_data: Value = serde_json::from_str(&create_resp.body).expect("Invalid JSON");
    let queue_url = queue_data["QueueUrl"]
        .as_str()
        .expect("Missing QueueUrl")
        .to_string();

    // Send messages
    handler.handle(make_request(
        "SendMessage",
        json!({"QueueUrl": &queue_url, "MessageBody": "msg1"}),
    ));
    handler.handle(make_request(
        "SendMessage",
        json!({"QueueUrl": &queue_url, "MessageBody": "msg2"}),
    ));

    // Purge
    let purge_req = make_request("PurgeQueue", json!({"QueueUrl": &queue_url}));
    let purge_resp = handler.handle(purge_req);
    assert_eq!(purge_resp.status, 200);

    // Verify empty
    let recv_req = make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 10}),
    );
    let recv_resp = handler.handle(recv_req);
    let recv_data: Value = serde_json::from_str(&recv_resp.body).expect("Invalid JSON");
    assert!(
        recv_data.get("Messages").is_none()
            || recv_data["Messages"]
                .as_array()
                .map(|a| a.is_empty())
                .unwrap_or(true)
    );
}

#[test]
fn test_purge_queue_nonexistent() {
    let handler = DefaultSqsHandler::new();

    let purge_req = make_request(
        "PurgeQueue",
        json!({"QueueUrl": "http://sqs.us-east-1.localhost.robotocore.cloud:4566/123456789012/ghost"}),
    );
    let purge_resp = handler.handle(purge_req);
    assert_eq!(purge_resp.status, 400);
    assert!(purge_resp.body.contains("NonExistentQueue"));
}

#[test]
fn test_list_queues_prefix_filter() {
    let handler = DefaultSqsHandler::new();

    handler.handle(make_request("CreateQueue", json!({"QueueName": "alpha-1"})));
    handler.handle(make_request("CreateQueue", json!({"QueueName": "alpha-2"})));
    handler.handle(make_request("CreateQueue", json!({"QueueName": "beta-1"})));

    // Filter by prefix
    let list_req = make_request("ListQueues", json!({"QueueNamePrefix": "alpha"}));
    let list_resp = handler.handle(list_req);
    let list_data: Value = serde_json::from_str(&list_resp.body).expect("Invalid JSON");
    let urls = list_data["QueueUrls"]
        .as_array()
        .expect("Missing QueueUrls");
    assert_eq!(urls.len(), 2);
    for url in urls {
        assert!(url.as_str().unwrap().contains("alpha"));
    }
}

#[test]
fn test_receive_message_max_count_validation() {
    let handler = DefaultSqsHandler::new();

    // Create queue
    handler.handle(make_request(
        "CreateQueue",
        json!({"QueueName": "maxcount-queue"}),
    ));
    let create_resp = handler.handle(make_request(
        "CreateQueue",
        json!({"QueueName": "maxcount-queue"}),
    ));
    let queue_data: Value = serde_json::from_str(&create_resp.body).expect("Invalid JSON");
    let queue_url = queue_data["QueueUrl"]
        .as_str()
        .expect("Missing QueueUrl")
        .to_string();

    // moto 400s an out-of-range MaxNumberOfMessages in its response layer
    // (live-verified: "Value 11 for parameter MaxNumberOfMessages is
    // invalid. Reason: must be between 1 and 10, if provided." — the
    // old twin clamp is the divergence this closes).
    let recv_req = make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 11}),
    );
    let recv_resp = handler.handle(recv_req);
    assert_eq!(recv_resp.status, 400);
    assert!(
        recv_resp.body.contains(
            "Value 11 for parameter MaxNumberOfMessages is invalid. Reason: must be \
             between 1 and 10, if provided."
        ),
        "got {}",
        recv_resp.body
    );
}

#[test]
fn test_bridged_message_gets_real_receipt_handle() {
    let handler = DefaultSqsHandler::new();

    let create_resp = handler.handle(make_request(
        "CreateQueue",
        json!({"QueueName": "bridge-queue"}),
    ));
    assert_eq!(create_resp.status, 200);
    let queue_data: Value = serde_json::from_str(&create_resp.body).expect("Invalid JSON");
    let queue_url = queue_data["QueueUrl"]
        .as_str()
        .expect("Missing QueueUrl")
        .to_string();

    // Cross-service delivery (EventBridge/S3 fanout path) before any receive.
    assert!(crate::bridge::deliver_to_queue(
        123456789012,
        "us-east-1",
        "bridge-queue",
        "bridged body"
    ));

    let recv_resp = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 1}),
    ));
    assert_eq!(recv_resp.status, 200);
    let recv_data: Value = serde_json::from_str(&recv_resp.body).expect("Invalid JSON");
    let msg = &recv_data["Messages"][0];
    assert_eq!(msg["Body"].as_str().unwrap(), "bridged body");
    let receipt_handle = msg["ReceiptHandle"].as_str().expect("Missing handle");
    assert!(!receipt_handle.is_empty(), "receipt handle must be minted");

    // The handle must be usable to delete the bridged message.
    let del_resp = handler.handle(make_request(
        "DeleteMessage",
        json!({"QueueUrl": &queue_url, "ReceiptHandle": receipt_handle}),
    ));
    assert_eq!(del_resp.status, 200);

    let again = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 1}),
    ));
    let again_data: Value = serde_json::from_str(&again.body).expect("Invalid JSON");
    let remaining = again_data
        .get("Messages")
        .map(|a| a.as_array().map(|a| a.len()).unwrap_or(0))
        .unwrap_or(0);
    assert_eq!(remaining, 0);
}

fn create_queue(handler: &DefaultSqsHandler, name: &str) -> String {
    let resp = handler.handle(make_request("CreateQueue", json!({"QueueName": name})));
    assert_eq!(resp.status, 200);
    let data: Value = serde_json::from_str(&resp.body).expect("Invalid JSON");
    data["QueueUrl"]
        .as_str()
        .expect("Missing QueueUrl")
        .to_string()
}

fn create_fifo_queue(handler: &DefaultSqsHandler, name: &str, content_dedup: bool) -> String {
    let attrs = if content_dedup {
        json!({"FifoQueue": "true", "ContentBasedDeduplication": "true"})
    } else {
        json!({"FifoQueue": "true"})
    };
    let resp = handler.handle(make_request(
        "CreateQueue",
        json!({"QueueName": name, "Attributes": attrs}),
    ));
    assert_eq!(resp.status, 200);
    let data: Value = serde_json::from_str(&resp.body).expect("Invalid JSON");
    data["QueueUrl"]
        .as_str()
        .expect("Missing QueueUrl")
        .to_string()
}

#[test]
fn test_fifo_second_message_blocked_while_first_in_flight() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_fifo_queue(&handler, "fifo-block.fifo", false);

    for i in 0..3 {
        let resp = handler.handle(make_request(
            "SendMessage",
            json!({"QueueUrl": &queue_url, "MessageBody": format!("m{i}"), "MessageGroupId": "g1"}),
        ));
        assert_eq!(resp.status, 200);
    }

    // First receive: m0. Short visibility window so the test stays fast.
    let recv = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 1, "VisibilityTimeout": 1}),
    ));
    let data: Value = serde_json::from_str(&recv.body).expect("Invalid JSON");
    assert_eq!(data["Messages"][0]["Body"], "m0");

    // While m0 is in flight, g1's later messages must not be delivered.
    let recv2 = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 1, "WaitTimeSeconds": 0}),
    ));
    let data2: Value = serde_json::from_str(&recv2.body).expect("Invalid JSON");
    assert!(
        data2.get("Messages").is_none()
            || data2["Messages"]
                .as_array()
                .map(|a| a.is_empty())
                .unwrap_or(true),
        "g1 second message must be blocked while m0 is in flight"
    );

    // Long poll: the worker waiting for g1 must be released once m0's
    // visibility expires. AWS redelivers m0 (it is still undeleted), never
    // skipping to m1.
    let recv3 = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 1, "WaitTimeSeconds": 5, "VisibilityTimeout": 1}),
    ));
    let data3: Value = serde_json::from_str(&recv3.body).expect("Invalid JSON");
    assert_eq!(data3["Messages"][0]["Body"], "m0");

    // Delete the redelivered m0; only then may m1 be delivered.
    handler.handle(make_request(
        "DeleteMessage",
        json!({
            "QueueUrl": &queue_url,
            "ReceiptHandle": data3["Messages"][0]["ReceiptHandle"].as_str().unwrap(),
        }),
    ));
    let recv4 = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 1, "WaitTimeSeconds": 5, "VisibilityTimeout": 1}),
    ));
    let data4: Value = serde_json::from_str(&recv4.body).expect("Invalid JSON");
    assert_eq!(data4["Messages"][0]["Body"], "m1");
}

#[test]
fn test_fifo_other_groups_may_deliver_while_group_blocked() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_fifo_queue(&handler, "fifo-groups.fifo", false);

    let resp = handler.handle(make_request(
        "SendMessage",
        json!({"QueueUrl": &queue_url, "MessageBody": "a", "MessageGroupId": "gA"}),
    ));
    assert_eq!(resp.status, 200);
    let recv = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 1}),
    ));
    let data: Value = serde_json::from_str(&recv.body).expect("Invalid JSON");
    assert_eq!(data["Messages"][0]["Body"], "a");

    let resp2 = handler.handle(make_request(
        "SendMessage",
        json!({"QueueUrl": &queue_url, "MessageBody": "b", "MessageGroupId": "gB"}),
    ));
    assert_eq!(resp2.status, 200);

    // gA is blocked (its message is in flight); gB's message still delivers.
    let recv2 = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 1}),
    ));
    let data2: Value = serde_json::from_str(&recv2.body).expect("Invalid JSON");
    assert_eq!(data2["Messages"][0]["Body"], "b");
}

#[test]
fn test_fifo_dedup_suppressed_after_original_deleted() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_fifo_queue(&handler, "fifo-retract.fifo", true);

    let send = handler.handle(make_request(
        "SendMessage",
        json!({"QueueUrl": &queue_url, "MessageBody": "dup", "MessageGroupId": "g1"}),
    ));
    let data: Value = serde_json::from_str(&send.body).expect("Invalid JSON");
    let original_id = data["MessageId"].as_str().unwrap().to_string();

    // Receive and delete the original message.
    let recv = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 1}),
    ));
    let data: Value = serde_json::from_str(&recv.body).expect("Invalid JSON");
    handler.handle(make_request(
        "DeleteMessage",
        json!({
            "QueueUrl": &queue_url,
            "ReceiptHandle": data["Messages"][0]["ReceiptHandle"].as_str().unwrap(),
        }),
    ));

    // Dedup window runs 5 minutes from the first send: the re-send must be
    // suppressed (respond with the original's id, no new message enqueued).
    let resend = handler.handle(make_request(
        "SendMessage",
        json!({"QueueUrl": &queue_url, "MessageBody": "dup", "MessageGroupId": "g1"}),
    ));
    assert_eq!(resend.status, 200);
    let data: Value = serde_json::from_str(&resend.body).expect("Invalid JSON");
    assert_eq!(data["MessageId"].as_str().unwrap(), original_id);

    let recv2 = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 10}),
    ));
    let data2: Value = serde_json::from_str(&recv2.body).expect("Invalid JSON");
    let messages = data2
        .get("Messages")
        .map(|a| a.as_array().map(|a| a.len()).unwrap_or(0))
        .unwrap_or(0);
    assert_eq!(messages, 0, "dedup re-send must not enqueue a message");
}

#[test]
fn test_send_message_batch_bad_entry_is_entry_error() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_fifo_queue(&handler, "fifo-batch-err.fifo", false);

    // One good entry (with group id), one bad (missing MessageGroupId).
    let resp = handler.handle(make_request(
        "SendMessageBatch",
        json!({
            "QueueUrl": &queue_url,
            "Entries": [
                {"Id": "good", "MessageBody": "hello", "MessageGroupId": "g1"},
                {"Id": "bad", "MessageBody": "no group"}
            ],
        }),
    ));
    // The bad entry must NOT fail the whole request.
    assert_eq!(resp.status, 200);
    let data: Value = serde_json::from_str(&resp.body).expect("Invalid JSON");

    let successful = data["Successful"].as_array().expect("Successful array");
    assert_eq!(successful.len(), 1);
    assert_eq!(successful[0]["Id"], "good");

    let failed = data["Failed"].as_array().expect("Failed array");
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0]["Id"], "bad");
    assert_eq!(failed[0]["SenderFault"], true);
    assert!(
        failed[0]["Code"]
            .as_str()
            .map(|c| !c.is_empty())
            .unwrap_or(false),
        "entry failure must carry an error code"
    );

    // The good entry's message was actually enqueued.
    let recv = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 10}),
    ));
    let recv_data: Value = serde_json::from_str(&recv.body).expect("Invalid JSON");
    assert_eq!(recv_data["Messages"][0]["Body"], "hello");
    let received = recv_data["Messages"].as_array().unwrap().len();
    assert_eq!(received, 1);
}

#[test]
fn test_redrive_move_to_dlq_preserves_message_id() {
    let handler = DefaultSqsHandler::new();

    let resp = handler.handle(make_request(
        "CreateQueue",
        json!({"QueueName": "src-redrive-q"}),
    ));
    let src_url: String = serde_json::from_str::<Value>(&resp.body).unwrap()["QueueUrl"]
        .as_str()
        .unwrap()
        .into();
    let resp = handler.handle(make_request(
        "CreateQueue",
        json!({"QueueName": "src-redrive-dlq"}),
    ));
    let dlq_url: String = serde_json::from_str::<Value>(&resp.body).unwrap()["QueueUrl"]
        .as_str()
        .unwrap()
        .into();

    // Wire the redrive policy maxReceiveCount=1.
    let dlq_arn = "arn:aws:sqs:us-east-1:123456789012:src-redrive-dlq";
    handler.handle(make_request(
        "SetQueueAttributes",
        json!({
            "QueueUrl": src_url,
            "Attributes": {
                "RedrivePolicy": json!({
                    "deadLetterTargetArn": dlq_arn,
                    "maxReceiveCount": 1,
                })
                .to_string(),
            },
        }),
    ));

    let sent = handler.handle(make_request(
        "SendMessage",
        json!({"QueueUrl": src_url, "MessageBody": "redrive-me"}),
    ));
    let sent_id: String = serde_json::from_str::<Value>(&sent.body).unwrap()["MessageId"]
        .as_str()
        .unwrap()
        .into();

    // First receive: receive_count=1 (<= max). The retry receive (visibility
    // 0, era re-delivery) lifts the count to 2 > maxReceiveCount, so the same
    // call redrives the message into the DLQ.
    let _ = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": src_url, "MaxNumberOfMessages": 1, "VisibilityTimeout": 0}),
    ));
    let _ = handler.handle(make_request(
        "ReceiveMessage",
        json!({
            "QueueUrl": src_url,
            "MaxNumberOfMessages": 1,
            "VisibilityTimeout": 0,
        }),
    ));
    let moved = handler.handle(make_request(
        "ReceiveMessage",
        json!({
            "QueueUrl": dlq_url,
            "MaxNumberOfMessages": 10,
            "WaitTimeSeconds": 2,
        }),
    ));
    let dlq_msgs: Value = serde_json::from_str(&moved.body).unwrap();
    let msgs = dlq_msgs["Messages"].as_array().cloned().unwrap_or_default();
    assert_eq!(msgs.len(), 1, "exhausted message redrives into the DLQ");
    assert_eq!(msgs[0]["Body"], "redrive-me", "payload is preserved");
    assert_eq!(
        msgs[0]["MessageId"], sent_id,
        "the dead-letter move preserves the original MessageId"
    );

    // The source no longer holds the moved message.
    let again = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": src_url, "MaxNumberOfMessages": 10, "WaitTimeSeconds": 0}),
    ));
    let src_msgs: Value = serde_json::from_str(&again.body).unwrap();
    assert!(
        src_msgs["Messages"]
            .as_array()
            .map(|m| m.is_empty())
            .unwrap_or(true),
        "moved messages leave the source queue"
    );
}

/// A second PurgeQueue on the same queue within 60 seconds answers
/// moto's PurgeQueueInProgress (HTTP 403) and leaves every earlier
/// message in place, exactly as the pinned fork's `purge_queue` does.
#[test]
fn purge_throttle_second_purge_within_60s_rejected() {
    let handler = DefaultSqsHandler::new();
    let name = "purge-throttle-repeated";
    let queue_url = create_queue(&handler, name);

    let first = handler.handle(make_request("PurgeQueue", json!({"QueueUrl": &queue_url})));
    assert_eq!(first.status, 200);

    // A message that arrives after the first purge must still be there
    // when the throttled second purge is rejected.
    handler.handle(make_request(
        "SendMessage",
        json!({"QueueUrl": &queue_url, "MessageBody": "survivor"}),
    ));

    let second = handler.handle(make_request("PurgeQueue", json!({"QueueUrl": &queue_url})));
    assert_eq!(second.status, 403);
    assert!(
        second
            .body
            .contains("AWS.SimpleQueueService.PurgeQueueInProgress"),
        "throttled purge must carry the PurgeQueueInProgress code, got {}",
        second.body
    );
    assert!(
        second.body.contains(&format!(
            "Only one PurgeQueue operation on {name} is allowed every 60 seconds."
        )),
        "throttled purge must carry moto's exact wording, got {}",
        second.body
    );

    // The rejected purge must not have touched the queue's messages.
    let recv = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 10}),
    ));
    let data: Value = serde_json::from_str(&recv.body).expect("Invalid JSON");
    assert_eq!(
        data["Messages"].as_array().map(|m| m.len()),
        Some(1),
        "a PurgeQueueInProgress rejection must leave stored messages alone"
    );
}

/// The throttle is per queue: purging one queue never blocks a purge of
/// another queue (each queue carries its own purge timestamp).
#[test]
fn purge_throttle_is_per_queue() {
    let handler = DefaultSqsHandler::new();
    let url_a = create_queue(&handler, "purge-throttle-a");
    let url_b = create_queue(&handler, "purge-throttle-b");

    assert_eq!(
        handler
            .handle(make_request("PurgeQueue", json!({"QueueUrl": &url_a})))
            .status,
        200
    );
    // Same instant, different queue: allowed.
    assert_eq!(
        handler
            .handle(make_request("PurgeQueue", json!({"QueueUrl": &url_b})))
            .status,
        200
    );
}

/// Once 60 seconds have passed since the last purge, purging succeeds
/// again. The stored timestamp is rewound directly instead of sleeping.
#[test]
fn purge_throttle_expires_after_60_seconds() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "purge-throttle-expiry");

    let first = handler.handle(make_request("PurgeQueue", json!({"QueueUrl": &queue_url})));
    assert_eq!(first.status, 200);
    assert_eq!(
        handler
            .handle(make_request("PurgeQueue", json!({"QueueUrl": &queue_url})))
            .status,
        403,
        "the second immediate purge must still be throttled"
    );

    // Rewind the recorded purge 61 seconds back.
    let now_ms = (Utc::now().timestamp_millis()) as u64;
    let queue_name = "purge-throttle-expiry";
    let store = handler.get_store(123456789012, "us-east-1");
    let queue_arc = store.get_queue(queue_name).expect("queue must exist");
    queue_arc.write().last_purged_at = Some(now_ms - 61_000);

    assert_eq!(
        handler
            .handle(make_request("PurgeQueue", json!({"QueueUrl": &queue_url})))
            .status,
        200,
        "a purge after the 60-second window must succeed again"
    );
}

// --- Batch entry validation (moto PR getmoto/moto#10346) -------------------

/// Send two messages and receive their handles (default visibility), the
/// fixture for proving top-level batch errors leave no entry-level side
/// effect behind (mirrors the PR's `_send_and_receive_two_messages`).
fn send_and_receive_two_messages(handler: &DefaultSqsHandler, queue_url: &str) -> Vec<Value> {
    for i in 0..2 {
        let resp = handler.handle(make_request(
            "SendMessage",
            json!({"QueueUrl": queue_url, "MessageBody": i.to_string()}),
        ));
        assert_eq!(resp.status, 200);
    }
    let resp = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": queue_url, "MaxNumberOfMessages": 2}),
    ));
    let data: Value = serde_json::from_str(&resp.body).expect("Invalid JSON");
    let messages = data["Messages"]
        .as_array()
        .expect("two queued messages received")
        .to_vec();
    assert_eq!(messages.len(), 2, "must receive both messages back");
    messages
}

/// Read (visible, not-visible, delayed) message counts for a queue.
fn queued_message_counts(handler: &DefaultSqsHandler, queue_url: &str) -> (u32, u32, u32) {
    let resp = handler.handle(make_request(
        "GetQueueAttributes",
        json!({"QueueUrl": queue_url, "AttributeNames": ["All"]}),
    ));
    let data: Value = serde_json::from_str(&resp.body).expect("Invalid JSON");
    let attrs = &data["Attributes"];
    let get = |name: &str| {
        attrs[name]
            .as_str()
            .expect("attribute must be reported")
            .parse()
            .expect("integer attribute")
    };
    (
        get("ApproximateNumberOfMessages"),
        get("ApproximateNumberOfMessagesNotVisible"),
        get("ApproximateNumberOfMessagesDelayed"),
    )
}

/// moto's PR #10346 id row: empty, punctuation, space, over-long (81 chars).
fn invalid_batch_ids() -> Vec<String> {
    vec![
        "".to_string(),
        ".!@#$%^&*()+=".to_string(),
        "has space".to_string(),
        "i".repeat(81),
    ]
}

/// DeleteMessageBatch: every Id failing moto's `^[\w-]{1,80}$` row
/// answers InvalidBatchEntryId as a top-level 400, before any entry is
/// processed (no message deleted).
#[test]
fn batch_validate_delete_message_batch_invalid_entry_id() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "batch-validate-del-bad-id");
    let handles = send_and_receive_two_messages(&handler, &queue_url);

    for id in invalid_batch_ids() {
        let resp = handler.handle(make_request(
            "DeleteMessageBatch",
            json!({
                "QueueUrl": &queue_url,
                "Entries": [{"Id": id, "ReceiptHandle": handles[0]["ReceiptHandle"]}],
            }),
        ));
        assert_eq!(resp.status, 400, "invalid Id {id:?} must fail the request");
        assert!(resp
            .body
            .contains("AWS.SimpleQueueService.InvalidBatchEntryId"));
        assert!(resp.body.contains(
            "A batch entry id can only contain alphanumeric characters, hyphens and underscores. It can be at most 80 letters long."
        ));
    }

    let counts = queued_message_counts(&handler, &queue_url);
    assert_eq!(counts.1, 2, "no message may be deleted by a rejected batch");
}

/// Repeated Ids (with distinct receipt handles) answer BatchEntryIdsNotDistinct
/// carrying the first duplicate, before side effects.
#[test]
fn batch_validate_delete_message_batch_entry_ids_not_distinct() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "batch-validate-del-dup-id");
    let handles = send_and_receive_two_messages(&handler, &queue_url);

    let resp = handler.handle(make_request(
        "DeleteMessageBatch",
        json!({
            "QueueUrl": &queue_url,
            "Entries": [
                {"Id": "dup", "ReceiptHandle": handles[0]["ReceiptHandle"]},
                {"Id": "dup", "ReceiptHandle": handles[1]["ReceiptHandle"]},
            ],
        }),
    ));
    assert_eq!(resp.status, 400);
    assert!(resp
        .body
        .contains("AWS.SimpleQueueService.BatchEntryIdsNotDistinct"));
    assert!(resp.body.contains("Id dup repeated."));

    let counts = queued_message_counts(&handler, &queue_url);
    assert_eq!(counts.1, 2, "no message may be deleted by a rejected batch");
}

/// More than 10 entries answer TooManyEntriesInBatchRequest with moto's
/// exact count wording.
#[test]
fn batch_validate_delete_message_batch_too_many_entries() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "batch-validate-del-too-many");
    let handles = send_and_receive_two_messages(&handler, &queue_url);

    let entries: Vec<Value> = (0..11)
        .map(|i| {
            json!({
                "Id": format!("id_{i}"),
                "ReceiptHandle": handles[(i % 2) as usize]["ReceiptHandle"],
            })
        })
        .collect();
    let resp = handler.handle(make_request(
        "DeleteMessageBatch",
        json!({"QueueUrl": &queue_url, "Entries": entries}),
    ));
    assert_eq!(resp.status, 400);
    assert!(resp
        .body
        .contains("AWS.SimpleQueueService.TooManyEntriesInBatchRequest"));
    assert!(resp
        .body
        .contains("Maximum number of entries per request are 10. You have sent 11."));

    let counts = queued_message_counts(&handler, &queue_url);
    assert_eq!(counts.1, 2, "no message may be deleted by a rejected batch");
}

/// An empty Entries list answers EmptyBatchRequest naming the
/// DeleteMessageBatch request entry.
#[test]
fn batch_validate_delete_message_batch_empty_entries() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "batch-validate-del-empty");

    let resp = handler.handle(make_request(
        "DeleteMessageBatch",
        json!({"QueueUrl": &queue_url, "Entries": []}),
    ));
    assert_eq!(resp.status, 400);
    assert!(resp
        .body
        .contains("AWS.SimpleQueueService.EmptyBatchRequest"));
    assert!(resp
        .body
        .contains("There should be at least one DeleteMessageBatchRequestEntry in the request."));
}

/// ChangeMessageVisibilityBatch: moto PR #10346 adds the EmptyBatchRequest
/// check with the ChangeMessageVisibilityBatchRequestEntry wording, on top
/// of the shared Id validation.
#[test]
fn batch_validate_change_message_visibility_batch_empty_entries() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "batch-validate-cmv-empty");

    let resp = handler.handle(make_request(
        "ChangeMessageVisibilityBatch",
        json!({"QueueUrl": &queue_url, "Entries": []}),
    ));
    assert_eq!(resp.status, 400);
    assert!(resp
        .body
        .contains("AWS.SimpleQueueService.EmptyBatchRequest"));
    assert!(resp.body.contains(
        "There should be at least one ChangeMessageVisibilityBatchRequestEntry in the request."
    ));
}

/// Invalid Ids on ChangeMessageVisibilityBatch answer InvalidBatchEntryId
/// before any visibility timeout is touched.
#[test]
fn batch_validate_change_message_visibility_batch_invalid_entry_id() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "batch-validate-cmv-bad-id");
    let handles = send_and_receive_two_messages(&handler, &queue_url);

    for id in invalid_batch_ids() {
        let resp = handler.handle(make_request(
            "ChangeMessageVisibilityBatch",
            json!({
                "QueueUrl": &queue_url,
                "Entries": [{
                    "Id": id,
                    "ReceiptHandle": handles[0]["ReceiptHandle"],
                    "VisibilityTimeout": 0,
                }],
            }),
        ));
        assert_eq!(resp.status, 400, "invalid Id {id} must fail the request");
        assert!(resp
            .body
            .contains("AWS.SimpleQueueService.InvalidBatchEntryId"));
        assert!(resp.body.contains(
            "A batch entry id can only contain alphanumeric characters, hyphens and underscores. It can be at most 80 letters long."
        ));
    }

    // No visibility timeout was changed: both handles remain in flight.
    let counts = queued_message_counts(&handler, &queue_url);
    assert_eq!(
        counts.1, 2,
        "no handle may leave flight via a rejected batch"
    );
}

/// Duplicate Ids on ChangeMessageVisibilityBatch answer
/// BatchEntryIdsNotDistinct with moto's "Id {id} repeated." wording.
#[test]
fn batch_validate_change_message_visibility_batch_entry_ids_not_distinct() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "batch-validate-cmv-dup-id");
    let handles = send_and_receive_two_messages(&handler, &queue_url);

    let resp = handler.handle(make_request(
        "ChangeMessageVisibilityBatch",
        json!({
            "QueueUrl": &queue_url,
            "Entries": [
                {"Id": "dup", "ReceiptHandle": handles[0]["ReceiptHandle"], "VisibilityTimeout": 0},
                {"Id": "dup", "ReceiptHandle": handles[1]["ReceiptHandle"], "VisibilityTimeout": 0},
            ],
        }),
    ));
    assert_eq!(resp.status, 400);
    assert!(resp
        .body
        .contains("AWS.SimpleQueueService.BatchEntryIdsNotDistinct"));
    assert!(resp.body.contains("Id dup repeated."));

    // No visibility timeout was changed: both handles remain in flight.
    let counts = queued_message_counts(&handler, &queue_url);
    assert_eq!(
        counts.1, 2,
        "no handle may leave flight via a rejected batch"
    );
}

/// More than 10 entries on ChangeMessageVisibilityBatch answer
/// TooManyEntriesInBatchRequest with moto's exact count wording.
#[test]
fn batch_validate_change_message_visibility_batch_too_many_entries() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "batch-validate-cmv-too-many");
    let handles = send_and_receive_two_messages(&handler, &queue_url);

    let entries: Vec<Value> = (0..11)
        .map(|i| {
            json!({
                "Id": format!("id_{i}"),
                "ReceiptHandle": handles[(i % 2) as usize]["ReceiptHandle"],
                "VisibilityTimeout": 0,
            })
        })
        .collect();
    let resp = handler.handle(make_request(
        "ChangeMessageVisibilityBatch",
        json!({"QueueUrl": &queue_url, "Entries": entries}),
    ));
    assert_eq!(resp.status, 400);
    assert!(resp
        .body
        .contains("AWS.SimpleQueueService.TooManyEntriesInBatchRequest"));
    assert!(resp
        .body
        .contains("Maximum number of entries per request are 10. You have sent 11."));
}

/// SendMessageBatch answers the same top-level Id errors it always did —
/// now via the shared helper — before enqueueing anything.
#[test]
fn batch_validate_send_message_batch_invalid_entry_id() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "batch-validate-send-bad-id");

    for i in 0..2 {
        let resp = handler.handle(make_request(
            "SendMessage",
            json!({"QueueUrl": &queue_url, "MessageBody": i.to_string()}),
        ));
        assert_eq!(resp.status, 200);
    }

    let resp = handler.handle(make_request(
        "SendMessageBatch",
        json!({
            "QueueUrl": &queue_url,
            "Entries": [{"Id": "bad!id", "MessageBody": "queued?"}],
        }),
    ));
    assert_eq!(resp.status, 400);
    assert!(resp
        .body
        .contains("AWS.SimpleQueueService.InvalidBatchEntryId"));

    let counts = queued_message_counts(&handler, &queue_url);
    assert_eq!(
        counts.0, 2,
        "no message may be enqueued by a rejected batch"
    );
}

/// Duplicate Ids fail SendMessageBatch as before, before side effects.
#[test]
fn batch_validate_send_message_batch_entry_ids_not_distinct() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "batch-validate-send-dup-id");

    let resp = handler.handle(make_request(
        "SendMessageBatch",
        json!({
            "QueueUrl": &queue_url,
            "Entries": [
                {"Id": "dup", "MessageBody": "one"},
                {"Id": "dup", "MessageBody": "two"},
            ],
        }),
    ));
    assert_eq!(resp.status, 400);
    assert!(resp
        .body
        .contains("AWS.SimpleQueueService.BatchEntryIdsNotDistinct"));
    assert!(resp.body.contains("Id dup repeated."));
}

/// More than 10 entries on SendMessageBatch answer TooManyEntriesInBatchRequest.
#[test]
fn batch_validate_send_message_batch_too_many_entries() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "batch-validate-send-too-many");

    let entries: Vec<Value> = (0..11)
        .map(|i| json!({"Id": format!("id_{i}"), "MessageBody": "body"}))
        .collect();
    let resp = handler.handle(make_request(
        "SendMessageBatch",
        json!({"QueueUrl": &queue_url, "Entries": entries}),
    ));
    assert_eq!(resp.status, 400);
    assert!(resp
        .body
        .contains("AWS.SimpleQueueService.TooManyEntriesInBatchRequest"));
    assert!(resp
        .body
        .contains("Maximum number of entries per request are 10. You have sent 11."));
}

/// An empty Entries list on SendMessageBatch answers EmptyBatchRequest
/// (moto responses.py `if entries == {}: raise EmptyBatchRequest()`),
/// not a 200 with empty success/failure lists.
#[test]
fn batch_validate_send_message_batch_empty_entries() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "batch-validate-send-empty");

    let resp = handler.handle(make_request(
        "SendMessageBatch",
        json!({"QueueUrl": &queue_url, "Entries": []}),
    ));
    assert_eq!(resp.status, 400);
    assert!(resp
        .body
        .contains("AWS.SimpleQueueService.EmptyBatchRequest"));
    assert!(resp
        .body
        .contains("There should be at least one SendMessageBatchRequestEntry in the request."));
}

/// moto models.py send_message_batch backend (live-verified): the summed
/// entry bodies above MAXIMUM_MESSAGE_LENGTH (1048576 chars) answer
/// BatchRequestTooLong with moto's exact wording, beating the Id and
/// entry-count checks, and nothing may be enqueued by a rejected batch.
/// A huge aggregate against a nonexistent queue follows moto's
/// response-layer order and answers NonExistentQueue instead (queue
/// lookup first on the send op).
#[test]
fn batch_request_too_long_send_message_batch_rejects_over_cap() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "batch-too-long");

    let big = "x".repeat(524289);
    let resp = handler.handle(make_request(
        "SendMessageBatch",
        json!({
            "QueueUrl": &queue_url,
            "Entries": [
                {"Id": "a", "MessageBody": big},
                {"Id": "b", "MessageBody": big},
            ],
        }),
    ));
    assert_eq!(resp.status, 400);
    assert!(resp
        .body
        .contains("AWS.SimpleQueueService.BatchRequestTooLong"));
    assert!(resp.body.contains(
        "Batch requests cannot be longer than 1048576 bytes. You have sent 1048578 bytes."
    ));

    let counts = queued_message_counts(&handler, &queue_url);
    assert_eq!(counts.0, 0, "nothing may be enqueued by a rejected batch");

    // The queue resolution runs before the aggregate check (item: send op
    // resolves the queue first), so TooLong never fires on a ghost queue.
    let ghost = format!("{}x", queue_url);
    let resp = handler.handle(make_request(
        "SendMessageBatch",
        json!({
            "QueueUrl": &ghost,
            "Entries": [{"Id": "a", "MessageBody": "y".repeat(1048577u32 as usize)}],
        }),
    ));
    assert_eq!(resp.status, 400);
    assert!(resp
        .body
        .contains("AWS.SimpleQueueService.NonExistentQueue"));
    assert!(
        !resp
            .body
            .contains("AWS.SimpleQueueService.BatchRequestTooLong"),
        "queue lookup must precede the aggregate limit"
    );
}

/// Exactly 1048576 chars is under moto's strictly-greater check
/// (live-verified) and the batch is sent.
#[test]
fn batch_request_too_long_send_message_batch_exact_cap_sent() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "batch-too-long-boundary");

    let resp = handler.handle(make_request(
        "SendMessageBatch",
        json!({
            "QueueUrl": &queue_url,
            "Entries": [{"Id": "a", "MessageBody": "x".repeat(1048576u32 as usize)}],
        }),
    ));
    assert_eq!(resp.status, 200);
    let data: Value = serde_json::from_str(&resp.body).expect("Invalid JSON");
    let successful = data["Successful"].as_array().expect("Successful array");
    assert_eq!(successful.len(), 1);
    assert_eq!(successful[0]["Id"], "a");
    assert_eq!(
        successful[0]["MD5OfMessageBody"], "b561f87202d04959e37588ee05cf5b10",
        "verify the full 1 MiB body traveled"
    );
}

/// moto's backend order (live-verified): the aggregate check fires
/// before TooManyEntriesInBatchRequest, so eleven over-cap entries
/// answer BatchRequestTooLong rather than the entry-count error.
#[test]
fn batch_request_too_long_beats_too_many_entries() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "batch-too-long-vs-too-many");

    let entries: Vec<Value> = (0..11)
        .map(|i| json!({"Id": format!("id_{i}"), "MessageBody": "z".repeat(200000u32 as usize)}))
        .collect();
    let resp = handler.handle(make_request(
        "SendMessageBatch",
        json!({"QueueUrl": &queue_url, "Entries": entries}),
    ));
    assert_eq!(resp.status, 400);
    assert!(resp
        .body
        .contains("AWS.SimpleQueueService.BatchRequestTooLong"));
    assert!(
        !resp
            .body
            .contains("AWS.SimpleQueueService.TooManyEntriesInBatchRequest"),
        "the aggregate limit must precede the entry-count check"
    );
}

/// moto/sqs/models.py Queue defaults report MAXIMUM_MESSAGE_LENGTH as the
/// default MaximumMessageSize queue attribute (live-verified: a fresh
/// GetQueueAttributes answers 1048576, not the 262144 figure the
/// previous default carried).
#[test]
fn attribute_defaults_maximum_message_size_matches_moto() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "attr-default-max-size");

    let resp = handler.handle(make_request(
        "GetQueueAttributes",
        json!({"QueueUrl": &queue_url, "AttributeNames": ["All"]}),
    ));
    assert_eq!(resp.status, 200);
    let data: Value = serde_json::from_str(&resp.body).expect("Invalid JSON");
    assert_eq!(data["Attributes"]["MaximumMessageSize"], "1048576");
}

/// moto responses.py delete_message_batch (pinned 5.2.4): two entries
/// sharing one ReceiptHandle answer BatchEntryIdsNotDistinct with the
/// SECOND entry's Id ("Id b repeated."), before any entry is processed.
#[test]
fn batch_validate_delete_message_batch_duplicate_receipt_handle() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "batch-validate-del-dup-handle");
    let handles = send_and_receive_two_messages(&handler, &queue_url);

    let resp = handler.handle(make_request(
        "DeleteMessageBatch",
        json!({
            "QueueUrl": &queue_url,
            "Entries": [
                {"Id": "a", "ReceiptHandle": handles[0]["ReceiptHandle"]},
                {"Id": "b", "ReceiptHandle": handles[0]["ReceiptHandle"]},
            ],
        }),
    ));
    assert_eq!(resp.status, 400);
    assert!(resp
        .body
        .contains("AWS.SimpleQueueService.BatchEntryIdsNotDistinct"));
    assert!(resp.body.contains("Id b repeated."));

    let counts = queued_message_counts(&handler, &queue_url);
    assert_eq!(counts.1, 2, "no message may be deleted by a rejected batch");
}

/// The Id-distinct check (PR getmoto/moto#10346) stays on top of the
/// receipt-handle check: duplicate Ids with DISTINCT handles still fail
/// the request, before any delete. moto 5.2.4 alone would accept this
/// (live-verified: 200 with both rows in Successful) — we keep the
/// stricter port behavior deliberately.
#[test]
fn batch_validate_delete_message_batch_duplicate_ids_distinct_handles() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "batch-validate-del-dup-ids");
    let handles = send_and_receive_two_messages(&handler, &queue_url);

    let resp = handler.handle(make_request(
        "DeleteMessageBatch",
        json!({
            "QueueUrl": &queue_url,
            "Entries": [
                {"Id": "dup", "ReceiptHandle": handles[0]["ReceiptHandle"]},
                {"Id": "dup", "ReceiptHandle": handles[1]["ReceiptHandle"]},
            ],
        }),
    ));
    assert_eq!(resp.status, 400);
    assert!(resp
        .body
        .contains("AWS.SimpleQueueService.BatchEntryIdsNotDistinct"));
    assert!(resp.body.contains("Id dup repeated."));

    let counts = queued_message_counts(&handler, &queue_url);
    assert_eq!(counts.1, 2, "no message may be deleted by a rejected batch");
}

/// The response layer caps ChangeMessageVisibility at moto's
/// MAXIMUM_VISIBILITY_TIMEOUT (43200) before the queue or receipt handle
/// are resolved: an over-cap value answers InvalidParameterValue with
/// moto's exact wording, and still does on a nonexistent queue (both
/// live-verified against the pinned fork). A u32-overflowing value takes
/// the same path, whether it arrives as a JSON number or a numeric
/// string the way the gateway's query-protocol parser produces them.
#[test]
fn visibility_cap_change_message_visibility_over_limit() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "vis-cap-single");
    handler.handle(make_request(
        "SendMessage",
        json!({"QueueUrl": &queue_url, "MessageBody": "capped"}),
    ));
    let recv = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 1}),
    ));
    let receipt_handle: String = serde_json::from_str::<Value>(&recv.body).unwrap()["Messages"][0]
        ["ReceiptHandle"]
        .as_str()
        .unwrap()
        .into();

    for (value, label) in [
        (json!(43201u64), "over-cap"),
        (json!(4294967300u64), "u32-overflow number"),
        (json!("4294967300"), "u32-overflow numeric string"),
    ] {
        let resp = handler.handle(make_request(
            "ChangeMessageVisibility",
            json!({
                "QueueUrl": &queue_url,
                "ReceiptHandle": receipt_handle,
                "VisibilityTimeout": value,
            }),
        ));
        assert_eq!(resp.status, 400, "cap breach {label} must answer 400");
        assert!(resp.body.contains("InvalidParameterValue"));
        assert!(
            resp.body
                .contains("Invalid request, maximum visibility timeout is 43200"),
            "over-cap ChangeMessageVisibility must carry moto's wording, got {}",
            resp.body
        );
    }

    // The rejected change left the message in flight untouched.
    let counts = queued_message_counts(&handler, &queue_url);
    assert_eq!(
        counts.1, 1,
        "a rejected cap breach must not change flight state"
    );

    // moto validates the timeout in the response layer, before the
    // backend queue lookup: the cap error beats a nonexistent queue too.
    let ghost = format!("{}x", queue_url);
    let resp = handler.handle(make_request(
        "ChangeMessageVisibility",
        json!({
            "QueueUrl": &ghost,
            "ReceiptHandle": "anything",
            "VisibilityTimeout": 43201,
        }),
    ));
    assert_eq!(resp.status, 400);
    assert!(resp.body.contains("InvalidParameterValue"));
    assert!(resp
        .body
        .contains("Invalid request, maximum visibility timeout is 43200"));
    assert!(
        !resp
            .body
            .contains("AWS.SimpleQueueService.NonExistentQueue"),
        "the cap check must precede the queue lookup"
    );
}

/// ChangeMessageVisibilityBatch turns an over-cap (or u32-overflowing)
/// per-entry timeout into a Failed row with moto's live-verified shape
/// — SenderFault true, Code InvalidParameterValue, Message "Visibility
/// timeout invalid" — while valid entries still apply.
#[test]
fn visibility_cap_change_message_visibility_batch_over_limit_rows() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "vis-cap-batch");
    let handles = send_and_receive_two_messages(&handler, &queue_url);

    let resp = handler.handle(make_request(
        "ChangeMessageVisibilityBatch",
        json!({
            "QueueUrl": &queue_url,
            "Entries": [
                {"Id": "ok", "ReceiptHandle": handles[0]["ReceiptHandle"], "VisibilityTimeout": 0},
                {"Id": "cap", "ReceiptHandle": handles[1]["ReceiptHandle"], "VisibilityTimeout": 43201},
                {"Id": "huge", "ReceiptHandle": handles[0]["ReceiptHandle"], "VisibilityTimeout": 4294967300u64},
            ],
        }),
    ));
    assert_eq!(resp.status, 200);
    let data: Value = serde_json::from_str(&resp.body).expect("Invalid JSON");
    let successful = data["Successful"].as_array().expect("Successful array");
    assert_eq!(successful.len(), 1);
    assert_eq!(successful[0]["Id"], "ok");

    let failed = data["Failed"].as_array().expect("Failed array");
    assert_eq!(failed.len(), 2);
    assert_eq!(failed[0]["Id"], "cap");
    assert_eq!(failed[1]["Id"], "huge");
    for row in failed {
        assert_eq!(row["SenderFault"], true);
        assert_eq!(row["Code"], "InvalidParameterValue");
        assert_eq!(row["Message"], "Visibility timeout invalid");
    }

    // The "ok" entry returned its message to the visible pool; entries
    // rejected for the cap unchanged (still in flight).
    let counts = queued_message_counts(&handler, &queue_url);
    assert_eq!(counts.0, 1, "the zeroed entry is visible again");
    assert_eq!(counts.1, 1, "the rejected entries stay in flight");
}

/// moto resolves the queue only inside the per-row backend change
/// (`change_message_visibility` -> `get_queue`, moto/sqs/models.py:1082):
/// rows that fail the per-entry parse or the cap check answer 200 Failed
/// rows and never touch the queue. Live-verified against the pinned fork:
/// both an over-cap-only and a missing-VisibilityTimeout ghost-queue batch
/// answer 200 with the Failed rows (the reviewer's ITEM-1 option (a)),
/// replacing the twin's previous eager NonExistentQueue resolution.
#[test]
fn visibility_batch_ghost_queue_failed_rows_never_resolve_queue() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "vis-batch-ghost-rows");
    let ghost = format!("{}x", queue_url);

    let resp = handler.handle(make_request(
        "ChangeMessageVisibilityBatch",
        json!({
            "QueueUrl": &ghost,
            "Entries": [
                {"Id": "cap", "ReceiptHandle": "bogus", "VisibilityTimeout": 50000},
                {"Id": "novis", "ReceiptHandle": "bogus"},
            ],
        }),
    ));
    assert_eq!(
        resp.status, 200,
        "per-row parse failures must beat the queue lookup"
    );
    assert!(
        !resp.body.contains("NonExistentQueue"),
        "a batch whose every row fails parsing must never resolve the queue, got {}",
        resp.body
    );
    let data: Value = serde_json::from_str(&resp.body).expect("Invalid JSON");
    assert_eq!(data["Successful"].as_array().map(|a| a.len()), Some(0));
    let failed = data["Failed"].as_array().expect("Failed array");
    assert_eq!(failed.len(), 2, "both rows fail parsing, in entry order");
    for row in failed {
        assert_eq!(row["SenderFault"], true);
        assert_eq!(row["Code"], "InvalidParameterValue");
        assert_eq!(row["Message"], "Visibility timeout invalid");
    }
}

/// A parse-OK row resolves the queue; on a nonexistent queue moto raises the
/// global QueueDoesNotExist and the Failed rows collected for earlier
/// parse-failing entries are discarded (live-verified: both
/// [over-cap, valid] and [missing-VisibilityTimeout, valid] ghost-queue
/// mixes answer 400 NonExistentQueue).
#[test]
fn visibility_batch_ghost_queue_valid_row_answers_nonexistent_discarding_rows() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "vis-batch-ghost-valid");
    let ghost = format!("{}x", queue_url);

    for (label, entries) in [
        (
            "over-cap then valid",
            json!([
                {"Id": "cap", "ReceiptHandle": "bogus", "VisibilityTimeout": 50000},
                {"Id": "ok", "ReceiptHandle": "bogus", "VisibilityTimeout": 30},
            ]),
        ),
        (
            "missing-visibility then valid",
            json!([
                {"Id": "novis", "ReceiptHandle": "bogus"},
                {"Id": "ok", "ReceiptHandle": "bogus", "VisibilityTimeout": 30},
            ]),
        ),
    ] {
        let resp = handler.handle(make_request(
            "ChangeMessageVisibilityBatch",
            json!({"QueueUrl": &ghost, "Entries": entries}),
        ));
        assert_eq!(resp.status, 400, "{label} must answer 400");
        assert!(
            resp.body
                .contains("AWS.SimpleQueueService.NonExistentQueue"),
            "{label}: the valid row resolves the queue and wins over prior rows, got {}",
            resp.body
        );
    }
}

/// Per-entry ReceiptHandleIsInvalid rows carry moto's live-verified shape —
/// SenderFault true and the ServiceException message text
/// ("The input receipt handle is invalid.",
/// moto/sqs/models.py:1128-1136).
#[test]
fn visibility_batch_invalid_handle_row_shape() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "vis-batch-bogus-handle");
    let handles = send_and_receive_two_messages(&handler, &queue_url);

    let resp = handler.handle(make_request(
        "ChangeMessageVisibilityBatch",
        json!({
            "QueueUrl": &queue_url,
            "Entries": [
                {"Id": "bad", "ReceiptHandle": "no-such-handle", "VisibilityTimeout": 30},
                {"Id": "good", "ReceiptHandle": handles[0]["ReceiptHandle"], "VisibilityTimeout": 30},
            ],
        }),
    ));
    assert_eq!(resp.status, 200);
    let data: Value = serde_json::from_str(&resp.body).expect("Invalid JSON");
    let successful = data["Successful"].as_array().expect("Successful array");
    assert_eq!(successful.len(), 1);
    assert_eq!(successful[0]["Id"], "good");

    let failed = data["Failed"].as_array().expect("Failed array");
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0]["Id"], "bad");
    assert_eq!(failed[0]["SenderFault"], true);
    assert_eq!(failed[0]["Code"], "ReceiptHandleIsInvalid");
    assert_eq!(failed[0]["Message"], "The input receipt handle is invalid.");
}

/// moto accepts negative VisibilityTimeouts (moto 5.2.4 live-verified:
/// top-level ChangeMessageVisibility with VisibilityTimeout=-5 answers 200
/// and the message is re-receivable immediately — Message.change_visibility
/// stores visible_at in the past). The string and number shapes take the
/// same path through the query/JSON protocols.
#[test]
fn negative_visibility_top_level_immediately_visible() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "neg-vis-top");
    handler.handle(make_request(
        "SendMessage",
        json!({"QueueUrl": &queue_url, "MessageBody": "0"}),
    ));
    let recv = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 1, "VisibilityTimeout": 30}),
    ));
    let data: Value = serde_json::from_str(&recv.body).expect("Invalid JSON");
    // Each re-receive mints a fresh handle; refresh before every CMV call.
    let mut receipt_handle: String = data["Messages"][0]["ReceiptHandle"]
        .as_str()
        .unwrap()
        .into();

    for (shape, value) in [("number", json!(-5)), ("string", json!("-5"))] {
        let resp = handler.handle(make_request(
            "ChangeMessageVisibility",
            json!({
                "QueueUrl": &queue_url,
                "ReceiptHandle": &receipt_handle,
                "VisibilityTimeout": value,
            }),
        ));
        assert_eq!(resp.status, 200, "{shape} negative must answer 200");

        // moto answers a negative like VisibilityTimeout=0 on
        // observability: no invisible window at all, the same message is
        // re-receivable on the next poll (live-verified I2-C/RAW-2 flow).
        let again = handler.handle(make_request(
            "ReceiveMessage",
            json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 1, "VisibilityTimeout": 30}),
        ));
        let data: Value = serde_json::from_str(&again.body).expect("Invalid JSON");
        let bodies: Vec<&str> = data["Messages"]
            .as_array()
            .map(|m| m.iter().filter_map(|x| x["Body"].as_str()).collect())
            .unwrap_or_default();
        assert_eq!(
            bodies,
            vec!["0"],
            "{shape} negative must make the message visible immediately"
        );
        receipt_handle = data["Messages"][0]["ReceiptHandle"]
            .as_str()
            .unwrap()
            .into();
    }

    // A large negative behaves the same.
    let resp = handler.handle(make_request(
        "ChangeMessageVisibility",
        json!({
            "QueueUrl": &queue_url,
            "ReceiptHandle": &receipt_handle,
            "VisibilityTimeout": -100,
        }),
    ));
    assert_eq!(resp.status, 200);
    let again = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 1, "VisibilityTimeout": 30}),
    ));
    let data: Value = serde_json::from_str(&again.body).expect("Invalid JSON");
    assert_eq!(
        data["Messages"].as_array().map(|m| m.len()),
        Some(1),
        "every negative timeout means immediate visibility"
    );
}

/// Negative visibility through a batch row: moto answers a Successful row
/// (int("-5") passes the <= MAXIMUM_VISIBILITY_TIMEOUT assert,
/// models.py:1108-1109) and the message becomes visible immediately
/// (live-verified).
#[test]
fn negative_visibility_batch_row_applies_and_unhides() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "neg-vis-batch");
    let handles = send_and_receive_two_messages(&handler, &queue_url);
    // Each re-receive mints a fresh handle; refresh before every batch row.
    let mut handle: String = handles[0]["ReceiptHandle"].as_str().unwrap().into();

    for (shape, timeout) in [("number", json!(-5)), ("string", json!("-5"))] {
        let resp = handler.handle(make_request(
            "ChangeMessageVisibilityBatch",
            json!({
                "QueueUrl": &queue_url,
                "Entries": [{"Id": "neg", "ReceiptHandle": &handle, "VisibilityTimeout": timeout}],
            }),
        ));
        assert_eq!(resp.status, 200);
        let data: Value = serde_json::from_str(&resp.body).expect("Invalid JSON");
        let successful = data["Successful"].as_array().expect("Successful array");
        assert_eq!(successful.len(), 1, "{shape} negative row must succeed");
        assert_eq!(successful[0]["Id"], "neg");
        let failed = data["Failed"].as_array().expect("Failed array");
        assert_eq!(failed.len(), 0, "{shape} negative row must succeed");

        // The message is visible again immediately; the second queued
        // message (hidden for the default 30s) is not.
        let again = handler.handle(make_request(
            "ReceiveMessage",
            json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 2}),
        ));
        let data: Value = serde_json::from_str(&again.body).expect("Invalid JSON");
        let bodies: Vec<&str> = data["Messages"]
            .as_array()
            .map(|m| m.iter().filter_map(|x| x["Body"].as_str()).collect())
            .unwrap_or_default();
        assert_eq!(
            bodies,
            vec!["0"],
            "{shape} negative row must unhide message 0 only"
        );
        handle = data["Messages"][0]["ReceiptHandle"]
            .as_str()
            .unwrap()
            .into();
    }
}

/// A negative timeout still runs moto's handle validation: top-level
/// answers 400 ReceiptHandleIsInvalid on an unknown handle, a batch row
/// fails with SenderFault and moto's row message (the twin always resolves
/// the queue before the handle check, per the ITEM-1 restructure).
#[test]
fn negative_visibility_invalid_handle_still_errors() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "neg-vis-bogus");

    let resp = handler.handle(make_request(
        "ChangeMessageVisibility",
        json!({
            "QueueUrl": &queue_url,
            "ReceiptHandle": "no-such-handle",
            "VisibilityTimeout": -5,
        }),
    ));
    assert_eq!(resp.status, 400);
    assert!(resp.body.contains("ReceiptHandleIsInvalid"));

    let resp = handler.handle(make_request(
        "ChangeMessageVisibilityBatch",
        json!({
            "QueueUrl": &queue_url,
            "Entries": [{"Id": "neg", "ReceiptHandle": "no-such-handle", "VisibilityTimeout": -5}],
        }),
    ));
    assert_eq!(resp.status, 200);
    let data: Value = serde_json::from_str(&resp.body).expect("Invalid JSON");
    let failed = data["Failed"].as_array().expect("Failed array");
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0]["SenderFault"], true);
    assert_eq!(failed[0]["Code"], "ReceiptHandleIsInvalid");
    assert_eq!(failed[0]["Message"], "The input receipt handle is invalid.");
}

/// moto's int(value) has no magnitude limit: a negative value beyond every
/// integer's range behaves like every other negative — accepted, immediate
/// visibility (live-verified via the bare-except flow; the i64::MIN
/// saturation lands in the clamp-to-0 arm).
#[test]
fn negative_visibility_overflow_string_immediately_visible() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "neg-vis-overflow");
    handler.handle(make_request(
        "SendMessage",
        json!({"QueueUrl": &queue_url, "MessageBody": "overflow-huge"}),
    ));
    let recv = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 1, "VisibilityTimeout": 30}),
    ));
    let receipt_handle: String = serde_json::from_str::<Value>(&recv.body).unwrap()["Messages"][0]
        ["ReceiptHandle"]
        .as_str()
        .unwrap()
        .into();

    let resp = handler.handle(make_request(
        "ChangeMessageVisibility",
        json!({
            "QueueUrl": &queue_url,
            "ReceiptHandle": &receipt_handle,
            "VisibilityTimeout": "-99999999999999999999",
        }),
    ));
    assert_eq!(resp.status, 200);
    let again = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 1, "VisibilityTimeout": 30}),
    ));
    let data: Value = serde_json::from_str(&again.body).expect("Invalid JSON");
    assert_eq!(
        data["Messages"].as_array().map(|m| m.len()),
        Some(1),
        "negative overflow must mean immediate visibility"
    );
}

/// moto's per-row arm ORDER in change_message_visibility_batch
/// (moto/sqs/models.py:1106-1126): the VisibilityTimeout parse + cap
/// assert run inside one broad bare except BEFORE
/// `entry["ReceiptHandle"]` is read. A row carrying NEITHER key fails
/// the visibility arm first and answers the per-row Failed entry
/// (live-verified: Entries=[{"Id":"m1"}] answers 200 with Failed
/// SenderFault/InvalidParameterValue/"Visibility timeout invalid"),
/// while a row whose visibility PARSED fine still crashes the whole
/// request on the absent handle key (pinned separately).
#[test]
fn cmv_batch_row_missing_both_keys_answers_failed_row() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "cmv-batch-both-missing");
    let handles = send_and_receive_two_messages(&handler, &queue_url);

    let resp = handler.handle(make_request(
        "ChangeMessageVisibilityBatch",
        json!({
            "QueueUrl": &queue_url,
            "Entries": [
                {"Id": "ok", "ReceiptHandle": handles[0]["ReceiptHandle"], "VisibilityTimeout": 0},
                {"Id": "m1"},
            ],
        }),
    ));
    assert_eq!(resp.status, 200, "got {}", resp.body);
    let data: Value = serde_json::from_str(&resp.body).expect("Invalid JSON");
    let successful = data["Successful"].as_array().expect("Successful array");
    assert_eq!(successful.len(), 1, "the first row applied before row 2");
    assert_eq!(successful[0]["Id"], "ok");

    let failed = data["Failed"].as_array().expect("Failed array");
    assert_eq!(failed.len(), 1, "the keyless row fails the visibility arm");
    assert_eq!(failed[0]["Id"], "m1");
    assert_eq!(failed[0]["SenderFault"], true);
    assert_eq!(failed[0]["Code"], "InvalidParameterValue");
    assert_eq!(failed[0]["Message"], "Visibility timeout invalid");
}

/// JSON null VisibilityTimeout members are dropped by moto's parse layer
/// (`BaseJSONParser._parse_structure` skips a member whose wire value
/// parses to None — parse.py:314-315): a receive with VisibilityTimeout:
/// null answers 200 applying the QUEUE attribute (live-verified: the twin
/// previously crashed on the Some(Null) shape), and a null on a ghost
/// queue answers NonExistentQueue like every absent resolve. The queue
/// default really applies: a queue pinned to 1s puts the message in
/// flight for that window (not a no-op, not a crash).
#[test]
fn receive_null_visibility_takes_queue_default() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "recv-null-vis");
    handler.handle(make_request(
        "SetQueueAttributes",
        json!({
            "QueueUrl": &queue_url,
            "Attributes": {"VisibilityTimeout": "1"}
        }),
    ));
    handler.handle(make_request(
        "SendMessage",
        json!({"QueueUrl": &queue_url, "MessageBody": "null-vis"}),
    ));

    let resp = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "VisibilityTimeout": Value::Null}),
    ));
    assert_eq!(
        resp.status, 200,
        "null vis must take the fallback, got {}",
        resp.body
    );

    // The 1s queue default was applied: an immediate re-receive (short
    // poll) finds the message still in flight, then it returns normally.
    let again = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 1}),
    ));
    assert!(
        serde_json::from_str::<Value>(&again.body)
            .expect("json")
            .get("Messages")
            .is_none(),
        "the null-vis receive must apply the queue default, got {}",
        again.body
    );
    thread::sleep(Duration::from_millis(1100));
    let counts = queued_message_counts(&handler, &queue_url);
    assert_eq!(counts.0, 1, "the 1s default expired");

    // On a nonexistent queue the null behaves like an absent resolve.
    let ghost = format!("{}x", queue_url);
    let resp = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &ghost, "VisibilityTimeout": Value::Null}),
    ));
    assert_eq!(resp.status, 400, "got {}", resp.body);
    assert!(
        resp.body
            .contains("AWS.SimpleQueueService.NonExistentQueue"),
        "a null vis must not crash ahead of the queue resolution, got {}",
        resp.body
    );
}

/// A null VisibilityTimeout in a batch ROW takes the missing-key route:
/// moto's parse drops the member, the backend's bare except catches the
/// lookup failure and answers the per-row Failed arm (live-verified:
/// row {"Id":"x","ReceiptHandle":h,"VisibilityTimeout":null} answers 200
/// with Failed InvalidParameterValue "Visibility timeout invalid") —
/// not the parse-layer crash a present junk value would produce.
#[test]
fn batch_row_null_visibility_answers_failed_row() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "cmv-batch-null-vis");
    let handles = send_and_receive_two_messages(&handler, &queue_url);

    let resp = handler.handle(make_request(
        "ChangeMessageVisibilityBatch",
        json!({
            "QueueUrl": &queue_url,
            "Entries": [
                {"Id": "n", "ReceiptHandle": handles[0]["ReceiptHandle"], "VisibilityTimeout": Value::Null},
                {"Id": "ok", "ReceiptHandle": handles[1]["ReceiptHandle"], "VisibilityTimeout": 30},
            ],
        }),
    ));
    assert_eq!(resp.status, 200, "got {}", resp.body);
    let data: Value = serde_json::from_str(&resp.body).expect("Invalid JSON");
    let failed = data["Failed"].as_array().expect("Failed array");
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0]["Id"], "n");
    assert_eq!(failed[0]["SenderFault"], true);
    assert_eq!(failed[0]["Code"], "InvalidParameterValue");
    assert_eq!(failed[0]["Message"], "Visibility timeout invalid");
    let successful = data["Successful"].as_array().expect("Successful array");
    assert_eq!(successful.len(), 1);
    assert_eq!(successful[0]["Id"], "ok");
}

/// The dropped-null route covers the receive bounds too: a null
/// MaxNumberOfMessages / WaitTimeSeconds member is absent for moto's
/// parse layer — the defaults (1 message, the queue wait) apply and the
/// request answers 200 (live-verified), not a parse-layer 500.
#[test]
fn receive_null_max_and_wait_take_defaults() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "recv-null-bounds");
    handler.handle(make_request(
        "SendMessage",
        json!({"QueueUrl": &queue_url, "MessageBody": "null-bounds"}),
    ));

    let resp = handler.handle(make_request(
        "ReceiveMessage",
        json!({
            "QueueUrl": &queue_url,
            "MaxNumberOfMessages": Value::Null,
            "WaitTimeSeconds": Value::Null,
            "VisibilityTimeout": Value::Null,
        }),
    ));
    assert_eq!(
        resp.status, 200,
        "null members take the defaults, got {}",
        resp.body
    );
    let data: Value = serde_json::from_str(&resp.body).expect("Invalid JSON");
    assert_eq!(
        data["Messages"].as_array().map(|m| m.len()),
        Some(1),
        "the Max default of 1 applied, got {}",
        resp.body
    );
}

/// A null ReceiptHandle in a batch ROW crashes the whole request: moto's
/// parse layer drops null members, so the row reaches the backend without
/// the key and the `entry["ReceiptHandle"]` KeyError — raised OUTSIDE the
/// per-row bare except — fails everything as a werkzeug 500 (live-verified,
/// same shape as the missing-key crash). The twin previously steered the
/// null into moto's None-mode match and bumped the FIRST message's
/// visibility — a state mutation this test also guards against: after the
/// crash the untouched first message must still be immediately receivable.
#[test]
fn cmv_batch_null_receipt_handle_crashes_like_missing() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "cmv-batch-null-handle");
    let handles = send_and_receive_two_messages(&handler, &queue_url);
    // Return both messages to the visible pool so a None-mode bump would
    // be observable as a silent state change.
    handler.handle(make_request(
        "ChangeMessageVisibilityBatch",
        json!({
            "QueueUrl": &queue_url,
            "Entries": [
                {"Id": "a", "ReceiptHandle": handles[0]["ReceiptHandle"], "VisibilityTimeout": 0},
                {"Id": "b", "ReceiptHandle": handles[1]["ReceiptHandle"], "VisibilityTimeout": 0},
            ],
        }),
    ));

    for (label, queue) in [
        ("existing queue", json!(&queue_url)),
        (
            "ghost queue keeps the crash",
            json!(format!("{}x", queue_url)),
        ),
    ] {
        let resp = handler.handle(make_request(
            "ChangeMessageVisibilityBatch",
            json!({
                "QueueUrl": queue,
                "Entries": [{"Id": "x", "ReceiptHandle": Value::Null, "VisibilityTimeout": 30}],
            }),
        ));
        assert_eq!(resp.status, 500, "{label}: got {}", resp.body);
        assert!(
            resp.body.contains("<!doctype html>")
                && resp.body.contains("500 Internal Server Error"),
            "{label}: not the moto crash envelope, got {}",
            resp.body
        );
    }

    // No row touched the queue: the first visible message is still
    // immediately receivable (the dropped-null route must not fall into
    // the None-mode bump the top-level op takes).
    let recv = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 1}),
    ));
    let data: Value = serde_json::from_str(&recv.body).expect("json");
    assert_eq!(
        data["Messages"].as_array().map(|m| m.len()),
        Some(1),
        "the 500 must leave the first message untouched, got {}",
        recv.body
    );
}

/// The top-level ChangeMessageVisibility null handle is the OPPOSITE
/// shape: the dropped member reaches the backend as None, and
/// `None in all_receipt_handles` matches every extant message's
/// pre-receive slot — the FIRST message in queue order gets bumped, 200
/// (live-verified: the bump silences the message, and a null on an empty
/// queue falls out to ReceiptHandleIsInvalid like the missing-key pin).
/// Batch rows never take this route (pinned separately).
#[test]
fn cmv_null_receipt_handle_matches_first_message() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "cmv-null-handle");
    handler.handle(make_request(
        "SendMessage",
        json!({"QueueUrl": &queue_url, "MessageBody": "t"}),
    ));

    let resp = handler.handle(make_request(
        "ChangeMessageVisibility",
        json!({
            "QueueUrl": &queue_url,
            "ReceiptHandle": Value::Null,
            "VisibilityTimeout": 300,
        }),
    ));
    assert_eq!(resp.status, 200, "got {}", resp.body);
    let recv = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 1}),
    ));
    assert!(
        serde_json::from_str::<Value>(&recv.body)
            .expect("json")
            .get("Messages")
            .is_none(),
        "the null handle bump applies like a missing handle, got {}",
        recv.body
    );

    // On an empty queue the None-mode match falls out to
    // ReceiptHandleIsInvalid, like the missing-key pin.
    let empty = create_queue(&handler, "cmv-null-empty");
    let resp = handler.handle(make_request(
        "ChangeMessageVisibility",
        json!({"QueueUrl": &empty, "ReceiptHandle": Value::Null, "VisibilityTimeout": 30}),
    ));
    assert_eq!(resp.status, 400, "got {}", resp.body);
    assert!(
        resp.body.contains("ReceiptHandleIsInvalid"),
        "got {}",
        resp.body
    );
}

/// moto's scripted parse coerces string members with the real python
/// int() (moto/core/parse.py `_parse_integer`): surrounding whitespace
/// strips (" 30 " = 30 — live-verified 200 with the bump applied and the
/// queue's short default overridden), unicode Nd digits map to their
/// ASCII value ("٣٠" = 30, "  ٣_0  " too, live-verified), PEP 515
/// underscores parse only BETWEEN digits ("1_0" = 10 and "٣_٠" = 30,
/// live-verified; "_30" raises ValueError and crashes the request in the
/// parse layer — a batch row's "_30" kills the whole request), a leading
/// sign is honored ("+30", and "+0" releases an in-flight message), and
/// the bounds accept the same coercions on their members (Max/Wait "1_0"
/// ride in-range as 10; "000" parses as 0). Junk inside the digits ("3 0")
/// never parses.
#[test]
fn python_int_coercions_string_shapes() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "py-int-coerce");
    handler.handle(make_request(
        "SetQueueAttributes",
        json!({
            "QueueUrl": &queue_url,
            "Attributes": {"VisibilityTimeout": "1"}
        }),
    ));

    // A fresh visible message per cell, released via CMV where needed.
    let seq = std::cell::Cell::new(0usize);
    let fresh_message = |handler: &DefaultSqsHandler| -> String {
        let body = format!("cell-{}", seq.get() + 1);
        seq.set(seq.get() + 1);
        handler.handle(make_request(
            "SendMessage",
            json!({"QueueUrl": &queue_url, "MessageBody": &body}),
        ));
        // A VisibilityTimeout=0 receive mints a real handle and keeps the
        // message immediately receivable; the body assert anchors every
        // cell to its own freshly sent message.
        let recv = handler.handle(make_request(
            "ReceiveMessage",
            json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 1, "VisibilityTimeout": 0}),
        ));
        let data: Value = serde_json::from_str(&recv.body).expect("json");
        assert_eq!(
            data["Messages"][0]["Body"].as_str(),
            Some(body.as_str()),
            "each cell must start from its own message, got {}",
            recv.body
        );
        data["Messages"][0]["ReceiptHandle"]
            .as_str()
            .expect("handle")
            .to_string()
    };
    let visible_count = |handler: &DefaultSqsHandler| queued_message_counts(handler, &queue_url).0;
    let inflight_count = |handler: &DefaultSqsHandler| queued_message_counts(handler, &queue_url).1;
    let crash_envelope =
        |body: &str| body.contains("<!doctype html>") && body.contains("500 Internal Server Error");
    let cleanup = |handler: &DefaultSqsHandler, handle: &str| {
        let resp = handler.handle(make_request(
            "DeleteMessage",
            json!({"QueueUrl": &queue_url, "ReceiptHandle": handle}),
        ));
        assert_eq!(resp.status, 200, "cleanup delete: got {}", resp.body);
        let counts = queued_message_counts(handler, &queue_url);
        assert_eq!(
            (counts.0, counts.1),
            (0, 0),
            "cleanup must leave the queue empty between cells"
        );
    };

    // " 30 ": the parse strips the surrounding whitespace and applies 30,
    // outlasting the queue's short default (live-verified cell). The
    // receive delivers the fresh message, then the bump holds it in
    // flight across an immediate re-receive.
    handler.handle(make_request(
        "SendMessage",
        json!({"QueueUrl": &queue_url, "MessageBody": "ws-recv"}),
    ));
    let resp = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "VisibilityTimeout": " 30 "}),
    ));
    assert_eq!(resp.status, 200, "got {}", resp.body);
    let data: Value = serde_json::from_str(&resp.body).expect("Invalid JSON");
    assert_eq!(
        data["Messages"][0]["Body"].as_str(),
        Some("ws-recv"),
        "got {}",
        resp.body
    );
    assert_eq!(
        inflight_count(&handler),
        1,
        "the whitespace-padded bump must keep the message in flight"
    );
    let resp = handler.handle(make_request(
        "DeleteMessage",
        json!({
            "QueueUrl": &queue_url,
            "ReceiptHandle": data["Messages"][0]["ReceiptHandle"].as_str().unwrap(),
        }),
    ));
    assert_eq!(resp.status, 200, "got {}", resp.body);
    let counts = queued_message_counts(&handler, &queue_url);
    assert_eq!(
        (counts.0, counts.1),
        (0, 0),
        "the queue resets for the next cell"
    );

    // CMV " 0 " releases the message instantly (0 applies, reachable
    // through the trimmed string, live-verified).
    let handle = fresh_message(&handler);
    let resp = handler.handle(make_request(
        "ChangeMessageVisibility",
        json!({
            "QueueUrl": &queue_url,
            "ReceiptHandle": &handle,
            "VisibilityTimeout": " 0 ",
        }),
    ));
    assert_eq!(resp.status, 200, "got {}", resp.body);
    assert_eq!(
        visible_count(&handler),
        1,
        "CMV ' 0 ' must return the message to the pool"
    );
    cleanup(&handler, &handle);

    // "٣_٠" = 30 via unicode digits + inner underscore; "1_0" = 10.
    let handle = fresh_message(&handler);
    let resp = handler.handle(make_request(
        "ChangeMessageVisibility",
        json!({
            "QueueUrl": &queue_url,
            "ReceiptHandle": &handle,
            "VisibilityTimeout": "٣_٠",
        }),
    ));
    assert_eq!(resp.status, 200, "got {}", resp.body);
    assert_eq!(
        inflight_count(&handler),
        1,
        "٣_٠ must parse to 30 and keep the message in flight"
    );
    cleanup(&handler, &handle);
    let handle = fresh_message(&handler);
    let resp = handler.handle(make_request(
        "ChangeMessageVisibility",
        json!({
            "QueueUrl": &queue_url,
            "ReceiptHandle": &handle,
            "VisibilityTimeout": "1_0",
        }),
    ));
    assert_eq!(resp.status, 200, "got {}", resp.body);
    assert_eq!(
        inflight_count(&handler),
        1,
        "1_0 must parse to 10 and keep the message in flight"
    );
    cleanup(&handler, &handle);

    // "_30" never parses: a motocrash-shaped 500 (live-verified), and a
    // batch row carrying it kills the whole request at the parse layer
    // (neither crash touches the queue; clean up the visible message for
    // the next cell).
    let handle = fresh_message(&handler);
    let resp = handler.handle(make_request(
        "ChangeMessageVisibility",
        json!({
            "QueueUrl": &queue_url,
            "ReceiptHandle": &handle,
            "VisibilityTimeout": "_30",
        }),
    ));
    assert_eq!(resp.status, 500, "got {}", resp.body);
    assert!(crash_envelope(&resp.body), "got {}", resp.body);
    cleanup(&handler, &handle);
    let resp = handler.handle(make_request(
        "ChangeMessageVisibilityBatch",
        json!({
            "QueueUrl": &queue_url,
            "Entries": [
                {"Id": "x", "ReceiptHandle": "any", "VisibilityTimeout": "_30"},
            ],
        }),
    ));
    assert_eq!(resp.status, 500, "got {}", resp.body);
    assert!(crash_envelope(&resp.body), "got {}", resp.body);

    // "+30" parses with the sign (200, message bumps into flight), and
    // "+0" releases the bump.
    let handle = fresh_message(&handler);
    let resp = handler.handle(make_request(
        "ChangeMessageVisibility",
        json!({"QueueUrl": &queue_url, "ReceiptHandle": &handle, "VisibilityTimeout": "+30"}),
    ));
    assert_eq!(resp.status, 200, "got {}", resp.body);
    assert_eq!(
        inflight_count(&handler),
        1,
        "+30 must parse to a 30s in-flight bump"
    );
    let resp = handler.handle(make_request(
        "ChangeMessageVisibility",
        json!({"QueueUrl": &queue_url, "ReceiptHandle": &handle, "VisibilityTimeout": "+0"}),
    ));
    assert_eq!(resp.status, 200, "got {}", resp.body);
    assert_eq!(
        visible_count(&handler),
        1,
        "+0 must release the message immediately"
    );
    cleanup(&handler, &handle);

    // Batch rows take the same coercions (live-verified cells: a " 0 "
    // row and a "٣_٠" row both succeed; the trailing row leaves the
    // message in flight for its 30).
    let handle = fresh_message(&handler);
    let resp = handler.handle(make_request(
        "ChangeMessageVisibilityBatch",
        json!({
            "QueueUrl": &queue_url,
            "Entries": [
                {"Id": "ws", "ReceiptHandle": &handle, "VisibilityTimeout": " 0 "},
                {"Id": "uni", "ReceiptHandle": &handle, "VisibilityTimeout": "٣_٠"},
            ],
        }),
    ));
    assert_eq!(resp.status, 200, "got {}", resp.body);
    let data: Value = serde_json::from_str(&resp.body).expect("Invalid JSON");
    let successful = data["Successful"].as_array().expect("Successful array");
    assert_eq!(successful.len(), 2, "got {}", resp.body);
    cleanup(&handler, &handle);

    // The receive bounds accept the same ints: "1_0" = 10 rides in
    // range for Max (live-verified) and lands inside the wait bounds;
    // in-bounds numeric coercions keep their 200.
    let resp = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": "1_0"}),
    ));
    assert_eq!(
        resp.status, 200,
        "Max '1_0' parses to 10, got {}",
        resp.body
    );
    let resp = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "WaitTimeSeconds": "000"}),
    ));
    assert_eq!(
        resp.status, 200,
        "Wait '000' parses to 0, got {}",
        resp.body
    );
}

/// The rewired happy path: valid entries still delete, through the same
/// helper that rejects the invalid ones.
#[test]
fn batch_validate_delete_message_batch_valid_entries_still_delete() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "batch-validate-del-valid");
    let handles = send_and_receive_two_messages(&handler, &queue_url);

    let resp = handler.handle(make_request(
        "DeleteMessageBatch",
        json!({
            "QueueUrl": &queue_url,
            "Entries": [
                {"Id": "first", "ReceiptHandle": handles[0]["ReceiptHandle"]},
                {"Id": "second", "ReceiptHandle": handles[1]["ReceiptHandle"]},
            ],
        }),
    ));
    assert_eq!(resp.status, 200);
    let data: Value = serde_json::from_str(&resp.body).expect("Invalid JSON");
    let successful = data["Successful"].as_array().expect("Successful array");
    assert_eq!(successful.len(), 2);
    assert!(
        data.get("Failed")
            .map(|f| f.as_array().unwrap().is_empty())
            .unwrap_or(true),
        "no entry may fail on valid handles"
    );

    let counts = queued_message_counts(&handler, &queue_url);
    assert_eq!(counts.1, 0, "both in-flight handles must be deleted");
}

/// The rewired happy path: valid entries still change visibility, and a
/// zero timeout returns the messages to the visible pool immediately.
#[test]
fn batch_validate_change_message_visibility_batch_valid_entries_still_apply() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "batch-validate-cmv-valid");
    let handles = send_and_receive_two_messages(&handler, &queue_url);

    let resp = handler.handle(make_request(
        "ChangeMessageVisibilityBatch",
        json!({
            "QueueUrl": &queue_url,
            "Entries": [
                {"Id": "first", "ReceiptHandle": handles[0]["ReceiptHandle"], "VisibilityTimeout": 0},
                {"Id": "second", "ReceiptHandle": handles[1]["ReceiptHandle"], "VisibilityTimeout": 0},
            ],
        }),
    ));
    assert_eq!(resp.status, 200);
    let data: Value = serde_json::from_str(&resp.body).expect("Invalid JSON");
    assert_eq!(
        data["Successful"]
            .as_array()
            .expect("Successful array")
            .len(),
        2
    );

    // Zero visibility means both messages are deliverable again at once.
    let again = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 2}),
    ));
    let data: Value = serde_json::from_str(&again.body).expect("Invalid JSON");
    assert_eq!(
        data["Messages"].as_array().map(|m| m.len()),
        Some(2),
        "visibility zero must return both messages for delivery"
    );
}

/// SendMessageBatch mirrors moto responses.py `send_message_batch`, which
/// resolves the queue via the backend BEFORE inspecting Entries: a
/// missing queue answers NonExistentQueue even with a malformed batch,
/// and the batch Id errors only fire against a real queue (both
/// live-verified against the pinned fork). The "/x" suffix turns the real
/// queue's URL into one for a queue that was never created without
/// hardcoding an endpoint.
#[test]
fn send_message_batch_resolves_queue_before_batch_validation() {
    let handler = DefaultSqsHandler::new();
    let queue_url = format!("{}x", create_queue(&handler, "bq-lookup-send-anchor"));

    // Nonexistent queue wins over every batch-shaped error, even with
    // duplicate Ids that themselves would fail the request.
    let resp = handler.handle(make_request(
        "SendMessageBatch",
        json!({
            "QueueUrl": &queue_url,
            "Entries": [
                {"Id": "dup", "MessageBody": "one"},
                {"Id": "dup", "MessageBody": "two"},
            ],
        }),
    ));
    assert_eq!(resp.status, 400);
    assert!(resp
        .body
        .contains("AWS.SimpleQueueService.NonExistentQueue"));
    assert!(
        !resp
            .body
            .contains("AWS.SimpleQueueService.BatchEntryIdsNotDistinct"),
        "queue lookup must fire before batch Id validation on send"
    );
    assert!(
        !resp
            .body
            .contains("AWS.SimpleQueueService.EmptyBatchRequest"),
        "queue lookup must fire before the entries check on send"
    );

    // A real queue lets the batch validation answer for a malformed batch.
    let real_url = create_queue(&handler, "bq-lookup-send-real");
    let resp = handler.handle(make_request(
        "SendMessageBatch",
        json!({
            "QueueUrl": &real_url,
            "Entries": [
                {"Id": "dup", "MessageBody": "one"},
                {"Id": "dup", "MessageBody": "two"},
            ],
        }),
    ));
    assert_eq!(resp.status, 400);
    assert!(resp
        .body
        .contains("AWS.SimpleQueueService.BatchEntryIdsNotDistinct"));
    assert!(resp.body.contains("Id dup repeated."));
    let resp = handler.handle(make_request(
        "SendMessageBatch",
        json!({"QueueUrl": &real_url, "Entries": []}),
    ));
    assert_eq!(resp.status, 400);
    assert!(resp
        .body
        .contains("AWS.SimpleQueueService.EmptyBatchRequest"));
}

/// DeleteMessageBatch with duplicate Ids on a missing queue must answer
/// BatchEntryIdsNotDistinct, not QueueDoesNotExist (validation runs first).
#[test]
fn batch_validate_before_queue_lookup_delete_message_batch() {
    let handler = DefaultSqsHandler::new();
    let queue_url = format!("{}x", create_queue(&handler, "bq-lookup-del-anchor"));

    let resp = handler.handle(make_request(
        "DeleteMessageBatch",
        json!({
            "QueueUrl": &queue_url,
            "Entries": [
                {"Id": "dup", "ReceiptHandle": "h1"},
                {"Id": "dup", "ReceiptHandle": "h2"},
            ],
        }),
    ));
    assert_eq!(resp.status, 400);
    assert!(resp
        .body
        .contains("AWS.SimpleQueueService.BatchEntryIdsNotDistinct"));
    assert!(resp.body.contains("Id dup repeated."));
    assert!(
        !resp
            .body
            .contains("AWS.SimpleQueueService.NonExistentQueue"),
        "queue existence must not be resolved before batch validation"
    );
}

/// ChangeMessageVisibilityBatch with duplicate Ids on a missing queue must
/// answer BatchEntryIdsNotDistinct, not QueueDoesNotExist.
#[test]
fn batch_validate_before_queue_lookup_change_message_visibility_batch() {
    let handler = DefaultSqsHandler::new();
    let queue_url = format!("{}x", create_queue(&handler, "bq-lookup-cmv-anchor"));

    let resp = handler.handle(make_request(
        "ChangeMessageVisibilityBatch",
        json!({
            "QueueUrl": &queue_url,
            "Entries": [
                {"Id": "dup", "ReceiptHandle": "h1", "VisibilityTimeout": 0},
                {"Id": "dup", "ReceiptHandle": "h2", "VisibilityTimeout": 0},
            ],
        }),
    ));
    assert_eq!(resp.status, 400);
    assert!(resp
        .body
        .contains("AWS.SimpleQueueService.BatchEntryIdsNotDistinct"));
    assert!(resp.body.contains("Id dup repeated."));
    assert!(
        !resp
            .body
            .contains("AWS.SimpleQueueService.NonExistentQueue"),
        "queue existence must not be resolved before batch validation"
    );
}

/// moto's receive path validates the requested VisibilityTimeout in the
/// response layer AFTER the queue lookup but before the backend
/// receive/long-poll (`_get_validated_visibility_timeout` →
/// MaxVisibilityTimeout, responses.py receive_message): values above 43200
/// answer 400 InvalidParameterValue with moto's exact wording on both the
/// JSON-number and the query-protocol string shapes, and 43200 passes at
/// the boundary (live-verified: 50000 → 400, 43200 → 200, 43201 → 400).
#[test]
fn receive_visibility_over_cap_answers_max_visibility_wording() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "recv-cap-boundary");

    // The boundary receive (empty queue, no wait) answers 200 and no error.
    let resp = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 1, "VisibilityTimeout": 43200}),
    ));
    assert_eq!(resp.status, 200, "43200 passes the cap at the boundary");

    // A visible message is NOT consumed by the rejected calls, and moto's
    // exact wording carries on both value shapes.
    handler.handle(make_request(
        "SendMessage",
        json!({"QueueUrl": &queue_url, "MessageBody": "cap-guard"}),
    ));
    for (shape, value) in [
        ("number 50000", json!(50000)),
        ("string 50000", json!("50000")),
        ("number 43201", json!(43201)),
        ("string 43201", json!("43201")),
    ] {
        let resp = handler.handle(make_request(
            "ReceiveMessage",
            json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 1, "VisibilityTimeout": value}),
        ));
        assert_eq!(
            resp.status, 400,
            "over-cap receive ({shape}) must answer 400"
        );
        assert!(
            resp.body.contains("InvalidParameterValue"),
            "over-cap receive ({shape}) must be InvalidParameterValue, got {}",
            resp.body
        );
        assert!(
            resp.body
                .contains("Invalid request, maximum visibility timeout is 43200"),
            "over-cap receive ({shape}) must carry moto's wording, got {}",
            resp.body
        );
    }
    let counts = queued_message_counts(&handler, &queue_url);
    assert_eq!(
        counts.0, 1,
        "the rejected receive must not consume the message"
    );
    assert_eq!(
        counts.1, 0,
        "the rejected receive must not touch flight state"
    );
}

/// moto validates the receive VisibilityTimeout AFTER the queue lookup
/// (responses.py receive_message resolves the queue first): the same
/// over-cap request on a nonexistent queue answers NonExistentQueue, not
/// the cap error (live-verified) — so the guard sits after resolution.
#[test]
fn receive_visibility_cap_requires_existing_queue_first() {
    let handler = DefaultSqsHandler::new();
    let ghost = format!("{}x", create_queue(&handler, "recv-cap-ghost"));

    for (label, entry) in [
        (
            "over-cap on ghost queue",
            json!({"QueueUrl": &ghost, "MaxNumberOfMessages": 1, "VisibilityTimeout": 50000}),
        ),
        (
            "valid on ghost queue",
            json!({"QueueUrl": &ghost, "MaxNumberOfMessages": 1, "VisibilityTimeout": 30}),
        ),
    ] {
        let resp = handler.handle(make_request("ReceiveMessage", entry));
        assert_eq!(resp.status, 400, "{label} must answer 400");
        assert!(
            resp.body
                .contains("AWS.SimpleQueueService.NonExistentQueue"),
            "{label}: the queue lookup precedes the receive cap (live-verified order), got {}",
            resp.body
        );
    }
}

/// Pull the first message's original sent_timestamp into the past so the
/// Total-VisibilityTimeout rule can be exercised at deterministic ages —
/// the live fork accumulates age with wall-clock time, which unit tests
/// cannot wait out.
fn age_first_message(handler: &DefaultSqsHandler, queue_url_for_store: &str, age_seconds: u64) {
    let queue_name = queue_url_for_store
        .rsplit('/')
        .next()
        .expect("queue name from url");
    let store = handler.get_store(123456789012, "us-east-1");
    let queue = store.get_queue(queue_name).expect("queue exists");
    let mut q = queue.write();
    let msg = q.messages.front_mut().expect("message exists");
    msg.sent_timestamp = (Utc::now().timestamp_millis() as u64).saturating_sub(age_seconds * 1000);
}

/// moto's Total-VisibilityTimeout rule (moto/sqs/models.py:1082-1096):
/// `ChangeMessageVisibility(43200)` on a message whose accumulated
/// lifetime (age + new timeout) exceeds 43200s answers 400
/// InvalidParameterValue with the exact total-rule wording (live-verified:
/// a fresh message already breaches at vis=43200 because the HTTP round
/// trip puts it 1ms past send).
#[test]
fn total_visibility_timeout_rule_fresh_top_level() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "total-rule-fresh");
    handler.handle(make_request(
        "SendMessage",
        json!({"QueueUrl": &queue_url, "MessageBody": "t"}),
    ));
    let recv = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 1}),
    ));
    let handle: String = serde_json::from_str::<Value>(&recv.body).expect("json")["Messages"][0]
        ["ReceiptHandle"]
        .as_str()
        .expect("handle")
        .to_string();

    // The twin services requests in-process; outcome = age + 43200 > 43200
    // needs a non-zero age, mirrored here with a small sleep.
    thread::sleep(Duration::from_millis(5));
    let resp = handler.handle(make_request(
        "ChangeMessageVisibility",
        json!({
            "QueueUrl": &queue_url,
            "ReceiptHandle": handle,
            "VisibilityTimeout": 43200
        }),
    ));
    assert_eq!(resp.status, 400);
    assert!(
        resp.body.contains("InvalidParameterValue"),
        "got {}",
        resp.body
    );
    assert!(
        resp.body.contains(
            "Value 43200 for parameter VisibilityTimeout is invalid. Reason: Total \
             VisibilityTimeout for the message is beyond the limit [43200 seconds]"
        ),
        "got {}",
        resp.body
    );
}

/// The static 43200 cap wins BEFORE the total rule: 50000 is refused by
/// the response-layer MaxVisibilityTimeout wording, never the total-rule
/// message (live-verified 1a2).
#[test]
fn total_visibility_timeout_rule_static_cap_wins_first() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "total-rule-cap");
    handler.handle(make_request(
        "SendMessage",
        json!({"QueueUrl": &queue_url, "MessageBody": "t"}),
    ));
    let recv = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 1}),
    ));
    let handle: String = serde_json::from_str::<Value>(&recv.body).expect("json")["Messages"][0]
        ["ReceiptHandle"]
        .as_str()
        .expect("handle")
        .to_string();

    let resp = handler.handle(make_request(
        "ChangeMessageVisibility",
        json!({
            "QueueUrl": &queue_url,
            "ReceiptHandle": handle,
            "VisibilityTimeout": 50000
        }),
    ));
    assert_eq!(resp.status, 400);
    assert!(
        resp.body
            .contains("Invalid request, maximum visibility timeout is 43200"),
        "got {}",
        resp.body
    );
    assert!(
        !resp.body.contains("Total VisibilityTimeout"),
        "the total rule must not fire before the static cap, got {}",
        resp.body
    );
}

/// Aged message: the rule counts from the ORIGINAL send, so an aged
/// message breaches the total rule even for visibilities under the static
/// cap; a mid-range visibility with enough headroom still applies (200).
/// Live-verified 1d: aged(5s) 43200 and 43195 -> 400 total-rule, 42000 ->
/// 200 OK; the comparison stays exclusive (`> 43200*1000`).
#[test]
fn total_visibility_timeout_rule_aged_message_top_level() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "total-rule-aged");
    handler.handle(make_request(
        "SendMessage",
        json!({"QueueUrl": &queue_url, "MessageBody": "t"}),
    ));
    let recv = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 1}),
    ));
    let handle: String = serde_json::from_str::<Value>(&recv.body).expect("json")["Messages"][0]
        ["ReceiptHandle"]
        .as_str()
        .expect("handle")
        .to_string();

    age_first_message(&handler, &queue_url, 120);

    for (label, vis, expect_ok) in [
        ("43200 on aged (43320 total) breaches", 43200, false),
        ("43195 on aged (43315 total) breaches", 43195, false),
        ("42000 on aged (42120 total) applies", 42000, true),
    ] {
        let resp = handler.handle(make_request(
            "ChangeMessageVisibility",
            json!({
                "QueueUrl": &queue_url,
                "ReceiptHandle": handle,
                "VisibilityTimeout": vis
            }),
        ));
        assert_eq!(
            resp.status,
            if expect_ok { 200 } else { 400 },
            "{label}: got {}",
            resp.body
        );
        if !expect_ok {
            assert!(
                resp.body.contains(
                    "Reason: Total VisibilityTimeout for the message is beyond the limit [43200 seconds]"
                ),
                "{label}: got {}",
                resp.body
            );
        }
    }
}

/// The batch form does NOT turn a Total-rule breach into a per-row Failed
/// entry: moto's change_message_visibility_batch catches only
/// ReceiptHandleIsInvalid, so the raised InvalidParameterValue fails the
/// WHOLE request (live-verified 1b) and collected rows are discarded.
#[test]
fn total_visibility_timeout_rule_batch_whole_request() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "total-rule-batch");
    handler.handle(make_request(
        "SendMessage",
        json!({"QueueUrl": &queue_url, "MessageBody": "t"}),
    ));
    let recv = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 1}),
    ));
    let handle: String = serde_json::from_str::<Value>(&recv.body).expect("json")["Messages"][0]
        ["ReceiptHandle"]
        .as_str()
        .expect("handle")
        .to_string();
    age_first_message(&handler, &queue_url, 100);

    let resp = handler.handle(make_request(
        "ChangeMessageVisibilityBatch",
        json!({
            "QueueUrl": &queue_url,
            "Entries": [
                {"Id": "a", "ReceiptHandle": handle, "VisibilityTimeout": 43200}
            ]
        }),
    ));
    assert_eq!(resp.status, 400, "whole request 400, got {}", resp.body);
    assert!(
        resp.body.contains("InvalidParameterValue"),
        "got {}",
        resp.body
    );
    assert!(
        resp.body.contains(
            "Reason: Total VisibilityTimeout for the message is beyond the limit [43200 seconds]"
        ),
        "got {}",
        resp.body
    );
    // A 400 error envelope carries no batch rows at all.
    assert!(!resp.body.contains("Failed"), "got {}", resp.body);
}

/// Per-row over-cap values keep the Failed-row shape — the parse/assert
/// arm fires before any total-rule consideration — while the aged
/// under-cap row in the same batch still applies (live-verified 1c2).
#[test]
fn total_visibility_timeout_rule_batch_over_cap_keeps_failed_row() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "total-rule-batch-cap");
    handler.handle(make_request(
        "SendMessage",
        json!({"QueueUrl": &queue_url, "MessageBody": "t"}),
    ));
    let recv = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 1}),
    ));
    let handle: String = serde_json::from_str::<Value>(&recv.body).expect("json")["Messages"][0]
        ["ReceiptHandle"]
        .as_str()
        .expect("handle")
        .to_string();
    age_first_message(&handler, &queue_url, 100);

    let resp = handler.handle(make_request(
        "ChangeMessageVisibilityBatch",
        json!({
            "QueueUrl": &queue_url,
            "Entries": [
                {"Id": "over", "ReceiptHandle": handle, "VisibilityTimeout": 50000},
                {"Id": "ok", "ReceiptHandle": handle, "VisibilityTimeout": 42000}
            ]
        }),
    ));
    assert_eq!(
        resp.status, 200,
        "over-cap rows stay per-row, got {}",
        resp.body
    );
    let data: Value = serde_json::from_str(&resp.body).expect("Invalid JSON");
    let failed = data["Failed"].as_array().expect("Failed array");
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0]["Id"], "over");
    assert_eq!(failed[0]["Code"], "InvalidParameterValue");
    assert_eq!(failed[0]["Message"], "Visibility timeout invalid");
    let successful = data["Successful"].as_array().expect("Successful array");
    assert_eq!(successful.len(), 1);
    assert_eq!(successful[0]["Id"], "ok");
}

/// The total rule takes the timeout signed: a negative timeout on a hugely
/// aged message still breaches (now + (-5s) - sent_timestamp > 43200s,
/// live-verified via the C1 signed path; the fresh arm is live-verified
/// G2: 200 and the message becomes visible immediately).
#[test]
fn total_visibility_timeout_rule_signed_with_negatives() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "total-rule-signed");
    handler.handle(make_request(
        "SendMessage",
        json!({"QueueUrl": &queue_url, "MessageBody": "t"}),
    ));
    let recv = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 1}),
    ));
    let handle: String = serde_json::from_str::<Value>(&recv.body).expect("json")["Messages"][0]
        ["ReceiptHandle"]
        .as_str()
        .expect("handle")
        .to_string();

    // Aged far beyond the limit: a negative value still breaches.
    age_first_message(&handler, &queue_url, 50_000);
    let resp = handler.handle(make_request(
        "ChangeMessageVisibility",
        json!({
            "QueueUrl": &queue_url,
            "ReceiptHandle": handle,
            "VisibilityTimeout": -5
        }),
    ));
    assert_eq!(
        resp.status, 400,
        "aged-negative breaches, got {}",
        resp.body
    );
    assert!(
        resp.body
            .contains("Value -5 for parameter VisibilityTimeout is invalid"),
        "got {}",
        resp.body
    );

    // Fresh message with a negative timeout: passes the rule, message
    // becomes visible immediately (pre-existing clamp behavior).
    handler.handle(make_request(
        "SendMessage",
        json!({"QueueUrl": &queue_url, "MessageBody": "fresh"}),
    ));
    let recv = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 1}),
    ));
    let handle: String = serde_json::from_str::<Value>(&recv.body).expect("json")["Messages"][0]
        ["ReceiptHandle"]
        .as_str()
        .expect("handle")
        .to_string();
    let resp = handler.handle(make_request(
        "ChangeMessageVisibility",
        json!({
            "QueueUrl": &queue_url,
            "ReceiptHandle": handle,
            "VisibilityTimeout": -5
        }),
    ));
    assert_eq!(
        resp.status, 200,
        "fresh-negative applies, got {}",
        resp.body
    );
    let counts = queued_message_counts(&handler, &queue_url);
    assert_eq!(counts.0, 1, "the message is visible again");
}

/// moto crashes (plain werkzeug-500 HTML, no AWS envelope) on a top-level
/// ChangeMessageVisibility whose VisibilityTimeout is missing or
/// unparseable: `int("abc")` raises ValueError inside the automated parse
/// layer and a missing value reaches `_get_validated_visibility_timeout`'s
/// `None > 43200` TypeError (live-verified both shapes). Numeric strings
/// stay honored — the query protocol delivers them and moto's model parser
/// coerces with int() (live-verified: "30" applies). The crash precedes
/// the queue resolution entirely (live-verified: junk vis on a nonexistent
/// queue answers the 500, not NonExistentQueue; a missing QueueUrl with a
/// missing VisibilityTimeout answers 500 as well).
#[test]
fn cmv_junk_visibility_crash_mirrors_moto() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "cmv-junk-mirror");
    handler.handle(make_request(
        "SendMessage",
        json!({"QueueUrl": &queue_url, "MessageBody": "t"}),
    ));
    let recv = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 1}),
    ));
    let handle: String = serde_json::from_str::<Value>(&recv.body).expect("json")["Messages"][0]
        ["ReceiptHandle"]
        .as_str()
        .expect("handle")
        .to_string();

    let base = |extra: Value| {
        let mut params = serde_json::Map::new();
        params.insert("QueueUrl".into(), json!(queue_url));
        params.insert("ReceiptHandle".into(), json!(handle));
        if let Some(obj) = extra.as_object() {
            for (k, v) in obj {
                params.insert(k.clone(), v.clone());
            }
        }
        handler.handle(make_request(
            "ChangeMessageVisibility",
            Value::Object(params),
        ))
    };

    for (label, extra) in [
        ("junk string vis", json!({"VisibilityTimeout": "abc"})),
        ("missing vis", json!({})),
        ("object vis shape", json!({"VisibilityTimeout": {"x": 1}})),
        (
            "junk vis on ghost queue",
            json!({"QueueUrl": format!("{}x", queue_url), "VisibilityTimeout": "abc"}),
        ),
        (
            "QueueUrl null and missing vis",
            json!({"QueueUrl": Value::Null}),
        ),
    ] {
        let resp = base(extra);
        assert_eq!(resp.status, 500, "{label}: got {}", resp.body);
        assert!(
            resp.body.contains("<!doctype html>")
                && resp.body.contains("500 Internal Server Error"),
            "{label}: not the moto crash envelope, got {}",
            resp.body
        );
    }

    // The numeric-string shape still parses and applies (query-protocol
    // form values are coerced by moto's model parser, live-verified).
    let resp = handler.handle(make_request(
        "ChangeMessageVisibility",
        json!({
            "QueueUrl": &queue_url,
            "ReceiptHandle": handle,
            "VisibilityTimeout": "30"
        }),
    ));
    assert_eq!(
        resp.status, 200,
        "numeric strings are honored, got {}",
        resp.body
    );
}

/// Batch counterpart: moto's automated parse int()s every row's
/// VisibilityTimeout before the response layer runs, so ONE junk row
/// anywhere in Entries kills the whole request with the werkzeug 500 —
/// the backend's broad `except:` never gets to see that value
/// (live-verified: a lone "abc" row and a [valid, junk] mix both crash).
/// A MISSING key does not crash: the response-layer bare except catches
/// the lookup failure and answers the per-row Failed arm, which the twin
/// already models.
#[test]
fn cmv_junk_visibility_batch_row_kills_whole_request() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "cmv-junk-batch");
    let handles = send_and_receive_two_messages(&handler, &queue_url);

    for (label, entries) in [
        (
            "lone junk row",
            json!([{"Id": "j", "ReceiptHandle": handles[0]["ReceiptHandle"], "VisibilityTimeout": "abc"}]),
        ),
        (
            "valid row then junk row",
            json!([
                {"Id": "ok", "ReceiptHandle": handles[0]["ReceiptHandle"], "VisibilityTimeout": 30},
                {"Id": "j", "ReceiptHandle": handles[1]["ReceiptHandle"], "VisibilityTimeout": "abc"},
            ]),
        ),
    ] {
        let resp = handler.handle(make_request(
            "ChangeMessageVisibilityBatch",
            json!({"QueueUrl": &queue_url, "Entries": entries}),
        ));
        assert_eq!(resp.status, 500, "{label}: got {}", resp.body);
        assert!(
            resp.body.contains("<!doctype html>")
                && resp.body.contains("500 Internal Server Error"),
            "{label}: not the moto crash envelope, got {}",
            resp.body
        );
        // The crash discards any other rows: no batch result at all.
        assert!(!resp.body.contains("Failed"), "{label}: got {}", resp.body);
    }

    // Missing-key rows keep the Failed-row arm (no crash), on a live
    // queue: moto's per-row bare except catches the absent key.
    let resp = handler.handle(make_request(
        "ChangeMessageVisibilityBatch",
        json!({
            "QueueUrl": &queue_url,
            "Entries": [
                {"Id": "novis", "ReceiptHandle": handles[0]["ReceiptHandle"]},
                {"Id": "ok", "ReceiptHandle": handles[1]["ReceiptHandle"], "VisibilityTimeout": 30},
            ]
        }),
    ));
    assert_eq!(
        resp.status, 200,
        "absent-key rows stay per-row, got {}",
        resp.body
    );
    let data: Value = serde_json::from_str(&resp.body).expect("Invalid JSON");
    let failed = data["Failed"].as_array().expect("Failed array");
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0]["Code"], "InvalidParameterValue");
    assert_eq!(failed[0]["Message"], "Visibility timeout invalid");
    let successful = data["Successful"].as_array().expect("Successful array");
    assert_eq!(successful.len(), 1);
    assert_eq!(successful[0]["Id"], "ok");
}

/// A present-but-unparseable ReceiveMessage VisibilityTimeout mirrors
/// moto's parse-layer crash: the automated parameter parse int()s the
/// model-declared integer before the response method runs, so `"abc"`,
/// an object shape or any non-numeric member kills the request as a
/// plain werkzeug 500 — on an EXISTING queue and on a nonexistent one
/// alike (live-verified: the crash precedes the queue resolution and
/// even the QueueUrl handling). A MISSING value does not crash: the
/// response layer catches the fallback TypeError and applies the queue
/// attribute (live-verified). Numeric strings and booleans keep moto's
/// int() coercion (live-verified: "30" is honored, `true` receives at
/// vis=1).
#[test]
fn receive_junk_visibility_crash() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "receive-junk-vis");

    for (label, vis) in [
        ("junk string", json!("abc")),
        ("empty string", json!("")),
        ("object shape", json!({ "x": 1 })),
        ("array shape", json!([1])),
    ] {
        let resp = handler.handle(make_request(
            "ReceiveMessage",
            json!({"QueueUrl": &queue_url, "VisibilityTimeout": vis}),
        ));
        assert_eq!(resp.status, 500, "{label}: got {}", resp.body);
        assert!(
            resp.body.contains("<!doctype html>")
                && resp.body.contains("500 Internal Server Error"),
            "{label}: not the moto crash envelope, got {}",
            resp.body
        );
    }

    // The crash precedes the queue resolution: a nonexistent queue keeps
    // the same 500 instead of NonExistentQueue.
    let resp = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": format!("{}x", queue_url), "VisibilityTimeout": "abc"}),
    ));
    assert_eq!(
        resp.status, 500,
        "crash beats the queue lookup, got {}",
        resp.body
    );

    // A missing value takes the queue-attribute fallback (no crash), and a
    // nonexistent queue still answers NonExistentQueue for it.
    let resp = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url}),
    ));
    assert_eq!(
        resp.status, 200,
        "absent vis receives normally, got {}",
        resp.body
    );
    let resp = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": format!("{}x", queue_url)}),
    ));
    assert_eq!(
        resp.status, 400,
        "absent vis on ghost queue, got {}",
        resp.body
    );
    assert!(
        resp.body
            .contains("AWS.SimpleQueueService.NonExistentQueue"),
        "got {}",
        resp.body
    );

    // Numeric strings keep the int() coercion: "0" releases instantly
    // where the un-parsed 30s queue default would stay in flight.
    handler.handle(make_request(
        "SendMessage",
        json!({"QueueUrl": &queue_url, "MessageBody": "a"}),
    ));
    let resp = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "VisibilityTimeout": "0"}),
    ));
    assert_eq!(
        resp.status, 200,
        "numeric string vis=0 parses, got {}",
        resp.body
    );
    let counts = queued_message_counts(&handler, &queue_url);
    assert_eq!(counts.0, 1, "the numeric-string vis=0 was applied");
    assert_eq!(counts.1, 0);

    // Booleans are Python ints for moto's parser (live-verified:
    // VisibilityTimeout=true answers 200): int(false) == 0 releases
    // instantly.
    handler.handle(make_request(
        "SendMessage",
        json!({"QueueUrl": &queue_url, "MessageBody": "b"}),
    ));
    let resp = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "VisibilityTimeout": false}),
    ));
    assert_eq!(
        resp.status, 200,
        "bool is int(False) == 0, got {}",
        resp.body
    );
    let counts = queued_message_counts(&handler, &queue_url);
    assert_eq!(counts.0, 2, "the boolean vis=0 was applied instantly");
    assert_eq!(counts.1, 0);

    // int(true) == 1 puts the message in flight for one second — distinct
    // from both a crash and the 30s queue default.
    handler.handle(make_request(
        "SendMessage",
        json!({"QueueUrl": &queue_url, "MessageBody": "c"}),
    ));
    let resp = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "VisibilityTimeout": true}),
    ));
    assert_eq!(
        resp.status, 200,
        "bool is int(True) == 1, got {}",
        resp.body
    );
    let counts = queued_message_counts(&handler, &queue_url);
    assert_eq!(counts.1, 1, "vis=1 keeps the message in flight");
    thread::sleep(Duration::from_millis(1100));
    let counts = queued_message_counts(&handler, &queue_url);
    assert_eq!(counts.0, 3, "vis=1 expired; queue-default 30s would not");
    assert_eq!(counts.1, 0);
}

/// Empty-string junk is a parse-layer ValueError for moto (`int("")`),
/// and it crashes like any other junk string everywhere the automated
/// parse int()s a member: top-level ChangeMessageVisibility
/// VisibilityTimeout, the receive VisibilityTimeout, the receive
/// MaxNumberOfMessages and WaitTimeSeconds bounds, and batch rows
/// (live-verified, each cell answering the 500 HTML page; the batch-row
/// cell notably kills the WHOLE request — the parse layer runs before
/// the backend's per-row bare except can turn anything into a Failed
/// row). An empty RECEIPT HANDLE keeps the per-row Failed arm — a string
/// shape, never int()-ed (pinned with the missing-key crash).
#[test]
fn empty_string_visibility_junk_crashes_like_moto() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "empty-vis-junk");
    handler.handle(make_request(
        "SendMessage",
        json!({"QueueUrl": &queue_url, "MessageBody": "t"}),
    ));
    let recv = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 1}),
    ));
    let handle: String = serde_json::from_str::<Value>(&recv.body).expect("json")["Messages"][0]
        ["ReceiptHandle"]
        .as_str()
        .expect("handle")
        .to_string();

    let crash_envelope =
        |body: &str| body.contains("<!doctype html>") && body.contains("500 Internal Server Error");

    // Top-level ChangeMessageVisibility with "".
    let resp = handler.handle(make_request(
        "ChangeMessageVisibility",
        json!({
            "QueueUrl": &queue_url,
            "ReceiptHandle": &handle,
            "VisibilityTimeout": "",
        }),
    ));
    assert_eq!(resp.status, 500, "got {}", resp.body);
    assert!(crash_envelope(&resp.body), "got {}", resp.body);

    // The receive visibility (also covered by receive_junk_visibility_crash).
    let resp = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "VisibilityTimeout": ""}),
    ));
    assert_eq!(resp.status, 500, "got {}", resp.body);
    assert!(crash_envelope(&resp.body), "got {}", resp.body);

    // The receive bounds junk route (Max=''/Wait='') is pinned
    // separately with the junk MaxNumberOfMessages/WaitTimeSeconds
    // finding — the vis-family mask above cannot reach those call sites
    // on its own.

    // A batch row's "" visibility kills the whole request at the parse
    // layer (live-verified 500, no Failed rows) — the backend's per-row
    // bare except never sees the value.
    let resp = handler.handle(make_request(
        "ChangeMessageVisibilityBatch",
        json!({
            "QueueUrl": &queue_url,
            "Entries": [
                {"Id": "x", "ReceiptHandle": &handle, "VisibilityTimeout": ""},
            ],
        }),
    ));
    assert_eq!(resp.status, 500, "got {}", resp.body);
    assert!(crash_envelope(&resp.body), "got {}", resp.body);
    assert!(
        !resp.body.contains("Failed"),
        "the parse-layer crash discards all rows, got {}",
        resp.body
    );

    // The empty-string RECEIPT HANDLE keeps the per-row Failed arm
    // (moto matches nothing and raises ReceiptHandleIsInvalid inside the
    // per-row try, live-verified) — string shapes never reach int().
    let resp = handler.handle(make_request(
        "ChangeMessageVisibilityBatch",
        json!({
            "QueueUrl": &queue_url,
            "Entries": [
                {"Id": "e", "ReceiptHandle": "", "VisibilityTimeout": 30},
            ],
        }),
    ));
    assert_eq!(resp.status, 200, "got {}", resp.body);
    let data: Value = serde_json::from_str(&resp.body).expect("Invalid JSON");
    let failed = data["Failed"].as_array().expect("Failed array");
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0]["Code"], "ReceiptHandleIsInvalid");
    assert_eq!(failed[0]["Message"], "The input receipt handle is invalid.");
}

/// moto 400s an out-of-range MaxNumberOfMessages in the ReceiveMessage
/// response layer, after the queue resolution (live-verified wording:
/// "An error occurred (InvalidParameterValue) when calling the
/// ReceiveMessage operation: Value 0 for parameter MaxNumberOfMessages is
/// invalid. Reason: must be between 1 and 10, if provided."). Boundaries:
/// 0 and 11 fail, 1 and 10 pass. The message stays deliverable for
/// remaining requests (validation happens before any receive).
#[test]
fn receive_bounds_max_number_of_messages() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "recv-bounds-max");
    handler.handle(make_request(
        "SendMessage",
        json!({"QueueUrl": &queue_url, "MessageBody": "b"}),
    ));

    for (label, max, expect_ok) in [
        ("0 below range", 0, false),
        ("11 above range", 11, false),
        ("100 above range", 100, false),
        ("1 at floor", 1, true),
        ("10 at ceiling", 10, true),
    ] {
        let resp = handler.handle(make_request(
            "ReceiveMessage",
            json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": max, "VisibilityTimeout": 0}),
        ));
        assert_eq!(
            resp.status,
            if expect_ok { 200 } else { 400 },
            "{label}: got {}",
            resp.body
        );
        if !expect_ok {
            assert!(
                resp.body.contains(&format!(
                    "Value {max} for parameter MaxNumberOfMessages is invalid. Reason: \
                     must be between 1 and 10, if provided."
                )),
                "{label}: got {}",
                resp.body
            );
            assert!(
                resp.body.contains(
                    "An error occurred (InvalidParameterValue) when calling the ReceiveMessage operation:"
                ),
                "{label}: moto frames the message with the operation prefix, got {}",
                resp.body
            );
        }
    }

    // The numeric-string shape the query protocol delivers parses the same
    // way (moto's model parser int()s it, live-verified behavior).
    let resp = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": "0"}),
    ));
    assert_eq!(resp.status, 400, "numeric string parses, got {}", resp.body);
    assert!(
        resp.body
            .contains("Value 0 for parameter MaxNumberOfMessages"),
        "got {}",
        resp.body
    );
}

/// The bounds ride AFTER the queue resolution: both an out-of-range
/// MaxNumberOfMessages and a WaitTimeSeconds=25 on a nonexistent queue
/// answer NonExistentQueue (live-verified ordering).
#[test]
fn receive_bounds_ghost_queue_answers_nonexistent_first() {
    let handler = DefaultSqsHandler::new();
    let ghost = format!("{}x", create_queue(&handler, "recv-bounds-ghost"));

    for (label, req) in [
        (
            "Max=0",
            json!({"QueueUrl": &ghost, "MaxNumberOfMessages": 0}),
        ),
        (
            "Max=11",
            json!({"QueueUrl": &ghost, "MaxNumberOfMessages": 11}),
        ),
        (
            "Wait=25",
            json!({"QueueUrl": &ghost, "WaitTimeSeconds": 25}),
        ),
        (
            "Max=0 Wait=25",
            json!({"QueueUrl": &ghost, "MaxNumberOfMessages": 0, "WaitTimeSeconds": 25}),
        ),
    ] {
        let resp = handler.handle(make_request("ReceiveMessage", req));
        assert_eq!(resp.status, 400, "{label}: got {}", resp.body);
        assert!(
            resp.body
                .contains("AWS.SimpleQueueService.NonExistentQueue"),
            "{label}: the queue lookup precedes the bounds, got {}",
            resp.body
        );
    }
}

/// moto validates the wait bounds on the EFFECTIVE wait — the request
/// value or the queue attribute (live-verified: a queue configured with
/// ReceiveMessageWaitTimeSeconds=25 makes a plain receive answer the
/// WaitTimeSeconds error). The wait bounds lose to the Max bounds and win
/// over the VisibilityTimeout cap. The wordings are moto's exact strings,
/// including the wait message's html-escaped oddity.
#[test]
fn receive_bounds_wait_time_seconds() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "recv-bounds-wait");
    // A queued message keeps the in-range receives fast (no long poll).
    handler.handle(make_request(
        "SendMessage",
        json!({"QueueUrl": &queue_url, "MessageBody": "b"}),
    ));

    for (label, wait, expect_ok) in [
        ("25 above range", 25, false),
        ("100 above range", 100, false),
        ("-1 below range", -1, false),
        ("0 low boundary", 0, true),
        ("20 high boundary", 20, true),
    ] {
        let resp = handler.handle(make_request(
            "ReceiveMessage",
            json!({
                "QueueUrl": &queue_url,
                "WaitTimeSeconds": wait,
                // keep the message re-available for the next sub-case
                "VisibilityTimeout": 0
            }),
        ));
        assert_eq!(
            resp.status,
            if expect_ok { 200 } else { 400 },
            "{label}: got {}",
            resp.body
        );
        if !expect_ok {
            assert!(
                resp.body.contains(&format!(
                    "Value {wait} for parameter WaitTimeSeconds is invalid. Reason: \
                     must be &lt;= 0 and &gt;= 20 if provided."
                )),
                "{label}: got {}",
                resp.body
            );
        }
    }

    // Numeric strings parse the same way (int("-25") = -25 errors with
    // the same wording).
    let resp = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "WaitTimeSeconds": "-25"}),
    ));
    assert_eq!(resp.status, 400, "got {}", resp.body);
    assert!(
        resp.body
            .contains("Value -25 for parameter WaitTimeSeconds is invalid"),
        "got {}",
        resp.body
    );

    // Order: Max bounds beat Wait bounds; both bounds beat the
    // VisibilityTimeout cap (live-verified pairings).
    for (label, req, expected) in [
        (
            "Max error beats Wait",
            json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 0, "WaitTimeSeconds": 25}),
            "Value 0 for parameter MaxNumberOfMessages is invalid",
        ),
        (
            "Wait error beats the vis cap",
            json!({"QueueUrl": &queue_url, "WaitTimeSeconds": 25, "VisibilityTimeout": 50000}),
            "Value 25 for parameter WaitTimeSeconds is invalid",
        ),
        (
            "Max error beats the vis cap",
            json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 0, "VisibilityTimeout": 50000}),
            "Value 0 for parameter MaxNumberOfMessages is invalid",
        ),
        (
            "vis cap alone still errors",
            json!({"QueueUrl": &queue_url, "VisibilityTimeout": 50000}),
            "Invalid request, maximum visibility timeout is 43200",
        ),
    ] {
        let resp = handler.handle(make_request("ReceiveMessage", req));
        assert_eq!(resp.status, 400, "{label}: got {}", resp.body);
        assert!(
            resp.body.contains(expected),
            "{label}: expected {expected}, got {}",
            resp.body
        );
    }

    // Queue attribute participates: ReceiveMessageWaitTimeSeconds=25 makes
    // a plain receive answer the WaitTimeSeconds error (live-verified:
    // Value 25 wording identical to the request-parameter one).
    let q25 = create_queue(&handler, "recv-wait-attr");
    let resp = handler.handle(make_request(
        "SetQueueAttributes",
        json!({
            "QueueUrl": &q25,
            "Attributes": {"ReceiveMessageWaitTimeSeconds": "25"}
        }),
    ));
    assert_eq!(resp.status, 200, "got {}", resp.body);
    let resp = handler.handle(make_request("ReceiveMessage", json!({"QueueUrl": &q25})));
    assert_eq!(
        resp.status, 400,
        "queue wait=25 must error, got {}",
        resp.body
    );
    assert!(
        resp.body
            .contains("Value 25 for parameter WaitTimeSeconds is invalid"),
        "got {}",
        resp.body
    );
}

/// Re-adjudication of the round-1 MotoCrash pin for a MISSING top-level
/// ChangeMessageVisibility ReceiptHandle (round-2 could not reproduce the
/// 500). Live-verified verdict against the pinned fork: moto does NOT
/// crash — the backend matches `None in all_receipt_handles`, and every
/// extant moto Message keeps its pre-receive None in that list, so the
/// FIRST message in queue order wins and the visibility bump applies
/// (200 OK, MessageNotVisible counter reflects the bump; a ghost queue
/// still answers NonExistentQueue first). The MotoCrash answer is removed.
#[test]
fn cmv_missing_receipt_handle_matches_first_message() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "cmv-none-handle");
    handler.handle(make_request(
        "SendMessage",
        json!({"QueueUrl": &queue_url, "MessageBody": "t"}),
    ));

    let resp = handler.handle(make_request(
        "ChangeMessageVisibility",
        json!({"QueueUrl": &queue_url, "VisibilityTimeout": 300}),
    ));
    assert_eq!(
        resp.status, 200,
        "a missing handle matches the first message like moto, got {}",
        resp.body
    );
    // The 300s bump silenced the first message: the short poll finds
    // nothing. (message_counts() keeps never-received messages in the
    // visible bucket — its is_inflight gate predates this lane — so the
    // receive wire shape is the discriminating check here.)
    let recv = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 1}),
    ));
    assert!(
        serde_json::from_str::<Value>(&recv.body)
            .expect("json")
            .get("Messages")
            .is_none(),
        "the bumped message must be invisible, got {}",
        recv.body
    );

    // Release the bump through the same None-mode (vis 0), then run the
    // with-handle arm: the same 200 flows for the real handle
    // (live-verified 54).
    let resp = handler.handle(make_request(
        "ChangeMessageVisibility",
        json!({"QueueUrl": &queue_url, "VisibilityTimeout": 0}),
    ));
    assert_eq!(resp.status, 200, "got {}", resp.body);

    let recv = handler.handle(make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 1, "VisibilityTimeout": 0}),
    ));
    let handle: String = serde_json::from_str::<Value>(&recv.body).expect("json")["Messages"][0]
        ["ReceiptHandle"]
        .as_str()
        .expect("handle")
        .to_string();
    let resp = handler.handle(make_request(
        "ChangeMessageVisibility",
        json!({"QueueUrl": &queue_url, "ReceiptHandle": handle, "VisibilityTimeout": 10}),
    ));
    assert_eq!(resp.status, 200, "got {}", resp.body);
    let counts = queued_message_counts(&handler, &queue_url);
    assert_eq!(
        counts.1, 1,
        "the with-handle bump kept the received message in flight"
    );
}

/// The None-mode match only exists while a message is present: on an
/// empty (or all-unmatched) queue moto's loop falls out to the bare
/// `raise ReceiptHandleIsInvalid` (live-verified B1: 400 with ". The
/// input receipt handle is invalid." wording).
#[test]
fn cmv_missing_receipt_handle_empty_queue_is_invalid() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "cmv-none-empty");

    let resp = handler.handle(make_request(
        "ChangeMessageVisibility",
        json!({"QueueUrl": &queue_url, "VisibilityTimeout": 30}),
    ));
    assert_eq!(resp.status, 400, "got {}", resp.body);
    assert!(
        resp.body.contains("ReceiptHandleIsInvalid"),
        "got {}",
        resp.body
    );
    assert!(
        resp.body.contains("The input receipt handle is invalid."),
        "got {}",
        resp.body
    );
}

/// The total rule runs on the message the None-mode match found
/// (live-verified C1: a missing handle with vis=43200 on an aged message
/// answers the Total-rule 400, not 200).
#[test]
fn cmv_missing_receipt_handle_runs_total_rule() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "cmv-none-total");
    handler.handle(make_request(
        "SendMessage",
        json!({"QueueUrl": &queue_url, "MessageBody": "t"}),
    ));
    age_first_message(&handler, &queue_url, 100);

    let resp = handler.handle(make_request(
        "ChangeMessageVisibility",
        json!({"QueueUrl": &queue_url, "VisibilityTimeout": 43200}),
    ));
    assert_eq!(resp.status, 400, "got {}", resp.body);
    assert!(
        resp.body.contains(
            "Reason: Total VisibilityTimeout for the message is beyond the limit [43200 seconds]"
        ),
        "got {}",
        resp.body
    );
}

/// Ordering stands: the queue resolution precedes the handle match, so a
/// missing handle on a ghost queue answers NonExistentQueue, not the moto
/// None-mode 200 nor an invalid-handle error (live-verified 53).
#[test]
fn cmv_missing_receipt_handle_ghost_queue_answers_nonexistent() {
    let handler = DefaultSqsHandler::new();
    let ghost = format!("{}x", create_queue(&handler, "cmv-none-ghost"));

    let resp = handler.handle(make_request(
        "ChangeMessageVisibility",
        json!({"QueueUrl": &ghost, "VisibilityTimeout": 30}),
    ));
    assert_eq!(resp.status, 400, "got {}", resp.body);
    assert!(
        resp.body
            .contains("AWS.SimpleQueueService.NonExistentQueue"),
        "got {}",
        resp.body
    );
}

/// The SPLIT for batch rows: moto reads entry["ReceiptHandle"] strictly —
/// a row without the key raises KeyError outside the visibility bare-
/// except and crashes the WHOLE request as a werkzeug 500 (live-verified;
/// it also precedes the per-row queue lookup), while a present bogus or
/// empty handle keeps the per-row Failed arm.
#[test]
fn cmv_batch_row_without_receipt_handle_key_crashes_like_moto() {
    let handler = DefaultSqsHandler::new();
    let queue_url = create_queue(&handler, "cmv-none-batch");

    for (label, queue) in [
        ("existing queue", json!(&queue_url)),
        (
            "ghost queue keeps the crash",
            json!(format!("{}x", queue_url)),
        ),
    ] {
        let resp = handler.handle(make_request(
            "ChangeMessageVisibilityBatch",
            json!({
                "QueueUrl": queue,
                "Entries": [{"Id": "a", "VisibilityTimeout": 30}]
            }),
        ));
        assert_eq!(resp.status, 500, "{label}: got {}", resp.body);
        assert!(
            resp.body.contains("<!doctype html>")
                && resp.body.contains("500 Internal Server Error"),
            "{label}: not the moto crash envelope, got {}",
            resp.body
        );
    }

    // The with-handle arm keeps the Failed row for unmatched values
    // (live-verified 56/57: empty-string and bogus handles answer 200
    // with a ReceiptHandleIsInvalid Failed entry).
    let handles = send_and_receive_two_messages(&handler, &queue_url);
    let resp = handler.handle(make_request(
        "ChangeMessageVisibilityBatch",
        json!({
            "QueueUrl": &queue_url,
            "Entries": [
                {"Id": "e", "ReceiptHandle": "", "VisibilityTimeout": 30},
                {"Id": "b", "ReceiptHandle": handles[0]["ReceiptHandle"], "VisibilityTimeout": 30},
            ]
        }),
    ));
    assert_eq!(resp.status, 200, "got {}", resp.body);
    let data: Value = serde_json::from_str(&resp.body).expect("Invalid JSON");
    let failed = data["Failed"].as_array().expect("Failed array");
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0]["Code"], "ReceiptHandleIsInvalid");
    assert_eq!(failed[0]["Message"], "The input receipt handle is invalid.");
    let successful = data["Successful"].as_array().expect("Successful array");
    assert_eq!(successful.len(), 1);
    assert_eq!(successful[0]["Id"], "b");
}
