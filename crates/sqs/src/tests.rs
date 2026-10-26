//! Unit and integration tests for SQS handler

use crate::bridge;
use crate::handler::DefaultSqsHandler;
use crate::models::{Queue, SqsStore};
use crate::protocol::{AwsRequest, SqsHandler};
use bytes::Bytes;
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

    // MaxNumberOfMessages > 10 should be clamped (or error)
    let recv_req = make_request(
        "ReceiveMessage",
        json!({"QueueUrl": &queue_url, "MaxNumberOfMessages": 11}),
    );
    let recv_resp = handler.handle(recv_req);
    // The handler clamps to 10, so this should succeed with empty result
    assert_eq!(recv_resp.status, 200);
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
