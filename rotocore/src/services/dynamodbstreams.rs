//! DynamoDB Streams backend (json protocol; target prefix
//! `DynamoDBStreams_20120810`).
//!
//! Mirrors moto's `DynamoDBStreamsBackend`: the stream metadata and the
//! captured mutation records live inside the dynamodb crate's tables,
//! reached through that crate's cross-service bridge the way moto's
//! backend reaches `dynamodb_backends[account][region]`. Shard iterators
//! are this backend's own per-(account, region) state; tokens are
//! moto-shaped (`{stream_arn}|1|{random-blob}`), and `GetRecords` mints
//! the `NextShardIterator` moto-style — positioned after the last record
//! returned, or at the current position when the shard returned nothing.

use crate::store;
use crate::{Request, Response};
use base64::Engine as _;
use once_cell::sync::Lazy;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

use dynamodb::models::Table;

/// moto's default page size when `GetRecords(Limit=...)` is omitted.
const DEFAULT_LIMIT: u64 = 1000;

/// A minted shard iterator: where a token reads from.
#[derive(Debug, Clone)]
struct IteratorRec {
    stream_arn: String,
    shard_id: String,
    /// Absolute sequence number this iterator starts at (inclusive). u128:
    /// moto's starting constant is a 22-digit literal and exceeds u64.
    position: u128,
}

/// A shard's starting sequence number as moto compares it.
fn starting_number(s: &str) -> u128 {
    s.parse().unwrap_or(0)
}

#[derive(Default)]
struct State {
    /// token -> minted iterator (moto's `shard_iterators` dict).
    iterators: HashMap<String, IteratorRec>,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("dynamodbstreams"));
    SLOT.clone()
}

fn ok(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

fn error(status: u16, code: &str, message: &str) -> Response {
    crate::proto::json_error(status, code, message)
}

fn param_str<'a>(p: &'a Value, key: &str) -> Option<&'a str> {
    p.get(key).and_then(Value::as_str)
}

fn param_u64(p: &Value, key: &str) -> Option<u64> {
    p.get(key).and_then(Value::as_u64)
}

/// Table name out of a table or stream ARN
/// (`arn:aws:dynamodb:...:table/<name>[/stream/<label>]`) — moto parses
/// `arn.split(":", 6)[5].split("/")[1]` the same way.
fn table_name_from_arn(arn: &str) -> Option<&str> {
    let after = arn.split_once(":table/")?.1;
    let name = after.split('/').next()?;
    if name.is_empty() {
        None
    } else {
        Some(name)
    }
}

/// The live table behind a stream/table ARN, or the moto-shaped error
/// (moto's `_get_table_from_arn` → `dynamodb_backends.get_table`).
fn table_from_arn(req: &Request, arn: &str) -> Result<Arc<Table>, Response> {
    let Some(name) = table_name_from_arn(arn) else {
        return Err(error(400, "ValidationException", "Invalid StreamArn"));
    };
    dynamodb::bridge::get_table(req.account, &req.region, name).ok_or_else(|| {
        error(
            400,
            "ResourceNotFoundException",
            &format!("Requested resource not found: Table: {name} not found"),
        )
    })
}

/// Mint a moto-shaped iterator token: `{stream_arn}|1|{base64(blob)}`.
fn mint_token(stream_arn: &str) -> String {
    let mut blob = Vec::with_capacity(472);
    while blob.len() < 472 {
        blob.extend_from_slice(uuid::Uuid::new_v4().as_bytes());
    }
    blob.truncate(472);
    format!(
        "{stream_arn}|1|{}",
        base64::engine::general_purpose::STANDARD.encode(&blob)
    )
}

/// Record an iterator and return its token.
fn mint_iterator(st: &mut State, rec: IteratorRec) -> String {
    let token = mint_token(&rec.stream_arn);
    st.iterators.insert(token.clone(), rec);
    token
}

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "dynamodbstreams" {
        return None;
    }
    let body = crate::proto::parse_json_body(&req.body);
    let resp = match req.operation.as_str() {
        "ListStreams" => list_streams(req, &body),
        "DescribeStream" => describe_stream(req, &body),
        "GetShardIterator" => get_shard_iterator(req, &body),
        "GetRecords" => get_records(req, &body),
        _ => return None,
    };
    Some(resp)
}

/// The stream-permutation entry: `{StreamArn, TableName, StreamLabel}` for
/// every stream-enabled table, moto's `list_streams` shape.
fn stream_entry(table: &Table) -> Value {
    let label = table.stream_label.read().clone().unwrap_or_default();
    let arn = format!("{}/stream/{}", table.table_arn.read(), label);
    json!({
        "StreamArn": arn,
        "TableName": table.name,
        "StreamLabel": label,
    })
}

fn list_streams(req: &Request, p: &Value) -> Response {
    let table_filter = param_str(p, "TableName").map(String::from);

    let mut streams: Vec<Value> = Vec::new();
    if let Some(state) = dynamodb::bridge::get_state(req.account, &req.region) {
        // Deterministic order (moto walks its dict; the legacy bridge walked
        // alphabetically) — one shard per stream-enabled table, moto-style.
        let tables = state.tables.read();
        let mut names: Vec<&String> = tables.keys().collect();
        names.sort();
        for name in names {
            let table = &tables[name];
            if let Some(filter) = &table_filter {
                if name != filter {
                    continue;
                }
            }
            if table.stream_label.read().is_some() {
                streams.push(stream_entry(table));
            }
        }
    }

    // moto ignores `ExclusiveStartStreamArn` (responses.py passes only
    // TableName down to the backend), so we do too.
    if let Some(limit) = param_u64(p, "Limit") {
        streams.truncate(limit as usize);
    }
    let out = json!({ "Streams": streams });
    ok(&out)
}

fn describe_stream(req: &Request, p: &Value) -> Response {
    let Some(arn) = param_str(p, "StreamArn").map(String::from) else {
        return error(400, "ValidationException", "StreamArn is required");
    };
    let table = match table_from_arn(req, &arn) {
        Ok(t) => t,
        Err(e) => return e,
    };

    let mut shards: Vec<Value> = Vec::new();
    {
        let shard_guard = table.stream_shard.read();
        if let Some(shard) = shard_guard.as_ref() {
            shards.push(shard.to_json());
        }
    }
    // Honor Limit (botocore member; moto ignores it, but the shard count of
    // a stream-enabled table is 1 so truncation can only ever satisfy it).
    if let Some(limit) = param_u64(p, "Limit") {
        shards.truncate(limit as usize);
    }

    let label = table.stream_label.read().clone().unwrap_or_default();
    let key_schema: Vec<Value> = table
        .key_schema
        .iter()
        .map(|k| json!({"AttributeName": k.attribute_name, "KeyType": k.key_type}))
        .collect();
    let description = json!({
        "StreamArn": arn,
        "StreamLabel": label,
        "StreamStatus": if table.stream_label.read().is_some() { "ENABLED" } else { "DISABLED" },
        "StreamViewType": table.stream_view_type.read().clone(),
        "CreationRequestDateTime": crate::proto::now_unix(),
        "TableName": table.name,
        "KeySchema": key_schema,
        "Shards": shards,
    });
    ok(&json!({ "StreamDescription": description }))
}

fn get_shard_iterator(req: &Request, p: &Value) -> Response {
    let Some(arn) = param_str(p, "StreamArn").map(String::from) else {
        return error(400, "ValidationException", "StreamArn is required");
    };
    let shard_id = param_str(p, "ShardId").unwrap_or("").to_string();
    let iter_type = param_str(p, "ShardIteratorType").unwrap_or("");
    let seq_param = param_str(p, "SequenceNumber").and_then(|s| s.parse::<u128>().ok());

    let table = match table_from_arn(req, &arn) {
        Ok(t) => t,
        Err(e) => return e,
    };

    let position = {
        let shard_guard = table.stream_shard.read();
        let Some(shard) = shard_guard.as_ref() else {
            return error(
                400,
                "ShardNotFoundException",
                &format!("Shard not found: {shard_id}"),
            );
        };
        if shard.shard_id != shard_id {
            return error(
                400,
                "ShardNotFoundException",
                &format!("Shard not found: {shard_id}"),
            );
        }
        match iter_type {
            "TRIM_HORIZON" => starting_number(&shard.starting_sequence_number),
            "LATEST" => {
                starting_number(&shard.starting_sequence_number)
                    + shard.records.read().len() as u128
            }
            "AT_SEQUENCE_NUMBER" | "AFTER_SEQUENCE_NUMBER" => {
                let Some(seq) = seq_param else {
                    return error(
                        400,
                        "ValidationException",
                        "SequenceNumber is required when ShardIteratorType is AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER",
                    );
                };
                if iter_type == "AFTER_SEQUENCE_NUMBER" {
                    seq + 1
                } else {
                    seq
                }
            }
            other => {
                return error(
                    400,
                    "ValidationException",
                    &format!("Unknown ShardIteratorType valid values: {other}"),
                )
            }
        }
    };

    let slot = state();
    let mut st = slot.write();
    let bucket = st.entry((req.account, req.region.clone())).or_default();
    let token = mint_iterator(
        bucket,
        IteratorRec {
            stream_arn: arn,
            shard_id,
            position,
        },
    );
    ok(&json!({ "ShardIterator": token }))
}

fn get_records(req: &Request, p: &Value) -> Response {
    let Some(token) = param_str(p, "ShardIterator").map(String::from) else {
        return error(400, "ValidationException", "ShardIterator is required");
    };
    let slot = state();
    let mut st = slot.write();
    let bucket = st.entry((req.account, req.region.clone())).or_default();
    let Some(rec) = bucket.iterators.get(&token).cloned() else {
        return error(
            400,
            "ValidationException",
            "The provided shard iterator has expired or is invalid",
        );
    };

    let table = match table_from_arn(req, &rec.stream_arn) {
        Ok(t) => t,
        Err(e) => return e,
    };
    let start_guard = table.stream_shard.read();
    let starting = start_guard
        .as_ref()
        .map(|s| starting_number(&s.starting_sequence_number))
        .unwrap_or(rec.position);
    let offset = rec.position.saturating_sub(starting);
    let records: Vec<Value> = start_guard
        .as_ref()
        .map(|s| {
            let all = s.records.read();
            all.iter()
                .enumerate()
                .filter(|(idx, _)| (*idx as u128) >= offset)
                .map(|(_, r)| r.clone())
                .take(param_u64(p, "Limit").unwrap_or(DEFAULT_LIMIT) as usize)
                .collect()
        })
        .unwrap_or_default();
    drop(start_guard);

    // minted seq position for the next read (moto: AFTER the last record
    // when the shard returned anything, AT the current position otherwise).
    let last_seq = records
        .last()
        .and_then(|r| r["dynamodb"]["SequenceNumber"].as_str())
        .and_then(|s| s.parse::<u128>().ok());
    let next_position = last_seq.map_or(rec.position, |last| last + 1);
    let next = mint_iterator(
        bucket,
        IteratorRec {
            stream_arn: rec.stream_arn,
            shard_id: rec.shard_id,
            position: next_position,
        },
    );
    ok(&json!({ "NextShardIterator": next, "Records": records }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dynamodb::handler::DynamoDbHandler;
    use dynamodb::protocol::AwsRequest;
    use serde_json::json as j;

    fn ddb(region: &str, op: &str, params: Value) -> AwsRequest {
        AwsRequest {
            service: "dynamodb".to_string(),
            operation: op.to_string(),
            account: 123456789012,
            region: region.to_string(),
            params: params.clone(),
            body: serde_json::to_vec(&params).unwrap().into(),
        }
    }

    fn streams_req(region: &str, op: &str, body: Value) -> Request {
        Request {
            service: "dynamodbstreams".to_string(),
            operation: op.to_string(),
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: serde_json::to_vec(&body).unwrap().into(),
            account: 123456789012,
            region: region.to_string(),
        }
    }

    fn parsed(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap()
    }

    /// CreateTable+PutItem through the real dynamodb handler (whose
    /// get_state call registers the bridge), then the streams surface.
    #[test]
    fn list_describe_iterator_records_roundtrip() {
        let handler = DynamoDbHandler::new();
        let created = handler.handle(ddb(
            "streams-rt-1",
            "CreateTable",
            j!({
                "TableName": "streams-rt",
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "BillingMode": "PAY_PER_REQUEST",
                "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"},
            }),
        ));
        assert_eq!(created.status, 200, "{}", created.body);
        let out = handler.handle(ddb(
            "streams-rt-1",
            "PutItem",
            j!({"TableName": "streams-rt", "Item": {"pk": {"S": "a"}, "val": {"S": "v1"}}}),
        ));
        assert_eq!(out.status, 200, "{}", out.body);

        let listed = parsed(
            &dispatch(&streams_req(
                "streams-rt-1",
                "ListStreams",
                j!({"TableName": "streams-rt"}),
            ))
            .unwrap(),
        );
        assert!(!listed["Streams"].as_array().unwrap().is_empty());
        let arn = listed["Streams"][0]["StreamArn"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(arn.starts_with("arn:aws:dynamodb:streams-rt-1:123456789012:table/streams-rt"));

        let desc = parsed(
            &dispatch(&streams_req(
                "streams-rt-1",
                "DescribeStream",
                j!({"StreamArn": arn}),
            ))
            .unwrap(),
        )["StreamDescription"]
            .clone();
        assert_eq!(desc["TableName"], "streams-rt");
        assert_eq!(desc["StreamViewType"], "NEW_AND_OLD_IMAGES");
        assert_eq!(desc["StreamStatus"], "ENABLED");
        let shard_id = desc["Shards"][0]["ShardId"].as_str().unwrap().to_string();
        let starting = desc["Shards"][0]["SequenceNumberRange"]["StartingSequenceNumber"]
            .as_str()
            .unwrap()
            .to_string();

        // TRIM_HORIZON reads the put, INSERT with NewImage but no OldImage.
        let it = parsed(
            &dispatch(&streams_req(
                "streams-rt-1",
                "GetShardIterator",
                j!({"StreamArn": arn, "ShardId": shard_id, "ShardIteratorType": "TRIM_HORIZON"}),
            ))
            .unwrap(),
        )["ShardIterator"]
            .as_str()
            .unwrap()
            .to_string();
        let recs = parsed(
            &dispatch(&streams_req(
                "streams-rt-1",
                "GetRecords",
                j!({"ShardIterator": it}),
            ))
            .unwrap(),
        );
        let records = recs["Records"].as_array().unwrap();
        assert!(!records.is_empty(), "records={records:?}");
        assert_eq!(records[0]["eventName"], "INSERT");
        assert_eq!(records[0]["dynamodb"]["SequenceNumber"], starting.as_str());
        assert_eq!(records[0]["dynamodb"]["Keys"]["pk"]["S"], "a");
        assert_eq!(records[0]["dynamodb"]["NewImage"]["val"]["S"], "v1");
        assert!(recs["NextShardIterator"].is_string());
        let seq_num = records[0]["dynamodb"]["SequenceNumber"].as_str().unwrap();

        // AT_SEQUENCE_NUMBER is inclusive.
        let it_at = parsed(&dispatch(&streams_req(
            "streams-rt-1",
            "GetShardIterator",
            j!({"StreamArn": arn, "ShardId": shard_id, "ShardIteratorType": "AT_SEQUENCE_NUMBER", "SequenceNumber": seq_num}),
        )).unwrap())["ShardIterator"]
            .as_str()
            .unwrap()
            .to_string();
        let at = parsed(
            &dispatch(&streams_req(
                "streams-rt-1",
                "GetRecords",
                j!({"ShardIterator": it_at}),
            ))
            .unwrap(),
        );
        assert_eq!(at["Records"].as_array().unwrap().len(), 1);

        // MODIFY carries both images on update; REMOVE fires on delete.
        let _ = handler.handle(ddb(
            "streams-rt-1",
            "UpdateItem",
            j!({
                "TableName": "streams-rt",
                "Key": {"pk": {"S": "a"}},
                "UpdateExpression": "SET val = :v",
                "ExpressionAttributeValues": {":v": {"S": "v2"}},
            }),
        ));
        let _ = handler.handle(ddb(
            "streams-rt-1",
            "DeleteItem",
            j!({"TableName": "streams-rt", "Key": {"pk": {"S": "a"}}}),
        ));
        let it_after = parsed(&dispatch(&streams_req(
            "streams-rt-1",
            "GetShardIterator",
            j!({"StreamArn": arn, "ShardId": shard_id, "ShardIteratorType": "AFTER_SEQUENCE_NUMBER", "SequenceNumber": seq_num}),
        )).unwrap())["ShardIterator"]
            .as_str()
            .unwrap()
            .to_string();
        let recs2 = parsed(
            &dispatch(&streams_req(
                "streams-rt-1",
                "GetRecords",
                j!({"ShardIterator": it_after}),
            ))
            .unwrap(),
        );
        let arr2 = recs2["Records"].as_array().unwrap();
        let events: Vec<&str> = arr2
            .iter()
            .map(|r| r["eventName"].as_str().unwrap())
            .collect();
        assert_eq!(events, vec!["MODIFY", "REMOVE"], "events={events:?}");
        let modify = &arr2[0];
        assert!(modify["dynamodb"]["OldImage"].is_object());
        assert!(modify["dynamodb"]["NewImage"].is_object());
        let remove = &arr2[1];
        assert!(remove["dynamodb"]["OldImage"].is_object());
    }

    #[test]
    fn list_streams_honors_limit_and_table_filter() {
        let handler = DynamoDbHandler::new();
        for name in ["streams-a", "streams-b", "plain"] {
            let mut spec = j!({"StreamEnabled": true, "StreamViewType": "NEW_IMAGE"});
            if name == "plain" {
                spec = j!({"StreamEnabled": false});
            }
            let created = handler.handle(ddb(
                "streams-rt-2",
                "CreateTable",
                j!({
                    "TableName": name,
                    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                    "BillingMode": "PAY_PER_REQUEST",
                    "StreamSpecification": spec,
                }),
            ));
            assert_eq!(created.status, 200, "{}", created.body);
        }

        let listed = parsed(
            &dispatch(&streams_req(
                "streams-rt-2",
                "ListStreams",
                j!({"TableName": "streams-a"}),
            ))
            .unwrap(),
        );
        let names: Vec<&str> = listed["Streams"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["TableName"].as_str().unwrap())
            .collect();
        assert_eq!(names, vec!["streams-a"]);
        assert!(listed["Streams"][0]["StreamArn"]
            .as_str()
            .unwrap()
            .contains("/stream/"));

        let limited = parsed(
            &dispatch(&streams_req(
                "streams-rt-2",
                "ListStreams",
                j!({"Limit": 1}),
            ))
            .unwrap(),
        );
        assert!(limited["Streams"].as_array().unwrap().len() <= 1);
    }

    #[test]
    fn unknown_stream_and_shard_error() {
        let r = dispatch(&streams_req(
            "streams-rt-9",
            "DescribeStream",
            j!({"StreamArn": "arn:aws:dynamodb:streams-rt-9:123456789012:table/nope-rt/stream/l"}),
        ))
        .unwrap();
        assert_eq!(r.status, 400);
        let v = parsed(&r);
        assert_eq!(v["__type"], "ResourceNotFoundException");
    }
}
