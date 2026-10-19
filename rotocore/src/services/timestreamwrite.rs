//! Timestream Write backend (JSON protocol, target `Timestream_20181101`).
//!
//! Port of moto's timestreamwrite: databases, tables, records, endpoints
//! and tags, per (account, region). The router sends write ops here and
//! query ops to [`super::timestreamquery`].

use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Value};

#[derive(Default)]
struct State {
    databases: Vec<Database>,
    tags: Vec<(String, Value)>,
}

#[derive(Default, Clone)]
struct Database {
    name: String,
    arn: String,
    kms_key_id: Value,
    tables: Vec<Table>,
}

#[derive(Default, Clone)]
struct Table {
    name: String,
    arn: String,
    db: String,
    retention: Value,
    magnetic: Value,
    schema: Value,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("timestreamwrite"));
    SLOT.clone()
}

fn ok(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

fn not_found(kind: &str, name: &str) -> Response {
    let e = json!({
        "__type": "ResourceNotFoundException",
        "message": format!("The {kind} {name} does not exist."),
    });
    Response::json(400, serde_json::to_vec(&e).unwrap_or_default())
}

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "timestreamwrite" {
        return None;
    }
    let body = crate::proto::parse_json_body(&req.body);
    let slot = state();
    let mut st = slot.write();
    let b = st.entry((req.account, req.region.clone())).or_default();
    Some(handle(req, b, &body))
}

fn db_desc(db: &Database) -> Value {
    json!({
        "Arn": db.arn,
        "DatabaseName": db.name,
        "TableCount": db.tables.len(),
        "KmsKeyId": db.kms_key_id,
        "CreationTime": now_epoch(),
        "LastUpdatedTime": now_epoch(),
    })
}

fn table_desc(t: &Table) -> Value {
    let retention = if t.retention.is_null() {
        json!({
            "MemoryStoreRetentionPeriodInHours": 6,
            "MagneticStoreRetentionPeriodInDays": 73000,
        })
    } else {
        t.retention.clone()
    };
    json!({
        "Arn": t.arn,
        "TableName": t.name,
        "DatabaseName": t.db,
        "TableStatus": "ACTIVE",
        "RetentionProperties": retention,
        "MagneticStoreWriteProperties": t.magnetic,
        "Schema": t.schema,
    })
}

/// moto stores these timestamps as floats (moto's `unix_time`).
fn now_epoch() -> f64 {
    chrono::Utc::now().timestamp_millis() as f64 / 1000.0
}

fn db_arn(region: &str, account: u64, name: &str) -> String {
    format!("arn:aws:timestream:{region}:{account}:database/{name}")
}

fn table_arn(region: &str, account: u64, db: &str, name: &str) -> String {
    format!("arn:aws:timestream:{region}:{account}:database/{db}/table/{name}")
}

fn handle(req: &Request, st: &mut State, p: &Value) -> Response {
    let region = req.region.as_str();
    let account = req.account;
    match req.operation.as_str() {
        "CreateDatabase" => {
            let name = match p.get("DatabaseName").and_then(Value::as_str) {
                Some(n) => n.to_string(),
                None => return not_found("database", "?"),
            };
            let arn = db_arn(region, account, &name);
            let kms = p.get("KmsKeyId").cloned().unwrap_or(json!(format!(
                "arn:aws:kms:{region}:{account}:key/default_key"
            )));
            if st.databases.iter().any(|db| db.name == name) {
                return not_found("database", &name);
            }
            st.databases.push(Database {
                name: name.clone(),
                arn: arn.clone(),
                kms_key_id: kms,
                tables: vec![],
            });
            set_tags(st, &arn, "Tags", p);
            for db in &st.databases {
                if db.arn == arn {
                    return ok(&json!({"Database": db_desc(db)}));
                }
            }
            not_found("database", &name)
        }
        "DeleteDatabase" => {
            let name = p.get("DatabaseName").and_then(Value::as_str).unwrap_or("");
            st.databases.retain(|db| db.name != name);
            let table_arn_prefix = format!("database/{name}");
            st.tags.retain(|(a, _)| !a.contains(&table_arn_prefix));
            ok(&json!({}))
        }
        "DescribeDatabase" => {
            let name = p.get("DatabaseName").and_then(Value::as_str).unwrap_or("");
            match st.databases.iter().find(|db| db.name == name) {
                Some(db) => ok(&json!({"Database": db_desc(db)})),
                None => not_found("database", name),
            }
        }
        "UpdateDatabase" => {
            let name = p.get("DatabaseName").and_then(Value::as_str).unwrap_or("");
            for db in st.databases.iter_mut() {
                if db.name == name {
                    if let Some(kms) = p.get("KmsKeyId") {
                        db.kms_key_id = kms.clone();
                    }
                    return ok(&json!({"Database": db_desc(db)}));
                }
            }
            not_found("database", name)
        }
        "ListDatabases" => {
            let out: Vec<Value> = st.databases.iter().map(db_desc).collect();
            ok(&json!({"Databases": out}))
        }
        "CreateTable" => {
            let dbname = p.get("DatabaseName").and_then(Value::as_str).unwrap_or("");
            let tname = p.get("TableName").and_then(Value::as_str).unwrap_or("");
            if !st.databases.iter().any(|db| db.name == dbname) {
                return not_found("database", dbname);
            }
            let arn = table_arn(region, account, dbname, tname);
            let schema = match p.get("Schema") {
                Some(s) => s.clone(),
                None => json!({"CompositePartitionKey": []}),
            };
            let table = Table {
                name: tname.to_string(),
                arn: arn.clone(),
                db: dbname.to_string(),
                retention: p.get("RetentionProperties").cloned().unwrap_or(Value::Null),
                magnetic: p
                    .get("MagneticStoreWriteProperties")
                    .cloned()
                    .unwrap_or(Value::Null),
                schema,
            };
            for db in st.databases.iter_mut() {
                if db.name == dbname {
                    db.tables.push(table.clone());
                }
            }
            set_tags(st, &arn, "Tags", p);
            ok(&json!({"Table": table_desc(&table)}))
        }
        "DeleteTable" => {
            let dbname = p.get("DatabaseName").and_then(Value::as_str).unwrap_or("");
            let tname = p.get("TableName").and_then(Value::as_str).unwrap_or("");
            let Some(db) = st.databases.iter_mut().find(|db| db.name == dbname) else {
                return not_found("database", dbname);
            };
            db.tables.retain(|t| t.name != tname);
            ok(&json!({}))
        }
        "DescribeTable" => {
            let dbname = p.get("DatabaseName").and_then(Value::as_str).unwrap_or("");
            let tname = p.get("TableName").and_then(Value::as_str).unwrap_or("");
            match st.databases.iter().find(|db| db.name == dbname) {
                Some(db) => match db.tables.iter().find(|t| t.name == tname) {
                    Some(t) => ok(&json!({"Table": table_desc(t)})),
                    None => not_found("table", tname),
                },
                None => not_found("database", dbname),
            }
        }
        "ListTables" => {
            let mut out = Vec::new();
            match p.get("DatabaseName").and_then(Value::as_str) {
                Some(dbname) => {
                    let Some(db) = st.databases.iter().find(|db| db.name == dbname) else {
                        return not_found("database", dbname);
                    };
                    out.extend(db.tables.iter().map(table_desc));
                }
                None => {
                    for db in &st.databases {
                        out.extend(db.tables.iter().map(table_desc));
                    }
                }
            }
            ok(&json!({"Tables": out}))
        }
        "UpdateTable" => {
            let dbname = p.get("DatabaseName").and_then(Value::as_str).unwrap_or("");
            let tname = p.get("TableName").and_then(Value::as_str).unwrap_or("");
            let Some(db) = st.databases.iter_mut().find(|db| db.name == dbname) else {
                return not_found("database", dbname);
            };
            let Some(tidx) = db.tables.iter().position(|t| t.name == tname) else {
                return not_found("table", tname);
            };
            if let Some(r) = p.get("RetentionProperties") {
                db.tables[tidx].retention = r.clone();
            }
            if let Some(m) = p.get("MagneticStoreWriteProperties") {
                db.tables[tidx].magnetic = m.clone();
            }
            if let Some(s) = p.get("Schema") {
                db.tables[tidx].schema = s.clone();
            }
            ok(&json!({"Table": table_desc(&db.tables[tidx].clone())}))
        }
        "WriteRecords" => {
            let dbname = p.get("DatabaseName").and_then(Value::as_str).unwrap_or("");
            let _tname = p.get("TableName").and_then(Value::as_str).unwrap_or("");
            if !st.databases.iter().any(|db| db.name == dbname) {
                return not_found("database", dbname);
            }
            let rec_count = p
                .get("Records")
                .and_then(Value::as_array)
                .map(|a| a.len())
                .unwrap_or(0);
            ok(&json!({
                "RecordsIngested": {
                    "Total": rec_count,
                    "MemoryStore": rec_count,
                    "MagneticStore": 0,
                }
            }))
        }
        "DescribeEndpoints" => {
            let e = json!({
                "Endpoints": [{
                    "Address": format!("ingest.timestream.{region}.amazonaws.com"),
                    "CachePeriodInMinutes": 1440,
                }]
            });
            ok(&e)
        }
        "ListTagsForResource" => {
            let arn = p.get("ResourceARN").and_then(Value::as_str).unwrap_or("");
            let tags = tag_map(st, arn);
            let list: Vec<Value> = tags
                .as_object()
                .map(|m| {
                    m.iter()
                        .map(|(k, v)| json!({"Key": k, "Value": v}))
                        .collect()
                })
                .unwrap_or_default();
            ok(&json!({"Tags": list}))
        }
        "TagResource" => {
            let arn = p.get("ResourceARN").and_then(Value::as_str).unwrap_or("");
            set_tags(st, arn, "Tags", p);
            ok(&json!({}))
        }
        "UntagResource" => {
            let arn = p.get("ResourceARN").and_then(Value::as_str).unwrap_or("");
            let keys: Vec<String> = p
                .get("TagKeys")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .map(String::from)
                        .collect()
                })
                .unwrap_or_default();
            for (a, v) in st.tags.iter_mut() {
                if a == arn {
                    if let Some(obj) = v.as_object_mut() {
                        for k in &keys {
                            obj.remove(k);
                        }
                    }
                }
            }
            ok(&json!({}))
        }
        _ => {
            let e = json!({
                "__type": "NotImplemented",
                "message": format!("The {} action has not been implemented", req.operation),
            });
            Response::json(501, serde_json::to_vec(&e).unwrap_or_default())
        }
    }
}

fn tag_map(st: &mut State, arn: &str) -> Value {
    st.tags
        .iter()
        .find(|(a, _)| a == arn)
        .map(|(_, v)| v.clone())
        .unwrap_or(json!({}))
}

/// Merge tags from `p[tags_field]` (list of {Key, Value}) into the tag map
/// moto maintains for `arn`.
fn set_tags(st: &mut State, arn: &str, tags_field: &str, p: &Value) {
    let incoming = p.get(tags_field).and_then(Value::as_array).cloned();
    let Some(incoming) = incoming else { return };
    let entry = match st.tags.iter_mut().find(|(a, _)| a == arn) {
        Some((_, v)) => v,
        None => {
            st.tags.push((arn.to_string(), json!({})));
            let i = st.tags.len() - 1;
            &mut st.tags[i].1
        }
    };
    let Some(obj) = entry.as_object_mut() else {
        return;
    };
    for tag in incoming {
        let k = tag.get("Key").and_then(Value::as_str).unwrap_or("");
        if k.is_empty() {
            continue;
        }
        obj.insert(
            k.to_string(),
            tag.get("Value").cloned().unwrap_or(Value::Null),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn req(op: &str) -> Request {
        Request {
            service: "timestreamwrite".into(),
            operation: op.into(),
            method: "POST".into(),
            path: "/".into(),
            query_string: String::new(),
            headers: Default::default(),
            body: bytes::Bytes::new(),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    #[test]
    fn database_lifecycle() {
        let slot = state();
        let mut st = slot.write();
        let b = st.entry((123456789012, "us-east-1".into())).or_default();

        let created = handle(
            &req("CreateDatabase"),
            b,
            &json!({"DatabaseName": "db-flow"}),
        );
        let created = serde_json::from_slice::<Value>(&created.body).unwrap();
        assert_eq!(created["Database"]["DatabaseName"], "db-flow");
        assert!(created["Database"]["Arn"]
            .as_str()
            .unwrap()
            .starts_with("arn:aws:timestream:"));

        let listed = handle(&req("ListDatabases"), b, &json!({}));
        assert_eq!(listed.status, 200);
        let listed = serde_json::from_slice::<Value>(&listed.body).unwrap();
        assert_eq!(listed["Databases"].as_array().unwrap().len(), 1);

        handle(
            &req("DeleteDatabase"),
            b,
            &json!({"DatabaseName": "db-flow"}),
        );
        let after = handle(&req("ListDatabases"), b, &json!({}));
        let after = serde_json::from_slice::<Value>(&after.body).unwrap();
        assert!(after["Databases"].as_array().unwrap().is_empty());
    }

    #[test]
    fn table_roundtrip_and_endpoints() {
        let slot = state();
        let mut st = slot.write();
        let b = st.entry((123456789012, "us-east-1".into())).or_default();
        handle(&req("CreateDatabase"), b, &json!({"DatabaseName": "db"}));
        let t = handle(
            &req("CreateTable"),
            b,
            &json!({"DatabaseName": "db", "TableName": "tbl"}),
        );
        let t = serde_json::from_slice::<Value>(&t.body).unwrap();
        assert_eq!(t["Table"]["TableStatus"], "ACTIVE");

        let eps = handle(&req("DescribeEndpoints"), b, &json!({}));
        let eps = serde_json::from_slice::<Value>(&eps.body).unwrap();
        assert_eq!(
            eps["Endpoints"][0]["Address"],
            "ingest.timestream.us-east-1.amazonaws.com"
        );
    }
}
