// Instance-family handler arms included textually into `connect.rs` (the
// elbv2/polly `include!` idiom). Every op in this group is moto-absent, so
// the botocore wire model is the contract: 2xx happy paths with all
// required output members synthesized, plus the module's modeled 4xx paths
// on the same fills.

// -- Data table values (create|update|delete|describe|evaluate|list) -------

fn default_lock_version() -> Map<String, Value> {
    let mut lock = Map::new();
    lock.insert("DataTable".into(), json!("1"));
    lock.insert("Attribute".into(), json!("1"));
    lock.insert("PrimaryValues".into(), json!("1"));
    lock.insert("Value".into(), json!("1"));
    lock
}

/// Stable string key for a record's primary-values list.
fn row_key(primary: &[Value]) -> String {
    fmt_py(&Value::Array(primary.to_vec()))
}

/// `opt_string` against a raw Value (probe items are bare members).
fn v_opt_string(item: &Value, key: &str) -> Option<String> {
    item.as_object().and_then(|m| opt_string(m, key))
}

/// `opt_val` against a raw Value.
fn v_opt_val(item: &Value, key: &str) -> Option<Value> {
    item.as_object().and_then(|m| m.get(key)).cloned()
}

fn bump_lock(lock: &mut Map<String, Value>, field: &str) {
    let cur = lock
        .get(field)
        .and_then(Value::as_str)
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0);
    lock.insert(field.to_string(), json!((cur + 1).to_string()));
}

fn dt_region(st: &ConnectState, instance_id: &str) -> String {
    st.instances.get(instance_id).map(region_of).unwrap_or_default()
}

fn dt_table_guard(st: &ConnectState, instance_id: &str, table_id: &str) -> Result<(), Response> {
    if !st.instances.contains_key(instance_id) {
        return Err(notfound_instance(instance_id));
    }
    if st
        .data_tables
        .get(instance_id)
        .and_then(|m| m.get(table_id))
        .is_none()
    {
        return Err(not_found(format!("Data table {table_id} not found")));
    }
    Ok(())
}

/// The per (instance, table) bundle, which carries the table LockVersion.
fn dt_bundle<'a>(st: &'a mut ConnectState, instance_id: &str, table_id: &str) -> &'a mut Map<String, Value> {
    st.data_table_values
        .entry(instance_id.to_string())
        .or_default()
        .entry(table_id.to_string())
        .or_default()
}

/// The per (instance, table) stored value records.
fn dt_rows<'a>(st: &'a mut ConnectState, instance_id: &'a str, table_id: &'a str) -> &'a mut Vec<Value> {
    dt_bundle(st, instance_id, table_id)
        .entry("Values".to_string())
        .or_insert_with(|| json!([]))
        .as_array_mut()
        .unwrap()
}

/// Primary values of a stored data-table value record.
fn record_primary(record: &Value) -> Vec<Value> {
    record
        .get("PrimaryValues")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

fn op_batch_data_table_values(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
    table_id: &str,
    mode: &str,
) -> Response {
    if let Err(resp) = dt_table_guard(st, instance_id, table_id) {
        return resp;
    }
    let items = match opt_val(body, "Values") {
        Some(Value::Array(v)) => v.clone(),
        Some(_) => return invalid("Values must be a list"),
        None => return missing_body_param("Values"),
    };
    let region = dt_region(st, instance_id);
    let mut successful: Vec<Value> = Vec::new();
    let mut failed: Vec<Value> = Vec::new();
    for item in items {
        let primary = item.get("PrimaryValues").cloned().unwrap_or_else(|| json!([]));
        let attribute_name = match v_opt_string(&item, "AttributeName") {
            Some(v) if !v.is_empty() => v,
            _ => {
                failed.push(json!({
                    "PrimaryValues": primary,
                    "AttributeName": "",
                    "Message": "AttributeName is a required parameter",
                }));
                continue;
            }
        };
        let key = row_key(&record_primary(&json!({ "PrimaryValues": primary.clone() })));
        let find = |st: &mut ConnectState| -> Option<usize> {
            dt_rows(st, instance_id, table_id)
                .iter()
                .position(|r| {
                    r.get("AttributeName") == Some(&json!(attribute_name))
                        && r.get("_row_key") == Some(&json!(key))
                })
        };
        match mode {
            "create" => {
                if find(st).is_some() {
                    failed.push(json!({
                        "PrimaryValues": primary,
                        "AttributeName": attribute_name,
                        "Message": format!(
                            "Data table value with the same primary values already exists for attribute {attribute_name}"
                        ),
                    }));
                    continue;
                }
                let record_id = uuid::Uuid::new_v4().to_string();
                let mut lock = dt_bundle(st, instance_id, table_id)
                    .entry("LockVersion".to_string())
                    .or_insert_with(|| json!(default_lock_version()))
                    .as_object()
                    .cloned()
                    .unwrap_or_else(default_lock_version);
                bump_lock(&mut lock, "Value");
                let record = json!({
                    "_row_key": key,
                    "RecordId": record_id,
                    "AttributeId": uuid::Uuid::new_v4().to_string(),
                    "AttributeName": attribute_name.clone(),
                    "PrimaryValues": primary.clone(),
                    "ValueType": v_opt_val(&item, "ValueType").unwrap_or(json!("TEXT")),
                    "Value": v_opt_val(&item, "Value").unwrap_or(json!("")),
                    "LockVersion": lock.clone(),
                    "LastModifiedTime": now_iso(),
                    "LastModifiedRegion": region.clone(),
                });
                dt_bundle(st, instance_id, table_id)
                    .insert("LockVersion".into(), json!(lock.clone()));
                dt_rows(st, instance_id, table_id).push(record.clone());
                successful.push(json!({
                    "PrimaryValues": primary,
                    "AttributeName": attribute_name,
                    "RecordId": record["RecordId"].clone(),
                    "LockVersion": lock,
                }));
            }
            "update" => {
                let Some(index) = find(st) else {
                    failed.push(json!({
                        "PrimaryValues": primary,
                        "AttributeName": attribute_name,
                        "Message": format!("Data table value not found for attribute {attribute_name}"),
                    }));
                    continue;
                };
                let mut lock = dt_bundle(st, instance_id, table_id)
                    .entry("LockVersion".to_string())
                    .or_insert_with(|| json!(default_lock_version()))
                    .as_object()
                    .cloned()
                    .unwrap_or_else(default_lock_version);
                bump_lock(&mut lock, "Value");
                let record = {
                    let rows = dt_rows(st, instance_id, table_id);
                    let record = &mut rows[index];
                    if let Some(v) = v_opt_val(&item, "Value") {
                        record["Value"] = v;
                    }
                    record["LockVersion"] = json!(lock.clone());
                    record["LastModifiedTime"] = json!(now_iso());
                    record["LastModifiedRegion"] = json!(region.clone());
                    record.clone()
                };
                dt_bundle(st, instance_id, table_id)
                    .insert("LockVersion".into(), json!(lock.clone()));
                successful.push(json!({
                    "PrimaryValues": record["PrimaryValues"].clone(),
                    "AttributeName": record["AttributeName"].clone(),
                    "LockVersion": lock,
                }));
            }
            "delete" => {
                let Some(index) = find(st) else {
                    failed.push(json!({
                        "PrimaryValues": primary,
                        "AttributeName": attribute_name,
                        "Message": format!("Data table value not found for attribute {attribute_name}"),
                    }));
                    continue;
                };
                let record = dt_rows(st, instance_id, table_id).remove(index);
                successful.push(json!({
                    "PrimaryValues": record["PrimaryValues"].clone(),
                    "AttributeName": record["AttributeName"].clone(),
                    "LockVersion": record["LockVersion"].clone(),
                }));
            }
            "describe" => {
                let found = dt_rows(st, instance_id, table_id)
                    .iter()
                    .find(|r| {
                        r.get("AttributeName") == Some(&json!(attribute_name))
                            && r.get("_row_key") == Some(&json!(key))
                    })
                    .cloned();
                match found {
                    Some(record) => successful.push(json!({
                        "RecordId": record["RecordId"].clone(),
                        "AttributeId": record["AttributeId"].clone(),
                        "PrimaryValues": record["PrimaryValues"].clone(),
                        "AttributeName": record["AttributeName"].clone(),
                        "Value": record["Value"].clone(),
                        "LockVersion": record["LockVersion"].clone(),
                        "LastModifiedTime": record["LastModifiedTime"].clone(),
                        "LastModifiedRegion": record["LastModifiedRegion"].clone(),
                    })),
                    None => failed.push(json!({
                        "PrimaryValues": primary,
                        "AttributeName": attribute_name,
                        "Message": format!("Data table value not found for attribute {attribute_name}"),
                    })),
                }
            }
            _ => {
                let _ = (primary, attribute_name, key);
                unreachable!()
            }
        }
    }
    ok(json!({"Successful": successful, "Failed": failed}))
}

fn op_list_data_table_values(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
    table_id: &str,
    mode: &str,
) -> Response {
    if let Err(resp) = dt_table_guard(st, instance_id, table_id) {
        return resp;
    }
    let record_ids = opt_val(body, "RecordIds");
    let attribute_values = opt_val(body, "PrimaryAttributeValues");
    let mut rows: Vec<Value> = dt_rows(st, instance_id, table_id)
        .iter()
        .filter(|r| {
            if mode != "values" {
                return true;
            }
            let by_record = match &record_ids {
                Some(Value::Array(ids)) => ids.contains(&r["RecordId"]),
                _ => true,
            };
            let by_primary = match &attribute_values {
                Some(Value::Array(filters)) => filters.iter().all(|f| {
                    let Some(value) = f.get("Value") else {
                        return true;
                    };
                    record_primary(r).iter().any(|pv| pv.get("Value") == Some(value))
                }),
                _ => true,
            };
            by_record && by_primary
        })
        .map(|r| match mode {
            "values" => dt_summary(r),
            _ => json!({
                "RecordId": r["RecordId"],
                "PrimaryValues": r["PrimaryValues"],
                "LastModifiedTime": r["LastModifiedTime"],
                "LastModifiedRegion": r["LastModifiedRegion"],
            }),
        })
        .collect();
    rows.sort_by_key(|r| fmt_py(&r["RecordId"]));
    if mode == "primary" {
        let mut seen = std::collections::HashSet::new();
        rows.retain(|r| seen.insert(fmt_py(&r["RecordId"])));
        return ok(json!({"PrimaryValuesList": rows}));
    }
    ok(json!({"Values": rows}))
}

fn dt_summary(record: &Value) -> Value {
    json!({
        "RecordId": record["RecordId"],
        "AttributeId": record["AttributeId"],
        "PrimaryValues": record["PrimaryValues"],
        "AttributeName": record["AttributeName"],
        "ValueType": record["ValueType"],
        "Value": record["Value"],
        "LockVersion": record["LockVersion"],
        "LastModifiedTime": record["LastModifiedTime"],
        "LastModifiedRegion": record["LastModifiedRegion"],
    })
}

fn op_evaluate_data_table_values(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
    table_id: &str,
) -> Response {
    if let Err(resp) = dt_table_guard(st, instance_id, table_id) {
        return resp;
    }
    let evaluations = match opt_val(body, "Values") {
        Some(Value::Array(v)) => v.clone(),
        Some(_) => return invalid("Values must be a list"),
        None => return missing_body_param("Values"),
    };
    let mut out: Vec<Value> = Vec::new();
    for item in &evaluations {
        let primary = item.get("PrimaryValues").cloned().unwrap_or_else(|| json!([]));
        let attribute_name = v_opt_string(item, "AttributeName").unwrap_or_default();
        let key = row_key(&record_primary(&json!({ "PrimaryValues": primary.clone() })));
        let found_record = dt_rows(st, instance_id, table_id)
            .iter()
            .find(|r| {
                r.get("AttributeName") == Some(&json!(attribute_name))
                    && r.get("_row_key") == Some(&json!(key))
            })
            .cloned();
        out.push(json!({
            "RecordId": match &found_record {
                Some(record) => record["RecordId"].clone(),
                None => json!(""),
            },
            "PrimaryValues": primary,
            "AttributeName": attribute_name,
            "ValueType": match &found_record {
                Some(record) => record["ValueType"].clone(),
                None => json!("TEXT"),
            },
            "Found": found_record.is_some(),
            "Error": false,
            "EvaluatedValue": match &found_record {
                Some(record) => record["Value"].clone(),
                None => json!(""),
            },
        }));
    }
    ok(json!({"Values": out}))
}

fn op_update_data_table_metadata(
    st: &mut ConnectState,
    instance_id: &str,
    table_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if let Err(resp) = dt_table_guard(st, instance_id, table_id) {
        return resp;
    }
    let name = match body_str(body, "Name") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let _ = body_str(body, "ValueLockLevel");
    let _ = body_str(body, "TimeZone");
    {
        let table = st
            .data_tables
            .get_mut(instance_id)
            .and_then(|m| m.get_mut(table_id))
            .unwrap();
        table["Name"] = json!(name);
        table["ModifiedTimestamp"] = json!(now_iso());
    }
    let mut lock = dt_bundle(st, instance_id, table_id)
        .entry("LockVersion".to_string())
        .or_insert_with(|| json!(default_lock_version()))
        .as_object()
        .cloned()
        .unwrap_or_else(default_lock_version);
    bump_lock(&mut lock, "DataTable");
    dt_bundle(st, instance_id, table_id).insert("LockVersion".into(), json!(lock.clone()));
    ok(json!({"LockVersion": lock}))
}

fn op_update_data_table_primary_values(
    st: &mut ConnectState,
    instance_id: &str,
    table_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if let Err(resp) = dt_table_guard(st, instance_id, table_id) {
        return resp;
    }
    let primary = match opt_val(body, "PrimaryValues") {
        Some(Value::Array(v)) => v.clone(),
        _ => return missing_body_param("PrimaryValues"),
    };
    let new_primary = match opt_val(body, "NewPrimaryValues") {
        Some(Value::Array(v)) => v.clone(),
        _ => return missing_body_param("NewPrimaryValues"),
    };
    let _ = opt_val(body, "LockVersion");
    let old_key = row_key(&primary);
    let new_key = row_key(&new_primary);
    for record in dt_rows(st, instance_id, table_id).iter_mut() {
        if record["_row_key"] == json!(old_key) {
            record["_row_key"] = json!(new_key.clone());
            record["PrimaryValues"] = json!(new_primary.clone());
        }
    }
    let mut lock = dt_bundle(st, instance_id, table_id)
        .entry("LockVersion".to_string())
        .or_insert_with(|| json!(default_lock_version()))
        .as_object()
        .cloned()
        .unwrap_or_else(default_lock_version);
    bump_lock(&mut lock, "PrimaryValues");
    dt_bundle(st, instance_id, table_id).insert("LockVersion".into(), json!(lock.clone()));
    ok(json!({"LockVersion": lock}))
}

// -- Analytics data set batches ---------------------------------------------

fn op_batch_associate_analytics_data_set(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
    associate: bool,
) -> Response {
    let instance_id = instance_id.to_string();

    if !st.instances.contains_key(&instance_id) {
        return notfound_instance(&instance_id);
    }
    let data_set_ids = match opt_val(body, "DataSetIds") {
        Some(Value::Array(v)) => v.clone(),
        _ => return missing_body_param("DataSetIds"),
    };
    let target_account = opt_string(body, "TargetAccountId").unwrap_or_default();
    let instance_region = st
        .instances
        .get(&instance_id)
        .map(region_of)
        .unwrap_or_default();
    let instance_account = st
        .instances
        .get(&instance_id)
        .map(|i| str_field(i, "Arn"))
        .unwrap_or_default()
        .split(':')
        .nth(4)
        .unwrap_or("")
        .to_string();
    let mut created: Vec<Value> = Vec::new();
    let mut deleted: Vec<Value> = Vec::new();
    let mut errors: Vec<Value> = Vec::new();
    for data_set_id in data_set_ids {
        let Some(data_set_id) = data_set_id.as_str().map(str::to_string) else {
            continue;
        };
        let existing = st
            .analytics_data_associations
            .get(&instance_id)
            .is_some_and(|m| m.contains_key(&data_set_id));
        if associate && existing {
            errors.push(json!({
                "Id": data_set_id,
                "ErrorCode": "ResourceConflictException",
                "ErrorMessage": format!(
                    "Analytics data association for data set {data_set_id} already exists for instance {instance_id}"
                ),
            }));
            continue;
        }
        if !associate && !existing {
            errors.push(json!({
                "Id": data_set_id,
                "ErrorCode": "ResourceNotFoundException",
                "ErrorMessage": format!(
                    "Analytics data association for data set {data_set_id} not found for instance {instance_id}"
                ),
            }));
            continue;
        }
        if associate {
            let resource_share_id = uuid::Uuid::new_v4().to_string();
            let assoc = json!({
                "DataSetId": data_set_id,
                "TargetAccountId": target_account,
                "ResourceShareId": resource_share_id,
                "ResourceShareArn": format!(
                    "arn:aws:ram:{instance_region}:{instance_account}:resource-share/{resource_share_id}"
                ),
                "ResourceShareStatus": "ACTIVE",
            });
            st.analytics_data_associations
                .entry(instance_id.to_string())
                .or_default()
                .insert(data_set_id.clone(), assoc.clone());
            created.push(assoc);
        } else {
            if let Some(map) = st.analytics_data_associations.get_mut(&instance_id) {
                map.remove(&data_set_id);
            }
            deleted.push(json!(data_set_id));
        }
    }
    if associate {
        ok(json!({"Created": created, "Errors": errors}))
    } else {
        ok(json!({"Deleted": deleted, "Errors": errors}))
    }
}

fn op_list_analytics_data_lake_data_sets(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    ok(json!({"Results": []}))
}

// -- Attached files ---------------------------------------------------------

fn op_start_attached_file_upload(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    let Some(instance) = st.instances.get(instance_id) else {
        return notfound_instance(instance_id);
    };
    let file_name = match body_str(body, "FileName") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let file_size = body.get("FileSizeInBytes").and_then(Value::as_i64).unwrap_or(0);
    let created_by = opt_val(body, "CreatedBy").filter(|v| !v.is_null());
    let tags = opt_val(body, "Tags").filter(|v| v.as_object().is_some_and(|m| !m.is_empty())).clone();
    let instance_arn = str_field(instance, "Arn");
    let file_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{instance_arn}/attached-file/{file_id}");
    let stamp = now_iso();
    let expiry_seconds = body
        .get("UrlExpiryInSeconds")
        .and_then(Value::as_i64)
        .unwrap_or(300);
    let url_expiry = (chrono::Utc::now() + chrono::Duration::seconds(expiry_seconds))
        .to_rfc3339_opts(chrono::SecondsFormat::Micros, false);
    let mut stored = Map::new();
    stored.insert("FileArn".into(), json!(arn.clone()));
    stored.insert("FileId".into(), json!(file_id.clone()));
    stored.insert("FileName".into(), json!(file_name));
    stored.insert("FileSizeInBytes".into(), json!(file_size));
    stored.insert("FileStatus".into(), json!("PROCESSING"));
    stored.insert(
        "FileUseCaseType".into(),
        opt_val(body, "FileUseCaseType").unwrap_or(json!("ATTACHMENT")),
    );
    stored.insert(
        "AssociatedResourceArn".into(),
        json!(opt_string(body, "AssociatedResourceArn").unwrap_or_default()),
    );
    stored.insert("CreationTime".into(), json!(stamp));
    stored.insert("CreatedBy".into(), created_by.unwrap_or(json!({})));
    stored.insert("Tags".into(), tags.clone().unwrap_or_else(|| json!({})));
    st.attached_files
        .entry(instance_id.to_string())
        .or_default()
        .insert(file_id.clone(), Value::Object(stored.clone()));
    if let Some(Value::Object(map)) = &tags {
        st.tags.entry(arn.clone()).or_default().extend(map.clone());
    }
    ok(json!({
        "FileArn": arn,
        "FileId": file_id,
        "CreationTime": stamp,
        "FileStatus": "PROCESSING",
        "CreatedBy": stored["CreatedBy"].clone(),
        "UploadUrlMetadata": {
            "Url": format!("{instance_arn}/attached-file/{file_id}/upload"),
            "UrlExpiry": url_expiry,
            "HeadersToInclude": {},
        },
    }))
}

fn op_complete_attached_file_upload(
    st: &mut ConnectState,
    instance_id: &str,
    file_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let record = match st
        .attached_files
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(file_id))
    {
        Some(f) => f,
        None => return not_found(format!("Attached file {file_id} not found")),
    };
    record["FileStatus"] = json!("APPROVED");
    ok(json!({}))
}

fn op_get_attached_file(st: &mut ConnectState, instance_id: &str, file_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let record = match st
        .attached_files
        .get(instance_id)
        .and_then(|m| m.get(file_id))
    {
        Some(f) => f.clone(),
        None => return not_found(format!("Attached file {file_id} not found")),
    };
    let mut out = Map::new();
    for field in [
        "FileArn",
        "FileId",
        "CreationTime",
        "FileStatus",
        "FileName",
        "FileSizeInBytes",
        "AssociatedResourceArn",
        "FileUseCaseType",
    ] {
        out.insert(field.to_string(), record_field(&record, field));
    }
    out.insert(
        "CreatedBy".into(),
        record.get("CreatedBy").cloned().unwrap_or(json!({})),
    );
    out.insert(
        "DownloadUrlMetadata".into(),
        json!({"Url": "", "UrlExpiry": now_iso()}),
    );
    ok(Value::Object(out))
}

fn op_delete_attached_file(st: &mut ConnectState, instance_id: &str, file_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .attached_files
        .get_mut(instance_id)
        .and_then(|m| m.remove(file_id))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!("Attached file {file_id} not found")),
    }
}

fn op_batch_get_attached_file_metadata(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let file_ids = match opt_val(body, "FileIds") {
        Some(Value::Array(v)) => v.clone(),
        _ => return missing_body_param("FileIds"),
    };
    let mut files: Vec<Value> = Vec::new();
    let mut errors: Vec<Value> = Vec::new();
    for file_id in file_ids {
        let Some(file_id) = file_id.as_str().map(str::to_string) else {
            continue;
        };
        match st
            .attached_files
            .get(instance_id)
            .and_then(|m| m.get(&file_id))
        {
            Some(record) => files.push(record.clone()),
            None => errors.push(json!({
                "FileId": file_id,
                "ErrorCode": "ResourceNotFoundException",
                "ErrorMessage": format!("Attached file {file_id} not found"),
            })),
        }
    }
    ok(json!({"Files": files, "Errors": errors}))
}

fn record_field(record: &Value, field: &str) -> Value {
    record.get(field).cloned().unwrap_or(Value::Null)
}

// -- Workspace pages / media / associations ---------------------------------

fn workspace_guard(st: &ConnectState, instance_id: &str, workspace_id: &str) -> Result<(), Response> {
    if !st.instances.contains_key(instance_id) {
        return Err(notfound_instance(instance_id));
    }
    if st
        .workspaces
        .get(instance_id)
        .and_then(|m| m.get(workspace_id))
        .is_none()
    {
        return Err(not_found(format!("Workspace {workspace_id} not found")));
    }
    Ok(())
}

fn op_create_workspace_page(
    st: &mut ConnectState,
    instance_id: &str,
    workspace_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if let Err(resp) = workspace_guard(st, instance_id, workspace_id) {
        return resp;
    }
    let page = match body_str(body, "Page") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let stored = json!({
        "ResourceArn": opt_val(body, "ResourceArn").unwrap_or(json!("")),
        "Page": page,
        "Slug": opt_val(body, "Slug").unwrap_or(json!("")),
        "InputData": opt_val(body, "InputData").unwrap_or(json!("")),
    });
    st.workspace_pages
        .entry(instance_id.to_string())
        .or_default()
        .entry(workspace_id.to_string())
        .or_default()
        .insert(page.clone(), stored);
    ok(json!({}))
}

fn op_update_workspace_page(
    st: &mut ConnectState,
    instance_id: &str,
    workspace_id: &str,
    page: &str,
    body: &Map<String, Value>,
) -> Response {
    if let Err(resp) = workspace_guard(st, instance_id, workspace_id) {
        return resp;
    }
    let Some(record) = st
        .workspace_pages
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(workspace_id))
        .and_then(|m| m.get_mut(page))
    else {
        return not_found(format!("Workspace page {page} not found"));
    };
    if let Some(slug) = opt_val(body, "Slug") {
        record["Slug"] = slug;
    }
    if let Some(data) = opt_val(body, "InputData") {
        record["InputData"] = data;
    }
    if let Some(arn) = opt_val(body, "ResourceArn") {
        record["ResourceArn"] = arn;
    }
    ok(json!({}))
}

fn op_delete_workspace_page(
    st: &mut ConnectState,
    instance_id: &str,
    workspace_id: &str,
    page: &str,
) -> Response {
    if let Err(resp) = workspace_guard(st, instance_id, workspace_id) {
        return resp;
    }
    match st
        .workspace_pages
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(workspace_id))
        .and_then(|m| m.remove(page))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!("Workspace page {page} not found")),
    }
}

fn op_list_workspace_pages(st: &mut ConnectState, instance_id: &str, workspace_id: &str) -> Response {
    if let Err(resp) = workspace_guard(st, instance_id, workspace_id) {
        return resp;
    }
    let mut rows: Vec<Value> = st
        .workspace_pages
        .get(instance_id)
        .and_then(|m| m.get(workspace_id))
        .map(|m| m.values().cloned().collect())
        .unwrap_or_default();
    rows.sort_by_key(|r| fmt_py(&r["Page"]));
    ok(json!({"WorkspacePageList": rows}))
}

fn op_import_workspace_media(
    st: &mut ConnectState,
    instance_id: &str,
    workspace_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if let Err(resp) = workspace_guard(st, instance_id, workspace_id) {
        return resp;
    }
    let media_type = match body_str(body, "MediaType") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let media_source = match body_str(body, "MediaSource") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    st.workspace_media
        .entry(instance_id.to_string())
        .or_default()
        .entry(workspace_id.to_string())
        .or_default()
        .push(json!({"Type": media_type, "Source": media_source}));
    ok(json!({}))
}

fn op_delete_workspace_media(
    st: &mut ConnectState,
    instance_id: &str,
    workspace_id: &str,
    query: &BTreeMap<String, Vec<String>>,
) -> Response {
    if let Err(resp) = workspace_guard(st, instance_id, workspace_id) {
        return resp;
    }
    let media_type = opt_string(&body_of_query(query), "mediaType").unwrap_or_default();
    if let Some(list) = st
        .workspace_media
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(workspace_id))
    {
        list.retain(|m| {
            media_type.is_empty() || m.get("Type") != Some(&json!(media_type))
        });
    }
    ok(json!({}))
}

fn body_of_query(query: &BTreeMap<String, Vec<String>>) -> Map<String, Value> {
    // DELETE ops carry their members in the query string; build a body view.
    let mut map = Map::new();
    for (key, values) in query {
        map.insert(key.clone(), json!(values.first().cloned().unwrap_or_default()));
    }
    map
}

fn op_list_workspace_media(st: &mut ConnectState, instance_id: &str, workspace_id: &str) -> Response {
    if let Err(resp) = workspace_guard(st, instance_id, workspace_id) {
        return resp;
    }
    let rows: Vec<Value> = st
        .workspace_media
        .get(instance_id)
        .and_then(|m| m.get(workspace_id))
        .cloned()
        .unwrap_or_default();
    ok(json!({"Media": rows}))
}

fn op_associate_workspace(
    st: &mut ConnectState,
    instance_id: &str,
    workspace_id: &str,
    body: &Map<String, Value>,
    associate: bool,
) -> Response {
    if let Err(resp) = workspace_guard(st, instance_id, workspace_id) {
        return resp;
    }
    let arns = match opt_val(body, "ResourceArns") {
        Some(Value::Array(v)) => v.clone(),
        _ => return missing_body_param("ResourceArns"),
    };
    let existing = st
        .workspace_associations
        .get(instance_id)
        .and_then(|m| m.get(workspace_id))
        .cloned()
        .unwrap_or_default();
    let mut successful: Vec<Value> = Vec::new();
    let mut failed: Vec<Value> = Vec::new();
    for arn in arns {
        let Some(arn) = arn.as_str().map(str::to_string) else {
            continue;
        };
        let present = existing.iter().any(|r| r.get("ResourceArn") == Some(&json!(arn)));
        if associate && present {
            failed.push(json!({
                "ResourceArn": arn,
                "ErrorCode": "ResourceConflictException",
                "ErrorMessage": format!("Workspace association for {arn} already exists"),
            }));
            continue;
        }
        if !associate && !present {
            failed.push(json!({
                "ResourceArn": arn,
                "ErrorCode": "ResourceNotFoundException",
                "ErrorMessage": format!("Workspace association for {arn} not found"),
            }));
            continue;
        }
        let record = json!({
            "ResourceId": arn.clone(),
            "ResourceArn": arn.clone(),
            "ResourceType": "APP",
            "ResourceName": "",
        });
        if associate {
            st.workspace_associations
                .entry(instance_id.to_string())
                .or_default()
                .entry(workspace_id.to_string())
                .or_default()
                .push(record.clone());
        } else if let Some(map) = st.workspace_associations.get_mut(instance_id) {
            if let Some(list) = map.get_mut(workspace_id) {
                list.retain(|r| r.get("ResourceArn") != Some(&json!(arn)));
            }
        }
        successful.push(json!({
            "ResourceId": record["ResourceId"].clone(),
            "ResourceArn": record["ResourceArn"].clone(),
            "ResourceType": record["ResourceType"].clone(),
        }));
    }
    ok(json!({"SuccessfulList": successful, "FailedList": failed}))
}
fn op_search_workspace_associations(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    let instance_id = instance_id.to_string();
    if !st.instances.contains_key(&instance_id) {
        return notfound_instance(&instance_id);
    }
    let _ = body;
    let rows: Vec<Value> = st
        .workspace_associations
        .get(&instance_id)
        .map(|m| m.values().flatten().cloned().collect())
        .unwrap_or_default();
    ok(json!({"WorkspaceAssociations": rows, "ApproximateTotalCount": rows.len()}))
}

fn op_update_workspace_field(
    st: &mut ConnectState,
    instance_id: &str,
    workspace_id: &str,
    body: &Map<String, Value>,
    field: Option<&str>,
) -> Response {
    if let Err(resp) = workspace_guard(st, instance_id, workspace_id) {
        return resp;
    }
    let record = match st
        .workspaces
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(workspace_id))
        .and_then(Value::as_object_mut)
    {
        Some(r) => r,
        None => return not_found(format!("Workspace {workspace_id} not found")),
    };
    match field {
        None => {
            if let Some(name) = opt_val(body, "Name") {
                record.insert("Name".into(), name);
            }
            if let Some(desc) = opt_val(body, "Description") {
                record.insert("Description".into(), desc);
            }
            if let Some(title) = opt_val(body, "Title") {
                record.insert("Title".into(), title);
            }
        }
        // theme rides as a sub-map keyed WorkspaceTheme per OP table
        Some("WorkspaceTheme") => {
            if let Some(theme) = opt_val(body, "Theme") {
                record.insert("WorkspaceTheme".into(), theme);
            }
        }
        // visibility sub-key; only members the shape declares land here
        _ => {
            if let Some(value) = field.and_then(|f| opt_val(body, f)) {
                record.insert(field.unwrap_or("").to_string(), value);
            }
        }
    }
    record.insert("ModifiedTimestamp".into(), json!(now_iso()));
    ok(json!({}))
}

// -- Email address aliases + queue email addresses ---------------------------

fn op_associate_email_alias(
    st: &mut ConnectState,
    instance_id: &str,
    email_id: &str,
    body: &Map<String, Value>,
    associate: bool,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    if st
        .email_addresses
        .get(instance_id)
        .and_then(|m| m.get(email_id))
        .is_none()
    {
        return not_found(format!("Email address {email_id} not found"));
    }
    if associate {
        st.email_aliases
            .entry(instance_id.to_string())
            .or_default()
            .insert(
                email_id.to_string(),
                opt_val(body, "AliasConfiguration")
                    .and_then(|v| v.as_object().cloned())
                    .unwrap_or_default(),
            );
    } else {
        st.email_aliases
            .entry(instance_id.to_string())
            .or_default()
            .remove(email_id);
    }
    ok(json!({}))
}

fn op_update_email_address_metadata(
    st: &mut ConnectState,
    instance_id: &str,
    email_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let record = match st
        .email_addresses
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(email_id))
    {
        Some(e) => e,
        None => return not_found(format!("Email address {email_id} not found")),
    };
    if let Some(desc) = opt_val(body, "Description") {
        record["Description"] = desc;
    }
    if let Some(name) = opt_val(body, "DisplayName") {
        record["DisplayName"] = name;
    }
    record["ModifiedTimestamp"] = json!(now_iso());
    ok(json!({
        "EmailAddressId": record["EmailAddressId"].clone(),
        "EmailAddressArn": record["EmailAddressArn"].clone(),
    }))
}

fn op_associate_queue_emails(
    st: &mut ConnectState,
    instance_id: &str,
    queue_id: &str,
    body: &Map<String, Value>,
    associate: bool,
) -> Response {
    let instance_id = instance_id.to_string();

    if !st.instances.contains_key(&instance_id) {
        return notfound_instance(&instance_id);
    }
    if !st
        .queues
        .get(&instance_id)
        .map(|m| m.contains_key(queue_id))
        .unwrap_or(false)
    {
        return not_found(format!("Queue {queue_id} not found"));
    }
    let email_ids: Vec<String> = if associate {
        match opt_val(body, "EmailAddressesConfig") {
            Some(Value::Array(v)) => v
                .iter()
                .filter_map(|c| c.get("EmailAddressId").and_then(Value::as_str))
                .map(str::to_string)
                .collect(),
            _ => return missing_body_param("EmailAddressesConfig"),
        }
    } else {
        match opt_val(body, "EmailAddressesId") {
            Some(Value::Array(v)) => v
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect(),
            _ => return missing_body_param("EmailAddressesId"),
        }
    };
    let existing = st
        .queue_email_associations
        .get(&instance_id)
        .and_then(|m| m.get(queue_id))
        .cloned()
        .unwrap_or_default();
    for email in email_ids {
        let known = st
            .email_addresses
            .get(&instance_id)
            .is_some_and(|m| m.contains_key(&email));
        if !known {
            continue;
        }
        let queued = existing.contains(&email);
        if associate {
            if !queued {
                st.queue_email_associations
                    .entry(instance_id.to_string())
                    .or_default()
                    .entry(queue_id.to_string())
                    .or_default()
                    .push(email.clone());
            }
        } else if queued {
            if let Some(map) = st.queue_email_associations.get_mut(&instance_id) {
                if let Some(list) = map.get_mut(queue_id) {
                    list.retain(|e| *e != email);
                }
            }
        }
    }
    ok(json!({}))
}

fn op_list_queue_emails(st: &mut ConnectState, instance_id: &str, queue_id: &str) -> Response {
    let instance_id = instance_id.to_string();
    let queue_id = queue_id.to_string();
    if !st.instances.contains_key(&instance_id) {
        return notfound_instance(&instance_id);
    }
    if st
        .queues
        .get(&instance_id)
        .and_then(|m| m.get(&queue_id))
        .is_none()
    {
        return not_found(format!("Queue {queue_id} not found"));
    }
    let ids = st
        .queue_email_associations
        .get(&instance_id)
        .and_then(|m| m.get(&queue_id))
        .cloned()
        .unwrap_or_default();
    let rows: Vec<Value> = ids
        .iter()
        .filter_map(|email| st.email_addresses.get(&instance_id).and_then(|m| m.get(email)))
        .map(|e| json!({"Id": e["EmailAddressId"], "Arn": e["EmailAddressArn"]}))
        .collect();
    ok(json!({
        "EmailAddressMetadataList": rows,
        "LastModifiedTime": now_iso(),
        "LastModifiedRegion": st.instances.get(&instance_id).map(region_of).unwrap_or_default(),
    }))
}

// -- Traffic distribution config + group users ------------------------------

fn ensure_traffic_distribution(st: &ConnectState, tdg_id: &str) -> Result<(), Response> {
    if !st.traffic_distribution_groups.contains_key(tdg_id) {
        return Err(not_found(format!(
            "Traffic distribution group {tdg_id} not found"
        )));
    }
    Ok(())
}

fn op_get_traffic_distribution(st: &mut ConnectState, id: &str) -> Response {
    if let Err(resp) = ensure_traffic_distribution(st, id) {
        return resp;
    }
    let record = st.traffic_distribution_groups.get(id).cloned().unwrap();
    let record_id = str_field(&record, "Id");
    let record_arn = str_field(&record, "Arn");
    let region = str_field(&record, "InstanceArn")
        .split(':')
        .nth(3)
        .unwrap_or("")
        .to_string();
    ok(json!({
        "Id": record_id,
        "Arn": record_arn,
        "TelephonyConfig": record.get("TelephonyConfig").cloned().unwrap_or_else(|| json!({
            "Distributions": [{"Region": region.clone(), "Percentage": 100}]
        })),
        "SignInConfig": record.get("SignInConfig").cloned().unwrap_or_else(|| json!({
            "Distributions": [{"Region": region, "Enabled": true}]
        })),
        "AgentConfig": record.get("AgentConfig").cloned().unwrap_or_else(|| json!({"Distributions": []})),
    }))
}

fn op_update_traffic_distribution(
    st: &mut ConnectState,
    id: &str,
    body: &Map<String, Value>,
) -> Response {
    if let Err(resp) = ensure_traffic_distribution(st, id) {
        return resp;
    }
    let record = st
        .traffic_distribution_groups
        .get_mut(id)
        .and_then(Value::as_object_mut)
        .unwrap();
    if let Some(telephony) = opt_val(body, "TelephonyConfig") {
        record.insert("TelephonyConfig".into(), telephony);
    }
    if let Some(signin) = opt_val(body, "SignInConfig") {
        record.insert("SignInConfig".into(), signin);
    }
    if let Some(agent) = opt_val(body, "AgentConfig") {
        record.insert("AgentConfig".into(), agent);
    }
    ok(json!({}))
}

fn op_associate_tdg_user(
    st: &mut ConnectState,
    body: &Map<String, Value>,
    tdg_id: &str,
    associate: bool,
) -> Response {
    if let Err(resp) = ensure_traffic_distribution(st, tdg_id) {
        return resp;
    }
    let user_id = match body_str(body, "UserId") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let instance_id = match body_str(body, "InstanceId") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let present = st
        .traffic_distribution_group_users
        .get(tdg_id)
        .is_some_and(|v| v.iter().any(|u| u["UserId"] == json!(user_id)));
    if associate && present {
        return duplicate(format!(
            "User {user_id} is already associated to the traffic distribution group"
        ));
    }
    if !associate && !present {
        return not_found(format!(
            "User {user_id} is not associated to the traffic distribution group"
        ));
    }
    if associate {
        st.traffic_distribution_group_users
            .entry(tdg_id.to_string())
            .or_default()
            .push(json!({"UserId": user_id, "InstanceId": instance_id}));
    } else if let Some(v) = st.traffic_distribution_group_users.get_mut(tdg_id) {
        v.retain(|u| u["UserId"] != json!(user_id));
    }
    ok(json!({}))
}

fn op_list_tdg_users(
    st: &mut ConnectState,
    tdg_id: &str,
    query: &BTreeMap<String, Vec<String>>,
) -> Response {
    if let Err(resp) = ensure_traffic_distribution(st, tdg_id) {
        return resp;
    }
    let max_results = q1(query, "maxResults")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(10);
    let users: Vec<Value> = st
        .traffic_distribution_group_users
        .get(tdg_id)
        .map(|v| v.iter().take(max_results).cloned().collect())
        .unwrap_or_default();
    let rows: Vec<Value> = users
        .into_iter()
        .map(|u| json!({"UserId": u.get("UserId").cloned().unwrap_or(json!(""))}))
        .collect();
    ok(json!({"TrafficDistributionGroupUserSummaryList": rows}))
}


// -- Authentication profiles --------------------------------------------------

fn op_list_authentication_profiles(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let rows: Vec<Value> = st
        .authentication_profiles
        .get(instance_id)
        .map(|m| m.values().map(auth_profile_summary).collect())
        .unwrap_or_default();
    ok(json!({"AuthenticationProfileSummaryList": rows}))
}

fn auth_profile_summary(profile: &Value) -> Value {
    json!({
        "Id": profile["Id"],
        "Arn": profile["Arn"],
        "Name": profile["Name"],
        "IsDefault": profile["IsDefault"],
        "LastModifiedTime": profile["LastModifiedTime"],
        "LastModifiedRegion": profile["LastModifiedRegion"],
    })
}

fn op_describe_authentication_profile(
    st: &mut ConnectState,
    instance_id: &str,
    profile_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .authentication_profiles
        .get(instance_id)
        .and_then(|m| m.get(profile_id))
    {
        Some(p) => ok(json!({"AuthenticationProfile": p.clone()})),
        None => not_found(format!(
            "Authentication profile {profile_id} not found in instance {instance_id}"
        )),
    }
}

fn op_update_authentication_profile(
    st: &mut ConnectState,
    instance_id: &str,
    profile_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let profile = match st
        .authentication_profiles
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(profile_id))
        .and_then(Value::as_object_mut)
    {
        Some(p) => p,
        None => {
            return not_found(format!(
                "Authentication profile {profile_id} not found in instance {instance_id}"
            ))
        }
    };
    if let Some(name) = opt_val(body, "Name") {
        profile["Name"] = name;
    }
    if let Some(desc) = opt_val(body, "Description") {
        profile["Description"] = desc;
    }
    for field in [
        "AllowedIps",
        "BlockedIps",
        "PeriodicSessionDuration",
        "SessionInactivityDuration",
        "SessionInactivityHandlingEnabled",
    ] {
        if let Some(v) = opt_val(body, field) {
            profile.insert(field.to_string(), v);
        }
    }
    profile["LastModifiedTime"] = json!(now_iso());
    ok(json!({}))
}

// -- Users: runtime status / config, proficiencies, notifications ------------

fn user_guard(st: &ConnectState, instance_id: &str, user_id: &str) -> Result<(), Response> {
    if !st.instances.contains_key(instance_id) {
        return Err(notfound_instance(instance_id));
    }
    if st
        .users
        .get(instance_id)
        .and_then(|m| m.get(user_id))
        .is_none()
    {
        return Err(not_found(format!(
            "User {user_id} not found in instance {instance_id}"
        )));
    }
    Ok(())
}

fn op_put_user_status(
    st: &mut ConnectState,
    instance_id: &str,
    user_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if let Err(resp) = user_guard(st, instance_id, user_id) {
        return resp;
    }
    let agent_status_id = match body_str(body, "AgentStatusId") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let has_status = st
        .agent_statuses
        .get(instance_id)
        .map(|m| m.contains_key(&agent_status_id))
        .unwrap_or(false);
    if !has_status {
        return not_found(format!("Agent status {agent_status_id} not found"));
    }
    st.user_runtime
        .entry(instance_id.to_string())
        .or_default()
        .entry(user_id.to_string())
        .or_default()
        .insert("Status".into(), json!(agent_status_id.clone()));
    ok(json!({}))
}

fn op_update_user_config(
    st: &mut ConnectState,
    instance_id: &str,
    user_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if let Err(resp) = user_guard(st, instance_id, user_id) {
        return resp;
    }
    let mut config = Map::new();
    for field in [
        "AutoAcceptConfigs",
        "AfterContactWorkConfigs",
        "PhoneNumberConfigs",
        "PersistentConnectionConfigs",
        "VoiceEnhancementConfigs",
    ] {
        if let Some(v) = opt_val(body, field) {
            config.insert(field.to_string(), v);
        }
    }
    st.user_runtime
        .entry(instance_id.to_string())
        .or_default()
        .entry(user_id.to_string())
        .or_default()
        .insert("Config".into(), json!(config));
    ok(json!({}))
}

// -- Proficiencies ----------------------------------------------------------

fn op_update_user_proficiencies_inner(
    st: &mut ConnectState,
    instance_id: &str,
    user_id: &str,
    body: &Map<String, Value>,
    associate: bool,
) -> Response {
    if let Err(resp) = user_guard(st, instance_id, user_id) {
        return resp;
    }
    let proficiencies = match opt_val(body, "UserProficiencies") {
        Some(Value::Array(v)) => v.clone(),
        _ => return missing_body_param("UserProficiencies"),
    };
    let slot = st
        .user_proficiencies
        .entry(instance_id.to_string())
        .or_default()
        .entry(user_id.to_string())
        .or_default();
    if associate {
        for proficiency in &proficiencies {
            let name = v_opt_string(proficiency, "AttributeName").unwrap_or_default();
            let value = v_opt_string(proficiency, "AttributeValue").unwrap_or_default();
            slot.retain(|p| {
                !(p.get("AttributeName") == proficiency.get("AttributeName")
                    && p.get("AttributeValue") == proficiency.get("AttributeValue"))
            });
            slot.push(json!({
                "AttributeName": name,
                "AttributeValue": value,
                "Level": proficiency.get("Level").cloned().unwrap_or(json!(1.0)),
            }));
        }
    } else {
        for proficiency in &proficiencies {
            slot.retain(|p| {
                !(p.get("AttributeName") == proficiency.get("AttributeName")
                    && p.get("AttributeValue") == proficiency.get("AttributeValue"))
            });
        }
    }
    ok(json!({}))
}

fn op_disassociate_user_proficiencies(
    st: &mut ConnectState,
    instance_id: &str,
    user_id: &str,
    body: &Map<String, Value>,
) -> Response {
    op_update_user_proficiencies_inner(st, instance_id, user_id, body, false)
}

fn op_list_user_proficiencies(
    st: &mut ConnectState,
    instance_id: &str,
    user_id: &str,
) -> Response {
    if let Err(resp) = user_guard(st, instance_id, user_id) {
        return resp;
    }
    let rows: Vec<Value> = st
        .user_proficiencies
        .get(instance_id)
        .and_then(|m| m.get(user_id))
        .cloned()
        .unwrap_or_default();
    ok(json!({
        "UserProficiencyList": rows,
        "LastModifiedTime": now_iso(),
        "LastModifiedRegion": st.instances.get(instance_id).map(region_of).unwrap_or_default(),
    }))
}

// -- User notifications -----------------------------------------------------

fn op_list_user_notifications(
    st: &mut ConnectState,
    instance_id: &str,
    user_id: &str,
    query: &BTreeMap<String, Vec<String>>,
) -> Response {
    if let Err(resp) = user_guard(st, instance_id, user_id) {
        return resp;
    }
    let max_results = q1(query, "maxResults")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1000);
    let stored: Vec<Value> = st
        .user_notifications
        .get(instance_id)
        .and_then(|m| m.get(user_id))
        .map(|m| m.values().take(max_results).cloned().collect())
        .unwrap_or_default();
    ok(json!({"UserNotifications": stored}))
}

fn op_update_user_notification_status(
    st: &mut ConnectState,
    instance_id: &str,
    user_id: &str,
    notification_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if let Err(resp) = user_guard(st, instance_id, user_id) {
        return resp;
    }
    let status = match body_str(body, "Status") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let record = st
        .user_notifications
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(user_id))
        .and_then(|m| m.get_mut(notification_id));
    match record {
        None => not_found(format!("Notification {notification_id} not found")),
        Some(record) => {
            record["NotificationStatus"] = json!(status);
            record["LastModifiedTime"] = json!(now_iso());
            ok(json!({}))
        }
    }
}



fn op_update_notification_content(
    st: &mut ConnectState,
    instance_id: &str,
    notification_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let record = match st
        .notifications
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(notification_id))
    {
        Some(n) => n,
        None => return not_found(format!("Notification {notification_id} not found")),
    };
    let Some(content) = opt_val(body, "Content") else {
        return missing_body_param("Content");
    };
    record["Content"] = content;
    record["LastModifiedTime"] = json!(now_iso());
    ok(json!({}))
}

// -- Hours of operations family ------------------------------------------------

fn op_associate_hours_of_operations(
    st: &mut ConnectState,
    instance_id_param: &str,
    hours_of_operation_id_param: &str,
    body: &Map<String, Value>,
) -> Response {
    let instance_id = instance_id_param.to_string();
    let hoo_id = hours_of_operation_id_param.to_string();
    let _ = body_str(body, "InstanceId");
    if let Err(resp) = hoo_guard(st, &instance_id, &hoo_id) {
        return resp;
    }
    let parents = match opt_val(body, "ParentHoursOfOperationConfigs") {
        Some(Value::Array(v)) => v,
        _ => return missing_body_param("ParentHoursOfOperationConfigs"),
    };
    let mut ids: Vec<String> = Vec::new();
    for parent in parents {
        if let Some(id) = parent.get("HoursOfOperationId").and_then(Value::as_str) {
            ids.push(id.to_string());
        }
    }
    let existing = st
        .child_hours_of_operations
        .get(&instance_id)
        .and_then(|m| m.get(hoo_id.as_str()))
        .cloned()
        .unwrap_or_default();
    for parent_id in &ids {
        if existing.contains(parent_id) {
            return duplicate(format!(
                "Hours of operation {parent_id} is already associated to {hoo_id}"
            ));
        }
    }
    st.child_hours_of_operations
        .entry(instance_id.to_string())
        .or_default()
        .entry(hoo_id.clone())
        .or_default()
        .extend(ids.iter().cloned());
    ok(json!({}))
}

fn op_disassociate_hours_of_operations(
    st: &mut ConnectState,
    instance_id_param: &str,
    hours_of_operation_id_param: &str,
    body: &Map<String, Value>,
) -> Response {
    let instance_id = instance_id_param.to_string();
    let hoo_id = hours_of_operation_id_param.to_string();
    let _ = body_str(body, "InstanceId");
    if let Err(resp) = hoo_guard(st, &instance_id, &hoo_id) {
        return resp;
    }
    let parents: Vec<String> = match opt_val(body, "ParentHoursOfOperationIds") {
        Some(Value::Array(v)) => v
            .into_iter()
            .map(|p| v_opt_string(&p, "HoursOfOperationId").unwrap_or_default())
            .collect(),
        _ => return missing_body_param("ParentHoursOfOperationIds"),
    };
    let hoo_id_owned = hoo_id.to_string();
    let slot = st
        .child_hours_of_operations
        .get_mut(&instance_id)
        .and_then(|m| m.get_mut(hoo_id_owned.as_str()));
    match slot {
        None => not_found(format!(
            "Hours of operation {hoo_id} is not associated with any parent hours"
        )),
        Some(list) => {
            for parent_id in &parents {
                list.retain(|p| p != parent_id);
            }
            ok(json!({}))
        }
    }
}

fn hoo_guard(st: &ConnectState, instance_id: &str, hoo_id: &str) -> Result<(), Response> {
    if !st.instances.contains_key(instance_id) {
        return Err(notfound_instance(instance_id));
    }
    if st
        .hours_of_operations
        .get(instance_id)
        .and_then(|m| m.get(hoo_id))
        .is_none()
    {
        return Err(not_found(format!(
            "Hours of operation {hoo_id} not found in instance {instance_id}"
        )));
    }
    Ok(())
}

fn op_list_child_hours_of_operations(
    st: &mut ConnectState,
    instance_id: &str,
    hoo_id: &str,
) -> Response {
    if let Err(resp) = hoo_guard(st, instance_id, hoo_id) {
        return resp;
    }
    let children = st
        .child_hours_of_operations
        .get(instance_id)
        .and_then(|m| m.get(hoo_id))
        .cloned()
        .unwrap_or_default();
    let empty = HashMap::new();
    let all = st.hours_of_operations.get(instance_id).unwrap_or(&empty);
    let rows: Vec<Value> = children
        .iter()
        .filter_map(|id| all.get(id))
        .map(|child| json!({"Name": child["Name"], "Id": child["Id"], "Arn": child["Arn"]}))
        .collect();
    ok(json!({
        "ChildHoursOfOperationsSummaryList": rows,
        "LastModifiedTime": now_iso(),
        "LastModifiedRegion": st.instances.get(instance_id).map(region_of).unwrap_or_default(),
    }))
}

fn op_get_effective_hours_of_operations(
    st: &mut ConnectState,
    instance_id: &str,
    hoo_id: &str,
    query: &BTreeMap<String, Vec<String>>,
) -> Response {
    let _ = (q1(query, "FromDate"), q1(query, "ToDate"));
    if let Err(resp) = hoo_guard(st, instance_id, hoo_id) {
        return resp;
    }
    let hours = st
        .hours_of_operations
        .get(instance_id)
        .and_then(|m| m.get(hoo_id))
        .cloned()
        .unwrap_or_default();
    let zone = match hours.get("TimeZone").cloned() {
        Some(z) if !z.as_str().unwrap_or_default().is_empty() => z,
        _ => json!("UTC"),
    };
    ok(json!({
        "EffectiveHoursOfOperationList": [{
            "Date": "2026-10-22",
            "OperationalHours": [{"Start": {"Hours": 0, "Minutes": 0}, "End": {"Hours": 23, "Minutes": 59}}]
        }],
        "EffectiveOverrideHoursList": [],
        "TimeZone": zone,
    }))
}

// -- Lex bots -----------------------------------------------------------------

fn op_associate_lex_bot(st: &mut ConnectState, instance_id: &str, body: &Map<String, Value>) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let Some(bot) = opt_val(body, "LexBot") else {
        return missing_body_param("LexBot");
    };
    let name = v_opt_string(&bot, "Name").unwrap_or_default();
    let dup = st
        .bots
        .get(instance_id)
        .is_some_and(|v| v.iter().any(|b| v_opt_string(b, "Name") == Some(name.clone())));
    if dup {
        return duplicate(format!("Bot {name} is already associated to instance {instance_id}"));
    }
    st.bots
        .entry(instance_id.to_string())
        .or_default()
        .push(bot);
    ok(json!({}))
}

fn op_disassociate_lex_bot(
    st: &mut ConnectState,
    instance_id: &str,
    query: &BTreeMap<String, Vec<String>>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let bot_name = q1(query, "botName").unwrap_or_default();
    let lex_region = q1(query, "lexRegion").unwrap_or_default();
    if bot_name.is_empty() {
        return invalid("BotName is a required parameter");
    }
    let _ = lex_region;
    if let Some(v) = st.bots.get_mut(instance_id) {
        v.retain(|b| v_opt_string(b, "Name").as_deref() != Some(bot_name));
    }
    ok(json!({}))
}

fn op_list_lex_bots(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let rows: Vec<Value> = st
        .bots
        .get(instance_id)
        .cloned()
        .unwrap_or_default();
    ok(json!({"LexBots": rows}))
}

// -- Security profile associations -----------------------------------------------

fn op_associate_security_profiles(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
    associate: bool,
) -> Response {
    let entity_arn = match body_str(body, "EntityArn") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let _ = body_str(body, "EntityType");
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let profiles = match opt_val(body, "SecurityProfiles") {
        Some(Value::Array(v)) => v.clone(),
        _ => return missing_body_param("SecurityProfiles"),
    };
    let existing = st
        .entity_security_profiles
        .get(instance_id)
        .and_then(|m| m.get(entity_arn.as_str()))
        .cloned()
        .unwrap_or_default();
    for profile in profiles {
        let Some(id) = profile.get("Id").and_then(Value::as_str) else {
            continue;
        };
        let present = existing.iter().any(|p| p.get("Id") == Some(&json!(id)));
        if associate && present {
            return duplicate(format!(
                "Security profile {id} is already associated to the entity"
            ));
        }
        if !associate && !present {
            return not_found(format!(
                "Security profile {id} is not associated with the entity"
            ));
        }
        if associate {
            st.entity_security_profiles
                .entry(instance_id.to_string())
                .or_default()
                .entry(entity_arn.clone())
                .or_default()
                .push(json!({"Id": id}));
        } else if let Some(map) = st.entity_security_profiles.get_mut(instance_id) {
            if let Some(list) = map.get_mut(&entity_arn) {
                list.retain(|p| p.get("Id") != Some(&json!(id)));
            }
        }
    }
    ok(json!({}))
}

fn op_list_entity_security_profiles(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let Some(entity_arn) = opt_string(body, "EntityArn") else {
        return missing_body_param("EntityArn");
    };
    let rows: Vec<Value> = st
        .entity_security_profiles
        .get(instance_id)
        .and_then(|m| m.get(&entity_arn))
        .cloned()
        .unwrap_or_default();
    ok(json!({"SecurityProfiles": rows}))
}

fn op_list_security_profile_flow_modules(
    st: &mut ConnectState,
    instance_id: &str,
    security_profile_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let record = match st
        .security_profiles
        .get(instance_id)
        .and_then(|m| m.get(security_profile_id))
    {
        Some(r) => r.clone(),
        None => {
            return not_found(format!(
                "Security profile {security_profile_id} not found in instance {instance_id}"
            ))
        }
    };
    let flows = record.get("AllowedFlowModules").cloned().unwrap_or_else(|| json!([]));
    ok(json!({
        "AllowedFlowModules": flows,
        "LastModifiedTime": now_iso(),
        "LastModifiedRegion": st.instances.get(instance_id).map(region_of).unwrap_or_default(),
    }))
}

fn op_list_routing_profile_manual_assignment_queues(
    st: &mut ConnectState,
    instance_id: &str,
    routing_profile_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    if st
        .routing_profiles
        .get(instance_id)
        .and_then(|m| m.get(routing_profile_id))
        .is_none()
    {
        return not_found(format!(
            "Routing profile {routing_profile_id} not found in instance {instance_id}"
        ));
    }
    ok(json!({
        "RoutingProfileManualAssignmentQueueConfigSummaryList": [],
        "LastModifiedTime": now_iso(),
        "LastModifiedRegion": st.instances.get(instance_id).map(region_of).unwrap_or_default(),
    }))
}

fn op_update_routing_profile_agent_availability_timer(
    st: &mut ConnectState,
    instance_id: &str,
    routing_profile_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let Some(timer) = opt_val(body, "AgentAvailabilityTimer") else {
        return missing_body_param("AgentAvailabilityTimer");
    };
    let record = match st
        .routing_profiles
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(routing_profile_id))
    {
        Some(r) => r,
        None => {
            return not_found(format!(
                "Routing profile {routing_profile_id} not found in instance {instance_id}"
            ))
        }
    };
    record["AgentAvailabilityTimer"] = timer;
    ok(json!({}))
}

fn op_update_queue_outbound_email_config(
    st: &mut ConnectState,
    instance_id: &str,
    queue_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let Some(config) = opt_val(body, "OutboundEmailConfig") else {
        return missing_body_param("OutboundEmailConfig");
    };
    let record = match st
        .queues
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(queue_id))
    {
        Some(q) => q,
        None => return not_found(format!("Queue {queue_id} not found")),
    };
    record["OutboundEmailConfig"] = config;
    ok(json!({}))
}

fn op_send_outbound_email(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    for field in ["FromEmailAddress", "DestinationEmailAddress", "EmailMessage", "TrafficType"] {
        if opt_val(body, field).is_none() {
            return missing_body_param(field);
        }
    }
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    ok(json!({}))
}

// -- Contact life-cycle family ----------------------------------------------

/// Create a contact record (the module's idiom, distilled): callers give
/// channel, initiation method and already-modeled extra fields.
fn new_contact_record(
    st: &mut ConnectState,
    instance_id: &str,
    channel: &str,
    initiation_method: &str,
    extra: Value,
) -> Result<Value, Response> {
    let Some(instance) = st.instances.get(instance_id) else {
        return Err(notfound_instance(instance_id));
    };
    let contact_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{}/contact/{contact_id}", str_field(instance, "Arn"));
    let stamp = now_iso();
    let mut contact = Map::new();
    contact.insert("Id".into(), json!(contact_id.clone()));
    contact.insert("Arn".into(), json!(arn.clone()));
    contact.insert("Channel".into(), json!(channel));
    contact.insert("InitiationMethod".into(), json!(initiation_method));
    contact.insert("InitiationTimestamp".into(), json!(stamp.clone()));
    contact.insert("LastUpdateTimestamp".into(), json!(stamp));
    if let Value::Object(map) = extra {
        for (k, v) in map {
            contact.insert(k, v);
        }
    }
    st.contacts
        .entry(instance_id.to_string())
        .or_default()
        .insert(contact_id.clone(), Value::Object(contact));
    Ok(json!({"Id": contact_id, "Arn": arn}))
}

fn instance_guard_body(body: &Map<String, Value>) -> Result<String, Response> {
    match body_str(body, "InstanceId") {
        Ok(v) if !v.is_empty() => Ok(v),
        _ => Err(missing_body_param("InstanceId")),
    }
}

fn contact_guard(st: &ConnectState, instance_id: &str, contact_id: &str) -> Result<(), Response> {
    if !st.instances.contains_key(instance_id) {
        return Err(notfound_instance(instance_id));
    }
    if st
        .contacts
        .get(instance_id)
        .and_then(|m| m.get(contact_id))
        .is_none()
    {
        return Err(not_found(format!("Contact {contact_id} not found")));
    }
    Ok(())
}

fn op_start_chat_contact(st: &mut ConnectState, body: &Map<String, Value>) -> Response {
    let instance_id = match instance_guard_body(body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let flow_id = match body_str(body, "ContactFlowId") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if opt_val(body, "ParticipantDetails").is_none() {
        return missing_body_param("ParticipantDetails");
    }
    let contact = match new_contact_record(
        st,
        &instance_id,
        "CHAT",
        "CHAT",
        json!({
            "ContactFlowId": flow_id,
            "ParticipantDetails": opt_val(body, "ParticipantDetails").unwrap_or(json!({})),
            "SegmentAttributes": opt_val(body, "SegmentAttributes").unwrap_or(json!({})),
        }),
    ) {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    ok(json!({
        "ContactId": contact["Id"].clone(),
        "ParticipantId": uuid::Uuid::new_v4().to_string(),
        "ParticipantToken": uuid::Uuid::new_v4().to_string(),
    }))
}

fn op_start_outbound_voice_contact(st: &mut ConnectState, body: &Map<String, Value>) -> Response {
    let instance_id = match instance_guard_body(body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if opt_val(body, "DestinationPhoneNumber").is_none() {
        return missing_body_param("DestinationPhoneNumber");
    }
    let flow_id = match body_str(body, "ContactFlowId") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let contact = match new_contact_record(
        st,
        &instance_id,
        "VOICE",
        "OUTBOUND",
        json!({
            "ContactFlowId": flow_id,
            "DestinationPhoneNumber": opt_val(body, "DestinationPhoneNumber").unwrap_or(json!("")),
            "QueueId": opt_val(body, "QueueId").unwrap_or(json!("")),
            "Attributes": opt_val(body, "Attributes").unwrap_or(json!({})),
        }),
    ) {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    ok(json!({"ContactId": contact["Id"].clone()}))
}

fn op_start_task_contact(st: &mut ConnectState, body: &Map<String, Value>) -> Response {
    let instance_id = match instance_guard_body(body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let name = match body_str(body, "Name") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let contact = match new_contact_record(
        st,
        &instance_id,
        "TASK",
        "API",
        json!({
            "Name": name,
            "TaskTemplateId": opt_val(body, "TaskTemplateId").unwrap_or(json!("")),
            "Attributes": opt_val(body, "Attributes").unwrap_or(json!({})),
        }),
    ) {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    ok(json!({"ContactId": contact["Id"].clone()}))
}

fn op_start_email_contact(st: &mut ConnectState, body: &Map<String, Value>, outbound: bool) -> Response {
    let instance_id = match instance_guard_body(body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if opt_val(body, "EmailMessage").is_none() {
        return missing_body_param("EmailMessage");
    }
    if opt_val(body, "DestinationEmailAddress").is_none() {
        return missing_body_param("DestinationEmailAddress");
    }
    let contact = match new_contact_record(
        st,
        &instance_id,
        "EMAIL",
        if outbound { "OUTBOUND" } else { "INBOUND" },
        json!({
            "ContactFlowId": opt_val(body, "ContactFlowId").unwrap_or(json!("")),
            "FromEmailAddress": opt_val(body, "FromEmailAddress").unwrap_or(json!({})),
            "DestinationEmailAddress": opt_val(body, "DestinationEmailAddress").unwrap_or(json!({})),
            "EmailMessage": opt_val(body, "EmailMessage").unwrap_or(json!({})),
        }),
    ) {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    ok(json!({"ContactId": contact["Id"].clone()}))
}

fn op_start_outbound_chat_contact(st: &mut ConnectState, body: &Map<String, Value>) -> Response {
    for field in ["SourceEndpoint", "DestinationEndpoint", "SegmentAttributes", "ContactFlowId"] {
        if opt_val(body, field).is_none() {
            return missing_body_param(field);
        }
    }
    let instance_id = match instance_guard_body(body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let contact = match new_contact_record(
        st,
        &instance_id,
        "CHAT",
        "OUTBOUND",
        json!({
            "ContactFlowId": opt_val(body, "ContactFlowId").unwrap_or(json!("")),
            "SegmentAttributes": opt_val(body, "SegmentAttributes").unwrap_or(json!({})),
            "ParticipantDetails": opt_val(body, "ParticipantDetails").unwrap_or(json!({})),
        }),
    ) {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    ok(json!({"ContactId": contact["Id"].clone()}))
}

fn op_start_webrtc_contact(st: &mut ConnectState, body: &Map<String, Value>) -> Response {
    for field in ["ParticipantDetails", "ContactFlowId"] {
        if opt_val(body, field).is_none() {
            return missing_body_param(field);
        }
    }
    let instance_id = match instance_guard_body(body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let contact = match new_contact_record(
        st,
        &instance_id,
        "VOICE",
        "WEBRTC_API",
        json!({
            "ContactFlowId": opt_val(body, "ContactFlowId").unwrap_or(json!("")),
            "ParticipantDetails": opt_val(body, "ParticipantDetails").unwrap_or(json!({})),
        }),
    ) {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    ok(json!({
        "ConnectionData": "",
        "ContactId": contact["Id"].clone(),
        "ParticipantId": uuid::Uuid::new_v4().to_string(),
        "ParticipantToken": uuid::Uuid::new_v4().to_string(),
    }))
}

fn op_send_chat_integration_event(st: &mut ConnectState, body: &Map<String, Value>) -> Response {
    for field in ["SourceId", "DestinationId", "Event"] {
        if opt_val(body, field).is_none() {
            return missing_body_param(field);
        }
    }
    let event = opt_val(body, "Event").unwrap_or(json!({}));
    let event_type = v_opt_string(&event, "Type").unwrap_or_default();
    let disconnect = event_type == "DISCONNECT" || event_type == "DISMISS_AGENT_CHAT";
    if !disconnect {
        let details = opt_val(body, "NewSessionDetails").unwrap_or(json!({}));
        if let Some(instance_id) = v_opt_string(&details, "InstanceId").filter(|v| !v.is_empty()) {
            let contact = match new_contact_record(
                st,
                &instance_id,
                "CHAT",
                "INBOUND",
                json!({"SourceId": opt_val(body, "SourceId").unwrap_or(json!(""))}),
            ) {
                Ok(c) => c,
                Err(resp) => return resp,
            };
            return ok(json!({
                "InitialContactId": contact["Id"].clone(),
                "NewChatCreated": true,
            }));
        }
    }
    ok(json!({"InitialContactId": "", "NewChatCreated": false}))
}

fn op_create_participant(
    st: &mut ConnectState,
    instance_id: &str,
    contact_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if let Err(resp) = contact_guard(st, instance_id, contact_id) {
        return resp;
    }
    if opt_val(body, "ParticipantDetails").is_none() {
        return missing_body_param("ParticipantDetails");
    }
    ok(json!({
        "ParticipantId": uuid::Uuid::new_v4().to_string(),
        "ParticipantCredentials": {
            "ParticipantToken": uuid::Uuid::new_v4().to_string(),
            "Expiry": now_iso(),
        },
    }))
}
fn op_create_persistent_contact_association(
    st: &mut ConnectState,
    instance_id: &str,
    initial_contact_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if let Err(resp) = contact_guard(st, instance_id, initial_contact_id) {
        return resp;
    }
    let _ = body_str(body, "RehydrationType");
    let source_contact_id = match body_str(body, "SourceContactId") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    ok(json!({"ContinuedFromContactId": source_contact_id}))
}

fn op_batch_put_contact(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    let instance_id = instance_id.to_string();
    if !st.instances.contains_key(&instance_id) {
        return notfound_instance(&instance_id);
    }
    let items = match opt_val(body, "ContactDataRequestList") {
        Some(Value::Array(v)) => v.clone(),
        _ => return missing_body_param("ContactDataRequestList"),
    };
    let mut successful: Vec<Value> = Vec::new();
    let failed: Vec<Value> = Vec::new();
    for item in &items {
        let identifier = v_opt_string(item, "RequestIdentifier").unwrap_or_default();
        let channel = v_opt_string(item, "Channel").unwrap_or_else(|| "CHAT".to_string());
        let initiation = v_opt_string(item, "InitiationMethod").unwrap_or_else(|| "API".to_string());
        match new_contact_record(
            st,
            &instance_id,
            &channel,
            &initiation,
            json!({
                "Name": v_opt_val(item, "Name").unwrap_or(json!("")),
                "SegmentAttributes": v_opt_val(item, "SegmentAttributes").unwrap_or(json!({})),
            }),
        ) {
            Ok(contact) => successful.push(json!({
                "RequestIdentifier": identifier,
                "ContactId": contact["Id"].clone(),
            })),
            Err(resp) => return resp,
        }
    }
    ok(json!({"SuccessfulRequestList": successful, "FailedRequestList": failed}))
}

fn op_monitor_contact(st: &mut ConnectState, body: &Map<String, Value>) -> Response {
    let instance_id = match instance_guard_body(body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let contact_id = match body_str(body, "ContactId") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if let Err(resp) = contact_guard(st, &instance_id, &contact_id) {
        return resp;
    }
    let _ = opt_val(body, "UserId");
    let arn = st
        .contacts
        .get(&instance_id)
        .and_then(|m| m.get(&contact_id))
        .map(|c| c["Arn"].clone())
        .unwrap_or(json!(""));
    ok(json!({"ContactId": contact_id.clone(), "ContactArn": arn}))
}

fn op_transfer_contact(st: &mut ConnectState, body: &Map<String, Value>) -> Response {
    let instance_id = match instance_guard_body(body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let contact_id = match body_str(body, "ContactId") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let flow_id = match body_str(body, "ContactFlowId") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if let Err(resp) = contact_guard(st, &instance_id, &contact_id) {
        return resp;
    }
    let channel = st
        .contacts
        .get(&instance_id)
        .and_then(|m| m.get(&contact_id))
        .and_then(|c| v_opt_string(c, "Channel"))
        .unwrap_or_else(|| "VOICE".to_string());
    let contact = match new_contact_record(
        st,
        &instance_id,
        &channel,
        "TRANSFER",
        json!({
            "ContactFlowId": flow_id,
            "PreviousContactId": contact_id.clone(),
            "QueueId": opt_val(body, "QueueId").unwrap_or(json!("")),
            "UserId": opt_val(body, "UserId").unwrap_or(json!("")),
        }),
    ) {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    ok(json!({"ContactId": contact["Id"].clone(), "ContactArn": contact["Arn"].clone()}))
}

fn op_pause_contact(st: &mut ConnectState, body: &Map<String, Value>, pause: bool) -> Response {
    let instance_id = match instance_guard_body(body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let contact_id = match body_str(body, "ContactId") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if let Err(resp) = contact_guard(st, &instance_id, &contact_id) {
        return resp;
    }
    let record = st
        .contacts
        .get_mut(&instance_id)
        .and_then(|m| m.get_mut(&contact_id))
        .unwrap();
    record["PausedStatus"] = json!(if pause { "PAUSED" } else { "RESUMED" });
    record["LastUpdateTimestamp"] = json!(now_iso());
    ok(json!({}))
}

fn op_dismiss_user_contact(
    st: &mut ConnectState,
    instance_id: &str,
    user_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if let Err(resp) = user_guard(st, instance_id, user_id) {
        return resp;
    }
    let contact_id = match body_str(body, "ContactId") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if let Err(resp) = contact_guard(st, instance_id, &contact_id) {
        return resp;
    }
    ok(json!({}))
}
fn op_set_contact_recording(st: &mut ConnectState, body: &Map<String, Value>, state: &str) -> Response {
    let instance_id = match instance_guard_body(body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let contact_id = match body_str(body, "ContactId") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if let Err(resp) = contact_guard(st, &instance_id, &contact_id) {
        return resp;
    }
    let _ = opt_val(body, "InitialContactId");
    let record = st
        .contacts
        .get_mut(&instance_id)
        .and_then(|m| m.get_mut(&contact_id))
        .unwrap();
    record["RecordingStatus"] = json!(state);
    record["LastUpdateTimestamp"] = json!(now_iso());
    ok(json!({}))
}

fn op_contact_media_processing(st: &mut ConnectState, body: &Map<String, Value>, start: bool) -> Response {
    // no required inputs: instances and contacts the probe did not create
    // still answer the modeled empty envelope
    let Ok(instance_id) = instance_guard_body(body) else {
        return ok(json!({}));
    };
    let Ok(contact_id) = body_str(body, "ContactId") else {
        return ok(json!({}));
    };
    if contact_guard(st, &instance_id, &contact_id).is_err() {
        return ok(json!({}));
    }
    let record = st
        .contacts
        .get_mut(&instance_id)
        .and_then(|m| m.get_mut(&contact_id))
        .unwrap();
    record["MediaProcessingStatus"] = json!(if start { "ACTIVE" } else { "INACTIVE" });
    record["LastUpdateTimestamp"] = json!(now_iso());
    ok(json!({}))
}

fn op_start_screen_sharing(st: &mut ConnectState, body: &Map<String, Value>) -> Response {
    let instance_id = match instance_guard_body(body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let contact_id = match body_str(body, "ContactId") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if let Err(resp) = contact_guard(st, &instance_id, &contact_id) {
        return resp;
    }
    ok(json!({}))
}

fn op_start_contact_streaming(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    let contact_id = match body_str(body, "ContactId") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if let Err(resp) = contact_guard(st, instance_id, &contact_id) {
        return resp;
    }
    if opt_val(body, "ChatStreamingConfiguration").is_none() {
        return missing_body_param("ChatStreamingConfiguration");
    }
    let _ = body_str(body, "ClientToken");
    let streaming_id = uuid::Uuid::new_v4().to_string();
    st.contact_streaming_ids
        .entry(instance_id.to_string())
        .or_default()
        .insert(contact_id.clone(), json!(streaming_id.clone()));
    ok(json!({"StreamingId": streaming_id}))
}
fn op_stop_contact_streaming(st: &mut ConnectState, body: &Map<String, Value>) -> Response {
    let instance_id = match instance_guard_body(body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let contact_id = match body_str(body, "ContactId") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let streaming_id = match body_str(body, "StreamingId") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if let Err(resp) = contact_guard(st, &instance_id, &contact_id) {
        return resp;
    }
    let record = st
        .contact_streaming_ids
        .get(&instance_id)
        .and_then(|m| m.get(&contact_id))
        .cloned()
        .unwrap_or(json!(""));
    if record != json!(streaming_id) {
        return not_found(format!("Streaming id {streaming_id} not found"));
    }
    ok(json!({}))
}

fn op_tag_contact(st: &mut ConnectState, body: &Map<String, Value>) -> Response {
    let instance_id = match instance_guard_body(body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let contact_id = match body_str(body, "ContactId") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if let Err(resp) = contact_guard(st, &instance_id, &contact_id) {
        return resp;
    }
    let Some(tags) = opt_val(body, "Tags") else {
        return missing_body_param("Tags");
    };
    let record = st
        .contacts
        .get_mut(&instance_id)
        .and_then(|m| m.get_mut(&contact_id))
        .and_then(Value::as_object_mut)
        .unwrap();
    let contact_arn = record
        .get("Arn")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if let Value::Object(tags) = &tags {
        let merged: Map<String, Value> = record
            .get("Tags")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .chain(tags.iter().map(|(k, v)| (k.clone(), v.clone())))
            .collect();
        record.insert("Tags".into(), json!(merged));
    }
    if let Some(Value::Object(map)) = record.get("Tags") {
        st.tags
            .entry(contact_arn)
            .or_default()
            .extend(map.iter().map(|(k, v)| (k.clone(), v.clone())));
    }
    ok(json!({}))
}

fn op_untag_contact(
    st: &mut ConnectState,
    instance_id: &str,
    contact_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if let Err(resp) = contact_guard(st, instance_id, contact_id) {
        return resp;
    }
    let tag_keys = match opt_val(body, "TagKeys") {
        Some(Value::Array(v)) => v.clone(),
        _ => return missing_body_param("TagKeys"),
    };
    let record = st
        .contacts
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(contact_id))
        .and_then(Value::as_object_mut)
        .unwrap();
    let contact_arn = record
        .get("Arn")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let kept: Map<String, Value> = record
        .get("Tags")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default()
        .iter()
        .filter(|(k, _)| !tag_keys.iter().any(|key| key.as_str() == Some(k.as_str())))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    record.insert("Tags".into(), json!(kept));
    if let Some(map) = st.tags.get_mut(&contact_arn) {
        let keys: Vec<String> = tag_keys.iter().map(|k| k.as_str().unwrap_or_default().to_string()).collect();
        for key in keys {
            map.remove(&key);
        }
    }
    ok(json!({}))
}

fn op_update_contact_routing_data(
    st: &mut ConnectState,
    instance_id: &str,
    contact_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if let Err(resp) = contact_guard(st, instance_id, contact_id) {
        return resp;
    }
    let record = st
        .contacts
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(contact_id))
        .unwrap();
    if let Some(v) = opt_val(body, "QueueTimeAdjustmentSeconds") {
        record["QueueTimeAdjustmentSeconds"] = v;
    }
    if let Some(v) = opt_val(body, "QueuePriority") {
        record["QueuePriority"] = v;
    }
    if let Some(v) = opt_val(body, "RoutingCriteria") {
        record["RoutingCriteria"] = v;
    }
    record["LastUpdateTimestamp"] = json!(now_iso());
    ok(json!({}))
}
fn op_update_contact_schedule(st: &mut ConnectState, body: &Map<String, Value>) -> Response {
    let instance_id = match instance_guard_body(body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let contact_id = match body_str(body, "ContactId") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if let Err(resp) = contact_guard(st, &instance_id, &contact_id) {
        return resp;
    }
    let Some(scheduled) = opt_val(body, "ScheduledTime") else {
        return missing_body_param("ScheduledTime");
    };
    let record = st
        .contacts
        .get_mut(&instance_id)
        .and_then(|m| m.get_mut(&contact_id))
        .unwrap();
    record["ScheduledTimestamp"] = scheduled;
    record["LastUpdateTimestamp"] = json!(now_iso());
    ok(json!({}))
}

fn op_update_participant_authentication(_st: &mut ConnectState, body: &Map<String, Value>) -> Response {
    let _ = (opt_val(body, "State"), opt_val(body, "InstanceId"));
    ok(json!({}))
}

// -- Metrics + realtime analysis + evaluations ----------------------------------

fn op_get_contact_metrics(st: &mut ConnectState, body: &Map<String, Value>) -> Response {
    let instance_id = match instance_guard_body(body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let contact_id = match body_str(body, "ContactId") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if let Err(resp) = contact_guard(st, &instance_id, &contact_id) {
        return resp;
    }
    let arn = st
        .contacts
        .get(&instance_id)
        .and_then(|m| m.get(&contact_id))
        .map(|c| c["Arn"].clone())
        .unwrap_or(json!(""));
    let requested = opt_val(body, "Metrics").unwrap_or(json!([]));
    let results: Vec<Value> = requested
        .as_array()
        .map(|m| {
            m.iter()
                .map(|metric| {
                    json!({
                        "Name": metric.get("Name").cloned().unwrap_or(json!("")),
                        "Value": {"Number": 0},
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    ok(json!({"MetricResults": results, "Id": contact_id.clone(), "Arn": arn}))
}

fn op_get_current_metric_data(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let _ = opt_val(body, "Filters");
    let requested = opt_val(body, "CurrentMetrics").unwrap_or(json!([]));
    let results: Vec<Value> = requested
        .as_array()
        .map(|m| {
            m.iter()
                .map(|metric| {
                    json!({
                        "Dimensions": {},
                        "Collections": [{"Metric": metric.clone(), "Value": 0}],
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    ok(json!({
        "MetricResults": results,
        "DataSnapshotTime": now_iso(),
        "ApproximateTotalCount": 0,
    }))
}

fn op_get_current_user_data(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let _ = opt_val(body, "Filters");
    let rows: Vec<Value> = st
        .users
        .get(instance_id)
        .map(|m| {
            m.values()
                .map(|u| json!({"User": {"Id": u["Id"], "Arn": u["Arn"]}}))
                .collect()
        })
        .unwrap_or_default();
    ok(json!({"UserDataList": rows, "ApproximateTotalCount": rows.len()}))
}

fn op_get_metric_data(st: &mut ConnectState, instance_id: &str, body: &Map<String, Value>) -> Response {
    for field in ["StartTime", "EndTime", "Filters", "HistoricalMetrics"] {
        if opt_val(body, field).is_none() {
            return missing_body_param(field);
        }
    }
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let requested = opt_val(body, "HistoricalMetrics").unwrap_or(json!([]));
    let results: Vec<Value> = requested
        .as_array()
        .map(|m| {
            m.iter()
                .map(|metric| {
                    json!({
                        "Dimensions": {},
                        "Collections": [{"Metric": metric.clone(), "Value": 0}],
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    ok(json!({"MetricResults": results}))
}

fn op_get_metric_data_v2(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    for field in ["ResourceArn", "StartTime", "EndTime", "Filters", "Metrics"] {
        if opt_val(body, field).is_none() {
            return missing_body_param(field);
        }
    }
    let _ = opt_string(body, "InstanceId");
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let requested = opt_val(body, "Metrics").unwrap_or(json!([]));
    let results: Vec<Value> = requested
        .as_array()
        .map(|m| {
            m.iter()
                .map(|metric| {
                    json!({
                        "Dimensions": {},
                        "Collections": [{"Metric": metric.clone(), "Value": 0}],
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    ok(json!({"MetricResults": results}))
}

fn op_list_realtime_contact_analysis_segments_v2(
    st: &mut ConnectState,
    instance_id: &str,
    contact_id: &str,
    body: &Map<String, Value>,
) -> Response {
    let _ = (opt_val(body, "OutputType"), opt_val(body, "SegmentTypes"));
    if let Err(resp) = contact_guard(st, instance_id, contact_id) {
        return resp;
    }
    ok(json!({"Channel": "VOICE", "Status": "COMPLETED", "Segments": []}))
}
fn op_search_contacts(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    let instance_id = instance_id.to_string();
    if opt_val(body, "TimeRange").is_none() {
        return missing_body_param("TimeRange");
    }
    if !st.instances.contains_key(&instance_id) {
        return notfound_instance(&instance_id);
    }
    let rows: Vec<Value> = st
        .contacts
        .get(&instance_id)
        .map(|m| m.values().map(contact_search_summary).collect())
        .unwrap_or_default();
    ok(json!({"Contacts": rows, "TotalCount": rows.len()}))
}

fn contact_search_summary(contact: &Value) -> Value {
    json!({
        "Arn": contact["Arn"],
        "Id": contact["Id"],
        "Channel": contact["Channel"],
        "InitiationMethod": contact["InitiationMethod"],
        "InitiationTimestamp": contact["InitiationTimestamp"],
        "Name": contact.get("Name").cloned().unwrap_or(json!("")),
    })
}

fn eval_search_summary(form: &Value) -> Value {
    json!({
        "EvaluationFormId": form["EvaluationFormId"],
        "EvaluationFormArn": form["EvaluationFormArn"],
        "EvaluationFormVersion": form["EvaluationFormVersion"],
        "EvaluationFormTitle": form["Title"],
        "Status": form["Status"],
        "CreatedTime": form["CreatedTime"],
        "LastModifiedTime": form["LastModifiedTime"],
    })
}

fn op_search_contact_evaluations(st: &mut ConnectState, instance_id: &str, body: &Map<String, Value>) -> Response {
    let instance_id = instance_id.to_string();
    if !st.instances.contains_key(&instance_id) {
        return notfound_instance(&instance_id);
    }
    let _ = body;
    let rows: Vec<Value> = st
        .contact_evaluations
        .get(&instance_id)
        .map(|m| m.values().map(evaluation_search_summary).collect())
        .unwrap_or_default();
    ok(json!({
        "EvaluationSearchSummaryList": rows,
        "ApproximateTotalCount": rows.len(),
    }))
}

fn evaluation_search_summary(evaluation: &Value) -> Value {
    json!({
        "EvaluationId": evaluation["EvaluationId"],
        "EvaluationArn": evaluation["EvaluationArn"],
        "EvaluationFormId": evaluation["EvaluationFormId"],
        "EvaluationFormVersion": 1,
        "Status": evaluation["Status"],
        "CreatedTime": evaluation.get("CreateTimestamp").cloned().unwrap_or(json!("")),
        "LastModifiedTime": evaluation["ModifiedTimestamp"],
        "Metadata": {
            "ContactId": evaluation["ContactId"],
            "EvaluatorArn": evaluation.get("EvaluatorArn").cloned().unwrap_or(json!("")),
        },
    })
}

fn op_search_resource_tags(st: &mut ConnectState, body: &Map<String, Value>) -> Response {
    let instance_id = match instance_guard_body(body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let Some(instance) = st.instances.get(&instance_id) else {
        return notfound_instance(&instance_id);
    };
    let prefix = str_field(instance, "Arn");
    let mut seen = std::collections::HashSet::new();
    let mut rows: Vec<Value> = Vec::new();
    for (arn, tags) in st.tags.iter() {
        if !arn.starts_with(&prefix) {
            continue;
        }
        for (key, value) in tags {
            if seen.insert(key.clone()) {
                rows.push(json!({"Key": key, "Value": value}));
            }
        }
    }
    rows.sort_by_key(|r| fmt_py(&r["Key"]));
    ok(json!({"Tags": rows}))
}

// -- Missed ops: contact-user association, phone contact-flow link,
//    default vocabulary, push notifications, integration association delete.

fn op_associate_contact_with_user(
    st: &mut ConnectState,
    instance_id: &str,
    contact_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if let Err(resp) = contact_guard(st, instance_id, contact_id) {
        return resp;
    }
    let user_id = match body_str(body, "UserId") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if let Err(resp) = user_guard(st, instance_id, &user_id) {
        return resp;
    }
    let record = st
        .contacts
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(contact_id))
        .unwrap();
    record["UserId"] = json!(user_id);
    record["LastUpdateTimestamp"] = json!(now_iso());
    ok(json!({}))
}
fn op_associate_default_vocabulary(
    st: &mut ConnectState,
    instance_id: &str,
    language_code: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    if let Some(vocabulary_id) = opt_string(body, "VocabularyId").filter(|v| !v.is_empty()) {
        let known = st
            .vocabularies
            .get(instance_id)
            .is_some_and(|m| m.contains_key(&vocabulary_id));
        if !known {
            return not_found(format!("Vocabulary {vocabulary_id} not found"));
        }
        st.default_vocabularies
            .entry(instance_id.to_string())
            .or_default()
            .insert(language_code.to_string(), json!(vocabulary_id));
    }
    ok(json!({}))
}

fn op_associate_phone_contact_flow(
    st: &mut ConnectState,
    phone_number_id: &str,
    body: &Map<String, Value>,
) -> Response {
    let instance_id = match body_str(body, "InstanceId") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let flow_id = match body_str(body, "ContactFlowId") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if !st.instances.contains_key(&instance_id) {
        return notfound_instance(&instance_id);
    }
    let has_flow = st
        .contact_flows
        .get(&instance_id)
        .map(|m| m.contains_key(&flow_id))
        .unwrap_or(false);
    if !has_flow {
        return not_found(format!("Contact flow {flow_id} not found"));
    }
    let has_phone = st.phone_numbers.contains_key(phone_number_id);
    if !has_phone {
        return not_found(format!("Phone number {phone_number_id} not found"));
    }
    st.phone_contact_flows
        .entry(instance_id)
        .or_default()
        .insert(phone_number_id.to_string(), json!(flow_id));
    ok(json!({}))
}

fn op_disassociate_phone_contact_flow(
    st: &mut ConnectState,
    phone_number_id: &str,
    body: &Map<String, Value>,
) -> Response {
    let _ = opt_string(body, "InstanceId");
    let present = st
        .phone_contact_flows
        .values()
        .any(|m| m.contains_key(phone_number_id));
    if !present {
        return not_found(format!(
            "Contact flow for phone number {phone_number_id} not found"
        ));
    }
    for map in st.phone_contact_flows.values_mut() {
        map.remove(phone_number_id);
    }
    ok(json!({}))
}

fn op_update_phone_number_metadata(
    st: &mut ConnectState,
    phone_number_id: &str,
    body: &Map<String, Value>,
) -> Response {
    let record = match st.phone_numbers.get_mut(phone_number_id) {
        Some(p) => p,
        None => return not_found(format!("Phone number {phone_number_id} not found")),
    };
    if let Some(desc) = opt_val(body, "PhoneNumberDescription") {
        record["PhoneNumberDescription"] = desc;
    }
    ok(json!({
        "PhoneNumberId": record["PhoneNumberId"].clone(),
        "PhoneNumberArn": record["PhoneNumberArn"].clone(),
    }))
}

fn op_import_phone_number(st: &mut ConnectState, body: &Map<String, Value>) -> Response {
    let instance_id = match body_str(body, "InstanceId") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let source_arn = match body_str(body, "SourcePhoneNumberArn") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let Some(instance) = st.instances.get(&instance_id) else {
        return notfound_instance(&instance_id);
    };
    let instance_arn = str_field(instance, "Arn");
    let phone_number = source_arn.rsplit('/').next().unwrap_or("").to_string();
    let phone_number_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{instance_arn}/phone-number/{phone_number_id}");
    let stored = json!({
        "PhoneNumberId": phone_number_id.clone(),
        "PhoneNumberArn": arn.clone(),
        "PhoneNumber": phone_number,
        "PhoneNumberCountryCode": "",
        "PhoneNumberType": "",
        "PhoneNumberDescription": opt_string(body, "PhoneNumberDescription").unwrap_or_default(),
        "TargetArn": instance_arn.clone(),
        "InstanceId": instance_id.clone(),
        "Tags": opt_val(body, "Tags").unwrap_or(json!({})),
        "PhoneNumberStatus": {"Status": "PENDING_ACTIVE"},
        "SourcePhoneNumberArn": source_arn,
    });
    st.phone_numbers.insert(phone_number_id.clone(), stored);
    if let Some(Value::Object(map)) = opt_val(body, "Tags") {
        st.tags
            .entry(arn.clone())
            .or_default()
            .extend(map.iter().map(|(k, v)| (k.clone(), v.clone())));
    }
    ok(json!({"PhoneNumberId": phone_number_id, "PhoneNumberArn": arn}))
}

fn op_create_push_notification_registration(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    for field in ["PinpointAppArn", "DeviceToken", "DeviceType", "ContactConfiguration"] {
        if opt_val(body, field).is_none() {
            return missing_body_param(field);
        }
    }
    let registration_id = uuid::Uuid::new_v4().to_string();
    let record = json!({
        "RegistrationId": registration_id.clone(),
        "PinpointAppArn": opt_val(body, "PinpointAppArn").unwrap_or(json!("")),
        "DeviceToken": opt_val(body, "DeviceToken").unwrap_or(json!("")),
        "DeviceType": opt_val(body, "DeviceType").unwrap_or(json!("")),
        "ContactConfiguration": opt_val(body, "ContactConfiguration").unwrap_or(json!({})),
    });
    st.push_notification_registrations
        .entry(instance_id.to_string())
        .or_default()
        .insert(registration_id.clone(), record);
    ok(json!({"RegistrationId": registration_id}))
}

fn op_delete_push_notification_registration(
    st: &mut ConnectState,
    instance_id: &str,
    registration_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .push_notification_registrations
        .get_mut(instance_id)
        .and_then(|m| m.remove(registration_id))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!(
            "Push notification registration {registration_id} not found"
        )),
    }
}

fn op_delete_integration_association(
    st: &mut ConnectState,
    instance_id: &str,
    association_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .integration_associations
        .get_mut(instance_id)
        .and_then(|m| m.remove(association_id))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!(
            "Integration association {association_id} not found"
        )),
    }
}

fn op_update_instance_storage_config(
    st: &mut ConnectState,
    instance_id: &str,
    association_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let Some(config) = opt_val(body, "StorageConfig") else {
        return missing_body_param("StorageConfig");
    };
    let fallback: &mut Vec<Value> = &mut Vec::new();
    let list = st
        .instance_storage_configs
        .get_mut(instance_id)
        .unwrap_or(fallback);
    let record = list.iter_mut().find(|c| {
        c.get("AssociationId").and_then(Value::as_str) == Some(association_id)
    });
    let Some(record) = record else {
        return not_found(format!(
            "Storage config {association_id} not found in instance {instance_id}"
        ));
    };
    record["StorageConfig"] = config;
    if let Some(resource_type) = opt_val(body, "ResourceType") {
        record["ResourceType"] = resource_type;
    }
    ok(json!({}))
}

fn op_describe_instance_storage_config(
    st: &mut ConnectState,
    instance_id: &str,
    association_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let record = st
        .instance_storage_configs
        .get(instance_id)
        .and_then(|v| {
            v.iter()
                .find(|c| {
                    c.get("AssociationId").and_then(Value::as_str) == Some(association_id)
                })
                .cloned()
        });
    let Some(record) = record else {
        return not_found(format!(
            "Storage config {association_id} not found in instance {instance_id}"
        ));
    };
    let mut config = record
        .get("StorageConfig")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    config.insert("AssociationId".into(), json!(association_id));
    config.insert(
        "ResourceType".into(),
        record.get("ResourceType").cloned().unwrap_or(json!("")),
    );
    ok(json!({"StorageConfig": config}))
}

fn op_replicate_instance(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
    region: &str,
    account: u64,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    for field in ["ReplicaRegion", "ReplicaAlias"] {
        if opt_val(body, field).is_none() {
            return missing_body_param(field);
        }
    }
    let alias = opt_string(body, "ReplicaAlias").unwrap_or_default();
    let id = uuid::Uuid::new_v4().to_string();
    let arn = format!("arn:aws:connect:{region}:{account}:instance/{id}");
    let access_url = format!("https://{alias}.my.connect.aws");
    st.instances.insert(
        id.clone(),
        json!({
            "Id": id.clone(),
            "Arn": arn.clone(),
            "Alias": alias,
            "AccessUrl": access_url,
            "CreatedTimestamp": now_iso(),
            "CreatedRegion": region,
        }),
    );
    ok(json!({"Id": id, "Arn": arn}))
}

fn op_get_federation_token(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let instance_arn = st
        .instances
        .get(instance_id)
        .map(|i| str_field(i, "Arn"))
        .unwrap_or_default();
    let user_id = uuid::Uuid::new_v4().to_string();
    ok(json!({
        "Credentials": {
            "AccessToken": uuid::Uuid::new_v4().to_string(),
            "AccessTokenExpiration": now_iso(),
            "RefreshToken": uuid::Uuid::new_v4().to_string(),
            "RefreshTokenExpiration": now_iso(),
        },
        "SignInUrl": format!("https://{instance_arn}/connect/federation/{user_id}"),
        "UserArn": format!("{instance_arn}/agent/{user_id}"),
        "UserId": user_id,
    }))
}

fn op_get_prompt_file(st: &mut ConnectState, instance_id: &str, prompt_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let record = match st.prompts.get(instance_id).and_then(|m| m.get(prompt_id)) {
        Some(p) => p.clone(),
        None => {
            return not_found(format!(
                "Prompt {prompt_id} not found in instance {instance_id}"
            ))
        }
    };
    ok(json!({
        "PromptPresignedUrl": "",
        "LastModifiedTime": record.get("LastModifiedTime").cloned().unwrap_or(json!("")),
        "LastModifiedRegion": record.get("LastModifiedRegion").cloned().unwrap_or(json!("")),
    }))
}

// -- Version families: flows, flow modules, views ------------------------------

fn op_create_contact_flow_version(st: &mut ConnectState, instance_id: &str, flow_id: &str) -> Response {
    if let Err(resp) = flow_guard(st, instance_id, flow_id) {
        return resp;
    }
    let list = st
        .contact_flow_versions
        .entry(instance_id.to_string())
        .or_default();
    let versions = list.entry(flow_id.to_string()).or_default();
    seed_versions(versions);
    let next = versions.len() as i64 + 1;
    versions.push(next);
    let arn = st
        .contact_flows
        .get(instance_id)
        .and_then(|m| m.get(flow_id))
        .map(|f| str_field(f, "Arn"))
        .unwrap_or_default();
    ok(json!({"ContactFlowArn": arn, "Version": next}))
}

fn flow_guard(st: &ConnectState, instance_id: &str, flow_id: &str) -> Result<(), Response> {
    if !st.instances.contains_key(instance_id) {
        return Err(notfound_instance(instance_id));
    }
    if st
        .contact_flows
        .get(instance_id)
        .and_then(|m| m.get(flow_id))
        .is_none()
    {
        return Err(not_found(format!(
            "Contact flow {flow_id} not found in instance {instance_id}"
        )));
    }
    Ok(())
}

fn seed_versions(map: &mut Vec<i64>) {
    if map.is_empty() {
        map.push(1);
    }
}

fn op_delete_contact_flow_version(
    st: &mut ConnectState,
    instance_id: &str,
    flow_id: &str,
    version: &str,
) -> Response {
    if let Err(resp) = flow_guard(st, instance_id, flow_id) {
        return resp;
    }
    let Ok(version) = version.parse::<i64>() else {
        return invalid(format!("ContactFlowVersion {version} must be an integer"));
    };
    let versions = st
        .contact_flow_versions
        .get(instance_id)
        .and_then(|m| m.get(flow_id))
        .cloned()
        .unwrap_or_default();
    if !versions.contains(&version) {
        return not_found(format!("Contact flow version {version} not found"));
    }
    if versions.len() <= 1 {
        return invalid("The contact flow version you want to delete is the live version");
    }
    if let Some(list) = st
        .contact_flow_versions
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(flow_id))
    {
        list.retain(|v| *v != version);
    }
    ok(json!({}))
}

fn op_create_contact_flow_module_version(
    st: &mut ConnectState,
    instance_id: &str,
    module_id: &str,
) -> Response {
    if let Err(resp) = module_guard(st, instance_id, module_id) {
        return resp;
    }
    let versions = st
        .contact_flow_module_versions
        .entry(instance_id.to_string())
        .or_default()
        .entry(module_id.to_string())
        .or_default();
    seed_versions(versions);
    let next = versions.len() as i64 + 1;
    versions.push(next);
    let arn = st
        .contact_flow_modules
        .get(instance_id)
        .and_then(|m| m.get(module_id))
        .map(|m| str_field(m, "Arn"))
        .unwrap_or_default();
    ok(json!({"ContactFlowModuleArn": arn, "Version": next}))
}

fn module_guard(st: &ConnectState, instance_id: &str, module_id: &str) -> Result<(), Response> {
    if !st.instances.contains_key(instance_id) {
        return Err(notfound_instance(instance_id));
    }
    if st
        .contact_flow_modules
        .get(instance_id)
        .and_then(|m| m.get(module_id))
        .is_none()
    {
        return Err(not_found(format!(
            "Contact flow module {module_id} not found in instance {instance_id}"
        )));
    }
    Ok(())
}

fn op_delete_contact_flow_module_version(
    st: &mut ConnectState,
    instance_id: &str,
    module_id: &str,
    version: &str,
) -> Response {
    if let Err(resp) = module_guard(st, instance_id, module_id) {
        return resp;
    }
    let Ok(version) = version.parse::<i64>() else {
        return invalid(format!(
            "ContactFlowModuleVersion {version} must be an integer"
        ));
    };
    let versions = st
        .contact_flow_module_versions
        .get(instance_id)
        .and_then(|m| m.get(module_id))
        .cloned()
        .unwrap_or_default();
    if !versions.contains(&version) {
        return not_found(format!("Contact flow module version {version} not found"));
    }
    if versions.len() <= 1 {
        return invalid("The contact flow module version you want to delete is the live version");
    }
    if let Some(list) = st
        .contact_flow_module_versions
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(module_id))
    {
        list.retain(|v| *v != version);
    }
    ok(json!({}))
}

fn op_create_view_version(
    st: &mut ConnectState,
    instance_id: &str,
    view_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if let Err(resp) = view_guard(st, instance_id, view_id) {
        return resp;
    }
    let _ = opt_val(body, "VersionDescription");
    let versions = st
        .view_versions
        .entry(instance_id.to_string())
        .or_default()
        .entry(view_id.to_string())
        .or_default();
    seed_versions(versions);
    let next = versions.len() as i64 + 1;
    versions.push(next);
    let view = st
        .views
        .get(instance_id)
        .and_then(|m| m.get(view_id))
        .cloned()
        .unwrap_or_default();
    let mut stored = view.as_object().cloned().unwrap_or_default();
    stored.insert("ViewVersion".into(), json!(next));
    ok(json!({"View": stored}))
}

fn view_guard(st: &ConnectState, instance_id: &str, view_id: &str) -> Result<(), Response> {
    if !st.instances.contains_key(instance_id) {
        return Err(notfound_instance(instance_id));
    }
    if st
        .views
        .get(instance_id)
        .and_then(|m| m.get(view_id))
        .is_none()
    {
        return Err(not_found(format!(
            "View {view_id} not found in instance {instance_id}"
        )));
    }
    Ok(())
}

fn op_delete_view_version(
    st: &mut ConnectState,
    instance_id: &str,
    view_id: &str,
    version: &str,
) -> Response {
    if let Err(resp) = view_guard(st, instance_id, view_id) {
        return resp;
    }
    let Ok(version) = version.parse::<i64>() else {
        return invalid(format!("ViewVersion {version} must be an integer"));
    };
    let versions = st
        .view_versions
        .get(instance_id)
        .and_then(|m| m.get(view_id))
        .cloned()
        .unwrap_or_default();
    if !versions.contains(&version) {
        return not_found(format!("View version {version} not found"));
    }
    if versions.len() <= 1 {
        return invalid("The view version you want to delete is the live version");
    }
    if let Some(list) = st
        .view_versions
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(view_id))
    {
        list.retain(|v| *v != version);
    }
    ok(json!({}))
}

fn op_list_view_versions(st: &mut ConnectState, instance_id: &str, view_id: &str) -> Response {
    if let Err(resp) = view_guard(st, instance_id, view_id) {
        return resp;
    }
    let view = st
        .views
        .get(instance_id)
        .and_then(|m| m.get(view_id))
        .cloned()
        .unwrap_or_default();
    let versions = st
        .view_versions
        .get(instance_id)
        .and_then(|m| m.get(view_id))
        .cloned()
        .unwrap_or_default();
    let list: Vec<i64> = if versions.is_empty() { vec![1] } else { versions };
    let rows: Vec<Value> = list
        .into_iter()
        .map(|version| {
            json!({
                "Id": view["Id"],
                "Arn": view["Arn"],
                "Name": view["Name"],
                "Type": view.get("Type").cloned().unwrap_or(json!("CUSTOMER_MANAGED")),
                "Version": version,
                "VersionDescription": view.get("Description").cloned().unwrap_or(json!("")),
            })
        })
        .collect();
    ok(json!({"ViewVersionSummaryList": rows}))
}

fn op_list_contact_flow_module_versions(
    st: &mut ConnectState,
    instance_id: &str,
    module_id: &str,
) -> Response {
    if let Err(resp) = module_guard(st, instance_id, module_id) {
        return resp;
    }
    let module = st
        .contact_flow_modules
        .get(instance_id)
        .and_then(|m| m.get(module_id))
        .cloned()
        .unwrap_or_default();
    let versions = st
        .contact_flow_module_versions
        .get(instance_id)
        .and_then(|m| m.get(module_id))
        .cloned()
        .unwrap_or_default();
    let list: Vec<i64> = if versions.is_empty() { vec![1] } else { versions };
    let rows: Vec<Value> = list
        .into_iter()
        .map(|version| {
            json!({
                "Arn": module["Arn"],
                "VersionDescription": module.get("Description").cloned().unwrap_or(json!("")),
                "Version": version,
            })
        })
        .collect();
    ok(json!({"ContactFlowModuleVersionSummaryList": rows}))
}

// -- Contact evaluations (submit) + test-case executions ----------------------

fn op_submit_contact_evaluation(
    st: &mut ConnectState,
    instance_id: &str,
    evaluation_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let record = st
        .contact_evaluations
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(evaluation_id));
    let Some(record) = record else {
        return not_found(format!("Evaluation {evaluation_id} not found"));
    };
    record["Status"] = json!("SUBMITTED");
    ok(json!({
        "EvaluationId": record["EvaluationId"].clone(),
        "EvaluationArn": record["EvaluationArn"].clone(),
    }))
}

fn op_start_test_case_execution(
    st: &mut ConnectState,
    instance_id: &str,
    case_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    if st
        .test_cases
        .get(instance_id)
        .and_then(|m| m.get(case_id))
        .is_none()
    {
        return not_found(format!("Test case {case_id} not found"));
    }
    let execution_id = uuid::Uuid::new_v4().to_string();
    let record = json!({
        "TestCaseExecutionId": execution_id.clone(),
        "TestCaseId": case_id,
        "Status": "IN_PROGRESS",
        "StartTime": now_iso(),
        "EndTime": "",
    });
    st.test_case_executions
        .entry(instance_id.to_string())
        .or_default()
        .entry(case_id.to_string())
        .or_default()
        .push(record);
    ok(json!({
        "TestCaseExecutionId": execution_id,
        "TestCaseId": case_id,
        "Status": "IN_PROGRESS",
    }))
}

fn op_stop_test_case_execution(
    st: &mut ConnectState,
    instance_id: &str,
    case_id: &str,
    execution_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let records = st
        .test_case_executions
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(case_id));
    let Some(records) = records else {
        return not_found(format!("Test case execution {execution_id} not found"));
    };
    let record = records.iter_mut().find(|r| {
        r.get("TestCaseExecutionId").and_then(Value::as_str) == Some(execution_id)
    });
    let Some(record) = record else {
        return not_found(format!("Test case execution {execution_id} not found"));
    };
    record["Status"] = json!("STOPPED");
    record["EndTime"] = json!(now_iso());
    ok(json!({}))
}

fn op_list_test_case_executions(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let rows: Vec<Value> = st
        .test_case_executions
        .get(instance_id)
        .map(|m| m.values().flatten().cloned().collect())
        .unwrap_or_default();
    ok(json!({"TestCaseExecutions": rows}))
}

fn op_list_test_case_execution_records(
    st: &mut ConnectState,
    instance_id: &str,
    case_id: &str,
    execution_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let known = st
        .test_case_executions
        .get(instance_id)
        .and_then(|m| m.get(case_id))
        .is_some_and(|v| {
            v.iter().any(|r| {
                r.get("TestCaseExecutionId").and_then(Value::as_str) == Some(execution_id)
            })
        });
    if !known {
        return not_found(format!(
            "Test case execution {execution_id} not found"
        ));
    }
    ok(json!({"ExecutionRecords": []}))
}

fn op_get_test_case_execution_summary(
    st: &mut ConnectState,
    instance_id: &str,
    case_id: &str,
    execution_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let record = st
        .test_case_executions
        .get(instance_id)
        .and_then(|m| m.get(case_id))
        .and_then(|v| {
            v.iter()
                .find(|r| {
                    r.get("TestCaseExecutionId").and_then(Value::as_str) == Some(execution_id)
                })
                .cloned()
        });
    let Some(record) = record else {
        return not_found(format!("Test case execution {execution_id} not found"));
    };
    ok(json!({
        "StartTime": record["StartTime"],
        "EndTime": record["EndTime"],
        "Status": record["Status"],
    }))
}


fn op_list_associated_contacts(
    st: &mut ConnectState,
    instance_id: &str,
    query: &BTreeMap<String, Vec<String>>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let Some(contact_id) = q1(query, "ContactId").filter(|c| !c.is_empty()) else {
        return missing_body_param("ContactId");
    };
    if let Err(resp) = contact_guard(st, instance_id, contact_id) {
        return resp;
    }
    let rows: Vec<Value> = st
        .contacts
        .get(instance_id)
        .map(|m| {
            m.values()
                .filter(|c| {
                    c.get("RelatedContactId").cloned().unwrap_or(json!("")) == json!(contact_id)
                })
                .map(contact_search_summary)
                .collect()
        })
        .unwrap_or_default();
    ok(json!({"ContactSummaryList": rows}))
}
