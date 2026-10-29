
// ---------------------------------------------------------------------------
// Task templates
// ---------------------------------------------------------------------------

fn task_template_to_dict(v: &Value) -> Value {
    let mut out = Map::new();
    for f in [
        "Id",
        "Arn",
        "Name",
        "Description",
        "Fields",
        "Defaults",
        "Constraints",
        "Status",
        "Tags",
        "CreatedTime",
        "LastModifiedTime",
        "InstanceId",
    ] {
        out.insert(f.to_string(), v[f].clone());
    }
    let has_cf = v
        .get("ContactFlowId")
        .and_then(Value::as_str)
        .map(|s| !s.is_empty())
        .unwrap_or(false);
    if has_cf {
        out.insert("ContactFlowId".into(), v["ContactFlowId"].clone());
    }
    Value::Object(out)
}

fn task_template_mut(
    st: &mut ConnectState,
    instance_id: &str,
    template_id: &str,
) -> Result<&mut Value, Response> {
    if !st.instances.contains_key(instance_id) {
        return Err(notfound_instance(instance_id));
    }
    match st
        .task_templates
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(template_id))
    {
        Some(t) => Ok(t),
        None => Err(not_found(format!("Task template {template_id} not found"))),
    }
}

fn op_create_task_template(st: &mut ConnectState, instance_id: &str, body: &Map<String, Value>) -> Response {
    let instance_arn = match instance_arn_or_404(st, instance_id) {
        Some(a) => a,
        None => return notfound_instance(instance_id),
    };
    let name = match body_str(body, "Name") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let template_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{instance_arn}/task-template/{template_id}");
    let stamp = now_iso();
    let mut stored = Map::new();
    stored.insert("Id".into(), json!(template_id.clone()));
    stored.insert("Arn".into(), json!(arn.clone()));
    stored.insert("Name".into(), json!(name));
    stored.insert(
        "Description".into(),
        json!(opt_string(body, "Description").unwrap_or_default()),
    );
    stored.insert("Fields".into(), opt_val(body, "Fields").unwrap_or_else(|| json!([])));
    stored.insert("Defaults".into(), opt_val(body, "Defaults").unwrap_or_else(|| json!({})));
    stored.insert(
        "Constraints".into(),
        opt_val(body, "Constraints").unwrap_or_else(|| json!({})),
    );
    stored.insert(
        "Status".into(),
        json!(opt_string(body, "Status").unwrap_or_else(|| "ACTIVE".into())),
    );
    stored.insert("Tags".into(), opt_val(body, "Tags").unwrap_or_else(|| json!({})));
    stored.insert("CreatedTime".into(), json!(stamp.clone()));
    stored.insert("LastModifiedTime".into(), json!(stamp));
    stored.insert(
        "InstanceId".into(),
        json!(instance_arn.rsplit('/').next().unwrap_or("")),
    );
    if let Some(cf) = opt_val(body, "ContactFlowId").filter(|v| !v.as_str().unwrap_or_default().is_empty()) {
        stored.insert("ContactFlowId".into(), cf);
    }
    st.task_templates
        .entry(instance_id.to_string())
        .or_default()
        .insert(template_id.clone(), Value::Object(stored));
    ok(json!({"Id": template_id, "Arn": arn}))
}

fn op_get_task_template(st: &mut ConnectState, instance_id: &str, template_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .task_templates
        .get(instance_id)
        .and_then(|m| m.get(template_id))
    {
        Some(t) => ok(task_template_to_dict(t)),
        None => not_found(format!("Task template {template_id} not found")),
    }
}

fn op_update_task_template(
    st: &mut ConnectState,
    instance_id: &str,
    template_id: &str,
    body: &Map<String, Value>,
) -> Response {
    let template = match task_template_mut(st, instance_id, template_id) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    if let Some(name) = opt_val(body, "Name") {
        template["Name"] = name;
    }
    if let Some(desc) = opt_val(body, "Description") {
        template["Description"] = desc;
    }
    if let Some(fields) = opt_val(body, "Fields") {
        template["Fields"] = fields;
    }
    if let Some(defaults) = opt_val(body, "Defaults") {
        template["Defaults"] = defaults;
    }
    if let Some(constraints) = opt_val(body, "Constraints") {
        template["Constraints"] = constraints;
    }
    if let Some(cf) = opt_val(body, "ContactFlowId") {
        template["ContactFlowId"] = cf;
    }
    if let Some(status) = opt_val(body, "Status") {
        template["Status"] = status;
    }
    template["LastModifiedTime"] = json!(now_iso());
    ok(task_template_to_dict(template))
}

fn op_delete_task_template(st: &mut ConnectState, instance_id: &str, template_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .task_templates
        .get_mut(instance_id)
        .and_then(|m| m.remove(template_id))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!("Task template {template_id} not found")),
    }
}

fn op_list_task_templates(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let empty = HashMap::new();
    let templates = st.task_templates.get(instance_id).unwrap_or(&empty);
    let rows: Vec<Value> = templates
        .values()
        .map(|t| {
            json!({
                "Id": t["Id"],
                "Arn": t["Arn"],
                "Name": t["Name"],
                "Description": t["Description"],
                "Status": t["Status"],
                "CreatedTime": t["CreatedTime"],
                "LastModifiedTime": t["LastModifiedTime"],
            })
        })
        .collect();
    ok(json!({"TaskTemplates": rows}))
}

// ---------------------------------------------------------------------------
// Integration associations + use cases
// ---------------------------------------------------------------------------

fn op_create_integration_association(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    let instance = match st.instances.get(instance_id) {
        Some(i) => i.clone(),
        None => return notfound_instance(instance_id),
    };
    let integration_type = match body_str(body, "IntegrationType") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let integration_arn = match body_str(body, "IntegrationArn") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let assoc_id = uuid::Uuid::new_v4().to_string();
    let arn = format!(
        "{}/integration-association/{assoc_id}",
        str_field(&instance, "Arn")
    );
    let mut stored = Map::new();
    stored.insert("IntegrationAssociationId".into(), json!(assoc_id.clone()));
    stored.insert("IntegrationAssociationArn".into(), json!(arn.clone()));
    stored.insert(
        "InstanceId".into(),
        json!(str_field(&instance, "Arn").rsplit('/').next().unwrap_or("")),
    );
    stored.insert("IntegrationType".into(), json!(integration_type));
    stored.insert("IntegrationArn".into(), json!(integration_arn));
    stored.insert(
        "SourceApplicationUrl".into(),
        json!(opt_string(body, "SourceApplicationUrl").unwrap_or_default()),
    );
    stored.insert(
        "SourceApplicationName".into(),
        json!(opt_string(body, "SourceApplicationName").unwrap_or_default()),
    );
    stored.insert(
        "SourceType".into(),
        json!(opt_string(body, "SourceType").unwrap_or_default()),
    );
    st.integration_associations
        .entry(instance_id.to_string())
        .or_default()
        .insert(assoc_id.clone(), Value::Object(stored));
    tag_resource(st, &arn, opt_val(body, "Tags"));
    ok(json!({"IntegrationAssociationId": assoc_id, "IntegrationAssociationArn": arn}))
}

fn op_delete_integration_association(
    st: &mut ConnectState,
    instance_id: &str,
    assoc_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .integration_associations
        .get_mut(instance_id)
        .and_then(|m| m.remove(assoc_id))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!("Integration association {assoc_id} not found")),
    }
}

fn op_list_integration_associations(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let empty = HashMap::new();
    let assocs = st.integration_associations.get(instance_id).unwrap_or(&empty);
    // moto mirrors the to_dict entries in the summary list
    let rows: Vec<Value> = assocs.values().cloned().collect();
    ok(json!({"IntegrationAssociationSummaryList": rows}))
}

fn op_create_use_case(
    st: &mut ConnectState,
    instance_id: &str,
    integration_association_id: &str,
    body: &Map<String, Value>,
) -> Response {
    let instance = match st.instances.get(instance_id) {
        Some(i) => i.clone(),
        None => return notfound_instance(instance_id),
    };
    let use_case_type = match body_str(body, "UseCaseType") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let uc_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{}/use-case/{uc_id}", str_field(&instance, "Arn"));
    let mut stored = Map::new();
    stored.insert("UseCaseId".into(), json!(uc_id.clone()));
    stored.insert("UseCaseArn".into(), json!(arn.clone()));
    stored.insert("UseCaseType".into(), json!(use_case_type));
    stored.insert(
        "__integration_association_id".into(),
        json!(integration_association_id),
    );
    st.use_cases
        .entry(instance_id.to_string())
        .or_default()
        .insert(uc_id.clone(), Value::Object(stored));
    ok(json!({"UseCaseId": uc_id, "UseCaseArn": arn}))
}

fn op_delete_use_case(st: &mut ConnectState, instance_id: &str, use_case_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .use_cases
        .get_mut(instance_id)
        .and_then(|m| m.remove(use_case_id))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!("Use case {use_case_id} not found")),
    }
}

fn op_list_use_cases(
    st: &mut ConnectState,
    instance_id: &str,
    integration_association_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let empty = HashMap::new();
    let ucs = st.use_cases.get(instance_id).unwrap_or(&empty);
    let rows: Vec<Value> = ucs
        .values()
        .filter(|uc| {
            uc.get("__integration_association_id") == Some(&Value::String(integration_association_id.to_string()))
        })
        .map(|uc| {
            json!({
                "UseCaseId": uc["UseCaseId"],
                "UseCaseArn": uc["UseCaseArn"],
                "UseCaseType": uc["UseCaseType"],
            })
        })
        .collect();
    ok(json!({"UseCaseSummaryList": rows}))
}

// ---------------------------------------------------------------------------
// Email addresses
// ---------------------------------------------------------------------------

fn op_create_email_address(st: &mut ConnectState, instance_id: &str, body: &Map<String, Value>) -> Response {
    let instance = match st.instances.get(instance_id) {
        Some(i) => i.clone(),
        None => return notfound_instance(instance_id),
    };
    let email_address = match body_str(body, "EmailAddress") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let email_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{}/email-address/{email_id}", str_field(&instance, "Arn"));
    let stamp = now_iso();
    let mut stored = Map::new();
    stored.insert("EmailAddressId".into(), json!(email_id.clone()));
    stored.insert("EmailAddressArn".into(), json!(arn.clone()));
    stored.insert("EmailAddress".into(), json!(email_address));
    stored.insert(
        "DisplayName".into(),
        json!(opt_string(body, "DisplayName").unwrap_or_default()),
    );
    stored.insert(
        "Description".into(),
        json!(opt_string(body, "Description").unwrap_or_default()),
    );
    stored.insert("Tags".into(), opt_val(body, "Tags").unwrap_or_else(|| json!({})));
    stored.insert("CreateTimestamp".into(), json!(stamp.clone()));
    stored.insert("ModifiedTimestamp".into(), json!(stamp));
    st.email_addresses
        .entry(instance_id.to_string())
        .or_default()
        .insert(email_id.clone(), Value::Object(stored));
    tag_resource(st, &arn, opt_val(body, "Tags"));
    ok(json!({"EmailAddressId": email_id, "EmailAddressArn": arn}))
}

fn op_describe_email_address(st: &mut ConnectState, instance_id: &str, email_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .email_addresses
        .get(instance_id)
        .and_then(|m| m.get(email_id))
    {
        // moto returns the bare to_dict for DescribeEmailAddress
        Some(e) => ok(e.clone()),
        None => not_found(format!("Email address {email_id} not found")),
    }
}

fn op_delete_email_address(st: &mut ConnectState, instance_id: &str, email_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .email_addresses
        .get_mut(instance_id)
        .and_then(|m| m.remove(email_id))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!("Email address {email_id} not found")),
    }
}

// ---------------------------------------------------------------------------
// Notifications
// ---------------------------------------------------------------------------

fn op_create_notification(st: &mut ConnectState, instance_id: &str, body: &Map<String, Value>) -> Response {
    let instance = match st.instances.get(instance_id) {
        Some(i) => i.clone(),
        None => return notfound_instance(instance_id),
    };
    let recipients = match opt_val(body, "Recipients") {
        Some(v) => v,
        None => return missing_body_param("Recipients"),
    };
    let content = match opt_val(body, "Content") {
        Some(v) => v,
        None => return missing_body_param("Content"),
    };
    let notification_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{}/notification/{notification_id}", str_field(&instance, "Arn"));
    let stamp = now_iso();
    let mut stored = Map::new();
    stored.insert("Id".into(), json!(notification_id.clone()));
    stored.insert("Arn".into(), json!(arn.clone()));
    stored.insert("Content".into(), content);
    stored.insert("Recipients".into(), recipients);
    stored.insert("CreatedAt".into(), json!(stamp.clone()));
    stored.insert("LastModifiedTime".into(), json!(stamp.clone()));
    stored.insert("LastModifiedRegion".into(), json!(region_of(&instance)));
    stored.insert("Tags".into(), opt_val(body, "Tags").unwrap_or_else(|| json!({})));
    if let Some(priority) = opt_val(body, "Priority").filter(|v| !v.is_null()) {
        stored.insert("Priority".into(), priority);
    }
    if let Some(expires) = opt_val(body, "ExpiresAt").filter(|v| !v.is_null()) {
        stored.insert("ExpiresAt".into(), expires);
    }
    st.notifications
        .entry(instance_id.to_string())
        .or_default()
        .insert(notification_id.clone(), Value::Object(stored));
    ok(json!({"NotificationId": notification_id, "NotificationArn": arn}))
}

fn op_describe_notification(st: &mut ConnectState, instance_id: &str, notification_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .notifications
        .get(instance_id)
        .and_then(|m| m.get(notification_id))
    {
        // moto returns the bare to_dict for DescribeNotification
        Some(n) => ok(n.clone()),
        None => not_found(format!("Notification {notification_id} not found")),
    }
}

fn op_delete_notification(st: &mut ConnectState, instance_id: &str, notification_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .notifications
        .get_mut(instance_id)
        .and_then(|m| m.remove(notification_id))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!("Notification {notification_id} not found")),
    }
}

fn op_list_notifications(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let empty = HashMap::new();
    let rows: Vec<Value> = st
        .notifications
        .get(instance_id)
        .unwrap_or(&empty)
        .values()
        .cloned()
        .collect();
    ok(json!({"NotificationSummaryList": rows}))
}

// ---------------------------------------------------------------------------
// Workspaces
// ---------------------------------------------------------------------------

fn op_create_workspace(st: &mut ConnectState, instance_id: &str, body: &Map<String, Value>) -> Response {
    let instance = match st.instances.get(instance_id) {
        Some(i) => i.clone(),
        None => return notfound_instance(instance_id),
    };
    let name = match body_str(body, "Name") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let workspace_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{}/workspace/{workspace_id}", str_field(&instance, "Arn"));
    let stamp = now_iso();
    let mut stored = Map::new();
    stored.insert("WorkspaceId".into(), json!(workspace_id.clone()));
    stored.insert("WorkspaceArn".into(), json!(arn.clone()));
    stored.insert("Name".into(), json!(name));
    stored.insert(
        "Description".into(),
        json!(opt_string(body, "Description").unwrap_or_default()),
    );
    stored.insert("Tags".into(), opt_val(body, "Tags").unwrap_or_else(|| json!({})));
    stored.insert("CreateTimestamp".into(), json!(stamp.clone()));
    stored.insert("ModifiedTimestamp".into(), json!(stamp));
    st.workspaces
        .entry(instance_id.to_string())
        .or_default()
        .insert(workspace_id.clone(), Value::Object(stored));
    tag_resource(st, &arn, opt_val(body, "Tags"));
    ok(json!({"WorkspaceId": workspace_id, "WorkspaceArn": arn}))
}

fn op_describe_workspace(st: &mut ConnectState, instance_id: &str, workspace_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .workspaces
        .get(instance_id)
        .and_then(|m| m.get(workspace_id))
    {
        Some(w) => ok(json!({"Workspace": w.clone()})),
        None => not_found(format!("Workspace {workspace_id} not found")),
    }
}

fn op_delete_workspace(st: &mut ConnectState, instance_id: &str, workspace_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .workspaces
        .get_mut(instance_id)
        .and_then(|m| m.remove(workspace_id))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!("Workspace {workspace_id} not found")),
    }
}

fn op_list_workspaces(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let empty = HashMap::new();
    let rows: Vec<Value> = st
        .workspaces
        .get(instance_id)
        .unwrap_or(&empty)
        .values()
        .cloned()
        .collect();
    ok(json!({"WorkspaceSummaryList": rows}))
}

// ---------------------------------------------------------------------------
// Test cases
// ---------------------------------------------------------------------------

fn op_create_test_case(st: &mut ConnectState, instance_id: &str, body: &Map<String, Value>) -> Response {
    let instance = match st.instances.get(instance_id) {
        Some(i) => i.clone(),
        None => return notfound_instance(instance_id),
    };
    let name = match body_str(body, "Name") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let case_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{}/test-case/{case_id}", str_field(&instance, "Arn"));
    let stamp = now_iso();
    let mut stored = Map::new();
    stored.insert("TestCaseId".into(), json!(case_id.clone()));
    stored.insert("TestCaseArn".into(), json!(arn.clone()));
    stored.insert("Name".into(), json!(name));
    stored.insert(
        "Status".into(),
        json!(opt_string(body, "Status").unwrap_or_else(|| "DRAFT".into())),
    );
    stored.insert(
        "Description".into(),
        json!(opt_string(body, "Description").unwrap_or_default()),
    );
    stored.insert("Tags".into(), opt_val(body, "Tags").unwrap_or_else(|| json!({})));
    stored.insert("CreateTimestamp".into(), json!(stamp.clone()));
    stored.insert("ModifiedTimestamp".into(), json!(stamp));
    st.test_cases
        .entry(instance_id.to_string())
        .or_default()
        .insert(case_id.clone(), Value::Object(stored));
    ok(json!({"TestCaseId": case_id, "TestCaseArn": arn}))
}

fn op_describe_test_case(st: &mut ConnectState, instance_id: &str, case_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st.test_cases.get(instance_id).and_then(|m| m.get(case_id)) {
        Some(c) => ok(json!({"TestCase": c.clone()})),
        None => not_found(format!("Test case {case_id} not found")),
    }
}

fn op_update_test_case(
    st: &mut ConnectState,
    instance_id: &str,
    case_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let case = match st.test_cases.get_mut(instance_id).and_then(|m| m.get_mut(case_id)) {
        Some(c) => c,
        None => return not_found(format!("Test case {case_id} not found")),
    };
    if let Some(name) = opt_val(body, "Name") {
        case["Name"] = name;
    }
    if let Some(status) = opt_val(body, "Status") {
        case["Status"] = status;
    }
    if let Some(desc) = opt_val(body, "Description") {
        case["Description"] = desc;
    }
    if let Some(tags) = opt_val(body, "Tags") {
        case["Tags"] = tags;
    }
    case["ModifiedTimestamp"] = json!(now_iso());
    ok(json!({"TestCase": case.clone()}))
}

fn op_delete_test_case(st: &mut ConnectState, instance_id: &str, case_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st.test_cases.get_mut(instance_id).and_then(|m| m.remove(case_id)) {
        Some(_) => ok(json!({})),
        None => not_found(format!("Test case {case_id} not found")),
    }
}

fn op_list_test_cases(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let empty = HashMap::new();
    let rows: Vec<Value> = st
        .test_cases
        .get(instance_id)
        .unwrap_or(&empty)
        .values()
        .cloned()
        .collect();
    ok(json!({"TestCaseSummaryList": rows}))
}

// ---------------------------------------------------------------------------
// Data tables + attributes
// ---------------------------------------------------------------------------

fn op_create_data_table(st: &mut ConnectState, instance_id: &str, body: &Map<String, Value>) -> Response {
    let instance = match st.instances.get(instance_id) {
        Some(i) => i.clone(),
        None => return notfound_instance(instance_id),
    };
    let name = match body_str(body, "Name") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let table_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{}/data-table/{table_id}", str_field(&instance, "Arn"));
    let stamp = now_iso();
    let mut stored = Map::new();
    stored.insert("DataTableId".into(), json!(table_id.clone()));
    stored.insert("DataTableArn".into(), json!(arn.clone()));
    stored.insert("Name".into(), json!(name));
    stored.insert(
        "Schema".into(),
        opt_val(body, "Schema").unwrap_or_else(|| json!({})),
    );
    stored.insert("Tags".into(), opt_val(body, "Tags").unwrap_or_else(|| json!({})));
    stored.insert("CreateTimestamp".into(), json!(stamp.clone()));
    stored.insert("ModifiedTimestamp".into(), json!(stamp));
    st.data_tables
        .entry(instance_id.to_string())
        .or_default()
        .insert(table_id.clone(), Value::Object(stored));
    tag_resource(st, &arn, opt_val(body, "Tags"));
    ok(json!({
        "Id": table_id,
        "Arn": arn,
        "LockVersion": {"DataTable": "1", "Attribute": "1", "PrimaryValues": "1", "Value": "1"},
    }))
}

fn op_describe_data_table(st: &mut ConnectState, instance_id: &str, table_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st.data_tables.get(instance_id).and_then(|m| m.get(table_id)) {
        Some(t) => ok(json!({"DataTable": t.clone()})),
        None => not_found(format!("Data table {table_id} not found")),
    }
}

fn op_delete_data_table(st: &mut ConnectState, instance_id: &str, table_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st.data_tables.get_mut(instance_id).and_then(|m| m.remove(table_id)) {
        Some(_) => {
            st.data_table_attributes
                .get_mut(instance_id)
                .and_then(|m| m.remove(table_id));
            ok(json!({}))
        }
        None => not_found(format!("Data table {table_id} not found")),
    }
}

fn op_list_data_tables(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let empty = HashMap::new();
    let rows: Vec<Value> = st
        .data_tables
        .get(instance_id)
        .unwrap_or(&empty)
        .values()
        .cloned()
        .collect();
    ok(json!({"DataTableSummaryList": rows}))
}

fn op_create_data_table_attribute(
    st: &mut ConnectState,
    instance_id: &str,
    table_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    if st
        .data_tables
        .get(instance_id)
        .and_then(|m| m.get(table_id))
        .is_none()
    {
        return not_found(format!("Data table {table_id} not found"));
    }
    let name = match body_str(body, "Name") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let value_type = match body_str(body, "ValueType") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let attribute_id = uuid::Uuid::new_v4().to_string();
    let stamp = now_iso();
    let mut stored = Map::new();
    stored.insert("AttributeName".into(), json!(name.clone()));
    stored.insert("DataType".into(), json!(value_type));
    stored.insert(
        "Required".into(),
        json!(body.get("Required").and_then(Value::as_bool).unwrap_or(false)),
    );
    if let Some(dv) = opt_val(body, "DefaultValue").filter(|v| !v.is_null()) {
        stored.insert("DefaultValue".into(), dv);
    }
    stored.insert("CreateTimestamp".into(), json!(stamp.clone()));
    stored.insert("ModifiedTimestamp".into(), json!(stamp));
    st.data_table_attributes
        .entry(instance_id.to_string())
        .or_default()
        .entry(table_id.to_string())
        .or_default()
        .insert(name, Value::Object(stored));
    ok(json!({"Name": name, "AttributeId": attribute_id, "LockVersion": {}}))
}

fn data_table_attr_to_dict(a: &Value) -> Value {
    let mut out = Map::new();
    for f in ["AttributeName", "DataType", "Required", "CreateTimestamp", "ModifiedTimestamp"] {
        out.insert(f.to_string(), a[f].clone());
    }
    if let Some(dv) = a.get("DefaultValue").filter(|v| !v.is_null()) {
        out.insert("DefaultValue".into(), dv.clone());
    }
    Value::Object(out)
}

fn op_describe_data_table_attribute(
    st: &mut ConnectState,
    instance_id: &str,
    table_id: &str,
    attribute_name: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .data_table_attributes
        .get(instance_id)
        .and_then(|m| m.get(table_id))
        .and_then(|a| a.get(attribute_name))
    {
        Some(a) => ok(json!({"DataTableAttribute": data_table_attr_to_dict(a)})),
        None => not_found(format!("Data table attribute {attribute_name} not found")),
    }
}

fn op_list_data_table_attributes(st: &mut ConnectState, instance_id: &str, table_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let rows: Vec<Value> = st
        .data_table_attributes
        .get(instance_id)
        .and_then(|m| m.get(table_id))
        .map(|a| a.values().map(data_table_attr_to_dict).collect())
        .unwrap_or_default();
    ok(json!({"DataTableAttributeSummaryList": rows}))
}

fn op_update_data_table_attribute(
    st: &mut ConnectState,
    instance_id: &str,
    table_id: &str,
    attribute_name: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let attr = match st
        .data_table_attributes
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(table_id))
        .and_then(|a| a.get_mut(attribute_name))
    {
        Some(a) => a,
        None => return not_found(format!("Data table attribute {attribute_name} not found")),
    };
    if let Some(dt) = opt_val(body, "DataType") {
        attr["DataType"] = dt;
    }
    if let Some(dv) = opt_val(body, "DefaultValue") {
        attr["DefaultValue"] = dv;
    }
    if let Some(required) = opt_val(body, "Required") {
        attr["Required"] = required;
    }
    attr["ModifiedTimestamp"] = json!(now_iso());
    ok(json!({"DataTableAttribute": data_table_attr_to_dict(attr)}))
}

fn op_delete_data_table_attribute(
    st: &mut ConnectState,
    instance_id: &str,
    table_id: &str,
    attribute_name: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .data_table_attributes
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(table_id))
        .and_then(|a| a.remove(attribute_name))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!("Data table attribute {attribute_name} not found")),
    }
}

// ---------------------------------------------------------------------------
// Traffic distribution groups (global store)
// ---------------------------------------------------------------------------

fn op_create_traffic_distribution_group(
    st: &mut ConnectState,
    body: &Map<String, Value>,
    region: &str,
    account: u64,
) -> Response {
    let name = match body_str(body, "Name") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let instance_id = opt_string(body, "InstanceId").unwrap_or_default();
    let tdg_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("arn:aws:connect:{region}:{account}:traffic-distribution-group/{tdg_id}");
    let mut stored = Map::new();
    stored.insert("Id".into(), json!(tdg_id.clone()));
    stored.insert("Arn".into(), json!(arn.clone()));
    stored.insert("Name".into(), json!(name));
    stored.insert(
        "Description".into(),
        json!(opt_string(body, "Description").unwrap_or_default()),
    );
    stored.insert(
        "InstanceArn".into(),
        json!(format!("arn:aws:connect:{region}:{account}:instance/{instance_id}")),
    );
    stored.insert("Status".into(), json!("ACTIVE"));
    stored.insert("Tags".into(), opt_val(body, "Tags").unwrap_or_else(|| json!({})));
    stored.insert("IsDefault".into(), json!(false));
    st.traffic_distribution_groups
        .insert(tdg_id.clone(), Value::Object(stored));
    ok(json!({"Id": tdg_id, "Arn": arn}))
}

fn op_describe_traffic_distribution_group(st: &mut ConnectState, tdg_id: &str) -> Response {
    match st.traffic_distribution_groups.get(tdg_id) {
        Some(t) => ok(json!({"TrafficDistributionGroup": t.clone()})),
        None => not_found(format!("Traffic distribution group {tdg_id} not found")),
    }
}

fn op_delete_traffic_distribution_group(st: &mut ConnectState, tdg_id: &str) -> Response {
    match st.traffic_distribution_groups.remove(tdg_id) {
        Some(_) => ok(json!({})),
        None => not_found(format!("Traffic distribution group {tdg_id} not found")),
    }
}

fn op_list_traffic_distribution_groups(
    st: &mut ConnectState,
    query: &BTreeMap<String, Vec<String>>,
) -> Response {
    let instance_filter = q1(query, "instanceId");
    let rows: Vec<Value> = st
        .traffic_distribution_groups
        .values()
        .filter(|tdg| match instance_filter {
            Some(instance_id) => {
                let suffix = format!("instance/{instance_id}");
                str_field(tdg, "InstanceArn").ends_with(&suffix)
            }
            None => true,
        })
        .cloned()
        .collect();
    ok(json!({"TrafficDistributionGroupSummaryList": rows}))
}
