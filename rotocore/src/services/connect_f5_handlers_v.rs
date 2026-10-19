
// ---------------------------------------------------------------------------
// Views
// ---------------------------------------------------------------------------

fn op_create_view(st: &mut ConnectState, instance_id: &str, body: &Map<String, Value>) -> Response {
    let instance_arn = match instance_arn_or_404(st, instance_id) {
        Some(a) => a,
        None => return notfound_instance(instance_id),
    };
    let name = match body_str(body, "Name") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let status = match body_str(body, "Status") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let content = match opt_val(body, "Content") {
        Some(v) => v,
        None => return missing_body_param("Content"),
    };
    let view_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{instance_arn}/view/{view_id}");
    let mut stored = Map::new();
    stored.insert("Id".into(), json!(view_id.clone()));
    stored.insert("Arn".into(), json!(arn.clone()));
    stored.insert("Name".into(), json!(name));
    stored.insert("Status".into(), json!(status));
    stored.insert("Type".into(), json!("CUSTOMER_MANAGED"));
    stored.insert(
        "Description".into(),
        json!(opt_string(body, "Description").unwrap_or_default()),
    );
    stored.insert("Version".into(), json!(1));
    stored.insert("Content".into(), content);
    stored.insert("Tags".into(), opt_val(body, "Tags").unwrap_or_else(|| json!({})));
    st.views
        .entry(instance_id.to_string())
        .or_default()
        .insert(view_id.clone(), Value::Object(stored.clone()));
    tag_resource(st, &arn, opt_val(body, "Tags"));
    // moto returns the create response whole-view
    ok(json!({"View": stored}))
}

fn op_describe_view(st: &mut ConnectState, instance_id: &str, view_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st.views.get(instance_id).and_then(|m| m.get(view_id)) {
        Some(v) => ok(json!({"View": v.clone()})),
        None => not_found(format!("View {view_id} not found in instance {instance_id}")),
    }
}

fn op_update_view_content(
    st: &mut ConnectState,
    instance_id: &str,
    view_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let content = match opt_val(body, "Content") {
        Some(v) => v,
        None => return missing_body_param("Content"),
    };
    let view = match st.views.get_mut(instance_id).and_then(|m| m.get_mut(view_id)) {
        Some(v) => v,
        None => return not_found(format!("View {view_id} not found")),
    };
    view["Content"] = content;
    let version = view["Version"].as_i64().unwrap_or(0) + 1;
    view["Version"] = json!(version);
    if let Some(status) = opt_val(body, "Status").filter(|v| !v.is_null()) {
        view["Status"] = status;
    }
    ok(json!({"View": view.clone()}))
}

fn op_update_view_metadata(
    st: &mut ConnectState,
    instance_id: &str,
    view_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let view = match st.views.get_mut(instance_id).and_then(|m| m.get_mut(view_id)) {
        Some(v) => v,
        None => return not_found(format!("View {view_id} not found")),
    };
    if let Some(name) = opt_val(body, "Name") {
        view["Name"] = name;
    }
    if let Some(desc) = opt_val(body, "Description") {
        view["Description"] = desc;
    }
    ok(json!({}))
}

fn op_delete_view(st: &mut ConnectState, instance_id: &str, view_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st.views.get_mut(instance_id).and_then(|m| m.remove(view_id)) {
        Some(_) => ok(json!({})),
        None => not_found(format!("View {view_id} not found")),
    }
}

fn op_list_views(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let empty = HashMap::new();
    let views = st.views.get(instance_id).unwrap_or(&empty);
    let rows: Vec<Value> = views
        .values()
        .map(|v| {
            json!({
                "Id": v["Id"],
                "Arn": v["Arn"],
                "Name": v["Name"],
                "Type": v["Type"],
                "Status": v["Status"],
                "Description": v["Description"],
            })
        })
        .collect();
    ok(json!({"ViewsSummaryList": rows}))
}

// ---------------------------------------------------------------------------
// Vocabularies
// ---------------------------------------------------------------------------

fn op_create_vocabulary(st: &mut ConnectState, instance_id: &str, body: &Map<String, Value>) -> Response {
    let instance = match st.instances.get(instance_id) {
        Some(i) => i.clone(),
        None => return notfound_instance(instance_id),
    };
    let name = match body_str(body, "VocabularyName") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let language_code = match body_str(body, "LanguageCode") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let content = match body_str(body, "Content") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let vocab_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{}/vocabulary/{vocab_id}", str_field(&instance, "Arn"));
    let mut stored = Map::new();
    stored.insert("Name".into(), json!(name));
    stored.insert("Id".into(), json!(vocab_id.clone()));
    stored.insert("Arn".into(), json!(arn.clone()));
    stored.insert("LanguageCode".into(), json!(language_code));
    stored.insert("State".into(), json!("ACTIVE"));
    stored.insert("LastModifiedTime".into(), json!(now_iso()));
    stored.insert("Content".into(), json!(content));
    stored.insert("Tags".into(), opt_val(body, "Tags").unwrap_or_else(|| json!({})));
    st.vocabularies
        .entry(instance_id.to_string())
        .or_default()
        .insert(vocab_id.clone(), Value::Object(stored.clone()));
    tag_resource(st, &arn, opt_val(body, "Tags"));
    ok(json!({"VocabularyArn": arn, "VocabularyId": vocab_id, "State": "ACTIVE"}))
}

fn op_describe_vocabulary(st: &mut ConnectState, instance_id: &str, vocab_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .vocabularies
        .get(instance_id)
        .and_then(|m| m.get(vocab_id))
    {
        Some(v) => ok(json!({"Vocabulary": v.clone()})),
        None => not_found(format!(
            "Vocabulary {vocab_id} not found in instance {instance_id}"
        )),
    }
}

fn op_delete_vocabulary(st: &mut ConnectState, instance_id: &str, vocab_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .vocabularies
        .get_mut(instance_id)
        .and_then(|m| m.remove(vocab_id))
    {
        Some(v) => ok(json!({
            "VocabularyArn": v["Arn"].clone(),
            "VocabularyId": v["Id"].clone(),
            "State": "DELETE_IN_PROGRESS",
        })),
        None => not_found(format!("Vocabulary {vocab_id} not found")),
    }
}

fn op_search_vocabularies(st: &mut ConnectState, instance_id: &str, body: &Map<String, Value>) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let state = opt_string(body, "State");
    let name_prefix = opt_string(body, "NameStartsWith");
    let language_code = opt_string(body, "LanguageCode");
    let empty = HashMap::new();
    let vocabs = st.vocabularies.get(instance_id).unwrap_or(&empty);
    let rows: Vec<Value> = vocabs
        .values()
        .filter(|v| match &state {
            Some(s) => v.get("State") == Some(&Value::String(s.clone())),
            None => true,
        })
        .filter(|v| match &name_prefix {
            Some(p) => str_field(v, "Name").starts_with(p),
            None => true,
        })
        .filter(|v| match &language_code {
            Some(c) => v.get("LanguageCode") == Some(&Value::String(c.clone())),
            None => true,
        })
        .cloned()
        .collect();
    ok(json!({"VocabularySummaryList": rows}))
}

fn op_list_default_vocabularies(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let empty = HashMap::new();
    let vocabs = st.vocabularies.get(instance_id).unwrap_or(&empty);
    let rows: Vec<Value> = vocabs
        .values()
        .map(|v| {
            json!({
                "InstanceId": instance_id,
                "LanguageCode": v["LanguageCode"],
                "VocabularyId": v["Id"],
                "VocabularyName": v["Name"],
            })
        })
        .collect();
    ok(json!({"DefaultVocabularyList": rows}))
}
