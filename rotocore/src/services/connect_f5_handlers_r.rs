
// ---------------------------------------------------------------------------
// Rules
// ---------------------------------------------------------------------------

fn op_create_rule(st: &mut ConnectState, instance_id: &str, body: &Map<String, Value>) -> Response {
    let instance_arn = match instance_arn_or_404(st, instance_id) {
        Some(a) => a,
        None => return notfound_instance(instance_id),
    };
    let name = match body_str(body, "Name") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let trigger = match opt_val(body, "TriggerEventSource") {
        Some(v) => v,
        None => return missing_body_param("TriggerEventSource"),
    };
    let function = match body_str(body, "Function") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let actions = match opt_val(body, "Actions") {
        Some(v) => v,
        None => return missing_body_param("Actions"),
    };
    let publish_status = match body_str(body, "PublishStatus") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let rule_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{instance_arn}/rule/{rule_id}");
    let stamp = now_iso();
    let mut stored = Map::new();
    stored.insert("RuleId".into(), json!(rule_id.clone()));
    stored.insert("RuleArn".into(), json!(arn.clone()));
    stored.insert("Name".into(), json!(name));
    stored.insert("TriggerEventSource".into(), trigger);
    stored.insert("Function".into(), json!(function));
    stored.insert("Actions".into(), actions);
    stored.insert("PublishStatus".into(), json!(publish_status));
    stored.insert("Tags".into(), json!({}));
    stored.insert("CreatedTime".into(), json!(stamp.clone()));
    stored.insert("LastUpdatedTime".into(), json!(stamp));
    st.rules
        .entry(instance_id.to_string())
        .or_default()
        .insert(rule_id.clone(), Value::Object(stored));
    ok(json!({"RuleArn": arn, "RuleId": rule_id}))
}

fn op_describe_rule(st: &mut ConnectState, instance_id: &str, rule_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st.rules.get(instance_id).and_then(|m| m.get(rule_id)) {
        Some(r) => ok(json!({"Rule": r.clone()})),
        None => not_found(format!("Rule {rule_id} not found in instance {instance_id}")),
    }
}

fn op_update_rule(st: &mut ConnectState, instance_id: &str, rule_id: &str, body: &Map<String, Value>) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let name = match body_str(body, "Name") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let function = match body_str(body, "Function") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let actions = match opt_val(body, "Actions") {
        Some(v) => v,
        None => return missing_body_param("Actions"),
    };
    let publish_status = match body_str(body, "PublishStatus") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let rule = match st
        .rules
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(rule_id))
    {
        Some(r) => r,
        None => return not_found(format!("Rule {rule_id} not found")),
    };
    rule["Name"] = json!(name);
    rule["Function"] = json!(function);
    rule["Actions"] = actions;
    rule["PublishStatus"] = json!(publish_status);
    rule["LastUpdatedTime"] = json!(now_iso());
    ok(json!({}))
}

fn op_delete_rule(st: &mut ConnectState, instance_id: &str, rule_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st.rules.get_mut(instance_id).and_then(|m| m.remove(rule_id)) {
        Some(_) => ok(json!({})),
        None => not_found(format!("Rule {rule_id} not found")),
    }
}

fn op_list_rules(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let empty = HashMap::new();
    let rules = st.rules.get(instance_id).unwrap_or(&empty);
    let rows: Vec<Value> = rules
        .values()
        .map(|r| {
            let summaries: Vec<Value> = r
                .get("Actions")
                .and_then(Value::as_array)
                .map(|actions| {
                    actions
                        .iter()
                        .map(|x| {
                            json!({"ActionType": x
                                .get("ActionType")
                                .cloned()
                                .unwrap_or_else(|| json!("UNKNOWN"))})
                        })
                        .collect()
                })
                .unwrap_or_default();
            json!({
                "RuleId": r["RuleId"],
                "RuleArn": r["RuleArn"],
                "Name": r["Name"],
                "PublishStatus": r["PublishStatus"],
                "ActionSummaries": summaries,
                "CreatedTime": r["CreatedTime"],
                "LastUpdatedTime": r["LastUpdatedTime"],
            })
        })
        .collect();
    ok(json!({"RuleSummaryList": rows}))
}

// ---------------------------------------------------------------------------
// Evaluation forms
// ---------------------------------------------------------------------------

fn op_create_evaluation_form(st: &mut ConnectState, instance_id: &str, body: &Map<String, Value>) -> Response {
    let instance_arn = match instance_arn_or_404(st, instance_id) {
        Some(a) => a,
        None => return notfound_instance(instance_id),
    };
    let title = match body_str(body, "Title") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let form_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{instance_arn}/evaluation-form/{form_id}");
    let stamp = now_iso();
    let mut stored = Map::new();
    stored.insert("EvaluationFormId".into(), json!(form_id.clone()));
    stored.insert("EvaluationFormArn".into(), json!(arn.clone()));
    stored.insert("Title".into(), json!(title));
    stored.insert(
        "Description".into(),
        json!(opt_string(body, "Description").unwrap_or_default()),
    );
    stored.insert("Status".into(), json!("DRAFT"));
    stored.insert("Items".into(), opt_val(body, "Items").unwrap_or_else(|| json!([])));
    stored.insert("EvaluationFormVersion".into(), json!(1));
    stored.insert("CreatedTime".into(), json!(stamp.clone()));
    stored.insert("LastModifiedTime".into(), json!(stamp));
    stored.insert("Tags".into(), json!({}));
    if let Some(s) = opt_val(body, "ScoringStrategy") {
        stored.insert("ScoringStrategy".into(), s);
    }
    st.evaluation_forms
        .entry(instance_id.to_string())
        .or_default()
        .insert(form_id.clone(), Value::Object(stored.clone()));
    ok(json!({"EvaluationFormId": form_id, "EvaluationFormArn": arn}))
}

fn op_describe_evaluation_form(st: &mut ConnectState, instance_id: &str, form_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .evaluation_forms
        .get(instance_id)
        .and_then(|m| m.get(form_id))
    {
        Some(f) => ok(json!({"EvaluationForm": f.clone()})),
        None => not_found(format!(
            "Evaluation form {form_id} not found in instance {instance_id}"
        )),
    }
}

fn op_update_evaluation_form(
    st: &mut ConnectState,
    instance_id: &str,
    form_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let title = match body_str(body, "Title") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let form = match st
        .evaluation_forms
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(form_id))
    {
        Some(f) => f,
        None => return not_found(format!("Evaluation form {form_id} not found")),
    };
    form["Title"] = json!(title);
    form["Description"] = json!(opt_string(body, "Description").unwrap_or_default());
    if let Some(items) = opt_val(body, "Items") {
        form["Items"] = items;
    }
    if let Some(s) = opt_val(body, "ScoringStrategy") {
        form["ScoringStrategy"] = s;
    }
    let version = form["EvaluationFormVersion"].as_i64().unwrap_or(0) + 1;
    form["EvaluationFormVersion"] = json!(version);
    form["LastModifiedTime"] = json!(now_iso());
    ok(json!({
        "EvaluationFormId": form["EvaluationFormId"].clone(),
        "EvaluationFormArn": form["EvaluationFormArn"].clone(),
        "EvaluationFormVersion": version,
    }))
}

fn op_activate_evaluation_form(
    st: &mut ConnectState,
    instance_id: &str,
    form_id: &str,
    activate: bool,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let form = match st
        .evaluation_forms
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(form_id))
    {
        Some(f) => f,
        None => return not_found(format!("Evaluation form {form_id} not found")),
    };
    form["Status"] = json!(if activate { "ACTIVE" } else { "DRAFT" });
    form["LastModifiedTime"] = json!(now_iso());
    ok(json!({
        "EvaluationFormId": form["EvaluationFormId"].clone(),
        "EvaluationFormArn": form["EvaluationFormArn"].clone(),
        "EvaluationFormVersion": form["EvaluationFormVersion"].clone(),
    }))
}

fn op_delete_evaluation_form(st: &mut ConnectState, instance_id: &str, form_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .evaluation_forms
        .get_mut(instance_id)
        .and_then(|m| m.remove(form_id))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!("Evaluation form {form_id} not found")),
    }
}

fn op_list_evaluation_forms(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let empty = HashMap::new();
    let forms = st.evaluation_forms.get(instance_id).unwrap_or(&empty);
    let rows: Vec<Value> = forms
        .values()
        .map(|f| {
            json!({
                "EvaluationFormId": f["EvaluationFormId"],
                "EvaluationFormArn": f["EvaluationFormArn"],
                "Title": f["Title"],
                "Status": f["Status"],
                "CreatedTime": f["CreatedTime"],
                "LastModifiedTime": f["LastModifiedTime"],
                "LatestVersion": f["EvaluationFormVersion"],
            })
        })
        .collect();
    ok(json!({"EvaluationFormSummaryList": rows}))
}

fn op_list_evaluation_form_versions(st: &mut ConnectState, instance_id: &str, form_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let rows: Vec<Value> = st
        .evaluation_forms
        .get(instance_id)
        .and_then(|m| m.get(form_id))
        .map(|f| {
            vec![json!({
                "EvaluationFormArn": f["EvaluationFormArn"],
                "EvaluationFormId": f["EvaluationFormId"],
                "EvaluationFormVersion": f["EvaluationFormVersion"],
                "CreatedTime": f["CreatedTime"],
                "LastModifiedTime": f["LastModifiedTime"],
                "Status": f["Status"],
            })]
        })
        .unwrap_or_default();
    ok(json!({"EvaluationFormVersionSummaryList": rows}))
}

// ---------------------------------------------------------------------------
// Predefined attributes
// ---------------------------------------------------------------------------

fn op_create_predefined_attribute(st: &mut ConnectState, instance_id: &str, body: &Map<String, Value>) -> Response {
    let instance_arn = match instance_arn_or_404(st, instance_id) {
        Some(a) => a,
        None => return notfound_instance(instance_id),
    };
    let name = match body_str(body, "Name") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let values = opt_val(body, "Values").unwrap_or_else(|| json!({}));
    let values_present = values
        .as_object()
        .map(|m| !m.is_empty())
        .unwrap_or(true);
    let mut stored = Map::new();
    stored.insert("Name".into(), json!(name.clone()));
    stored.insert("Values".into(), values);
    stored.insert("LastModifiedTime".into(), json!(now_iso()));
    stored.insert(
        "LastModifiedRegion".into(),
        json!(instance_arn.split(':').nth(3).unwrap_or("")),
    );
    stored.insert("__values_present".into(), json!(values_present));
    st.predefined_attributes
        .entry(instance_id.to_string())
        .or_default()
        .insert(name.clone(), Value::Object(stored));
    ok(json!({}))
}

/// moto's `to_dict` skips `Values` only when it keys off a truthy values map.
fn pa_to_dict(v: &Value) -> Value {
    let mut out = Map::new();
    for f in ["Name", "LastModifiedTime", "LastModifiedRegion"] {
        out.insert(f.to_string(), v[f].clone());
    }
    let has_values = v
        .get("Values")
        .and_then(Value::as_object)
        .map(|m| !m.is_empty())
        .unwrap_or(false);
    if has_values {
        out.insert("Values".into(), v["Values"].clone());
    }
    let _ = v.get("__values_present").and_then(Value::as_bool);
    Value::Object(out)
}

fn op_describe_predefined_attribute(st: &mut ConnectState, instance_id: &str, name: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .predefined_attributes
        .get(instance_id)
        .and_then(|m| m.get(name))
    {
        Some(a) => ok(json!({"PredefinedAttribute": pa_to_dict(a)})),
        None => not_found(format!("Predefined attribute {name} not found")),
    }
}

fn op_update_predefined_attribute(
    st: &mut ConnectState,
    instance_id: &str,
    name: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let values = match opt_val(body, "Values") {
        Some(v) => v,
        None => return missing_body_param("Values"),
    };
    let attr = match st
        .predefined_attributes
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(name))
    {
        Some(a) => a,
        None => return not_found(format!("Predefined attribute {name} not found")),
    };
    attr["Values"] = values;
    attr["LastModifiedTime"] = json!(now_iso());
    ok(json!({}))
}

fn op_delete_predefined_attribute(st: &mut ConnectState, instance_id: &str, name: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .predefined_attributes
        .get_mut(instance_id)
        .and_then(|m| m.remove(name))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!("Predefined attribute {name} not found")),
    }
}

fn op_list_predefined_attributes(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let empty = HashMap::new();
    let attrs = st.predefined_attributes.get(instance_id).unwrap_or(&empty);
    let rows: Vec<Value> = attrs
        .values()
        .map(|a| {
            json!({
                "Name": a["Name"],
                "LastModifiedTime": a["LastModifiedTime"],
                "LastModifiedRegion": a["LastModifiedRegion"],
            })
        })
        .collect();
    ok(json!({"PredefinedAttributeSummaryList": rows}))
}
