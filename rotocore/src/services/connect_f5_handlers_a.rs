
// ---------------------------------------------------------------------------
// Phone numbers (moto global store; all instances visible)
// ---------------------------------------------------------------------------

fn rand_digits(n: usize) -> String {
    let mut out = String::with_capacity(n);
    let mut seed = uuid::Uuid::new_v4().as_u128();
    while out.len() < n {
        out.push_str(&(seed % 10).to_string());
        seed >>= 4;
        if seed == 0 {
            seed = uuid::Uuid::new_v4().as_u128();
        }
    }
    out[..n].to_string()
}

fn op_claim_phone_number(
    st: &mut ConnectState,
    body: &Map<String, Value>,
    region: &str,
    account: u64,
) -> Response {
    let _ = (region, account);
    let phone_number = match body_str(body, "PhoneNumber") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let instance_id = opt_string(body, "InstanceId")
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| {
            opt_string(body, "TargetArn")
                .unwrap_or_default()
                .rsplit('/')
                .next()
                .unwrap_or("")
                .to_string()
        });
    let instance = match st.instances.get(&instance_id) {
        Some(i) => i.clone(),
        None => return notfound_instance(&instance_id),
    };
    let instance_arn = str_field(&instance, "Arn");
    let duplicate_claim = st
        .phone_numbers
        .values()
        .any(|pn| pn.get("PhoneNumber") == Some(&Value::String(phone_number.clone())));
    if duplicate_claim {
        return duplicate(format!("Phone number {phone_number} is already claimed"));
    }
    let country = opt_string(body, "PhoneNumberCountryCode").unwrap_or_else(|| "US".into());
    let number_type = opt_string(body, "PhoneNumberType").unwrap_or_else(|| "DID".into());
    let phone_number_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{instance_arn}/phone-number/{phone_number_id}");
    let mut stored = Map::new();
    stored.insert("PhoneNumberId".into(), json!(phone_number_id.clone()));
    stored.insert("PhoneNumberArn".into(), json!(arn.clone()));
    stored.insert("PhoneNumber".into(), json!(phone_number));
    stored.insert("PhoneNumberCountryCode".into(), json!(country));
    stored.insert("PhoneNumberType".into(), json!(number_type));
    stored.insert(
        "PhoneNumberDescription".into(),
        json!(opt_string(body, "PhoneNumberDescription").unwrap_or_default()),
    );
    stored.insert("TargetArn".into(), json!(instance_arn));
    stored.insert("InstanceId".into(), json!(instance_id));
    stored.insert("Tags".into(), opt_val(body, "Tags").unwrap_or_else(|| json!({})));
    stored.insert("PhoneNumberStatus".into(), json!({"Status": "CLAIMED"}));
    st.phone_numbers
        .insert(phone_number_id.clone(), Value::Object(stored));
    tag_resource(st, &arn, opt_val(body, "Tags"));
    ok(json!({"PhoneNumberId": phone_number_id, "PhoneNumberArn": arn}))
}

fn op_describe_phone_number(st: &mut ConnectState, phone_number_id: &str) -> Response {
    match st.phone_numbers.get(phone_number_id) {
        Some(pn) => ok(json!({"ClaimedPhoneNumberSummary": pn.clone()})),
        None => not_found(format!("Phone number {phone_number_id} not found")),
    }
}

fn op_update_phone_number(st: &mut ConnectState, phone_number_id: &str, body: &Map<String, Value>) -> Response {
    let pn = match st.phone_numbers.get_mut(phone_number_id) {
        Some(pn) => pn,
        None => return not_found(format!("Phone number {phone_number_id} not found")),
    };
    let target_arn = opt_string(body, "TargetArn").filter(|v| !v.is_empty());
    let instance_id = opt_string(body, "InstanceId").filter(|v| !v.is_empty());
    if let Some(target) = target_arn {
        pn["TargetArn"] = json!(target);
        pn["InstanceId"] = json!(target.rsplit('/').next().unwrap_or(""));
    } else if let Some(instance_id) = instance_id {
        if !st.instances.contains_key(&instance_id) {
            return notfound_instance(&instance_id);
        }
        pn["TargetArn"] = json!(instance_arn_of(st, &instance_id));
        pn["InstanceId"] = json!(instance_id);
    }
    ok(json!({
        "PhoneNumberId": pn["PhoneNumberId"].clone(),
        "PhoneNumberArn": pn["PhoneNumberArn"].clone(),
    }))
}

fn instance_arn_of(st: &ConnectState, instance_id: &str) -> String {
    st.instances
        .get(instance_id)
        .map(|i| str_field(i, "Arn"))
        .unwrap_or_default()
}

fn op_release_phone_number(st: &mut ConnectState, phone_number_id: &str) -> Response {
    match st.phone_numbers.remove(phone_number_id) {
        Some(_) => ok(json!({})),
        None => not_found(format!("Phone number {phone_number_id} not found")),
    }
}

fn op_list_phone_numbers(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let rows: Vec<Value> = st
        .phone_numbers
        .values()
        .filter(|pn| pn.get("InstanceId") == Some(&Value::String(instance_id.to_string())))
        .map(|pn| {
            json!({
                "PhoneNumber": pn["PhoneNumber"],
                "PhoneNumberType": pn["PhoneNumberType"],
                "PhoneNumberCountryCode": pn["PhoneNumberCountryCode"],
                "PhoneNumberId": pn["PhoneNumberId"],
                "PhoneNumberArn": pn["PhoneNumberArn"],
            })
        })
        .collect();
    ok(json!({"PhoneNumberSummaryList": rows}))
}

fn op_list_phone_numbers_v2(st: &mut ConnectState) -> Response {
    let rows: Vec<Value> = st
        .phone_numbers
        .values()
        .map(|pn| {
            json!({
                "PhoneNumberId": pn["PhoneNumberId"],
                "PhoneNumberArn": pn["PhoneNumberArn"],
                "PhoneNumber": pn["PhoneNumber"],
                "PhoneNumberCountryCode": pn["PhoneNumberCountryCode"],
                "PhoneNumberType": pn["PhoneNumberType"],
                "TargetArn": pn["TargetArn"],
            })
        })
        .collect();
    ok(json!({"ListPhoneNumbersSummaryList": rows}))
}

fn op_search_available_phone_numbers(st: &mut ConnectState, body: &Map<String, Value>) -> Response {
    let _ = st;
    let country = match body_str(body, "PhoneNumberCountryCode") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let number_type = match body_str(body, "PhoneNumberType") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    match body_str(body, "TargetArn") {
        Ok(_) => {}
        Err(resp) => return resp,
    }
    let prefix = opt_string(body, "PhoneNumberPrefix").unwrap_or_default();
    let count = body
        .get("MaxResults")
        .and_then(Value::as_i64)
        .unwrap_or(10)
        .clamp(1, 25) as usize;
    let rows: Vec<Value> = (0..count)
        .map(|i| {
            let number = if country == "US" && prefix.len() == 3 {
                format!("+1{prefix}{}", rand_digits(4 - (i as u128 % 7 == 0) as usize))
                    .chars()
                    .take(12)
                    .collect::<String>()
            } else if country == "GB" {
                format!("+4420{}{}", rand_digits(4), i)
            } else {
                format!("+1{}", rand_digits(10))
            };
            json!({
                "PhoneNumber": number,
                "PhoneNumberCountryCode": country,
                "PhoneNumberType": number_type,
            })
        })
        .collect();
    ok(json!({"AvailableNumbersList": rows}))
}

// ---------------------------------------------------------------------------
// Prompts
// ---------------------------------------------------------------------------

fn op_create_prompt(st: &mut ConnectState, instance_id: &str, body: &Map<String, Value>) -> Response {
    let instance = match st.instances.get(instance_id) {
        Some(i) => i.clone(),
        None => return notfound_instance(instance_id),
    };
    let name = match body_str(body, "Name") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let s3_uri = match body_str(body, "S3Uri") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let prompt_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{}/prompt/{prompt_id}", str_field(&instance, "Arn"));
    let mut stored = Map::new();
    stored.insert("PromptARN".into(), json!(arn.clone()));
    stored.insert("PromptId".into(), json!(prompt_id.clone()));
    stored.insert("Name".into(), json!(name));
    // moto keeps the s3 URI on the model; to_dict never serializes it
    stored.insert("__s3_uri".into(), json!(s3_uri));
    stored.insert(
        "Description".into(),
        json!(opt_string(body, "Description").unwrap_or_default()),
    );
    stored.insert("Tags".into(), opt_val(body, "Tags").unwrap_or_else(|| json!({})));
    stored.insert("LastModifiedTime".into(), json!(now_iso()));
    stored.insert("LastModifiedRegion".into(), json!(region_of(&instance)));
    st.prompts
        .entry(instance_id.to_string())
        .or_default()
        .insert(prompt_id.clone(), Value::Object(stored));
    tag_resource(st, &arn, opt_val(body, "Tags"));
    ok(json!({"PromptARN": arn, "PromptId": prompt_id}))
}

fn op_describe_prompt(st: &mut ConnectState, instance_id: &str, prompt_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st.prompts.get(instance_id).and_then(|m| m.get(prompt_id)) {
        Some(p) => ok(json!({"Prompt": p.clone()})),
        None => not_found(format!(
            "Prompt {prompt_id} not found in instance {instance_id}"
        )),
    }
}

fn op_update_prompt(st: &mut ConnectState, instance_id: &str, prompt_id: &str, body: &Map<String, Value>) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let prompt = match st
        .prompts
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(prompt_id))
    {
        Some(p) => p,
        None => return not_found(format!("Prompt {prompt_id} not found")),
    };
    if let Some(name) = opt_val(body, "Name") {
        prompt["Name"] = name;
    }
    if let Some(desc) = opt_val(body, "Description") {
        prompt["Description"] = desc;
    }
    if let Some(uri) = opt_val(body, "S3Uri") {
        prompt["__s3_uri"] = uri;
    }
    prompt["LastModifiedTime"] = json!(now_iso());
    ok(json!({
        "PromptARN": prompt["PromptARN"].clone(),
        "PromptId": prompt["PromptId"].clone(),
    }))
}

fn op_delete_prompt(st: &mut ConnectState, instance_id: &str, prompt_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st.prompts.get_mut(instance_id).and_then(|m| m.remove(prompt_id)) {
        Some(_) => ok(json!({})),
        None => not_found(format!("Prompt {prompt_id} not found")),
    }
}

fn op_list_prompts(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let empty = HashMap::new();
    let prompts = st.prompts.get(instance_id).unwrap_or(&empty);
    let rows: Vec<Value> = prompts
        .values()
        .map(|p| {
            json!({
                "Id": p["PromptId"],
                "Arn": p["PromptARN"],
                "Name": p["Name"],
                "LastModifiedTime": p["LastModifiedTime"],
                "LastModifiedRegion": p["LastModifiedRegion"],
            })
        })
        .collect();
    ok(json!({"PromptSummaryList": rows}))
}
