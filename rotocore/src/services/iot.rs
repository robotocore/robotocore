
fn describe_thing(s: &mut State, name: String) -> Result<Response, Response> {
    let Some(idx) = find_thing_idx(s, &name) else {
        return Err(not_found());
    };
    let mut thing = s.things[idx].clone();
    thing.as_object_mut().map(|o| o.remove("thingShadows"));
    if let Some(o) = thing.as_object_mut() {
        o.insert("defaultClientId".into(), json!(name));
    }
    Ok(ok_json(&thing))
}

fn update_thing(s: &mut State, name: String, body: &Value) -> Result<Response, Response> {
    let Some(idx) = find_thing_idx(s, &name) else {
        return Err(not_found());
    };
    let type_name = b_str(body, "thingTypeName").map(str::to_owned);
    let remove_type = b_bool(body, "removeThingType");
    if remove_type == Some(true) && type_name.is_some() {
        return Err(invalid_request("The request is not valid."));
    }
    if let Some(tn) = type_name {
        let tt_ok = s
            .thing_types
            .iter()
            .find(|t| t["thingTypeName"] == json!(tn))
            .map(|tt| tt["thingTypeMetadata"]["deprecated"] != json!(true));
        match tt_ok {
            Some(true) => {
                s.things[idx]["thingTypeName"] = json!(tn);
            }
            Some(false) => {
                return Err(invalid_request(&format!(
                    "Can not update a thing to use deprecated thing type: {tn}"
                )));
            }
            None => return Err(not_found()),
        }
    } else if remove_type == Some(true) {
        s.things[idx].as_object_mut().map(|o| o.remove("thingTypeName"));
    }
    if let Some(payload) = body.get("attributePayload") {
        if !payload.is_null() {
            if let Some(attrs) = payload.get("attributes") {
                let merge = payload.get("merge") == Some(&json!(true));
                if !merge {
                    s.things[idx]["attributes"] = attrs.clone();
                } else if let (Some(cur), Some(new)) =
                    (s.things[idx]["attributes"].as_object_mut(), attrs.as_object())
                {
                    for (k, v) in new {
                        cur.insert(k.clone(), v.clone());
                    }
                }
                if let Some(cur) = s.things[idx]["attributes"].as_object_mut() {
                    cur.retain(|_, v| !v.is_null());
                }
            }
        }
    }
    Ok(ok_empty())
}

fn delete_thing(s: &mut State, name: String) -> Result<Response, Response> {
    let Some(idx) = find_thing_idx(s, &name) else {
        return Err(not_found());
    };
    if s.principal_things.iter().any(|(_, tn)| tn == &name) {
        return Err(invalid_request(&format!(
            "Cannot delete. Thing {name} is still attached to one or more principals"
        )));
    }
    let arn = s.things[idx]["thingArn"].as_str().unwrap_or_default().to_string();
    for g in &mut s.billing_groups {
        if let Some(arr) = g["things"].as_array_mut() {
            arr.retain(|a| a != &json!(arn));
        }
    }
    s.principal_things.retain(|(_, tn)| tn != &name);
    s.things.remove(idx);
    Ok(ok_empty())
}

fn list_things(
    s: &mut State,
    body: &Value,
    query: &BTreeMap<String, Vec<String>>,
) -> Result<Response, Response> {
    let attr_name = p_str(body, query, "attributeName").unwrap_or_default();
    let attr_value = p_str(body, query, "attributeValue").unwrap_or_default();
    let type_name = p_str(body, query, "thingTypeName").unwrap_or_default();
    let mut items = Vec::new();
    for t in &s.things {
        if !attr_name.is_empty() {
            let got = t["attributes"].get(&attr_name).and_then(Value::as_str).unwrap_or("");
            if got != attr_value {
                continue;
            }
        }
        if !type_name.is_empty()
            && t["thingTypeName"].as_str().unwrap_or_default() != type_name
        {
            continue;
        }
        let mut obj = t.clone();
        if let Some(o) = obj.as_object_mut() {
            o.remove("thingShadows");
            o.remove("thingId");
        }
        items.push(obj);
    }
    Ok(ok_json(&json!({"things": items, "nextToken": Value::Null})))
}

fn register_thing(_body: &Value) -> Result<Response, Response> {
    Ok(ok_json(&json!({"resourceArns": {}})))
}

// ---------------------------------------------------------------------------
// Thing principals — impls
// ---------------------------------------------------------------------------

/// moto's `_get_principal`: certificate ARNs (and thing-group ARNs) must
/// exist before an attach/detach succeeds.
fn require_principal(s: &State, principal_arn: &str) -> Result<(), Response> {
    if principal_arn.contains(":cert/") {
        if s.certificates.iter().any(|c| c["arn"] == json!(principal_arn)) {
            return Ok(());
        }
        return Err(not_found());
    }
    if principal_arn.contains(":thinggroup/") {
        if s.thing_groups.iter().any(|g| g["arn"] == json!(principal_arn)) {
            return Ok(());
        }
        return Err(not_found());
    }
    Err(not_found())
}

fn attach_thing_principal(
    s: &mut State,
    thing_name: String,
    req: &Request,
) -> Result<Response, Response> {
    let principal = header(req, "x-amzn-principal").unwrap_or_default().to_string();
    require_principal(s, &principal)?;
    find_thing_idx(s, &thing_name).ok_or_else(not_found)?;
    let key = (principal, thing_name);
    if !s.principal_things.contains(&key) {
        s.principal_things.push(key);
    }
    Ok(ok_empty())
}

fn detach_thing_principal(
    s: &mut State,
    thing_name: String,
    req: &Request,
) -> Result<Response, Response> {
    let principal = header(req, "x-amzn-principal").unwrap_or_default().to_string();
    require_principal(s, &principal)?;
    find_thing_idx(s, &thing_name).ok_or_else(not_found)?;
    let before = s.principal_things.len();
    s.principal_things.retain(|k| k.0 != principal || k.1 != thing_name);
    if before_len(s, s.principal_things.len()) {
        return Err(not_found());
    }
    Ok(ok_empty())
}
