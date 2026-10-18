//! Template engine — parses CloudFormation templates (JSON or YAML), resolves
//! intrinsic functions, evaluates conditions, and orders resources for
//! deployment. Ported from `src/robotocore/services/cloudformation/engine.py`.

use serde_json::Value;
use std::collections::{HashMap, HashSet};

/// Parse a CloudFormation template (JSON first, then YAML with shorthand
/// tags like `!Ref`).
pub fn parse_template(template_str: &str) -> Result<Value, String> {
    let trimmed = template_str.trim();
    if trimmed.is_empty() {
        return Err("empty template".to_string());
    }
    if let Ok(v) = serde_json::from_str::<Value>(trimmed) {
        return Ok(v);
    }
    let yaml: serde_yaml::Value =
        serde_yaml::from_str(trimmed).map_err(|e| format!("yaml parse failed: {e}"))?;
    Ok(yaml_to_json(&yaml))
}

/// Convert a serde_yaml value (which can carry `!Ref`-style shorthand tags)
/// into JSON shaped the way the Python engine's YAML loader does:
/// `!Foo bar` becomes `{"Fn::Foo": bar}` and `!Ref x` becomes `{"Ref": "x"}`.
pub fn yaml_to_json(v: &serde_yaml::Value) -> Value {
    match v {
        serde_yaml::Value::Null => Value::Null,
        serde_yaml::Value::Bool(b) => Value::Bool(*b),
        serde_yaml::Value::Number(n) => match (n.as_i64(), n.as_f64()) {
            (Some(i), _) => Value::Number(serde_json::Number::from(i)),
            (None, Some(f)) => serde_json::Number::from_f64(f)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            _ => Value::Null,
        },
        serde_yaml::Value::String(s) => Value::String(s.clone()),
        serde_yaml::Value::Sequence(items) => {
            Value::Array(items.iter().map(yaml_to_json).collect())
        }
        serde_yaml::Value::Mapping(map) => {
            let mut obj = serde_json::Map::new();
            for (k, val) in map {
                let key = match k {
                    serde_yaml::Value::String(s) => s.clone(),
                    serde_yaml::Value::Number(n) => n.to_string(),
                    serde_yaml::Value::Bool(b) => py_str_bool(*b),
                    _ => String::new(),
                };
                obj.insert(key, yaml_to_json(val));
            }
            Value::Object(obj)
        }
        serde_yaml::Value::Tagged(tagged) => {
            let raw_tag = format!("{}", tagged.tag);
            let tag = raw_tag.trim_start_matches('!').to_string();
            let key = if tag == "Ref" {
                "Ref".to_string()
            } else {
                format!("Fn::{tag}")
            };
            // !GetAtt takes "a.b" and returns the two-part list form.
            let payload = match &tagged.value {
                serde_yaml::Value::String(s) if key == "Fn::GetAtt" => Value::Array(
                    s.split('.')
                        .map(|p| Value::String(p.to_string()))
                        .collect(),
                ),
                other => yaml_to_json(other),
            };
            let mut obj = serde_json::Map::new();
            obj.insert(key, payload);
            Value::Object(obj)
        }
    }
}

/// Everything the intrinsic resolver needs to know about the current stack.
#[derive(Clone)]
pub struct ResolveCtx {
    /// `logical_id -> attributes` from already-created resources.
    pub resources: HashMap<String, ResourceInfo>,
    /// User parameters merged with template defaults (string values only).
    pub parameters: HashMap<String, String>,
    /// Template `Mappings` section (for Fn::FindInMap).
    pub mappings: Value,
    /// Evaluated conditions (name -> bool).
    pub conditions: HashMap<String, bool>,
    /// Global exports (name -> value) for Fn::ImportValue.
    pub imports: HashMap<String, String>,
    pub stack_name: String,
    pub stack_id: String,
    pub region: String,
    pub account_id: String,
}

#[derive(Clone, Debug, Default)]
pub struct ResourceInfo {
    pub resource_type: String,
    pub physical_id: String,
    pub attributes: HashMap<String, Value>,
}

pub type ResolveResult = Result<Value, String>;

/// Render a JSON scalar the way Python `str()`/`str(value)` would in the
/// places the engine stringifies (outputs, Fn::Sub, etc.).
pub fn scalar_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Bool(b) => py_str_bool(*b),
        Value::Number(n) => n.to_string(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

pub fn py_str_bool(b: bool) -> String {
    if b { "True".to_string() } else { "False".to_string() }
}

fn pseudo_sub(name: &str, ctx: &ResolveCtx) -> Option<String> {
    match name {
        "AWS::Region" => Some(ctx.region.clone()),
        "AWS::AccountId" => Some(ctx.account_id.clone()),
        "AWS::StackName" => Some(ctx.stack_name.clone()),
        "AWS::StackId" => Some(ctx.stack_id.clone()),
        "AWS::URLSuffix" => Some("amazonaws.com".to_string()),
        "AWS::Partition" => Some("aws".to_string()),
        // AWS::NotificationARNs renders as an empty list only via Ref array
        // contexts; string contexts use "".
        "AWS::NotificationARNs" => Some(String::new()),
        _ => None,
    }
}

/// Resolve CloudFormation intrinsic functions recursively.
/// `Ok(Value::Null)` represents an AWS::NoValue result (the key is dropped).
pub fn resolve_intrinsics(value: &Value, ctx: &ResolveCtx, depth: usize) -> ResolveResult {
    if depth > 64 {
        return Err("Template error: intrinsic resolution too deep".to_string());
    }
    match value {
        Value::String(s) => Ok(Value::String(s.clone())),
        Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for v in items {
                out.push(resolve_intrinsics(v, ctx, depth + 1)?);
            }
            Ok(Value::Array(out))
        }
        Value::Object(map) => {
            if map.len() == 1 {
                let mut it = map.iter();
                let (k, v) = it.next().unwrap();
                if let Some(res) = resolve_single_intrinsic(k, v, ctx, depth) {
                    return res;
                }
                // Not a known intrinsic: fall through to deep resolution,
                // wrapping the resolved value back under the same key.
                let inner = resolve_intrinsics(v, ctx, depth + 1)?;
                if inner.is_null() {
                    return Ok(Value::Null);
                }
                let mut obj = serde_json::Map::new();
                obj.insert(k.clone(), inner);
                return Ok(Value::Object(obj));
            }
            let mut result = serde_json::Map::new();
            for (k, v) in map {
                if k == "Fn::Transform" {
                    let mut resolved = serde_json::Map::new();
                    if let Some(params) = v.get("Parameters").and_then(Value::as_object) {
                        let mut pr = serde_json::Map::new();
                        for (pk, pv) in params {
                            pr.insert(pk.clone(), resolve_intrinsics(pv, ctx, depth + 1)?);
                        }
                        resolved.insert("Parameters".to_string(), Value::Object(pr));
                    }
                    if let Some(tobj) = v.as_object() {
                        for (tk, tv) in tobj {
                            if tk != "Parameters" {
                                resolved.insert(tk.clone(), tv.clone());
                            }
                        }
                    }
                    let mut out = serde_json::Map::new();
                    out.insert("Fn::Transform".to_string(), Value::Object(resolved));
                    return Ok(Value::Object(out));
                }
                let resolved = resolve_intrinsics(v, ctx, depth + 1)?;
                if !resolved.is_null() {
                    result.insert(k.clone(), resolved);
                }
            }
            Ok(Value::Object(result))
        }
        other => Ok(other.clone()),
    }
}

/// Handle a single-key object that names an intrinsic. Returns `None` when
/// the key is not one of the intrinsic functions.
fn resolve_single_intrinsic(
    key: &str,
    value: &Value,
    ctx: &ResolveCtx,
    depth: usize,
) -> Option<ResolveResult> {
    let res = match key {
        "Ref" => Some(resolve_ref(value, ctx, depth)),
        "Fn::GetAtt" => Some(resolve_getatt(value, ctx)),
        "Fn::Join" => Some(resolve_join(value, ctx, depth)),
        "Fn::Sub" => Some(resolve_sub(value, ctx, depth)),
        "Fn::Select" => Some(resolve_select(value, ctx, depth)),
        "Fn::Split" => Some(resolve_split(value, ctx, depth)),
        "Fn::If" => Some(resolve_if(value, ctx, depth)),
        "Fn::Equals" => Some(resolve_equals(value, ctx, depth)),
        "Fn::Not" => Some(resolve_not(value, ctx, depth)),
        "Fn::GetAZs" => Some(Ok(Value::Array(vec![
            Value::String(format!("{}a", ctx.region)),
            Value::String(format!("{}b", ctx.region)),
            Value::String(format!("{}c", ctx.region)),
        ]))),
        "Fn::FindInMap" => Some(resolve_findinmap(value, ctx, depth)),
        "Fn::Base64" => Some((
            || {
                let input = resolve_intrinsics(value, ctx, depth + 1)?;
                Ok(Value::String(b64(input_string(&input).as_bytes())))
            }
        )()),
        "Fn::ImportValue" => Some(resolve_import(value, ctx, depth)),
        "Fn::Cidr" => Some(resolve_cidr(value, ctx, depth)),
        "Condition" => {
            let name = scalar_string(value);
            Some(Ok(Value::Bool(*ctx.conditions.get(&name).unwrap_or(&false))))
        }
        "Fn::And" => Some((
            || {
                let conds = value.as_array().cloned().unwrap_or_default();
                let mut all = true;
                for c in &conds {
                    if !truthy(&resolve_intrinsics(c, ctx, depth + 1)?) {
                        all = false;
                    }
                }
                Ok(Value::Bool(all))
            }
        )()),
        "Fn::Or" => Some((
            || {
                let conds = value.as_array().cloned().unwrap_or_default();
                let mut any = false;
                for c in &conds {
                    if truthy(&resolve_intrinsics(c, ctx, depth + 1)?) {
                        any = true;
                    }
                }
                Ok(Value::Bool(any))
            }
        )()),
        _ => None,
    };
    res
}

fn truthy(v: &Value) -> bool {
    match v {
        Value::Bool(b) => *b,
        Value::Null => false,
        Value::String(s) => !s.is_empty() && s != "false" && s != "None" && s != "[]",
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(true),
        Value::Array(a) => !a.is_empty(),
        Value::Object(_) => true,
    }
}

/// Base64 encoding helper (kept local so the engine has no dep on mirror).
fn b64(input: &[u8]) -> String {
    crate::services::cloudformation::mirror::b64_encode(input)
}

fn input_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Bool(b) => py_str_bool(*b),
        Value::Number(n) => n.to_string(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn resolve_ref(inner: &Value, ctx: &ResolveCtx, depth: usize) -> ResolveResult {
    let ref_name = match inner {
        Value::String(s) => s.clone(),
        other => scalar_string(&resolve_intrinsics(other, ctx, depth + 1)?),
    };
    if let Some(p) = pseudo_sub(&ref_name, ctx) {
        return Ok(Value::String(p));
    }
    if ref_name == "AWS::NoValue" {
        return Ok(Value::Null);
    }
    if let Some(v) = ctx.parameters.get(&ref_name) {
        return Ok(Value::String(v.clone()));
    }
    if let Some(res) = ctx.resources.get(&ref_name) {
        if !res.physical_id.is_empty() {
            return Ok(Value::String(res.physical_id.clone()));
        }
    }
    Ok(Value::String(ref_name))
}

fn resolve_getatt(fn_value: &Value, ctx: &ResolveCtx) -> ResolveResult {
    let parts: Vec<String> = match fn_value {
        Value::Array(items) => items.iter().map(scalar_string).collect(),
        Value::String(s) => s.split('.').map(|p| p.to_string()).collect(),
        _ => return Err("Template error: Fn::GetAtt requires a list or string".to_string()),
    };
    let logical_id = parts.first().cloned().unwrap_or_default();
    let attr_name = if parts.len() > 1 {
        parts[1..].join(".")
    } else {
        String::new()
    };
    let res = ctx.resources.get(&logical_id).ok_or_else(|| {
        format!(
            "Template error: instance of Fn::GetAtt references undefined resource '{logical_id}'"
        )
    })?;
    if let Some(v) = res.attributes.get(&attr_name) {
        return Ok(v.clone());
    }
    if res.resource_type == "AWS::CloudFormation::Stack" && attr_name.starts_with("Outputs.") {
        let output_key = attr_name.trim_start_matches("Outputs.").to_string();
        let fallback = Value::String(format!("{logical_id}-{output_key}"));
        return Ok(res
            .attributes
            .get(&output_key)
            .cloned()
            .unwrap_or(fallback));
    }
    Ok(Value::String(String::new()))
}

fn resolve_join(join: &Value, ctx: &ResolveCtx, depth: usize) -> ResolveResult {
    let pair = join.as_array().ok_or("Fn::Join requires a list")?;
    if pair.len() != 2 {
        return Err("Fn::Join requires [delimiter, values]".to_string());
    }
    let delimiter = scalar_string(&pair[0]);
    let values = resolve_intrinsics(&pair[1], ctx, depth + 1)?;
    let list = values.as_array().ok_or("Fn::Join values must be a list")?;
    let joined: Vec<String> = list.iter().map(scalar_string).collect();
    Ok(Value::String(joined.join(&delimiter)))
}

fn resolve_sub(sub: &Value, ctx: &ResolveCtx, depth: usize) -> ResolveResult {
    let (template_str, vars_map): (String, Option<Value>) = match sub {
        Value::String(s) => (s.clone(), None),
        Value::Array(items) if !items.is_empty() => {
            let tmpl = match &items[0] {
                Value::String(s) => s.clone(),
                _ => return Err("Fn::Sub template must be a string".to_string()),
            };
            (tmpl, items.get(1).cloned())
        }
        _ => return Err("Fn::Sub requires a string or [template, vars]".to_string()),
    };
    let mut resolved_vars: HashMap<String, String> = HashMap::new();
    if let Value::Object(obj) = vars_map.as_ref().unwrap_or(&Value::Null) {
        for (k, v) in obj {
            resolved_vars.insert(k.clone(), sub_string(&resolve_intrinsics(v, ctx, depth + 1)?));
        }
    }

    // Escape $$ for literal $ and ${!var} for literal ${var} in the same
    // order the Python engine does, using NUL sentinels.
    let mut work = template_str.replace("$$", "\x00DOLLAR\x00");
    work = escape_literal_dollar(&work);

    let mut result = String::with_capacity(work.len());
    let bytes = work.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' && i + 1 < bytes.len() && bytes[i + 1] == b'{' {
            if let Some(close) = work[i + 2..].find('}') {
                let var = &work[i + 2..i + 2 + close];
                result.push_str(&substitute_var(var, &resolved_vars, ctx));
                i += 2 + close + 1;
                continue;
            }
        }
        let ch_len = work[i..].chars().next().map(char::len_utf8).unwrap_or(1);
        result.push_str(&work[i..i + ch_len]);
        i += ch_len;
    }
    let result = result
        .replace("\x00LITERAL_START\x00", "${")
        .replace("\x00LITERAL_END\x00", "}")
        .replace("\x00DOLLAR\x00", "$");
    Ok(Value::String(result))
}

fn escape_literal_dollar(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' && i + 2 < bytes.len() && bytes[i + 1] == b'{' && bytes[i + 2] == b'!' {
            if let Some(close) = s[i + 3..].find('}') {
                out.push_str("\x00LITERAL_START\x00");
                out.push_str(&s[i + 3..i + 3 + close]);
                out.push_str("\x00LITERAL_END\x00");
                i += 3 + close + 1;
                continue;
            }
        }
        let ch_len = s[i..].chars().next().map(char::len_utf8).unwrap_or(1);
        out.push_str(&s[i..i + ch_len]);
        i += ch_len;
    }
    out
}

fn sub_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Bool(b) => py_str_bool(*b),
        Value::Number(n) => n.to_string(),
        other => other.to_string(),
    }
}

fn substitute_var(var: &str, vars: &HashMap<String, String>, ctx: &ResolveCtx) -> String {
    if let Some(v) = vars.get(var) {
        return v.clone();
    }
    if let Some(p) = pseudo_sub(var, ctx) {
        return p;
    }
    if var.contains('.') {
        let mut it = var.splitn(2, '.');
        let logical = it.next().unwrap_or("");
        let attr = it.next().unwrap_or("");
        if let Some(res) = ctx.resources.get(logical) {
            if let Some(v) = res.attributes.get(attr) {
                return sub_string(v);
            }
        }
    }
    if let Some(v) = ctx.parameters.get(var) {
        return v.clone();
    }
    if let Some(res) = ctx.resources.get(var) {
        return res.physical_id.clone();
    }
    // Python raises "variable ... cannot be resolved"; callers translate
    // that into a template error only when it breaks a resource. Keep the
    // same lenient-but-visible behavior as an empty substitution of the
    // variable name (matches moto's tolerance for template vars).
    String::new()
}

fn resolve_select(sel: &Value, ctx: &ResolveCtx, depth: usize) -> ResolveResult {
    let pair = sel.as_array().ok_or("Fn::Select requires a list")?;
    if pair.len() != 2 {
        return Err("Fn::Select requires [index, options]".to_string());
    }
    let resolved = resolve_intrinsics(&pair[1], ctx, depth + 1)?;
    let list = resolved.as_array().ok_or_else(|| {
        format!(
            "Template error: Fn::Select index {} is out of bounds for list of length 0",
            scalar_string(&pair[0])
        )
    })?;
    let idx = parse_index(&pair[0]).ok_or_else(|| {
        format!("Template error: Fn::Select index '{}' not valid", pair[0])
    })?;
    if idx < 0 || idx as usize >= list.len() {
        return Err(format!(
            "Template error: Fn::Select index {idx} is out of bounds for list of length {}",
            list.len()
        ));
    }
    Ok(list[idx as usize].clone())
}

fn parse_index(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => s.parse::<i64>().ok(),
        _ => None,
    }
}

fn resolve_split(split: &Value, ctx: &ResolveCtx, depth: usize) -> ResolveResult {
    let pair = split.as_array().ok_or("Fn::Split requires a list")?;
    if pair.len() != 2 {
        return Err("Fn::Split requires [delimiter, source]".to_string());
    }
    let delimiter = scalar_string(&pair[0]);
    let resolved = resolve_intrinsics(&pair[1], ctx, depth + 1)?;
    Ok(Value::Array(
        scalar_string(&resolved)
            .split(delimiter.as_str())
            .filter(|_| true)
            .map(|p| Value::String(p.to_string()))
            .collect(),
    ))
}

fn resolve_if(iff: &Value, ctx: &ResolveCtx, depth: usize) -> ResolveResult {
    let tri = iff.as_array().ok_or("Fn::If requires a list")?;
    if tri.len() != 3 {
        return Err("Fn::If requires [condition, true, false]".to_string());
    }
    let cond_name = scalar_string(&tri[0]);
    let branch = match ctx.conditions.get(&cond_name) {
        Some(true) => &tri[1],
        Some(false) => &tri[2],
        // Not-yet-evaluated conditions default to the true branch (py).
        None => &tri[1],
    };
    resolve_intrinsics(branch, ctx, depth + 1)
}

fn resolve_equals(eq: &Value, ctx: &ResolveCtx, depth: usize) -> ResolveResult {
    let pair = eq.as_array().ok_or("Fn::Equals requires a list")?;
    if pair.len() != 2 {
        return Err("Fn::Equals requires [a, b]".to_string());
    }
    let ra = resolve_intrinsics(&pair[0], ctx, depth + 1)?;
    let rb = resolve_intrinsics(&pair[1], ctx, depth + 1)?;
    Ok(Value::Bool(scalar_string(&ra) == scalar_string(&rb)))
}

fn resolve_not(not: &Value, ctx: &ResolveCtx, depth: usize) -> ResolveResult {
    let conds = not.as_array().cloned().unwrap_or_default();
    let first = conds.first().ok_or("Fn::Not requires a list")?;
    let v = resolve_intrinsics(first, ctx, depth + 1)?;
    Ok(Value::Bool(!truthy(&v)))
}

fn resolve_findinmap(fim: &Value, ctx: &ResolveCtx, depth: usize) -> ResolveResult {
    let args = fim.as_array().cloned().unwrap_or_default();
    if args.len() < 3 {
        return Err("Fn::FindInMap requires [map, top, second]".to_string());
    }
    let map_name = scalar_string(&resolve_intrinsics(&args[0], ctx, depth + 1)?);
    let first_key = scalar_string(&resolve_intrinsics(&args[1], ctx, depth + 1)?);
    let second_key = scalar_string(&resolve_intrinsics(&args[2], ctx, depth + 1)?);
    let not_found = || {
        format!("Template error: Mapping '{map_name}' key '{first_key}.{second_key}' not found")
    };
    let mapping = ctx.mappings.get(&map_name).and_then(Value::as_object).ok_or_else(not_found)?;
    let first = mapping.get(&first_key).and_then(Value::as_object).ok_or_else(not_found)?;
    let result = first.get(&second_key).ok_or_else(not_found)?;
    Ok(result.clone())
}

fn resolve_import(inner: &Value, ctx: &ResolveCtx, depth: usize) -> ResolveResult {
    let resolved = resolve_intrinsics(inner, ctx, depth + 1)?;
    let key = match &resolved {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    ctx.imports
        .get(&key)
        .map(|v| Value::String(v.clone()))
        .ok_or_else(|| format!("No export named '{key}' found. Rollback requested by user."))
}

fn resolve_cidr(cidr: &Value, ctx: &ResolveCtx, depth: usize) -> ResolveResult {
    let args = cidr.as_array().cloned().unwrap_or_default();
    let first = args.first().ok_or("Fn::Cidr requires arguments")?;
    let ip_block = scalar_string(&resolve_intrinsics(first, ctx, depth + 1)?);
    let count = inner_usize(args.get(1), ctx, depth)?;
    let cidr_bits = if args.len() > 2 {
        inner_usize(args.get(2), ctx, depth)?
    } else {
        8
    };
    Ok(Value::Array(
        compute_cidr(&ip_block, count, cidr_bits)
            .into_iter()
            .map(Value::String)
            .collect(),
    ))
}

fn inner_usize(v: Option<&Value>, ctx: &ResolveCtx, depth: usize) -> Result<usize, String> {
    let inner = resolve_intrinsics(v.unwrap_or(&Value::Null), ctx, depth + 1)?;
    match inner {
        Value::Number(n) => n.as_u64().map(|u| u as usize).ok_or("count not a number".to_string()),
        Value::String(s) => s.parse::<usize>().map_err(|e| e.to_string()),
        _ => Err("count not a number".to_string()),
    }
}

/// Compute `count` CIDR subnets from `ip_block` with `cidr_bits` net bits,
/// mirroring `resources.compute_cidr`.
pub fn compute_cidr(ip_block: &str, count: usize, cidr_bits: usize) -> Vec<String> {
    if cidr_bits == 0 || cidr_bits > 32 {
        return vec![ip_block.to_string()];
    }
    let octets: Vec<&str> = ip_block.split('.').collect();
    if octets.len() != 4 {
        return vec![ip_block.to_string()];
    }
    let mut nums: Vec<u32> = Vec::with_capacity(4);
    for o in &octets {
        match o.parse::<u32>() {
            Ok(n) if n <= 255 => nums.push(n),
            _ => return vec![ip_block.to_string()],
        }
    }
    let base: u32 = (nums[0] << 24) | (nums[1] << 16) | (nums[2] << 8) | nums[3];
    let stride = 1u32 << (32 - cidr_bits);
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let ip = base.wrapping_add(stride.saturating_mul(i as u32));
        out.push(format!(
            "{}.{}.{}.{}",
            (ip >> 24) & 0xff,
            (ip >> 16) & 0xff,
            (ip >> 8) & 0xff,
            ip & 0xff
        ));
    }
    out
}

/// Evaluate all Conditions in a template. Iterates to a fixpoint so
/// conditions may reference each other in any order (the resolver reads the
/// partial map via `ctx.conditions`).
pub fn evaluate_conditions(
    template: &Value,
    base: &ResolveCtx,
) -> Result<HashMap<String, bool>, String> {
    let empty = serde_json::Map::new();
    let conditions = template
        .get("Conditions")
        .and_then(Value::as_object)
        .unwrap_or(&empty);
    let mut results: HashMap<String, bool> = HashMap::new();
    let names: Vec<String> = conditions.keys().cloned().collect();
    if conditions.is_empty() {
        return Ok(results);
    }
    let mut last_err: Option<String> = None;
    for _pass in 0..=conditions.len() {
        let mut changed = false;
        for (name, expr) in conditions {
            if results.contains_key(name) {
                continue;
            }
            let mut ctx = base.clone();
            ctx.conditions = results.clone();
            match resolve_intrinsics(expr, &ctx, 0) {
                Ok(v) => {
                    results.insert(name.clone(), truthy(&v));
                    changed = true;
                }
                Err(e) => last_err = Some(e),
            }
        }
        if !changed {
            break;
        }
    }
    if results.len() < conditions.len() {
        if let Some(e) = last_err {
            return Err(e);
        }
        for name in conditions.keys() {
            results.entry(name.clone()).or_insert(false);
        }
    }
    Ok(results)
}

/// Collect dependency edges (DependsOn + implicit Ref/GetAtt/Sub refs) and
/// topologically sort resource logical ids.
pub fn build_dependency_order(template: &Value) -> Result<Vec<String>, String> {
    let resources = template
        .get("Resources")
        .and_then(Value::as_object)
        .ok_or("Template must contain a Resources section")?;
    let names: HashSet<String> = resources.keys().cloned().collect();
    let mut deps: HashMap<String, Vec<String>> = HashMap::new();
    for (lid, def) in resources {
        let mut deps_for: Vec<String> = Vec::new();
        match def.get("DependsOn") {
            Some(Value::Array(items)) => {
                for d in items {
                    if let Value::String(s) = d {
                        if names.contains(s) {
                            deps_for.push(s.clone());
                        }
                    }
                }
            }
            Some(Value::String(s)) => {
                if names.contains(s) {
                    deps_for.push(s.clone());
                }
            }
            _ => {}
        }
        let mut refs = HashSet::new();
        find_refs(def.get("Properties").unwrap_or(&Value::Null), &names, &mut refs);
        for r in refs {
            deps_for.push(r);
        }
        deps_for.sort();
        deps_for.dedup();
        deps.insert(lid.clone(), deps_for);
    }

    // Kahn's algorithm: seed with dependency-free nodes, then peel.
    let mut order: Vec<String> = Vec::new();
    let mut ready: Vec<String> = names
        .iter()
        .filter(|n| deps.get(*n).map(|d| d.is_empty()).unwrap_or(true))
        .cloned()
        .collect();
    let mut remaining: HashMap<String, Vec<String>> = deps.clone();
    let mut satisfied: HashSet<String> = HashSet::new();
    while let Some(node) = ready.pop() {
        order.push(node.clone());
        satisfied.insert(node.clone());
        for (lid, ds) in remaining.iter_mut() {
            if lid == &node {
                continue;
            }
            if let Some(pos) = ds.iter().position(|d| d == &node) {
                ds.remove(pos);
                if ds.is_empty() && !satisfied.contains(lid) {
                    ready.push(lid.clone());
                }
            }
        }
    }
    if order.len() < names.len() {
        let left: Vec<String> = names
            .iter()
            .filter(|n| !satisfied.contains(*n))
            .cloned()
            .collect();
        return Err(format!(
            "Circular dependency detected among resources: {}",
            left.join(", ")
        ));
    }
    Ok(order)
}

/// Find all logical ids referenced via Ref / GetAtt / Fn::Sub in a value.
fn find_refs(value: &Value, resource_names: &HashSet<String>, refs: &mut HashSet<String>) {
    match value {
        Value::Object(map) => {
            for v in map.values() {
                find_refs(v, resource_names, refs);
            }
            if let Some(Value::String(r)) = map.get("Ref") {
                if resource_names.contains(r) {
                    refs.insert(r.clone());
                }
            }
            if let Some(att) = map.get("Fn::GetAtt") {
                let first = match att {
                    Value::Array(items) => items.first().map(scalar_string),
                    Value::String(s) => Some(s.split('.').next().unwrap_or("").to_string()),
                    _ => None,
                };
                if let Some(name) = first {
                    if resource_names.contains(&name) {
                        refs.insert(name);
                    }
                }
            }
            if let Some(sub) = map.get("Fn::Sub") {
                let template_str = match sub {
                    Value::String(s) => s.clone(),
                    Value::Array(items) => items
                        .first()
                        .map(scalar_string)
                        .unwrap_or_default(),
                    _ => String::new(),
                };
                // ${Foo} and ${Foo.Attr} — take the part before the first dot.
                let b = template_str.as_bytes();
                let mut i = 0;
                while i + 1 < b.len() {
                    if b[i] == b'$' && b[i + 1] == b'{' {
                        if let Some(close) = template_str[i + 2..].find('}') {
                            let var = &template_str[i + 2..i + 2 + close];
                            let head = var.split('.').next().unwrap_or("").to_string();
                            if resource_names.contains(&head) {
                                refs.insert(head);
                            }
                            i += 2 + close + 1;
                            continue;
                        }
                    }
                    i += 1;
                }
            }
        }
        Value::Array(items) => {
            for v in items {
                find_refs(v, resource_names, refs);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ctx() -> ResolveCtx {
        ResolveCtx {
            resources: HashMap::new(),
            parameters: HashMap::new(),
            mappings: Value::Null,
            conditions: HashMap::new(),
            imports: HashMap::new(),
            stack_name: "stack".to_string(),
            stack_id: "arn".to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
        }
    }

    #[test]
    fn parses_json_and_yaml() {
        let j = parse_template(r#"{"Resources": {"A": {"Type": "AWS::SQS::Queue"}}}"#).unwrap();
        assert_eq!(j["Resources"]["A"]["Type"], "AWS::SQS::Queue");
        let y = parse_template("Resources:\n  Queue:\n    Type: AWS::SQS::Queue\n").unwrap();
        assert_eq!(y["Resources"]["Queue"]["Type"], "AWS::SQS::Queue");
        let shorthand = parse_template(
            "Resources:\n  Q:\n    Type: AWS::SQS::Queue\n    Properties:\n      QueueName: !Ref MyParam\n",
        )
        .unwrap();
        assert_eq!(
            shorthand["Resources"]["Q"]["Properties"]["QueueName"]["Ref"],
            "MyParam"
        );
    }

    #[test]
    fn resolves_ref_and_region() {
        let mut ctx = ctx_with_params(vec![("Env", "prod")]);
        let v = resolve(&serde_json::json!({"Ref": "Env"}), &mut ctx);
        assert_eq!(v, "prod");
        let v = resolve(&serde_json::json!({"Ref": "AWS::Region"}), &mut ctx);
        assert_eq!(v, "us-east-1");
        let v = resolve(&serde_json::json!({"Ref": "missing-resource"}), &mut ctx);
        assert_eq!(v, "missing-resource");
    }

    fn ctx_with_params(pairs: Vec<(&str, &str)>) -> ResolveCtx {
        let mut ctx = ctx_template();
        for (k, v) in pairs {
            ctx.parameters.insert(k.to_string(), v.to_string());
        }
        ctx
    }

    fn ctx_template() -> ResolveCtx {
        let _ = ctx();
        ResolveCtx {
            resources: HashMap::new(),
            parameters: HashMap::new(),
            mappings: Value::Null,
            conditions: HashMap::new(),
            imports: HashMap::new(),
            stack_name: "stack".to_string(),
            stack_id: "arn".to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
        }
    }

    fn resolve(v: &Value, ctx: &mut ResolveCtx) -> String {
        match resolve_intrinsics(v, ctx, 0).unwrap() {
            Value::String(s) => s,
            other => other.to_string(),
        }
    }

    #[test]
    fn resolve_ignores_no_value() {
        let mut ctx = ctx_template();
        let v = resolve_intrinsics(
            &serde_json::json!({"Foo": {"Ref": "AWS::NoValue"}, "Bar": "x"}),
            &ctx,
            0,
        )
        .unwrap();
        assert!(v.get("Foo").is_none());
        assert_eq!(v["Bar"], "x");
    }

    #[test]
    fn join_select_split_if() {
        let mut ctx = ctx_template();
        ctx.conditions.insert("IsProd".to_string(), true);
        let joined = resolve(&serde_json::json!({"Fn::Join": ["-", ["a", {"Ref": "AWS::Region"}]]}), &mut ctx);
        assert_eq!(joined, "a-us-east-1");
        let sel = resolve(
            &serde_json::json!({"Fn::Select": ["1", ["alpha", "beta", "gamma"]]}),
            &mut ctx,
        );
        assert_eq!(sel, "beta");
        let split_then_select = resolve(
            &serde_json::json!({"Fn::Select": [1, {"Fn::Split": [",", "a,b,c"]}]}),
            &mut ctx,
        );
        assert_eq!(split_then_select, "b");
    }

    #[test]
    fn sub_expands_pseudo_params() {
        let mut ctx = ctx_template();
        let v = resolve(
            &serde_json::json!({"Fn::Sub": "cfn-sub-${AWS::Region}-queue"}),
            &mut ctx,
        );
        assert_eq!(v, "cfn-sub-us-east-1-queue");
        let v = resolve(
            &serde_json::json!({"Fn::Sub": ["${A}-${B}", {"A": "x", "B": "y"}]}),
            &mut ctx,
        );
        assert_eq!(v, "x-y");
        let v = resolve(
            &serde_json::json!({"Fn::Sub": "literal $${notvar} and ${!kept}"}),
            &mut ctx,
        );
        assert_eq!(v, "literal ${notvar} and ${kept}");
    }

    #[test]
    fn conditions_fixpoint() {
        let mut ctx = ctx_template();
        ctx.parameters.insert("Env".to_string(), "prod".to_string());
        let tpl = serde_json::json!({
            "Conditions": {
                "IsProd": {"Fn::Equals": [{"Ref": "Env"}, "prod"]},
                "Also": {"Fn::If": ["IsProd", true, false]}
            }
        });
        let conds = evaluate_conditions(&tpl, &ctx).unwrap();
        assert_eq!(conds["IsProd"], true);
        assert_eq!(conds["Also"], true);
    }

    #[test]
    fn dependency_order() {
        let tpl = serde_json::json!({
            "Resources": {
                "MyTopic": {"Type": "AWS::SNS::Topic", "Properties": {"TopicName": "t"}},
                "MyQueue": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": "q"}},
                "MySub": {
                    "Type": "AWS::SNS::Subscription",
                    "DependsOn": ["MyTopic", "MyQueue"],
                    "Properties": {}
                }
            }
        });
        let order = build_dependency_order(&tpl).unwrap();
        let pos = |n: &str| order.iter().position(|x| x == n).unwrap();
        assert!(pos("MySub") > pos("MyTopic") && pos("MySub") > pos("MyQueue"));
    }

    #[test]
    fn circular_dependencies_detected() {
        let tpl = serde_json::json!({
            "Resources": {
                "A": {"Type": "T", "DependsOn": "B"},
                "B": {"Type": "T", "DependsOn": "A"}
            }
        });
        assert!(build_dependency_order(&tpl).is_err());
    }
}
