//! Port of moto's IAM policy-document validator (iam/policy_validation.py)
//! onto robotocore's policy put/create surfaces: PutUserPolicy,
//! PutRolePolicy, PutGroupPolicy, CreatePolicy and CreatePolicyVersion run
//! the managed-policy flavor; UpdateAssumeRolePolicy runs the trust flavor.
//! Messages are moto's verbatim sentences and the layer order follows
//! moto's short-circuiting validate(): syntax → version → legacy parsing →
//! Sid uniqueness → action existence → accumulated resource errors →
//! action prefixes → flavor extras. Verified live against moto 5.2.5 on a
//! 28-shape matrix (2026-11-08).

use serde_json::Value;

pub const MSG_SYNTAX: &str = "Syntax errors in policy.";
pub const MSG_VERSION: &str = "Policy document must be version 2012-10-17 or greater.";
pub const MSG_LEGACY: &str = "The policy failed legacy parsing";
pub const MSG_SID: &str = "Statement IDs (SID) in a single policy must be unique.";
pub const MSG_ACTIONS: &str = "Policy statement must contain actions.";
pub const MSG_RESOURCES: &str = "Policy statement must contain resources.";
pub const MSG_TRUST_ACTIONS: &str = "Trust Policy statement actions can only be \
     sts:AssumeRole, sts:AssumeRoleWithSAML,  and sts:AssumeRoleWithWebIdentity";
pub const MSG_PROHIBITED: &str = "Has prohibited field Resource.";
const MSG_ACTION_VENDOR: &str =
    "Actions/Conditions must be prefaced by a vendor, e.g., iam, sdb, ec2, etc.";
const MSG_ACTION_COLON: &str = "Actions/Condition can contain only one colon.";
const MSG_IAM_PATH: &str = "IAM resource path must either be \"*\" or start with \
     user/, federated-user/, role/, group/, instance-profile/, mfa/, \
     server-certificate/, policy/, sms-mfa/, saml-provider/, oidc-provider/, \
     report/, access-report/.";

const VALID_TOP: [&str; 4] = ["Version", "Id", "Statement", "Conditions"];
const VALID_VERSIONS: [&str; 2] = ["2008-10-17", "2012-10-17"];
const VALID_STATEMENT_ELEMENTS: [&str; 9] = [
    "Sid",
    "Action",
    "NotAction",
    "Resource",
    "NotResource",
    "Effect",
    "Principal",
    "NotPrincipal",
    "Condition",
];
const VALID_EFFECTS: [&str; 2] = ["Allow", "Deny"];
const VALID_CONDITION_PREFIXES: [&str; 2] = ["ForAnyValue:", "ForAllValues:"];
const VALID_CONDITION_POSTFIX: [&str; 1] = ["IfExists"];
// moto policy_validation.py:26-54 (verbatim)
const VALID_CONDITIONS: [&str; 27] = [
    "StringEquals",
    "StringNotEquals",
    "StringEqualsIgnoreCase",
    "StringNotEqualsIgnoreCase",
    "StringLike",
    "StringNotLike",
    "NumericEquals",
    "NumericNotEquals",
    "NumericLessThan",
    "NumericLessThanEquals",
    "NumericGreaterThan",
    "NumericGreaterThanEquals",
    "DateEquals",
    "DateNotEquals",
    "DateLessThan",
    "DateLessThanEquals",
    "DateGreaterThan",
    "DateGreaterThanEquals",
    "Bool",
    "BinaryEquals",
    "IpAddress",
    "NotIpAddress",
    "ArnEquals",
    "ArnLike",
    "ArnNotEquals",
    "ArnNotLike",
    "Null",
];
// moto utilities.utils.PARTITION_NAMES (region-prefix map values + default)
const PARTITION_NAMES: [&str; 6] = [
    "aws",
    "aws-cn",
    "aws-eusc",
    "aws-iso",
    "aws-iso-b",
    "aws-us-gov",
];
// moto policy_validation.py:71-90 (IAM flavor resource paths)
const IAM_RESOURCE_PATH_STARTS: [&str; 13] = [
    "user/",
    "federated-user/",
    "role/",
    "group/",
    "instance-profile/",
    "mfa/",
    "server-certificate/",
    "policy/",
    "sms-mfa/",
    "saml-provider/",
    "oidc-provider/",
    "report/",
    "access-report/",
];
// moto IAMTrustPolicyDocumentValidator._validate_trust_policy_action
// (policy_validation.py:591-605)
const TRUST_ACTIONS: [&str; 11] = [
    "sts:AssumeRole",
    "sts:AssumeRoleWithSAML",
    "sts:AssumeRoleWithWebIdentity",
    "sts:DecodeAuthorizationMessage",
    "sts:GetAccessKeyInfo",
    "sts:GetCallerIdentity",
    "sts:GetFederationToken",
    "sts:GetServiceBearerToken",
    "sts:GetSessionToken",
    "sts:SetSourceIdentity",
    "sts:TagSession",
];

/// Validate a policy document. `trust` selects moto's
/// IAMTrustPolicyDocumentValidator flavor (UpdateAssumeRolePolicy); the
/// managed flavor otherwise. The error string is moto's exact
/// MalformedPolicyDocument message.
pub fn validate_document(doc: &str, trust: bool) -> Result<(), String> {
    // Layer 1 — _validate_syntax: JSON that fails to parse, is not an
    // object, has unknown top-level members, an off-whitelist Version, a
    // non-string Id, an illegal Statement shape, or any statement-level
    // syntax breach surfaces moto's generic syntax message.
    let json: Value = serde_json::from_str(doc).map_err(|_| MSG_SYNTAX.to_string())?;
    let statements = statements_of(&json).map_err(|_| MSG_SYNTAX.to_string())?;
    for stmt in &statements {
        statement_syntax_ok(stmt).map_err(|_| MSG_SYNTAX.to_string())?;
    }

    // Layer 2 — Version strictly 2012-10-17 (a missing Version lands here
    // too: moto's bar raises when the member is absent).
    if json.get("Version").and_then(|v| v.as_str()) != Some("2012-10-17") {
        return Err(MSG_VERSION.to_string());
    }

    // Layer 3 — legacy parsing: case-SENSITIVE Effect membership and
    // Date-condition token checks; then resource formats accumulate the
    // error, and moto's per-statement legacy resource assertions run only
    // where no format error was recorded yet.
    let mut resource_error = String::new();
    for stmt in &statements {
        legacy_statement(stmt).map_err(|_| MSG_LEGACY.to_string())?;
    }
    for key in ["Resource", "NotResource"] {
        for stmt in &statements {
            resource_formats(stmt, key, &mut resource_error);
            if resource_error.is_empty() {
                legacy_resource_like(stmt, key).map_err(|_| MSG_LEGACY.to_string())?;
            }
        }
    }

    // Layer 4 — Sid uniqueness (empty Sids are skipped, moto's truthiness).
    let mut sids: Vec<&str> = Vec::new();
    for stmt in &statements {
        if let Some(sid) = stmt.get("Sid").and_then(|v| v.as_str()) {
            if !sid.is_empty() {
                if sids.contains(&sid) {
                    return Err(MSG_SID.to_string());
                }
                sids.push(sid);
            }
        }
    }

    // Layer 5 — actions presence.
    for stmt in &statements {
        let action = stmt.get("Action");
        let not_action = stmt.get("NotAction");
        let empty_list = |v: Option<&Value>| matches!(v, Some(Value::Array(a)) if a.is_empty());
        if action.is_none() && not_action.is_none() {
            return Err(MSG_ACTIONS.to_string());
        }
        if empty_list(action) || empty_list(not_action) {
            return Err(MSG_ACTIONS.to_string());
        }
    }

    // Layer 6 — moto raises the accumulated resource-format error only
    // after the earlier layers pass.
    if !resource_error.is_empty() {
        return Err(resource_error);
    }

    // Layer 7 — action vendor prefixes: these raise their own direct
    // messages. moto would crash (HTTP 500) on a null list element;
    // the twin answers moto's vendor message instead.
    for stmt in &statements {
        for key in ["Action", "NotAction"] {
            match stmt.get(key) {
                Some(Value::String(s)) => action_prefix(s)?,
                Some(Value::Array(items)) => {
                    for item in items {
                        match item.as_str() {
                            Some(s) => action_prefix(s)?,
                            None => return Err(MSG_ACTION_VENDOR.to_string()),
                        }
                    }
                }
                _ => {}
            }
        }
    }

    if trust {
        // Flavor extras (IAMTrustPolicyDocumentValidator.validate):
        // whitelisted sts actions, then no Resource/NotResource anywhere.
        for stmt in &statements {
            match stmt.get("Action") {
                Some(Value::String(s)) => {
                    if !TRUST_ACTIONS.contains(&s.as_str()) {
                        return Err(MSG_TRUST_ACTIONS.to_string());
                    }
                }
                Some(Value::Array(items)) => {
                    for item in items {
                        let Some(s) = item.as_str() else {
                            return Err(MSG_TRUST_ACTIONS.to_string());
                        };
                        if !TRUST_ACTIONS.contains(&s) {
                            return Err(MSG_TRUST_ACTIONS.to_string());
                        }
                    }
                }
                _ => return Err(MSG_TRUST_ACTIONS.to_string()),
            }
        }
        for stmt in &statements {
            if stmt.get("Resource").is_some() || stmt.get("NotResource").is_some() {
                return Err(MSG_PROHIBITED.to_string());
            }
        }
    } else {
        // Managed-policy flavor: every statement carries a Resource or a
        // NotResource (a list member must be non-empty).
        for stmt in &statements {
            let empty_list = |v: Option<&Value>| matches!(v, Some(Value::Array(a)) if a.is_empty());
            if stmt.get("Resource").is_none() && stmt.get("NotResource").is_none() {
                return Err(MSG_RESOURCES.to_string());
            }
            if empty_list(stmt.get("Resource")) || empty_list(stmt.get("NotResource")) {
                return Err(MSG_RESOURCES.to_string());
            }
        }
    }
    Ok(())
}

// -- layer 1: syntax ---------------------------------------------------------

/// `_validate_top_elements` + `_validate_version_syntax` +
/// `_validate_id_syntax` + `_validate_statements_syntax`: Statement is an
/// object (single statement) or a non-empty array.
fn statements_of(json: &Value) -> Result<Vec<Value>, ()> {
    let obj = json.as_object().ok_or(())?;
    for k in obj.keys() {
        if !VALID_TOP.contains(&k.as_str()) {
            return Err(());
        }
    }
    if let Some(ver) = obj.get("Version") {
        let Some(s) = ver.as_str() else {
            return Err(());
        };
        if !VALID_VERSIONS.contains(&s) {
            return Err(());
        }
    }
    if let Some(id) = obj.get("Id") {
        if !id.is_string() {
            return Err(());
        }
    }
    match obj.get("Statement") {
        Some(Value::Object(o)) => Ok(vec![Value::Object(o.clone())]),
        Some(Value::Array(items)) if !items.is_empty() => Ok(items.clone()),
        _ => Err(()),
    }
}

/// _validate_statement_syntax (policy_validation.py:176-263).
fn statement_syntax_ok(stmt: &Value) -> Result<(), ()> {
    let obj = stmt.as_object().ok_or(())?;
    for k in obj.keys() {
        if !VALID_STATEMENT_ELEMENTS.contains(&k.as_str()) {
            return Err(());
        }
    }
    if obj.contains_key("Resource") && obj.contains_key("NotResource") {
        return Err(());
    }
    if obj.contains_key("Action") && obj.contains_key("NotAction") {
        return Err(());
    }
    let eff = obj.get("Effect").and_then(|v| v.as_str()).ok_or(())?;
    let lowered = eff.to_lowercase();
    if lowered != "allow" && lowered != "deny" {
        return Err(());
    }
    string_or_list_of_strings(obj.get("Action"))?;
    string_or_list_of_strings(obj.get("NotAction"))?;
    string_or_list_of_strings(obj.get("Resource"))?;
    string_or_list_of_strings(obj.get("NotResource"))?;
    condition_syntax_ok(obj.get("Condition"))?;
    if let Some(sid) = obj.get("Sid") {
        if !sid.is_string() {
            return Err(());
        }
    }
    Ok(())
}

fn string_or_list_of_strings(v: Option<&Value>) -> Result<(), ()> {
    match v {
        None => Ok(()), // asserted only when the member is present
        Some(Value::String(_)) => Ok(()),
        Some(Value::Array(items)) => {
            if items.iter().all(|x| x.is_string() || x.is_null()) {
                Ok(())
            } else {
                Err(())
            }
        }
        Some(_) => Err(()),
    }
}

/// _validate_condition_syntax (cond dict of dicts; values bool|str|list;
/// no-op for a stripped-unknown operator → empty value required).
fn condition_syntax_ok(v: Option<&Value>) -> Result<(), ()> {
    let Some(cond) = v else { return Ok(()) };
    let obj = cond.as_object().ok_or(())?;
    for (key, val) in obj {
        let container = val.as_object().ok_or(())?;
        for element in container.values() {
            if !element.is_boolean() && !element.is_string() && !element.is_array() {
                return Err(());
            }
        }
        if !VALID_CONDITIONS.contains(&strip_condition_key(key).as_str()) && !container.is_empty() {
            return Err(());
        }
    }
    Ok(())
}

// _strip_condition_key (251-263): strip ONE prefix, then ONE postfix.
fn strip_condition_key(key: &str) -> String {
    let mut k = key.to_string();
    for p in VALID_CONDITION_PREFIXES {
        if let Some(rest) = k.strip_prefix(p) {
            k = rest.to_string();
            break;
        }
    }
    for p in VALID_CONDITION_POSTFIX {
        if let Some(rest) = k.strip_suffix(p) {
            k = rest.to_string();
            break;
        }
    }
    k
}

// -- layer 3: legacy parsing -------------------------------------------------

/// _legacy_parse_statement: case-SENSITIVE Effect membership plus the
/// Date-condition literal validation.
fn legacy_statement(stmt: &Value) -> Result<(), ()> {
    let obj = stmt.as_object().ok_or(())?;
    let eff = obj.get("Effect").and_then(|v| v.as_str()).ok_or(())?;
    if !VALID_EFFECTS.contains(&eff) {
        return Err(());
    }
    if let Some(cond) = obj.get("Condition").and_then(|c| c.as_object()) {
        for (key, val) in cond {
            if strip_condition_key(key).starts_with("Date") {
                let container = val.as_object().ok_or(())?;
                for element in container.values() {
                    match element {
                        Value::String(s) => legacy_date_value(s)?,
                        // moto iterates anything else as a list; non-lists
                        // raise TypeError → the legacy layer catches it.
                        Value::Array(items) => {
                            for item in items {
                                legacy_date_item(item)?;
                            }
                        }
                        _ => return Err(()),
                    }
                }
            }
        }
    }
    Ok(())
}

fn legacy_date_item(item: &Value) -> Result<(), ()> {
    match item {
        Value::String(s) => legacy_date_value(s),
        Value::Number(n) => {
            // python int(x) truncates floats; bools are 0/1.
            let as_int = if let Some(i) = n.as_i64() {
                i128::from(i)
            } else if let Some(f) = n.as_f64() {
                f.trunc() as i128
            } else {
                return Err(());
            };
            if (0..=9223372036854775807).contains(&as_int) {
                Ok(())
            } else {
                Err(())
            }
        }
        _ => Err(()),
    }
}

/// _legacy_parse_date_condition_value (+ _validate_iso_8601_datetime,
/// policy_validation.py:486-544); moto lowercases before the ISO path.
fn legacy_date_value(raw: &str) -> Result<(), ()> {
    let lowered = raw.to_lowercase();
    if lowered.contains('t') || lowered.contains('-') {
        validate_iso_8601(&lowered)
    } else {
        // `0 <= int(x) <= 2^63-1`; int() accepts an optional sign and
        // surrounding whitespace but no decimal point.
        let v: i128 = raw.trim().parse().map_err(|_| ())?;
        if (0..=9223372036854775807).contains(&v) {
            Ok(())
        } else {
            Err(())
        }
    }
}

fn py_int(s: &str) -> Result<i128, ()> {
    s.trim().parse::<i128>().map_err(|_| ())
}

fn validate_iso_8601(s: &str) -> Result<(), ()> {
    let (date_part, _, time_part) = partition(s, 't');
    let negative_year = date_part.starts_with('-');
    let date_core = if negative_year {
        &date_part[1..]
    } else {
        date_part
    };
    let date_parts: Vec<&str> = date_core.split('-').collect();
    let year_str = if negative_year {
        format!("-{}", date_parts[0])
    } else {
        date_parts[0].to_string()
    };
    let year: i128 = py_int(&year_str)?;
    if !(-292275054..=292278993).contains(&year) {
        return Err(());
    }
    if date_parts.len() > 1 {
        let month: i128 = py_int(date_parts[1])?;
        if !(1..=12).contains(&month) {
            return Err(());
        }
    }
    if date_parts.len() > 2 {
        let day: i128 = py_int(date_parts[2])?;
        if !(1..=31).contains(&day) {
            return Err(());
        }
    }
    if date_parts.len() >= 4 {
        return Err(());
    }
    let time_parts: Vec<&str> = time_part.split(':').collect();
    if !time_parts[0].is_empty() {
        let hours: i128 = py_int(time_parts[0])?;
        if !(0..=23).contains(&hours) {
            return Err(());
        }
    }
    if time_parts.len() > 1 {
        let minutes: i128 = py_int(time_parts[1])?;
        if !(0..=59).contains(&minutes) {
            return Err(());
        }
    }
    if time_parts.len() > 2 {
        let secs_field = time_parts[2];
        let (seconds_with_frac, tz_ok) = if secs_field.contains('z') {
            let (before, after) = secs_field.split_once('z').ok_or(())?;
            (before, after.is_empty())
        } else if secs_field.contains('+') {
            let (before, after) = secs_field.split_once('+').ok_or(())?;
            let (tz_hours, tz_sep, tz_minutes) = partition(after, ':');
            if tz_hours.len() != 2 {
                return Err(());
            }
            let tz_hours_v: i128 = py_int(tz_hours)?;
            if !(0..=23).contains(&tz_hours_v) {
                return Err(());
            }
            if tz_sep == ":" {
                if tz_minutes.len() != 2 {
                    return Err(());
                }
                let tz_minutes_v: i128 = py_int(tz_minutes)?;
                if !(0..=59).contains(&tz_minutes_v) {
                    return Err(());
                }
            }
            (before, true)
        } else {
            (secs_field, true)
        };
        if !tz_ok {
            return Err(());
        }
        let (secs, frac) = match seconds_with_frac.split_once('.') {
            Some((s, f)) => (s, f),
            None => (seconds_with_frac, ""),
        };
        let seconds: i128 = py_int(secs)?;
        if !(0..=59).contains(&seconds) {
            return Err(());
        }
        if !frac.is_empty() {
            let frac_v: i128 = py_int(frac)?;
            if !(0..=999999999).contains(&frac_v) {
                return Err(());
            }
        }
    }
    Ok(())
}

// -- resource formats ---------------------------------------------------------

/// _validate_resources_for_formats / _validate_not_resources_for_formats:
/// strings validated directly; lists in reverse-sorted order (moto sorts
/// then reverses), skipping empty/None elements.
fn resource_formats(stmt: &Value, key: &str, resource_error: &mut String) {
    let Some(member) = stmt.get(key) else { return };
    match member {
        Value::String(r) => resource_format(r, resource_error),
        Value::Array(items) => {
            let mut resources: Vec<&str> = items
                .iter()
                .filter_map(|x| x.as_str())
                .filter(|s| !s.is_empty())
                .collect();
            resources.sort();
            resources.reverse();
            for r in resources {
                resource_format(r, resource_error);
            }
        }
        _ => {}
    }
}

/// _validate_resource_format (policy_validation.py:344-437) — moto's
/// assignment to _resource_error is unconditional, so later errors
/// overwrite earlier ones before the layer-6 raise.
fn resource_format(resource: &str, resource_error: &mut String) {
    if resource == "*" {
        return;
    }
    let (_before1, sep1, tail1) = partition(resource, ':');
    if sep1.is_empty() {
        *resource_error = format!("Resource {resource} must be in ARN format or \"*\".");
        return;
    }
    let (partition_name, sep2, rest1) = partition(tail1, ':');
    if partition_name != "*" && !PARTITION_NAMES.contains(&partition_name) {
        let remaining: Vec<&str> = rest1.split(':').collect();
        let arn1 = if !remaining[0].is_empty() || remaining.len() > 1 {
            remaining[0]
        } else {
            "*"
        };
        let arn2 = remaining.get(1).copied().unwrap_or("*");
        let arn3 = remaining.get(2).copied().unwrap_or("*");
        let arn4 = if remaining.len() > 3 {
            remaining[3..].join(":")
        } else {
            "*".to_string()
        };
        *resource_error = format!(
            "Partition \"{partition_name}\" is not valid for resource \
             \"arn:{partition_name}:{arn1}:{arn2}:{arn3}:{arn4}\"."
        );
        return;
    }
    if sep2.is_empty() {
        *resource_error =
            "Resource vendor must be fully qualified and cannot contain regexes.".to_string();
        return;
    }
    // Sequential partitions mirror moto's nested reassignments:
    // p3 → (service, ":", rest3); region tracks the FULL rest3; p4/p5
    // re-partition the remainder and the resource id is p5's tail.
    let (service, _, rest3) = partition(rest1, ':');
    let region = rest3.to_string();
    let (_, _, rest4) = partition(rest3, ':');
    let (_, _, rest5) = partition(rest4, ':');
    let resource_id = rest5;
    if !region.starts_with(':') {
        match (service, resource_id.starts_with("accesspoint/")) {
            ("iam", _) => {
                *resource_error =
                    format!("IAM resource {resource} cannot contain region information.");
                return;
            }
            ("s3", false) => {
                *resource_error =
                    format!("Resource {resource} can not contain region information.");
                return;
            }
            _ => {}
        }
    }
    if service == "iam" {
        let valid = IAM_RESOURCE_PATH_STARTS
            .iter()
            .any(|start| resource_id.starts_with(start));
        if !valid {
            *resource_error = MSG_IAM_PATH.to_string();
        }
    }
}

/// _legacy_parse_resource_like (policy_validation.py:454-464).
fn legacy_resource_like(stmt: &Value, key: &str) -> Result<(), ()> {
    let Some(member) = stmt.get(key) else {
        return Ok(());
    };
    match member {
        Value::String(r) => {
            if r != "*" {
                if r.matches(':').count() < 5 && r.contains("::") {
                    return Err(());
                }
                let segs: Vec<&str> = r.split(':').collect();
                if segs.len() < 3 || segs[2].is_empty() {
                    return Err(());
                }
            }
            Ok(())
        }
        Value::Array(items) => {
            for item in items {
                // python filters out None and "" with truthiness.
                let Some(r) = item.as_str() else { continue };
                if r.is_empty() || r == "*" {
                    continue;
                }
                if r.matches(':').count() < 5 && r.contains("::") {
                    return Err(());
                }
                // python `resource[2] != ""` subsists on the THIRD
                // CHARACTER: shorter strings IndexError → legacy fail.
                if r.len() < 3 {
                    return Err(());
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// python str.partition(sep) semantics.
fn partition(s: &str, sep: char) -> (&str, &str, &str) {
    match s.find(sep) {
        Some(idx) => (
            &s[..idx],
            &s[idx..idx + sep.len_utf8()],
            &s[idx + sep.len_utf8()..],
        ),
        None => (s, "", ""),
    }
}

// -- layer 7: action vendor prefixes ------------------------------------------

fn action_prefix(action: &str) -> Result<(), String> {
    let parts: Vec<&str> = action.split(':').collect();
    if parts.len() == 1 && parts[0] != "*" {
        return Err(MSG_ACTION_VENDOR.to_string());
    } else if parts.len() > 2 {
        return Err(MSG_ACTION_COLON.to_string());
    }
    let bad_vendor = !parts[0]
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '.');
    if parts[0] != "*" && bad_vendor {
        return Err(format!("Vendor {} is not valid", parts[0]));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const OK: &str = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:*"],"Resource":"*"}]}"#;

    #[test]
    fn accepts_a_valid_policy() {
        assert_eq!(validate_document(OK, false), Ok(()));
    }

    #[test]
    fn rejects_syx() {
        assert_eq!(validate_document("not json", false), Err(MSG_SYNTAX.into()));
    }

    #[test]
    fn rejects_missing_version() {
        let d = json_missing_version();
        assert_eq!(
            validate_document(&d, false),
            Err("Policy document must be version 2012-10-17 or greater.".to_string())
        );
    }

    fn json_missing_version() -> String {
        r#"{"Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#.to_string()
    }

    #[test]
    fn orders_actions_before_resources() {
        let d = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow"}]}"#;
        assert_eq!(validate_document(d, false), Err(MSG_ACTIONS.to_string()));
    }

    #[test]
    fn trust_policy_prohibits_resource() {
        let d = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"sts:AssumeRole","Principal":{"AWS":"*"},"Resource":"*"}]}"#;
        assert_eq!(validate_document(d, true), Err(MSG_PROHIBITED.to_string()));
    }

    #[test]
    fn accepts_trust_happy() {
        let d = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"sts:AssumeRole","Principal":{"AWS":"*"}}]}"#;
        assert_eq!(validate_document(d, true), Ok(()));
    }
}
