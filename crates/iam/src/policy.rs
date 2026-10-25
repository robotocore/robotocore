//! IAM policy evaluation engine.
//!
//! Decision-layer semantics come from moto's `moto/iam/access_control.py`
//! evaluator (`IAMPolicy` / `IAMPolicyStatement`): statement-by-statement
//! evaluation where an explicit Deny short-circuits the whole request,
//! matching not only Effect/Action but also Resource/NotResource and
//! Condition limbs, wildcard matching with `*` → `.*` regex semantics
//! (so multi-star patterns work), and trust-policy-style Principal checks.
//!
//! Condition operators are ported from the Python-era engine
//! (`src/robotocore/services/iam/conditions.py` @ 6448ae45^): the string,
//! numeric, date, boolean, ARN and null operator families plus
//! `IfExists` suffixes and `ForAllValues:`/`ForAnyValue:` set prefixes.
//! Condition keys with no value in the supplied context count as
//! unsatisfied and are reported back as missing context values, which is
//! how the AWS Simulate APIs behave.

use serde_json::Value;
use std::collections::HashMap;

/// Effect limb of a statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Effect {
    Allow,
    Deny,
}

/// Final decision for one action/resource pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Allowed,
    ExplicitDeny,
    ImplicitDeny,
}

/// Result of evaluating a whole policy set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Evaluation {
    pub decision: Decision,
    /// Statement summary lines (one per statement that applied), each
    /// tagged with the SourcePolicyId the caller assigned to its policy.
    pub matched: Vec<String>,
    /// Condition keys referenced by otherwise-applicable statements but
    /// not present in the request context.
    pub missing_context: Vec<String>,
}

impl Evaluation {
    /// Combine the identity decision with an optional resource-policy
    /// decision. Mirrors AWS cross-account semantics: same-account access
    /// needs an allow from either side; cross-account access needs one from
    /// both; a deny anywhere is an explicit deny.
    pub fn combine_with_resource_policy(
        &self,
        rp: Option<&Evaluation>,
        cross_account: bool,
    ) -> Evaluation {
        let rp = match rp {
            Some(rp) => rp,
            None => return self.clone(),
        };
        let decision = match (self.decision, rp.decision) {
            (Decision::ExplicitDeny, _) | (_, Decision::ExplicitDeny) => Decision::ExplicitDeny,
            (Decision::Allowed, Decision::Allowed) => Decision::Allowed,
            (Decision::Allowed, Decision::ImplicitDeny)
            | (Decision::ImplicitDeny, Decision::Allowed) => {
                if cross_account {
                    Decision::ImplicitDeny
                } else {
                    Decision::Allowed
                }
            }
            (Decision::ImplicitDeny, Decision::ImplicitDeny) => Decision::ImplicitDeny,
        };
        let mut combined = self.clone();
        combined.matched.extend(rp.matched.clone());
        for k in rp.missing_context.clone() {
            if !combined.missing_context.contains(&k) {
                combined.missing_context.push(k);
            }
        }
        combined.decision = decision;
        combined
    }
}

/// Condition keys/values supplied with a simulate request (ContextEntries).
#[derive(Debug, Default, Clone)]
pub struct ContextValues {
    values: HashMap<String, Vec<String>>,
}

impl ContextValues {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, key: String, values: Vec<String>) {
        self.values.insert(key, values);
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// First context value for a key, if present.
    fn value(&self, key: &str) -> Option<&str> {
        self.values
            .get(key)
            .and_then(|v| v.first())
            .map(|s| s.as_str())
    }

    /// Every context value for a key, if present.
    fn values_of(&self, key: &str) -> Option<&Vec<String>> {
        self.values.get(key)
    }

    fn contains_key(&self, key: &str) -> bool {
        self.values.contains_key(key)
    }
}

/// Substitute policy variables (`${aws:username}`) from the context.
/// Unresolvable variables are left untouched.
fn substitute_variables(value: &str, context: &ContextValues) -> String {
    let mut out = String::with_capacity(value.len());
    let mut rest = value;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        match after.find('}') {
            Some(end) => {
                let var_name = &after[..end];
                match context.value(var_name) {
                    Some(v) => out.push_str(v),
                    None => out.push_str("${"),
                }
                rest = if context.contains_key(var_name) {
                    &after[end + 1..]
                } else {
                    &after[end..]
                };
            }
            None => {
                out.push_str("${");
                rest = after;
            }
        }
    }
    out.push_str(rest);
    out
}

/// IAM wildcard matching. Only `*` is a wildcard (no `?` or `[]`), matching
/// moto's `IAMPolicyStatement._match` (`pattern.replace("*", ".*")` full
/// regex match) and the era engine's `_iam_match`. Multi-star patterns
/// (e.g. `arn:aws:s3:*:*:bucket/*`) work like the equivalent regex.
pub fn wildcard_match(pattern: &str, text: &str, case_insensitive: bool) -> bool {
    let (pattern, text) = if case_insensitive {
        (pattern.to_lowercase(), text.to_lowercase())
    } else {
        (pattern.to_string(), text.to_string())
    };
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    glob_match_chars(&p, &t)
}

/// Iterative glob matcher with `*` backtracking (equivalent to a
/// `*` → `.*` full-match regex).
fn glob_match_chars(p: &[char], t: &[char]) -> bool {
    let mut pi = 0usize;
    let mut ti = 0usize;
    let mut star: Option<(usize, usize)> = None;
    while ti < t.len() {
        if pi < p.len() && p[pi] == '*' {
            star = Some((pi + 1, ti));
            pi += 1;
        } else if pi < p.len() && p[pi] == t[ti] {
            pi += 1;
            ti += 1;
        } else if let Some((star_pat, star_txt)) = star {
            pi = star_pat;
            star = Some((star_pat, star_txt + 1));
            ti = star_txt + 1;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

fn any_match(patterns: &[&str], text: &str, case_insensitive: bool) -> bool {
    patterns
        .iter()
        .any(|p| wildcard_match(p, text, case_insensitive))
}

fn limb_strings(v: &Value, name: &str) -> Vec<String> {
    match v.get(name) {
        Some(Value::String(s)) => vec![s.clone()],
        Some(Value::Array(a)) => a
            .iter()
            .filter_map(|x| x.as_str().map(String::from))
            .collect(),
        _ => Vec::new(),
    }
}

/// Statements of a policy document, handling single-object or array form.
/// Each element is either the `Statement` array's items or, for the
/// single-statement form, the object of `Statement` itself.
pub fn statements_of(doc: &Value) -> Vec<&Value> {
    match doc.get("Statement").or_else(|| doc.get("statement")) {
        Some(Value::Array(arr)) => arr.iter().collect(),
        Some(s @ Value::Object(_)) => vec![s],
        _ => Vec::new(),
    }
}

fn parse_effect(stmt: &Value) -> Option<Effect> {
    match stmt.get("Effect").and_then(|e| e.as_str()) {
        Some("Allow") => Some(Effect::Allow),
        Some("Deny") => Some(Effect::Deny),
        _ => None,
    }
}

/// Evaluate one statement. Returns the effect if and only if the statement
/// applies to the action/resource pair with its conditions satisfied.
/// Condition keys that are absent from the context are collected as
/// `missing_context` when the statement was otherwise applicable.
///
/// Mirrors moto `IAMPolicyStatement.is_action_permitted` plus the era
/// engine's Resource/NotAction/NotResource and condition port:
/// - no Resource/NotResource limb → applies to * (trust-policy shape)
/// - conditions unsatisfied → statement does not apply (no effect)
pub fn evaluate_statement(
    stmt: &Value,
    action: &str,
    resource: &str,
    context: &ContextValues,
    missing_context: &mut Vec<String>,
) -> Option<Effect> {
    let effect = parse_effect(stmt)?;
    let action_concerned = if stmt.get("NotAction").is_some() {
        let not_actions = limb_strings(stmt, "NotAction");
        let refs = not_actions.iter().map(String::as_str).collect::<Vec<_>>();
        !any_match(&refs, action, true)
    } else {
        let actions = limb_strings(stmt, "Action");
        let refs = actions.iter().map(String::as_str).collect::<Vec<_>>();
        any_match(&refs, action, true)
    };
    if !action_concerned {
        return None;
    }
    if !resource_limb_matches(stmt, resource, context) {
        return None;
    }
    if let Some(condition) = stmt.get("Condition") {
        let result = evaluate_condition_block(condition, context);
        for key in result.missing_keys {
            if !missing_context.contains(&key) {
                missing_context.push(key);
            }
        }
        if !result.satisfied {
            return None;
        }
    }
    Some(effect)
}

fn resource_limb_matches(stmt: &Value, resource: &str, context: &ContextValues) -> bool {
    if stmt.get("Resource").is_some() {
        let resources = limb_strings(stmt, "Resource");
        let resolved: Vec<String> = resources
            .iter()
            .map(|r| substitute_variables(r, context))
            .collect();
        let refs = resolved.iter().map(String::as_str).collect::<Vec<_>>();
        any_match(&refs, resource, false)
    } else if stmt.get("NotResource").is_some() {
        let not_resources = limb_strings(stmt, "NotResource");
        let resolved: Vec<String> = not_resources
            .iter()
            .map(|r| substitute_variables(r, context))
            .collect();
        let refs = resolved.iter().map(String::as_str).collect::<Vec<_>>();
        !any_match(&refs, resource, false)
    } else {
        true
    }
}

/// Evaluate a list of policy documents (identity policies) for one
/// action/resource pair.
///
/// Explicit Deny beats explicit Allow beats implicit deny, with Deny
/// short-circuiting the whole evaluation (moto decision layer).
pub fn evaluate_policy_docs(
    docs: &[(String, Value)],
    action: &str,
    resource: &str,
    context: &ContextValues,
) -> Evaluation {
    let mut matched = Vec::new();
    let mut missing_context: Vec<String> = Vec::new();
    let mut has_allow = false;
    for (policy_id, doc) in docs {
        for stmt in statements_of(doc) {
            let mut stmt_missing: Vec<String> = Vec::new();
            let applies = evaluate_statement(stmt, action, resource, context, &mut stmt_missing);
            for key in stmt_missing {
                if !missing_context.contains(&key) {
                    missing_context.push(key);
                }
            }
            if let Some(effect) = applies {
                matched.push(policy_id.clone());
                if effect == Effect::Deny {
                    return Evaluation {
                        decision: Decision::ExplicitDeny,
                        matched,
                        missing_context,
                    };
                }
                has_allow = true;
            }
        }
    }
    Evaluation {
        decision: if has_allow {
            Decision::Allowed
        } else {
            Decision::ImplicitDeny
        },
        matched,
        missing_context,
    }
}

/// Evaluate a resource-based policy (has Principal limbs) for a caller ARN.
/// Ported from the era engine's `evaluate_resource_policy` /
/// `_principal_matches` (moto checks the same shape but only handles `*`).
pub fn evaluate_resource_policy(
    doc: &Value,
    caller_arn: Option<&str>,
    action: &str,
    resource: &str,
    context: &ContextValues,
) -> Evaluation {
    let mut matched = Vec::new();
    let mut missing_context: Vec<String> = Vec::new();
    let mut has_allow = false;
    for stmt in statements_of(doc) {
        if let Some(caller) = caller_arn {
            if !principal_matches(stmt, caller) {
                continue;
            }
        }
        let mut stmt_missing: Vec<String> = Vec::new();
        let applies = evaluate_statement(stmt, action, resource, context, &mut stmt_missing);
        for key in stmt_missing {
            if !missing_context.contains(&key) {
                missing_context.push(key);
            }
        }
        if let Some(effect) = applies {
            matched.push("ResourcePolicy".to_string());
            if effect == Effect::Deny {
                return Evaluation {
                    decision: Decision::ExplicitDeny,
                    matched,
                    missing_context,
                };
            }
            has_allow = true;
        }
    }
    Evaluation {
        decision: if has_allow {
            Decision::Allowed
        } else {
            Decision::ImplicitDeny
        },
        matched,
        missing_context,
    }
}

/// Whether a statement's Principal/NotPrincipal limbs apply to `principal`.
fn principal_matches(stmt: &Value, principal: &str) -> bool {
    if let Some(not_principal) = stmt.get("NotPrincipal") {
        return !not_principal_matches(not_principal, principal);
    }
    let stmt_principal = match stmt.get("Principal") {
        Some(Value::String(s)) => Value::String(s.clone()),
        Some(Value::Object(_)) | Some(Value::Array(_)) => {
            stmt.get("Principal").cloned().unwrap_or(Value::Null)
        }
        _ => return true, // no Principal limb → applies to everyone
    };
    principal_limb_matches(&stmt_principal, principal)
}

fn not_principal_matches(not_principal: &Value, principal: &str) -> bool {
    match not_principal {
        Value::String(s) => s == "*" || wildcard_match(principal, s, false),
        Value::Object(map) => {
            for values in map.values() {
                for v in principal_values(values) {
                    if principal_value_matches(&v, principal) {
                        return true;
                    }
                }
            }
            false
        }
        _ => false,
    }
}

fn principal_values(values: &Value) -> Vec<String> {
    match values {
        Value::String(s) => vec![s.clone()],
        Value::Array(a) => a
            .iter()
            .filter_map(|x| x.as_str().map(String::from))
            .collect(),
        _ => Vec::new(),
    }
}

fn principal_value_matches(value: &str, principal: &str) -> bool {
    if value == "*" {
        return true;
    }
    if wildcard_match(value, principal, false) {
        return true;
    }
    // Bare account number: matches any principal ARN from that account.
    if value.len() == 12
        && value.chars().all(|c| c.is_ascii_digit())
        && principal.contains(&format!(":{value}:"))
    {
        return true;
    }
    false
}

fn principal_limb_matches(principal: &Value, caller: &str) -> bool {
    match principal {
        Value::String(s) => s == "*" || wildcard_match(s, caller, false),
        Value::Object(map) => {
            for values in map.values() {
                for v in principal_values(values) {
                    if principal_value_matches(&v, caller) {
                        return true;
                    }
                }
            }
            false
        }
        _ => false,
    }
}

/// Extract the account id from an ARN ("arn:aws:iam::1234:user/x" →
/// "1234"). Returns None for overly short strings.
pub fn arn_account(arn: &str) -> Option<&str> {
    let parts: Vec<&str> = arn.split(':').collect();
    if parts.len() >= 5 && !parts[4].is_empty() {
        Some(parts[4])
    } else {
        None
    }
}

// ------------------------------------------------------------------------
// Condition operators
// ------------------------------------------------------------------------

/// Result of evaluating one Condition block.
struct ConditionEval {
    satisfied: bool,
    missing_keys: Vec<String>,
}

fn evaluate_condition_block(block: &Value, context: &ContextValues) -> ConditionEval {
    let map = match block.as_object() {
        Some(map) => map,
        None => {
            return ConditionEval {
                satisfied: true,
                missing_keys: Vec::new(),
            }
        }
    };
    let mut missing_keys = Vec::new();
    for (raw_operator, key_value_map) in map {
        // Split set-operator prefix and IfExists suffix from the operator.
        let mut operator = raw_operator.as_str();
        let mut set_prefix = "";
        for prefix in ["ForAllValues:", "ForAnyValue:"] {
            if let Some(rest) = operator.strip_prefix(prefix) {
                set_prefix = prefix.strip_suffix(':').unwrap_or(prefix);
                operator = rest;
                break;
            }
        }
        let mut if_exists = false;
        if let Some(base) = operator.strip_suffix("IfExists") {
            if_exists = true;
            operator = base;
        }

        let key_map = match key_value_map.as_object() {
            Some(m) => m,
            None => {
                return ConditionEval {
                    satisfied: false,
                    missing_keys,
                }
            }
        };
        for (condition_key, policy_values) in key_map {
            let policy_values: Vec<String> = match policy_values {
                Value::String(s) => vec![s.clone()],
                Value::Array(a) => a
                    .iter()
                    .filter_map(|x| x.as_str().map(String::from))
                    .collect(),
                _ => Vec::new(),
            };
            let satisfied = if set_prefix.is_empty() {
                eval_single_operator(
                    operator,
                    condition_key,
                    &policy_values,
                    context,
                    if_exists,
                    &mut missing_keys,
                )
            } else {
                eval_set_operator(
                    set_prefix,
                    operator,
                    condition_key,
                    &policy_values,
                    context,
                    if_exists,
                    &mut missing_keys,
                )
            };
            if !satisfied {
                return ConditionEval {
                    satisfied: false,
                    missing_keys,
                };
            }
        }
    }
    ConditionEval {
        satisfied: true,
        missing_keys,
    }
}

#[allow(clippy::too_many_arguments)]
fn eval_single_operator(
    operator: &str,
    condition_key: &str,
    policy_values: &[String],
    context: &ContextValues,
    if_exists: bool,
    missing_keys: &mut Vec<String>,
) -> bool {
    if operator == "Null" {
        let expect_null = policy_values
            .first()
            .map(|v| v.to_lowercase() == "true")
            .unwrap_or(false);
        let key_present = context.contains_key(condition_key);
        return expect_null != key_present;
    }
    let Some(ctx_val) = context.value(condition_key) else {
        if if_exists {
            return true;
        }
        if !missing_keys.contains(&condition_key.to_string()) {
            missing_keys.push(condition_key.to_string());
        }
        return false;
    };
    let op = match operator_fn(operator) {
        Some(op) => op,
        None => return false, // unknown operator → condition unsatisfied
    };
    // Any policy value matching counts as success (OR within values).
    for pv in policy_values {
        let resolved = substitute_variables(pv, context);
        if op(ctx_val, &resolved) {
            return true;
        }
    }
    false
}

#[allow(clippy::too_many_arguments)]
fn eval_set_operator(
    set_prefix: &str,
    operator: &str,
    condition_key: &str,
    policy_values: &[String],
    context: &ContextValues,
    if_exists: bool,
    missing_keys: &mut Vec<String>,
) -> bool {
    let Some(op) = operator_fn(operator) else {
        return false;
    };
    let Some(ctx_values) = context.values_of(condition_key) else {
        if if_exists {
            return true;
        }
        // ForAllValues with no context values: vacuously true;
        // ForAnyValue with no context values: false.
        if set_prefix == "ForAllValues" {
            return true;
        }
        if !missing_keys.contains(&condition_key.to_string()) {
            missing_keys.push(condition_key.to_string());
        }
        return false;
    };
    let resolved: Vec<String> = policy_values
        .iter()
        .map(|pv| substitute_variables(pv, context))
        .collect();
    if set_prefix == "ForAllValues" {
        ctx_values
            .iter()
            .all(|cv| resolved.iter().any(|pv| op(cv, pv)))
    } else {
        // ForAnyValue
        ctx_values
            .iter()
            .any(|cv| resolved.iter().any(|pv| op(cv, pv)))
    }
}

type OpFn = fn(&str, &str) -> bool;

fn operator_fn(name: &str) -> Option<OpFn> {
    Some(match name {
        "StringEquals" => |c: &str, p: &str| c == p,
        "StringNotEquals" => |c: &str, p: &str| c != p,
        "StringEqualsIgnoreCase" => |c: &str, p: &str| c.eq_ignore_ascii_case(p),
        "StringNotEqualsIgnoreCase" => |c: &str, p: &str| !c.eq_ignore_ascii_case(p),
        "StringLike" => |c: &str, p: &str| fnmatch(c, p),
        "StringNotLike" => |c: &str, p: &str| !fnmatch(c, p),
        "NumericEquals" => |c: &str, p: &str| num_eq(c, p),
        "NumericNotEquals" => |c: &str, p: &str| !num_eq(c, p),
        "NumericLessThan" => |c: &str, p: &str| num_cmp(c, p) == Some(std::cmp::Ordering::Less),
        "NumericLessThanEquals" => |c: &str, p: &str| {
            num_cmp(c, p)
                .map(|o| o != std::cmp::Ordering::Greater)
                .unwrap_or(false)
        },
        "NumericGreaterThan" => {
            |c: &str, p: &str| num_cmp(c, p) == Some(std::cmp::Ordering::Greater)
        }
        "NumericGreaterThanEquals" => |c: &str, p: &str| {
            num_cmp(c, p)
                .map(|o| o != std::cmp::Ordering::Less)
                .unwrap_or(false)
        },
        "DateEquals" => |c: &str, p: &str| date_eq(c, p),
        "DateNotEquals" => |c: &str, p: &str| !date_eq(c, p),
        "DateLessThan" => |c: &str, p: &str| date_cmp(c, p) == Some(std::cmp::Ordering::Less),
        "DateLessThanEquals" => |c: &str, p: &str| {
            date_cmp(c, p)
                .map(|o| o != std::cmp::Ordering::Greater)
                .unwrap_or(false)
        },
        "DateGreaterThan" => |c: &str, p: &str| date_cmp(c, p) == Some(std::cmp::Ordering::Greater),
        "DateGreaterThanEquals" => |c: &str, p: &str| {
            date_cmp(c, p)
                .map(|o| o != std::cmp::Ordering::Less)
                .unwrap_or(false)
        },
        "Bool" => |c: &str, p: &str| c.to_lowercase() == p.to_lowercase(),
        "IpAddress" => ip_address,
        "NotIpAddress" => |c: &str, p: &str| !ip_address(c, p),
        "ArnEquals" => |c: &str, p: &str| arn_match(c, p, false),
        "ArnNotEquals" => |c: &str, p: &str| !arn_match(c, p, false),
        "ArnLike" => |c: &str, p: &str| arn_match(c, p, true),
        "ArnNotLike" => |c: &str, p: &str| !arn_match(c, p, true),
        _ => return None,
    })
}

fn num_eq(context_val: &str, policy_val: &str) -> bool {
    num_cmp(context_val, policy_val) == Some(std::cmp::Ordering::Equal)
}

fn num_cmp(context_val: &str, policy_val: &str) -> Option<std::cmp::Ordering> {
    let c: f64 = context_val.parse().ok()?;
    let p: f64 = policy_val.parse().ok()?;
    c.partial_cmp(&p)
}

fn date_eq(context_val: &str, policy_val: &str) -> bool {
    date_cmp(context_val, policy_val) == Some(std::cmp::Ordering::Equal)
}

fn date_cmp(context_val: &str, policy_val: &str) -> Option<std::cmp::Ordering> {
    let (c, p) = (parse_date(context_val)?, parse_date(policy_val)?);
    c.partial_cmp(&p)
}

/// Parse an ISO 8601-ish timestamp the way the era engine's
/// `_to_datetime` did, normalizing to an RFC 3339 form chrono accepts.
fn parse_date(s: &str) -> Option<chrono::DateTime<chrono::FixedOffset>> {
    let s = s.trim();
    let s = if s.len() == 10 && s.as_bytes().get(4) == Some(&b'-') {
        // "2026-01-02" → midnight
        format!("{s}T00:00:00Z")
    } else if s.ends_with('Z') {
        s.to_string()
    } else if s.len() == 19 && s.as_bytes().get(10) == Some(&b'T') {
        // "2026-01-02T10:00:00" → treat as UTC
        format!("{s}Z")
    } else {
        s.to_string()
    };
    chrono::DateTime::parse_from_rfc3339(&s).ok()
}

/// `fnmatch`-style matching: `*`, `?`, `[seq]`, `[!seq]`.
fn fnmatch(text: &str, pattern: &str) -> bool {
    let pat: Vec<char> = pattern.chars().collect();
    let txt: Vec<char> = text.chars().collect();
    fnmatch_at(&pat, 0, &txt, 0)
}

fn fnmatch_at(pat: &[char], mut pi: usize, txt: &[char], mut ti: usize) -> bool {
    while ti < txt.len() {
        if pi < pat.len() && pat[pi] == '*' {
            for skip in ti..=txt.len() {
                if fnmatch_at(pat, pi + 1, txt, skip) {
                    return true;
                }
            }
            return false;
        }
        if pi >= pat.len() {
            return false;
        }
        let matched = match pat[pi] {
            '?' => true,
            '[' => match charset_match(pat, pi, txt[ti]) {
                Some(next_pi) => {
                    pi = next_pi;
                    ti += 1;
                    continue;
                }
                None => return false,
            },
            c => c == txt[ti],
        };
        if !matched {
            return false;
        }
        pi += 1;
        ti += 1;
    }
    while pi < pat.len() && pat[pi] == '*' {
        pi += 1;
    }
    pi == pat.len()
}

/// Match a `[...]` charset at pattern index `pi`; returns the next pattern
/// index after the closing `]`, or None when there is no closing bracket
/// or the char does not match.
fn charset_match(pat: &[char], pi: usize, c: char) -> Option<usize> {
    let mut i = pi + 1;
    let neg = matches!(pat.get(i), Some('!')) || matches!(pat.get(i), Some('^'));
    if neg {
        i += 1;
    }
    let mut first = true;
    let mut matched = false;
    loop {
        if i >= pat.len() {
            // Unterminated `[` — treat `[` literally (not a charset).
            return if !neg && c == '[' { Some(pi + 1) } else { None };
        }
        if pat[i] == ']' && !first {
            return match matched {
                true => Some(i + 1),
                false => {
                    if neg {
                        Some(i + 1)
                    } else {
                        None
                    }
                }
            };
        }
        first = false;
        // Range like a-z
        if pat[i] != '\\' && i + 2 < pat.len() && pat[i + 1] == '-' && pat.get(i + 2) != Some(&']')
        {
            let (lo, hi) = (pat[i], pat.get(i + 2).copied().unwrap_or(pat[i]));
            if (lo..=hi).contains(&c) {
                matched = true;
            }
            i += 3;
            continue;
        }
        if pat[i] == c {
            matched = true;
        }
        i += 1;
    }
}

/// IPv4 / IPv6 membership with CIDR support, mirroring the era engine's
/// use of Python's `ipaddress` module.
fn ip_address(context_val: &str, policy_val: &str) -> bool {
    if policy_val.contains(':') {
        // IPv6 network or address.
        if let Some((base, prefix)) = policy_val.split_once('/') {
            return ipv6_prefix(context_val, base, prefix);
        }
        return ipv6_normalize(context_val) == ipv6_normalize(policy_val);
    }
    if let Some((base, prefix)) = policy_val.split_once('/') {
        let (Some(addr), Ok(prefix_len), Some(net)) = (
            parse_ipv4(context_val),
            prefix.parse::<u32>(),
            parse_ipv4(base),
        ) else {
            return false;
        };
        if !(0..=32).contains(&prefix_len) {
            return false;
        }
        let mask = if prefix_len == 0 {
            0
        } else {
            u32::MAX << (32 - prefix_len)
        };
        return addr & mask == net & mask;
    }
    match (parse_ipv4(context_val), parse_ipv4(policy_val)) {
        (Some(a), Some(b)) => a == b,
        // Unparseable on either side (e.g. IPv6 in a v4-shaped pattern):
        // fall back to string equality.
        _ => context_val == policy_val,
    }
}

fn parse_ipv4(s: &str) -> Option<u32> {
    let mut out: u32 = 0;
    let mut octets = 0;
    for part in s.split('.') {
        if part.is_empty() || octets >= 4 {
            return None;
        }
        let v: u32 = part.parse().ok()?;
        if v > 255 {
            return None;
        }
        out = (out << 8) | v;
        octets += 1;
    }
    if octets == 4 {
        Some(out)
    } else {
        None
    }
}

/// Parse an IPv6 address into its 8 hextets, supporting `::` compression
/// and an embedded IPv4 tail (`::ffff:192.168.0.1`).
fn ipv6_hextets(s: &str) -> Option<[u16; 8]> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    // Convert an embedded IPv4 tail into two hextets first.
    let mut text = s.to_string();
    if text.contains('.') {
        let idx = text.rfind(':')? + 1;
        let tail = &text[idx..];
        let v = parse_ipv4(tail)?;
        text = format!("{}{:x}:{:x}", &text[..idx], v >> 16, v & 0xFFFF);
    }
    if text.matches("::").count() > 1 {
        return None;
    }
    match text.split_once("::") {
        Some((head, tail)) => {
            let head = split_hextets(head)?;
            let tail = split_hextets(tail)?;
            let fill = 8usize.checked_sub(head.len() + tail.len())?;
            let zeros = std::iter::repeat_n(0u16, fill);
            let groups: Vec<u16> = head.into_iter().chain(zeros).chain(tail).collect();
            let mut out = [0u16; 8];
            out.copy_from_slice(&groups);
            Some(out)
        }
        None => {
            let groups = split_hextets(&text)?;
            if groups.len() != 8 {
                return None;
            }
            let mut out = [0u16; 8];
            out.copy_from_slice(&groups);
            Some(out)
        }
    }
}

fn split_hextets(s: &str) -> Option<Vec<u16>> {
    let mut out = Vec::with_capacity(8);
    for part in s.split(':') {
        if part.is_empty() {
            continue; // from `::` boundaries or trailing separators
        }
        out.push(u16::from_str_radix(part, 16).ok()?);
    }
    Some(out)
}

fn ipv6_normalize(s: &str) -> String {
    match ipv6_hextets(s) {
        Some(h) => h
            .iter()
            .map(|v| format!("{v:x}"))
            .collect::<Vec<_>>()
            .join(":"),
        None => s.trim().to_lowercase(),
    }
}

fn ipv6_prefix(context_val: &str, net: &str, prefix: &str) -> bool {
    let Ok(plen) = prefix.parse::<u32>() else {
        return false;
    };
    if !(0..=128).contains(&plen) {
        return false;
    }
    let (Some(addr), Some(net_addr)) = (ipv6_hextets(context_val), ipv6_hextets(net)) else {
        return false;
    };
    let addr: u128 = addr.iter().fold(0u128, |acc, v| (acc << 16) | *v as u128);
    let net: u128 = net_addr
        .iter()
        .fold(0u128, |acc, v| (acc << 16) | *v as u128);
    if plen == 0 {
        return true;
    }
    let mask = u128::MAX << (128 - plen);
    addr & mask == net & mask
}

/// Section-wise ARN match
/// (`arn:partition:service:region:account:resource[/sub]`).
/// The resource portion (everything after the 5th colon) may itself
/// contain colons, so it is re-joined. With `wildcard` set, each section
/// supports `*` globs; otherwise sections compare exactly.
fn arn_match(text: &str, pattern: &str, wildcard: bool) -> bool {
    let text_parts: Vec<&str> = text.split(':').collect();
    let pattern_parts: Vec<&str> = pattern.split(':').collect();
    if text_parts.len() < 6 || pattern_parts.len() < 6 {
        return false;
    }
    let text_fixed: Vec<String> = text_parts[..5]
        .iter()
        .map(|s| s.to_string())
        .chain(std::iter::once(text_parts[5..].join(":")))
        .collect();
    let pattern_fixed: Vec<String> = pattern_parts[..5]
        .iter()
        .map(|s| s.to_string())
        .chain(std::iter::once(pattern_parts[5..].join(":")))
        .collect();
    for (a, p) in text_fixed.iter().zip(&pattern_fixed) {
        if wildcard {
            if !fnmatch(a, p) {
                return false;
            }
        } else if a != p {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ctx(kvs: &[(&str, Vec<&str>)]) -> ContextValues {
        let mut cx = ContextValues::new();
        for (k, vals) in kvs {
            cx.insert(k.to_string(), vals.iter().map(|v| v.to_string()).collect());
        }
        cx
    }

    fn doc(v: serde_json::Value) -> (String, Value) {
        ("PolicyInputList.1".to_string(), v)
    }

    // ------------------------------------------------------------------
    // Wildcard matching (multi-star)
    // ------------------------------------------------------------------

    #[test]
    fn wildcard_single_star() {
        assert!(wildcard_match("s3:Get*", "s3:GetObject", false));
        assert!(!wildcard_match("s3:Get*", "s3:PutObject", false));
        assert!(wildcard_match("*", "anything at all", false));
        assert!(!wildcard_match("s3:*", "sts:AssumeRole", false));
    }

    #[test]
    fn wildcard_multi_star() {
        assert!(wildcard_match("a*b*c", "axxbyyc", false));
        assert!(wildcard_match("a*b*c", "abc", false));
        assert!(!wildcard_match("a*b*c", "axxyyc", false));
        assert!(wildcard_match("s3:*b*ck*", "s3:my-bucket", false));
        assert!(wildcard_match(
            "arn:aws:s3:*:*:my-bucket/*",
            "arn:aws:s3:us-west-2:123456789012:my-bucket/keys/a",
            false
        ));
        assert!(!wildcard_match(
            "arn:aws:s3:*:*:my-bucket/*",
            "arn:aws:s3:us-west-2:123456789012:other-bucket/keys/a",
            false
        ));
    }

    #[test]
    fn wildcard_case_and_literal_chars() {
        assert!(wildcard_match("S3:GET*", "s3:getobject", true));
        // Action/Resource limbs match case-sensitively only if caller says
        // so; the matcher itself never lowercases unless asked.
        assert!(!wildcard_match("S3:Get*", "s3:GetObject", false));
        // Only * is a wildcard — ? and [] are literal characters.
        assert!(!wildcard_match("s3:Get?", "s3:GetX", false));
        assert!(wildcard_match("s3:[a-b]", "s3:[a-b]", false));
    }

    // ------------------------------------------------------------------
    // Effect/Action/Resource evaluation
    // ------------------------------------------------------------------

    #[test]
    fn allow_action_match_implicit_deny_otherwise() {
        let doc_value = json!({
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Action": "s3:*", "Resource": "*"}
            ]
        });
        let cx = ContextValues::new();
        let eval = evaluate_policy_docs(&[doc(doc_value)], "s3:GetObject", "*", &cx);
        assert_eq!(eval.decision, Decision::Allowed);
        assert_eq!(eval.matched, vec!["PolicyInputList.1".to_string()]);

        let eval = evaluate_policy_docs(
            &[doc(json!({
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]
            }))],
            "ec2:DescribeInstances",
            "*",
            &cx,
        );
        assert_eq!(eval.decision, Decision::ImplicitDeny);
        assert!(eval.matched.is_empty());
    }

    #[test]
    fn deny_overrides_allow_regardless_of_order() {
        for order in [true, false] {
            let allow = json!({"Effect": "Allow", "Action": "s3:*", "Resource": "*"});
            let deny = json!({"Effect": "Deny", "Action": "s3:DeleteObject", "Resource": "*"});
            let statements = if order {
                vec![allow, deny]
            } else {
                vec![deny, allow]
            };
            let cx = ContextValues::new();
            let eval = evaluate_policy_docs(
                &[doc(json!({"Statement": statements}))],
                "s3:DeleteObject",
                "*",
                &cx,
            );
            assert_eq!(eval.decision, Decision::ExplicitDeny);
            // An allowed (non-denied) action still evaluates to allowed.
            let eval = evaluate_policy_docs(
                &[doc(json!({"Statement": statements}))],
                "s3:GetObject",
                "*",
                &cx,
            );
            assert_eq!(eval.decision, Decision::Allowed);
        }
    }

    #[test]
    fn multi_statement_across_policies() {
        let first = json!({
            "Statement": [
                {"Effect": "Allow", "Action": ["ec2:Describe*"], "Resource": "*"},
            ]
        });
        let second = json!({
            "Statement": [
                {"Effect": "Deny", "Action": "ec2:TerminateInstances", "Resource": "*"},
                {"Effect": "Deny", "Condition": {"Bool": {"aws:SecureTransport": "false"}}, "Action": "s3:*", "Resource": "*"}
            ]
        });
        let docs = vec![
            ("PolicyInputList.1".to_string(), first),
            ("PolicyInputList.2".to_string(), second),
        ];
        let cx = ContextValues::new();
        let eval = evaluate_policy_docs(&docs, "ec2:TerminateInstances", "*", &cx);
        assert_eq!(eval.decision, Decision::ExplicitDeny);
        assert_eq!(eval.matched, vec!["PolicyInputList.2".to_string()]);

        let eval = evaluate_policy_docs(&docs, "ec2:DescribeInstances", "*", &cx);
        assert_eq!(eval.decision, Decision::Allowed);
        assert_eq!(eval.matched, vec!["PolicyInputList.1".to_string()]);

        // The conditional deny blocks S3 when SecureTransport is false.
        let eval = evaluate_policy_docs(&docs, "s3:GetObject", "*", &cx);
        assert_eq!(eval.decision, Decision::ImplicitDeny);
        assert!(eval
            .missing_context
            .contains(&"aws:SecureTransport".to_string()));
    }

    #[test]
    fn resource_limb_guards_arns() {
        let doc_value = json!({
            "Statement": [{
                "Effect": "Allow",
                "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::secret/*",
            }]
        });
        let cx = ContextValues::new();
        let eval = evaluate_policy_docs(
            &[(("PolicyInputList.1").to_string(), doc_value.clone())],
            "s3:GetObject",
            "arn:aws:s3:::secret/token",
            &cx,
        );
        assert_eq!(eval.decision, Decision::Allowed);
        let eval = evaluate_policy_docs(
            &[("PolicyInputList.1".to_string(), doc_value)],
            "s3:GetObject",
            "arn:aws:s3:::other/file",
            &cx,
        );
        assert_eq!(eval.decision, Decision::ImplicitDeny);
    }

    #[test]
    fn not_action_concern_logic() {
        // NotAction deny: applies to everything EXCEPT the listed actions.
        let doc_value = json!({
            "Statement": [{
                "Effect": "Deny",
                "NotAction": "s3:Delete*",
                "Resource": "*",
            }]
        });
        let cx = ContextValues::new();
        let eval = evaluate_policy_docs(&[doc(doc_value)], "s3:GetObject", "*", &cx);
        assert_eq!(eval.decision, Decision::ExplicitDeny);
        let eval = evaluate_policy_docs(
            &[doc(
                json!({"Statement": [{"Effect": "Deny", "NotAction": "s3:Delete*", "Resource": "*"}]}),
            )],
            "s3:DeleteObject",
            "*",
            &cx,
        );
        assert_eq!(eval.decision, Decision::ImplicitDeny);
    }

    // ------------------------------------------------------------------
    // Conditions
    // ------------------------------------------------------------------

    #[test]
    fn condition_string_equals() {
        let doc_value = json!({
            "Statement": [{
                "Effect": "Allow",
                "Action": "s3:GetObject",
                "Resource": "*",
                "Condition": {
                    "StringEquals": {"aws:RequestedRegion": ["eu-west-1", "eu-north-1"]}
                }
            }]
        });
        // Matching context region.
        let cx = ctx(&[("aws:RequestedRegion", vec!["eu-west-1"])]);
        let eval = evaluate_policy_docs(&[doc(doc_value.clone())], "s3:GetObject", "*", &cx);
        assert_eq!(eval.decision, Decision::Allowed);
        assert!(eval.missing_context.is_empty());

        // Wrong value → implicit deny, no missing keys (key was provided).
        let cx = ctx(&[("aws:RequestedRegion", vec!["us-east-1"])]);
        let eval = evaluate_policy_docs(&[doc(doc_value.clone())], "s3:GetObject", "*", &cx);
        assert_eq!(eval.decision, Decision::ImplicitDeny);
        assert!(eval.missing_context.is_empty());

        // Missing key → implicit deny and the key is reported as missing.
        let eval = evaluate_policy_docs(
            &[doc(doc_value)],
            "s3:GetObject",
            "*",
            &ContextValues::new(),
        );
        assert_eq!(eval.decision, Decision::ImplicitDeny);
        assert_eq!(
            eval.missing_context,
            vec!["aws:RequestedRegion".to_string()]
        );
    }

    #[test]
    fn condition_string_like_multistar() {
        let doc_value = json!({
            "Statement": [{
                "Effect": "Deny",
                "Action": "dynamodb:Get*",
                "Resource": "*",
                "Condition": {"StringLike": {"dynamodb:TableName": "*rbt-*cov*"}}
            }]
        });
        // "rbt-" then "cov" anywhere after: matches the multi-star pattern.
        let cx = ctx(&[("dynamodb:TableName", vec!["team-rbt-core-cov-1"])]);
        let eval = evaluate_policy_docs(&[doc(doc_value.clone())], "dynamodb:GetItem", "*", &cx);
        assert_eq!(eval.decision, Decision::ExplicitDeny);

        // No "rbt-"... "cov" run anywhere → condition unsatisfied.
        let cx = ctx(&[("dynamodb:TableName", vec!["prod"])]);
        let eval = evaluate_policy_docs(&[doc(doc_value)], "dynamodb:GetItem", "*", &cx);
        assert_eq!(eval.decision, Decision::ImplicitDeny);
    }

    #[test]
    fn condition_if_exists_and_null() {
        // StringEqualsIfExists with an absent key is vacuously satisfied.
        let doc_value = json!({
            "Statement": [{
                "Effect": "Allow",
                "Action": "s3:PutObject",
                "Resource": "*",
                "Condition": {"StringEqualsIfExists": {"s3:x-amz-acl": "public-read"}}
            }]
        });
        let eval = evaluate_policy_docs(
            &[doc(doc_value.clone())],
            "s3:PutObject",
            "*",
            &ContextValues::new(),
        );
        assert_eq!(eval.decision, Decision::Allowed);

        // Present and equal → allow; present and unequal → deny path.
        let cx = ctx(&[("s3:x-amz-acl", vec!["public-read"])]);
        let eval = evaluate_policy_docs(&[doc(doc_value.clone())], "s3:PutObject", "*", &cx);
        assert_eq!(eval.decision, Decision::Allowed);
        let cx = ctx(&[("s3:x-amz-acl", vec!["private"])]);
        let eval = evaluate_policy_docs(&[doc(doc_value)], "s3:PutObject", "*", &cx);
        assert_eq!(eval.decision, Decision::ImplicitDeny);

        // Null check: "Null": "true" matches when the key is absent.
        let doc_value = json!({
            "Statement": [{
                "Effect": "Deny",
                "Action": "sts:AssumeRole",
                "Resource": "*",
                "Condition": {"Null": {"aws:PrincipalOrgID": "false"}}
            }]
        });
        let cx = ctx(&[("aws:PrincipalOrgID", vec!["o-123"])]);
        let eval = evaluate_policy_docs(&[doc(doc_value.clone())], "sts:AssumeRole", "*", &cx);
        assert_eq!(eval.decision, Decision::ExplicitDeny);
        let eval = evaluate_policy_docs(
            &[doc(doc_value)],
            "sts:AssumeRole",
            "*",
            &ContextValues::new(),
        );
        assert_eq!(eval.decision, Decision::ImplicitDeny);
    }

    #[test]
    fn condition_numeric_and_bool() {
        let doc_value = json!({
            "Statement": [{
                "Effect": "Allow",
                "Action": "sqs:SendMessage",
                "Resource": "*",
                "Condition": {
                    "NumericLessThanEquals": {"sqs:max": ["10"]},
                    "Bool": {"aws:SecureTransport": "true"}
                }
            }]
        });
        let cx = ctx(&[
            ("sqs:max", vec!["10"]),
            ("aws:SecureTransport", vec!["true"]),
        ]);
        let eval = evaluate_policy_docs(&[doc(doc_value.clone())], "sqs:SendMessage", "*", &cx);
        assert_eq!(eval.decision, Decision::Allowed);

        let cx = ctx(&[
            ("sqs:max", vec!["11"]),
            ("aws:SecureTransport", vec!["true"]),
        ]);
        let eval = evaluate_policy_docs(&[doc(doc_value.clone())], "sqs:SendMessage", "*", &cx);
        assert_eq!(eval.decision, Decision::ImplicitDeny);

        let cx = ctx(&[
            ("sqs:max", vec!["5"]),
            ("aws:SecureTransport", vec!["TRUE"]),
        ]);
        let eval = evaluate_policy_docs(&[doc(doc_value)], "sqs:SendMessage", "*", &cx);
        assert_eq!(eval.decision, Decision::Allowed);
    }

    #[test]
    fn condition_set_operators_and_unknown_operator() {
        let doc_value = json!({
            "Statement": [{
                "Effect": "Deny",
                "Action": "s3:*",
                "Resource": "*",
                "Condition": {
                    "ForAllValues:StringEquals": {"aws:PrincipalTag/team": ["alpha", "bravo"]}
                }
            }]
        });
        let cx = ctx(&[("aws:PrincipalTag/team", vec!["alpha", "alpha"])]);
        let eval = evaluate_policy_docs(&[doc(doc_value.clone())], "s3:PutObject", "*", &cx);
        assert_eq!(eval.decision, Decision::ExplicitDeny);

        // One two-tags case breaking the ALL requirement unblocks the deny.
        let cx = ctx(&[("aws:PrincipalTag/team", vec!["alpha", "charlie"])]);
        let eval = evaluate_policy_docs(&[doc(doc_value.clone())], "s3:PutObject", "*", &cx);
        assert_eq!(eval.decision, Decision::ImplicitDeny);

        // ForAnyValue which is not satisfied in any element blocks nothing here.
        let doc_value = json!({
            "Statement": [{
                "Effect": "Deny",
                "Action": "kms:Decrypt",
                "Resource": "*",
                "Condition": {
                    "ForAnyValue:StringEquals": {"aws:PrincipalTag/team": ["denied-team"]}
                }
            }]
        });
        let cx = ctx(&[("aws:PrincipalTag/team", vec!["alpha"])]);
        let eval = evaluate_policy_docs(&[doc(doc_value.clone())], "kms:Decrypt", "*", &cx);
        assert_eq!(eval.decision, Decision::ImplicitDeny);

        // Unknown operators never satisfy the condition.
        let doc_value = json!({
            "Statement": [{
                "Effect": "Allow",
                "Action": "s3:GetObject",
                "Resource": "*",
                "Condition": {"MyMadeUpOperator": {"aws:username": "admin"}}
            }]
        });
        let cx = ctx(&[("aws:username", vec!["admin"])]);
        let eval = evaluate_policy_docs(&[doc(doc_value)], "s3:GetObject", "*", &cx);
        assert_eq!(eval.decision, Decision::ImplicitDeny);
    }

    #[test]
    fn condition_ip_address() {
        let doc_value = json!({
            "Statement": [{
                "Effect": "Deny",
                "Action": "s3:*",
                "Resource": "*",
                "Condition": {"NotIpAddress": {"aws:SourceIp": ["203.0.113.0/24"]}}
            }]
        });
        let cx = ctx(&[("aws:SourceIp", vec!["203.0.113.55"])]);
        let eval = evaluate_policy_docs(&[doc(doc_value.clone())], "s3:GetObject", "*", &cx);
        assert_eq!(eval.decision, Decision::ImplicitDeny);

        let cx = ctx(&[("aws:SourceIp", vec!["198.51.100.9"])]);
        let eval = evaluate_policy_docs(&[doc(doc_value.clone())], "s3:GetObject", "*", &cx);
        assert_eq!(eval.decision, Decision::ExplicitDeny);

        // Zero-prefix CIDR allows the whole space; /32 pins one address.
        let doc_value = json!({
            "Statement": [{
                "Effect": "Deny",
                "Action": "s3:*",
                "Resource": "*",
                "Condition": {"IpAddress": {"aws:SourceIp": ["0.0.0.0/0"]}}
            }]
        });
        let cx = ctx(&[("aws:SourceIp", vec!["100.100.100.100"])]);
        let eval = evaluate_policy_docs(&[doc(doc_value.clone())], "s3:GetObject", "*", &cx);
        assert_eq!(eval.decision, Decision::ExplicitDeny);
        let doc_value = json!({
            "Statement": [{
                "Effect": "Deny",
                "Action": "s3:*",
                "Resource": "*",
                "Condition": {"IpAddress": {"aws:SourceIp": ["198.51.100.7/32"]}}
            }]
        });
        let cx = ctx(&[("aws:SourceIp", vec!["198.51.100.8"])]);
        let eval = evaluate_policy_docs(&[doc(doc_value)], "s3:GetObject", "*", &cx);
        assert_eq!(eval.decision, Decision::ImplicitDeny);
    }

    #[test]
    fn condition_arn_like_sectionwise() {
        let doc_value = json!({
            "Statement": [{
                "Effect": "Deny",
                "Action": "sns:Publish",
                "Resource": "*",
                "Condition": {"ArnLike": {"aws:SourceArn": [
                    "arn:aws:sns:*:123456789012:rbt-*"
                ]}}
            }]
        });
        let cx = ctx(&[(
            "aws:SourceArn",
            vec!["arn:aws:sns:us-east-1:123456789012:rbt-topic"],
        )]);
        let eval = evaluate_policy_docs(&[doc(doc_value.clone())], "sns:Publish", "*", &cx);
        assert_eq!(eval.decision, Decision::ExplicitDeny);

        // Different account section fails the sectionwise ArnLike match.
        let cx = ctx(&[(
            "aws:SourceArn",
            vec!["arn:aws:sns:us-east-1:222233334444:rbt-topic"],
        )]);
        let eval = evaluate_policy_docs(&[doc(doc_value)], "sns:Publish", "*", &cx);
        assert_eq!(eval.decision, Decision::ImplicitDeny);
    }

    #[test]
    fn condition_policy_variables_substituted() {
        let doc_value = json!({
            "Statement": [{
                "Effect": "Allow",
                "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::home/${aws:username}/*",
            }]
        });
        let cx = ctx(&[("aws:username", vec!["alice"])]);
        let eval = evaluate_policy_docs(
            &[doc(doc_value)],
            "s3:GetObject",
            "arn:aws:s3:::home/alice/file.txt",
            &cx,
        );
        assert_eq!(eval.decision, Decision::Allowed);
    }

    // ------------------------------------------------------------------
    // Resource policies / principal matching
    // ------------------------------------------------------------------

    #[test]
    fn resource_policy_principal_checks() {
        // Bare account number as Principal (the era engine/moto shape for
        // cross-account grants).
        let rp = json!({
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"AWS": "222233334444"},
                "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::cross-bucket/*"
            }]
        });
        let cx = ContextValues::new();
        // No caller ARN: policy evaluates as a same-account contributor.
        let eval = evaluate_resource_policy(
            &rp,
            None,
            "s3:GetObject",
            "arn:aws:s3:::cross-bucket/x",
            &cx,
        );
        assert_eq!(eval.decision, Decision::Allowed);

        // Assumed-role session from the granted account.
        let eval = evaluate_resource_policy(
            &rp,
            Some("arn:aws:sts::222233334444:assumed-role/admin/session"),
            "s3:GetObject",
            "arn:aws:s3:::cross-bucket/x",
            &cx,
        );
        assert_eq!(eval.decision, Decision::Allowed);

        // Caller from a different account: Principal does not match.
        let eval = evaluate_resource_policy(
            &rp,
            Some("arn:aws:sts::111122223333:assumed-role/admin/session"),
            "s3:GetObject",
            "arn:aws:s3:::cross-bucket/x",
            &cx,
        );
        assert_eq!(eval.decision, Decision::ImplicitDeny);
    }

    #[test]
    fn resource_policy_bare_account_and_star_principal() {
        let rp = json!({
            "Statement": [{
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:*",
                "Resource": "arn:aws:s3:::public/*"
            }]
        });
        let cx = ContextValues::new();
        let eval = evaluate_resource_policy(
            &rp,
            Some("arn:aws:iam::999988887777:user/any"),
            "s3:PutObject",
            "arn:aws:s3:::public/x",
            &cx,
        );
        assert_eq!(eval.decision, Decision::Allowed);

        let rp_account = json!({
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"AWS": ["123456789012"]},
                "Action": "sqs:ReceiveMessage",
                "Resource": "arn:aws:sqs:us-east-1:123456789012:queue"
            }]
        });
        let eval = evaluate_resource_policy(
            &rp_account,
            Some("arn:aws:iam::123456789012:user/alice"),
            "sqs:ReceiveMessage",
            "arn:aws:sqs:us-east-1:123456789012:queue",
            &ContextValues::new(),
        );
        assert_eq!(eval.decision, Decision::Allowed);
    }

    #[test]
    fn cross_account_combine_rules() {
        let identity_allow = Evaluation {
            decision: Decision::Allowed,
            matched: vec!["PolicyInputList.1".to_string()],
            missing_context: vec![],
        };
        let rp_allow = Evaluation {
            decision: Decision::Allowed,
            matched: vec!["ResourcePolicy".to_string()],
            missing_context: vec![],
        };
        let rp_neutral = Evaluation {
            decision: Decision::ImplicitDeny,
            matched: vec![],
            missing_context: vec![],
        };
        let rp_deny = Evaluation {
            decision: Decision::ExplicitDeny,
            matched: vec![],
            missing_context: vec![],
        };

        // Same account: either side allowing is enough.
        assert_eq!(
            identity_allow
                .combine_with_resource_policy(Some(&rp_neutral), false)
                .decision,
            Decision::Allowed
        );
        assert_eq!(
            rp_neutral
                .combine_with_resource_policy(Some(&rp_allow), false)
                .decision,
            Decision::Allowed
        );

        // Cross account: both must allow.
        assert_eq!(
            identity_allow
                .combine_with_resource_policy(Some(&rp_neutral), true)
                .decision,
            Decision::ImplicitDeny
        );
        assert_eq!(
            identity_allow
                .combine_with_resource_policy(Some(&rp_deny), true)
                .decision,
            Decision::ExplicitDeny
        );
        assert_eq!(
            identity_allow
                .combine_with_resource_policy(Some(&rp_allow), true)
                .decision,
            Decision::Allowed
        );
    }

    #[test]
    fn terminate_style_resource_policy_deny_first() {
        // Deny statement inside the resource policy blocks that principal.
        let rp = json!({
            "Statement": [
                {"Effect": "Allow", "Principal": "*", "Action": "sqs:SendMessage",
                 "Resource": "arn:aws:sqs:us-east-1:123456789012:q"},
                {"Effect": "Deny",
                 "Principal": {"AWS": "arn:aws:iam::999988887777:root"},
                 "Action": "sqs:SendMessage",
                 "Resource": "arn:aws:sqs:us-east-1:123456789012:q"}
            ]
        });
        let eval = evaluate_resource_policy(
            &rp,
            Some("arn:aws:iam::999988887777:root"),
            "sqs:SendMessage",
            "arn:aws:sqs:us-east-1:123456789012:q",
            &ContextValues::new(),
        );
        assert_eq!(eval.decision, Decision::ExplicitDeny);
    }

    #[test]
    fn arn_account_extracts_field() {
        assert_eq!(
            arn_account("arn:aws:iam::123456789012:user/emil"),
            Some("123456789012")
        );
        assert_eq!(arn_account("arn:aws:s3:::bucket"), None);
        assert_eq!(arn_account("not an arn"), None);
    }
}
