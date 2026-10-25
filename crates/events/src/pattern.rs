//! EventBridge event pattern matching semantics, ported from the Python
//! provider's `_match_pattern` (models.py): prefix, suffix, numeric, exists,
//! anything-but, exact list matches, and nested objects.

use serde_json::{Map, Value};

/// Match a full event pattern against an event.
pub fn matches_pattern(pattern: &Value, event: &Value) -> bool {
    match (pattern.as_object(), event.as_object()) {
        (Some(p), Some(e)) => matches(p, e),
        _ => false,
    }
}

fn matches(pattern: &Map<String, Value>, event: &Map<String, Value>) -> bool {
    for (key, pattern_value) in pattern {
        // $or compound patterns: any sub-pattern may match.
        if key == "$or" {
            let Some(subs) = pattern_value.as_array() else {
                return false;
            };
            if !subs
                .iter()
                .filter_map(|sp| sp.as_object())
                .any(|sp| matches(sp, event))
            {
                return false;
            }
            continue;
        }

        let event_value = match event.get(key) {
            Some(v) => v,
            None => {
                // Absent key: only a list whose sole match is "exists": false passes.
                let absent_ok = pattern_value.as_array().is_some_and(|list| {
                    list.iter().any(|pv| {
                        pv.as_object()
                            .and_then(|o| o.get("exists"))
                            .and_then(|e| e.as_bool())
                            == Some(false)
                    })
                });
                if !absent_ok {
                    return false;
                }
                continue;
            }
        };

        match pattern_value {
            Value::Object(sub_pattern) => {
                if !event_value.is_object() {
                    return false;
                }
                if !matches(sub_pattern, event_value.as_object().unwrap()) {
                    return false;
                }
            }
            Value::Array(list) => {
                if !list_match(list, event_value) {
                    return false;
                }
            }
            scalar => {
                if event_value != scalar {
                    return false;
                }
            }
        }
    }
    true
}

/// A list of acceptable values/matchers for one event field.
///
/// Public so the Lambda event-source-mapping engine can reuse the exact
/// same operator semantics (prefix/suffix/numeric/exists/anything-but)
/// for its `FilterCriteria` leaves.
pub fn list_match(pattern_values: &[Value], event_value: &Value) -> bool {
    for pv in pattern_values {
        if let Some(obj) = pv.as_object() {
            if let Some(p) = obj.get("prefix").and_then(|v| v.as_str()) {
                if let Some(s) = event_value.as_str() {
                    if s.starts_with(p) {
                        return true;
                    }
                }
                if let Some(arr) = event_value.as_array() {
                    if arr
                        .iter()
                        .any(|v| v.as_str().map(|s| s.starts_with(p)) == Some(true))
                    {
                        return true;
                    }
                }
            } else if let Some(p) = obj.get("suffix").and_then(|v| v.as_str()) {
                if let Some(s) = event_value.as_str() {
                    if s.ends_with(p) {
                        return true;
                    }
                }
                if let Some(arr) = event_value.as_array() {
                    if arr
                        .iter()
                        .any(|v| v.as_str().map(|s| s.ends_with(p)) == Some(true))
                    {
                        return true;
                    }
                }
            } else if let Some(excluded) = obj.get("anything-but") {
                if anything_but(excluded, event_value) {
                    return true;
                }
            } else if let Some(ops) = obj.get("numeric") {
                if let Some(ops) = ops.as_array() {
                    if match_numeric(ops, event_value) {
                        return true;
                    }
                }
            } else if let Some(expected) = obj.get("exists").and_then(|e| e.as_bool()) {
                // An `exists` leaf tests the presence of the event value
                // itself, in both directions (moto
                // `_does_item_match_named_filter`: `exists = item is not
                // UNDEFINED`; era `_match_value_list`: a non-None value
                // satisfies `exists: true`). Explicit JSON null counts as
                // absent (era parity: `event.get(key) is None`).
                if expected != event_value.is_null() {
                    return true;
                }
            }
        } else if event_value == pv {
            return true;
        } else if let Some(arr) = event_value.as_array() {
            if arr.contains(pv) {
                return true;
            }
        }
    }
    false
}

fn anything_but(excluded: &Value, event_value: &Value) -> bool {
    match event_value {
        Value::Array(arr) => match excluded {
            // moto evaluates the exclusion per element (`any(evaluate(i))`):
            // an array event value matches when ANY element differs. The
            // previous all-elements-differ reading inverted the operator
            // (era parity: `if not any(v in excluded for v in event_value)`
            // — era's scalar branch adds the residual case below).
            Value::Array(ex) => arr.iter().any(|v| !ex.contains(v)),
            _ => arr.iter().any(|v| v != excluded),
        },
        _ => match excluded {
            Value::Object(obj) => {
                if let Some(prefix) = obj.get("prefix").and_then(|p| p.as_str()) {
                    event_value.as_str().map(|s| !s.starts_with(prefix)) == Some(true)
                } else if let Some(suffix) = obj.get("suffix").and_then(|s| s.as_str()) {
                    event_value.as_str().map(|s| !s.ends_with(suffix)) == Some(true)
                } else {
                    false
                }
            }
            Value::Array(ex) => !ex.contains(event_value),
            _ => event_value != excluded,
        },
    }
}

/// Python-style alternating (op, value) numeric list: [">", 1, "<", 5].
fn match_numeric(ops: &[Value], value: &Value) -> bool {
    let n = match value.as_f64() {
        Some(n) => n,
        None => return false,
    };
    let mut i = 0;
    while i + 1 < ops.len() {
        let op = ops[i].as_str().unwrap_or("");
        let rhs = ops[i + 1].as_f64().unwrap_or(f64::NAN);
        let ok = match op {
            "<" => n < rhs,
            "<=" => n <= rhs,
            ">" => n > rhs,
            ">=" => n >= rhs,
            "=" => n == rhs,
            _ => false,
        };
        if !ok {
            return false;
        }
        i += 2;
    }
    true
}

/// Resolve a JSONPath of the form `$.a.b` against an event.
pub fn resolve_json_path(path: &str, event: &Value) -> Option<Value> {
    let rest = path.strip_prefix("$.")?;
    if rest.is_empty() {
        return Some(event.clone());
    }
    let mut cur = event.clone();
    for part in rest.split('.') {
        cur = cur.get(part)?.clone();
    }
    Some(cur)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // -- exists leaves (F1, agent-221 finding 1) ------------------------------

    #[test]
    fn exists_true_matches_present_values() {
        let pattern = json!({"detail": {"state": [{"exists": true}]}});
        assert!(matches_pattern(&pattern, &json!({"detail": {"state": "running"}})));
        assert!(matches_pattern(&pattern, &json!({"detail": {"state": 0}})));
        // Absent key: `exists: true` must not match.
        assert!(!matches_pattern(&pattern, &json!({"detail": {}})));
        // Explicit null counts as absent (era parity).
        assert!(!matches_pattern(&pattern, &json!({"detail": {"state": null}})));
    }

    #[test]
    fn exists_false_matches_absent_and_null() {
        let pattern = json!({"detail": {"state": [{"exists": false}]}});
        assert!(matches_pattern(&pattern, &json!({"detail": {}})));
        assert!(matches_pattern(&pattern, &json!({"detail": {"state": null}})));
        assert!(!matches_pattern(&pattern, &json!({"detail": {"state": "on"}})));
    }

    // -- anything-but on array values (F2, agent-221 finding 2) ----------------

    #[test]
    fn anything_but_matches_arrays_with_any_excluded_element() {
        let pattern = json!({"detail": {"state": [{"anything-but": ["a"]}]}});
        // moto/event-bridge: any element differing ⇒ match.
        assert!(matches_pattern(&pattern, &json!({"detail": {"state": ["a", "b"]}})));
        assert!(matches_pattern(&pattern, &json!({"detail": {"state": ["b", "c"]}})));
        // Every element excluded: no match.
        assert!(!matches_pattern(&pattern, &json!({"detail": {"state": ["a"]}})));
        assert!(!matches_pattern(&pattern, &json!({"detail": {"state": ["a", "a"]}})));
    }

    #[test]
    fn anything_but_scalar_exclusion_on_arrays() {
        let pattern = json!({"detail": {"state": [{"anything-but": "a"}]}});
        assert!(matches_pattern(&pattern, &json!({"detail": {"state": ["a", "b"]}})));
        assert!(!matches_pattern(&pattern, &json!({"detail": {"state": ["a", "a"]}})));
    }
}
