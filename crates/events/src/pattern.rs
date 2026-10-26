//! EventBridge event pattern matching semantics, ported from the Python
//! provider's `_match_pattern` (models.py): prefix, suffix, numeric, exists,
//! anything-but, exact list matches, and nested objects — including moto
//! PR #10360's array-of-objects matching (event-ruler "JSON Array
//! Matching").

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
                if absent_ok {
                    continue;
                }
                // moto still descends into an object pattern behind an
                // absent ancestor: `_does_event_match` walks the pattern
                // by path prefix and only resolves leaves against the
                // flattened event, where a missing parent reads back
                // UNDEFINED. The whole subtree is therefore judged
                // against an undefined item rather than failing here.
                if let Some(sub_pattern) = pattern_value.as_object() {
                    if pattern_matches_undefined(sub_pattern) {
                        continue;
                    }
                }
                return false;
            }
        };

        match pattern_value {
            Value::Object(sub_pattern) => {
                let ok = match event_value {
                    Value::Object(obj) => matches(sub_pattern, obj),
                    Value::Array(arr) => array_objects_match(sub_pattern, arr),
                    // An explicit null ancestor behaves like an absent
                    // one for moto: the flattened event stores
                    // `"detail": null` as a terminal leaf and the
                    // pattern walk beneath it resolves UNDEFINED.
                    Value::Null => pattern_matches_undefined(sub_pattern),
                    _ => false,
                };
                if !ok {
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

/// Judge a pattern subtree against a missing or null region, where every
/// leaf reads back moto's UNDEFINED sentinel
/// (`_does_item_match_filters(UNDEFINED, filters)`): a filter list is
/// satisfied there iff any member is an `{"exists": false}` filter, and
/// no other leaf shape can see the item. Nested objects recurse the same
/// way, `$or` needs any one sub-pattern to pass, and scalar pattern
/// leaves never do.
fn pattern_matches_undefined(pattern: &Map<String, Value>) -> bool {
    pattern.iter().all(|(key, pattern_value)| {
        if key == "$or" {
            let Some(subs) = pattern_value.as_array() else {
                return false;
            };
            subs.iter()
                .filter_map(|sp| sp.as_object())
                .any(pattern_matches_undefined)
        } else {
            match pattern_value {
                Value::Object(sub_pattern) => pattern_matches_undefined(sub_pattern),
                Value::Array(list) => list.iter().any(|pv| {
                    pv.as_object()
                        .and_then(|o| o.get("exists"))
                        .and_then(Value::as_bool)
                        == Some(false)
                }),
                _ => false,
            }
        }
    })
}

/// A nested pattern against an array (moto PR #10360, event-ruler "JSON
/// Array Matching"): it must be satisfied by a SINGLE element, so fields
/// never mix across different elements of the same array. Elements that
/// are arrays are looked through too (moto `_objects_in_array`); scalar
/// elements never match.
fn array_objects_match(pattern: &Map<String, Value>, arr: &[Value]) -> bool {
    arr.iter().any(|item| match item {
        Value::Object(obj) => matches(pattern, obj),
        Value::Array(nested) => array_objects_match(pattern, nested),
        _ => false,
    })
}

/// A list of acceptable values/matchers for one event field.
///
/// Public so the Lambda event-source-mapping engine can reuse the exact
/// same operator semantics (prefix/suffix/equals-ignore-case/numeric/
/// exists/anything-but/cidr/wildcard) for its `FilterCriteria` leaves.
///
/// Dict leaves are moto's `_does_item_match_named_filter`: the `exists`
/// filter applies to the value itself, every other named filter is
/// evaluated per element of an array value, and unnamed dict leaves never
/// match (era: no branch fires).
pub fn list_match(pattern_values: &[Value], event_value: &Value) -> bool {
    for pv in pattern_values {
        if let Some(filter) = pv.as_object() {
            if named_filter_matches(filter, event_value) {
                return true;
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

/// One named filter (`{"prefix": "a"}`, `{"exists": true}`, ...) against
/// the whole event value. moto checks `exists` on the value itself and
/// applies every other named filter per element of an array value.
fn named_filter_matches(filter: &Map<String, Value>, event_value: &Value) -> bool {
    if let Some(expected) = filter.get("exists").and_then(Value::as_bool) {
        // An `exists` leaf tests literal key presence, not JSON
        // nullness: moto `_does_item_match_named_filter` computes
        // `exists = item is not UNDEFINED; return exists ==
        // filter_value`, and a flattened JSON null is a present value
        // (event-ruler's exists machine likewise answers on the key,
        // not its contents). The value in hand is present by
        // construction — `matches` routes absent keys and null-ancestor
        // regions through `pattern_matches_undefined` and never feeds
        // them to a leaf — so `exists: true` matches any value,
        // explicit null included, and `exists: false` never matches at
        // a leaf (absence is decided upstream).
        return expected;
    }
    match event_value {
        Value::Array(arr) => arr.iter().any(|item| evaluate_named_filter(filter, item)),
        other => evaluate_named_filter(filter, other),
    }
}

/// Evaluate a named filter against one (scalar) event value. Several
/// recognized filter names may appear in one leaf; they OR together.
fn evaluate_named_filter(filter: &Map<String, Value>, value: &Value) -> bool {
    // prefix / suffix: the target is a string, a list of strings, or an
    // {"equals-ignore-case": "..."} object (moto prefix/suffix forms).
    if let Some(target) = filter.get("prefix") {
        if string_targets_match(target, value.as_str(), true) {
            return true;
        }
    }
    if let Some(target) = filter.get("suffix") {
        if string_targets_match(target, value.as_str(), false) {
            return true;
        }
    }
    if let Some(target) = filter.get("equals-ignore-case") {
        let Some(s) = value.as_str() else {
            return false;
        };
        let matches = match target {
            Value::String(t) => s.to_lowercase() == t.to_lowercase(),
            Value::Array(list) => list
                .iter()
                .any(|t| t.as_str().map(|t| s.to_lowercase() == t.to_lowercase()) == Some(true)),
            _ => false,
        };
        if matches {
            return true;
        }
    }
    if let Some(ops) = filter.get("numeric").and_then(Value::as_array) {
        if match_numeric(ops, value) {
            return true;
        }
    }
    if let Some(excluded) = filter.get("anything-but") {
        if anything_but(excluded, value) {
            return true;
        }
    }
    if let Some(network) = filter.get("cidr") {
        if cidr_matches(value, network) {
            return true;
        }
    }
    if let Some(target) = filter.get("wildcard") {
        if wildcard_targets_match(target, value.as_str()) {
            return true;
        }
    }
    false
}

/// String prefix/suffix against a scalar, list, or
/// `equals-ignore-case`-object target (moto prefix/suffix forms).
fn string_targets_match(target: &Value, value: Option<&str>, prefix: bool) -> bool {
    let Some(s) = value else {
        return false;
    };
    match target {
        Value::String(t) => {
            if prefix {
                s.starts_with(t)
            } else {
                s.ends_with(t)
            }
        }
        Value::Array(list) => list.iter().any(|t| {
            t.as_str().map(|t| {
                if prefix {
                    s.starts_with(t)
                } else {
                    s.ends_with(t)
                }
            }) == Some(true)
        }),
        Value::Object(obj) => obj
            .get("equals-ignore-case")
            .and_then(Value::as_str)
            .map(|t| {
                let sv = s.to_lowercase();
                let tv = t.to_lowercase();
                if prefix {
                    sv.starts_with(&tv)
                } else {
                    sv.ends_with(&tv)
                }
            })
            .unwrap_or(false),
        _ => false,
    }
}

/// wildcard target: a glob string or a list of them.
fn wildcard_targets_match(target: &Value, value: Option<&str>) -> bool {
    let Some(s) = value else {
        return false;
    };
    match target {
        Value::String(pattern) => wildcard_match(s, pattern),
        Value::Array(list) => list
            .iter()
            .any(|p| p.as_str().map(|p| wildcard_match(s, p)) == Some(true)),
        _ => false,
    }
}

/// moto `_wildcard_match`: `*` matches any run of characters, `\*` and
/// `\\` are literal, any other `\x` pair unescapes to backslash + `x`
/// (moto re-escapes the backslash and keeps scanning at the next char).
fn wildcard_match(item: &str, pattern: &str) -> bool {
    // Split on unescaped `*` into literal segments.
    let mut segments: Vec<String> = vec![String::new()];
    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\\' => {
                if let Some(&next_c) = chars.peek() {
                    if next_c == '*' || next_c == '\\' {
                        segments.last_mut().expect("seeded").push(next_c);
                        chars.next();
                        continue;
                    }
                }
                segments.last_mut().expect("seeded").push('\\');
            }
            '*' => segments.push(String::new()),
            other => segments.last_mut().expect("seeded").push(other),
        }
    }
    if segments.len() == 1 {
        return item == segments[0];
    }
    let first = &segments[0];
    let last = segments.last().expect("seeded");
    let Some(after_first) = item.strip_prefix(first.as_str()) else {
        return false;
    };
    // The trailing literal must fit; middle literals appear in order in
    // the region strictly before it (moto glues the literals with `.*`
    // and fullmatches).
    if after_first.len() < last.len() {
        return false;
    }
    let middle_area = &after_first[..after_first.len() - last.len()];
    let mut cursor = 0usize;
    for segment in &segments[1..segments.len() - 1] {
        match middle_area[cursor..].find(segment.as_str()) {
            Some(pos) => cursor += pos + segment.len(),
            None => return false,
        }
    }
    true
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
            Value::Object(obj) => arr.iter().any(|v| !named_filter_matches(obj, v)),
            _ => arr.iter().any(|v| v != excluded),
        },
        _ => match excluded {
            Value::Object(obj) => !named_filter_matches(obj, event_value),
            Value::Array(ex) => !ex.contains(event_value),
            _ => event_value != excluded,
        },
    }
}

/// moto `cidr`: the value is an IP inside `ip_network(filter_value)`,
/// strict (host bits set in the base raise → no match). Scalar targets
/// only (moto's `ip_network` rejects lists the same way).
fn cidr_matches(value: &Value, network: &Value) -> bool {
    let (Some(address), Some(spec)) = (value.as_str(), network.as_str()) else {
        return false;
    };
    let (base_str, prefix_str) = spec.split_once('/').unwrap_or((spec, ""));
    let (Ok(address), Ok(base)) = (
        address.parse::<std::net::IpAddr>(),
        base_str.parse::<std::net::IpAddr>(),
    ) else {
        return false;
    };
    if std::mem::discriminant(&address) != std::mem::discriminant(&base) {
        return false;
    }
    let bits: u32 = match base {
        std::net::IpAddr::V4(_) => 32,
        std::net::IpAddr::V6(_) => 128,
    };
    let prefix: u32 = match prefix_str.parse::<u32>() {
        // ip_network without a prefix takes the full netmask.
        _ if prefix_str.is_empty() => bits,
        Ok(len) if len <= bits => len,
        _ => return false,
    };
    let as_bits = |ip: &std::net::IpAddr| -> u128 {
        match ip {
            std::net::IpAddr::V4(v4) => u32::from(*v4) as u128,
            std::net::IpAddr::V6(v6) => u128::from(*v6),
        }
    };
    let net_mask = if prefix == 0 {
        0
    } else {
        u128::MAX << (bits - prefix)
    };
    let (base_bits, address_bits) = (as_bits(&base), as_bits(&address));
    // Strict: host bits set → ip_network raises → moto returns false.
    if base_bits & net_mask != base_bits {
        return false;
    }
    base_bits == (address_bits & net_mask)
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

/// Store-time event-pattern validation (moto `EventPatternParser`:
/// `_validate_event_pattern` — era's `_put_rule` runs the same top-level
/// scan). Each value in the pattern must be an object or a non-empty
/// array; object leaves recurse.
pub fn validate_event_pattern(pattern: &Value) -> Result<(), String> {
    fn sub_reason(so_far: String, key: &str) -> String {
        if so_far.is_empty() {
            key.to_string()
        } else {
            format!("{so_far}.{key}")
        }
    }
    match pattern {
        Value::Object(map) => {
            for (key, value) in map {
                match value {
                    Value::Object(_) => {
                        validate_event_pattern(value)?;
                    }
                    Value::Array(list) => {
                        if list.is_empty() {
                            return Err("Empty arrays are not allowed".to_string());
                        }
                    }
                    _ => {
                        let path = sub_reason(String::new(), key);
                        return Err(format!("'{path}' must be an object or an array"));
                    }
                }
            }
            Ok(())
        }
        Value::Array(list) if list.is_empty() => Err("Empty arrays are not allowed".to_string()),
        _ => Err("Value of each key must be an array or an object.".to_string()),
    }
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
    //
    // `exists` is decided by literal key presence, not JSON nullness:
    // moto `_does_item_match_named_filter` computes
    // `exists = item is not UNDEFINED; return exists == filter_value`
    // (verified live against the pinned fork: a flattened null leaf is
    // a present value) and event-ruler's exists machine likewise
    // answers on the key rather than its contents. These tests
    // previously pinned the predecessor's `is_null()` reading, which
    // had the null column of the truth table inverted; matching moto
    // here is the deliberate real-AWS-better decision.

    #[test]
    fn exists_true_matches_present_values() {
        let pattern = json!({"detail": {"state": [{"exists": true}]}});
        assert!(matches_pattern(
            &pattern,
            &json!({"detail": {"state": "running"}})
        ));
        assert!(matches_pattern(&pattern, &json!({"detail": {"state": 0}})));
        // A null value is still a present key: `exists: true` matches
        // it (moto: `item is not UNDEFINED` is true for a flattened
        // null; the old era reading said "absent" here).
        assert!(matches_pattern(
            &pattern,
            &json!({"detail": {"state": null}})
        ));
        // Absent key: `exists: true` must not match.
        assert!(!matches_pattern(&pattern, &json!({"detail": {}})));
        // A stored whole-ancestor null (`detail: null`) leaves the leaf
        // behind it UNDEFINED: no match.
        assert!(!matches_pattern(&pattern, &json!({"detail": null})));
        // Same presence reading when the key ITSELF is the null leaf:
        // pattern tests `detail` directly, event has `detail: null`.
        let key_pattern = json!({"detail": [{"exists": true}]});
        assert!(matches_pattern(&key_pattern, &json!({"detail": null})));
        assert!(!matches_pattern(&key_pattern, &json!({})));
    }

    #[test]
    fn exists_false_matches_absent_keys_only() {
        let pattern = json!({"detail": {"state": [{"exists": false}]}});
        // The absent leaf is the one case exists:false matches.
        assert!(matches_pattern(&pattern, &json!({"detail": {}})));
        // A null ancestor reads UNDEFINED beneath it (absent-ancestor
        // descent): matches.
        assert!(matches_pattern(&pattern, &json!({"detail": null})));
        // A null-valued leaf is present: `exists: false` must not match
        // (moto: `exists = item is not UNDEFINED` -> True != False; the
        // old era reading matched here).
        assert!(!matches_pattern(
            &pattern,
            &json!({"detail": {"state": null}})
        ));
        assert!(!matches_pattern(
            &pattern,
            &json!({"detail": {"state": "on"}})
        ));
        // The key itself null = present: `exists: false` misses it.
        assert!(!matches_pattern(
            &json!({"detail": [{"exists": false}]}),
            &json!({"detail": null})
        ));
        assert!(matches_pattern(
            &json!({"detail": [{"exists": false}]}),
            &json!({})
        ));
    }

    // -- absent ancestors must not kill exists:false subtrees ------------------
    // moto walks the pattern recursively by path, as long as the pattern
    // continues, even if the parent key is absent (event-ruler "Machines
    // behind absent-key patterns").
    #[test]
    fn exists_false_under_absent_ancestor_matches() {
        let pattern = json!({"detail": {"state": [{"exists": false}]}});
        // The whole `detail` tree is missing (previously returned false).
        assert!(matches_pattern(
            &pattern,
            &json!({"source": "app", "detail-type": "t"})
        ));
        // `detail: null` is stored as a terminal leaf, wherever the pattern
        // asks below it, moto reads UNDEFINED and matches.
        assert!(matches_pattern(&pattern, &json!({"detail": null})));
        // Deep nesting through absent ancestors.
        assert!(matches_pattern(
            &json!({"detail": {"a": {"b": [{"exists": false}]}}}),
            &json!({"source": "app"})
        ));
        // $or needs only one sub-pattern to be satisfiable by a missing key.
        assert!(matches_pattern(
            &json!({"detail": {"$or": [{"state": [{"exists": false}]}, {"other": [{"exists": true}]}]}}),
            &json!({})
        ));
        // A filter list passes so long as some member is exists:false.
        assert!(matches_pattern(
            &json!({"detail": {"state": [{"exists": false}, "running"]}}),
            &json!({})
        ));
    }

    #[test]
    fn absent_ancestor_rejects_filters_that_need_a_value() {
        // exists:true at a leaf, under absent and null ancestors, no match.
        let pattern = json!({"detail": {"state": [{"exists": true}]}});
        assert!(!matches_pattern(&pattern, &json!({})));
        assert!(!matches_pattern(&pattern, &json!({"detail": null})));
        // Scalar and plain-value list leaves have nothing to see.
        assert!(!matches_pattern(
            &json!({"detail": {"state": ["running"]}}),
            &json!({})
        ));
        assert!(!matches_pattern(
            &json!({"detail": {"state": ["running", "stopped"]}}),
            &json!({"detail": null})
        ));
        // A sibling exists:true kills the subtree even when another leaf
        // is exists:false.
        let mixed = json!({
            "detail": {
                "state": [{"exists": false}],
                "code": [{"exists": true}]
            }
        });
        assert!(!matches_pattern(&mixed, &json!({})));
        // A present leaf value still means the subtree does not match.
        assert!(!matches_pattern(
            &json!({"detail": {"state": [{"exists": false}]}}),
            &json!({"detail": {"state": "running"}})
        ));
        assert!(!matches_pattern(
            &json!({"detail": {"a": {"b": [{"exists": false}]}}}),
            &json!({"detail": {"a": {"b": "x"}}})
        ));
    }

    // -- anything-but on array values (F2, agent-221 finding 2) ----------------

    #[test]
    fn anything_but_matches_arrays_with_any_excluded_element() {
        let pattern = json!({"detail": {"state": [{"anything-but": ["a"]}]}});
        // moto/event-bridge: any element differing ⇒ match.
        assert!(matches_pattern(
            &pattern,
            &json!({"detail": {"state": ["a", "b"]}})
        ));
        assert!(matches_pattern(
            &pattern,
            &json!({"detail": {"state": ["b", "c"]}})
        ));
        // Every element excluded: no match.
        assert!(!matches_pattern(
            &pattern,
            &json!({"detail": {"state": ["a"]}})
        ));
        assert!(!matches_pattern(
            &pattern,
            &json!({"detail": {"state": ["a", "a"]}})
        ));
    }

    #[test]
    fn anything_but_scalar_exclusion_on_arrays() {
        let pattern = json!({"detail": {"state": [{"anything-but": "a"}]}});
        assert!(matches_pattern(
            &pattern,
            &json!({"detail": {"state": ["a", "b"]}})
        ));
        assert!(!matches_pattern(
            &pattern,
            &json!({"detail": {"state": ["a", "a"]}})
        ));
    }

    // -- moto named filter set (F3, agent-221 finding 3) -----------------------

    #[test]
    fn equals_ignore_case_and_caseless_prefix_suffix() {
        let eq = json!({"source": [{"equals-ignore-case": "My.App"}]});
        assert!(matches_pattern(&eq, &json!({"source": "my.app"})));
        assert!(matches_pattern(&eq, &json!({"source": "MY.APP"})));
        assert!(!matches_pattern(&eq, &json!({"source": "my.app2"})));
        // ...and as the value form of prefix/suffix.
        let pfx = json!({"detail": {"kind": [{"prefix": {"equals-ignore-case": "CREAT"}}]}});
        assert!(matches_pattern(
            &pfx,
            &json!({"detail": {"kind": "created"}})
        ));
        assert!(!matches_pattern(
            &pfx,
            &json!({"detail": {"kind": "deleted"}})
        ));
        let sfx = json!({"detail": {"kind": [{"suffix": {"equals-ignore-case": "ED"}}]}});
        assert!(matches_pattern(
            &sfx,
            &json!({"detail": {"kind": "Created"}})
        ));
        assert!(!matches_pattern(
            &sfx,
            &json!({"detail": {"kind": "delete"}})
        ));
    }

    #[test]
    fn list_valued_prefix_and_suffix() {
        let pattern = json!({"source": [{"prefix": ["my.a", "your.b"]}]});
        assert!(matches_pattern(&pattern, &json!({"source": "your.bucket"})));
        assert!(!matches_pattern(
            &pattern,
            &json!({"source": "other.bucket"})
        ));
        let pattern = json!({"source": [{"suffix": [".png", ".jpg"]}]});
        assert!(matches_pattern(&pattern, &json!({"source": "pic.jpg"})));
        assert!(!matches_pattern(&pattern, &json!({"source": "pic.gif"})));
    }

    #[test]
    fn cidr_named_filter() {
        let pattern = json!({"detail": {"source_ip": [{"cidr": "203.0.113.0/24"}]}});
        assert!(matches_pattern(
            &pattern,
            &json!({"detail": {"source_ip": "203.0.113.9"}})
        ));
        assert!(!matches_pattern(
            &pattern,
            &json!({"detail": {"source_ip": "203.0.114.1"}})
        ));
        // Non-IP values and foreign families never match.
        assert!(!matches_pattern(
            &pattern,
            &json!({"detail": {"source_ip": "none"}})
        ));
        assert!(!matches_pattern(
            &json!({"detail": {"source_ip": [{"cidr": "2001:db8::/32"}]}}),
            &json!({"detail": {"source_ip": "203.0.113.9"}})
        ));
    }

    #[test]
    fn wildcard_named_filter() {
        let pattern = json!({"detail": {"path": [{"wildcard": "/tmp/*"}]}});
        assert!(matches_pattern(
            &pattern,
            &json!({"detail": {"path": "/tmp/logs/app"}})
        ));
        assert!(!matches_pattern(
            &pattern,
            &json!({"detail": {"path": "/var/tmp/x"}})
        ));
        // Escaped stars are literal; list targets OR together.
        assert!(matches_pattern(
            &json!({"detail": {"path": [{"wildcard": ["C:\\\\*", "id-*"]}]}}),
            &json!({"detail": {"path": "id-42"}})
        ));
        assert!(!matches_pattern(
            &json!({"detail": {"path": [{"wildcard": "a\\*b"}]}}),
            &json!({"detail": {"path": "axxb"}})
        ));
        assert!(matches_pattern(
            &json!({"detail": {"path": [{"wildcard": "a\\*b"}]}}),
            &json!({"detail": {"path": "a*b"}})
        ));
        // Per-element evaluation on array event values (moto any(evaluate(i))).
        assert!(matches_pattern(
            &pattern,
            &json!({"detail": {"path": ["/var/tmp", "/tmp/x"]}})
        ));
        assert!(!matches_pattern(
            &pattern,
            &json!({"detail": {"path": ["/var/tmp", "/var/x"]}})
        ));
    }

    // -- array-of-objects matching (moto PR #10360) ---------------------------

    #[test]
    fn array_object_nested_pattern_matches_array_of_objects() {
        let pattern = json!({"detail": {"responseElements": {"instancesSet": {"items": {"instanceId": ["i-2"]}}}}});
        // Any single element satisfying the nested pattern matches.
        assert!(matches_pattern(
            &pattern,
            &json!({"detail": {"responseElements": {"instancesSet": {"items": [{"instanceId": "i-1"}, {"instanceId": "i-2"}]}}}})
        ));
        assert!(!matches_pattern(
            &pattern,
            &json!({"detail": {"responseElements": {"instancesSet": {"items": [{"instanceId": "i-1"}, {"instanceId": "i-3"}]}}}})
        ));
        // A single object (non-array) keeps the old semantics.
        assert!(matches_pattern(
            &pattern,
            &json!({"detail": {"responseElements": {"instancesSet": {"items": {"instanceId": "i-2"}}}}})
        ));
        assert!(!matches_pattern(
            &pattern,
            &json!({"detail": {"responseElements": {"instancesSet": {"items": {"instanceId": "i-3"}}}}})
        ));
        // An array of objects nested one level inside an element.
        assert!(matches_pattern(
            &json!({"detail": {"pairs": {"instances": {"items": {"id": ["i-2"]}}}}}),
            &json!({"detail": {"pairs": [{"instances": {"items": [{"id": "i-1"}, {"id": "i-2"}]}}]}})
        ));
    }

    #[test]
    fn array_object_fields_must_match_within_one_element() {
        let pattern =
            json!({"detail": {"employees": {"firstName": ["Anna"], "lastName": ["Smith"]}}});
        // No single element has both fields matching (John lacks firstName,
        // Anna lacks lastName): no cross-element mixing.
        assert!(!matches_pattern(
            &pattern,
            &json!({"detail": {"employees": [
                {"firstName": "John", "lastName": "Smith"},
                {"firstName": "Anna", "lastName": "Doe"}
            ]}})
        ));
        assert!(matches_pattern(
            &pattern,
            &json!({"detail": {"employees": [
                {"firstName": "John", "lastName": "Smith"},
                {"firstName": "Anna", "lastName": "Smith"}
            ]}})
        ));
        // The attribute must sit inside the same element.
        let pattern = json!({"detail": {"items": {"id": ["x"]}}});
        assert!(matches_pattern(
            &pattern,
            &json!({"detail": {"items": [{"id": "x"}, {"other": 1}]}})
        ));
        assert!(!matches_pattern(
            &pattern,
            &json!({"detail": {"items": [{"id": "y"}]}})
        ));
        assert!(!matches_pattern(
            &pattern,
            &json!({"detail": {"items": [{"id": "y"}, {"id2": "x"}]}})
        ));
    }

    #[test]
    fn array_object_named_filters_and_multilevel_arrays() {
        let pattern =
            json!({"detail": {"containers": {"state": {"exitCode": [{"numeric": [">", 0]}]}}}});
        assert!(matches_pattern(
            &pattern,
            &json!({"detail": {"containers": [{"state": {"exitCode": 0}}, {"state": {"exitCode": 137}}]}})
        ));
        assert!(!matches_pattern(
            &pattern,
            &json!({"detail": {"containers": [{"state": {"exitCode": 0}}]}})
        ));
        // Arrays of arrays are looked through the same way.
        assert!(matches_pattern(
            &pattern,
            &json!({"detail": {"containers": [[{"state": {"exitCode": 0}}], [{"state": {"exitCode": 1}}]]}})
        ));
        assert!(!matches_pattern(
            &pattern,
            &json!({"detail": {"containers": [[{"state": {"exitCode": 0}}]]}})
        ));
        // Empty arrays and scalar-only arrays match nothing.
        assert!(!matches_pattern(
            &pattern,
            &json!({"detail": {"containers": []}})
        ));
        assert!(!matches_pattern(
            &pattern,
            &json!({"detail": {"containers": ["a", "b"]}})
        ));
    }
}
