//! AWS StepFunctions JSONPath dialect evaluator (hand-rolled; no new crate
//! dependency).
//!
//! The dialect subset follows moto's `extract_json` (the pinned oracle)
//! with the two real-AWS divergences moto records in issue
//! [#10078](https://github.com/getmoto/moto/issues/10078):
//!
//! - filter expressions accept the AWS conjunctions `&&` / `||` (CDK emits
//!   them everywhere — `$.queueItems[?(@.a=='1' && @.b=='2')]`);
//! - a filter (or wildcard / slice) that matches zero elements yields `[]`,
//!   where moto raised `NoSuchJsonPathError`;
//!
//! and the #7825 shape fix: a projection after any array traversal
//! (wildcard, slice, filter or definite index) keeps the array shape, while
//! a path ending in a literal `[n]` unpacks its single match.
//!
//! Supported syntax: `$.a.b`, bracket-quoted fields `['a.b']`, `[0]`
//! (negative counts from the end), `[start:end:step]` slices, `[*]` / `.*`
//! wildcards and `[?( ... )]` filters with `==` `!=` `<` `<=` `>` `>=`,
//! regex literals `== /re/i` and `=~`, `&&` `||` `!`, `@`-relative paths,
//! `$`-rooted paths inside filters, and `true` / `false` / `null` literals.
//! `..` descendant recursion and script expressions are not part of the
//! dialect; those paths resolve to `None` (treated as a missing path, like
//! the era's `_resolve_path`) rather than failing the engine.

use serde_json::{Value, Value as V};

/// Resolve an AWS-dialect JSONPath against `base`, carrying moto's
/// `extract_json` post-processing: `None` for a definite path that does not
/// exist, the field value for plain dot paths, `[]` for multi-valued paths
/// that match nothing, arrays for any array traversal (see module docs).
pub(crate) fn resolve(path: &str, base: &Value) -> Option<Value> {
    if path == "$" || path == "$$" {
        return Some(base.clone());
    }
    let steps = parse_steps(path)?;
    let mut matches = Vec::new();
    walk_seq(vec![base.clone()], &steps, base, &mut matches);
    if matches.is_empty() {
        // Empty filters (and slices / wildcards) yield []; a miss on a
        // definite path stays a missing path (era `_resolve_path`).
        return if steps.iter().any(Step::is_multi_valued) {
            Some(Value::Array(Vec::new()))
        } else {
            None
        };
    }
    if matches.len() > 1 {
        return Some(Value::Array(matches));
    }
    // Single match: a path ending in a literal index is a definite access
    // (moto `_is_singleton_array_access`), so AWS unpacks it; otherwise any
    // array traversal in the chain preserves the array shape (moto #7825)
    // and a plain dot path yields the field value itself.
    if steps.last().is_some_and(|s| matches!(s, Step::Index(_))) {
        return Some(matches.pop().expect("at least one match"));
    }
    if steps.iter().any(Step::traverses_array) {
        return Some(Value::Array(matches));
    }
    Some(matches.pop().expect("at least one match"))
}

/// One segment of a JSONPath after the leading `$`.
#[derive(Debug, Clone, PartialEq)]
enum Step {
    /// `.name` or `['name']` — single child access.
    Field(String),
    /// `[n]` — definite index (negative counts from the end).
    Index(i64),
    /// `[start:end:step]`.
    Slice {
        start: Option<f64>,
        end: Option<f64>,
        step: Option<f64>,
    },
    /// `[*]` or `.*` — all children.
    Wildcard,
    /// `[?( ... )]`.
    Filter(FilterRule),
}

impl Step {
    /// The step can expand into more than one value.
    fn is_multi_valued(&self) -> bool {
        matches!(self, Step::Wildcard | Step::Slice { .. } | Step::Filter(_))
    }

    /// The step traverses array land at some point (a wildcard, slice, filter
    /// or a definite index) — moto's singleton-shape heuristics key on array
    /// traversal rather than multi-valuedness alone (moto #7825).
    fn traverses_array(&self) -> bool {
        matches!(
            self,
            Step::Wildcard | Step::Slice { .. } | Step::Filter(_) | Step::Index(_)
        )
    }
}

/// A `[?( ... )]` filter expression in the AWS dialect.
#[derive(Debug, Clone, PartialEq)]
enum FilterRule {
    /// `&&` — the AWS-conformant conjunction (moto #10078; CDK emits these).
    And(Vec<FilterRule>),
    /// `||` — the AWS-conformant disjunction.
    Or(Vec<FilterRule>),
    Not(Box<FilterRule>),
    Cmp(FilterVal, FilterOp, FilterVal),
    /// Bare path / literal as a truthiness test (`[?(@.enabled)]`).
    Truthy(FilterVal),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilterOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    /// `== /re/` and `=~ /re/` (moto #7825 emits regex literals after plain
    /// equality too).
    Regex,
}

#[derive(Debug, Clone, PartialEq)]
enum FilterVal {
    /// `@.a[0].b` — relative to the filtered element — or `$.a[b]`, rooted
    /// at the path's own input.
    Path {
        from_root: bool,
        segs: Vec<FilterSeg>,
    },
    Str(String),
    Num(f64),
    Bool(bool),
    Null,
    Regex {
        pattern: String,
        ignore_case: bool,
    },
}

#[derive(Debug, Clone, PartialEq)]
enum FilterSeg {
    Field(String),
    Index(i64),
}

/// Upper bound on parsed path segments: longer paths read as unresolvable
/// syntax (resolving to a missing value, like other off-dialect paths)
/// instead of walking memory for a 20,000-segment crafted field.
const MAX_PATH_STEPS: usize = 512;

fn parse_steps(path: &str) -> Option<Vec<Step>> {
    let chars: Vec<char> = path.chars().collect();
    if chars.len() < 2 || chars[0] != '$' {
        return None;
    }
    // Not in the dialect: `..` descendant recursion (moto's JsonPathLexer
    // error in #7825 reproduces as a missing path here).
    if path.starts_with("$..") {
        return None;
    }
    let mut cur = Cursor { p: &chars, i: 1 };
    let mut steps: Vec<Step> = Vec::new();
    while let Some(c) = cur.peek() {
        match c {
            '$' => return None,
            '.' => {
                cur.i += 1;
                match cur.peek() {
                    Some('*') => {
                        cur.i += 1;
                        steps.push(Step::Wildcard);
                    }
                    Some('.') => return None,
                    _ => {
                        let name = take_name(&mut cur);
                        if name.is_empty() {
                            return None;
                        }
                        steps.push(Step::Field(name));
                    }
                }
            }
            '[' => {
                cur.i += 1;
                steps.push(parse_bracket(&mut cur)?);
                cur.skip_ws();
                if !cur.eat(']') {
                    return None;
                }
            }
            _ => return None,
        }
    }
    if steps.len() > MAX_PATH_STEPS {
        return None;
    }
    Some(steps)
}

fn parse_bracket(cur: &mut Cursor) -> Option<Step> {
    cur.skip_ws();
    let c = cur.peek()?;
    match c {
        '*' => {
            cur.i += 1;
            Some(Step::Wildcard)
        }
        '?' => {
            cur.i += 1;
            cur.skip_ws();
            if !cur.eat('(') {
                return None;
            }
            let rule = parse_filter_or(cur)?;
            cur.skip_ws();
            if !cur.eat(')') {
                return None;
            }
            Some(Step::Filter(rule))
        }
        '\'' | '"' => {
            let name = take_quoted(cur)?;
            Some(Step::Field(name))
        }
        _ => {
            cur.skip_ws();
            let first = match cur.peek() {
                Some(':') => None,
                _ => Some(take_number(cur)?),
            };
            cur.skip_ws();
            if cur.peek() == Some(':') {
                // Slice: one or two more optional numbers, then a step.
                cur.i += 1;
                cur.skip_ws();
                let end = match cur.peek() {
                    Some(':') | Some(']') => None,
                    _ => Some(take_number(cur)?),
                };
                cur.skip_ws();
                let step = if cur.peek() == Some(':') {
                    cur.i += 1;
                    if cur.peek() == Some(']') {
                        None
                    } else {
                        let s = take_number(cur)?;
                        if s == 0.0 {
                            // A zero step matches nothing (jsonpath_ng).
                            return Some(Step::Slice {
                                start: first,
                                end,
                                step: Some(0.0),
                            });
                        }
                        Some(s.floor())
                    }
                } else {
                    None
                };
                cur.skip_ws();
                Some(Step::Slice {
                    start: first,
                    end,
                    step,
                })
            } else {
                // Definite index; a fractional index reads as unknown syntax.
                let n = first?;
                (n.fract() == 0.0).then_some(Step::Index(n as i64))
            }
        }
    }
}

/// Characters an unquoted JSONPath/filter field name may contain (the JSON
/// key charset; keys with dots or brackets need the bracket-quoted form).
fn is_path_name_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '$' || c == '-' || c == '!'
}

fn take_name(cur: &mut Cursor) -> String {
    let mut name = String::new();
    while let Some(c) = cur.peek() {
        if is_path_name_char(c) {
            name.push(c);
            cur.i += 1;
        } else {
            break;
        }
    }
    name
}

fn take_quoted(cur: &mut Cursor) -> Option<String> {
    let quote = cur.bump()?;
    let mut out = String::new();
    loop {
        let c = cur.bump()?;
        match c {
            c if c == quote => break,
            '\\' => {
                let esc = cur.bump()?;
                out.push(match esc {
                    'n' => '\n',
                    't' => '\t',
                    'r' => '\r',
                    other => other,
                });
            }
            c => out.push(c),
        }
    }
    Some(out)
}

fn take_number(cur: &mut Cursor) -> Option<f64> {
    cur.skip_ws();
    let mut text = String::new();
    if cur.peek() == Some('-') || cur.peek() == Some('+') {
        text.push(cur.bump()?);
    }
    let mut digits = 0;
    while cur.peek().is_some_and(|c| c.is_ascii_digit()) {
        text.push(cur.bump()?);
        digits += 1;
    }
    if cur.peek() == Some('.') {
        text.push(cur.bump()?);
        while cur.peek().is_some_and(|c| c.is_ascii_digit()) {
            text.push(cur.bump()?);
        }
    }
    if cur.peek().is_some_and(|c| c == 'e' || c == 'E') {
        let save = cur.i;
        let mut exp = String::new();
        exp.push(cur.bump()?);
        if matches!(cur.peek(), Some('+') | Some('-')) {
            exp.push(cur.bump()?);
        }
        if cur.peek().is_some_and(|c| c.is_ascii_digit()) {
            while cur.peek().is_some_and(|c| c.is_ascii_digit()) {
                exp.push(cur.bump()?);
            }
            text.push_str(&exp);
        } else {
            cur.i = save;
        }
    }
    (digits > 0).then(|| text.parse::<f64>().unwrap_or(f64::NAN))
}

// ---- Filter expression grammar -------------------------------------------

/// A char cursor over a path / filter / pattern source, positioned by
/// `char` index (sources are pre-collected into a `Vec<char>`).
struct Cursor<'a> {
    p: &'a [char],
    i: usize,
}

impl<'a> Cursor<'a> {
    fn peek(&self) -> Option<char> {
        self.p.get(self.i).copied()
    }

    fn bump(&mut self) -> Option<char> {
        let c = self.peek();
        if c.is_some() {
            self.i += 1;
        }
        c
    }

    fn eat(&mut self, c: char) -> bool {
        if self.peek() == Some(c) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn eat_text(&mut self, text: &str) -> bool {
        let want: Vec<char> = text.chars().collect();
        if self.p[self.i.min(self.p.len())..].starts_with(&want[..]) {
            self.i += want.len();
            true
        } else {
            false
        }
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(' ') | Some('\t')) {
            self.i += 1;
        }
    }
}

/// `<and> ('||' <and>)*`
fn parse_filter_or(cur: &mut Cursor) -> Option<FilterRule> {
    let mut parts = vec![parse_filter_and(cur)?];
    cur.skip_ws();
    while cur.eat_text("||") {
        parts.push(parse_filter_and(cur)?);
        cur.skip_ws();
    }
    if parts.len() == 1 {
        Some(parts.pop()?)
    } else {
        Some(FilterRule::Or(parts))
    }
}

/// `<not> ('&&' <not>)*`
fn parse_filter_and(cur: &mut Cursor) -> Option<FilterRule> {
    let mut parts = vec![parse_filter_unary(cur)?];
    cur.skip_ws();
    while cur.eat_text("&&") {
        parts.push(parse_filter_unary(cur)?);
        cur.skip_ws();
    }
    if parts.len() == 1 {
        Some(parts.pop()?)
    } else {
        Some(FilterRule::And(parts))
    }
}

fn parse_filter_unary(cur: &mut Cursor) -> Option<FilterRule> {
    cur.skip_ws();
    if cur.eat('!') {
        return Some(FilterRule::Not(Box::new(parse_filter_unary(cur)?)));
    }
    if cur.eat('(') {
        let rule = parse_filter_or(cur)?;
        cur.skip_ws();
        return if cur.eat(')') { Some(rule) } else { None };
    }
    let lhs = parse_filter_val(cur)?;
    cur.skip_ws();
    let op: Option<FilterOp> = if cur.eat_text("==") {
        Some(FilterOp::Eq)
    } else if cur.eat_text("!=") {
        Some(FilterOp::Ne)
    } else if cur.eat_text("<=") {
        Some(FilterOp::Le)
    } else if cur.eat_text(">=") {
        Some(FilterOp::Ge)
    } else if cur.eat_text("=~") {
        Some(FilterOp::Regex)
    } else if cur.eat_text("==") {
        Some(FilterOp::Eq)
    } else if cur.eat('<') {
        Some(FilterOp::Lt)
    } else if cur.eat('>') {
        Some(FilterOp::Gt)
    } else {
        None
    };
    match op {
        Some(op) => {
            let rhs = parse_filter_val(cur)?;
            Some(FilterRule::Cmp(lhs, op, rhs))
        }
        None => Some(FilterRule::Truthy(lhs)),
    }
}

/// A comparison operand: literal, regex literal or `@`/`$`-rooted path.
fn parse_filter_val(cur: &mut Cursor) -> Option<FilterVal> {
    cur.skip_ws();
    match cur.peek()? {
        '\'' | '"' => Some(FilterVal::Str(take_quoted(cur)?)),
        '/' => {
            cur.i += 1;
            let mut pattern = String::new();
            let mut escaped = false;
            loop {
                let c = cur.bump()?;
                if escaped {
                    pattern.push(c);
                    escaped = false;
                    continue;
                }
                match c {
                    '\\' => escaped = true,
                    '/' => break,
                    c => pattern.push(c),
                }
            }
            if pattern.is_empty() {
                return None;
            }
            let ignore_case = if cur.peek() == Some('i') {
                cur.i += 1;
                true
            } else {
                false
            };
            Some(FilterVal::Regex {
                pattern,
                ignore_case,
            })
        }
        '@' => {
            cur.i += 1;
            let segs = parse_filter_path(cur)?;
            Some(FilterVal::Path {
                from_root: false,
                segs,
            })
        }
        '$' => {
            cur.i += 1;
            let segs = parse_filter_path(cur)?;
            Some(FilterVal::Path {
                from_root: true,
                segs,
            })
        }
        c if c.is_ascii_digit() || c == '-' => Some(FilterVal::Num(take_number(cur)?)),
        't' | 'f' | 'n' => {
            for (word, val) in [
                ("true", FilterVal::Bool(true)),
                ("false", FilterVal::Bool(false)),
                ("null", FilterVal::Null),
            ] {
                let save = cur.i;
                if cur.eat_text(word) {
                    return Some(val);
                }
                cur.i = save;
            }
            None
        }
        _ => None,
    }
}

/// The `@.a[0].b` / `$.a[0].b` segment list after the root sigil.
fn parse_filter_path(cur: &mut Cursor) -> Option<Vec<FilterSeg>> {
    let mut segs = Vec::new();
    loop {
        match cur.peek() {
            Some('.') => {
                cur.i += 1;
                if cur.peek() == Some('.') {
                    return None;
                }
                let name = take_name(cur);
                if name.is_empty() {
                    return None;
                }
                segs.push(FilterSeg::Field(name));
            }
            Some('[') => {
                cur.i += 1;
                cur.skip_ws();
                let n = take_number(cur)?;
                if n.fract() != 0.0 {
                    return None;
                }
                cur.skip_ws();
                if !cur.eat(']') {
                    return None;
                }
                segs.push(FilterSeg::Index(n as i64));
            }
            _ => break,
        }
    }
    Some(segs)
}

// ---- Filter evaluation ----------------------------------------------------

/// Evaluate a filter rule against one candidate element.
fn filter_matches(rule: &FilterRule, item: &Value, root: &Value) -> bool {
    match rule {
        FilterRule::And(parts) => parts.iter().all(|r| filter_matches(r, item, root)),
        FilterRule::Or(parts) => parts.iter().any(|r| filter_matches(r, item, root)),
        FilterRule::Not(inner) => !filter_matches(inner, item, root),
        FilterRule::Truthy(val) => match resolve_val(val, item, root) {
            Some(v) => is_truthy(&v),
            None => false,
        },
        FilterRule::Cmp(lhs, op, rhs) => cmp_eval(lhs, *op, rhs, item, root),
    }
}

/// Python-style truthiness, as jsonpath_ng's bare-atom filters behave.
fn is_truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0 && !f.is_nan()).unwrap_or(false),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

/// Resolve one comparison operand; regex literals resolve to `None` here and
/// are handled by the comparison branch itself.
fn resolve_val(val: &FilterVal, item: &Value, root: &Value) -> Option<Value> {
    match val {
        FilterVal::Path { from_root, segs } => {
            let mut cur = if *from_root { root } else { item };
            for seg in segs {
                match seg {
                    FilterSeg::Field(name) => cur = cur.get(name)?,
                    FilterSeg::Index(idx) => cur = index_of(cur, *idx)?,
                }
            }
            Some(cur.clone())
        }
        FilterVal::Str(s) => Some(V::String(s.clone())),
        FilterVal::Num(n) => Some(number_value(*n)),
        FilterVal::Bool(b) => Some(V::Bool(*b)),
        FilterVal::Null => Some(Value::Null),
        FilterVal::Regex { .. } => None,
    }
}

/// Loose-precision number shape like the era's Python rounding: whole
/// values become integers (`1.0` -> `1`) and NaN/inf yield null.
pub(crate) fn number_value(n: f64) -> Value {
    if n.is_finite() && n.fract() == 0.0 && n.abs() <= 9.007_199_254_740_991e15 {
        V::Number(serde_json::Number::from(n as i64))
    } else {
        serde_json::Number::from_f64(n)
            .map(V::Number)
            .unwrap_or(Value::Null)
    }
}

fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        // Python equality: 1 == 1.0 (jsonpath_ng semantics), and bools are
        // NOT numeric (`true == 1` is false, as in Python).
        (Value::Number(x), Value::Number(y)) => match (x.as_f64(), y.as_f64()) {
            (Some(x), Some(y)) => x == y,
            _ => false,
        },
        _ => a == b,
    }
}

/// One filter comparison; a regex literal on either side turns the
/// comparison into a regex search (jsonpath_ng ext semantics), and a missing
/// path comparison fails both `==` and `!=` like jsonpath_ng's Leaf.
fn cmp_eval(lhs: &FilterVal, op: FilterOp, rhs: &FilterVal, item: &Value, root: &Value) -> bool {
    for (pattern_side, other) in [(&rhs, lhs), (&lhs, rhs)] {
        if let FilterVal::Regex {
            pattern,
            ignore_case,
        } = pattern_side
        {
            return match resolve_val(other, item, root) {
                Some(Value::String(text)) => {
                    let matched = regex_matches(pattern, *ignore_case, &text);
                    match op {
                        FilterOp::Eq | FilterOp::Regex => matched,
                        FilterOp::Ne => !matched,
                        _ => false,
                    }
                }
                _ => false,
            };
        }
    }
    let (Some(a), Some(b)) = (resolve_val(lhs, item, root), resolve_val(rhs, item, root)) else {
        return false;
    };
    match op {
        FilterOp::Eq => values_equal(&a, &b),
        FilterOp::Ne => !values_equal(&a, &b),
        _ => ordered_cmp(&a, &b, op),
    }
}

/// `<`/`<=`/`>`/`>=` over numbers (bools excluded) or strings (timestamps
/// compare lexicographically, like the era's comparators).
fn ordered_cmp(a: &Value, b: &Value, op: FilterOp) -> bool {
    let ord = if a.is_number() && b.is_number() && !a.is_boolean() && !b.is_boolean() {
        match (a.as_f64(), b.as_f64()) {
            (Some(x), Some(y)) => x.partial_cmp(&y),
            _ => None,
        }
    } else {
        // Strings (name and timestamp comparisons order lexicographically).
        match (a.as_str(), b.as_str()) {
            (Some(x), Some(y)) => x.partial_cmp(y),
            _ => None,
        }
    };
    matches!(
        (ord, op),
        (Some(std::cmp::Ordering::Less), FilterOp::Lt)
            | (
                Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal),
                FilterOp::Le
            )
            | (Some(std::cmp::Ordering::Greater), FilterOp::Gt)
            | (
                Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal),
                FilterOp::Ge
            )
    )
}

// ---- Path walking ---------------------------------------------------------

/// Left-to-right match collection over a sequence of intermediate values.
///
/// Fields, wildcards and filters project per value; an index or slice that
/// follows a multi-valued step (wildcard / slice / filter) targets the
/// RESULT LIST itself — `$.items[?(@.picked)][0]` selects the first hit of
/// the filter rather than indexing each element (the per-element reading is
/// what produced the localstack singleton collapse noted in moto #7825).
///
/// Iterative over the step list: a path may carry up to `MAX_PATH_STEPS`
/// segments, and one stack frame per segment would abort the process on a
/// crafted field.
fn walk_seq(mut set: Vec<Value>, steps: &[Step], root: &Value, out: &mut Vec<Value>) {
    let mut prev_multi = false;
    for step in steps {
        let mut next: Vec<Value> = Vec::new();
        match step {
            Step::Field(name) => {
                for v in &set {
                    if let Some(sub) = v.get(name) {
                        next.push(sub.clone());
                    }
                }
            }
            Step::Index(idx) => {
                if prev_multi {
                    if let Some(sub) = seq_index(&set, *idx) {
                        next.push(sub.clone());
                    }
                } else {
                    for v in &set {
                        if let Some(sub) = index_of(v, *idx) {
                            next.push(sub.clone());
                        }
                    }
                }
            }
            Step::Slice { start, end, step } => {
                if prev_multi {
                    for i in slice_indices(set.len(), *start, *end, *step) {
                        next.push(set[i].clone());
                    }
                } else {
                    for v in &set {
                        if let Value::Array(items) = v {
                            for i in slice_indices(items.len(), *start, *end, *step) {
                                next.push(items[i].clone());
                            }
                        }
                    }
                }
            }
            Step::Wildcard => {
                for v in &set {
                    match v {
                        Value::Object(map) => {
                            for sub in map.values() {
                                next.push(sub.clone());
                            }
                        }
                        Value::Array(items) => {
                            for sub in items {
                                next.push(sub.clone());
                            }
                        }
                        _ => {}
                    }
                }
            }
            Step::Filter(rule) => {
                for v in &set {
                    match v {
                        Value::Object(map) => {
                            for sub in map.values() {
                                if filter_matches(rule, sub, root) {
                                    next.push(sub.clone());
                                }
                            }
                        }
                        Value::Array(items) => {
                            for sub in items {
                                if filter_matches(rule, sub, root) {
                                    next.push(sub.clone());
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        prev_multi = step.is_multi_valued();
        set = next;
    }
    out.extend(set);
}

/// Index into a collected result list (same negative-index rule as arrays).
fn seq_index(set: &[Value], idx: i64) -> Option<&Value> {
    if idx >= 0 {
        set.get(idx as usize)
    } else {
        let i = set.len() as i64 + idx;
        (i >= 0).then(|| set.get(i as usize)).flatten()
    }
}

fn index_of(v: &Value, idx: i64) -> Option<&Value> {
    let arr = v.as_array()?;
    if idx >= 0 {
        arr.get(idx as usize)
    } else {
        let i = arr.len() as i64 + idx;
        (i >= 0).then(|| arr.get(i as usize)).flatten()
    }
}

/// Python slice semantics: negative indices wrap, boundaries clamp, a
/// negative step descends.
fn slice_indices(
    len: usize,
    start: Option<f64>,
    end: Option<f64>,
    step: Option<f64>,
) -> Vec<usize> {
    let step: i64 = step
        .filter(|s| *s != 0.0)
        .map(|s| s.floor() as i64)
        .unwrap_or(1);
    let adjust = |v: f64| -> i64 {
        let i = v.floor() as i64;
        if i < 0 {
            len as i64 + i
        } else {
            i
        }
    };
    let mut out = Vec::new();
    if step > 0 {
        let s = start.map(adjust).unwrap_or(0).clamp(0, len as i64);
        let e = end.map(adjust).unwrap_or(len as i64).clamp(0, len as i64);
        let mut i = s;
        while i < e {
            out.push(i as usize);
            i += step;
        }
    } else {
        let s = start
            .map(adjust)
            .unwrap_or(len as i64 - 1)
            .clamp(0, len as i64 - 1);
        let e = end.map(adjust).unwrap_or(-1).max(-1);
        let mut i = s;
        while i > e {
            out.push(i as usize);
            i += step;
        }
    }
    out
}

// ---- Regex engine (no new dependency) -------------------------------------

/// Node-count cap so a crafted pattern cannot exhaust memory.
const NFA_NODE_CAP: usize = 8_192;
const NFA_PARSE_DEPTH_CAP: usize = 16;
/// `{m,n}` repetition is expanded structurally within this bound.
const NFA_MAX_REPEAT: u32 = 64;

/// Regex match for the JSONPath filter dialect's `/re/` literals.
fn regex_matches(pattern: &str, ignore_case: bool, text: &str) -> bool {
    let (pat, txt): (Vec<char>, Vec<char>) = if ignore_case {
        (
            pattern.to_lowercase().chars().collect(),
            text.to_lowercase().chars().collect(),
        )
    } else {
        (pattern.chars().collect(), text.chars().collect())
    };
    // Unsupported regex constructs read as a non-matching pattern.
    Nfa::compile(&pat).is_some_and(|nfa| nfa.search(&txt))
}

/// One edge predicate in the automaton.
#[derive(Debug, Clone, PartialEq)]
enum Pred {
    Lit(char),
    Any,
    Class {
        neg: bool,
        ranges: Vec<(char, char)>,
    },
}

impl Pred {
    fn matches(&self, c: char) -> bool {
        match self {
            Pred::Lit(p) => *p == c,
            Pred::Any => true,
            Pred::Class { neg, ranges } => ranges.iter().any(|(a, b)| *a <= c && c <= *b) != *neg,
        }
    }
}

/// Zero-width anchors ride the epsilon graph with these guards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Anchor {
    Start,
    End,
}

struct Nfa {
    /// Consuming edges: from-node -> (predicate, to-node).
    steps: Vec<Vec<(Pred, usize)>>,
    /// Epsilon edges: from-node -> (anchor guard, to-node).
    eps: Vec<Vec<(Option<Anchor>, usize)>>,
    start: usize,
    accept: usize,
}

impl Nfa {
    fn new() -> Self {
        Self {
            steps: Vec::new(),
            eps: Vec::new(),
            start: 0,
            accept: 0,
        }
    }

    fn node(&mut self) -> Option<usize> {
        if self.steps.len() >= NFA_NODE_CAP {
            return None;
        }
        self.steps.push(Vec::new());
        self.eps.push(Vec::new());
        Some(self.steps.len() - 1)
    }

    fn step(&mut self, a: usize, pred: Pred, b: usize) {
        self.steps[a].push((pred, b));
    }

    fn eps(&mut self, a: usize, b: usize, guard: Option<Anchor>) {
        self.eps[a].push((guard, b));
    }

    /// Compile a pattern or `None` for unsupported regex syntax.
    fn compile(pat: &[char]) -> Option<Nfa> {
        let mut nfa = Nfa::new();
        let mut cur = Cursor { p: pat, i: 0 };
        let (start, accept) = regex_alts(&mut cur, &mut nfa, 0)?;
        if cur.i != pat.len() {
            return None;
        }
        nfa.start = start;
        nfa.accept = accept;
        Some(nfa)
    }

    /// Left-to-right automaton walk, re-seeded at every offset to implement
    /// unanchored search; `^`/`$` ride the AtStart/AtEnd guards.
    fn search(&self, txt: &[char]) -> bool {
        let len = txt.len();
        let mut cur = vec![false; self.steps.len()];
        let mut pos = 0usize;
        loop {
            cur[self.start] = true;
            self.closure(pos, len, &mut cur);
            if cur[self.accept] {
                return true;
            }
            if pos == len {
                return false;
            }
            let mut next = vec![false; self.steps.len()];
            for (u, active) in cur.iter().enumerate() {
                if !active {
                    continue;
                }
                for (pred, v) in &self.steps[u] {
                    if pred.matches(txt[pos]) {
                        next[*v] = true;
                    }
                }
            }
            cur = next;
            pos += 1;
        }
    }

    fn closure(&self, pos: usize, len: usize, cur: &mut [bool]) {
        let mut stack: Vec<usize> = cur
            .iter()
            .enumerate()
            .filter(|(_, active)| **active)
            .map(|(u, _)| u)
            .collect();
        while let Some(u) = stack.pop() {
            for (guard, v) in self.eps[u].clone() {
                if cur[v] {
                    continue;
                }
                let ok = match guard {
                    None => true,
                    Some(Anchor::Start) => pos == 0,
                    Some(Anchor::End) => pos == len,
                };
                if ok {
                    cur[v] = true;
                    stack.push(v);
                }
            }
        }
    }
}

/// `<alt> ('|' <alt>)*` — the regex grammar root (or a `(...)` group body).
fn regex_alts(cur: &mut Cursor, nfa: &mut Nfa, depth: usize) -> Option<(usize, usize)> {
    if depth >= NFA_PARSE_DEPTH_CAP {
        return None;
    }
    let entry = nfa.node()?;
    let exit = nfa.node()?;
    loop {
        let (a_start, a_end) = regex_seq(cur, nfa, depth)?;
        nfa.eps(entry, a_start, None);
        nfa.eps(a_end, exit, None);
        if !cur.eat('|') {
            break;
        }
    }
    Some((entry, exit))
}

/// A quantified concatenation (stops at `|`, `)` or the pattern end).
fn regex_seq(cur: &mut Cursor, nfa: &mut Nfa, depth: usize) -> Option<(usize, usize)> {
    let mut chain: Option<(usize, usize)> = None;
    loop {
        match cur.peek() {
            None | Some('|') | Some(')') => break,
            _ => {}
        }
        let (a_start, a_end, span_len) = regex_atom(cur, nfa, depth)?;
        let span_start = cur.i - span_len;
        let (a_start, a_end) =
            regex_quantifier(cur, nfa, span_start, span_len, a_start, a_end, depth)?;
        chain = Some(match chain {
            None => (a_start, a_end),
            Some((prev_start, prev_end)) => {
                nfa.eps(prev_end, a_start, None);
                (prev_start, a_end)
            }
        });
    }
    // An empty sequence (`a|`, `()` and friends) matches the empty string.
    match chain {
        Some(c) => Some(c),
        None => {
            let empty = nfa.node()?;
            Some((empty, empty))
        }
    }
}

/// One regex atom (literal, wildcard, class, group, anchor or escape) plus
/// its source-span length — the span is what `{m,n}` copies re-parse.
fn regex_atom(cur: &mut Cursor, nfa: &mut Nfa, depth: usize) -> Option<(usize, usize, usize)> {
    let span_start = cur.i;
    let c = cur.bump()?;
    let (a_start, a_end) = match c {
        '(' => {
            let (s, e) = regex_alts(cur, nfa, depth + 1)?;
            if !cur.eat(')') {
                return None;
            }
            (s, e)
        }
        '[' => regex_class(cur, nfa)?,
        '.' => pred_pair(nfa, Pred::Any)?,
        '^' => {
            let (s, e) = (nfa.node()?, nfa.node()?);
            nfa.eps(s, e, Some(Anchor::Start));
            (s, e)
        }
        '$' => {
            let (s, e) = (nfa.node()?, nfa.node()?);
            nfa.eps(s, e, Some(Anchor::End));
            (s, e)
        }
        '\\' => {
            let esc = cur.bump()?;
            let pred = match escape_pred(esc) {
                // `\b`-style escapes the engine does not support read as
                // unsupported syntax (no match).
                None => Pred::Lit(esc_literal(esc)?),
                Some(pred) => pred,
            };
            pred_pair(nfa, pred)?
        }
        // A dangling quantifier / alternator is unsupported syntax.
        '|' | '*' | '+' | '?' => return None,
        c => pred_pair(nfa, Pred::Lit(c))?,
    };
    Some((a_start, a_end, cur.i - span_start))
}

/// A new pair of nodes joined by one consuming edge; `None` aborts the
/// compile when the node cap is hit (callers treat that as a no-match).
fn pred_pair(nfa: &mut Nfa, pred: Pred) -> Option<(usize, usize)> {
    let s = nfa.node()?;
    let e = nfa.node()?;
    nfa.step(s, pred, e);
    Some((s, e))
}

/// `*`, `+`, `?`, `{m}`, `{m,}`, `{m,n}` — structural NFA rewrites. `{m,n}`
/// re-parses the recorded atom span for every required copy.
fn regex_quantifier(
    cur: &mut Cursor,
    nfa: &mut Nfa,
    span_start: usize,
    span_len: usize,
    a_start: usize,
    a_end: usize,
    depth: usize,
) -> Option<(usize, usize)> {
    let (min, max): (u32, Option<u32>) = match cur.peek() {
        Some('*') => {
            cur.i += 1;
            (0, None)
        }
        Some('+') => {
            cur.i += 1;
            (1, None)
        }
        Some('?') => {
            cur.i += 1;
            (0, Some(1))
        }
        Some('{') => {
            // `{` not followed by a bound is a literal brace in this dialect
            // (jsonpath_ng's backend leaves it to the atom).
            let save = cur.i;
            cur.i += 1;
            let Some(m) = regex_int(cur) else {
                cur.i = save;
                return Some((a_start, a_end));
            };
            if cur.eat('}') {
                if m > NFA_MAX_REPEAT {
                    return None;
                }
                (m, Some(m))
            } else if cur.eat(',') {
                let max = if cur.peek() == Some('}') {
                    None
                } else {
                    let n = regex_int(cur)?;
                    if n > NFA_MAX_REPEAT || m > n {
                        return None;
                    }
                    Some(n)
                };
                if !cur.eat('}') || m > NFA_MAX_REPEAT {
                    return None;
                }
                (m, max)
            } else {
                cur.i = save;
                return Some((a_start, a_end));
            }
        }
        _ => return Some((a_start, a_end)),
    };
    match (min, max) {
        (0, None) => {
            let entry = nfa.node()?;
            let exit = nfa.node()?;
            nfa.eps(entry, a_start, None);
            nfa.eps(a_end, a_start, None);
            nfa.eps(a_end, exit, None);
            nfa.eps(entry, exit, None);
            Some((entry, exit))
        }
        (1, None) => {
            let exit = nfa.node()?;
            nfa.eps(a_end, a_start, None);
            nfa.eps(a_end, exit, None);
            Some((a_start, exit))
        }
        (0, Some(1)) => {
            let exit = nfa.node()?;
            nfa.eps(a_end, exit, None);
            Some((a_start, exit))
        }
        (min, max) => {
            // The already-parsed fragment is required copy 1; further
            // required (then optional) copies are re-parsed from the span.
            let mut cur_end = a_end;
            for _ in 1..min {
                let (s, e) = regex_copied_atom(nfa, cur, span_start, span_len, depth)?;
                nfa.eps(cur_end, s, None);
                cur_end = e;
            }
            match max {
                None => {
                    // {m,}: loop the whole sequence once more than required.
                    let exit = nfa.node()?;
                    nfa.eps(cur_end, exit, None);
                    nfa.eps(cur_end, a_start, None);
                    Some((a_start, exit))
                }
                Some(n) => {
                    // {m,n}: (n - m) optional trailing copies.
                    let exit = nfa.node()?;
                    let mut u = cur_end;
                    nfa.eps(u, exit, None);
                    for _ in min..n {
                        let (s, e) = regex_copied_atom(nfa, cur, span_start, span_len, depth)?;
                        nfa.eps(u, s, None);
                        nfa.eps(e, exit, None);
                        u = e;
                    }
                    Some((a_start, exit))
                }
            }
        }
    }
}

/// Re-parse the recorded atom span into a fresh NFA fragment.
fn regex_copied_atom(
    nfa: &mut Nfa,
    cur: &Cursor,
    span_start: usize,
    span_len: usize,
    depth: usize,
) -> Option<(usize, usize)> {
    let span = cur.p.get(span_start..span_start + span_len)?;
    let mut sub = Cursor { p: span, i: 0 };
    let (s, e, _len) = regex_atom(&mut sub, nfa, depth)?;
    if sub.i != span.len() {
        return None;
    }
    Some((s, e))
}

/// A `[...]` character class (ranges, `^` negation, literal `\` escapes).
fn regex_class(cur: &mut Cursor, nfa: &mut Nfa) -> Option<(usize, usize)> {
    let neg = cur.eat('^');
    let mut ranges: Vec<(char, char)> = Vec::new();
    loop {
        let c = cur.peek()?;
        if c == ']' {
            if ranges.is_empty() {
                return None; // `[]` reads as an unclosed class
            }
            cur.i += 1;
            break;
        }
        cur.i += 1;
        let lo = if c == '\\' {
            let esc = cur.bump()?;
            esc_literal(esc)?
        } else {
            c
        };
        let after_dash = cur.p.get(cur.i + 1).copied();
        if cur.peek() == Some('-') && after_dash.is_some() && after_dash != Some(']') {
            cur.i += 1; // the dash itself belongs to the range
            let hi = match cur.bump()? {
                '\\' => {
                    let esc = cur.bump()?;
                    esc_literal(esc)?
                }
                hi => hi,
            };
            if lo > hi {
                return None;
            }
            ranges.push((lo, hi));
        } else {
            ranges.push((lo, lo));
        }
    }
    let (s, e) = pred_pair(nfa, Pred::Class { neg, ranges })?;
    Some((s, e))
}

fn regex_int(cur: &mut Cursor) -> Option<u32> {
    let mut digits = 0u32;
    let mut have = false;
    while cur.peek().is_some_and(|c| c.is_ascii_digit()) {
        have = true;
        let d = cur.bump()?.to_digit(10)?;
        digits = digits.saturating_mul(10).saturating_add(d);
    }
    have.then_some(digits)
}

/// `\d \w \s` (and negated) character classes for escapes; `None` marks an
/// escape the engine treats as a literal instead.
fn escape_pred(esc: char) -> Option<Pred> {
    match esc {
        'd' => Some(Pred::Class {
            neg: false,
            ranges: vec![('0', '9')],
        }),
        'D' => Some(Pred::Class {
            neg: true,
            ranges: vec![('0', '9')],
        }),
        'w' => Some(Pred::Class {
            neg: false,
            ranges: vec![('0', '9'), ('A', 'Z'), ('_', '_'), ('a', 'z')],
        }),
        'W' => Some(Pred::Class {
            neg: true,
            ranges: vec![('0', '9'), ('A', 'Z'), ('_', '_'), ('a', 'z')],
        }),
        's' => Some(Pred::Class {
            neg: false,
            ranges: vec![(' ', ' '), ('\t', '\r')],
        }),
        'S' => Some(Pred::Class {
            neg: true,
            ranges: vec![(' ', ' '), ('\t', '\r')],
        }),
        _ => None,
    }
}

/// Escapes that reduce to a literal character (`\n`, `\t`, `\.`-style, and
/// punctuation).
fn esc_literal(esc: char) -> Option<char> {
    match esc {
        'n' => Some('\n'),
        't' => Some('\t'),
        'r' => Some('\r'),
        '0' => Some('\0'),
        c if "\\^$.|?*+()[]{}-/".contains(c) => Some(c),
        _ => None,
    }
}

#[cfg(test)]
mod scratch {
    use super::*;
    use serde_json::json;

    #[test]
    fn zdbg() {
        let users = json!({"users": [
            {"name": "a", "n": 5, "role": "x"},
            {"name": "b", "n": 15, "role": "y"},
            {"name": "c", "n": 0, "role": "y"},
        ]});
        println!("lt:     {:?}", resolve("$.users[?(@.name < 'b')]", &users));
        println!("gone:   {:?}", resolve("$.users[?(!@.gone).name]", &users));
        println!(
            "role:   {:?}",
            resolve("$.users[?(@.role == 'y')].name", &users)
        );
        println!(
            "group:  {:?}",
            resolve("$.users[?(@.role == 'y' || !@.gone)]", &users)
        );
        println!(
            "paren:  {:?}",
            resolve("$.users[?(@.role == 'y' || (!@.gone))]", &users)
        );
        println!(
            "combo: {:?}",
            resolve(
                "$.users[?(@.name < 'b' && (@.role == 'y' || !@.gone))].name",
                &users
            )
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn deep_path_within_the_step_cap_resolves_iteratively() {
        let mut value = json!(42);
        for _ in 0..512 {
            value = json!({ "a": value });
        }
        // Exactly at the cap — walks without recursion over the step list.
        let path = format!("${}", ".a".repeat(512));
        assert_eq!(resolve(&path, &value), Some(json!(42)));
    }

    #[test]
    fn paths_beyond_the_step_cap_resolve_as_unresolvable() {
        // Past the cap the path counts as off-dialect syntax: a missing
        // value, never a stack-overflow abort (the 20k-segment case).
        for segs in [513usize, 5_000, 6_000, 20_000] {
            let path = format!("${}", ".a".repeat(segs));
            assert_eq!(resolve(&path, &json!({})), None, "depth {segs}");
        }
    }
}
