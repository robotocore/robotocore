//! DynamoDB expression engine.
//!
//! Implements condition / key-condition / filter expression evaluation, update
//! expressions (SET / REMOVE / ADD / DELETE) and projection expressions over
//! items stored as `serde_json::Map<String, Value>` where values are typed
//! attribute values ({ "S": ... }, { "N": ... } etc.).

use serde_json::{json, Map, Value};

pub type Names = std::collections::HashMap<String, String>;
pub type Values = std::collections::HashMap<String, Value>;

// ---------------------------------------------------------------------------
// Lexer
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Path(String),  // `#a.b` placeholder path
    Val(String),   // expression attribute value reference, e.g. `:val`
    Ident(String), // keyword, function name, or bare attribute name
    Op(String),    // = <> <= >= < > + -
    LParen,
    RParen,
    Comma,
}

fn lex(expr: &str) -> Result<Vec<Tok>, String> {
    let mut toks = Vec::new();
    let mut chars = expr.chars().peekable();
    while let Some(&c) = chars.peek() {
        match c {
            ' ' | '\t' | '\n' | '\r' => {
                chars.next();
            }
            '(' => {
                toks.push(Tok::LParen);
                chars.next();
            }
            ')' => {
                toks.push(Tok::RParen);
                chars.next();
            }
            ',' => {
                toks.push(Tok::Comma);
                chars.next();
            }
            ':' => {
                chars.next();
                let mut s = String::from(":");
                while let Some(&c2) = chars.peek() {
                    if c2.is_alphanumeric() || c2 == '_' {
                        s.push(c2);
                        chars.next();
                    } else {
                        break;
                    }
                }
                if s.len() == 1 {
                    return Err("empty expression attribute value reference".to_string());
                }
                toks.push(Tok::Val(s));
            }
            '=' | '<' | '>' | '+' | '-' => {
                let mut s = String::new();
                s.push(c);
                chars.next();
                if (c == '<' && (chars.peek() == Some(&'=') || chars.peek() == Some(&'>')))
                    || ((c == '<' || c == '>') && chars.peek() == Some(&'='))
                {
                    s.push(chars.next().unwrap());
                }
                toks.push(Tok::Op(s));
            }
            _ if c == '#' || c.is_alphanumeric() || c == '_' => {
                let mut s = String::new();
                while let Some(&c2) = chars.peek() {
                    if c2.is_alphanumeric() || c2 == '_' || c2 == '.' || c2 == '#' {
                        s.push(c2);
                        chars.next();
                    } else {
                        break;
                    }
                }
                if s.is_empty() {
                    return Err(format!("unexpected character '{c}' in expression"));
                }
                if s.starts_with('#') {
                    toks.push(Tok::Path(s));
                } else {
                    toks.push(Tok::Ident(s));
                }
            }
            _ => return Err(format!("unexpected character '{c}' in expression")),
        }
    }
    Ok(toks)
}

/// Resolve an expression path token into attribute path segments, substituting
/// `#name` placeholders and splitting dotted paths.
fn resolve_path(token: &str, names: &Names) -> Vec<String> {
    let mut segments = Vec::new();
    for part in token.split('.') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some(stripped) = part.strip_prefix('#') {
            let resolved = names
                .get(&format!("#{stripped}"))
                .cloned()
                .unwrap_or_else(|| part.to_string());
            segments.push(resolved);
        } else {
            segments.push(part.to_string());
        }
    }
    segments
}

// ---------------------------------------------------------------------------
// Type-aware value helpers
// ---------------------------------------------------------------------------

/// The DynamoDB type tag of an attribute value ("S", "N", "B", ...).
pub fn type_of(val: &Value) -> Option<&'static str> {
    let obj = val.as_object()?;
    let (k, _) = obj.into_iter().next()?;
    Some(match k.as_str() {
        "S" => "S",
        "N" => "N",
        "B" => "B",
        "BOOL" => "BOOL",
        "NULL" => "NULL",
        "M" => "M",
        "L" => "L",
        "SS" => "SS",
        "NS" => "NS",
        "BS" => "BS",
        _ => return None,
    })
}

fn as_number(val: &Value) -> Option<f64> {
    let s = val.get("N")?.as_str()?;
    s.parse::<f64>().ok()
}

/// Format a numeric result, preferring integer rendering for whole numbers.
pub fn fmt_num(n: f64) -> Value {
    if n.fract() == 0.0 && n.abs() < 9.0e15 {
        json!({ "N": format!("{}", n as i64) })
    } else {
        json!({ "N": format!("{}", n) })
    }
}

/// Equality of two attribute values under DynamoDB semantics.
pub fn attr_eq(a: &Value, b: &Value) -> bool {
    match (type_of(a), type_of(b)) {
        (Some("S"), Some("S")) | (Some("B"), Some("B")) | (Some("BOOL"), Some("BOOL")) => a == b,
        (Some("N"), Some("N")) => as_number(a) == as_number(b),
        (Some("NULL"), Some("NULL")) => true,
        _ => a == b,
    }
}

/// Ordering comparison for same-typed S / N / B values.
fn attr_cmp(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (type_of(a)?, type_of(b)?) {
        ("S", "S") | ("B", "B") => {
            let av = a
                .as_object()
                .and_then(|o| o.values().next())
                .and_then(|v| v.as_str())?;
            let bv = b
                .as_object()
                .and_then(|o| o.values().next())
                .and_then(|v| v.as_str())?;
            Some(av.cmp(bv))
        }
        ("N", "N") => as_number(a)?.partial_cmp(&as_number(b)?),
        _ => None,
    }
}

/// The size() of an attribute value.
fn size_of(val: &Value) -> Option<f64> {
    match type_of(val)? {
        "S" | "B" => val
            .as_object()?
            .values()
            .next()
            .and_then(|v| v.as_str())
            .map(|s| s.len() as f64),
        "SS" | "NS" | "BS" => val
            .as_object()?
            .values()
            .next()
            .and_then(|v| v.as_array())
            .map(|a| a.len() as f64),
        "L" => val
            .get("L")
            .and_then(|v| v.as_array())
            .map(|a| a.len() as f64),
        "M" => val
            .get("M")
            .and_then(|v| v.as_object())
            .map(|m| m.len() as f64),
        _ => None,
    }
}

/// Look up a possibly-nested attribute path in an item. Intermediate typed
/// maps ({"M": {...}}) are unwrapped transparently.
pub fn lookup<'v>(item: &'v Value, segs: &[String]) -> Option<&'v Value> {
    let mut cur = item;
    for seg in segs {
        cur = descend(cur, seg)?;
    }
    Some(cur)
}

fn descend<'v>(cur: &'v Value, seg: &str) -> Option<&'v Value> {
    let obj = cur.as_object()?;
    if let Some(v) = obj.get(seg) {
        return Some(v);
    }
    // Typed map wrapper: try inside "M".
    if obj.len() == 1 {
        if let Some(inner) = obj.get("M").and_then(|m| m.as_object()) {
            if let Some(v) = inner.get(seg) {
                return Some(v);
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Operands
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum Operand {
    Path(Vec<String>),
    Val(Value),
    Size(Vec<String>),
    Plus(Box<Operand>, Box<Operand>),
    Minus(Box<Operand>, Box<Operand>),
}

impl Operand {
    /// Resolve to a typed attribute value against an item. Err = unresolvable.
    fn resolve(&self, item: &Value) -> Result<Value, ()> {
        match self {
            Operand::Path(segs) => lookup(item, segs).cloned().ok_or(()),
            Operand::Val(v) => Ok(v.clone()),
            Operand::Size(segs) => {
                let v = lookup(item, segs).ok_or(())?;
                size_of(v).map(|n| json!({ "N": n.to_string() })).ok_or(())
            }
            Operand::Plus(a, b) => {
                let av = as_number(&a.resolve(item)?).ok_or(())?;
                let bv = as_number(&b.resolve(item)?).ok_or(())?;
                Ok(fmt_num(av + bv))
            }
            Operand::Minus(a, b) => {
                let av = as_number(&a.resolve(item)?).ok_or(())?;
                let bv = as_number(&b.resolve(item)?).ok_or(())?;
                Ok(fmt_num(av - bv))
            }
        }
    }

    fn resolve_opt(&self, item: &Value) -> Option<Value> {
        self.resolve(item).ok()
    }

    fn path(&self) -> Option<&[String]> {
        match self {
            Operand::Path(segs) => Some(segs),
            Operand::Size(segs) => Some(segs),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Condition parser
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq)]
enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug, Clone)]
enum Cond {
    And(Box<Cond>, Box<Cond>),
    Or(Box<Cond>, Box<Cond>),
    Not(Box<Cond>),
    Cmp(CmpOp, Operand, Operand),
    Between(Operand, Operand, Operand),
    In(Operand, Vec<Operand>),
    AttrExists(Vec<String>, bool),
    BeginsWith(Operand, Operand),
    Contains(Operand, Operand),
    AttrType(Operand, Operand),
}

struct Parser<'a> {
    toks: Vec<Tok>,
    pos: usize,
    names: &'a Names,
    values: &'a Values,
}

impl<'a> Parser<'a> {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos)
    }

    fn next(&mut self) -> Option<Tok> {
        let t = self.toks.get(self.pos).cloned();
        if t.is_some() {
            self.pos += 1;
        }
        t
    }

    /// Consume the next token if it is a specific identifier (case-insensitive).
    fn eat_kw(&mut self, kw: &str) -> bool {
        if let Some(Tok::Ident(s)) = self.peek() {
            if s.eq_ignore_ascii_case(kw) {
                self.pos += 1;
                return true;
            }
        }
        false
    }

    fn parse_operand(&mut self) -> Result<Operand, String> {
        match self.next() {
            Some(Tok::Path(p)) => Ok(Operand::Path(resolve_path(&p, self.names))),
            Some(Tok::Val(v)) => {
                let val = self
                    .values
                    .get(&v)
                    .cloned()
                    .ok_or_else(|| format!("unknown expression attribute value {v}"))?;
                Ok(Operand::Val(val))
            }
            Some(Tok::Ident(f)) => {
                if f.eq_ignore_ascii_case("size") && self.peek() == Some(&Tok::LParen) {
                    self.next(); // (
                    let segs = match self.next() {
                        Some(Tok::Path(p)) => resolve_path(&p, self.names),
                        Some(Tok::Ident(p)) => resolve_path(&p, self.names),
                        _ => return Err("size() requires an attribute path".to_string()),
                    };
                    if self.next() != Some(Tok::RParen) {
                        return Err("expected ) after size(...)".to_string());
                    }
                    return Ok(Operand::Size(segs));
                }
                Ok(Operand::Path(resolve_path(&f, self.names)))
            }
            other => Err(format!("expected operand, found {other:?}")),
        }
    }

    fn parse_primary(&mut self) -> Result<Cond, String> {
        if self.eat_kw("NOT") {
            return Ok(Cond::Not(Box::new(self.parse_primary()?)));
        }
        if let Some(Tok::LParen) = self.peek() {
            self.next();
            let inner = self.parse_or()?;
            if self.next() != Some(Tok::RParen) {
                return Err("expected closing )".to_string());
            }
            return Ok(inner);
        }
        let first = self.parse_operand()?;

        // Function call: fn(paths...) treated as a boolean predicate.
        if let Some(Tok::LParen) = self.peek() {
            self.next();
            return self.parse_function(first);
        }

        if self.eat_kw("BETWEEN") {
            let lo = self.parse_operand()?;
            if !self.eat_kw("AND") {
                return Err("expected AND in BETWEEN".to_string());
            }
            let hi = self.parse_operand()?;
            return Ok(Cond::Between(first, lo, hi));
        }

        if self.eat_kw("IN") {
            if self.next() != Some(Tok::LParen) {
                return Err("expected ( after IN".to_string());
            }
            let mut vals = Vec::new();
            loop {
                vals.push(self.parse_operand()?);
                match self.next() {
                    Some(Tok::Comma) => continue,
                    Some(Tok::RParen) => break,
                    _ => return Err("malformed IN list".to_string()),
                }
            }
            return Ok(Cond::In(first, vals));
        }

        let op = match self.next() {
            Some(Tok::Op(o)) => o,
            other => return Err(format!("expected comparison operator, found {other:?}")),
        };
        let second = self.parse_operand()?;
        let cmp = match op.as_str() {
            "=" => CmpOp::Eq,
            "<>" => CmpOp::Ne,
            "<" => CmpOp::Lt,
            "<=" => CmpOp::Le,
            ">" => CmpOp::Gt,
            ">=" => CmpOp::Ge,
            _ => return Err(format!("unsupported operator {op}")),
        };
        Ok(Cond::Cmp(cmp, first, second))
    }

    /// Parse the remainder of `fnname(args)`. The opening paren is consumed.
    fn parse_function(&mut self, name: Operand) -> Result<Cond, String> {
        let fname = name
            .path()
            .and_then(|p| p.first())
            .cloned()
            .unwrap_or_default()
            .to_lowercase();

        let arg1 = self.parse_operand()?;
        match self.next() {
            Some(Tok::RParen) => match fname.as_str() {
                "attribute_exists" => {
                    Ok(Cond::AttrExists(arg1.path().unwrap_or(&[]).to_vec(), true))
                }
                "attribute_not_exists" => {
                    Ok(Cond::AttrExists(arg1.path().unwrap_or(&[]).to_vec(), false))
                }
                _ => Err(format!("unsupported function {fname}")),
            },
            Some(Tok::Comma) => {
                let arg2 = self.parse_operand()?;
                if self.next() != Some(Tok::RParen) {
                    return Err(format!("expected ) in {fname}"));
                }
                match fname.as_str() {
                    "begins_with" => Ok(Cond::BeginsWith(arg1, arg2)),
                    "contains" => Ok(Cond::Contains(arg1, arg2)),
                    "attribute_type" => Ok(Cond::AttrType(arg1, arg2)),
                    _ => Err(format!("unsupported function {fname}")),
                }
            }
            _ => Err(format!("malformed call to {fname}")),
        }
    }

    fn parse_or(&mut self) -> Result<Cond, String> {
        let mut left = self.parse_and()?;
        while self.eat_kw("OR") {
            let right = self.parse_and()?;
            left = Cond::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Cond, String> {
        let mut left = self.parse_primary()?;
        while self.eat_kw("AND") {
            let right = self.parse_primary()?;
            left = Cond::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }
}

// ---------------------------------------------------------------------------
// Evaluator
// ---------------------------------------------------------------------------

fn eval_cond(cond: &Cond, item: &Value) -> bool {
    match cond {
        Cond::And(a, b) => eval_cond(a, item) && eval_cond(b, item),
        Cond::Or(a, b) => eval_cond(a, item) || eval_cond(b, item),
        Cond::Not(c) => !eval_cond(c, item),
        Cond::Cmp(op, l, r) => {
            let (Some(lv), Some(rv)) = (l.resolve_opt(item), r.resolve_opt(item)) else {
                return false;
            };
            match op {
                CmpOp::Eq => attr_eq(&lv, &rv),
                CmpOp::Ne => !attr_eq(&lv, &rv),
                CmpOp::Lt | CmpOp::Le | CmpOp::Gt | CmpOp::Ge => match attr_cmp(&lv, &rv) {
                    Some(std::cmp::Ordering::Less) => matches!(op, CmpOp::Lt | CmpOp::Le),
                    Some(std::cmp::Ordering::Equal) => matches!(op, CmpOp::Le | CmpOp::Ge),
                    Some(std::cmp::Ordering::Greater) => matches!(op, CmpOp::Gt | CmpOp::Ge),
                    None => false,
                },
            }
        }
        Cond::Between(v, lo, hi) => {
            let (Some(v), Some(lo), Some(hi)) = (
                v.resolve_opt(item),
                lo.resolve_opt(item),
                hi.resolve_opt(item),
            ) else {
                return false;
            };
            match (attr_cmp(&v, &lo), attr_cmp(&v, &hi)) {
                (Some(a), Some(b)) => {
                    !matches!(a, std::cmp::Ordering::Less)
                        && !matches!(b, std::cmp::Ordering::Greater)
                }
                _ => false,
            }
        }
        Cond::In(v, list) => {
            let Some(v) = v.resolve_opt(item) else {
                return false;
            };
            list.iter().any(|o| {
                o.resolve_opt(item)
                    .map(|x| attr_eq(&v, &x))
                    .unwrap_or(false)
            })
        }
        Cond::AttrExists(segs, exists) => {
            let found = lookup(item, segs).is_some();
            if *exists {
                found
            } else {
                !found
            }
        }
        Cond::BeginsWith(l, r) => {
            let (Some(lv), Some(rv)) = (l.resolve_opt(item), r.resolve_opt(item)) else {
                return false;
            };
            match (as_str_val(&lv), as_str_val(&rv)) {
                (Some(a), Some(b)) => a.starts_with(b.as_str()),
                _ => false,
            }
        }
        Cond::Contains(l, r) => {
            let (Some(lv), Some(rv)) = (l.resolve_opt(item), r.resolve_opt(item)) else {
                return false;
            };
            contains_val(&lv, &rv)
        }
        Cond::AttrType(l, r) => {
            let (Some(lv), Some(rv)) = (l.resolve_opt(item), r.resolve_opt(item)) else {
                return false;
            };
            match (type_of(&lv), rv.get("S")) {
                (Some(t), Some(rt)) => t == rt,
                _ => false,
            }
        }
    }
}

fn as_str_val(v: &Value) -> Option<String> {
    v.as_object()
        .and_then(|o| o.values().next())
        .and_then(|x| x.as_str())
        .map(|s| s.to_string())
}

/// Compare a raw set element against an untyped scalar of a given DynamoDB type.
fn scalars_match(set_kind: Option<&str>, raw: &Value, needle_scalar: Option<&Value>) -> bool {
    let Some(needle_scalar) = needle_scalar else {
        return false;
    };
    match (set_kind, needle_scalar) {
        (Some("SS"), Value::String(s)) => raw.as_str() == Some(s.as_str()),
        (Some("NS"), Value::Number(n)) => {
            raw.as_str().and_then(|r| r.parse::<f64>().ok()) == Some(n.as_f64().unwrap_or(f64::NAN))
        }
        (Some("BS"), Value::String(s)) => raw.as_str() == Some(s.as_str()),
        _ => attr_eq_raw(raw, needle_scalar),
    }
}

fn attr_eq_raw(a: &Value, b: &Value) -> bool {
    a == b
}

fn contains_val(container: &Value, needle: &Value) -> bool {
    match type_of(container) {
        Some("S") => as_str_val(container)
            .is_some_and(|s| as_str_val(needle).is_some_and(|n| s.contains(&n))),
        Some("SS") | Some("NS") | Some("BS") => {
            // Set elements are raw scalars; the needle is a typed value.
            let typed = needle.as_object().and_then(|o| o.values().next());
            container
                .as_object()
                .and_then(|o| o.values().next())
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .any(|e| scalars_match(type_of(container), e, typed))
                })
                .unwrap_or(false)
        }
        Some("L") => container
            .get("L")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().any(|e| attr_eq(e, needle)))
            .unwrap_or(false),
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Condition evaluation public API
// ---------------------------------------------------------------------------

/// Parse and evaluate a condition / key-condition / filter expression against
/// an item. Missing operands make comparisons false, per AWS semantics.
pub fn eval_condition(
    expr: &str,
    item: &Value,
    names: &Names,
    values: &Values,
) -> Result<bool, String> {
    let toks = lex(expr)?;
    if toks.is_empty() {
        return Ok(true);
    }
    let mut p = Parser {
        toks,
        pos: 0,
        names,
        values,
    };
    let cond = p.parse_or()?;
    if p.pos != p.toks.len() {
        return Err("unconsumed tokens in condition expression".to_string());
    }
    Ok(eval_cond(&cond, item))
}

// ---------------------------------------------------------------------------
// Update expressions
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum SetAction {
    Val(Operand),
    IfNotExists(Vec<String>, Box<SetAction>),
    ListAppend(Operand, Operand),
}

#[derive(Debug, Clone)]
enum UpdateClause {
    Set(Vec<(Vec<String>, SetAction)>),
    Remove(Vec<Vec<String>>),
    Add(Vec<(Vec<String>, Value)>),
    Delete(Vec<(Vec<String>, Value)>),
}

struct UpdateParser<'a> {
    toks: Vec<Tok>,
    pos: usize,
    names: &'a Names,
    values: &'a Values,
}

impl<'a> UpdateParser<'a> {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos)
    }

    fn next(&mut self) -> Option<Tok> {
        let t = self.toks.get(self.pos).cloned();
        if t.is_some() {
            self.pos += 1;
        }
        t
    }

    fn parse_path(&mut self) -> Result<Vec<String>, String> {
        match self.next() {
            Some(Tok::Path(p)) => Ok(resolve_path(&p, self.names)),
            Some(Tok::Ident(p)) => Ok(resolve_path(&p, self.names)),
            other => Err(format!("expected attribute path, found {other:?}")),
        }
    }

    fn parse_operand(&mut self) -> Result<Operand, String> {
        match self.next() {
            Some(Tok::Path(p)) => Ok(Operand::Path(resolve_path(&p, self.names))),
            Some(Tok::Val(v)) => {
                let val = self
                    .values
                    .get(&v)
                    .cloned()
                    .ok_or_else(|| format!("unknown expression attribute value {v}"))?;
                Ok(Operand::Val(val))
            }
            Some(Tok::Ident(f)) => {
                if f.eq_ignore_ascii_case("size") && self.peek() == Some(&Tok::LParen) {
                    self.next();
                    let segs = match self.next() {
                        Some(Tok::Path(p)) => resolve_path(&p, self.names),
                        Some(Tok::Ident(p)) => resolve_path(&p, self.names),
                        _ => return Err("size() requires an attribute path".to_string()),
                    };
                    if self.next() != Some(Tok::RParen) {
                        return Err("expected ) after size(...)".to_string());
                    }
                    return Ok(Operand::Size(segs));
                }
                Ok(Operand::Path(resolve_path(&f, self.names)))
            }
            other => Err(format!("expected operand, found {other:?}")),
        }
    }

    fn parse_set_rhs(&mut self) -> Result<SetAction, String> {
        // if_not_exists(path, rhs)
        if let Some(Tok::Ident(f)) = self.peek() {
            let name = f.clone();
            if name.eq_ignore_ascii_case("if_not_exists") {
                self.next();
                if self.next() != Some(Tok::LParen) {
                    return Err("expected ( after if_not_exists".to_string());
                }
                let target = match self.next() {
                    Some(Tok::Path(p)) => resolve_path(&p, self.names),
                    Some(Tok::Ident(p)) => resolve_path(&p, self.names),
                    _ => return Err("if_not_exists requires an attribute path".to_string()),
                };
                if self.next() != Some(Tok::Comma) {
                    return Err("expected , in if_not_exists".to_string());
                }
                let rhs = self.parse_set_rhs()?;
                if self.next() != Some(Tok::RParen) {
                    return Err("expected ) after if_not_exists".to_string());
                }
                return Ok(SetAction::IfNotExists(target, Box::new(rhs)));
            }
            if name.eq_ignore_ascii_case("list_append") {
                self.next();
                if self.next() != Some(Tok::LParen) {
                    return Err("expected ( after list_append".to_string());
                }
                let a = self.parse_operand()?;
                if self.next() != Some(Tok::Comma) {
                    return Err("expected , in list_append".to_string());
                }
                let b = self.parse_operand()?;
                if self.next() != Some(Tok::RParen) {
                    return Err("expected ) after list_append".to_string());
                }
                return Ok(SetAction::ListAppend(a, b));
            }
        }

        let lhs = self.parse_operand()?;
        let op_tok = match self.peek() {
            Some(Tok::Op(o)) if o == "+" || o == "-" => Some(o.clone()),
            _ => None,
        };
        if let Some(o) = op_tok {
            self.next();
            let rhs = self.parse_operand()?;
            return Ok(SetAction::Val(if o == "+" {
                Operand::Plus(Box::new(lhs), Box::new(rhs))
            } else {
                Operand::Minus(Box::new(lhs), Box::new(rhs))
            }));
        }
        Ok(SetAction::Val(lhs))
    }

    fn parse_value_operand(&mut self) -> Result<Value, String> {
        match self.next() {
            Some(Tok::Val(v)) => self
                .values
                .get(&v)
                .cloned()
                .ok_or_else(|| format!("unknown expression attribute value {v}")),
            other => Err(format!(
                "expected expression attribute value, found {other:?}"
            )),
        }
    }

    fn parse_clause(&mut self) -> Result<UpdateClause, String> {
        match self.next() {
            Some(Tok::Ident(k)) if k.eq_ignore_ascii_case("SET") => {
                let mut actions = Vec::new();
                loop {
                    let path = self.parse_path()?;
                    match self.next() {
                        Some(Tok::Op(o)) if o == "=" => {}
                        other => return Err(format!("expected = in SET, found {other:?}")),
                    }
                    let rhs = self.parse_set_rhs()?;
                    actions.push((path, rhs));
                    match self.peek() {
                        Some(Tok::Comma) => {
                            self.next();
                        }
                        Some(Tok::Ident(k))
                            if k.eq_ignore_ascii_case("REMOVE")
                                || k.eq_ignore_ascii_case("ADD")
                                || k.eq_ignore_ascii_case("DELETE") =>
                        {
                            break;
                        }
                        None => break,
                        _ => {
                            return Err("expected , or a clause keyword in SET".to_string());
                        }
                    }
                }
                Ok(UpdateClause::Set(actions))
            }
            Some(Tok::Ident(k)) if k.eq_ignore_ascii_case("REMOVE") => {
                let mut paths = Vec::new();
                loop {
                    paths.push(self.parse_path()?);
                    match self.peek() {
                        Some(Tok::Comma) => {
                            self.next();
                        }
                        Some(Tok::Ident(k))
                            if k.eq_ignore_ascii_case("SET")
                                || k.eq_ignore_ascii_case("ADD")
                                || k.eq_ignore_ascii_case("DELETE") =>
                        {
                            break;
                        }
                        None => break,
                        _ => return Err("expected , or a clause keyword in REMOVE".to_string()),
                    }
                }
                Ok(UpdateClause::Remove(paths))
            }
            Some(Tok::Ident(k)) if k.eq_ignore_ascii_case("ADD") => {
                let mut actions = Vec::new();
                loop {
                    let path = self.parse_path()?;
                    let val = self.parse_value_operand()?;
                    actions.push((path, val));
                    match self.peek() {
                        Some(Tok::Comma) => {
                            self.next();
                        }
                        Some(Tok::Ident(k))
                            if k.eq_ignore_ascii_case("SET")
                                || k.eq_ignore_ascii_case("REMOVE")
                                || k.eq_ignore_ascii_case("DELETE") =>
                        {
                            break;
                        }
                        None => break,
                        _ => return Err("expected , or a clause keyword in ADD".to_string()),
                    }
                }
                Ok(UpdateClause::Add(actions))
            }
            Some(Tok::Ident(k)) if k.eq_ignore_ascii_case("DELETE") => {
                let mut actions = Vec::new();
                loop {
                    let path = self.parse_path()?;
                    let val = self.parse_value_operand()?;
                    actions.push((path, val));
                    match self.peek() {
                        Some(Tok::Comma) => {
                            self.next();
                        }
                        Some(Tok::Ident(k))
                            if k.eq_ignore_ascii_case("SET")
                                || k.eq_ignore_ascii_case("REMOVE")
                                || k.eq_ignore_ascii_case("ADD") =>
                        {
                            break;
                        }
                        None => break,
                        _ => return Err("expected , or a clause keyword in DELETE".to_string()),
                    }
                }
                Ok(UpdateClause::Delete(actions))
            }
            other => Err(format!("expected update clause keyword, found {other:?}")),
        }
    }
}

/// Paths touched by SET actions in an update expression (for ReturnValues).
pub struct UpdateOutcome {
    /// Left-hand paths of SET actions and their post-update values.
    pub set_paths: Vec<(Vec<String>, Value)>,
    /// Paths of ADD / DELETE actions and their post-update values.
    pub add_paths: Vec<(Vec<String>, Value)>,
}

/// Apply an update expression to `attrs` in place.
pub fn apply_update(
    attrs: &mut Map<String, Value>,
    expr: &str,
    names: &Names,
    values: &Values,
) -> Result<UpdateOutcome, String> {
    let expr = expr.trim();
    if expr.is_empty() {
        return Ok(UpdateOutcome {
            set_paths: Vec::new(),
            add_paths: Vec::new(),
        });
    }
    let toks = lex(expr)?;
    let mut p = UpdateParser {
        toks,
        pos: 0,
        names,
        values,
    };
    let mut clauses = Vec::new();
    while p.pos < p.toks.len() {
        clauses.push(p.parse_clause()?);
    }

    let mut outcome = UpdateOutcome {
        set_paths: Vec::new(),
        add_paths: Vec::new(),
    };
    // SET actions apply left to right, so each action must see the writes
    // made by the previous ones (SET a = :one, b = a + :two).
    let mut item_view = Value::Object(attrs.clone());

    for clause in clauses {
        match clause {
            UpdateClause::Set(actions) => {
                for (path, act) in actions {
                    let val = eval_set_action(&item_view, &act)?;
                    set_path(attrs, &path, val.clone())?;
                    outcome.set_paths.push((path, val));
                    item_view = Value::Object(attrs.clone());
                }
            }
            UpdateClause::Remove(paths) => {
                for path in paths {
                    remove_path(attrs, &path);
                }
                item_view = Value::Object(attrs.clone());
            }
            UpdateClause::Add(actions) => {
                for (path, val) in actions {
                    let updated = add_to_attr(attrs.get_path(&path).cloned(), &val);
                    set_path(attrs, &path, updated.clone())?;
                    outcome.add_paths.push((path, updated));
                    item_view = Value::Object(attrs.clone());
                }
            }
            UpdateClause::Delete(actions) => {
                for (path, val) in actions {
                    let updated = delete_from_attr(attrs.get_path(&path).cloned(), &val)?;
                    set_path(attrs, &path, updated.clone())?;
                    outcome.add_paths.push((path, updated));
                    item_view = Value::Object(attrs.clone());
                }
            }
        }
    }
    Ok(outcome)
}

fn eval_set_action(item: &Value, a: &SetAction) -> Result<Value, String> {
    match a {
        SetAction::Val(op) => op
            .resolve(item)
            .map_err(|_| "operand could not be resolved".to_string()),
        SetAction::IfNotExists(target, rhs) => {
            if lookup(item, target).is_some() {
                Ok(lookup(item, target).cloned().unwrap_or(Value::Null))
            } else {
                eval_set_action(item, rhs)
            }
        }
        SetAction::ListAppend(l, r) => {
            let lv = l.resolve(item).map_err(|_| "list_append operand missing")?;
            let rv = r.resolve(item).map_err(|_| "list_append operand missing")?;
            let mut out = lv
                .get("L")
                .and_then(|v| v.as_array())
                .cloned()
                .ok_or("list_append requires a list")?;
            let add = rv
                .get("L")
                .and_then(|v| v.as_array())
                .cloned()
                .ok_or("list_append requires a list")?;
            out.extend(add);
            Ok(json!({ "L": out }))
        }
    }
}

/// Get a nested path from a map. Defined as a trait for Map for convenience.
trait MapPathExt {
    fn get_path(&self, segs: &[String]) -> Option<&Value>;
}

impl MapPathExt for Map<String, Value> {
    fn get_path(&self, segs: &[String]) -> Option<&Value> {
        let mut cur = self.get(segs.first()?)?;
        for seg in &segs[1..] {
            cur = descend(cur, seg)?;
        }
        Some(cur)
    }
}

/// Set a possibly-nested attribute path, creating intermediate maps.
pub fn set_path(attrs: &mut Map<String, Value>, segs: &[String], val: Value) -> Result<(), String> {
    let mut cur = attrs;
    for (i, seg) in segs.iter().enumerate() {
        if i == segs.len() - 1 {
            cur.insert(seg.clone(), val);
            return Ok(());
        }
        let entry = cur.entry(seg.clone()).or_insert_with(|| json!({ "M": {} }));
        if !entry.is_object() {
            return Err(format!("cannot descend into non-map attribute {seg}"));
        }
        cur = entry
            .get_mut("M")
            .and_then(|m| m.as_object_mut())
            .ok_or_else(|| format!("cannot descend into non-map attribute {seg}"))?;
    }
    Ok(())
}

/// Remove a possibly-nested attribute path (no-op when missing).
pub fn remove_path(attrs: &mut Map<String, Value>, segs: &[String]) {
    if segs.is_empty() {
        return;
    }
    if segs.len() == 1 {
        attrs.remove(&segs[0]);
        return;
    }
    if let Some(parent) = nested_map_mut(attrs, &segs[..segs.len() - 1]) {
        parent.remove(&segs[segs.len() - 1]);
    }
}

fn nested_map_mut<'m>(
    attrs: &'m mut Map<String, Value>,
    segs: &[String],
) -> Option<&'m mut Map<String, Value>> {
    let mut cur = attrs;
    for seg in segs {
        let entry = cur.get_mut(seg)?;
        cur = entry.get_mut("M").and_then(|m| m.as_object_mut())?;
    }
    Some(cur)
}

/// ADD: numeric addition for N, set union for SS/NS/BS, otherwise copy.
fn add_to_attr(current: Option<Value>, val: &Value) -> Value {
    match (current, type_of(val)) {
        (Some(cur), Some("N")) if type_of(&cur) == Some("N") => {
            let a = cur
                .get("N")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok());
            let b = val
                .get("N")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok());
            fmt_num(a.unwrap_or(0.0) + b.unwrap_or(0.0))
        }
        (Some(cur), Some("SS")) if type_of(&cur) == Some("SS") => merge_sets(&cur, val),
        (Some(cur), Some("NS")) if type_of(&cur) == Some("NS") => merge_sets(&cur, val),
        (Some(cur), Some("BS")) if type_of(&cur) == Some("BS") => merge_sets(&cur, val),
        (Some(cur), Some("L")) if type_of(&cur) == Some("L") => {
            let mut out = cur
                .get("L")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            let add = val
                .get("L")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            out.extend(add);
            json!({ "L": out })
        }
        _ => val.clone(),
    }
}

/// DELETE: set difference for SS/NS/BS.
/// DELETE: set difference for SS/NS/BS. Treats a missing attribute as no-op.
fn delete_from_attr(current: Option<Value>, val: &Value) -> Result<Value, String> {
    let Some(cur) = current else {
        return Ok(val.clone()); // attribute absent — nothing to delete from
    };
    let tag = type_of(&cur).unwrap_or("SS");
    if !matches!(tag, "SS" | "NS" | "BS") {
        return Err("DELETE requires a set attribute".to_string());
    }
    let mut out = cur
        .as_object()
        .and_then(|o| o.values().next())
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    for needle in val
        .as_object()
        .and_then(|o| o.values().next())
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default()
    {
        out.retain(|e| !attr_eq(e, &needle));
    }
    Ok(json!({ tag: out }))
}

fn merge_sets(cur: &Value, val: &Value) -> Value {
    let tag = type_of(cur).unwrap_or("SS");
    let mut out = cur
        .as_object()
        .and_then(|o| o.values().next())
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let add = val
        .as_object()
        .and_then(|o| o.values().next())
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    for e in add {
        if !out.iter().any(|x| attr_eq(x, &e)) {
            out.push(e);
        }
    }
    json!({ tag: out })
}

// ---------------------------------------------------------------------------
// Projection expressions
// ---------------------------------------------------------------------------

/// Parse a projection expression into attribute paths.
pub fn parse_projection(expr: &str, names: &Names) -> Result<Vec<Vec<String>>, String> {
    let expr = expr.trim();
    if expr.is_empty() {
        return Ok(Vec::new());
    }
    let mut paths = Vec::new();
    for part in split_top_level_commas(expr) {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let toks = lex(part)?;
        if toks.len() != 1 {
            return Err("invalid projection attribute".to_string());
        }
        let segs = match &toks[0] {
            Tok::Path(p) => resolve_path(p, names),
            Tok::Ident(p) => resolve_path(p, names),
            _ => return Err("invalid projection attribute".to_string()),
        };
        paths.push(segs);
    }
    Ok(paths)
}

/// Split on top-level commas (no parens expected in projection expressions).
fn split_top_level_commas(expr: &str) -> Vec<&str> {
    expr.split(',').collect()
}

/// Apply projection paths to an item, producing a nested projected map.
pub fn project_item(item: &Value, paths: &[Vec<String>]) -> Map<String, Value> {
    let mut out = Map::new();
    for segs in paths {
        if segs.is_empty() {
            continue;
        }
        if let Some(val) = lookup(item, segs).cloned() {
            let _ = set_path(&mut out, segs, val);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    fn item(v: Value) -> Value {
        v
    }

    #[test]
    fn test_simple_eq() {
        let it = item(json!({"pk": {"S": "a"}, "v": {"N": "5"}}));
        let names = HashMap::new();
        let mut values: Values = HashMap::new();
        values.insert(":v".to_string(), json!({"N": "5"}));
        values.insert(":s".to_string(), json!({"S": "a"}));
        assert!(eval_condition("v = :v", &it, &names, &values).unwrap());
        assert!(eval_condition("pk = :s", &it, &names, &values).unwrap());
        assert!(!eval_condition("v = :s", &it, &names, &values).unwrap());
    }

    #[test]
    fn test_between_and_names() {
        let it = item(json!({"pk": {"S": "u"}, "sk": {"N": "5"}}));
        let mut names = HashMap::new();
        names.insert("#k".to_string(), "sk".to_string());
        let mut values: Values = HashMap::new();
        values.insert(":lo".to_string(), json!({"N": "3"}));
        values.insert(":hi".to_string(), json!({"N": "7"}));
        values.insert(":pk".to_string(), json!({"S": "u"}));
        assert!(
            eval_condition("pk = :pk AND #k BETWEEN :lo AND :hi", &it, &names, &values).unwrap()
        );
        values.insert(":lo".to_string(), json!({"N": "8"}));
        assert!(
            !eval_condition("pk = :pk AND #k BETWEEN :lo AND :hi", &it, &names, &values).unwrap()
        );
    }

    #[test]
    fn test_contains_size_functions() {
        let it = item(json!({"tags": {"SS": ["a", "aws"]}, "data": {"S": "abcdef"}}));
        let names = HashMap::new();
        let mut values: Values = HashMap::new();
        values.insert(":t".to_string(), json!({"S": "aws"}));
        values.insert(":l".to_string(), json!({"N": "3"}));
        assert!(eval_condition("contains(tags, :t)", &it, &names, &values).unwrap());
        assert!(eval_condition("size(data) > :l", &it, &names, &values).unwrap());
    }

    #[test]
    fn test_update_set_nested_and_arith() {
        let mut attrs: Map<String, Value> = serde_json::from_value(json!({
            "pk": {"S": "x"}, "count": {"N": "10"},
            "profile": {"M": {"age": {"N": "30"}}}
        }))
        .unwrap();
        let names = HashMap::new();
        let mut values: Values = HashMap::new();
        values.insert(":age".to_string(), json!({"N": "31"}));
        values.insert(":inc".to_string(), json!({"N": "5"}));
        let outcome = apply_update(
            &mut attrs,
            "SET profile.age = :age, count = count + :inc",
            &names,
            &values,
        )
        .unwrap();
        assert_eq!(
            attrs.get_path(&["profile".into(), "age".into()]).cloned(),
            Some(json!({"N": "31"}))
        );
        assert_eq!(attrs.get("count").cloned(), Some(json!({"N": "15"})));
        assert_eq!(outcome.set_paths.len(), 2);
    }

    #[test]
    fn test_update_sequential_set_sees_prior_writes() {
        // AWS applies SET actions left to right: the second action sees the
        // attribute written by the first.
        let mut attrs: Map<String, Value> = serde_json::from_value(json!({
            "pk": {"S": "x"}
        }))
        .unwrap();
        let names = HashMap::new();
        let mut values: Values = HashMap::new();
        values.insert(":one".to_string(), json!({"N": "1"}));
        values.insert(":two".to_string(), json!({"N": "2"}));
        apply_update(&mut attrs, "SET a = :one, b = a + :two", &names, &values).unwrap();
        assert_eq!(attrs.get("a").cloned(), Some(json!({"N": "1"})));
        assert_eq!(attrs.get("b").cloned(), Some(json!({"N": "3"})));
        // And a chained overwrite too.
        apply_update(&mut attrs, "SET a = a + :two, b = a", &names, &values).unwrap();
        assert_eq!(attrs.get("a").cloned(), Some(json!({"N": "3"})));
        assert_eq!(attrs.get("b").cloned(), Some(json!({"N": "3"})));
    }


    #[test]
    fn test_update_add_delete_sets() {
        let mut attrs: Map<String, Value> = serde_json::from_value(json!({
            "pk": {"S": "x"}, "colors": {"SS": ["red"]}, "tags": {"SS": ["a", "b"]}
        }))
        .unwrap();
        let names = HashMap::new();
        let mut values: Values = HashMap::new();
        values.insert(":s".to_string(), json!({"SS": ["blue", "red"]}));
        values.insert(":d".to_string(), json!({"SS": ["b"]}));
        apply_update(&mut attrs, "ADD colors :s", &names, &values).unwrap();
        apply_update(&mut attrs, "DELETE tags :d", &names, &values).unwrap();
        let colors = attrs.get("colors").cloned().unwrap();
        assert_eq!(
            colors.get("SS").and_then(|v| v.as_array()).map(|a| a.len()),
            Some(2)
        );
        let tags = attrs.get("tags").cloned().unwrap();
        assert_eq!(
            tags.get("SS").and_then(|v| v.as_array()).map(|a| a.len()),
            Some(1)
        );
    }

    #[test]
    fn test_if_not_exists_and_remove() {
        let mut attrs: Map<String, Value> = serde_json::from_value(json!({
            "pk": {"S": "x"}, "existing": {"S": "yes"}, "drop": {"S": "bye"}
        }))
        .unwrap();
        let names = HashMap::new();
        let mut values: Values = HashMap::new();
        values.insert(":def".to_string(), json!({"S": "created"}));
        apply_update(
            &mut attrs,
            "SET existing = if_not_exists(existing, :def), fresh = if_not_exists(fresh, :def) REMOVE drop",
            &names,
            &values,
        )
        .unwrap();
        assert_eq!(attrs.get("existing").cloned(), Some(json!({"S": "yes"})));
        assert_eq!(attrs.get("fresh").cloned(), Some(json!({"S": "created"})));
        assert!(attrs.get("drop").is_none());
    }

    #[test]
    fn test_projection_nested() {
        let names = HashMap::new();
        let paths = parse_projection("profile.address.city", &names).unwrap();
        let it = json!({
            "pk": {"S": "a"},
            "profile": {"M": {"address": {"M": {"city": {"S": "NYC"}, "zip": {"S": "1"}}}}},
            "other": {"S": "x"}
        });
        let proj = project_item(&it, &paths);
        let city = proj
            .get("profile")
            .and_then(|p| p.get("M"))
            .and_then(|m| m.get("address"))
            .and_then(|a| a.get("M"))
            .and_then(|m| m.get("city"))
            .cloned();
        assert_eq!(city, Some(json!({"S": "NYC"})));
        assert!(proj.get("other").is_none());
        let proj_value = Value::Object(proj);
        assert!(lookup(&proj_value, &["profile".into(), "address".into()]).is_some());
    }

    #[test]
    fn test_list_append_update() {
        let mut attrs: Map<String, Value> = serde_json::from_value(json!({
            "pk": {"S": "x"}, "vals": {"L": [{"S": "a"}]}
        }))
        .unwrap();
        let names = HashMap::new();
        let mut values: Values = HashMap::new();
        values.insert(":new".to_string(), json!({"L": [{"S": "b"}, {"S": "c"}]}));
        apply_update(
            &mut attrs,
            "SET vals = list_append(vals, :new)",
            &names,
            &values,
        )
        .unwrap();
        let vals = attrs.get("vals").cloned().unwrap();
        assert_eq!(
            vals.get("L").and_then(|v| v.as_array()).map(|a| a.len()),
            Some(3)
        );
    }
}
