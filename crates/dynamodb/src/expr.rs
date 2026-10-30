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
                    } else if c2 == '[' {
                        // Document-path list index: name[0][1] stays one token.
                        s.push(c2);
                        chars.next();
                        let mut digits = String::new();
                        while let Some(&c3) = chars.peek() {
                            if c3.is_ascii_digit() {
                                digits.push(c3);
                                chars.next();
                            } else {
                                break;
                            }
                        }
                        if digits.is_empty() || chars.next() != Some(']') {
                            return Err(format!("invalid document path near '{s}'"));
                        }
                        s.push_str(&digits);
                        s.push(']');
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
/// `#name` placeholders and splitting dotted paths. List indices ([0]) become
/// their own "[N]" segments:
/// `#l[0].a` resolves to ["l-name", "[0]", "a"].
fn resolve_path(token: &str, names: &Names) -> Vec<String> {
    let mut segments = Vec::new();
    for part in token.split('.') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        // Peel the trailing [N] index groups off the head name.
        let mut idx_segs: Vec<String> = Vec::new();
        let mut head = part;
        while let Some(open) = head.rfind('[') {
            if head.ends_with(']') && index_of(&head[open..]).is_some() {
                idx_segs.push(head[open..].to_string());
                head = &head[..open];
            } else {
                break;
            }
        }
        idx_segs.reverse();
        if head.is_empty() {
            // Index-only fragment (cannot happen through lex()); be permissive.
            segments.extend(idx_segs);
            continue;
        }
        if let Some(stripped) = head.strip_prefix('#') {
            let resolved = names
                .get(&format!("#{stripped}"))
                .cloned()
                .unwrap_or_else(|| head.to_string());
            segments.push(resolved);
        } else {
            segments.push(head.to_string());
        }
        segments.extend(idx_segs);
    }
    segments
}

/// Parse a "[N]" segment into its list index. Indices traverse L only.
fn index_of(seg: &str) -> Option<usize> {
    let inner = seg.strip_prefix('[')?;
    let digits = inner.strip_suffix(']')?;
    digits.parse::<usize>().ok()
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

// ---------------------------------------------------------------------------
// Exact decimal-string arithmetic
// ---------------------------------------------------------------------------

/// Strip leading zeros from a digit string ("0" when all zeros).
fn strip_lead_zeros(d: &str) -> &str {
    let t = d.trim_start_matches('0');
    if t.is_empty() {
        "0"
    } else {
        t
    }
}

/// Compare two raw digit strings numerically.
fn cmp_mag(a: &str, b: &str) -> std::cmp::Ordering {
    let (a, b) = (strip_lead_zeros(a), strip_lead_zeros(b));
    match a.len().cmp(&b.len()) {
        std::cmp::Ordering::Equal => a.cmp(b),
        other => other,
    }
}

/// a + b over raw digit strings.
fn add_mag(a: &str, b: &str) -> String {
    let ax: Vec<u8> = a.bytes().rev().collect();
    let bx: Vec<u8> = b.bytes().rev().collect();
    let n = ax.len().max(bx.len());
    let mut out = Vec::with_capacity(n + 1);
    let mut carry = 0u8;
    for i in 0..n {
        let ad = *ax.get(i).unwrap_or(&b'0') - b'0';
        let bd = *bx.get(i).unwrap_or(&b'0') - b'0';
        let s = ad + bd + carry;
        out.push(s % 10 + b'0');
        carry = s / 10;
    }
    if carry > 0 {
        out.push(b'0' + carry);
    }
    let s: String = out.into_iter().rev().map(|c| c as char).collect();
    strip_lead_zeros(&s).to_string()
}

/// a - b over raw digit strings; a must be greater or equal.
fn sub_mag(a: &str, b: &str) -> String {
    let ax: Vec<u8> = a.bytes().rev().collect();
    let bx: Vec<u8> = b.bytes().rev().collect();
    let mut out = Vec::with_capacity(ax.len());
    let mut borrow = 0u8;
    for (i, &byte) in ax.iter().enumerate() {
        let ad = byte - b'0';
        let db = bx.get(i).copied().unwrap_or(b'0') - b'0' + borrow;
        let (d, br) = if ad >= db {
            (ad - db, 0)
        } else {
            (ad + 10 - db, 1)
        };
        out.push(d + b'0');
        borrow = br;
    }
    debug_assert_eq!(borrow, 0);
    let s: String = out.into_iter().rev().map(|c| c as char).collect();
    strip_lead_zeros(&s).to_string()
}

/// Parse a DynamoDB number into (negative, unscaled digits, scale), where
/// scale counts fraction digits after applying an exponent (negative scale
/// shifts become trailing zeros). Accepts up to 38 digits without loss.
fn parse_decimal(s: &str) -> Option<(bool, String, i64)> {
    let t = s.trim();
    let (neg, body) = match t.as_bytes().first()? {
        b'-' => (true, &t[1..]),
        b'+' => (false, &t[1..]),
        _ => (false, t),
    };
    let (mant, exp) = match body.split_once(['e', 'E']) {
        Some((m, e)) => (m, e.parse::<i64>().ok()?),
        None => (body, 0),
    };
    let (int_part, frac_part) = match mant.split_once('.') {
        Some((i, f)) => (i, f),
        None => (mant, ""),
    };
    let all_digits = |x: &str| x.chars().all(|c| c.is_ascii_digit());
    if (int_part.is_empty() && frac_part.is_empty())
        || !all_digits(int_part)
        || !all_digits(frac_part)
    {
        return None;
    }
    let digits = format!("{int_part}{frac_part}");
    let scale = frac_part.len() as i64 - exp;
    // An exponent can scale the digit string enormously ("1e2000000000");
    // cap it so a hostile-but-legal input cannot trigger multi-GB
    // allocations. Digits stay exact within the ±256 shift window; the
    // write path (number_validity) rejects the out-of-band magnitudes
    // before anything reaches the store, so the clamp can only ever see the
    // in-band values moto would accept (moto itself 400s both |x|>9.99…e126
    // and sub-1e-130). Padding beyond the bound falls back to an inexact
    // (f64) value in the callers.
    const MAX_SCALE_SHIFT: i64 = 256;
    if scale >= 0 {
        Some((neg, digits, scale.min(MAX_SCALE_SHIFT)))
    } else {
        Some((
            neg,
            digits + &"0".repeat((-scale).min(MAX_SCALE_SHIFT) as usize),
            0,
        ))
    }
}

/// Render (negative, unscaled digits, scale) as a DynamoDB number string,
/// without leading or trailing zeros ("2.0" renders as "2").
/// Render (negative, unscaled digits, scale) as a DynamoDB number string,
/// without leading or trailing zeros ("2.0" renders as "2").
fn render_decimal(neg: bool, digits: String, scale: usize) -> String {
    let digits = strip_lead_zeros(&digits);
    if digits == "0" {
        return "0".to_string();
    }
    let mut s = if scale == 0 {
        digits.to_string()
    } else if digits.len() <= scale {
        format!("0.{:0>width$}", digits, width = scale)
    } else {
        let split = digits.len() - scale;
        format!("{}.{}", &digits[..split], &digits[split..])
    };
    // Trim trailing fraction zeros ("2.0" -> "2", "0.0000000000000002" kept).
    if let Some(p) = s.find('.') {
        let kept = s[p + 1..].trim_end_matches('0');
        s = if kept.is_empty() {
            s[..p].to_string()
        } else {
            format!("{}.{}", &s[..p], kept)
        };
    }
    if neg && s != "0" {
        s = format!("-{s}");
    }
    s
}

/// Canonical DynamoDB number rendering for storage keys: numeric-equal
/// numbers ("1", "1.0", "1e0") must collapse onto ONE item (real AWS keys
/// numeric-equal values equal; moto's DynamoType equality is Decimal-based),
/// so the canonical form rides the exact decimal renderer. Unparseable
/// inputs stay verbatim.
pub fn canonical_decimal(raw: &str) -> String {
    match parse_decimal(raw) {
        Some((neg, digits, scale)) => render_decimal(neg, digits, scale as usize),
        None => raw.to_string(),
    }
}

/// The numeric-literal verdict for the write path — the three failures moto's
/// `_validate_number_type` (table.py:578-619) raises, each with its own wire
/// message: unparseable / whitespace-padded are "cannot be converted",
/// magnitudes beyond the 1E-130..9.999…E+125 band are Number overflow /
/// underflow.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum NumberValidity {
    Valid,
    NonNumeric,
    Overflow,
    Underflow,
}

pub fn number_validity(raw: &str) -> NumberValidity {
    // moto validates through python's float(): whitespace-padded literals
    // PARSE there, so the explicit whitespace check fires AFTER the parse —
    // " 1", "1 ", "\n1" are non-numeric too ("value != value.strip()"); so
    // is anything float cannot read (empty, inf/nan spellings, junk).
    if raw.trim() != raw || parse_decimal(raw).is_none() {
        return NumberValidity::NonNumeric;
    }
    // The magnitude band rides f64 exactly like moto's float() checks, so
    // edge values land the same verdicts.
    let Ok(v) = raw.parse::<f64>() else {
        return NumberValidity::NonNumeric;
    };
    if !v.is_finite() {
        return NumberValidity::NonNumeric;
    }
    if v != 0.0 {
        if v.abs() > 9.9999999999999999999999999999999999999e125 {
            return NumberValidity::Overflow;
        }
        if v.abs() < 1e-130 {
            return NumberValidity::Underflow;
        }
    }
    NumberValidity::Valid
}

/// Exact add on DynamoDB number strings; None falls back to the caller's f64
/// path (inputs that are not plain decimals, e.g. unresolved values).
fn dec_add(a: &str, b: &str) -> Option<String> {
    dec_compute(a, false, b)
}

/// Exact a - b on DynamoDB number strings.
fn dec_sub(a: &str, b: &str) -> Option<String> {
    dec_compute(a, true, b)
}

/// Common core: align both operands to the max fraction scale, then do
/// signed digit arithmetic.
fn dec_compute(a: &str, flip_b: bool, b: &str) -> Option<String> {
    let (neg_a, d_a, s_a) = parse_decimal(a)?;
    let (mut neg_b, d_b, s_b) = parse_decimal(b)?;
    if flip_b {
        neg_b = !neg_b;
    }
    let scale = s_a.max(s_b).max(0);
    let pad = |d: &str, s: i64| format!("{d}{}", "0".repeat((scale - s).max(0) as usize));
    let m1 = pad(&d_a, s_a);
    let m2 = pad(&d_b, s_b);
    let (neg, digits) = if neg_a == neg_b {
        (neg_a, add_mag(&m1, &m2))
    } else {
        match cmp_mag(&m1, &m2) {
            std::cmp::Ordering::Equal => (false, "0".to_string()),
            std::cmp::Ordering::Greater => (neg_a, sub_mag(&m1, &m2)),
            std::cmp::Ordering::Less => (neg_b, sub_mag(&m2, &m1)),
        }
    };
    Some(render_decimal(neg, digits, scale as usize))
}

/// Exact numeric comparison of two "N" strings; None falls back to f64.
fn dec_cmp(a: &str, b: &str) -> Option<std::cmp::Ordering> {
    let (neg_a, d_a, s_a) = parse_decimal(a)?;
    let (neg_b, d_b, s_b) = parse_decimal(b)?;
    let zero = |d: &str| strip_lead_zeros(d) == "0";
    let scale = s_a.max(s_b).max(0);
    let pad = |d: &str, s: i64| format!("{d}{}", "0".repeat((scale - s).max(0) as usize));
    match (neg_a, neg_b) {
        (false, false) => Some(cmp_mag(&pad(&d_a, s_a), &pad(&d_b, s_b))),
        (true, true) => Some(cmp_mag(&pad(&d_b, s_b), &pad(&d_a, s_a))),
        (false, true) => {
            if zero(&d_a) && zero(&d_b) {
                Some(std::cmp::Ordering::Equal)
            } else {
                Some(std::cmp::Ordering::Greater)
            }
        }
        (true, false) => {
            if zero(&d_a) && zero(&d_b) {
                Some(std::cmp::Ordering::Equal)
            } else {
                Some(std::cmp::Ordering::Less)
            }
        }
    }
}

/// Equality of two attribute values under DynamoDB semantics.
pub fn attr_eq(a: &Value, b: &Value) -> bool {
    match (type_of(a), type_of(b)) {
        (Some("S"), Some("S")) | (Some("B"), Some("B")) | (Some("BOOL"), Some("BOOL")) => a == b,
        (Some("N"), Some("N")) => {
            let (Some(x), Some(y)) = (
                a.get("N").and_then(|v| v.as_str()),
                b.get("N").and_then(|v| v.as_str()),
            ) else {
                return as_number(a) == as_number(b);
            };
            dec_cmp(x, y)
                .map(|o| o.is_eq())
                .unwrap_or_else(|| as_number(a) == as_number(b))
        }
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
        ("N", "N") => {
            let ord = dec_cmp(a.get("N")?.as_str()?, b.get("N")?.as_str()?);
            ord.or_else(|| as_number(a)?.partial_cmp(&as_number(b)?))
        }
        _ => None,
    }
}

/// The size() of an attribute value. B sizes count DECODED bytes;
/// strings count characters (code points), matching the ground truth twin.
fn size_of(val: &Value) -> Option<f64> {
    match type_of(val)? {
        "S" => val
            .as_object()?
            .values()
            .next()
            .and_then(|v| v.as_str())
            .map(|s| s.chars().count() as f64),
        "B" => val
            .as_object()?
            .values()
            .next()
            .and_then(|v| v.as_str())
            .and_then(|s| {
                use base64::Engine;
                base64::engine::general_purpose::STANDARD
                    .decode(s)
                    .ok()
                    .map(|b| b.len() as f64)
            }),
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
/// maps ({"M": {...}}) are unwrapped transparently. "[N]" segments index
/// into L lists.
pub fn lookup<'v>(item: &'v Value, segs: &[String]) -> Option<&'v Value> {
    let mut cur = item;
    for seg in segs {
        cur = descend(cur, seg)?;
    }
    Some(cur)
}

fn descend<'v>(cur: &'v Value, seg: &str) -> Option<&'v Value> {
    let obj = cur.as_object()?;
    if let Some(idx) = index_of(seg) {
        return obj.get("L")?.as_array()?.get(idx);
    }
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
                let av = a.resolve(item)?;
                let bv = b.resolve(item)?;
                num_operation(&av, &bv, false)
            }
            Operand::Minus(a, b) => {
                let av = a.resolve(item)?;
                let bv = b.resolve(item)?;
                num_operation(&av, &bv, true)
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

/// Exact numeric + / - over N values; falls back to f64 for inputs the
/// decimal parser does not recognise (DynamoDB numbers reach 38 digits,
/// beyond f64 precision).
fn num_operation(a: &Value, b: &Value, sub: bool) -> Result<Value, ()> {
    let (Some(x), Some(y)) = (
        a.get("N").and_then(|v| v.as_str()),
        b.get("N").and_then(|v| v.as_str()),
    ) else {
        return Err(());
    };
    let result = if sub { dec_sub(x, y) } else { dec_add(x, y) };
    match result {
        Some(n) => Ok(json!({ "N": n })),
        None => {
            let (av, bv) = (as_number(a).ok_or(())?, as_number(b).ok_or(())?);
            Ok(fmt_num(if sub { av - bv } else { av + bv }))
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
                    let Some(updated) = delete_from_attr(attrs.get_path(&path).cloned(), &val)?
                    else {
                        // Attribute absent: DELETE is a no-op, nothing created.
                        item_view = Value::Object(attrs.clone());
                        continue;
                    };
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

/// Set a possibly-nested attribute path, creating intermediate maps and
/// lists. "[N]" segments index into L values; an index past the end appends
/// (matching the local ground-truth twin's leniency).
pub fn set_path(attrs: &mut Map<String, Value>, segs: &[String], val: Value) -> Result<(), String> {
    let Some((seg, rest)) = segs.split_first() else {
        return Ok(());
    };
    if rest.is_empty() {
        // A path never starts with an index (lex() binds [N] to a name).
        attrs.insert(seg.clone(), val);
        return Ok(());
    }
    // The intermediate wrapper type follows how the path continues.
    let entry = attrs.entry(seg.clone()).or_insert_with(|| {
        if index_of(&rest[0]).is_some() {
            json!({ "L": [] })
        } else {
            json!({ "M": {} })
        }
    });
    set_in_typed(entry, rest, val)
}

/// Continue a set within a typed attribute value: "[N]" indexes its L array,
/// other segments descend through "M".
fn set_in_typed(cur: &mut Value, segs: &[String], val: Value) -> Result<(), String> {
    let Some((seg, rest)) = segs.split_first() else {
        // Ran out of path: the target is this typed value itself.
        *cur = val;
        return Ok(());
    };
    if let Some(idx) = index_of(seg) {
        let arr = cur
            .get_mut("L")
            .and_then(|v| v.as_array_mut())
            .ok_or_else(|| "cannot index into a non-list attribute".to_string())?;
        let at = idx.min(arr.len());
        if rest.is_empty() {
            if at == arr.len() {
                arr.push(val);
            } else {
                arr[at] = val;
            }
            return Ok(());
        }
        if at == arr.len() {
            // Auto-create the container that matches the NEXT path segment
            // (map or list), mirroring the choice at the end of this fn.
            if index_of(&rest[0]).is_some() {
                arr.push(json!({ "L": [] }));
            } else {
                arr.push(json!({ "M": {} }));
            }
        }
        return set_in_typed(&mut arr[at], rest, val);
    }
    let inner = cur
        .get_mut("M")
        .and_then(|m| m.as_object_mut())
        .ok_or_else(|| "cannot descend into non-map attribute".to_string())?;
    if rest.is_empty() {
        inner.insert(seg.clone(), val);
        return Ok(());
    }
    let entry = inner.entry(seg.clone()).or_insert_with(|| {
        if index_of(&rest[0]).is_some() {
            json!({ "L": [] })
        } else {
            json!({ "M": {} })
        }
    });
    set_in_typed(entry, rest, val)
}

/// Remove a possibly-nested attribute path (no-op when missing). Removing a
/// list element with "[N]" in range drops it from the list; out of range it
/// is a no-op.
pub fn remove_path(attrs: &mut Map<String, Value>, segs: &[String]) {
    let Some((last, parents)) = segs.split_last() else {
        return;
    };
    let parent: Option<&mut Value> = if parents.is_empty() {
        // A top-level path cannot start with an index; "last" is a name.
        None
    } else {
        let Some(head) = attrs.get_mut(&parents[0]) else {
            return;
        };
        nested_value_mut(head, &parents[1..])
    };
    let Some(parent) = parent else {
        if let Some(idx) = index_of(last) {
            if let Some(arr) = attrs.get_mut("L").and_then(|v| v.as_array_mut()) {
                if idx < arr.len() {
                    arr.remove(idx);
                }
            }
            return;
        }
        attrs.remove(last);
        return;
    };
    if let Some(idx) = index_of(last) {
        if let Some(arr) = parent.get_mut("L").and_then(|v| v.as_array_mut()) {
            if idx < arr.len() {
                arr.remove(idx);
            }
        }
        return;
    }
    if let Some(inner) = parent.get_mut("M").and_then(|m| m.as_object_mut()) {
        inner.remove(last);
    }
}

/// Recursive mutable lookup inside a typed value ("[N]" through L, names
/// through M). Used to find the parent of a REMOVE path.
fn nested_value_mut<'m>(tv: &'m mut Value, segs: &[String]) -> Option<&'m mut Value> {
    if segs.is_empty() {
        return Some(tv);
    }
    let (seg, rest) = segs.split_first()?;
    let next: Option<&mut Value> = match index_of(seg) {
        Some(idx) => {
            let arr = tv.get_mut("L").and_then(|v| v.as_array_mut())?;
            arr.get_mut(idx)
        }
        None => {
            let inner = tv.get_mut("M").and_then(|m| m.as_object_mut())?;
            inner.get_mut(seg)
        }
    };
    nested_value_mut(next?, rest)
}

/// ADD: numeric addition for N, set union for SS/NS/BS, otherwise copy.
fn add_to_attr(current: Option<Value>, val: &Value) -> Value {
    match (current, type_of(val)) {
        (Some(cur), Some("N")) if type_of(&cur) == Some("N") => {
            let a = cur.get("N").and_then(|v| v.as_str()).unwrap_or("0");
            let b = val.get("N").and_then(|v| v.as_str()).unwrap_or("0");
            match dec_add(a, b) {
                Some(n) => json!({ "N": n }),
                None => fmt_num(a.parse::<f64>().unwrap_or(0.0) + b.parse::<f64>().unwrap_or(0.0)),
            }
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

/// DELETE: set difference for SS/NS/BS. A missing attribute is a no-op
/// (None): nothing is created or reported.
fn delete_from_attr(current: Option<Value>, val: &Value) -> Result<Option<Value>, String> {
    let Some(cur) = current else {
        return Ok(None); // attribute absent — nothing to delete from
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
    Ok(Some(json!({ tag: out })))
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
    fn test_update_delete_missing_attribute_is_noop() {
        let mut attrs: Map<String, Value> = serde_json::from_value(json!({
            "pk": {"S": "x"}, "tags": {"SS": ["a", "b"]}
        }))
        .unwrap();
        let names = HashMap::new();
        let mut values: Values = HashMap::new();
        values.insert(":d".to_string(), json!({"SS": ["a"]}));
        // DELETE on an attribute that never exists must not create it.
        let outcome =
            apply_update(&mut attrs, "DELETE tags :d, ghost :d", &names, &values).unwrap();
        assert!(attrs.get("ghost").is_none());
        assert_eq!(attrs.get("tags").cloned(), Some(json!({"SS": ["b"]})));
        assert_eq!(
            outcome.add_paths.len(),
            1,
            "only the existing attr reported"
        );
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

    #[test]
    fn test_lookup_lookup_and_index_paths() {
        let it = json!({"l": {"L": [{"M": {"a": {"S": "x"}}}, {"M": {"a": {"S": "y"}}}]}});
        assert_eq!(
            lookup(&it, &["l".into(), "[0]".into(), "a".into()]).cloned(),
            Some(json!({"S": "x"}))
        );
        assert_eq!(
            lookup(&it, &["l".into(), "[1]".into()]).cloned(),
            Some(json!({"M": {"a": {"S": "y"}}}))
        );
        // Index into a non-list attribute is unresolvable.
        assert!(lookup(&it, &["l".into(), "a".into(), "[0]".into()]).is_none());
        // ... and so is an index past a list's end.
        assert!(lookup(&it, &["l".into(), "[2]".into()]).is_none());

        // Through the lexer / resolver.
        let mut names = HashMap::new();
        names.insert("#l".to_string(), "l".to_string());
        assert_eq!(
            resolve_path("#l[1].a", &names),
            vec!["l".to_string(), "[1]".to_string(), "a".to_string()]
        );
        assert_eq!(
            resolve_path("mylist[0]", &names),
            vec!["mylist".to_string(), "[0]".to_string()]
        );
    }

    #[test]
    fn test_update_set_list_index() {
        let mut attrs: Map<String, Value> = serde_json::from_value(json!({
            "pk": {"S": "x"},
            "mylist": {"L": [{"S": "a"}, {"S": "b"}]},
            "deep": {"L": [{"M": {"a": {"S": "x"}}}]}
        }))
        .unwrap();
        let names = HashMap::new();
        let mut values: Values = HashMap::new();
        values.insert(":z".to_string(), json!({"S": "z"}));
        // Replace in place.
        apply_update(&mut attrs, "SET mylist[0] = :z", &names, &values).unwrap();
        assert_eq!(
            attrs.get("mylist").cloned(),
            Some(json!({"L": [{"S": "z"}, {"S": "b"}]}))
        );
        // Index past the end appends, like the ground-truth twin.
        apply_update(&mut attrs, "SET mylist[5] = :z", &names, &values).unwrap();
        assert_eq!(
            attrs.get("mylist").cloned(),
            Some(json!({"L": [{"S": "z"}, {"S": "b"}, {"S": "z"}]}))
        );
        // Nested element under an index.
        apply_update(&mut attrs, "SET deep[0].a = :z", &names, &values).unwrap();
        assert_eq!(
            attrs.get("deep").cloned(),
            Some(json!({"L": [{"M": {"a": {"S": "z"}}}]}))
        );
        // Index into a non-list attribute fails cleanly.
        let mut attrs2: Map<String, Value> = serde_json::from_value(json!({
            "pk": {"S": "x"}, "plain": {"S": "s"}
        }))
        .unwrap();
        assert!(apply_update(&mut attrs2, "SET plain[0] = :z", &names, &values).is_err());
    }

    #[test]
    fn test_remove_list_index_shrinks() {
        let mut attrs: Map<String, Value> = serde_json::from_value(json!({
            "pk": {"S": "x"},
            "mylist": {"L": [{"S": "a"}, {"S": "b"}, {"S": "c"}]},
            "deep": {"L": [{"M": {"a": {"S": "x"}}}, {"M": {"a": {"S": "y"}}}]}
        }))
        .unwrap();
        let names = HashMap::new();
        let values: Values = HashMap::new();
        // In range: the element drops out.
        apply_update(&mut attrs, "REMOVE mylist[1], deep[0].a", &names, &values).unwrap();
        assert_eq!(
            attrs.get("mylist").cloned(),
            Some(json!({"L": [{"S": "a"}, {"S": "c"}]}))
        );
        assert_eq!(
            attrs.get("deep").cloned(),
            Some(json!({"L": [{"M": {}}, {"M": {"a": {"S": "y"}}}]}))
        );
        // Out of range: a no-op.
        apply_update(&mut attrs, "REMOVE mylist[9]", &names, &values).unwrap();
        assert_eq!(
            attrs.get("mylist").cloned(),
            Some(json!({"L": [{"S": "a"}, {"S": "c"}]}))
        );
    }

    #[test]
    fn test_projection_with_index_path() {
        let mut names: Names = HashMap::new();
        names.insert("#l".to_string(), "mylist".to_string());
        let paths = parse_projection("#l[1]", &names).unwrap();
        let it = json!({
            "pk": {"S": "a"},
            "mylist": {"L": [{"S": "a"}, {"S": "b"}, {"S": "c"}]}
        });
        let proj = project_item(&it, &paths);
        assert_eq!(
            proj.get("mylist").cloned(),
            Some(json!({"L": [{"S": "b"}]}))
        );
    }

    #[test]
    fn test_condition_on_list_index() {
        let names = HashMap::new();
        let mut values: Values = HashMap::new();
        values.insert(":b".to_string(), json!({"S": "b"}));
        values.insert(":z".to_string(), json!({"S": "z"}));
        let it = json!({"pk": {"S": "a"}, "mylist": {"L": [{"S": "a"}, {"S": "b"}]}});
        assert!(eval_condition("mylist[1] = :b", &it, &names, &values).unwrap());
        assert!(!eval_condition("mylist[0] = :b", &it, &names, &values).unwrap());
        assert!(!eval_condition("mylist[9] = :b", &it, &names, &values).unwrap());
        // attribute_not_exists over an index past the end is true.
        assert!(eval_condition("attribute_not_exists(mylist[9])", &it, &names, &values).unwrap());
    }

    #[test]
    fn test_decimal_add_sub_exact() {
        // Beyond 2^53 precision and up to the 38-digit limit.
        assert_eq!(
            dec_add("9007199254740993", "0").unwrap(),
            "9007199254740993"
        );
        assert_eq!(
            dec_add("999999999999999999999999999999", "1").unwrap(),
            "1000000000000000000000000000000"
        );
        assert_eq!(
            dec_sub("0", "123456789012345678901234567890").unwrap(),
            "-123456789012345678901234567890"
        );
        assert_eq!(
            dec_add("123456789012345678901234567890", "1").unwrap(),
            "123456789012345678901234567891"
        );
        // Fraction alignment, signs and zero normalization.
        assert_eq!(
            dec_sub("1.0000000000000002", "1").unwrap(),
            "0.0000000000000002"
        );
        assert_eq!(dec_add("0.5", "0.5").unwrap(), "1");
        assert_eq!(dec_add("-2", "-3").unwrap(), "-5");
        assert_eq!(dec_add("2", "-5").unwrap(), "-3");
        assert_eq!(dec_sub("5", "2").unwrap(), "3");
        assert_eq!(dec_add("0001", "0001").unwrap(), "2");
        assert_eq!(dec_add("0", "-5").unwrap(), "-5");
        assert_eq!(dec_sub("5", "5").unwrap(), "0");
        assert_eq!(dec_add("1e4", "0.5").unwrap(), "10000.5");
        // Unparseable inputs return None so the caller can fall back.
        assert!(dec_add("abc", "1").is_none());
    }

    #[test]
    fn test_decimal_arithmetic_in_expressions() {
        // SET with big numbers keeps full precision.
        let mut attrs: Map<String, Value> = serde_json::from_value(json!({
            "pk": {"S": "x"}, "big": {"N": "999999999999999999999999999999"}
        }))
        .unwrap();
        let names = HashMap::new();
        let mut values: Values = HashMap::new();
        values.insert(":one".to_string(), json!({"N": "1"}));
        apply_update(&mut attrs, "SET big = big + :one", &names, &values).unwrap();
        assert_eq!(
            attrs.get("big").cloned(),
            Some(json!({"N": "1000000000000000000000000000000"}))
        );
        // ADD too.
        apply_update(&mut attrs, "ADD big :one", &names, &values).unwrap();
        assert_eq!(
            attrs.get("big").cloned(),
            Some(json!({"N": "1000000000000000000000000000001"}))
        );
        // And condition comparisons reject the f64 collision.
        let diff = json!({"N": "123456789012345678901234567891"});
        let base = json!({"N": "123456789012345678901234567890"});
        assert!(!attr_eq(&diff, &base));
        assert!(
            dec_cmp(
                "123456789012345678901234567891",
                "123456789012345678901234567890"
            ) == Some(std::cmp::Ordering::Greater)
        );
    }

    #[test]
    fn test_size_b_counts_decoded_bytes() {
        // base64 "aGVsbG8=" encodes the 5 bytes of "hello"; the encoded
        // string itself is 8 characters.
        let blob = json!({ "B": "aGVsbG8=" });
        assert_eq!(size_of(&blob), Some(5.0));
        // Empty payload.
        assert_eq!(size_of(&json!({ "B": "" })), Some(0.0));
        // L counts elements, M counts entries, SS counts elements.
        assert_eq!(
            size_of(&json!({ "L": [{"S": "a"}, {"S": "b"}] })),
            Some(2.0)
        );
        assert_eq!(
            size_of(&json!({ "M": {"a": {"S": "x"}, "b": {"S": "y"}} })),
            Some(2.0)
        );
        assert_eq!(size_of(&json!({ "SS": ["a", "b", "c"] })), Some(3.0));
        // Strings measure characters, not UTF-8 bytes ("café" is 4 chars).
        assert_eq!(size_of(&json!({ "S": "café" })), Some(4.0));
        // size() resolves through the condition evaluator as well.
        let mut values: Values = HashMap::new();
        values.insert(":five".to_string(), json!({"N": "5"}));
        values.insert(":eight".to_string(), json!({"N": "8"}));
        let it = json!({"pk": {"S": "s"}, "blob": {"B": "aGVsbG8="}});
        let names = HashMap::new();
        assert!(eval_condition("size(blob) = :five", &it, &names, &values).unwrap());
        assert!(!eval_condition("size(blob) = :eight", &it, &names, &values).unwrap());
    }
}
