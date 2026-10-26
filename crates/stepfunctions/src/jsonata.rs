//! JSONata 2.0.6 subset evaluator for `QueryLanguage: JSONATA` Step Functions
//! states.
//!
//! AWS documents the behavior at
//! https://docs.aws.amazon.com/step-functions/latest/dg/transforming-data.html:
//! strings wholly wrapped in `{% ... %}` keep their native type, `{% %}`
//! spans embedded inside larger strings interpolate as strings, `$states`
//! carries `input` / `result` / `errorOutput` / `context`, and any evaluation
//! failure surfaces as the runtime error `States.QueryEvaluationError`
//! (assembled by the caller in `asl.rs`).
//!
//! Implemented by hand in Rust (no new crate dependency) with a pragmatic,
//! AWS-conformant subset:
//!
//! - operators: string concat `&`, arithmetic `+ - * / %`, comparisons
//!   `= == != < <= > >= in`, boolean `and` / `or` (and defensive `&&` `||`
//!   `not` aliases), ternary `?:`, default `??`, array/object literals,
//!   positional/boolean predicates (zero offset, like jsonata's
//!   `evaluateFilter`), the `[]` force-sequence step and `a..b` ranges;
//!   JSONata 2.0.6 has no slicing operator (a `[a:b]` predicate parses as a
//!   syntax error there too)
//! - `$states` variable: `.input` (every evaluating field), `.result` and
//!   `.errorOutput` (Task/Parallel/Map outputs, Catch payloads), `.context`
//! - functions (the SFN-eligible subset): `$format`, `$fromMillis`,
//!   `$toMillis`, `$substring`, `$trim`, `$lowercase`, `$uppercase`,
//!   `$length`, `$join`, `$contains`, `$map`, `$filter`, `$flatMap`,
//!   `$exists`, `$type`, `$sum`, `$average`, `$string`, `$boolean`,
//!   `$number`, `$sift`, `$merge`, plus `$parse` (the AWS replacement for
//!   `$eval`) and `$uuid`
//!
//! Deliberately not covered (each reads as an unknown-function or
//! unknown-variable evaluation error): `$match`, `$replace`, `$split`,
//! `$eval`, `$sift` on non-object inputs, `~>` chaining, function sequencing,
//! reducers (`$reduce`), `$sort`-family, `$now` / `$millis` / `$random` /
//! `$uuid` / `$hash`, computed object keys, lambda invocation of held values
//! beyond the HOF passage, and Variables (`Assign`) — `$name` reads as an
//! unknown variable error, mirroring a machine that never declared it.

use crate::jsonpath::number_value;
use chrono::{Datelike, Timelike};
use serde_json::{json, Map, Value};
use std::rc::Rc;

/// Bindings for the reserved `$states.*` variable during one field
/// evaluation. `result` / `error_output` are `None` where AWS makes them
/// unavailable (Parameters of a Task before execution, Choice conditions,
/// Pass payloads), and referencing them errors like the real engine.
pub struct StatesCtx<'a> {
    pub input: &'a Value,
    pub result: Option<&'a Value>,
    pub context: &'a Value,
    pub error_output: Option<&'a Value>,
}

impl<'a> StatesCtx<'a> {
    /// The evaluating state's input only (no result bindings in scope).
    pub fn input_only(input: &'a Value, context: &'a Value) -> Self {
        Self {
            input,
            result: None,
            context,
            error_output: None,
        }
    }
}

/// A runtime evaluation failure: the caller wraps these into
/// `States.QueryEvaluationError` with this message as the cause.
#[derive(Debug, Clone)]
pub struct EvalError {
    pub message: String,
}

impl EvalError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

pub type EvalResult = Result<Value, EvalError>;

/// Whether a string is EXACTLY one `{% ... %}` envelope — the AWS rule: the
/// string must start with `{%` with no leading spaces and end with `%}` with
/// no trailing spaces.
pub fn is_expanded(s: &str) -> bool {
    s.starts_with("{%") && s.ends_with("%}") && s.len() >= 4
}

/// Evaluate one ASL value with embedded `{% %}` JSONata payloads.
///
/// Objects and arrays recurse with literal keys and non-template scalars
/// untouched; a whole-string envelope keeps the native result type while
/// spans embedded inside longer strings stringify (per AWS: "JSONata
/// evaluation happens for all strings whether they occur on their own or
/// inside a JSON object or a JSON array").
pub fn eval_template(value: &Value, states: &StatesCtx) -> Result<Value, EvalError> {
    match value {
        Value::String(s) => {
            if is_expanded(s) {
                let inner = &s[2..s.len() - 2];
                eval_single_expression(inner, states)
            } else if s.contains("{%") {
                eval_embedded(s, states)
            } else {
                Ok(value.clone())
            }
        }
        Value::Object(map) => {
            let mut out = Map::new();
            for (k, v) in map {
                out.insert(k.clone(), eval_template(v, states)?);
            }
            Ok(Value::Object(out))
        }
        Value::Array(items) => {
            let mut out = Vec::new();
            for v in items {
                out.push(eval_template(v, states)?);
            }
            Ok(Value::Array(out))
        }
        _ => Ok(value.clone()),
    }
}

/// Interpolate `{% expr %}` spans inside a larger string; the span's value is
/// stringified (`&`-concatenation rules), and an undefined span part fails
/// the evaluation like a bare undefined result would.
fn eval_embedded(s: &str, states: &StatesCtx) -> Result<Value, EvalError> {
    let mut out = String::new();
    let mut rest = s;
    while let Some(open) = rest.find("{%") {
        let Some(close_rel) = rest[open + 2..].find("%}") else {
            return Err(EvalError::new(
                "an embedded {% %} expression has no closing %}".to_string(),
            ));
        };
        let close = open + 2 + close_rel;
        out.push_str(&rest[..open]);
        let inner = rest[open + 2..close].trim();
        let value = eval_single_expression(inner, states)?;
        out.push_str(&jsonata_string_form(&value));
        rest = rest.get(close + 2..).unwrap_or("");
    }
    out.push_str(rest);
    Ok(Value::String(out))
}

/// The `&`-concatenation string form of a value inside a longer string.
fn jsonata_string_form(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => string_value(other),
    }
}

/// The minimal JSONata `$string()`-style rendering used by `&` and
/// interpolation: whole numbers print without a fraction.
fn string_value(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.to_string()
            } else if let Some(f) = n.as_f64() {
                if f.is_finite() && f.fract() == 0.0 && f.abs() < 1e15 {
                    format!("{}", f as i64)
                } else {
                    format!("{f}")
                }
            } else {
                n.to_string()
            }
        }
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".to_string(),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

// ---- Lexer ----------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    /// Bare identifier or keyword (`and`, `true`, field names...).
    Id(String),
    /// `$states`, `$x`, or a function ref (`$format`); `$` alone is root.
    Var(String),
    Str(String),
    Num(f64),
    Op(Op),
    End,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    LParen,
    RParen,
    LBracket,
    RBracket,
    LBrace,
    RBrace,
    Comma,
    Colon,
    Dot,
    DotDot,
    Question,
    QuestionDefault,
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Amp,
    AndAnd,
    OrOr,
    Eq,
    NotEq,
    Lt,
    Le,
    Gt,
    Ge,
}

fn tokenize(src: &str) -> Option<Vec<Tok>> {
    let chars: Vec<char> = src.chars().collect();
    let mut toks = Vec::new();
    let mut i = 0usize;
    while i < chars.len() {
        let c = chars[i];
        match c {
            c if c.is_whitespace() => {
                i += 1;
            }
            '{' => {
                toks.push(Tok::Op(Op::LBrace));
                i += 1;
            }
            '}' => {
                toks.push(Tok::Op(Op::RBrace));
                i += 1;
            }
            '(' => {
                toks.push(Tok::Op(Op::LParen));
                i += 1;
            }
            ')' => {
                toks.push(Tok::Op(Op::RParen));
                i += 1;
            }
            '[' => {
                toks.push(Tok::Op(Op::LBracket));
                i += 1;
            }
            ']' => {
                toks.push(Tok::Op(Op::RBracket));
                i += 1;
            }
            ',' => {
                toks.push(Tok::Op(Op::Comma));
                i += 1;
            }
            ':' => {
                toks.push(Tok::Op(Op::Colon));
                i += 1;
            }
            '+' => {
                toks.push(Tok::Op(Op::Plus));
                i += 1;
            }
            '-' => {
                toks.push(Tok::Op(Op::Minus));
                i += 1;
            }
            '*' => {
                toks.push(Tok::Op(Op::Star));
                i += 1;
            }
            '/' => {
                toks.push(Tok::Op(Op::Slash));
                i += 1;
            }
            '%' => {
                toks.push(Tok::Op(Op::Percent));
                i += 1;
            }
            // `&&` and `||` read as word-boolean aliases; a single `&` is
            // string concatenation.
            '&' => {
                if chars.get(i + 1) == Some(&'&') {
                    i += 2;
                    toks.push(Tok::Op(Op::AndAnd));
                } else {
                    toks.push(Tok::Op(Op::Amp));
                    i += 1;
                }
            }
            '|' => {
                if chars.get(i + 1) == Some(&'|') {
                    i += 2;
                    toks.push(Tok::Op(Op::OrOr));
                } else {
                    return None;
                }
            }
            '=' => {
                // `==` and `=` both read as equality (JSONata uses `=`).
                if chars.get(i + 1) == Some(&'=') {
                    i += 2;
                } else {
                    i += 1;
                }
                toks.push(Tok::Op(Op::Eq));
            }
            '!' => {
                if chars.get(i + 1) == Some(&'=') {
                    i += 2;
                    toks.push(Tok::Op(Op::NotEq));
                } else {
                    return None; // bare `!` is not a JSONata operator
                }
            }
            '<' if chars.get(i + 1) == Some(&'=') => {
                i += 2;
                toks.push(Tok::Op(Op::Le));
            }
            '>' if chars.get(i + 1) == Some(&'=') => {
                i += 2;
                toks.push(Tok::Op(Op::Ge));
            }
            '<' => {
                toks.push(Tok::Op(Op::Lt));
                i += 1;
            }
            '>' => {
                toks.push(Tok::Op(Op::Gt));
                i += 1;
            }
            '?' => {
                if chars.get(i + 1) == Some(&'?') {
                    i += 2;
                    toks.push(Tok::Op(Op::QuestionDefault));
                } else {
                    toks.push(Tok::Op(Op::Question));
                    i += 1;
                }
            }
            '$' => {
                // Function / variable reference: `$name`, or just `$`.
                let mut name = String::new();
                i += 1;
                while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                    name.push(chars[i]);
                    i += 1;
                }
                toks.push(Tok::Var(if name.is_empty() {
                    "root".to_string()
                } else {
                    name
                }));
            }
            c if c.is_ascii_digit()
                || (c == '.' && chars.get(i + 1).is_some_and(|n| n.is_ascii_digit())) =>
            {
                let mut text = String::new();
                while i < chars.len()
                    && (chars[i].is_ascii_digit()
                        || chars[i] == '.'
                        || matches!(chars[i], 'e' | 'E')
                        || (matches!(chars[i], '+' | '-')
                            && matches!(text.chars().last(), Some('e') | Some('E'))))
                {
                    text.push(chars[i]);
                    i += 1;
                }
                toks.push(Tok::Num(text.parse::<f64>().ok()?));
            }
            c if c.is_alphabetic() || c == '_' => {
                let mut word = String::new();
                while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                    word.push(chars[i]);
                    i += 1;
                }
                toks.push(Tok::Id(word));
            }
            '\'' | '"' => {
                let quote = c;
                i += 1;
                let mut text = String::new();
                loop {
                    let Some(ch) = chars.get(i).copied() else {
                        return None; // 'unterminated string'
                    };
                    i += 1;
                    match ch {
                        c if c == quote => break,
                        '\\' => {
                            let esc = chars.get(i).copied()?;
                            i += 1;
                            match esc {
                                'n' => text.push('\n'),
                                't' => text.push('\t'),
                                'r' => text.push('\r'),
                                'b' => text.push('\u{8}'),
                                'f' => text.push('\u{c}'),
                                'u' => {
                                    let hex: String = chars.get(i..i + 4)?.iter().collect();
                                    i += 4;
                                    text.push(char::from_u32(u32::from_str_radix(&hex, 16).ok()?)?);
                                }
                                other => text.push(other),
                            }
                        }
                        c => text.push(c),
                    }
                }
                toks.push(Tok::Str(text));
            }
            '.' => {
                if chars.get(i + 1) == Some(&'.') {
                    // The `a..b` range operator.
                    i += 2;
                    toks.push(Tok::Op(Op::DotDot));
                } else {
                    toks.push(Tok::Op(Op::Dot));
                    i += 1;
                }
            }
            _ => return None, // unsupported JSONata syntax
        }
    }
    toks.push(Tok::End);
    Some(toks)
}
// ---- AST and parser --------------------------------------------------------

#[derive(Debug, Clone)]
enum VarRef {
    /// Bare `$states`.
    States,
    /// Bare `$` — the current context item (the JSONata root binding).
    Root,
    /// Any other `$name` (lambdas and misdeclared Variables).
    User(String),
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Concat,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
    In,
}

#[derive(Debug, Clone)]
enum Node {
    Lit(Value),
    Var(VarRef),
    Field(Box<Node>, String),
    /// `a.*`.
    Wildcard(Box<Node>),
    /// `a[pred]` — the predicate evaluates per item with the item as
    /// context (jsonata's `evaluateFilter`): a numeric result
    /// position-selects that item (zero offset, negative counts from the
    /// end), anything else filters by imported truthiness.
    Bracket {
        base: Box<Node>,
        pred: Box<Node>,
    },
    /// `a[]` — force the result sequence to array shape (the JSONata
    /// "singleton array and value equivalence" tool).
    ForceSeq(Box<Node>),
    Call(String, Vec<Node>),
    Apply(Box<Node>, Vec<Node>),
    Neg(Box<Node>),
    Not(Box<Node>),
    Bin(BinOp, Box<Node>, Box<Node>),
    /// Integer range `a..b` (inclusive; errors on non-integer bounds).
    Range(Box<Node>, Box<Node>),
    Ternary(Box<Node>, Box<Node>, Option<Box<Node>>),
    Default(Box<Node>, Box<Node>),
    Arr(Vec<Node>),
    Obj(Vec<(String, Node)>),
    Lambda(Vec<String>, Box<Node>),
}

/// Upper bound on parser nesting: every descent into a nested expression
/// (paren group, argument, literal member, predicate bracket, lambda body,
/// ternary branch, unary `not` / `-`) counts against this, so the parser's
/// own recursion can never exhaust the thread stack. 128 levels keeps the
/// worst-case parser stack near 1MB, comfortably inside the 2MiB stacks
/// test and runtime worker threads run on.
const PARSE_DEPTH_CAP: u32 = 128;

struct P {
    toks: Vec<Tok>,
    i: usize,
    /// Current nesting depth: incremented at every descent into a nested
    /// expression, decremented when that sub-parse completes. All recursive
    /// descents go through `enter()`, which refuses past the cap.
    depth: u32,
}

impl P {
    fn peek(&self) -> Tok {
        self.toks.get(self.i).cloned().unwrap_or(Tok::End)
    }

    fn eat_op(&mut self, op: Op) -> bool {
        if self.peek() == Tok::Op(op) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn expect(&mut self, op: Op) -> Result<(), EvalError> {
        if self.eat_op(op) {
            Ok(())
        } else {
            Err(EvalError::new(format!(
                "expected '{}' at argument {}",
                op_name(op),
                self.i + 1
            )))
        }
    }

    fn eat_word(&mut self, word: &str) -> bool {
        if self.peek() == Tok::Id(word.to_string()) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn whole_expr_paren(&mut self) -> Result<Node, EvalError> {
        let inner = self.nested_expr()?;
        self.expect(Op::RParen)?;
        Ok(inner)
    }

    /// One nested expression argument (call arguments, postfix predicate
    /// brackets, object/array members); each is its own descent.
    fn nested_expr(&mut self) -> Result<Node, EvalError> {
        self.enter()?;
        let node = self.ternary();
        self.depth -= 1;
        node
    }

    /// Descend one nesting level, refusing past the cap. The matching pop
    /// happens after the sub-parse; on error the whole parse aborts anyway.
    fn enter(&mut self) -> Result<(), EvalError> {
        self.depth += 1;
        check_depth(self.depth)
    }

    /// `cond ? then : else` — the loosest operator; a missing `:` reads as
    /// the JSONata no-output else branch.
    fn ternary(&mut self) -> Result<Node, EvalError> {
        let cond = self.default()?;
        if self.eat_op(Op::Question) {
            if self.eat_op(Op::Colon) {
                return Ok(Node::Ternary(
                    Box::new(cond),
                    Box::new(Node::Lit(Value::Null)),
                    None,
                ));
            }
            let then = self.nested_expr()?;
            if self.eat_op(Op::Colon) {
                let other = self.nested_expr()?;
                return Ok(Node::Ternary(
                    Box::new(cond),
                    Box::new(then),
                    Some(Box::new(other)),
                ));
            }
            return Err(EvalError::new(
                "expected ':' in a ternary expression".to_string(),
            ));
        }
        Ok(cond)
    }

    /// `a ?? b`.
    fn default(&mut self) -> Result<Node, EvalError> {
        let mut left = self.or_expr()?;
        while self.eat_op(Op::QuestionDefault) {
            let right = self.or_expr()?;
            left = Node::Default(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn or_expr(&mut self) -> Result<Node, EvalError> {
        let mut left = self.and_expr()?;
        loop {
            let is_or = self.eat_word("or") || self.eat_op(Op::OrOr);
            if !is_or {
                return Ok(left);
            }
            let right = self.and_expr()?;
            left = Node::Bin(BinOp::Or, Box::new(left), Box::new(right));
        }
    }

    fn and_expr(&mut self) -> Result<Node, EvalError> {
        let mut left = self.range_expr()?;
        loop {
            let is_and = self.eat_word("and") || self.eat_op(Op::AndAnd);
            if !is_and {
                return Ok(left);
            }
            let right = self.range_expr()?;
            left = Node::Bin(BinOp::And, Box::new(left), Box::new(right));
        }
    }

    /// `a..b` (integer range, inclusive).
    fn range_expr(&mut self) -> Result<Node, EvalError> {
        let mut left = self.not_expr()?;
        while self.peek() == Tok::Op(Op::DotDot) {
            self.i += 1;
            let right = self.not_expr()?;
            left = Node::Range(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn not_expr(&mut self) -> Result<Node, EvalError> {
        if self.eat_word("not") {
            self.enter()?;
            let inner = self.not_expr();
            self.depth -= 1;
            return Ok(Node::Not(Box::new(inner?)));
        }
        self.cmp_expr()
    }

    fn cmp_expr(&mut self) -> Result<Node, EvalError> {
        let mut left = self.concat_expr()?;
        loop {
            let op = match self.peek() {
                Tok::Op(Op::Eq) => BinOp::Eq,
                Tok::Op(Op::NotEq) => BinOp::Ne,
                Tok::Op(Op::Lt) => BinOp::Lt,
                Tok::Op(Op::Le) => BinOp::Le,
                Tok::Op(Op::Gt) => BinOp::Gt,
                Tok::Op(Op::Ge) => BinOp::Ge,
                Tok::Id(w) if w == "in" => BinOp::In,
                _ => break,
            };
            self.i += 1;
            let right = self.concat_expr()?;
            left = Node::Bin(op, Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn concat_expr(&mut self) -> Result<Node, EvalError> {
        let mut left = self.add_expr()?;
        while self.peek() == Tok::Op(Op::Amp) {
            self.i += 1;
            let right = self.add_expr()?;
            left = Node::Bin(BinOp::Concat, Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn add_expr(&mut self) -> Result<Node, EvalError> {
        let mut left = self.mul_expr()?;
        loop {
            let op = match self.peek() {
                Tok::Op(Op::Plus) => BinOp::Add,
                Tok::Op(Op::Minus) => BinOp::Sub,
                _ => break,
            };
            self.i += 1;
            let right = self.mul_expr()?;
            left = Node::Bin(op, Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn mul_expr(&mut self) -> Result<Node, EvalError> {
        let mut left = self.unary()?;
        loop {
            let op = match self.peek() {
                Tok::Op(Op::Star) => BinOp::Mul,
                Tok::Op(Op::Slash) => BinOp::Div,
                Tok::Op(Op::Percent) => BinOp::Mod,
                _ => break,
            };
            self.i += 1;
            let right = self.unary()?;
            left = Node::Bin(op, Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn unary(&mut self) -> Result<Node, EvalError> {
        if self.eat_op(Op::Minus) {
            self.enter()?;
            let inner = self.unary();
            self.depth -= 1;
            return Ok(Node::Neg(Box::new(inner?)));
        }
        let node = self.primary()?;
        self.postfix(node)
    }

    fn postfix(&mut self, mut node: Node) -> Result<Node, EvalError> {
        loop {
            match self.peek() {
                Tok::Op(Op::Dot) => {
                    self.i += 1;
                    if self.peek() == Tok::Op(Op::Star) {
                        self.i += 1;
                        node = Node::Wildcard(Box::new(node));
                    } else {
                        let name = self.dot_name()?;
                        node = Node::Field(Box::new(node), name);
                    }
                }
                Tok::Op(Op::LBracket) => {
                    self.i += 1;
                    if self.peek() == Tok::Op(Op::RBracket) {
                        // `a[]` — the JSONata force-singleton-array step.
                        self.i += 1;
                        node = Node::ForceSeq(Box::new(node));
                        continue;
                    }
                    let first = self.nested_expr()?;
                    if self.peek() == Tok::Op(Op::Colon) {
                        // JSONata 2.0.6 has no slicing operator; a `:` inside
                        // a predicate is a syntax error there too.
                        return Err(EvalError::new(
                            "slicing is not part of the JSONata subset (use $substring and $filter)".to_string(),
                        ));
                    }
                    self.expect(Op::RBracket)?;
                    node = Node::Bracket {
                        base: Box::new(node),
                        pred: Box::new(first),
                    };
                }
                Tok::Op(Op::LParen) => {
                    // Function application: `$f(a, b)` on any expression.
                    self.i += 1;
                    let mut args = Vec::new();
                    if self.peek() != Tok::Op(Op::RParen) {
                        loop {
                            args.push(self.nested_expr()?);
                            if !self.eat_op(Op::Comma) {
                                break;
                            }
                        }
                    }
                    self.expect(Op::RParen)?;
                    node = match node {
                        // A `$name(...)` call with no prior postfix — static
                        // function invocation.
                        Node::Var(VarRef::User(name)) => Node::Call(name, args),
                        other => Node::Apply(Box::new(other), args),
                    };
                }
                _ => return Ok(node),
            }
        }
    }

    /// A field name after `.`: bare identifier or quoted string.
    fn dot_name(&mut self) -> Result<String, EvalError> {
        match self.peek() {
            Tok::Id(name) => {
                self.i += 1;
                Ok(name)
            }
            Tok::Str(name) => {
                self.i += 1;
                Ok(name)
            }
            other => Err(EvalError::new(format!(
                "expected a field name after '.', found {}",
                tok_name(&other)
            ))),
        }
    }

    fn primary(&mut self) -> Result<Node, EvalError> {
        match self.peek() {
            Tok::Op(Op::LParen) => {
                self.i += 1;
                self.whole_expr_paren()
            }
            Tok::Op(Op::LBracket) => {
                self.i += 1;
                let mut items = Vec::new();
                while self.peek() != Tok::Op(Op::RBracket) {
                    items.push(self.nested_expr()?);
                    if !self.eat_op(Op::Comma) {
                        break;
                    }
                }
                self.expect(Op::RBracket)?;
                Ok(Node::Arr(items))
            }
            Tok::Op(Op::LBrace) => {
                self.i += 1;
                let mut pairs = Vec::new();
                while self.peek() != Tok::Op(Op::RBrace) {
                    let key = match self.peek() {
                        Tok::Id(name) => {
                            self.i += 1;
                            name
                        }
                        Tok::Str(name) => {
                            self.i += 1;
                            name
                        }
                        other => {
                            return Err(EvalError::new(format!(
                                "expected a literal object key, found {}",
                                tok_name(&other)
                            )))
                        }
                    };
                    self.expect(Op::Colon)?;
                    // Function-style shorthands like `{a: $x}` take a full
                    // expression after the colon.
                    let value = self.nested_expr()?;
                    pairs.push((key, value));
                    if !self.eat_op(Op::Comma) {
                        break;
                    }
                }
                self.expect(Op::RBrace)?;
                Ok(Node::Obj(pairs))
            }
            Tok::Str(s) => {
                self.i += 1;
                Ok(Node::Lit(Value::String(s)))
            }
            Tok::Num(n) => {
                self.i += 1;
                Ok(Node::Lit(number_value(n)))
            }
            Tok::Var(name) => {
                self.i += 1;
                Ok(Node::Var(var_ref(&name)))
            }
            Tok::Id(word) => {
                self.i += 1;
                match word.as_str() {
                    "true" => Ok(Node::Lit(Value::Bool(true))),
                    "false" => Ok(Node::Lit(Value::Bool(false))),
                    "null" => Ok(Node::Lit(Value::Null)),
                    "function" => self.lambda(),
                    "and" | "or" | "not" | "in" => Err(EvalError::new(format!(
                        "the keyword '{}' cannot start an expression",
                        word
                    ))),
                    _ => Ok(Node::Field(Box::new(Node::Var(VarRef::Root)), word)),
                }
            }
            Tok::End => Err(EvalError::new("unexpected end of expression".to_string())),
            other => Err(EvalError::new(format!(
                "unexpected token {} in the expression",
                tok_name(&other)
            ))),
        }
    }

    /// `function($a, $b) { expr }`.
    fn lambda(&mut self) -> Result<Node, EvalError> {
        // The `function` keyword was consumed by the caller.
        self.expect(Op::LParen)?;
        let mut params = Vec::new();
        if self.peek() != Tok::Op(Op::RParen) {
            loop {
                match self.peek() {
                    Tok::Var(name) => {
                        self.i += 1;
                        params.push(name);
                    }
                    other => {
                        return Err(EvalError::new(format!(
                            "lambda parameters must be $-prefixed variables, found {}",
                            tok_name(&other)
                        )))
                    }
                }
                if !self.eat_op(Op::Comma) {
                    break;
                }
            }
        }
        self.expect(Op::RParen)?;
        self.expect(Op::LBrace)?;
        let body = self.nested_expr()?;
        self.expect(Op::RBrace)?;
        Ok(Node::Lambda(params, Box::new(body)))
    }
}

fn op_name(op: Op) -> &'static str {
    match op {
        Op::LParen => "(",
        Op::RParen => ")",
        Op::LBracket => "[",
        Op::RBracket => "]",
        Op::LBrace => "{",
        Op::RBrace => "}",
        Op::Comma => ",",
        Op::Colon => ":",
        Op::Dot => ".",
        Op::DotDot => "..",
        Op::Question => "?",
        Op::QuestionDefault => "??",
        Op::Plus => "+",
        Op::Minus => "-",
        Op::Star => "*",
        Op::Slash => "/",
        Op::Percent => "%",
        Op::Amp => "&",
        Op::AndAnd => "&&",
        Op::OrOr => "||",
        Op::Eq => "=",
        Op::NotEq => "!=",
        Op::Lt => "<",
        Op::Le => "<=",
        Op::Gt => ">",
        Op::Ge => ">=",
    }
}

/// Parse one expression (entry point: the lowest-precedence level).
fn parse(src: &str) -> Result<Node, EvalError> {
    let Some(toks) = tokenize(src) else {
        return Err(EvalError::new(format!(
            "The expression is not valid JSONata syntax: {src}"
        )));
    };
    if matches!(toks.first(), Some(Tok::End)) {
        return Err(EvalError::new("The expression is empty".to_string()));
    }
    let mut p = P {
        toks,
        i: 0,
        depth: 0,
    };
    let node = p.ternary()?;
    if !matches!(p.peek(), Tok::End) {
        return Err(EvalError::new(format!(
            "Unexpected token at position {}: the expression is not valid JSONata syntax",
            p.i
        )));
    }
    Ok(node)
}

fn tok_name(tok: &Tok) -> &'static str {
    match tok {
        Tok::Id(_) => "an identifier",
        Tok::Var(_) => "a $variable",
        Tok::Str(_) => "a string literal",
        Tok::Num(_) => "a number literal",
        Tok::Op(op) => op_name(*op),
        Tok::End => "the end of the expression",
    }
}

fn var_ref(name: &str) -> VarRef {
    match name {
        "root" => VarRef::Root,
        "states" => VarRef::States,
        other => VarRef::User(other.to_string()),
    }
}

fn check_depth(self_depth: u32) -> Result<(), EvalError> {
    if self_depth >= PARSE_DEPTH_CAP {
        Err(EvalError::new(
            "The expression is too deeply nested".to_string(),
        ))
    } else {
        Ok(())
    }
}

// ---- Evaluator -------------------------------------------------------------

const EVAL_DEPTH_CAP: u32 = 128;

/// Internal evaluation values: JSON data, the JSONata "no value" (undefined)
/// and lambdas in flight.
#[derive(Debug, Clone)]
enum Jv {
    Val(Value),
    Missing,
    Func(Vec<String>, Rc<Node>, Rc<Scope>),
}

/// Variable scope chain; `root` mirrors the JSONata implicit context.
#[derive(Debug, Clone)]
struct Scope {
    vars: Vec<(String, Jv)>,
    parent: Option<Rc<Scope>>,
}

impl Scope {
    fn lookup(&self, name: &str) -> Option<Jv> {
        let mut node: Option<&Scope> = Some(self);
        while let Some(s) = node {
            if let Some((_, v)) = s.vars.iter().rev().find(|(k, _)| k == name) {
                return Some(v.clone());
            }
            node = s.parent.as_deref();
        }
        None
    }
}

fn base_scope(states: &StatesCtx) -> Rc<Scope> {
    Rc::new(Scope {
        vars: vec![("root".to_string(), Jv::Val(states.input.clone()))],
        parent: None,
    })
}

fn child_scope(scope: &Rc<Scope>, entry: (String, Jv)) -> Rc<Scope> {
    Rc::new(Scope {
        vars: vec![entry],
        parent: Some(scope.clone()),
    })
}

/// Evaluate one whole `{% ... %}` expression; undefined results error.
pub fn eval(src: &str, states: &StatesCtx) -> Result<Value, EvalError> {
    let node = parse(src)?;
    let scope = base_scope(states);
    match eval_node(&node, &scope, states, 0)? {
        Jv::Val(v) => Ok(v),
        Jv::Missing => Err(EvalError::new(format!(
            "The expression '{expr}' evaluated to no value, which cannot be represented in JSON",
            expr = src.trim()
        ))),
        Jv::Func { .. } => Err(EvalError::new(
            "The expression evaluated to a function, which cannot be a value".to_string(),
        )),
    }
}

/// Same as `eval`, but `&`-stringified for embedded spans.
pub fn eval_single_expression(src: &str, states: &StatesCtx) -> Result<Value, EvalError> {
    eval(src, states)
}

fn eval_var(r: &VarRef, scope: &Rc<Scope>, states: &StatesCtx) -> Result<Jv, EvalError> {
    Ok(match r {
        VarRef::States => Jv::Val(states_value(states)),
        VarRef::Root => scope
            .lookup("root")
            .unwrap_or(Jv::Val(Value::Object(Map::new()))),
        VarRef::User(name) => scope.lookup(name).ok_or_else(|| {
            EvalError::new(format!(
                "The variable '${name}' is not defined (declaring Variables is outside this evaluator's supported subset)"
            ))
        })?,
    })
}

/// The whole `$states` object with only the currently available parts.
fn states_value(states: &StatesCtx) -> Value {
    let mut m = Map::new();
    m.insert("input".to_string(), states.input.clone());
    m.insert("context".to_string(), states.context.clone());
    if let Some(r) = states.result {
        m.insert("result".to_string(), r.clone());
    }
    if let Some(e) = states.error_output {
        m.insert("errorOutput".to_string(), e.clone());
    }
    Value::Object(m)
}

fn eval_node(
    node: &Node,
    scope: &Rc<Scope>,
    states: &StatesCtx,
    depth: u32,
) -> Result<Jv, EvalError> {
    if depth > EVAL_DEPTH_CAP {
        return Err(EvalError::new(
            "The expression evaluation exceeded the recursion limit".to_string(),
        ));
    }
    match node {
        Node::Lit(v) => Ok(Jv::Val(v.clone())),
        Node::Var(r) => eval_var(r, scope, states),
        Node::Field(base, name) => {
            // `$states.*` needs the binding-dependent errors, not a generic
            // missing field.
            if matches!(**base, Node::Var(VarRef::States)) {
                return Ok(match name.as_str() {
                    "input" => Jv::Val(states.input.clone()),
                    "context" => Jv::Val(states.context.clone()),
                    "result" => match states.result {
                        Some(v) => Jv::Val(v.clone()),
                        None => {
                            return Err(EvalError::new(
                                "$states.result is not available in this field (it is only bound in the Output of Task, Parallel and Map states)".to_string(),
                            ))
                        }
                    },
                    "errorOutput" => match states.error_output {
                        Some(v) => Jv::Val(v.clone()),
                        None => {
                            return Err(EvalError::new(
                                "$states.errorOutput is only available inside a Catch block".to_string(),
                            ))
                        }
                    },
                    _ => Jv::Missing,
                });
            }
            let b = eval_node(base, scope, states, depth + 1)?;
            field_of(&b, name, depth)
        }
        Node::Wildcard(base) => {
            let b = eval_node(base, scope, states, depth + 1)?;
            wildcard_of(&b, depth)
        }
        Node::ForceSeq(base) => {
            let b = eval_node(base, scope, states, depth + 1)?;
            Ok(match b {
                Jv::Val(Value::Array(items)) => Jv::Val(Value::Array(items)),
                Jv::Val(v) => Jv::Val(Value::Array(vec![v])),
                Jv::Missing => Jv::Val(Value::Array(Vec::new())),
                Jv::Func { .. } => {
                    return Err(EvalError::new(
                        "a function cannot be forced to array shape".to_string(),
                    ))
                }
            })
        }
        Node::Bracket { base, pred } => {
            let b = eval_node(base, scope, states, depth + 1)?;
            eval_bracket(b, pred, scope, states, depth)
        }
        Node::Call(name, args) => eval_call(name, args, scope, states, depth),
        Node::Apply(fexpr, args) => {
            let f = eval_node(fexpr, scope, states, depth + 1)?;
            let mut vals = Vec::new();
            for a in args {
                vals.push(eval_node(a, scope, states, depth + 1)?);
            }
            apply_func(&f, &vals, states, depth)
        }
        Node::Neg(x) => {
            let v = require_number(&eval_node(x, scope, states, depth + 1)?, "-")?;
            Ok(Jv::Val(number_value(-v)))
        }
        Node::Not(x) => {
            let v = eval_node(x, scope, states, depth + 1)?;
            Ok(Jv::Val(Value::Bool(!jv_truthy(&v))))
        }
        Node::Bin(op, l, r) => eval_bin(*op, l, r, scope, states, depth),
        Node::Range(l, r) => {
            let a = require_number(&eval_node(l, scope, states, depth + 1)?, "..")? as i64;
            let b = require_number(&eval_node(r, scope, states, depth + 1)?, "..")? as i64;
            if b < a {
                return Err(EvalError::new(
                    "The JSONata range operator requires an incrementing range".to_string(),
                ));
            }
            let items: Vec<Value> = (a..=b).map(|i| number_value(i as f64)).collect();
            Ok(Jv::Val(Value::Array(items)))
        }
        Node::Ternary(c, t, other) => {
            let cond = eval_node(c, scope, states, depth + 1)?;
            if jv_truthy(&cond) {
                eval_node(t, scope, states, depth + 1)
            } else if let Some(e) = other {
                eval_node(e, scope, states, depth + 1)
            } else {
                Ok(Jv::Missing)
            }
        }
        Node::Default(l, r) => {
            let left = eval_node(l, scope, states, depth + 1)?;
            match left {
                Jv::Missing => eval_node(r, scope, states, depth + 1),
                v => Ok(v),
            }
        }
        Node::Arr(items) => {
            let mut out = Vec::new();
            for item in items {
                match eval_node(item, scope, states, depth + 1)? {
                    Jv::Val(v) => out.push(v),
                    // Undefined array members are dropped (JSONata
                    // flattening).
                    Jv::Missing => {}
                    Jv::Func { .. } => {
                        return Err(EvalError::new(
                            "a function cannot be an array element".to_string(),
                        ))
                    }
                }
            }
            Ok(Jv::Val(Value::Array(out)))
        }
        Node::Obj(pairs) => {
            let mut out = Map::new();
            for (k, vexpr) in pairs {
                match eval_node(vexpr, scope, states, depth + 1)? {
                    Jv::Val(v) => {
                        out.insert(k.clone(), v);
                    }
                    // Undefined object members drop their pair (JSONata).
                    Jv::Missing => {}
                    Jv::Func { .. } => {
                        return Err(EvalError::new(
                            "a function cannot be an object value".to_string(),
                        ))
                    }
                }
            }
            Ok(Jv::Val(Value::Object(out)))
        }
        Node::Lambda(params, body) => {
            let body: Rc<Node> = Rc::new(body.as_ref().clone());
            Ok(Jv::Func(params.clone(), body, scope.clone()))
        }
    }
}

fn field_of(base: &Jv, name: &str, depth: u32) -> Result<Jv, EvalError> {
    let _ = depth;
    match base {
        Jv::Missing => Ok(Jv::Missing),
        Jv::Func { .. } => Err(EvalError::new(
            "a function cannot be used in a field reference".to_string(),
        )),
        Jv::Val(Value::Object(map)) => Ok(map
            .get(name)
            .map(|v| Jv::Val(v.clone()))
            .unwrap_or(Jv::Missing)),
        // Array of objects: project the field per element (JSONata map), an
        // empty result reading as undefined.
        Jv::Val(Value::Array(items)) => {
            let mut out = Vec::new();
            for item in items {
                match field_of(&Jv::Val(item.clone()), name, depth)? {
                    Jv::Val(v) => out.push(v),
                    Jv::Missing => {}
                    Jv::Func { .. } => {
                        return Err(EvalError::new(
                            "a function cannot be used in a field reference".to_string(),
                        ))
                    }
                }
            }
            if out.is_empty() {
                Ok(Jv::Missing)
            } else {
                Ok(Jv::Val(Value::Array(out)))
            }
        }
        Jv::Val(_) => Ok(Jv::Missing),
    }
}

fn wildcard_of(base: &Jv, depth: u32) -> Result<Jv, EvalError> {
    let _ = depth;
    match base {
        Jv::Val(Value::Object(map)) => Ok(Jv::Val(Value::Array(map.values().cloned().collect()))),
        Jv::Missing => Ok(Jv::Missing),
        Jv::Func { .. } => Err(EvalError::new(
            "a function cannot be used with the * operator".to_string(),
        )),
        Jv::Val(_) => Ok(Jv::Missing),
    }
}

/// jsonata's `evaluateFilter`: the predicate evaluates per item with the
/// item as context. A numeric result selects THIS item when its
/// floor-rounded value equals the item's position in the sequence (zero
/// offset, negative counts from the end), an all-numeric array selects any
/// listed position, and any other result includes the item by truthiness —
/// mixed numeric and boolean predicates mix both effects. Afterwards a
/// one-element result reads as that element (jsonata's singleton
/// equivalence; `[]` on the step forces the array shape back).
fn eval_bracket(
    base: Jv,
    pred: &Node,
    scope: &Rc<Scope>,
    states: &StatesCtx,
    depth: u32,
) -> Result<Jv, EvalError> {
    let items: Vec<Value> = match &base {
        Jv::Val(Value::Array(list)) => list.clone(),
        Jv::Val(Value::Object(map)) => map.values().cloned().collect(),
        Jv::Missing => return Ok(Jv::Missing),
        Jv::Func { .. } => {
            return Err(EvalError::new("a function cannot be filtered".to_string()));
        }
        Jv::Val(_) => return Ok(Jv::Missing),
    };
    let mut out = Vec::new();
    for (idx, item) in items.iter().enumerate() {
        let frame = child_scope(scope, ("root".to_string(), Jv::Val(item.clone())));
        let res = eval_node(pred, &frame, states, depth + 1)?;
        match res {
            Jv::Val(Value::Number(n)) => {
                if index_hits(n.as_f64().unwrap_or(0.0), idx, items.len()) {
                    out.push(item.clone());
                }
            }
            Jv::Val(Value::Array(nums)) if nums.iter().all(|n| n.is_number()) => {
                for n in nums {
                    if index_hits(n.as_f64().unwrap_or(0.0), idx, items.len()) {
                        out.push(item.clone());
                        break;
                    }
                }
            }
            Jv::Missing => {}
            other => {
                if jv_truthy(&other) {
                    out.push(item.clone());
                }
            }
        }
    }
    if out.is_empty() {
        return Ok(Jv::Missing);
    }
    if out.len() == 1 {
        return Ok(Jv::Val(out.remove(0)));
    }
    Ok(Jv::Val(Value::Array(out)))
}

/// The `evaluateFilter` position test for one candidate item: a numeric
/// predicate rounds down (negative counts from the end) and selects the
/// item at that offset in the sequence.
fn index_hits(n: f64, idx: usize, len: usize) -> bool {
    let target = n.floor() as i64;
    let pos = if target < 0 {
        len as i64 + target
    } else {
        target
    };
    pos == idx as i64
}

fn jv_truthy(v: &Jv) -> bool {
    match v {
        Jv::Missing => false,
        Jv::Func { .. } => true,
        Jv::Val(val) => val_truthy(val),
    }
}

fn val_truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0 && !f.is_nan()).unwrap_or(false),
        Value::String(s) => !s.is_empty(),
        Value::Array(items) => items.iter().any(val_truthy),
        Value::Object(_) => true,
    }
}

fn require_number(jv: &Jv, op: &str) -> Result<f64, EvalError> {
    match jv {
        Jv::Val(Value::Number(n)) => Ok(n.as_f64().unwrap_or(f64::NAN)),
        _ => Err(EvalError::new(format!(
            "The operator '{op}' requires numeric operands"
        ))),
    }
}

fn eval_bin(
    op: BinOp,
    l: &Node,
    r: &Node,
    scope: &Rc<Scope>,
    states: &StatesCtx,
    depth: u32,
) -> Result<Jv, EvalError> {
    // Boolean operators short-circuit; the rest need both operands.
    match op {
        BinOp::And => {
            let a = eval_node(l, scope, states, depth + 1)?;
            if !jv_truthy(&a) {
                return Ok(Jv::Val(Value::Bool(false)));
            }
            let b = eval_node(r, scope, states, depth + 1)?;
            return Ok(Jv::Val(Value::Bool(jv_truthy(&b))));
        }
        BinOp::Or => {
            let a = eval_node(l, scope, states, depth + 1)?;
            if jv_truthy(&a) {
                return Ok(Jv::Val(Value::Bool(true)));
            }
            let b = eval_node(r, scope, states, depth + 1)?;
            return Ok(Jv::Val(Value::Bool(jv_truthy(&b))));
        }
        _ => {}
    }
    let a = eval_node(l, scope, states, depth + 1)?;
    let b = eval_node(r, scope, states, depth + 1)?;
    match op {
        BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod => {
            let x = require_number(&a, op_name_of(op))?;
            let y = require_number(&b, op_name_of(op))?;
            let v = match op {
                BinOp::Add => x + y,
                BinOp::Sub => x - y,
                BinOp::Mul => x * y,
                BinOp::Div => {
                    if y == 0.0 {
                        return Err(EvalError::new(
                            "The JSONata division operator cannot divide by zero".to_string(),
                        ));
                    }
                    x / y
                }
                _ => {
                    if y == 0.0 {
                        return Err(EvalError::new(
                            "The JSONata % operator cannot divide by zero".to_string(),
                        ));
                    }
                    x % y
                }
            };
            if !v.is_finite() {
                return Err(EvalError::new(
                    "The arithmetic operation produced a non-finite value".to_string(),
                ));
            }
            Ok(Jv::Val(number_value(v)))
        }
        BinOp::Concat => {
            let x = concat_form(&a)?;
            let y = concat_form(&b)?;
            Ok(Jv::Val(Value::String(format!("{x}{y}"))))
        }
        BinOp::Eq => Ok(Jv::Val(Value::Bool(jv_equal(&a, &b)))),
        BinOp::Ne => Ok(Jv::Val(Value::Bool(!jv_equal(&a, &b)))),
        BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge => {
            Ok(Jv::Val(Value::Bool(jv_order(&a, op, &b))))
        }
        BinOp::In => {
            let needle = concat_or_value(&a)?;
            match &b {
                Jv::Val(Value::Array(items)) => {
                    Ok(Jv::Val(Value::Bool(items.iter().any(|v| v == &needle))))
                }
                Jv::Missing => Ok(Jv::Val(Value::Bool(false))),
                _ => Err(EvalError::new(
                    "The right operand of 'in' must be an array".to_string(),
                )),
            }
        }
        // Handled above.
        BinOp::And | BinOp::Or => unreachable!(),
    }
}

fn op_name_of(op: BinOp) -> &'static str {
    match op {
        BinOp::Add => "+",
        BinOp::Sub => "-",
        BinOp::Mul => "*",
        BinOp::Div => "/",
        BinOp::Mod => "%",
        BinOp::Concat => "&",
        BinOp::Eq => "=",
        BinOp::Ne => "!=",
        BinOp::Lt => "<",
        BinOp::Le => "<=",
        BinOp::Gt => ">",
        BinOp::Ge => ">=",
        BinOp::And => "and",
        BinOp::Or => "or",
        BinOp::In => "in",
    }
}

fn concat_form(jv: &Jv) -> Result<String, EvalError> {
    match jv {
        Jv::Missing => Err(EvalError::new(
            "The & operator requires values (an operand evaluated to nothing)".to_string(),
        )),
        Jv::Func { .. } => Err(EvalError::new(
            "a function cannot take part in string concatenation".to_string(),
        )),
        Jv::Val(v) => match v {
            Value::String(s) => Ok(s.clone()),
            Value::Null => Ok(String::new()),
            other => Ok(string_value(other)),
        },
    }
}

fn concat_or_value(jv: &Jv) -> Result<Value, EvalError> {
    match jv {
        Jv::Val(v) => Ok(v.clone()),
        Jv::Missing | Jv::Func { .. } => Err(EvalError::new(
            "The 'in' operator requires comparable operands".to_string(),
        )),
    }
}

/// Deep equality with JSONata's numeric tolerance (`1` == `1.0`) and no
/// cross-type coercion (`true` != `1`, `"1"` != `1`).
fn jv_equal(a: &Jv, b: &Jv) -> bool {
    match (a, b) {
        (Jv::Val(x), Jv::Val(y)) => {
            match (x.as_f64(), x.is_boolean(), y.as_f64(), y.is_boolean()) {
                (Some(x), false, Some(y), false) => x == y,
                _ => x == y,
            }
        }
        _ => false,
    }
}

/// Ordering comparisons on same-typed operands (numbers, then strings by
/// codepoint for names and timestamps); differing types are not ordered.
fn jv_order(a: &Jv, op: BinOp, b: &Jv) -> bool {
    let (Jv::Val(x), Jv::Val(y)) = (a, b) else {
        return false;
    };
    let ord = if x.is_number() && !x.is_boolean() && y.is_number() && !y.is_boolean() {
        x.as_f64()
            .unwrap_or(f64::NAN)
            .partial_cmp(&y.as_f64().unwrap_or(f64::NAN))
    } else {
        match (x.as_str(), y.as_str()) {
            (Some(x), Some(y)) => x.partial_cmp(y),
            _ => None,
        }
    };
    matches!(
        (ord, op),
        (Some(std::cmp::Ordering::Less), BinOp::Lt)
            | (
                Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal),
                BinOp::Le
            )
            | (Some(std::cmp::Ordering::Greater), BinOp::Gt)
            | (
                Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal),
                BinOp::Ge
            )
    )
}

// ---- Built-in functions ----------------------------------------------------

fn eval_call(
    name: &str,
    args: &[Node],
    scope: &Rc<Scope>,
    states: &StatesCtx,
    depth: u32,
) -> Result<Jv, EvalError> {
    // Function references the evaluator does not have fall back to a value
    // bound in scope (a lambda variable), so `$f(x)` still works when `$f`
    // holds a lambda.
    let builtin = matches!(
        name,
        "format"
            | "fromMillis"
            | "toMillis"
            | "substring"
            | "trim"
            | "lowercase"
            | "uppercase"
            | "length"
            | "join"
            | "contains"
            | "map"
            | "filter"
            | "flatMap"
            | "exists"
            | "type"
            | "sum"
            | "average"
            | "string"
            | "boolean"
            | "number"
            | "sift"
            | "merge"
            | "parse"
            | "uuid"
    );
    if builtin {
        let mut vals = Vec::new();
        for a in args {
            vals.push(eval_node(a, scope, states, depth + 1)?);
        }
        return call_builtin(name, &vals, scope, states, depth);
    }
    // Function calls with a lambda flow as `$f(...)`.
    if let Some(f) = scope.lookup(name) {
        return apply_func(
            &f,
            borrow_arg_values(args, scope, states, depth)?.as_slice(),
            states,
            depth,
        );
    }
    Err(EvalError::new(format!(
        "Unknown JSONata function '${name}'"
    )))
}

fn borrow_arg_values(
    args: &[Node],
    scope: &Rc<Scope>,
    states: &StatesCtx,
    depth: u32,
) -> Result<Vec<Jv>, EvalError> {
    let mut vals = Vec::new();
    for a in args {
        vals.push(eval_node(a, scope, states, depth + 1)?);
    }
    Ok(vals)
}

fn apply_func(f: &Jv, args: &[Jv], states: &StatesCtx, depth: u32) -> Result<Jv, EvalError> {
    let (params, body, closure) = match f {
        Jv::Func(params, body, closure) => (params.clone(), body.clone(), closure.clone()),
        _ => {
            return Err(EvalError::new(
                "The expression cannot be invoked as a function".to_string(),
            ))
        }
    };
    if depth + 1 > EVAL_DEPTH_CAP {
        return Err(EvalError::new(
            "The expression evaluation exceeded the recursion limit".to_string(),
        ));
    }
    let frame_vars: Vec<(String, Jv)> = params
        .iter()
        .enumerate()
        .map(|(i, p)| (p.clone(), args.get(i).cloned().unwrap_or(Jv::Missing)))
        .collect();
    let frame = Rc::new(Scope {
        vars: frame_vars,
        parent: Some(closure),
    });
    eval_node(&body, &frame, states, depth + 1)
}

fn call_builtin(
    name: &str,
    vals: &[Jv],
    _scope: &Rc<Scope>,
    _states: &StatesCtx,
    _depth: u32,
) -> Result<Jv, EvalError> {
    match name {
        "exists" => {
            let Some(arg) = vals.first() else {
                return Err(EvalError::new("$exists requires one argument".to_string()));
            };
            Ok(Jv::Val(Value::Bool(!matches!(arg, Jv::Missing))))
        }
        "type" => {
            let Some(arg) = vals.first() else {
                return Err(EvalError::new("$type requires one argument".to_string()));
            };
            match arg {
                Jv::Func { .. } => Ok(Jv::Val(Value::String("function".to_string()))),
                Jv::Missing => Ok(Jv::Missing),
                Jv::Val(v) => Ok(Jv::Val(Value::String(type_of(v).to_string()))),
            }
        }
        "format" => {
            let template = jv_arg(vals.first(), "$format", 1, "a template string")?;
            let template = string_of(template, "$format")?;
            let args: Vec<Value> = vals
                .iter()
                .skip(1)
                .map(|v| match v {
                    Jv::Val(v) => Ok(v.clone()),
                    Jv::Missing => Ok(Value::Null),
                    Jv::Func { .. } => Err(EvalError::new(
                        "$format arguments cannot be functions".to_string(),
                    )),
                })
                .collect::<Result<Vec<Value>, EvalError>>()?;
            Ok(Jv::Val(Value::String(render_format(&template, &args)?)))
        }
        "substring" => {
            let s = string_of(
                jv_arg(vals.first(), "$substring", 1, "a string")?,
                "$substring",
            )?;
            let start = number_of(
                jv_arg(vals.get(1), "$substring", 2, "a number")?,
                "$substring",
            )?;
            let chars: Vec<char> = s.chars().collect();
            let len = chars.len() as i64;
            let mut start = start.floor() as i64;
            if start < 0 {
                // Counted back from the end, clamped to the start.
                start = std::cmp::max(len + start, 0);
            } else {
                start = std::cmp::min(len, start);
            }
            let end = match vals.get(2) {
                None => Some(len),
                Some(v) => {
                    let length = number_of(jv_value(v)?, "$substring")? as i64;
                    if length < 0 {
                        // A negative length counts back from the end.
                        Some(len + length - start)
                    } else {
                        Some(start + length)
                    }
                }
            };
            let end = end.unwrap_or(len).clamp(0, len);
            let start = start.clamp(0, end);
            Ok(Jv::Val(Value::String(
                chars[start as usize..end as usize].iter().collect(),
            )))
        }
        "trim" => {
            let s = string_of(jv_arg(vals.first(), "$trim", 1, "a string")?, "$trim")?;
            Ok(Jv::Val(Value::String(
                s.split_whitespace().collect::<Vec<&str>>().join(" "),
            )))
        }
        "lowercase" => {
            let s = string_of(
                jv_arg(vals.first(), "$lowercase", 1, "a string")?,
                "$lowercase",
            )?;
            Ok(Jv::Val(Value::String(s.to_lowercase())))
        }
        "uppercase" => {
            let s = string_of(
                jv_arg(vals.first(), "$uppercase", 1, "a string")?,
                "$uppercase",
            )?;
            Ok(Jv::Val(Value::String(s.to_uppercase())))
        }
        "length" => {
            let s = string_of(jv_arg(vals.first(), "$length", 1, "a string")?, "$length")?;
            Ok(Jv::Val(number_value(s.chars().count() as f64)))
        }
        "join" => {
            let items = array_of(jv_arg(vals.first(), "$join", 1, "an array")?, "$join")?;
            let sep = match vals.get(1) {
                None => String::new(),
                Some(v) => string_of(jv_value(v)?, "$join")?,
            };
            let mut out = String::new();
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push_str(&sep);
                }
                match jv_scalar(item) {
                    Some(s) => out.push_str(&s),
                    None => {
                        return Err(EvalError::new(
                            "The array passed to $join must contain only strings and numbers"
                                .to_string(),
                        ))
                    }
                }
            }
            Ok(Jv::Val(Value::String(out)))
        }
        "contains" => {
            let a = jv_arg(vals.first(), "$contains", 1, "a string")?;
            let b = jv_arg(vals.get(1), "$contains", 2, "a substring")?;
            match (a, b) {
                (Value::String(a), Value::String(b)) => {
                    Ok(Jv::Val(Value::Bool(a.contains(b.as_str()))))
                }
                _ => Err(EvalError::new(
                    "$contains requires two string arguments".to_string(),
                )),
            }
        }
        "map" | "flatMap" => {
            let items = array_of(jv_arg(vals.first(), name_of(name, 1), 1, "an array")?, name)?;
            let (params, body, closure) = hof_arg(vals.get(1), name)?;
            let func = Jv::Func(params.clone(), body.clone(), closure.clone());
            let mut out = Vec::new();
            for (i, item) in items.iter().enumerate() {
                // The second argument (index) is 1-based in JSONata.
                let result = apply_func(
                    &func,
                    &[Jv::Val(item.clone()), Jv::Val(number_value((i + 1) as f64))],
                    _states,
                    _depth,
                )?;
                if let Jv::Val(val) = result {
                    if val.is_array() {
                        // Collected sequences append flattened one level.
                        out.extend(val.as_array().expect("checked").iter().cloned());
                    } else {
                        out.push(val);
                    }
                }
            }
            Ok(Jv::Val(Value::Array(out)))
        }
        "filter" => {
            let items = array_of(jv_arg(vals.first(), "$filter", 1, "an array")?, "$filter")?;
            let (params, body, closure) = hof_arg(vals.get(1), "$filter")?;
            let func = Jv::Func(params.clone(), body.clone(), closure.clone());
            let mut out = Vec::new();
            for item in &items {
                let result = apply_func(&func, &[Jv::Val(item.clone())], _states, _depth)?;
                if jv_truthy(&result) {
                    out.push(item.clone());
                }
            }
            Ok(Jv::Val(Value::Array(out)))
        }
        "sift" => {
            let obj = match vals.first() {
                Some(arg) => match jv_value(arg)? {
                    Value::Object(o) => o,
                    _ => {
                        return Err(EvalError::new(
                            "$sift requires an object as its first argument".to_string(),
                        ))
                    }
                },
                None => {
                    return Err(EvalError::new(
                        "$sift requires an object as its first argument".to_string(),
                    ))
                }
            };
            let (params, body, closure) = hof_arg(vals.get(1), "$sift")?;
            let mut out = Map::new();
            for (k, v) in &obj {
                let mut args = vec![Jv::Val(v.clone())];
                if params.len() > 1 {
                    args.push(Jv::Val(Value::String(k.clone())));
                }
                let result = apply_func(
                    &Jv::Func(params.clone(), body.clone(), closure.clone()),
                    &args,
                    _states,
                    _depth,
                )?;
                if jv_truthy(&result) {
                    out.insert(k.clone(), v.clone());
                }
            }
            Ok(Jv::Val(Value::Object(out)))
        }
        "merge" => {
            let items = array_of(jv_arg(vals.first(), "$merge", 1, "an array")?, "$merge")?;
            let mut out = Map::new();
            for item in items {
                match item {
                    Value::Object(o) => {
                        for (k, v) in o {
                            out.insert(k, v);
                        }
                    }
                    _ => {
                        return Err(EvalError::new(
                            "$merge requires an array of objects".to_string(),
                        ))
                    }
                }
            }
            Ok(Jv::Val(Value::Object(out)))
        }
        "sum" => {
            let items = array_of(jv_arg(vals.first(), "$sum", 1, "an array")?, "$sum")?;
            let mut sum = 0.0f64;
            for item in &items {
                match item.as_f64() {
                    Some(n) if !item.is_boolean() => sum += n,
                    _ => {
                        return Err(EvalError::new(
                            "$sum requires an array of numbers".to_string(),
                        ))
                    }
                }
            }
            Ok(Jv::Val(number_value(sum)))
        }
        "average" => {
            let Some(arg) = vals.first() else {
                return Err(EvalError::new("$average requires one argument".to_string()));
            };
            if matches!(arg, Jv::Missing) {
                return Ok(Jv::Missing);
            }
            let items = array_of(jv_value(arg)?, "$average")?;
            if items.is_empty() {
                return Ok(Jv::Val(Value::Null));
            }
            let mut sum = 0.0f64;
            for item in &items {
                match item.as_f64() {
                    Some(n) if !item.is_boolean() => sum += n,
                    _ => {
                        return Err(EvalError::new(
                            "$average requires an array of numbers".to_string(),
                        ))
                    }
                }
            }
            Ok(Jv::Val(number_value(sum / items.len() as f64)))
        }
        "string" => {
            let v = jv_arg(vals.first(), "$string", 1, "a value")?;
            let pretty = match vals.get(1) {
                Some(v) => val_truthy(&jv_value(v)?),
                None => false,
            };
            match &v {
                Value::Array(_) | Value::Object(_) => {
                    let text = if pretty {
                        serde_json::to_string_pretty(&v)
                    } else {
                        serde_json::to_string(&v)
                    };
                    Ok(Jv::Val(Value::String(
                        text.unwrap_or_else(|_| "null".to_string()),
                    )))
                }
                other => Ok(Jv::Val(Value::String(string_value(other)))),
            }
        }
        "boolean" => {
            let v = jv_arg(vals.first(), "$boolean", 1, "a value")?;
            Ok(Jv::Val(Value::Bool(val_truthy(&v))))
        }
        "number" => {
            let v = jv_arg(vals.first(), "$number", 1, "a value")?;
            let out = match v {
                Value::Number(_) => v.clone(),
                Value::Bool(b) => {
                    if b {
                        json!(1.0)
                    } else {
                        json!(0.0)
                    }
                }
                Value::String(s) => {
                    let parsed = s.parse::<f64>().ok();
                    match parsed {
                        Some(n) if n.is_finite() => number_value(n),
                        _ => {
                            return Err(EvalError::new(
                                "The argument of $number does not parse to a number".to_string(),
                            ))
                        }
                    }
                }
                Value::Null => Value::Null,
                _ => {
                    return Err(EvalError::new(
                        "$number requires a number, string or null".to_string(),
                    ))
                }
            };
            Ok(Jv::Val(out))
        }
        "parse" => {
            let s = string_of(jv_arg(vals.first(), "$parse", 1, "a string")?, "$parse")?;
            match serde_json::from_str::<Value>(&s) {
                Ok(v) => Ok(Jv::Val(v)),
                Err(e) => Err(EvalError::new(format!(
                    "The $parse argument is not valid JSON: {e}"
                ))),
            }
        }
        "uuid" => Ok(Jv::Val(Value::String(uuid::Uuid::new_v4().to_string()))),
        "fromMillis" => {
            let millis = number_of(
                jv_arg(vals.first(), "$fromMillis", 1, "a number")?,
                "$fromMillis",
            )?
            .floor() as i64;
            let format = match vals.get(1) {
                None => None,
                Some(v) => match jv_value(v)? {
                    Value::Null => None,
                    Value::String(s) => Some(s.clone()),
                    _ => {
                        return Err(EvalError::new(
                            "$fromMillis requires a string format picture".to_string(),
                        ))
                    }
                },
            };
            Ok(Jv::Val(millis_to_iso(millis, format.as_deref())?))
        }
        "toMillis" => {
            let iso = string_of(
                jv_arg(vals.first(), "$toMillis", 1, "a timestamp string")?,
                "$toMillis",
            )?;
            if vals.get(1).is_some() {
                return Err(EvalError::new(
                    "$toMillis supports only ISO 8601 timestamps, not format pictures".to_string(),
                ));
            }
            parse_iso_millis(&iso)
        }
        _ => Err(EvalError::new(format!(
            "Unknown JSONata function '${name}'"
        ))),
    }
}

/// One bound function argument as a JSON value or a tailored error.
fn jv_arg(arg: Option<&Jv>, fname: &str, pos: usize, what: &str) -> Result<Value, EvalError> {
    let Some(arg) = arg else {
        return Err(EvalError::new(format!(
            "${fname} requires argument {pos} ({what})"
        )));
    };
    jv_value(arg)
}

fn jv_value(jv: &Jv) -> Result<Value, EvalError> {
    match jv {
        Jv::Val(v) => Ok(v.clone()),
        Jv::Missing => Err(EvalError::new(
            "The function was invoked with an argument that evaluated to no value".to_string(),
        )),
        Jv::Func { .. } => Err(EvalError::new(
            "a function cannot be used as a value".to_string(),
        )),
    }
}

fn string_of(v: Value, fname: &str) -> Result<String, EvalError> {
    v.as_str()
        .map(|s| s.to_string())
        .ok_or_else(|| EvalError::new(format!("${fname} requires string arguments")))
}

fn number_of(v: Value, fname: &str) -> Result<f64, EvalError> {
    v.as_f64()
        .ok_or_else(|| EvalError::new(format!("${fname} requires numeric arguments")))
}

fn array_of(v: Value, fname: &str) -> Result<Vec<Value>, EvalError> {
    v.as_array()
        .cloned()
        .ok_or_else(|| EvalError::new(format!("${fname} requires an array argument")))
}

fn jv_scalar(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(_) => Some(string_value(v)),
        _ => None,
    }
}

fn name_of(name: &str, _pos: usize) -> &'static str {
    match name {
        "map" => "$map",
        "flatMap" => "$flatMap",
        "filter" => "$filter",
        "sift" => "$sift",
        _ => "$fn",
    }
}

/// A lambda binding: parameter names, body AST, and captured scope.
type FuncParts<'a> = (&'a Vec<String>, &'a Rc<Node>, &'a Rc<Scope>);

fn func_parts(f: &Jv) -> Result<FuncParts<'_>, EvalError> {
    match f {
        Jv::Func(params, body, closure) => Ok((params, body, closure)),
        _ => Err(EvalError::new(
            "The function's second argument must be a lambda function".to_string(),
        )),
    }
}

/// A lambda captured and rebound per higher-order call site.
type BoundLambda = (Vec<String>, Rc<Node>, Rc<Scope>);

fn hof_arg(f: Option<&Jv>, fname: &str) -> Result<BoundLambda, EvalError> {
    let f = f.ok_or_else(|| {
        EvalError::new(format!(
            "${fname} requires a lambda function as its second argument"
        ))
    })?;
    let (params, body, closure) = func_parts(f)?;
    Ok((params.clone(), body.clone(), closure.clone()))
}

fn render_format(template: &str, args: &[Value]) -> Result<String, EvalError> {
    let chars: Vec<char> = template.chars().collect();
    let mut out = String::new();
    let mut i = 0usize;
    while i < chars.len() {
        match chars[i] {
            '{' if chars.get(i + 1) == Some(&'{') => {
                out.push('{');
                i += 2;
            }
            '}' if chars.get(i + 1) == Some(&'}') => {
                out.push('}');
                i += 2;
            }
            '{' => {
                let mut j = i + 1;
                while j < chars.len() && chars[j].is_ascii_digit() {
                    j += 1;
                }
                if j > i + 1 && chars.get(j) == Some(&'}') {
                    let idx: usize = template
                        .chars()
                        .skip(i + 1)
                        .take(j - i - 1)
                        .collect::<String>()
                        .parse()
                        .map_err(|_| {
                            EvalError::new("$format placement index is too large".to_string())
                        })?;
                    let placed = args.get(idx).ok_or_else(|| {
                        EvalError::new("$format has no such argument".to_string())
                    })?;
                    out.push_str(&string_value(placed));
                    i = j + 1;
                } else {
                    return Err(EvalError::new(
                        "The $format template has an unbalanced brace".to_string(),
                    ));
                }
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    Ok(out)
}

fn type_of(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

// ---- Date/time helpers -----------------------------------------------------

/// `$fromMillis`: epoch millis -> ISO 8601 UTC (or a handful of the JSONata
/// date-format picture tokens).
fn millis_to_iso(millis: i64, format: Option<&str>) -> Result<Value, EvalError> {
    let seconds = millis.div_euclid(1000);
    let nanos = (millis.rem_euclid(1000) * 1_000_000) as u32;
    let dt = chrono::DateTime::from_timestamp(seconds, nanos)
        .ok_or_else(|| EvalError::new("$fromMillis: the timestamp is out of range".to_string()))?;
    match format {
        None => Ok(Value::String(
            dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        )),
        Some(pic) => Ok(Value::String(format_picture(dt, pic)?)),
    }
}

/// The supported format picture subset: `yyyy`, `MM`, `dd`, `HH`, `mm`,
/// `ss`, `SSS` and single-quoted literals.
fn format_picture(dt: chrono::DateTime<chrono::Utc>, pic: &str) -> Result<String, EvalError> {
    let chars: Vec<char> = pic.chars().collect();
    let mut out = String::new();
    let mut i = 0usize;
    while i < chars.len() {
        if chars[i] == '\'' {
            i += 1;
            while i < chars.len() && chars[i] != '\'' {
                out.push(chars[i]);
                i += 1;
            }
            if i < chars.len() {
                i += 1; // the closing quote
            }
            continue;
        }
        let token = |len: usize| chars.get(i..i + len).map(|s| s.iter().collect::<String>());
        if token(3) == Some("SSS".to_string()) {
            out.push_str(&format!("{:03}", dt.timestamp_subsec_millis()));
            i += 3;
        } else if token(4) == Some("yyyy".to_string()) {
            out.push_str(&format!("{:04}", dt.year()));
            i += 4;
        } else if matches!(token(2), Some(t) if t == "MM") {
            out.push_str(&format!("{:02}", dt.month()));
            i += 2;
        } else if matches!(token(2), Some(t) if t == "dd") {
            out.push_str(&format!("{:02}", dt.day()));
            i += 2;
        } else if matches!(token(2), Some(t) if t == "HH") {
            out.push_str(&format!("{:02}", dt.hour()));
            i += 2;
        } else if matches!(token(2), Some(t) if t == "mm") {
            out.push_str(&format!("{:02}", dt.minute()));
            i += 2;
        } else if matches!(token(2), Some(t) if t == "ss") {
            out.push_str(&format!("{:02}", dt.second()));
            i += 2;
        } else {
            return Err(EvalError::new(format!(
                "The $fromMillis format picture uses the unsupported token '{}'",
                chars[i]
            )));
        }
    }
    Ok(out)
}

fn parse_iso_millis(iso: &str) -> Result<Jv, EvalError> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(iso) {
        return Ok(Jv::Val(number_value(dt.timestamp_millis() as f64)));
    }
    // Date-only forms read as UTC midnight.
    if let Ok(date) = chrono::NaiveDate::parse_from_str(iso, "%Y-%m-%d") {
        let mid = date.and_hms_opt(0, 0, 0).expect("midnight is a valid time");
        return Ok(Jv::Val(number_value(
            mid.and_utc().timestamp_millis() as f64
        )));
    }
    Err(EvalError::new(
        "The $toMillis argument is not a valid ISO 8601 timestamp".to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn eval_input(src: &str, input: &Value) -> Result<Value, EvalError> {
        eval(src, &StatesCtx::input_only(input, &json!({})))
    }

    #[test]
    fn parser_depth_cap_stops_runaway_paren_nesting() {
        // ~800 paren pairs used to escape the cap and abort the process;
        // every descent now counts, so this returns a clean EvalError.
        let src = format!("{}1{}", "(".repeat(10_000), ")".repeat(10_000));
        let err = eval_input(&src, &json!({})).unwrap_err();
        assert!(err.message.contains("too deeply nested"), "{}", err.message);
    }

    #[test]
    fn paren_nesting_within_the_cap_still_parses() {
        let src = format!("{}1{}", "(".repeat(100), ")".repeat(100));
        assert_eq!(eval_input(&src, &json!({})).unwrap(), json!(1));
    }

    #[test]
    fn sibling_arguments_do_not_accumulate_parse_depth() {
        // Depth is per nesting level, not per descent: a flat literal with
        // 200 members and 200 call arguments parse like their 2-member
        // versions.
        let items: String = (0..200).map(|_| "1").collect::<Vec<_>>().join(",");
        let expected = Value::Array((0..200).map(|_| json!(1)).collect());
        assert_eq!(
            eval_input(&format!("[{items}]"), &json!({})).unwrap(),
            expected
        );
        let args: String = (0..200).map(|_| "1").collect::<Vec<_>>().join(",");
        assert_eq!(
            eval_input(&format!("$sum([{args}])"), &json!({})).unwrap(),
            json!(200)
        );
    }

    #[test]
    fn mixed_numeric_and_truthy_predicates_unwrap_singletons() {
        // The review case: a numerically-failing item plus a truthy item used
        // to panic on `first.expect(...)`; truthy inclusion wins its place
        // and the one-element result reads as that element.
        let input = json!({"items": [{"v": 5}, {"v": "y"}]});
        assert_eq!(eval_input("items[v]", &input).unwrap(), json!({"v": "y"}));
    }

    #[test]
    fn multi_numeric_predicates_select_every_matching_position() {
        // Both items carry the index matching their own position; the old
        // was_number/first pair silently dropped every match but the last.
        let input = json!({"vals": [10, 20], "items": [{"v": 0}, {"v": 1}]});
        assert_eq!(
            eval_input("items[v]", &input).unwrap(),
            json!([{"v": 0}, {"v": 1}])
        );
        // A literal position still reads as the definite item's value.
        assert_eq!(
            eval_input("vals[1]", &json!({"vals": [10, 20]})).unwrap(),
            json!(20)
        );
    }

    #[test]
    fn bracket_never_panics_on_mixed_multi_numeric() {
        // Numeric items, truthy items and multi-element numeric arrays in
        // one filter: numbers position-select their own item, strings
        // include, empty multi-numeric excludes — no panic in any mixture.
        let input = json!({"items": [
            {"v": 0},
            {"v": "y"},
            {"v": [0, 2]},
            {"v": []},
            {"v": 4}
        ]});
        assert_eq!(
            eval_input("items[v]", &input).unwrap(),
            json!([{"v": 0}, {"v": "y"}, {"v": [0, 2]}, {"v": 4}])
        );
    }
}
