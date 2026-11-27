//! The restricted filter language for record browsing and transform specs.
//!
//! The twin of `mrl_runtime.builtin.transform.Expr` (the language is described there); both
//! run `tests/fixtures/p27/expressions.json`. No code runs: an expression is parsed into a
//! small tree and evaluated against one JSON record.

use serde_json::{Map, Value};

/// Longest expression accepted, in characters.
pub const MAX_CHARS: usize = 2000;
/// Deepest nesting accepted.
pub const MAX_DEPTH: usize = 32;

/// An expression that can't be read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message} (at character {})", .pos + 1)]
pub struct ExprError {
    /// What is wrong, in plain words.
    pub message: String,
    /// Character index where reading stopped.
    pub pos: usize,
}

fn err<T>(message: impl Into<String>, pos: usize) -> Result<T, ExprError> {
    Err(ExprError {
        message: message.into(),
        pos,
    })
}

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Num(Value),
    Str(String),
    Name(String),
    Op(&'static str),
    End,
}

const OPS: [&str; 12] = [
    "==", "!=", "<=", ">=", "<", ">", "(", ")", "[", "]", ",", ".",
];
const KEYWORDS: [&str; 7] = ["and", "or", "not", "in", "true", "false", "null"];
const FUNCS: [(&str, usize); 7] = [
    ("len", 1),
    ("lower", 1),
    ("upper", 1),
    ("contains", 2),
    ("startswith", 2),
    ("endswith", 2),
    ("has", 1),
];

fn tokens(src: &str) -> Result<Vec<(Tok, usize)>, ExprError> {
    let chars: Vec<char> = src.chars().collect();
    if chars.len() > MAX_CHARS {
        return err(
            format!("the expression is longer than {MAX_CHARS} characters"),
            0,
        );
    }
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        let start = i;
        if c.is_whitespace() {
            i += 1;
        } else if c.is_ascii_digit() {
            while i < chars.len() && chars[i].is_ascii_digit() {
                i += 1;
            }
            let mut float = false;
            if i + 1 < chars.len() && chars[i] == '.' && chars[i + 1].is_ascii_digit() {
                float = true;
                i += 1;
                while i < chars.len() && chars[i].is_ascii_digit() {
                    i += 1;
                }
            }
            if i < chars.len() && matches!(chars[i], 'e' | 'E') {
                let mut j = i + 1;
                if j < chars.len() && matches!(chars[j], '+' | '-') {
                    j += 1;
                }
                if j < chars.len() && chars[j].is_ascii_digit() {
                    float = true;
                    i = j;
                    while i < chars.len() && chars[i].is_ascii_digit() {
                        i += 1;
                    }
                }
            }
            let raw: String = chars[start..i].iter().collect();
            let value = if float {
                match raw.parse::<f64>() {
                    Ok(f) if f.is_finite() => serde_json::Number::from_f64(f).map(Value::Number),
                    _ => None,
                }
            } else {
                raw.parse::<i64>().ok().map(Value::from)
            };
            match value {
                Some(v) => out.push((Tok::Num(v), start)),
                None => return err("the number is too large", start),
            }
        } else if c == '\'' || c == '"' {
            i += 1;
            let mut s = String::new();
            loop {
                match chars.get(i) {
                    None => return err("the text is not closed", start),
                    Some(&q) if q == c => break,
                    Some('\\') => {
                        let e = chars.get(i + 1).copied();
                        s.push(match e {
                            Some('\\') => '\\',
                            Some('\'') => '\'',
                            Some('"') => '"',
                            Some('n') => '\n',
                            Some('t') => '\t',
                            Some('r') => '\r',
                            Some(o) => return err(format!("unknown escape \\{o}"), i),
                            None => return err("the text is not closed", start),
                        });
                        i += 2;
                    }
                    Some(&o) => {
                        s.push(o);
                        i += 1;
                    }
                }
            }
            i += 1;
            out.push((Tok::Str(s), start));
        } else if c.is_ascii_alphabetic() || c == '_' {
            while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            out.push((Tok::Name(chars[start..i].iter().collect()), start));
        } else {
            let rest: String = chars[i..chars.len().min(i + 2)].iter().collect();
            match OPS.iter().find(|op| rest.starts_with(**op)) {
                Some(op) => {
                    i += op.len();
                    out.push((Tok::Op(op), start));
                }
                None => return err(format!("unexpected character {c:?}"), start),
            }
        }
    }
    out.push((Tok::End, chars.len()));
    Ok(out)
}

#[derive(Debug, Clone, PartialEq)]
enum Part {
    Key(String),
    Index(usize),
}

#[derive(Debug, Clone, PartialEq)]
enum Node {
    Lit(Value),
    Path(Vec<Part>),
    List(Vec<Node>),
    Or(Box<Node>, Box<Node>),
    And(Box<Node>, Box<Node>),
    Not(Box<Node>),
    In(Box<Node>, Box<Node>),
    Cmp(&'static str, Box<Node>, Box<Node>),
    Call(&'static str, Vec<Node>),
}

struct Parser {
    toks: Vec<(Tok, usize)>,
    i: usize,
    depth: usize,
}

impl Parser {
    fn peek(&self) -> &(Tok, usize) {
        &self.toks[self.i.min(self.toks.len() - 1)]
    }
    fn take(&mut self) -> (Tok, usize) {
        let t = self.peek().clone();
        self.i += 1;
        t
    }
    fn word(&mut self, w: &str) -> bool {
        if matches!(&self.peek().0, Tok::Name(n) if n == w) {
            self.i += 1;
            return true;
        }
        false
    }
    fn sym(&mut self, s: &str) -> bool {
        if matches!(self.peek().0, Tok::Op(o) if o == s) {
            self.i += 1;
            return true;
        }
        false
    }
    fn expect(&mut self, s: &str) -> Result<(), ExprError> {
        if self.sym(s) {
            return Ok(());
        }
        let (t, pos) = self.peek().clone();
        err(format!("expected {s:?} but found {}", show(&t)), pos)
    }
    fn nest(&mut self) -> Result<(), ExprError> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return err(
                format!("the expression nests deeper than {MAX_DEPTH} levels"),
                self.peek().1,
            );
        }
        Ok(())
    }

    fn or_expr(&mut self) -> Result<Node, ExprError> {
        self.nest()?;
        let mut node = self.and_expr()?;
        while self.word("or") {
            node = Node::Or(Box::new(node), Box::new(self.and_expr()?));
        }
        self.depth -= 1;
        Ok(node)
    }

    fn and_expr(&mut self) -> Result<Node, ExprError> {
        let mut node = self.not_expr()?;
        while self.word("and") {
            node = Node::And(Box::new(node), Box::new(self.not_expr()?));
        }
        Ok(node)
    }

    fn not_expr(&mut self) -> Result<Node, ExprError> {
        if self.word("not") {
            self.nest()?;
            let node = Node::Not(Box::new(self.not_expr()?));
            self.depth -= 1;
            return Ok(node);
        }
        self.comparison()
    }

    fn comparison(&mut self) -> Result<Node, ExprError> {
        let left = self.operand()?;
        if let Tok::Op(op) = self.peek().0 {
            if matches!(op, "==" | "!=" | "<" | "<=" | ">" | ">=") {
                self.i += 1;
                return Ok(Node::Cmp(op, Box::new(left), Box::new(self.operand()?)));
            }
        }
        if self.word("in") {
            return Ok(Node::In(Box::new(left), Box::new(self.operand()?)));
        }
        let next_is_in = matches!(self.toks.get(self.i + 1), Some((Tok::Name(n), _)) if n == "in");
        if matches!(&self.peek().0, Tok::Name(n) if n == "not") && next_is_in {
            self.i += 2;
            let inner = Node::In(Box::new(left), Box::new(self.operand()?));
            return Ok(Node::Not(Box::new(inner)));
        }
        Ok(left)
    }

    fn operand(&mut self) -> Result<Node, ExprError> {
        let (t, pos) = self.take();
        match t {
            Tok::Num(v) => Ok(Node::Lit(v)),
            Tok::Str(s) => Ok(Node::Lit(Value::String(s))),
            Tok::Op("(") => {
                let node = self.or_expr()?;
                self.expect(")")?;
                Ok(node)
            }
            Tok::Op("[") => {
                self.nest()?;
                let mut items = Vec::new();
                if !self.sym("]") {
                    items.push(self.or_expr()?);
                    while self.sym(",") {
                        items.push(self.or_expr()?);
                    }
                    self.expect("]")?;
                }
                self.depth -= 1;
                Ok(Node::List(items))
            }
            Tok::Name(n) => match n.as_str() {
                "true" => Ok(Node::Lit(Value::Bool(true))),
                "false" => Ok(Node::Lit(Value::Bool(false))),
                "null" => Ok(Node::Lit(Value::Null)),
                k if KEYWORDS.contains(&k) => err(format!("unexpected {k:?}"), pos),
                f => match FUNCS.iter().find(|(name, _)| *name == f) {
                    Some(&(name, arity)) if self.sym("(") => self.call(name, arity, pos),
                    _ => self.path(n),
                },
            },
            other => err(format!("unexpected {}", show(&other)), pos),
        }
    }

    fn call(&mut self, name: &'static str, arity: usize, pos: usize) -> Result<Node, ExprError> {
        self.nest()?;
        let mut args = Vec::new();
        if !self.sym(")") {
            if name == "has" {
                match self.take() {
                    (Tok::Name(n), _) if !KEYWORDS.contains(&n.as_str()) => {
                        args.push(self.path(n)?)
                    }
                    (_, p) => return err("has() takes a field name", p),
                }
            } else {
                args.push(self.or_expr()?);
            }
            while self.sym(",") {
                args.push(self.or_expr()?);
            }
            self.expect(")")?;
        }
        self.depth -= 1;
        if args.len() != arity {
            return err(format!("{name}() takes {arity} value(s)"), pos);
        }
        Ok(Node::Call(name, args))
    }

    fn path(&mut self, first: String) -> Result<Node, ExprError> {
        let mut parts = vec![Part::Key(first)];
        loop {
            if self.sym(".") {
                match self.take() {
                    (Tok::Name(n), _) => parts.push(Part::Key(n)),
                    (_, p) => return err("expected a field name after '.'", p),
                }
            } else if self.sym("[") {
                match self.take() {
                    (Tok::Num(Value::Number(n)), _) if n.is_u64() => parts
                        .push(Part::Index(n.as_u64().map_or(usize::MAX, |v| {
                            usize::try_from(v).unwrap_or(usize::MAX)
                        }))),
                    (_, p) => return err("expected a whole number inside [ ]", p),
                }
                self.expect("]")?;
            } else {
                return Ok(Node::Path(parts));
            }
        }
    }
}

fn show(t: &Tok) -> String {
    match t {
        Tok::End => "the end".to_owned(),
        Tok::Num(v) => v.to_string(),
        Tok::Str(s) => format!("{s:?}"),
        Tok::Name(n) => format!("{n:?}"),
        Tok::Op(o) => format!("{o:?}"),
    }
}

/// A parsed filter expression.
#[derive(Debug, Clone, PartialEq)]
pub struct Expr(Node);

impl Expr {
    /// Parses `src`.
    pub fn parse(src: &str) -> Result<Self, ExprError> {
        let mut p = Parser {
            toks: tokens(src)?,
            i: 0,
            depth: 0,
        };
        let node = p.or_expr()?;
        let (t, pos) = p.peek().clone();
        if t != Tok::End {
            return err(format!("unexpected {}", show(&t)), pos);
        }
        Ok(Self(node))
    }

    /// The value of the expression for `record`.
    pub fn evaluate(&self, record: &Value) -> Value {
        eval(&self.0, record)
    }

    /// True when the expression is exactly `true` for `record`.
    pub fn matches(&self, record: &Value) -> bool {
        self.evaluate(record) == Value::Bool(true)
    }
}

fn lookup<'a>(record: &'a Value, parts: &[Part]) -> Option<&'a Value> {
    parts.iter().try_fold(record, |cur, p| match p {
        Part::Key(k) => cur.as_object().and_then(|o| o.get(k)),
        Part::Index(i) => cur.as_array().and_then(|a| a.get(*i)),
    })
}

fn num_eq(a: &serde_json::Number, b: &serde_json::Number) -> bool {
    match (a.as_i64(), b.as_i64()) {
        (Some(x), Some(y)) => x == y,
        _ => a.as_f64() == b.as_f64(),
    }
}

fn eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => num_eq(x, y),
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(p, q)| eq(p, q))
        }
        (Value::Object(x), Value::Object(y)) => obj_eq(x, y),
        _ => a == b,
    }
}

fn obj_eq(x: &Map<String, Value>, y: &Map<String, Value>) -> bool {
    x.len() == y.len() && x.iter().all(|(k, v)| y.get(k).is_some_and(|w| eq(v, w)))
}

fn order(op: &str, a: &Value, b: &Value) -> bool {
    let ord = match (a, b) {
        (Value::Number(x), Value::Number(y)) => match (x.as_i64(), y.as_i64()) {
            (Some(p), Some(q)) => Some(p.cmp(&q)),
            _ => x
                .as_f64()
                .zip(y.as_f64())
                .and_then(|(p, q)| p.partial_cmp(&q)),
        },
        (Value::String(x), Value::String(y)) => Some(x.cmp(y)),
        _ => None,
    };
    let Some(o) = ord else { return false };
    match op {
        "<" => o.is_lt(),
        "<=" => o.is_le(),
        ">" => o.is_gt(),
        _ => o.is_ge(),
    }
}

fn contained(needle: &Value, hay: &Value) -> bool {
    match hay {
        Value::String(h) => needle.as_str().is_some_and(|n| h.contains(n)),
        Value::Array(items) => items.iter().any(|x| eq(needle, x)),
        Value::Object(o) => needle.as_str().is_some_and(|n| o.contains_key(n)),
        _ => false,
    }
}

fn truthy(v: &Value) -> bool {
    *v == Value::Bool(true)
}

fn eval(node: &Node, rec: &Value) -> Value {
    match node {
        Node::Lit(v) => v.clone(),
        Node::Path(p) => lookup(rec, p).cloned().unwrap_or(Value::Null),
        Node::List(items) => Value::Array(items.iter().map(|n| eval(n, rec)).collect()),
        Node::Or(a, b) => Value::Bool(truthy(&eval(a, rec)) || truthy(&eval(b, rec))),
        Node::And(a, b) => Value::Bool(truthy(&eval(a, rec)) && truthy(&eval(b, rec))),
        Node::Not(a) => Value::Bool(!truthy(&eval(a, rec))),
        Node::In(a, b) => Value::Bool(contained(&eval(a, rec), &eval(b, rec))),
        Node::Cmp(op, a, b) => {
            let (x, y) = (eval(a, rec), eval(b, rec));
            Value::Bool(match *op {
                "==" => eq(&x, &y),
                "!=" => !eq(&x, &y),
                o => order(o, &x, &y),
            })
        }
        Node::Call(name, args) => call(name, args, rec),
    }
}

fn call(name: &str, args: &[Node], rec: &Value) -> Value {
    if name == "has" {
        let found = matches!(&args[0], Node::Path(p) if lookup(rec, p).is_some());
        return Value::Bool(found);
    }
    let vals: Vec<Value> = args.iter().map(|a| eval(a, rec)).collect();
    let v = &vals[0];
    match name {
        "len" => Value::from(match v {
            Value::String(s) => s.chars().count(),
            Value::Array(a) => a.len(),
            Value::Object(o) => o.len(),
            _ => 0,
        }),
        "lower" => v
            .as_str()
            .map_or(Value::Null, |s| Value::String(s.to_lowercase())),
        "upper" => v
            .as_str()
            .map_or(Value::Null, |s| Value::String(s.to_uppercase())),
        "contains" => Value::Bool(contained(&vals[1], v)),
        _ => Value::Bool(match (v.as_str(), vals[1].as_str()) {
            (Some(s), Some(p)) if name == "startswith" => s.starts_with(p),
            (Some(s), Some(p)) => s.ends_with(p),
            _ => false,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The cases Python's evaluator runs too.
    #[test]
    fn shared_cases_match_python() {
        let doc: Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/p27/expressions.json"
        ))
        .unwrap();
        let record = &doc["record"];
        for case in doc["cases"].as_array().unwrap() {
            let src = case["expr"].as_str().unwrap();
            let got = Expr::parse(src);
            if case["error"] == Value::Bool(true) {
                assert!(got.is_err(), "{src} should be refused");
                continue;
            }
            let value = got
                .unwrap_or_else(|e| panic!("{src}: {e}"))
                .evaluate(record);
            let want = &case["result"];
            // Same value and the same JSON kind (an integer stays an integer).
            assert!(
                eq(&value, want) && value.is_f64() == want.is_f64(),
                "{src}: {value} != {want}"
            );
        }
    }

    #[test]
    fn errors_point_at_the_place() {
        let e = Expr::parse("lang = 'en'").unwrap_err();
        assert_eq!(e.pos, 5);
        assert!(e.to_string().contains("character 6"));
        assert!(Expr::parse(&"x".repeat(MAX_CHARS + 1)).is_err());
        assert!(Expr::parse(&format!("{}n{}", "(".repeat(40), ")".repeat(40))).is_err());
    }
}
