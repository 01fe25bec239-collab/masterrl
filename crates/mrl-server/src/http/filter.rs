//! `?filter=` expressions: `state in (RUNNING,QUEUED) and algorithm_id = "sft"`.
//!
//! Grammar (keywords are case-insensitive):
//! ```text
//! expr  := and ("or" and)*
//! and   := atom ("and" atom)*
//! atom  := "(" expr ")" | field op value | field "in" "(" value ("," value)* ")"
//! op    := "=" | "!=" | "<" | "<=" | ">" | ">="
//! value := "string" | number | word | true | false
//! ```
//! Fields must be on the resource's allow-list. Modules turn the tree into SQL themselves.

use crate::error::{ApiError, ErrorCode};
use std::fmt;

const MAX_LEN: usize = 2000;
const MAX_DEPTH: usize = 16;
const MAX_IN: usize = 100;

/// A parsed filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Filter {
    /// All must match.
    And(Vec<Filter>),
    /// Any must match.
    Or(Vec<Filter>),
    /// `field op value`.
    Compare {
        /// Allow-listed field.
        field: String,
        /// Operator.
        op: Op,
        /// Right-hand value.
        value: Value,
    },
    /// `field in (v, …)`.
    In {
        /// Allow-listed field.
        field: String,
        /// Accepted values.
        values: Vec<Value>,
    },
}

/// Comparison operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    /// `=`
    Eq,
    /// `!=`
    Ne,
    /// `<`
    Lt,
    /// `<=`
    Le,
    /// `>`
    Gt,
    /// `>=`
    Ge,
}

impl Op {
    fn as_str(self) -> &'static str {
        match self {
            Op::Eq => "=",
            Op::Ne => "!=",
            Op::Lt => "<",
            Op::Le => "<=",
            Op::Gt => ">",
            Op::Ge => ">=",
        }
    }
}

/// A literal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    /// `"quoted"`.
    Str(String),
    /// `12`, `-0.5` (kept as text so no precision is lost).
    Number(String),
    /// A bare word like `RUNNING`.
    Word(String),
    /// `true` / `false`.
    Bool(bool),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Str(s) => {
                f.write_str("\"")?;
                for c in s.chars() {
                    if c == '"' || c == '\\' {
                        f.write_str("\\")?;
                    }
                    write!(f, "{c}")?;
                }
                f.write_str("\"")
            }
            Value::Number(n) | Value::Word(n) => f.write_str(n),
            Value::Bool(b) => write!(f, "{b}"),
        }
    }
}

impl fmt::Display for Filter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let group = |f: &mut fmt::Formatter<'_>, items: &[Filter], sep: &str| {
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    f.write_str(sep)?;
                }
                match item {
                    Filter::And(_) | Filter::Or(_) => write!(f, "({item})")?,
                    _ => write!(f, "{item}")?,
                }
            }
            Ok(())
        };
        match self {
            Filter::And(items) => group(f, items, " and "),
            Filter::Or(items) => group(f, items, " or "),
            Filter::Compare { field, op, value } => write!(f, "{field} {} {value}", op.as_str()),
            Filter::In { field, values } => {
                write!(f, "{field} in (")?;
                for (i, v) in values.iter().enumerate() {
                    if i > 0 {
                        f.write_str(",")?;
                    }
                    write!(f, "{v}")?;
                }
                f.write_str(")")
            }
        }
    }
}

/// Parses `input`, allowing only `fields`. Errors are 400 MRL-VAL-006.
pub fn parse(input: &str, fields: &[&str]) -> Result<Filter, ApiError> {
    if input.len() > MAX_LEN {
        return Err(err(
            0,
            &format!("the filter is longer than {MAX_LEN} characters"),
        ));
    }
    let tokens = lex(input)?;
    let mut p = Parser {
        tokens,
        pos: 0,
        fields,
        end: input.len(),
    };
    let tree = p.expr(0)?;
    match p.tokens.get(p.pos) {
        None => Ok(tree),
        Some((at, _)) => Err(err(*at, "expected `and`, `or` or the end of the filter")),
    }
}

fn err(at: usize, msg: &str) -> ApiError {
    ApiError::new(
        ErrorCode::Filter,
        format!("filter, character {}: {msg}.", at + 1),
    )
}

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Ident(String),
    Str(String),
    Num(String),
    Op(Op),
    LParen,
    RParen,
    Comma,
}

fn is_ident_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_'
}

fn is_ident(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '.'
}

fn lex(input: &str) -> Result<Vec<(usize, Tok)>, ApiError> {
    let mut out = Vec::new();
    let mut chars = input.char_indices().peekable();
    while let Some((at, c)) = chars.next() {
        let tok = match c {
            ' ' | '\t' | '\n' | '\r' => continue,
            '(' => Tok::LParen,
            ')' => Tok::RParen,
            ',' => Tok::Comma,
            '=' => Tok::Op(Op::Eq),
            '!' => match chars.next() {
                Some((_, '=')) => Tok::Op(Op::Ne),
                _ => return Err(err(at, "`!` must be followed by `=`")),
            },
            '<' | '>' => {
                let eq = chars.next_if(|(_, c)| *c == '=').is_some();
                Tok::Op(match (c, eq) {
                    ('<', false) => Op::Lt,
                    ('<', true) => Op::Le,
                    ('>', false) => Op::Gt,
                    _ => Op::Ge,
                })
            }
            '"' => {
                let mut s = String::new();
                loop {
                    match chars.next() {
                        None => return Err(err(at, "this text has no closing quote")),
                        Some((_, '"')) => break,
                        Some((_, '\\')) => match chars.next() {
                            Some((_, e @ ('"' | '\\'))) => s.push(e),
                            Some((i, _)) => {
                                return Err(err(i, "only \\\" and \\\\ can follow a backslash"))
                            }
                            None => return Err(err(at, "this text has no closing quote")),
                        },
                        Some((_, ch)) => s.push(ch),
                    }
                }
                Tok::Str(s)
            }
            c if c == '-' || c.is_ascii_digit() => {
                let mut s = String::from(c);
                while let Some((_, d)) = chars.next_if(|(_, d)| d.is_ascii_digit() || *d == '.') {
                    s.push(d);
                }
                if !valid_number(&s) {
                    return Err(err(at, &format!("{s:?} is not a number the filter reads")));
                }
                Tok::Num(s)
            }
            c if is_ident_start(c) => {
                let mut s = String::from(c);
                while let Some((_, d)) = chars.next_if(|(_, d)| is_ident(*d)) {
                    s.push(d);
                }
                Tok::Ident(s)
            }
            other => return Err(err(at, &format!("{other:?} is not allowed here"))),
        };
        out.push((at, tok));
    }
    Ok(out)
}

fn valid_number(s: &str) -> bool {
    let digits = s.strip_prefix('-').unwrap_or(s);
    let mut parts = digits.split('.');
    let whole = parts.next().unwrap_or("");
    let frac = parts.next();
    let digits_ok = |p: &str| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit());
    parts.next().is_none() && digits_ok(whole) && frac.is_none_or(digits_ok)
}

struct Parser<'a> {
    tokens: Vec<(usize, Tok)>,
    pos: usize,
    fields: &'a [&'a str],
    end: usize,
}

impl Parser<'_> {
    fn peek(&self) -> Option<&Tok> {
        self.tokens.get(self.pos).map(|(_, t)| t)
    }

    fn at(&self) -> usize {
        self.tokens.get(self.pos).map_or(self.end, |(a, _)| *a)
    }

    fn keyword(&mut self, kw: &str) -> bool {
        match self.peek() {
            Some(Tok::Ident(s)) if s.eq_ignore_ascii_case(kw) => {
                self.pos += 1;
                true
            }
            _ => false,
        }
    }

    fn expect(&mut self, tok: Tok, what: &str) -> Result<(), ApiError> {
        if self.peek() == Some(&tok) {
            self.pos += 1;
            Ok(())
        } else {
            Err(err(self.at(), &format!("expected {what}")))
        }
    }

    fn expr(&mut self, depth: usize) -> Result<Filter, ApiError> {
        if depth > MAX_DEPTH {
            return Err(err(
                self.at(),
                &format!("the filter nests more than {MAX_DEPTH} levels"),
            ));
        }
        let mut items = vec![self.and(depth)?];
        while self.keyword("or") {
            items.push(self.and(depth)?);
        }
        Ok(one_or(items, Filter::Or))
    }

    fn and(&mut self, depth: usize) -> Result<Filter, ApiError> {
        let mut items = vec![self.atom(depth)?];
        while self.keyword("and") {
            items.push(self.atom(depth)?);
        }
        Ok(one_or(items, Filter::And))
    }

    fn atom(&mut self, depth: usize) -> Result<Filter, ApiError> {
        if self.peek() == Some(&Tok::LParen) {
            self.pos += 1;
            let inner = self.expr(depth + 1)?;
            self.expect(Tok::RParen, "`)`")?;
            return Ok(inner);
        }
        let at = self.at();
        let field = match self.peek() {
            Some(Tok::Ident(s)) if !is_keyword(s) => s.clone(),
            _ => return Err(err(at, "expected a field name")),
        };
        if !self.fields.contains(&field.as_str()) {
            return Err(err(
                at,
                &format!(
                    "{field:?} is not a field you can filter on here; use one of: {}",
                    self.fields.join(", ")
                ),
            ));
        }
        self.pos += 1;
        if self.keyword("in") {
            self.expect(Tok::LParen, "`(` after `in`")?;
            let mut values = vec![self.value()?];
            while self.peek() == Some(&Tok::Comma) {
                self.pos += 1;
                values.push(self.value()?);
                if values.len() > MAX_IN {
                    return Err(err(
                        self.at(),
                        &format!("`in` takes at most {MAX_IN} values"),
                    ));
                }
            }
            self.expect(Tok::RParen, "`,` or `)`")?;
            return Ok(Filter::In { field, values });
        }
        let op = match self.peek() {
            Some(Tok::Op(op)) => *op,
            _ => return Err(err(self.at(), "expected an operator like `=` or `in`")),
        };
        self.pos += 1;
        let value = self.value()?;
        Ok(Filter::Compare { field, op, value })
    }

    fn value(&mut self) -> Result<Value, ApiError> {
        let v = match self.peek() {
            Some(Tok::Str(s)) => Value::Str(s.clone()),
            Some(Tok::Num(n)) => Value::Number(n.clone()),
            Some(Tok::Ident(w)) if w.eq_ignore_ascii_case("true") => Value::Bool(true),
            Some(Tok::Ident(w)) if w.eq_ignore_ascii_case("false") => Value::Bool(false),
            Some(Tok::Ident(w)) if !is_keyword(w) => Value::Word(w.clone()),
            _ => return Err(err(self.at(), "expected a value")),
        };
        self.pos += 1;
        Ok(v)
    }
}

fn is_keyword(s: &str) -> bool {
    ["and", "or", "in", "true", "false"]
        .iter()
        .any(|k| s.eq_ignore_ascii_case(k))
}

fn one_or(mut items: Vec<Filter>, wrap: fn(Vec<Filter>) -> Filter) -> Filter {
    if items.len() == 1 {
        items.remove(0)
    } else {
        wrap(items)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const FIELDS: &[&str] = &["state", "algorithm_id", "created_at", "gpu.count", "f"];

    fn cmp(field: &str, op: Op, value: Value) -> Filter {
        Filter::Compare {
            field: field.to_owned(),
            op,
            value,
        }
    }

    #[test]
    fn parses_the_documented_example() {
        let f = parse(
            "state in (RUNNING,QUEUED) and algorithm_id = \"sft\"",
            FIELDS,
        )
        .unwrap();
        assert_eq!(
            f,
            Filter::And(vec![
                Filter::In {
                    field: "state".into(),
                    values: vec![Value::Word("RUNNING".into()), Value::Word("QUEUED".into())],
                },
                cmp("algorithm_id", Op::Eq, Value::Str("sft".into())),
            ])
        );
    }

    #[test]
    fn and_binds_tighter_than_or_and_parens_group() {
        let f = parse("f = 1 or f = 2 and f = 3", FIELDS).unwrap();
        assert!(matches!(&f, Filter::Or(v) if matches!(v[1], Filter::And(_))));
        let g = parse("(f = 1 or f = 2) AND f >= -3.5", FIELDS).unwrap();
        assert!(matches!(&g, Filter::And(v) if matches!(v[0], Filter::Or(_))));
        assert_eq!(g.to_string(), "(f = 1 or f = 2) and f >= -3.5");
    }

    #[test]
    fn all_operators_and_values() {
        for (src, op) in [
            ("=", Op::Eq),
            ("!=", Op::Ne),
            ("<", Op::Lt),
            ("<=", Op::Le),
            (">", Op::Gt),
            (">=", Op::Ge),
        ] {
            assert_eq!(
                parse(&format!("f {src} 1"), FIELDS).unwrap(),
                cmp("f", op, Value::Number("1".into()))
            );
        }
        assert_eq!(
            parse("f = TRUE", FIELDS).unwrap(),
            cmp("f", Op::Eq, Value::Bool(true))
        );
        assert_eq!(
            parse(r#"f = "a \"q\" \\ b""#, FIELDS).unwrap(),
            cmp("f", Op::Eq, Value::Str(r#"a "q" \ b"#.into()))
        );
        assert!(parse("gpu.count > 2", FIELDS).is_ok());
    }

    #[test]
    fn errors_name_a_position() {
        for bad in [
            "",
            "state",
            "state =",
            "bogus = 1",
            "state in ()",
            "state in (A,",
            "state = \"open",
            "f = 1 f = 2",
            "f = 1.2.3",
            "f ! 1",
            "(f = 1",
            "f = 1 and",
            "and = 1",
            "f = \"\\n\"",
            "f = @",
        ] {
            let e = parse(bad, FIELDS).unwrap_err();
            assert_eq!(e.code, ErrorCode::Filter, "{bad}");
            assert!(e.detail.contains("character"), "{bad}: {}", e.detail);
        }
        let unknown = parse("bogus = 1", FIELDS).unwrap_err();
        assert!(unknown.detail.contains("algorithm_id"));
    }

    #[test]
    fn limits_hold() {
        let deep = format!("{}f = 1{}", "(".repeat(40), ")".repeat(40));
        assert!(parse(&deep, FIELDS).is_err());
        assert!(parse(&"f = 1 and ".repeat(300), FIELDS).is_err());
        let many = (0..=MAX_IN)
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(",");
        assert!(parse(&format!("f in ({many})"), FIELDS).is_err());
    }

    fn value() -> impl Strategy<Value = Value> {
        prop_oneof![
            any::<String>().prop_map(Value::Str),
            (-1000i64..1000).prop_map(|n| Value::Number(n.to_string())),
            "[A-Z][A-Z_]{0,8}"
                .prop_filter("keywords are not words", |w| !is_keyword(w))
                .prop_map(Value::Word),
            any::<bool>().prop_map(Value::Bool),
        ]
    }

    fn tree() -> impl Strategy<Value = Filter> {
        let field = prop::sample::select(FIELDS.to_vec()).prop_map(str::to_owned);
        let op = prop::sample::select(vec![Op::Eq, Op::Ne, Op::Lt, Op::Le, Op::Gt, Op::Ge]);
        let leaf = prop_oneof![
            (field.clone(), op, value()).prop_map(|(field, op, value)| Filter::Compare {
                field,
                op,
                value
            }),
            (field, prop::collection::vec(value(), 1..5))
                .prop_map(|(field, values)| Filter::In { field, values }),
        ];
        leaf.prop_recursive(4, 24, 4, |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 2..4).prop_map(Filter::And),
                prop::collection::vec(inner, 2..4).prop_map(Filter::Or),
            ]
        })
    }

    proptest! {
        #[test]
        fn never_panics(s in any::<String>()) {
            let _ = parse(&s, FIELDS);
        }

        #[test]
        fn never_panics_on_filter_like_text(s in "[a-z_ ()=<>!,\"\\\\0-9.-]{0,80}") {
            let _ = parse(&s, FIELDS);
        }

        #[test]
        fn display_round_trips(f in tree()) {
            let text = f.to_string();
            prop_assume!(text.len() <= MAX_LEN);
            prop_assert_eq!(parse(&text, FIELDS).unwrap(), f);
        }
    }
}
