use crate::sql::ast::{BinOp, Expr, Literal, UnOp};

/// A typed runtime value.
#[derive(Debug, Clone)]
pub enum Value {
    Int(i64),
    Float(f64),
    Text(String),
    Bool(bool),
    Null,
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Int(_) => "int8",
            Self::Float(_) => "float8",
            Self::Text(_) => "text",
            Self::Bool(_) => "bool",
            Self::Null => "null",
        }
    }

    /// Encode value as UTF-8 text for the wire (Postgres text format).
    pub fn to_wire_bytes(&self) -> Option<Vec<u8>> {
        match self {
            Self::Null => None,
            Self::Int(n) => Some(n.to_string().into_bytes()),
            Self::Float(f) => Some(format!("{f}").into_bytes()),
            Self::Text(s) => Some(s.as_bytes().to_vec()),
            Self::Bool(b) => Some(if *b { b"t".to_vec() } else { b"f".to_vec() }),
        }
    }

    pub fn type_oid(&self) -> i32 {
        use crate::wire::messages::oid;
        match self {
            Self::Int(_) => oid::INT8,
            Self::Float(_) => oid::FLOAT8,
            Self::Text(_) => oid::TEXT,
            Self::Bool(_) => oid::BOOL,
            Self::Null => oid::TEXT,
        }
    }

    pub fn is_truthy(&self) -> bool {
        match self {
            Self::Bool(b) => *b,
            Self::Null => false,
            Self::Int(n) => *n != 0,
            Self::Float(f) => *f != 0.0,
            Self::Text(s) => !s.is_empty(),
        }
    }
}

pub fn eval_expr(expr: &Expr) -> anyhow::Result<Value> {
    match expr {
        Expr::Literal(lit) => Ok(match lit {
            Literal::Int(n) => Value::Int(*n),
            Literal::Float(f) => Value::Float(*f),
            Literal::Text(s) => Value::Text(s.clone()),
            Literal::Bool(b) => Value::Bool(*b),
            Literal::Null => Value::Null,
        }),

        Expr::Ident(name) => {
            // No table context in MVP — identifiers refer to nothing.
            anyhow::bail!("column \"{name}\" does not exist (no FROM clause in MVP)")
        }
        Expr::QualifiedIdent(table, col) => {
            anyhow::bail!("column \"{table}.{col}\" does not exist (no FROM clause in MVP)")
        }
        Expr::Param(n) => {
            anyhow::bail!("parameter ${n} not bound")
        }

        Expr::UnaryOp { op, expr } => {
            let v = eval_expr(expr)?;
            match op {
                UnOp::Neg => match v {
                    Value::Int(n) => Ok(Value::Int(-n)),
                    Value::Float(f) => Ok(Value::Float(-f)),
                    _ => anyhow::bail!("cannot negate {}", v.type_name()),
                },
                UnOp::Not => Ok(Value::Bool(!v.is_truthy())),
            }
        }

        Expr::BinaryOp { op, lhs, rhs } => eval_binop(op, lhs, rhs),

        Expr::IsNull { expr, negated } => {
            let v = eval_expr(expr)?;
            let is_null = matches!(v, Value::Null);
            Ok(Value::Bool(if *negated { !is_null } else { is_null }))
        }
        Expr::IsTrue { expr, negated } => {
            let v = eval_expr(expr)?;
            let result = matches!(v, Value::Bool(true));
            Ok(Value::Bool(if *negated { !result } else { result }))
        }
        Expr::IsFalse { expr, negated } => {
            let v = eval_expr(expr)?;
            let result = matches!(v, Value::Bool(false));
            Ok(Value::Bool(if *negated { !result } else { result }))
        }

        Expr::Between { expr, low, high, negated } => {
            let v = eval_expr(expr)?;
            let lo = eval_expr(low)?;
            let hi = eval_expr(high)?;
            let in_range = value_compare(&v, &lo)? >= 0 && value_compare(&v, &hi)? <= 0;
            Ok(Value::Bool(if *negated { !in_range } else { in_range }))
        }

        Expr::Like { expr, pattern, negated } => {
            let v = eval_expr(expr)?;
            let p = eval_expr(pattern)?;
            match (&v, &p) {
                (Value::Text(s), Value::Text(pat)) => {
                    let matched = like_match(s, pat);
                    Ok(Value::Bool(if *negated { !matched } else { matched }))
                }
                _ => anyhow::bail!("LIKE requires text operands"),
            }
        }

        Expr::In { expr, list, negated } => {
            let v = eval_expr(expr)?;
            let mut found = false;
            for item in list {
                let iv = eval_expr(item)?;
                if value_compare(&v, &iv).is_ok_and(|c| c == 0) {
                    found = true;
                    break;
                }
            }
            Ok(Value::Bool(if *negated { !found } else { found }))
        }

        Expr::Cast { expr, ty } => {
            let v = eval_expr(expr)?;
            cast_value(v, ty)
        }

        Expr::Function { name, args, distinct: _ } => eval_function(name, args),

        Expr::Case { operand, branches, else_ } => {
            let base = operand.as_ref().map(|e| eval_expr(e)).transpose()?;
            for (cond, result) in branches {
                let cond_val = if let Some(ref b) = base {
                    let cv = eval_expr(cond)?;
                    value_compare(b, &cv).is_ok_and(|c| c == 0)
                } else {
                    eval_expr(cond)?.is_truthy()
                };
                if cond_val {
                    return eval_expr(result);
                }
            }
            if let Some(e) = else_ {
                eval_expr(e)
            } else {
                Ok(Value::Null)
            }
        }

        Expr::Subquery(_) => {
            anyhow::bail!("subqueries not yet supported in expression context (use RowContext)")
        }

        Expr::Window { func, args, partition_by: _, order_by: _, frame: _ } => {
            // For now, just evaluate the function
            eval_function(func, args)
        }
    }
}

fn eval_binop(op: &BinOp, lhs: &Expr, rhs: &Expr) -> anyhow::Result<Value> {
    // Short-circuit AND / OR before evaluating rhs
    match op {
        BinOp::And => {
            let l = eval_expr(lhs)?;
            if !l.is_truthy() { return Ok(Value::Bool(false)); }
            let r = eval_expr(rhs)?;
            return Ok(Value::Bool(r.is_truthy()));
        }
        BinOp::Or => {
            let l = eval_expr(lhs)?;
            if l.is_truthy() { return Ok(Value::Bool(true)); }
            let r = eval_expr(rhs)?;
            return Ok(Value::Bool(r.is_truthy()));
        }
        _ => {}
    }

    let l = eval_expr(lhs)?;
    let r = eval_expr(rhs)?;

    match op {
        BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod => {
            eval_arithmetic(&l, &r, op)
        }
        BinOp::Concat => match (&l, &r) {
            (Value::Text(a), Value::Text(b)) => Ok(Value::Text(format!("{a}{b}"))),
            _ => Ok(Value::Text(format!("{}{}", val_to_text(&l), val_to_text(&r)))),
        },
        BinOp::Eq => Ok(Value::Bool(value_compare(&l, &r).is_ok_and(|c| c == 0))),
        BinOp::NotEq => Ok(Value::Bool(value_compare(&l, &r).is_ok_and(|c| c != 0))),
        BinOp::Lt => Ok(Value::Bool(value_compare(&l, &r).is_ok_and(|c| c < 0))),
        BinOp::Lte => Ok(Value::Bool(value_compare(&l, &r).is_ok_and(|c| c <= 0))),
        BinOp::Gt => Ok(Value::Bool(value_compare(&l, &r).is_ok_and(|c| c > 0))),
        BinOp::Gte => Ok(Value::Bool(value_compare(&l, &r).is_ok_and(|c| c >= 0))),
        BinOp::Arrow | BinOp::LongArrow => {
            anyhow::bail!("JSON operators require a table context")
        }
        BinOp::And | BinOp::Or => unreachable!(),
    }
}

fn eval_arithmetic(l: &Value, r: &Value, op: &BinOp) -> anyhow::Result<Value> {
    // Coerce to float if either side is float.
    match (l, r) {
        (Value::Int(a), Value::Int(b)) => {
            let result = match op {
                BinOp::Add => a.checked_add(*b).ok_or_else(|| anyhow::anyhow!("integer overflow"))?,
                BinOp::Sub => a.checked_sub(*b).ok_or_else(|| anyhow::anyhow!("integer overflow"))?,
                BinOp::Mul => a.checked_mul(*b).ok_or_else(|| anyhow::anyhow!("integer overflow"))?,
                BinOp::Div => {
                    if *b == 0 { anyhow::bail!("division by zero"); }
                    a / b
                }
                BinOp::Mod => {
                    if *b == 0 { anyhow::bail!("division by zero"); }
                    a % b
                }
                _ => unreachable!(),
            };
            Ok(Value::Int(result))
        }
        _ => {
            let a = coerce_float(l)?;
            let b = coerce_float(r)?;
            let result = match op {
                BinOp::Add => a + b,
                BinOp::Sub => a - b,
                BinOp::Mul => a * b,
                BinOp::Div => {
                    if b == 0.0 { anyhow::bail!("division by zero"); }
                    a / b
                }
                BinOp::Mod => a % b,
                _ => unreachable!(),
            };
            Ok(Value::Float(result))
        }
    }
}

fn coerce_float(v: &Value) -> anyhow::Result<f64> {
    match v {
        Value::Int(n) => Ok(*n as f64),
        Value::Float(f) => Ok(*f),
        Value::Text(s) => s.parse::<f64>().map_err(|_| anyhow::anyhow!("cannot cast text to float")),
        _ => anyhow::bail!("cannot cast {} to float", v.type_name()),
    }
}

pub fn value_compare(a: &Value, b: &Value) -> anyhow::Result<i32> {
    match (a, b) {
        (Value::Null, Value::Null) => Ok(0),
        (Value::Null, _) | (_, Value::Null) => anyhow::bail!("null comparison"),
        (Value::Int(x), Value::Int(y)) => Ok(x.cmp(y) as i32),
        (Value::Float(x), Value::Float(y)) => Ok(x.partial_cmp(y).map(|o| o as i32).unwrap_or(0)),
        (Value::Int(x), Value::Float(y)) => Ok((*x as f64).partial_cmp(y).map(|o| o as i32).unwrap_or(0)),
        (Value::Float(x), Value::Int(y)) => Ok(x.partial_cmp(&(*y as f64)).map(|o| o as i32).unwrap_or(0)),
        (Value::Text(x), Value::Text(y)) => Ok(x.cmp(y) as i32),
        (Value::Bool(x), Value::Bool(y)) => Ok(x.cmp(y) as i32),
        _ => anyhow::bail!("incompatible types for comparison: {} vs {}", a.type_name(), b.type_name()),
    }
}

fn val_to_text(v: &Value) -> String {
    match v {
        Value::Int(n) => n.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Text(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Null => String::new(),
    }
}

fn cast_value(v: Value, ty: &str) -> anyhow::Result<Value> {
    match ty.to_lowercase().as_str() {
        "int" | "int4" | "int8" | "integer" | "bigint" => match &v {
            Value::Int(n) => Ok(Value::Int(*n)),
            Value::Float(f) => Ok(Value::Int(*f as i64)),
            Value::Text(s) => Ok(Value::Int(s.trim().parse()?)),
            Value::Bool(b) => Ok(Value::Int(if *b { 1 } else { 0 })),
            Value::Null => Ok(Value::Null),
        },
        "float" | "float8" | "real" | "double" | "double precision" | "numeric" => match &v {
            Value::Float(f) => Ok(Value::Float(*f)),
            Value::Int(n) => Ok(Value::Float(*n as f64)),
            Value::Text(s) => Ok(Value::Float(s.trim().parse()?)),
            Value::Null => Ok(Value::Null),
            _ => anyhow::bail!("cannot cast {} to float", v.type_name()),
        },
        "text" | "varchar" | "char" => Ok(Value::Text(val_to_text(&v))),
        "bool" | "boolean" => match &v {
            Value::Bool(b) => Ok(Value::Bool(*b)),
            Value::Int(n) => Ok(Value::Bool(*n != 0)),
            Value::Text(s) => match s.to_lowercase().as_str() {
                "t" | "true" | "yes" | "on" | "1" => Ok(Value::Bool(true)),
                _ => Ok(Value::Bool(false)),
            },
            Value::Null => Ok(Value::Null),
            _ => anyhow::bail!("cannot cast {} to bool", v.type_name()),
        },
        _ => anyhow::bail!("unknown cast target type: {ty}"),
    }
}

fn eval_function(name: &str, args: &[Expr]) -> anyhow::Result<Value> {
    match name.to_lowercase().as_str() {
        "version" => Ok(Value::Text("TPT Keystone 0.1.0 on Rust".into())),
        "pg_sleep" => Ok(Value::Null),
        "now" | "current_timestamp" => Ok(Value::Text("2026-06-30 00:00:00+00".into())),
        "current_date" => Ok(Value::Text("2026-06-30".into())),
        "current_user" | "user" | "session_user" => Ok(Value::Text("postgres".into())),
        "current_database" | "current_catalog" => Ok(Value::Text("postgres".into())),
        "current_schema" | "current_schemas" => Ok(Value::Text("public".into())),
        "pg_backend_pid" => Ok(Value::Int(1)),
        "pg_postmaster_start_time" => Ok(Value::Text("2026-06-30 00:00:00+00".into())),
        "abs" => {
            let v = eval_expr(args.first().ok_or_else(|| anyhow::anyhow!("abs() requires 1 argument"))?)?;
            match v {
                Value::Int(n) => Ok(Value::Int(n.abs())),
                Value::Float(f) => Ok(Value::Float(f.abs())),
                _ => anyhow::bail!("abs() requires numeric argument"),
            }
        }
        "upper" => {
            let v = eval_expr(args.first().ok_or_else(|| anyhow::anyhow!("upper() requires 1 argument"))?)?;
            match v {
                Value::Text(s) => Ok(Value::Text(s.to_uppercase())),
                _ => anyhow::bail!("upper() requires text argument"),
            }
        }
        "lower" => {
            let v = eval_expr(args.first().ok_or_else(|| anyhow::anyhow!("lower() requires 1 argument"))?)?;
            match v {
                Value::Text(s) => Ok(Value::Text(s.to_lowercase())),
                _ => anyhow::bail!("lower() requires text argument"),
            }
        }
        "length" | "char_length" | "character_length" => {
            let v = eval_expr(args.first().ok_or_else(|| anyhow::anyhow!("length() requires 1 argument"))?)?;
            match v {
                Value::Text(s) => Ok(Value::Int(s.chars().count() as i64)),
                _ => anyhow::bail!("length() requires text argument"),
            }
        }
        "coalesce" => {
            for arg in args {
                let v = eval_expr(arg)?;
                if !matches!(v, Value::Null) {
                    return Ok(v);
                }
            }
            Ok(Value::Null)
        }
        "nullif" => {
            if args.len() != 2 {
                anyhow::bail!("nullif() requires exactly 2 arguments");
            }
            let a = eval_expr(&args[0])?;
            let b = eval_expr(&args[1])?;
            if value_compare(&a, &b).is_ok_and(|c| c == 0) {
                Ok(Value::Null)
            } else {
                Ok(a)
            }
        }
        "greatest" => {
            let mut best: Option<Value> = None;
            for arg in args {
                let v = eval_expr(arg)?;
                if matches!(v, Value::Null) { continue; }
                best = Some(match best {
                    None => v,
                    Some(ref b) if value_compare(&v, b).is_ok_and(|c| c > 0) => v,
                    Some(b) => b,
                });
            }
            Ok(best.unwrap_or(Value::Null))
        }
        "least" => {
            let mut best: Option<Value> = None;
            for arg in args {
                let v = eval_expr(arg)?;
                if matches!(v, Value::Null) { continue; }
                best = Some(match best {
                    None => v,
                    Some(ref b) if value_compare(&v, b).is_ok_and(|c| c < 0) => v,
                    Some(b) => b,
                });
            }
            Ok(best.unwrap_or(Value::Null))
        }
        other => anyhow::bail!("function \"{other}\" does not exist"),
    }
}

/// Simple LIKE pattern matching (% = any sequence, _ = any char).
fn like_match(s: &str, pattern: &str) -> bool {
    let s: Vec<char> = s.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    like_recursive(&s, &p, 0, 0)
}

fn like_recursive(s: &[char], p: &[char], si: usize, pi: usize) -> bool {
    if pi == p.len() {
        return si == s.len();
    }
    if p[pi] == '%' {
        for i in si..=s.len() {
            if like_recursive(s, p, i, pi + 1) {
                return true;
            }
        }
        false
    } else if si < s.len() && (p[pi] == '_' || s[si] == p[pi]) {
        like_recursive(s, p, si + 1, pi + 1)
    } else {
        false
    }
}