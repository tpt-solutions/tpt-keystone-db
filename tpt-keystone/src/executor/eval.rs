use std::collections::HashMap;
use std::sync::Arc;

use crate::sql::ast::{BinOp, Expr, InList, Literal, UnOp};
use crate::storage::database::Database;
use crate::storage::TableSchema;

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

    /// Parse a raw wire-format byte value (or NULL) into a typed `Value`,
    /// using the same int/float/bool/text sniffing used for stored rows.
    pub fn from_bytes(bytes: Option<&[u8]>) -> Value {
        match bytes {
            None => Value::Null,
            Some(b) => {
                if let Ok(s) = std::str::from_utf8(b) {
                    if let Ok(n) = s.parse::<i64>() {
                        return Value::Int(n);
                    }
                    if let Ok(f) = s.parse::<f64>() {
                        return Value::Float(f);
                    }
                    if s == "t" {
                        return Value::Bool(true);
                    }
                    if s == "f" {
                        return Value::Bool(false);
                    }
                    Value::Text(s.to_string())
                } else {
                    Value::Text(String::from_utf8_lossy(b).to_string())
                }
            }
        }
    }
}

/// A single row of an outer query, kept around so a correlated subquery can
/// resolve column references that aren't satisfied by its own FROM clause.
#[derive(Clone, Default)]
pub struct OuterRow {
    pub schema: Option<Arc<TableSchema>>,
    pub values: Vec<Option<Vec<u8>>>,
    /// (table alias or name, column index range) for each table contributing
    /// to `schema`/`values`, in row-layout order — lets a qualified column
    /// reference (`e.dept`) pick the right table when two joined/nested
    /// tables share a column name.
    pub table_scopes: Vec<(String, std::ops::Range<usize>)>,
}

impl OuterRow {
    fn find_scope(&self, qualifier: &str) -> Option<std::ops::Range<usize>> {
        self.table_scopes.iter().find(|(name, _)| name == qualifier).map(|(_, r)| r.clone())
    }
}

/// A row with its column values, plus everything needed to evaluate an
/// expression against it: the enclosing table schema, database (for
/// subqueries), any outer-query rows (for correlated subqueries), bound
/// `$n` parameters (for prepared statements), and any pre-computed values
/// (aggregate/window function results keyed by the rendered expression).
#[derive(Clone)]
pub struct RowContext {
    pub values: Vec<Option<Vec<u8>>>,
    pub schema: Option<Arc<TableSchema>>,
    pub db: Option<Arc<Database>>,
    pub outer: Vec<OuterRow>,
    pub params: Vec<Value>,
    pub computed: Option<Arc<HashMap<String, Value>>>,
    pub table_scopes: Vec<(String, std::ops::Range<usize>)>,
}

impl RowContext {
    pub fn empty() -> Self {
        Self {
            values: Vec::new(),
            schema: None,
            db: None,
            outer: Vec::new(),
            params: Vec::new(),
            computed: None,
            table_scopes: Vec::new(),
        }
    }

    pub fn with_table_scopes(mut self, table_scopes: Vec<(String, std::ops::Range<usize>)>) -> Self {
        self.table_scopes = table_scopes;
        self
    }

    fn find_scope(&self, qualifier: &str) -> Option<std::ops::Range<usize>> {
        self.table_scopes.iter().find(|(name, _)| name == qualifier).map(|(_, r)| r.clone())
    }

    pub fn new(values: Vec<Option<Vec<u8>>>, schema: Option<Arc<TableSchema>>) -> Self {
        Self { values, schema, ..Self::empty() }
    }

    pub fn with_db(values: Vec<Option<Vec<u8>>>, schema: Option<Arc<TableSchema>>, db: Arc<Database>) -> Self {
        Self { values, schema, db: Some(db), ..Self::empty() }
    }

    pub fn with_outer(mut self, outer: Vec<OuterRow>) -> Self {
        self.outer = outer;
        self
    }

    pub fn with_params(mut self, params: Vec<Value>) -> Self {
        self.params = params;
        self
    }

    pub fn with_computed(mut self, computed: Arc<HashMap<String, Value>>) -> Self {
        self.computed = Some(computed);
        self
    }

    /// The (schema, values) pair for this row, usable as an outer context
    /// for a nested (correlated) subquery.
    pub fn as_outer_row(&self) -> OuterRow {
        OuterRow { schema: self.schema.clone(), values: self.values.clone(), table_scopes: self.table_scopes.clone() }
    }

    /// The full outer chain a nested subquery should see: this row's own
    /// outer chain, plus this row itself as the innermost link.
    pub fn outer_chain_for_subquery(&self) -> Vec<OuterRow> {
        let mut chain = self.outer.clone();
        chain.push(self.as_outer_row());
        chain
    }

    /// Evaluate an expression against this row.
    pub fn eval(&self, expr: &Expr) -> anyhow::Result<Value> {
        match expr {
            Expr::Literal(lit) => Ok(match lit {
                Literal::Int(n) => Value::Int(*n),
                Literal::Float(f) => Value::Float(*f),
                Literal::Text(s) => Value::Text(s.clone()),
                Literal::Bool(b) => Value::Bool(*b),
                Literal::Null => Value::Null,
            }),

            Expr::Ident(name) => self.resolve_column(None, name),
            Expr::QualifiedIdent(table, col) => self.resolve_column(Some(table), col),

            Expr::Param(n) => {
                let idx = (*n).checked_sub(1).ok_or_else(|| anyhow::anyhow!("invalid parameter ${n}"))? as usize;
                self.params.get(idx).cloned().ok_or_else(|| anyhow::anyhow!("parameter ${n} not bound"))
            }

            Expr::UnaryOp { op, expr } => {
                let v = self.eval(expr)?;
                match op {
                    UnOp::Neg => match v {
                        Value::Int(n) => Ok(Value::Int(-n)),
                        Value::Float(f) => Ok(Value::Float(-f)),
                        _ => anyhow::bail!("cannot negate {}", v.type_name()),
                    },
                    UnOp::Not => Ok(Value::Bool(!v.is_truthy())),
                }
            }

            Expr::BinaryOp { op, lhs, rhs } => self.eval_binop(op, lhs, rhs),

            Expr::IsNull { expr, negated } => {
                let v = self.eval(expr)?;
                let is_null = matches!(v, Value::Null);
                Ok(Value::Bool(if *negated { !is_null } else { is_null }))
            }
            Expr::IsTrue { expr, negated } => {
                let v = self.eval(expr)?;
                let result = matches!(v, Value::Bool(true));
                Ok(Value::Bool(if *negated { !result } else { result }))
            }
            Expr::IsFalse { expr, negated } => {
                let v = self.eval(expr)?;
                let result = matches!(v, Value::Bool(false));
                Ok(Value::Bool(if *negated { !result } else { result }))
            }

            Expr::Between { expr, low, high, negated } => {
                let v = self.eval(expr)?;
                let lo = self.eval(low)?;
                let hi = self.eval(high)?;
                let in_range = value_compare(&v, &lo)? >= 0 && value_compare(&v, &hi)? <= 0;
                Ok(Value::Bool(if *negated { !in_range } else { in_range }))
            }

            Expr::Like { expr, pattern, negated } => {
                let v = self.eval(expr)?;
                let p = self.eval(pattern)?;
                match (&v, &p) {
                    (Value::Text(s), Value::Text(pat)) => {
                        let matched = like_match(s, pat);
                        Ok(Value::Bool(if *negated { !matched } else { matched }))
                    }
                    _ => anyhow::bail!("LIKE requires text operands"),
                }
            }

            Expr::In { expr, list, negated } => {
                let found = match list {
                    InList::Exprs(items) => {
                        let v = self.eval(expr)?;
                        let mut found = false;
                        for item in items {
                            let iv = self.eval(item)?;
                            if value_compare(&v, &iv).is_ok_and(|c| c == 0) {
                                found = true;
                                break;
                            }
                        }
                        found
                    }
                    InList::Subquery(subquery) => {
                        let v = self.eval(expr)?;
                        let rows = self.run_subquery_rows(subquery)?;
                        rows.into_iter().any(|row| {
                            row.first().map(|cell| {
                                let iv = Value::from_bytes(cell.as_ref().map(|b| b.as_slice()));
                                value_compare(&v, &iv).is_ok_and(|c| c == 0)
                            }).unwrap_or(false)
                        })
                    }
                };
                Ok(Value::Bool(if *negated { !found } else { found }))
            }

            Expr::Exists { subquery, negated } => {
                let rows = self.run_subquery_rows(subquery)?;
                let exists = !rows.is_empty();
                Ok(Value::Bool(if *negated { !exists } else { exists }))
            }

            Expr::Cast { expr, ty } => {
                let v = self.eval(expr)?;
                cast_value(v, ty)
            }

            Expr::Function { name, args, distinct } => {
                let key = format!("{expr:?}");
                if let Some(v) = self.computed.as_ref().and_then(|m| m.get(&key)) {
                    return Ok(v.clone());
                }
                if is_aggregate_name(name) {
                    anyhow::bail!("aggregate function {name}() is not allowed in this context");
                }
                self.eval_function(name, args, *distinct)
            }

            Expr::Case { operand, branches, else_ } => {
                let base = operand.as_ref().map(|e| self.eval(e)).transpose()?;
                for (cond, result) in branches {
                    let cond_val = if let Some(ref b) = base {
                        let cv = self.eval(cond)?;
                        value_compare(b, &cv).is_ok_and(|c| c == 0)
                    } else {
                        self.eval(cond)?.is_truthy()
                    };
                    if cond_val {
                        return self.eval(result);
                    }
                }
                if let Some(e) = else_ {
                    self.eval(e)
                } else {
                    Ok(Value::Null)
                }
            }

            Expr::Subquery(subquery) => {
                let rows = self.run_subquery_rows(subquery)?;
                if let Some(first_row) = rows.first() {
                    if let Some(first_col) = first_row.first() {
                        return Ok(Value::from_bytes(first_col.as_ref().map(|b| b.as_slice())));
                    }
                }
                Ok(Value::Null)
            }

            Expr::Window { func, args, .. } => {
                let key = format!("{expr:?}");
                if let Some(v) = self.computed.as_ref().and_then(|m| m.get(&key)) {
                    return Ok(v.clone());
                }
                // No pre-computed window pass ran (e.g. context-free eval) — fall
                // back to evaluating the underlying function with no windowing.
                self.eval_function(func, args, false)
            }
        }
    }

    /// Run a (possibly correlated) subquery and return its raw result rows.
    fn run_subquery_rows(&self, subquery: &crate::sql::ast::SelectStmt) -> anyhow::Result<Vec<Vec<Option<Vec<u8>>>>> {
        let db = self.db.clone().ok_or_else(|| anyhow::anyhow!("subqueries require a database context"))?;
        let outer = self.outer_chain_for_subquery();
        let result = super::execute_select_with_cte(
            subquery.clone(),
            db,
            &mut super::CteContext::new(),
            &outer,
            &self.params,
        )?;
        Ok(result.rows)
    }

    fn resolve_column(&self, table_qualifier: Option<&str>, name: &str) -> anyhow::Result<Value> {
        // A qualifier that names a table/alias known to this row (or an
        // enclosing correlated row) picks that table's column directly —
        // this matters when an inner and outer table share a column name.
        if let Some(q) = table_qualifier {
            if let Some(range) = self.find_scope(q) {
                if let Some(v) = self.lookup_in_range(&self.schema, &self.values, range, name) {
                    return Ok(v);
                }
            }
            for outer_row in self.outer.iter().rev() {
                if let Some(range) = outer_row.find_scope(q) {
                    if let Some(v) = self.lookup_in_range(&outer_row.schema, &outer_row.values, range, name) {
                        return Ok(v);
                    }
                }
            }
        }

        if let Some(schema) = self.schema.as_ref() {
            if let Some(idx) = schema.columns.iter().position(|c| c.name == name) {
                return Ok(Value::from_bytes(self.values.get(idx).and_then(|v| v.as_deref())));
            }
        }
        for outer_row in self.outer.iter().rev() {
            if let Some(schema) = outer_row.schema.as_ref() {
                if let Some(idx) = schema.columns.iter().position(|c| c.name == name) {
                    return Ok(Value::from_bytes(outer_row.values.get(idx).and_then(|v| v.as_deref())));
                }
            }
        }
        if self.schema.is_none() && self.outer.is_empty() {
            anyhow::bail!("column \"{name}\" does not exist (no FROM clause)")
        } else {
            anyhow::bail!("column \"{name}\" does not exist")
        }
    }

    fn lookup_in_range(
        &self,
        schema: &Option<Arc<TableSchema>>,
        values: &[Option<Vec<u8>>],
        range: std::ops::Range<usize>,
        name: &str,
    ) -> Option<Value> {
        let schema = schema.as_ref()?;
        let cols = schema.columns.get(range.clone())?;
        let idx = cols.iter().position(|c| c.name == name)?;
        Some(Value::from_bytes(values.get(range.start + idx).and_then(|v| v.as_deref())))
    }

    fn eval_binop(&self, op: &BinOp, lhs: &Expr, rhs: &Expr) -> anyhow::Result<Value> {
        // Short-circuit AND / OR before evaluating rhs
        match op {
            BinOp::And => {
                let l = self.eval(lhs)?;
                if !l.is_truthy() { return Ok(Value::Bool(false)); }
                let r = self.eval(rhs)?;
                return Ok(Value::Bool(r.is_truthy()));
            }
            BinOp::Or => {
                let l = self.eval(lhs)?;
                if l.is_truthy() { return Ok(Value::Bool(true)); }
                let r = self.eval(rhs)?;
                return Ok(Value::Bool(r.is_truthy()));
            }
            _ => {}
        }

        let l = self.eval(lhs)?;
        let r = self.eval(rhs)?;

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
                anyhow::bail!("JSON operators are not yet supported")
            }
            BinOp::And | BinOp::Or => unreachable!(),
        }
    }

    fn eval_function(&self, name: &str, args: &[Expr], distinct: bool) -> anyhow::Result<Value> {
        let _ = distinct;
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
                let v = self.eval(args.first().ok_or_else(|| anyhow::anyhow!("abs() requires 1 argument"))?)?;
                match v {
                    Value::Int(n) => Ok(Value::Int(n.abs())),
                    Value::Float(f) => Ok(Value::Float(f.abs())),
                    _ => anyhow::bail!("abs() requires numeric argument"),
                }
            }
            "upper" => {
                let v = self.eval(args.first().ok_or_else(|| anyhow::anyhow!("upper() requires 1 argument"))?)?;
                match v {
                    Value::Text(s) => Ok(Value::Text(s.to_uppercase())),
                    _ => anyhow::bail!("upper() requires text argument"),
                }
            }
            "lower" => {
                let v = self.eval(args.first().ok_or_else(|| anyhow::anyhow!("lower() requires 1 argument"))?)?;
                match v {
                    Value::Text(s) => Ok(Value::Text(s.to_lowercase())),
                    _ => anyhow::bail!("lower() requires text argument"),
                }
            }
            "length" | "char_length" | "character_length" => {
                let v = self.eval(args.first().ok_or_else(|| anyhow::anyhow!("length() requires 1 argument"))?)?;
                match v {
                    Value::Text(s) => Ok(Value::Int(s.chars().count() as i64)),
                    _ => anyhow::bail!("length() requires text argument"),
                }
            }
            "coalesce" => {
                for arg in args {
                    let v = self.eval(arg)?;
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
                let a = self.eval(&args[0])?;
                let b = self.eval(&args[1])?;
                if value_compare(&a, &b).is_ok_and(|c| c == 0) {
                    Ok(Value::Null)
                } else {
                    Ok(a)
                }
            }
            "greatest" => {
                let mut best: Option<Value> = None;
                for arg in args {
                    let v = self.eval(arg)?;
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
                    let v = self.eval(arg)?;
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
}

/// Evaluate an expression with no row/table context (DDL defaults, INSERT
/// literal values, LIMIT/OFFSET, and other row-independent expressions).
/// Bound `$n` parameters are still honored.
pub fn eval_expr(expr: &Expr, params: &[Value]) -> anyhow::Result<Value> {
    RowContext::empty().with_params(params.to_vec()).eval(expr)
}

pub fn is_aggregate_name(name: &str) -> bool {
    matches!(name.to_lowercase().as_str(), "count" | "sum" | "avg" | "min" | "max")
}

/// Reduce an aggregate function call over a group of rows.
pub fn eval_aggregate(
    name: &str,
    arg: Option<&Expr>,
    distinct: bool,
    rows: &[Vec<Option<Vec<u8>>>],
    schema: &Option<Arc<TableSchema>>,
    db: &Option<Arc<Database>>,
) -> anyhow::Result<Value> {
    let name = name.to_lowercase();
    let ctx_for = |row: &Vec<Option<Vec<u8>>>| -> RowContext {
        match db {
            Some(d) => RowContext::with_db(row.clone(), schema.clone(), d.clone()),
            None => RowContext::new(row.clone(), schema.clone()),
        }
    };

    if name == "count" {
        let is_star = arg.is_none() || matches!(arg, Some(Expr::Ident(s)) if s == "*");
        if is_star {
            return Ok(Value::Int(rows.len() as i64));
        }
        let arg = arg.unwrap();
        let mut seen: Vec<String> = Vec::new();
        let mut count = 0i64;
        for row in rows {
            let v = ctx_for(row).eval(arg)?;
            if matches!(v, Value::Null) {
                continue;
            }
            if distinct {
                let key = format!("{v:?}");
                if seen.contains(&key) {
                    continue;
                }
                seen.push(key);
            }
            count += 1;
        }
        return Ok(Value::Int(count));
    }

    let arg = arg.ok_or_else(|| anyhow::anyhow!("{name}() requires an argument"))?;
    let mut values: Vec<Value> = Vec::new();
    let mut seen: Vec<String> = Vec::new();
    for row in rows {
        let v = ctx_for(row).eval(arg)?;
        if matches!(v, Value::Null) {
            continue;
        }
        if distinct {
            let key = format!("{v:?}");
            if seen.contains(&key) {
                continue;
            }
            seen.push(key);
        }
        values.push(v);
    }

    match name.as_str() {
        "sum" => {
            if values.is_empty() {
                return Ok(Value::Null);
            }
            let mut is_float = false;
            let mut int_sum = 0i64;
            let mut float_sum = 0f64;
            for v in &values {
                match v {
                    Value::Int(n) => { int_sum += n; float_sum += *n as f64; }
                    Value::Float(f) => { is_float = true; float_sum += f; }
                    _ => anyhow::bail!("sum() requires numeric argument"),
                }
            }
            Ok(if is_float { Value::Float(float_sum) } else { Value::Int(int_sum) })
        }
        "avg" => {
            if values.is_empty() {
                return Ok(Value::Null);
            }
            let mut total = 0f64;
            for v in &values {
                total += match v {
                    Value::Int(n) => *n as f64,
                    Value::Float(f) => *f,
                    _ => anyhow::bail!("avg() requires numeric argument"),
                };
            }
            Ok(Value::Float(total / values.len() as f64))
        }
        "min" => {
            let mut best: Option<Value> = None;
            for v in values {
                best = Some(match best {
                    None => v,
                    Some(b) => if value_compare(&v, &b).is_ok_and(|c| c < 0) { v } else { b },
                });
            }
            Ok(best.unwrap_or(Value::Null))
        }
        "max" => {
            let mut best: Option<Value> = None;
            for v in values {
                best = Some(match best {
                    None => v,
                    Some(b) => if value_compare(&v, &b).is_ok_and(|c| c > 0) { v } else { b },
                });
            }
            Ok(best.unwrap_or(Value::Null))
        }
        other => anyhow::bail!("unknown aggregate function \"{other}\""),
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
