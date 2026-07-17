use std::collections::HashMap;
use std::sync::Arc;

use crate::geo::geometry::{Coord, Geometry};
use crate::geo::raster::Raster;
use crate::sql::ast::{BinOp, Expr, InList, Literal, UnOp};
use crate::storage::database::Database;
use crate::storage::{StorageEngine, TableSchema};

/// A typed runtime value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i64),
    Float(f64),
    Text(String),
    Bool(bool),
    Null,
    /// A `float8[]` value — a contiguous vector of `f64` (little-endian on the
    /// wire). Powers in-DB array work such as a UDF receiving a signal window
    /// for FFT-style processing.
    FloatArray(Vec<f64>),
    /// A `bytea` value — an opaque byte string (hex `\x..` on the wire).
    Bytea(Vec<u8>),
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Int(_) => "int8",
            Self::Float(_) => "float8",
            Self::Text(_) => "text",
            Self::Bool(_) => "bool",
            Self::Null => "null",
            Self::FloatArray(_) => "float8[]",
            Self::Bytea(_) => "bytea",
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
            Self::FloatArray(a) => Some(
                format!("{{{}}}", a.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(","))
                    .into_bytes(),
            ),
            Self::Bytea(b) => Some(format!("\\x{}", hex::encode(b)).into_bytes()),
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
            // `float8[]` array OID (Postgres 1022); `bytea` OID is 17.
            Self::FloatArray(_) => 1022,
            Self::Bytea(_) => 17,
        }
    }

    /// Coerces a numeric `Value` to `f64`, for functions (e.g. `ST_MakePoint`)
    /// that accept either `INT` or `FLOAT` arguments interchangeably.
    pub fn as_f64(&self) -> anyhow::Result<f64> {
        match self {
            Self::Int(n) => Ok(*n as f64),
            Self::Float(f) => Ok(*f),
            other => anyhow::bail!("expected a numeric value, got {}", other.type_name()),
        }
    }

    pub fn is_truthy(&self) -> bool {
        match self {
            Self::Bool(b) => *b,
            Self::Null => false,
            Self::Int(n) => *n != 0,
            Self::Float(f) => *f != 0.0,
            Self::Text(s) => !s.is_empty(),
            Self::FloatArray(a) => !a.is_empty(),
            Self::Bytea(b) => !b.is_empty(),
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
        self.table_scopes
            .iter()
            .find(|(name, _)| name == qualifier)
            .map(|(_, r)| r.clone())
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

    pub fn with_table_scopes(
        mut self,
        table_scopes: Vec<(String, std::ops::Range<usize>)>,
    ) -> Self {
        self.table_scopes = table_scopes;
        self
    }

    fn find_scope(&self, qualifier: &str) -> Option<std::ops::Range<usize>> {
        self.table_scopes
            .iter()
            .find(|(name, _)| name == qualifier)
            .map(|(_, r)| r.clone())
    }

    pub fn new(values: Vec<Option<Vec<u8>>>, schema: Option<Arc<TableSchema>>) -> Self {
        Self {
            values,
            schema,
            ..Self::empty()
        }
    }

    pub fn with_db(
        values: Vec<Option<Vec<u8>>>,
        schema: Option<Arc<TableSchema>>,
        db: Arc<Database>,
    ) -> Self {
        Self {
            values,
            schema,
            db: Some(db),
            ..Self::empty()
        }
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
        OuterRow {
            schema: self.schema.clone(),
            values: self.values.clone(),
            table_scopes: self.table_scopes.clone(),
        }
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
                Literal::FloatArray(a) => Value::FloatArray(a.clone()),
            }),

            Expr::Ident(name) => self.resolve_column(None, name),
            Expr::QualifiedIdent(table, col) => self.resolve_column(Some(table), col),

            Expr::Param(n) => {
                let idx = (*n)
                    .checked_sub(1)
                    .ok_or_else(|| anyhow::anyhow!("invalid parameter ${n}"))?
                    as usize;
                self.params
                    .get(idx)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("parameter ${n} not bound"))
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

            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let v = self.eval(expr)?;
                let lo = self.eval(low)?;
                let hi = self.eval(high)?;
                let in_range = value_compare(&v, &lo)? >= 0 && value_compare(&v, &hi)? <= 0;
                Ok(Value::Bool(if *negated { !in_range } else { in_range }))
            }

            Expr::Like {
                expr,
                pattern,
                negated,
            } => {
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

            Expr::In {
                expr,
                list,
                negated,
            } => {
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
                            row.first()
                                .map(|cell| {
                                    let iv = Value::from_bytes(cell.as_ref().map(|b| b.as_slice()));
                                    value_compare(&v, &iv).is_ok_and(|c| c == 0)
                                })
                                .unwrap_or(false)
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

            Expr::Function {
                name,
                args,
                distinct,
            } => {
                let key = format!("{expr:?}");
                if let Some(v) = self.computed.as_ref().and_then(|m| m.get(&key)) {
                    return Ok(v.clone());
                }
                if is_aggregate_name(name) {
                    anyhow::bail!("aggregate function {name}() is not allowed in this context");
                }
                self.eval_function(name, args, *distinct)
            }

            Expr::Case {
                operand,
                branches,
                else_,
            } => {
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
    fn run_subquery_rows(
        &self,
        subquery: &crate::sql::ast::SelectStmt,
    ) -> anyhow::Result<Vec<Vec<Option<Vec<u8>>>>> {
        let db = self
            .db
            .clone()
            .ok_or_else(|| anyhow::anyhow!("subqueries require a database context"))?;
        let outer = self.outer_chain_for_subquery();
        let result = super::execute_select_with_cte(
            subquery.clone(),
            db,
            &mut super::CteContext::new(),
            &outer,
            &self.params,
            None,
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
                    if let Some(v) =
                        self.lookup_in_range(&outer_row.schema, &outer_row.values, range, name)
                    {
                        return Ok(v);
                    }
                }
            }
        }

        if let Some(schema) = self.schema.as_ref() {
            if let Some(idx) = schema.columns.iter().position(|c| c.name == name) {
                return Ok(Value::from_bytes(
                    self.values.get(idx).and_then(|v| v.as_deref()),
                ));
            }
        }
        for outer_row in self.outer.iter().rev() {
            if let Some(schema) = outer_row.schema.as_ref() {
                if let Some(idx) = schema.columns.iter().position(|c| c.name == name) {
                    return Ok(Value::from_bytes(
                        outer_row.values.get(idx).and_then(|v| v.as_deref()),
                    ));
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
        Some(Value::from_bytes(
            values.get(range.start + idx).and_then(|v| v.as_deref()),
        ))
    }

    fn eval_binop(&self, op: &BinOp, lhs: &Expr, rhs: &Expr) -> anyhow::Result<Value> {
        // Short-circuit AND / OR before evaluating rhs
        match op {
            BinOp::And => {
                let l = self.eval(lhs)?;
                if !l.is_truthy() {
                    return Ok(Value::Bool(false));
                }
                let r = self.eval(rhs)?;
                return Ok(Value::Bool(r.is_truthy()));
            }
            BinOp::Or => {
                let l = self.eval(lhs)?;
                if l.is_truthy() {
                    return Ok(Value::Bool(true));
                }
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
                _ => Ok(Value::Text(format!(
                    "{}{}",
                    val_to_text(&l),
                    val_to_text(&r)
                ))),
            },
            BinOp::Eq => Ok(Value::Bool(value_compare(&l, &r).is_ok_and(|c| c == 0))),
            BinOp::NotEq => Ok(Value::Bool(value_compare(&l, &r).is_ok_and(|c| c != 0))),
            BinOp::Lt => Ok(Value::Bool(value_compare(&l, &r).is_ok_and(|c| c < 0))),
            BinOp::Lte => Ok(Value::Bool(value_compare(&l, &r).is_ok_and(|c| c <= 0))),
            BinOp::Gt => Ok(Value::Bool(value_compare(&l, &r).is_ok_and(|c| c > 0))),
            BinOp::Gte => Ok(Value::Bool(value_compare(&l, &r).is_ok_and(|c| c >= 0))),
            BinOp::Arrow => json_arrow(&l, &r, false),
            BinOp::LongArrow => json_arrow(&l, &r, true),
            BinOp::HashArrow => json_path_arrow(&l, &r, false),
            BinOp::HashLongArrow => json_path_arrow(&l, &r, true),
            BinOp::Contains => {
                let a = json_of(&l)?;
                let b = json_of(&r)?;
                Ok(Value::Bool(json_contains(&a, &b)))
            }
            BinOp::RegexMatch
            | BinOp::RegexNotMatch
            | BinOp::RegexMatchCI
            | BinOp::RegexNotMatchCI => match (&l, &r) {
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                _ => {
                    let subject = val_to_text(&l);
                    let pattern = val_to_text(&r);
                    let ci = matches!(op, BinOp::RegexMatchCI | BinOp::RegexNotMatchCI);
                    let negated = matches!(op, BinOp::RegexNotMatch | BinOp::RegexNotMatchCI);
                    let matched = regex_is_match(&subject, &pattern, ci)?;
                    Ok(Value::Bool(if negated { !matched } else { matched }))
                }
            },
            BinOp::And | BinOp::Or => unreachable!(),
        }
    }

    /// Evaluate a `nextval`/`currval`/`setval` sequence-name argument. A
    /// trailing `::regclass` cast (real `pg_dump` output writes
    /// `nextval('foo_seq'::regclass)`) is handled transparently by
    /// `cast_value`'s regclass pass-through — nothing special needed here.
    fn eval_sequence_name_arg(&self, args: &[Expr], fn_name: &str) -> anyhow::Result<String> {
        let arg = args
            .first()
            .ok_or_else(|| anyhow::anyhow!("{fn_name}() requires a sequence name argument"))?;
        match self.eval(arg)? {
            Value::Text(s) => Ok(s),
            other => anyhow::bail!(
                "{fn_name}() requires a text sequence name, got {}",
                other.type_name()
            ),
        }
    }

    /// Evaluates `expr` and parses it as WKT (or SRID-prefixed EWKT)
    /// geometry text — the common argument shape for every `ST_*` function.
    /// Any `SRID=<n>;` prefix is stripped and discarded here; use
    /// `eval_geom_srid` when the SRID itself is needed.
    fn eval_geom(&self, expr: &Expr) -> anyhow::Result<Geometry> {
        self.eval_geom_srid(expr).map(|(g, _)| g)
    }

    /// Like `eval_geom`, but also returns the geometry's SRID (`None` if the
    /// text had no `SRID=<n>;` prefix — PostGIS's SRID 0/"unspecified").
    fn eval_geom_srid(&self, expr: &Expr) -> anyhow::Result<(Geometry, Option<i32>)> {
        match self.eval(expr)? {
            Value::Text(s) => Geometry::from_ewkt(&s),
            other => anyhow::bail!("expected geometry (WKT text), got {}", other.type_name()),
        }
    }

    /// Evaluates `expr` and parses it as hex-encoded raster bytes — same
    /// "no new `Value` variant, reuse `Value::Text`" precedent as
    /// `eval_geom`/WKB (see `storage::ColumnType::Raster`'s doc comment).
    fn eval_raster(&self, expr: &Expr) -> anyhow::Result<Raster> {
        match self.eval(expr)? {
            Value::Text(s) => Raster::from_hex(&s),
            other => anyhow::bail!("expected raster (hex text), got {}", other.type_name()),
        }
    }

    /// Evaluates `expr` and parses it as `[1.0,2.0,...]` vector literal text
    /// — same "no new `Value` variant, reuse `Value::Text`" precedent as
    /// `eval_geom`/WKT (see `storage::ColumnType::Vector`'s doc comment).
    fn eval_vector(&self, expr: &Expr) -> anyhow::Result<crate::vector::vector::Vector> {
        match self.eval(expr)? {
            Value::Text(s) => crate::vector::vector::Vector::from_text(&s),
            other => anyhow::bail!(
                "expected vector (\"[1.0,2.0,...]\" text), got {}",
                other.type_name()
            ),
        }
    }

    fn eval_function(&self, name: &str, args: &[Expr], distinct: bool) -> anyhow::Result<Value> {
        let _ = distinct;
        // Strip a `pg_catalog.` schema qualifier so `pg_catalog.pg_get_userbyid(...)`
        // resolves to the same built-in as a bare `pg_get_userbyid(...)`.
        let raw = name.to_lowercase();
        let norm = raw.strip_prefix("pg_catalog.").unwrap_or(&raw);
        match norm {
            "version" => Ok(Value::Text("TPT Keystone 0.1.0 on Rust".into())),
            "pg_sleep" => Ok(Value::Null),
            "now" | "current_timestamp" => Ok(Value::Text("2026-06-30 00:00:00+00".into())),
            "current_date" => Ok(Value::Text("2026-06-30".into())),
            "current_user" | "user" | "session_user" => Ok(Value::Text("postgres".into())),
            "current_database" | "current_catalog" => Ok(Value::Text("postgres".into())),
            "current_schema" | "current_schemas" => Ok(Value::Text("public".into())),
            "pg_backend_pid" => Ok(Value::Int(1)),
            "pg_postmaster_start_time" => Ok(Value::Text("2026-06-30 00:00:00+00".into())),
            // psql `\dt` / `\d` call `pg_catalog.pg_get_userbyid(relowner)` to
            // label a relation's owner. The engine doesn't track per-table
            // owners, so every relation is owned by the bootstrap role — `tpt`
            // (consistent with `pg_tables.tableowner`).
            "pg_get_userbyid" => Ok(Value::Text("tpt".into())),
            // `pg_table_is_visible(oid)` — true if the relation is in a schema
            // on the search path. Our search path is just `public`, so resolve
            // the OID back to a known public relation (table or index).
            "pg_table_is_visible" => {
                let db = self.db.as_ref().ok_or_else(|| {
                    anyhow::anyhow!("pg_table_is_visible() requires a database context")
                })?;
                let oid = match self.eval(args.first().ok_or_else(|| {
                    anyhow::anyhow!("pg_table_is_visible() requires 1 argument")
                })?)? {
                    Value::Int(n) => n,
                    other => anyhow::bail!(
                        "pg_table_is_visible() requires an integer oid, got {}",
                        other.type_name()
                    ),
                };
                let visible = {
                    let tables = db.list_tables().unwrap_or_default();
                    let indexes = db.list_indexes();
                    tables
                        .iter()
                        .any(|t| crate::executor::catalog::synthetic_oid(t) as i64 == oid)
                        || indexes.iter().any(|(t, c)| {
                            let idx = format!("{t}_{c}_idx");
                            crate::executor::catalog::synthetic_oid(&idx) as i64 == oid
                        })
                };
                Ok(Value::Bool(visible))
            }
            // `format_type(oid, typmod)` — psql's `\d` column-type display and
            // many other introspection queries. `typmod` (arg 2) is ignored.
            "format_type" => {
                let oid =
                    match self.eval(args.first().ok_or_else(|| {
                        anyhow::anyhow!("format_type() requires 1 or 2 arguments")
                    })?)? {
                        Value::Int(n) => n,
                        other => anyhow::bail!(
                            "format_type() requires an integer type oid, got {}",
                            other.type_name()
                        ),
                    };
                Ok(Value::Text(
                    crate::executor::catalog::pg_type_name_by_oid(oid as i32).to_string(),
                ))
            }
            "nextval" => {
                let db = self
                    .db
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("nextval() requires a database context"))?;
                let name = self.eval_sequence_name_arg(args, "nextval")?;
                Ok(Value::Int(db.nextval(&name)?))
            }
            "currval" => {
                let db = self
                    .db
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("currval() requires a database context"))?;
                let name = self.eval_sequence_name_arg(args, "currval")?;
                Ok(Value::Int(db.currval(&name)?))
            }
            "setval" => {
                let db = self
                    .db
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("setval() requires a database context"))?;
                if args.len() < 2 || args.len() > 3 {
                    anyhow::bail!("setval() requires 2 or 3 arguments");
                }
                let name = self.eval_sequence_name_arg(args, "setval")?;
                let value = match self.eval(&args[1])? {
                    Value::Int(n) => n,
                    other => anyhow::bail!(
                        "setval() requires an integer value, got {}",
                        other.type_name()
                    ),
                };
                let is_called = match args.get(2) {
                    Some(e) => self.eval(e)?.is_truthy(),
                    None => true,
                };
                Ok(Value::Int(db.setval(&name, value, is_called)?))
            }
            "abs" => {
                let v = self.eval(
                    args.first()
                        .ok_or_else(|| anyhow::anyhow!("abs() requires 1 argument"))?,
                )?;
                match v {
                    Value::Int(n) => Ok(Value::Int(n.abs())),
                    Value::Float(f) => Ok(Value::Float(f.abs())),
                    _ => anyhow::bail!("abs() requires numeric argument"),
                }
            }
            "upper" => {
                let v = self.eval(
                    args.first()
                        .ok_or_else(|| anyhow::anyhow!("upper() requires 1 argument"))?,
                )?;
                match v {
                    Value::Text(s) => Ok(Value::Text(s.to_uppercase())),
                    _ => anyhow::bail!("upper() requires text argument"),
                }
            }
            "lower" => {
                let v = self.eval(
                    args.first()
                        .ok_or_else(|| anyhow::anyhow!("lower() requires 1 argument"))?,
                )?;
                match v {
                    Value::Text(s) => Ok(Value::Text(s.to_lowercase())),
                    _ => anyhow::bail!("lower() requires text argument"),
                }
            }
            "length" | "char_length" | "character_length" => {
                let v = self.eval(
                    args.first()
                        .ok_or_else(|| anyhow::anyhow!("length() requires 1 argument"))?,
                )?;
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
                    if matches!(v, Value::Null) {
                        continue;
                    }
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
                    if matches!(v, Value::Null) {
                        continue;
                    }
                    best = Some(match best {
                        None => v,
                        Some(ref b) if value_compare(&v, b).is_ok_and(|c| c < 0) => v,
                        Some(b) => b,
                    });
                }
                Ok(best.unwrap_or(Value::Null))
            }
            // Meridian: geospatial scalar functions (`2meridianspec.txt`).
            // Geometry values are represented as WKT text (`Value::Text`) —
            // see `geo::geometry` — so these are ordinary functions from
            // the evaluator's point of view, not a distinct value variant.
            "st_makepoint" | "st_point" => {
                if args.len() < 2 || args.len() > 4 {
                    anyhow::bail!("{name}() requires 2-4 arguments (x, y, [z], [t])");
                }
                let nums = args
                    .iter()
                    .map(|a| self.eval(a)?.as_f64())
                    .collect::<anyhow::Result<Vec<f64>>>()?;
                let coord = Coord {
                    x: nums[0],
                    y: nums[1],
                    z: nums.get(2).copied(),
                    t: nums.get(3).map(|t| *t as i64),
                };
                Ok(Value::Text(Geometry::Point(coord).to_wkt()))
            }
            // ST_GeomFromText(wkt [, srid]) / ST_GeographyFromText(wkt [, srid])
            // — the optional 2nd-arg SRID overrides any `SRID=`-prefix already
            // embedded in the first argument's text (matching PostGIS). The
            // result is EWKT (`SRID=<n>;...`) whenever a SRID is known, so it
            // round-trips through `eval_geom_srid`/`ST_SRID` unchanged.
            "st_geomfromtext" | "st_geographyfromtext" | "st_geomfromewkt" => {
                if args.is_empty() || args.len() > 2 {
                    anyhow::bail!("{name}() requires 1-2 arguments (wkt, [srid])");
                }
                let (geom, mut srid) = self.eval_geom_srid(&args[0])?;
                if let Some(srid_arg) = args.get(1) {
                    srid = Some(self.eval(srid_arg)?.as_f64()? as i32);
                }
                Ok(Value::Text(geom.to_ewkt(srid)))
            }
            "st_astext" => {
                let geom = self.eval_geom(
                    args.first()
                        .ok_or_else(|| anyhow::anyhow!("st_astext() requires 1 argument"))?,
                )?;
                Ok(Value::Text(geom.to_wkt()))
            }
            "st_asewkt" => {
                let (geom, srid) = self.eval_geom_srid(
                    args.first()
                        .ok_or_else(|| anyhow::anyhow!("st_asewkt() requires 1 argument"))?,
                )?;
                Ok(Value::Text(geom.to_ewkt(srid)))
            }
            "st_srid" => {
                let (_, srid) = self.eval_geom_srid(
                    args.first()
                        .ok_or_else(|| anyhow::anyhow!("st_srid() requires 1 argument"))?,
                )?;
                Ok(Value::Int(srid.unwrap_or(0) as i64))
            }
            "st_setsrid" => {
                if args.len() != 2 {
                    anyhow::bail!("st_setsrid() requires 2 arguments (geom, srid)");
                }
                let (geom, _) = self.eval_geom_srid(&args[0])?;
                let srid = self.eval(&args[1])?.as_f64()? as i32;
                Ok(Value::Text(geom.to_ewkt(Some(srid))))
            }
            // ST_Transform: only the EPSG:4326<->3857 pair is implemented
            // (see `geo::geometry::transform_geometry`'s doc comment) — no
            // general CRS reprojection library is in scope here.
            "st_transform" => {
                if args.len() != 2 {
                    anyhow::bail!("st_transform() requires 2 arguments (geom, target_srid)");
                }
                let (geom, srid) = self.eval_geom_srid(&args[0])?;
                let from_srid = srid.ok_or_else(|| {
                    anyhow::anyhow!("st_transform() requires a geometry with a known SRID")
                })?;
                let to_srid = self.eval(&args[1])?.as_f64()? as i32;
                let transformed =
                    crate::geo::geometry::transform_geometry(&geom, from_srid, to_srid)?;
                Ok(Value::Text(transformed.to_ewkt(Some(to_srid))))
            }
            // WKB/EWKB binary I/O, surfaced as hex text (see `geo::geometry`
            // module doc comment: no dedicated binary `Value` variant here,
            // same "reuse Value::Text" precedent as WKT).
            "st_asbinary" => {
                let geom = self.eval_geom(
                    args.first()
                        .ok_or_else(|| anyhow::anyhow!("st_asbinary() requires 1 argument"))?,
                )?;
                Ok(Value::Text(geom.to_wkb_hex(None)))
            }
            "st_asewkb" => {
                let (geom, srid) = self.eval_geom_srid(
                    args.first()
                        .ok_or_else(|| anyhow::anyhow!("st_asewkb() requires 1 argument"))?,
                )?;
                Ok(Value::Text(geom.to_wkb_hex(srid)))
            }
            "st_geomfromwkb" | "st_geomfromewkb" => {
                let hex = match self.eval(
                    args.first()
                        .ok_or_else(|| anyhow::anyhow!("{name}() requires 1 argument"))?,
                )? {
                    Value::Text(s) => s,
                    other => anyhow::bail!("{name}() requires hex text, got {}", other.type_name()),
                };
                let (geom, srid) = Geometry::from_wkb_hex(&hex)?;
                Ok(Value::Text(geom.to_ewkt(srid)))
            }
            "st_x" | "st_y" | "st_z" => {
                let geom = self.eval_geom(
                    args.first()
                        .ok_or_else(|| anyhow::anyhow!("{name}() requires 1 argument"))?,
                )?;
                let c = geom.representative_point();
                match name.to_ascii_lowercase().as_str() {
                    "st_x" => Ok(Value::Float(c.x)),
                    "st_y" => Ok(Value::Float(c.y)),
                    _ => c.z.map(Value::Float).map_or(Ok(Value::Null), Ok),
                }
            }
            "st_t" | "st_time" => {
                let geom = self.eval_geom(
                    args.first()
                        .ok_or_else(|| anyhow::anyhow!("{name}() requires 1 argument"))?,
                )?;
                Ok(geom
                    .representative_point()
                    .t
                    .map(Value::Int)
                    .unwrap_or(Value::Null))
            }
            "st_distance" => {
                if args.len() != 2 {
                    anyhow::bail!("st_distance() requires 2 arguments");
                }
                let a = self.eval_geom(&args[0])?.representative_point();
                let b = self.eval_geom(&args[1])?.representative_point();
                Ok(Value::Float(crate::geo::geometry::haversine_distance_m(
                    a.x, a.y, b.x, b.y,
                )))
            }
            "st_dwithin" => {
                if args.len() != 3 {
                    anyhow::bail!("st_dwithin() requires 3 arguments (geom, geom, radius_meters)");
                }
                let a = self.eval_geom(&args[0])?.representative_point();
                let b = self.eval_geom(&args[1])?.representative_point();
                let radius = self.eval(&args[2])?.as_f64()?;
                let dist = crate::geo::geometry::haversine_distance_m(a.x, a.y, b.x, b.y);
                Ok(Value::Bool(dist <= radius))
            }
            "st_within" | "st_contains" => {
                if args.len() != 2 {
                    anyhow::bail!("{name}() requires 2 arguments");
                }
                // ST_Within(point, polygon) vs ST_Contains(polygon, point) —
                // same underlying test, arguments swapped.
                let (point_arg, poly_arg) = if name.eq_ignore_ascii_case("st_within") {
                    (&args[0], &args[1])
                } else {
                    (&args[1], &args[0])
                };
                let point = self.eval_geom(point_arg)?.representative_point();
                let poly = self.eval_geom(poly_arg)?;
                let Geometry::Polygon(rings) = &poly else {
                    anyhow::bail!("{name}() requires a POLYGON argument");
                };
                let exterior = rings.first().map(|r| r.as_slice()).unwrap_or(&[]);
                Ok(Value::Bool(crate::geo::geometry::point_in_polygon(
                    point.x, point.y, exterior,
                )))
            }
            "st_intersects" => {
                if args.len() != 2 {
                    anyhow::bail!("st_intersects() requires 2 arguments");
                }
                let a = self.eval_geom(&args[0])?;
                let b = self.eval_geom(&args[1])?;
                Ok(Value::Bool(crate::geo::geometry::bbox_intersects(
                    &a.bbox(),
                    &b.bbox(),
                )))
            }
            "st_length" => {
                let geom = self.eval_geom(
                    args.first()
                        .ok_or_else(|| anyhow::anyhow!("st_length() requires 1 argument"))?,
                )?;
                let Geometry::LineString(pts) = &geom else {
                    anyhow::bail!("st_length() requires a LINESTRING argument");
                };
                let total = pts
                    .windows(2)
                    .map(|w| {
                        crate::geo::geometry::haversine_distance_m(w[0].x, w[0].y, w[1].x, w[1].y)
                    })
                    .sum();
                Ok(Value::Float(total))
            }
            "st_area" => {
                // Planar shoelace formula in raw coordinate units (degrees^2
                // if the geometry is lon/lat) — a documented simplification,
                // not a geodesic ellipsoidal area calculation.
                let geom = self.eval_geom(
                    args.first()
                        .ok_or_else(|| anyhow::anyhow!("st_area() requires 1 argument"))?,
                )?;
                let Geometry::Polygon(rings) = &geom else {
                    anyhow::bail!("st_area() requires a POLYGON argument");
                };
                let ring = rings.first().map(|r| r.as_slice()).unwrap_or(&[]);
                let mut sum = 0.0;
                for w in ring.windows(2) {
                    sum += w[0].x * w[1].y - w[1].x * w[0].y;
                }
                if let (Some(first), Some(last)) = (ring.first(), ring.last()) {
                    sum += last.x * first.y - first.x * last.y;
                }
                Ok(Value::Float((sum / 2.0).abs()))
            }
            // --- OGC Simple Features scalar functions not previously wired
            // (added while building the OGC conformance test suite, see
            // `executor/ogc_conformance_tests.rs`).
            "st_geometrytype" => {
                let geom = self
                    .eval_geom(args.first().ok_or_else(|| {
                        anyhow::anyhow!("st_geometrytype() requires 1 argument")
                    })?)?;
                Ok(Value::Text(
                    match geom {
                        Geometry::Point(_) => "ST_Point",
                        Geometry::LineString(_) => "ST_LineString",
                        Geometry::Polygon(_) => "ST_Polygon",
                    }
                    .to_string(),
                ))
            }
            // 0 for a point, 1 for a curve/line, 2 for a surface/polygon —
            // the OGC SFA topological dimension of the geometry's type
            // (not the coordinate dimension `Z`/`M` adds).
            "st_dimension" => {
                let geom = self.eval_geom(
                    args.first()
                        .ok_or_else(|| anyhow::anyhow!("st_dimension() requires 1 argument"))?,
                )?;
                Ok(Value::Int(match geom {
                    Geometry::Point(_) => 0,
                    Geometry::LineString(_) => 1,
                    Geometry::Polygon(_) => 2,
                }))
            }
            // True for a LINESTRING with no points or a POLYGON with no
            // rings. A POINT is never empty in this model (`Coord` always
            // carries x/y) — matches the OGC spec's own allowance that
            // emptiness is type-dependent.
            "st_isempty" => {
                let geom = self.eval_geom(
                    args.first()
                        .ok_or_else(|| anyhow::anyhow!("st_isempty() requires 1 argument"))?,
                )?;
                Ok(Value::Bool(match &geom {
                    Geometry::Point(_) => false,
                    Geometry::LineString(pts) => pts.is_empty(),
                    Geometry::Polygon(rings) => rings.is_empty() || rings[0].is_empty(),
                }))
            }
            // The geometry's bounding box, returned as a closed WKT POLYGON
            // (matches PostGIS's `ST_Envelope`) built from the existing
            // `Geometry::bbox`/`BBox`.
            "st_envelope" => {
                let geom = self.eval_geom(
                    args.first()
                        .ok_or_else(|| anyhow::anyhow!("st_envelope() requires 1 argument"))?,
                )?;
                let b = geom.bbox();
                let ring = vec![
                    Coord::xy(b.min_x, b.min_y),
                    Coord::xy(b.max_x, b.min_y),
                    Coord::xy(b.max_x, b.max_y),
                    Coord::xy(b.min_x, b.max_y),
                    Coord::xy(b.min_x, b.min_y),
                ];
                Ok(Value::Text(Geometry::Polygon(vec![ring]).to_wkt()))
            }
            // Exact coordinate-sequence equality, not full OGC spatial
            // equivalence (PostGIS's `ST_Equals` treats two polygons with the
            // same shape but different ring-start-vertex/winding as equal;
            // this doesn't) — a documented simplification, same discipline as
            // `ST_Intersects`'s bbox-only precision.
            "st_equals" => {
                if args.len() != 2 {
                    anyhow::bail!("st_equals() requires 2 arguments");
                }
                let a = self.eval_geom(&args[0])?;
                let b = self.eval_geom(&args[1])?;
                Ok(Value::Bool(a == b))
            }
            // --- Raster (`geo::raster::Raster`) — see that module's doc
            // comment for exact scope (single f64 band, hex-text storage).
            "st_makeemptyraster" => {
                if args.len() != 6 && args.len() != 7 {
                    anyhow::bail!(
                        "st_makeemptyraster() requires 6-7 arguments (width, height, upperleftx, upperlefty, scalex, scaley, [srid])"
                    );
                }
                let width = self.eval(&args[0])?.as_f64()? as u32;
                let height = self.eval(&args[1])?.as_f64()? as u32;
                let ulx = self.eval(&args[2])?.as_f64()?;
                let uly = self.eval(&args[3])?.as_f64()?;
                let sx = self.eval(&args[4])?.as_f64()?;
                let sy = self.eval(&args[5])?.as_f64()?;
                let srid = match args.get(6) {
                    Some(e) => self.eval(e)?.as_f64()? as i32,
                    None => 0,
                };
                Ok(Value::Text(
                    Raster::new_empty(width, height, ulx, uly, sx, sy, srid).to_hex(),
                ))
            }
            "st_value" => {
                if args.len() != 3 {
                    anyhow::bail!("st_value() requires 3 arguments (raster, x, y)");
                }
                let r = self.eval_raster(&args[0])?;
                let x = self.eval(&args[1])?.as_f64()? as u32;
                let y = self.eval(&args[2])?.as_f64()? as u32;
                Ok(r.value(x, y).map(Value::Float).unwrap_or(Value::Null))
            }
            "st_setvalue" => {
                if args.len() != 4 {
                    anyhow::bail!("st_setvalue() requires 4 arguments (raster, x, y, value)");
                }
                let mut r = self.eval_raster(&args[0])?;
                let x = self.eval(&args[1])?.as_f64()? as u32;
                let y = self.eval(&args[2])?.as_f64()? as u32;
                let v = self.eval(&args[3])?.as_f64()?;
                r.set_value(x, y, v)?;
                Ok(Value::Text(r.to_hex()))
            }
            "st_width" => Ok(Value::Int(
                self.eval_raster(
                    args.first()
                        .ok_or_else(|| anyhow::anyhow!("st_width() requires 1 argument"))?,
                )?
                .width as i64,
            )),
            "st_height" => Ok(Value::Int(
                self.eval_raster(
                    args.first()
                        .ok_or_else(|| anyhow::anyhow!("st_height() requires 1 argument"))?,
                )?
                .height as i64,
            )),
            // Rasterizes a geometry (point/linestring/polygon) into a new
            // raster at the given pixel scale — see `Raster::rasterize`.
            "st_asraster" => {
                if args.len() < 3 || args.len() > 4 {
                    anyhow::bail!(
                        "st_asraster() requires 3-4 arguments (geom, scalex, scaley, [value])"
                    );
                }
                let (geom, srid) = self.eval_geom_srid(&args[0])?;
                let scale_x = self.eval(&args[1])?.as_f64()?;
                let scale_y = self.eval(&args[2])?.as_f64()?;
                let value = match args.get(3) {
                    Some(e) => self.eval(e)?.as_f64()?,
                    None => 1.0,
                };
                let raster = Raster::rasterize(&geom, scale_x, scale_y, value, srid.unwrap_or(0))?;
                Ok(Value::Text(raster.to_hex()))
            }
            // Prism: vector distance/similarity scalar functions (`3prismspec.txt`).
            // Vectors are `Value::Text` holding `"[1.0,2.0,...]"`, same
            // as Meridian's WKT-as-text `Geometry` precedent above — see
            // `eval_vector`/`storage::ColumnType::Vector`'s doc comments.
            "l2_distance" | "vector_l2_distance" => {
                if args.len() != 2 {
                    anyhow::bail!("{name}() requires 2 arguments");
                }
                let a = self.eval_vector(&args[0])?;
                let b = self.eval_vector(&args[1])?;
                Ok(Value::Float(
                    crate::vector::vector::l2_distance(a.as_slice(), b.as_slice())? as f64,
                ))
            }
            "cosine_distance" | "vector_cosine_distance" => {
                if args.len() != 2 {
                    anyhow::bail!("{name}() requires 2 arguments");
                }
                let a = self.eval_vector(&args[0])?;
                let b = self.eval_vector(&args[1])?;
                Ok(Value::Float(
                    crate::vector::vector::cosine_distance(a.as_slice(), b.as_slice())? as f64,
                ))
            }
            "cosine_similarity" | "vector_cosine_similarity" => {
                if args.len() != 2 {
                    anyhow::bail!("{name}() requires 2 arguments");
                }
                let a = self.eval_vector(&args[0])?;
                let b = self.eval_vector(&args[1])?;
                Ok(Value::Float(crate::vector::vector::cosine_similarity(
                    a.as_slice(),
                    b.as_slice(),
                )? as f64))
            }
            "dot_product" | "vector_dot_product" | "inner_product" => {
                if args.len() != 2 {
                    anyhow::bail!("{name}() requires 2 arguments");
                }
                let a = self.eval_vector(&args[0])?;
                let b = self.eval_vector(&args[1])?;
                Ok(Value::Float(
                    crate::vector::vector::dot_product(a.as_slice(), b.as_slice())? as f64,
                ))
            }
            "vector_dims" => {
                let v = self.eval_vector(
                    args.first()
                        .ok_or_else(|| anyhow::anyhow!("vector_dims() requires 1 argument"))?,
                )?;
                Ok(Value::Int(v.dim() as i64))
            }
            // Chronos: SQL time extensions (`8chronos` phase). Timestamps
            // are plain `Value::Int` unix-millisecond values (no dedicated
            // `Value::Timestamp`/`Interval` variant exists in this engine —
            // see module docs), and interval literals are hand-parsed text
            // like `'1 hour'` rather than a real `INTERVAL` AST type.
            "time_bucket" => {
                if args.len() != 2 {
                    anyhow::bail!("time_bucket() requires 2 arguments (interval, timestamp)");
                }
                let interval_ms = self.eval_interval_arg(&args[0], "time_bucket")?;
                let ts = self.eval_timestamp_arg(&args[1], "time_bucket")?;
                Ok(Value::Int(ts.div_euclid(interval_ms) * interval_ms))
            }
            // Canopy (Phase 10): JSON/JSONB functions. `json_*`/`jsonb_*`
            // names are accepted interchangeably — this engine has no
            // separate binary-jsonb-vs-text-json distinction at the `Value`
            // layer (see the module note above `json_of`).
            "json_typeof" | "jsonb_typeof" => {
                let doc = json_of(
                    &self
                        .eval(args.first().ok_or_else(|| {
                            anyhow::anyhow!("json_typeof() requires 1 argument")
                        })?)?,
                )?;
                Ok(Value::Text(
                    match doc {
                        serde_json::Value::Null => "null",
                        serde_json::Value::Bool(_) => "boolean",
                        serde_json::Value::Number(_) => "number",
                        serde_json::Value::String(_) => "string",
                        serde_json::Value::Array(_) => "array",
                        serde_json::Value::Object(_) => "object",
                    }
                    .to_string(),
                ))
            }
            "json_valid" | "jsonb_valid" => {
                let v = self.eval(
                    args.first()
                        .ok_or_else(|| anyhow::anyhow!("json_valid() requires 1 argument"))?,
                )?;
                Ok(Value::Bool(match v {
                    Value::Text(s) => serde_json::from_str::<serde_json::Value>(&s).is_ok(),
                    Value::Null => true,
                    _ => false,
                }))
            }
            "json_array_length" | "jsonb_array_length" => {
                let doc = json_of(&self.eval(args.first().ok_or_else(|| {
                    anyhow::anyhow!("json_array_length() requires 1 argument")
                })?)?)?;
                match doc {
                    serde_json::Value::Array(a) => Ok(Value::Int(a.len() as i64)),
                    _ => anyhow::bail!("json_array_length() requires a JSON array"),
                }
            }
            "json_extract_path"
            | "jsonb_extract_path"
            | "json_extract_path_text"
            | "jsonb_extract_path_text" => {
                let as_text = name.to_lowercase().ends_with("_text");
                let doc =
                    json_of(&self.eval(args.first().ok_or_else(|| {
                        anyhow::anyhow!("{name}() requires at least 1 argument")
                    })?)?)?;
                let mut path = Vec::with_capacity(args.len().saturating_sub(1));
                for a in &args[1..] {
                    match self.eval(a)? {
                        Value::Text(s) => path.push(s),
                        other => anyhow::bail!(
                            "{name}(): expected a text path segment, got {}",
                            other.type_name()
                        ),
                    }
                }
                Ok(json_to_value(json_at_path(&doc, &path), as_text))
            }
            "jsonb_set" | "json_set" => {
                if args.len() < 3 || args.len() > 4 {
                    anyhow::bail!("jsonb_set() requires 3 or 4 arguments (target, path, new_value, [create_missing])");
                }
                let mut doc = json_of(&self.eval(&args[0])?)?;
                let path_text = match self.eval(&args[1])? {
                    Value::Text(s) => s,
                    other => anyhow::bail!(
                        "jsonb_set(): expected a '{{a,b,c}}' path literal, got {}",
                        other.type_name()
                    ),
                };
                let new_value = json_of(&self.eval(&args[2])?)?;
                let create_missing = match args.get(3) {
                    Some(e) => self.eval(e)?.is_truthy(),
                    None => true,
                };
                json_set_path(
                    &mut doc,
                    &parse_path_literal(&path_text),
                    new_value,
                    create_missing,
                );
                Ok(Value::Text(doc.to_string()))
            }
            "jsonb_contains" | "json_contains" => {
                if args.len() != 2 {
                    anyhow::bail!("jsonb_contains() requires 2 arguments");
                }
                let a = json_of(&self.eval(&args[0])?)?;
                let b = json_of(&self.eval(&args[1])?)?;
                Ok(Value::Bool(json_contains(&a, &b)))
            }
            "jsonb_build_object" | "json_build_object" => {
                if args.len() % 2 != 0 {
                    anyhow::bail!(
                        "{name}() requires an even number of arguments (key, value, ...)"
                    );
                }
                let mut map = serde_json::Map::new();
                for pair in args.chunks(2) {
                    let key = match self.eval(&pair[0])? {
                        Value::Text(s) => s,
                        other => anyhow::bail!(
                            "{name}(): expected a text key, got {}",
                            other.type_name()
                        ),
                    };
                    map.insert(key, value_to_json(&self.eval(&pair[1])?));
                }
                Ok(Value::Text(serde_json::Value::Object(map).to_string()))
            }
            "jsonb_build_array" | "json_build_array" => {
                let items: Vec<serde_json::Value> = args
                    .iter()
                    .map(|a| Ok(value_to_json(&self.eval(a)?)))
                    .collect::<anyhow::Result<_>>()?;
                Ok(Value::Text(serde_json::Value::Array(items).to_string()))
            }
            "to_json" | "to_jsonb" => {
                let v = self.eval(
                    args.first()
                        .ok_or_else(|| anyhow::anyhow!("{name}() requires 1 argument"))?,
                )?;
                Ok(Value::Text(value_to_json(&v).to_string()))
            }
            other => {
                let Some(db) = &self.db else {
                    anyhow::bail!("function \"{other}\" does not exist")
                };
                let Some(uf) = db.get_function(other) else {
                    anyhow::bail!("function \"{other}\" does not exist")
                };
                let arg_vals: Vec<Value> = args
                    .iter()
                    .map(|a| self.eval(a))
                    .collect::<anyhow::Result<_>>()?;
                super::udf::call(db.udf_config(), &uf, &arg_vals)
            }
        }
    }

    fn eval_interval_arg(&self, expr: &Expr, fname: &str) -> anyhow::Result<i64> {
        match self.eval(expr)? {
            Value::Text(s) => parse_interval(&s)
                .ok_or_else(|| anyhow::anyhow!("{fname}(): invalid interval literal {s:?}")),
            other => anyhow::bail!(
                "{fname}(): expected an interval string literal, got {}",
                other.type_name()
            ),
        }
    }

    fn eval_timestamp_arg(&self, expr: &Expr, fname: &str) -> anyhow::Result<i64> {
        match self.eval(expr)? {
            Value::Int(n) => Ok(n),
            Value::Float(f) => Ok(f as i64),
            other => anyhow::bail!(
                "{fname}(): expected a timestamp (unix-ms integer), got {}",
                other.type_name()
            ),
        }
    }
}

/// Parses a Chronos interval literal (`'<n> <unit>'`, e.g. `'1 hour'`,
/// `'30 days'`) into milliseconds. Shared by `time_bucket()` and
/// `CREATE INDEX ... USING TIME WITH (interval = ..., retention = ...)`
/// DDL parsing (`executor::execute_create_index`) — this engine has no real
/// `INTERVAL` type/arithmetic, so both paths go through this one parser
/// instead.
pub fn parse_interval(s: &str) -> Option<i64> {
    let s = s.trim();
    let split_at = s.find(|c: char| !c.is_ascii_digit())?;
    let (num_str, unit) = s.split_at(split_at);
    let n: i64 = num_str.trim().parse().ok()?;
    let unit = unit.trim().trim_end_matches('s').to_ascii_lowercase();
    let unit_ms: i64 = match unit.as_str() {
        "ms" | "millisecond" => 1,
        "s" | "sec" | "second" => 1_000,
        "m" | "min" | "minute" => 60_000,
        "h" | "hr" | "hour" => 3_600_000,
        "d" | "day" => 86_400_000,
        "w" | "week" => 7 * 86_400_000,
        // Not a true calendar month — a documented approximation, same
        // caveat any fixed-ms "month" bucketing carries.
        "mon" | "month" => 30 * 86_400_000,
        _ => return None,
    };
    Some(n * unit_ms)
}

/// Evaluate an expression with no row/table context (DDL defaults, INSERT
/// literal values, LIMIT/OFFSET, and other row-independent expressions).
/// Bound `$n` parameters are still honored.
pub fn eval_expr(expr: &Expr, params: &[Value]) -> anyhow::Result<Value> {
    RowContext::empty().with_params(params.to_vec()).eval(expr)
}

/// Like `eval_expr`, but with `db` available — needed for a column DEFAULT
/// like `nextval('seq')`, which is the one row-independent expression that
/// still needs write access to the database.
pub fn eval_expr_with_db(
    expr: &Expr,
    db: Arc<Database>,
    params: &[Value],
) -> anyhow::Result<Value> {
    let mut ctx = RowContext::empty().with_params(params.to_vec());
    ctx.db = Some(db);
    ctx.eval(expr)
}

pub fn is_aggregate_name(name: &str) -> bool {
    matches!(
        name.to_lowercase().as_str(),
        "count" | "sum" | "avg" | "min" | "max"
    )
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
                    Value::Int(n) => {
                        int_sum += n;
                        float_sum += *n as f64;
                    }
                    Value::Float(f) => {
                        is_float = true;
                        float_sum += f;
                    }
                    _ => anyhow::bail!("sum() requires numeric argument"),
                }
            }
            Ok(if is_float {
                Value::Float(float_sum)
            } else {
                Value::Int(int_sum)
            })
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
                    Some(b) => {
                        if value_compare(&v, &b).is_ok_and(|c| c < 0) {
                            v
                        } else {
                            b
                        }
                    }
                });
            }
            Ok(best.unwrap_or(Value::Null))
        }
        "max" => {
            let mut best: Option<Value> = None;
            for v in values {
                best = Some(match best {
                    None => v,
                    Some(b) => {
                        if value_compare(&v, &b).is_ok_and(|c| c > 0) {
                            v
                        } else {
                            b
                        }
                    }
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
                BinOp::Add => a
                    .checked_add(*b)
                    .ok_or_else(|| anyhow::anyhow!("integer overflow"))?,
                BinOp::Sub => a
                    .checked_sub(*b)
                    .ok_or_else(|| anyhow::anyhow!("integer overflow"))?,
                BinOp::Mul => a
                    .checked_mul(*b)
                    .ok_or_else(|| anyhow::anyhow!("integer overflow"))?,
                BinOp::Div => {
                    if *b == 0 {
                        anyhow::bail!("division by zero");
                    }
                    a / b
                }
                BinOp::Mod => {
                    if *b == 0 {
                        anyhow::bail!("division by zero");
                    }
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
                    if b == 0.0 {
                        anyhow::bail!("division by zero");
                    }
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
        Value::Text(s) => s
            .parse::<f64>()
            .map_err(|_| anyhow::anyhow!("cannot cast text to float")),
        _ => anyhow::bail!("cannot cast {} to float", v.type_name()),
    }
}

pub fn value_compare(a: &Value, b: &Value) -> anyhow::Result<i32> {
    match (a, b) {
        (Value::Null, Value::Null) => Ok(0),
        (Value::Null, _) | (_, Value::Null) => anyhow::bail!("null comparison"),
        (Value::Int(x), Value::Int(y)) => Ok(x.cmp(y) as i32),
        (Value::Float(x), Value::Float(y)) => Ok(x.partial_cmp(y).map(|o| o as i32).unwrap_or(0)),
        (Value::Int(x), Value::Float(y)) => {
            Ok((*x as f64).partial_cmp(y).map(|o| o as i32).unwrap_or(0))
        }
        (Value::Float(x), Value::Int(y)) => {
            Ok(x.partial_cmp(&(*y as f64)).map(|o| o as i32).unwrap_or(0))
        }
        (Value::Text(x), Value::Text(y)) => Ok(x.cmp(y) as i32),
        (Value::Bool(x), Value::Bool(y)) => Ok(x.cmp(y) as i32),
        _ => anyhow::bail!(
            "incompatible types for comparison: {} vs {}",
            a.type_name(),
            b.type_name()
        ),
    }
}

// --- Canopy (Phase 10): JSON operators/functions --------------------------
//
// `Value` has no dedicated JSON variant (see its doc comment) — a `Json`
// column's value is carried as `Value::Text` holding JSON text, exactly the
// pattern `Geometry` already uses for WKT text. Every JSON operator/function
// below parses that text with `serde_json` on demand and re-serializes the
// result, rather than adding a new `Value` variant that would ripple through
// `to_wire_bytes`/`type_oid`/every other `Value` match arm in this codebase.

/// Parses a `Value::Text` (or `Value::Null`, which passes through as JSON
/// `null`) as JSON. Any other `Value` variant is a type error — SQL never
/// produces a "JSON int" or "JSON bool" `Value` on its own, only text that
/// happens to parse as JSON.
fn json_of(v: &Value) -> anyhow::Result<serde_json::Value> {
    match v {
        Value::Text(s) => serde_json::from_str(s).map_err(|e| anyhow::anyhow!("invalid JSON: {e}")),
        Value::Null => Ok(serde_json::Value::Null),
        other => anyhow::bail!("expected JSON (text), got {}", other.type_name()),
    }
}

/// Converts a `serde_json::Value` back to a `Value` for the SQL layer.
/// `as_text` mirrors `->` (`false`, JSON stays JSON — a string result keeps
/// its quotes as re-serialized JSON text) vs `->>`/`#>>` (`true`, a string
/// result is unwrapped to plain text, matching Postgres's `->>` semantics).
fn json_to_value(j: &serde_json::Value, as_text: bool) -> Value {
    match j {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::String(s) if as_text => Value::Text(s.clone()),
        other => Value::Text(other.to_string()),
    }
}

/// Converts a SQL `Value` into the `serde_json::Value` it represents as a
/// JSON scalar — used by `jsonb_build_object`/`jsonb_build_array`/`to_json`.
/// `Value::Text` becomes a JSON *string* (not re-parsed as JSON) — the SQL
/// caller passed a text value, not a JSON fragment.
fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(n) => serde_json::json!(n),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Text(s) => serde_json::Value::String(s.clone()),
        Value::FloatArray(a) => serde_json::Value::Array(
            a.iter()
                .map(|x| serde_json::Number::from_f64(*x).map(serde_json::Value::Number).unwrap_or(serde_json::Value::Null))
                .collect(),
        ),
        Value::Bytea(b) => serde_json::Value::String(format!("\\x{}", hex::encode(b))),
    }
}

/// `l -> r` / `l ->> r`: object-key or array-index lookup one level deep.
fn json_arrow(l: &Value, r: &Value, as_text: bool) -> anyhow::Result<Value> {
    let doc = json_of(l)?;
    let sub = match r {
        Value::Text(key) => doc.as_object().and_then(|o| o.get(key)).cloned(),
        Value::Int(idx) => doc.as_array().and_then(|a| a.get(*idx as usize)).cloned(),
        other => anyhow::bail!("invalid JSON key/index: {}", other.type_name()),
    };
    Ok(sub
        .map(|j| json_to_value(&j, as_text))
        .unwrap_or(Value::Null))
}

/// Parses a Postgres-style path literal (`'{a,b,c}'`) into its segments.
/// Also tolerates a bare comma list without braces.
fn parse_path_literal(s: &str) -> Vec<String> {
    let s = s.trim();
    let inner = s
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .unwrap_or(s);
    if inner.is_empty() {
        Vec::new()
    } else {
        inner.split(',').map(|seg| seg.trim().to_string()).collect()
    }
}

fn json_at_path<'a>(mut current: &'a serde_json::Value, path: &[String]) -> &'a serde_json::Value {
    static NULL: serde_json::Value = serde_json::Value::Null;
    for seg in path {
        current = match current {
            serde_json::Value::Object(o) => o.get(seg).unwrap_or(&NULL),
            serde_json::Value::Array(a) => seg
                .parse::<usize>()
                .ok()
                .and_then(|i| a.get(i))
                .unwrap_or(&NULL),
            _ => &NULL,
        };
    }
    current
}

/// `l #> r` / `l #>> r`: multi-segment path extraction, `r` a `'{a,b,c}'`
/// path literal (the array-literal counterpart of `->`/`->>`'s single key).
fn json_path_arrow(l: &Value, r: &Value, as_text: bool) -> anyhow::Result<Value> {
    let doc = json_of(l)?;
    let path_text = match r {
        Value::Text(s) => s.clone(),
        other => anyhow::bail!(
            "expected a '{{a,b,c}}' path literal, got {}",
            other.type_name()
        ),
    };
    let path = parse_path_literal(&path_text);
    Ok(json_to_value(json_at_path(&doc, &path), as_text))
}

/// `l @> r`: JSONB containment — every key/value in `r` is present (and,
/// recursively, contained) in `l`; every element of an array `r` is found
/// somewhere in array `l`. Scalars must match exactly.
fn json_contains(container: &serde_json::Value, contained: &serde_json::Value) -> bool {
    use serde_json::Value::{Array, Object};
    match (container, contained) {
        (Object(a), Object(b)) => b
            .iter()
            .all(|(k, bv)| a.get(k).is_some_and(|av| json_contains(av, bv))),
        (Array(a), Array(b)) => b.iter().all(|bv| a.iter().any(|av| json_contains(av, bv))),
        (a, b) => a == b,
    }
}

/// `jsonb_set(target, '{path}', new_value, [create_missing])`: returns a new
/// document with the value at `path` replaced (or inserted, if
/// `create_missing` — default `true` — and every ancestor along the path
/// exists as an object/array).
fn json_set_path(
    doc: &mut serde_json::Value,
    path: &[String],
    new_value: serde_json::Value,
    create_missing: bool,
) {
    let Some((head, rest)) = path.split_first() else {
        *doc = new_value;
        return;
    };
    match doc {
        serde_json::Value::Object(map) => {
            if rest.is_empty() {
                if create_missing || map.contains_key(head) {
                    map.insert(head.clone(), new_value);
                }
            } else if let Some(sub) = map.get_mut(head) {
                json_set_path(sub, rest, new_value, create_missing);
            } else if create_missing {
                let mut sub = serde_json::Value::Object(Default::default());
                json_set_path(&mut sub, rest, new_value, create_missing);
                map.insert(head.clone(), sub);
            }
        }
        serde_json::Value::Array(arr) => {
            if let Ok(idx) = head.parse::<usize>() {
                if rest.is_empty() {
                    if idx < arr.len() {
                        arr[idx] = new_value;
                    } else if create_missing {
                        arr.push(new_value);
                    }
                } else if let Some(sub) = arr.get_mut(idx) {
                    json_set_path(sub, rest, new_value, create_missing);
                }
            }
        }
        _ => {}
    }
}

fn val_to_text(v: &Value) -> String {
    match v {
        Value::Int(n) => n.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Text(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Null => String::new(),
        Value::FloatArray(a) => format!(
            "{{{}}}",
            a.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(",")
        ),
        Value::Bytea(b) => format!("\\x{}", hex::encode(b)),
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
            _ => anyhow::bail!("cannot cast {} to int", v.type_name()),
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
        // `pg_dump` output routinely casts identifiers this way (e.g.
        // `nextval('foo_id_seq'::regclass)`) — we have no real OID/regclass
        // semantics, so just pass the text through unchanged.
        "regclass" | "regproc" | "regtype" => Ok(Value::Text(val_to_text(&v))),
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

/// POSIX regex match for the `~` / `!~` / `~*` / `!~*` infix operators. A
/// `null` operand yields `null` (handled by the caller); an invalid pattern
/// errors, matching Postgres (`ERROR: invalid regular expression`).
///
/// Compiled `Regex`es are memoised per-thread keyed by `(pattern, ci)` because
/// a hot `WHERE col ~ '...'` filter would otherwise re-parse the same pattern
/// on every row. `regex::Regex` is cheaply cloneable (shares an `Arc`), so the
/// cache value is retrieved by cloning rather than re-borrowing.
fn regex_is_match(subject: &str, pattern: &str, ci: bool) -> anyhow::Result<bool> {
    use std::cell::RefCell;
    thread_local! {
        static CACHE: RefCell<std::collections::HashMap<(String, bool), regex::Regex>> =
            RefCell::new(std::collections::HashMap::new());
    }
    let key = (pattern.to_string(), ci);
    let compiled = CACHE.with(|c| {
        if let Some(re) = c.borrow().get(&key) {
            return Ok(re.clone());
        }
        let mut builder = regex::RegexBuilder::new(pattern);
        builder.case_insensitive(ci);
        match builder.build() {
            Ok(re) => {
                c.borrow_mut().insert(key.clone(), re.clone());
                Ok(re)
            }
            Err(e) => Err(anyhow::anyhow!("invalid regular expression: {e}")),
        }
    })?;
    Ok(compiled.is_match(subject))
}
