//! Typed query builder — Phase 5's "AI-optimised SDK" checklist item,
//! scoped to what that phase's own note says: a schema-aware builder layered
//! on top of the existing [`keystone::KeystoneClient`], not a fourth SDK.
//! Callers can hand-implement [`Table`] for any struct, or generate it via
//! `bin/typegen.rs` from a live server's `information_schema` — either way
//! produces the exact same trait impl, so generated and hand-written tables
//! are interchangeable.
//!
//! This never blocks raw SQL: [`QueryBuilder::build`] returns a plain
//! `(String, Vec<Value>)` you can hand to `query_params` yourself, and
//! [`QueryBuilder::fetch`] is just that call wrapped for convenience.

use crate::keystone::{KeystoneClient, KeystoneError, QueryResult, Value};

/// Minimal per-table metadata the builder needs: the table name and its
/// column list, in the order `SELECT *` would return them. `typegen`
/// generates one of these per table; nothing stops a caller from writing
/// one by hand for a table typegen was never pointed at.
pub trait Table {
    const NAME: &'static str;
    const COLUMNS: &'static [&'static str];
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Order {
    Asc,
    Desc,
}

/// Builds a parameterized `SELECT` against `T` without the caller
/// hand-formatting SQL text. Every method takes `self` by value and returns
/// `Self` (builder style); the terminal step is [`build`](Self::build)
/// (just the SQL) or [`fetch`](Self::fetch) (SQL + execute over the wire).
pub struct QueryBuilder<T: Table> {
    columns: Vec<String>,
    filters: Vec<(String, Value)>,
    order_by: Option<(String, Order)>,
    limit: Option<u64>,
    offset: Option<u64>,
    _table: std::marker::PhantomData<T>,
}

impl<T: Table> Default for QueryBuilder<T> {
    fn default() -> Self {
        Self {
            columns: T::COLUMNS.iter().map(|c| c.to_string()).collect(),
            filters: Vec::new(),
            order_by: None,
            limit: None,
            offset: None,
            _table: std::marker::PhantomData,
        }
    }
}

impl<T: Table> QueryBuilder<T> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Restricts the selected columns (default: every column in `T::COLUMNS`,
    /// i.e. `SELECT *`-equivalent but with real column names, not `*`).
    pub fn select(mut self, cols: &[&str]) -> Self {
        self.columns = cols.iter().map(|c| c.to_string()).collect();
        self
    }

    /// Adds a `column = value` filter, AND-ed with any other filters already
    /// added. Values are always sent as bind parameters (`$1`, `$2`, ...),
    /// never inlined as SQL text — this is the whole point of the builder
    /// over hand-formatted strings.
    pub fn filter_eq(mut self, col: &str, value: impl Into<Value>) -> Self {
        self.filters.push((col.to_string(), value.into()));
        self
    }

    pub fn order_by(mut self, col: &str, order: Order) -> Self {
        self.order_by = Some((col.to_string(), order));
        self
    }

    pub fn limit(mut self, n: u64) -> Self {
        self.limit = Some(n);
        self
    }

    pub fn offset(mut self, n: u64) -> Self {
        self.offset = Some(n);
        self
    }

    /// Renders the built-up state into `(sql, params)` — `params[i]` binds
    /// to `${i+1}` in `sql`, matching `KeystoneClient::query_params`'s
    /// positional convention.
    pub fn build(&self) -> (String, Vec<Value>) {
        let mut sql = format!("SELECT {} FROM {}", self.columns.join(", "), T::NAME);
        let mut params = Vec::with_capacity(self.filters.len());

        if !self.filters.is_empty() {
            let clauses: Vec<String> = self
                .filters
                .iter()
                .enumerate()
                .map(|(i, (col, _))| format!("{col} = ${}", i + 1))
                .collect();
            sql.push_str(" WHERE ");
            sql.push_str(&clauses.join(" AND "));
            params.extend(self.filters.iter().map(|(_, v)| v.clone()));
        }
        if let Some((col, order)) = &self.order_by {
            let dir = match order {
                Order::Asc => "ASC",
                Order::Desc => "DESC",
            };
            sql.push_str(&format!(" ORDER BY {col} {dir}"));
        }
        if let Some(n) = self.limit {
            sql.push_str(&format!(" LIMIT {n}"));
        }
        if let Some(n) = self.offset {
            sql.push_str(&format!(" OFFSET {n}"));
        }
        (sql, params)
    }

    /// Builds and executes over `client` in one step.
    pub async fn fetch(&self, client: &mut KeystoneClient) -> Result<QueryResult, KeystoneError> {
        let (sql, params) = self.build();
        client.query_params(&sql, &params).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Users;
    impl Table for Users {
        const NAME: &'static str = "users";
        const COLUMNS: &'static [&'static str] = &["id", "name", "signed_up_at"];
    }

    #[test]
    fn build_select_star_equivalent_with_no_filters() {
        let (sql, params) = QueryBuilder::<Users>::new().build();
        assert_eq!(sql, "SELECT id, name, signed_up_at FROM users");
        assert!(params.is_empty());
    }

    #[test]
    fn build_applies_filter_order_limit_offset() {
        let (sql, params) = QueryBuilder::<Users>::new()
            .select(&["id", "name"])
            .filter_eq("name", "Ada")
            .order_by("id", Order::Desc)
            .limit(10)
            .offset(5)
            .build();
        assert_eq!(sql, "SELECT id, name FROM users WHERE name = $1 ORDER BY id DESC LIMIT 10 OFFSET 5");
        assert_eq!(params, vec![Value::Text("Ada".to_string())]);
    }

    #[test]
    fn build_multiple_filters_are_and_ed_with_positional_params() {
        let (sql, params) = QueryBuilder::<Users>::new()
            .filter_eq("id", 1i64)
            .filter_eq("name", "Ada")
            .build();
        assert_eq!(sql, "SELECT id, name, signed_up_at FROM users WHERE id = $1 AND name = $2");
        assert_eq!(params, vec![Value::Int(1), Value::Text("Ada".to_string())]);
    }
}
