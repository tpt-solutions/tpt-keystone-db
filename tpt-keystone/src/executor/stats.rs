//! `ANALYZE` — computes and persists per-table row counts and per-column
//! distinct-value counts into `_tpt_stats`, giving the planner real
//! statistics instead of only the runtime-materialized row counts a single
//! join's build-side heuristic already sees at execution time. Feeds
//! `planner::reorder_inner_joins`'s multi-way join-order heuristic
//! (Phase 12a/18's "query planner statistics" follow-up).
//!
//! Scope cut, same discipline as `TimeIndex`/`Partition` retention
//! elsewhere: stats are computed synchronously by a full table scan when
//! `ANALYZE` runs (no background auto-analyze, no sampling — a huge table
//! makes `ANALYZE` itself slow, the same tradeoff Harbor's row-count
//! verification already accepts), and there's no automatic re-`ANALYZE` on
//! write, so stats go stale until a caller re-runs it (matching real
//! Postgres's own "you have to run ANALYZE" behavior, just without the
//! autovacuum daemon that usually does it for you).

use std::collections::HashSet;
use std::sync::Arc;

use super::{parse_rows, QueryResult};
use crate::storage::database::Database;
use crate::storage::{ColumnType, StorageEngine, TableSchema};
use crate::synapse::{cell_i64, col, encode_cells, int_cell, now_ms, text_cell};

const TABLE: &str = "_tpt_stats";
/// Column-level rows use this synthetic key so a bare table name (used for
/// the table-level row) can never collide with `<table><SEP><column>`.
const SEP: char = '\u{1f}';

fn ensure_schema(db: &Database) -> anyhow::Result<()> {
    if db.get_table(TABLE)?.is_some() {
        return Ok(());
    }
    db.create_table_with_constraints(
        TABLE,
        &[
            col("stat_key", ColumnType::Text, false, true),
            col("table_name", ColumnType::Text, false, false),
            col("column_name", ColumnType::Text, false, false),
            col("row_count", ColumnType::Int8, false, false),
            col("distinct_count", ColumnType::Int8, false, false),
            col("updated_at", ColumnType::Int8, false, false),
        ],
        vec![],
        vec![],
    )
}

fn write_stat_row(
    db: &Database,
    table: &str,
    column: &str,
    row_count: i64,
    distinct_count: i64,
    ts: i64,
) -> anyhow::Result<()> {
    let stat_key = if column.is_empty() {
        table.to_string()
    } else {
        format!("{table}{SEP}{column}")
    };
    let cells = vec![
        text_cell(&stat_key),
        text_cell(table),
        text_cell(column),
        int_cell(row_count),
        int_cell(distinct_count),
        int_cell(ts),
    ];
    db.write(TABLE, stat_key.as_bytes(), &encode_cells(&cells))
}

fn analyze_one(db: &Database, table: &str, schema: &TableSchema) -> anyhow::Result<()> {
    let kvs = db.scan(table)?;
    let rows = parse_rows(&kvs, &Some(schema.clone()));
    let row_count = rows.len() as i64;
    let ts = now_ms();

    write_stat_row(db, table, "", row_count, -1, ts)?;

    for (idx, coldef) in schema.columns.iter().enumerate() {
        let mut distinct: HashSet<Vec<u8>> = HashSet::new();
        for row in &rows {
            if let Some(Some(cell)) = row.get(idx) {
                distinct.insert(cell.clone());
            }
        }
        write_stat_row(
            db,
            table,
            &coldef.name,
            row_count,
            distinct.len() as i64,
            ts,
        )?;
    }
    Ok(())
}

/// `ANALYZE [table]` — `None` analyzes every user table (any table not
/// starting with `_`, i.e. not one of this engine's own system catalogs).
pub fn execute_analyze(table: Option<String>, db: Arc<Database>) -> anyhow::Result<QueryResult> {
    ensure_schema(&db)?;
    let targets = match table {
        Some(t) => vec![t],
        None => db
            .list_tables()?
            .into_iter()
            .filter(|t| !t.starts_with('_'))
            .collect(),
    };

    for name in &targets {
        let Some(schema) = db.get_table(name)? else {
            anyhow::bail!("table \"{name}\" does not exist");
        };
        analyze_one(&db, name, &schema)?;
    }

    Ok(QueryResult {
        fields: vec![],
        rows: vec![],
        tag: format!("ANALYZE {}", targets.len()),
    })
}

/// Persisted row-count estimate for `table`, or `None` if `ANALYZE` has
/// never run against it. Read directly by primary key (`stat_key == table`,
/// the table-level row `analyze_one` writes) — a single point lookup, not a
/// scan.
pub fn table_row_count_estimate(db: &Database, table: &str) -> Option<i64> {
    let value = db.read(TABLE, table.as_bytes()).ok()??;
    cell_i64(&crate::synapse::decode_cell(&value, 3))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::execute_query;
    use crate::storage::config::NodeRole;
    use crate::storage::lease::LeaseManager;
    use crate::storage::objectstore::{LocalFsObjectStore, ObjectStore};
    use std::time::Duration;

    fn test_db() -> (Arc<Database>, tempfile::TempDir, tempfile::TempDir) {
        let bucket = tempfile::tempdir().unwrap();
        let local = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFsObjectStore::open(bucket.path()).unwrap());
        let lease = Arc::new(LeaseManager::new(
            store.clone(),
            "db",
            "node-1".into(),
            Duration::from_secs(30),
        ));
        lease.try_acquire().unwrap();
        let db = Arc::new(
            Database::open(
                local.path(),
                store,
                lease.handle(),
                NodeRole::Writer,
                Default::default(),
            )
            .unwrap(),
        );
        (db, bucket, local)
    }

    #[test]
    fn analyze_persists_row_count_and_is_queryable() {
        let (db, _b, _l) = test_db();
        execute_query("CREATE TABLE t (id INT4, name TEXT)", db.clone()).unwrap();
        execute_query("INSERT INTO t VALUES (1, 'a')", db.clone()).unwrap();
        execute_query("INSERT INTO t VALUES (2, 'a')", db.clone()).unwrap();
        execute_query("INSERT INTO t VALUES (3, 'b')", db.clone()).unwrap();

        let result = execute_query("ANALYZE t", db.clone()).unwrap();
        assert_eq!(result.tag, "ANALYZE 1");

        assert_eq!(table_row_count_estimate(&db, "t"), Some(3));

        let value = db.read(TABLE, b"t\x1fname").unwrap().unwrap();
        let distinct = cell_i64(&crate::synapse::decode_cell(&value, 4)).unwrap();
        assert_eq!(distinct, 2);
    }

    #[test]
    fn bare_analyze_covers_every_user_table() {
        let (db, _b, _l) = test_db();
        execute_query("CREATE TABLE a (id INT4)", db.clone()).unwrap();
        execute_query("CREATE TABLE b (id INT4)", db.clone()).unwrap();
        execute_query("INSERT INTO a VALUES (1)", db.clone()).unwrap();

        let result = execute_query("ANALYZE", db.clone()).unwrap();
        assert_eq!(result.tag, "ANALYZE 2");
        assert_eq!(table_row_count_estimate(&db, "a"), Some(1));
        assert_eq!(table_row_count_estimate(&db, "b"), Some(0));
    }
}
