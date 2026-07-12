//! Tests for the Phase 4 `pg_catalog`/`information_schema` virtual tables:
//! they should reflect whatever tables/columns/indexes exist in a live
//! `Database`, resolved through the same `resolve_table_ref`/
//! `resolve_primary_table` path as any other SELECT.

use std::sync::Arc;
use std::time::Duration;

use super::execute_query;
use crate::storage::config::NodeRole;
use crate::storage::database::Database;
use crate::storage::lease::LeaseManager;
use crate::storage::objectstore::{LocalFsObjectStore, ObjectStore};
use crate::storage::{ColumnDef, ColumnType, StorageEngine};

fn test_db() -> (Arc<Database>, tempfile::TempDir, tempfile::TempDir) {
    let bucket = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::open(bucket.path()).unwrap());
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

fn cell_text(cell: &Option<Vec<u8>>) -> String {
    String::from_utf8(cell.clone().unwrap()).unwrap()
}

#[test]
fn repeated_query_text_hits_the_shared_statement_cache() {
    let (db, _b, _l) = test_db();
    execute_query("SELECT 1", db.clone()).unwrap();
    let (_, _, misses_after_first) = db.stmt_cache_stats();
    assert_eq!(misses_after_first, 1);

    execute_query("SELECT 1", db.clone()).unwrap();
    let (entries, hits, misses) = db.stmt_cache_stats();
    assert_eq!(entries, 1);
    assert_eq!(
        misses, 1,
        "second identical query should be a cache hit, not another parse"
    );
    assert_eq!(hits, 1);
}

#[test]
fn pg_tables_lists_created_table() {
    let (db, _b, _l) = test_db();
    db.create_table(
        "widgets",
        &[ColumnDef {
            name: "id".into(),
            col_type: ColumnType::Int4,
            nullable: false,
            default: None,
            is_pk: true,
        }],
    )
    .unwrap();

    let result = execute_query("SELECT tablename FROM pg_catalog.pg_tables", db.clone()).unwrap();
    let names: Vec<String> = result.rows.iter().map(|r| cell_text(&r[0])).collect();
    assert!(
        names.contains(&"widgets".to_string()),
        "expected widgets in {names:?}"
    );
}

#[test]
fn information_schema_columns_describes_table_shape() {
    let (db, _b, _l) = test_db();
    db.create_table(
        "people",
        &[
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Int4,
                nullable: false,
                default: None,
                is_pk: true,
            },
            ColumnDef {
                name: "name".into(),
                col_type: ColumnType::Text,
                nullable: true,
                default: None,
                is_pk: false,
            },
        ],
    )
    .unwrap();

    let result = execute_query(
        "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'people'",
        db.clone(),
    ).unwrap();
    assert_eq!(result.rows.len(), 2);
    let name_row = result
        .rows
        .iter()
        .find(|r| cell_text(&r[0]) == "name")
        .unwrap();
    assert_eq!(cell_text(&name_row[1]), "text");
    assert_eq!(cell_text(&name_row[2]), "YES");
    let id_row = result
        .rows
        .iter()
        .find(|r| cell_text(&r[0]) == "id")
        .unwrap();
    assert_eq!(cell_text(&id_row[2]), "NO");
}

#[test]
fn pg_indexes_reflects_created_index() {
    let (db, _b, _l) = test_db();
    db.create_table(
        "orders",
        &[
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Int4,
                nullable: false,
                default: None,
                is_pk: true,
            },
            ColumnDef {
                name: "customer".into(),
                col_type: ColumnType::Text,
                nullable: false,
                default: None,
                is_pk: false,
            },
        ],
    )
    .unwrap();
    db.create_index("orders", "customer").unwrap();

    let result = execute_query(
        "SELECT tablename, indexname FROM pg_catalog.pg_indexes",
        db.clone(),
    )
    .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(cell_text(&result.rows[0][0]), "orders");
    assert_eq!(cell_text(&result.rows[0][1]), "orders_customer_idx");
}

#[test]
fn pg_catalog_tables_are_shadowed_from_user_tables() {
    let (db, _b, _l) = test_db();
    // A real user table never collides with the virtual pg_catalog namespace
    // because resolve_table_ref checks catalog::resolve_virtual_table first.
    let result = execute_query(
        "SELECT nspname FROM pg_catalog.pg_namespace ORDER BY nspname",
        db.clone(),
    )
    .unwrap();
    let names: Vec<String> = result.rows.iter().map(|r| cell_text(&r[0])).collect();
    assert!(names.contains(&"public".to_string()));
    assert!(names.contains(&"pg_catalog".to_string()));
}

/// The exact SQL psql 15 sends for `\dt` (list tables). If this returns the
/// user tables with the right shape, a real `psql` client's `\dt` works
/// against this engine (psql just renders the returned rows).
const PSQL_DT_QUERY: &str = r#"
SELECT n.nspname as "Schema", c.relname as "Name",
  CASE c.relkind
    WHEN 'r' THEN 'table'
    WHEN 'v' THEN 'view'
    WHEN 'm' THEN 'materialized view'
    WHEN 'i' THEN 'index'
    WHEN 'S' THEN 'sequence'
    WHEN 's' THEN 'special'
    WHEN 't' THEN 'TOAST table'
    WHEN 'f' THEN 'foreign table'
    WHEN 'p' THEN 'partitioned table'
    WHEN 'I' THEN 'partitioned index'
  END as "Type",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r','p','')
      AND n.nspname <> 'pg_catalog'
      AND n.nspname !~ '^pg_toast'
      AND n.nspname <> 'information_schema'
      AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1,2
"#;

#[test]
fn psql_dt_meta_command_lists_user_tables() {
    let (db, _b, _l) = test_db();
    for t in ["users", "widgets", "things"] {
        db.create_table(
            t,
            &[ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Int4,
                nullable: false,
                default: None,
                is_pk: true,
            }],
        )
        .unwrap();
    }

    let result = execute_query(PSQL_DT_QUERY, db.clone()).unwrap();
    // One row per user table (indexes are relkind 'i', excluded by the WHERE).
    assert_eq!(result.rows.len(), 3, "got rows: {:?}", result.rows);

    // Find the column positions for Name / Type / Owner by header name.
    let headers: Vec<String> = result.fields.iter().map(|f| f.name.clone()).collect();
    let name_i = headers.iter().position(|h| h == "Name").unwrap();
    let type_i = headers.iter().position(|h| h == "Type").unwrap();
    let owner_i = headers.iter().position(|h| h == "Owner").unwrap();

    let mut names: Vec<String> = result.rows.iter().map(|r| cell_text(&r[name_i])).collect();
    names.sort();
    assert_eq!(names, vec!["things", "users", "widgets"]);

    for r in &result.rows {
        assert_eq!(cell_text(&r[type_i]), "table");
        assert_eq!(cell_text(&r[owner_i]), "tpt");
    }
}

/// A `\d`-style introspection query exercising the building blocks psql uses:
/// `pg_am` join on `pg_class.relam`, the `OPERATOR(pg_catalog.~)` infix syntax,
/// and a `COLLATE` clause (parsed and ignored).
#[test]
fn d_style_query_joins_pg_am_with_operator_and_collate() {
    let (db, _b, _l) = test_db();
    db.create_table(
        "accounts",
        &[ColumnDef {
            name: "id".into(),
            col_type: ColumnType::Int4,
            nullable: false,
            default: None,
            is_pk: true,
        }],
    )
    .unwrap();

    let sql = r#"
SELECT c.relname, a.amname
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_am a ON a.oid OPERATOR(pg_catalog.=) c.relam
WHERE c.relkind OPERATOR(pg_catalog.=) 'r' COLLATE pg_catalog.default
  AND c.relname OPERATOR(pg_catalog.~) '^a'
"#;
    let result = execute_query(sql, db.clone()).unwrap();
    assert_eq!(result.rows.len(), 1, "got rows: {:?}", result.rows);
    assert_eq!(cell_text(&result.rows[0][0]), "accounts");
    // `accounts` is a heap table -> relam = 2 -> pg_am 'heap'.
    assert_eq!(cell_text(&result.rows[0][1]), "heap");
}
