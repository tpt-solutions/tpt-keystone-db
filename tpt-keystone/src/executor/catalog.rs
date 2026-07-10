//! Virtual `pg_catalog` / `information_schema` tables, materialized on
//! demand from the live schema catalog (`Database::list_tables`/`get_table`)
//! and index registry (`Database::list_indexes`). These let `psql` and
//! standard Postgres tooling introspect the schema with plain `SELECT`s.
//!
//! Not a full `pg_catalog` implementation: OIDs are a stable hash of the
//! object name (not true Postgres OID semantics), and only the handful of
//! tables/columns commonly queried by simple introspection tools are
//! covered. `psql`'s own `\d`/`\dt` meta-commands issue more elaborate
//! queries (joins through `pg_type`, calls to `format_type()`,
//! `pg_table_is_visible()`, etc.) that are not reproduced here.

use std::sync::Arc;

use crate::storage::database::Database;
use crate::storage::{ColumnDef, ColumnType, StorageEngine, TableSchema};

type Row = Vec<Option<Vec<u8>>>;
type VirtualTable = (Arc<TableSchema>, Vec<Row>);

/// A stable-but-synthetic OID for a named catalog object (table, index, ...).
/// Not real Postgres OID semantics — just enough to keep joins between the
/// virtual `pg_class`/`pg_attribute`/`pg_index` tables internally consistent
/// within and across queries.
fn synthetic_oid(name: &str) -> i32 {
    let mut hash: u32 = 2166136261; // FNV-1a
    for b in name.as_bytes() {
        hash ^= *b as u32;
        hash = hash.wrapping_mul(16777619);
    }
    (hash & 0x7fff_ffff) as i32
}

fn col(name: &str, ty: ColumnType) -> ColumnDef {
    ColumnDef { name: name.to_string(), col_type: ty, nullable: true, default: None, is_pk: false }
}

fn schema(name: &str, columns: Vec<ColumnDef>) -> Arc<TableSchema> {
    Arc::new(TableSchema { name: name.to_string(), columns, pk_columns: vec![], unique_groups: vec![], foreign_keys: vec![], json_schemas: vec![] })
}

fn text(s: impl Into<String>) -> Option<Vec<u8>> {
    Some(s.into().into_bytes())
}

fn int(n: i32) -> Option<Vec<u8>> {
    Some(n.to_string().into_bytes())
}

fn boolean(b: bool) -> Option<Vec<u8>> {
    Some(if b { b"t".to_vec() } else { b"f".to_vec() })
}

/// Strip a `pg_catalog.`/`information_schema.` prefix (case-insensitive) if
/// present, returning the bare table name.
fn strip_schema_prefix(name: &str) -> &str {
    for prefix in ["pg_catalog.", "information_schema."] {
        if name.len() > prefix.len() && name[..prefix.len()].eq_ignore_ascii_case(prefix) {
            return &name[prefix.len()..];
        }
    }
    name
}

/// Recognize and materialize a virtual system catalog table. Returns `None`
/// if `name` isn't a known virtual table (the caller should then fall back
/// to normal user-table resolution).
pub fn resolve_virtual_table(name: &str, db: &Arc<Database>) -> Option<anyhow::Result<VirtualTable>> {
    let bare = strip_schema_prefix(name).to_ascii_lowercase();
    match bare.as_str() {
        "pg_tables" => Some(pg_tables(db)),
        "pg_class" => Some(pg_class(db)),
        "pg_namespace" => Some(Ok(pg_namespace())),
        "pg_attribute" => Some(pg_attribute(db)),
        "pg_type" => Some(Ok(pg_type())),
        "pg_indexes" => Some(pg_indexes(db)),
        "pg_index" => Some(pg_index(db)),
        "pg_constraint" => Some(pg_constraint(db)),
        "pg_sequence" => Some(Ok(pg_sequence(db))),
        "tables" if is_information_schema(name) => Some(information_schema_tables(db)),
        "columns" if is_information_schema(name) => Some(information_schema_columns(db)),
        _ => None,
    }
}

fn is_information_schema(name: &str) -> bool {
    name.len() > "information_schema.".len()
        && name[.."information_schema.".len()].eq_ignore_ascii_case("information_schema.")
}

fn pg_tables(db: &Arc<Database>) -> anyhow::Result<VirtualTable> {
    let s = schema("pg_tables", vec![
        col("schemaname", ColumnType::Text),
        col("tablename", ColumnType::Text),
        col("tableowner", ColumnType::Text),
        col("tablespace", ColumnType::Text),
        col("hasindexes", ColumnType::Bool),
        col("hasrules", ColumnType::Bool),
        col("hastriggers", ColumnType::Bool),
        col("rowsecurity", ColumnType::Bool),
    ]);
    let indexed_tables: std::collections::HashSet<String> =
        db.list_indexes().into_iter().map(|(t, _)| t).collect();
    let rows = db
        .list_tables()?
        .into_iter()
        .map(|t| {
            let has_idx = indexed_tables.contains(&t);
            vec![text("public"), text(t), text("tpt"), None, boolean(has_idx), boolean(false), boolean(false), boolean(false)]
        })
        .collect();
    Ok((s, rows))
}

fn pg_namespace() -> VirtualTable {
    let s = schema("pg_namespace", vec![
        col("oid", ColumnType::Int4),
        col("nspname", ColumnType::Text),
    ]);
    let rows = vec![
        vec![int(synthetic_oid("pg_catalog")), text("pg_catalog")],
        vec![int(synthetic_oid("public")), text("public")],
        vec![int(synthetic_oid("information_schema")), text("information_schema")],
    ];
    (s, rows)
}

fn pg_class(db: &Arc<Database>) -> anyhow::Result<VirtualTable> {
    let s = schema("pg_class", vec![
        col("oid", ColumnType::Int4),
        col("relname", ColumnType::Text),
        col("relnamespace", ColumnType::Int4),
        col("relkind", ColumnType::Text),
    ]);
    let public_ns = synthetic_oid("public");
    let mut rows: Vec<Row> = db
        .list_tables()?
        .into_iter()
        .map(|t| vec![int(synthetic_oid(&t)), text(t), int(public_ns), text("r")])
        .collect();
    for (table, column) in db.list_indexes() {
        let idx_name = format!("{table}_{column}_idx");
        rows.push(vec![int(synthetic_oid(&idx_name)), text(idx_name), int(public_ns), text("i")]);
    }
    Ok((s, rows))
}

fn pg_attribute(db: &Arc<Database>) -> anyhow::Result<VirtualTable> {
    let s = schema("pg_attribute", vec![
        col("attrelid", ColumnType::Int4),
        col("attname", ColumnType::Text),
        col("atttypid", ColumnType::Int4),
        col("attnum", ColumnType::Int4),
        col("attnotnull", ColumnType::Bool),
    ]);
    let mut rows = Vec::new();
    for t in db.list_tables()? {
        if let Some(table_schema) = db.get_table(&t)? {
            let relid = synthetic_oid(&t);
            for (i, c) in table_schema.columns.iter().enumerate() {
                rows.push(vec![
                    int(relid),
                    text(c.name.clone()),
                    int(c.col_type.oid()),
                    int((i + 1) as i32),
                    boolean(!c.nullable),
                ]);
            }
        }
    }
    Ok((s, rows))
}

fn pg_type() -> VirtualTable {
    let s = schema("pg_type", vec![
        col("oid", ColumnType::Int4),
        col("typname", ColumnType::Text),
    ]);
    let types: &[(&str, ColumnType)] = &[
        ("int8", ColumnType::Int8), ("int4", ColumnType::Int4), ("int2", ColumnType::Int2),
        ("float8", ColumnType::Float8), ("float4", ColumnType::Float4), ("text", ColumnType::Text),
        ("bool", ColumnType::Bool), ("timestamp", ColumnType::Timestamp), ("date", ColumnType::Date),
        ("json", ColumnType::Json), ("bytea", ColumnType::Bytea),
        ("geometry", ColumnType::Geometry),
        ("vector", ColumnType::Vector),
    ];
    let rows = types.iter().map(|(name, ty)| vec![int(ty.oid()), text(*name)]).collect();
    (s, rows)
}

fn pg_indexes(db: &Arc<Database>) -> anyhow::Result<VirtualTable> {
    let s = schema("pg_indexes", vec![
        col("schemaname", ColumnType::Text),
        col("tablename", ColumnType::Text),
        col("indexname", ColumnType::Text),
        col("tablespace", ColumnType::Text),
        col("indexdef", ColumnType::Text),
    ]);
    let rows = db
        .list_indexes()
        .into_iter()
        .map(|(table, column)| {
            let idx_name = format!("{table}_{column}_idx");
            let indexdef = format!("CREATE INDEX {idx_name} ON {table} ({column})");
            vec![text("public"), text(table), text(idx_name), None, text(indexdef)]
        })
        .chain(db.list_spatial_indexes().into_iter().map(|(table, column)| {
            let idx_name = format!("{table}_{column}_idx");
            let indexdef = format!("CREATE INDEX {idx_name} ON {table} USING SPATIAL ({column})");
            vec![text("public"), text(table), text(idx_name), None, text(indexdef)]
        }))
        .collect();
    Ok((s, rows))
}

fn pg_index(db: &Arc<Database>) -> anyhow::Result<VirtualTable> {
    let s = schema("pg_index", vec![
        col("indexrelid", ColumnType::Int4),
        col("indrelid", ColumnType::Int4),
    ]);
    let rows = db
        .list_indexes()
        .into_iter()
        .map(|(table, column)| {
            let idx_name = format!("{table}_{column}_idx");
            vec![int(synthetic_oid(&idx_name)), int(synthetic_oid(&table))]
        })
        .collect();
    Ok((s, rows))
}

/// One row per PK/UNIQUE/FK constraint across every table. `contype`
/// follows Postgres's convention: `'p'` primary key, `'u'` unique,
/// `'f'` foreign key. `confrelid`/`confkey` (the referenced table/column)
/// are only meaningful for `'f'` rows.
fn pg_constraint(db: &Arc<Database>) -> anyhow::Result<VirtualTable> {
    let s = schema("pg_constraint", vec![
        col("conname", ColumnType::Text),
        col("contype", ColumnType::Text),
        col("conrelid", ColumnType::Int4),
        col("confrelid", ColumnType::Int4),
    ]);
    let mut rows = Vec::new();
    for t in db.list_tables()? {
        let Some(table_schema) = db.get_table(&t)? else { continue };
        let relid = synthetic_oid(&t);
        if !table_schema.pk_columns.is_empty() {
            rows.push(vec![text(format!("{t}_pkey")), text("p"), int(relid), None]);
        }
        for group in &table_schema.unique_groups {
            let cols: Vec<&str> = group.iter().map(|&i| table_schema.columns[i].name.as_str()).collect();
            rows.push(vec![text(format!("{t}_{}_key", cols.join("_"))), text("u"), int(relid), None]);
        }
        for fk in &table_schema.foreign_keys {
            let col_name = &table_schema.columns[fk.column].name;
            rows.push(vec![
                text(format!("{t}_{col_name}_fkey")),
                text("f"),
                int(relid),
                int(synthetic_oid(&fk.ref_table)),
            ]);
        }
    }
    Ok((s, rows))
}

fn pg_sequence(db: &Arc<Database>) -> VirtualTable {
    let s = schema("pg_sequence", vec![
        col("seqrelid", ColumnType::Int4),
        col("seqname", ColumnType::Text),
        col("start_value", ColumnType::Int8),
        col("increment_by", ColumnType::Int8),
        col("last_value", ColumnType::Int8),
    ]);
    let rows = db
        .list_sequences()
        .into_iter()
        .map(|seq| {
            vec![
                int(synthetic_oid(&seq.name)),
                text(seq.name.clone()),
                int(0), // start value isn't retained after creation — only the running counter is
                Some(seq.increment.to_string().into_bytes()),
                Some(seq.value.to_string().into_bytes()),
            ]
        })
        .collect();
    (s, rows)
}

fn information_schema_tables(db: &Arc<Database>) -> anyhow::Result<VirtualTable> {
    let s = schema("tables", vec![
        col("table_catalog", ColumnType::Text),
        col("table_schema", ColumnType::Text),
        col("table_name", ColumnType::Text),
        col("table_type", ColumnType::Text),
    ]);
    let rows = db
        .list_tables()?
        .into_iter()
        .map(|t| vec![text("tpt"), text("public"), text(t), text("BASE TABLE")])
        .collect();
    Ok((s, rows))
}

fn information_schema_columns(db: &Arc<Database>) -> anyhow::Result<VirtualTable> {
    let s = schema("columns", vec![
        col("table_catalog", ColumnType::Text),
        col("table_schema", ColumnType::Text),
        col("table_name", ColumnType::Text),
        col("column_name", ColumnType::Text),
        col("ordinal_position", ColumnType::Int4),
        col("is_nullable", ColumnType::Text),
        col("data_type", ColumnType::Text),
    ]);
    let mut rows = Vec::new();
    for t in db.list_tables()? {
        if let Some(table_schema) = db.get_table(&t)? {
            for (i, c) in table_schema.columns.iter().enumerate() {
                rows.push(vec![
                    text("tpt"),
                    text("public"),
                    text(t.clone()),
                    text(c.name.clone()),
                    int((i + 1) as i32),
                    text(if c.nullable { "YES" } else { "NO" }),
                    text(type_name(&c.col_type)),
                ]);
            }
        }
    }
    Ok((s, rows))
}

fn type_name(ty: &ColumnType) -> &'static str {
    match ty {
        ColumnType::Int8 => "bigint",
        ColumnType::Int4 => "integer",
        ColumnType::Int2 => "smallint",
        ColumnType::Float8 => "double precision",
        ColumnType::Float4 => "real",
        ColumnType::Text => "text",
        ColumnType::Bool => "boolean",
        ColumnType::Timestamp => "timestamp without time zone",
        ColumnType::Date => "date",
        ColumnType::Json => "json",
        ColumnType::Bytea => "bytea",
        ColumnType::Geometry => "geometry",
        ColumnType::Vector => "vector",
    }
}
