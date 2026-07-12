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
/// within and across queries. `pub(crate)` so `eval` can resolve an OID back
/// to a relation when implementing `pg_table_is_visible`.
pub(crate) fn synthetic_oid(name: &str) -> i32 {
    let mut hash: u32 = 2166136261; // FNV-1a
    for b in name.as_bytes() {
        hash ^= *b as u32;
        hash = hash.wrapping_mul(16777619);
    }
    (hash & 0x7fff_ffff) as i32
}

fn col(name: &str, ty: ColumnType) -> ColumnDef {
    ColumnDef {
        name: name.to_string(),
        col_type: ty,
        nullable: true,
        default: None,
        is_pk: false,
    }
}

fn schema(name: &str, columns: Vec<ColumnDef>) -> Arc<TableSchema> {
    Arc::new(TableSchema {
        name: name.to_string(),
        columns,
        pk_columns: vec![],
        unique_groups: vec![],
        foreign_keys: vec![],
        json_schemas: vec![],
    })
}

fn text(s: impl Into<String>) -> Option<Vec<u8>> {
    Some(s.into().into_bytes())
}

fn int(n: i32) -> Option<Vec<u8>> {
    Some(n.to_string().into_bytes())
}

/// Owner OID assigned to every relation we materialize. Postgres's bootstrap
/// superuser is OID 10; we mirror that so `pg_get_userbyid` has a stable,
/// recognizable value to resolve (the engine doesn't track per-table owners).
const OWNER_OID: i32 = 10;
/// Access-method OIDs, matching Postgres's `pg_am` so `pg_class.relam` joins
/// cleanly in introspection queries (`\d` displays the AM name via this join).
const HEAP_AM_OID: i32 = 2; // "heap" — ordinary tables
const BTREE_AM_OID: i32 = 403; // "btree" — ordinary secondary indexes

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
pub fn resolve_virtual_table(
    name: &str,
    db: &Arc<Database>,
) -> Option<anyhow::Result<VirtualTable>> {
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
        "pg_am" => Some(Ok(pg_am())),
        "pg_sequence" => Some(Ok(pg_sequence(db))),
        "pg_roles" => Some(pg_roles(db)),
        "pg_auth_members" => Some(pg_auth_members(db)),
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
    let s = schema(
        "pg_tables",
        vec![
            col("schemaname", ColumnType::Text),
            col("tablename", ColumnType::Text),
            col("tableowner", ColumnType::Text),
            col("tablespace", ColumnType::Text),
            col("hasindexes", ColumnType::Bool),
            col("hasrules", ColumnType::Bool),
            col("hastriggers", ColumnType::Bool),
            col("rowsecurity", ColumnType::Bool),
        ],
    );
    let indexed_tables: std::collections::HashSet<String> =
        db.list_indexes().into_iter().map(|(t, _)| t).collect();
    let rows = db
        .list_tables()?
        .into_iter()
        .map(|t| {
            let has_idx = indexed_tables.contains(&t);
            vec![
                text("public"),
                text(t),
                text("tpt"),
                None,
                boolean(has_idx),
                boolean(false),
                boolean(false),
                boolean(false),
            ]
        })
        .collect();
    Ok((s, rows))
}

fn pg_namespace() -> VirtualTable {
    let s = schema(
        "pg_namespace",
        vec![
            col("oid", ColumnType::Int4),
            col("nspname", ColumnType::Text),
        ],
    );
    let rows = vec![
        vec![int(synthetic_oid("pg_catalog")), text("pg_catalog")],
        vec![int(synthetic_oid("public")), text("public")],
        vec![
            int(synthetic_oid("information_schema")),
            text("information_schema"),
        ],
    ];
    (s, rows)
}

fn pg_class(db: &Arc<Database>) -> anyhow::Result<VirtualTable> {
    let s = schema(
        "pg_class",
        vec![
            col("oid", ColumnType::Int4),
            col("relname", ColumnType::Text),
            col("relnamespace", ColumnType::Int4),
            col("relkind", ColumnType::Text),
            col("relowner", ColumnType::Int4),
            col("relam", ColumnType::Int4),
        ],
    );
    let public_ns = synthetic_oid("public");
    let mut rows: Vec<Row> = db
        .list_tables()?
        .into_iter()
        .map(|t| {
            vec![
                int(synthetic_oid(&t)),
                text(t),
                int(public_ns),
                text("r"),
                int(OWNER_OID),
                int(HEAP_AM_OID),
            ]
        })
        .collect();
    for (table, column) in db.list_indexes() {
        let idx_name = format!("{table}_{column}_idx");
        rows.push(vec![
            int(synthetic_oid(&idx_name)),
            text(idx_name),
            int(public_ns),
            text("i"),
            int(OWNER_OID),
            int(BTREE_AM_OID),
        ]);
    }
    Ok((s, rows))
}

/// `pg_am` — access methods. Only the rows Postgres exposes that introspection
/// queries join against (`\d` shows the index AM via `pg_class.relam = pg_am.oid`).
fn pg_am() -> VirtualTable {
    let s = schema(
        "pg_am",
        vec![
            col("oid", ColumnType::Int4),
            col("amname", ColumnType::Text),
        ],
    );
    let rows = vec![
        vec![int(HEAP_AM_OID), text("heap")],
        vec![int(BTREE_AM_OID), text("btree")],
        vec![int(405), text("hash")],
        vec![int(783), text("gist")],
        vec![int(2742), text("gin")],
        vec![int(2743), text("spgist")],
        vec![int(3580), text("brin")],
    ];
    (s, rows)
}

fn pg_attribute(db: &Arc<Database>) -> anyhow::Result<VirtualTable> {
    let s = schema(
        "pg_attribute",
        vec![
            col("attrelid", ColumnType::Int4),
            col("attname", ColumnType::Text),
            col("atttypid", ColumnType::Int4),
            col("attnum", ColumnType::Int4),
            col("attnotnull", ColumnType::Bool),
        ],
    );
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
    let s = schema(
        "pg_type",
        vec![
            col("oid", ColumnType::Int4),
            col("typname", ColumnType::Text),
        ],
    );
    let types: &[(&str, ColumnType)] = &[
        ("int8", ColumnType::Int8),
        ("int4", ColumnType::Int4),
        ("int2", ColumnType::Int2),
        ("float8", ColumnType::Float8),
        ("float4", ColumnType::Float4),
        ("text", ColumnType::Text),
        ("bool", ColumnType::Bool),
        ("timestamp", ColumnType::Timestamp),
        ("date", ColumnType::Date),
        ("json", ColumnType::Json),
        ("bytea", ColumnType::Bytea),
        ("geometry", ColumnType::Geometry),
        ("geography", ColumnType::Geography),
        ("vector", ColumnType::Vector),
        ("raster", ColumnType::Raster),
    ];
    let rows = types
        .iter()
        .map(|(name, ty)| vec![int(ty.oid()), text(*name)])
        .collect();
    (s, rows)
}

fn pg_indexes(db: &Arc<Database>) -> anyhow::Result<VirtualTable> {
    let s = schema(
        "pg_indexes",
        vec![
            col("schemaname", ColumnType::Text),
            col("tablename", ColumnType::Text),
            col("indexname", ColumnType::Text),
            col("tablespace", ColumnType::Text),
            col("indexdef", ColumnType::Text),
        ],
    );
    let rows = db
        .list_indexes()
        .into_iter()
        .map(|(table, column)| {
            let idx_name = format!("{table}_{column}_idx");
            let indexdef = format!("CREATE INDEX {idx_name} ON {table} ({column})");
            vec![
                text("public"),
                text(table),
                text(idx_name),
                None,
                text(indexdef),
            ]
        })
        .chain(
            db.list_spatial_indexes()
                .into_iter()
                .map(|(table, column)| {
                    let idx_name = format!("{table}_{column}_idx");
                    let indexdef =
                        format!("CREATE INDEX {idx_name} ON {table} USING SPATIAL ({column})");
                    vec![
                        text("public"),
                        text(table),
                        text(idx_name),
                        None,
                        text(indexdef),
                    ]
                }),
        )
        .collect();
    Ok((s, rows))
}

fn pg_index(db: &Arc<Database>) -> anyhow::Result<VirtualTable> {
    let s = schema(
        "pg_index",
        vec![
            col("indexrelid", ColumnType::Int4),
            col("indrelid", ColumnType::Int4),
        ],
    );
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
    let s = schema(
        "pg_constraint",
        vec![
            col("conname", ColumnType::Text),
            col("contype", ColumnType::Text),
            col("conrelid", ColumnType::Int4),
            col("confrelid", ColumnType::Int4),
        ],
    );
    let mut rows = Vec::new();
    for t in db.list_tables()? {
        let Some(table_schema) = db.get_table(&t)? else {
            continue;
        };
        let relid = synthetic_oid(&t);
        if !table_schema.pk_columns.is_empty() {
            rows.push(vec![text(format!("{t}_pkey")), text("p"), int(relid), None]);
        }
        for group in &table_schema.unique_groups {
            let cols: Vec<&str> = group
                .iter()
                .map(|&i| table_schema.columns[i].name.as_str())
                .collect();
            rows.push(vec![
                text(format!("{t}_{}_key", cols.join("_"))),
                text("u"),
                int(relid),
                None,
            ]);
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
    let s = schema(
        "pg_sequence",
        vec![
            col("seqrelid", ColumnType::Int4),
            col("seqname", ColumnType::Text),
            col("start_value", ColumnType::Int8),
            col("increment_by", ColumnType::Int8),
            col("last_value", ColumnType::Int8),
        ],
    );
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
    let s = schema(
        "tables",
        vec![
            col("table_catalog", ColumnType::Text),
            col("table_schema", ColumnType::Text),
            col("table_name", ColumnType::Text),
            col("table_type", ColumnType::Text),
        ],
    );
    let rows = db
        .list_tables()?
        .into_iter()
        .map(|t| vec![text("tpt"), text("public"), text(t), text("BASE TABLE")])
        .collect();
    Ok((s, rows))
}

fn information_schema_columns(db: &Arc<Database>) -> anyhow::Result<VirtualTable> {
    let s = schema(
        "columns",
        vec![
            col("table_catalog", ColumnType::Text),
            col("table_schema", ColumnType::Text),
            col("table_name", ColumnType::Text),
            col("column_name", ColumnType::Text),
            col("ordinal_position", ColumnType::Int4),
            col("is_nullable", ColumnType::Text),
            col("data_type", ColumnType::Text),
        ],
    );
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
        ColumnType::Geography => "geography",
        ColumnType::Vector => "vector",
        ColumnType::Raster => "raster",
        ColumnType::Float8Array => "double precision[]",
    }
}

/// Reverse of `ColumnType::oid()` — map a Postgres type OID back to its human
/// name. Used by `format_type(oid, typmod)` (psql's `\d` column-type display,
/// and many other introspection queries). Unknown OIDs fall back to `text`,
/// matching how `ColumnType::oid()` maps the engine's non-Postgres types
/// (geometry/vector/raster) to TEXT's OID.
pub(crate) fn pg_type_name_by_oid(oid: i32) -> &'static str {
    use crate::wire::messages::oid::{BOOL, FLOAT8, INT8, TEXT};
    match oid {
        INT8 => "bigint",
        23 => "integer",
        21 => "smallint",
        FLOAT8 => "double precision",
        700 => "real",
        TEXT => "text",
        BOOL => "boolean",
        1114 => "timestamp without time zone",
        1082 => "date",
        114 => "json",
        17 => "bytea",
        _ => "text",
    }
}

/// `pg_catalog.pg_roles` — one row per role in `_tpt_roles`, with the OID,
/// superuser flag, and login privilege Postgres exposes.
fn pg_roles(db: &Arc<Database>) -> anyhow::Result<VirtualTable> {
    let s = schema(
        "pg_roles",
        vec![
            col("oid", ColumnType::Int4),
            col("rolname", ColumnType::Text),
            col("rolsuper", ColumnType::Bool),
            col("rolcanlogin", ColumnType::Bool),
        ],
    );
    let mut rows = Vec::new();
    for kv in db.scan("_tpt_roles")? {
        let rolname = match crate::synapse::decode_cell(&kv.value, 0) {
            Some(b) => match String::from_utf8(b) {
                Ok(s) => s,
                Err(_) => continue,
            },
            None => continue,
        };
        let superuser = crate::synapse::decode_cell(&kv.value, 6)
            .and_then(|b| String::from_utf8(b).ok())
            .map(|s| s == "t")
            .unwrap_or(false);
        let can_login = crate::synapse::decode_cell(&kv.value, 7)
            .and_then(|b| String::from_utf8(b).ok())
            .map(|s| s == "t")
            .unwrap_or(true);
        rows.push(vec![
            int(synthetic_oid(&rolname)),
            text(rolname),
            boolean(superuser),
            boolean(can_login),
        ]);
    }
    Ok((s, rows))
}

/// `pg_catalog.pg_auth_members` — one row per `member → group` edge in
/// `_tpt_role_members`, with both ends resolved to their synthetic OIDs.
fn pg_auth_members(db: &Arc<Database>) -> anyhow::Result<VirtualTable> {
    let s = schema(
        "pg_auth_members",
        vec![
            col("oid", ColumnType::Int4),
            col("roleid", ColumnType::Int4),
            col("member", ColumnType::Int4),
            col("admin_option", ColumnType::Bool),
        ],
    );
    let mut rows = Vec::new();
    for kv in db.scan("_tpt_role_members")? {
        let member = match crate::synapse::decode_cell(&kv.value, 1) {
            Some(b) => match String::from_utf8(b) {
                Ok(s) => s,
                Err(_) => continue,
            },
            None => continue,
        };
        let group = match crate::synapse::decode_cell(&kv.value, 2) {
            Some(b) => match String::from_utf8(b) {
                Ok(s) => s,
                Err(_) => continue,
            },
            None => continue,
        };
        rows.push(vec![
            int(synthetic_oid(&format!("{member}\u{0}{group}"))),
            int(synthetic_oid(&group)),
            int(synthetic_oid(&member)),
            boolean(false),
        ]);
    }
    Ok((s, rows))
}
