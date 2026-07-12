//! DDL execution: CREATE/DROP TABLE, CREATE INDEX (including the per-engine
//! `USING SPATIAL/TIME/GRAPH/JSONPATH/GIN/VECTOR` variants), CREATE SEQUENCE,
//! CREATE TOPIC (Flux), ALTER TABLE, and CREATE FUNCTION (WASM UDFs).

use std::sync::Arc;

use super::eval;
use super::udf;
use super::QueryResult;
use crate::sql::ast::{Expr, Literal, UnOp};
use crate::storage::database::Database;
use crate::storage::{ColumnDef, ColumnType, ForeignKey, StorageEngine, TableSchema};

/// Serialize the small subset of expressions a column DEFAULT realistically
/// needs (literals, `nextval('seq')`-style function calls, and `::type`
/// casts like `pg_dump`'s `nextval(...)::regclass`) back to SQL text, so it
/// can be persisted in `storage::ColumnDef.default: Option<String>` (which
/// can't hold a parsed `Expr` directly — `Expr::Subquery` pulls in the
/// entire `SelectStmt` type graph, which isn't `Serialize`) and re-parsed
/// via `sql::parse_expr_text` at INSERT time. Anything more complex is a
/// `CREATE TABLE`/`ALTER TABLE` error rather than a silently dropped default.
pub(super) fn default_expr_to_text(e: &Expr) -> anyhow::Result<String> {
    match e {
        Expr::Literal(Literal::Int(n)) => Ok(n.to_string()),
        Expr::Literal(Literal::Float(f)) => Ok(f.to_string()),
        Expr::Literal(Literal::Text(s)) => Ok(format!("'{}'", s.replace('\'', "''"))),
        Expr::Literal(Literal::Bool(b)) => Ok(b.to_string()),
        Expr::Literal(Literal::Null) => Ok("NULL".to_string()),
        Expr::Literal(Literal::FloatArray(a)) => Ok(format!(
            "'{{{}}}'",
            a.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(",")
        )),
        Expr::UnaryOp { op: UnOp::Neg, expr } => Ok(format!("-{}", default_expr_to_text(expr)?)),
        Expr::Function { name, args, .. } => {
            let arg_texts: Vec<String> = args.iter().map(default_expr_to_text).collect::<anyhow::Result<_>>()?;
            Ok(format!("{name}({})", arg_texts.join(", ")))
        }
        Expr::Cast { expr, ty } => Ok(format!("{}::{}", default_expr_to_text(expr)?, ty)),
        other => anyhow::bail!(
            "DEFAULT expression too complex to persist in this version (supported: literals, nextval()-style function calls): {other:?}"
        ),
    }
}

/// Resolve a table-level constraint's column-name list to indices into
/// `columns` (in declared position order), for `TableSchema.unique_groups`.
fn resolve_column_indices(columns: &[ColumnDef], names: &[String]) -> anyhow::Result<Vec<usize>> {
    names
        .iter()
        .map(|n| {
            columns
                .iter()
                .position(|c| &c.name == n)
                .ok_or_else(|| anyhow::anyhow!("column \"{n}\" does not exist"))
        })
        .collect()
}

pub(super) fn execute_create_table(
    ct: crate::sql::ast::CreateTableStmt,
    db: Arc<Database>,
) -> anyhow::Result<QueryResult> {
    if ct.if_not_exists && db.list_tables()?.iter().any(|name| name == &ct.table) {
        return Ok(QueryResult {
            fields: vec![],
            rows: vec![],
            tag: "CREATE TABLE".into(),
        });
    }

    let mut columns: Vec<ColumnDef> = Vec::with_capacity(ct.columns.len());
    let mut unique_groups: Vec<Vec<usize>> = Vec::new();
    let mut foreign_keys: Vec<ForeignKey> = Vec::new();
    // (column index, sequence name) for SERIAL columns — the sequence is
    // created after the table so its implicit default can name it.
    let mut serial_columns: Vec<(usize, String)> = Vec::new();

    for (i, c) in ct.columns.iter().enumerate() {
        let col_type = ColumnType::from_name(&c.col_type).unwrap_or(ColumnType::Text);
        let default = c.default.as_ref().map(default_expr_to_text).transpose()?;

        if c.is_unique {
            unique_groups.push(vec![i]);
        }
        if let Some((ref_table, ref_column)) = &c.references {
            foreign_keys.push(ForeignKey {
                column: i,
                ref_table: ref_table.clone(),
                ref_column: ref_column.clone(),
            });
        }
        if c.is_serial {
            serial_columns.push((i, format!("{}_{}_seq", ct.table, c.name)));
        }

        columns.push(ColumnDef {
            name: c.name.clone(),
            col_type,
            nullable: c.nullable,
            default,
            is_pk: c.is_pk,
        });
    }

    for constraint in &ct.table_constraints {
        match constraint {
            crate::sql::ast::TableConstraint::Unique(names) => {
                unique_groups.push(resolve_column_indices(&columns, names)?);
            }
            crate::sql::ast::TableConstraint::ForeignKey {
                column,
                ref_table,
                ref_column,
            } => {
                let idx = resolve_column_indices(&columns, std::slice::from_ref(column))?[0];
                foreign_keys.push(ForeignKey {
                    column: idx,
                    ref_table: ref_table.clone(),
                    ref_column: ref_column.clone(),
                });
            }
        }
    }

    db.create_table_with_constraints(&ct.table, &columns, unique_groups, foreign_keys)?;

    // Canopy (Phase 10): `WITH (json_schema_col = ..., json_schema = '...',
    // json_schema_mode = 'strict' | 'relaxed' | 'off')` attaches a JSON
    // Schema validation rule, enforced on INSERT/UPDATE by `validate_json_schemas`.
    let opt = |key: &str| {
        ct.options
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(key))
            .map(|(_, v)| v.as_str())
    };
    if let Some(col) = opt("json_schema_col") {
        let schema_text = opt("json_schema").ok_or_else(|| {
            anyhow::anyhow!("json_schema_col requires WITH (json_schema = '<json schema text>')")
        })?;
        serde_json::from_str::<serde_json::Value>(schema_text)
            .map_err(|e| anyhow::anyhow!("invalid json_schema: {e}"))?;
        let mode = opt("json_schema_mode").unwrap_or("strict");
        crate::storage::json_schema::Mode::parse(mode).ok_or_else(|| {
            anyhow::anyhow!(
                "invalid json_schema_mode \"{mode}\" (expected strict, relaxed, or off)"
            )
        })?;
        db.set_json_schema(
            &ct.table,
            crate::storage::JsonSchemaRule {
                column: col.to_string(),
                mode: mode.to_string(),
                schema: schema_text.to_string(),
            },
        )?;
    }

    if !serial_columns.is_empty() {
        for (idx, seq_name) in serial_columns {
            db.create_sequence(&seq_name, 1, 1)?;
            columns[idx].default = Some(format!("nextval('{seq_name}')"));
        }
        // Re-persist with the implicit SERIAL defaults now that the backing
        // sequences exist (create_table_with_constraints already wrote the
        // pre-SERIAL-default version).
        let schema = db.get_table(&ct.table)?.expect("just created");
        db.update_table_schema(TableSchema { columns, ..schema })?;
    }

    Ok(QueryResult {
        fields: vec![],
        rows: vec![],
        tag: "CREATE TABLE".into(),
    })
}

pub(super) fn execute_drop_table(
    _dt: crate::sql::ast::DropTableStmt,
    _db: Arc<Database>,
) -> anyhow::Result<QueryResult> {
    // For now, just return success
    Ok(QueryResult {
        fields: vec![],
        rows: vec![],
        tag: "DROP TABLE".into(),
    })
}

pub(super) fn execute_create_index(
    ci: crate::sql::ast::CreateIndexStmt,
    db: Arc<Database>,
) -> anyhow::Result<QueryResult> {
    match ci.using.as_deref() {
        None => db.create_index(&ci.table, &ci.column)?,
        Some(m) if m.eq_ignore_ascii_case("spatial") || m.eq_ignore_ascii_case("gist") => {
            // 1km sizing hint: no SQL syntax yet to tune the underlying grid
            // level per index, so pick a level that comfortably serves
            // typical "within N meters" queries without either bucketing
            // everything into one giant cell or fragmenting into millions.
            const DEFAULT_SPATIAL_RADIUS_HINT_M: f64 = 1000.0;
            db.create_spatial_index(&ci.table, &ci.column, DEFAULT_SPATIAL_RADIUS_HINT_M)?
        }
        Some(m) if m.eq_ignore_ascii_case("time") || m.eq_ignore_ascii_case("chronos") => {
            let opt = |key: &str| ci.options.iter().find(|(k, _)| k.eq_ignore_ascii_case(key)).map(|(_, v)| v.as_str());
            // Default 1-hour buckets — a reasonable middle ground between
            // hourly/daily/monthly partitioning without requiring `interval`
            // on every `CREATE INDEX ... USING TIME`.
            let granularity_ms = match opt("interval") {
                Some(s) => eval::parse_interval(s).ok_or_else(|| anyhow::anyhow!("invalid interval {s:?}"))?,
                None => 3_600_000,
            };
            let retention_ms = match opt("retention") {
                Some(s) => Some(eval::parse_interval(s).ok_or_else(|| anyhow::anyhow!("invalid retention {s:?}"))?),
                None => None,
            };
            let schema = db.get_table(&ci.table)?.ok_or_else(|| anyhow::anyhow!("table \"{}\" does not exist", ci.table))?;
            let value_column = match opt("value") {
                Some(v) => v.to_string(),
                // No explicit `value` option: use the first numeric column
                // that isn't the indexed timestamp column itself.
                None => schema.columns.iter()
                    .find(|c| c.name != ci.column && matches!(c.col_type, crate::storage::ColumnType::Int8 | crate::storage::ColumnType::Int4 | crate::storage::ColumnType::Int2 | crate::storage::ColumnType::Float8 | crate::storage::ColumnType::Float4))
                    .map(|c| c.name.clone())
                    .ok_or_else(|| anyhow::anyhow!("USING TIME requires a numeric value column; specify WITH (value = '<column>')"))?,
            };
            let policy = crate::storage::ts_index::TimeBucketPolicy { granularity_ms, retention_ms };
            db.create_time_index(&ci.table, &ci.column, &value_column, policy)?
        }
        Some(m) if m.eq_ignore_ascii_case("graph") || m.eq_ignore_ascii_case("plexus") => {
            let opt = |key: &str| ci.options.iter().find(|(k, _)| k.eq_ignore_ascii_case(key)).map(|(_, v)| v.as_str());
            let to_column = opt("to").ok_or_else(|| anyhow::anyhow!("USING GRAPH requires WITH (to = '<destination column>')"))?;
            let type_column = opt("type");
            db.create_graph_index(&ci.table, &ci.column, to_column, type_column)?
        }
        Some(m) if m.eq_ignore_ascii_case("jsonpath") => {
            let opt = |key: &str| ci.options.iter().find(|(k, _)| k.eq_ignore_ascii_case(key)).map(|(_, v)| v.as_str());
            let json_path = opt("path")
                .ok_or_else(|| anyhow::anyhow!("USING JSONPATH requires WITH (path = '<dot.separated.path>')"))?;
            db.create_json_path_index(&ci.table, &ci.column, json_path)?
        }
        Some(m) if m.eq_ignore_ascii_case("gin") || m.eq_ignore_ascii_case("fts") => {
            db.create_fts_index(&ci.table, &ci.column)?
        }
        Some(m) if m.eq_ignore_ascii_case("vector") || m.eq_ignore_ascii_case("hnsw") => {
            let opt = |key: &str| ci.options.iter().find(|(k, _)| k.eq_ignore_ascii_case(key)).map(|(_, v)| v.as_str());
            let metric = match opt("metric") {
                Some(s) if s.eq_ignore_ascii_case("cosine") => crate::vector::hnsw::Metric::Cosine,
                Some(s) if s.eq_ignore_ascii_case("l2") => crate::vector::hnsw::Metric::L2,
                Some(other) => anyhow::bail!("unsupported vector metric \"{other}\" (supported: l2, cosine)"),
                None => crate::vector::hnsw::Metric::L2,
            };
            let mut config = crate::vector::hnsw::HnswConfig::default();
            if let Some(s) = opt("m") {
                config.m = s.parse().map_err(|_| anyhow::anyhow!("invalid m {s:?}"))?;
                config.m0 = config.m * 2;
            }
            if let Some(s) = opt("ef_construction") {
                config.ef_construction = s.parse().map_err(|_| anyhow::anyhow!("invalid ef_construction {s:?}"))?;
            }
            if let Some(s) = opt("ef_search") {
                config.ef_search = s.parse().map_err(|_| anyhow::anyhow!("invalid ef_search {s:?}"))?;
            }
            db.create_vector_index(&ci.table, &ci.column, metric, config)?
        }
        Some(m) if m.eq_ignore_ascii_case("ivfpq") || m.eq_ignore_ascii_case("ivf") => {
            let opt = |key: &str| ci.options.iter().find(|(k, _)| k.eq_ignore_ascii_case(key)).map(|(_, v)| v.as_str());
            let metric = match opt("metric") {
                Some(s) if s.eq_ignore_ascii_case("cosine") => crate::vector::hnsw::Metric::Cosine,
                Some(s) if s.eq_ignore_ascii_case("l2") => crate::vector::hnsw::Metric::L2,
                Some(other) => anyhow::bail!("unsupported vector metric \"{other}\" (supported: l2, cosine)"),
                None => crate::vector::hnsw::Metric::L2,
            };
            // Defaults: 100 inverted lists (the FAISS-recommended rule of
            // thumb is roughly sqrt(n) lists; 100 is a reasonable stand-in
            // when the table size isn't known ahead of a WITH-clause
            // override), 8 PQ subvectors, probe the 8 nearest lists.
            let n_lists = match opt("lists") {
                Some(s) => s.parse().map_err(|_| anyhow::anyhow!("invalid lists {s:?}"))?,
                None => 100,
            };
            let pq_m = match opt("pq_m") {
                Some(s) => s.parse().map_err(|_| anyhow::anyhow!("invalid pq_m {s:?}"))?,
                None => 8,
            };
            let n_probe = match opt("n_probe") {
                Some(s) => s.parse().map_err(|_| anyhow::anyhow!("invalid n_probe {s:?}"))?,
                None => 8,
            };
            db.create_ivfpq_index(&ci.table, &ci.column, metric, n_lists, pq_m, n_probe)?
        }
        Some(other) => anyhow::bail!("unsupported index method \"{other}\" (supported: default B-Tree, SPATIAL/GIST, TIME/CHRONOS, GRAPH/PLEXUS, JSONPATH, GIN/FTS, VECTOR/HNSW, IVFPQ)"),
    }
    Ok(QueryResult {
        fields: vec![],
        rows: vec![],
        tag: "CREATE INDEX".into(),
    })
}

pub(super) fn execute_create_sequence(
    cs: crate::sql::ast::CreateSequenceStmt,
    db: Arc<Database>,
) -> anyhow::Result<QueryResult> {
    db.create_sequence(&cs.name, cs.start, cs.increment)?;
    Ok(QueryResult {
        fields: vec![],
        rows: vec![],
        tag: "CREATE SEQUENCE".into(),
    })
}

/// `CREATE TOPIC name WITH (partitions = n, retention = '<interval>',
/// retention_bytes = n)` (Flux). Mirrors `execute_create_index`'s structure:
/// pull known keys out of the generic `options` list, parse durations via
/// `eval::parse_interval` (same parser Chronos's `retention` index option
/// uses), then call the one `Database` method that does the real work.
pub(super) fn execute_create_topic(
    ct: crate::sql::ast::CreateTopicStmt,
    db: Arc<Database>,
) -> anyhow::Result<QueryResult> {
    if ct.if_not_exists && db.list_topics().iter().any(|(name, _)| name == &ct.name) {
        return Ok(QueryResult {
            fields: vec![],
            rows: vec![],
            tag: "CREATE TOPIC".into(),
        });
    }
    let opt = |key: &str| {
        ct.options
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(key))
            .map(|(_, v)| v.as_str())
    };
    let partitions = match opt("partitions") {
        Some(s) => s
            .parse::<u32>()
            .map_err(|_| anyhow::anyhow!("invalid partitions value {s:?}"))?,
        None => 1,
    };
    let retention_ms = match opt("retention") {
        Some(s) => Some(
            eval::parse_interval(s).ok_or_else(|| anyhow::anyhow!("invalid retention {s:?}"))?,
        ),
        None => None,
    };
    let retention_bytes = match opt("retention_bytes") {
        Some(s) => Some(
            s.parse::<u64>()
                .map_err(|_| anyhow::anyhow!("invalid retention_bytes value {s:?}"))?,
        ),
        None => None,
    };
    db.create_topic(&ct.name, partitions, retention_ms, retention_bytes)?;
    Ok(QueryResult {
        fields: vec![],
        rows: vec![],
        tag: "CREATE TOPIC".into(),
    })
}

/// Apply an `ALTER TABLE ... ALTER COLUMN ...` metadata-only action
/// (`SET`/`DROP DEFAULT`, `SET`/`DROP NOT NULL`) by mutating and
/// re-persisting the schema. `ADD`/`DROP COLUMN` would need to backfill
/// every existing row's encoding and are left as a pre-existing TODO.
pub(super) fn execute_alter_table(
    at: crate::sql::ast::AlterTableStmt,
    db: Arc<Database>,
) -> anyhow::Result<QueryResult> {
    use crate::sql::ast::{AlterTableAction, ColumnAction};
    match at.action {
        AlterTableAction::AlterColumn { name, action } => {
            let mut schema = db
                .get_table(&at.table)?
                .ok_or_else(|| anyhow::anyhow!("table \"{}\" does not exist", at.table))?;
            let col = schema
                .columns
                .iter_mut()
                .find(|c| c.name == name)
                .ok_or_else(|| anyhow::anyhow!("column \"{name}\" does not exist"))?;
            match action {
                ColumnAction::SetDefault(expr) => col.default = Some(default_expr_to_text(&expr)?),
                ColumnAction::DropDefault => col.default = None,
                ColumnAction::SetNotNull => col.nullable = false,
                ColumnAction::DropNotNull => col.nullable = true,
            }
            db.update_table_schema(schema)?;
        }
        AlterTableAction::AddColumn(_) | AlterTableAction::DropColumn(_) => {
            // Pre-existing gap (row width/encoding change needs a backfill
            // pass this version doesn't implement) — accepted syntactically,
            // no-op, same as before this change.
        }
    }
    Ok(QueryResult {
        fields: vec![],
        rows: vec![],
        tag: "ALTER TABLE".into(),
    })
}

/// Resolve a `CREATE FUNCTION` type name to the restricted set of types
/// WASM UDFs support — deliberately narrower than `ColumnType::from_name`
/// (which also accepts `text`/`int4`/etc.), since those would require a
/// linear-memory ABI this version doesn't implement.
fn udf_column_type(name: &str) -> anyhow::Result<ColumnType> {
    match name.to_lowercase().as_str() {
        "int8" | "bigint" => Ok(ColumnType::Int8),
        "float8" | "double" | "double precision" => Ok(ColumnType::Float8),
        "bool" | "boolean" => Ok(ColumnType::Bool),
        "float8[]" | "double precision[]" | "double[]" | "float[]" => Ok(ColumnType::Float8Array),
        "bytea" | "blob" => Ok(ColumnType::Bytea),
        other => anyhow::bail!(
            "WASM UDFs only support int8, float8, bool, float8[], and bytea argument/return types, got \"{other}\""
        ),
    }
}

pub(super) fn execute_create_function(
    cf: crate::sql::ast::CreateFunctionStmt,
    db: Arc<Database>,
) -> anyhow::Result<QueryResult> {
    if !cf.language.eq_ignore_ascii_case("wasm") {
        anyhow::bail!(
            "unsupported CREATE FUNCTION language \"{}\" (only \"wasm\" is supported)",
            cf.language
        );
    }

    let arg_types: Vec<ColumnType> = cf
        .args
        .iter()
        .map(|(_, ty)| udf_column_type(ty))
        .collect::<anyhow::Result<_>>()?;
    let return_type = udf_column_type(&cf.return_type)?;

    use base64::Engine as _;
    let wasm_bytes = base64::engine::general_purpose::STANDARD
        .decode(cf.body_base64.trim())
        .map_err(|e| anyhow::anyhow!("CREATE FUNCTION body is not valid base64: {e}"))?;

    udf::validate_module(
        &wasm_bytes,
        &cf.name,
        &arg_types,
        &return_type,
        db.udf_config().max_module_bytes,
    )?;

    db.create_function(crate::storage::UserFunction {
        name: cf.name,
        arg_types,
        return_type,
        wasm_bytes,
    })?;
    Ok(QueryResult {
        fields: vec![],
        rows: vec![],
        tag: "CREATE FUNCTION".into(),
    })
}
