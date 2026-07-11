//! FROM-clause table-valued function dispatch (`table.name` with
//! `table.func_args` set — see `TableRef` docs): Plexus graph
//! traversal/algorithms, Canopy JSON lookups, Prism vector/hybrid search,
//! Synapse recall, Mirror replay, and Flux windowing/time-travel functions.

use std::sync::Arc;

use super::eval::{eval_expr, Value};
use super::parse_rows;
use crate::sql::ast::Expr;
use crate::storage::database::Database;
use crate::storage::{ColumnDef, ColumnType, StorageEngine, TableSchema};

/// Dispatch a table-valued function call in the FROM clause (`table.name`
/// with `table.func_args` set — see `TableRef` docs) to one of the Plexus
/// graph-traversal/algorithm entry points. Arguments are evaluated as
/// constants (`eval_expr`, no row context), consistent with how `CREATE
/// INDEX ... WITH (...)` options are constant-only elsewhere in this engine.
pub(super) fn resolve_graph_function(
    name: &str,
    args: &[Expr],
    db: &Arc<Database>,
    params: &[Value],
) -> anyhow::Result<(Option<Arc<TableSchema>>, Vec<Vec<Option<Vec<u8>>>>)> {
    use crate::graph::Direction;

    let arg_val = |i: usize| -> anyhow::Result<Value> {
        args.get(i)
            .map(|e| eval_expr(e, params))
            .transpose()?
            .ok_or_else(|| anyhow::anyhow!("{name}: missing argument {}", i + 1))
    };
    let arg_text = |i: usize| -> anyhow::Result<String> {
        match arg_val(i)? {
            Value::Text(s) => Ok(s),
            Value::Int(n) => Ok(n.to_string()),
            other => anyhow::bail!(
                "{name}: expected text argument {}, got {}",
                i + 1,
                other.type_name()
            ),
        }
    };
    let arg_bytes = |i: usize| -> anyhow::Result<Vec<u8>> {
        arg_val(i)?
            .to_wire_bytes()
            .ok_or_else(|| anyhow::anyhow!("{name}: argument {} must not be NULL", i + 1))
    };
    let arg_usize_or = |i: usize, default: usize| -> anyhow::Result<usize> {
        match args.get(i) {
            Some(e) => Ok(eval_expr(e, params)?.as_f64()? as usize),
            None => Ok(default),
        }
    };
    let arg_f64_or = |i: usize, default: f64| -> anyhow::Result<f64> {
        match args.get(i) {
            Some(e) => eval_expr(e, params)?.as_f64(),
            None => Ok(default),
        }
    };
    let arg_direction_or = |i: usize, default: Direction| -> anyhow::Result<Direction> {
        match args.get(i) {
            Some(_) => {
                let s = arg_text(i)?;
                Direction::parse(&s).ok_or_else(|| {
                    anyhow::anyhow!("{name}: invalid direction \"{s}\" (expected out/in/both)")
                })
            }
            None => Ok(default),
        }
    };

    fn func_schema(name: &str, cols: &[&str]) -> Arc<TableSchema> {
        Arc::new(TableSchema {
            name: name.to_string(),
            columns: cols
                .iter()
                .map(|c| ColumnDef {
                    name: c.to_string(),
                    col_type: ColumnType::Text,
                    nullable: true,
                    default: None,
                    is_pk: false,
                })
                .collect(),
            pk_columns: vec![],
            unique_groups: vec![],
            foreign_keys: vec![],
            json_schemas: vec![],
        })
    }
    fn cell(b: Vec<u8>) -> Option<Vec<u8>> {
        Some(b)
    }
    fn opt_cell(s: Option<String>) -> Option<Vec<u8>> {
        s.map(|s| s.into_bytes())
    }
    fn num_cell(n: impl ToString) -> Option<Vec<u8>> {
        Some(n.to_string().into_bytes())
    }

    match name.to_ascii_lowercase().as_str() {
        "graph_neighbors" => {
            let table = arg_text(0)?;
            let from_col = arg_text(1)?;
            let key = arg_bytes(2)?;
            let dir = arg_direction_or(3, Direction::Out)?;
            let neighbors = db
                .graph_neighbors(&table, &from_col, &key, dir)
                .ok_or_else(|| {
                    anyhow::anyhow!("no graph index on {table}.{from_col} (or unknown vertex)")
                })?;
            let rows = neighbors
                .into_iter()
                .map(|(k, rel)| vec![cell(k), opt_cell(rel)])
                .collect();
            Ok((
                Some(func_schema("graph_neighbors", &["neighbor", "rel_type"])),
                rows,
            ))
        }
        "graph_bfs" => {
            let table = arg_text(0)?;
            let from_col = arg_text(1)?;
            let key = arg_bytes(2)?;
            let max_depth = arg_usize_or(3, 10)?;
            let dir = arg_direction_or(4, Direction::Out)?;
            let visited = db
                .graph_bfs(&table, &from_col, &key, max_depth, dir)
                .ok_or_else(|| {
                    anyhow::anyhow!("no graph index on {table}.{from_col} (or unknown vertex)")
                })?;
            let rows = visited
                .into_iter()
                .map(|(k, depth)| vec![cell(k), num_cell(depth)])
                .collect();
            Ok((Some(func_schema("graph_bfs", &["vertex", "depth"])), rows))
        }
        "graph_shortest_path" => {
            let table = arg_text(0)?;
            let from_col = arg_text(1)?;
            let start = arg_bytes(2)?;
            let end = arg_bytes(3)?;
            let dir = arg_direction_or(4, Direction::Both)?;
            let path = db
                .graph_shortest_path(&table, &from_col, &start, &end, dir)
                .ok_or_else(|| {
                    anyhow::anyhow!("no graph index on {table}.{from_col} (or unknown vertex)")
                })?;
            let rows = match path {
                Some(vertices) => vertices
                    .into_iter()
                    .enumerate()
                    .map(|(i, k)| vec![num_cell(i), cell(k)])
                    .collect(),
                None => Vec::new(),
            };
            Ok((
                Some(func_schema("graph_shortest_path", &["step", "vertex"])),
                rows,
            ))
        }
        "graph_connected_components" => {
            let table = arg_text(0)?;
            let from_col = arg_text(1)?;
            let comps = db
                .graph_connected_components(&table, &from_col)
                .ok_or_else(|| anyhow::anyhow!("no graph index on {table}.{from_col}"))?;
            let rows = comps
                .into_iter()
                .map(|(k, c)| vec![cell(k), num_cell(c)])
                .collect();
            Ok((
                Some(func_schema(
                    "graph_connected_components",
                    &["vertex", "component"],
                )),
                rows,
            ))
        }
        "graph_pagerank" => {
            let table = arg_text(0)?;
            let from_col = arg_text(1)?;
            let iterations = arg_usize_or(2, 20)?;
            let damping = arg_f64_or(3, 0.85)?;
            let ranks = db
                .graph_pagerank(&table, &from_col, damping, iterations)
                .ok_or_else(|| anyhow::anyhow!("no graph index on {table}.{from_col}"))?;
            let rows = ranks
                .into_iter()
                .map(|(k, r)| vec![cell(k), num_cell(r)])
                .collect();
            Ok((
                Some(func_schema("graph_pagerank", &["vertex", "score"])),
                rows,
            ))
        }
        "graph_triangle_count" => {
            let table = arg_text(0)?;
            let from_col = arg_text(1)?;
            let counts = db
                .graph_triangle_count(&table, &from_col)
                .ok_or_else(|| anyhow::anyhow!("no graph index on {table}.{from_col}"))?;
            let rows = counts
                .into_iter()
                .map(|(k, c)| vec![cell(k), num_cell(c)])
                .collect();
            Ok((
                Some(func_schema(
                    "graph_triangle_count",
                    &["vertex", "triangles"],
                )),
                rows,
            ))
        }
        "json_path_lookup" => {
            let table = arg_text(0)?;
            let column = arg_text(1)?;
            let value_text = arg_text(2)?;
            let keys = db
                .json_path_lookup(&table, &column, &value_text)
                .ok_or_else(|| anyhow::anyhow!("no JSONPATH index on {table}.{column}"))?;
            let rows = keys.into_iter().map(|k| vec![cell(k)]).collect();
            Ok((Some(func_schema("json_path_lookup", &["row_key"])), rows))
        }
        "json_text_search" => {
            let table = arg_text(0)?;
            let column = arg_text(1)?;
            let query = arg_text(2)?;
            let keys = db
                .fts_search(&table, &column, &query)
                .ok_or_else(|| anyhow::anyhow!("no full-text index on {table}.{column}"))?;
            let rows = keys.into_iter().map(|k| vec![cell(k)]).collect();
            Ok((Some(func_schema("json_text_search", &["row_key"])), rows))
        }
        // --- Prism (Phase 7) ----------------------------------------------
        // A k-NN search doesn't fit the planner's WHERE-clause predicate
        // pushdown pattern (`extract_spatial_predicate`/
        // `extract_time_bucket_predicate`) the way Meridian/Chronos do — it's
        // fundamentally an `ORDER BY distance LIMIT k` shape, not a filter.
        // Following Plexus's own precedent (its graph traversals never got
        // planner pushdown either), this is exposed purely as a table-valued
        // function: `vector_search('table', 'column', '[0.1,0.2,...]', k
        // [, ef_search])`. Unlike the graph/JSON functions above (which
        // return a synthetic `row_key`-only schema), this returns the full
        // matched row's real columns plus an appended `distance` column, so
        // hybrid vector + SQL-filter queries (spec section on hybrid search)
        // compose naturally: `SELECT * FROM vector_search(...) v JOIN ...`.
        "vector_search" => {
            let table = arg_text(0)?;
            let column = arg_text(1)?;
            let query_text = arg_text(2)?;
            let k = arg_usize_or(3, 10)?;
            let ef_search = if args.len() > 4 {
                Some(arg_usize_or(4, 0)?)
            } else {
                None
            };
            let query = crate::vector::vector::Vector::from_text(&query_text)
                .map_err(|e| anyhow::anyhow!("vector_search: invalid query vector: {e}"))?;
            let hits = db
                .vector_knn_query(&table, &column, query.as_slice(), k, ef_search)
                .ok_or_else(|| anyhow::anyhow!("no vector index on {table}.{column}"))?;
            let table_schema = db
                .get_table(&table)?
                .ok_or_else(|| anyhow::anyhow!("table \"{table}\" does not exist"))?;
            let kvs: Vec<crate::storage::KeyValue> =
                hits.iter().map(|(kv, _)| kv.clone()).collect();
            let mut rows = parse_rows(&kvs, &Some(table_schema.clone()));
            for (row, (_, dist)) in rows.iter_mut().zip(hits.iter()) {
                row.push(num_cell(*dist as f64));
            }
            let mut out_schema = table_schema;
            out_schema.columns.push(ColumnDef {
                name: "distance".to_string(),
                col_type: ColumnType::Float8,
                nullable: false,
                default: None,
                is_pk: false,
            });
            Ok((Some(Arc::new(out_schema)), rows))
        }
        // Hybrid vector + BM25 full-text search, fused by Reciprocal Rank
        // Fusion (RRF, `score = sum(1 / (60 + rank))` over whichever of the
        // two ranked lists a row appears in, 1-indexed rank, standard
        // constant 60 per the original RRF paper — no tunable weights
        // between the two signals, same "pick the well-known default, don't
        // add a knob nobody asked for" precedent as BM25's k1/b above).
        // Still two internal lookups (HNSW k-NN, FTS BM25) merged into one
        // ranked result in a single table function — not literally a single
        // index scan, an honest scope note consistent with every other
        // "hybrid" checklist item in this codebase.
        "hybrid_search" => {
            let table = arg_text(0)?;
            let vec_column = arg_text(1)?;
            let vec_query_text = arg_text(2)?;
            let fts_column = arg_text(3)?;
            let fts_query_text = arg_text(4)?;
            let k = arg_usize_or(5, 10)?;
            let pool = (k * 4).max(50);

            let vec_query = crate::vector::vector::Vector::from_text(&vec_query_text)
                .map_err(|e| anyhow::anyhow!("hybrid_search: invalid query vector: {e}"))?;
            let vec_hits = db
                .vector_knn_query(&table, &vec_column, vec_query.as_slice(), pool, None)
                .ok_or_else(|| anyhow::anyhow!("no vector index on {table}.{vec_column}"))?;
            let bm25_hits = db
                .fts_search_bm25(&table, &fts_column, &fts_query_text, pool)
                .ok_or_else(|| anyhow::anyhow!("no full-text index on {table}.{fts_column}"))?;

            const RRF_K: f64 = 60.0;
            let mut fused: std::collections::HashMap<Vec<u8>, (f64, Option<f32>, Option<f64>)> =
                std::collections::HashMap::new();
            for (rank, (kv, dist)) in vec_hits.iter().enumerate() {
                let entry = fused.entry(kv.key.clone()).or_insert((0.0, None, None));
                entry.0 += 1.0 / (RRF_K + rank as f64 + 1.0);
                entry.1 = Some(*dist);
            }
            for (rank, (row_key, score)) in bm25_hits.iter().enumerate() {
                let entry = fused.entry(row_key.clone()).or_insert((0.0, None, None));
                entry.0 += 1.0 / (RRF_K + rank as f64 + 1.0);
                entry.2 = Some(*score);
            }

            let mut ranked: Vec<(Vec<u8>, f64, Option<f32>, Option<f64>)> = fused
                .into_iter()
                .map(|(key, (fused_score, dist, bm25))| (key, fused_score, dist, bm25))
                .collect();
            ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            ranked.truncate(k);

            let table_schema = db
                .get_table(&table)?
                .ok_or_else(|| anyhow::anyhow!("table \"{table}\" does not exist"))?;
            let keys: Vec<Vec<u8>> = ranked.iter().map(|(key, ..)| key.clone()).collect();
            let kvs = db.rows_by_keys(&table, &keys);
            let kv_by_key: std::collections::HashMap<&[u8], &crate::storage::KeyValue> =
                kvs.iter().map(|kv| (kv.key.as_slice(), kv)).collect();

            let mut rows = Vec::with_capacity(ranked.len());
            for (key, fused_score, dist, bm25) in &ranked {
                let Some(kv) = kv_by_key.get(key.as_slice()) else {
                    continue;
                };
                let mut row =
                    parse_rows(std::slice::from_ref(*kv), &Some(table_schema.clone())).remove(0);
                row.push(dist.map(|d| num_cell(d as f64)).unwrap_or(None));
                row.push(bm25.map(num_cell).unwrap_or(None));
                row.push(num_cell(*fused_score));
                rows.push(row);
            }

            let mut out_schema = table_schema;
            out_schema.columns.push(ColumnDef {
                name: "vec_distance".to_string(),
                col_type: ColumnType::Float8,
                nullable: true,
                default: None,
                is_pk: false,
            });
            out_schema.columns.push(ColumnDef {
                name: "bm25_score".to_string(),
                col_type: ColumnType::Float8,
                nullable: true,
                default: None,
                is_pk: false,
            });
            out_schema.columns.push(ColumnDef {
                name: "fused_score".to_string(),
                col_type: ColumnType::Float8,
                nullable: false,
                default: None,
                is_pk: false,
            });
            Ok((Some(Arc::new(out_schema)), rows))
        }
        // Canopy's MongoDB-compatible aggregation pipeline (Phase 10):
        // `aggregate('table', '<json array of pipeline stages>')`. Turns
        // each row of `table` into a JSON document (using the row's own
        // column names/values — a `Json`-typed column's text is parsed as
        // nested JSON, every other column becomes a JSON scalar), runs it
        // through `canopy_aggregate::run_pipeline`, and re-flattens the
        // resulting documents into rows: the output schema is the union of
        // every key seen across the result documents (first-seen order),
        // each cell either the field's scalar text or, for an
        // object/array-valued field (e.g. `$group`'s `_id` when it's a
        // compound key, or `$push`'s array), its compact JSON text — same
        // "no dedicated binary cell type, reuse text" precedent as every
        // other engine-extension value in this codebase.
        "aggregate" => {
            let table = arg_text(0)?;
            let pipeline_text = arg_text(1)?;
            let stages_val: serde_json::Value = serde_json::from_str(&pipeline_text)
                .map_err(|e| anyhow::anyhow!("aggregate: invalid pipeline JSON: {e}"))?;
            let stages = stages_val
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("aggregate: pipeline must be a JSON array of stages"))?
                .clone();

            let schema = db
                .get_table(&table)?
                .ok_or_else(|| anyhow::anyhow!("table \"{table}\" does not exist"))?;
            let schema_arc = Arc::new(schema);
            let raw_rows = db.scan(&table)?;
            let rows = parse_rows(&raw_rows, &Some((*schema_arc).clone()));

            let docs: Vec<serde_json::Map<String, serde_json::Value>> = rows
                .iter()
                .map(|row| {
                    let ctx = super::eval::RowContext::new(row.clone(), Some(schema_arc.clone()));
                    let mut map = serde_json::Map::new();
                    for col in &schema_arc.columns {
                        let v = ctx.eval(&Expr::Ident(col.name.clone())).unwrap_or(Value::Null);
                        map.insert(col.name.clone(), value_to_json(&v, &col.col_type));
                    }
                    map
                })
                .collect();

            let result_docs = crate::executor::canopy_aggregate::run_pipeline(docs, &stages)?;

            let mut columns: Vec<String> = Vec::new();
            for doc in &result_docs {
                for key in doc.keys() {
                    if !columns.contains(key) {
                        columns.push(key.clone());
                    }
                }
            }
            let out_schema = func_schema(
                "aggregate",
                &columns.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            );
            let out_rows = result_docs
                .iter()
                .map(|doc| {
                    columns
                        .iter()
                        .map(|c| doc.get(c).and_then(json_to_cell))
                        .collect()
                })
                .collect();
            Ok((Some(out_schema), out_rows))
        }
        // --- Synapse (Phase 16) -------------------------------------------
        // Only the two ranked-recall paths get a SQL surface — agent
        // lifecycle (spawn/pause/checkpoint), task delegation, and shared
        // workflow state are Rust `Database`-adjacent APIs
        // (`synapse::actor`/`synapse::coordination`), not new SQL statements,
        // mirroring Flux's own "polling is a `Database` method, not new SQL
        // syntax" precedent. These two fit the same `vector_search`
        // "ORDER BY distance LIMIT k" shape, so they get the same
        // table-function treatment.
        "synapse_recall_semantic" => {
            let agent_id = arg_text(0)?;
            let query_text = arg_text(1)?;
            let k = arg_usize_or(2, 10)?;
            let query = crate::vector::vector::Vector::from_text(&query_text).map_err(|e| {
                anyhow::anyhow!("synapse_recall_semantic: invalid query vector: {e}")
            })?;
            let mem = crate::synapse::memory::MemoryStore::new(db.clone())?;
            let hits = mem.recall_semantic(&agent_id, query.as_slice(), k)?;
            let rows = hits
                .into_iter()
                .map(|(entry, dist)| {
                    vec![
                        cell(entry.id.into_bytes()),
                        cell(entry.content.into_bytes()),
                        num_cell(dist as f64),
                    ]
                })
                .collect();
            Ok((
                Some(func_schema(
                    "synapse_recall_semantic",
                    &["id", "content", "distance"],
                )),
                rows,
            ))
        }
        "synapse_discover_tools" => {
            let query_text = arg_text(0)?;
            let k = arg_usize_or(1, 10)?;
            let query = crate::vector::vector::Vector::from_text(&query_text).map_err(|e| {
                anyhow::anyhow!("synapse_discover_tools: invalid query vector: {e}")
            })?;
            let reg = crate::synapse::tools::ToolRegistry::new(db.clone())?;
            let hits = reg.discover(query.as_slice(), k)?;
            let rows = hits
                .into_iter()
                .map(|(tool, dist)| {
                    vec![
                        cell(tool.name.into_bytes()),
                        cell(tool.description.into_bytes()),
                        num_cell(dist as f64),
                    ]
                })
                .collect();
            Ok((
                Some(func_schema(
                    "synapse_discover_tools",
                    &["name", "description", "distance"],
                )),
                rows,
            ))
        }
        // --- Mirror (Phase 17) --------------------------------------------
        // Read-only data sources a real dashboard/debug REPL would query —
        // see `mirror` module docs for why no dashboard UI itself was
        // built in this pass. Both replay a Flux/Chronos-backed history
        // rather than a live table, so (unlike `vector_search`/
        // `synapse_recall_semantic`) there's no "distance" column — these
        // are ordered event/metric feeds, not ranked search results.
        "mirror_session_events" => {
            let session_id = arg_text(0)?;
            let engine = crate::mirror::replay::ReplayEngine::new(db.clone());
            let events = engine.replay_session(&session_id)?;
            let rows = events
                .into_iter()
                .map(|e| {
                    vec![
                        num_cell(e.offset),
                        num_cell(e.ts),
                        cell(e.agent_id.into_bytes()),
                        cell(e.kind.into_bytes()),
                        cell(e.detail.into_bytes()),
                        opt_cell(e.tool_name),
                        opt_cell(e.input),
                        opt_cell(e.output),
                        opt_cell(e.error),
                    ]
                })
                .collect();
            // "offset" is a reserved SQL keyword in this parser (the
            // LIMIT/OFFSET clause) — `evt_offset` avoids colliding with it
            // in ORDER BY/WHERE.
            Ok((
                Some(func_schema(
                    "mirror_session_events",
                    &[
                        "evt_offset",
                        "ts",
                        "agent_id",
                        "kind",
                        "detail",
                        "tool_name",
                        "input",
                        "output",
                        "error",
                    ],
                )),
                rows,
            ))
        }
        "mirror_agent_metrics" => {
            let agent_id = arg_text(0)?;
            let t0 = arg_val(1)?.as_f64()? as i64;
            let t1 = arg_val(2)?.as_f64()? as i64;
            let store = crate::mirror::metrics::MetricsStore::new(db.clone())?;
            let entries = store.range(&agent_id, t0, t1)?;
            let rows = entries
                .into_iter()
                .map(|e| {
                    vec![
                        cell(e.agent_id.into_bytes()),
                        cell(e.session_id.into_bytes()),
                        num_cell(e.latency_ms),
                        num_cell(e.tokens),
                        num_cell(if e.success { 1 } else { 0 }),
                        num_cell(e.ts),
                    ]
                })
                .collect();
            Ok((
                Some(func_schema(
                    "mirror_agent_metrics",
                    &[
                        "agent_id",
                        "session_id",
                        "latency_ms",
                        "tokens",
                        "success",
                        "ts",
                    ],
                )),
                rows,
            ))
        }
        // --- Flux (Phase 11) ---------------------------------------------
        "flux_time_travel" => {
            let table = arg_text(0)?;
            let cutoff_ms = arg_val(1)?.as_f64()? as i64;
            let topic = format!("__cdc_{table}");
            let records = db.flux_all(&topic, 0)
                .ok_or_else(|| anyhow::anyhow!("no CDC events for table \"{table}\" (topic \"{topic}\" doesn't exist yet — insert/update/delete something first)"))?;
            // Reconstructed state, `row_key` (hex-encoded, matching
            // `publish_cdc_event`) -> the row's `after` JSON as of the last
            // applicable event. A `BTreeMap` just for deterministic output
            // ordering, not because row keys are ordered data.
            let mut state: std::collections::BTreeMap<String, serde_json::Value> =
                std::collections::BTreeMap::new();
            for rec in &records {
                let event: serde_json::Value = match serde_json::from_slice(&rec.value) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let ts = event.get("ts").and_then(|v| v.as_i64()).unwrap_or(i64::MAX);
                if ts > cutoff_ms {
                    continue;
                }
                let row_key = event
                    .get("row_key")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                if event.get("op").and_then(|v| v.as_str()) == Some("delete") {
                    state.remove(&row_key);
                } else if let Some(after) = event.get("after") {
                    state.insert(row_key, after.clone());
                }
            }
            // Generic `(row_key, data)` shape rather than the live table
            // schema: `after` is already a JSON object of column name ->
            // text value (see `publish_cdc_event`), and the table's schema
            // may itself have changed since the events being replayed were
            // recorded, so re-serializing to JSON is the only shape that's
            // always honest about what was actually captured.
            let rows = state
                .into_iter()
                .map(|(k, v)| vec![cell(k.into_bytes()), cell(v.to_string().into_bytes())])
                .collect();
            Ok((
                Some(func_schema("flux_time_travel", &["row_key", "data"])),
                rows,
            ))
        }
        "flux_window_tumbling" => {
            let topic = arg_text(0)?;
            let window_ms = arg_val(1)?.as_f64()? as i64;
            anyhow::ensure!(
                window_ms > 0,
                "flux_window_tumbling: window_ms must be positive"
            );
            let n = db
                .flux_num_partitions(&topic)
                .ok_or_else(|| anyhow::anyhow!("topic \"{topic}\" does not exist"))?;
            anyhow::ensure!(n == 1, "flux_window_tumbling only supports single-partition topics (multi-partition merge not implemented — see storage::flux module docs)");
            let records = db.flux_all(&topic, 0).unwrap_or_default();
            let mut buckets: std::collections::BTreeMap<i64, u64> =
                std::collections::BTreeMap::new();
            for rec in &records {
                let start = rec.timestamp_ms.div_euclid(window_ms) * window_ms;
                *buckets.entry(start).or_insert(0) += 1;
            }
            let rows = buckets
                .into_iter()
                .map(|(start, count)| {
                    vec![
                        num_cell(start),
                        num_cell(start + window_ms),
                        num_cell(count),
                    ]
                })
                .collect();
            Ok((
                Some(func_schema(
                    "flux_window_tumbling",
                    &["window_start", "window_end", "count"],
                )),
                rows,
            ))
        }
        "flux_window_session" => {
            let topic = arg_text(0)?;
            let gap_ms = arg_val(1)?.as_f64()? as i64;
            anyhow::ensure!(gap_ms > 0, "flux_window_session: gap_ms must be positive");
            let n = db
                .flux_num_partitions(&topic)
                .ok_or_else(|| anyhow::anyhow!("topic \"{topic}\" does not exist"))?;
            anyhow::ensure!(n == 1, "flux_window_session only supports single-partition topics (multi-partition merge not implemented — see storage::flux module docs)");
            let mut records = db.flux_all(&topic, 0).unwrap_or_default();
            records.sort_by_key(|r| r.timestamp_ms);
            let mut rows = Vec::new();
            let mut current: Option<(i64, i64, u64)> = None; // (start, end, count)
            for rec in &records {
                current = Some(match current {
                    Some((start, end, count)) if rec.timestamp_ms - end <= gap_ms => {
                        (start, rec.timestamp_ms, count + 1)
                    }
                    Some((start, end, count)) => {
                        rows.push(vec![num_cell(start), num_cell(end), num_cell(count)]);
                        (rec.timestamp_ms, rec.timestamp_ms, 1)
                    }
                    None => (rec.timestamp_ms, rec.timestamp_ms, 1),
                });
            }
            if let Some((start, end, count)) = current {
                rows.push(vec![num_cell(start), num_cell(end), num_cell(count)]);
            }
            Ok((
                Some(func_schema(
                    "flux_window_session",
                    &["window_start", "window_end", "count"],
                )),
                rows,
            ))
        }
        "flux_window_sliding" => {
            let topic = arg_text(0)?;
            let window_size_ms = arg_val(1)?.as_f64()? as i64;
            let slide_ms = arg_val(2)?.as_f64()? as i64;
            anyhow::ensure!(
                window_size_ms > 0 && slide_ms > 0,
                "flux_window_sliding: window_size_ms and slide_ms must be positive"
            );
            let n = db
                .flux_num_partitions(&topic)
                .ok_or_else(|| anyhow::anyhow!("topic \"{topic}\" does not exist"))?;
            anyhow::ensure!(n == 1, "flux_window_sliding only supports single-partition topics (multi-partition merge not implemented — see storage::flux module docs)");
            let records = db.flux_all(&topic, 0).unwrap_or_default();
            let mut rows = Vec::new();
            if let (Some(min_ts), Some(max_ts)) = (
                records.iter().map(|r| r.timestamp_ms).min(),
                records.iter().map(|r| r.timestamp_ms).max(),
            ) {
                // One row per slide boundary >= the first record, each
                // covering the trailing `[boundary - window_size_ms,
                // boundary)` window — boundaries with zero records in range
                // are skipped rather than emitted as empty rows.
                let mut boundary = min_ts.div_euclid(slide_ms) * slide_ms + slide_ms;
                while boundary - window_size_ms <= max_ts {
                    let window_start = boundary - window_size_ms;
                    let count = records
                        .iter()
                        .filter(|r| r.timestamp_ms >= window_start && r.timestamp_ms < boundary)
                        .count() as u64;
                    if count > 0 {
                        rows.push(vec![
                            num_cell(window_start),
                            num_cell(boundary),
                            num_cell(count),
                        ]);
                    }
                    boundary += slide_ms;
                }
            }
            Ok((
                Some(func_schema(
                    "flux_window_sliding",
                    &["window_start", "window_end", "count"],
                )),
                rows,
            ))
        }
        other => anyhow::bail!("unknown table function \"{other}\""),
    }
}

/// Converts one evaluated row cell to a JSON value for
/// `canopy_aggregate::run_pipeline`'s document model: a `Json`-typed
/// column's text is parsed as nested JSON (falling back to a JSON string if
/// it's not valid JSON, e.g. a legacy/malformed cell), every other type maps
/// to the corresponding JSON scalar.
fn value_to_json(v: &Value, col_type: &ColumnType) -> serde_json::Value {
    if *col_type == ColumnType::Json {
        if let Value::Text(s) = v {
            return serde_json::from_str(s).unwrap_or_else(|_| serde_json::Value::String(s.clone()));
        }
    }
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(n) => serde_json::Value::Number((*n).into()),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Text(s) => serde_json::Value::String(s.clone()),
    }
}

/// The inverse direction for `aggregate`'s output rows: a JSON scalar
/// becomes its plain text form, an object/array becomes its compact JSON
/// text (no dedicated binary cell type for this table function's output,
/// same "reuse text" precedent as every other engine-extension value in
/// this codebase), and `null` becomes a SQL NULL cell.
fn json_to_cell(v: &serde_json::Value) -> Option<Vec<u8>> {
    match v {
        serde_json::Value::Null => None,
        serde_json::Value::Bool(b) => Some(if *b { b"t".to_vec() } else { b"f".to_vec() }),
        serde_json::Value::Number(n) => Some(n.to_string().into_bytes()),
        serde_json::Value::String(s) => Some(s.as_bytes().to_vec()),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            serde_json::to_string(v).ok().map(|s| s.into_bytes())
        }
    }
}
