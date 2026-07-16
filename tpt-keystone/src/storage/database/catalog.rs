use anyhow::Result;

use super::Database;
use crate::sql::ast::Stmt;
use crate::storage::config::UdfConfig;
use crate::storage::ColumnDef;
use crate::storage::ForeignKey;
use crate::storage::JsonSchemaRule;
use crate::storage::KeyValue;
use crate::storage::Sequence;
use crate::storage::StorageEngine;
use crate::storage::TableSchema;
use crate::storage::UserFunction;
use tracing::info;

impl Database {
    /// Create a table with `UNIQUE`/`FOREIGN KEY` constraints attached
    /// (indices already resolved against `columns`). The plain
    /// `StorageEngine::create_table` is a thin wrapper over this with no
    /// constraints, for callers (tests, `COPY`, CTE materialization) that
    /// don't need them.
    pub fn create_table_with_constraints(
        &self,
        name: &str,
        columns: &[ColumnDef],
        unique_groups: Vec<Vec<usize>>,
        foreign_keys: Vec<ForeignKey>,
    ) -> Result<()> {
        self.check_writable()?;
        let mut schemas = self.schemas.lock().unwrap();
        if schemas.contains_key(name) {
            anyhow::bail!("table \"{name}\" already exists");
        }

        let pk_columns: Vec<usize> = columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.is_pk)
            .map(|(i, _)| i)
            .collect();

        let schema = TableSchema {
            name: name.to_string(),
            columns: columns.to_vec(),
            pk_columns,
            unique_groups,
            foreign_keys,
            json_schemas: vec![],
        };

        // Persist schema to the shared object store so every compute node
        // (writer or reader) sees the same table catalog.
        self.store
            .put(&format!("schemas/{name}.bin"), &schema.encode()?)?;

        schemas.insert(name.to_string(), schema);
        info!(table = name, "table created");
        Ok(())
    }

    /// Overwrite an existing table's schema (used by `ALTER TABLE ... ALTER
    /// COLUMN ... SET/DROP DEFAULT|NOT NULL`, which only mutate column
    /// metadata — row width/encoding is untouched, so no backfill is
    /// needed).
    pub fn update_table_schema(&self, schema: TableSchema) -> Result<()> {
        self.check_writable()?;
        self.store
            .put(&format!("schemas/{}.bin", schema.name), &schema.encode()?)?;
        self.schemas
            .lock()
            .unwrap()
            .insert(schema.name.clone(), schema);
        Ok(())
    }

    /// Encode a schema-ordered row of wire-encoded cells into the
    /// length-prefixed on-disk row value (`len(4) | bytes`, or `-1i32` for
    /// NULL). Mirrors `executor::dml::build_row_value`'s cell layout (without
    /// the binary-jsonb rewrite path — ALTER backfill preserves each existing
    /// cell verbatim and only synthesizes a fresh cell for the affected
    /// column).
    pub(crate) fn encode_row_cells(cells: &[Option<Vec<u8>>]) -> Vec<u8> {
        let mut buf = Vec::new();
        for cell in cells {
            match cell {
                Some(data) => {
                    buf.extend_from_slice(&(data.len() as u32).to_be_bytes());
                    buf.extend_from_slice(data);
                }
                None => buf.extend_from_slice(&(-1i32).to_be_bytes()),
            }
        }
        buf
    }

    /// Decode a length-prefixed on-disk row value back into schema-ordered
    /// cells (`None` for a `-1i32` NULL sentinel). Native-binary jsonb cells
    /// are decoded to canonical JSON text so the backfilled row is
    /// self-consistent with the normal read path.
    fn decode_row_cells(value: &[u8]) -> Vec<Option<Vec<u8>>> {
        let mut cells = Vec::new();
        let mut pos = 0usize;
        while pos + 4 <= value.len() {
            let len = i32::from_be_bytes(value[pos..pos + 4].try_into().unwrap());
            pos += 4;
            if len < 0 {
                cells.push(None);
                continue;
            }
            let end = pos + len as usize;
            if end > value.len() {
                cells.push(None);
                break;
            }
            let cell = &value[pos..end];
            match crate::storage::jsonb::decode_cell(cell) {
                Some(text) => cells.push(Some(text)),
                None => cells.push(Some(cell.to_vec())),
            }
            pos = end;
        }
        cells
    }

    /// Apply `ALTER TABLE ... ADD COLUMN` by appending a synthesized cell to
    /// every existing row and re-persisting it. `default_cell` is the
    /// wire-encoded default value for the new column (or `None` for NULL),
    /// already resolved by the executor (so `nextval()`/`DEFAULT` expressions
    /// work). The whole scan-rewrite pass holds the LSM mutex once to avoid
    /// interleaving with concurrent writers; it does not re-enter the
    /// `StorageEngine` trait methods (which would re-lock and could mutate
    /// indexes mid-rewrite).
    ///
    /// Non-crash-atomicity is a documented limitation: a crash mid-rewrite
    /// leaves a partially-migrated table. The whole pass is serialized under
    /// the global LSM lock, so concurrent readers see either all-old or
    /// all-new rows but never a torn mix at the row level.
    pub fn alter_table_add_column(
        &self,
        table: &str,
        new_col: ColumnDef,
        default_cell: Option<Vec<u8>>,
    ) -> Result<()> {
        self.check_writable()?;
        let mut schema = self
            .get_table(table)?
            .ok_or_else(|| anyhow::anyhow!("table \"{table}\" does not exist"))?;
        if schema.columns.iter().any(|c| c.name == new_col.name) {
            anyhow::bail!("column \"{}\" of relation \"{}\" already exists", new_col.name, table);
        }

        let mut lsm = self.lsm.lock().unwrap();
        let prefix = Self::make_prefix(table);
        let rows = lsm.scan()?;
        for (composite_key, value) in rows.iter().filter(|(k, _)| k.starts_with(&prefix)) {
            let mut cells = Self::decode_row_cells(value);
            cells.push(default_cell.clone());
            let new_value = Self::encode_row_cells(&cells);
            lsm.write(table, composite_key, &new_value)?;
        }
        drop(lsm);

        schema.columns.push(new_col);
        self.update_table_schema(schema)?;
        info!(table, "ADD COLUMN applied");
        Ok(())
    }

    /// Apply `ALTER TABLE ... DROP COLUMN`. Rejects dropping a column that
    /// participates in the primary key, a UNIQUE group, a FOREIGN KEY, or a
    /// secondary index (B-Tree). Like `alter_table_add_column`, the whole
    /// scan-rewrite pass holds the LSM mutex once and does not re-enter the
    /// `StorageEngine` trait methods.
    pub fn alter_table_drop_column(&self, table: &str, column: &str) -> Result<()> {
        self.check_writable()?;
        let mut schema = self
            .get_table(table)?
            .ok_or_else(|| anyhow::anyhow!("table \"{table}\" does not exist"))?;
        let idx = schema
            .columns
            .iter()
            .position(|c| c.name == column)
            .ok_or_else(|| anyhow::anyhow!("column \"{column}\" does not exist"))?;

        if schema.pk_columns.contains(&idx) {
            anyhow::bail!("cannot drop column \"{column}\" because it is part of the primary key");
        }
        if schema.unique_groups.iter().any(|g| g.contains(&idx)) {
            anyhow::bail!("cannot drop column \"{column}\" because it is part of a UNIQUE constraint");
        }
        if schema.foreign_keys.iter().any(|fk| fk.column == idx) {
            anyhow::bail!("cannot drop column \"{column}\" because it is part of a FOREIGN KEY constraint");
        }
        let indexed = {
            let idx_map = self.indexes.lock().unwrap();
            idx_map
                .get(table)
                .is_some_and(|cols| cols.contains_key(column))
        };
        if indexed {
            anyhow::bail!("cannot drop column \"{column}\" because an index depends on it");
        }

        let mut lsm = self.lsm.lock().unwrap();
        let prefix = Self::make_prefix(table);
        let rows = lsm.scan()?;
        for (composite_key, value) in rows.iter().filter(|(k, _)| k.starts_with(&prefix)) {
            let mut cells = Self::decode_row_cells(value);
            cells.remove(idx);
            let new_value = Self::encode_row_cells(&cells);
            lsm.write(table, composite_key, &new_value)?;
        }
        drop(lsm);

        schema.columns.remove(idx);
        schema.pk_columns.retain(|&c| c != idx);
        schema.pk_columns.iter_mut().for_each(|c| {
            if *c > idx {
                *c -= 1;
            }
        });
        schema.unique_groups.iter_mut().for_each(|g| {
            g.retain(|&c| c != idx);
            g.iter_mut().for_each(|c| {
                if *c > idx {
                    *c -= 1;
                }
            });
        });
        schema.foreign_keys.retain(|fk| fk.column != idx);
        schema.foreign_keys.iter_mut().for_each(|fk| {
            if fk.column > idx {
                fk.column -= 1;
            }
        });
        self.update_table_schema(schema)?;
        info!(table, "DROP COLUMN applied");
        Ok(())
    }

    /// Drop a table: remove its schema from the shared catalog, purge every
    /// row from the LSM store, and clear all of its per-table secondary
    /// indexes (B-Tree, spatial, time, graph, JSON-path, full-text, vector,
    /// IVF-PQ). Reader-node convergence of the dropped schema is a documented
    /// gap — `refresh()` only `or_insert`s newly seen schemas, so a reader may
    /// keep serving a dropped table until it restarts (tracked as a known
    /// follow-up).
    pub fn drop_table(&self, name: &str, if_exists: bool) -> Result<()> {
        self.check_writable()?;

        let schema_exists = self.schemas.lock().unwrap().contains_key(name);
        if !schema_exists {
            if if_exists {
                return Ok(());
            }
            anyhow::bail!("table \"{name}\" does not exist");
        }

        // Purge all rows for this table from the durable LSM store.
        let prefix = Self::make_prefix(name);
        let all = self.lsm.lock().unwrap().scan()?;
        let mut lsm = self.lsm.lock().unwrap();
        for (key, _value) in all.iter().filter(|(k, _)| k.starts_with(&prefix)) {
            lsm.delete(name, key)?;
        }
        drop(lsm);

        // Remove the schema from the shared object store and in-memory map.
        self.store.delete(&format!("schemas/{name}.bin"))?;
        self.schemas.lock().unwrap().remove(name);

        // Drop every per-table secondary index (in-memory maps + on-disk files).
        self.indexes.lock().unwrap().remove(name);
        self.geo_indexes.lock().unwrap().remove(name);
        self.ts_indexes.lock().unwrap().remove(name);
        self.graph_indexes.lock().unwrap().remove(name);
        self.json_indexes.lock().unwrap().remove(name);
        self.fts_indexes.lock().unwrap().remove(name);
        self.vector_indexes.lock().unwrap().remove(name);
        self.ivf_pq_indexes.lock().unwrap().remove(name);

        let index_dir = &self.local_index_dir;
        if index_dir.exists() {
            if let Ok(entries) = std::fs::read_dir(index_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                        if let Some((table, _col)) = stem.split_once('_') {
                            if table == name {
                                let _ = std::fs::remove_file(&path);
                            }
                        }
                    }
                }
            }
        }

        // Drop any implicit `__cdc_<table>` Flux topic created for this table.
        self.topics.lock().unwrap().remove(&format!("__cdc_{name}"));

        info!(table = name, "table dropped");
        Ok(())
    }

    /// Create a named sequence, persisted like `TableSchema`/`UserFunction`.
    pub fn create_sequence(&self, name: &str, start: i64, increment: i64) -> Result<()> {
        self.check_writable()?;
        let mut sequences = self.sequences.lock().unwrap();
        if sequences.contains_key(name) {
            anyhow::bail!("sequence \"{name}\" already exists");
        }
        // A sequence's first `nextval()` returns `start`, so the stored
        // "last value" starts one increment behind it.
        let seq = Sequence {
            name: name.to_string(),
            value: start - increment,
            increment,
        };
        self.store
            .put(&format!("sequences/{name}.bin"), &seq.encode()?)?;
        sequences.insert(name.to_string(), seq);
        Ok(())
    }

    /// Atomically advance and return a sequence's next value.
    pub fn nextval(&self, name: &str) -> Result<i64> {
        self.check_writable()?;
        let mut sequences = self.sequences.lock().unwrap();
        let seq = sequences
            .get_mut(name)
            .ok_or_else(|| anyhow::anyhow!("sequence \"{name}\" does not exist"))?;
        seq.value += seq.increment;
        let value = seq.value;
        self.store
            .put(&format!("sequences/{name}.bin"), &seq.encode()?)?;
        Ok(value)
    }

    /// The sequence's current value (process-wide, not per-session — see
    /// `storage::Sequence`'s doc comment for why).
    pub fn currval(&self, name: &str) -> Result<i64> {
        let sequences = self.sequences.lock().unwrap();
        let seq = sequences
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("sequence \"{name}\" does not exist"))?;
        Ok(seq.value)
    }

    /// Set a sequence's value (`SELECT setval(name, value [, is_called])`).
    /// `is_called = true` (Postgres's default) means the *next* `nextval()`
    /// returns `value + increment`; `is_called = false` means the next
    /// `nextval()` returns `value` itself. Returns `value`, matching
    /// Postgres's `setval()`.
    pub fn setval(&self, name: &str, value: i64, is_called: bool) -> Result<i64> {
        self.check_writable()?;
        let mut sequences = self.sequences.lock().unwrap();
        let seq = sequences
            .get_mut(name)
            .ok_or_else(|| anyhow::anyhow!("sequence \"{name}\" does not exist"))?;
        seq.value = if is_called {
            value
        } else {
            value - seq.increment
        };
        self.store
            .put(&format!("sequences/{name}.bin"), &seq.encode()?)?;
        Ok(value)
    }

    /// List all sequences (name, current value, increment), for
    /// `pg_catalog.pg_sequence` introspection.
    pub fn list_sequences(&self) -> Vec<Sequence> {
        self.sequences.lock().unwrap().values().cloned().collect()
    }

    /// Whether a B-Tree index exists for `table.column`.
    pub fn indexed_column(&self, table: &str, column: &str) -> bool {
        self.indexes
            .lock()
            .unwrap()
            .get(table)
            .is_some_and(|m| m.contains_key(column))
    }

    /// List all `(table, column)` pairs that have a B-Tree index, for
    /// `pg_catalog.pg_indexes` introspection.
    pub fn list_indexes(&self) -> Vec<(String, String)> {
        self.indexes
            .lock()
            .unwrap()
            .iter()
            .flat_map(|(table, cols)| cols.keys().map(move |col| (table.clone(), col.clone())))
            .collect()
    }

    /// Publish a `NOTIFY` to any session currently listening on `channel`.
    /// No-op if there are currently no subscribers.
    pub fn notify(&self, channel: &str, payload: &str) {
        let _ = self
            .notify_bus
            .send((channel.to_string(), payload.to_string()));
    }

    /// Subscribe to the `LISTEN`/`NOTIFY` bus. Each session holds its own
    /// receiver and filters by channel name itself.
    pub fn subscribe_notifications(&self) -> tokio::sync::broadcast::Receiver<(String, String)> {
        self.notify_bus.subscribe()
    }

    /// Register a WASM UDF, persisting it to the shared object store so
    /// every compute node sees the same function catalog (mirrors
    /// `create_table`).
    pub fn create_function(&self, uf: UserFunction) -> Result<()> {
        self.check_writable()?;
        let mut functions = self.functions.lock().unwrap();
        if functions.contains_key(&uf.name) {
            anyhow::bail!("function \"{}\" already exists", uf.name);
        }
        self.store
            .put(&format!("functions/{}.bin", uf.name), &uf.encode()?)?;
        info!(function = %uf.name, "function created");
        functions.insert(uf.name.clone(), uf);
        Ok(())
    }

    /// Look up a registered WASM UDF by name.
    pub fn get_function(&self, name: &str) -> Option<UserFunction> {
        self.functions.lock().unwrap().get(name).cloned()
    }

    /// Sandboxing limits applied to WASM UDF invocations on this node.
    pub fn udf_config(&self) -> UdfConfig {
        self.udf_config
    }

    /// Parse `sql`, reusing a cached `Stmt` for identical SQL text seen
    /// before on *any* connection — see `sql::cache::StatementCache`.
    pub fn parse_cached(&self, sql: &str) -> anyhow::Result<Stmt> {
        self.stmt_cache.parse(sql)
    }

    /// `(entry_count, hits, misses)` for the shared statement cache — for
    /// tests/observability.
    pub fn stmt_cache_stats(&self) -> (usize, u64, u64) {
        self.stmt_cache.stats()
    }

    /// Point-lookup a row via a B-Tree index on `column = value` (`value`
    /// encoded the same way `Value::to_wire_bytes` encodes it). Skips (and
    /// treats as a miss) any index entry whose row has since been deleted,
    /// since the B-Tree has no delete/compaction support yet.
    pub fn index_lookup(
        &self,
        table: &str,
        column: &str,
        value: &[u8],
    ) -> Result<Option<KeyValue>> {
        let pk = {
            let idx_map = self.indexes.lock().unwrap();
            match idx_map.get(table).and_then(|m| m.get(column)) {
                Some(btree) => btree.search(value)?,
                None => return Ok(None),
            }
        };
        let Some(pk) = pk else { return Ok(None) };
        match self.read(table, &pk)? {
            Some(v) => Ok(Some(KeyValue { key: pk, value: v })),
            None => Ok(None),
        }
    }

    /// Fetches whichever of `keys` are still live rows in `table`, for
    /// stitching a value-less key list (e.g. `fts_search_bm25`'s output) back
    /// into full rows. One point read per key via the same `read` path
    /// `vector_knn_query` already uses — not a table scan.
    pub fn rows_by_keys(&self, table: &str, keys: &[Vec<u8>]) -> Vec<KeyValue> {
        keys.iter()
            .filter_map(|k| {
                self.read(table, k).ok().flatten().map(|v| KeyValue {
                    key: k.clone(),
                    value: v,
                })
            })
            .collect()
    }

    /// Attach or replace a JSON Schema validation rule for one `Json` column
    /// (`CREATE TABLE ... WITH (json_schema_col = ..., json_schema = ...,
    /// json_schema_mode = ...)`). Persisted like any other schema mutation
    /// (`update_table_schema`).
    pub fn set_json_schema(&self, table: &str, rule: JsonSchemaRule) -> Result<()> {
        self.check_writable()?;
        let mut schema = self
            .schemas
            .lock()
            .unwrap()
            .get(table)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("table \"{table}\" does not exist"))?;
        schema.json_schemas.retain(|r| r.column != rule.column);
        schema.json_schemas.push(rule);
        self.update_table_schema(schema)
    }
}
