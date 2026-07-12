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
