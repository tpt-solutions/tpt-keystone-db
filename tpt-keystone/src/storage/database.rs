use anyhow::Result;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tracing::info;

use super::btree::BTree;
use super::config::NodeRole;
use super::lease::LeaseHandle;
use super::lsm::LsmEngine;
use super::mvcc::MvccStore;
use super::objectstore::ObjectStore;
use super::tx::{IsolationLevel, Transaction, TransactionManager};
use super::{ColumnDef, ColumnType, KeyValue, StorageEngine, StorageStats, TableSchema};

/// The main database engine that ties together LSM storage, MVCC, schemas, and indexes.
pub struct Database {
    lsm: Arc<Mutex<LsmEngine>>,
    mvcc: Arc<MvccStore>,
    tx_mgr: TransactionManager,
    schemas: Arc<Mutex<HashMap<String, TableSchema>>>,
    indexes: Arc<Mutex<HashMap<String, HashMap<String, BTree>>>>, // table → (column_name → BTree)
    store: Arc<dyn ObjectStore>,
    local_index_dir: PathBuf,
    role: NodeRole,
    /// In-process pub/sub bus for `LISTEN`/`NOTIFY`. Notifications are
    /// process-local (not shared across compute nodes via the object
    /// store) — a session only sees `NOTIFY`s issued on the same node.
    notify_bus: tokio::sync::broadcast::Sender<(String, String)>,
}

impl Database {
    /// Open or create a database. `local_dir` holds only node-local
    /// state (active WAL segment, local B-Tree indexes, NVMe cache staging);
    /// `store` is the shared durable object store all compute nodes point
    /// at. `lease` gates whether this node is allowed to flush (see
    /// `storage::lease`); `role` gates whether it's allowed to accept writes
    /// at all.
    pub fn open(local_dir: &Path, store: Arc<dyn ObjectStore>, lease: Arc<LeaseHandle>, role: NodeRole) -> Result<Self> {
        std::fs::create_dir_all(local_dir)?;

        let lsm = Arc::new(Mutex::new(LsmEngine::open(&local_dir.join("wal"), store.clone(), lease)?));
        let mvcc = Arc::new(MvccStore::new());
        let tx_mgr = TransactionManager::new(mvcc.clone());
        let schemas = Arc::new(Mutex::new(HashMap::new()));
        let indexes = Arc::new(Mutex::new(HashMap::new()));

        // Schemas live in the shared object store (not local disk) so every
        // compute node — writer or reader — sees the same table catalog.
        for key in store.list("schemas/")? {
            if let Some((data, _meta)) = store.get(&key)? {
                if let Ok(schema) = TableSchema::decode(&data) {
                    schemas.lock().unwrap().insert(schema.name.clone(), schema);
                }
            }
        }

        // B-Tree secondary indexes are a local-only accelerator (they already
        // have no delete/compaction support); each node rebuilds its own
        // rather than sharing them through the object store.
        let local_index_dir = local_dir.join("indexes");
        if local_index_dir.exists() {
            if let Ok(entries) = std::fs::read_dir(&local_index_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if let Some(name) = path.file_stem() {
                        if let Some(name_str) = name.to_str() {
                            if let Some((table, col)) = name_str.split_once('_') {
                                if let Ok(btree) = BTree::open(&path) {
                                    let mut idx_map = indexes.lock().unwrap();
                                    idx_map
                                        .entry(table.to_string())
                                        .or_insert_with(HashMap::new)
                                        .insert(col.to_string(), btree);
                                }
                            }
                        }
                    }
                }
            }
        }

        info!(schemas = schemas.lock().unwrap().len(), ?role, "Database opened");

        let (notify_bus, _) = tokio::sync::broadcast::channel(1024);

        Ok(Self {
            lsm,
            mvcc,
            tx_mgr,
            schemas,
            indexes,
            store,
            local_index_dir,
            role,
            notify_bus,
        })
    }

    /// Get the transaction manager.
    pub fn tx_mgr(&self) -> &TransactionManager {
        &self.tx_mgr
    }

    /// Get the MVCC store.
    pub fn mvcc(&self) -> &Arc<MvccStore> {
        &self.mvcc
    }

    /// Get storage statistics.
    pub fn stats(&self) -> StorageStats {
        self.lsm.lock().unwrap().stats()
    }

    /// Force the active memtable to flush to a new SSTable in the shared
    /// object store, updating the manifest. Rejected if this node doesn't
    /// hold a valid write lease.
    pub fn flush(&self) -> Result<()> {
        self.check_writable()?;
        self.lsm.lock().unwrap().flush()
    }

    /// Poll the shared manifest and schema catalog for changes made by the
    /// writer node. Reader-role nodes call this on an interval to converge.
    pub fn refresh(&self) -> Result<()> {
        self.lsm.lock().unwrap().refresh()?;
        for key in self.store.list("schemas/")? {
            if let Some((data, _meta)) = self.store.get(&key)? {
                if let Ok(schema) = TableSchema::decode(&data) {
                    self.schemas.lock().unwrap().entry(schema.name.clone()).or_insert(schema);
                }
            }
        }
        Ok(())
    }

    fn check_writable(&self) -> Result<()> {
        if self.role == NodeRole::Reader {
            anyhow::bail!("this node is a read-only replica; send writes to the writer node");
        }
        Ok(())
    }
}

impl StorageEngine for Database {
    fn write(&self, table: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.check_writable()?;
        let composite_key = Self::make_key(table, key);
        self.lsm.lock().unwrap().write(table, &composite_key, value)?;

        // Maintain any B-Tree indexes defined on this table.
        let indexed_cols: Vec<(String, usize)> = {
            let schemas = self.schemas.lock().unwrap();
            let idx_map = self.indexes.lock().unwrap();
            match (schemas.get(table), idx_map.get(table)) {
                (Some(schema), Some(cols)) => cols.keys().filter_map(|col| {
                    schema.columns.iter().position(|c| c.name == *col).map(|i| (col.clone(), i))
                }).collect(),
                _ => Vec::new(),
            }
        };
        if !indexed_cols.is_empty() {
            let mut idx_map = self.indexes.lock().unwrap();
            if let Some(cols) = idx_map.get_mut(table) {
                for (col, pos) in indexed_cols {
                    if let Some(col_bytes) = decode_column(value, pos) {
                        if let Some(btree) = cols.get_mut(&col) {
                            btree.insert(&col_bytes, key)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn read(&self, table: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let composite_key = Self::make_key(table, key);
        self.lsm.lock().unwrap().read(&composite_key)
    }

    fn delete(&self, table: &str, key: &[u8]) -> Result<()> {
        self.check_writable()?;
        let composite_key = Self::make_key(table, key);
        self.lsm.lock().unwrap().delete(table, &composite_key)?;
        Ok(())
    }

    fn scan(&self, table: &str) -> Result<Vec<KeyValue>> {
        let all = self.lsm.lock().unwrap().scan()?;
        let prefix = Self::make_prefix(table);
        let results = all
            .into_iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(k, v)| {
                // Strip the table prefix from the key
                let key = k[prefix.len()..].to_vec();
                KeyValue { key, value: v }
            })
            .collect();
        Ok(results)
    }

    fn create_table(&self, name: &str, columns: &[ColumnDef]) -> Result<()> {
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
        };

        // Persist schema to the shared object store so every compute node
        // (writer or reader) sees the same table catalog.
        self.store.put(&format!("schemas/{name}.bin"), &schema.encode()?)?;

        schemas.insert(name.to_string(), schema);
        info!(table = name, "table created");
        Ok(())
    }

    fn get_table(&self, name: &str) -> Result<Option<TableSchema>> {
        Ok(self.schemas.lock().unwrap().get(name).cloned())
    }

    fn list_tables(&self) -> Result<Vec<String>> {
        Ok(self.schemas.lock().unwrap().keys().cloned().collect())
    }

    /// Create a B-Tree index on a column, backfilling it from existing rows.
    fn create_index(&self, table: &str, column: &str) -> Result<()> {
        self.check_writable()?;
        let index_dir = &self.local_index_dir;
        std::fs::create_dir_all(index_dir)?;
        let index_path = index_dir.join(format!("{}_{}", table, column));

        let mut btree = BTree::open(&index_path)?;

        let schema = self.schemas.lock().unwrap().get(table).cloned()
            .ok_or_else(|| anyhow::anyhow!("table \"{table}\" does not exist"))?;
        let col_idx = schema.columns.iter().position(|c| c.name == column)
            .ok_or_else(|| anyhow::anyhow!("column \"{column}\" does not exist"))?;

        for kv in self.scan(table)? {
            if let Some(col_bytes) = decode_column(&kv.value, col_idx) {
                btree.insert(&col_bytes, &kv.key)?;
            }
        }

        let mut idx_map = self.indexes.lock().unwrap();
        idx_map
            .entry(table.to_string())
            .or_insert_with(HashMap::new)
            .insert(column.to_string(), btree);

        info!(table, column, "index created");
        Ok(())
    }
}

impl Database {
    /// Whether a B-Tree index exists for `table.column`.
    pub fn indexed_column(&self, table: &str, column: &str) -> bool {
        self.indexes.lock().unwrap().get(table).is_some_and(|m| m.contains_key(column))
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
        let _ = self.notify_bus.send((channel.to_string(), payload.to_string()));
    }

    /// Subscribe to the `LISTEN`/`NOTIFY` bus. Each session holds its own
    /// receiver and filters by channel name itself.
    pub fn subscribe_notifications(&self) -> tokio::sync::broadcast::Receiver<(String, String)> {
        self.notify_bus.subscribe()
    }

    /// Point-lookup a row via a B-Tree index on `column = value` (`value`
    /// encoded the same way `Value::to_wire_bytes` encodes it). Skips (and
    /// treats as a miss) any index entry whose row has since been deleted,
    /// since the B-Tree has no delete/compaction support yet.
    pub fn index_lookup(&self, table: &str, column: &str, value: &[u8]) -> Result<Option<KeyValue>> {
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
}

/// Decode the `idx`-th column's raw bytes from a length-prefixed row value
/// blob (see `executor::parse_rows` for the encoding).
fn decode_column(value: &[u8], idx: usize) -> Option<Vec<u8>> {
    let mut pos = 0usize;
    let mut i = 0usize;
    while pos + 4 <= value.len() {
        let len = i32::from_be_bytes(value[pos..pos + 4].try_into().unwrap());
        pos += 4;
        if len < 0 {
            if i == idx { return None; }
            i += 1;
            continue;
        }
        let end = pos + len as usize;
        if end > value.len() { return None; }
        if i == idx { return Some(value[pos..end].to_vec()); }
        pos = end;
        i += 1;
    }
    None
}

impl Database {
    /// Create a composite key: table_name + NUL + key
    fn make_key(table: &str, key: &[u8]) -> Vec<u8> {
        let mut composite = Vec::with_capacity(table.len() + 1 + key.len());
        composite.extend_from_slice(table.as_bytes());
        composite.push(0);
        composite.extend_from_slice(key);
        composite
    }

    /// Create a prefix for scanning all keys in a table.
    fn make_prefix(table: &str) -> Vec<u8> {
        let mut prefix = Vec::with_capacity(table.len() + 1);
        prefix.extend_from_slice(table.as_bytes());
        prefix.push(0);
        prefix
    }
}