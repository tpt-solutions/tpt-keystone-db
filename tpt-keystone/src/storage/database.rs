use anyhow::Result;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tracing::info;

use super::btree::BTree;
use super::lsm::LsmEngine;
use super::mvcc::MvccStore;
use super::tx::{IsolationLevel, Transaction, TransactionManager};
use super::{ColumnDef, ColumnType, KeyValue, StorageEngine, StorageStats, TableSchema};

/// The main database engine that ties together LSM storage, MVCC, schemas, and indexes.
pub struct Database {
    lsm: Arc<Mutex<LsmEngine>>,
    mvcc: Arc<MvccStore>,
    tx_mgr: TransactionManager,
    schemas: Arc<Mutex<HashMap<String, TableSchema>>>,
    indexes: Arc<Mutex<HashMap<String, HashMap<String, BTree>>>>, // table → (column_name → BTree)
    data_dir: PathBuf,
}

impl Database {
    /// Open or create a database at the given directory.
    pub fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let data_dir = dir.to_path_buf();

        let lsm = Arc::new(Mutex::new(LsmEngine::open(dir)?));
        let mvcc = Arc::new(MvccStore::new());
        let tx_mgr = TransactionManager::new(mvcc.clone());
        let schemas = Arc::new(Mutex::new(HashMap::new()));
        let indexes = Arc::new(Mutex::new(HashMap::new()));

        // Load schemas from disk
        let schema_dir = dir.join("schemas");
        if schema_dir.exists() {
            if let Ok(entries) = std::fs::read_dir(&schema_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if let Ok(data) = std::fs::read(&path) {
                        if let Ok(schema) = TableSchema::decode(&data) {
                            let name = schema.name.clone();
                            schemas.lock().unwrap().insert(name, schema);
                        }
                    }
                }
            }
        }

        // Load B-Tree indexes
        let index_dir = dir.join("indexes");
        if index_dir.exists() {
            if let Ok(entries) = std::fs::read_dir(&index_dir) {
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

        info!(schemas = schemas.lock().unwrap().len(), "Database opened");

        Ok(Self {
            lsm,
            mvcc,
            tx_mgr,
            schemas,
            indexes,
            data_dir,
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
}

impl StorageEngine for Database {
    fn write(&self, table: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let composite_key = Self::make_key(table, key);
        self.lsm.lock().unwrap().write(table, &composite_key, value)?;
        Ok(())
    }

    fn read(&self, table: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let composite_key = Self::make_key(table, key);
        self.lsm.lock().unwrap().read(&composite_key)
    }

    fn delete(&self, table: &str, key: &[u8]) -> Result<()> {
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

        // Persist schema
        let schema_dir = self.data_dir.join("schemas");
        std::fs::create_dir_all(&schema_dir)?;
        let schema_path = schema_dir.join(format!("{}.schema", name));
        std::fs::write(&schema_path, schema.encode()?)?;

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

    /// Create a B-Tree index on a column.
    fn create_index(&self, table: &str, column: &str) -> Result<()> {
        let index_dir = self.data_dir.join("indexes");
        std::fs::create_dir_all(&index_dir)?;
        let index_path = index_dir.join(format!("{}_{}", table, column));

        let btree = BTree::open(&index_path)?;

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