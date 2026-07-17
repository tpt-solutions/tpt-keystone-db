use anyhow::Result;
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tracing::info;

use crate::storage::database::txn::TxnHandle;
use crate::storage::StorageEngine;

use super::btree::BTree;
use super::canopy_index::{FtsIndex, JsonPathIndex};
use super::config::{NodeRole, UdfConfig};
use super::flux::{FluxLog, FluxRecord};
use super::geo_index::GeoIndex;
use super::graph_index::GraphIndex;
use super::ivf_pq_index::IvfPqStorageIndex;
use super::lease::LeaseHandle;
use super::lsm::LsmEngine;
use super::mvcc::MvccStore;
use super::objectstore::ObjectStore;
use super::ts_index::{TimeBucketPolicy, TimeIndex};
use super::tx::TransactionManager;
use super::vector_index::VectorIndex;
use super::{Sequence, StorageStats, TableSchema, UserFunction};
use crate::vector::hnsw::{HnswConfig, Metric};

/// A Chronos time index plus the name of the numeric column it tracks
/// alongside the indexed timestamp column (the index is keyed by
/// `(table, timestamp_column)`, but a bucket also needs to know which
/// column's values to compress/roll up).
struct TsIndexEntry {
    index: TimeIndex,
    value_column: String,
}

/// Current unix-ms timestamp, used for staleness bookkeeping. Local helper so
/// `storage` doesn't depend on the (higher-level) `synapse` module for a clock.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Tracks a reader node's manifest-refresh health so staleness is observable
/// by clients (Phase 12a follow-up: a reader whose refresh keeps failing must
/// not silently serve last-known state). `Database::refresh` updates this on
/// every attempt; `publish_metrics` surfaces it through the Prometheus
/// endpoint, and `is_stale` lets the wire/session layer refuse reads if a
/// caller opts into strict freshness.
#[derive(Clone, Default)]
pub struct ReaderStaleness {
    last_success_ms: Arc<AtomicU64>,
    consecutive_failures: Arc<AtomicU64>,
    last_error: Arc<Mutex<Option<String>>>,
}

impl ReaderStaleness {
    pub fn record_success(&self) {
        self.last_success_ms.store(now_ms(), Ordering::Relaxed);
        self.consecutive_failures.store(0, Ordering::Relaxed);
        *self.last_error.lock().unwrap() = None;
    }

    pub fn record_failure(&self, msg: String) {
        self.consecutive_failures.fetch_add(1, Ordering::Relaxed);
        *self.last_error.lock().unwrap() = Some(msg);
    }

    pub fn consecutive_failures(&self) -> u64 {
        self.consecutive_failures.load(Ordering::Relaxed)
    }

    pub fn last_success_ms(&self) -> u64 {
        self.last_success_ms.load(Ordering::Relaxed)
    }

    pub fn last_error(&self) -> Option<String> {
        self.last_error.lock().unwrap().clone()
    }

    /// True if refresh is currently failing, or the last success is older than
    /// `max_age_ms` (a reader that's merely quiet — no writer activity — also
    /// ages out and should be flagged rather than trusted blindly).
    pub fn is_stale(&self, max_age_ms: u64) -> bool {
        if self.consecutive_failures.load(Ordering::Relaxed) > 0 {
            return true;
        }
        let last = self.last_success_ms.load(Ordering::Relaxed);
        last != 0 && now_ms().saturating_sub(last) > max_age_ms
    }

    /// Push the current staleness state into the metrics registry so a
    /// Prometheus scrape exposes it to clients/alerts.
    pub fn publish_metrics(&self, max_age_ms: u64) {
        let stale = self.is_stale(max_age_ms);
        crate::metrics::Metrics::global().set_reader_manifest_stale(stale);
        let age = if self.last_success_ms.load(Ordering::Relaxed) == 0 {
            0
        } else {
            (now_ms().saturating_sub(self.last_success_ms.load(Ordering::Relaxed))) / 1000
        };
        crate::metrics::Metrics::global().set_reader_last_refresh_age_seconds(age as f64);
    }
}

/// The main database engine that ties together LSM storage, MVCC, schemas, and indexes.
pub struct Database {
    lsm: Arc<Mutex<LsmEngine>>,
    mvcc: Arc<MvccStore>,
    tx_mgr: TransactionManager,
    schemas: Arc<Mutex<HashMap<String, TableSchema>>>,
    functions: Arc<Mutex<HashMap<String, UserFunction>>>,
    sequences: Arc<Mutex<HashMap<String, Sequence>>>,
    indexes: Arc<Mutex<HashMap<String, HashMap<String, BTree>>>>, // table → (column_name → BTree)
    /// Meridian spatial indexes, `CREATE INDEX ... USING SPATIAL`, kept
    /// separate from `indexes` since a `GeoIndex` answers radius/time-range
    /// queries rather than exact-match point lookups. Local-only, same
    /// scope cut as `indexes`.
    geo_indexes: Arc<Mutex<HashMap<String, HashMap<String, GeoIndex>>>>,
    /// Chronos time indexes, `CREATE INDEX ... USING TIME`, keyed by
    /// `(table, timestamp_column)` — same local-only accelerator scope cut
    /// as `indexes`/`geo_indexes`.
    ts_indexes: Arc<Mutex<HashMap<String, HashMap<String, TsIndexEntry>>>>,
    /// Plexus adjacency indexes, `CREATE INDEX ... USING GRAPH`, keyed by
    /// `(table, from_column)` — same local-only accelerator scope cut as
    /// `indexes`/`geo_indexes`/`ts_indexes`.
    graph_indexes: Arc<Mutex<HashMap<String, HashMap<String, GraphIndex>>>>,
    /// Canopy path indexes, `CREATE INDEX ... USING JSONPATH`, keyed by
    /// `(table, column)` — same local-only accelerator scope cut as the
    /// other index maps. One indexed path per `(table, column)`.
    json_indexes: Arc<Mutex<HashMap<String, HashMap<String, JsonPathIndex>>>>,
    /// Canopy inverted full-text indexes, `CREATE INDEX ... USING GIN/FTS`,
    /// keyed by `(table, column)`.
    fts_indexes: Arc<Mutex<HashMap<String, HashMap<String, FtsIndex>>>>,
    /// Prism vector (HNSW) indexes, `CREATE INDEX ... USING VECTOR/HNSW`,
    /// keyed by `(table, column)` — same local-only accelerator scope cut as
    /// the other index maps.
    vector_indexes: Arc<Mutex<HashMap<String, HashMap<String, VectorIndex>>>>,
    /// Prism IVF-PQ indexes, `CREATE INDEX ... USING IVFPQ`, keyed by
    /// `(table, column)` — same local-only accelerator scope cut as
    /// `vector_indexes`. A `(table, column)` pair can have both a
    /// `vector_indexes` (HNSW) and an `ivf_pq_indexes` entry at once; see
    /// `vector_knn_query`'s automatic-selection heuristic for how a query
    /// picks between them.
    ivf_pq_indexes: Arc<Mutex<HashMap<String, HashMap<String, IvfPqStorageIndex>>>>,
    /// Flux topics (`CREATE TOPIC`), keyed by topic name — local-only, not
    /// object-store-replicated (see `storage::flux` module docs). Includes
    /// the implicit `__cdc_<table>` topics auto-created by
    /// `executor::execute_insert`/`update`/`delete`.
    topics: Arc<Mutex<HashMap<String, FluxLog>>>,
    /// Directory each topic's subdirectory lives under (`<local_dir>/flux/<topic>/`).
    flux_dir: PathBuf,
    /// In-process pub/sub bus for Flux publishes, keyed by topic name — the
    /// WebSocket streaming endpoint (`wire::websocket`) subscribes here to
    /// push newly published records to a connected client. Same "process-
    /// local, not cross-node" scope as `notify_bus` below.
    flux_bus: tokio::sync::broadcast::Sender<(String, FluxRecord)>,
    store: Arc<dyn ObjectStore>,
    local_index_dir: PathBuf,
    role: NodeRole,
    udf_config: UdfConfig,
    /// In-process pub/sub bus for `LISTEN`/`NOTIFY`. Notifications are
    /// process-local (not shared across compute nodes via the object
    /// store) — a session only sees `NOTIFY`s issued on the same node.
    notify_bus: tokio::sync::broadcast::Sender<(String, String)>,
    /// Shared across every connection, since every connection already
    /// shares this one `Database` — a hot query's lex/parse cost is paid
    /// once regardless of which connection asks for it next.
    stmt_cache: crate::sql::cache::StatementCache,
    /// Reader-node manifest-refresh health (Phase 12a staleness signal).
    reader_staleness: ReaderStaleness,
    /// Whether `Json` columns are stored using Canopy's native binary jsonb
    /// format (`storage::jsonb`) instead of raw JSON text. Off by default —
    /// see `set_jsonb_binary_storage`. Read on every INSERT/UPDATE/COPY.
    jsonb_binary: std::sync::atomic::AtomicBool,
}

impl Database {
    /// Open or create a database. `local_dir` holds only node-local
    /// state (active WAL segment, local B-Tree indexes, NVMe cache staging);
    /// `store` is the shared durable object store all compute nodes point
    /// at. `lease` gates whether this node is allowed to flush (see
    /// `storage::lease`); `role` gates whether it's allowed to accept writes
    /// at all.
    #[tracing::instrument(skip_all)]
    pub fn open(
        local_dir: &Path,
        store: Arc<dyn ObjectStore>,
        lease: Arc<LeaseHandle>,
        role: NodeRole,
        udf_config: UdfConfig,
    ) -> Result<Self> {
        std::fs::create_dir_all(local_dir)?;

        let lsm = Arc::new(Mutex::new(LsmEngine::open(
            &local_dir.join("wal"),
            store.clone(),
            lease,
        )?));
        let mvcc = Arc::new(MvccStore::new());
        let tx_mgr = TransactionManager::new(mvcc.clone());
        let schemas = Arc::new(Mutex::new(HashMap::new()));
        let functions = Arc::new(Mutex::new(HashMap::new()));
        let sequences = Arc::new(Mutex::new(HashMap::new()));
        let indexes = Arc::new(Mutex::new(HashMap::new()));
        let geo_indexes = Arc::new(Mutex::new(HashMap::new()));
        let ts_indexes = Arc::new(Mutex::new(HashMap::new()));
        let graph_indexes = Arc::new(Mutex::new(HashMap::new()));
        let json_indexes = Arc::new(Mutex::new(HashMap::new()));
        let fts_indexes = Arc::new(Mutex::new(HashMap::new()));
        let vector_indexes = Arc::new(Mutex::new(HashMap::new()));
        let ivf_pq_indexes = Arc::new(Mutex::new(HashMap::new()));
        let topics = Arc::new(Mutex::new(HashMap::new()));

        // Schemas live in the shared object store (not local disk) so every
        // compute node — writer or reader — sees the same table catalog.
        for key in store.list("schemas/")? {
            if let Some((data, _meta)) = store.get(&key)? {
                if let Ok(schema) = TableSchema::decode(&data) {
                    schemas.lock().unwrap().insert(schema.name.clone(), schema);
                }
            }
        }

        // Function definitions (WASM bytes + signature) are also shared via
        // the object store, same as schemas.
        for key in store.list("functions/")? {
            if let Some((data, _meta)) = store.get(&key)? {
                if let Ok(uf) = UserFunction::decode(&data) {
                    functions.lock().unwrap().insert(uf.name.clone(), uf);
                }
            }
        }

        // Sequences are also shared via the object store, same as schemas.
        for key in store.list("sequences/")? {
            if let Some((data, _meta)) = store.get(&key)? {
                if let Ok(seq) = Sequence::decode(&data) {
                    sequences.lock().unwrap().insert(seq.name.clone(), seq);
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
                    let is_geo = path.extension().is_some_and(|e| e == "geo");
                    let is_ts = path.extension().is_some_and(|e| e == "ts");
                    let is_graph = path.extension().is_some_and(|e| e == "graph");
                    let is_jsonpath = path.extension().is_some_and(|e| e == "jsonpath");
                    let is_fts = path.extension().is_some_and(|e| e == "fts");
                    let is_vector = path.extension().is_some_and(|e| e == "vec");
                    let is_ivfpq = path.extension().is_some_and(|e| e == "ivfpq");
                    if let Some(name) = path.file_stem() {
                        if let Some(name_str) = name.to_str() {
                            if let Some((table, col)) = name_str.split_once('_') {
                                if is_geo {
                                    // Level is read back from the file's own header
                                    // (see `GeoIndex::open`) — the fallback value here
                                    // is only used if the file doesn't exist yet, which
                                    // can't happen on this read_dir-driven path.
                                    if let Ok(geo) = GeoIndex::open(&path, 0) {
                                        let mut idx_map = geo_indexes.lock().unwrap();
                                        idx_map
                                            .entry(table.to_string())
                                            .or_insert_with(HashMap::new)
                                            .insert(col.to_string(), geo);
                                    }
                                } else if is_ts {
                                    // Policy/value-column are read back from the
                                    // file's own header (see `TimeIndex::open`) —
                                    // the fallback values here are only used if the
                                    // file doesn't exist yet, which can't happen on
                                    // this read_dir-driven path.
                                    let fallback_policy = TimeBucketPolicy {
                                        granularity_ms: 3_600_000,
                                        retention_ms: None,
                                    };
                                    if let Ok(ts) = TimeIndex::open(&path, fallback_policy, "") {
                                        let value_column = ts.value_column().to_string();
                                        let mut idx_map = ts_indexes.lock().unwrap();
                                        idx_map
                                            .entry(table.to_string())
                                            .or_insert_with(HashMap::new)
                                            .insert(
                                                col.to_string(),
                                                TsIndexEntry {
                                                    index: ts,
                                                    value_column,
                                                },
                                            );
                                    }
                                } else if is_graph {
                                    // to_column/type_column are read back from
                                    // the file's own header (see
                                    // `GraphIndex::open`) — the fallback
                                    // values here are only used if the file
                                    // doesn't exist yet, which can't happen
                                    // on this read_dir-driven path.
                                    if let Ok(graph) = GraphIndex::open(&path, "", None) {
                                        let mut idx_map = graph_indexes.lock().unwrap();
                                        idx_map
                                            .entry(table.to_string())
                                            .or_insert_with(HashMap::new)
                                            .insert(col.to_string(), graph);
                                    }
                                } else if is_jsonpath {
                                    // json_path is read back from the file's own
                                    // header (see `JsonPathIndex::open`) — the
                                    // fallback value here is only used if the file
                                    // doesn't exist yet, which can't happen on this
                                    // read_dir-driven path.
                                    if let Ok(jp) = JsonPathIndex::open(&path, "") {
                                        let mut idx_map = json_indexes.lock().unwrap();
                                        idx_map
                                            .entry(table.to_string())
                                            .or_insert_with(HashMap::new)
                                            .insert(col.to_string(), jp);
                                    }
                                } else if is_fts {
                                    if let Ok(fts) = FtsIndex::open(&path) {
                                        let mut idx_map = fts_indexes.lock().unwrap();
                                        idx_map
                                            .entry(table.to_string())
                                            .or_insert_with(HashMap::new)
                                            .insert(col.to_string(), fts);
                                    }
                                } else if is_vector {
                                    // Metric/config are read back from the file's own
                                    // header (see `VectorIndex::open`) — the fallback
                                    // values here are only used if the file
                                    // doesn't exist yet, which can't happen on this
                                    // read_dir-driven path.
                                    if let Ok(vec_idx) =
                                        VectorIndex::open(&path, Metric::L2, HnswConfig::default())
                                    {
                                        let mut idx_map = vector_indexes.lock().unwrap();
                                        idx_map
                                            .entry(table.to_string())
                                            .or_insert_with(HashMap::new)
                                            .insert(col.to_string(), vec_idx);
                                    }
                                } else if is_ivfpq {
                                    // Reopening retrains from the replayed
                                    // log against the header's stored
                                    // metric/n_lists/pq_m/n_probe — see
                                    // `IvfPqStorageIndex::open`'s doc-comment.
                                    if let Ok(ivf_idx) = IvfPqStorageIndex::open(&path) {
                                        let mut idx_map = ivf_pq_indexes.lock().unwrap();
                                        idx_map
                                            .entry(table.to_string())
                                            .or_insert_with(HashMap::new)
                                            .insert(col.to_string(), ivf_idx);
                                    }
                                } else if let Ok(btree) = BTree::open(&path) {
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

        // Flux topics are a local-only accelerator (see `storage::flux`
        // module docs); each node rebuilds its own from `flux/<topic>/`
        // rather than sharing them through the object store.
        let flux_dir = local_dir.join("flux");
        if flux_dir.exists() {
            if let Ok(entries) = std::fs::read_dir(&flux_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if !path.is_dir() {
                        continue;
                    }
                    let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                        continue;
                    };
                    if let Ok(log) = FluxLog::open(&path) {
                        topics.lock().unwrap().insert(name.to_string(), log);
                    }
                }
            }
        }

        info!(
            schemas = schemas.lock().unwrap().len(),
            ?role,
            "Database opened"
        );

        let (notify_bus, _) = tokio::sync::broadcast::channel(1024);
        let (flux_bus, _) = tokio::sync::broadcast::channel(1024);

        Ok(Self {
            lsm,
            mvcc,
            tx_mgr,
            schemas,
            functions,
            sequences,
            indexes,
            geo_indexes,
            ts_indexes,
            graph_indexes,
            json_indexes,
            fts_indexes,
            vector_indexes,
            ivf_pq_indexes,
            topics,
            flux_dir,
            flux_bus,
            store,
            local_index_dir,
            role,
            udf_config,
            notify_bus,
            stmt_cache: crate::sql::cache::StatementCache::new(256),
            reader_staleness: ReaderStaleness::default(),
            jsonb_binary: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Get the transaction manager.
    pub fn tx_mgr(&self) -> &TransactionManager {
        &self.tx_mgr
    }

    /// Whether `Json` columns are stored using Canopy's native binary jsonb
    /// format (`storage::jsonb`). See `set_jsonb_binary_storage`.
    pub fn jsonb_binary_storage(&self) -> bool {
        self.jsonb_binary.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Enable/disable native binary jsonb storage for `Json` columns. Wired
    /// from `TPT_JSONB_BINARY=1` in `main.rs`; also settable directly (used by
    /// tests). Decoding is self-describing (a marker byte on each stored cell,
    /// see `storage::jsonb::decode_cell`), so flipping this off later still
    /// reads back rows previously written in binary form — only new writes are
    /// affected.
    pub fn set_jsonb_binary_storage(&self, enabled: bool) {
        self.jsonb_binary
            .store(enabled, std::sync::atomic::Ordering::Relaxed);
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
    ///
    /// Every attempt updates `reader_staleness` so a reader whose refresh keeps
    /// failing is observable (Phase 12a staleness signal) instead of silently
    /// serving last-known state.
    #[tracing::instrument(skip_all)]
    pub fn refresh(&self) -> Result<()> {
        let result: Result<()> = (|| {
            self.lsm.lock().unwrap().refresh()?;
            for key in self.store.list("schemas/")? {
                if let Some((data, _meta)) = self.store.get(&key)? {
                    if let Ok(schema) = TableSchema::decode(&data) {
                        self.schemas
                            .lock()
                            .unwrap()
                            .entry(schema.name.clone())
                            .or_insert(schema);
                    }
                }
            }
            for key in self.store.list("functions/")? {
                if let Some((data, _meta)) = self.store.get(&key)? {
                    if let Ok(uf) = UserFunction::decode(&data) {
                        self.functions
                            .lock()
                            .unwrap()
                            .entry(uf.name.clone())
                            .or_insert(uf);
                    }
                }
            }
            for key in self.store.list("sequences/")? {
                if let Some((data, _meta)) = self.store.get(&key)? {
                    if let Ok(seq) = Sequence::decode(&data) {
                        self.sequences
                            .lock()
                            .unwrap()
                            .entry(seq.name.clone())
                            .or_insert(seq);
                    }
                }
            }
            Ok(())
        })();
        match &result {
            Ok(()) => self.reader_staleness.record_success(),
            Err(e) => self.reader_staleness.record_failure(format!("{e:#}")),
        }
        result
    }

    /// Reader-node manifest-refresh health (Phase 12a staleness signal). Clone
    /// shares the same underlying atomics with every other handle.
    pub fn reader_staleness(&self) -> ReaderStaleness {
        self.reader_staleness.clone()
    }

    fn check_writable(&self) -> Result<()> {
        if self.role == NodeRole::Reader {
            anyhow::bail!("this node is a read-only replica; send writes to the writer node");
        }
        Ok(())
    }

    // ---- transactions (Phase 1, Stage 1: read-committed) ------------------

    /// Begin a new read-committed transaction, returning a shareable handle
    /// the session keeps for the connection. Reads through the handle see
    /// its own staged writes; `COMMIT`/`ROLLBACK` flush or discard them.
    pub fn begin_txn(&self) -> crate::storage::database::txn::TxnHandle {
        use crate::storage::database::txn::TxnHandle;
        let id = super::mvcc::new_tx_id();
        TxnHandle::new(id)
    }

    /// Commit a transaction: replay its staged writes into the committed LSM
    /// store (plus any affected secondary indexes) atomically under the LSM
    /// lock, then mark it finished. Returns `Ok` even for an already-finished
    /// handle (idempotent, mirrors Postgres's "no-op COMMIT" after ROLLBACK).
    pub fn commit_txn(&self, txn: &crate::storage::database::txn::TxnHandle) -> Result<()> {
        let staged = txn.take_staged();
        if staged.is_empty() {
            return Ok(());
        }
        self.check_writable()?;
        let mut lsm = self.lsm.lock().unwrap();
        for (composite_key, write) in staged {
            // Extract the table name from the composite `table\0key` bytes.
            let table = {
                let idx = composite_key.iter().position(|&b| b == 0);
                match idx {
                    Some(i) => match std::str::from_utf8(&composite_key[..i]) {
                        Ok(t) => t,
                        Err(_) => continue,
                    },
                    None => continue,
                }
            };
            match write.value {
                Some(value) => {
                    lsm.write(table, &composite_key, &value)?;
                    drop(lsm);
                    self.maintain_indexes_on_write(table, &composite_key, &value)?;
                    lsm = self.lsm.lock().unwrap();
                }
                None => {
                    lsm.delete(table, &composite_key)?;
                }
            }
        }
        drop(lsm);
        Ok(())
    }

    /// Roll back a transaction, discarding all staged writes. Idempotent.
    pub fn rollback_txn(&self, txn: &crate::storage::database::txn::TxnHandle) {
        txn.discard();
    }

    /// Apply a single write on behalf of a transaction (or, if `txn` is
    /// `None`, commit it immediately through the normal `StorageEngine`
    /// path). Used by the executor's DML so a statement behaves identically
    /// inside or outside a transaction.
    pub fn txn_write(
        &self,
        txn: Option<&crate::storage::database::txn::TxnHandle>,
        table: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        match txn {
            Some(t) => {
                let composite = Self::make_key(table, key);
                t.stage_write(composite, value.to_vec());
                Ok(())
            }
            None => self.write(table, key, value),
        }
    }

    /// Apply a single delete on behalf of a transaction (or immediately).
    pub fn txn_delete(
        &self,
        txn: Option<&crate::storage::database::txn::TxnHandle>,
        table: &str,
        key: &[u8],
    ) -> Result<()> {
        match txn {
            Some(t) => {
                let composite = Self::make_key(table, key);
                t.stage_delete(composite);
                Ok(())
            }
            None => self.delete(table, key),
        }
    }

    /// Read a row, consulting the transaction's staging buffer first so an
    /// open transaction sees its own uncommitted writes. `txn == None` is the
    /// legacy immediate path.
    pub fn txn_read(
        &self,
        txn: Option<&crate::storage::database::txn::TxnHandle>,
        table: &str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let composite = Self::make_key(table, key);
        if let Some(t) = txn {
            match t.staged_read(&composite) {
                Ok(Some(v)) => return Ok(Some(v)),
                Ok(None) => return Ok(None), // tombstone
                Err(()) => {}
            }
        }
        self.lsm.lock().unwrap().read(&composite)
    }

    /// Scan all rows of a table, merging the transaction's staged writes over
    /// the committed snapshot: staged inserts/updates override committed
    /// values, staged deletes are removed, and any newly-staged keys not yet
    /// present in the committed store appear as new rows.
    pub fn txn_scan(
        &self,
        txn: Option<&crate::storage::database::txn::TxnHandle>,
        table: &str,
    ) -> Result<Vec<crate::storage::KeyValue>> {
        let prefix = Self::make_prefix(table);
        let committed: Vec<(Vec<u8>, Vec<u8>)> = {
            let all = self.lsm.lock().unwrap().scan()?;
            all.into_iter()
                .filter(|(k, _)| k.starts_with(&prefix))
                .collect()
        };
        let mut merged: BTreeMap<Vec<u8>, Option<Vec<u8>>> = committed
            .into_iter()
            .map(|(k, v)| (k, Some(v)))
            .collect();
        if let Some(t) = txn {
            let state = t.inner.lock().unwrap();
            for (composite_key, write) in &state.staged {
                if composite_key.starts_with(&prefix) {
                    merged.insert(composite_key.clone(), write.value.clone());
                }
            }
        }
        let results = merged
            .into_iter()
            .filter_map(|(k, v)| v.map(|value| crate::storage::KeyValue {
                key: k[prefix.len()..].to_vec(),
                value,
            }))
            .collect();
        Ok(results)
    }
}

/// Parses a row cell's text-encoded (see `Value::to_wire_bytes`) bytes as an
/// integer — used to extract a timestamp column's value for a Chronos time
/// index without pulling in the executor's `Value`/`eval` machinery here.
fn decode_i64(bytes: &[u8]) -> Option<i64> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

/// Same as [`decode_i64`] but for the numeric metric column a Chronos time
/// index compresses/rolls up.
fn decode_f64(bytes: &[u8]) -> Option<f64> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
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
            if i == idx {
                return None;
            }
            i += 1;
            continue;
        }
        let end = pos + len as usize;
        if end > value.len() {
            return None;
        }
        if i == idx {
            let cell = &value[pos..end];
            // If this cell was stored as native binary jsonb, hand callers
            // (index maintenance, etc.) the decoded JSON text — the same shape
            // the raw-text storage path produces.
            if let Some(decoded) = super::jsonb::decode_cell(cell) {
                return Some(decoded);
            }
            return Some(cell.to_vec());
        }
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

pub mod catalog;
pub mod flux;
pub mod graph;
pub mod json;
pub mod spatial;
pub mod storage_engine;
pub mod time;
pub mod vector;

pub mod txn;

#[cfg(test)]
mod reader_staleness_tests {
    use super::ReaderStaleness;

    #[test]
    fn fresh_staleness_is_not_stale() {
        let s = ReaderStaleness::default();
        // Never having refreshed at all (last_success_ms == 0) is treated
        // as "not yet stale" rather than "infinitely stale" — a brand new
        // reader shouldn't immediately trip an alert before its first
        // refresh has had a chance to run.
        assert!(!s.is_stale(1_000));
        assert_eq!(s.consecutive_failures(), 0);
        assert_eq!(s.last_error(), None);
    }

    #[test]
    fn failure_marks_stale_until_a_success_clears_it() {
        let s = ReaderStaleness::default();
        s.record_failure("object store unreachable".into());
        assert!(s.is_stale(u64::MAX));
        assert_eq!(s.consecutive_failures(), 1);
        assert_eq!(s.last_error().as_deref(), Some("object store unreachable"));

        s.record_failure("still unreachable".into());
        assert_eq!(s.consecutive_failures(), 2);

        s.record_success();
        assert!(!s.is_stale(u64::MAX));
        assert_eq!(s.consecutive_failures(), 0);
        assert_eq!(s.last_error(), None);
        assert!(s.last_success_ms() > 0);
    }

    #[test]
    fn success_older_than_max_age_is_stale() {
        let s = ReaderStaleness::default();
        s.record_success();
        std::thread::sleep(std::time::Duration::from_millis(5));
        // A refresh that keeps succeeding but hasn't run recently enough
        // (relative to a caller-chosen max age) must still be flagged —
        // "quiet because no writer activity" isn't distinguishable from
        // "quiet because refresh stopped running" without this check.
        assert!(s.is_stale(1));
        assert!(!s.is_stale(u64::MAX));
    }
}

