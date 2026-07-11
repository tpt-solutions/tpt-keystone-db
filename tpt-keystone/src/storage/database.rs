use anyhow::Result;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tracing::info;

use super::btree::BTree;
use super::canopy_index::{FtsIndex, JsonPathIndex};
use super::config::{NodeRole, UdfConfig};
use super::flux::{FluxLog, FluxRecord, RetentionPolicy};
use super::geo_index::GeoIndex;
use super::graph_index::GraphIndex;
use super::ivf_pq_index::IvfPqStorageIndex;
use super::lease::LeaseHandle;
use super::lsm::LsmEngine;
use super::mvcc::MvccStore;
use super::objectstore::ObjectStore;
use super::ts_index::{Rollup, TimeBucketPolicy, TimeIndex};
use super::tx::{IsolationLevel, Transaction, TransactionManager};
use super::vector_index::VectorIndex;
use super::{
    ColumnDef, ColumnType, JsonSchemaRule, KeyValue, Sequence, StorageEngine, StorageStats,
    TableSchema, UserFunction,
};
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
                                    // values here are only used if the file doesn't
                                    // exist yet, which can't happen on this
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
}

impl StorageEngine for Database {
    fn write(&self, table: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.check_writable()?;
        let composite_key = Self::make_key(table, key);
        self.lsm
            .lock()
            .unwrap()
            .write(table, &composite_key, value)?;

        // Maintain any B-Tree indexes defined on this table.
        let indexed_cols: Vec<(String, usize)> = {
            let schemas = self.schemas.lock().unwrap();
            let idx_map = self.indexes.lock().unwrap();
            match (schemas.get(table), idx_map.get(table)) {
                (Some(schema), Some(cols)) => cols
                    .keys()
                    .filter_map(|col| {
                        schema
                            .columns
                            .iter()
                            .position(|c| c.name == *col)
                            .map(|i| (col.clone(), i))
                    })
                    .collect(),
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

        // Maintain any spatial (Meridian) indexes defined on this table.
        let geo_cols: Vec<(String, usize)> = {
            let schemas = self.schemas.lock().unwrap();
            let idx_map = self.geo_indexes.lock().unwrap();
            match (schemas.get(table), idx_map.get(table)) {
                (Some(schema), Some(cols)) => cols
                    .keys()
                    .filter_map(|col| {
                        schema
                            .columns
                            .iter()
                            .position(|c| c.name == *col)
                            .map(|i| (col.clone(), i))
                    })
                    .collect(),
                _ => Vec::new(),
            }
        };
        if !geo_cols.is_empty() {
            let mut idx_map = self.geo_indexes.lock().unwrap();
            if let Some(cols) = idx_map.get_mut(table) {
                for (col, pos) in geo_cols {
                    if let Some(wkt_bytes) = decode_column(value, pos) {
                        if let (Ok(wkt), Some(geo)) =
                            (String::from_utf8(wkt_bytes), cols.get_mut(&col))
                        {
                            if let Ok(geom) = crate::geo::geometry::Geometry::from_wkt(&wkt) {
                                let c = geom.representative_point();
                                geo.insert(key, c.x, c.y, c.t)?;
                            }
                        }
                    }
                }
            }
        }

        // Maintain any Chronos time indexes defined on this table.
        let ts_cols: Vec<(String, usize, usize)> = {
            let schemas = self.schemas.lock().unwrap();
            let idx_map = self.ts_indexes.lock().unwrap();
            match (schemas.get(table), idx_map.get(table)) {
                (Some(schema), Some(cols)) => cols
                    .iter()
                    .filter_map(|(col, entry)| {
                        let ts_pos = schema.columns.iter().position(|c| c.name == *col)?;
                        let val_pos = schema
                            .columns
                            .iter()
                            .position(|c| c.name == entry.value_column)?;
                        Some((col.clone(), ts_pos, val_pos))
                    })
                    .collect(),
                _ => Vec::new(),
            }
        };
        if !ts_cols.is_empty() {
            let mut idx_map = self.ts_indexes.lock().unwrap();
            if let Some(cols) = idx_map.get_mut(table) {
                for (col, ts_pos, val_pos) in ts_cols {
                    let ts_bytes = decode_column(value, ts_pos);
                    let val_bytes = decode_column(value, val_pos);
                    if let (Some(ts_bytes), Some(val_bytes)) = (ts_bytes, val_bytes) {
                        if let (Some(ts), Some(val)) =
                            (decode_i64(&ts_bytes), decode_f64(&val_bytes))
                        {
                            if let Some(entry) = cols.get_mut(&col) {
                                entry.index.insert(key, ts, val)?;
                            }
                        }
                    }
                }
            }
        }

        // Maintain any Plexus graph (adjacency) indexes defined on this
        // table: `col` is the from-column; `to`/`type` columns are recorded
        // in the `GraphIndex` itself (set at `CREATE INDEX` time).
        let graph_cols: Vec<(String, usize, usize, Option<usize>)> = {
            let schemas = self.schemas.lock().unwrap();
            let idx_map = self.graph_indexes.lock().unwrap();
            match (schemas.get(table), idx_map.get(table)) {
                (Some(schema), Some(cols)) => cols
                    .iter()
                    .filter_map(|(col, graph)| {
                        let from_pos = schema.columns.iter().position(|c| c.name == *col)?;
                        let to_pos = schema
                            .columns
                            .iter()
                            .position(|c| c.name == graph.to_column())?;
                        let type_pos = graph
                            .type_column()
                            .and_then(|t| schema.columns.iter().position(|c| c.name == t));
                        Some((col.clone(), from_pos, to_pos, type_pos))
                    })
                    .collect(),
                _ => Vec::new(),
            }
        };
        if !graph_cols.is_empty() {
            let mut idx_map = self.graph_indexes.lock().unwrap();
            if let Some(cols) = idx_map.get_mut(table) {
                for (col, from_pos, to_pos, type_pos) in graph_cols {
                    let from_bytes = decode_column(value, from_pos);
                    let to_bytes = decode_column(value, to_pos);
                    if let (Some(from_bytes), Some(to_bytes)) = (from_bytes, to_bytes) {
                        let rel_type = type_pos
                            .and_then(|p| decode_column(value, p))
                            .and_then(|b| String::from_utf8(b).ok());
                        if let Some(graph) = cols.get_mut(&col) {
                            graph.insert(&from_bytes, &to_bytes, rel_type)?;
                        }
                    }
                }
            }
        }

        // Maintain any Canopy JSON path indexes defined on this table.
        let json_cols: Vec<(String, usize)> = {
            let schemas = self.schemas.lock().unwrap();
            let idx_map = self.json_indexes.lock().unwrap();
            match (schemas.get(table), idx_map.get(table)) {
                (Some(schema), Some(cols)) => cols
                    .keys()
                    .filter_map(|col| {
                        schema
                            .columns
                            .iter()
                            .position(|c| c.name == *col)
                            .map(|i| (col.clone(), i))
                    })
                    .collect(),
                _ => Vec::new(),
            }
        };
        if !json_cols.is_empty() {
            let mut idx_map = self.json_indexes.lock().unwrap();
            if let Some(cols) = idx_map.get_mut(table) {
                for (col, pos) in json_cols {
                    if let Some(text_bytes) = decode_column(value, pos) {
                        if let (Ok(text), Some(jp)) =
                            (String::from_utf8(text_bytes), cols.get_mut(&col))
                        {
                            jp.insert(key, &text)?;
                        }
                    }
                }
            }
        }

        // Maintain any Canopy full-text indexes defined on this table.
        let fts_cols: Vec<(String, usize)> = {
            let schemas = self.schemas.lock().unwrap();
            let idx_map = self.fts_indexes.lock().unwrap();
            match (schemas.get(table), idx_map.get(table)) {
                (Some(schema), Some(cols)) => cols
                    .keys()
                    .filter_map(|col| {
                        schema
                            .columns
                            .iter()
                            .position(|c| c.name == *col)
                            .map(|i| (col.clone(), i))
                    })
                    .collect(),
                _ => Vec::new(),
            }
        };
        if !fts_cols.is_empty() {
            let mut idx_map = self.fts_indexes.lock().unwrap();
            if let Some(cols) = idx_map.get_mut(table) {
                for (col, pos) in fts_cols {
                    if let Some(text_bytes) = decode_column(value, pos) {
                        if let (Ok(text), Some(fts)) =
                            (String::from_utf8(text_bytes), cols.get_mut(&col))
                        {
                            let searchable = match serde_json::from_str::<serde_json::Value>(&text)
                            {
                                Ok(doc) => {
                                    let mut s = String::new();
                                    super::canopy_index::collect_json_strings(&doc, &mut s);
                                    if s.is_empty() {
                                        text.clone()
                                    } else {
                                        s
                                    }
                                }
                                Err(_) => text.clone(),
                            };
                            fts.insert(key, &searchable)?;
                        }
                    }
                }
            }
        }

        // Maintain any Prism vector (HNSW) indexes defined on this table.
        let vector_cols: Vec<(String, usize)> = {
            let schemas = self.schemas.lock().unwrap();
            let idx_map = self.vector_indexes.lock().unwrap();
            match (schemas.get(table), idx_map.get(table)) {
                (Some(schema), Some(cols)) => cols
                    .keys()
                    .filter_map(|col| {
                        schema
                            .columns
                            .iter()
                            .position(|c| c.name == *col)
                            .map(|i| (col.clone(), i))
                    })
                    .collect(),
                _ => Vec::new(),
            }
        };
        if !vector_cols.is_empty() {
            let mut idx_map = self.vector_indexes.lock().unwrap();
            if let Some(cols) = idx_map.get_mut(table) {
                for (col, pos) in vector_cols {
                    if let Some(text_bytes) = decode_column(value, pos) {
                        if let (Ok(text), Some(vec_idx)) =
                            (String::from_utf8(text_bytes), cols.get_mut(&col))
                        {
                            if let Ok(vector) = crate::vector::vector::Vector::from_text(&text) {
                                vec_idx.insert(key, vector.0)?;
                            }
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
        self.create_table_with_constraints(name, columns, vec![], vec![])
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

        let schema = self
            .schemas
            .lock()
            .unwrap()
            .get(table)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("table \"{table}\" does not exist"))?;
        let col_idx = schema
            .columns
            .iter()
            .position(|c| c.name == column)
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
        foreign_keys: Vec<super::ForeignKey>,
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
    pub fn parse_cached(&self, sql: &str) -> anyhow::Result<crate::sql::ast::Stmt> {
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

    /// Create a Meridian spatial index (`CREATE INDEX ... USING SPATIAL`) on
    /// a `GEOMETRY` column, backfilling from existing rows. `radius_hint_m`
    /// sizes the underlying S2-inspired grid level (see
    /// `geo::s2::level_for_radius`) — pick it around the typical
    /// `ST_DWithin` radius this index will serve; it's stored in the index
    /// file so later opens keep the same bucketing.
    pub fn create_spatial_index(
        &self,
        table: &str,
        column: &str,
        radius_hint_m: f64,
    ) -> Result<()> {
        self.check_writable()?;
        let index_dir = &self.local_index_dir;
        std::fs::create_dir_all(index_dir)?;
        let index_path = index_dir.join(format!("{}_{}.geo", table, column));

        let level = crate::geo::s2::level_for_radius(radius_hint_m);
        let mut geo = GeoIndex::open(&index_path, level)?;

        let schema = self
            .schemas
            .lock()
            .unwrap()
            .get(table)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("table \"{table}\" does not exist"))?;
        let col_idx = schema
            .columns
            .iter()
            .position(|c| c.name == column)
            .ok_or_else(|| anyhow::anyhow!("column \"{column}\" does not exist"))?;

        for kv in self.scan(table)? {
            if let Some(wkt_bytes) = decode_column(&kv.value, col_idx) {
                if let Ok(wkt) = String::from_utf8(wkt_bytes) {
                    if let Ok(geom) = crate::geo::geometry::Geometry::from_wkt(&wkt) {
                        let c = geom.representative_point();
                        geo.insert(&kv.key, c.x, c.y, c.t)?;
                    }
                }
            }
        }

        let mut idx_map = self.geo_indexes.lock().unwrap();
        idx_map
            .entry(table.to_string())
            .or_insert_with(HashMap::new)
            .insert(column.to_string(), geo);

        info!(table, column, "spatial index created");
        Ok(())
    }

    /// Whether a spatial index exists for `table.column`.
    pub fn indexed_column_spatial(&self, table: &str, column: &str) -> bool {
        self.geo_indexes
            .lock()
            .unwrap()
            .get(table)
            .is_some_and(|m| m.contains_key(column))
    }

    /// List all `(table, column)` pairs that have a spatial index, for
    /// `pg_catalog.pg_indexes` introspection.
    pub fn list_spatial_indexes(&self) -> Vec<(String, String)> {
        self.geo_indexes
            .lock()
            .unwrap()
            .iter()
            .flat_map(|(table, cols)| cols.keys().map(move |col| (table.clone(), col.clone())))
            .collect()
    }

    /// Row keys within `radius_m` meters of `(lon, lat)` on `table.column`'s
    /// spatial index, optionally also filtered to a `[t0, t1]` time range —
    /// a single index lookup answers both predicates at once. Returns
    /// `None` (rather than an empty vec) if no spatial index exists, so
    /// callers can distinguish "no index" from "index, zero matches".
    pub fn spatial_query(
        &self,
        table: &str,
        column: &str,
        lon: f64,
        lat: f64,
        radius_m: f64,
        time_range: Option<(i64, i64)>,
    ) -> Option<Vec<KeyValue>> {
        let idx_map = self.geo_indexes.lock().unwrap();
        let geo = idx_map.get(table)?.get(column)?;
        let keys = geo.query_radius(lon, lat, radius_m, time_range);
        drop(idx_map);
        Some(
            keys.into_iter()
                .filter_map(|k| {
                    self.read(table, &k)
                        .ok()
                        .flatten()
                        .map(|v| KeyValue { key: k, value: v })
                })
                .collect(),
        )
    }

    /// Create a Chronos time index (`CREATE INDEX ... USING TIME`) on a
    /// `TIMESTAMP` column, backfilling from existing rows. `value_column`
    /// names the numeric column whose values are bucketed/compressed
    /// alongside each row's timestamp (see `storage::ts_index`); `policy`
    /// fixes the bucket granularity and retention for the lifetime of the
    /// index, stored in the index file so later opens keep the same
    /// bucketing.
    pub fn create_time_index(
        &self,
        table: &str,
        column: &str,
        value_column: &str,
        policy: TimeBucketPolicy,
    ) -> Result<()> {
        self.check_writable()?;
        let index_dir = &self.local_index_dir;
        std::fs::create_dir_all(index_dir)?;
        let index_path = index_dir.join(format!("{}_{}.ts", table, column));

        let mut ts = TimeIndex::open(&index_path, policy, value_column)?;

        let schema = self
            .schemas
            .lock()
            .unwrap()
            .get(table)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("table \"{table}\" does not exist"))?;
        let ts_idx = schema
            .columns
            .iter()
            .position(|c| c.name == column)
            .ok_or_else(|| anyhow::anyhow!("column \"{column}\" does not exist"))?;
        let val_idx = schema
            .columns
            .iter()
            .position(|c| c.name == value_column)
            .ok_or_else(|| anyhow::anyhow!("column \"{value_column}\" does not exist"))?;

        for kv in self.scan(table)? {
            let ts_bytes = decode_column(&kv.value, ts_idx);
            let val_bytes = decode_column(&kv.value, val_idx);
            if let (Some(ts_bytes), Some(val_bytes)) = (ts_bytes, val_bytes) {
                if let (Some(t), Some(v)) = (decode_i64(&ts_bytes), decode_f64(&val_bytes)) {
                    ts.insert(&kv.key, t, v)?;
                }
            }
        }

        let mut idx_map = self.ts_indexes.lock().unwrap();
        idx_map
            .entry(table.to_string())
            .or_insert_with(HashMap::new)
            .insert(
                column.to_string(),
                TsIndexEntry {
                    index: ts,
                    value_column: value_column.to_string(),
                },
            );

        info!(table, column, value_column, "time index created");
        Ok(())
    }

    /// Whether a Chronos time index exists for `table.column`.
    pub fn indexed_column_time(&self, table: &str, column: &str) -> bool {
        self.ts_indexes
            .lock()
            .unwrap()
            .get(table)
            .is_some_and(|m| m.contains_key(column))
    }

    /// List all `(table, column)` pairs that have a time index, for
    /// `pg_catalog.pg_indexes` introspection.
    pub fn list_time_indexes(&self) -> Vec<(String, String)> {
        self.ts_indexes
            .lock()
            .unwrap()
            .iter()
            .flat_map(|(table, cols)| cols.keys().map(move |col| (table.clone(), col.clone())))
            .collect()
    }

    /// Row keys with `t0 <= timestamp <= t1` on `table.column`'s time index.
    /// Returns `None` (rather than an empty vec) if no time index exists.
    pub fn time_range_query(
        &self,
        table: &str,
        column: &str,
        t0: i64,
        t1: i64,
    ) -> Option<Vec<KeyValue>> {
        let idx_map = self.ts_indexes.lock().unwrap();
        let ts = &idx_map.get(table)?.get(column)?.index;
        let keys = ts.query_range(t0, t1);
        drop(idx_map);
        Some(
            keys.into_iter()
                .filter_map(|k| {
                    self.read(table, &k)
                        .ok()
                        .flatten()
                        .map(|v| KeyValue { key: k, value: v })
                })
                .collect(),
        )
    }

    /// Per-bucket rollups covering `[t0, t1]` on `table.column`'s time
    /// index — the continuous-aggregate query path (e.g. `moving_average`),
    /// which can answer from rollups even for buckets whose raw rows have
    /// already been evicted by retention. Returns `None` if no time index
    /// exists.
    pub fn rollup_query(
        &self,
        table: &str,
        column: &str,
        t0: i64,
        t1: i64,
    ) -> Option<Vec<(i64, Rollup)>> {
        let idx_map = self.ts_indexes.lock().unwrap();
        let ts = &idx_map.get(table)?.get(column)?.index;
        Some(ts.query_rollups(t0, t1))
    }

    /// Create a Plexus adjacency index (`CREATE INDEX ... USING GRAPH`) on an
    /// edge table, backfilling from existing rows. `from_column` is the
    /// indexed column (matches `CreateIndexStmt.column`); `to_column` names
    /// the destination-vertex column and `type_column` (optional) names a
    /// relationship-type column for multi-relational (typed-edge) graphs.
    pub fn create_graph_index(
        &self,
        table: &str,
        from_column: &str,
        to_column: &str,
        type_column: Option<&str>,
    ) -> Result<()> {
        self.check_writable()?;
        let index_dir = &self.local_index_dir;
        std::fs::create_dir_all(index_dir)?;
        let index_path = index_dir.join(format!("{}_{}.graph", table, from_column));

        let mut graph = GraphIndex::open(&index_path, to_column, type_column)?;

        let schema = self
            .schemas
            .lock()
            .unwrap()
            .get(table)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("table \"{table}\" does not exist"))?;
        let from_idx = schema
            .columns
            .iter()
            .position(|c| c.name == from_column)
            .ok_or_else(|| anyhow::anyhow!("column \"{from_column}\" does not exist"))?;
        let to_idx = schema
            .columns
            .iter()
            .position(|c| c.name == to_column)
            .ok_or_else(|| anyhow::anyhow!("column \"{to_column}\" does not exist"))?;
        let type_idx = type_column
            .map(|t| {
                schema
                    .columns
                    .iter()
                    .position(|c| c.name == t)
                    .ok_or_else(|| anyhow::anyhow!("column \"{t}\" does not exist"))
            })
            .transpose()?;

        for kv in self.scan(table)? {
            let from_bytes = decode_column(&kv.value, from_idx);
            let to_bytes = decode_column(&kv.value, to_idx);
            if let (Some(from_bytes), Some(to_bytes)) = (from_bytes, to_bytes) {
                let rel_type = type_idx
                    .and_then(|i| decode_column(&kv.value, i))
                    .and_then(|b| String::from_utf8(b).ok());
                graph.insert(&from_bytes, &to_bytes, rel_type)?;
            }
        }

        let mut idx_map = self.graph_indexes.lock().unwrap();
        idx_map
            .entry(table.to_string())
            .or_insert_with(HashMap::new)
            .insert(from_column.to_string(), graph);

        info!(table, from_column, to_column, "graph index created");
        Ok(())
    }

    /// Whether a graph (adjacency) index exists for `table.from_column`.
    pub fn indexed_column_graph(&self, table: &str, from_column: &str) -> bool {
        self.graph_indexes
            .lock()
            .unwrap()
            .get(table)
            .is_some_and(|m| m.contains_key(from_column))
    }

    /// List all `(table, from_column)` pairs that have a graph index, for
    /// `pg_catalog.pg_indexes` introspection.
    pub fn list_graph_indexes(&self) -> Vec<(String, String)> {
        self.graph_indexes
            .lock()
            .unwrap()
            .iter()
            .flat_map(|(table, cols)| cols.keys().map(move |col| (table.clone(), col.clone())))
            .collect()
    }

    /// Neighbours of `vertex_key` in the given direction on `table.from_column`'s
    /// graph index, each as `(neighbour_key, rel_type)`. `None` if no such
    /// index exists or the vertex was never indexed (no edges touch it).
    pub fn graph_neighbors(
        &self,
        table: &str,
        from_column: &str,
        vertex_key: &[u8],
        dir: crate::graph::Direction,
    ) -> Option<Vec<(Vec<u8>, Option<String>)>> {
        let idx_map = self.graph_indexes.lock().unwrap();
        let graph = idx_map.get(table)?.get(from_column)?.graph();
        let id = graph.id_of(vertex_key)?;
        Some(
            graph
                .neighbors(id, dir)
                .into_iter()
                .filter_map(|(n, rel)| graph.key_of(n).map(|k| (k.to_vec(), rel)))
                .collect(),
        )
    }

    /// Bounded-depth breadth-first traversal from `start_key`, as
    /// `(vertex_key, depth)` pairs.
    pub fn graph_bfs(
        &self,
        table: &str,
        from_column: &str,
        start_key: &[u8],
        max_depth: usize,
        dir: crate::graph::Direction,
    ) -> Option<Vec<(Vec<u8>, usize)>> {
        let idx_map = self.graph_indexes.lock().unwrap();
        let graph = idx_map.get(table)?.get(from_column)?.graph();
        let start = graph.id_of(start_key)?;
        Some(
            crate::graph::algorithms::bfs_traverse(graph, start, max_depth, dir)
                .into_iter()
                .filter_map(|(id, depth)| graph.key_of(id).map(|k| (k.to_vec(), depth)))
                .collect(),
        )
    }

    /// Unweighted shortest path between two vertex keys, as an ordered list
    /// of vertex keys including both endpoints. `Some(None)` means the index
    /// exists but no path was found; `None` means the index or an endpoint
    /// vertex doesn't exist.
    pub fn graph_shortest_path(
        &self,
        table: &str,
        from_column: &str,
        start_key: &[u8],
        end_key: &[u8],
        dir: crate::graph::Direction,
    ) -> Option<Option<Vec<Vec<u8>>>> {
        let idx_map = self.graph_indexes.lock().unwrap();
        let graph = idx_map.get(table)?.get(from_column)?.graph();
        let start = graph.id_of(start_key)?;
        let end = graph.id_of(end_key)?;
        Some(
            crate::graph::algorithms::shortest_path(graph, start, end, dir).map(|path| {
                path.into_iter()
                    .filter_map(|id| graph.key_of(id).map(|k| k.to_vec()))
                    .collect()
            }),
        )
    }

    /// Weakly-connected component id per vertex, as `(vertex_key, component_id)`.
    pub fn graph_connected_components(
        &self,
        table: &str,
        from_column: &str,
    ) -> Option<Vec<(Vec<u8>, u32)>> {
        let idx_map = self.graph_indexes.lock().unwrap();
        let graph = idx_map.get(table)?.get(from_column)?.graph();
        let components = crate::graph::algorithms::connected_components(graph);
        Some(
            graph
                .vertex_ids()
                .filter_map(|id| {
                    graph
                        .key_of(id)
                        .map(|k| (k.to_vec(), components[id as usize]))
                })
                .collect(),
        )
    }

    /// PageRank score per vertex, as `(vertex_key, score)`.
    pub fn graph_pagerank(
        &self,
        table: &str,
        from_column: &str,
        damping: f64,
        iterations: usize,
    ) -> Option<Vec<(Vec<u8>, f64)>> {
        let idx_map = self.graph_indexes.lock().unwrap();
        let graph = idx_map.get(table)?.get(from_column)?.graph();
        let ranks = crate::graph::algorithms::pagerank(graph, damping, iterations);
        Some(
            graph
                .vertex_ids()
                .filter_map(|id| graph.key_of(id).map(|k| (k.to_vec(), ranks[id as usize])))
                .collect(),
        )
    }

    /// Per-vertex triangle count, as `(vertex_key, triangle_count)`.
    pub fn graph_triangle_count(
        &self,
        table: &str,
        from_column: &str,
    ) -> Option<Vec<(Vec<u8>, u64)>> {
        let idx_map = self.graph_indexes.lock().unwrap();
        let graph = idx_map.get(table)?.get(from_column)?.graph();
        let (counts, _total) = crate::graph::algorithms::triangle_count(graph);
        Some(
            graph
                .vertex_ids()
                .filter_map(|id| graph.key_of(id).map(|k| (k.to_vec(), counts[id as usize])))
                .collect(),
        )
    }

    /// Create a Prism vector index (`CREATE INDEX ... USING VECTOR/HNSW`) on
    /// a `VECTOR` column, backfilling from existing rows. `metric`/`config`
    /// are stored in the index file so later opens keep the same HNSW graph
    /// shape (mirrors how Meridian's `radius_hint_m` fixes a spatial index's
    /// grid level for its lifetime).
    pub fn create_vector_index(
        &self,
        table: &str,
        column: &str,
        metric: Metric,
        config: HnswConfig,
    ) -> Result<()> {
        self.check_writable()?;
        let index_dir = &self.local_index_dir;
        std::fs::create_dir_all(index_dir)?;
        let index_path = index_dir.join(format!("{}_{}.vec", table, column));

        let mut vec_idx = VectorIndex::open(&index_path, metric, config)?;

        let schema = self
            .schemas
            .lock()
            .unwrap()
            .get(table)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("table \"{table}\" does not exist"))?;
        let col_idx = schema
            .columns
            .iter()
            .position(|c| c.name == column)
            .ok_or_else(|| anyhow::anyhow!("column \"{column}\" does not exist"))?;

        for kv in self.scan(table)? {
            if let Some(text_bytes) = decode_column(&kv.value, col_idx) {
                if let Ok(text) = String::from_utf8(text_bytes) {
                    if let Ok(vector) = crate::vector::vector::Vector::from_text(&text) {
                        vec_idx.insert(&kv.key, vector.0)?;
                    }
                }
            }
        }

        let mut idx_map = self.vector_indexes.lock().unwrap();
        idx_map
            .entry(table.to_string())
            .or_insert_with(HashMap::new)
            .insert(column.to_string(), vec_idx);

        info!(table, column, "vector index created");
        Ok(())
    }

    /// Whether a vector (HNSW) index exists for `table.column`.
    pub fn indexed_column_vector(&self, table: &str, column: &str) -> bool {
        self.vector_indexes
            .lock()
            .unwrap()
            .get(table)
            .is_some_and(|m| m.contains_key(column))
    }

    /// List all `(table, column)` pairs that have a vector (HNSW) index, for
    /// `pg_catalog.pg_indexes` introspection.
    pub fn list_vector_indexes(&self) -> Vec<(String, String)> {
        self.vector_indexes
            .lock()
            .unwrap()
            .iter()
            .flat_map(|(table, cols)| cols.keys().map(move |col| (table.clone(), col.clone())))
            .collect()
    }

    /// Create a Prism IVF-PQ index (`CREATE INDEX ... USING IVFPQ`) on a
    /// `VECTOR` column, training the coarse quantizer + PQ codebooks from
    /// existing rows (backfill-only — see `IvfPqStorageIndex`'s doc-comment
    /// for why this can't start empty the way `create_vector_index`'s HNSW
    /// index can).
    pub fn create_ivfpq_index(
        &self,
        table: &str,
        column: &str,
        metric: Metric,
        n_lists: usize,
        pq_m: usize,
        n_probe: usize,
    ) -> Result<()> {
        self.check_writable()?;
        let index_dir = &self.local_index_dir;
        std::fs::create_dir_all(index_dir)?;
        let index_path = index_dir.join(format!("{}_{}.ivfpq", table, column));

        let schema = self
            .schemas
            .lock()
            .unwrap()
            .get(table)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("table \"{table}\" does not exist"))?;
        let col_idx = schema
            .columns
            .iter()
            .position(|c| c.name == column)
            .ok_or_else(|| anyhow::anyhow!("column \"{column}\" does not exist"))?;

        let mut training = Vec::new();
        for kv in self.scan(table)? {
            if let Some(text_bytes) = decode_column(&kv.value, col_idx) {
                if let Ok(text) = String::from_utf8(text_bytes) {
                    if let Ok(vector) = crate::vector::vector::Vector::from_text(&text) {
                        training.push((kv.key, vector.0));
                    }
                }
            }
        }

        let ivf_idx = IvfPqStorageIndex::train_and_create(
            &index_path,
            metric,
            n_lists,
            pq_m,
            n_probe,
            training,
        )?;

        let mut idx_map = self.ivf_pq_indexes.lock().unwrap();
        idx_map
            .entry(table.to_string())
            .or_insert_with(HashMap::new)
            .insert(column.to_string(), ivf_idx);

        info!(table, column, "IVF-PQ index created");
        Ok(())
    }

    /// Whether an IVF-PQ index exists for `table.column`.
    pub fn indexed_column_ivfpq(&self, table: &str, column: &str) -> bool {
        self.ivf_pq_indexes
            .lock()
            .unwrap()
            .get(table)
            .is_some_and(|m| m.contains_key(column))
    }

    /// List all `(table, column)` pairs that have an IVF-PQ index, for
    /// `pg_catalog.pg_indexes` introspection.
    pub fn list_ivfpq_indexes(&self) -> Vec<(String, String)> {
        self.ivf_pq_indexes
            .lock()
            .unwrap()
            .iter()
            .flat_map(|(table, cols)| cols.keys().map(move |col| (table.clone(), col.clone())))
            .collect()
    }

    /// Approximate k-nearest-neighbor search against `table.column`'s vector
    /// index, returning `(row, distance)` pairs sorted nearest-first. `None`
    /// (rather than an empty vec) if no vector index of either kind exists,
    /// so callers can distinguish "no index" from "index, zero matches" —
    /// same convention as `spatial_query`/`time_range_query`.
    ///
    /// **Automatic index selection** (`TODO.md` Phase 7): when a column has
    /// *both* an HNSW and an IVF-PQ index, this picks HNSW below
    /// `IVFPQ_PREFERRED_ROW_COUNT` rows and IVF-PQ at or above it — a
    /// documented, honest size-based heuristic (HNSW gives better recall per
    /// query at small/medium scale; IVF-PQ's compressed in-memory
    /// representation matters once the graph would otherwise be large), not
    /// a latency/recall cost-model the way a real query optimizer would do
    /// it. There's no benchmark harness in this repo to tune or validate a
    /// fancier policy against (same honesty precedent as every other
    /// "automatic"/"optimal" claim in this codebase).
    pub fn vector_knn_query(
        &self,
        table: &str,
        column: &str,
        query: &[f32],
        k: usize,
        ef_search: Option<usize>,
    ) -> Option<Vec<(KeyValue, f32)>> {
        const IVFPQ_PREFERRED_ROW_COUNT: usize = 100_000;

        let hits = {
            let vec_map = self.vector_indexes.lock().unwrap();
            let hnsw_idx = vec_map.get(table).and_then(|m| m.get(column));
            let ivf_map = self.ivf_pq_indexes.lock().unwrap();
            let ivf_idx = ivf_map.get(table).and_then(|m| m.get(column));

            match (hnsw_idx, ivf_idx) {
                (Some(_hnsw), Some(ivf)) if ivf.len() >= IVFPQ_PREFERRED_ROW_COUNT => {
                    ivf.query_knn(query, k, ef_search)
                }
                (Some(hnsw), _) => hnsw.query_knn(query, k, ef_search),
                (None, Some(ivf)) => ivf.query_knn(query, k, ef_search),
                // No HNSW/IVF-PQ index: fall back to a GPU brute-force batch
                // k-NN when an adapter is available (fails safe to `None`, the
                // historical "no vector index" contract, when it isn't).
                (None, None) => return self.gpu_vector_knn(table, column, query, k, ef_search),
            }
        };
        Some(
            hits.into_iter()
                .filter_map(|(k, dist)| {
                    self.read(table, &k)
                        .ok()
                        .flatten()
                        .map(|v| (KeyValue { key: k, value: v }, dist))
                })
                .collect(),
        )
    }

    /// GPU-accelerated brute-force k-NN fallback for `vector_knn_query`, used
    /// when no HNSW/IVF-PQ index exists on `table.column`. Scans every row,
    /// extracts its `VECTOR` cell into a flat `f32` base matrix, runs the
    /// whole query×base distance matrix on the device, and returns the `k`
    /// nearest by distance.
    ///
    /// Fails safe to `None` — exactly the historical "no vector index" contract
    /// — whenever the GPU path is unavailable (`gpu_available()` is `false`),
    /// any row fails to decode, dimensions mismatch, or the dispatch is refused
    /// for being too large. So `vector_search`/`hybrid_search` keep erroring
    /// exactly as they always did when no GPU is present. Metric defaults to
    /// L2 (the HNSW default) since a bare `VECTOR` column carries no metric
    /// metadata.
    fn gpu_vector_knn(
        &self,
        table: &str,
        column: &str,
        query: &[f32],
        k: usize,
        _ef_search: Option<usize>,
    ) -> Option<Vec<(KeyValue, f32)>> {
        if !crate::vector::gpu::gpu_available() {
            return None;
        }
        let dim = query.len();
        if dim == 0 {
            return None;
        }
        let schema = self.get_table(table).ok().flatten()?;
        let col_pos = schema.columns.iter().position(|c| c.name == column)?;
        if schema.columns[col_pos].col_type != ColumnType::Vector {
            return None;
        }

        let rows = self.scan(table).ok()?;
        if rows.is_empty() {
            return None;
        }

        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut base: Vec<f32> = Vec::with_capacity(rows.len() * dim);
        for kv in &rows {
            let text_bytes = decode_column(&kv.value, col_pos)?;
            let text = String::from_utf8(text_bytes).ok()?;
            let vector = crate::vector::vector::Vector::from_text(&text).ok()?;
            if vector.dim() != dim {
                return None;
            }
            keys.push(kv.key.clone());
            base.extend_from_slice(vector.as_slice());
        }

        let hits = crate::vector::gpu::gpu_brute_force_knn(query, &base, dim, Metric::L2, k).ok()?;
        Some(
            hits.into_iter()
                .filter_map(|(idx, dist)| {
                    let key = &keys[idx as usize];
                    self.read(table, key)
                        .ok()
                        .flatten()
                        .map(|v| (KeyValue { key: key.clone(), value: v }, dist))
                })
                .collect(),
        )
    }

    /// Create a Canopy path index (`CREATE INDEX ... USING JSONPATH ON
    /// t(col) WITH (path = 'user.address.city')`) on a `Json` column,
    /// backfilling from existing rows.
    pub fn create_json_path_index(&self, table: &str, column: &str, json_path: &str) -> Result<()> {
        self.check_writable()?;
        let index_dir = &self.local_index_dir;
        std::fs::create_dir_all(index_dir)?;
        let index_path = index_dir.join(format!("{}_{}.jsonpath", table, column));

        let mut jp = JsonPathIndex::open(&index_path, json_path)?;

        let schema = self
            .schemas
            .lock()
            .unwrap()
            .get(table)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("table \"{table}\" does not exist"))?;
        let col_idx = schema
            .columns
            .iter()
            .position(|c| c.name == column)
            .ok_or_else(|| anyhow::anyhow!("column \"{column}\" does not exist"))?;

        for kv in self.scan(table)? {
            if let Some(text_bytes) = decode_column(&kv.value, col_idx) {
                if let Ok(text) = String::from_utf8(text_bytes) {
                    jp.insert(&kv.key, &text)?;
                }
            }
        }

        let mut idx_map = self.json_indexes.lock().unwrap();
        idx_map
            .entry(table.to_string())
            .or_insert_with(HashMap::new)
            .insert(column.to_string(), jp);

        info!(table, column, json_path, "json path index created");
        Ok(())
    }

    /// Whether a Canopy path index exists for `table.column`.
    pub fn indexed_column_json_path(&self, table: &str, column: &str) -> bool {
        self.json_indexes
            .lock()
            .unwrap()
            .get(table)
            .is_some_and(|m| m.contains_key(column))
    }

    /// List all `(table, column)` pairs that have a JSON path index, for
    /// `pg_catalog.pg_indexes` introspection.
    pub fn list_json_indexes(&self) -> Vec<(String, String)> {
        self.json_indexes
            .lock()
            .unwrap()
            .iter()
            .flat_map(|(table, cols)| cols.keys().map(move |col| (table.clone(), col.clone())))
            .collect()
    }

    /// Row keys whose `table.column` document has `value_text` at the
    /// indexed path. `None` if no path index exists.
    pub fn json_path_lookup(
        &self,
        table: &str,
        column: &str,
        value_text: &str,
    ) -> Option<Vec<Vec<u8>>> {
        let idx_map = self.json_indexes.lock().unwrap();
        Some(idx_map.get(table)?.get(column)?.lookup(value_text))
    }

    /// Create a Canopy inverted full-text index (`CREATE INDEX ... USING
    /// GIN`/`USING FTS`) over a `Json` or `Text` column, backfilling from
    /// existing rows. For a `Json` column, only the string leaf values in
    /// each document are tokenized (see `canopy_index::collect_json_strings`).
    pub fn create_fts_index(&self, table: &str, column: &str) -> Result<()> {
        self.check_writable()?;
        let index_dir = &self.local_index_dir;
        std::fs::create_dir_all(index_dir)?;
        let index_path = index_dir.join(format!("{}_{}.fts", table, column));

        let mut fts = FtsIndex::open(&index_path)?;

        let schema = self
            .schemas
            .lock()
            .unwrap()
            .get(table)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("table \"{table}\" does not exist"))?;
        let col_idx = schema
            .columns
            .iter()
            .position(|c| c.name == column)
            .ok_or_else(|| anyhow::anyhow!("column \"{column}\" does not exist"))?;

        for kv in self.scan(table)? {
            if let Some(text_bytes) = decode_column(&kv.value, col_idx) {
                if let Ok(text) = String::from_utf8(text_bytes) {
                    let searchable = match serde_json::from_str::<serde_json::Value>(&text) {
                        Ok(doc) => {
                            let mut s = String::new();
                            super::canopy_index::collect_json_strings(&doc, &mut s);
                            if s.is_empty() {
                                text.clone()
                            } else {
                                s
                            }
                        }
                        Err(_) => text.clone(),
                    };
                    fts.insert(&kv.key, &searchable)?;
                }
            }
        }

        let mut idx_map = self.fts_indexes.lock().unwrap();
        idx_map
            .entry(table.to_string())
            .or_insert_with(HashMap::new)
            .insert(column.to_string(), fts);

        info!(table, column, "full-text index created");
        Ok(())
    }

    /// Whether a full-text index exists for `table.column`.
    pub fn indexed_column_fts(&self, table: &str, column: &str) -> bool {
        self.fts_indexes
            .lock()
            .unwrap()
            .get(table)
            .is_some_and(|m| m.contains_key(column))
    }

    /// List all `(table, column)` pairs that have a full-text index, for
    /// `pg_catalog.pg_indexes` introspection.
    pub fn list_fts_indexes(&self) -> Vec<(String, String)> {
        self.fts_indexes
            .lock()
            .unwrap()
            .iter()
            .flat_map(|(table, cols)| cols.keys().map(move |col| (table.clone(), col.clone())))
            .collect()
    }

    /// Row keys whose `table.column` text contains every token in `query`
    /// (AND semantics). `None` if no full-text index exists.
    pub fn fts_search(&self, table: &str, column: &str, query: &str) -> Option<Vec<Vec<u8>>> {
        let idx_map = self.fts_indexes.lock().unwrap();
        Some(idx_map.get(table)?.get(column)?.search_and(query))
    }

    /// Row keys ranked by BM25 relevance against `query` (OR semantics,
    /// descending score, truncated to `limit`). `None` if no full-text index
    /// exists on `table.column`.
    pub fn fts_search_bm25(
        &self,
        table: &str,
        column: &str,
        query: &str,
        limit: usize,
    ) -> Option<Vec<(Vec<u8>, f64)>> {
        let idx_map = self.fts_indexes.lock().unwrap();
        Some(idx_map.get(table)?.get(column)?.search_bm25(query, limit))
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

    /// Create a named Flux topic (`CREATE TOPIC`), failing if one with this
    /// name already exists — callers implementing `IF NOT EXISTS` check
    /// `list_topics` first (mirrors `create_sequence`).
    pub fn create_topic(
        &self,
        name: &str,
        partitions: u32,
        retention_ms: Option<i64>,
        retention_bytes: Option<u64>,
    ) -> Result<()> {
        self.check_writable()?;
        let mut topics = self.topics.lock().unwrap();
        if topics.contains_key(name) {
            anyhow::bail!("topic \"{name}\" already exists");
        }
        let dir = self.flux_dir.join(name);
        let log = FluxLog::create(
            &dir,
            partitions,
            RetentionPolicy {
                retention_ms,
                retention_bytes,
            },
        )?;
        topics.insert(name.to_string(), log);
        info!(topic = name, partitions, "topic created");
        Ok(())
    }

    /// Creates `__cdc_<table>` (1 partition, unlimited retention) the first
    /// time a mutation happens on `table`, otherwise a no-op. Unlike
    /// `create_topic`, this never errors on "already exists" — it's called
    /// unconditionally on every insert/update/delete.
    fn ensure_cdc_topic(&self, table: &str) -> Result<String> {
        let topic = format!("__cdc_{table}");
        let mut topics = self.topics.lock().unwrap();
        if !topics.contains_key(&topic) {
            let dir = self.flux_dir.join(&topic);
            let log = FluxLog::create(&dir, 1, RetentionPolicy::default())?;
            topics.insert(topic.clone(), log);
        }
        Ok(topic)
    }

    /// Publish one record to `topic`. See `storage::flux` module docs for
    /// partition assignment when `partition` is `None`.
    pub fn flux_publish(
        &self,
        topic: &str,
        partition: Option<u32>,
        key: Option<Vec<u8>>,
        value: Vec<u8>,
    ) -> Result<(u32, u64)> {
        self.check_writable()?;
        let mut topics = self.topics.lock().unwrap();
        let log = topics
            .get_mut(topic)
            .ok_or_else(|| anyhow::anyhow!("topic \"{topic}\" does not exist"))?;
        let (p, offset) = log.publish(partition, key.clone(), value.clone())?;
        let _ = self.flux_bus.send((
            topic.to_string(),
            FluxRecord {
                offset,
                key,
                value,
                timestamp_ms: super::flux::now_ms(),
            },
        ));
        Ok((p, offset))
    }

    /// Publish a native CDC event (see `executor::execute_insert`/`update`/
    /// `delete`) to `table`'s implicit `__cdc_<table>` topic, auto-creating
    /// it on first use.
    pub fn flux_publish_cdc(&self, table: &str, event: serde_json::Value) -> Result<()> {
        let topic = self.ensure_cdc_topic(table)?;
        let bytes = serde_json::to_vec(&event)?;
        self.flux_publish(&topic, None, None, bytes)?;
        Ok(())
    }

    /// Records at/after `group`'s tracked offset on `topic`'s `partition`,
    /// without advancing it.
    pub fn flux_poll(
        &self,
        topic: &str,
        partition: u32,
        group: &str,
        max: usize,
    ) -> Result<Vec<FluxRecord>> {
        let topics = self.topics.lock().unwrap();
        let log = topics
            .get(topic)
            .ok_or_else(|| anyhow::anyhow!("topic \"{topic}\" does not exist"))?;
        log.poll(group, partition, max)
    }

    /// Advances `group`'s tracked offset on `topic`'s `partition`.
    pub fn flux_commit(&self, topic: &str, partition: u32, group: &str, offset: u64) -> Result<()> {
        let mut topics = self.topics.lock().unwrap();
        let log = topics
            .get_mut(topic)
            .ok_or_else(|| anyhow::anyhow!("topic \"{topic}\" does not exist"))?;
        log.commit(group, partition, offset)
    }

    /// Every record currently retained on `topic`'s `partition`, bypassing
    /// consumer-group tracking — used by the time-travel/windowing table
    /// functions, which replay the whole log rather than one consumer's
    /// unread tail.
    pub fn flux_all(&self, topic: &str, partition: u32) -> Option<Vec<FluxRecord>> {
        let topics = self.topics.lock().unwrap();
        topics.get(topic)?.all_records(partition)
    }

    /// Number of partitions on `topic`, or `None` if it doesn't exist.
    pub fn flux_num_partitions(&self, topic: &str) -> Option<u32> {
        self.topics
            .lock()
            .unwrap()
            .get(topic)
            .map(|t| t.num_partitions())
    }

    /// `(name, partition_count)` for every topic — `pg_catalog`-style
    /// introspection, mirrors `list_sequences`.
    pub fn list_topics(&self) -> Vec<(String, u32)> {
        self.topics
            .lock()
            .unwrap()
            .iter()
            .map(|(name, log)| (name.clone(), log.num_partitions()))
            .collect()
    }

    /// Subscribe to every Flux publish across every topic; the WebSocket
    /// endpoint filters by topic name itself (mirrors
    /// `subscribe_notifications`).
    pub fn subscribe_flux(&self) -> tokio::sync::broadcast::Receiver<(String, FluxRecord)> {
        self.flux_bus.subscribe()
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
