pub mod btree;
pub mod cache;
pub mod canopy_index;
#[cfg(test)]
mod chaos_tests;
pub mod compress;
pub mod config;
pub mod database;
pub mod diskann_index;
pub mod flux;
pub mod geo_index;
pub mod graph_index;
pub mod guard;
pub mod io_backend;
pub mod ivf_pq_index;
pub mod json_schema;
pub mod jsonb;
pub mod lease;
pub mod lsm;
pub mod manifest;
pub mod mvcc;
#[cfg(test)]
mod mvcc_tests;
pub mod objectstore;
#[cfg(test)]
mod phase3_tests;
pub mod sharded_vector_index;
pub mod sstable;
#[cfg(test)]
pub(crate) mod test_support;
pub mod ts_index;
pub mod tx;
pub mod vector_index;
pub mod wal;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// A key-value pair stored in the database.
#[derive(Debug, Clone, PartialEq)]
pub struct KeyValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

/// A record type tag for the WAL and internal storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordType {
    Insert,
    Update,
    Delete,
}

/// The core storage engine interface used by the SQL executor.
pub trait StorageEngine: Send + Sync {
    /// Insert or update a row.
    fn write(&self, table: &str, key: &[u8], value: &[u8]) -> Result<()>;
    /// Read a row by key.
    fn read(&self, table: &str, key: &[u8]) -> Result<Option<Vec<u8>>>;
    /// Delete a row by key.
    fn delete(&self, table: &str, key: &[u8]) -> Result<()>;
    /// Scan all rows in a table.
    fn scan(&self, table: &str) -> Result<Vec<KeyValue>>;
    /// Create a new table (schema tracking).
    fn create_table(&self, name: &str, columns: &[ColumnDef]) -> Result<()>;
    /// Get table schema.
    fn get_table(&self, name: &str) -> Result<Option<TableSchema>>;
    /// List all tables.
    fn list_tables(&self) -> Result<Vec<String>>;
    /// Create a B-Tree index on a column.
    fn create_index(&self, table: &str, column: &str) -> Result<()>;
}

/// Column definition for DDL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub col_type: ColumnType,
    pub nullable: bool,
    pub default: Option<String>,
    pub is_pk: bool,
}

/// Supported column types.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColumnType {
    Int8,
    Int4,
    Int2,
    Float8,
    Float4,
    Text,
    Bool,
    Timestamp,
    Date,
    Json,
    Bytea,
    /// Meridian geospatial type. Stored on the wire/in cells as WKT text
    /// (`Value::Text`) — see `geo::geometry` — so no new row-encoding path
    /// is needed; this variant exists for DDL/catalog purposes (so
    /// `\d table` and `pg_attribute` report it distinctly from `TEXT`) and
    /// so the planner/eval layer can tell a geometry column from a plain
    /// text one when deciding whether to build/use a spatial index.
    /// Planar semantics: `ST_Area`/`ST_Length`-style functions treat
    /// coordinates as flat-plane units, no implied SRID.
    Geometry,
    /// OGC `GEOGRAPHY` type — same WKT-as-text storage and DDL/catalog
    /// treatment as `Geometry`, kept as a separate variant purely so
    /// `\d table`/`pg_attribute` and the parser can distinguish the two
    /// (`GEOMETRY(...)` vs `GEOGRAPHY(...)` in DDL), per the OGC/SQL-MM
    /// convention that `GEOGRAPHY` implies geodetic (lon/lat, SRID 4326)
    /// coordinates while plain `GEOMETRY` is CRS-agnostic/planar. This
    /// engine's distance/area functions already use haversine great-circle
    /// math unconditionally (see `geo::geometry::haversine_distance_m`), so
    /// there's no separate geodetic code path yet — the distinction is
    /// currently catalog-only, not a different evaluation strategy.
    Geography,
    /// Prism vector/embedding type. Stored on the wire/in cells as
    /// `[1.0,2.0,3.0]` text (`Value::Text`) — see `vector::vector::Vector`
    /// — following the exact same "no new row-encoding path" precedent as
    /// `Geometry`'s WKT-as-text representation. Exists as its own variant
    /// so DDL/catalog introspection and the executor's vector-index
    /// backfill path can tell a `VECTOR` column apart from plain `TEXT`.
    Vector,
    /// Meridian raster type — a single-band `f64` grid with a georeferenced
    /// origin/pixel scale/SRID (`geo::raster::Raster`), stored as
    /// `Value::Text` holding a hex-encoded hand-written binary encoding
    /// (`Raster::to_hex`/`from_hex`) — the exact same "no new row-encoding
    /// path, reuse `Value::Text`" precedent as `Geometry`'s WKB hex. Exists
    /// as its own variant purely for DDL/catalog reporting, same reasoning
    /// as `Geometry`/`Geography`.
    Raster,
}

impl ColumnType {
    pub fn oid(&self) -> i32 {
        use crate::wire::messages::oid;
        match self {
            Self::Int8 => oid::INT8,
            Self::Int4 => 23,
            Self::Int2 => 21,
            Self::Float8 => oid::FLOAT8,
            Self::Float4 => 700,
            Self::Text => oid::TEXT,
            Self::Bool => oid::BOOL,
            Self::Timestamp => 1114,
            Self::Date => 1082,
            Self::Json => 114,
            Self::Bytea => 17,
            // No real Postgres OID for a WKT-as-text geometry type; reuse
            // TEXT's OID so wire clients that don't know GEOMETRY still
            // render it as a plain string instead of erroring.
            Self::Geometry => oid::TEXT,
            // Same reasoning as Geometry.
            Self::Geography => oid::TEXT,
            // Same reasoning as Geometry: no real Postgres OID for a
            // pgvector-style VECTOR type in this from-scratch wire protocol.
            Self::Vector => oid::TEXT,
            // Same reasoning as Geometry: no real Postgres raster OID here.
            Self::Raster => oid::TEXT,
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_lowercase().as_str() {
            "int8" | "bigint" => Some(Self::Int8),
            "int4" | "integer" | "int" => Some(Self::Int4),
            "int2" | "smallint" => Some(Self::Int2),
            "float8" | "double" | "double precision" | "float" => Some(Self::Float8),
            "float4" | "real" => Some(Self::Float4),
            "text" | "varchar" | "char" | "character varying" | "character" => Some(Self::Text),
            "bool" | "boolean" => Some(Self::Bool),
            "timestamp" | "timestamptz" => Some(Self::Timestamp),
            "date" => Some(Self::Date),
            "json" | "jsonb" => Some(Self::Json),
            "bytea" | "blob" => Some(Self::Bytea),
            "geometry" | "point" => Some(Self::Geometry),
            "geography" => Some(Self::Geography),
            "vector" | "embedding" => Some(Self::Vector),
            "raster" => Some(Self::Raster),
            _ => None,
        }
    }
}

/// A single-column foreign key: `columns[column]` must match some row's
/// `ref_column` value in `ref_table`, or be NULL. Composite FKs are a
/// documented scope cut.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKey {
    pub column: usize,
    pub ref_table: String,
    pub ref_column: String,
}

/// Schema for a single table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub pk_columns: Vec<usize>, // indices into columns
    /// One entry per `UNIQUE` constraint (single or composite); indices
    /// into `columns`. Enforced by scanning the table on insert/update — no
    /// index acceleration yet.
    #[serde(default)]
    pub unique_groups: Vec<Vec<usize>>,
    #[serde(default)]
    pub foreign_keys: Vec<ForeignKey>,
    /// Canopy (Phase 10) JSON Schema validation rules, one per validated
    /// `Json` column, set by `CREATE TABLE ... WITH (json_schema_col = ...,
    /// json_schema = '<json schema text>', json_schema_mode = 'strict' |
    /// 'relaxed' | 'off')`. Empty for a table with no JSON Schema attached.
    #[serde(default)]
    pub json_schemas: Vec<JsonSchemaRule>,
}

/// One JSON Schema validation rule attached to a `Json` column.
/// `mode` is `"strict"` (reject on any violation), `"relaxed"` (only the
/// top-level `type` is checked; unknown/extra properties and nested
/// violations are tolerated), or `"off"` (rule is stored but never
/// evaluated — lets a table keep its schema on file while validation is
/// temporarily disabled).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonSchemaRule {
    pub column: String,
    pub mode: String,
    pub schema: String,
}

impl TableSchema {
    /// Serialize schema to bytes for persistent storage.
    pub fn encode(&self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(self)?)
    }

    /// Deserialize schema from bytes.
    pub fn decode(data: &[u8]) -> Result<Self> {
        Ok(bincode::deserialize(data)?)
    }
}

/// A named monotonic counter backing `SERIAL` columns and explicit
/// `CREATE SEQUENCE`/`nextval`/`setval`. `currval` is intentionally *not*
/// per-session here — it returns the sequence's current process-wide value,
/// a documented simplification (true per-session `currval` would need
/// session-scoped state threaded through query evaluation, which doesn't
/// exist today).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sequence {
    pub name: String,
    pub value: i64,
    pub increment: i64,
}

impl Sequence {
    pub fn encode(&self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(self)?)
    }
    pub fn decode(data: &[u8]) -> Result<Self> {
        Ok(bincode::deserialize(data)?)
    }
}

/// A registered WASM user-defined function. Only `Int8`/`Float8`/`Bool`
/// argument and return types are supported — see `executor::udf`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserFunction {
    pub name: String,
    pub arg_types: Vec<ColumnType>,
    pub return_type: ColumnType,
    pub wasm_bytes: Vec<u8>,
}

impl UserFunction {
    /// Serialize to bytes for persistent storage.
    pub fn encode(&self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(self)?)
    }

    /// Deserialize from bytes.
    pub fn decode(data: &[u8]) -> Result<Self> {
        Ok(bincode::deserialize(data)?)
    }
}

/// Storage statistics for monitoring.
#[derive(Debug, Clone, Default)]
pub struct StorageStats {
    pub wal_bytes_written: u64,
    pub memtable_entries: usize,
    pub sstable_count: usize,
    pub total_disk_bytes: u64,
}
