pub mod btree;
pub mod cache;
pub mod config;
pub mod database;
pub mod lease;
pub mod lsm;
pub mod manifest;
pub mod mvcc;
pub mod objectstore;
#[cfg(test)]
mod phase3_tests;
pub mod sstable;
pub mod tx;
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
            _ => None,
        }
    }
}

/// Schema for a single table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub pk_columns: Vec<usize>, // indices into columns
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

/// Storage statistics for monitoring.
#[derive(Debug, Clone, Default)]
pub struct StorageStats {
    pub wal_bytes_written: u64,
    pub memtable_entries: usize,
    pub sstable_count: usize,
    pub total_disk_bytes: u64,
}