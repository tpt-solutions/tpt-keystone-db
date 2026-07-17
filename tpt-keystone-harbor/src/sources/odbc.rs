//! Harbor/ODBC — generic ODBC source connector. Unlike this crate's other
//! source connectors, which each hand-write one vendor's own wire protocol
//! (TDS for MSSQL, TNS for Oracle, Bolt for Neo4j, ...), ODBC is a
//! vendor-neutral C API: the OS driver manager (unixODBC / Windows odbc32)
//! plus whatever vendor driver is installed at runtime speak the real wire
//! protocol opaquely on our behalf. There is no single publicly documented
//! protocol here to reimplement, so wrapping `odbc-api` (a safe binding over
//! the ODBC C API) doesn't conflict with this crate's "no pgwire-style
//! crate" rule — see the Cargo.toml comment on the `odbc-api` dependency.
//! This connector's value is covering whatever database doesn't have (or
//! doesn't need) its own hand-written connector here: DB2, Sybase, Teradata,
//! Snowflake, and any other ODBC-capable data source.
//!
//! Discovery uses the standard ODBC catalog functions — `SQLTables`,
//! `SQLColumns`, `SQLPrimaryKeys` — exposed by `odbc-api` as
//! `Connection::tables`/`columns`/`primary_keys`. Column types come back as
//! a numeric `SQL_*` type code (see [`crate::schema::from_odbc_sql_type`]),
//! not a vendor type-name string like this module's other connectors get.
//! Snapshot and checksums run `SELECT * FROM <schema>.<table>` and walk the
//! cursor row-by-row via `CursorRow::get_text` — this works generically
//! across any driver, but binding typed columnar buffers per-column (which
//! would need runtime type dispatch) would be faster; out of scope for a
//! first generic pass. CDC is scope-cut: there is no cross-vendor standard
//! change-feed API over ODBC.
//!
//! `odbc-api` is a blocking C API, unlike this crate's other connectors
//! (which use native async I/O over `TcpStream`). Rather than keeping one
//! long-lived connection alive across `.await` points (which would require
//! a dedicated actor-thread + channel-RPC design to work around ODBC
//! handles not being `Send`), every method here opens its own
//! `Environment`/`Connection` for that one call inside a single
//! `tokio::task::spawn_blocking` closure. Reconnecting per call is wasted
//! work under heavy use, but this connector isn't a hot loop — discover
//! runs once, snapshot/checksums run once per table.
//!
//! **Confidence note:** this has not been run against a real ODBC driver/DSN
//! (none is installed in this environment). Treat this as a structural
//! implementation following the standard ODBC catalog/cursor APIs, not as
//! validated against a live driver — same caveat this crate already carries
//! for `sources/oracle.rs`'s TTC layer.

use crate::connector::{ChangeEvent, ConnectorError, SourceConnector, SourceRow};
use crate::schema::{from_odbc_sql_type, ColumnSchema, TableSchema};
use anyhow::{Context, Result};
use async_trait::async_trait;
use odbc_api::{Connection, ConnectionOptions, Cursor, Environment, ResultSetMetadata};
use std::collections::HashSet;
use tokio::sync::mpsc::Sender;

const SNAPSHOT_BATCH_SIZE: usize = 5_000;

/// Opens a fresh `Environment`/`Connection` for the duration of `f`, then
/// drops both. `Connection<'env>` borrows its `Environment`, so both must
/// live in the same stack frame; `f`'s return value must be owned data (not
/// borrow the connection) to escape this function.
fn with_connection<T>(conn_str: &str, f: impl FnOnce(&Connection<'_>) -> Result<T>) -> Result<T> {
    let env = Environment::new().context("initializing ODBC environment")?;
    let conn = env
        .connect_with_connection_string(conn_str, ConnectionOptions::default())
        .context("connecting via ODBC connection string")?;
    f(&conn)
}

/// Reads every column of the cursor's current row as text (`None` = SQL
/// NULL), producing a [`SourceRow`].
fn fetch_row_as_cells(row: &mut odbc_api::CursorRow<'_>, num_cols: i16) -> Result<SourceRow> {
    let mut cells = Vec::with_capacity(num_cols.max(0) as usize);
    for col in 1..=num_cols as u16 {
        let mut buf = Vec::new();
        let has_value = row.get_text(col, &mut buf).context("fetching column value")?;
        cells.push(if has_value { Some(buf) } else { None });
    }
    Ok(cells)
}

pub struct OdbcSource {
    conn_str: String,
}

impl OdbcSource {
    pub async fn connect(conn_str: &str) -> Result<Self> {
        let conn_str = conn_str.to_string();
        let probe = conn_str.clone();
        tokio::task::spawn_blocking(move || with_connection(&probe, |_conn| Ok(())))
            .await
            .context("ODBC connect task panicked")??;
        Ok(Self { conn_str })
    }
}

#[async_trait]
impl SourceConnector for OdbcSource {
    fn name(&self) -> &'static str {
        "Harbor/ODBC"
    }

    async fn discover(&mut self) -> Result<Vec<TableSchema>, ConnectorError> {
        let conn_str = self.conn_str.clone();
        let tables = tokio::task::spawn_blocking(move || -> Result<Vec<TableSchema>> {
            with_connection(&conn_str, |conn| {
                let mut tables = Vec::new();

                for table_row in conn.tables("", "", "", "TABLE").context("SQLTables")? {
                    let table_row = table_row.context("reading SQLTables row")?;
                    let schema_name = table_row.schema.as_str().ok().flatten().unwrap_or("").to_string();
                    let table_name = table_row.table.as_str().ok().flatten().unwrap_or("").to_string();
                    if table_name.is_empty() {
                        continue;
                    }

                    let pk_cols: HashSet<String> = conn
                        .primary_keys(None, Some(schema_name.as_str()), &table_name)
                        .context("SQLPrimaryKeys")?
                        .filter_map(|r| r.ok())
                        .filter_map(|r| r.column.as_str().ok().flatten().map(str::to_string))
                        .collect();

                    let mut columns = Vec::new();
                    for col_row in conn
                        .columns("", schema_name.as_str(), table_name.as_str(), "")
                        .context("SQLColumns")?
                    {
                        let col_row = col_row.context("reading SQLColumns row")?;
                        let name = col_row.column_name.as_str().ok().flatten().unwrap_or("").to_string();
                        let source_type = col_row.type_name.as_str().ok().flatten().unwrap_or("").to_string();
                        // SQLColumns' NULLABLE: 0 = SQL_NO_NULLS, 1 = SQL_NULLABLE, 2 = unknown.
                        // Treat "unknown" as nullable (permissive default).
                        let nullable = col_row.nullable != 0;
                        let is_primary_key = pk_cols.contains(&name);
                        columns.push(ColumnSchema {
                            name,
                            source_type,
                            keystone_type: from_odbc_sql_type(col_row.data_type),
                            nullable,
                            is_primary_key,
                        });
                    }

                    tables.push(TableSchema { schema: schema_name, name: table_name, columns });
                }

                Ok(tables)
            })
        })
        .await
        .context("ODBC discover task panicked")??;

        Ok(tables)
    }

    async fn snapshot_table(&mut self, table: &TableSchema, tx: Sender<Vec<SourceRow>>) -> Result<u64, ConnectorError> {
        let conn_str = self.conn_str.clone();
        let query = format!("SELECT * FROM {}", table.qualified_name());

        let total = tokio::task::spawn_blocking(move || -> Result<u64> {
            with_connection(&conn_str, |conn| {
                let mut cursor = conn
                    .execute(&query, (), None)
                    .context("executing snapshot SELECT")?
                    .context("snapshot SELECT produced no result set")?;
                let num_cols = cursor.num_result_cols().context("num_result_cols")?;

                let mut total: u64 = 0;
                let mut batch = Vec::with_capacity(SNAPSHOT_BATCH_SIZE);
                while let Some(mut row) = cursor.next_row().context("fetching row")? {
                    batch.push(fetch_row_as_cells(&mut row, num_cols)?);
                    total += 1;
                    if batch.len() >= SNAPSHOT_BATCH_SIZE {
                        if tx.blocking_send(std::mem::replace(&mut batch, Vec::with_capacity(SNAPSHOT_BATCH_SIZE))).is_err() {
                            return Ok(total);
                        }
                    }
                }
                if !batch.is_empty() {
                    let _ = tx.blocking_send(batch);
                }
                Ok(total)
            })
        })
        .await
        .context("ODBC snapshot task panicked")??;

        Ok(total)
    }

    async fn replicate(&mut self, _tables: &[TableSchema], _resume_token: Option<String>, _tx: Sender<ChangeEvent>) -> Result<(), ConnectorError> {
        Err(ConnectorError::Unimplemented {
            connector: "Harbor/ODBC",
            detail: "generic ODBC has no standard cross-vendor CDC/change-feed API",
        })
    }

    async fn row_checksums(&mut self, table: &TableSchema) -> Result<Vec<u64>, ConnectorError> {
        let conn_str = self.conn_str.clone();
        let pk_cols = table.primary_key_columns();
        let query = if pk_cols.is_empty() {
            format!("SELECT * FROM {}", table.qualified_name())
        } else {
            format!("SELECT * FROM {} ORDER BY {}", table.qualified_name(), pk_cols.join(", "))
        };

        let checksums = tokio::task::spawn_blocking(move || -> Result<Vec<u64>> {
            with_connection(&conn_str, |conn| {
                let mut cursor = conn
                    .execute(&query, (), None)
                    .context("executing checksum SELECT")?
                    .context("checksum SELECT produced no result set")?;
                let num_cols = cursor.num_result_cols().context("num_result_cols")?;

                let mut out = Vec::new();
                while let Some(mut row) = cursor.next_row().context("fetching row")? {
                    let cells = fetch_row_as_cells(&mut row, num_cols)?;
                    out.push(crate::verify::hash_row(&cells));
                }
                Ok(out)
            })
        })
        .await
        .context("ODBC checksum task panicked")??;

        Ok(checksums)
    }
}
