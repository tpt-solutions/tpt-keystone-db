//! Synchronous wrapper over [`super::KeystoneClient`] — the "Synchronous
//! and async APIs" checklist item. Owns a single-threaded Tokio runtime and
//! blocks the calling thread on it, so it's suitable for a plain
//! non-async Tauri/GTK callback or CLI tool that never touches Tokio
//! directly.

use super::{KeystoneClient, KeystoneError, QueryResult, Value};

pub struct Client {
    inner: KeystoneClient,
    rt: tokio::runtime::Runtime,
}

impl Client {
    pub fn connect(addr: &str) -> Result<Self, KeystoneError> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .expect("failed to start SDK runtime");
        let inner = rt.block_on(KeystoneClient::connect(addr))?;
        Ok(Self { inner, rt })
    }

    pub fn query(&mut self, sql: &str) -> Result<QueryResult, KeystoneError> {
        self.rt.block_on(self.inner.query(sql))
    }

    pub fn query_params(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, KeystoneError> {
        self.rt.block_on(self.inner.query_params(sql, params))
    }

    pub fn copy_in(
        &mut self,
        table: &str,
        columns: &[&str],
        rows: &[Vec<Value>],
    ) -> Result<u64, KeystoneError> {
        self.rt.block_on(self.inner.copy_in(table, columns, rows))
    }
}
