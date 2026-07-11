//! A bounded, shared cache of parsed statements keyed by raw SQL text.
//!
//! Every client connection shares one `Arc<Database>` already (see
//! `storage::database::Database`), so a per-connection statement cache
//! wouldn't buy anything a repeated query from a *different* connection
//! couldn't also benefit from — this cache lives on `Database` itself and
//! is shared by every session, letting a hot query's lex/parse cost be paid
//! once regardless of which connection asks for it next.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use super::ast::Stmt;

struct Entry {
    stmt: Stmt,
    last_access: u64,
}

/// An LRU-by-count cache (same recency-clock eviction shape as
/// `storage::cache::NvmeCache`, but bounded by entry count rather than
/// bytes, since a `Stmt` doesn't have a meaningful byte size to budget by).
pub struct StatementCache {
    max_entries: usize,
    entries: Mutex<HashMap<String, Entry>>,
    clock: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl StatementCache {
    pub fn new(max_entries: usize) -> Self {
        Self {
            max_entries,
            entries: Mutex::new(HashMap::new()),
            clock: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Parse `sql`, reusing a cached `Stmt` for identical SQL text when
    /// available. Cache misses fall through to `super::parse` and populate
    /// the cache; parse errors are never cached.
    pub fn parse(&self, sql: &str) -> anyhow::Result<Stmt> {
        {
            let mut entries = self.entries.lock().unwrap();
            if let Some(entry) = entries.get_mut(sql) {
                entry.last_access = self.clock.fetch_add(1, Ordering::Relaxed);
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Ok(entry.stmt.clone());
            }
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        let stmt = super::parse(sql)?;

        let mut entries = self.entries.lock().unwrap();
        let tick = self.clock.fetch_add(1, Ordering::Relaxed);
        entries.insert(
            sql.to_string(),
            Entry {
                stmt: stmt.clone(),
                last_access: tick,
            },
        );
        if entries.len() > self.max_entries {
            if let Some(victim) = entries
                .iter()
                .min_by_key(|(_, e)| e.last_access)
                .map(|(k, _)| k.clone())
            {
                entries.remove(&victim);
            }
        }
        Ok(stmt)
    }

    /// `(entry_count, hits, misses)` — for tests/observability.
    pub fn stats(&self) -> (usize, u64, u64) {
        let entries = self.entries.lock().unwrap();
        (
            entries.len(),
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
        )
    }
}
