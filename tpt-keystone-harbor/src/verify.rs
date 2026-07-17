//! Verification Engine — per-row xxHash3 checksums plus a coarse
//! distribution check (row counts) between source and target. "Query
//! regression testing" (TODO.md) is not attempted: it would need a corpus
//! of representative source queries replayed against the target and a
//! judgment call on acceptable plan/latency drift, which is out of scope
//! for this pass — see the "Core engine + Harbor/PG only" scope decision.

use crate::connector::SourceRow;
use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::xxh3_64;

/// Canonical byte encoding a row hashes over: column count implicit in
/// call site, each cell as a length-prefixed byte string with a distinct
/// prefix for NULL, so `(NULL, "1")` and `("", NULL)` don't collide.
pub fn hash_row(cells: &[Option<Vec<u8>>]) -> u64 {
    let mut buf = Vec::new();
    for cell in cells {
        match cell {
            None => buf.push(0u8),
            Some(bytes) => {
                buf.push(1u8);
                buf.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
                buf.extend_from_slice(bytes);
            }
        }
    }
    xxh3_64(&buf)
}

pub fn hash_rows(rows: &[SourceRow]) -> Vec<u64> {
    rows.iter().map(|r| hash_row(r)).collect()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableVerification {
    pub table: String,
    pub source_row_count: u64,
    pub target_row_count: u64,
    pub mismatched_rows: u64,
    pub passed: bool,
}

/// Compare two per-row checksum sets. Row order must match on both sides
/// (both connectors' `row_checksums` order by primary key when one
/// exists) — a mismatch count above zero means either row content or row
/// count diverged; which one is visible in the returned counts.
pub fn verify_table(table: &str, source: &[u64], target: &[u64]) -> TableVerification {
    let n = source.len().min(target.len());
    let mismatched = (0..n).filter(|&i| source[i] != target[i]).count() as u64;
    let extra = (source.len() as i64 - target.len() as i64).unsigned_abs();
    let mismatched_rows = mismatched + extra;
    TableVerification {
        table: table.to_string(),
        source_row_count: source.len() as u64,
        target_row_count: target.len() as u64,
        mismatched_rows,
        passed: mismatched_rows == 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_rows_hash_equal() {
        let a: SourceRow = vec![Some(b"1".to_vec()), None, Some(b"hello".to_vec())];
        let b: SourceRow = vec![Some(b"1".to_vec()), None, Some(b"hello".to_vec())];
        assert_eq!(hash_row(&a), hash_row(&b));
    }

    #[test]
    fn null_vs_empty_string_do_not_collide() {
        let a: SourceRow = vec![None];
        let b: SourceRow = vec![Some(Vec::new())];
        assert_ne!(hash_row(&a), hash_row(&b));
    }

    #[test]
    fn verify_table_detects_row_count_mismatch() {
        let source = vec![1, 2, 3];
        let target = vec![1, 2];
        let result = verify_table("t", &source, &target);
        assert!(!result.passed);
        assert_eq!(result.mismatched_rows, 1);
    }

    #[test]
    fn verify_table_passes_on_match() {
        let source = vec![1, 2, 3];
        let target = vec![1, 2, 3];
        let result = verify_table("t", &source, &target);
        assert!(result.passed);
    }
}
