//! Canopy (Phase 10) secondary indexes: path-based deep indexing over JSON
//! documents, and an inverted full-text index over the string fields within
//! them. Same scope cut as Meridian/Chronos/Plexus's spatial/time/graph
//! indexes: a local-only, node-rebuilt-from-disk accelerator layered on top
//! of the existing row-oriented LSM storage, not a replicated index format.
//!
//! Unlike the B-Tree in `btree.rs` (unique-key, one primary key per indexed
//! value — a pre-existing limitation of the plain secondary-index path),
//! both indexes here are genuinely multi-valued: many rows can share the
//! same path value or contain the same token, exactly the shape a JSON path
//! index / GIN-style inverted index needs. They're backed by an in-memory
//! `HashMap` (equality/token lookups don't benefit from a B-Tree's ordered
//! scan the way range queries do — the same reasoning real inverted/hash
//! indexes are built on) and persisted as a single `bincode`-encoded blob,
//! rewritten on every mutation. That "rewrite the whole file" persistence
//! strategy is the honest trade-off for keeping this module small; it does
//! not scale to huge indexes the way the append-only `sst`/`wal` formats do.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Walks a dot-separated path (`user.address.city`) through a JSON document.
/// Purely object-key traversal — array indices in the path aren't supported
/// (a documented scope cut; array *contents* are still reachable if you
/// index a path that ends before the array).
pub fn extract_path<'a>(doc: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = doc;
    for segment in path.split('.') {
        if segment.is_empty() {
            continue;
        }
        current = current.as_object()?.get(segment)?;
    }
    Some(current)
}

/// Canonical text form of a scalar JSON value for use as an index key.
/// Objects/arrays return `None` — only scalar leaves are indexed.
pub fn scalar_key_text(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(n.to_string()),
        Value::String(s) => Some(s.clone()),
        Value::Array(_) | Value::Object(_) => None,
    }
}

#[derive(Serialize, Deserialize, Default)]
struct PathIndexData {
    json_path: String,
    map: HashMap<String, Vec<Vec<u8>>>,
}

/// A path-based deep index: `CREATE INDEX ... USING JSONPATH ON t(col)
/// WITH (path = 'user.address.city')`. Maps the text form of the value found
/// at that path to every row key whose document has that value there.
pub struct JsonPathIndex {
    file_path: PathBuf,
    data: PathIndexData,
}

impl JsonPathIndex {
    pub fn create(file_path: &Path, json_path: &str) -> Self {
        Self { file_path: file_path.to_path_buf(), data: PathIndexData { json_path: json_path.to_string(), map: HashMap::new() } }
    }

    /// Open an existing index file, or fall back to an empty index keyed on
    /// `fallback_path` if the file doesn't exist yet (mirrors
    /// `TimeIndex::open`/`GraphIndex::open`'s header-driven reopen pattern).
    pub fn open(file_path: &Path, fallback_path: &str) -> Result<Self> {
        if file_path.exists() {
            let bytes = std::fs::read(file_path)?;
            let data: PathIndexData = bincode::deserialize(&bytes)?;
            Ok(Self { file_path: file_path.to_path_buf(), data })
        } else {
            Ok(Self::create(file_path, fallback_path))
        }
    }

    pub fn json_path(&self) -> &str {
        &self.data.json_path
    }

    /// Index one document's row. `doc_text` is the raw JSON text stored in
    /// the column; malformed JSON or a path that doesn't resolve to a scalar
    /// leaf is silently skipped (the row just isn't indexed, still visible
    /// via a full scan).
    pub fn insert(&mut self, row_key: &[u8], doc_text: &str) -> Result<()> {
        if let Ok(doc) = serde_json::from_str::<Value>(doc_text) {
            if let Some(key_text) = extract_path(&doc, &self.data.json_path).and_then(scalar_key_text) {
                let bucket = self.data.map.entry(key_text).or_default();
                if !bucket.iter().any(|k| k == row_key) {
                    bucket.push(row_key.to_vec());
                }
            }
        }
        self.save()
    }

    /// Row keys whose document has `value_text` at the indexed path.
    pub fn lookup(&self, value_text: &str) -> Vec<Vec<u8>> {
        self.data.map.get(value_text).cloned().unwrap_or_default()
    }

    fn save(&self) -> Result<()> {
        let bytes = bincode::serialize(&self.data)?;
        std::fs::write(&self.file_path, bytes)?;
        Ok(())
    }
}

/// Lowercase, alphanumeric-run tokenizer shared by indexing and querying —
/// deliberately simple (no stemming, no stop-word list) so a token always
/// means the same thing on both sides.
pub fn tokenize(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_lowercase())
        .collect()
}

/// Recursively collects every string leaf value in a JSON document into one
/// space-joined blob for tokenization — an object's/array's structure isn't
/// searchable, just the text it contains.
pub fn collect_json_strings(v: &Value, out: &mut String) {
    match v {
        Value::String(s) => {
            if !out.is_empty() {
                out.push(' ');
            }
            out.push_str(s);
        }
        Value::Array(items) => items.iter().for_each(|i| collect_json_strings(i, out)),
        Value::Object(map) => map.values().for_each(|i| collect_json_strings(i, out)),
        _ => {}
    }
}

/// Term-frequency postings: token → (row key → number of times the token
/// occurs in that row's indexed text). Replaces the old presence-only
/// `Vec<Vec<u8>>` bucket so real BM25 scoring (which needs `f(t,D)`, not
/// just "does D contain t") has something to score against.
#[derive(Serialize, Deserialize, Default)]
struct FtsIndexData {
    postings: HashMap<String, HashMap<Vec<u8>, u32>>,
    /// Total token count per row, for BM25's document-length normalization
    /// (`|D|` and `avgdl` below).
    doc_lengths: HashMap<Vec<u8>, u32>,
}

/// An inverted full-text index (`CREATE INDEX ... USING GIN` / `USING FTS`)
/// over a `Json` or `Text` column: token → every row key whose column value
/// contains that token, with per-row term frequency and document length so
/// both boolean AND search and ranked BM25 search can be answered from the
/// same structure.
pub struct FtsIndex {
    file_path: PathBuf,
    data: FtsIndexData,
}

/// BM25 free parameters at their standard textbook defaults (Robertson/Zaragoza).
/// `k1` controls term-frequency saturation, `b` controls document-length
/// normalization strength; not exposed as tunables — same "one obvious
/// choice, not a knob nobody will turn" precedent as this codebase's other
/// fixed algorithm constants (e.g. Chronos's Gorilla codec has no tunables
/// either).
const BM25_K1: f64 = 1.2;
const BM25_B: f64 = 0.75;

impl FtsIndex {
    pub fn create(file_path: &Path) -> Self {
        Self { file_path: file_path.to_path_buf(), data: FtsIndexData::default() }
    }

    pub fn open(file_path: &Path) -> Result<Self> {
        if file_path.exists() {
            let bytes = std::fs::read(file_path)?;
            let data: FtsIndexData = bincode::deserialize(&bytes)?;
            Ok(Self { file_path: file_path.to_path_buf(), data })
        } else {
            Ok(Self::create(file_path))
        }
    }

    /// Index one row's raw column text (JSON document text or plain text —
    /// callers decide which; JSON documents should be pre-flattened via
    /// `collect_json_strings`). Re-indexing the same `row_key` (e.g. after an
    /// `UPDATE`) first clears its old postings/length so term frequencies
    /// don't double-count — the same "no stale entry" caveat this index's
    /// module docs already state for path indexing.
    pub fn insert(&mut self, row_key: &[u8], text: &str) -> Result<()> {
        self.remove(row_key);
        let tokens = tokenize(text);
        self.data.doc_lengths.insert(row_key.to_vec(), tokens.len() as u32);
        for token in tokens {
            let bucket = self.data.postings.entry(token).or_default();
            *bucket.entry(row_key.to_vec()).or_insert(0) += 1;
        }
        self.save()
    }

    /// Removes any existing postings/length for `row_key` without persisting
    /// (the caller — `insert` — saves once afterward).
    fn remove(&mut self, row_key: &[u8]) {
        if self.data.doc_lengths.remove(row_key).is_some() {
            for bucket in self.data.postings.values_mut() {
                bucket.remove(row_key);
            }
        }
    }

    /// Row keys whose indexed text contains every token in `query` (AND
    /// semantics — the common case for a search box; ranked search is
    /// `search_bm25` below).
    pub fn search_and(&self, query: &str) -> Vec<Vec<u8>> {
        let tokens = tokenize(query);
        if tokens.is_empty() {
            return Vec::new();
        }
        let mut iter = tokens.iter();
        let first = iter.next().unwrap();
        let mut result: Vec<Vec<u8>> = self.data.postings.get(first)
            .map(|m| m.keys().cloned().collect()).unwrap_or_default();
        for token in iter {
            let bucket = self.data.postings.get(token);
            result.retain(|k| bucket.is_some_and(|b| b.contains_key(k)));
        }
        result
    }

    /// Ranks every row containing at least one query term by Okapi BM25
    /// score (Robertson/Zaragoza, standard `k1`/`b` defaults above),
    /// descending, truncated to `limit`. OR semantics (unlike `search_and`):
    /// a document doesn't need every query term, just a nonzero score.
    pub fn search_bm25(&self, query: &str, limit: usize) -> Vec<(Vec<u8>, f64)> {
        let tokens = tokenize(query);
        if tokens.is_empty() || self.data.doc_lengths.is_empty() {
            return Vec::new();
        }
        let n = self.data.doc_lengths.len() as f64;
        let avgdl = self.data.doc_lengths.values().map(|&l| l as f64).sum::<f64>() / n;

        let mut scores: HashMap<Vec<u8>, f64> = HashMap::new();
        for token in &tokens {
            let Some(bucket) = self.data.postings.get(token) else { continue };
            let n_t = bucket.len() as f64;
            // BM25 IDF (the "+1" form keeps it non-negative even when a term
            // appears in more than half the corpus).
            let idf = ((n - n_t + 0.5) / (n_t + 0.5) + 1.0).ln();
            for (row_key, &tf) in bucket {
                let doc_len = *self.data.doc_lengths.get(row_key).unwrap_or(&0) as f64;
                let tf = tf as f64;
                let denom = tf + BM25_K1 * (1.0 - BM25_B + BM25_B * doc_len / avgdl);
                let term_score = idf * (tf * (BM25_K1 + 1.0)) / denom;
                *scores.entry(row_key.clone()).or_insert(0.0) += term_score;
            }
        }

        let mut ranked: Vec<(Vec<u8>, f64)> = scores.into_iter().collect();
        ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        ranked.truncate(limit);
        ranked
    }

    fn save(&self) -> Result<()> {
        let bytes = bincode::serialize(&self.data)?;
        std::fs::write(&self.file_path, bytes)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_index_extracts_nested_scalar() {
        let doc: Value = serde_json::from_str(r#"{"user":{"address":{"city":"Wellington"}}}"#).unwrap();
        let v = extract_path(&doc, "user.address.city").unwrap();
        assert_eq!(scalar_key_text(v).unwrap(), "Wellington");
    }

    #[test]
    fn path_index_roundtrips_through_disk() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("docs_data.jsonpath");
        {
            let mut idx = JsonPathIndex::create(&file, "user.address.city");
            idx.insert(b"row1", r#"{"user":{"address":{"city":"Wellington"}}}"#).unwrap();
            idx.insert(b"row2", r#"{"user":{"address":{"city":"Auckland"}}}"#).unwrap();
            idx.insert(b"row3", r#"{"user":{"address":{"city":"Wellington"}}}"#).unwrap();
        }
        let idx = JsonPathIndex::open(&file, "").unwrap();
        assert_eq!(idx.json_path(), "user.address.city");
        let mut hits = idx.lookup("Wellington");
        hits.sort();
        assert_eq!(hits, vec![b"row1".to_vec(), b"row3".to_vec()]);
        assert_eq!(idx.lookup("Auckland"), vec![b"row2".to_vec()]);
        assert!(idx.lookup("Nowhere").is_empty());
    }

    #[test]
    fn fts_index_and_search() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("docs_data.fts");
        let mut idx = FtsIndex::create(&file);
        idx.insert(b"row1", "the quick brown fox").unwrap();
        idx.insert(b"row2", "the lazy dog").unwrap();
        idx.insert(b"row3", "quick lazy fox").unwrap();

        let mut hits = idx.search_and("quick fox");
        hits.sort();
        assert_eq!(hits, vec![b"row1".to_vec(), b"row3".to_vec()]);

        assert_eq!(idx.search_and("lazy dog"), vec![b"row2".to_vec()]);
        assert!(idx.search_and("nonexistent").is_empty());
    }

    #[test]
    fn collect_json_strings_flattens_nested_text() {
        let doc: Value = serde_json::from_str(r#"{"title":"Hello","tags":["fox","dog"]}"#).unwrap();
        let mut out = String::new();
        collect_json_strings(&doc, &mut out);
        for word in ["Hello", "fox", "dog"] {
            assert!(out.contains(word));
        }
    }
}
