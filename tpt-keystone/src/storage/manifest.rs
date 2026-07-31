//! The shared source of truth for which SSTables/WAL segments currently make
//! up the database, stored as a single object (`manifest.bin`) in the
//! object store. Every compute node — writer or reader — reads this to know
//! what exists; the writer updates it via compare-and-swap after each flush
//! so readers polling it always see a consistent, monotonically-advancing
//! view (this is what makes "two compute nodes share one bucket, queries
//! return consistent results" true rather than aspirational).

use super::objectstore::{CasError, ObjectStore};
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Manifest {
    /// IDs of all live (flushed) SSTables, oldest first.
    pub sstable_ids: Vec<u64>,
    /// Highest WAL segment ID that has been sealed and shipped to the store.
    pub wal_segment_seq: u64,
    /// Fencing token of the writer that produced this manifest revision.
    pub writer_fencing_token: u64,
    /// Highest MVCC commit-sequence number durably reflected by this
    /// manifest revision (i.e. by the SSTables it lists). See `ManifestV0`
    /// for how a manifest written before this field existed still decodes.
    #[serde(default)]
    pub max_commit_seq: u64,
}

/// The pre-`max_commit_seq` on-disk shape, kept only so [`Manifest::load`]
/// can still decode a manifest written before that field existed.
///
/// `#[serde(default)]` on `Manifest::max_commit_seq` does *not* by itself
/// make this work: bincode is a positional, non-self-describing format, so
/// decoding a shorter old blob directly as the current (longer) `Manifest`
/// doesn't gracefully default the missing field the way a self-describing
/// format (JSON, etc.) would — the derived decoder tries to read bytes for
/// every field in order, and hits a clean EOF partway through the field that
/// was never written, rather than backfilling it. `load` tries the current
/// shape first (the common case) and only falls back to this shape — which
/// has no trailing field to run out of bytes on — if that fails.
///
/// This same limitation applies to `TableSchema`'s `unique_groups`/
/// `foreign_keys`/`json_schemas` fields (`storage/mod.rs`), which are not
/// actually protected by their `#[serde(default)]` annotations either; this
/// type's doc comment is the record of that finding, since fixing that path
/// too is out of scope for the MVCC work this manifest field was added for.
#[derive(Debug, Clone, Deserialize)]
struct ManifestV0 {
    sstable_ids: Vec<u64>,
    wal_segment_seq: u64,
    writer_fencing_token: u64,
}

impl From<ManifestV0> for Manifest {
    fn from(v0: ManifestV0) -> Self {
        Manifest {
            sstable_ids: v0.sstable_ids,
            wal_segment_seq: v0.wal_segment_seq,
            writer_fencing_token: v0.writer_fencing_token,
            max_commit_seq: 0,
        }
    }
}

impl Manifest {
    const KEY: &'static str = "manifest.bin";

    /// Load the current manifest and its ETag, or `None` if the database has
    /// never flushed anything yet. Falls back to decoding the pre-
    /// `max_commit_seq` shape (see [`ManifestV0`]) if the current shape
    /// doesn't decode, so a data directory last written by an older binary
    /// still opens.
    pub fn load(store: &dyn ObjectStore) -> Result<Option<(Manifest, String)>> {
        match store.get(Self::KEY)? {
            Some((bytes, meta)) => {
                let manifest = bincode::deserialize::<Manifest>(&bytes)
                    .or_else(|_| bincode::deserialize::<ManifestV0>(&bytes).map(Manifest::from))?;
                Ok(Some((manifest, meta.etag)))
            }
            None => Ok(None),
        }
    }

    /// Attempt to write a new manifest revision, only if `expected_etag`
    /// still matches the store's current state (`None` = manifest must not
    /// exist yet). Returns the new ETag on success.
    pub fn save_cas(
        store: &dyn ObjectStore,
        manifest: &Manifest,
        expected_etag: Option<&str>,
    ) -> Result<String, CasError> {
        let bytes = bincode::serialize(manifest).map_err(|e| CasError::Other(e.into()))?;
        let meta = store.put_if_match(Self::KEY, &bytes, expected_etag)?;
        Ok(meta.etag)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::objectstore::LocalFsObjectStore;

    #[test]
    fn load_missing_is_none() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsObjectStore::open(dir.path()).unwrap();
        assert!(Manifest::load(&store).unwrap().is_none());
    }

    #[test]
    fn save_cas_then_reload_roundtrips() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsObjectStore::open(dir.path()).unwrap();
        let m = Manifest {
            sstable_ids: vec![1, 2, 3],
            wal_segment_seq: 5,
            writer_fencing_token: 1,
            max_commit_seq: 42,
        };
        let etag = Manifest::save_cas(&store, &m, None).unwrap();

        let (loaded, loaded_etag) = Manifest::load(&store).unwrap().unwrap();
        assert_eq!(loaded.sstable_ids, vec![1, 2, 3]);
        assert_eq!(loaded_etag, etag);

        // Stale etag is rejected.
        let m2 = Manifest {
            sstable_ids: vec![1, 2, 3, 4],
            ..m.clone()
        };
        let err = Manifest::save_cas(&store, &m2, None).unwrap_err();
        assert!(matches!(err, CasError::Conflict { .. }));

        // Correct etag succeeds.
        Manifest::save_cas(&store, &m2, Some(&etag)).unwrap();
        let (loaded2, _) = Manifest::load(&store).unwrap().unwrap();
        assert_eq!(loaded2.sstable_ids, vec![1, 2, 3, 4]);
    }

    /// A manifest encoded before `max_commit_seq` existed must still load
    /// through `Manifest::load` (via the `ManifestV0` fallback) — proving
    /// the fallback actually engages against a real stored object, not just
    /// that a raw `bincode::deserialize::<Manifest>` call would (it
    /// wouldn't: see `ManifestV0`'s doc comment for why plain
    /// `#[serde(default)]` doesn't help bincode with a missing trailing
    /// field, which is exactly what this test caught when first written).
    #[test]
    fn decodes_a_pre_max_commit_seq_manifest_with_the_default() {
        #[derive(Serialize)]
        struct OldManifest {
            sstable_ids: Vec<u64>,
            wal_segment_seq: u64,
            writer_fencing_token: u64,
        }
        let old = OldManifest {
            sstable_ids: vec![7, 8],
            wal_segment_seq: 3,
            writer_fencing_token: 2,
        };
        let bytes = bincode::serialize(&old).unwrap();

        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsObjectStore::open(dir.path()).unwrap();
        store.put(Manifest::KEY, &bytes).unwrap();

        let (decoded, _etag) = Manifest::load(&store).unwrap().unwrap();
        assert_eq!(decoded.sstable_ids, vec![7, 8]);
        assert_eq!(decoded.wal_segment_seq, 3);
        assert_eq!(decoded.writer_fencing_token, 2);
        assert_eq!(decoded.max_commit_seq, 0, "missing field must default to 0");
    }
}
