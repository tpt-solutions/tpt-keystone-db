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
    /// manifest revision (i.e. by the SSTables it lists). `#[serde(default)]`
    /// so a manifest written before this field existed still decodes (as
    /// `0`, the same "nothing committed yet" sentinel a fresh database
    /// starts with) — same precedent as `TableSchema`'s `unique_groups`/
    /// `foreign_keys`/`json_schemas` fields.
    #[serde(default)]
    pub max_commit_seq: u64,
}

impl Manifest {
    const KEY: &'static str = "manifest.bin";

    /// Load the current manifest and its ETag, or `None` if the database has
    /// never flushed anything yet.
    pub fn load(store: &dyn ObjectStore) -> Result<Option<(Manifest, String)>> {
        match store.get(Self::KEY)? {
            Some((bytes, meta)) => Ok(Some((bincode::deserialize(&bytes)?, meta.etag))),
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

    /// A manifest encoded before `max_commit_seq` existed must still decode
    /// into the current `Manifest` — this is what actually proves bincode's
    /// positional (non-self-describing) format honors `#[serde(default)]`
    /// for a missing *trailing* field, rather than erroring on the shorter
    /// input, since that's not exercised by round-tripping the current shape
    /// through itself.
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

        let decoded: Manifest = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.sstable_ids, vec![7, 8]);
        assert_eq!(decoded.wal_segment_seq, 3);
        assert_eq!(decoded.writer_fencing_token, 2);
        assert_eq!(decoded.max_commit_seq, 0, "missing field must default to 0");
    }
}
