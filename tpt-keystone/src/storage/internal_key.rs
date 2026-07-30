//! `InternalKey` — a user key tagged with an MVCC commit-sequence number and
//! a put/delete tag, used as the sort key for the MemTable and the SSTable
//! index (`storage::lsm`, `storage::sstable`) so multiple versions of the
//! same user key sort together, newest first, without disturbing the
//! relative ordering of *distinct* user keys.
//!
//! Ordering compares `user_key` in isolation first — never as part of a
//! concatenated byte blob — because this repo's primary keys are decimal
//! text (e.g. `"orders\01"` vs `"orders\010"`), and one user key can be a
//! byte-prefix of another. Concatenating a version suffix onto raw key bytes
//! and comparing the result as one blob can flip that relative ordering
//! depending on the suffix bytes that happen to follow; comparing the two
//! parts separately sidesteps the issue entirely. Only when two `user_key`s
//! are equal does `seq` (higher sorts first, i.e. newest-first within a key)
//! break the tie.

use std::cmp::Ordering;

pub const PUT_TAG: u8 = 0;
pub const DELETE_TAG: u8 = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalKey {
    pub user_key: Vec<u8>,
    pub seq: u64,
    pub tag: u8,
}

impl InternalKey {
    pub fn new(user_key: Vec<u8>, seq: u64, tag: u8) -> Self {
        Self {
            user_key,
            seq,
            tag,
        }
    }

    /// The seek key for "the newest version of `user_key` visible to
    /// `snapshot_seq`": the first stored key `>=` this one (per this type's
    /// `Ord`) is exactly that version, provided its `user_key` matches —
    /// every version of `user_key` newer than `snapshot_seq` sorts strictly
    /// before this seek key, so a `range(seek..)` scan skips them for free.
    pub fn seek(user_key: &[u8], snapshot_seq: u64) -> Self {
        Self {
            user_key: user_key.to_vec(),
            seq: snapshot_seq,
            tag: PUT_TAG,
        }
    }

    pub fn is_delete(&self) -> bool {
        self.tag == DELETE_TAG
    }
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.user_key
            .cmp(&other.user_key)
            .then_with(|| other.seq.cmp(&self.seq)) // higher seq sorts first
            .then_with(|| self.tag.cmp(&other.tag))
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn orders_by_user_key_first_regardless_of_seq() {
        // "orders\x001" is a byte-prefix of "orders\x0010" — a naive
        // concatenated-byte-key encoding could flip this ordering depending
        // on the version suffix; comparing `user_key` in isolation must not.
        let a = InternalKey::new(b"orders\x001".to_vec(), 100, PUT_TAG);
        let b = InternalKey::new(b"orders\x0010".to_vec(), 1, PUT_TAG);
        assert_eq!(
            a.cmp(&b),
            Ordering::Less,
            "\"...1\" must sort before \"...10\" regardless of seq"
        );
    }

    #[test]
    fn newest_seq_sorts_first_within_the_same_key() {
        let newer = InternalKey::new(b"k".to_vec(), 100, PUT_TAG);
        let older = InternalKey::new(b"k".to_vec(), 50, PUT_TAG);
        assert!(newer < older);
    }

    #[test]
    fn seek_lands_on_first_entry_with_seq_at_or_below_snapshot() {
        use std::collections::BTreeSet;
        let mut set = BTreeSet::new();
        set.insert(InternalKey::new(b"k".to_vec(), 100, PUT_TAG));
        set.insert(InternalKey::new(b"k".to_vec(), 90, PUT_TAG));
        set.insert(InternalKey::new(b"k".to_vec(), 50, PUT_TAG));

        let seek = InternalKey::seek(b"k", 95);
        let hit = set.range(seek..).next().unwrap();
        assert_eq!(
            hit.seq, 90,
            "seek at snapshot 95 must land on seq 90, not the newer seq 100"
        );
    }

    #[test]
    fn seek_skips_a_key_with_no_visible_version() {
        use std::collections::BTreeSet;
        let mut set = BTreeSet::new();
        set.insert(InternalKey::new(b"k".to_vec(), 100, PUT_TAG));
        set.insert(InternalKey::new(b"z".to_vec(), 1, PUT_TAG));

        // Every version of "k" is newer than the snapshot; the seek must
        // land past all of them, on the next distinct key ("z"), not on "k".
        let seek = InternalKey::seek(b"k", 5);
        let hit = set.range(seek..).next().unwrap();
        assert_eq!(hit.user_key, b"z");
    }
}
