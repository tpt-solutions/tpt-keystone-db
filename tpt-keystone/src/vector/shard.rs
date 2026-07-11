//! Consistent hashing for distributed vector shards (Prism roadmap item).
//!
//! Both `VectorIndex` (HNSW) and `IvfPqStorageIndex` (IVF-PQ) are local/
//! single-node structures — every node that opens one replays the *entire*
//! on-disk log into memory, so today a vector column's index is duplicated
//! in full on every node, not partitioned across them (see those modules'
//! own doc comments). This module provides the routing primitive a real
//! partitioned deployment needs: a stable, near-uniform mapping from a row
//! key to one of N logical shards, using the standard "hash ring + virtual
//! nodes" scheme (Karger et al., as used by Dynamo/Cassandra), so that:
//!
//! - row placement is deterministic and independent of insertion order,
//! - the row keys are spread roughly evenly across shards even for a small
//!   number of shards (virtual nodes smooth out an unlucky hash draw a
//!   single point per shard would otherwise produce), and
//! - adding or removing a shard only remaps the fraction of keys that
//!   *must* move (~1/N of the keyspace), not the whole ring — the property
//!   that makes consistent hashing worth using over plain `hash(key) % n`.
//!
//! `ShardedVectorIndex` (`storage::sharded_vector_index`) wires this ring
//! into a set of per-shard `VectorIndex` (HNSW) logs and is the actual
//! sharded index a `CREATE INDEX ... USING VECTOR` column could use; this
//! module is deliberately storage-agnostic so the same ring can also route
//! `IvfPqStorageIndex` shards or, beyond vectors entirely, any other
//! row-keyed partitioning this engine grows later.
//!
//! Scope cut, same honesty policy as every other "local-only" index in this
//! codebase: this is the *routing* primitive, not a distributed deployment.
//! There is no cross-node RPC anywhere in this engine (the cloud-native
//! model shares state through the object store, not node-to-node calls —
//! see `storage::objectstore`/`storage::manifest`), so today all shards a
//! ring produces are opened and queried in-process on one node, scattered
//! and gathered locally (`ShardedVectorIndex::query_knn`). The ring and the
//! per-shard on-disk layout are exactly what a future multi-writer
//! partitioned deployment would need — assigning each shard's log file to a
//! different node/prefix — but that assignment/ownership layer doesn't
//! exist yet.

use std::collections::BTreeMap;

/// Number of virtual nodes placed on the ring per real shard. Higher values
/// give a more even key distribution at the cost of a slightly larger ring
/// to search; 128 is the same order of magnitude Dynamo/Cassandra use in
/// practice and is small enough that `shard_for` (an O(log ring_size)
/// binary search) stays cheap even for a few hundred shards.
const VIRTUAL_NODES_PER_SHARD: u32 = 128;

fn hash64(bytes: &[u8]) -> u64 {
    // FNV-1a — same simple, dependency-free hash `executor::catalog`'s
    // `synthetic_oid` already uses elsewhere in this codebase, widened to
    // 64 bits for a bigger ring keyspace — then run through a
    // splitmix64-style finalizer (`fmix64`). Plain FNV-1a has weak
    // avalanche on short, near-identical keys (exactly the shape of the
    // virtual-node labels below, `shard-N-vn-M`): two inputs differing by
    // one digit can land close together on the ring instead of scattering,
    // which visibly skewed shard sizes before this finalizer was added
    // (`distributes_keys_roughly_evenly` caught it — one shard got 3100 of
    // 10000 keys, another got 200, against an even 1250 target).
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in bytes {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    fmix64(hash)
}

/// The finalizer mix from MurmurHash3's 128-bit variant: three
/// xor-shift/multiply rounds that spread any input bit's influence across
/// the full 64-bit output, independent of what hash produced the input.
fn fmix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51afd7ed558ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ceb9fe1a85ec53);
    k ^= k >> 33;
    k
}

/// A consistent-hash ring mapping arbitrary byte keys to shard ids `0..n`.
#[derive(Debug, Clone)]
pub struct ConsistentHashRing {
    /// Ring position -> owning shard id. A `BTreeMap` gives an O(log n)
    /// "first entry at or after this position, wrapping" lookup for free
    /// via `range` + falling back to the first entry.
    ring: BTreeMap<u64, u32>,
    shard_count: u32,
}

impl ConsistentHashRing {
    /// Builds a ring with `shard_count` shards (ids `0..shard_count`), each
    /// placed at `VIRTUAL_NODES_PER_SHARD` positions.
    pub fn new(shard_count: u32) -> Self {
        let mut ring = ConsistentHashRing {
            ring: BTreeMap::new(),
            shard_count: 0,
        };
        for shard in 0..shard_count {
            ring.add_shard(shard);
        }
        ring
    }

    pub fn shard_count(&self) -> u32 {
        self.shard_count
    }

    /// Adds a shard's virtual nodes to the ring. No-op if the shard is
    /// already present.
    pub fn add_shard(&mut self, shard: u32) {
        if self.ring.values().any(|&s| s == shard) {
            return;
        }
        for v in 0..VIRTUAL_NODES_PER_SHARD {
            let key = format!("shard-{shard}-vn-{v}");
            self.ring.insert(hash64(key.as_bytes()), shard);
        }
        self.shard_count += 1;
    }

    /// Removes a shard's virtual nodes from the ring. Every key that hashed
    /// to one of them remaps to its next-clockwise neighbor.
    pub fn remove_shard(&mut self, shard: u32) {
        let before = self.ring.len();
        self.ring.retain(|_, &mut s| s != shard);
        if self.ring.len() != before {
            self.shard_count -= 1;
        }
    }

    /// Returns the shard id `key` is routed to. Panics if the ring has no
    /// shards — same "empty is a caller bug" contract as indexing an empty
    /// slice, since there is no sensible shard to return.
    pub fn shard_for(&self, key: &[u8]) -> u32 {
        assert!(!self.ring.is_empty(), "ConsistentHashRing has no shards");
        let pos = hash64(key);
        match self.ring.range(pos..).next() {
            Some((_, &shard)) => shard,
            // Past the last ring entry: wrap around to the smallest position.
            None => *self.ring.values().next().unwrap(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distributes_keys_roughly_evenly() {
        let ring = ConsistentHashRing::new(8);
        let mut counts = [0u32; 8];
        for i in 0..10_000u32 {
            let shard = ring.shard_for(format!("row-{i}").as_bytes());
            counts[shard as usize] += 1;
        }
        // Perfectly even would be 1250/shard; virtual nodes should keep
        // every shard within a generous band of that, not e.g. one shard
        // getting 90% of the keys.
        for c in counts {
            assert!(
                (600..2200).contains(&c),
                "shard got {c} of 10000 keys, counts={counts:?}"
            );
        }
    }

    #[test]
    fn same_key_always_routes_to_same_shard() {
        let ring = ConsistentHashRing::new(5);
        let shard = ring.shard_for(b"stable-key");
        for _ in 0..100 {
            assert_eq!(ring.shard_for(b"stable-key"), shard);
        }
    }

    #[test]
    fn adding_a_shard_remaps_only_a_fraction_of_keys() {
        let before = ConsistentHashRing::new(4);
        let mut after = before.clone();
        after.add_shard(4);

        let keys: Vec<String> = (0..5000).map(|i| format!("row-{i}")).collect();
        let moved = keys
            .iter()
            .filter(|k| before.shard_for(k.as_bytes()) != after.shard_for(k.as_bytes()))
            .count();

        // Adding a 5th shard should only move keys onto the new shard
        // (~1/5 of the keyspace), never touch keys already owned by two
        // *other* existing shards. A naive `hash(key) % n` scheme would
        // remap the vast majority of keys on every membership change —
        // that's exactly the failure mode consistent hashing avoids.
        let fraction = moved as f64 / keys.len() as f64;
        assert!(
            fraction < 0.35,
            "expected roughly 1/5 of keys to move, moved {fraction:.2}"
        );

        // Every remapped key must have moved *onto* the new shard, never
        // between two pre-existing shards (the defining minimal-disruption
        // property).
        for k in &keys {
            let old = before.shard_for(k.as_bytes());
            let new = after.shard_for(k.as_bytes());
            if old != new {
                assert_eq!(new, 4, "key moved to {new}, not onto the new shard");
            }
        }
    }

    #[test]
    fn removing_a_shard_only_reassigns_its_own_keys() {
        let before = ConsistentHashRing::new(4);
        let mut after = before.clone();
        after.remove_shard(1);
        assert_eq!(after.shard_count(), 3);

        let keys: Vec<String> = (0..5000).map(|i| format!("row-{i}")).collect();
        for k in &keys {
            let old = before.shard_for(k.as_bytes());
            let new = after.shard_for(k.as_bytes());
            if old != 1 {
                // A key that wasn't on the removed shard must stay put.
                assert_eq!(old, new);
            }
        }
    }

    #[test]
    fn single_shard_ring_routes_everything_to_it() {
        let ring = ConsistentHashRing::new(1);
        for i in 0..100 {
            assert_eq!(ring.shard_for(format!("row-{i}").as_bytes()), 0);
        }
    }

    #[test]
    #[should_panic(expected = "no shards")]
    fn empty_ring_panics() {
        let ring = ConsistentHashRing::new(0);
        ring.shard_for(b"x");
    }
}
