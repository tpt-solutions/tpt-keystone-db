# SSTable Format

Source: `tpt-keystone/src/storage/sstable.rs`.

An SSTable is one immutable object-store blob (key convention: `sst/<table>/<id>`
or similar, chosen by the caller) produced when the LSM engine flushes a
MemTable. The whole blob is read into memory once fetched (SSTables are
flush-sized — a few MB at most) rather than seeked into repeatedly.

## Layout

```
+-------------------+
| data section      |   starts at offset 0
+-------------------+
| index section      |   starts at `index_offset`
+-------------------+
| bloom section       |  starts at `bloom_offset`
+-------------------+
| footer (24 bytes)   |  last 24 bytes of the blob
+-------------------+
```

All integers are big-endian.

### Data section

A sequence of records, one per non-tombstone **version**, in the same order the
entries were passed to the encoder (the LSM engine sorts them in `InternalKey`
order before flushing):

```
u32 key_len
<key_len> bytes key          (the user key; the version metadata lives in the index)
u32 value_len
<value_len> bytes value
```

A single user key can have several records here, one per retained version.
The data record does not carry its commit sequence. That lives in the index.

Tombstones (entries with `tag == DELETE_TAG`) are **not** written to the data
section at all. They appear only in the index, with `value_len = 0`. A
tombstone is identified by its `tag`, not by `value_len == 0`, so a PUT of an
empty value is still distinct from a delete.

### Index section

```
u32 entry_count
entry_count * {
    u32 key_len
    <key_len> bytes key
    u64 seq           (MVCC commit sequence of this version)
    u8  tag           (0 = put, 2 = delete; see internal_key.rs)
    u64 offset        (offset into the data section; meaningless if tag is delete)
    u32 value_len     (0 for a tombstone)
}
```

Index entries are sorted by `InternalKey` order: user key ascending, then
`seq` **descending** (newest version first), then `tag` ascending. Callers
pass already-sorted entries. The reader binary-searches this index on the
user key and then on `seq` to find the version visible to a snapshot.

### Bloom section

Backed by the [`bloomfilter`](https://docs.rs/bloomfilter) crate (Rust). Layout:

```
u32 bitmap_len
<bitmap_len> bytes bitmap
u64 num_bits
u32 num_hash_functions
2 * { u64 sip_key_0, u64 sip_key_1 }   (always exactly 2 SipHash key pairs,
                                         regardless of num_hash_functions —
                                         this is a property of the
                                         `bloomfilter` crate's `sip_keys()`,
                                         not a value chosen per filter)
```

Reconstructing the filter for reads requires the exact same bloom-filter
algorithm as the `bloomfilter` crate (SipHash-keyed bit array, standard
Bloom filter set/check using `num_hash_functions` derived hash values per
element) — this section is not self-describing beyond the raw bitmap and its
parameters.

### Footer (fixed 24 bytes, last 24 bytes of the blob)

```
u64 data_offset    (always 0 — the data section always starts at byte 0)
u64 index_offset
u64 bloom_offset
```

## Reading

1. Read the last 24 bytes as the footer; get `index_offset`, `bloom_offset`.
2. Parse the index section starting at `index_offset` (runs until
   `bloom_offset`).
3. Parse the bloom section starting at `bloom_offset` (runs until
   `blob_len - 24`).
4. Snapshot point lookup (`read_at(key, snapshot_seq)`): check the bloom filter
   first (fast negative; the bloom is keyed on the user key only). On a
   possible hit, binary-search the index for the first entry whose user key
   matches and whose `seq <= snapshot_seq`. If none, the key is absent from
   this table. If that entry's `tag` is delete, the key is a tombstone at this
   snapshot. Otherwise seek into the data section at the entry's `offset`, skip
   `key_len`/`key`, and read `value_len` bytes.
5. Full scan: walk the data section sequentially from offset 0 until
   `index_offset`, decoding `(key_len, key, value_len, value)` records back
   to back. This does **not** need the index or bloom filter, and it yields
   only the put versions. Tombstones are never in the data section, so use the
   index when deletes matter.

## Versioning

No version byte. The index entry gained `seq` and `tag` during the MVCC work
(TODO Phase 1 Stage 2+3). An SSTable written before that change is **not**
readable by this layout. The old index had no `seq`/`tag` fields, so every
index entry would be misparsed. See the Versioning section of
[`README.md`](README.md#versioning).
