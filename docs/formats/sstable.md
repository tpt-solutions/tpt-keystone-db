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

A sequence of records, one per non-tombstone entry, in the same order the
entries were passed to the encoder (which the LSM engine sorts by key before
flushing):

```
u32 key_len
<key_len> bytes key
u32 value_len
<value_len> bytes value
```

Tombstones (deleted keys, `record_type == 2`) are **not** written to the data
section at all — only to the index (see below), with `value_len = 0`.

### Index section

```
u32 entry_count
entry_count * {
    u32 key_len
    <key_len> bytes key
    u64 offset        (offset into the data section; meaningless if value_len == 0)
    u32 value_len      (0 marks a tombstone)
}
```

Index entries are in the same order as `entries` was passed to the builder —
callers are expected to pass already-sorted entries, since `SSTable::read`
does a binary search over this index assuming key order.

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
4. Point lookup: check the bloom filter first (fast negative); on a possible
   hit, binary-search the index by key; if found with `value_len > 0`, seek
   into the data section at the index entry's `offset` and read the record
   (re-reading `key_len`/`key` to skip to the value, then `value_len` bytes).
5. Full scan: walk the data section sequentially from offset 0 until
   `index_offset`, decoding `(key_len, key, value_len, value)` records back
   to back — this does **not** need the index or bloom filter, and does not
   see tombstoned keys (they were never written to the data section).
