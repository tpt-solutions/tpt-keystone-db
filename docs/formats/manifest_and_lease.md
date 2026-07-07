# Shared Manifest and Write-Lease Formats

Source: `tpt-keystone/src/storage/manifest.rs`, `tpt-keystone/src/storage/lease.rs`.
Both are Phase 3 (cloud-native/disaggregated storage) objects living in the
shared object store, both are plain [bincode-v1 blobs](primitives.md#bincode-v1-blobs)
of a single Rust struct — there's no separate framing beyond what bincode
itself produces.

## Manifest — object key `manifest.bin`

The single source of truth for what SSTables/WAL state currently make up the
database. One node (the writer) updates it via compare-and-swap after every
flush; every other node (readers) polls and re-reads it.

Struct (bincode-v1 encoded, field order matters):

```rust
struct Manifest {
    sstable_ids: Vec<u64>,        // IDs of all live SSTables, oldest first
    wal_segment_seq: u64,         // highest sealed+shipped WAL segment ID
    writer_fencing_token: u64,    // fencing token of the writer that wrote this revision
}
```

Per the [bincode-v1 conventions](primitives.md#bincode-v1-blobs): `Vec<u64>`
is a little-endian `u64` length prefix followed by that many little-endian
`u64` elements; the two trailing `u64` fields are plain little-endian
8-byte integers. Total size for a manifest with `n` sstable IDs is
`8 + 8*n + 8 + 8` bytes.

Writes always go through `put_if_match` (see `objectstore.rs`) with the
caller's last-known ETag as the expected value (`None` only for the very
first manifest write, which must create the object). A CAS conflict means a
different writer's revision landed first — see `CasError::Conflict`.

## Write lease — object key `_lease/{keyspace}` (e.g. `_lease/db`)

Enforces "exactly one writer at a time" via the object store's conditional
PUT, independent of the manifest.

Struct (bincode-v1 encoded):

```rust
struct Lease {
    holder_id: String,     // arbitrary node identifier
    fencing_token: u64,    // monotonically increasing; bumped every acquire/takeover
    expires_at_ms: u64,    // unix milliseconds; a stale-looking value is eligible for takeover
}
```

`String` follows the same bincode-v1 sequence convention as `Vec<T>`: a
little-endian `u64` byte-length prefix followed by the raw UTF-8 bytes (no
NUL terminator).

Acquiring or renewing the lease is a `put_if_match` against this key: read
the current lease (if any) and its ETag, decide whether it's expired or
absent, then CAS-write a new `Lease` with the same or an incremented
`fencing_token`. `fencing_token` never decreases, and a manifest write must
be tagged in application logic with the current `fencing_token` a writer
believes it holds — this is what makes a "zombie" writer's late manifest CAS
fail even if its own lease-expiry check hadn't yet fired locally (see the
module doc comment in `lease.rs`).
