# Meridian (Phase 6) Spatial Index Format

Source: `tpt-keystone/src/storage/geo_index.rs`. Local per-node only — not
replicated through the shared object store (same scope cut documented for
the B-Tree, Chronos, and Plexus local indexes; each node rebuilds it from
the table on first open if the file is missing).

Uses the [append-only record log convention](primitives.md#append-only-record-log-convention).

## File header (1 byte)

```
u8 level    (the fixed S2-inspired grid level, chosen at CREATE INDEX
             time from `geo::s2::level_for_radius(expected_query_radius)`)
```

## Record value: `GeoEntry` (bincode-v1, per the [record-log convention](primitives.md#append-only-record-log-convention))

```rust
struct GeoEntry {
    row_key: Vec<u8>,
    lon: f64,
    lat: f64,
    time: Option<i64>,   // unix milliseconds; Meridian's 4D spatiotemporal `t` ordinate
}
```

Encoded per the [bincode-v1 conventions](primitives.md#bincode-v1-blobs):
`Vec<u8>` as a little-endian `u64` length prefix + raw bytes, `f64` as raw
8-byte little-endian IEEE-754, `Option<i64>` as a `u8` tag (`0`/`1`) followed
by an 8-byte little-endian `i64` if present.

## In-memory index structure (not persisted directly)

On open, every record is replayed and re-bucketed into
`HashMap<CellId, Vec<GeoEntry>>` keyed by `geo::s2::cell_id_for_point(lon,
lat, level)` — the S2-inspired cell ID is **not** stored in the record
itself; it's always recomputed from `(lon, lat, level)` on load. This means
changing the on-disk `level` byte without re-indexing would silently
re-bucket every entry incorrectly relative to what a fixed `level` assumed
at index-creation time — the level byte in the header must stay consistent
for the file's entire life.

## Query semantics (not part of the on-disk format, but needed to
reimplement equivalent behavior)

`query_radius(center_lon, center_lat, radius_m, time_range)`:

1. Compute the covering cell set via `s2::neighborhood(center_lon,
   center_lat, level)` (the handful of grid cells that could contain a point
   within `radius_m` of the center, at this grid `level`).
2. For every entry in those cells, compute the exact haversine distance and
   filter to `<= radius_m` — the cell lookup is a coarse pre-filter, not an
   exact spatial predicate.
3. If a `time_range` was given, additionally filter entries by inclusive
   `[t0, t1]` on `entry.time` — done as a second predicate over the same
   per-cell entry list, not a separate index structure.
