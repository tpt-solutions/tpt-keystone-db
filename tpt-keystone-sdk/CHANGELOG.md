# Changelog

All notable changes to `tpt-keystone-sdk` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the crate uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This crate is at `0.1.0` and has not been published to crates.io yet, so all changes are listed under
`[Unreleased]`. Changes that span the whole repository are also recorded in the root
[`CHANGELOG.md`](../CHANGELOG.md).

## [Unreleased]

### Added

- `KeystoneClient`, an async client that speaks the Postgres wire protocol v3, with `connect`,
  `connect_with_params`, `query`, `query_params`, `copy_in`, and `close`.
- `keystone::blocking::Client`, a synchronous wrapper for command-line tools.
- Bulk ingest through `COPY ... FROM STDIN` (`copy_in`).
- Typed query builder (`QueryBuilder<T: Table>`) with equality filters, ordering, and `LIMIT`/`OFFSET`. It
  produces SQL and parameters for `query_params`.
- `tpt-keystone-sdk-typegen`, which generates `Table` implementations from a server's schema.
- Zero-copy row views (`zerocopy::RowView`), borrowed from the read buffer.
- `ffi` feature with a C ABI: `tpt_sdk_connect`, `tpt_sdk_query`, result accessors, `tpt_sdk_free_result`,
  `tpt_sdk_free_client`, and `tpt_sdk_last_error`.
- `canvas` feature that re-exports the reactive primitives from `tpt-keystone-canvas`.
- Per-crate `README.md`, `CHANGELOG.md`, and `LICENSE-MIT`/`LICENSE-APACHE` files, and crates.io
  metadata (`repository`, `readme`, `keywords`, `categories`, `rust-version`).

### Changed

- Renamed from `tpt-sdk` to `tpt-keystone-sdk`. The library name is still `tpt_sdk`.
- Reformatted to the workspace's `rustfmt` style.
- Licensed under `MIT OR Apache-2.0`.

### Known limits

- No native GPU or WebGPU rendering. The `canvas` feature exposes reactive primitives only.
- FFI covers request and response queries only, with no streaming or async callbacks.
- The query builder supports equality filters only.
