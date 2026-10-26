# Changelog

All notable changes to `tpt-keystone-cli` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the crate uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This crate is at `0.1.0` and has not been published to crates.io yet, so all changes are listed under
`[Unreleased]`. Changes that span the whole repository are also recorded in the root
[`CHANGELOG.md`](../CHANGELOG.md).

## [Unreleased]

### Added

- The `tpt` binary, with these subcommands:
  - `repl`: interactive SQL REPL, the default when no subcommand is given
  - `query`: run one statement, or a SQL file with `-f`, and print the result as table, JSON, or CSV
  - `stream`: tail a Flux topic over WebSocket (`--flux-port`, default 5434)
  - `migrate up` and `migrate status`: apply and list `.sql` migrations, tracked in `_tpt_migrations`
  - `export` and `import`: move a table's rows to or from CSV or JSON
  - `schema`: list tables, or describe one table's columns
- Global `--host` and `--port` options.
- Per-crate `README.md`, `CHANGELOG.md`, and `LICENSE-MIT`/`LICENSE-APACHE` files, and crates.io
  metadata (`repository`, `readme`, `keywords`, `categories`, `rust-version`).

### Changed

- Renamed from `tpt-cli` to `tpt-keystone-cli`. The binary is still called `tpt`.
- Depends on `tpt-keystone-sdk`'s blocking client for all Keystone access, instead of a separate copy of
  the wire client.
- Licensed under `MIT OR Apache-2.0`.

### Known limits

- The client connects without credentials, so it works only with a node that has no roles configured.
- No TLS option.
