# tpt-keystone-cli

[![crates.io](https://img.shields.io/crates/v/tpt-keystone-cli.svg)](https://crates.io/crates/tpt-keystone-cli)
[![license](https://img.shields.io/crates/l/tpt-keystone-cli.svg)](https://github.com/tpt-solutions/tpt-keystone-db/blob/master/tpt-keystone-cli/LICENSE-MIT)

`tpt` is the command-line client for [TPT Keystone](../tpt-keystone/README.md). It is a single binary
that provides an interactive SQL REPL, one-shot queries, schema introspection, table export and import,
Flux stream tailing, and schema migrations.

It is a small client, not a `psql` replacement. The REPL has no `\d`-style meta-commands; its only
backslash command is `\q`.

## Requirements

- Rust **1.85** or newer
- A running `tpt-keystone` node. The CLI speaks the Postgres wire protocol to it, and `tpt stream`
  also needs the Flux WebSocket bridge.

## Install

From crates.io, once published:

```bash
cargo install tpt-keystone-cli
```

From a checkout:

```bash
cd tpt-keystone-cli
cargo install --path .
```

The binary is called `tpt`, not `tpt-keystone-cli`.

## Global options

```
tpt [OPTIONS] [COMMAND]

Options:
      --host <HOST>  Keystone host [default: 127.0.0.1]
      --port <PORT>  Keystone Postgres-wire port [default: 5432]
  -h, --help         Print help
  -V, --version      Print version
```

> **Port default:** `tpt` connects to port **5432** by default, but `tpt-keystone` listens on
> **55432** by default. Pass `--port 55432` when connecting to a default-configured node, or run the
> server with `TPT_PG_ADDR=0.0.0.0:5432`.

When no command is given, `tpt` starts the REPL.

## Commands

| Command | What it does |
|---|---|
| `repl` | Interactive SQL REPL. This is the default with no subcommand. |
| `query <SQL>` | Runs one statement and prints the result. `-f <FILE>` reads the SQL from a file. `--format table\|json\|csv`. |
| `stream <TOPIC>` | Tails a Flux topic over WebSocket. `--flux-port` defaults to 5434. |
| `migrate up` | Applies pending migrations from a directory. `--dir` defaults to `migrations`. |
| `migrate status` | Shows which migrations are applied and which are pending. |
| `export <TABLE>` | Writes a table's rows. `--format csv\|json` (default `csv`); `-o <FILE>` writes to a file instead of stdout. |
| `import <TABLE> -f <FILE>` | Loads rows from a CSV or JSON file. `--format csv\|json` (default `csv`). |
| `schema [TABLE]` | Lists tables, or describes one table's columns. |

## REPL

```
$ tpt repl
Type SQL statements terminated by ';', or \q to quit.
```

Statements may span several lines and are sent when a line ends in `;`. Enter `\q`, `\quit`, `quit`,
or `exit` to leave.

## Examples

```bash
# One-shot query against a node on the default port
tpt --port 55432 query "SELECT count(*) FROM events WHERE ts > now() - interval '1 hour'"

# Run a SQL file and print the result as JSON
tpt query -f report.sql --format json

# Describe a table, then list all tables
tpt schema users
tpt schema

# Tail a Flux topic
tpt stream orders --flux-port 5434

# Export a table to a file, then load it into another node
tpt export users -o users.csv
tpt --host other-host --port 55432 import users -f users.csv

# Apply migrations from ./migrations
tpt migrate status
tpt migrate up --dir ./migrations
```

## Migrations

A migration is a plain `.sql` file in a directory. Migrations are applied in filename order, for
example `0001_init.sql` before `0002_add_users.sql`. Each applied migration is recorded by ID in a
`_tpt_migrations (id TEXT PRIMARY KEY, applied_at TEXT)` table, which the CLI creates on first use.
`migrate up` applies only migrations that are not yet recorded.

## Streaming

`tpt stream` connects to the Flux WebSocket bridge (default port 5434). It uses a small, hand-written
RFC 6455 client in `src/stream.rs`, so it does not depend on a WebSocket crate. The Postgres-wire port
is not used for streaming.

## How it connects

The CLI uses the blocking client from [`tpt-keystone-sdk`](../tpt-keystone-sdk/README.md), which
speaks the hand-written Postgres wire protocol v3 used by the server. The REPL, `query`, `export`,
`import`, `schema`, and `migrate` all go through that client.

## Limitations

- No authentication flags yet. The client connects without credentials, so it works only with a node
  that has no roles configured (the zero-config default).
- No TLS option.
- Output formats are limited to `table`, `json`, and `csv`.

## Changelog

Release notes are in [`CHANGELOG.md`](CHANGELOG.md).

## Contributing

See [`CONTRIBUTING.md`](../CONTRIBUTING.md).

## License

Licensed under either of

- Apache License, Version 2.0 ([`LICENSE-APACHE`](LICENSE-APACHE))
- MIT license ([`LICENSE-MIT`](LICENSE-MIT))

at your option. Copyright 2026 TPT Solutions.
