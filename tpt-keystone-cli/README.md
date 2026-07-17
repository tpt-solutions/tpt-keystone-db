# tpt-keystone-cli

Single-binary command-line client for TPT Keystone: interactive SQL REPL, query execution,
schema introspection, export/import, Flux stream tailing, and schema migrations.

## Install

```bash
cd tpt-keystone-cli
cargo install --path .
```

## Commands

```
tpt [OPTIONS] <COMMAND>

Options:
  --host <HOST>   Keystone host [default: 127.0.0.1]
  --port <PORT>   Postgres wire port [default: 55432]

Commands:
  repl      Interactive SQL REPL
  query     Run a SQL query and print results
  schema    Introspect tables, columns, and indexes
  export    Export a table to JSON or CSV
  import    Bulk-import JSON or CSV into a table
  stream    Tail a Flux streaming topic (WebSocket, port 5434)
  migrate   Apply schema migrations
```

## Examples

```bash
# Interactive REPL
tpt repl

# One-shot query
tpt query "SELECT count(*) FROM events WHERE ts > now() - interval '1 hour'"

# Tail a Flux topic
tpt stream --topic orders

# Export a table to JSON
tpt export --table users --format json > users.json
```

Communicates with Keystone over the hand-written Postgres wire protocol v3 via `tpt-keystone-sdk`'s
synchronous blocking client. `tpt stream` uses a separate hand-rolled RFC 6455 WebSocket client
against the Flux bridge (default port 5434).

## License

Apache-2.0 — Copyright 2026 TPT Solutions
