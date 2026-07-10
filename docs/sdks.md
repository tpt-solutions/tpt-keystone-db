# SDKs

Every SDK below (Phase 14) is a hand-written Postgres-wire (or, for
browser/edge targets, HTTP/JSON-bridge) client — none depend on `pg`/
`psycopg2`/`lib/pq`/`pgwire`/etc., the same from-scratch-protocol rule
`tpt-keystone` itself follows. See each package's own README/module docs
for full detail; this is an index of what exists and where.

| SDK | Path | Language/runtime | Notes |
|---|---|---|---|
| `tpt-sdk` | `tpt-sdk/` | Rust | `keystone`/`canvas`/`ffi` feature flags; sync + async clients; C ABI for FFI |
| `@tpt/sdk-web` | `packages/sdk-web/` | Browser (TS) | HTTP/JSON bridge + Flux WebSocket; React hooks; Canvas plugin API |
| `@tpt/sdk-server` | `packages/sdk-server/` | Node/Deno/Bun (TS) | Full Postgres wire client incl. streaming queries; Flux broadcast server |
| `@tpt/sdk-edge` | `packages/sdk-edge/` | Cloudflare/Fastly/Vercel/Lambda@Edge (TS) | Zero-dependency, ~4.6KB; same HTTP/JSON bridge as sdk-web |
| `tpt-sdk` (Python) | `sdk-python/` | Python 3 | `asyncio`; `to_pandas()`; Jupyter `_repr_html_` |
| `sdk-go` | `sdk-go/` | Go | `context.Context` cancellation; connection pooling |
| `tpt` CLI | `tpt-cli/` | — | REPL, `query`/`export`/`import`/`migrate`/`stream`/`schema` subcommands |

## Common shape across every SDK

1. **Schema introspection** — every SDK can fetch live column/type/nullable/
   PK/FK info from a running Keystone node (`information_schema` over the
   wire, or `GET /schema` over HTTP for the browser/edge SDKs) and generate
   typed bindings from it: `tpt-canvas`'s `tsgen`, `@tpt/sdk-web`'s
   `tpt-typegen`, the Rust/TS/Python query-builder codegen below.
2. **Typed query builder** (this session's addition — see below) — a
   schema-driven builder that produces parameterized SQL from typed method
   calls, instead of hand-formatting raw SQL strings, for the three SDKs
   most likely to be driven by an LLM/agent (Rust, TypeScript, Python).
3. **Raw query escape hatch** — every SDK still exposes a plain
   `query(sql, params)`-shaped method; the typed builder is sugar over it,
   never a replacement that blocks arbitrary SQL.

## Typed query builder + codegen (Phase 5)

Phase 5's "AI-optimised SDK" checklist item is scoped to **adding a typed
query builder on top of the existing Phase 14 SDKs**, not standing up a
fourth parallel SDK family. Each of Rust/TypeScript/Python gets:

- A **codegen step** that reads `GET /schema` (or the equivalent wire-level
  introspection) and emits one type per table — a Rust struct
  (`#[derive(...)]`), a TypeScript `interface`, or a Python `dataclass` —
  with each column's SQL type mapped to the closest native type.
- A **typed query builder** parameterized over that generated type:
  `.select([...cols]).where(...).limit(n)` (or the language-idiomatic
  equivalent), building a parameterized SQL string + parameter list
  internally rather than the caller formatting SQL text by hand. This is
  new sugar over each SDK's existing wire client — it does not replace the
  raw `query()`/`queryParams()` escape hatch.
- The generated types and the builder are two different concerns kept in
  two different files/modules per SDK, so a project that only wants typed
  results (not the builder) can use the codegen output alone.

Implemented for Rust, TypeScript (`@tpt/sdk-web` and `@tpt/sdk-server` each
get their own copy — the two packages have no workspace dependency between
them, so this follows the same "duplicate the hand-written protocol code
per package" precedent their wire clients already set, not a shared
library), and Python:
- Rust: `tpt-sdk/src/query_builder.rs` (`Table` trait + `QueryBuilder<T>`) +
  `tpt-sdk/src/bin/typegen.rs` (`cargo run --bin tpt-sdk-typegen -- host:port`)
- TypeScript: `packages/sdk-web/src/query-builder.ts` (`TableDef<Row>` +
  `TypedQueryBuilder<Row>`, `.fetch()` over the HTTP bridge), with
  `bin/typegen.ts` extended to also emit a `TableDef` const per generated
  interface. `packages/sdk-server/src/query-builder.ts` is the same design
  against the Postgres-wire client instead (`.fetch()` calls
  `KeystoneClient.queryParams` and decodes via `Row.toObject()`). Not yet
  ported to `@tpt/sdk-edge` (same HTTP bridge as sdk-web, so it would be a
  near-identical copy — left as a follow-up).
- Python: `sdk-python/tpt_sdk/query_builder.py` (`TableDef` + `QueryBuilder`)
  + `sdk-python/tpt_sdk/typegen.py` (`python -m tpt_sdk.typegen host:port`)

Batch operations, streaming results, and a built-in connection pool already
exist per-SDK from Phase 14 (`streamQuery`/`Pool`/`blocking::Client` etc.)
— the typed builder composes with those, it doesn't duplicate them.
