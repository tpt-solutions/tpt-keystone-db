# Phase 12 Security Audit — Wire Auth, WASM Sandbox, S3 Credentials

Date: 2026-07-07. Scope: a focused, code-level review (not a penetration
test) of the three areas named by the Phase 12 checklist item "Security
audit (wire protocol auth, WASM sandbox, S3 credential handling)":
`src/wire/session.rs`/`codec.rs`/`messages.rs`, `src/executor/udf.rs`, and
`src/storage/objectstore.rs`/`config.rs`.

## Summary

The wire-auth gap (no password/SCRAM check, no TLS) is the single most
severe finding and is the headline Phase 12 item — every other finding here
is defense-in-depth that only matters once a client can reach the server at
all, which today any TCP client can. The WASM UDF sandbox's containment
model (empty `Linker`, fuel limit, memory cap) is architecturally sound but
has two gaps worth closing: no bound on the compile-time cost of an
attacker-supplied module, and trap-handling behavior that is documented as
unverified on the actual deployment OS (see `executor/udf.rs`'s own test
comment — wasmtime traps crash the whole process on this dev sandbox's
Windows environment; this needs confirmation on Linux/production before
being trusted as a hard boundary). S3 credential handling showed no leakage
or unsafe-default issues.

## 1. Wire protocol auth

| Severity | Finding |
|---|---|
| **High** | No authentication (`wire/session.rs:64`): `AuthenticationOk` is sent unconditionally right after `read_startup()`, with no password/MD5/SCRAM challenge and no check of the client-supplied `user`/`database` startup parameters. Any TCP client that can reach the listener has full, unrestricted access to every table and to WASM `CREATE FUNCTION`. **Fix:** implement at minimum SCRAM-SHA-256 (Postgres' current default), gated by config, defaulting closed outside an explicit dev mode. |
| Low | Startup parameters are inert (`wire/codec.rs:78-86`, `wire/session.rs:62`): `user`/`database`/etc. are parsed but never read downstream — no multi-tenancy, no per-user/per-database scoping. Not an injection vector (never interpolated into SQL/paths/logs), but could mislead an operator reading connection logs into assuming scoping exists. |
| Informational | Startup message length is bounded to `8..=65535` before buffering (`wire/codec.rs:59`) — no unbounded-allocation risk from a malformed startup message. |
| Low | No cap on the number/total size of repeated key/value pairs within a valid-length (≤64KB) startup message. |
| Informational | TLS is explicitly declined: the server always answers an `SSLRequest` with `N` and continues in cleartext (`wire/codec.rs:69-73`). Combined with no-auth, once auth is added, credentials and all query data would still traverse the wire unencrypted until this is also addressed. |
| Informational | Connection-count exhaustion is bounded by a `Semaphore` sized from `TPT_MAX_CONNECTIONS` (`main.rs:70,124`, default 1000); excess connections queue rather than erroring. `CancelRequest` is a no-op (`session.rs:220-222`), so it carries no resource cost either. |

## 2. WASM UDF sandbox (`executor/udf.rs`)

| Severity | Finding |
|---|---|
| Medium | `wasmtime::Config::new()` (`udf.rs:84`) only sets `consume_fuel(true)` explicitly; SIMD, reference types, multi-memory, and (version-dependent) the threads/shared-memory proposal are left at wasmtime's library defaults rather than an explicit allow-list. None of these alone breaks the sandbox today (the `Linker` is empty, so there's no imported memory/host state to corrupt or race), but relying on upstream defaults is fragile against future wasmtime default changes. **Fix:** pin an explicit `Config` allow-list. |
| Medium — **fixed** | No module size or compile-time bound (`udf.rs:44-46,87`): `Module::new` compiled attacker-supplied bytes with no pre-check on `wasm_bytes.len()` and no compile-time fuel/timeout, so a large or adversarially-crafted module could burn significant CPU/memory *during compilation*, before the fuel limiter (which only bounds execution) engages. Closed by adding `UdfConfig::max_module_bytes` (default 4MiB, overridable via `TPT_UDF_MAX_MODULE_BYTES`), checked in `validate_module` before `Module::new` is ever called — see `create_function_rejects_oversized_module` in `udf.rs`'s test module. A wall-clock compile timeout for modules within the size limit is still not implemented. |
| Low | A fresh `Engine`/`Module` is compiled on **every call** (`udf.rs:86-87`), not just at `CREATE FUNCTION` time — amplifies the compile-time-bomb concern above per-query rather than one-time, and is a plain perf cost regardless. |
| Informational | Trap-vs-error handling is **unverified in practice**: the module's own test comment (`udf.rs:190-204`) documents that fuel exhaustion/OOM traps crash the whole process on this Windows dev sandbox (`STATUS_STACK_BUFFER_OVERRUN` inside wasmtime's own trap-handling frames, not `executor::udf` code). If a trap crashes the whole server process rather than returning `Err` to one connection in the real deployment environment, that's a live-availability risk, not just a test artifact — needs confirmation on the actual production OS (Linux) before the fuel/memory limiter is trusted as a hard boundary. |
| Informational | The containment design itself is sound: `Linker` is genuinely empty (no host imports — a module has no I/O and no ambient authority), and `StoreLimitsBuilder::memory_size` + `store.limiter(|s| s)` correctly wire the memory cap (`udf.rs:89-91`). |

## 3. S3 credential handling (`storage/objectstore.rs`, `storage/config.rs`)

| Severity | Finding |
|---|---|
| Informational | Credentials never touch application code or logs: `S3ObjectStore::connect` (`objectstore.rs:229-246`) sources credentials purely through `aws_config::defaults(...).load()` (the AWS SDK's own env/instance-profile/SSO chain) and never logs the resolved config or credentials. Errors are wrapped via `anyhow!(err).context(...)` (`objectstore.rs:294,311,344,359,378`), and the underlying SDK error types' `Display` impls don't include Authorization headers or signing keys — no credential leakage into error messages found. |
| Low | Bucket/region/endpoint config (`config.rs:76-85`, `TPT_S3_*` env vars) is trusted, unvalidated operator input — standard for server config, not attacker-reachable today, but flagged since (combined with the wire-auth gap, hypothetically, if an attacker ever gained env-write/code-exec capability) it's a direct pivot to an attacker-controlled S3-compatible endpoint. |
| Low | No panics/`unwrap()` on the S3 request path — `get`/`put`/`put_if_match`/`delete`/`list` all propagate errors via `Result`/`CasError`; no panic-based leakage risk. |
| Informational | Conditional-PUT correctness looks right: `put_if_match` (`objectstore.rs:318-348`) maps `None` → `if_none_match("*")` and `Some(etag)` → `if_match` (re-quoted), and distinguishes HTTP 412 (`CasError::Conflict`) from other failures via response status — matches the CAS semantics the manifest/lease code depends on. |
| Informational | No request/response body or header logging anywhere in `objectstore.rs` — `tracing` isn't invoked in this file at all, so no risk of a `debug!`/`info!` call accidentally dumping a signed request or object body containing secrets. |

## 4. Non-Postgres bridge listeners — auth added (Phase 3 / Phase 20 follow-up)

The original audit (sections 1–3) scoped its wire-auth review to the Postgres
wire protocol listener (`src/wire/session.rs`). The engine also brings up four
*other* network listeners — the Canvas HTTP query bridge (`wire/http_query.rs`),
the Flux WebSocket streaming endpoint (`wire/websocket.rs`), the Flux gRPC
endpoint (`wire/grpc/mod.rs`), and the MCP server (`src/mcp/`) — and none of
these were covered by the original audit, nor did they have any authentication
at the time.

As of Phase 3, all four now share one authentication helper,
`wire::bridge_auth` (`src/wire/bridge_auth.rs`), modeled on `session::run`'s
zero-config behavior:

- **HTTP, WebSocket, gRPC** authenticate with `Authorization: Basic` (an
  `Authorization` metadata header on gRPC). When `_tpt_roles` is empty (the
  default quickstart) the check short-circuits to `Actor::unrestricted()`,
  exactly preserving the documented zero-config experience; once at least one
  role exists, a valid `Basic` credential (password verified against the
  stored SCRAM credential, role must have `LOGIN`) is required.
- **MCP** keeps its existing `X-TPT-Token` gate and now additionally resolves a
  concrete RBAC `Actor` (the first superuser role) so downstream tool handlers
  can enforce per-table RBAC, mirroring the other bridges.

The authenticated `Actor` is threaded into `http_query.rs` and the MCP tool
handlers (`src/mcp/tools.rs`, `src/mcp/protocol.rs`) for real per-table RBAC.
Connection-level rate limiting was also added across all four bridges
(`TPT_HTTP_MAX_CONNECTIONS`, `TPT_FLUX_WS_MAX_CONNECTIONS`,
`TPT_FLUX_GRPC_MAX_CONNECTIONS`, `TPT_MCP_MAX_CONNECTIONS`, default 1000) as a
`tokio::sync::Semaphore` held for the connection's lifetime.

| Severity | Finding |
|---|---|
| **Fixed** | The four non-Postgres listeners now require authentication once roles are configured (zero-config preserved). See `wire/bridge_auth.rs` and `src/wire/grpc/mod.rs:416`. |
| Low — **still open** | The HTTP/WebSocket/gRPC bridges enforce *authentication* (who you are) but not *per-topic authorization* — there is no topic-level privilege model in `rbac.rs`, so any authenticated caller can publish/subscribe/poll any Flux topic. Documented as a known architectural ceiling, not fixed here. MCP tool handlers do enforce per-table RBAC. |
| Informational | The authenticated `Actor` is `unrestricted()` under the zero-config path; this is intended (same as the Postgres listener) and only changes once `_tpt_roles` is populated. |

The Phase 12 "still open" top finding — wire-protocol authentication — is now
closed for **all five** listeners (Postgres via SCRAM-SHA-256 per
`tpt-keystone`'s existing session handshake, plus these four bridges via Basic
token / MCP token). TLS remains opt-in (`TPT_TLS_CERT_PATH` /
`TPT_TLS_KEY_PATH`); the deployment guidance is to terminate TLS in front of
the listeners until in-process TLS is the default.


1. Wire protocol authentication (SCRAM) + TLS — the only finding that
   changes the server's actual trust boundary; everything else assumes an
   attacker can already connect. **Still open.**
2. WASM UDF compile-time bound (module size cap) — **closed** in this pass
   (`UdfConfig::max_module_bytes`).
3. Verify wasmtime trap behavior on the real deployment OS (Linux) before
   relying on fuel/memory limits as a hard isolation boundary in production.
   **Still open** — requires a non-Windows environment to test.
4. Explicit `wasmtime::Config` hardening (disable unused proposals) as
   defense-in-depth, lower urgency than 1 and 3. **Still open.**
