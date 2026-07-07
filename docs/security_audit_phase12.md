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

## Recommended priority for closing these out

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
