# Contributing to TPT

Thanks for your interest in contributing. This is a from-scratch, multi-engine data platform
(see `CLAUDE.md` for the full architecture map) — a few things are worth knowing before you dive in.

## Hard constraints

- **No `pgwire`/`sqlparser-rs` or similar crates.** The Postgres wire protocol codec (`tpt-keystone/src/wire/`)
  and the SQL lexer/parser/AST (`tpt-keystone/src/sql/`) are hand-written by design. PRs that
  reintroduce a wire/SQL-parsing library will be declined regardless of how much code they remove.
- Row-Level Security is permanently out of scope (access control follows a Zanzibar-style ReBAC +
  RBAC model instead — see `CLAUDE.md`).
- Every crate builds independently — there is no root Cargo workspace. `cd` into the crate you're
  changing before running `cargo build`/`cargo test`.

## Before you start

1. Read `CLAUDE.md` (architecture, hard constraints, build/run/test commands) and `TODO.md`
   (current task list and known scope cuts) — most "is X implemented?" questions are answered there.
2. For anything beyond a small fix, open an issue first describing the change, especially if it
   touches `tpt-keystone/src/wire/`, `sql/`, `storage/`, or `executor/` — these are the shared
   foundation every other crate and SDK depends on.

## Making a change

1. Fork and branch from `master`.
2. Match the existing code's documentation style: when you make a deliberate scope cut or leave a
   known limitation, say so explicitly in a doc comment (see `tpt-keystone-canvas/src/lib.rs`'s module doc
   comment for the convention) rather than leaving it undocumented.
3. Add or extend tests for the crate you're touching (`cargo test`, or `cargo test <module>::` for a
   single suite — see `CLAUDE.md` for per-crate specifics). A change with no test coverage is a much
   harder review.
4. Run `cargo fmt` and `cargo clippy` at your discretion — there's no enforced convention yet, but
   keep changes internally consistent with the surrounding file.
5. Update `TODO.md` if your change closes or opens a tracked item.

## Submitting

- Keep PRs focused — one logical change per PR is much easier to review than a bundle of unrelated
  fixes.
- Describe *why* the change is needed in the PR description, not just what changed.
- CI (`.github/workflows/ci.yml`) runs the test suite for every crate/package on every push — make
  sure it's green before requesting review.

## License

By contributing, you agree your contributions are dual-licensed under MIT and Apache-2.0, the same
as the rest of the project (see `LICENSE`).
