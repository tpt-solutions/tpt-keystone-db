# Contributing to TPT

Thanks for your interest in TPT. This is a from-scratch, multi-engine data platform (see `CLAUDE.md`
for the full architecture map).

**Contributions are accepted as issues only.** Please open an issue to report a bug, request a feature,
or propose a change. Pull requests are not accepted, so please don't open one or send a patch, and
don't expect a fork to be merged.

## Hard constraints

These apply to anything you propose, including in an issue:

- **No `pgwire`/`sqlparser-rs` or similar crates.** The Postgres wire protocol codec (`tpt-keystone/src/wire/`)
  and the SQL lexer/parser/AST (`tpt-keystone/src/sql/`) are hand-written by design. Proposals that
  reintroduce a wire/SQL-parsing library will be closed regardless of how much code they remove.
- Row-Level Security is permanently out of scope (access control follows a Zanzibar-style ReBAC +
  RBAC model instead — see `CLAUDE.md`).
- Every crate builds independently — there is no root Cargo workspace.

## Before you open an issue

1. Read `CLAUDE.md` (architecture, hard constraints, build/run/test commands) and `TODO.md`
   (current task list and known scope cuts). Most "is X implemented?" questions are answered there.
2. Search existing issues to avoid duplicates.
3. Describe the problem or proposal and why it matters. For bugs, include the steps to reproduce,
   the expected and actual behavior, and the crate and version or commit you tested.

## License

Issues are not licensed contributions. The project itself is dual-licensed under MIT and Apache-2.0
(see `LICENSE`).
