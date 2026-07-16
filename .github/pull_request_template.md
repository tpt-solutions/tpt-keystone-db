## What does this change and why?

<!-- Focus on *why* — the problem or gap being addressed — not just a restatement of the diff. -->

## Which crate(s)/package(s) does this touch?

## Testing

<!-- What did you run, and what were the results? A change with no test coverage is a much harder
review — see CONTRIBUTING.md. -->

## Checklist

- [ ] I read `CONTRIBUTING.md`, including the hard constraints (no wire-protocol/SQL-parser
      libraries, no Row-Level Security)
- [ ] Tests added/updated for the affected crate(s)
- [ ] `cargo test` (or the equivalent for a non-Rust package) passes locally
- [ ] `TODO.md` updated if this closes or opens a tracked item
- [ ] Deliberate scope cuts or known limitations introduced by this change are documented inline
      (doc comment), not left silent
