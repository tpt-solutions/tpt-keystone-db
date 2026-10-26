# TPT Keystone — Documentation

Phase 12 checklist item: "Documentation site (architecture, SQL reference, SDK
docs, tutorials)." Scoped down to a static Markdown tree in-repo, no site
generator/build step — the content is what matters; wiring it through
mdBook/similar is a follow-up if a hosted site is ever wanted.

Everything here describes **Keystone** (`tpt-keystone/`), the core engine. The
other engines (Meridian, Prism, Chronos, Plexus, Canopy, Flux) and the agent
layers (Synapse, Mirror) run inside the same binary as SQL extensions, and their
SQL surface is documented in [`sql-reference.md`](sql-reference.md). See the root
[`TODO.md`](../TODO.md) for per-phase status, including what has only been
verified in-process, and [`CLAUDE.md`](../CLAUDE.md) for the condensed
architecture summary this longer set of docs expands on.

## Index

| Document | Covers |
|---|---|
| [`architecture.md`](architecture.md) | How `wire`/`sql`/`executor`/`storage` fit together, and the Phase 3 cloud-native storage model |
| [`sql-reference.md`](sql-reference.md) | SQL surface: DDL, DML, query clauses, and every engine-specific function/table-function (`ST_*`, `time_bucket`, `graph_*`, `vector_search`, `hybrid_search`, `json_*`, `flux_*`, `synapse_*`, `mirror_*`) |
| [`sdks.md`](sdks.md) | Client libraries: Rust, TypeScript (web/server/edge), Python, Go, CLI — what each one gives you and where its code lives |
| [`tutorials/quickstart.md`](tutorials/quickstart.md) | Build, run, connect with `psql`, create a table, run your first query |
| [`tutorials/hybrid-search.md`](tutorials/hybrid-search.md) | Worked example: vector search, BM25 full-text search, and `hybrid_search`'s Reciprocal Rank Fusion of the two |
| [`tutorials/cookbook.md`](tutorials/cookbook.md) | Seven-engine walkthrough. The runnable script is [`cookbook.sql`](tutorials/cookbook.sql) |
| [`formats/README.md`](formats/README.md) | On-disk binary format specifications (SSTable, WAL, every secondary index) |
| [`security_audit_phase12.md`](security_audit_phase12.md) | Phase 12 security audit findings, with a 2026-10-26 status table |
| [`concurrency_model_audit.md`](concurrency_model_audit.md) | Lock structure of the LSM engine: which operations can block which |
| [`mvcc_snapshot_audit.md`](mvcc_snapshot_audit.md) | Historical. The pre-implementation snapshot-isolation audit, with a status note on what has landed since |

## Honesty policy

Every doc in this tree describes what the code actually does today, not what
the spec files (`1keystonespec.txt` etc.) or `TODO.md`'s longer-term roadmap
describe. Where a feature is a documented scope cut (for example,
`CREATE INDEX IF NOT EXISTS` and `DROP INDEX` are not supported), the doc says so
explicitly rather than staying silent about it. That is the same discipline
`TODO.md` and `docs/formats/` already hold themselves to.

Audit and status documents are dated point-in-time records. Where one has been
overtaken by later work, it carries a status note at the top saying so, and the
original text is kept as history.
