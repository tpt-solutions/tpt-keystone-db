# TPT Keystone — Documentation

Phase 12 checklist item: "Documentation site (architecture, SQL reference, SDK
docs, tutorials)." Scoped down to a static Markdown tree in-repo, no site
generator/build step — the content is what matters; wiring it through
mdBook/similar is a follow-up if a hosted site is ever wanted.

Everything here describes **Keystone** (`tpt-keystone/`), the only engine
implemented so far. See the root [`TODO.md`](../TODO.md) for what's built vs.
roadmap across the other six engines, and [`CLAUDE.md`](../CLAUDE.md) for the
condensed architecture summary this longer set of docs expands on.

## Index

| Document | Covers |
|---|---|
| [`architecture.md`](architecture.md) | How `wire`/`sql`/`executor`/`storage` fit together, and the Phase 3 cloud-native storage model |
| [`sql-reference.md`](sql-reference.md) | SQL surface: DDL, DML, query clauses, and every engine-specific function/table-function (`ST_*`, `time_bucket`, `graph_*`, `vector_search`, `hybrid_search`, `json_*`, `flux_*`, `synapse_*`, `mirror_*`) |
| [`sdks.md`](sdks.md) | Client libraries: Rust, TypeScript (web/server/edge), Python, Go, CLI — what each one gives you and where its code lives |
| [`tutorials/quickstart.md`](tutorials/quickstart.md) | Build, run, connect with `psql`, create a table, run your first query |
| [`tutorials/hybrid-search.md`](tutorials/hybrid-search.md) | Worked example: vector search, BM25 full-text search, and `hybrid_search`'s Reciprocal Rank Fusion of the two |
| [`formats/README.md`](formats/README.md) | On-disk binary format specifications (SSTable, WAL, every secondary index) |
| [`security_audit_phase12.md`](security_audit_phase12.md) | Phase 12 security audit findings |

## Honesty policy

Every doc in this tree describes what the code actually does today, not what
the spec files (`1keystonespec.txt` etc.) or `TODO.md`'s longer-term roadmap
describe. Where a feature is a documented scope cut (e.g. no raw-`COPY` bulk
load, no `ALTER TABLE ADD COLUMN`), the doc says so explicitly rather than
staying silent about it — the same discipline `TODO.md` and `docs/formats/`
already hold themselves to.
