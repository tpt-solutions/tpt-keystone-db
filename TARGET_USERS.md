# Who TPT is for

TPT is a consolidation play: one Postgres-wire-compatible engine standing in for a stack that
today usually means running Postgres *and* PostGIS *and* pgvector *and* InfluxDB/TimescaleDB *and*
Neo4j *and* MongoDB *and* Kafka side by side, plus a migration platform, a matching frontend
framework, and SDKs across languages — "all sharing one storage substrate" (`README.md:3-7`). The
people who get the most value from it are teams currently paying the integration tax of that stack
and willing to adopt a young, single-implementation engine to remove it.

## By engine

- **Keystone (relational core)** — teams tired of Postgres's operational friction in cloud/
  serverless environments: connection-pooler workarounds for the process-per-connection model,
  autovacuum bloat, and compute-storage coupling that fights Kubernetes-style horizontal scaling
  (`1keystonespec.txt:3-4`). Anyone already on Postgres wire protocol can point existing
  drivers/ORMs/migration tools at it unchanged.
- **Meridian (geospatial)** — teams doing real-time tracking of many moving objects, where PostGIS's
  row-oriented storage and R-Tree indexing struggle: autonomous vehicles, drones, IoT fleets, urban
  planning, robotics, climate research, indie games (`2meridianspec.txt:3,5`).
- **Prism (vector/AI)** — teams that want vector search living next to relational data in one
  ACID system, instead of pushing pgvector past its memory limits or running a separate Pinecone/
  Weaviate and dealing with the sync overhead (`3prismspec.txt:3`).
- **Chronos (time-series)** — IoT, application-metrics, financial-tick, and log workloads currently
  split across a relational database and a dedicated time-series store (InfluxDB/TimescaleDB), where
  teams want that data joined with relational state in one query instead of stitched across two
  systems (`4chronosspec.txt:3`).
- **Plexus (graph)** — social-network, fraud-detection, supply-chain, and AI-knowledge-graph
  workloads that hit self-join walls doing multi-hop traversal in a relational database, or that don't
  want a separately licensed/siloed graph database like Neo4j (`5plexusspec.txt:3,5`).
- **Canopy (document/JSON)** — teams with document-shaped data (user profiles, product catalogs,
  config, API responses) who don't want to choose between JSON-in-relational's weak indexing and a
  dedicated document store's weaker transactions/joins/schema validation (`6canopyspec.txt:3`).
- **Flux (event streaming)** — teams wanting event sourcing, CQRS, or CDC without operating a
  separate broker cluster, and who want to query the event log alongside relational data in the same
  transaction (`7fluxspec.txt:3-4`).
- **Canvas (WASM frontend)** — frontend developers building data-rich dashboards/maps/graphs who
  currently hand-stitch React + a data-fetching layer + Mapbox + D3 + Cytoscape + custom WebSocket
  code, none of which understand each other or the underlying multi-model data (`8canvasspec.txt:3`).
- **SDKs** — developers building on Canvas and Keystone across Rust, Go, Python, TypeScript
  (web/server/edge/React Native), Dart/Flutter, and Kotlin/Android (`9sdkspec.txt:1-4`).
- **Harbor (migration)** — organizations with production data already on Postgres, PostGIS, MySQL,
  MSSQL, Oracle, MongoDB, Neo4j, InfluxDB, Kafka, Elasticsearch, or Pinecone/Weaviate/Qdrant who want
  a de-risked path onto Keystone rather than a hand-rolled migration (`10harbourspec.txt:3`,
  `tpt-keystone-harbor/README.md:9-12`). PostgreSQL migration is the one verified end-to-end today
  (schema discovery, snapshot, `pgoutput` CDC, live cutover); the other sources have discovery,
  snapshot, and checksum verification, with live CDC as a named per-connector scope cut except Kafka
  (`tpt-keystone-harbor/README.md:9-16`).

## Who this isn't for yet

TPT is "a from-scratch, single-team project, not a production-hardened platform" — most components
are verified in-process or against each other, not at scale or against real third-party traffic
(`README.md:9-14`). Every "implemented" feature means "unit/integration-tested in this repo," not
"battle-tested in production." That makes today's realistic user an early adopter or technical
evaluator who can absorb that risk in exchange for the consolidation win above — not a risk-averse
enterprise buyer looking for a drop-in, vendor-supported Postgres/Neo4j/InfluxDB/Kafka replacement.
`TODO.md` is the authoritative source for exactly which pieces are verified vs. scope-cut before
betting production traffic on any one engine.
