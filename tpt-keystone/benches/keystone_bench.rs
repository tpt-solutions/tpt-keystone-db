//! Phase 12's "formal benchmark suite" — scoped down to what's actually
//! feasible in this environment: real, repeatable `criterion` measurements
//! of Keystone's *own* throughput/latency, not a head-to-head comparison
//! against Postgres/InfluxDB/Neo4j/MongoDB/Kafka (none of those are
//! installed here, and installing five external systems is out of scope
//! for a benchmark harness living in this crate). Every other phase's
//! "1M rows/sec"/"<10ms on 10M rows"-style milestone claims in `TODO.md`
//! are explicitly marked unverified for the same reason — this harness is
//! the first step toward actually measuring instead of guessing, against
//! this engine alone.
//!
//! Every benchmark runs against an in-process `Database` backed by
//! `LocalFsObjectStore` (the same dev/test object-store emulation
//! `phase3_tests.rs` and every `*_tests.rs` end-to-end test already use) —
//! there is no real S3/NVMe-cache path exercised here, so these numbers
//! describe the engine's CPU/logic cost, not a production deployment's I/O
//! profile.
//!
//! Run with `cargo bench` from `tpt-keystone/`.

use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};

use tpt_keystone::executor::execute_query;
use tpt_keystone::storage::config::NodeRole;
use tpt_keystone::storage::database::Database;
use tpt_keystone::storage::lease::LeaseManager;
use tpt_keystone::storage::objectstore::{LocalFsObjectStore, ObjectStore};

/// Same harness every `executor/*_tests.rs` module already uses (see e.g.
/// `canopy_tests.rs::test_db`) — a fresh single-node writer over a fresh
/// `LocalFsObjectStore` root, kept alive by returning its temp dirs.
fn test_db() -> (Arc<Database>, tempfile::TempDir, tempfile::TempDir) {
    let bucket = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::open(bucket.path()).unwrap());
    let lease = Arc::new(LeaseManager::new(
        store.clone(),
        "db",
        "bench-node".into(),
        Duration::from_secs(30),
    ));
    lease.try_acquire().unwrap();
    let db = Arc::new(
        Database::open(
            local.path(),
            store,
            lease.handle(),
            NodeRole::Writer,
            Default::default(),
        )
        .unwrap(),
    );
    (db, bucket, local)
}

fn q(db: &Arc<Database>, sql: &str) {
    execute_query(sql, db.clone()).unwrap();
}

/// INSERT throughput on a plain 3-column table, batched so each criterion
/// iteration measures `N` inserts against a fresh table (steady-state
/// per-row cost, not "insert into an ever-growing table").
fn bench_insert_throughput(c: &mut Criterion) {
    const N: usize = 500;
    let mut group = c.benchmark_group("insert_throughput");
    group.throughput(Throughput::Elements(N as u64));
    group.sample_size(20);
    group.bench_function("insert_500_rows", |b| {
        b.iter_batched(
            || {
                let (db, bucket, local) = test_db();
                q(
                    &db,
                    "CREATE TABLE bench_ins (id INT4, name TEXT, score FLOAT8)",
                );
                (db, bucket, local)
            },
            |(db, _bucket, _local)| {
                for i in 0..N {
                    q(
                        &db,
                        &format!(
                            "INSERT INTO bench_ins VALUES ({i}, 'row-{i}', {})",
                            i as f64 * 1.5
                        ),
                    );
                }
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

/// Point-lookup latency (`WHERE id = ?`) against a pre-populated table —
/// the query-shape half of Phase 1's "INSERT rows, restart, SELECT them
/// back" milestone and Phase 2's planner's index-aware point-lookup path.
fn bench_point_select(c: &mut Criterion) {
    let (db, _bucket, _local) = test_db();
    q(
        &db,
        "CREATE TABLE bench_pt (id INT4, name TEXT, score FLOAT8)",
    );
    for i in 0..5000 {
        q(
            &db,
            &format!(
                "INSERT INTO bench_pt VALUES ({i}, 'row-{i}', {})",
                i as f64 * 1.5
            ),
        );
    }
    let mut group = c.benchmark_group("point_select");
    group.bench_function("select_by_id_5000_row_table", |b| {
        let mut i = 0usize;
        b.iter(|| {
            i = (i + 37) % 5000; // walk the key space instead of hammering one hot row
            execute_query(
                &format!("SELECT * FROM bench_pt WHERE id = {i}"),
                db.clone(),
            )
            .unwrap()
        });
    });
    group.finish();
}

/// Full-table-scan aggregate latency — the "no index to exploit" baseline
/// every indexed benchmark below should beat.
fn bench_full_scan(c: &mut Criterion) {
    let (db, _bucket, _local) = test_db();
    q(
        &db,
        "CREATE TABLE bench_scan (id INT4, name TEXT, score FLOAT8)",
    );
    for i in 0..5000 {
        q(
            &db,
            &format!(
                "INSERT INTO bench_scan VALUES ({i}, 'row-{i}', {})",
                i as f64 * 1.5
            ),
        );
    }
    let mut group = c.benchmark_group("full_scan");
    group.bench_function("count_5000_rows", |b| {
        b.iter(|| execute_query("SELECT COUNT(*) FROM bench_scan", db.clone()).unwrap());
    });
    group.finish();
}

/// Prism (Phase 7) HNSW k-NN latency via `vector_search`, against a
/// deterministic pseudo-random 3D vector set.
fn bench_vector_knn(c: &mut Criterion) {
    let (db, _bucket, _local) = test_db();
    q(&db, "CREATE TABLE bench_vec (id INT4, embedding VECTOR)");
    for i in 0..2000 {
        let (x, y, z) = pseudo_vec3(i);
        q(
            &db,
            &format!("INSERT INTO bench_vec VALUES ({i}, '[{x},{y},{z}]')"),
        );
    }
    q(
        &db,
        "CREATE INDEX ON bench_vec USING VECTOR (embedding) WITH (metric = 'l2')",
    );

    let mut group = c.benchmark_group("vector_knn");
    group.bench_function("knn_10_of_2000", |b| {
        b.iter(|| {
            execute_query(
                "SELECT * FROM vector_search('bench_vec', 'embedding', '[0.5,0.5,0.5]', 10)",
                db.clone(),
            )
            .unwrap()
        });
    });
    group.finish();
}

/// Canopy (Phase 10) BM25 full-text ranking latency via the new
/// `Database::fts_search_bm25` path this session added.
fn bench_bm25_search(c: &mut Criterion) {
    let (db, _bucket, _local) = test_db();
    q(&db, "CREATE TABLE bench_fts (id INT4, body TEXT)");
    let words = [
        "rust",
        "systems",
        "programming",
        "database",
        "engine",
        "storage",
        "query",
        "index",
    ];
    for i in 0..2000 {
        let body = (0..12)
            .map(|j| words[(i * 7 + j) % words.len()])
            .collect::<Vec<_>>()
            .join(" ");
        q(
            &db,
            &format!("INSERT INTO bench_fts VALUES ({i}, '{body}')"),
        );
    }
    q(&db, "CREATE INDEX ON bench_fts USING GIN (body)");

    let mut group = c.benchmark_group("bm25_search");
    group.bench_function("bm25_top10_of_2000", |b| {
        b.iter(|| {
            db.fts_search_bm25("bench_fts", "body", "rust database engine", 10)
                .unwrap()
        });
    });
    group.finish();
}

/// Cheap deterministic pseudo-random unit-ish 3-vector, no external `rand`
/// dependency needed in the bench binary — same "small linear congruential
/// generator" shape as elsewhere in this codebase when a seedable sequence
/// is all that's needed.
fn pseudo_vec3(seed: usize) -> (f64, f64, f64) {
    let mut s = seed as u64 * 2654435761 + 1;
    let mut next = || {
        s = s
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((s >> 33) as f64 / u32::MAX as f64) * 2.0 - 1.0
    };
    (next(), next(), next())
}

criterion_group!(
    benches,
    bench_insert_throughput,
    bench_point_select,
    bench_full_scan,
    bench_vector_knn,
    bench_bm25_search,
);
criterion_main!(benches);
