//! One-off manual smoke test: exercises the full discover -> validate ->
//! snapshot -> verify pipeline against two live Keystone nodes standing in
//! for a real source/target pair (Keystone speaks pgwire +
//! information_schema, same as real Postgres, per Phase 4). Not part of
//! the crate's test suite — run with
//! `cargo run --example smoke -- <source_addr> <target_addr>`.

use tpt_harbor::engine::MigrationEngine;
use tpt_harbor::pgwire::Client;
use tpt_harbor::sources::postgres::PostgresSource;
use tpt_harbor::target::keystone::KeystoneTarget;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let src_addr = args.next().unwrap_or_else(|| "127.0.0.1:55001".to_string());
    let dst_addr = args.next().unwrap_or_else(|| "127.0.0.1:55002".to_string());

    let mut setup = Client::connect(&src_addr, &[("user", "smoke")]).await?;
    setup.execute("CREATE TABLE IF NOT EXISTS smoke_users (id INTEGER, name TEXT, note TEXT)").await?;
    setup.execute("DELETE FROM smoke_users").await?;
    setup
        .execute("INSERT INTO smoke_users (id, name, note) VALUES (1, 'alice', 'has a note'), (2, 'bob', NULL), (3, 'o''brien', 'quote test')")
        .await?;
    println!("seeded {src_addr}/smoke_users with 3 rows");

    let src = PostgresSource::connect(&src_addr, &[("user", "smoke")]).await?;
    let target = KeystoneTarget::connect(&dst_addr).await?;
    let checkpoint_path = std::env::temp_dir().join("tpt-harbor-smoke-checkpoint.json");
    let _ = std::fs::remove_file(&checkpoint_path);
    let mut engine = MigrationEngine::new(Box::new(src), Box::new(target), checkpoint_path)?;

    let tables: Vec<_> = engine.discover().await.map_err(|e| anyhow::anyhow!(e))?.into_iter().filter(|t| t.name == "smoke_users").collect();
    println!("discovered {} table(s)", tables.len());
    for ddl in engine.validate(&tables) {
        println!("validate ddl:\n{ddl}");
    }

    engine
        .snapshot(&tables, |t, n| println!("[{t}] {n} rows copied"))
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    let results = engine.verify(&tables).await.map_err(|e| anyhow::anyhow!(e))?;
    for r in &results {
        println!(
            "verify {}: source={} target={} mismatched={} passed={}",
            r.table, r.source_row_count, r.target_row_count, r.mismatched_rows, r.passed
        );
    }
    assert!(results.iter().all(|r| r.passed), "verification did not pass");

    let ok = engine.cutover(&results)?;
    println!("cutover approved: {ok}");

    let mut check = Client::connect(&dst_addr, &[("user", "smoke")]).await?;
    let res = check.query("SELECT id, name, note FROM smoke_users ORDER BY id").await?;
    println!("target now has {} row(s):", res.rows.len());
    for row in &res.rows {
        println!("  id={:?} name={:?} note={:?}", row.get_str("id"), row.get_str("name"), row.get_str("note"));
    }
    assert_eq!(res.rows.len(), 3);
    assert_eq!(res.rows[2].get_str("name"), Some("o'brien"), "quote escaping round-trip failed");
    println!("SMOKE TEST PASSED");
    Ok(())
}
