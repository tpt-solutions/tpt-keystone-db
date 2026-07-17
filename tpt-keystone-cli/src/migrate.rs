//! `tpt migrate` — schema migration tooling per `TODO.md`'s checklist item.
//!
//! Migrations are plain `.sql` files in a directory, applied in filename
//! order (e.g. `0001_init.sql`, `0002_add_users.sql`). Applied migrations
//! are tracked in a `_tpt_migrations(id TEXT PRIMARY KEY, applied_at TEXT)`
//! table created on first use. Each migration file runs inside its own
//! `BEGIN`/`COMMIT` — the whole file is one simple-query call so a
//! multi-statement migration is atomic; a failed one rolls back and stops
//! `up` before applying anything after it.

use std::collections::HashSet;
use std::fs;
use std::path::Path;

use tpt_sdk::keystone::blocking::Client;
use tpt_sdk::keystone::Value;

const TRACKING_TABLE_DDL: &str =
    "CREATE TABLE IF NOT EXISTS _tpt_migrations (id TEXT PRIMARY KEY, applied_at TEXT)";

fn ensure_tracking_table(client: &mut Client) -> anyhow::Result<()> {
    client.query(TRACKING_TABLE_DDL).map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

fn applied_ids(client: &mut Client) -> anyhow::Result<HashSet<String>> {
    let result = client.query("SELECT id FROM _tpt_migrations").map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(result.rows.iter().filter_map(|r| r.get_str(0).map(str::to_string)).collect())
}

fn migration_files(dir: &Path) -> anyhow::Result<Vec<(String, String)>> {
    let mut files: Vec<(String, String)> = fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map(|ext| ext == "sql").unwrap_or(false))
        .map(|e| {
            let path = e.path();
            let id = path.file_stem().unwrap().to_string_lossy().into_owned();
            (id, path.to_string_lossy().into_owned())
        })
        .collect();
    files.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(files)
}

pub fn up(mut client: Client, dir: &Path) -> anyhow::Result<()> {
    ensure_tracking_table(&mut client)?;
    let applied = applied_ids(&mut client)?;

    for (id, path) in migration_files(dir)? {
        if applied.contains(&id) {
            continue;
        }
        let sql = fs::read_to_string(&path)?;

        client.query("BEGIN").map_err(|e| anyhow::anyhow!("{e}"))?;
        if let Err(e) = client.query(&sql) {
            let _ = client.query("ROLLBACK");
            anyhow::bail!("migration {id} failed: {e}");
        }
        if let Err(e) = client.query_params(
            "INSERT INTO _tpt_migrations (id, applied_at) VALUES ($1, now())",
            &[Value::from(id.as_str())],
        ) {
            let _ = client.query("ROLLBACK");
            anyhow::bail!("migration {id} failed to record: {e}");
        }
        client.query("COMMIT").map_err(|e| anyhow::anyhow!("{e}"))?;
        println!("applied {id}");
    }
    Ok(())
}

pub fn status(mut client: Client, dir: &Path) -> anyhow::Result<()> {
    ensure_tracking_table(&mut client)?;
    let applied = applied_ids(&mut client)?;

    for (id, _) in migration_files(dir)? {
        let mark = if applied.contains(&id) { "applied" } else { "pending" };
        println!("{mark:<8} {id}");
    }
    Ok(())
}
