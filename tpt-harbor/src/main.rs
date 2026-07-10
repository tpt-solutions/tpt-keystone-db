//! `tpt-harbor` CLI — `discover / validate / transfer / replicate / verify
//! / cutover`, per TODO.md Phase 15's checklist. `transfer` covers both
//! the Snapshot phase (DDL + bulk copy); `replicate` is the separate
//! live-CDC phase, meant to run after `transfer` (typically as a
//! long-lived process until `cutover`).

use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;
use tpt_harbor::connector::{SourceConnector, TargetConnector};
use tpt_harbor::dashboard::{self, StatusHandle};
use tpt_harbor::engine::MigrationEngine;
use tpt_harbor::sources::{
    elasticsearch::ElasticsearchSource, influxdb::InfluxDbSource, kafka::KafkaSource,
    mongodb::MongoSource, mssql::MsSqlSource, mysql::MySqlSource, neo4j::Neo4jSource,
    oracle::OracleSource, postgres::PostgresSource, postgis::PostGisSource,
    vector::VectorSource, SourceKind,
};
use tpt_harbor::target::keystone::KeystoneTarget;

#[derive(Parser)]
#[command(name = "tpt-harbor", about = "Universal data migration platform for TPT engines")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Args)]
struct SourceArgs {
    /// Which source connector to use. Only `postgres` is implemented; the
    /// rest report "not yet implemented" (see `sources::SourceKind`).
    #[arg(long, value_enum, default_value = "postgres")]
    source: SourceKind,
    /// Source connection address, e.g. `127.0.0.1:5432`.
    #[arg(long)]
    source_addr: String,
    #[arg(long, default_value = "postgres")]
    source_user: String,
    #[arg(long, default_value = "postgres")]
    source_db: String,
}

#[derive(Args)]
struct TargetArgs {
    /// Keystone target connection address, e.g. `127.0.0.1:5432`.
    #[arg(long)]
    target_addr: String,
}

#[derive(Args)]
struct CheckpointArgs {
    #[arg(long, default_value = "./tpt-harbor-checkpoint.json")]
    checkpoint: PathBuf,
}

#[derive(Args)]
struct DashboardArgs {
    /// If set, serves a live read-only migration-progress dashboard at
    /// `http://<addr>/` for the duration of this command (e.g.
    /// `127.0.0.1:5436`). Not persisted anywhere — the dashboard exists
    /// only while this process is running.
    #[arg(long)]
    dashboard_addr: Option<String>,
}

/// Spawns the dashboard HTTP server if `--dashboard-addr` was given,
/// printing where to find it. Returns a `StatusHandle` regardless (a no-op
/// handle nobody's polling if no address was given) so callers don't need
/// to special-case "no dashboard" at every progress-reporting call site.
fn maybe_start_dashboard(args: &DashboardArgs) -> StatusHandle {
    let status = StatusHandle::new();
    if let Some(addr) = &args.dashboard_addr {
        let addr = addr.clone();
        let status = status.clone();
        tokio::spawn(async move {
            if let Err(e) = dashboard::serve(&addr, status).await {
                eprintln!("dashboard server on {addr} failed: {e}");
            }
        });
        println!("Dashboard: http://{}/", args.dashboard_addr.as_ref().unwrap());
    }
    status
}

#[derive(Subcommand)]
enum Command {
    /// Discover phase: list source tables and their translated schema.
    Discover {
        #[command(flatten)]
        source: SourceArgs,
    },
    /// Validate (dry run) phase: print the Keystone DDL each source table
    /// would translate to, without executing anything.
    Validate {
        #[command(flatten)]
        source: SourceArgs,
    },
    /// Snapshot phase: create tables on the target and bulk-copy all rows.
    Transfer {
        #[command(flatten)]
        source: SourceArgs,
        #[command(flatten)]
        target: TargetArgs,
        #[command(flatten)]
        checkpoint: CheckpointArgs,
        #[command(flatten)]
        dashboard: DashboardArgs,
    },
    /// Replicate phase: stream live changes from the source to the target
    /// until interrupted (Ctrl-C) — resumable via the checkpoint file.
    Replicate {
        #[command(flatten)]
        source: SourceArgs,
        #[command(flatten)]
        target: TargetArgs,
        #[command(flatten)]
        checkpoint: CheckpointArgs,
        #[command(flatten)]
        dashboard: DashboardArgs,
    },
    /// Verify phase: compare per-row checksums between source and target.
    Verify {
        #[command(flatten)]
        source: SourceArgs,
        #[command(flatten)]
        target: TargetArgs,
        #[command(flatten)]
        checkpoint: CheckpointArgs,
        #[command(flatten)]
        dashboard: DashboardArgs,
    },
    /// Cutover phase: confirm verification passed and mark the migration done.
    Cutover {
        #[command(flatten)]
        source: SourceArgs,
        #[command(flatten)]
        target: TargetArgs,
        #[command(flatten)]
        checkpoint: CheckpointArgs,
        #[command(flatten)]
        dashboard: DashboardArgs,
    },
}

async fn open_source(args: &SourceArgs) -> anyhow::Result<Box<dyn SourceConnector>> {
    match args.source {
        SourceKind::Postgres => {
            let params = [("user", args.source_user.as_str()), ("database", args.source_db.as_str())];
            Ok(Box::new(PostgresSource::connect(&args.source_addr, &params).await?))
        }
        SourceKind::Gis => {
            let params = [("user", args.source_user.as_str()), ("database", args.source_db.as_str())];
            Ok(Box::new(PostGisSource::connect(&args.source_addr, &params).await?))
        }
        SourceKind::MySql => {
            Ok(Box::new(MySqlSource::connect(&args.source_addr, &args.source_user, &args.source_db).await?))
        }
        SourceKind::MsSql => {
            Ok(Box::new(MsSqlSource::connect(&args.source_addr, &args.source_user, &args.source_db).await?))
        }
        SourceKind::Mongo => {
            Ok(Box::new(MongoSource::connect(&args.source_addr, &args.source_db).await?))
        }
        SourceKind::Graph => {
            Ok(Box::new(Neo4jSource::connect(&args.source_addr).await?))
        }
        SourceKind::TimeSeries => {
            Ok(Box::new(InfluxDbSource::connect(&args.source_addr, &args.source_db).await?))
        }
        SourceKind::Stream => {
            Ok(Box::new(KafkaSource::connect(&args.source_addr).await?))
        }
        SourceKind::Vector => {
            Ok(Box::new(VectorSource::connect(&args.source_addr, &args.source_db).await?))
        }
        SourceKind::Search => {
            Ok(Box::new(ElasticsearchSource::connect(&args.source_addr).await?))
        }
        SourceKind::Oracle => {
            Ok(Box::new(OracleSource::connect(&args.source_addr, &args.source_user, &args.source_db).await?))
        }
    }
}

async fn open_target(args: &TargetArgs) -> anyhow::Result<Box<dyn TargetConnector>> {
    Ok(Box::new(KeystoneTarget::connect(&args.target_addr).await?))
}

async fn build_engine(source: &SourceArgs, target: &TargetArgs, checkpoint: &CheckpointArgs) -> anyhow::Result<MigrationEngine> {
    let src = open_source(source).await?;
    let tgt = open_target(target).await?;
    MigrationEngine::new(src, tgt, checkpoint.checkpoint.clone())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Discover { source } => {
            let mut src = open_source(&source).await?;
            let tables = src.discover().await?;
            for t in &tables {
                println!("{} ({} columns, pk: {:?})", t.qualified_name(), t.columns.len(), t.primary_key_columns());
            }
            println!("\n{} table(s) discovered.", tables.len());
        }

        Command::Validate { source } => {
            let mut src = open_source(&source).await?;
            let tables = src.discover().await?;
            for ddl in tables.iter().map(|t| t.to_keystone_ddl()) {
                println!("{ddl};\n");
            }
        }

        Command::Transfer { source, target, checkpoint, dashboard } => {
            let status = maybe_start_dashboard(&dashboard);
            let mut engine = build_engine(&source, &target, &checkpoint).await?;
            status.set_phase("discover");
            let tables = engine.discover().await?;
            status.set_phase("snapshot");
            status.set_tables(&tables.iter().map(|t| t.qualified_name()).collect::<Vec<_>>());
            let result = engine
                .snapshot(&tables, |table, rows| {
                    status.record_progress(table, rows);
                    println!("[{table}] {rows} rows copied");
                })
                .await;
            if let Err(e) = &result {
                status.set_phase("failed");
                status.set_error(e);
            } else {
                for t in &tables {
                    status.mark_table_done(&t.qualified_name());
                }
                status.set_phase("done");
            }
            result?;
            println!("Transfer complete: {} table(s).", tables.len());
        }

        Command::Replicate { source, target, checkpoint, dashboard } => {
            let status = maybe_start_dashboard(&dashboard);
            let mut engine = build_engine(&source, &target, &checkpoint).await?;
            status.set_phase("discover");
            let tables = engine.discover().await?;
            status.set_phase("replicate");
            status.set_tables(&tables.iter().map(|t| t.qualified_name()).collect::<Vec<_>>());
            println!("Replicating {} table(s); Ctrl-C to stop.", tables.len());
            let stop = tokio::signal::ctrl_c();
            tokio::select! {
                result = engine.replicate(&tables, || false) => {
                    if let Err(e) = &result { status.set_phase("failed"); status.set_error(e); }
                    result?;
                }
                _ = stop => {
                    status.set_phase("stopped");
                    println!("stop requested");
                }
            }
        }

        Command::Verify { source, target, checkpoint, dashboard } => {
            let status = maybe_start_dashboard(&dashboard);
            let mut engine = build_engine(&source, &target, &checkpoint).await?;
            status.set_phase("discover");
            let tables = engine.discover().await?;
            status.set_phase("verify");
            status.set_tables(&tables.iter().map(|t| t.qualified_name()).collect::<Vec<_>>());
            let results = engine.verify(&tables).await?;
            status.set_verifications(&results);
            let mut all_passed = true;
            for r in &results {
                all_passed &= r.passed;
                println!(
                    "{}: source={} target={} mismatched={} {}",
                    r.table,
                    r.source_row_count,
                    r.target_row_count,
                    r.mismatched_rows,
                    if r.passed { "PASS" } else { "FAIL" }
                );
            }
            status.set_phase(if all_passed { "done" } else { "failed" });
            if !all_passed {
                status.set_error("verification failed for one or more tables");
                anyhow::bail!("verification failed for one or more tables");
            }
        }

        Command::Cutover { source, target, checkpoint, dashboard } => {
            let status = maybe_start_dashboard(&dashboard);
            let mut engine = build_engine(&source, &target, &checkpoint).await?;
            status.set_phase("discover");
            let tables = engine.discover().await?;
            status.set_phase("cutover");
            status.set_tables(&tables.iter().map(|t| t.qualified_name()).collect::<Vec<_>>());
            let results = engine.verify(&tables).await?;
            status.set_verifications(&results);
            let ok = engine.cutover(&results)?;
            status.set_phase(if ok { "done" } else { "failed" });
            if ok {
                println!("Cutover approved: all tables verified. Redirect application traffic to the target now.");
            } else {
                status.set_error("cutover blocked: verification did not pass for all tables");
                anyhow::bail!("cutover blocked: verification did not pass for all tables");
            }
        }
    }

    Ok(())
}
