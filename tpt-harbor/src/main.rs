//! `tpt-harbor` CLI — `discover / validate / transfer / replicate / verify
//! / cutover`, per TODO.md Phase 15's checklist. `transfer` covers both
//! the Snapshot phase (DDL + bulk copy); `replicate` is the separate
//! live-CDC phase, meant to run after `transfer` (typically as a
//! long-lived process until `cutover`).

use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;
use tpt_harbor::connector::{SourceConnector, TargetConnector};
use tpt_harbor::engine::MigrationEngine;
use tpt_harbor::sources::{postgres::PostgresSource, SourceKind};
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
    },
    /// Verify phase: compare per-row checksums between source and target.
    Verify {
        #[command(flatten)]
        source: SourceArgs,
        #[command(flatten)]
        target: TargetArgs,
        #[command(flatten)]
        checkpoint: CheckpointArgs,
    },
    /// Cutover phase: confirm verification passed and mark the migration done.
    Cutover {
        #[command(flatten)]
        source: SourceArgs,
        #[command(flatten)]
        target: TargetArgs,
        #[command(flatten)]
        checkpoint: CheckpointArgs,
    },
}

async fn open_source(args: &SourceArgs) -> anyhow::Result<Box<dyn SourceConnector>> {
    match args.source {
        SourceKind::Postgres => {
            let params = [("user", args.source_user.as_str()), ("database", args.source_db.as_str())];
            Ok(Box::new(PostgresSource::connect(&args.source_addr, &params).await?))
        }
        other => anyhow::bail!("{:?} is not yet implemented; only --source postgres is (target engine would be {})", other, other.target_engine()),
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

        Command::Transfer { source, target, checkpoint } => {
            let mut engine = build_engine(&source, &target, &checkpoint).await?;
            let tables = engine.discover().await?;
            engine
                .snapshot(&tables, |table, rows| {
                    println!("[{table}] {rows} rows copied");
                })
                .await?;
            println!("Transfer complete: {} table(s).", tables.len());
        }

        Command::Replicate { source, target, checkpoint } => {
            let mut engine = build_engine(&source, &target, &checkpoint).await?;
            let tables = engine.discover().await?;
            println!("Replicating {} table(s); Ctrl-C to stop.", tables.len());
            let stop = tokio::signal::ctrl_c();
            tokio::select! {
                result = engine.replicate(&tables, || false) => { result?; }
                _ = stop => { println!("stop requested"); }
            }
        }

        Command::Verify { source, target, checkpoint } => {
            let mut engine = build_engine(&source, &target, &checkpoint).await?;
            let tables = engine.discover().await?;
            let results = engine.verify(&tables).await?;
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
            if !all_passed {
                anyhow::bail!("verification failed for one or more tables");
            }
        }

        Command::Cutover { source, target, checkpoint } => {
            let mut engine = build_engine(&source, &target, &checkpoint).await?;
            let tables = engine.discover().await?;
            let results = engine.verify(&tables).await?;
            let ok = engine.cutover(&results)?;
            if ok {
                println!("Cutover approved: all tables verified. Redirect application traffic to the target now.");
            } else {
                anyhow::bail!("cutover blocked: verification did not pass for all tables");
            }
        }
    }

    Ok(())
}
