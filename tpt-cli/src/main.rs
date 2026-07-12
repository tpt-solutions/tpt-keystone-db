//! `tpt` — single-binary CLI for TPT Keystone (`TODO.md`'s "SDK/CLI"
//! checklist): interactive REPL, `tpt query`, `tpt stream`, `tpt migrate`,
//! plus `tpt export`/`tpt import`/`tpt schema` (export/import and schema
//! introspection, called out in the same checklist item's description).
//!
//! Talks to Keystone via [`tpt_sdk::keystone::blocking::Client`] — the
//! synchronous wrapper `tpt-sdk` already documents as built for exactly
//! this ("a plain non-async ... CLI tool that never touches Tokio
//! directly"). `tpt stream` talks to the separate Flux WebSocket bridge
//! (default port 5434) via a small hand-rolled client in `stream.rs`.

mod data;
mod format;
mod migrate;
mod repl;
mod schema;
mod stream;

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use tpt_sdk::keystone::blocking::Client;

use format::OutputFormat;

#[derive(Parser)]
#[command(name = "tpt", version, about = "TPT Keystone command-line client")]
struct Cli {
    /// Keystone host
    #[arg(long, global = true, default_value = "127.0.0.1")]
    host: String,

    /// Keystone Postgres-wire port
    #[arg(long, global = true, default_value_t = 5432)]
    port: u16,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Launch the interactive REPL (also the default with no subcommand)
    Repl,
    /// Execute one SQL statement and print the result
    Query {
        sql: String,
        #[arg(long, default_value = "table")]
        format: OutputFormat,
        /// Read SQL from this file instead of the `sql` argument
        #[arg(short = 'f', long, conflicts_with = "sql")]
        file: Option<PathBuf>,
    },
    /// Tail a Flux event stream topic in real time
    Stream {
        topic: String,
        /// Flux WebSocket bridge port (separate from the Postgres-wire port)
        #[arg(long, default_value_t = 5434)]
        flux_port: u16,
    },
    /// Schema migration tooling
    Migrate {
        #[command(subcommand)]
        action: MigrateAction,
    },
    /// Export a table's rows
    Export {
        table: String,
        #[arg(long, default_value = "csv")]
        format: OutputFormat,
        /// Write to this file instead of stdout
        #[arg(short = 'o', long)]
        output: Option<PathBuf>,
    },
    /// Import rows into a table from a CSV or JSON file
    Import {
        table: String,
        #[arg(short = 'f', long)]
        file: PathBuf,
        #[arg(long, default_value = "csv")]
        format: OutputFormat,
    },
    /// Show tables, or describe one table's columns
    Schema { table: Option<String> },
}

#[derive(Subcommand)]
enum MigrateAction {
    /// Apply all pending migrations in `dir`
    Up {
        #[arg(long, default_value = "migrations")]
        dir: PathBuf,
    },
    /// Show which migrations in `dir` are applied vs. pending
    Status {
        #[arg(long, default_value = "migrations")]
        dir: PathBuf,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let addr = format!("{}:{}", cli.host, cli.port);

    match cli.command.unwrap_or(Command::Repl) {
        Command::Repl => {
            let client = connect(&addr)?;
            repl::run(client, &addr)?;
        }
        Command::Query { sql, format, file } => {
            let mut client = connect(&addr)?;
            let sql = match file {
                Some(path) => std::fs::read_to_string(path)?,
                None => sql,
            };
            let result = client.query(&sql).map_err(|e| anyhow::anyhow!("{e}"))?;
            format::print_result(&result, format);
        }
        Command::Stream { topic, flux_port } => {
            stream::run(&cli.host, flux_port, &topic)?;
        }
        Command::Migrate { action } => match action {
            MigrateAction::Up { dir } => migrate::up(connect(&addr)?, &dir)?,
            MigrateAction::Status { dir } => migrate::status(connect(&addr)?, &dir)?,
        },
        Command::Export { table, format, output } => {
            data::export(connect(&addr)?, &table, format, output.as_deref())?;
        }
        Command::Import { table, file, format } => {
            data::import(connect(&addr)?, &table, &file, format)?;
        }
        Command::Schema { table } => {
            schema::run(connect(&addr)?, table.as_deref())?;
        }
    }

    Ok(())
}

fn connect(addr: &str) -> anyhow::Result<Client> {
    Client::connect(addr).map_err(|e| anyhow::anyhow!("failed to connect to {addr}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_localhost_5432() {
        let cli = Cli::try_parse_from(["tpt"]).unwrap();
        assert_eq!(format!("{}:{}", cli.host, cli.port), "127.0.0.1:5432");
        assert!(matches!(cli.command, Some(Command::Repl) | None));
    }

    #[test]
    fn global_host_and_port_flags_apply() {
        let cli = Cli::try_parse_from(["tpt", "--host", "db.example", "--port", "6543", "query", "SELECT 1"]).unwrap();
        assert_eq!(cli.host, "db.example");
        assert_eq!(cli.port, 6543);
        match cli.command {
            Some(Command::Query { sql, .. }) => assert_eq!(sql, "SELECT 1"),
            _ => panic!("expected query command"),
        }
    }

    #[test]
    fn query_file_conflicts_with_sql() {
        let err = Cli::try_parse_from(["tpt", "query", "SELECT 1", "-f", "x.sql"]);
        assert!(err.is_err());
    }

    #[test]
    fn export_defaults_to_csv_and_stdout() {
        let cli = Cli::try_parse_from(["tpt", "export", "widgets"]).unwrap();
        match cli.command {
            Some(Command::Export { table, format, output }) => {
                assert_eq!(table, "widgets");
                assert_eq!(format, OutputFormat::Csv);
                assert!(output.is_none());
            }
            _ => panic!("expected export command"),
        }
    }

    #[test]
    fn migrate_status_default_dir() {
        let cli = Cli::try_parse_from(["tpt", "migrate", "status"]).unwrap();
        match cli.command {
            Some(Command::Migrate { action: MigrateAction::Status { dir } }) => {
                assert_eq!(dir, PathBuf::from("migrations"));
            }
            _ => panic!("expected migrate status command"),
        }
    }
}
