//! Interactive REPL (`tpt` with no subcommand, or `tpt repl`) — the
//! "interactive REPL" half of `TODO.md`'s single-binary CLI checklist item.
//!
//! Scope cut: line editing is `std::io::stdin().read_line` — no history,
//! no arrow-key/readline-style editing, no multi-line statement buffering
//! beyond `;`-terminated single-line statements. Good enough for ad hoc
//! querying; not a `psql` replacement.

use std::io::{self, Write};

use tpt_sdk::keystone::blocking::Client;

use crate::format::{self, OutputFormat};

pub fn run(mut client: Client, addr: &str) -> anyhow::Result<()> {
    println!("tpt REPL — connected to {addr}");
    println!("Type SQL statements terminated by ';', or \\q to quit.");

    let mut buffer = String::new();
    loop {
        if buffer.is_empty() {
            print!("tpt=> ");
        } else {
            print!("tpt-> ");
        }
        io::stdout().flush()?;

        let mut line = String::new();
        let n = io::stdin().read_line(&mut line)?;
        if n == 0 {
            println!();
            break;
        }
        let trimmed = line.trim();

        if buffer.is_empty() {
            match trimmed {
                "\\q" | "\\quit" | "exit" | "quit" => break,
                "\\dt" => {
                    run_and_print(&mut client, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'");
                    continue;
                }
                _ => {}
            }
        }

        buffer.push_str(&line);
        if trimmed.ends_with(';') {
            let sql = buffer.trim().to_string();
            buffer.clear();
            if sql.is_empty() {
                continue;
            }
            run_and_print(&mut client, &sql);
        }
    }
    Ok(())
}

fn run_and_print(client: &mut Client, sql: &str) {
    match client.query(sql) {
        Ok(result) => format::print_result(&result, OutputFormat::Table),
        Err(e) => eprintln!("error: {e}"),
    }
}
