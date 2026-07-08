//! `tpt export`/`tpt import` — the "export/import" half of `TODO.md`'s
//! single-binary CLI checklist item. Export reuses [`crate::format`];
//! import hand-parses CSV (no `csv` crate — the format is simple enough
//! and this project already hand-writes low-level parsers everywhere) or
//! JSON (`serde_json`, already a dependency) and issues one parameterized
//! `INSERT` per row over `query_params`.

use std::fs;
use std::path::Path;

use tpt_sdk::keystone::blocking::Client;
use tpt_sdk::keystone::Value;

use crate::format::{self, OutputFormat};

pub fn export(mut client: Client, table: &str, format: OutputFormat, output: Option<&Path>) -> anyhow::Result<()> {
    let sql = format!("SELECT * FROM {table}");
    let result = client.query(&sql).map_err(|e| anyhow::anyhow!("{e}"))?;

    match output {
        None => format::print_result(&result, format),
        Some(path) => {
            // Reuse the same rendering by capturing stdout would need extra
            // plumbing; simplest correct approach is to render directly here.
            let rendered = format::render_to_string(&result, format);
            fs::write(path, rendered)?;
            eprintln!("wrote {} row(s) to {}", result.rows.len(), path.display());
        }
    }
    Ok(())
}

/// Minimal CSV parser: handles quoted fields (with embedded commas/
/// newlines) and `""` as an escaped quote. Not a full RFC 4180
/// implementation (no configurable delimiter/dialect) — matches the scope
/// this CLI needs for round-tripping `tpt export --format csv` output.
fn parse_csv(content: &str) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    let mut row = Vec::new();
    let mut field = String::new();
    let mut in_quotes = false;
    let mut chars = content.chars().peekable();

    while let Some(c) = chars.next() {
        if in_quotes {
            if c == '"' {
                if chars.peek() == Some(&'"') {
                    field.push('"');
                    chars.next();
                } else {
                    in_quotes = false;
                }
            } else {
                field.push(c);
            }
        } else {
            match c {
                '"' => in_quotes = true,
                ',' => row.push(std::mem::take(&mut field)),
                '\r' => {}
                '\n' => {
                    row.push(std::mem::take(&mut field));
                    rows.push(std::mem::take(&mut row));
                }
                _ => field.push(c),
            }
        }
    }
    if !field.is_empty() || !row.is_empty() {
        row.push(field);
        rows.push(row);
    }
    rows.retain(|r| !(r.len() == 1 && r[0].is_empty()));
    rows
}

pub fn import(mut client: Client, table: &str, file: &Path, format: OutputFormat) -> anyhow::Result<()> {
    let content = fs::read_to_string(file)?;

    let (columns, data_rows): (Vec<String>, Vec<Vec<Value>>) = match format {
        OutputFormat::Csv => {
            let mut rows = parse_csv(&content);
            anyhow::ensure!(!rows.is_empty(), "empty CSV file");
            let columns = rows.remove(0);
            let data = rows.into_iter().map(|r| r.into_iter().map(|c| Value::from(c.as_str())).collect()).collect();
            (columns, data)
        }
        OutputFormat::Json => {
            let parsed: Vec<serde_json::Map<String, serde_json::Value>> = serde_json::from_str(&content)?;
            anyhow::ensure!(!parsed.is_empty(), "empty JSON array");
            let columns: Vec<String> = parsed[0].keys().cloned().collect();
            let data = parsed
                .into_iter()
                .map(|obj| columns.iter().map(|c| json_to_value(obj.get(c))).collect())
                .collect();
            (columns, data)
        }
        OutputFormat::Table => anyhow::bail!("import does not support the 'table' format; use csv or json"),
    };

    let col_list = columns.join(", ");
    let placeholders: Vec<String> = (1..=columns.len()).map(|i| format!("${i}")).collect();
    let sql = format!("INSERT INTO {table} ({col_list}) VALUES ({})", placeholders.join(", "));

    let mut count = 0usize;
    for row in &data_rows {
        client.query_params(&sql, row).map_err(|e| anyhow::anyhow!("row {}: {e}", count + 1))?;
        count += 1;
    }
    println!("imported {count} row(s) into {table}");
    Ok(())
}

fn json_to_value(v: Option<&serde_json::Value>) -> Value {
    match v {
        None | Some(serde_json::Value::Null) => Value::Null,
        Some(serde_json::Value::Bool(b)) => Value::Bool(*b),
        Some(serde_json::Value::Number(n)) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else {
                Value::Float(n.as_f64().unwrap_or_default())
            }
        }
        Some(serde_json::Value::String(s)) => Value::Text(s.clone()),
        Some(other) => Value::Text(other.to_string()),
    }
}
