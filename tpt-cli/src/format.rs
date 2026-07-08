//! Renders a [`QueryResult`] as a table (psql-style), JSON, or CSV — the
//! three formats `tpt query`/`tpt export` support per `TODO.md`'s
//! "output JSON/CSV/table" checklist item.

use std::str::FromStr;

use tpt_sdk::keystone::{QueryResult, Row, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Table,
    Json,
    Csv,
}

impl FromStr for OutputFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "table" => Ok(OutputFormat::Table),
            "json" => Ok(OutputFormat::Json),
            "csv" => Ok(OutputFormat::Csv),
            other => Err(format!("unknown format '{other}' (expected table|json|csv)")),
        }
    }
}

pub fn print_result(result: &QueryResult, format: OutputFormat) {
    print!("{}", render_to_string(result, format));
}

pub fn render_to_string(result: &QueryResult, format: OutputFormat) -> String {
    match format {
        OutputFormat::Table => render_table(result),
        OutputFormat::Json => render_json(result),
        OutputFormat::Csv => render_csv(result),
    }
}

fn cell_text(row: &Row, i: usize) -> String {
    row.get_str(i).map(str::to_string).unwrap_or_else(|| "NULL".to_string())
}

fn render_table(result: &QueryResult) -> String {
    if result.columns.is_empty() {
        return match &result.command_tag {
            Some(tag) => format!("{tag}\n"),
            None => String::new(),
        };
    }

    let mut widths: Vec<usize> = result.columns.iter().map(|c| c.len()).collect();
    for row in &result.rows {
        for (i, w) in widths.iter_mut().enumerate() {
            *w = (*w).max(cell_text(row, i).len());
        }
    }

    let sep: String = widths.iter().map(|w| "-".repeat(w + 2)).collect::<Vec<_>>().join("+");
    let header: String = result
        .columns
        .iter()
        .zip(&widths)
        .map(|(c, w)| format!(" {c:<w$} "))
        .collect::<Vec<_>>()
        .join("|");

    let mut out = String::new();
    out.push_str(&header);
    out.push('\n');
    out.push_str(&sep);
    out.push('\n');
    for row in &result.rows {
        let line: String = widths
            .iter()
            .enumerate()
            .map(|(i, w)| format!(" {:<w$} ", cell_text(row, i), w = w))
            .collect::<Vec<_>>()
            .join("|");
        out.push_str(&line);
        out.push('\n');
    }

    let n = result.rows.len();
    out.push_str(&format!("({n} row{})\n", if n == 1 { "" } else { "s" }));
    out
}

fn value_to_json(v: Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(b),
        Value::Int(i) => serde_json::Value::Number(i.into()),
        Value::Float(f) => serde_json::Number::from_f64(f).map(serde_json::Value::Number).unwrap_or(serde_json::Value::Null),
        Value::Text(s) => serde_json::Value::String(s),
    }
}

fn render_json(result: &QueryResult) -> String {
    let rows: Vec<serde_json::Value> = result
        .rows
        .iter()
        .map(|row| {
            let mut obj = serde_json::Map::new();
            for (i, col) in result.columns.iter().enumerate() {
                obj.insert(col.clone(), value_to_json(row.get_value(i)));
            }
            serde_json::Value::Object(obj)
        })
        .collect();
    let mut s = serde_json::to_string_pretty(&rows).expect("json rows are always serializable");
    s.push('\n');
    s
}

fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

fn render_csv(result: &QueryResult) -> String {
    let mut out = String::new();
    out.push_str(&result.columns.iter().map(|c| csv_escape(c)).collect::<Vec<_>>().join(","));
    out.push('\n');
    for row in &result.rows {
        let line: String = (0..result.columns.len())
            .map(|i| csv_escape(row.get_str(i).unwrap_or("")))
            .collect::<Vec<_>>()
            .join(",");
        out.push_str(&line);
        out.push('\n');
    }
    out
}
