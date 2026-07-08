//! `tpt schema [table]` — schema introspection, the third piece of
//! `TODO.md`'s single-binary CLI checklist item. With no table given,
//! lists tables in the `public` schema; with one, describes its columns.

use tpt_sdk::keystone::blocking::Client;
use tpt_sdk::keystone::Value;

use crate::format::{self, OutputFormat};

pub fn run(mut client: Client, table: Option<&str>) -> anyhow::Result<()> {
    let sql = match table {
        None => "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name".to_string(),
        Some(_) => {
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns \
             WHERE table_name = $1 ORDER BY ordinal_position"
                .to_string()
        }
    };

    let result = match table {
        None => client.query(&sql),
        Some(t) => client.query_params(&sql, &[Value::from(t)]),
    }
    .map_err(|e| anyhow::anyhow!("{e}"))?;

    if let Some(t) = table {
        anyhow::ensure!(!result.rows.is_empty(), "table '{t}' not found (or has no columns)");
    }

    format::print_result(&result, OutputFormat::Table);
    Ok(())
}
