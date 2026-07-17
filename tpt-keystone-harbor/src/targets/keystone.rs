//! Keystone target connector. Talks the same `pgwire::Client` Harbor/PG's
//! source side uses (see `src/pgwire.rs`'s module doc) since Keystone is
//! itself Postgres-wire-compatible.

use crate::connector::{ChangeEvent, ConnectorError, SourceRow, TargetConnector};
use crate::pgwire::Client;
use crate::schema::TableSchema;
use async_trait::async_trait;

const WRITE_BATCH_SIZE: usize = 500;

pub struct KeystoneTarget {
    client: Client,
}

impl KeystoneTarget {
    pub async fn connect(addr: &str) -> anyhow::Result<Self> {
        Ok(Self { client: Client::connect(addr, &[("user", "tpt_harbor")]).await? })
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Render one cell (already-decoded Postgres text-format bytes) as a SQL
/// literal. Everything is quoted as a string literal — Keystone's parser
/// coerces on INSERT, and this sidesteps per-type literal syntax
/// (booleans/numerics/timestamps all round-trip fine as quoted text).
fn cell_literal(cell: &Option<Vec<u8>>) -> String {
    match cell {
        None => "NULL".to_string(),
        Some(bytes) => {
            let s = String::from_utf8_lossy(bytes);
            format!("'{}'", s.replace('\'', "''"))
        }
    }
}

fn insert_sql(table: &TableSchema, rows: &[SourceRow]) -> String {
    let cols = table.columns.iter().map(|c| quote_ident(&c.name)).collect::<Vec<_>>().join(", ");
    let values = rows
        .iter()
        .map(|row| format!("({})", row.iter().map(cell_literal).collect::<Vec<_>>().join(", ")))
        .collect::<Vec<_>>()
        .join(", ");
    format!("INSERT INTO {} ({}) VALUES {}", quote_ident(&table.name), cols, values)
}

#[async_trait]
impl TargetConnector for KeystoneTarget {
    fn name(&self) -> &'static str {
        "Keystone"
    }

    async fn apply_ddl(&mut self, table: &TableSchema) -> Result<(), ConnectorError> {
        self.client.execute(&table.to_keystone_ddl()).await.map_err(ConnectorError::Other)
    }

    async fn write_rows(&mut self, table: &TableSchema, rows: Vec<SourceRow>) -> Result<(), ConnectorError> {
        for chunk in rows.chunks(WRITE_BATCH_SIZE) {
            if chunk.is_empty() {
                continue;
            }
            self.client.execute(&insert_sql(table, chunk)).await.map_err(ConnectorError::Other)?;
        }
        Ok(())
    }

    async fn apply_change(&mut self, table: &TableSchema, event: &ChangeEvent) -> Result<(), ConnectorError> {
        match event {
            ChangeEvent::Insert { row, .. } => {
                self.client.execute(&insert_sql(table, std::slice::from_ref(row))).await.map_err(ConnectorError::Other)?;
            }
            ChangeEvent::Update { key, row, .. } => {
                let sets = table
                    .columns
                    .iter()
                    .zip(row.iter())
                    .map(|(c, v)| format!("{} = {}", quote_ident(&c.name), cell_literal(v)))
                    .collect::<Vec<_>>()
                    .join(", ");
                let where_clause = where_from_key(table, key);
                self.client
                    .execute(&format!("UPDATE {} SET {} WHERE {}", quote_ident(&table.name), sets, where_clause))
                    .await
                    .map_err(ConnectorError::Other)?;
            }
            ChangeEvent::Delete { key, .. } => {
                let where_clause = where_from_key(table, key);
                self.client
                    .execute(&format!("DELETE FROM {} WHERE {}", quote_ident(&table.name), where_clause))
                    .await
                    .map_err(ConnectorError::Other)?;
            }
            ChangeEvent::CommitLsn(_) => {}
        }
        Ok(())
    }

    async fn row_checksums(&mut self, table: &TableSchema) -> Result<Vec<u64>, ConnectorError> {
        let pk = table.primary_key_columns();
        let order_by = if pk.is_empty() { String::new() } else { format!(" ORDER BY {}", pk.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ")) };
        let res = self
            .client
            .query(&format!("SELECT * FROM {}{}", quote_ident(&table.name), order_by))
            .await
            .map_err(ConnectorError::Other)?;
        Ok(res.rows.iter().map(|r| crate::verify::hash_row(&r.to_cell_vec())).collect())
    }
}

/// A CDC `key`/`row` from `pgoutput` is column-order-aligned with the full
/// table schema (replica identity "full" sends every column; the default
/// replica identity sends only PK columns padded with `None` elsewhere),
/// so matching on the table's declared PK columns by position is safe
/// either way as long as at least the PK columns are non-NULL.
fn where_from_key(table: &TableSchema, key: &SourceRow) -> String {
    let pk = table.primary_key_columns();
    let conditions: Vec<String> = if pk.is_empty() {
        table
            .columns
            .iter()
            .zip(key.iter())
            .filter(|(_, v)| v.is_some())
            .map(|(c, v)| format!("{} = {}", quote_ident(&c.name), cell_literal(v)))
            .collect()
    } else {
        pk.iter()
            .filter_map(|pk_col| {
                let idx = table.columns.iter().position(|c| c.name == *pk_col)?;
                let v = key.get(idx)?;
                Some(format!("{} = {}", quote_ident(pk_col), cell_literal(v)))
            })
            .collect()
    };
    if conditions.is_empty() {
        "1 = 0".to_string() // no identifiable key: safest is to match nothing rather than the whole table
    } else {
        conditions.join(" AND ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ColumnSchema;

    fn users_table() -> TableSchema {
        TableSchema {
            schema: "public".into(),
            name: "users".into(),
            columns: vec![
                ColumnSchema { name: "id".into(), source_type: "int4".into(), keystone_type: "INTEGER".into(), nullable: false, is_primary_key: true },
                ColumnSchema { name: "email".into(), source_type: "text".into(), keystone_type: "TEXT".into(), nullable: true, is_primary_key: false },
            ],
        }
    }

    #[test]
    fn insert_sql_quotes_and_escapes() {
        let rows = vec![vec![Some(b"1".to_vec()), Some(b"o'brien@example.com".to_vec())]];
        let sql = insert_sql(&users_table(), &rows);
        assert!(sql.contains("'o''brien@example.com'"));
        assert!(sql.starts_with("INSERT INTO \"users\" (\"id\", \"email\") VALUES"));
    }

    #[test]
    fn where_from_key_uses_primary_key_column() {
        let key = vec![Some(b"42".to_vec()), None];
        let clause = where_from_key(&users_table(), &key);
        assert_eq!(clause, "\"id\" = '42'");
    }
}
