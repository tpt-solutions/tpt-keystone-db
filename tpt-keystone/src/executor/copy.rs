//! Helpers for the `COPY ... FROM STDIN` / `COPY ... TO STDOUT` wire
//! sub-protocol. The actual `CopyData`/`CopyDone`/`CopyFail` message
//! exchange lives in `wire::session` (it needs direct access to the
//! connection), so this module only handles the SQL-level pieces: mapping a
//! COPY column list to schema positions, and encoding/decoding rows in
//! Postgres's default COPY text format (tab-delimited, `\N` for NULL,
//! `\t`/`\n`/`\r`/`\\` backslash-escaped).

use std::sync::Arc;

use super::parse_rows;
use crate::storage::database::Database;
use crate::storage::{StorageEngine, TableSchema};

/// Resolve a COPY column list to positions in `schema.columns`, defaulting
/// to every column in schema order when the statement didn't specify one.
pub fn target_columns(schema: &TableSchema, columns: &[String]) -> anyhow::Result<Vec<usize>> {
    if columns.is_empty() {
        return Ok((0..schema.columns.len()).collect());
    }
    columns
        .iter()
        .map(|name| {
            schema
                .columns
                .iter()
                .position(|c| &c.name == name)
                .ok_or_else(|| anyhow::anyhow!("column \"{name}\" does not exist"))
        })
        .collect()
}

/// Decode one COPY text-format line and write it as a row of `table`, using
/// the same length-prefixed cell encoding `INSERT` uses (see
/// `executor::parse_rows`). Columns not present in `target` are stored NULL.
pub fn insert_copy_line(
    db: &Arc<Database>,
    table: &str,
    schema: &TableSchema,
    target: &[usize],
    line: &str,
) -> anyhow::Result<()> {
    let raw_cells: Vec<Option<Vec<u8>>> = line.split('\t').map(decode_copy_cell).collect();
    if raw_cells.len() != target.len() {
        anyhow::bail!(
            "COPY: expected {} columns, got {}",
            target.len(),
            raw_cells.len()
        );
    }

    let mut cells: Vec<Option<Vec<u8>>> = vec![None; schema.columns.len()];
    for (cell, &col_idx) in raw_cells.into_iter().zip(target) {
        cells[col_idx] = cell;
    }

    let pk_idx = schema.pk_columns.first().copied().unwrap_or(0);
    let pk_bytes = cells.get(pk_idx).cloned().flatten().unwrap_or_default();

    let mut value_buf = Vec::new();
    for cell in &cells {
        match cell {
            Some(data) => {
                value_buf.extend_from_slice(&(data.len() as u32).to_be_bytes());
                value_buf.extend_from_slice(data);
            }
            None => value_buf.extend_from_slice(&(-1i32).to_be_bytes()),
        }
    }

    db.write(table, &pk_bytes, &value_buf)?;
    Ok(())
}

fn decode_copy_cell(raw: &str) -> Option<Vec<u8>> {
    if raw == "\\N" {
        return None;
    }
    let mut out = Vec::with_capacity(raw.len());
    let mut chars = raw.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('t') => out.push(b'\t'),
                Some('n') => out.push(b'\n'),
                Some('r') => out.push(b'\r'),
                Some('\\') => out.push(b'\\'),
                Some(other) => out.extend(other.to_string().as_bytes()),
                None => {}
            }
        } else {
            let mut buf = [0u8; 4];
            out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
        }
    }
    Some(out)
}

/// Scan `table` and project each row down to `target` columns, ready for
/// `encode_copy_line`.
pub fn scan_for_copy(
    db: &Arc<Database>,
    table: &str,
    target: &[usize],
) -> anyhow::Result<Vec<Vec<Option<Vec<u8>>>>> {
    let kvs = db.scan(table)?;
    let schema = db.get_table(table)?;
    let rows = parse_rows(&kvs, &schema);
    Ok(rows
        .into_iter()
        .map(|row| {
            target
                .iter()
                .map(|&i| row.get(i).cloned().flatten())
                .collect()
        })
        .collect())
}

/// Encode one row as a COPY text-format line (tab-delimited, `\N` for NULL).
pub fn encode_copy_line(row: &[Option<Vec<u8>>]) -> Vec<u8> {
    let mut out = Vec::new();
    for (i, cell) in row.iter().enumerate() {
        if i > 0 {
            out.push(b'\t');
        }
        match cell {
            None => out.extend_from_slice(b"\\N"),
            Some(data) => {
                for &b in data {
                    match b {
                        b'\t' => out.extend_from_slice(b"\\t"),
                        b'\n' => out.extend_from_slice(b"\\n"),
                        b'\r' => out.extend_from_slice(b"\\r"),
                        b'\\' => out.extend_from_slice(b"\\\\"),
                        _ => out.push(b),
                    }
                }
            }
        }
    }
    out.push(b'\n');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::config::NodeRole;
    use crate::storage::lease::LeaseManager;
    use crate::storage::objectstore::{LocalFsObjectStore, ObjectStore};
    use crate::storage::ColumnDef;
    use std::time::Duration;

    fn test_db() -> (Arc<Database>, tempfile::TempDir, tempfile::TempDir) {
        let bucket = tempfile::tempdir().unwrap();
        let local = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFsObjectStore::open(bucket.path()).unwrap());
        let lease = Arc::new(LeaseManager::new(
            store.clone(),
            "db",
            "node-1".into(),
            Duration::from_secs(30),
        ));
        lease.try_acquire().unwrap();
        let db = Arc::new(
            Database::open(
                local.path(),
                store,
                lease.handle(),
                NodeRole::Writer,
                Default::default(),
            )
            .unwrap(),
        );
        (db, bucket, local)
    }

    #[test]
    fn copy_in_then_copy_out_round_trip() {
        let (db, _b, _l) = test_db();
        db.create_table(
            "people",
            &[
                ColumnDef {
                    name: "id".into(),
                    col_type: crate::storage::ColumnType::Int4,
                    nullable: false,
                    default: None,
                    is_pk: true,
                },
                ColumnDef {
                    name: "name".into(),
                    col_type: crate::storage::ColumnType::Text,
                    nullable: true,
                    default: None,
                    is_pk: false,
                },
            ],
        )
        .unwrap();
        let schema = db.get_table("people").unwrap().unwrap();
        let target = target_columns(&schema, &[]).unwrap();
        insert_copy_line(&db, "people", &schema, &target, "1\tAlice").unwrap();
        insert_copy_line(&db, "people", &schema, &target, "2\t\\N").unwrap();

        let rows = scan_for_copy(&db, "people", &target).unwrap();
        assert_eq!(rows.len(), 2);
        let lines: Vec<String> = rows
            .iter()
            .map(|r| {
                String::from_utf8_lossy(&encode_copy_line(r))
                    .trim_end()
                    .to_string()
            })
            .collect();
        assert!(lines.contains(&"1\tAlice".to_string()));
        assert!(lines.contains(&"2\t\\N".to_string()));
    }

    #[test]
    fn target_columns_rejects_unknown_column() {
        let schema = TableSchema {
            name: "t".into(),
            columns: vec![ColumnDef {
                name: "a".into(),
                col_type: crate::storage::ColumnType::Int4,
                nullable: false,
                default: None,
                is_pk: true,
            }],
            pk_columns: vec![0],
            unique_groups: vec![],
            foreign_keys: vec![],
            json_schemas: vec![],
        };
        assert!(target_columns(&schema, &["nope".to_string()]).is_err());
    }

    #[test]
    fn target_columns_honors_explicit_column_order() {
        let schema = TableSchema {
            name: "t".into(),
            columns: vec![
                ColumnDef {
                    name: "a".into(),
                    col_type: crate::storage::ColumnType::Int4,
                    nullable: false,
                    default: None,
                    is_pk: true,
                },
                ColumnDef {
                    name: "b".into(),
                    col_type: crate::storage::ColumnType::Text,
                    nullable: true,
                    default: None,
                    is_pk: false,
                },
            ],
            pk_columns: vec![0],
            unique_groups: vec![],
            foreign_keys: vec![],
            json_schemas: vec![],
        };
        // COPY t (b, a) FROM STDIN — reversed order.
        let target = target_columns(&schema, &["b".to_string(), "a".to_string()]).unwrap();
        assert_eq!(target, vec![1, 0]);
    }
}
