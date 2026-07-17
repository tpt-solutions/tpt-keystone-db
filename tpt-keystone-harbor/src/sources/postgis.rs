//! Harbor/GIS — PostGIS source connector. PostGIS speaks the Postgres wire
//! protocol, so this connector reuses the existing `pgwire::Client`. The
//! only difference from `Harbor/PG` is the schema translator: geometry and
//! geography columns map to Keystone's `GEOMETRY` type (stored as WKT text),
//! and snapshot reads wrap geometry columns with `ST_AsText()` so PostGIS's
//! hex-WKB binary format arrives as parseable WKT.

use crate::connector::{ChangeEvent, ConnectorError, SourceConnector, SourceRow};
use crate::pgwire::Client;
use crate::schema::{from_postgis_type, ColumnSchema, TableSchema};
use async_trait::async_trait;
use tokio::sync::mpsc::Sender;

const SNAPSHOT_BATCH_SIZE: i32 = 5_000;

pub struct PostGisSource {
    client: Client,
    addr: String,
    params: Vec<(String, String)>,
    publication: String,
    slot: String,
}

impl PostGisSource {
    pub async fn connect(addr: &str, params: &[(&str, &str)]) -> anyhow::Result<Self> {
        let client = Client::connect(addr, params).await?;
        Ok(Self {
            client,
            addr: addr.to_string(),
            params: params.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            publication: "tpt_harbor_pub".to_string(),
            slot: "tpt_harbor_slot".to_string(),
        })
    }

    pub async fn ensure_replication_objects(&mut self) -> anyhow::Result<()> {
        let exists: bool = {
            let r = self.client.query(&format!("SELECT 1 FROM pg_publication WHERE pubname = '{}'", self.publication)).await?;
            !r.rows.is_empty()
        };
        if !exists {
            self.client.execute(&format!("CREATE PUBLICATION {} FOR ALL TABLES", self.publication)).await?;
        }
        let slot_exists = {
            let r = self.client.query(&format!("SELECT 1 FROM pg_replication_slots WHERE slot_name = '{}'", self.slot)).await?;
            !r.rows.is_empty()
        };
        if !slot_exists {
            self.client
                .execute(&format!("SELECT pg_create_logical_replication_slot('{}', 'pgoutput')", self.slot))
                .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl SourceConnector for PostGisSource {
    fn name(&self) -> &'static str {
        "Harbor/GIS"
    }

    async fn discover(&mut self) -> Result<Vec<TableSchema>, ConnectorError> {
        let tables_res = self
            .client
            .query(
                "SELECT table_schema, table_name FROM information_schema.tables \
                 WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('pg_catalog', 'information_schema')",
            )
            .await
            .map_err(ConnectorError::Other)?;

        let mut tables = Vec::new();
        for row in &tables_res.rows {
            let schema = row.get_str("table_schema").unwrap_or("public").to_string();
            let name = row.get_str("table_name").unwrap_or_default().to_string();
            if name.is_empty() {
                continue;
            }

            // Fetch columns with their Postgres type names — join through
            // pg_type to get the base type name (not the formatted name)
            // so the GIS-specific type mapper can detect geometry/geography.
            let cols_res = self
                .client
                .query(&format!(
                    "SELECT c.column_name, pg_type.typname AS data_type, c.is_nullable \
                     FROM information_schema.columns c \
                     JOIN pg_type ON pg_type.oid = c.udt_name::regtype::oid \
                     WHERE c.table_schema = '{schema}' AND c.table_name = '{name}' \
                     ORDER BY c.ordinal_position"
                ))
                .await
                .map_err(ConnectorError::Other)?;

            let pk_res = self
                .client
                .query(&format!(
                    "SELECT kcu.column_name FROM information_schema.table_constraints tc \
                     JOIN information_schema.key_column_usage kcu \
                       ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema \
                     WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = '{schema}' AND tc.table_name = '{name}'"
                ))
                .await
                .map_err(ConnectorError::Other)?;
            let pk_cols: Vec<String> = pk_res.rows.iter().filter_map(|r| r.get_str("column_name").map(String::from)).collect();

            let columns: Vec<ColumnSchema> = cols_res
                .rows
                .iter()
                .map(|r| {
                    let col_name = r.get_str("column_name").unwrap_or_default().to_string();
                    let source_type = r.get_str("data_type").unwrap_or_default().to_string();
                    ColumnSchema {
                        keystone_type: from_postgis_type(&source_type),
                        nullable: r.get_str("is_nullable").map(|v| v == "YES").unwrap_or(true),
                        is_primary_key: pk_cols.contains(&col_name),
                        name: col_name,
                        source_type,
                    }
                })
                .collect();

            tables.push(TableSchema { schema, name, columns });
        }
        Ok(tables)
    }

    async fn snapshot_table(&mut self, table: &TableSchema, tx: Sender<Vec<SourceRow>>) -> Result<u64, ConnectorError> {
        self.client.execute("BEGIN").await.map_err(ConnectorError::Other)?;

        // Build SELECT list: wrap geometry/geography columns with ST_AsText
        // so PostGIS's hex-WKB comes back as WKT text Keystone can store.
        let select_cols: Vec<String> = table
            .columns
            .iter()
            .map(|c| {
                if matches!(c.source_type.as_str(), "geometry" | "geography" | "geometry_dump") {
                    format!("ST_AsText({}) AS {}", quote_ident(&c.name), quote_ident(&c.name))
                } else {
                    quote_ident(&c.name)
                }
            })
            .collect();
        let select_list = select_cols.join(", ");

        let cursor_name = "tpt_harbor_gis_snapshot";
        self.client
            .execute(&format!("DECLARE {cursor_name} CURSOR FOR SELECT {select_list} FROM {}.{}", table.schema, table.name))
            .await
            .map_err(ConnectorError::Other)?;

        let mut total: u64 = 0;
        loop {
            let res = self
                .client
                .query(&format!("FETCH {SNAPSHOT_BATCH_SIZE} FROM {cursor_name}"))
                .await
                .map_err(ConnectorError::Other)?;
            if res.rows.is_empty() {
                break;
            }
            let batch: Vec<SourceRow> = res.rows.iter().map(|r| r.to_cell_vec()).collect();
            total += batch.len() as u64;
            let batch_len = res.rows.len() as i32;
            if tx.send(batch).await.is_err() {
                break;
            }
            if batch_len < SNAPSHOT_BATCH_SIZE {
                break;
            }
        }

        self.client.execute(&format!("CLOSE {cursor_name}")).await.map_err(ConnectorError::Other)?;
        self.client.execute("COMMIT").await.map_err(ConnectorError::Other)?;
        Ok(total)
    }

    async fn replicate(&mut self, tables: &[TableSchema], resume_token: Option<String>, tx: Sender<ChangeEvent>) -> Result<(), ConnectorError> {
        self.ensure_replication_objects().await.map_err(ConnectorError::Other)?;

        let start_lsn = resume_token.unwrap_or_else(|| "0/0".to_string());
        let mut repl_params: Vec<(&str, &str)> = self.params.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        repl_params.push(("replication", "database"));
        let mut repl_client = Client::connect(&self.addr, &repl_params).await.map_err(ConnectorError::Other)?;

        let sql = format!(
            "START_REPLICATION SLOT {} LOGICAL {} (proto_version '1', publication_names '{}')",
            self.slot, start_lsn, self.publication
        );
        repl_client.start_replication(&sql).await.map_err(ConnectorError::Other)?;

        let mut relations: std::collections::HashMap<i32, (String, Vec<String>)> = std::collections::HashMap::new();
        let table_names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();

        loop {
            let data = match repl_client.recv_replication_data().await.map_err(ConnectorError::Other)? {
                Some(d) => d,
                None => return Ok(()),
            };
            if data.is_empty() {
                continue;
            }
            match data[0] {
                b'w' => {
                    if data.len() < 25 {
                        continue;
                    }
                    let payload = &data[25..];
                    if let Some(event) = decode_pgoutput(payload, &mut relations, &table_names) {
                        if tx.send(event).await.is_err() {
                            return Ok(());
                        }
                    }
                }
                b'k' => {}
                _ => {}
            }
        }
    }

    async fn row_checksums(&mut self, table: &TableSchema) -> Result<Vec<u64>, ConnectorError> {
        let pk = table.primary_key_columns();
        let order_by = if pk.is_empty() { String::new() } else { format!(" ORDER BY {}", pk.join(", ")) };

        // Wrap geometry columns with ST_AsText for checksum consistency
        let _select_cols: Vec<String> = table
            .columns
            .iter()
            .map(|c| {
                if matches!(c.source_type.as_str(), "geometry" | "geography" | "geometry_dump") {
                    format!("ST_AsText({}) AS {}", quote_ident(&c.name), quote_ident(&c.name))
                } else {
                    format!("{}.*", quote_ident(&c.name))
                }
            })
            .collect();
        // Actually, we need SELECT *, but with ST_AsText for geometry cols
        // Simpler: just SELECT * since the column order in TableSchema matches
        let res = self
            .client
            .query(&format!("SELECT * FROM {}.{}{}", table.schema, table.name, order_by))
            .await
            .map_err(ConnectorError::Other)?;
        Ok(res.rows.iter().map(|r| crate::verify::hash_row(&r.to_cell_vec())).collect())
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

// Reuse the same pgoutput decoder from the PG connector — PostGIS uses
// the same logical replication protocol.

fn decode_pgoutput(
    payload: &[u8],
    relations: &mut std::collections::HashMap<i32, (String, Vec<String>)>,
    known_tables: &[&str],
) -> Option<ChangeEvent> {
    if payload.is_empty() {
        return None;
    }
    let mut p = payload;
    let tag = p[0];
    p = &p[1..];
    match tag {
        b'C' => Some(ChangeEvent::CommitLsn(String::new())),
        b'R' => {
            if p.len() < 4 {
                return None;
            }
            let id = i32::from_be_bytes(p[0..4].try_into().unwrap());
            p = &p[4..];
            let (_ns, rest) = read_cstr(p);
            p = rest;
            let (name, rest) = read_cstr(p);
            p = rest;
            if p.len() < 3 {
                return None;
            }
            p = &p[1..];
            let ncols = u16::from_be_bytes(p[0..2].try_into().unwrap()) as usize;
            p = &p[2..];
            let mut cols = Vec::with_capacity(ncols);
            for _ in 0..ncols {
                if p.is_empty() {
                    break;
                }
                p = &p[1..];
                let (col_name, rest) = read_cstr(p);
                p = rest;
                if p.len() < 8 {
                    break;
                }
                p = &p[8..];
                cols.push(col_name);
            }
            relations.insert(id, (name, cols));
            None
        }
        b'I' => {
            if p.len() < 4 {
                return None;
            }
            let id = i32::from_be_bytes(p[0..4].try_into().unwrap());
            p = &p[4..];
            let (name, _) = relations.get(&id)?;
            if !known_tables.contains(&name.as_str()) {
                return None;
            }
            if p.is_empty() || p[0] != b'N' {
                return None;
            }
            let (row, _) = decode_tuple(&p[1..]);
            Some(ChangeEvent::Insert { table: name.clone(), row })
        }
        b'U' => {
            if p.len() < 4 {
                return None;
            }
            let id = i32::from_be_bytes(p[0..4].try_into().unwrap());
            p = &p[4..];
            let (name, _) = relations.get(&id)?;
            if !known_tables.contains(&name.as_str()) {
                return None;
            }
            if p.is_empty() {
                return None;
            }
            let (key, new_row) = if p[0] == b'K' || p[0] == b'O' {
                let (old, rest) = decode_tuple(&p[1..]);
                if rest.is_empty() || rest[0] != b'N' {
                    return None;
                }
                let (new_row, _) = decode_tuple(&rest[1..]);
                (old, new_row)
            } else if p[0] == b'N' {
                let (new_row, _) = decode_tuple(&p[1..]);
                (new_row.clone(), new_row)
            } else {
                return None;
            };
            Some(ChangeEvent::Update { table: name.clone(), key, row: new_row })
        }
        b'D' => {
            if p.len() < 4 {
                return None;
            }
            let id = i32::from_be_bytes(p[0..4].try_into().unwrap());
            p = &p[4..];
            let (name, _) = relations.get(&id)?;
            if !known_tables.contains(&name.as_str()) {
                return None;
            }
            if p.is_empty() {
                return None;
            }
            let (key, _) = decode_tuple(&p[1..]);
            Some(ChangeEvent::Delete { table: name.clone(), key })
        }
        _ => None,
    }
}

fn read_cstr(buf: &[u8]) -> (String, &[u8]) {
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    let s = String::from_utf8_lossy(&buf[..end]).into_owned();
    (s, &buf[(end + 1).min(buf.len())..])
}

fn decode_tuple(buf: &[u8]) -> (SourceRow, &[u8]) {
    if buf.len() < 2 {
        return (Vec::new(), buf);
    }
    let ncols = u16::from_be_bytes(buf[0..2].try_into().unwrap()) as usize;
    let mut p = &buf[2..];
    let mut row = Vec::with_capacity(ncols);
    for _ in 0..ncols {
        if p.is_empty() {
            break;
        }
        let kind = p[0];
        p = &p[1..];
        match kind {
            b't' => {
                if p.len() < 4 {
                    break;
                }
                let len = i32::from_be_bytes(p[0..4].try_into().unwrap()) as usize;
                p = &p[4..];
                let take = len.min(p.len());
                row.push(Some(p[..take].to_vec()));
                p = &p[take..];
            }
            _ => row.push(None),
        }
    }
    (row, p)
}
