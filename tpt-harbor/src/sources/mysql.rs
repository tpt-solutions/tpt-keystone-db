//! Harbor/MySQL — MySQL/MariaDB source connector. Hand-written MySQL
//! client/server protocol v10 over TCP (port 3306). Discovery uses MySQL's
//! own `information_schema` (same concept as Postgres). Snapshot streams
//! results via `COM_QUERY`. CDC is scope-cut to `Unimplemented` — MySQL
//! binlog CDC is a substantial protocol effort on its own.

use crate::connector::{ConnectorError, SourceConnector, SourceRow, ChangeEvent};
use crate::schema::{from_mysql_type, ColumnSchema, TableSchema};
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;

const SNAPSHOT_BATCH_SIZE: usize = 5_000;

/// Minimal MySQL wire-protocol client.
struct MysqlConn {
    stream: TcpStream,
    read_buf: BytesMut,
    write_buf: BytesMut,
    sequence_id: u8,
    server_capabilities: u32,
    charset: u16,
}

#[derive(Debug)]
struct MysqlRow {
    columns: Vec<String>,
    cells: Vec<Option<Vec<u8>>>,
}

#[derive(Debug)]
struct QueryResult {
    columns: Vec<String>,
    rows: Vec<MysqlRow>,
}

impl MysqlConn {
    async fn connect(addr: &str, user: &str, database: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr).await.with_context(|| format!("connecting to MySQL at {addr}"))?;
        let mut conn = Self {
            stream,
            read_buf: BytesMut::with_capacity(16384),
            write_buf: BytesMut::with_capacity(16384),
            sequence_id: 0,
            server_capabilities: 0,
            charset: 33, // utf8
        };

        // Read server greeting
        conn.fill(4).await?;
        let _header = conn.read_buf.split_to(4);
        let packet_len = u32::from_le_bytes(_header[0..4].try_into().unwrap()) as usize;
        conn.fill(packet_len).await?;
        let payload = conn.read_buf.split_to(packet_len);

        let mut p = &payload[..];
        if p.is_empty() {
            bail!("empty server greeting");
        }
        let protocol_version = p[0];
        p = &p[1..];
        if protocol_version != 10 {
            bail!("unsupported MySQL protocol version {protocol_version}");
        }

        // Server version (null-terminated string)
        let ver_end = p.iter().position(|&b| b == 0).unwrap_or(p.len());
        let _server_version = String::from_utf8_lossy(&p[..ver_end]);
        p = &p[ver_end + 1..];

        if p.len() < 4 {
            bail!("greeting too short");
        }
        let _thread_id = u32::from_le_bytes(p[0..4].try_into().unwrap());
        p = &p[4..];

        // auth_plugin_data_part_1 (8 bytes) + filler (1 byte)
        if p.len() < 9 {
            bail!("greeting too short for auth data");
        }
        let auth_part1 = &p[0..8];
        p = &p[9..];

        // capabilities (lower 2 bytes)
        if p.len() < 2 {
            bail!("greeting too short for capabilities");
        }
        let cap_lower = u16::from_le_bytes(p[0..2].try_into().unwrap()) as u32;
        p = &p[2..];

        let mut server_caps = cap_lower;

        if !p.is_empty() {
            let charset = p[0] as u16;
            conn.charset = charset;
            p = &p[1..];
        }

        if p.len() >= 2 {
            let cap_upper = u16::from_le_bytes(p[0..2].try_into().unwrap()) as u32;
            server_caps |= cap_upper << 16;
            p = &p[2..];
        } else {
            p = &[];
        }

        // Skip status flags (2 bytes) and capability flags upper (if more)
        if p.len() >= 10 {
            p = &p[10..]; // status(2) + cap_upper(2) + auth_len(1) + reserved(10) - already consumed above
        }

        // auth_plugin_data_part_2 (if SECURE_CONNECTION)
        let mut auth_data = Vec::from(auth_part1);
        if server_caps & (1 << 19) != 0 && p.len() >= 13 {
            let part2_len = p[0].saturating_sub(1) as usize;
            if p.len() >= 1 + part2_len {
                auth_data.extend_from_slice(&p[1..1 + part2_len]);
            }
        }

        conn.server_capabilities = server_caps;

        // Send login packet
        conn.sequence_id = 1;
        let mut login_body = BytesMut::new();

        // Client capabilities (4 bytes)
        let client_caps: u32 = 1 | (1 << 3) | (1 << 7) | (1 << 11) | (1 << 17) | (1 << 18) | (1 << 20);
        // CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH | CLIENT_CONNECT_WITH_DB
        login_body.put_u32_le(client_caps);

        // Max packet size (4 bytes)
        login_body.put_u32_le(1 << 24);

        // Charset (1 byte + 23 reserved bytes)
        login_body.put_u8(conn.charset as u8);
        login_body.put_bytes(0, 23);

        // Username (null-terminated)
        login_body.put_slice(user.as_bytes());
        login_body.put_u8(0);

        // Auth response (length + data)
        login_body.put_u8(auth_data.len() as u8);
        login_body.put_slice(&auth_data);

        // Database (null-terminated, only if CLIENT_CONNECT_WITH_DB)
        if !database.is_empty() {
            login_body.put_slice(database.as_bytes());
            login_body.put_u8(0);
        }

        // Auth plugin name
        login_body.put_slice(b"mysql_native_password\0");

        conn.write_packet(&login_body).await?;

        // Read login response
        let resp = conn.read_packet().await?;
        if resp.is_empty() {
            bail!("empty login response");
        }
        if resp[0] == 0xFF {
            let msg = String::from_utf8_lossy(&resp[1..]);
            bail!("login failed: {msg}");
        }

        // OK packet — connection established
        Ok(conn)
    }

    async fn fill(&mut self, n: usize) -> Result<()> {
        while self.read_buf.len() < n {
            let read = self.stream.read_buf(&mut self.read_buf).await?;
            if read == 0 {
                bail!("connection closed by peer");
            }
        }
        Ok(())
    }

    async fn read_packet(&mut self) -> Result<Vec<u8>> {
        self.fill(4).await?;
        let len = u32::from_le_bytes(self.read_buf[0..4].try_into().unwrap()) as usize;
        let _seq = self.read_buf[4];
        self.read_buf.advance(5);
        self.fill(len).await?;
        let data = self.read_buf.split_to(len).to_vec();
        self.sequence_id = self.sequence_id.wrapping_add(1);
        Ok(data)
    }

    async fn write_packet(&mut self, body: &[u8]) -> Result<()> {
        self.write_buf.put_u32_le(body.len() as u32);
        self.write_buf.put_u8(self.sequence_id);
        self.write_buf.put_slice(body);
        self.stream.write_all(&self.write_buf).await?;
        self.stream.flush().await?;
        self.write_buf.clear();
        self.sequence_id = self.sequence_id.wrapping_add(1);
        Ok(())
    }

    async fn query(&mut self, sql: &str) -> Result<QueryResult> {
        self.sequence_id = 0;
        // COM_QUERY = 0x03
        let mut body = BytesMut::new();
        body.put_u8(0x03);
        body.put_slice(sql.as_bytes());
        self.write_packet(&body).await?;

        // Read column count
        let packet = self.read_packet().await?;
        if packet.is_empty() {
            bail!("empty query response");
        }
        if packet[0] == 0xFF {
            let msg = String::from_utf8_lossy(&packet[5..]);
            bail!("query error: {msg}");
        }
        if packet[0] == 0xFE {
            // OK or EOF
            return Ok(QueryResult { columns: vec![], rows: vec![] });
        }

        let col_count = packet[0] as usize;

        // Read column definitions
        let mut columns = Vec::new();
        for _ in 0..col_count {
            let pkt = self.read_packet().await?;
            // Column definition: catalog(4+len) + schema(4+len) + table(4+len) + org_table(4+len) + name(4+len) + org_name(4+len) + ...
            let mut p = &pkt[..];
            // Skip catalog, schema, table, org_table
            for _ in 0..4 {
                let len = if p.is_empty() { 0 } else {
                    if p[0] == 0xFC { // length-encoded integer (2 bytes)
                        let v = u16::from_le_bytes(p[1..3].try_into().unwrap_or([0, 0])) as usize;
                        p = &p[3..];
                        v
                    } else if p[0] == 0xFB {
                        p = &p[1..]; // NULL
                        0
                    } else {
                        let v = p[0] as usize;
                        p = &p[1..];
                        v
                    }
                };
                if p.len() < len { break; }
                p = &p[len..];
            }
            // Skip filler (2 bytes)
            if p.len() >= 2 { p = &p[2..]; }

            // Character set (2 bytes)
            if p.len() >= 2 { p = &p[2..]; }
            // Column length (4 bytes)
            if p.len() >= 4 { p = &p[4..]; }
            // Column type (1 byte)
            let _col_type = if p.is_empty() { 0 } else { p[0] };
            if !p.is_empty() { p = &p[1..]; }
            // Flags (2 bytes), decimals (1 byte)
            if p.len() >= 3 { p = &p[3..]; }

            // Name (length-encoded)
            if !p.is_empty() {
                if p[0] == 0xFC && p.len() >= 3 {
                    let name_len = u16::from_le_bytes(p[1..3].try_into().unwrap_or([0, 0])) as usize;
                    p = &p[3..];
                    if p.len() >= name_len {
                        let name = String::from_utf8_lossy(&p[..name_len]).to_string();
                        columns.push(name);
                        p = &p[name_len..];
                    }
                } else if p[0] != 0xFB {
                    let name_len = p[0] as usize;
                    p = &p[1..];
                    if p.len() >= name_len {
                        let name = String::from_utf8_lossy(&p[..name_len]).to_string();
                        columns.push(name);
                        p = &p[name_len..];
                    }
                }
            }
        }

        // Read EOF marker
        let _eof = self.read_packet().await?;

        // Read rows
        let mut rows = Vec::new();
        loop {
            let pkt = self.read_packet().await?;
            if pkt.is_empty() {
                break;
            }
            if pkt[0] == 0xFE {
                // EOF
                break;
            }
            if pkt[0] == 0xFF {
                let msg = String::from_utf8_lossy(&pkt[5..]);
                bail!("row read error: {msg}");
            }

            // Parse row
            let mut cells = Vec::new();
            let mut p = &pkt[..];
            while !p.is_empty() {
                if p[0] == 0xFB {
                    cells.push(None);
                    p = &p[1..];
                } else if p[0] == 0xFC && p.len() >= 3 {
                    let len = u16::from_le_bytes(p[1..3].try_into().unwrap_or([0, 0])) as usize;
                    p = &p[3..];
                    let take = len.min(p.len());
                    cells.push(Some(p[..take].to_vec()));
                    p = &p[take..];
                } else {
                    let len = p[0] as usize;
                    p = &p[1..];
                    let take = len.min(p.len());
                    cells.push(Some(p[..take].to_vec()));
                    p = &p[take..];
                }
            }

            rows.push(MysqlRow { columns: columns.clone(), cells });
        }

        Ok(QueryResult { columns, rows })
    }
}

pub struct MySqlSource {
    conn: MysqlConn,
}

impl MySqlSource {
    pub async fn connect(addr: &str, user: &str, database: &str) -> Result<Self> {
        let conn = MysqlConn::connect(addr, user, database).await?;
        Ok(Self { conn })
    }
}

#[async_trait]
impl SourceConnector for MySqlSource {
    fn name(&self) -> &'static str {
        "Harbor/MySQL"
    }

    async fn discover(&mut self) -> Result<Vec<TableSchema>, ConnectorError> {
        let tables_res = self
            .conn
            .query("SELECT table_schema, table_name FROM information_schema.tables \
                    WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')")
            .await
            .map_err(ConnectorError::Other)?;

        let mut tables = Vec::new();
        for row in &tables_res.rows {
            let schema = row.cells.get(0).and_then(|c| c.as_ref()).map(|b| String::from_utf8_lossy(b).to_string()).unwrap_or_default();
            let name = row.cells.get(1).and_then(|c| c.as_ref()).map(|b| String::from_utf8_lossy(b).to_string()).unwrap_or_default();
            if name.is_empty() {
                continue;
            }

            let cols_res = self
                .conn
                .query(&format!(
                    "SELECT column_name, column_type, is_nullable, column_key \
                     FROM information_schema.columns \
                     WHERE table_schema = '{schema}' AND table_name = '{name}' \
                     ORDER BY ordinal_position"
                ))
                .await
                .map_err(ConnectorError::Other)?;

            let columns: Vec<ColumnSchema> = cols_res
                .rows
                .iter()
                .map(|r| {
                    let col_name = r.cells.get(0).and_then(|c| c.as_ref()).map(|b| String::from_utf8_lossy(b).to_string()).unwrap_or_default();
                    let source_type = r.cells.get(1).and_then(|c| c.as_ref()).map(|b| String::from_utf8_lossy(b).to_string()).unwrap_or_default();
                    let nullable_str = r.cells.get(2).and_then(|c| c.as_ref()).map(|b| String::from_utf8_lossy(b).to_string()).unwrap_or_default();
                    let pk_str = r.cells.get(3).and_then(|c| c.as_ref()).map(|b| String::from_utf8_lossy(b).to_string()).unwrap_or_default();
                    ColumnSchema {
                        keystone_type: from_mysql_type(&source_type),
                        nullable: nullable_str == "YES",
                        is_primary_key: pk_str == "PRI",
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
        let res = self
            .conn
            .query(&format!("SELECT * FROM {}.{}", table.schema, table.name))
            .await
            .map_err(ConnectorError::Other)?;

        let mut total: u64 = 0;
        for chunk in res.rows.chunks(SNAPSHOT_BATCH_SIZE) {
            let batch: Vec<SourceRow> = chunk.iter().map(|r| r.cells.clone()).collect();
            total += batch.len() as u64;
            if tx.send(batch).await.is_err() {
                break;
            }
        }
        Ok(total)
    }

    async fn replicate(&mut self, _tables: &[TableSchema], _resume_token: Option<String>, _tx: Sender<ChangeEvent>) -> Result<(), ConnectorError> {
        Err(ConnectorError::Unimplemented { connector: "Harbor/MySQL", detail: "MySQL binlog CDC not yet written" })
    }

    async fn row_checksums(&mut self, table: &TableSchema) -> Result<Vec<u64>, ConnectorError> {
        let pk = table.primary_key_columns();
        let order_by = if pk.is_empty() { String::new() } else { format!(" ORDER BY {}", pk.join(", ")) };
        let res = self
            .conn
            .query(&format!("SELECT * FROM {}.{}{}", table.schema, table.name, order_by))
            .await
            .map_err(ConnectorError::Other)?;
        Ok(res.rows.iter().map(|r| crate::verify::hash_row(&r.cells)).collect())
    }
}
