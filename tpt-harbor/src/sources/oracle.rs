//! Harbor/Oracle — Oracle source connector. Hand-written Oracle Net (TNS)
//! framing over TCP (port 1521), carrying a best-effort TTC (Two-Task
//! Common) session layer on top. Discovery queries `ALL_TAB_COLUMNS`,
//! snapshot uses cursor-based fetch. CDC is scope-cut — Oracle LogMiner CDC
//! is a separate substantial effort, consistent with every other source's
//! CDC being a documented cut here.
//!
//! **Confidence note (unlike this crate's other five connectors):** the
//! *TNS packet-framing layer* below (8-byte header, CONNECT/ACCEPT/REFUSE/
//! REDIRECT/DATA packet types, the CONNECT payload's field layout) matches
//! Oracle Net's publicly documented/reverse-engineered wire format (the
//! same shape every public TNS protocol dissector describes) and is written
//! with the same confidence as the TDS/Bolt/wire-protocol code in this
//! crate's other connectors. The *TTC layer inside DATA packets* — the
//! login/auth handshake (O3LOGON/O5LOGON) and the OALL8/OFETCH RPC opcodes
//! used for query execution — has no public specification at all; Oracle's
//! own client (OCI) is the only officially documented way to speak it. The
//! TTC code below is a best-effort reconstruction and is **not expected to
//! work byte-for-byte against a real Oracle server without correction
//! against a packet capture** — this is a materially higher-risk claim than
//! this repo's other "unverified against a real server" connectors, which
//! at least implement fully public protocols. Treat `discover`/
//! `snapshot_table`/`row_checksums` here as a structural skeleton for that
//! correction pass, not as validated protocol code.

use crate::connector::{ChangeEvent, ConnectorError, SourceConnector, SourceRow};
use crate::schema::{from_oracle_type, ColumnSchema, TableSchema};
use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut, BytesMut};
use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;

const SNAPSHOT_BATCH_SIZE: usize = 5_000;

// TNS packet types (Oracle Net Services common packet header, byte 4 of an
// 8-byte header: length:u16be, checksum:u16be, type:u8, reserved:u8,
// header_checksum:u16be).
const TNS_TYPE_CONNECT: u8 = 1;
const TNS_TYPE_ACCEPT: u8 = 2;
const TNS_TYPE_REFUSE: u8 = 4;
const TNS_TYPE_DATA: u8 = 6;

/// A raw TNS packet: header fields plus payload (payload excludes the
/// 8-byte header).
struct TnsPacket {
    packet_type: u8,
    payload: Vec<u8>,
}

/// Oracle Net (TNS) transport + a best-effort TTC session on top. See the
/// module doc comment for which half of this is solid vs. reconstructed.
struct OracleConn {
    stream: TcpStream,
    read_buf: BytesMut,
    cursor_seq: u32,
}

impl OracleConn {
    async fn connect(addr: &str, user: &str, password: &str, service_name: &str) -> Result<Self> {
        let (host, port) = if let Some(colon_idx) = addr.find(':') {
            (addr[..colon_idx].to_string(), addr[colon_idx + 1..].parse().unwrap_or(1521))
        } else {
            (addr.to_string(), 1521)
        };

        let stream = TcpStream::connect(format!("{host}:{port}"))
            .await
            .with_context(|| format!("connecting to Oracle at {host}:{port}"))?;

        let mut conn = Self { stream, read_buf: BytesMut::with_capacity(65536), cursor_seq: 0 };

        conn.send_connect(&host, port, service_name).await?;
        conn.expect_accept().await?;
        conn.login(user, password, service_name).await?;

        Ok(conn)
    }

    // --- TNS transport ---------------------------------------------------

    async fn send_packet(&mut self, packet_type: u8, payload: &[u8]) -> Result<()> {
        let mut buf = BytesMut::with_capacity(8 + payload.len());
        buf.put_u16(8 + payload.len() as u16);
        buf.put_u16(0); // packet checksum (unused, 0 = none)
        buf.put_u8(packet_type);
        buf.put_u8(0); // reserved
        buf.put_u16(0); // header checksum (unused, 0 = none)
        buf.put_slice(payload);
        self.stream.write_all(&buf).await?;
        self.stream.flush().await?;
        Ok(())
    }

    async fn read_packet(&mut self) -> Result<TnsPacket> {
        self.fill(8).await?;
        let length = u16::from_be_bytes(self.read_buf[0..2].try_into().unwrap()) as usize;
        let packet_type = self.read_buf[4];
        if length < 8 {
            bail!("malformed TNS packet: length {length} < header size");
        }
        self.fill(length).await?;
        self.read_buf.advance(8);
        let payload = self.read_buf.split_to(length - 8).to_vec();
        Ok(TnsPacket { packet_type, payload })
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

    /// Send a TNS CONNECT packet carrying a standard Oracle connect
    /// descriptor string. This part of the payload (the descriptor string
    /// syntax) is the same one `tnsnames.ora`/`EZCONNECT` use and is
    /// genuinely public.
    async fn send_connect(&mut self, host: &str, port: u16, service_name: &str) -> Result<()> {
        let descriptor = format!(
            "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={host})(PORT={port}))\
             (CONNECT_DATA=(SERVICE_NAME={service_name})))"
        );
        let data = descriptor.as_bytes();

        let mut payload = BytesMut::new();
        payload.put_u16(0x0139); // version (12.2-ish)
        payload.put_u16(0x0121); // version compatible with
        payload.put_u16(0x0c00); // service options
        payload.put_u16(0x2000); // SDU size (8192)
        payload.put_u16(0x7fff); // TDU size (max)
        payload.put_u16(0x0000); // protocol characteristics
        payload.put_u16(0x0000); // line turnaround
        payload.put_u16(0x0001); // value of 1 (endianness marker placeholder)
        payload.put_u16(data.len() as u16); // connect data length
        payload.put_u16(58); // connect data offset (fixed header below is 58 bytes)
        payload.put_u32(0x0000ffff); // max receivable connect data
        payload.put_u8(0); // connect flags 0
        payload.put_u8(0); // connect flags 1
        payload.put_u64(0); // trace cross-facility items (unused here)
        payload.put_u64(0);
        payload.put_u32(0);
        payload.put_slice(data);

        self.send_packet(TNS_TYPE_CONNECT, &payload).await
    }

    async fn expect_accept(&mut self) -> Result<()> {
        let pkt = self.read_packet().await?;
        match pkt.packet_type {
            TNS_TYPE_ACCEPT => Ok(()),
            TNS_TYPE_REFUSE => bail!("Oracle refused the connection: {}", String::from_utf8_lossy(&pkt.payload)),
            other => bail!("unexpected TNS packet type {other} while waiting for ACCEPT (redirect chains are not followed)"),
        }
    }

    // --- TTC session layer (best-effort, see module doc comment) --------

    /// Send a login request and read the server's reply. Real Oracle
    /// (10g+) requires the O3LOGON/O5LOGON challenge-response dance with a
    /// server-supplied session-key/salt before the password ever crosses
    /// the wire; there is no public spec for that exchange, so this sends
    /// a single best-effort TTC "function call" frame carrying the
    /// username/password directly and treats any DATA reply that isn't an
    /// explicit error marker as success. Real servers will very likely
    /// reject this outright — see the module doc comment.
    async fn login(&mut self, user: &str, password: &str, service_name: &str) -> Result<()> {
        let mut body = BytesMut::new();
        body.put_u8(0x03); // TTI message type: function call (best-effort opcode)
        body.put_u8(0x76); // function code: OLOGON (best-effort opcode)
        put_ttc_str(&mut body, user);
        put_ttc_str(&mut body, password);
        put_ttc_str(&mut body, service_name);

        self.send_packet(TNS_TYPE_DATA, &body).await?;
        let reply = self.read_packet().await?;
        if reply.packet_type != TNS_TYPE_DATA {
            bail!("Oracle login: unexpected packet type {} in reply", reply.packet_type);
        }
        if reply.payload.first() == Some(&0x04) {
            // TTI message type 0x04 is used below as this client's own
            // "error" marker convention for parity with `execute`.
            bail!("Oracle login rejected");
        }
        Ok(())
    }

    /// Execute a SQL statement and fetch all rows. Real Oracle's OALL8/
    /// OFETCH opcodes carry typed, length-prefixed columns (`SQLT_CHR`,
    /// `SQLT_NUM`, `SQLT_DAT`, ...); this client speaks a simplified
    /// best-effort framing (function code + NUL-terminated SQL text in,
    /// a row count followed by length-prefixed text cells out) rather than
    /// reproducing that typed wire format, since the exact opcode/type
    /// byte values aren't public. Every cell therefore round-trips as text.
    async fn execute(&mut self, sql: &str) -> Result<Vec<Vec<Option<Vec<u8>>>>> {
        self.cursor_seq = self.cursor_seq.wrapping_add(1);

        let mut body = BytesMut::new();
        body.put_u8(0x03); // TTI message type: function call
        body.put_u8(0x5e); // function code: OALL8 (best-effort opcode)
        body.put_u32(self.cursor_seq);
        put_ttc_str(&mut body, sql);

        self.send_packet(TNS_TYPE_DATA, &body).await?;

        let mut rows = Vec::new();
        loop {
            let reply = self.read_packet().await?;
            if reply.packet_type != TNS_TYPE_DATA {
                bail!("Oracle execute: unexpected packet type {}", reply.packet_type);
            }
            if reply.payload.is_empty() {
                break;
            }
            match reply.payload[0] {
                0x04 => bail!("Oracle execute error: {}", String::from_utf8_lossy(&reply.payload[1..])),
                0x06 => {
                    // Row batch: [0x06][row_count:u16be][rows...]
                    let mut p = &reply.payload[1..];
                    if p.len() < 2 {
                        break;
                    }
                    let row_count = u16::from_be_bytes(p[0..2].try_into().unwrap());
                    p = &p[2..];
                    for _ in 0..row_count {
                        let (row, rest) = read_ttc_row(p)?;
                        rows.push(row);
                        p = rest;
                    }
                }
                0x07 => break, // end-of-fetch marker
                other => bail!("Oracle execute: unrecognized reply tag {other}"),
            }
        }
        Ok(rows)
    }
}

fn put_ttc_str(buf: &mut BytesMut, s: &str) {
    buf.put_u32(s.len() as u32);
    buf.put_slice(s.as_bytes());
}

/// Read one length-prefixed-cell row: `[ncols:u16be]` then, per column,
/// `[len:u32be]` (`0xffffffff` = NULL) followed by that many bytes.
fn read_ttc_row(p: &[u8]) -> Result<(Vec<Option<Vec<u8>>>, &[u8])> {
    if p.len() < 2 {
        bail!("truncated TTC row header");
    }
    let ncols = u16::from_be_bytes(p[0..2].try_into().unwrap()) as usize;
    let mut p = &p[2..];
    let mut cells = Vec::with_capacity(ncols);
    for _ in 0..ncols {
        if p.len() < 4 {
            bail!("truncated TTC cell length");
        }
        let len = u32::from_be_bytes(p[0..4].try_into().unwrap());
        p = &p[4..];
        if len == 0xffff_ffff {
            cells.push(None);
        } else {
            let len = len as usize;
            if p.len() < len {
                bail!("truncated TTC cell data");
            }
            cells.push(Some(p[..len].to_vec()));
            p = &p[len..];
        }
    }
    Ok((cells, p))
}

fn cell_str(cells: &[Option<Vec<u8>>], idx: usize) -> String {
    cells.get(idx).and_then(|c| c.as_ref()).map(|b| String::from_utf8_lossy(b).to_string()).unwrap_or_default()
}

pub struct OracleSource {
    conn: OracleConn,
}

impl OracleSource {
    /// No `--source-password` flag exists anywhere in this CLI yet (every
    /// connector here is credential-optional/trust-auth in this repo's
    /// current scope, matching `tpt-keystone`'s own no-auth wire protocol);
    /// connects with an empty password, same convention `MsSqlSource`
    /// already uses.
    pub async fn connect(addr: &str, user: &str, service_name: &str) -> Result<Self> {
        let conn = OracleConn::connect(addr, user, "", service_name).await?;
        Ok(Self { conn })
    }
}

#[async_trait]
impl SourceConnector for OracleSource {
    fn name(&self) -> &'static str {
        "Harbor/Oracle"
    }

    async fn discover(&mut self) -> Result<Vec<TableSchema>, ConnectorError> {
        let table_rows = self
            .conn
            .execute(
                "SELECT OWNER, TABLE_NAME FROM ALL_TABLES \
                 WHERE OWNER NOT IN ('SYS','SYSTEM','OUTLN','XDB','WMSYS','CTXSYS','MDSYS','ORDSYS')",
            )
            .await
            .map_err(ConnectorError::Other)?;

        let mut tables = Vec::new();
        for row in &table_rows {
            let owner = cell_str(row, 0);
            let name = cell_str(row, 1);
            if name.is_empty() {
                continue;
            }

            let col_rows = self
                .conn
                .execute(&format!(
                    "SELECT c.COLUMN_NAME, c.DATA_TYPE, c.NULLABLE, \
                     CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 'Y' ELSE 'N' END \
                     FROM ALL_TAB_COLUMNS c \
                     LEFT JOIN ( \
                       SELECT ucc.COLUMN_NAME, ucc.OWNER, ucc.TABLE_NAME \
                       FROM ALL_CONSTRAINTS uc \
                       JOIN ALL_CONS_COLUMNS ucc ON uc.CONSTRAINT_NAME = ucc.CONSTRAINT_NAME AND uc.OWNER = ucc.OWNER \
                       WHERE uc.CONSTRAINT_TYPE = 'P' \
                     ) pk ON pk.COLUMN_NAME = c.COLUMN_NAME AND pk.OWNER = c.OWNER AND pk.TABLE_NAME = c.TABLE_NAME \
                     WHERE c.OWNER = '{owner}' AND c.TABLE_NAME = '{name}' \
                     ORDER BY c.COLUMN_ID"
                ))
                .await
                .map_err(ConnectorError::Other)?;

            let columns: Vec<ColumnSchema> = col_rows
                .iter()
                .map(|r| {
                    let source_type = cell_str(r, 1);
                    ColumnSchema {
                        name: cell_str(r, 0),
                        keystone_type: from_oracle_type(&source_type),
                        nullable: cell_str(r, 2) == "Y",
                        is_primary_key: cell_str(r, 3) == "Y",
                        source_type,
                    }
                })
                .collect();

            if !columns.is_empty() {
                tables.push(TableSchema { schema: owner, name, columns });
            }
        }
        Ok(tables)
    }

    async fn snapshot_table(&mut self, table: &TableSchema, tx: Sender<Vec<SourceRow>>) -> Result<u64, ConnectorError> {
        let rows = self
            .conn
            .execute(&format!("SELECT * FROM \"{}\".\"{}\"", table.schema, table.name))
            .await
            .map_err(ConnectorError::Other)?;

        let mut total: u64 = 0;
        for chunk in rows.chunks(SNAPSHOT_BATCH_SIZE) {
            let batch: Vec<SourceRow> = chunk.to_vec();
            total += batch.len() as u64;
            if tx.send(batch).await.is_err() {
                break;
            }
        }
        Ok(total)
    }

    async fn replicate(&mut self, _tables: &[TableSchema], _resume_token: Option<String>, _tx: Sender<ChangeEvent>) -> Result<(), ConnectorError> {
        Err(ConnectorError::Unimplemented { connector: "Harbor/Oracle", detail: "Oracle LogMiner CDC not yet written" })
    }

    async fn row_checksums(&mut self, table: &TableSchema) -> Result<Vec<u64>, ConnectorError> {
        let pk = table.primary_key_columns();
        let order_by = if pk.is_empty() { String::new() } else { format!(" ORDER BY {}", pk.join(", ")) };
        let rows = self
            .conn
            .execute(&format!("SELECT * FROM \"{}\".\"{}\"{}", table.schema, table.name, order_by))
            .await
            .map_err(ConnectorError::Other)?;
        Ok(rows.iter().map(|r| crate::verify::hash_row(r)).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ttc_row_roundtrip() {
        let mut buf = BytesMut::new();
        buf.put_u16(2); // ncols
        buf.put_u32(3);
        buf.put_slice(b"abc");
        buf.put_u32(0xffff_ffff); // NULL
        buf.extend_from_slice(b"trailing");

        let (row, rest) = read_ttc_row(&buf).unwrap();
        assert_eq!(row, vec![Some(b"abc".to_vec()), None]);
        assert_eq!(rest, b"trailing");
    }

    #[test]
    fn ttc_row_rejects_truncated_cell() {
        let mut buf = BytesMut::new();
        buf.put_u16(1);
        buf.put_u32(10); // claims 10 bytes but none follow
        assert!(read_ttc_row(&buf).is_err());
    }

    #[test]
    fn ttc_str_length_prefixed() {
        let mut buf = BytesMut::new();
        put_ttc_str(&mut buf, "hello");
        assert_eq!(&buf[0..4], &5u32.to_be_bytes());
        assert_eq!(&buf[4..9], b"hello");
    }
}
