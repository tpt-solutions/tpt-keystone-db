//! Harbor/MSSQL — SQL Server source connector. Hand-written TDS 7.4
//! protocol over TCP (port 1433). Discovery uses SQL Server's own
//! `information_schema`. Snapshot streams results via cursor-based fetch.
//! CDC (`replicate`) polls `cdc.fn_cdc_get_all_changes_<capture_instance>`
//! — plain SQL through the same `TdsConn::query` path, since SQL Server's
//! own CDC/LSN bookkeeping is exposed as ordinary result sets, not a
//! separate wire-level change-feed protocol. Requires CDC already enabled
//! on the source database/tables (`sys.sp_cdc_enable_db`/`_table`) — Harbor
//! detects and reports a missing prerequisite rather than enabling it
//! itself, since that's an elevated-permission, DBA-owned decision.

use crate::connector::{ConnectorError, SourceConnector, SourceRow, ChangeEvent};
use crate::schema::{from_mssql_type, ColumnSchema, TableSchema};
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use bytes::{Buf, BufMut, BytesMut};
use std::collections::HashMap;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;

const SNAPSHOT_BATCH_SIZE: usize = 5_000;
const CDC_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Minimal TDS 7.4 client for SQL Server.
struct TdsConn {
    stream: TcpStream,
    read_buf: BytesMut,
    write_buf: BytesMut,
    packet_id: u8,
}

#[derive(Debug)]
struct MssqlRow {
    cells: Vec<Option<Vec<u8>>>,
}

#[derive(Debug)]
struct QueryResult {
    rows: Vec<MssqlRow>,
}

/// How a TDS column's row-level value is length-delimited, per its
/// TYPE_INFO category (MS-TDS §2.2.5.4). `ByteLenNoInfo` is `DATEN`/
/// `TIMEN`/`DATETIME2N`/`DATETIMEOFFSETN` — like `ByteLen` (1-byte value
/// length per row cell) but TYPE_INFO itself carries no max-length byte.
#[derive(Debug, Clone, Copy)]
enum TdsLenKind {
    Fixed(usize),
    ByteLen,
    ByteLenNoInfo,
    UShortLen,
    LongLen,
}

#[derive(Debug, Clone, Copy)]
struct ColumnMeta {
    tds_type: u8,
    len_kind: TdsLenKind,
    /// DECIMAL/NUMERIC scale, or TIME-family fractional-second scale (0-7).
    scale: u8,
    is_unicode: bool,
    /// `USHORTLEN_TYPE` with `max_len == 0xFFFF` — a `MAX` type using PLP
    /// chunked row encoding instead of a plain 2-byte length prefix.
    is_plp: bool,
}

/// Parses one column's TYPE_INFO (MS-TDS §2.2.5.4) starting at `p[0]` (the
/// type byte), returning the parsed metadata and the remaining bytes.
fn parse_type_info(p: &[u8]) -> Result<(ColumnMeta, &[u8])> {
    if p.is_empty() { bail!("truncated TYPE_INFO"); }
    let tds_type = p[0];
    let mut p = &p[1..];
    let meta = match tds_type {
        0x1F => ColumnMeta { tds_type, len_kind: TdsLenKind::Fixed(0), scale: 0, is_unicode: false, is_plp: false },
        0x30 | 0x32 => ColumnMeta { tds_type, len_kind: TdsLenKind::Fixed(1), scale: 0, is_unicode: false, is_plp: false }, // INT1/BIT
        0x34 => ColumnMeta { tds_type, len_kind: TdsLenKind::Fixed(2), scale: 0, is_unicode: false, is_plp: false }, // INT2
        0x38 | 0x3A | 0x3B | 0x7A => ColumnMeta { tds_type, len_kind: TdsLenKind::Fixed(4), scale: 0, is_unicode: false, is_plp: false }, // INT4/DATETIM4/FLT4/MONEY4
        0x3C | 0x3D | 0x3E | 0x7F => ColumnMeta { tds_type, len_kind: TdsLenKind::Fixed(8), scale: 0, is_unicode: false, is_plp: false }, // MONEY/DATETIME/FLT8/INT8

        // BYTELEN_TYPE: 1-byte max_len in TYPE_INFO.
        0x24 | 0x26 | 0x68 | 0x6D | 0x6E | 0x6F => { // GUID/INTN/BITN/FLTN/MONEYN/DATETIMN
            if p.is_empty() { bail!("truncated TYPE_INFO (bytelen max_len)"); }
            p = &p[1..];
            ColumnMeta { tds_type, len_kind: TdsLenKind::ByteLen, scale: 0, is_unicode: false, is_plp: false }
        }
        0x37 | 0x3F | 0x6A | 0x6C => { // DECIMAL/NUMERIC/DECIMALN/NUMERICN: max_len + precision + scale
            if p.len() < 3 { bail!("truncated TYPE_INFO (decimal)"); }
            let scale = p[2];
            p = &p[3..];
            ColumnMeta { tds_type, len_kind: TdsLenKind::ByteLen, scale, is_unicode: false, is_plp: false }
        }
        0x28 => ColumnMeta { tds_type, len_kind: TdsLenKind::ByteLenNoInfo, scale: 0, is_unicode: false, is_plp: false }, // DATEN — no extra TYPE_INFO bytes
        0x29 | 0x2A | 0x2B => { // TIMEN/DATETIME2N/DATETIMEOFFSETN: 1-byte scale
            if p.is_empty() { bail!("truncated TYPE_INFO (time scale)"); }
            let scale = p[0];
            p = &p[1..];
            ColumnMeta { tds_type, len_kind: TdsLenKind::ByteLenNoInfo, scale, is_unicode: false, is_plp: false }
        }

        // USHORTLEN_TYPE: 2-byte max_len (+5-byte collation for char types).
        0xA7 | 0xAF | 0xE7 | 0xEF => { // BIGVARCHAR/BIGCHAR/NVARCHAR/NCHAR
            if p.len() < 2 { bail!("truncated TYPE_INFO (char max_len)"); }
            let max_len = u16::from_le_bytes(p[0..2].try_into().unwrap());
            p = &p[2..];
            if p.len() < 5 { bail!("truncated TYPE_INFO (collation)"); }
            p = &p[5..];
            ColumnMeta { tds_type, len_kind: TdsLenKind::UShortLen, scale: 0, is_unicode: matches!(tds_type, 0xE7 | 0xEF), is_plp: max_len == 0xFFFF }
        }
        0xA5 | 0xAD => { // BIGVARBINARY/BIGBINARY
            if p.len() < 2 { bail!("truncated TYPE_INFO (binary max_len)"); }
            let max_len = u16::from_le_bytes(p[0..2].try_into().unwrap());
            p = &p[2..];
            ColumnMeta { tds_type, len_kind: TdsLenKind::UShortLen, scale: 0, is_unicode: false, is_plp: max_len == 0xFFFF }
        }

        // LONGLEN_TYPE: legacy TEXT/NTEXT/IMAGE — 4-byte max_len, optional
        // collation, then a table-name part list to skip over.
        0x23 | 0x63 | 0x22 => {
            if p.len() < 4 { bail!("truncated TYPE_INFO (longlen max_len)"); }
            p = &p[4..];
            let is_unicode = tds_type == 0x63;
            if tds_type != 0x22 {
                if p.len() < 5 { bail!("truncated TYPE_INFO (text collation)"); }
                p = &p[5..];
            }
            if p.is_empty() { bail!("truncated TYPE_INFO (table name parts)"); }
            let num_parts = p[0];
            p = &p[1..];
            for _ in 0..num_parts {
                if p.len() < 2 { bail!("truncated TYPE_INFO (table name part length)"); }
                let clen = u16::from_le_bytes(p[0..2].try_into().unwrap()) as usize;
                p = &p[2..];
                let byte_len = clen * 2;
                if p.len() < byte_len { bail!("truncated TYPE_INFO (table name part)"); }
                p = &p[byte_len..];
            }
            ColumnMeta { tds_type, len_kind: TdsLenKind::LongLen, scale: 0, is_unicode, is_plp: false }
        }

        other => bail!("unsupported TDS type 0x{:02X} in COLMETADATA", other),
    };
    Ok((meta, p))
}

/// Reads one row cell for `meta`, returning its already-decoded UTF-8 text
/// bytes (or `None` for SQL NULL) and the remaining row bytes — text, not
/// raw wire bytes, because `targets/keystone.rs::cell_literal` treats every
/// `SourceRow` cell as ready-to-quote text.
fn read_cell<'a>(p: &'a [u8], meta: &ColumnMeta) -> Result<(Option<Vec<u8>>, &'a [u8])> {
    match meta.len_kind {
        TdsLenKind::Fixed(0) => Ok((None, p)),
        TdsLenKind::Fixed(n) => {
            if p.len() < n { bail!("truncated fixed-length cell (need {n} bytes)"); }
            let text = fixed_to_text(meta.tds_type, &p[..n])?;
            Ok((Some(text), &p[n..]))
        }
        TdsLenKind::ByteLen => {
            if p.is_empty() { bail!("truncated BYTELEN_TYPE cell"); }
            let len = p[0] as usize;
            let p = &p[1..];
            if len == 0 { return Ok((None, p)); }
            if p.len() < len { bail!("truncated BYTELEN_TYPE cell data"); }
            let text = bytelen_to_text(meta, &p[..len])?;
            Ok((Some(text), &p[len..]))
        }
        TdsLenKind::ByteLenNoInfo => {
            if p.is_empty() { bail!("truncated date/time cell"); }
            let len = p[0] as usize;
            let p = &p[1..];
            if len == 0 { return Ok((None, p)); }
            if p.len() < len { bail!("truncated date/time cell data"); }
            let text = datetimeish_to_text(meta, &p[..len])?;
            Ok((Some(text), &p[len..]))
        }
        TdsLenKind::UShortLen => {
            if meta.is_plp {
                read_plp_cell(p, meta)
            } else {
                if p.len() < 2 { bail!("truncated USHORTLEN_TYPE cell"); }
                let len = u16::from_le_bytes(p[0..2].try_into().unwrap());
                let p = &p[2..];
                if len == 0xFFFF { return Ok((None, p)); }
                let len = len as usize;
                if p.len() < len { bail!("truncated USHORTLEN_TYPE cell data"); }
                let text = charbin_to_text(meta, &p[..len])?;
                Ok((Some(text), &p[len..]))
            }
        }
        TdsLenKind::LongLen => read_longlen_cell(p, meta),
    }
}

/// PLP (Partially Length-Prefixed) row encoding, used by `VARCHAR(MAX)`/
/// `NVARCHAR(MAX)`/`VARBINARY(MAX)`: an 8-byte total length (or the
/// all-`0xFF` "unknown/NULL" sentinel), then a sequence of 4-byte
/// chunk-length + chunk-bytes, terminated by a 4-byte zero.
fn read_plp_cell<'a>(p: &'a [u8], meta: &ColumnMeta) -> Result<(Option<Vec<u8>>, &'a [u8])> {
    if p.len() < 8 { bail!("truncated PLP total length"); }
    let total_len = u64::from_le_bytes(p[0..8].try_into().unwrap());
    let mut p = &p[8..];
    if total_len == u64::MAX {
        return Ok((None, p));
    }
    let mut data = Vec::new();
    loop {
        if p.len() < 4 { bail!("truncated PLP chunk length"); }
        let chunk_len = u32::from_le_bytes(p[0..4].try_into().unwrap());
        p = &p[4..];
        if chunk_len == 0 { break; }
        let chunk_len = chunk_len as usize;
        if p.len() < chunk_len { bail!("truncated PLP chunk data"); }
        data.extend_from_slice(&p[..chunk_len]);
        p = &p[chunk_len..];
    }
    let text = charbin_to_text(meta, &data)?;
    Ok((Some(text), p))
}

/// Legacy `TEXT`/`NTEXT`/`IMAGE` row encoding: a 1-byte textptr length (0 =
/// NULL), else that many textptr bytes + an 8-byte timestamp + a 4-byte
/// actual data length + the data.
fn read_longlen_cell<'a>(p: &'a [u8], meta: &ColumnMeta) -> Result<(Option<Vec<u8>>, &'a [u8])> {
    if p.is_empty() { bail!("truncated LONGLEN_TYPE cell"); }
    let ptr_len = p[0] as usize;
    let mut p = &p[1..];
    if ptr_len == 0 {
        return Ok((None, p));
    }
    if p.len() < ptr_len + 8 + 4 { bail!("truncated LONGLEN_TYPE cell header"); }
    p = &p[ptr_len + 8..];
    let data_len = u32::from_le_bytes(p[0..4].try_into().unwrap()) as usize;
    p = &p[4..];
    if p.len() < data_len { bail!("truncated LONGLEN_TYPE cell data"); }
    let text = charbin_to_text(meta, &p[..data_len])?;
    Ok((Some(text), &p[data_len..]))
}

fn charbin_to_text(meta: &ColumnMeta, raw: &[u8]) -> Result<Vec<u8>> {
    match meta.tds_type {
        0xA5 | 0xAD | 0x22 => Ok(format!("\\x{}", hex_encode(raw)).into_bytes()), // binary types — Keystone bytea text format
        _ if meta.is_unicode => {
            if raw.len() % 2 != 0 { bail!("odd-length UTF-16LE cell"); }
            let units: Vec<u16> = raw.chunks_exact(2).map(|c| u16::from_le_bytes([c[0], c[1]])).collect();
            Ok(String::from_utf16_lossy(&units).into_bytes())
        }
        // 8-bit codepage char types — best-effort passthrough, not a full
        // SQL Server collation/codepage translation table.
        _ => Ok(raw.to_vec()),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0xF) as usize] as char);
    }
    s
}

fn fixed_to_text(tds_type: u8, raw: &[u8]) -> Result<Vec<u8>> {
    let text = match tds_type {
        0x30 => (raw[0] as u32).to_string(),
        0x32 => if raw[0] != 0 { "1".to_string() } else { "0".to_string() },
        0x34 => i16::from_le_bytes(raw[0..2].try_into().unwrap()).to_string(),
        0x38 => i32::from_le_bytes(raw[0..4].try_into().unwrap()).to_string(),
        0x7F => i64::from_le_bytes(raw[0..8].try_into().unwrap()).to_string(),
        0x3B => f32::from_le_bytes(raw[0..4].try_into().unwrap()).to_string(),
        0x3E => f64::from_le_bytes(raw[0..8].try_into().unwrap()).to_string(),
        0x3C => {
            let high = i32::from_le_bytes(raw[0..4].try_into().unwrap());
            let low = u32::from_le_bytes(raw[4..8].try_into().unwrap());
            scaled_i64_to_text(((high as i64) << 32) | (low as i64), 4)
        }
        0x7A => scaled_i64_to_text(i32::from_le_bytes(raw[0..4].try_into().unwrap()) as i64, 4),
        0x3D => datetime8_to_text(raw)?,
        0x3A => datetim4_to_text(raw)?,
        other => bail!("unsupported fixed-length TDS type 0x{:02X}", other),
    };
    Ok(text.into_bytes())
}

fn bytelen_to_text(meta: &ColumnMeta, raw: &[u8]) -> Result<Vec<u8>> {
    match meta.tds_type {
        0x24 => guid_to_text(raw),
        0x26 => fixed_to_text(int_n_underlying_type(raw.len())?, raw),
        0x68 => Ok(if raw[0] != 0 { b"1".to_vec() } else { b"0".to_vec() }),
        0x6D => fixed_to_text(if raw.len() == 4 { 0x3B } else { 0x3E }, raw),
        0x6E => fixed_to_text(if raw.len() == 4 { 0x7A } else { 0x3C }, raw),
        0x6F => fixed_to_text(if raw.len() == 4 { 0x3A } else { 0x3D }, raw),
        0x37 | 0x3F | 0x6A | 0x6C => {
            if raw.is_empty() { bail!("empty DECIMAL/NUMERIC cell"); }
            decimal_mantissa_to_text(&raw[1..], meta.scale, raw[0] == 0)
        }
        other => bail!("unsupported BYTELEN_TYPE TDS type 0x{:02X}", other),
    }
}

fn int_n_underlying_type(len: usize) -> Result<u8> {
    Ok(match len {
        1 => 0x30,
        2 => 0x34,
        4 => 0x38,
        8 => 0x7F,
        other => bail!("unsupported INTN width {other}"),
    })
}

fn guid_to_text(raw: &[u8]) -> Result<Vec<u8>> {
    if raw.len() != 16 { bail!("GUID cell wrong length: {}", raw.len()); }
    let d1 = u32::from_le_bytes(raw[0..4].try_into().unwrap());
    let d2 = u16::from_le_bytes(raw[4..6].try_into().unwrap());
    let d3 = u16::from_le_bytes(raw[6..8].try_into().unwrap());
    let d4 = &raw[8..16];
    Ok(format!(
        "{:08x}-{:04x}-{:04x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        d1, d2, d3, d4[0], d4[1], d4[2], d4[3], d4[4], d4[5], d4[6], d4[7]
    ).into_bytes())
}

/// Formats an integer scaled by `10^-scale` (MONEY-family types) as decimal text.
fn scaled_i64_to_text(value: i64, scale: u32) -> String {
    let negative = value < 0;
    let s = value.unsigned_abs().to_string();
    let scale = scale as usize;
    let text = if scale == 0 {
        s
    } else if s.len() <= scale {
        format!("0.{}{}", "0".repeat(scale - s.len()), s)
    } else {
        let split = s.len() - scale;
        format!("{}.{}", &s[..split], &s[split..])
    };
    if negative { format!("-{text}") } else { text }
}

/// Formats a DECIMAL/NUMERIC mantissa (sign byte already stripped, up to a
/// 16-byte little-endian unsigned integer) at the given `scale` as decimal text.
fn decimal_mantissa_to_text(raw: &[u8], scale: u8, negative: bool) -> Result<Vec<u8>> {
    if raw.len() > 16 { bail!("DECIMAL/NUMERIC mantissa too wide ({} bytes)", raw.len()); }
    let mut buf = [0u8; 16];
    buf[..raw.len()].copy_from_slice(raw);
    let mantissa = u128::from_le_bytes(buf);
    let s = mantissa.to_string();
    let scale = scale as usize;
    let text = if scale == 0 {
        s
    } else if s.len() <= scale {
        format!("0.{}{}", "0".repeat(scale - s.len()), s)
    } else {
        let split = s.len() - scale;
        format!("{}.{}", &s[..split], &s[split..])
    };
    let text = if negative && mantissa != 0 { format!("-{text}") } else { text };
    Ok(text.into_bytes())
}

/// Days since 1970-01-01 (Unix epoch) for a proleptic-Gregorian civil date —
/// Howard Hinnant's `days_from_civil`/`civil_from_days` algorithm
/// (http://howardhinnant.github.io/date_algorithms.html), used to convert
/// TDS's 1900-01-01/0001-01-01-epoch day counts without a date-library
/// dependency (matching this crate's no-heavy-dependency defaults).
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400;
    let mp = ((m as i64 + 9) % 12) as i64;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = (if z >= 0 { z } else { z - 146096 }) / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

fn datetime8_to_text(raw: &[u8]) -> Result<String> {
    let days = i32::from_le_bytes(raw[0..4].try_into().unwrap()) as i64;
    let ticks = u32::from_le_bytes(raw[4..8].try_into().unwrap());
    let (y, m, d) = civil_from_days(days + days_from_civil(1900, 1, 1));
    let total_ms = (ticks as u64 * 10) / 3; // 1 tick = 1/300s = 10/3 ms
    let (h, min, sec, ms) = (total_ms / 3_600_000, (total_ms / 60_000) % 60, (total_ms / 1000) % 60, total_ms % 1000);
    Ok(format!("{y:04}-{m:02}-{d:02} {h:02}:{min:02}:{sec:02}.{ms:03}"))
}

fn datetim4_to_text(raw: &[u8]) -> Result<String> {
    let days = u16::from_le_bytes(raw[0..2].try_into().unwrap()) as i64;
    let minutes = u16::from_le_bytes(raw[2..4].try_into().unwrap()) as i64;
    let (y, m, d) = civil_from_days(days + days_from_civil(1900, 1, 1));
    Ok(format!("{y:04}-{m:02}-{d:02} {:02}:{:02}:00", minutes / 60, minutes % 60))
}

fn datetimeish_to_text(meta: &ColumnMeta, raw: &[u8]) -> Result<Vec<u8>> {
    let text = match meta.tds_type {
        0x28 => {
            if raw.len() != 3 { bail!("DATE cell wrong length: {}", raw.len()); }
            let days = (raw[0] as i64) | ((raw[1] as i64) << 8) | ((raw[2] as i64) << 16);
            let (y, m, d) = civil_from_days(days + days_from_civil(1, 1, 1));
            format!("{y:04}-{m:02}-{d:02}")
        }
        0x29 => {
            let (h, min, sec, frac) = decode_time_bytes(raw, meta.scale)?;
            format!("{h:02}:{min:02}:{sec:02}{frac}")
        }
        0x2A => {
            if raw.len() < 3 { bail!("DATETIME2 cell too short"); }
            let (time_bytes, date_bytes) = raw.split_at(raw.len() - 3);
            let (h, min, sec, frac) = decode_time_bytes(time_bytes, meta.scale)?;
            let days = (date_bytes[0] as i64) | ((date_bytes[1] as i64) << 8) | ((date_bytes[2] as i64) << 16);
            let (y, m, d) = civil_from_days(days + days_from_civil(1, 1, 1));
            format!("{y:04}-{m:02}-{d:02} {h:02}:{min:02}:{sec:02}{frac}")
        }
        0x2B => {
            if raw.len() < 5 { bail!("DATETIMEOFFSET cell too short"); }
            let (rest, offset_bytes) = raw.split_at(raw.len() - 2);
            let offset_min = i16::from_le_bytes(offset_bytes.try_into().unwrap());
            let (time_bytes, date_bytes) = rest.split_at(rest.len() - 3);
            let (h, min, sec, frac) = decode_time_bytes(time_bytes, meta.scale)?;
            let days = (date_bytes[0] as i64) | ((date_bytes[1] as i64) << 8) | ((date_bytes[2] as i64) << 16);
            let (y, m, d) = civil_from_days(days + days_from_civil(1, 1, 1));
            let (sign, off_abs) = (if offset_min < 0 { '-' } else { '+' }, offset_min.unsigned_abs());
            format!("{y:04}-{m:02}-{d:02} {h:02}:{min:02}:{sec:02}{frac}{sign}{:02}:{:02}", off_abs / 60, off_abs % 60)
        }
        other => bail!("unsupported date/time TDS type 0x{:02X}", other),
    };
    Ok(text.into_bytes())
}

/// Decodes a TIME-family value: an integer count of `10^-scale`-second
/// units since midnight, little-endian in 3/4/5 bytes depending on scale.
/// Returns `(hour, minute, second, ".fractional" or "")`.
fn decode_time_bytes(raw: &[u8], scale: u8) -> Result<(u64, u64, u64, String)> {
    if raw.len() > 8 { bail!("TIME cell too wide ({} bytes)", raw.len()); }
    let mut buf = [0u8; 8];
    buf[..raw.len()].copy_from_slice(raw);
    let value = u64::from_le_bytes(buf);
    let scale = (scale as u32).min(7);
    let hundred_ns = value * 10u64.pow(7 - scale);
    let total_sec = hundred_ns / 10_000_000;
    let frac_100ns = hundred_ns % 10_000_000;
    let frac = if scale == 0 { String::new() } else { format!(".{}", &format!("{frac_100ns:07}")[..scale as usize]) };
    Ok((total_sec / 3600, (total_sec / 60) % 60, total_sec % 60, frac))
}

// ── CDC ──────────────────────────────────────────────────────────────

/// SQL Server LSNs are natively a 10-byte `varbinary`; represented on the
/// wire (and as Harbor's resume token) as a `0x`-prefixed hex string, the
/// same convention SSMS/native tools use.
fn lsn_to_hex(lsn: &[u8]) -> String {
    format!("0x{}", hex_encode(lsn))
}

fn hex_to_lsn(s: &str) -> Result<Vec<u8>> {
    let s = s.trim();
    let s = s.strip_prefix("0x").or_else(|| s.strip_prefix("\\x")).unwrap_or(s);
    if s.is_empty() || s.len() % 2 != 0 { bail!("invalid LSN hex string: {s:?}"); }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).with_context(|| format!("invalid LSN hex string: {s:?}")))
        .collect()
}

/// Maps one `cdc.fn_cdc_get_all_changes_<instance>` result row to a
/// `ChangeEvent`. That function's rows are shaped as four metadata columns
/// (`__$start_lsn`, `__$seqval`, `__$operation`, `__$update_mask`) followed
/// by the captured table's columns in declared order. `__$operation`:
/// 1=delete, 2=insert, 3=update-before-image (skipped — informational
/// only, same simplification as Postgres's own TOAST-column handling),
/// 4=update-after-image.
fn map_cdc_operation(cells: &[Option<Vec<u8>>], table: &TableSchema) -> Result<Option<ChangeEvent>> {
    if cells.len() < 4 { bail!("CDC change row missing the four __$ metadata columns"); }
    let op: i64 = cells[2]
        .as_ref()
        .map(|b| String::from_utf8_lossy(b).trim().to_string())
        .context("CDC change row missing __$operation")?
        .parse()
        .context("parsing __$operation")?;
    let row: SourceRow = cells[4..].to_vec();
    let pk_cols = table.primary_key_columns();
    let key: SourceRow = table.columns.iter().zip(row.iter()).map(|(c, v)| if pk_cols.contains(&c.name.as_str()) { v.clone() } else { None }).collect();
    Ok(match op {
        2 => Some(ChangeEvent::Insert { table: table.name.clone(), row }),
        4 => Some(ChangeEvent::Update { table: table.name.clone(), key, row }),
        1 => Some(ChangeEvent::Delete { table: table.name.clone(), key }),
        _ => None,
    })
}

impl TdsConn {
    async fn connect(addr: &str, user: &str, password: &str, database: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr).await.with_context(|| format!("connecting to SQL Server at {addr}"))?;
        let mut conn = Self {
            stream,
            read_buf: BytesMut::with_capacity(16384),
            write_buf: BytesMut::with_capacity(16384),
            packet_id: 0,
        };

        // TDS 7.4 pre-login handshake
        conn.send_pre_login().await?;
        conn.read_pre_login_response().await?;

        // TDS login
        conn.send_login(user, password, database).await?;
        let login_ok = conn.read_response().await?;
        if !login_ok {
            bail!("SQL Server login rejected");
        }

        Ok(conn)
    }

    async fn send_pre_login(&mut self) -> Result<()> {
        // Pre-login packet with version and encryption option
        let mut token_data = BytesMut::new();
        // TOKEN: VERSION = 0x00
        token_data.put_u8(0x00);
        token_data.put_u16(6); // offset
        token_data.put_u16(6); // length (6 bytes for version)

        // TOKEN: ENCRYPTION = 0x01
        token_data.put_u8(0x01);
        token_data.put_u16(12); // offset
        token_data.put_u16(1);  // length

        // TOKEN: TERMINATOR = 0xFF
        token_data.put_u8(0xFF);

        // Version data: TDS version 7.4 (0x74000004)
        token_data.put_u32(0x00000004); // major/minor
        token_data.put_u16(0x0007); // subversion

        // Encryption: 0x02 = NotSup
        token_data.put_u8(0x02);

        self.send_tds_packet(0x12, 0, &token_data).await // 0x12 = Pre-Login
    }

    async fn read_pre_login_response(&mut self) -> Result<()> {
        loop {
            let header = self.read_tds_header().await?;
            if header.status & 1 == 0 {
                // Last packet
                let _body = self.read_exact(header.length as usize - 8).await?;
                return Ok(());
            }
            let _body = self.read_exact(header.length as usize - 8).await?;
        }
    }

    async fn send_login(&mut self, user: &str, password: &str, database: &str) -> Result<()> {
        let mut body = BytesMut::new();

        // TDS 7.4 login header
        let _length_offset = body.len();
        body.put_u32_le(0); // length (filled later)
        body.put_u32_le(0); // tds_version
        body.put_u32_le(0); // packet_size
        body.put_u32_le(0); // client_prog_ver
        body.put_u32_le(0); // client_pid
        body.put_u32_le(0); // connection_id
        body.put_u8(0);     // option_flags1
        body.put_u8(0);     // option_flags2
        body.put_u8(0);     // type_flags
        body.put_u8(0);     // option_flags3
        body.put_u16_le(0); // time_zone
        body.put_u16_le(0); // collation

        // Variable-length data starts here
        let mut data = BytesMut::new();

        // Adds a string field (TDS uses UCS-2LE for login strings), taking
        // `data` as an explicit parameter rather than a captured closure —
        // a closure holding a mutable borrow of `data` for its whole
        // lifetime would conflict with the direct `data.len()`/`put_slice`
        // calls below (password field, XOR-encoded rather than UCS-2).
        fn add_str(data: &mut BytesMut, s: &str) -> (u16, u16) {
            let offset = data.len() as u16;
            for ch in s.chars() {
                data.put_u16_le(ch as u16);
            }
            (offset, s.len() as u16)
        }

        // Hostname (offset 0)
        let hostname = add_str(&mut data, "");
        // Username
        let username = add_str(&mut data, user);
        // Password (XOR-encoded)
        let pwd_offset = data.len() as u16;
        let pwd_bytes: Vec<u8> = password.bytes().map(|b| b ^ 0xA5).collect();
        let pwd_len = password.len() as u16;
        data.put_slice(&pwd_bytes);
        // App name
        let appname = add_str(&mut data, "tpt-keystone-harbor");
        // Server name (empty = use connection addr)
        let servername = add_str(&mut data, "");
        // Extension
        let _extension = (0u16, 0u16);
        // Ctl int name
        let _ctl = add_str(&mut data, "");
        // Language
        let _lang = add_str(&mut data, "");
        // Database name
        let dbname = if database.is_empty() { (0u16, 0u16) } else { add_str(&mut data, database) };

        // Build the fixed header with offsets
        let var_offset_base = 86; // fixed header size before variable data
        let total_data_offset = var_offset_base;

        body.truncate(0);
        // Length (will be filled in)
        let len_offset = body.len();
        body.put_u32_le(0);
        // TDS version 7.4
        body.put_u32_le(0x74000004);
        // Packet size
        body.put_u32_le(4096);
        // Client prog ver
        body.put_u32_le(0);
        // Client PID
        body.put_u32_le(std::process::id());
        // Connection ID
        body.put_u32_le(0);
        // Option flags
        body.put_u8(0);
        body.put_u8(0);
        body.put_u8(0);
        body.put_u8(0);
        // Time zone
        body.put_u16_le(0);
        // Collation
        body.put_u16_le(0);

        // Now write the offset/length pairs for all 7 string fields
        let fields = [hostname, username, (pwd_offset, pwd_len), appname, servername, _extension, _ctl, _lang, dbname];
        for &(off, len) in &fields {
            body.put_u16_le(off + total_data_offset as u16);
            body.put_u16_le(len * 2); // UCS-2 is 2 bytes per char
        }

        // Append the actual string data
        body.put_slice(&data);

        // Fill in the length
        let total_len = body.len() as u32;
        body[len_offset..len_offset + 4].copy_from_slice(&total_len.to_le_bytes());

        self.send_tds_packet(0x10, 0, &body).await // 0x10 = Login
    }

    async fn read_response(&mut self) -> Result<bool> {
        loop {
            let header = self.read_tds_header().await?;
            let body = self.read_exact(header.length as usize - 8).await?;

            if header.status & 1 == 0 {
                // Last packet — parse tokens
                return self.parse_token_stream(&body);
            }
        }
    }

    fn parse_token_stream(&self, body: &[u8]) -> Result<bool> {
        let mut p = body;
        while !p.is_empty() {
            let token = p[0];
            p = &p[1..];
            match token {
                0xAD => {
                    // LOGINACK
                    if p.len() < 1 { break; }
                    let _ack_len = p[0] as usize;
                    p = &p[1..];
                    if p.len() < 8 { break; }
                    let _interface = p[0];
                    let _tds_version = u32::from_be_bytes(p[1..5].try_into().unwrap_or([0;4]));
                    let prog_name_len = p[5] as usize;
                    p = &p[6..];
                    if p.len() < prog_name_len + 4 { break; }
                    p = &p[prog_name_len..];
                    let _prog_ver = u32::from_be_bytes(p[0..4].try_into().unwrap_or([0;4]));
                    return Ok(true);
                }
                0xE3 => {
                    // ENVCHANGE
                    if p.len() < 2 { break; }
                    let len = u16::from_le_bytes(p[0..2].try_into().unwrap_or([0,0])) as usize;
                    p = &p[2..];
                    if p.len() < len { break; }
                    p = &p[len..];
                }
                0xAA => {
                    // ERROR
                    if p.len() < 2 { break; }
                    let len = u16::from_le_bytes(p[0..2].try_into().unwrap_or([0,0])) as usize;
                    p = &p[2..];
                    if p.len() < len { break; }
                    p = &p[len..];
                }
                0xAB => {
                    // INFO
                    if p.len() < 2 { break; }
                    let len = u16::from_le_bytes(p[0..2].try_into().unwrap_or([0,0])) as usize;
                    p = &p[2..];
                    if p.len() < len { break; }
                    p = &p[len..];
                }
                0xFD => {
                    // DONE — end of stream
                    return Ok(true);
                }
                0xFE => {
                    // DONEPROC
                    return Ok(true);
                }
                _ => break,
            }
        }
        Ok(true)
    }

    async fn query(&mut self, sql: &str) -> Result<QueryResult> {
        // SQL Batch = 0x01
        let mut body = BytesMut::new();
        // All headers length (4 bytes, just 0 for a simple batch)
        body.put_u32_le(0);
        body.put_slice(sql.as_bytes());
        self.send_tds_packet(0x01, 0, &body).await?;

        // Read response packets
        let mut all_data = BytesMut::new();
        loop {
            let header = self.read_tds_header().await?;
            let body = self.read_exact(header.length as usize - 8).await?;
            all_data.extend_from_slice(&body);
            if header.status & 1 == 0 {
                break;
            }
        }

        Self::parse_result_set(&all_data)
    }

    /// No `self` needed — pure byte-parsing, kept as an associated function
    /// (rather than a free one) purely to stay grouped with `query()`;
    /// callable directly (`TdsConn::parse_result_set`) for unit tests
    /// without a live connection.
    fn parse_result_set(body: &[u8]) -> Result<QueryResult> {
        let mut p = body;
        let mut rows = Vec::new();
        let mut columns: Vec<ColumnMeta> = Vec::new();

        while !p.is_empty() {
            let token = p[0];
            p = &p[1..];
            match token {
                0x81 => {
                    // COLMETADATA (MS-TDS §2.2.7.4): Count(2) then, per
                    // column, UserType(4) + Flags(2) + TYPE_INFO + ColName
                    // (B_VARCHAR). 0xFFFF is the "no metadata" sentinel.
                    if p.len() < 2 { break; }
                    let col_count = u16::from_le_bytes(p[0..2].try_into().unwrap_or([0, 0])) as usize;
                    p = &p[2..];
                    if col_count == 0xFFFF {
                        columns.clear();
                        continue;
                    }
                    let mut cols = Vec::with_capacity(col_count);
                    for _ in 0..col_count {
                        if p.len() < 6 { bail!("truncated COLMETADATA column header"); }
                        p = &p[6..]; // UserType + Flags
                        let (meta, rest) = parse_type_info(p)?;
                        p = rest;
                        if p.is_empty() { bail!("truncated COLMETADATA column name"); }
                        let name_bytes = (p[0] as usize) * 2;
                        p = &p[1..];
                        if p.len() < name_bytes { bail!("truncated COLMETADATA column name data"); }
                        p = &p[name_bytes..];
                        cols.push(meta);
                    }
                    columns = cols;
                }
                0xD1 | 0xC1 => {
                    // ROW / old-style ROW — typed per-column decode driven
                    // by the COLMETADATA parsed above, not a delimiter scan.
                    let mut cells = Vec::with_capacity(columns.len());
                    for meta in &columns {
                        let (cell, rest) = read_cell(p, meta)?;
                        cells.push(cell);
                        p = rest;
                    }
                    rows.push(MssqlRow { cells });
                }
                0xD2 => bail!("NBCROW (compressed row) is not supported by this connector"),
                0xFD | 0xFE => break, // DONE / DONEPROC
                0xC3 | 0xC2 => {
                    // ROWFMT (old-style)
                    let len = if token == 0xC3 {
                        if p.len() < 2 { break; }
                        let l = u16::from_le_bytes(p[0..2].try_into().unwrap_or([0, 0]));
                        p = &p[2..];
                        l as usize
                    } else {
                        if p.is_empty() { break; }
                        let l = p[0] as usize;
                        p = &p[1..];
                        l
                    };
                    if p.len() < len { break; }
                    p = &p[len..];
                }
                _ => break,
            }
        }

        Ok(QueryResult { rows })
    }

    async fn send_tds_packet(&mut self, packet_type: u8, status: u8, body: &[u8]) -> Result<()> {
        let len = 8 + body.len();
        self.write_buf.put_u8(packet_type);
        self.write_buf.put_u8(status);
        self.write_buf.put_u16_le(len as u16);
        self.write_buf.put_u16_le(0); // spid
        self.write_buf.put_u8(self.packet_id);
        self.write_buf.put_u8(0); // window
        self.write_buf.put_slice(body);
        self.stream.write_all(&self.write_buf).await?;
        self.stream.flush().await?;
        self.write_buf.clear();
        self.packet_id = self.packet_id.wrapping_add(1);
        Ok(())
    }

    async fn read_tds_header(&mut self) -> Result<TdsHeader> {
        self.fill(8).await?;
        let _packet_type = self.read_buf[0];
        let status = self.read_buf[1];
        let length = u16::from_le_bytes(self.read_buf[2..4].try_into().unwrap());
        let _spid = u16::from_le_bytes(self.read_buf[4..6].try_into().unwrap());
        let _packet_id = self.read_buf[6];
        let _window = self.read_buf[7];
        self.read_buf.advance(8);
        Ok(TdsHeader { status, length })
    }

    async fn read_exact(&mut self, n: usize) -> Result<Vec<u8>> {
        self.fill(n).await?;
        let data = self.read_buf.split_to(n).to_vec();
        Ok(data)
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
}

struct TdsHeader {
    status: u8,
    length: u16,
}

pub struct MsSqlSource {
    conn: TdsConn,
}

impl MsSqlSource {
    pub async fn connect(addr: &str, user: &str, database: &str) -> Result<Self> {
        // For SQL Server, use empty password with trusted connection,
        // or the user can provide credentials via the connection string
        let conn = TdsConn::connect(addr, user, "", database).await?;
        Ok(Self { conn })
    }

    /// Checks CDC is enabled at the database level and for every table
    /// about to be replicated, returning each table's `capture_instance`
    /// name (needed to address `cdc.fn_cdc_get_all_changes_<instance>`).
    /// Enabling CDC itself is left to the DBA — it's an elevated-permission,
    /// operationally significant decision Harbor shouldn't make silently.
    async fn check_cdc_enabled(&mut self, tables: &[TableSchema]) -> Result<HashMap<String, String>> {
        let db_res = self.conn.query("SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()").await?;
        let db_enabled = db_res
            .rows
            .first()
            .and_then(|r| r.cells.first())
            .and_then(|c| c.as_ref())
            .map(|b| String::from_utf8_lossy(b).trim() == "1")
            .unwrap_or(false);
        if !db_enabled {
            bail!("CDC is not enabled on this database. Run: EXEC sys.sp_cdc_enable_db;");
        }

        let mut instances = HashMap::new();
        let mut missing = Vec::new();
        for table in tables {
            let res = self
                .conn
                .query(&format!(
                    "SELECT ct.capture_instance FROM cdc.change_tables ct \
                     JOIN sys.tables t ON ct.source_object_id = t.object_id \
                     JOIN sys.schemas s ON t.schema_id = s.schema_id \
                     WHERE s.name = '{}' AND t.name = '{}'",
                    table.schema, table.name
                ))
                .await?;
            match res.rows.first().and_then(|r| r.cells.first()).and_then(|c| c.as_ref()) {
                Some(b) => {
                    instances.insert(format!("{}.{}", table.schema, table.name), String::from_utf8_lossy(b).to_string());
                }
                None => missing.push(format!("{}.{}", table.schema, table.name)),
            }
        }
        if !missing.is_empty() {
            bail!(
                "CDC is not enabled on: {}. Run for each: EXEC sys.sp_cdc_enable_table @source_schema=N'<schema>', @source_name=N'<table>', @role_name=NULL;",
                missing.join(", ")
            );
        }
        Ok(instances)
    }
}

#[async_trait]
impl SourceConnector for MsSqlSource {
    fn name(&self) -> &'static str {
        "Harbor/MSSQL"
    }

    async fn discover(&mut self) -> Result<Vec<TableSchema>, ConnectorError> {
        let tables_res = self
            .conn
            .query("SELECT table_schema, table_name FROM information_schema.tables \
                    WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('sys', 'INFORMATION_SCHEMA')")
            .await
            .map_err(ConnectorError::Other)?;

        let mut tables = Vec::new();
        for row in &tables_res.rows {
            let cells = &row.cells;
            let schema = cells.get(0).and_then(|c| c.as_ref()).map(|b| String::from_utf8_lossy(b).to_string()).unwrap_or_default();
            let name = cells.get(1).and_then(|c| c.as_ref()).map(|b| String::from_utf8_lossy(b).to_string()).unwrap_or_default();
            if name.is_empty() {
                continue;
            }

            let cols_res = self
                .conn
                .query(&format!(
                    "SELECT c.column_name, c.data_type, c.is_nullable, \
                     CASE WHEN pk.column_name IS NOT NULL THEN 'YES' ELSE 'NO' END AS is_pk \
                     FROM information_schema.columns c \
                     LEFT JOIN ( \
                         SELECT ku.column_name, ku.table_schema, ku.table_name \
                         FROM information_schema.table_constraints tc \
                         JOIN information_schema.key_column_usage ku \
                           ON tc.constraint_name = ku.constraint_name AND tc.table_schema = ku.table_schema \
                         WHERE tc.constraint_type = 'PRIMARY KEY' \
                     ) pk ON pk.column_name = c.column_name AND pk.table_schema = c.table_schema AND pk.table_name = c.table_name \
                     WHERE c.table_schema = '{schema}' AND c.table_name = '{name}' \
                     ORDER BY c.ordinal_position"
                ))
                .await
                .map_err(ConnectorError::Other)?;

            let columns: Vec<ColumnSchema> = cols_res
                .rows
                .iter()
                .map(|r| {
                    let cells = &r.cells;
                    let col_name = cells.get(0).and_then(|c| c.as_ref()).map(|b| String::from_utf8_lossy(b).to_string()).unwrap_or_default();
                    let source_type = cells.get(1).and_then(|c| c.as_ref()).map(|b| String::from_utf8_lossy(b).to_string()).unwrap_or_default();
                    let nullable_str = cells.get(2).and_then(|c| c.as_ref()).map(|b| String::from_utf8_lossy(b).to_string()).unwrap_or_default();
                    let pk_str = cells.get(3).and_then(|c| c.as_ref()).map(|b| String::from_utf8_lossy(b).to_string()).unwrap_or_default();
                    ColumnSchema {
                        keystone_type: from_mssql_type(&source_type),
                        nullable: nullable_str == "YES",
                        is_primary_key: pk_str == "YES",
                        name: col_name,
                        source_type,
                    }
                })
                .collect();

            tables.push(TableSchema { schema, name, columns, indexes: vec![], topic_partitions: None });
        }
        Ok(tables)
    }

    async fn snapshot_table(&mut self, table: &TableSchema, tx: Sender<Vec<SourceRow>>) -> Result<u64, ConnectorError> {
        let res = self
            .conn
            .query(&format!("SELECT * FROM [{}].[{}]", table.schema, table.name))
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

    async fn replicate(&mut self, tables: &[TableSchema], resume_token: Option<String>, tx: Sender<ChangeEvent>) -> Result<(), ConnectorError> {
        let capture_instances = self.check_cdc_enabled(tables).await.map_err(ConnectorError::Other)?;

        let mut last_lsn: Vec<u8> = match resume_token {
            Some(t) => hex_to_lsn(&t).map_err(ConnectorError::Other)?,
            None => {
                // First run: start from the earliest LSN any captured table
                // still has change data for.
                let mut min_lsn: Option<Vec<u8>> = None;
                for instance in capture_instances.values() {
                    let res = self
                        .conn
                        .query(&format!("SELECT sys.fn_cdc_get_min_lsn('{instance}')"))
                        .await
                        .map_err(ConnectorError::Other)?;
                    if let Some(cell) = res.rows.first().and_then(|r| r.cells.first()).and_then(|c| c.as_ref()) {
                        let lsn = hex_to_lsn(&String::from_utf8_lossy(cell)).map_err(ConnectorError::Other)?;
                        if min_lsn.as_ref().is_none_or(|m| lsn < *m) {
                            min_lsn = Some(lsn);
                        }
                    }
                }
                min_lsn.ok_or_else(|| ConnectorError::Other(anyhow::anyhow!("could not determine a starting LSN for CDC (no captured tables?)")))?
            }
        };

        loop {
            let max_res = self.conn.query("SELECT sys.fn_cdc_get_max_lsn()").await.map_err(ConnectorError::Other)?;
            let max_lsn = match max_res.rows.first().and_then(|r| r.cells.first()).and_then(|c| c.as_ref()) {
                Some(cell) => hex_to_lsn(&String::from_utf8_lossy(cell)).map_err(ConnectorError::Other)?,
                None => Vec::new(),
            };

            if max_lsn.is_empty() || max_lsn <= last_lsn {
                tokio::time::sleep(CDC_POLL_INTERVAL).await;
                continue;
            }

            let from_res = self
                .conn
                .query(&format!("SELECT sys.fn_cdc_increment_lsn({})", lsn_to_hex(&last_lsn)))
                .await
                .map_err(ConnectorError::Other)?;
            let from_lsn = match from_res.rows.first().and_then(|r| r.cells.first()).and_then(|c| c.as_ref()) {
                Some(cell) => hex_to_lsn(&String::from_utf8_lossy(cell)).map_err(ConnectorError::Other)?,
                None => last_lsn.clone(),
            };

            for table in tables {
                let Some(instance) = capture_instances.get(&format!("{}.{}", table.schema, table.name)) else { continue };
                let sql = format!(
                    "SELECT * FROM cdc.fn_cdc_get_all_changes_{instance}({}, {}, 'all') ORDER BY __$start_lsn, __$seqval",
                    lsn_to_hex(&from_lsn),
                    lsn_to_hex(&max_lsn)
                );
                let res = self.conn.query(&sql).await.map_err(ConnectorError::Other)?;
                for row in &res.rows {
                    if let Some(event) = map_cdc_operation(&row.cells, table).map_err(ConnectorError::Other)? {
                        if tx.send(event).await.is_err() {
                            return Ok(());
                        }
                    }
                }
            }

            last_lsn = max_lsn;
            if tx.send(ChangeEvent::CommitLsn(lsn_to_hex(&last_lsn))).await.is_err() {
                return Ok(());
            }
        }
    }

    async fn row_checksums(&mut self, table: &TableSchema) -> Result<Vec<u64>, ConnectorError> {
        let pk = table.primary_key_columns();
        let order_by = if pk.is_empty() { String::new() } else { format!(" ORDER BY {}", pk.join(", ")) };
        let res = self
            .conn
            .query(&format!("SELECT * FROM [{}].[{}]{}", table.schema, table.name, order_by))
            .await
            .map_err(ConnectorError::Other)?;
        Ok(res.rows.iter().map(|r| crate::verify::hash_row(&r.cells)).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ucs2le(s: &str) -> Vec<u8> {
        let mut out = Vec::new();
        for ch in s.chars() {
            out.extend_from_slice(&(ch as u16).to_le_bytes());
        }
        out
    }

    fn col_header(name: &str, type_info: &[u8]) -> Vec<u8> {
        let mut out = vec![0, 0, 0, 0, 0, 0]; // UserType(4) + Flags(2)
        out.extend_from_slice(type_info);
        let name_bytes = ucs2le(name);
        out.push((name_bytes.len() / 2) as u8);
        out.extend_from_slice(&name_bytes);
        out
    }

    fn colmetadata(cols: &[Vec<u8>]) -> Vec<u8> {
        let mut out = vec![0x81];
        out.extend_from_slice(&(cols.len() as u16).to_le_bytes());
        for c in cols {
            out.extend_from_slice(c);
        }
        out
    }

    #[test]
    fn parses_typed_row_int_and_nvarchar() {
        let mut body = colmetadata(&[col_header("id", &[0x38]), col_header("name", &{
            let mut ti = vec![0xE7];
            ti.extend_from_slice(&100u16.to_le_bytes());
            ti.extend_from_slice(&[0u8; 5]);
            ti
        })]);
        body.push(0xD1); // ROW
        body.extend_from_slice(&42i32.to_le_bytes());
        let name_utf16 = ucs2le("Ada");
        body.extend_from_slice(&(name_utf16.len() as u16).to_le_bytes());
        body.extend_from_slice(&name_utf16);
        body.push(0xFD); // DONE

        let result = TdsConn::parse_result_set(&body).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].cells[0], Some(b"42".to_vec()));
        assert_eq!(result.rows[0].cells[1], Some(b"Ada".to_vec()));
    }

    #[test]
    fn parses_null_and_multiple_rows() {
        let mut body = colmetadata(&[col_header("n", &{
            let mut ti = vec![0xE7];
            ti.extend_from_slice(&100u16.to_le_bytes());
            ti.extend_from_slice(&[0u8; 5]);
            ti
        })]);
        body.push(0xD1);
        body.extend_from_slice(&0xFFFFu16.to_le_bytes()); // NULL sentinel
        body.push(0xD1);
        let val = ucs2le("x");
        body.extend_from_slice(&(val.len() as u16).to_le_bytes());
        body.extend_from_slice(&val);
        body.push(0xFD);

        let result = TdsConn::parse_result_set(&body).unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].cells[0], None);
        assert_eq!(result.rows[1].cells[0], Some(b"x".to_vec()));
    }

    #[test]
    fn parses_decimal_and_bigint() {
        let mut body = colmetadata(&[
            col_header("amount", &[0x6A, 17, 10, 2]), // DECIMALN, max_len=17, precision=10, scale=2
            col_header("big", &[0x7F]),
        ]);
        body.push(0xD1);
        // DECIMAL cell: 1-byte length + sign(1=positive) + mantissa LE (12345 -> "123.45" at scale 2)
        body.push(3); // length: sign + 2-byte mantissa
        body.push(1); // positive
        body.extend_from_slice(&12345u16.to_le_bytes());
        body.extend_from_slice(&9_999_999_999_i64.to_le_bytes());
        body.push(0xFD);

        let result = TdsConn::parse_result_set(&body).unwrap();
        assert_eq!(result.rows[0].cells[0], Some(b"123.45".to_vec()));
        assert_eq!(result.rows[0].cells[1], Some(b"9999999999".to_vec()));
    }

    #[test]
    fn parses_datetime_and_date() {
        let mut body = colmetadata(&[col_header("d", &[0x3D]), col_header("dt", &[0x28])]);
        body.push(0xD1);
        let days_since_1900 = (days_from_civil(2000, 1, 1) - days_from_civil(1900, 1, 1)) as i32;
        body.extend_from_slice(&days_since_1900.to_le_bytes());
        body.extend_from_slice(&0u32.to_le_bytes()); // midnight
        body.push(3); // DATE length
        let days_since_0001 = days_from_civil(2000, 1, 1) - days_from_civil(1, 1, 1);
        body.push((days_since_0001 & 0xFF) as u8);
        body.push(((days_since_0001 >> 8) & 0xFF) as u8);
        body.push(((days_since_0001 >> 16) & 0xFF) as u8);
        body.push(0xFD);

        let result = TdsConn::parse_result_set(&body).unwrap();
        assert_eq!(result.rows[0].cells[0], Some(b"2000-01-01 00:00:00.000".to_vec()));
        assert_eq!(result.rows[0].cells[1], Some(b"2000-01-01".to_vec()));
    }

    #[test]
    fn lsn_hex_round_trips() {
        let lsn = vec![0x00, 0x00, 0x00, 0x2A, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01];
        let hex = lsn_to_hex(&lsn);
        assert_eq!(hex, "0x0000002a000000010001");
        assert_eq!(hex_to_lsn(&hex).unwrap(), lsn);
        assert_eq!(hex_to_lsn("0000002A000000010001").unwrap(), lsn);
    }

    #[test]
    fn hex_to_lsn_rejects_odd_length() {
        assert!(hex_to_lsn("0x123").is_err());
    }

    #[test]
    fn civil_days_round_trip() {
        for (y, m, d) in [(1970, 1, 1), (1900, 1, 1), (2000, 1, 1), (1, 1, 1), (2024, 2, 29), (1969, 12, 31)] {
            let days = days_from_civil(y, m, d);
            assert_eq!(civil_from_days(days), (y, m, d), "round trip failed for {y}-{m}-{d}");
        }
        assert_eq!(days_from_civil(1970, 1, 1), 0);
    }

    #[test]
    fn scaled_i64_formats_money() {
        assert_eq!(scaled_i64_to_text(1234, 4), "0.1234");
        assert_eq!(scaled_i64_to_text(123456, 4), "12.3456");
        assert_eq!(scaled_i64_to_text(-500, 4), "-0.0500");
        assert_eq!(scaled_i64_to_text(0, 4), "0.0000");
    }

    #[test]
    fn guid_formats_standard_layout() {
        // {01020304-0506-0708-090A-0B0C0D0E0F10}-style bytes, mixed-endian per TDS.
        let raw: Vec<u8> = vec![0x04, 0x03, 0x02, 0x01, 0x06, 0x05, 0x08, 0x07, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10];
        let text = String::from_utf8(guid_to_text(&raw).unwrap()).unwrap();
        assert_eq!(text, "01020304-0506-0708-090a-0b0c0d0e0f10");
    }

    fn table_with_pk() -> TableSchema {
        TableSchema {
            schema: "dbo".into(),
            name: "users".into(),
            columns: vec![
                ColumnSchema { name: "id".into(), source_type: "int".into(), keystone_type: "INTEGER".into(), nullable: false, is_primary_key: true },
                ColumnSchema { name: "name".into(), source_type: "nvarchar".into(), keystone_type: "TEXT".into(), nullable: true, is_primary_key: false },
            ],
            indexes: vec![],
            topic_partitions: None,
        }
    }

    fn cdc_row(op: i64, id: &str, name: Option<&str>) -> Vec<Option<Vec<u8>>> {
        vec![
            Some(b"0x00".to_vec()),      // __$start_lsn
            Some(b"0x00".to_vec()),      // __$seqval
            Some(op.to_string().into_bytes()), // __$operation
            None,                        // __$update_mask
            Some(id.as_bytes().to_vec()),
            name.map(|n| n.as_bytes().to_vec()),
        ]
    }

    #[test]
    fn map_cdc_operation_insert_update_delete() {
        let table = table_with_pk();

        let insert = map_cdc_operation(&cdc_row(2, "1", Some("Ada")), &table).unwrap().unwrap();
        assert!(matches!(insert, ChangeEvent::Insert { row, .. } if row[0] == Some(b"1".to_vec())));

        let update = map_cdc_operation(&cdc_row(4, "1", Some("Ada Lovelace")), &table).unwrap().unwrap();
        match update {
            ChangeEvent::Update { key, row, .. } => {
                assert_eq!(key[0], Some(b"1".to_vec()));
                assert_eq!(key[1], None); // non-PK column not in key
                assert_eq!(row[1], Some(b"Ada Lovelace".to_vec()));
            }
            other => panic!("expected Update, got {other:?}"),
        }

        let delete = map_cdc_operation(&cdc_row(1, "1", Some("Ada")), &table).unwrap().unwrap();
        assert!(matches!(delete, ChangeEvent::Delete { key, .. } if key[0] == Some(b"1".to_vec())));

        // op 3 (update-before image) is informational only — skipped.
        assert!(map_cdc_operation(&cdc_row(3, "1", Some("Ada")), &table).unwrap().is_none());
    }
}
