//! Hand-written computational geometry: WKT parsing/serialization, distance,
//! bounding boxes, and point-in-polygon — the "replaces GEOS" piece of
//! Meridian. Deliberately narrow: enough geometry to support `ST_*` scalar
//! functions and spatial indexing, not a full computational-geometry suite
//! (no buffering, no polygon boolean ops, no CRS reprojection).

use anyhow::{bail, Result};

/// Mean Earth radius in meters (IUGG mean radius), used for great-circle
/// distance. Good enough for "within N meters" queries; not geodesically
/// exact (a full WGS84 ellipsoidal solution is out of scope here).
pub const EARTH_RADIUS_M: f64 = 6_371_008.8;

/// A single coordinate. `z` (altitude, meters) and `t` (time, unix
/// milliseconds) are both optional, giving Meridian's "4D spatiotemporal"
/// point type: `(x=lon, y=lat, z=alt, t=time)`. WKT spells `t` as the `M`
/// ("measure") ordinate, since standard WKT has no native time axis.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Coord {
    pub x: f64,
    pub y: f64,
    pub z: Option<f64>,
    pub t: Option<i64>,
}

impl Coord {
    pub fn xy(x: f64, y: f64) -> Self {
        Self {
            x,
            y,
            z: None,
            t: None,
        }
    }
}

/// The subset of OGC Simple Features geometry types Meridian understands.
#[derive(Debug, Clone, PartialEq)]
pub enum Geometry {
    Point(Coord),
    LineString(Vec<Coord>),
    Polygon(Vec<Vec<Coord>>), // rings: [0] = exterior, rest = holes
}

/// Axis-aligned bounding box in (lon, lat).
#[derive(Debug, Clone, Copy)]
pub struct BBox {
    pub min_x: f64,
    pub min_y: f64,
    pub max_x: f64,
    pub max_y: f64,
}

impl Geometry {
    pub fn bbox(&self) -> BBox {
        let mut b = BBox {
            min_x: f64::INFINITY,
            min_y: f64::INFINITY,
            max_x: f64::NEG_INFINITY,
            max_y: f64::NEG_INFINITY,
        };
        let mut acc = |c: &Coord| {
            b.min_x = b.min_x.min(c.x);
            b.min_y = b.min_y.min(c.y);
            b.max_x = b.max_x.max(c.x);
            b.max_y = b.max_y.max(c.y);
        };
        match self {
            Geometry::Point(c) => acc(c),
            Geometry::LineString(pts) => pts.iter().for_each(&mut acc),
            Geometry::Polygon(rings) => rings.iter().flatten().for_each(&mut acc),
        }
        b
    }

    /// The representative point used for distance/index purposes: the
    /// point itself, the line's first vertex, or the exterior ring's
    /// centroid (arithmetic mean of vertices — not area-weighted).
    pub fn representative_point(&self) -> Coord {
        match self {
            Geometry::Point(c) => *c,
            Geometry::LineString(pts) => pts.first().copied().unwrap_or(Coord::xy(0.0, 0.0)),
            Geometry::Polygon(rings) => {
                let ring = rings.first().map(|r| r.as_slice()).unwrap_or(&[]);
                if ring.is_empty() {
                    return Coord::xy(0.0, 0.0);
                }
                let (sx, sy) = ring
                    .iter()
                    .fold((0.0, 0.0), |(sx, sy), c| (sx + c.x, sy + c.y));
                Coord::xy(sx / ring.len() as f64, sy / ring.len() as f64)
            }
        }
    }

    pub fn to_wkt(&self) -> String {
        fn coord_str(c: &Coord) -> String {
            let mut s = format!("{} {}", c.x, c.y);
            if let Some(z) = c.z {
                s.push_str(&format!(" {z}"));
            }
            if let Some(t) = c.t {
                s.push_str(&format!(" {t}"));
            }
            s
        }
        fn tag(has_z: bool, has_t: bool) -> &'static str {
            match (has_z, has_t) {
                (true, true) => " ZM",
                (true, false) => " Z",
                (false, true) => " M",
                (false, false) => "",
            }
        }
        match self {
            Geometry::Point(c) => format!(
                "POINT{}({})",
                tag(c.z.is_some(), c.t.is_some()),
                coord_str(c)
            ),
            Geometry::LineString(pts) if pts.is_empty() => "LINESTRING EMPTY".to_string(),
            Geometry::LineString(pts) => {
                let has_z = pts.iter().any(|c| c.z.is_some());
                let has_t = pts.iter().any(|c| c.t.is_some());
                let body = pts.iter().map(coord_str).collect::<Vec<_>>().join(", ");
                format!("LINESTRING{}({})", tag(has_z, has_t), body)
            }
            Geometry::Polygon(rings) if rings.is_empty() || rings[0].is_empty() => {
                "POLYGON EMPTY".to_string()
            }
            Geometry::Polygon(rings) => {
                let has_z = rings.iter().flatten().any(|c| c.z.is_some());
                let has_t = rings.iter().flatten().any(|c| c.t.is_some());
                let body = rings
                    .iter()
                    .map(|r| {
                        format!(
                            "({})",
                            r.iter().map(coord_str).collect::<Vec<_>>().join(", ")
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("POLYGON{}({})", tag(has_z, has_t), body)
            }
        }
    }

    pub fn from_wkt(s: &str) -> Result<Self> {
        let s = s.trim();
        let upper = s.to_uppercase();
        // Standard OGC WKT "EMPTY" geometries. `POINT EMPTY` has no
        // representation in this crate's `Geometry::Point(Coord)` model (a
        // point always carries coordinates) — that case is a documented gap,
        // not silently accepted. `LINESTRING EMPTY`/`POLYGON EMPTY` map
        // cleanly onto an empty `Vec`, so those are supported.
        if upper == "LINESTRING EMPTY" {
            return Ok(Geometry::LineString(Vec::new()));
        }
        if upper == "POLYGON EMPTY" {
            return Ok(Geometry::Polygon(Vec::new()));
        }
        if let Some(rest) = strip_tag(&upper, s, "POINT") {
            let coords = parse_coord_list(rest)?;
            let c = coords
                .into_iter()
                .next()
                .ok_or_else(|| anyhow::anyhow!("POINT with no coordinate"))?;
            return Ok(Geometry::Point(c));
        }
        if let Some(rest) = strip_tag(&upper, s, "LINESTRING") {
            let coords = parse_coord_list(rest)?;
            return Ok(Geometry::LineString(coords));
        }
        if let Some(rest) = strip_tag(&upper, s, "POLYGON") {
            let rest = rest.trim();
            let inner = rest
                .strip_prefix('(')
                .and_then(|r| r.strip_suffix(')'))
                .unwrap_or(rest);
            let mut rings = Vec::new();
            for ring_src in split_top_level(inner) {
                let ring_src = ring_src
                    .trim()
                    .trim_start_matches('(')
                    .trim_end_matches(')');
                rings.push(parse_coord_list_body(ring_src)?);
            }
            return Ok(Geometry::Polygon(rings));
        }
        bail!("unsupported or malformed WKT: {s}")
    }
}

/// Strips a WKT type tag (`POINT`, `POINT Z`, `POINT ZM`, ...) and returns
/// the remaining `(...)` coordinate text, using `upper` for case-insensitive
/// tag matching but slicing the original-case `orig` so numeric text is
/// untouched.
fn strip_tag<'a>(upper: &str, orig: &'a str, tag: &str) -> Option<&'a str> {
    if !upper.starts_with(tag) {
        return None;
    }
    let after_tag = &orig[tag.len()..];
    let after_tag_upper = &upper[tag.len()..];
    let paren = after_tag_upper.find('(')?;
    // Reject if there's a longer identifier prefix (e.g. "POINTY" shouldn't
    // match "POINT") by requiring only whitespace/Z/M letters before '('.
    let between = after_tag_upper[..paren].trim();
    if !between.is_empty()
        && !between
            .chars()
            .all(|c| c == 'Z' || c == 'M' || c.is_whitespace())
    {
        return None;
    }
    Some(&after_tag[paren..])
}

fn parse_coord_list(paren_wrapped: &str) -> Result<Vec<Coord>> {
    let inner = paren_wrapped
        .trim()
        .strip_prefix('(')
        .and_then(|r| r.strip_suffix(')'))
        .ok_or_else(|| anyhow::anyhow!("expected parenthesized coordinate list"))?;
    parse_coord_list_body(inner)
}

fn parse_coord_list_body(inner: &str) -> Result<Vec<Coord>> {
    if inner.trim().is_empty() {
        return Ok(Vec::new());
    }
    inner
        .split(',')
        .map(|part| {
            let nums: Vec<f64> = part
                .split_whitespace()
                .map(|n| {
                    n.parse::<f64>()
                        .map_err(|e| anyhow::anyhow!("bad coordinate number {n:?}: {e}"))
                })
                .collect::<Result<_>>()?;
            match nums.len() {
                2 => Ok(Coord {
                    x: nums[0],
                    y: nums[1],
                    z: None,
                    t: None,
                }),
                3 => Ok(Coord {
                    x: nums[0],
                    y: nums[1],
                    z: Some(nums[2]),
                    t: None,
                }),
                4 => Ok(Coord {
                    x: nums[0],
                    y: nums[1],
                    z: Some(nums[2]),
                    t: Some(nums[3] as i64),
                }),
                n => bail!("expected 2-4 ordinates per coordinate, got {n}"),
            }
        })
        .collect()
}

/// Splits `"(1 2),(3 4)"`-style text on top-level commas (i.e. not commas
/// nested inside another paren group), for polygon ring lists.
fn split_top_level(s: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut start = 0usize;
    for (i, ch) in s.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                out.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    out.push(&s[start..]);
    out
}

/// Great-circle distance between two lon/lat points, in meters (haversine).
pub fn haversine_distance_m(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let (lat1r, lat2r) = (lat1.to_radians(), lat2.to_radians());
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let a = (dlat / 2.0).sin().powi(2) + lat1r.cos() * lat2r.cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();
    EARTH_RADIUS_M * c
}

/// Planar (flat-plane, Pythagorean) distance — used when coordinates aren't
/// geographic lon/lat (e.g. a local XY game-world coordinate system).
pub fn planar_distance(x1: f64, y1: f64, x2: f64, y2: f64) -> f64 {
    ((x2 - x1).powi(2) + (y2 - y1).powi(2)).sqrt()
}

/// Ray-casting point-in-polygon test against the exterior ring only (holes
/// not subtracted — a documented simplification).
pub fn point_in_polygon(px: f64, py: f64, ring: &[Coord]) -> bool {
    let mut inside = false;
    let n = ring.len();
    if n < 3 {
        return false;
    }
    let mut j = n - 1;
    for i in 0..n {
        let (xi, yi) = (ring[i].x, ring[i].y);
        let (xj, yj) = (ring[j].x, ring[j].y);
        if ((yi > py) != (yj > py)) && (px < (xj - xi) * (py - yi) / (yj - yi) + xi) {
            inside = !inside;
        }
        j = i;
    }
    inside
}

pub fn bbox_intersects(a: &BBox, b: &BBox) -> bool {
    a.min_x <= b.max_x && a.max_x >= b.min_x && a.min_y <= b.max_y && a.max_y >= b.min_y
}

// --- EWKT: SRID-prefixed WKT (`SRID=4326;POINT(...)`), PostGIS's convention
// since standard WKT/OGC has no SRID slot at all. Kept as a thin text-prefix
// wrapper rather than a `Geometry` struct field so every existing
// `Geometry::Point(c)`-style match arm elsewhere in the codebase is
// untouched; SRID is metadata carried alongside the geometry, not part of
// its shape.

/// Splits a leading `SRID=<n>;` off `s` if present, returning `(srid, rest)`.
/// `srid` is `None` when no prefix is present (an "unspecified" SRID, same
/// as PostGIS's SRID 0).
pub fn strip_srid_prefix(s: &str) -> (Option<i32>, &str) {
    let s = s.trim();
    if let Some(rest) = s.strip_prefix("SRID=").or_else(|| s.strip_prefix("srid=")) {
        if let Some((num, geom)) = rest.split_once(';') {
            if let Ok(srid) = num.trim().parse::<i32>() {
                return (Some(srid), geom);
            }
        }
    }
    (None, s)
}

impl Geometry {
    /// Parses EWKT (`SRID=<n>;<WKT>`) or plain WKT, returning the geometry
    /// and the SRID if one was present.
    pub fn from_ewkt(s: &str) -> Result<(Self, Option<i32>)> {
        let (srid, rest) = strip_srid_prefix(s);
        Ok((Geometry::from_wkt(rest)?, srid))
    }

    /// Renders as EWKT (`SRID=<n>;<WKT>`) when `srid` is `Some`, plain WKT
    /// otherwise.
    pub fn to_ewkt(&self, srid: Option<i32>) -> String {
        match srid {
            Some(s) => format!("SRID={s};{}", self.to_wkt()),
            None => self.to_wkt(),
        }
    }
}

// --- WKB / EWKB: binary geometry encoding (ISO/OGC Well-Known Binary, with
// PostGIS's EWKB extension for the Z/M/SRID flag bits in the type word —
// the de facto standard `ST_AsBinary`/`ST_AsEWKB` produce). Since this engine
// has no dedicated binary `Value` variant (see `Value::Text`-as-WKT
// precedent above), WKB bytes are surfaced as lowercase hex text, matching
// how `psql`/PostGIS render `bytea`/`ST_AsBinary` output.

const WKB_POINT: u32 = 1;
const WKB_LINESTRING: u32 = 2;
const WKB_POLYGON: u32 = 3;
/// EWKB flag bits OR'd into the type word (PostGIS convention, not in the
/// plain ISO WKB spec).
const EWKB_Z: u32 = 0x8000_0000;
const EWKB_M: u32 = 0x4000_0000;
const EWKB_SRID: u32 = 0x2000_0000;

pub fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn from_hex(s: &str) -> Result<Vec<u8>> {
    let s = s.trim();
    let s = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("\\x"))
        .unwrap_or(s);
    if s.len() % 2 != 0 {
        bail!("odd-length hex string");
    }
    (0..s.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&s[i..i + 2], 16)
                .map_err(|e| anyhow::anyhow!("bad hex byte {:?}: {e}", &s[i..i + 2]))
        })
        .collect()
}

impl Geometry {
    /// Encodes as little-endian EWKB hex text. `srid` is embedded in the
    /// type word's SRID flag when present (PostGIS EWKB convention); plain
    /// ISO WKB (no SRID) is produced by passing `None`.
    pub fn to_wkb_hex(&self, srid: Option<i32>) -> String {
        let mut buf = Vec::new();
        write_geom_wkb(self, srid, &mut buf);
        to_hex(&buf)
    }

    /// Decodes little- or big-endian (E)WKB hex text, returning the geometry
    /// and the embedded SRID if the EWKB SRID flag was set.
    pub fn from_wkb_hex(hex: &str) -> Result<(Self, Option<i32>)> {
        let bytes = from_hex(hex)?;
        let mut cur = 0usize;
        read_geom_wkb(&bytes, &mut cur)
    }
}

fn write_coord(c: &Coord, has_z: bool, has_t: bool, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&c.x.to_le_bytes());
    buf.extend_from_slice(&c.y.to_le_bytes());
    if has_z {
        buf.extend_from_slice(&c.z.unwrap_or(0.0).to_le_bytes());
    }
    if has_t {
        buf.extend_from_slice(&(c.t.unwrap_or(0) as f64).to_le_bytes());
    }
}

fn write_geom_wkb(g: &Geometry, srid: Option<i32>, buf: &mut Vec<u8>) {
    let (has_z, has_t) = match g {
        Geometry::Point(c) => (c.z.is_some(), c.t.is_some()),
        Geometry::LineString(pts) => (
            pts.iter().any(|c| c.z.is_some()),
            pts.iter().any(|c| c.t.is_some()),
        ),
        Geometry::Polygon(rings) => (
            rings.iter().flatten().any(|c| c.z.is_some()),
            rings.iter().flatten().any(|c| c.t.is_some()),
        ),
    };
    let mut type_word = match g {
        Geometry::Point(_) => WKB_POINT,
        Geometry::LineString(_) => WKB_LINESTRING,
        Geometry::Polygon(_) => WKB_POLYGON,
    };
    if has_z {
        type_word |= EWKB_Z;
    }
    if has_t {
        type_word |= EWKB_M;
    }
    if srid.is_some() {
        type_word |= EWKB_SRID;
    }
    buf.push(1); // byte order: 1 = little-endian (NDR)
    buf.extend_from_slice(&type_word.to_le_bytes());
    if let Some(s) = srid {
        buf.extend_from_slice(&(s as u32).to_le_bytes());
    }
    match g {
        Geometry::Point(c) => write_coord(c, has_z, has_t, buf),
        Geometry::LineString(pts) => {
            buf.extend_from_slice(&(pts.len() as u32).to_le_bytes());
            for c in pts {
                write_coord(c, has_z, has_t, buf);
            }
        }
        Geometry::Polygon(rings) => {
            buf.extend_from_slice(&(rings.len() as u32).to_le_bytes());
            for ring in rings {
                buf.extend_from_slice(&(ring.len() as u32).to_le_bytes());
                for c in ring {
                    write_coord(c, has_z, has_t, buf);
                }
            }
        }
    }
}

fn read_u32(bytes: &[u8], cur: &mut usize, little_endian: bool) -> Result<u32> {
    let b = bytes
        .get(*cur..*cur + 4)
        .ok_or_else(|| anyhow::anyhow!("truncated WKB (expected u32)"))?;
    *cur += 4;
    let arr: [u8; 4] = b.try_into().unwrap();
    Ok(if little_endian {
        u32::from_le_bytes(arr)
    } else {
        u32::from_be_bytes(arr)
    })
}

fn read_f64(bytes: &[u8], cur: &mut usize, little_endian: bool) -> Result<f64> {
    let b = bytes
        .get(*cur..*cur + 8)
        .ok_or_else(|| anyhow::anyhow!("truncated WKB (expected f64)"))?;
    *cur += 8;
    let arr: [u8; 8] = b.try_into().unwrap();
    Ok(if little_endian {
        f64::from_le_bytes(arr)
    } else {
        f64::from_be_bytes(arr)
    })
}

fn read_coord(bytes: &[u8], cur: &mut usize, le: bool, has_z: bool, has_t: bool) -> Result<Coord> {
    let x = read_f64(bytes, cur, le)?;
    let y = read_f64(bytes, cur, le)?;
    let z = if has_z {
        Some(read_f64(bytes, cur, le)?)
    } else {
        None
    };
    let t = if has_t {
        Some(read_f64(bytes, cur, le)? as i64)
    } else {
        None
    };
    Ok(Coord { x, y, z, t })
}

fn read_geom_wkb(bytes: &[u8], cur: &mut usize) -> Result<(Geometry, Option<i32>)> {
    let byte_order = *bytes
        .get(*cur)
        .ok_or_else(|| anyhow::anyhow!("empty WKB"))?;
    *cur += 1;
    let le = match byte_order {
        1 => true,
        0 => false,
        b => bail!("unknown WKB byte order marker {b}"),
    };
    let type_word = read_u32(bytes, cur, le)?;
    let has_z = type_word & EWKB_Z != 0;
    let has_t = type_word & EWKB_M != 0;
    let has_srid = type_word & EWKB_SRID != 0;
    let srid = if has_srid {
        Some(read_u32(bytes, cur, le)? as i32)
    } else {
        None
    };
    let base_type = type_word & 0x0000_00ff;
    let geom = match base_type {
        WKB_POINT => Geometry::Point(read_coord(bytes, cur, le, has_z, has_t)?),
        WKB_LINESTRING => {
            let n = read_u32(bytes, cur, le)? as usize;
            let pts = (0..n)
                .map(|_| read_coord(bytes, cur, le, has_z, has_t))
                .collect::<Result<Vec<_>>>()?;
            Geometry::LineString(pts)
        }
        WKB_POLYGON => {
            let n_rings = read_u32(bytes, cur, le)? as usize;
            let rings = (0..n_rings)
                .map(|_| {
                    let n = read_u32(bytes, cur, le)? as usize;
                    (0..n)
                        .map(|_| read_coord(bytes, cur, le, has_z, has_t))
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?;
            Geometry::Polygon(rings)
        }
        t => bail!("unsupported WKB geometry type {t}"),
    };
    Ok((geom, srid))
}

// --- ST_Transform: CRS reprojection. Full arbitrary-CRS reprojection (a
// real PROJ-equivalent) is out of scope (see module doc comment); the one
// pair actually implemented — EPSG:4326 (WGS84 lon/lat) <-> EPSG:3857 (Web
// Mercator) — covers the overwhelmingly common "store in 4326, render on a
// web map in 3857" case without pulling in a general CRS library.

const WEB_MERCATOR_MAX_LAT: f64 = 85.051_128_78; // Web Mercator's lat clamp

// EPSG:3857 is defined against the WGS84 *equatorial* (semi-major axis)
// radius, not the mean earth radius used elsewhere in this module for
// geodesic distance — using the mean radius here would shrink every
// projected coordinate by ~0.11% relative to real Web Mercator output.
const WEB_MERCATOR_RADIUS_M: f64 = 6_378_137.0;

fn transform_coord(c: Coord, from_srid: i32, to_srid: i32) -> Result<Coord> {
    if from_srid == to_srid {
        return Ok(c);
    }
    let (x, y) = match (from_srid, to_srid) {
        (4326, 3857) => {
            let lat = c.y.clamp(-WEB_MERCATOR_MAX_LAT, WEB_MERCATOR_MAX_LAT);
            let x = c.x.to_radians() * WEB_MERCATOR_RADIUS_M;
            let y = (lat.to_radians() / 2.0 + std::f64::consts::FRAC_PI_4)
                .tan()
                .ln()
                * WEB_MERCATOR_RADIUS_M;
            (x, y)
        }
        (3857, 4326) => {
            let lon = (c.x / WEB_MERCATOR_RADIUS_M).to_degrees();
            let lat = (2.0 * (c.y / WEB_MERCATOR_RADIUS_M).exp().atan()
                - std::f64::consts::FRAC_PI_2)
                .to_degrees();
            (lon, lat)
        }
        _ => bail!(
            "ST_Transform: unsupported SRID pair {from_srid} -> {to_srid} (only 4326<->3857 are implemented; no general CRS reprojection)"
        ),
    };
    Ok(Coord { x, y, ..c })
}

/// Reprojects every coordinate of `g` from `from_srid` to `to_srid`.
pub fn transform_geometry(g: &Geometry, from_srid: i32, to_srid: i32) -> Result<Geometry> {
    Ok(match g {
        Geometry::Point(c) => Geometry::Point(transform_coord(*c, from_srid, to_srid)?),
        Geometry::LineString(pts) => Geometry::LineString(
            pts.iter()
                .map(|c| transform_coord(*c, from_srid, to_srid))
                .collect::<Result<_>>()?,
        ),
        Geometry::Polygon(rings) => Geometry::Polygon(
            rings
                .iter()
                .map(|r| {
                    r.iter()
                        .map(|c| transform_coord(*c, from_srid, to_srid))
                        .collect::<Result<_>>()
                })
                .collect::<Result<_>>()?,
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn point_roundtrip() {
        let g = Geometry::from_wkt("POINT(1.5 2.5)").unwrap();
        assert_eq!(g, Geometry::Point(Coord::xy(1.5, 2.5)));
        assert_eq!(g.to_wkt(), "POINT(1.5 2.5)");
    }

    #[test]
    fn point_zm_roundtrip() {
        let g = Geometry::from_wkt("POINT ZM (1 2 3 1000)").unwrap();
        match &g {
            Geometry::Point(c) => {
                assert_eq!(c.z, Some(3.0));
                assert_eq!(c.t, Some(1000));
            }
            _ => panic!("expected point"),
        }
    }

    #[test]
    fn polygon_and_pip() {
        let g = Geometry::from_wkt("POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))").unwrap();
        let Geometry::Polygon(rings) = &g else {
            panic!()
        };
        assert!(point_in_polygon(5.0, 5.0, &rings[0]));
        assert!(!point_in_polygon(50.0, 50.0, &rings[0]));
    }

    #[test]
    fn haversine_known_distance() {
        // London to Paris is roughly 344 km.
        let d = haversine_distance_m(-0.1276, 51.5074, 2.3522, 48.8566);
        assert!((300_000.0..390_000.0).contains(&d), "got {d}");
    }

    #[test]
    fn ewkt_roundtrip() {
        let (g, srid) = Geometry::from_ewkt("SRID=4326;POINT(1.5 2.5)").unwrap();
        assert_eq!(g, Geometry::Point(Coord::xy(1.5, 2.5)));
        assert_eq!(srid, Some(4326));
        assert_eq!(g.to_ewkt(Some(4326)), "SRID=4326;POINT(1.5 2.5)");
        assert_eq!(g.to_ewkt(None), "POINT(1.5 2.5)");
    }

    #[test]
    fn ewkt_no_srid_is_none() {
        let (g, srid) = Geometry::from_ewkt("POINT(1 2)").unwrap();
        assert_eq!(g, Geometry::Point(Coord::xy(1.0, 2.0)));
        assert_eq!(srid, None);
    }

    #[test]
    fn wkb_point_roundtrip() {
        let g = Geometry::Point(Coord::xy(1.5, -2.5));
        let hex = g.to_wkb_hex(None);
        let (g2, srid) = Geometry::from_wkb_hex(&hex).unwrap();
        assert_eq!(g, g2);
        assert_eq!(srid, None);
    }

    #[test]
    fn wkb_with_srid_and_z_roundtrip() {
        let g = Geometry::Point(Coord {
            x: 1.0,
            y: 2.0,
            z: Some(3.0),
            t: None,
        });
        let hex = g.to_wkb_hex(Some(4326));
        let (g2, srid) = Geometry::from_wkb_hex(&hex).unwrap();
        assert_eq!(g, g2);
        assert_eq!(srid, Some(4326));
    }

    #[test]
    fn wkb_polygon_roundtrip() {
        let g = Geometry::from_wkt("POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))").unwrap();
        let hex = g.to_wkb_hex(None);
        let (g2, srid) = Geometry::from_wkb_hex(&hex).unwrap();
        assert_eq!(g, g2);
        assert_eq!(srid, None);
    }

    #[test]
    fn transform_identity_when_same_srid() {
        let g = Geometry::Point(Coord::xy(10.0, 20.0));
        let g2 = transform_geometry(&g, 4326, 4326).unwrap();
        assert_eq!(g, g2);
    }

    #[test]
    fn transform_4326_to_3857_and_back() {
        let g = Geometry::Point(Coord::xy(-0.1276, 51.5074)); // London
        let merc = transform_geometry(&g, 4326, 3857).unwrap();
        let Geometry::Point(m) = &merc else { panic!() };
        // Known approximate Web Mercator coords for London.
        assert!((-14210.0..-14190.0).contains(&m.x), "got {}", m.x);
        assert!((6710000.0..6720000.0).contains(&m.y), "got {}", m.y);

        let back = transform_geometry(&merc, 3857, 4326).unwrap();
        let Geometry::Point(b) = &back else { panic!() };
        assert!((b.x - g.bbox().min_x).abs() < 1e-6);
    }

    #[test]
    fn transform_unsupported_pair_errors() {
        let g = Geometry::Point(Coord::xy(1.0, 2.0));
        assert!(transform_geometry(&g, 4326, 2154).is_err());
    }
}
