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
        Self { x, y, z: None, t: None }
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
        let mut b = BBox { min_x: f64::INFINITY, min_y: f64::INFINITY, max_x: f64::NEG_INFINITY, max_y: f64::NEG_INFINITY };
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
                let (sx, sy) = ring.iter().fold((0.0, 0.0), |(sx, sy), c| (sx + c.x, sy + c.y));
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
            Geometry::Point(c) => format!("POINT{}({})", tag(c.z.is_some(), c.t.is_some()), coord_str(c)),
            Geometry::LineString(pts) => {
                let has_z = pts.iter().any(|c| c.z.is_some());
                let has_t = pts.iter().any(|c| c.t.is_some());
                let body = pts.iter().map(coord_str).collect::<Vec<_>>().join(", ");
                format!("LINESTRING{}({})", tag(has_z, has_t), body)
            }
            Geometry::Polygon(rings) => {
                let has_z = rings.iter().flatten().any(|c| c.z.is_some());
                let has_t = rings.iter().flatten().any(|c| c.t.is_some());
                let body = rings
                    .iter()
                    .map(|r| format!("({})", r.iter().map(coord_str).collect::<Vec<_>>().join(", ")))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("POLYGON{}({})", tag(has_z, has_t), body)
            }
        }
    }

    pub fn from_wkt(s: &str) -> Result<Self> {
        let s = s.trim();
        let upper = s.to_uppercase();
        if let Some(rest) = strip_tag(&upper, s, "POINT") {
            let coords = parse_coord_list(rest)?;
            let c = coords.into_iter().next().ok_or_else(|| anyhow::anyhow!("POINT with no coordinate"))?;
            return Ok(Geometry::Point(c));
        }
        if let Some(rest) = strip_tag(&upper, s, "LINESTRING") {
            let coords = parse_coord_list(rest)?;
            return Ok(Geometry::LineString(coords));
        }
        if let Some(rest) = strip_tag(&upper, s, "POLYGON") {
            let rest = rest.trim();
            let inner = rest.strip_prefix('(').and_then(|r| r.strip_suffix(')')).unwrap_or(rest);
            let mut rings = Vec::new();
            for ring_src in split_top_level(inner) {
                let ring_src = ring_src.trim().trim_start_matches('(').trim_end_matches(')');
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
    if !between.is_empty() && !between.chars().all(|c| c == 'Z' || c == 'M' || c.is_whitespace()) {
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
    inner
        .split(',')
        .map(|part| {
            let nums: Vec<f64> = part
                .split_whitespace()
                .map(|n| n.parse::<f64>().map_err(|e| anyhow::anyhow!("bad coordinate number {n:?}: {e}")))
                .collect::<Result<_>>()?;
            match nums.len() {
                2 => Ok(Coord { x: nums[0], y: nums[1], z: None, t: None }),
                3 => Ok(Coord { x: nums[0], y: nums[1], z: Some(nums[2]), t: None }),
                4 => Ok(Coord { x: nums[0], y: nums[1], z: Some(nums[2]), t: Some(nums[3] as i64) }),
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
        let Geometry::Polygon(rings) = &g else { panic!() };
        assert!(point_in_polygon(5.0, 5.0, &rings[0]));
        assert!(!point_in_polygon(50.0, 50.0, &rings[0]));
    }

    #[test]
    fn haversine_known_distance() {
        // London to Paris is roughly 344 km.
        let d = haversine_distance_m(-0.1276, 51.5074, 2.3522, 48.8566);
        assert!((300_000.0..390_000.0).contains(&d), "got {d}");
    }
}
