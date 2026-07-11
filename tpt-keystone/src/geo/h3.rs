//! H3-*inspired* hexagonal grid: axial-coordinate hex binning of lon/lat
//! (longitude pre-scaled by `cos(lat)` to soften — not eliminate — polar
//! compression), with a `2x` finer hexagon at each successive resolution
//! level.
//!
//! Honest caveat: this is a from-scratch simplified approximation of the
//! *idea* of Uber's H3 (hexagons — no equal-neighbor-distance quirk that a
//! square grid has, hierarchical resolutions), not a port of it. Real H3
//! projects onto an icosahedron for near-uniform cell area globally and
//! uses an aperture-7 (not 2x) parent/child subdivision with a specific bit
//! layout; this implementation uses a flat equirectangular-ish projection
//! (accurate locally, increasingly distorted near the poles) and aperture-2
//! subdivision, and is not bit-compatible with real H3 indexes.

/// A packed cell id: `res(5 bits) | q(27 bits, bias-offset) | r(27 bits, bias-offset)`.
pub type CellId = u64;

const MAX_RES: u8 = 20;
const BIAS: i64 = 1 << 26;

/// Base hex size (in "flattened degrees" — see `project`) at resolution 0.
/// Chosen so resolution 0 hexagons are continent-scale and halving 20 times
/// still leaves sub-meter resolution at the equator.
const BASE_SIZE: f64 = 10.0;

fn pack(res: u8, q: i64, r: i64) -> CellId {
    let qb = (q + BIAS) as u64 & 0x07FF_FFFF;
    let rb = (r + BIAS) as u64 & 0x07FF_FFFF;
    ((res as u64) << 54) | (qb << 27) | rb
}

pub fn unpack(id: CellId) -> (u8, i64, i64) {
    let res = (id >> 54) as u8 & 0x1F;
    let qb = (id >> 27) & 0x07FF_FFFF;
    let rb = id & 0x07FF_FFFF;
    (res, qb as i64 - BIAS, rb as i64 - BIAS)
}

/// Projects lon/lat degrees onto a local flat plane: longitude is scaled by
/// `cos(lat)` so hex sizing is roughly comparable in meters across
/// latitudes (breaks down near the poles — documented limitation shared
/// with any non-icosahedral projection).
fn project(lon: f64, lat: f64) -> (f64, f64) {
    (lon * lat.to_radians().cos(), lat)
}

fn hex_size(res: u8) -> f64 {
    BASE_SIZE / (1u64 << res.min(MAX_RES)) as f64
}

/// Axial pointy-top pixel→hex conversion + cube rounding (standard
/// hex-grid algorithm).
fn pixel_to_axial(x: f64, y: f64, size: f64) -> (i64, i64) {
    let q = (3f64.sqrt() / 3.0 * x - 1.0 / 3.0 * y) / size;
    let r = (2.0 / 3.0 * y) / size;
    cube_round(q, -q - r, r)
}

fn cube_round(x: f64, y: f64, z: f64) -> (i64, i64) {
    let (mut rx, ry, mut rz) = (x.round(), y.round(), z.round());
    let (dx, dy, dz) = ((rx - x).abs(), (ry - y).abs(), (rz - z).abs());
    if dx > dy && dx > dz {
        rx = -ry - rz;
    } else if dy > dz {
        // y (unreturned — only q=rx, r=rz go out) has the largest rounding
        // error here, so rx/rz are already the more trustworthy roundings
        // and need no correction.
    } else {
        rz = -rx - ry;
    }
    (rx as i64, rz as i64)
}

fn axial_to_pixel(q: i64, r: i64, size: f64) -> (f64, f64) {
    let x = size * (3f64.sqrt() * q as f64 + 3f64.sqrt() / 2.0 * r as f64);
    let y = size * (1.5 * r as f64);
    (x, y)
}

/// Cell id for `(lon, lat)` at resolution `res` (0 = coarsest).
pub fn cell_id_for_point(lon: f64, lat: f64, res: u8) -> CellId {
    let res = res.min(MAX_RES);
    let (x, y) = project(lon, lat);
    let (q, r) = pixel_to_axial(x, y, hex_size(res));
    pack(res, q, r)
}

/// Approximate center of a cell, in (lon, lat)-ish flattened-plane units
/// (undoes `project`'s longitude scaling using the cell's own latitude).
pub fn cell_center(id: CellId) -> (f64, f64) {
    let (res, q, r) = unpack(id);
    let (x, y) = axial_to_pixel(q, r, hex_size(res));
    let lat = y;
    let lon = if lat.to_radians().cos().abs() > 1e-9 {
        x / lat.to_radians().cos()
    } else {
        x
    };
    (lon, lat)
}

const AXIAL_DIRS: [(i64, i64); 6] = [(1, 0), (1, -1), (0, -1), (-1, 0), (-1, 1), (0, 1)];

/// The 6 edge-adjacent neighbors of `id` at the same resolution.
pub fn neighbors(id: CellId) -> Vec<CellId> {
    let (res, q, r) = unpack(id);
    AXIAL_DIRS
        .iter()
        .map(|(dq, dr)| pack(res, q + dq, r + dr))
        .collect()
}

/// All cells within `k` hex-steps of `id` (inclusive of `id` itself), i.e.
/// a "k-ring" / disk — used to cover a radius query.
pub fn k_ring(id: CellId, k: u32) -> Vec<CellId> {
    let (res, cq, cr) = unpack(id);
    let mut out = vec![pack(res, cq, cr)];
    for radius in 1..=k as i64 {
        let (mut q, mut r) = (cq + AXIAL_DIRS[4].0 * radius, cr + AXIAL_DIRS[4].1 * radius);
        for (dq, dr) in AXIAL_DIRS.iter() {
            for _ in 0..radius {
                out.push(pack(res, q, r));
                q += dq;
                r += dr;
            }
        }
    }
    out
}

/// Picks the finest resolution whose hex circumradius is still `>= radius_m`
/// (in the same flattened-degree units `project` uses — callers pass a
/// caller-converted radius), so `k_ring(.., 1)` or `2` comfortably covers
/// the query radius without enumerating an enormous cell set.
pub fn res_for_radius(radius_units: f64) -> u8 {
    for res in (0..=MAX_RES).rev() {
        if hex_size(res) >= radius_units.max(1e-9) {
            return res;
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_point_same_cell() {
        let a = cell_id_for_point(-122.4194, 37.7749, 10);
        let b = cell_id_for_point(-122.4194, 37.7749, 10);
        assert_eq!(a, b);
    }

    #[test]
    fn neighbors_are_distinct_from_self() {
        let center = cell_id_for_point(0.0, 0.0, 10);
        let ns = neighbors(center);
        assert_eq!(ns.len(), 6);
        assert!(!ns.contains(&center));
    }

    #[test]
    fn k_ring_grows_with_k() {
        let center = cell_id_for_point(0.0, 0.0, 10);
        let ring1 = k_ring(center, 1);
        let ring2 = k_ring(center, 2);
        assert_eq!(ring1.len(), 7); // 1 + 6
        assert!(ring2.len() > ring1.len());
        assert!(ring1.iter().all(|c| ring2.contains(c)));
    }
}
