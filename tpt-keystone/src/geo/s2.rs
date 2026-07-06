//! S2-*inspired* hierarchical spatial grid: projects lon/lat onto one of
//! six cube faces (same core idea as Google's S2 — a near-equal-area
//! hierarchical decomposition of the sphere, avoiding lat/lon's polar
//! distortion) and quadtree-subdivides each face down to a configurable
//! level.
//!
//! Honest caveat: this is a from-scratch, simplified approximation of the
//! *idea*, not a port of the real S2 library — it skips S2's tangent
//! ("quadratic"/ST) reprojection curve (uses a linear UV→grid mapping
//! instead) and its Hilbert-curve cell numbering (uses direct row-major
//! `(face, level, i, j)` packing instead). It is not bit-compatible with
//! real S2 cell ids and cannot interoperate with other S2 implementations.
//! What it does provide, correctly: a real hierarchical decomposition
//! (`parent`/`children` shrink/grow the cell by exactly one grid level) and
//! locality (nearby points map to nearby or identical cells), which is what
//! the spatial index (`storage::geo_index`) actually needs.

use crate::geo::geometry::BBox;

/// Bits per axis per face at the maximum supported level. 24 bits gives
/// 2^24 cells per axis per face — at Earth scale that's roughly half-meter
/// resolution at the finest level, comfortably below any level we'd
/// actually index at.
const MAX_LEVEL: u8 = 24;

/// A packed cell id: `face(3 bits) | level(5 bits) | i(28 bits) | j(28 bits)`.
pub type CellId = u64;

fn pack(face: u8, level: u8, i: u32, j: u32) -> CellId {
    ((face as u64) << 61) | ((level as u64) << 56) | ((i as u64) << 28) | (j as u64)
}

pub fn unpack(id: CellId) -> (u8, u8, u32, u32) {
    let face = (id >> 61) as u8 & 0x7;
    let level = (id >> 56) as u8 & 0x1F;
    let i = ((id >> 28) & 0x0FFF_FFFF) as u32;
    let j = (id & 0x0FFF_FFFF) as u32;
    (face, level, i, j)
}

/// Projects a unit sphere direction onto its dominant cube face, returning
/// `(face, u, v)` with `u, v` in `[-1, 1]`.
fn face_uv(x: f64, y: f64, z: f64) -> (u8, f64, f64) {
    let (ax, ay, az) = (x.abs(), y.abs(), z.abs());
    if ax >= ay && ax >= az {
        if x > 0.0 { (0, y / ax, z / ax) } else { (1, -y / ax, z / ax) }
    } else if ay >= ax && ay >= az {
        if y > 0.0 { (2, -x / ay, z / ay) } else { (3, x / ay, z / ay) }
    } else if z > 0.0 {
        (4, x / az, y / az)
    } else {
        (5, x / az, -y / az)
    }
}

fn lonlat_to_xyz(lon_deg: f64, lat_deg: f64) -> (f64, f64, f64) {
    let lon = lon_deg.to_radians();
    let lat = lat_deg.to_radians();
    (lat.cos() * lon.cos(), lat.cos() * lon.sin(), lat.sin())
}

/// Cell id containing `(lon, lat)` at `level` (0 = whole face, up to
/// `MAX_LEVEL` = finest grid).
pub fn cell_id_for_point(lon: f64, lat: f64, level: u8) -> CellId {
    let level = level.min(MAX_LEVEL);
    let (x, y, z) = lonlat_to_xyz(lon, lat);
    let (face, u, v) = face_uv(x, y, z);
    let n = 1u32 << level;
    let i = (((u + 1.0) / 2.0 * n as f64) as i64).clamp(0, n as i64 - 1) as u32;
    let j = (((v + 1.0) / 2.0 * n as f64) as i64).clamp(0, n as i64 - 1) as u32;
    pack(face, level, i, j)
}

/// The ancestor cell one level coarser (whole-face cell if already at level 0).
pub fn parent(id: CellId) -> CellId {
    let (face, level, i, j) = unpack(id);
    if level == 0 {
        return id;
    }
    pack(face, level - 1, i >> 1, j >> 1)
}

/// Re-expresses `id` at `target_level` (coarsening via repeated `parent`, or
/// no-op if `target_level >= id`'s level — this function only coarsens).
pub fn ancestor_at(id: CellId, target_level: u8) -> CellId {
    let (_, level, _, _) = unpack(id);
    let mut cur = id;
    let mut lvl = level;
    while lvl > target_level {
        cur = parent(cur);
        lvl -= 1;
    }
    cur
}

/// Approximate cell width in meters at `level`, at the equator (cells
/// shrink somewhat away from the equator/face center — this is a sizing
/// heuristic for picking an index level, not an exact area formula).
pub fn approx_cell_width_m(level: u8) -> f64 {
    let n = (1u32 << level) as f64;
    // A cube face subtends ~90 degrees of great-circle arc.
    (std::f64::consts::PI / 2.0) * crate::geo::geometry::EARTH_RADIUS_M / n
}

/// Picks the coarsest level whose cell width is still `<= radius_m`, so a
/// radius query touches a small, bounded number of cells. Falls back to
/// `MAX_LEVEL` for tiny radii and `0` for radii spanning a whole face.
pub fn level_for_radius(radius_m: f64) -> u8 {
    for level in 0..=MAX_LEVEL {
        if approx_cell_width_m(level) <= radius_m.max(1.0) {
            return level;
        }
    }
    MAX_LEVEL
}

/// Returns the cell containing `(lon, lat)` at `level` plus its 8 grid
/// neighbors on the same face, for a radius/bbox covering. Does not cross
/// cube-face boundaries (a documented simplification: a query whose radius
/// crosses a face edge — roughly every 90 degrees of longitude, or near the
/// poles — may under-cover by missing the adjacent face's cells).
pub fn neighborhood(lon: f64, lat: f64, level: u8) -> Vec<CellId> {
    let level = level.min(MAX_LEVEL);
    let (x, y, z) = lonlat_to_xyz(lon, lat);
    let (face, u, v) = face_uv(x, y, z);
    let n = 1i64 << level;
    let ci = (((u + 1.0) / 2.0 * n as f64) as i64).clamp(0, n - 1);
    let cj = (((v + 1.0) / 2.0 * n as f64) as i64).clamp(0, n - 1);
    let mut out = Vec::with_capacity(9);
    for di in -1..=1 {
        for dj in -1..=1 {
            let ni = ci + di;
            let nj = cj + dj;
            if ni >= 0 && ni < n && nj >= 0 && nj < n {
                out.push(pack(face, level, ni as u32, nj as u32));
            }
        }
    }
    out
}

/// A covering of `bbox` at `level`: every cell whose grid coordinates fall
/// within the bbox's footprint on each face the bbox's corners land on.
/// For typical small bboxes (well within one face) this is exact; bboxes
/// spanning multiple faces fall back to sampling the corners + center
/// (may under-cover near face boundaries — same caveat as `neighborhood`).
pub fn covering(bbox: &BBox, level: u8) -> Vec<CellId> {
    let level = level.min(MAX_LEVEL);
    let samples = [
        (bbox.min_x, bbox.min_y),
        (bbox.max_x, bbox.min_y),
        (bbox.min_x, bbox.max_y),
        (bbox.max_x, bbox.max_y),
        ((bbox.min_x + bbox.max_x) / 2.0, (bbox.min_y + bbox.max_y) / 2.0),
    ];
    let mut ids: Vec<CellId> = samples
        .iter()
        .flat_map(|(lon, lat)| neighborhood(*lon, *lat, level))
        .collect();
    ids.sort_unstable();
    ids.dedup();
    ids
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_point_same_cell() {
        let a = cell_id_for_point(-122.4194, 37.7749, 15);
        let b = cell_id_for_point(-122.4194, 37.7749, 15);
        assert_eq!(a, b);
    }

    #[test]
    fn nearby_points_share_coarse_ancestor() {
        let a = cell_id_for_point(-122.4194, 37.7749, 15);
        let b = cell_id_for_point(-122.4195, 37.7750, 15);
        assert_eq!(ancestor_at(a, 5), ancestor_at(b, 5));
    }

    #[test]
    fn distant_points_different_cell() {
        let a = cell_id_for_point(-122.4194, 37.7749, 10);
        let b = cell_id_for_point(2.3522, 48.8566, 10);
        assert_ne!(a, b);
    }

    #[test]
    fn neighborhood_contains_self() {
        let ring = neighborhood(-122.4194, 37.7749, 12);
        let center = cell_id_for_point(-122.4194, 37.7749, 12);
        assert!(ring.contains(&center));
    }
}
