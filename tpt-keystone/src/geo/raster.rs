//! Meridian raster storage — the Phase 6 "raster + vector unified storage
//! model" TODO item.
//!
//! Scope actually implemented: a single-band `f64` grid, georeferenced by an
//! upper-left origin + per-axis pixel scale + SRID (the same handful of
//! fields PostGIS's raster type calls `ST_UpperLeftX`/`Y`, `ST_ScaleX`/`Y`,
//! `ST_SRID`), plus `ST_Value`/`ST_SetValue` pixel access and `ST_AsRaster`
//! to rasterize a `Geometry` (point or polygon) into a new raster using the
//! existing `point_in_polygon` test. "Unified" means: a raster is stored the
//! same way `Geometry` is — hex-encoded bytes surfaced as `Value::Text`, no
//! new row-encoding path — and `ST_AsRaster`/rasterization share the same
//! geometry model and functions vector queries already use, not a separate
//! raster subsystem bolted on alongside.
//!
//! Honest scope cuts, matching this module's existing "not bit-compatible,
//! not the full suite" discipline: single band only (no multi-band/RGB, no
//! pixel type variety — every cell is `f64`), no image-format import
//! (`ST_FromGDALRaster`/loading real GeoTIFF/PNG tiles), no raster-vs-raster
//! algebra (`ST_MapAlgebra`), and the on-disk encoding here is a hand-rolled
//! header+row-major-f64-array format — not PostGIS's actual WKB-raster wire
//! format, the same "approximates the idea, not bit-compatible" caveat as
//! `geo::s2`/`geo::h3`.

use super::geometry::{from_hex, point_in_polygon, to_hex, BBox, Coord, Geometry};
use anyhow::{bail, Result};

/// A single-band raster: a `width x height` grid of `f64` cells, georeferenced
/// by an upper-left origin and a per-axis pixel scale (PostGIS raster
/// convention: `scale_y` is typically negative, since raster row 0 is the
/// northernmost row while `y` increases northward).
#[derive(Debug, Clone, PartialEq)]
pub struct Raster {
    pub width: u32,
    pub height: u32,
    pub upper_left_x: f64,
    pub upper_left_y: f64,
    pub scale_x: f64,
    pub scale_y: f64,
    pub srid: i32,
    /// Row-major cell values, length `width * height`.
    pub band: Vec<f64>,
}

const MAGIC: u32 = 0x5450_5252; // "TPRR" ("TPT Raster")
const VERSION: u8 = 1;

impl Raster {
    /// A new raster covering `width x height` cells, every cell initialized
    /// to `0.0`.
    pub fn new_empty(
        width: u32,
        height: u32,
        upper_left_x: f64,
        upper_left_y: f64,
        scale_x: f64,
        scale_y: f64,
        srid: i32,
    ) -> Self {
        Self {
            width,
            height,
            upper_left_x,
            upper_left_y,
            scale_x,
            scale_y,
            srid,
            band: vec![0.0; (width as usize) * (height as usize)],
        }
    }

    fn index(&self, x: u32, y: u32) -> Option<usize> {
        if x >= self.width || y >= self.height {
            return None;
        }
        Some(y as usize * self.width as usize + x as usize)
    }

    /// Value of cell `(x, y)` (0-indexed, column then row). `None` if the
    /// coordinates fall outside the raster's extent.
    pub fn value(&self, x: u32, y: u32) -> Option<f64> {
        self.index(x, y).map(|i| self.band[i])
    }

    /// Set cell `(x, y)` to `v`. Errors if the coordinates fall outside the
    /// raster's extent (matches PostGIS `ST_SetValue`'s out-of-range error,
    /// rather than silently no-op'ing or panicking).
    pub fn set_value(&mut self, x: u32, y: u32, v: f64) -> Result<()> {
        match self.index(x, y) {
            Some(i) => {
                self.band[i] = v;
                Ok(())
            }
            None => bail!(
                "ST_SetValue: cell ({x}, {y}) is outside the raster's {width}x{height} extent",
                width = self.width,
                height = self.height
            ),
        }
    }

    /// The georeferenced coordinate of cell `(x, y)`'s upper-left corner.
    fn cell_origin(&self, x: u32, y: u32) -> (f64, f64) {
        (
            self.upper_left_x + x as f64 * self.scale_x,
            self.upper_left_y + y as f64 * self.scale_y,
        )
    }

    /// The georeferenced coordinate of cell `(x, y)`'s center — used as the
    /// sample point for rasterization (a cell "contains" a polygon if its
    /// center does).
    fn cell_center(&self, x: u32, y: u32) -> (f64, f64) {
        let (ox, oy) = self.cell_origin(x, y);
        (ox + self.scale_x / 2.0, oy + self.scale_y / 2.0)
    }

    /// Rasterizes `geom` into a new raster covering its bounding box at the
    /// given pixel scale, writing `value` into every cell whose center falls
    /// inside the geometry (a `Point`/`LineString` vertex counts as "inside"
    /// if a cell center coincides with it within half a pixel; a `Polygon`
    /// uses the existing exterior-ring `point_in_polygon` test — holes are
    /// not subtracted, the same simplification `point_in_polygon` itself
    /// already documents). `scale_y` is stored negative internally (raster
    /// convention) even though the caller passes a positive magnitude, same
    /// as PostGIS's `ST_AsRaster`.
    pub fn rasterize(
        geom: &Geometry,
        scale_x: f64,
        scale_y: f64,
        value: f64,
        srid: i32,
    ) -> Result<Self> {
        if scale_x <= 0.0 || scale_y <= 0.0 {
            bail!("ST_AsRaster: scale_x/scale_y must be positive pixel sizes");
        }
        let bbox: BBox = geom.bbox();
        let width = (((bbox.max_x - bbox.min_x) / scale_x).ceil() as u32).max(1);
        let height = (((bbox.max_y - bbox.min_y) / scale_y).ceil() as u32).max(1);
        let mut r = Raster::new_empty(
            width, height, bbox.min_x, bbox.max_y, scale_x, -scale_y, srid,
        );

        match geom {
            Geometry::Point(c) => {
                if let Some((x, y)) = r.nearest_cell(c) {
                    r.set_value(x, y, value)?;
                }
            }
            Geometry::LineString(pts) => {
                for c in pts {
                    if let Some((x, y)) = r.nearest_cell(c) {
                        r.set_value(x, y, value)?;
                    }
                }
            }
            Geometry::Polygon(rings) => {
                let exterior = rings.first().map(|r| r.as_slice()).unwrap_or(&[]);
                for y in 0..height {
                    for x in 0..width {
                        let (cx, cy) = r.cell_center(x, y);
                        if point_in_polygon(cx, cy, exterior) {
                            r.set_value(x, y, value)?;
                        }
                    }
                }
            }
        }
        Ok(r)
    }

    /// The cell whose center is closest to `c` — used to place a `Point`/
    /// `LineString` vertex onto the grid in `rasterize`.
    fn nearest_cell(&self, c: &Coord) -> Option<(u32, u32)> {
        if self.width == 0 || self.height == 0 {
            return None;
        }
        let fx = ((c.x - self.upper_left_x) / self.scale_x).floor();
        let fy = ((c.y - self.upper_left_y) / self.scale_y).floor();
        if fx < 0.0 || fy < 0.0 {
            return None;
        }
        let x = (fx as u32).min(self.width - 1);
        let y = (fy as u32).min(self.height - 1);
        Some((x, y))
    }

    /// Encodes as hex text: a fixed header (magic, version, width, height,
    /// upper-left x/y, scale x/y, srid) followed by the row-major `f64` band,
    /// all little-endian. Not PostGIS's WKB-raster format — see module docs.
    pub fn to_hex(&self) -> String {
        let mut buf = Vec::with_capacity(4 + 1 + 4 + 4 + 8 * 4 + 4 + self.band.len() * 8);
        buf.extend_from_slice(&MAGIC.to_le_bytes());
        buf.push(VERSION);
        buf.extend_from_slice(&self.width.to_le_bytes());
        buf.extend_from_slice(&self.height.to_le_bytes());
        buf.extend_from_slice(&self.upper_left_x.to_le_bytes());
        buf.extend_from_slice(&self.upper_left_y.to_le_bytes());
        buf.extend_from_slice(&self.scale_x.to_le_bytes());
        buf.extend_from_slice(&self.scale_y.to_le_bytes());
        buf.extend_from_slice(&self.srid.to_le_bytes());
        for v in &self.band {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        to_hex(&buf)
    }

    /// Decodes hex text produced by [`Self::to_hex`].
    pub fn from_hex(s: &str) -> Result<Self> {
        let buf = from_hex(s)?;
        if buf.len() < 4 + 1 + 4 + 4 + 8 * 4 + 4 {
            bail!("raster hex too short to contain a header");
        }
        let mut pos = 0usize;
        fn take<'a>(buf: &'a [u8], pos: &mut usize, n: usize) -> &'a [u8] {
            let s = &buf[*pos..*pos + n];
            *pos += n;
            s
        }
        let magic = u32::from_le_bytes(take(&buf, &mut pos, 4).try_into().unwrap());
        if magic != MAGIC {
            bail!("not a TPT raster (bad magic)");
        }
        let version = take(&buf, &mut pos, 1)[0];
        if version != VERSION {
            bail!("unsupported raster encoding version {version}");
        }
        let width = u32::from_le_bytes(take(&buf, &mut pos, 4).try_into().unwrap());
        let height = u32::from_le_bytes(take(&buf, &mut pos, 4).try_into().unwrap());
        let upper_left_x = f64::from_le_bytes(take(&buf, &mut pos, 8).try_into().unwrap());
        let upper_left_y = f64::from_le_bytes(take(&buf, &mut pos, 8).try_into().unwrap());
        let scale_x = f64::from_le_bytes(take(&buf, &mut pos, 8).try_into().unwrap());
        let scale_y = f64::from_le_bytes(take(&buf, &mut pos, 8).try_into().unwrap());
        let srid = i32::from_le_bytes(take(&buf, &mut pos, 4).try_into().unwrap());

        let expected_cells = width as usize * height as usize;
        let remaining = buf.len() - pos;
        if remaining != expected_cells * 8 {
            bail!(
                "raster hex band length mismatch: expected {expected_cells} cells ({} bytes), got {remaining} bytes",
                expected_cells * 8
            );
        }
        let mut band = Vec::with_capacity(expected_cells);
        for _ in 0..expected_cells {
            band.push(f64::from_le_bytes(
                take(&buf, &mut pos, 8).try_into().unwrap(),
            ));
        }

        Ok(Self {
            width,
            height,
            upper_left_x,
            upper_left_y,
            scale_x,
            scale_y,
            srid,
            band,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_raster_roundtrips_through_hex() {
        let r = Raster::new_empty(3, 2, 10.0, 20.0, 1.0, -1.0, 4326);
        let hex = r.to_hex();
        let back = Raster::from_hex(&hex).unwrap();
        assert_eq!(r, back);
    }

    #[test]
    fn value_and_set_value_roundtrip() {
        let mut r = Raster::new_empty(4, 4, 0.0, 0.0, 1.0, -1.0, 0);
        assert_eq!(r.value(1, 2), Some(0.0));
        r.set_value(1, 2, 42.5).unwrap();
        assert_eq!(r.value(1, 2), Some(42.5));
        // Round-trips through hex too.
        let back = Raster::from_hex(&r.to_hex()).unwrap();
        assert_eq!(back.value(1, 2), Some(42.5));
    }

    #[test]
    fn set_value_out_of_range_errors() {
        let mut r = Raster::new_empty(2, 2, 0.0, 0.0, 1.0, -1.0, 0);
        assert!(r.set_value(5, 5, 1.0).is_err());
    }

    #[test]
    fn from_hex_rejects_bad_magic() {
        let bogus = to_hex(&[0u8; 40]);
        assert!(Raster::from_hex(&bogus).is_err());
    }

    #[test]
    fn rasterize_polygon_fills_only_interior_cells() {
        // A 4x4 square from (0,0) to (4,4), rasterized at 1-unit pixels.
        let geom = Geometry::from_wkt("POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))").unwrap();
        let r = Raster::rasterize(&geom, 1.0, 1.0, 7.0, 4326).unwrap();
        assert_eq!(r.width, 4);
        assert_eq!(r.height, 4);
        // Every cell center falls inside this square, so every cell is filled.
        assert!(r.band.iter().all(|&v| v == 7.0));
    }

    #[test]
    fn rasterize_polygon_leaves_exterior_cells_unfilled() {
        // A triangle with legs along the axes — its bbox's far corner (away
        // from the origin, across the hypotenuse) is well outside it, while
        // the corner cell nearest the origin is well inside.
        let geom = Geometry::from_wkt("POLYGON((0 0, 10 0, 0 10, 0 0))").unwrap();
        let r = Raster::rasterize(&geom, 1.0, 1.0, 9.0, 4326).unwrap();
        // Cell (9, 0): center (9.5, 9.5) — beyond the x+y=10 hypotenuse.
        assert_eq!(r.value(9, 0), Some(0.0));
        // Cell (0, 9): center (0.5, 0.5) — well inside, near the origin.
        assert_eq!(r.value(0, 9), Some(9.0));
    }

    #[test]
    fn rasterize_point_sets_single_nearest_cell() {
        let geom = Geometry::from_wkt("POINT(2.4 2.4)").unwrap();
        let r = Raster::rasterize(&geom, 1.0, 1.0, 5.0, 0).unwrap();
        // A single-point bbox rasterizes to a 1x1 raster.
        assert_eq!((r.width, r.height), (1, 1));
        assert_eq!(r.value(0, 0), Some(5.0));
    }
}
