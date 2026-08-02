//! Backend-agnostic 2D pan/zoom transform, shared by `CanvasMap`/`CanvasGraph`'s
//! Canvas2D *and* WebGPU render paths (Phase 10) so pan/zoom math is written
//! once rather than duplicated per backend. Pure `f64` arithmetic, no DOM —
//! host-testable without `#[cfg(target_arch = "wasm32")]`, unlike almost
//! everything else that touches rendering in this crate.
//!
//! Convention: `screen = data * scale + translate`. "Screen" space is CSS
//! pixels with `(0,0)` top-left, matching every existing component's
//! `Canvas2d`/click-handler coordinate convention; "data" space is whatever
//! the caller projected world coordinates into before this transform was
//! applied (e.g. `map::project`'s equirectangular output, or a graph layout's
//! node positions).

/// Scale is clamped to this range on every `zoom` call so a runaway wheel
/// gesture (or repeated double-tap-zoom) can't invert or degenerate the view.
pub const MIN_SCALE: f64 = 0.05;
pub const MAX_SCALE: f64 = 50.0;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ViewTransform {
    pub scale: f64,
    pub tx: f64,
    pub ty: f64,
}

impl Default for ViewTransform {
    fn default() -> Self {
        Self::identity()
    }
}

impl ViewTransform {
    pub fn identity() -> Self {
        Self { scale: 1.0, tx: 0.0, ty: 0.0 }
    }

    /// Data space -> screen space.
    pub fn to_screen(&self, x: f64, y: f64) -> (f64, f64) {
        (x * self.scale + self.tx, y * self.scale + self.ty)
    }

    /// Screen space -> data space (the inverse of `to_screen`).
    pub fn to_data(&self, x: f64, y: f64) -> (f64, f64) {
        ((x - self.tx) / self.scale, (y - self.ty) / self.scale)
    }

    /// Converts a screen-space distance (e.g. a hit-test radius in pixels)
    /// into the equivalent distance in data space at the current zoom level.
    pub fn screen_to_data_distance(&self, d: f64) -> f64 {
        d / self.scale
    }

    /// Translates the view by `(dx, dy)` screen pixels — a drag gesture's
    /// per-frame delta.
    pub fn pan(&mut self, dx: f64, dy: f64) {
        self.tx += dx;
        self.ty += dy;
    }

    /// Multiplies the current scale by `factor` (clamped to
    /// `[MIN_SCALE, MAX_SCALE]`), pivoting so the data point currently under
    /// screen coordinate `(px, py)` stays under that same screen coordinate
    /// after the zoom — the standard "zoom toward the cursor" behavior.
    pub fn zoom(&mut self, factor: f64, px: f64, py: f64) {
        let new_scale = (self.scale * factor).clamp(MIN_SCALE, MAX_SCALE);
        let actual_factor = new_scale / self.scale;
        self.tx = px - (px - self.tx) * actual_factor;
        self.ty = py - (py - self.ty) * actual_factor;
        self.scale = new_scale;
    }
}

/// Maps a point already in `[0,1] x [0,1]` normalized space (`(0,0)`
/// top-left, `(1,1)` bottom-right — matching `webgpu::timeseries`'s existing
/// convention) to `[-1,1]` WebGPU clip space (`(-1,-1)` bottom-left,
/// y-up).
pub fn normalized_to_clip(x: f64, y: f64) -> (f32, f32) {
    ((2.0 * x - 1.0) as f32, (1.0 - 2.0 * y) as f32)
}

/// CPU-expands an axis-aligned quad centered at `(cx, cy)` (already in clip
/// space) with clip-space half-extents `(half_w, half_h)` into two
/// counter-clockwise triangles (6 vertices), for `GpuPrimitiveTopology::TriangleList`
/// marker/node rendering — this crate's WebGPU renderers don't use vertex-buffer
/// instancing (see `webgpu/mod.rs` module docs), so every quad is expanded on
/// the CPU into plain triangle vertices instead.
pub fn quad_vertices_clip(cx: f32, cy: f32, half_w: f32, half_h: f32) -> [(f32, f32); 6] {
    let (l, r, t, b) = (cx - half_w, cx + half_w, cy + half_h, cy - half_h);
    [(l, b), (r, b), (r, t), (l, b), (r, t), (l, t)]
}

/// Converts a `WheelEvent.deltaY` reading into a multiplicative zoom factor
/// for `ViewTransform::zoom` — negative `delta_y` (scroll up / away from the
/// user) zooms in (`factor > 1`), positive zooms out (`factor < 1`).
/// Exponential rather than linear so repeated small scroll ticks compound
/// smoothly instead of the zoom speed depending on the current scale.
pub fn zoom_factor_from_wheel_delta(delta_y: f64) -> f64 {
    (-delta_y * 0.001).exp()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_is_a_noop() {
        let v = ViewTransform::identity();
        assert_eq!(v.to_screen(3.0, 4.0), (3.0, 4.0));
        assert_eq!(v.to_data(3.0, 4.0), (3.0, 4.0));
    }

    #[test]
    fn to_screen_and_to_data_round_trip() {
        let mut v = ViewTransform::identity();
        v.pan(15.0, -8.0);
        v.zoom(2.5, 100.0, 100.0);
        for &(x, y) in &[(0.0, 0.0), (12.3, -45.6), (1000.0, -1000.0)] {
            let (sx, sy) = v.to_screen(x, y);
            let (dx, dy) = v.to_data(sx, sy);
            assert!((dx - x).abs() < 1e-9, "x round-trip: {dx} vs {x}");
            assert!((dy - y).abs() < 1e-9, "y round-trip: {dy} vs {y}");
        }
    }

    #[test]
    fn zoom_keeps_pivot_point_fixed_in_data_space() {
        let mut v = ViewTransform::identity();
        v.pan(50.0, -20.0);
        let pivot = (123.0, 456.0);
        let before = v.to_data(pivot.0, pivot.1);
        v.zoom(3.0, pivot.0, pivot.1);
        let after = v.to_data(pivot.0, pivot.1);
        assert!((before.0 - after.0).abs() < 1e-9);
        assert!((before.1 - after.1).abs() < 1e-9);

        // Repeated zoom-out around a different pivot must also hold.
        let pivot2 = (10.0, 10.0);
        let before2 = v.to_data(pivot2.0, pivot2.1);
        v.zoom(0.4, pivot2.0, pivot2.1);
        let after2 = v.to_data(pivot2.0, pivot2.1);
        assert!((before2.0 - after2.0).abs() < 1e-9);
        assert!((before2.1 - after2.1).abs() < 1e-9);
    }

    #[test]
    fn zoom_scale_is_clamped() {
        let mut v = ViewTransform::identity();
        for _ in 0..200 {
            v.zoom(10.0, 0.0, 0.0);
        }
        assert!(v.scale <= MAX_SCALE, "scale {} exceeded MAX_SCALE", v.scale);

        let mut v = ViewTransform::identity();
        for _ in 0..200 {
            v.zoom(0.1, 0.0, 0.0);
        }
        assert!(v.scale >= MIN_SCALE, "scale {} below MIN_SCALE", v.scale);
    }

    #[test]
    fn screen_to_data_distance_scales_inversely() {
        let mut v = ViewTransform::identity();
        v.zoom(4.0, 0.0, 0.0);
        assert!((v.screen_to_data_distance(40.0) - 10.0).abs() < 1e-9);
    }

    #[test]
    fn normalized_to_clip_maps_corners() {
        assert_eq!(normalized_to_clip(0.0, 0.0), (-1.0, 1.0));
        assert_eq!(normalized_to_clip(1.0, 1.0), (1.0, -1.0));
        assert_eq!(normalized_to_clip(0.5, 0.5), (0.0, 0.0));
    }

    #[test]
    fn quad_vertices_clip_covers_expected_bounds() {
        let quad = quad_vertices_clip(0.0, 0.0, 0.1, 0.2);
        let xs: Vec<f32> = quad.iter().map(|p| p.0).collect();
        let ys: Vec<f32> = quad.iter().map(|p| p.1).collect();
        assert!(xs.iter().all(|&x| (-0.1..=0.1).contains(&x)));
        assert!(ys.iter().all(|&y| (-0.2..=0.2).contains(&y)));
        assert_eq!(quad.len(), 6);
    }

    #[test]
    fn zoom_factor_from_wheel_delta_direction() {
        assert!(zoom_factor_from_wheel_delta(-100.0) > 1.0);
        assert!(zoom_factor_from_wheel_delta(100.0) < 1.0);
        assert_eq!(zoom_factor_from_wheel_delta(0.0), 1.0);
    }
}
