// GPU broad-phase spatial join kernels (Meridian). Two independent entry
// points sharing the same dispatch/readback shape: each invocation handles
// one (left_idx, right_idx) pair and writes a 0/1 match flag into a flat
// row-major result buffer (result[i * right_len + j]). No shared state
// between invocations, no atomics needed.
//
// f32 throughout: WGSL has no portable f64. Coordinates are downcast from
// the engine's f64 lon/lat before upload — see geo/gpu.rs module docs for
// why this is an accepted, documented precision narrowing for the broad
// phase only.

struct BBox {
    min_x: f32,
    min_y: f32,
    max_x: f32,
    max_y: f32,
};

struct Point {
    x: f32,
    y: f32,
};

struct Params {
    left_len: u32,
    right_len: u32,
    radius_m: f32,
    _pad: u32,
};

const EARTH_RADIUS_M: f32 = 6371008.8;

// Every resource variable below has a distinct (group, binding) pair even
// though each compute entry point only ever touches a subset of them — WGSL
// requires unique binding slots per module-scope variable regardless of
// which entry point reaches them. `layout: "auto"` pipeline creation then
// derives a per-entry-point bind group layout containing only the bindings
// that entry point's reachable code actually uses.
@group(0) @binding(0) var<uniform> params: Params;
@group(0) @binding(3) var<storage, read_write> result: array<u32>;

@group(0) @binding(1) var<storage, read> left_bboxes: array<BBox>;
@group(0) @binding(2) var<storage, read> right_bboxes: array<BBox>;

// Closed-interval AABB overlap test, matching geometry::bbox_intersects
// exactly: a.min_x <= b.max_x && a.max_x >= b.min_x && (same for y).
@compute @workgroup_size(8, 8, 1)
fn bbox_overlap(@builtin(global_invocation_id) gid: vec3<u32>) {
    let i = gid.x;
    let j = gid.y;
    if (i >= params.left_len || j >= params.right_len) {
        return;
    }
    let a = left_bboxes[i];
    let b = right_bboxes[j];
    let overlap = a.min_x <= b.max_x && a.max_x >= b.min_x && a.min_y <= b.max_y && a.max_y >= b.min_y;
    result[i * params.right_len + j] = select(0u, 1u, overlap);
}

@group(0) @binding(4) var<storage, read> left_points: array<Point>;
@group(0) @binding(5) var<storage, read> right_points: array<Point>;

// Haversine great-circle distance, ported from geometry::haversine_distance_m,
// compared against params.radius_m.
fn haversine_m(lon1: f32, lat1: f32, lon2: f32, lat2: f32) -> f32 {
    let lat1r = radians(lat1);
    let lat2r = radians(lat2);
    let dlat = radians(lat2 - lat1);
    let dlon = radians(lon2 - lon1);
    let sin_dlat = sin(dlat / 2.0);
    let sin_dlon = sin(dlon / 2.0);
    let a = sin_dlat * sin_dlat + cos(lat1r) * cos(lat2r) * sin_dlon * sin_dlon;
    let c = 2.0 * asin(sqrt(a));
    return EARTH_RADIUS_M * c;
}

@compute @workgroup_size(8, 8, 1)
fn dwithin(@builtin(global_invocation_id) gid: vec3<u32>) {
    let i = gid.x;
    let j = gid.y;
    if (i >= params.left_len || j >= params.right_len) {
        return;
    }
    let a = left_points[i];
    let b = right_points[j];
    let within = haversine_m(a.x, a.y, b.x, b.y) <= params.radius_m;
    result[i * params.right_len + j] = select(0u, 1u, within);
}
