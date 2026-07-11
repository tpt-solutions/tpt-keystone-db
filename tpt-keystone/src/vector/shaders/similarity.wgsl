// GPU batch vector-similarity kernel (Prism). One invocation per
// (query_index, base_index) pair. Writes the pairwise distance into a flat,
// row-major matrix (out[q * num_base + b]).
//
// f32 throughout: WGSL has no portable f64, and the CPU path this mirrors
// (`vector::vector`) is already f32, so this is an exact-precision match, not
// a narrowing like Meridian's geo::gpu (whose CPU path is f64).
//
// One dispatch covers the whole query×base matrix (a matmul-shaped batch
// workload), then the host does a cheap top-k over the returned row. No
// shared-memory top-k bookkeeping on the device — not worth it for the small
// k vector search uses.

struct Params {
    num_queries: u32,
    num_base: u32,
    dim: u32,
    metric: u32, // 0 = L2 (euclidean), 1 = cosine distance
    _pad0: u32,
    _pad1: u32,
    _pad2: u32,
    _pad3: u32,
};

@group(0) @binding(0) var<uniform> params: Params;
@group(0) @binding(1) var<storage, read> queries: array<f32>;
@group(0) @binding(2) var<storage, read> base: array<f32>;
@group(0) @binding(3) var<storage, read_write> out_dist: array<f32>;

@compute @workgroup_size(8, 8, 1)
fn main(@builtin(global_invocation_id) gid: vec3<u32>) {
    let q = gid.x;
    let b = gid.y;
    if (q >= params.num_queries || b >= params.num_base) {
        return;
    }
    let dim = params.dim;
    let qoff = q * dim;
    let boff = b * dim;

    var sum_sq: f32 = 0.0;
    var dot: f32 = 0.0;
    var q_norm: f32 = 0.0;
    var b_norm: f32 = 0.0;
    for (var i: u32 = 0u; i < dim; i = i + 1u) {
        let x = queries[qoff + i];
        let y = base[boff + i];
        let d = x - y;
        sum_sq = sum_sq + d * d;
        dot = dot + x * y;
        q_norm = q_norm + x * x;
        b_norm = b_norm + y * y;
    }

    var dist: f32;
    if (params.metric == 1u) {
        // cosine distance = 1 - (dot / (|q| * |b|)); clamp ratio to [-1, 1].
        let denom = sqrt(q_norm) * sqrt(b_norm);
        if (denom <= 0.0) {
            dist = 1.0;
        } else {
            var c = dot / denom;
            c = clamp(c, -1.0, 1.0);
            dist = 1.0 - c;
        }
    } else {
        dist = sqrt(sum_sq);
    }
    out_dist[q * params.num_base + b] = dist;
}
