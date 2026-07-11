//! TPT Prism — vector/AI engine, implemented as a native module inside
//! Keystone (per `7prismspec.txt`) rather than a separate crate/process.
//!
//! Scope actually implemented in this pass:
//! - `vector`: a plain `Vec<f32>` vector type, hand-written L2 (Euclidean)
//!   distance, cosine distance/similarity, and dot product, plus text
//!   literal parsing/serialization (`"[1.0, 2.0, 3.0]"`) mirroring how
//!   `geo::geometry` round-trips WKT text. Honest scope cut: these are
//!   straightforward scalar Rust loops with **no explicit SIMD intrinsics**
//!   (no `std::arch`, no `packed_simd`) — hand-written SIMD can't be
//!   verified for correctness or actual speedup without a benchmarking
//!   harness in this sandbox (see the wasmtime-on-Windows crash note in
//!   project memory for how badly "wrote low-level code, never verified it"
//!   goes here). The scalar loops are straight line/iterator code that the
//!   compiler's auto-vectorizer is free to vectorize; no claim is made about
//!   whether it actually does on any given target.
//! - `hnsw`: a real, from-scratch multi-layer Hierarchical Navigable Small
//!   World approximate-nearest-neighbor graph index (Malkov & Yashunin),
//!   with configurable `M`/`ef_construction`/`ef_search`, insert and k-NN
//!   search — not a brute-force scan pretending to be HNSW.
//! - `kmeans`/`pq`/`ivf_pq`: a real IVF-PQ index (inverted file of coarse
//!   k-means clusters, each storing product-quantized residual codes rather
//!   than raw floats) — the memory-constrained/larger-scale counterpart to
//!   HNSW's graph, per `vector::ivf_pq`'s module doc-comment. This is also
//!   where "native product quantization" lives: `pq::ProductQuantizer` is
//!   real PQ (subvector k-means codebooks + asymmetric distance tables), not
//!   a stub.
//!
//! Hybrid vector+BM25+SQL-filter search now exists (`hybrid_search` table
//! function, `executor/graph_fn.rs`, backed by Canopy's BM25 `FtsIndex`).
//!
//! GPU offload for batch similarity is now implemented (`gpu`): a WGSL
//! compute shader computes the full query×base distance matrix on the
//! device, wired into `Database::vector_knn_query` as a fail-safe brute-force
//! k-NN path for `vector_search`/`hybrid_search` when no HNSW/IVF-PQ index
//! exists and a GPU adapter is available (same WGSL/`f32`/fail-safe discipline
//! as Meridian's `geo::gpu`). This is the "CUDA/ROCm GPU offload" item from the
//! roadmap, delivered via `wgpu` (Vulkan/Metal/DX12) rather than a vendor
//! CUDA/ROCm backend — see `gpu`'s module docs for the honest portability note.
//!
//! Explicitly still NOT implemented (documented scope cuts, tracked in
//! `TODO.md`): DiskANN (on-disk billion-scale graphs), scalar/binary
//! quantization (only product quantization exists), and consistent hashing for
//! distributed shards. All are left unchecked in `TODO.md` rather than stubbed
//! out and claimed done.

pub mod gpu;
pub mod hnsw;
pub mod ivf_pq;
pub mod kmeans;
pub mod pq;
pub mod vector;
