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
//!
//! Explicitly NOT implemented (documented scope cuts, tracked in
//! `TODO.md`): IVF-PQ, DiskANN, automatic latency/recall-aware index
//! selection, hybrid vector+BM25+SQL-filter search (no full-text engine
//! exists in this codebase), native product/scalar/binary quantization,
//! consistent hashing for distributed shards, and CUDA/ROCm GPU offload.
//! All are left unchecked in `TODO.md` rather than stubbed out and claimed
//! done.

pub mod hnsw;
pub mod vector;
