//! Standalone GPU smoke test for Meridian's spatial-join compute shaders.
//!
//! Run manually with `cargo run --example gpu_smoke_test` *before* trusting
//! any GPU-path change to `geo::gpu` or the join executor. This runs as its
//! own OS process, separate from `cargo test`/the server binary — that
//! separation is the actual point: an OS-level GPU driver trap (not a
//! catchable Rust panic) can't be caught by `catch_unwind`, so an isolated
//! process is the only real safety margin against a bad driver/pipeline
//! crashing something more important than a smoke test. This repo has prior
//! history of untested native/low-level code paths crashing hard rather than
//! failing gracefully (see `docs/security_audit_phase12.md`'s Wasmtime-trap
//! note) — treat any crash here as a stop-and-reassess signal, not something
//! to debug blind inside the query executor.

use tpt_keystone::geo::gpu::{gpu_bbox_overlap_pairs, GpuBBox};

fn main() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    std::panic::set_hook(Box::new(|info| {
        eprintln!("GPU smoke test PANICKED: {info}");
    }));

    let result = std::panic::catch_unwind(run);
    match result {
        Ok(Ok(())) => {
            println!("GPU smoke test: PASS");
        }
        Ok(Err(e)) => {
            eprintln!("GPU smoke test FAILED: {e}");
            std::process::exit(1);
        }
        Err(_) => {
            eprintln!("GPU smoke test PANICKED (see message above)");
            std::process::exit(1);
        }
    }
}

fn run() -> anyhow::Result<()> {
    // A hand-computed known-answer case: overlap, no-overlap, and an
    // edge-touching (closed-interval boundary) pair.
    let left = [
        GpuBBox { min_x: 0.0, min_y: 0.0, max_x: 10.0, max_y: 10.0 },
        GpuBBox { min_x: 100.0, min_y: 100.0, max_x: 110.0, max_y: 110.0 },
        GpuBBox { min_x: 0.0, min_y: 0.0, max_x: 5.0, max_y: 5.0 },
    ];
    let right = [
        GpuBBox { min_x: 5.0, min_y: 5.0, max_x: 15.0, max_y: 15.0 }, // overlaps left[0], touches left[2] at (5,5)
        GpuBBox { min_x: 200.0, min_y: 200.0, max_x: 210.0, max_y: 210.0 }, // overlaps nothing
    ];
    let expected: Vec<(u32, u32)> = vec![(0, 0), (2, 0)];

    let mut got = gpu_bbox_overlap_pairs(&left, &right)?;
    got.sort();

    if got != expected {
        anyhow::bail!("mismatch: expected {expected:?}, got {got:?}");
    }

    println!("GPU spatial-join broad-phase result matched expected pairs: {got:?}");
    Ok(())
}
