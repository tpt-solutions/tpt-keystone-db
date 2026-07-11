//! Hand-written vector arithmetic: the "replaces BLAS/ndarray" piece of
//! Prism. Deliberately narrow — just the three similarity/distance
//! functions HNSW and the SQL scalar functions need, plus a text literal
//! format (`"[1.0, 2.0, 3.0]"`) that mirrors how `geo::geometry` round-trips
//! WKT text through `Value::Text`.
//!
//! No explicit SIMD intrinsics are used anywhere in this file (see the
//! module doc-comment in `vector::mod` for why) — these are plain scalar
//! loops over `&[f32]` that the compiler's auto-vectorizer is free to use.

use anyhow::{bail, Result};

/// A dense embedding vector. A thin newtype over `Vec<f32>` so call sites
/// read as vector operations rather than generic slice math.
#[derive(Debug, Clone, PartialEq)]
pub struct Vector(pub Vec<f32>);

impl Vector {
    pub fn dim(&self) -> usize {
        self.0.len()
    }

    pub fn as_slice(&self) -> &[f32] {
        &self.0
    }

    /// Parses a vector literal of the form `[1.0, 2.0, 3.0]` (whitespace
    /// around commas/brackets tolerated). This is the on-the-wire text
    /// representation used for `VECTOR` column values, the same way
    /// `geo::geometry::Geometry::from_wkt` parses `POINT(...)` text.
    pub fn from_text(s: &str) -> Result<Self> {
        let trimmed = s.trim();
        let inner = trimmed
            .strip_prefix('[')
            .and_then(|s| s.strip_suffix(']'))
            .ok_or_else(|| anyhow::anyhow!("invalid vector literal: {s}"))?;
        let inner = inner.trim();
        if inner.is_empty() {
            return Ok(Vector(Vec::new()));
        }
        let mut out = Vec::new();
        for part in inner.split(',') {
            let v: f32 = part
                .trim()
                .parse()
                .map_err(|_| anyhow::anyhow!("invalid vector component: {part}"))?;
            out.push(v);
        }
        Ok(Vector(out))
    }

    /// Serializes back to the `[1.0, 2.0, 3.0]` text form.
    pub fn to_text(&self) -> String {
        let parts: Vec<String> = self.0.iter().map(|v| v.to_string()).collect();
        format!("[{}]", parts.join(","))
    }
}

/// Checks both vectors share a dimension, returning it on success.
fn check_dims(a: &[f32], b: &[f32]) -> Result<usize> {
    if a.len() != b.len() {
        bail!("vector dimension mismatch: {} vs {}", a.len(), b.len());
    }
    if a.is_empty() {
        bail!("vectors must be non-empty");
    }
    Ok(a.len())
}

/// Squared L2 distance — avoids the `sqrt` for callers (like HNSW's
/// candidate-ranking inner loop) that only need relative ordering.
pub fn l2_distance_squared(a: &[f32], b: &[f32]) -> Result<f32> {
    check_dims(a, b)?;
    let mut sum = 0.0f32;
    for i in 0..a.len() {
        let d = a[i] - b[i];
        sum += d * d;
    }
    Ok(sum)
}

/// Euclidean (L2) distance between two vectors of equal dimension.
pub fn l2_distance(a: &[f32], b: &[f32]) -> Result<f32> {
    Ok(l2_distance_squared(a, b)?.sqrt())
}

/// Dot product (inner product) of two vectors of equal dimension.
pub fn dot_product(a: &[f32], b: &[f32]) -> Result<f32> {
    check_dims(a, b)?;
    let mut sum = 0.0f32;
    for i in 0..a.len() {
        sum += a[i] * b[i];
    }
    Ok(sum)
}

fn norm(a: &[f32]) -> f32 {
    a.iter().map(|v| v * v).sum::<f32>().sqrt()
}

/// Cosine similarity in `[-1.0, 1.0]` (up to floating point error). Returns
/// an error for a zero vector, since cosine similarity is undefined there.
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> Result<f32> {
    check_dims(a, b)?;
    let na = norm(a);
    let nb = norm(b);
    if na == 0.0 || nb == 0.0 {
        bail!("cosine similarity is undefined for a zero vector");
    }
    Ok(dot_product(a, b)? / (na * nb))
}

/// Cosine *distance* = `1 - cosine_similarity`, the form ANN indexes
/// typically minimize (0 = identical direction, 2 = opposite direction).
pub fn cosine_distance(a: &[f32], b: &[f32]) -> Result<f32> {
    Ok(1.0 - cosine_similarity(a, b)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn l2_distance_known_cases() {
        assert!((l2_distance(&[0.0, 0.0], &[3.0, 4.0]).unwrap() - 5.0).abs() < 1e-6);
        assert!((l2_distance(&[1.0, 1.0, 1.0], &[1.0, 1.0, 1.0]).unwrap() - 0.0).abs() < 1e-6);
        assert!((l2_distance(&[1.0], &[4.0]).unwrap() - 3.0).abs() < 1e-6);
    }

    #[test]
    fn dot_product_known_cases() {
        assert!((dot_product(&[1.0, 2.0, 3.0], &[4.0, 5.0, 6.0]).unwrap() - 32.0).abs() < 1e-6);
        assert!((dot_product(&[1.0, 0.0], &[0.0, 1.0]).unwrap() - 0.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_identical_vectors_is_one() {
        let a = [1.0f32, 2.0, 3.0];
        let sim = cosine_similarity(&a, &a).unwrap();
        assert!((sim - 1.0).abs() < 1e-5);
        assert!(cosine_distance(&a, &a).unwrap().abs() < 1e-5);
    }

    #[test]
    fn cosine_orthogonal_vectors_is_zero() {
        let sim = cosine_similarity(&[1.0, 0.0], &[0.0, 1.0]).unwrap();
        assert!(sim.abs() < 1e-6);
    }

    #[test]
    fn cosine_opposite_vectors_is_minus_one() {
        let sim = cosine_similarity(&[1.0, 0.0], &[-1.0, 0.0]).unwrap();
        assert!((sim + 1.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_normalized_vectors_matches_dot_product() {
        // For unit vectors, cosine similarity equals the dot product exactly.
        let a = [0.6f32, 0.8]; // already unit length (3-4-5 triangle / 5)
        let b = [0.8f32, 0.6];
        let dot = dot_product(&a, &b).unwrap();
        let cos = cosine_similarity(&a, &b).unwrap();
        assert!((dot - cos).abs() < 1e-5);
    }

    #[test]
    fn dimension_mismatch_errors() {
        assert!(l2_distance(&[1.0, 2.0], &[1.0]).is_err());
        assert!(dot_product(&[1.0, 2.0], &[1.0]).is_err());
        assert!(cosine_similarity(&[1.0, 2.0], &[1.0]).is_err());
    }

    #[test]
    fn text_round_trip() {
        let v = Vector::from_text("[1.0, 2.5, -3.0]").unwrap();
        assert_eq!(v.0, vec![1.0, 2.5, -3.0]);
        let text = v.to_text();
        let v2 = Vector::from_text(&text).unwrap();
        assert_eq!(v, v2);
    }
}
