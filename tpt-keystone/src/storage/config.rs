//! Compute-node configuration: which object-store backend to use, cache
//! sizing, node identity, and role (writer vs. read replica). Everything
//! comes from environment variables with defaults that reproduce the old
//! single-node local-disk behavior, so `cargo run` with no env vars set
//! still works exactly as before — just routed through the object-store +
//! cache seam instead of raw `std::fs`.

use super::objectstore::{LocalFsObjectStore, ObjectStore, S3ObjectStore};
use anyhow::Result;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

/// Sandboxing limits applied to every WASM UDF invocation (see
/// `executor::udf`): a fuel budget (bounds execution steps, so a UDF can't
/// hang a connection) and a linear-memory cap (bounds how much a UDF's own
/// module can allocate).
#[derive(Debug, Clone, Copy)]
pub struct UdfConfig {
    pub fuel_limit: u64,
    pub memory_limit_bytes: usize,
}

impl Default for UdfConfig {
    fn default() -> Self {
        Self { fuel_limit: 100_000_000, memory_limit_bytes: 16 * 1024 * 1024 }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    /// Holds the write lease; the only node allowed to flush/mutate.
    Writer,
    /// Read-only replica; polls the manifest to converge with the writer.
    Reader,
}

#[derive(Debug, Clone)]
pub enum StorageBackend {
    Local { dir: PathBuf },
    S3 { bucket: String, region: Option<String>, endpoint_url: Option<String>, prefix: String },
}

#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub backend: StorageBackend,
    pub cache_dir: PathBuf,
    pub cache_max_bytes: u64,
    pub node_id: String,
    pub role: NodeRole,
    pub lease_ttl: Duration,
    pub manifest_refresh_interval: Duration,
    /// Root for this node's disposable local state (active WAL segment,
    /// local B-Tree indexes) — everything durable lives in the object store.
    pub local_dir: PathBuf,
    pub udf: UdfConfig,
    /// Max concurrent client connections. Every connection already shares
    /// one `Database`/one LSM engine (there's no per-connection backend
    /// process to pool the way pgbouncer pools real Postgres backends), so
    /// this is admission control/backpressure rather than resource pooling
    /// — connections beyond the limit queue instead of erroring.
    pub max_connections: usize,
    /// Bearer token required on the `X-TPT-Token` header for the MCP server
    /// (port 5433). Unset means no auth — matches the Postgres listener's
    /// existing no-auth dev-mode default.
    pub mcp_token: Option<String>,
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

impl StorageConfig {
    pub fn from_env() -> Self {
        let backend = if env_or("TPT_STORAGE_BACKEND", "local").eq_ignore_ascii_case("s3") {
            StorageBackend::S3 {
                bucket: env::var("TPT_S3_BUCKET").expect("TPT_S3_BUCKET must be set when TPT_STORAGE_BACKEND=s3"),
                region: env::var("TPT_S3_REGION").ok(),
                endpoint_url: env::var("TPT_S3_ENDPOINT").ok(),
                prefix: env_or("TPT_S3_PREFIX", ""),
            }
        } else {
            StorageBackend::Local { dir: PathBuf::from(env_or("TPT_LOCAL_STORE_DIR", "tpt-data/objects")) }
        };

        let role = match env_or("TPT_NODE_ROLE", "writer").to_lowercase().as_str() {
            "reader" | "replica" => NodeRole::Reader,
            _ => NodeRole::Writer,
        };

        let node_id = env::var("TPT_NODE_ID").unwrap_or_else(|_| format!("node-{:08x}", rand::random::<u32>()));

        Self {
            backend,
            cache_dir: PathBuf::from(env_or("TPT_CACHE_DIR", "tpt-data/cache")),
            cache_max_bytes: env::var("TPT_CACHE_MAX_BYTES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(256 * 1024 * 1024),
            node_id,
            role,
            lease_ttl: Duration::from_secs(env::var("TPT_LEASE_TTL_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(15)),
            manifest_refresh_interval: Duration::from_secs(
                env::var("TPT_MANIFEST_REFRESH_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(5),
            ),
            local_dir: PathBuf::from(env_or("TPT_LOCAL_DIR", "tpt-data")),
            udf: UdfConfig {
                fuel_limit: env::var("TPT_UDF_FUEL_LIMIT").ok().and_then(|v| v.parse().ok()).unwrap_or(UdfConfig::default().fuel_limit),
                memory_limit_bytes: env::var("TPT_UDF_MEMORY_LIMIT_BYTES").ok().and_then(|v| v.parse().ok()).unwrap_or(UdfConfig::default().memory_limit_bytes),
            },
            max_connections: env::var("TPT_MAX_CONNECTIONS").ok().and_then(|v| v.parse().ok()).unwrap_or(1000),
            mcp_token: env::var("TPT_MCP_TOKEN").ok(),
        }
    }

    pub async fn build_object_store(&self) -> Result<Arc<dyn ObjectStore>> {
        match &self.backend {
            StorageBackend::Local { dir } => Ok(Arc::new(LocalFsObjectStore::open(dir)?)),
            StorageBackend::S3 { bucket, region, endpoint_url, prefix } => {
                let store = S3ObjectStore::connect(bucket.clone(), region.clone(), endpoint_url.clone(), prefix.clone()).await?;
                Ok(Arc::new(store))
            }
        }
    }
}
