//! The storage-backend seam for disaggregated storage.
//!
//! `ObjectStore` is the boundary between the LSM engine (SSTables, sealed WAL
//! segments, the manifest, the writer lease) and *where those bytes actually
//! live*. Two implementations exist:
//!
//! - [`LocalFsObjectStore`] emulates S3 semantics (content-hash ETags,
//!   conditional PUT) on the local filesystem, so the disaggregated design can
//!   be exercised end-to-end (including "two compute nodes share one bucket")
//!   without a real bucket.
//! - [`S3ObjectStore`] talks to a real S3-compatible endpoint (AWS S3, MinIO,
//!   etc.) using conditional `If-Match` / `If-None-Match` headers for genuine
//!   compare-and-swap.

use anyhow::{anyhow, Context, Result};
use sha2::{Digest, Sha256};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Metadata returned alongside an object's bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    pub etag: String,
    pub size: u64,
}

/// Error returned by a failed compare-and-swap (`put_if_match`).
#[derive(Debug)]
pub enum CasError {
    /// The object's current state didn't match what the caller expected.
    Conflict { current_etag: Option<String> },
    /// Some other failure (I/O, network, etc).
    Other(anyhow::Error),
}

impl fmt::Display for CasError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CasError::Conflict { current_etag } => {
                write!(f, "conditional put failed: current etag = {current_etag:?}")
            }
            CasError::Other(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for CasError {}

impl From<anyhow::Error> for CasError {
    fn from(e: anyhow::Error) -> Self {
        CasError::Other(e)
    }
}

/// A blob store keyed by opaque string keys (e.g. `"sst/orders/00000001"`,
/// `"manifest.bin"`, `"_lease/db"`). Implementations must support conditional
/// writes so callers can build compare-and-swap protocols (manifest updates,
/// lease acquisition/fencing) on top.
pub trait ObjectStore: Send + Sync {
    /// Fetch an object's bytes and metadata, or `None` if it doesn't exist.
    fn get(&self, key: &str) -> Result<Option<(Vec<u8>, ObjectMeta)>>;
    /// Unconditionally write an object, returning its new metadata.
    fn put(&self, key: &str, data: &[u8]) -> Result<ObjectMeta>;
    /// Write an object only if its current ETag matches `expected_etag`
    /// (`None` means "only if the object does not currently exist").
    fn put_if_match(&self, key: &str, data: &[u8], expected_etag: Option<&str>) -> Result<ObjectMeta, CasError>;
    /// Delete an object. Deleting a missing object is not an error.
    fn delete(&self, key: &str) -> Result<()>;
    /// List all keys with the given prefix.
    fn list(&self, prefix: &str) -> Result<Vec<String>>;
}

// Lets an `Arc<dyn ObjectStore>` itself be used anywhere an `S: ObjectStore`
// is expected (e.g. wrapping it in `CachedObjectStore<Arc<dyn ObjectStore>>`)
// without needing to know the concrete backend type.
impl ObjectStore for std::sync::Arc<dyn ObjectStore> {
    fn get(&self, key: &str) -> Result<Option<(Vec<u8>, ObjectMeta)>> {
        (**self).get(key)
    }
    fn put(&self, key: &str, data: &[u8]) -> Result<ObjectMeta> {
        (**self).put(key, data)
    }
    fn put_if_match(&self, key: &str, data: &[u8], expected_etag: Option<&str>) -> Result<ObjectMeta, CasError> {
        (**self).put_if_match(key, data, expected_etag)
    }
    fn delete(&self, key: &str) -> Result<()> {
        (**self).delete(key)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        (**self).list(prefix)
    }
}

fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// Emulates an S3-compatible object store on the local filesystem.
///
/// Writes are atomic via temp-file-then-rename; conditional writes are
/// serialized behind a single in-process mutex (this is a single-machine
/// emulation for dev/testing — it does not provide cross-process atomicity
/// guarantees the way a real object store's conditional-write API would).
pub struct LocalFsObjectStore {
    root: PathBuf,
    cas_lock: Mutex<()>,
}

impl LocalFsObjectStore {
    pub fn open(root: &Path) -> Result<Self> {
        fs::create_dir_all(root)?;
        Ok(Self {
            root: root.to_path_buf(),
            cas_lock: Mutex::new(()),
        })
    }

    fn path_for(&self, key: &str) -> PathBuf {
        // Keys are internally-generated (table names, numeric ids), so a
        // direct join is safe; normalize backslashes in case a caller passes
        // a Windows-style separator.
        self.root.join(key.replace('\\', "/"))
    }
}

impl ObjectStore for LocalFsObjectStore {
    fn get(&self, key: &str) -> Result<Option<(Vec<u8>, ObjectMeta)>> {
        let path = self.path_for(key);
        match fs::read(&path) {
            Ok(data) => {
                let etag = sha256_hex(&data);
                let size = data.len() as u64;
                Ok(Some((data, ObjectMeta { etag, size })))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).context(format!("reading object {key}")),
        }
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<ObjectMeta> {
        let path = self.path_for(key);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| format!("creating parent dir for object {key} ({})", parent.display()))?;
        }
        let tmp_name = format!("{}.tmp-{}", path.file_name().unwrap().to_string_lossy(), rand::random::<u64>());
        let tmp_path = path.with_file_name(tmp_name);
        fs::write(&tmp_path, data).with_context(|| format!("writing temp object for {key} at {}", tmp_path.display()))?;
        fs::rename(&tmp_path, &path).with_context(|| format!("renaming temp object into place for {key} ({} -> {})", tmp_path.display(), path.display()))?;
        Ok(ObjectMeta {
            etag: sha256_hex(data),
            size: data.len() as u64,
        })
    }

    fn put_if_match(&self, key: &str, data: &[u8], expected_etag: Option<&str>) -> Result<ObjectMeta, CasError> {
        let _guard = self.cas_lock.lock().unwrap();
        let current = self.get(key)?;
        let current_etag = current.as_ref().map(|(_, m)| m.etag.clone());
        let matches = match (expected_etag, current_etag.as_deref()) {
            (None, None) => true,
            (Some(exp), Some(cur)) => exp == cur,
            _ => false,
        };
        if !matches {
            return Err(CasError::Conflict { current_etag });
        }
        Ok(self.put(key, data)?)
    }

    fn delete(&self, key: &str) -> Result<()> {
        let path = self.path_for(key);
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e).context(format!("deleting object {key}")),
        }
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut results = Vec::new();
        visit_dir(&self.root, &self.root, &mut results)?;
        Ok(results.into_iter().filter(|k| k.starts_with(prefix)).collect())
    }
}

fn visit_dir(root: &Path, dir: &Path, out: &mut Vec<String>) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            visit_dir(root, &path, out)?;
        } else {
            let rel = path.strip_prefix(root).unwrap_or(&path);
            let key = rel.to_string_lossy().replace('\\', "/");
            // Skip in-flight temp files from `put`.
            if !key.contains(".tmp-") {
                out.push(key);
            }
        }
    }
    Ok(())
}

/// A real S3-compatible object store, backed by `aws-sdk-s3`. Conditional
/// writes use `If-Match` / `If-None-Match` headers, which S3 (and
/// S3-compatible stores such as MinIO) honor as true compare-and-swap.
///
/// The SDK's client is async; this type bridges to the engine's synchronous
/// `ObjectStore` trait via `tokio::task::block_in_place` + the captured
/// runtime handle, so callers on a multi-threaded Tokio runtime can call it
/// like any other blocking storage call.
pub struct S3ObjectStore {
    client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
    handle: tokio::runtime::Handle,
}

impl S3ObjectStore {
    /// Build a client from the ambient AWS config (env vars / instance
    /// profile / etc), optionally pointed at a custom endpoint (e.g. MinIO).
    pub async fn connect(bucket: String, region: Option<String>, endpoint_url: Option<String>, prefix: String) -> Result<Self> {
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let Some(region) = region {
            loader = loader.region(aws_config::Region::new(region));
        }
        let sdk_config = loader.load().await;
        let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&sdk_config);
        if let Some(endpoint) = endpoint_url {
            s3_config_builder = s3_config_builder.endpoint_url(endpoint).force_path_style(true);
        }
        let client = aws_sdk_s3::Client::from_conf(s3_config_builder.build());
        Ok(Self {
            client,
            bucket,
            prefix,
            handle: tokio::runtime::Handle::current(),
        })
    }

    fn full_key(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}/{}", self.prefix.trim_end_matches('/'), key)
        }
    }

    fn block_on<F: std::future::Future>(&self, fut: F) -> F::Output {
        tokio::task::block_in_place(|| self.handle.block_on(fut))
    }

    fn strip_etag(raw: Option<&str>) -> String {
        raw.unwrap_or_default().trim_matches('"').to_string()
    }
}

impl ObjectStore for S3ObjectStore {
    fn get(&self, key: &str) -> Result<Option<(Vec<u8>, ObjectMeta)>> {
        let full = self.full_key(key);
        let result = self.block_on(
            self.client
                .get_object()
                .bucket(&self.bucket)
                .key(&full)
                .send(),
        );
        match result {
            Ok(output) => {
                let etag = Self::strip_etag(output.e_tag());
                let body = self
                    .block_on(output.body.collect())
                    .context("reading S3 object body")?
                    .into_bytes()
                    .to_vec();
                let size = body.len() as u64;
                Ok(Some((body, ObjectMeta { etag, size })))
            }
            Err(err) => {
                if err
                    .as_service_error()
                    .map(|e| e.is_no_such_key())
                    .unwrap_or(false)
                {
                    Ok(None)
                } else {
                    Err(anyhow!(err).context(format!("S3 GetObject {key}")))
                }
            }
        }
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<ObjectMeta> {
        let full = self.full_key(key);
        let output = self
            .block_on(
                self.client
                    .put_object()
                    .bucket(&self.bucket)
                    .key(&full)
                    .body(data.to_vec().into())
                    .send(),
            )
            .map_err(|e| anyhow!(e).context(format!("S3 PutObject {key}")))?;
        Ok(ObjectMeta {
            etag: Self::strip_etag(output.e_tag()),
            size: data.len() as u64,
        })
    }

    fn put_if_match(&self, key: &str, data: &[u8], expected_etag: Option<&str>) -> Result<ObjectMeta, CasError> {
        let full = self.full_key(key);
        let mut req = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&full)
            .body(data.to_vec().into());
        req = match expected_etag {
            Some(etag) => req.if_match(format!("\"{etag}\"")),
            None => req.if_none_match("*"),
        };
        match self.block_on(req.send()) {
            Ok(output) => Ok(ObjectMeta {
                etag: Self::strip_etag(output.e_tag()),
                size: data.len() as u64,
            }),
            Err(err) => {
                let is_precondition_failed = err
                    .raw_response()
                    .map(|r| r.status().as_u16() == 412)
                    .unwrap_or(false);
                if is_precondition_failed {
                    let current_etag = self.get(key).ok().flatten().map(|(_, m)| m.etag);
                    Err(CasError::Conflict { current_etag })
                } else {
                    Err(CasError::Other(anyhow!(err).context(format!("S3 PutObject(if-match) {key}"))))
                }
            }
        }
    }

    fn delete(&self, key: &str) -> Result<()> {
        let full = self.full_key(key);
        self.block_on(
            self.client
                .delete_object()
                .bucket(&self.bucket)
                .key(&full)
                .send(),
        )
        .map_err(|e| anyhow!(e).context(format!("S3 DeleteObject {key}")))?;
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let full_prefix = self.full_key(prefix);
        let mut keys = Vec::new();
        let mut continuation_token = None;
        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&full_prefix);
            if let Some(token) = continuation_token.take() {
                req = req.continuation_token(token);
            }
            let output = self
                .block_on(req.send())
                .map_err(|e| anyhow!(e).context(format!("S3 ListObjectsV2 {prefix}")))?;
            for obj in output.contents() {
                if let Some(k) = obj.key() {
                    let stripped = if self.prefix.is_empty() {
                        k.to_string()
                    } else {
                        k.strip_prefix(&format!("{}/", self.prefix.trim_end_matches('/')))
                            .unwrap_or(k)
                            .to_string()
                    };
                    keys.push(stripped);
                }
            }
            if output.is_truncated().unwrap_or(false) {
                continuation_token = output.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }
        Ok(keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsObjectStore::open(dir.path()).unwrap();
        store.put("foo/bar", b"hello").unwrap();
        let (data, meta) = store.get("foo/bar").unwrap().unwrap();
        assert_eq!(data, b"hello");
        assert_eq!(meta.size, 5);
        assert_eq!(meta.etag, sha256_hex(b"hello"));
    }

    #[test]
    fn get_missing_is_none() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsObjectStore::open(dir.path()).unwrap();
        assert!(store.get("nope").unwrap().is_none());
    }

    #[test]
    fn delete_missing_is_ok() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsObjectStore::open(dir.path()).unwrap();
        store.delete("nope").unwrap();
    }

    #[test]
    fn put_if_match_create_then_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsObjectStore::open(dir.path()).unwrap();

        // Create-if-absent succeeds the first time.
        let meta1 = store.put_if_match("k", b"v1", None).unwrap();

        // A second create-if-absent must fail: object now exists.
        let err = store.put_if_match("k", b"v2", None).unwrap_err();
        assert!(matches!(err, CasError::Conflict { .. }));

        // CAS with the correct etag succeeds.
        let meta2 = store.put_if_match("k", b"v2", Some(&meta1.etag)).unwrap();
        assert_eq!(store.get("k").unwrap().unwrap().0, b"v2");

        // CAS with a stale etag fails.
        let err = store.put_if_match("k", b"v3", Some(&meta1.etag)).unwrap_err();
        assert!(matches!(err, CasError::Conflict { .. }));
        assert_eq!(store.get("k").unwrap().unwrap().0, b"v2");
        let _ = meta2;
    }

    #[test]
    fn list_filters_by_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsObjectStore::open(dir.path()).unwrap();
        store.put("sst/orders/1", b"a").unwrap();
        store.put("sst/orders/2", b"b").unwrap();
        store.put("wal/orders/1", b"c").unwrap();

        let mut keys = store.list("sst/orders/").unwrap();
        keys.sort();
        assert_eq!(keys, vec!["sst/orders/1".to_string(), "sst/orders/2".to_string()]);
    }
}
