//! The `KeystoneCluster` CRD: one custom resource describes one TPT Keystone
//! cluster (one writer + N readers sharing one object-store bucket, per the
//! Phase 3 disaggregated-storage design in `tpt-keystone`). The operator's
//! entire job is reconciling this spec into the StatefulSet/Deployment/
//! Service/CronJob resources documented in `resources.rs`.

use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "tpt.dev",
    version = "v1alpha1",
    kind = "KeystoneCluster",
    plural = "keystoneclusters",
    shortname = "ksc",
    namespaced,
    status = "KeystoneClusterStatus",
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Writer","type":"string","jsonPath":".status.writerReady"}"#,
    printcolumn = r#"{"name":"Readers","type":"string","jsonPath":".status.readyReaders"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct KeystoneClusterSpec {
    /// Container image for both the writer and reader pods, e.g.
    /// `ghcr.io/example/tpt-keystone:0.3.0`. Changing this triggers the
    /// lease-aware rolling upgrade described in `reconcile.rs`.
    pub image: String,

    /// Number of reader (read-replica) pods. The writer is always exactly
    /// one pod — `tpt-keystone`'s write lease already enforces single-writer
    /// at the application level (see `tpt-keystone/src/storage/lease.rs`),
    /// so a second writer replica would just sit idle waiting for a lease
    /// that only fails over on the first writer's failure; the operator
    /// models that failover as a StatefulSet restart, not a second replica.
    #[serde(default = "default_reader_replicas")]
    pub reader_replicas: i32,

    /// Shared object-store backend every pod in the cluster points at.
    pub storage: StorageSpec,

    /// Resource requests/limits applied to both writer and reader pods.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirements>,

    /// Optional reader autoscaling; if omitted, `reader_replicas` above is
    /// authoritative and never adjusted by the operator.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub autoscaling: Option<AutoscalingSpec>,

    /// Optional periodic backup hook (see `resources::build_backup_cronjob`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup: Option<BackupSpec>,

    /// Extra environment variables merged into every pod (writer and
    /// reader), lowest priority — the operator's own `TPT_*` variables for
    /// role/storage/identity always win on key collision.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra_env: BTreeMap<String, String>,
}

fn default_reader_replicas() -> i32 {
    1
}

/// Deliberately a flat struct rather than a Rust enum with per-variant
/// fields: Kubernetes CRDs validate against a "structural schema", and
/// `kube-derive`'s schema generation flattens an internally-tagged enum's
/// variants into one shared `properties` object — which panics at CRD-print
/// time the moment two variants use the same discriminant field
/// (`backend`) with different fixed enum values, as `Local`/`S3` would
/// here. A flat struct with backend-specific fields left optional (and
/// validated against `backend` at reconcile time — see
/// `resources::storage_env_and_volumes`) sidesteps that entirely; it's the
/// standard workaround for this class of CRD, not specific to this field.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StorageSpec {
    pub backend: StorageBackend,

    /// Required when `backend: Local` — name of a pre-existing PVC with
    /// `ReadWriteMany` access mode, emulating one shared bucket. Intended
    /// for local/dev clusters only (most CSI drivers that support RWX are
    /// network filesystems, which is the whole point here, but this is not
    /// a production object-store substitute).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claim_name: Option<String>,

    /// Required when `backend: S3` — bucket name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// Name of a Secret containing `AWS_ACCESS_KEY_ID`/
    /// `AWS_SECRET_ACCESS_KEY` (or role-based auth env vars) to inject into
    /// every pod via `envFrom`. Omit to rely on pod-identity/IRSA instead
    /// of static credentials. Only meaningful when `backend: S3`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials_secret: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "PascalCase")]
pub enum StorageBackend {
    Local,
    S3,
}

impl StorageSpec {
    /// Checks the backend-specific fields `resources::storage_env_and_volumes`
    /// requires are actually present, so a malformed spec is rejected with a
    /// clear message (and a `Degraded` status — see `reconcile::reconcile`)
    /// instead of panicking partway through building Kubernetes resources.
    pub fn validate(&self) -> Result<(), String> {
        match self.backend {
            StorageBackend::Local if self.claim_name.is_none() => {
                Err("storage.backend is \"Local\" but storage.claimName is not set".to_string())
            }
            StorageBackend::S3 if self.bucket.is_none() => Err("storage.backend is \"S3\" but storage.bucket is not set".to_string()),
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResourceRequirements {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requests_cpu: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requests_memory: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limits_cpu: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limits_memory: Option<String>,
}

impl ResourceRequirements {
    pub fn to_k8s(&self) -> k8s_openapi::api::core::v1::ResourceRequirements {
        let mut requests = BTreeMap::new();
        let mut limits = BTreeMap::new();
        if let Some(v) = &self.requests_cpu {
            requests.insert("cpu".to_string(), Quantity(v.clone()));
        }
        if let Some(v) = &self.requests_memory {
            requests.insert("memory".to_string(), Quantity(v.clone()));
        }
        if let Some(v) = &self.limits_cpu {
            limits.insert("cpu".to_string(), Quantity(v.clone()));
        }
        if let Some(v) = &self.limits_memory {
            limits.insert("memory".to_string(), Quantity(v.clone()));
        }
        k8s_openapi::api::core::v1::ResourceRequirements {
            requests: (!requests.is_empty()).then_some(requests),
            limits: (!limits.is_empty()).then_some(limits),
            ..Default::default()
        }
    }
}

/// Reader-only autoscaling, driven by the operator scraping each reader
/// pod's own `/metrics` endpoint (`tpt_connections_active`) on every
/// reconcile tick — see `autoscale.rs`. This is a simple operator-driven
/// loop, not a Kubernetes `HorizontalPodAutoscaler`/custom-metrics-adapter
/// integration; it trades sophistication for not requiring a metrics
/// pipeline to already exist in the cluster.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AutoscalingSpec {
    pub min_replicas: i32,
    pub max_replicas: i32,
    /// Scale reader replicas up/down to keep each reader's active
    /// connection count near this target (simple proportional control,
    /// evaluated once per reconcile — see `autoscale::desired_replicas`).
    pub target_connections_per_reader: i32,
}

/// A periodic backup hook. `tpt-keystone`'s durable state already lives
/// entirely in the shared object store (Phase 3 design), so "backup" here
/// means "run some job that copies/snapshots that store elsewhere" — the
/// operator has no engine-specific backup logic of its own; it just runs
/// whatever `image`/`command` the cluster operator supplies, on schedule,
/// with the same storage env vars as the writer/reader pods so the job can
/// reach the same bucket.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackupSpec {
    /// Standard cron schedule, e.g. `"0 3 * * *"`.
    pub schedule: String,
    /// Image to run for the backup job. Defaults to the cluster's own
    /// `spec.image` if omitted (useful if the backup is itself a
    /// `tpt-keystone` subcommand or a wrapper script baked into that image).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    /// Command + args to run in the backup job's container. Required —
    /// the operator does not assume any default backup mechanism.
    pub command: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct KeystoneClusterStatus {
    /// High-level rollup: `Provisioning`, `Ready`, `RollingUpgrade`, or `Degraded`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub writer_ready: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready_readers: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub desired_readers: Option<i32>,
    /// Image currently running on the writer pod, tracked separately from
    /// `spec.image` so the operator (and `kubectl get`) can see when a
    /// rolling upgrade is still in flight.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_writer_image: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_reconcile_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn storage(backend: StorageBackend) -> StorageSpec {
        StorageSpec { backend, claim_name: None, bucket: None, region: None, endpoint: None, prefix: None, credentials_secret: None }
    }

    #[test]
    fn local_requires_claim_name() {
        assert!(storage(StorageBackend::Local).validate().is_err());
        assert!(StorageSpec { claim_name: Some("pvc".into()), ..storage(StorageBackend::Local) }.validate().is_ok());
    }

    #[test]
    fn s3_requires_bucket() {
        assert!(storage(StorageBackend::S3).validate().is_err());
        assert!(StorageSpec { bucket: Some("b".into()), ..storage(StorageBackend::S3) }.validate().is_ok());
    }
}
