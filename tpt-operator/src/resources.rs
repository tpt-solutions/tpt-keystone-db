//! Builders for the Kubernetes resources one `KeystoneCluster` reconciles
//! into: a single-replica writer `StatefulSet`, a reader `Deployment`, one
//! `Service` per role, and an optional backup `CronJob`. Kept as pure
//! functions (spec in, resource out) so `reconcile.rs` only has to decide
//! *when* to apply them, not *how* to build them.

use std::collections::BTreeMap;

use k8s_openapi::api::apps::v1::{Deployment, DeploymentSpec, StatefulSet, StatefulSetSpec, StatefulSetUpdateStrategy};
use k8s_openapi::api::batch::v1::{CronJob, CronJobSpec, JobSpec, JobTemplateSpec};
use k8s_openapi::api::core::v1::{
    Container, ContainerPort, EnvVar, EnvVarSource, ObjectFieldSelector, PersistentVolumeClaimVolumeSource, Pod, PodSpec, PodTemplateSpec,
    SecretEnvSource, Service, ServicePort, ServiceSpec, Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, OwnerReference};
use kube::api::ObjectMeta;
use kube::{Resource, ResourceExt};

use crate::types::{KeystoneCluster, StorageBackend, StorageSpec};

pub const PG_PORT: i32 = 5432;
pub const MCP_PORT: i32 = 5433;
pub const FLUX_WS_PORT: i32 = 5434;
pub const METRICS_PORT: i32 = 9187;

pub fn writer_name(cluster: &KeystoneCluster) -> String {
    format!("{}-writer", cluster.name_any())
}

pub fn reader_name(cluster: &KeystoneCluster) -> String {
    format!("{}-reader", cluster.name_any())
}

fn owner_reference(cluster: &KeystoneCluster) -> OwnerReference {
    cluster
        .controller_owner_ref(&())
        .expect("KeystoneCluster is namespaced and always has metadata.uid once persisted")
}

fn common_labels(cluster: &KeystoneCluster, role: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("app.kubernetes.io/name".to_string(), "tpt-keystone".to_string()),
        ("app.kubernetes.io/instance".to_string(), cluster.name_any()),
        ("app.kubernetes.io/managed-by".to_string(), "tpt-operator".to_string()),
        ("tpt.dev/role".to_string(), role.to_string()),
    ])
}

/// Storage-backend env vars + volumes/mounts shared by the writer and every
/// reader pod. Returns `(env_vars, volumes, volume_mounts)`.
///
/// Panics if `storage.backend` doesn't have the fields it requires (e.g.
/// `Local` without `claim_name`) — callers must run `StorageSpec::validate`
/// first (`reconcile::reconcile` does, before calling any builder in this
/// file) so this is unreachable in practice, not a substitute for that check.
fn storage_env_and_volumes(storage: &StorageSpec) -> (Vec<EnvVar>, Vec<Volume>, Vec<VolumeMount>) {
    let mut env = Vec::new();
    let mut volumes = Vec::new();
    let mut mounts = Vec::new();

    match storage.backend {
        StorageBackend::Local => {
            let claim_name = storage.claim_name.as_ref().expect("validated by StorageSpec::validate before this is called");
            env.push(env_var("TPT_STORAGE_BACKEND", "local"));
            env.push(env_var("TPT_LOCAL_STORE_DIR", "/data/store"));
            volumes.push(Volume {
                name: "store".to_string(),
                persistent_volume_claim: Some(PersistentVolumeClaimVolumeSource { claim_name: claim_name.clone(), ..Default::default() }),
                ..Default::default()
            });
            mounts.push(VolumeMount { name: "store".to_string(), mount_path: "/data/store".to_string(), ..Default::default() });
        }
        StorageBackend::S3 => {
            let bucket = storage.bucket.as_ref().expect("validated by StorageSpec::validate before this is called");
            env.push(env_var("TPT_STORAGE_BACKEND", "s3"));
            env.push(env_var("TPT_S3_BUCKET", bucket));
            if let Some(r) = &storage.region {
                env.push(env_var("TPT_S3_REGION", r));
            }
            if let Some(e) = &storage.endpoint {
                env.push(env_var("TPT_S3_ENDPOINT", e));
            }
            if let Some(p) = &storage.prefix {
                env.push(env_var("TPT_S3_PREFIX", p));
            }
        }
    }

    // Per-node local cache/working directory: deliberately `emptyDir`, not
    // the shared claim/bucket above — this is disposable compute-node
    // state (WAL-in-flight, NVMe cache, rebuilt local secondary indexes),
    // matching the "stateless compute node" design in
    // `tpt-keystone/src/main.rs`. Losing it on pod restart is expected and
    // safe; it's why the object store is the durability boundary, not this.
    volumes.push(Volume { name: "local".to_string(), empty_dir: Some(Default::default()), ..Default::default() });
    mounts.push(VolumeMount { name: "local".to_string(), mount_path: "/data/local".to_string(), ..Default::default() });
    env.push(env_var("TPT_LOCAL_DIR", "/data/local"));
    env.push(env_var("TPT_CACHE_DIR", "/data/local/cache"));

    (env, volumes, mounts)
}

fn env_var(name: &str, value: &str) -> EnvVar {
    EnvVar { name: name.to_string(), value: Some(value.to_string()), ..Default::default() }
}

fn pod_spec(cluster: &KeystoneCluster, role: &str) -> PodSpec {
    let (mut env, volumes, mounts) = storage_env_and_volumes(&cluster.spec.storage);

    env.push(env_var("TPT_NODE_ROLE", role));
    env.push(EnvVar {
        name: "TPT_NODE_ID".to_string(),
        value_from: Some(EnvVarSource {
            field_ref: Some(ObjectFieldSelector { field_path: "metadata.name".to_string(), ..Default::default() }),
            ..Default::default()
        }),
        ..Default::default()
    });
    for (k, v) in &cluster.spec.extra_env {
        // Operator-managed vars above always win — pushed first, and
        // `tpt-keystone`'s own env parsing takes whichever value the
        // container process sees, so duplicates here would only matter if
        // the container runtime preserved both; it doesn't, first/only
        // definition per name in the final `env` list is what's injected.
        if !env.iter().any(|e| &e.name == k) {
            env.push(env_var(k, v));
        }
    }

    let env_from = match (&cluster.spec.storage.backend, &cluster.spec.storage.credentials_secret) {
        (StorageBackend::S3, Some(secret)) => {
            vec![k8s_openapi::api::core::v1::EnvFromSource {
                secret_ref: Some(SecretEnvSource { name: secret.clone(), optional: Some(false) }),
                ..Default::default()
            }]
        }
        _ => vec![],
    };

    let resources = cluster.spec.resources.as_ref().map(|r| r.to_k8s());

    PodSpec {
        containers: vec![Container {
            name: "tpt-keystone".to_string(),
            image: Some(cluster.spec.image.clone()),
            env: Some(env),
            env_from: (!env_from.is_empty()).then_some(env_from),
            ports: Some(vec![
                ContainerPort { name: Some("pg".to_string()), container_port: PG_PORT, ..Default::default() },
                ContainerPort { name: Some("mcp".to_string()), container_port: MCP_PORT, ..Default::default() },
                ContainerPort { name: Some("flux-ws".to_string()), container_port: FLUX_WS_PORT, ..Default::default() },
                ContainerPort { name: Some("metrics".to_string()), container_port: METRICS_PORT, ..Default::default() },
            ]),
            volume_mounts: (!mounts.is_empty()).then_some(mounts),
            resources,
            ..Default::default()
        }],
        volumes: (!volumes.is_empty()).then_some(volumes),
        ..Default::default()
    }
}

/// The writer `StatefulSet` — always exactly one replica (see the module
/// doc on `KeystoneClusterSpec::reader_replicas` for why). Uses
/// `updateStrategy: OnDelete` rather than the default `RollingUpdate` so a
/// spec image bump does **not** immediately restart the writer pod; the
/// reconciler (`reconcile::maybe_upgrade_writer`) deletes it explicitly,
/// once readers are healthy, to control the timing of the one unavoidable
/// write-availability gap a single-writer restart causes.
pub fn build_writer_statefulset(cluster: &KeystoneCluster) -> StatefulSet {
    let name = writer_name(cluster);
    let labels = common_labels(cluster, "writer");
    StatefulSet {
        metadata: ObjectMeta {
            name: Some(name.clone()),
            namespace: cluster.namespace(),
            labels: Some(labels.clone()),
            owner_references: Some(vec![owner_reference(cluster)]),
            ..Default::default()
        },
        spec: Some(StatefulSetSpec {
            replicas: Some(1),
            service_name: Some(name),
            selector: LabelSelector { match_labels: Some(labels.clone()), ..Default::default() },
            update_strategy: Some(StatefulSetUpdateStrategy { type_: Some("OnDelete".to_string()), ..Default::default() }),
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta { labels: Some(labels), ..Default::default() }),
                spec: Some(pod_spec(cluster, "Writer")),
            },
            ..Default::default()
        }),
        status: None,
    }
}

/// The reader `Deployment`. Ordinary `RollingUpdate` is fine here — readers
/// carry no write lease and are individually disposable; losing one
/// mid-restart just means one fewer read replica until it comes back.
pub fn build_reader_deployment(cluster: &KeystoneCluster, replicas: i32) -> Deployment {
    let name = reader_name(cluster);
    let labels = common_labels(cluster, "reader");
    Deployment {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: cluster.namespace(),
            labels: Some(labels.clone()),
            owner_references: Some(vec![owner_reference(cluster)]),
            ..Default::default()
        },
        spec: Some(DeploymentSpec {
            replicas: Some(replicas),
            selector: LabelSelector { match_labels: Some(labels.clone()), ..Default::default() },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta { labels: Some(labels), ..Default::default() }),
                spec: Some(pod_spec(cluster, "Reader")),
            },
            ..Default::default()
        }),
        status: None,
    }
}

fn build_service(cluster: &KeystoneCluster, name: String, role: &str) -> Service {
    let labels = common_labels(cluster, role);
    Service {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: cluster.namespace(),
            labels: Some(labels.clone()),
            owner_references: Some(vec![owner_reference(cluster)]),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            selector: Some(labels),
            ports: Some(vec![
                ServicePort { name: Some("pg".to_string()), port: PG_PORT, ..Default::default() },
                ServicePort { name: Some("mcp".to_string()), port: MCP_PORT, ..Default::default() },
                ServicePort { name: Some("flux-ws".to_string()), port: FLUX_WS_PORT, ..Default::default() },
                ServicePort { name: Some("metrics".to_string()), port: METRICS_PORT, ..Default::default() },
            ]),
            ..Default::default()
        }),
        status: None,
    }
}

pub fn build_writer_service(cluster: &KeystoneCluster) -> Service {
    build_service(cluster, writer_name(cluster), "writer")
}

pub fn build_reader_service(cluster: &KeystoneCluster) -> Service {
    build_service(cluster, reader_name(cluster), "reader")
}

/// A `CronJob` that runs the user-supplied backup command on schedule, with
/// the same storage env vars as the writer/reader pods (minus the
/// node-role/node-id ones, which don't mean anything for a one-shot job).
/// See the `BackupSpec` doc comment for why this doesn't do anything
/// engine-specific.
pub fn build_backup_cronjob(cluster: &KeystoneCluster) -> Option<CronJob> {
    let backup = cluster.spec.backup.as_ref()?;
    let (env, volumes, mounts) = storage_env_and_volumes(&cluster.spec.storage);
    let image = backup.image.clone().unwrap_or_else(|| cluster.spec.image.clone());

    let env_from = match (&cluster.spec.storage.backend, &cluster.spec.storage.credentials_secret) {
        (StorageBackend::S3, Some(secret)) => {
            vec![k8s_openapi::api::core::v1::EnvFromSource {
                secret_ref: Some(SecretEnvSource { name: secret.clone(), optional: Some(false) }),
                ..Default::default()
            }]
        }
        _ => vec![],
    };

    Some(CronJob {
        metadata: ObjectMeta {
            name: Some(format!("{}-backup", cluster.name_any())),
            namespace: cluster.namespace(),
            labels: Some(common_labels(cluster, "backup")),
            owner_references: Some(vec![owner_reference(cluster)]),
            ..Default::default()
        },
        spec: CronJobSpec {
            schedule: backup.schedule.clone(),
            job_template: JobTemplateSpec {
                spec: Some(JobSpec {
                    template: PodTemplateSpec {
                        metadata: Some(ObjectMeta::default()),
                        spec: Some(PodSpec {
                            restart_policy: Some("OnFailure".to_string()),
                            containers: vec![Container {
                                name: "backup".to_string(),
                                image: Some(image),
                                command: Some(backup.command.clone()),
                                env: Some(env),
                                env_from: (!env_from.is_empty()).then_some(env_from),
                                volume_mounts: (!mounts.is_empty()).then_some(mounts),
                                ..Default::default()
                            }],
                            volumes: (!volumes.is_empty()).then_some(volumes),
                            ..Default::default()
                        }),
                    },
                    ..Default::default()
                }),
                ..Default::default()
            },
            ..Default::default()
        },
        status: None,
    })
}

/// True if `pod`'s only container is running the given image — used to
/// decide whether the writer pod still needs a lease-aware restart after a
/// `spec.image` change (see `reconcile::maybe_upgrade_writer`).
pub fn pod_runs_image(pod: &Pod, image: &str) -> bool {
    pod.spec.as_ref().and_then(|s| s.containers.first()).and_then(|c| c.image.as_deref()) == Some(image)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{KeystoneCluster, KeystoneClusterSpec, StorageBackend, StorageSpec};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    fn cluster(backend: StorageBackend, backup: Option<crate::types::BackupSpec>) -> KeystoneCluster {
        let storage = match backend {
            StorageBackend::Local => StorageSpec {
                backend,
                claim_name: Some("shared-pvc".into()),
                ..Default::default()
            },
            StorageBackend::S3 => StorageSpec {
                backend,
                bucket: Some("my-bucket".into()),
                region: Some("us-east-1".into()),
                ..Default::default()
            },
        };
        KeystoneCluster {
            metadata: ObjectMeta {
                name: Some("demo".into()),
                namespace: Some("tpt-system".into()),
                ..Default::default()
            },
            spec: KeystoneClusterSpec {
                image: "ghcr.io/example/tpt-keystone:0.3.0".into(),
                reader_replicas: 3,
                storage,
                backup,
                ..Default::default()
            },
            status: None,
        }
    }

    #[test]
    fn resource_names_follow_role_convention() {
        let c = cluster(StorageBackend::S3, None);
        assert_eq!(writer_name(&c), "demo-writer");
        assert_eq!(reader_name(&c), "demo-reader");
    }

    #[test]
    fn writer_is_single_replica_statefulset_on_delete() {
        let c = cluster(StorageBackend::S3, None);
        let ss = build_writer_statefulset(&c);
        assert_eq!(ss.spec.as_ref().unwrap().replicas, Some(1));
        assert_eq!(
            ss.spec.as_ref().unwrap().update_strategy.as_ref().unwrap().type_.as_deref(),
            Some("OnDelete")
        );
        assert_eq!(ss.metadata.name.as_deref(), Some("demo-writer"));
    }

    #[test]
    fn reader_deployment_honors_desired_replicas() {
        let c = cluster(StorageBackend::S3, None);
        let dep = build_reader_deployment(&c, c.spec.reader_replicas);
        assert_eq!(dep.spec.as_ref().unwrap().replicas, Some(3));
    }

    #[test]
    fn services_expose_all_listener_ports() {
        let c = cluster(StorageBackend::S3, None);
        for svc in [build_writer_service(&c), build_reader_service(&c)] {
            let ports = svc.spec.as_ref().unwrap().ports.as_ref().unwrap();
            let port_numbers: Vec<i32> = ports.iter().map(|p| p.port).collect();
            assert_eq!(port_numbers, vec![PG_PORT, MCP_PORT, FLUX_WS_PORT, METRICS_PORT]);
        }
    }

    #[test]
    fn s3_storage_injects_bucket_env_and_no_pvc() {
        let c = cluster(StorageBackend::S3, None);
        let spec = build_writer_statefulset(&c).spec.unwrap();
        let env: Vec<String> = spec
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers[0]
            .env
            .as_ref()
            .unwrap()
            .iter()
            .map(|e| e.name.clone())
            .collect();
        assert!(env.contains(&"TPT_STORAGE_BACKEND".to_string()));
        assert!(env.contains(&"TPT_S3_BUCKET".to_string()));
        assert!(env.contains(&"TPT_S3_REGION".to_string()));
        // S3 backend must not mount a local PVC claim.
        assert!(spec.template.spec.as_ref().unwrap().volumes.is_none());
    }

    #[test]
    fn local_storage_mounts_claim_and_skips_s3_env() {
        let c = cluster(StorageBackend::Local, None);
        let spec = build_writer_statefulset(&c).spec.unwrap();
        let volumes = spec.template.spec.as_ref().unwrap().volumes.as_ref().unwrap();
        assert!(volumes.iter().any(|v| v.persistent_volume_claim.is_some()));
        let env: Vec<String> = spec
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers[0]
            .env
            .as_ref()
            .unwrap()
            .iter()
            .map(|e| e.name.clone())
            .collect();
        assert!(!env.contains(&"TPT_S3_BUCKET".to_string()));
    }

    #[test]
    fn backup_cronjob_is_built_when_specified() {
        let c = cluster(
            StorageBackend::S3,
            Some(crate::types::BackupSpec {
                schedule: "0 3 * * *".into(),
                image: None,
                command: vec!["/bin/backup".into(), "--all".into()],
            }),
        );
        let cj = build_backup_cronjob(&c).expect("backup cronjob should be built");
        assert_eq!(cj.spec.unwrap().schedule, "0 3 * * *");
        assert_eq!(cj.metadata.name.as_deref(), Some("demo-backup"));
        let container = &cj.spec.unwrap().job_template.spec.unwrap().template.spec.unwrap().containers[0];
        assert_eq!(container.command.as_ref().unwrap(), &vec!["/bin/backup".into(), "--all".into()]);
    }

    #[test]
    fn backup_cronjob_absent_without_spec() {
        let c = cluster(StorageBackend::S3, None);
        assert!(build_backup_cronjob(&c).is_none());
    }

    #[test]
    fn pod_runs_image_compares_only_first_container() {
        let pod = Pod {
            spec: Some(k8s_openapi::api::core::v1::PodSpec {
                containers: vec![k8s_openapi::api::core::v1::Container {
                    image: Some("img:1".into()),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(pod_runs_image(&pod, "img:1"));
        assert!(!pod_runs_image(&pod, "img:2"));
    }

    #[test]
    fn common_labels_are_namespaced_by_role() {
        let c = cluster(StorageBackend::S3, None);
        let writer = common_labels(&c, "writer");
        let reader = common_labels(&c, "reader");
        assert_eq!(writer.get("tpt.dev/role").map(String::as_str), Some("writer"));
        assert_eq!(reader.get("tpt.dev/role").map(String::as_str), Some("reader"));
        assert_eq!(writer.get("app.kubernetes.io/instance").map(String::as_str), Some("demo"));
    }
}
