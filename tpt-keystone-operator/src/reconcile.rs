//! The reconcile loop: given the latest observed `KeystoneCluster`, drive
//! the cluster's real Kubernetes resources toward what the spec describes.
//! Every apply is a server-side apply (SSA) under the `tpt-keystone-operator` field
//! manager, so re-applying an unchanged resource is a no-op rather than a
//! spurious update — this function runs on every watch event and every
//! periodic resync, so it has to be safe to call constantly.

use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, DeleteParams, Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::{Client, ResourceExt};
use tracing::{info, warn};

use crate::types::{KeystoneCluster, KeystoneClusterStatus};
use crate::{autoscale, resources};

pub struct Context {
    pub client: Client,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("kube API error: {0}")]
    Kube(#[from] kube::Error),
    #[error("cluster resource has no namespace")]
    NoNamespace,
}

const FIELD_MANAGER: &str = "tpt-keystone-operator";

/// Requeue interval used both as the periodic resync (so autoscaling and
/// the writer-upgrade check re-evaluate even with no watch events) and as
/// the backoff after a successful reconcile.
const RESYNC: Duration = Duration::from_secs(30);

pub async fn reconcile(cluster: Arc<KeystoneCluster>, ctx: Arc<Context>) -> Result<Action, Error> {
    let ns = cluster.namespace().ok_or(Error::NoNamespace)?;
    let client = ctx.client.clone();
    info!(cluster = %cluster.name_any(), namespace = %ns, "reconciling");

    if let Err(reason) = cluster.spec.storage.validate() {
        warn!(cluster = %cluster.name_any(), %reason, "invalid KeystoneCluster spec, skipping reconcile until fixed");
        write_invalid_status(&client, &cluster, &ns, &reason).await?;
        return Ok(Action::requeue(RESYNC));
    }

    apply(&client, &ns, &resources::build_writer_service(&cluster)).await?;
    apply(&client, &ns, &resources::build_reader_service(&cluster)).await?;
    apply(&client, &ns, &resources::build_writer_statefulset(&cluster)).await?;

    let desired_readers = resolve_desired_readers(&client, &cluster, &ns).await;
    apply(&client, &ns, &resources::build_reader_deployment(&cluster, desired_readers)).await?;

    if let Some(cronjob) = resources::build_backup_cronjob(&cluster) {
        apply(&client, &ns, &cronjob).await?;
    }

    maybe_upgrade_writer(&client, &cluster, &ns, desired_readers).await?;

    update_status(&client, &cluster, &ns, desired_readers).await?;

    Ok(Action::requeue(RESYNC))
}

pub fn error_policy(cluster: Arc<KeystoneCluster>, err: &Error, _ctx: Arc<Context>) -> Action {
    warn!(cluster = %cluster.name_any(), error = %err, "reconcile failed, retrying");
    Action::requeue(Duration::from_secs(10))
}

async fn apply<K>(client: &Client, ns: &str, resource: &K) -> Result<(), Error>
where
    K: kube::Resource<Scope = kube::core::NamespaceResourceScope> + serde::Serialize + serde::de::DeserializeOwned + Clone + std::fmt::Debug,
    K::DynamicType: Default,
{
    let api: Api<K> = Api::namespaced(client.clone(), ns);
    let name = resource.meta().name.clone().expect("resource builders always set metadata.name");
    let pp = PatchParams::apply(FIELD_MANAGER).force();
    api.patch(&name, &pp, &Patch::Apply(resource)).await?;
    Ok(())
}

/// If autoscaling is configured, scrape reader pods and pick a new replica
/// count; otherwise (or if the scrape yields nothing usable) fall back to
/// `spec.reader_replicas` unchanged. Deliberately reads the *existing*
/// Deployment's replica count first so a scrape failure holds steady at
/// whatever's currently running instead of silently reverting to the
/// spec's static count out from under an active autoscaling decision.
async fn resolve_desired_readers(client: &Client, cluster: &KeystoneCluster, ns: &str) -> i32 {
    let Some(autoscaling) = &cluster.spec.autoscaling else {
        return cluster.spec.reader_replicas;
    };

    let deployments: Api<Deployment> = Api::namespaced(client.clone(), ns);
    let current = deployments
        .get_opt(&resources::reader_name(cluster))
        .await
        .ok()
        .flatten()
        .and_then(|d| d.spec.and_then(|s| s.replicas))
        .unwrap_or(autoscaling.min_replicas);

    let selector = format!(
        "app.kubernetes.io/instance={},tpt.dev/role=reader",
        cluster.name_any()
    );
    autoscale::desired_replicas(client, ns, &selector, current, autoscaling).await.unwrap_or(current)
}

/// A spec image bump doesn't touch the writer `StatefulSet`'s running pod
/// on its own (`updateStrategy: OnDelete` — see `resources::build_writer_statefulset`).
/// This function is what actually completes the upgrade: once the
/// StatefulSet's pod template has the new image (done by the `apply` call
/// in `reconcile` above) *and* reader replicas look healthy enough to keep
/// serving reads while the writer is briefly gone, delete the writer pod so
/// the StatefulSet controller recreates it from the now-updated template.
///
/// "Healthy enough" here is intentionally simple: at least one ready reader
/// if any readers are desired at all. A cluster configured with zero
/// readers has no read-availability to protect and upgrades immediately.
async fn maybe_upgrade_writer(client: &Client, cluster: &KeystoneCluster, ns: &str, desired_readers: i32) -> Result<(), Error> {
    let pods: Api<Pod> = Api::namespaced(client.clone(), ns);
    let writer_pod_name = format!("{}-0", resources::writer_name(cluster));
    let Some(pod) = pods.get_opt(&writer_pod_name).await? else {
        return Ok(()); // StatefulSet hasn't created the pod yet — nothing to upgrade.
    };

    if resources::pod_runs_image(&pod, &cluster.spec.image) {
        return Ok(()); // already on the desired image
    }

    if desired_readers > 0 {
        let ready = count_ready_readers(client, cluster, ns).await;
        if ready == 0 {
            info!(
                cluster = %cluster.name_any(),
                "writer image differs from spec but deferring restart — no ready readers to absorb read traffic during the restart"
            );
            return Ok(());
        }
    }

    info!(cluster = %cluster.name_any(), pod = %writer_pod_name, "deleting writer pod to complete rolling upgrade");
    pods.delete(&writer_pod_name, &DeleteParams::default()).await?;
    Ok(())
}

async fn count_ready_readers(client: &Client, cluster: &KeystoneCluster, ns: &str) -> i32 {
    let pods: Api<Pod> = Api::namespaced(client.clone(), ns);
    let selector = format!("app.kubernetes.io/instance={},tpt.dev/role=reader", cluster.name_any());
    let list = match pods.list(&kube::api::ListParams::default().labels(&selector)).await {
        Ok(l) => l,
        Err(_) => return 0,
    };
    list.items
        .iter()
        .filter(|p| {
            p.status
                .as_ref()
                .and_then(|s| s.conditions.as_ref())
                .map(|conds| conds.iter().any(|c| c.type_ == "Ready" && c.status == "True"))
                .unwrap_or(false)
        })
        .count() as i32
}

async fn update_status(client: &Client, cluster: &KeystoneCluster, ns: &str, desired_readers: i32) -> Result<(), Error> {
    let pods: Api<Pod> = Api::namespaced(client.clone(), ns);
    let writer_pod = pods.get_opt(&format!("{}-0", resources::writer_name(cluster))).await?;
    let writer_ready = writer_pod
        .as_ref()
        .and_then(|p| p.status.as_ref())
        .and_then(|s| s.conditions.as_ref())
        .map(|conds| conds.iter().any(|c| c.type_ == "Ready" && c.status == "True"))
        .unwrap_or(false);
    let current_writer_image = writer_pod.as_ref().and_then(|p| p.spec.as_ref()).and_then(|s| s.containers.first()).and_then(|c| c.image.clone());

    let ready_readers = count_ready_readers(client, cluster, ns).await;

    let phase = if !writer_ready {
        "Provisioning"
    } else if current_writer_image.as_deref() != Some(cluster.spec.image.as_str()) {
        "RollingUpgrade"
    } else if ready_readers < desired_readers {
        "Degraded"
    } else {
        "Ready"
    };

    let status = KeystoneClusterStatus {
        phase: Some(phase.to_string()),
        writer_ready: Some(writer_ready),
        ready_readers: Some(ready_readers),
        desired_readers: Some(desired_readers),
        current_writer_image,
        last_reconcile_time: Some(chrono::Utc::now().to_rfc3339()),
        message: None,
    };

    let api: Api<KeystoneCluster> = Api::namespaced(client.clone(), ns);
    let patch = serde_json::json!({ "status": status });
    api.patch_status(&cluster.name_any(), &PatchParams::apply(FIELD_MANAGER).force(), &Patch::Merge(&patch)).await?;
    Ok(())
}

/// Writes an `Invalid` status without touching any owned resource — used
/// when `spec.storage.validate()` fails, so the spec error is visible via
/// `kubectl get keystonecluster` instead of only in operator logs.
async fn write_invalid_status(client: &Client, cluster: &KeystoneCluster, ns: &str, reason: &str) -> Result<(), Error> {
    let status = KeystoneClusterStatus {
        phase: Some("Invalid".to_string()),
        message: Some(reason.to_string()),
        last_reconcile_time: Some(chrono::Utc::now().to_rfc3339()),
        ..Default::default()
    };
    let api: Api<KeystoneCluster> = Api::namespaced(client.clone(), ns);
    let patch = serde_json::json!({ "status": status });
    api.patch_status(&cluster.name_any(), &PatchParams::apply(FIELD_MANAGER).force(), &Patch::Merge(&patch)).await?;
    Ok(())
}
