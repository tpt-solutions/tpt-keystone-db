mod autoscale;
mod reconcile;
mod resources;
mod types;

use std::sync::Arc;

use futures::StreamExt;
use k8s_openapi::api::apps::v1::{Deployment, StatefulSet};
use k8s_openapi::api::core::v1::Service;
use kube::runtime::{watcher, Controller};
use kube::{Api, Client, CustomResourceExt};
use tracing::{error, info};

use reconcile::{error_policy, reconcile, Context};
use types::KeystoneCluster;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")))
        .init();

    // `tpt-operator --print-crd` emits the KeystoneCluster CustomResourceDefinition
    // as YAML on stdout, for `kubectl apply -f -` — see deploy/README.md for the
    // full bootstrap sequence (CRD, RBAC, then this binary as a Deployment).
    if std::env::args().nth(1).as_deref() == Some("--print-crd") {
        let crd = KeystoneCluster::crd();
        println!("{}", serde_yaml_no_dep::to_yaml(&crd)?);
        return Ok(());
    }

    let client = Client::try_default().await?;
    info!("tpt-operator connected to Kubernetes API, watching KeystoneCluster resources");

    let clusters: Api<KeystoneCluster> = Api::all(client.clone());
    let statefulsets: Api<StatefulSet> = Api::all(client.clone());
    let deployments: Api<Deployment> = Api::all(client.clone());
    let services: Api<Service> = Api::all(client.clone());

    Controller::new(clusters, watcher::Config::default())
        .owns(statefulsets, watcher::Config::default())
        .owns(deployments, watcher::Config::default())
        .owns(services, watcher::Config::default())
        .run(reconcile, error_policy, Arc::new(Context { client }))
        .for_each(|res| async move {
            match res {
                Ok(o) => info!(?o, "reconciled"),
                Err(e) => error!(error = %e, "reconcile error"),
            }
        })
        .await;

    Ok(())
}

/// `serde_yaml` is unmaintained upstream and this operator only needs the
/// one-shot `--print-crd` output path, so this is a minimal
/// JSON-to-YAML-ish passthrough rather than adding a full YAML dependency:
/// Kubernetes' API server and `kubectl` both accept JSON anywhere YAML is
/// accepted (JSON is a YAML subset), so emitting pretty JSON satisfies
/// `kubectl apply -f -` just as well as real YAML would.
mod serde_yaml_no_dep {
    pub fn to_yaml<T: serde::Serialize>(value: &T) -> anyhow::Result<String> {
        Ok(serde_json::to_string_pretty(value)?)
    }
}
