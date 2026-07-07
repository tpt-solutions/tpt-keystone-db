//! Reader autoscaling: a simple operator-driven loop, evaluated once per
//! reconcile tick, rather than a Kubernetes `HorizontalPodAutoscaler` +
//! custom-metrics-adapter integration. Scrapes each ready reader pod's own
//! `/metrics` endpoint (`tpt-keystone`'s Prometheus exporter, see
//! `tpt-keystone/src/metrics.rs`) directly over the pod IP — no metrics
//! pipeline needs to already exist in the cluster for this to work, at the
//! cost of only seeing point-in-time samples rather than a real time series.

use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, ListParams};
use kube::Client;
use tracing::warn;

use crate::types::AutoscalingSpec;

/// Fetches `tpt_connections_active` from every `Running` pod matching
/// `label_selector` in `namespace`, sums them, and returns the replica
/// count that would bring per-pod connections down to `target_connections_per_reader`
/// — clamped to `[min_replicas, max_replicas]`. Returns `None` if no ready
/// pods could be scraped at all (leaves the caller to keep the current
/// replica count rather than guessing).
pub async fn desired_replicas(
    client: &Client,
    namespace: &str,
    label_selector: &str,
    current_replicas: i32,
    autoscaling: &AutoscalingSpec,
) -> Option<i32> {
    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let list = match pods.list(&ListParams::default().labels(label_selector)).await {
        Ok(l) => l,
        Err(e) => {
            warn!(namespace, label_selector, error = %e, "autoscaler: failed to list reader pods");
            return None;
        }
    };

    let mut total_active: f64 = 0.0;
    let mut scraped = 0;
    for pod in &list.items {
        let Some(ip) = pod.status.as_ref().and_then(|s| s.pod_ip.as_deref()) else { continue };
        let is_running = pod.status.as_ref().and_then(|s| s.phase.as_deref()) == Some("Running");
        if !is_running {
            continue;
        }
        match scrape_connections_active(ip).await {
            Ok(v) => {
                total_active += v;
                scraped += 1;
            }
            Err(e) => warn!(pod = %pod.name_any_or_unknown(), error = %e, "autoscaler: metrics scrape failed"),
        }
    }

    if scraped == 0 {
        return None;
    }

    let target = autoscaling.target_connections_per_reader.max(1) as f64;
    let raw_desired = (total_active / target).ceil() as i32;
    let desired = raw_desired.clamp(autoscaling.min_replicas, autoscaling.max_replicas);

    // Avoid a needless "update" churn when nothing actually points to a
    // different replica count than what's already running.
    if desired == current_replicas {
        None
    } else {
        Some(desired)
    }
}

async fn scrape_connections_active(pod_ip: &str) -> anyhow::Result<f64> {
    let url = format!("http://{pod_ip}:{}/metrics", crate::resources::METRICS_PORT);
    let body = reqwest::get(&url).await?.error_for_status()?.text().await?;
    parse_gauge(&body, "tpt_connections_active").ok_or_else(|| anyhow::anyhow!("tpt_connections_active not found in {url} response"))
}

/// Pulls a single `<metric_name> <value>` line's value out of a Prometheus
/// text-exposition body — no need for a full parser, `tpt-keystone`'s
/// exporter always emits exactly one `# TYPE`/value pair per metric name.
fn parse_gauge(body: &str, metric_name: &str) -> Option<f64> {
    body.lines().find_map(|line| {
        let rest = line.strip_prefix(metric_name)?;
        let rest = rest.strip_prefix(' ')?;
        rest.trim().parse::<f64>().ok()
    })
}

trait NameOrUnknown {
    fn name_any_or_unknown(&self) -> String;
}

impl NameOrUnknown for Pod {
    fn name_any_or_unknown(&self) -> String {
        self.metadata.name.clone().unwrap_or_else(|| "<unknown>".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_gauge_finds_exact_metric() {
        let body = "\
# HELP tpt_connections_active x
# TYPE tpt_connections_active gauge
tpt_connections_active 7
# HELP tpt_connections_total x
tpt_connections_total 42
";
        assert_eq!(parse_gauge(body, "tpt_connections_active"), Some(7.0));
        assert_eq!(parse_gauge(body, "tpt_connections_total"), Some(42.0));
        assert_eq!(parse_gauge(body, "tpt_missing"), None);
    }
}
