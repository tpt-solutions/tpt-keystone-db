# tpt-operator

A Kubernetes operator for **TPT Keystone** clusters, built with
[`kube-rs`](https://kube.rs/). One `KeystoneCluster` custom resource
describes one Keystone cluster (a single writer + N read replicas sharing
one object-store bucket, per the Phase 3 disaggregated-storage design in
`tpt-keystone`); the operator reconciles it into real Kubernetes resources
and keeps them converged.

This is a separate crate/binary/container image from `tpt-keystone` itself
— it's cluster-lifecycle tooling, not part of the database engine. It has
its own `Cargo.toml` and is not part of a Cargo workspace with
`tpt-keystone`, so building/testing one never requires touching the other.

## What it manages, per `KeystoneCluster`

| Resource | Purpose |
|---|---|
| `StatefulSet` (`<name>-writer`, 1 replica) | The write node. `updateStrategy: OnDelete` — the operator, not Kubernetes' default rolling-update, decides exactly when to restart it (see "Rolling upgrades" below). |
| `Deployment` (`<name>-reader`) | Read replicas. Ordinary rolling updates — readers are individually disposable. |
| `Service` × 2 (`<name>-writer`, `<name>-reader`) | ClusterIP services exposing the Postgres wire port (5432), MCP (5433), Flux WebSocket (5434), and the Prometheus `/metrics` port (9187) for each role. |
| `CronJob` (`<name>-backup`, optional) | Runs a user-supplied backup command on schedule — see "Backup" below. |

Reconciliation runs on every watch event for the `KeystoneCluster` and its
owned resources, plus a 30-second periodic resync (`reconcile::RESYNC`) so
autoscaling and the writer-upgrade check keep re-evaluating even with no new
events.

## Rolling upgrades

Because `tpt-keystone` enforces single-writer at the application level (a
CAS-based lease with a fencing token — see
`tpt-keystone/src/storage/lease.rs`), the writer `StatefulSet` is always
exactly one replica; there's no such thing as a rolling update *across
writer replicas*. Instead, a `spec.image` change:

1. Updates the `StatefulSet`'s pod template immediately (via the same
   server-side apply as everything else).
2. Because `updateStrategy: OnDelete`, Kubernetes does **not** restart the
   existing writer pod on its own.
3. On the next reconcile, `reconcile::maybe_upgrade_writer` checks whether
   at least one reader is `Ready` (skipped entirely if `readerReplicas`/the
   autoscaler's floor is `0` — nothing to protect). Once satisfied, it
   deletes the writer pod, and the `StatefulSet` controller recreates it
   from the now-updated template.

This bounds (but does not eliminate) the write-availability gap during an
upgrade to "however long the new writer pod takes to start and re-acquire
the lease" — reads keep flowing from readers throughout.

## Reader autoscaling

If `spec.autoscaling` is set, the operator scrapes `tpt_connections_active`
from every `Ready` reader pod's own `/metrics` endpoint directly (pod IP,
no cluster-wide metrics pipeline required — see `autoscale.rs`), sums it,
and picks a new replica count to keep each reader near
`targetConnectionsPerReader`, clamped to `[minReplicas, maxReplicas]`. This
is intentionally a simple proportional controller evaluated once per
reconcile tick, not a Kubernetes `HorizontalPodAutoscaler` integration — no
metrics-server/custom-metrics-adapter needs to already exist in the
cluster. If `spec.autoscaling` is omitted, `spec.readerReplicas` is
authoritative and never adjusted automatically.

## Backup

`tpt-keystone`'s durable state already lives entirely in the shared object
store (that's the Phase 3 design). "Backup" here just means "run some job,
on schedule, with the cluster's own storage env vars and credentials" — the
operator has **no** engine-specific backup logic; `spec.backup.command` is
whatever you supply (e.g. `aws s3 sync` to a second bucket, a `restic`
invocation, etc.). See `deploy/sample-cluster.yaml` for the shape.

## Storage backends

`spec.storage.backend` is `Local` or `S3` (a flat struct with
backend-specific optional fields — see the doc comment on
`types::StorageSpec` for why this isn't a Rust enum with per-variant
fields: `kube-derive`'s CRD schema generation panics on that shape today).

- **`S3`**: sets `TPT_S3_BUCKET`/`TPT_S3_REGION`/`TPT_S3_ENDPOINT`/`TPT_S3_PREFIX`
  from `spec.storage`; optionally injects `spec.storage.credentialsSecret`
  via `envFrom` for static credentials, or omit it to rely on pod-identity/IRSA.
- **`Local`**: mounts a pre-existing `ReadWriteMany` `PersistentVolumeClaim`
  (`spec.storage.claimName`) at `/data/store`, shared by every pod in the
  cluster to emulate one bucket. Dev/test only — most RWX-capable CSI
  drivers are themselves network filesystems, which is the point, but this
  is not a substitute for a real object store in production.

Every pod also gets an `emptyDir` at `/data/local` (`TPT_LOCAL_DIR`) — this
is the per-node disposable cache/working directory (NVMe cache, WAL in
flight, rebuilt local secondary indexes), intentionally **not** part of the
shared claim/bucket, matching `tpt-keystone`'s "stateless compute node"
design. Losing it on pod restart is expected and safe.

## Building and deploying

```sh
# Build + push both images (replace with your registry):
docker build -t ghcr.io/example/tpt-keystone:0.1.0 -f ../tpt-keystone/Dockerfile ../tpt-keystone
docker build -t ghcr.io/example/tpt-operator:0.1.0 -f Dockerfile .
docker push ghcr.io/example/tpt-keystone:0.1.0
docker push ghcr.io/example/tpt-operator:0.1.0

# Generate and apply the CRD:
cargo run -- --print-crd > deploy/crd.json
kubectl apply -f deploy/crd.json

# Bootstrap the operator itself (edit deploy/operator.yaml's image first):
kubectl apply -f deploy/namespace.yaml
kubectl apply -f deploy/rbac.yaml
kubectl apply -f deploy/operator.yaml

# Deploy a cluster (edit deploy/sample-cluster.yaml's image/bucket first):
kubectl apply -f deploy/sample-cluster.yaml
kubectl get keystoneclusters
```

`cargo run -- --print-crd` emits the CRD as pretty-printed JSON rather than
YAML (see the comment on `serde_yaml_no_dep` in `main.rs` for why) —
`kubectl apply -f` accepts either, since JSON is a YAML subset.

## Known limitations

- No `KeystoneCluster` deletion/finalizer logic beyond Kubernetes' own
  owner-reference garbage collection — deleting the custom resource cascades
  to everything it owns (StatefulSet/Deployment/Services/CronJob) via GC,
  but there's no pre-delete hook (e.g. "refuse to delete while the writer
  still holds an active lease" or "run a final backup on delete").
- If `spec.backup` is removed after having been set, the operator does not
  delete the previously-created `CronJob` — it simply stops updating it.
- The writer-upgrade "healthy enough" gate (`maybe_upgrade_writer`) only
  checks *reader* readiness, not application-level lag (e.g. how far behind
  the manifest a reader's local state is) — there's no such metric exposed
  today for the operator to check.
- No admission webhook / `CustomResourceValidation` beyond what the
  generated OpenAPI schema itself expresses — invalid combinations (e.g.
  `backend: S3` with no `bucket`) are only caught at reconcile time
  (`StorageSpec::validate`, surfaced as `status.phase: Invalid` with a
  message), not rejected at `kubectl apply` time.
