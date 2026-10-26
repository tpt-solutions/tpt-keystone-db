# Changelog

All notable changes to `tpt-keystone-operator` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the crate uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This crate is at `0.1.0` and has not been published to crates.io yet, so all changes are listed under
`[Unreleased]`. Changes that span the whole repository are also recorded in the root
[`CHANGELOG.md`](../CHANGELOG.md).

## [Unreleased]

### Added

- `KeystoneCluster` custom resource and its CRD. `tpt-keystone-operator --print-crd` prints the CRD for
  `kubectl apply`.
- Reconciliation of a `KeystoneCluster` into a writer `StatefulSet` (`OnDelete` updates), a reader
  `Deployment`, two `Service`s, and an optional backup `CronJob`. Reconciliation runs on watch events and
  on a 30-second resync.
- Rolling upgrades that wait until at least one reader is ready before restarting the writer.
- Reader autoscaling. It reads `tpt_connections_active` from each reader's `/metrics` and adjusts the
  replica count within `minReplicas` and `maxReplicas`.
- Storage backends: S3 (bucket, region, endpoint, prefix, and optional credentials secret) and local
  emulation on a shared `ReadWriteMany` claim.
- `Dockerfile` and `deploy/` manifests: namespace, RBAC, operator deployment, and a sample cluster.
- Per-crate `README.md`, `CHANGELOG.md`, and `LICENSE-MIT`/`LICENSE-APACHE` files, and crates.io
  metadata (`repository`, `readme`, `keywords`, `categories`, `rust-version`).

### Changed

- Renamed from `tpt-operator` to `tpt-keystone-operator`. The binary is `tpt-keystone-operator`.
- Licensed under `MIT OR Apache-2.0`.

### Known limits

- No finalizer or pre-delete hook. Deleting a `KeystoneCluster` relies on Kubernetes garbage collection.
- Removing `spec.backup` does not delete the `CronJob` it created.
- The upgrade gate checks reader readiness, not replication lag.
- Invalid combinations, such as `backend: S3` with no bucket, are rejected at reconcile time, not at apply
  time.
