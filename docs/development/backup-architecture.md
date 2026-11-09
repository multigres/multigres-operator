# Backup Architecture (Developer Internals)

For user-facing backup documentation, see [docs/backup-restore.md](../backup-restore.md).

## Overview

Backup configuration is defined at the `MultigresCluster` level and propagates down to Shards via a text-merge strategy. The operator supports two backends: **S3** (object storage) and **Filesystem** (PVC-based).

## Filesystem repository

pgBackRest needs one repository per shard. For filesystem backups, the operator creates one PVC named `backup-data-{cluster}-{db}-{tg}-{shard}` (`backup-data-{cluster}-{db}-{tg}-{tgid}-{shard}` for a shard of a dynamic tablegroup, so a tablegroup recreated under a dropped one's name never reuses its repository) and mounts it at `/backups` in every pooler pod.

That PVC must be RWX when more than one pod needs it, including when a shard spans cells. A cell-local volume gives different poolers different repositories and cannot be used for a multi-cell shard. Use S3 if a shared filesystem is not available.

## Replica Selection Logic (Upstream Behavior)
MultiAdmin prefers a healthy replica when taking a backup. That replica may be in any cell, so every pooler cell needs access to the repository.
