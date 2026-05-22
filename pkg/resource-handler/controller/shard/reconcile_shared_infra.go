package shard

import (
	"context"
	"crypto/x509"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/cert"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// reconcilePgHbaConfigMap creates or updates the pg_hba ConfigMap for a shard.
// This ConfigMap is shared across all pools and contains the authentication template.
func (r *ShardReconciler) reconcilePgHbaConfigMap(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) error {
	desired, err := BuildPgHbaConfigMap(shard, r.Scheme)
	if err != nil {
		return fmt.Errorf("failed to build pg_hba ConfigMap: %w", err)
	}

	// Server Side Apply
	desired.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("ConfigMap"))
	if err := r.Patch(
		ctx,
		desired,
		client.Apply,
		client.ForceOwnership,
		client.FieldOwner("multigres-operator"),
	); err != nil {
		return fmt.Errorf("failed to apply pg_hba ConfigMap: %w", err)
	}

	return nil
}

// reconcilePostgresPasswordSecret ensures the postgres-password Secret used by
// every component is available.
//
// Two modes:
//
//  1. If shard.Spec.PostgresPasswordSecretRef is set, the user owns the Secret.
//     The operator does not create one — it only validates the referenced Secret
//     exists and contains the named key. This is the recommended production
//     mode: it lets the password be generated and rotated outside the operator
//     (e.g. via the customer-provisioning worker, sops, vault, etc.) and mounted
//     into pgctld at initdb time so pg_authid is seeded with the final SCRAM
//     hash, eliminating the need for a post-initdb ALTER USER pass.
//
//  2. Otherwise, the operator creates a per-shard managed Secret with a
//     placeholder default value (v1alpha1 only).
func (r *ShardReconciler) reconcilePostgresPasswordSecret(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) error {
	if ref := shard.Spec.PostgresPasswordSecretRef; ref != nil && ref.Name != "" {
		key := ref.Key
		if key == "" {
			key = PostgresPasswordSecretKey
		}
		secret := &corev1.Secret{}
		// Use APIReader (uncached) — the operator's informer cache filters by
		// managed-by label, so externally-owned Secrets are invisible to r.Get.
		if err := r.APIReader.Get(ctx, types.NamespacedName{
			Name:      ref.Name,
			Namespace: shard.Namespace,
		}, secret); err != nil {
			return fmt.Errorf(
				"postgres password secret %q not found (referenced by shard.spec.postgresPasswordSecretRef): %w",
				ref.Name,
				err,
			)
		}
		if _, ok := secret.Data[key]; !ok {
			return fmt.Errorf(
				"postgres password secret %q missing key %q",
				ref.Name, key,
			)
		}
		return nil
	}

	desired, err := BuildPostgresPasswordSecret(shard, r.Scheme)
	if err != nil {
		return fmt.Errorf("failed to build postgres password Secret: %w", err)
	}

	// Server Side Apply
	desired.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Secret"))
	if err := r.Patch(
		ctx,
		desired,
		client.Apply,
		client.ForceOwnership,
		client.FieldOwner("multigres-operator"),
	); err != nil {
		return fmt.Errorf("failed to apply postgres password Secret: %w", err)
	}

	return nil
}

// reconcilePgBackRestCerts ensures pgBackRest TLS certificates are available.
// For user-provided certs, validates the Secret exists and has the required keys
// using an uncached API reader (the informer cache filters by managed-by label).
// For auto-generated certs, uses pkg/cert to create and rotate CA + server Secrets.
func (r *ShardReconciler) reconcilePgBackRestCerts(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) error {
	if shard.Spec.Backup == nil {
		return nil
	}

	// User-provided Secret: validate via uncached API reader.
	// We use APIReader instead of the cached client because the informer cache
	// only stores operator-labeled Secrets, making external Secrets (e.g.,
	// cert-manager) invisible to the cached r.Get().
	if shard.Spec.Backup.PgBackRestTLS != nil &&
		shard.Spec.Backup.PgBackRestTLS.SecretName != "" {
		secretName := shard.Spec.Backup.PgBackRestTLS.SecretName
		secret := &corev1.Secret{}
		if err := r.APIReader.Get(ctx, types.NamespacedName{
			Name:      secretName,
			Namespace: shard.Namespace,
		}, secret); err != nil {
			return fmt.Errorf("pgbackrest TLS secret %q not found: %w", secretName, err)
		}
		for _, key := range []string{"ca.crt", "tls.crt", "tls.key"} {
			if _, ok := secret.Data[key]; !ok {
				return fmt.Errorf(
					"pgbackrest TLS secret %q missing required key %q",
					secretName,
					key,
				)
			}
		}
		return nil
	}

	// Auto-generate: use pkg/cert to create CA + server cert Secrets.
	clusterName := shard.Labels[metadata.LabelMultigresCluster]
	rotator := cert.NewManager(r.Client, r.Recorder, cert.Options{
		Namespace:        shard.Namespace,
		CASecretName:     shard.Name + "-pgbackrest-ca",
		ServerSecretName: shard.Name + "-pgbackrest-tls",
		ServiceName:      "pgbackrest",
		ExtKeyUsages: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth,
		},
		Organization:  "Multigres",
		Owner:         shard,
		ComponentName: "pgbackrest",
		Labels:        metadata.BuildStandardLabels(clusterName, "pgbackrest-tls"),
	})
	return rotator.Bootstrap(ctx)
}

// reconcileSharedBackupPVC creates or updates the shared backup PVC for a specific cell.
func (r *ShardReconciler) reconcileSharedBackupPVC(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	cellName string,
) error {
	// S3 backups use object storage; no shared PVC is needed.
	// TODO: Consider cleaning up orphaned backup PVCs when migrating from filesystem to S3.
	if shard.Spec.Backup != nil && shard.Spec.Backup.Type == multigresv1alpha1.BackupTypeS3 {
		return nil
	}

	desired, err := BuildSharedBackupPVC(
		shard,
		cellName,
		ShouldDeleteShardLevelPVCOnRemoval(shard),
		r.Scheme,
	)
	if err != nil {
		return fmt.Errorf("failed to build shared backup PVC: %w", err)
	}
	if desired == nil {
		return nil
	}

	// Server Side Apply
	desired.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"))
	if err := r.Patch(
		ctx,
		desired,
		client.Apply,
		client.ForceOwnership,
		client.FieldOwner("multigres-operator"),
	); err != nil {
		return fmt.Errorf("failed to apply shared backup PVC: %w", err)
	}

	r.Recorder.Eventf(
		shard,
		"Normal",
		"Applied",
		"Applied %s %s",
		desired.GroupVersionKind().Kind,
		desired.Name,
	)

	return nil
}

// reconcilePoolPDB applies the PodDisruptionBudget for the pool in the specific cell.
func (r *ShardReconciler) reconcilePoolPDB(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	poolName string,
	cellName string,
) error {
	desired, err := BuildPoolPodDisruptionBudget(shard, poolName, cellName, r.Scheme)
	if err != nil {
		return fmt.Errorf("failed to build pool PDB: %w", err)
	}

	// Server Side Apply for PDB
	desired.SetGroupVersionKind(policyv1.SchemeGroupVersion.WithKind("PodDisruptionBudget"))
	if err := r.Patch(
		ctx,
		desired,
		client.Apply,
		client.ForceOwnership,
		client.FieldOwner("multigres-operator"),
	); err != nil {
		return fmt.Errorf("failed to apply pool PDB: %w", err)
	}

	return nil
}

// reconcilePoolHeadlessService creates or updates the headless Service for a pool in a specific cell.
func (r *ShardReconciler) reconcilePoolHeadlessService(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	poolName string,
	cellName string,
	poolSpec multigresv1alpha1.PoolSpec,
) error {
	desired, err := BuildPoolHeadlessService(shard, poolName, cellName, poolSpec, r.Scheme)
	if err != nil {
		return fmt.Errorf("failed to build pool headless Service: %w", err)
	}

	// Server Side Apply
	desired.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Service"))
	if err := r.Patch(
		ctx,
		desired,
		client.Apply,
		client.ForceOwnership,
		client.FieldOwner("multigres-operator"),
	); err != nil {
		return fmt.Errorf("failed to apply pool headless Service: %w", err)
	}

	return nil
}
