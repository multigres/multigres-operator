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

// reconcilePostgresPasswordSecret validates the referenced postgres password Secret.
func (r *ShardReconciler) reconcilePostgresPasswordSecret(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) error {
	_, err := r.postgresPasswordSecretData(ctx, shard)
	return err
}

func (r *ShardReconciler) postgresPasswordSecretData(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) ([]byte, error) {
	_, _, data, err := r.postgresPasswordSecret(ctx, shard)
	return data, err
}

func (r *ShardReconciler) postgresPasswordSecret(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) (*corev1.Secret, string, []byte, error) {
	secretName, secretKey := postgresPasswordSecretRef(shard)
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}

	secret := &corev1.Secret{}
	if err := reader.Get(ctx, types.NamespacedName{
		Namespace: shard.Namespace,
		Name:      secretName,
	}, secret); err != nil {
		return nil, "", nil, fmt.Errorf(
			"failed to get postgres password Secret %q: %w",
			secretName,
			err,
		)
	}

	data, ok := secret.Data[secretKey]
	if !ok {
		return nil, "", nil, fmt.Errorf(
			"key %q not found in postgres password Secret %q",
			secretKey,
			secretName,
		)
	}
	if len(data) == 0 {
		return nil, "", nil, fmt.Errorf(
			"key %q in postgres password Secret %q is empty",
			secretKey,
			secretName,
		)
	}
	return secret, secretKey, data, nil
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

// reconcileBackupCipherSecret validates the pgBackRest backup encryption
// cipher key Secret when client-side backup encryption is enabled.
func (r *ShardReconciler) reconcileBackupCipherSecret(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) error {
	if shard.Spec.Backup == nil || shard.Spec.Backup.Encryption == nil {
		return nil
	}

	secretName := shard.Spec.Backup.Encryption.SecretName
	if secretName == "" {
		return fmt.Errorf(
			"pgbackrest cipher key secret name is required when encryption is enabled",
		)
	}

	// Validate via APIReader instead of the cached client because the informer cache
	// only stores operator-labeled Secrets.
	secret := &corev1.Secret{}
	if err := r.APIReader.Get(ctx, types.NamespacedName{
		Name:      secretName,
		Namespace: shard.Namespace,
	}, secret); err != nil {
		return fmt.Errorf("pgbackrest cipher key secret %q not found: %w", secretName, err)
	}
	if _, ok := secret.Data[PgBackRestCipherKeyDataKey]; !ok {
		return fmt.Errorf(
			"pgbackrest cipher key secret %q missing required key %q",
			secretName,
			PgBackRestCipherKeyDataKey,
		)
	}
	return nil
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
