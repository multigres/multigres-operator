package shard

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/cert"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	pvcutil "github.com/multigres/multigres-operator/pkg/util/pvc"
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

// reconcilePostgresExporterQueriesConfigMap creates or updates the custom
// postgres_exporter queries ConfigMap for a shard. Shared across all pools and
// mounted into every pool pod's exporter sidecar via --extend.query-path.
func (r *ShardReconciler) reconcilePostgresExporterQueriesConfigMap(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) error {
	desired, err := BuildPostgresExporterQueriesConfigMap(shard, r.Scheme)
	if err != nil {
		return fmt.Errorf("failed to build postgres_exporter queries ConfigMap: %w", err)
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
		return fmt.Errorf("failed to apply postgres_exporter queries ConfigMap: %w", err)
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

func (r *ShardReconciler) reconcilePostgresInitSecretsSecret(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) error {
	if !initSecretsConfigured(shard) {
		return nil
	}

	secretName, secretKey := postgresInitSecretsRef(shard)
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}

	secret := &corev1.Secret{}
	if err := reader.Get(ctx, types.NamespacedName{
		Namespace: shard.Namespace,
		Name:      secretName,
	}, secret); err != nil {
		return fmt.Errorf(
			"failed to get postgres init-secrets Secret %q: %w",
			secretName,
			err,
		)
	}

	data, ok := secret.Data[secretKey]
	if !ok {
		return fmt.Errorf(
			"key %q not found in postgres init-secrets Secret %q",
			secretKey,
			secretName,
		)
	}
	if len(data) == 0 {
		return fmt.Errorf(
			"key %q in postgres init-secrets Secret %q is empty",
			secretKey,
			secretName,
		)
	}

	if !json.Valid(data) {
		return fmt.Errorf(
			"key %q in postgres init-secrets Secret %q is not valid JSON",
			secretKey,
			secretName,
		)
	}

	var payload *struct {
		Roles            map[string]string            `json:"roles"`
		DatabaseSettings map[string]map[string]string `json:"database_settings"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return fmt.Errorf(
			"key %q in postgres init-secrets Secret %q must be a JSON object with string values in roles and database_settings",
			secretKey,
			secretName,
		)
	}
	if payload == nil {
		return fmt.Errorf(
			"key %q in postgres init-secrets Secret %q must be a JSON object, not null",
			secretKey,
			secretName,
		)
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
		Organization:       "Multigres",
		Owner:              shard,
		ComponentName:      "pgbackrest",
		Labels:             metadata.BuildStandardLabels(clusterName, "pgbackrest-tls"),
		AdditionalDNSNames: pgBackRestPoolDNSNames(shard),
	})
	return rotator.Bootstrap(ctx)
}

// pgBackRestPoolDNSNames returns the DNS SANs needed for the pgBackRest server
// cert to be validated when pgbackrest clients connect to a specific replica
// pod for pg2 (standby) backups. Those connections target the pod's per-pool,
// per-cell headless-Service FQDN (<pod>.<headless-svc>.<ns>.svc[.cluster.local]).
func pgBackRestPoolDNSNames(shard *multigresv1alpha1.Shard) []string {
	var dnsNames []string
	for poolName, poolSpec := range shard.Spec.Pools {
		for _, cellName := range poolSpec.Cells {
			headlessName := buildPoolHeadlessServiceName(shard, string(poolName), string(cellName))
			dnsNames = append(dnsNames,
				fmt.Sprintf("*.%s.%s.svc", headlessName, shard.Namespace),
				fmt.Sprintf("*.%s.%s.svc.cluster.local", headlessName, shard.Namespace),
			)
		}
	}
	return dnsNames
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

// reconcileSharedBackupPVC creates or updates the shared backup PVC for a shard.
func (r *ShardReconciler) reconcileSharedBackupPVC(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) error {
	// S3 backups use object storage; no shared PVC is needed.
	// TODO: Consider cleaning up orphaned backup PVCs when migrating from filesystem to S3.
	if shard.Spec.Backup != nil && shard.Spec.Backup.Type == multigresv1alpha1.BackupTypeS3 {
		return nil
	}

	desired, err := BuildSharedBackupPVC(
		shard,
		ShouldDeleteShardLevelPVCOnRemoval(shard),
		r.Scheme,
	)
	if err != nil {
		return fmt.Errorf("failed to build shared backup PVC: %w", err)
	}
	if desired == nil {
		return nil
	}

	// A prior teardown of this shard may have left the PVC labelled orphan:
	// cleanupShardPVCs marks the backup PVC rather than deleting it, because
	// resolvePodIndex finds no ordinal in its name-hash suffix. The apply below
	// will not undo that, since the payload never mentions the label and
	// server-side apply only removes fields this manager already owns. Clear it
	// explicitly, as the per-pool data PVC path does on reuse, so multigres-gc
	// does not collect a volume we are putting straight back into service.
	existing := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(desired), existing); err != nil {
		if !errors.IsNotFound(err) {
			return fmt.Errorf("failed to get shared backup PVC: %w", err)
		}
	} else if err := pvcutil.ClearOrphan(ctx, log.FromContext(ctx), r.Client, existing); err != nil {
		return fmt.Errorf("failed to clear orphan label on shared backup PVC: %w", err)
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

// reconcileShardPDB applies the shard-wide PodDisruptionBudget and removes
// obsolete operator-owned PDBs.
func (r *ShardReconciler) reconcileShardPDB(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) error {
	desired, err := BuildShardPodDisruptionBudgets(shard, r.Scheme)
	if err != nil {
		return fmt.Errorf("failed to build shard PDBs: %w", err)
	}
	poolers := &corev1.PodList{}
	if err := r.List(
		ctx,
		poolers,
		client.InNamespace(shard.Namespace),
		client.MatchingLabels(desired[0].Spec.Selector.MatchLabels),
	); err != nil {
		return fmt.Errorf("failed to list shard poolers for PDB sizing: %w", err)
	}
	var surgeCount int32
	for i := range poolers.Items {
		if isActiveMaintenanceSurge(shard, &poolers.Items[i]) {
			surgeCount++
		}
	}
	minAvailable := intstr.FromInt32(
		minAvailableForTotal(shardTotalReplicas(shard) + surgeCount),
	)
	desired[0].Spec.MinAvailable = &minAvailable

	desiredNames := make(map[string]struct{}, len(desired))
	for _, pdb := range desired {
		desiredNames[pdb.Name] = struct{}{}
		pdb.SetGroupVersionKind(policyv1.SchemeGroupVersion.WithKind("PodDisruptionBudget"))
		if err := r.Patch(
			ctx,
			pdb,
			client.Apply,
			client.ForceOwnership,
			client.FieldOwner("multigres-operator"),
		); err != nil {
			return fmt.Errorf("failed to apply shard PDB %s: %w", pdb.Name, err)
		}
	}

	// Owner references do not garbage-collect obsolete PDBs until the Shard is
	// deleted. Remove legacy pool-cell budgets and no-longer-required cell
	// budgets only after every replacement has been applied.
	pdbs := &policyv1.PodDisruptionBudgetList{}
	if err := r.List(
		ctx,
		pdbs,
		client.InNamespace(shard.Namespace),
		client.MatchingLabels(desired[0].Spec.Selector.MatchLabels),
	); err != nil {
		return fmt.Errorf("failed to list obsolete shard PDBs: %w", err)
	}

	for i := range pdbs.Items {
		pdb := &pdbs.Items[i]
		if _, keep := desiredNames[pdb.Name]; keep || !metav1.IsControlledBy(pdb, shard) {
			continue
		}
		if err := r.Delete(ctx, pdb); err != nil && !errors.IsNotFound(err) {
			return fmt.Errorf("failed to delete obsolete shard PDB %s: %w", pdb.Name, err)
		}
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
