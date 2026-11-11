package shard

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/cert"
	"github.com/multigres/multigres-operator/pkg/ownership"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	pvcutil "github.com/multigres/multigres-operator/pkg/util/pvc"
)

// pvcFieldOwner is the server-side-apply field manager for the shared backup PVC.
const pvcFieldOwner = ownership.ManagerChildWriter

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
		client.FieldOwner(ownership.ManagerChildWriter),
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
		client.FieldOwner(ownership.ManagerChildWriter),
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
	backup := shard.Spec.SidecarBackupLocation
	if backup == nil {
		return nil
	}

	// User-provided Secret: validate via uncached API reader.
	// We use APIReader instead of the cached client because the informer cache
	// only stores operator-labeled Secrets, making external Secrets (e.g.,
	// cert-manager) invisible to the cached r.Get().
	if backup.PgBackRestTLS != nil &&
		backup.PgBackRestTLS.SecretName != "" {
		secretName := backup.PgBackRestTLS.SecretName
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
	backup := shard.Spec.SidecarBackupLocation
	if backup == nil || backup.Encryption == nil {
		return nil
	}

	secretName := backup.Encryption.SecretName
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

// reconcileSharedBackupPVC creates or updates the shared backup PVC for a
// shard, reclaiming it first if a previous incarnation of the shard left it
// marked orphan. A PVC being deleted is left alone: an apply would only update
// a claim that is about to vanish, and the first apply after it is gone, which
// its deletion event triggers, creates the empty replacement.
func (r *ShardReconciler) reconcileSharedBackupPVC(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) error {
	// S3 backups use object storage; no shared PVC is needed.
	// TODO: Consider cleaning up orphaned backup PVCs when migrating from filesystem to S3.
	if sidecarLocation(shard).Type == multigresv1alpha1.BackupTypeS3 {
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

	existing := &corev1.PersistentVolumeClaim{}
	err = r.Get(ctx, client.ObjectKeyFromObject(desired), existing)
	switch {
	case errors.IsNotFound(err):
	case err != nil:
		return fmt.Errorf("failed to get shared backup PVC: %w", err)
	case pvcutil.IsTerminating(existing):
		log.FromContext(ctx).Info(
			"Shared backup PVC is being deleted; not applying it",
			"pvc", existing.Name,
		)
		return nil
	default:
		// The apply below never mentions the orphan label, so it would leave
		// it in place on a PVC back in service, for multigres-gc to delete.
		terminating, err := r.reclaimPVC(ctx, existing)
		if err != nil {
			return fmt.Errorf("failed to reclaim shared backup PVC: %w", err)
		}
		if terminating {
			return nil
		}
		if err := r.keepUnchangeablePVCFields(ctx, shard, desired, existing); err != nil {
			return err
		}
	}

	// Server Side Apply
	desired.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"))
	if err := r.Patch(
		ctx,
		desired,
		client.Apply,
		client.ForceOwnership,
		client.FieldOwner(ownership.ManagerChildWriter),
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

// alignPVC rewrites desired in place and returns one note per change a user
// would otherwise not see.
//
// It keeps every field the API server refuses to change on a live claim at
// its live value, so a claim created from the shard-level backup config
// survives the switch to the sidecar location:
//
//   - a field the operator owns is re-sent with the live value;
//   - a field it does not own is sent only when it already equals the live
//     value, or when the claim has no class yet and one is now specified
//     (Kubernetes allows that, and the claim must not stay Pending). Otherwise
//     it is omitted so the live value stands, which also means the operator
//     never takes ownership of a class nobody specified, such as the one the
//     default StorageClass plugin fills in;
//   - volumeName is never sent;
//   - the request is never lowered, except to a value strictly above the
//     claim's status.capacity, which is the recovery path after a failed
//     expansion and is what the API server accepts; a claim that is not bound
//     keeps its request.
func alignPVC(desired, existing *corev1.PersistentVolumeClaim) []string {
	owned := operatorOwnedSpecFields(existing)
	desiredClass := ptr.Deref(desired.Spec.StorageClassName, "")
	existingClass := ptr.Deref(existing.Spec.StorageClassName, "")

	var notes []string
	switch {
	case owned["f:storageClassName"]:
		if desiredClass != "" && desiredClass != existingClass {
			notes = append(notes, fmt.Sprintf(
				"storageClassName %q ignored, the claim keeps %q", desiredClass, existingClass))
		}
		desired.Spec.StorageClassName = existing.Spec.StorageClassName
	case existingClass == "" && desiredClass != "":
	case desiredClass != "" && desiredClass == existingClass:
	default:
		if desiredClass != "" {
			notes = append(notes, fmt.Sprintf(
				"storageClassName %q ignored, the claim keeps %q", desiredClass, existingClass))
		}
		desired.Spec.StorageClassName = nil
	}

	switch {
	case slices.Equal(desired.Spec.AccessModes, existing.Spec.AccessModes):
	case owned["f:accessModes"]:
		notes = append(notes, fmt.Sprintf(
			"accessModes %v ignored, the claim keeps %v",
			desired.Spec.AccessModes, existing.Spec.AccessModes))
		desired.Spec.AccessModes = existing.Spec.AccessModes
	default:
		notes = append(notes, fmt.Sprintf(
			"accessModes %v ignored, the claim keeps %v",
			desired.Spec.AccessModes, existing.Spec.AccessModes))
		desired.Spec.AccessModes = nil
	}

	// The operator never sets volumeName; it is the binder's.
	desired.Spec.VolumeName = ""

	if have, ok := existing.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
		want := desired.Spec.Resources.Requests[corev1.ResourceStorage]
		keep := want
		switch {
		case existing.Status.Phase != corev1.ClaimBound:
			keep = have
		case want.Cmp(have) < 0:
			// Lowering is accepted only strictly above status.capacity, the way
			// out of an expansion the backend could not satisfy.
			capacity, reported := existing.Status.Capacity[corev1.ResourceStorage]
			if !reported || want.Cmp(capacity) <= 0 {
				keep = have
			}
		}
		if keep.Cmp(want) != 0 {
			notes = append(notes, fmt.Sprintf(
				"storage request %s ignored, the claim keeps %s", want.String(), keep.String()))
			desired.Spec.Resources.Requests[corev1.ResourceStorage] = keep
		}
	}
	return notes
}

// operatorOwnedSpecFields returns the spec fields the operator's apply manager
// owns on obj, keyed by their managed-fields name (for example "f:accessModes").
func operatorOwnedSpecFields(obj *corev1.PersistentVolumeClaim) map[string]bool {
	owned := map[string]bool{}
	for _, mf := range obj.ManagedFields {
		if mf.Manager != pvcFieldOwner || mf.Operation != metav1.ManagedFieldsOperationApply ||
			mf.FieldsV1 == nil {
			continue
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(mf.FieldsV1.GetRawBytes(), &fields); err != nil {
			continue
		}
		var spec map[string]json.RawMessage
		if err := json.Unmarshal(fields["f:spec"], &spec); err != nil {
			continue
		}
		for k := range spec {
			owned[k] = true
		}
	}
	return owned
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
			client.FieldOwner(ownership.ManagerChildWriter),
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
		client.FieldOwner(ownership.ManagerChildWriter),
	); err != nil {
		return fmt.Errorf("failed to apply pool headless Service: %w", err)
	}

	return nil
}

// keepUnchangeablePVCFields copies onto desired the parts of an existing PVC's
// spec that the API server would refuse to change: a storage request it cannot
// apply (see storageRequestFor), and the storage class and access modes, which
// are fixed at creation. A refused apply fails on every attempt, and since the
// shared backup PVC is applied before the pools, the PDB and the data plane, it
// would stall all of them for as long as the spec disagrees with the PVC. The
// disagreement is reported by the StorageSpecIgnored condition instead.
func (r *ShardReconciler) keepUnchangeablePVCFields(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	desired, existing *corev1.PersistentVolumeClaim,
) error {
	request, _, err := r.storageRequestFor(
		ctx,
		existing,
		desired.Spec.Resources.Requests[corev1.ResourceStorage],
	)
	if err != nil {
		return err
	}
	// A classless claim may be assigned its first class, but not a missing
	// one: that would strand an existing backup claim on an unusable class.
	// Keep the requested value in the Shard so StorageSpecIgnored reports it.
	if ptr.Deref(existing.Spec.StorageClassName, "") == "" &&
		ptr.Deref(desired.Spec.StorageClassName, "") != "" {
		exists, err := r.validateStorageClassExists(ctx, *desired.Spec.StorageClassName)
		if err != nil {
			return err
		}
		if !exists {
			desired.Spec.StorageClassName = existing.Spec.StorageClassName
		}
	}
	notes := alignPVC(desired, existing)
	for _, note := range notes {
		r.Recorder.Eventf(shard, "Warning", "BackupPVCSpecIgnored",
			"Shared backup PVC %s: %s", desired.Name, note)
	}
	// Keep main's StorageClass-aware, non-shrinking policy even where
	// alignPVC would permit recovery of a failed expansion.
	desired.Spec.Resources.Requests[corev1.ResourceStorage] = request
	return nil
}
