package shard

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/postgresconfig"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// renderedConfig bundles the once-per-reconcile render result so the status path
// (drift detection via the hashes) and the ConfigMap-delivery path
// (reconcilePostgresConfig) share a single value instead of threading the
// content, hashes, and error positionally through every signature.
//
// hash is the restart-hash (postmaster/internal settings) stamped on
// AnnotationPostgresConfigHash and folded into the pod spec-hash; reloadHash is
// the reload-safe subset stamped on AnnotationPostgresReloadHash and applied via
// a reload instead of pod recreation.
type renderedConfig struct {
	content    string
	hash       string
	reloadHash string
	// reloadSettings is the effective reload-safe name->value map (values
	// unquoted to match pg_file_settings), passed to the multipooler ReloadConfig
	// RPC as expected_settings so it reloads only against a file that already
	// carries them.
	reloadSettings map[string]string
	err            error
}

// renderEffectiveConfig renders the shard's effective postgresql.conf once per
// reconcile. Both the status path (drift detection via the hash) and
// reconcilePostgresConfig (ConfigMap delivery) consume this single result, so the
// legacy PostgresConfigRef ConfigMap is fetched and the template rendered exactly
// once.
func (r *ShardReconciler) renderEffectiveConfig(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) renderedConfig {
	var refContent string
	if shard.Spec.PostgresConfigRef != nil {
		content, cerr := r.postgresConfigRefContent(ctx, shard)
		if cerr != nil {
			return renderedConfig{err: cerr}
		}
		refContent = content
	}
	rendered, split, err := renderPostgresConfig(shard, refContent)
	return renderedConfig{
		content:        rendered,
		hash:           split.RestartHash,
		reloadHash:     split.ReloadHash,
		reloadSettings: split.ReloadSettings,
		err:            err,
	}
}

// reconcilePostgresConfig delivers the shard's effective postgresql.conf into
// the operator-owned ConfigMap and stamps its content hash on the shard as an
// in-memory annotation, so that config changes produce a different pod spec-hash
// and trigger the existing rolling update mechanism. cfg comes from
// renderEffectiveConfig (run once per reconcile); a render/read failure fails the
// reconcile here, after updateStatus has already surfaced it in the config
// status. Delivery always happens, so every shard gets an operator-owned config
// ConfigMap.
func (r *ShardReconciler) reconcilePostgresConfig(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	cfg renderedConfig,
) error {
	if cfg.err != nil {
		return cfg.err
	}

	if err := r.applyPostgresConfigMap(ctx, shard, cfg.content); err != nil {
		return err
	}

	if shard.Annotations == nil {
		shard.Annotations = make(map[string]string)
	}
	shard.Annotations[metadata.AnnotationPostgresConfigHash] = cfg.hash
	shard.Annotations[metadata.AnnotationPostgresReloadHash] = cfg.reloadHash
	return nil
}

// renderPostgresConfig renders the effective postgresql.conf for a shard and
// returns it together with its split content hashes: the restart-hash stamped on
// AnnotationPostgresConfigHash (and folded into the pod spec-hash) and the
// reload-hash stamped on AnnotationPostgresReloadHash. It applies
// resource-derived sizing computed from the shard's pool resources (see
// reduceShardResources), and layers the shard's inline spec.postgresConfig map
// on top. refContent is the legacy PostgresConfigRef body (empty when unset). It
// is re-exported to black-box tests as RenderPostgresConfig via export_test.go so
// they can reproduce the hashes the controller stamps.
func renderPostgresConfig(
	shard *multigresv1alpha1.Shard,
	refContent string,
) (rendered string, split postgresconfig.ConfigSplit, err error) {
	cfg := postgresconfig.Defaults()
	cfg.ClusterName = shardClusterName(shard)
	memBytes, cpuMillicores, diskBytes := reduceShardResources(shard)
	if err := postgresconfig.ApplyResourceSizing(
		&cfg,
		memBytes,
		cpuMillicores,
		diskBytes,
	); err != nil {
		return "", postgresconfig.ConfigSplit{}, err
	}

	rendered, err = postgresconfig.Render(cfg, refContent, shard.Spec.PostgresConfig)
	if err != nil {
		return "", postgresconfig.ConfigSplit{}, err
	}
	return rendered, postgresconfig.SplitConfig(rendered), nil
}

// shardClusterName builds the postgresql.conf cluster_name for a shard: a
// slash-joined cluster/database/tablegroup/shard path so the owning shard is
// identifiable in Postgres process titles and logs (every shard is a distinct
// set of Postgres instances). Empty components are dropped, and it falls back to
// the Shard object name when the cluster label is absent (e.g. a Shard applied
// directly without the operator's labels). The components are DNS-safe k8s names
// and validated identifiers, so they never contain the single quote the template
// wraps this value in.
func shardClusterName(shard *multigresv1alpha1.Shard) string {
	parts := make([]string, 0, 4)
	for _, p := range []string{
		shard.Labels[metadata.LabelMultigresCluster],
		string(shard.Spec.DatabaseName),
		string(shard.Spec.TableGroupName),
		string(shard.Spec.ShardName),
	} {
		if p != "" {
			parts = append(parts, p)
		}
	}
	if len(parts) == 0 {
		return shard.Name
	}
	return strings.Join(parts, "/")
}

// reduceShardResources reduces the shard's per-pool resources to the single
// basis used for config sizing: memory and CPU take the max across pools (so the
// replication-sensitive settings stay valid on every pod), disk takes the min
// (so WAL budgeting never overfills the smallest volume). Each value falls back
// from limit to request. Absent inputs return zero, which leaves the baseline.
func reduceShardResources(
	shard *multigresv1alpha1.Shard,
) (memBytes, cpuMillicores, diskBytes int64) {
	for _, pool := range shard.Spec.Pools {
		memQ := effectiveResource(pool.Postgres.Resources, corev1.ResourceMemory)
		if mem := memQ.Value(); mem > memBytes {
			memBytes = mem
		}
		cpuQ := effectiveResource(pool.Postgres.Resources, corev1.ResourceCPU)
		if cpu := cpuQ.MilliValue(); cpu > cpuMillicores {
			cpuMillicores = cpu
		}
		if disk := parseStorageBytes(
			pool.Storage.Size,
		); disk > 0 &&
			(diskBytes == 0 || disk < diskBytes) {
			diskBytes = disk
		}
	}
	return memBytes, cpuMillicores, diskBytes
}

// effectiveResource returns the limit for a resource, falling back to the
// request, matching "size off the ceiling, guaranteed floor otherwise".
func effectiveResource(
	res corev1.ResourceRequirements,
	name corev1.ResourceName,
) resource.Quantity {
	if q, ok := res.Limits[name]; ok && !q.IsZero() {
		return q
	}
	if q, ok := res.Requests[name]; ok && !q.IsZero() {
		return q
	}
	return resource.Quantity{}
}

func parseStorageBytes(size string) int64 {
	if size == "" {
		return 0
	}
	q, err := resource.ParseQuantity(size)
	if err != nil {
		return 0
	}
	return q.Value()
}

// postgresConfigRefContent fetches the body of the shard's legacy
// PostgresConfigRef ConfigMap key. The caller guarantees PostgresConfigRef is
// non-nil.
func (r *ShardReconciler) postgresConfigRefContent(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) (string, error) {
	ref := shard.Spec.PostgresConfigRef

	cm := &corev1.ConfigMap{}
	if err := r.Get(ctx, client.ObjectKey{
		Namespace: shard.Namespace,
		Name:      ref.Name,
	}, cm); err != nil {
		return "", fmt.Errorf("failed to get ConfigMap %q: %w", ref.Name, err)
	}

	data, ok := cm.Data[ref.Key]
	if !ok {
		return "", fmt.Errorf("key %q not found in ConfigMap %q", ref.Key, ref.Name)
	}
	return data, nil
}

// applyPostgresConfigMap server-side-applies the operator-owned ConfigMap that
// holds the rendered postgresql.conf for a shard.
func (r *ShardReconciler) applyPostgresConfigMap(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	rendered string,
) error {
	desired, err := BuildPostgresConfigMap(shard, rendered, r.Scheme)
	if err != nil {
		return fmt.Errorf("failed to build postgres config ConfigMap: %w", err)
	}

	desired.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("ConfigMap"))
	if err := r.Patch(
		ctx,
		desired,
		client.Apply,
		client.ForceOwnership,
		client.FieldOwner("multigres-operator"),
	); err != nil {
		return fmt.Errorf("failed to apply postgres config ConfigMap: %w", err)
	}
	return nil
}

// enqueueFromPostgresConfigMap returns reconcile requests for Shards that
// reference the changed ConfigMap via spec.postgresConfigRef.name.
func (r *ShardReconciler) enqueueFromPostgresConfigMap(
	ctx context.Context,
	o client.Object,
) []reconcile.Request {
	shards := &multigresv1alpha1.ShardList{}
	if err := r.List(ctx, shards, client.InNamespace(o.GetNamespace())); err != nil {
		return nil
	}

	var requests []reconcile.Request
	for _, s := range shards.Items {
		if s.Spec.PostgresConfigRef != nil && s.Spec.PostgresConfigRef.Name == o.GetName() {
			requests = append(requests, reconcile.Request{
				NamespacedName: client.ObjectKeyFromObject(&s),
			})
		}
	}
	return requests
}
