package handlers

import (
	"context"
	"fmt"
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/monitoring"
	"github.com/multigres/multigres-operator/pkg/resolver"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// ============================================================================
// MultigresCluster Validator
// ============================================================================

// +kubebuilder:webhook:path=/validate-multigres-com-v1alpha1-multigrescluster,mutating=false,failurePolicy=fail,sideEffects=None,groups=multigres.com,resources=multigresclusters,verbs=create;update,versions=v1alpha1,name=vmultigrescluster.kb.io,admissionReviewVersions=v1

// MultigresClusterValidator validates Create and Update events for MultigresClusters.
type MultigresClusterValidator struct {
	Client client.Client
}

var _ webhook.CustomValidator = &MultigresClusterValidator{}

// NewMultigresClusterValidator creates a new validator for MultigresClusters.
func NewMultigresClusterValidator(c client.Client) *MultigresClusterValidator {
	return &MultigresClusterValidator{Client: c}
}

// ValidateCreate validates a MultigresCluster on creation.
func (v *MultigresClusterValidator) ValidateCreate(
	ctx context.Context,
	obj runtime.Object,
) (admission.Warnings, error) {
	return v.validate(ctx, obj)
}

// ValidateUpdate validates a MultigresCluster on update, including storage shrink and etcd replica checks.
func (v *MultigresClusterValidator) ValidateUpdate(
	ctx context.Context,
	oldObj, newObj runtime.Object,
) (admission.Warnings, error) {
	warnings, err := v.validate(ctx, newObj)
	if err != nil {
		return warnings, err
	}
	shrinkWarnings, shrinkErr := validateNoStorageShrink(oldObj, newObj)
	warnings = append(warnings, shrinkWarnings...)
	if shrinkErr != nil {
		return warnings, shrinkErr
	}
	etcdWarnings, etcdErr := validateEtcdReplicasImmutable(oldObj, newObj)
	warnings = append(warnings, etcdWarnings...)
	return warnings, etcdErr
}

// ValidateDelete is a no-op for MultigresCluster deletion.
func (v *MultigresClusterValidator) ValidateDelete(
	ctx context.Context,
	obj runtime.Object,
) (admission.Warnings, error) {
	return nil, nil
}

func (v *MultigresClusterValidator) validate(
	ctx context.Context,
	obj runtime.Object,
) (admission.Warnings, error) {
	start := time.Now()
	ctx, span := monitoring.StartChildSpan(ctx, "Webhook.Validate")
	defer span.End()

	logger := log.FromContext(ctx)
	logger.V(1).Info("validation webhook started")

	cluster, ok := obj.(*multigresv1alpha1.MultigresCluster)
	if !ok {
		err := fmt.Errorf("expected MultigresCluster, got %T", obj)
		monitoring.RecordSpanError(span, err)
		monitoring.RecordWebhookRequest("VALIDATE", "MultigresCluster", err, time.Since(start))
		return nil, err
	}

	// 1. Stateful Validation (Level 4): Referential Integrity
	{
		_, childSpan := monitoring.StartChildSpan(ctx, "Webhook.ValidateTemplatesExist")
		if err := v.validateTemplatesExist(ctx, cluster); err != nil {
			monitoring.RecordSpanError(childSpan, err)
			childSpan.End()
			monitoring.RecordWebhookRequest("VALIDATE", "MultigresCluster", err, time.Since(start))
			return nil, err
		}
		childSpan.End()
	}

	// 2. Deep Logic Validation (Safety Checks)
	{
		_, childSpan := monitoring.StartChildSpan(ctx, "Webhook.ValidateLogic")
		if warnings, err := v.validateLogic(ctx, cluster); err != nil {
			monitoring.RecordSpanError(childSpan, err)
			childSpan.End()
			monitoring.RecordWebhookRequest("VALIDATE", "MultigresCluster", err, time.Since(start))
			return warnings, err
		} else if len(warnings) > 0 {
			childSpan.End()
			monitoring.RecordWebhookRequest("VALIDATE", "MultigresCluster", nil, time.Since(start))
			return warnings, nil
		}
		childSpan.End()
	}

	monitoring.RecordWebhookRequest("VALIDATE", "MultigresCluster", nil, time.Since(start))
	logger.V(1).Info("validation webhook complete", "duration", time.Since(start).String())
	return nil, nil
}

func (v *MultigresClusterValidator) validateTemplatesExist(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
) error {
	// Create an ephemeral resolver for validation
	// "Shared Resolver Pattern"
	res := resolver.NewResolver(v.Client, cluster.Namespace)
	return res.ValidateClusterIntegrity(ctx, cluster)
}

func (v *MultigresClusterValidator) validateLogic(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
) (admission.Warnings, error) {
	// Create an ephemeral resolver for validation
	res := resolver.NewResolver(v.Client, cluster.Namespace)
	return res.ValidateClusterLogic(ctx, cluster)
}

// validateNoStorageShrink compares pool storage sizes between old and new cluster
// specs, rejecting any decrease. PVC shrink is not supported by Kubernetes.
func validateNoStorageShrink(
	oldObj, newObj runtime.Object,
) (admission.Warnings, error) {
	oldCluster, ok := oldObj.(*multigresv1alpha1.MultigresCluster)
	if !ok {
		return nil, nil
	}
	newCluster, ok := newObj.(*multigresv1alpha1.MultigresCluster)
	if !ok {
		return nil, nil
	}

	oldSizes := collectPoolStorageSizes(oldCluster)
	newSizes := collectPoolStorageSizes(newCluster)

	for key, oldSize := range oldSizes {
		newSize, exists := newSizes[key]
		if !exists {
			continue
		}
		oldQty, err := resource.ParseQuantity(oldSize)
		if err != nil {
			continue
		}
		newQty, err := resource.ParseQuantity(newSize)
		if err != nil {
			continue
		}
		if newQty.Cmp(oldQty) < 0 {
			return nil, fmt.Errorf(
				"storage.size cannot be decreased (%s: %s → %s); PVC shrink is not supported by Kubernetes",
				key,
				oldSize,
				newSize,
			)
		}
	}

	return nil, nil
}

// collectPoolStorageSizes walks the cluster spec and returns a map of
// "db/tg/shard/pool" → storage size string for all pools.
func collectPoolStorageSizes(cluster *multigresv1alpha1.MultigresCluster) map[string]string {
	sizes := make(map[string]string)
	for _, db := range cluster.Spec.Databases {
		for _, tg := range db.TableGroups {
			for _, shard := range tg.Shards {
				var pools map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec
				if shard.Spec != nil {
					pools = shard.Spec.Pools
				} else if shard.Overrides != nil {
					pools = shard.Overrides.Pools
				}
				for poolName, pool := range pools {
					if pool.Storage.Size != "" {
						key := fmt.Sprintf("%s/%s/%s/%s",
							db.Name, tg.Name, shard.Name, poolName)
						sizes[key] = pool.Storage.Size
					}
				}
			}
		}
	}
	return sizes
}

// validateEtcdReplicasImmutable rejects changes to etcd replica count after
// cluster creation. etcd uses static bootstrap (ETCD_INITIAL_CLUSTER_STATE=new)
// which is a one-time operation — scaling requires etcdctl member management
// that the operator does not implement.
func validateEtcdReplicasImmutable(
	oldObj, newObj runtime.Object,
) (admission.Warnings, error) {
	oldCluster, ok := oldObj.(*multigresv1alpha1.MultigresCluster)
	if !ok {
		return nil, nil
	}
	newCluster, ok := newObj.(*multigresv1alpha1.MultigresCluster)
	if !ok {
		return nil, nil
	}

	oldReplicas := effectiveEtcdReplicas(oldCluster)
	newReplicas := effectiveEtcdReplicas(newCluster)

	// Both 0 means both use external topo — no managed etcd to validate.
	// One 0 and one non-0 means switching between managed/external, which is
	// a separate concern (and would fail for other reasons).
	if oldReplicas == 0 || newReplicas == 0 {
		return nil, nil
	}

	if oldReplicas != newReplicas {
		return nil, fmt.Errorf(
			"topology server (etcd) replicas cannot be changed after cluster creation (%d → %d); "+
				"etcd uses static bootstrap which does not support dynamic member addition",
			oldReplicas,
			newReplicas,
		)
	}

	return nil, nil
}

// effectiveEtcdReplicas returns the etcd replica count for a cluster,
// resolving nil/absent fields to the default. Returns 0 for external topo.
func effectiveEtcdReplicas(cluster *multigresv1alpha1.MultigresCluster) int32 {
	if cluster.Spec.GlobalTopoServer != nil && cluster.Spec.GlobalTopoServer.External != nil {
		return 0
	}
	if cluster.Spec.GlobalTopoServer != nil &&
		cluster.Spec.GlobalTopoServer.Etcd != nil &&
		cluster.Spec.GlobalTopoServer.Etcd.Replicas != nil {
		return *cluster.Spec.GlobalTopoServer.Etcd.Replicas
	}
	return resolver.DefaultEtcdReplicas
}

// ============================================================================
// Template Validators (In-Use Protection)
// ============================================================================

// +kubebuilder:webhook:path=/validate-multigres-com-v1alpha1-coretemplate,mutating=false,failurePolicy=fail,sideEffects=None,groups=multigres.com,resources=coretemplates,verbs=delete,versions=v1alpha1,name=vcoretemplate.kb.io,admissionReviewVersions=v1
// +kubebuilder:webhook:path=/validate-multigres-com-v1alpha1-celltemplate,mutating=false,failurePolicy=fail,sideEffects=None,groups=multigres.com,resources=celltemplates,verbs=delete,versions=v1alpha1,name=vcelltemplate.kb.io,admissionReviewVersions=v1
// +kubebuilder:webhook:path=/validate-multigres-com-v1alpha1-shardtemplate,mutating=false,failurePolicy=fail,sideEffects=None,groups=multigres.com,resources=shardtemplates,verbs=create;update;delete,versions=v1alpha1,name=vshardtemplate.kb.io,admissionReviewVersions=v1

// TemplateValidator validates Delete events to ensure templates are not in use.
type TemplateValidator struct {
	Client client.Client
	Kind   string
}

var _ webhook.CustomValidator = &TemplateValidator{}

// NewTemplateValidator creates a validator that prevents deletion of in-use templates.
func NewTemplateValidator(c client.Client, kind string) *TemplateValidator {
	return &TemplateValidator{Client: c, Kind: kind}
}

// ValidateCreate validates pool name map keys for ShardTemplates on creation.
func (v *TemplateValidator) ValidateCreate(
	ctx context.Context,
	obj runtime.Object,
) (admission.Warnings, error) {
	return v.validatePoolNames(obj)
}

// ValidateUpdate validates pool name map keys for ShardTemplates on update.
func (v *TemplateValidator) ValidateUpdate(
	ctx context.Context,
	oldObj, newObj runtime.Object,
) (admission.Warnings, error) {
	return v.validatePoolNames(newObj)
}

// validatePoolNames validates pool name map keys for ShardTemplates.
// CRD structural schema does not enforce validation markers on map keys.
func (v *TemplateValidator) validatePoolNames(obj runtime.Object) (admission.Warnings, error) {
	if v.Kind != "ShardTemplate" {
		return nil, nil
	}
	tpl, ok := obj.(*multigresv1alpha1.ShardTemplate)
	if !ok {
		return nil, nil
	}
	for poolName := range tpl.Spec.Pools {
		if err := resolver.ValidatePoolName(poolName); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// ValidateDelete rejects deletion of templates that are referenced by a MultigresCluster.
func (v *TemplateValidator) ValidateDelete(
	ctx context.Context,
	obj runtime.Object,
) (admission.Warnings, error) {
	// We need the Name and Namespace of the template being deleted
	metaObj, ok := obj.(client.Object)
	if !ok {
		return nil, fmt.Errorf("expected client.Object, got %T", obj)
	}
	templateName := metaObj.GetName()
	namespace := metaObj.GetNamespace()

	// Pre-filter clusters by tracking label to narrow the search space.
	kindLabel := ""
	switch v.Kind {
	case "CoreTemplate":
		kindLabel = metadata.LabelUsesCoreTemplate
	case "CellTemplate":
		kindLabel = metadata.LabelUsesCellTemplate
	case "ShardTemplate":
		kindLabel = metadata.LabelUsesShardTemplate
	}

	listOpts := []client.ListOption{client.InNamespace(namespace)}
	if kindLabel != "" {
		listOpts = append(listOpts, client.MatchingLabels{kindLabel: "true"})
	}

	clusters := &multigresv1alpha1.MultigresClusterList{}
	if err := v.Client.List(ctx, clusters, listOpts...); err != nil {
		return nil, fmt.Errorf("failed to list clusters for validation: %w", err)
	}

	for _, cluster := range clusters.Items {
		if v.isTemplateInUse(&cluster, templateName) {
			return nil, fmt.Errorf(
				"cannot delete %s '%s' because it is in use by MultigresCluster '%s'",
				v.Kind, templateName, cluster.Name,
			)
		}
	}

	return nil, nil
}

func (v *TemplateValidator) isTemplateInUse(
	cluster *multigresv1alpha1.MultigresCluster,
	name string,
) bool {
	refName := multigresv1alpha1.TemplateRef(name)
	switch v.Kind {
	case "CoreTemplate":
		if cluster.Spec.TemplateDefaults.CoreTemplate == refName {
			return true
		}
		if cluster.Spec.Multiadmin != nil && cluster.Spec.Multiadmin.TemplateRef == refName {
			return true
		}
		if cluster.Spec.GlobalTopoServer != nil &&
			cluster.Spec.GlobalTopoServer.TemplateRef == refName {
			return true
		}
		if cluster.Spec.MultiadminWeb != nil &&
			cluster.Spec.MultiadminWeb.TemplateRef == refName {
			return true
		}
	case "CellTemplate":
		if cluster.Spec.TemplateDefaults.CellTemplate == refName {
			return true
		}
		for _, cell := range cluster.Spec.Cells {
			if cell.CellTemplate == refName {
				return true
			}
		}
	case "ShardTemplate":
		if cluster.Spec.TemplateDefaults.ShardTemplate == refName {
			return true
		}
		for _, db := range cluster.Spec.Databases {
			for _, tg := range db.TableGroups {
				for _, shard := range tg.Shards {
					if shard.ShardTemplate == refName {
						return true
					}
				}
			}
		}
	}
	return false
}

// ============================================================================
// Child Resource Validator (Fallback)
// ============================================================================

// +kubebuilder:webhook:path=/validate-multigres-com-v1alpha1-cell,mutating=false,failurePolicy=fail,sideEffects=None,groups=multigres.com,resources=cells,verbs=create;update;delete,versions=v1alpha1,name=vcell.kb.io,admissionReviewVersions=v1
// +kubebuilder:webhook:path=/validate-multigres-com-v1alpha1-shard,mutating=false,failurePolicy=fail,sideEffects=None,groups=multigres.com,resources=shards,verbs=create;update;delete,versions=v1alpha1,name=vshard.kb.io,admissionReviewVersions=v1
// +kubebuilder:webhook:path=/validate-multigres-com-v1alpha1-toposerver,mutating=false,failurePolicy=fail,sideEffects=None,groups=multigres.com,resources=toposervers,verbs=create;update;delete,versions=v1alpha1,name=vtoposerver.kb.io,admissionReviewVersions=v1
// +kubebuilder:webhook:path=/validate-multigres-com-v1alpha1-tablegroup,mutating=false,failurePolicy=fail,sideEffects=None,groups=multigres.com,resources=tablegroups,verbs=create;update;delete,versions=v1alpha1,name=vtablegroup.kb.io,admissionReviewVersions=v1

// ChildResourceValidator prevents direct modification of managed child resources.
type ChildResourceValidator struct {
	exemptPrincipals []string
}

var _ webhook.CustomValidator = &ChildResourceValidator{}

// NewChildResourceValidator creates a validator that blocks direct modification of managed child resources.
func NewChildResourceValidator(exemptPrincipals ...string) *ChildResourceValidator {
	return &ChildResourceValidator{
		exemptPrincipals: exemptPrincipals,
	}
}

// ValidateCreate rejects direct creation of managed child resources.
func (v *ChildResourceValidator) ValidateCreate(
	ctx context.Context,
	obj runtime.Object,
) (admission.Warnings, error) {
	return v.validate(ctx, obj)
}

// ValidateUpdate rejects direct modification of managed child resources.
func (v *ChildResourceValidator) ValidateUpdate(
	ctx context.Context,
	oldObj, newObj runtime.Object,
) (admission.Warnings, error) {
	return v.validate(ctx, newObj)
}

// ValidateDelete rejects direct deletion of managed child resources.
func (v *ChildResourceValidator) ValidateDelete(
	ctx context.Context,
	obj runtime.Object,
) (admission.Warnings, error) {
	return v.validate(ctx, obj)
}

func (v *ChildResourceValidator) validate(
	ctx context.Context,
	obj runtime.Object,
) (admission.Warnings, error) {
	req, err := admission.RequestFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not get admission request: %w", err)
	}

	if slices.Contains(v.exemptPrincipals, req.UserInfo.Username) {
		return nil, nil
	}

	// Determine kind for error message
	kind := obj.GetObjectKind().GroupVersionKind().Kind
	if kind == "" {
		// Fallback if GVK is not set on the object
		kind = "Resource"
	}

	return nil, fmt.Errorf(
		"direct modification of %s is prohibited; this resource is managed by the MultigresCluster parent object",
		kind,
	)
}
