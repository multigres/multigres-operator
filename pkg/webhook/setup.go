package webhook

import (
	"fmt"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/resolver"
	"github.com/multigres/multigres-operator/pkg/webhook/handlers"
)

// Options contains the configuration required to set up the webhook server.
type Options struct {
	Namespace          string
	ServiceAccountName string
}

// Setup configures the webhook handlers using the builder pattern.
func Setup(mgr ctrl.Manager, res *resolver.Resolver, opts Options) error {
	logger := mgr.GetLogger().WithName("webhook-setup")
	logger.Info("Registering webhook handlers")

	// SAFETY CHECK: Ensure resolver is provided
	if res == nil {
		return fmt.Errorf("webhook setup failed: resolver cannot be nil")
	}

	// 1. Mutating Webhook: MultigresCluster
	if err := ctrl.NewWebhookManagedBy(mgr, &multigresv1alpha1.MultigresCluster{}).
		WithCustomDefaulter(handlers.NewMultigresClusterDefaulter(res)).
		Complete(); err != nil {
		return fmt.Errorf("failed to register MultigresCluster defaulter: %w", err)
	}

	// 2. Validating Webhook: MultigresCluster
	if err := ctrl.NewWebhookManagedBy(mgr, &multigresv1alpha1.MultigresCluster{}).
		WithCustomValidator(handlers.NewMultigresClusterValidator(mgr.GetClient())).
		Complete(); err != nil {
		return fmt.Errorf("failed to register MultigresCluster validator: %w", err)
	}

	// 3. Validating Webhook: Templates (Core, Cell, Shard)
	templates := map[client.Object]string{
		&multigresv1alpha1.CoreTemplate{}:  "CoreTemplate",
		&multigresv1alpha1.CellTemplate{}:  "CellTemplate",
		&multigresv1alpha1.ShardTemplate{}: "ShardTemplate",
	}

	for obj, kind := range templates {
		if err := ctrl.NewWebhookManagedBy(mgr, obj).
			WithCustomValidator(handlers.NewTemplateValidator(mgr.GetClient(), kind)).
			Complete(); err != nil {
			return fmt.Errorf("failed to register validator for %s: %w", kind, err)
		}
	}

	// 4. Validating Webhook: Child Resources (Cell, Shard, TopoServer, TableGroup)
	operatorPrincipal := fmt.Sprintf(
		"system:serviceaccount:%s:%s",
		opts.Namespace,
		opts.ServiceAccountName,
	)
	if opts.ServiceAccountName == "" {
		operatorPrincipal = fmt.Sprintf(
			"system:serviceaccount:%s:multigres-operator",
			opts.Namespace,
		)
	}
	childValidator := handlers.NewChildResourceValidator(
		operatorPrincipal,
		"system:serviceaccount:kube-system:generic-garbage-collector",
		"system:serviceaccount:kube-system:namespace-controller",
	)

	childResources := []client.Object{
		&multigresv1alpha1.Cell{},
		&multigresv1alpha1.Shard{},
		&multigresv1alpha1.TopoServer{},
		&multigresv1alpha1.TableGroup{},
	}

	for _, obj := range childResources {
		if err := ctrl.NewWebhookManagedBy(mgr, obj).
			WithCustomValidator(childValidator).
			Complete(); err != nil {
			kind := obj.GetObjectKind().GroupVersionKind().Kind
			return fmt.Errorf("failed to register validator for %s: %w", kind, err)
		}
	}

	return nil
}
