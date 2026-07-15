package multigrescluster

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/resolver"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// Builder function variables to allow mocking in tests
var (
	buildMultiadminService                = BuildMultiadminService
	buildMultiadminWebDeployment          = BuildMultiadminWebDeployment
	buildMultiadminWebService             = BuildMultiadminWebService
	buildMultigatewayGlobalService        = BuildMultigatewayGlobalService
	buildMultigatewayGlobalReplicaService = BuildMultigatewayGlobalReplicaService
)

func (r *MultigresClusterReconciler) reconcileGlobalComponents(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	res *resolver.Resolver,
) error {
	if err := r.reconcileGlobalTopoServer(ctx, cluster, res); err != nil {
		return err
	}
	if err := r.reconcileMultiadmin(ctx, cluster, res); err != nil {
		return err
	}
	if err := r.reconcileMultiadminWeb(ctx, cluster, res); err != nil {
		return err
	}
	return nil
}

// reconcileGlobalTopoServer reconciles the global TopoServer resource.
func (r *MultigresClusterReconciler) reconcileGlobalTopoServer(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	res *resolver.Resolver,
) error {
	spec, err := res.ResolveGlobalTopo(ctx, cluster)
	if err != nil {
		r.Recorder.Event(cluster, "Warning", "TemplateMissing", err.Error())
		return fmt.Errorf("failed to resolve global topo: %w", err)
	}

	desired, err := BuildGlobalTopoServer(cluster, spec, r.Scheme)
	if err != nil {
		return fmt.Errorf("failed to build global topo server: %w", err)
	}

	// If desired is nil, it means we don't need a managed TopoServer (e.g. external).
	// Clean up any existing managed TopoServer that may be left over from a mode switch.
	if desired == nil {
		existing := &multigresv1alpha1.TopoServerList{}
		if err := r.List(ctx, existing,
			client.InNamespace(cluster.Namespace),
			client.MatchingLabels{metadata.LabelMultigresCluster: cluster.Name},
		); err != nil {
			return fmt.Errorf("failed to list existing topo servers: %w", err)
		}
		for i := range existing.Items {
			if err := r.Delete(ctx, &existing.Items[i]); err != nil && !errors.IsNotFound(err) {
				return fmt.Errorf("failed to delete stale topo server %s: %w",
					existing.Items[i].Name, err)
			}
			r.Recorder.Eventf(cluster, "Normal", "Deleted",
				"Deleted stale TopoServer %s (switched to external)", existing.Items[i].Name)
		}
		return nil
	}

	// Server Side Apply
	desired.SetGroupVersionKind(multigresv1alpha1.GroupVersion.WithKind("TopoServer"))
	if err := r.Patch(
		ctx,
		desired,
		client.Apply,
		client.ForceOwnership,
		client.FieldOwner("multigres-operator"),
	); err != nil {
		return fmt.Errorf("failed to apply global topo server: %w", err)
	}

	r.Recorder.Eventf(
		cluster,
		"Normal",
		"Applied",
		"Applied Global TopoServer %s",
		desired.Name,
	)

	return nil
}

// reconcileMultiadmin reconciles the Multiadmin Deployment.
func (r *MultigresClusterReconciler) reconcileMultiadmin(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	res *resolver.Resolver,
) error {
	spec, err := res.ResolveMultiadmin(ctx, cluster)
	if err != nil {
		r.Recorder.Event(cluster, "Warning", "TemplateMissing", err.Error())
		return fmt.Errorf("failed to resolve multiadmin: %w", err)
	}

	desired, err := BuildMultiadminDeployment(cluster, spec, r.Scheme)
	if err != nil {
		return fmt.Errorf("failed to build multiadmin deployment: %w", err)
	}

	// Server Side Apply
	desired.SetGroupVersionKind(appsv1.SchemeGroupVersion.WithKind("Deployment"))
	if err := r.Patch(
		ctx,
		desired,
		client.Apply,
		client.ForceOwnership,
		client.FieldOwner("multigres-operator"),
	); err != nil {
		return fmt.Errorf("failed to apply multiadmin deployment: %w", err)
	}

	r.Recorder.Eventf(
		cluster,
		"Normal",
		"Applied",
		"Applied Multiadmin Deployment %s",
		desired.Name,
	)

	// Reconcile Service
	desiredSvc, err := buildMultiadminService(cluster, r.Scheme)
	if err != nil {
		return fmt.Errorf("failed to build multiadmin service: %w", err)
	}

	desiredSvc.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Service"))
	if err := r.Patch(
		ctx,
		desiredSvc,
		client.Apply,
		client.ForceOwnership,
		client.FieldOwner("multigres-operator"),
	); err != nil {
		return fmt.Errorf("failed to apply multiadmin service: %w", err)
	}

	return nil
}

// globalTopoRef resolves the topology server reference, shared by Cells and Databases reconciliation.
func (r *MultigresClusterReconciler) globalTopoRef(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	res *resolver.Resolver,
) (multigresv1alpha1.GlobalTopoServerRef, error) {
	spec, err := res.ResolveGlobalTopo(ctx, cluster)
	if err != nil {
		r.Recorder.Event(cluster, "Warning", "TemplateMissing", err.Error())
		return multigresv1alpha1.GlobalTopoServerRef{}, err
	}

	address := ""
	if spec.Etcd != nil {
		address = fmt.Sprintf("%s-global-topo.%s.svc:2379", cluster.Name, cluster.Namespace)
	} else if spec.External != nil && len(spec.External.Endpoints) > 0 {
		address = string(spec.External.Endpoints[0])
	}

	rootPath := ""
	implementation := ""

	if spec.External != nil {
		rootPath = spec.External.RootPath
		implementation = spec.External.Implementation
	} else if spec.Etcd != nil {
		rootPath = spec.Etcd.RootPath
		implementation = "etcd"
	}

	return multigresv1alpha1.GlobalTopoServerRef{
		Address:        address,
		RootPath:       rootPath,
		Implementation: implementation,
	}, nil
}

// reconcileMultiadminWeb reconciles the MultiadminWeb Deployment and Service.
func (r *MultigresClusterReconciler) reconcileMultiadminWeb(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	res *resolver.Resolver,
) error {
	spec, err := res.ResolveMultiadminWeb(ctx, cluster)
	if err != nil {
		r.Recorder.Event(cluster, "Warning", "TemplateMissing", err.Error())
		return fmt.Errorf("failed to resolve multiadmin-web: %w", err)
	}

	// 1. Reconcile Deployment
	desiredDeploy, err := buildMultiadminWebDeployment(cluster, spec, r.Scheme)
	if err != nil {
		return fmt.Errorf("failed to build multiadmin-web deployment: %w", err)
	}

	desiredDeploy.SetGroupVersionKind(appsv1.SchemeGroupVersion.WithKind("Deployment"))
	if err := r.Patch(
		ctx,
		desiredDeploy,
		client.Apply,
		client.ForceOwnership,
		client.FieldOwner("multigres-operator"),
	); err != nil {
		return fmt.Errorf("failed to apply multiadmin-web deployment: %w", err)
	}

	r.Recorder.Eventf(
		cluster,
		"Normal",
		"Applied",
		"Applied MultiadminWeb Deployment %s",
		desiredDeploy.Name,
	)

	// 2. Reconcile Service
	desiredSvc, err := buildMultiadminWebService(cluster, cluster.Spec.ExternalAdminWeb, r.Scheme)
	if err != nil {
		return fmt.Errorf("failed to build multiadmin-web service: %w", err)
	}

	desiredSvc.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Service"))
	if err := r.Patch(
		ctx,
		desiredSvc,
		client.Apply,
		client.ForceOwnership,
		client.FieldOwner("multigres-operator"),
	); err != nil {
		return fmt.Errorf("failed to apply multiadmin-web service: %w", err)
	}

	// 3. Reconcile global multigateway Service
	desiredGwSvc, err := buildMultigatewayGlobalService(
		cluster,
		cluster.Spec.ExternalGateway,
		r.Scheme,
	)
	if err != nil {
		return fmt.Errorf("failed to build global multigateway service: %w", err)
	}

	desiredGwSvc.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Service"))
	if err := r.Patch(
		ctx,
		desiredGwSvc,
		client.Apply,
		client.ForceOwnership,
		client.FieldOwner("multigres-operator"),
	); err != nil {
		return fmt.Errorf("failed to apply global multigateway service: %w", err)
	}

	// 4. Reconcile global replica-read multigateway Service
	desiredReplicaGwSvc, err := buildMultigatewayGlobalReplicaService(cluster, r.Scheme)
	if err != nil {
		return fmt.Errorf("failed to build global replica multigateway service: %w", err)
	}

	desiredReplicaGwSvc.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Service"))
	if err := r.Patch(
		ctx,
		desiredReplicaGwSvc,
		client.Apply,
		client.ForceOwnership,
		client.FieldOwner("multigres-operator"),
	); err != nil {
		return fmt.Errorf("failed to apply global replica multigateway service: %w", err)
	}

	return nil
}
