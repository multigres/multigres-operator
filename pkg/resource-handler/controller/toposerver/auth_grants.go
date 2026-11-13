package toposerver

import (
	"context"
	"fmt"
	"path"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/resolver"
	"github.com/multigres/multigres-operator/pkg/topology"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

const authRecheckInterval = 30 * time.Second

func topologyOwnedBy(ts *api.TopoServer, owner client.Object) bool {
	return ts.Namespace == owner.GetNamespace() && metav1.IsControlledBy(ts, owner)
}

func externalTopologyMatches(
	ts *api.TopoServer,
	namespace string,
	external *api.ExternalTopoServerSpec,
) bool {
	if external == nil || (external.Implementation != "" && external.Implementation != "etcd") {
		return false
	}
	return slices.ContainsFunc(external.Endpoints, func(e api.EndpointUrl) bool {
		return topology.EndpointMatchesTopoServer(ts, namespace, string(e))
	})
}

type projectGrants struct {
	roots   topology.Roots
	grants  map[string]*etcdIdentityGrant
	cluster *api.MultigresCluster
}

func (p *projectGrants) add(root string) error {
	if !api.OperatorIssuesTopoClientCert(p.cluster) {
		return fmt.Errorf(
			"cannot authorize referencing cluster %s/%s: an operator-issued topology client certificate is required",
			p.cluster.Namespace,
			p.cluster.Name,
		)
	}
	if !strings.HasPrefix(root+"/", p.roots.KeyPrefix()) || strings.ContainsRune(root, 0) ||
		path.Clean(root) != root {
		return fmt.Errorf(
			"topology root %q is outside project prefix %q or is not canonical",
			root,
			p.roots.KeyPrefix(),
		)
	}
	name := p.roots.ClusterRoot()
	grant := p.grants[name]
	if grant == nil {
		grant = &etcdIdentityGrant{Name: name}
		p.grants[name] = grant
	}
	grant.Permissions = append(grant.Permissions, etcdPrefixPermission(root+"/"))
	return nil
}

func desiredEtcdGrants(
	ctx context.Context,
	reader client.Reader,
	ts *api.TopoServer,
) ([]etcdIdentityGrant, error) {
	clusters, cells := &api.MultigresClusterList{}, &api.CellList{}
	if err := reader.List(ctx, clusters); err != nil {
		return nil, err
	}
	if err := reader.List(ctx, cells); err != nil {
		return nil, err
	}
	grants := map[string]*etcdIdentityGrant{}
	matchedCells := map[client.ObjectKey]bool{}
	for i := range clusters.Items {
		cluster := &clusters.Items[i]
		res := resolver.NewResolver(reader, cluster.Namespace)
		global, err := res.ResolveGlobalTopo(ctx, cluster)
		if err != nil {
			return nil, fmt.Errorf("resolving cluster %s topology: %w", cluster.Name, err)
		}
		roots, err := topology.ForCluster(cluster)
		if err != nil {
			return nil, err
		}
		project := projectGrants{roots: roots, grants: grants, cluster: cluster}
		ownedGlobal := global.Etcd != nil && topologyOwnedBy(ts, cluster)
		matchedEndpoint := global.Etcd != nil && ts.Namespace == cluster.Namespace &&
			ts.Name == cluster.Name+"-global-topo"
		matchesGlobal := externalTopologyMatches(ts, cluster.Namespace, global.External) ||
			ownedGlobal || matchedEndpoint
		if matchesGlobal {
			var rootPath string
			if global.Etcd != nil {
				rootPath = global.Etcd.RootPath
			} else {
				rootPath = global.External.RootPath
			}
			if err := project.add(rootPath); err != nil {
				return nil, err
			}
		}
		for j := range cells.Items {
			cell := &cells.Items[j]
			if cell.Namespace != cluster.Namespace {
				continue
			}
			if !metav1.IsControlledBy(cell, cluster) &&
				cell.Labels[metadata.LabelMultigresCluster] != cluster.Name {
				continue
			}
			matchedCells[client.ObjectKeyFromObject(cell)] = true
			if err := project.addCell(ts, cell); err != nil {
				return nil, err
			}
		}
		// Resolve the parent desired cells too, so first activation cannot race
		// child creation or grant only a subset during a cell rollout.
		for j := range cluster.Spec.Cells {
			config := &cluster.Spec.Cells[j]
			_, _, local, err := res.ResolveCell(ctx, cluster, config)
			if err != nil {
				return nil, err
			}
			if externalTopologyMatches(ts, cluster.Namespace, externalLocal(local)) {
				if err := project.add(local.External.RootPath); err != nil {
					return nil, err
				}
			}
		}
	}
	for i := range cells.Items {
		cell := &cells.Items[i]
		if matchedCells[client.ObjectKeyFromObject(cell)] {
			continue
		}
		global := cell.Spec.GlobalTopoServer.Address
		if topology.EndpointMatchesTopoServer(ts, cell.Namespace, global) ||
			externalTopologyMatches(ts, cell.Namespace, externalLocal(cell.Spec.TopoServer)) ||
			topologyOwnedBy(ts, cell) {
			return nil, fmt.Errorf(
				"cannot resolve project identity for referencing cell %s/%s",
				cell.Namespace,
				cell.Name,
			)
		}
	}
	result := make([]etcdIdentityGrant, 0, len(grants))
	for _, grant := range grants {
		slices.SortFunc(grant.Permissions, func(a, b etcdPermission) int {
			if n := strings.Compare(a.Key, b.Key); n != 0 {
				return n
			}
			return int(a.Type) - int(b.Type)
		})
		grant.Permissions = slices.Compact(grant.Permissions)
		result = append(result, *grant)
	}
	slices.SortFunc(result, func(a, b etcdIdentityGrant) int {
		return strings.Compare(a.Name, b.Name)
	})
	return result, nil
}

func externalLocal(local *api.LocalTopoServerSpec) *api.ExternalTopoServerSpec {
	if local == nil {
		return nil
	}
	return local.External
}

func (p *projectGrants) addCell(ts *api.TopoServer, cell *api.Cell) error {
	global := cell.Spec.GlobalTopoServer
	if topology.EndpointMatchesTopoServer(ts, cell.Namespace, global.Address) {
		if err := p.add(global.RootPath); err != nil {
			return err
		}
	}
	local := cell.Spec.TopoServer
	if externalTopologyMatches(ts, cell.Namespace, externalLocal(local)) {
		return p.add(local.External.RootPath)
	}
	if local != nil && local.Etcd != nil && topologyOwnedBy(ts, cell) {
		return p.add(local.Etcd.RootPath)
	}
	return nil
}

func topologyAuthManaged(ts *api.TopoServer) bool {
	return ts.Spec.Auth.IsEnabled() || ts.Status.AuthEnabled || len(ts.Status.AuthIdentities) != 0
}

func (r *TopoServerReconciler) reconcileTopologyAuth(
	ctx context.Context,
	ts *api.TopoServer,
) (ready bool, reconcileErr error) {
	condition := metav1.Condition{
		Type:               "AuthorizationReady",
		Status:             metav1.ConditionFalse,
		Reason:             "PendingCredentials",
		Message:            "Waiting for topology serving and administrator credentials",
		ObservedGeneration: ts.Generation,
	}
	defer func() {
		if reconcileErr != nil {
			condition.Reason, condition.Message = "ReconcileFailed", reconcileErr.Error()
		}
		meta.SetStatusCondition(&ts.Status.Conditions, condition)
	}()
	if !topologyAuthManaged(ts) {
		condition.Reason, condition.Message = "ManagementDisabled", "Managed topology authorization has not been requested"
		return true, nil
	}
	if ts.Spec.Etcd == nil || !ts.Spec.TLS.IsEnabled() {
		return false, fmt.Errorf("topology authorization requires managed etcd with TLS")
	}
	desired, err := desiredEtcdGrants(ctx, r.Client, ts)
	if err != nil {
		return false, err
	}
	desiredNames := make(map[string]bool, len(desired))
	for _, grant := range desired {
		desiredNames[grant.Name] = true
	}
	// The cache may lag; confirm a removal against the API server before pruning.
	if slices.ContainsFunc(ts.Status.AuthIdentities, func(identity string) bool {
		return !desiredNames[identity]
	}) {
		if r.APIReader == nil {
			return false, fmt.Errorf(
				"cannot confirm topology identity removal without an uncached API reader",
			)
		}
		desired, err = desiredEtcdGrants(ctx, r.APIReader, ts)
		if err != nil {
			return false, fmt.Errorf("confirming topology identity removal: %w", err)
		}
	}
	if len(desired) == 0 && len(ts.Status.AuthIdentities) == 0 && !ts.Status.AuthEnabled {
		return false, fmt.Errorf("no project identities resolved for topology server")
	}
	for _, name := range []string{api.TopoServerCertSecretName(ts.Name), api.TopoServerAdminCertSecretName(ts.Name)} {
		secret := &corev1.Secret{}
		key := client.ObjectKey{Namespace: ts.Namespace, Name: name}
		if err := r.maintenanceReader().Get(ctx, key, secret); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		if len(secret.Data[corev1.TLSCertKey]) == 0 ||
			len(secret.Data[corev1.TLSPrivateKeyKey]) == 0 ||
			len(secret.Data["ca.crt"]) == 0 {
			return false, nil
		}
	}
	authCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := r.reconcileEtcdIdentities(authCtx, ts, desired); err != nil {
		return false, err
	}
	ts.Status.AuthEnabled = true
	condition.Status, condition.Reason, condition.Message = metav1.ConditionTrue, "Reconciled", "Topology authorization grants are reconciled"
	return true, nil
}
