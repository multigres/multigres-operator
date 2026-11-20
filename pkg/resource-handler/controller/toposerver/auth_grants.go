package toposerver

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	api "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/resolver"
	"github.com/multigres/multigres-operator/pkg/topology"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

const (
	topologyServerCellIndex = "spec.topologyServers"
	// clusterTopologyServerIndex covers only the topology servers a cluster
	// names inline. References that arrive through a template surface through
	// the Cells they produce.
	clusterTopologyServerIndex = "spec.inlineTopologyServers"
)

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
	key := client.ObjectKeyFromObject(p.cluster)
	if grant == nil {
		grant = &etcdIdentityGrant{Name: name}
		p.grants[name] = grant
	}
	if !slices.Contains(grant.Clusters, key) {
		grant.Clusters = append(grant.Clusters, key)
	}
	grant.Permissions = append(grant.Permissions, etcdPrefixPermission(root+"/"))
	return nil
}

func desiredEtcdGrants(
	ctx context.Context,
	reader client.Reader,
	ts *api.TopoServer,
) ([]etcdIdentityGrant, error) {
	return desiredEtcdGrantsWithDiscovery(ctx, reader, ts, true)
}

func desiredEtcdGrantsWithDiscovery(
	ctx context.Context,
	reader client.Reader,
	ts *api.TopoServer,
	useIndex bool,
) ([]etcdIdentityGrant, error) {
	clusters, cells, err := grantDiscoveryObjects(ctx, reader, ts, useIndex)
	if err != nil {
		return nil, err
	}
	cellsByCluster := indexCellsByCluster(cells.Items)
	grants := map[string]*etcdIdentityGrant{}
	matchedCells := map[client.ObjectKey]bool{}
	for i := range clusters.Items {
		cluster := &clusters.Items[i]
		clusterCells := cellsByCluster[client.ObjectKeyFromObject(cluster)]
		for _, cell := range clusterCells {
			matchedCells[client.ObjectKeyFromObject(cell)] = true
		}

		if err := addClusterGrants(ctx, reader, ts, cluster, clusterCells, grants); err != nil {
			return nil, err
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
	return finalizeGrants(grants), nil
}

func finalizeGrants(grants map[string]*etcdIdentityGrant) []etcdIdentityGrant {
	result := make([]etcdIdentityGrant, 0, len(grants))
	for _, grant := range grants {
		slices.SortFunc(grant.Permissions, func(a, b etcdPermission) int {
			if n := strings.Compare(a.Key, b.Key); n != 0 {
				return n
			}
			return int(a.Type) - int(b.Type)
		})
		grant.Permissions = slices.Compact(grant.Permissions)
		slices.SortFunc(grant.Clusters, func(a, b client.ObjectKey) int {
			return strings.Compare(a.String(), b.String())
		})
		result = append(result, *grant)
	}
	slices.SortFunc(result, func(a, b etcdIdentityGrant) int {
		return strings.Compare(a.Name, b.Name)
	})
	return result
}

func addClusterGrants(
	ctx context.Context,
	reader client.Reader,
	ts *api.TopoServer,
	cluster *api.MultigresCluster,
	clusterCells []*api.Cell,
	grants map[string]*etcdIdentityGrant,
) error {
	res := resolver.NewResolver(reader, cluster.Namespace)
	global, globalErr := res.ResolveGlobalTopo(ctx, cluster)
	matchesGlobal := false
	if globalErr == nil {
		matchesGlobal = externalTopologyMatches(ts, cluster.Namespace, global.External) ||
			(global.Etcd != nil && topologyOwnedBy(ts, cluster))
	}

	referencesServer := topologyOwnedBy(ts, cluster) || matchesGlobal
	for _, cell := range clusterCells {
		referencesServer = referencesServer || cellReferencesTopoServer(ts, cell)
	}

	type resolvedCell struct {
		local *api.LocalTopoServerSpec
		err   error
	}
	resolvedCells := make([]resolvedCell, 0, len(cluster.Spec.Cells))
	materialized := make(map[api.CellName]bool, len(clusterCells))
	for _, cell := range clusterCells {
		materialized[cell.Spec.Name] = true
	}
	// Materialized Cells are the steady-state source of truth. Resolve only
	// desired Cells that do not exist yet, so activation can pre-grant them.
	for i := range cluster.Spec.Cells {
		config := &cluster.Spec.Cells[i]
		if materialized[config.Name] {
			continue
		}
		_, _, local, err := res.ResolveCell(ctx, cluster, config)
		resolvedCells = append(resolvedCells, resolvedCell{local: local, err: err})
		if err == nil && externalTopologyMatches(ts, cluster.Namespace, externalLocal(local)) {
			referencesServer = true
		}
	}

	// Resolution failures in unrelated clusters must not block this server.
	// Ownership, a materialized Cell, or a successfully resolved reference
	// establishes association and makes incomplete discovery fail closed.
	if !referencesServer {
		return nil
	}
	if globalErr != nil {
		return fmt.Errorf("resolving cluster %s topology: %w", cluster.Name, globalErr)
	}
	for _, resolved := range resolvedCells {
		if resolved.err != nil {
			return fmt.Errorf(
				"resolving cluster %s cell topology: %w",
				cluster.Name,
				resolved.err,
			)
		}
	}

	roots, err := topology.ForCluster(cluster)
	if err != nil {
		return err
	}
	project := projectGrants{roots: roots, grants: grants, cluster: cluster}
	if matchesGlobal {
		var rootPath string
		if global.Etcd != nil {
			rootPath = global.Etcd.RootPath
		} else {
			rootPath = global.External.RootPath
		}
		if err := project.add(rootPath); err != nil {
			return err
		}
	}
	for _, cell := range clusterCells {
		if err := project.addCell(ts, cell); err != nil {
			return err
		}
	}
	for _, resolved := range resolvedCells {
		if externalTopologyMatches(ts, cluster.Namespace, externalLocal(resolved.local)) {
			if err := project.add(resolved.local.External.RootPath); err != nil {
				return err
			}
		}
	}
	return nil
}

func grantDiscoveryObjects(
	ctx context.Context,
	reader client.Reader,
	ts *api.TopoServer,
	useIndex bool,
) (*api.MultigresClusterList, *api.CellList, error) {
	clusters, cells := &api.MultigresClusterList{}, &api.CellList{}
	owner := metav1.GetControllerOf(ts)
	if owner != nil && owner.Kind != "MultigresCluster" && owner.Kind != "Cell" {
		return nil, nil, fmt.Errorf(
			"unsupported topology server owner kind %q for %s",
			owner.Kind,
			client.ObjectKeyFromObject(ts),
		)
	}
	if !useIndex {
		if err := reader.List(ctx, clusters); err != nil {
			return nil, nil, err
		}
		if err := reader.List(ctx, cells); err != nil {
			return nil, nil, err
		}
		return clusters, cells, nil
	}

	seenClusters := map[client.ObjectKey]bool{}
	if owner != nil {
		ownerCluster, topologyOwnerCell, err := topologyServerOwnerCluster(ctx, reader, ts, owner)
		if err != nil {
			return nil, nil, err
		}
		if ownerCluster == nil {
			return clusters, cells, nil
		}
		clusters.Items = append(clusters.Items, *ownerCluster)
		seenClusters[client.ObjectKeyFromObject(ownerCluster)] = true
		if err := listClusterCells(ctx, reader, ownerCluster, cells); err != nil {
			return nil, nil, err
		}
		if topologyOwnerCell != nil {
			appendCellIfMissing(cells, topologyOwnerCell)
		}
	}

	key := client.ObjectKeyFromObject(ts).String()
	referencing := &api.CellList{}
	if err := reader.List(
		ctx,
		referencing,
		client.MatchingFields{topologyServerCellIndex: key},
	); err != nil {
		return nil, nil, fmt.Errorf("listing Cells that reference topology server %s: %w", key, err)
	}
	for i := range referencing.Items {
		cell := &referencing.Items[i]
		appendCellIfMissing(cells, cell)
		clusterKey, ok := cellClusterKey(cell)
		if !ok || seenClusters[clusterKey] {
			continue
		}
		cluster := &api.MultigresCluster{}
		if err := reader.Get(ctx, clusterKey, cluster); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, nil, fmt.Errorf(
				"reading cluster for referencing Cell %s: %w",
				client.ObjectKeyFromObject(cell),
				err,
			)
		}
		seenClusters[clusterKey] = true
		clusters.Items = append(clusters.Items, *cluster)
		if err := listClusterCells(ctx, reader, cluster, cells); err != nil {
			return nil, nil, err
		}
	}
	if owner != nil {
		return clusters, cells, nil
	}

	// A standalone server also serves clusters that name it inline but have no
	// Cells yet.
	referencingClusters := &api.MultigresClusterList{}
	if err := reader.List(
		ctx,
		referencingClusters,
		client.MatchingFields{clusterTopologyServerIndex: key},
	); err != nil {
		return nil, nil, fmt.Errorf(
			"listing clusters that reference topology server %s: %w",
			key,
			err,
		)
	}
	for i := range referencingClusters.Items {
		cluster := &referencingClusters.Items[i]
		clusterKey := client.ObjectKeyFromObject(cluster)
		if seenClusters[clusterKey] {
			continue
		}
		seenClusters[clusterKey] = true
		clusters.Items = append(clusters.Items, *cluster)
		if err := listClusterCells(ctx, reader, cluster, cells); err != nil {
			return nil, nil, err
		}
	}
	return clusters, cells, nil
}

func topologyServerOwnerCluster(
	ctx context.Context,
	reader client.Reader,
	ts *api.TopoServer,
	owner *metav1.OwnerReference,
) (*api.MultigresCluster, *api.Cell, error) {
	clusterName := ""
	var topologyOwnerCell *api.Cell
	switch owner.Kind {
	case "MultigresCluster":
		clusterName = owner.Name
	case "Cell":
		ownerCell := &api.Cell{}
		key := client.ObjectKey{Namespace: ts.Namespace, Name: owner.Name}
		if err := reader.Get(ctx, key, ownerCell); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, nil, nil
			}
			return nil, nil, fmt.Errorf("reading topology server owner Cell %s: %w", key, err)
		}
		if owner.UID != "" && ownerCell.UID != owner.UID {
			return nil, nil, fmt.Errorf("topology server owner Cell %s has changed identity", key)
		}
		clusterKey, ok := cellClusterKey(ownerCell)
		if ok {
			clusterName = clusterKey.Name
		}
		if clusterName == "" {
			return nil, nil, fmt.Errorf(
				"cannot resolve project identity for topology server owner Cell %s",
				key,
			)
		}
		topologyOwnerCell = ownerCell
	default:
		return nil, nil, fmt.Errorf(
			"unsupported topology server owner kind %q for %s",
			owner.Kind,
			client.ObjectKeyFromObject(ts),
		)
	}

	cluster := &api.MultigresCluster{}
	key := client.ObjectKey{Namespace: ts.Namespace, Name: clusterName}
	if err := reader.Get(ctx, key, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("reading topology server owner cluster %s: %w", key, err)
	}
	if owner.Kind == "MultigresCluster" && owner.UID != "" && cluster.UID != owner.UID {
		return nil, nil, fmt.Errorf("topology server owner cluster %s has changed identity", key)
	}
	return cluster, topologyOwnerCell, nil
}

func listClusterCells(
	ctx context.Context,
	reader client.Reader,
	cluster *api.MultigresCluster,
	cells *api.CellList,
) error {
	listed := &api.CellList{}
	if err := reader.List(
		ctx,
		listed,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{metadata.LabelMultigresCluster: cluster.Name},
	); err != nil {
		return fmt.Errorf(
			"listing Cells for cluster %s: %w",
			client.ObjectKeyFromObject(cluster),
			err,
		)
	}
	for i := range listed.Items {
		appendCellIfMissing(cells, &listed.Items[i])
	}
	return nil
}

func appendCellIfMissing(cells *api.CellList, cell *api.Cell) {
	if slices.ContainsFunc(cells.Items, func(existing api.Cell) bool {
		return existing.Namespace == cell.Namespace && existing.Name == cell.Name
	}) {
		return
	}
	cells.Items = append(cells.Items, *cell)
}

func cellClusterKey(cell *api.Cell) (client.ObjectKey, bool) {
	clusterName := cell.Labels[metadata.LabelMultigresCluster]
	if owner := metav1.GetControllerOf(cell); owner != nil && owner.Kind == "MultigresCluster" {
		clusterName = owner.Name
	}
	return client.ObjectKey{Namespace: cell.Namespace, Name: clusterName}, clusterName != ""
}

func cellTopologyServerKeys(object client.Object) []string {
	cell, ok := object.(*api.Cell)
	if !ok {
		return nil
	}
	servers := cellTopologyServers(cell)
	keys := make([]string, 0, len(servers))
	for _, server := range servers {
		keys = append(keys, server.String())
	}
	return keys
}

func cellTopologyServers(cell *api.Cell) []client.ObjectKey {
	endpoints := []api.EndpointUrl{api.EndpointUrl(cell.Spec.GlobalTopoServer.Address)}
	if local := externalLocal(cell.Spec.TopoServer); local != nil {
		endpoints = append(endpoints, local.Endpoints...)
	}
	return referencedTopologyServers(cell.Namespace, endpoints)
}

func clusterTopologyServerKeys(object client.Object) []string {
	cluster, ok := object.(*api.MultigresCluster)
	if !ok {
		return nil
	}
	var endpoints []api.EndpointUrl
	if global := cluster.Spec.GlobalTopoServer; global != nil && global.External != nil {
		endpoints = append(endpoints, global.External.Endpoints...)
	}
	for i := range cluster.Spec.Cells {
		if inline := cluster.Spec.Cells[i].Spec; inline != nil {
			if external := externalLocal(inline.LocalTopoServer); external != nil {
				endpoints = append(endpoints, external.Endpoints...)
			}
		}
	}
	servers := referencedTopologyServers(cluster.Namespace, endpoints)
	keys := make([]string, 0, len(servers))
	for _, server := range servers {
		keys = append(keys, server.String())
	}
	return keys
}

func referencedTopologyServers(namespace string, endpoints []api.EndpointUrl) []client.ObjectKey {
	var servers []client.ObjectKey
	for _, endpoint := range endpoints {
		server, ok := endpointTopologyServer(namespace, string(endpoint))
		if ok && !slices.Contains(servers, server) {
			servers = append(servers, server)
		}
	}
	return servers
}

func (r *TopoServerReconciler) authManagedTopoServersForCell(
	ctx context.Context,
	object client.Object,
) []reconcile.Request {
	cell, ok := object.(*api.Cell)
	if !ok {
		return nil
	}
	return r.authManagedRequests(ctx, cellTopologyServers(cell), object)
}

// authManagedTopoServersForDeletedCluster resolves the servers a deleted
// cluster pointed at from its last known spec. A template that cannot be read
// leaves its servers to the Cell events and the periodic resync.
func (r *TopoServerReconciler) authManagedTopoServersForDeletedCluster(
	ctx context.Context,
	object client.Object,
) []reconcile.Request {
	cluster, ok := object.(*api.MultigresCluster)
	if !ok {
		return nil
	}
	res := resolver.NewResolver(r.Client, cluster.Namespace)
	var endpoints []api.EndpointUrl
	if global, err := res.ResolveGlobalTopo(ctx, cluster); err == nil &&
		global.External != nil {
		endpoints = append(endpoints, global.External.Endpoints...)
	}
	for i := range cluster.Spec.Cells {
		_, _, local, err := res.ResolveCell(ctx, cluster, &cluster.Spec.Cells[i])
		if err != nil {
			continue
		}
		if external := externalLocal(local); external != nil {
			endpoints = append(endpoints, external.Endpoints...)
		}
	}
	return r.authManagedRequests(
		ctx,
		referencedTopologyServers(cluster.Namespace, endpoints),
		object,
	)
}

func (r *TopoServerReconciler) authManagedRequests(
	ctx context.Context,
	servers []client.ObjectKey,
	source client.Object,
) []reconcile.Request {
	var requests []reconcile.Request
	for _, server := range servers {
		ts := &api.TopoServer{}
		switch err := r.Get(ctx, server, ts); {
		case apierrors.IsNotFound(err):
			continue
		case err != nil:
			log.FromContext(ctx).Error(
				err,
				"Failed to read referenced TopoServer, enqueueing anyway",
				"toposerver", server,
				"source", client.ObjectKeyFromObject(source),
				"kind", fmt.Sprintf("%T", source),
			)
		case !topology.AuthManaged(ts):
			continue
		}
		requests = append(requests, reconcile.Request{NamespacedName: server})
	}
	return requests
}

func endpointTopologyServer(namespace, endpoint string) (client.ObjectKey, bool) {
	if !strings.Contains(endpoint, "://") {
		endpoint = "https://" + endpoint
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.User != nil || (u.Scheme != "http" && u.Scheme != "https") ||
		(u.Port() != "" && u.Port() != "2379") ||
		(u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return client.ObjectKey{}, false
	}
	labels := strings.Split(strings.TrimSuffix(strings.ToLower(u.Hostname()), "."), ".")
	service, serverNamespace := "", ""
	switch {
	case len(labels) == 1:
		service, serverNamespace = labels[0], namespace
	case len(labels) == 2:
		service, serverNamespace = labels[0], labels[1]
	case len(labels) >= 3 && labels[2] == "svc":
		service, serverNamespace = labels[0], labels[1]
	case len(labels) >= 4 && labels[3] == "svc":
		service, serverNamespace = labels[1], labels[2]
	default:
		return client.ObjectKey{}, false
	}
	service = strings.TrimSuffix(service, "-headless")
	if service == "" || serverNamespace == "" {
		return client.ObjectKey{}, false
	}
	return client.ObjectKey{Namespace: serverNamespace, Name: service}, true
}

func indexCellsByCluster(cells []api.Cell) map[client.ObjectKey][]*api.Cell {
	result := make(map[client.ObjectKey][]*api.Cell)
	for i := range cells {
		cell := &cells[i]
		clusterName := cell.Labels[metadata.LabelMultigresCluster]
		if owner := metav1.GetControllerOf(cell); owner != nil && owner.Kind == "MultigresCluster" {
			clusterName = owner.Name
		}
		if clusterName == "" {
			continue
		}
		key := client.ObjectKey{Namespace: cell.Namespace, Name: clusterName}
		result[key] = append(result[key], cell)
	}
	return result
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

func cellReferencesTopoServer(ts *api.TopoServer, cell *api.Cell) bool {
	return topology.EndpointMatchesTopoServer(
		ts,
		cell.Namespace,
		cell.Spec.GlobalTopoServer.Address,
	) ||
		externalTopologyMatches(ts, cell.Namespace, externalLocal(cell.Spec.TopoServer)) ||
		(cell.Spec.TopoServer != nil && cell.Spec.TopoServer.Etcd != nil && topologyOwnedBy(ts, cell))
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
			condition.Reason, condition.Message = authFailureReason(
				reconcileErr,
			), reconcileErr.Error()
		}
		meta.SetStatusCondition(&ts.Status.Conditions, condition)
	}()
	if !topology.AuthManaged(ts) {
		r.authVerified.evict(client.ObjectKeyFromObject(ts))
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
	if removed := slices.DeleteFunc(
		slices.Clone(ts.Status.AuthIdentities),
		func(identity string) bool {
			return desiredNames[identity]
		},
	); len(
		removed,
	) > 0 {
		desired, err = r.confirmIdentityRemoval(ctx, ts, desired, removed)
		if err != nil {
			return false, fmt.Errorf("confirming topology identity removal: %w", err)
		}
	}
	// A standalone server is built to be shared, so its first tenant must find
	// auth already on. An owned server with no identity has failed to resolve one.
	if len(desired) == 0 && len(ts.Status.AuthIdentities) == 0 && !ts.Status.AuthEnabled &&
		metav1.GetControllerOf(ts) != nil {
		return false, fmt.Errorf("no project identities resolved for topology server")
	}
	var conflictErr error
	var retain map[string]bool
	if conflicts := grantConflicts(desired); len(conflicts) > 0 {
		conflictErr = &grantConflictError{conflicts: conflicts}
		if !ts.Status.AuthEnabled {
			return false, conflictErr
		}
		// Neither side of a conflict gains access. Tracked identities keep what
		// they already hold and are not pruned.
		retain = conflictedIdentities(conflicts)
		desired = slices.DeleteFunc(slices.Clone(desired), func(grant etcdIdentityGrant) bool {
			return retain[grant.Name]
		})
	}
	for _, key := range []client.ObjectKey{
		{Namespace: ts.Namespace, Name: api.TopoServerCertSecretName(ts.Name)},
		topology.AdminSecretKey(r.OperatorNamespace, ts),
	} {
		secret := &corev1.Secret{}
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
	if err := r.reconcileEtcdIdentities(ctx, ts, desired, retain); err != nil {
		return false, err
	}
	if conflictErr != nil {
		return false, conflictErr
	}
	ts.Status.AuthEnabled = true
	condition.Status, condition.Reason, condition.Message = metav1.ConditionTrue, "Reconciled", "Topology authorization grants are reconciled"
	if len(desired) == 0 {
		condition.Message = "Topology authorization is enabled and no cluster references this server yet"
	}
	return true, nil
}

func authFailureReason(err error) string {
	var conflict *grantConflictError
	var failures *identityFailuresError
	switch {
	case errors.As(err, &conflict):
		return "IdentityConflict"
	case errors.As(err, &failures):
		return "IdentitiesFailed"
	default:
		return "ReconcileFailed"
	}
}
